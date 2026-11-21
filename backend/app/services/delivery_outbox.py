"""通知投递登记箱（outbox）：把"事件落库"与"通知投递"用同一个投递身份串起来。

设计目标
--------
1. 质量事件落库时同步登记一条投递凭据（状态 ``待投递``），事件登记与投递凭据
   在同一份内存仓库里一次写入，避免"先写事件再发通知"中途停下造成两边不一致。
2. 每条凭据有全局唯一的 ``delivery_id``（投递身份）。通知网关按投递身份幂等
   发送，同一波动被重复触发/恢复时，实际通知只投递一次。
3. 发送前先把凭据置为 ``投递中``：
   - 凭据停在 ``投递中`` 且 ``投递尝试次数 == 0``：发送前失败/中止，可安全重发；
   - 凭据停在 ``投递中`` 且 ``投递尝试次数 >= 1``：通知已实际送达但回执丢失，
     恢复时只确认既有投递，不再重复发送。
4. 收到发送回执后置 ``已投递``，并把回执（通知编号/时间）回写到质量事件处置
   清单，告警通知按投递身份消费异常数据。
5. 恢复按投递身份加锁串行化，并发恢复只有一笔能真正执行投递，其余拿到同一份
   回执，保证只确认一次实际投递。

domain_chain: 质量事件 (dq_incident)
  └─ 登记凭据 → 通知投递箱 (notification_delivery)
  └─ 按投递身份发送 → 告警通知 (dq_alert_channel)
  └─ 回执回写 → 质量事件处置清单 (dq_incident)
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

from app.services.base import BaseModuleService
from app.store import store

# ---- 投递状态机 ---------------------------------------------------------
STATUS_PENDING = "待投递"      # 凭据已落库，尚未开始发送（发送前中止停在这里或"投递中"）
STATUS_IN_FLIGHT = "投递中"    # 已认领发送，回执尚未回写
STATUS_DELIVERED = "已投递"    # 已收到发送回执，回执已回写事件
STATUS_FAILED = "发送失败"     # 发送前失败，保留凭据等待恢复

# 测试/演示用的故障注入开关：置位后下一次发送在网关调用前抛错（模拟发送前中止）
FAIL_NEXT_SEND = "fail_next_send"
# 测试/演示用的故障注入开关：置位后下一次发送网关已实际投递，但回执回写前抛错
# （模拟送达后丢回执）
LOSE_NEXT_RECEIPT = "lose_next_receipt"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class NotificationGateway:
    """模拟通知网关：真实环境里这里会换成邮件/短信/电话通道适配。

    网关自身按投递身份幂等：同一个 ``delivery_id`` 只会产生一条实际通知，
    重复调用直接返回首次投递的回执。``inbox`` 记录实际收到的通知，测试用它
    核对"实际收到的通知数"。
    """

    def __init__(self) -> None:
        self._delivered: dict[str, dict[str, Any]] = {}
        self.inbox: list[dict[str, Any]] = []

    def send(self, delivery_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        # 幂等：该投递身份已实际送达，直接返回原回执，不再产生第二条通知。
        existing = self._delivered.get(delivery_id)
        if existing is not None:
            return dict(existing)

        alert_rows = store.rows("dq_alert_channel")
        new_id = max((int(r.get("id", 0)) for r in alert_rows), default=0) + 1
        receipt = {
            "通知编号": f"DQ_A-{new_id:04d}",
            "投递身份": delivery_id,
            "关联规则集": payload["关联规则集"],
            "通知方式": payload["通知方式"],
            "接收人": payload["接收人"],
            "告警级别": payload["告警级别"],
            "消息摘要": payload["消息摘要"][:100],
            "关联事件": payload["关联事件"],
            "发送时间": _now(),
            "状态": "已发送",
        }
        alert_rows.append({
            "id": new_id,
            "配置编号": receipt["通知编号"],
            "投递身份": delivery_id,
            "关联规则集": receipt["关联规则集"],
            "通知方式": receipt["通知方式"],
            "接收人": receipt["接收人"],
            "告警级别": receipt["告警级别"],
            "静默时段": receipt["发送时间"],
            "聚合策略": "立即",
            "通道状态": "已发送",
            "关联事件": payload["关联事件"],
            "status": "已生效",
            "pending": False,
            "abnormal": payload["告警级别"] in ("critical", "warning"),
        })
        self._delivered[delivery_id] = receipt
        self.inbox.append(dict(receipt))
        return dict(receipt)

    def delivered_count(self) -> int:
        """实际投递成功的通知条数（按投递身份去重）。"""
        return len(self.inbox)

    def reset(self) -> None:
        self._delivered.clear()
        self.inbox.clear()


# 进程内单例网关与投递箱锁：并发恢复靠它串行化。
gateway = NotificationGateway()
_outbox_lock = threading.RLock()
# 每个投递身份一把互斥锁，保证同一事件的并发恢复只有一笔进入发送临界区。
_delivery_locks: dict[str, threading.Lock] = {}


def _lock_for(delivery_id: str) -> threading.Lock:
    with _outbox_lock:
        lock = _delivery_locks.get(delivery_id)
        if lock is None:
            lock = threading.Lock()
            _delivery_locks[delivery_id] = lock
        return lock


class DeliveryOutboxService(BaseModuleService):
    """通知投递登记箱领域服务。"""

    MODULE = "notification_delivery"
    ENTITY = "通知投递凭据"
    LIST_FIELDS = [
        "投递身份", "关联事件", "关联规则集", "告警级别", "通知方式",
        "接收人", "投递状态", "通知编号", "投递尝试次数", "回执时间",
    ]
    REQUIRED_FIELDS = ["投递身份", "关联事件", "关联规则集"]
    STATUS_ORDER = [STATUS_PENDING, STATUS_IN_FLIGHT, STATUS_DELIVERED, STATUS_FAILED]
    ACTION_RULES: dict[str, str] = {}

    # ---- 凭据登记 -------------------------------------------------------
    def register_delivery(
        self,
        *,
        incident_id: str,
        rule_set: str,
        severity: str,
        message: str,
        notify_method: str,
        recipients: str,
        table_name: str = "",
        field_name: str = "",
    ) -> dict[str, Any]:
        """事件落库后登记投递凭据。返回带投递身份的凭据行。"""
        rows = store.rows(self.MODULE)
        new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        delivery_id = f"DLV-{new_id:06d}"
        entry = {
            "id": new_id,
            "投递身份": delivery_id,
            "关联事件": incident_id,
            "关联规则集": rule_set,
            "目标表名": table_name,
            "监控字段": field_name,
            "告警级别": severity,
            "消息摘要": message[:100],
            "通知方式": notify_method,
            "接收人": recipients,
            "投递状态": STATUS_PENDING,
            "投递尝试次数": 0,
            "通知编号": "",
            "回执时间": "",
            "失败原因": "",
            "登记时间": _now(),
            "更新时间": _now(),
            # 列表/概览兼容字段
            "status": STATUS_PENDING,
            "pending": True,
            "abnormal": severity in ("critical", "warning"),
        }
        rows.append(entry)
        return dict(entry)

    # ---- 查询 -----------------------------------------------------------
    def find_by_incident(self, incident_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("关联事件") == incident_id:
                return row
        return None

    def find_by_delivery(self, delivery_id: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("投递身份") == delivery_id:
                return row
        return None

    def list_pending(self) -> list[dict[str, Any]]:
        return [
            row for row in store.rows(self.MODULE)
            if row.get("投递状态") in (STATUS_PENDING, STATUS_IN_FLIGHT, STATUS_FAILED)
        ]

    # ---- 发送与恢复（同一套幂等逻辑）-----------------------------------
    def send_with_receipt(self, delivery_id: str,
                          fault_flags: set[str] | None = None) -> dict[str, Any]:
        """按投递身份发送通知并回写回执；恢复中断的投递也走这里。

        并发安全：同一投递身份的调用在锁内串行，先到的一笔完成投递，后到的
        一笔看到 ``已投递`` 后直接返回既有回执，不会产生第二条实际通知。

        故障注入（``fault_flags``）：
        - ``FAIL_NEXT_SEND``：进入网关前抛错，模拟发送前中止，凭据回到
          ``待投递`` 且尝试次数为 0；
        - ``LOSE_NEXT_RECEIPT``：网关已实际投递（inbox 有记录），但回执回写
          前抛错，凭据停在 ``投递中`` 且尝试次数为 1，恢复时只确认不重发。
        """
        fault_flags = fault_flags or set()
        lock = _lock_for(delivery_id)
        with lock:
            entry = self.find_by_delivery(delivery_id)
            if entry is None:
                return {"ok": False, "message": f"投递身份 {delivery_id} 不存在",
                        "投递身份": delivery_id, "实际投递": False}

            # 已投递：直接返回既有回执（兼容已有回执，重复恢复不重发）。
            if entry["投递状态"] == STATUS_DELIVERED:
                return {
                    "ok": True,
                    "投递身份": delivery_id,
                    "关联事件": entry["关联事件"],
                    "通知编号": entry["通知编号"],
                    "回执时间": entry["回执时间"],
                    "投递状态": STATUS_DELIVERED,
                    "实际投递": False,
                    "消息": "回执已存在，复用既有投递凭据，未重复发送",
                }

            payload = {
                "关联规则集": entry["关联规则集"],
                "通知方式": entry["通知方式"],
                "接收人": entry["接收人"],
                "告警级别": entry["告警级别"],
                "消息摘要": entry["消息摘要"],
                "关联事件": entry["关联事件"],
            }

            # 区分"送达后丢回执"与"发送前失败"：
            # 已有尝试记录说明网关此前已实际投递，恢复时只确认，不再调用网关。
            already_sent = entry["投递状态"] == STATUS_IN_FLIGHT and entry["投递尝试次数"] >= 1

            if not already_sent:
                entry["投递状态"] = STATUS_IN_FLIGHT
                entry["status"] = STATUS_IN_FLIGHT
                entry["更新时间"] = _now()

                if FAIL_NEXT_SEND in fault_flags:
                    # 发送前中止：网关尚未被调用，无实际通知，凭据可安全重发。
                    entry["投递状态"] = STATUS_FAILED
                    entry["status"] = STATUS_FAILED
                    entry["失败原因"] = "发送前中止，通知未离开本系统"
                    entry["更新时间"] = _now()
                    self._sync_incident_delivery(entry)
                    return {
                        "ok": False,
                        "投递身份": delivery_id,
                        "关联事件": entry["关联事件"],
                        "投递状态": STATUS_FAILED,
                        "实际投递": False,
                        "消息": entry["失败原因"],
                    }

                receipt = gateway.send(delivery_id, payload)
                entry["投递尝试次数"] = int(entry.get("投递尝试次数", 0)) + 1
                entry["通知编号"] = receipt["通知编号"]

                if LOSE_NEXT_RECEIPT in fault_flags:
                    # 通知已实际送达（网关 inbox 已有这条），但回执回写前中断。
                    entry["失败原因"] = "通知已送达，回执回写中断"
                    entry["更新时间"] = _now()
                    self._sync_incident_delivery(entry)
                    return {
                        "ok": False,
                        "投递身份": delivery_id,
                        "关联事件": entry["关联事件"],
                        "通知编号": receipt["通知编号"],
                        "投递状态": STATUS_IN_FLIGHT,
                        "实际投递": True,
                        "消息": "已实际投递但回执丢失，凭据停在投递中",
                    }
            else:
                # 恢复送达后丢回执的凭据：网关幂等返回原回执，不产生新通知。
                receipt = gateway.send(delivery_id, payload)

            # 正常回执回写：登记箱 + 质量事件处置清单一起更新。
            entry["投递状态"] = STATUS_DELIVERED
            entry["status"] = STATUS_DELIVERED
            entry["pending"] = False
            entry["回执时间"] = receipt["发送时间"]
            entry["通知编号"] = receipt["通知编号"]
            entry["失败原因"] = ""
            entry["更新时间"] = _now()
            self._sync_incident_delivery(entry)
            return {
                "ok": True,
                "投递身份": delivery_id,
                "关联事件": entry["关联事件"],
                "通知编号": entry["通知编号"],
                "回执时间": entry["回执时间"],
                "投递状态": STATUS_DELIVERED,
                "实际投递": not already_sent,
                "消息": "通知已投递，回执已回写质量事件处置清单",
            }

    def recover_by_incident(self, incident_id: str,
                            fault_flags: set[str] | None = None) -> dict[str, Any]:
        """值班员从质量事件发起的恢复入口：按原事件的投递身份续发。"""
        entry = self.find_by_incident(incident_id)
        if entry is None:
            return {"ok": False, "message": f"事件 {incident_id} 没有登记投递凭据，无法恢复",
                    "关联事件": incident_id, "实际投递": False}
        result = self.send_with_receipt(entry["投递身份"], fault_flags)
        result.setdefault("关联事件", incident_id)
        return result

    # ---- 回执回写质量事件处置清单 --------------------------------------
    def _sync_incident_delivery(self, delivery: dict[str, Any]) -> None:
        """把投递凭据/回执回写到质量事件处置清单，处置单与投递状态保持一致。"""
        incident_rows = store.rows("dq_incident")
        for incident in incident_rows:
            if incident.get("事件编号") != delivery["关联事件"]:
                continue
            incident["投递身份"] = delivery["投递身份"]
            incident["投递状态"] = delivery["投递状态"]
            incident["通知编号"] = delivery.get("通知编号", "")
            incident["回执时间"] = delivery.get("回执时间", "")
            incident["投递尝试次数"] = delivery.get("投递尝试次数", 0)
            if delivery["投递状态"] == STATUS_DELIVERED:
                incident["处理状态"] = "通知已送达"
            elif delivery["投递状态"] == STATUS_IN_FLIGHT:
                incident["处理状态"] = "通知投递中"
            elif delivery["投递状态"] == STATUS_FAILED:
                incident["处理状态"] = "通知待重发"
            else:
                incident["处理状态"] = "通知待投递"
            return

    def domain_snapshot(self) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        return {
            "module": self.MODULE,
            "entity": "通知投递凭据",
            "artifact": "通知投递登记箱",
            "count": len(rows),
            "by_status": {
                status: sum(1 for row in rows if row.get("投递状态") == status)
                for status in (STATUS_PENDING, STATUS_IN_FLIGHT, STATUS_DELIVERED, STATUS_FAILED)
            },
            "actual_delivered": gateway.delivered_count(),
        }
