"""通知投递一致性测试：事件登记与告警通知的 outbox 投递恢复链路。

覆盖：
1. 正常链路：事件落库即有投递凭据，发送后回执回写质量事件处置清单；
2. 发送前中止：凭据可续发，恢复后只产生一条实际通知；
3. 送达后丢回执：凭据停在投递中，恢复只确认不重发，通知数不增加；
4. 并发恢复：同一事件两笔恢复请求，只有一次实际投递，回执一致；
5. 幂等兼容：已送达事件重复恢复直接返回既有回执；
6. 告警通知按投递身份消费：同一身份只落一条通知记录。
"""
from __future__ import annotations

import os
import sys
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.delivery_outbox import (
    FAIL_NEXT_SEND,
    LOSE_NEXT_RECEIPT,
    STATUS_DELIVERED,
    STATUS_FAILED,
    STATUS_IN_FLIGHT,
    STATUS_PENDING,
    DeliveryOutboxService,
    gateway,
)
from app.services.metric_monitor import MetricMonitorService
from app.store import store


def _reset_state() -> None:
    """每个用例前清空链路相关表与模拟网关。"""
    store._tables["dq_incident"] = []
    store._tables["dq_alert_channel"] = []
    store._tables["notification_delivery"] = []
    gateway.reset()


class TestDeliveryConsistency:
    def setup_method(self) -> None:
        _reset_state()
        self.svc = MetricMonitorService()

    def _trigger_incident(self, table: str = "ods_order_amt", field: str = "amount",
                          fault_flags=None) -> dict:
        # 指标当前值带随机波动，重试到命中 critical/warning 阈值为止。
        for _ in range(50):
            result = self.svc.query_metric_values(table, field, 1000, fault_flags=fault_flags)
            if result["告警级别"] in ("critical", "warning"):
                return result
        raise AssertionError("随机数据始终未触发告警阈值")

    def _trigger_with_fault(self, fault_flag: str) -> dict:
        """带故障注入触发波动：随机重试前先清空残留，保证只留下目标事件。"""
        for _ in range(50):
            _reset_state()
            result = self.svc.query_metric_values(
                "ods_order_amt", "amount", 1000, fault_flags={fault_flag})
            if result["告警级别"] in ("critical", "warning"):
                return result
        raise AssertionError("随机数据始终未触发告警阈值")

    def test_incident_persisted_before_notification_with_credential(self):
        """事件落库与凭据先于通知持久化，凭据带全局唯一投递身份。"""
        result = self._trigger_incident()
        incident_id = result["事件编号"]

        incident = self.svc.outbox.find_by_incident(incident_id)
        assert incident is not None
        credential = self.svc.outbox.find_by_delivery(incident["投递身份"])
        assert credential is not None
        assert credential["投递状态"] == STATUS_DELIVERED
        assert credential["通知编号"]  # 回执已回写

        incident_row = next(r for r in store.rows("dq_incident") if r["事件编号"] == incident_id)
        assert incident_row["投递身份"] == credential["投递身份"]
        assert incident_row["处理状态"] == "通知已送达"
        assert incident_row["通知编号"] == credential["通知编号"]
        assert gateway.delivered_count() == 1

    def test_fail_before_send_can_resume_with_single_delivery(self):
        """发送前中止：事件/凭据已落库，恢复续发后只有一条实际通知。"""
        result = self._trigger_with_fault(FAIL_NEXT_SEND)
        incident_id = result["事件编号"]

        credential = self.svc.outbox.find_by_incident(incident_id)
        assert credential["投递状态"] == STATUS_FAILED
        assert credential["投递尝试次数"] == 0
        assert gateway.delivered_count() == 0  # 通知从未离开系统

        # 值班员按原事件编号恢复
        recovery = self.svc.recover_incident_delivery(incident_id)
        assert recovery["ok"] is True
        assert recovery["实际投递"] is True
        assert gateway.delivered_count() == 1

        refreshed = self.svc.outbox.find_by_incident(incident_id)
        assert refreshed["投递状态"] == STATUS_DELIVERED
        assert refreshed["投递尝试次数"] == 1
        assert refreshed["通知编号"]

    def test_delivered_but_receipt_lost_resume_confirms_without_resend(self):
        """送达后丢回执：凭据停在投递中，恢复只确认既有投递，不重复发送。"""
        result = self._trigger_with_fault(LOSE_NEXT_RECEIPT)
        incident_id = result["事件编号"]

        credential = self.svc.outbox.find_by_incident(incident_id)
        assert credential["投递状态"] == STATUS_IN_FLIGHT
        assert credential["投递尝试次数"] == 1
        assert credential["通知编号"]  # 网关已产生通知编号
        assert gateway.delivered_count() == 1  # 实际已送达一条

        recovery = self.svc.recover_incident_delivery(incident_id)
        assert recovery["ok"] is True
        assert recovery["实际投递"] is False  # 不再重复发送
        assert gateway.delivered_count() == 1

        refreshed = self.svc.outbox.find_by_incident(incident_id)
        assert refreshed["投递状态"] == STATUS_DELIVERED
        assert refreshed["通知编号"] == credential["通知编号"]  # 兼容既有回执

    def test_concurrent_recovery_confirms_single_delivery(self):
        """同一次波动两笔恢复并发：只确认一次实际投递，回执为同一份。"""
        # 先制造一个发送前中止、待恢复的事件
        result = self._trigger_with_fault(FAIL_NEXT_SEND)
        incident_id = result["事件编号"]
        assert gateway.delivered_count() == 0

        outcomes: list[dict] = []
        barrier = threading.Barrier(2)

        def recover() -> None:
            barrier.wait()  # 两个值班员同时触发恢复
            outcomes.append(self.svc.recover_incident_delivery(incident_id))

        t1 = threading.Thread(target=recover)
        t2 = threading.Thread(target=recover)
        t1.start(); t2.start()
        t1.join(); t2.join()

        assert len(outcomes) == 2
        assert all(o["ok"] for o in outcomes)
        assert sum(1 for o in outcomes if o["实际投递"]) == 1
        assert gateway.delivered_count() == 1
        # 两笔拿到同一投递身份、同一份通知回执
        assert {o["投递身份"] for o in outcomes} == {outcomes[0]["投递身份"]}
        assert {o["通知编号"] for o in outcomes} == {outcomes[0]["通知编号"]}

    def test_repeat_recovery_after_delivered_reuses_receipt(self):
        """已送达事件再次恢复：复用既有回执，兼容旧通知，不产生新通知。"""
        result = self._trigger_incident()
        incident_id = result["事件编号"]
        first_credential = self.svc.outbox.find_by_incident(incident_id)
        assert gateway.delivered_count() == 1

        again = self.svc.recover_incident_delivery(incident_id)
        assert again["ok"] is True
        assert again["实际投递"] is False
        assert again["通知编号"] == first_credential["通知编号"]
        assert gateway.delivered_count() == 1

    def test_alert_consumed_by_delivery_identity(self):
        """告警通知按投递身份消费异常数据：同一身份只有一条通知记录。"""
        result = self._trigger_incident()
        delivery_id = result["投递身份"]
        alert_rows = [
            a for a in store.rows("dq_alert_channel")
            if a.get("投递身份") == delivery_id
        ]
        assert len(alert_rows) == 1

        # 再恢复两次，通知记录与网关收件箱都不应增加
        self.svc.recover_incident_delivery(result["事件编号"])
        self.svc.recover_incident_delivery(result["事件编号"])
        alert_rows = [
            a for a in store.rows("dq_alert_channel")
            if a.get("投递身份") == delivery_id
        ]
        assert len(alert_rows) == 1
        assert gateway.delivered_count() == 1

    def test_recover_unknown_incident_rejected(self):
        """没有凭据的事件不能凭空恢复。"""
        outcome = self.svc.recover_incident_delivery("DQ_I-4040")
        assert outcome["ok"] is False
        assert gateway.delivered_count() == 0

    def test_dispatch_alert_idempotent_with_delivery_id(self):
        """告警通道服务自身也按投递身份幂等，兼容无身份的旧调用。"""
        from app.services.dq_alert_channel import DqAlertChannelService
        channel = DqAlertChannelService()
        first = channel.dispatch_alert("metric:x.y", "warning", "msg", "DQ_I-1", "DLV-X-1")
        second = channel.dispatch_alert("metric:x.y", "warning", "msg", "DQ_I-1", "DLV-X-1")
        assert first["通知编号"] == second["通知编号"]
        assert second.get("幂等命中") is True
        # 无投递身份的旧链路仍走静默去重，不报错
        legacy = channel.dispatch_alert("legacy:rule", "info", "legacy msg")
        assert "通知编号" in legacy or "通知结果" in legacy


class TestOutboxSnapshot:
    def setup_method(self) -> None:
        _reset_state()

    def test_snapshot_counts_actual_deliveries(self):
        svc = MetricMonitorService()
        for _ in range(50):
            _reset_state()
            r1 = svc.query_metric_values("t1", "f1", 1000, fault_flags={FAIL_NEXT_SEND})
            if r1["告警级别"] not in ("critical", "warning"):
                continue
            r2 = svc.query_metric_values("t2", "f2", 1000)
            if r2["告警级别"] in ("critical", "warning"):
                break
        snapshot = DeliveryOutboxService().domain_snapshot()
        assert snapshot["count"] == 2
        assert snapshot["by_status"][STATUS_FAILED] == 1
        assert snapshot["by_status"][STATUS_DELIVERED] == 1
        assert snapshot["actual_delivered"] == 1
