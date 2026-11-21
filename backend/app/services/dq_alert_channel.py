"""告警通知通道服务：管理告警规则和分发通道，连接巡检、事件和监控模块。

domain_chain: 告警通知 (dq_alert_channel)
  └─ 关联 → 质量事件 (dq_incident)
  └─ 关联 → 指标监控 (metric_monitor)
  └─ 导入 → 巡检任务 (dq_check_run)
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class DqAlertChannelService(BaseModuleService):
    MODULE = "dq_alert_channel"
    ENTITY = "告警配置"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["配置编号", "关联规则集", "通知方式", "接收人", "告警级别", "静默时段", "聚合策略", "通道状态"]
    REQUIRED_FIELDS = ["配置编号", "关联规则集", "通知方式"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用通道": "已停用"}
    NEGATIVE_ACTIONS = ["停用通道"]

    def dispatch_alert(self, rule_set: str, severity: str, message: str,
                        incident_id: str = "",
                        delivery_id: str = "") -> dict[str, Any]:
        """分发告警通知：根据告警级别选择通道和接收人，写入事件记录。

        查询关联的质量事件和巡检任务，聚合告警上下文后导入通知通道。
        根据严重级别决定通知方式（邮件/短信/电话）和接收人。

        告警通知按投递身份（``delivery_id``）消费异常数据：同一投递身份
        只分发一次，重复请求直接返回既有通知回执，兼容没有投递身份的旧调用。
        """
        channel_map = {
            "critical": ("phone+sms", "dq-admin,dq-lead"),
            "warning": ("email+sms", "dq-admin"),
            "info": ("email", "dq-monitor"),
        }
        notify_method, recipients = channel_map.get(severity, ("email", "dq-admin"))

        all_alerts = store.rows(self.MODULE)

        # 投递身份幂等：同一凭据的重复分发直接复用既有回执，不产生第二条通知。
        if delivery_id:
            for alert in all_alerts:
                if alert.get("投递身份") == delivery_id:
                    return {
                        "通知编号": alert["配置编号"],
                        "投递身份": delivery_id,
                        "通知方式": alert["通知方式"],
                        "接收人": alert["接收人"],
                        "告警级别": alert["告警级别"],
                        "消息摘要": message[:100],
                        "发送时间": alert.get("静默时段", ""),
                        "关联事件": incident_id or alert.get("关联事件", ""),
                        "状态": "已发送",
                        "幂等命中": True,
                    }

        # 去重检查：静默时段内不重复发送（兼容无投递身份的既有链路）
        dedup_key = f"{rule_set}:{severity}:{message[:20]}"
        recent_duplicates = [
            a for a in all_alerts
            if a.get("关联规则集") == rule_set
            and a.get("告警级别") == severity
            and a.get("status") == "已生效"
            and not a.get("投递身份")
        ]
        if recent_duplicates:
            return {
                "通知结果": "静默跳过",
                "原因": f"已有{len(recent_duplicates)}条同类告警未处理",
                "关联规则集": rule_set,
            }

        # 写入告警通知记录
        new_id = max((int(r.get("id", 0)) for r in all_alerts), default=0) + 1
        entry = {
            "id": new_id,
            "配置编号": f"DQ_A-{new_id:04d}",
            "投递身份": delivery_id,
            "关联规则集": rule_set,
            "通知方式": notify_method,
            "接收人": recipients,
            "告警级别": severity,
            "静默时段": datetime.now(timezone.utc).isoformat(),
            "聚合策略": "立即" if severity in ("critical", "warning") else "聚合",
            "通道状态": "已发送",
            "关联事件": incident_id,
            "status": "已生效",
            "pending": False,
            "abnormal": severity in ("critical", "warning"),
        }
        all_alerts.append(entry)

        return {
            "通知编号": entry["配置编号"],
            "投递身份": delivery_id,
            "通知方式": notify_method,
            "接收人": recipients,
            "告警级别": severity,
            "消息摘要": message[:100],
            "发送时间": entry["静默时段"],
            "关联事件": incident_id,
            "状态": "已发送",
        }

    def aggregate_alert_summary(self, days: int = 7) -> dict[str, Any]:
        """聚合告警通知摘要：按级别和规则集统计告警分布。

        查询告警通知记录，聚合各维度告警数据，
        计算告警趋势和响应时效，汇总告警统计报告。
        """
        all_alerts = store.rows(self.MODULE)
        by_level = {"critical": 0, "warning": 0, "info": 0}
        by_rule_set: dict[str, int] = {}

        for alert in all_alerts:
            level = alert.get("告警级别", "info")
            by_level[level] = by_level.get(level, 0) + 1
            rule_set = alert.get("关联规则集", "未知")
            by_rule_set[rule_set] = by_rule_set.get(rule_set, 0) + 1

        return {
            "统计周期": f"近{days}天",
            "告警总数": len(all_alerts),
            "按级别": by_level,
            "顶级告警源": dict(
                sorted(by_rule_set.items(), key=lambda x: x[1], reverse=True)[:5]
            ),
            "聚合时间": datetime.now(timezone.utc).isoformat(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "关联"
        snapshot["desc"] = "分发告警通知并关联到质量事件"
        snapshot["artifact"] = "告警通知记录数据集"
        snapshot["artifact_desc"] = "按级别和通道分发的告警通知记录数据集"
        return snapshot