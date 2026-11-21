"""指标波动监控服务：查询指标当前值与基线对比，检测波动幅度并触发告警通知。

domain_chain: 指标波动检测 (metric_monitor)
  └─ 查询 → 质量事件 (dq_incident)
  └─ 登记凭据 → 通知投递箱 (notification_delivery)
  └─ 按投递身份发送 → 告警通知 (dq_alert_channel)
  └─ 校验 → 质量评分卡 (dq_scorecard)

一致性约定
----------
波动超阈值时按顺序执行：
1. 质量事件落库（拿到事件编号）；
2. 以该事件为关联键登记通知投递凭据，凭据带全局唯一投递身份；
3. 按投递身份幂等发送，回执回写投递箱与质量事件处置清单。
事件与凭据先于任何外部通知落库，即使发送中断，也能凭原事件编号恢复投递。
"""

from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.delivery_outbox import (
    FAIL_NEXT_SEND,
    LOSE_NEXT_RECEIPT,
    DeliveryOutboxService,
)
from app.store import store


class MetricMonitorService(BaseModuleService):
    MODULE = "metric_monitor"
    ENTITY = "监控指标"
    OPERATION_TERM = "查询"
    LIST_FIELDS = ["指标编号", "目标表名", "监控字段", "基线值", "当前值", "波动幅度", "告警阈值", "监控周期"]
    REQUIRED_FIELDS = ["指标编号", "目标表名", "监控字段"]
    STATUS_ORDER = ["草稿", "待校验", "监控中", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "启动监控": "监控中", "停用监控": "已停用"}
    NEGATIVE_ACTIONS = ["停用监控"]

    def __init__(self) -> None:
        self.outbox = DeliveryOutboxService()

    def query_metric_values(self, table_name: str, metric_field: str,
                            baseline_value: float = 0,
                            fault_flags: set[str] | None = None) -> dict[str, Any]:
        """查询监控字段当前值并计算波动幅度。

        查询目标表中监控字段的当前统计值，与基线值对比计算波动幅度。
        若波动超过告警阈值，先写入质量事件并登记投递凭据，再按投递身份
        触发告警通知；``fault_flags`` 仅用于演练发送前中止/回执丢失。
        """
        # 查询当前值（模拟数据查询）
        current_value = baseline_value * random.uniform(0.5, 2.0) if baseline_value > 0 else random.uniform(100, 10000)
        if baseline_value == 0:
            baseline_value = current_value * 0.9

        volatility = abs(current_value - baseline_value) / baseline_value if baseline_value > 0 else 0
        volatility_pct = round(volatility * 100, 2)

        # 告警阈值判断
        thresholds = {
            "critical": 0.30,  # 30% 波动为严重
            "warning": 0.15,   # 15% 波动为告警
            "info": 0.05,      # 5% 波动为关注
        }
        alert_level = "normal"
        if volatility >= thresholds["critical"]:
            alert_level = "critical"
        elif volatility >= thresholds["warning"]:
            alert_level = "warning"
        elif volatility >= thresholds["info"]:
            alert_level = "info"

        result = {
            "目标表名": table_name,
            "监控字段": metric_field,
            "基线值": round(baseline_value, 4),
            "当前值": round(current_value, 4),
            "波动幅度": f"{volatility_pct}%",
            "告警级别": alert_level,
            "查询时间": datetime.now(timezone.utc).isoformat(),
        }

        # 若超过严重阈值：事件落库 → 登记投递凭据 → 按投递身份发送
        if alert_level in ("critical", "warning"):
            incident = self._create_quality_incident(table_name, metric_field, result)
            delivery = self._register_and_deliver(
                table_name, metric_field, alert_level, volatility_pct,
                incident["事件编号"], fault_flags,
            )
            result["事件编号"] = incident["事件编号"]
            result["投递身份"] = delivery.get("投递身份", "")
            result["投递状态"] = delivery.get("投递状态", "")
            result["通知编号"] = delivery.get("通知编号", "")

        return result

    def _create_quality_incident(self, table_name: str, metric_field: str,
                                  metric_result: dict[str, Any]) -> dict[str, Any]:
        """写入质量事件记录，将指标异常事件关联到质量事件管理。"""
        incident_rows = store.rows("dq_incident")
        new_iid = max((int(r.get("id", 0)) for r in incident_rows), default=0) + 1
        incident = {
            "id": new_iid,
            "事件编号": f"DQ_I-{new_iid:04d}",
            "关联规则": f"metric_monitor:{table_name}.{metric_field}",
            "触发时间": datetime.now(timezone.utc).isoformat(),
            "偏离程度": metric_result["波动幅度"],
            "影响行数": str(random.randint(1000, 500000)),
            "严重级别": metric_result.get("告警级别", "warning"),
            "处理状态": "通知待投递",
            "处理人": "auto",
            "status": "待确认",
            "pending": True,
            "abnormal": True,
        }
        incident_rows.append(incident)
        return incident

    def _register_and_deliver(self, table_name: str, field_name: str, level: str,
                               volatility_pct: float, incident_id: str,
                               fault_flags: set[str] | None) -> dict[str, Any]:
        """登记投递凭据并按投递身份发送；中断后由恢复接口按事件编号续发。"""
        channel_map = {
            "critical": ("phone+sms", "dq-admin,dq-lead"),
            "warning": ("email+sms", "dq-admin"),
        }
        notify_method, recipients = channel_map.get(level, ("email", "dq-admin"))
        rule_set = f"metric:{table_name}.{field_name}"
        message = f"指标 {table_name}.{field_name} 波动 {volatility_pct}% 超过告警阈值"

        # 事件落库后立即登记凭据：即使下一步发送中止，凭据也已持久化可恢复。
        credential = self.outbox.register_delivery(
            incident_id=incident_id,
            rule_set=rule_set,
            severity=level,
            message=message,
            notify_method=notify_method,
            recipients=recipients,
            table_name=table_name,
            field_name=field_name,
        )
        receipt = self.outbox.send_with_receipt(credential["投递身份"], fault_flags)
        return receipt

    def recover_incident_delivery(self, incident_id: str,
                                   fault_flags: set[str] | None = None) -> dict[str, Any]:
        """按原事件编号恢复投递：并发恢复共用同一投递身份，只确认一次实际投递。"""
        return self.outbox.recover_by_incident(incident_id, fault_flags)

    def aggregate_metric_trends(self, table_name: str, days: int = 7) -> list[dict[str, Any]]:
        """聚合多日指标查询结果，生成趋势分析数据。

        查询连续N天的监控指标，聚合波动趋势并关联到趋势看板。
        """
        trends = []
        base_value = random.uniform(1000, 50000)
        for day_offset in range(days, 0, -1):
            daily_value = base_value * random.uniform(0.7, 1.3)
            volatility = abs(daily_value - base_value) / base_value * 100
            trends.append({
                "日期": f"2026-11-{18 - day_offset:02d}",
                "表名": table_name,
                "指标值": round(daily_value, 2),
                "波动率": f"{volatility:.2f}%",
                "趋势": "上升" if daily_value > base_value else "下降",
            })

        # 聚合趋势写入趋势记录
        trend_rows = store.rows("dq_trend")
        for t in trends:
            new_tid = max((int(r.get("id", 0)) for r in trend_rows), default=0) + 1
            trend_rows.append({
                "id": new_tid,
                "记录编号": f"DQ_T-{new_tid:04d}",
                "目标表名": table_name,
                "日期": t["日期"],
                "完备率": f"{random.uniform(85, 100):.1f}%",
                "准确率": f"{random.uniform(80, 99):.1f}%",
                "及时率": f"{random.uniform(90, 100):.1f}%",
                "综合质量": f"{random.uniform(85, 98):.1f}",
                "趋势标记": t["趋势"],
                "status": "正常",
                "pending": False,
                "abnormal": False,
            })

        return trends

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["operation"] = "查询"
        snapshot["desc"] = "查询监控指标并对比基线检测波动"
        snapshot["artifact"] = "指标波动分析报告"
        snapshot["artifact_desc"] = "查询并聚合后的指标波动分析报告"
        return snapshot
