"""指标波动触发与投递恢复路由。

- POST /api/metric-monitor/query  触发一次指标波动检测，超阈值则落事件、登凭据、发通知
- POST /api/delivery/recover/{incident_id}  按原事件恢复投递（并发安全、幂等）
- GET  /api/delivery/by-incident/{incident_id}  查询事件持久化的投递凭据
- GET  /api/delivery/box  投递登记箱清单（处置单可重开的凭据视图）
- GET  /api/delivery/snapshot  凭据状态与网关实际通知数
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel

from app.schemas import PageResult
from app.services.delivery_outbox import (
    FAIL_NEXT_SEND,
    LOSE_NEXT_RECEIPT,
    gateway,
    DeliveryOutboxService,
)
from app.services.metric_monitor import MetricMonitorService

router = APIRouter(prefix="/api", tags=["指标波动与通知投递恢复"])

outbox_service = DeliveryOutboxService()
metric_service = MetricMonitorService()

# 仅用于演练/测试的故障注入开关，生产调用不传 simulate 即为正常投递。
_FAULT_MAP = {
    "fail_before_send": FAIL_NEXT_SEND,
    "lose_receipt": LOSE_NEXT_RECEIPT,
}


class MetricQueryPayload(BaseModel):
    table_name: str
    metric_field: str
    baseline_value: float = 0
    simulate: str | None = None  # fail_before_send / lose_receipt


class RecoverPayload(BaseModel):
    simulate: str | None = None


@router.post("/metric-monitor/query")
def trigger_metric_query(payload: MetricQueryPayload) -> dict[str, Any]:
    """触发指标波动检测；返回结果含事件编号、投递身份与投递状态。"""
    flags = set()
    if payload.simulate:
        flag = _FAULT_MAP.get(payload.simulate)
        if flag:
            flags.add(flag)
    result = metric_service.query_metric_values(
        payload.table_name, payload.metric_field,
        payload.baseline_value, flags or None,
    )
    result["网关实际通知数"] = gateway.delivered_count()
    return result


@router.post("/delivery/recover/{incident_id}")
def recover_delivery(incident_id: str, payload: RecoverPayload | None = None) -> dict[str, Any]:
    """按原事件编号恢复投递。

    同一事件的并发恢复在投递身份锁内串行，只有一笔执行实际投递，
    其余复用同一份回执；已送达的恢复不产生重复通知。
    """
    flags = set()
    if payload and payload.simulate:
        flag = _FAULT_MAP.get(payload.simulate)
        if flag:
            flags.add(flag)
    result = outbox_service.recover_by_incident(incident_id, flags or None)
    result["网关实际通知数"] = gateway.delivered_count()
    return result


@router.get("/delivery/by-incident/{incident_id}")
def delivery_by_incident(incident_id: str) -> dict[str, Any]:
    """返回事件持久化的投递凭据（事件落库后即可查到，哪怕通知尚未发出）。"""
    entry = outbox_service.find_by_incident(incident_id)
    if entry is None:
        return {"ok": False, "message": f"事件 {incident_id} 暂无投递凭据"}
    return {"ok": True, "credential": entry,
            "网关实际通知数": gateway.delivered_count()}


@router.get("/delivery/box", response_model=PageResult[dict])
def delivery_box(page: int = 1, size: int = 20) -> PageResult[dict]:
    """投递登记箱清单：值班员可从这里重开事件处置记录。"""
    rows = list(outbox_service.list_pending())
    total = len(rows)
    start = (page - 1) * size
    return PageResult(items=rows[start:start + size], total=total, page=page, size=size)


@router.get("/delivery/snapshot")
def delivery_snapshot() -> dict[str, Any]:
    snapshot = outbox_service.domain_snapshot()
    snapshot["inbox"] = list(gateway.inbox)
    return snapshot
