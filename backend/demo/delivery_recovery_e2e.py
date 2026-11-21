"""端到端演练：事件登记后的投递恢复（线程内起真实 uvicorn，全部走 HTTP）。

运行：.venv/bin/python -I demo/delivery_recovery_e2e.py

演练内容
--------
1. 发送前中止：事件与凭据已落库、通知数为 0，刷新处置单后提交恢复，通知送达；
2. 送达后丢回执：通知已实际收到但凭据停在"投递中"，恢复只确认不重发；
3. 并发恢复：同一次波动两笔交付请求同时恢复，只确认一次实际投递，回执一致；
4. 查看事件持久化的凭据与网关实际收到的通知数。
"""
from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import uvicorn  # noqa: E402

from app.main import app  # noqa: E402
from app.services.delivery_outbox import (  # noqa: E402
    FAIL_NEXT_SEND,
    LOSE_NEXT_RECEIPT,
    gateway,
)
from app.services.metric_monitor import MetricMonitorService  # noqa: E402
from app.store import store  # noqa: E402


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


PORT = _free_port()
BASE = f"http://127.0.0.1:{PORT}/api"


def http(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def section(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def seed_incident_with_fault(fault: str) -> str:
    """在服务进程内预置一个带故障态的波动事件，返回事件编号。"""
    svc = MetricMonitorService()
    for _ in range(50):
        before = len(store.rows("notification_delivery"))
        result = svc.query_metric_values(
            "ods_order_e2e", "amount", 1000, fault_flags={fault})
        if result["告警级别"] in ("critical", "warning"):
            assert len(store.rows("notification_delivery")) == before + 1
            return result["事件编号"]
    raise RuntimeError("随机数据未触发告警")


def show_credential(incident_id: str) -> dict:
    data = http("GET", f"/delivery/by-incident/{incident_id}")
    cred = data["credential"]
    print(f"事件 {incident_id} 持久化凭据：")
    print(f"  投递身份   : {cred['投递身份']}")
    print(f"  投递状态   : {cred['投递状态']}（尝试 {cred['投递尝试次数']} 次）")
    print(f"  通知编号   : {cred['通知编号'] or '—'}")
    print(f"  回执时间   : {cred['回执时间'] or '—'}")
    print(f"  失败原因   : {cred['失败原因'] or '—'}")
    incident = next(r for r in store.rows("dq_incident") if r["事件编号"] == incident_id)
    print(f"  处置单状态 : {incident['处理状态']} / 回执通知 {incident['通知编号'] or '—'}")
    print(f"  网关实际通知数: {data['网关实际通知数']}")
    return cred


def main() -> None:
    # 干净起点
    store._tables["dq_incident"] = []
    store._tables["dq_alert_channel"] = []
    store._tables["notification_delivery"] = []
    gateway.reset()

    config = uvicorn.Config(app, host="127.0.0.1", port=PORT, log_level="warning")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    for _ in range(50):
        try:
            http("GET", "/health")
            break
        except Exception:
            time.sleep(0.2)

    try:
        # ---- 场景一：发送前中止，恢复后续发 -----------------------------
        section("场景一：发送前中止（事件已落库，通知未发出）")
        incident_fail = seed_incident_with_fault(FAIL_NEXT_SEND)
        show_credential(incident_fail)
        assert gateway.delivered_count() == 0

        print("\n>> 值班员刷新处置单后提交恢复（HTTP POST /delivery/recover）")
        recovery = http("POST", f"/delivery/recover/{incident_fail}", {})
        print("  恢复结果:", json.dumps(recovery, ensure_ascii=False))
        assert recovery["ok"] is True and recovery["实际投递"] is True
        show_credential(incident_fail)
        assert gateway.delivered_count() == 1

        # ---- 场景二：送达后丢回执，恢复只确认 ---------------------------
        section("场景二：送达后丢回执（通知已收到，回执回写中断）")
        incident_lost = seed_incident_with_fault(LOSE_NEXT_RECEIPT)
        show_credential(incident_lost)
        inbox_before = gateway.delivered_count()
        assert inbox_before == 2  # 场景一 1 条 + 本场景 1 条

        print("\n>> 值班员从消息重开处置记录并提交恢复")
        recovery = http("POST", f"/delivery/recover/{incident_lost}", {})
        print("  恢复结果:", json.dumps(recovery, ensure_ascii=False))
        assert recovery["ok"] is True and recovery["实际投递"] is False
        assert gateway.delivered_count() == inbox_before  # 没有重复通知
        show_credential(incident_lost)

        # ---- 场景三：并发恢复，只确认一次实际投递 -----------------------
        section("场景三：两笔交付请求并发恢复同一事件")
        incident_concurrent = seed_incident_with_fault(FAIL_NEXT_SEND)
        show_credential(incident_concurrent)
        before = gateway.delivered_count()

        print("\n>> 两位值班员同时触发恢复（并发 HTTP 请求）")
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(http, "POST", f"/delivery/recover/{incident_concurrent}", {}),
                pool.submit(http, "POST", f"/delivery/recover/{incident_concurrent}", {}),
            ]
            results = [f.result() for f in futures]
        for idx, item in enumerate(results, 1):
            print(f"  请求{idx}: 实际投递={item['实际投递']} 通知编号={item['通知编号']} "
                  f"投递身份={item['投递身份']}")

        assert sum(1 for r in results if r["实际投递"]) == 1
        assert len({r["通知编号"] for r in results}) == 1
        assert gateway.delivered_count() == before + 1
        show_credential(incident_concurrent)

        # ---- 汇总：事件凭据 vs 实际通知数 -------------------------------
        section("汇总：持久化凭据与实际收到的通知数")
        snapshot = http("GET", "/delivery/snapshot")
        print(json.dumps(snapshot, ensure_ascii=False, indent=2))
        assert snapshot["count"] == 3
        assert snapshot["by_status"]["已投递"] == 3
        assert snapshot["actual_delivered"] == 3
        print("\n全部断言通过：3 个事件各 1 条实际通知，并发恢复未产生重复投递。")
    finally:
        server.should_exit = True
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
