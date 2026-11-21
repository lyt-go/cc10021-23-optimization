"""纯 HTTP 端到端演练：经 vite 代理或直连后端，验证投递恢复一致性。

用法：
  python3 -I demo/delivery_recovery_http.py [base_url]
默认 http://127.0.0.1:8000（也可传 http://127.0.0.1:5173 走前端代理）。
"""
from __future__ import annotations

import json
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000").rstrip("/") + "/api"


def call(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def section(title: str) -> None:
    print("\n" + "=" * 64 + f"\n{title}\n" + "=" * 64)


def trigger(fault: str | None, table: str) -> str:
    """触发波动直到产生告警事件，返回事件编号。"""
    for _ in range(60):
        payload = {"table_name": table, "metric_field": "amount",
                   "baseline_value": 1000, "simulate": fault}
        result = call("POST", "/metric-monitor/query", payload)
        if result["告警级别"] in ("critical", "warning"):
            print(f"触发事件 {result['事件编号']} 级别={result['告警级别']} "
                  f"投递状态={result.get('投递状态')} 网关通知数={result['网关实际通知数']}")
            return result["事件编号"]
    raise RuntimeError("未触发告警")


def show(incident_id: str) -> dict:
    data = call("GET", f"/delivery/by-incident/{incident_id}")
    c = data["credential"]
    print(f"凭据 {c['投递身份']} 状态={c['投递状态']} 尝试={c['投递尝试次数']} "
          f"通知={c['通知编号'] or '—'} 网关通知数={data['网关实际通知数']} "
          f"原因={c['失败原因'] or '—'}")
    return c


def main() -> None:
    print("目标接口：", BASE)
    call("GET", "/health")
    base_snapshot = call("GET", "/delivery/snapshot")
    base_delivered = base_snapshot["actual_delivered"]
    base_count = base_snapshot["count"]
    print(f"演练前基线：凭据 {base_count} 条，实际通知 {base_delivered} 条")

    section("场景一：发送前中止 → 刷新处置单 → 恢复续发")
    iid1 = trigger("fail_before_send", "ods_http_fail")
    c1 = show(iid1)
    assert c1["投递状态"] == "发送失败" and c1["投递尝试次数"] == 0
    r1 = call("POST", f"/delivery/recover/{iid1}", {})
    print("恢复:", r1["通知编号"], "实际投递=", r1["实际投递"])
    assert r1["实际投递"] is True
    assert show(iid1)["投递状态"] == "已投递"

    section("场景二：送达后丢回执 → 恢复只确认、不重发")
    iid2 = trigger("lose_receipt", "ods_http_lost")
    c2 = show(iid2)
    assert c2["投递状态"] == "投递中" and c2["投递尝试次数"] == 1
    inbox_before = c2 and call("GET", "/delivery/snapshot")["actual_delivered"]
    r2 = call("POST", f"/delivery/recover/{iid2}", {})
    print("恢复:", r2["通知编号"], "实际投递=", r2["实际投递"])
    assert r2["实际投递"] is False
    inbox_after = call("GET", "/delivery/snapshot")["actual_delivered"]
    assert inbox_after == inbox_before
    assert show(iid2)["通知编号"] == c2["通知编号"]

    section("场景三：同一次波动两笔并发恢复 → 只确认一次实际投递")
    iid3 = trigger("fail_before_send", "ods_http_concurrent")
    before = call("GET", "/delivery/snapshot")["actual_delivered"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        rs = list(pool.map(
            lambda _: call("POST", f"/delivery/recover/{iid3}", {}), range(2)))
    for i, r in enumerate(rs, 1):
        print(f"并发请求{i}: 实际投递={r['实际投递']} 通知={r['通知编号']} 身份={r['投递身份']}")
    assert sum(1 for r in rs if r["实际投递"]) == 1
    assert len({r["通知编号"] for r in rs}) == 1
    assert call("GET", "/delivery/snapshot")["actual_delivered"] == before + 1

    section("汇总：事件凭据与实际收到的通知数")
    snap = call("GET", "/delivery/snapshot")
    print(json.dumps({"count": snap["count"], "by_status": snap["by_status"],
                      "actual_delivered": snap["actual_delivered"]}, ensure_ascii=False))
    assert snap["by_status"]["已投递"] == base_delivered + 3
    assert snap["actual_delivered"] == base_delivered + 3
    assert snap["count"] == base_count + 3
    print("\n全部断言通过：本次 3 事件各 1 条实际通知，并发恢复无重复投递。")


if __name__ == "__main__":
    main()
