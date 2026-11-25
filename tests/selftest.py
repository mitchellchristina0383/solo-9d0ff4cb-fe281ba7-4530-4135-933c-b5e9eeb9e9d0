#!/usr/bin/env python3
"""端到端自测：驱动探测 API，覆盖并行拨号、提前失败、总时限等场景。

用法：先启动两个中继和探测服务，然后
    python3 tests/selftest.py [http://127.0.0.1:8000]
只用标准库。
"""
import json
import sys
import time
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
FAILED = []


def probe(body):
    req = urllib.request.Request(
        BASE + "/api/probe",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read())


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append((name, detail))


def evts(r):
    return " | ".join(f"{e['t']}:{e['msg']}" for e in r["events"])


def scenario(title, body):
    print(f"\n== {title} ==")
    print(f"   参数: {json.dumps(body, ensure_ascii=False)}")
    return probe(body)


# ① 正常：首选问候快，备用不应起拨
r = scenario("① 正常（首选胜出）", {
    "relay_a": {"greeting_delay": 0.2, "response_delay": 0.1},
    "relay_b": {"greeting_delay": 0.2, "response_delay": 0.1},
    "backup_wait": 1.0, "total_timeout": 5.0})
check("结果为 pass", r["outcome"] == "pass", evts(r))
check("首选收齐问候", r["relays"]["a"]["greeting"]["ok"] is True)
check("问候为多行(>=3行)", len(r["relays"]["a"]["greeting"]["lines"]) >= 3,
      str(r["relays"]["a"]["greeting"]))
check("备用未起拨", r["relays"]["b"]["dialed"] is False)
check("NOOP 沿首选发出且 250", r["noop"]["sent"] and r["noop"]["relay"] == "a"
      and r["noop"]["ok"] and r["noop"]["code"] == 250, str(r["noop"]))
check("首选连接已释放", r["relays"]["a"]["closed"] is True)

# ② 首选问候慢：等待期满并行起拨备用，备用胜出，首选被取消并关闭
r = scenario("② 首选问候慢 → 并行拨备用", {
    "relay_a": {"greeting_delay": 2.5, "response_delay": 0.1},
    "relay_b": {"greeting_delay": 0.3, "response_delay": 0.1},
    "backup_wait": 1.0, "total_timeout": 6.0})
check("结果为 pass", r["outcome"] == "pass", evts(r))
check("两路都拨号", r["relays"]["a"]["dialed"] and r["relays"]["b"]["dialed"])
check("备用在等待期满后起拨(≈1.0s)",
      abs(r["relays"]["b"]["dial_at"] - 1.0) < 0.4,
      f"dial_at={r['relays']['b']['dial_at']}")
check("备用胜出", r["noop"]["relay"] == "b" and r["noop"]["ok"], str(r["noop"]))
check("首选被取消且连接关闭", r["relays"]["a"]["cancelled"] is True
      and r["relays"]["a"]["closed"] is True, str(r["relays"]["a"]))

# ③ 首选建连即 RST：立即起拨备用（不等到备用等待期满）
r = scenario("③ 首选复位 → 立即拨备用", {
    "relay_a": {"greeting_delay": 0.2, "response_delay": 0.1, "reset": True},
    "relay_b": {"greeting_delay": 0.3, "response_delay": 0.1},
    "backup_wait": 5.0, "total_timeout": 6.0})
check("结果为 pass", r["outcome"] == "pass", evts(r))
check("首选问候失败", r["relays"]["a"]["greeting"]["ok"] is False,
      str(r["relays"]["a"]))
check("备用立即起拨(远早于 5s 等待期)",
      r["relays"]["b"]["dial_at"] < 1.0,
      f"dial_at={r['relays']['b']['dial_at']}")
check("备用胜出且 250", r["noop"]["relay"] == "b" and r["noop"]["ok"])

# ④ 两路问候都慢：到达总时限停止，连接全部释放
r = scenario("④ 两路都慢 → 总时限停止", {
    "relay_a": {"greeting_delay": 10.0, "response_delay": 0.1},
    "relay_b": {"greeting_delay": 10.0, "response_delay": 0.1},
    "backup_wait": 1.0, "total_timeout": 3.0})
check("结果为 timeout", r["outcome"] == "timeout", evts(r))
check("总耗时≈总时限(3s)", abs(r["duration"] - 3.0) < 0.6,
      f"duration={r['duration']}")
check("两路均未收齐问候",
      not (r["relays"]["a"]["greeting"] and r["relays"]["a"]["greeting"]["ok"])
      and not (r["relays"]["b"]["greeting"] and r["relays"]["b"]["greeting"]["ok"]))
check("NOOP 未发送", r["noop"]["sent"] is False)
check("两路连接均已释放", r["relays"]["a"]["closed"] and r["relays"]["b"]["closed"],
      f"a={r['relays']['a']['closed']} b={r['relays']['b']['closed']}")

# ⑤ 问候快但 NOOP 应答超过总时限
r = scenario("⑤ NOOP 应答超时限", {
    "relay_a": {"greeting_delay": 0.2, "response_delay": 3.0},
    "relay_b": {"greeting_delay": 0.2, "response_delay": 0.1},
    "backup_wait": 2.0, "total_timeout": 1.5})
check("结果为 noop_failed", r["outcome"] == "noop_failed", evts(r))
check("NOOP 只发一次且沿首选", r["noop"]["sent"] and r["noop"]["relay"] == "a",
      str(r["noop"]))
check("首选连接已释放", r["relays"]["a"]["closed"] is True)

# ⑥ 中继侧计数核对：accepted 与 closed 最终相等（无泄漏）。
# 探测端关闭连接后，中继侧会话协程察觉 EOF/RST 需要一点时间
# （场景④的中继还在 greeting_delay 里睡眠），这里轮询等待其收敛。
def relay_states():
    with urllib.request.urlopen(BASE + "/api/relay-state", timeout=10) as resp:
        return json.loads(resp.read())


print("\n== 中继侧连接计数（轮询等待收敛）==")
settled = {}
for _ in range(40):  # 最多等约 20s
    time.sleep(0.5)
    states = relay_states()
    ok = True
    for key in ("a", "b"):
        s = (states.get(key) or {}).get("stats")
        if not s or s["accepted"] != s["closed"]:
            ok = False
    settled = states
    if ok:
        break
for key, label in (("a", "relay-a"), ("b", "relay-b")):
    s = (settled.get(key) or {}).get("stats")
    print(f"   {label}: {s}")
    check(f"{label} accepted==closed", s is not None and s["accepted"] == s["closed"],
          str(s))

print("\n" + ("全部通过 ✔" if not FAILED else f"有 {len(FAILED)} 项失败: {FAILED}"))
sys.exit(1 if FAILED else 0)
