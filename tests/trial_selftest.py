#!/usr/bin/env python3
"""批量试投端到端自测：驱动 /api/trial，覆盖混合应答、正文暂拒、末尾断连。

另外用一条原始 TCP 连接直连首选中继的 SMTP 口，手工验证：
- PIPELINING 一次 write 连发 MAIL/RCPT.../DATA，对端逐条应答；
- 多行 RCPT 应答跨 TCP 分段到达，必须整段收齐并按命令顺序配对。

用法：先启动两个中继和探测服务，然后
    python3 tests/trial_selftest.py [http://127.0.0.1:8000]
只用标准库。
"""
import json
import socket
import sys
import time
import urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SMTP_HOST, SMTP_PORT = "127.0.0.1", 2525
FAILED = []

ADDRS = [
    "alerts@sample.test",
    "oncall@sample.test",
    "ops@multiline.test",
    "deferred@sample.test",
    "deadaddr@sample.test",
]


def post(path, body):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read())


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILED.append((name, detail))


def scenario(title, body):
    print(f"\n== {title} ==")
    print(f"   参数: {json.dumps(body, ensure_ascii=False)}")
    r = post("/api/trial", body)
    print(f"   outcome={r['outcome']} 耗时={r['duration']}s "
          f"可重投={r['resendable']}")
    for rc in r["recipients"]:
        print(f"     - {rc['address']}: RCPT {rc['rcpt_code']}"
              f"({'多行' if rc['rcpt_multiline'] else '单行'})"
              f" -> 最终 {rc['final_status']} (final_code={rc['final_code']})")
    return r


def policy(*pols):
    return dict(zip(ADDRS, pols))


# Ⓐ 收件人混合应答：3 接收（含分段多行）、1 暂拒、1 永久拒收；正文回 250
r = scenario("Ⓐ 收件人混合应答 + 正文 250", {
    "rcpt_policy": policy("accept", "accept", "accept", "tempfail", "reject"),
    "data_final": "accept", "timeout": 10})
check("outcome=final_250", r["outcome"] == "final_250", r["outcome"])
check("DATA 收到 354 才发正文",
      r["data_reply"] and r["data_reply"]["code"] == 354, str(r["data_reply"]))
check("最终应答 250",
      r["final_reply"] and r["final_reply"]["code"] == 250)
check("PIPELINING 标记为真且 EHLO 声明能力",
      r["pipelining"] is True and r.get("ehlo_pipelining") is True)
rcpts = {x["address"]: x for x in r["recipients"]}
check("结果顺序与 RCPT TO 原顺序一致",
      [x["address"] for x in r["recipients"]] == ADDRS)
check("三个接收地址 RCPT=250 且最终 accepted",
      all(rcpts[a]["rcpt_code"] == 250 and rcpts[a]["final_status"] == "accepted"
          for a in ADDRS[:3]), str(rcpts))
check("分段多行样例 RCPT 为多行应答且码 250",
      rcpts["ops@multiline.test"]["rcpt_multiline"] is True
      and rcpts["ops@multiline.test"]["rcpt_code"] == 250)
check("暂拒地址 RCPT=451 并保留 tempfail 结论",
      rcpts["deferred@sample.test"]["rcpt_code"] == 451
      and rcpts["deferred@sample.test"]["final_status"] == "tempfail")
check("永久拒收地址 RCPT=550 并保留 reject 结论",
      rcpts["deadaddr@sample.test"]["rcpt_code"] == 550
      and rcpts["deadaddr@sample.test"]["final_status"] == "reject")
check("可重投名单只含明确暂拒地址",
      r["resendable"] == ["deferred@sample.test"], str(r["resendable"]))
check("仅暂拒条目标记 resendable",
      [x["address"] for x in r["recipients"] if x["resendable"]]
      == ["deferred@sample.test"])

# Ⓑ 全部 RCPT 接收，正文最终回 451：通过 RCPT 的地址全部转为暂拒
r = scenario("Ⓑ 全部接收 + 正文 451 暂拒", {
    "rcpt_policy": policy("accept", "accept", "accept", "accept", "accept"),
    "data_final": "tempfail", "timeout": 10})
check("outcome=final_tempfail", r["outcome"] == "final_tempfail", r["outcome"])
check("最终应答 451",
      r["final_reply"] and r["final_reply"]["code"] == 451)
check("通过 RCPT 的地址全部以正文应答为准 -> tempfail",
      all(x["rcpt_code"] == 250 and x["final_status"] == "tempfail"
          for x in r["recipients"]), str(r["recipients"]))
# 需求约定：可重投名单只列"明确暂拒"（RCPT 451）的地址；
# 正文级 451 不属于明确的 RCPT 暂拒，不进可重投名单。
check("正文级暂拒不进可重投名单（只认明确暂拒）",
      r["resendable"] == [], str(r["resendable"]))

# Ⓒ 全部接收，正文发完后半条应答断连：通过 RCPT 的地址标待核
r = scenario("Ⓒ 全部接收 + 末尾断连（待核）", {
    "rcpt_policy": policy("accept", "accept", "accept", "accept", "accept"),
    "data_final": "half_close", "timeout": 10})
check("outcome=final_incomplete", r["outcome"] == "final_incomplete", r["outcome"])
check("最终应答码为空且留有半条残行",
      r["final_reply"] and r["final_reply"]["code"] is None
      and r["final_reply"]["partial"], str(r.get("final_reply")))
check("通过 RCPT 的地址全部标 unverified（待核）",
      all(x["rcpt_code"] == 250 and x["final_status"] == "unverified"
          for x in r["recipients"]), str(r["recipients"]))
check("待核场景可重投名单为空", r["resendable"] == [], str(r["resendable"]))

# Ⓓ 混合策略 + 末尾断连：被拒/暂拒的保留原结论，通过的才待核
r = scenario("Ⓓ 混合策略 + 末尾断连", {
    "rcpt_policy": policy("accept", "accept", "accept", "tempfail", "reject"),
    "data_final": "half_close", "timeout": 10})
rcpts = {x["address"]: x for x in r["recipients"]}
check("接收的 3 个地址待核",
      all(rcpts[a]["final_status"] == "unverified" for a in ADDRS[:3]))
check("暂拒地址保留 tempfail 且仍可重投",
      rcpts["deferred@sample.test"]["final_status"] == "tempfail"
      and rcpts["deferred@sample.test"]["resendable"] is True)
check("拒收地址保留 reject",
      rcpts["deadaddr@sample.test"]["final_status"] == "reject")
check("可重投仍只有明确暂拒地址",
      r["resendable"] == ["deferred@sample.test"], str(r["resendable"]))

# ── 原始 TCP：验证 PIPELINING 一次连发 + 分段多行 RCPT 按序配对 ──
print("\n== Ⓔ 原始 socket 直连：PIPELINING 与跨分段多行 RCPT ==")
# 先通过控制口把 relay-a 配成混合策略 + 正文 250
cfg_req = urllib.request.Request(
    "http://127.0.0.1:8081/config",
    data=json.dumps({
        "rcpt_policy": policy("accept", "accept", "accept",
                              "tempfail", "reject"),
        "data_final": "accept",
    }).encode(),
    headers={"Content-Type": "application/json"}, method="POST")
with urllib.request.urlopen(cfg_req, timeout=10) as resp:
    json.loads(resp.read())

sock = socket.create_connection((SMTP_HOST, SMTP_PORT), timeout=10)
sock.settimeout(5)
buf = bytearray()
seg_events = []  # 每次 recv 收到的字节数，用来核对跨分段


def fill_byte():
    """逐字节 recv：每个 TCP 分段第一次 recv 即返回，天然暴露分段边界。"""
    chunk = sock.recv(1)
    if not chunk:
        raise EOFError("连接已关闭，缓冲残留: " + repr(bytes(buf)))
    seg_events.append(len(chunk))
    buf.extend(chunk)


def read_reply():
    """从全局缓冲按 SMTP 规则读完整一段应答（多行读到 '码+空格'），
    数据按实际 TCP 分段逐字节到达，本函数负责跨行、跨分段重组。"""
    lines = []
    while True:
        line = bytearray()
        while True:
            if not buf:
                fill_byte()
            b = buf.pop(0)
            if b == 0x0A and line and line[-1] == 0x0D:
                line.pop()
                break
            line.append(b)
        text = line.decode()
        lines.append(text)
        if len(text) >= 4 and text[3] == " ":
            return int(text[:3]), lines


def drain_waves_for_multiline():
    """持续读到多行应答终行进缓冲，期间统计独立 recv 次数（分段波次）。"""
    start_events = len(seg_events)
    while True:
        text = bytes(buf).decode("utf-8", "replace")
        # 出现"三位码+空格"终行即整段应答已到齐
        if any(len(l) >= 4 and l[:3].isdigit() and l[3] == " "
               for l in text.split("\r\n")):
            break
        chunk = sock.recv(4096)
        if not chunk:
            raise EOFError("等多行应答时连接关闭")
        seg_events.append(len(chunk))
        buf.extend(chunk)
    return len(seg_events) - start_events


# 问候：逐字节读，记录 recv 波次（中继把问候拆 4 段、段间 50ms）
code, greet_lines = read_reply()
greet_recvs = len(seg_events)
check("问候 220 且为多行", code == 220 and len(greet_lines) >= 3, str(greet_lines))
check("问候数据跨多个 TCP 分段到达（recv 次数>行数）",
      greet_recvs > len(greet_lines), f"recvs={greet_recvs}")

sock.sendall(b"EHLO raw.test\r\n")
code, _ = read_reply()
check("EHLO 回 250 且声明 PIPELINING", code == 250)

# 一次 sendall 连发全部命令（真正的 PIPELINING）
pipeline = b"MAIL FROM:<probe@raw.test>\r\n" + b"".join(
    f"RCPT TO:<{a}>\r\n".encode() for a in ADDRS) + b"DATA\r\n"
sock.sendall(pipeline)

code, _ = read_reply()
check("流水线第 1 个应答（MAIL FROM）=250", code == 250)
expect = [250, 250, 250, 451, 550]
got = []
multiline_recvs = 0
for i, addr in enumerate(ADDRS):
    if addr == "ops@multiline.test":
        # 先把缓冲排空，再统计这一段多行应答经历的 recv 波次
        multiline_recvs = drain_waves_for_multiline()
    code, lines = read_reply()
    got.append(code)
    if addr == "ops@multiline.test":
        check("该 RCPT 应答为多行（250- 续行）且整段收齐",
              code == 250 and len(lines) >= 3, str(lines))
check("RCPT 应答按原命令顺序配对 250/250/250/451/550", got == expect, str(got))
check("多行 RCPT 应答跨 TCP 分段到达（多次 recv 拼齐）",
      multiline_recvs >= 2, f"recvs={multiline_recvs}")
code, _ = read_reply()
check("DATA 应答为 354", code == 354)
sock.sendall(b"Subject: raw pipeline test\r\n\r\nbody line\r\n.\r\n")
code, last = read_reply()
check("正文最终应答 250", code == 250, str(last))
sock.sendall(b"QUIT\r\n")
code, _ = read_reply()
check("QUIT 回 221", code == 221)
sock.close()

# 中继侧计数核对：accepted 与 closed 收敛
print("\n== 中继侧连接计数（轮询等待收敛）==")
with urllib.request.urlopen("http://127.0.0.1:8000/api/relay-state",
                            timeout=10) as resp:
    states0 = json.loads(resp.read())
base_accepted = states0["a"]["stats"]["accepted"]
settled = None
for _ in range(40):
    time.sleep(0.5)
    with urllib.request.urlopen("http://127.0.0.1:8000/api/relay-state",
                                timeout=10) as resp:
        settled = json.loads(resp.read())
    s = settled["a"]["stats"]
    if s["accepted"] == s["closed"]:
        break
s = settled["a"]["stats"]
print(f"   relay-a: {s}")
check("relay-a accepted==closed（无连接泄漏）",
      s["accepted"] == s["closed"], str(s))
check("累计发生过半条断连", s["half_close"] >= 2, str(s["half_close"]))
check("累计多行 RCPT 应答数与试投次数匹配（>=4）",
      s["multiline_rcpt"] >= 4, str(s["multiline_rcpt"]))
check("DATA 计数与成功进入正文的会话数匹配（>=4）",
      s["data_sessions"] >= 4, str(s["data_sessions"]))

print("\n" + ("全部通过 ✔" if not FAILED else f"有 {len(FAILED)} 项失败: {FAILED}"))
sys.exit(1 if FAILED else 0)
