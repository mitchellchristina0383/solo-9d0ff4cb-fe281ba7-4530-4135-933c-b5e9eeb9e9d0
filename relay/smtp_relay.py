"""SMTP 测试中继。

- SMTP 端口：模拟 220 问候（多行、分段发送），支持完整 EHLO/MAIL/RCPT/DATA
  事务（含 PIPELINING 连发）与 NOOP/RSET/QUIT。
- 控制端口：极简 HTTP，POST /config 动态调整行为，GET /state 查看配置与计数。

可配项（POST /config，JSON）：
  greeting_delay  发送问候前的等待秒数
  response_delay  每条命令应答前的等待秒数
  reset           true 时建连后立即以 RST 断开（模拟对端失败）
  rcpt_policy     本地样例地址 -> accept（250）/ tempfail（451 暂拒）/
                  reject（550 永久拒收）；未知地址一律 550
  data_final      正文收齐后的最终应答：accept（250）/ tempfail（451）/
                  half_close（发出半条应答后断连，模拟最终应答不完整）

约定：样例地址 ops@multiline.test 的 RCPT 应答固定为跨 TCP 分段发送的
多行 250 应答，用于检验探测端按行收齐、按命令顺序配对。
"""
import asyncio
import json
import os
import socket
import struct
import time

NAME = os.environ.get("RELAY_NAME", "relay")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "2525"))
CTRL_PORT = int(os.environ.get("CTRL_PORT", "8081"))

# 本地样例地址（探测页与自测共用这一份清单）
SAMPLE_ADDRESSES = [
    "alerts@sample.test",
    "oncall@sample.test",
    "ops@multiline.test",      # RCPT 应答走分段多行 250
    "deferred@sample.test",
    "deadaddr@sample.test",
]
POLICIES = ("accept", "tempfail", "reject")
DATA_FINALS = ("accept", "tempfail", "half_close")
MULTILINE_RCPT = "ops@multiline.test"

DEFAULT_RCPT_POLICY = {
    "alerts@sample.test": "accept",
    "oncall@sample.test": "accept",
    "ops@multiline.test": "accept",
    "deferred@sample.test": "tempfail",
    "deadaddr@sample.test": "reject",
}

STATE = {
    "greeting_delay": 0.2,
    "response_delay": 0.1,
    "reset": False,
    "rcpt_policy": dict(DEFAULT_RCPT_POLICY),
    "data_final": "accept",
}
STATS = {
    "accepted": 0,        # 接受的 TCP 连接数
    "reset": 0,           # 主动 RST 的次数
    "greeted": 0,         # 完整发出问候的次数
    "noop": 0,            # 收到 NOOP 的次数
    "mail_from": 0,       # 收到 MAIL FROM 的次数
    "rcpt_total": 0,      # 收到 RCPT TO 的总次数
    "rcpt_accept": 0,     # RCPT 回 250 的次数
    "rcpt_tempfail": 0,   # RCPT 回 4xx 的次数
    "rcpt_reject": 0,     # RCPT 回 5xx 的次数
    "multiline_rcpt": 0,  # 分段多行 RCPT 应答次数
    "data_sessions": 0,   # 进入 DATA 正文阶段的次数
    "final_250": 0,       # 正文最终回 250 的次数
    "final_451": 0,       # 正文最终回 451 的次数
    "half_close": 0,      # 正文末尾半条应答后断连的次数
    "closed": 0,          # 会话结束（连接释放）次数
}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [{NAME}] {msg}", flush=True)


def _reset_connection(writer: asyncio.StreamWriter) -> None:
    """SO_LINGER(1,0) + close，让对端收到 RST，模拟对端失败/中途断连。"""
    sock = writer.get_extra_info("socket")
    if sock is not None:
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
        except OSError:
            pass
    writer.close()


async def _delay() -> None:
    d = float(STATE.get("response_delay", 0.0))
    if d > 0:
        await asyncio.sleep(d)


async def _write_chunked_multiline(writer: asyncio.StreamWriter,
                                   payload: bytes) -> None:
    """把多行应答拆成多个 TCP 分段、段间留小间隔发送。"""
    n = len(payload)
    bounds = [0, n // 3, (2 * n) // 3, n]
    for a, b in zip(bounds, bounds[1:]):
        writer.write(payload[a:b])
        await writer.drain()
        await asyncio.sleep(0.05)


def _extract_address(cmd: str) -> str:
    """从 'RCPT TO:<a@b>' / 'MAIL FROM:<a@b>' 里取地址，小写化。"""
    if "<" in cmd and ">" in cmd:
        addr = cmd[cmd.index("<") + 1:cmd.index(">")]
    else:
        addr = cmd.split(":", 1)[1] if ":" in cmd else ""
    return addr.strip().lower()


async def _send_rcpt_reply(writer: asyncio.StreamWriter,
                           addr: str, policy: str) -> int:
    """按地址策略回 RCPT 应答；multiline 样例行跨 TCP 分段发多行 250。"""
    STATS["rcpt_total"] += 1
    if addr == MULTILINE_RCPT and policy == "accept":
        STATS["multiline_rcpt"] += 1
        payload = (
            f"250-{NAME}.test accepted (multiline)\r\n"
            f"250-recipient {addr}\r\n"
            f"250 OK\r\n"
        ).encode()
        log(f"RCPT {addr}: 分段发送多行 250 应答（{len(payload)} 字节）")
        await _write_chunked_multiline(writer, payload)
        STATS["rcpt_accept"] += 1
        return 250
    if policy == "accept":
        writer.write(f"250 {addr} accepted\r\n".encode())
        STATS["rcpt_accept"] += 1
        return 250
    if policy == "tempfail":
        writer.write(
            f"451 {addr} temporarily unavailable, try again later\r\n".encode())
        STATS["rcpt_tempfail"] += 1
        return 451
    # 永久拒收（含未知地址，未知地址由调用方统一走 550）
    writer.write(f"550 {addr} mailbox unavailable\r\n".encode())
    STATS["rcpt_reject"] += 1
    return 550


async def _read_data_body(reader: asyncio.StreamReader) -> None:
    """读 DATA 正文直到仅含 '.' 的结束行（结束行为 \r\n.\r\n，可能裸 \n）。"""
    while True:
        line = await reader.readline()
        if not line:
            raise EOFError("正文读取过程中对端关闭了连接")
        text = line.decode("utf-8", "replace")
        if text in (".\r\n", ".\n", "."):
            return
        # 点填塞行（"..x"）按 SMTP 规则还原为 ".x"，这里只需正确识别结束行


async def smtp_session(reader: asyncio.StreamReader,
                       writer: asyncio.StreamWriter) -> None:
    peer = writer.get_extra_info("peername")
    STATS["accepted"] += 1
    log(f"接受连接 {peer}")
    rcpt_count = 0
    try:
        if STATE.get("reset"):
            STATS["reset"] += 1
            log("按配置立即复位连接 (RST)")
            _reset_connection(writer)
            return

        delay = float(STATE.get("greeting_delay", 0.0))
        if delay > 0:
            await asyncio.sleep(delay)

        # 多行 220 问候，故意拆成多个 TCP 分段、段间留小间隔，
        # 探测端必须能跨分段、跨行收齐。
        payload = (
            f"220-{NAME}.test ESMTP TestRelay\r\n"
            f"220-PIPELINING\r\n"
            f"220 {NAME}.test ready\r\n"
        ).encode()
        n = len(payload)
        bounds = [0, n // 4, n // 2, (3 * n) // 4, n]
        for a, b in zip(bounds, bounds[1:]):
            writer.write(payload[a:b])
            await writer.drain()
            await asyncio.sleep(0.05)
        STATS["greeted"] += 1
        log("已发送分段多行 220 问候")

        while True:
            line = await reader.readline()
            if not line:
                break
            cmd = line.decode("utf-8", "replace").strip()
            if not cmd:
                continue

            verb = cmd.split()[0].upper() if cmd.split() else ""
            await _delay()
            if verb == "NOOP":
                STATS["noop"] += 1
                writer.write(b"250 OK\r\n")
            elif verb in ("EHLO", "HELO"):
                writer.write(
                    f"250-{NAME}.test\r\n250-PIPELINING\r\n250 OK\r\n".encode())
            elif verb == "MAIL":
                STATS["mail_from"] += 1
                writer.write(b"250 sender OK\r\n")
            elif verb == "RCPT":
                addr = _extract_address(cmd)
                policy = STATE.get("rcpt_policy", {}).get(addr)
                if policy is None:
                    STATS["rcpt_total"] += 1
                    writer.write(
                        f"550 {addr} no such recipient here\r\n".encode())
                    STATS["rcpt_reject"] += 1
                    log(f"RCPT {addr}: 未知地址，回 550")
                else:
                    code = await _send_rcpt_reply(writer, addr, policy)
                    log(f"RCPT {addr}: 策略 {policy} -> {code}")
                rcpt_count += 1
            elif verb == "DATA":
                writer.write(b"354 Start mail input; end with <CRLF>.<CRLF>\r\n")
                await writer.drain()
                log("DATA 已回 354，等待正文")
                # 正文在同一连接内同步读到结束行，再给最终应答
                await _read_data_body(reader)
                STATS["data_sessions"] += 1
                await _delay()
                mode = STATE.get("data_final", "accept")
                log(f"正文收齐（{rcpt_count} 个 RCPT），最终应答模式: {mode}")
                if mode == "accept":
                    writer.write(b"250 OK message accepted for delivery\r\n")
                    STATS["final_250"] += 1
                elif mode == "tempfail":
                    writer.write(
                        b"451 Requested action aborted: local error, "
                        b"try again later\r\n")
                    STATS["final_451"] += 1
                else:  # half_close：半条最终应答后断连
                    partial = b"250 OK queued as "  # 故意没有 CRLF
                    writer.write(partial)
                    await writer.drain()
                    STATS["half_close"] += 1
                    log("已发出半条最终应答，按配置断连 (RST)")
                    _reset_connection(writer)
                    return
            elif verb == "RSET":
                rcpt_count = 0
                writer.write(b"250 OK reset\r\n")
            elif verb == "QUIT":
                writer.write(b"221 Bye\r\n")
                await writer.drain()
                break
            else:
                writer.write(b"502 Command not implemented\r\n")
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError) as e:
        log(f"连接异常: {type(e).__name__}: {e}")
    except Exception as e:  # noqa: BLE001 - 测试中继不让单会话拖垮服务
        log(f"会话错误: {type(e).__name__}: {e}")
    finally:
        STATS["closed"] += 1
        try:
            writer.close()
        except Exception:
            pass
        log(f"连接释放 {peer}")


async def _send_json(writer: asyncio.StreamWriter, status: int, obj: dict) -> None:
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found"}.get(status, "OK")
    body = json.dumps(obj).encode()
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: close\r\n\r\n".encode() + body
    )
    await writer.drain()


async def ctrl_session(reader: asyncio.StreamReader,
                       writer: asyncio.StreamWriter) -> None:
    try:
        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5)
        request_line, *header_lines = head.decode("iso-8859-1").split("\r\n")
        method, path, _ = request_line.split(" ", 2)
        headers = {}
        for hl in header_lines:
            if ":" in hl:
                k, v = hl.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        body = b""
        clen = int(headers.get("content-length", "0") or "0")
        if clen:
            body = await reader.readexactly(clen)

        if method == "POST" and path == "/config":
            data = json.loads(body.decode() or "{}")
            for k in ("greeting_delay", "response_delay"):
                if k in data:
                    STATE[k] = max(0.0, float(data[k]))
            if "reset" in data:
                STATE["reset"] = bool(data["reset"])
            if "rcpt_policy" in data:
                policy = data["rcpt_policy"]
                if not isinstance(policy, dict):
                    raise ValueError("rcpt_policy 必须是地址->策略的对象")
                for addr, p in policy.items():
                    if addr not in SAMPLE_ADDRESSES:
                        raise ValueError(f"未知样例地址: {addr}")
                    if p not in POLICIES:
                        raise ValueError(
                            f"非法策略 {p}（允许: {', '.join(POLICIES)}）")
                STATE["rcpt_policy"] = {
                    addr: policy.get(addr, STATE["rcpt_policy"].get(addr))
                    for addr in SAMPLE_ADDRESSES
                }
            if "data_final" in data:
                if data["data_final"] not in DATA_FINALS:
                    raise ValueError(
                        f"非法 data_final（允许: {', '.join(DATA_FINALS)}）")
                STATE["data_final"] = data["data_final"]
            log(f"配置更新: {STATE}")
            await _send_json(writer, 200, {"ok": True, "state": STATE})
        elif method == "GET" and path == "/state":
            await _send_json(writer, 200, {
                "name": NAME,
                "addresses": SAMPLE_ADDRESSES,
                "state": STATE,
                "stats": STATS,
            })
        elif method == "GET" and path == "/healthz":
            await _send_json(writer, 200, {"ok": True})
        else:
            await _send_json(writer, 404, {"error": "not found"})
    except Exception as e:  # noqa: BLE001
        try:
            await _send_json(writer, 400, {"error": f"{type(e).__name__}: {e}"})
        except Exception:
            pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


async def main() -> None:
    smtp_server = await asyncio.start_server(smtp_session, "0.0.0.0", SMTP_PORT)
    ctrl_server = await asyncio.start_server(ctrl_session, "0.0.0.0", CTRL_PORT)
    log(f"SMTP 监听 :{SMTP_PORT}，控制口 :{CTRL_PORT}")
    async with smtp_server, ctrl_server:
        await asyncio.gather(smtp_server.serve_forever(),
                             ctrl_server.serve_forever())


if __name__ == "__main__":
    asyncio.run(main())
