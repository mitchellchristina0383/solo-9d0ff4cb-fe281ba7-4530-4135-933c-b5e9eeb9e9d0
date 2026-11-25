"""SMTP 中继探测台（FastAPI）。

探测流程：
1. 先把两路延迟配置下发到两个测试中继的控制口；
2. 先拨首选 relay-a；备用等待期满仍未收齐 220 问候则并行起拨 relay-b，
   首选提前失败（建连失败/被复位/问候中断）则立即起拨备用；
3. 问候可能跨 TCP 分段、跨多行（220-...），必须收齐到 "220 " 行为止；
4. 首条收齐问候的连接胜出：取消落选拨号任务并关掉落选连接，
   只沿胜出连接发一次 NOOP，收到 250 判定通过；
5. 到达探测总时限即停止；探测结束释放所有连接。
"""
import asyncio
import os
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


RELAYS = {
    "a": {
        "name": _env("RELAY_A_NAME", "relay-a"),
        "host": _env("RELAY_A_HOST", "127.0.0.1"),
        "smtp_port": int(_env("RELAY_A_SMTP_PORT", "2525")),
        "ctrl": _env("RELAY_A_CTRL", "http://127.0.0.1:8081"),
    },
    "b": {
        "name": _env("RELAY_B_NAME", "relay-b"),
        "host": _env("RELAY_B_HOST", "127.0.0.1"),
        "smtp_port": int(_env("RELAY_B_SMTP_PORT", "2526")),
        "ctrl": _env("RELAY_B_CTRL", "http://127.0.0.1:8082"),
    },
}

MAX_REPLY_LINES = 100  # 问候/应答行数上限，防对端刷行


class RelayCfg(BaseModel):
    greeting_delay: float = Field(0.2, ge=0, le=60, description="问候延迟(秒)")
    response_delay: float = Field(0.1, ge=0, le=60, description="应答延迟(秒)")
    reset: bool = Field(False, description="建连后立即 RST，模拟对端失败")


class ProbeReq(BaseModel):
    relay_a: RelayCfg = RelayCfg()
    relay_b: RelayCfg = RelayCfg()
    backup_wait: float = Field(1.0, ge=0, le=120, description="备用起拨前等待(秒)")
    total_timeout: float = Field(5.0, ge=0.2, le=180, description="探测总时限(秒)")


POLICY_ACCEPT = "accept"
POLICY_TEMPFAIL = "tempfail"
POLICY_REJECT = "reject"
DATA_FINAL_CHOICES = ("accept", "tempfail", "half_close")


class TrialReq(BaseModel):
    # 地址 -> accept（接收）/ tempfail（暂拒 451）/ reject（永久拒收 550）
    rcpt_policy: dict[str, str]
    data_final: str = Field("accept", description="正文最终应答模式")
    timeout: float = Field(10.0, ge=1.0, le=60.0, description="试投总时限(秒)")

    @field_validator("rcpt_policy")
    @classmethod
    def _check_policy(cls, v: dict) -> dict:
        for addr, p in v.items():
            if p not in (POLICY_ACCEPT, POLICY_TEMPFAIL, POLICY_REJECT):
                raise ValueError(f"地址 {addr} 的策略非法: {p}")
        return v

    @field_validator("data_final")
    @classmethod
    def _check_data_final(cls, v: str) -> str:
        if v not in DATA_FINAL_CHOICES:
            raise ValueError(f"data_final 非法: {v}")
        return v


app = FastAPI(title="SMTP 中继探测台")
INDEX_HTML = Path(__file__).with_name("index.html")
probe_lock = asyncio.Lock()  # 中继配置是全局的，探测串行执行避免互相改配置


async def read_smtp_reply(reader: asyncio.StreamReader):
    """读一段 SMTP 应答：跨 TCP 分段缓冲成行，跨 220- 续行直到 '代码+空格' 行。"""
    lines = []
    while True:
        raw = await reader.readline()  # readline 自动跨分段拼行
        if not raw:
            raise EOFError("对端关闭了连接")
        text = raw.decode("utf-8", "replace").rstrip("\r\n")
        lines.append(text)
        if len(text) >= 4 and text[:3].isdigit() and text[3] == " ":
            return int(text[:3]), lines
        if len(text) >= 4 and text[:3].isdigit() and text[3] == "-":
            continue  # 多行续行，继续等
        if len(lines) >= MAX_REPLY_LINES:
            raise ValueError("应答行数超限，判定格式异常")


async def read_final_reply(reader: asyncio.StreamReader):
    """读正文最终应答；EOF 时若已读到半条（无 CRLF 的残行），按"应答不完整"上报。

    返回 (code|None, lines, partial)：
    - 正常收齐终行：code 为三位码，partial 为 None；
    - 断连且一行都没收到：抛 EOFError；
    - 断连但留下半条应答：code=None，partial 为残行原文。
    """
    lines = []
    while True:
        raw = await reader.readline()  # 跨分段拼行；残行（无 CRLF）在 EOF 时也会吐出
        if not raw:
            if not lines:
                raise EOFError("正文发完后对端直接断连，没有任何最终应答")
            return None, lines, lines[-1]
        text = raw.decode("utf-8", "replace").rstrip("\r\n")
        lines.append(text)
        if len(text) >= 4 and text[:3].isdigit() and text[3] == " ":
            return int(text[:3]), lines, None
        if len(text) >= 4 and text[:3].isdigit() and text[3] == "-":
            continue
        if len(lines) >= MAX_REPLY_LINES:
            raise ValueError("应答行数超限，判定格式异常")


class TrialError(Exception):
    """试投在正文开始前失败（连接/问候/EHLO/MAIL/RCPT/DATA 354 阶段）。"""


async def run_trial(req: TrialReq) -> dict:
    """沿首选 relay-a 做一次完整批量试投（PIPELINING），返回逐收件结果。"""
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    deadline = t0 + req.timeout
    events: list[dict] = []
    cfg = RELAYS["a"]

    def now() -> float:
        return round(loop.time() - t0, 3)

    def log(msg: str) -> None:
        events.append({"t": now(), "msg": msg})

    async def read_with_deadline(reader, final=False):
        remaining = deadline - loop.time()
        if remaining <= 0:
            raise TimeoutError("已超过试投总时限")
        if final:
            return await asyncio.wait_for(
                read_final_reply(reader), timeout=remaining)
        return await asyncio.wait_for(read_smtp_reply(reader), timeout=remaining)

    addresses = list(req.rcpt_policy.keys())
    recipients = []  # 逐收件结果，顺序与 RCPT TO 一致
    result = {
        "outcome": None,
        "duration": None,
        "relay": cfg["name"],
        "addresses": addresses,
        "pipelining": True,
        "mail_from": None,
        "data_reply": None,
        "final_reply": None,
        "recipients": recipients,
        "resendable": [],
        "events": events,
    }
    writer = None
    try:
        log(f"连接首选中继 {cfg['host']}:{cfg['smtp_port']}")
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(cfg["host"], cfg["smtp_port"]),
            timeout=deadline - loop.time())

        code, lines = await read_with_deadline(reader)
        if code != 220:
            raise TrialError(f"问候码异常: {code}")
        log(f"220 问候收齐（{len(lines)} 行）")

        # EHLO：确认对端声明 PIPELINING
        writer.write(b"EHLO probe.local\r\n")
        await writer.drain()
        code, lines = await read_with_deadline(reader)
        if code != 250:
            raise TrialError(f"EHLO 被拒: {code}")
        caps = " ".join(lines).upper()
        result["ehlo_pipelining"] = "PIPELINING" in caps
        log(f"EHLO 应答 {code}，PIPELINING 能力: {result['ehlo_pipelining']}")

        # 一条真实连接、一次 write 连发 MAIL FROM + 各 RCPT TO + DATA（PIPELINING）
        mail = "MAIL FROM:<probe@probe.local>\r\n"
        rcpt_cmds = [f"RCPT TO:<{addr}>\r\n" for addr in addresses]
        pipeline = (mail + "".join(rcpt_cmds) + "DATA\r\n").encode()
        writer.write(pipeline)
        await writer.drain()
        result["mail_from"] = "probe@probe.local"
        log(f"PIPELINING 已发出: MAIL FROM + {len(rcpt_cmds)} 个 RCPT TO + DATA"
            f"（{len(pipeline)} 字节，一次 write）")

        # MAIL FROM 应答
        code, lines = await read_with_deadline(reader)
        if code != 250:
            raise TrialError(f"MAIL FROM 被拒（{code}），终止试投")
        log(f"MAIL FROM 应答 {code}")

        # 逐个收齐 RCPT 应答并按原命令顺序配对（多行应答须整段收齐）
        for addr in addresses:
            code, lines = await read_with_deadline(reader)
            policy = req.rcpt_policy[addr]
            entry = {
                "address": addr,
                "configured_policy": policy,
                "rcpt_code": code,
                "rcpt_lines": lines,
                "rcpt_multiline": len(lines) > 1,
                "rcpt_status": ("accepted" if code == 250
                                else "tempfail" if 400 <= code < 500
                                else "reject"),
                "final_status": None,   # 正文最终应答后的结论
                "final_code": None,
                "resendable": False,
            }
            recipients.append(entry)
            log(f"RCPT {addr} 应答收齐: {code}"
                + ("（多行）" if len(lines) > 1 else ""))

        # DATA 应答：必须收到 354 才发正文
        code, lines = await read_with_deadline(reader)
        result["data_reply"] = {"code": code, "lines": lines}
        if code != 354:
            raise TrialError(f"DATA 未获 354（实际 {code}），不发送正文")
        log("收到 354，开始发送正文")

        body = (
            "From: probe@probe.local\r\n"
            f"To: {', '.join(addresses)}\r\n"
            "Subject: 中继批量试投测试\r\n"
            "\r\n"
            "这是一封由探测台发出的试投邮件，用于核对中继对各收件地址的应答。\r\n"
            ".\r\n"
        ).encode()
        writer.write(body)
        await writer.drain()
        log(f"正文已发完（{len(body)} 字节），等待最终应答")

        # 正文最终应答：半条断连要标"待核"
        code, lines, partial = await read_with_deadline(reader, final=True)
        result["final_reply"] = {"code": code, "lines": lines, "partial": partial}
        if code is None:
            result["outcome"] = "final_incomplete"
            log(f"最终应答不完整（断连），残行: {partial!r}")
        elif code == 250:
            result["outcome"] = "final_250"
            log(f"正文最终应答 {code}，通过 RCPT 的地址判定为已接收")
        elif 400 <= code < 500:
            result["outcome"] = "final_tempfail"
            log(f"正文最终应答 {code}（暂拒），通过 RCPT 的地址判定为暂拒")
        else:
            result["outcome"] = "final_reject"
            log(f"正文最终应答 {code}（永久失败）")
    except TrialError as e:
        result["outcome"] = "pre_data_failed"
        result["error"] = str(e)
        log(f"试投提前终止: {e}")
    except (EOFError, ConnectionResetError, BrokenPipeError) as e:
        # 正文已发完后断连才算"待核"；更早的断连归为连接失败
        if result["final_reply"] is None and result["data_reply"] is not None:
            result["outcome"] = "final_incomplete"
            result["final_reply"] = {
                "code": None, "lines": [],
                "partial": f"{type(e).__name__}: {e}"}
            log("正文发完后连接中断，未收到完整最终应答")
        else:
            result["outcome"] = "connection_failed"
            result["error"] = f"{type(e).__name__}: {e}"
            log(f"连接失败: {type(e).__name__}: {e}")
    except (asyncio.TimeoutError, TimeoutError) as e:
        result["outcome"] = "timeout"
        result["error"] = f"{type(e).__name__}: {e}"
        log("试投超过总时限")
    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
            log("连接已释放")

    # 汇总结论：RCPT 被拒/暂拒的保留原结论；RCPT 通过的以正文最终应答为准
    resendable = []
    for r in recipients:
        if r["rcpt_status"] == "reject":
            r["final_status"] = "reject"          # 永久拒收，保留原结论
        elif r["rcpt_status"] == "tempfail":
            r["final_status"] = "tempfail"        # 明确暂拒，保留原结论
            r["resendable"] = True
            resendable.append(r["address"])
        else:  # RCPT 250 通过，结论看正文最终应答
            if result["outcome"] == "final_250":
                r["final_status"] = "accepted"
            elif result["outcome"] == "final_tempfail":
                r["final_status"] = "tempfail"
            elif result["outcome"] == "final_reject":
                r["final_status"] = "reject"
            else:  # final_incomplete / timeout / 其他：待核
                r["final_status"] = "unverified"
        r["final_code"] = (result["final_reply"] or {}).get("code")
    result["resendable"] = resendable
    result["duration"] = now()
    log(f"逐收件结论汇总完成，可重投 {len(resendable)} 个: {resendable}")
    return result


async def run_probe(req: ProbeReq) -> dict:
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    deadline = t0 + req.total_timeout
    events = []

    def now() -> float:
        return round(loop.time() - t0, 3)

    def log(msg: str) -> None:
        events.append({"t": now(), "msg": msg})

    relays = {
        key: {
            "name": RELAYS[key]["name"],
            "role": "primary" if key == "a" else "backup",
            "dialed": False,
            "dial_at": None,
            "connect": None,    # {ok, at, error?}
            "greeting": None,   # {ok, at, code, lines} 或 {ok: False, at, error}
            "cancelled": False,
            "closed": False,
            "note": "",
        }
        for key in ("a", "b")
    }
    open_writers = {}  # key -> writer，探测结束统一释放

    async def close_writer(key: str, writer) -> None:
        try:
            writer.close()
        except Exception:
            pass
        open_writers.pop(key, None)
        relays[key]["closed"] = True

    async def attempt(key: str):
        """拨号并等收齐问候；成功返回连接，失败抛异常。被取消时自行关闭连接。"""
        info = relays[key]
        cfg = RELAYS[key]
        info["dialed"] = True
        info["dial_at"] = now()
        log(f"{cfg['name']}: 拨号 {cfg['host']}:{cfg['smtp_port']}")
        writer = None
        try:
            reader, writer = await asyncio.open_connection(
                cfg["host"], cfg["smtp_port"])
            open_writers[key] = writer
            info["connect"] = {"ok": True, "at": now()}
            log(f"{cfg['name']}: TCP 建连成功，等待 220 问候")
            code, lines = await read_smtp_reply(reader)
            info["greeting"] = {"ok": True, "at": now(),
                                "code": code, "lines": lines}
            log(f"{cfg['name']}: 问候收齐（代码 {code}，{len(lines)} 行）")
            return {"key": key, "reader": reader, "writer": writer,
                    "code": code, "lines": lines}
        except asyncio.CancelledError:
            info["cancelled"] = True
            log(f"{cfg['name']}: 落选，拨号/等待被取消")
            if writer is not None:
                await close_writer(key, writer)
            raise
        except Exception as e:
            err = f"{type(e).__name__}: {e}"
            if info["connect"] is None:
                info["connect"] = {"ok": False, "at": now(), "error": err}
                log(f"{cfg['name']}: 建连失败（{err}）")
            else:
                info["greeting"] = {"ok": False, "at": now(), "error": err}
                log(f"{cfg['name']}: 问候未收齐（{err}）")
            if writer is not None:
                await close_writer(key, writer)
            raise

    primary = asyncio.create_task(attempt("a"), name="a")
    backup = None
    timer = asyncio.create_task(asyncio.sleep(req.backup_wait), name="timer")
    winner = None
    outcome = None

    def ensure_backup() -> None:
        nonlocal backup
        if backup is None:
            backup = asyncio.create_task(attempt("b"), name="b")
            log(f"{RELAYS['b']['name']}: 起拨备用")

    try:
        while winner is None:
            active = [t for t in (primary, backup) if t is not None and not t.done()]
            wait_set = set(active)
            if not timer.done():
                wait_set.add(timer)
            if not wait_set:
                outcome = "no_greeting"
                log("两路均未收齐问候")
                break
            remaining = deadline - loop.time()
            if remaining <= 0:
                outcome = "timeout"
                log("到达探测总时限，停止")
                break
            done, _ = await asyncio.wait(
                wait_set, timeout=remaining,
                return_when=asyncio.FIRST_COMPLETED)
            if not done:
                outcome = "timeout"
                log("到达探测总时限，停止")
                break
            if timer in done:
                log(f"备用等待 {req.backup_wait}s 期满，首选问候仍未收齐")
                if not primary.done():
                    ensure_backup()
            # 首选优先处理，保证同时完成时首选胜出、结果确定
            for t in sorted(done, key=lambda x: 0 if x is primary else 1):
                if t is timer or t.cancelled():
                    continue
                exc = t.exception()
                if exc is not None:
                    if t is primary:
                        log("首选提前失败，立即起拨备用")
                        ensure_backup()
                    continue
                if winner is None:
                    winner = t.result()
                    relays[t.get_name()]["note"] = "胜出连接"
                else:  # 同一轮里另一路也收齐：落选连接立即关闭
                    other = t.result()
                    relays[t.get_name()]["note"] = "同时收齐但落选，已关闭"
                    await close_writer(t.get_name(), other["writer"])
    finally:
        timer.cancel()
        for t in (primary, backup):
            if t is not None and not t.done():
                t.cancel()
        await asyncio.gather(
            *[t for t in (primary, backup) if t is not None],
            return_exceptions=True)

    noop = {"sent": False}
    if winner is not None:
        key = winner["key"]
        writer = winner["writer"]
        noop.update({"sent": True, "relay": key,
                     "relay_name": RELAYS[key]["name"]})
        try:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise TimeoutError("已超过探测总时限")
            log(f"沿 {RELAYS[key]['name']} 发送 NOOP（仅此一次）")
            async def _noop_roundtrip():
                writer.write(b"NOOP\r\n")
                await writer.drain()
                return await read_smtp_reply(winner["reader"])
            code, lines = await asyncio.wait_for(_noop_roundtrip(), remaining)
            noop.update({"ok": code == 250, "code": code, "lines": lines})
            log(f"NOOP 应答 {code}" + ("，判定通过" if code == 250 else "，非 250"))
            outcome = "pass" if code == 250 else "noop_bad_reply"
        except Exception as e:
            noop.update({"ok": False,
                         "error": f"{type(e).__name__}: {e}"})
            log(f"NOOP 失败: {type(e).__name__}: {e}")
            outcome = "noop_failed"
        finally:
            await close_writer(key, writer)

    # 探测结束：释放所有残留连接（落选/超时未关的）
    for key, writer in list(open_writers.items()):
        log(f"释放残留连接 {RELAYS[key]['name']}")
        await close_writer(key, writer)

    return {
        "outcome": outcome,
        "duration": now(),
        "params": req.model_dump(),
        "relays": relays,
        "noop": noop,
        "events": events,
    }


@app.get("/")
async def index():
    return FileResponse(INDEX_HTML)


@app.get("/api/relays")
async def relay_targets():
    return {
        k: {"name": v["name"], "host": v["host"], "smtp_port": v["smtp_port"]}
        for k, v in RELAYS.items()
    }


@app.get("/api/relay-state")
async def relay_state():
    """拉回两路中继的当前配置与连接计数（核对连接释放用）。"""
    out = {}
    async with httpx.AsyncClient(timeout=5.0) as client:
        for key in ("a", "b"):
            try:
                r = await client.get(RELAYS[key]["ctrl"] + "/state")
                out[key] = r.json()
            except Exception as e:
                out[key] = {"error": f"{type(e).__name__}: {e}"}
    return out


@app.get("/api/trial-config")
async def trial_config():
    """首选中继上的样例地址清单、当前策略与正文最终应答模式。"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(RELAYS["a"]["ctrl"] + "/state")
            r.raise_for_status()
            data = r.json()
        return {
            "relay": RELAYS["a"]["name"],
            "addresses": data.get("addresses", []),
            "rcpt_policy": data.get("state", {}).get("rcpt_policy", {}),
            "data_final": data.get("state", {}).get("data_final", "accept"),
            "data_final_choices": list(DATA_FINAL_CHOICES),
        }
    except Exception as e:
        raise HTTPException(
            502, f"无法读取 {RELAYS['a']['name']} 配置: {e}") from e


@app.post("/api/trial")
async def api_trial(req: TrialReq):
    """批量试投：先把地址策略/最终应答模式下发首选 relay-a，再走一次完整 SMTP 事务。"""
    async with probe_lock:
        # 与探测接口共用锁：中继配置是全局的，串行避免互相改配置
        async with httpx.AsyncClient(timeout=5.0) as client:
            try:
                r = await client.post(
                    RELAYS["a"]["ctrl"] + "/config",
                    json={
                        "rcpt_policy": req.rcpt_policy,
                        "data_final": req.data_final,
                        # 试投不受探测页遗留的延迟/复位配置影响
                        "greeting_delay": 0.0,
                        "response_delay": 0.0,
                        "reset": False,
                    })
                r.raise_for_status()
            except Exception as e:
                raise HTTPException(
                    502, f"无法配置 {RELAYS['a']['name']}: {e}") from e

            result = await run_trial(req)

            try:
                r = await client.get(RELAYS["a"]["ctrl"] + "/state")
                result["relay_stats"] = r.json().get("stats")
            except Exception:
                result["relay_stats"] = None
            return result


@app.post("/api/probe")
async def api_probe(req: ProbeReq):
    async with probe_lock:
        # 1) 下发两路延迟配置到中继控制口
        async with httpx.AsyncClient(timeout=5.0) as client:
            for key, cfg in (("a", req.relay_a), ("b", req.relay_b)):
                try:
                    r = await client.post(RELAYS[key]["ctrl"] + "/config",
                                          json=cfg.model_dump())
                    r.raise_for_status()
                except Exception as e:
                    raise HTTPException(
                        502, f"无法配置 {RELAYS[key]['name']}: {e}") from e
            # 2) 执行探测
            result = await run_probe(req)
            # 3) 顺带取回中继侧连接计数，便于核对连接是否释放
            stats = {}
            for key in ("a", "b"):
                try:
                    r = await client.get(RELAYS[key]["ctrl"] + "/state")
                    stats[key] = r.json().get("stats")
                except Exception:
                    stats[key] = None
            result["relay_stats"] = stats
            return result
