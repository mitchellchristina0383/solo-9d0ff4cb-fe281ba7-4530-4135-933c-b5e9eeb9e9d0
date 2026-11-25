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
from pydantic import BaseModel, Field


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
