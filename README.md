# SMTP 中继探测台

告警邮件发不出去时，值班员用这个小工具定位中继卡在哪一步：FastAPI 探测页通过本地 TCP
分别连到两个 SMTP 测试中继，按"首选 → 备用"的竞速策略探测，并沿首条收齐问候的连接
发一次 `NOOP` 验证应答。

## 组成

```
├── docker-compose.yml   # 一键启动：relay-a、relay-b、app
├── relay/               # SMTP 测试中继（纯标准库）
│   └── smtp_relay.py    #   SMTP 口 + HTTP 控制口（动态调延迟/模拟复位）
├── app/                 # FastAPI 探测服务
│   ├── main.py          #   探测编排：竞速拨号、问候收齐、NOOP、总时限、连接释放
│   └── index.html       #   探测页（参数、场景预置、结果与时间线展示）
└── tests/selftest.py    # 端到端自测（标准库，打 /api/probe）
```

## 启动（Docker Compose）

```bash
docker compose up --build
# 打开 http://localhost:8000
```

端口：探测页 `8000`；relay-a SMTP `2525` / 控制口 `8081`；relay-b SMTP `2526` / 控制口 `8082`。

无 Docker 时本地运行：

```bash
pip install -r app/requirements.txt
RELAY_NAME=relay-a SMTP_PORT=2525 CTRL_PORT=8081 python3 relay/smtp_relay.py &
RELAY_NAME=relay-b SMTP_PORT=2526 CTRL_PORT=8082 python3 relay/smtp_relay.py &
cd app && uvicorn main:app --port 8000
```

## 探测流程（app/main.py）

1. 页面提交两路的问候/应答延迟（可勾选"建连后立即复位"模拟失败）、备用起拨前等待、
   探测总时限；服务先把配置下发到两个中继的控制口。
2. **先拨首选** relay-a。两个触发条件并行监控：
   - 备用等待期满首选仍未收齐 220 问候 → **并行起拨备用**；
   - 首选提前失败（建连失败 / 被 RST / 问候中断）→ **立即起拨备用**。
3. 问候按 SMTP 规则收齐：`readline` 跨 TCP 分段拼行，`220-` 续行读到 `220 ` 终行
   为止（中继会故意把多行问候拆成 4 个分段发送）。
4. 首条收齐问候的连接胜出：**取消落选拨号任务并关掉落选连接**（同一轮都收齐时
   首选优先，落选连接显式关闭）；只沿胜出连接**发一次 NOOP**，收到 `250` 判定通过。
5. **到达总时限即停止**（含 NOOP 等待在内）；探测结束**释放所有连接**。

页面展示：两路各自的拨号时刻、建连结果、问候结果（含问候原文）、结局（胜出/落选取消/
未起拨），NOOP 的应答行或失败原因，带相对时间戳的事件时间线，以及中继侧连接计数
（accepted 与 closed 对齐即无泄漏）。

## 自测

```bash
python3 tests/selftest.py http://127.0.0.1:8000
```

覆盖：① 首选正常胜出（备用不起拨）② 首选问候慢 → 等待期满并行起拨、备用胜出、首选
被取消并关闭 ③ 首选 RST → 立即起拨备用 ④ 两路都慢 → 总时限停止、连接全释放
⑤ NOOP 应答超时限 ⑥ 中继侧计数核对无泄漏。页面上的 5 个场景预置按钮与之一一对应。

## API

- `GET /` 探测页
- `POST /api/probe` 执行探测，入参 `{relay_a, relay_b, backup_wait, total_timeout}`，
  其中每路 `{greeting_delay, response_delay, reset}`
- `GET /api/relays` 当前两路目标地址
- `GET /api/relay-state` 两路中继的当前配置与连接计数

中继控制口：`POST /config`（`greeting_delay` / `response_delay` / `reset`）、
`GET /state`、`GET /healthz`。

## 已知限制（余项）

- 中继的"失败"用建连后立即 RST 模拟；真实的"连接拒绝"（端口不通）走同一失败路径
  （`open_connection` 抛错 → 立即起拨备用），但未做场景预置按钮。
- 中继配置是全局的，探测接口用锁串行执行，避免并发探测互相改配置。
- 探测结果为一次性快照（页面不实时推送）；时间线精度到毫秒。
- 仅实现 NOOP 验证，不探测真实发信（MAIL/RCPT/DATA）流程。
