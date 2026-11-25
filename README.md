# SMTP 中继探测台

告警邮件发不出去时，值班员用这个小工具定位中继卡在哪一步：FastAPI 探测页通过本地 TCP
分别连到两个 SMTP 测试中继，按"首选 → 备用"的竞速策略探测，并沿首条收齐问候的连接
发一次 `NOOP` 验证应答。**首选中继还支持批量试投**：一条真实 SMTP 连接用 PIPELINING
连发 `MAIL FROM`/各 `RCPT TO`/`DATA`，网页按线上应答逐个列出收件结果，并给出只含
明确暂拒地址的可重投名单。

## 组成

```
├── docker-compose.yml   # 一键启动：relay-a、relay-b、app
├── relay/               # SMTP 测试中继（纯标准库）
│   └── smtp_relay.py    #   SMTP 口（问候/完整事务/分段多行 RCPT/半条断连）+ HTTP 控制口
├── app/                 # FastAPI 探测服务
│   ├── main.py          #   竞速探测编排 + 批量试投编排（PIPELINING、按序配对、待核判定）
│   └── index.html       #   探测页 + 批量试投面板（地址策略、结果表、可重投名单）
└── tests/
    ├── selftest.py      # 端到端自测：竞速探测（标准库，打 /api/probe）
    └── trial_selftest.py# 端到端自测：批量试投 + 原始 socket 验证 PIPELINING/分段重组
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

## 批量试投（首选 relay-a，app/main.py `run_trial`）

告警只发到部分地址时，值班员用它核对每个地址的线上应答，直接拿到可重投名单。

1. 网页为每个**本地样例地址**（共 5 个，清单由中继 `/state` 下发，单一事实来源）
   设置策略：接收（250）、暂拒（451）或永久拒收（550）；正文收齐后的最终应答可选
   回 250、回 451，或**发出半条最终应答后断连**。提交后服务先把配置 POST 到 relay-a
   控制口（同时把延迟/复位复位为默认，避免探测页遗留配置干扰试投）。
2. 建一条真实 TCP 连接，收齐分段多行 220 问候后发 `EHLO`，核对应答里声明
   `PIPELINING`；随后**一次 `write` 连发** `MAIL FROM`、各 `RCPT TO` 与 `DATA`。
3. 应答逐个收齐并**按原命令顺序配对**：`readline` 跨 TCP 分段拼行，多行应答
   （`250-` 续行）读到 `250 ` 终行为止。样例地址 `ops@multiline.test` 的 RCPT 应答
   固定由中继拆成 3 个 TCP 分段发送多行 250，专门检验这一能力。
4. **收到 `354` 才发正文**；正文以 `<CRLF>.<CRLF>` 结束。
5. 最终结论规则：
   - RCPT 被拒（451/550）的地址**保留 RCPT 原结论**，不看正文应答；
   - RCPT 通过（250）的地址**以正文最终应答为准**：250→已接收，451→暂拒，5xx→拒收；
   - 正文发完却没收到完整最终应答（半条残行/直接断连/超时）→ 通过 RCPT 的地址标
     **待核（unverified）**，残行原文随结果返回。
6. **可重投名单只列明确暂拒（RCPT 451）的地址**；正文级 451 与待核都不算明确暂拒。
7. 试投与探测共用同一把串行锁；结束后无论成败都关闭连接，中继侧 accepted/closed
   计数可核对无泄漏。

页面展示：逐收件结果表（设置策略、RCPT 码/是否多行、RCPT 结论、正文最终码、最终
结论、是否可重投，按 RCPT TO 原顺序）、可重投名单、带时间戳的试投时间线和中继侧
计数。三个试投预置按钮对应：Ⓐ 混合应答+正文 250、Ⓑ 全接收+正文 451、Ⓒ 全接收+
末尾断连。

## 自测

```bash
# 竞速探测自测
python3 tests/selftest.py http://127.0.0.1:8000
# 批量试投自测（含原始 socket 的 PIPELINING / 跨分段多行 RCPT 验证）
python3 tests/trial_selftest.py http://127.0.0.1:8000
```

探测自测覆盖：① 首选正常胜出（备用不起拨）② 首选问候慢 → 等待期满并行起拨、备用
胜出、首选被取消并关闭 ③ 首选 RST → 立即起拨备用 ④ 两路都慢 → 总时限停止、连接
全释放 ⑤ NOOP 应答超时限 ⑥ 中继侧计数核对无泄漏。页面上的 5 个场景预置按钮与之一一
对应。

试投自测覆盖：Ⓐ 收件人混合应答（250×3 含分段多行 / 451 / 550）+ 正文 250，结论配对与
可重投名单核对；Ⓑ 全接收 + 正文 451；Ⓒ 全接收 + 半条断连（待核）；Ⓓ 混合策略 +
断连（被拒的保留原结论、通过的待核）；Ⓔ 原始 TCP 连接一次 sendall 流水线连发，逐字节
读取验证问候与多行 RCPT 确实跨分段到达、应答按命令顺序配对、354 后才发正文；以及
中继侧计数核对。

## API

- `GET /` 探测页（含批量试投面板）
- `POST /api/probe` 执行探测，入参 `{relay_a, relay_b, backup_wait, total_timeout}`，
  其中每路 `{greeting_delay, response_delay, reset}`
- `GET /api/relays` 当前两路目标地址
- `GET /api/relay-state` 两路中继的当前配置与连接计数
- `GET /api/trial-config` 首选中继的样例地址清单、当前地址策略与正文应答模式
- `POST /api/trial` 批量试投，入参 `{rcpt_policy: {地址: accept|tempfail|reject},
  data_final: accept|tempfail|half_close, timeout}`，返回逐收件结果、可重投名单与时间线

中继控制口：`POST /config`（`greeting_delay` / `response_delay` / `reset` /
`rcpt_policy` / `data_final`，带白名单校验）、`GET /state`（含样例地址清单与事务
计数）、`GET /healthz`。

## 已知限制（余项）

- 中继的"失败"用建连后立即 RST 模拟；真实的"连接拒绝"（端口不通）走同一失败路径
  （`open_connection` 抛错 → 立即起拨备用），但未做场景预置按钮。
- 中继配置是全局的，探测/试投接口共用一把锁串行执行，避免并发互相改配置。
- 探测结果为一次性快照（页面不实时推送）；时间线精度到毫秒。
- 批量试投只针对首选 relay-a（需求定位为"给首选测试中继加批量试投"）；备用中继保留
  同一套 SMTP 能力但页面不提供试投入口。
- 试投邮件正文为固定模板（发件人 probe@probe.local），不支持自定义正文/附件；
  地址清单为中继内置的 5 个样例地址，新增地址需改中继 `SAMPLE_ADDRESSES`。
- 半条断连固定发送无 CRLF 的 `250 OK queued as ` 残行后 RST；正文级 451 按需求
  不进入可重投名单（可重投只认 RCPT 明确暂拒）。
