# Latitude Alpha 统一控制台(HTML shell + 只读数据 API)

施工包05 呈现层的**方向修正**:原 5A/5B 在 Streamlit 里模仿模板,受框架限制无法
还原 `docs/dashboard_spec/latitude_console_full.html` 的样子。本目录直接以该模板为
前端,配一个 FastAPI 只读数据服务喂真数据——**所见即模板,数字变真**。

## 构成

- `console.html` — 模板本体(6 页:总览/Var·Decibel/Polymarket/Predict.fun/Single Account/打新)。
  关键数值带 `data-k` 钩子,前端每 15s 拉 `/api/state` 覆盖真数据;无真数据或数据过期时
  显示“未知/待核验”,绝不保留模板示例值。
- `console_app.py` — FastAPI:`/` 返回 console.html;`/api/state` 读取现有状态文件/库。
  Var/Decibel 原生操作端点复用既有 `dashboard_jobs` 安全队列，不读取或迁移私钥。
- `varia-decibel-manual-job.service` — 一次性人工任务 worker；每次只消费一条已提交任务，
  完成即退出，不恢复旧自动化循环。

所有受控写端点受 `LATITUDE_ENABLE_WRITES` 总闸和审计日志约束。真实 Var/Decibel
开仓和平仓仅允许 Tailscale 内网入口；公网反代保持只读。

## 真实性规则

- Var/Decibel 只有在同一主机的两家交易所读取均成功且快照未过期时，才判断空仓、对冲或单腿。
- 过期/缺源快照只显示“仓位未知”，可附上次看到的币种，但不计入当前持仓或单腿告警。
- Polymarket 引擎停止或状态文件过期时，活跃挂单显示“待核验”，不复用历史状态文件里的数量。
- Mac mini signer 使用 TCP 可达性作为控制台健康信号；launchd 进程字段只作辅助信息。
- Var/Decibel 与 Polymarket 生产面板的初始 HTML 不含仓位、事件或账号运行样例；
  页面首次加载和接口失败时统一显示“读取中/未知”。

## 本地运行

```bash
cd deploy/latitude-console
LATITUDE_DATA_DIR=../../data <venv>/bin/python -m uvicorn console_app:app --port 8610
# 打开 http://127.0.0.1:8610
```

## VPS 部署(与 5C 统一入口合并)

数据目录指向生产:`LATITUDE_DATA_DIR=/home/ubuntu/polymarket-bot/data`。
建议 systemd 常驻,绑 `127.0.0.1:8600`,nginx 把 `/` 从当前的 Streamlit(8501)
改指向本服务:

```nginx
location / {
    proxy_pass http://127.0.0.1:8600;   # 原为 127.0.0.1:8501(Streamlit),改为控制台
}
```

Var/Decibel 的仓位、报价、开仓参数、止盈止损、开仓、一键平仓和后台任务结果均在
统一站点内原生呈现，不使用 iframe，也不跳转旧 Streamlit 页面。旧 8503 服务可在迁移
验收期保留为内部诊断工具，但不再是用户操作入口。

安装一次性 worker：

```bash
sudo install -m 0644 deploy/latitude-console/varia-decibel-manual-job.service \
  /etc/systemd/system/varia-decibel-manual-job.service
sudo systemctl daemon-reload
# 不 enable；由控制台每次提交人工任务时 start，执行一条后自动退出。
```

## Var/Decibel 权益与积分口径

- 四个独立来源为 `VPS1/VPS2 × Decibel/Variational`。任一来源过期或读取失败时，
  不显示合计权益、积分或盈亏。
- `home_equity_principal.json` 是运营侧的追加式本金账本，不提交 Git。每个来源记录
  `initial`、外部 `cashflows`（充值为正、提现为负）和 `reconciled: true`。
- 控制台的“总权益”是四源当前权益之和；其下方的 `▲/▼` 是
  `当前总权益 - (初始本金 + 累计净充值/提现)`，即真实净交易结果，而非充值带来的账面增长。
- `points_by_venue` 同时返回 Decibel/Var 的总积分和 VPS1/VPS2 分项，前端不再把两个
  平台的积分混成一个不可核查的估算值。
- 旧的 `home_active_total_equity_history.json` 是调试遗留聚合快照，不参与收益展示。
  新曲线只使用已对账后的 `reconciled_pnl_history.json`，其 `last` 为当前真实净交易结果，
  `change` 为本次已记录区间内的变化。

安装五分钟只读快照任务：

```bash
sudo install -m 0644 deploy/systemd/latitude-reconciled-equity.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/latitude-reconciled-equity.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now latitude-reconciled-equity.timer
```

该任务只读取四源状态并写入本地报告 JSON；不启动 worker、不下单、不接触私钥。

## 每日港股新股导入

使用 systemd timer 取代单次 cron。任务在每天 01:00 运行；若 Windows/Tailnet
暂时不可达，会每 15 分钟重试，直到当天首次成功。成功后写入
`/home/ubuntu/ipo_import.success`，同一天后续检查不会重复抓取。

```bash
sudo install -m 0644 deploy/systemd/latitude-ipo-import.service /etc/systemd/system/
sudo install -m 0644 deploy/systemd/latitude-ipo-import.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now latitude-ipo-import.timer
```

## 数据接入进度(有真数据先接,其余占位)

已接真数据(`/api/state`):
- **Polymarket**:engine_state_N.json 聚合(运行账号/挂单/今日量/PnL/sibling 统计)
- **Single Account**:paper 状态 JSON(候选/可执行/最高分)+ 模拟器库虚拟权益
- **Research 数据**:记录器心跳 + market.db 存在性
- **Var/Decibel**:VPS1 本机状态 + VPS2 peer 快照的四源权益、积分、仓位与交易量；
  已对账本金账本与五分钟净交易结果快照

占位待接(模板示例值,数据表/字段建成后接入):
- Single Account 战绩矩阵/权益曲线/持仓(需 strategy_daily/positions_closed/equity_snapshots——
  施工包01 已建表,记录器出数据后即可接)
- Predict.fun 风险闸门、打新核算台明细

**四源不混算**:PM 账号矩阵按 account_idx 逐个读各自 engine_state,不跨账号复制;
Var/Decibel 聚合接入 peer 快照时须逐 host 相加,不拿 VPS1 冒充 VPS2(见仓库审计结论)。
