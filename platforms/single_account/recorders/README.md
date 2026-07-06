# Single Account 数据记录器(施工包01 · 任务 A)

三个只读行情记录器,共用 `data/single_account_market.db`(SQLite,WAL):

| 模块 | 表 | 节奏 |
|---|---|---|
| `kline_recorder` | `klines` | 启动回补(默认 7 天)+ 每 60s 拉最近 3 根 |
| `funding_recorder` | `funding` | 每 300s 采样,按结算周期对齐 upsert |
| `rwa_basis_recorder` | `basis_ticks` | 每 5s;`rth` 品种仅美东盘中 |

端点与字段依据见 [ENDPOINTS.md](ENDPOINTS.md)。日志:`logs/recorder_<name>.jsonl`;心跳:`data/.recorder_<name>.heartbeat`。

## 前置:API key(必须)

Decibel 公开行情走 Aptos Labs 网关,**所有请求需要 Bearer key**(免费网关限流 key,非交易凭证)。部署方自行申请(Aptos Build / geomi.dev),通过环境变量注入:

```bash
export DECIBEL_API_BEARER=<你的key>      # 必需
export DECIBEL_ORIGIN=<你的来源域>        # 可选,文档要求 Origin 头时填
```

代码只读这两个环境变量,不读 `.env`,key 不会出现在日志/库/输出里。**没有 key 时**:进程照常运行(心跳持续、日志记 `auth_error` 并退避重试),但不落任何行情数据。

## 本地前台试跑

```bash
cd /home/ubuntu/polymarket-bot        # 或本仓库根目录
<venv>/bin/python -m platforms.single_account.recorders.kline_recorder --once   # 单轮验证
<venv>/bin/python -m platforms.single_account.recorders.kline_recorder          # 前台常驻
<venv>/bin/python -m platforms.single_account.recorders.funding_recorder
<venv>/bin/python -m platforms.single_account.recorders.rwa_basis_recorder
```

## systemd 安装(由运维手动执行,本仓库只提供文件)

服务文件在 `deploy/systemd/`:`sa-kline-recorder.service`、`sa-funding-recorder.service`、`sa-basis-recorder.service`。

```bash
# 1) 替换 venv python 占位符(路径按 VPS 实际情况改)
sudo cp deploy/systemd/sa-*.service /etc/systemd/system/
sudo sed -i 's|<VENV_PYTHON_REPLACE_ME>|/home/ubuntu/polymarket-bot/venv/bin/python|' /etc/systemd/system/sa-*.service

# 2) key 文件(自管,勿入 git;data/ 已在 .gitignore)
printf 'DECIBEL_API_BEARER=%s\n' '<你的key>' > /home/ubuntu/polymarket-bot/data/recorders.env
chmod 600 /home/ubuntu/polymarket-bot/data/recorders.env

# 3) 启动
sudo systemctl daemon-reload
sudo systemctl enable --now sa-kline-recorder sa-funding-recorder sa-basis-recorder

# 4) 查日志
journalctl -u sa-kline-recorder -f
tail -f logs/recorder_kline.jsonl
```

## 验证数据(A6 实录验收命令,配好 key 后执行)

```bash
DB=data/single_account_market.db
# 三表行数(跑 10 分钟后应持续增长;RTH 外 basis 允许只有 24h 品种)
sqlite3 $DB 'SELECT COUNT(*) FROM klines; SELECT COUNT(*) FROM funding; SELECT COUNT(*) FROM basis_ticks;'
# kill 重启幂等:重启前后各取一次,主键防重,不应出现重复行
sqlite3 $DB 'SELECT venue,symbol,tf,open_ts,COUNT(*) c FROM klines GROUP BY 1,2,3,4 HAVING c>1;'  # 应为空
# 心跳(应在刷新)
ls -l data/.recorder_*.heartbeat
# basis 统计
<venv>/bin/python -m platforms.single_account.recorders.basis_report --days 1
```

## 已知边界

- funding 历史:市场级历史端点不存在(账户级除外),故不回补,只从启动时刻开始积累。
- basis ref 价:默认 `yfinance_delayed`(延迟数据),够测 basis 水平/分布,不够测秒级滞后;AlpacaIEX 接口已留(TODO,下一批)。
- RTH 判断不含交易所节假日(TODO,下一批)。
