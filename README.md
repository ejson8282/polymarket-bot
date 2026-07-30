# Polymarket Bot

Polymarket 实盘做市、奖励观测、候选筛选、账户状态和风控仓库。

> **Live domain。** `main` 是开发和发布权威来源，但合并不等于部署。任何会影响真实订单、仓位、
> 风控、签名或服务启停的变更都必须经过测试、PR 和明确部署授权。

## 仓库边界

本仓库负责：

- `platforms/polymarket/maker/` 下的做市、奖励、候选筛选和 fail-closed 风控；
- Polymarket 账户状态、观察器和受控运行脚本；
- 与 Polymarket 业务直接相关的测试和 systemd 模板；
- Predict.fun 与共用 shadow/research 内核中的既有代码，迁移完成前保持兼容。

本仓库不再负责：

- Latitude 统一 Dashboard 页面或跨项目协调；
- Var/Decibel、网格、港股账户运营等其他业务域的交易实现；
- 运行数据、账户配置、密钥、日志、数据库、备份或机器级私有配置。

统一 Dashboard 的唯一源码是
[`ejson8282/latitude-alpha`](https://github.com/ejson8282/latitude-alpha)。
本仓库中的 `deploy/latitude-console/` 和旧 `dashboard/` 页面属于历史兼容副本，禁止继续开发或
直接部署。Dashboard 需要新的 Polymarket 字段时，应先在本仓库定义稳定的状态/API 契约，再在
`latitude-alpha` 中实现展示。

## 正确工作流

1. 从最新 GitHub `main` 建 `agent/<scope>` 独立分支或 worktree。
2. 修改前核对开放 PR、运行服务实际路径、Git 状态和目标文件哈希。
3. 一个 PR 只处理一个明确范围；显式暂存相关文件。
4. 运行与改动匹配的测试，交易路径必须覆盖风控和失败关闭。
5. PR 审查并合并后，从精确 commit SHA 建 release；生产目录不得直接开发。
6. 发布后记录 repo、SHA、配置摘要、时间、健康检查和回滚点。

禁止在生产运行目录使用 `git pull`、`reset`、`checkout`、`git clean` 或整目录覆盖。生产出现
差异时，先只读保存、分类，再把有效代码移植到新分支。

更完整的 AI 规则见 [AGENTS.md](AGENTS.md)，当前生产收口状态见
[docs/PRODUCTION_CONVERGENCE.md](docs/PRODUCTION_CONVERGENCE.md)。

## 安全边界

- 不读取、输出或提交私钥、助记词、API key、Cookies、账户配置和 webhook。
- 新增交易能力不得绕过限额、余额检查、盘口新鲜度、部分成交对账或安全暂停。
- `LIVE`、`PAPER`、`SHADOW`、`RESEARCH` 必须明确区分。
- Service active 只代表进程存在，不代表账户健康、订单有效或策略盈利。

## 验证入口

按改动范围运行对应测试；常见入口：

```bash
python -m pytest -q tests/test_polymarket_maker_engine.py
python -m pytest -q tests/test_polymarket_auto_curator.py
python -m pytest -q tests/test_polymarket_reward_observer.py
python -m pytest -q tests/test_polymarket_sponsored_guard.py
```

Rust shadow 内核的检查由 `.github/workflows/rust-maker.yml` 执行。
