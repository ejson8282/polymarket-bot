# Polymarket Bot — AI 协作规则

本文件适用于整个仓库。开始工作前先读 `README.md` 和
`docs/PRODUCTION_CONVERGENCE.md`。

## 权威来源与边界

- GitHub `ejson8282/polymarket-bot` 的最新 `main` 是本业务域的代码权威来源。
- 本仓库只负责 Polymarket 交易域及现存的 Predict.fun/shadow 兼容代码。
- Latitude 统一 Dashboard 的唯一源码是 `ejson8282/latitude-alpha`。
- 禁止修改或部署本仓库中的历史 `deploy/latitude-console/` 和旧 Dashboard 页面副本。

## 强制流程

1. 从最新 GitHub `main` 创建 `agent/<scope>` 分支或独立 worktree。
2. 修改前只读核对开放 PR、生产服务路径、目标 SHA、`git status` 和目标文件哈希。
3. 不在生产运行目录执行 `pull`、`reset`、`checkout`、`clean`、merge 或整目录同步。
4. 一个 PR 只处理一个明确范围；显式暂存文件，不提交顺手修改。
5. 合并后只从精确 SHA 构建 release；记录配置摘要、部署时间、健康检查和回滚点。
6. 生产 worktree 有漂移时，先保存清单和哈希，再分类移植；不得把 dirty 内容整体回传。

## 实盘安全

- 未经用户对具体范围明确授权，不部署、不重启、不启停服务、不下单、不撤单、不平仓。
- 交易、签名、仓位、风控、余额、服务控制和写接口变更必须走 PR 并测试。
- 任何数据源缺失、过期、异常或不一致都应 fail closed；不能用另一账户或旧快照补成实时值。
- 不得把回测、PAPER、SHADOW 或 RESEARCH 结果描述成 LIVE 结论。

## 秘密与运行文件

禁止读取、输出或提交：

- `.env*`、私钥、助记词、API key、Cookies、webhook 和账户配置；
- 数据库、日志、PID、状态快照、浏览器资料、OpenClaw 会话；
- `.bak*`、candidate、`._*`、临时 patch、机器生成的服务配置。

备份和运行数据必须放在源码仓库外。需要恢复历史时，使用带 SHA-256 清单的外部归档。

## 验证

- 运行与改动匹配的 Python/Rust 测试和静态检查。
- Maker 变更至少覆盖限额、余额、报价新鲜度、部分成交、撤单和安全暂停。
- 缺少依赖或无法执行测试时必须如实记录，不得把“未运行”写成“通过”。
