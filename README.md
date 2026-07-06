# Latitude Alpha Unified Dashboard

这个仓库是 Kevin 的生产系统统一入口。当前目标是把 Polymarket、Predict.fun、Var/Decibel、Single Account 策略草稿统一收口到同一个 Streamlit dashboard 中，同时保持各项目原有生产 worker、签名服务和密钥隔离。

## 当前模块

| 模块 | Dashboard 入口 | 生产/状态来源 |
| --- | --- | --- |
| Home | `Overview / Home` | 只读聚合各模块状态 |
| Polymarket | `Market Making / Polymarket` | `platforms/polymarket/maker`, `data/engine_state*.json` |
| Predict.fun | `Market Making / Predict.fun` | `dashboard/predictfun_view.py`, `data/predictfun_mainnet_*` |
| Var/Decibel | `Airdrop Farming / Var/Decibel` | 外部仓库 `/home/ubuntu/varia-decibel-farming-live` 的 dashboard 嵌入 |
| Single Account | `Automated Trading / Single Account` | `platforms/single_account`, `data/single_account_paper_state.json` |
| Research Data | Home 只读卡片 | 预留为数据采集、回测、报告入口 |
| HK/US Accounts | Home 只读卡片 | 预留为港股/美股账户管理入口 |

## 安全边界

1. Dashboard 先做只读聚合和清晰入口，不迁移私钥，不把 key 写入 git、日志、聊天或页面。
2. Mac mini signer、VPS systemd worker、交易执行脚本继续由各自项目控制。
3. 新增按钮必须调用已有 wrapper，不直接拼交易命令。
4. Live 交易能力必须保留日志、Discord 通知、风控和人工可见状态。
5. Var/Decibel、Polymarket、Predict.fun、Single Account 是不同业务线，不应混用配置或密钥。

## 运行

在 VPS 上：

```bash
cd /home/ubuntu/polymarket-bot
streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
```

如果 Var/Decibel 仓库不在默认位置，可设置：

```bash
export VAR_DECIBEL_HEDGE_BOT_DIR=/home/ubuntu/varia-decibel-farming-live
```

## 迁移阶段

1. 已完成：统一 Home 总览、保留原有项目页面、补 README。
2. 下一步：把各模块状态字段标准化，例如 running、last_update、equity、open_positions、last_error。
3. 再下一步：只接安全 wrapper 按钮，例如启动/停止 worker、刷新状态、导出报告。
4. 最后：把 Research 数据、单账号策略回测、港股/美股账户管理做成独立页面。

## 给接手 AI 的规则

- 先读本 README，再读 `dashboard/app.py`。
- 修改 UI 前先确认不影响生产 worker。
- 不读取、不输出、不复制任何私钥或 API secret。
- Git 只提交明确相关文件，不把本地 `CLAUDE.md`、`.env`、`data/*.db`、日志和状态文件误提交。
