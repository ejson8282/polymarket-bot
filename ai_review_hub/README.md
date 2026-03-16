# AI Review Hub (方案C: `/review`)

在 Discord 里用一个 slash command 触发多模型评审：

- Round1: 独立初答
- Round2: 互评冲突
- Final: 裁判汇总（共识 + 对比）

## 1) 安装

```bash
cd ai_review_hub
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS/Linux
# source .venv/bin/activate
pip install -r requirements.txt
```

## 2) 配置

复制 `.env.example` 为 `.env`，填写：

- `DISCORD_BOT_TOKEN`
- `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` / `GOOGLE_API_KEY`
- 频道 ID（结果频道、归档频道）

## 3) 运行

```bash
python review_bot.py
```

## 4) 在 Discord 使用

在服务器中输入：

`/review`

参数：
- issue
- goal
- constraints
- risk
- deadline (可选)

## 5) 说明

- 未配置某家 API key 时，该家会返回 `[SKIP] ... 未配置`，流程不中断。
- 当前实现优先“尽快可用”，后续可继续加：
  - 成本上限
  - 超时重试
  - 结果评分
  - 多线程并发优化
