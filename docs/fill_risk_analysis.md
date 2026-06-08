## Polymarket 做市机器人 — 被动成交风险分析

### 项目背景

我在 Polymarket（链上预测市场）上运行一个做市机器人，策略是：在 reward zone 内挂 BUY 限价单赚取流动性激励（rewards），**不希望被成交**。一旦被成交（fill），需要挂 SELL 单平仓退出。

### 市场特性

- Polymarket 使用 CLOB（中央限价订单簿），tick size 有两种：0.01 和 0.001
- Taker 可以发 market sell 一次穿透多档深度
- 所有操作通过 REST API 轮询，没有 WebSocket 推送

### 当前挂单策略

**定价逻辑（`_build_price_legs`）：**
- 从 API 拉取当前 `best_bid` / `best_ask`
- 挂单价格范围：`[mid - spread, best_bid - 1tick]`
- 永远不挂在 best_bid 上，最高挂到 `best_bid - 1tick`
- tick=0.01 市场最多 3 腿，tick=0.001 市场最多 5 腿
- 挂单前检查 `front_bid_notional`：我的挂单价以上的深度（前方 bid 的总名义金额），低于阈值（默认 $2000）则跳过该价位

**例子（tick=0.01）：**
```
best_bid = 0.30, spread = 0.04, mid = 0.305
reward_lower = 0.305 - 0.04 = 0.265

挂单：
  leg1: 0.29  (best_bid - 1tick)
  leg2: 0.28  (best_bid - 2tick)
  leg3: 0.27  (best_bid - 3tick)
```

**Reprice 机制：**
- 每轮报价循环（requote_interval_ms=500ms，但实际遍历 20+ 个 token 一轮要 10-30 秒）
- 计算新的 `plan_sig`（价格:数量 的拼接字符串），如果和上次一样且 live 订单数足够 → 跳过
- 如果 plan_sig 变了 → 取消该 token 全部旧单，重新挂全部新单

### 当前防护机制

**`best_bid_guard_loop`（后台独立循环）：**
- 外循环每 2 秒跑一次，拉取全部 live orders
- 对每个 token，每 10 秒检查一次订单簿
- 如果发现我的挂单价格 >= 当前 best_bid → 立刻取消，并触发重新报价

### 仍然会被成交的风险场景

**场景 1：best_bid 被吃/撤，我的单变成 best_bid（10 秒暴露窗口）**

```
T=0s: best_bid=0.30，我的最高挂单=0.29（安全）
T=1s: 0.30 被吃掉/撤掉，现在我的 0.29 就是 best_bid
T=1~10s: guard loop 还没轮到这个 token，10 秒内我是最优 bid
       → 任何 taker 的 market sell 直接成交我
T=11s: guard loop 检测到，取消（但可能已经晚了）
```

**场景 2：大单穿透多档深度**

```
best_bid=0.30 深度只有 $100
我的 0.29 挂了 $500

一个 taker 发 $600 的 market sell：
  → 先吃掉 0.30 的 $100
  → 穿透到 0.29，吃掉我的 $500
```

`front_bid_notional` 只在挂单时检查一次（阈值 $2000），不会持续监控。如果挂单后前面的深度被撤走/吃掉，我不知道。

**场景 3：tick=0.001 市场的"夹层"风险**

tick=0.001 时 best_bid=0.295，我挂在 0.294。看似安全，但 0.295 上可能只有很薄的深度，一个小单就穿到我。而且 guard 只检查 `>= best_bid`，不检查"前面深度是否变薄"。

### 当前参数汇总

| 参数 | 值 | 说明 |
|---|---|---|
| safe_top | best_bid - 1tick | 最高挂单价 |
| guard 外循环 | 2s | 拉取全部 orders |
| guard per-token 检查 | 10s | 每 token 的订单簿拉取间隔 |
| requote_interval_ms | 500ms | 报价节流（但实际一轮远大于此） |
| front_bid_notional 阈值 | $2000 | 挂单时一次性检查 |
| max legs | 3（tick=0.01）/ 5（tick=0.001） | — |

### 我想解决的问题

在不大幅增加 API 调用量的前提下，如何降低被动成交的概率？可能的方向包括但不限于：
- 缩短 guard 检查间隔
- 增大安全距离（safe_top 从 -1tick 改为 -2tick）
- 持续监控前方深度（不只挂单时查一次）
- 波动率感知（market 快速移动时暂停或加大距离）
- 更根本的架构改动（WebSocket？）
