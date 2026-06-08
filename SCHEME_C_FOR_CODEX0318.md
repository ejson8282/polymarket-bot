# 方案 C 改造说明（发给 Codex）

## 背景
当前项目已经具备 scanner / planner / execution / 多 event runner 的基础框架。现在不做大平台化，不做 Redis bus / 多 worker / 独立 signer server（这些属于方案 D / Later）。

当前要做的是：

> 在保留现有 scanner 框架的前提下，把执行层从“慢速 planner 驱动整组撤挂”升级为“market-driven top leg defensive loop + user WS fill halt + event state machine + execution 前风险闸门”。

目标不是架构漂亮，而是：
- 降低 fill rate
- 保住 rewards-only 场景下的 rewards capture
- 避免 top leg 被慢 planner / 整组重建拖死
- 避免 fill 后被自动补挂逻辑打穿

---

## 一、总体要求

### 保留
- 现有 scanner / reward market discovery
- 现有基本 planner / execution 能力
- 现有多 event / runner 基础

### 当前不要优先做
- Redis 行情总线
- 多 execution worker
- 独立 signer server 平台化
- 多账号统一总线
- 为“架构漂亮”做大拆大改

### 当前主矛盾
不是“发现市场不够快”，而是：
1. top leg 仍被 planner 的整组重建逻辑绑定
2. market 风险变化后不能快速局部防守
3. fill 后缺少硬 halt 语义
4. 自动补挂/runner 可能绕过防守状态
5. 系统缺少 kill path latency 数据，无法知道普通市场到底能不能做

---

## 二、这次改造的正确理解

### 不是简单“筛掉普通市场”
我们已经讨论过：
- 好市场当然更容易做
- 但普通市场并不等于不能做
- 市场一般，只要监控足够快、撤单足够快、仓位更小、阈值更保守，仍然可以做

所以这次要实现的不是“静态市场筛选器”，而是：

> 市场条件 × 当前系统能力 的动态匹配执行系统

也就是 execution 层要回答：
- 这个 market 当前能不能做
- 如果能做，允许挂多大
- top leg 应该 keep / move-back / cancel / halt 哪一种
- 当前系统 latency 是否足以支撑这个 market 的风险结构

---

## 三、top leg 的业务定义

### 定义
top leg = 当前整组报价中：
- 最靠近 touch
- 最容易成交
- 风险最高
的那一档单

### 为什么要拆出来
当前如果还是：
- market 条件变化 -> planner 周期触发 -> 整组撤挂 / 重建

会导致：
- 空窗
- 排队位置清零
- 本来只需要局部后撤，却升级成整组重建
- 对 rewards-only 非常伤

### 目标
把 top leg 从整组 plan_sig / planner 里拆出来，做成独立的快速防守对象：
- 可单独 keep
- 可单独 move-back
- 可单独 cancel
- 必要时触发 event halt

---

## 四、top leg 风险判断：不要只看深度

### 关键共识
“厚”不能只看绝对深度。
必须联合看：
1. 绝对深度
2. 相对深度（front depth / my order size）
3. tick 距离
4. 稳定性
5. 衰减速度
6. 数据可信度
7. 当前系统 kill path latency

### 举例
如果某价位前面挂着 220 万，而我只挂 1000U，这当然是好信号。  
但如果这 220 万在快速衰减，或者数据 stale / integrity bad，仍然不安全。

### 正确理解
安全不是盘口天然给的，而是：

> 盘口条件 + 你的系统反应能力 共同决定的

所以 execution 里不能只判断“market 好不好”，还要判断：

> 以当前系统的监控/决策/签名/发送/撤单确认速度，是否足以在这个 market 里安全留单

---

## 五、需要实现的四个核心模块

# 模块 A：Quote Gate / Feasibility Gate

## 作用
scanner 找到候选 market 后，不直接让 planner 下单。  
先经过一个 execution 前风险闸门，决定：
- can_quote
- size_cap
- top_leg_action
- risk_grade
- reason

## 输入
至少应接入：
- reward eligibility
- best bid / ask
- ticks_to_touch
- front_depth_ahead
- depth_vs_my_size
- depth_stability
- depth_decay_rate
- book freshness
- book integrity / placeholder suspicion
- current local capability snapshot（来自 latency metrics）

## 输出建议
```python
{
  "can_quote": bool,
  "size_cap": float,
  "top_leg_action": "keep|move_back|cancel|halt",
  "risk_grade": "A|B|C|BLOCK",
  "reason": [...],
}
```

## 业务规则
不是简单做“好 market / 坏 market”二分类。  
而是：
- A：可以正常小中仓做
- B：可以做，但更小仓、更保守阈值
- C：仅极小测试仓，强防守
- BLOCK：当前系统能力不匹配，禁止挂

---

# 模块 B：Top Leg Defensive Loop

## 作用
将 top leg 的防守从 planner 周期逻辑中剥离，改成 market WS 直接驱动。

## 正确链路
```text
market_ws_update -> normalize -> top_leg_risk_eval -> defense_action
```

## 错误链路（不要这样）
```text
market_ws_update -> 更新缓存 -> 等 planner 下一个周期
```

## 第一版只需要支持的动作
- KEEP
- MOVE_BACK_TOP_LEG
- CANCEL_TOP_LEG
- HALT_EVENT

## 判定原则
allow_quote 不能只看 reward band。建议落实成：

```text
allow_quote =
in_reward_band
AND ticks_to_touch >= dynamic_min_tick
AND front_depth_ahead >= min_depth_buffer
AND depth_decay_rate <= max_decay
AND book_freshness <= freshness_threshold
AND book_integrity == healthy
AND current_kill_path_latency <= allowed_budget
```

## 一个重要业务语义
市场一般，不代表不能做；  
只是普通市场要求：
- 更小 size
- 更快风险监控
- 更硬 move-back/cancel
- 更低阈值进入 defensive

---

# 模块 C：User WS Fill Halt

## 作用
建立真正硬的止血机制。

## 第一版必须实现
- partial fill -> HALTED_ON_FILL
- full fill -> HALTED_ON_FILL

## 行为要求
进入 HALTED_ON_FILL 后：
- 立即阻止自动补挂
- planner/runner 不得绕过
- 只能走显式恢复流程，不能自动恢复

## 注意
user WS 不是主防守路径，而是最后止血线。  
主防守仍然是 market-driven top leg defense。

---

# 模块 D：Event State Machine

## 作用
把 defense / halt / canceling 的语义做成硬门禁，防止系统被旧逻辑打穿。

## 最少状态
- ACTIVE
- DEFENSIVE
- CANCELING
- HALTED_ON_FILL
- HALTED_ON_DATA
- COOLDOWN

## 强规则
1. HALTED_ON_FILL / HALTED_ON_DATA 下，planner 不得自动补挂
2. 进入 CANCELING 后，未确认 orders cleared 前，不得视为恢复
3. stale / integrity bad / 状态不一致 / cancel ack timeout 时，应进入保护态
4. defensive / halt 的优先级高于普通 requote/planner 周期

---

## 六、需要落的延迟指标（必须做）

当前阶段可以先用日志，不一定上完整 dashboard。  
但必须能量出关键 kill path。

### 至少记录
- t_detect
- t_decision
- t_sign_start
- t_sign_done
- t_send
- t_exchange_accept
- t_cancel_ack
- t_fill_seen
- t_halt_entered
- t_orders_cleared

### 至少能算
- detect -> decision
- decision -> sign
- sign -> send
- send -> ack
- fill seen -> halt entered
- halt entered -> orders cleared

### 为什么必须做
因为后续你要判断：
- 这种一般市场能不能做
- 哪些 market 只能小仓做
- 当前系统 latency 是否匹配当前盘口风险

没有这些指标，就只能靠感觉争论。

---

## 七、按现有项目结构的改造建议

### `scanner.py`
保留 scanner 主体。  
继续负责：
- reward market discovery
- candidate event / market 输出
- 基础 market ranking

可以考虑补一些粗粒度 market facts 输出：
- best bid/ask snapshot
- reward band 基础信息
- top-of-book 粗摘要
- 基础 freshness

不要把 top leg 防守 / fill halt / 状态机塞进 scanner。

---

### `engine.py`
这是本次主要改造位置。

需要新增：
1. quote gate / feasibility gate
2. top leg defensive loop
3. 和 state machine 的强耦合点
4. market WS 驱动的即时防守动作

需要做到：
- top leg 不再必须跟随整组重建
- planner 继续管理整体结构/后排，不独占前排风险动作
- market 变化时，允许局部 move-back / cancel，而不是强制整组撤挂

---

### `multi_runner.py`
需要确保 runner 尊重 event state：
- halt 后不再继续自动调度该 event
- defensive / canceling 优先级高于普通周期任务
- 不允许旧任务把 halt event 又拉起来

---

### `WS handlers`
#### market WS
应直接驱动 top leg 风险评估。  
维护：
- best bid/ask
- ticks_to_touch
- front depth
- depth decay
- freshness
- integrity

#### user WS
先完成最小闭环：
- partial/full fill -> halt

---

### `metrics`
如果已有 metrics 模块，接进去。  
如果没有，先落结构化日志也可以。  
重点不是面板，而是先把 kill path 时间量出来。

---

## 八、推荐开发顺序（严格按顺序）

### Step 1
先补 event state machine  
把 halt / defensive 语义做硬。

### Step 2
接 user WS -> fill halt  
先建立止血能力。

### Step 3
把 top leg 从 planner 里拆出来  
至少支持独立 cancel / move-back。

### Step 4
接 market WS -> top leg defensive loop  
让 market event 直接驱动防守。

### Step 5
加 quote gate / feasibility gate  
把 market 条件 × 系统能力匹配纳入执行前判断。

### Step 6
埋延迟指标  
开始拿真实 kill path 数据。

### Step 7
小流量灰度  
只在单账号、单事件、低仓位下验证：
- fill rate 是否下降
- rewards capture 是否还可接受

---

## 九、验收标准（第一阶段）

满足以下条件即可认为第一阶段完成：
1. top leg 不再必须跟随整组重建
2. market WS 能直接触发 top leg 防守动作
3. partial/full fill 能立即触发 event halt
4. halt 不会被 planner / runner 自动补挂打穿
5. 日志或 metrics 可以看到 kill path 关键延迟
6. 单账号低流量灰度下：
   - fill rate 有下降
   - rewards capture 仍在可接受范围

---

## 十、明确避免的错误方向

### 不要这样做
- 只接 market WS，但继续等 planner 周期处理
- 只做 fill 检测，不做 hard halt
- 把 halt 当成“建议状态”而不是门禁
- 没测 latency 就宣称普通市场也能安全做
- 先做 bus / signer / multi-worker，再回头补主防守链路

---

## 十一、最终一句话
本次改造不是重做系统，而是：

> 保留 scanner，把 execution 层升级为：有 quote gate、有 market-driven top leg 防守、有 user WS fill halt、有 event state machine 硬门禁，并且能根据 market 条件与当前系统能力动态决定是否挂单与挂多大。
