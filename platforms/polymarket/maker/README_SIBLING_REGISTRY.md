# SiblingOrderRegistry · 跨账号自成交防线(施工包04)

## 是什么

multi_runner 单进程跑全部账号,但每个 engine 只识别自家订单:A 的 exit-SELL 可能被 B 的 BID 吃掉 = 跨账号自成交(双倍手续费 + 对敲形态风险)。本防线在**订单发出前**做一道进程内交叉检查,不改任何报价/exit 定价/风控参数。**扩到 10 账号前的必做项。**

## 工作方式

- `multi_runner` 创建单例 `SiblingOrderRegistry` 注入全部 engine(engine 单跑时自建空实例,行为不变)。
- 两个下单出口挂钩:BUY 统一收口 `_submit_post_order`、exit-SELL `_place_sell_order`;下单成功即登记。
- 生命周期同步与引擎本地清单同点:`_refresh_live_orders` 整体对账、`_cancel_order_ids`/各 guard 撤单点剔除、cancel-all 族清空(`_cancel_all_except_exit` 保护 exit 单)、关停清空。
- 配对 token(Yes/No)互补 BUY:v1 **只观察**(`sibling_complement_observe` 日志与计数),不拦截。

## 配置(engine 的 config_N.json 新增段;缺省即以下默认值)

```json
"sibling_registry": {
  "enabled": true,
  "mode": "observe",
  "adjust_ticks": 1
}
```

| mode | 行为 |
|---|---|
| `observe` | 只记日志与计数,**不改变任何交易行为**(默认) |
| `adjust` | 向不交叉方向退 `adjust_ticks` 档;买单退让后低于报价约束下限(mid−spread 口径)或快照不可得 → 跳过本次挂单(等同 block) |
| `block` | 直接跳过本次挂单 |

注意:adjust/block 同样作用于 exit-SELL——若与兄弟 BID 冲突,exit 会被退档/推迟(由重试循环再试)。observe 模式无此影响。

## 部署流程(重要,按顺序)

1. **首次上线保持默认 `observe` 模式跑 24h**:只统计会发生的冲突,不改变任何交易行为;
2. 24h 后审阅统计:各账号 `data/engine_state_N.json` 的 `sibling_registry` 键(checked / conflicts_detected / adjusted / skipped / complement_observed / live_orders),以及引擎日志中的 `[sibling_conflict]` / `[sibling_stats]` 行;
3. 由 Kevin 审阅统计后决定是否把 config 切到 `"mode": "adjust"` 并重启 multi_runner;
4. `complement_observed`(配对 token 互补 BUY)本期只看规模,数据说话后再决定二期是否处理。

## 可观测

- `engine_state_N.json` → `sibling_registry` 统计键(state 写循环随写随更新);
- 引擎日志:`[sibling_conflict]`(每次冲突,含双方 funder 短址/token/价格)、`[sibling_adjust]`、`[sibling_skip]`、`[sibling_complement_observe]`、`[sibling_stats]`(计数变化时)。

## 测试

`tests/test_sibling_registry.py` 7 例(全 mock,不连交易所):直接交叉/adjust/block/observe/生命周期/线程安全/配对观察。
