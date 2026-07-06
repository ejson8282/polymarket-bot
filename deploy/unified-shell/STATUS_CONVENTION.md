# status.json 字段约定(统一导航壳 · 施工包05·5C)

各系统按本 schema 输出 `status.json`(文件或 HTTP 端点均可),总览聚合器
`overview_app.py` 消费。**所有字段可缺省——聚合器必须容错**;系统侧后续按此
逐步补齐即可,缺字段只影响该卡片展示的丰富度,不影响在线判定。

## Schema(v1)

```json
{
  "name": "Var/Decibel",                 // 系统名(可省,聚合器配置里有)
  "version": 1,                          // schema 版本
  "ts": "2026-07-06T04:00:00Z",          // 状态生成时间,ISO8601 UTC
  "ok": true,                            // 系统自评健康(可省;在线判定以「能拉到」为准)
  "mode": "live",                        // live | paper | dry_run | testnet
  "summary": {                           // 项目卡 KPI(展示前 5 个键值,字符串/数字均可)
    "总权益": "$2,418.62",
    "今日净损耗": "-$1.84",
    "今日交易量": "$8,720",
    "自动化": "运行中"
  },
  "alerts": [                            // 活跃告警(卡片上显示「告警 N」)
    {"severity": "critical", "message": "VPS2 SOL 单腿", "ts": "2026-07-06T03:58:00Z"}
  ],
  "events": [                            // 近期事件(并入合并事件流,建议 ≤20 条)
    {"ts": "2026-07-06T03:58:00Z", "severity": "critical",
     "kind": "single_leg_risk", "message": "VPS2 · SOL 单腿,Decibel 空腿裸露 $88"}
  ]
}
```

## 字段规则

- `ts` / `events[].ts`:ISO8601,统一 UTC(带 Z);聚合器按字符串倒序排合并流。
- `severity` 取值:`info` | `warning`(或 warn)| `critical`(或 error/crit);
  未知值按 info 处理。
- `summary` 值直接原样展示(系统侧自行格式化货币/百分比),不做二次计算。
- 聚合器「离线」判定:文件读不到 / URL 3s 超时或非 2xx / JSON 解析失败
  → 灰卡 + error 摘要;**绝不因单系统故障影响其他卡片**。

## 各系统当前可用的状态源(补齐 status.json 前的过渡)

| 系统 | 现有文件 | 备注 |
|---|---|---|
| Var/Decibel(varia) | `data/ops_state.json` | 字段不同,建议由 ops 循环另写 status.json |
| Polymarket(pmbot) | `data/engine_state_N.json` | 同上;多账号可聚合成一份 |
| Predict.fun | `data/predictfun_mainnet_runner_state.json` | dry-run 状态 |
| Single Account | `data/single_account_paper_state.json` | 已含 summary 键,最接近本约定 |
| 港股核算台 | 自带 API | 直接暴露一个 /status 端点即可 |
