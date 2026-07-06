# Decibel 公开行情端点侦察结论(施工包01 · A0)

侦察来源:
1. 只读参考仓库 `varia-decibel-farming` 的 `src/exchanges/decibel.py`(客户端封装、字段兼容写法);
2. 官方文档 docs.decibel.trade(REST overview / market-data 各端点页,2026-07-05 抓取);
3. 实测:mainnet 与 testnet 匿名 GET 均返回 `HTTP 401 Unauthorized`("anonymous requests are not allowed")。

## 基础信息

- Base URL(mainnet):`https://api.mainnet.aptoslabs.com/decibel`
- REST root:`/api/v1`(完整根 = `https://api.mainnet.aptoslabs.com/decibel/api/v1`)
- 鉴权:**所有端点**要求 `Authorization: Bearer <key>`(Aptos Build / Geomi 签发的网关限流 key,非交易凭证);文档另要求 `Origin` 头。
  - 本仓库实现:key 只从环境变量 `DECIBEL_API_BEARER` 读取(兼容 varia 的命名),`Origin` 从 `DECIBEL_ORIGIN` 读取(可缺省);代码**不读 `.env`、不打印 key**。
  - 无 key 时:recorder 正常启动,每轮请求 401 → 记 `auth_error` 日志 → 退避重试,不退出、不落行情数据。实录验收推迟到部署方配好 key(见 README)。

## 端点清单(已确认)

### GET /api/v1/markets

- 参数:无。
- 关键响应字段(varia 解析逻辑确认):列表项含 `market_addr`(市场地址,candlesticks 的 `market` 参数用它)与市场名字段(`symbol` / `market` / `marketName` / `market_name` / `name` 之一)。
- 符号命名变体(varia `_symbol_names`):`BTC`、`BTC-USD`、`BTC/USD`、`BTC-PERP`。

### GET /api/v1/prices

- 参数:`market`(可省略或 `all` = 全部市场)。
- 响应:`PriceDto` 数组,字段:
  `market`(市场地址)、`mark_px`、`oracle_px`(index)、`mid_px`、
  `funding_rate_bps`(**小时**资金费率,bps)、`is_funding_positive`(方向)、
  `funding_period_s`(结算周期秒数)、`transaction_unix_ms`、`open_interest`。

### GET /api/v1/candlesticks

- 参数:`market`(**市场地址**,必填)、`interval`(必填,支持 `1m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d, 3d, 1w, 1mo`)、`startTime` / `endTime`(必填,毫秒)、`filterWicks` / `nSigma`(可选,不使用)。
- 响应:数组,每项 `{t(开盘ms), T(收盘ms), o, h, l, c, v, i(interval)}`;单次最多 1000 根。
- **结论:K线端点存在,支持历史回补 → 不启用"5s 采样本地聚合"兜底方案。**
- 未收盘K线的处理:**选择"存且持续更新最后一根"**——每轮拉最近 N=3 根全部 upsert,未收盘那根随行情被 `INSERT OR REPLACE` 反复更新,收盘后自然定格。

### GET /api/v1/orderbook

- 参数:`ticker_id`(如 `BTC-PERP`,必填)。
- 响应:`{ticker_id, timestamp(ms), bids: [[price, qty]...], asks: [[price, qty]...]}`,价格/数量为字符串,bids 降序、asks 升序,每侧最多 50 档。basis recorder 取首档为 best bid/ask。

### GET /api/v1/funding_rate_history

- 参数:`account` **必填**(用户账户地址)→ **该端点是账户级(个人已实现资金费流水),不存在市场级资金费历史端点**。
- 结论:A3 规格"若端点提供历史 funding,启动时回补 30 天"条件**不满足**,funding recorder 只做轮询采样,不回补。

## funding 数值换算约定(funding_recorder 落库口径)

- `rate` = `funding_rate_bps / 10000`,符号由 `is_funding_positive` 决定(正 = 多头支付),为**小时**费率。
- `interval_hours` = `funding_period_s / 3600`。
- `ts` = 抓取时刻按 `funding_period_s` 向下对齐的结算周期起点(epoch 秒),保证同一周期内多次轮询 upsert 成同一行。
- `predicted_next` = NULL(端点不提供下期预测)。
