CREATE TABLE IF NOT EXISTS orders(
  order_id TEXT PRIMARY KEY, strategy TEXT, symbol TEXT,
  side TEXT CHECK(side IN ('buy','sell')),
  type TEXT CHECK(type IN ('market','limit')),
  qty REAL, limit_price REAL, created_ts INTEGER,
  status TEXT CHECK(status IN ('new','filled','canceled','rejected')),
  reason TEXT);
CREATE TABLE IF NOT EXISTS fills(
  fill_id TEXT PRIMARY KEY, order_id TEXT, ts INTEGER,
  price REAL, qty REAL, fee REAL, slippage_bps REAL);
CREATE TABLE IF NOT EXISTS positions_closed(
  id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT, symbol TEXT, side TEXT,
  qty REAL, entry_ts INTEGER, entry_price REAL, exit_ts INTEGER, exit_price REAL,
  gross_pnl REAL, fees REAL, funding REAL, net_pnl REAL,
  r_multiple REAL, exit_reason TEXT, holding_secs INTEGER, tags_json TEXT);
CREATE TABLE IF NOT EXISTS equity_snapshots(
  ts INTEGER PRIMARY KEY, equity REAL, cash REAL, unrealized REAL, drawdown REAL);
CREATE TABLE IF NOT EXISTS funding_events(
  ts INTEGER, symbol TEXT, rate REAL, pos_qty REAL, amount REAL);
CREATE TABLE IF NOT EXISTS strategy_daily(
  date TEXT, strategy TEXT, trades INTEGER, wins INTEGER,
  gross REAL, fees REAL, funding REAL, net REAL, mdd_intraday REAL,
  PRIMARY KEY(date, strategy));
CREATE TABLE IF NOT EXISTS decisions(
  ts INTEGER, strategy TEXT, symbol TEXT, action TEXT,
  score_json TEXT, taken INTEGER, skip_reason TEXT);
CREATE TABLE IF NOT EXISTS sim_meta(key TEXT PRIMARY KEY, value TEXT);  -- ambiguous_bars 计数等
