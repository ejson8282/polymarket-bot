CREATE TABLE IF NOT EXISTS klines(
  venue TEXT, symbol TEXT, tf TEXT, open_ts INTEGER,
  open REAL, high REAL, low REAL, close REAL, volume REAL,
  PRIMARY KEY(venue, symbol, tf, open_ts));
CREATE TABLE IF NOT EXISTS funding(
  venue TEXT, symbol TEXT, ts INTEGER,
  rate REAL, interval_hours REAL, predicted_next REAL,
  PRIMARY KEY(venue, symbol, ts));
CREATE TABLE IF NOT EXISTS basis_ticks(
  ts INTEGER, venue TEXT, symbol TEXT,
  platform_mark REAL, platform_bid REAL, platform_ask REAL, platform_index REAL,
  ref_price REAL, ref_ts INTEGER, ref_source TEXT,
  PRIMARY KEY(ts, venue, symbol));
CREATE TABLE IF NOT EXISTS recorder_meta(key TEXT PRIMARY KEY, value TEXT);
