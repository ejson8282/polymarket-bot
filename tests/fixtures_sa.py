"""施工包01 · §B8 手工K线夹具与共用工具。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from platforms.single_account.recorders.common import open_market_db, upsert_klines
from platforms.single_account.sim.orders import Bar
from platforms.single_account.sim.persistence import open_paper_db

BASE_TS = 1_760_000_000 - (1_760_000_000 % 900)  # 15m 对齐
TF = "15m"
SYMBOL = "BTC"
VENUE = "decibel"

# 手工数列:含突破(idx7 上破)、跳空(idx12 开盘远高于前收)、深跌(idx16)
_OHLC = [
    # (open, high, low, close)
    (100.0, 101.0, 99.0, 100.5),
    (100.5, 101.5, 99.5, 101.0),
    (101.0, 101.8, 100.2, 101.2),
    (101.2, 102.0, 100.5, 100.8),
    (100.8, 101.5, 100.0, 101.1),
    (101.1, 102.2, 100.9, 102.0),
    (102.0, 102.5, 101.0, 101.5),
    (101.5, 105.5, 101.3, 105.0),   # 突破
    (105.0, 106.0, 104.0, 105.5),
    (105.5, 106.5, 104.8, 106.0),
    (106.0, 106.8, 105.2, 105.8),
    (105.8, 106.2, 104.9, 105.1),
    (108.0, 109.0, 107.0, 108.5),   # 跳空高开
    (108.5, 109.5, 107.5, 108.0),
    (108.0, 108.8, 106.9, 107.2),
    (107.2, 107.9, 106.0, 106.5),
    (106.5, 106.9, 101.0, 101.8),   # 深跌
    (101.8, 103.0, 101.2, 102.5),
    (102.5, 103.5, 102.0, 103.0),
    (103.0, 104.0, 102.6, 103.8),
    (103.8, 104.5, 103.0, 104.0),
    (104.0, 104.8, 103.4, 104.5),
    (104.5, 105.2, 103.9, 104.8),
    (104.8, 105.5, 104.2, 105.0),
    (105.0, 105.8, 104.5, 105.5),
    (105.5, 106.2, 104.9, 105.8),
    (105.8, 106.5, 105.1, 106.0),
    (106.0, 106.8, 105.4, 106.3),
    (106.3, 107.0, 105.7, 106.6),
    (106.6, 107.2, 106.0, 106.9),
]

BARS_A = [
    Bar(symbol=SYMBOL, tf=TF, open_ts=BASE_TS + i * 900,
        open=o, high=h, low=l, close=c, volume=10.0)
    for i, (o, h, l, c) in enumerate(_OHLC)
]

# 零滑点成本配置:市价按 bar.open 原价成交(测试 3 需要 entry 精确=100)
ZERO_SLIP_CFG = {"default_spread_bps": 0, "fixed_impact_bps": 0, "taker_fee_bps": 0,
                 "maker_fee_bps": 0}


def paper_conn(tmp_path: Path):
    return open_paper_db(tmp_path / "paper.db")


# ---------- 施工包02 · 策略测试助手 ----------

def make_bars_df(closes, base_ts: int = BASE_TS, tf_sec: int = 900):
    """从收盘价序列构造 bars DataFrame(index=open_ts epoch 秒)。"""
    import pandas as pd

    closes = list(closes)
    opens = [closes[0]] + closes[:-1]
    highs = [max(o, c) * 1.0005 for o, c in zip(opens, closes)]
    lows = [min(o, c) * 0.9995 for o, c in zip(opens, closes)]
    idx = [base_ts + i * tf_sec for i in range(len(closes))]
    return pd.DataFrame({"open": opens, "high": highs, "low": lows,
                         "close": closes, "volume": [10.0] * len(closes)},
                        index=idx, dtype=float)


def insert_funding_rows(market_conn, symbol: str, rows, venue: str = VENUE):
    """rows: [(ts, rate)];interval_hours=1。"""
    market_conn.executemany(
        "INSERT OR REPLACE INTO funding(venue, symbol, ts, rate, interval_hours, predicted_next) "
        "VALUES(?,?,?,?,1.0,NULL)",
        [(venue, symbol, ts, rate) for ts, rate in rows])
    market_conn.commit()


def insert_basis_rows(market_conn, symbol: str, rows, venue: str = VENUE):
    """rows: [(ts, mark, bid, ask, ref_price, ref_ts)]。"""
    market_conn.executemany(
        "INSERT OR REPLACE INTO basis_ticks(ts, venue, symbol, platform_mark, platform_bid, "
        "platform_ask, platform_index, ref_price, ref_ts, ref_source) "
        "VALUES(?,?,?,?,?,?,NULL,?,?,'test')",
        [(ts, venue, symbol, mark, bid, ask, ref, ref_ts)
         for ts, mark, bid, ask, ref, ref_ts in rows])
    market_conn.commit()


def make_ctx(bars_df, market_conn, position=None, equity: float = 10000.0,
             event_gate=None, extras=None, symbol: str = SYMBOL):
    """用 bars_df 最后一根构造 Context。"""
    from platforms.single_account.sim.orders import Bar
    from platforms.single_account.strategies.base import Context, MarketData

    last_ts = int(bars_df.index[-1])
    row = bars_df.iloc[-1]
    bar = Bar(symbol=symbol, tf=TF, open_ts=last_ts, open=float(row["open"]),
              high=float(row["high"]), low=float(row["low"]), close=float(row["close"]),
              volume=float(row["volume"]))
    return Context(bar=bar, bars=bars_df, position=position, equity=equity,
                   funding_rate=None, event_gate=event_gate,
                   data=MarketData(market_conn), now_ts=last_ts + 900,
                   extras=extras or {})


def market_conn_with_bars(tmp_path: Path, bars=BARS_A):
    conn = open_market_db(tmp_path / "market.db")
    upsert_klines(conn, [
        (VENUE, bar.symbol, bar.tf, bar.open_ts, bar.open, bar.high, bar.low,
         bar.close, bar.volume)
        for bar in bars
    ])
    return conn
