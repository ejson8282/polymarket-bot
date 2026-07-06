"""施工包02 · §5 mean_reversion 三场景测试。"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import make_bars_df, make_ctx

from platforms.single_account.recorders.common import open_market_db
from platforms.single_account.strategies.mean_reversion import MeanReversion

N = 2400

# 阈值类参数按规格是 config 可调项;测试用放宽的阈值验证**机制接线**
# (正弦夹具难以同时满足 RSI3<8×严格下穿×振幅限制的生产阈值组合)
TEST_CFG = {"rsi_max": 30.0, "trigger_range_atr_mult": 4.0}


def _range_closes(steps=(0.05, 0.6)):
    """震荡收敛:前段幅度大、近段幅度小(带宽分位低),末尾阴跌下穿下轨。"""
    closes = []
    for i in range(N - 400):
        closes.append(100.0 + 1.2 * math.sin(i / 7.0))
    for i in range(400 - len(steps)):
        closes.append(100.0 + 0.4 * math.sin((N - 400 - len(steps) + i) / 7.0))
    last = closes[-1]
    for step in steps:                             # 连续下跌:RSI(3) 走低,收盘下穿下轨
        last -= step
        closes.append(last)
    return closes


def test_range_touch_lower_band_low_rsi_opens_long(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = make_bars_df(_range_closes())
    signals = MeanReversion(TEST_CFG).on_bar(make_ctx(bars, conn))
    assert len(signals) == 1
    sig = signals[0]
    assert sig.action == "open_long"
    assert sig.limit_price is not None            # 下轨挂限价
    assert sig.limit_price > float(bars["close"].iloc[-1])   # 限价在现价上方的下轨处
    assert sig.stop_price < sig.limit_price       # 止损在入场外 1×ATR
    assert sig.tags["entry_kind"] == "limit"


def test_trending_regime_filtered(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    closes = [100.0 + i * 0.05 for i in range(N)]  # 趋势:1h ADX 高
    closes[-1] -= 3.0                              # 制造一个下跌触发条件
    bars = make_bars_df(closes)
    assert MeanReversion(TEST_CFG).on_bar(make_ctx(bars, conn)) == []


def test_explosive_trigger_bar_filtered(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = make_bars_df(_range_closes())
    # 把触发K线改造成爆发K线:振幅拉到 ATR 的数倍
    last_ts = bars.index[-1]
    bars.loc[last_ts, "high"] = bars.loc[last_ts, "close"] + 5.0
    bars.loc[last_ts, "low"] = bars.loc[last_ts, "close"] - 5.0
    assert MeanReversion(TEST_CFG).on_bar(make_ctx(bars, conn)) == []


def test_loss_streak_blacklist(tmp_path):
    from tests.fixtures_sa import paper_conn

    conn = open_market_db(tmp_path / "m.db")
    pconn = paper_conn(tmp_path)
    bars = make_bars_df(_range_closes())
    now = int(bars.index[-1]) + 900
    with pconn:
        for i in range(3):   # 连亏3笔,最近一笔在 1h 前 → 拉黑
            pconn.execute(
                "INSERT INTO positions_closed(strategy, symbol, side, qty, entry_ts, "
                "entry_price, exit_ts, exit_price, gross_pnl, fees, funding, net_pnl, "
                "r_multiple, exit_reason, holding_secs, tags_json) "
                "VALUES('mean_reversion','BTC','long',1,?,100,?,99,-1,0,0,-1,NULL,'stop_loss',900,'{}')",
                (now - 7200 - i * 900, now - 3600 - i * 900))
    ctx = make_ctx(bars, conn, extras={"paper_conn": pconn})
    assert MeanReversion(TEST_CFG).on_bar(ctx) == []
