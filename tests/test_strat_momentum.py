"""施工包02 · §5 momentum_breakout 三场景测试 + 部分平仓扩展。"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import BASE_TS, make_bars_df, make_ctx, paper_conn

from platforms.single_account.recorders.common import open_market_db
from platforms.single_account.sim.broker import PaperBroker
from platforms.single_account.sim.orders import Bar, Order
from platforms.single_account.sim.position import Position
from platforms.single_account.strategies import indicators as ind
from platforms.single_account.strategies.momentum_breakout import MomentumBreakout

N = 3400


def _breakout_bars(volume_last: float = 30.0):
    """缓涨→末段加速放量突破:满足 Donchian/量/ATR扩张/4h多头 全部条件。"""
    closes = [100.0 + i * 0.02 for i in range(N - 40)]
    last = closes[-1]
    closes += [last + (i + 1) * 0.6 for i in range(40)]  # 末段陡升,ATR 扩张
    bars = make_bars_df(closes)
    volumes = [10.0] * len(closes)
    volumes[-1] = volume_last
    bars["volume"] = volumes
    return bars


def test_breakout_with_volume_and_trend_opens_long(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _breakout_bars(volume_last=30.0)   # 3×SMA20 放量
    signals = MomentumBreakout().on_bar(make_ctx(bars, conn))
    assert len(signals) == 1
    sig = signals[0]
    assert sig.action == "open_long"
    assert sig.stop_price < bars["close"].iloc[-1]
    assert sig.tags["breakout_level"] < bars["close"].iloc[-1]
    assert sig.qty > 0


def test_breakout_without_volume_filtered(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _breakout_bars(volume_last=10.0)   # 缩量:等于均量,不足 1.5×
    assert MomentumBreakout().on_bar(make_ctx(bars, conn)) == []


def test_chandelier_stop_calculation(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _breakout_bars()
    strat = MomentumBreakout()
    entry = float(bars["close"].iloc[-30])
    pos = Position(strategy=strat.name, symbol="BTC", side="long", qty=1.0,
                   entry_ts=BASE_TS, entry_price=entry, stop=entry - 5.0,
                   tags={"initial_stop": entry - 5.0, "scaled_out": True})
    strat.on_bar(make_ctx(bars, conn, position=pos))
    atr_now = float(ind.atr(bars["high"], bars["low"], bars["close"], 14).iloc[-1])
    expected = float(bars["high"].tail(22).max()) - 2.5 * atr_now
    assert abs(pos.stop - expected) < 1e-9        # 吊灯止损计算正确
    assert pos.stop > entry - 5.0                  # 只收紧不放松

    # 再走一根更低的 bar:吊灯值下降 → 止损保持不动
    lower = bars.copy()
    new_ts = int(bars.index[-1]) + 900
    lower.loc[new_ts] = [bars["close"].iloc[-1], bars["close"].iloc[-1] * 1.0001,
                         bars["close"].iloc[-1] * 0.98, bars["close"].iloc[-1] * 0.985, 10.0]
    stop_before = pos.stop
    strat.on_bar(make_ctx(lower, conn, position=pos))
    assert pos.stop >= stop_before


def test_scale_out_one_third_at_2r(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _breakout_bars()
    strat = MomentumBreakout()
    close = float(bars["close"].iloc[-1])
    entry = close - 10.0
    pos = Position(strategy=strat.name, symbol="BTC", side="long", qty=3.0,
                   entry_ts=BASE_TS, entry_price=entry,
                   stop=close * 2,  # 设远高吊灯的 stop,确保吊灯不再收紧影响判断
                   tags={"initial_stop": entry - 4.0})   # R=4,浮盈10 ≥ 2R
    signals = strat.on_bar(make_ctx(bars, conn, position=pos))
    assert len(signals) == 1
    sig = signals[0]
    assert sig.action == "close" and abs(sig.qty - 1.0) < 1e-9
    assert pos.tags["scaled_out"] is True
    # 再来一根:不再重复减仓
    assert all(s.action != "close" or "备用离场" in s.reason
               for s in strat.on_bar(make_ctx(bars, conn, position=pos)))


def test_broker_partial_close_extension(tmp_path):
    """01 内核扩展:reduce_only qty<持仓 → 部分平仓,费用/funding 按比例分摊。"""
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn, sim_cfg={"default_spread_bps": 0, "fixed_impact_bps": 0,
                                        "taker_fee_bps": 0, "maker_fee_bps": 0})
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="market",
                              qty=3.0, created_ts=0, tags={"k": "v"}))
    broker.process_pending(Bar("BTC", "15m", 900, 100.0, 101.0, 99.0, 100.0))
    broker.submit_order(Order(strategy="s", symbol="BTC", side="sell", type="market",
                              qty=1.0, reduce_only=True))
    broker.process_pending(Bar("BTC", "15m", 1800, 106.0, 106.5, 105.5, 106.0))

    pos = broker.account.position_for("s", "BTC")
    assert pos is not None and abs(pos.qty - 2.0) < 1e-12   # 剩 2/3
    row = conn.execute("SELECT qty, gross_pnl, net_pnl, tags_json FROM positions_closed").fetchone()
    assert abs(row[0] - 1.0) < 1e-12 and abs(row[1] - 6.0) < 1e-9
    assert '"k": "v"' in row[3]                              # tags 落库

    # 重启恢复:剩余持仓与现金一致
    restored = PaperBroker.from_db(conn)
    rpos = restored.account.position_for("s", "BTC")
    assert rpos is not None and abs(rpos.qty - 2.0) < 1e-12
    assert abs(restored.account.cash - broker.account.cash) < 1e-9
    assert rpos.tags.get("k") == "v"
