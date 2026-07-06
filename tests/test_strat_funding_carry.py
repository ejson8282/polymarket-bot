"""施工包02 · §5 funding_carry 三场景测试。"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import (
    BASE_TS,
    insert_funding_rows,
    make_bars_df,
    make_ctx,
)
from tests.fixtures_sa import market_conn_with_bars  # noqa: F401  (复用 open 逻辑)

from platforms.single_account.recorders.common import open_market_db
from platforms.single_account.sim.position import Position
from platforms.single_account.strategies.funding_carry import FundingCarry

N_BARS = 3400  # 覆盖 4h EMA200 暖机(200×16 根 15m)


def _flat_closes(n=N_BARS):
    return [100.0 + 0.5 * math.sin(i / 10.0) for i in range(n)]


def _uptrend_closes(n=N_BARS):
    return [100.0 + i * 0.05 for i in range(n)]


def _seed_high_positive_funding(conn, end_ts):
    # 30 天基线小费率,最近 6 期高费率 → apr≈175%,z 高
    rows = [(end_ts - i * 3600, 0.00001) for i in range(720, 6, -1)]
    rows += [(end_ts - i * 3600, 0.0002) for i in range(6, 0, -1)]
    insert_funding_rows(conn, "BTC", rows)


def test_high_funding_no_trend_opens_short(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = make_bars_df(_flat_closes())
    end_ts = int(bars.index[-1]) + 900
    _seed_high_positive_funding(conn, end_ts)

    signals = FundingCarry().on_bar(make_ctx(bars, conn))
    assert len(signals) == 1
    sig = signals[0]
    assert sig.action == "open_short"
    assert sig.stop_price > bars["close"].iloc[-1]     # 空头止损在上方
    assert sig.qty > 0
    assert sig.tags["apr"] > 0.25 and sig.tags["z"] > 1.5


def test_high_funding_strong_uptrend_filtered(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = make_bars_df(_uptrend_closes())
    end_ts = int(bars.index[-1]) + 900
    _seed_high_positive_funding(conn, end_ts)

    assert FundingCarry().on_bar(make_ctx(bars, conn)) == []  # 趋势过滤拒绝做空


def test_funding_weakens_two_periods_closes(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = make_bars_df(_flat_closes())
    end_ts = int(bars.index[-1]) + 900
    # 弱费率历史(apr≈0.9%,低于 8% 出场线)
    insert_funding_rows(conn, "BTC", [(end_ts - i * 3600, 0.000001) for i in range(720, 0, -1)])

    strat = FundingCarry()
    pos = Position(strategy=strat.name, symbol="BTC", side="short", qty=1.0,
                   entry_ts=end_ts - 3600, entry_price=100.0, stop=105.0)
    # 第 1 期转弱:计数=1,不平
    assert strat.on_bar(make_ctx(bars, conn, position=pos)) == []
    # 出现新一期 funding(仍弱)→ 计数=2 → 平仓信号
    insert_funding_rows(conn, "BTC", [(end_ts + 3600, 0.000001)])
    bars2 = make_bars_df(_flat_closes(N_BARS + 8))
    signals = strat.on_bar(make_ctx(bars2, conn, position=pos))
    assert len(signals) == 1 and signals[0].action == "close"
    assert "funding转弱" in signals[0].reason


def test_time_stop_five_days(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = make_bars_df(_flat_closes())
    end_ts = int(bars.index[-1]) + 900
    _seed_high_positive_funding(conn, end_ts)
    strat = FundingCarry()
    pos = Position(strategy=strat.name, symbol="BTC", side="short", qty=1.0,
                   entry_ts=end_ts - 6 * 86400, entry_price=100.0, stop=105.0)
    signals = strat.on_bar(make_ctx(bars, conn, position=pos))
    assert len(signals) == 1 and "时间止损" in signals[0].reason
