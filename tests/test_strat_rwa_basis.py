"""施工包02 · §5 rwa_basis 三场景测试。"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import insert_basis_rows, make_bars_df, make_ctx

from platforms.single_account.recorders.common import open_market_db
from platforms.single_account.strategies.rwa_basis import RwaBasis, minutes_to_ny_close

# 2026-07-06 是周一;14:30 UTC = 10:30 ET(EDT,盘中)
RTH_TS = int(datetime(2026, 7, 6, 14, 30, tzinfo=timezone.utc).timestamp())
RTH_TS -= RTH_TS % 900  # 对齐 15m(14:30 UTC 本身对齐)
NIGHT_TS = int(datetime(2026, 7, 6, 2, 0, tzinfo=timezone.utc).timestamp())


def _bars_ending_at(end_open_ts: int, n: int = 50):
    base = end_open_ts - (n - 1) * 900
    return make_bars_df([100.0] * n, base_ts=base)


def _seed_span(conn, symbol: str, end_ts: int, days: float = 8.0):
    """铺 8 天的背景 basis 数据(每小时一个点,满足 ≥7 天准入门)。"""
    rows = [(end_ts - int(d * 3600), 100.0, 99.9, 100.1, 100.0, end_ts - int(d * 3600))
            for d in range(int(days * 24), 2, -1)]
    insert_basis_rows(conn, symbol, rows)


def _seed_jump(conn, symbol: str, now_ts: int):
    """ref 1分钟内 +50bps,platform 纹丝不动(滞后)。"""
    insert_basis_rows(conn, symbol, [
        (now_ts - 70, 100.0, 99.95, 100.05, 100.0, now_ts - 70),
        (now_ts - 5, 100.0, 99.95, 100.05, 100.5, now_ts - 5),   # ref +50bps, mark 不变
    ])


def test_insufficient_data_skips_with_reason(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _bars_ending_at(RTH_TS - 900)
    now = int(bars.index[-1]) + 900
    insert_basis_rows(conn, "QQQ", [(now - 3600, 100, 99.9, 100.1, 100, now - 3600)])  # 仅1小时数据
    ctx = make_ctx(bars, conn, symbol="QQQ")
    assert RwaBasis().on_bar(ctx) == []
    assert ctx.extras["skip_events"] == [
        {"strategy": "rwa_basis", "symbol": "QQQ", "reason": "insufficient_basis_data"}]


def test_ref_jump_platform_lag_opens_toward_ref(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _bars_ending_at(RTH_TS - 900)
    now = int(bars.index[-1]) + 900
    assert minutes_to_ny_close(now) > 10
    _seed_span(conn, "QQQ", now)
    _seed_jump(conn, "QQQ", now)
    ctx = make_ctx(bars, conn, symbol="QQQ")
    signals = RwaBasis().on_bar(ctx)
    assert len(signals) == 1
    sig = signals[0]
    assert sig.action == "open_long"                      # ref 上跳 → 顺 ref 方向
    assert sig.tags["ref_move_bps"] > 30
    assert sig.stop_price < 100.0                          # 多头止损在下方
    assert sig.qty > 0
    # 名义上限 5% 权益(流动性薄)
    assert sig.qty * 100.0 <= 0.05 * 10000 + 1e-9


def test_outside_rth_no_entry(tmp_path):
    conn = open_market_db(tmp_path / "m.db")
    bars = _bars_ending_at(NIGHT_TS - 900)
    now = int(bars.index[-1]) + 900
    _seed_span(conn, "QQQ", now)
    _seed_jump(conn, "QQQ", now)
    ctx = make_ctx(bars, conn, symbol="QQQ")
    assert RwaBasis().on_bar(ctx) == []
    assert ctx.extras["skip_events"][0]["reason"] == "outside_rth"


def test_position_converge_and_time_stop(tmp_path):
    from platforms.single_account.sim.position import Position

    conn = open_market_db(tmp_path / "m.db")
    bars = _bars_ending_at(RTH_TS - 900)
    now = int(bars.index[-1]) + 900
    _seed_span(conn, "QQQ", now)
    # 收敛:gap 仅 2bps
    insert_basis_rows(conn, "QQQ", [(now - 5, 100.0, 99.95, 100.05, 100.02, now - 5)])
    strat = RwaBasis()
    pos = Position(strategy=strat.name, symbol="QQQ", side="long", qty=1.0,
                   entry_ts=now - 60, entry_price=100.0,
                   tags={"entry_gap_bps": 50.0})
    signals = strat.on_bar(make_ctx(bars, conn, position=pos, symbol="QQQ"))
    assert len(signals) == 1 and "收敛" in signals[0].reason
    # 时间止损:持仓超过 300s
    pos2 = Position(strategy=strat.name, symbol="QQQ", side="long", qty=1.0,
                    entry_ts=now - 400, entry_price=100.0,
                    tags={"entry_gap_bps": 50.0})
    signals2 = strat.on_bar(make_ctx(bars, conn, position=pos2, symbol="QQQ"))
    assert len(signals2) == 1 and "时间止损" in signals2[0].reason
