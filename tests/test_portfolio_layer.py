"""施工包02 · §5 组合层测试(优先级链)。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import BASE_TS, paper_conn

from platforms.single_account.recorders.common import open_market_db
from platforms.single_account.runner_paper import PortfolioEngine
from platforms.single_account.sim.broker import PaperBroker
from platforms.single_account.sim.orders import Bar
from platforms.single_account.sim.position import Position
from platforms.single_account.strategies.base import Signal

ZERO_COST = {"default_spread_bps": 0, "fixed_impact_bps": 0,
             "taker_fee_bps": 0, "maker_fee_bps": 0}


def _engine(tmp_path, sim_cfg=None):
    pconn = paper_conn(tmp_path)
    mconn = open_market_db(tmp_path / "m.db")
    cfg = dict(ZERO_COST)
    cfg.update(sim_cfg or {})
    broker = PaperBroker(pconn, sim_cfg=cfg)
    engine = PortfolioEngine(broker, pconn, mconn,
                             ["funding_carry", "momentum_breakout"], cfg)
    return engine, broker, pconn


def _bar(open_ts=BASE_TS, close=100.0, symbol="BTC"):
    return Bar(symbol=symbol, tf="15m", open_ts=open_ts, open=close,
               high=close * 1.001, low=close * 0.999, close=close)


def _sig(strategy, action, qty=1.0, symbol="BTC", limit=None):
    return Signal(strategy, symbol, action, qty, None, None, "test", {}, limit_price=limit)


def test_opposite_direction_second_strategy_rejected(tmp_path):
    engine, broker, pconn = _engine(tmp_path)
    bar = _bar()
    broker.account.add_position(Position(strategy="funding_carry", symbol="BTC",
                                         side="long", qty=1.0, entry_ts=BASE_TS,
                                         entry_price=100.0))
    engine._route_signal(_sig("momentum_breakout", "open_short"), bar, equity=10000.0)
    assert broker.account.position_for("momentum_breakout", "BTC") is None
    row = pconn.execute("SELECT taken, skip_reason FROM decisions "
                        "WHERE strategy='momentum_breakout'").fetchone()
    assert row[0] == 0 and row[1].startswith("opposite_position_exists")


def test_global_daily_loss_stop_rejects_opens_allows_close(tmp_path):
    engine, broker, pconn = _engine(tmp_path)
    bar = _bar()
    engine.process_bar(bar, equity=10000.0)      # 建立 day_start_equity=10000

    # 亏损 3% 后:open 被拒
    engine._route_signal(_sig("funding_carry", "open_long"), bar, equity=9700.0)
    open_row = pconn.execute("SELECT taken, skip_reason FROM decisions "
                             "WHERE action='open_long'").fetchone()
    assert open_row[0] == 0 and open_row[1] == "global_daily_loss_stop"

    # 平仓照常
    broker.account.add_position(Position(strategy="funding_carry", symbol="BTC",
                                         side="long", qty=1.0, entry_ts=BASE_TS,
                                         entry_price=100.0))
    engine._route_signal(_sig("funding_carry", "close"), bar, equity=9700.0)
    close_row = pconn.execute("SELECT taken FROM decisions WHERE action='close'").fetchone()
    assert close_row[0] == 1
    assert any(o.reduce_only for o in broker.pending)


def test_gross_exposure_cap(tmp_path):
    engine, broker, pconn = _engine(tmp_path)
    bar = _bar()
    engine.process_bar(bar, equity=10000.0)
    broker.account.add_position(Position(strategy="funding_carry", symbol="ETH",
                                         side="long", qty=140.0, entry_ts=BASE_TS,
                                         entry_price=100.0))          # 名义 14000
    broker.account.last_marks["ETH"] = 100.0
    engine._route_signal(_sig("momentum_breakout", "open_long", qty=15.0), bar, 10000.0)
    row = pconn.execute("SELECT skip_reason FROM decisions WHERE action='open_long'").fetchone()
    assert row[0] == "gross_exposure_cap"       # 14000+1500 > 1.5×10000


def test_strategy_notional_cap(tmp_path):
    engine, broker, pconn = _engine(
        tmp_path, {"strategies": {"momentum_breakout": {"max_notional_pct": 0.10}}})
    bar = _bar()
    engine.process_bar(bar, equity=10000.0)
    engine._route_signal(_sig("momentum_breakout", "open_long", qty=20.0), bar, 10000.0)
    row = pconn.execute("SELECT skip_reason FROM decisions WHERE action='open_long'").fetchone()
    assert row[0] == "strategy_notional_cap"    # 2000 > 10%×10000


def test_event_gate_blocks_open_records_decision(tmp_path):
    engine, broker, pconn = _engine(tmp_path)
    engine.gate._calendar_broken = True          # 失败安全:封锁全部
    bar = _bar()
    engine.process_bar(bar, equity=10000.0)
    engine._route_signal(_sig("funding_carry", "open_long"), bar, 10000.0)
    row = pconn.execute("SELECT taken, skip_reason FROM decisions "
                        "WHERE action='open_long'").fetchone()
    assert row[0] == 0 and "fail_safe" in row[1]


def test_accepted_signal_becomes_order_and_fills(tmp_path):
    engine, broker, pconn = _engine(tmp_path)
    bar = _bar()
    engine.process_bar(bar, equity=10000.0)
    engine._route_signal(_sig("funding_carry", "open_long", qty=2.0), bar, 10000.0)
    assert len(broker.pending) == 1
    fills = broker.process_pending(_bar(open_ts=BASE_TS + 900, close=101.0))
    assert len(fills) == 1
    pos = broker.account.position_for("funding_carry", "BTC")
    assert pos is not None and pos.qty == 2.0
    taken = pconn.execute("SELECT taken FROM decisions WHERE action='open_long'").fetchone()[0]
    assert taken == 1


def test_stale_limit_entry_canceled(tmp_path):
    engine, broker, pconn = _engine(tmp_path)
    for i in range(1):
        engine.process_bar(_bar(open_ts=BASE_TS + i * 900), equity=10000.0)
    sig = _sig("funding_carry", "open_long", qty=1.0, limit=90.0)
    sig.tags["cancel_after_bars"] = 2
    engine._route_signal(sig, _bar(open_ts=BASE_TS), 10000.0)
    assert len(broker.pending) == 1
    engine.process_bar(_bar(open_ts=BASE_TS + 900), equity=10000.0)   # 1 根后仍在
    assert len(broker.pending) == 1
    engine.process_bar(_bar(open_ts=BASE_TS + 1800), equity=10000.0)  # 第 2 根 → 撤单
    assert broker.pending == []
    status = pconn.execute("SELECT status FROM orders WHERE type='limit'").fetchone()[0]
    assert status == "canceled"
