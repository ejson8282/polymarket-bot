"""施工包01 · §B8 必过测试 5(重启恢复)+ §B5/§B9 持久化与崩溃恢复。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import BARS_A, BASE_TS, paper_conn, market_conn_with_bars

from platforms.single_account.runner_paper import run_replay
from platforms.single_account.sim import persistence
from platforms.single_account.sim.broker import PaperBroker
from platforms.single_account.sim.orders import Bar, Order


def _bar(open_, high, low, close, open_ts=900):
    return Bar(symbol="BTC", tf="15m", open_ts=open_ts,
               open=open_, high=high, low=low, close=close)


# 5. §B8.5:买 1@(open=100) 不平仓 → 新 broker 从库加载 → 持仓一致;
#    close=102 时 equity = 10000 − 100.07 − 0.050035 + 102 = 10001.879965(1e-6)
def test_restart_recovery(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="market",
                              qty=1.0, created_ts=0, stop=90.0, tp=120.0))
    broker.process_pending(_bar(100.0, 101.0, 99.0, 100.5))

    restored = PaperBroker.from_db(conn)
    pos = restored.account.position_for("s", "BTC")
    assert pos is not None
    assert pos.qty == 1.0
    assert abs(pos.entry_price - 100.07) < 1e-9
    assert pos.side == "long"
    assert pos.stop == 90.0 and pos.tp == 120.0  # 经 sim_meta 恢复
    assert abs(restored.account.cash - broker.account.cash) < 1e-12

    equity, _ = restored.account.mark_to_market(102.0)
    assert abs(equity - 10001.879965) < 1e-6


def test_restart_recovery_after_close_no_position(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="market",
                              qty=1.0, created_ts=0))
    broker.process_pending(_bar(100.0, 101.0, 99.0, 100.5))
    broker.submit_order(Order(strategy="s", symbol="BTC", side="sell", type="market",
                              qty=1.0, reduce_only=True))
    broker.process_pending(_bar(102.0, 102.5, 101.0, 102.0, open_ts=1800))

    restored = PaperBroker.from_db(conn)
    assert restored.account.positions == {}
    assert abs(restored.account.cash - broker.account.cash) < 1e-12


def test_pending_limit_order_restored(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="limit",
                              qty=1.0, limit_price=99.0, created_ts=0,
                              stop=95.0, tp=110.0))
    restored = PaperBroker.from_db(conn)
    assert len(restored.pending) == 1
    order = restored.pending[0]
    assert order.type == "limit" and order.limit_price == 99.0
    assert order.stop == 95.0 and order.tp == 110.0 and not order.reduce_only


def test_funding_events_restored_in_cash(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="market",
                              qty=2.0, created_ts=0))
    broker.process_pending(_bar(100.0, 101.0, 99.0, 100.5))
    from platforms.single_account.sim import funding
    funding.accrue(conn, broker.account, ts=1000, marks={"BTC": 100.0},
                   rate_for=lambda s, t: 0.0001)

    restored = PaperBroker.from_db(conn)
    assert abs(restored.account.cash - broker.account.cash) < 1e-12
    assert abs(restored.account.position_for("s", "BTC").funding_paid - 0.02) < 1e-12


def test_atomic_write_json(tmp_path):
    target = tmp_path / "out" / "state.json"
    persistence.atomic_write_json(target, {"a": 1})
    persistence.atomic_write_json(target, {"a": 2})
    assert json.loads(target.read_text(encoding="utf-8")) == {"a": 2}
    assert list(target.parent.glob("*.tmp")) == []


# §B9:回放冒烟 —— orders/fills/positions_closed/equity_snapshots 均有数据
def test_replay_smoke_populates_all_tables(tmp_path):
    market_conn_with_bars(tmp_path)
    now = BARS_A[-1].open_ts + 900
    summary = run_replay(market_db=tmp_path / "market.db", paper_db=tmp_path / "paper.db",
                         symbol="BTC", tf="15m", days=1.0, smoke=True, now=now, sim_cfg={})
    assert summary["bars_processed"] == 30
    assert summary["orders"] == 2 and summary["fills"] == 2
    assert summary["positions_closed"] == 1
    assert summary["equity_snapshots"] == 30
    assert summary["open_positions"] == 0


# §B9:崩溃恢复 —— 中途中断(等价 kill -9 后的磁盘状态)→ 重启续跑 → 无重复行、持仓恢复
def test_replay_crash_recovery_no_duplicates(tmp_path):
    market_conn_with_bars(tmp_path)
    mid = BARS_A[14].open_ts + 900   # 跑到第 15 根后"崩溃"
    end = BARS_A[-1].open_ts + 900
    kwargs = dict(market_db=tmp_path / "market.db", paper_db=tmp_path / "paper.db",
                  symbol="BTC", tf="15m", days=1.0, smoke=True, sim_cfg={})

    first = run_replay(now=mid, **kwargs)
    assert first["bars_processed"] == 15
    assert first["open_positions"] == 1      # smoke 单已开仓未平

    second = run_replay(now=end, **kwargs)
    assert second["bars_processed"] == 15    # 只补跑剩余 15 根
    assert second["orders"] == 2 and second["fills"] == 2
    assert second["positions_closed"] == 1
    assert second["equity_snapshots"] == 30  # ts 主键,无重复
    assert second["open_positions"] == 0

    third = run_replay(now=end, **kwargs)    # 再跑一遍:全部跳过,零变化
    assert third["bars_processed"] == 0
    assert (third["orders"], third["fills"], third["positions_closed"],
            third["equity_snapshots"]) == (2, 2, 1, 30)
