"""施工包01 · §B8 必过测试 1–4 + §B3.6 单持仓规则。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import BARS_A, ZERO_SLIP_CFG, paper_conn

from platforms.single_account.sim import funding, persistence
from platforms.single_account.sim.broker import PaperBroker
from platforms.single_account.sim.orders import Bar, Order
from platforms.single_account.sim.position import Position


def _bar(open_, high, low, close, open_ts=900):
    return Bar(symbol="BTC", tf="15m", open_ts=open_ts,
               open=open_, high=high, low=low, close=close)


# 1. 市价单成本(§B8.1):下一bar open=100 → 成交 100.07,fee=0.0500350
def test_market_fill_costs(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy",
                              type="market", qty=1.0, created_ts=0))
    fills = broker.process_pending(_bar(100.0, 101.0, 99.0, 100.5))
    assert len(fills) == 1
    fill = fills[0]
    assert abs(fill.price - 100.07) < 1e-9          # 100×(1+7/10000)
    assert abs(fill.fee - 0.0500350) < 1e-9          # 100.07×1×0.0005
    assert fill.slippage_bps == 7.0
    assert abs(broker.account.cash - (10000 - 100.07 - 0.050035)) < 1e-9
    db_fill = conn.execute("SELECT price, fee, slippage_bps FROM fills").fetchone()
    assert abs(db_fill[0] - 100.07) < 1e-9 and abs(db_fill[1] - 0.0500350) < 1e-9


# 2. 限价严格穿越(§B8.2):low==99 触碰不成交;low=98.9 成交 99,maker 2bps
def test_limit_strict_cross(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="limit",
                              qty=1.0, limit_price=99.0, created_ts=0))
    assert broker.process_pending(_bar(100.0, 100.5, 99.0, 100.0)) == []
    fills = broker.process_pending(_bar(100.0, 100.5, 98.9, 100.0, open_ts=1800))
    assert len(fills) == 1
    assert fills[0].price == 99.0
    assert abs(fills[0].fee - 99.0 * 1.0 * 0.0002) < 1e-12


# 3. 同 bar SL/TP 最坏假设(§B8.3):以 97 止损,r=-1.0,ambiguous_bars==1
def test_same_bar_sl_tp_worst_case(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn, sim_cfg=ZERO_SLIP_CFG)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="market",
                              qty=1.0, created_ts=0, stop=97.0, tp=106.0))
    broker.process_pending(_bar(100.0, 100.5, 99.5, 100.0))  # 零滑点 → entry=100
    pos = broker.account.position_for("s", "BTC")
    assert pos.entry_price == 100.0 and pos.stop == 97.0 and pos.tp == 106.0

    closed = broker.check_stops(_bar(100.0, 107.0, 96.0, 100.0, open_ts=1800))
    assert len(closed) == 1
    row = closed[0]
    assert row["exit_price"] == 97.0 and row["exit_reason"] == "stop_loss"
    assert row["r_multiple"] == -1.0
    assert persistence.get_meta(conn, "ambiguous_bars") == "1"
    assert broker.account.position_for("s", "BTC") is None
    db = conn.execute("SELECT exit_price, r_multiple, exit_reason FROM positions_closed").fetchone()
    assert db == (97.0, -1.0, "stop_loss")


# 空头对称:同 bar 双触发 → 按止损(high>=stop),r=-1
def test_same_bar_sl_tp_short_symmetric(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn, sim_cfg=ZERO_SLIP_CFG)
    broker.account.add_position(Position(strategy="s", symbol="BTC", side="short",
                                         qty=1.0, entry_ts=0, entry_price=100.0,
                                         stop=103.0, tp=94.0))
    closed = broker.check_stops(_bar(100.0, 104.0, 93.0, 100.0))
    assert closed[0]["exit_price"] == 103.0 and closed[0]["exit_reason"] == "stop_loss"
    assert closed[0]["r_multiple"] == -1.0
    assert persistence.get_meta(conn, "ambiguous_bars") == "1"


# 4. funding 符号(§B8.4):多头 qty=2 mark=100 rate=+0.0001 → cash −0.02,amount=−0.02
def test_funding_sign(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.account.add_position(Position(strategy="s", symbol="BTC", side="long",
                                         qty=2.0, entry_ts=0, entry_price=100.0))
    cash_before = broker.account.cash
    funding.accrue(conn, broker.account, ts=1000, marks={"BTC": 100.0},
                   rate_for=lambda symbol, ts: 0.0001)
    assert abs((cash_before - broker.account.cash) - 0.02) < 1e-12
    rows = conn.execute("SELECT ts, symbol, rate, pos_qty, amount FROM funding_events").fetchall()
    assert len(rows) == 1
    ts, symbol, rate, pos_qty, amount = rows[0]
    assert (ts, symbol, rate, pos_qty) == (1000, "BTC", 0.0001, 2.0)
    assert abs(amount - (-0.02)) < 1e-12  # 负=支出(文档约定)
    assert abs(broker.account.position_for("s", "BTC").funding_paid - 0.02) < 1e-12


# 空头收正 funding:amount 为正(收入)
def test_funding_sign_short_receives(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn)
    broker.account.add_position(Position(strategy="s", symbol="BTC", side="short",
                                         qty=2.0, entry_ts=0, entry_price=100.0))
    cash_before = broker.account.cash
    funding.accrue(conn, broker.account, ts=1000, marks={"BTC": 100.0},
                   rate_for=lambda symbol, ts: 0.0001)
    assert abs((broker.account.cash - cash_before) - 0.02) < 1e-12


# §B3.6:同向加仓/反向对锁 reject;reduce_only 平仓走通
def test_single_position_rule_and_close(tmp_path):
    conn = paper_conn(tmp_path)
    broker = PaperBroker(conn, sim_cfg=ZERO_SLIP_CFG)
    broker.submit_order(Order(strategy="s", symbol="BTC", side="buy", type="market",
                              qty=1.0, created_ts=0))
    broker.process_pending(_bar(100.0, 101.0, 99.0, 100.0))

    add = broker.submit_order(Order(strategy="s", symbol="BTC", side="buy",
                                    type="market", qty=1.0))
    hedge = broker.submit_order(Order(strategy="s", symbol="BTC", side="sell",
                                      type="market", qty=1.0))
    assert add.status == "rejected" and "加仓" in add.reason
    assert hedge.status == "rejected" and "对锁" in hedge.reason
    statuses = {row[0] for row in conn.execute(
        "SELECT status FROM orders WHERE order_id IN (?,?)",
        (add.order_id, hedge.order_id))}
    assert statuses == {"rejected"}

    close = broker.submit_order(Order(strategy="s", symbol="BTC", side="sell",
                                      type="market", qty=1.0, reduce_only=True))
    assert close.status == "new"
    broker.process_pending(_bar(102.0, 102.5, 101.5, 102.0, open_ts=1800))
    assert broker.account.position_for("s", "BTC") is None
    net = conn.execute("SELECT net_pnl FROM positions_closed").fetchone()[0]
    assert abs(net - 2.0) < 1e-9  # 零费用零滑点:(102−100)×1


# BARS_A 夹具完整性(突破/跳空存在,供回放类测试复用)
def test_bars_a_fixture_shape():
    assert len(BARS_A) == 30
    assert BARS_A[7].high > max(bar.high for bar in BARS_A[:7])          # 突破
    assert BARS_A[12].open > BARS_A[11].close + 2.0                     # 跳空
    assert all(bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
               for bar in BARS_A)
