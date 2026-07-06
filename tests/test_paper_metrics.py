"""施工包01 · §B8 必过测试 6(指标)+ §B4 补充。"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fixtures_sa import paper_conn

from platforms.single_account.sim import metrics, persistence


# 6. §B8.6:MDD=0.25;R=2.0;PF=3.0;胜率 2/3
def test_metrics():
    assert metrics.max_drawdown([100, 110, 99, 105, 120, 90]) == 0.25

    # 单笔 entry=100/stop=95/exit=110 多头 → R = (110−100)/(100−95) = 2.0
    r = (110 - 100) / (100 - 95)
    assert r == 2.0
    assert metrics.expectancy_r([r]) == 2.0

    nets = [10.0, 5.0, -5.0]
    assert metrics.profit_factor(nets) == 3.0
    assert metrics.win_rate(nets) == 2 / 3


def test_sharpe_requires_two_days():
    assert metrics.sharpe([]) is None
    assert metrics.sharpe([0.01]) is None
    assert metrics.sharpe([0.01, 0.01]) is None  # std=0 → None
    value = metrics.sharpe([0.01, 0.02, -0.005])
    assert value is not None and value > 0


def test_expectancy_ignores_null_r():
    assert metrics.expectancy_r([2.0, None, -1.0]) == 0.5
    assert metrics.expectancy_r([None]) is None


def test_aggregate_strategy_daily(tmp_path):
    conn = paper_conn(tmp_path)
    day0 = 1_760_000_000 - (1_760_000_000 % 86400)  # UTC 日起点
    with conn:
        for i, (net, strategy) in enumerate([(10.0, "a"), (-4.0, "a"), (3.0, "b")]):
            persistence.insert_position_closed(conn, {
                "strategy": strategy, "symbol": "BTC", "side": "long", "qty": 1.0,
                "entry_ts": day0 + i * 600, "entry_price": 100.0,
                "exit_ts": day0 + i * 600 + 300, "exit_price": 100.0 + net,
                "gross_pnl": net + 0.1, "fees": 0.1, "funding": 0.0, "net_pnl": net,
                "r_multiple": None, "exit_reason": "close_order",
                "holding_secs": 300, "tags_json": "{}",
            })
        for i, equity in enumerate([10000, 10010, 10005]):
            persistence.snapshot_equity(conn, day0 + i * 600 + 300, equity, equity, 0.0, 0.0)

    assert metrics.aggregate_strategy_daily(conn) == 2
    rows = {row[0]: row for row in conn.execute(
        "SELECT strategy, trades, wins, net, mdd_intraday FROM strategy_daily")}
    assert rows["a"][1:4] == (2, 1, 6.0)
    assert rows["b"][1:4] == (1, 1, 3.0)
    expected_mdd = 1 - 10005 / 10010
    assert abs(rows["a"][4] - expected_mdd) < 1e-12
