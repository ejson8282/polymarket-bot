"""施工包05 · 5B pmbot 面板夹具测试(纯构造函数,不启动 streamlit)。"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dashboard.overview_pm import (
    account_matrix_html,
    account_matrix_rows,
    banner_html,
    equity_summary_html,
    skip_reason_html,
)


# 1. 横幅文案正确
def test_banner_text_and_tone():
    mainnet = banner_html("mainnet")
    assert "MAINNET · LIVE" in mainnet and "pm-banner mainnet" in mainnet
    testnet = banner_html("testnet")
    assert "TESTNET · DRY-RUN" in testnet and "pm-banner testnet" in testnet


# 2. 账号矩阵:2 账号夹具 → 2 行,暂停状态标签正确
def test_account_matrix_two_accounts_with_pause():
    import time as _time

    states = {
        1: {"ts": "2026-07-06T01:00:00Z", "funder": "0xabcdef0123456789abcdef0123456789abcdef01",
            "balance": 512.5,
            "markets": {"tok1": {"live_orders": [{"id": "a"}, {"id": "b"}]}},
            # 引擎口径:fills[].ts 为 unix 秒;第二笔超 24h 应被排除
            "fills": [{"ts": _time.time() - 3600, "price": 0.45, "size": 100, "pnl": 1.2},
                      {"ts": _time.time() - 200000, "price": 0.5, "size": 50, "pnl": 0.1}],
            "sibling_registry": {"mode": "observe", "conflicts_detected": 3}},
        2: {"ts": "2026-07-06T01:00:00Z", "funder": "0x1111222233334444555566667777888899990000",
            "balance": 208.0, "markets": {}, "fills": []},
    }
    rows = account_matrix_rows(states, alive={1: True, 2: False}, paused={2: True})
    assert len(rows) == 2
    one, two = rows
    assert one["status"] == "运行中" and one["status_cls"] == "ok"
    assert two["status"] == "已暂停" and two["status_cls"] == "warn"
    assert one["live_orders"] == 2
    assert one["fills_today"] == 1 and abs(one["volume_today"] - 45.0) < 1e-9
    assert one["sibling_conflicts"] == 3
    assert one["funder_short"].startswith("0xabcd") and "…" in one["funder_short"]

    html = account_matrix_html(rows)
    assert html.count("<tr><td>") == 2      # 数据行(表头行不计)
    assert "已暂停" in html and "运行中" in html
    assert "3(observe)" in html.replace(" ", "")


# 3. Single Account 区:无 DB 优雅降级
def test_single_account_blocks_degrade_without_db(tmp_path):
    missing = tmp_path / "nope.db"
    assert "待一期" in skip_reason_html(missing)
    assert "待一期" in equity_summary_html(missing)
    assert "待一期" in skip_reason_html(None)
    # 有 DB 但缺表 → 同样降级不报错
    empty_db = tmp_path / "empty.db"
    sqlite3.connect(str(empty_db)).close()
    assert "待一期" in skip_reason_html(empty_db)
    assert "待一期" in equity_summary_html(empty_db)


# 4. Single Account 区:有数据时正确聚合
def test_single_account_blocks_with_data(tmp_path):
    db = tmp_path / "paper.db"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE decisions(ts INTEGER, strategy TEXT, symbol TEXT, "
                 "action TEXT, score_json TEXT, taken INTEGER, skip_reason TEXT)")
    for reason, n in [("outside_rth", 5), ("insufficient_basis_data", 3)]:
        for _ in range(n):
            conn.execute("INSERT INTO decisions VALUES(1,'s','BTC','open','{}',0,?)", (reason,))
    conn.execute("CREATE TABLE equity_snapshots(ts INTEGER PRIMARY KEY, equity REAL, "
                 "cash REAL, unrealized REAL, drawdown REAL)")
    for i, eq in enumerate([10000, 10100, 9898, 10050]):
        conn.execute("INSERT INTO equity_snapshots VALUES(?,?,?,0,0)", (1700000000 + i * 60, eq, eq))
    conn.commit()
    conn.close()

    skip = skip_reason_html(db)
    assert "outside_rth" in skip and "insufficient_basis_data" in skip and "待一期" not in skip
    equity = equity_summary_html(db)
    assert "$10,050.00" in equity
    assert "2.00%" in equity            # MDD = 1 − 9898/10100
