"""施工包02 · §5 event_gate 测试。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from platforms.single_account.strategies.event_gate import EventGate

T0 = 1_760_000_000


def _calendar(tmp_path: Path, items) -> Path:
    path = tmp_path / "macro_calendar.json"
    path.write_text(json.dumps(items), encoding="utf-8")
    return path


def test_macro_window_blocks_all_symbols(tmp_path):
    gate = EventGate(calendar_path=_calendar(tmp_path, [{
        "symbols": [], "type": "fomc", "impact": "high",
        "window_start_ts": T0, "window_end_ts": T0 + 3600,
    }]))
    blocked, reason = gate.blocked("BTC", T0 + 100)
    assert blocked and reason == "macro_window:fomc"
    # ±30min 缓冲
    assert gate.blocked("NVDA", T0 - 1799)[0]
    assert gate.blocked("NVDA", T0 + 3600 + 1799)[0]
    assert not gate.blocked("NVDA", T0 - 1801)[0]
    assert not gate.blocked("NVDA", T0 + 3600 + 1801)[0]


def test_earnings_blocks_symbol_and_forces_flat(tmp_path):
    gate = EventGate(calendar_path=_calendar(tmp_path, [{
        "symbols": ["NVDA"], "type": "earnings", "impact": "high",
        "window_start_ts": T0, "window_end_ts": T0,
    }]))
    # 财报前 24h 至后 2h 禁该 symbol
    assert gate.blocked("NVDA", T0 - 24 * 3600 + 1)[0]
    assert gate.blocked("NVDA", T0 + 2 * 3600 - 1)[0]
    assert not gate.blocked("NVDA", T0 - 24 * 3600 - 1)[0]
    assert not gate.blocked("BTC", T0)[0]          # 只禁映射个股
    # 强制平净
    flat, reason = gate.force_flat("NVDA", T0 - 3600)
    assert flat and reason.startswith("earnings_force_flat")
    assert not gate.force_flat("BTC", T0 - 3600)[0]


def test_missing_calendar_means_no_events(tmp_path):
    gate = EventGate(calendar_path=tmp_path / "nope.json")
    assert gate.blocked("BTC", T0) == (False, "")


def test_broken_calendar_fails_safe_blocks_all(tmp_path):
    path = tmp_path / "macro_calendar.json"
    path.write_text("{not valid json", encoding="utf-8")
    gate = EventGate(calendar_path=path)
    blocked, reason = gate.blocked("BTC", T0)
    assert blocked and "fail_safe" in reason      # 绝不因数据坏而放开
    assert not gate.force_flat("BTC", T0)[0]      # 坏数据不触发误平仓


def test_missing_events_table_degrades_to_calendar(tmp_path):
    import sqlite3

    conn = sqlite3.connect(":memory:")            # 无 events 表 → 退化静态日历
    gate = EventGate(calendar_path=_calendar(tmp_path, [{
        "symbols": [], "type": "cpi", "impact": "high",
        "window_start_ts": T0, "window_end_ts": T0,
    }]), paper_conn=conn)
    assert gate.blocked("ETH", T0)[0]


def test_events_table_used_when_present(tmp_path):
    import sqlite3

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE events(symbols_json TEXT, type TEXT, impact TEXT, "
                 "window_start_ts INTEGER, window_end_ts INTEGER)")
    conn.execute("INSERT INTO events VALUES(?,?,?,?,?)",
                 (json.dumps(["QQQ"]), "earnings", "high", T0, T0))
    gate = EventGate(calendar_path=tmp_path / "nope.json", paper_conn=conn)
    assert gate.blocked("QQQ", T0 - 3600)[0]
    assert not gate.blocked("BTC", T0 - 3600)[0]
