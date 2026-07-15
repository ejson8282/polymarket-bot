from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUSHER_PATH = ROOT / "deploy" / "latitude-console" / "alert_pusher.py"

spec = importlib.util.spec_from_file_location("latitude_alert_pusher", PUSHER_PATH)
pusher = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(pusher)


def test_digest_uses_reconciled_capital_not_equity_history_last() -> None:
    line = pusher._equity_digest_line({
        "capital": {
            "complete": True,
            "current_equity": 2023.28,
            "pnl": 25.91,
            "pnl_pct": 1.30,
        },
        "equity_history": {"present": True, "valid": True, "last": 9999, "change": 5},
    })

    assert line == "总权益 $2,023.28 · 相对投入 ▲+$25.91 (+1.30%)"
    assert "9999" not in line


def test_digest_does_not_report_a_curve_when_capital_is_incomplete() -> None:
    line = pusher._equity_digest_line({
        "capital": {"complete": False, "reason": "待对账"},
        "equity_history": {"present": True, "valid": True, "last": 25.91},
    })

    assert line == "权益对账暂不可用(本金账本未完成)"
