from decimal import Decimal
from pathlib import Path
import sys


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from engine import PolyLPSMulti, _compute_quote_target_shares  # noqa: E402


def test_quote_target_honors_absolute_share_cap():
    target, warning = _compute_quote_target_shares(
        available=Decimal("905.57"),
        rewards_min=Decimal("200"),
        min_order_size=Decimal("5"),
        budget_pct=Decimal("0.95"),
        size_cap=Decimal("1"),
        max_quote_shares=Decimal("200"),
    )

    assert target == Decimal("200")
    assert warning == ""


def test_quote_target_applies_feasibility_size_cap():
    target, warning = _compute_quote_target_shares(
        available=Decimal("905.57"),
        rewards_min=Decimal("200"),
        min_order_size=Decimal("5"),
        budget_pct=Decimal("0.95"),
        size_cap=Decimal("0.5"),
        max_quote_shares=Decimal("200"),
    )

    assert target == Decimal("0")
    assert warning.startswith("budget_below_min|")


def test_quote_target_never_exceeds_available_budget():
    target, warning = _compute_quote_target_shares(
        available=Decimal("350"),
        rewards_min=Decimal("100"),
        min_order_size=Decimal("5"),
        budget_pct=Decimal("0.6"),
        size_cap=Decimal("1"),
        max_quote_shares=Decimal("0"),
    )

    assert target == Decimal("210")
    assert warning == ""


def _budget_engine() -> PolyLPSMulti:
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {
        "101": {"paired_token_id": "102"},
        "102": {"paired_token_id": "101", "_dual_side_auto": True},
        "201": {"paired_token_id": "202"},
        "202": {"paired_token_id": "201", "_dual_side_auto": True},
    }
    engine._night_market_cfg = {}
    engine._paired_token_cache = {
        "101": "102",
        "102": "101",
        "201": "202",
        "202": "201",
    }
    engine._market_live_orders = {
        "101": [{"side": "BUY", "status": "live", "asset_id": "101", "price": "0.6", "size": "900"}],
        "102": [{"side": "BUY", "status": "live", "asset_id": "102", "price": "0.4", "size": "900"}],
        "201": [{"side": "BUY", "status": "live", "asset_id": "201", "price": "0.7", "size": "950"}],
        "202": [{"side": "BUY", "status": "live", "asset_id": "202", "price": "0.3", "size": "950"}],
    }
    engine._pending_order_reserve = {}
    return engine


def test_event_reserve_does_not_sum_different_events():
    engine = _budget_engine()

    assert engine._event_reserved_collateral("101") == Decimal("900")
    assert engine._event_reserved_collateral("201") == Decimal("950")


def test_event_reserve_includes_only_same_event_pending_orders():
    engine = _budget_engine()
    engine._pending_order_reserve = {
        "same-event": ("101", Decimal("40")),
        "other-event": ("201", Decimal("500")),
    }

    assert engine._event_reserved_collateral("101") == Decimal("940")
    assert engine._event_reserved_collateral(
        "101",
        extra_entries=[("102", Decimal("30")), ("201", Decimal("800"))],
    ) == Decimal("940")


def test_one_hundred_events_each_reuse_the_same_account_budget():
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {}
    engine._night_market_cfg = {}
    engine._paired_token_cache = {}
    engine._market_live_orders = {}
    engine._pending_order_reserve = {}

    event_tokens = []
    for idx in range(100):
        yes = str(10000 + idx * 2)
        no = str(10001 + idx * 2)
        event_tokens.append(yes)
        engine.market_cfg[yes] = {"paired_token_id": no}
        engine.market_cfg[no] = {"paired_token_id": yes, "_dual_side_auto": True}
        engine._paired_token_cache[yes] = no
        engine._paired_token_cache[no] = yes
        engine._market_live_orders[yes] = [
            {"side": "BUY", "status": "live", "asset_id": yes, "price": "0.6", "size": "950"},
        ]
        engine._market_live_orders[no] = [
            {"side": "BUY", "status": "live", "asset_id": no, "price": "0.4", "size": "950"},
        ]

    assert all(
        engine._event_reserved_collateral(token) == Decimal("950")
        for token in event_tokens
    )
