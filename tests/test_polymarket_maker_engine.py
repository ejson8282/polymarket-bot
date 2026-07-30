import asyncio
from decimal import Decimal
import json
from pathlib import Path
import sys


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from engine import (  # noqa: E402
    EVENT_ACTIVE,
    EVENT_EXIT_PENDING,
    EVENT_PENDING_MANUAL_EXIT,
    EventHaltPreempted,
    PolyLPSMulti,
    _compute_quote_target_shares,
)


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


def test_night_session_does_not_fall_back_to_day_markets_when_pool_is_empty():
    engine = object.__new__(PolyLPSMulti)
    engine._session_enabled = True
    engine.market_cfg = {"101": {"session": "day"}}
    engine._night_market_cfg = {}
    engine._current_session = lambda: "night"

    assert engine._active_market_cfg() == {}
    assert engine._session_allows("101") is False


def test_day_market_carry_is_explicit_and_fails_closed():
    engine = object.__new__(PolyLPSMulti)
    cutoff = 1_000.0

    engine._session_carry_day_markets_to_night = False
    assert engine._should_carry_day_market_to_night(2_000.0, cutoff) is False

    engine._session_carry_day_markets_to_night = True
    assert engine._should_carry_day_market_to_night(None, cutoff) is False
    assert engine._should_carry_day_market_to_night(999.0, cutoff) is False
    assert engine._should_carry_day_market_to_night(1_000.0, cutoff) is True


def test_runtime_dashboard_add_revalidates_fresh_observer(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {}
    engine._night_market_cfg = {}
    engine._eligibility_observer_path = tmp_path / "reward_observer_state.json"
    engine._eligibility_observer_path.write_text(
        json.dumps(
            {
                "generated_at": __import__("time").time(),
                "candidates": [
                    {
                        "token_id": "101",
                        "paired_token_id": "102",
                        "verification_recommended": True,
                        "market_phase": "normal",
                        "rewards_max_spread": 0.05,
                        "fill_risk": 20,
                        "condition_id": "condition",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    captured = {}

    def add_market_runtime(**kwargs):
        captured.update(kwargs)
        return True

    engine.add_market_runtime = add_market_runtime

    status = engine._runtime_add_from_command(
        {
            "token_id": "101",
            "paired_token_id": "102",
            "price_tick": 0.01,
            "min_distance_from_best_bid": 0.01,
        }
    )

    assert status == "added"
    assert captured["source"] == "dashboard_confirmed"
    assert captured["eligibility_managed"] is True
    assert captured["eligibility_base_risk"] == "low"
    assert captured["spread"] == 0.05


def test_runtime_dashboard_add_rejects_no_longer_eligible_market(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {}
    engine._night_market_cfg = {}
    engine._eligibility_observer_path = tmp_path / "reward_observer_state.json"
    engine._eligibility_observer_path.write_text(
        json.dumps(
            {
                "generated_at": __import__("time").time(),
                "candidates": [
                    {
                        "token_id": "101",
                        "verification_recommended": False,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    try:
        engine._runtime_add_from_command(
            {"token_id": "101", "paired_token_id": "102"}
        )
    except ValueError as exc:
        assert "no longer eligible" in str(exc)
    else:
        raise AssertionError("ineligible market must not be hot-added")


def test_cancel_quotes_preserves_unregistered_sell_exit():
    engine = object.__new__(PolyLPSMulti)
    orders = [
        {"id": "buy-1", "status": "live", "side": "BUY"},
        {"id": "sell-exit", "status": "live", "side": "SELL"},
    ]
    canceled = []

    class Client:
        def get_open_orders(self):
            return [o for o in orders if o["id"] not in canceled]

        def cancel(self, order_id):
            canceled.append(order_id)

    class Registry:
        def clear_funder(self, *_args, **_kwargs):
            return None

    engine.client = Client()
    engine._active_exit_orders = {}
    engine._market_live_orders = {}
    engine._sibling_registry = Registry()
    engine._funder_lc = "account"

    assert asyncio.run(engine._cancel_all_except_exit()) is True
    assert canceled == ["buy-1"]
    assert [o["id"] for o in engine.client.get_open_orders()] == ["sell-exit"]


def test_missing_exit_order_with_inventory_stays_pending():
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine._unwind_check_interval_sec = 0
    engine._unwind_max_age_sec = 14_400
    engine._active_exit_orders = {"101": "exit-1"}
    engine._pending_unwinds = [
        {
            "token_id": "101",
            "fill_price": 0.5,
            "fill_size": 150,
            "order_id": "exit-1",
            "placed_at": 1,
            "reason": "test",
        }
    ]
    states = []
    notifications = []

    class Client:
        def get_open_orders(self):
            return []

    async def position(_token_id):
        engine._running = False
        return 150.0

    engine.client = Client()
    engine._get_token_position = position
    engine._set_event_state = lambda *args: states.append(args)
    engine._notify_status = lambda *args, **kwargs: notifications.append((args, kwargs))
    engine._resume_halted_markets = lambda *_args: (_ for _ in ()).throw(
        AssertionError("inventory must not resume")
    )

    asyncio.run(engine.unwind_tracking_loop())

    assert len(engine._pending_unwinds) == 1
    assert engine._pending_unwinds[0]["missing_order_alerted"] is True
    assert "101" not in engine._active_exit_orders
    assert states[-1][1] == EVENT_PENDING_MANUAL_EXIT
    assert notifications


def _paired_state_engine() -> PolyLPSMulti:
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {
        "101": {"paired_token_id": "102"},
        "102": {"paired_token_id": "101", "_dual_side_auto": True},
    }
    engine._night_market_cfg = {}
    engine._paired_token_cache = {"101": "102", "102": "101"}
    engine._event_states = {
        "101": {"state": EVENT_ACTIVE, "reason": "init", "updated_at": 0},
        "102": {"state": EVENT_ACTIVE, "reason": "init", "updated_at": 0},
    }
    engine._halt_requested = {"101": None, "102": None}
    return engine


def test_exit_pending_blocks_both_sides_of_the_same_event():
    engine = _paired_state_engine()
    engine._event_states["102"]["state"] = EVENT_EXIT_PENDING

    assert engine._event_blocks_quote("102") is True
    assert engine._event_blocks_quote("101") is True
    assert engine._halt_preemption_reason("101").startswith(
        "paired_event_state=EXIT_PENDING"
    )


def test_submit_rechecks_state_after_signing_before_posting():
    engine = _paired_state_engine()
    posted = []

    class RemoteSigner:
        def sign_order(self, *_args):
            engine._event_states["102"]["state"] = EVENT_EXIT_PENDING
            return object()

    class Client:
        def post_order(self, *_args):
            posted.append(True)
            return {"orderID": "should-not-post"}

    engine.remote_signer = RemoteSigner()
    engine.client = Client()
    engine._sibling_gate = lambda _token, _side, price, _label: price

    async def acquire(*_args):
        return "reserve"

    async def release(*_args):
        return None

    engine._acquire_budget_reserve = acquire
    engine._release_budget_reserve = release
    engine._mark_latency = lambda *_args: None
    engine._mark_signer_recovered = lambda: None

    try:
        asyncio.run(
            engine._submit_post_order(
                "101",
                Decimal("0.40"),
                Decimal("10"),
                "race-test",
            )
        )
    except EventHaltPreempted:
        pass
    else:
        raise AssertionError("submit should be preempted after paired exit begins")

    assert posted == []


def test_latency_record_includes_cancel_clear_timing():
    engine = object.__new__(PolyLPSMulti)
    engine._latency_marks = {
        "101": {
            "t_detect": 100.00,
            "t_decision": 100.01,
            "t_send": 100.02,
            "t_cancel_ack": 100.12,
            "t_orders_cleared": 100.15,
        }
    }
    engine._latency_records = []

    engine._emit_latency_record("101", "volatility_watch")

    record = engine._latency_records[0]
    assert record["cancel_ack_ms"] == 100.0
    assert record["detect_to_cancel_ack_ms"] == 120.0
    assert record["cancel_ack_to_cleared_ms"] == 30.0
    assert record["send_to_cleared_ms"] == 130.0
    assert record["detect_to_cleared_ms"] == 150.0


def test_deactivate_market_disables_config_and_removes_current_runtime(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {
        "101": {"paired_token_id": "102"},
        "102": {"paired_token_id": "101", "_dual_side_auto": True},
    }
    engine._night_market_cfg = {}
    engine._paired_token_cache = {"101": "102", "102": "101"}
    engine._event_banned_until = {}
    engine._market_skip_until = {}
    engine.event_ban_ttl_sec = 86400
    engine._token_slug_cache = {"101": "example-market"}
    engine.client = type(
        "Client",
        (),
        {"get_open_orders": lambda _self: []},
    )()
    engine.notify_discord = lambda *_args, **_kwargs: None
    engine._notify_status = lambda *_args, **_kwargs: None
    engine._event_bus = type(
        "EventBus",
        (),
        {"publish": lambda _self, *_args, **_kwargs: None},
    )()

    config_path = tmp_path / "config.json"
    config_path.write_text(
        json.dumps({
            "markets": [
                {"token_id": "101", "enabled": True},
                {"token_id": "102", "enabled": True},
            ],
            "night_markets": [],
        }),
        encoding="utf-8",
    )
    engine._config_path = config_path
    removed = []

    async def remove_market_runtime(token_id, reason):
        removed.append((token_id, reason))
        engine.market_cfg.pop("101", None)
        engine.market_cfg.pop("102", None)
        return True

    engine.remove_market_runtime = remove_market_runtime

    asyncio.run(
        PolyLPSMulti._deactivate_market(
            engine,
            "101",
            "sponsored_guard:test",
        )
    )

    config = json.loads(config_path.read_text(encoding="utf-8"))
    assert all(market["enabled"] is False for market in config["markets"])
    assert removed == [("101", "deactivated:sponsored_guard:test")]
    assert engine.market_cfg == {}
