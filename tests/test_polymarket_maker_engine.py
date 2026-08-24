import asyncio
from decimal import Decimal
import json
from pathlib import Path
import sys
import time
from unittest.mock import AsyncMock

import pytest


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

import engine as engine_module  # noqa: E402
from account_profiles import parse_lp_account_profile  # noqa: E402
from engine import (  # noqa: E402
    EVENT_ACTIVE,
    EVENT_CANCELING,
    EVENT_COOLDOWN,
    EVENT_DEFENSIVE,
    EVENT_EXIT_PENDING,
    EVENT_HALTED_ON_FILL,
    EVENT_HALTED_ON_DATA,
    EVENT_PENDING_MANUAL_EXIT,
    EVENT_QUARANTINE,
    EVENT_WATCH,
    EventHaltPreempted,
    PolyLPSMulti,
    _ProxiedClobClient,
    _compute_quote_target_shares,
    _restore_activity_records,
)


class _RecordingEventBus:
    def __init__(self):
        self.events = []

    def publish(self, event_type, payload):
        self.events.append((event_type, payload))


def test_restore_activity_records_keeps_recent_history_only():
    fills = [{"id": index} for index in range(140)]
    exits = [{"id": index} for index in range(120)]

    restored_fills, restored_exits = _restore_activity_records(
        {
            "account_index": 2,
            "fills": fills,
            "exit_records": exits,
            "pending_unwinds": [{"id": "must-not-restore"}],
        },
        2,
    )

    assert [row["id"] for row in restored_fills] == list(range(40, 140))
    assert [row["id"] for row in restored_exits] == list(range(20, 120))


def test_restore_activity_records_rejects_wrong_account_or_invalid_payloads():
    assert _restore_activity_records(
        {"account_index": 1, "fills": [{"id": 1}]},
        2,
    ) == ([], [])
    assert _restore_activity_records(
        {"account_index": "bad", "fills": [{"id": 1}]},
        2,
    ) == ([], [])
    assert _restore_activity_records(
        {"account_index": 2, "fills": "bad", "exit_records": [1, {"id": 2}]},
        2,
    ) == ([], [{"id": 2}])


def test_restore_activity_records_drops_legacy_aggregate_trade_accounting():
    fills, exits = _restore_activity_records(
        {
            "account_index": 1,
            "fills": [
                {
                    "token_id": "101",
                    "reason": "TRADES_POLL:legacy",
                    "size": 40928.76,
                    "price": 0.78,
                    "ts": 100,
                },
                {
                    "token_id": "102",
                    "reason": "TRADES_POLL:attributed",
                    "size": 50,
                    "size_source": "account_order_fill",
                    "ts": 200,
                },
                {
                    "token_id": "103",
                    "reason": "FILL_WS",
                    "size": 25,
                    "ts": 300,
                },
            ],
            "exit_records": [
                {
                    "token_id": "999",
                    "size": 40928.76,
                    "fill_price": 0.22,
                    "ts": 110,
                },
                {
                    "token_id": "102",
                    "size": 50,
                    "size_source": "position",
                    "ts": 210,
                },
            ],
        },
        1,
    )

    assert [row["token_id"] for row in fills] == ["102", "103"]
    assert [row["token_id"] for row in exits] == ["102"]


def _global_cooldown_engine() -> PolyLPSMulti:
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {"101": {}, "102": {}, "103": {}}
    engine._night_market_cfg = {"201": {}}
    engine._event_states = {
        "101": {"state": EVENT_COOLDOWN, "reason": "global_cooldown"},
        "102": {"state": EVENT_COOLDOWN, "reason": "balance_or_allowance"},
        "103": {"state": EVENT_ACTIVE, "reason": "planner_sync_complete"},
        "201": {"state": EVENT_COOLDOWN, "reason": "global_cooldown"},
    }
    engine._event_bus = _RecordingEventBus()
    engine._cooldown_until = time.time() - 1
    engine._require_recovery_gate = False
    return engine


def test_expired_global_cooldown_resumes_only_matching_markets():
    engine = _global_cooldown_engine()

    resumed = engine._resume_expired_global_cooldown_markets("recovered")

    assert resumed == 2
    assert engine._event_state_name("101") == EVENT_ACTIVE
    assert engine._event_state_name("201") == EVENT_ACTIVE
    assert engine._event_state_name("102") == EVENT_COOLDOWN
    assert engine._event_state_entry("102")["reason"] == "balance_or_allowance"
    assert engine._event_state_name("103") == EVENT_ACTIVE


def test_global_cooldown_does_not_resume_before_timer_or_recovery_gate():
    engine = _global_cooldown_engine()
    engine._cooldown_until = time.time() + 60

    assert engine._resume_expired_global_cooldown_markets("too_early") == 0
    assert engine._event_state_name("101") == EVENT_COOLDOWN

    engine._cooldown_until = time.time() - 1
    engine._require_recovery_gate = True
    assert engine._resume_expired_global_cooldown_markets("not_healthy") == 0
    assert engine._event_state_name("101") == EVENT_COOLDOWN


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


def test_managed_lp_account_caps_live_quote_capital_at_its_principal():
    engine = object.__new__(PolyLPSMulti)
    engine.min_order_size = Decimal("5")
    engine.max_quote_shares_per_market = Decimal("0")
    engine.lp_account_profile = parse_lp_account_profile(
        {
            "lp_account": {
                "profile_type": "aggressive",
                "target_principal_usdc": 50,
            }
        },
        3,
    )

    async def market_meta(_token_id):
        return {"rewardsMinSize": "5"}

    async def collateral_available():
        return Decimal("1000")

    engine._get_market_meta = market_meta
    engine._get_collateral_available = collateral_available

    bid, ask, warning = asyncio.run(
        engine._compute_target_shares("101", budget_pct=Decimal("0.95"))
    )

    assert (bid, ask) == (Decimal("47"), Decimal("47"))
    assert warning == ""


def test_legacy_lp_account_still_uses_full_live_balance():
    engine = object.__new__(PolyLPSMulti)
    engine.min_order_size = Decimal("5")
    engine.max_quote_shares_per_market = Decimal("0")
    engine.lp_account_profile = parse_lp_account_profile({}, 1)

    async def market_meta(_token_id):
        return {"rewardsMinSize": "5"}

    async def collateral_available():
        return Decimal("1000")

    engine._get_market_meta = market_meta
    engine._get_collateral_available = collateral_available

    bid, ask, warning = asyncio.run(
        engine._compute_target_shares("101", budget_pct=Decimal("0.95"))
    )

    assert (bid, ask) == (Decimal("950"), Decimal("950"))
    assert warning == ""


def test_price_legs_skip_when_reward_zone_has_no_passive_tick():
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {
        "101": {
            "tick": Decimal("0.01"),
            "spread": Decimal("0.01"),
            "min_distance_ticks": 1,
        }
    }
    engine._night_market_cfg = {}
    engine._token_slug_cache = {}

    prices = engine._build_price_legs(
        "101",
        engine_module.TopOfBook(
            best_bid=Decimal("0.50"),
            best_ask=Decimal("0.51"),
        ),
    )

    assert prices == []


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
                        "market_active": True,
                        "market_closed": False,
                        "market_archived": False,
                        "accepting_orders": True,
                        "market_end_ts": time.time() + 86400,
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


def test_runtime_dashboard_add_is_blocked_in_multi_account_roster_mode():
    engine = object.__new__(PolyLPSMulti)
    engine._runtime_market_updates_enabled = False

    with pytest.raises(ValueError, match="multi-account market coordinator"):
        engine._runtime_add_from_command(
            {"token_id": "101", "paired_token_id": "102"}
        )


def test_stable_replacement_revalidates_account_depth(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine.min_front_bid_notional_usdc = Decimal("2000")
    engine._eligibility_observer_path = tmp_path / "reward_observer_state.json"
    engine._eligibility_observer_path.write_text(
        json.dumps(
            {
                "generated_at": time.time(),
                "candidates": [
                    {
                        "token_id": "201",
                        "paired_token_id": "202",
                        "condition_id": "0xabc",
                        "verification_recommended": True,
                        "market_phase": "normal",
                        "market_active": True,
                        "market_closed": False,
                        "market_archived": False,
                        "accepting_orders": True,
                        "market_end_ts": time.time() + 86400,
                        "front_depth_status": "verified",
                        "front_depth_observed_at": time.time(),
                        "yes_front_bid_notional_usd": 1999,
                        "no_front_bid_notional_usd": 6000,
                        "stability_score": 90,
                        "fill_risk": 20,
                        "risk_adjusted_daily_roi_pct": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="below account minimum"):
        engine._validate_stable_replacement_candidate(
            {
                "token_id": "201",
                "paired_token_id": "202",
                "condition_id": "0xabc",
            },
            {
                "max_observer_age_sec": 900,
                "max_depth_age_sec": 600,
                "min_stability_score": 70,
                "max_fill_risk": 35,
                "min_risk_adjusted_daily_roi_pct": 0.1,
            },
        )


def _stable_replacement_engine(tmp_path: Path) -> PolyLPSMulti:
    engine = object.__new__(PolyLPSMulti)
    engine._runtime_market_updates_enabled = True
    engine._account_idx = 1
    engine.market_cfg = {"101": {"paired_token_id": "102"}, "102": {}}
    engine._night_market_cfg = {}
    engine._active_exit_orders = {}
    engine._pending_unwinds = {}
    engine.client = type("Client", (), {"get_open_orders": lambda self: []})()
    engine._config_path = tmp_path / "config_1.json"
    engine._config_path.write_text(
        json.dumps(
            {
                "markets": [
                    {"token_id": "101", "paired_token_id": "102"},
                    {
                        "token_id": "201",
                        "paired_token_id": "202",
                        "enabled": False,
                        "pending_activation": True,
                    },
                ],
                "night_markets": [],
            }
        ),
        encoding="utf-8",
    )
    candidate = {
        "token_id": "201",
        "paired_token_id": "202",
        "condition_id": "0xabc",
        "slug": "new-market",
        "question": "New market?",
        "rewards_max_spread": 0.05,
        "fill_risk": 20,
    }
    engine._load_stable_replacement_command = lambda _command: (
        {},
        {
            "retire": {"token_id": "101"},
            "add": {"token_id": "201", "paired_token_id": "202"},
        },
        candidate,
    )
    engine._get_token_position = AsyncMock(return_value=0.0)
    engine._cancel_token_orders = AsyncMock(return_value=True)
    engine._set_event_state = lambda *_args, **_kwargs: None
    engine._request_market_ws_resubscribe = lambda: None
    engine.send_discord = lambda *_args, **_kwargs: None
    engine._discord_market_name = lambda token: token

    def add_candidate(_market, _candidate, *, session, persist, notify):
        assert session == "day"
        assert persist is False
        assert notify is False
        engine.market_cfg["201"] = {"paired_token_id": "202"}
        engine.market_cfg["202"] = {}
        return True

    def drop_market(token_id):
        pair = str((engine.market_cfg.get(token_id) or {}).get("paired_token_id") or "")
        found = token_id in engine.market_cfg
        engine.market_cfg.pop(token_id, None)
        if pair:
            engine.market_cfg.pop(pair, None)
        return found

    engine._add_runtime_candidate = add_candidate
    engine._drop_market_runtime_state = drop_market
    return engine


def _stable_replacement_command() -> dict:
    return {
        "replacement_id": "a" * 64,
        "retire_token_id": "101",
        "target_config_section": "markets",
        "market": {
            "token_id": "201",
            "paired_token_id": "202",
            "condition_id": "0xabc",
        },
    }


def test_runtime_replacement_cancels_both_old_sides_before_atomic_swap(tmp_path):
    engine = _stable_replacement_engine(tmp_path)

    status = asyncio.run(
        engine._runtime_replace_from_command(_stable_replacement_command())
    )

    assert status == "replaced"
    assert set(engine.market_cfg) == {"201", "202"}
    assert [call.args[0] for call in engine._cancel_token_orders.await_args_list] == [
        "101",
        "102",
    ]
    assert engine._get_token_position.await_count == 4
    config = json.loads(engine._config_path.read_text(encoding="utf-8"))
    assert [row["token_id"] for row in config["markets"]] == ["201"]
    assert config["markets"][0]["enabled"] is True
    assert "pending_activation" not in config["markets"][0]
    assert config["markets"][0]["replaced_token_id"] == "101"


def test_runtime_replacement_preserves_the_retired_night_section(tmp_path):
    engine = _stable_replacement_engine(tmp_path)
    engine.market_cfg = {}
    engine._night_market_cfg = {
        "101": {"paired_token_id": "102"},
        "102": {},
    }
    engine._config_path.write_text(
        json.dumps(
            {
                "markets": [],
                "night_markets": [
                    {"token_id": "101", "paired_token_id": "102"},
                ],
            }
        ),
        encoding="utf-8",
    )

    def add_candidate(_market, _candidate, *, session, persist, notify):
        assert session == "night"
        assert persist is False
        assert notify is False
        engine._night_market_cfg["201"] = {"paired_token_id": "202"}
        engine._night_market_cfg["202"] = {}
        return True

    def drop_market(token_id):
        source = engine.market_cfg if token_id in engine.market_cfg else engine._night_market_cfg
        pair = str((source.get(token_id) or {}).get("paired_token_id") or "")
        found = token_id in source
        engine.market_cfg.pop(token_id, None)
        engine._night_market_cfg.pop(token_id, None)
        if pair:
            engine.market_cfg.pop(pair, None)
            engine._night_market_cfg.pop(pair, None)
        return found

    engine._add_runtime_candidate = add_candidate
    engine._drop_market_runtime_state = drop_market
    command = _stable_replacement_command()
    command["target_config_section"] = "night_markets"

    status = asyncio.run(engine._runtime_replace_from_command(command))

    assert status == "replaced"
    assert set(engine._night_market_cfg) == {"201", "202"}
    config = json.loads(engine._config_path.read_text(encoding="utf-8"))
    assert config["markets"] == []
    assert [row["token_id"] for row in config["night_markets"]] == ["201"]


def test_runtime_replacement_keeps_old_market_when_cancel_is_unconfirmed(tmp_path):
    engine = _stable_replacement_engine(tmp_path)
    engine._cancel_token_orders = AsyncMock(side_effect=[True, False])

    with pytest.raises(ValueError, match="cancellation is unconfirmed"):
        asyncio.run(
            engine._runtime_replace_from_command(_stable_replacement_command())
        )

    assert set(engine.market_cfg) == {"101", "102"}
    config = json.loads(engine._config_path.read_text(encoding="utf-8"))
    assert config["markets"][0]["token_id"] == "101"
    assert config["markets"][1]["pending_activation"] is True


def test_runtime_replacement_stops_if_an_order_appears_after_cancellation(tmp_path):
    engine = _stable_replacement_engine(tmp_path)
    responses = [
        [],
        [
            {
                "id": "late-order",
                "asset_id": "101",
                "side": "BUY",
                "status": "live",
            }
        ],
    ]
    engine.client = type(
        "Client",
        (),
        {"get_open_orders": lambda self: responses.pop(0)},
    )()

    with pytest.raises(ValueError, match="still has a live order"):
        asyncio.run(
            engine._runtime_replace_from_command(_stable_replacement_command())
        )

    assert set(engine.market_cfg) == {"101", "102"}
    config = json.loads(engine._config_path.read_text(encoding="utf-8"))
    assert config["markets"][0]["token_id"] == "101"


def test_runtime_command_partial_json_is_restored_for_retry(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine._runtime_command_parse_failures = {}
    engine._runtime_command_parse_retry_limit = 5
    processing = tmp_path / "dashboard-add-2.processing"
    processing.write_text("", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(processing.read_text(encoding="utf-8"))

    assert engine._defer_runtime_command_parse_error(
        processing,
        caught.value,
    ) is True
    assert not processing.exists()
    assert (tmp_path / "dashboard-add-2.json").exists()
    assert engine._runtime_command_parse_failures == {"dashboard-add-2": 1}


def test_runtime_command_corrupt_json_stops_retrying_at_limit(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine._runtime_command_parse_failures = {"dashboard-add-2": 1}
    engine._runtime_command_parse_retry_limit = 1
    processing = tmp_path / "dashboard-add-2.processing"
    processing.write_text("{", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError) as caught:
        json.loads(processing.read_text(encoding="utf-8"))

    assert engine._defer_runtime_command_parse_error(
        processing,
        caught.value,
    ) is False
    assert processing.exists()
    assert engine._runtime_command_parse_failures == {}


def test_runtime_command_failure_clears_pending_by_command_id(tmp_path):
    engine = object.__new__(PolyLPSMulti)
    engine._config_path = tmp_path / "config.json"
    engine._config_path.write_text(
        json.dumps(
            {
                "markets": [
                    {
                        "token_id": "101",
                        "enabled": False,
                        "pending_activation": True,
                        "pending_command_id": "dashboard-add-2",
                    },
                    {
                        "token_id": "201",
                        "enabled": False,
                        "pending_activation": True,
                        "pending_command_id": "other-command",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    engine._mark_runtime_pending_failed(
        "",
        "dashboard-add-2",
        "JSONDecodeError: incomplete command",
    )

    config = json.loads(engine._config_path.read_text(encoding="utf-8"))
    failed, untouched = config["markets"]
    assert failed["pending_activation"] is False
    assert failed["activation_error"] == "JSONDecodeError: incomplete command"
    assert untouched["pending_activation"] is True


def test_runtime_command_loop_processes_completed_retry_once(tmp_path, monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._runtime_command_dir = tmp_path / "runtime_commands_2"
    engine._runtime_command_dir.mkdir()
    engine._runtime_command_parse_failures = {}
    engine._runtime_command_parse_retry_limit = 5
    engine._running = True
    command_path = engine._runtime_command_dir / "dashboard-add-2.json"
    command_path.write_text("", encoding="utf-8")
    added = []
    results = []

    def add_market(market):
        added.append(dict(market))
        return "added"

    def write_result(command_id, payload):
        results.append((command_id, dict(payload)))
        engine._running = False

    engine._runtime_add_from_command = add_market
    engine._write_runtime_result = write_result
    engine._mark_runtime_pending_failed = lambda *_args: pytest.fail(
        "a partial command must not be rejected"
    )

    sleep_calls = 0

    async def complete_copy_after_retry(_delay):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls == 1:
            command_path.write_text(
                json.dumps(
                    {
                        "command_id": "dashboard-add-2",
                        "action": "add_market",
                        "market": {
                            "token_id": "101",
                            "paired_token_id": "102",
                        },
                    }
                ),
                encoding="utf-8",
            )

    monkeypatch.setattr(engine_module.asyncio, "sleep", complete_copy_after_retry)

    asyncio.run(engine.runtime_command_loop())

    assert added == [{"token_id": "101", "paired_token_id": "102"}]
    assert results == [
        (
            "dashboard-add-2",
            {"ok": True, "status": "added", "token_id": "101"},
        )
    ]
    assert not list(engine._runtime_command_dir.iterdir())


def test_runtime_command_loop_dispatches_confirmed_replacement(tmp_path, monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._runtime_command_dir = tmp_path / "runtime_commands_1"
    engine._runtime_command_dir.mkdir()
    engine._runtime_command_parse_failures = {}
    engine._runtime_command_parse_retry_limit = 5
    engine._running = True
    command = {
        "command_id": "dashboard-replace-1",
        "action": "replace_market",
        "replacement_id": "a" * 64,
        "retire_token_id": "101",
        "market": {"token_id": "201", "paired_token_id": "202"},
    }
    (engine._runtime_command_dir / "dashboard-replace-1.json").write_text(
        json.dumps(command),
        encoding="utf-8",
    )
    engine._runtime_replace_from_command = AsyncMock(return_value="replaced")
    results = []
    engine._write_runtime_result = lambda command_id, payload: results.append(
        (command_id, dict(payload))
    )
    engine._mark_runtime_pending_failed = lambda *_args: pytest.fail(
        "a valid replacement must not be rejected"
    )

    async def stop_after_dispatch(_delay):
        engine._running = False

    monkeypatch.setattr(engine_module.asyncio, "sleep", stop_after_dispatch)

    asyncio.run(engine.runtime_command_loop())

    engine._runtime_replace_from_command.assert_awaited_once_with(command)
    assert results == [
        (
            "dashboard-replace-1",
            {
                "ok": True,
                "status": "replaced",
                "token_id": "201",
                "retired_token_id": "101",
                "replacement_id": "a" * 64,
            },
        )
    ]


def test_cancel_quotes_preserves_unregistered_sell_exit():
    engine = object.__new__(PolyLPSMulti)
    orders = [
        {"id": "buy-1", "status": "live", "side": "BUY"},
        {"id": "sell-exit", "status": "live", "side": "SELL"},
    ]
    canceled_batches = []

    class Client:
        def get_open_orders(self):
            canceled = {
                order_id
                for batch in canceled_batches
                for order_id in batch
            }
            return [o for o in orders if o["id"] not in canceled]

        def cancel_orders(self, order_ids):
            canceled_batches.append(list(order_ids))

    class Registry:
        def clear_funder(self, *_args, **_kwargs):
            return None

    engine.client = Client()
    engine._active_exit_orders = {}
    engine._market_live_orders = {}
    engine._sibling_registry = Registry()
    engine._funder_lc = "account"
    engine._invalidate_all_orders_cache = lambda: None

    assert asyncio.run(engine._cancel_all_except_exit()) is True
    assert canceled_batches == [["buy-1"]]
    assert [o["id"] for o in engine.client.get_open_orders()] == ["sell-exit"]


def test_statusless_open_orders_are_handled_conservatively():
    assert engine_module._order_is_live({"id": "statusless"}) is True
    assert engine_module._order_is_live(
        {"id": "unknown", "status": "pending"}
    ) is True
    assert engine_module._order_is_live(
        {"id": "done", "status": "filled"}
    ) is False

    engine = object.__new__(PolyLPSMulti)
    orders = [
        {"id": "buy-statusless", "side": "BUY"},
        {"id": "sell-statusless", "side": "SELL"},
        {"id": "buy-filled", "status": "filled", "side": "BUY"},
    ]
    canceled_batches = []

    class Client:
        def get_open_orders(self):
            canceled = {
                order_id
                for batch in canceled_batches
                for order_id in batch
            }
            return [order for order in orders if order["id"] not in canceled]

        def cancel_orders(self, order_ids):
            canceled_batches.append(list(order_ids))

    class Registry:
        def clear_funder(self, *_args, **_kwargs):
            return None

    engine.client = Client()
    engine._active_exit_orders = {}
    engine._market_live_orders = {}
    engine._sibling_registry = Registry()
    engine._funder_lc = "account"
    engine._invalidate_all_orders_cache = lambda: None

    assert asyncio.run(engine._cancel_all_except_exit()) is True
    assert canceled_batches == [["buy-statusless"]]
    assert {order["id"] for order in engine.client.get_open_orders()} == {
        "sell-statusless",
        "buy-filled",
    }


def test_cancel_all_fails_closed_when_verification_response_is_invalid():
    engine = object.__new__(PolyLPSMulti)
    responses = [
        [{"id": "buy-1", "status": "live", "side": "BUY"}],
        None,
    ]

    class Client:
        def get_open_orders(self):
            return responses.pop(0)

        def cancel_orders(self, _order_ids):
            return None

    class Registry:
        def clear_funder(self, *_args, **_kwargs):
            return None

    engine.client = Client()
    engine._active_exit_orders = {}
    engine._market_live_orders = {}
    engine._sibling_registry = Registry()
    engine._funder_lc = "account"
    engine._invalidate_all_orders_cache = lambda: None

    assert asyncio.run(engine._cancel_all_except_exit()) is False


def test_order_path_fails_closed_while_account_is_paused():
    engine = object.__new__(PolyLPSMulti)
    engine._is_account_paused = lambda: True
    engine._halt_preemption_reason = lambda _token_id: None

    try:
        engine._ensure_order_path_open("101", "submit_pre_post:test")
    except EventHaltPreempted as exc:
        assert "account_paused" in str(exc)
    else:
        raise AssertionError("paused account must reject every BUY submission path")


def test_proxied_client_converts_batch_order_book_dicts():
    class Client:
        def get_order_books(self, _params):
            return [
                {
                    "market": "condition-1",
                    "asset_id": "101",
                    "timestamp": "123",
                    "hash": "hash",
                    "bids": [{"price": "0.40", "size": "10"}],
                    "asks": [{"price": "0.60", "size": "10"}],
                    "min_order_size": "5",
                    "neg_risk": False,
                    "tick_size": "0.01",
                    "last_trade_price": "0.50",
                }
            ]

    books = _ProxiedClobClient(Client(), object()).get_order_books(
        [{"token_id": "101"}]
    )

    assert len(books) == 1
    assert books[0].asset_id == "101"
    assert books[0].bids
    assert books[0].asks


def test_token_cleanup_uses_batch_cancel_and_confirms_remote_orders():
    engine = object.__new__(PolyLPSMulti)
    reads = [
        [
            {
                "id": "buy-1",
                "asset_id": "101",
                "side": "BUY",
                "status": "LIVE",
            },
            {
                "id": "sell-1",
                "asset_id": "101",
                "side": "SELL",
                "status": "LIVE",
            },
        ],
        [
            {
                "id": "sell-1",
                "asset_id": "101",
                "side": "SELL",
                "status": "LIVE",
            }
        ],
    ]

    class Client:
        def get_open_orders(self):
            return reads.pop(0)

        def cancel(self, _order_id):
            raise AssertionError("legacy single-order cancel must not be used")

    canceled = []

    async def cancel_order_ids(token_id, order_ids, reason):
        canceled.append((token_id, tuple(order_ids), reason))
        return True

    engine.client = Client()
    engine._cancel_order_ids = cancel_order_ids

    assert asyncio.run(
        engine._cancel_token_orders("101", reason="test_cleanup")
    ) is True
    assert canceled == [
        ("101", ("buy-1",), "test_cleanup:attempt_1")
    ]


def test_token_cleanup_fails_closed_when_remote_buy_remains_live():
    engine = object.__new__(PolyLPSMulti)

    class Client:
        def get_open_orders(self):
            return [
                {
                    "id": "buy-still-live",
                    "asset_id": "101",
                    "side": "BUY",
                    "status": "LIVE",
                }
            ]

    attempts = []

    async def cancel_order_ids(_token_id, order_ids, _reason):
        attempts.append(tuple(order_ids))
        return True

    engine.client = Client()
    engine._cancel_order_ids = cancel_order_ids

    assert asyncio.run(
        engine._cancel_token_orders(
            "101",
            reason="test_unconfirmed",
            max_attempts=2,
        )
    ) is False
    assert attempts == [
        ("buy-still-live",),
        ("buy-still-live",),
    ]


def test_token_cleanup_treats_missing_status_as_open():
    engine = object.__new__(PolyLPSMulti)
    reads = [
        [
            {
                "id": "buy-without-status",
                "asset_id": "101",
                "side": "BUY",
            }
        ],
        [],
    ]

    class Client:
        def get_open_orders(self):
            return reads.pop(0)

    attempts = []

    async def cancel_order_ids(_token_id, order_ids, _reason):
        attempts.append(tuple(order_ids))
        return True

    engine.client = Client()
    engine._cancel_order_ids = cancel_order_ids

    assert asyncio.run(
        engine._cancel_token_orders("101", reason="missing_status")
    ) is True
    assert attempts == [("buy-without-status",)]


def _cross_side_cancel_engine(cancel_result: bool):
    engine = object.__new__(PolyLPSMulti)
    engine._cross_side_cancel_inflight = {"102"}
    engine._volatility_tracker = {}
    engine._event_states = {
        "102": {
            "state": EVENT_ACTIVE,
            "reason": "init",
            "updated_at": 0,
        }
    }
    engine._event_bus = type(
        "EventBus",
        (),
        {"publish": lambda _self, *_args, **_kwargs: None},
    )()
    marked = []
    engine.cross_side_sentinel = type(
        "Sentinel",
        (),
        {"mark_cancelled": lambda _self, token_id: marked.append(token_id)},
    )()
    engine._notify_risk = lambda *_args, **_kwargs: None
    engine.notify_discord = lambda *_args, **_kwargs: None
    spawned = []
    kill_reasons = []
    cancel_calls = []

    async def cancel_risk_buys(_token_id, reason):
        cancel_calls.append((_token_id, reason))
        if not cancel_result:
            await kill_switch(f"risk_cancel_unconfirmed:{_token_id}:{reason}")
        return cancel_result

    async def kill_switch(_reason):
        kill_reasons.append(_reason)
        return None

    def spawn(coro, *, name):
        spawned.append(name)
        coro.close()
        return None

    engine._cancel_risk_buys = cancel_risk_buys
    engine.trigger_global_kill_switch = kill_switch
    engine._spawn_bg = spawn
    return engine, marked, spawned, kill_reasons, cancel_calls


def test_cross_side_cancel_blocks_requote_and_marks_only_after_confirmation():
    engine, marked, spawned, kill_reasons, cancel_calls = (
        _cross_side_cancel_engine(True)
    )

    result = asyncio.run(
        engine._execute_cross_side_cancel(
            "101",
            "102",
            "depth_drop",
            max_ask=10_000,
            current_ask=2_000,
            consumed_pct=0.8,
        )
    )

    assert result is True
    assert engine._event_state_name("102") == EVENT_WATCH
    assert engine._volatility_tracker["102"]["watch_enter_ts"] > 0
    assert marked == ["102"]
    assert spawned == []
    assert kill_reasons == []
    assert cancel_calls == [
        ("102", "cross_side_sentinel:101:depth_drop"),
    ]
    assert engine._cross_side_cancel_inflight == set()


def test_cross_side_cancel_failure_does_not_claim_success_and_escalates():
    engine, marked, spawned, kill_reasons, cancel_calls = (
        _cross_side_cancel_engine(False)
    )

    result = asyncio.run(
        engine._execute_cross_side_cancel(
            "101",
            "102",
            "depth_drop",
            max_ask=10_000,
            current_ask=2_000,
            consumed_pct=0.8,
        )
    )

    assert result is False
    assert engine._event_state_name("102") == EVENT_WATCH
    assert marked == []
    assert spawned == []
    assert cancel_calls == [
        ("102", "cross_side_sentinel:101:depth_drop"),
    ]
    assert kill_reasons == [
        "risk_cancel_unconfirmed:102:cross_side_sentinel:101:depth_drop"
    ]
    assert engine._cross_side_cancel_inflight == set()


def _risk_cancel_engine(results: list[bool]):
    engine = object.__new__(PolyLPSMulti)
    engine._market_live_orders = {}
    calls = []
    kills = []
    latency = []
    notifications = []

    async def cancel_token_orders(token_id, *, reason, max_attempts=3):
        calls.append((token_id, reason, max_attempts))
        return results.pop(0)

    async def kill_switch(reason):
        kills.append(reason)

    engine._cancel_token_orders = cancel_token_orders
    engine.trigger_global_kill_switch = kill_switch
    engine._mark_latency = lambda token_id, label: latency.append(
        (token_id, label)
    )
    engine._notify_risk = lambda title, **payload: notifications.append(
        (title, payload)
    )
    return engine, calls, kills, latency, notifications


def test_risk_cancel_confirms_before_reporting_orders_cleared():
    engine, calls, kills, latency, notifications = _risk_cancel_engine([True])

    result = asyncio.run(engine._cancel_risk_buys("101", "watch:bba_jump"))

    assert result is True
    assert calls == [("101", "watch:bba_jump", 3)]
    assert kills == []
    assert latency == [("101", "t_orders_cleared")]
    assert notifications == []


def test_risk_cancel_dispatches_cached_buys_before_official_verification():
    engine = object.__new__(PolyLPSMulti)
    engine._market_live_orders = {
        "101": [
            {
                "id": "cached-buy",
                "asset_id": "101",
                "side": "BUY",
                "status": "live",
                "price": "0.50",
            },
            {
                "id": "cached-sell",
                "asset_id": "101",
                "side": "SELL",
                "status": "live",
                "price": "0.60",
            },
        ]
    }
    sequence = []
    latency = []

    async def cancel_order_ids(token_id, order_ids, reason):
        sequence.append(("fast", token_id, tuple(order_ids), reason))
        return True

    async def cancel_token_orders(token_id, *, reason, max_attempts=3):
        sequence.append(("verify", token_id, reason, max_attempts))
        return True

    engine._cancel_order_ids = cancel_order_ids
    engine._cancel_token_orders = cancel_token_orders
    engine._mark_latency = lambda token_id, label: latency.append(
        (token_id, label)
    )

    assert asyncio.run(engine._cancel_risk_buys("101", "watch:bba_jump")) is True
    assert sequence == [
        (
            "fast",
            "101",
            ("cached-buy",),
            "watch:bba_jump:fast_cached",
        ),
        ("verify", "101", "watch:bba_jump", 3),
    ]
    assert latency == [("101", "t_orders_cleared")]


def test_risk_cancel_escalates_then_rechecks_official_orders():
    engine, calls, kills, latency, notifications = _risk_cancel_engine(
        [False, True]
    )

    result = asyncio.run(engine._cancel_risk_buys("101", "watch:bba_jump"))

    assert result is True
    assert calls == [
        ("101", "watch:bba_jump", 3),
        ("101", "watch:bba_jump:post_global_verify", 1),
    ]
    assert kills == ["risk_cancel_unconfirmed:101:watch:bba_jump"]
    assert latency == [("101", "t_orders_cleared")]
    assert notifications == [
        (
            "风险挂单撤销未确认",
            {"token": "101", "reason": "watch:bba_jump"},
        )
    ]


def _event_halt_engine(cancel_result: bool, initial_state: str = EVENT_ACTIVE):
    engine = object.__new__(PolyLPSMulti)
    states = [initial_state]
    reasons = []
    cancel_calls = []
    engine._event_locks = {"101": asyncio.Lock()}
    engine._halt_requested = {"101": None}
    engine._top_leg_defense_tasks = {}
    engine._event_state_name = lambda _token_id: states[-1]
    engine._set_event_state = (
        lambda _token_id, state, reason: (
            states.append(state),
            reasons.append(reason),
        )
    )
    engine._latency_flow_reset = lambda *_args, **_kwargs: None
    engine._mark_latency = lambda *_args, **_kwargs: None
    engine._fills_record = []
    engine._emit_latency_record = lambda *_args, **_kwargs: None
    engine._paired_token_cache = {}
    engine._market_live_orders = {}
    engine.market_cfg = {}

    async def get_live_orders(_token_id):
        raise AssertionError("risk halt must not depend on the local order cache")

    async def cancel_risk_buys(token_id, reason):
        cancel_calls.append((token_id, reason))
        return cancel_result

    engine._get_live_orders_fast = get_live_orders
    engine._cancel_risk_buys = cancel_risk_buys
    return engine, states, reasons, cancel_calls


def test_event_halt_does_not_claim_final_state_before_remote_confirmation():
    engine, states, reasons, cancel_calls = _event_halt_engine(False)

    asyncio.run(
        engine._request_event_halt(
            "101",
            EVENT_HALTED_ON_DATA,
            "bad_market_snapshot",
        )
    )

    assert states[-1] == EVENT_CANCELING
    assert EVENT_HALTED_ON_DATA not in states
    assert cancel_calls == [("101", "halt:bad_market_snapshot")]
    assert reasons[-1] == "bad_market_snapshot"


def test_event_halt_enters_final_state_after_remote_confirmation():
    engine, states, _reasons, cancel_calls = _event_halt_engine(True)

    asyncio.run(
        engine._request_event_halt(
            "101",
            EVENT_HALTED_ON_DATA,
            "bad_market_snapshot",
        )
    )

    assert states[-1] == EVENT_HALTED_ON_DATA
    assert cancel_calls == [("101", "halt:bad_market_snapshot")]


def test_confirmed_fill_promotes_data_halt_to_fill_halt():
    engine, states, _reasons, cancel_calls = _event_halt_engine(
        True,
        initial_state=EVENT_HALTED_ON_DATA,
    )

    asyncio.run(
        engine._request_event_halt(
            "101",
            EVENT_HALTED_ON_FILL,
            "late_confirmed_fill",
        )
    )

    assert states[-1] == EVENT_HALTED_ON_FILL
    assert cancel_calls == [("101", "halt:late_confirmed_fill")]


def _fill_signal_engine(state: str = EVENT_ACTIVE) -> PolyLPSMulti:
    engine = _paired_state_engine()
    engine._event_states["101"]["state"] = state
    engine._signal_seen_ts = {}
    engine._event_last_trigger_ts = {}
    engine._event_banned_until = {}
    engine.fill_debounce_sec = 15
    return engine


def test_confirmed_fill_signal_survives_defensive_states_and_ban_ttl():
    for state in (
        EVENT_WATCH,
        EVENT_QUARANTINE,
        EVENT_CANCELING,
        EVENT_HALTED_ON_DATA,
    ):
        engine = _fill_signal_engine(state)
        engine._event_banned_until[engine._event_key("101")] = time.time() + 3600

        assert engine._allow_signal("101", f"late-fill-{state}") is True


def test_confirmed_fill_signal_keeps_dedup_and_exit_ownership():
    engine = _fill_signal_engine()

    assert engine._allow_signal("101", "trade-1") is True
    assert engine._allow_signal("101", "trade-1") is False

    for state in (
        EVENT_HALTED_ON_FILL,
        EVENT_EXIT_PENDING,
        EVENT_PENDING_MANUAL_EXIT,
    ):
        engine = _fill_signal_engine(state)
        assert engine._allow_signal("101", f"blocked-{state}") is False


def _fill_halt_policy_engine(
    collateral_available: Decimal | None,
    runtime_floor_usdc: Decimal = Decimal("100"),
    balance_error: Exception | None = None,
):
    engine = object.__new__(PolyLPSMulti)
    engine.runtime_floor_usdc = runtime_floor_usdc
    engine._market_snapshots = {}
    engine.market_states = {}
    engine._event_bus = _RecordingEventBus()
    calls = {
        "global_cancel": 0,
        "halt": [],
        "parent_watch": [],
        "spawned": [],
        "balance_refresh": [],
    }

    async def available(force_refresh=False):
        calls["balance_refresh"].append(force_refresh)
        if balance_error is not None:
            raise balance_error
        return collateral_available

    async def global_cancel():
        calls["global_cancel"] += 1
        return True

    async def event_halt(
        token_id,
        final_state,
        reason,
        matched_size=None,
        matched_price=None,
        halt_key="t_fill_seen",
    ):
        calls["halt"].append(
            (
                token_id,
                final_state,
                reason,
                matched_size,
                matched_price,
                halt_key,
            )
        )

    async def parent_watch(token_id, reason, primary_decision):
        calls["parent_watch"].append((token_id, reason, primary_decision))

    def spawn(coro, *, name):
        calls["spawned"].append(name)
        coro.close()

    engine._get_collateral_available = available
    engine._cancel_all_except_exit = global_cancel
    engine._request_event_halt = event_halt
    engine._enter_parent_event_shock_watch = parent_watch
    engine._spawn_bg = spawn
    engine.notify_discord = lambda *_args, **_kwargs: None
    engine._format_fill_alert = lambda *_args, **_kwargs: "fill"
    return engine, calls


def test_fill_with_collateral_floor_halts_only_filled_event():
    engine, calls = _fill_halt_policy_engine(Decimal("765.54"))

    asyncio.run(
        engine._trigger_event_offline(
            "101",
            "test_fill",
            matched_size=Decimal("10"),
            matched_price=Decimal("0.50"),
        )
    )

    assert calls["balance_refresh"] == [True]
    assert calls["global_cancel"] == 0
    assert calls["halt"] == [
        (
            "101",
            EVENT_HALTED_ON_FILL,
            "test_fill",
            Decimal("10"),
            Decimal("0.50"),
            "t_fill_seen",
        )
    ]
    assert calls["parent_watch"] == [
        ("101", "fill:test_fill", "skip")
    ]
    assert calls["spawned"] == ["attempt_exit_sell:101"]


def test_fill_keeps_matched_price_when_current_ask_is_higher():
    engine, calls = _fill_halt_policy_engine(Decimal("765.54"))
    engine._market_snapshots["101"] = engine_module.MarketSnapshot(
        best_bid=Decimal("0.55"),
        best_ask=Decimal("0.56"),
    )
    attempt_exit = AsyncMock()

    def spawn(coro, *, name):
        calls["spawned"].append(name)
        coro.close()

    engine._attempt_exit_sell = attempt_exit
    engine._spawn_bg = spawn

    asyncio.run(
        engine._trigger_event_offline(
            "101",
            "test_fill",
            matched_size=Decimal("517.63"),
            matched_price=Decimal("0.53"),
        )
    )

    attempt_exit.assert_called_once_with(
        "101", Decimal("0.53"), Decimal("517.63"), "test_fill"
    )


def test_paired_exit_uses_complement_of_matched_price_not_source_book(monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._exit_delay_sec = 0
    engine._active_exit_orders = {}
    engine._event_states = {
        "101": {"state": EVENT_ACTIVE, "reason": "init", "updated_at": 0},
    }
    engine._event_bus = _RecordingEventBus()
    engine.market_cfg = {
        "101": {"paired_token_id": "102"},
        "102": {"paired_token_id": "101"},
    }
    engine._night_market_cfg = {}
    engine._market_snapshots = {
        "101": engine_module.MarketSnapshot(
            best_bid=Decimal("0.55"),
            best_ask=Decimal("0.56"),
        ),
        "102": engine_module.MarketSnapshot(
            best_bid=Decimal("0.42"),
            best_ask=Decimal("0.43"),
        ),
    }
    engine.market_states = {}
    engine._exit_dust_threshold = Decimal("0.5")
    engine._exit_max_loss_usd = Decimal("0")
    engine._exit_stop_loss_wait_sec = 3600
    engine._exit_retry_count = 1
    engine._pending_unwinds = []
    engine._running = True
    placed = []

    async def no_sleep(_seconds):
        return None

    async def cancel_token_orders(_token_id, *, reason):
        return True

    async def token_position(token_id):
        return 0.0 if token_id == "101" else 517.645966

    async def scan_for_position(_tokens):
        return "102", 517.645966

    async def place_sell(token_id, price, size):
        placed.append((token_id, price, size))
        return {"orderID": "exit-order"}

    def spawn(coro, *, name):
        coro.close()

    monkeypatch.setattr(engine_module.asyncio, "sleep", no_sleep)
    engine._cancel_token_orders = cancel_token_orders
    engine._get_token_position = token_position
    engine._scan_for_position = scan_for_position
    engine._place_sell_order = place_sell
    engine._spawn_bg = spawn
    engine.notify_discord = lambda *_args, **_kwargs: None
    engine.send_discord = lambda *_args, **_kwargs: None
    engine._discord_market_name = lambda token_id: token_id

    asyncio.run(
        engine._attempt_exit_sell(
            "101",
            Decimal("0.53"),
            Decimal("517.63"),
            "test_fill",
        )
    )

    assert placed == [
        ("102", Decimal("0.47"), Decimal("517.645966"))
    ]
    unwind = engine._pending_unwinds[-1]
    assert unwind["fill_price"] == pytest.approx(0.47)
    assert unwind["source_token_id"] == "101"
    assert unwind["fill_size"] == pytest.approx(517.645966)
    assert unwind["exit_size"] == pytest.approx(517.645966)
    assert unwind["reported_fill_size"] == pytest.approx(517.63)


@pytest.mark.parametrize(
    ("collateral_available", "runtime_floor_usdc", "refresh_expected"),
    [
        (Decimal("99.99"), Decimal("100"), True),
        (None, Decimal("100"), True),
        (Decimal("1000"), Decimal("0"), False),
    ],
)
def test_fill_uses_global_cancel_when_event_scoped_halt_is_not_safe(
    collateral_available,
    runtime_floor_usdc,
    refresh_expected,
):
    engine, calls = _fill_halt_policy_engine(
        collateral_available,
        runtime_floor_usdc,
    )

    asyncio.run(
        engine._trigger_event_offline(
            "101",
            "test_fill",
            matched_size=Decimal("10"),
            matched_price=Decimal("0.50"),
        )
    )

    assert calls["balance_refresh"] == (
        [True] if refresh_expected else []
    )
    assert calls["global_cancel"] == 1
    assert len(calls["halt"]) == 1
    assert calls["spawned"] == ["attempt_exit_sell:101"]


def test_fill_balance_refresh_exception_falls_back_to_global_cancel():
    engine, calls = _fill_halt_policy_engine(
        Decimal("1000"),
        balance_error=RuntimeError("balance API unavailable"),
    )

    asyncio.run(
        engine._trigger_event_offline(
            "101",
            "test_fill",
            matched_size=Decimal("10"),
            matched_price=Decimal("0.50"),
        )
    )

    assert calls["balance_refresh"] == [True]
    assert calls["global_cancel"] == 1
    assert len(calls["halt"]) == 1
    assert calls["spawned"] == ["attempt_exit_sell:101"]


def test_exit_does_not_place_sell_when_buy_cancellation_is_unconfirmed(
    monkeypatch,
):
    engine = object.__new__(PolyLPSMulti)
    engine._exit_delay_sec = 0
    engine._active_exit_orders = {}
    engine._event_states = {
        "101": {
            "state": EVENT_ACTIVE,
            "reason": "init",
            "updated_at": 0,
        }
    }
    engine._event_bus = type(
        "EventBus",
        (),
        {"publish": lambda _self, *_args, **_kwargs: None},
    )()
    notices = []
    spawned = []

    async def no_sleep(_seconds):
        return None

    async def cancel_token_orders(_token_id, *, reason):
        return False

    async def cancel_all_except_exit():
        return False

    async def kill_switch(_reason):
        return None

    def spawn(coro, *, name):
        spawned.append(name)
        coro.close()
        return None

    monkeypatch.setattr(engine_module.asyncio, "sleep", no_sleep)
    engine._cancel_token_orders = cancel_token_orders
    engine._cancel_all_except_exit = cancel_all_except_exit
    engine.trigger_global_kill_switch = kill_switch
    engine._spawn_bg = spawn
    engine.send_discord = notices.append

    asyncio.run(
        engine._attempt_exit_sell(
            "101",
            Decimal("0.50"),
            Decimal("10"),
            "test_fill",
        )
    )

    assert engine._event_state_name("101") == EVENT_PENDING_MANUAL_EXIT
    assert notices and "无法确认买单已经撤净" in notices[-1]
    assert spawned == ["exit_cancel_kill_switch:101"]


def _managed_trade_engine() -> PolyLPSMulti:
    engine = object.__new__(PolyLPSMulti)
    engine._managed_buy_order_ids = {"bot-buy-1"}
    engine._managed_buy_order_ids_order = ["bot-buy-1"]
    engine._managed_order_history_limit = 3
    return engine


def test_trade_classifier_ignores_manual_sell():
    engine = _managed_trade_engine()

    actionable, reason = engine._trade_is_managed_inventory_increase({
        "id": "manual-sell-trade",
        "side": "SELL",
        "taker_order_id": "manual-sell-order",
        "maker_orders": [],
    })

    assert actionable is False
    assert reason == "manual_or_external_sell"


def test_trade_classifier_accepts_managed_maker_buy_when_taker_side_is_sell():
    engine = _managed_trade_engine()

    actionable, reason = engine._trade_is_managed_inventory_increase({
        "id": "bot-fill",
        # CLOB top-level side may describe the taker. The maker order carries
        # the account's actual side.
        "side": "SELL",
        "trader_side": "MAKER",
        "maker_orders": [{
            "order_id": "bot-buy-1",
            "side": "BUY",
            "matched_amount": "50",
        }],
    })

    assert actionable is True
    assert reason == "managed_maker_buy"


def test_trade_details_use_matching_maker_amount_not_aggregate_trade_size():
    engine = _managed_trade_engine()

    actionable, reason, size, price, token_id = (
        engine._managed_inventory_increase_details({
            "id": "aggregate-market-trade",
            "side": "SELL",
            "size": "40928.76",
            "price": "0.78",
            "maker_orders": [{
                "order_id": "bot-buy-1",
                "side": "BUY",
                "matched_amount": "50",
                "price": "0.53",
                "asset_id": "101",
            }],
        })
    )

    assert actionable is True
    assert reason == "managed_maker_buy"
    assert size == Decimal("50")
    assert price == Decimal("0.53")
    assert token_id == "101"


def test_trade_details_sum_multiple_managed_maker_orders():
    engine = _managed_trade_engine()
    engine._managed_buy_order_ids.add("bot-buy-2")

    actionable, reason, size, price, token_id = (
        engine._managed_inventory_increase_details({
            "asset_id": "taker-asset",
            "side": "SELL",
            "size": "1000",
            "price": "0.90",
            "maker_orders": [
                {
                    "order_id": "bot-buy-1",
                    "asset_id": "101",
                    "side": "BUY",
                    "matched_amount": "20",
                    "price": "0.50",
                },
                {
                    "order_id": "bot-buy-2",
                    "asset_id": "101",
                    "side": "BUY",
                    "matched_amount": "30",
                    "price": "0.60",
                },
            ],
        })
    )

    assert actionable is True
    assert reason == "managed_maker_buy"
    assert size == Decimal("50")
    assert price == Decimal("0.56")
    assert token_id == "101"


def test_trade_poll_uses_attributed_maker_fill(monkeypatch):
    engine = _managed_trade_engine()
    engine._running = True
    engine.market_cfg = {"101": {}}
    engine._night_market_cfg = {}
    engine._seen_trade_ids = set()
    engine._seen_trade_ids_order = []
    engine._fills_seen = 0
    engine.fill_size_threshold = Decimal("0.1")
    engine._trade_poll_req_exc_streak = 0
    captured = []
    trade = {
        "id": "aggregate-market-trade",
        "asset_id": "taker-asset",
        "side": "SELL",
        "size": "40928.76",
        "price": "0.78",
        "maker_orders": [{
            "order_id": "bot-buy-1",
            "side": "BUY",
            "matched_amount": "50",
            "price": "0.53",
            "asset_id": "101",
        }],
    }

    class Client:
        calls = 0

        def get_trades(self):
            self.calls += 1
            return [] if self.calls == 1 else [trade]

    async def trigger(
        token_id,
        reason,
        size,
        price,
        *,
        matched_size_source="",
    ):
        captured.append((
            token_id,
            reason,
            size,
            price,
            matched_size_source,
        ))
        engine._running = False

    async def no_wait(_seconds):
        return None

    engine.client = Client()
    engine._allow_signal = lambda *_args: True
    engine._trigger_event_offline = trigger
    engine._is_req_exc = lambda _exc: False
    monkeypatch.setattr(engine_module.asyncio, "sleep", no_wait)

    asyncio.run(engine._trade_poll_watch())

    assert captured == [(
        "101",
        "TRADES_POLL:aggregate-market-trade",
        Decimal("50"),
        Decimal("0.53"),
        "account_order_fill",
    )]
    assert engine._fills_seen == 1


def test_trade_classifier_ignores_unmanaged_manual_buy():
    engine = _managed_trade_engine()

    actionable, reason = engine._trade_is_managed_inventory_increase({
        "id": "manual-buy-trade",
        "side": "BUY",
        "taker_order_id": "manual-buy-order",
        "maker_orders": [],
    })

    assert actionable is False
    assert reason == "unmanaged_buy"


def test_managed_order_history_is_bounded():
    engine = _managed_trade_engine()

    for order_id in ("bot-buy-2", "bot-buy-3", "bot-buy-4"):
        engine._track_managed_buy_order(order_id)

    assert engine._managed_buy_order_ids_order == [
        "bot-buy-2",
        "bot-buy-3",
        "bot-buy-4",
    ]
    assert engine._managed_buy_order_ids == {
        "bot-buy-2",
        "bot-buy-3",
        "bot-buy-4",
    }


def test_manual_sell_blocks_rebuy_and_only_cancels_managed_buy():
    engine = _managed_trade_engine()
    engine._active_exit_orders = {}
    engine._manual_exit_cooldown_sec = 900
    engine._manual_exit_event_until = {}
    engine._manual_exit_last_notice = {}
    engine._event_token_ids = lambda _token_id: ["101", "102"]
    engine._event_key = lambda _token_id: "event-1"
    orders = [
        {
            "id": "manual-sell",
            "status": "live",
            "side": "SELL",
            "asset_id": "101",
        },
        {
            "id": "bot-buy-1",
            "status": "live",
            "side": "BUY",
            "asset_id": "102",
        },
        {
            "id": "manual-buy",
            "status": "live",
            "side": "BUY",
            "asset_id": "101",
        },
    ]
    canceled = []

    async def all_orders():
        return orders

    async def cancel(_token_id, ids, reason):
        canceled.append((ids, reason))
        return True

    engine._get_all_orders_cached = all_orders
    engine._cancel_order_ids = cancel

    assert asyncio.run(engine._manual_exit_blocks_quote("101")) is True
    assert canceled == [(["bot-buy-1"], "manual_exit_protection")]
    assert engine._manual_exit_event_until["event-1"] > time.time()


def test_completed_manual_sell_cancels_managed_buys_and_starts_cooldown():
    engine = object.__new__(PolyLPSMulti)
    engine._manual_exit_cooldown_sec = 900
    engine._manual_exit_event_until = {}
    engine._managed_buy_order_ids = {"bot-buy-yes", "bot-buy-no"}
    engine._event_key = lambda _token_id: "101|102"
    engine._event_token_ids = lambda _token_id: ["101", "102"]
    engine._invalidate_all_orders_cache = lambda: None
    canceled = []

    async def all_orders():
        return [
            {
                "id": "bot-buy-yes",
                "status": "live",
                "side": "BUY",
                "asset_id": "101",
            },
            {
                "id": "bot-buy-no",
                "status": "live",
                "side": "BUY",
                "asset_id": "102",
            },
            {
                "id": "manual-buy",
                "status": "live",
                "side": "BUY",
                "asset_id": "101",
            },
        ]

    async def cancel(token_id, order_ids, reason):
        canceled.append((token_id, order_ids, reason))
        return True

    engine._get_all_orders_cached = all_orders
    engine._cancel_order_ids = cancel

    asyncio.run(engine._register_manual_sell_trade("101", "trade-1"))

    assert canceled == [
        ("101", ["bot-buy-yes"], "manual_sell_trade_protection"),
        ("102", ["bot-buy-no"], "manual_sell_trade_protection"),
    ]
    assert engine._manual_exit_event_until["101|102"] > time.time()


def test_balance_resize_plan_only_trims_oversized_managed_layers():
    engine = _budget_engine()
    engine.budget_reserve_safety_margin_usdc = Decimal("0")
    engine._managed_buy_order_ids = {
        "yes-top",
        "yes-back",
        "no-top",
        "no-back",
        "small-top",
    }
    engine._market_live_orders = {
        "101": [
            {
                "id": "yes-top",
                "side": "BUY",
                "status": "live",
                "asset_id": "101",
                "price": "0.60",
                "size": "400",
            },
            {
                "id": "yes-back",
                "side": "BUY",
                "status": "live",
                "asset_id": "101",
                "price": "0.55",
                "size": "500",
            },
        ],
        "102": [
            {
                "id": "no-top",
                "side": "BUY",
                "status": "live",
                "asset_id": "102",
                "price": "0.40",
                "size": "400",
            },
            {
                "id": "no-back",
                "side": "BUY",
                "status": "live",
                "asset_id": "102",
                "price": "0.35",
                "size": "500",
            },
        ],
        "201": [
            {
                "id": "small-top",
                "side": "BUY",
                "status": "live",
                "asset_id": "201",
                "price": "0.70",
                "size": "300",
            },
        ],
        "202": [],
    }
    engine._active_market_cfg = lambda: engine.market_cfg

    plan = engine._balance_resize_plan(Decimal("500"))

    assert len(plan) == 1
    assert plan[0]["event_key"] == "101|102"
    assert plan[0]["trim_by_token"] == {
        "101": ["yes-back"],
        "102": ["no-back"],
    }


def test_balance_resize_plan_can_include_safe_events_for_balance_increase():
    engine = _budget_engine()
    engine.budget_reserve_safety_margin_usdc = Decimal("0")
    engine._managed_buy_order_ids = {"event-one", "event-two"}
    engine._market_live_orders = {
        "101": [
            {
                "id": "event-one",
                "side": "BUY",
                "status": "live",
                "asset_id": "101",
                "price": "0.60",
                "size": "400",
            },
        ],
        "102": [],
        "201": [
            {
                "id": "event-two",
                "side": "BUY",
                "status": "live",
                "asset_id": "201",
                "price": "0.70",
                "size": "300",
            },
        ],
        "202": [],
    }
    engine._active_market_cfg = lambda: engine.market_cfg

    plan = engine._balance_resize_plan(
        Decimal("1000"),
        include_within_limit=True,
    )

    assert [item["event_key"] for item in plan] == ["101|102", "201|202"]
    assert all(item["oversized"] is False for item in plan)
    assert all(item["trim_by_token"] == {} for item in plan)


def test_balance_change_rebalances_without_position_scan_or_global_cancel(monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    balances = iter([Decimal("100"), Decimal("50")])
    published = []
    resized = []

    async def available(force_refresh=False):
        return next(balances)

    async def resize(previous, available):
        resized.append((previous, available))
        return {"status": "complete"}

    sleep_calls = 0

    async def no_wait(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 2:
            engine._running = False

    class EventBus:
        def publish(self, event, payload):
            published.append((event, payload))

    engine._get_collateral_available = available
    engine._rebalance_quotes_after_balance_change = resize
    engine.notify_discord = lambda *_args, **_kwargs: None
    engine._event_bus = EventBus()
    engine._cancel_all_except_exit = lambda: (_ for _ in ()).throw(
        AssertionError("balance resize must not globally cancel orders")
    )
    engine._scan_for_position = lambda *_args: (_ for _ in ()).throw(
        AssertionError("balance resize must not scan user positions")
    )
    monkeypatch.setattr(engine_module.asyncio, "sleep", no_wait)

    asyncio.run(engine._balance_drop_watch())

    assert published[0] == (
        "balance_change",
        {"prev": "100", "now": "50", "change": "-50"},
    )
    assert published[1][0] == "balance_drop"
    assert published[1][1]["drop"] == "50"
    assert resized == [(Decimal("100"), Decimal("50"))]


def test_managed_balance_change_above_principal_does_not_rebalance(monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine.lp_account_profile = parse_lp_account_profile(
        {
            "lp_account": {
                "profile_type": "aggressive",
                "target_principal_usdc": 50,
            }
        },
        1,
    )
    balances = iter([Decimal("1000"), Decimal("900")])
    published = []
    resized = []

    async def available(force_refresh=False):
        return next(balances)

    async def resize(previous, current):
        resized.append((previous, current))

    sleep_calls = 0

    async def no_wait(_seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls >= 2:
            engine._running = False

    class EventBus:
        def publish(self, event, payload):
            published.append((event, payload))

    engine._get_collateral_available = available
    engine._rebalance_quotes_after_balance_change = resize
    engine.notify_discord = lambda *_args, **_kwargs: None
    engine._event_bus = EventBus()
    monkeypatch.setattr(engine_module.asyncio, "sleep", no_wait)

    asyncio.run(engine._balance_drop_watch())

    assert published == []
    assert resized == []


def test_missing_exit_order_with_inventory_stays_pending():
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine._unwind_check_interval_sec = 0
    engine._unwind_max_age_sec = 14_400
    engine._exit_dust_threshold = 0.5
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
    engine._notify_attention = lambda *args, **kwargs: notifications.append((args, kwargs))
    engine._resume_halted_markets = lambda *_args: (_ for _ in ()).throw(
        AssertionError("inventory must not resume")
    )

    asyncio.run(engine.unwind_tracking_loop())

    assert len(engine._pending_unwinds) == 1
    assert engine._pending_unwinds[0]["missing_order_alerted"] is True
    assert "101" not in engine._active_exit_orders
    assert states[-1][1] == EVENT_PENDING_MANUAL_EXIT
    assert notifications


def test_missing_exit_order_with_dust_resumes_other_markets():
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine._unwind_check_interval_sec = 0
    engine._unwind_max_age_sec = 14_400
    engine._exit_dust_threshold = 0.5
    engine._active_exit_orders = {"101": "exit-1"}
    engine._pending_unwinds = [
        {
            "token_id": "101",
            "fill_price": 0.82,
            "fill_size": 56_140.56,
            "order_id": "exit-1",
            "placed_at": 1,
            "reason": "test",
        }
    ]
    states = []
    notifications = []
    resumes = []

    class Client:
        def get_open_orders(self):
            return []

    async def position(_token_id):
        engine._running = False
        return 0.004392

    engine.client = Client()
    engine._get_token_position = position
    engine._set_event_state = lambda *args: states.append(args)
    engine._notify_attention = lambda *args, **kwargs: notifications.append((args, kwargs))
    engine._resume_halted_markets = lambda trigger: resumes.append(trigger)

    asyncio.run(engine.unwind_tracking_loop())

    assert len(engine._pending_unwinds) == 1
    assert engine._pending_unwinds[0]["dust_alerted"] is True
    assert "101" not in engine._active_exit_orders
    assert states == [("101", EVENT_PENDING_MANUAL_EXIT, "dust_position_after_unwind")]
    assert resumes == ["unwind_dust_position"]
    assert notifications[0][1]["position"] == "0.0044 份"
    assert "其他市场已自动恢复" in notifications[0][1]["action"]


def test_zero_position_clears_unwind_and_active_exit():
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine._unwind_check_interval_sec = 0
    engine._unwind_max_age_sec = 14_400
    engine._exit_dust_threshold = 0.5
    engine._active_exit_orders = {"101": "exit-1"}
    engine._exit_records = []
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
    resumes = []

    class Client:
        def get_open_orders(self):
            return []

    async def position(_token_id):
        engine._running = False
        return 0.0

    engine.client = Client()
    engine._get_token_position = position
    activations = []
    engine._activate_resolved_exit_tokens = (
        lambda token_ids, trigger: activations.append((token_ids, trigger))
    )
    engine._resume_halted_markets = lambda trigger: resumes.append(trigger)

    asyncio.run(engine.unwind_tracking_loop())

    assert engine._pending_unwinds == []
    assert engine._active_exit_orders == {}
    assert len(engine._exit_records) == 1
    assert activations == [({"101"}, "unwind_position_zero")]
    assert resumes == ["unwind_position_zero"]


def test_unknown_unwind_position_does_not_resume():
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine._unwind_check_interval_sec = 0
    engine._unwind_max_age_sec = 14_400
    engine._exit_dust_threshold = 0.5
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

    class Client:
        def get_open_orders(self):
            return []

    async def position(_token_id):
        engine._running = False
        return -1.0

    engine.client = Client()
    engine._get_token_position = position
    engine._set_event_state = lambda *_args: None
    engine._notify_attention = lambda *_args, **_kwargs: None
    engine._resume_halted_markets = lambda *_args: (_ for _ in ()).throw(
        AssertionError("unknown position must not resume")
    )

    asyncio.run(engine.unwind_tracking_loop())

    assert len(engine._pending_unwinds) == 1
    assert engine._pending_unwinds[0]["missing_order_alerted"] is True


def test_top_leg_defense_skips_preempted_token_before_market_reads():
    engine = object.__new__(PolyLPSMulti)
    engine._top_leg_defense_active = set()
    engine._top_leg_defense_pending = {}
    engine._top_leg_defense_tasks = {}
    engine._halt_preemption_reason = lambda _token_id: "fill:trade_match"
    engine._defense_blocks_requote = lambda _token_id: False
    engine._effective_snapshot_for_gate = lambda *_args: (_ for _ in ()).throw(
        AssertionError("preempted defense must not read market data")
    )

    asyncio.run(
        engine._maybe_run_top_leg_defense(
            "101",
            "market_ws:price_change",
            object(),
        )
    )

    assert engine._top_leg_defense_active == set()
    assert engine._top_leg_defense_tasks == {}


def test_top_leg_defense_is_quiet_while_account_is_paused():
    engine = object.__new__(PolyLPSMulti)
    engine._top_leg_defense_active = set()
    engine._top_leg_defense_pending = {}
    engine._top_leg_defense_tasks = {}
    engine._is_account_paused = lambda: True
    engine._halt_preemption_reason = lambda _token_id: (_ for _ in ()).throw(
        AssertionError("paused defense must stop before market checks")
    )

    asyncio.run(
        engine._maybe_run_top_leg_defense(
            "101",
            "market_ws:price_change",
            object(),
        )
    )

    assert engine._top_leg_defense_active == set()
    assert engine._top_leg_defense_tasks == {}


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


def _exit_resume_engine() -> PolyLPSMulti:
    engine = _paired_state_engine()
    engine._event_bus = _RecordingEventBus()
    engine.market_cfg["201"] = {}
    engine._event_states.update({
        "101": {"state": EVENT_EXIT_PENDING, "reason": "exit_sell"},
        "102": {"state": EVENT_HALTED_ON_FILL, "reason": "fill"},
        "201": {"state": EVENT_HALTED_ON_FILL, "reason": "global_fill_halt"},
    })
    engine._halt_requested = {
        "101": "old_fill",
        "102": "old_fill",
        "201": "old_fill",
    }
    engine._active_exit_orders = {"102": "exit-order"}
    engine._pending_unwinds = [{
        "token_id": "102",
        "source_token_id": "101",
        "fill_price": 0.53,
        "fill_size": 50,
        "exit_size": 50,
        "reported_fill_size": 40928.76,
        "sell_price": 0.54,
    }]
    engine._exit_records = []
    engine._night_market_cfg = {}
    engine._cooldown_until = time.time() + 60
    engine._require_recovery_gate = True
    engine._exit_recovery_protection_sec = 120
    engine._exit_recovery_protection_until = {}
    return engine


def test_resume_halted_markets_clears_only_resolved_event_preemption():
    engine = _exit_resume_engine()

    engine._resume_halted_markets("unrelated_resume")

    assert engine._event_state_name("101") == EVENT_EXIT_PENDING
    assert engine._event_state_name("102") == EVENT_HALTED_ON_FILL
    assert engine._halt_requested["102"] == "old_fill"
    assert engine._event_state_name("201") == EVENT_ACTIVE
    assert engine._halt_requested["201"] is None


def test_pause_resume_clears_only_stale_active_preemption():
    engine = _exit_resume_engine()
    engine._event_states["201"]["state"] = EVENT_ACTIVE

    cleared = engine._clear_stale_active_halt_preemptions("dashboard_resume")

    assert cleared == 1
    assert engine._halt_requested["201"] is None
    assert engine._halt_requested["101"] == "old_fill"
    assert engine._halt_requested["102"] == "old_fill"


def test_completed_exit_consumes_tracking_and_reactivates_both_event_tokens():
    engine = _exit_resume_engine()
    engine._active_exit_orders.pop("102")

    resolved = engine._consume_resolved_unwinds("102")
    resumed = engine._activate_resolved_exit_tokens(
        resolved,
        "position_zero",
    )

    assert resolved == {"101", "102"}
    assert resumed == 2
    assert engine._pending_unwinds == []
    assert engine._event_state_name("101") == EVENT_ACTIVE
    assert engine._event_state_name("102") == EVENT_ACTIVE
    assert engine._halt_requested["101"] is None
    assert engine._halt_requested["102"] is None


def test_completed_exit_does_not_override_intentional_watch_state():
    engine = _exit_resume_engine()
    engine._active_exit_orders = {}
    engine._pending_unwinds = []
    engine._event_states["101"]["state"] = EVENT_WATCH

    resumed = engine._activate_resolved_exit_tokens({"101"}, "position_zero")

    assert resumed == 0
    assert engine._event_state_name("101") == EVENT_WATCH
    assert engine._halt_requested["101"] == "old_fill"


def test_exit_record_uses_actual_exit_size_not_reported_market_trade_size():
    engine = _exit_resume_engine()

    engine._record_exit("102")

    assert len(engine._exit_records) == 1
    record = engine._exit_records[0]
    assert record["size"] == pytest.approx(50)
    assert record["reported_size"] == pytest.approx(40928.76)
    assert record["loss"] == pytest.approx(-0.5)


def test_finalize_exit_with_unknown_position_keeps_tracking_and_no_record():
    engine = _exit_resume_engine()

    async def stable(_token_id):
        return True

    async def unknown_position(_token_id):
        return None

    engine._await_balance_stable = stable
    engine._get_token_position = unknown_position
    engine._resume_halted_markets = lambda _trigger: (_ for _ in ()).throw(
        AssertionError("unknown position must not resume markets")
    )
    engine.send_fill_discord = lambda _message: (_ for _ in ()).throw(
        AssertionError("unknown position must not report completion")
    )

    asyncio.run(engine._finalize_exit_resume("102"))

    assert engine._pending_unwinds
    assert engine._exit_records == []
    assert engine._event_state_name("101") == EVENT_EXIT_PENDING
    assert engine._event_state_name("102") == EVENT_HALTED_ON_FILL


def test_exit_pending_blocks_both_sides_of_the_same_event():
    engine = _paired_state_engine()
    engine._event_states["102"]["state"] = EVENT_EXIT_PENDING

    assert engine._event_blocks_quote("102") is True
    assert engine._event_blocks_quote("101") is True
    assert engine._halt_preemption_reason("101").startswith(
        "paired_event_state=EXIT_PENDING"
    )


def test_aggressive_pair_token_is_scoped_to_isolated_runtime():
    engine = _paired_state_engine()
    engine._runtime_scope = "normal"

    assert engine._aggressive_pair_token("101") == ""

    engine._runtime_scope = "aggressive"
    assert engine._aggressive_pair_token("101") == "102"


def test_aggressive_pair_preflight_checks_mid_price_sibling_gate():
    engine = _paired_state_engine()
    engine._dual_side_max_mid = Decimal("0.10")
    engine._market_snapshots = {
        "102": type(
            "Snapshot",
            (),
            {"best_bid": Decimal("0.55"), "best_ask": Decimal("0.56")},
        )(),
    }
    engine._market_meta_cache = {"102": ({"rewardsMinSize": 200}, time.time())}
    engine._build_price_legs = lambda *_args, **_kwargs: [Decimal("0.54")]
    engine._effective_snapshot_for_gate = lambda _token_id, snapshot: snapshot
    engine._quote_gate = lambda *_args, **_kwargs: (True, "")
    engine._feasibility_gate = lambda *_args, **_kwargs: {"can_quote": False}
    engine._last_balance = Decimal("200")

    assert engine._paired_side_ready(
        "101",
        "102",
        Decimal("0.40"),
    ) == (True, "")
    assert engine._paired_side_ready(
        "101",
        "102",
        Decimal("0.40"),
        enforce_all_pairs=True,
    ) == (False, "paired_side_gate_failed")


def test_aggressive_pair_preflight_rejects_invalid_sibling_snapshot():
    engine = _paired_state_engine()
    engine._dual_side_max_mid = Decimal("0.10")
    engine._market_snapshots = {
        "102": type(
            "Snapshot",
            (),
            {"best_bid": Decimal("0.55"), "best_ask": Decimal("0.56")},
        )(),
    }
    engine._effective_snapshot_for_gate = lambda _token_id, snapshot: snapshot
    engine._quote_gate = lambda *_args, **_kwargs: (False, "snapshot_stale")

    assert engine._paired_side_ready(
        "101",
        "102",
        Decimal("0.40"),
        enforce_all_pairs=True,
    ) == (False, "paired_side_quote_gate:snapshot_stale")


def test_aggressive_pair_budget_uses_actual_two_leg_notional():
    engine = _paired_state_engine()
    engine._dual_side_max_mid = Decimal("0.10")
    engine.min_order_size = Decimal("5")
    engine._market_snapshots = {
        "102": type(
            "Snapshot",
            (),
            {"best_bid": Decimal("0.55"), "best_ask": Decimal("0.56")},
        )(),
    }
    engine._market_meta_cache = {
        "101": ({"rewardsMinSize": 200}, time.time()),
        "102": ({"rewardsMinSize": 200}, time.time()),
    }
    engine._build_price_legs = lambda *_args, **_kwargs: [Decimal("0.56")]
    engine._effective_snapshot_for_gate = lambda _token_id, snapshot: snapshot
    engine._quote_gate = lambda *_args, **_kwargs: (True, "")
    engine._feasibility_gate = lambda *_args, **_kwargs: {"can_quote": True}
    engine._last_balance = Decimal("201.688043")
    engine._token_slug_cache = {}

    assert engine._paired_side_ready(
        "101",
        "102",
        Decimal("0.41"),
        enforce_all_pairs=True,
    ) == (True, "")

    engine._last_balance = Decimal("197")
    assert engine._paired_side_ready(
        "101",
        "102",
        Decimal("0.41"),
        enforce_all_pairs=True,
    ) == (False, "paired_side_budget_insufficient")


def test_aggressive_pair_cancel_preempts_and_clears_both_quote_legs():
    engine = _paired_state_engine()
    engine._runtime_scope = "aggressive"
    engine._top_leg_defense_tasks = {}
    engine._defense_block_until = {}
    engine._defense_requote_block_sec = 15
    engine._event_bus = _RecordingEventBus()
    canceled = []

    async def cancel_risk_buys(token_id, reason):
        canceled.append((token_id, reason))
        return True

    engine._cancel_risk_buys = cancel_risk_buys

    assert asyncio.run(
        engine._cancel_aggressive_pair_quotes(
            "101",
            "feasibility_gate:front_depth_critical",
        )
    ) is True
    assert [token_id for token_id, _reason in canceled] == ["102", "101"]
    assert all("aggressive_pair:" in reason for _token_id, reason in canceled)
    assert engine._event_state_name("101") == EVENT_DEFENSIVE
    assert engine._event_state_name("102") == EVENT_DEFENSIVE
    assert engine._halt_requested == {"101": None, "102": None}
    assert engine._defense_blocks_requote("101") is True
    assert engine._defense_blocks_requote("102") is True


def test_aggressive_pair_cancel_stays_preempted_when_either_cancel_is_unconfirmed():
    engine = _paired_state_engine()
    engine._runtime_scope = "aggressive"
    engine._top_leg_defense_tasks = {}
    engine._defense_block_until = {}
    engine._defense_requote_block_sec = 15
    engine._event_bus = _RecordingEventBus()

    async def cancel_risk_buys(token_id, _reason):
        return token_id == "102"

    engine._cancel_risk_buys = cancel_risk_buys

    assert asyncio.run(
        engine._cancel_aggressive_pair_quotes(
            "101",
            "feasibility_gate:front_depth_critical",
        )
    ) is False
    assert engine._event_state_name("101") == EVENT_CANCELING
    assert engine._event_state_name("102") == EVENT_CANCELING
    assert engine._halt_requested["101"]
    assert engine._halt_requested["102"]


def _aggressive_pair_submit_engine() -> PolyLPSMulti:
    engine = _paired_state_engine()
    engine._runtime_scope = "aggressive"
    engine.market_cfg["103"] = {}
    engine._token_slug_cache = {
        "101": "example-yes",
        "102": "example-no",
        "103": "unrelated",
    }
    engine._global_order_lock = asyncio.Lock()
    engine._global_last_order_ts = 0.0
    engine._global_order_min_sec = 7.0
    engine._global_order_max_sec = 7.0
    engine._per_token_order_min_sec = 0.0
    engine._per_token_last_order_ts = {"101": 0.0, "102": 0.0, "103": 0.0}
    engine._aggressive_pair_submit_timeout_sec = 5.0
    engine._aggressive_pair_retry_cooldown_sec = 15.0
    engine._aggressive_pair_submit_lock = asyncio.Lock()
    engine._aggressive_pair_submit_seq = 0
    engine._aggressive_pair_submit_attempts = {}
    engine._aggressive_pair_submit_watchdogs = {}
    engine._market_budget_skip_until = {}
    engine._last_plan_sig = {}
    engine._last_top_plan_sig = {}
    engine._last_back_plan_sig = {}
    return engine


def test_aggressive_pair_follower_bypasses_only_global_order_throttle(monkeypatch):
    engine = _aggressive_pair_submit_engine()
    sleeps = []
    clock = [100.0]

    async def fake_sleep(delay):
        sleeps.append(delay)
        clock[0] += delay

    monkeypatch.setattr(engine_module.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(engine_module.time, "time", lambda: clock[0])

    async def scenario():
        leader_claim = await engine._acquire_order_throttle("101", "leader")
        assert leader_claim is not None and leader_claim[2] == "leader"
        attempt = await engine._aggressive_pair_attempt(leader_claim)
        attempt["posted"].add("101")
        attempt["first_post_at"] = time.time()
        attempt["leader_posted"].set()

        follower_claim = await engine._acquire_order_throttle("102", "follower")
        assert follower_claim is not None and follower_claim[2] == "follower"
        assert sleeps == []

        await engine._acquire_order_throttle("103", "unrelated")
        await engine._clear_aggressive_pair_submit(
            leader_claim[0],
            leader_claim[1],
        )

    asyncio.run(scenario())

    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(7.0, abs=0.1)


def test_aggressive_pair_submit_timeout_cancels_single_live_leg():
    engine = _aggressive_pair_submit_engine()
    engine._aggressive_pair_submit_timeout_sec = 0.0
    engine._cancel_aggressive_pair_quotes = AsyncMock(return_value=True)
    notices = []
    engine.notify_discord = lambda *args: notices.append(args)
    engine._invalidate_all_orders_cache = lambda: None

    async def refresh(token_id):
        if token_id == "101":
            return [{"id": "only-live-leg"}]
        return []

    engine._refresh_live_orders = refresh

    async def scenario():
        claim = await engine._claim_aggressive_pair_submit("101")
        attempt = await engine._aggressive_pair_attempt(claim)
        attempt["posted"].add("101")
        attempt["first_post_at"] = time.time()
        attempt["leader_posted"].set()
        await engine._aggressive_pair_submit_watchdog(
            claim[0],
            claim[1],
            "101",
        )

    asyncio.run(scenario())

    engine._cancel_aggressive_pair_quotes.assert_awaited_once()
    assert engine._aggressive_pair_submit_attempts == {}
    assert engine._market_budget_skip_until["101"] > time.time()
    assert engine._market_budget_skip_until["102"] > time.time()
    assert notices and notices[0][0] == "激进 LP 双腿建单失败"


def test_aggressive_pair_submit_watchdog_keeps_confirmed_pair():
    engine = _aggressive_pair_submit_engine()
    engine._aggressive_pair_submit_timeout_sec = 0.0
    engine._cancel_aggressive_pair_quotes = AsyncMock(return_value=True)
    engine.notify_discord = lambda *_args: None
    engine._invalidate_all_orders_cache = lambda: None

    async def refresh(token_id):
        return [{"id": f"live-{token_id}"}]

    engine._refresh_live_orders = refresh

    async def scenario():
        claim = await engine._claim_aggressive_pair_submit("101")
        attempt = await engine._aggressive_pair_attempt(claim)
        attempt["posted"].add("101")
        attempt["first_post_at"] = time.time()
        attempt["leader_posted"].set()
        await engine._aggressive_pair_submit_watchdog(
            claim[0],
            claim[1],
            "101",
        )

    asyncio.run(scenario())

    engine._cancel_aggressive_pair_quotes.assert_not_awaited()
    assert engine._aggressive_pair_submit_attempts == {}
    assert engine._market_budget_skip_until == {}


def test_aggressive_pair_both_posts_clear_watchdog_without_cancel():
    engine = _aggressive_pair_submit_engine()
    engine._aggressive_pair_submit_timeout_sec = 60.0
    engine._cancel_aggressive_pair_quotes = AsyncMock(return_value=True)

    async def scenario():
        leader_claim = await engine._claim_aggressive_pair_submit("101")
        follower_claim = await engine._claim_aggressive_pair_submit("102")
        await engine._aggressive_pair_submit_succeeded("101", leader_claim)
        watchdog = engine._aggressive_pair_submit_watchdogs[leader_claim[0]]
        await engine._aggressive_pair_submit_succeeded("102", follower_claim)
        await asyncio.sleep(0)
        return watchdog

    watchdog = asyncio.run(scenario())

    assert watchdog.cancelled()
    engine._cancel_aggressive_pair_quotes.assert_not_awaited()
    assert engine._aggressive_pair_submit_attempts == {}
    assert engine._aggressive_pair_submit_watchdogs == {}


def test_aggressive_pair_post_throttle_validation_failure_cleans_attempt():
    engine = _aggressive_pair_submit_engine()
    validations = 0

    async def validate(*_args):
        nonlocal validations
        validations += 1
        if validations == 2:
            raise RuntimeError("book moved")

    engine._validate_passive_buy_quote = validate
    engine._aggressive_pair_submit_failed = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="book moved"):
        asyncio.run(
            engine._preflight_post_order(
                "101",
                Decimal("0.40"),
                "moving-book",
            )
        )

    claim = engine._aggressive_pair_submit_failed.await_args.args[1]
    assert claim is not None and claim[2] == "leader"
    engine._aggressive_pair_submit_failed.assert_awaited_once_with(
        "101",
        claim,
        "post_throttle:moving-book:RuntimeError",
    )


def test_infeasible_reward_minimum_cancels_both_aggressive_quote_legs():
    engine = object.__new__(PolyLPSMulti)
    engine._gate_decisions = {}
    engine._market_budget_skip_until = {}
    engine.budget_skip_cooldown_sec = 120
    engine._cancel_aggressive_pair_quotes = AsyncMock(return_value=True)
    gate = {
        "can_quote": True,
        "size_cap": 0.5,
        "reason": ["sponsored_share_high"],
    }

    asyncio.run(
        engine._cancel_infeasible_quote_target(
            "101",
            "102",
            gate,
            "budget_below_min|required=200|size_cap=0.5",
        )
    )

    decision = engine._gate_decisions["101"]
    assert decision["can_quote"] is False
    assert decision["top_leg_action"] == "cancel"
    assert decision["reason"][-1].startswith("minimum_quote_infeasible:")
    assert engine._market_budget_skip_until["101"] > time.time()
    assert engine._market_budget_skip_until["102"] > time.time()
    engine._cancel_aggressive_pair_quotes.assert_awaited_once_with(
        "101",
        "minimum_quote_infeasible:budget_below_min|required=200|size_cap=0.5",
    )


def test_parent_event_metadata_groups_separate_conditions():
    engine = object.__new__(PolyLPSMulti)
    engine._market_parent_event_ids = {}
    engine._parent_event_tokens = {}
    normalized = {}

    parent_id = engine._remember_parent_event(
        ["101", "102"],
        {
            "events": [
                {
                    "id": "481717",
                    "slug": "fed-decision-in-september-762",
                }
            ]
        },
        normalized,
    )
    engine._remember_parent_event(
        ["201", "202"],
        {"events": [{"id": "481717"}]},
        {},
    )

    assert parent_id == "481717"
    assert normalized["parent_event_id"] == "481717"
    assert normalized["parent_event_slug"] == "fed-decision-in-september-762"
    assert engine._parent_event_tokens["481717"] == {
        "101",
        "102",
        "201",
        "202",
    }


def test_parent_event_cooldown_blocks_related_market_requote():
    engine = _paired_state_engine()
    engine._market_parent_event_ids = {"101": "fed", "102": "fed"}
    engine._parent_event_cooldown_until = {"fed": time.time() + 60}

    assert engine._event_blocks_quote("101") is True
    assert engine._halt_preemption_reason("102").startswith(
        "parent_event_cooldown=fed:"
    )


def _parent_event_guard_engine():
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {
        "101": {},
        "102": {},
        "201": {},
        "202": {},
    }
    engine._night_market_cfg = {}
    engine._market_parent_event_ids = {
        "101": "fed",
        "102": "fed",
        "201": "fed",
        "202": "fed",
    }
    engine._parent_event_tokens = {
        "fed": {"101", "102", "201", "202"},
    }
    engine._parent_event_cooldown_until = {}
    engine._parent_event_last_shock_ts = {}
    engine._market_live_orders = {
        token_id: [{"id": f"order-{token_id}", "side": "BUY"}]
        for token_id in engine.market_cfg
    }
    engine._parent_event_shock_guard_enabled = True
    engine._parent_event_shock_cooldown_sec = 1800
    engine._parent_event_shock_debounce_sec = 2
    engine._vol_watch_duration_sec = 120
    engine._repeat_defense_ban_count = 3
    engine.event_ban_ttl_sec = 86400
    engine._event_banned_until = {}
    engine._paired_token_cache = {}
    engine._event_states = {
        token_id: {
            "state": EVENT_ACTIVE,
            "reason": "init",
            "updated_at": 0,
        }
        for token_id in engine.market_cfg
    }
    engine._volatility_tracker = {
        token_id: {
            "front_notional_history": [],
            "defense_actions": [],
            "bba_prev": None,
            "watch_count": 0,
        }
        for token_id in engine.market_cfg
    }
    engine._latency_marks = {token_id: {} for token_id in engine.market_cfg}
    engine._latency_records = []
    engine._token_slug_cache = {}
    engine._event_bus = type(
        "EventBus",
        (),
        {"publish": lambda _self, *_args, **_kwargs: None},
    )()
    engine.send_discord = lambda *_args, **_kwargs: None
    engine._notify_risk = lambda *_args, **_kwargs: None
    engine._status_notifications = []
    engine._notify_status = lambda title, **fields: engine._status_notifications.append(
        (title, fields)
    )
    canceled = []

    async def get_live_orders(token_id):
        return [{"id": f"order-{token_id}", "side": "BUY"}]

    async def cancel_risk_buys(token_id, reason):
        canceled.append((token_id, (f"order-{token_id}",), reason))
        return True

    engine._get_live_orders_fast = get_live_orders
    engine._cancel_risk_buys = cancel_risk_buys
    return engine, canceled


def test_parent_event_shock_cancels_all_related_conditions_once():
    engine, canceled = _parent_event_guard_engine()

    asyncio.run(
        engine._enter_parent_event_shock_watch(
            "101",
            "bba_jump:test",
        )
    )
    asyncio.run(
        engine._enter_parent_event_shock_watch(
            "201",
            "bba_jump:duplicate",
        )
    )

    assert {token_id for token_id, _, _ in canceled} == {
        "101",
        "102",
        "201",
        "202",
    }
    assert all(
        engine._event_state_name(token_id) == EVENT_WATCH
        for token_id in engine.market_cfg
    )
    assert engine._parent_event_cooldown_until["fed"] > time.time() + 1700
    assert len(canceled) == 4
    assert engine._status_notifications == [
        (
            "关联市场已暂停",
            {
                "parent_event": "fed",
                "trigger": "101",
                "markets": 4,
                "cooldown_sec": 1800.0,
                "reason": "bba_jump:test",
            },
        )
    ]


def test_fill_shock_leaves_primary_condition_to_fill_halt_path():
    engine, canceled = _parent_event_guard_engine()
    engine._paired_token_cache = {
        "101": "102",
        "102": "101",
        "201": "202",
        "202": "201",
    }
    engine.market_cfg["101"]["paired_token_id"] = "102"
    engine.market_cfg["102"]["paired_token_id"] = "101"

    asyncio.run(
        engine._enter_parent_event_shock_watch(
            "101",
            "fill:test",
            primary_decision="skip",
        )
    )

    assert {token_id for token_id, _, _ in canceled} == {"201", "202"}
    assert engine._event_state_name("101") == EVENT_ACTIVE
    assert engine._event_state_name("102") == EVENT_ACTIVE
    assert engine._event_state_name("201") == EVENT_WATCH
    assert engine._event_state_name("202") == EVENT_WATCH


def test_quote_scoring_does_not_consume_bba_jump_baseline():
    engine = object.__new__(PolyLPSMulti)
    engine.market_cfg = {"101": {"tick": Decimal("0.01")}}
    engine._night_market_cfg = {}
    engine._vol_bba_jump_ticks = 2
    engine._volatility_tracker = {
        "101": {
            "front_notional_history": [],
            "defense_actions": [],
            "bba_prev": (Decimal("0.50"), Decimal("0.51")),
        }
    }

    assert engine._vol_check_bba_jump(
        "101",
        Decimal("0.52"),
        Decimal("0.53"),
        update_baseline=False,
    )
    assert engine._volatility_tracker["101"]["bba_prev"] == (
        Decimal("0.50"),
        Decimal("0.51"),
    )
    assert engine._vol_check_bba_jump(
        "101",
        Decimal("0.52"),
        Decimal("0.53"),
    )
    assert engine._volatility_tracker["101"]["bba_prev"] == (
        Decimal("0.52"),
        Decimal("0.53"),
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


def test_submit_uses_exchange_post_only_after_final_quote_validation():
    engine = object.__new__(PolyLPSMulti)
    calls = []

    class RemoteSigner:
        def sign_order(self, *_args):
            calls.append("signed")
            return object()

    class Client:
        def post_order(self, signed, order_type, **kwargs):
            calls.append(("posted", signed, order_type, kwargs))
            return {"orderID": "maker-order"}

    async def acquire(*_args):
        return "reserve"

    async def release(*_args):
        calls.append("released")

    async def validate(*_args):
        calls.append("validated")

    async def refresh(*_args):
        return []

    engine.remote_signer = RemoteSigner()
    engine.client = Client()
    engine._ensure_order_path_open = lambda *_args: None
    engine._sibling_gate = lambda _token, _side, price, _label: price
    engine._acquire_budget_reserve = acquire
    engine._release_budget_reserve = release
    engine._mark_latency = lambda *_args: None
    engine._mark_signer_recovered = lambda: None
    engine._validate_passive_buy_quote = validate
    engine._invalidate_all_orders_cache = lambda: None
    engine._sibling_register_resp = lambda *_args: None
    engine._refresh_live_orders = refresh

    response = asyncio.run(
        engine._submit_post_order(
            "101",
            Decimal("0.40"),
            Decimal("10"),
            "post-only-test",
        )
    )

    assert response == {"orderID": "maker-order"}
    assert calls[0:2] == ["signed", "validated"]
    post_call = calls[2]
    assert post_call[0] == "posted"
    assert post_call[3] == {"post_only": True}
    assert calls[-1] == "released"


def test_submit_does_not_post_when_final_quote_validation_fails():
    engine = object.__new__(PolyLPSMulti)
    posted = []

    class RemoteSigner:
        def sign_order(self, *_args):
            engine._market_snapshots["101"] = engine_module.MarketSnapshot(
                best_bid=Decimal("0.40"),
                best_ask=Decimal("0.41"),
                last_update_ts=time.time(),
            )
            return object()

    class Client:
        def post_order(self, *_args, **_kwargs):
            posted.append(True)
            return {"orderID": "must-not-post"}

    async def acquire(*_args):
        return "reserve"

    async def release(*_args):
        return None

    async def market_meta(*_args):
        return {"maxIncentiveSpread": "0.10"}

    async def start_guard(*_args, **_kwargs):
        return False

    engine.remote_signer = RemoteSigner()
    engine.client = Client()
    engine._ensure_order_path_open = lambda *_args: None
    engine._sibling_gate = lambda _token, _side, price, _label: price
    engine._acquire_budget_reserve = acquire
    engine._release_budget_reserve = release
    engine._mark_latency = lambda *_args: None
    engine._mark_signer_recovered = lambda: None
    engine._get_market_meta = market_meta
    engine._enforce_start_guard = start_guard
    engine._market_snapshots = {}
    engine._market_depth_snapshots = {}
    engine._market_snapshot_stale_sec = 5
    engine._token_slug_cache = {}
    engine.market_cfg = {
        "101": {
            "tick": Decimal("0.01"),
            "spread": Decimal("0.10"),
        }
    }
    engine._night_market_cfg = {}

    try:
        asyncio.run(
            engine._submit_post_order(
                "101",
                Decimal("0.40"),
                Decimal("10"),
                "moving-book-test",
            )
        )
    except RuntimeError as exc:
        assert "price_above_legal_top" in str(exc)
        pass
    else:
        raise AssertionError("a moved book must prevent order submission")

    assert posted == []


def test_cached_book_keeps_original_observation_time(monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._market_snapshots = {}
    engine._market_depth_snapshots = {}
    engine._market_snapshot_stale_sec = 5
    engine.market_states = {}
    engine._token_slug_cache = {}

    snapshot = engine._update_market_snapshot(
        "101",
        best_bid=Decimal("0.40"),
        best_ask=Decimal("0.41"),
        bids=[(Decimal("0.40"), Decimal("100"))],
        asks=[(Decimal("0.41"), Decimal("100"))],
        source="shared_batch",
        observed_ts=100.0,
    )

    assert snapshot is not None
    assert snapshot.last_update_ts == 100.0
    assert engine._market_depth_snapshots["101"].last_update_ts == 100.0
    monkeypatch.setattr(engine_module.time, "time", lambda: 106.0)
    assert engine._snapshot_is_stale("101", snapshot) is True


def test_signer_outage_triggers_fail_safe_after_threshold_and_throttles(monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._signer_failure_since = 0.0
    engine._signer_fail_safe_fired_at = 0.0
    engine.signer_fail_safe_after_sec = 30.0
    engine.signer_fail_safe_cooldown_sec = 120.0
    engine.cooldown_seconds = 60
    triggered = []

    async def trigger(reason):
        triggered.append(reason)

    engine.trigger_global_kill_switch = trigger
    clock = iter([100.0, 129.0, 131.0, 200.0, 252.0])
    monkeypatch.setattr(engine_module.time, "time", lambda: next(clock))

    for _ in range(5):
        asyncio.run(
            engine._handle_signer_failure("101", RuntimeError("offline"), "buy")
        )

    assert triggered == ["remote_signer_unreachable", "remote_signer_unreachable"]
    assert engine._signer_failure_since == 100.0
    assert engine._signer_fail_safe_fired_at == 252.0


def test_market_ws_outage_cancels_quotes_and_preserves_exit_path(monkeypatch):
    engine = object.__new__(PolyLPSMulti)
    engine._running = True
    engine._last_market_ws_ok_ts = 100.0
    engine._market_ws_down_cancel_sec = 30.0
    engine._proxy_failover_ws_down_trigger_sec = 300.0
    engine.market_cfg = {"101": {}}
    engine._last_plan_sig = {"101": "quoted"}
    engine.last_quote_ts = {"101": 123.0}
    cancelled = []
    notices = []

    async def cancel_all_except_exit():
        cancelled.append(True)

    async def stop_after_guard_tick(_seconds):
        engine._running = False

    engine._cancel_all_except_exit = cancel_all_except_exit
    engine._notify_attention = lambda title, **fields: notices.append((title, fields))
    monkeypatch.setattr(engine_module.time, "time", lambda: 140.0)
    monkeypatch.setattr(engine_module.asyncio, "sleep", stop_after_guard_tick)

    asyncio.run(engine.best_bid_guard_loop())

    assert cancelled == [True]
    assert notices == [
        (
            "Market WS down",
            {"age_sec": "40", "action": "cancelled quotes; preserved SELL exits"},
        )
    ]
    assert engine._last_plan_sig["101"] == ""
    assert engine.last_quote_ts["101"] == 0.0


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


def test_shutdown_cancels_maker_buys_through_exit_preserving_path():
    class Engine:
        def __init__(self):
            self.called = 0

        async def _cancel_all_except_exit(self):
            self.called += 1
            return True

    engine = Engine()

    asyncio.run(engine_module._cancel_maker_orders_for_shutdown(engine))

    assert engine.called == 1


def test_shutdown_fails_when_maker_order_cancellation_is_unverified():
    class Engine:
        async def _cancel_all_except_exit(self):
            return False

    with pytest.raises(RuntimeError, match="was not verified"):
        asyncio.run(engine_module._cancel_maker_orders_for_shutdown(Engine()))
