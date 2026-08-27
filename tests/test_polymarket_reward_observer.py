import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pytest

from platforms.polymarket.maker import reward_observer
from platforms.polymarket.maker.reward_observer import (
    ObserverAccountPolicy,
    observe_reward_markets,
    refresh_observer_state,
)


def _market(
    *,
    reward: str = "80",
    min_size: str = "10",
    slug: str = "will-rates-change-in-2026",
) -> dict:
    return {
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "endDate": "2099-12-31T23:59:00Z",
        "conditionId": "condition-1",
        "question": "Will rates change in 2026?",
        "slug": slug,
        "clobTokenIds": '["yes-token", "no-token"]',
        "clobRewards": [{"rewardsDailyRate": reward}],
        "rewardsMinSize": min_size,
        "rewardsMaxSpread": "5",
        "bestBid": "0.48",
        "bestAsk": "0.52",
        "oneDayPriceChange": "0.01",
        "oneHourPriceChange": "0.002",
    }


def _book(size: str = "5", *, tick_size="0.01") -> dict:
    book = {
        "bids": [{"price": "0.48", "size": size}],
        "asks": [{"price": "0.52", "size": size}],
    }
    if tick_size is not None:
        book["tick_size"] = tick_size
    return book


def _deep_book(size: str = "2500", *, tick_size: str = "0.01") -> dict:
    return {
        "tick_size": tick_size,
        "bids": [
            {"price": "0.48", "size": size},
            {"price": "0.47", "size": size},
            {"price": "0.46", "size": size},
        ],
        "asks": [{"price": "0.52", "size": size}],
    }


def _token_sample(
    token_id: str,
    order_ids: list[str],
    *,
    observed_at: float,
    scoring: bool,
) -> dict:
    live_hash = hashlib.sha256(
        json.dumps(
            sorted(order_ids),
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    material = {
        "token_id": token_id,
        "observed_at": round(float(observed_at), 6),
        "scoring": scoring,
        "live_order_ids_sha256": live_hash,
    }
    return {
        **material,
        "sample_id": hashlib.sha256(
            json.dumps(
                material,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }


def _account_policy(
    *,
    available: Decimal | None = Decimal("100"),
    min_depth: Decimal = Decimal("2000"),
    configured: bool = False,
    scoring: bool | None = None,
    observed_q: Decimal | None = None,
) -> ObserverAccountPolicy:
    tokens = frozenset({"yes-token", "no-token"}) if configured else frozenset()
    scoring_by_token = (
        {"yes-token": scoring, "no-token": scoring}
        if scoring is not None
        else {}
    )
    scoring_sample_by_token = (
        {"yes-token": "a" * 64, "no-token": "b" * 64}
        if scoring is not None
        else {}
    )
    scoring_sample_at_by_token = (
        {"yes-token": 1_800_000_000.0, "no-token": 1_800_000_000.0}
        if scoring is not None
        else {}
    )
    return ObserverAccountPolicy(
        account_index=1,
        account_id="account_01",
        available_usdc=available,
        available_source=("engine_balance" if available is not None else "unavailable"),
        min_distance_ticks=1,
        min_order_size=Decimal("5"),
        budget_pct=Decimal("1"),
        max_quote_shares=Decimal("0"),
        max_notional_per_order=Decimal("0"),
        min_front_bid_notional_usdc=min_depth,
        configured_tokens=tokens,
        market_runtime=(
            {
                "yes-token": {"q_min": str(observed_q)},
                "no-token": {"q_min": str(observed_q)},
            }
            if observed_q is not None
            else {}
        ),
        scoring_by_token=scoring_by_token,
        scoring_sample_by_token=scoring_sample_by_token,
        scoring_sample_at_by_token=scoring_sample_at_by_token,
        reward_percentages={},
        account_uid="137:1:0x" + "1" * 40,
        host_id="vps1",
    )


def _observe_with_history(
    tmp_path: Path,
    monkeypatch,
    *,
    policy: ObserverAccountPolicy,
    book: dict,
) -> dict:
    now = [1_800_000_000.0]
    monkeypatch.setattr(reward_observer.time, "time", lambda: now[0])
    state = {}
    for _ in range(12):
        state = observe_reward_markets(
            [_market()],
            lambda _token: book,
            account_policies=[policy],
        )
        reward_observer._apply_observation_history(
            tmp_path,
            state,
            now[0],
            {"stable_min_front_bid_notional_usdc": Decimal("2000")},
        )
        now[0] += 300
    return state["candidates"][0]


def test_observer_includes_rewards_below_old_hundred_dollar_gate() -> None:
    result = observe_reward_markets(
        [_market(reward="25")],
        lambda _token: _book(),
        probe_budget_usdc=Decimal("100"),
    )

    assert result["rewarded_markets_seen"] == 1
    assert result["candidates_ready"] == 1
    candidate = result["candidates"][0]
    assert candidate["daily_reward_usd"] == 25.0
    assert candidate["status"] == "observe_only"
    assert candidate["execution_status"] == "executable_observation"
    assert candidate["actual_reward_share_pct"] is None
    assert candidate["market_type"] == "always_on"
    assert candidate["market_active"] is True
    assert candidate["market_closed"] is False
    assert candidate["market_archived"] is False
    assert candidate["accepting_orders"] is True
    assert candidate["market_end_ts"] == reward_observer._timestamp(
        "2099-12-31T23:59:00Z"
    )


def test_observer_reserves_book_analysis_for_efficient_smaller_reward_pools() -> None:
    markets = []
    for index in range(12):
        market = _market(reward="100", slug=f"large-pool-{index}")
        market["conditionId"] = f"large-{index}"
        market["clobTokenIds"] = json.dumps(
            [f"large-yes-{index}", f"large-no-{index}"]
        )
        markets.append(market)
    for index in range(4):
        market = _market(reward="5", slug=f"small-pool-{index}")
        market["conditionId"] = f"small-{index}"
        market["clobTokenIds"] = json.dumps(
            [f"small-yes-{index}", f"small-no-{index}"]
        )
        market["rewardsMinSize"] = "1"
        markets.append(market)

    result = observe_reward_markets(
        markets,
        lambda _token: _book(),
        candidate_limit=8,
        lower_reward_reserve_ratio=Decimal("0.25"),
    )

    assert result["candidates_evaluated"] == 8
    assert result["selection_lanes"]["lower_reward_efficiency"] == 2
    assert sum(
        candidate["selection_lane"] == "lower_reward_efficiency"
        for candidate in result["candidates"]
    ) == 2
    assert any(candidate["daily_reward_usd"] == 5 for candidate in result["candidates"])


def test_configured_markets_bypass_candidate_limit_and_others_are_unassessed() -> None:
    markets = []
    for index in range(3):
        market = _market(reward=str(100 - index), slug=f"market-{index}")
        market["conditionId"] = f"condition-{index}"
        market["clobTokenIds"] = json.dumps(
            [f"yes-{index}", f"no-{index}"]
        )
        markets.append(market)
    policy = replace(
        _account_policy(),
        configured_tokens=frozenset({"yes-2", "no-2"}),
        configured_market_refs=(
            {
                "account_index": 1,
                "condition_id": "condition-2",
                "token_id": "yes-2",
                "paired_token_id": "no-2",
                "question": "Market 2?",
                "slug": "market-2",
            },
        ),
    )

    result = observe_reward_markets(
        markets,
        lambda _token: _book(),
        candidate_limit=1,
        account_policies=[policy],
    )

    assert result["candidates_evaluated"] == 2
    assert result["selection_lanes"]["configured_or_watchlist"] == 1
    assert "condition-2" in {
        row["condition_id"] for row in result["candidates"]
    }
    assert result["candidates_unassessed"] == 1
    assert result["unassessed_candidates"][0]["admission_level"] == "unassessed"
    assert result["unassessed_candidates"][0]["reason_codes"] == [
        "selection_budget_not_evaluated"
    ]


def test_configured_condition_bypasses_limit_when_token_ids_are_missing() -> None:
    markets = []
    for index in range(3):
        market = _market(reward=str(100 - index), slug=f"market-{index}")
        market["conditionId"] = f"condition-{index}"
        market["clobTokenIds"] = json.dumps(
            [f"yes-{index}", f"no-{index}"]
        )
        markets.append(market)
    policy = replace(
        _account_policy(),
        configured_tokens=frozenset(),
        configured_market_refs=(
            {
                "account_index": 1,
                "condition_id": "condition-2",
                "token_id": "",
                "paired_token_id": "",
                "question": "Market 2?",
                "slug": "market-2",
            },
        ),
    )

    result = observe_reward_markets(
        markets,
        lambda _token: _book(),
        candidate_limit=1,
        account_policies=[policy],
    )

    assert result["candidates_evaluated"] == 2
    assert result["selection_lanes"]["configured_or_watchlist"] == 1
    assert "condition-2" in {
        row["condition_id"] for row in result["candidates"]
    }


def test_estimated_daily_payout_floor_blocks_confirmation_not_observation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = [1_800_000_000.0]
    monkeypatch.setattr(reward_observer.time, "time", lambda: now[0])

    state = {}
    for _ in range(12):
        state = refresh_observer_state(
            tmp_path,
            fetch_markets=lambda: [_market(reward="0.50")],
            fetch_book=lambda _token: _book("1000"),
        )
        now[0] += 300

    candidate = state["candidates"][0]
    assert candidate["estimated_daily_gross_usd"] < 1
    assert candidate["min_estimated_daily_payout_usd"] == 1
    assert "estimated_daily_payout_below_floor" in candidate["reasons"]
    assert candidate["verification_status"] == "stable"
    assert candidate["verification_recommended"] is False


def test_observer_excludes_expired_market_even_when_gamma_still_accepts_orders() -> None:
    expired = _market(slug="ceasefire-by-july-31")
    expired["endDate"] = "2020-07-31T23:59:00Z"

    result = observe_reward_markets([expired], lambda _token: _book())

    assert result["rewarded_markets_seen"] == 0
    assert result["candidates"] == []


def test_observer_excludes_market_that_stopped_accepting_orders() -> None:
    market = _market()
    market["acceptingOrders"] = False

    result = observe_reward_markets([market], lambda _token: _book())

    assert result["rewarded_markets_seen"] == 0
    assert result["candidates"] == []


def test_candidate_url_uses_parent_event_and_market_slugs() -> None:
    market = _market(slug="will-rates-change-in-2026-outcome")
    market["events"] = [{"slug": "will-rates-change-in-2026"}]

    candidate = observe_reward_markets(
        [market],
        lambda _token: _book(),
    )["candidates"][0]

    assert candidate["event_slug"] == "will-rates-change-in-2026"
    assert candidate["market_url"] == (
        "https://polymarket.com/event/will-rates-change-in-2026/"
        "will-rates-change-in-2026-outcome"
    )


def test_candidate_without_parent_event_does_not_publish_broken_url() -> None:
    candidate = observe_reward_markets(
        [_market()],
        lambda _token: _book(),
    )["candidates"][0]

    assert candidate["event_slug"] == ""
    assert candidate["market_url"] == ""


def test_low_competition_market_estimates_better_share_and_return() -> None:
    low = observe_reward_markets(
        [_market()],
        lambda _token: _book("5"),
    )["candidates"][0]
    crowded = observe_reward_markets(
        [_market()],
        lambda _token: _book("1000"),
    )["candidates"][0]

    assert low["estimated_reward_share_pct"] > crowded["estimated_reward_share_pct"]
    assert low["estimated_daily_gross_usd"] > crowded["estimated_daily_gross_usd"]
    assert "estimated_majority_share" in low["reasons"]


def test_minimum_share_size_above_budget_is_not_reported_as_executable() -> None:
    candidate = observe_reward_markets(
        [_market(min_size="200")],
        lambda _token: _book(),
        probe_budget_usdc=Decimal("100"),
    )["candidates"][0]

    assert candidate["probe_capital_usd"] == 0
    assert candidate["probe_shares_each_side"] == 0
    assert candidate["executable_q_min"] == 0
    assert candidate["executable_reward_share_pct"] == 0
    assert candidate["status"] == "observe_only"
    assert candidate["execution_status"] == "blocked_observation"
    assert "minimum_size_exceeds_executable_budget" in candidate["reasons"]


def test_observer_records_engine_aligned_front_depth_for_both_legs() -> None:
    book = {
        "tick_size": "0.01",
        "bids": [
            {"price": "0.48", "size": "100"},
            {"price": "0.47", "size": "100"},
            {"price": "0.46", "size": "100"},
        ],
        "asks": [{"price": "0.52", "size": "100"}],
    }

    candidate = observe_reward_markets([_market()], lambda _token: book)["candidates"][0]

    assert candidate["front_depth_status"] == "verified"
    assert candidate["yes_safe_quote"] == 0.47
    assert candidate["no_safe_quote"] == 0.47
    assert candidate["yes_front_bid_notional_usd"] == 95.0
    assert candidate["no_front_bid_notional_usd"] == 95.0
    assert candidate["yes_front_bid_levels"] == 2
    assert candidate["no_front_bid_levels"] == 2


def test_observer_marks_missing_tick_depth_unavailable_without_dropping_candidate() -> None:
    candidate = observe_reward_markets(
        [_market()],
        lambda _token: _book(tick_size=None),
    )["candidates"][0]

    assert candidate["front_depth_status"] == "missing_tick_size"
    assert candidate["min_front_bid_notional_usd"] is None


def test_observer_rejects_mismatched_leg_ticks_for_depth_preflight() -> None:
    books = iter((_book(tick_size="0.01"), _book(tick_size="0.001")))

    candidate = observe_reward_markets([_market()], lambda _token: next(books))["candidates"][0]

    assert candidate["front_depth_status"] == "tick_size_mismatch"
    assert candidate["min_front_bid_notional_usd"] is None


def test_account_with_fresh_capital_and_deep_book_gets_full_admission(
    tmp_path: Path,
    monkeypatch,
) -> None:
    candidate = _observe_with_history(
        tmp_path,
        monkeypatch,
        policy=_account_policy(
            available=Decimal("200"),
            configured=True,
            scoring=True,
            observed_q=Decimal("10"),
        ),
        book=_deep_book(),
    )

    assert candidate["admission_level"] == "full"
    assert candidate["stable_lp_recommended"] is True
    assert candidate["account_admission"] == [
        {
            "account_index": 1,
            "level": "full",
            "reason_codes": ["account_executable_and_verified"],
            "canary_requires_scoring_validation": False,
        }
    ]


def test_shallow_but_executable_book_gets_canary_not_full(
    tmp_path: Path,
    monkeypatch,
) -> None:
    candidate = _observe_with_history(
        tmp_path,
        monkeypatch,
        policy=_account_policy(),
        book=_deep_book("100"),
    )

    assert candidate["admission_level"] == "canary"
    assert candidate["stable_lp_recommended"] is False
    assert candidate["canary_proposal_eligible"] is False
    assert "front_depth_below_full_minimum" in candidate["account_admission"][0][
        "reason_codes"
    ]


def test_configured_market_with_failed_official_scoring_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    candidate = _observe_with_history(
        tmp_path,
        monkeypatch,
        policy=_account_policy(configured=True, scoring=False),
        book=_deep_book(),
    )

    assert candidate["admission_level"] == "reject"
    assert candidate["stable_lp_recommended"] is False
    assert "official_order_scoring_false" in candidate["account_admission"][0][
        "reason_codes"
    ]


def test_scoring_evidence_ignores_closed_orders_and_uses_current_live_order():
    scores, samples, sample_times = reward_observer._scoring_evidence_by_token(
        {
            "token_samples": {
                "yes-token": _token_sample(
                    "yes-token",
                    ["live-current"],
                    observed_at=200,
                    scoring=False,
                )
            },
            "orders": {
                "closed-old": {
                    "order_id": "closed-old",
                    "token_id": "yes-token",
                    "live": False,
                    "last_scoring": True,
                    "observations": [
                        {
                            "status": "observed",
                            "scoring": True,
                            "observed_at": 100,
                        }
                    ],
                },
                "live-current": {
                    "order_id": "live-current",
                    "token_id": "yes-token",
                    "live": True,
                    "last_scoring": False,
                    "observations": [
                        {
                            "status": "observed",
                            "scoring": False,
                            "observed_at": 200,
                        }
                    ],
                },
            }
        },
        now_ts=200,
    )

    assert scores == {"yes-token": False}
    assert set(samples) == {"yes-token"}
    assert sample_times == {"yes-token": 200.0}


def test_scoring_sample_does_not_advance_when_live_membership_changes():
    payload = {
        "token_samples": {
            "yes-token": _token_sample(
                "yes-token",
                ["first", "second"],
                observed_at=500,
                scoring=True,
            )
        },
        "orders": {
            order_id: {
                "order_id": order_id,
                "token_id": "yes-token",
                "live": True,
                "last_scoring": True,
                "observations": [
                    {
                        "status": "observed",
                        "scoring": True,
                        "observed_at": 500,
                    }
                ],
            }
            for order_id in ("first", "second")
        }
    }
    _, samples_before, _ = reward_observer._scoring_evidence_by_token(
        payload,
        now_ts=500,
    )
    payload["orders"]["first"]["live"] = False
    _, samples_after, _ = reward_observer._scoring_evidence_by_token(
        payload,
        now_ts=501,
    )

    assert set(samples_before) == {"yes-token"}
    assert samples_after == {}


def test_scoring_evidence_rejects_stale_token_sample():
    payload = {
        "token_samples": {
            "yes-token": _token_sample(
                "yes-token",
                ["first", "second"],
                observed_at=500,
                scoring=True,
            )
        },
        "orders": {
            order_id: {
                "order_id": order_id,
                "token_id": "yes-token",
                "live": True,
                "last_scoring": True,
                "observations": [
                    {
                        "status": "observed",
                        "scoring": True,
                        "observed_at": observed_at,
                    }
                ],
            }
            for order_id, observed_at in (("first", 500), ("second", 501))
        }
    }

    scores, samples, sample_times = reward_observer._scoring_evidence_by_token(
        payload,
        now_ts=900,
    )
    assert scores == {"yes-token": None}
    assert samples == {}
    assert sample_times == {}


def test_pair_scoring_and_observed_q_require_both_current_legs():
    policy = replace(
        _account_policy(configured=True),
        market_runtime={"yes-token": {"q_min": "10"}},
        scoring_by_token={"yes-token": True},
        scoring_sample_by_token={"yes-token": "a" * 64},
        scoring_sample_at_by_token={"yes-token": 1_800_000_000.0},
    )

    candidate = observe_reward_markets(
        [_market()],
        lambda _token: _deep_book(),
        account_policies=[policy],
    )["candidates"][0]
    evidence = candidate["account_execution"][0]

    assert evidence["observed_q_min"] is None
    assert evidence["official_scoring"] is None
    assert evidence["scoring_sample_id"] is None


def test_pair_scoring_sample_requires_same_official_query_batch():
    policy = replace(
        _account_policy(
            configured=True,
            scoring=True,
            observed_q=Decimal("10"),
        ),
        scoring_sample_at_by_token={
            "yes-token": 1_800_000_000.0,
            "no-token": 1_800_000_001.0,
        },
    )

    candidate = observe_reward_markets(
        [_market()],
        lambda _token: _deep_book(),
        account_policies=[policy],
    )["candidates"][0]
    evidence = candidate["account_execution"][0]

    assert evidence["official_scoring"] is True
    assert evidence["scoring_sample_id"] is None
    assert evidence["scoring_sample_observed_at"] is None


def test_reward_percentages_cannot_cross_account_uid_boundary(tmp_path: Path):
    now = 1_800_000_000.0
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    data_dir.mkdir()
    (config_dir / "config_1.json").write_text(
        json.dumps(
            {
                "account": {
                    "funder": "0x" + "1" * 40,
                    "chain_id": 137,
                    "signature_type": 0,
                },
                "runtime": {"host_id": "vps1"},
                "markets": [],
                "night_markets": [],
            }
        ),
        encoding="utf-8",
    )
    (data_dir / "engine_state_1.json").write_text(
        json.dumps(
            {
                "generated_at": now,
                "balance": 100,
                "account_uid_key": reward_observer._account_uid_key(
                    "137:0:0x" + "1" * 40
                ),
                "runtime": {"host_id": "vps1"},
            }
        ),
        encoding="utf-8",
    )
    (data_dir / "rewards_live.json").write_text(
        json.dumps(
            {
                "generated_at": now,
                "accounts": {
                    "1": {
                        "account_uid": "137:0:0x" + "2" * 40,
                        "percentage_status": "ok",
                        "reward_percentages": {"condition-1": 42},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    scoring_path = data_dir / "order_scoring_state_1.json"
    scoring_payload = {
        "generated_at": now,
        "account_uid_key": "wrong-account",
        "host_id": "vps1",
        "token_samples": {
            "yes-token": _token_sample(
                "yes-token",
                ["order-1"],
                observed_at=now,
                scoring=True,
            )
        },
        "orders": {
            "order-1": {
                "order_id": "order-1",
                "token_id": "yes-token",
                "live": True,
                "last_scoring": True,
                "observations": [
                    {
                        "status": "observed",
                        "scoring": True,
                        "observed_at": now,
                    }
                ],
            }
        },
    }
    scoring_path.write_text(json.dumps(scoring_payload), encoding="utf-8")

    policy = reward_observer._load_account_policies(
        config_dir,
        data_dir,
        now_ts=now,
    )[0]

    assert policy.account_uid == "137:0:0x" + "1" * 40
    assert policy.host_id == "vps1"
    assert policy.reward_percentages == {}
    assert policy.scoring_by_token == {}

    scoring_payload["account_uid_key"] = reward_observer._account_uid_key(
        policy.account_uid
    )
    scoring_path.write_text(json.dumps(scoring_payload), encoding="utf-8")
    matching_policy = reward_observer._load_account_policies(
        config_dir,
        data_dir,
        now_ts=now,
    )[0]
    assert matching_policy.scoring_by_token == {"yes-token": True}

    engine_state = json.loads(
        (data_dir / "engine_state_1.json").read_text(encoding="utf-8")
    )
    engine_state["account_uid_key"] = "foreign-account"
    (data_dir / "engine_state_1.json").write_text(
        json.dumps(engine_state),
        encoding="utf-8",
    )
    replaced_policy = reward_observer._load_account_policies(
        config_dir,
        data_dir,
        now_ts=now,
    )[0]
    assert replaced_policy.available_usdc is None
    assert replaced_policy.market_runtime == {}
    assert replaced_policy.scoring_by_token == {"yes-token": True}


def test_account_policy_uses_local_hostname_when_host_is_not_configured(
    tmp_path: Path,
    monkeypatch,
):
    now = 1_800_000_000.0
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    data_dir.mkdir()
    monkeypatch.delenv("POLYMARKET_HOST_ID", raising=False)
    monkeypatch.setattr(reward_observer.socket, "gethostname", lambda: "VM-0-11-Ubuntu")
    account_uid = "137:0:0x" + "1" * 40
    account_uid_key = reward_observer._account_uid_key(account_uid)
    (config_dir / "config_1.json").write_text(
        json.dumps(
            {
                "account": {
                    "funder": "0x" + "1" * 40,
                    "chain_id": 137,
                    "signature_type": 0,
                },
                "markets": [],
                "night_markets": [],
            }
        ),
        encoding="utf-8",
    )
    (data_dir / "engine_state_1.json").write_text(
        json.dumps(
            {
                "generated_at": now,
                "balance": 100,
                "account_uid_key": account_uid_key,
                "runtime": {"host_id": "vm-0-11-ubuntu"},
            }
        ),
        encoding="utf-8",
    )
    scoring_payload = {
        "generated_at": now,
        "account_uid_key": account_uid_key,
        "host_id": "vm-0-11-ubuntu",
        "token_samples": {
            "yes-token": _token_sample(
                "yes-token",
                ["order-1"],
                observed_at=now,
                scoring=True,
            )
        },
        "orders": {
            "order-1": {
                "order_id": "order-1",
                "token_id": "yes-token",
                "live": True,
            }
        },
    }
    scoring_path = data_dir / "order_scoring_state_1.json"
    scoring_path.write_text(json.dumps(scoring_payload), encoding="utf-8")

    policy = reward_observer._load_account_policies(
        config_dir,
        data_dir,
        now_ts=now,
    )[0]

    assert policy.host_id == "vm-0-11-ubuntu"
    assert policy.available_usdc == Decimal("100")
    assert policy.scoring_by_token == {"yes-token": True}

    scoring_payload["host_id"] = "vm-0-3-ubuntu"
    scoring_path.write_text(json.dumps(scoring_payload), encoding="utf-8")
    wrong_host_policy = reward_observer._load_account_policies(
        config_dir,
        data_dir,
        now_ts=now,
    )[0]
    assert wrong_host_policy.scoring_by_token == {}


def test_sports_market_is_classified_without_excluding_generic_markets() -> None:
    sports = _market(slug="nba-lal-bos-2026-07-30")
    sports["gameStartTime"] = "2026-07-30T12:00:00Z"
    result = observe_reward_markets(
        [sports, _market(slug="will-inflation-fall-in-2026")],
        lambda _token: _book(),
    )

    assert {row["market_type"] for row in result["candidates"]} == {
        "sports",
        "always_on",
    }


def test_market_phase_distinguishes_normal_pregame_and_live() -> None:
    sports = _market(slug="nba-lal-bos-2026-07-30")
    sports["gameStartTime"] = "2026-07-30T12:00:00Z"
    start = reward_observer._timestamp(sports["gameStartTime"])

    assert reward_observer._market_phase(_market(), now_ts=start)[0] == "normal"
    assert reward_observer._market_phase(sports, now_ts=start - 60)[0] == "pregame"
    assert reward_observer._market_phase(sports, now_ts=start + 60)[0] == "live"


def test_standalone_refresh_writes_read_only_dashboard_state(
    tmp_path: Path,
) -> None:
    state = refresh_observer_state(
        tmp_path,
        fetch_markets=lambda: [_market(reward="25")],
        fetch_book=lambda _token: _book(),
    )

    saved = json.loads(
        (tmp_path / "reward_observer_state.json").read_text(
            encoding="utf-8"
        )
    )
    assert state["status"] == "ready"
    assert saved["mode"] == "observe_only"
    assert saved["source"] == "public_gamma_and_clob"
    assert saved["candidates"][0]["actual_reward_share_pct"] is None
    assert saved["candidates"][0]["verification_status"] == "collecting"


def test_refresh_writes_review_only_stable_rotation_proposal(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    config_path = config_dir / "config_1.json"
    config_path.write_text(
        json.dumps(
            {
                "account": {"funder": "0x" + "1" * 40},
                "execution": {"min_front_bid_notional_usdc": 1},
                "markets": [],
                "night_markets": [],
            }
        ),
        encoding="utf-8",
    )
    config_before = config_path.read_bytes()

    state = refresh_observer_state(
        data_dir,
        config_dir=config_dir,
        fetch_markets=lambda: [_market(reward="25")],
        fetch_book=lambda _token: _book(),
    )

    proposal_path = data_dir / "stable_rotation_proposal.json"
    proposal = json.loads(proposal_path.read_text(encoding="utf-8"))
    assert state["stable_rotation_proposal"]["output"] == proposal_path.name
    assert proposal["mode"] == "proposal_only"
    assert proposal["safety"]["runtime_config_writes"] is False
    assert proposal["safety"]["runtime_commands"] is False
    assert proposal["safety"]["trading_actions"] is False
    assert config_path.read_bytes() == config_before


def test_rotation_planner_failure_does_not_break_reward_observer(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    (config_dir / "config_1.json").write_text("not-json", encoding="utf-8")

    state = refresh_observer_state(
        data_dir,
        config_dir=config_dir,
        fetch_markets=lambda: [_market(reward="25")],
        fetch_book=lambda _token: _book(),
    )

    proposal = json.loads(
        (data_dir / "stable_rotation_proposal.json").read_text(encoding="utf-8")
    )
    assert state["status"] == "ready"
    assert state["stable_rotation_proposal"]["status"] == "blocked"
    assert proposal["status"] == "blocked"
    assert proposal["reason"] == "planner_input_error"
    assert proposal["safety"]["trading_actions"] is False
    assert "not-json" not in json.dumps(proposal)


def test_history_deduplicates_same_timestamp(tmp_path: Path) -> None:
    state = observe_reward_markets([_market()], lambda _token: _book())

    reward_observer._apply_observation_history(
        tmp_path,
        state,
        1_800_000_000,
    )
    reward_observer._apply_observation_history(
        tmp_path,
        state,
        1_800_000_000,
    )

    history = json.loads(
        (tmp_path / "reward_observer_history.json").read_text(encoding="utf-8")
    )
    assert len(history["markets"]["condition:condition-1"]["samples"]) == 1


def test_history_retains_full_seven_days_of_five_minute_samples(
    tmp_path: Path,
) -> None:
    now_ts = 1_800_000_000.0
    samples = [
        {"ts": now_ts - index * 300, "share": index % 10, "risk": 10}
        for index in range(2_017)
    ]
    (tmp_path / "reward_observer_history.json").write_text(
        json.dumps(
            {
                "markets": {
                    "condition:legacy": {
                        "question": "Legacy?",
                        "slug": "legacy",
                        "samples": samples,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    reward_observer._apply_observation_history(
        tmp_path,
        {"candidates": []},
        now_ts,
    )

    history = json.loads(
        (tmp_path / "reward_observer_history.json").read_text(encoding="utf-8")
    )
    retained = history["markets"]["condition:legacy"]["samples"]
    assert len(retained) == 2_016
    assert retained[-1]["ts"] - retained[0]["ts"] == 2_015 * 300


def test_configured_market_records_missing_book_reason(tmp_path: Path) -> None:
    state = {
        "candidates": [],
        "configured_market_refs": [
            {
                "account_index": 1,
                "condition_id": "condition-1",
                "token_id": "yes-token",
                "paired_token_id": "no-token",
                "question": "Market one?",
                "slug": "market-one",
            }
        ],
        "rewarded_market_keys": ["condition:condition-1"],
    }

    reward_observer._apply_observation_history(
        tmp_path,
        state,
        1_800_000_000,
    )

    history = json.loads(
        (tmp_path / "reward_observer_history.json").read_text(encoding="utf-8")
    )
    sample = history["markets"]["condition:condition-1"]["samples"][0]
    assert sample["missing_reason"] == "order_book_unavailable_or_invalid"
    assert sample["configured_account_indexes"] == [1]


def test_finalized_market_earnings_calibrate_same_day_prediction(
    tmp_path: Path,
) -> None:
    account_uid = "137:1:0x" + "a" * 40
    policy = replace(_account_policy(), account_uid=account_uid)
    state = observe_reward_markets(
        [_market()],
        lambda _token: _book(size="500"),
        account_policies=[policy],
    )
    account_key = state["candidates"][0]["account_execution"][0][
        "account_uid_key"
    ]
    forecast_day = "2027-01-15"
    now_ts = datetime(2027, 1, 16, 12, tzinfo=timezone.utc).timestamp()
    (tmp_path / "reward_observer_history.json").write_text(
        json.dumps(
            {
                "markets": {
                    "condition:condition-1": {
                        "samples": [
                            {
                                "ts": now_ts - 86_400,
                                "forecast_business_day": forecast_day,
                                "share": 10,
                                "risk": 20,
                                "account_execution": [
                                    {
                                        "account_uid_key": account_key,
                                        "predicted_daily_gross_usd": 4,
                                    }
                                ],
                            }
                        ]
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "reward_ledger.json").write_text(
        json.dumps(
            {
                "records": {
                    "native": {
                        "account_uid": account_uid,
                        "business_day": forecast_day,
                        "condition_id": "condition-1",
                        "reward_type": "native_lp",
                        "usd_amount": 1.5,
                        "fresh": True,
                        "finalized": True,
                    },
                    "sponsored": {
                        "account_uid": account_uid,
                        "business_day": forecast_day,
                        "condition_id": "condition-1",
                        "reward_type": "sponsored_lp",
                        "usd_amount": 0.5,
                        "fresh": True,
                        "finalized": True,
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    reward_observer._apply_observation_history(tmp_path, state, now_ts)

    candidate = state["candidates"][0]
    assert candidate["earnings_calibration_scopes"] == 1
    assert candidate["earnings_prediction_mae_usd"] == 2.0
    assert candidate["earnings_prediction_bias_usd"] == 2.0
    assert candidate["earnings_calibration_ratio"] == 0.5
    history = json.loads(
        (tmp_path / "reward_observer_history.json").read_text(encoding="utf-8")
    )
    execution = history["markets"]["condition:condition-1"]["samples"][0][
        "account_execution"
    ][0]
    assert execution["official_finalized_lp_earnings_usd"] == 2.0
    assert execution["official_finalized_lp_earnings_by_type"] == {
        "native_lp": 1.5,
        "sponsored_lp": 0.5,
    }


def test_candidate_requires_repeated_stable_samples_before_verification(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = [1_800_000_000.0]
    monkeypatch.setattr(reward_observer.time, "time", lambda: now[0])

    state = {}
    for _ in range(12):
        state = refresh_observer_state(
            tmp_path,
            fetch_markets=lambda: [_market(reward="25")],
            fetch_book=lambda _token: _book(),
        )
        now[0] += 300

    candidate = state["candidates"][0]
    assert candidate["observation_samples"] == 12
    assert candidate["observation_span_sec"] == 3300
    assert candidate["estimated_share_range_pp"] == 0
    assert candidate["stability_score"] == 100
    assert candidate["verification_status"] == "stable"
    assert candidate["verification_recommended"] is True
    assert candidate["stable_lp_recommended"] is False
    assert "front_depth_below_stable_minimum" in candidate[
        "stable_lp_rejection_reasons"
    ]
    assert candidate["risk_adjusted_daily_roi_pct"] > 0


def test_weather_market_remains_observe_only_for_stable_lp(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = [1_800_000_000.0]
    monkeypatch.setattr(reward_observer.time, "time", lambda: now[0])
    weather = _market(slug="highest-temperature-in-guangzhou-on-august-26")
    weather["question"] = "What will be the highest temperature in Guangzhou?"

    state = {}
    for _ in range(12):
        state = refresh_observer_state(
            tmp_path,
            fetch_markets=lambda: [weather],
            fetch_book=lambda _token: _deep_book(),
        )
        now[0] += 300

    candidate = state["candidates"][0]
    assert candidate["verification_recommended"] is False
    assert candidate["weather_market"] is True
    assert candidate["market_type"] == "weather"
    assert candidate["stable_lp_recommended"] is False
    assert "weather_observe_only" in candidate["stable_lp_rejection_reasons"]
    assert candidate["admission_level"] == "reject"


@pytest.mark.parametrize(
    "question",
    [
        "Will it rain in London tomorrow?",
        "Will New York reach 85 degrees Fahrenheit?",
        "Will the temperature in Paris be 29 C?",
        "Will Paris's temperature reach 29 C?",
        "Will Hong Kong record 29°C on Friday?",
    ],
)
def test_weather_classifier_covers_common_question_formats(question: str) -> None:
    market = _market(slug="daily-city-contract")
    market["question"] = question

    assert reward_observer._is_weather_market(market) is True


def test_stable_lp_requires_meaningful_front_depth(
    tmp_path: Path,
    monkeypatch,
) -> None:
    now = [1_800_000_000.0]
    monkeypatch.setattr(reward_observer.time, "time", lambda: now[0])

    state = {}
    for _ in range(12):
        state = refresh_observer_state(
            tmp_path,
            fetch_markets=lambda: [_market(reward="25")],
            fetch_book=lambda _token: _deep_book("100"),
        )
        now[0] += 300

    candidate = state["candidates"][0]
    assert candidate["front_depth_status"] == "verified"
    assert candidate["verification_recommended"] is True
    assert candidate["stable_lp_recommended"] is False
    assert "front_depth_below_stable_minimum" in candidate[
        "stable_lp_rejection_reasons"
    ]
