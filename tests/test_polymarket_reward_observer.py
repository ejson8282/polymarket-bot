import json
from decimal import Decimal
from pathlib import Path

import pytest

from platforms.polymarket.maker import reward_observer
from platforms.polymarket.maker.reward_observer import (
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


def test_minimum_share_size_can_raise_probe_capital_above_budget() -> None:
    candidate = observe_reward_markets(
        [_market(min_size="200")],
        lambda _token: _book(),
        probe_budget_usdc=Decimal("100"),
    )["candidates"][0]

    assert candidate["probe_capital_usd"] > 100
    assert candidate["probe_shares_each_side"] == 200
    assert "minimum_size_raises_capital" in candidate["reasons"]


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
    assert candidate["verification_recommended"] is True
    assert candidate["weather_market"] is True
    assert candidate["market_type"] == "weather"
    assert candidate["stable_lp_recommended"] is False
    assert "weather_observe_only" in candidate["stable_lp_rejection_reasons"]


@pytest.mark.parametrize(
    "question",
    [
        "Will it rain in London tomorrow?",
        "Will New York reach 85 degrees Fahrenheit?",
        "Will the temperature in Paris be 29 C?",
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
