import json
from decimal import Decimal
from pathlib import Path

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
    assert candidate["risk_adjusted_daily_roi_pct"] > 0
