from decimal import Decimal

from platforms.polymarket.maker.quote_feasibility import (
    compute_quote_target_shares,
    distance_score,
    evaluate_paired_quote,
    executable_quote_boundary,
)


def test_quote_boundary_matches_engine_safe_top_and_reward_zone() -> None:
    boundary = executable_quote_boundary(
        best_bid=Decimal("0.48"),
        midpoint=Decimal("0.50"),
        tick=Decimal("0.01"),
        max_spread=Decimal("0.10"),
        min_distance_ticks=1,
    )

    assert boundary.executable is True
    assert boundary.quote == Decimal("0.47")
    assert boundary.reward_lower == Decimal("0.40")


def test_quote_boundary_blocks_when_safe_price_leaves_reward_zone() -> None:
    boundary = executable_quote_boundary(
        best_bid=Decimal("0.48"),
        midpoint=Decimal("0.50"),
        tick=Decimal("0.01"),
        max_spread=Decimal("0.02"),
        min_distance_ticks=1,
    )

    assert boundary.executable is False
    assert boundary.blocked_reason == "no_executable_quote_in_reward_zone"


def test_target_shares_never_exceeds_available_cap_to_meet_reward_minimum() -> None:
    target, warning = compute_quote_target_shares(
        available=Decimal("100"),
        rewards_min=Decimal("200"),
        min_order_size=Decimal("5"),
        budget_pct=Decimal("1"),
        size_cap=Decimal("1"),
        max_quote_shares=Decimal("0"),
    )

    assert target == 0
    assert warning.startswith("budget_below_min|")


def test_paired_quote_reports_theoretical_and_executable_q_separately() -> None:
    bids = [
        (Decimal("0.48"), Decimal("100")),
        (Decimal("0.47"), Decimal("100")),
    ]
    execution = evaluate_paired_quote(
        yes_bids=bids,
        no_bids=bids,
        yes_best_bid=Decimal("0.48"),
        no_best_bid=Decimal("0.48"),
        yes_midpoint=Decimal("0.50"),
        no_midpoint=Decimal("0.50"),
        yes_tick=Decimal("0.01"),
        no_tick=Decimal("0.01"),
        max_spread=Decimal("0.10"),
        min_distance_ticks=1,
        available=Decimal("100"),
        rewards_min=Decimal("10"),
        min_order_size=Decimal("5"),
    )

    assert execution.executable is True
    assert execution.yes_quote == Decimal("0.47")
    assert execution.theoretical_q_min == Decimal("64.00")
    assert execution.executable_q_min == Decimal("49.00")
    assert execution.theoretical_share > execution.executable_share


def test_paired_quote_rejects_different_leg_ticks() -> None:
    bids = [(Decimal("0.48"), Decimal("100"))]
    execution = evaluate_paired_quote(
        yes_bids=bids,
        no_bids=bids,
        yes_best_bid=Decimal("0.48"),
        no_best_bid=Decimal("0.48"),
        yes_midpoint=Decimal("0.50"),
        no_midpoint=Decimal("0.50"),
        yes_tick=Decimal("0.01"),
        no_tick=Decimal("0.001"),
        max_spread=Decimal("0.10"),
        min_distance_ticks=1,
        available=Decimal("100"),
        rewards_min=Decimal("10"),
        min_order_size=Decimal("5"),
    )

    assert execution.executable is False
    assert "tick_size_mismatch" in execution.blocked_reasons


def test_paired_quote_caps_single_boundary_quote_by_per_order_notional() -> None:
    bids = [(Decimal("0.48"), Decimal("1000"))]
    execution = evaluate_paired_quote(
        yes_bids=bids,
        no_bids=bids,
        yes_best_bid=Decimal("0.48"),
        no_best_bid=Decimal("0.48"),
        yes_midpoint=Decimal("0.50"),
        no_midpoint=Decimal("0.50"),
        yes_tick=Decimal("0.01"),
        no_tick=Decimal("0.01"),
        max_spread=Decimal("0.10"),
        min_distance_ticks=1,
        available=Decimal("1000"),
        rewards_min=Decimal("10"),
        min_order_size=Decimal("5"),
        max_notional_per_order=Decimal("100"),
    )

    assert execution.executable is True
    assert execution.target_shares == Decimal("212.765")
    assert execution.collateral_required == Decimal("212.765")
    assert execution.executable_q_min == Decimal("104.25485")


def test_paired_quote_blocks_when_per_order_cap_cannot_meet_reward_minimum() -> None:
    bids = [(Decimal("0.48"), Decimal("1000"))]
    execution = evaluate_paired_quote(
        yes_bids=bids,
        no_bids=bids,
        yes_best_bid=Decimal("0.48"),
        no_best_bid=Decimal("0.48"),
        yes_midpoint=Decimal("0.50"),
        no_midpoint=Decimal("0.50"),
        yes_tick=Decimal("0.01"),
        no_tick=Decimal("0.01"),
        max_spread=Decimal("0.10"),
        min_distance_ticks=1,
        available=Decimal("1000"),
        rewards_min=Decimal("10"),
        min_order_size=Decimal("5"),
        max_notional_per_order=Decimal("4"),
    )

    assert execution.executable is False
    assert execution.target_shares == Decimal("8.510")
    assert "yes:per_order_cap_below_reward_minimum" in execution.blocked_reasons
    assert "no:per_order_cap_below_reward_minimum" in execution.blocked_reasons


def test_distance_score_is_zero_at_reward_boundary() -> None:
    assert distance_score(
        Decimal("0.40"),
        Decimal("0.50"),
        Decimal("0.10"),
    ) == 0
