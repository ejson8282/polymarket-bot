"""Pure Polymarket LP quote feasibility calculations.

The engine and read-only reward observer share these functions so an observer
estimate cannot silently quote at a price the live engine would refuse.  This
module has no network, signer, configuration-write, or order side effects.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Iterable, Sequence


ZERO = Decimal("0")
ONE = Decimal("1")


def normalize_reward_spread(value: Decimal) -> Decimal:
    """Normalize a reward spread expressed either in dollars or cents."""

    spread = Decimal(str(value))
    if spread > ONE:
        spread /= Decimal("100")
    return max(ZERO, spread)


def floor_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    tick = Decimal(str(tick))
    if tick <= ZERO:
        raise ValueError("tick must be positive")
    return (Decimal(str(value)) / tick).to_integral_value(rounding=ROUND_DOWN) * tick


def distance_score(
    price: Decimal,
    midpoint: Decimal,
    max_spread: Decimal,
) -> Decimal:
    """Return Polymarket's per-share distance score in the 0..1 interval."""

    spread = normalize_reward_spread(max_spread)
    if spread <= ZERO:
        return ZERO
    distance = abs(Decimal(str(midpoint)) - Decimal(str(price)))
    if distance >= spread:
        return ZERO
    ratio = (spread - distance) / spread
    return ratio * ratio


@dataclass(frozen=True)
class QuoteBoundary:
    quote: Decimal
    reward_lower: Decimal
    safe_top: Decimal
    blocked_reason: str = ""

    @property
    def executable(self) -> bool:
        return self.quote > ZERO and not self.blocked_reason


def executable_quote_boundary(
    *,
    best_bid: Decimal,
    midpoint: Decimal,
    tick: Decimal,
    max_spread: Decimal,
    min_distance_ticks: int,
) -> QuoteBoundary:
    """Return the same top reward-zone quote boundary used by the engine."""

    best_bid = Decimal(str(best_bid))
    midpoint = Decimal(str(midpoint))
    tick = Decimal(str(tick))
    spread = normalize_reward_spread(max_spread)
    if tick <= ZERO:
        return QuoteBoundary(ZERO, ZERO, ZERO, "missing_tick_size")
    if best_bid <= ZERO or midpoint <= ZERO:
        return QuoteBoundary(ZERO, ZERO, ZERO, "invalid_book")
    if spread <= ZERO:
        return QuoteBoundary(ZERO, ZERO, ZERO, "missing_reward_spread")

    reward_lower = max(tick, midpoint - spread)
    distance_ticks = max(1, int(min_distance_ticks))
    safe_top = best_bid - tick * Decimal(distance_ticks)
    if safe_top < reward_lower or safe_top < tick:
        return QuoteBoundary(
            ZERO,
            reward_lower,
            safe_top,
            "no_executable_quote_in_reward_zone",
        )
    quote = floor_to_tick(safe_top, tick)
    if quote < reward_lower or quote < tick:
        return QuoteBoundary(
            ZERO,
            reward_lower,
            safe_top,
            "tick_rounding_outside_reward_zone",
        )
    return QuoteBoundary(quote, reward_lower, safe_top)


def compute_quote_target_shares(
    *,
    available: Decimal,
    rewards_min: Decimal,
    min_order_size: Decimal,
    budget_pct: Decimal,
    size_cap: Decimal,
    max_quote_shares: Decimal,
) -> tuple[Decimal, str]:
    """Apply the engine's configured budget and risk caps to target shares."""

    available = max(ZERO, Decimal(str(available)))
    rewards_min = max(ZERO, Decimal(str(rewards_min)))
    min_order_size = max(ZERO, Decimal(str(min_order_size)))
    pct = max(ZERO, min(Decimal(str(budget_pct)), ONE))
    cap = max(ZERO, min(Decimal(str(size_cap)), ONE))
    max_quote_shares = max(ZERO, Decimal(str(max_quote_shares)))
    budget = available * pct
    if max_quote_shares > ZERO:
        budget = min(budget, max_quote_shares)
    budget *= cap
    target = budget.to_integral_value(rounding=ROUND_DOWN)
    required = max(rewards_min, min_order_size)
    if target < required:
        return ZERO, (
            f"budget_below_min|available={available}|budget={budget}|"
            f"required={required}|pct={pct}|size_cap={cap}"
        )
    return target, ""


def aggregate_bid_q(
    levels: Iterable[tuple[Decimal, Decimal]],
    *,
    midpoint: Decimal,
    max_spread: Decimal,
) -> Decimal:
    return sum(
        (
            distance_score(price, midpoint, max_spread) * size
            for price, size in levels
            if size > ZERO
        ),
        ZERO,
    )


def front_bid_notional(
    levels: Iterable[tuple[Decimal, Decimal]],
    quote: Decimal,
) -> tuple[Decimal, int]:
    front = [
        (price, size)
        for price, size in levels
        if price >= quote and size > ZERO
    ]
    return sum((price * size for price, size in front), ZERO), len(front)


@dataclass(frozen=True)
class PairExecution:
    theoretical_share: Decimal
    executable_share: Decimal
    theoretical_q_min: Decimal
    executable_q_min: Decimal
    competition_q: Decimal
    target_shares: Decimal
    collateral_required: Decimal
    yes_quote: Decimal
    no_quote: Decimal
    yes_front_notional: Decimal
    no_front_notional: Decimal
    yes_front_levels: int
    no_front_levels: int
    blocked_reasons: tuple[str, ...]

    @property
    def executable(self) -> bool:
        return self.executable_q_min > ZERO and not self.blocked_reasons


def _share(own_q: Decimal, competition_q: Decimal) -> Decimal:
    if own_q <= ZERO:
        return ZERO
    return max(ZERO, min(ONE, own_q / (max(ZERO, competition_q) + own_q)))


def evaluate_paired_quote(
    *,
    yes_bids: Sequence[tuple[Decimal, Decimal]],
    no_bids: Sequence[tuple[Decimal, Decimal]],
    yes_best_bid: Decimal,
    no_best_bid: Decimal,
    yes_midpoint: Decimal,
    no_midpoint: Decimal,
    yes_tick: Decimal,
    no_tick: Decimal,
    max_spread: Decimal,
    min_distance_ticks: int,
    available: Decimal,
    rewards_min: Decimal,
    min_order_size: Decimal,
    budget_pct: Decimal = ONE,
    size_cap: Decimal = ONE,
    max_quote_shares: Decimal = ZERO,
    max_notional_per_order: Decimal = ZERO,
) -> PairExecution:
    """Compare a best-bid theory with a price and capital executable quote."""

    reasons: list[str] = []
    yes_tick = Decimal(str(yes_tick))
    no_tick = Decimal(str(no_tick))
    if yes_tick > ZERO and no_tick > ZERO and yes_tick != no_tick:
        reasons.append("tick_size_mismatch")
    target, target_warning = compute_quote_target_shares(
        available=available,
        rewards_min=rewards_min,
        min_order_size=min_order_size,
        budget_pct=budget_pct,
        size_cap=size_cap,
        max_quote_shares=max_quote_shares,
    )
    if target <= ZERO:
        reasons.append(target_warning or "quote_target_zero")

    yes_boundary = executable_quote_boundary(
        best_bid=yes_best_bid,
        midpoint=yes_midpoint,
        tick=yes_tick,
        max_spread=max_spread,
        min_distance_ticks=min_distance_ticks,
    )
    no_boundary = executable_quote_boundary(
        best_bid=no_best_bid,
        midpoint=no_midpoint,
        tick=no_tick,
        max_spread=max_spread,
        min_distance_ticks=min_distance_ticks,
    )
    if yes_boundary.blocked_reason:
        reasons.append(f"yes:{yes_boundary.blocked_reason}")
    if no_boundary.blocked_reason:
        reasons.append(f"no:{no_boundary.blocked_reason}")

    required = max(Decimal(str(rewards_min)), Decimal(str(min_order_size)))
    max_notional_per_order = max(ZERO, Decimal(str(max_notional_per_order)))
    if max_notional_per_order > ZERO:
        per_order_share_caps: list[Decimal] = []
        for side, quote in (("yes", yes_boundary.quote), ("no", no_boundary.quote)):
            if quote <= ZERO:
                continue
            share_cap = floor_to_tick(
                max_notional_per_order / quote,
                Decimal("0.001"),
            )
            per_order_share_caps.append(share_cap)
            if share_cap < required:
                reasons.append(f"{side}:per_order_cap_below_reward_minimum")
        if per_order_share_caps:
            target = min(target, min(per_order_share_caps))

    yes_theoretical_score = distance_score(
        yes_best_bid,
        yes_midpoint,
        max_spread,
    )
    no_theoretical_score = distance_score(
        no_best_bid,
        no_midpoint,
        max_spread,
    )
    theoretical_q = min(
        yes_theoretical_score * target,
        no_theoretical_score * target,
    )
    yes_executable_score = distance_score(
        yes_boundary.quote,
        yes_midpoint,
        max_spread,
    ) if yes_boundary.executable else ZERO
    no_executable_score = distance_score(
        no_boundary.quote,
        no_midpoint,
        max_spread,
    ) if no_boundary.executable else ZERO
    executable_q = min(
        yes_executable_score * target,
        no_executable_score * target,
    )
    if target > ZERO and executable_q <= ZERO:
        reasons.append("executable_q_min_zero")

    competition_q = min(
        aggregate_bid_q(
            yes_bids,
            midpoint=yes_midpoint,
            max_spread=max_spread,
        ),
        aggregate_bid_q(
            no_bids,
            midpoint=no_midpoint,
            max_spread=max_spread,
        ),
    )
    yes_front, yes_levels = front_bid_notional(yes_bids, yes_boundary.quote)
    no_front, no_levels = front_bid_notional(no_bids, no_boundary.quote)
    return PairExecution(
        theoretical_share=_share(theoretical_q, competition_q),
        executable_share=_share(executable_q, competition_q),
        theoretical_q_min=theoretical_q,
        executable_q_min=executable_q,
        competition_q=competition_q,
        target_shares=target,
        collateral_required=target,
        yes_quote=yes_boundary.quote,
        no_quote=no_boundary.quote,
        yes_front_notional=yes_front,
        no_front_notional=no_front,
        yes_front_levels=yes_levels,
        no_front_levels=no_levels,
        blocked_reasons=tuple(dict.fromkeys(reason for reason in reasons if reason)),
    )
