"""Compare ladder shapes by reward Q on real books, read-only.

The engine spreads BUY size over up to three consecutive ticks below the
safe top with geometric decay weights. Polymarket's distance score is
quadratic in closeness to the midpoint, so concentrating the same shares
nearer the top generally raises Q — at the cost of concentrated fill risk.
This tool quantifies that trade-off per market so the ladder debate can be
settled with numbers instead of taste.

Pure computation plus an optional CLI that refetches books for the top
markets of a ``competition_scan`` report. No credentials, no orders.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from platforms.polymarket.maker.quote_feasibility import (
    aggregate_bid_q,
    executable_quote_boundary,
    normalize_reward_spread,
)

try:
    from .competition_scan import book_levels, book_top
    from .public_data import fetch_books, new_session
except ImportError:  # pragma: no cover - direct script execution
    from competition_scan import book_levels, book_top
    from public_data import fetch_books, new_session


ZERO = Decimal("0")
SHARE_QUANTUM = Decimal("0.001")

# name -> (n_legs, decay). "engine_3" mirrors the live planner's regular
# market shape: up to three consecutive ticks, level_weight_decay 0.82.
DEFAULT_SHAPES: tuple[tuple[str, int, Decimal], ...] = (
    ("concentrated_1", 1, Decimal("1")),
    ("split_2", 2, Decimal("0.82")),
    ("engine_3", 3, Decimal("0.82")),
    ("deep_6", 6, Decimal("0.82")),
)


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


def decay_weights(n_legs: int, decay: Decimal) -> list[Decimal]:
    """Normalized geometric weights, mirroring the engine's allocator."""
    if n_legs <= 0:
        return []
    weights: list[Decimal] = []
    current = Decimal("1")
    for _ in range(n_legs):
        weights.append(current)
        current *= decay
    total = sum(weights)
    return [weight / total for weight in weights]


def build_ladder(
    *,
    safe_top: Decimal,
    tick: Decimal,
    reward_lower: Decimal,
    n_legs: int,
    decay: Decimal,
    total_shares: Decimal,
) -> list[tuple[Decimal, Decimal]]:
    """Place total_shares over consecutive ticks below safe_top."""
    if safe_top <= ZERO or tick <= ZERO or total_shares <= ZERO:
        return []
    prices: list[Decimal] = []
    for index in range(max(1, n_legs)):
        price = safe_top - tick * Decimal(index)
        if price < reward_lower or price < tick:
            break
        prices.append(price)
    if not prices:
        return []
    weights = decay_weights(len(prices), decay)
    ladder: list[tuple[Decimal, Decimal]] = []
    for price, weight in zip(prices, weights):
        size = (total_shares * weight).quantize(SHARE_QUANTUM, rounding=ROUND_DOWN)
        if size > ZERO:
            ladder.append((price, size))
    return ladder


@dataclass(frozen=True)
class ShapeResult:
    name: str
    legs: int
    q_min: Decimal
    share: Decimal
    q_vs_baseline: Decimal


def evaluate_shapes(
    *,
    yes_book: Mapping[str, Any],
    no_book: Mapping[str, Any],
    tick: Decimal,
    max_spread: Decimal,
    total_shares: Decimal,
    min_distance_ticks: int = 1,
    shapes: Sequence[tuple[str, int, Decimal]] = DEFAULT_SHAPES,
    baseline_name: str = "engine_3",
) -> list[ShapeResult]:
    """Return pair Q_min and competitor share for each ladder shape."""
    yes_bids = book_levels(yes_book, "bids")
    no_bids = book_levels(no_book, "bids")
    yes_best_bid, yes_best_ask = book_top(yes_book)
    no_best_bid, no_best_ask = book_top(no_book)
    if min(yes_best_bid, yes_best_ask, no_best_bid, no_best_ask) <= ZERO:
        return []
    yes_mid = (yes_best_bid + yes_best_ask) / Decimal("2")
    no_mid = (no_best_bid + no_best_ask) / Decimal("2")
    spread = normalize_reward_spread(max_spread)
    competition_q = min(
        aggregate_bid_q(yes_bids, midpoint=yes_mid, max_spread=spread),
        aggregate_bid_q(no_bids, midpoint=no_mid, max_spread=spread),
    )

    boundaries = {}
    for side, best_bid, midpoint in (
        ("yes", yes_best_bid, yes_mid),
        ("no", no_best_bid, no_mid),
    ):
        boundaries[side] = executable_quote_boundary(
            best_bid=best_bid,
            midpoint=midpoint,
            tick=tick,
            max_spread=max_spread,
            min_distance_ticks=min_distance_ticks,
        )
    if not boundaries["yes"].executable or not boundaries["no"].executable:
        return []

    raw: list[tuple[str, int, Decimal]] = []
    for name, n_legs, decay in shapes:
        side_q: dict[str, Decimal] = {}
        placed_legs = 0
        for side, midpoint in (("yes", yes_mid), ("no", no_mid)):
            boundary = boundaries[side]
            ladder = build_ladder(
                safe_top=boundary.quote,
                tick=tick,
                reward_lower=boundary.reward_lower,
                n_legs=n_legs,
                decay=decay,
                total_shares=total_shares,
            )
            placed_legs = max(placed_legs, len(ladder))
            side_q[side] = aggregate_bid_q(
                ladder, midpoint=midpoint, max_spread=spread
            )
        raw.append((name, placed_legs, min(side_q["yes"], side_q["no"])))

    baseline_q = next((q for name, _, q in raw if name == baseline_name), ZERO)
    results: list[ShapeResult] = []
    for name, legs, q_min in raw:
        share = (
            q_min / (q_min + competition_q)
            if q_min > ZERO
            else ZERO
        )
        results.append(
            ShapeResult(
                name=name,
                legs=legs,
                q_min=q_min,
                share=share,
                q_vs_baseline=(q_min / baseline_q) if baseline_q > ZERO else ZERO,
            )
        )
    return results


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-scan", type=Path, required=True,
                        help="competition_scan.json report to pick markets from")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--shares", type=str, default="1000")
    parser.add_argument("--out", type=Path, default=Path("ladder_compare.json"))
    args = parser.parse_args(argv)

    report = json.loads(args.from_scan.read_text(encoding="utf-8"))
    markets = [
        row
        for row in report.get("markets") or []
        if not row.get("blocked_reason")
    ][: max(1, args.top)]
    session = new_session()
    token_ids = [
        token
        for row in markets
        for token in (str(row.get("yes_token")), str(row.get("no_token")))
    ]
    books = fetch_books(token_ids, session)

    total_shares = _decimal(args.shares)
    output: list[dict[str, Any]] = []
    for row in markets:
        yes_book = books.get(str(row.get("yes_token")))
        no_book = books.get(str(row.get("no_token")))
        if yes_book is None or no_book is None:
            continue
        results = evaluate_shapes(
            yes_book=yes_book,
            no_book=no_book,
            tick=_decimal(row.get("tick"), Decimal("0.01")),
            max_spread=_decimal(row.get("rewards_max_spread")),
            total_shares=total_shares,
        )
        if not results:
            continue
        output.append(
            {
                "slug": row.get("slug"),
                "condition_id": row.get("condition_id"),
                "daily_rate_usd": row.get("daily_rate_usd"),
                "shapes": [
                    {
                        "name": result.name,
                        "legs": result.legs,
                        "q_min": str(result.q_min),
                        "share": str(result.share),
                        "q_vs_engine_3": str(result.q_vs_baseline),
                    }
                    for result in results
                ],
            }
        )
        line = " ".join(
            f"{result.name}=x{result.q_vs_baseline:.3f}" for result in results
        )
        print(f"{(row.get('slug') or '?')[:48]:<48} {line}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps({"shares": str(total_shares), "markets": output},
                   ensure_ascii=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
