from decimal import Decimal

from platforms.polymarket.research.ladder_compare import (
    build_ladder,
    decay_weights,
    evaluate_shapes,
)


def test_decay_weights_match_engine_allocator() -> None:
    weights = decay_weights(3, Decimal("0.82"))

    assert len(weights) == 3
    assert sum(weights) == Decimal("1")
    assert weights[0] > weights[1] > weights[2]
    # Same geometric ratio as the engine's _alloc_weights.
    ratio = (weights[1] / weights[0]).quantize(Decimal("0.0001"))
    assert ratio == Decimal("0.8200")


def test_build_ladder_respects_reward_lower_and_total() -> None:
    ladder = build_ladder(
        safe_top=Decimal("0.47"),
        tick=Decimal("0.01"),
        reward_lower=Decimal("0.46"),
        n_legs=5,
        decay=Decimal("0.82"),
        total_shares=Decimal("1000"),
    )

    # Only 0.47 and 0.46 stay inside the reward zone.
    assert [price for price, _ in ladder] == [Decimal("0.47"), Decimal("0.46")]
    assert sum(size for _, size in ladder) <= Decimal("1000")
    assert all(size > 0 for _, size in ladder)


def _book(best_bid: str, best_ask: str, depth: str = "800") -> dict:
    return {
        "bids": [
            {"price": best_bid, "size": depth},
            {"price": str(Decimal(best_bid) - Decimal("0.01")), "size": depth},
        ],
        "asks": [
            {"price": best_ask, "size": depth},
            {"price": str(Decimal(best_ask) + Decimal("0.01")), "size": depth},
        ],
    }


def test_concentrated_shape_beats_deeper_ladders_on_quadratic_scoring() -> None:
    results = evaluate_shapes(
        yes_book=_book("0.48", "0.52"),
        no_book=_book("0.48", "0.52"),
        tick=Decimal("0.01"),
        max_spread=Decimal("3.5"),
        total_shares=Decimal("1000"),
    )

    assert results, "symmetric mid-price books must be evaluable"
    by_name = {result.name: result for result in results}
    assert by_name["concentrated_1"].q_min >= by_name["engine_3"].q_min
    assert by_name["engine_3"].q_min >= by_name["deep_6"].q_min
    assert by_name["engine_3"].q_vs_baseline == Decimal("1")
    assert Decimal("0") < by_name["concentrated_1"].share < Decimal("1")


def test_evaluate_shapes_returns_empty_on_unusable_book() -> None:
    assert (
        evaluate_shapes(
            yes_book={"bids": [], "asks": []},
            no_book=_book("0.48", "0.52"),
            tick=Decimal("0.01"),
            max_spread=Decimal("3.5"),
            total_shares=Decimal("1000"),
        )
        == []
    )
