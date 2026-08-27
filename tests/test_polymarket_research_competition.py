from decimal import Decimal

from platforms.polymarket.research.competition_scan import (
    MarketInfo,
    book_levels,
    book_top,
    evaluate_market,
    parse_sampling_market,
)


def _sampling_row() -> dict:
    return {
        "condition_id": "0xabc",
        "question": "Will it settle YES?",
        "market_slug": "will-it-settle-yes",
        "minimum_tick_size": 0.01,
        "tokens": [
            {"token_id": "111", "outcome": "Yes"},
            {"token_id": "222", "outcome": "No"},
        ],
        "rewards": {
            "min_size": 50,
            "max_spread": 3.5,
            "rates": [
                {"asset_address": "0xusdc", "rewards_daily_rate": 30},
                {"asset_address": "0xother", "rewards_daily_rate": 10},
            ],
        },
    }


def _book(best_bid: str, best_ask: str, depth: str = "500") -> dict:
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


def test_parse_sampling_market_extracts_pair_and_rates() -> None:
    info = parse_sampling_market(_sampling_row())

    assert info is not None
    assert info.yes_token == "111"
    assert info.no_token == "222"
    assert info.daily_rate_usd == Decimal("40")
    assert info.max_spread == Decimal("3.5")
    assert info.rewards_min_size == Decimal("50")
    assert info.tick == Decimal("0.01")


def test_parse_sampling_market_rejects_missing_rewards() -> None:
    row = _sampling_row()
    row.pop("rewards")
    assert parse_sampling_market(row) is None

    row = _sampling_row()
    row["rewards"]["rates"] = []
    assert parse_sampling_market(row) is None


def test_book_levels_sorted_and_top_extracted() -> None:
    book = {
        "bids": [
            {"price": "0.45", "size": "10"},
            {"price": "0.48", "size": "20"},
        ],
        "asks": [
            {"price": "0.55", "size": "10"},
            {"price": "0.52", "size": "20"},
        ],
    }

    bids = book_levels(book, "bids")
    asks = book_levels(book, "asks")
    best_bid, best_ask = book_top(book)

    assert bids[0][0] == Decimal("0.48")
    assert asks[0][0] == Decimal("0.52")
    assert best_bid == Decimal("0.48")
    assert best_ask == Decimal("0.52")


def _info() -> MarketInfo:
    info = parse_sampling_market(_sampling_row())
    assert info is not None
    return info


def test_evaluate_market_reports_competition_and_diminishing_marginal_roi() -> None:
    grid = (Decimal("100"), Decimal("1000"), Decimal("5000"))
    row = evaluate_market(
        _info(),
        _book("0.48", "0.52"),
        _book("0.48", "0.52"),
        capital_grid=grid,
    )

    assert row["blocked_reason"] == ""
    assert Decimal(row["competition_q_min"]) > 0
    curve = row["capital_curve"]
    assert [entry["capital_usd"] for entry in curve] == ["100", "1000", "5000"]

    expected = [Decimal(entry["expected_daily_usd"]) for entry in curve]
    assert expected[0] > 0
    assert expected == sorted(expected)

    shares = [Decimal(entry["executable_share"]) for entry in curve]
    assert all(Decimal("0") < share < Decimal("1") for share in shares)

    marginals = [
        Decimal(entry["marginal_daily_roi_pct"])
        for entry in curve
        if "marginal_daily_roi_pct" in entry
    ]
    assert len(marginals) == 2
    assert marginals[0] >= marginals[1]


def test_evaluate_market_blocks_on_crossed_or_empty_book() -> None:
    crossed = evaluate_market(
        _info(),
        _book("0.55", "0.52"),
        _book("0.48", "0.52"),
    )
    empty = evaluate_market(_info(), {"bids": [], "asks": []}, _book("0.48", "0.52"))

    assert crossed["blocked_reason"] == "book_empty_or_crossed"
    assert crossed["capital_curve"] == []
    assert empty["blocked_reason"] == "book_empty_or_crossed"
