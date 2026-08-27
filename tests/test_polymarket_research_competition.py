import time
from decimal import Decimal

from platforms.polymarket.research.competition_scan import (
    MarketInfo,
    book_levels,
    book_top,
    evaluate_market,
    market_passes_guards,
    parse_sampling_market,
    portfolio_view,
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


def test_parse_sampling_market_reads_end_date() -> None:
    row = _sampling_row()
    row["end_date_iso"] = "2026-09-30T00:00:00Z"
    info = parse_sampling_market(row)

    assert info is not None
    assert info.end_ts is not None
    assert info.end_ts > 0


def test_market_passes_guards_filters_expiry_and_slugs() -> None:
    now = time.time()
    soon = MarketInfo(
        condition_id="0x1", question="q", slug="expiring-market",
        yes_token="1", no_token="2", tick=Decimal("0.01"),
        max_spread=Decimal("3.5"), rewards_min_size=Decimal("50"),
        daily_rate_usd=Decimal("100"), end_ts=now + 3600,
    )
    weather = MarketInfo(
        condition_id="0x2", question="q",
        slug="highest-temperature-in-shanghai-on-august-27",
        yes_token="3", no_token="4", tick=Decimal("0.01"),
        max_spread=Decimal("3.5"), rewards_min_size=Decimal("50"),
        daily_rate_usd=Decimal("100"), end_ts=now + 30 * 86400,
    )
    long_dated = MarketInfo(
        condition_id="0x3", question="q", slug="iran-agreement-by-september-30",
        yes_token="5", no_token="6", tick=Decimal("0.01"),
        max_spread=Decimal("3.5"), rewards_min_size=Decimal("50"),
        daily_rate_usd=Decimal("100"), end_ts=now + 30 * 86400,
    )
    unknown_end = MarketInfo(
        condition_id="0x4", question="q", slug="no-end-date",
        yes_token="7", no_token="8", tick=Decimal("0.01"),
        max_spread=Decimal("3.5"), rewards_min_size=Decimal("50"),
        daily_rate_usd=Decimal("100"),
    )

    kwargs = {
        "now_ts": now,
        "min_hours_to_end": 12.0,
        "exclude_slug_keywords": ("temperature-in", "-updown-"),
    }
    assert market_passes_guards(soon, **kwargs) == (False, "too_close_to_end")
    passed, reason = market_passes_guards(weather, **kwargs)
    assert (passed, reason) == (False, "slug_excluded:temperature-in")
    assert market_passes_guards(long_dated, **kwargs) == (True, "")
    # Fail closed: unknown end date is dropped when the guard is armed.
    assert market_passes_guards(unknown_end, **kwargs) == (False, "end_date_unknown")
    # Guards off: everything passes.
    assert market_passes_guards(soon, now_ts=now) == (True, "")


def test_portfolio_view_stacks_rewards_on_reused_principal() -> None:
    def _row(slug: str, expected: str, shares: str, roi: str) -> dict:
        return {
            "slug": slug,
            "condition_id": "0x" + slug,
            "hours_to_end": 500.0,
            "daily_rate_usd": "200",
            "competition_q_min": "100",
            "blocked_reason": "",
            "capital_curve": [
                {
                    "capital_usd": "1000",
                    "target_shares": shares,
                    "executable_share": "0.5",
                    "expected_daily_usd": expected,
                    "daily_roi_pct": roi,
                    "blocked_reasons": [],
                }
            ],
        }

    report = {
        "reference_capital_usd": "1000",
        "markets": [
            _row("a", "30", "1000", "3.0"),
            _row("b", "20", "1000", "2.0"),
            {"slug": "blocked", "blocked_reason": "book_empty_or_crossed"},
            _row("c", "1", "1000", "0.1"),  # below min ROI
        ],
    }

    view = portfolio_view(
        report,
        principal_usd=Decimal("1000"),
        top_n=10,
        min_daily_roi_pct=Decimal("0.5"),
    )

    assert view["markets_quoted"] == 2
    # Rewards stack on one reused principal: 30 + 20 on the same $1000.
    assert Decimal(view["total_expected_daily_usd"]) == Decimal("50")
    assert Decimal(view["stacked_daily_roi_pct"]) == Decimal("5")
    # If both events filled fully, collateral needed is 2x the principal.
    assert Decimal(view["overcommit_multiple"]) == Decimal("2")


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
