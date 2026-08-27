from decimal import Decimal

from platforms.polymarket.research.markout import (
    Fill,
    markout_for_fill,
    normalize_trades,
    price_at,
    summarize_markouts,
)


def _fill(side: str, ts: int = 1_000, price: str = "0.50", size: str = "100") -> Fill:
    return Fill(
        ts=ts,
        token_id="111",
        condition_id="0xabc",
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        slug="demo-market",
        outcome="Yes",
        tx="0xdeadbeef",
    )


def test_normalize_trades_parses_and_skips_malformed() -> None:
    rows = [
        {
            "asset": "111",
            "conditionId": "0xabc",
            "side": "buy",
            "price": 0.42,
            "size": 120,
            "timestamp": 2_000,
            "slug": "demo-market",
            "outcome": "Yes",
            "transactionHash": "0x01",
        },
        {"asset": "111", "side": "BUY", "price": 0.42, "size": 0, "timestamp": 2_100},
        {"asset": "", "side": "SELL", "price": 0.42, "size": 5, "timestamp": 2_200},
        {"asset": "111", "side": "HOLD", "price": 0.42, "size": 5, "timestamp": 2_300},
        {
            "asset": "111",
            "side": "SELL",
            "price": 0.44,
            "size": 10,
            "timestamp": 1_500,
        },
    ]

    fills = normalize_trades(rows)

    assert [fill.ts for fill in fills] == [1_500, 2_000]
    assert fills[1].side == "BUY"
    assert fills[1].price == Decimal("0.42")


def test_price_at_uses_nearest_sample_within_tolerance() -> None:
    series = [(1_000, 0.50), (1_060, 0.52), (1_120, 0.55)]

    assert price_at(series, 1_055, tolerance_sec=30) == Decimal("0.52")
    assert price_at(series, 1_090, tolerance_sec=30) == Decimal("0.52")
    assert price_at(series, 5_000, tolerance_sec=30) is None
    assert price_at([], 1_000) is None


def test_markout_sign_convention_by_side() -> None:
    series = [(1_000, 0.50), (1_060, 0.54)]

    buy = markout_for_fill(_fill("BUY"), series, 60, tolerance_sec=30)
    sell = markout_for_fill(_fill("SELL"), series, 60, tolerance_sec=30)

    assert buy == Decimal("0.04")
    assert sell == Decimal("-0.04")


def test_summarize_markouts_aggregates_and_counts_gaps() -> None:
    fills = [
        _fill("BUY", ts=1_000, price="0.50", size="100"),
        _fill("BUY", ts=1_000, price="0.50", size="300"),
    ]
    series = {"111": [(1_000, 0.50), (1_060, 0.52)]}

    report = summarize_markouts(
        fills,
        series,
        horizons_sec=(60, 300),
        tolerance_sec=30,
    )

    assert report["fills_total"] == 2
    # The 300s horizon has no sample within tolerance for either fill.
    assert report["samples_skipped_missing_price"] == 2
    stats = report["by_horizon_sec"]["60"]
    assert stats["n"] == 2
    assert stats["mean_per_share"] == 0.02
    assert stats["total_usd"] == 8.0  # 0.02 * (100 + 300)
    assert stats["win_rate"] == 1.0
    assert report["by_market"]["demo-market"]["60"]["n"] == 2
