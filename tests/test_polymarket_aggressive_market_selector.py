import json
from pathlib import Path

import pytest

from platforms.polymarket.maker.aggressive_market_selector import (
    AggressiveSelectionError,
    main,
    select_aggressive_market_universe,
)


NOW = 2_000_000.0


def _candidate(
    token: int,
    *,
    roi: float,
    capital: float = 100.0,
    fill_risk: float = 20.0,
    stability: float = 90.0,
    recommended: bool = True,
    phase: str = "normal",
    yes_depth: float = 6_000.0,
    no_depth: float = 6_000.0,
    depth_status: str = "verified",
    depth_observed_at: float = NOW - 5,
) -> dict:
    return {
        "condition_id": "0x" + f"{token:064x}",
        "token_id": str(token),
        "paired_token_id": str(token + 10_000),
        "question": f"Market {token}?",
        "slug": f"market-{token}",
        "market_phase": phase,
        "verification_recommended": recommended,
        "probe_capital_usd": capital,
        "probe_shares_each_side": 101.0101,
        "rewards_max_spread": 0.045,
        "estimated_daily_gross_usd": roi * capital / 100,
        "risk_adjusted_daily_roi_pct": roi,
        "fill_risk": fill_risk,
        "stability_score": stability,
        "front_depth_status": depth_status,
        "front_depth_observed_at": depth_observed_at,
        "yes_tick_size": 0.01,
        "no_tick_size": 0.01,
        "yes_front_bid_notional_usd": yes_depth,
        "no_front_bid_notional_usd": no_depth,
        "min_front_bid_notional_usd": min(yes_depth, no_depth),
    }


def _observer(*rows: dict, generated_at: float = NOW - 30) -> dict:
    return {"generated_at": generated_at, "candidates": list(rows)}


def test_selector_ranks_qualified_markets_and_renders_review_only_universe() -> None:
    payload = select_aggressive_market_universe(
        _observer(
            _candidate(1, roi=1.0),
            _candidate(2, roi=4.0),
            _candidate(3, roi=8.0, phase="live"),
            _candidate(4, roi=7.0, recommended=False),
        ),
        principal_usdc=200,
        min_front_bid_notional_usdc=5_000,
        limit=2,
        now_ts=NOW,
    )

    assert [row["token_id"] for row in payload["markets"]] == ["2", "1"]
    assert payload["night_markets"] == []
    assert payload["build"]["selection_mode"] == "review_only"
    assert all(row["source"] == "aggressive_observer_selected" for row in payload["markets"])
    assert all(row["eligibility_managed"] is True for row in payload["markets"])


@pytest.mark.parametrize(
    "observer, principal, message",
    [
        (_observer(_candidate(1, roi=1), generated_at=NOW - 901), 200, "stale"),
        (_observer(_candidate(1, roi=1, capital=250)), 200, "no eligible"),
        (_observer(_candidate(1, roi=1, fill_risk=65)), 200, "no eligible"),
        (_observer(_candidate(1, roi=1, stability=69)), 200, "no eligible"),
    ],
)
def test_selector_fails_closed_for_stale_or_ineligible_input(
    observer: dict,
    principal: float,
    message: str,
) -> None:
    with pytest.raises(AggressiveSelectionError, match=message):
        select_aggressive_market_universe(
            observer,
            principal_usdc=principal,
            min_front_bid_notional_usdc=5_000,
            now_ts=NOW,
        )


def test_selector_skips_shallow_top_candidate_and_records_reason() -> None:
    payload = select_aggressive_market_universe(
        _observer(
            _candidate(1, roi=9.0, yes_depth=4_999),
            _candidate(2, roi=5.0),
        ),
        principal_usdc=200,
        min_front_bid_notional_usdc=5_000,
        now_ts=NOW,
    )

    assert [row["token_id"] for row in payload["markets"]] == ["2"]
    assert payload["build"]["depth_rejections"] == [
        {
            "token_id": "1",
            "condition_id": "0x" + f"{1:064x}",
            "reason": "front_depth_below_min",
            "yes_front_bid_notional_usd": 4_999,
            "no_front_bid_notional_usd": 6_000.0,
        }
    ]


@pytest.mark.parametrize(
    "candidate, reason",
    [
        (_candidate(1, roi=1, no_depth=4_999), "no eligible"),
        (_candidate(1, roi=1, depth_status="missing_tick_size"), "no eligible"),
        (_candidate(1, roi=1, depth_observed_at=NOW - 301), "no eligible"),
        (_candidate(1, roi=1, yes_depth=float("nan")), "no eligible"),
    ],
)
def test_selector_fails_closed_when_either_depth_leg_is_unusable(
    candidate: dict,
    reason: str,
) -> None:
    with pytest.raises(AggressiveSelectionError, match=reason):
        select_aggressive_market_universe(
            _observer(candidate),
            principal_usdc=200,
            min_front_bid_notional_usdc=5_000,
            now_ts=NOW,
        )


def test_selector_depth_rejections_remain_strict_json_for_nonfinite_input() -> None:
    payload = select_aggressive_market_universe(
        _observer(
            _candidate(1, roi=9.0, yes_depth=float("nan")),
            _candidate(2, roi=5.0),
        ),
        principal_usdc=200,
        min_front_bid_notional_usdc=5_000,
        now_ts=NOW,
    )

    assert payload["build"]["depth_rejections"][0]["yes_front_bid_notional_usd"] is None
    json.dumps(payload, allow_nan=False)


def test_selector_cli_does_not_write_without_output(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    observer_path = tmp_path / "observer.json"
    observer_path.write_text(
        json.dumps(_observer(_candidate(1, roi=1), generated_at=NOW)),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "platforms.polymarket.maker.aggressive_market_selector.time.time",
        lambda: NOW,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "aggressive_market_selector.py",
            "--observer",
            str(observer_path),
            "--principal-usdc",
            "200",
            "--min-front-bid-notional-usdc",
            "5000",
        ],
    )

    assert main() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["markets"][0]["token_id"] == "1"
    assert list(tmp_path.iterdir()) == [observer_path]
