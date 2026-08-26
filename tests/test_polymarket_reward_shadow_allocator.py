from platforms.polymarket.maker.reward_shadow_allocator import build_shadow_budget


def _candidate(
    condition: str,
    *,
    actual_share: float | None,
    depth: float,
    admission: str = "full",
) -> dict:
    return {
        "condition_id": condition,
        "token_id": condition + "-yes",
        "paired_token_id": condition + "-no",
        "question": condition,
        "daily_reward_usd": 100,
        "fill_risk": 20,
        "eligible_sample_ratio": 1,
        "scoring_sample_ratio": 1,
        "account_admission": [
            {"account_index": 1, "level": admission, "reason_codes": []}
        ],
        "account_execution": [
            {
                "account_index": 1,
                "executable": True,
                "executable_q_min": 50,
                "target_shares": 100,
                "collateral_required_usdc": 100,
                "actual_reward_share_pct": actual_share,
                "executable_share_pct": actual_share,
                "min_front_bid_notional_usd": depth,
                "min_front_bid_notional_usdc": 2_000,
                "budget_pct": 0.8,
            }
        ],
    }


def test_shadow_allocator_uses_actual_reward_and_normalizes_full_markets() -> None:
    report = build_shadow_budget(
        {
            "generated_at": 1_800_000_000,
            "candidates": [
                _candidate("deep", actual_share=10, depth=4_000),
                _candidate("thin", actual_share=10, depth=500),
            ],
        }
    )

    rows = report["accounts"][0]["suggestions"]
    by_condition = {row["condition_id"]: row for row in rows}
    assert by_condition["deep"]["reward_evidence_source"] == (
        "official_current_percentage"
    )
    assert by_condition["deep"]["suggested_budget_pct"] > by_condition["thin"][
        "suggested_budget_pct"
    ]
    assert round(sum(row["suggested_budget_pct"] for row in rows), 5) == 1
    assert report["production_dynamic_budget_changed"] is False


def test_canary_is_reported_but_gets_no_production_budget_suggestion() -> None:
    report = build_shadow_budget(
        {
            "candidates": [
                _candidate("canary", actual_share=20, depth=4_000, admission="canary")
            ]
        }
    )

    row = report["accounts"][0]["suggestions"][0]
    assert row["eligible_for_shadow_allocation"] is False
    assert row["suggested_budget_pct"] == 0


def test_shadow_allocator_calibrates_model_with_finalized_official_earnings() -> None:
    candidate = _candidate("calibrated", actual_share=None, depth=4_000)
    candidate["earnings_calibration_scopes"] = 2
    candidate["earnings_calibration_ratio"] = 0.5
    candidate["account_execution"][0]["executable_share_pct"] = 10

    report = build_shadow_budget({"candidates": [candidate]})

    row = report["accounts"][0]["suggestions"][0]
    assert row["reward_evidence_source"] == "official_earnings_calibrated_model"
    assert row["calibrated_daily_reward_usd"] == 5.0
    assert row["earnings_calibration_ratio"] == 0.5


def test_shadow_allocator_ignores_probe_account_zero() -> None:
    candidate = _candidate("probe", actual_share=10, depth=4_000)
    candidate["account_admission"][0]["account_index"] = 0
    candidate["account_execution"][0]["account_index"] = 0

    report = build_shadow_budget({"candidates": [candidate]})

    assert report["accounts"] == []
