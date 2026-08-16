import json
from pathlib import Path

from platforms.polymarket.maker.stable_rotation_planner import (
    OUTPUT_NAME,
    build_stable_rotation_proposal,
    load_stable_account_configs,
    refresh_stable_rotation_proposal,
)


NOW = 1_800_000_000.0


def _candidate(
    index: int,
    *,
    roi: float = 5.0,
    recommended: bool = True,
    fill_risk: float = 20.0,
    stability: float = 90.0,
    phase: str = "normal",
    active: bool = True,
    closed: bool = False,
    archived: bool = False,
    accepting_orders: bool = True,
    market_end_ts: float = NOW + 86_400,
    depth_status: str = "verified",
    depth_observed_at: float = NOW - 10,
    yes_depth: float = 6_000.0,
    no_depth: float = 6_000.0,
) -> dict:
    return {
        "condition_id": "0x" + f"{index:064x}",
        "token_id": str(index),
        "paired_token_id": str(index + 1_000),
        "question": f"Market {index}?",
        "slug": f"market-{index}",
        "market_url": f"https://polymarket.com/event/market-{index}",
        "market_type": "sports" if phase in {"pregame", "live"} else "always_on",
        "market_phase": phase,
        "game_start_ts": NOW + 18_000 if phase == "pregame" else None,
        "market_active": active,
        "market_closed": closed,
        "market_archived": archived,
        "accepting_orders": accepting_orders,
        "market_end_ts": market_end_ts,
        "verification_recommended": recommended,
        "verification_status": "stable" if recommended else "warming",
        "stability_score": stability,
        "fill_risk": fill_risk,
        "risk_adjusted_daily_roi_pct": roi,
        "estimated_daily_gross_usd": roi,
        "estimated_reward_share_pct": 25.0,
        "daily_reward_usd": 40.0,
        "probe_capital_usd": 100.0,
        "front_depth_status": depth_status,
        "front_depth_observed_at": depth_observed_at,
        "yes_front_bid_notional_usd": yes_depth,
        "no_front_bid_notional_usd": no_depth,
        "min_front_bid_notional_usd": min(yes_depth, no_depth),
    }


def _market(candidate: dict, *, enabled: bool = True) -> dict:
    return {
        "condition_id": candidate["condition_id"],
        "token_id": candidate["token_id"],
        "paired_token_id": candidate["paired_token_id"],
        "question": candidate["question"],
        "slug": candidate["slug"],
        "enabled": enabled,
    }


def _account(index: int, *markets: dict, min_depth: float = 2_000.0) -> dict:
    return {
        "account_index": index,
        "config_name": f"config_{index}.json",
        "min_front_bid_notional_usdc": min_depth,
        "markets": list(markets),
    }


def _observer(*candidates: dict, generated_at: float = NOW - 30) -> dict:
    return {
        "generated_at": generated_at,
        "candidates": list(candidates),
    }


def test_proposal_selects_per_account_and_allows_cross_account_duplicates() -> None:
    current = _candidate(1, roi=10.0)
    proposal = build_stable_rotation_proposal(
        _observer(
            current,
            _candidate(2, roi=9.0),
            _candidate(3, roi=8.0),
        ),
        [_account(1, _market(current)), _account(2)],
        now_ts=NOW,
        max_add_per_account=2,
    )

    account_one, account_two = proposal["accounts"]
    assert [row["token_id"] for row in account_one["keep"]] == ["1"]
    assert [row["token_id"] for row in account_one["add"]] == ["2", "3"]
    assert [row["token_id"] for row in account_two["add"]] == ["1", "2"]
    assert proposal["policy"]["cross_account_duplicate_events"] == "allowed"
    assert proposal["summary"]["planned_additions"] == 4
    assert proposal["safety"] == {
        "proposal_only": True,
        "runtime_config_writes": False,
        "runtime_commands": False,
        "trading_actions": False,
        "requires_manual_review": True,
    }


def test_proposal_rejects_unverified_risky_live_and_stale_candidates() -> None:
    good = _candidate(1, roi=5.0)
    shallow = _candidate(2, roi=9.0, yes_depth=1_999.0)
    proposal = build_stable_rotation_proposal(
        _observer(
            _candidate(3, roi=10.0, recommended=False),
            _candidate(4, roi=10.0, fill_risk=35.0),
            _candidate(5, roi=10.0, phase="live"),
            _candidate(6, roi=10.0, depth_observed_at=NOW - 601),
            shallow,
            good,
        ),
        [_account(1)],
        now_ts=NOW,
    )

    assert [row["token_id"] for row in proposal["accounts"][0]["add"]] == ["1"]
    rejected = {
        row["token_id"]: set(row["reason_codes"])
        for row in proposal["rejected_candidates"]
    }
    assert "verification_not_recommended" in rejected["3"]
    assert "fill_risk_above_stable_limit" in rejected["4"]
    assert "live_market_observe_only" in rejected["5"]
    assert "front_depth_stale" in rejected["6"]
    assert proposal["unassigned_candidates"][0]["token_id"] == "2"
    assert proposal["unassigned_candidates"][0]["reason_codes_by_account"] == {
        "1": "front_depth_below_account_min"
    }


def test_operator_disabled_market_is_never_reintroduced() -> None:
    disabled = _candidate(1, roi=9.0)
    active_elsewhere = _candidate(2, roi=8.0)
    proposal = build_stable_rotation_proposal(
        _observer(disabled, active_elsewhere),
        [_account(1, _market(disabled, enabled=False)), _account(2)],
        now_ts=NOW,
    )

    account_one, account_two = proposal["accounts"]
    assert [row["token_id"] for row in account_one["disabled_hold"]] == ["1"]
    assert [row["token_id"] for row in account_one["add"]] == ["2"]
    assert [row["token_id"] for row in account_two["add"]] == ["1", "2"]


def test_current_markets_get_explicit_review_reasons_without_auto_retirement() -> None:
    closed = _candidate(1, closed=True, active=False)
    missing = {
        "condition_id": "0x" + f"{999:064x}",
        "token_id": "999",
        "paired_token_id": "1999",
        "question": "Missing from observer?",
        "enabled": True,
    }
    proposal = build_stable_rotation_proposal(
        _observer(closed),
        [_account(1, _market(closed), missing)],
        now_ts=NOW,
    )

    review = {row["token_id"]: row for row in proposal["accounts"][0]["review"]}
    assert review["1"]["action"] == "review_retire"
    assert "market_not_active" in review["1"]["reason_codes"]
    assert review["999"]["action"] == "review"
    assert review["999"]["reason_codes"] == [
        "not_in_current_observer_top_candidates"
    ]
    assert proposal["safety"]["trading_actions"] is False


def test_stale_observer_blocks_every_addition() -> None:
    proposal = build_stable_rotation_proposal(
        _observer(_candidate(1), generated_at=NOW - 901),
        [_account(1)],
        now_ts=NOW,
    )

    assert proposal["status"] == "blocked"
    assert proposal["reason"] == "reward_observer_snapshot_stale_or_invalid"
    assert proposal["summary"]["planned_additions"] == 0
    assert proposal["accounts"] == []


def test_refresh_writes_only_proposal_and_does_not_mutate_configs(tmp_path: Path) -> None:
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    candidate = _candidate(1)
    config_path = config_dir / "config_1.json"
    config_path.write_text(
        json.dumps(
            {
                "account": {"funder": "0x" + "1" * 40},
                "execution": {"min_front_bid_notional_usdc": 2_000},
                "markets": [],
                "night_markets": [],
            }
        ),
        encoding="utf-8",
    )
    before = config_path.read_bytes()

    summary = refresh_stable_rotation_proposal(
        data_dir,
        config_dir,
        _observer(candidate),
        now_ts=NOW,
    )

    assert summary is not None
    assert summary["planned_additions"] == 1
    assert config_path.read_bytes() == before
    payload = json.loads((data_dir / OUTPUT_NAME).read_text(encoding="utf-8"))
    assert payload["mode"] == "proposal_only"
    assert payload["accounts"][0]["add"][0]["token_id"] == "1"
    assert "funder" not in json.dumps(payload)


def test_aggressive_configs_are_out_of_scope(tmp_path: Path) -> None:
    config_dir = tmp_path / "maker"
    data_dir = tmp_path / "data"
    config_dir.mkdir()
    (config_dir / "config_1.json").write_text(
        json.dumps(
            {
                "account": {"lp_account": {"profile_type": "aggressive"}},
                "markets": [],
                "night_markets": [],
            }
        ),
        encoding="utf-8",
    )

    assert load_stable_account_configs(config_dir) == []
    assert (
        refresh_stable_rotation_proposal(
            data_dir,
            config_dir,
            _observer(_candidate(1)),
            now_ts=NOW,
        )
        is None
    )
    assert not (data_dir / OUTPUT_NAME).exists()
