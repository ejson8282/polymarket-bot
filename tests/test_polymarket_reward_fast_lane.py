from pathlib import Path

from platforms.polymarket.maker.reward_fast_lane import (
    forced_condition_ids,
    update_fast_lane,
)


def _candidate(
    *,
    scoring=None,
    observed_q=None,
    reward=100.0,
    admission="canary",
) -> dict:
    return {
        "condition_id": "condition-1",
        "token_id": "1",
        "paired_token_id": "2",
        "question": "Market one?",
        "slug": "market-one",
        "daily_reward_usd": reward,
        "rewards_min_size_shares": 10,
        "rewards_max_spread": 0.05,
        "market_end_ts": 2_000_000_000,
        "competition_score_estimate": 100,
        "admission_level": admission,
        "account_execution": [
            {
                "account_index": 1,
                "configured": scoring is not None,
                "executable": True,
                "official_scoring": scoring,
                "observed_q_min": observed_q,
            }
        ],
    }


def test_first_sample_enters_watchlist_and_forces_next_poll(tmp_path: Path) -> None:
    update_fast_lane(
        tmp_path,
        {"candidates": [], "unassessed_candidates": []},
        now_ts=1_799_999_700,
    )
    state = {
        "candidates": [],
        "unassessed_candidates": [
            {
                "condition_id": "condition-1",
                "token_id": "1",
                "paired_token_id": "2",
                "daily_reward_usd": 100,
                "rewards_min_size_shares": 10,
                "rewards_max_spread": 0.05,
                "assessment_status": "unassessed",
            }
        ],
    }

    fast = update_fast_lane(tmp_path, state, now_ts=1_800_000_000)

    assert fast["markets"]["condition:condition-1"]["stage"] == "watchlist"
    assert forced_condition_ids(tmp_path, now_ts=1_800_000_300) == {
        "condition-1"
    }


def test_bootstrap_seeds_existing_markets_without_forcing_all_books(
    tmp_path: Path,
) -> None:
    state = {
        "candidates": [],
        "unassessed_candidates": [
            {
                "condition_id": "existing",
                "token_id": "1",
                "paired_token_id": "2",
                "daily_reward_usd": 100,
                "rewards_min_size_shares": 10,
                "rewards_max_spread": 0.05,
            }
        ],
    }

    fast = update_fast_lane(tmp_path, state, now_ts=1_800_000_000)

    row = fast["markets"]["condition:existing"]
    assert row["trigger_reasons"] == ["baseline_seeded"]
    assert row["force_next_poll"] is False
    assert forced_condition_ids(tmp_path, now_ts=1_800_000_300) == set()


def test_two_executable_samples_enable_canary_proposal(tmp_path: Path) -> None:
    first = {"candidates": [_candidate()], "unassessed_candidates": []}
    update_fast_lane(tmp_path, first, now_ts=1_800_000_000)
    assert first["candidates"][0]["canary_proposal_eligible"] is False

    second = {"candidates": [_candidate()], "unassessed_candidates": []}
    fast = update_fast_lane(tmp_path, second, now_ts=1_800_000_300)

    row = fast["markets"]["condition:condition-1"]
    assert row["stage"] == "canary_proposal"
    assert row["consecutive_executable_samples"] == 2
    assert second["candidates"][0]["canary_proposal_eligible"] is True


def test_reward_change_resets_executable_evidence(tmp_path: Path) -> None:
    update_fast_lane(
        tmp_path,
        {"candidates": [_candidate()], "unassessed_candidates": []},
        now_ts=1_800_000_000,
    )
    state = {
        "candidates": [_candidate(reward=120.0)],
        "unassessed_candidates": [],
    }

    fast = update_fast_lane(tmp_path, state, now_ts=1_800_000_300)

    row = fast["markets"]["condition:condition-1"]
    assert row["consecutive_executable_samples"] == 1
    assert row["stage"] == "watchlist"
    assert "reward_config_changed" in row["trigger_reasons"]


def test_official_scoring_and_positive_q_allow_expansion_evidence(
    tmp_path: Path,
) -> None:
    state = {
        "candidates": [_candidate(scoring=True, observed_q=12.5)],
        "unassessed_candidates": [],
    }

    fast = update_fast_lane(tmp_path, state, now_ts=1_800_000_000)

    row = fast["markets"]["condition:condition-1"]
    assert row["stage"] == "expansion_validated"
    assert row["expansion_eligible"] is True


def test_rejected_market_cannot_become_canary_or_expansion_eligible(
    tmp_path: Path,
) -> None:
    state = {
        "candidates": [
            _candidate(scoring=True, observed_q=12.5, admission="reject")
        ],
        "unassessed_candidates": [],
    }

    fast = update_fast_lane(tmp_path, state, now_ts=1_800_000_000)

    row = fast["markets"]["condition:condition-1"]
    assert row["stage"] == "watchlist"
    assert row["canary_proposal_eligible"] is False
    assert row["expansion_eligible"] is False


def test_canary_evidence_is_tracked_per_account(tmp_path: Path) -> None:
    first_candidate = _candidate()
    first_candidate["account_admission"] = [
        {"account_index": 1, "level": "canary"},
        {"account_index": 2, "level": "canary"},
    ]
    first_candidate["account_execution"] = [
        {
            "account_index": 1,
            "configured": False,
            "executable": True,
            "official_scoring": None,
            "observed_q_min": None,
        },
        {
            "account_index": 2,
            "configured": False,
            "executable": False,
            "official_scoring": None,
            "observed_q_min": None,
        },
    ]
    update_fast_lane(
        tmp_path,
        {"candidates": [first_candidate], "unassessed_candidates": []},
        now_ts=1_800_000_000,
    )

    second_candidate = _candidate()
    second_candidate["account_admission"] = first_candidate["account_admission"]
    second_candidate["account_execution"] = [
        {**first_candidate["account_execution"][0]},
        {**first_candidate["account_execution"][1], "executable": True},
    ]
    fast = update_fast_lane(
        tmp_path,
        {"candidates": [second_candidate], "unassessed_candidates": []},
        now_ts=1_800_000_300,
    )

    row = fast["markets"]["condition:condition-1"]
    assert row["accounts"]["1"]["consecutive_executable_samples"] == 2
    assert row["accounts"]["2"]["consecutive_executable_samples"] == 1
    assert row["canary_proposal_eligible_account_indexes"] == [1]
    assert second_candidate["canary_proposal_eligible_account_indexes"] == [1]
