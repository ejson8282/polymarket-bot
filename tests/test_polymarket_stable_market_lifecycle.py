from __future__ import annotations

import time

from platforms.polymarket.maker.stable_market_lifecycle import (
    build_lifecycle_plan,
    candidate_is_executable_for_account,
)


def _market(token_id: str, paired_token_id: str, **extra):
    return {
        "token_id": token_id,
        "paired_token_id": paired_token_id,
        **extra,
    }


def _proposal(*, generated_at: float, account: dict) -> dict:
    return {
        "status": "ready",
        "generated_at": generated_at,
        "accounts": [account],
    }


def test_account_executable_candidate_requires_nonzero_q_and_canary_evidence():
    row = {
        "stable_lp_recommended": False,
        "account_admission": [
            {"account_index": 1, "level": "canary", "reason_codes": []}
        ],
        "account_execution": [
            {
                "account_index": 1,
                "executable": True,
                "executable_q_min": 12.5,
            }
        ],
        "canary_proposal_eligible_account_indexes": [1],
    }

    assert candidate_is_executable_for_account(
        row,
        1,
        allow_canary=True,
    )[:2] == (True, "canary")
    row["account_execution"][0]["executable_q_min"] = 0
    assert candidate_is_executable_for_account(
        row,
        1,
        allow_canary=True,
    ) == (False, "canary", ("executable_q_min_zero",))


def test_lifecycle_additions_are_account_local_limited_and_idempotent():
    now = time.time()
    proposal = _proposal(
        generated_at=now,
        account={
            "account_index": 1,
            "add": [
                _market("101", "102"),
                _market("201", "202"),
            ],
            "canary": [_market("301", "302")],
            "keep": [],
            "review": [],
        },
    )

    first = build_lifecycle_plan(
        proposal,
        account_index=1,
        configured_token_ids=set(),
        managed_token_ids=set(),
        previous_state={},
        now_ts=now + 1,
        max_add_per_cycle=2,
    )

    assert [row["market"]["token_id"] for row in first["add"]] == [
        "101",
        "201",
    ]
    assert first["new_sample"] is True

    repeated = build_lifecycle_plan(
        proposal,
        account_index=1,
        configured_token_ids=set(),
        managed_token_ids=set(),
        previous_state=first,
        now_ts=now + 2,
        max_add_per_cycle=2,
    )
    assert repeated["new_sample"] is False
    assert repeated["add"] == []


def test_soft_review_retires_only_after_three_distinct_samples():
    now = time.time()
    state = {}
    for offset in range(3):
        proposal = _proposal(
            generated_at=now + offset * 300,
            account={
                "account_index": 1,
                "add": [],
                "canary": [],
                "keep": [],
                "review": [
                    _market(
                        "101",
                        "102",
                        action="review_rotate",
                        reason_codes=["front_depth_below_account_min"],
                    )
                ],
            },
        )
        state = build_lifecycle_plan(
            proposal,
            account_index=1,
            configured_token_ids={"101", "102"},
            managed_token_ids={"101"},
            previous_state=state,
            now_ts=now + offset * 300 + 1,
            soft_failure_threshold=3,
        )
        assert bool(state["retire"]) is (offset == 2)

    assert state["markets"]["101"]["consecutive_failures"] == 3


def test_hard_review_retires_on_first_fresh_sample():
    now = time.time()
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now,
            account={
                "account_index": 1,
                "add": [],
                "canary": [],
                "keep": [],
                "review": [
                    _market(
                        "101",
                        "102",
                        action="review_retire",
                        reason_codes=["market_not_accepting_orders"],
                    )
                ],
            },
        ),
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        previous_state={},
        now_ts=now + 1,
    )

    assert state["retire"][0]["token_id"] == "101"
    assert state["markets"]["101"]["hard_failure"] is True


def test_stale_or_missing_proposal_never_adds_or_retires():
    now = time.time()
    previous_sample = now - 600
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now - 3600,
            account={
                "account_index": 1,
                "add": [_market("201", "202")],
                "canary": [],
                "keep": [],
                "review": [
                    _market(
                        "101",
                        "102",
                        action="review_retire",
                        reason_codes=["market_closed_or_unknown"],
                    )
                ],
            },
        ),
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        previous_state={"last_proposal_generated_at": previous_sample},
        now_ts=now,
        max_proposal_age_sec=900,
    )

    assert state["status"] == "blocked"
    assert state["add"] == []
    assert state["retire"] == []
    assert state["last_proposal_generated_at"] == previous_sample
