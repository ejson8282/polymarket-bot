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


def test_lifecycle_limits_canaries_per_account_and_respects_budget():
    now = time.time()
    proposal = _proposal(
        generated_at=now,
        account={
            "account_index": 1,
            "add": [],
            "canary": [
                _market("301", "302", rewards_min_size_shares=50),
                _market("401", "402", rewards_min_size_shares=101),
            ],
            "keep": [],
            "review": [],
        },
    )

    plan = build_lifecycle_plan(
        proposal,
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        managed_market_stages={"101": "canary"},
        previous_state={},
        now_ts=now + 1,
        max_active_canaries=2,
        canary_budget_usdc=100,
    )

    assert plan["active_canaries"] == 1
    assert [row["market"]["token_id"] for row in plan["add"]] == ["301"]

    capped = build_lifecycle_plan(
        _proposal(
            generated_at=now + 300,
            account=proposal["accounts"][0],
        ),
        account_index=1,
        configured_token_ids={"101", "102", "201", "202"},
        managed_token_ids={"101", "201"},
        managed_market_stages={"101": "canary", "201": "canary"},
        previous_state=plan,
        now_ts=now + 301,
        max_active_canaries=2,
        canary_budget_usdc=100,
    )

    assert capped["active_canaries"] == 2
    assert capped["add"] == []


def test_canary_promotes_only_after_three_consecutive_scoring_samples():
    now = time.time()
    state = {}
    for offset in range(3):
        proposal = _proposal(
            generated_at=now + offset * 300,
            account={
                "account_index": 1,
                "add": [],
                "canary": [],
                "keep": [
                    _market(
                        "101",
                        "102",
                        fill_risk=20,
                        account_execution_evidence={
                            "account_index": 1,
                            "official_scoring": True,
                            "observed_q_min": 12.5,
                            "executable_q_min": 15,
                            "scoring_sample_id": f"{offset + 1:064x}",
                        },
                    )
                ],
                "review": [],
            },
        )
        state = build_lifecycle_plan(
            proposal,
            account_index=1,
            configured_token_ids={"101", "102"},
            managed_token_ids={"101"},
            managed_market_stages={"101": "canary"},
            previous_state=state,
            now_ts=now + offset * 300 + 1,
            promotion_scoring_threshold=3,
        )
        assert bool(state["promote"]) is (offset == 2)

    assert state["promote"] == [
        {
            "token_id": "101",
            "target_risk": "low",
            "consecutive_scoring_samples": 3,
        }
    ]


def test_same_scoring_sample_is_counted_only_once_across_proposal_refreshes():
    now = time.time()
    state = {}
    for offset in range(3):
        state = build_lifecycle_plan(
            _proposal(
                generated_at=now + offset * 300,
                account={
                    "account_index": 1,
                    "add": [],
                    "canary": [],
                    "keep": [
                        _market(
                            "101",
                            "102",
                            account_execution_evidence={
                                "account_index": 1,
                                "official_scoring": True,
                                "observed_q_min": 9,
                                "scoring_sample_id": "a" * 64,
                            },
                        )
                    ],
                    "review": [],
                },
            ),
            account_index=1,
            configured_token_ids={"101", "102"},
            managed_token_ids={"101"},
            managed_market_stages={"101": "canary"},
            previous_state=state,
            now_ts=now + offset * 300 + 1,
            promotion_scoring_threshold=1,
        )

    assert state["promote"] == []
    assert state["markets"]["101"]["consecutive_scoring_samples"] == 1


def test_v2_scoring_counters_are_not_carried_into_v3():
    now = time.time()
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now,
            account={
                "account_index": 1,
                "add": [],
                "canary": [],
                "keep": [
                    _market(
                        "101",
                        "102",
                        account_execution_evidence={
                            "account_index": 1,
                            "official_scoring": True,
                            "observed_q_min": 9,
                            "scoring_sample_id": "b" * 64,
                        },
                    )
                ],
                "review": [],
            },
        ),
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        managed_market_stages={"101": "canary"},
        previous_state={
            "version": 2,
            "last_proposal_generated_at": now - 300,
            "markets": {
                "101": {
                    "consecutive_scoring_samples": 2,
                    "last_scoring_sample_id": "a" * 64,
                }
            },
        },
        now_ts=now + 1,
        promotion_scoring_threshold=1,
    )

    assert state["version"] == 3
    assert state["promote"] == []
    assert state["markets"]["101"]["consecutive_scoring_samples"] == 1


def test_hard_canary_cap_blocks_additions_and_promotions_when_exceeded():
    now = time.time()
    stages = {str(index): "canary" for index in range(101, 112)}
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now,
            account={
                "account_index": 1,
                "add": [],
                "canary": [_market("301", "302", rewards_min_size_shares=1)],
                "keep": [
                    _market(
                        "101",
                        "102",
                        account_execution_evidence={
                            "account_index": 1,
                            "official_scoring": True,
                            "observed_q_min": 9,
                            "scoring_sample_id": "c" * 64,
                        },
                    )
                ],
                "review": [],
            },
        ),
        account_index=1,
        configured_token_ids=set(stages),
        managed_token_ids={"101"},
        managed_market_stages=stages,
        previous_state={},
        now_ts=now + 1,
        max_active_canaries=99,
        canary_budget_usdc=1_000,
        promotion_scoring_threshold=1,
    )

    assert state["max_active_canaries"] == 10
    assert state["active_canaries"] == 11
    assert state["canary_limit_exceeded"] is True
    assert state["add"] == []
    assert state["promote"] == []


def test_proposal_account_and_host_identity_must_match_runtime():
    now = time.time()
    account = {
        "account_index": 1,
        "account_uid_key": "account-a",
        "host_id": "vps1",
        "add": [_market("101", "102")],
        "canary": [],
        "keep": [],
        "review": [],
    }

    uid_mismatch = build_lifecycle_plan(
        _proposal(generated_at=now, account=account),
        account_index=1,
        configured_token_ids=set(),
        managed_token_ids=set(),
        previous_state={},
        now_ts=now + 1,
        expected_account_uid_key="account-b",
        expected_host_id="vps1",
    )
    host_mismatch = build_lifecycle_plan(
        _proposal(generated_at=now, account=account),
        account_index=1,
        configured_token_ids=set(),
        managed_token_ids=set(),
        previous_state={},
        now_ts=now + 1,
        expected_account_uid_key="account-a",
        expected_host_id="vps2",
    )

    assert uid_mismatch["reason"] == "proposal_account_identity_mismatch"
    assert uid_mismatch["add"] == []
    assert host_mismatch["reason"] == "proposal_host_identity_mismatch"
    assert host_mismatch["add"] == []


def test_lifecycle_scoring_streak_cannot_cross_account_identity():
    now = time.time()
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now,
            account={
                "account_index": 1,
                "account_uid_key": "account-b",
                "host_id": "vps1",
                "add": [],
                "canary": [],
                "keep": [
                    _market(
                        "101",
                        "102",
                        account_execution_evidence={
                            "account_index": 1,
                            "account_uid_key": "account-b",
                            "host_id": "vps1",
                            "official_scoring": True,
                            "observed_q_min": 9,
                            "scoring_sample_id": "b" * 64,
                        },
                    )
                ],
                "review": [],
            },
        ),
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        managed_market_stages={"101": "canary"},
        previous_state={
            "version": 3,
            "account_index": 1,
            "account_uid_key": "account-a",
            "host_id": "vps1",
            "last_proposal_generated_at": now - 300,
            "markets": {
                "101": {
                    "consecutive_scoring_samples": 2,
                    "last_scoring_sample_id": "a" * 64,
                }
            },
        },
        now_ts=now + 1,
        expected_account_uid_key="account-b",
        expected_host_id="vps1",
    )

    assert state["account_uid_key"] == "account-b"
    assert state["host_id"] == "vps1"
    assert state["promote"] == []
    assert state["markets"]["101"]["consecutive_scoring_samples"] == 1


def test_canary_promotion_streak_resets_when_scoring_is_not_true():
    now = time.time()
    state = {}
    for offset, scoring in enumerate((True, False, True, True)):
        state = build_lifecycle_plan(
            _proposal(
                generated_at=now + offset * 300,
                account={
                    "account_index": 1,
                    "add": [],
                    "canary": [],
                    "keep": [
                        _market(
                            "101",
                            "102",
                            account_execution_evidence={
                                "account_index": 1,
                                "official_scoring": scoring,
                                "observed_q_min": 9,
                                "scoring_sample_id": (
                                    f"{offset + 1:064x}" if scoring else None
                                ),
                            },
                        )
                    ],
                    "review": [],
                },
            ),
            account_index=1,
            configured_token_ids={"101", "102"},
            managed_token_ids={"101"},
            managed_market_stages={"101": "canary"},
            previous_state=state,
            now_ts=now + offset * 300 + 1,
            promotion_scoring_threshold=3,
        )

    assert state["promote"] == []
    assert state["markets"]["101"]["consecutive_scoring_samples"] == 2


def test_canary_promotion_streak_resets_when_market_is_unassessed():
    now = time.time()
    scoring_row = _market(
        "101",
        "102",
        account_execution_evidence={
            "account_index": 1,
            "official_scoring": True,
            "observed_q_min": 9,
            "scoring_sample_id": "1" * 64,
        },
    )
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now,
            account={
                "account_index": 1,
                "add": [],
                "canary": [],
                "keep": [scoring_row],
                "review": [],
            },
        ),
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        managed_market_stages={"101": "canary"},
        previous_state={},
        now_ts=now + 1,
    )
    state = build_lifecycle_plan(
        _proposal(
            generated_at=now + 300,
            account={
                "account_index": 1,
                "add": [],
                "canary": [],
                "keep": [],
                "review": [],
            },
        ),
        account_index=1,
        configured_token_ids={"101", "102"},
        managed_token_ids={"101"},
        managed_market_stages={"101": "canary"},
        previous_state=state,
        now_ts=now + 301,
    )

    assert state["promote"] == []
    assert state["markets"]["101"]["status"] == "unassessed"
    assert state["markets"]["101"]["consecutive_scoring_samples"] == 0


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
        previous_state={
            "version": 3,
            "account_index": 1,
            "last_proposal_generated_at": previous_sample,
        },
        now_ts=now,
        max_proposal_age_sec=900,
    )

    assert state["status"] == "blocked"
    assert state["add"] == []
    assert state["retire"] == []
    assert state["last_proposal_generated_at"] == previous_sample
