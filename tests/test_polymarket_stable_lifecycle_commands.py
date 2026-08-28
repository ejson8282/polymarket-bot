from __future__ import annotations

import copy

import pytest

from platforms.polymarket.maker.stable_lifecycle_commands import (
    StableLifecycleCommandError,
    build_stable_lifecycle_command,
    confirmation_for_change,
    normalized_lifecycle_config,
    validate_enablement_proposal,
    validate_stable_lifecycle_command,
)


NOW = 1_800_000_000.0
ACCOUNT_UID_KEY = "a1b2c3d4e5f60718"
HOST_ID = "vm-0-11-ubuntu"


def _proposal() -> dict:
    return {
        "schema_version": 3,
        "mode": "proposal_only",
        "status": "ready",
        "generated_at": NOW - 30,
        "safety": {
            "proposal_only": True,
            "requires_manual_review": True,
            "trading_actions": False,
            "runtime_commands": False,
            "runtime_config_writes": False,
        },
        "accounts": [
            {
                "account_index": 1,
                "account_uid_key": ACCOUNT_UID_KEY,
                "host_id": HOST_ID,
                "add": [],
                "canary": [],
                "keep": [],
                "review": [],
            }
        ],
    }


def test_enable_command_is_bound_to_fresh_host_local_proposal() -> None:
    command = build_stable_lifecycle_command(
        _proposal(),
        account_index=1,
        enabled=True,
        command_id="dashboard-lifecycle-1-enable",
        now_ts=NOW,
    )

    assert command == {
        "version": 1,
        "command_id": "dashboard-lifecycle-1-enable",
        "action": "set_stable_market_lifecycle",
        "created_at": NOW,
        "account_index": 1,
        "enabled": True,
        "account_uid_key": ACCOUNT_UID_KEY,
        "host_id": HOST_ID,
        "proposal_generated_at": NOW - 30,
        "confirm": confirmation_for_change(
            1,
            True,
            identity={
                "account_index": 1,
                "account_uid_key": ACCOUNT_UID_KEY,
                "host_id": HOST_ID,
                "proposal_generated_at": NOW - 30,
            },
        ),
    }
    validated = validate_stable_lifecycle_command(
        command,
        proposal=_proposal(),
        expected_account_index=1,
        expected_account_uid_key=ACCOUNT_UID_KEY,
        expected_host_id=HOST_ID,
        now_ts=NOW,
    )
    assert validated["enabled"] is True


def test_disable_command_needs_no_proposal_but_remains_account_scoped() -> None:
    command = build_stable_lifecycle_command(
        None,
        account_index=2,
        enabled=False,
        command_id="dashboard-lifecycle-2-disable",
        now_ts=NOW,
    )

    assert command["confirm"] == "CONFIRM-STABLE-LIFECYCLE:2:OFF"
    assert "proposal_generated_at" not in command
    assert validate_stable_lifecycle_command(
        command,
        proposal=None,
        expected_account_index=2,
        expected_account_uid_key="unused",
        expected_host_id="unused",
        now_ts=NOW,
    )["enabled"] is False


def test_enablement_rejects_stale_unsafe_or_cross_host_proposal() -> None:
    stale = _proposal()
    with pytest.raises(StableLifecycleCommandError, match="stale"):
        validate_enablement_proposal(
            stale,
            account_index=1,
            now_ts=NOW + 1_000,
        )

    unsafe = _proposal()
    unsafe["safety"]["runtime_commands"] = True
    with pytest.raises(StableLifecycleCommandError, match="permit side effects"):
        validate_enablement_proposal(
            unsafe,
            account_index=1,
            now_ts=NOW,
        )

    with pytest.raises(StableLifecycleCommandError, match="another runtime host"):
        validate_enablement_proposal(
            _proposal(),
            account_index=1,
            expected_host_id="vm-0-3-ubuntu",
            now_ts=NOW,
        )


def test_delivered_enable_command_rejects_identity_or_field_tampering() -> None:
    command = build_stable_lifecycle_command(
        _proposal(),
        account_index=1,
        enabled=True,
        command_id="dashboard-lifecycle-1-enable",
        now_ts=NOW,
    )
    tampered = copy.deepcopy(command)
    tampered["host_id"] = "vm-0-3-ubuntu"
    with pytest.raises(StableLifecycleCommandError, match="runtime host changed"):
        validate_stable_lifecycle_command(
            tampered,
            proposal=_proposal(),
            expected_account_index=1,
            expected_account_uid_key=ACCOUNT_UID_KEY,
            expected_host_id=HOST_ID,
            now_ts=NOW,
        )

    extra = copy.deepcopy(command)
    extra["config"] = {"markets": []}
    with pytest.raises(StableLifecycleCommandError, match="fields are invalid"):
        validate_stable_lifecycle_command(
            extra,
            proposal=_proposal(),
            expected_account_index=1,
            expected_account_uid_key=ACCOUNT_UID_KEY,
            expected_host_id=HOST_ID,
            now_ts=NOW,
        )


def test_toggle_normalization_cannot_loosen_canary_or_failure_limits() -> None:
    normalized = normalized_lifecycle_config(
        {
            "max_proposal_age_sec": 3_600,
            "max_add_per_cycle": 99,
            "max_active_canaries": 99,
            "canary_principal_fraction": "0.75",
            "canary_max_usdc": "999",
            "promotion_scoring_threshold": 1,
            "soft_failure_threshold": 99,
            "hard_failure_threshold": 99,
            "future_safe_field": "preserved",
        },
        enabled=True,
    )

    assert normalized == {
        "enabled": True,
        "max_proposal_age_sec": 900.0,
        "max_add_per_cycle": 5,
        "max_active_canaries": 10,
        "canary_principal_fraction": "0.1",
        "canary_max_usdc": "100.0",
        "promotion_scoring_threshold": 3,
        "soft_failure_threshold": 3,
        "hard_failure_threshold": 1,
        "future_safe_field": "preserved",
    }
