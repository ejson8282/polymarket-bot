import pytest

from platforms.polymarket.maker.stable_rotation_commands import (
    StableRotationCommandError,
    build_confirmed_replacement_command,
    confirmation_for_replacement,
    find_confirmed_replacement,
)


NOW = 1_800_000_000.0
REPLACEMENT_ID = "a" * 64


def _proposal() -> dict:
    return {
        "schema_version": 2,
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
                "replace": [
                    {
                        "action": "replace",
                        "replacement_id": REPLACEMENT_ID,
                        "retire": {
                            "token_id": "101",
                            "paired_token_id": "102",
                        },
                        "add": {
                            "token_id": "201",
                            "paired_token_id": "202",
                            "condition_id": "0xabc",
                            "slug": "new-market",
                            "question": "New market?",
                        },
                        "selection": {
                            "primary_metric": "risk_adjusted_daily_roi_pct",
                            "target_config_section": "night_markets",
                        },
                    }
                ],
            }
        ],
    }


def test_build_confirmed_replacement_command_is_account_and_proposal_scoped() -> None:
    command = build_confirmed_replacement_command(
        _proposal(),
        account_index=1,
        replacement_id=REPLACEMENT_ID,
        command_id="dashboard-replace-1",
        now_ts=NOW,
    )

    assert command["action"] == "replace_market"
    assert command["retire_token_id"] == "101"
    assert command["market"]["token_id"] == "201"
    assert command["account_index"] == 1
    assert command["target_config_section"] == "night_markets"
    assert command["proposal_generated_at"] == NOW - 30
    assert command["confirm"] == confirmation_for_replacement(REPLACEMENT_ID)


def test_find_replacement_rejects_stale_or_unsafe_proposal() -> None:
    with pytest.raises(StableRotationCommandError, match="stale"):
        find_confirmed_replacement(
            _proposal(),
            account_index=1,
            replacement_id=REPLACEMENT_ID,
            now_ts=NOW + 1_000,
        )

    unsafe = _proposal()
    unsafe["safety"]["runtime_commands"] = True
    with pytest.raises(StableRotationCommandError, match="permit side effects"):
        find_confirmed_replacement(
            unsafe,
            account_index=1,
            replacement_id=REPLACEMENT_ID,
            now_ts=NOW,
        )


def test_find_replacement_rejects_wrong_account_or_id() -> None:
    with pytest.raises(StableRotationCommandError, match="account is not"):
        find_confirmed_replacement(
            _proposal(),
            account_index=2,
            replacement_id=REPLACEMENT_ID,
            now_ts=NOW,
        )
    with pytest.raises(StableRotationCommandError, match="sha256"):
        confirmation_for_replacement("short")
