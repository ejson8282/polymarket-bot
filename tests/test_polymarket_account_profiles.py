from decimal import Decimal

import pytest

from platforms.polymarket.maker.account_profiles import (
    market_event_key,
    parse_lp_account_profile,
    shared_event_owner,
    validate_shared_allocation,
)
from scripts.generate_configs import _render, _validate_roster


@pytest.mark.parametrize(
    ("principal", "pause", "daily_loss"),
    [
        (50, "42.50", "2.50"),
        (100, "85.00", "5.00"),
        (150, "127.50", "7.50"),
        (200, "170.00", "10.00"),
    ],
)
def test_aggressive_profile_derives_guardrails_from_any_principal(
    principal: int,
    pause: str,
    daily_loss: str,
) -> None:
    profile = parse_lp_account_profile(
        {
            "lp_account": {
                "profile_type": "aggressive",
                "target_principal_usdc": principal,
                "allocation_mode": "exclusive",
            }
        },
        3,
    )

    assert profile.managed is True
    assert profile.target_principal_usdc == Decimal(str(principal))
    assert profile.pause_equity_usdc == Decimal(pause)
    assert profile.daily_loss_limit_usdc == Decimal(daily_loss)
    assert profile.sweep_above_usdc == Decimal(str(principal))
    assert profile.public_dict()["guardrails_enforced"] is False


def test_profile_principal_is_not_limited_to_presets() -> None:
    profile = parse_lp_account_profile(
        {
            "lp_account": {
                "profile_type": "aggressive",
                "target_principal_usdc": "75.25",
            }
        },
        4,
    )

    assert profile.target_principal_usdc == Decimal("75.25")
    assert profile.pause_equity_usdc == Decimal("63.96")
    assert profile.daily_loss_limit_usdc == Decimal("3.76")
    assert profile.effective_available(Decimal("150")) == Decimal("75.25")
    assert profile.effective_available(Decimal("40")) == Decimal("40")


def test_missing_profile_preserves_legacy_unmanaged_behavior() -> None:
    profile = parse_lp_account_profile({}, 1)

    assert profile.managed is False
    assert profile.allocation_mode == "disabled"
    assert profile.effective_available(Decimal("123.45")) == Decimal("123.45")


def test_profile_rejects_string_boolean_values() -> None:
    with pytest.raises(ValueError, match="enabled must be a boolean"):
        parse_lp_account_profile(
            {
                "lp_account": {
                    "enabled": "false",
                    "target_principal_usdc": 50,
                }
            },
            1,
        )


@pytest.mark.parametrize("field", ["auto_top_up", "auto_sweep"])
def test_automatic_money_movement_is_rejected(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        parse_lp_account_profile(
            {
                "lp_account": {
                    "profile_type": "aggressive",
                    "target_principal_usdc": 100,
                    field: True,
                }
            },
            1,
        )


def _pair(yes: str, no: str) -> dict[str, dict[str, str]]:
    return {
        yes: {"paired_token_id": no},
        no: {"paired_token_id": yes},
    }


def _profile(index: int, principal: int, group: str = "aggressive"):
    return parse_lp_account_profile(
        {
            "lp_account": {
                "account_id": f"lp_{index}",
                "profile_type": "aggressive",
                "strategy_group": group,
                "target_principal_usdc": principal,
                "allocation_mode": "exclusive",
            }
        },
        index,
    )


def test_market_event_key_is_identical_for_both_sides() -> None:
    assert market_event_key("yes", {"paired_token_id": "no"}) == market_event_key(
        "no", {"paired_token_id": "yes"}
    )


def test_different_strategy_groups_do_not_take_markets_from_each_other() -> None:
    profiles = {1: _profile(1, 50, "aggressive-a"), 2: _profile(2, 200, "aggressive-b")}

    assert shared_event_owner(1, "yes", {"paired_token_id": "no"}, profiles) == 1
    assert shared_event_owner(2, "yes", {"paired_token_id": "no"}, profiles) == 2


def test_shared_event_owner_matches_both_sides_and_is_deterministic() -> None:
    profiles = {1: _profile(1, 50), 2: _profile(2, 200)}

    yes_owner = shared_event_owner(1, "yes", {"paired_token_id": "no"}, profiles)
    no_owner = shared_event_owner(2, "no", {"paired_token_id": "yes"}, profiles)

    assert yes_owner == no_owner
    assert yes_owner in {1, 2}


def test_shared_event_distribution_is_weighted_by_principal() -> None:
    profiles = {1: _profile(1, 50), 2: _profile(2, 200)}
    counts = {1: 0, 2: 0}

    for index in range(1000):
        owner = shared_event_owner(
            1,
            str(index),
            {"condition_id": f"0x{index}"},
            profiles,
        )
        counts[owner] += 1

    assert 150 <= counts[1] <= 250
    assert counts[1] + counts[2] == 1000


def test_disabled_allocation_keeps_existing_duplicate_market_lists() -> None:
    disabled = parse_lp_account_profile(
        {
            "lp_account": {
                "profile_type": "aggressive",
                "target_principal_usdc": 100,
                "allocation_mode": "disabled",
            }
        },
        1,
    )
    profiles = {1: disabled, 2: _profile(2, 100)}

    assert shared_event_owner(1, "yes", {"paired_token_id": "no"}, profiles) == 1


def test_shared_allocation_accepts_matching_market_universes() -> None:
    profiles = {1: _profile(1, 50), 2: _profile(2, 200)}
    markets = {1: _pair("yes", "no"), 2: _pair("yes", "no")}

    validate_shared_allocation(profiles, markets)


def test_shared_allocation_rejects_different_market_universes() -> None:
    profiles = {1: _profile(1, 50), 2: _profile(2, 200)}
    markets = {
        1: _pair("yes", "no"),
        2: {**_pair("yes", "no"), "other": {"condition_id": "0xother"}},
    }

    with pytest.raises(ValueError, match="different market universes"):
        validate_shared_allocation(profiles, markets)


def test_generated_config_copies_non_sensitive_lp_metadata() -> None:
    entry = {
        "funder": "0x" + "1" * 40,
        "clash_port": 7903,
        "lp_account": {
            "account_id": "aggressive_75",
            "profile_type": "aggressive",
            "target_principal_usdc": 75,
            "allocation_mode": "exclusive",
        },
    }

    rendered = _render({"account": {"signer_token": "kept"}}, entry, "127.0.0.1")

    assert rendered["account"]["funder"] == entry["funder"]
    assert rendered["account"]["private_key"] == "REDACTED"
    assert rendered["lp_account"] == entry["lp_account"]
    assert rendered["lp_account"] is not entry["lp_account"]


def test_roster_rejects_non_object_lp_metadata() -> None:
    with pytest.raises(SystemExit, match="lp_account must be an object"):
        _validate_roster(
            [
                {
                    "funder": "0x" + "1" * 40,
                    "clash_port": 7901,
                    "lp_account": 50,
                }
            ]
        )


def test_roster_rejects_invalid_lp_profile_before_writing_configs() -> None:
    with pytest.raises(SystemExit, match="target_principal_usdc"):
        _validate_roster(
            [
                {
                    "funder": "0x" + "1" * 40,
                    "clash_port": 7901,
                    "lp_account": {
                        "profile_type": "aggressive",
                        "target_principal_usdc": 0,
                    },
                }
            ]
        )


def test_roster_rejects_duplicate_lp_account_ids() -> None:
    with pytest.raises(SystemExit, match="used by both account 1 and 2"):
        _validate_roster(
            [
                {
                    "funder": "0x" + "1" * 40,
                    "clash_port": 7901,
                    "lp_account": {
                        "account_id": "aggressive_50",
                        "profile_type": "aggressive",
                        "target_principal_usdc": 50,
                    },
                },
                {
                    "funder": "0x" + "2" * 40,
                    "clash_port": 7902,
                    "lp_account": {
                        "account_id": "AGGRESSIVE_50",
                        "profile_type": "aggressive",
                        "target_principal_usdc": 100,
                    },
                },
            ]
        )
