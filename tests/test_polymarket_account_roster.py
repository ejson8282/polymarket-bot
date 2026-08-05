from decimal import Decimal
import json
from pathlib import Path

import pytest

from platforms.polymarket.maker.account_roster import (
    local_runtime_accounts,
    market_universe_sha256,
    parse_runtime_roster,
    roster_hosts,
    routing_profiles,
    routing_roster_sha256,
    runtime_roster_scope,
)


def _account(index: int, host_id: str, port: int, principal: int = 100) -> dict:
    return {
        "account_index": index,
        "host_id": host_id,
        "funder": "0x" + f"{index:040x}",
        "clash_port": port,
        "lp_account": {
            "account_id": f"aggressive_{index}",
            "profile_type": "aggressive",
            "strategy_group": "aggressive",
            "target_principal_usdc": principal,
            "allocation_mode": "exclusive",
        },
    }


def test_roster_routes_global_indexes_to_each_host() -> None:
    accounts = parse_runtime_roster(
        {
            "schema_version": 1,
            "accounts": [
                _account(6, "VPS2", 7901, 50),
                _account(1, "VPS1", 7901, 200),
                _account(2, "VPS1", 7902, 100),
            ],
        }
    )

    assert [account.account_index for account in accounts] == [1, 2, 6]
    assert roster_hosts(accounts) == ("vps1", "vps2")
    assert [account.account_index for account in local_runtime_accounts(accounts, "VPS1")] == [1, 2]
    assert set(routing_profiles(accounts)) == {1, 2, 6}
    assert routing_profiles(accounts)[1].target_principal_usdc == Decimal("200.00")


def test_roster_digest_is_stable_across_input_order() -> None:
    first = parse_runtime_roster([_account(1, "vps1", 7901), _account(6, "vps2", 7901)])
    second = parse_runtime_roster([_account(6, "vps2", 7901), _account(1, "vps1", 7901)])

    assert routing_roster_sha256(first) == routing_roster_sha256(second)


def test_runtime_scope_is_validated_and_changes_roster_digest() -> None:
    payload = {
        "schema_version": 1,
        "runtime_scope": "aggressive",
        "accounts": [_account(1, "aggressive-a", 7901)],
    }
    accounts = parse_runtime_roster(payload)

    assert runtime_roster_scope(payload) == "aggressive"
    assert routing_roster_sha256(accounts, "aggressive") != routing_roster_sha256(
        accounts
    )

    with pytest.raises(ValueError, match="runtime_scope is invalid"):
        parse_runtime_roster({**payload, "runtime_scope": "../normal"})


def test_roster_digest_changes_for_runtime_sensitive_fields() -> None:
    base = _account(1, "vps1", 7901)
    baseline = routing_roster_sha256(parse_runtime_roster([base]))

    changed_funder = _account(1, "vps1", 7901)
    changed_funder["funder"] = "0x" + "f" * 40
    changed_port = _account(1, "vps1", 7902)
    changed_loss = _account(1, "vps1", 7901)
    changed_loss["lp_account"]["daily_loss_limit_usdc"] = 7

    assert routing_roster_sha256(parse_runtime_roster([changed_funder])) != baseline
    assert routing_roster_sha256(parse_runtime_roster([changed_port])) != baseline
    assert routing_roster_sha256(parse_runtime_roster([changed_loss])) != baseline


def test_market_universe_digest_is_order_independent_and_change_sensitive() -> None:
    first = {
        "markets": [
            {"token_id": "2", "paired_token_id": "20", "enabled": True},
            {"token_id": "1", "paired_token_id": "10", "enabled": True},
        ],
        "night_markets": [{"token_id": "3", "risk": "low"}],
    }
    second = {
        "markets": list(reversed(first["markets"])),
        "night_markets": list(first["night_markets"]),
    }
    changed = json.loads(json.dumps(first))
    changed["markets"][0]["enabled"] = False

    assert market_universe_sha256(first) == market_universe_sha256(second)
    assert market_universe_sha256(first) != market_universe_sha256(changed)


def test_same_proxy_port_is_allowed_on_different_hosts() -> None:
    accounts = parse_runtime_roster(
        [_account(1, "vps1", 7901), _account(6, "vps2", 7901)]
    )

    assert len(accounts) == 2


@pytest.mark.parametrize(
    "secret_field",
    ["private_key", "signer_token", "api_key", "password", "webhook_url"],
)
def test_roster_rejects_secrets_at_any_depth(secret_field: str) -> None:
    row = _account(1, "vps1", 7901)
    row["metadata"] = {secret_field: "must-not-be-here"}

    with pytest.raises(ValueError, match="must not contain"):
        parse_runtime_roster([row])


def test_roster_rejects_unknown_fields_instead_of_ignoring_typos() -> None:
    row = _account(1, "vps1", 7901)
    row["hots_id"] = "vps2"

    with pytest.raises(ValueError, match="unsupported fields"):
        parse_runtime_roster([row])


def test_disabled_account_is_excluded_from_runtime_routing() -> None:
    enabled = _account(1, "vps1", 7901)
    disabled = _account(2, "vps2", 7901)
    disabled["enabled"] = False
    accounts = parse_runtime_roster([enabled, disabled])

    assert roster_hosts(accounts) == ("vps1",)
    assert set(routing_profiles(accounts)) == {1}


def test_example_preserves_existing_account_host_ownership() -> None:
    root = Path(__file__).resolve().parents[1]
    payload = json.loads(
        (root / "scripts" / "accounts.example.json").read_text(encoding="utf-8")
    )
    accounts = parse_runtime_roster(payload)
    by_index = {account.account_index: account for account in accounts}

    assert by_index[1].host_id == "vps1"
    assert by_index[2].host_id == "vps2"
    assert len(local_runtime_accounts(accounts, "vps1")) == 5
    assert len(local_runtime_accounts(accounts, "vps2")) == 5


def test_aggressive_example_is_a_separate_runtime_domain() -> None:
    root = Path(__file__).resolve().parents[1]
    payload = json.loads(
        (root / "scripts" / "accounts.aggressive.example.json").read_text(
            encoding="utf-8"
        )
    )
    accounts = parse_runtime_roster(payload)

    assert runtime_roster_scope(payload) == "aggressive"
    assert roster_hosts(accounts) == ("aggressive-a", "aggressive-b")
    assert len(local_runtime_accounts(accounts, "aggressive-a")) == 5
    assert len(local_runtime_accounts(accounts, "aggressive-b")) == 5
    assert all(account.profile.profile_type == "aggressive" for account in accounts)
