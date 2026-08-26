from __future__ import annotations

from pathlib import Path

from platforms.polymarket.maker.reward_ledger import (
    canonical_account_uid,
    load_reward_ledger,
    mark_reward_scope_stale,
    merge_reward_ledgers,
    register_account_alias,
    replace_reward_scope,
    reward_ledger_summary,
    save_reward_ledger,
)


DAY = "2026-08-26"
ACCOUNT_UID = canonical_account_uid(137, 2, "0x" + "a" * 40)


def _row(amount: float, *, condition: str = "condition-a") -> dict:
    return {
        "condition_id": condition,
        "asset_address": "asset-a",
        "usd_amount": amount,
    }


def test_scope_replacement_is_idempotent_and_types_stay_separate() -> None:
    ledger = load_reward_ledger(Path("/not-present.json"))
    for reward_type, amount in (
        ("native_lp", 1.0),
        ("sponsored_lp", 2.0),
        ("maker_rebate", 0.3),
        ("trading_pnl", -0.2),
    ):
        replace_reward_scope(
            ledger,
            business_day=DAY,
            account_uid=ACCOUNT_UID,
            reward_type=reward_type,
            records=[_row(amount)],
            observed_at="2026-08-26T01:00:00Z",
            finalized=False,
        )

    replace_reward_scope(
        ledger,
        business_day=DAY,
        account_uid=ACCOUNT_UID,
        reward_type="native_lp",
        records=[_row(1.25)],
        observed_at="2026-08-26T01:05:00Z",
        finalized=False,
    )

    summary = reward_ledger_summary(ledger, DAY)
    assert summary["record_count"] == 4
    assert summary["current_record_count"] == 4
    assert summary["current_usd_by_type"] == {
        "maker_rebate": 0.3,
        "native_lp": 1.25,
        "sponsored_lp": 2.0,
        "trading_pnl": -0.2,
    }
    assert summary["current_total_usd"] == 3.35


def test_stale_scope_is_last_known_but_not_current() -> None:
    ledger = load_reward_ledger(Path("/not-present.json"))
    replace_reward_scope(
        ledger,
        business_day=DAY,
        account_uid=ACCOUNT_UID,
        reward_type="native_lp",
        records=[_row(4.5)],
        observed_at="2026-08-26T01:00:00Z",
        finalized=False,
    )
    mark_reward_scope_stale(
        ledger,
        business_day=DAY,
        account_uid=ACCOUNT_UID,
        reward_type="native_lp",
        observed_at="2026-08-26T01:05:00Z",
        error="TimeoutError",
    )

    summary = reward_ledger_summary(ledger, DAY)
    assert summary["current_usd_by_type"]["native_lp"] == 0.0
    assert summary["last_known_usd_by_type"]["native_lp"] == 4.5
    assert summary["stale_record_count"] == 1


def test_host_ledgers_merge_by_canonical_record_key() -> None:
    first = load_reward_ledger(Path("/not-present-a.json"))
    second = load_reward_ledger(Path("/not-present-b.json"))
    for ledger, host in ((first, "vps1"), (second, "vps2")):
        replace_reward_scope(
            ledger,
            business_day=DAY,
            account_uid=ACCOUNT_UID,
            reward_type="native_lp",
            records=[_row(2.0)],
            observed_at="2026-08-26T01:00:00Z",
            finalized=False,
        )
        register_account_alias(
            ledger,
            account_uid=ACCOUNT_UID,
            account_index=2,
            host=host,
        )
    mark_reward_scope_stale(
        second,
        business_day=DAY,
        account_uid=ACCOUNT_UID,
        reward_type="native_lp",
        observed_at="2026-08-26T01:10:00Z",
        error="signer-unavailable",
    )

    merged = merge_reward_ledgers(first, second)
    summary = reward_ledger_summary(merged, DAY)
    assert summary["record_count"] == 1
    assert summary["current_total_usd"] == 2.0
    assert summary["account_count"] == 1
    assert merged["account_aliases"][ACCOUNT_UID] == [
        {"account_index": 2, "host": "vps1"},
        {"account_index": 2, "host": "vps2"},
    ]


def test_ledger_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "reward_ledger.json"
    ledger = load_reward_ledger(path)
    replace_reward_scope(
        ledger,
        business_day=DAY,
        account_uid=ACCOUNT_UID,
        reward_type="maker_rebate",
        records=[_row(0.75)],
        observed_at="2026-08-26T01:00:00Z",
        finalized=False,
    )
    save_reward_ledger(path, ledger)

    loaded = load_reward_ledger(path)
    assert reward_ledger_summary(loaded, DAY)["current_total_usd"] == 0.75
