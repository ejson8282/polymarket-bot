"""Canonical, idempotent Polymarket income records.

The ledger key is independent of host and local config index so copying an
account cache between VPS hosts cannot double count the same maker income.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


SCHEMA_VERSION = 1
REWARD_TYPES = {
    "native_lp",
    "sponsored_lp",
    "maker_rebate",
    "trading_pnl",
}


def _empty_ledger() -> dict[str, Any]:
    return {
        "version": SCHEMA_VERSION,
        "updated_at": None,
        "records": {},
        "scopes": {},
        "account_aliases": {},
    }


def _address(value: Any) -> str:
    text = str(value or "").strip().lower()
    if (
        len(text) == 42
        and text.startswith("0x")
        and all(ch in "0123456789abcdef" for ch in text[2:])
    ):
        return text
    return ""


def canonical_account_uid(
    chain_id: Any,
    signature_type: Any,
    maker_address: Any,
) -> str:
    maker = _address(maker_address)
    if not maker:
        raise ValueError("valid maker address required")
    return f"{int(chain_id)}:{int(signature_type)}:{maker}"


def ledger_record_key(record: Mapping[str, Any]) -> str:
    values = (
        str(record.get("business_day") or "").strip(),
        str(record.get("account_uid") or "").strip(),
        str(record.get("condition_id") or "").strip().lower(),
        str(record.get("reward_type") or "").strip(),
        str(record.get("asset_address") or "").strip().lower(),
    )
    if not all(values):
        raise ValueError("ledger record is missing an identity field")
    if values[3] not in REWARD_TYPES:
        raise ValueError(f"unsupported reward type: {values[3]}")
    return "|".join(values)


def load_reward_ledger(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}
    records = payload.get("records")
    scopes = payload.get("scopes")
    aliases = payload.get("account_aliases")
    ledger = _empty_ledger()
    ledger.update(
        {
            "updated_at": payload.get("updated_at"),
            "records": records if isinstance(records, dict) else {},
            "scopes": scopes if isinstance(scopes, dict) else {},
            "account_aliases": aliases if isinstance(aliases, dict) else {},
        }
    )
    return ledger


def save_reward_ledger(path: Path, ledger: Mapping[str, Any]) -> None:
    payload = dict(ledger)
    payload["version"] = SCHEMA_VERSION
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


def _scope_key(business_day: str, account_uid: str, reward_type: str) -> str:
    return f"{business_day}|{account_uid}|{reward_type}"


def replace_reward_scope(
    ledger: dict[str, Any],
    *,
    business_day: str,
    account_uid: str,
    reward_type: str,
    records: Iterable[Mapping[str, Any]],
    observed_at: str,
    finalized: bool,
) -> None:
    """Atomically replace one account/day/type scope after a successful read."""
    if reward_type not in REWARD_TYPES:
        raise ValueError(f"unsupported reward type: {reward_type}")
    record_map = ledger.setdefault("records", {})
    if not isinstance(record_map, dict):
        record_map = {}
        ledger["records"] = record_map
    scope_key = _scope_key(business_day, account_uid, reward_type)
    prefix = f"{business_day}|{account_uid}|"
    stale_keys = [
        key
        for key, row in record_map.items()
        if key.startswith(prefix)
        and isinstance(row, Mapping)
        and row.get("reward_type") == reward_type
    ]
    for key in stale_keys:
        record_map.pop(key, None)

    for source in records:
        row = dict(source)
        row.update(
            {
                "business_day": business_day,
                "account_uid": account_uid,
                "reward_type": reward_type,
                "observed_at": observed_at,
                "finalized": bool(finalized),
                "fresh": True,
            }
        )
        key = ledger_record_key(row)
        row["record_key"] = key
        record_map[key] = row

    scopes = ledger.setdefault("scopes", {})
    scopes[scope_key] = {
        "business_day": business_day,
        "account_uid": account_uid,
        "reward_type": reward_type,
        "status": "finalized" if finalized else "current",
        "fresh": True,
        "observed_at": observed_at,
        "error": None,
    }


def mark_reward_scope_stale(
    ledger: dict[str, Any],
    *,
    business_day: str,
    account_uid: str,
    reward_type: str,
    observed_at: str,
    error: str,
) -> None:
    """Retain last-known rows while making them ineligible for current totals."""
    scope_key = _scope_key(business_day, account_uid, reward_type)
    records = ledger.setdefault("records", {})
    if isinstance(records, dict):
        for row in records.values():
            if (
                isinstance(row, dict)
                and row.get("business_day") == business_day
                and row.get("account_uid") == account_uid
                and row.get("reward_type") == reward_type
            ):
                row["fresh"] = False
    ledger.setdefault("scopes", {})[scope_key] = {
        "business_day": business_day,
        "account_uid": account_uid,
        "reward_type": reward_type,
        "status": "stale",
        "fresh": False,
        "observed_at": observed_at,
        "error": str(error),
    }


def register_account_alias(
    ledger: dict[str, Any],
    *,
    account_uid: str,
    account_index: int,
    host: Optional[str] = None,
) -> None:
    aliases = ledger.setdefault("account_aliases", {})
    current = aliases.setdefault(account_uid, [])
    if not isinstance(current, list):
        current = []
        aliases[account_uid] = current
    alias = {
        "account_index": int(account_index),
        "host": str(host or "local"),
    }
    if alias not in current:
        current.append(alias)
        current.sort(key=lambda item: (item.get("host", ""), item["account_index"]))


def merge_reward_ledgers(*sources: Mapping[str, Any]) -> dict[str, Any]:
    """Merge host ledgers without double counting a canonical record.

    A fresh observation wins over a stale cache even when the stale cache was
    written later. Among observations with the same freshness, the newest
    timestamp wins. This lets either VPS keep a shared maker current without a
    transient outage on the other host poisoning the aggregate.
    """
    merged = _empty_ledger()

    def rank(row: Mapping[str, Any]) -> tuple[int, str]:
        return (
            1 if row.get("fresh") is True else 0,
            str(row.get("observed_at") or ""),
        )

    for source in sources:
        if not isinstance(source, Mapping):
            continue
        merged["updated_at"] = max(
            str(merged.get("updated_at") or ""),
            str(source.get("updated_at") or ""),
        ) or None
        records = source.get("records")
        if isinstance(records, Mapping):
            for key, raw in records.items():
                if not isinstance(raw, Mapping):
                    continue
                try:
                    canonical_key = ledger_record_key(raw)
                except ValueError:
                    continue
                existing = merged["records"].get(canonical_key)
                if not isinstance(existing, Mapping) or rank(raw) > rank(existing):
                    row = dict(raw)
                    row["record_key"] = canonical_key
                    merged["records"][canonical_key] = row
        scopes = source.get("scopes")
        if isinstance(scopes, Mapping):
            for key, raw in scopes.items():
                if not isinstance(raw, Mapping):
                    continue
                existing = merged["scopes"].get(str(key))
                if not isinstance(existing, Mapping) or rank(raw) > rank(existing):
                    merged["scopes"][str(key)] = dict(raw)
        aliases = source.get("account_aliases")
        if isinstance(aliases, Mapping):
            for account_uid, raw_aliases in aliases.items():
                if not isinstance(raw_aliases, list):
                    continue
                current = merged["account_aliases"].setdefault(str(account_uid), [])
                for alias in raw_aliases:
                    if isinstance(alias, Mapping):
                        normalized = dict(alias)
                        if normalized not in current:
                            current.append(normalized)
                current.sort(
                    key=lambda item: (
                        str(item.get("host") or ""),
                        int(item.get("account_index") or 0),
                    )
                )
    return merged


def reward_ledger_summary(ledger: Mapping[str, Any], business_day: str) -> dict:
    records = ledger.get("records")
    if not isinstance(records, Mapping):
        records = {}
    current: dict[str, float] = defaultdict(float)
    last_known: dict[str, float] = defaultdict(float)
    condition_ids: set[str] = set()
    account_uids: set[str] = set()
    stale_records = 0
    current_records = 0
    for row in records.values():
        if not isinstance(row, Mapping) or row.get("business_day") != business_day:
            continue
        reward_type = str(row.get("reward_type") or "")
        try:
            usd_amount = float(row.get("usd_amount") or 0.0)
        except (TypeError, ValueError):
            continue
        last_known[reward_type] += usd_amount
        if row.get("fresh") is True:
            current[reward_type] += usd_amount
            current_records += 1
        else:
            stale_records += 1
        condition_ids.add(str(row.get("condition_id") or "").lower())
        account_uids.add(str(row.get("account_uid") or ""))
    types = sorted(REWARD_TYPES)
    return {
        "business_day": business_day,
        "current_usd_by_type": {
            key: round(current.get(key, 0.0), 6) for key in types
        },
        "last_known_usd_by_type": {
            key: round(last_known.get(key, 0.0), 6) for key in types
        },
        "current_total_usd": round(sum(current.values()), 6),
        "last_known_total_usd": round(sum(last_known.values()), 6),
        "record_count": sum(
            1
            for row in records.values()
            if isinstance(row, Mapping) and row.get("business_day") == business_day
        ),
        "current_record_count": current_records,
        "stale_record_count": stale_records,
        "condition_count": len(condition_ids - {""}),
        "account_count": len(account_uids - {""}),
    }
