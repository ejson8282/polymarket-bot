"""Non-secret host routing for the Polymarket multi-account runtime.

The roster is deliberately separate from per-account runtime configs. It may
contain public funder addresses, host placement, proxy ports, and LP profile
metadata, but never signer tokens or private credentials.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from .account_profiles import LPAccountProfile, parse_lp_account_profile
except ImportError:  # pragma: no cover - direct script execution
    from account_profiles import LPAccountProfile, parse_lp_account_profile


MAX_ACCOUNTS = 30
_HOST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_RUNTIME_SCOPE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")
_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_SECRET_FIELDS = frozenset(
    {
        "authorization",
        "api_key",
        "api_passphrase",
        "api_secret",
        "cookie",
        "cookies",
        "mnemonic",
        "password",
        "private_key",
        "secret",
        "signer_token",
        "webhook",
        "webhook_url",
    }
)
_ACCOUNT_FIELDS = frozenset(
    {
        "account_index",
        "host_id",
        "enabled",
        "funder",
        "clash_port",
        "lp_account",
    }
)


@dataclass(frozen=True)
class RuntimeAccount:
    account_index: int
    host_id: str
    enabled: bool
    funder: str
    clash_port: int
    profile: LPAccountProfile
    has_lp_account: bool

    def generation_entry(self) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "account_index": self.account_index,
            "host_id": self.host_id,
            "enabled": self.enabled,
            "funder": self.funder,
            "clash_port": self.clash_port,
        }
        if self.has_lp_account:
            entry["lp_account"] = {
                "account_id": self.profile.account_id,
                "enabled": self.profile.enabled,
                "profile_type": self.profile.profile_type,
                "strategy_group": self.profile.strategy_group,
                "target_principal_usdc": str(self.profile.target_principal_usdc),
                "pause_equity_usdc": str(self.profile.pause_equity_usdc),
                "daily_loss_limit_usdc": str(self.profile.daily_loss_limit_usdc),
                "allocation_mode": self.profile.allocation_mode,
                "auto_top_up": self.profile.auto_top_up,
                "auto_sweep": self.profile.auto_sweep,
            }
        return entry

    def routing_dict(self) -> dict[str, Any]:
        return {
            "account_index": self.account_index,
            "host_id": self.host_id,
            "enabled": self.enabled,
            "funder": self.funder.lower(),
            "clash_port": self.clash_port,
            "account_id": self.profile.account_id,
            "profile_enabled": self.profile.enabled,
            "profile_type": self.profile.profile_type,
            "strategy_group": self.profile.strategy_group,
            "target_principal_usdc": str(self.profile.target_principal_usdc),
            "pause_equity_usdc": str(self.profile.pause_equity_usdc),
            "daily_loss_limit_usdc": str(self.profile.daily_loss_limit_usdc),
            "allocation_mode": self.profile.allocation_mode,
            "auto_top_up": self.profile.auto_top_up,
            "auto_sweep": self.profile.auto_sweep,
        }


def _find_secret_field(value: object, path: str = "roster") -> str | None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            child_path = f"{path}.{key_text}"
            if key_text.casefold() in _SECRET_FIELDS:
                return child_path
            found = _find_secret_field(child, child_path)
            if found:
                return found
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, child in enumerate(value):
            found = _find_secret_field(child, f"{path}[{index}]")
            if found:
                return found
    return None


def _account_rows(raw: object) -> list[object]:
    if isinstance(raw, Mapping):
        unknown = set(raw) - {"schema_version", "runtime_scope", "accounts"}
        if unknown:
            raise ValueError(
                "roster object has unsupported fields: " + ", ".join(sorted(map(str, unknown)))
            )
        if raw.get("schema_version", 1) != 1:
            raise ValueError("roster.schema_version must be 1")
        raw = raw.get("accounts")
    if not isinstance(raw, list) or not raw:
        raise ValueError("roster must be a non-empty account array")
    if len(raw) > MAX_ACCOUNTS:
        raise ValueError(f"roster has {len(raw)} accounts; maximum is {MAX_ACCOUNTS}")
    return list(raw)


def runtime_roster_scope(raw: object) -> str:
    """Return the optional isolation scope declared by a roster."""
    if not isinstance(raw, Mapping):
        return ""
    scope = str(raw.get("runtime_scope") or "").strip().lower()
    if scope and not _RUNTIME_SCOPE_RE.fullmatch(scope):
        raise ValueError(f"roster.runtime_scope is invalid: {scope!r}")
    return scope


def parse_runtime_roster(
    raw: object,
    *,
    default_host_id: str = "local",
) -> tuple[RuntimeAccount, ...]:
    runtime_roster_scope(raw)
    secret_path = _find_secret_field(raw)
    if secret_path:
        raise ValueError(f"non-secret roster must not contain {secret_path}")

    rows = _account_rows(raw)
    accounts: list[RuntimeAccount] = []
    indexes: set[int] = set()
    funders: dict[str, int] = {}
    account_ids: dict[str, int] = {}
    ports: dict[tuple[str, int], int] = {}

    for ordinal, row in enumerate(rows, start=1):
        if not isinstance(row, Mapping):
            raise ValueError(f"roster entry {ordinal} must be an object")
        unknown = set(row) - _ACCOUNT_FIELDS
        if unknown:
            raise ValueError(
                f"roster entry {ordinal} has unsupported fields: "
                + ", ".join(sorted(map(str, unknown)))
            )

        account_index = row.get("account_index", ordinal)
        if isinstance(account_index, bool) or not isinstance(account_index, int):
            raise ValueError(f"roster entry {ordinal}: account_index must be an integer")
        if not 1 <= account_index <= MAX_ACCOUNTS:
            raise ValueError(
                f"roster entry {ordinal}: account_index must be between 1 and {MAX_ACCOUNTS}"
            )
        if account_index in indexes:
            raise ValueError(f"duplicate account_index {account_index}")

        host_id = str(row.get("host_id") or default_host_id).strip().lower()
        if not _HOST_ID_RE.fullmatch(host_id):
            raise ValueError(f"roster entry {ordinal}: invalid host_id {host_id!r}")

        enabled = row.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"roster entry {ordinal}: enabled must be a boolean")

        funder = str(row.get("funder") or "").strip()
        if not _ADDRESS_RE.fullmatch(funder):
            raise ValueError(
                f"roster entry {ordinal}: funder must be a 0x-prefixed 20-byte address"
            )
        funder_key = funder.casefold()
        if funder_key in funders:
            raise ValueError(
                f"funder {funder} used by both account "
                f"{funders[funder_key]} and {account_index}"
            )

        clash_port = row.get("clash_port")
        if isinstance(clash_port, bool) or not isinstance(clash_port, int):
            raise ValueError(f"roster entry {ordinal}: clash_port must be an integer")
        if not 1024 <= clash_port <= 65535:
            raise ValueError(f"roster entry {ordinal}: clash_port is out of range")
        port_key = (host_id, clash_port)
        if port_key in ports:
            raise ValueError(
                f"clash_port {clash_port} on {host_id} is used by account "
                f"{ports[port_key]} and {account_index}"
            )

        lp_raw = row.get("lp_account")
        if lp_raw is not None and not isinstance(lp_raw, Mapping):
            raise ValueError(f"roster entry {ordinal}: lp_account must be an object")
        profile = parse_lp_account_profile(
            {"lp_account": dict(lp_raw)} if lp_raw is not None else {},
            account_index,
        )
        account_key = profile.account_id.casefold()
        if account_key in account_ids:
            raise ValueError(
                f"lp_account.account_id {profile.account_id!r} used by both account "
                f"{account_ids[account_key]} and {account_index}"
            )

        indexes.add(account_index)
        funders[funder_key] = account_index
        account_ids[account_key] = account_index
        ports[port_key] = account_index
        accounts.append(
            RuntimeAccount(
                account_index=account_index,
                host_id=host_id,
                enabled=enabled,
                funder=funder,
                clash_port=clash_port,
                profile=profile,
                has_lp_account=lp_raw is not None,
            )
        )

    return tuple(sorted(accounts, key=lambda item: item.account_index))


def load_runtime_roster(
    path: Path,
    *,
    default_host_id: str = "local",
) -> tuple[RuntimeAccount, ...]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"roster file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"roster is not valid JSON: {path}: {exc}") from exc
    return parse_runtime_roster(raw, default_host_id=default_host_id)


def load_runtime_roster_scope(path: Path) -> str:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"roster file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"roster is not valid JSON: {path}: {exc}") from exc
    return runtime_roster_scope(raw)


def roster_hosts(accounts: Sequence[RuntimeAccount]) -> tuple[str, ...]:
    return tuple(sorted({account.host_id for account in accounts if account.enabled}))


def local_runtime_accounts(
    accounts: Sequence[RuntimeAccount],
    host_id: str,
) -> tuple[RuntimeAccount, ...]:
    normalized = host_id.strip().lower()
    return tuple(
        account
        for account in accounts
        if account.enabled and account.host_id == normalized
    )


def routing_profiles(
    accounts: Sequence[RuntimeAccount],
) -> dict[int, LPAccountProfile]:
    return {
        account.account_index: account.profile
        for account in accounts
        if account.enabled
    }


def routing_roster_sha256(
    accounts: Sequence[RuntimeAccount],
    runtime_scope: str = "",
) -> str:
    scope = str(runtime_scope or "").strip().lower()
    if scope and not _RUNTIME_SCOPE_RE.fullmatch(scope):
        raise ValueError(f"runtime scope is invalid: {scope!r}")
    rows = [
        account.routing_dict()
        for account in sorted(accounts, key=lambda item: item.account_index)
    ]
    payload: object = {"runtime_scope": scope, "accounts": rows} if scope else rows
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def market_universe_sha256(config: Mapping[str, Any]) -> str:
    """Hash the complete day/night market inputs, independent of list order."""
    payload: dict[str, list[Mapping[str, Any]]] = {}
    for section in ("markets", "night_markets"):
        raw_rows = config.get(section, [])
        if raw_rows is None:
            raw_rows = []
        if not isinstance(raw_rows, list):
            raise ValueError(f"config.{section} must be an array")
        rows: list[Mapping[str, Any]] = []
        for ordinal, row in enumerate(raw_rows, start=1):
            if not isinstance(row, Mapping):
                raise ValueError(f"config.{section}[{ordinal}] must be an object")
            rows.append(dict(row))
        try:
            payload[section] = sorted(
                rows,
                key=lambda row: json.dumps(
                    row,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                ),
            )
            encoded = json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("market configuration must be JSON serializable") from exc
    return hashlib.sha256(encoded).hexdigest()
