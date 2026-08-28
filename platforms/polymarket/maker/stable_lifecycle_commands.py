"""Validated runtime command contract for stable-LP lifecycle control."""

from __future__ import annotations

import hashlib
import math
import re
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

DEFAULT_MAX_PROPOSAL_AGE_SEC = 900.0
DEFAULT_MAX_ADD_PER_CYCLE = 5
DEFAULT_SOFT_FAILURE_THRESHOLD = 3
DEFAULT_HARD_FAILURE_THRESHOLD = 1
MAX_ACTIVE_CANARIES_LIMIT = 10
MAX_CANARY_PRINCIPAL_FRACTION = 0.10
MAX_CANARY_USDC = 100.0
MIN_PROMOTION_SCORING_SAMPLES = 3
CONFIRMATION_PREFIX = "CONFIRM-STABLE-LIFECYCLE"


class StableLifecycleCommandError(ValueError):
    """A lifecycle command is malformed, stale, or targets another engine."""


def _finite_float(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise StableLifecycleCommandError(f"{field} is invalid") from exc
    if not math.isfinite(number):
        raise StableLifecycleCommandError(f"{field} is invalid")
    return number


def _bounded_int(value: Any, *, default: int, low: int, high: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    return max(low, min(high, number))


def _bounded_decimal(
    value: Any,
    *,
    default: str,
    low: Decimal,
    high: Decimal,
) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        number = Decimal(default)
    if not number.is_finite():
        number = Decimal(default)
    return max(low, min(high, number))


def normalized_lifecycle_config(
    current: Any,
    *,
    enabled: bool,
) -> dict[str, Any]:
    """Return a conservative config without allowing the toggle to loosen caps."""

    config = dict(current) if isinstance(current, Mapping) else {}
    max_age = _finite_float(
        config.get("max_proposal_age_sec", DEFAULT_MAX_PROPOSAL_AGE_SEC),
        field="max_proposal_age_sec",
    )
    config.update(
        {
            "enabled": bool(enabled),
            "max_proposal_age_sec": max(
                60.0,
                min(DEFAULT_MAX_PROPOSAL_AGE_SEC, max_age),
            ),
            "max_add_per_cycle": _bounded_int(
                config.get("max_add_per_cycle"),
                default=DEFAULT_MAX_ADD_PER_CYCLE,
                low=0,
                high=DEFAULT_MAX_ADD_PER_CYCLE,
            ),
            "max_active_canaries": _bounded_int(
                config.get("max_active_canaries"),
                default=MAX_ACTIVE_CANARIES_LIMIT,
                low=0,
                high=MAX_ACTIVE_CANARIES_LIMIT,
            ),
            "canary_principal_fraction": str(
                _bounded_decimal(
                    config.get("canary_principal_fraction"),
                    default=str(MAX_CANARY_PRINCIPAL_FRACTION),
                    low=Decimal("0"),
                    high=Decimal(str(MAX_CANARY_PRINCIPAL_FRACTION)),
                )
            ),
            "canary_max_usdc": str(
                _bounded_decimal(
                    config.get("canary_max_usdc"),
                    default=str(MAX_CANARY_USDC),
                    low=Decimal("0"),
                    high=Decimal(str(MAX_CANARY_USDC)),
                )
            ),
            "promotion_scoring_threshold": max(
                MIN_PROMOTION_SCORING_SAMPLES,
                _bounded_int(
                    config.get("promotion_scoring_threshold"),
                    default=MIN_PROMOTION_SCORING_SAMPLES,
                    low=MIN_PROMOTION_SCORING_SAMPLES,
                    high=1000,
                ),
            ),
            "soft_failure_threshold": _bounded_int(
                config.get("soft_failure_threshold"),
                default=DEFAULT_SOFT_FAILURE_THRESHOLD,
                low=1,
                high=DEFAULT_SOFT_FAILURE_THRESHOLD,
            ),
            "hard_failure_threshold": _bounded_int(
                config.get("hard_failure_threshold"),
                default=DEFAULT_HARD_FAILURE_THRESHOLD,
                low=1,
                high=DEFAULT_HARD_FAILURE_THRESHOLD,
            ),
        }
    )
    return config


def _proposal_account(
    proposal: Mapping[str, Any],
    *,
    account_index: int,
    now_ts: float,
    max_proposal_age_sec: float,
) -> Mapping[str, Any]:
    if int(proposal.get("schema_version") or 0) < 3:
        raise StableLifecycleCommandError("stable lifecycle proposal schema is too old")
    if proposal.get("mode") != "proposal_only" or proposal.get("status") != "ready":
        raise StableLifecycleCommandError("stable lifecycle proposal is not ready")
    safety = proposal.get("safety")
    if not isinstance(safety, Mapping) or safety.get("proposal_only") is not True:
        raise StableLifecycleCommandError("proposal-only safety marker is missing")
    if safety.get("requires_manual_review") is not True:
        raise StableLifecycleCommandError("proposal review marker is missing")
    if any(
        safety.get(key) is not False
        for key in ("trading_actions", "runtime_commands", "runtime_config_writes")
    ):
        raise StableLifecycleCommandError("proposal safety flags permit side effects")

    generated_at = _finite_float(
        proposal.get("generated_at"),
        field="proposal generated_at",
    )
    age = now_ts - generated_at
    if generated_at <= 0 or age < -30 or age > max_proposal_age_sec:
        raise StableLifecycleCommandError("stable lifecycle proposal is stale")
    accounts = proposal.get("accounts")
    if not isinstance(accounts, list):
        raise StableLifecycleCommandError("proposal accounts are invalid")
    for account in accounts:
        if not isinstance(account, Mapping):
            continue
        if int(account.get("account_index") or 0) == account_index:
            return account
    raise StableLifecycleCommandError("account is not in the lifecycle proposal")


def validate_enablement_proposal(
    proposal: Mapping[str, Any],
    *,
    account_index: int,
    expected_account_uid_key: str = "",
    expected_host_id: str = "",
    now_ts: float | None = None,
    max_proposal_age_sec: float = DEFAULT_MAX_PROPOSAL_AGE_SEC,
) -> dict[str, Any]:
    """Validate one fresh host-local proposal account and return its identity."""

    if not isinstance(proposal, Mapping):
        raise StableLifecycleCommandError("stable lifecycle proposal must be an object")
    idx = int(account_index)
    if idx < 1 or idx > 30:
        raise StableLifecycleCommandError("account_index is invalid")
    now = time.time() if now_ts is None else float(now_ts)
    account = _proposal_account(
        proposal,
        account_index=idx,
        now_ts=now,
        max_proposal_age_sec=max(60.0, float(max_proposal_age_sec)),
    )
    account_uid_key = str(account.get("account_uid_key") or "").strip().lower()
    host_id = str(account.get("host_id") or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{16}", account_uid_key):
        raise StableLifecycleCommandError("proposal account identity is invalid")
    if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", host_id):
        raise StableLifecycleCommandError("proposal host identity is invalid")
    if expected_account_uid_key and account_uid_key != str(
        expected_account_uid_key
    ).strip().lower():
        raise StableLifecycleCommandError("proposal targets another account identity")
    if expected_host_id and host_id != str(expected_host_id).strip().lower():
        raise StableLifecycleCommandError("proposal targets another runtime host")
    return {
        "account_index": idx,
        "account_uid_key": account_uid_key,
        "host_id": host_id,
        "proposal_generated_at": float(proposal["generated_at"]),
    }


def _proposal_binding(identity: Mapping[str, Any]) -> str:
    material = (
        f"{int(identity['account_index'])}|"
        f"{float(identity['proposal_generated_at']):.6f}|"
        f"{identity['account_uid_key']}|{identity['host_id']}"
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def confirmation_for_change(
    account_index: int,
    enabled: bool,
    *,
    identity: Mapping[str, Any] | None = None,
) -> str:
    idx = int(account_index)
    if idx < 1 or idx > 30:
        raise StableLifecycleCommandError("account_index is invalid")
    if not enabled:
        return f"{CONFIRMATION_PREFIX}:{idx}:OFF"
    if not isinstance(identity, Mapping):
        raise StableLifecycleCommandError("enablement identity is required")
    return f"{CONFIRMATION_PREFIX}:{idx}:ON:{_proposal_binding(identity)}"


def build_stable_lifecycle_command(
    proposal: Mapping[str, Any] | None,
    *,
    account_index: int,
    enabled: bool,
    command_id: str,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Build the minimal account-scoped command accepted by the engine."""

    now = time.time() if now_ts is None else float(now_ts)
    command_id = str(command_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", command_id):
        raise StableLifecycleCommandError("command_id is invalid")
    idx = int(account_index)
    command: dict[str, Any] = {
        "version": 1,
        "command_id": command_id,
        "action": "set_stable_market_lifecycle",
        "created_at": now,
        "account_index": idx,
        "enabled": bool(enabled),
    }
    identity = None
    if enabled:
        identity = validate_enablement_proposal(
            proposal or {},
            account_index=idx,
            now_ts=now,
        )
        command.update(identity)
    command["confirm"] = confirmation_for_change(
        idx,
        bool(enabled),
        identity=identity,
    )
    return command


def validate_stable_lifecycle_command(
    command: Mapping[str, Any],
    *,
    proposal: Mapping[str, Any] | None,
    expected_account_index: int,
    expected_account_uid_key: str,
    expected_host_id: str,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Validate a delivered command against the target engine and proposal."""

    if not isinstance(command, Mapping):
        raise StableLifecycleCommandError("stable lifecycle command must be an object")
    enabled = command.get("enabled")
    if not isinstance(enabled, bool):
        raise StableLifecycleCommandError("enabled must be boolean")
    expected_keys = {
        "version",
        "command_id",
        "action",
        "created_at",
        "account_index",
        "enabled",
        "confirm",
    }
    if enabled:
        expected_keys.update(
            {
                "proposal_generated_at",
                "account_uid_key",
                "host_id",
            }
        )
    if set(command) != expected_keys:
        raise StableLifecycleCommandError("stable lifecycle command fields are invalid")
    if int(command.get("version") or 0) != 1:
        raise StableLifecycleCommandError("stable lifecycle command version is invalid")
    if command.get("action") != "set_stable_market_lifecycle":
        raise StableLifecycleCommandError("stable lifecycle action is invalid")
    command_id = str(command.get("command_id") or "")
    if not re.fullmatch(r"[A-Za-z0-9._-]{1,160}", command_id):
        raise StableLifecycleCommandError("command_id is invalid")
    idx = int(command.get("account_index") or 0)
    if idx != int(expected_account_index):
        raise StableLifecycleCommandError("lifecycle command targets another account")
    now = time.time() if now_ts is None else float(now_ts)
    created_at = _finite_float(command.get("created_at"), field="created_at")
    age = now - created_at
    if created_at <= 0 or age < -30 or age > DEFAULT_MAX_PROPOSAL_AGE_SEC:
        raise StableLifecycleCommandError("stable lifecycle command is stale")

    identity = None
    if enabled:
        identity = validate_enablement_proposal(
            proposal or {},
            account_index=idx,
            expected_account_uid_key=expected_account_uid_key,
            expected_host_id=expected_host_id,
            now_ts=now,
        )
        if abs(
            float(command.get("proposal_generated_at") or 0.0)
            - float(identity["proposal_generated_at"])
        ) > 0.001:
            raise StableLifecycleCommandError("command references another proposal")
        if str(command.get("account_uid_key") or "").lower() != identity[
            "account_uid_key"
        ]:
            raise StableLifecycleCommandError("command account identity changed")
        if str(command.get("host_id") or "").lower() != identity["host_id"]:
            raise StableLifecycleCommandError("command runtime host changed")
    expected_confirmation = confirmation_for_change(
        idx,
        enabled,
        identity=identity,
    )
    if str(command.get("confirm") or "") != expected_confirmation:
        raise StableLifecycleCommandError("stable lifecycle confirmation does not match")
    return {
        "account_index": idx,
        "enabled": enabled,
        "command_id": command_id,
        "identity": identity,
    }
