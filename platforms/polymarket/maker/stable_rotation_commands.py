"""Validated command contract for manually confirmed stable LP rotations."""

from __future__ import annotations

import time
from typing import Any, Mapping


DEFAULT_MAX_PROPOSAL_AGE_SEC = 900.0
CONFIRMATION_PREFIX = "CONFIRM-STABLE-ROTATION"


class StableRotationCommandError(ValueError):
    """The proposal or requested replacement is not safe to execute."""


def confirmation_for_replacement(replacement_id: str) -> str:
    replacement_id = str(replacement_id or "").strip()
    if len(replacement_id) != 64:
        raise StableRotationCommandError("replacement_id must be a sha256 digest")
    try:
        int(replacement_id, 16)
    except ValueError as exc:
        raise StableRotationCommandError(
            "replacement_id must be a sha256 digest"
        ) from exc
    return f"{CONFIRMATION_PREFIX}:{replacement_id}"


def find_confirmed_replacement(
    proposal: Mapping[str, Any],
    *,
    account_index: int,
    replacement_id: str,
    now_ts: float | None = None,
    max_proposal_age_sec: float = DEFAULT_MAX_PROPOSAL_AGE_SEC,
) -> dict[str, Any]:
    """Return one account-scoped replacement from a fresh review-only proposal."""

    if not isinstance(proposal, Mapping):
        raise StableRotationCommandError("stable rotation proposal must be an object")
    if int(proposal.get("schema_version") or 0) < 2:
        raise StableRotationCommandError("stable rotation proposal schema is too old")
    if proposal.get("mode") != "proposal_only" or proposal.get("status") != "ready":
        raise StableRotationCommandError("stable rotation proposal is not ready")
    safety = proposal.get("safety")
    if not isinstance(safety, Mapping) or safety.get("proposal_only") is not True:
        raise StableRotationCommandError("proposal-only safety marker is missing")
    if safety.get("requires_manual_review") is not True:
        raise StableRotationCommandError("manual review is not required by proposal")
    if any(
        safety.get(key) is not False
        for key in ("trading_actions", "runtime_commands", "runtime_config_writes")
    ):
        raise StableRotationCommandError("proposal safety flags permit side effects")

    generated_at = float(proposal.get("generated_at") or 0.0)
    now = time.time() if now_ts is None else float(now_ts)
    age = now - generated_at
    if generated_at <= 0 or age < -30 or age > max_proposal_age_sec:
        raise StableRotationCommandError("stable rotation proposal is stale")

    requested_account = int(account_index)
    requested_id = str(replacement_id or "").strip()
    confirmation_for_replacement(requested_id)
    accounts = proposal.get("accounts")
    if not isinstance(accounts, list):
        raise StableRotationCommandError("proposal accounts are invalid")
    for account in accounts:
        if not isinstance(account, Mapping):
            continue
        if int(account.get("account_index") or 0) != requested_account:
            continue
        replacements = account.get("replace")
        if not isinstance(replacements, list):
            raise StableRotationCommandError("account replacements are invalid")
        for replacement in replacements:
            if not isinstance(replacement, Mapping):
                continue
            if str(replacement.get("replacement_id") or "") != requested_id:
                continue
            retire = replacement.get("retire")
            add = replacement.get("add")
            if not isinstance(retire, Mapping) or not isinstance(add, Mapping):
                raise StableRotationCommandError("replacement market pair is invalid")
            retire_token = str(retire.get("token_id") or "")
            add_token = str(add.get("token_id") or "")
            add_pair = str(add.get("paired_token_id") or "")
            if not retire_token.isdigit():
                raise StableRotationCommandError("retirement token is invalid")
            if not add_token.isdigit() or not add_pair.isdigit() or add_token == add_pair:
                raise StableRotationCommandError("replacement token pair is invalid")
            return dict(replacement)
        raise StableRotationCommandError("replacement is not in the account proposal")
    raise StableRotationCommandError("account is not in the proposal")


def build_confirmed_replacement_command(
    proposal: Mapping[str, Any],
    *,
    account_index: int,
    replacement_id: str,
    command_id: str,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Build the only runtime payload accepted for a reviewed replacement."""

    now = time.time() if now_ts is None else float(now_ts)
    replacement = find_confirmed_replacement(
        proposal,
        account_index=account_index,
        replacement_id=replacement_id,
        now_ts=now,
    )
    command_id = str(command_id or "").strip()
    if not command_id:
        raise StableRotationCommandError("command_id is required")
    retire = replacement["retire"]
    add = replacement["add"]
    selection = replacement.get("selection")
    if not isinstance(selection, Mapping):
        raise StableRotationCommandError("replacement selection is invalid")
    target_section = str(
        selection.get("target_config_section") or ""
    ).strip()
    if target_section not in {"markets", "night_markets"}:
        raise StableRotationCommandError("replacement target section is invalid")
    return {
        "version": 2,
        "command_id": command_id,
        "action": "replace_market",
        "created_at": now,
        "account_index": int(account_index),
        "proposal_generated_at": float(proposal["generated_at"]),
        "replacement_id": str(replacement_id),
        "confirm": confirmation_for_replacement(replacement_id),
        "retire_token_id": str(retire["token_id"]),
        "target_config_section": target_section,
        "market": {
            "token_id": str(add["token_id"]),
            "paired_token_id": str(add["paired_token_id"]),
            "condition_id": str(add.get("condition_id") or ""),
            "slug": str(add.get("slug") or ""),
            "question": str(add.get("question") or ""),
        },
        "selection": dict(selection),
    }
