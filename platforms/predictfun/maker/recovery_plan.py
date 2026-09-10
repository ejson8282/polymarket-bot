"""Offline review gate for legacy submissions; never changes runtime state.

Inputs are evidence supplied by an independently reviewed collector. This module
checks consistency, not provenance, and never authorizes activation or a write.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
import re
from typing import Any


EXCHANGES = {
    (False, False): "0x8bc070bedab741406f4b1eb65a72bee27894b689",
    (True, False): "0x365fb81bd4a24d6303cd2f19c349de6894d8d58a",
    (False, True): "0x6beb5a40c032afc305961162d8204cda16decfa5",
    (True, True): "0x8a289d458f5a134ba40015085a8f50ffb681b41d",
}


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _time(value: object) -> float:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone_required")
    return parsed.timestamp()


def _uint(value: object) -> bool:
    return type(value) is int and 0 <= value < 2**256


def assess_recovery(
    managed_state: dict[str, Any], *, account_id: str, keys: list[str],
    fence: dict[str, Any], baseline: dict[str, Any],
    nonce_evidence: list[dict[str, Any]], now: datetime,
) -> dict[str, Any]:
    """Return a plan, never a replacement registry or a resume permission.

Each key needs a reviewed old-nonce upper bound and a receipt-backed nonce
barrier in its exact maker/exchange context. No default 24-hour expiry inference.
"""
    if now.tzinfo is None:
        raise ValueError("timezone_required")
    if not account_id or not keys or any(not isinstance(k, str) or not k for k in keys):
        raise ValueError("exact_account_and_keys_required")
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate_keys")
    if not isinstance(managed_state, dict) or not isinstance(managed_state.get("pending_submissions"), list):
        raise ValueError("invalid_registry")
    selected = [r for r in managed_state["pending_submissions"]
                if isinstance(r, dict) and r.get("account_id") == account_id
                and r.get("idempotency_key") in keys]
    if len(selected) != len(keys) or {r["idempotency_key"] for r in selected} != set(keys):
        raise ValueError("pending_allowlist_mismatch")
    blocks: list[str] = []
    clock = now.timestamp()

    def recent(value: object) -> bool:
        try:
            return 0 <= clock - _time(value) <= 60
        except (TypeError, ValueError, OverflowError):
            return False

    if not isinstance(fence, dict) or not isinstance(baseline, dict):
        raise ValueError("invalid_evidence")
    if (fence.get("account_id") != account_id or fence.get("keys") != sorted(keys)
            or fence.get("enforced") is not True
            or fence.get("strict_ledger") is not True
            or fence.get("all_writers_quiesced") is not True
            or not recent(fence.get("verified_at"))):
        blocks.append("signer_fence_not_verified")
    if (baseline.get("account_id") != account_id or baseline.get("ok") is not True
            or baseline.get("pagination_complete") is not True
            or baseline.get("open_orders") != [] or baseline.get("positions") != []
            or re.fullmatch(r"0x[0-9a-fA-F]{40}", str(baseline.get("maker", ""))) is None
            or not recent(baseline.get("observed_at"))):
        blocks.append("account_baseline_not_empty_fresh_complete")
    if not isinstance(nonce_evidence, list):
        raise ValueError("invalid_nonce_evidence")
    for pending in selected:
        key = pending["idempotency_key"]
        matches = [r for r in nonce_evidence if isinstance(r, dict)
                   and r.get("account_id") == account_id and r.get("idempotency_key") == key]
        if len(matches) != 1:
            blocks.append(f"nonce_evidence_missing_or_duplicate:{key}")
            continue
        item = matches[0]
        bound, current = item.get("old_nonce_upper_bound"), item.get("current_nonce")
        valid = (
            item.get("chain_id") == 56
            and _uint(bound) and _uint(current) and current > bound
            and item.get("old_nonce_valid") is False
            and item.get("current_nonce_valid") is True
            and item.get("receipt_success") is True
            and type(item.get("confirmations")) is int and item["confirmations"] >= 12
            and item.get("market_id") == pending.get("market_id")
            and type(item.get("is_neg_risk")) is bool
            and type(item.get("is_yield_bearing")) is bool
            and item.get("is_neg_risk") is pending.get("is_neg_risk")
            and item.get("is_yield_bearing") is pending.get("is_yield_bearing")
            and isinstance(item.get("maker"), str)
            and re.fullmatch(r"0x[0-9a-fA-F]{40}", item["maker"]) is not None
            and item["maker"].lower() == str(baseline.get("maker", "")).lower()
            and isinstance(item.get("exchange"), str)
            and re.fullmatch(r"0x[0-9a-fA-F]{40}", item["exchange"]) is not None
            and item["exchange"].lower() == EXCHANGES.get((item["is_neg_risk"], item["is_yield_bearing"]))
            and re.fullmatch(r"[0-9a-f]{64}", str(item.get("signing_audit_sha256", ""))) is not None
            and re.fullmatch(r"0x[0-9a-fA-F]{64}", str(item.get("tx_hash", ""))) is not None
            and recent(item.get("observed_at"))
        )
        # The fresh account baseline must follow the confirmed invalidation.
        try:
            valid = valid and _time(baseline.get("observed_at")) >= _time(item.get("confirmed_at"))
            valid = valid and _time(item.get("confirmed_at")) >= _time(fence.get("enforced_at"))
            valid = valid and _time(item.get("observed_at")) >= _time(item.get("confirmed_at"))
        except (TypeError, ValueError, OverflowError):
            valid = False
        if not valid:
            blocks.append(f"nonce_barrier_not_verified:{key}")
    return {
        "schema_version": 1, "mode": "RESEARCH", "account_id": account_id,
        "status": "blocked" if blocks else "ready_for_independent_review",
        "activation_allowed": False, "runtime_write_allowed": False,
        "blocks": blocks, "registry_sha256": _digest(managed_state),
        "evidence_sha256": _digest({"fence": fence, "baseline": baseline, "nonces": nonce_evidence}),
        "archive_candidates": deepcopy(selected),
        "historical_submission_outcome": "unknown",
        "required_next_step": "independent_evidence_review_then_authorized_maintenance",
    }
