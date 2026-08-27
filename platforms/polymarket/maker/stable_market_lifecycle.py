"""Deterministic stable-LP market lifecycle decisions.

The reward producer remains read-only.  Each engine consumes the proposal for
its own account and applies the resulting actions behind an explicit config
gate.  This module performs no network or filesystem writes.
"""

from __future__ import annotations

import math
import re
from typing import Any, Mapping, Sequence


STATE_VERSION = 3
MAX_ACTIVE_CANARIES_LIMIT = 10
MAX_CANARY_PRINCIPAL_FRACTION = 0.10
MAX_CANARY_USDC = 100.0
MIN_PROMOTION_SCORING_SAMPLES = 3
HARD_RETIRE_REASONS = frozenset(
    {
        "market_not_active",
        "market_closed_or_unknown",
        "market_archived_or_unknown",
        "market_not_accepting_orders",
        "market_expired_or_end_unknown",
        "market_ends_too_soon",
        "live_market_observe_only",
        "weather_observe_only",
    }
)


def _number(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def account_admission(
    row: Mapping[str, Any],
    account_index: int,
) -> tuple[str, tuple[str, ...]] | None:
    """Return the account-specific full/canary/reject admission."""

    admissions = row.get("account_admission")
    if not isinstance(admissions, Sequence) or isinstance(
        admissions,
        (str, bytes),
    ):
        return None
    for admission in admissions:
        if not isinstance(admission, Mapping):
            continue
        if int(_number(admission.get("account_index"), -1)) != account_index:
            continue
        level = str(admission.get("level") or "reject").strip().lower()
        if level not in {"full", "canary", "reject"}:
            level = "reject"
        reasons = tuple(
            dict.fromkeys(
                str(reason)
                for reason in admission.get("reason_codes") or []
                if str(reason)
            )
        )
        return level, reasons
    return None


def account_execution(
    row: Mapping[str, Any],
    account_index: int,
) -> Mapping[str, Any] | None:
    executions = row.get("account_execution")
    if not isinstance(executions, Sequence) or isinstance(
        executions,
        (str, bytes),
    ):
        return None
    for execution in executions:
        if not isinstance(execution, Mapping):
            continue
        if int(_number(execution.get("account_index"), -1)) == account_index:
            return execution
    return None


def account_scoring_evidence(
    row: Mapping[str, Any],
    account_index: int,
    *,
    expected_account_uid_key: str = "",
    expected_host_id: str = "",
) -> Mapping[str, Any] | None:
    """Return sanitized account-local scoring evidence from a proposal row."""

    evidence = row.get("account_execution_evidence")
    if not isinstance(evidence, Mapping):
        return None
    if int(_number(evidence.get("account_index"), -1)) != account_index:
        return None
    if expected_account_uid_key and str(
        evidence.get("account_uid_key") or ""
    ).strip() != expected_account_uid_key:
        return None
    if expected_host_id and str(evidence.get("host_id") or "").strip().lower() != (
        expected_host_id.strip().lower()
    ):
        return None
    return evidence


def scoring_sample_is_valid(
    row: Mapping[str, Any],
    account_index: int,
    *,
    expected_account_uid_key: str = "",
    expected_host_id: str = "",
) -> bool:
    """Require current paired scoring, a unique sample, and positive observed Q."""

    evidence = account_scoring_evidence(
        row,
        account_index,
        expected_account_uid_key=expected_account_uid_key,
        expected_host_id=expected_host_id,
    )
    if evidence is None or evidence.get("official_scoring") is not True:
        return False
    sample_id = str(evidence.get("scoring_sample_id") or "").strip().lower()
    if not re.fullmatch(r"[0-9a-f]{64}", sample_id):
        return False
    observed_q = evidence.get("observed_q_min")
    return observed_q is not None and _number(observed_q) > 0


def scoring_sample_id(
    row: Mapping[str, Any],
    account_index: int,
    *,
    expected_account_uid_key: str = "",
    expected_host_id: str = "",
) -> str:
    evidence = account_scoring_evidence(
        row,
        account_index,
        expected_account_uid_key=expected_account_uid_key,
        expected_host_id=expected_host_id,
    )
    if evidence is None:
        return ""
    sample_id = str(evidence.get("scoring_sample_id") or "").strip().lower()
    return sample_id if re.fullmatch(r"[0-9a-f]{64}", sample_id) else ""


def candidate_is_executable_for_account(
    row: Mapping[str, Any],
    account_index: int,
    *,
    allow_canary: bool,
) -> tuple[bool, str, tuple[str, ...]]:
    """Fail closed unless this account can form a non-zero eligible Q."""

    admission = account_admission(row, account_index)
    if admission is None:
        if row.get("stable_lp_recommended") is not True:
            return False, "reject", ("account_admission_unavailable",)
        level, reasons = "full", ()
    else:
        level, reasons = admission
    if level == "reject":
        return False, level, reasons or ("account_admission_rejected",)
    if level == "canary":
        if not allow_canary:
            return False, level, ("canary_not_allowed",)
        eligible_indexes = row.get("canary_proposal_eligible_account_indexes")
        if not isinstance(eligible_indexes, list) or account_index not in {
            int(_number(value, -1)) for value in eligible_indexes
        }:
            return False, level, ("canary_observation_incomplete",)

    execution = account_execution(row, account_index)
    if execution is None:
        return False, level, ("account_execution_unavailable",)
    if execution.get("executable") is not True:
        blocked = tuple(
            str(reason)
            for reason in execution.get("blocked_reasons") or []
            if str(reason)
        )
        return False, level, blocked or ("account_quote_not_executable",)
    if _number(execution.get("executable_q_min")) <= 0:
        return False, level, ("executable_q_min_zero",)
    return True, level, reasons


def _proposal_account(
    proposal: Mapping[str, Any],
    account_index: int,
) -> Mapping[str, Any] | None:
    accounts = proposal.get("accounts")
    if not isinstance(accounts, Sequence) or isinstance(accounts, (str, bytes)):
        return None
    for account in accounts:
        if not isinstance(account, Mapping):
            continue
        if int(_number(account.get("account_index"), -1)) == account_index:
            return account
    return None


def _token_rows(rows: Any) -> dict[str, Mapping[str, Any]]:
    result: dict[str, Mapping[str, Any]] = {}
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        return result
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        token_id = str(row.get("token_id") or "").strip()
        if token_id.isdigit():
            result[token_id] = row
    return result


def build_lifecycle_plan(
    proposal: Mapping[str, Any],
    *,
    account_index: int,
    configured_token_ids: set[str],
    managed_token_ids: set[str],
    previous_state: Mapping[str, Any] | None,
    now_ts: float,
    managed_market_stages: Mapping[str, str] | None = None,
    max_proposal_age_sec: float = 900.0,
    max_add_per_cycle: int = 5,
    max_active_canaries: int = 10,
    canary_budget_usdc: float | None = None,
    promotion_scoring_threshold: int = 3,
    soft_failure_threshold: int = 3,
    hard_failure_threshold: int = 1,
    expected_account_uid_key: str = "",
    expected_host_id: str = "",
) -> dict[str, Any]:
    """Build one idempotent plan from a fresh account-local proposal sample."""

    previous = dict(previous_state or {})
    if int(_number(previous.get("version"), 0)) != STATE_VERSION:
        # v2 counted proposal refreshes rather than distinct paired scoring
        # samples. Preserve proposal idempotence, but do not carry promotion or
        # retirement counters into the v3 contract.
        previous = {
            "last_proposal_generated_at": previous.get(
                "last_proposal_generated_at"
            )
        }
    current_stages = {
        str(token): str(stage or "full").strip().lower()
        for token, stage in (managed_market_stages or {}).items()
    }
    last_sample = _number(previous.get("last_proposal_generated_at"), 0.0)
    markets_state = previous.get("markets")
    if not isinstance(markets_state, Mapping):
        markets_state = {}
    max_canaries = min(
        MAX_ACTIVE_CANARIES_LIMIT,
        max(0, int(max_active_canaries)),
    )
    active_canaries = sum(stage == "canary" for stage in current_stages.values())
    output = {
        "version": STATE_VERSION,
        "account_index": account_index,
        "status": "blocked",
        "reason": "proposal_invalid",
        "generated_at": now_ts,
        "proposal_generated_at": None,
        "last_proposal_generated_at": last_sample,
        "new_sample": False,
        "add": [],
        "promote": [],
        "retire": [],
        "active_canaries": active_canaries,
        "max_active_canaries": max_canaries,
        "canary_limit_exceeded": active_canaries > max_canaries,
        "markets": {
            str(token): dict(state)
            for token, state in markets_state.items()
            if isinstance(state, Mapping)
        },
    }
    if proposal.get("status") != "ready":
        output["reason"] = "proposal_not_ready"
        return output
    proposal_generated_at = _number(proposal.get("generated_at"), -1.0)
    age = now_ts - proposal_generated_at
    if proposal_generated_at <= 0 or age < -30 or age > max_proposal_age_sec:
        output["reason"] = "proposal_stale_or_invalid"
        return output
    account = _proposal_account(proposal, account_index)
    if account is None:
        output["reason"] = "account_not_in_proposal"
        return output
    if expected_account_uid_key and str(
        account.get("account_uid_key") or ""
    ).strip() != expected_account_uid_key:
        output["reason"] = "proposal_account_identity_mismatch"
        return output
    if expected_host_id and str(account.get("host_id") or "").strip().lower() != (
        expected_host_id.strip().lower()
    ):
        output["reason"] = "proposal_host_identity_mismatch"
        return output

    new_sample = proposal_generated_at > last_sample + 0.001
    output.update(
        {
            "status": "ready",
            "reason": "ok",
            "proposal_generated_at": proposal_generated_at,
            "new_sample": new_sample,
            "last_proposal_generated_at": max(
                proposal_generated_at,
                last_sample,
            ),
        }
    )
    if not new_sample:
        return output

    additions: list[dict[str, Any]] = []
    add_limit = max(0, int(max_add_per_cycle))
    canary_slots = max(
        0,
        max_canaries - int(output["active_canaries"]),
    )
    if output["canary_limit_exceeded"]:
        add_limit = 0
    for stage, key in (("full", "add"), ("canary", "canary")):
        if add_limit == 0:
            break
        rows = account.get(key)
        if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
            continue
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            token_id = str(row.get("token_id") or "").strip()
            paired = str(row.get("paired_token_id") or "").strip()
            if (
                not token_id.isdigit()
                or not paired.isdigit()
                or token_id in configured_token_ids
                or paired in configured_token_ids
            ):
                continue
            if stage == "canary":
                if canary_slots <= 0:
                    continue
                rewards_min = _number(row.get("rewards_min_size_shares"), 0.0)
                if (
                    canary_budget_usdc is not None
                    and rewards_min > max(0.0, float(canary_budget_usdc))
                ):
                    continue
            additions.append({"stage": stage, "market": dict(row)})
            if stage == "canary":
                canary_slots -= 1
            if len(additions) >= add_limit:
                break
        if len(additions) >= add_limit:
            break
    output["add"] = additions

    keep = _token_rows(account.get("keep"))
    review = _token_rows(account.get("review"))
    for token_id in sorted(managed_token_ids):
        prior = output["markets"].get(token_id)
        if not isinstance(prior, Mapping):
            prior = {}
        state = dict(prior)
        stage = current_stages.get(
            token_id,
            str(prior.get("lifecycle_stage") or "full").strip().lower(),
        )
        if stage not in {"canary", "full"}:
            stage = "full"
        state["lifecycle_stage"] = stage
        state["last_proposal_generated_at"] = proposal_generated_at
        if token_id in keep:
            scoring_valid = scoring_sample_is_valid(
                keep[token_id],
                account_index,
                expected_account_uid_key=expected_account_uid_key,
                expected_host_id=expected_host_id,
            )
            sample_id = scoring_sample_id(
                keep[token_id],
                account_index,
                expected_account_uid_key=expected_account_uid_key,
                expected_host_id=expected_host_id,
            )
            prior_sample_id = str(prior.get("last_scoring_sample_id") or "")
            if stage == "canary" and scoring_valid:
                scoring_samples = int(prior.get("consecutive_scoring_samples") or 0)
                if sample_id != prior_sample_id:
                    scoring_samples += 1
            else:
                scoring_samples = 0
            promotion_threshold = max(
                MIN_PROMOTION_SCORING_SAMPLES,
                int(promotion_scoring_threshold),
            )
            promotion_due = bool(
                stage == "canary"
                and scoring_samples >= promotion_threshold
                and not output["canary_limit_exceeded"]
            )
            state.update(
                {
                    "status": "promotion_due" if promotion_due else "eligible",
                    "consecutive_failures": 0,
                    "consecutive_scoring_samples": scoring_samples,
                    "promotion_scoring_threshold": promotion_threshold,
                    "official_scoring_sample_valid": scoring_valid,
                    "last_scoring_sample_id": (
                        sample_id if scoring_valid else prior_sample_id
                    ),
                    "reason_codes": [],
                }
            )
            output["markets"][token_id] = state
            if promotion_due:
                output["promote"].append(
                    {
                        "token_id": token_id,
                        "target_risk": (
                            "low"
                            if _number(keep[token_id].get("fill_risk"), 100.0)
                            < 35.0
                            else "mid"
                        ),
                        "consecutive_scoring_samples": scoring_samples,
                    }
                )
            continue
        row = review.get(token_id)
        if row is None:
            state.update(
                {
                    "status": "unassessed",
                    "consecutive_scoring_samples": 0,
                    "reason_codes": ["configured_market_missing_from_proposal"],
                }
            )
            output["markets"][token_id] = state
            continue

        reasons = tuple(
            dict.fromkeys(
                str(reason)
                for reason in row.get("reason_codes") or []
                if str(reason)
            )
        )
        failures = int(prior.get("consecutive_failures") or 0) + 1
        hard = bool(HARD_RETIRE_REASONS.intersection(reasons)) or str(
            row.get("action") or ""
        ) == "review_retire"
        threshold = max(
            1,
            int(hard_failure_threshold if hard else soft_failure_threshold),
        )
        due = failures >= threshold
        state.update(
            {
                "status": "retire_due" if due else "watch",
                "consecutive_failures": failures,
                "consecutive_scoring_samples": 0,
                "failure_threshold": threshold,
                "hard_failure": hard,
                "reason_codes": list(reasons),
            }
        )
        output["markets"][token_id] = state
        if due:
            output["retire"].append(
                {
                    "token_id": token_id,
                    "hard_failure": hard,
                    "reason_codes": list(reasons),
                }
            )
    return output
