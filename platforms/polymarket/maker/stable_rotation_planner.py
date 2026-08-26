"""Build a review-only market rotation proposal for stable LP accounts.

The planner consumes the public reward observer snapshot and non-secret market
configuration. It never mutates configuration, writes runtime commands, or
performs trading actions. Its output is an audit document for later review.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = 2
OUTPUT_NAME = "stable_rotation_proposal.json"
DEFAULT_MAX_OBSERVER_AGE_SEC = 900.0
DEFAULT_MAX_DEPTH_AGE_SEC = 600.0
DEFAULT_MAX_ADD_PER_ACCOUNT = 5
DEFAULT_MIN_STABILITY_SCORE = 70.0
DEFAULT_MAX_FILL_RISK = 35.0
DEFAULT_MIN_RISK_ADJUSTED_DAILY_ROI_PCT = 0.1
DEFAULT_MIN_SPORTS_LEAD_SEC = 3 * 60 * 60
DEFAULT_MIN_MARKET_TIME_TO_END_SEC = 12 * 60 * 60
DEFAULT_MIN_FRONT_BID_NOTIONAL_USDC = 2_000.0
_CONFIG_NAME_RE = re.compile(r"^config_(\d+)\.json$")


class StableRotationError(ValueError):
    """The proposal input is malformed or unsafe to interpret."""


def _number(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _token_pair(row: Mapping[str, Any]) -> tuple[str, str]:
    token_id = str(row.get("token_id") or "").strip()
    paired_token_id = str(row.get("paired_token_id") or "").strip()
    return token_id, paired_token_id


def _event_aliases(row: Mapping[str, Any]) -> tuple[str, ...]:
    aliases: list[str] = []
    condition_id = str(row.get("condition_id") or "").strip().lower()
    if condition_id:
        aliases.append(f"condition:{condition_id}")
    token_id, paired_token_id = _token_pair(row)
    if token_id and paired_token_id and token_id != paired_token_id:
        aliases.append("pair:" + ":".join(sorted((token_id, paired_token_id))))
    return tuple(aliases)


def _canonical_event_key(row: Mapping[str, Any]) -> str:
    aliases = _event_aliases(row)
    if not aliases:
        return ""
    return aliases[0]


def _public_market_fields(row: Mapping[str, Any]) -> dict[str, Any]:
    token_id, paired_token_id = _token_pair(row)
    return {
        "event_key": _canonical_event_key(row),
        "condition_id": str(row.get("condition_id") or "").strip().lower(),
        "token_id": token_id,
        "paired_token_id": paired_token_id,
        "question": str(row.get("question") or "").strip(),
        "slug": str(row.get("slug") or "").strip(),
        "market_url": str(row.get("market_url") or "").strip(),
    }


def _candidate_metrics(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "daily_reward_usd": round(_number(row.get("daily_reward_usd")), 2),
        "estimated_reward_share_pct": round(
            _number(row.get("estimated_reward_share_pct")), 2
        ),
        "estimated_daily_gross_usd": round(
            _number(row.get("estimated_daily_gross_usd")), 2
        ),
        "risk_adjusted_daily_roi_pct": round(
            _number(row.get("risk_adjusted_daily_roi_pct")), 2
        ),
        "fill_risk": round(_number(row.get("fill_risk"), 100.0), 1),
        "stability_score": round(_number(row.get("stability_score")), 1),
        "verification_status": str(row.get("verification_status") or "unknown"),
        "min_front_bid_notional_usd": round(
            _number(row.get("min_front_bid_notional_usd")), 2
        ),
        "probe_capital_usd": round(_number(row.get("probe_capital_usd")), 2),
        "theoretical_reward_share_pct": round(
            _number(row.get("theoretical_reward_share_pct")), 2
        ),
        "executable_reward_share_pct": round(
            _number(row.get("executable_reward_share_pct")), 2
        ),
        "executable_q_min": round(_number(row.get("executable_q_min")), 6),
        "admission_level": str(row.get("admission_level") or "unknown"),
    }


def _proposal_row(
    row: Mapping[str, Any],
    *,
    action: str,
    reason_codes: Sequence[str],
    config_section: str = "",
) -> dict[str, Any]:
    proposal = {
        "action": action,
        **_public_market_fields(row),
        **_candidate_metrics(row),
        "reason_codes": list(reason_codes),
    }
    if config_section in {"markets", "night_markets"}:
        proposal["config_section"] = config_section
    return proposal


def _current_row(
    row: Mapping[str, Any],
    *,
    action: str,
    reason_codes: Sequence[str],
) -> dict[str, Any]:
    current = {
        "action": action,
        **_public_market_fields(row),
        "enabled": row.get("enabled", True) is not False,
        "reason_codes": list(reason_codes),
    }
    section = str(row.get("section") or "")
    if section in {"markets", "night_markets"}:
        current["config_section"] = section
    return current


def _review_current_row(
    current: Mapping[str, Any],
    candidate: Mapping[str, Any],
    *,
    action: str,
    reason_codes: Sequence[str],
) -> dict[str, Any]:
    """Keep the configured token pair while attaching current observer metrics."""

    row = {
        "action": action,
        **_public_market_fields(current),
        **_candidate_metrics(candidate),
        "reason_codes": list(reason_codes),
    }
    for key in ("condition_id", "question", "slug", "market_url"):
        if not row.get(key):
            row[key] = _public_market_fields(candidate).get(key, "")
    section = str(current.get("section") or "")
    if section in {"markets", "night_markets"}:
        row["config_section"] = section
    return row


def _candidate_rank(row: Mapping[str, Any]) -> tuple[float, ...]:
    executable_share = row.get("executable_reward_share_pct")
    if executable_share is None:
        executable_share = row.get("estimated_reward_share_pct")
    return (
        _number(row.get("risk_adjusted_daily_roi_pct"), -1.0),
        _number(row.get("estimated_daily_gross_usd"), -1.0),
        _number(executable_share, -1.0),
        _number(row.get("stability_score"), -1.0),
        -_number(row.get("fill_risk"), 100.0),
    )


def _account_admission(
    row: Mapping[str, Any],
    account_index: int,
) -> tuple[str, list[str]] | None:
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
        reasons = [
            str(reason)
            for reason in admission.get("reason_codes") or []
            if str(reason)
        ]
        return level, reasons
    return None


def _retirement_rank(row: Mapping[str, Any]) -> tuple[float, ...]:
    """Put unsafe and unobservable markets ahead of merely inefficient ones."""

    action_priority = {
        "review_retire": 3.0,
        "review": 2.0,
        "review_rotate": 1.0,
    }.get(str(row.get("action") or ""), 0.0)
    return (
        action_priority,
        -_number(row.get("risk_adjusted_daily_roi_pct"), -1.0),
        -_number(row.get("estimated_daily_gross_usd"), -1.0),
        -_number(row.get("estimated_reward_share_pct"), -1.0),
        _number(row.get("fill_risk"), 100.0),
        -_number(row.get("stability_score"), -1.0),
    )


def _replacement_id(
    *,
    account_index: int,
    generated_at: float,
    retire: Mapping[str, Any],
    add: Mapping[str, Any],
) -> str:
    material = {
        "schema_version": SCHEMA_VERSION,
        "account_index": account_index,
        "generated_at": generated_at,
        "retire_event_key": str(retire.get("event_key") or ""),
        "retire_token_id": str(retire.get("token_id") or ""),
        "add_event_key": str(add.get("event_key") or ""),
        "add_token_id": str(add.get("token_id") or ""),
    }
    encoded = json.dumps(
        material,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _replacement_row(
    *,
    account_index: int,
    generated_at: float,
    retire: Mapping[str, Any],
    add: Mapping[str, Any],
    min_front_bid_notional_usdc: float,
) -> dict[str, Any]:
    return {
        "action": "replace",
        "replacement_id": _replacement_id(
            account_index=account_index,
            generated_at=generated_at,
            retire=retire,
            add=add,
        ),
        "retire": dict(retire),
        "add": dict(add),
        "selection": {
            "primary_metric": "risk_adjusted_daily_roi_pct",
            "competition_metric": "executable_reward_share_pct",
            "risk_metric": "fill_risk",
            "depth_guard_unchanged": True,
            "min_front_bid_notional_usdc": round(
                min_front_bid_notional_usdc,
                2,
            ),
            "target_config_section": str(
                retire.get("config_section") or "markets"
            ),
        },
    }


def _global_rejections(
    row: Mapping[str, Any],
    *,
    now_ts: float,
    max_depth_age_sec: float,
    min_stability_score: float,
    max_fill_risk: float,
    min_risk_adjusted_daily_roi_pct: float,
    min_sports_lead_sec: float,
    min_market_time_to_end_sec: float,
) -> list[str]:
    reasons: list[str] = []
    token_id, paired_token_id = _token_pair(row)
    if not token_id.isdigit() or not paired_token_id.isdigit() or token_id == paired_token_id:
        reasons.append("invalid_token_pair")
    if row.get("market_active") is not True:
        reasons.append("market_not_active")
    if row.get("market_closed") is not False:
        reasons.append("market_closed_or_unknown")
    if row.get("market_archived") is not False:
        reasons.append("market_archived_or_unknown")
    if row.get("accepting_orders") is not True:
        reasons.append("market_not_accepting_orders")
    market_end_ts = _number(row.get("market_end_ts"), -1.0)
    if market_end_ts <= now_ts:
        reasons.append("market_expired_or_end_unknown")
    elif market_end_ts < now_ts + min_market_time_to_end_sec:
        reasons.append("market_ends_too_soon")
    account_admissions = row.get("account_admission")
    has_account_admission = isinstance(account_admissions, Sequence) and not isinstance(
        account_admissions,
        (str, bytes),
    )
    if has_account_admission:
        levels = {
            str(admission.get("level") or "reject")
            for admission in account_admissions
            if isinstance(admission, Mapping)
        }
        if not levels.intersection({"full", "canary"}):
            reasons.append("all_accounts_reject_candidate")
    else:
        if row.get("verification_recommended") is not True:
            reasons.append("verification_not_recommended")
        if row.get("stable_lp_recommended") is not True:
            stable_reasons = [
                str(reason)
                for reason in row.get("stable_lp_rejection_reasons") or []
                if str(reason)
            ]
            reasons.extend(stable_reasons)
            if not stable_reasons:
                reasons.append("stable_lp_not_recommended")
        if _number(row.get("stability_score")) < min_stability_score:
            reasons.append("stability_below_min")
        if _number(row.get("fill_risk"), 100.0) >= max_fill_risk:
            reasons.append("fill_risk_above_stable_limit")
        if (
            _number(row.get("risk_adjusted_daily_roi_pct"))
            < min_risk_adjusted_daily_roi_pct
        ):
            reasons.append("risk_adjusted_roi_below_min")
    phase = str(row.get("market_phase") or "").strip().lower()
    if phase == "live":
        reasons.append("live_market_observe_only")
    elif phase == "pregame":
        game_start_ts = _number(row.get("game_start_ts"), -1.0)
        if game_start_ts < now_ts + min_sports_lead_sec:
            reasons.append("sports_start_too_close")
    if str(row.get("front_depth_status") or "") != "verified":
        reasons.append("front_depth_unavailable")
    depth_observed_at = _number(row.get("front_depth_observed_at"), -1.0)
    depth_age = now_ts - depth_observed_at
    if depth_observed_at <= 0 or depth_age < -30 or depth_age > max_depth_age_sec:
        reasons.append("front_depth_stale")
    return reasons


def _account_depth_rejection(
    row: Mapping[str, Any],
    *,
    min_front_bid_notional_usdc: float,
) -> str | None:
    yes_depth = _number(row.get("yes_front_bid_notional_usd"), -1.0)
    no_depth = _number(row.get("no_front_bid_notional_usd"), -1.0)
    if min(yes_depth, no_depth) < min_front_bid_notional_usdc:
        return "front_depth_below_account_min"
    return None


def _profile_type(config: Mapping[str, Any]) -> str:
    account = config.get("account")
    if not isinstance(account, Mapping):
        return ""
    profile = account.get("lp_account")
    if not isinstance(profile, Mapping):
        return ""
    return str(profile.get("profile_type") or "").strip().lower()


def load_stable_account_configs(config_dir: Path) -> list[dict[str, Any]]:
    """Load only stable/legacy account market rows without retaining secrets."""

    accounts: list[dict[str, Any]] = []
    for path in sorted(config_dir.glob("config_*.json")):
        match = _CONFIG_NAME_RE.fullmatch(path.name)
        if match is None:
            continue
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise StableRotationError(f"invalid account config: {path.name}") from exc
        if not isinstance(config, Mapping):
            raise StableRotationError(f"account config must be an object: {path.name}")
        if _profile_type(config) == "aggressive" or str(
            config.get("runtime_scope") or ""
        ).strip().lower() == "aggressive":
            continue

        markets: list[dict[str, Any]] = []
        for section in ("markets", "night_markets"):
            rows = config.get(section) or []
            if not isinstance(rows, list):
                raise StableRotationError(f"{path.name}.{section} must be an array")
            for raw in rows:
                if not isinstance(raw, Mapping):
                    raise StableRotationError(
                        f"{path.name}.{section} contains a non-object market"
                    )
                row = dict(raw)
                row["section"] = section
                markets.append(row)

        execution = config.get("execution")
        if not isinstance(execution, Mapping):
            execution = {}
        min_depth = _number(
            execution.get("min_front_bid_notional_usdc"),
            DEFAULT_MIN_FRONT_BID_NOTIONAL_USDC,
        )
        accounts.append(
            {
                "account_index": int(match.group(1)),
                "config_name": path.name,
                "min_front_bid_notional_usdc": max(1.0, min_depth),
                "markets": markets,
            }
        )
    accounts.sort(key=lambda row: row["account_index"])
    return accounts


def _candidate_lookup(
    candidates: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Mapping[str, Any]], list[Mapping[str, Any]]]:
    lookup: dict[str, Mapping[str, Any]] = {}
    unique: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for row in sorted(candidates, key=_candidate_rank, reverse=True):
        aliases = _event_aliases(row)
        if not aliases or any(alias in seen for alias in aliases):
            continue
        unique.append(row)
        seen.update(aliases)
        for alias in aliases:
            lookup[alias] = row
    return lookup, unique


def _find_candidate(
    current: Mapping[str, Any],
    lookup: Mapping[str, Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    for alias in _event_aliases(current):
        candidate = lookup.get(alias)
        if candidate is not None:
            return candidate
    return None


def _hard_retire_reasons(reasons: Sequence[str]) -> bool:
    return bool(
        {
            "market_not_active",
            "market_closed_or_unknown",
            "market_archived_or_unknown",
            "market_not_accepting_orders",
            "market_expired_or_end_unknown",
            "live_market_observe_only",
        }.intersection(reasons)
    )


def build_stable_rotation_proposal(
    observer: Mapping[str, Any],
    accounts: Sequence[Mapping[str, Any]],
    *,
    now_ts: float | None = None,
    max_observer_age_sec: float = DEFAULT_MAX_OBSERVER_AGE_SEC,
    max_depth_age_sec: float = DEFAULT_MAX_DEPTH_AGE_SEC,
    max_add_per_account: int = DEFAULT_MAX_ADD_PER_ACCOUNT,
    min_stability_score: float = DEFAULT_MIN_STABILITY_SCORE,
    max_fill_risk: float = DEFAULT_MAX_FILL_RISK,
    min_risk_adjusted_daily_roi_pct: float = (
        DEFAULT_MIN_RISK_ADJUSTED_DAILY_ROI_PCT
    ),
    min_sports_lead_sec: float = DEFAULT_MIN_SPORTS_LEAD_SEC,
    min_market_time_to_end_sec: float = DEFAULT_MIN_MARKET_TIME_TO_END_SEC,
) -> dict[str, Any]:
    """Return an auditable proposal with no executable configuration payload."""

    if max_add_per_account < 0:
        raise StableRotationError("max_add_per_account must not be negative")
    generated_at = time.time() if now_ts is None else float(now_ts)
    observer_generated_at = _number(observer.get("generated_at"), -1.0)
    observer_age = generated_at - observer_generated_at
    candidates_raw = observer.get("candidates")
    if not isinstance(candidates_raw, Sequence) or isinstance(
        candidates_raw, (str, bytes)
    ):
        raise StableRotationError("reward observer candidates are invalid")
    candidates = [row for row in candidates_raw if isinstance(row, Mapping)]

    account_rows: list[dict[str, Any]] = []
    for account in accounts:
        account_index = int(account.get("account_index") or 0)
        if account_index <= 0:
            raise StableRotationError("account_index must be positive")
        current_markets = account.get("markets") or []
        if not isinstance(current_markets, Sequence) or isinstance(
            current_markets, (str, bytes)
        ):
            raise StableRotationError("account markets are invalid")
        normalized_current = [
            dict(row) for row in current_markets if isinstance(row, Mapping)
        ]
        account_rows.append(
            {
                "account_index": account_index,
                "config_name": str(account.get("config_name") or ""),
                "min_front_bid_notional_usdc": max(
                    1.0,
                    _number(
                        account.get("min_front_bid_notional_usdc"),
                        DEFAULT_MIN_FRONT_BID_NOTIONAL_USDC,
                    ),
                ),
                "current": normalized_current,
                "add": [],
                "canary": [],
                "keep": [],
                "review": [],
                "disabled_hold": [],
                "replace": [],
            }
        )
    account_rows.sort(key=lambda row: row["account_index"])

    safety = {
        "proposal_only": True,
        "runtime_config_writes": False,
        "runtime_commands": False,
        "trading_actions": False,
        "requires_manual_review": True,
    }
    policy = {
        "max_observer_age_sec": round(max_observer_age_sec, 1),
        "max_depth_age_sec": round(max_depth_age_sec, 1),
        "max_add_per_account": int(max_add_per_account),
        "min_stability_score": round(min_stability_score, 1),
        "max_fill_risk": round(max_fill_risk, 1),
        "min_risk_adjusted_daily_roi_pct": round(
            min_risk_adjusted_daily_roi_pct, 2
        ),
        "min_sports_lead_sec": round(min_sports_lead_sec, 1),
        "min_market_time_to_end_sec": round(min_market_time_to_end_sec, 1),
        "allocation": "independent_per_account_best_candidates",
        "cross_account_duplicate_events": "allowed",
        "candidate_ranking": [
            "risk_adjusted_daily_roi_pct_desc",
            "estimated_daily_gross_usd_desc",
            "estimated_reward_share_pct_desc",
            "stability_score_desc",
            "fill_risk_asc",
        ],
        "replacement_requires_manual_confirmation": True,
        "depth_guard_relaxed": False,
    }
    if (
        observer_generated_at <= 0
        or observer_age < -30
        or observer_age > max_observer_age_sec
    ):
        return {
            "schema_version": SCHEMA_VERSION,
            "mode": "proposal_only",
            "status": "blocked",
            "reason": "reward_observer_snapshot_stale_or_invalid",
            "generated_at": generated_at,
            "observer_generated_at": observer_generated_at,
            "observer_age_sec": round(max(0.0, observer_age), 1),
            "safety": safety,
            "policy": policy,
            "accounts": [],
            "rejected_candidates": [],
            "summary": {
                "accounts": len(account_rows),
                "candidate_count": len(candidates),
                "planned_additions": 0,
                "planned_canaries": 0,
                "planned_replacements": 0,
            },
        }

    candidate_lookup, unique_candidates = _candidate_lookup(candidates)
    candidate_rejections: dict[str, list[str]] = {}
    globally_eligible: list[Mapping[str, Any]] = []
    for candidate in unique_candidates:
        event_key = _canonical_event_key(candidate)
        reasons = _global_rejections(
            candidate,
            now_ts=generated_at,
            max_depth_age_sec=max_depth_age_sec,
            min_stability_score=min_stability_score,
            max_fill_risk=max_fill_risk,
            min_risk_adjusted_daily_roi_pct=min_risk_adjusted_daily_roi_pct,
            min_sports_lead_sec=min_sports_lead_sec,
            min_market_time_to_end_sec=min_market_time_to_end_sec,
        )
        if reasons:
            candidate_rejections[event_key] = reasons
        else:
            globally_eligible.append(candidate)

    known_aliases_by_account: dict[int, set[str]] = {}
    for account in account_rows:
        account_index = account["account_index"]
        seen_current: set[str] = set()
        known_aliases_by_account[account_index] = set()
        for current in account["current"]:
            event_key = _canonical_event_key(current)
            if event_key and event_key in seen_current:
                continue
            if event_key:
                seen_current.add(event_key)
            known_aliases_by_account[account_index].update(_event_aliases(current))
            if current.get("enabled", True) is False:
                account["disabled_hold"].append(
                    _current_row(
                        current,
                        action="disabled_hold",
                        reason_codes=("operator_disabled",),
                    )
                )
                continue
            candidate = _find_candidate(current, candidate_lookup)
            if candidate is None:
                account["review"].append(
                    _current_row(
                        current,
                        action="review",
                        reason_codes=("not_in_current_observer_top_candidates",),
                    )
                )
                continue
            reasons = list(candidate_rejections.get(_canonical_event_key(candidate), ()))
            admission = _account_admission(candidate, account_index)
            admission_level = admission[0] if admission is not None else "legacy"
            if admission_level == "reject":
                reasons.extend(admission[1])
            elif admission_level == "legacy":
                depth_reason = _account_depth_rejection(
                    candidate,
                    min_front_bid_notional_usdc=account[
                        "min_front_bid_notional_usdc"
                    ],
                )
                if depth_reason is not None:
                    reasons.append(depth_reason)
            if reasons:
                action = "review_retire" if _hard_retire_reasons(reasons) else "review_rotate"
                account["review"].append(
                    _review_current_row(
                        current,
                        candidate,
                        action=action,
                        reason_codes=reasons,
                    )
                )
            else:
                account["keep"].append(
                    _proposal_row(
                        candidate,
                        action=(
                            "keep_canary"
                            if admission_level == "canary"
                            else "keep"
                        ),
                        reason_codes=(
                            tuple(admission[1])
                            if admission_level == "canary" and admission is not None
                            else ("verified_low_risk_efficient",)
                        ),
                        config_section=str(current.get("section") or ""),
                    )
                )
    unassigned: list[dict[str, Any]] = []
    for candidate in globally_eligible:
        account_reasons: dict[int, str] = {}
        assigned_accounts = 0
        already_configured_accounts = 0
        for account in account_rows:
            account_index = account["account_index"]
            aliases = _event_aliases(candidate)
            if known_aliases_by_account[account_index].intersection(aliases):
                already_configured_accounts += 1
                continue
            if len(account["add"]) + len(account["canary"]) >= max_add_per_account:
                account_reasons[account_index] = "account_add_limit_reached"
                continue
            admission = _account_admission(candidate, account_index)
            if admission is not None and admission[0] == "reject":
                account_reasons[account_index] = (
                    admission[1][0]
                    if admission[1]
                    else "account_admission_rejected"
                )
                continue
            if admission is None:
                depth_reason = _account_depth_rejection(
                    candidate,
                    min_front_bid_notional_usdc=account[
                        "min_front_bid_notional_usdc"
                    ],
                )
                if depth_reason is not None:
                    account_reasons[account_index] = depth_reason
                    continue
            target = account["canary"] if admission and admission[0] == "canary" else account["add"]
            target.append(
                _proposal_row(
                    candidate,
                    action=("canary" if admission and admission[0] == "canary" else "add"),
                    reason_codes=(
                        tuple(admission[1])
                        if admission and admission[0] == "canary"
                        else (
                            "verified_low_risk_efficient",
                            "independent_account_selection",
                        )
                    ),
                )
            )
            known_aliases_by_account[account_index].update(aliases)
            assigned_accounts += 1
        if assigned_accounts == 0 and already_configured_accounts == 0:
            unassigned.append(
                {
                    **_public_market_fields(candidate),
                    **_candidate_metrics(candidate),
                    "reason_codes_by_account": {
                        str(index): reason
                        for index, reason in sorted(account_reasons.items())
                    },
                }
            )

    output_accounts: list[dict[str, Any]] = []
    for account in account_rows:
        retirement_candidates = sorted(
            account["review"],
            key=_retirement_rank,
            reverse=True,
        )
        account["replace"] = [
            _replacement_row(
                account_index=account["account_index"],
                generated_at=generated_at,
                retire=retire,
                add=add,
                min_front_bid_notional_usdc=account[
                    "min_front_bid_notional_usdc"
                ],
            )
            for add, retire in zip(account["add"], retirement_candidates)
        ]
        enabled_count = sum(
            1 for row in account["current"] if row.get("enabled", True) is not False
        )
        disabled_count = len(account["current"]) - enabled_count
        output_accounts.append(
            {
                "account_index": account["account_index"],
                "config_name": account["config_name"],
                "configured_enabled": enabled_count,
                "configured_disabled": disabled_count,
                "min_front_bid_notional_usdc": round(
                    account["min_front_bid_notional_usdc"], 2
                ),
                "add": account["add"],
                "canary": account["canary"],
                "keep": account["keep"],
                "review": account["review"],
                "replace": account["replace"],
                "disabled_hold": account["disabled_hold"],
            }
        )

    rejected_rows = [
        {
            **_public_market_fields(candidate),
            **_candidate_metrics(candidate),
            "reason_codes": candidate_rejections[_canonical_event_key(candidate)],
        }
        for candidate in unique_candidates
        if _canonical_event_key(candidate) in candidate_rejections
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "proposal_only",
        "status": "ready",
        "generated_at": generated_at,
        "observer_generated_at": observer_generated_at,
        "observer_age_sec": round(max(0.0, observer_age), 1),
        "safety": safety,
        "policy": policy,
        "accounts": output_accounts,
        "rejected_candidates": rejected_rows,
        "unassigned_candidates": unassigned,
        "summary": {
            "accounts": len(output_accounts),
            "candidate_count": len(unique_candidates),
            "globally_eligible_candidates": len(globally_eligible),
            "planned_additions": sum(len(row["add"]) for row in output_accounts),
            "planned_canaries": sum(len(row["canary"]) for row in output_accounts),
            "planned_replacements": sum(
                len(row["replace"]) for row in output_accounts
            ),
            "kept_markets": sum(len(row["keep"]) for row in output_accounts),
            "review_markets": sum(len(row["review"]) for row in output_accounts),
            "operator_disabled_markets": sum(
                len(row["disabled_hold"]) for row in output_accounts
            ),
            "rejected_candidates": len(rejected_rows),
            "unassigned_candidates": len(unassigned),
        },
    }


def _write_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False),
        encoding="utf-8",
    )
    temporary.replace(path)


def refresh_stable_rotation_proposal(
    data_dir: Path,
    config_dir: Path,
    observer: Mapping[str, Any],
    *,
    now_ts: float | None = None,
) -> dict[str, Any] | None:
    accounts = load_stable_account_configs(config_dir)
    if not accounts:
        return None
    proposal = build_stable_rotation_proposal(observer, accounts, now_ts=now_ts)
    _write_atomic(data_dir / OUTPUT_NAME, proposal)
    return {
        "status": proposal["status"],
        "output": OUTPUT_NAME,
        **proposal["summary"],
    }


def write_blocked_stable_rotation_proposal(
    data_dir: Path,
    observer: Mapping[str, Any],
    error: Exception,
) -> dict[str, Any]:
    generated_at = time.time()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "mode": "proposal_only",
        "status": "blocked",
        "reason": "planner_input_error",
        "error_type": type(error).__name__,
        "generated_at": generated_at,
        "observer_generated_at": _number(observer.get("generated_at"), 0.0),
        "safety": {
            "proposal_only": True,
            "runtime_config_writes": False,
            "runtime_commands": False,
            "trading_actions": False,
            "requires_manual_review": True,
        },
        "accounts": [],
        "rejected_candidates": [],
        "unassigned_candidates": [],
        "summary": {
            "accounts": 0,
            "candidate_count": 0,
            "planned_additions": 0,
            "planned_canaries": 0,
            "planned_replacements": 0,
        },
    }
    _write_atomic(data_dir / OUTPUT_NAME, payload)
    return {
        "status": "blocked",
        "output": OUTPUT_NAME,
        "error_type": type(error).__name__,
        **payload["summary"],
    }
