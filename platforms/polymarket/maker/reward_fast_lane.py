"""Read-only state machine for newly discovered Polymarket reward pools."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any, Mapping


STATE_NAME = "reward_fast_lane_state.json"
STATE_VERSION = 3
CANARY_EXECUTABLE_SAMPLES = 2
COMPETITION_DROP_RATIO = 0.30
MISSING_RETENTION_SEC = 7 * 24 * 60 * 60
FORCED_EVALUATION_WINDOW_SEC = 60 * 60


def _market_key(row: Mapping[str, Any]) -> str:
    condition_id = str(row.get("condition_id") or "").strip().lower()
    if condition_id:
        return f"condition:{condition_id}"
    tokens = sorted(
        {
            str(row.get("token_id") or "").strip(),
            str(row.get("paired_token_id") or "").strip(),
        }
        - {""}
    )
    return "pair:" + ":".join(tokens) if tokens else ""


def _fingerprint(row: Mapping[str, Any]) -> str:
    payload = {
        "daily_reward_usd": row.get("daily_reward_usd"),
        "rewards_min_size_shares": row.get("rewards_min_size_shares"),
        "rewards_max_spread": row.get("rewards_max_spread"),
        "reward_terms": row.get("reward_terms") or [],
        "market_end_ts": row.get("market_end_ts"),
    }
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_state(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def forced_condition_ids(data_dir: Path, *, now_ts: float) -> set[str]:
    """Return fresh watchlist conditions that must bypass observer top-K."""

    state = _read_state(data_dir / STATE_NAME)
    markets = state.get("markets")
    if not isinstance(markets, Mapping):
        return set()
    result: set[str] = set()
    for row in markets.values():
        if not isinstance(row, Mapping):
            continue
        try:
            last_seen = float(row.get("last_seen_at") or 0)
        except (TypeError, ValueError):
            continue
        if now_ts - last_seen > MISSING_RETENTION_SEC:
            continue
        if row.get("force_next_poll") is not True:
            continue
        condition_id = str(row.get("condition_id") or "").strip().lower()
        if condition_id:
            result.add(condition_id)
    return result


def _account_admission_levels(row: Mapping[str, Any]) -> dict[int, str]:
    levels: dict[int, str] = {}
    for admission in row.get("account_admission") or []:
        if not isinstance(admission, Mapping):
            continue
        try:
            account_index = int(admission.get("account_index"))
        except (TypeError, ValueError):
            continue
        if account_index > 0:
            levels[account_index] = str(
                admission.get("level") or "reject"
            ).lower()
    return levels


def _account_execution_evidence(
    row: Mapping[str, Any],
) -> dict[int, dict[str, Any]]:
    evidence: dict[int, dict[str, Any]] = {}
    for execution in row.get("account_execution") or []:
        if not isinstance(execution, Mapping):
            continue
        try:
            account_index = int(execution.get("account_index"))
        except (TypeError, ValueError):
            continue
        if account_index <= 0:
            continue
        account_uid_key = str(
            execution.get("account_uid_key") or ""
        ).strip().lower()
        host_id = str(execution.get("host_id") or "").strip().lower()
        identity_valid = bool(
            re.fullmatch(r"[0-9a-f]{16}", account_uid_key) and host_id
        )
        try:
            observed_q = float(execution.get("observed_q_min"))
        except (TypeError, ValueError):
            observed_q = 0.0
        scoring_q_validated = bool(
            identity_valid
            and execution.get("configured") is True
            and execution.get("official_scoring") is True
            and re.fullmatch(
                r"[0-9a-f]{64}",
                str(execution.get("scoring_sample_id") or "").strip().lower(),
            )
            and observed_q > 0
        )
        current = {
            "account_uid_key": account_uid_key,
            "host_id": host_id,
            "executable": identity_valid and execution.get("executable") is True,
            "scoring_q_validated": scoring_q_validated,
        }
        previous = evidence.get(account_index)
        if previous is None:
            evidence[account_index] = current
            continue
        if (
            previous["account_uid_key"],
            previous["host_id"],
        ) != (account_uid_key, host_id):
            evidence[account_index] = {
                "account_uid_key": "",
                "host_id": "",
                "executable": False,
                "scoring_q_validated": False,
            }
            continue
        previous["executable"] = bool(
            previous["executable"] or current["executable"]
        )
        previous["scoring_q_validated"] = bool(
            previous["scoring_q_validated"]
            or current["scoring_q_validated"]
        )
    return evidence


def update_fast_lane(
    data_dir: Path,
    observer_state: dict[str, Any],
    *,
    now_ts: float,
) -> dict[str, Any]:
    """Update watchlist/canary evidence without changing runtime configuration."""

    path = data_dir / STATE_NAME
    previous_state = _read_state(path)
    bootstrap = int(previous_state.get("version") or 0) != STATE_VERSION
    previous_markets = previous_state.get("markets")
    if not isinstance(previous_markets, Mapping):
        previous_markets = {}

    rows = [
        row
        for group in (
            observer_state.get("candidates") or [],
            observer_state.get("unassessed_candidates") or [],
        )
        for row in group
        if isinstance(row, dict)
    ]
    current: dict[str, dict[str, Any]] = {}
    candidate_by_key = {
        _market_key(row): row
        for row in observer_state.get("candidates") or []
        if isinstance(row, dict) and _market_key(row)
    }
    for row in rows:
        key = _market_key(row)
        if not key:
            continue
        previous = previous_markets.get(key)
        if not isinstance(previous, Mapping):
            previous = {}
        fingerprint = _fingerprint(row)
        previous_fingerprint = str(previous.get("reward_fingerprint") or "")
        config_changed = bool(
            previous_fingerprint and previous_fingerprint != fingerprint
        )
        baseline_seed = bool(bootstrap and not previous_fingerprint)
        is_new = bool(not bootstrap and not previous_fingerprint)
        default_admission = str(row.get("admission_level") or "reject").lower()
        admission_levels = _account_admission_levels(row)
        execution_evidence = _account_execution_evidence(row)
        previous_accounts = previous.get("accounts")
        if not isinstance(previous_accounts, Mapping):
            previous_accounts = {}
        account_states: dict[str, dict[str, Any]] = {}
        for account_index, execution in sorted(
            execution_evidence.items()
        ):
            executable = bool(execution.get("executable"))
            scoring_q_validated = bool(
                execution.get("scoring_q_validated")
            )
            account_uid_key = str(
                execution.get("account_uid_key") or ""
            ).strip().lower()
            host_id = str(execution.get("host_id") or "").strip().lower()
            previous_account = previous_accounts.get(str(account_index))
            if not isinstance(previous_account, Mapping):
                previous_account = {}
            same_identity = bool(
                account_uid_key
                and host_id
                and str(
                    previous_account.get("account_uid_key") or ""
                ).strip().lower()
                == account_uid_key
                and str(previous_account.get("host_id") or "").strip().lower()
                == host_id
            )
            if executable and not config_changed and same_identity:
                consecutive = int(
                    previous_account.get("consecutive_executable_samples") or 0
                ) + 1
            elif executable:
                consecutive = 1
            else:
                consecutive = 0
            admission_level = admission_levels.get(
                account_index,
                default_admission,
            )
            admission_eligible = admission_level in {"canary", "full"}
            if scoring_q_validated and admission_eligible:
                account_stage = "expansion_validated"
            elif consecutive >= CANARY_EXECUTABLE_SAMPLES and admission_eligible:
                account_stage = "canary_proposal"
            else:
                account_stage = "watchlist"
            account_states[str(account_index)] = {
                "account_index": account_index,
                "account_uid_key": account_uid_key,
                "host_id": host_id,
                "admission_level": admission_level,
                "stage": account_stage,
                "consecutive_executable_samples": consecutive,
                "scoring_q_validated": scoring_q_validated,
                "canary_proposal_eligible": account_stage
                in {"canary_proposal", "expansion_validated"},
                "expansion_eligible": account_stage == "expansion_validated",
            }

        stage_rank = {
            "watchlist": 0,
            "canary_proposal": 1,
            "expansion_validated": 2,
        }
        stage = max(
            (
                str(account.get("stage") or "watchlist")
                for account in account_states.values()
            ),
            key=lambda value: stage_rank.get(value, 0),
            default="watchlist",
        )
        consecutive = max(
            (
                int(account.get("consecutive_executable_samples") or 0)
                for account in account_states.values()
            ),
            default=0,
        )
        scoring_q_validated = any(
            account.get("scoring_q_validated") is True
            for account in account_states.values()
        )

        try:
            competition_q = float(row.get("competition_score_estimate"))
        except (TypeError, ValueError):
            competition_q = None
        try:
            previous_competition_q = float(previous.get("competition_q"))
        except (TypeError, ValueError):
            previous_competition_q = None
        competition_drop = bool(
            competition_q is not None
            and previous_competition_q is not None
            and previous_competition_q > 0
            and competition_q
            <= previous_competition_q * (1.0 - COMPETITION_DROP_RATIO)
        )

        triggers: list[str] = []
        if baseline_seed:
            triggers.append("baseline_seeded")
        if is_new:
            triggers.append("new_reward_config")
        if config_changed:
            triggers.append("reward_config_changed")
        if competition_drop:
            triggers.append("competition_significantly_lower")
        if stage == "canary_proposal" and str(previous.get("stage") or "") != stage:
            triggers.append("consecutive_executable_samples_met")
        if (
            scoring_q_validated
            and str(previous.get("stage") or "") != stage
        ):
            triggers.append("official_scoring_and_q_validated")

        first_seen_at = (
            now_ts
            if is_new or config_changed or baseline_seed
            else float(previous.get("first_seen_at") or now_ts)
        )
        force_next_poll = bool(
            not baseline_seed
            and now_ts - first_seen_at <= FORCED_EVALUATION_WINDOW_SEC
            and (
                is_new
                or config_changed
                or any(
                    account.get("stage") == "watchlist"
                    for account in account_states.values()
                )
                or not account_states
            )
        )
        canary_account_indexes = sorted(
            int(account["account_index"])
            for account in account_states.values()
            if account.get("admission_level") == "canary"
            and account.get("canary_proposal_eligible") is True
        )
        expansion_account_indexes = sorted(
            int(account["account_index"])
            for account in account_states.values()
            if account.get("expansion_eligible") is True
        )
        current[key] = {
            "condition_id": str(row.get("condition_id") or "").strip().lower(),
            "token_id": str(row.get("token_id") or "").strip(),
            "paired_token_id": str(row.get("paired_token_id") or "").strip(),
            "question": str(row.get("question") or ""),
            "slug": str(row.get("slug") or ""),
            "reward_fingerprint": fingerprint,
            "first_seen_at": first_seen_at,
            "last_seen_at": now_ts,
            "assessment_status": str(
                row.get("assessment_status")
                or ("assessed" if key in candidate_by_key else "unassessed")
            ),
            "stage": stage,
            "consecutive_executable_samples": consecutive,
            "scoring_q_validated": scoring_q_validated,
            "accounts": account_states,
            "competition_q": competition_q,
            "force_next_poll": force_next_poll,
            "trigger_reasons": triggers,
            "canary_proposal_eligible": stage in {
                "canary_proposal",
                "expansion_validated",
            },
            "expansion_eligible": stage == "expansion_validated",
            "canary_proposal_eligible_account_indexes": canary_account_indexes,
            "expansion_eligible_account_indexes": expansion_account_indexes,
        }

        candidate = candidate_by_key.get(key)
        if candidate is not None:
            candidate["new_pool_fast_lane"] = {
                "stage": stage,
                "consecutive_executable_samples": consecutive,
                "scoring_q_validated": scoring_q_validated,
                "accounts": account_states,
                "trigger_reasons": triggers,
            }
            candidate["canary_proposal_eligible_account_indexes"] = (
                canary_account_indexes
            )
            candidate["canary_proposal_eligible"] = bool(canary_account_indexes)

    for key, row in previous_markets.items():
        if key in current or not isinstance(row, Mapping):
            continue
        try:
            last_seen = float(row.get("last_seen_at") or 0)
        except (TypeError, ValueError):
            continue
        if now_ts - last_seen <= MISSING_RETENTION_SEC:
            retained = dict(row)
            retained["assessment_status"] = "missing"
            retained["canary_proposal_eligible"] = False
            retained["expansion_eligible"] = False
            retained["force_next_poll"] = False
            current[str(key)] = retained

    payload = {
        "version": STATE_VERSION,
        "mode": "watchlist_and_proposal_only",
        "generated_at": now_ts,
        "poll_interval_target_sec": 300,
        "canary_executable_samples_required": CANARY_EXECUTABLE_SAMPLES,
        "runtime_config_writes": False,
        "runtime_commands": False,
        "trading_actions": False,
        "markets": current,
        "summary": {
            "watchlist": sum(row["stage"] == "watchlist" for row in current.values()),
            "canary_proposal": sum(
                row["stage"] == "canary_proposal" for row in current.values()
            ),
            "expansion_validated": sum(
                row["stage"] == "expansion_validated" for row in current.values()
            ),
        },
    }
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)
    observer_state["new_pool_fast_lane"] = {
        "output": path.name,
        **payload["summary"],
    }
    return payload
