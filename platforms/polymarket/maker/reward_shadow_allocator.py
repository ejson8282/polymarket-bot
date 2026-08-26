"""Read-only LP budget comparison based on executable reward evidence."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping


OUTPUT_NAME = "reward_shadow_budget.json"
SCHEMA_VERSION = 1


def _number(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if math.isfinite(result) else default


def _account_admission(row: Mapping[str, Any], account_index: int) -> str:
    for admission in row.get("account_admission") or []:
        if not isinstance(admission, Mapping):
            continue
        if int(_number(admission.get("account_index"), -1)) == account_index:
            return str(admission.get("level") or "reject")
    return "reject"


def build_shadow_budget(observer_state: Mapping[str, Any]) -> dict[str, Any]:
    """Score marginal LP rewards without changing live dynamic budgets."""

    by_account: dict[int, list[dict[str, Any]]] = {}
    for candidate in observer_state.get("candidates") or []:
        if not isinstance(candidate, Mapping):
            continue
        daily_reward = max(0.0, _number(candidate.get("daily_reward_usd")))
        fill_risk = min(100.0, max(0.0, _number(candidate.get("fill_risk"), 100.0)))
        uptime = min(
            1.0,
            max(0.0, _number(candidate.get("eligible_sample_ratio"), 0.0)),
        )
        scoring_ratio_raw = candidate.get("scoring_sample_ratio")
        scoring_factor = (
            min(1.0, max(0.0, _number(scoring_ratio_raw)))
            if scoring_ratio_raw is not None
            else 0.5
        )
        for execution in candidate.get("account_execution") or []:
            if not isinstance(execution, Mapping):
                continue
            account_index = int(_number(execution.get("account_index"), -1))
            if account_index <= 0:
                continue
            admission = _account_admission(candidate, account_index)
            executable_q = max(0.0, _number(execution.get("executable_q_min")))
            target_shares = max(0.0, _number(execution.get("target_shares")))
            capital = max(
                0.0,
                _number(execution.get("collateral_required_usdc")),
            )
            actual_share_raw = execution.get("actual_reward_share_pct")
            if actual_share_raw is not None:
                reward_share = max(0.0, _number(actual_share_raw)) / 100.0
                reward_source = "official_current_percentage"
            else:
                reward_share = max(
                    0.0,
                    _number(execution.get("executable_share_pct")),
                ) / 100.0
                reward_source = "executable_quote_model"
            calibrated_reward = daily_reward * reward_share
            calibration_ratio_raw = candidate.get("earnings_calibration_ratio")
            calibration_scopes = int(
                _number(candidate.get("earnings_calibration_scopes"), 0)
            )
            calibration_ratio = (
                min(4.0, max(0.0, _number(calibration_ratio_raw)))
                if calibration_ratio_raw is not None and calibration_scopes > 0
                else None
            )
            if reward_source == "executable_quote_model" and calibration_ratio is not None:
                calibrated_reward *= calibration_ratio
                reward_source = "official_earnings_calibrated_model"
            q_efficiency = (
                min(1.0, executable_q / target_shares)
                if target_shares > 0
                else 0.0
            )
            front_depth = max(
                0.0,
                _number(execution.get("min_front_bid_notional_usd")),
            )
            required_depth = max(
                1.0,
                _number(execution.get("min_front_bid_notional_usdc"), 1.0),
            )
            depth_factor = min(1.0, front_depth / required_depth)
            risk_factor = max(0.0, 1.0 - fill_risk / 100.0)
            marginal_roi = calibrated_reward / capital if capital > 0 else 0.0
            evidence_factor = uptime * scoring_factor
            score = (
                marginal_roi
                * evidence_factor
                * q_efficiency
                * depth_factor
                * risk_factor
            )
            eligible = bool(
                admission == "full"
                and execution.get("executable") is True
                and executable_q > 0
                and score > 0
            )
            by_account.setdefault(account_index, []).append(
                {
                    "condition_id": str(candidate.get("condition_id") or ""),
                    "token_id": str(candidate.get("token_id") or ""),
                    "paired_token_id": str(candidate.get("paired_token_id") or ""),
                    "question": str(candidate.get("question") or ""),
                    "admission_level": admission,
                    "eligible_for_shadow_allocation": eligible,
                    "calibrated_daily_reward_usd": round(calibrated_reward, 6),
                    "reward_evidence_source": reward_source,
                    "earnings_calibration_ratio": (
                        round(calibration_ratio, 6)
                        if calibration_ratio is not None
                        else None
                    ),
                    "eligible_uptime": round(uptime, 4),
                    "scoring_uptime": (
                        round(_number(scoring_ratio_raw), 4)
                        if scoring_ratio_raw is not None
                        else None
                    ),
                    "q_efficiency": round(q_efficiency, 6),
                    "depth_factor": round(depth_factor, 6),
                    "fill_risk": round(fill_risk, 2),
                    "marginal_reward_per_usdc": round(marginal_roi, 8),
                    "shadow_score": round(score, 10),
                    "current_budget_pct": round(
                        _number(execution.get("budget_pct")),
                        6,
                    ),
                }
            )

    accounts: list[dict[str, Any]] = []
    for account_index, rows in sorted(by_account.items()):
        eligible_total = sum(
            row["shadow_score"]
            for row in rows
            if row["eligible_for_shadow_allocation"]
        )
        for row in rows:
            row["suggested_budget_pct"] = round(
                row["shadow_score"] / eligible_total
                if row["eligible_for_shadow_allocation"] and eligible_total > 0
                else 0.0,
                6,
            )
        rows.sort(
            key=lambda row: (
                row["eligible_for_shadow_allocation"],
                row["shadow_score"],
            ),
            reverse=True,
        )
        accounts.append(
            {
                "account_index": account_index,
                "suggestions": rows,
                "eligible_markets": sum(
                    row["eligible_for_shadow_allocation"] for row in rows
                ),
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "shadow_only",
        "source_observer_generated_at": observer_state.get("generated_at"),
        "production_dynamic_budget_changed": False,
        "runtime_config_writes": False,
        "runtime_commands": False,
        "trading_actions": False,
        "cross_account_duplicate_events": "allowed",
        "accounts": accounts,
    }


def write_shadow_budget(
    data_dir: Path,
    observer_state: dict[str, Any],
) -> dict[str, Any]:
    payload = build_shadow_budget(observer_state)
    path = data_dir / OUTPUT_NAME
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(path)
    observer_state["shadow_budget"] = {
        "output": path.name,
        "mode": "shadow_only",
        "accounts": len(payload["accounts"]),
        "production_dynamic_budget_changed": False,
    }
    return payload
