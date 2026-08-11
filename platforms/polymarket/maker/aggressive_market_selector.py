"""Select a reviewed aggressive-LP market universe from observer state.

This module never signs, posts, cancels, or changes a running configuration.
It only validates a fresh read-only observer snapshot and renders a candidate
market-universe document for later review and deployment.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from .sponsored_guard import SponsoredRiskGuard
except ImportError:  # pragma: no cover - direct script execution
    from sponsored_guard import SponsoredRiskGuard


class AggressiveSelectionError(ValueError):
    pass


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _finite_optional_number(value: Any) -> float | None:
    number = _number(value, float("nan"))
    return round(number, 2) if math.isfinite(number) else None


def _candidate_rank(row: Mapping[str, Any]) -> tuple[float, float, float, float]:
    return (
        _number(row.get("risk_adjusted_daily_roi_pct"), -1.0),
        _number(row.get("estimated_daily_gross_usd"), -1.0),
        _number(row.get("stability_score"), -1.0),
        -_number(row.get("fill_risk"), 100.0),
    )


def _market_row(candidate: Mapping[str, Any]) -> dict[str, Any]:
    token_id = str(candidate.get("token_id") or "").strip()
    paired_token_id = str(candidate.get("paired_token_id") or "").strip()
    if not token_id.isdigit() or not paired_token_id.isdigit():
        raise AggressiveSelectionError("candidate token pair is invalid")
    if token_id == paired_token_id:
        raise AggressiveSelectionError("candidate token pair is self-referential")
    spread = _number(candidate.get("rewards_max_spread"), 0.0)
    quote_size = _number(candidate.get("probe_shares_each_side"), 0.0)
    tick = _number(candidate.get("yes_tick_size"), 0.0)
    if spread <= 0 or quote_size <= 0 or tick <= 0:
        raise AggressiveSelectionError("candidate quote parameters are invalid")
    fill_risk = _number(candidate.get("fill_risk"), 100.0)
    risk = "low" if fill_risk < 35 else "mid"
    return {
        "token_id": token_id,
        "paired_token_id": paired_token_id,
        "side": "YES",
        "max_incentive_spread": round(
            spread,
            6,
        ),
        "price_tick": round(tick, 6),
        "min_distance_from_best_bid": round(tick, 6),
        "min_distance_ticks": 1,
        "quote_size": round(
            quote_size,
            4,
        ),
        "risk": risk,
        "enabled": True,
        "source": "aggressive_observer_selected",
        "eligibility_managed": True,
        "eligibility_base_risk": risk,
        "condition_id": str(candidate.get("condition_id") or "").strip().lower(),
        "slug": str(candidate.get("slug") or "").strip(),
        "question": str(candidate.get("question") or "").strip(),
        "game_start_ts": _number(candidate.get("game_start_ts"), 0.0),
        "market_end_ts": _number(candidate.get("market_end_ts"), 0.0),
    }


def _market_eligibility_rejection(
    candidate: Mapping[str, Any],
    *,
    now_ts: float,
) -> str | None:
    if candidate.get("market_active") is not True:
        return "market_not_active"
    if candidate.get("market_closed") is not False:
        return "market_closed_or_unknown"
    if candidate.get("market_archived") is not False:
        return "market_archived_or_unknown"
    if candidate.get("accepting_orders") is not True:
        return "market_not_accepting_orders"
    market_end_ts = _number(candidate.get("market_end_ts"), float("nan"))
    if not math.isfinite(market_end_ts) or market_end_ts <= 0:
        return "market_end_unavailable"
    if market_end_ts <= now_ts:
        return "market_expired"
    return None


def _front_depth_rejection(
    candidate: Mapping[str, Any],
    *,
    min_front_bid_notional_usdc: float,
    now_ts: float,
    max_depth_age_sec: float,
) -> str | None:
    if str(candidate.get("front_depth_status") or "") != "verified":
        return "front_depth_unavailable"
    observed_at = _number(candidate.get("front_depth_observed_at"), 0.0)
    if not math.isfinite(observed_at):
        return "front_depth_unavailable"
    age = now_ts - observed_at
    if observed_at <= 0 or age < -30 or age > max_depth_age_sec:
        return "front_depth_stale"
    yes_depth = _number(candidate.get("yes_front_bid_notional_usd"), -1.0)
    no_depth = _number(candidate.get("no_front_bid_notional_usd"), -1.0)
    if not math.isfinite(yes_depth) or not math.isfinite(no_depth):
        return "front_depth_unavailable"
    if min(yes_depth, no_depth) < min_front_bid_notional_usdc:
        return "front_depth_below_min"
    return None


def _sponsored_feasibility_rejection(
    candidate: Mapping[str, Any],
    assessments: Mapping[str, Any],
    *,
    principal_usdc: float,
    quote_budget_pct: float,
    now_ts: float,
    max_age_sec: float,
) -> tuple[str | None, dict[str, Any]]:
    condition_id = str(candidate.get("condition_id") or "").strip().lower()
    assessment = assessments.get(condition_id)
    detail = {
        "token_id": str(candidate.get("token_id") or ""),
        "condition_id": condition_id,
    }
    if not condition_id or not isinstance(assessment, Mapping):
        return "sponsored_risk_unavailable", detail

    assessed_at = _number(assessment.get("assessed_at"), 0.0)
    age = now_ts - assessed_at
    if assessed_at <= 0 or age < -30 or age > max_age_sec:
        return "sponsored_risk_stale", detail

    status = str(assessment.get("status") or "unknown").strip().lower()
    size_cap = _number(assessment.get("size_cap"), float("nan"))
    reasons = [str(reason) for reason in (assessment.get("reasons") or [])]
    detail.update(
        {
            "status": status,
            "size_cap": round(size_cap, 4) if math.isfinite(size_cap) else None,
            "reasons": reasons,
        }
    )
    if status not in {"safe", "caution", "disabled"}:
        return f"sponsored_risk_{status or 'unknown'}", detail
    if not math.isfinite(size_cap) or size_cap < 0 or size_cap > 1:
        return "sponsored_size_cap_invalid", detail

    required_shares = _number(candidate.get("rewards_min_size_shares"), 0.0)
    if required_shares <= 0:
        return "reward_min_size_unavailable", detail
    capped_shares = math.floor(
        max(0.0, principal_usdc)
        * max(0.0, min(quote_budget_pct, 1.0))
        * size_cap
    )
    detail.update(
        {
            "required_min_shares": round(required_shares, 4),
            "capped_quote_shares": capped_shares,
        }
    )
    if capped_shares < required_shares:
        return "sponsored_size_cap_below_reward_min", detail
    return None, detail


async def _fetch_sponsored_assessments(
    observer: Mapping[str, Any],
    config: Mapping[str, Any] | None,
) -> dict[str, dict[str, Any]]:
    guard = SponsoredRiskGuard(dict(config or {}))
    refreshed = await guard.refresh(force=True)
    if guard.enabled and not refreshed:
        raise AggressiveSelectionError("official sponsored reward source is unavailable")

    now_ts = time.time()
    assessments: dict[str, dict[str, Any]] = {}
    for row in observer.get("candidates") or []:
        if not isinstance(row, Mapping):
            continue
        condition_id = str(row.get("condition_id") or "").strip().lower()
        if not condition_id or condition_id in assessments:
            continue
        assessments[condition_id] = guard.assess(
            condition_id,
            for_admission=True,
            now=now_ts,
        )
    return assessments


def _load_sponsored_config(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AggressiveSelectionError("sponsored risk config must be an object")
    nested = payload.get("sponsored_risk_guard")
    if nested is None:
        return payload
    if not isinstance(nested, dict):
        raise AggressiveSelectionError("sponsored_risk_guard config must be an object")
    return nested


def select_aggressive_market_universe(
    observer: Mapping[str, Any],
    *,
    principal_usdc: float,
    min_front_bid_notional_usdc: float,
    limit: int = 1,
    now_ts: float | None = None,
    max_age_sec: float = 900.0,
    max_fill_risk: float = 65.0,
    min_stability_score: float = 70.0,
    max_depth_age_sec: float = 300.0,
    quote_budget_pct: float = 1.0,
    max_sponsored_age_sec: float = 180.0,
) -> dict[str, Any]:
    if not math.isfinite(principal_usdc) or principal_usdc <= 0:
        raise AggressiveSelectionError("principal_usdc must be positive")
    if limit <= 0:
        raise AggressiveSelectionError("limit must be positive")
    if (
        not math.isfinite(min_front_bid_notional_usdc)
        or min_front_bid_notional_usdc <= 0
    ):
        raise AggressiveSelectionError("min_front_bid_notional_usdc must be positive")
    if not math.isfinite(max_depth_age_sec) or max_depth_age_sec <= 0:
        raise AggressiveSelectionError("max_depth_age_sec must be positive")
    if not math.isfinite(quote_budget_pct) or not 0 < quote_budget_pct <= 1:
        raise AggressiveSelectionError("quote_budget_pct must be within (0, 1]")
    if not math.isfinite(max_sponsored_age_sec) or max_sponsored_age_sec <= 0:
        raise AggressiveSelectionError("max_sponsored_age_sec must be positive")
    selection_ts = time.time() if now_ts is None else now_ts
    generated_at = _number(observer.get("generated_at"), 0.0)
    age = selection_ts - generated_at
    if generated_at <= 0 or age < -30 or age > max_age_sec:
        raise AggressiveSelectionError("reward observer snapshot is stale")

    candidates = observer.get("candidates")
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise AggressiveSelectionError("reward observer candidates are invalid")
    sponsored_assessments = observer.get("sponsored_risk_assessments")
    if not isinstance(sponsored_assessments, Mapping):
        raise AggressiveSelectionError("sponsored risk assessments are unavailable")

    eligible: list[Mapping[str, Any]] = []
    eligibility_rejections: list[dict[str, Any]] = []
    depth_rejections: list[dict[str, Any]] = []
    sponsored_rejections: list[dict[str, Any]] = []
    seen_events: set[str] = set()
    for row in sorted(
        (item for item in candidates if isinstance(item, Mapping)),
        key=_candidate_rank,
        reverse=True,
    ):
        if row.get("verification_recommended") is not True:
            continue
        if str(row.get("market_phase") or "").strip().lower() == "live":
            continue
        eligibility_reason = _market_eligibility_rejection(
            row,
            now_ts=selection_ts,
        )
        if eligibility_reason is not None:
            eligibility_rejections.append(
                {
                    "token_id": str(row.get("token_id") or ""),
                    "condition_id": str(row.get("condition_id") or "").strip().lower(),
                    "reason": eligibility_reason,
                    "market_end_ts": _finite_optional_number(row.get("market_end_ts")),
                }
            )
            continue
        if _number(row.get("fill_risk"), 100.0) >= max_fill_risk:
            continue
        if _number(row.get("stability_score"), 0.0) < min_stability_score:
            continue
        probe_capital = _number(row.get("probe_capital_usd"), 0.0)
        if probe_capital <= 0 or probe_capital > principal_usdc * 1.05:
            continue
        sponsored_reason, sponsored_detail = _sponsored_feasibility_rejection(
            row,
            sponsored_assessments,
            principal_usdc=principal_usdc,
            quote_budget_pct=quote_budget_pct,
            now_ts=selection_ts,
            max_age_sec=max_sponsored_age_sec,
        )
        if sponsored_reason is not None:
            sponsored_rejections.append(
                {**sponsored_detail, "reason": sponsored_reason}
            )
            continue
        depth_reason = _front_depth_rejection(
            row,
            min_front_bid_notional_usdc=min_front_bid_notional_usdc,
            now_ts=selection_ts,
            max_depth_age_sec=max_depth_age_sec,
        )
        if depth_reason is not None:
            depth_rejections.append(
                {
                    "token_id": str(row.get("token_id") or ""),
                    "condition_id": str(row.get("condition_id") or "").strip().lower(),
                    "reason": depth_reason,
                    "yes_front_bid_notional_usd": _finite_optional_number(
                        row.get("yes_front_bid_notional_usd")
                    ),
                    "no_front_bid_notional_usd": _finite_optional_number(
                        row.get("no_front_bid_notional_usd")
                    ),
                }
            )
            continue
        event_key = str(row.get("condition_id") or "").strip().lower()
        if not event_key:
            event_key = ":".join(
                sorted(
                    (
                        str(row.get("token_id") or ""),
                        str(row.get("paired_token_id") or ""),
                    )
                )
            )
        if event_key in seen_events:
            continue
        _market_row(row)
        seen_events.add(event_key)
        eligible.append(row)
        if len(eligible) >= limit:
            break

    if not eligible:
        raise AggressiveSelectionError("no eligible aggressive LP market")

    return {
        "schema_version": 1,
        "markets": [_market_row(row) for row in eligible],
        "night_markets": [],
        "build": {
            "source": "reward_observer_state.json",
            "observer_generated_at": generated_at,
            "observer_age_sec": round(max(0.0, age), 1),
            "principal_usdc": round(principal_usdc, 2),
            "selection_limit": limit,
            "selection_mode": "review_only",
            "min_front_bid_notional_usdc": round(min_front_bid_notional_usdc, 2),
            "max_depth_age_sec": round(max_depth_age_sec, 1),
            "quote_budget_pct": round(quote_budget_pct, 4),
            "max_sponsored_age_sec": round(max_sponsored_age_sec, 1),
            "eligibility_rejections": eligibility_rejections,
            "depth_rejections": depth_rejections,
            "sponsored_rejections": sponsored_rejections,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render a review-only aggressive LP market universe"
    )
    parser.add_argument("--observer", type=Path, required=True)
    parser.add_argument("--principal-usdc", type=float, required=True)
    parser.add_argument(
        "--min-front-bid-notional-usdc",
        type=float,
        required=True,
        help="Require each YES/NO leg to meet the runtime front-depth gate",
    )
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--max-depth-age-sec", type=float, default=300.0)
    parser.add_argument("--quote-budget-pct", type=float, default=1.0)
    parser.add_argument("--max-sponsored-age-sec", type=float, default=180.0)
    parser.add_argument(
        "--sponsored-risk-config",
        type=Path,
        help="Optional runtime/base config whose sponsored guard policy must match",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    try:
        observer = json.loads(args.observer.read_text(encoding="utf-8"))
        if not isinstance(observer, dict):
            raise AggressiveSelectionError("reward observer state must be an object")
        sponsored_config = _load_sponsored_config(args.sponsored_risk_config)
        observer = dict(observer)
        observer["sponsored_risk_assessments"] = asyncio.run(
            _fetch_sponsored_assessments(observer, sponsored_config)
        )
        payload = select_aggressive_market_universe(
            observer,
            principal_usdc=args.principal_usdc,
            min_front_bid_notional_usdc=args.min_front_bid_notional_usdc,
            limit=args.limit,
            max_depth_age_sec=args.max_depth_age_sec,
            quote_budget_pct=args.quote_budget_pct,
            max_sponsored_age_sec=args.max_sponsored_age_sec,
        )
    except (OSError, json.JSONDecodeError, AggressiveSelectionError) as exc:
        print(f"ERROR: {exc}")
        return 1

    rendered = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if args.output is None:
        print(rendered, end="")
        return 0
    args.output.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.output.with_suffix(args.output.suffix + ".tmp")
    tmp.write_text(rendered, encoding="utf-8")
    tmp.replace(args.output)
    print(f"Wrote review-only universe: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
