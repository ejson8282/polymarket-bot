"""Select a reviewed aggressive-LP market universe from observer state.

This module never signs, posts, cancels, or changes a running configuration.
It only validates a fresh read-only observer snapshot and renders a candidate
market-universe document for later review and deployment.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


class AggressiveSelectionError(ValueError):
    pass


def _number(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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
    if spread <= 0 or quote_size <= 0:
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
        "price_tick": 0.01,
        "min_distance_from_best_bid": 0.01,
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
    }


def select_aggressive_market_universe(
    observer: Mapping[str, Any],
    *,
    principal_usdc: float,
    limit: int = 1,
    now_ts: float | None = None,
    max_age_sec: float = 900.0,
    max_fill_risk: float = 65.0,
    min_stability_score: float = 70.0,
) -> dict[str, Any]:
    if principal_usdc <= 0:
        raise AggressiveSelectionError("principal_usdc must be positive")
    if limit <= 0:
        raise AggressiveSelectionError("limit must be positive")
    generated_at = _number(observer.get("generated_at"), 0.0)
    age = (time.time() if now_ts is None else now_ts) - generated_at
    if generated_at <= 0 or age < -30 or age > max_age_sec:
        raise AggressiveSelectionError("reward observer snapshot is stale")

    candidates = observer.get("candidates")
    if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
        raise AggressiveSelectionError("reward observer candidates are invalid")

    eligible: list[Mapping[str, Any]] = []
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
        if _number(row.get("fill_risk"), 100.0) >= max_fill_risk:
            continue
        if _number(row.get("stability_score"), 0.0) < min_stability_score:
            continue
        probe_capital = _number(row.get("probe_capital_usd"), 0.0)
        if probe_capital <= 0 or probe_capital > principal_usdc * 1.05:
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
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Render a review-only aggressive LP market universe"
    )
    parser.add_argument("--observer", type=Path, required=True)
    parser.add_argument("--principal-usdc", type=float, required=True)
    parser.add_argument("--limit", type=int, default=1)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    try:
        observer = json.loads(args.observer.read_text(encoding="utf-8"))
        if not isinstance(observer, dict):
            raise AggressiveSelectionError("reward observer state must be an object")
        payload = select_aggressive_market_universe(
            observer,
            principal_usdc=args.principal_usdc,
            limit=args.limit,
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
