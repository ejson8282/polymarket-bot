from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.maker.intents import utc_now


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def build_research_state(plan_state: dict[str, Any]) -> dict[str, Any]:
    rows = [_score_plan(plan) for plan in plan_state.get("plans") or [] if isinstance(plan, dict)]
    rows.sort(key=lambda row: row["suitability_score"], reverse=True)
    return {
        "ts": utc_now(),
        "mode": "market_research",
        "source_ts": plan_state.get("ts"),
        "summary": {
            "markets": len(rows),
            "tradable_now": sum(1 for row in rows if row["bucket"] == "tradable"),
            "watchlist": sum(1 for row in rows if row["bucket"] == "watchlist"),
            "avoid": sum(1 for row in rows if row["bucket"] == "avoid"),
        },
        "markets": rows,
    }


def _score_plan(plan: dict[str, Any]) -> dict[str, Any]:
    market = plan.get("market") if isinstance(plan.get("market"), dict) else {}
    hourly = _dec(market.get("hourly_rate"))
    spread_threshold = _dec(market.get("spread_threshold"))
    share_threshold = max(_dec(market.get("share_threshold")), Decimal("1"))
    mid = _dec(plan.get("mid"))
    best_bid = _dec(plan.get("best_yes_bid"))
    best_ask = _dec(plan.get("best_yes_ask"))
    spread = best_ask - best_bid if best_ask > best_bid else Decimal("0")
    quote_legs = len(plan.get("yes_quotes") or []) + len(plan.get("no_quotes") or [])

    reward_efficiency = hourly / share_threshold
    spread_room = spread_threshold - spread if spread > 0 else Decimal("0")
    book_quality = Decimal("1") if best_bid > 0 and best_ask > 0 and best_ask > best_bid else Decimal("0")
    quote_score = Decimal(quote_legs) * Decimal("0.5")
    mid_penalty = Decimal("1") if Decimal("0.40") <= mid <= Decimal("0.60") else Decimal("0")
    variant_penalty = Decimal("2") if str(market.get("market_variant") or "") == "CRYPTO_UP_DOWN" else Decimal("0")
    skip_penalty = Decimal("1.5") if plan.get("skip_reason") else Decimal("0")

    score = reward_efficiency + max(Decimal("0"), spread_room * Decimal("10")) + book_quality + quote_score
    score -= mid_penalty + variant_penalty + skip_penalty
    bucket = "tradable" if plan.get("can_quote") and score >= Decimal("1") else "watchlist" if score >= Decimal("0") else "avoid"

    return {
        "market_id": market.get("id"),
        "title": market.get("title"),
        "variant": market.get("market_variant"),
        "hourly_rate": str(hourly),
        "share_threshold": str(share_threshold),
        "spread_threshold": str(spread_threshold),
        "book_spread": str(spread),
        "mid": str(mid),
        "quote_legs": quote_legs,
        "can_quote": bool(plan.get("can_quote")),
        "skip_reason": plan.get("skip_reason") or "",
        "suitability_score": float(score),
        "bucket": bucket,
        "notes": _notes(plan, score, mid, spread),
    }


def _notes(plan: dict[str, Any], score: Decimal, mid: Decimal, spread: Decimal) -> str:
    notes: list[str] = []
    if plan.get("can_quote"):
        notes.append("quotes available")
    if plan.get("skip_reason"):
        notes.append(str(plan.get("skip_reason")))
    if Decimal("0.40") <= mid <= Decimal("0.60"):
        notes.append("mid near 50/50")
    if spread <= 0:
        notes.append("no reliable top-of-book spread")
    notes.append(f"score={score:.2f}")
    return "; ".join(notes)


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict.fun market suitability research.")
    parser.add_argument("--plans", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    state = build_research_state(load_json(Path(args.plans).resolve()))
    write_json(Path(args.out).resolve(), state)
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
