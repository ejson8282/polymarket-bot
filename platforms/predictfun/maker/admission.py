from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable, Protocol, TypeVar


class ScoredMarket(Protocol):
    id: int
    score: Decimal


MarketT = TypeVar("MarketT", bound=ScoredMarket)


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def select_stable_markets(
    candidates: list[MarketT],
    *,
    previous_state: dict[str, Any] | None,
    max_markets: int,
    min_dwell_sec: float,
    replacement_score_margin: Decimal,
    pinned_market_ids: Iterable[int] = (),
    now_ts: float | None = None,
) -> tuple[list[MarketT], dict[str, Any]]:
    """Select markets without churning the active set on small score moves.

    The scanner should provide a candidate pool larger than ``max_markets``.
    Existing markets remain admitted until a materially better candidate is
    available and their minimum dwell time has elapsed. Markets with an open
    position are pinned and never displaced by ranking changes.
    """

    now = float(now_ts if now_ts is not None else datetime.now(timezone.utc).timestamp())
    limit = max(0, int(max_markets))
    if limit == 0:
        return [], _state_payload([], {}, now)

    by_id = {int(market.id): market for market in candidates if int(market.id) > 0}
    ranked = sorted(by_id.values(), key=lambda market: market.score, reverse=True)
    previous = _previous_entries(previous_state)
    pinned = {int(market_id) for market_id in pinned_market_ids if int(market_id) > 0}

    selected_ids: list[int] = []
    admitted_at: dict[int, float] = {}

    for market_id in sorted(pinned):
        if market_id not in by_id:
            continue
        selected_ids.append(market_id)
        admitted_at[market_id] = _admitted_at(previous.get(market_id), now)

    incumbents = [
        market_id
        for market_id in previous
        if market_id in by_id and market_id not in selected_ids
    ]
    incumbents.sort(key=lambda market_id: by_id[market_id].score, reverse=True)
    for market_id in incumbents:
        if len(selected_ids) >= limit and market_id not in pinned:
            break
        selected_ids.append(market_id)
        admitted_at[market_id] = _admitted_at(previous.get(market_id), now)

    for market in ranked:
        market_id = int(market.id)
        if market_id in selected_ids:
            continue
        if len(selected_ids) < limit:
            selected_ids.append(market_id)
            admitted_at[market_id] = now
            continue

        replaceable = [
            current_id
            for current_id in selected_ids
            if current_id not in pinned
            and now - admitted_at.get(current_id, now) >= max(0.0, min_dwell_sec)
        ]
        if not replaceable:
            continue
        worst_id = min(replaceable, key=lambda current_id: by_id[current_id].score)
        if market.score <= by_id[worst_id].score + replacement_score_margin:
            continue
        selected_ids[selected_ids.index(worst_id)] = market_id
        admitted_at.pop(worst_id, None)
        admitted_at[market_id] = now

    selected = [by_id[market_id] for market_id in selected_ids if market_id in by_id]
    selected.sort(key=lambda market: market.score, reverse=True)
    state = _state_payload(selected, admitted_at, now, pinned=pinned)
    return selected, state


def _previous_entries(state: dict[str, Any] | None) -> dict[int, dict[str, Any]]:
    if not isinstance(state, dict):
        return {}
    rows = state.get("markets")
    if not isinstance(rows, dict):
        return {}
    out: dict[int, dict[str, Any]] = {}
    for raw_id, row in rows.items():
        if not isinstance(row, dict):
            continue
        try:
            market_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        if market_id > 0:
            out[market_id] = row
    return out


def _admitted_at(row: dict[str, Any] | None, fallback: float) -> float:
    try:
        return float((row or {}).get("admitted_at_ts") or fallback)
    except (TypeError, ValueError):
        return fallback


def _state_payload(
    selected: list[ScoredMarket],
    admitted_at: dict[int, float],
    now: float,
    *,
    pinned: set[int] | None = None,
) -> dict[str, Any]:
    pinned = pinned or set()
    return {
        "ts": datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "markets": {
            str(market.id): {
                "market_id": int(market.id),
                "score": str(market.score),
                "admitted_at_ts": admitted_at.get(int(market.id), now),
                "last_seen_at_ts": now,
                "pinned_by_position": int(market.id) in pinned,
            }
            for market in selected
        },
        "summary": {
            "selected": len(selected),
            "pinned": sum(1 for market in selected if int(market.id) in pinned),
        },
    }
