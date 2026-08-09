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


def update_simulation(
    *,
    previous_state: dict[str, Any],
    plan_state: dict[str, Any],
    intents_state: dict[str, Any],
    execution_report: dict[str, Any],
    max_fill_size: Decimal,
) -> dict[str, Any]:
    active_orders = {
        str(row.get("intent_id")): row
        for row in previous_state.get("active_orders", [])
        if isinstance(row, dict) and row.get("intent_id")
    }
    fills = [row for row in previous_state.get("fills", []) if isinstance(row, dict)]

    diff = intents_state.get("diff") if isinstance(intents_state.get("diff"), dict) else {}
    successful = {
        (str(row.get("action") or ""), str(row.get("intent_id") or ""))
        for row in execution_report.get("results") or []
        if isinstance(row, dict) and row.get("ok") and row.get("intent_id")
    }
    cancelled_intent_ids = {
        intent_id for action, intent_id in successful if action == "cancel"
    }
    # The simulation state can survive a process or release restart while the
    # dry-run managed-order registry does not. Planner cancels remain
    # authoritative for virtual orders even when there is no registry entry to
    # produce an execution result.
    cancelled_intent_ids.update(
        str(item.get("intent_id"))
        for item in diff.get("cancel") or []
        if isinstance(item, dict) and item.get("intent_id")
    )
    for intent_id in cancelled_intent_ids:
        active_orders.pop(intent_id, None)
    for item in diff.get("create") or []:
        if (
            isinstance(item, dict)
            and item.get("intent_id")
            and ("create", str(item.get("intent_id"))) in successful
        ):
            active_orders[str(item["intent_id"])] = _order_from_intent(item)

    books = _market_books(plan_state)
    newly_filled: list[dict[str, Any]] = []
    for intent_id, order in list(active_orders.items()):
        fill = _maybe_fill(order, books.get(str(order.get("market_id"))), max_fill_size=max_fill_size)
        if not fill:
            continue
        newly_filled.append(fill)
        fills.append(fill)
        active_orders.pop(intent_id, None)

    positions = _positions(fills, books)
    realized_cost = sum(_dec(row.get("notional")) for row in fills)
    marked_value = sum(_dec(row.get("mark_value")) for row in positions)
    unrealized_pnl = marked_value - realized_cost

    return {
        "ts": utc_now(),
        "mode": "simulated_live",
        "source_ts": {
            "plans": plan_state.get("ts"),
            "intents": intents_state.get("ts"),
            "execution": execution_report.get("ts"),
        },
        "summary": {
            "active_orders": len(active_orders),
            "fills_total": len(fills),
            "fills_new": len(newly_filled),
            "position_legs": len(positions),
            "gross_cost": str(realized_cost),
            "marked_value": str(marked_value),
            "unrealized_pnl": str(unrealized_pnl),
        },
        "active_orders": list(active_orders.values()),
        "new_fills": newly_filled,
        "fills": fills[-500:],
        "positions": positions,
    }


def _order_from_intent(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "intent_id": str(item.get("intent_id") or ""),
        "account_id": str(item.get("account_id") or "acct01"),
        "market_id": int(item.get("market_id") or 0),
        "outcome": str(item.get("outcome") or ""),
        "side": str(item.get("side") or ""),
        "price": str(item.get("price") or "0"),
        "size": str(item.get("size") or "0"),
        "notional": str(item.get("notional") or "0"),
        "created_at": utc_now(),
        "reason": str(item.get("reason") or ""),
    }


def _market_books(plan_state: dict[str, Any]) -> dict[str, dict[str, Decimal]]:
    out: dict[str, dict[str, Decimal]] = {}
    for plan in plan_state.get("plans") or []:
        if not isinstance(plan, dict):
            continue
        market = plan.get("market") if isinstance(plan.get("market"), dict) else {}
        market_id = str(market.get("id") or "")
        if not market_id:
            continue
        yes_bid = _dec(plan.get("best_yes_bid"))
        yes_ask = _dec(plan.get("best_yes_ask"))
        mid = _dec(plan.get("mid"))
        out[market_id] = {
            "YES_bid": yes_bid,
            "YES_ask": yes_ask,
            "YES_mid": mid,
            "NO_bid": max(Decimal("0"), Decimal("1") - yes_ask),
            "NO_ask": max(Decimal("0"), Decimal("1") - yes_bid),
            "NO_mid": max(Decimal("0"), Decimal("1") - mid),
        }
    return out


def _maybe_fill(
    order: dict[str, Any],
    book: dict[str, Decimal] | None,
    *,
    max_fill_size: Decimal,
) -> dict[str, Any] | None:
    if not book:
        return None
    side = str(order.get("side") or "").upper()
    outcome = str(order.get("outcome") or "").upper()
    price = _dec(order.get("price"))
    size = _dec(order.get("size"))
    if side not in {"BUY", "SELL"} or outcome not in {"YES", "NO"}:
        return None
    if side == "BUY":
        ask = book.get(f"{outcome}_ask", Decimal("0"))
        if ask <= 0 or price < ask:
            return None
        fill_reason = f"sim crossed ask={ask}"
    else:
        bid = book.get(f"{outcome}_bid", Decimal("0"))
        if bid <= 0 or price > bid:
            return None
        fill_reason = f"sim crossed bid={bid}"
    fill_size = min(size, max_fill_size if max_fill_size > 0 else size)
    return {
        "ts": utc_now(),
        "intent_id": order.get("intent_id"),
        "account_id": order.get("account_id") or "acct01",
        "market_id": order.get("market_id"),
        "outcome": outcome,
        "side": side,
        "price": str(price),
        "size": str(fill_size),
        "notional": str(price * fill_size),
        "fill_reason": fill_reason,
    }


def _positions(fills: list[dict[str, Any]], books: dict[str, dict[str, Decimal]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], dict[str, Decimal]] = {}
    for fill in fills:
        account_id = str(fill.get("account_id") or "acct01")
        market_id = str(fill.get("market_id") or "")
        outcome = str(fill.get("outcome") or "").upper()
        if not market_id or not outcome:
            continue
        key = (account_id, market_id, outcome)
        row = grouped.setdefault(key, {"size": Decimal("0"), "cost": Decimal("0"), "realized_pnl": Decimal("0")})
        size = _dec(fill.get("size"))
        notional = _dec(fill.get("notional"))
        side = str(fill.get("side") or "").upper()
        if side == "SELL":
            existing = row["size"]
            sell_size = min(size, existing) if existing > 0 else Decimal("0")
            avg_cost = row["cost"] / existing if existing > 0 else Decimal("0")
            row["size"] -= sell_size
            row["cost"] -= avg_cost * sell_size
            row["realized_pnl"] += notional - avg_cost * sell_size
        else:
            row["size"] += size
            row["cost"] += notional

    out: list[dict[str, Any]] = []
    for (account_id, market_id, outcome), row in sorted(grouped.items()):
        size = row["size"]
        cost = row["cost"]
        if size <= 0:
            continue
        mark = (books.get(market_id) or {}).get(f"{outcome}_mid", Decimal("0"))
        mark_value = mark * size
        out.append(
            {
                "account_id": account_id,
                "market_id": market_id,
                "outcome": outcome,
                "size": str(size),
                "avg_cost": str(cost / size) if size else "0",
                "cost": str(cost),
                "mark": str(mark),
                "mark_value": str(mark_value),
                "unrealized_pnl": str(mark_value - cost),
                "realized_pnl": str(row["realized_pnl"]),
            }
        )
    return out


def _dec(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal("0")


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict.fun simulated live state update.")
    parser.add_argument("--plans", required=True)
    parser.add_argument("--intents", required=True)
    parser.add_argument("--execution", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--max-fill-size", default="10")
    args = parser.parse_args()

    state_path = Path(args.state).resolve()
    state = update_simulation(
        previous_state=load_json(state_path),
        plan_state=load_json(Path(args.plans).resolve()),
        intents_state=load_json(Path(args.intents).resolve()),
        execution_report=load_json(Path(args.execution).resolve()),
        max_fill_size=_dec(args.max_fill_size),
    )
    write_json(state_path, state)
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
