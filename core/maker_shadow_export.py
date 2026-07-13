"""Read-only exporters for the shared Rust maker shadow contract.

The functions in this module only transform existing JSON state. They do not
perform HTTP requests, signing, cancellation, or order placement.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable


DEFAULT_RISK_LIMITS = {
    "min_price": "0.01",
    "max_price": "0.99",
    "max_quantity": "1000000",
    "max_order_notional": "1000000",
    "max_account_notional": "10000000",
    "max_account_market_notional": "1000000",
    "max_open_orders_per_account": 1000,
    "max_book_age_ms": 5000,
    "require_book_age": True,
}


def export_predictfun_snapshot(
    *,
    intents_state: dict[str, Any],
    actual_state: dict[str, Any],
    plans_state: dict[str, Any],
    risk_limits: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Convert Predict.fun dry-run state into the normalized shadow contract."""

    desired = [
        _predictfun_intent(row)
        for row in intents_state.get("intents") or []
        if isinstance(row, dict)
    ]
    desired_by_slot = {row["slot_id"]: row for row in desired}
    actual = [
        _predictfun_live_order(row, desired_by_slot)
        for row in actual_state.get("active_orders") or []
        if isinstance(row, dict)
    ]

    plan_ts = str(plans_state.get("ts") or intents_state.get("ts") or "")
    actual_ts = str(actual_state.get("ts") or "")
    age_ms = _age_ms(plan_ts, now=now)
    if actual and actual_ts:
        actual_age_ms = _age_ms(actual_ts, now=now)
        if age_ms is not None and actual_age_ms is not None:
            age_ms = max(age_ms, actual_age_ms)
    elif actual:
        age_ms = None
    books = _book_rows(desired, age_ms)
    return _shadow_case(
        desired=desired,
        actual=actual,
        books=books,
        risk_limits=risk_limits,
        metadata={
            "source": "predictfun_python_state",
            "source_ts": {
                "plans": plan_ts,
                "actual": actual_ts,
            },
            "desired_orders": len(desired),
            "actual_orders": len(actual),
        },
    )


def export_polymarket_snapshot(
    *,
    engine_states: Iterable[dict[str, Any]],
    risk_limits: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Combine one or more Polymarket engine states into one shadow case."""

    desired: list[dict[str, Any]] = []
    actual: list[dict[str, Any]] = []
    book_age_by_key: dict[tuple[str, str, str, str], int] = {}
    source_timestamps: list[str] = []

    for ordinal, state in enumerate(engine_states, start=1):
        if not isinstance(state, dict):
            continue
        account_id = _polymarket_account_id(state, ordinal)
        state_ts = str(state.get("ts") or "")
        if state_ts:
            source_timestamps.append(state_ts)
        markets = state.get("markets") if isinstance(state.get("markets"), dict) else {}
        for token_id, market in sorted(markets.items()):
            if not isinstance(market, dict):
                continue
            instrument = _polymarket_instrument(str(token_id), market)
            plan = _parse_polymarket_plan(str(market.get("desired_plan_sig") or ""))
            desired_rows = [
                _polymarket_intent(
                    account_id=account_id,
                    instrument=instrument,
                    level=level,
                    price=price,
                    quantity=quantity,
                )
                for level, (price, quantity) in enumerate(plan)
            ]
            desired.extend(desired_rows)

            live_orders = [
                row for row in market.get("orders") or [] if isinstance(row, dict)
            ]
            actual.extend(
                _polymarket_live_orders(
                    account_id=account_id,
                    instrument=instrument,
                    rows=live_orders,
                    managed="desired_plan_sig" in market,
                )
            )

            if desired_rows:
                state_age_ms = _age_ms(state_ts, now=now)
                snapshot_age = market.get("snapshot_age_ms")
                if state_age_ms is not None:
                    snapshot_age_ms = max(
                        0,
                        int(Decimal(str(snapshot_age or 0))),
                    )
                    key = _instrument_key(desired_rows[0])
                    age_value = state_age_ms + snapshot_age_ms
                    book_age_by_key[key] = max(book_age_by_key.get(key, 0), age_value)

    books = [
        {
            "venue": key[0],
            "instrument": {
                "market_id": key[1],
                "outcome_id": key[2],
                **({"token_id": key[3]} if key[3] else {}),
            },
            "age_ms": age_ms,
        }
        for key, age_ms in sorted(book_age_by_key.items())
    ]
    return _shadow_case(
        desired=desired,
        actual=actual,
        books=books,
        risk_limits=risk_limits,
        metadata={
            "source": "polymarket_engine_state",
            "source_ts": sorted(source_timestamps),
            "accounts": len({row["account_id"] for row in desired + actual}),
            "desired_orders": len(desired),
            "actual_orders": len(actual),
        },
    )


def _shadow_case(
    *,
    desired: list[dict[str, Any]],
    actual: list[dict[str, Any]],
    books: list[dict[str, Any]],
    risk_limits: dict[str, Any] | None,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "metadata": {"mode": "read_only_shadow", **metadata},
        "desired": desired,
        "actual": actual,
        "books": books,
        "risk_limits": _risk_limits(risk_limits),
        "reconcile_policy": {
            "price_epsilon": "0",
            "quantity_epsilon": "0",
        },
    }


def _predictfun_intent(row: dict[str, Any]) -> dict[str, Any]:
    token_id = str(row.get("token_id") or "")
    instrument = {
        "market_id": str(row.get("market_id") or ""),
        "outcome_id": str(row.get("outcome") or "").upper(),
    }
    if token_id:
        instrument["token_id"] = token_id
    return {
        "slot_id": str(row.get("intent_id") or ""),
        "account_id": str(row.get("account_id") or "acct01"),
        "strategy_id": "predictfun-maker",
        "venue": "predict_fun",
        "instrument": instrument,
        "side": _side(row.get("side")),
        "price": _decimal_text(row.get("price")),
        "quantity": _decimal_text(row.get("size")),
        "post_only": True,
        "reduce_only": False,
    }


def _predictfun_live_order(
    row: dict[str, Any],
    desired_by_slot: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    slot_id = str(row.get("intent_id") or "")
    desired = desired_by_slot.get(slot_id)
    if desired:
        instrument = dict(desired["instrument"])
    else:
        instrument = {
            "market_id": str(row.get("market_id") or ""),
            "outcome_id": str(row.get("outcome") or "").upper(),
        }
    return {
        "order_id": str(row.get("order_id") or f"sim:{slot_id}"),
        "managed_slot": slot_id,
        "account_id": str(row.get("account_id") or "acct01"),
        "venue": "predict_fun",
        "instrument": instrument,
        "side": _side(row.get("side")),
        "price": _decimal_text(row.get("price")),
        "quantity": _decimal_text(row.get("size")),
        "filled_quantity": _decimal_text(row.get("filled_quantity")),
        "status": _status(row.get("status")),
        "post_only": True,
    }


def _polymarket_account_id(state: dict[str, Any], ordinal: int) -> str:
    explicit = str(state.get("account_id") or "").strip()
    if explicit:
        return explicit
    index = state.get("account_index")
    if index is None:
        index = ordinal
    return f"pm-account-{index}"


def _polymarket_instrument(token_id: str, market: dict[str, Any]) -> dict[str, str]:
    condition_id = str(market.get("condition_id") or token_id)
    return {
        "market_id": condition_id,
        "outcome_id": token_id,
        "token_id": token_id,
    }


def _polymarket_intent(
    *,
    account_id: str,
    instrument: dict[str, str],
    level: int,
    price: str,
    quantity: str,
) -> dict[str, Any]:
    token_id = instrument["token_id"]
    return {
        "slot_id": f"{account_id}:{token_id}:buy:l{level}",
        "account_id": account_id,
        "strategy_id": "polymarket-maker",
        "venue": "polymarket",
        "instrument": dict(instrument),
        "side": "buy",
        "price": price,
        "quantity": quantity,
        "post_only": True,
        "reduce_only": False,
    }


def _polymarket_live_orders(
    *,
    account_id: str,
    instrument: dict[str, str],
    rows: list[dict[str, Any]],
    managed: bool,
) -> list[dict[str, Any]]:
    maker_rows = [row for row in rows if _side(row.get("side")) == "buy" and not row.get("is_exit")]
    maker_rows.sort(
        key=lambda row: (
            -_decimal(row.get("price_raw", row.get("price"))),
            str(row.get("id") or row.get("order_id") or ""),
        )
    )
    managed_slots = (
        {
            id(row): f"{account_id}:{instrument['token_id']}:buy:l{level}"
            for level, row in enumerate(maker_rows)
        }
        if managed
        else {}
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        order_id = str(row.get("id") or row.get("order_id") or "")
        managed_slot = managed_slots.get(id(row))
        item = {
            "order_id": order_id,
            "account_id": account_id,
            "venue": "polymarket",
            "instrument": dict(instrument),
            "side": _side(row.get("side")),
            "price": _decimal_text(row.get("price_raw", row.get("price"))),
            "quantity": _decimal_text(row.get("size_raw", row.get("size"))),
            "filled_quantity": _decimal_text(row.get("size_matched_raw")),
            "status": _status(row.get("status")),
            "post_only": bool(row.get("post_only", True)),
        }
        if managed_slot:
            item["managed_slot"] = managed_slot
        out.append(item)
    return out


def _parse_polymarket_plan(signature: str) -> list[tuple[str, str]]:
    plan: list[tuple[str, str]] = []
    for leg in signature.split("|"):
        leg = leg.strip()
        if not leg or ":" not in leg:
            continue
        price, quantity = leg.rsplit(":", 1)
        price_text = _decimal_text(price)
        quantity_text = _decimal_text(quantity)
        if _decimal(price_text) > 0 and _decimal(quantity_text) > 0:
            plan.append((price_text, quantity_text))
    return plan


def _book_rows(desired: list[dict[str, Any]], age_ms: int | None) -> list[dict[str, Any]]:
    if age_ms is None:
        return []
    unique = {_instrument_key(row): row for row in desired}
    return [
        {
            "venue": row["venue"],
            "instrument": dict(row["instrument"]),
            "age_ms": age_ms,
        }
        for _, row in sorted(unique.items())
    ]


def _instrument_key(row: dict[str, Any]) -> tuple[str, str, str, str]:
    instrument = row.get("instrument") if isinstance(row.get("instrument"), dict) else {}
    return (
        str(row.get("venue") or ""),
        str(instrument.get("market_id") or ""),
        str(instrument.get("outcome_id") or ""),
        str(instrument.get("token_id") or ""),
    )


def _risk_limits(overrides: dict[str, Any] | None) -> dict[str, Any]:
    limits = {**DEFAULT_RISK_LIMITS, **(overrides or {})}
    for key in (
        "min_price",
        "max_price",
        "max_quantity",
        "max_order_notional",
        "max_account_notional",
        "max_account_market_notional",
    ):
        limits[key] = _decimal_text(limits.get(key))
    limits["max_open_orders_per_account"] = int(limits["max_open_orders_per_account"])
    limits["max_book_age_ms"] = int(limits["max_book_age_ms"])
    limits["require_book_age"] = bool(limits["require_book_age"])
    return limits


def _age_ms(value: str, *, now: datetime | None) -> int | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0, int((current - timestamp).total_seconds() * 1000))


def _side(value: Any) -> str:
    normalized = str(value or "buy").strip().lower()
    return "sell" if normalized in {"sell", "ask"} else "buy"


def _status(value: Any) -> str:
    normalized = str(value or "open").strip().lower()
    aliases = {
        "live": "open",
        "matched": "partially_filled",
        "canceled": "cancelled",
    }
    normalized = aliases.get(normalized, normalized)
    if normalized not in {"open", "partially_filled", "filled", "cancelled", "rejected"}:
        return "open"
    return normalized


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _decimal_text(value: Any) -> str:
    decimal = _decimal(value)
    if decimal == 0:
        return "0"
    return format(decimal.normalize(), "f")
