"""Pure-Python reference oracle for the Rust maker core.

This module has no HTTP, signing, or execution capability. It intentionally
uses only the standard library so offline shadow parity can run anywhere the
existing Python project runs.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Any, Iterable


ACTIVE_STATUSES = {"open", "partially_filled"}


def evaluate_case(case: dict[str, Any]) -> dict[str, Any]:
    desired = list(case.get("desired") or [])
    actual = list(case.get("actual") or [])
    books = list(case.get("books") or [])
    limits = dict(case.get("risk_limits") or {})
    policy = dict(case.get("reconcile_policy") or {})

    risk = evaluate_risk(desired, books, limits)
    if not risk["allowed"]:
        return {
            "mode": "dry_run",
            "can_execute": False,
            "risk": risk,
        }

    try:
        plan = reconcile(desired, actual, policy)
    except ValueError as error:
        return {
            "mode": "dry_run",
            "can_execute": False,
            "risk": risk,
            "error": str(error),
        }
    return {
        "mode": "dry_run",
        "can_execute": True,
        "risk": risk,
        "plan": plan,
    }


def evaluate_risk(
    desired: list[dict[str, Any]],
    books: list[dict[str, Any]],
    limits: dict[str, Any],
) -> dict[str, Any]:
    violations: list[dict[str, Any]] = []
    min_price = _decimal(limits.get("min_price"))
    max_price = _decimal(limits.get("max_price"))
    max_quantity = _decimal(limits.get("max_quantity"))
    max_order_notional = _decimal(limits.get("max_order_notional"))
    max_account_notional = _decimal(limits.get("max_account_notional"))
    max_account_market_notional = _decimal(limits.get("max_account_market_notional"))
    max_order_count = int(limits.get("max_open_orders_per_account") or 0)
    max_book_age_ms = int(limits.get("max_book_age_ms") or 0)
    require_book_age = bool(limits.get("require_book_age", True))

    book_ages = {
        _instrument_key(book): int(book.get("age_ms") or 0)
        for book in books
    }
    account_notional: dict[str, Decimal] = defaultdict(Decimal)
    account_market_notional: dict[tuple[str, tuple[str, ...]], Decimal] = defaultdict(Decimal)
    account_order_count: dict[str, int] = defaultdict(int)

    for intent in desired:
        slot_id = str(intent.get("slot_id") or "")
        account_id = str(intent.get("account_id") or "")
        price = _decimal(intent.get("price"))
        quantity = _decimal(intent.get("quantity"))
        notional = price * quantity

        if price < min_price or price > max_price:
            _add_violation(
                violations,
                "price_out_of_range",
                f"slot {slot_id} price is outside configured bounds",
                [slot_id],
            )
        if quantity <= 0 or quantity > max_quantity:
            _add_violation(
                violations,
                "quantity_out_of_range",
                f"slot {slot_id} quantity is outside configured bounds",
                [slot_id],
            )
        if notional > max_order_notional:
            _add_violation(
                violations,
                "order_notional_exceeded",
                f"slot {slot_id} exceeds max order notional",
                [slot_id],
            )

        key = _instrument_key(intent)
        if key not in book_ages and require_book_age:
            _add_violation(
                violations,
                "missing_book_age",
                f"slot {slot_id} has no order-book freshness record",
                [slot_id],
            )
        elif book_ages.get(key, 0) > max_book_age_ms:
            _add_violation(
                violations,
                "stale_book",
                f"slot {slot_id} uses a stale order book",
                [slot_id],
            )

        account_notional[account_id] += notional
        account_market_notional[(account_id, key)] += notional
        account_order_count[account_id] += 1

    for account_id, notional in account_notional.items():
        if notional > max_account_notional:
            _add_violation(
                violations,
                "account_notional_exceeded",
                f"account {account_id} exceeds max aggregate notional",
                _slots_for_account(desired, account_id),
            )
    for (account_id, key), notional in account_market_notional.items():
        if notional > max_account_market_notional:
            _add_violation(
                violations,
                "account_market_notional_exceeded",
                f"account {account_id} exceeds market notional on {key[0]}/{key[1]}",
                [
                    str(intent.get("slot_id") or "")
                    for intent in desired
                    if str(intent.get("account_id") or "") == account_id
                    and _instrument_key(intent) == key
                ],
            )
    for account_id, count in account_order_count.items():
        if count > max_order_count:
            _add_violation(
                violations,
                "account_order_count_exceeded",
                f"account {account_id} exceeds max desired order count",
                _slots_for_account(desired, account_id),
            )

    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for intent in desired:
        grouped[_instrument_key(intent)].append(intent)
    for key, intents in grouped.items():
        buys = [intent for intent in intents if str(intent.get("side") or "").lower() == "buy"]
        sells = [intent for intent in intents if str(intent.get("side") or "").lower() == "sell"]
        for buy in buys:
            for sell in sells:
                if _decimal(buy.get("price")) >= _decimal(sell.get("price")):
                    scope = (
                        "same-account"
                        if buy.get("account_id") == sell.get("account_id")
                        else "cross-account"
                    )
                    _add_violation(
                        violations,
                        "self_trade_risk",
                        f"{scope} quotes cross on {key[0]}/{key[1]}",
                        [str(buy.get("slot_id") or ""), str(sell.get("slot_id") or "")],
                    )

    violations.sort(key=lambda row: (row["code"], row["slot_ids"], row["message"]))
    return {"allowed": not violations, "violations": violations}


def reconcile(
    desired: list[dict[str, Any]],
    actual: list[dict[str, Any]],
    policy: dict[str, Any],
) -> dict[str, Any]:
    price_epsilon = _decimal(policy.get("price_epsilon"))
    quantity_epsilon = _decimal(policy.get("quantity_epsilon"))
    desired_by_slot: dict[str, dict[str, Any]] = {}
    for intent in desired:
        slot_id = str(intent.get("slot_id") or "")
        if slot_id in desired_by_slot:
            raise ValueError(f"duplicate desired slot_id: {slot_id}")
        desired_by_slot[slot_id] = intent

    actual_by_slot: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmanaged_order_ids: list[str] = []
    for order in actual:
        if str(order.get("status") or "").lower() not in ACTIVE_STATUSES:
            continue
        slot_id = order.get("managed_slot")
        if slot_id is None:
            unmanaged_order_ids.append(str(order.get("order_id") or ""))
        else:
            actual_by_slot[str(slot_id)].append(order)
    unmanaged_order_ids.sort()
    for orders in actual_by_slot.values():
        orders.sort(key=lambda row: str(row.get("order_id") or ""))

    actions: list[dict[str, Any]] = []
    warnings: list[str] = []
    consumed_slots: set[str] = set()
    for slot_id, intent in sorted(desired_by_slot.items()):
        consumed_slots.add(slot_id)
        orders = actual_by_slot.get(slot_id)
        if not orders:
            actions.append({"action": "create", "intent": intent})
            continue

        equivalent_index = next(
            (
                index
                for index, order in enumerate(orders)
                if _equivalent(order, intent, price_epsilon, quantity_epsilon)
            ),
            None,
        )
        primary_index = equivalent_index if equivalent_index is not None else 0
        primary = orders[primary_index]
        if equivalent_index is not None:
            actions.append(
                {
                    "action": "keep",
                    "order_id": str(primary.get("order_id") or ""),
                    "slot_id": slot_id,
                }
            )
        else:
            actions.append(
                {
                    "action": "replace",
                    "order_id": str(primary.get("order_id") or ""),
                    "intent": intent,
                    "reason": "managed quote changed",
                }
            )
        for index, duplicate in enumerate(orders):
            if index != primary_index:
                actions.append(
                    {
                        "action": "cancel",
                        "order_id": str(duplicate.get("order_id") or ""),
                        "reason": f"duplicate managed order for slot {slot_id}",
                    }
                )
        if len(orders) > 1:
            warnings.append(f"slot {slot_id} has {len(orders)} active managed orders")

    for slot_id, orders in sorted(actual_by_slot.items()):
        if slot_id in consumed_slots:
            continue
        for order in orders:
            actions.append(
                {
                    "action": "cancel",
                    "order_id": str(order.get("order_id") or ""),
                    "reason": f"slot {slot_id} is no longer desired",
                }
            )

    return {
        "actions": actions,
        "unmanaged_order_ids": unmanaged_order_ids,
        "warnings": warnings,
    }


def canonical_result(result: dict[str, Any]) -> dict[str, Any]:
    risk = result.get("risk") if isinstance(result.get("risk"), dict) else {}
    canonical: dict[str, Any] = {
        "can_execute": bool(result.get("can_execute")),
        "risk_allowed": bool(risk.get("allowed")),
        "risk_violations": sorted(
            (
                str(row.get("code") or ""),
                tuple(str(slot) for slot in row.get("slot_ids") or []),
            )
            for row in risk.get("violations") or []
            if isinstance(row, dict)
        ),
    }
    plan = result.get("plan") if isinstance(result.get("plan"), dict) else {}
    canonical["actions"] = sorted(
        (_canonical_action(action) for action in plan.get("actions") or []),
        key=repr,
    )
    canonical["unmanaged_order_ids"] = sorted(plan.get("unmanaged_order_ids") or [])
    canonical["warnings"] = sorted(plan.get("warnings") or [])
    canonical["error"] = str(result.get("error") or "")
    return canonical


def _canonical_action(action: dict[str, Any]) -> tuple[Any, ...]:
    kind = str(action.get("action") or "")
    intent = action.get("intent") if isinstance(action.get("intent"), dict) else {}
    instrument = intent.get("instrument") if isinstance(intent.get("instrument"), dict) else {}
    return (
        kind,
        str(action.get("order_id") or ""),
        str(action.get("slot_id") or intent.get("slot_id") or ""),
        str(intent.get("account_id") or ""),
        str(intent.get("venue") or ""),
        str(instrument.get("market_id") or ""),
        str(instrument.get("outcome_id") or ""),
        str(instrument.get("token_id") or ""),
        str(intent.get("side") or ""),
        _canonical_decimal(intent.get("price")),
        _canonical_decimal(intent.get("quantity")),
        bool(intent.get("post_only")),
        bool(intent.get("reduce_only")),
    )


def _equivalent(
    order: dict[str, Any],
    intent: dict[str, Any],
    price_epsilon: Decimal,
    quantity_epsilon: Decimal,
) -> bool:
    return (
        str(order.get("account_id") or "") == str(intent.get("account_id") or "")
        and str(order.get("venue") or "") == str(intent.get("venue") or "")
        and _instrument_key(order) == _instrument_key(intent)
        and str(order.get("side") or "") == str(intent.get("side") or "")
        and bool(order.get("post_only")) == bool(intent.get("post_only"))
        and abs(_decimal(order.get("price")) - _decimal(intent.get("price")))
        <= price_epsilon
        and abs(_decimal(order.get("quantity")) - _decimal(intent.get("quantity")))
        <= quantity_epsilon
    )


def _instrument_key(row: dict[str, Any]) -> tuple[str, ...]:
    instrument = row.get("instrument") if isinstance(row.get("instrument"), dict) else {}
    return (
        str(row.get("venue") or ""),
        str(instrument.get("market_id") or ""),
        str(instrument.get("outcome_id") or ""),
        str(instrument.get("token_id") or ""),
    )


def _slots_for_account(desired: Iterable[dict[str, Any]], account_id: str) -> list[str]:
    return [
        str(intent.get("slot_id") or "")
        for intent in desired
        if str(intent.get("account_id") or "") == account_id
    ]


def _add_violation(
    violations: list[dict[str, Any]],
    code: str,
    message: str,
    slot_ids: list[str],
) -> None:
    violations.append({"code": code, "message": message, "slot_ids": slot_ids})


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or "0"))


def _canonical_decimal(value: Any) -> str:
    decimal = _decimal(value)
    if decimal == 0:
        return "0"
    return format(decimal.normalize(), "f")
