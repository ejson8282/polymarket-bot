from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from platforms.predictfun.maker.executor import (
    ExecutableOrder,
    ExecutionResult,
    LiveOrder,
)
from platforms.predictfun.maker.intents import utc_now


TERMINAL_STATUSES = frozenset({"cancelled", "canceled", "filled", "expired", "rejected", "closed"})


@dataclass(frozen=True)
class ManagedOrder:
    order_id: str
    intent_id: str
    account_id: str
    market_id: int
    outcome: str
    side: str
    purpose: str
    status: str
    created_at: str
    updated_at: str
    idempotency_key: str = ""
    submission_generation: int = 0


class ManagedOrderRegistry:
    """Tracks only orders created by this engine.

    Account feeds may also contain website/manual orders. They must never enter
    this registry merely because they share an account or market.
    """

    def __init__(
        self,
        orders: list[ManagedOrder] | None = None,
        *,
        history_limit: int = 1000,
        submission_generations: dict[str, dict[str, int]] | None = None,
    ) -> None:
        self.history_limit = max(1, int(history_limit))
        self._orders = {order.order_id: order for order in orders or [] if order.order_id}
        self._submission_generations = self._validated_generations(
            submission_generations
        )
        self._seed_generations_from_orders()

    @classmethod
    def from_state(cls, state: dict[str, Any] | None, *, history_limit: int = 1000) -> "ManagedOrderRegistry":
        rows = state.get("orders") if isinstance(state, dict) else []
        orders: list[ManagedOrder] = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            order_id = str(row.get("order_id") or "").strip()
            intent_id = str(row.get("intent_id") or "").strip()
            if not order_id or not intent_id:
                continue
            try:
                market_id = int(row.get("market_id") or 0)
            except (TypeError, ValueError):
                continue
            try:
                submission_generation = max(
                    0, int(row.get("submission_generation") or 0)
                )
            except (TypeError, ValueError):
                submission_generation = 0
            orders.append(
                ManagedOrder(
                    order_id=order_id,
                    intent_id=intent_id,
                    account_id=str(row.get("account_id") or ""),
                    market_id=market_id,
                    outcome=str(row.get("outcome") or ""),
                    side=str(row.get("side") or ""),
                    purpose=str(row.get("purpose") or "maker_quote"),
                    status=str(row.get("status") or "open").lower(),
                    created_at=str(row.get("created_at") or utc_now()),
                    updated_at=str(row.get("updated_at") or utc_now()),
                    idempotency_key=str(row.get("idempotency_key") or ""),
                    submission_generation=submission_generation,
                )
            )
        generations = (
            state.get("submission_generations")
            if isinstance(state, dict)
            and isinstance(state.get("submission_generations"), dict)
            else {}
        )
        return cls(
            orders,
            history_limit=history_limit,
            submission_generations=generations,
        )

    def idempotency_key_for_create(self, intent_id: str, account_id: str) -> str:
        """Return a stable key for this attempt and rotate after successful creates."""

        intent_id = str(intent_id or "").strip()
        account_id = str(account_id or "").strip()
        generation = self._submission_generations.get(account_id, {}).get(
            intent_id, 0
        )
        return intent_id if generation <= 0 else f"{intent_id}:g{generation + 1}"

    def record_create(self, order: ExecutableOrder, result: ExecutionResult) -> None:
        if not result.ok or not result.order_id:
            return
        now = utc_now()
        idempotency_key = str(order.idempotency_key or order.intent_id).strip()
        generation = self._generation_for_key(order.intent_id, idempotency_key)
        if not result.order_id.startswith("dry:"):
            account_generations = self._submission_generations.setdefault(
                order.account_id, {}
            )
            generation = max(
                generation,
                account_generations.get(order.intent_id, 0) + 1,
            )
            account_generations[order.intent_id] = generation
        self._orders[result.order_id] = ManagedOrder(
            order_id=result.order_id,
            intent_id=order.intent_id,
            account_id=order.account_id,
            market_id=order.market_id,
            outcome=order.outcome,
            side=order.side,
            purpose=order.purpose,
            status=(result.status or "open").lower(),
            created_at=now,
            updated_at=now,
            idempotency_key=idempotency_key,
            submission_generation=generation,
        )
        self._trim()

    def active_for_intent(self, intent_id: str, account_id: str) -> list[ManagedOrder]:
        return [
            order
            for order in self.active()
            if order.intent_id == intent_id and (not account_id or order.account_id == account_id)
        ]

    def record_cancel(self, order_id: str, result: ExecutionResult) -> None:
        if not result.ok:
            return
        order = self._orders.get(order_id)
        if order is None:
            return
        now = utc_now()
        self._orders[order_id] = ManagedOrder(
            **{
                **asdict(order),
                "status": (result.status or "cancelled").lower(),
                "updated_at": now,
            }
        )

    def discard_simulated(self) -> int:
        """Drop dry-run placeholders before a live reconciliation cycle."""

        simulated_ids = [
            order_id
            for order_id in self._orders
            if order_id.startswith("dry:")
        ]
        for order_id in simulated_ids:
            self._orders.pop(order_id, None)
        return len(simulated_ids)

    def sync_live_orders(self, live_orders: list[LiveOrder]) -> None:
        """Refresh engine-owned rows without adopting manual website orders."""

        live_by_id = {
            order.order_id: order for order in live_orders if order.order_id
        }
        now = utc_now()
        for order_id, managed in list(self._orders.items()):
            live = live_by_id.get(order_id)
            if live is None:
                continue
            if live.account_id and live.account_id != managed.account_id:
                continue
            status = str(live.status or managed.status).lower()
            self._orders[order_id] = ManagedOrder(
                **{
                    **asdict(managed),
                    "status": status,
                    "updated_at": now,
                }
            )

    def owns_order_id(self, order_id: str) -> bool:
        return str(order_id or "") in self._orders

    def active(self) -> list[ManagedOrder]:
        return [order for order in self._orders.values() if order.status not in TERMINAL_STATUSES]

    def to_state(self) -> dict[str, Any]:
        rows = sorted(self._orders.values(), key=lambda order: (order.updated_at, order.order_id))
        return {
            "ts": utc_now(),
            "summary": {
                "tracked": len(rows),
                "active": sum(1 for order in rows if order.status not in TERMINAL_STATUSES),
                "inventory_exits": sum(
                    1
                    for order in rows
                    if order.status not in TERMINAL_STATUSES and order.purpose == "inventory_exit"
                ),
            },
            "submission_generations": {
                account_id: dict(sorted(rows.items()))
                for account_id, rows in sorted(
                    self._submission_generations.items()
                )
                if rows
            },
            "orders": [asdict(order) for order in rows],
        }

    @staticmethod
    def _validated_generations(
        raw: dict[str, dict[str, int]] | None,
    ) -> dict[str, dict[str, int]]:
        out: dict[str, dict[str, int]] = {}
        for account_id, rows in (raw or {}).items():
            if not isinstance(rows, dict):
                continue
            cleaned: dict[str, int] = {}
            for intent_id, value in rows.items():
                try:
                    generation = max(0, int(value))
                except (TypeError, ValueError):
                    continue
                if intent_id and generation > 0:
                    cleaned[str(intent_id)] = generation
            if cleaned:
                out[str(account_id)] = cleaned
        return out

    @staticmethod
    def _generation_for_key(intent_id: str, idempotency_key: str) -> int:
        if not idempotency_key or idempotency_key == intent_id:
            return 1
        prefix = f"{intent_id}:g"
        if idempotency_key.startswith(prefix):
            try:
                return max(1, int(idempotency_key[len(prefix) :]))
            except ValueError:
                pass
        return 1

    def _seed_generations_from_orders(self) -> None:
        for order in self._orders.values():
            if order.order_id.startswith("dry:"):
                continue
            generation = max(
                order.submission_generation,
                self._generation_for_key(
                    order.intent_id,
                    order.idempotency_key or order.intent_id,
                ),
            )
            account_generations = self._submission_generations.setdefault(
                order.account_id, {}
            )
            account_generations[order.intent_id] = max(
                generation,
                account_generations.get(order.intent_id, 0),
            )

    def _trim(self) -> None:
        if len(self._orders) <= self.history_limit:
            return
        terminal = sorted(
            (order for order in self._orders.values() if order.status in TERMINAL_STATUSES),
            key=lambda order: (order.updated_at, order.order_id),
        )
        for order in terminal[: max(0, len(self._orders) - self.history_limit)]:
            self._orders.pop(order.order_id, None)
