"""Read-only observations of when natural maker orders start scoring.

The observer is intentionally action-free: callers provide the current open
orders and an authenticated read-only scoring query.  It never signs, posts,
cancels, or mutates an order.  Persistent checkpoints let us measure the
undocumented scoring warm-up window without manufacturing quote churn.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence


DEFAULT_CHECKPOINTS_SEC = (10, 30, 60, 120, 180, 300)
DEFAULT_RETENTION_SEC = 7 * 24 * 60 * 60


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


def _order_id(order: Mapping[str, Any]) -> str:
    return str(order.get("id") or order.get("orderID") or "").strip()


def _token_id(order: Mapping[str, Any]) -> str:
    return str(order.get("asset_id") or order.get("token_id") or "").strip()


def _created_at(order: Mapping[str, Any], fallback: float) -> float:
    value = _safe_float(order.get("created_at"), 0.0)
    # The API returns Unix seconds.  Ignore millisecond values and malformed
    # historical fixtures rather than inventing an order lifetime.
    if 1_000_000_000 <= value < 10_000_000_000:
        return value
    return fallback


def normalize_scoring_response(payload: object) -> bool:
    """Return the official boolean result or raise on an invalid response."""
    if isinstance(payload, bool):
        return payload
    if isinstance(payload, Mapping) and isinstance(payload.get("scoring"), bool):
        return bool(payload["scoring"])
    raise ValueError("order-scoring response is missing a boolean scoring field")


@dataclass
class OrderScoringObserver:
    state_path: Path
    checkpoints_sec: Sequence[int] = DEFAULT_CHECKPOINTS_SEC
    retention_sec: float = DEFAULT_RETENTION_SEC
    steady_state_interval_sec: float = 0.0
    account_uid_key: str = ""
    host_id: str = ""
    _orders: dict[str, dict[str, Any]] = field(default_factory=dict, init=False)
    _last_error: str = field(default="", init=False)
    _last_poll_at: float = field(default=0.0, init=False)
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False)

    def __post_init__(self) -> None:
        checkpoints = sorted({int(value) for value in self.checkpoints_sec if int(value) > 0})
        if not checkpoints:
            raise ValueError("at least one positive scoring checkpoint is required")
        self.checkpoints_sec = tuple(checkpoints)
        self.retention_sec = max(float(self.retention_sec), float(checkpoints[-1]))
        self.steady_state_interval_sec = max(
            0.0,
            float(self.steady_state_interval_sec),
        )
        self.account_uid_key = str(self.account_uid_key or "").strip().lower()
        self.host_id = str(self.host_id or "").strip().lower()
        self._load()

    def bind_identity(self, *, account_uid_key: str, host_id: str) -> None:
        """Bind observations to one account on one host, clearing foreign state."""

        identity = (
            str(account_uid_key or "").strip().lower(),
            str(host_id or "").strip().lower(),
        )
        with self._lock:
            if identity == (self.account_uid_key, self.host_id):
                return
            self.account_uid_key, self.host_id = identity
            self._orders = {}
            self._last_error = ""
            self._last_poll_at = 0.0

    def _load(self) -> None:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, TypeError, ValueError):
            return
        if not isinstance(payload, Mapping):
            return
        stored_identity = (
            str(payload.get("account_uid_key") or "").strip().lower(),
            str(payload.get("host_id") or "").strip().lower(),
        )
        expected_identity = (self.account_uid_key, self.host_id)
        if stored_identity != expected_identity:
            return
        orders = payload.get("orders")
        if not isinstance(orders, Mapping):
            return
        self._orders = {
            str(order_id): dict(row)
            for order_id, row in orders.items()
            if str(order_id) and isinstance(row, Mapping)
        }

    def _save(self, now: float) -> dict[str, Any]:
        payload = self.public_state(now)
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.state_path.with_name(f".{self.state_path.name}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.state_path)
        return payload

    def _new_row(self, order: Mapping[str, Any], now: float) -> dict[str, Any]:
        opened_at = _created_at(order, now)
        age_at_discovery = max(0.0, now - opened_at)
        observations: list[dict[str, Any]] = []
        # An order first discovered after a checkpoint cannot tell us what its
        # status was at that earlier time.  Mark it unknown instead of falsely
        # backfilling the current answer.
        for checkpoint in self.checkpoints_sec:
            if checkpoint < age_at_discovery:
                observations.append(
                    {
                        "checkpoint_sec": checkpoint,
                        "observed_at": None,
                        "observed_age_sec": None,
                        "scoring": None,
                        "status": "missed_before_observer",
                    }
                )
        return {
            "order_id": _order_id(order),
            "token_id": _token_id(order),
            "side": str(order.get("side") or "").upper(),
            "price": str(order.get("price") or ""),
            "size": str(order.get("original_size") or order.get("size") or ""),
            "opened_at": opened_at,
            "first_seen_at": now,
            "last_seen_at": now,
            "closed_at": None,
            "live": True,
            "observations": observations,
            "first_scoring_age_sec": None,
            "last_scoring": None,
            "query_failures": 0,
        }

    def _next_due_checkpoint(self, row: Mapping[str, Any], now: float) -> int | None:
        age = max(0.0, now - _safe_float(row.get("opened_at"), now))
        seen = {
            int(observation.get("checkpoint_sec") or 0)
            for observation in row.get("observations") or []
            if isinstance(observation, Mapping)
        }
        due = [value for value in self.checkpoints_sec if value <= age and value not in seen]
        if not due:
            if (
                self.steady_state_interval_sec <= 0
                or age < self.checkpoints_sec[-1]
            ):
                return None
            last_observed_at = max(
                (
                    _safe_float(observation.get("observed_at"), 0.0)
                    for observation in row.get("observations") or []
                    if isinstance(observation, Mapping)
                    and observation.get("status") == "observed"
                ),
                default=0.0,
            )
            if (
                last_observed_at > 0
                and now - last_observed_at < self.steady_state_interval_sec
            ):
                return None
            return max(self.checkpoints_sec[-1] + 1, int(age))
        # If polling was delayed, query once at the latest elapsed checkpoint.
        # Earlier checkpoints are recorded as missed, never inferred.
        selected = max(due)
        observations = row.get("observations")
        if isinstance(observations, list):
            for checkpoint in due:
                if checkpoint == selected:
                    continue
                observations.append(
                    {
                        "checkpoint_sec": checkpoint,
                        "observed_at": None,
                        "observed_age_sec": None,
                        "scoring": None,
                        "status": "missed_poll_window",
                    }
                )
        return selected

    def poll(
        self,
        live_orders: Iterable[Mapping[str, Any]],
        query_scoring: Callable[[str], object],
        *,
        now: float,
    ) -> dict[str, Any]:
        """Observe one point in time without performing any trading action."""
        with self._lock:
            return self._poll_locked(live_orders, query_scoring, now=now)

    def _poll_locked(
        self,
        live_orders: Iterable[Mapping[str, Any]],
        query_scoring: Callable[[str], object],
        *,
        now: float,
    ) -> dict[str, Any]:
        self._last_poll_at = float(now)
        current: dict[str, Mapping[str, Any]] = {}
        for order in live_orders:
            if not isinstance(order, Mapping):
                continue
            order_id = _order_id(order)
            if order_id:
                current[order_id] = order

        for order_id, order in current.items():
            row = self._orders.get(order_id)
            if row is None:
                row = self._new_row(order, now)
                self._orders[order_id] = row
            row["last_seen_at"] = now
            row["closed_at"] = None
            row["live"] = True
            checkpoint = self._next_due_checkpoint(row, now)
            if checkpoint is None:
                continue
            age = max(0.0, now - _safe_float(row.get("opened_at"), now))
            try:
                scoring = normalize_scoring_response(query_scoring(order_id))
            except Exception as exc:
                row["query_failures"] = int(row.get("query_failures") or 0) + 1
                row["last_query_error"] = f"{type(exc).__name__}: {str(exc)[:160]}"
                self._last_error = row["last_query_error"]
                continue
            row.pop("last_query_error", None)
            row["last_scoring"] = scoring
            row["observations"].append(
                {
                    "checkpoint_sec": checkpoint,
                    "observed_at": now,
                    "observed_age_sec": round(age, 3),
                    "scoring": scoring,
                    "status": "observed",
                }
            )
            if scoring and row.get("first_scoring_age_sec") is None:
                row["first_scoring_age_sec"] = round(age, 3)

        for order_id, row in list(self._orders.items()):
            if order_id not in current and row.get("live"):
                row["live"] = False
                row["closed_at"] = now
            last_seen = _safe_float(row.get("last_seen_at"), 0.0)
            if not row.get("live") and now - last_seen > self.retention_sec:
                self._orders.pop(order_id, None)

        if not any(row.get("last_query_error") for row in self._orders.values()):
            self._last_error = ""
        return self._save(now)

    def public_state(self, now: float) -> dict[str, Any]:
        with self._lock:
            return self._public_state_locked(now)

    def _public_state_locked(self, now: float) -> dict[str, Any]:
        rows = list(self._orders.values())
        first_scoring = [
            float(row["first_scoring_age_sec"])
            for row in rows
            if row.get("first_scoring_age_sec") is not None
        ]
        return {
            "schema_version": 2,
            "mode": "authenticated_read_only",
            "source": "official_order_scoring",
            "generated_at": float(now),
            "account_uid_key": self.account_uid_key or None,
            "host_id": self.host_id or None,
            "checkpoints_sec": list(self.checkpoints_sec),
            "steady_state_interval_sec": self.steady_state_interval_sec,
            "last_error": self._last_error or None,
            "summary": {
                "tracked_orders": len(rows),
                "live_orders": sum(1 for row in rows if row.get("live")),
                "orders_seen_scoring": len(first_scoring),
                "earliest_confirmed_scoring_sec": min(first_scoring) if first_scoring else None,
            },
            "orders": {row["order_id"]: row for row in rows},
        }
