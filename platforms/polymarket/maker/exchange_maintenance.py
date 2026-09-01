from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Any, Callable, Optional


PHASE_NORMAL = "normal"
PHASE_MAINTENANCE = "maintenance"
PHASE_RECOVERING = "recovering"


def _status_code(error: BaseException) -> Optional[int]:
    for candidate in (
        getattr(error, "status_code", None),
        getattr(getattr(error, "response", None), "status_code", None),
    ):
        try:
            if candidate is not None:
                return int(candidate)
        except (TypeError, ValueError):
            continue
    return None


def classify_exchange_maintenance(error: BaseException) -> Optional[str]:
    """Return a narrow, non-secret maintenance reason for official CLOB errors."""
    message = str(error).strip().lower()
    if "trading is currently disabled" in message:
        return "trading_disabled"
    if "cancels are disabled" in message or "cancellations are disabled" in message:
        return "cancellations_disabled"
    if (
        "order placement is disabled" in message
        or "new orders are disabled" in message
        or "cancel-only mode" in message
    ):
        return "order_placement_disabled"
    if _status_code(error) == 425:
        return "matching_engine_restarting"
    return None


@dataclass(frozen=True)
class MaintenanceUpdate:
    reason: str
    entered: bool
    reason_changed: bool


@dataclass
class ExchangeMaintenanceGuard:
    """Fail-closed exchange maintenance latch with bounded recovery probes."""

    min_hold_sec: float = 60.0
    recovery_read_successes: int = 3
    recovery_read_spacing_sec: float = 10.0
    cancel_backoff_initial_sec: float = 30.0
    cancel_backoff_max_sec: float = 300.0
    buy_probe_retry_sec: float = 30.0
    clock: Callable[[], float] = time.time

    phase: str = PHASE_NORMAL
    reason: str = ""
    entered_at: float = 0.0
    last_error_at: float = 0.0
    last_action: str = ""
    read_success_streak: int = 0
    last_read_success_at: float = 0.0
    next_read_success_at: float = 0.0
    cancel_failure_count: int = 0
    next_cancel_retry_at: float = 0.0
    cancel_probe_inflight: bool = False
    buy_probe_inflight: bool = False
    next_buy_probe_at: float = 0.0
    recovery_probe_count: int = 0
    _lock: threading.RLock = field(
        default_factory=threading.RLock,
        init=False,
        repr=False,
    )

    @property
    def active(self) -> bool:
        return self.phase != PHASE_NORMAL

    @property
    def blocks_new_buys(self) -> bool:
        return self.phase == PHASE_MAINTENANCE

    def observe_error(
        self,
        error: BaseException,
        action: str,
        *,
        now: Optional[float] = None,
    ) -> Optional[MaintenanceUpdate]:
        reason = classify_exchange_maintenance(error)
        if reason is None:
            return None
        current = self.clock() if now is None else float(now)
        with self._lock:
            return self._enter_maintenance(
                reason,
                action,
                current=current,
                cancel_failure="cancel" in str(action).lower(),
                immediate_cancel=False,
            )

    def force_maintenance(
        self,
        reason: str,
        action: str,
        *,
        cancel_failure: bool = False,
        immediate_cancel: bool = False,
        now: Optional[float] = None,
    ) -> MaintenanceUpdate:
        """Return to maintenance after an internally failed recovery proof."""
        current = self.clock() if now is None else float(now)
        with self._lock:
            return self._enter_maintenance(
                str(reason),
                action,
                current=current,
                cancel_failure=cancel_failure,
                immediate_cancel=immediate_cancel,
            )

    def note_authenticated_read_success(
        self,
        *,
        now: Optional[float] = None,
    ) -> bool:
        """Return True only when the guard advances into recovering."""
        current = self.clock() if now is None else float(now)
        with self._lock:
            if self.phase != PHASE_MAINTENANCE:
                return False
            if current - self.last_error_at < self.min_hold_sec:
                return False
            if current < self.next_read_success_at:
                return False
            self.read_success_streak += 1
            self.last_read_success_at = current
            self.next_read_success_at = current + self.recovery_read_spacing_sec
            if self.read_success_streak < max(1, self.recovery_read_successes):
                return False
            self.phase = PHASE_RECOVERING
            self.buy_probe_inflight = False
            self.next_buy_probe_at = current
            return True

    def claim_buy_attempt(self, *, now: Optional[float] = None) -> Optional[str]:
        current = self.clock() if now is None else float(now)
        with self._lock:
            if self.phase == PHASE_NORMAL:
                return "normal"
            if self.phase != PHASE_RECOVERING:
                return None
            if (
                self.buy_probe_inflight
                or self.cancel_probe_inflight
                or current < self.next_buy_probe_at
                or current < self.next_cancel_retry_at
            ):
                return None
            self.buy_probe_inflight = True
            # The recovery BUY owns the cancellation lane until its exact
            # order has been canceled and verified absent. This prevents the
            # global kill switch from racing the proof sequence.
            self.cancel_probe_inflight = True
            self.recovery_probe_count += 1
            return f"recovery_probe:{self.recovery_probe_count}"

    def finish_buy_attempt(
        self,
        claim: Optional[str],
        *,
        success: bool,
        now: Optional[float] = None,
    ) -> bool:
        """Release a probe claim; return True when a post recovers the venue."""
        if not claim or not claim.startswith("recovery_probe:"):
            return False
        current = self.clock() if now is None else float(now)
        with self._lock:
            self.buy_probe_inflight = False
            self.cancel_probe_inflight = False
            if success and self.phase == PHASE_RECOVERING:
                self._reset_to_normal()
                return True
            if self.phase == PHASE_RECOVERING:
                self.next_buy_probe_at = current + self.buy_probe_retry_sec
            return False

    def cancel_retry_delay(self, *, now: Optional[float] = None) -> float:
        current = self.clock() if now is None else float(now)
        with self._lock:
            if not self.active:
                return 0.0
            return max(0.0, self.next_cancel_retry_at - current)

    def claim_cancel_attempt(self, *, now: Optional[float] = None) -> Optional[str]:
        current = self.clock() if now is None else float(now)
        with self._lock:
            if not self.active:
                return "normal"
            if self.cancel_probe_inflight or current < self.next_cancel_retry_at:
                return None
            self.cancel_probe_inflight = True
            return "maintenance_cancel_probe"

    def finish_cancel_attempt(
        self,
        claim: Optional[str],
        *,
        success: bool,
        now: Optional[float] = None,
    ) -> None:
        if claim != "maintenance_cancel_probe":
            return
        current = self.clock() if now is None else float(now)
        with self._lock:
            self.cancel_probe_inflight = False
            if success:
                self.cancel_failure_count = 0
                self.next_cancel_retry_at = 0.0
            elif self.active and self.next_cancel_retry_at <= current:
                self.next_cancel_retry_at = (
                    current + self.cancel_backoff_initial_sec
                )

    def note_cancel_success(self) -> None:
        with self._lock:
            self.cancel_failure_count = 0
            self.next_cancel_retry_at = 0.0

    def snapshot(self, *, now: Optional[float] = None) -> dict[str, Any]:
        current = self.clock() if now is None else float(now)
        with self._lock:
            return {
                "active": self.active,
                "phase": self.phase,
                "reason": self.reason or None,
                "entered_at": self.entered_at or None,
                "last_error_at": self.last_error_at or None,
                "last_action": self.last_action or None,
                "read_success_streak": self.read_success_streak,
                "read_success_required": max(1, self.recovery_read_successes),
                "cancel_failure_count": self.cancel_failure_count,
                "cancel_probe_inflight": self.cancel_probe_inflight,
                "cancel_retry_in_sec": max(
                    0.0,
                    self.next_cancel_retry_at - current,
                ),
                "buy_probe_inflight": self.buy_probe_inflight,
                "buy_probe_in_sec": max(0.0, self.next_buy_probe_at - current),
                "recovery_probe_count": self.recovery_probe_count,
            }

    def _enter_maintenance(
        self,
        reason: str,
        action: str,
        *,
        current: float,
        cancel_failure: bool,
        immediate_cancel: bool,
    ) -> MaintenanceUpdate:
        previous_phase = self.phase
        entered = previous_phase != PHASE_MAINTENANCE
        reason_changed = self.reason != reason
        if previous_phase == PHASE_NORMAL:
            self.entered_at = current
        self.phase = PHASE_MAINTENANCE
        self.reason = reason
        self.last_error_at = current
        self.last_action = str(action)
        self.read_success_streak = 0
        self.last_read_success_at = 0.0
        self.next_read_success_at = current + self.recovery_read_spacing_sec
        self.buy_probe_inflight = False
        self.next_buy_probe_at = 0.0
        self.cancel_probe_inflight = False

        if immediate_cancel:
            self.next_cancel_retry_at = current
        elif cancel_failure:
            self.cancel_failure_count += 1
            exponent = max(0, self.cancel_failure_count - 1)
            delay = min(
                self.cancel_backoff_max_sec,
                self.cancel_backoff_initial_sec * (2**exponent),
            )
            self.next_cancel_retry_at = max(
                self.next_cancel_retry_at,
                current + delay,
            )
        else:
            delay = self.cancel_backoff_initial_sec
            self.next_cancel_retry_at = max(
                self.next_cancel_retry_at,
                current + delay,
            )
        return MaintenanceUpdate(
            reason=reason,
            entered=entered,
            reason_changed=reason_changed,
        )

    def _reset_to_normal(self) -> None:
        self.phase = PHASE_NORMAL
        self.reason = ""
        self.entered_at = 0.0
        self.last_error_at = 0.0
        self.last_action = ""
        self.read_success_streak = 0
        self.last_read_success_at = 0.0
        self.next_read_success_at = 0.0
        self.cancel_failure_count = 0
        self.next_cancel_retry_at = 0.0
        self.cancel_probe_inflight = False
        self.buy_probe_inflight = False
        self.next_buy_probe_at = 0.0
