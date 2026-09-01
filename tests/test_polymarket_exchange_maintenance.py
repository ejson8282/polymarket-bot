from pathlib import Path
import sys


MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from exchange_maintenance import (  # noqa: E402
    ExchangeMaintenanceGuard,
    PHASE_MAINTENANCE,
    PHASE_NORMAL,
    PHASE_RECOVERING,
    classify_exchange_maintenance,
)


class _Clock:
    def __init__(self, value: float = 1000.0):
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class _StatusError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


def test_classifier_is_narrow_and_uses_official_maintenance_signals():
    assert classify_exchange_maintenance(
        RuntimeError(
            '503 {"error":"Trading is currently disabled. '
            'Check polymarket.com for updates"}'
        )
    ) == "trading_disabled"
    assert classify_exchange_maintenance(
        RuntimeError('503 {"error":"cancels are disabled"}')
    ) == "cancellations_disabled"
    assert classify_exchange_maintenance(
        RuntimeError("exchange is in cancel-only mode")
    ) == "order_placement_disabled"
    assert classify_exchange_maintenance(
        _StatusError(425, "matching engine warming up")
    ) == "matching_engine_restarting"

    assert classify_exchange_maintenance(
        _StatusError(503, "Service unavailable")
    ) is None
    assert classify_exchange_maintenance(
        _StatusError(500, "Internal server error")
    ) is None
    assert classify_exchange_maintenance(
        _StatusError(429, "Too many requests")
    ) is None


def test_maintenance_latch_blocks_buys_and_backs_off_cancel_retries():
    clock = _Clock()
    guard = ExchangeMaintenanceGuard(
        clock=clock,
        cancel_backoff_initial_sec=30,
        cancel_backoff_max_sec=120,
    )

    update = guard.observe_error(
        RuntimeError("Trading is currently disabled"),
        "cancel_all_except_exit",
    )

    assert update is not None and update.entered is True
    assert guard.phase == PHASE_MAINTENANCE
    assert guard.blocks_new_buys is True
    assert guard.claim_buy_attempt() is None
    assert guard.cancel_retry_delay() == 30
    assert guard.claim_cancel_attempt() is None

    clock.advance(30)
    claim = guard.claim_cancel_attempt()
    assert claim == "maintenance_cancel_probe"
    guard.observe_error(
        RuntimeError("cancels are disabled"),
        "cancel_all_except_exit",
    )
    guard.finish_cancel_attempt(claim, success=False)
    assert guard.cancel_retry_delay() == 60


def test_recovery_requires_spaced_reads_then_exactly_one_post_only_probe():
    clock = _Clock()
    guard = ExchangeMaintenanceGuard(
        clock=clock,
        min_hold_sec=60,
        recovery_read_successes=3,
        recovery_read_spacing_sec=10,
        buy_probe_retry_sec=30,
    )
    guard.observe_error(
        RuntimeError("Trading is currently disabled"),
        "post_only_buy",
    )

    clock.advance(59)
    assert guard.note_authenticated_read_success() is False
    assert guard.read_success_streak == 0

    clock.advance(1)
    assert guard.note_authenticated_read_success() is False
    clock.advance(10)
    assert guard.note_authenticated_read_success() is False
    clock.advance(10)
    assert guard.note_authenticated_read_success() is True
    assert guard.phase == PHASE_RECOVERING

    first = guard.claim_buy_attempt()
    assert first == "recovery_probe:1"
    assert guard.claim_buy_attempt() is None
    guard.finish_buy_attempt(first, success=False)
    assert guard.phase == PHASE_RECOVERING
    assert guard.claim_buy_attempt() is None

    clock.advance(30)
    second = guard.claim_buy_attempt()
    assert second == "recovery_probe:2"
    assert guard.finish_buy_attempt(second, success=True) is True
    assert guard.phase == PHASE_NORMAL
    assert guard.blocks_new_buys is False


def test_new_maintenance_error_resets_recovery_evidence():
    clock = _Clock()
    guard = ExchangeMaintenanceGuard(
        clock=clock,
        min_hold_sec=0,
        recovery_read_successes=2,
        recovery_read_spacing_sec=5,
    )
    guard.observe_error(RuntimeError("cancels are disabled"), "cancel")
    clock.advance(5)
    assert guard.note_authenticated_read_success() is False
    assert guard.read_success_streak == 1

    update = guard.observe_error(
        RuntimeError("Trading is currently disabled"),
        "read",
    )
    assert update is not None and update.reason_changed is True
    assert guard.read_success_streak == 0
    assert guard.phase == PHASE_MAINTENANCE


def test_snapshot_contains_state_without_raw_exchange_error_text():
    clock = _Clock()
    guard = ExchangeMaintenanceGuard(clock=clock)
    guard.observe_error(
        RuntimeError(
            "Trading is currently disabled; Authorization: secret-value"
        ),
        "post_only_buy",
    )

    state = guard.snapshot()

    assert state["active"] is True
    assert state["reason"] == "trading_disabled"
    assert "secret-value" not in repr(state)
