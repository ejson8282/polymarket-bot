import asyncio
import json
from decimal import Decimal
from pathlib import Path
import sys

import pytest

MAKER_DIR = Path(__file__).resolve().parents[1] / "platforms" / "polymarket" / "maker"
sys.path.insert(0, str(MAKER_DIR))

from account_profiles import parse_lp_account_profile  # noqa: E402
from aggressive_guardrails import (  # noqa: E402
    AggressiveGuardrailState,
    risk_day_key,
)
import engine as engine_module  # noqa: E402
from engine import PolyLPSMulti  # noqa: E402


def test_risk_day_uses_beijing_0800_cutoff() -> None:
    before = 1785887940.0  # 2026-08-05 07:59 CST
    after = 1785888000.0   # 2026-08-05 08:00 CST

    assert risk_day_key(before) == "2026-08-04"
    assert risk_day_key(after) == "2026-08-05"


def test_daily_loss_is_measured_from_persisted_equity_baseline() -> None:
    state = AggressiveGuardrailState()
    first = state.observe(
        equity=Decimal("100"),
        collateral=Decimal("80"),
        position_value=Decimal("20"),
        now=1785888000.0,
        cutoff_hour=8,
        baseline_cap=Decimal("100"),
        pause_equity=Decimal("85"),
        daily_loss_limit=Decimal("5"),
    )
    second = state.observe(
        equity=Decimal("94.99"),
        collateral=Decimal("74.99"),
        position_value=Decimal("20"),
        now=1785888060.0,
        cutoff_hour=8,
        baseline_cap=Decimal("100"),
        pause_equity=Decimal("85"),
        daily_loss_limit=Decimal("5"),
    )

    assert first == ""
    assert second == "daily_loss:5.01>=5"
    assert state.daily_loss_usdc == "5.01"


def test_new_risk_day_resets_baseline_without_clearing_latch() -> None:
    state = AggressiveGuardrailState(latched=True, reason="daily_loss")
    state.observe(
        equity=Decimal("90"),
        collateral=Decimal("90"),
        position_value=Decimal("0"),
        now=1785888000.0,
        cutoff_hour=8,
        baseline_cap=Decimal("100"),
        pause_equity=Decimal("80"),
        daily_loss_limit=Decimal("5"),
    )
    state.observe(
        equity=Decimal("92"),
        collateral=Decimal("92"),
        position_value=Decimal("0"),
        now=1785974400.0,
        cutoff_hour=8,
        baseline_cap=Decimal("100"),
        pause_equity=Decimal("80"),
        daily_loss_limit=Decimal("5"),
    )

    assert state.baseline_equity_usdc == "92"
    assert state.latched is True


def test_unavailable_equity_fails_closed_only_after_stale_window() -> None:
    state = AggressiveGuardrailState()

    assert state.observe_failure(now=100.0, stale_after_sec=90.0) == ""
    assert state.observe_failure(now=189.9, stale_after_sec=90.0) == ""
    assert state.observe_failure(now=190.0, stale_after_sec=90.0) == (
        "equity_unavailable:90s"
    )


def test_profit_withdrawal_above_principal_is_not_counted_as_daily_loss() -> None:
    state = AggressiveGuardrailState()
    state.observe(
        equity=Decimal("105"),
        collateral=Decimal("105"),
        position_value=Decimal("0"),
        now=1785888000.0,
        cutoff_hour=8,
        baseline_cap=Decimal("100"),
        pause_equity=Decimal("85"),
        daily_loss_limit=Decimal("5"),
    )
    reason = state.observe(
        equity=Decimal("100"),
        collateral=Decimal("100"),
        position_value=Decimal("0"),
        now=1785888060.0,
        cutoff_hour=8,
        baseline_cap=Decimal("100"),
        pause_equity=Decimal("85"),
        daily_loss_limit=Decimal("5"),
    )

    assert state.baseline_equity_usdc == "100"
    assert state.daily_loss_usdc == "0"
    assert reason == ""


def test_guardrail_state_round_trips_and_rejects_non_object(tmp_path) -> None:
    path = tmp_path / "guardrail.json"
    state = AggressiveGuardrailState(latched=True, reason="equity_floor")
    state.save(path)

    assert AggressiveGuardrailState.load(path).reason == "equity_floor"
    path.write_text("[]", encoding="utf-8")
    assert AggressiveGuardrailState.load(path) == AggressiveGuardrailState()


class _EventBus:
    def __init__(self) -> None:
        self.events = []

    def publish(self, name, payload) -> None:
        self.events.append((name, payload))


def _engine(tmp_path) -> PolyLPSMulti:
    engine = PolyLPSMulti.__new__(PolyLPSMulti)
    engine._account_idx = 3
    engine.lp_account_profile = parse_lp_account_profile(
        {
            "lp_account": {
                "account_id": "aggressive_100",
                "profile_type": "aggressive",
                "target_principal_usdc": 100,
            }
        },
        3,
    )
    engine._pause_flag_path = tmp_path / ".account_3.paused"
    engine._aggressive_guardrail_state_path = tmp_path / "guardrail_3.json"
    engine._aggressive_guardrail_latch_path = tmp_path / ".account_3.guardrail"
    engine._aggressive_guardrail_reset_path = tmp_path / ".account_3.reset"
    engine._aggressive_guardrail_state = AggressiveGuardrailState(
        last_equity_usdc="84",
        daily_loss_usdc="6",
    )
    engine._aggressive_guardrail_cancel_complete = False
    engine._aggressive_guardrail_cutoff_hour = 8
    engine._event_bus = _EventBus()
    engine.notifications = []
    engine.notify_discord = lambda *args: engine.notifications.append(args)
    return engine


def test_trigger_is_account_local_and_preserves_exit_cancel_path(tmp_path) -> None:
    engine = _engine(tmp_path)
    calls = []

    async def cancel_except_exit():
        calls.append("cancel")
        return True

    engine._cancel_all_except_exit = cancel_except_exit
    asyncio.run(engine._trigger_aggressive_guardrail("equity_floor:84<=85", 123.0))
    asyncio.run(engine._trigger_aggressive_guardrail("equity_floor:84<=85", 124.0))

    assert calls == ["cancel"]
    assert engine._pause_flag_path.exists()
    assert engine._aggressive_guardrail_latch_path.exists()
    latch = json.loads(engine._aggressive_guardrail_latch_path.read_text())
    assert latch["account_index"] == 3
    assert "private" not in json.dumps(latch).lower()
    assert engine._event_bus.events[0][0] == "aggressive_guardrail_triggered"
    assert len(engine.notifications) == 1


def test_manual_reset_requires_equity_above_floor(tmp_path) -> None:
    engine = _engine(tmp_path)
    engine._aggressive_guardrail_latch_path.touch()
    engine._pause_flag_path.touch()
    engine._aggressive_guardrail_reset_path.touch()

    async def low_equity():
        return Decimal("85"), Decimal("80"), Decimal("5")

    engine._get_aggressive_equity_snapshot = low_equity
    assert asyncio.run(engine._reset_aggressive_guardrail()) is False
    assert engine._aggressive_guardrail_latch_path.exists()

    async def healthy_equity():
        return Decimal("90"), Decimal("80"), Decimal("10")

    engine._get_aggressive_equity_snapshot = healthy_equity
    assert asyncio.run(engine._reset_aggressive_guardrail()) is True
    assert not engine._aggressive_guardrail_latch_path.exists()
    assert not engine._pause_flag_path.exists()
    assert engine._aggressive_guardrail_state.baseline_equity_usdc == "90"


def test_reset_request_cannot_remove_an_unrelated_manual_pause(tmp_path) -> None:
    engine = _engine(tmp_path)
    engine._aggressive_guardrail_state.latched = False
    engine._pause_flag_path.touch()
    engine._aggressive_guardrail_reset_path.touch()

    assert asyncio.run(engine._reset_aggressive_guardrail()) is False
    assert engine._pause_flag_path.exists()


def test_equity_snapshot_combines_cash_and_position_value(tmp_path) -> None:
    engine = _engine(tmp_path)

    async def collateral():
        return Decimal("70")

    async def positions():
        return Decimal("25.50")

    engine._get_collateral_balance = collateral
    engine._get_total_position_value = positions

    assert asyncio.run(engine._get_aggressive_equity_snapshot()) == (
        Decimal("95.50"),
        Decimal("70"),
        Decimal("25.50"),
    )


def test_position_value_accepts_only_the_configured_account(tmp_path, monkeypatch) -> None:
    engine = _engine(tmp_path)
    engine.cfg = {}
    engine._funder_lc = "0x" + "a" * 40
    engine._read_proxies_for_token = lambda: None

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return [{"user": engine._funder_lc, "value": 12.5}]

    monkeypatch.setattr(engine_module.requests, "get", lambda *args, **kwargs: Response())
    assert asyncio.run(engine._get_total_position_value()) == Decimal("12.5")

    class WrongAccountResponse(Response):
        def json(self):
            return [{"user": "0x" + "b" * 40, "value": 12.5}]

    monkeypatch.setattr(
        engine_module.requests,
        "get",
        lambda *args, **kwargs: WrongAccountResponse(),
    )
    with pytest.raises(RuntimeError, match="user mismatch"):
        asyncio.run(engine._get_total_position_value())
