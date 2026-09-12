"""Fault regressions for the existing LP submit/cancel path, with no venue IO."""

import asyncio
from decimal import Decimal as D
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "platforms/polymarket/maker"))
import engine as mod
from aggressive_guardrails import AggressiveGuardrailState
from account_profiles import parse_lp_account_profile
from tests.test_polymarket_aggressive_guardrails import _engine as guard_engine
from tests.test_polymarket_maker_engine import (
    _aggressive_pair_submit_engine, _budget_engine, _RecordingEventBus,
)


def profile():
    return parse_lp_account_profile({"lp_account": {
        "profile_type": "aggressive", "target_principal_usdc": 200,
        "pause_equity_usdc": 170, "daily_loss_limit_usdc": 10,
        "auto_top_up": False, "auto_sweep": False,
    }}, 3)


def prepare_submit(e):
    e._is_account_paused = lambda: False
    e._validate_passive_buy_quote = AsyncMock()
    e._mark_latency = lambda *_: None
    e._invalidate_all_orders_cache = lambda: None
    e.remote_signer = None
    e._sibling_gate = lambda _t, _s, price, _l: price
    e._sibling_register_resp = lambda *_: None
    e._refresh_live_orders = AsyncMock(return_value=[])
    e._managed_buy_order_ids = set()
    e._managed_buy_order_ids_order = []
    e._managed_order_history_limit = 100
    e._market_live_orders = {}
    return e


@pytest.mark.parametrize("available", [None, D("0"), D("NaN"), D("Infinity")])
def test_required_balance_rejected_before_signing_or_post(available):
    e = prepare_submit(_budget_engine())
    e.lp_account_profile = profile()
    e._halt_preemption_reason = lambda *_: None
    e._get_collateral_available = AsyncMock(return_value=available)
    e.budget_reserve_enabled = True
    e.budget_reserve_safety_margin_usdc = D("1")
    e._budget_reserve_lock = asyncio.Lock()
    e._budget_reserve_seq = 0
    e.client = SimpleNamespace(
        create_order=lambda *_: pytest.fail("must not sign"),
        post_order=lambda *_: pytest.fail("must not post"),
    )
    with pytest.raises(mod.SoftQuoteSkip, match="balance_unavailable"):
        asyncio.run(e._submit_post_order("101", D("0.41"), D("100"), "test"))
    assert not e._pending_order_reserve


@pytest.mark.parametrize("failing_write", ["state", "latch", "pause", "all"])
def test_loss_stop_survives_failed_writes(tmp_path, monkeypatch, failing_write):
    e = guard_engine(tmp_path)
    e.lp_account_profile = profile()
    e._aggressive_guardrail_state.observe(
        equity=D("200"), collateral=D("200"), position_value=D("0"),
        now=time.time(), cutoff_hour=8, baseline_cap=D("200"),
        pause_equity=D("170"), daily_loss_limit=D("10"),
    )
    e._get_aggressive_equity_snapshot = AsyncMock(return_value=(D("190"), D("190"), D("0")))
    e._cancel_all_except_exit = AsyncMock(return_value=True)
    e._aggressive_guardrail_interval_sec = 15
    e._aggressive_guardrail_stale_after_sec = 90
    e._running = True

    def fail(*_args, **_kwargs):
        raise OSError("injected disk full")

    if failing_write in {"state", "all"}:
        monkeypatch.setattr(AggressiveGuardrailState, "save", fail)
    if failing_write in {"latch", "all"}:
        e._write_aggressive_guardrail_latch = fail
    if failing_write in {"pause", "all"}:
        monkeypatch.setattr(Path, "touch", fail)

    async def stop(_):
        e._running = False

    monkeypatch.setattr(mod.asyncio, "sleep", stop)
    asyncio.run(e.aggressive_guardrail_loop())
    assert e._aggressive_guardrail_state.daily_loss_usdc == "10"
    assert e._aggressive_guardrail_state.latched
    assert e._aggressive_guardrail_storage_error
    e._cancel_all_except_exit.assert_awaited_once()
    with pytest.raises(mod.EventHaltPreempted):
        e._ensure_order_path_open("101", "after_fault")
    if failing_write == "state":
        assert e._aggressive_guardrail_latch_path.exists()
    if failing_write == "latch":
        assert AggressiveGuardrailState.load(e._aggressive_guardrail_state_path).latched


def test_aggressive_startup_requires_successful_guard_observation():
    e = _aggressive_pair_submit_engine()
    e._aggressive_guardrails_enabled = True
    e._aggressive_guardrail_ready = False
    with pytest.raises(mod.EventHaltPreempted, match="startup"):
        e._ensure_order_path_open("101", "startup")
    e._aggressive_guardrail_ready = True
    e._is_account_paused = lambda: False
    e._ensure_order_path_open("101", "ready")


def test_initial_equity_failure_cannot_open_startup_gate(tmp_path, monkeypatch):
    e = guard_engine(tmp_path)
    e._aggressive_guardrails_enabled = True
    e._aggressive_guardrail_ready = False
    e._aggressive_guardrail_state = AggressiveGuardrailState()
    e._get_aggressive_equity_snapshot = AsyncMock(side_effect=OSError("unavailable"))
    e._aggressive_guardrail_interval_sec = 15
    e._aggressive_guardrail_stale_after_sec = 90
    e._running = True

    async def stop(_):
        e._running = False

    monkeypatch.setattr(mod.asyncio, "sleep", stop)
    asyncio.run(e.aggressive_guardrail_loop())
    assert not e._aggressive_guardrail_ready
    with pytest.raises(mod.EventHaltPreempted, match="startup"):
        e._ensure_order_path_open("101", "failed_first_sample")


def test_reset_with_failed_storage_retains_buy_stop(tmp_path, monkeypatch):
    e = guard_engine(tmp_path)
    e._aggressive_guardrail_state.latched = True
    e._get_aggressive_equity_snapshot = AsyncMock(return_value=(D("100"), D("100"), D("0")))
    e._cancel_all_except_exit = AsyncMock(return_value=True)

    def fail(*_):
        raise OSError("injected disk full")

    monkeypatch.setattr(AggressiveGuardrailState, "save", fail)
    assert not asyncio.run(e._reset_aggressive_guardrail())
    assert e._aggressive_guardrail_state.latched
    with pytest.raises(mod.EventHaltPreempted):
        e._ensure_order_path_open("101", "failed_reset")


def test_kill_switch_blocks_before_cancel_ack():
    e = _aggressive_pair_submit_engine()
    e._running = True
    e._kill_switch_lock = asyncio.Lock()
    e._cooldown_until = 0
    e._require_recovery_gate = False
    e._active_exit_orders = {}
    e._is_account_paused = lambda: False

    async def run():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def cancel():
            entered.set()
            await release.wait()
            return True

        e._cancel_all_except_exit = cancel
        task = asyncio.create_task(e.trigger_global_kill_switch("remote_signer_unreachable"))
        try:
            await asyncio.wait_for(entered.wait(), 1)
            with pytest.raises(mod.EventHaltPreempted, match="kill_switch"):
                e._ensure_order_path_open("101", "while_canceling")
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
    assert e._kill_switch_cancel_pending


@pytest.mark.parametrize("cause", ["pair_timeout", "waiter_cancel", "global_stop"])
def test_late_post_is_owned_until_cleanup_and_preserves_sell(cause):
    e = prepare_submit(_aggressive_pair_submit_engine())
    e._top_leg_defense_tasks = {}
    e._aggressive_pair_submit_timeout_sec = 0
    e._event_bus = _RecordingEventBus()
    e._defense_block_until = {}
    e._defense_requote_block_sec = 0
    e._acquire_budget_reserve = AsyncMock(return_value="reserved")
    e._release_budget_reserve = AsyncMock()
    e.notify_discord = lambda *_: None
    live = [
        {"id": "leader-buy", "asset_id": "101", "side": "BUY", "status": "LIVE"},
        {"id": "exit-sell", "asset_id": "102", "side": "SELL", "status": "LIVE"},
    ]
    entered, release = threading.Event(), threading.Event()

    def post(*_, **__):
        entered.set()
        assert release.wait(3)
        live.append({"id": "late-buy", "asset_id": "102", "side": "BUY", "status": "LIVE"})
        return {"orderID": "late-buy", "success": True}

    async def refresh(token):
        rows = [o for o in live if o["asset_id"] == token]
        e._market_live_orders[token] = rows
        return rows

    async def cancel(token, reason):
        live[:] = [o for o in live if not (o["asset_id"] == token and o["side"] == "BUY")]
        await refresh(token)
        return True

    e.client = SimpleNamespace(create_order=lambda *_: object(), post_order=post)
    e._refresh_live_orders = refresh
    e._cancel_risk_buys = cancel

    async def run():
        leader = await e._claim_aggressive_pair_submit("101")
        follower = await e._claim_aggressive_pair_submit("102")
        attempt = await e._aggressive_pair_attempt(leader)
        attempt["posted"].add("101")
        attempt["first_post_at"] = time.time()
        attempt["leader_posted"].set()
        e._preflight_post_order = AsyncMock(return_value=follower)
        task = asyncio.create_task(e._place_post_only_order_fast("102", D("0.56"), D("100"), "test"))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            if cause == "pair_timeout":
                await e._aggressive_pair_submit_watchdog(leader[0], leader[1], "101")
                assert e._aggressive_pair_submit_attempts == {}
            elif cause == "waiter_cancel":
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
            else:
                e._buy_stop_epoch = 1
            e._release_budget_reserve.assert_not_awaited()
            with pytest.raises(mod.EventHaltPreempted, match="cleanup_pending"):
                e._ensure_order_path_open("101", "before_late_receipt")
        finally:
            release.set()
            result = (await asyncio.gather(task, return_exceptions=True))[0]
        expected = asyncio.CancelledError if cause == "waiter_cancel" else mod.EventHaltPreempted
        assert isinstance(result, expected), repr(result)

    asyncio.run(run())
    assert [o["id"] for o in live] == ["exit-sell"]
    assert "late-buy" in e._managed_buy_order_ids
    assert not e._buy_posts_inflight
    e._release_budget_reserve.assert_awaited_once_with("reserved")


def test_overdue_single_leg_still_requests_cleanup_when_rest_fails(monkeypatch):
    e = _aggressive_pair_submit_engine()
    e._running = True
    e._paired_single_leg_grace_sec = 0
    e._paired_reconcile_interval_sec = 0
    e._paired_single_leg_since = {"101|102": time.time() - 100}
    e._market_live_orders = {"101": [{"id": "known-buy", "side": "BUY", "status": "LIVE"}], "102": []}
    e._invalidate_all_orders_cache = lambda: None
    e._refresh_live_orders = AsyncMock(side_effect=RuntimeError("REST unavailable"))
    e._cancel_coordinated_pair_quotes = AsyncMock(return_value=False)

    async def stop(_):
        e._running = False

    monkeypatch.setattr(mod.asyncio, "sleep", stop)
    asyncio.run(e.paired_quote_invariant_loop())
    e._cancel_coordinated_pair_quotes.assert_awaited()
    assert "101|102" in e._paired_single_leg_since
