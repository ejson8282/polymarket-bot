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


def fault_venue():
    """Real coordinators and cancel logic; only venue IO is synthetic."""
    e = prepare_submit(_aggressive_pair_submit_engine())
    e.market_cfg.pop("103", None)
    e._top_leg_defense_tasks = {}
    e._event_bus = _RecordingEventBus()
    e._defense_block_until = {}
    e._defense_requote_block_sec = 0
    e._aggressive_pair_submit_timeout_sec = 0
    e._acquire_budget_reserve = AsyncMock(return_value="reserved")
    e._release_budget_reserve = AsyncMock()
    e.notify_discord = lambda *_: None
    e._running = True
    e._kill_switch_lock = asyncio.Lock()
    e._cooldown_until = 0
    e._require_recovery_gate = False
    e.cooldown_seconds = 60
    e.cancel_retry_step_sec = 1
    e._active_exit_orders = {"102": "exit-sell"}
    e._sibling_registry = SimpleNamespace(clear_funder=lambda *_, **__: None)
    e._funder_lc = "synthetic"
    live = [
        {"id": "leader-buy", "asset_id": "101", "side": "BUY", "status": "LIVE"},
        {"id": "exit-sell", "asset_id": "102", "side": "SELL", "status": "LIVE"},
    ]
    entered, release = threading.Event(), threading.Event()
    cancel_calls = []

    def post(*_, **__):
        entered.set()
        assert release.wait(4), "synthetic worker timeout"
        live.append({"id": "late-buy", "asset_id": "102", "side": "BUY", "status": "LIVE"})
        return {"orderID": "late-buy", "success": True}

    async def refresh(token):
        await asyncio.sleep(0)
        rows = [dict(o) for o in live if o["asset_id"] == token]
        e._market_live_orders[token] = rows
        return rows

    async def read(_action):
        await asyncio.sleep(0)
        return [dict(o) for o in live]

    async def cancel_ids(token, ids, reason):
        await asyncio.sleep(0)
        cancel_calls.append((token, list(ids), reason))
        live[:] = [o for o in live if o["id"] not in ids]
        return True

    async def execute(_action, _method, ids):
        await asyncio.sleep(0)
        live[:] = [o for o in live if o["id"] not in ids]
        return True

    e.client = SimpleNamespace(create_order=lambda *_: object(), post_order=post, cancel_orders=lambda *_: None)
    e._refresh_live_orders = refresh
    e._read_open_orders = read
    e._cancel_order_ids = cancel_ids
    e._execute_exchange_cancel = execute
    return e, live, entered, release, cancel_calls


async def start_follower(e):
    leader = await e._claim_aggressive_pair_submit("101")
    claim = await e._claim_aggressive_pair_submit("102")
    attempt = await e._aggressive_pair_attempt(leader)
    attempt["posted"].add("101")
    attempt["first_post_at"] = time.time()
    attempt["leader_posted"].set()
    e._preflight_post_order = AsyncMock(return_value=claim)
    task = asyncio.create_task(e._place_post_only_order_fast("102", D("0.56"), D("100"), "test"))
    return leader, task


@pytest.mark.parametrize("cancel_while_worker_runs", [False, True])
def test_cancel_during_receipt_cleanup_keeps_owner_and_reserve(cancel_while_worker_runs):
    e, live, entered, release, _ = fault_venue()

    async def run():
        leader, task = await start_follower(e)
        cleanup_entered = asyncio.Event()
        finish_cleanup = asyncio.Event()
        real_cancel = e._cancel_order_ids

        async def paused_cancel(token, ids, reason):
            if "late-buy" in ids:
                cleanup_entered.set()
                await finish_cleanup.wait()
            return await real_cancel(token, ids, reason)

        e._cancel_order_ids = paused_cancel
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            if cancel_while_worker_runs:
                task.cancel()
                await asyncio.sleep(0)
                assert not task.done()
                e._release_budget_reserve.assert_not_awaited()
            await asyncio.wait_for(e._aggressive_pair_submit_watchdog(leader[0], leader[1], "101"), 1)
            assert not e._aggressive_pair_submit_attempts
            release.set()
            await asyncio.wait_for(cleanup_entered.wait(), 1)
            task.cancel()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            e._release_budget_reserve.assert_not_awaited()
            assert not task.done()
            assert e._buy_posts_inflight
        finally:
            release.set()
            finish_cleanup.set()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)
        assert [o["id"] for o in live] == ["exit-sell"]
        assert not e._buy_posts_inflight
        e._release_budget_reserve.assert_awaited_once_with("reserved")

    asyncio.run(run())


def test_real_global_stop_cleans_delayed_receipt():
    e, live, entered, release, _ = fault_venue()

    async def run():
        _leader, task = await start_follower(e)
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            await asyncio.wait_for(e.trigger_global_kill_switch("synthetic_stop"), 1)
            assert [o["id"] for o in live] == ["exit-sell"]
            e._release_budget_reserve.assert_not_awaited()
            with pytest.raises(mod.EventHaltPreempted, match="cleanup_pending"):
                e._ensure_order_path_open("101", "during_delayed_post")
        finally:
            release.set()
            result = (await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1))[0]
        assert isinstance(result, mod.EventHaltPreempted), repr(result)
        assert [o["id"] for o in live] == ["exit-sell"]
        assert not e._buy_posts_inflight
        e._release_budget_reserve.assert_awaited_once_with("reserved")

    asyncio.run(run())


@pytest.mark.parametrize("known_on_first", [True, False])
def test_known_single_leg_canceled_before_other_leg_rest_retry(known_on_first):
    e, live, _entered, _release, cancel_calls = fault_venue()
    first = next(iter(set(e.market_cfg) | set(e._night_market_cfg)))
    paired = e._coordinated_pair_token(first)
    known_token = first if known_on_first else paired
    live[:] = [
        {"id": "known-buy", "asset_id": known_token, "side": "BUY", "status": "LIVE"},
        {"id": "exit-sell", "asset_id": paired, "side": "SELL", "status": "LIVE"},
    ]
    e._market_live_orders = {token: [dict(o) for o in live if o["asset_id"] == token] for token in (first, paired)}
    e._paired_single_leg_grace_sec = 0
    e._paired_reconcile_interval_sec = 0
    e._paired_single_leg_since = {"101|102": time.time() - 100}
    e._refresh_live_orders = AsyncMock(side_effect=OSError("REST unavailable"))
    e._read_open_orders = AsyncMock(side_effect=OSError("REST unavailable"))

    async def run():
        global_cancel_entered = asyncio.Event()
        real_global_cancel = e._cancel_all_except_exit

        async def observe_global_cancel():
            global_cancel_entered.set()
            return await real_global_cancel()

        e._cancel_all_except_exit = observe_global_cancel
        task = asyncio.create_task(e.paired_quote_invariant_loop())
        try:
            await asyncio.wait_for(global_cancel_entered.wait(), 1)
            assert any("known-buy" in ids for _, ids, _ in cancel_calls)
            assert [o["id"] for o in live] == ["exit-sell"]
            with pytest.raises(mod.EventHaltPreempted):
                e._ensure_order_path_open(known_token, "unverified_cancellation")
        finally:
            e._running = False
            task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 1)

    asyncio.run(run())
