from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest

from platforms.predictfun.maker import dry_run, runner
from platforms.predictfun.maker.deploy_release import LiveCleanupError, _verify_live_shutdown_cleanup
from platforms.predictfun.maker.dry_run import load_config
from platforms.predictfun.maker.executor import AccountPosition, ExecutionResult, LiveOrder
from platforms.predictfun.maker.intents import build_intent_state, build_intents_from_plans, utc_now
from platforms.predictfun.maker.managed_orders import ManagedOrder, ManagedOrderRegistry
from platforms.predictfun.maker.market_exclusions import MarketExclusions
from platforms.predictfun.maker.reconcile import (
    _to_order, reconcile_cancel_only, reconcile_once, reconcile_reduce_only,
    recover_uncertain_submissions,
)


POLICY = {"manual_market_exclusions": {"test_account": [42]}}
EXCLUDED = MarketExclusions.from_config(POLICY)


def intent(account="test_account", market=42, purpose="maker_quote", side="BUY"):
    return {"account_id": account, "market_id": market, "outcome": "YES", "side": side,
            "intent_id": f"{account}-{market}-{purpose}", "purpose": purpose,
            "price": "0.4", "size": "3", "token_id": "synthetic-token"}


def registry():
    rows = []
    for account, market, purpose in (
        ("test_account", 42, "maker_quote"),
        ("test_account", 42, "inventory_exit"),
        ("test_account", 43, "maker_quote"),
        ("other_account", 42, "maker_quote"),
    ):
        row = intent(account, market, purpose)
        rows.append(ManagedOrder(
            order_id=f"order-{row['intent_id']}", intent_id=row["intent_id"],
            account_id=account, market_id=market, outcome="YES", side="BUY",
            purpose=purpose, status="open", created_at=utc_now(), updated_at=utc_now(),
        ))
    return ManagedOrderRegistry(rows)


class SpyExecutor:
    def __init__(self):
        self.calls = []

    def recover_submission(self, order):
        self.calls.append(("recover", order.account_id, order.market_id))
        return None

    def create(self, order):
        self.calls.append(("create", order.account_id, order.market_id))
        return ExecutionResult(intent_id=order.intent_id, account_id=order.account_id,
                               action="create", ok=True, status="open", order_id="synthetic-new", message="synthetic")

    def cancel(self, order_id, *, intent_id="", account_id=""):
        self.calls.append(("cancel", account_id, order_id))
        return ExecutionResult(intent_id=intent_id, account_id=account_id,
                               action="cancel", ok=True, status="cancelled", order_id=order_id, message="synthetic")

    def capabilities(self):
        return {"ok": True, "live_order_read": True, "live_balance_read": True,
                "live_position_read": True, "live_order_submit": True, "live_order_cancel": True}

    def list_orders(self):
        return []

    def list_balances(self):
        return []

    def list_positions(self):
        return [position(42), position(43)]

    def get_order(self, *args, **kwargs):
        return None


def position(market=42):
    return AccountPosition(account_id="test_account", market_id=market, outcome="YES",
                           size=Decimal("3"), avg_price=Decimal("0.4"), mark_price=Decimal("0.5"),
                           value_usd=Decimal("1.5"))


def plan(market=42):
    return {"can_quote": True, "best_yes_bid": "0.4", "best_yes_ask": "0.6",
            "market": {"id": market, "status": "REGISTERED", "trading_status": "OPEN",
                       "decimal_precision": 2, "yes_token_id": "yes", "no_token_id": "no"},
            "yes_quotes": [{"outcome": "YES", "side": "BUY", "price": "0.4", "size": "3"}],
            "no_quotes": [{"outcome": "NO", "side": "BUY", "price": "0.4", "size": "3"}]}


@pytest.mark.parametrize("raw", [None, False, [], "42", {"": [42]}, {" a": [42]},
                                    {"a": "42"}, {"a": [True]}, {"a": [42.0]},
                                    {"a": ["42"]}, {"a": [0]}, {"a": [-1]}])
def test_invalid_policy_never_becomes_unprotected(raw, tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"manual_market_exclusions": raw}))
    with pytest.raises(ValueError):
        load_config(path)


def test_explicit_scope_and_immutable_snapshot():
    cfg = deepcopy(POLICY)
    policy = MarketExclusions.from_config(cfg)
    cfg["manual_market_exclusions"]["test_account"].clear()
    assert policy.blocks("test_account", 42)
    assert policy.blocks("test_account", "42")
    assert not policy.blocks("test_account", 43)
    assert not policy.blocks("other_account", 42)
    assert MarketExclusions.from_config({}).as_dict() == {}
    assert MarketExclusions.from_config({"manual_market_exclusions": {}}).as_dict() == {}
    for malformed in (None, True, 0, 42.0, "unknown"):
        assert policy.blocks("test_account", malformed)


@pytest.mark.parametrize("size", ["0", "3", "30"])
def test_quote_and_exit_exclusion_without_inferring_position_ownership(size):
    positions = [{"account_id": a, "market_id": m, "outcome": o, "size": size}
                 for a in ("test_account", "other_account") for m in (42, 43)
                 for o in ("YES", "NO")]
    before = deepcopy(positions)
    rows = build_intents_from_plans(
        [plan(), plan(43)], accounts_config={"ids": ["test_account", "other_account"], "assignment": "all"},
        inventory_positions=positions, market_exclusions=EXCLUDED,
    )
    assert rows
    assert not any(r.account_id == "test_account" and r.market_id == 42 for r in rows)
    assert any(r.account_id == "other_account" and r.market_id == 42 for r in rows)
    assert any(r.account_id == "test_account" and r.market_id == 43 for r in rows)
    if size != "0":
        assert any(r.purpose == "inventory_exit" for r in rows)
    assert positions == before


@pytest.mark.parametrize("resting_side", ["BUY", "SELL", None])
def test_manual_order_fill_or_disappearance_does_not_reenable_excluded_exit(resting_side):
    orders = [] if resting_side is None else [LiveOrder(
        order_id="manual-only", intent_id="", account_id="test_account", market_id=42,
        outcome="YES", side=resting_side, price=Decimal("0.4"), size=Decimal("3"),
        filled_size=Decimal("1"), status="open",
    )]
    _, positions, _ = runner._apply_manual_order_constraints(
        balances=[], positions=[position()], live_orders=orders, registry=ManagedOrderRegistry(),
    )
    rows = build_intents_from_plans([plan()], accounts_config=["test_account"],
                                    inventory_positions=[asdict(p) for p in positions],
                                    market_exclusions=EXCLUDED)
    assert rows == []


def test_diff_does_not_cancel_prior_excluded_intents():
    prior = [intent(), intent(market=43)]
    state = build_intent_state(environment="test", plans=[], previous_intents=prior,
                               accounts_config=["test_account"], market_exclusions=EXCLUDED)
    assert state["diff"]["cancel"] == [prior[1]]
    assert state["manual_market_exclusions"] == {"test_account": [42]}


@pytest.mark.parametrize("mode", ["normal", "reduce", "cancel"])
def test_all_reconcilers_preserve_excluded_managed_orders_and_stale_intents(mode):
    reg = registry()
    initial = reg.to_state()
    executor = SpyExecutor()
    creates = [intent(purpose="inventory_exit", side="SELL"),
               intent(market=44, purpose="inventory_exit", side="SELL")]
    stale = {"intents": creates, "diff": {"create": creates,
             "cancel": [{"intent_id": r.intent_id, "account_id": r.account_id,
                         "market_id": 999} for r in reg.active()]}}
    if mode == "normal":
        result = reconcile_once(stale, executor=executor, managed_state=initial,
                                mode="live", market_exclusions=EXCLUDED)
    elif mode == "reduce":
        result = reconcile_reduce_only(stale, executor=executor, managed_state=initial,
                                       mode="live", market_exclusions=EXCLUDED)
    else:
        result = reconcile_cancel_only(executor=executor, managed_state=initial,
                                       reason="kill_switch", mode="live", market_exclusions=EXCLUDED)
    assert not any(a == "test_account" and (m == 42 or "test_account-42" in str(m))
                   for _, a, m in executor.calls)
    assert any(call[0] == "cancel" for call in executor.calls)
    if mode != "cancel":
        assert ("create", "test_account", 44) in executor.calls
    after = ManagedOrderRegistry.from_state(result["managed_orders"])
    for row in initial["orders"]:
        if row["account_id"] == "test_account" and row["market_id"] == 42:
            assert row in after.to_state()["orders"]
    assert result["manual_market_exclusions"]["excluded_managed_active_orders"] == 2
    assert result["manual_market_exclusions"]["requires_manual_order_review"] is True


def test_pending_preserved_and_still_blocks_other_new_submissions():
    reg = ManagedOrderRegistry()
    reg.record_submission_pending(_to_order(intent()))
    before = reg.to_state()
    executor = SpyExecutor()
    assert recover_uncertain_submissions({}, registry=reg, executor=executor,
                                         market_exclusions=EXCLUDED) == []
    assert executor.calls == []
    assert reg.to_state() == before
    report = reconcile_once({"diff": {"create": [intent(market=43)]}},
                            managed_state=before, executor=executor, mode="live",
                            market_exclusions=EXCLUDED)
    assert not any(c[0] == "create" for c in executor.calls)
    assert report["summary"]["blocked"] == 1
    assert report["managed_orders"]["pending_submissions"] == before["pending_submissions"]


def test_shutdown_keeps_excluded_existing_orders(tmp_path):
    path = tmp_path / "report.json"
    path.write_text(json.dumps({"managed_orders": registry().to_state()}))
    executor = SpyExecutor()
    report = runner._cancel_managed_on_shutdown(path, executor, market_exclusions=EXCLUDED)
    assert report["summary"]["cancel"] == 2
    assert report["manual_market_exclusions"]["excluded_managed_active_orders"] == 2
    assert not any(a == "test_account" and "test_account-42" in str(m)
                   for _, a, m in executor.calls)


@pytest.mark.parametrize("risk_mode", ["allow", "reduce_only", "blocked", "exception", "shutdown"])
def test_real_runner_routes_use_policy_even_with_stale_plan(risk_mode, tmp_path, monkeypatch):
    outputs = {key: key + ".json" for key in (
        "state_path", "intents_path", "execution_report_path", "runner_state_path",
        "ws_state_path", "simulation_state_path", "risk_state_path", "kill_switch_path",
        "research_state_path", "status_path",
    )}
    cfg = {**POLICY, "base_url": "https://synthetic.invalid", "accounts": {"ids": ["test_account"]},
           "output": outputs, "simulation": {"enabled": False}}
    path = tmp_path / "config.json"
    path.write_text(json.dumps(cfg))
    report_path = tmp_path / outputs["execution_report_path"]
    report_path.write_text(json.dumps({"mode": "live", "managed_orders": registry().to_state()}))
    executor = SpyExecutor()
    gate = SimpleNamespace(allowed=True, effective_mode="live", requested_mode="live",
                           to_state=lambda: {"allowed": True, "effective_mode": "live"})
    monkeypatch.setattr(runner, "resolve_execution_gate", lambda *a, **k: gate)
    monkeypatch.setattr(runner, "_live_executor", lambda *a, **k: executor)
    monkeypatch.setattr(runner, "PredictFunClient", lambda **k: object())
    monkeypatch.setattr(runner, "_refresh_status_snapshot", lambda **k: None)
    monkeypatch.setattr(runner, "_STOP", False)

    def generate(*args, **kwargs):
        if risk_mode == "exception":
            raise RuntimeError("synthetic plan failure")
        if risk_mode == "shutdown":
            runner._STOP = True
        creates = [intent(purpose="inventory_exit", side="SELL"),
                   intent(market=44, purpose="inventory_exit", side="SELL")]
        (tmp_path / outputs["intents_path"]).write_text(json.dumps(
            {"ts": utc_now(), "intents": creates, "diff": {"create": creates}}))
        return {"ts": utc_now(), "plans": []}

    monkeypatch.setattr(runner, "run_once", generate)

    def risk(**kwargs):
        assert len(kwargs["inventory_state"]["positions"]) == 2
        return {"execution_mode": risk_mode, "status": "OK"}

    monkeypatch.setattr(runner, "evaluate_risk", risk)
    state = runner.run_loop(config_path=path, interval_sec=1, once=True)
    if risk_mode != "exception":
        assert state["consecutive_error_count"] == 0
    assert not any(a == "test_account" and (m == 42 or "test_account-42" in str(m))
                   for _, a, m in executor.calls)
    result = json.loads(report_path.read_text())
    assert result["manual_market_exclusions"]["excluded_managed_active_orders"] == 2
    if risk_mode in ("blocked", "exception", "shutdown"):
        assert any(c[0] == "cancel" for c in executor.calls)


def test_invalid_policy_stops_before_executor_construction(tmp_path, monkeypatch):
    cfg = {"manual_market_exclusions": None}
    def forbidden(*a, **k):
        raise AssertionError("constructed network client with invalid policy")
    monkeypatch.setattr(runner, "PredictFunClient", forbidden)
    with pytest.raises(ValueError):
        runner._run_loop_locked(config_path=tmp_path / "config.json", cfg=cfg, interval_sec=1, once=True)


def test_real_planner_loads_policy_after_restart_and_market_disappearance(tmp_path, monkeypatch):
    cfg = {**POLICY, "accounts": {"ids": ["test_account"]}, "signer": {"enabled": False},
           "output": {"intents_path": "intents.json"}}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(cfg))
    monkeypatch.setattr(dry_run, "scan_markets", lambda *a, **k: [])
    for _ in range(2):
        loaded = load_config(config_path)
        dry_run.run_once(object(), loaded, config_path=config_path,
                         previous_intents=[intent(), intent(market=43)], inventory_positions=[])
        output = json.loads((tmp_path / "intents.json").read_text())
        assert output["manual_market_exclusions"] == {"test_account": [42]}
        assert output["diff"]["cancel"] == [intent(market=43)]


def test_preserved_orders_do_not_falsely_pass_release_cleanup(tmp_path):
    path = tmp_path / "report.json"
    path.write_text(json.dumps({"managed_orders": registry().to_state()}))
    before = datetime.now(timezone.utc) - timedelta(seconds=1)
    runner._cancel_managed_on_shutdown(path, SpyExecutor(), market_exclusions=EXCLUDED)
    with pytest.raises(LiveCleanupError, match="active"):
        _verify_live_shutdown_cleanup(SimpleNamespace(execution_report=path), before)


@pytest.mark.parametrize("market", [None, True, "invalid"])
def test_malformed_cached_create_is_not_recovered_or_submitted(market):
    executor = SpyExecutor()
    result = reconcile_once({"diff": {"create": [intent(market=market)]}}, executor=executor,
                            mode="live", market_exclusions=EXCLUDED)
    assert executor.calls == []
    assert result["summary"]["actions"] == 0
