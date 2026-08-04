from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import asdict
from decimal import Decimal
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Optional

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.client import PredictFunClient
from platforms.predictfun.maker.dry_run import (
    _configured_account_ids,
    load_config,
    run_once,
)
from platforms.predictfun.maker.capital import build_account_capital_rows
from platforms.predictfun.maker.execution_gate import resolve_execution_gate
from platforms.predictfun.maker.executor import (
    AccountBalance,
    AccountPosition,
    DryRunExecutor,
    LiveOrder,
    MultiAccountExecutor,
    PredictFunExecutor,
    PredictFunLiveExecutor,
)
from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry
from platforms.predictfun.maker.research import build_research_state
from platforms.predictfun.maker.risk import blocked_execution_report, evaluate_risk
from platforms.predictfun.maker.reconcile import (
    load_json,
    reconcile_cancel_only,
    reconcile_once,
    reconcile_reduce_only,
    write_json,
)
from platforms.predictfun.maker.simulator import update_simulation
from platforms.predictfun.maker.status import build_status_snapshot


_STOP = False


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _release_metadata(
    environ: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    env = environ if environ is not None else os.environ
    return {
        "release_sha": str(env.get("PREDICTFUN_RELEASE_SHA", "")),
        "release_required": _truthy(
            str(env.get("PREDICTFUN_REQUIRE_RELEASE", ""))
        ),
    }


def _live_executor(
    cfg: dict[str, Any], account_ids: list[str]
) -> MultiAccountExecutor:
    signer_cfg = (
        cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    )
    strategy_cfg = (
        cfg.get("strategy")
        if isinstance(cfg.get("strategy"), dict)
        else {}
    )
    base_url = str(
        signer_cfg.get("base_url") or cfg.get("base_url") or ""
    ).rstrip("/")
    if not base_url:
        raise ValueError("Predict.fun signer base URL is missing")
    max_order_notional = Decimal(
        str(strategy_cfg.get("max_order_notional") or "0")
    )
    timeout = float(signer_cfg.get("timeout_sec") or 20)
    return MultiAccountExecutor(
        {
            account_id: PredictFunLiveExecutor(
                signer_url=base_url,
                account_id=account_id,
                max_order_notional=max_order_notional,
                timeout=timeout,
            )
            for account_id in account_ids
        }
    )


def _previous_intents_from_managed(
    registry: ManagedOrderRegistry,
) -> list[dict[str, Any]]:
    return [
        {
            "intent_id": row.intent_id,
            "account_id": row.account_id,
            "market_id": row.market_id,
            "outcome": row.outcome,
            "side": row.side,
            "purpose": row.purpose,
        }
        for row in registry.active()
    ]


def _sync_managed_live_orders(
    registry: ManagedOrderRegistry,
    live_orders: list[LiveOrder],
    executor: PredictFunExecutor,
) -> None:
    """Resolve disappeared engine orders without adopting website orders."""

    registry.sync_live_orders(live_orders)
    visible_ids = {row.order_id for row in live_orders if row.order_id}
    for managed in list(registry.active()):
        if managed.order_id in visible_ids:
            continue
        resolved = executor.get_order(
            managed.order_id, account_id=managed.account_id
        )
        if resolved is not None:
            registry.sync_live_orders([resolved])


def _apply_manual_order_constraints(
    *,
    balances: list[AccountBalance],
    positions: list[AccountPosition],
    live_orders: list[LiveOrder],
    registry: ManagedOrderRegistry,
) -> tuple[list[AccountBalance], list[AccountPosition], dict[str, Any]]:
    reserved_buys: dict[str, Decimal] = {}
    blocked_sell_markets: set[tuple[str, int]] = set()
    manual_counts: dict[str, int] = {}
    for order in live_orders:
        if registry.owns_order_id(order.order_id):
            continue
        account_id = str(order.account_id or "")
        if not account_id:
            continue
        remaining = max(Decimal("0"), order.size - order.filled_size)
        if remaining <= 0:
            continue
        manual_counts[account_id] = manual_counts.get(account_id, 0) + 1
        if order.side.upper() == "BUY":
            reserved_buys[account_id] = (
                reserved_buys.get(account_id, Decimal("0"))
                + max(Decimal("0"), order.price) * remaining
            )
        elif order.side.upper() == "SELL" and order.market_id > 0:
            blocked_sell_markets.add((account_id, order.market_id))

    adjusted_balances = [
        AccountBalance(
            asset=row.asset,
            available=max(
                Decimal("0"),
                row.available - reserved_buys.get(row.account_id, Decimal("0")),
            ),
            total=row.total,
            account_id=row.account_id,
        )
        for row in balances
    ]
    exit_positions = [
        row
        for row in positions
        if (row.account_id, row.market_id) not in blocked_sell_markets
    ]
    account_ids = sorted(
        {
            *manual_counts,
            *reserved_buys,
            *(account_id for account_id, _market_id in blocked_sell_markets),
        }
    )
    summary = {
        account_id: {
            "manual_open_orders": manual_counts.get(account_id, 0),
            "manual_buy_reserved_notional": str(
                reserved_buys.get(account_id, Decimal("0"))
            ),
            "manual_sell_blocked_markets": sorted(
                market_id
                for row_account_id, market_id in blocked_sell_markets
                if row_account_id == account_id
            ),
        }
        for account_id in account_ids
    }
    return adjusted_balances, exit_positions, summary


def _cancel_managed_on_shutdown(
    report_path: Path,
    executor: PredictFunExecutor,
) -> dict[str, Any]:
    previous = load_json(report_path)
    managed_state = (
        previous.get("managed_orders")
        if isinstance(previous.get("managed_orders"), dict)
        else {}
    )
    report = reconcile_cancel_only(
        managed_state=managed_state,
        executor=executor,
        reason="runner_shutdown",
        risk_state={"status": "BLOCKED", "reason": "runner_shutdown"},
        mode="live",
    )
    write_json(report_path, report)
    return report


def _handle_stop(signum: int, frame: object) -> None:
    global _STOP
    _STOP = True


def _configured_path(config_path: Path, cfg: dict[str, Any], key: str, default: str) -> Path:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    raw = str(out.get(key) or default)
    return (config_path.parent / raw).resolve()


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_runner_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(path)


def run_loop(
    *,
    config_path: Path,
    interval_sec: float,
    once: bool = False,
) -> dict[str, Any]:
    cfg = load_config(config_path)
    api_key = os.getenv(str(cfg.get("api_key_env") or "PREDICTFUN_API_KEY"), "")
    client = PredictFunClient(base_url=str(cfg["base_url"]), api_key=api_key)

    plan_state_path = _configured_path(config_path, cfg, "state_path", "../../../data/predictfun_state.json")
    intents_path = _configured_path(config_path, cfg, "intents_path", "../../../data/predictfun_desired_orders.json")
    report_path = _configured_path(config_path, cfg, "execution_report_path", "../../../data/predictfun_execution_report.json")
    runner_state_path = _configured_path(config_path, cfg, "runner_state_path", "../../../data/predictfun_runner_state.json")
    ws_state_path = _configured_path(config_path, cfg, "ws_state_path", "../../../data/predictfun_ws_state.json")
    simulation_state_path = _configured_path(config_path, cfg, "simulation_state_path", "../../../data/predictfun_simulation_state.json")
    risk_state_path = _configured_path(config_path, cfg, "risk_state_path", "../../../data/predictfun_risk_state.json")
    kill_switch_path = _configured_path(config_path, cfg, "kill_switch_path", "../../../data/predictfun_kill_switch.json")
    research_state_path = _configured_path(config_path, cfg, "research_state_path", "../../../data/predictfun_market_research.json")
    status_path = _configured_path(config_path, cfg, "status_path", "../../../data/predictfun_status.json")

    deployment_cfg = (
        cfg.get("deployment")
        if isinstance(cfg.get("deployment"), dict)
        else {}
    )
    account_ids = _configured_account_ids(cfg.get("accounts"))
    release_metadata = _release_metadata()
    gate = resolve_execution_gate(
        cfg, environ=os.environ, release=release_metadata
    )
    executor: PredictFunExecutor = DryRunExecutor()
    capabilities: dict[str, Any] = executor.capabilities()
    gate_state = gate.to_state()
    if gate.allowed and gate.effective_mode == "live":
        try:
            executor = _live_executor(cfg, account_ids)
            capabilities = executor.capabilities()
        except Exception as exc:
            capabilities = {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        if capabilities.get("ok") is not True:
            gate_state["blocks"] = [
                *list(gate_state.get("blocks") or []),
                "live_capabilities_incomplete",
            ]
            gate_state["allowed"] = False
            gate_state["effective_mode"] = "blocked"
            executor = DryRunExecutor()
    effective_mode = str(gate_state.get("effective_mode") or "blocked")
    state: dict[str, Any] = {
        "ts": _utc_now(),
        "started_at": _utc_now(),
        "stopped_at": "",
        "running": True,
        "pid": os.getpid(),
        "mode": effective_mode,
        "requested_mode": gate.requested_mode,
        "execution_gate": gate_state,
        "capabilities": capabilities,
        "environment": cfg.get("environment", "testnet"),
        "base_url": cfg.get("base_url"),
        "deployment_profile": str(deployment_cfg.get("profile") or ""),
        "account_ids": account_ids,
        **release_metadata,
        "interval_sec": interval_sec,
        "cycle_count": 0,
        "error_count": 0,
        "last_cycle_started_at": "",
        "last_cycle_finished_at": "",
        "last_error": "",
        "last_plan_summary": {},
        "last_execution_summary": {},
        "last_risk_summary": {},
        "last_simulation_summary": {},
        "last_research_summary": {},
        "last_auth_summary": {},
        "last_account_summary": {},
        "capital_profiles": {},
    }
    _write_runner_state(runner_state_path, state)

    plan_state = load_json(plan_state_path)
    intents_state = load_json(intents_path)
    report = load_json(report_path)
    risk_state = load_json(risk_state_path)
    simulation_state = load_json(simulation_state_path)
    research_state = load_json(research_state_path)
    ws_state = load_json(ws_state_path)
    live_orders: list[LiveOrder] = []
    live_balances: list[AccountBalance] = []
    live_positions: list[AccountPosition] = []
    manual_order_constraints: dict[str, Any] = {}

    while not _STOP:
        cycle_started = _utc_now()
        state["last_cycle_started_at"] = cycle_started
        state["ts"] = cycle_started
        _write_runner_state(runner_state_path, state)
        fast_requote = False
        plan_state = load_json(plan_state_path)
        intents_state = load_json(intents_path)
        report = load_json(report_path)
        risk_state = load_json(risk_state_path)
        simulation_state = load_json(simulation_state_path)
        research_state = load_json(research_state_path)
        ws_state = load_json(ws_state_path)
        previous_execution = load_json(report_path)
        managed_state = (
            previous_execution.get("managed_orders")
            if isinstance(previous_execution.get("managed_orders"), dict)
            else {}
        )
        registry = ManagedOrderRegistry.from_state(managed_state)
        try:
            cycle_cfg = deepcopy(cfg)
            if effective_mode == "live":
                live_orders = executor.list_orders()
                live_balances = executor.list_balances()
                live_positions = executor.list_positions()
                _sync_managed_live_orders(registry, live_orders, executor)
                managed_state = registry.to_state()
                previous_intents = _previous_intents_from_managed(registry)
                (
                    capital_balances,
                    exit_positions,
                    manual_order_constraints,
                ) = _apply_manual_order_constraints(
                    balances=live_balances,
                    positions=live_positions,
                    live_orders=live_orders,
                    registry=registry,
                )
                inventory_positions = [asdict(row) for row in exit_positions]
            else:
                live_orders = []
                live_balances = []
                live_positions = []
                capital_balances = []
                manual_order_constraints = {}
                previous_intents = _previous_intents_from_simulation(
                    simulation_state
                )
                inventory_positions = (
                    simulation_state.get("positions")
                    if isinstance(simulation_state.get("positions"), list)
                    else None
                )

            capital_cfg = (
                cfg.get("capital")
                if isinstance(cfg.get("capital"), dict)
                else {}
            )
            previous_profiles = {
                str(account_id): str(row.get("capital_profile") or "")
                for account_id, row in (
                    state.get("capital_profiles") or {}
                ).items()
                if isinstance(row, dict)
            }
            capital_rows = build_account_capital_rows(
                account_ids,
                balances=capital_balances,
                positions=live_positions,
                previous_profiles=previous_profiles,
                fallback_equity=Decimal(
                    str(capital_cfg.get("fallback_equity") or "100")
                ),
                allow_fallback=effective_mode == "dry_run",
            )
            if isinstance(executor, MultiAccountExecutor):
                executor.set_order_notional_limits(
                    {
                        str(row.get("account_id") or ""): Decimal(
                            str(row.get("max_order_notional") or "0")
                        )
                        for row in capital_rows
                    }
                )
            cycle_accounts = (
                cycle_cfg.get("accounts")
                if isinstance(cycle_cfg.get("accounts"), dict)
                else {}
            )
            cycle_accounts["ids"] = capital_rows
            cycle_accounts["max_active_accounts"] = len(capital_rows)
            cycle_cfg["accounts"] = cycle_accounts
            state["capital_profiles"] = {
                row["account_id"]: row for row in capital_rows
            }
            state["last_account_summary"] = {
                "orders": len(
                    [row for row in live_orders if row.status == "open"]
                ),
                "positions": len(live_positions),
                "balances": {
                    row.account_id: str(row.total) for row in live_balances
                },
                "manual_order_constraints": manual_order_constraints,
            }

            plan_state = run_once(
                client,
                cycle_cfg,
                config_path=config_path,
                previous_intents=previous_intents,
                inventory_positions=inventory_positions,
                execution_mode=effective_mode,
            )
            intents_state = load_json(intents_path)
            research_state = build_research_state(plan_state)
            write_json(research_state_path, research_state)

            ws_state = load_json(ws_state_path)
            risk_state = evaluate_risk(
                cfg=cycle_cfg,
                plan_state=plan_state,
                intents_state=intents_state,
                runner_state=state,
                ws_state=ws_state,
                simulation_state=simulation_state,
                kill_switch_state=load_json(kill_switch_path),
                inventory_state=(
                    {"positions": [asdict(row) for row in live_positions]}
                    if effective_mode == "live"
                    else simulation_state
                ),
                inventory_source=(
                    "live" if effective_mode == "live" else "simulation"
                ),
            )
            write_json(risk_state_path, risk_state)

            execution_mode = str(risk_state.get("execution_mode") or "blocked")
            if effective_mode == "blocked":
                execution_mode = "blocked"
            action_mode = effective_mode
            if effective_mode == "blocked":
                report = blocked_execution_report(
                    {
                        **risk_state,
                        "execution_gate": gate_state,
                    },
                    intents_state.get("ts"),
                    managed_state=managed_state,
                )
            elif execution_mode == "blocked":
                report = reconcile_cancel_only(
                    managed_state=managed_state,
                    executor=executor,
                    reason="risk_gate",
                    risk_state=risk_state,
                    mode=action_mode,
                )
            elif execution_mode == "reduce_only":
                report = reconcile_reduce_only(
                    intents_state,
                    managed_state=managed_state,
                    executor=executor,
                    risk_state=risk_state,
                    mode=action_mode,
                )
            else:
                report = reconcile_once(
                    intents_state,
                    managed_state=managed_state,
                    executor=executor,
                    mode=action_mode,
                )
            write_json(report_path, report)

            sim_cfg = cfg.get("simulation") if isinstance(cfg.get("simulation"), dict) else {}
            if effective_mode == "dry_run" and bool(sim_cfg.get("enabled", True)) and execution_mode != "blocked":
                simulation_state = update_simulation(
                    previous_state=simulation_state,
                    plan_state=plan_state,
                    intents_state=intents_state,
                    execution_report=report,
                    max_fill_size=Decimal(str(sim_cfg.get("max_fill_size") or "10")),
                )
                write_json(simulation_state_path, simulation_state)
                fills_new = int((simulation_state.get("summary") or {}).get("fills_new") or 0)
                fast_requote = fills_new > 0

            state["cycle_count"] = int(state.get("cycle_count") or 0) + 1
            state["last_error"] = ""
            state["last_plan_summary"] = {
                "ts": plan_state.get("ts"),
                "plans": len(plan_state.get("plans") or []),
                "quotable": sum(1 for plan in plan_state.get("plans") or [] if plan.get("can_quote")),
                "intents": plan_state.get("intents") or {},
            }
            state["last_execution_summary"] = report.get("summary") or {}
            state["last_risk_summary"] = risk_state.get("summary") or {}
            state["last_risk_status"] = risk_state.get("status") or "UNKNOWN"
            state["last_simulation_summary"] = simulation_state.get("summary") or {}
            state["last_research_summary"] = research_state.get("summary") or {}
            state["last_auth_summary"] = (
                plan_state.get("auth")
                if isinstance(plan_state.get("auth"), dict)
                else {}
            )
            state["fast_requote"] = bool(fast_requote)
        except Exception as exc:
            state["error_count"] = int(state.get("error_count") or 0) + 1
            state["last_error"] = f"{exc.__class__.__name__}: {exc}"
            if (
                effective_mode == "live"
                and str(previous_execution.get("mode") or "").startswith("live")
            ):
                try:
                    report = reconcile_cancel_only(
                        managed_state=managed_state,
                        executor=executor,
                        reason="runner_exception",
                        risk_state={
                            "status": "BLOCKED",
                            "error": state["last_error"],
                        },
                        mode="live",
                    )
                    write_json(report_path, report)
                except Exception as cancel_exc:
                    state["emergency_cancel_error"] = (
                        f"{type(cancel_exc).__name__}: {cancel_exc}"
                    )
        finally:
            state["last_cycle_finished_at"] = _utc_now()
            state["ts"] = state["last_cycle_finished_at"]
            _refresh_status_snapshot(
                status_path=status_path,
                cfg=cfg,
                runner_state=state,
                plan_state=plan_state,
                intents_state=intents_state,
                execution_state=report,
                risk_state=risk_state,
                simulation_state=simulation_state,
                research_state=research_state,
                ws_state=ws_state,
                live_orders=live_orders,
                live_balances=live_balances,
                live_positions=live_positions,
            )
            _write_runner_state(runner_state_path, state)

        if once:
            break

        slept = 0.0
        runner_cfg = cfg.get("runner") if isinstance(cfg.get("runner"), dict) else {}
        fast_requote_sec = float(runner_cfg.get("fast_requote_after_fill_sec") or 2)
        sleep_for = max(1.0, min(interval_sec, fast_requote_sec) if fast_requote else interval_sec)
        while slept < sleep_for and not _STOP:
            step = min(1.0, sleep_for - slept)
            time.sleep(step)
            slept += step

    if effective_mode == "live" and _STOP:
        try:
            report = _cancel_managed_on_shutdown(report_path, executor)
            shutdown_summary = report.get("summary") or {}
            state["shutdown_cancel_summary"] = shutdown_summary
            state["last_execution_summary"] = shutdown_summary
            failed = int(shutdown_summary.get("failed") or 0)
            if failed:
                state["emergency_cancel_error"] = (
                    f"runner_shutdown_cancel_failed:{failed}"
                )
        except Exception as exc:
            state["emergency_cancel_error"] = (
                f"runner_shutdown_cancel_error:{type(exc).__name__}: {exc}"
            )

    state["running"] = False
    state["stopped_at"] = _utc_now()
    state["ts"] = state["stopped_at"]
    _refresh_status_snapshot(
        status_path=status_path,
        cfg=cfg,
        runner_state=state,
        plan_state=plan_state,
        intents_state=intents_state,
        execution_state=report,
        risk_state=risk_state,
        simulation_state=simulation_state,
        research_state=research_state,
        ws_state=ws_state,
        live_orders=live_orders,
        live_balances=live_balances,
        live_positions=live_positions,
    )
    _write_runner_state(runner_state_path, state)
    return state


def _refresh_status_snapshot(
    *,
    status_path: Path,
    cfg: dict[str, Any],
    runner_state: dict[str, Any],
    plan_state: dict[str, Any],
    intents_state: dict[str, Any],
    execution_state: dict[str, Any],
    risk_state: dict[str, Any],
    simulation_state: dict[str, Any],
    research_state: dict[str, Any],
    ws_state: dict[str, Any],
    live_orders: list[LiveOrder],
    live_balances: list[AccountBalance],
    live_positions: list[AccountPosition],
) -> None:
    try:
        write_json(
            status_path,
            build_status_snapshot(
                cfg=cfg,
                runner_state=runner_state,
                plan_state=plan_state,
                intents_state=intents_state,
                execution_state=execution_state,
                risk_state=risk_state,
                simulation_state=simulation_state,
                research_state=research_state,
                ws_state=ws_state,
                live_orders=live_orders,
                live_balances=live_balances,
                live_positions=live_positions,
            ),
        )
        runner_state["status_snapshot_error"] = ""
    except Exception as exc:
        runner_state["status_snapshot_error"] = (
            f"{exc.__class__.__name__}: {exc}"
        )


def _previous_intents_from_simulation(simulation_state: dict[str, Any]) -> list[dict[str, Any]] | None:
    if not simulation_state:
        return None
    active = simulation_state.get("active_orders")
    if isinstance(active, list):
        return [row for row in active if isinstance(row, dict)]
    return []


def main() -> None:
    default_config = Path(__file__).with_name("config.testnet.json")
    parser = argparse.ArgumentParser(description="Predict.fun dry-run maker runner.")
    parser.add_argument("--config", default=str(default_config))
    parser.add_argument("--interval-sec", type=float, default=0.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    runner_cfg = cfg.get("runner") if isinstance(cfg.get("runner"), dict) else {}
    interval_sec = args.interval_sec if args.interval_sec > 0 else float(runner_cfg.get("interval_sec") or 30)
    state = run_loop(config_path=config_path, interval_sec=interval_sec, once=args.once)
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
