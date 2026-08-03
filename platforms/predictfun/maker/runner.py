from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.client import PredictFunClient
from platforms.predictfun.maker.dry_run import load_config, run_once
from platforms.predictfun.maker.research import build_research_state
from platforms.predictfun.maker.risk import evaluate_risk
from platforms.predictfun.maker.reconcile import (
    load_json,
    reconcile_cancel_only,
    reconcile_once,
    reconcile_reduce_only,
    write_json,
)
from platforms.predictfun.maker.simulator import update_simulation


_STOP = False


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

    intents_path = _configured_path(config_path, cfg, "intents_path", "../../../data/predictfun_desired_orders.json")
    report_path = _configured_path(config_path, cfg, "execution_report_path", "../../../data/predictfun_execution_report.json")
    runner_state_path = _configured_path(config_path, cfg, "runner_state_path", "../../../data/predictfun_runner_state.json")
    ws_state_path = _configured_path(config_path, cfg, "ws_state_path", "../../../data/predictfun_ws_state.json")
    simulation_state_path = _configured_path(config_path, cfg, "simulation_state_path", "../../../data/predictfun_simulation_state.json")
    risk_state_path = _configured_path(config_path, cfg, "risk_state_path", "../../../data/predictfun_risk_state.json")
    kill_switch_path = _configured_path(config_path, cfg, "kill_switch_path", "../../../data/predictfun_kill_switch.json")
    research_state_path = _configured_path(config_path, cfg, "research_state_path", "../../../data/predictfun_market_research.json")

    state: dict[str, Any] = {
        "ts": _utc_now(),
        "started_at": _utc_now(),
        "stopped_at": "",
        "running": True,
        "pid": os.getpid(),
        "mode": "dry_run",
        "environment": cfg.get("environment", "testnet"),
        "base_url": cfg.get("base_url"),
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
    }
    _write_runner_state(runner_state_path, state)

    while not _STOP:
        cycle_started = _utc_now()
        state["last_cycle_started_at"] = cycle_started
        state["ts"] = cycle_started
        _write_runner_state(runner_state_path, state)
        fast_requote = False
        try:
            simulation_state = load_json(simulation_state_path)
            previous_intents = _previous_intents_from_simulation(simulation_state)
            plan_state = run_once(
                client,
                cfg,
                config_path=config_path,
                previous_intents=previous_intents,
                inventory_positions=simulation_state.get("positions") if isinstance(simulation_state.get("positions"), list) else None,
            )
            intents_state = load_json(intents_path)
            previous_execution = load_json(report_path)
            managed_state = (
                previous_execution.get("managed_orders")
                if isinstance(previous_execution.get("managed_orders"), dict)
                else {}
            )
            research_state = build_research_state(plan_state)
            write_json(research_state_path, research_state)

            risk_state = evaluate_risk(
                cfg=cfg,
                plan_state=plan_state,
                intents_state=intents_state,
                runner_state=state,
                ws_state=load_json(ws_state_path),
                simulation_state=simulation_state,
                kill_switch_state=load_json(kill_switch_path),
            )
            write_json(risk_state_path, risk_state)

            execution_mode = str(risk_state.get("execution_mode") or "blocked")
            if execution_mode == "blocked":
                report = reconcile_cancel_only(
                    managed_state=managed_state,
                    reason="risk_gate",
                    risk_state=risk_state,
                )
            elif execution_mode == "reduce_only":
                report = reconcile_reduce_only(
                    intents_state,
                    managed_state=managed_state,
                    risk_state=risk_state,
                )
            else:
                report = reconcile_once(intents_state, managed_state=managed_state)
            write_json(report_path, report)

            sim_cfg = cfg.get("simulation") if isinstance(cfg.get("simulation"), dict) else {}
            if bool(sim_cfg.get("enabled", True)) and execution_mode != "blocked":
                from decimal import Decimal

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
            state["fast_requote"] = bool(fast_requote)
        except Exception as exc:
            state["error_count"] = int(state.get("error_count") or 0) + 1
            state["last_error"] = f"{exc.__class__.__name__}: {exc}"
        finally:
            state["last_cycle_finished_at"] = _utc_now()
            state["ts"] = state["last_cycle_finished_at"]
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

    state["running"] = False
    state["stopped_at"] = _utc_now()
    state["ts"] = state["stopped_at"]
    _write_runner_state(runner_state_path, state)
    return state


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
