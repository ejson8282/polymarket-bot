from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.maker.intents import utc_now


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def evaluate_risk(
    *,
    cfg: dict[str, Any],
    plan_state: dict[str, Any],
    intents_state: dict[str, Any],
    runner_state: dict[str, Any],
    ws_state: dict[str, Any],
    simulation_state: dict[str, Any],
    kill_switch_state: dict[str, Any],
) -> dict[str, Any]:
    risk_cfg = cfg.get("risk") if isinstance(cfg.get("risk"), dict) else {}
    data_cfg = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
    checks: list[dict[str, Any]] = []

    _check_bool(
        checks,
        name="global_kill_switch",
        blocked=bool(kill_switch_state.get("enabled")),
        detail=str(kill_switch_state.get("reason") or ""),
    )

    _check_age(
        checks,
        name="plan_state_fresh",
        ts=str(plan_state.get("ts") or ""),
        max_age_sec=float(risk_cfg.get("max_plan_state_age_sec") or 180),
    )
    if bool(data_cfg.get("use_ws_orderbook_cache", True)) and bool(risk_cfg.get("warn_on_stale_ws_state", False)):
        _check_age(
            checks,
            name="ws_state_fresh",
            ts=str(ws_state.get("ts") or ""),
            max_age_sec=float(risk_cfg.get("max_ws_state_age_sec") or 180),
            warn_only=True,
        )

    total_notional = _dec((intents_state.get("summary") or {}).get("total_notional"))
    _check_limit(
        checks,
        name="desired_total_notional",
        value=total_notional,
        limit=_dec(risk_cfg.get("max_total_desired_notional"), "2500"),
    )

    account_cfg = cfg.get("accounts") if isinstance(cfg.get("accounts"), dict) else {}
    max_accounts = int(account_cfg.get("max_active_accounts") or risk_cfg.get("max_active_accounts") or 10)
    account_ids = sorted({_account_id(item) for item in intents_state.get("intents") or [] if isinstance(item, dict)})
    _check_limit(
        checks,
        name="active_account_count",
        value=Decimal(len(account_ids)),
        limit=Decimal(max_accounts),
    )

    max_account_notional = _dec(risk_cfg.get("max_account_desired_notional"), "100")
    by_account: dict[str, Decimal] = {}
    by_account_market: dict[tuple[str, str], Decimal] = {}
    for item in intents_state.get("intents") or []:
        if not isinstance(item, dict):
            continue
        account_id = _account_id(item)
        market_id = str(item.get("market_id") or "")
        notional = _dec(item.get("notional"))
        by_account[account_id] = by_account.get(account_id, Decimal("0")) + notional
        by_account_market[(account_id, market_id)] = by_account_market.get((account_id, market_id), Decimal("0")) + notional
    for account_id, value in sorted(by_account.items()):
        _check_limit(
            checks,
            name=f"account_notional_{account_id}",
            value=value,
            limit=max_account_notional,
        )

    max_account_market_notional = _dec(risk_cfg.get("max_account_market_desired_notional"), "40")
    for (account_id, market_id), value in sorted(by_account_market.items()):
        _check_limit(
            checks,
            name=f"account_market_notional_{account_id}_{market_id}",
            value=value,
            limit=max_account_market_notional,
        )

    max_market_notional = _dec(risk_cfg.get("max_market_desired_notional"), "75")
    by_market: dict[str, Decimal] = {}
    for item in intents_state.get("intents") or []:
        if isinstance(item, dict):
            market_id = str(item.get("market_id") or "")
            by_market[market_id] = by_market.get(market_id, Decimal("0")) + _dec(item.get("notional"))
    for market_id, value in sorted(by_market.items()):
        _check_limit(
            checks,
            name=f"market_notional_{market_id}",
            value=value,
            limit=max_market_notional,
        )

    max_position = _dec(risk_cfg.get("max_market_position_size"), "100")
    max_account_position = _dec(risk_cfg.get("max_account_market_position_size"), str(max_position))
    for pos in simulation_state.get("positions") or []:
        if isinstance(pos, dict):
            account_id = _account_id(pos)
            _check_limit(
                checks,
                name=f"sim_position_{pos.get('market_id')}_{pos.get('outcome')}",
                value=abs(_dec(pos.get("size"))),
                limit=max_position,
            )
            _check_limit(
                checks,
                name=f"sim_account_position_{account_id}_{pos.get('market_id')}_{pos.get('outcome')}",
                value=abs(_dec(pos.get("size"))),
                limit=max_account_position,
            )

    max_errors = int(risk_cfg.get("max_runner_errors") or 3)
    error_count = int(runner_state.get("error_count") or 0)
    checks.append(
        {
            "name": "runner_errors",
            "status": "BLOCK" if error_count > max_errors else "OK",
            "value": error_count,
            "limit": max_errors,
            "detail": "",
        }
    )

    blocked = any(row["status"] == "BLOCK" for row in checks)
    warn = any(row["status"] == "WARN" for row in checks)
    return {
        "ts": utc_now(),
        "mode": "risk_gate",
        "status": "BLOCK" if blocked else "WARN" if warn else "OK",
        "blocked": blocked,
        "summary": {
            "checks": len(checks),
            "blocked": sum(1 for row in checks if row["status"] == "BLOCK"),
            "warn": sum(1 for row in checks if row["status"] == "WARN"),
            "desired_total_notional": str(total_notional),
            "active_accounts": len(account_ids),
            "sim_positions": len(simulation_state.get("positions") or []),
        },
        "checks": checks,
    }


def blocked_execution_report(risk_state: dict[str, Any], source_ts: str | None) -> dict[str, Any]:
    return {
        "ts": utc_now(),
        "mode": "risk_blocked",
        "source_ts": source_ts,
        "summary": {
            "actions": 0,
            "create": 0,
            "cancel": 0,
            "failed": 0,
            "blocked": 1,
        },
        "results": [
            {
                "intent_id": "",
                "action": "risk_gate",
                "ok": False,
                "message": "PF reconcile skipped by risk gate",
            }
        ],
        "risk": risk_state,
    }


def _check_bool(checks: list[dict[str, Any]], *, name: str, blocked: bool, detail: str = "") -> None:
    checks.append(
        {
            "name": name,
            "status": "BLOCK" if blocked else "OK",
            "value": "enabled" if blocked else "disabled",
            "limit": "disabled",
            "detail": detail,
        }
    )


def _check_age(
    checks: list[dict[str, Any]],
    *,
    name: str,
    ts: str,
    max_age_sec: float,
    warn_only: bool = False,
) -> None:
    age = _age_sec(ts)
    bad = age is None or age > max_age_sec
    checks.append(
        {
            "name": name,
            "status": "WARN" if bad and warn_only else "BLOCK" if bad else "OK",
            "value": "missing" if age is None else round(age, 1),
            "limit": max_age_sec,
            "detail": f"ts={ts or 'n/a'}",
        }
    )


def _check_limit(
    checks: list[dict[str, Any]],
    *,
    name: str,
    value: Decimal,
    limit: Decimal,
) -> None:
    checks.append(
        {
            "name": name,
            "status": "BLOCK" if limit > 0 and value > limit else "OK",
            "value": str(value),
            "limit": str(limit),
            "detail": "",
        }
    )


def _account_id(item: dict[str, Any]) -> str:
    return str(item.get("account_id") or "acct01")


def _age_sec(ts: str) -> float | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
    except Exception:
        return None


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def main() -> None:
    parser = argparse.ArgumentParser(description="Predict.fun risk gate evaluation.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--plans", required=True)
    parser.add_argument("--intents", required=True)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--ws", required=True)
    parser.add_argument("--simulation", required=True)
    parser.add_argument("--kill-switch", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    state = evaluate_risk(
        cfg=load_json(Path(args.config).resolve()),
        plan_state=load_json(Path(args.plans).resolve()),
        intents_state=load_json(Path(args.intents).resolve()),
        runner_state=load_json(Path(args.runner).resolve()),
        ws_state=load_json(Path(args.ws).resolve()),
        simulation_state=load_json(Path(args.simulation).resolve()),
        kill_switch_state=load_json(Path(args.kill_switch).resolve()),
    )
    write_json(Path(args.out).resolve(), state)
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
