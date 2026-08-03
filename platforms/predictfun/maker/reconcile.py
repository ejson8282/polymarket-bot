from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.maker.executor import DryRunExecutor, ExecutableOrder, PredictFunExecutor
from platforms.predictfun.maker.intents import utc_now
from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def _configured_path(config_path: Path, cfg: dict[str, Any], key: str, default: str) -> Path:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    raw = str(out.get(key) or default)
    return (config_path.parent / raw).resolve()


def _to_order(item: dict[str, Any]) -> ExecutableOrder:
    return ExecutableOrder(
        intent_id=str(item.get("intent_id") or ""),
        account_id=str(item.get("account_id") or "acct01"),
        market_id=int(item.get("market_id") or 0),
        outcome=str(item.get("outcome") or ""),
        side=str(item.get("side") or ""),
        price=Decimal(str(item.get("price") or "0")),
        size=Decimal(str(item.get("size") or "0")),
        token_id=str(item.get("token_id") or ""),
        fee_rate_bps=int(item.get("fee_rate_bps") or 0),
        is_neg_risk=bool(item.get("is_neg_risk")),
        is_yield_bearing=bool(item.get("is_yield_bearing")),
        market_mode=str(item.get("market_mode") or "standard"),
        purpose=str(item.get("purpose") or "maker_quote"),
    )


def reconcile_once(
    intents_state: dict[str, Any],
    *,
    executor: PredictFunExecutor | None = None,
    managed_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    executor = executor or DryRunExecutor()
    registry = ManagedOrderRegistry.from_state(managed_state)
    diff = intents_state.get("diff") if isinstance(intents_state.get("diff"), dict) else {}
    results = []
    for item in diff.get("cancel") or []:
        if isinstance(item, dict):
            intent_id = str(item.get("intent_id") or "")
            account_id = str(item.get("account_id") or "acct01")
            for managed in registry.active_for_intent(intent_id, account_id):
                result = executor.cancel(
                    managed.order_id,
                    intent_id=managed.intent_id,
                    account_id=managed.account_id,
                )
                registry.record_cancel(managed.order_id, result)
                results.append(asdict(result))
    for item in diff.get("create") or []:
        if isinstance(item, dict):
            order = _to_order(item)
            result = executor.create(order)
            registry.record_create(order, result)
            results.append(asdict(result))
    return {
        "ts": utc_now(),
        "mode": "dry_run",
        "source_ts": intents_state.get("ts"),
        "summary": {
            "actions": len(results),
            "create": sum(1 for row in results if row.get("action") == "create"),
            "cancel": sum(1 for row in results if row.get("action") == "cancel"),
            "failed": sum(1 for row in results if not row.get("ok")),
        },
        "results": results,
        "managed_orders": registry.to_state(),
    }


def reconcile_cancel_only(
    *,
    managed_state: dict[str, Any] | None,
    executor: PredictFunExecutor | None = None,
    reason: str,
    risk_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Cancel engine-owned orders while keeping all create actions disabled."""

    executor = executor or DryRunExecutor()
    registry = ManagedOrderRegistry.from_state(managed_state)
    results: list[dict[str, Any]] = []
    for managed in registry.active():
        result = executor.cancel(
            managed.order_id,
            intent_id=managed.intent_id,
            account_id=managed.account_id,
        )
        registry.record_cancel(managed.order_id, result)
        results.append(asdict(result))
    return {
        "ts": utc_now(),
        "mode": "cancel_only" if results else "risk_blocked",
        "reason": reason,
        "summary": {
            "actions": len(results),
            "create": 0,
            "cancel": len(results),
            "failed": sum(1 for row in results if not row.get("ok")),
            "blocked": 1,
        },
        "results": results,
        "managed_orders": registry.to_state(),
        "risk": risk_state or {},
    }


def reconcile_reduce_only(
    intents_state: dict[str, Any],
    *,
    managed_state: dict[str, Any] | None,
    executor: PredictFunExecutor | None = None,
    risk_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Cancel maker quotes and allow only position-reducing exit intents."""

    executor = executor or DryRunExecutor()
    registry = ManagedOrderRegistry.from_state(managed_state)
    desired_exit_ids = {
        str(item.get("intent_id") or "")
        for item in intents_state.get("intents") or []
        if isinstance(item, dict) and str(item.get("purpose") or "") == "inventory_exit"
    }
    results: list[dict[str, Any]] = []
    for managed in registry.active():
        if managed.purpose == "inventory_exit" and managed.intent_id in desired_exit_ids:
            continue
        result = executor.cancel(
            managed.order_id,
            intent_id=managed.intent_id,
            account_id=managed.account_id,
        )
        registry.record_cancel(managed.order_id, result)
        results.append(asdict(result))

    diff = intents_state.get("diff") if isinstance(intents_state.get("diff"), dict) else {}
    for item in diff.get("create") or []:
        if not isinstance(item, dict) or str(item.get("purpose") or "") != "inventory_exit":
            continue
        order = _to_order(item)
        result = executor.create(order)
        registry.record_create(order, result)
        results.append(asdict(result))

    return {
        "ts": utc_now(),
        "mode": "reduce_only",
        "source_ts": intents_state.get("ts"),
        "summary": {
            "actions": len(results),
            "create": sum(1 for row in results if row.get("action") == "create"),
            "cancel": sum(1 for row in results if row.get("action") == "cancel"),
            "failed": sum(1 for row in results if not row.get("ok")),
            "blocked": 0,
            "reduce_only": 1,
        },
        "results": results,
        "managed_orders": registry.to_state(),
        "risk": risk_state or {},
    }


def main() -> None:
    default_config = Path(__file__).with_name("config.testnet.json")
    parser = argparse.ArgumentParser(description="Predict.fun dry-run reconcile executor.")
    parser.add_argument("--config", default=str(default_config))
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = load_json(config_path)
    intents_path = _configured_path(config_path, cfg, "intents_path", "../../../data/predictfun_desired_orders.json")
    report_path = _configured_path(config_path, cfg, "execution_report_path", "../../../data/predictfun_execution_report.json")
    report = reconcile_once(load_json(intents_path))
    write_json(report_path, report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
