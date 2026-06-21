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

from platforms.predictfun.maker.executor import DryRunExecutor, ExecutableOrder
from platforms.predictfun.maker.intents import utc_now


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
    )


def reconcile_once(intents_state: dict[str, Any]) -> dict[str, Any]:
    executor = DryRunExecutor()
    diff = intents_state.get("diff") if isinstance(intents_state.get("diff"), dict) else {}
    results = []
    for item in diff.get("cancel") or []:
        if isinstance(item, dict):
            results.append(
                asdict(
                    executor.cancel(
                        str(item.get("intent_id") or ""),
                        account_id=str(item.get("account_id") or "acct01"),
                    )
                )
            )
    for item in diff.get("create") or []:
        if isinstance(item, dict):
            results.append(asdict(executor.create(_to_order(item))))
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
