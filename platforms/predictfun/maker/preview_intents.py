from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.maker.dry_run import load_config


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _configured_path(config_path: Path, cfg: dict[str, Any], key: str, default: str) -> Path:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    raw = str(out.get(key) or default)
    return (config_path.parent / raw).resolve()


def _selected_intents(state: dict[str, Any], *, include_keep: bool, limit: int) -> list[dict[str, Any]]:
    diff = state.get("diff") if isinstance(state.get("diff"), dict) else {}
    rows = [row for row in diff.get("create") or [] if isinstance(row, dict)]
    if include_keep:
        rows.extend(row for row in diff.get("keep") or [] if isinstance(row, dict))
    if not rows and include_keep:
        rows = [row for row in state.get("intents") or [] if isinstance(row, dict)]
    return rows[: max(0, limit)]


def _preview_body(intent: dict[str, Any]) -> dict[str, Any]:
    return {
        "side": str(intent.get("side") or ""),
        "token_id": str(intent.get("token_id") or ""),
        "price": str(intent.get("price") or "0"),
        "size": str(intent.get("size") or "0"),
        "fee_rate_bps": int(intent.get("fee_rate_bps") or 0),
        "is_neg_risk": bool(intent.get("is_neg_risk")),
        "is_yield_bearing": bool(intent.get("is_yield_bearing")),
        "is_post_only": True,
    }


def _safe_preview(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": bool(payload.get("ok")),
        "alias": payload.get("alias"),
        "maker": payload.get("maker"),
        "signer_mode": payload.get("signer_mode"),
        "strategy": payload.get("strategy"),
        "order_hash": payload.get("order_hash"),
        "signature_present": bool(payload.get("signature_present")),
        "error": payload.get("error"),
    }


def preview_intents(
    *,
    cfg: dict[str, Any],
    config_path: Path,
    include_keep: bool,
    limit: int,
) -> dict[str, Any]:
    intents_path = _configured_path(config_path, cfg, "intents_path", "../../../data/predictfun_mainnet_desired_orders.json")
    state = _load_json(intents_path)
    signer_cfg = cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    signer_url = str(signer_cfg.get("base_url") or cfg.get("base_url") or "").rstrip("/")
    timeout = float(signer_cfg.get("timeout_sec") or 20)
    if not signer_url:
        return {"ok": False, "error": "missing_signer_base_url", "results": []}

    results: list[dict[str, Any]] = []
    for intent in _selected_intents(state, include_keep=include_keep, limit=limit):
        account_id = str(intent.get("account_id") or "account_01")
        token_id = str(intent.get("token_id") or "")
        if not token_id:
            results.append(
                {
                    "ok": False,
                    "intent_id": intent.get("intent_id"),
                    "account_id": account_id,
                    "market_id": intent.get("market_id"),
                    "outcome": intent.get("outcome"),
                    "side": intent.get("side"),
                    "error": "missing_token_id",
                }
            )
            continue
        try:
            resp = requests.post(
                f"{signer_url}/predictfun/accounts/{account_id}/preview-order",
                json=_preview_body(intent),
                timeout=timeout,
            )
            payload = resp.json() if resp.content else {}
        except Exception as exc:
            results.append(
                {
                    "ok": False,
                    "intent_id": intent.get("intent_id"),
                    "account_id": account_id,
                    "market_id": intent.get("market_id"),
                    "outcome": intent.get("outcome"),
                    "side": intent.get("side"),
                    "error": f"{type(exc).__name__}",
                }
            )
            continue
        safe = _safe_preview(payload if isinstance(payload, dict) else {})
        results.append(
            {
                "ok": resp.status_code == 200 and bool(safe.get("ok")),
                "status": resp.status_code,
                "intent_id": intent.get("intent_id"),
                "account_id": account_id,
                "market_id": intent.get("market_id"),
                "outcome": intent.get("outcome"),
                "side": intent.get("side"),
                "price": intent.get("price"),
                "size": intent.get("size"),
                "market_mode": intent.get("market_mode"),
                "preview": safe,
            }
        )
    return {
        "ok": bool(results) and all(bool(row.get("ok")) for row in results),
        "checked": len(results),
        "failed": sum(1 for row in results if not row.get("ok")),
        "source_ts": state.get("ts"),
        "results": results,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview current Predict.fun desired order intents through the Mac mini signer. Does not submit.")
    parser.add_argument("--config", default="platforms/predictfun/maker/config.mainnet.json")
    parser.add_argument("--include-keep", action="store_true", help="Also preview kept/current desired intents, not only creates.")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    result = preview_intents(
        cfg=load_config(config_path),
        config_path=config_path,
        include_keep=bool(args.include_keep),
        limit=args.limit,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
