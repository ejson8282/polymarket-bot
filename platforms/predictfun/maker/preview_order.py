from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import requests

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.client import PredictFunClient
from platforms.predictfun.maker.dry_run import load_config, run_once


def _first_quote(state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    for plan in state.get("plans", []):
        if not isinstance(plan, dict) or not plan.get("can_quote"):
            continue
        quotes = list(plan.get("yes_quotes") or []) + list(plan.get("no_quotes") or [])
        if quotes:
            return plan, quotes[0]
    raise RuntimeError("no_quote_available")


def _outcome_for_quote(raw_market: dict[str, Any], outcome_name: str) -> dict[str, Any]:
    outcomes = raw_market.get("outcomes") if isinstance(raw_market.get("outcomes"), list) else []
    for item in outcomes:
        if isinstance(item, dict) and str(item.get("name", "")).upper() == outcome_name.upper():
            return item
    if outcome_name.upper() == "YES" and outcomes:
        return outcomes[0]
    if outcome_name.upper() == "NO" and len(outcomes) > 1:
        return outcomes[1]
    raise RuntimeError("outcome_not_found")


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview a Predict.fun signed order via the Mac mini signer; does not submit.")
    parser.add_argument("--config", default="platforms/predictfun/maker/config.mainnet.json")
    parser.add_argument("--account", default="account_01")
    parser.add_argument("--max-markets", type=int, default=5)
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    if args.max_markets > 0:
        cfg.setdefault("scan", {})["max_markets"] = args.max_markets
    client = PredictFunClient(base_url=str(cfg["base_url"]))
    state = run_once(client, cfg, config_path=config_path)
    plan, quote = _first_quote(state)
    market = plan["market"]
    raw_market = client.get_market(market["id"])["data"]
    outcome = _outcome_for_quote(raw_market, quote["outcome"])
    signer_cfg = cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    signer_url = str(signer_cfg.get("base_url") or cfg.get("base_url") or "").rstrip("/")
    if not signer_url:
        raise RuntimeError("missing_signer_base_url")
    body = {
        "side": quote["side"],
        "token_id": str(outcome["onChainId"]),
        "price": quote["price"],
        "size": quote["size"],
        "fee_rate_bps": raw_market.get("feeRateBps", 0),
        "is_neg_risk": bool(raw_market.get("isNegRisk", False)),
        "is_yield_bearing": bool(raw_market.get("isYieldBearing", False)),
    }
    resp = requests.post(
        f"{signer_url}/predictfun/accounts/{args.account}/preview-order",
        json=body,
        timeout=float(signer_cfg.get("timeout_sec") or 20),
    )
    preview = resp.json()
    print(json.dumps({
        "ok": resp.status_code == 200 and bool(preview.get("ok")),
        "status": resp.status_code,
        "market_id": market["id"],
        "outcome": quote["outcome"],
        "side": quote["side"],
        "price": quote["price"],
        "size": quote["size"],
        "preview": preview,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
