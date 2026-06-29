from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

import requests

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.client import PredictFunClient
from platforms.predictfun.maker.dry_run import load_config
from platforms.predictfun.scanner import scan_markets


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _fmt(value: Decimal, places: int = 3) -> str:
    quant = Decimal(1).scaleb(-places)
    return str(value.quantize(quant, rounding=ROUND_DOWN).normalize())


def _pick_market(client: PredictFunClient, cfg: dict[str, Any], market_id: int | None) -> dict[str, Any]:
    if market_id:
        return client.get_market(market_id)["data"]
    scan_cfg = cfg.get("scan") if isinstance(cfg.get("scan"), dict) else {}
    markets = scan_markets(
        client,
        max_markets=10,
        first=int(scan_cfg.get("first") or 50),
        has_active_rewards=True,
        min_hourly_rate=Decimal("0"),
        include_crypto_updown=False,
    )
    if not markets:
        raise RuntimeError("no_open_reward_markets")
    return client.get_market(markets[0].id)["data"]


def _pick_outcome(market: dict[str, Any], outcome_name: str) -> dict[str, Any]:
    outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
    if not outcomes:
        raise RuntimeError("market_has_no_outcomes")
    if outcome_name:
        for item in outcomes:
            if isinstance(item, dict) and str(item.get("name", "")).upper() == outcome_name.upper():
                return item
        if outcome_name.upper() == "YES":
            return outcomes[0]
        if outcome_name.upper() == "NO" and len(outcomes) > 1:
            return outcomes[1]
        raise RuntimeError("outcome_not_found")
    return outcomes[0]


def _safe_buy_price(outcome: dict[str, Any], explicit_price: str) -> str:
    if explicit_price:
        return explicit_price
    bid = _dec((outcome.get("bestBid") or {}).get("price") if isinstance(outcome.get("bestBid"), dict) else None)
    ask = _dec((outcome.get("bestAsk") or {}).get("price") if isinstance(outcome.get("bestAsk"), dict) else None)
    floor = Decimal("0.001")
    if bid > Decimal("0.02"):
        price = bid - Decimal("0.01")
    elif bid > 0:
        price = bid / Decimal("2")
    else:
        price = Decimal("0.01")
    if ask > 0 and price >= ask:
        price = ask - Decimal("0.01")
    return _fmt(max(floor, price), 3)


def main() -> None:
    parser = argparse.ArgumentParser(description="Submit one tiny Predict.fun post-only order through Mac mini signer; default is preview only.")
    parser.add_argument("--config", default="platforms/predictfun/maker/config.mainnet.json")
    parser.add_argument("--account", default="account_01")
    parser.add_argument("--market-id", type=int, default=0)
    parser.add_argument("--outcome", default="")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"])
    parser.add_argument("--price", default="", help="Default auto-picks a non-crossing BUY price.")
    parser.add_argument("--size", default="1", help="Share quantity. Default 1 share.")
    parser.add_argument("--max-notional", default="1", help="Hard USDC notional cap for live submission.")
    parser.add_argument("--live", action="store_true", help="Actually submit the order.")
    parser.add_argument("--confirm", default="", help="Must be SUBMIT_PREDICTFUN_ORDER for live submission.")
    parser.add_argument("--remove-after", action="store_true", help="After a successful submit, remove by order hash.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    client = PredictFunClient(base_url=str(cfg["base_url"]))
    market = _pick_market(client, cfg, args.market_id or None)
    outcome = _pick_outcome(market, args.outcome)
    if args.side != "BUY" and not args.price:
        raise RuntimeError("SELL requires explicit --price and existing position balance")
    price = _safe_buy_price(outcome, args.price) if args.side == "BUY" else args.price
    body = {
        "side": args.side,
        "token_id": str(outcome["onChainId"]),
        "price": price,
        "size": args.size,
        "fee_rate_bps": market.get("feeRateBps", 0),
        "is_neg_risk": bool(market.get("isNegRisk", False)),
        "is_yield_bearing": bool(market.get("isYieldBearing", False)),
        "is_post_only": True,
        "max_notional_usdc": args.max_notional,
    }
    signer_cfg = cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    signer_url = str(signer_cfg.get("base_url") or cfg.get("base_url") or "").rstrip("/")
    if not signer_url:
        raise RuntimeError("missing_signer_base_url")
    if args.live:
        body["submit"] = True
        body["confirm"] = args.confirm
        endpoint = "submit-order"
    else:
        endpoint = "preview-order"
    resp = requests.post(
        f"{signer_url}/predictfun/accounts/{args.account}/{endpoint}",
        json=body,
        timeout=float(signer_cfg.get("timeout_sec") or 20),
    )
    payload = resp.json()
    result: dict[str, Any] = {
        "ok": resp.status_code == 200 and bool(payload.get("ok")),
        "status": resp.status_code,
        "live": bool(args.live),
        "market_id": market.get("id"),
        "market_title": market.get("title"),
        "outcome": outcome.get("name"),
        "side": args.side,
        "price": price,
        "size": args.size,
        "response": payload,
    }
    if args.live and args.remove_after and result["ok"] and payload.get("order_hash"):
        remove_resp = requests.post(
            f"{signer_url}/predictfun/accounts/{args.account}/remove-order-by-hash",
            json={"remove": True, "confirm": "REMOVE_PREDICTFUN_ORDER", "hash": payload["order_hash"]},
            timeout=float(signer_cfg.get("timeout_sec") or 20),
        )
        result["remove"] = {"status": remove_resp.status_code, "response": remove_resp.json()}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
