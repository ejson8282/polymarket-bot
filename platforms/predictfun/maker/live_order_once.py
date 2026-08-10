from __future__ import annotations

import argparse
import json
import re
import sys
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any

import requests

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.client import PredictFunClient
from platforms.predictfun.maker.dry_run import load_config
from platforms.predictfun.maker.validation import validate_final_order
from platforms.predictfun.scanner import scan_markets


IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9_.:-]{1,160}")


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _fmt(value: Decimal, places: int = 3) -> str:
    quant = Decimal(1).scaleb(-places)
    return str(value.quantize(quant, rounding=ROUND_DOWN).normalize())


def _emit_result(result: dict[str, Any]) -> int:
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result.get("ok") is True else 1


def _mode_key(*, is_neg_risk: bool, is_yield_bearing: bool) -> str:
    if is_neg_risk and is_yield_bearing:
        return "neg_risk_yield_bearing"
    if is_neg_risk:
        return "neg_risk"
    if is_yield_bearing:
        return "yield_bearing"
    return "standard"


def _buy_notional_usdc(price: str, size: str) -> Decimal:
    return _dec(price) * _dec(size)


def _allowance_preflight(
    *,
    signer_url: str,
    account: str,
    timeout: float,
    mode_key: str,
    required_notional: Decimal,
) -> dict[str, Any]:
    url = f"{signer_url}/predictfun/accounts/{account}/allowances"
    try:
        resp = requests.post(url, timeout=timeout)
        payload = resp.json() if resp.content else {}
    except Exception as exc:
        return {"ok": False, "error": "allowance_check_failed", "detail": type(exc).__name__}
    if not isinstance(payload, dict) or not payload.get("ok"):
        return {
            "ok": False,
            "error": "allowance_check_failed",
            "status": resp.status_code,
            "body": payload if isinstance(payload, dict) else {},
        }
    modes = payload.get("modes") if isinstance(payload.get("modes"), dict) else {}
    row = modes.get(mode_key) if isinstance(modes.get(mode_key), dict) else {}
    allowance = _dec(row.get("allowance"), "0")
    return {
        "ok": allowance >= required_notional,
        "mode": mode_key,
        "allowance": str(allowance),
        "required_notional": str(required_notional),
        "spender": row.get("spender"),
        "error": "" if allowance >= required_notional else "collateral_allowance_too_low",
    }


def _cancel_preflight(
    *, signer_url: str, account: str, timeout: float, min_gas_bnb: str
) -> dict[str, Any]:
    try:
        resp = requests.post(
            f"{signer_url}/predictfun/accounts/{account}/cancel-orders",
            json={
                "cancel": True,
                "confirm": "CANCEL_PREDICTFUN_ORDERS",
                "preflight_only": True,
                "min_gas_bnb": min_gas_bnb,
            },
            timeout=timeout,
        )
        payload = resp.json()
    except Exception as exc:
        return {
            "ok": False,
            "error": "cancel_preflight_failed",
            "detail": type(exc).__name__,
        }
    payload = payload if isinstance(payload, dict) else {}
    ok = (
        resp.status_code == 200
        and payload.get("ok") is True
        and payload.get("preflight_only") is True
        and payload.get("on_chain_action") is False
    )
    return {
        "ok": ok,
        "status": resp.status_code,
        "gas_balance_bnb": payload.get("gas_balance_bnb"),
        "min_gas_bnb": payload.get("min_gas_bnb") or min_gas_bnb,
        "error": "" if ok else str(payload.get("error") or "cancel_preflight_failed"),
        "response": payload,
    }


def _cancel_submitted_order(
    *,
    signer_url: str,
    account: str,
    timeout: float,
    order_hash: str,
    min_gas_bnb: str,
) -> dict[str, Any]:
    try:
        resp = requests.post(
            f"{signer_url}/predictfun/accounts/{account}/cancel-orders",
            json={
                "cancel": True,
                "confirm": "CANCEL_PREDICTFUN_ORDERS",
                "hashes": [order_hash],
                "min_gas_bnb": min_gas_bnb,
            },
            timeout=timeout,
        )
        payload = resp.json()
    except Exception as exc:
        return {
            "ok": False,
            "error": "verified_cancel_failed",
            "detail": type(exc).__name__,
        }
    payload = payload if isinstance(payload, dict) else {}
    transactions = payload.get("transactions")
    transactions = transactions if isinstance(transactions, list) else []
    receipts_verified = bool(transactions) and all(
        isinstance(row, dict)
        and row.get("success") is True
        and int(row.get("receipt_status") or 0) == 1
        and bool(
            re.fullmatch(
                r"0x[0-9a-fA-F]{64}", str(row.get("tx_hash") or "")
            )
        )
        for row in transactions
    )
    ok = (
        resp.status_code == 200
        and payload.get("ok") is True
        and payload.get("verified") is True
        and payload.get("on_chain_cancelled") is True
        and payload.get("off_book_removed") is True
        and not payload.get("open_hashes")
        and receipts_verified
    )
    return {
        "ok": ok,
        "status": resp.status_code,
        "verified": payload.get("verified") is True,
        "on_chain_cancelled": payload.get("on_chain_cancelled") is True,
        "receipts_verified": receipts_verified,
        "transactions": transactions,
        "error": "" if ok else str(payload.get("error") or "verified_cancel_failed"),
        "response": payload,
    }


def _pick_market(client: PredictFunClient, cfg: dict[str, Any], market_id: int | None) -> dict[str, Any]:
    if market_id:
        return client.get_market(market_id)["data"]
    scan_cfg = cfg.get("scan") if isinstance(cfg.get("scan"), dict) else {}
    strategy_cfg = cfg.get("strategy") if isinstance(cfg.get("strategy"), dict) else {}
    markets = scan_markets(
        client,
        max_markets=10,
        first=int(scan_cfg.get("first") or 50),
        has_active_rewards=True,
        min_hourly_rate=Decimal("0"),
        include_crypto_updown=False,
        scoring_profile=str(strategy_cfg.get("profile") or "conservative"),
    )
    if not markets:
        raise RuntimeError("no_open_reward_markets")
    return client.get_market(markets[0].id)["data"]


def _pick_outcome(market: dict[str, Any], outcome_name: str) -> dict[str, Any]:
    outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
    outcomes_by_name = {
        str(item.get("name") or "").strip().upper(): item
        for item in outcomes
        if isinstance(item, dict)
    }
    if len(outcomes) != 2 or set(outcomes_by_name) != {"YES", "NO"}:
        raise RuntimeError("market_requires_canonical_yes_no_outcomes")
    selected = str(outcome_name or "YES").strip().upper()
    if selected not in outcomes_by_name:
        raise RuntimeError("outcome_not_found")
    return outcomes_by_name[selected]


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Submit one tiny Predict.fun post-only order through Mac mini signer; default is preview only.")
    parser.add_argument("--config", default="platforms/predictfun/maker/config.mainnet.json")
    parser.add_argument("--account", default="account_01")
    parser.add_argument("--market-id", type=int, default=0)
    parser.add_argument("--outcome", default="")
    parser.add_argument("--side", default="BUY", choices=["BUY", "SELL"])
    parser.add_argument("--price", default="", help="Default auto-picks a non-crossing BUY price.")
    parser.add_argument("--size", default="1", help="Share quantity. Default 1 share.")
    parser.add_argument("--max-notional", default="1", help="Hard USDC notional cap for live submission.")
    parser.add_argument(
        "--idempotency-key",
        default="",
        help="Required stable operator-supplied key for a live canary and any retry.",
    )
    parser.add_argument("--live", action="store_true", help="Actually submit the order.")
    parser.add_argument("--confirm", default="", help="Must be SUBMIT_PREDICTFUN_ORDER for live submission.")
    parser.add_argument(
        "--cancel-after",
        action="store_true",
        help="After submission, require a verified on-chain cancellation.",
    )
    parser.add_argument(
        "--remove-after",
        action="store_true",
        help="Deprecated alias for --cancel-after; now performs full on-chain cancellation.",
    )
    parser.add_argument(
        "--min-cancel-gas-bnb",
        default="0.0001",
        help="Minimum signer BNB balance required before live submission.",
    )
    args = parser.parse_args()
    cancel_after = bool(args.cancel_after or args.remove_after)
    if args.live and not cancel_after:
        parser.error(
            "--live requires --cancel-after so the canary cannot leave an order open"
        )
    idempotency_key = str(args.idempotency_key or "").strip()
    if args.live and not idempotency_key:
        parser.error("--live requires --idempotency-key")
    if idempotency_key and not IDEMPOTENCY_KEY_RE.fullmatch(idempotency_key):
        parser.error(
            "--idempotency-key must be 1-160 characters using letters, digits, '.', '_', ':', or '-'"
        )

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    client = PredictFunClient(base_url=str(cfg["base_url"]))
    market = _pick_market(client, cfg, args.market_id or None)
    outcome = _pick_outcome(market, args.outcome)
    if args.side != "BUY" and not args.price:
        raise RuntimeError("SELL requires explicit --price and existing position balance")
    price = _safe_buy_price(outcome, args.price) if args.side == "BUY" else args.price
    body = {
        "market_id": int(market["id"]),
        "side": args.side,
        "token_id": str(outcome["onChainId"]),
        "price": price,
        "size": args.size,
        "fee_rate_bps": market.get("feeRateBps", 0),
        "is_neg_risk": bool(market.get("isNegRisk", False)),
        "is_yield_bearing": bool(market.get("isYieldBearing", False)),
        "is_post_only": True,
        "self_trade_prevention": "CANCEL_MAKER",
        "max_notional_usdc": args.max_notional,
    }
    if idempotency_key:
        body["idempotency_key"] = idempotency_key
        body["intent_id"] = idempotency_key
    signer_cfg = cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    signer_url = str(
        signer_cfg.get("base_url") or cfg.get("base_url") or ""
    ).rstrip("/")
    if not signer_url:
        raise RuntimeError("missing_signer_base_url")
    if args.live:
        timeout = float(signer_cfg.get("timeout_sec") or 20)
        cancel_preflight = _cancel_preflight(
            signer_url=signer_url,
            account=args.account,
            timeout=timeout,
            min_gas_bnb=args.min_cancel_gas_bnb,
        )
        if not cancel_preflight.get("ok"):
            return _emit_result(
                {
                    "ok": False,
                    "status": 0,
                    "live": True,
                    "market_id": market.get("id"),
                    "market_title": market.get("title"),
                    "outcome": outcome.get("name"),
                    "side": args.side,
                    "price": price,
                    "size": args.size,
                    "error": (
                        cancel_preflight.get("error")
                        or "cancel_preflight_failed"
                    ),
                    "cancel_preflight": cancel_preflight,
                }
            )
        mode_key = _mode_key(
            is_neg_risk=bool(market.get("isNegRisk", False)),
            is_yield_bearing=bool(market.get("isYieldBearing", False)),
        )
        required_notional = (
            _buy_notional_usdc(price, args.size)
            if args.side == "BUY"
            else Decimal("0")
        )
        allowance_check = _allowance_preflight(
            signer_url=signer_url,
            account=args.account,
            timeout=timeout,
            mode_key=mode_key,
            required_notional=required_notional,
        )
        if args.side == "BUY" and not allowance_check.get("ok"):
            return _emit_result(
                {
                    "ok": False,
                    "status": 0,
                    "live": True,
                    "market_id": market.get("id"),
                    "market_title": market.get("title"),
                    "outcome": outcome.get("name"),
                    "side": args.side,
                    "price": price,
                    "size": args.size,
                    "error": allowance_check.get("error") or "allowance_preflight_failed",
                    "allowance": allowance_check,
                }
            )
        try:
            fresh_payload = client.get_market(int(market.get("id") or 0))
            fresh_market = (
                fresh_payload.get("data")
                if isinstance(fresh_payload.get("data"), dict)
                else {}
            )
            final_preflight = validate_final_order(
                original_market=market,
                fresh_market=fresh_market,
                token_id=str(outcome.get("onChainId") or ""),
                side=args.side,
                price=_dec(price),
                size=_dec(args.size),
                max_notional=_dec(args.max_notional),
            )
        except Exception as exc:
            final_preflight = {
                "ok": False,
                "reason": f"fresh_market_check_failed:{type(exc).__name__}",
            }
        if not final_preflight.get("ok"):
            return _emit_result(
                {
                    "ok": False,
                    "status": 0,
                    "live": True,
                    "market_id": market.get("id"),
                    "market_title": market.get("title"),
                    "outcome": outcome.get("name"),
                    "side": args.side,
                    "price": price,
                    "size": args.size,
                    "error": "final_preflight_blocked",
                    "final_preflight": final_preflight,
                }
            )
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
    if args.live:
        result["final_preflight"] = final_preflight
        result["cancel_preflight"] = cancel_preflight
    if args.live and result["ok"]:
        order_hash = str(payload.get("order_hash") or "")
        if not order_hash:
            result["ok"] = False
            result["error"] = "missing_order_hash_for_verified_cancel"
        else:
            cancel_result = _cancel_submitted_order(
                signer_url=signer_url,
                account=args.account,
                timeout=float(signer_cfg.get("timeout_sec") or 20),
                order_hash=order_hash,
                min_gas_bnb=args.min_cancel_gas_bnb,
            )
            result["cancel"] = cancel_result
            if not cancel_result.get("ok"):
                result["ok"] = False
                result["error"] = (
                    cancel_result.get("error") or "verified_cancel_failed"
                )
    return _emit_result(result)


if __name__ == "__main__":
    raise SystemExit(main())
