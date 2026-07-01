from __future__ import annotations

import argparse
import json
import os
import sys
import time
import requests
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.client import PredictFunClient, as_decimal, decimal_tick
from platforms.predictfun.maker.intents import (
    build_intent_state,
    load_previous_intents,
    write_intent_state,
)
from platforms.predictfun.scanner import PredictMarket, market_to_jsonable, scan_markets


@dataclass
class QuoteLevel:
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    notional: Decimal
    reason: str


@dataclass
class DryRunPlan:
    market: PredictMarket
    can_quote: bool
    skip_reason: str
    orderbook_source: str
    best_yes_bid: Decimal
    best_yes_ask: Decimal
    mid: Decimal
    yes_quotes: list[QuoteLevel]
    no_quotes: list[QuoteLevel]


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _allowed_market_modes(raw: Any) -> set[tuple[bool, bool]]:
    if not raw:
        return set()
    rows = raw if isinstance(raw, list) else [raw]
    modes: set[tuple[bool, bool]] = set()
    for row in rows:
        if isinstance(row, dict):
            modes.add((bool(row.get("neg_risk")), bool(row.get("yield_bearing"))))
        elif isinstance(row, str):
            normalized = row.strip().lower().replace("-", "_").replace(" ", "")
            if normalized in {"standard", "false_false", "negfalse_yieldfalse"}:
                modes.add((False, False))
            elif normalized in {"neg_risk", "true_false", "negtrue_yieldfalse"}:
                modes.add((True, False))
            elif normalized in {"yield_bearing", "false_true", "negfalse_yieldtrue"}:
                modes.add((False, True))
            elif normalized in {"neg_risk_yield_bearing", "true_true", "negtrue_yieldtrue"}:
                modes.add((True, True))
    return modes


def _market_mode_skip_reason(market: PredictMarket, allowed_modes: set[tuple[bool, bool]]) -> str:
    if not allowed_modes or (market.is_neg_risk, market.is_yield_bearing) in allowed_modes:
        return ""
    return (
        "market mode not enabled "
        f"(neg_risk={market.is_neg_risk}, yield_bearing={market.is_yield_bearing})"
    )


def build_quote_plan(
    market: PredictMarket,
    orderbook: dict[str, Any],
    *,
    quote_size: Decimal,
    edge_ticks: int,
    backoff_ticks: int,
    max_quote_levels: int,
    seed_empty_books: bool,
    seed_mid_price: Decimal,
    seed_distance_ticks: int,
    min_seconds_to_expiry: float,
    avoid_mid_band_low: Decimal,
    avoid_mid_band_high: Decimal,
    allow_crypto_updown_quotes: bool,
    min_order_notional: Decimal = Decimal("0"),
    max_order_notional: Decimal = Decimal("0"),
    min_depth_notional: Decimal = Decimal("0"),
    min_depth_shares: Decimal = Decimal("0"),
    depth_levels: int = 3,
) -> DryRunPlan:
    tick = decimal_tick(market.decimal_precision)
    data = orderbook.get("data") if isinstance(orderbook.get("data"), dict) else {}
    bids = _levels(data.get("bids"))
    asks = _levels(data.get("asks"))

    best_yes_bid = bids[0][0] if bids else market.best_yes_bid
    best_yes_ask = asks[0][0] if asks else market.best_yes_ask
    mid = Decimal("0")
    if best_yes_bid > 0 and best_yes_ask > 0:
        mid = (best_yes_bid + best_yes_ask) / Decimal("2")

    truly_empty_book = not bids and not asks and market.best_yes_bid <= 0 and market.best_yes_ask <= 0
    if truly_empty_book and (min_depth_notional > 0 or min_depth_shares > 0):
        return DryRunPlan(
            market=market,
            can_quote=False,
            skip_reason="empty book liquidity guard",
            orderbook_source=str(orderbook.get("_source") or "unknown"),
            best_yes_bid=best_yes_bid,
            best_yes_ask=best_yes_ask,
            mid=mid,
            yes_quotes=[],
            no_quotes=[],
        )
    if seed_empty_books and truly_empty_book:
        skip = _basic_skip_reason(
            market,
            min_seconds_to_expiry=min_seconds_to_expiry,
            allow_crypto_updown_quotes=allow_crypto_updown_quotes,
        )
        if skip:
            return DryRunPlan(
                market=market,
                can_quote=False,
                skip_reason=skip,
                orderbook_source=str(orderbook.get("_source") or "unknown"),
                best_yes_bid=best_yes_bid,
                best_yes_ask=best_yes_ask,
                mid=mid,
                yes_quotes=[],
                no_quotes=[],
            )
        seed_mid = min(Decimal("0.95"), max(Decimal("0.05"), seed_mid_price))
        seed_quotes = _filter_quotes(
            _seed_empty_book_quotes(
                tick=tick,
                quote_size=quote_size,
                mid=seed_mid,
                distance_ticks=seed_distance_ticks,
                backoff_ticks=backoff_ticks,
                max_quote_levels=max_quote_levels,
            ),
            min_order_notional=min_order_notional,
            max_order_notional=max_order_notional,
        )
        return DryRunPlan(
            market=market,
            can_quote=bool(seed_quotes),
            skip_reason="" if seed_quotes else "empty book seed produced no quotes",
            orderbook_source=f"{orderbook.get('_source') or 'unknown'}:empty_seed",
            best_yes_bid=best_yes_bid,
            best_yes_ask=best_yes_ask,
            mid=seed_mid,
            yes_quotes=[quote for quote in seed_quotes if quote.outcome == "YES"],
            no_quotes=[quote for quote in seed_quotes if quote.outcome == "NO"],
        )

    skip = _quote_skip_reason(
        market,
        best_yes_bid=best_yes_bid,
        best_yes_ask=best_yes_ask,
        mid=mid,
        min_seconds_to_expiry=min_seconds_to_expiry,
        avoid_mid_band_low=avoid_mid_band_low,
        avoid_mid_band_high=avoid_mid_band_high,
        allow_crypto_updown_quotes=allow_crypto_updown_quotes,
    )
    if skip:
        return DryRunPlan(
            market=market,
            can_quote=False,
            skip_reason=skip,
            orderbook_source=str(orderbook.get("_source") or "unknown"),
            best_yes_bid=best_yes_bid,
            best_yes_ask=best_yes_ask,
            mid=mid,
            yes_quotes=[],
            no_quotes=[],
        )

    liquidity_skip = _liquidity_skip_reason(
        bids,
        asks,
        depth_levels=depth_levels,
        min_depth_notional=min_depth_notional,
        min_depth_shares=min_depth_shares,
    )
    if liquidity_skip:
        return DryRunPlan(
            market=market,
            can_quote=False,
            skip_reason=liquidity_skip,
            orderbook_source=str(orderbook.get("_source") or "unknown"),
            best_yes_bid=best_yes_bid,
            best_yes_ask=best_yes_ask,
            mid=mid,
            yes_quotes=[],
            no_quotes=[],
        )

    yes_quotes = _filter_quotes(_bid_ladder(
        outcome="YES",
        best_bid=best_yes_bid,
        best_ask=best_yes_ask,
        mid=mid,
        tick=tick,
        spread_threshold=market.spread_threshold,
        quote_size=quote_size,
        edge_ticks=edge_ticks,
        backoff_ticks=backoff_ticks,
        max_quote_levels=max_quote_levels,
    ), min_order_notional=min_order_notional, max_order_notional=max_order_notional)

    no_best_bid = _round_price(Decimal("1") - best_yes_ask, tick)
    no_best_ask = _round_price(Decimal("1") - best_yes_bid, tick)
    no_mid = Decimal("1") - mid if mid > 0 else Decimal("0")
    no_quotes = _filter_quotes(_bid_ladder(
        outcome="NO",
        best_bid=no_best_bid,
        best_ask=no_best_ask,
        mid=no_mid,
        tick=tick,
        spread_threshold=market.spread_threshold,
        quote_size=quote_size,
        edge_ticks=edge_ticks,
        backoff_ticks=backoff_ticks,
        max_quote_levels=max_quote_levels,
    ), min_order_notional=min_order_notional, max_order_notional=max_order_notional)

    return DryRunPlan(
        market=market,
        can_quote=bool(yes_quotes or no_quotes),
        skip_reason="" if (yes_quotes or no_quotes) else "no legal passive quote inside reward band",
        orderbook_source=str(orderbook.get("_source") or "unknown"),
        best_yes_bid=best_yes_bid,
        best_yes_ask=best_yes_ask,
        mid=mid,
        yes_quotes=yes_quotes,
        no_quotes=no_quotes,
    )


def _bid_ladder(
    *,
    outcome: str,
    best_bid: Decimal,
    best_ask: Decimal,
    mid: Decimal,
    tick: Decimal,
    spread_threshold: Decimal,
    quote_size: Decimal,
    edge_ticks: int,
    backoff_ticks: int,
    max_quote_levels: int,
) -> list[QuoteLevel]:
    if best_bid <= 0 or best_ask <= 0 or best_ask <= best_bid:
        return []

    reward_floor = max(tick, mid - spread_threshold)
    safe_top = best_bid - tick * max(1, edge_ticks)
    if safe_top < reward_floor:
        return []

    quotes: list[QuoteLevel] = []
    step = tick * max(1, backoff_ticks)
    price = _round_price(safe_top, tick)
    for level in range(max_quote_levels):
        if price < reward_floor or price <= 0:
            break
        quotes.append(
            QuoteLevel(
                outcome=outcome,
                side="BUY",
                price=price,
                size=quote_size,
                notional=price * quote_size,
                reason=f"level={level + 1} reward_floor={reward_floor} safe_top={safe_top}",
            )
        )
        price = _round_price(price - step, tick)
    return quotes


def _filter_quotes(
    quotes: list[QuoteLevel],
    *,
    min_order_notional: Decimal,
    max_order_notional: Decimal,
) -> list[QuoteLevel]:
    out: list[QuoteLevel] = []
    for quote in quotes:
        if quote.price <= 0:
            continue
        size = quote.size
        notional = quote.notional
        reason = quote.reason
        if min_order_notional > 0 and notional < min_order_notional:
            required_size = (min_order_notional / quote.price).quantize(Decimal("0.000001"), rounding=ROUND_UP)
            if required_size > size:
                size = required_size
                notional = quote.price * size
                reason = f"{reason} min_notional_adjusted={min_order_notional}"
        if max_order_notional > 0 and notional > max_order_notional:
            continue
        out.append(
            QuoteLevel(
                outcome=quote.outcome,
                side=quote.side,
                price=quote.price,
                size=size,
                notional=notional,
                reason=reason,
            )
        )
    return out


def _liquidity_skip_reason(
    bids: list[tuple[Decimal, Decimal]],
    asks: list[tuple[Decimal, Decimal]],
    *,
    depth_levels: int,
    min_depth_notional: Decimal,
    min_depth_shares: Decimal,
) -> str:
    if min_depth_notional <= 0 and min_depth_shares <= 0:
        return ""
    levels = max(1, depth_levels)
    bid_notional = _depth_notional(bids, levels)
    ask_notional = _depth_notional(asks, levels)
    bid_shares = _depth_shares(bids, levels)
    ask_shares = _depth_shares(asks, levels)
    if min_depth_notional > 0 and (bid_notional < min_depth_notional or ask_notional < min_depth_notional):
        return f"thin book depth_notional bid={bid_notional} ask={ask_notional} min={min_depth_notional}"
    if min_depth_shares > 0 and (bid_shares < min_depth_shares or ask_shares < min_depth_shares):
        return f"thin book depth_shares bid={bid_shares} ask={ask_shares} min={min_depth_shares}"
    return ""


def _depth_notional(levels: list[tuple[Decimal, Decimal]], limit: int) -> Decimal:
    return sum((price * size for price, size in levels[:limit]), Decimal("0"))


def _depth_shares(levels: list[tuple[Decimal, Decimal]], limit: int) -> Decimal:
    return sum((size for _, size in levels[:limit]), Decimal("0"))


def _seed_empty_book_quotes(
    *,
    tick: Decimal,
    quote_size: Decimal,
    mid: Decimal,
    distance_ticks: int,
    backoff_ticks: int,
    max_quote_levels: int,
) -> list[QuoteLevel]:
    if tick <= 0 or quote_size <= 0:
        return []
    distance = tick * max(1, distance_ticks)
    step = tick * max(1, backoff_ticks)
    start = min(Decimal("1") - tick, max(tick, _round_price(mid - distance, tick)))
    quotes: list[QuoteLevel] = []
    for level in range(max_quote_levels):
        price = _round_price(start - step * level, tick)
        if price <= 0 or price >= 1:
            continue
        reason = f"empty_book_seed level={level + 1} seed_mid={mid} distance_ticks={distance_ticks}"
        for outcome in ("YES", "NO"):
            quotes.append(
                QuoteLevel(
                    outcome=outcome,
                    side="BUY",
                    price=price,
                    size=quote_size,
                    notional=price * quote_size,
                    reason=reason,
                )
            )
    return quotes


def _basic_skip_reason(
    market: PredictMarket,
    *,
    min_seconds_to_expiry: float,
    allow_crypto_updown_quotes: bool,
) -> str:
    if market.hourly_rate <= 0:
        return "no active reward"
    if market.market_variant == "CRYPTO_UP_DOWN" and not allow_crypto_updown_quotes:
        return "crypto up/down disabled in config"
    seconds_left = _seconds_to(market.ends_at)
    if seconds_left is not None and seconds_left < min_seconds_to_expiry:
        return f"near expiry seconds_left={seconds_left:.0f}"
    return ""


def _quote_skip_reason(
    market: PredictMarket,
    *,
    best_yes_bid: Decimal,
    best_yes_ask: Decimal,
    mid: Decimal,
    min_seconds_to_expiry: float,
    avoid_mid_band_low: Decimal,
    avoid_mid_band_high: Decimal,
    allow_crypto_updown_quotes: bool,
) -> str:
    basic = _basic_skip_reason(
        market,
        min_seconds_to_expiry=min_seconds_to_expiry,
        allow_crypto_updown_quotes=allow_crypto_updown_quotes,
    )
    if basic:
        return basic
    if best_yes_bid <= 0 or best_yes_ask <= 0:
        return "empty book"
    if best_yes_ask <= best_yes_bid:
        return "crossed book"
    if avoid_mid_band_low <= mid <= avoid_mid_band_high:
        return f"mid risk band mid={mid}"
    return ""


def _levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    if not isinstance(raw, list):
        return []
    out: list[tuple[Decimal, Decimal]] = []
    for row in raw:
        if not isinstance(row, list) or len(row) < 2:
            continue
        out.append((as_decimal(row[0]), as_decimal(row[1])))
    return out


def _round_price(value: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return value
    return value.quantize(tick, rounding=ROUND_DOWN)


def _seconds_to(value: str) -> float | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return (dt - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return None


def quote_to_jsonable(quote: QuoteLevel) -> dict[str, Any]:
    data = asdict(quote)
    for key, value in list(data.items()):
        if isinstance(value, Decimal):
            data[key] = str(value)
    return data


def plan_to_jsonable(plan: DryRunPlan) -> dict[str, Any]:
    return {
        "market": market_to_jsonable(plan.market),
        "can_quote": plan.can_quote,
        "skip_reason": plan.skip_reason,
        "orderbook_source": plan.orderbook_source,
        "best_yes_bid": str(plan.best_yes_bid),
        "best_yes_ask": str(plan.best_yes_ask),
        "mid": str(plan.mid),
        "yes_quotes": [quote_to_jsonable(q) for q in plan.yes_quotes],
        "no_quotes": [quote_to_jsonable(q) for q in plan.no_quotes],
    }


def main() -> None:
    default_config = Path(__file__).with_name("config.testnet.json")
    parser = argparse.ArgumentParser(description="Predict.fun dry-run maker for testnet.")
    parser.add_argument("--config", default=str(default_config))
    parser.add_argument("--once", action="store_true", help="Run one scan/plan cycle and exit.")
    parser.add_argument("--json", action="store_true", help="Print full JSON state.")
    parser.add_argument("--interval-sec", type=float, default=30.0, help="Loop interval when --once is not set.")
    parser.add_argument("--include-crypto-updown", action="store_true", help="Override config and include short-window crypto markets.")
    parser.add_argument("--max-markets", type=int, default=0, help="Override scan.max_markets.")
    parser.add_argument("--min-hourly-rate", default="", help="Override scan.min_hourly_rate.")
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = load_config(config_path)
    if args.include_crypto_updown:
        cfg.setdefault("scan", {})["include_crypto_updown"] = True
        cfg.setdefault("risk", {})["allow_crypto_updown_quotes"] = True
    if args.max_markets > 0:
        cfg.setdefault("scan", {})["max_markets"] = args.max_markets
    if args.min_hourly_rate:
        cfg.setdefault("scan", {})["min_hourly_rate"] = args.min_hourly_rate
    api_key = os.getenv(str(cfg.get("api_key_env") or "PREDICTFUN_API_KEY"), "")
    client = PredictFunClient(base_url=str(cfg["base_url"]), api_key=api_key)

    while True:
        state = run_once(client, cfg, config_path=config_path)
        if args.json:
            print(json.dumps(state, indent=2))
        else:
            _print_state(state)
        if args.once:
            break
        time.sleep(max(1.0, args.interval_sec))


def _configured_account_ids(raw: dict[str, Any] | list[Any] | None) -> list[str]:
    if isinstance(raw, list):
        rows = raw
        max_active = len(rows) or 1
    elif isinstance(raw, dict):
        if raw.get("enabled") is False:
            return []
        rows = raw.get("ids") or raw.get("account_ids") or raw.get("accounts") or []
        max_active = int(as_decimal(raw.get("max_active_accounts"), "10") or Decimal("10"))
        if not rows:
            rows = [f"account_{i:02d}" for i in range(1, max_active + 1)]
    else:
        return []

    account_ids: list[str] = []
    seen: set[str] = set()
    for item in rows:
        account_id = ""
        if isinstance(item, dict):
            account_id = str(item.get("account_id") or item.get("id") or item.get("name") or "").strip()
        else:
            account_id = str(item or "").strip()
        if not account_id or account_id in seen:
            continue
        account_ids.append(account_id)
        seen.add(account_id)
        if len(account_ids) >= max(1, max_active):
            break
    return account_ids


def _check_auth_accounts(cfg: dict[str, Any], accounts_cfg: dict[str, Any] | list[Any]) -> dict[str, Any]:
    signer_cfg = cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    if not signer_cfg or signer_cfg.get("enabled") is False:
        return {"enabled": False}
    base_url = str(signer_cfg.get("base_url") or cfg.get("base_url") or "").rstrip("/")
    if not base_url:
        return {"enabled": True, "ok": False, "error": "missing_signer_base_url", "accounts": []}
    path_template = str(signer_cfg.get("auth_check_path") or "/predictfun/accounts/{account_id}/auth-check")
    timeout = float(signer_cfg.get("timeout_sec") or 10)
    accounts = _configured_account_ids(accounts_cfg)
    results: list[dict[str, Any]] = []
    for account_id in accounts:
        path = path_template.format(account_id=account_id)
        url = base_url + (path if path.startswith("/") else "/" + path)
        try:
            resp = requests.post(url, timeout=timeout)
            payload = resp.json() if resp.content else {}
        except Exception as exc:
            results.append({"account_id": account_id, "ok": False, "error": type(exc).__name__})
            continue
        if not isinstance(payload, dict):
            payload = {}
        results.append(
            {
                "account_id": account_id,
                "ok": bool(payload.get("ok")),
                "status": payload.get("status") or resp.status_code,
                "signer": payload.get("signer"),
                "token_present": bool(payload.get("token_present")),
                "error": payload.get("error"),
            }
        )
    return {"enabled": True, "ok": bool(results) and all(row.get("ok") for row in results), "accounts": results}

def run_once(
    client: PredictFunClient,
    cfg: dict[str, Any],
    *,
    config_path: Path,
    previous_intents: list[dict[str, Any]] | None = None,
    inventory_positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    scan_cfg = cfg.get("scan") or {}
    strategy = cfg.get("strategy") or {}
    risk = cfg.get("risk") or {}
    data_cfg = cfg.get("data") or {}
    liquidity_cfg = cfg.get("liquidity") if isinstance(cfg.get("liquidity"), dict) else {}
    out_cfg = cfg.get("output") or {}
    accounts_cfg = cfg.get("accounts") if isinstance(cfg.get("accounts"), (dict, list)) else {}
    inventory_cfg = cfg.get("inventory") if isinstance(cfg.get("inventory"), dict) else {}
    ws_state = {}
    ws_state_path_raw = str(out_cfg.get("ws_state_path") or "").strip()
    if bool(data_cfg.get("use_ws_orderbook_cache", True)) and ws_state_path_raw:
        ws_state_path = (config_path.parent / ws_state_path_raw).resolve()
        ws_state = _load_fresh_ws_state(
            ws_state_path,
            max_age_sec=float(data_cfg.get("ws_state_max_age_sec") or 120),
        )
    markets = scan_markets(
        client,
        max_markets=int(scan_cfg.get("max_markets") or 20),
        first=int(scan_cfg.get("first") or 50),
        has_active_rewards=bool(scan_cfg.get("has_active_rewards", True)),
        min_hourly_rate=as_decimal(scan_cfg.get("min_hourly_rate")),
        include_crypto_updown=bool(scan_cfg.get("include_crypto_updown", False)),
    )

    plans: list[DryRunPlan] = []
    liquidity_blocks = _liquidity_blocks_from_ws_state(ws_state)
    allowed_market_modes = _allowed_market_modes(risk.get("allowed_market_modes"))
    for market in markets:
        mode_skip = _market_mode_skip_reason(market, allowed_market_modes)
        if mode_skip:
            plans.append(
                DryRunPlan(
                    market=market,
                    can_quote=False,
                    skip_reason=mode_skip,
                    orderbook_source="config:market_mode_guard",
                    best_yes_bid=market.best_yes_bid,
                    best_yes_ask=market.best_yes_ask,
                    mid=market.mid,
                    yes_quotes=[],
                    no_quotes=[],
                )
            )
            continue
        block_reason = liquidity_blocks.get(str(market.id))
        if block_reason:
            plans.append(
                DryRunPlan(
                    market=market,
                    can_quote=False,
                    skip_reason=block_reason,
                    orderbook_source="ws:liquidity_sentinel",
                    best_yes_bid=market.best_yes_bid,
                    best_yes_ask=market.best_yes_ask,
                    mid=market.mid,
                    yes_quotes=[],
                    no_quotes=[],
                )
            )
            continue
        cached_book = _book_from_ws_state(ws_state, market.id)
        if cached_book:
            orderbook = cached_book
        else:
            try:
                orderbook = client.get_orderbook(market.id)
                orderbook["_source"] = "rest"
            except Exception as exc:
                orderbook = {"data": {"bids": [], "asks": []}, "error": str(exc), "_source": "rest_error"}
        quote_size = max(
            as_decimal(strategy.get("min_quote_size"), "10"),
            market.share_threshold * as_decimal(strategy.get("quote_size_pct_of_share_threshold"), "1"),
        )
        plans.append(
            build_quote_plan(
                market,
                orderbook,
                quote_size=quote_size,
                edge_ticks=int(strategy.get("edge_ticks") or 1),
                backoff_ticks=int(strategy.get("backoff_ticks") or 2),
                max_quote_levels=int(strategy.get("max_quote_levels") or 2),
                seed_empty_books=bool(strategy.get("seed_empty_books", False)),
                seed_mid_price=as_decimal(strategy.get("seed_mid_price"), "0.50"),
                seed_distance_ticks=int(strategy.get("seed_distance_ticks") or 5),
                min_seconds_to_expiry=float(risk.get("min_seconds_to_expiry") or 3600),
                avoid_mid_band_low=as_decimal(risk.get("avoid_mid_band_low"), "0.35"),
                avoid_mid_band_high=as_decimal(risk.get("avoid_mid_band_high"), "0.65"),
                allow_crypto_updown_quotes=bool(risk.get("allow_crypto_updown_quotes", False)),
                min_order_notional=as_decimal(strategy.get("min_order_notional"), "0"),
                max_order_notional=as_decimal(strategy.get("max_order_notional"), "0"),
                min_depth_notional=as_decimal(liquidity_cfg.get("min_depth_notional"), "0"),
                min_depth_shares=as_decimal(liquidity_cfg.get("min_depth_shares"), "0"),
                depth_levels=int(liquidity_cfg.get("depth_levels") or 3),
            )
        )

    auth_check = _check_auth_accounts(cfg, accounts_cfg)
    state = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "environment": cfg.get("environment", "testnet"),
        "base_url": cfg.get("base_url"),
        "plans": [plan_to_jsonable(plan) for plan in plans],
    }
    if auth_check.get("enabled"):
        state["auth"] = auth_check

    state_path_raw = str(out_cfg.get("state_path") or "").strip()
    if state_path_raw:
        state_path = (config_path.parent / state_path_raw).resolve()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        tmp.replace(state_path)

    intents_path_raw = str(out_cfg.get("intents_path") or "").strip()
    if intents_path_raw:
        intents_path = (config_path.parent / intents_path_raw).resolve()
        if previous_intents is None:
            previous_intents = load_previous_intents(intents_path)
        if inventory_positions is None:
            inventory_positions = _load_inventory_positions(config_path, cfg)
        intent_state = build_intent_state(
            environment=str(cfg.get("environment", "testnet")),
            plans=state["plans"],
            previous_intents=previous_intents,
            accounts_config=accounts_cfg,
            inventory_positions=inventory_positions,
            inventory_config=inventory_cfg,
            planner_config={
                "max_account_notional": risk.get("max_account_desired_notional"),
                "max_account_market_notional": risk.get("max_account_market_desired_notional"),
            },
        )
        write_intent_state(intents_path, intent_state)
        state["intents"] = intent_state.get("summary", {})
    return state


def _print_state(state: dict[str, Any]) -> None:
    print(f"[{state['ts']}] Predict.fun dry-run environment={state['environment']}")
    if state.get("intents"):
        summary = state["intents"]
        print(
            "intents "
            f"desired={summary.get('desired', 0)} "
            f"create={summary.get('create', 0)} "
            f"keep={summary.get('keep', 0)} "
            f"cancel={summary.get('cancel', 0)} "
            f"notional={summary.get('total_notional', '0')}"
        )
    if state.get("auth"):
        auth = state["auth"]
        accounts = auth.get("accounts") if isinstance(auth.get("accounts"), list) else []
        details = ", ".join(
            f"{row.get('account_id')}={'ok' if row.get('ok') else row.get('error') or 'failed'}"
            for row in accounts
            if isinstance(row, dict)
        )
        print(f"auth ok={auth.get('ok')} {details}")
    for plan in state["plans"]:
        status = "QUOTE" if plan["can_quote"] else f"SKIP {plan['skip_reason']}"
        market = plan["market"]
        print(
            f"{status} id={market['id']} hourly={market['hourly_rate']} "
            f"mid={plan['mid']} source={plan.get('orderbook_source', 'unknown')} title={market['title']}"
        )
        for quote in plan["yes_quotes"] + plan["no_quotes"]:
            print(
                f"    {quote['outcome']} {quote['side']} "
                f"px={quote['price']} size={quote['size']} notional={quote['notional']}"
            )


def _load_fresh_ws_state(path: Path, *, max_age_sec: float) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    ts = str(data.get("ts") or "")
    if not ts:
        return {}
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return {}
    age = (datetime.now(timezone.utc) - dt).total_seconds()
    if age > max_age_sec:
        return {}
    return data


def _liquidity_blocks_from_ws_state(ws_state: dict[str, Any]) -> dict[str, str]:
    alerts = ws_state.get("liquidity_alerts") if isinstance(ws_state.get("liquidity_alerts"), dict) else {}
    out: dict[str, str] = {}
    now = datetime.now(timezone.utc)
    for market_id, alert in alerts.items():
        if not isinstance(alert, dict) or not alert.get("active"):
            continue
        until_raw = str(alert.get("cooldown_until") or "")
        if until_raw:
            try:
                until = datetime.fromisoformat(until_raw.replace("Z", "+00:00"))
                if until <= now:
                    continue
            except Exception:
                pass
        side = str(alert.get("side") or "book")
        pct = str(alert.get("consumed_pct") or "")
        reason = str(alert.get("reason") or "liquidity_alert")
        out[str(market_id)] = f"liquidity sentinel {reason} side={side} consumed_pct={pct}"
    return out


def _book_from_ws_state(ws_state: dict[str, Any], market_id: int) -> dict[str, Any]:
    books = ws_state.get("orderbooks") if isinstance(ws_state.get("orderbooks"), dict) else {}
    book = books.get(str(market_id))
    if not isinstance(book, dict):
        return {}
    return {"data": book, "_source": "ws"}


def _load_inventory_positions(config_path: Path, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    out_cfg = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    raw = str(out_cfg.get("simulation_state_path") or "").strip()
    if not raw:
        return []
    path = (config_path.parent / raw).resolve()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    positions = data.get("positions") if isinstance(data, dict) else []
    return positions if isinstance(positions, list) else []


if __name__ == "__main__":
    main()
