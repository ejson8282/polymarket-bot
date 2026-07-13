"""Public Polymarket book observer and reference quote planner.

This module deliberately has no signer, authenticated client, order, or cancel
dependency. It reads the public CLOB order book and writes sanitized JSON for
the Python/Rust shadow comparison and the Latitude console.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_CLOB_URL = "https://clob.polymarket.com"
DEFAULT_TIMEOUT_SECONDS = 8.0
MAX_LEVELS = 5


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def normalize_book(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a validated, sorted public top-of-book snapshot."""

    bids = _levels(payload.get("bids"), reverse=True)
    asks = _levels(payload.get("asks"), reverse=False)
    if not bids or not asks:
        raise ValueError("public book is empty")
    best_bid = bids[0]["price"]
    best_ask = asks[0]["price"]
    if best_bid <= 0 or best_ask >= 1 or best_bid >= best_ask:
        raise ValueError("public book is crossed or outside the valid price range")
    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": (best_bid + best_ask) / Decimal("2"),
        "bids": bids[:MAX_LEVELS],
        "asks": asks[:MAX_LEVELS],
    }


def build_reference_plan(
    *,
    book: dict[str, Any],
    market: dict[str, Any],
    strategy: dict[str, Any],
) -> list[dict[str, str]]:
    """Build a safe, read-only quote reference from public inputs.

    The price grid mirrors the mechanical subset of the Python live planner.
    Sizing intentionally uses configured ``quote_size`` only; live balance,
    inventory, rewards metadata, and execution gates are not available here.
    """

    tick = _positive_decimal(
        market.get("price_tick"),
        strategy.get("default_price_tick"),
        default="0.01",
    )
    spread = _positive_decimal(
        market.get("max_incentive_spread"),
        strategy.get("default_max_incentive_spread"),
        default="0.02",
    )
    if spread > 1:
        spread /= Decimal("100")
    quantity = _positive_decimal(
        market.get("quote_size"),
        strategy.get("default_quote_size"),
        default="1",
    )
    distance_ticks = _positive_int(
        market.get("min_distance_ticks"),
        strategy.get("min_distance_ticks"),
        default=1,
    )
    best_bid = _decimal(book["best_bid"])
    mid = _decimal(book["mid"])
    reward_lower = max(tick, mid - spread)
    safe_top = best_bid - tick * Decimal(distance_ticks)

    if safe_top < reward_lower or safe_top < tick:
        if best_bid < reward_lower or best_bid < tick:
            return []
        prices = [_floor_to_tick(best_bid, tick)]
    elif tick < Decimal("0.01"):
        prices = _fine_tick_prices(
            tick=tick,
            reward_lower=reward_lower,
            safe_top=safe_top,
            strategy=strategy,
        )
    else:
        range_ticks = int((safe_top - reward_lower) / tick) + 1
        prices = []
        for level in range(min(max(range_ticks, 0), 3)):
            price = _floor_to_tick(safe_top - tick * Decimal(level), tick)
            if price >= reward_lower and price >= tick and price not in prices:
                prices.append(price)

    return [
        {"price": _decimal_text(price), "quantity": _decimal_text(quantity)}
        for price in prices
        if Decimal("0") < price < Decimal("1")
    ]


def observe_once(
    *,
    config_paths: Iterable[Path],
    output_dir: Path,
    fetch_book: Callable[[str, str, float], dict[str, Any]] | None = None,
    now: datetime | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Poll every configured public book once and persist sanitized state."""

    observed_at = (now or utc_now()).astimezone(timezone.utc)
    configs = [
        _load_public_config(path.resolve(), ordinal)
        for ordinal, path in enumerate(config_paths, start=1)
    ]
    if not configs:
        raise ValueError("at least one --config is required")
    indexes = [row["account_index"] for row in configs]
    if len(indexes) != len(set(indexes)):
        raise ValueError("observer account indexes must be unique")

    fetch = fetch_book or _fetch_public_book
    books: dict[str, dict[str, Any]] = {}
    fetch_errors: dict[str, str] = {}
    token_hosts = {
        market["token_id"]: config["rest_base_url"]
        for config in configs
        for market in config["markets"]
    }
    if token_hosts:
        with ThreadPoolExecutor(max_workers=min(8, len(token_hosts))) as executor:
            futures = {
                executor.submit(fetch, host, token_id, timeout_seconds): token_id
                for token_id, host in token_hosts.items()
            }
            for future in as_completed(futures):
                token_id = futures[future]
                try:
                    books[token_id] = normalize_book(future.result())
                except Exception as error:
                    fetch_errors[token_id] = _safe_error(error)

    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    account_status = []
    for config in configs:
        state, errors = _account_state(
            config=config,
            books=books,
            fetch_errors=fetch_errors,
            observed_at=observed_at,
        )
        path = output_dir / f"polymarket_observer_state_{config['account_index']}.json"
        changed, state_at = _write_state_if_changed(path, state, observed_at)
        account_status.append(
            {
                "account_index": config["account_index"],
                "state_file": path.name,
                "markets": len(config["markets"]),
                "ready_markets": len(state["markets"]),
                "plans": sum(
                    1 for row in state["markets"].values() if row.get("desired_plan_sig")
                ),
                "errors": errors,
                "state_updated": changed,
                "last_state_at": state_at,
            }
        )

    status = {
        "schema_version": 1,
        "mode": "read_only",
        "source": "public_clob",
        "last_poll_at": _iso(observed_at),
        "healthy": all(not row["errors"] for row in account_status),
        "accounts": account_status,
        "summary": {
            "accounts": len(account_status),
            "markets": sum(row["markets"] for row in account_status),
            "ready_markets": sum(row["ready_markets"] for row in account_status),
            "plans": sum(row["plans"] for row in account_status),
            "errors": sum(len(row["errors"]) for row in account_status),
        },
    }
    _write_json(output_dir / "polymarket_observer_status.json", status)
    return status


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Observe public Polymarket books and reference plans. "
            "Never signs, authenticates, sends, or cancels orders."
        )
    )
    parser.add_argument("--config", type=Path, action="append", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--interval-seconds", type=float, default=15.0)
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--run-seconds", type=float, default=0.0)
    args = parser.parse_args()
    if args.interval_seconds < 5:
        parser.error("--interval-seconds must be at least 5")
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")

    started = time.monotonic()
    while True:
        try:
            status = observe_once(
                config_paths=args.config,
                output_dir=args.output_dir,
                timeout_seconds=args.timeout_seconds,
            )
            summary = status["summary"]
            print(
                "observer poll "
                f"accounts={summary['accounts']} markets={summary['markets']} "
                f"ready={summary['ready_markets']} plans={summary['plans']} "
                f"errors={summary['errors']}",
                flush=True,
            )
        except Exception as error:
            print(f"observer error: {_safe_error(error)}", flush=True)
            if args.once:
                return 1
        if args.once:
            return 0 if status["summary"]["ready_markets"] else 1
        if args.run_seconds > 0 and time.monotonic() - started >= args.run_seconds:
            return 0
        try:
            time.sleep(args.interval_seconds)
        except KeyboardInterrupt:
            return 0


def _load_public_config(path: Path, ordinal: int) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"expected a JSON object: {path.name}")
    strategy = raw.get("strategy") if isinstance(raw.get("strategy"), dict) else {}
    account_index = _account_index(path, ordinal)
    markets = []
    raw_markets = raw.get("markets")
    if isinstance(raw_markets, dict):
        market_rows = []
        for key, value in raw_markets.items():
            if isinstance(value, dict):
                market_rows.append({**value, "token_id": str(value.get("token_id") or key)})
    elif isinstance(raw_markets, list):
        market_rows = [row for row in raw_markets if isinstance(row, dict)]
    else:
        market_rows = []
    for row in market_rows:
        token_id = str(row.get("token_id") or "").strip()
        if not token_id or row.get("enabled") is False:
            continue
        markets.append(
            {
                key: row[key]
                for key in (
                    "condition_id",
                    "display_name",
                    "label",
                    "market_slug",
                    "max_incentive_spread",
                    "min_distance_ticks",
                    "price_tick",
                    "quote_size",
                    "token_id",
                )
                if key in row
            }
        )
        markets[-1]["token_id"] = token_id
    return {
        "account_index": account_index,
        "rest_base_url": str(raw.get("rest_base_url") or DEFAULT_CLOB_URL).rstrip("/"),
        "strategy": {
            key: strategy[key]
            for key in (
                "default_max_incentive_spread",
                "default_price_tick",
                "default_quote_size",
                "fine_tick_max_legs",
                "fine_tick_zone_use_pct",
                "min_distance_ticks",
            )
            if key in strategy
        },
        "markets": markets,
    }


def _account_state(
    *,
    config: dict[str, Any],
    books: dict[str, dict[str, Any]],
    fetch_errors: dict[str, str],
    observed_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    markets: dict[str, dict[str, Any]] = {}
    errors: list[dict[str, str]] = []
    for market in config["markets"]:
        token_id = market["token_id"]
        book = books.get(token_id)
        if book is None:
            errors.append(
                {"token": _short_token(token_id), "error": fetch_errors.get(token_id, "book unavailable")}
            )
            continue
        plan = build_reference_plan(
            book=book,
            market=market,
            strategy=config["strategy"],
        )
        markets[token_id] = {
            "condition_id": str(market.get("condition_id") or token_id),
            "display_name": _display_name(market, token_id),
            "best_bid": _decimal_text(book["best_bid"]),
            "best_ask": _decimal_text(book["best_ask"]),
            "mid": _decimal_text(book["mid"]),
            "bids": [_public_level(row) for row in book["bids"]],
            "asks": [_public_level(row) for row in book["asks"]],
            "snapshot_age_ms": 0,
            "desired_plan_sig": "|".join(
                f"{row['price']}:{row['quantity']}" for row in plan
            ),
            "reference_plan": plan,
            "plan_kind": "reference_only",
            "orders": [],
            "actual_orders_available": False,
            "status": "ready" if plan else "no_reference_plan",
        }
    state = {
        "schema_version": 1,
        "mode": "read_only",
        "source": "public_clob",
        "plan_kind": "reference_only",
        "actual_orders_available": False,
        "account_index": config["account_index"],
        "account_id": f"pm-account-{config['account_index']}",
        "markets": markets,
        "summary": {
            "configured_markets": len(config["markets"]),
            "ready_markets": len(markets),
            "reference_plans": sum(1 for row in markets.values() if row["reference_plan"]),
            "errors": len(errors),
        },
    }
    state["content_fingerprint"] = _content_fingerprint(state)
    return state, errors


def _write_state_if_changed(
    path: Path,
    state: dict[str, Any],
    observed_at: datetime,
) -> tuple[bool, str]:
    previous = _read_json(path)
    fingerprint = state["content_fingerprint"]
    if previous and previous.get("content_fingerprint") == fingerprint:
        return False, str(previous.get("ts") or "")
    state = {"ts": _iso(observed_at), **state}
    _write_json(path, state)
    return True, state["ts"]


def _fetch_public_book(host: str, token_id: str, timeout: float) -> dict[str, Any]:
    request = Request(
        f"{host.rstrip('/')}/book?{urlencode({'token_id': token_id})}",
        headers={"User-Agent": "latitude-alpha-read-only-observer/1"},
    )
    with urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("public book response is not an object")
    return payload


def _levels(value: Any, *, reverse: bool) -> list[dict[str, Decimal]]:
    rows = []
    for level in value if isinstance(value, list) else []:
        if not isinstance(level, dict):
            continue
        try:
            price = _decimal(level.get("price"))
            size = _decimal(level.get("size"))
        except (InvalidOperation, ValueError):
            continue
        if price > 0 and size > 0:
            rows.append({"price": price, "size": size})
    rows.sort(key=lambda row: row["price"], reverse=reverse)
    return rows


def _fine_tick_prices(
    *,
    tick: Decimal,
    reward_lower: Decimal,
    safe_top: Decimal,
    strategy: dict[str, Any],
) -> list[Decimal]:
    max_legs = min(10, _positive_int(strategy.get("fine_tick_max_legs"), default=5))
    zone_use = _positive_decimal(strategy.get("fine_tick_zone_use_pct"), default="0.50")
    zone_use = max(Decimal("0.10"), min(zone_use, Decimal("0.80")))
    width = safe_top - reward_lower
    top = _floor_to_tick(safe_top, tick)
    bottom = _floor_to_tick(max(reward_lower, safe_top - width * zone_use), tick)
    if top < reward_lower or top < tick:
        return []
    if max_legs <= 1 or top <= bottom:
        return [top]
    step = max((top - bottom) / Decimal(max_legs - 1), tick)
    prices = []
    for level in range(max_legs):
        price = _floor_to_tick(top - step * Decimal(level), tick)
        if price >= reward_lower and price >= tick and price not in prices:
            prices.append(price)
    return prices


def _positive_decimal(*values: Any, default: str) -> Decimal:
    for value in values:
        try:
            result = _decimal(value)
        except (InvalidOperation, ValueError):
            continue
        if result > 0:
            return result
    return Decimal(default)


def _positive_int(*values: Any, default: int) -> int:
    for value in values:
        try:
            result = int(value)
        except (TypeError, ValueError):
            continue
        if result > 0:
            return result
    return default


def _decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    result = Decimal(str(value))
    if not result.is_finite():
        raise ValueError("non-finite decimal")
    return result


def _floor_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _decimal_text(value: Any) -> str:
    text = format(_decimal(value), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _public_level(row: dict[str, Decimal]) -> dict[str, str]:
    return {"price": _decimal_text(row["price"]), "size": _decimal_text(row["size"])}


def _display_name(market: dict[str, Any], token_id: str) -> str:
    for key in ("display_name", "label", "market_slug"):
        value = str(market.get(key) or "").strip()
        if value:
            return value[:80]
    return _short_token(token_id)


def _short_token(token_id: str) -> str:
    return token_id if len(token_id) <= 16 else f"{token_id[:8]}...{token_id[-6:]}"


def _account_index(path: Path, ordinal: int) -> int:
    stem = path.stem
    if stem.startswith("config_"):
        try:
            return int(stem.split("_", 1)[1])
        except ValueError:
            pass
    return ordinal


def _content_fingerprint(state: dict[str, Any]) -> str:
    payload = json.dumps(state, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _safe_error(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}".replace("\n", " ")[:240]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except Exception:
        return None


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
