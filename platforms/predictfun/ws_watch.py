from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
import time

import websockets

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from platforms.predictfun.client import PredictFunClient, PREDICT_TESTNET_BASE
from platforms.predictfun.scanner import PredictMarket, scan_markets
from platforms.predictfun.maker.liquidity_sentinel import LiquiditySentinel


DEFAULT_WS_URL = "wss://ws.predict.fun/ws"
TRADING_STATUSES = frozenset({"OPEN", "MATCHING_NOT_ENABLED", "CANCEL_ONLY", "CLOSED"})
MARKET_STATUSES = frozenset({"OPEN", "REGISTERED", "RESOLVED", "CLOSED", "CANCELLED"})
STATE_WRITE_INTERVAL_SEC = 0.25


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _state_path(config_path: Path, cfg: dict[str, Any]) -> Path:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    raw = str(out.get("ws_state_path") or "../../../data/predictfun_ws_state.json")
    return (config_path.parent / raw).resolve()


def _write_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(path)


def _write_state_if_due(
    path: Path,
    state: dict[str, Any],
    last_written_monotonic: float,
    *,
    force: bool = False,
    now_monotonic: float | None = None,
) -> float:
    now = time.monotonic() if now_monotonic is None else now_monotonic
    if force or now - last_written_monotonic >= STATE_WRITE_INTERVAL_SEC:
        _write_state(path, state)
        return now
    return last_written_monotonic


async def _connect(ws_url: str, api_key: str):
    headers = {"x-api-key": api_key} if api_key else None
    try:
        return await websockets.connect(ws_url, additional_headers=headers)
    except TypeError:
        return await websockets.connect(ws_url, extra_headers=headers)


async def watch_orderbooks(
    *,
    ws_url: str,
    api_key: str,
    market_ids: list[int],
    state_path: Path,
    max_messages: int = 0,
    timeout_sec: float = 0,
    max_runtime_sec: float = 0,
    sentinel_config: dict[str, Any] | None = None,
    initial_market_statuses: dict[str, dict[str, Any]] | None = None,
    session_number: int = 1,
    reconnect_count: int = 0,
    market_refresher: Callable[[], Awaitable[list[PredictMarket]]] | None = None,
    refresh_sec: float = 0,
) -> dict[str, Any]:
    allowed_market_ids = {str(value) for value in market_ids}
    market_statuses = {
        str(market_id): dict(row)
        for market_id, row in (initial_market_statuses or {}).items()
        if str(market_id) in allowed_market_ids
        and isinstance(row, dict)
        and str(row.get("status") or "").upper() in MARKET_STATUSES
    }
    state: dict[str, Any] = {
        "schema_version": 2,
        "ts": _utc_now(),
        "ws_url": ws_url,
        "market_ids": market_ids,
        "connected": False,
        "last_connected": False,
        "session_number": session_number,
        "reconnect_count": reconnect_count,
        "refresh_count": 0,
        "discovery_failure_count": 0,
        "consecutive_discovery_failures": 0,
        "discovery_error": "",
        "last_discovery_at": _utc_now(),
        "completed": False,
        "error": "",
        "messages": [],
        "orderbooks": {},
        "orderbook_updated_at": {},
        "orderbook_upstream_updated_at_ms": {},
        "orderbook_latency_ms": {},
        "orderbook_errors": {},
        "trading_statuses": {},
        "market_statuses": market_statuses,
        "liquidity": {},
        "liquidity_alerts": {},
    }
    _write_state(state_path, state)
    last_state_write_monotonic = time.monotonic()

    received = 0
    request_id = 1
    started_monotonic = time.monotonic()
    sentinel = LiquiditySentinel.from_config(sentinel_config or {})
    try:
        async with await _connect(ws_url, api_key) as ws:
            state["connected"] = True
            state["last_connected"] = True
            state["ts"] = _utc_now()
            last_state_write_monotonic = _write_state_if_due(
                state_path,
                state,
                last_state_write_monotonic,
                force=True,
            )

            topics = _market_topics(market_ids)
            state["subscribed_topics"] = topics
            for topic in topics:
                await ws.send(json.dumps({"method": "subscribe", "requestId": request_id, "params": [topic]}))
                request_id += 1

            active_market_ids = {str(value) for value in market_ids}
            last_message_monotonic = time.monotonic()
            next_refresh_monotonic = (
                last_message_monotonic + refresh_sec
                if market_refresher is not None and refresh_sec > 0
                else 0.0
            )
            while True:
                # Refresh subscriptions on the live socket without masking real message silence.
                now_monotonic = time.monotonic()
                receive_timeout: float | None = None
                if timeout_sec > 0:
                    receive_timeout = max(
                        0.01,
                        last_message_monotonic + timeout_sec - now_monotonic,
                    )
                if next_refresh_monotonic > 0:
                    until_refresh = max(0.01, next_refresh_monotonic - now_monotonic)
                    receive_timeout = (
                        until_refresh
                        if receive_timeout is None
                        else min(receive_timeout, until_refresh)
                    )
                try:
                    if receive_timeout is None:
                        raw = await ws.recv()
                    else:
                        raw = await asyncio.wait_for(ws.recv(), timeout=receive_timeout)
                except asyncio.TimeoutError:
                    now_monotonic = time.monotonic()
                    if (
                        timeout_sec > 0
                        and now_monotonic - last_message_monotonic >= timeout_sec
                    ):
                        raise
                    if (
                        market_refresher is None
                        or next_refresh_monotonic <= 0
                        or now_monotonic < next_refresh_monotonic
                    ):
                        raise
                    try:
                        refreshed_markets = await market_refresher()
                        refreshed_market_ids = [market.id for market in refreshed_markets]
                        if not refreshed_market_ids:
                            raise RuntimeError("no eligible Predict.fun markets discovered")
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        state["discovery_failure_count"] += 1
                        state["consecutive_discovery_failures"] += 1
                        state["discovery_error"] = f"{exc.__class__.__name__}: {exc}"
                        state["ts"] = _utc_now()
                        retry_sec = min(
                            refresh_sec,
                            max(
                                5.0,
                                float(2 ** min(state["consecutive_discovery_failures"], 5)),
                            ),
                        )
                        next_refresh_monotonic = time.monotonic() + retry_sec
                        last_state_write_monotonic = _write_state_if_due(
                            state_path,
                            state,
                            last_state_write_monotonic,
                            force=True,
                        )
                        continue

                    request_id = await _sync_market_subscriptions(
                        ws,
                        state,
                        market_ids=refreshed_market_ids,
                        market_statuses=_market_status_snapshot(refreshed_markets),
                        request_id=request_id,
                    )
                    active_market_ids = {str(value) for value in refreshed_market_ids}
                    state["refresh_count"] += 1
                    state["consecutive_discovery_failures"] = 0
                    state["discovery_error"] = ""
                    state["last_discovery_at"] = _utc_now()
                    state["ts"] = state["last_discovery_at"]
                    next_refresh_monotonic = time.monotonic() + refresh_sec
                    last_state_write_monotonic = _write_state_if_due(
                        state_path,
                        state,
                        last_state_write_monotonic,
                        force=True,
                    )
                    continue

                last_message_monotonic = time.monotonic()
                msg = json.loads(raw)
                received += 1

                topic = str(msg.get("topic") or "")
                if topic == "heartbeat":
                    await ws.send(json.dumps({"method": "heartbeat", "data": msg.get("data")}))
                    state["last_heartbeat_at"] = _utc_now()
                elif topic:
                    message_market_id = topic.rsplit("/", 1)[-1] if "/" in topic else ""
                    if not message_market_id or message_market_id in active_market_ids:
                        try:
                            _apply_market_message(state, msg, sentinel=sentinel, now=time.time())
                            _prune_market_state(state, active_market_ids)
                        except ValueError as exc:
                            state["orderbook_errors"][message_market_id] = str(exc)

                compact = {
                    "ts": _utc_now(),
                    "type": msg.get("type"),
                    "topic": topic,
                    "requestId": msg.get("requestId"),
                    "success": msg.get("success"),
                }
                state["messages"].append(compact)
                state["messages"] = state["messages"][-100:]
                state["ts"] = _utc_now()
                state["last_message_at"] = state["ts"]
                last_state_write_monotonic = _write_state_if_due(
                    state_path,
                    state,
                    last_state_write_monotonic,
                )

                if max_messages > 0 and received >= max_messages:
                    break
                if max_runtime_sec > 0 and time.monotonic() - started_monotonic >= max_runtime_sec:
                    state["note"] = "subscription refresh"
                    break
            state["completed"] = True
    except Exception as exc:
        if isinstance(exc, asyncio.TimeoutError) and received > 0:
            state["completed"] = True
            state["error"] = ""
            state["note"] = "no messages before timeout after successful subscription"
        else:
            state["error"] = f"{exc.__class__.__name__}: {exc}"
        state["connected"] = False
        state["ts"] = _utc_now()
        _write_state_if_due(
            state_path,
            state,
            last_state_write_monotonic,
            force=True,
        )
    else:
        state["connected"] = False
        state["ts"] = _utc_now()
        _write_state_if_due(
            state_path,
            state,
            last_state_write_monotonic,
            force=True,
        )
    return state


async def watch_orderbooks_forever(
    *,
    client: PredictFunClient,
    cfg: dict[str, Any],
    ws_url: str,
    api_key: str,
    state_path: Path,
    discover_limit: int,
    refresh_sec: float,
) -> None:
    session_number = 0
    reconnect_count = 0
    consecutive_failures = 0

    async def refresh_markets() -> list[PredictMarket]:
        return await asyncio.to_thread(
            discover_markets,
            client,
            cfg,
            discover_limit,
        )

    while True:
        session_number += 1
        try:
            markets = await refresh_markets()
            market_ids = [market.id for market in markets]
            if not market_ids:
                raise RuntimeError("no eligible Predict.fun markets discovered")
            state = await watch_orderbooks(
                ws_url=ws_url,
                api_key=api_key,
                market_ids=market_ids,
                state_path=state_path,
                max_messages=0,
                timeout_sec=max(30.0, refresh_sec + 30.0),
                max_runtime_sec=0,
                initial_market_statuses=_market_status_snapshot(markets),
                sentinel_config=(
                    cfg.get("liquidity_sentinel")
                    if isinstance(cfg.get("liquidity_sentinel"), dict)
                    else {}
                ),
                session_number=session_number,
                reconnect_count=reconnect_count,
                market_refresher=refresh_markets,
                refresh_sec=max(30.0, refresh_sec),
            )
            if state.get("error"):
                consecutive_failures += 1
                reconnect_count += 1
            else:
                consecutive_failures = 0
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            consecutive_failures += 1
            reconnect_count += 1
            _write_state(
                state_path,
                {
                    "schema_version": 2,
                    "ts": _utc_now(),
                    "ws_url": ws_url,
                    "connected": False,
                    "completed": False,
                    "session_number": session_number,
                    "reconnect_count": reconnect_count,
                    "error": f"{exc.__class__.__name__}: {exc}",
                    "market_ids": [],
                    "orderbooks": {},
                    "orderbook_updated_at": {},
                    "trading_statuses": {},
                    "market_statuses": {},
                },
            )
        if consecutive_failures:
            backoff = min(30.0, float(2 ** min(consecutive_failures - 1, 5)))
            await asyncio.sleep(backoff + random.random())
        else:
            await asyncio.sleep(0.25)


def discover_markets(
    client: PredictFunClient,
    cfg: dict[str, Any],
    limit: int,
) -> list[PredictMarket]:
    scan_cfg = cfg.get("scan") if isinstance(cfg.get("scan"), dict) else {}
    strategy_cfg = cfg.get("strategy") if isinstance(cfg.get("strategy"), dict) else {}
    return scan_markets(
        client,
        max_markets=limit,
        first=int(scan_cfg.get("first") or 50),
        has_active_rewards=bool(scan_cfg.get("has_active_rewards", True)),
        include_crypto_updown=bool(scan_cfg.get("include_crypto_updown", False)),
        scoring_profile=str(strategy_cfg.get("profile") or "conservative"),
        status_filter=str(scan_cfg.get("status_filter") or "OPEN"),
    )


def discover_market_ids(
    client: PredictFunClient,
    cfg: dict[str, Any],
    limit: int,
) -> list[int]:
    return [market.id for market in discover_markets(client, cfg, limit)[:limit]]


def _market_status_snapshot(
    markets: list[PredictMarket],
) -> dict[str, dict[str, Any]]:
    updated_at = _utc_now()
    return {
        str(market.id): {
            "status": market.status,
            "updated_at": updated_at,
            "upstream_ts_ms": 0,
            "source": "rest_discovery",
        }
        for market in markets
        if market.id > 0 and market.status in MARKET_STATUSES
    }


def _market_topics(market_ids: list[int]) -> list[str]:
    topics: list[str] = []
    for market_id in market_ids:
        topics.extend(
            (
                f"predictOrderbook/{market_id}",
                f"predictTradingStatus/{market_id}",
                f"predictMarketStatus/{market_id}",
            )
        )
    return topics


MARKET_STATE_FIELDS = (
    "orderbooks",
    "orderbook_updated_at",
    "orderbook_upstream_updated_at_ms",
    "orderbook_latency_ms",
    "orderbook_errors",
    "trading_statuses",
    "market_statuses",
    "liquidity",
    "liquidity_alerts",
)


def _prune_market_state(state: dict[str, Any], market_ids: set[str]) -> None:
    for field in MARKET_STATE_FIELDS:
        values = state.get(field)
        if not isinstance(values, dict):
            state[field] = {}
            continue
        state[field] = {
            str(market_id): value
            for market_id, value in values.items()
            if str(market_id) in market_ids
        }


async def _sync_market_subscriptions(
    websocket: Any,
    state: dict[str, Any],
    *,
    market_ids: list[int],
    market_statuses: dict[str, dict[str, Any]],
    request_id: int,
) -> int:
    normalized_ids = list(dict.fromkeys(int(value) for value in market_ids if int(value) > 0))
    old_ids = {int(value) for value in state.get("market_ids", []) if int(value) > 0}
    new_ids = set(normalized_ids)
    removed_ids = sorted(old_ids - new_ids)
    added_ids = sorted(new_ids - old_ids)

    for topic in _market_topics(removed_ids):
        await websocket.send(
            json.dumps(
                {"method": "unsubscribe", "requestId": request_id, "params": [topic]}
            )
        )
        request_id += 1
    for topic in _market_topics(added_ids):
        await websocket.send(
            json.dumps(
                {"method": "subscribe", "requestId": request_id, "params": [topic]}
            )
        )
        request_id += 1

    state["market_ids"] = normalized_ids
    state["subscribed_topics"] = _market_topics(normalized_ids)
    active_ids = {str(value) for value in normalized_ids}
    _prune_market_state(state, active_ids)
    current_statuses = state["market_statuses"]
    for market_id, row in market_statuses.items():
        market_key = str(market_id)
        if market_key not in active_ids or not isinstance(row, dict):
            continue
        current_statuses.setdefault(market_key, dict(row))
    state["last_subscription_refresh"] = {
        "at": _utc_now(),
        "added_market_ids": added_ids,
        "removed_market_ids": removed_ids,
    }
    return request_id


def _topic_market_id(topic: str, expected_prefix: str) -> int:
    prefix = f"{expected_prefix}/"
    if not topic.startswith(prefix):
        raise ValueError("unexpected_topic")
    try:
        market_id = int(topic[len(prefix):])
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_topic_market_id") from exc
    if market_id <= 0:
        raise ValueError("invalid_topic_market_id")
    return market_id


def _message_market_id(data: dict[str, Any], topic_market_id: int) -> int:
    try:
        market_id = int(data.get("marketId"))
    except (TypeError, ValueError) as exc:
        raise ValueError("payload_market_id_missing") from exc
    if market_id != topic_market_id:
        raise ValueError("payload_market_id_mismatch")
    return market_id


def _normalize_levels(raw: Any, *, descending: bool) -> list[list[str]]:
    if not isinstance(raw, list):
        raise ValueError("orderbook_levels_invalid")
    levels: list[tuple[Decimal, Decimal]] = []
    for row in raw:
        if not isinstance(row, list) or len(row) < 2:
            raise ValueError("orderbook_level_invalid")
        try:
            price = Decimal(str(row[0]))
            quantity = Decimal(str(row[1]))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("orderbook_level_invalid") from exc
        if (
            not price.is_finite()
            or not quantity.is_finite()
            or not Decimal("0") < price < Decimal("1")
            or quantity < Decimal("0")
        ):
            raise ValueError("orderbook_level_out_of_range")
        if quantity == Decimal("0"):
            continue
        levels.append((price, quantity))
    levels.sort(key=lambda item: item[0], reverse=descending)
    return [[format(price, "f"), format(quantity, "f")] for price, quantity in levels]


def normalize_orderbook_payload(topic: str, data: dict[str, Any]) -> dict[str, Any]:
    topic_market_id = _topic_market_id(topic, "predictOrderbook")
    market_id = _message_market_id(data, topic_market_id)
    try:
        updated_at_ms = int(data.get("updateTimestampMs"))
    except (TypeError, ValueError) as exc:
        raise ValueError("orderbook_timestamp_missing") from exc
    if updated_at_ms <= 0:
        raise ValueError("orderbook_timestamp_invalid")
    bids = _normalize_levels(data.get("bids"), descending=True)
    asks = _normalize_levels(data.get("asks"), descending=False)
    if bids and asks and Decimal(bids[0][0]) >= Decimal(asks[0][0]):
        raise ValueError("orderbook_crossed")
    return {
        "version": int(data.get("version") or 0),
        "marketId": market_id,
        "updateTimestampMs": updated_at_ms,
        "orderCount": int(data.get("orderCount") or 0),
        "bids": bids,
        "asks": asks,
    }


def _apply_market_message(
    state: dict[str, Any],
    msg: dict[str, Any],
    *,
    sentinel: LiquiditySentinel,
    now: float,
) -> None:
    topic = str(msg.get("topic") or "")
    data = msg.get("data") if isinstance(msg.get("data"), dict) else {}
    if topic.startswith("predictOrderbook/"):
        normalized = normalize_orderbook_payload(topic, data)
        market_id = str(normalized["marketId"])
        upstream_ms = int(normalized["updateTimestampMs"])
        previous_ms = int(state["orderbook_upstream_updated_at_ms"].get(market_id) or 0)
        if previous_ms and upstream_ms < previous_ms:
            raise ValueError("orderbook_update_out_of_order")
        state["orderbooks"][market_id] = normalized
        state["orderbook_updated_at"][market_id] = _utc_now()
        state["orderbook_upstream_updated_at_ms"][market_id] = upstream_ms
        state["orderbook_latency_ms"][market_id] = max(0, int(now * 1000) - upstream_ms)
        state["orderbook_errors"].pop(market_id, None)
        sentinel.record(market_id, normalized, ts=now)
        state["liquidity"] = sentinel.metrics_json(now=now)
        state["liquidity_alerts"] = sentinel.alerts_json(now=now)
        return
    if topic.startswith("predictTradingStatus/"):
        topic_market_id = _topic_market_id(topic, "predictTradingStatus")
        market_id = _message_market_id(data, topic_market_id)
        status = str(data.get("tradingStatus") or "").upper()
        if status not in TRADING_STATUSES:
            raise ValueError("trading_status_invalid")
        state["trading_statuses"][str(market_id)] = {
            "status": status,
            "updated_at": _utc_now(),
            "upstream_ts_ms": int(data.get("tsMs") or 0),
        }
        return
    if topic.startswith("predictMarketStatus/"):
        topic_market_id = _topic_market_id(topic, "predictMarketStatus")
        market_id = _message_market_id(data, topic_market_id)
        status = str(data.get("status") or "").upper()
        if status not in MARKET_STATUSES:
            raise ValueError("market_status_invalid")
        state["market_statuses"][str(market_id)] = {
            "status": status,
            "updated_at": _utc_now(),
            "upstream_ts_ms": int(data.get("tsMs") or 0),
        }


def main() -> None:
    default_config = Path(__file__).parent / "maker" / "config.testnet.json"
    parser = argparse.ArgumentParser(description="Predict.fun WebSocket orderbook watcher.")
    parser.add_argument("--config", default=str(default_config))
    parser.add_argument("--market-id", action="append", type=int, default=[])
    parser.add_argument("--discover", type=int, default=3, help="Discover this many reward markets when --market-id is omitted.")
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--timeout-sec", type=float, default=12.0)
    parser.add_argument("--forever", action="store_true")
    parser.add_argument("--refresh-sec", type=float, default=300.0)
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = _load_config(config_path)
    api_key = os.getenv(str(cfg.get("api_key_env") or "PREDICTFUN_API_KEY"), "")
    client = PredictFunClient(base_url=str(cfg.get("base_url") or PREDICT_TESTNET_BASE), api_key=api_key)
    if args.forever:
        asyncio.run(
            watch_orderbooks_forever(
                client=client,
                cfg=cfg,
                ws_url=str(cfg.get("ws_url") or DEFAULT_WS_URL),
                api_key=api_key,
                state_path=_state_path(config_path, cfg),
                discover_limit=max(1, args.discover),
                refresh_sec=max(30.0, args.refresh_sec),
            )
        )
        return
    initial_market_statuses: dict[str, dict[str, Any]] = {}
    if args.market_id:
        market_ids = args.market_id
    else:
        markets = discover_markets(client, cfg, args.discover)
        market_ids = [market.id for market in markets]
        initial_market_statuses = _market_status_snapshot(markets)
    state = asyncio.run(
        watch_orderbooks(
            ws_url=str(cfg.get("ws_url") or DEFAULT_WS_URL),
            api_key=api_key,
            market_ids=market_ids,
            state_path=_state_path(config_path, cfg),
            max_messages=args.max_messages,
            timeout_sec=args.timeout_sec,
            initial_market_statuses=initial_market_statuses,
            sentinel_config=(cfg.get("liquidity_sentinel") if isinstance(cfg.get("liquidity_sentinel"), dict) else {}),
        )
    )
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
