from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import time

import websockets

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from platforms.predictfun.client import PredictFunClient, PREDICT_TESTNET_BASE
from platforms.predictfun.scanner import scan_markets
from platforms.predictfun.maker.liquidity_sentinel import LiquiditySentinel


DEFAULT_WS_URL = "wss://ws.predict.fun/ws"


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
    sentinel_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "ts": _utc_now(),
        "ws_url": ws_url,
        "market_ids": market_ids,
        "connected": False,
        "last_connected": False,
        "completed": False,
        "error": "",
        "messages": [],
        "orderbooks": {},
        "orderbook_updated_at": {},
        "liquidity": {},
        "liquidity_alerts": {},
    }
    _write_state(state_path, state)

    received = 0
    request_id = 1
    sentinel = LiquiditySentinel.from_config(sentinel_config or {})
    try:
        async with await _connect(ws_url, api_key) as ws:
            state["connected"] = True
            state["last_connected"] = True
            state["ts"] = _utc_now()
            _write_state(state_path, state)

            for market_id in market_ids:
                topic = f"predictOrderbook/{market_id}"
                await ws.send(json.dumps({"method": "subscribe", "requestId": request_id, "params": [topic]}))
                request_id += 1

            while True:
                if timeout_sec > 0:
                    raw = await asyncio.wait_for(ws.recv(), timeout=timeout_sec)
                else:
                    raw = await ws.recv()
                msg = json.loads(raw)
                received += 1

                topic = str(msg.get("topic") or "")
                if topic == "heartbeat":
                    await ws.send(json.dumps({"method": "heartbeat", "data": msg.get("data")}))
                elif topic.startswith("predictOrderbook/") and isinstance(msg.get("data"), dict):
                    market_id = topic.rsplit("/", 1)[-1]
                    state["orderbooks"][market_id] = msg["data"]
                    state["orderbook_updated_at"][market_id] = _utc_now()
                    now = time.time()
                    sentinel.record(market_id, msg["data"], ts=now)
                    state["liquidity"] = sentinel.metrics_json(now=now)
                    state["liquidity_alerts"] = sentinel.alerts_json(now=now)

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
                _write_state(state_path, state)

                if max_messages > 0 and received >= max_messages:
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
        _write_state(state_path, state)
    return state


def discover_market_ids(client: PredictFunClient, cfg: dict[str, Any], limit: int) -> list[int]:
    scan_cfg = cfg.get("scan") if isinstance(cfg.get("scan"), dict) else {}
    strategy_cfg = cfg.get("strategy") if isinstance(cfg.get("strategy"), dict) else {}
    markets = scan_markets(
        client,
        max_markets=limit,
        first=int(scan_cfg.get("first") or 50),
        has_active_rewards=bool(scan_cfg.get("has_active_rewards", True)),
        include_crypto_updown=bool(scan_cfg.get("include_crypto_updown", False)),
        scoring_profile=str(strategy_cfg.get("profile") or "conservative"),
    )
    return [m.id for m in markets[:limit]]


def main() -> None:
    default_config = Path(__file__).parent / "maker" / "config.testnet.json"
    parser = argparse.ArgumentParser(description="Predict.fun WebSocket orderbook watcher.")
    parser.add_argument("--config", default=str(default_config))
    parser.add_argument("--market-id", action="append", type=int, default=[])
    parser.add_argument("--discover", type=int, default=3, help="Discover this many reward markets when --market-id is omitted.")
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--timeout-sec", type=float, default=12.0)
    args = parser.parse_args()

    config_path = Path(args.config).resolve()
    cfg = _load_config(config_path)
    api_key = os.getenv(str(cfg.get("api_key_env") or "PREDICTFUN_API_KEY"), "")
    client = PredictFunClient(base_url=str(cfg.get("base_url") or PREDICT_TESTNET_BASE), api_key=api_key)
    market_ids = args.market_id or discover_market_ids(client, cfg, args.discover)
    state = asyncio.run(
        watch_orderbooks(
            ws_url=str(cfg.get("ws_url") or DEFAULT_WS_URL),
            api_key=api_key,
            market_ids=market_ids,
            state_path=_state_path(config_path, cfg),
            max_messages=args.max_messages,
            timeout_sec=args.timeout_sec,
            sentinel_config=(cfg.get("liquidity_sentinel") if isinstance(cfg.get("liquidity_sentinel"), dict) else {}),
        )
    )
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    main()
