from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Iterable

import websockets


DEFAULT_UPSTREAM_URL = "wss://ws.predict.fun/ws"
DEFAULT_SECRET_FILE = Path.home() / ".macmini-secrets" / "predictfun.env"
DEFAULT_ALLOWED_CLIENTS = frozenset(
    {
        "127.0.0.1",
        "::1",
        "100.91.159.54",
        "100.122.255.98",
        "100.101.50.40",
    }
)
ALLOWED_TOPIC = re.compile(
    r"^(?:predictOrderbook|predictTradingStatus|predictMarketStatus|predictMarketChanged)/[1-9][0-9]*$"
)
MAX_CLIENT_MESSAGE_BYTES = 64 * 1024
DEFAULT_MAX_CLIENTS = 3


class RelayProtocolError(ValueError):
    pass


def load_api_key(secret_file: Path) -> str:
    if not secret_file.is_file():
        raise RelayProtocolError("secret_file_missing")
    with secret_file.open("r", encoding="utf-8") as handle:
        for raw in handle:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() != "PREDICTFUN_API_KEY":
                continue
            value = value.strip()
            if (
                len(value) >= 2
                and value[0] == value[-1]
                and value[0] in {"'", '"'}
            ):
                value = value[1:-1]
            if value:
                return value
    raise RelayProtocolError("api_key_missing")


def validate_client_message(raw: str | bytes) -> dict[str, Any]:
    if isinstance(raw, bytes):
        if len(raw) > MAX_CLIENT_MESSAGE_BYTES:
            raise RelayProtocolError("message_too_large")
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RelayProtocolError("message_not_utf8") from exc
    elif len(raw.encode("utf-8")) > MAX_CLIENT_MESSAGE_BYTES:
        raise RelayProtocolError("message_too_large")

    try:
        message = json.loads(raw)
    except Exception as exc:
        raise RelayProtocolError("invalid_json") from exc
    if not isinstance(message, dict):
        raise RelayProtocolError("message_must_be_object")

    method = str(message.get("method") or "")
    if method == "heartbeat":
        if "data" not in message:
            raise RelayProtocolError("heartbeat_data_missing")
        return {"method": "heartbeat", "data": message.get("data")}
    if method not in {"subscribe", "unsubscribe"}:
        raise RelayProtocolError("method_not_allowed")

    request_id = message.get("requestId")
    if isinstance(request_id, bool) or not isinstance(request_id, int) or request_id < 0:
        raise RelayProtocolError("request_id_invalid")
    params = message.get("params")
    if not isinstance(params, list) or len(params) != 1 or not isinstance(params[0], str):
        raise RelayProtocolError("single_topic_required")
    topic = params[0]
    if not ALLOWED_TOPIC.fullmatch(topic):
        raise RelayProtocolError("topic_not_allowed")
    return {"method": method, "requestId": request_id, "params": [topic]}


def server_message_is_allowed(raw: str | bytes) -> bool:
    try:
        message = json.loads(raw)
    except Exception:
        return False
    if not isinstance(message, dict):
        return False
    topic = str(message.get("topic") or "")
    if not topic:
        return str(message.get("type") or "") == "R"
    return topic == "heartbeat" or bool(ALLOWED_TOPIC.fullmatch(topic))


async def connect_upstream(upstream_url: str, api_key: str):
    headers = {"x-api-key": api_key}
    try:
        return await websockets.connect(
            upstream_url,
            additional_headers=headers,
            proxy=None,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )
    except TypeError:
        return await websockets.connect(
            upstream_url,
            extra_headers=headers,
            proxy=None,
            ping_interval=20,
            ping_timeout=20,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )


async def probe_relay(
    ws_url: str,
    market_id: int,
    *,
    timeout_sec: float = 10.0,
) -> dict[str, Any]:
    if market_id <= 0:
        raise RelayProtocolError("probe_market_id_invalid")
    topic = f"predictOrderbook/{market_id}"
    request_id = 1
    deadline = time.monotonic() + max(1.0, timeout_sec)
    async with websockets.connect(
        ws_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=8 * 1024 * 1024,
    ) as websocket:
        await websocket.send(
            json.dumps(
                {
                    "method": "subscribe",
                    "requestId": request_id,
                    "params": [topic],
                },
                separators=(",", ":"),
            )
        )
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RelayProtocolError("probe_timeout")
            raw = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            try:
                message = json.loads(raw)
            except Exception as exc:
                raise RelayProtocolError("probe_invalid_json") from exc
            if not isinstance(message, dict):
                raise RelayProtocolError("probe_invalid_message")
            if str(message.get("topic") or "") == "heartbeat":
                await websocket.send(
                    json.dumps(
                        {"method": "heartbeat", "data": message.get("data")},
                        separators=(",", ":"),
                    )
                )
                continue
            if message.get("requestId") == request_id:
                if message.get("success") is not True:
                    raise RelayProtocolError("probe_subscription_rejected")
                return {
                    "ok": True,
                    "market_id": market_id,
                    "source": "subscription_response",
                }
            if str(message.get("topic") or "") == topic:
                return {
                    "ok": True,
                    "market_id": market_id,
                    "source": "market_message",
                }


def _remote_ip(websocket: Any) -> str:
    remote = getattr(websocket, "remote_address", None)
    if isinstance(remote, tuple) and remote:
        return str(remote[0])
    return ""


def client_admission_error(
    client_ip: str,
    *,
    allowed_clients: frozenset[str],
    active_clients: frozenset[str] | set[str],
    max_clients: int,
) -> str:
    if client_ip not in allowed_clients:
        return "client_not_allowed"
    if client_ip in active_clients:
        return "duplicate_client"
    if len(active_clients) >= max(1, max_clients):
        return "relay_capacity_reached"
    return ""


async def _send_protocol_error(websocket: Any, reason: str) -> None:
    await websocket.send(
        json.dumps(
            {
                "type": "R",
                "requestId": -1,
                "success": False,
                "error": {"code": reason},
            }
        )
    )


async def _client_to_upstream(client: Any, upstream: Any) -> None:
    async for raw in client:
        try:
            message = validate_client_message(raw)
        except RelayProtocolError as exc:
            await _send_protocol_error(client, str(exc))
            continue
        await upstream.send(json.dumps(message, separators=(",", ":")))


async def _upstream_to_client(upstream: Any, client: Any) -> None:
    async for raw in upstream:
        if server_message_is_allowed(raw):
            await client.send(raw)


async def relay_connection(
    client: Any,
    *,
    upstream_url: str,
    secret_file: Path,
    allowed_clients: frozenset[str],
) -> None:
    client_ip = _remote_ip(client)
    if client_ip not in allowed_clients:
        logging.warning("Predict WS relay rejected client=%s", client_ip or "unknown")
        await client.close(code=1008, reason="client_not_allowed")
        return

    try:
        api_key = load_api_key(secret_file)
        async with await connect_upstream(upstream_url, api_key) as upstream:
            logging.info("Predict WS relay connected client=%s", client_ip)
            to_upstream = asyncio.create_task(_client_to_upstream(client, upstream))
            to_client = asyncio.create_task(_upstream_to_client(upstream, client))
            done, pending = await asyncio.wait(
                {to_upstream, to_client},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                exception = task.exception()
                if exception is not None:
                    raise exception
    except RelayProtocolError as exc:
        await _send_protocol_error(client, str(exc))
        await client.close(code=1011, reason="relay_configuration_error")
    finally:
        logging.info("Predict WS relay disconnected client=%s", client_ip or "unknown")


async def serve_relay(
    *,
    host: str,
    port: int,
    upstream_url: str,
    secret_file: Path,
    allowed_clients: Iterable[str],
    max_clients: int = DEFAULT_MAX_CLIENTS,
) -> None:
    allowed = frozenset(str(value).strip() for value in allowed_clients if str(value).strip())
    active_clients: set[str] = set()
    admission_lock = asyncio.Lock()

    async def handler(websocket: Any, _path: str = "") -> None:
        client_ip = _remote_ip(websocket)
        async with admission_lock:
            reason = client_admission_error(
                client_ip,
                allowed_clients=allowed,
                active_clients=active_clients,
                max_clients=max_clients,
            )
            if not reason:
                active_clients.add(client_ip)
        if reason:
            logging.warning(
                "Predict WS relay rejected client=%s reason=%s",
                client_ip or "unknown",
                reason,
            )
            await websocket.close(code=1013, reason=reason)
            return
        try:
            await relay_connection(
                websocket,
                upstream_url=upstream_url,
                secret_file=secret_file,
                allowed_clients=allowed,
            )
        finally:
            async with admission_lock:
                active_clients.discard(client_ip)

    async with websockets.serve(
        handler,
        host,
        port,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=5,
        max_size=MAX_CLIENT_MESSAGE_BYTES,
    ):
        logging.info("Predict WS relay listening host=%s port=%s", host, port)
        await asyncio.Future()


def main() -> None:
    parser = argparse.ArgumentParser(description="Mac-mini-only Predict.fun market-data WebSocket relay.")
    parser.add_argument("--host", default="100.91.159.54")
    parser.add_argument("--port", type=int, default=8792)
    parser.add_argument("--upstream-url", default=DEFAULT_UPSTREAM_URL)
    parser.add_argument("--secret-file", type=Path, default=DEFAULT_SECRET_FILE)
    parser.add_argument("--max-clients", type=int, default=DEFAULT_MAX_CLIENTS)
    parser.add_argument(
        "--allowed-client",
        action="append",
        default=[],
        help="Allowed client IP; repeat to replace the built-in Tailscale allowlist.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    allowed = args.allowed_client or sorted(DEFAULT_ALLOWED_CLIENTS)
    asyncio.run(
        serve_relay(
            host=args.host,
            port=args.port,
            upstream_url=args.upstream_url,
            secret_file=args.secret_file,
            allowed_clients=allowed,
            max_clients=max(1, args.max_clients),
        )
    )


if __name__ == "__main__":
    main()
