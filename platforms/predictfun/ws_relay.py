from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import websockets
from websockets.exceptions import ConnectionClosed


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
        # Predict's application heartbeat is authoritative; protocol pings caused false timeouts.
        return await websockets.connect(
            upstream_url,
            additional_headers=headers,
            proxy=None,
            ping_interval=None,
            close_timeout=5,
            max_size=8 * 1024 * 1024,
        )
    except TypeError:
        return await websockets.connect(
            upstream_url,
            extra_headers=headers,
            proxy=None,
            ping_interval=None,
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


async def _send_subscription_result(
    client: Any,
    request_id: int,
    *,
    success: bool = True,
    error: Any = None,
) -> None:
    payload: dict[str, Any] = {
        "type": "R",
        "requestId": request_id,
        "success": success,
    }
    if not success:
        payload["error"] = error or {"code": "upstream_rejected"}
    await client.send(json.dumps(payload, separators=(",", ":")))


@dataclass
class _PendingControl:
    method: str
    topic: str
    acknowledgements: list[tuple[Any, int]] = field(default_factory=list)


class SharedUpstreamRelay:
    """Multiplex public subscriptions over one authenticated upstream socket."""

    def __init__(
        self,
        *,
        upstream_url: str,
        secret_file: Path,
        connector: Any = connect_upstream,
    ) -> None:
        self.upstream_url = upstream_url
        self.secret_file = secret_file
        self.connector = connector
        self._upstream: Any | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._clients: dict[Any, set[str]] = {}
        self._active_topics: set[str] = set()
        self._pending_controls: dict[int, _PendingControl] = {}
        self._pending_by_topic: dict[tuple[str, str], int] = {}
        self._deferred_subscribe_acks: dict[str, list[tuple[Any, int]]] = {}
        self._connect_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._next_request_id = 1

    async def add_client(self, client: Any) -> None:
        await self._ensure_upstream()
        async with self._state_lock:
            self._clients.setdefault(client, set())

    async def remove_client(self, client: Any) -> None:
        controls: list[tuple[int, _PendingControl]] = []
        async with self._state_lock:
            removed_topics = self._clients.pop(client, set())
            for pending in self._pending_controls.values():
                pending.acknowledgements = [
                    acknowledgement
                    for acknowledgement in pending.acknowledgements
                    if acknowledgement[0] is not client
                ]
            for topic, acknowledgements in list(
                self._deferred_subscribe_acks.items()
            ):
                kept = [
                    acknowledgement
                    for acknowledgement in acknowledgements
                    if acknowledgement[0] is not client
                ]
                if kept:
                    self._deferred_subscribe_acks[topic] = kept
                else:
                    self._deferred_subscribe_acks.pop(topic, None)
            for topic in removed_topics:
                if self._topic_is_desired_locked(topic):
                    continue
                if self._pending_id_locked("subscribe", topic) is not None:
                    continue
                if self._pending_id_locked("unsubscribe", topic) is not None:
                    continue
                if topic in self._active_topics:
                    controls.append(
                        self._register_control_locked("unsubscribe", topic)
                    )
        for request_id, pending in controls:
            try:
                await self._send_registered_control(request_id, pending)
            except Exception:
                logging.exception(
                    "Predict WS relay failed to unsubscribe orphaned topic=%s",
                    pending.topic,
                )

    async def handle_client_message(self, client: Any, raw: str | bytes) -> None:
        try:
            message = validate_client_message(raw)
        except RelayProtocolError as exc:
            await _send_protocol_error(client, str(exc))
            return

        if message["method"] == "heartbeat":
            # The relay acknowledges each upstream heartbeat exactly once.
            return

        method = str(message["method"])
        request_id = int(message["requestId"])
        topic = str(message["params"][0])
        control: tuple[int, _PendingControl] | None = None
        immediate: list[tuple[Any, int]] = []
        async with self._state_lock:
            if client not in self._clients:
                raise RelayProtocolError("client_not_registered")
            topics = self._clients[client]
            if method == "subscribe":
                topics.add(topic)
                pending_unsubscribe = self._pending_control_locked(
                    "unsubscribe",
                    topic,
                )
                pending_subscribe = self._pending_control_locked(
                    "subscribe",
                    topic,
                )
                if pending_unsubscribe is not None:
                    self._deferred_subscribe_acks.setdefault(topic, []).append(
                        (client, request_id)
                    )
                elif topic in self._active_topics:
                    immediate.append((client, request_id))
                elif pending_subscribe is not None:
                    pending_subscribe.acknowledgements.append(
                        (client, request_id)
                    )
                else:
                    control = self._register_control_locked(
                        "subscribe",
                        topic,
                        [(client, request_id)],
                    )
            else:
                was_subscribed = topic in topics
                topics.discard(topic)
                pending_unsubscribe = self._pending_control_locked(
                    "unsubscribe",
                    topic,
                )
                pending_subscribe = self._pending_control_locked(
                    "subscribe",
                    topic,
                )
                if not was_subscribed or self._topic_is_desired_locked(topic):
                    immediate.append((client, request_id))
                elif pending_unsubscribe is not None:
                    deferred = self._deferred_subscribe_acks.get(topic, [])
                    cancelled = [
                        acknowledgement
                        for acknowledgement in deferred
                        if acknowledgement[0] is client
                    ]
                    kept = [
                        acknowledgement
                        for acknowledgement in deferred
                        if acknowledgement[0] is not client
                    ]
                    if kept:
                        self._deferred_subscribe_acks[topic] = kept
                    else:
                        self._deferred_subscribe_acks.pop(topic, None)
                    immediate.extend(cancelled)
                    pending_unsubscribe.acknowledgements.append(
                        (client, request_id)
                    )
                elif pending_subscribe is not None:
                    # Local delivery stops immediately. Once the in-flight
                    # subscribe settles, the relay removes the orphan upstream.
                    immediate.append((client, request_id))
                elif topic in self._active_topics:
                    control = self._register_control_locked(
                        "unsubscribe",
                        topic,
                        [(client, request_id)],
                    )
                else:
                    immediate.append((client, request_id))
        await self._send_results(immediate, success=True)
        if control is not None:
            await self._send_registered_control(*control)

    async def close(self) -> None:
        async with self._state_lock:
            clients = list(self._clients)
            self._clients.clear()
            self._active_topics.clear()
            self._pending_controls.clear()
            self._pending_by_topic.clear()
            self._deferred_subscribe_acks.clear()
        for client in clients:
            try:
                await client.close(code=1001, reason="relay_stopping")
            except Exception:
                pass
        task = self._reader_task
        self._reader_task = None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        upstream = self._upstream
        self._upstream = None
        if upstream is not None:
            try:
                await upstream.close()
            except Exception:
                pass

    async def _ensure_upstream(self) -> None:
        async with self._connect_lock:
            if self._upstream is not None:
                return
            api_key = load_api_key(self.secret_file)
            upstream = await self.connector(self.upstream_url, api_key)
            self._upstream = upstream
            self._reader_task = asyncio.create_task(
                self._read_upstream(upstream)
            )
            logging.info("Predict WS relay shared upstream connected")

    def _topic_is_desired_locked(self, topic: str) -> bool:
        return any(topic in topics for topics in self._clients.values())

    def _pending_id_locked(self, method: str, topic: str) -> int | None:
        return self._pending_by_topic.get((method, topic))

    def _pending_control_locked(
        self,
        method: str,
        topic: str,
    ) -> _PendingControl | None:
        request_id = self._pending_id_locked(method, topic)
        return self._pending_controls.get(request_id) if request_id is not None else None

    def _register_control_locked(
        self,
        method: str,
        topic: str,
        acknowledgements: list[tuple[Any, int]] | None = None,
    ) -> tuple[int, _PendingControl]:
        request_id = self._next_request_id
        self._next_request_id += 1
        pending = _PendingControl(
            method=method,
            topic=topic,
            acknowledgements=list(acknowledgements or []),
        )
        self._pending_controls[request_id] = pending
        self._pending_by_topic[(method, topic)] = request_id
        return request_id, pending

    async def _send_registered_control(
        self,
        request_id: int,
        pending: _PendingControl,
    ) -> None:
        try:
            await self._send_upstream(
                {
                    "method": pending.method,
                    "requestId": request_id,
                    "params": [pending.topic],
                }
            )
        except Exception:
            await self._control_send_failed(request_id, pending)
            raise

    async def _control_send_failed(
        self,
        request_id: int,
        pending: _PendingControl,
    ) -> None:
        failed: list[tuple[Any, int]] = []
        deferred: list[tuple[Any, int]] = []
        async with self._state_lock:
            if self._pending_controls.get(request_id) is not pending:
                return
            self._pending_controls.pop(request_id, None)
            self._pending_by_topic.pop((pending.method, pending.topic), None)
            failed = list(pending.acknowledgements)
            if pending.method == "subscribe":
                for client, _client_request_id in failed:
                    self._clients.get(client, set()).discard(pending.topic)
            else:
                deferred = self._deferred_subscribe_acks.pop(
                    pending.topic,
                    [],
                )
                recovered = [
                    acknowledgement
                    for acknowledgement in recovered
                    if pending.topic in self._clients.get(
                        acknowledgement[0],
                        set(),
                    )
                ]
        await self._send_results(
            failed,
            success=False,
            error={"code": "upstream_send_failed"},
        )
        await self._send_results(
            deferred,
            success=False,
            error={"code": "upstream_send_failed"},
        )
        upstream = self._upstream
        if upstream is not None:
            try:
                await upstream.close()
            except Exception:
                pass

    async def _send_upstream(self, message: dict[str, Any]) -> None:
        async with self._send_lock:
            upstream = self._upstream
            if upstream is None:
                raise RelayProtocolError("upstream_not_connected")
            await upstream.send(json.dumps(message, separators=(",", ":")))

    async def _read_upstream(self, upstream: Any) -> None:
        try:
            async for raw in upstream:
                if not server_message_is_allowed(raw):
                    continue
                try:
                    message = json.loads(raw)
                except Exception:
                    continue
                topic = str(message.get("topic") or "")
                if topic == "heartbeat":
                    await self._send_upstream(
                        {"method": "heartbeat", "data": message.get("data")}
                    )
                    async with self._state_lock:
                        targets = list(self._clients)
                elif ALLOWED_TOPIC.fullmatch(topic):
                    async with self._state_lock:
                        targets = [
                            client
                            for client, topics in self._clients.items()
                            if topic in topics
                        ]
                elif str(message.get("type") or "") == "R":
                    await self._handle_upstream_response(message)
                    continue
                else:
                    continue
                results = await asyncio.gather(
                    *(client.send(raw) for client in targets),
                    return_exceptions=True,
                )
                for client, result in zip(targets, results):
                    if isinstance(result, Exception):
                        await self.remove_client(client)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning(
                "Predict WS relay shared upstream disconnected: %s: %s",
                exc.__class__.__name__,
                exc,
            )
        finally:
            async with self._connect_lock:
                if self._upstream is upstream:
                    self._upstream = None
            async with self._state_lock:
                clients = list(self._clients)
                self._clients.clear()
                self._active_topics.clear()
                self._pending_controls.clear()
                self._pending_by_topic.clear()
                self._deferred_subscribe_acks.clear()
            for client in clients:
                try:
                    await client.close(
                        code=1011,
                        reason="upstream_disconnected",
                    )
                except Exception:
                    pass
            try:
                await upstream.close()
            except Exception:
                pass

    async def _handle_upstream_response(self, message: dict[str, Any]) -> None:
        request_id = message.get("requestId")
        if isinstance(request_id, bool) or not isinstance(request_id, int):
            return
        acknowledgements: list[tuple[Any, int]] = []
        deferred_failure: list[tuple[Any, int]] = []
        follow_up: tuple[int, _PendingControl] | None = None
        reset_upstream = False
        success = message.get("success") is True
        error = message.get("error") or {"code": "upstream_rejected"}
        async with self._state_lock:
            pending = self._pending_controls.pop(request_id, None)
            if pending is None:
                return
            self._pending_by_topic.pop((pending.method, pending.topic), None)
            acknowledgements = list(pending.acknowledgements)
            desired = self._topic_is_desired_locked(pending.topic)
            if pending.method == "subscribe":
                if success:
                    self._active_topics.add(pending.topic)
                    if not desired:
                        follow_up = self._register_control_locked(
                            "unsubscribe",
                            pending.topic,
                        )
                else:
                    self._active_topics.discard(pending.topic)
                    for client, _client_request_id in acknowledgements:
                        self._clients.get(client, set()).discard(pending.topic)
            elif success:
                self._active_topics.discard(pending.topic)
                deferred = self._deferred_subscribe_acks.pop(
                    pending.topic,
                    [],
                )
                deferred = [
                    acknowledgement
                    for acknowledgement in deferred
                    if pending.topic in self._clients.get(
                        acknowledgement[0],
                        set(),
                    )
                ]
                if deferred:
                    follow_up = self._register_control_locked(
                        "subscribe",
                        pending.topic,
                        deferred,
                    )
            else:
                # The server's state is uncertain after a rejected last
                # unsubscribe. Reset the shared socket instead of pretending
                # that either the old or a deferred subscription is active.
                deferred_failure = self._deferred_subscribe_acks.pop(
                    pending.topic,
                    [],
                )
                reset_upstream = True
        await self._send_results(
            acknowledgements,
            success=success,
            error=error,
        )
        await self._send_results(
            deferred_failure,
            success=False,
            error=error,
        )
        if reset_upstream:
            raise RelayProtocolError("upstream_unsubscribe_rejected")
        if follow_up is not None:
            await self._send_registered_control(*follow_up)

    async def _send_results(
        self,
        acknowledgements: list[tuple[Any, int]],
        *,
        success: bool,
        error: Any = None,
    ) -> None:
        if not acknowledgements:
            return
        results = await asyncio.gather(
            *(
                _send_subscription_result(
                    client,
                    request_id,
                    success=success,
                    error=error,
                )
                for client, request_id in acknowledgements
            ),
            return_exceptions=True,
        )
        for (client, _request_id), result in zip(acknowledgements, results):
            if isinstance(result, Exception):
                await self.remove_client(client)


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
    relay = SharedUpstreamRelay(
        upstream_url=upstream_url,
        secret_file=secret_file,
    )

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
            await relay.add_client(websocket)
            logging.info("Predict WS relay connected client=%s", client_ip)
            async for raw in websocket:
                await relay.handle_client_message(websocket, raw)
        except ConnectionClosed:
            pass
        except RelayProtocolError as exc:
            await _send_protocol_error(websocket, str(exc))
            await websocket.close(code=1011, reason="relay_configuration_error")
        except Exception:
            logging.exception(
                "Predict WS relay client failed client=%s",
                client_ip or "unknown",
            )
            await websocket.close(code=1011, reason="relay_upstream_error")
        finally:
            await relay.remove_client(websocket)
            async with admission_lock:
                active_clients.discard(client_ip)
            logging.info(
                "Predict WS relay disconnected client=%s",
                client_ip or "unknown",
            )

    try:
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
    finally:
        await relay.close()


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
