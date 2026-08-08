from __future__ import annotations

import asyncio
import json

import pytest

from platforms.predictfun.ws_relay import (
    RelayProtocolError,
    SharedUpstreamRelay,
    client_admission_error,
    connect_upstream,
    probe_relay,
    server_message_is_allowed,
    validate_client_message,
)


_STOP = object()


class FakeRelaySocket:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed: list[tuple[int | None, str]] = []

    async def send(self, raw: str) -> None:
        self.sent.append(raw)

    async def close(
        self,
        *,
        code: int | None = None,
        reason: str = "",
    ) -> None:
        self.closed.append((code, reason))


class FakeUpstream(FakeRelaySocket):
    def __init__(self) -> None:
        super().__init__()
        self.incoming: asyncio.Queue[object] = asyncio.Queue()

    def __aiter__(self) -> "FakeUpstream":
        return self

    async def __anext__(self) -> str:
        item = await self.incoming.get()
        if item is _STOP:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return str(item)

    async def push(self, payload: dict[str, object]) -> None:
        await self.incoming.put(json.dumps(payload, separators=(",", ":")))

    async def fail(self, exc: BaseException) -> None:
        await self.incoming.put(exc)


def _messages(socket: FakeRelaySocket) -> list[dict[str, object]]:
    return [json.loads(raw) for raw in socket.sent]


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


def test_upstream_websocket_bypasses_system_proxy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    sentinel = object()

    async def fake_connect(url: str, **kwargs: object) -> object:
        calls.append((url, kwargs))
        return sentinel

    monkeypatch.setattr(
        "platforms.predictfun.ws_relay.websockets.connect",
        fake_connect,
    )

    result = asyncio.run(connect_upstream("wss://ws.predict.fun/ws", "fixture"))

    assert result is sentinel
    assert calls[0][0] == "wss://ws.predict.fun/ws"
    assert calls[0][1]["proxy"] is None
    assert calls[0][1]["ping_interval"] is None
    assert "ping_timeout" not in calls[0][1]


@pytest.mark.parametrize(
    "topic",
    [
        "predictOrderbook/58416",
        "predictTradingStatus/58416",
        "predictMarketStatus/58416",
        "predictMarketChanged/58416",
    ],
)
def test_relay_allows_only_public_market_topics(topic: str) -> None:
    message = validate_client_message(
        json.dumps({"method": "subscribe", "requestId": 7, "params": [topic]})
    )
    assert message == {"method": "subscribe", "requestId": 7, "params": [topic]}


@pytest.mark.parametrize(
    "topic",
    [
        "predictWalletEvents/private-jwt",
        "predictOrderbook/not-a-number",
        "predictOrderbook/0",
        "otherTopic/58416",
    ],
)
def test_relay_rejects_account_and_unknown_topics(topic: str) -> None:
    with pytest.raises(RelayProtocolError, match="topic_not_allowed"):
        validate_client_message(
            json.dumps({"method": "subscribe", "requestId": 7, "params": [topic]})
        )


def test_relay_heartbeat_echo_never_requires_credentials() -> None:
    assert validate_client_message('{"method":"heartbeat","data":1736696400000}') == {
        "method": "heartbeat",
        "data": 1736696400000,
    }


def test_relay_rejects_arbitrary_methods_and_multiple_topics() -> None:
    with pytest.raises(RelayProtocolError, match="method_not_allowed"):
        validate_client_message('{"method":"submitOrder","requestId":1,"params":[]}')
    with pytest.raises(RelayProtocolError, match="single_topic_required"):
        validate_client_message(
            '{"method":"subscribe","requestId":1,"params":["predictOrderbook/1","predictOrderbook/2"]}'
        )


def test_relay_filters_upstream_messages_to_market_data_and_responses() -> None:
    assert server_message_is_allowed('{"type":"R","requestId":1,"success":true}') is True
    assert server_message_is_allowed('{"type":"M","topic":"heartbeat","data":1}') is True
    assert server_message_is_allowed('{"type":"M","topic":"predictOrderbook/1","data":{}}') is True
    assert server_message_is_allowed('{"type":"M","topic":"predictWalletEvents/secret","data":{}}') is False
    assert server_message_is_allowed("not-json") is False


def test_relay_allows_one_connection_per_tailnet_client() -> None:
    allowed = frozenset({"100.122.255.98", "100.101.50.40"})
    assert client_admission_error(
        "100.122.255.98",
        allowed_clients=allowed,
        active_clients=set(),
        max_clients=3,
    ) == ""
    assert client_admission_error(
        "100.122.255.98",
        allowed_clients=allowed,
        active_clients={"100.122.255.98"},
        max_clients=3,
    ) == "duplicate_client"
    assert client_admission_error(
        "100.90.90.90",
        allowed_clients=allowed,
        active_clients=set(),
        max_clients=3,
    ) == "client_not_allowed"
    assert client_admission_error(
        "100.101.50.40",
        allowed_clients=allowed,
        active_clients={"100.122.255.98"},
        max_clients=1,
    ) == "relay_capacity_reached"


def test_relay_probe_requires_positive_market_id() -> None:
    with pytest.raises(RelayProtocolError, match="probe_market_id_invalid"):
        asyncio.run(probe_relay("ws://127.0.0.1:1", 0))


def test_shared_upstream_deduplicates_topics_and_routes_by_subscriber(
    tmp_path,
) -> None:
    async def scenario() -> None:
        secret_file = tmp_path / "predictfun.env"
        secret_file.write_text("PREDICTFUN_API_KEY=fixture-key\n", encoding="utf-8")
        upstream = FakeUpstream()
        connector_calls: list[tuple[str, str]] = []

        async def connector(url: str, api_key: str) -> FakeUpstream:
            connector_calls.append((url, api_key))
            return upstream

        relay = SharedUpstreamRelay(
            upstream_url="wss://fixture.invalid/ws",
            secret_file=secret_file,
            connector=connector,
        )
        first = FakeRelaySocket()
        second = FakeRelaySocket()
        topic = "predictOrderbook/58416"
        try:
            await relay.add_client(first)
            await relay.add_client(second)
            assert connector_calls == [("wss://fixture.invalid/ws", "fixture-key")]

            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "subscribe", "requestId": 7, "params": [topic]}
                ),
            )
            assert first.sent == []
            assert _messages(upstream) == [
                {"method": "subscribe", "requestId": 1, "params": [topic]}
            ]

            await relay.handle_client_message(
                second,
                json.dumps(
                    {"method": "subscribe", "requestId": 19, "params": [topic]}
                ),
            )
            assert len(upstream.sent) == 1
            assert second.sent == []

            await upstream.push(
                {"type": "R", "requestId": 1, "success": True}
            )
            await _settle()
            assert _messages(first) == [
                {"type": "R", "requestId": 7, "success": True}
            ]
            assert _messages(second) == [
                {"type": "R", "requestId": 19, "success": True}
            ]

            market_message = {
                "type": "M",
                "topic": topic,
                "data": {"sequence": 1},
            }
            await upstream.push(market_message)
            await _settle()
            assert _messages(first)[-1] == market_message
            assert _messages(second)[-1] == market_message

            first_count = len(first.sent)
            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "unsubscribe", "requestId": 20, "params": [topic]}
                ),
            )
            assert _messages(first)[-1] == {
                "type": "R",
                "requestId": 20,
                "success": True,
            }
            assert len(upstream.sent) == 1

            market_message["data"] = {"sequence": 2}
            await upstream.push(market_message)
            await _settle()
            assert len(first.sent) == first_count + 1
            assert _messages(second)[-1] == market_message

            await relay.handle_client_message(
                second,
                json.dumps(
                    {"method": "unsubscribe", "requestId": 21, "params": [topic]}
                ),
            )
            assert _messages(upstream)[-1] == {
                "method": "unsubscribe",
                "requestId": 2,
                "params": [topic],
            }
            assert _messages(second)[-1] == market_message
            await upstream.push(
                {"type": "R", "requestId": 2, "success": True}
            )
            await _settle()
            assert _messages(second)[-1] == {
                "type": "R",
                "requestId": 21,
                "success": True,
            }
        finally:
            await relay.close()

    asyncio.run(scenario())


def test_shared_upstream_uses_one_heartbeat_ack_for_all_clients(tmp_path) -> None:
    async def scenario() -> None:
        secret_file = tmp_path / "predictfun.env"
        secret_file.write_text("PREDICTFUN_API_KEY=fixture-key\n", encoding="utf-8")
        upstream = FakeUpstream()

        async def connector(_url: str, _api_key: str) -> FakeUpstream:
            return upstream

        relay = SharedUpstreamRelay(
            upstream_url="wss://fixture.invalid/ws",
            secret_file=secret_file,
            connector=connector,
        )
        first = FakeRelaySocket()
        second = FakeRelaySocket()
        heartbeat = {"type": "M", "topic": "heartbeat", "data": 12345}
        try:
            await relay.add_client(first)
            await relay.add_client(second)
            await upstream.push(heartbeat)
            await _settle()

            assert _messages(first) == [heartbeat]
            assert _messages(second) == [heartbeat]
            assert _messages(upstream) == [
                {"method": "heartbeat", "data": 12345}
            ]

            await relay.handle_client_message(
                first,
                '{"method":"heartbeat","data":12345}',
            )
            await relay.handle_client_message(
                second,
                '{"method":"heartbeat","data":12345}',
            )
            assert len(upstream.sent) == 1
        finally:
            await relay.close()

    asyncio.run(scenario())


def test_shared_upstream_resubscribes_after_inflight_last_unsubscribe(
    tmp_path,
) -> None:
    async def scenario() -> None:
        secret_file = tmp_path / "predictfun.env"
        secret_file.write_text("PREDICTFUN_API_KEY=fixture-key\n", encoding="utf-8")
        upstream = FakeUpstream()

        async def connector(_url: str, _api_key: str) -> FakeUpstream:
            return upstream

        relay = SharedUpstreamRelay(
            upstream_url="wss://fixture.invalid/ws",
            secret_file=secret_file,
            connector=connector,
        )
        first = FakeRelaySocket()
        second = FakeRelaySocket()
        topic = "predictMarketChanged/58416"
        try:
            await relay.add_client(first)
            await relay.add_client(second)
            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "subscribe", "requestId": 1, "params": [topic]}
                ),
            )
            await upstream.push(
                {"type": "R", "requestId": 1, "success": True}
            )
            await _settle()

            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "unsubscribe", "requestId": 2, "params": [topic]}
                ),
            )
            await relay.handle_client_message(
                second,
                json.dumps(
                    {"method": "subscribe", "requestId": 3, "params": [topic]}
                ),
            )
            assert _messages(upstream)[-1] == {
                "method": "unsubscribe",
                "requestId": 2,
                "params": [topic],
            }
            assert second.sent == []

            await upstream.push(
                {"type": "R", "requestId": 2, "success": True}
            )
            await _settle()
            assert _messages(first)[-1] == {
                "type": "R",
                "requestId": 2,
                "success": True,
            }
            assert _messages(upstream)[-1] == {
                "method": "subscribe",
                "requestId": 3,
                "params": [topic],
            }
            assert second.sent == []

            await upstream.push(
                {"type": "R", "requestId": 3, "success": True}
            )
            await _settle()
            assert _messages(second)[-1] == {
                "type": "R",
                "requestId": 3,
                "success": True,
            }
        finally:
            await relay.close()

    asyncio.run(scenario())


def test_shared_upstream_cancels_deferred_resubscribe(tmp_path) -> None:
    async def scenario() -> None:
        secret_file = tmp_path / "predictfun.env"
        secret_file.write_text("PREDICTFUN_API_KEY=fixture-key\n", encoding="utf-8")
        upstream = FakeUpstream()

        async def connector(_url: str, _api_key: str) -> FakeUpstream:
            return upstream

        relay = SharedUpstreamRelay(
            upstream_url="wss://fixture.invalid/ws",
            secret_file=secret_file,
            connector=connector,
        )
        first = FakeRelaySocket()
        second = FakeRelaySocket()
        topic = "predictMarketStatus/58416"
        try:
            await relay.add_client(first)
            await relay.add_client(second)
            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "subscribe", "requestId": 1, "params": [topic]}
                ),
            )
            await upstream.push(
                {"type": "R", "requestId": 1, "success": True}
            )
            await _settle()

            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "unsubscribe", "requestId": 2, "params": [topic]}
                ),
            )
            await relay.handle_client_message(
                second,
                json.dumps(
                    {"method": "subscribe", "requestId": 3, "params": [topic]}
                ),
            )
            await relay.handle_client_message(
                second,
                json.dumps(
                    {"method": "unsubscribe", "requestId": 4, "params": [topic]}
                ),
            )
            assert _messages(second) == [
                {"type": "R", "requestId": 3, "success": True}
            ]

            await upstream.push(
                {"type": "R", "requestId": 2, "success": True}
            )
            await _settle()
            assert _messages(second)[-1] == {
                "type": "R",
                "requestId": 4,
                "success": True,
            }
            assert len(upstream.sent) == 2
        finally:
            await relay.close()

    asyncio.run(scenario())


def test_shared_upstream_rejected_last_unsubscribe_resets_clients(tmp_path) -> None:
    async def scenario() -> None:
        secret_file = tmp_path / "predictfun.env"
        secret_file.write_text("PREDICTFUN_API_KEY=fixture-key\n", encoding="utf-8")
        upstream = FakeUpstream()

        async def connector(_url: str, _api_key: str) -> FakeUpstream:
            return upstream

        relay = SharedUpstreamRelay(
            upstream_url="wss://fixture.invalid/ws",
            secret_file=secret_file,
            connector=connector,
        )
        first = FakeRelaySocket()
        second = FakeRelaySocket()
        topic = "predictOrderbook/58416"
        try:
            await relay.add_client(first)
            await relay.add_client(second)
            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "subscribe", "requestId": 1, "params": [topic]}
                ),
            )
            await upstream.push(
                {"type": "R", "requestId": 1, "success": True}
            )
            await _settle()
            await relay.handle_client_message(
                first,
                json.dumps(
                    {"method": "unsubscribe", "requestId": 2, "params": [topic]}
                ),
            )

            await upstream.push(
                {
                    "type": "R",
                    "requestId": 2,
                    "success": False,
                    "error": {"code": "fixture_unsubscribe_rejected"},
                }
            )
            await _settle()

            assert _messages(first)[-1] == {
                "type": "R",
                "requestId": 2,
                "success": False,
                "error": {"code": "fixture_unsubscribe_rejected"},
            }
            assert first.closed[-1] == (1011, "upstream_disconnected")
            assert second.closed[-1] == (1011, "upstream_disconnected")
        finally:
            await relay.close()

    asyncio.run(scenario())


def test_shared_upstream_rejection_and_disconnect_fail_closed(tmp_path) -> None:
    async def scenario() -> None:
        secret_file = tmp_path / "predictfun.env"
        secret_file.write_text("PREDICTFUN_API_KEY=fixture-key\n", encoding="utf-8")
        upstream = FakeUpstream()

        async def connector(_url: str, _api_key: str) -> FakeUpstream:
            return upstream

        relay = SharedUpstreamRelay(
            upstream_url="wss://fixture.invalid/ws",
            secret_file=secret_file,
            connector=connector,
        )
        first = FakeRelaySocket()
        second = FakeRelaySocket()
        topic = "predictTradingStatus/58416"
        try:
            await relay.add_client(first)
            await relay.add_client(second)
            for client, request_id in ((first, 3), (second, 4)):
                await relay.handle_client_message(
                    client,
                    json.dumps(
                        {
                            "method": "subscribe",
                            "requestId": request_id,
                            "params": [topic],
                        }
                    ),
                )
            await upstream.push(
                {
                    "type": "R",
                    "requestId": 1,
                    "success": False,
                    "error": {"code": "fixture_rejected"},
                }
            )
            await _settle()
            assert _messages(first)[-1] == {
                "type": "R",
                "requestId": 3,
                "success": False,
                "error": {"code": "fixture_rejected"},
            }
            assert _messages(second)[-1] == {
                "type": "R",
                "requestId": 4,
                "success": False,
                "error": {"code": "fixture_rejected"},
            }

            rejected_counts = (len(first.sent), len(second.sent))
            await upstream.push(
                {"type": "M", "topic": topic, "data": {"status": "OPEN"}}
            )
            await _settle()
            assert (len(first.sent), len(second.sent)) == rejected_counts

            await upstream.fail(ConnectionError("fixture disconnect"))
            await _settle()
            assert first.closed[-1] == (1011, "upstream_disconnected")
            assert second.closed[-1] == (1011, "upstream_disconnected")
        finally:
            await relay.close()

    asyncio.run(scenario())
