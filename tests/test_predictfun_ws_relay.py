from __future__ import annotations

import asyncio
import json

import pytest

from platforms.predictfun.ws_relay import (
    RelayProtocolError,
    client_admission_error,
    connect_upstream,
    probe_relay,
    server_message_is_allowed,
    validate_client_message,
)


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
