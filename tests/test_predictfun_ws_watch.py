from __future__ import annotations

import asyncio
import copy
import json
from types import SimpleNamespace
from typing import Any

import pytest

from platforms.predictfun import ws_watch


class FakeWebSocket:
    def __init__(self, *, fail_after_sends: int = 0) -> None:
        self.sent: list[dict[str, Any]] = []
        self.recv_calls = 0
        self.fail_after_sends = fail_after_sends

    async def __aenter__(self) -> "FakeWebSocket":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def send(self, raw: str) -> None:
        self.sent.append(json.loads(raw))
        if self.fail_after_sends and len(self.sent) > self.fail_after_sends:
            raise ConnectionError("relay send failed")

    async def recv(self) -> str:
        self.recv_calls += 1
        if self.recv_calls == 1:
            await asyncio.sleep(1)
        return json.dumps({"topic": "heartbeat", "data": 123})


def _market(market_id: int) -> SimpleNamespace:
    return SimpleNamespace(id=market_id, status="OPEN", trading_status="OPEN")


def test_subscription_refresh_reuses_connection_and_prunes_removed_markets() -> None:
    websocket = FakeWebSocket()
    state = {
        "market_ids": [1, 2],
        "orderbooks": {"1": {"bids": []}, "2": {"bids": []}},
        "orderbook_updated_at": {"1": "old", "2": "current"},
        "orderbook_upstream_updated_at_ms": {"1": 1, "2": 2},
        "orderbook_latency_ms": {"1": 1, "2": 2},
        "orderbook_errors": {"1": "old error"},
        "trading_statuses": {"1": {"status": "OPEN"}, "2": {"status": "OPEN"}},
        "market_statuses": {"1": {"status": "OPEN"}, "2": {"status": "OPEN"}},
        "liquidity": {"1": {}, "2": {}},
        "liquidity_alerts": {"1": {}, "2": {}},
    }

    next_request_id = asyncio.run(
        ws_watch._sync_market_subscriptions(
            websocket,
            state,
            market_ids=[2, 3],
            market_statuses={"2": {"status": "OPEN"}, "3": {"status": "OPEN"}},
            trading_statuses={
                "2": {"status": "OPEN"},
                "3": {"status": "OPEN"},
            },
            request_id=10,
        )
    )

    assert next_request_id == 16
    assert state["market_ids"] == [2, 3]
    assert "1" not in state["orderbooks"]
    assert "2" in state["orderbooks"]
    assert state["market_statuses"]["3"]["status"] == "OPEN"
    assert state["trading_statuses"]["3"]["status"] == "OPEN"
    assert [message["method"] for message in websocket.sent] == [
        "unsubscribe",
        "unsubscribe",
        "unsubscribe",
        "subscribe",
        "subscribe",
        "subscribe",
    ]
    assert state["last_subscription_refresh"]["removed_market_ids"] == [1]
    assert state["last_subscription_refresh"]["added_market_ids"] == [3]


def test_planned_market_refresh_never_publishes_disconnected_state(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    websocket = FakeWebSocket()
    writes: list[dict[str, Any]] = []

    async def fake_connect(_ws_url: str, _api_key: str) -> FakeWebSocket:
        return websocket

    async def refresh_markets() -> list[SimpleNamespace]:
        return [_market(2)]

    monkeypatch.setattr(ws_watch, "_connect", fake_connect)
    monkeypatch.setattr(
        ws_watch,
        "_write_state",
        lambda _path, state: writes.append(copy.deepcopy(state)),
    )

    state = asyncio.run(
        ws_watch.watch_orderbooks(
            ws_url="ws://relay.test",
            api_key="",
            market_ids=[1],
            state_path=tmp_path / "ws-state.json",
            max_messages=1,
            timeout_sec=1,
            initial_market_statuses={"1": {"status": "OPEN"}},
            market_refresher=refresh_markets,
            refresh_sec=0.01,
        )
    )

    refresh_writes = [row for row in writes if row.get("refresh_count") == 1]
    assert refresh_writes
    assert refresh_writes[0]["connected"] is True
    assert refresh_writes[0]["market_ids"] == [2]
    assert all(row["connected"] is True for row in writes[1:-1])
    assert writes[-1]["connected"] is False
    assert state["error"] == ""
    assert state["refresh_count"] == 1


def test_discovery_failure_keeps_existing_connection_and_subscriptions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    websocket = FakeWebSocket()
    writes: list[dict[str, Any]] = []

    async def fake_connect(_ws_url: str, _api_key: str) -> FakeWebSocket:
        return websocket

    async def refresh_markets() -> list[SimpleNamespace]:
        raise RuntimeError("temporary discovery failure")

    monkeypatch.setattr(ws_watch, "_connect", fake_connect)
    monkeypatch.setattr(
        ws_watch,
        "_write_state",
        lambda _path, state: writes.append(copy.deepcopy(state)),
    )

    state = asyncio.run(
        ws_watch.watch_orderbooks(
            ws_url="ws://relay.test",
            api_key="",
            market_ids=[1],
            state_path=tmp_path / "ws-state.json",
            max_messages=1,
            timeout_sec=1,
            initial_market_statuses={"1": {"status": "OPEN"}},
            market_refresher=refresh_markets,
            refresh_sec=0.01,
        )
    )

    failed_refresh_writes = [
        row for row in writes if row.get("discovery_failure_count") == 1
    ]
    assert failed_refresh_writes
    assert failed_refresh_writes[0]["connected"] is True
    assert failed_refresh_writes[0]["market_ids"] == [1]
    assert all(row["connected"] is True for row in writes[1:-1])
    assert writes[-1]["connected"] is False
    assert "temporary discovery failure" in state["discovery_error"]
    assert state["error"] == ""


def test_real_connection_failure_is_published_as_disconnected(tmp_path, monkeypatch) -> None:
    async def failed_connect(_ws_url: str, _api_key: str) -> FakeWebSocket:
        raise ConnectionError("relay unavailable")

    monkeypatch.setattr(ws_watch, "_connect", failed_connect)

    state = asyncio.run(
        ws_watch.watch_orderbooks(
            ws_url="ws://relay.test",
            api_key="",
            market_ids=[1],
            state_path=tmp_path / "ws-state.json",
        )
    )

    assert state["connected"] is False
    assert state["error"] == "ConnectionError: relay unavailable"


def test_subscription_transport_failure_is_published_as_disconnected(
    tmp_path,
    monkeypatch,
) -> None:
    websocket = FakeWebSocket(fail_after_sends=3)

    async def fake_connect(_ws_url: str, _api_key: str) -> FakeWebSocket:
        return websocket

    async def refresh_markets() -> list[SimpleNamespace]:
        return [_market(2)]

    monkeypatch.setattr(ws_watch, "_connect", fake_connect)

    state = asyncio.run(
        ws_watch.watch_orderbooks(
            ws_url="ws://relay.test",
            api_key="",
            market_ids=[1],
            state_path=tmp_path / "ws-state.json",
            timeout_sec=1,
            initial_market_statuses={"1": {"status": "OPEN"}},
            market_refresher=refresh_markets,
            refresh_sec=0.01,
        )
    )

    assert state["connected"] is False
    assert state["error"] == "ConnectionError: relay send failed"


def test_forever_uses_heartbeat_safe_idle_timeout_and_settles_before_reconnect(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    calls: list[dict[str, Any]] = []
    sleep_delays: list[float] = []

    monkeypatch.setattr(
        ws_watch,
        "discover_markets",
        lambda *_args, **_kwargs: [_market(1)],
    )

    async def fake_watch_orderbooks(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        if len(calls) == 1:
            return {"error": ""}
        raise asyncio.CancelledError

    async def fake_sleep(delay: float) -> None:
        sleep_delays.append(delay)

    monkeypatch.setattr(ws_watch, "watch_orderbooks", fake_watch_orderbooks)
    monkeypatch.setattr(ws_watch.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(
            ws_watch.watch_orderbooks_forever(
                client=object(),
                cfg={},
                ws_url="ws://relay.test",
                api_key="",
                state_path=tmp_path / "ws-state.json",
                discover_limit=20,
                refresh_sec=300,
                idle_timeout_sec=330,
            )
        )

    assert calls[0]["timeout_sec"] == 900
    assert calls[0]["session_number"] == 1
    assert calls[0]["reconnect_count"] == 0
    assert calls[0]["initial_market_statuses"]["1"]["source"] == "rest_discovery"
    assert calls[0]["initial_trading_statuses"]["1"]["source"] == "rest_discovery"
    assert calls[1]["session_number"] == 2
    assert calls[1]["reconnect_count"] == 1
    assert sleep_delays == [ws_watch.RECONNECT_SETTLE_SEC]


def test_forever_idle_timeout_scales_with_refresh_window() -> None:
    assert (
        ws_watch._forever_idle_timeout_sec(
            refresh_sec=600,
            configured_sec=900,
        )
        == 1800
    )
