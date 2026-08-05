import pytest

from platforms.polymarket.maker.event_bus import EventBus


def test_state_namespace_is_account_local() -> None:
    bus = EventBus(enabled=False, state_namespace="account:6")

    assert bus._state_key == "polymarket:state:account:6"
    assert bus._state_ts_key == "polymarket:state:ts:account:6"


def test_empty_namespace_preserves_single_account_keys() -> None:
    bus = EventBus(enabled=False)

    assert bus._state_key == "polymarket:state"
    assert bus._state_ts_key == "polymarket:state:ts"


def test_invalid_state_namespace_is_rejected() -> None:
    with pytest.raises(ValueError, match="invalid event-bus state namespace"):
        EventBus(enabled=False, state_namespace="../../other")


def test_runtime_namespace_isolates_events_history_and_state() -> None:
    bus = EventBus(
        enabled=False,
        runtime_namespace="aggressive",
        state_namespace="account:6",
    )

    assert bus._events_channel == "polymarket:aggressive:events"
    assert bus._history_key == "polymarket:aggressive:history"
    assert bus._state_key == "polymarket:aggressive:state:account:6"
    assert bus._state_ts_key == "polymarket:aggressive:state:ts:account:6"


def test_invalid_runtime_namespace_is_rejected() -> None:
    with pytest.raises(ValueError, match="invalid event-bus runtime namespace"):
        EventBus(enabled=False, runtime_namespace="../normal")
