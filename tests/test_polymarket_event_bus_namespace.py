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
