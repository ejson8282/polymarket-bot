from pathlib import Path

import pytest

from platforms.polymarket.maker.order_scoring_observer import (
    OrderScoringObserver,
    normalize_scoring_response,
)


EPOCH = 1_800_000_000.0


def _order(order_id: str = "order-1", created_at: float = EPOCH) -> dict:
    return {
        "id": order_id,
        "asset_id": "101",
        "side": "BUY",
        "price": "0.48",
        "original_size": "100",
        "created_at": created_at,
    }


def test_natural_order_records_first_confirmed_scoring_checkpoint(tmp_path: Path) -> None:
    observer = OrderScoringObserver(tmp_path / "scoring.json")
    answers = iter([{"scoring": False}, {"scoring": False}, {"scoring": True}])

    observer.poll([_order()], lambda _oid: next(answers), now=EPOCH)
    observer.poll([_order()], lambda _oid: next(answers), now=EPOCH + 10)
    observer.poll([_order()], lambda _oid: next(answers), now=EPOCH + 30)
    state = observer.poll([_order()], lambda _oid: next(answers), now=EPOCH + 60)

    row = state["orders"]["order-1"]
    assert [item["checkpoint_sec"] for item in row["observations"]] == [10, 30, 60]
    assert row["first_scoring_age_sec"] == 60
    assert state["summary"]["earliest_confirmed_scoring_sec"] == 60


def test_query_failure_is_retried_without_consuming_checkpoint(tmp_path: Path) -> None:
    observer = OrderScoringObserver(tmp_path / "scoring.json")
    observer.poll([_order()], lambda _oid: {"scoring": False}, now=EPOCH)

    def fail(_order_id: str):
        raise TimeoutError("read timed out")

    failed = observer.poll([_order()], fail, now=EPOCH + 10)
    assert failed["orders"]["order-1"]["observations"] == []
    assert failed["orders"]["order-1"]["query_failures"] == 1

    recovered = observer.poll(
        [_order()], lambda _oid: {"scoring": False}, now=EPOCH + 12
    )
    assert recovered["orders"]["order-1"]["observations"][0][
        "checkpoint_sec"
    ] == 10


def test_disappeared_order_is_closed_without_another_query(tmp_path: Path) -> None:
    observer = OrderScoringObserver(tmp_path / "scoring.json")
    calls = []
    observer.poll([_order()], lambda oid: calls.append(oid), now=EPOCH)
    state = observer.poll([], lambda oid: calls.append(oid), now=EPOCH + 15)

    assert calls == []
    assert state["orders"]["order-1"]["live"] is False
    assert state["orders"]["order-1"]["closed_at"] == EPOCH + 15


def test_old_order_does_not_backfill_earlier_scoring_status(tmp_path: Path) -> None:
    observer = OrderScoringObserver(tmp_path / "scoring.json")
    calls = []
    state = observer.poll(
        [_order(created_at=EPOCH)],
        lambda oid: calls.append(oid) or {"scoring": True},
        now=EPOCH + 100,
    )

    assert calls == []
    observations = state["orders"]["order-1"]["observations"]
    assert [row["checkpoint_sec"] for row in observations] == [10, 30, 60]
    assert all(row["status"] == "missed_before_observer" for row in observations)
    assert state["orders"]["order-1"]["first_scoring_age_sec"] is None


def test_state_survives_restart_and_rejects_invalid_payload(tmp_path: Path) -> None:
    path = tmp_path / "scoring.json"
    observer = OrderScoringObserver(path)
    observer.poll([_order()], lambda _oid: {"scoring": False}, now=EPOCH)
    observer.poll([_order()], lambda _oid: {"scoring": True}, now=EPOCH + 10)

    restored = OrderScoringObserver(path)
    assert restored.public_state(EPOCH + 11)["orders"]["order-1"][
        "first_scoring_age_sec"
    ] == 10

    path.write_text("[]", encoding="utf-8")
    assert OrderScoringObserver(path).public_state(EPOCH)["orders"] == {}


@pytest.mark.parametrize("payload", [None, {}, {"scoring": "true"}, []])
def test_invalid_official_response_fails_closed(payload) -> None:
    with pytest.raises(ValueError):
        normalize_scoring_response(payload)


def test_state_file_contains_no_credentials(tmp_path: Path) -> None:
    path = tmp_path / "scoring.json"
    observer = OrderScoringObserver(path)
    observer.poll([_order()], lambda _oid: {"scoring": False}, now=EPOCH)
    text = path.read_text(encoding="utf-8").lower()

    assert "api_secret" not in text
    assert "api_passphrase" not in text
    assert "private_key" not in text
