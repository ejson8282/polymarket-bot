from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from platforms.polymarket.maker.read_only_observer import (
    build_reference_plan,
    normalize_book,
    observe_once,
)


NOW = datetime(2026, 7, 13, 10, 0, tzinfo=timezone.utc)


def _book(bid: str = "0.48", ask: str = "0.52") -> dict:
    return {
        "bids": [
            {"price": "0.40", "size": "100"},
            {"price": bid, "size": "20"},
        ],
        "asks": [
            {"price": "0.70", "size": "100"},
            {"price": ask, "size": "30"},
        ],
    }


def _config(path: Path, token: str, *, secret: str = "must-not-leak") -> None:
    path.write_text(
        json.dumps(
            {
                "private_key": secret,
                "remote_signer": {"token": secret},
                "rest_base_url": "https://clob.polymarket.com",
                "strategy": {
                    "default_price_tick": 0.01,
                    "min_distance_ticks": 1,
                },
                "markets": [
                    {
                        "enabled": True,
                        "token_id": token,
                        "price_tick": 0.01,
                        "max_incentive_spread": 0.05,
                        "quote_size": 20,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_normalize_book_sorts_levels_and_rejects_crossed_books() -> None:
    book = normalize_book(_book())

    assert str(book["best_bid"]) == "0.48"
    assert str(book["best_ask"]) == "0.52"
    assert str(book["mid"]) == "0.50"

    crossed = _book(bid="0.60", ask="0.50")
    try:
        normalize_book(crossed)
    except ValueError as error:
        assert "crossed" in str(error)
    else:
        raise AssertionError("crossed book must be rejected")


def test_regular_reference_plan_uses_public_book_and_configured_size() -> None:
    plan = build_reference_plan(
        book=normalize_book(_book()),
        market={
            "price_tick": "0.01",
            "max_incentive_spread": "0.05",
            "quote_size": "20",
        },
        strategy={"min_distance_ticks": 1},
    )

    assert plan == [
        {"price": "0.47", "quantity": "20"},
        {"price": "0.46", "quantity": "20"},
        {"price": "0.45", "quantity": "20"},
    ]


def test_fine_tick_reference_plan_spreads_levels_inside_reward_zone() -> None:
    plan = build_reference_plan(
        book=normalize_book(_book(bid="0.501", ask="0.503")),
        market={
            "price_tick": "0.001",
            "max_incentive_spread": "0.01",
            "quote_size": "10",
        },
        strategy={
            "min_distance_ticks": 1,
            "fine_tick_max_legs": 5,
            "fine_tick_zone_use_pct": "0.50",
        },
    )

    assert [row["price"] for row in plan] == ["0.5", "0.499", "0.498", "0.497", "0.496"]


def test_observer_writes_independent_sanitized_account_states(tmp_path: Path) -> None:
    first = tmp_path / "config_1.json"
    second = tmp_path / "config_2.json"
    _config(first, "token-one")
    _config(second, "token-two")

    calls = []

    def fetch(host: str, token: str, timeout: float) -> dict:
        calls.append((host, token, timeout))
        return _book()

    status = observe_once(
        config_paths=[first, second],
        output_dir=tmp_path / "data",
        fetch_book=fetch,
        now=NOW,
    )

    state_one = json.loads((tmp_path / "data" / "polymarket_observer_state_1.json").read_text())
    state_two = json.loads((tmp_path / "data" / "polymarket_observer_state_2.json").read_text())
    combined = json.dumps({"status": status, "one": state_one, "two": state_two})

    assert {row[1] for row in calls} == {"token-one", "token-two"}
    assert state_one["account_id"] == "pm-account-1"
    assert set(state_one["markets"]) == {"token-one"}
    assert state_two["account_id"] == "pm-account-2"
    assert set(state_two["markets"]) == {"token-two"}
    assert state_one["actual_orders_available"] is False
    assert state_one["markets"]["token-one"]["plan_kind"] == "reference_only"
    assert "must-not-leak" not in combined
    assert "private_key" not in combined
    assert "remote_signer" not in combined


def test_unchanged_public_state_only_updates_heartbeat(tmp_path: Path) -> None:
    config = tmp_path / "config_1.json"
    output = tmp_path / "data"
    _config(config, "token-one")

    first = observe_once(
        config_paths=[config],
        output_dir=output,
        fetch_book=lambda *_: _book(),
        now=NOW,
    )
    state_path = output / "polymarket_observer_state_1.json"
    first_state = state_path.read_text(encoding="utf-8")

    second = observe_once(
        config_paths=[config],
        output_dir=output,
        fetch_book=lambda *_: _book(),
        now=NOW + timedelta(minutes=1),
    )
    second_state = state_path.read_text(encoding="utf-8")
    heartbeat = json.loads((output / "polymarket_observer_status.json").read_text())

    assert first["accounts"][0]["state_updated"] is True
    assert second["accounts"][0]["state_updated"] is False
    assert first_state == second_state
    assert heartbeat["last_poll_at"].startswith("2026-07-13T10:01:00")
    assert heartbeat["accounts"][0]["last_state_at"].startswith("2026-07-13T10:00:00")


def test_observer_reports_one_public_book_failure_without_leaking_config(tmp_path: Path) -> None:
    config = tmp_path / "config_1.json"
    _config(config, "token-one")

    def fail(*_args) -> dict:
        raise RuntimeError("temporary public endpoint failure")

    status = observe_once(
        config_paths=[config],
        output_dir=tmp_path / "data",
        fetch_book=fail,
        now=NOW,
    )

    assert status["healthy"] is False
    assert status["summary"]["ready_markets"] == 0
    assert status["accounts"][0]["errors"][0]["token"] == "token-one"
    state = json.loads((tmp_path / "data" / "polymarket_observer_state_1.json").read_text())
    assert state["markets"] == {}
    assert state["summary"]["errors"] == 1
