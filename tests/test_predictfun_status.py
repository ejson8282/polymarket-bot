from __future__ import annotations

from datetime import datetime, timezone
import json

from platforms.predictfun.maker.status import build_status_snapshot


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _healthy_snapshot() -> dict:
    now = _now()
    return build_status_snapshot(
        cfg={
            "deployment": {"profile": "vps1", "account_id": "account_01"},
            "accounts": {"ids": ["account_01"]},
            "data": {
                "require_ws_for_quotes": True,
                "ws_state_max_age_sec": 5,
            },
        },
        runner_state={
            "ts": now,
            "running": False,
            "cycle_count": 3,
            "error_count": 0,
            "last_error": "",
            "last_cycle_finished_at": now,
            "release_sha": "a" * 40,
            "release_required": True,
            "last_auth_summary": {
                "enabled": True,
                "ok": True,
                "accounts": [
                    {"account_id": "account_01", "ok": True, "status": 200}
                ],
            },
        },
        plan_state={
            "ts": now,
            "plans": [
                {
                    "market": {
                        "id": 58416,
                        "title": "Will Solana hit $60 or $140 first?",
                        "yes_label": "$60",
                        "no_label": "$140",
                        "status": "REGISTERED",
                        "trading_status": "OPEN",
                        "market_variant": "BINARY",
                        "hourly_rate": "10",
                        "score": "1.25",
                    },
                    "can_quote": True,
                    "orderbook_source": "ws",
                    "best_yes_bid": "0.40",
                    "best_yes_ask": "0.60",
                    "mid": "0.50",
                    "yes_quotes": [{"price": "0.41"}],
                    "no_quotes": [{"price": "0.39"}],
                }
            ],
        },
        intents_state={
            "ts": now,
            "summary": {"total_notional": "8.00"},
            "intents": [
                {
                    "intent_id": "intent-1",
                    "account_id": "account_01",
                    "market_id": 58416,
                    "outcome": "YES",
                    "side": "BUY",
                    "price": "0.40",
                    "size": "20",
                    "notional": "8",
                    "purpose": "maker_quote",
                }
            ],
        },
        execution_state={
            "ts": now,
            "results": [
                {
                    "intent_id": "intent-1",
                    "account_id": "account_01",
                    "action": "create",
                    "ok": True,
                    "status": "simulated",
                }
            ],
        },
        risk_state={
            "ts": now,
            "status": "OK",
            "execution_mode": "normal",
            "blocked": False,
            "hard_blocked": False,
            "checks": [{"name": "ws_connected", "status": "OK"}],
        },
        simulation_state={
            "ts": now,
            "summary": {
                "fills_total": 2,
                "unrealized_pnl": "0.25",
            },
            "active_orders": [],
            "positions": [],
        },
        research_state={
            "ts": now,
            "summary": {"markets": 20, "tradable_now": 4, "watchlist": 6},
        },
        ws_state={
            "ts": now,
            "connected": True,
            "last_message_at": now,
            "session_number": 2,
            "reconnect_count": 1,
            "market_ids": [58416],
            "orderbooks": {"58416": {"bids": [], "asks": []}},
            "orderbook_errors": {},
            "orderbook_latency_ms": {"58416": 25},
        },
    )


def test_status_contract_matches_poly_style_sections_without_claiming_live() -> None:
    status = _healthy_snapshot()

    assert status["schema_version"] == 2
    assert status["deployment"]["profile"] == "vps1"
    assert status["deployment"]["account_id"] == "account_01"
    assert status["health"]["status"] == "healthy"
    assert status["health"]["websocket"]["healthy"] is True
    assert status["overview"] == {
        "markets": 1,
        "quotable_markets": 1,
        "desired_orders": 1,
        "simulated_active_orders": 0,
        "simulated_positions": 0,
        "simulated_fills": 2,
        "desired_notional": "8.00",
        "simulated_unrealized_pnl": "0.25",
        "live_active_orders": 0,
        "live_positions": 0,
        "live_balance": "0",
        "live_position_value": "0",
        "scanner_markets": 20,
        "scanner_tradable_now": 4,
        "scanner_watchlist": 6,
    }
    assert status["markets"][0]["yes_label"] == "$60"
    assert status["markets"][0]["no_label"] == "$140"
    assert status["capabilities"]["live_order_submit"] is False
    assert status["capabilities"]["live_order_cancel"] is False
    assert status["capabilities"]["simulated_fills"] is True


def test_required_ws_failure_is_visible_as_blocked() -> None:
    status = _healthy_snapshot()
    status = build_status_snapshot(
        cfg={
            "accounts": {"ids": ["account_02"]},
            "data": {
                "require_ws_for_quotes": True,
                "ws_state_max_age_sec": 5,
            },
        },
        runner_state={
            "last_auth_summary": {
                "enabled": True,
                "ok": True,
                "accounts": [{"account_id": "account_02", "ok": True}],
            }
        },
        plan_state={},
        intents_state={},
        execution_state={},
        risk_state={},
        simulation_state={},
        research_state={},
        ws_state={"connected": False, "error": "relay unavailable"},
    )

    assert status["deployment"]["account_id"] == "account_02"
    assert status["health"]["status"] == "blocked"
    assert status["health"]["websocket"]["healthy"] is False
    assert status["health"]["websocket"]["last_error"] == "relay unavailable"


def test_isolated_book_error_is_attention_not_global_block() -> None:
    now = _now()
    status = build_status_snapshot(
        cfg={
            "deployment": {
                "profile": "vps1",
                "account_id": "account_01",
            },
            "accounts": {"ids": ["account_01"]},
            "data": {
                "require_ws_for_quotes": True,
                "ws_state_max_age_sec": 30,
            },
        },
        runner_state={
            "release_sha": "a" * 40,
            "release_required": True,
            "last_auth_summary": {
                "enabled": True,
                "ok": True,
                "accounts": [
                    {"account_id": "account_01", "ok": True, "status": 200}
                ],
            },
        },
        plan_state={"plans": []},
        intents_state={"intents": [], "summary": {}},
        execution_state={"results": []},
        risk_state={"status": "WARN", "execution_mode": "normal"},
        simulation_state={"active_orders": [], "positions": [], "summary": {}},
        research_state={"summary": {}},
        ws_state={
            "connected": True,
            "last_message_at": now,
            "orderbooks": {"42": {"bids": [], "asks": []}},
            "orderbook_errors": {"43": "orderbook_crossed"},
        },
    )

    assert status["health"]["status"] == "attention"
    assert status["health"]["websocket"]["transport_healthy"] is True
    assert status["health"]["websocket"]["healthy"] is False
    assert status["health"]["websocket"]["error_count"] == 1


def test_failed_account_read_is_attention_and_not_presented_as_available() -> None:
    now = _now()
    status = build_status_snapshot(
        cfg={
            "accounts": {"ids": ["account_01"]},
            "data": {"require_ws_for_quotes": True},
        },
        runner_state={
            "mode": "dry_run",
            "last_auth_summary": {
                "enabled": True,
                "ok": True,
                "accounts": [{"account_id": "account_01", "ok": True}],
            },
            "capabilities": {
                "live_balance_read": False,
                "live_order_read": True,
                "live_position_read": True,
            },
            "account_reads": {
                "ts": now,
                "accounts": {
                    "account_01": {
                        "orders": {"ok": True, "count": 0, "error": ""},
                        "balances": {
                            "ok": False,
                            "count": 0,
                            "error": "RuntimeError: unavailable",
                        },
                        "positions": {"ok": True, "count": 0, "error": ""},
                    }
                },
            },
        },
        plan_state={},
        intents_state={},
        execution_state={},
        risk_state={"status": "OK", "execution_mode": "normal"},
        simulation_state={},
        research_state={},
        ws_state={
            "connected": True,
            "last_message_at": now,
            "orderbooks": {"42": {"bids": [], "asks": []}},
        },
    )

    assert status["health"]["status"] == "attention"
    assert status["health"]["account_reads"]["ok"] is False
    assert status["capabilities"]["live_balance_read"] is False
    assert status["overview"]["live_balance"] == "0"
    assert status["sources"]["account_reads"] == now


def test_status_snapshot_is_json_safe_and_contains_no_secret_fields() -> None:
    encoded = json.dumps(_healthy_snapshot())
    lowered = encoded.lower()
    assert "private_key" not in lowered
    assert "api_key" not in lowered
    assert "bearer" not in lowered
    assert "jwt" not in lowered
