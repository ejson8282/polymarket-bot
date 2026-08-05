from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
from typing import Any

from platforms.predictfun.maker.executor import (
    AccountBalance,
    AccountPosition,
    LiveOrder,
)


SCHEMA_VERSION = 2


def build_status_snapshot(
    *,
    cfg: dict[str, Any],
    runner_state: dict[str, Any],
    plan_state: dict[str, Any],
    intents_state: dict[str, Any],
    execution_state: dict[str, Any],
    risk_state: dict[str, Any],
    simulation_state: dict[str, Any],
    research_state: dict[str, Any],
    ws_state: dict[str, Any],
    live_orders: list[LiveOrder] | None = None,
    live_balances: list[AccountBalance] | None = None,
    live_positions: list[AccountPosition] | None = None,
) -> dict[str, Any]:
    deployment = _object(cfg.get("deployment"))
    data_cfg = _object(cfg.get("data"))
    account_ids = _configured_accounts(cfg)
    plans = [_plan_row(row) for row in _rows(plan_state.get("plans"))]
    desired_orders = [_order_row(row) for row in _rows(intents_state.get("intents"))]
    active_orders = [
        _order_row(row) for row in _rows(simulation_state.get("active_orders"))
    ]
    positions = [
        _position_row(row) for row in _rows(simulation_state.get("positions"))
    ]
    recent_actions = [
        _action_row(row) for row in _rows(execution_state.get("results"))[-50:]
    ]
    risk_checks = [_risk_row(row) for row in _rows(risk_state.get("checks"))]
    live_order_rows = [_live_order_row(row) for row in live_orders or []]
    live_balance_rows = [_live_balance_row(row) for row in live_balances or []]
    live_position_rows = [
        _live_position_row(row) for row in live_positions or []
    ]

    ws_max_age = float(data_cfg.get("ws_state_max_age_sec") or 30)
    ws_age = _age_sec(ws_state.get("last_message_at"))
    ws_errors = _object(ws_state.get("orderbook_errors"))
    ws_error_count = sum(
        1 for value in ws_errors.values() if str(value or "")
    )
    ws_transport_healthy = (
        ws_state.get("connected") is True
        and not str(ws_state.get("error") or "")
        and ws_age is not None
        and ws_age <= ws_max_age
        and bool(_object(ws_state.get("orderbooks")))
    )
    ws_healthy = ws_transport_healthy and ws_error_count == 0
    auth = _object(runner_state.get("last_auth_summary"))
    auth_rows = [
        {
            "account_id": str(row.get("account_id") or ""),
            "ok": row.get("ok") is True,
            "status": _integer(row.get("status")),
            "error": str(row.get("error") or ""),
        }
        for row in _rows(auth.get("accounts"))
    ]
    signer_ok = (
        auth.get("enabled") is True
        and auth.get("ok") is True
        and bool(auth_rows)
        and all(row["ok"] for row in auth_rows)
    )
    risk_blocked = risk_state.get("hard_blocked") is True
    execution_gate = _object(runner_state.get("execution_gate"))
    gate_blocked = bool(_list(execution_gate.get("blocks")))
    runner_error = str(runner_state.get("last_error") or "")
    account_reads = _object(runner_state.get("account_reads"))
    account_read_rows = _object(account_reads.get("accounts"))
    account_read_errors = [
        {
            "account_id": str(account_id),
            "source": source,
            "error": str(read.get("error") or ""),
        }
        for account_id, raw_sources in account_read_rows.items()
        if isinstance(raw_sources, dict)
        for source, read in raw_sources.items()
        if isinstance(read, dict) and read.get("ok") is not True
    ]
    if gate_blocked or risk_blocked or (
        bool(data_cfg.get("require_ws_for_quotes"))
        and not ws_transport_healthy
    ):
        health_status = "blocked"
    elif runner_error or not signer_ok or ws_error_count or account_read_errors:
        health_status = "attention"
    else:
        health_status = "healthy"

    intent_summary = _object(intents_state.get("summary"))
    simulation_summary = _object(simulation_state.get("summary"))
    research_summary = _object(research_state.get("summary"))
    execution_mode = str(runner_state.get("mode") or "dry_run")
    requested_mode = str(
        runner_state.get("requested_mode") or execution_mode
    )
    simulation_cfg = _object(cfg.get("simulation"))
    simulation_enabled = (
        execution_mode == "dry_run" and simulation_cfg.get("enabled") is not False
    )
    if not simulation_enabled:
        active_orders = []
        positions = []
        simulation_summary = {}
    runtime_capabilities = _object(runner_state.get("capabilities"))
    latencies = sorted(
        number
        for value in _object(ws_state.get("orderbook_latency_ms")).values()
        if (number := _number(value)) is not None
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "ts": _utc_now(),
        "project": "predictfun",
        "deployment": {
            "profile": str(
                deployment.get("profile")
                or runner_state.get("deployment_profile")
                or ""
            ),
            "account_id": str(
                deployment.get("account_id")
                or (account_ids[0] if len(account_ids) == 1 else "")
            ),
            "account_ids": account_ids,
            "release_sha": str(runner_state.get("release_sha") or ""),
            "release_required": runner_state.get("release_required") is True,
            "mode": execution_mode,
            "requested_mode": requested_mode,
            "execution_gate": _object(runner_state.get("execution_gate")),
        },
        "health": {
            "status": health_status,
            "runner": {
                "running": runner_state.get("running") is True,
                "cycle_count": _integer(runner_state.get("cycle_count")),
                "error_count": _integer(runner_state.get("error_count")),
                "last_error": runner_error,
                "last_cycle_finished_at": str(
                    runner_state.get("last_cycle_finished_at") or ""
                ),
                "last_cycle_age_sec": _age_sec(
                    runner_state.get("last_cycle_finished_at")
                ),
            },
            "websocket": {
                "healthy": ws_healthy,
                "transport_healthy": ws_transport_healthy,
                "connected": ws_state.get("connected") is True,
                "last_message_at": str(ws_state.get("last_message_at") or ""),
                "last_message_age_sec": ws_age,
                "max_age_sec": ws_max_age,
                "session_number": _integer(ws_state.get("session_number")),
                "reconnect_count": _integer(ws_state.get("reconnect_count")),
                "market_count": len(_list(ws_state.get("market_ids"))),
                "book_count": len(_object(ws_state.get("orderbooks"))),
                "error_count": ws_error_count,
                "last_error": str(ws_state.get("error") or ""),
                "max_latency_ms": max(latencies) if latencies else None,
            },
            "signer": {"ok": signer_ok, "accounts": auth_rows},
            "risk": {
                "status": str(risk_state.get("status") or "UNKNOWN"),
                "execution_mode": str(
                    risk_state.get("execution_mode") or "blocked"
                ),
                "blocked": risk_state.get("blocked") is True,
                "hard_blocked": risk_blocked,
            },
            "execution_gate": execution_gate,
            "account_reads": {
                "ok": bool(account_read_rows) and not account_read_errors,
                "ts": str(account_reads.get("ts") or ""),
                "errors": account_read_errors,
            },
        },
        "overview": {
            "markets": len(plans),
            "quotable_markets": sum(1 for row in plans if row["can_quote"]),
            "desired_orders": len(desired_orders),
            "simulated_active_orders": len(active_orders),
            "simulated_positions": len(positions),
            "simulated_fills": _integer(simulation_summary.get("fills_total")),
            "desired_notional": _decimal_text(intent_summary.get("total_notional")),
            "simulated_unrealized_pnl": _decimal_text(
                simulation_summary.get("unrealized_pnl")
            ),
            "live_active_orders": sum(
                1 for row in live_order_rows if row["status"] == "open"
            ),
            "live_positions": len(live_position_rows),
            "live_balance": _decimal_text(
                sum(
                    (Decimal(row["total"]) for row in live_balance_rows),
                    Decimal("0"),
                )
            ),
            "live_position_value": _decimal_text(
                sum(
                    (Decimal(row["value_usd"]) for row in live_position_rows),
                    Decimal("0"),
                )
            ),
            "scanner_markets": _integer(research_summary.get("markets")),
            "scanner_tradable_now": _integer(
                research_summary.get("tradable_now")
            ),
            "scanner_watchlist": _integer(research_summary.get("watchlist")),
        },
        "markets": plans,
        "desired_orders": desired_orders,
        "simulated_active_orders": active_orders,
        "simulated_positions": positions,
        "live_orders": live_order_rows,
        "live_balances": live_balance_rows,
        "live_positions": live_position_rows,
        "accounts": _account_rows(
            account_ids=account_ids,
            orders=live_order_rows,
            balances=live_balance_rows,
            positions=live_position_rows,
            capital_profiles=_object(runner_state.get("capital_profiles")),
            capabilities=_object(runtime_capabilities.get("accounts")),
        ),
        "recent_actions": recent_actions,
        "risk_checks": risk_checks,
        "capabilities": {
            "public_market_websocket": True,
            "rest_market_scan": True,
            "account_auth_check": True,
            "dry_run_planning": True,
            "simulated_fills": simulation_enabled,
            "live_order_submit": runtime_capabilities.get("live_order_submit")
            is True,
            "live_order_cancel": runtime_capabilities.get("live_order_cancel")
            is True,
            "live_order_read": runtime_capabilities.get("live_order_read")
            is True,
            "live_balance_read": runtime_capabilities.get("live_balance_read")
            is True,
            "live_position_read": runtime_capabilities.get("live_position_read")
            is True,
            "live_fill_stream": False,
        },
        "sources": {
            "runner": str(runner_state.get("ts") or ""),
            "plans": str(plan_state.get("ts") or ""),
            "intents": str(intents_state.get("ts") or ""),
            "execution": str(execution_state.get("ts") or ""),
            "risk": str(risk_state.get("ts") or ""),
            "simulation": str(simulation_state.get("ts") or ""),
            "research": str(research_state.get("ts") or ""),
            "websocket": str(ws_state.get("ts") or ""),
            "account_reads": str(account_reads.get("ts") or ""),
        },
    }


def _plan_row(row: dict[str, Any]) -> dict[str, Any]:
    market = _object(row.get("market"))
    return {
        "market_id": _integer(market.get("id")),
        "title": str(market.get("title") or market.get("question") or ""),
        "yes_label": str(market.get("yes_label") or "YES"),
        "no_label": str(market.get("no_label") or "NO"),
        "status": str(market.get("status") or ""),
        "trading_status": str(market.get("trading_status") or ""),
        "market_variant": str(market.get("market_variant") or ""),
        "hourly_rate": _decimal_text(market.get("hourly_rate")),
        "score": _decimal_text(market.get("score")),
        "risk_note": str(market.get("risk_note") or ""),
        "can_quote": row.get("can_quote") is True,
        "skip_reason": str(row.get("skip_reason") or ""),
        "orderbook_source": str(row.get("orderbook_source") or ""),
        "best_yes_bid": _decimal_text(row.get("best_yes_bid")),
        "best_yes_ask": _decimal_text(row.get("best_yes_ask")),
        "mid": _decimal_text(row.get("mid")),
        "yes_quote_count": len(_list(row.get("yes_quotes"))),
        "no_quote_count": len(_list(row.get("no_quotes"))),
    }


def _order_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "intent_id": str(row.get("intent_id") or ""),
        "account_id": str(row.get("account_id") or ""),
        "market_id": _integer(row.get("market_id")),
        "outcome": str(row.get("outcome") or ""),
        "side": str(row.get("side") or ""),
        "price": _decimal_text(row.get("price")),
        "size": _decimal_text(row.get("size")),
        "notional": _decimal_text(row.get("notional")),
        "purpose": str(row.get("purpose") or ""),
    }


def _position_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "account_id": str(row.get("account_id") or ""),
        "market_id": _integer(row.get("market_id")),
        "outcome": str(row.get("outcome") or ""),
        "size": _decimal_text(row.get("size")),
        "cost": _decimal_text(row.get("cost")),
        "marked_value": _decimal_text(row.get("marked_value")),
        "unrealized_pnl": _decimal_text(row.get("unrealized_pnl")),
        "source": "simulation",
    }


def _live_order_row(row: LiveOrder) -> dict[str, Any]:
    return {
        "order_id": row.order_id,
        "external_order_id": row.external_order_id,
        "intent_id": row.intent_id,
        "account_id": row.account_id,
        "market_id": row.market_id,
        "outcome": row.outcome,
        "side": row.side,
        "price": _decimal_text(row.price),
        "size": _decimal_text(row.size),
        "filled_size": _decimal_text(row.filled_size),
        "status": str(row.status or "").lower(),
        "purpose": row.purpose,
        "token_id": row.token_id,
    }


def _live_balance_row(row: AccountBalance) -> dict[str, Any]:
    return {
        "account_id": row.account_id,
        "asset": row.asset,
        "available": _decimal_text(row.available),
        "total": _decimal_text(row.total),
    }


def _live_position_row(row: AccountPosition) -> dict[str, Any]:
    return {
        "account_id": row.account_id,
        "market_id": row.market_id,
        "outcome": row.outcome,
        "size": _decimal_text(row.size),
        "avg_price": _decimal_text(row.avg_price),
        "mark_price": _decimal_text(row.mark_price),
        "value_usd": _decimal_text(row.value_usd),
        "pnl_usd": _decimal_text(row.pnl_usd),
        "source": "live",
    }


def _account_rows(
    *,
    account_ids: list[str],
    orders: list[dict[str, Any]],
    balances: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    capital_profiles: dict[str, Any],
    capabilities: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for account_id in account_ids:
        account_orders = [
            row for row in orders if row.get("account_id") == account_id
        ]
        account_positions = [
            row for row in positions if row.get("account_id") == account_id
        ]
        total = sum(
            (
                Decimal(str(row.get("total") or "0"))
                for row in balances
                if row.get("account_id") == account_id
            ),
            Decimal("0"),
        )
        position_value = sum(
            (
                Decimal(str(row.get("value_usd") or "0"))
                for row in account_positions
            ),
            Decimal("0"),
        )
        position_pnl = sum(
            (
                Decimal(str(row.get("pnl_usd") or "0"))
                for row in account_positions
            ),
            Decimal("0"),
        )
        rows.append(
            {
                "account_id": account_id,
                "balance": _decimal_text(total),
                "position_value": _decimal_text(position_value),
                "equity": _decimal_text(total + position_value),
                "position_pnl": _decimal_text(position_pnl),
                "open_orders": sum(
                    1 for row in account_orders if row.get("status") == "open"
                ),
                "positions": len(account_positions),
                "capital": _object(capital_profiles.get(account_id)),
                "capabilities": _object(capabilities.get(account_id)),
            }
        )
    return rows


def _action_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "intent_id": str(row.get("intent_id") or ""),
        "account_id": str(row.get("account_id") or ""),
        "action": str(row.get("action") or ""),
        "ok": row.get("ok") is True,
        "status": str(row.get("status") or ""),
        "message": str(row.get("message") or ""),
        "order_id": str(row.get("order_id") or ""),
    }


def _risk_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": str(row.get("name") or ""),
        "status": str(row.get("status") or ""),
        "value": row.get("value"),
        "limit": row.get("limit"),
        "detail": str(row.get("detail") or ""),
        "block_scope": str(row.get("block_scope") or ""),
    }


def _configured_accounts(cfg: dict[str, Any]) -> list[str]:
    accounts = cfg.get("accounts")
    if isinstance(accounts, dict):
        rows: list[str] = []
        for value in _list(accounts.get("ids")):
            if isinstance(value, dict):
                if value.get("enabled") is False:
                    continue
                account_id = str(
                    value.get("account_id")
                    or value.get("id")
                    or value.get("name")
                    or ""
                ).strip()
            else:
                account_id = str(value or "").strip()
            if account_id and account_id not in rows:
                rows.append(account_id)
        return rows
    return []


def _rows(value: Any) -> list[dict[str, Any]]:
    return [row for row in _list(value) if isinstance(row, dict)]


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _integer(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _decimal_text(value: Any) -> str:
    try:
        number = Decimal(str(value or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return "0"
    if not number.is_finite():
        return "0"
    return format(number, "f")


def _age_sec(value: Any) -> float | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return round(
        max(0.0, (datetime.now(timezone.utc) - parsed).total_seconds()),
        3,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
