from __future__ import annotations

from collections import UserDict
from decimal import Decimal
import importlib.util
import json
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

from platforms.predictfun.maker.capital import build_account_capital_rows
from platforms.predictfun.maker.dry_run import QuoteLevel, _filter_quotes
from platforms.predictfun.maker.execution_gate import resolve_execution_gate
from platforms.predictfun.maker.executor import (
    AccountBalance,
    AccountPosition,
    ExecutableOrder,
    ExecutionResult,
    LiveOrder,
    MultiAccountExecutor,
    PredictFunLiveExecutor,
    PredictFunReadOnlyExecutor,
)
from platforms.predictfun.maker.intents import _configured_accounts, utc_now
from platforms.predictfun.maker.intents import build_intents_from_plans
from platforms.predictfun.maker import live_order_once
from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry
from platforms.predictfun.maker.risk import evaluate_risk
from platforms.predictfun.maker.runner import (
    _apply_configured_capital_caps,
    _apply_manual_order_constraints,
    _cancel_managed_on_shutdown,
    _mark_runner_error,
    _mark_runner_success,
    _runtime_read_capabilities,
    _sync_managed_live_orders,
    _ws_quote_fingerprint,
    run_loop,
)
from platforms.predictfun.maker.status import build_status_snapshot


def _live_cfg() -> dict[str, Any]:
    return {
        "environment": "mainnet",
        "execution": {"mode": "live"},
        "accounts": {"ids": ["account_01"]},
        "simulation": {"enabled": False},
    }


def _live_env() -> dict[str, str]:
    sha = "a" * 40
    return {
        "PREDICTFUN_LIVE_TRADING": "1",
        "PREDICTFUN_LIVE_CONFIRM": "ENABLE_PREDICTFUN_LIVE",
        "PREDICTFUN_LIVE_RELEASE_SHA": sha,
        "PREDICTFUN_LIVE_ACCOUNT_IDS": "account_01",
    }


def test_runner_error_streak_recovers_without_losing_lifetime_audit(
    capsys: pytest.CaptureFixture[str],
) -> None:
    state: dict[str, Any] = {
        "error_count": 52,
        "consecutive_error_count": 0,
        "last_error": "",
    }

    _mark_runner_error(state, TimeoutError("proxy timeout"))
    _mark_runner_error(state, ConnectionError("proxy unavailable"))

    assert state["error_count"] == 54
    assert state["consecutive_error_count"] == 2
    assert state["last_error"] == "ConnectionError: proxy unavailable"
    assert state["last_error_at"]

    _mark_runner_success(state)

    assert state["error_count"] == 54
    assert state["consecutive_error_count"] == 0
    assert state["last_error"] == ""
    assert state["last_success_at"]
    events = [
        json.loads(line)["event"]
        for line in capsys.readouterr().err.splitlines()
    ]
    assert events == [
        "predictfun_runner_error",
        "predictfun_runner_error",
        "predictfun_runner_recovered",
    ]


def test_live_execution_gate_requires_exact_release_account_set_and_no_simulation() -> None:
    release = {"release_sha": "a" * 40, "release_required": True}
    gate = resolve_execution_gate(_live_cfg(), environ=_live_env(), release=release)
    assert gate.allowed is True
    assert gate.effective_mode == "live"

    wrong_accounts = {**_live_env(), "PREDICTFUN_LIVE_ACCOUNT_IDS": "account_02"}
    gate = resolve_execution_gate(
        _live_cfg(), environ=wrong_accounts, release=release
    )
    assert gate.allowed is False
    assert "live_account_set_mismatch" in gate.blocks

    cfg = _live_cfg()
    cfg["simulation"] = {"enabled": True}
    gate = resolve_execution_gate(cfg, environ=_live_env(), release=release)
    assert "live_requires_simulation_disabled" in gate.blocks


def test_live_capital_never_invents_fallback_equity() -> None:
    rows = build_account_capital_rows(
        ["account_01"],
        balances=[],
        positions=[],
        fallback_equity=Decimal("100"),
        allow_fallback=False,
    )
    assert rows == [
        {
            "account_id": "account_01",
            "enabled": False,
            "quote_enabled": False,
            "equity": "0",
            "available_cash": "0",
            "capital_profile": "unavailable",
            "capital_source": "missing",
            "disabled_reason": "equity_unavailable",
            "reserve": "0",
            "max_account_notional": "0",
            "max_account_market_notional": "0",
            "max_order_notional": "0",
            "max_markets": 0,
            "inventory_warning": "0",
            "daily_loss_stop": "0",
        }
    ]
    assert _configured_accounts({"ids": rows, "max_active_accounts": 1}) == []
    assert build_intents_from_plans(
        [
            {
                "can_quote": True,
                "market": {"id": 42},
                "yes_quotes": [
                    {
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.4",
                        "size": "2",
                    }
                ],
            }
        ],
        accounts_config={"ids": rows, "max_active_accounts": 1},
    ) == []


def test_capital_quotes_are_capped_by_observed_available_cash() -> None:
    rows = build_account_capital_rows(
        ["account_01"],
        balances=[
            AccountBalance(
                asset="USDT",
                available=Decimal("5"),
                total=Decimal("5"),
                account_id="account_01",
            )
        ],
        positions=[
            AccountPosition(
                market_id=42,
                outcome="YES",
                size=Decimal("100"),
                avg_price=Decimal("0.5"),
                mark_price=Decimal("0.5"),
                account_id="account_01",
                value_usd=Decimal("50"),
            )
        ],
        allow_fallback=False,
    )
    assert rows[0]["enabled"] is True
    assert rows[0]["capital_source"] == "observed"
    assert Decimal(rows[0]["max_account_notional"]) <= Decimal("5")


def test_position_only_account_keeps_exit_order_limit_without_quoting() -> None:
    rows = build_account_capital_rows(
        ["account_01"],
        balances=[
            AccountBalance(
                asset="USDT",
                available=Decimal("0"),
                total=Decimal("0"),
                account_id="account_01",
            )
        ],
        positions=[
            AccountPosition(
                market_id=42,
                outcome="YES",
                size=Decimal("100"),
                avg_price=Decimal("0.5"),
                mark_price=Decimal("0.5"),
                account_id="account_01",
                value_usd=Decimal("100"),
            )
        ],
        allow_fallback=False,
    )

    assert rows[0]["enabled"] is True
    assert rows[0]["quote_enabled"] is False
    assert rows[0]["max_account_notional"] == "0"
    assert Decimal(rows[0]["max_order_notional"]) > 0


def test_multi_account_executor_applies_observed_account_order_limits() -> None:
    first = PredictFunLiveExecutor(
        signer_url="http://signer.invalid",
        account_id="account_01",
        max_order_notional=Decimal("8"),
    )
    second = PredictFunLiveExecutor(
        signer_url="http://signer.invalid",
        account_id="account_02",
        max_order_notional=Decimal("8"),
    )
    executor = MultiAccountExecutor(
        {"account_01": first, "account_02": second}
    )

    executor.set_order_notional_limits(
        {"account_01": Decimal("15"), "account_02": Decimal("5")}
    )

    assert first.max_order_notional == Decimal("15")
    assert second.max_order_notional == Decimal("5")


def test_limited_live_config_clamps_observed_capital_tier() -> None:
    rows = [
        {
            "account_id": "account_01",
            "max_account_notional": "14",
            "max_account_market_notional": "5",
            "max_order_notional": "1.6",
            "max_markets": 3,
            "inventory_warning": "6",
        }
    ]

    capped = _apply_configured_capital_caps(
        rows,
        {
            "scan": {"max_markets": 1},
            "strategy": {"max_order_notional": "1.60"},
            "risk": {
                "max_account_desired_notional": "1.60",
                "max_account_market_desired_notional": "1.60",
            },
        },
    )

    assert capped == [
        {
            "account_id": "account_01",
            "max_account_notional": "1.60",
            "max_account_market_notional": "1.60",
            "max_order_notional": "1.6",
            "max_markets": 1,
            "inventory_warning": "1.60",
        }
    ]
    assert rows[0]["max_account_notional"] == "14"


def test_limited_live_disables_new_buys_while_manual_buy_is_open() -> None:
    capped = _apply_configured_capital_caps(
        [
            {
                "account_id": "account_01",
                "quote_enabled": True,
                "max_account_notional": "14",
                "max_account_market_notional": "5",
                "max_order_notional": "1.6",
                "max_markets": 3,
                "inventory_warning": "6",
            }
        ],
        {
            "scan": {"max_markets": 1},
            "strategy": {"max_order_notional": "1.60"},
            "risk": {
                "max_account_desired_notional": "1.60",
                "max_account_market_desired_notional": "1.60",
            },
            "inventory": {
                "halt_new_buys_while_manual_buy_orders": True,
            },
        },
        manual_order_constraints={
            "account_01": {"manual_buy_reserved_notional": "0.50"}
        },
    )

    assert capped[0]["quote_enabled"] is False
    assert capped[0]["disabled_reason"] == "manual_buy_order_present"


def test_limited_live_resizes_large_reward_quote_to_static_cap() -> None:
    quotes = _filter_quotes(
        [
            QuoteLevel(
                outcome="YES",
                side="BUY",
                price=Decimal("0.40"),
                size=Decimal("100"),
                notional=Decimal("40"),
                reason="reward threshold",
            )
        ],
        min_order_notional=Decimal("0.90"),
        max_order_notional=Decimal("1.60"),
        resize_to_max_order_notional=True,
    )

    assert len(quotes) == 1
    assert quotes[0].size == Decimal("4.000000")
    assert quotes[0].notional == Decimal("1.60000000")
    assert "max_notional_adjusted=1.60" in quotes[0].reason


def test_limited_live_caps_produce_one_order_without_tripping_risk() -> None:
    cfg = {
        "accounts": {"ids": ["account_01"], "max_active_accounts": 1},
        "scan": {"max_markets": 1},
        "strategy": {"max_order_notional": "1.60"},
        "risk": {
            "max_plan_state_age_sec": 180,
            "max_total_desired_notional": "1.60",
            "max_account_desired_notional": "1.60",
            "max_account_market_desired_notional": "1.60",
            "max_market_desired_notional": "1.60",
        },
    }
    capital_rows = _apply_configured_capital_caps(
        [
            {
                "account_id": "account_01",
                "enabled": True,
                "quote_enabled": True,
                "max_account_notional": "14",
                "max_account_market_notional": "5",
                "max_order_notional": "1.6",
                "max_markets": 3,
                "inventory_warning": "6",
            }
        ],
        cfg,
    )
    plans = [
        {
            "can_quote": True,
            "market": {
                "id": 42,
                "decimal_precision": 2,
                "yes_token_id": "yes-token",
                "no_token_id": "no-token",
            },
            "yes_quotes": [
                {
                    "outcome": "YES",
                    "side": "BUY",
                    "price": "0.40",
                    "size": "10",
                }
            ],
            "no_quotes": [
                {
                    "outcome": "NO",
                    "side": "BUY",
                    "price": "0.60",
                    "size": "10",
                }
            ],
        }
    ]

    intents = build_intents_from_plans(
        plans,
        accounts_config={"ids": capital_rows, "max_active_accounts": 1},
        planner_config={
            "max_account_notional": "1.60",
            "max_account_market_notional": "1.60",
        },
    )
    intent_rows = [
        {
            **vars(intent),
            "price": str(intent.price),
            "size": str(intent.size),
            "notional": str(intent.notional),
        }
        for intent in intents
    ]

    assert len(intent_rows) == 1
    assert Decimal(intent_rows[0]["notional"]) <= Decimal("1.60")
    risk = evaluate_risk(
        cfg={**cfg, "accounts": {"ids": capital_rows, "max_active_accounts": 1}},
        plan_state={"ts": utc_now()},
        intents_state={
            "summary": {"total_notional": intent_rows[0]["notional"]},
            "intents": intent_rows,
        },
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={"positions": []},
        kill_switch_state={},
    )
    assert risk["blocked"] is False
    assert risk["execution_mode"] == "normal"


def test_limited_live_position_blocks_new_buys_in_another_market() -> None:
    intents = build_intents_from_plans(
        [
            {
                "can_quote": True,
                "market": {
                    "id": 42,
                    "yes_token_id": "yes-token",
                    "no_token_id": "no-token",
                },
                "yes_quotes": [
                    {
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.40",
                        "size": "4",
                    }
                ],
                "no_quotes": [],
            }
        ],
        accounts_config={
            "ids": [
                {
                    "account_id": "account_01",
                    "enabled": True,
                    "quote_enabled": True,
                    "max_account_notional": "1.60",
                    "max_account_market_notional": "1.60",
                    "max_order_notional": "1.60",
                    "max_markets": 1,
                }
            ],
            "max_active_accounts": 1,
        },
        inventory_positions=[
            {
                "account_id": "account_01",
                "market_id": 7,
                "outcome": "YES",
                "size": "2",
            }
        ],
        inventory_config={
            "enabled": True,
            "halt_all_buys_while_any_position": True,
        },
    )

    assert not [intent for intent in intents if intent.side == "BUY"]


def test_limited_live_combined_position_and_buy_risk_is_reduce_only() -> None:
    risk = evaluate_risk(
        cfg={
            "accounts": {"ids": ["account_01"], "max_active_accounts": 1},
            "risk": {
                "max_plan_state_age_sec": 180,
                "max_total_desired_notional": "1.60",
                "max_total_exposure_notional": "1.60",
            },
        },
        plan_state={"ts": utc_now()},
        intents_state={
            "summary": {"total_notional": "0.80"},
            "intents": [
                {
                    "account_id": "account_01",
                    "market_id": 42,
                    "outcome": "YES",
                    "side": "BUY",
                    "notional": "0.80",
                }
            ],
        },
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={},
        kill_switch_state={},
        inventory_state={
            "positions": [
                {
                    "account_id": "account_01",
                    "market_id": 7,
                    "outcome": "YES",
                    "size": "2",
                    "value_usd": "1.00",
                }
            ]
        },
        inventory_source="live",
    )

    assert risk["blocked"] is True
    assert risk["execution_mode"] == "reduce_only"
    combined = next(
        row
        for row in risk["checks"]
        if row["name"] == "total_position_plus_buy_notional"
    )
    assert combined["value"] == "1.80"
    assert combined["limit"] == "1.60"


def test_live_executor_omits_market_only_reserved_balance_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = PredictFunLiveExecutor(
        signer_url="http://signer.invalid",
        account_id="account_01",
        max_order_notional=Decimal("8"),
    )
    submitted: list[dict[str, Any]] = []

    def request(
        method: str,
        suffix: str,
        *,
        params: Any = None,
        json_body: Any = None,
    ) -> dict[str, Any]:
        del params
        assert method == "POST"
        assert suffix == "/submit-order"
        submitted.append(dict(json_body))
        return {"ok": True, "order_hash": "0x" + "1" * 64}

    monkeypatch.setattr(executor, "_request", request)

    result = executor.create(_order("account_01"))

    assert result.ok is True
    assert len(submitted) == 1
    assert submitted[0]["is_post_only"] is True
    assert submitted[0]["self_trade_prevention"] == "CANCEL_MAKER"
    assert "reserved_balance_policy" not in submitted[0]


def test_live_executor_separates_slot_intent_from_submission_idempotency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = PredictFunLiveExecutor(
        signer_url="http://signer.invalid",
        account_id="account_01",
        max_order_notional=Decimal("8"),
    )
    submitted: list[dict[str, Any]] = []

    def request(
        method: str,
        suffix: str,
        *,
        params: Any = None,
        json_body: Any = None,
    ) -> dict[str, Any]:
        del method, suffix, params
        submitted.append(dict(json_body))
        return {"ok": True, "order_hash": "0x" + "2" * 64}

    monkeypatch.setattr(executor, "_request", request)
    base = _order("account_01")
    order = ExecutableOrder(
        **{**base.__dict__, "idempotency_key": f"{base.intent_id}:g2"}
    )

    result = executor.create(order)

    assert result.ok is True
    assert submitted[0]["intent_id"] == base.intent_id
    assert submitted[0]["idempotency_key"] == f"{base.intent_id}:g2"


@pytest.mark.parametrize("inventory_source", ["live", "live_read_only"])
def test_live_inventory_is_used_by_risk_instead_of_simulation(
    inventory_source: str,
) -> None:
    risk = evaluate_risk(
        cfg={
            "risk": {
                "max_plan_state_age_sec": 180,
                "max_market_position_size": "1",
                "max_account_market_position_size": "1",
            }
        },
        plan_state={"ts": utc_now()},
        intents_state={"summary": {}, "intents": []},
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={"positions": []},
        kill_switch_state={},
        inventory_state={
            "positions": [
                {
                    "account_id": "account_01",
                    "market_id": 42,
                    "outcome": "YES",
                    "size": "2",
                    "value_usd": "1",
                }
            ]
        },
        inventory_source=inventory_source,
    )
    assert risk["summary"]["position_source"] == "live"
    assert risk["summary"]["live_positions"] == 1
    assert risk["summary"]["sim_positions"] == 0
    assert any(
        row["name"].startswith("live_position_")
        and row["status"] == "BLOCK"
        for row in risk["checks"]
    )


def test_account_inventory_and_unrealized_loss_limits_are_aggregated() -> None:
    risk = evaluate_risk(
        cfg={
            "accounts": {
                "ids": [
                    {
                        "account_id": "account_01",
                        "inventory_warning": "10",
                        "daily_loss_stop": "3",
                    }
                ]
            },
            "risk": {"max_plan_state_age_sec": 180},
        },
        plan_state={"ts": utc_now()},
        intents_state={"summary": {}, "intents": []},
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={},
        kill_switch_state={},
        inventory_state={
            "positions": [
                {
                    "account_id": "account_01",
                    "market_id": 42,
                    "outcome": "YES",
                    "size": "2",
                    "value_usd": "6",
                    "pnl_usd": "-2",
                },
                {
                    "account_id": "account_01",
                    "market_id": 43,
                    "outcome": "NO",
                    "size": "2",
                    "value_usd": "6",
                    "pnl_usd": "-2",
                },
            ]
        },
        inventory_source="live",
    )
    checks = {row["name"]: row for row in risk["checks"]}
    assert checks["account_position_value_account_01"]["value"] == "12"
    assert checks["account_position_value_account_01"]["status"] == "BLOCK"
    assert checks["account_unrealized_loss_account_01"]["value"] == "4"
    assert checks["account_unrealized_loss_account_01"]["status"] == "BLOCK"
    assert risk["execution_mode"] == "reduce_only"


def test_inventory_exit_is_split_by_account_order_limit() -> None:
    intents = build_intents_from_plans(
        [
            {
                "can_quote": False,
                "best_yes_bid": "0.4",
                "best_yes_ask": "0.6",
                "market": {
                    "id": 42,
                    "status": "REGISTERED",
                    "trading_status": "OPEN",
                    "decimal_precision": 2,
                    "yes_token_id": "yes-token",
                    "no_token_id": "no-token",
                },
            }
        ],
        accounts_config={
            "ids": [
                {
                    "account_id": "account_01",
                    "quote_enabled": False,
                    "max_order_notional": "8",
                }
            ]
        },
        inventory_positions=[
            {
                "account_id": "account_01",
                "market_id": 42,
                "outcome": "YES",
                "size": "100",
            }
        ],
    )
    assert len(intents) == 1
    assert intents[0].purpose == "inventory_exit"
    assert intents[0].side == "SELL"
    assert intents[0].notional <= Decimal("8")


class _AccountExecutor:
    def __init__(self, account_id: str) -> None:
        self.account_id = account_id
        self.created: list[ExecutableOrder] = []
        self.cancelled: list[str] = []
        self.resolved: LiveOrder | None = None

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        self.created.append(order)
        return ExecutionResult(
            intent_id=order.intent_id,
            account_id=self.account_id,
            action="create",
            ok=True,
            message="ok",
            order_id=f"order:{self.account_id}",
            status="open",
        )

    def cancel(
        self, order_id: str, *, intent_id: str = "", account_id: str = ""
    ) -> ExecutionResult:
        assert account_id == self.account_id
        self.cancelled.append(order_id)
        return ExecutionResult(
            intent_id=intent_id,
            account_id=account_id,
            action="cancel",
            ok=True,
            message="ok",
            order_id=order_id,
            status="cancelled",
        )

    def get_order(
        self, order_id: str, *, account_id: str = ""
    ) -> LiveOrder | None:
        assert account_id == self.account_id
        return self.resolved

    def list_orders(self) -> list[LiveOrder]:
        return []

    def list_balances(self) -> list[AccountBalance]:
        return []

    def list_positions(self) -> list[AccountPosition]:
        return []

    def capabilities(self) -> dict[str, Any]:
        return {
            "ok": True,
            "live_order_submit": True,
            "live_order_cancel": True,
            "live_order_read": True,
            "live_balance_read": True,
            "live_position_read": True,
        }


class _FailingPositionExecutor(_AccountExecutor):
    def list_positions(self) -> list[AccountPosition]:
        raise RuntimeError("positions unavailable")


def _order(account_id: str) -> ExecutableOrder:
    return ExecutableOrder(
        intent_id=f"intent:{account_id}",
        account_id=account_id,
        market_id=42,
        outcome="YES",
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("2"),
        token_id="42",
    )


def test_multi_account_executor_never_cross_routes_private_actions() -> None:
    first = _AccountExecutor("account_01")
    second = _AccountExecutor("account_02")
    executor = MultiAccountExecutor(
        {"account_01": first, "account_02": second}  # type: ignore[arg-type]
    )
    assert executor.create(_order("account_02")).ok is True
    assert first.created == []
    assert [row.account_id for row in second.created] == ["account_02"]

    assert executor.cancel("hash-1", account_id="account_01").ok is True
    assert first.cancelled == ["hash-1"]
    assert second.cancelled == []
    assert executor.cancel("hash-2", account_id="missing").ok is False


def test_read_only_executor_hard_blocks_writes_and_keeps_reads() -> None:
    underlying = _AccountExecutor("account_01")
    reader = PredictFunReadOnlyExecutor(underlying)  # type: ignore[arg-type]

    assert reader.create(_order("account_01")).ok is False
    assert reader.cancel("hash-1", account_id="account_01").ok is False
    assert underlying.created == []
    assert underlying.cancelled == []
    capabilities = reader.capabilities()
    assert capabilities["ok"] is True
    assert capabilities["live_order_submit"] is False
    assert capabilities["live_order_cancel"] is False
    assert capabilities["live_order_read"] is True


def test_account_reads_fail_independently_and_never_report_false_zero() -> None:
    executor = MultiAccountExecutor(
        {
            "account_01": _AccountExecutor("account_01"),
            "account_02": _FailingPositionExecutor("account_02"),
        },
        required_capabilities=(
            "live_order_read",
            "live_balance_read",
            "live_position_read",
        ),
    )

    orders, balances, positions, read_state = executor.read_account_state()
    assert orders == []
    assert balances == []
    assert positions == []
    assert read_state["accounts"]["account_01"]["positions"]["ok"] is True
    assert read_state["accounts"]["account_02"]["positions"] == {
        "ok": False,
        "count": 0,
        "error": "RuntimeError: positions unavailable",
    }

    capabilities = _runtime_read_capabilities(
        executor.capabilities(), read_state
    )
    assert capabilities["live_order_submit"] is False
    assert capabilities["live_order_cancel"] is False
    assert capabilities["live_order_read"] is True
    assert capabilities["live_balance_read"] is True
    assert capabilities["live_position_read"] is False
    assert capabilities["ok"] is False


def test_missing_managed_order_is_resolved_without_adopting_manual_orders() -> None:
    registry = ManagedOrderRegistry()
    created = _order("account_01")
    registry.record_create(
        created,
        ExecutionResult(
            intent_id=created.intent_id,
            account_id=created.account_id,
            action="create",
            ok=True,
            message="ok",
            order_id="managed-hash",
            status="open",
        ),
    )
    executor = _AccountExecutor("account_01")
    executor.resolved = LiveOrder(
        order_id="managed-hash",
        intent_id="",
        market_id=42,
        outcome="YES",
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("2"),
        filled_size=Decimal("2"),
        status="filled",
        account_id="account_01",
    )
    _sync_managed_live_orders(registry, [], executor)
    assert registry.active() == []

    manual = LiveOrder(
        order_id="manual-hash",
        intent_id="",
        market_id=42,
        outcome="NO",
        side="BUY",
        price=Decimal("0.6"),
        size=Decimal("1"),
        filled_size=Decimal("0"),
        status="open",
        account_id="account_01",
    )
    registry.sync_live_orders([manual])
    assert registry.owns_order_id("manual-hash") is False


def test_live_shutdown_cancels_only_engine_owned_orders(tmp_path: Path) -> None:
    order = _order("account_01")
    registry = ManagedOrderRegistry()
    registry.record_create(
        order,
        ExecutionResult(
            intent_id=order.intent_id,
            account_id=order.account_id,
            action="create",
            ok=True,
            message="ok",
            order_id="managed-hash",
            status="open",
        ),
    )
    report_path = tmp_path / "execution.json"
    report_path.write_text(
        json.dumps(
            {
                "mode": "live",
                "managed_orders": registry.to_state(),
                "manual_order_ids": ["manual-hash"],
            }
        ),
        encoding="utf-8",
    )
    account_executor = _AccountExecutor("account_01")
    executor = MultiAccountExecutor(
        {"account_01": account_executor}  # type: ignore[arg-type]
    )

    report = _cancel_managed_on_shutdown(report_path, executor)

    assert account_executor.cancelled == ["managed-hash"]
    assert report["mode"] == "live_cancel_only"
    assert report["reason"] == "runner_shutdown"
    assert report["summary"] == {
        "actions": 1,
        "create": 0,
        "cancel": 1,
        "failed": 0,
        "blocked": 1,
    }


def test_manual_orders_reserve_cash_and_block_duplicate_position_exit() -> None:
    managed = _order("account_01")
    registry = ManagedOrderRegistry()
    registry.record_create(
        managed,
        ExecutionResult(
            intent_id=managed.intent_id,
            account_id=managed.account_id,
            action="create",
            ok=True,
            message="ok",
            order_id="managed-buy",
            status="open",
        ),
    )
    live_orders = [
        LiveOrder(
            order_id="managed-buy",
            intent_id="",
            market_id=42,
            outcome="YES",
            side="BUY",
            price=Decimal("0.4"),
            size=Decimal("2"),
            filled_size=Decimal("0"),
            status="open",
            account_id="account_01",
        ),
        LiveOrder(
            order_id="manual-buy",
            intent_id="",
            market_id=43,
            outcome="YES",
            side="BUY",
            price=Decimal("0.5"),
            size=Decimal("10"),
            filled_size=Decimal("2"),
            status="open",
            account_id="account_01",
        ),
        LiveOrder(
            order_id="manual-sell",
            intent_id="",
            market_id=42,
            outcome="YES",
            side="SELL",
            price=Decimal("0.6"),
            size=Decimal("3"),
            filled_size=Decimal("0"),
            status="open",
            account_id="account_01",
        ),
    ]
    positions = [
        AccountPosition(
            market_id=42,
            outcome="YES",
            size=Decimal("3"),
            avg_price=Decimal("0.4"),
            mark_price=Decimal("0.5"),
            account_id="account_01",
            value_usd=Decimal("1.5"),
        ),
        AccountPosition(
            market_id=44,
            outcome="YES",
            size=Decimal("2"),
            avg_price=Decimal("0.4"),
            mark_price=Decimal("0.5"),
            account_id="account_01",
            value_usd=Decimal("1"),
        ),
    ]

    balances, exit_positions, summary = _apply_manual_order_constraints(
        balances=[
            AccountBalance(
                asset="USDT",
                available=Decimal("100"),
                total=Decimal("100"),
                account_id="account_01",
            )
        ],
        positions=positions,
        live_orders=live_orders,
        registry=registry,
    )

    assert balances[0].available == Decimal("96")
    assert [row.market_id for row in exit_positions] == [44]
    assert summary == {
        "account_01": {
            "manual_open_orders": 2,
            "manual_buy_reserved_notional": "4.0",
            "manual_sell_blocked_markets": [42],
        }
    }


def test_blocked_live_gate_never_constructs_live_executor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = {
        **_live_cfg(),
        "base_url": "http://127.0.0.1:9",
        "signer": {"enabled": False},
        "risk": {"max_plan_state_age_sec": 180},
        "data": {"require_ws_for_quotes": False},
        "output": {
            "state_path": "plan.json",
            "intents_path": "intents.json",
            "execution_report_path": "report.json",
            "runner_state_path": "runner.json",
            "ws_state_path": "ws.json",
            "simulation_state_path": "simulation.json",
            "risk_state_path": "risk.json",
            "kill_switch_path": "kill.json",
            "research_state_path": "research.json",
            "status_path": "status.json",
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "platforms.predictfun.maker.runner.load_config", lambda _path: cfg
    )
    monkeypatch.setattr(
        "platforms.predictfun.maker.runner.run_once",
        lambda *_args, **_kwargs: {"ts": utc_now(), "plans": []},
    )

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        raise AssertionError("blocked live gate constructed a live executor")

    monkeypatch.setattr(
        "platforms.predictfun.maker.runner._live_executor", forbidden
    )
    for name in (
        "PREDICTFUN_LIVE_TRADING",
        "PREDICTFUN_LIVE_CONFIRM",
        "PREDICTFUN_LIVE_RELEASE_SHA",
        "PREDICTFUN_LIVE_ACCOUNT_IDS",
        "PREDICTFUN_REQUIRE_RELEASE",
        "PREDICTFUN_RELEASE_SHA",
    ):
        monkeypatch.delenv(name, raising=False)

    state = run_loop(config_path=config_path, interval_sec=1, once=True)
    assert state["mode"] == "blocked"
    report = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))
    assert report["mode"] == "risk_blocked"
    assert report["summary"]["actions"] == 0


def test_dry_run_reads_real_account_state_without_enabling_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = utc_now()
    cfg = {
        "environment": "mainnet",
        "base_url": "http://proxy.invalid",
        "execution": {"mode": "dry_run"},
        "accounts": {"ids": ["account_01"], "max_active_accounts": 1},
        "runner": {"account_read_only_enabled": True},
        "simulation": {"enabled": False},
        "data": {"require_ws_for_quotes": False},
        "risk": {"max_plan_state_age_sec": 180},
        "output": {
            "state_path": "plan.json",
            "intents_path": "intents.json",
            "execution_report_path": "report.json",
            "runner_state_path": "runner.json",
            "ws_state_path": "ws.json",
            "simulation_state_path": "simulation.json",
            "risk_state_path": "risk.json",
            "kill_switch_path": "kill.json",
            "research_state_path": "research.json",
            "status_path": "status.json",
        },
    }
    config_path = tmp_path / "config.json"
    config_path.write_text("{}", encoding="utf-8")
    (tmp_path / "intents.json").write_text(
        json.dumps({"ts": now, "intents": [], "summary": {}}),
        encoding="utf-8",
    )

    class Reader:
        def capabilities(self) -> dict[str, Any]:
            account = {
                "ok": True,
                "live_order_submit": False,
                "live_order_cancel": False,
                "live_order_read": True,
                "live_balance_read": True,
                "live_position_read": True,
                "read_only": True,
            }
            return {**account, "accounts": {"account_01": account}}

        def read_account_state(self) -> tuple[Any, Any, Any, dict[str, Any]]:
            return (
                [],
                [
                    AccountBalance(
                        asset="USDT",
                        available=Decimal("12"),
                        total=Decimal("12"),
                        account_id="account_01",
                    )
                ],
                [
                    AccountPosition(
                        market_id=42,
                        outcome="YES",
                        size=Decimal("2"),
                        avg_price=Decimal("0.4"),
                        mark_price=Decimal("0.5"),
                        account_id="account_01",
                        value_usd=Decimal("1"),
                    )
                ],
                {
                    "accounts": {
                        "account_01": {
                            name: {"ok": True, "count": count, "error": ""}
                            for name, count in (
                                ("orders", 0),
                                ("balances", 1),
                                ("positions", 1),
                            )
                        }
                    }
                },
            )

    observed: dict[str, Any] = {}

    def fake_run_once(
        _client: Any, cycle_cfg: dict[str, Any], **kwargs: Any
    ) -> dict[str, Any]:
        observed["account"] = cycle_cfg["accounts"]["ids"][0]
        observed["inventory"] = kwargs.get("inventory_positions")
        return {
            "ts": now,
            "plans": [],
            "auth": {
                "enabled": True,
                "ok": True,
                "accounts": [{"account_id": "account_01", "ok": True}],
            },
        }

    monkeypatch.setattr(
        "platforms.predictfun.maker.runner.load_config", lambda _path: cfg
    )
    monkeypatch.setattr(
        "platforms.predictfun.maker.runner._read_only_executor",
        lambda *_args, **_kwargs: Reader(),
    )
    monkeypatch.setattr(
        "platforms.predictfun.maker.runner.run_once", fake_run_once
    )
    monkeypatch.setattr(
        "platforms.predictfun.maker.runner.evaluate_risk",
        lambda **_kwargs: {
            "ts": now,
            "status": "OK",
            "execution_mode": "normal",
            "summary": {},
            "checks": [],
        },
    )
    monkeypatch.setattr(
        "platforms.predictfun.maker.runner.build_research_state",
        lambda _plan: {"ts": now, "summary": {}},
    )

    state = run_loop(config_path=config_path, interval_sec=1, once=True)
    status = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))

    assert state["mode"] == "dry_run"
    assert state["capabilities"]["live_order_submit"] is False
    assert state["capabilities"]["live_order_cancel"] is False
    assert state["capabilities"]["live_balance_read"] is True
    assert state["last_account_summary"]["balances"] == {"account_01": "12"}
    assert observed["account"]["capital_source"] == "observed"
    assert observed["inventory"][0]["market_id"] == 42
    assert status["overview"]["live_balance"] == "12"
    assert status["overview"]["live_positions"] == 1
    assert status["capabilities"]["live_order_submit"] is False


def test_ws_quote_fingerprint_wakes_only_for_material_book_or_status_change() -> None:
    base = {
        "last_message_at": "2026-08-05T00:00:00Z",
        "orderbooks": {
            "42": {
                "bids": [["0.40", "10"], ["0.39", "20"]],
                "asks": [["0.60", "10"]],
                "updateTimestampMs": 1,
            }
        },
        "trading_statuses": {"42": {"status": "OPEN"}},
        "market_statuses": {"42": {"status": "REGISTERED"}},
    }
    heartbeat_only = {
        **base,
        "last_message_at": "2026-08-05T00:00:01Z",
        "orderbooks": {
            "42": {**base["orderbooks"]["42"], "updateTimestampMs": 2}
        },
    }
    changed_depth = {
        **base,
        "orderbooks": {
            "42": {**base["orderbooks"]["42"], "bids": [["0.40", "9"]]}
        },
    }
    changed_status = {
        **base,
        "trading_statuses": {"42": {"status": "PAUSED"}},
    }

    fingerprint = _ws_quote_fingerprint(base)
    assert _ws_quote_fingerprint(heartbeat_only) == fingerprint
    assert _ws_quote_fingerprint(changed_depth) != fingerprint
    assert _ws_quote_fingerprint(changed_status) != fingerprint


def test_live_status_never_presents_stale_simulation_as_active() -> None:
    status = build_status_snapshot(
        cfg={
            "accounts": {"ids": ["account_01"]},
            "simulation": {"enabled": False},
        },
        runner_state={"mode": "live", "requested_mode": "live"},
        plan_state={},
        intents_state={},
        execution_state={},
        risk_state={},
        simulation_state={
            "summary": {"fills_total": 99, "unrealized_pnl": "10"},
            "active_orders": [{"intent_id": "old-sim"}],
            "positions": [{"market_id": 42, "size": "10"}],
        },
        research_state={},
        ws_state={},
    )
    assert status["overview"]["simulated_active_orders"] == 0
    assert status["overview"]["simulated_positions"] == 0
    assert status["overview"]["simulated_fills"] == 0
    assert status["capabilities"]["simulated_fills"] is False
    assert status["simulated_active_orders"] == []
    assert status["simulated_positions"] == []


def _load_proxy_module() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "deploy/mac-mini/predictfun_api_proxy.py"
    spec = importlib.util.spec_from_file_location(
        "predictfun_api_proxy_under_test", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_proxy_server_absorbs_two_host_request_bursts() -> None:
    proxy = _load_proxy_module()

    assert issubclass(proxy.PredictFunHTTPServer, proxy.ThreadingHTTPServer)
    assert proxy.PredictFunHTTPServer.daemon_threads is True
    assert proxy.PredictFunHTTPServer.request_queue_size >= 64


def _proxy_env(max_order_notional: str = "8") -> dict[str, str]:
    return {
        "PREDICTFUN_ACCOUNT_KEYS_JSON": json.dumps(
            {
                "account_01": {
                    "api_key": "not-logged",
                    "private_key": "not-used",
                    "wallet_address": "0x0000000000000000000000000000000000000001",
                    "max_order_notional_usdc": max_order_notional,
                }
            }
        )
    }


def _proxy_order_body() -> dict[str, object]:
    return {
        "submit": True,
        "confirm": "SUBMIT_PREDICTFUN_ORDER",
        "idempotency_key": "intent-1",
        "intent_id": "intent-1",
        "market_id": 42,
        "token_id": "123",
        "side": "BUY",
        "price": "0.4",
        "size": "2",
        "is_post_only": True,
        "self_trade_prevention": "CANCEL_MAKER",
        "max_notional_usdc": "8",
    }


def test_proxy_idempotency_replays_same_payload_and_rejects_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(proxy, "ORDER_LEDGER_FILE", tmp_path / "ledger.json")
    upstream_payloads: list[dict[str, Any]] = []

    def signed_payload(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "order": {
                "side": 0,
                "makerAmount": str(8 * 10**17),
                "takerAmount": str(2 * 10**18),
                "maker": "0x0000000000000000000000000000000000000001",
            },
            "signed_order": {"signature": "present"},
            "amounts": {"pricePerShare": str(4 * 10**17)},
            "order_hash": "0x" + "1" * 64,
            "signer_mode": "predict_account",
        }

    def upstream(*_args: Any, **kwargs: Any) -> tuple[int, dict[str, Any]]:
        upstream_payloads.append(dict(kwargs["body"]["data"]))
        return 201, {
            "success": True,
            "data": {"orderHash": "0x" + "1" * 64},
        }

    monkeypatch.setattr(proxy, "_signed_order_payload", signed_payload)
    monkeypatch.setattr(proxy, "_authenticated_request", upstream)

    first = proxy.submit_order(_proxy_env(), "account_01", _proxy_order_body())
    second = proxy.submit_order(_proxy_env(), "account_01", _proxy_order_body())
    changed = _proxy_order_body()
    changed["price"] = "0.5"
    conflict = proxy.submit_order(_proxy_env(), "account_01", changed)

    assert first["ok"] is True
    assert second["idempotent_replay"] is True
    assert conflict == {
        "ok": False,
        "error": "idempotency_key_payload_mismatch",
        "alias": "account_01",
    }
    assert len(upstream_payloads) == 1
    assert upstream_payloads[0]["strategy"] == "LIMIT"
    assert upstream_payloads[0]["isPostOnly"] is True
    assert upstream_payloads[0]["selfTradePrevention"] == "CANCEL_MAKER"
    assert "reservedBalancePolicy" not in upstream_payloads[0]


def test_proxy_rejected_order_preserves_only_safe_upstream_details(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(proxy, "ORDER_LEDGER_FILE", tmp_path / "ledger.json")

    monkeypatch.setattr(
        proxy,
        "_signed_order_payload",
        lambda *_args, **_kwargs: {
            "order": {
                "side": 0,
                "makerAmount": str(8 * 10**17),
                "takerAmount": str(2 * 10**18),
                "maker": "0x0000000000000000000000000000000000000001",
                "expiration": "2000000000",
            },
            "signed_order": {"signature": "must-not-leak"},
            "amounts": {"pricePerShare": str(4 * 10**17)},
            "order_hash": "0x" + "5" * 64,
            "signer_mode": "predict_account",
        },
    )

    def upstream(
        _env: Any,
        _alias: str,
        path: str,
        **_kwargs: Any,
    ) -> tuple[int, dict[str, Any]]:
        if path == "/v1/orders":
            return 400, {
                "success": False,
                "error": "upstream_rejected",
                "api_key": "must-not-leak",
                "upstream": {
                    "success": False,
                    "code": "ORDER_SIZE_TOO_SMALL",
                    "error": "invalid_order",
                    "message": "Order size is below the market minimum",
                    "private_key": "must-not-leak",
                    "signature": "must-not-leak",
                },
            }
        return 404, {"success": False, "error": "not_found"}

    monkeypatch.setattr(proxy, "_authenticated_request", upstream)

    result = proxy.submit_order(
        _proxy_env(), "account_01", _proxy_order_body()
    )

    assert result["ok"] is False
    assert result["status"] == 400
    assert result["error"] == "order_rejected"
    assert result["upstream"] == {
        "success": False,
        "code": "ORDER_SIZE_TOO_SMALL",
        "error": "invalid_order",
        "message": "Order size is below the market minimum",
    }
    serialized = json.dumps(result)
    assert "must-not-leak" not in serialized
    ledger = json.loads((tmp_path / "ledger.json").read_text(encoding="utf-8"))
    ledger_serialized = json.dumps(ledger)
    assert "upstream" not in ledger_serialized
    assert "must-not-leak" not in ledger_serialized


def test_proxy_failed_retry_reuses_expiration_and_order_hash_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(proxy, "ORDER_LEDGER_FILE", tmp_path / "ledger.json")
    expirations: list[str] = []

    def signed_payload(_env: Any, _alias: str, body: dict[str, Any]) -> dict[str, Any]:
        expirations.append(str(body["expiration"]))
        return {
            "order": {
                "side": 0,
                "makerAmount": str(8 * 10**17),
                "takerAmount": str(2 * 10**18),
                "maker": "0x0000000000000000000000000000000000000001",
                "expiration": str(body["expiration"]),
            },
            "signed_order": {"signature": "present"},
            "amounts": {"pricePerShare": str(4 * 10**17)},
            "order_hash": "0x" + "3" * 64,
            "signer_mode": "predict_account",
        }

    monkeypatch.setattr(proxy, "_signed_order_payload", signed_payload)
    monkeypatch.setattr(
        proxy,
        "_authenticated_request",
        lambda *_args, **_kwargs: (
            503,
            {"success": False, "error": "upstream_unavailable"},
        ),
    )

    first = proxy.submit_order(_proxy_env(), "account_01", _proxy_order_body())
    second = proxy.submit_order(_proxy_env(), "account_01", _proxy_order_body())

    assert first["ok"] is False
    assert second["ok"] is False
    assert len(expirations) == 2
    assert expirations[0] == expirations[1]


def test_proxy_network_failure_persists_pending_order_for_safe_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(proxy, "ORDER_LEDGER_FILE", tmp_path / "ledger.json")
    expirations: list[str] = []
    upstream_calls = 0

    def signed_payload(
        _env: Any, _alias: str, body: dict[str, Any]
    ) -> dict[str, Any]:
        expirations.append(str(body["expiration"]))
        return {
            "order": {
                "side": 0,
                "makerAmount": str(8 * 10**17),
                "takerAmount": str(2 * 10**18),
                "maker": "0x0000000000000000000000000000000000000001",
                "expiration": str(body["expiration"]),
            },
            "signed_order": {"signature": "present"},
            "amounts": {"pricePerShare": str(4 * 10**17)},
            "order_hash": "0x" + "4" * 64,
            "signer_mode": "predict_account",
        }

    def upstream(*_args: Any, **_kwargs: Any) -> tuple[int, dict[str, Any]]:
        nonlocal upstream_calls
        upstream_calls += 1
        if upstream_calls == 1:
            raise TimeoutError("response lost")
        return 201, {
            "success": True,
            "data": {"orderHash": "0x" + "4" * 64},
        }

    monkeypatch.setattr(proxy, "_signed_order_payload", signed_payload)
    monkeypatch.setattr(proxy, "_authenticated_request", upstream)

    with pytest.raises(TimeoutError, match="response lost"):
        proxy.submit_order(_proxy_env(), "account_01", _proxy_order_body())
    pending = json.loads((tmp_path / "ledger.json").read_text(encoding="utf-8"))
    pending_row = next(iter(pending["orders"].values()))
    assert pending_row["error"] == "submission_pending"
    assert pending_row["order_hash"] == "0x" + "4" * 64

    result = proxy.submit_order(
        _proxy_env(), "account_01", _proxy_order_body()
    )

    assert result["ok"] is True
    assert expirations[0] == expirations[1]
    assert upstream_calls == 2


def test_proxy_enforces_mac_side_order_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(proxy, "ORDER_LEDGER_FILE", tmp_path / "ledger.json")
    body = _proxy_order_body()
    body["size"] = "40"

    monkeypatch.setattr(
        proxy,
        "_signed_order_payload",
        lambda *_args, **_kwargs: {
            "order": {
                "side": 0,
                "makerAmount": str(16 * 10**18),
                "takerAmount": str(40 * 10**18),
                "maker": "0x0000000000000000000000000000000000000001",
            },
            "signed_order": {"signature": "present"},
            "amounts": {"pricePerShare": str(4 * 10**17)},
            "order_hash": "0x" + "2" * 64,
            "signer_mode": "predict_account",
        },
    )
    monkeypatch.setattr(
        proxy,
        "_authenticated_request",
        lambda *_args, **_kwargs: pytest.fail("oversized order reached upstream"),
    )

    result = proxy.submit_order(_proxy_env("8"), "account_01", body)
    assert result["ok"] is False
    assert result["error"] == "max_notional_exceeded"
    assert result["max_notional_usdc"] == "8"


def test_proxy_cancel_preflight_checks_gas_without_order_hash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(
        proxy,
        "_cancel_gas_context",
        lambda *_args, **_kwargs: (
            {"eoa_address": "0x0000000000000000000000000000000000000001"},
            {
                "ok": True,
                "alias": "account_01",
                "gas_balance_bnb": "0.01",
                "min_gas_bnb": "0.0001",
                "error": "",
            },
        ),
    )

    result = proxy.cancel_orders_on_chain(
        _proxy_env(),
        "account_01",
        {
            "cancel": True,
            "confirm": "CANCEL_PREDICTFUN_ORDERS",
            "preflight_only": True,
            "min_gas_bnb": "0.0001",
        },
    )

    assert result["ok"] is True
    assert result["preflight_only"] is True
    assert result["on_chain_action"] is False
    assert result["gas_balance_bnb"] == "0.01"


def test_proxy_cancel_preflight_rejects_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy = _load_proxy_module()
    monkeypatch.setattr(
        proxy,
        "_cancel_gas_context",
        lambda *_args, **_kwargs: pytest.fail("unsafe preflight reached wallet"),
    )

    result = proxy.cancel_orders_on_chain(
        _proxy_env(),
        "account_01",
        {
            "cancel": True,
            "confirm": "CANCEL_PREDICTFUN_ORDERS",
            "preflight_only": True,
            "hashes": ["0x" + "1" * 64],
        },
    )

    assert result["ok"] is False
    assert result["error"] == "preflight_must_not_include_hashes"


def test_proxy_requires_successful_mined_cancel_receipt() -> None:
    proxy = _load_proxy_module()
    valid = {
        "success": True,
        "receipt_status": 1,
        "tx_hash": "0x" + "1" * 64,
    }
    assert proxy._cancel_receipt_verified(valid) is True
    assert proxy._cancel_receipt_verified({**valid, "receipt_status": 0}) is False
    assert proxy._cancel_receipt_verified({**valid, "tx_hash": "0x123"}) is False


def test_proxy_receipt_summary_accepts_web3_mapping() -> None:
    proxy = _load_proxy_module()
    receipt = UserDict(
        {
            "transactionHash": bytes.fromhex("ab" * 32),
            "blockNumber": 115071947,
            "status": 1,
        }
    )

    assert proxy._receipt_summary(receipt) == {
        "tx_hash": "0x" + "ab" * 32,
        "block_number": 115071947,
        "receipt_status": 1,
    }


class _CanaryResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> dict[str, Any]:
        return self._payload


def test_live_canary_preflights_cancel_gas_before_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def post(url: str, *, json: dict[str, Any], timeout: float) -> _CanaryResponse:
        assert timeout == 20
        calls.append((url, json))
        return _CanaryResponse(
            200,
            {
                "ok": True,
                "preflight_only": True,
                "on_chain_action": False,
                "gas_balance_bnb": "0.01",
                "min_gas_bnb": "0.0001",
            },
        )

    monkeypatch.setattr(live_order_once.requests, "post", post)

    result = live_order_once._cancel_preflight(
        signer_url="http://signer",
        account="account_01",
        timeout=20,
        min_gas_bnb="0.0001",
    )

    assert result["ok"] is True
    assert calls == [
        (
            "http://signer/predictfun/accounts/account_01/cancel-orders",
            {
                "cancel": True,
                "confirm": "CANCEL_PREDICTFUN_ORDERS",
                "preflight_only": True,
                "min_gas_bnb": "0.0001",
            },
        )
    ]


def test_live_canary_refuses_submission_without_verified_cancel(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        live_order_once.sys,
        "argv",
        [
            "live_order_once.py",
            "--live",
            "--confirm",
            "SUBMIT_PREDICTFUN_ORDER",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        live_order_once.main()

    assert exc.value.code == 2
    assert "--live requires --cancel-after" in capsys.readouterr().err


def test_live_canary_refuses_submission_without_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        live_order_once.sys,
        "argv",
        [
            "live_order_once.py",
            "--live",
            "--cancel-after",
            "--confirm",
            "SUBMIT_PREDICTFUN_ORDER",
        ],
    )

    with pytest.raises(SystemExit) as exc:
        live_order_once.main()

    assert exc.value.code == 2
    assert "--live requires --idempotency-key" in capsys.readouterr().err


def test_live_canary_forwards_explicit_idempotency_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    market = {
        "id": 10835,
        "title": "Canary market",
        "feeRateBps": 200,
        "isNegRisk": False,
        "isYieldBearing": True,
    }
    outcome = {
        "name": "Yes",
        "onChainId": "123",
        "bestBid": {"price": "0.052"},
        "bestAsk": {"price": "0.053"},
    }

    class FakeClient:
        def __init__(self, *, base_url: str) -> None:
            assert base_url == "http://api"

        def get_market(self, market_id: int) -> dict[str, Any]:
            assert market_id == 10835
            return {"data": market}

    submitted: list[tuple[str, dict[str, Any], float]] = []

    def post(
        url: str, *, json: dict[str, Any], timeout: float
    ) -> _CanaryResponse:
        submitted.append((url, json, timeout))
        return _CanaryResponse(
            200,
            {"ok": True, "order_hash": "0x" + "1" * 64},
        )

    monkeypatch.setattr(
        live_order_once,
        "load_config",
        lambda _path: {
            "base_url": "http://api",
            "signer": {"base_url": "http://signer", "timeout_sec": 20},
        },
    )
    monkeypatch.setattr(live_order_once, "PredictFunClient", FakeClient)
    monkeypatch.setattr(
        live_order_once,
        "_pick_market",
        lambda _client, _cfg, _market_id: market,
    )
    monkeypatch.setattr(
        live_order_once,
        "_pick_outcome",
        lambda _market, _outcome_name: outcome,
    )
    monkeypatch.setattr(
        live_order_once,
        "_cancel_preflight",
        lambda **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        live_order_once,
        "_allowance_preflight",
        lambda **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        live_order_once,
        "validate_final_order",
        lambda **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(live_order_once.requests, "post", post)
    monkeypatch.setattr(
        live_order_once,
        "_cancel_submitted_order",
        lambda **_kwargs: {"ok": True},
    )
    monkeypatch.setattr(
        live_order_once.sys,
        "argv",
        [
            "live_order_once.py",
            "--market-id",
            "10835",
            "--outcome",
            "YES",
            "--size",
            "1",
            "--max-notional",
            "0.10",
            "--idempotency-key",
            "canary-account01-20260809-01",
            "--live",
            "--cancel-after",
            "--confirm",
            "SUBMIT_PREDICTFUN_ORDER",
        ],
    )

    assert live_order_once.main() == 0
    assert len(submitted) == 1
    url, body, timeout = submitted[0]
    assert url == "http://signer/predictfun/accounts/account_01/submit-order"
    assert timeout == 20
    assert body["market_id"] == 10835
    assert body["idempotency_key"] == "canary-account01-20260809-01"
    assert body["intent_id"] == "canary-account01-20260809-01"
    assert body["is_post_only"] is True
    assert body["self_trade_prevention"] == "CANCEL_MAKER"
    assert "reserved_balance_policy" not in body


def test_live_canary_failure_returns_nonzero(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert live_order_once._emit_result({"ok": False, "error": "cancel_failed"}) == 1
    assert '"error": "cancel_failed"' in capsys.readouterr().out


def test_live_canary_requires_verified_on_chain_cancel_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "ok": True,
        "verified": True,
        "on_chain_cancelled": True,
        "off_book_removed": True,
        "open_hashes": [],
        "transactions": [
            {
                "success": True,
                "receipt_status": 1,
                "tx_hash": "0x" + "2" * 64,
            }
        ],
    }
    monkeypatch.setattr(
        live_order_once.requests,
        "post",
        lambda *_args, **_kwargs: _CanaryResponse(200, payload),
    )

    result = live_order_once._cancel_submitted_order(
        signer_url="http://signer",
        account="account_01",
        timeout=20,
        order_hash="0x" + "1" * 64,
        min_gas_bnb="0.0001",
    )

    assert result["ok"] is True
    assert result["verified"] is True
    assert result["receipts_verified"] is True

    payload["transactions"][0]["receipt_status"] = 0
    failed = live_order_once._cancel_submitted_order(
        signer_url="http://signer",
        account="account_01",
        timeout=20,
        order_hash="0x" + "1" * 64,
        min_gas_bnb="0.0001",
    )
    assert failed["ok"] is False
    assert failed["receipts_verified"] is False


def test_live_canary_never_uses_off_book_only_remove_endpoint() -> None:
    source = (
        Path(live_order_once.__file__).read_text(encoding="utf-8")
    )
    assert "remove-order-by-hash" not in source


@pytest.mark.parametrize(
    ("field", "value", "error"),
    [
        ("is_post_only", False, "post_only_required"),
        ("is_post_only", None, "post_only_required"),
        (
            "reserved_balance_policy",
            "REJECT_MARKET_ORDER",
            "reserved_balance_policy_not_allowed_for_limit",
        ),
        (
            "reserved_balance_policy",
            "ALLOW_MARKET_ORDER",
            "reserved_balance_policy_not_allowed_for_limit",
        ),
        (
            "reservedBalancePolicy",
            "REJECT_MARKET_ORDER",
            "reserved_balance_policy_not_allowed_for_limit",
        ),
        (
            "self_trade_prevention",
            "",
            "self_trade_prevention_required",
        ),
        (
            "self_trade_prevention",
            "NONE",
            "self_trade_prevention_required",
        ),
    ],
)
def test_proxy_requires_all_maker_safety_fields_before_signing(
    field: str,
    value: object,
    error: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proxy = _load_proxy_module()
    body = _proxy_order_body()
    if value is None:
        body.pop(field)
    else:
        body[field] = value
    monkeypatch.setattr(
        proxy,
        "_signed_order_payload",
        lambda *_args, **_kwargs: pytest.fail("unsafe order reached signing"),
    )

    with pytest.raises(ValueError, match=error):
        proxy.submit_order(_proxy_env(), "account_01", body)


@pytest.mark.parametrize("price", ["0", "1", "NaN", "Infinity"])
def test_proxy_rejects_invalid_binary_prices(price: str) -> None:
    proxy = _load_proxy_module()
    body = _proxy_order_body()
    body["price"] = price
    with pytest.raises(ValueError):
        proxy._build_order(
            {},
            "0x0000000000000000000000000000000000000001",
            body,
        )
