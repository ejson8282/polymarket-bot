from dataclasses import asdict
from decimal import Decimal

import pytest

from platforms.predictfun.maker.executor import (
    ExecutableOrder,
    ExecutionResult,
    PredictFunLiveExecutor,
)
from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry
from platforms.predictfun.maker.intents import build_intents_from_plans
from platforms.predictfun.maker.reconcile import (
    reconcile_once,
    reconcile_reduce_only,
    recover_uncertain_submissions,
)


def order(intent_id="exit-new", account_id="account_01"):
    return ExecutableOrder(
        intent_id=intent_id, account_id=account_id, market_id=42,
        outcome="NO", side="SELL", price=Decimal("0.31"),
        size=Decimal("5"), token_id="no-token", purpose="inventory_exit",
        idempotency_key=intent_id,
    )


class CancelFailureExecutor:
    def __init__(self, recovery=None):
        self.created = []
        self.cancelled = []
        self.recovery = recovery

    def recover_submission(self, request):
        return self.recovery

    def cancel(self, order_id, *, intent_id="", account_id=""):
        self.cancelled.append(order_id)
        return ExecutionResult(intent_id, account_id, "cancel", False,
                               "cancel failed", order_id, "open")

    def create(self, request):
        self.created.append(request)
        return ExecutionResult(request.intent_id, request.account_id,
                               "create", True, "accepted", "new-hash", "open")


@pytest.mark.parametrize("reconcile", [reconcile_once, reconcile_reduce_only])
def test_suppressed_replacements_do_not_accumulate_phantom_pending(reconcile):
    registry = ManagedOrderRegistry()
    old = order("exit-old")
    registry.record_create(old, ExecutionResult(
        old.intent_id, old.account_id, "create", True, "accepted", "old-hash", "open"
    ))
    state = registry.to_state()
    executor = CancelFailureExecutor()
    for n in range(3):
        replacement = asdict(order(f"exit-new-{n}"))
        report = reconcile(
            {"intents": [replacement], "diff": {
                "create": [replacement], "cancel": [asdict(old)],
            }},
            executor=executor, managed_state=state, mode="live",
        )
        state = report["managed_orders"]
        assert report["summary"]["create_suppressed"] == 1
        assert state["summary"]["pending_submissions"] == 0
    assert executor.created == []
    assert executor.cancelled == ["old-hash"] * 3


def test_unknown_attempt_is_preserved_but_unsubmitted_plan_is_not_pending():
    registry = ManagedOrderRegistry()
    registry.record_submission_pending(order("attempted"))
    recover_uncertain_submissions(
        {"diff": {"create": [asdict(order("unsubmitted"))]}},
        registry=registry, executor=CancelFailureExecutor(),
    )
    assert [r.intent_id for r in registry.pending_submissions()] == ["attempted"]


def test_ledger_confirmed_unknown_plan_is_retained_for_later_recovery():
    request = order()
    unknown = ExecutionResult(request.intent_id, request.account_id,
                              "create", False, "pending", status="unknown")
    registry = ManagedOrderRegistry()
    recover_uncertain_submissions(
        {"diff": {"create": [asdict(request)]}},
        registry=registry, executor=CancelFailureExecutor(unknown),
    )
    assert [r.intent_id for r in registry.pending_submissions()] == [request.intent_id]


@pytest.mark.parametrize("reconcile", [reconcile_once, reconcile_reduce_only])
@pytest.mark.parametrize("account_id", ["account_01", "account_02"])
def test_unknown_attempt_blocks_new_key_only_on_its_account(reconcile, account_id):
    registry = ManagedOrderRegistry()
    registry.record_submission_pending(order("old-attempt"))
    request = asdict(order(account_id=account_id))
    executor = CancelFailureExecutor()
    report = reconcile(
        {"intents": [request], "diff": {"create": [request]}},
        executor=executor, managed_state=registry.to_state(), mode="live",
    )
    blocked = account_id == "account_01"
    assert len(executor.created) == (0 if blocked else 1)
    assert report["summary"]["blocked"] == int(blocked)
    assert report["summary"]["failed"] == 0
    assert report["managed_orders"]["summary"]["pending_submissions"] == 1


def test_ledger_unknown_same_key_is_not_resubmitted():
    request = order()
    unknown = ExecutionResult(request.intent_id, request.account_id,
                              "create", False, "pending", status="unknown")
    executor = CancelFailureExecutor(unknown)
    report = reconcile_once(
        {"diff": {"create": [asdict(request)]}},
        executor=executor, managed_state={}, mode="live",
    )
    assert executor.created == []
    assert report["managed_orders"]["summary"]["pending_submissions"] == 1


@pytest.mark.parametrize("code, expected", [
    ("insufficient_bnb_for_cancel", "insufficient_bnb_for_cancel"),
    ("off_book_remove_failed", "off_book_remove_failed"),
    ("on_chain_cancel_receipt_unverified", "on_chain_cancel_receipt_unverified"),
    ("cancel_verification_timeout", "cancel_verification_timeout"),
    ("predictfun_post_failed", "predictfun_post_failed"),
    ("api_key=DO_NOT_LOG_THIS", "proxy_request_failed"),
])
def test_cancel_failure_has_safe_error_code_without_raw_payload(monkeypatch, code, expected):
    class Response:
        status_code = 502

        def json(self):
            return {"ok": False, "error": code, "detail": "DO_NOT_LOG_THIS"}

    class Session:
        def request(self, *args, **kwargs):
            return Response()

    executor = PredictFunLiveExecutor(
        signer_url="http://signer.invalid", account_id="account_01",
        max_order_notional=Decimal("1.60"), request_retries=1, session=Session(),
    )
    monkeypatch.setattr(executor, "get_order", lambda *a, **k: None)
    result = executor.cancel("old-hash", account_id="account_01")
    assert not result.ok
    assert result.status == "open"
    assert expected in result.message
    assert "HTTP 502" in result.message
    assert "DO_NOT_LOG_THIS" not in result.message


@pytest.mark.parametrize("source, reason, expected", [
    ("ws:required", "fresh ws orderbook required", 0),
    ("ws:liquidity_sentinel", "liquidity collapsed", 0),
    ("config:market_mode_guard", "market mode not allowed", 0),
    ("rest_error", "orderbook unavailable", 0),
    ("rest", "orderbook unavailable", 0),
    ("ws", "no legal passive quote inside reward band", 1),
    ("rest", "no legal passive quote inside reward band", 1),
    ("rest_reconcile", "no legal passive quote inside reward band", 1),
])
def test_exit_cannot_use_prices_from_missing_or_blocked_book(source, reason, expected):
    plan = {
        "market": {"id": 42, "status": "OPEN", "trading_status": "OPEN",
                   "decimal_precision": 2, "yes_token_id": "yes", "no_token_id": "no"},
        "can_quote": False, "skip_reason": reason, "orderbook_source": source,
        "best_yes_bid": "0.68", "best_yes_ask": "0.72",
        "yes_quotes": [], "no_quotes": [],
    }
    intents = build_intents_from_plans(
        [plan], accounts_config=[{"account_id": "account_01"}],
        inventory_positions=[{"account_id": "account_01", "market_id": 42,
                              "outcome": "NO", "size": "10"}],
    )
    assert len(intents) == expected
    assert all(i.purpose == "inventory_exit" and i.side == "SELL" for i in intents)
