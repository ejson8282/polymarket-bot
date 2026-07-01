from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol


@dataclass(frozen=True)
class ExecutionResult:
    intent_id: str
    account_id: str
    action: str
    ok: bool
    message: str


@dataclass(frozen=True)
class ExecutableOrder:
    intent_id: str
    account_id: str
    market_id: int
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    token_id: str = ""
    fee_rate_bps: int = 0
    is_neg_risk: bool = False
    is_yield_bearing: bool = False
    market_mode: str = "standard"


@dataclass(frozen=True)
class LiveOrder:
    order_id: str
    intent_id: str
    market_id: int
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    filled_size: Decimal
    status: str


@dataclass(frozen=True)
class AccountBalance:
    asset: str
    available: Decimal
    total: Decimal


@dataclass(frozen=True)
class AccountPosition:
    market_id: int
    outcome: str
    size: Decimal
    avg_price: Decimal
    mark_price: Decimal


class PredictFunExecutor(Protocol):
    def create(self, order: ExecutableOrder) -> ExecutionResult:
        ...

    def cancel(self, intent_id: str, account_id: str = "") -> ExecutionResult:
        ...

    def list_orders(self) -> list[LiveOrder]:
        ...

    def list_balances(self) -> list[AccountBalance]:
        ...

    def list_positions(self) -> list[AccountPosition]:
        ...


class DryRunExecutor:
    """Executor stub used before Predict.fun live auth/signing is available."""

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        return ExecutionResult(
            intent_id=order.intent_id,
            account_id=order.account_id,
            action="create",
            ok=True,
            message="dry-run only; no order submitted",
        )

    def cancel(self, intent_id: str, account_id: str = "") -> ExecutionResult:
        return ExecutionResult(
            intent_id=intent_id,
            account_id=account_id,
            action="cancel",
            ok=True,
            message="dry-run only; no order canceled",
        )

    def list_orders(self) -> list[LiveOrder]:
        return []

    def list_balances(self) -> list[AccountBalance]:
        return []

    def list_positions(self) -> list[AccountPosition]:
        return []


class PredictFunLiveExecutor:
    """Interface boundary for the future Predict.fun authenticated executor.

    Keep strategy, reconciliation, and dashboard code pointed at the executor
    protocol. Once Predict.fun mainnet auth is available, only this adapter
    should need SDK/JWT/signing details.
    """

    def __init__(self, *, base_url: str, api_key: str, signer_url: str = "") -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.signer_url = signer_url.rstrip("/")

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        del order
        return self._not_ready("create")

    def cancel(self, intent_id: str, account_id: str = "") -> ExecutionResult:
        return ExecutionResult(
            intent_id=intent_id,
            account_id=account_id,
            action="cancel",
            ok=False,
            message="live Predict.fun executor is blocked until API key, JWT, wallet, and signer path are confirmed",
        )

    def list_orders(self) -> list[LiveOrder]:
        return []

    def list_balances(self) -> list[AccountBalance]:
        return []

    def list_positions(self) -> list[AccountPosition]:
        return []

    def _not_ready(self, action: str) -> ExecutionResult:
        return ExecutionResult(
            intent_id="",
            account_id="",
            action=action,
            ok=False,
            message="live Predict.fun executor is not enabled",
        )
