from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Protocol

import requests


@dataclass(frozen=True)
class ExecutionResult:
    intent_id: str
    account_id: str
    action: str
    ok: bool
    message: str
    order_id: str = ""
    status: str = ""


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
    purpose: str = "maker_quote"
    idempotency_key: str = ""


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
    account_id: str = ""
    purpose: str = ""
    token_id: str = ""
    external_order_id: str = ""


@dataclass(frozen=True)
class AccountBalance:
    asset: str
    available: Decimal
    total: Decimal
    account_id: str = ""


@dataclass(frozen=True)
class AccountPosition:
    market_id: int
    outcome: str
    size: Decimal
    avg_price: Decimal
    mark_price: Decimal
    account_id: str = ""
    value_usd: Decimal = Decimal("0")
    pnl_usd: Decimal = Decimal("0")


class PredictFunExecutor(Protocol):
    def create(self, order: ExecutableOrder) -> ExecutionResult:
        ...

    def cancel(self, order_id: str, *, intent_id: str = "", account_id: str = "") -> ExecutionResult:
        ...

    def list_orders(self) -> list[LiveOrder]:
        ...

    def get_order(
        self, order_id: str, *, account_id: str = ""
    ) -> LiveOrder | None:
        ...

    def list_balances(self) -> list[AccountBalance]:
        ...

    def list_positions(self) -> list[AccountPosition]:
        ...

    def capabilities(self) -> dict[str, Any]:
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
            order_id=f"dry:{order.intent_id}",
            status="open",
        )

    def cancel(self, order_id: str, *, intent_id: str = "", account_id: str = "") -> ExecutionResult:
        return ExecutionResult(
            intent_id=intent_id,
            account_id=account_id,
            action="cancel",
            ok=True,
            message="dry-run only; no order canceled",
            order_id=order_id,
            status="cancelled",
        )

    def list_orders(self) -> list[LiveOrder]:
        return []

    def get_order(
        self, order_id: str, *, account_id: str = ""
    ) -> LiveOrder | None:
        del order_id, account_id
        return None

    def list_balances(self) -> list[AccountBalance]:
        return []

    def list_positions(self) -> list[AccountPosition]:
        return []

    def capabilities(self) -> dict[str, Any]:
        return {
            "live_order_submit": False,
            "live_order_cancel": False,
            "live_order_read": False,
            "live_balance_read": False,
            "live_position_read": False,
        }


class PredictFunLiveExecutor:
    """Account-scoped adapter for the Mac mini Predict.fun proxy."""

    def __init__(
        self,
        *,
        signer_url: str,
        account_id: str,
        max_order_notional: Decimal,
        timeout: float = 20.0,
        session: requests.Session | None = None,
    ) -> None:
        self.signer_url = signer_url.rstrip("/")
        self.account_id = str(account_id).strip()
        self.max_order_notional = _decimal(max_order_notional)
        self.timeout = max(1.0, float(timeout))
        self.session = session or requests.Session()
        if not self.account_id:
            raise ValueError("Predict.fun live executor requires account_id")
        if self.max_order_notional <= 0:
            raise ValueError(
                "Predict.fun live executor requires a positive max_order_notional"
            )

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        if order.account_id != self.account_id:
            return self._error(
                "create",
                "account mismatch; refusing cross-account order submission",
                intent_id=order.intent_id,
                account_id=order.account_id,
            )
        notional = order.price * order.size
        if notional <= 0 or notional > self.max_order_notional:
            return self._error(
                "create",
                "order notional exceeds account executor limit",
                intent_id=order.intent_id,
                account_id=order.account_id,
            )
        if not order.token_id:
            return self._error(
                "create",
                "missing Predict.fun token id",
                intent_id=order.intent_id,
                account_id=order.account_id,
            )
        idempotency_key = str(order.idempotency_key or order.intent_id).strip()
        if not idempotency_key:
            return self._error(
                "create",
                "missing Predict.fun idempotency key",
                intent_id=order.intent_id,
                account_id=order.account_id,
            )
        try:
            payload = self._request(
                "POST",
                "/submit-order",
                json_body={
                    "submit": True,
                    "confirm": "SUBMIT_PREDICTFUN_ORDER",
                    "idempotency_key": idempotency_key,
                    "intent_id": order.intent_id,
                    "market_id": order.market_id,
                    "token_id": order.token_id,
                    "side": order.side,
                    "price": str(order.price),
                    "size": str(order.size),
                    "fee_rate_bps": order.fee_rate_bps,
                    "is_neg_risk": order.is_neg_risk,
                    "is_yield_bearing": order.is_yield_bearing,
                    "is_post_only": True,
                    "self_trade_prevention": "CANCEL_MAKER",
                    "max_notional_usdc": str(self.max_order_notional),
                },
            )
        except Exception as exc:
            return self._error(
                "create",
                f"Predict.fun submit failed: {type(exc).__name__}",
                intent_id=order.intent_id,
                account_id=order.account_id,
            )
        ok = payload.get("ok") is True
        order_hash = str(payload.get("order_hash") or "")
        return ExecutionResult(
            intent_id=order.intent_id,
            account_id=order.account_id,
            action="create",
            ok=ok,
            message=(
                "order accepted"
                if ok
                else str(payload.get("error") or "order rejected")
            ),
            order_id=order_hash,
            status="open" if ok else "rejected",
        )

    def cancel(self, order_id: str, *, intent_id: str = "", account_id: str = "") -> ExecutionResult:
        if account_id and account_id != self.account_id:
            return self._error(
                "cancel",
                "account mismatch; refusing cross-account cancellation",
                intent_id=intent_id,
                account_id=account_id,
                order_id=order_id,
            )
        try:
            payload = self._request(
                "POST",
                "/cancel-orders",
                json_body={
                    "cancel": True,
                    "confirm": "CANCEL_PREDICTFUN_ORDERS",
                    "hashes": [order_id],
                },
            )
        except Exception as exc:
            return self._error(
                "cancel",
                f"Predict.fun cancel failed: {type(exc).__name__}",
                intent_id=intent_id,
                account_id=self.account_id,
                order_id=order_id,
            )
        ok = payload.get("ok") is True and payload.get("verified") is True
        return ExecutionResult(
            intent_id=intent_id,
            account_id=self.account_id,
            action="cancel",
            ok=ok,
            message=(
                "order cancelled on chain"
                if ok
                else str(payload.get("error") or "cancel not verified")
            ),
            order_id=order_id,
            status="cancelled" if ok else "open",
        )

    def list_orders(self) -> list[LiveOrder]:
        rows: list[LiveOrder] = []
        after = ""
        seen_cursors: set[str] = set()
        for _page in range(20):
            params: dict[str, Any] = {"first": 100, "status": "OPEN"}
            if after:
                params["after"] = after
            payload = self._request("GET", "/orders", params=params)
            rows.extend(
                row
                for item in _response_rows(payload)
                if (row := _live_order(item, self.account_id)) is not None
            )
            cursor = _response_cursor(payload)
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
            after = cursor
        return rows

    def get_order(
        self, order_id: str, *, account_id: str = ""
    ) -> LiveOrder | None:
        if account_id and account_id != self.account_id:
            return None
        try:
            payload = self._request("GET", f"/orders/{order_id}")
        except RuntimeError:
            return None
        item = _response_item(payload)
        return _live_order(item, self.account_id) if item else None

    def list_balances(self) -> list[AccountBalance]:
        payload = self._request("GET", "/allowances")
        balance = _decimal(payload.get("balance"))
        if payload.get("ok") is not True:
            raise RuntimeError(str(payload.get("error") or "balance read failed"))
        return [
            AccountBalance(
                asset=str(payload.get("collateral") or "USDT"),
                available=balance,
                total=balance,
                account_id=self.account_id,
            )
        ]

    def list_positions(self) -> list[AccountPosition]:
        rows: list[AccountPosition] = []
        after = ""
        seen_cursors: set[str] = set()
        for _page in range(20):
            params: dict[str, Any] = {
                "first": 100,
                "isResolved": "false",
            }
            if after:
                params["after"] = after
            payload = self._request("GET", "/positions", params=params)
            rows.extend(
                row
                for item in _response_rows(payload)
                if (row := _account_position(item, self.account_id)) is not None
            )
            cursor = _response_cursor(payload)
            if not cursor or cursor in seen_cursors:
                break
            seen_cursors.add(cursor)
            after = cursor
        return rows

    def capabilities(self) -> dict[str, Any]:
        payload = self._request("GET", "/capabilities")
        return {
            key: value
            for key, value in payload.items()
            if key
            in {
                "ok",
                "auth",
                "auth_error",
                "predict_account",
                "sdk_present",
                "live_order_submit",
                "live_order_cancel",
                "live_order_read",
                "live_balance_read",
                "live_position_read",
                "off_book_remove",
            }
        }

    def _request(
        self,
        method: str,
        suffix: str,
        *,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        url = (
            f"{self.signer_url}/predictfun/accounts/"
            f"{self.account_id}{suffix}"
        )
        response = self.session.request(
            method,
            url,
            params=dict(params or {}),
            json=dict(json_body) if json_body is not None else None,
            timeout=self.timeout,
        )
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError("Predict.fun proxy returned non-JSON") from exc
        if not isinstance(payload, dict):
            raise RuntimeError("Predict.fun proxy returned non-object JSON")
        if response.status_code >= 400 or payload.get("ok") is False:
            raise RuntimeError(str(payload.get("error") or response.status_code))
        return payload

    @staticmethod
    def _error(
        action: str,
        message: str,
        *,
        intent_id: str = "",
        account_id: str = "",
        order_id: str = "",
    ) -> ExecutionResult:
        return ExecutionResult(
            intent_id=intent_id,
            account_id=account_id,
            action=action,
            ok=False,
            message=message,
            order_id=order_id,
            status="rejected" if action == "create" else "open",
        )


class PredictFunReadOnlyExecutor:
    """Read account state through the proxy while hard-blocking all writes."""

    def __init__(self, executor: PredictFunLiveExecutor) -> None:
        self.executor = executor
        self.account_id = executor.account_id

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        return PredictFunLiveExecutor._error(
            "create",
            "read-only executor; live order submission disabled",
            intent_id=order.intent_id,
            account_id=order.account_id,
        )

    def cancel(
        self, order_id: str, *, intent_id: str = "", account_id: str = ""
    ) -> ExecutionResult:
        return PredictFunLiveExecutor._error(
            "cancel",
            "read-only executor; live order cancellation disabled",
            intent_id=intent_id,
            account_id=account_id or self.account_id,
            order_id=order_id,
        )

    def list_orders(self) -> list[LiveOrder]:
        return self.executor.list_orders()

    def get_order(
        self, order_id: str, *, account_id: str = ""
    ) -> LiveOrder | None:
        return self.executor.get_order(order_id, account_id=account_id)

    def list_balances(self) -> list[AccountBalance]:
        return self.executor.list_balances()

    def list_positions(self) -> list[AccountPosition]:
        return self.executor.list_positions()

    def capabilities(self) -> dict[str, Any]:
        capabilities = self.executor.capabilities()
        required = (
            "live_order_read",
            "live_balance_read",
            "live_position_read",
        )
        return {
            **capabilities,
            "ok": capabilities.get("ok") is True
            and all(capabilities.get(name) is True for name in required),
            "live_order_submit": False,
            "live_order_cancel": False,
            "read_only": True,
        }


class MultiAccountExecutor:
    """Routes private actions to isolated account adapters."""

    def __init__(
        self,
        executors: Mapping[str, PredictFunExecutor],
        *,
        required_capabilities: tuple[str, ...] | None = None,
    ) -> None:
        self.executors = {
            str(account_id): executor
            for account_id, executor in executors.items()
            if str(account_id)
        }
        if not self.executors:
            raise ValueError("at least one Predict.fun account executor is required")
        self.required_capabilities = required_capabilities or (
            "live_order_submit",
            "live_order_cancel",
            "live_order_read",
            "live_balance_read",
            "live_position_read",
        )

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        executor = self.executors.get(order.account_id)
        if executor is None:
            return PredictFunLiveExecutor._error(
                "create",
                "account executor unavailable",
                intent_id=order.intent_id,
                account_id=order.account_id,
            )
        return executor.create(order)

    def set_order_notional_limits(
        self, limits: Mapping[str, Decimal]
    ) -> None:
        """Apply observed per-account limits before any create action."""

        for account_id, executor in self.executors.items():
            limit = _decimal(limits.get(account_id))
            if limit > 0 and hasattr(executor, "max_order_notional"):
                executor.max_order_notional = limit

    def cancel(
        self, order_id: str, *, intent_id: str = "", account_id: str = ""
    ) -> ExecutionResult:
        executor = self.executors.get(account_id)
        if executor is None:
            return PredictFunLiveExecutor._error(
                "cancel",
                "account executor unavailable",
                intent_id=intent_id,
                account_id=account_id,
                order_id=order_id,
            )
        return executor.cancel(
            order_id, intent_id=intent_id, account_id=account_id
        )

    def list_orders(self) -> list[LiveOrder]:
        return [
            row
            for executor in self.executors.values()
            for row in executor.list_orders()
        ]

    def get_order(
        self, order_id: str, *, account_id: str = ""
    ) -> LiveOrder | None:
        executor = self.executors.get(account_id)
        if executor is None:
            return None
        return executor.get_order(order_id, account_id=account_id)

    def list_balances(self) -> list[AccountBalance]:
        return [
            row
            for executor in self.executors.values()
            for row in executor.list_balances()
        ]

    def list_positions(self) -> list[AccountPosition]:
        return [
            row
            for executor in self.executors.values()
            for row in executor.list_positions()
        ]

    def capabilities(self) -> dict[str, Any]:
        accounts = {
            account_id: executor.capabilities()
            for account_id, executor in self.executors.items()
        }
        capability_names = (
            "live_order_submit",
            "live_order_cancel",
            "live_order_read",
            "live_balance_read",
            "live_position_read",
        )
        return {
            "ok": bool(accounts)
            and all(
                row.get("ok") is True
                and all(
                    row.get(name) is True
                    for name in self.required_capabilities
                )
                for row in accounts.values()
            ),
            "accounts": accounts,
            **{
                name: bool(accounts)
                and all(row.get(name) is True for row in accounts.values())
                for name in capability_names
            },
        }

    def read_account_state(
        self,
    ) -> tuple[
        list[LiveOrder],
        list[AccountBalance],
        list[AccountPosition],
        dict[str, Any],
    ]:
        """Read each account independently so one endpoint cannot mask another."""

        orders: list[LiveOrder] = []
        balances: list[AccountBalance] = []
        positions: list[AccountPosition] = []
        accounts: dict[str, Any] = {}
        readers = (
            ("orders", "list_orders", orders),
            ("balances", "list_balances", balances),
            ("positions", "list_positions", positions),
        )
        for account_id, executor in self.executors.items():
            account_state: dict[str, Any] = {}
            for name, method_name, destination in readers:
                try:
                    rows = getattr(executor, method_name)()
                    destination.extend(rows)
                    account_state[name] = {
                        "ok": True,
                        "count": len(rows),
                        "error": "",
                    }
                except Exception as exc:
                    account_state[name] = {
                        "ok": False,
                        "count": 0,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
            accounts[account_id] = account_state
        return orders, balances, positions, {"accounts": accounts}


def _response_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    response = payload.get("response")
    response = response if isinstance(response, dict) else payload
    rows = response.get("data")
    return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


def _response_item(payload: dict[str, Any]) -> dict[str, Any]:
    response = payload.get("response")
    response = response if isinstance(response, dict) else payload
    row = response.get("data")
    return row if isinstance(row, dict) else {}


def _response_cursor(payload: dict[str, Any]) -> str:
    response = payload.get("response")
    response = response if isinstance(response, dict) else payload
    for key in ("nextCursor", "next_cursor", "cursor", "after"):
        value = response.get(key)
        if isinstance(value, str) and value:
            return value
    page = response.get("pageInfo") or response.get("pagination")
    if isinstance(page, dict):
        for key in ("endCursor", "nextCursor", "next_cursor", "cursor"):
            value = page.get(key)
            if isinstance(value, str) and value:
                return value
    return ""


def _live_order(row: dict[str, Any], account_id: str) -> LiveOrder | None:
    order = row.get("order") if isinstance(row.get("order"), dict) else {}
    order_hash = str(order.get("hash") or row.get("orderHash") or "")
    if not order_hash:
        return None
    side_value = int(_decimal(order.get("side")))
    maker_amount = _wei_decimal(order.get("makerAmount"))
    taker_amount = _wei_decimal(order.get("takerAmount"))
    if side_value == 0:
        size = taker_amount
        price = maker_amount / taker_amount if taker_amount > 0 else Decimal("0")
        side = "BUY"
    else:
        size = maker_amount
        price = taker_amount / maker_amount if maker_amount > 0 else Decimal("0")
        side = "SELL"
    return LiveOrder(
        order_id=order_hash,
        intent_id="",
        market_id=int(_decimal(row.get("marketId"))),
        outcome=str(row.get("outcome") or ""),
        side=side,
        price=price,
        size=size,
        filled_size=_api_decimal(row.get("amountFilled")),
        status=str(row.get("status") or "").lower(),
        account_id=account_id,
        token_id=str(order.get("tokenId") or ""),
        external_order_id=str(row.get("id") or ""),
    )


def _account_position(
    row: dict[str, Any], account_id: str
) -> AccountPosition | None:
    market = row.get("market") if isinstance(row.get("market"), dict) else {}
    outcome = row.get("outcome")
    outcome = outcome if isinstance(outcome, dict) else {}
    market_id = int(_decimal(market.get("id") or row.get("marketId")))
    size = _api_decimal(row.get("amount"))
    if market_id <= 0 or size <= 0:
        return None
    value = _api_decimal(row.get("valueUsd"))
    average = _api_decimal(row.get("averageBuyPriceUsd"))
    return AccountPosition(
        market_id=market_id,
        outcome=str(
            outcome.get("name")
            or outcome.get("label")
            or row.get("outcomeName")
            or ""
        ).upper(),
        size=size,
        avg_price=average,
        mark_price=value / size if size > 0 else Decimal("0"),
        account_id=account_id,
        value_usd=value,
        pnl_usd=_api_decimal(row.get("pnlUsd")),
    )


def _decimal(value: Any) -> Decimal:
    try:
        out = Decimal(str(value or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    return out if out.is_finite() else Decimal("0")


def _wei_decimal(value: Any) -> Decimal:
    return _decimal(value) / Decimal(10**18)


def _api_decimal(value: Any) -> Decimal:
    out = _decimal(value)
    return out / Decimal(10**18) if abs(out) >= Decimal(10**12) else out
