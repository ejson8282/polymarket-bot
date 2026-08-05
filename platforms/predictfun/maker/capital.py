from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from platforms.predictfun.maker.executor import AccountBalance, AccountPosition


@dataclass(frozen=True)
class CapitalProfile:
    name: str
    reference_equity: Decimal
    reserve: Decimal
    max_risk_notional: Decimal
    max_market_notional: Decimal
    max_order_notional: Decimal
    max_markets: int
    inventory_warning: Decimal
    daily_loss_stop: Decimal


MICRO_100 = CapitalProfile(
    name="micro_100",
    reference_equity=Decimal("100"),
    reserve=Decimal("30"),
    max_risk_notional=Decimal("70"),
    max_market_notional=Decimal("25"),
    max_order_notional=Decimal("8"),
    max_markets=3,
    inventory_warning=Decimal("30"),
    daily_loss_stop=Decimal("3"),
)

STANDARD_200 = CapitalProfile(
    name="standard_200",
    reference_equity=Decimal("200"),
    reserve=Decimal("40"),
    max_risk_notional=Decimal("160"),
    max_market_notional=Decimal("40"),
    max_order_notional=Decimal("15"),
    max_markets=6,
    inventory_warning=Decimal("75"),
    daily_loss_stop=Decimal("6"),
)


def select_capital_profile(
    equity: Decimal,
    *,
    previous: str = "",
    upgrade_at: Decimal = Decimal("160"),
    downgrade_at: Decimal = Decimal("140"),
) -> CapitalProfile:
    """Select a capital tier with hysteresis so fills do not flap profiles."""

    normalized = str(previous or "").strip().lower()
    if normalized == STANDARD_200.name and equity >= downgrade_at:
        return STANDARD_200
    if normalized == MICRO_100.name and equity < upgrade_at:
        return MICRO_100
    return STANDARD_200 if equity >= Decimal("150") else MICRO_100


def account_equity(
    account_id: str,
    balances: Iterable[AccountBalance],
    positions: Iterable[AccountPosition],
) -> Decimal:
    cash = sum(
        (row.total for row in balances if row.account_id == account_id),
        Decimal("0"),
    )
    position_value = sum(
        (row.value_usd for row in positions if row.account_id == account_id),
        Decimal("0"),
    )
    return max(Decimal("0"), cash + position_value)


def account_cash(
    account_id: str,
    balances: Iterable[AccountBalance],
) -> Decimal:
    return max(
        Decimal("0"),
        sum(
            (row.available for row in balances if row.account_id == account_id),
            Decimal("0"),
        ),
    )


def build_account_capital_rows(
    account_ids: Iterable[str],
    *,
    balances: Iterable[AccountBalance],
    positions: Iterable[AccountPosition],
    previous_profiles: Mapping[str, str] | None = None,
    fallback_equity: Decimal = Decimal("100"),
    allow_fallback: bool = True,
) -> list[dict[str, Any]]:
    previous_profiles = previous_profiles or {}
    balance_rows = list(balances)
    position_rows = list(positions)
    rows: list[dict[str, Any]] = []
    for raw_account_id in account_ids:
        account_id = str(raw_account_id).strip()
        if not account_id:
            continue
        observed = any(row.account_id == account_id for row in balance_rows) or any(
            row.account_id == account_id for row in position_rows
        )
        equity = account_equity(account_id, balance_rows, position_rows)
        cash = account_cash(account_id, balance_rows)
        if equity <= 0:
            if allow_fallback:
                equity = max(Decimal("0"), _decimal(fallback_equity))
                cash = equity
            else:
                rows.append(
                    {
                        "account_id": account_id,
                        "enabled": False,
                        "quote_enabled": False,
                        "equity": "0",
                        "available_cash": str(cash),
                        "capital_profile": "unavailable",
                        "capital_source": "observed" if observed else "missing",
                        "disabled_reason": (
                            "equity_zero" if observed else "equity_unavailable"
                        ),
                        "reserve": "0",
                        "max_account_notional": "0",
                        "max_account_market_notional": "0",
                        "max_order_notional": "0",
                        "max_markets": 0,
                        "inventory_warning": "0",
                        "daily_loss_stop": "0",
                    }
                )
                continue
        profile = select_capital_profile(
            equity, previous=str(previous_profiles.get(account_id) or "")
        )
        scale = min(Decimal("1"), equity / profile.reference_equity)
        max_account_notional = min(
            cash, profile.max_risk_notional * scale
        )
        # BUY exposure is cash-capped by max_account_notional. Keep a positive
        # per-order limit for SELL exits even when all cash is already invested.
        max_order_notional = profile.max_order_notional * scale
        rows.append(
            {
                "account_id": account_id,
                "enabled": True,
                "quote_enabled": cash > 0,
                "equity": str(equity),
                "available_cash": str(cash),
                "capital_profile": profile.name,
                "capital_source": "observed" if observed else "fallback",
                "disabled_reason": "" if cash > 0 else "cash_unavailable",
                "reserve": str(min(equity, profile.reserve * scale)),
                "max_account_notional": str(max_account_notional),
                "max_account_market_notional": str(
                    min(max_account_notional, profile.max_market_notional * scale)
                ),
                "max_order_notional": str(max_order_notional),
                "max_markets": profile.max_markets,
                "inventory_warning": str(profile.inventory_warning * scale),
                "daily_loss_stop": str(profile.daily_loss_stop * scale),
            }
        )
    return rows


def profile_to_jsonable(profile: CapitalProfile) -> dict[str, Any]:
    data = asdict(profile)
    return {
        key: str(value) if isinstance(value, Decimal) else value
        for key, value in data.items()
    }


def _decimal(value: Any) -> Decimal:
    try:
        number = Decimal(str(value or "0"))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal("0")
    return number if number.is_finite() else Decimal("0")
