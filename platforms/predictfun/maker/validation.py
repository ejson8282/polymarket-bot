from __future__ import annotations

from decimal import Decimal
from typing import Any


def validate_final_order(
    *,
    original_market: dict[str, Any],
    fresh_market: dict[str, Any],
    token_id: str,
    side: str,
    price: Decimal,
    size: Decimal,
    max_notional: Decimal,
) -> dict[str, Any]:
    """Fail-closed validation immediately before a post-only submission."""

    if not isinstance(original_market, dict) or not isinstance(fresh_market, dict):
        return _blocked("market_payload_invalid")
    original_id = _int(original_market.get("id"))
    fresh_id = _int(fresh_market.get("id"))
    if original_id <= 0 or fresh_id != original_id:
        return _blocked("market_identity_changed")

    status = str(fresh_market.get("status") or "").upper()
    trading_status = str(fresh_market.get("tradingStatus") or "").upper()
    if not status or not trading_status:
        return _blocked("market_status_missing")
    if status != "OPEN" or trading_status != "OPEN":
        return _blocked(f"market_not_open status={status} trading_status={trading_status}")

    if "feeRateBps" not in fresh_market or "feeRateBps" not in original_market:
        return _blocked("market_execution_mode_missing field=feeRateBps")
    original_fee = _strict_int(original_market.get("feeRateBps"))
    fresh_fee = _strict_int(fresh_market.get("feeRateBps"))
    if original_fee is None or fresh_fee is None or original_fee < 0 or fresh_fee < 0:
        return _blocked("market_execution_mode_invalid field=feeRateBps")
    if fresh_fee != original_fee:
        return _blocked("market_execution_mode_changed field=feeRateBps")
    for field in ("isNegRisk", "isYieldBearing"):
        if field not in fresh_market or field not in original_market:
            return _blocked(f"market_execution_mode_missing field={field}")
        original_flag = _strict_bool(original_market.get(field))
        fresh_flag = _strict_bool(fresh_market.get(field))
        if original_flag is None or fresh_flag is None:
            return _blocked(f"market_execution_mode_invalid field={field}")
        if fresh_flag != original_flag:
            return _blocked(f"market_execution_mode_changed field={field}")

    original_variant = str(original_market.get("marketVariant") or "").strip().upper()
    fresh_variant = str(fresh_market.get("marketVariant") or "").strip().upper()
    if not original_variant or not fresh_variant:
        return _blocked("market_execution_mode_missing field=marketVariant")
    if original_variant != fresh_variant:
        return _blocked("market_execution_mode_changed field=marketVariant")

    original_precision = _strict_int(original_market.get("decimalPrecision"))
    fresh_precision = _strict_int(fresh_market.get("decimalPrecision"))
    if (
        original_precision is None
        or fresh_precision is None
        or not 0 <= original_precision <= 18
        or not 0 <= fresh_precision <= 18
    ):
        return _blocked("market_execution_mode_invalid field=decimalPrecision")
    if original_precision != fresh_precision:
        return _blocked("market_execution_mode_changed field=decimalPrecision")

    if _canonical_outcomes(original_market) is None or _canonical_outcomes(fresh_market) is None:
        return _blocked("market_outcomes_not_canonical")
    original_outcome = _outcome_by_token(original_market, token_id)
    outcome = _outcome_by_token(fresh_market, token_id)
    if not original_outcome or not outcome:
        return _blocked("outcome_token_missing")
    if _outcome_name(original_outcome) != _outcome_name(outcome):
        return _blocked("outcome_token_changed")

    side = str(side or "").upper()
    if side not in {"BUY", "SELL"}:
        return _blocked("unsupported_side")
    if price <= 0 or price >= 1 or size <= 0:
        return _blocked("invalid_price_or_size")
    tick = Decimal(1).scaleb(-fresh_precision)
    if price != price.quantize(tick):
        return _blocked("invalid_price_tick")
    if max_notional <= 0:
        return _blocked("invalid_max_notional")
    notional = price * size
    if notional > max_notional:
        return _blocked("max_notional_exceeded", notional=notional)

    best_bid = _level_price(outcome.get("bestBid"))
    best_ask = _level_price(outcome.get("bestAsk"))
    if best_bid > 0 and best_ask > 0 and best_ask <= best_bid:
        return _blocked("fresh_orderbook_crossed", best_bid=best_bid, best_ask=best_ask, notional=notional)
    if side == "BUY" and best_ask > 0 and price >= best_ask:
        return _blocked("post_only_buy_would_cross", best_bid=best_bid, best_ask=best_ask, notional=notional)
    if side == "SELL" and best_bid > 0 and price <= best_bid:
        return _blocked("post_only_sell_would_cross", best_bid=best_bid, best_ask=best_ask, notional=notional)

    return {
        "ok": True,
        "reason": "",
        "best_bid": str(best_bid),
        "best_ask": str(best_ask),
        "notional": str(notional),
    }


def _outcome_by_token(market: dict[str, Any], token_id: str) -> dict[str, Any]:
    outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
    for outcome in outcomes:
        if isinstance(outcome, dict) and str(outcome.get("onChainId") or "") == str(token_id or ""):
            return outcome
    return {}


def _canonical_outcomes(market: dict[str, Any]) -> dict[str, dict[str, Any]] | None:
    outcomes = market.get("outcomes") if isinstance(market.get("outcomes"), list) else []
    if len(outcomes) != 2 or not all(isinstance(outcome, dict) for outcome in outcomes):
        return None
    by_name = {_outcome_name(outcome): outcome for outcome in outcomes}
    return by_name if set(by_name) == {"YES", "NO"} else None


def _outcome_name(outcome: dict[str, Any]) -> str:
    return str(outcome.get("name") or "").strip().upper()


def _level_price(value: Any) -> Decimal:
    if isinstance(value, dict):
        value = value.get("price")
    try:
        return Decimal(str(value or "0"))
    except Exception:
        return Decimal("0")


def _int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _strict_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _strict_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _blocked(
    reason: str,
    *,
    best_bid: Decimal = Decimal("0"),
    best_ask: Decimal = Decimal("0"),
    notional: Decimal = Decimal("0"),
) -> dict[str, Any]:
    return {
        "ok": False,
        "reason": reason,
        "best_bid": str(best_bid),
        "best_ask": str(best_ask),
        "notional": str(notional),
    }
