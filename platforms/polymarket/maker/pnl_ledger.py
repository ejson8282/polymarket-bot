"""Read-only realized PnL accounting from authenticated CLOB trades.

The maker engine's exit target is not an execution price.  This module uses
confirmed trades instead, reconstructs the account's own maker fills, applies
the live CLOB V2 market fee parameters to taker fills, and realizes inventory
with FIFO cost basis.  Incomplete inventory is reported explicitly rather than
being presented as zero PnL.
"""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional


_ZERO = Decimal("0")
_FEE_QUANTUM = Decimal("0.00001")
_CONFIRMED_STATUSES = {"CONFIRMED", "TRADE_STATUS_CONFIRMED"}


def _decimal(value: Any) -> Optional[Decimal]:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _epoch(value: Any) -> Optional[int]:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError):
        return None
    # Some feeds use milliseconds while /data/trades currently uses seconds.
    return parsed // 1000 if parsed > 10_000_000_000 else parsed


def _number(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP))


def _platform_fee(
    size: Decimal,
    price: Decimal,
    fee_details: Any,
) -> Optional[Decimal]:
    """Return the V2 taker fee in USD, or ``None`` when it is unverifiable."""
    if fee_details is None:
        return _ZERO
    if not isinstance(fee_details, Mapping):
        return None
    rate = _decimal(fee_details.get("r"))
    exponent = _decimal(fee_details.get("e"))
    if rate is None or exponent is None or rate < 0 or exponent < 0:
        return None
    if rate == 0:
        return _ZERO
    if price <= 0 or price >= 1 or size <= 0:
        return None
    try:
        base = price * (Decimal("1") - price)
        # V2 production currently advertises integer exponents.  Decimal power
        # keeps the common e=1 path exact and rejects unsupported fractions.
        if exponent != exponent.to_integral_value():
            return None
        fee = size * rate * (base ** int(exponent))
    except (InvalidOperation, OverflowError, ValueError):
        return None
    return fee.quantize(_FEE_QUANTUM, rounding=ROUND_HALF_UP)


def _trade_status_confirmed(trade: Mapping[str, Any]) -> bool:
    return str(trade.get("status") or "").strip().upper() in _CONFIRMED_STATUSES


def _account_addresses(values: Iterable[str]) -> set[str]:
    return {
        str(value).strip().lower()
        for value in values
        if str(value).strip().lower().startswith("0x")
    }


def _maker_fills(
    trade: Mapping[str, Any],
    addresses: set[str],
) -> Iterable[dict]:
    orders = trade.get("maker_orders")
    if not isinstance(orders, list):
        return []
    fills = []
    for pos, order in enumerate(orders):
        if not isinstance(order, Mapping):
            continue
        maker = str(order.get("maker_address") or "").strip().lower()
        if not maker or maker not in addresses:
            continue
        fills.append(
            {
                "fill_id": (
                    f"{trade.get('id') or 'trade'}:maker:"
                    f"{order.get('order_id') or pos}"
                ),
                "market": trade.get("market"),
                "asset_id": order.get("asset_id"),
                "side": order.get("side"),
                "size": order.get("matched_amount"),
                "price": order.get("price"),
                "role": "MAKER",
                "epoch": _epoch(trade.get("match_time")),
                "trade_id": trade.get("id"),
                "transaction_hash": trade.get("transaction_hash"),
            }
        )
    return fills


def _taker_fill(trade: Mapping[str, Any]) -> dict:
    return {
        "fill_id": f"{trade.get('id') or 'trade'}:taker",
        "market": trade.get("market"),
        "asset_id": trade.get("asset_id"),
        "side": trade.get("side"),
        "size": trade.get("size"),
        "price": trade.get("price"),
        "role": "TAKER",
        "epoch": _epoch(trade.get("match_time")),
        "trade_id": trade.get("id"),
        "transaction_hash": trade.get("transaction_hash"),
    }


def normalized_account_fills(
    trades: Iterable[Any],
    account_addresses: Iterable[str],
) -> List[dict]:
    """Return the authenticated account's confirmed fills in time order."""
    addresses = _account_addresses(account_addresses)
    fills: List[dict] = []
    seen: set[str] = set()
    for raw in trades:
        if not isinstance(raw, Mapping) or not _trade_status_confirmed(raw):
            continue
        role = str(raw.get("trader_side") or "").strip().upper()
        candidates: Iterable[dict]
        if role == "TAKER":
            candidates = [_taker_fill(raw)]
        elif role == "MAKER" and addresses:
            candidates = _maker_fills(raw, addresses)
        else:
            continue
        for fill in candidates:
            fill_id = str(fill.get("fill_id") or "")
            if not fill_id or fill_id in seen:
                continue
            seen.add(fill_id)
            fills.append(fill)
    fills.sort(key=lambda item: (item.get("epoch") or 0, item["fill_id"]))
    return fills


def _fee_details_by_market(
    client: Any,
    fills: Iterable[Mapping[str, Any]],
) -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    markets = sorted(
        {
            str(fill.get("market") or "").strip()
            for fill in fills
            if fill.get("role") == "TAKER" and fill.get("market")
        }
    )
    for market in markets:
        try:
            info = client.get_clob_market_info(market)
        except Exception:
            details[market] = "unavailable"
            continue
        details[market] = info.get("fd") if isinstance(info, Mapping) else "unavailable"
    return details


def calculate_realized_pnl(
    fills: Iterable[Mapping[str, Any]],
    fee_details: Mapping[str, Any],
    *,
    now: Optional[datetime] = None,
    max_exits: int = 100,
) -> dict:
    """Calculate FIFO realized PnL and retain explicit verification state."""
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current_day = current.astimezone(timezone.utc).date()
    cutoff_24h = int(current.timestamp()) - 86400

    lots: Dict[str, Deque[dict]] = defaultdict(deque)
    exits: List[dict] = []
    invalid_fills = 0
    fee_unverified = 0
    unmatched_sells = 0
    total_fees = _ZERO
    verified_pnl = _ZERO
    today_verified_pnl = _ZERO
    verified_pnl_24h = _ZERO
    fill_count = 0

    for fill in fills:
        fill_count += 1
        asset_id = str(fill.get("asset_id") or "").strip()
        side = str(fill.get("side") or "").strip().upper()
        size = _decimal(fill.get("size"))
        price = _decimal(fill.get("price"))
        epoch = _epoch(fill.get("epoch"))
        role = str(fill.get("role") or "").strip().upper()
        market = str(fill.get("market") or "").strip()
        if (
            not asset_id
            or side not in {"BUY", "SELL"}
            or size is None
            or price is None
            or size <= 0
            or price <= 0
            or price >= 1
            or epoch is None
            or role not in {"MAKER", "TAKER"}
        ):
            invalid_fills += 1
            continue

        fee = _ZERO
        fee_known = True
        if role == "TAKER":
            fee_value = _platform_fee(size, price, fee_details.get(market, "unavailable"))
            if fee_value is None:
                fee_known = False
                fee_unverified += 1
            else:
                fee = fee_value
                total_fees += fee

        if side == "BUY":
            lots[asset_id].append(
                {
                    "size": size,
                    "price": price,
                    "fee": fee,
                    "fee_known": fee_known,
                    "epoch": epoch,
                    "fill_id": fill.get("fill_id"),
                }
            )
            continue

        remaining = size
        matched = _ZERO
        entry_notional = _ZERO
        entry_fee = _ZERO
        entry_fee_known = True
        while remaining > 0 and lots[asset_id]:
            lot = lots[asset_id][0]
            take = min(remaining, lot["size"])
            ratio = take / lot["size"]
            allocated_fee = lot["fee"] * ratio
            matched += take
            entry_notional += take * lot["price"]
            entry_fee += allocated_fee
            entry_fee_known = entry_fee_known and bool(lot["fee_known"])
            lot["size"] -= take
            lot["fee"] -= allocated_fee
            remaining -= take
            if lot["size"] <= Decimal("0.000000001"):
                lots[asset_id].popleft()

        if remaining > Decimal("0.000000001"):
            unmatched_sells += 1
        complete = (
            remaining <= Decimal("0.000000001")
            and fee_known
            and entry_fee_known
            and matched > 0
        )
        gross = matched * price - entry_notional
        allocated_exit_fee = fee * (matched / size) if size else _ZERO
        net = gross - entry_fee - allocated_exit_fee
        weighted_entry = entry_notional / matched if matched else None
        exits.append(
            {
                "fill_id": fill.get("fill_id"),
                "trade_id": fill.get("trade_id"),
                "transaction_hash": fill.get("transaction_hash"),
                "market": market or None,
                "asset_id": asset_id,
                "epoch": epoch,
                "role": role,
                "size": _number(size),
                "matched_size": _number(matched),
                "unmatched_size": _number(max(remaining, _ZERO)),
                "entry_price": _number(weighted_entry) if weighted_entry is not None else None,
                "exit_price": _number(price),
                "gross_pnl_usd": _number(gross) if complete else None,
                "entry_fee_usd": _number(entry_fee) if entry_fee_known else None,
                "exit_fee_usd": _number(allocated_exit_fee) if fee_known else None,
                "net_pnl_usd": _number(net) if complete else None,
                "complete": complete,
                "status": "verified" if complete else "needs_review",
            }
        )
        if complete:
            verified_pnl += net
            if epoch >= cutoff_24h:
                verified_pnl_24h += net
            if datetime.fromtimestamp(epoch, timezone.utc).date() == current_day:
                today_verified_pnl += net

    verified = [row for row in exits if row["complete"]]
    open_size = sum(
        (lot["size"] for queue in lots.values() for lot in queue),
        _ZERO,
    )
    complete = invalid_fills == 0 and fee_unverified == 0 and unmatched_sells == 0
    status = "ok" if complete and fill_count else ("partial" if fill_count else "empty")
    exits.sort(key=lambda row: (row["epoch"], str(row.get("fill_id") or "")), reverse=True)
    return {
        "version": 1,
        "status": status,
        "complete": complete,
        "method": "confirmed_trades_fifo_v2_market_fees",
        "fee_rounding_decimals": 5,
        "realized_pnl_usd": _number(verified_pnl),
        "realized_pnl_24h_usd": _number(verified_pnl_24h),
        "realized_pnl_today_utc_usd": _number(today_verified_pnl),
        "fees_usd": _number(total_fees),
        "verified_exit_count": len(verified),
        "exit_count": len(exits),
        "unmatched_sell_count": unmatched_sells,
        "fee_unverified_count": fee_unverified,
        "invalid_fill_count": invalid_fills,
        "open_inventory_size": _number(open_size),
        "realized_exits": exits[: max(0, int(max_exits))],
    }


def fetch_realized_pnl(
    client: Any,
    account_addresses: Iterable[str],
    *,
    now: Optional[datetime] = None,
) -> dict:
    """Fetch authenticated trades and return a read-only PnL snapshot."""
    trades = client.get_trades()
    fills = normalized_account_fills(
        trades if isinstance(trades, list) else [],
        account_addresses,
    )
    fees = _fee_details_by_market(client, fills)
    result = calculate_realized_pnl(fills, fees, now=now)
    result["trade_count"] = len(trades) if isinstance(trades, list) else 0
    result["fill_count"] = len(fills)
    result["updated_at"] = (
        now or datetime.now(timezone.utc)
    ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return result
