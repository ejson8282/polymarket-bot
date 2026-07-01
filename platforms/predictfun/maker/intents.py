from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    account_id: str
    market_id: int
    outcome: str
    side: str
    price: Decimal
    size: Decimal
    notional: Decimal
    reason: str
    is_neg_risk: bool = False
    is_yield_bearing: bool = False
    market_mode: str = "standard"
    token_id: str = ""
    fee_rate_bps: int = 0


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_intents_from_plans(
    plans: list[dict[str, Any]],
    *,
    accounts_config: dict[str, Any] | list[Any] | None = None,
    inventory_positions: list[dict[str, Any]] | None = None,
    inventory_config: dict[str, Any] | None = None,
    planner_config: dict[str, Any] | None = None,
) -> list[OrderIntent]:
    intents: list[OrderIntent] = []
    accounts = _configured_accounts(accounts_config)
    assignment = _account_assignment(accounts_config)
    positions = _position_map(inventory_positions or [])
    reserved: dict[tuple[str, int, str], Decimal] = {}
    inventory = inventory_config or {}
    planner = planner_config or {}
    reserved_notional_by_account: dict[str, Decimal] = {}
    reserved_notional_by_account_market: dict[tuple[str, int], Decimal] = {}
    max_account_notional = _dec(planner.get("max_account_notional"))
    max_account_market_notional = _dec(planner.get("max_account_market_notional"))
    for plan_index, plan in enumerate(plans):
        if not plan.get("can_quote"):
            continue
        market = plan.get("market") if isinstance(plan.get("market"), dict) else {}
        market_id = int(market.get("id") or 0)
        is_neg_risk, is_yield_bearing, market_mode = _market_flags(market)
        fee_rate_bps = int(_dec(market.get("fee_rate_bps")))
        for account in _accounts_for_plan(accounts, plan_index, assignment):
            account_id = str(account["account_id"])
            for quote in list(plan.get("yes_quotes") or []) + list(plan.get("no_quotes") or []):
                outcome = str(quote.get("outcome") or "")
                side = str(quote.get("side") or "")
                price = _dec(quote.get("price"))
                size = _dec(quote.get("size"))
                if not _can_add_buy_inventory(
                    positions,
                    account_id=account_id,
                    market_id=market_id,
                    outcome=outcome,
                    size=size,
                    inventory=inventory,
                    reserved=reserved,
                ):
                    continue
                intent = _intent(
                    account_id=account_id,
                    market_id=market_id,
                    outcome=outcome,
                    side=side,
                    price=price,
                    size=size,
                    reason=str(quote.get("reason") or ""),
                    is_neg_risk=is_neg_risk,
                    is_yield_bearing=is_yield_bearing,
                    market_mode=market_mode,
                    token_id=_token_id_for_outcome(market, outcome),
                    fee_rate_bps=fee_rate_bps,
                )
                if not _can_add_notional(
                    reserved_notional_by_account,
                    reserved_notional_by_account_market,
                    account_id=account_id,
                    market_id=market_id,
                    notional=intent.notional,
                    max_account_notional=max_account_notional,
                    max_account_market_notional=max_account_market_notional,
                ):
                    continue
                intents.append(intent)
                _reserve_buy_inventory(reserved, intent)
                _reserve_notional(reserved_notional_by_account, reserved_notional_by_account_market, intent)
            intents.extend(
                _inventory_exit_intents(
                    plan,
                    account_id=account_id,
                    market_id=market_id,
                    positions=positions,
                    inventory=inventory,
                    is_neg_risk=is_neg_risk,
                    is_yield_bearing=is_yield_bearing,
                    market_mode=market_mode,
                    fee_rate_bps=fee_rate_bps,
                )
            )
    return intents


def stable_intent_id(
    *,
    account_id: str = "acct01",
    market_id: int,
    outcome: str,
    side: str,
    price: Decimal,
    size: Decimal,
) -> str:
    raw = "|".join([account_id, str(market_id), outcome.upper(), side.upper(), str(price), str(size)])
    return "pf-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def load_previous_intents(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    intents = data.get("intents") if isinstance(data, dict) else []
    return intents if isinstance(intents, list) else []


def build_intent_state(
    *,
    environment: str,
    plans: list[dict[str, Any]],
    previous_intents: list[dict[str, Any]],
    accounts_config: dict[str, Any] | list[Any] | None = None,
    inventory_positions: list[dict[str, Any]] | None = None,
    inventory_config: dict[str, Any] | None = None,
    planner_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    intents = build_intents_from_plans(
        plans,
        accounts_config=accounts_config,
        inventory_positions=inventory_positions,
        inventory_config=inventory_config,
        planner_config=planner_config,
    )
    desired = [intent_to_jsonable(intent) for intent in intents]
    previous_by_id = {
        str(item.get("intent_id")): item
        for item in previous_intents
        if isinstance(item, dict) and item.get("intent_id")
    }
    desired_by_id = {item["intent_id"]: item for item in desired}

    creates = [item for item in desired if item["intent_id"] not in previous_by_id]
    keeps = [item for item in desired if item["intent_id"] in previous_by_id]
    cancels = [
        item
        for intent_id, item in previous_by_id.items()
        if intent_id not in desired_by_id
    ]

    total_notional = sum(_dec(item.get("notional")) for item in desired)
    by_account: dict[str, dict[str, Any]] = {}
    by_mode: dict[str, dict[str, Any]] = {}
    for item in desired:
        account_id = str(item.get("account_id") or "acct01")
        row = by_account.setdefault(account_id, {"desired": 0, "total_notional": Decimal("0")})
        row["desired"] += 1
        row["total_notional"] += _dec(item.get("notional"))
        mode = str(item.get("market_mode") or "standard")
        mode_row = by_mode.setdefault(
            mode,
            {"desired": 0, "buy": 0, "sell": 0, "total_notional": Decimal("0"), "buy_notional": Decimal("0")},
        )
        notional = _dec(item.get("notional"))
        mode_row["desired"] += 1
        mode_row["total_notional"] += notional
        if str(item.get("side") or "").upper() == "BUY":
            mode_row["buy"] += 1
            mode_row["buy_notional"] += notional
        else:
            mode_row["sell"] += 1
    by_account_json = {
        account_id: {
            "desired": row["desired"],
            "total_notional": str(row["total_notional"]),
        }
        for account_id, row in sorted(by_account.items())
    }
    return {
        "ts": utc_now(),
        "environment": environment,
        "mode": "dry_run",
        "summary": {
            "desired": len(desired),
            "create": len(creates),
            "keep": len(keeps),
            "cancel": len(cancels),
            "total_notional": str(total_notional),
            "accounts": len(by_account),
            "market_modes": {
                mode: {
                    "desired": row["desired"],
                    "buy": row["buy"],
                    "sell": row["sell"],
                    "total_notional": str(row["total_notional"]),
                    "buy_notional": str(row["buy_notional"]),
                }
                for mode, row in sorted(by_mode.items())
            },
        },
        "accounts": by_account_json,
        "diff": {
            "create": creates,
            "keep": keeps,
            "cancel": cancels,
        },
        "intents": desired,
    }


def write_intent_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(path)


def intent_to_jsonable(intent: OrderIntent) -> dict[str, Any]:
    data = asdict(intent)
    for key, value in list(data.items()):
        if isinstance(value, Decimal):
            data[key] = str(value)
    return data


def _intent(
    *,
    account_id: str,
    market_id: int,
    outcome: str,
    side: str,
    price: Decimal,
    size: Decimal,
    reason: str,
    is_neg_risk: bool = False,
    is_yield_bearing: bool = False,
    market_mode: str = "standard",
    token_id: str = "",
    fee_rate_bps: int = 0,
) -> OrderIntent:
    notional = price * size
    intent_id = stable_intent_id(
        account_id=account_id,
        market_id=market_id,
        outcome=outcome,
        side=side,
        price=price,
        size=size,
    )
    return OrderIntent(
        intent_id=intent_id,
        account_id=account_id,
        market_id=market_id,
        outcome=outcome,
        side=side,
        price=price,
        size=size,
        notional=notional,
        reason=reason,
        is_neg_risk=is_neg_risk,
        is_yield_bearing=is_yield_bearing,
        market_mode=market_mode,
        token_id=token_id,
        fee_rate_bps=fee_rate_bps,
    )


def _configured_accounts(raw: dict[str, Any] | list[Any] | None) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        rows = raw
        max_active = len(rows) or 1
    elif isinstance(raw, dict):
        if raw.get("enabled") is False:
            return [{"account_id": "acct01"}]
        rows = raw.get("ids") or raw.get("account_ids") or raw.get("accounts") or []
        max_active = int(_dec(raw.get("max_active_accounts"), "10") or Decimal("10"))
        if not rows:
            rows = [f"acct{i:02d}" for i in range(1, max_active + 1)]
    else:
        rows = ["acct01"]
        max_active = 1

    accounts: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in rows:
        if isinstance(item, dict):
            account_id = str(item.get("account_id") or item.get("id") or item.get("name") or "").strip()
            payload = dict(item)
        else:
            account_id = str(item or "").strip()
            payload = {"account_id": account_id}
        if not account_id or account_id in seen:
            continue
        payload["account_id"] = account_id
        accounts.append(payload)
        seen.add(account_id)
        if len(accounts) >= max(1, max_active):
            break
    return accounts or [{"account_id": "acct01"}]


def _account_assignment(raw: dict[str, Any] | list[Any] | None) -> str:
    if isinstance(raw, dict):
        assignment = str(raw.get("assignment") or raw.get("mode") or "round_robin").lower()
        if assignment in {"all", "all_accounts"}:
            return "all"
    return "round_robin"


def _accounts_for_plan(accounts: list[dict[str, Any]], plan_index: int, assignment: str) -> list[dict[str, Any]]:
    if assignment == "all":
        return accounts
    return [accounts[plan_index % len(accounts)]]


def _can_add_notional(
    by_account: dict[str, Decimal],
    by_account_market: dict[tuple[str, int], Decimal],
    *,
    account_id: str,
    market_id: int,
    notional: Decimal,
    max_account_notional: Decimal,
    max_account_market_notional: Decimal,
) -> bool:
    if max_account_notional > 0 and by_account.get(account_id, Decimal("0")) + notional > max_account_notional:
        return False
    key = (account_id, market_id)
    if max_account_market_notional > 0 and by_account_market.get(key, Decimal("0")) + notional > max_account_market_notional:
        return False
    return True


def _reserve_notional(
    by_account: dict[str, Decimal],
    by_account_market: dict[tuple[str, int], Decimal],
    intent: OrderIntent,
) -> None:
    by_account[intent.account_id] = by_account.get(intent.account_id, Decimal("0")) + intent.notional
    key = (intent.account_id, intent.market_id)
    by_account_market[key] = by_account_market.get(key, Decimal("0")) + intent.notional


def _position_map(positions: list[dict[str, Any]]) -> dict[tuple[str, int, str], Decimal]:
    out: dict[tuple[str, int, str], Decimal] = {}
    for row in positions:
        if not isinstance(row, dict):
            continue
        account_id = str(row.get("account_id") or "acct01")
        market_id = int(_dec(row.get("market_id")))
        outcome = str(row.get("outcome") or "").upper()
        if not market_id or outcome not in {"YES", "NO"}:
            continue
        out[(account_id, market_id, outcome)] = _dec(row.get("size"))
    return out


def _can_add_buy_inventory(
    positions: dict[tuple[str, int, str], Decimal],
    *,
    account_id: str,
    market_id: int,
    outcome: str,
    size: Decimal,
    inventory: dict[str, Any],
    reserved: dict[tuple[str, int, str], Decimal],
) -> bool:
    if str(inventory.get("enabled", True)).lower() in {"false", "0", "no"}:
        return True
    cap = _dec(inventory.get("max_long_size_per_outcome"), "30")
    if cap <= 0:
        return True
    key = (account_id, market_id, outcome.upper())
    current = positions.get(key, Decimal("0"))
    pending = reserved.get(key, Decimal("0"))
    return current + pending + size <= cap


def _reserve_buy_inventory(reserved: dict[tuple[str, int, str], Decimal], intent: OrderIntent) -> None:
    if intent.side.upper() != "BUY":
        return
    key = (intent.account_id, intent.market_id, intent.outcome.upper())
    reserved[key] = reserved.get(key, Decimal("0")) + intent.size


def _inventory_exit_intents(
    plan: dict[str, Any],
    *,
    account_id: str,
    market_id: int,
    positions: dict[tuple[str, int, str], Decimal],
    inventory: dict[str, Any],
    is_neg_risk: bool,
    is_yield_bearing: bool,
    market_mode: str,
    fee_rate_bps: int,
) -> list[OrderIntent]:
    if str(inventory.get("enabled", True)).lower() in {"false", "0", "no"}:
        return []
    pct = _dec(inventory.get("exit_quote_size_pct_of_position"), "1")
    min_exit = _dec(inventory.get("min_exit_size"), "1")
    if pct <= 0:
        return []
    market = plan.get("market") if isinstance(plan.get("market"), dict) else {}
    tick = Decimal(1).scaleb(-int(_dec(market.get("decimal_precision"), "2")))
    out: list[OrderIntent] = []
    for outcome in ("YES", "NO"):
        position = positions.get((account_id, market_id, outcome), Decimal("0"))
        if position < min_exit:
            continue
        price = _exit_price(plan, outcome=outcome, tick=tick, inventory=inventory)
        if price <= 0:
            continue
        size = max(min_exit, (position * pct).quantize(Decimal("0.000001")))
        size = min(size, position)
        out.append(
            _intent(
                account_id=account_id,
                market_id=market_id,
                outcome=outcome,
                side="SELL",
                price=price,
                size=size,
                reason=f"inventory_exit position={position}",
                is_neg_risk=is_neg_risk,
                is_yield_bearing=is_yield_bearing,
                market_mode=market_mode,
                token_id=_token_id_for_outcome(market, outcome),
                fee_rate_bps=fee_rate_bps,
            )
        )
    return out


def _market_flags(market: dict[str, Any]) -> tuple[bool, bool, str]:
    is_neg_risk = _bool(
        market.get("is_neg_risk", market.get("isNegRisk", market.get("neg_risk", market.get("negRisk"))))
    )
    is_yield_bearing = _bool(
        market.get(
            "is_yield_bearing",
            market.get("isYieldBearing", market.get("yield_bearing", market.get("yieldBearing"))),
        )
    )
    if is_neg_risk and is_yield_bearing:
        mode = "neg_risk_yield_bearing"
    elif is_neg_risk:
        mode = "neg_risk"
    elif is_yield_bearing:
        mode = "yield_bearing"
    else:
        mode = "standard"
    return is_neg_risk, is_yield_bearing, mode


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _token_id_for_outcome(market: dict[str, Any], outcome: str) -> str:
    if outcome.upper() == "YES":
        return str(market.get("yes_token_id") or market.get("yesTokenId") or "")
    if outcome.upper() == "NO":
        return str(market.get("no_token_id") or market.get("noTokenId") or "")
    return ""


def _exit_price(plan: dict[str, Any], *, outcome: str, tick: Decimal, inventory: dict[str, Any]) -> Decimal:
    yes_bid = _dec(plan.get("best_yes_bid"))
    yes_ask = _dec(plan.get("best_yes_ask"))
    if yes_bid <= 0 or yes_ask <= 0 or yes_ask <= yes_bid:
        return Decimal("0")
    if outcome == "YES":
        best_bid = yes_bid
        best_ask = yes_ask
    else:
        best_bid = max(Decimal("0"), Decimal("1") - yes_ask)
        best_ask = max(Decimal("0"), Decimal("1") - yes_bid)
    edge_ticks = int(_dec(inventory.get("exit_edge_ticks"), "1"))
    price = best_ask - tick * max(1, edge_ticks)
    price = max(best_bid + tick, price)
    return min(Decimal("1") - tick, max(tick, price))


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)
