"""Bounded read-only CLOB observations, never budget approvals or live commands.

The caller owns the authenticated client and its network timeout. This module
does not load credentials, create accounts, import an engine or write a journal.
"""

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import re

from .reward_ledger import canonical_account_uid


CLOB_HOST = "https://clob.polymarket.com"
INITIAL_CURSOR = "MA=="
END_CURSOR = "LTE="
READ_PARAMS = {
    "/balance-allowance": {"asset_type", "signature_type"},
    "/data/orders": {"next_cursor"},
    "/data/trades": {"next_cursor", "after", "before"},
    "/order-scoring": {"order_id"},
}
TRADE_STATUSES = {"MATCHED", "MINED", "RETRYING", "CONFIRMED", "FAILED"}


class ObservationError(ValueError):
    """Stable code only; never echo raw API payloads or transport exceptions."""


def _require(ok, code):
    if not ok:
        raise ObservationError(code)


def _address(value):
    _require(isinstance(value, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", value), "invalid_address")
    return value.lower()


def _id(value):
    _require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", value), "invalid_id")
    return value


def _amount(value, *, price=False):
    _require(isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,12})?", value), "invalid_decimal")
    result = Decimal(value)
    if price:
        _require(0 < result < 1, "invalid_price")
    return result


def _time(value):
    _require(isinstance(value, datetime) and value.tzinfo is not None and
             value.utcoffset().total_seconds() == 0, "utc_clock_required")
    return value


def _iso(value):
    return _time(value).isoformat().replace("+00:00", "Z")


def _parse_time(value):
    try:
        return _time(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ObservationError("invalid_timestamp") from exc


def _epoch(value):
    _require(type(value) in (int, str) and re.fullmatch(r"[0-9]{1,12}", str(value)), "invalid_epoch")
    result = int(value)
    _require(0 < result < 10_000_000_000, "epoch_seconds_required")
    return result


def _identity(identity):
    _require(isinstance(identity, dict) and set(identity) == {"chain_id", "signature_type", "maker_address"}, "identity_fields")
    _require(type(identity["chain_id"]) is int and identity["chain_id"] == 137, "unsupported_chain")
    _require(type(identity["signature_type"]) is int and identity["signature_type"] in {0, 1, 2}, "signature_type")
    result = {**identity, "maker_address": _address(identity["maker_address"])}
    return result


class ClobReadTransport:
    """GET-only facade over an existing V2 SDK client; no credential setup.

    Binding checks public client configuration, not independent exchange proof
    of credential ownership. Responses still need row-level identity checks.
    The provided SDK's underlying HTTP transport MUST have a finite timeout.
    """

    source = "clob_sdk_readonly"

    def __init__(self, client, identity):
        self._client = client
        self.identity = _identity(identity)
        self._check_binding()

    def _check_binding(self):
        client = self._client
        _require(client.host == CLOB_HOST, "unexpected_clob_host")
        _require(not isinstance(client.builder.signature_type, bool), "signature_type")
        actual = _identity({"chain_id": client.chain_id,
                            "signature_type": int(client.builder.signature_type),
                            "maker_address": client.builder.funder})
        _require(actual == self.identity, "client_identity_mismatch")

    def get(self, path, params):
        _require(path in READ_PARAMS and isinstance(params, dict) and
                 set(params) <= READ_PARAMS[path], "read_scope_violation")
        _require(all(type(v) in (str, int) for v in params.values()), "invalid_query")
        self._check_binding()
        headers = self._client._l2_headers("GET", path)
        return self._client._get(CLOB_HOST + path, headers=headers, params=dict(params))


def _read(transport, path, params):
    try:
        return transport.get(path, params)
    except Exception:
        raise ObservationError("read_failed") from None


def _orders(raw, maker, _after, _before):
    _require(isinstance(raw, dict), "invalid_order")
    _require(_address(raw.get("maker_address")) == maker, "order_maker_mismatch")
    quantity, matched = _amount(raw.get("original_size")), _amount(raw.get("size_matched"))
    _require(quantity > 0 and 0 <= matched <= quantity, "invalid_remaining")
    _require(raw.get("side") in {"BUY", "SELL"}, "invalid_side")
    status = raw.get("status")
    _require(status in {"LIVE", "MATCHED", "DELAYED", "CANCELED", "CANCELLED", "UNMATCHED", "PENDING"}, "unknown_order_status")
    oid = _id(raw.get("id"))
    return [{"key": oid, "order_id": oid, "condition_id": _id(raw.get("market")),
             "token_id": _id(raw.get("asset_id")), "side": raw["side"], "status": status,
             "price": format(_amount(raw.get("price"), price=True), "f"),
             "original_size": format(quantity, "f"), "matched_size": format(matched, "f"),
             "remaining_size": format(quantity - matched, "f")}]


def _trades(raw, maker, after, before):
    _require(isinstance(raw, dict), "invalid_trade")
    trade_id = _id(raw.get("id"))
    status = raw.get("status")
    if isinstance(status, str) and status.startswith("TRADE_STATUS_"):
        status = status[len("TRADE_STATUS_"):]
    _require(status in TRADE_STATUSES, "unknown_trade_status")
    epoch = _epoch(raw.get("match_time"))
    _require(after <= epoch <= before, "trade_outside_window")
    condition = _id(raw.get("market"))
    role = raw.get("trader_side")
    if role == "MAKER":
        _require(isinstance(raw.get("maker_orders"), list), "maker_components_missing")
        candidates = []
        for component in raw["maker_orders"]:
            _require(isinstance(component, dict), "invalid_maker_component")
            if _address(component.get("maker_address")) == maker:
                candidates.append({**component, "quantity": component.get("matched_amount")})
        _require(bool(candidates), "own_maker_component_missing")
    elif role == "TAKER":
        _require(_address(raw.get("maker_address")) == maker, "taker_maker_mismatch")
        candidates = [{**raw, "order_id": raw.get("taker_order_id"), "quantity": raw.get("size")}]
    else:
        raise ObservationError("unknown_trade_role")
    rows = []
    for item in candidates:
        oid, token = _id(item.get("order_id")), _id(item.get("asset_id"))
        _require(item.get("side") in {"BUY", "SELL"}, "own_side_unknown")
        quantity, price = _amount(item.get("quantity")), _amount(item.get("price"), price=True)
        _require(quantity > 0, "invalid_fill_size")
        rows.append({"key": f"{trade_id}:{role}:{oid}", "trade_id": trade_id, "order_id": oid,
                     "condition_id": condition, "token_id": token, "role": role, "side": item["side"],
                     "quantity": format(quantity, "f"), "price": format(price, "f"),
                     "notional_usdc": format(quantity * price, "f"), "fee_usdc": None,
                     "status": status, "match_time": epoch})
    return rows


def _pages(transport, path, params, normalize, maker, after, before, max_pages, max_rows):
    cursor, seen, rows, raw_count = INITIAL_CURSOR, set(), {}, 0
    for page_number in range(1, max_pages + 1):
        _require(cursor not in seen, "cursor_cycle")
        seen.add(cursor)
        body = _read(transport, path, {**params, "next_cursor": cursor})
        _require(isinstance(body, dict) and isinstance(body.get("data"), list), "invalid_page")
        next_cursor = body.get("next_cursor")
        _require(isinstance(next_cursor, str) and 0 < len(next_cursor) <= 256, "missing_cursor")
        if "count" in body:
            _require(type(body["count"]) is int and body["count"] == len(body["data"]), "page_count_mismatch")
        raw_count += len(body["data"])
        _require(raw_count <= max_rows, "row_limit")
        for raw in body["data"]:
            for row in normalize(raw, maker, after, before):
                prior = rows.get(row["key"])
                _require(prior is None or prior == row, "duplicate_conflict")
                rows[row["key"]] = row
                _require(len(rows) <= max_rows, "component_limit")
        if next_cursor == END_CURSOR:
            return {"rows": sorted(rows.values(), key=lambda r: r["key"]), "pages": page_number,
                    "pagination_complete": True, "atomic_snapshot": False}
        cursor = next_cursor
    raise ObservationError("page_limit")


def _collateral(transport, signature_type, spender):
    raw = _read(transport, "/balance-allowance", {"asset_type": "COLLATERAL", "signature_type": signature_type})
    _require(isinstance(raw, dict), "invalid_collateral")
    def units(value):
        _require(isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]{0,77})", value), "invalid_base_units")
        return format(Decimal(value).scaleb(-6), "f")
    cash = units(raw.get("balance"))
    allowances = raw.get("allowances")
    _require(isinstance(allowances, dict), "allowances_unknown")
    selected = {}
    for key, value in allowances.items():
        address = _address(key)
        _require(address not in selected, "duplicate_spender")
        selected[address] = value
    allowance = units(selected[spender]) if spender in selected else None
    return {"balance_usdc": cash, "allowance_usdc": allowance, "spender": spender,
            "allowance_reason": None if allowance is not None else "selected_spender_missing",
            "fill_coverage": None, "inventory": None, "effective_capital_usdc": None}


def _sample(fn, clock, ttl):
    started = _time(clock())
    try:
        value = fn()
        reason = None
    except ObservationError as exc:
        value, reason = None, str(exc)
    except Exception:
        value, reason = None, "read_failed"
    finished = _time(clock())
    _require(finished >= started, "clock_regression")
    return {"status": "unknown" if reason else "current", "reason": reason,
            "observed_from": _iso(started), "observed_to": _iso(finished), "max_age_sec": ttl,
            "current": value, "last_known": None}


def observation_at(report, *, now):
    """Re-evaluate ages without turning old results into fresh source samples."""
    result = deepcopy(report)
    current = _time(now)
    _require(type(result.get("schema_version")) is int and result["schema_version"] == 1 and
             result.get("kind") == "small_cap_lp_observation" and result.get("mode") == "readonly",
             "observation_schema")
    _require(all(result.get(flag) is False for flag in
                 ("live_enabled", "mutation_enabled", "budget_admission_enabled", "atomic_snapshot")),
             "observation_capabilities")
    generated = _parse_time(result["generated_at"])
    _require(current >= generated, "view_before_report")
    samples = list(result["samples"].values()) + list(result["scoring"].values())
    for sample in samples:
        _require(type(sample["max_age_sec"]) is int and 0 < sample["max_age_sec"] <= 300, "invalid_ttl")
        _require(_parse_time(sample["observed_from"]) <= _parse_time(sample["observed_to"]) <= generated,
                 "sample_time_order")
        age = (current - _parse_time(sample["observed_from"])).total_seconds()
        if sample["status"] == "current" and not 0 <= age <= sample["max_age_sec"]:
            sample["last_known"] = sample["current"]
            sample["current"] = None
            sample["status"], sample["reason"] = "stale", "sample_expired"
    result["checked_at"] = _iso(current)
    return result


def collect_account_observation(transport, identity, *, collateral_spender, trade_after, trade_before,
                                clock=lambda: datetime.now(timezone.utc), max_pages=10,
                                max_rows=1000, max_scoring=20, max_age_sec=60):
    """Read four data families; output never authorizes budget or trading.

    Trade coverage is limited to the explicit queried interval. Pagination
    completion is NOT an atomic account snapshot or a fill/source watermark.
    """
    account = _identity(identity)
    _require(_identity(transport.identity) == account, "transport_identity_mismatch")
    _require(transport.source in {"clob_sdk_readonly", "synthetic"}, "transport_provenance")
    spender = _address(collateral_spender)
    after, before = _epoch(trade_after), _epoch(trade_before)
    _require(after < before <= int(_time(clock()).timestamp()), "invalid_trade_window")
    for value, limit in ((max_pages, 100), (max_rows, 10000), (max_scoring, 100), (max_age_sec, 300)):
        _require(type(value) is int and 0 < value <= limit, "invalid_bound")
    maker = account["maker_address"]
    with localcontext() as ctx:
        ctx.prec = 100
        def open_orders():
            return _pages(transport, "/data/orders", {}, _orders, maker, after, before, max_pages, max_rows)
        samples = {
            "collateral": _sample(lambda: _collateral(transport, account["signature_type"], spender), clock, max_age_sec),
            "orders_before_scoring": _sample(open_orders, clock, max_age_sec),
            "trades": _sample(lambda: _pages(transport, "/data/trades", {"after": after, "before": before},
                                            _trades, maker, after, before, max_pages, max_rows), clock, max_age_sec),
        }
        before_data = samples["orders_before_scoring"]["current"]
        candidates = [o for o in before_data["rows"] if o["side"] == "BUY" and o["status"] == "LIVE" and
                      Decimal(o["remaining_size"]) > 0] if before_data else []
        scoring = {}
        def read_score(oid):
            body = _read(transport, "/order-scoring", {"order_id": oid})
            _require(isinstance(body, dict) and type(body.get("scoring")) is bool, "invalid_scoring")
            return {"order_id": oid, "scoring": body["scoring"]}
        for order in candidates[:max_scoring]:
            scoring[order["order_id"]] = _sample(lambda oid=order["order_id"]: read_score(oid), clock, max_age_sec)
        samples["orders"] = _sample(open_orders, clock, max_age_sec)
        after_data = samples["orders"]["current"]
        current_orders = {o["order_id"]: o for o in after_data["rows"]} if after_data else {}
        for order in candidates[:max_scoring]:
            score = scoring[order["order_id"]]
            if current_orders.get(order["order_id"]) != order:
                score["last_known"], score["current"] = score["current"], None
                score["status"], score["reason"] = "unknown", "order_changed_or_unverified"
        generated = _time(clock())
        report = {"schema_version": 1, "kind": "small_cap_lp_observation", "mode": "readonly",
                  "source": transport.source, "identity": account,
                  "identity_binding": "configured_transport_and_row_checks_not_exchange_attestation",
                  "account_uid": canonical_account_uid(**account), "generated_at": _iso(generated),
                  "live_enabled": False, "mutation_enabled": False, "budget_admission_enabled": False,
                  "atomic_snapshot": False, "trade_window": {"after": after, "before": before},
                  "blocked_reasons": ["inventory_assets_not_integrated", "source_fill_coverage_unproven",
                                      "runtime_not_integrated"],
                  "samples": samples, "scoring": scoring,
                  "scoring_unchecked_order_ids": sorted({o["order_id"] for o in candidates[max_scoring:]} |
                      {oid for oid, order in current_orders.items() if order["side"] == "BUY" and
                       order["status"] == "LIVE" and Decimal(order["remaining_size"]) > 0 and oid not in scoring})}
        return observation_at(report, now=generated)
