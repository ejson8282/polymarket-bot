"""Offline small_cap_lp contract v1. No runner, transport, or live capability.

All public functions are pure: callers supply time and already-decoded data.
Synthetic evidence validates the wire format, never permission to trade.
"""

from __future__ import annotations

import copy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import re
from typing import Any
from uuid import UUID

from .account_roster import MAX_ACCOUNTS, market_universe_sha256
from .reward_ledger import canonical_account_uid


SCHEMA_VERSION = 1
STRATEGY = "small_cap_lp"
DASHBOARD_ROUTE_FAMILY = "/api/pm/small-cap"
FIXTURE_TIME = "2026-09-09T02:00:00Z"
COMMANDS = (
    "pause_account", "resume_account", "set_budget_tier",
    "request_reassessment", "apply_assignment",
)
DISABLED_REASONS = (
    "runtime_not_deployed", "transport_not_implemented",
    "atomic_budget_not_integrated", "cross_host_conflict_guard_not_integrated",
)
ACCOUNTING_TYPES = (
    "native_lp", "sponsored_lp", "maker_rebate", "trading_pnl",
    "inventory_unrealized_pnl", "estimated_lp", "fees", "net_cash_flow",
    "cash_flow_adjusted_nav_change",
)
BUDGET_INPUTS = (
    "asset_equity_usdc", "cash_usdc", "inventory_cost_usdc",
    "remaining_buy_usdc", "pending_buy_reserved_usdc",
    "unknown_buy_reserved_usdc", "fee_reserve_usdc",
)
BUDGET_DERIVED = ("effective_limit_usdc", "occupied_usdc", "available_usdc")
_ID = r"[a-z0-9][a-z0-9_.-]{0,63}"
_HASH = r"[0-9a-f]{64}"
_ADDRESS = r"0x[0-9a-f]{40}"
_CONDITION = r"0x[0-9a-f]{64}"
_TOKEN = r"[1-9][0-9]{0,77}"
_ORDER_ID = r"0x[0-9a-f]{64}"
_DECIMAL = r"-?(?:0|[1-9][0-9]{0,23})(?:\.[0-9]{1,18})?"
_FRESHNESS_FIELDS = "status observed_at max_age_sec age_sec reason"


class SmallCapContractError(ValueError):
    """A stable code and field path, without echoing untrusted field values."""

    def __init__(self, code: str, path: str):
        self.code = code
        self.path = path
        super().__init__(f"{code}: {path}")


def _require(ok: bool, path: str, code: str = "invalid_contract") -> None:
    if not ok:
        raise SmallCapContractError(code, path)


def _obj(value: Any, fields: str, path: str) -> dict:
    _require(type(value) is dict, path)
    _require(set(value) == set(fields.split()), path, "invalid_fields")
    return value


def _list(value: Any, path: str) -> list:
    _require(type(value) is list, path)
    return value


def _integer(value: Any, path: str, low: int = 0, high: int = 2**53 - 1) -> int:
    _require(type(value) is int and low <= value <= high, path)
    return value


def _match(value: Any, pattern: str, path: str) -> str:
    _require(type(value) is str and re.fullmatch(pattern, value) is not None, path)
    return value


def _choice(value: Any, choices: tuple, path: str) -> None:
    _require(type(value) is str and value in choices, path)


def _decimal(value: Any, path: str, *, signed: bool = False) -> Decimal:
    _match(value, _DECIMAL, path)
    result = Decimal(value)
    _require(signed or not value.startswith("-"), path, "invalid_money")
    return result


def _time(value: Any, path: str = "time") -> datetime:
    _match(value, r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?Z", path)
    try:
        fmt = "%Y-%m-%dT%H:%M:%S.%fZ" if "." in value else "%Y-%m-%dT%H:%M:%SZ"
        return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
    except ValueError:
        raise SmallCapContractError("invalid_timestamp", path) from None


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _elapsed_us(start: datetime, end: datetime) -> int:
    delta = end - start
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def business_day(at: str) -> str:
    """Date of the BJT 08:00-start day, equivalent to the UTC calendar date."""
    return _time(at).date().isoformat()


def _freshness(value: Any, generated_at: datetime, path: str) -> str:
    """Check the producer's serialized claim at report generation, not receipt."""
    f = _obj(value, _FRESHNESS_FIELDS, path)
    status = f["status"]
    _choice(status, ("fresh", "stale", "missing", "unavailable"), path + ".status")
    ttl = _integer(f["max_age_sec"], path + ".max_age_sec", 1, 86400)
    if status in ("missing", "unavailable"):
        _require(f["observed_at"] is None and f["age_sec"] is None, path)
    else:
        elapsed = _elapsed_us(_time(f["observed_at"], path + ".observed_at"), generated_at)
        age = (elapsed + 999999) // 1000000
        _integer(f["age_sec"], path + ".age_sec")
        _require(elapsed >= 0 and f["age_sec"] == age, path, "invalid_age")
        _require((age <= ttl) == (status == "fresh"), path, "freshness_mismatch")
    if status == "fresh":
        _require(f["reason"] is None, path)
    else:
        _match(f["reason"], _ID, path + ".reason")
    return status


def freshness_status_at(raw: Any, *, generated_at: str, now: str) -> str:
    """Read effective freshness without changing serialized ages or timestamps."""
    generated, consumed = _time(generated_at), _time(now)
    _require(generated <= consumed, "generated_at", "future_generation")
    status = _freshness(raw, generated, "freshness")
    if status == "fresh" and _elapsed_us(_time(raw["observed_at"]), consumed) > raw["max_age_sec"] * 1000000:
        return "stale"
    return status


def _assert_not_expired(value: Any, now: datetime, path: str) -> None:
    """After structural checks, reject newly expired current claims as a whole."""
    if isinstance(value, dict):
        if set(value) == set(_FRESHNESS_FIELDS.split()):
            if value["status"] == "fresh":
                _require(_elapsed_us(_time(value["observed_at"]), now) <= value["max_age_sec"] * 1000000,
                         path, "evidence_expired")
            return
        for key, child in value.items():
            _assert_not_expired(child, now, path + "." + key)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_not_expired(child, now, f"{path}[{index}]")


def _metric(value: Any, now: datetime, path: str, *, signed: bool = False,
            maximum: Decimal | None = None) -> Decimal | None:
    m = _obj(value, "current last_known freshness", path)
    status = _freshness(m["freshness"], now, path + ".freshness")
    if status == "fresh":
        _require(m["last_known"] is None, path)
        number = _decimal(m["current"], path + ".current", signed=signed)
        _require(maximum is None or number <= maximum, path, "metric_out_of_range")
        return number
    _require(m["current"] is None, path, "unknown_money")
    if status == "stale":
        number = _decimal(m["last_known"], path + ".last_known", signed=signed)
        _require(maximum is None or number <= maximum, path, "metric_out_of_range")
    else:
        _require(m["last_known"] is None, path, "unknown_money")
    return None


def _uid(identity: dict, path: str) -> str:
    _integer(identity["chain_id"], path + ".chain_id", 1)
    _integer(identity["signature_type"], path + ".signature_type", 0, 2)
    _match(identity["maker_address"], _ADDRESS, path + ".maker_address")
    uid = canonical_account_uid(identity["chain_id"], identity["signature_type"], identity["maker_address"])
    _require(identity["account_uid"] == uid, path, "identity_mismatch")
    _require(identity["account_uid_key"] == hashlib.sha256(uid.encode()).hexdigest()[:16],
             path, "identity_mismatch")
    return uid


def _maker_key(identity: dict) -> tuple[int, str]:
    return identity["chain_id"], identity["maker_address"].lower()


def _budget(value: Any, now: datetime, path: str) -> None:
    b = _obj(value, "tier_usdc " + " ".join(BUDGET_INPUTS + BUDGET_DERIVED), path)
    _choice(b["tier_usdc"], ("100", "150", "200"), path + ".tier_usdc")
    values = {k: _metric(b[k], now, path + "." + k) for k in BUDGET_INPUTS + BUDGET_DERIVED}
    if any(values[k] is None for k in BUDGET_INPUTS):
        _require(all(values[k] is None for k in BUDGET_DERIVED), path, "unknown_budget")
        return
    # These are report consistency checks, not a reservation or admission engine.
    with localcontext() as ctx:
        ctx.prec = 80
        limit = min(Decimal(b["tier_usdc"]), values["asset_equity_usdc"])
        holds = sum(values[k] for k in BUDGET_INPUTS[3:])
        occupied = values["inventory_cost_usdc"] + holds
        available = max(Decimal(0), min(limit - occupied, values["cash_usdc"] - holds))
        _require((values["effective_limit_usdc"], values["occupied_usdc"], values["available_usdc"])
                 == (limit, occupied, available), path, "budget_arithmetic_mismatch")


def _book(value: Any, now: datetime, path: str) -> None:
    b = _obj(value, "token_id ws_received book_event_at rest_fetched rest_complete "
             "usable_source external_front_depth_usdc own_orders_scope_complete", path)
    _match(b["token_id"], _TOKEN, path + ".token_id")
    ws = _freshness(b["ws_received"], now, path + ".ws_received")
    rest = _freshness(b["rest_fetched"], now, path + ".rest_fetched")
    _require(type(b["rest_complete"]) is bool and type(b["own_orders_scope_complete"]) is bool, path)
    if b["book_event_at"] is not None:
        event = _time(b["book_event_at"], path + ".book_event_at")
        _require(event <= now, path, "future_book_event")
    _choice(b["usable_source"], ("ws", "rest", "none"), path + ".usable_source")
    if b["usable_source"] == "ws":
        _require(ws == "fresh", path, "stale_book")
    if b["usable_source"] == "rest":
        _require(rest == "fresh" and b["rest_complete"], path, "stale_book")
    depth = _metric(b["external_front_depth_usdc"], now, path + ".external_front_depth_usdc")
    if depth is not None:
        _require(b["own_orders_scope_complete"] and b["usable_source"] != "none",
                 path, "external_depth_unverified")


def _scoring(value: Any, revision: int, effective_at: datetime, now: datetime, path: str) -> None:
    s = _obj(value, "status samples independent_valid_count", path)
    _choice(s["status"], ("unverified", "synthetic_validated", "stale"), path + ".status")
    count = 0
    seen = set()
    last_observed = {}
    has_stale = False
    for i, raw in enumerate(_list(s["samples"], path + ".samples")):
        p = f"{path}.samples[{i}]"
        sample = _obj(raw, "sample_id source_snapshot_id assignment_revision yes_order_id no_order_id "
                      "scoring_freshness scoring percentage q_min", p)
        for key in ("sample_id", "source_snapshot_id"):
            _match(sample[key], _ID, p + "." + key)
        for key in ("yes_order_id", "no_order_id"):
            _match(sample[key], _ORDER_ID, p + "." + key)
        _require(sample["yes_order_id"] != sample["no_order_id"], p, "duplicate_order")
        _integer(sample["assignment_revision"], p + ".assignment_revision")
        _require(sample["assignment_revision"] == revision, p, "revision_mismatch")
        for key in ("sample_id", "source_snapshot_id"):
            marker = (key, sample[key])
            _require(marker not in seen, p, "duplicate_evidence")
            seen.add(marker)
        score_status = _freshness(sample["scoring_freshness"], now, p + ".scoring_freshness")
        if score_status == "fresh":
            _require(type(sample["scoring"]) is bool, p)
        else:
            _require(sample["scoring"] is None, p)
        percentage = _metric(sample["percentage"], now, p + ".percentage", maximum=Decimal(1))
        q_min = _metric(sample["q_min"], now, p + ".q_min")
        for key in ("scoring_freshness", "percentage", "q_min"):
            freshness = sample[key] if key == "scoring_freshness" else sample[key]["freshness"]
            if freshness["observed_at"] is not None:
                observed = _time(freshness["observed_at"])
                _require(observed > effective_at, p + "." + key, "evidence_before_revision")
                _require(key not in last_observed or observed > last_observed[key], p, "duplicate_evidence")
                last_observed[key] = observed
        valid = sample["scoring"] is True and percentage is not None and percentage > 0 and q_min is not None and q_min > 0
        count = count + 1 if valid else 0
        has_stale |= "stale" in (score_status, sample["percentage"]["freshness"]["status"],
                                sample["q_min"]["freshness"]["status"])
    _integer(s["independent_valid_count"], path + ".independent_valid_count")
    _require(s["independent_valid_count"] == count, path, "evidence_count_mismatch")
    expected = "synthetic_validated" if count >= 3 else ("stale" if has_stale else "unverified")
    _require(s["status"] == expected, path, "evidence_status_mismatch")


def _cancel(value: Any, now: datetime, path: str) -> None:
    c = _obj(value, "status protection_triggered_at cancel_requested_at cancel_confirmed_at "
             "confirmation_latency_ms freshness", path)
    status = _freshness(c["freshness"], now, path + ".freshness")
    _choice(c["status"], ("unavailable", "pending", "unknown", "synthetic_confirmed"), path)
    stamps = []
    for key in ("protection_triggered_at", "cancel_requested_at", "cancel_confirmed_at"):
        stamps.append(_time(c[key], path + "." + key) if c[key] is not None else None)
    trigger, requested, confirmed = stamps
    _require(all(t is None or t <= now for t in stamps), path, "future_cancellation")
    if c["freshness"]["observed_at"] is not None:
        observed = _time(c["freshness"]["observed_at"])
        _require(all(t is None or t <= observed for t in stamps), path, "cancellation_after_observation")
    if requested is not None:
        _require(trigger is not None and requested >= trigger, path)
    if c["status"] == "synthetic_confirmed":
        _require(status == "fresh" and all(t is not None for t in stamps), path)
        _require(confirmed >= requested, path)
        latency = _integer(c["confirmation_latency_ms"], path + ".confirmation_latency_ms")
        _require(latency == (_elapsed_us(trigger, confirmed) + 999) // 1000, path)
    else:
        _require(confirmed is None and c["confirmation_latency_ms"] is None, path)
        if c["status"] == "pending":
            _require(status == "fresh" and requested is not None, path)
        if c["status"] == "unavailable":
            _require(status in ("missing", "unavailable") and all(t is None for t in stamps), path)


def _economics(value: Any, now: datetime, path: str) -> None:
    e = _obj(value, "status horizon_end lower_reward_increment_usdc scoring_uptime "
             "yes_exit_stress_usdc no_exit_stress_usdc verified_history_stress_usdc "
             "fees_usdc switching_cost_usdc net_increment_usdc history_calibration "
             "missing_reasons expansion_allowed", path)
    _choice(e["status"], ("unavailable", "uncalibrated", "shadow_only"), path)
    _choice(e["history_calibration"], ("missing", "synthetic_only"), path)
    if e["horizon_end"] is not None:
        end = _time(e["horizon_end"], path + ".horizon_end")
        _require(now < end <= now + timedelta(hours=24), path, "invalid_horizon")
    values = {}
    for key in ("lower_reward_increment_usdc", "scoring_uptime", "yes_exit_stress_usdc",
                "no_exit_stress_usdc", "verified_history_stress_usdc", "fees_usdc",
                "switching_cost_usdc", "net_increment_usdc"):
        values[key] = _metric(e[key], now, path + "." + key, signed=key == "net_increment_usdc",
                              maximum=Decimal(1) if key == "scoring_uptime" else None)
    reasons = _list(e["missing_reasons"], path + ".missing_reasons")
    for reason in reasons:
        _match(reason, _ID, path + ".missing_reasons")
    _require(len(reasons) == len(set(reasons)), path)
    if any(v is None for v in values.values()) or e["horizon_end"] is None:
        _require(bool(reasons) and values["net_increment_usdc"] is None, path, "unknown_economics")
        _require(e["status"] != "shadow_only", path, "unknown_economics")
    else:
        _require(e["status"] == "shadow_only" and e["history_calibration"] == "synthetic_only"
                 and not reasons, path, "uncalibrated_economics")
        with localcontext() as ctx:
            ctx.prec = 80
            net = (values["lower_reward_increment_usdc"] * values["scoring_uptime"]
                   - max(values[k] for k in ("yes_exit_stress_usdc", "no_exit_stress_usdc", "verified_history_stress_usdc"))
                   - values["fees_usdc"] - values["switching_cost_usdc"])
            _require(values["net_increment_usdc"] == net, path, "economics_arithmetic_mismatch")
    if e["history_calibration"] == "missing":
        _require(values["verified_history_stress_usdc"] is None, path, "uncalibrated_economics")
    if e["status"] == "unavailable":
        _require(all(v is None for v in values.values()) and e["horizon_end"] is None, path)
    _require(e["expansion_allowed"] is False, path, "live_capability_forbidden")


def _assignment(value: Any, account: dict, universe: dict, now: datetime, path: str) -> None:
    a = _obj(value, "assignment_id assignment_revision account_uid condition_id status quote "
             "scoring books cancellation economics", path)
    _match(a["assignment_id"], _ID, path + ".assignment_id")
    _require(a["account_uid"] == account["identity"]["account_uid"], path, "identity_mismatch")
    _integer(a["assignment_revision"], path + ".assignment_revision")
    _require(a["assignment_revision"] == account["assignment_revision"], path, "revision_mismatch")
    _match(a["condition_id"], _CONDITION, path + ".condition_id")
    _require(a["condition_id"] in universe, path, "market_outside_universe")
    _choice(a["status"], ("proposed", "shadow", "retire_pending"), path + ".status")
    q = _obj(a["quote"], "yes_price no_price yes_shares no_shares notional_usdc", path + ".quote")
    with localcontext() as ctx:
        ctx.prec = 80
        prices = [_decimal(q[k], path + ".quote." + k) for k in ("yes_price", "no_price")]
        _require(all(0 < p < 1 for p in prices), path, "invalid_price")
        sizes = [Decimal(_match(q[k], r"[1-9][0-9]{0,17}", path + ".quote." + k))
                 for k in ("yes_shares", "no_shares")]
        notional = _decimal(q["notional_usdc"], path + ".quote.notional_usdc")
        _require(notional == sum(p * s for p, s in zip(prices, sizes)), path, "notional_mismatch")
    _scoring(a["scoring"], a["assignment_revision"], _time(account["assignment_effective_at"]), now, path + ".scoring")
    books = _list(a["books"], path + ".books")
    _require(len(books) == 2, path)
    for i, book in enumerate(books):
        _book(book, now, f"{path}.books[{i}]")
    market = universe[a["condition_id"]]
    _require({b["token_id"] for b in books} == {market["token_id"], market["paired_token_id"]},
             path, "book_identity_mismatch")
    _cancel(a["cancellation"], now, path + ".cancellation")
    _economics(a["economics"], now, path + ".economics")


def _accounting(rows: Any, account: dict, now: datetime, path: str) -> None:
    seen = set()
    for i, row in enumerate(_list(rows, path)):
        p = f"{path}[{i}]"
        r = _obj(row, "business_day account_uid condition_id accounting_type asset_address amount_usdc", p)
        _require(r["account_uid"] == account["identity"]["account_uid"], p, "identity_mismatch")
        _match(r["business_day"], r"[0-9]{4}-[0-9]{2}-[0-9]{2}", p + ".business_day")
        day = _time(r["business_day"] + "T00:00:00Z", p + ".business_day")
        _require(day <= now, p, "future_business_day")
        _match(r["condition_id"], _CONDITION, p + ".condition_id")
        _match(r["asset_address"], _ADDRESS, p + ".asset_address")
        _choice(r["accounting_type"], ACCOUNTING_TYPES, p + ".accounting_type")
        key = tuple(r[k] for k in ("business_day", "account_uid", "condition_id", "accounting_type", "asset_address"))
        _require(key not in seen, p, "duplicate_accounting_record")
        seen.add(key)
        _metric(r["amount_usdc"], now, p + ".amount_usdc", signed=r["accounting_type"] in
                ("trading_pnl", "inventory_unrealized_pnl", "net_cash_flow", "cash_flow_adjusted_nav_change"))


def validate_state(raw: Any, *, now: str) -> dict:
    """Validate at generation, then expire current claims at consumption time."""
    consumed = _time(now)
    s = _obj(raw, "schema_version kind strategy_type runtime_scope source mode generated_at "
             "freshness transport capabilities group universe accounts accounting_complete receipts", "state")
    clock = _time(s["generated_at"])
    _require(clock <= consumed, "generated_at", "future_generation")
    _require(type(s["schema_version"]) is int and s["schema_version"] == SCHEMA_VERSION, "schema_version")
    _require(s["kind"] == "small_cap_lp_state" and s["strategy_type"] == STRATEGY
             and s["runtime_scope"] == STRATEGY and s["source"] == "synthetic", "identity")
    _choice(s["mode"], ("unavailable", "proposal_only", "shadow"), "mode")
    state_freshness = _freshness(s["freshness"], clock, "freshness")
    if s["freshness"]["observed_at"] is not None:
        _require(_time(s["freshness"]["observed_at"]) <= _time(s["generated_at"]), "freshness")
    if s["mode"] == "unavailable":
        _require(state_freshness == "unavailable" and not s["accounts"], "mode")
    t = _obj(s["transport"], "status service_url authentication dashboard_route_family", "transport")
    _require(t == {"status": "unavailable", "service_url": None, "authentication": None,
                   "dashboard_route_family": DASHBOARD_ROUTE_FAMILY}, "transport", "live_capability_forbidden")
    caps = _obj(s["capabilities"], "stage mutation_commands disabled_reasons", "capabilities")
    _require(caps["stage"] == "contract_only" and caps["disabled_reasons"] == list(DISABLED_REASONS), "capabilities")
    _obj(caps["mutation_commands"], " ".join(COMMANDS), "capabilities.mutation_commands")
    _require(all(v is False for v in caps["mutation_commands"].values()), "capabilities", "live_capability_forbidden")
    group = _obj(s["group"], "strategy_group revision routing_roster_sha256 market_universe_sha256", "group")
    _match(group["strategy_group"], _ID, "group.strategy_group")
    _integer(group["revision"], "group.revision")
    _match(group["routing_roster_sha256"], _HASH, "group.routing_roster_sha256")
    _match(group["market_universe_sha256"], _HASH, "group.market_universe_sha256")
    u = _obj(s["universe"], "markets night_markets", "universe")
    universe, tokens = {}, set()
    for section in ("markets", "night_markets"):
        for market in _list(u[section], "universe." + section):
            m = _obj(market, "condition_id token_id paired_token_id", "market")
            condition = _match(m["condition_id"], _CONDITION, "market.condition_id")
            yes = _match(m["token_id"], _TOKEN, "market.token_id")
            no = _match(m["paired_token_id"], _TOKEN, "market.paired_token_id")
            _require(condition not in universe and yes != no and not {yes, no} & tokens, "market", "duplicate_market")
            universe[condition] = m
            tokens.update((yes, no))
    _require(group["market_universe_sha256"] == market_universe_sha256(u), "universe", "universe_hash_mismatch")
    accounts = _list(s["accounts"], "accounts")
    _require(len(accounts) <= MAX_ACCOUNTS, "accounts")
    indexes, uids, ids, uid_keys, makers = set(), set(), set(), set(), set()
    for i, raw_account in enumerate(accounts):
        p = f"accounts[{i}]"
        a = _obj(raw_account, "identity strategy_group market_universe_sha256 assignment_revision assignment_effective_at "
                 "status budget assignments accounting", p)
        ident = _obj(a["identity"], "account_index account_id host_id chain_id signature_type maker_address "
                     "account_uid account_uid_key", p + ".identity")
        index = _integer(ident["account_index"], p + ".account_index", 1, MAX_ACCOUNTS)
        _match(ident["account_id"], _ID, p + ".account_id")
        _match(ident["host_id"], _ID, p + ".host_id")
        uid = _uid(ident, p + ".identity")
        _require(index not in indexes and uid not in uids and ident["account_id"] not in ids
                 and ident["account_uid_key"] not in uid_keys, p, "duplicate_account")
        maker = _maker_key(ident)
        _require(maker not in makers, p, "duplicate_maker")
        makers.add(maker)
        indexes.add(index)
        uids.add(uid)
        ids.add(ident["account_id"])
        uid_keys.add(ident["account_uid_key"])
        _require(a["strategy_group"] == group["strategy_group"]
                 and a["market_universe_sha256"] == group["market_universe_sha256"], p, "identity_mismatch")
        _integer(a["assignment_revision"], p + ".assignment_revision")
        _require(_time(a["assignment_effective_at"], p + ".assignment_effective_at") <= clock,
                 p + ".assignment_effective_at", "future_revision")
        _require(a["status"] == "not_deployed", p, "live_capability_forbidden")
        _budget(a["budget"], clock, p + ".budget")
        conditions, assignments = set(), set()
        for j, assignment in enumerate(_list(a["assignments"], p + ".assignments")):
            _assignment(assignment, a, universe, clock, f"{p}.assignments[{j}]")
            horizon = assignment["economics"]["horizon_end"]
            if horizon is not None:
                _require(consumed < _time(horizon),
                         f"{p}.assignments[{j}].economics.horizon_end", "horizon_expired")
            _require(assignment["condition_id"] not in conditions and assignment["assignment_id"] not in assignments,
                     p, "duplicate_assignment")
            conditions.add(assignment["condition_id"])
            assignments.add(assignment["assignment_id"])
        _accounting(a["accounting"], a, clock, p + ".accounting")
    _require(s["accounting_complete"] is False, "accounting_complete")
    _require(s["receipts"] == [], "receipts", "runtime_not_deployed")
    _assert_not_expired(s, consumed, "state")
    return copy.deepcopy(s)


def validate_transition(previous: dict, current: dict, *, previous_now: str, now: str) -> dict:
    """Check two supplied reports, not a durable registry or atomic CAS store."""
    before = validate_state(previous, now=previous_now)
    after = validate_state(current, now=now)
    _require(before["mode"] != "unavailable" and after["mode"] != "unavailable",
             "transition", "state_not_available")
    _require(_time(now) >= _time(previous_now), "transition", "time_regression")
    _require(_time(after["generated_at"]) >= _time(before["generated_at"]), "transition", "generation_regression")
    def token_pairs(report: dict) -> dict:
        return {m["condition_id"]: (m["token_id"], m["paired_token_id"])
                for section in ("markets", "night_markets") for m in report["universe"][section]}
    old_pairs, pairs = token_pairs(before), token_pairs(after)
    for condition in old_pairs.keys() & pairs.keys():
        _require(old_pairs[condition] == pairs[condition], "universe", "condition_token_remap")
    old_group, group = before["group"], after["group"]
    _require(group["strategy_group"] == old_group["strategy_group"], "group", "identity_mismatch")
    _require(group["revision"] >= old_group["revision"], "group.revision", "revision_regression")
    if any(group[k] != old_group[k] for k in ("market_universe_sha256", "routing_roster_sha256")):
        _require(group["revision"] > old_group["revision"], "group.revision", "revision_not_advanced")
    old_by_index = {a["identity"]["account_index"]: a for a in before["accounts"]}
    old_by_id = {a["identity"]["account_id"]: a for a in before["accounts"]}
    old_by_uid = {a["identity"]["account_uid"]: a for a in before["accounts"]}
    old_by_maker = {_maker_key(a["identity"]): a for a in before["accounts"]}
    for account in after["accounts"]:
        ident = account["identity"]
        matches = [row for row in (old_by_index.get(ident["account_index"]),
                                   old_by_id.get(ident["account_id"]),
                                   old_by_uid.get(ident["account_uid"]),
                                   old_by_maker.get(_maker_key(ident))) if row is not None]
        if not matches:
            _require(group["revision"] > old_group["revision"], "group.revision", "revision_not_advanced")
            continue
        prior = matches[0]
        _require(all(row["identity"] == prior["identity"] for row in matches),
                 "account.identity", "identity_reference_conflict")
        _require(ident == prior["identity"], "account.identity", "fixed_ownership_mismatch")
        _require(account["assignment_revision"] >= prior["assignment_revision"],
                 "assignment_revision", "revision_regression")
        effective = _time(account["assignment_effective_at"])
        old_effective = _time(prior["assignment_effective_at"])
        if account["assignment_revision"] == prior["assignment_revision"]:
            _require(effective == old_effective, "assignment_effective_at", "revision_boundary_mismatch")
        else:
            _require(effective > _time(before["generated_at"]) and effective > old_effective,
                     "assignment_effective_at", "revision_boundary_not_advanced")
        def assignment_inputs(row: dict) -> str:
            rows = [{k: a[k] for k in ("assignment_id", "condition_id", "quote", "status")}
                    for a in row["assignments"]]
            return json.dumps(sorted(rows, key=lambda a: a["assignment_id"]), sort_keys=True)
        if (assignment_inputs(account) != assignment_inputs(prior)
                or account["budget"]["tier_usdc"] != prior["budget"]["tier_usdc"]):
            _require(account["assignment_revision"] > prior["assignment_revision"],
                     "assignment_revision", "revision_not_advanced")
        if account["assignment_revision"] > prior["assignment_revision"]:
            def evidence_keys(row: dict) -> set:
                keys = set()
                for assignment in row["assignments"]:
                    for sample in assignment["scoring"]["samples"]:
                        for field in ("sample_id", "source_snapshot_id"):
                            keys.add((assignment["condition_id"], field, sample[field]))
                        for field in ("scoring_freshness", "percentage", "q_min"):
                            freshness = sample[field] if field == "scoring_freshness" else sample[field]["freshness"]
                            if freshness["observed_at"] is not None:
                                keys.add((assignment["condition_id"], field, _time(freshness["observed_at"])))
                return keys
            _require(not evidence_keys(account) & evidence_keys(prior), "scoring", "evidence_from_prior_revision")
    if set(old_by_index) != {a["identity"]["account_index"] for a in after["accounts"]}:
        _require(group["revision"] > old_group["revision"], "group.revision", "revision_not_advanced")
    return after


def _uuid(value: Any, path: str) -> None:
    _match(value, r"[0-9a-f-]{36}", path)
    try:
        parsed = UUID(value)
    except ValueError:
        raise SmallCapContractError("invalid_uuid", path) from None
    _require(str(parsed) == value and parsed.version == 4, path, "invalid_uuid")


def _command_shape(raw: Any) -> dict:
    c = _obj(raw, "schema_version strategy_type command_id idempotency_key action created_at expires_at "
             "strategy_group group_revision account_index account_uid account_uid_key host_id "
             "routing_roster_sha256 market_universe_sha256 expected_assignment_revision payload", "command")
    _require(type(c["schema_version"]) is int and c["schema_version"] == SCHEMA_VERSION, "command.schema_version")
    _require(c["strategy_type"] == STRATEGY, "command.strategy_type")
    _uuid(c["command_id"], "command.command_id")
    _uuid(c["idempotency_key"], "command.idempotency_key")
    _choice(c["action"], COMMANDS, "command.action")
    _match(c["strategy_group"], _ID, "command.strategy_group")
    _match(c["host_id"], _ID, "command.host_id")
    _match(c["account_uid"], r"[1-9][0-9]*:[012]:0x[0-9a-f]{40}", "command.account_uid")
    _require(c["account_uid_key"] == hashlib.sha256(c["account_uid"].encode()).hexdigest()[:16],
             "command.account_uid_key", "identity_mismatch")
    _integer(c["account_index"], "command.account_index", 1, MAX_ACCOUNTS)
    for field in ("group_revision", "expected_assignment_revision"):
        _integer(c[field], "command." + field)
    for field in ("routing_roster_sha256", "market_universe_sha256"):
        _match(c[field], _HASH, "command." + field)
    start, end = _time(c["created_at"]), _time(c["expires_at"])
    _require(start < end <= start + timedelta(minutes=5), "command.expires_at")
    if c["action"] == "set_budget_tier":
        p = _obj(c["payload"], "tier_usdc", "command.payload")
        _choice(p["tier_usdc"], ("100", "150", "200"), "command.payload.tier_usdc")
    elif c["action"] == "apply_assignment":
        p = _obj(c["payload"], "proposal_id", "command.payload")
        _match(p["proposal_id"], _HASH, "command.payload.proposal_id")
    else:
        _obj(c["payload"], "", "command.payload")
    return c


def validate_command(raw: Any, state: dict, *, now: str) -> dict:
    """Validate references only. Passing this function never enables a command."""
    s = validate_state(state, now=now)
    c = _command_shape(raw)
    _require(_time(c["created_at"]) <= _time(now) < _time(c["expires_at"]), "command", "command_expired")
    _require(s["freshness"]["status"] == "fresh", "state", "state_not_fresh")
    for key in ("strategy_group", "routing_roster_sha256", "market_universe_sha256"):
        _require(c[key] == s["group"][key], "command." + key, "identity_mismatch")
    _require(c["group_revision"] == s["group"]["revision"], "command.group_revision", "revision_conflict")
    account = next((a for a in s["accounts"] if a["identity"]["account_index"] == c["account_index"]), None)
    _require(account is not None, "command.account_index", "unknown_account")
    for key in ("account_uid", "account_uid_key", "host_id"):
        _require(c[key] == account["identity"][key], "command." + key, "identity_mismatch")
    _require(c["expected_assignment_revision"] == account["assignment_revision"],
             "command.expected_assignment_revision", "revision_conflict")
    return copy.deepcopy(c)


def _command_hash(command: dict) -> str:
    return hashlib.sha256(json.dumps(command, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def reject_command(raw: Any, state: dict, *, now: str) -> dict:
    """Return a local rejection; no dispatch, persistence, or runtime receipt."""
    command = validate_command(raw, state, now=now)
    return {
        "schema_version": SCHEMA_VERSION, "source": "contract_validator", "generated_at": now,
        "command": command, "command_sha256": _command_hash(command),
        "status": "rejected", "last_known_status": None, "reason": "runtime_not_deployed",
        "dispatched": False, "result": None, "applied_assignment_revision": None,
        "freshness": _stamp(now, now),
        "runtime_receipt_freshness": _stamp(None, now, absent="unavailable", reason="runtime_not_deployed"),
    }


def validate_receipt(raw: Any, command: dict, *, now: str) -> dict:
    """Validate a local rejection or explicitly synthetic pending/unknown sample."""
    c = _command_shape(command)
    r = _obj(raw, "schema_version source generated_at command command_sha256 status last_known_status reason dispatched "
             "result applied_assignment_revision freshness runtime_receipt_freshness", "receipt")
    generated, consumed = _time(r["generated_at"]), _time(now)
    _require(generated <= consumed, "receipt.generated_at", "future_generation")
    _require(_time(c["created_at"]) <= generated, "receipt.generated_at", "receipt_before_command")
    _require(type(r["schema_version"]) is int and r["schema_version"] == SCHEMA_VERSION, "receipt.schema_version")
    embedded = _command_shape(r["command"])
    _require(embedded == c and r["command_sha256"] == _command_hash(c), "receipt", "receipt_identity_mismatch")
    _choice(r["source"], ("contract_validator", "synthetic_fixture"), "receipt.source")
    _choice(r["status"], ("rejected", "pending", "unknown"), "receipt.status")
    _require(r["dispatched"] is False and r["result"] is None and r["applied_assignment_revision"] is None,
             "receipt", "live_capability_forbidden")
    status = _freshness(r["freshness"], generated, "receipt.freshness")
    if r["freshness"]["observed_at"] is not None:
        _require(_time(r["freshness"]["observed_at"]) >= _time(c["created_at"]), "receipt", "receipt_before_command")
    runtime = _freshness(r["runtime_receipt_freshness"], generated, "receipt.runtime_receipt_freshness")
    _require(runtime == "unavailable", "receipt.runtime_receipt_freshness", "runtime_not_deployed")
    _match(r["reason"], _ID, "receipt.reason")
    if r["source"] == "contract_validator":
        _require((status == "fresh" and r["status"] == "rejected" and r["reason"] == "runtime_not_deployed")
                 or (status != "fresh" and r["status"] == "unknown" and r["last_known_status"] == "rejected"), "receipt")
    if status != "fresh":
        _require(r["status"] == "unknown", "receipt", "stale_receipt")
    _require(r["last_known_status"] is None or
             (r["status"] == "unknown" and r["last_known_status"] in ("pending", "rejected")), "receipt")
    _assert_not_expired(r, consumed, "receipt")
    return copy.deepcopy(r)


def _stamp(at: str | None, now: str, *, ttl: int = 300, absent: str = "missing",
           reason: str = "not_observed") -> dict:
    age = (_elapsed_us(_time(at), _time(now)) + 999999) // 1000000 if at else None
    status = absent if at is None else ("fresh" if age <= ttl else "stale")
    return {"status": status, "observed_at": at, "max_age_sec": ttl, "age_sec": age,
            "reason": None if status == "fresh" else ("sample_expired" if status == "stale" else reason)}


def _sample(value: str | None, at: str | None, now: str, *, ttl: int = 300) -> dict:
    f = _stamp(at, now, ttl=ttl)
    return {"current": value if f["status"] == "fresh" else None,
            "last_known": value if f["status"] == "stale" else None, "freshness": f}


def synthetic_state(scenario: str, *, now: str = FIXTURE_TIME) -> dict:
    """Build reproducible in-memory examples, with no production observations."""
    _choice(scenario, ("unavailable", "proposed", "shadow", "stale"), "scenario")
    clock = _time(now)
    at = _iso(clock - timedelta(seconds=600 if scenario == "stale" else 0))
    universe = {"markets": [{"condition_id": "0x" + char * 64, "token_id": str(n),
                              "paired_token_id": str(n + 1)} for char, n in (("a", 101), ("b", 201))],
                "night_markets": []}
    digest = market_universe_sha256(universe)
    group = {"strategy_group": "fixture-small-cap", "revision": 1,
             "routing_roster_sha256": hashlib.sha256(b"synthetic-roster-placeholder").hexdigest(),
             "market_universe_sha256": digest}
    state = {
        "schema_version": SCHEMA_VERSION, "kind": "small_cap_lp_state", "strategy_type": STRATEGY,
        "runtime_scope": STRATEGY, "source": "synthetic",
        "mode": "unavailable" if scenario == "unavailable" else ("proposal_only" if scenario == "proposed" else "shadow"),
        "generated_at": now, "freshness": _stamp(at if scenario != "unavailable" else None, now,
                                                absent="unavailable", reason="runtime_not_deployed"),
        "transport": {"status": "unavailable", "service_url": None, "authentication": None,
                      "dashboard_route_family": DASHBOARD_ROUTE_FAMILY},
        "capabilities": {"stage": "contract_only", "mutation_commands": dict.fromkeys(COMMANDS, False),
                         "disabled_reasons": list(DISABLED_REASONS)},
        "group": group, "universe": universe, "accounts": [], "accounting_complete": False, "receipts": [],
    }
    for index, tier in (() if scenario == "unavailable" else ((7, "200"), (12, "100"))):
        uid = canonical_account_uid(137, 2, "0x" + f"{index:040x}")
        ident = {"account_index": index, "account_id": f"fixture-{index}", "host_id": f"fixture-host-{index}",
                 "chain_id": 137, "signature_type": 2, "maker_address": "0x" + f"{index:040x}",
                 "account_uid": uid, "account_uid_key": hashlib.sha256(uid.encode()).hexdigest()[:16]}
        amounts = dict.fromkeys(BUDGET_INPUTS + BUDGET_DERIVED, "0")
        amounts.update({k: tier for k in ("asset_equity_usdc", "cash_usdc", "effective_limit_usdc", "available_usdc")})
        account = {"identity": ident, "strategy_group": group["strategy_group"],
                   "market_universe_sha256": digest, "assignment_revision": 1,
                   "assignment_effective_at": _iso(_time(at) - timedelta(seconds=180)), "status": "not_deployed",
                   "budget": {"tier_usdc": tier, **{k: _sample(v, at, now) for k, v in amounts.items()}},
                   "assignments": [], "accounting": []}
        for i, market in enumerate(universe["markets"][:2 if index == 7 else 1]):
            scoring = {"status": "unverified", "samples": [], "independent_valid_count": 0}
            if scenario in ("shadow", "stale"):
                for n in range(3):
                    sampled = _iso(_time(at) - timedelta(seconds=(2 - n) * 60))
                    scoring["samples"].append({
                        "sample_id": f"sample-{n}", "source_snapshot_id": f"snapshot-{n}",
                        "assignment_revision": 1,
                        "yes_order_id": "0x" + hashlib.sha256(f"synthetic-{index}-{i}-yes".encode()).hexdigest(),
                        "no_order_id": "0x" + hashlib.sha256(f"synthetic-{index}-{i}-no".encode()).hexdigest(),
                        "scoring_freshness": _stamp(sampled, now), "scoring": True if scenario == "shadow" else None,
                        "percentage": _sample("0.05", sampled, now), "q_min": _sample("1", sampled, now),
                    })
                scoring.update(status="synthetic_validated" if scenario == "shadow" else "stale",
                               independent_valid_count=3 if scenario == "shadow" else 0)
            books = []
            for token in (market["token_id"], market["paired_token_id"]):
                books.append({"token_id": token, "ws_received": _stamp(_iso(_time(at) - timedelta(hours=1)), now, ttl=30),
                              "book_event_at": _iso(_time(at) - timedelta(hours=1)),
                              "rest_fetched": _stamp(at, now, ttl=30), "rest_complete": True,
                              "usable_source": "none" if scenario == "stale" else "rest",
                              "external_front_depth_usdc": _sample(None, None, now), "own_orders_scope_complete": False})
            economics = {"status": "uncalibrated", "horizon_end": None, "history_calibration": "missing",
                         "missing_reasons": ["fees_unknown", "exit_depth_unknown", "history_unavailable"], "expansion_allowed": False}
            for key in ("lower_reward_increment_usdc", "scoring_uptime", "yes_exit_stress_usdc", "no_exit_stress_usdc",
                        "verified_history_stress_usdc", "fees_usdc", "switching_cost_usdc", "net_increment_usdc"):
                economics[key] = _sample(None, None, now)
            account["assignments"].append({
                "assignment_id": f"fixture-{index}-{i}", "assignment_revision": 1, "account_uid": uid,
                "condition_id": market["condition_id"], "status": "proposed" if scenario == "proposed" else "shadow",
                "quote": {"yes_price": "0.94", "no_price": "0.04", "yes_shares": "100", "no_shares": "100", "notional_usdc": "98"},
                "scoring": scoring, "books": books,
                "cancellation": {"status": "unavailable", "protection_triggered_at": None, "cancel_requested_at": None,
                                 "cancel_confirmed_at": None, "confirmation_latency_ms": None,
                                 "freshness": _stamp(None, now, absent="unavailable", reason="runtime_not_deployed")},
                "economics": economics,
            })
        for kind in ACCOUNTING_TYPES:
            account["accounting"].append({"business_day": business_day(at), "account_uid": uid,
                                         "condition_id": universe["markets"][0]["condition_id"], "accounting_type": kind,
                                         "asset_address": "0x" + "c" * 40, "amount_usdc": _sample(None, None, now)})
        state["accounts"].append(account)
    return validate_state(state, now=now)


def synthetic_command(state: dict, *, action: str = "resume_account", now: str = FIXTURE_TIME) -> dict:
    """Example only, deliberately not a builder for runtime command delivery."""
    s = validate_state(state, now=now)
    _require(bool(s["accounts"]), "accounts", "unknown_account")
    a, group = s["accounts"][0], s["group"]
    payload = {"tier_usdc": "150"} if action == "set_budget_tier" else ({"proposal_id": "d" * 64} if action == "apply_assignment" else {})
    command = {"schema_version": SCHEMA_VERSION, "strategy_type": STRATEGY,
               "command_id": "00000000-0000-4000-8000-000000000001", "idempotency_key": "00000000-0000-4000-8000-000000000002",
               "action": action, "created_at": now, "expires_at": _iso(_time(now) + timedelta(minutes=2)),
               "strategy_group": group["strategy_group"], "group_revision": group["revision"],
               "routing_roster_sha256": group["routing_roster_sha256"], "market_universe_sha256": group["market_universe_sha256"],
               "expected_assignment_revision": a["assignment_revision"], "payload": payload,
               **{k: a["identity"][k] for k in ("account_index", "account_uid", "account_uid_key", "host_id")}}
    return validate_command(command, s, now=now)
