"""Synthetic, single-database budget replay. No executor or production adapter.

Public API: BudgetLedger.apply(key, event, now=...), snapshot(maker, now=...),
and assert_current(maker, binding, now=...). All input money is decimal text.
Wire version 2 is independent of the SQLite storage version 1.
"""

from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import re
import sqlite3


SCHEMA_VERSION = 2
STORAGE_VERSION = 1
BUDGET_MODEL = "event_local_reuse_v1"
SOURCE = "synthetic"
ZERO = Decimal(0)
ACTIVE = {"pending", "live", "unknown", "cancel_requested"}
SOURCE_NAMES = {"cash", "allowance", "inventory", "assets"}
FILL_NEXT = {
    "MATCHED": {"MINED", "RETRYING", "CONFIRMED", "FAILED"},
    "MINED": {"RETRYING", "CONFIRMED", "FAILED"},
    "RETRYING": {"MINED", "CONFIRMED", "FAILED"},
    "CONFIRMED": set(), "FAILED": set(),
}


class BudgetError(ValueError):
    """An invalid input or stale binding; no partial transaction is committed."""


class BudgetBusy(BudgetError):
    """The bounded SQLite writer wait expired; no proposal was admitted."""


def require(ok, reason):
    if not ok:
        raise BudgetError(reason)


def decimal(value):
    require(isinstance(value, str) and re.fullmatch(r"(?:0|[1-9][0-9]{0,17})(?:\.[0-9]{1,12})?", value),
            "decimal_text_required")
    return Decimal(value)


def money(value):
    return format(value, "f")


def stamp(value):
    require(isinstance(value, str), "timestamp_required")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BudgetError("invalid_timestamp") from exc
    require(result.tzinfo is not None and result.utcoffset().total_seconds() == 0, "utc_required")
    return result.astimezone(timezone.utc)


def ident(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", value), "invalid_id")
    return value


def canonical_maker(maker):
    require(isinstance(maker, dict) and set(maker) == {"chain_id", "maker_address"}, "maker_fields")
    chain, address = maker["chain_id"], maker["maker_address"]
    require(type(chain) is int and chain > 0, "invalid_chain")
    require(isinstance(address, str) and re.fullmatch(r"0x[0-9a-fA-F]{40}", address), "invalid_maker")
    return f"{chain}:{address.lower()}"


def _json(value):
    # Reject floats even in metadata: callers cannot smuggle imprecise amounts.
    def check(item):
        require(type(item) in (dict, list, str, int, bool, type(None)), "non_json_or_float")
        if isinstance(item, dict):
            require(all(isinstance(key, str) for key in item), "non_string_key")
            for part in item.values():
                check(part)
        elif isinstance(item, list):
            for part in item:
                check(part)
    check(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _fresh(evidence, now):
    if not evidence or evidence.get("trusted") is not True:
        return False
    observed = stamp(evidence["observed_at"])
    return 0 <= (stamp(now) - observed).total_seconds() <= evidence["max_age_sec"]


def _evidence(new, old, now):
    require(isinstance(new, dict) and new.get("source") == SOURCE, "synthetic_evidence_only")
    ident(new["source_id"])
    ident(new["sample_id"])
    require(type(new["watermark"]) is int and new["watermark"] > 0, "invalid_watermark")
    require(type(new["max_age_sec"]) is int and 0 < new["max_age_sec"] <= 300, "invalid_ttl")
    require(type(new["trusted"]) is bool, "invalid_trust")
    require(stamp(new["observed_at"]) <= stamp(now), "future_evidence")
    if old:
        require(new["source_id"] == old["source_id"], "source_rebinding")
        if new == old:
            return
        require(new["watermark"] > old["watermark"] and
                stamp(new["observed_at"]) >= stamp(old["observed_at"]) and
                new["sample_id"] != old["sample_id"], "evidence_regression_or_cached_reread")


def event_capacity(orders, yes_token, no_token, mode="paired"):
    """Pure unit helper; this is NOT minimum-size, cash, or reward admission."""
    require(mode in {"paired", "single"}, "invalid_capacity_mode")
    with localcontext() as ctx:
        ctx.prec = 80
        quantities = {yes_token: ZERO, no_token: ZERO}
        notional = ZERO
        for order in orders:
            require(order["token"] in quantities, "wrong_pair")
            require(order["side"] in {"BUY", "SELL"}, "invalid_side")
            require(order["state"] in ACTIVE | {"cancelled"}, "invalid_order_state")
            if order["side"] == "BUY" and order["state"] in ACTIVE:
                quantity, price = decimal(order["remaining"]), decimal(order["price"])
                quantities[order["token"]] += quantity
                notional += quantity * price
        shares = max(quantities.values())
        return {"yes_shares": money(quantities[yes_token]), "no_shares": money(quantities[no_token]),
                "actual_buy_notional_usdc": money(notional),
                "legacy_paired_capacity_shares": money(shares) if mode == "paired" else None,
                "capacity": money(shares if mode == "paired" else notional),
                "capacity_unit": "legacy_share_capacity" if mode == "paired" else "USDC"}


class BudgetLedger:
    """Only open caller-owned synthetic databases. Each instance owns a connection.

    `fault_hook` is an offline test hook called immediately before/after COMMIT;
    tests use os._exit, not exception rollback, to verify process death recovery.
    """

    def __init__(self, path, *, timeout=0.2, fault_hook=None):
        self.db = sqlite3.connect(str(path), timeout=timeout, isolation_level=None)
        self.fault_hook = fault_hook
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=FULL")
        try:
            self.db.execute("BEGIN IMMEDIATE")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            tables = self.db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            require(version == STORAGE_VERSION or (version == 0 and not tables), "storage_version_mismatch")
            self.db.execute("CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            marker = self.db.execute("SELECT value FROM metadata WHERE key='model'").fetchone()
            require(marker is None if version == 0 else marker == (BUDGET_MODEL,), "storage_model_mismatch")
            self.db.execute("INSERT OR REPLACE INTO metadata VALUES ('model', ?)", (BUDGET_MODEL,))
            self.db.execute("CREATE TABLE IF NOT EXISTS makers (maker TEXT PRIMARY KEY, account_id TEXT UNIQUE NOT NULL, account_index INTEGER UNIQUE NOT NULL, uid TEXT UNIQUE NOT NULL, state TEXT NOT NULL)")
            self.db.execute("CREATE TABLE IF NOT EXISTS events (key TEXT PRIMARY KEY, hash TEXT NOT NULL, input TEXT NOT NULL, result TEXT NOT NULL)")
            self.db.execute("CREATE TABLE IF NOT EXISTS exchange_orders (order_id TEXT PRIMARY KEY, maker TEXT NOT NULL, intent TEXT NOT NULL, UNIQUE(maker,intent))")
            self.db.execute(f"PRAGMA user_version={STORAGE_VERSION}")
            self.db.execute("COMMIT")
        except Exception:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def _load(self, maker):
        row = self.db.execute("SELECT state FROM makers WHERE maker=?", (maker,)).fetchone()
        require(row is not None, "unknown_maker")
        return json.loads(row[0])

    def apply(self, key, event, *, now):
        """Journal one explicit v2 synthetic event, returning receipt + current view.

        Replaying a key returns its historical receipt and a newly evaluated view;
        a previous admission is never represented as a fresh actionable proposal.
        """
        ident(key)
        stamp(now)
        require(isinstance(event, dict) and set(event) ==
                {"schema_version", "budget_model", "source", "maker", "route", "type", "data"}, "event_fields")
        require(type(event["schema_version"]) is int and event["schema_version"] == SCHEMA_VERSION and
                event["budget_model"] == BUDGET_MODEL and event["source"] == SOURCE, "wire_version_or_source")
        encoded, digest = _json(event), _hash(event)
        maker = canonical_maker(event["maker"])
        try:
            self.db.execute("BEGIN IMMEDIATE")
            with localcontext() as ctx:
                ctx.prec = 80
                previous = self.db.execute("SELECT hash,result FROM events WHERE key=?", (key,)).fetchone()
                if previous:
                    require(previous[0] == digest, "idempotency_conflict")
                    state = self._load(maker)
                    view = self._view(state, now)
                    self.db.execute("COMMIT")
                    return {"replayed": True, "receipt": json.loads(previous[1]), "current": view}
                if event["type"] == "register":
                    state = self._register(maker, event["route"], event["data"], now)
                else:
                    state = self._load(maker)
                    require(state["route"] == event["route"], "route_rebinding")
                    require(stamp(now) >= stamp(state["updated_at"]), "account_time_regression")
                outcome = {"accepted": True, "reason": "recorded"}
                if event["type"] != "register":
                    outcome = self._dispatch(state, event["type"], event["data"], now)
                state["version"] += 1
                state["updated_at"] = now
                view = self._view(state, now)
                state["proposals"] = view["conditions"]
                receipt = {"key": key, "input_hash": digest, "account_version": state["version"],
                           "generated_at": now, "source": SOURCE, "proposal_only": True,
                           "live_enabled": False, "mutation_enabled": False, **outcome}
                self.db.execute("UPDATE makers SET state=? WHERE maker=?", (_json(state), maker))
                self.db.execute("INSERT INTO events VALUES (?,?,?,?)", (key, digest, encoded, _json(receipt)))
                if self.fault_hook:
                    self.fault_hook("before_commit")
                self.db.execute("COMMIT")
                if self.fault_hook:
                    self.fault_hook("after_commit")
                return {"replayed": False, "receipt": receipt, "current": view}
        except sqlite3.OperationalError as exc:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                raise BudgetBusy("writer_timeout") from exc
            raise
        except sqlite3.IntegrityError as exc:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise BudgetError("identity_or_order_collision") from exc
        except Exception:
            if self.db.in_transaction:
                self.db.execute("ROLLBACK")
            raise

    def snapshot(self, maker, *, now):
        with localcontext() as ctx:
            ctx.prec = 80
            return self._view(self._load(canonical_maker(maker)), now)

    def assert_current(self, maker, binding, *, now):
        """Check-only: no public API can write externally supplied proposals."""
        view = self.snapshot(maker, now=now)
        condition = view["conditions"].get(binding.get("condition_id"))
        require(condition is not None and binding == condition["binding"], "stale_proposal_binding")
        require(not condition["reasons"], "proposal_requires_revalidation")
        return True

    def _register(self, maker, route, data, now):
        require(set(route) == {"account_id", "account_index", "account_uid", "signature_type", "host_id"}, "route_fields")
        ident(route["account_id"])
        ident(route["host_id"])
        require(type(route["account_index"]) is int and 1 <= route["account_index"] <= 30, "account_index")
        require(type(route["signature_type"]) is int and route["signature_type"] in {0, 1, 2}, "signature_type")
        require(route["account_uid"] == f"{maker}:{route['signature_type']}", "uid_mismatch")
        require(data["tier_usdc"] in {"100", "150", "200"}, "fixed_tier")
        decimal(data["margin_usdc"])
        state = {"maker": maker, "route": deepcopy(route), "tier_usdc": data["tier_usdc"],
                 "margin_usdc": data["margin_usdc"], "version": 0, "updated_at": now,
                 "sources": {}, "conditions": {}, "orders": {}, "fills": {}, "issues": [], "proposals": {},
                 "seen_samples": {}}
        self.db.execute("INSERT INTO makers VALUES (?,?,?,?,?)", (maker, route["account_id"], route["account_index"], route["account_uid"], _json(state)))
        return state

    def _dispatch(self, state, kind, data, now):
        handlers = {"sources": self._sources, "configure": self._configure,
                    "books": self._books, "submit": self._submit, "ack": self._ack,
                    "unknown": self._unknown, "cancel_requested": self._cancel_request,
                    "cancel_confirmed": self._cancel_confirm, "reconcile_order": self._reconcile_order,
                    "fill": self._fill, "reconcile_account": self._reconcile_account}
        require(kind in handlers, "unsupported_event")
        return handlers[kind](state, data, now) or {"accepted": True, "reason": "recorded"}

    def _sources(self, state, data, now):
        require(isinstance(data, dict) and data and set(data) <= SOURCE_NAMES, "source_fields")
        for name, new in data.items():
            old = state["sources"].get(name)
            self._evidence(state, "source:" + name, new, old, now)
            coverage = new["includes_fill_ids"]
            require(isinstance(coverage, list) and len(set(coverage)) == len(coverage) and
                    set(coverage) <= set(state["fills"]), "fill_coverage_unknown")
            if old:
                require(set(old["includes_fill_ids"]) <= set(coverage), "coverage_regression")
            for fill_id in coverage:
                require(stamp(state["fills"][fill_id]["occurred_at"]) <= stamp(new["observed_at"]), "coverage_before_fill")
            if new["value"] is not None:
                if name == "inventory":
                    require(isinstance(new["value"], dict), "inventory_required")
                    for token, position in new["value"].items():
                        ident(token)
                        require(set(position) == {"shares", "cost_usdc"}, "inventory_fields")
                        decimal(position["shares"])
                        decimal(position["cost_usdc"])
                else:
                    decimal(new["value"])
            state["sources"][name] = deepcopy(new)

    def _evidence(self, state, stream, new, old, now):
        _evidence(new, old, now)
        seen = state["seen_samples"].setdefault(stream, {})
        if new["sample_id"] in seen:
            require(new == old and seen[new["sample_id"]] == _hash(new), "sample_id_reused")
        seen[new["sample_id"]] = _hash(new)

    def _configure(self, state, data, now):
        cid = ident(data["condition_id"])
        old = state["conditions"].get(cid)
        yes, no = ident(data["yes_token"]), ident(data["no_token"])
        require(yes != no, "pair_collision")
        require(data["mode"] in {"paired", "single"}, "capacity_mode")
        require(type(data["assignment_revision"]) is int and data["assignment_revision"] > 0, "assignment_revision")
        require(stamp(data["effective_at"]) <= stamp(now), "future_assignment")
        if old:
            require((yes, no) == (old["yes_token"], old["no_token"]), "condition_rebinding")
            require(data["mode"] == old["mode"] or not any(
                o["condition_id"] == cid for o in state["orders"].values()), "capacity_mode_in_use")
            require(data["assignment_revision"] > old["assignment_revision"] and
                    stamp(data["effective_at"]) >= stamp(old["effective_at"]), "assignment_regression")
            require(stamp(data["effective_at"]) == stamp(now), "backdated_assignment_change")
        for other_id, other in state["conditions"].items():
            if other_id != cid:
                require(not {yes, no} & {other["yes_token"], other["no_token"]}, "token_condition_collision")
        require(ZERO < decimal(data["tick"]) < 1 and decimal(data["minimum_shares"]) > 0, "tick_or_minimum")
        decimal(data["min_front_depth_usdc"])
        require(type(data["min_seconds_to_end"]) is int and data["min_seconds_to_end"] >= 0, "expiry_guard")
        require(data["category"] in {"standard", "weather", "up_down"}, "category")
        stamp(data["end_at"])
        require(type(data["mock_eligible"]) is bool, "eligibility_required")
        previous_books = {**old.get("previous_books", {}), **old["books"]} if old else {}
        state["conditions"][cid] = {**deepcopy(data), "books": {}, "previous_books": previous_books}

    def _books(self, state, data, now):
        condition = state["conditions"][data["condition_id"]]
        require(type(data["assignment_revision"]) is int and data["assignment_revision"] == condition["assignment_revision"], "stale_assignment")
        require(data["samples"] and set(data["samples"]) <= {condition["yes_token"], condition["no_token"]}, "book_token")
        for token, sample in data["samples"].items():
            old = condition["books"].get(token, condition["previous_books"].get(token))
            self._evidence(state, "book:" + token, sample, old, now)
            if token not in condition["books"] and old:
                require(sample["watermark"] > old["watermark"], "book_reused_after_assignment")
            require(stamp(sample["observed_at"]) >= stamp(condition["effective_at"]), "book_before_assignment")
            require(type(sample["complete"]) is bool and (sample["scoring"] is None or type(sample["scoring"]) is bool), "book_evidence_fields")
            for field in ("best_ask", "reward_low", "reward_high", "front_depth_usdc"):
                if sample[field] is not None:
                    decimal(sample[field])
            if sample["fee_rate"] is not None:
                require(decimal(sample["fee_rate"]) <= 1, "fee_rate")
            if sample["best_ask"] is not None:
                require(ZERO < decimal(sample["best_ask"]) <= 1, "invalid_best_ask")
            if sample["reward_low"] is not None and sample["reward_high"] is not None:
                require(decimal(sample["reward_low"]) <= decimal(sample["reward_high"]) <= 1, "invalid_reward_zone")
            condition["books"][token] = deepcopy(sample)

    def _remaining(self, state, intent):
        order = state["orders"][intent]
        filled = sum((decimal(f["quantity"]) for f in state["fills"].values() if f["intent_id"] == intent), ZERO)
        return max(ZERO, decimal(order["quantity"]) - filled)

    def _order_view(self, state, intent):
        order = state["orders"][intent]
        return {**order, "remaining": money(self._remaining(state, intent))}

    def _submit(self, state, data, now):
        condition = state["conditions"][data["condition_id"]]
        require(type(data["assignment_revision"]) is int and data["assignment_revision"] == condition["assignment_revision"], "stale_assignment")
        require(type(data["account_version"]) is int and data["account_version"] == state["version"], "stale_account_version")
        require(isinstance(data["orders"], list) and data["orders"], "orders_required")
        staged = deepcopy(state)
        sides = set()
        for new in data["orders"]:
            require(set(new) == {"intent_id", "token", "side", "quantity", "price"}, "order_fields")
            intent = ident(new["intent_id"])
            require(intent not in staged["orders"], "intent_exists")
            require(new["token"] in {condition["yes_token"], condition["no_token"]}, "wrong_token")
            require(new["side"] in {"BUY", "SELL"}, "invalid_side")
            require(decimal(new["quantity"]) > 0 and ZERO < decimal(new["price"]) < 1, "invalid_order_amount")
            require(decimal(new["price"]) % decimal(condition["tick"]) == 0, "off_tick")
            sides.add(new["side"])
            staged["orders"][intent] = {**deepcopy(new), "condition_id": data["condition_id"],
                                        "state": "pending", "exchange_order_id": None, "created_at": now}
        require(len(sides) == 1, "submit_sell_separately_exit_priority")
        view = self._view(staged, now)
        if sides == {"BUY"}:
            reasons = view["conditions"][data["condition_id"]]["reasons"]
        else:
            reasons = self._sell_reasons(staged, now)
        if reasons:
            return {"accepted": False, "reason": "budget_or_evidence_block", "reasons": reasons}
        state["orders"] = staged["orders"]
        return {"accepted": True, "reason": "synthetic_intents_recorded"}

    def _bind_order(self, state, intent, exchange_id):
        ident(exchange_id)
        order = state["orders"][intent]
        require(order["exchange_order_id"] in {None, exchange_id}, "exchange_order_rebinding")
        row = self.db.execute("SELECT maker,intent FROM exchange_orders WHERE order_id=?", (exchange_id,)).fetchone()
        if row:
            require(row == (state["maker"], intent), "exchange_order_collision")
        else:
            self.db.execute("INSERT INTO exchange_orders VALUES (?,?,?)", (exchange_id, state["maker"], intent))
        order["exchange_order_id"] = exchange_id
        return order

    def _ack(self, state, data, now):
        order = self._bind_order(state, data["intent_id"], data["exchange_order_id"])
        require(order["state"] in {"pending", "live"}, "ack_requires_reconciliation")
        order["state"] = "live"

    def _unknown(self, state, data, now):
        order = state["orders"][data["intent_id"]]
        require(order["state"] in ACTIVE, "unknown_state")
        if order["state"] != "unknown":
            self._require_new_order_evidence(order, now)
        order["state"] = "unknown"

    def _require_new_order_evidence(self, order, now):
        # A fresh-by-TTL proof from an earlier attempt cannot resolve new risk.
        order["reconcile_after"] = now
        order["proof_watermark_before_transition"] = order.get("proof", {}).get("watermark", 0)

    def _cancel_request(self, state, data, now):
        order = state["orders"][data["intent_id"]]
        require(order["state"] in {"live", "pending", "cancel_requested"}, "cancel_requires_reconciliation")
        if order["state"] != "cancel_requested":
            order["cancel_requested_at"] = now
            self._require_new_order_evidence(order, now)
        order["state"] = "cancel_requested"

    def _order_proof(self, state, data, now, *, resolved_state):
        proof = data["proof"]
        previous_order = state["orders"][data["intent_id"]]
        previous_proof = previous_order.get("proof")
        self._evidence(state, "order:" + data["intent_id"], proof, previous_proof, now)
        require(_fresh(proof, now) and proof.get("exhaustive") is True, "order_reconciliation_required")
        order = self._bind_order(state, data["intent_id"], data["exchange_order_id"])
        fills = {key for key, fill in state["fills"].items() if fill["intent_id"] == data["intent_id"]}
        require(set(proof["includes_fill_ids"]) == fills and len(proof["includes_fill_ids"]) == len(fills), "order_fill_coverage")
        require(decimal(proof["remaining"]) == self._remaining(state, data["intent_id"]), "order_remaining_conflict")
        require(stamp(proof["observed_at"]) >= stamp(order["created_at"]), "order_proof_before_intent")
        require(stamp(proof["observed_at"]) >= stamp(order.get("reconcile_after", order["created_at"])),
                "order_proof_before_transition")
        require(proof["watermark"] > order.get("proof_watermark_before_transition", 0),
                "order_proof_reused_after_transition")
        if proof == previous_proof:
            require(previous_order.get("proof_resolved_state") == resolved_state, "order_proof_outcome_conflict")
        for fill_id in fills:
            require(stamp(state["fills"][fill_id]["occurred_at"]) <= stamp(proof["observed_at"]), "order_proof_before_fill")
        order["proof"] = deepcopy(proof)
        order["proof_resolved_state"] = resolved_state
        return order

    def _cancel_confirm(self, state, data, now):
        require(state["orders"][data["intent_id"]]["state"] in {"cancel_requested", "cancelled"}, "cancel_not_requested")
        order = self._order_proof(state, data, now, resolved_state="cancelled")
        require(stamp(order["proof"]["observed_at"]) >= stamp(order["cancel_requested_at"]), "cancel_proof_before_request")
        order["state"] = "cancelled"

    def _reconcile_order(self, state, data, now):
        require(data["resolved_state"] in {"live", "cancelled"}, "reconcile_state")
        order = self._order_proof(state, data, now, resolved_state=data["resolved_state"])
        require(order["state"] != "cancelled" or data["resolved_state"] == "cancelled", "cancelled_resurrection")
        order["state"] = data["resolved_state"]

    def _fill(self, state, data, now):
        require(set(data) == {"trade_id", "component_id", "intent_id", "exchange_order_id", "quantity", "price",
                              "fee_usdc", "inventory_cost_usdc", "occurred_at", "status"}, "fill_fields")
        ident(data["trade_id"])
        ident(data["component_id"])
        require(data["status"] in FILL_NEXT, "fill_status")
        require(decimal(data["quantity"]) > 0 and ZERO < decimal(data["price"]) < 1, "fill_amount")
        if data["fee_usdc"] is not None:
            decimal(data["fee_usdc"])
        require(stamp(data["occurred_at"]) <= stamp(now), "future_fill")
        order = self._bind_order(state, data["intent_id"], data["exchange_order_id"])
        require(stamp(data["occurred_at"]) >= stamp(order["created_at"]), "fill_before_intent")
        if order["side"] == "SELL":
            decimal(data["inventory_cost_usdc"])
        else:
            require(data["inventory_cost_usdc"] is None, "buy_cost_is_notional")
        fill_id = _hash([state["maker"], data["trade_id"], data["exchange_order_id"], data["component_id"]])
        previous = state["fills"].get(fill_id)
        if previous:
            require({k: v for k, v in previous.items() if k != "status"} ==
                    {k: v for k, v in data.items() if k != "status"}, "fill_payload_conflict")
            require(previous["status"] == data["status"] or data["status"] in FILL_NEXT[previous["status"]], "fill_status_regression")
        elif order["state"] == "cancelled":
            state["issues"].append(f"late_fill:{fill_id}")
        state["fills"][fill_id] = deepcopy(data)
        total = sum((decimal(f["quantity"]) for f in state["fills"].values() if f["intent_id"] == data["intent_id"]), ZERO)
        if total > decimal(order["quantity"]):
            state["issues"].append(f"overfill:{data['intent_id']}")
        if (order["side"] == "BUY" and decimal(data["price"]) > decimal(order["price"])) or (
                order["side"] == "SELL" and decimal(data["price"]) < decimal(order["price"])):
            state["issues"].append(f"limit_violation:{fill_id}")
        return {"accepted": True, "reason": "fill_recorded", "fill_id": fill_id}

    def _reconcile_account(self, state, data, now):
        # Only late-report flags are clearable here; failed/contradictory fills
        # need a future explicit correction contract, never a silent refund.
        require(set(data["resolved_issues"]) == set(state["issues"]), "incomplete_reconciliation")
        require(all(issue.startswith("late_fill:") for issue in state["issues"]), "correction_not_implemented")
        require(all(_fresh(state["sources"].get(n), now) and
                    set(state["sources"][n]["includes_fill_ids"]) == set(state["fills"])
                    for n in SOURCE_NAMES), "authoritative_coverage_required")
        require(not any(o["state"] == "unknown" for o in state["orders"].values()), "unknown_submission")
        state["issues"] = []

    def _finance(self, state, now):
        reasons, values = list(set(state["issues"])), {}
        for name in SOURCE_NAMES:
            source = state["sources"].get(name)
            if not _fresh(source, now) or source["value"] is None:
                reasons.append(f"{name}_unknown_or_stale")
                values[name] = None
                continue
            values[name] = deepcopy(source["value"]) if name == "inventory" else decimal(source["value"])
        for fill_id, fill in state["fills"].items():
            order = state["orders"][fill["intent_id"]]
            side, token = order["side"], order["token"]
            qty, cost = decimal(fill["quantity"]), decimal(fill["quantity"]) * decimal(fill["price"])
            fee = decimal(fill["fee_usdc"]) if fill["fee_usdc"] is not None else None
            if fee is None or fill["status"] == "FAILED":
                reasons.append("fill_fee_unknown" if fee is None else "failed_fill_reconcile_required")
            for name in ("cash", "allowance"):
                if values[name] is not None and fill_id not in state["sources"][name]["includes_fill_ids"]:
                    if side == "BUY":
                        values[name] = values[name] - cost - fee if fee is not None else None
                    # Unreflected SELL proceeds are never assumed spendable.
            if values["inventory"] is not None and fill_id not in state["sources"]["inventory"]["includes_fill_ids"]:
                position = values["inventory"].setdefault(token, {"shares": "0", "cost_usdc": "0"})
                position["shares"] = money(Decimal(position["shares"]) + (qty if side == "BUY" else -qty))
                position["cost_usdc"] = money(Decimal(position["cost_usdc"]) + (cost if side == "BUY" else -decimal(fill["inventory_cost_usdc"])))
            if values["assets"] is not None and fill_id not in state["sources"]["assets"]["includes_fill_ids"]:
                values["assets"] = values["assets"] - fee if fee is not None else None
                if side == "SELL" and values["assets"] is not None:
                    values["assets"] += min(ZERO, cost - decimal(fill["inventory_cost_usdc"]))
        if any(o["state"] == "unknown" for o in state["orders"].values()):
            reasons.append("unknown_submission_reconcile_required")
        inventory_cost = None
        if values["inventory"] is not None:
            inventory_cost = sum((Decimal(p["cost_usdc"]) for p in values["inventory"].values()), ZERO)
            if any(Decimal(p["shares"]) < 0 or Decimal(p["cost_usdc"]) < 0 for p in values["inventory"].values()):
                reasons.append("inventory_deficit")
        for name in ("cash", "allowance", "assets"):
            if values[name] is not None and values[name] < 0:
                reasons.append(f"{name}_deficit")
        effective = min(decimal(state["tier_usdc"]), max(ZERO, values["assets"])) if values["assets"] is not None else None
        headroom = max(ZERO, effective - inventory_cost) if effective is not None and inventory_cost is not None else None
        ceiling = None
        if not reasons:
            ceiling = max(ZERO, min(values["cash"], values["allowance"], headroom) - decimal(state["margin_usdc"]))
        return {"reasons": sorted(set(reasons)), "values": values, "inventory_cost": inventory_cost,
                "effective": effective, "headroom": headroom, "ceiling": ceiling}

    def _sell_reasons(self, state, now):
        source = state["sources"].get("inventory")
        if not _fresh(source, now) or source["value"] is None:
            return ["inventory_unknown_or_stale"]
        # Cash uncertainty blocks BUY, not exits from independently proven stock.
        # Never offer unreflected BUY fills as settled, spendable SELL inventory.
        held = {token: decimal(p["shares"]) for token, p in source["value"].items()}
        for fill_id, fill in state["fills"].items():
            order = state["orders"][fill["intent_id"]]
            if order["side"] == "SELL" and fill_id not in source["includes_fill_ids"]:
                held[order["token"]] = held.get(order["token"], ZERO) - decimal(fill["quantity"])
        reasons = ["inventory_deficit"] if any(v < 0 for v in held.values()) else []
        reserved = {}
        for intent, order in state["orders"].items():
            if order["side"] == "SELL" and order["state"] in ACTIVE:
                reserved[order["token"]] = reserved.get(order["token"], ZERO) + self._remaining(state, intent)
        for token, shares in reserved.items():
            if shares > held.get(token, ZERO):
                reasons.append("sell_shares_unavailable")
        return sorted(set(reasons))

    def _view(self, state, now):
        require(stamp(now) >= stamp(state["updated_at"]), "view_before_state")
        finance = self._finance(state, now)
        conditions, total = {}, ZERO
        watermarks = {name: {k: s[k] for k in ("source_id", "sample_id", "watermark", "observed_at")}
                      for name, s in state["sources"].items()}
        for cid, condition in state["conditions"].items():
            orders = [self._order_view(state, i) for i, o in state["orders"].items() if o["condition_id"] == cid]
            cap = event_capacity(orders, condition["yes_token"], condition["no_token"], condition["mode"])
            # Computed products/sums can exceed the wire input's decimal scale.
            notional = Decimal(cap["actual_buy_notional_usdc"])
            total += notional
            reasons = list(finance["reasons"])
            if condition["category"] != "standard" or not condition["mock_eligible"]:
                reasons.append("market_policy_block")
            if (stamp(condition["end_at"]) - stamp(now)).total_seconds() <= condition["min_seconds_to_end"]:
                reasons.append("near_or_after_end")
            fee_reserve = ZERO
            fee_known = True
            scoring = []
            for token in (condition["yes_token"], condition["no_token"]):
                book = condition["books"].get(token)
                if not _fresh(book, now) or not book["complete"]:
                    reasons.append(f"book_unknown_or_stale:{token}")
                    fee_known = False
                    continue
                scoring.append(book["scoring"])
                if any(book[f] is None for f in ("best_ask", "reward_low", "reward_high", "front_depth_usdc", "fee_rate")):
                    reasons.append(f"book_or_fee_unknown:{token}")
                    fee_known = False
                    continue
                if decimal(book["front_depth_usdc"]) < decimal(condition["min_front_depth_usdc"]):
                    reasons.append(f"front_depth:{token}")
                for order in orders:
                    if order["token"] != token or order["side"] != "BUY" or order["state"] not in ACTIVE or decimal(order["remaining"]) == 0:
                        continue
                    price, qty = decimal(order["price"]), decimal(order["remaining"])
                    fee_reserve += price * qty * decimal(book["fee_rate"])
                    if qty < decimal(condition["minimum_shares"]):
                        reasons.append("below_minimum_shares")
                    if price % decimal(condition["tick"]) != 0:
                        reasons.append("off_tick")
                    if not decimal(book["reward_low"]) <= price <= decimal(book["reward_high"]):
                        reasons.append("outside_mock_reward_zone")
                    if price >= decimal(book["best_ask"]):
                        reasons.append("would_cross_book")
            ceiling = finance["ceiling"]
            if ceiling is not None:
                if notional + fee_reserve > ceiling:
                    reasons.append("event_cash_notional")
                if condition["mode"] == "paired" and Decimal(cap["capacity"]) > ceiling:
                    reasons.append("legacy_paired_capacity")
            if condition["mode"] == "paired" and notional:
                # Cancelling/unknown legs reserve risk, but cannot qualify a pair.
                qualified = event_capacity([o for o in orders if o["state"] in {"pending", "live"}],
                                           condition["yes_token"], condition["no_token"])
                if not Decimal(qualified["yes_shares"]) or not Decimal(qualified["no_shares"]):
                    reasons.append("paired_leg_missing")
                elif min(Decimal(qualified["yes_shares"]), Decimal(qualified["no_shares"])) < decimal(condition["minimum_shares"]):
                    reasons.append("paired_leg_below_minimum")
            if any(o["side"] == "SELL" and o["state"] in ACTIVE and decimal(o["remaining"]) > 0 for o in orders):
                reasons.append("exit_sell_priority")
            score = "mock_pass" if len(scoring) == 2 and all(v is True for v in scoring) else "failed" if False in scoring else "unknown"
            if score != "mock_pass":
                reasons.append("mock_scoring_" + score)
            # Reductions are requests to re-plan, never fictitious executed cancels.
            action = "cancel_buy_proposal" if reasons and notional else "blocked" if reasons else "revalidate_proposal"
            binding = {"maker": state["maker"], "condition_id": cid, "account_version": state["version"],
                       "assignment_revision": condition["assignment_revision"], "sources": deepcopy(watermarks),
                       "books_hash": _hash(condition["books"])}
            conditions[cid] = {**cap, "fee_reserve_usdc": money(fee_reserve) if fee_known else None, "reasons": sorted(set(reasons)),
                               "action": action, "scoring": score, "reward_eligible": None,
                               "binding": binding, "exit_sell_priority": True}
        def optional(value):
            return money(value) if value is not None else None
        return {"schema_version": SCHEMA_VERSION, "storage_version": STORAGE_VERSION, "budget_model": BUDGET_MODEL,
                "source": SOURCE, "mode": "offline", "live_enabled": False, "mutation_enabled": False,
                "proposal_only": True, "maker": state["maker"], "route": deepcopy(state["route"]),
                "account_version": state["version"], "generated_at": now,
                "tier_usdc": state["tier_usdc"], "effective_capital_usdc": optional(finance["effective"]),
                "adjusted_cash_usdc": optional(finance["values"]["cash"]),
                "adjusted_allowance_usdc": optional(finance["values"]["allowance"]),
                "inventory": finance["values"]["inventory"], "inventory_cost_usdc": optional(finance["inventory_cost"]),
                "capital_headroom_usdc": optional(finance["headroom"]), "margin_usdc": state["margin_usdc"],
                "event_quote_ceiling_usdc": optional(finance["ceiling"]),
                "displayed_cross_condition_notional_usdc": money(total),
                "reconcile_reasons": finance["reasons"], "conditions": conditions,
                "orders": {i: self._order_view(state, i) for i in state["orders"]}, "fills": deepcopy(state["fills"])}
