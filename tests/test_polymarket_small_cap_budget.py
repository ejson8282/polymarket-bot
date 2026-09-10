"""Synthetic offline acceptance; all databases live in TemporaryDirectory."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

from platforms.polymarket.maker.small_cap_budget import (
    BUDGET_MODEL, BudgetBusy, BudgetError, BudgetLedger, canonical_maker,
    decimal, event_capacity,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = "2026-09-09T02:00:00Z"


def later(seconds):
    return (datetime(2026, 9, 9, 2, tzinfo=timezone.utc) + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")


def identity(index=1):
    maker = {"chain_id": 137, "maker_address": "0x" + str(index) * 40}
    route = {"account_id": f"synthetic-{index}", "account_index": index,
             "account_uid": f"{canonical_maker(maker)}:2", "signature_type": 2, "host_id": f"synthetic-host-{index}"}
    return maker, route


def envelope(kind, data, index=1):
    maker, route = identity(index)
    return {"schema_version": 2, "budget_model": BUDGET_MODEL, "source": "synthetic",
            "maker": maker, "route": route, "type": kind, "data": data}


def evidence(source_id, value=None, watermark=1, when=NOW, coverage=None):
    return {"source": "synthetic", "trusted": True, "source_id": source_id,
            "sample_id": f"{source_id}-{watermark}", "watermark": watermark,
            "observed_at": when, "max_age_sec": 300,
            "includes_fill_ids": list(coverage or []), "value": value}


def sources(cash="100", allowance="100", assets="100", inventory=None, watermark=1, when=NOW, coverage=None):
    return {name: evidence(name, value, watermark, when, coverage)
            for name, value in {"cash": cash, "allowance": allowance, "assets": assets,
                                "inventory": inventory if inventory is not None else {}}.items()}


def configuration(cid="A", revision=1, when=NOW, minimum="1", mode="single"):
    return {"condition_id": cid, "yes_token": cid + "-YES", "no_token": cid + "-NO",
            "assignment_revision": revision, "effective_at": when, "tick": "0.01",
            "minimum_shares": minimum, "mode": mode, "min_front_depth_usdc": "10",
            "end_at": later(86400), "min_seconds_to_end": 3600,
            "category": "standard", "mock_eligible": True}


def books(cid="A", revision=1, watermark=None, when=NOW):
    watermark = revision if watermark is None else watermark
    result = {}
    for side in ("YES", "NO"):
        token = cid + "-" + side
        result[token] = {**evidence(token + "-book", watermark=watermark, when=when),
                         "complete": True, "best_ask": "0.99", "reward_low": "0.01",
                         "reward_high": "0.98", "front_depth_usdc": "10000", "fee_rate": "0",
                         "scoring": True, "event_at": later(-10000)}
    return {"condition_id": cid, "assignment_revision": revision, "samples": result}


def order(intent="o1", token="A-YES", qty="70", price="0.70", side="BUY"):
    return {"intent_id": intent, "token": token, "quantity": qty, "price": price, "side": side}


class CapacityTests(unittest.TestCase):
    def capacity(self, legs, mode="paired"):
        return event_capacity([{**o, "state": "pending", "remaining": o["quantity"]} for o in legs], "A-YES", "A-NO", mode)

    def test_paired_capacity_units_not_cash(self):
        cases = [([order(), order("n", "A-NO")], "70"),
                 ([order(), order("y2")], "140"),
                 ([order(), order("n", "A-NO"), order("y2"), order("n2", "A-NO")], "140")]
        for legs, expected in cases:
            with self.subTest(expected=expected, legs=len(legs)):
                self.assertEqual(Decimal(self.capacity(legs)["capacity"]), Decimal(expected))

    def test_actual_pair_98_and_capacity_100(self):
        cap = self.capacity([order(qty="100", price="0.94"), order("n", "A-NO", "100", "0.04")])
        self.assertEqual(Decimal(cap["actual_buy_notional_usdc"]), 98)
        self.assertEqual(Decimal(cap["capacity"]), 100)

    def test_single_side_cash_unit_and_sell_excluded(self):
        cap = self.capacity([order(qty="100"), order("s", side="SELL")], "single")
        self.assertEqual(Decimal(cap["capacity"]), 70)
        self.assertEqual(cap["capacity_unit"], "USDC")
        self.assertIsNone(cap["legacy_paired_capacity_shares"])

    def test_cancel_requested_and_unknown_keep_capacity(self):
        for status in ("pending", "live", "unknown", "cancel_requested", "cancelled"):
            with self.subTest(status=status):
                o = {**order(), "state": status, "remaining": "70"}
                cap = event_capacity([o], "A-YES", "A-NO")
                self.assertEqual(Decimal(cap["capacity"]), 0 if status == "cancelled" else 70)

    def test_decimal_rejects_float_nonfinite_and_negative(self):
        for value in (1.1, 1, True, "NaN", "Infinity", "-1", "1e2", "0.0000000000001"):
            with self.subTest(value=value), self.assertRaises(BudgetError):
                decimal(value)

    def test_decimal_context_not_inherited(self):
        with localcontext() as ctx:
            ctx.prec = 3
            cap = self.capacity([order(qty="123.456789", price="0.123456")], "single")
        self.assertEqual(cap["capacity"], "15.241481342784")


class LedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "synthetic.sqlite3"
        self.ledger = BudgetLedger(self.path)
        self.counter = 0
        self.initialize()

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()

    def apply(self, kind, data, *, key=None, now=NOW, index=1):
        self.counter += 1
        return self.ledger.apply(key or f"event-{self.counter}", envelope(kind, data, index), now=now)

    def initialize(self, index=1, tier="100", margin="0", capital="100"):
        self.apply("register", {"tier_usdc": tier, "margin_usdc": margin}, index=index)
        self.apply("sources", sources(capital, capital, capital), index=index)
        for cid in ("A", "B"):
            self.apply("configure", configuration(cid), index=index)
            self.apply("books", books(cid), index=index)

    def view(self, now=NOW, index=1):
        return self.ledger.snapshot(identity(index)[0], now=now)

    def submit(self, legs=None, cid="A", *, key=None, index=1, now=NOW, revision=1):
        return self.apply("submit", {"condition_id": cid, "assignment_revision": revision,
                                     "account_version": self.view(now, index)["account_version"],
                                     "orders": legs or [order()]}, key=key, index=index, now=now)

    def ack(self, intent="o1", exchange_id="0x" + "a" * 64, now=NOW):
        return self.apply("ack", {"intent_id": intent, "exchange_order_id": exchange_id}, now=now)

    def fill_data(self, qty="20", component="part1", status="MATCHED", intent="o1", price="0.70", when=NOW, fee="0"):
        return {"trade_id": "trade1", "component_id": component, "intent_id": intent,
                "exchange_order_id": "0x" + "a" * 64, "quantity": qty, "price": price,
                "fee_usdc": fee, "inventory_cost_usdc": None, "occurred_at": when, "status": status}

    def proof_data(self, remaining, intent="o1", coverage=None, watermark=1, when=NOW):
        proof = evidence("order-proof-" + intent, watermark=watermark, when=when, coverage=coverage)
        proof.update(exhaustive=True, remaining=remaining)
        return {"intent_id": intent, "exchange_order_id": "0x" + "a" * 64, "proof": proof}

    def assert_amount(self, value, expected):
        self.assertIsNotNone(value)
        self.assertEqual(Decimal(value), Decimal(str(expected)))

    def paired(self, *, index=1, minimum="1"):
        self.apply("configure", configuration(revision=2, mode="paired", minimum=minimum), index=index)
        self.apply("books", books(revision=2), index=index)

    def test_every_output_explicit_offline_v2(self):
        view = self.view()
        self.assertEqual((view["schema_version"], view["storage_version"], view["budget_model"]), (2, 1, BUDGET_MODEL))
        self.assertEqual(view["source"], "synthetic")
        self.assertFalse(view["live_enabled"] or view["mutation_enabled"])
        self.assertTrue(view["proposal_only"])
        self.assertTrue(all(c["reward_eligible"] is None for c in view["conditions"].values()))

    def test_independent_conditions_reuse_cash_without_aggregate_ceiling(self):
        self.assertTrue(self.submit([order(qty="100")])["receipt"]["accepted"])
        self.assertTrue(self.submit([order("b", "B-YES", "100")], "B")["receipt"]["accepted"])
        view = self.view()
        self.assert_amount(view["displayed_cross_condition_notional_usdc"], 140)
        self.assert_amount(view["event_quote_ceiling_usdc"], 100)
        self.assertEqual(view["reconcile_reasons"], [])

    def test_same_condition_same_side_capacity_blocks_second(self):
        self.assertTrue(self.submit([order(qty="100")])["receipt"]["accepted"])
        result = self.submit([order("o2", qty="100")])
        self.assertFalse(result["receipt"]["accepted"])
        self.assertIn("event_cash_notional", result["receipt"]["reasons"])
        self.assertNotIn("o2", self.view()["orders"])

    def test_complementary70shares_allowed_not_two70usdc(self):
        self.paired()
        result = self.submit([order(price="0.50"), order("n", "A-NO", price="0.40")], revision=2)
        self.assertTrue(result["receipt"]["accepted"])
        self.assert_amount(result["current"]["conditions"]["A"]["capacity"], 70)
        self.assert_amount(result["current"]["displayed_cross_condition_notional_usdc"], 63)

    def test_two_complete_pairs_same_condition_rejected(self):
        self.paired()
        pair = [order(price="0.50"), order("n", "A-NO", price="0.40")]
        self.assertTrue(self.submit(pair, revision=2)["receipt"]["accepted"])
        self.assertFalse(self.submit([order("y2", price="0.50"), order("n2", "A-NO", price="0.40")], revision=2)["receipt"]["accepted"])

    def test_absolute_margin_separate_from_98_notional_and_100_capacity(self):
        self.initialize(index=2, margin="1")
        self.paired(index=2)
        result = self.submit([order(qty="100", price="0.94"), order("n", "A-NO", "100", "0.04")], index=2, revision=2)
        self.assertFalse(result["receipt"]["accepted"])
        self.assertEqual(result["receipt"]["reasons"], ["legacy_paired_capacity"])
        self.assert_amount(result["current"]["event_quote_ceiling_usdc"], 99)

    def test_100_150_200_minimum_pair_counts(self):
        for idx, tier, count in ((1, "100", 1), (2, "150", 1), (3, "200", 2)):
            with self.subTest(tier=tier):
                if idx != 1:
                    self.initialize(idx, tier=tier, capital=tier)
                self.paired(index=idx)
                pair = [order(qty="100", price="0.94"), order("n", "A-NO", "100", "0.04")]
                self.assertTrue(self.submit(pair, index=idx, revision=2)["receipt"]["accepted"])
                pair2 = [order("y2", qty="100", price="0.94"), order("n2", "A-NO", "100", "0.04")]
                self.assertEqual(self.submit(pair2, index=idx, revision=2)["receipt"]["accepted"], count == 2)
                self.assert_amount(self.view(index=idx)["conditions"]["A"]["actual_buy_notional_usdc"], count * 98)

    def test_fixed_tier_does_not_upgrade_from_profit(self):
        for watermark, assets, expected in ((2, "100", 100), (3, "120", 100), (4, "95", 95), (5, "105", 100)):
            self.apply("sources", sources(assets, assets, assets, watermark=watermark))
            self.assert_amount(self.view()["effective_capital_usdc"], expected)

    def test_cash_old100_fill70_then_cash30_cover_remains30(self):
        self.submit([order(qty="100")])
        self.ack()
        fill_id = self.apply("fill", self.fill_data("100"))["receipt"]["fill_id"]
        view = self.view()
        self.assert_amount(view["adjusted_cash_usdc"], 30)
        self.assert_amount(view["inventory_cost_usdc"], 70)
        self.assert_amount(view["event_quote_ceiling_usdc"], 30)
        self.apply("sources", {"cash": evidence("cash", "30", 2, coverage=[fill_id])})
        self.assert_amount(self.view()["adjusted_cash_usdc"], 30)

    def test_allowance_coverage_adjusted_before_min(self):
        self.submit([order(qty="100")])
        fill_id = self.apply("fill", self.fill_data("100"))["receipt"]["fill_id"]
        self.apply("sources", {"allowance": evidence("allowance", "30", 2, coverage=[fill_id])})
        view = self.view()
        self.assert_amount(view["adjusted_cash_usdc"], 30)
        self.assert_amount(view["adjusted_allowance_usdc"], 30)
        self.assert_amount(view["event_quote_ceiling_usdc"], 30)

    def test_new_time_does_not_prove_fill_covered(self):
        self.submit([order(qty="100")])
        self.apply("fill", self.fill_data("100"))
        self.apply("sources", {"cash": evidence("cash", "100", 2, later(1))}, now=later(1))
        self.assert_amount(self.view(later(1))["adjusted_cash_usdc"], 30)

    def test_inventory_and_assets_have_independent_coverage(self):
        self.submit([order(qty="100")])
        fill_id = self.apply("fill", self.fill_data("100", fee="1"))["receipt"]["fill_id"]
        self.apply("sources", sources("29", "29", "99", {"A-YES": {"shares": "100", "cost_usdc": "70"}},
                                      watermark=2, coverage=[fill_id]))
        view = self.view()
        self.assert_amount(view["inventory_cost_usdc"], 70)
        self.assert_amount(view["effective_capital_usdc"], 99)
        self.assert_amount(view["event_quote_ceiling_usdc"], 29)

    def test_missing_sources_nulls_not_zero(self):
        self.apply("sources", {"cash": evidence("cash", None, 2)})
        view = self.view()
        self.assertIsNone(view["event_quote_ceiling_usdc"])
        self.assertIsNone(view["adjusted_cash_usdc"])
        self.assertFalse(self.submit()["receipt"]["accepted"])

    def test_unknown_fee_persistent_block(self):
        self.submit()
        self.apply("fill", self.fill_data(fee=None))
        self.assertIn("fill_fee_unknown", self.view()["reconcile_reasons"])
        self.assertFalse(self.submit([order("b", "B-YES")], "B")["receipt"]["accepted"])

    def test_unknown_submission_blocks_maker_not_other_maker_and_survives_reopen(self):
        self.submit()
        self.apply("unknown", {"intent_id": "o1"})
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assertFalse(self.submit([order("b", "B-YES")], "B")["receipt"]["accepted"])
        self.initialize(2)
        self.assertTrue(self.submit(index=2)["receipt"]["accepted"])
        with self.assertRaises(BudgetError):
            self.ack()

    def test_unknown_recovery_requires_explicit_exhaustive_order_proof(self):
        self.submit()
        self.apply("unknown", {"intent_id": "o1"})
        proof = self.proof_data("70")
        proof["resolved_state"] = "live"
        bad = deepcopy(proof)
        bad["proof"]["exhaustive"] = False
        with self.assertRaises(BudgetError):
            self.apply("reconcile_order", bad)
        self.apply("reconcile_order", proof)
        self.assertTrue(self.submit([order("b", "B-YES")], "B")["receipt"]["accepted"])

    def test_pending_to_live_single_identity_no_extra_reserve(self):
        self.submit()
        before = self.view()["conditions"]["A"]["capacity"]
        self.ack()
        self.ack()
        self.assertEqual(self.view()["conditions"]["A"]["capacity"], before)
        self.assertEqual(len(self.view()["orders"]), 1)

    def test_order_exchange_hash_supported_unique_and_not_rebindable(self):
        self.submit([order(qty="20"), order("o2", qty="20")])
        self.ack()
        with self.assertRaises(BudgetError):
            self.ack("o2")
        with self.assertRaises(BudgetError):
            self.ack(exchange_id="0x" + "b" * 64)

    def test_partial_fill_cancel_requested_then_confirm_release_only_unfilled(self):
        self.submit([order(qty="100")])
        self.ack()
        first = self.apply("fill", self.fill_data())["receipt"]["fill_id"]
        self.assert_amount(self.view()["inventory_cost_usdc"], 14)
        self.assert_amount(self.view()["conditions"]["A"]["actual_buy_notional_usdc"], 56)
        self.apply("cancel_requested", {"intent_id": "o1"})
        second = self.apply("fill", self.fill_data("10", "part2"))["receipt"]["fill_id"]
        self.assert_amount(self.view()["inventory_cost_usdc"], 21)
        self.assert_amount(self.view()["conditions"]["A"]["actual_buy_notional_usdc"], 49)
        self.apply("cancel_confirmed", self.proof_data("70", coverage=[first, second]))
        self.assert_amount(self.view()["conditions"]["A"]["actual_buy_notional_usdc"], 0)
        self.assert_amount(self.view()["adjusted_cash_usdc"], 79)
        self.assert_amount(self.view()["inventory_cost_usdc"], 21)

    def test_late_fill_after_cancel_still_debits_and_requires_reconciliation(self):
        self.submit()
        self.apply("cancel_requested", {"intent_id": "o1"})
        self.apply("cancel_confirmed", self.proof_data("70"))
        fill_id = self.apply("fill", self.fill_data())["receipt"]["fill_id"]
        view = self.view()
        self.assert_amount(view["adjusted_cash_usdc"], 86)
        self.assertIn("late_fill:" + fill_id, view["reconcile_reasons"])
        self.assertFalse(self.submit([order("b", "B-YES")], "B")["receipt"]["accepted"])
        self.apply("sources", sources("86", "86", "100", {"A-YES": {"shares": "20", "cost_usdc": "14"}},
                                      watermark=2, coverage=[fill_id]))
        self.apply("reconcile_account", {"resolved_issues": ["late_fill:" + fill_id]})
        self.assertEqual(self.view()["reconcile_reasons"], [])

    def test_fill_component_idempotence_and_status_do_not_double_count(self):
        self.submit()
        data = self.fill_data()
        first = self.apply("fill", data)["receipt"]["fill_id"]
        self.apply("fill", data)
        for status in ("MINED", "RETRYING", "MINED", "CONFIRMED"):
            data["status"] = status
            self.apply("fill", data)
            self.assert_amount(self.view()["inventory_cost_usdc"], 14)
        self.assertEqual(len(self.view()["fills"]), 1)
        self.assertEqual(self.view()["fills"][first]["status"], "CONFIRMED")
        with self.assertRaises(BudgetError):
            self.apply("fill", self.fill_data())

    def test_same_top_trade_distinct_maker_order_components(self):
        self.submit([order(qty="20"), order("o2", qty="20")])
        first = self.fill_data("10")
        second = self.fill_data("10", intent="o2")
        second["exchange_order_id"] = "0x" + "b" * 64
        a = self.apply("fill", first)["receipt"]["fill_id"]
        b = self.apply("fill", second)["receipt"]["fill_id"]
        self.assertNotEqual(a, b)
        self.assert_amount(self.view()["inventory_cost_usdc"], 14)

    def test_fill_immutable_conflict_rejected_transactionally(self):
        self.submit()
        self.apply("fill", self.fill_data())
        before = self.view()
        for field, value in (("quantity", "21"), ("price", "0.71"), ("fee_usdc", "1")):
            with self.subTest(field=field):
                bad = self.fill_data()
                bad[field] = value
                with self.assertRaises(BudgetError):
                    self.apply("fill", bad)
                self.assertEqual(self.view(), before)

    def test_failed_fill_does_not_refund(self):
        self.submit()
        self.apply("fill", self.fill_data())
        self.apply("fill", self.fill_data(status="FAILED"))
        self.assert_amount(self.view()["adjusted_cash_usdc"], 86)
        self.assertIn("failed_fill_reconcile_required", self.view()["reconcile_reasons"])
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assertIn("failed_fill_reconcile_required", self.view()["reconcile_reasons"])

    def test_simultaneous_adverse_cross_condition_fills_record_deficit(self):
        self.submit([order(qty="100")])
        self.submit([order("b", "B-YES", "100")], "B")
        self.apply("fill", self.fill_data("100"))
        data = self.fill_data("100", intent="b")
        data["exchange_order_id"] = "0x" + "b" * 64
        self.apply("fill", data)
        self.assert_amount(self.view()["adjusted_cash_usdc"], -40)
        self.assertIn("cash_deficit", self.view()["reconcile_reasons"])
        self.assertIsNone(self.view()["event_quote_ceiling_usdc"])

    def test_replace_counts_old_and_new_until_cancel_confirmation(self):
        self.submit([order(qty="100")])
        self.apply("cancel_requested", {"intent_id": "o1"})
        self.assertFalse(self.submit([order("replacement", qty="100")])["receipt"]["accepted"])
        self.apply("cancel_confirmed", self.proof_data("100"))
        self.assertTrue(self.submit([order("replacement", qty="100")])["receipt"]["accepted"])

    def test_sell_reserves_shares_not_cash_and_exit_has_priority(self):
        self.apply("sources", {"inventory": evidence("inventory", {"A-YES": {"shares": "100", "cost_usdc": "20"}}, 2)})
        before = self.view()["event_quote_ceiling_usdc"]
        result = self.submit([order("sell", qty="60", side="SELL")])
        self.assertTrue(result["receipt"]["accepted"])
        self.assertEqual(self.view()["event_quote_ceiling_usdc"], before)
        self.assertFalse(self.submit([order("sell2", qty="60", side="SELL")])["receipt"]["accepted"])
        self.assertIn("exit_sell_priority", self.submit()["receipt"]["reasons"])

    def test_sell_exit_can_be_reserved_with_cash_unknown(self):
        self.apply("sources", {"cash": evidence("cash", None, 2),
                               "inventory": evidence("inventory", {"A-YES": {"shares": "100", "cost_usdc": "20"}}, 2)})
        self.assertTrue(self.submit([order("sell", side="SELL")])["receipt"]["accepted"])

    def test_unknown_buy_does_not_block_exit_from_trusted_existing_inventory(self):
        self.apply("sources", {"inventory": evidence("inventory", {"A-YES": {"shares": "100", "cost_usdc": "20"}}, 2)})
        self.submit([order("b", "B-YES")], "B")
        self.apply("unknown", {"intent_id": "b"})
        self.assertTrue(self.submit([order("sell", side="SELL")])["receipt"]["accepted"])
        self.assertFalse(self.submit([order("buy-again")])["receipt"]["accepted"])

    def test_unknown_sell_keeps_share_reservation(self):
        self.apply("sources", {"inventory": evidence("inventory", {"A-YES": {"shares": "100", "cost_usdc": "20"}}, 2)})
        self.submit([order("sell", side="SELL")])
        self.apply("unknown", {"intent_id": "sell"})
        blocked = self.submit([order("sell-more", qty="31", side="SELL")])
        self.assertIn("sell_shares_unavailable", blocked["receipt"]["reasons"])
        self.assertTrue(self.submit([order("sell-rest", qty="30", side="SELL")])["receipt"]["accepted"])

    def test_unreflected_buy_fill_not_trusted_sell_stock(self):
        self.submit()
        self.apply("fill", self.fill_data())
        blocked = self.submit([order("sell", qty="20", side="SELL")])
        self.assertIn("sell_shares_unavailable", blocked["receipt"]["reasons"])

    def test_missing_inventory_never_allows_sell(self):
        self.apply("sources", {"inventory": evidence("inventory", None, 2)})
        self.assertIn("inventory_unknown_or_stale", self.submit([order("sell", side="SELL")])["receipt"]["reasons"])

    def test_paired_single_leg_or_partial_below_minimum_rejected(self):
        self.paired(minimum="50")
        self.assertIn("paired_leg_missing", self.submit([order(price="0.50")], revision=2)["receipt"]["reasons"])
        result = self.submit([order(price="0.50"), order("n", "A-NO", price="0.40")], revision=2)
        self.assertTrue(result["receipt"]["accepted"])
        self.apply("fill", self.fill_data("30", price="0.50"))
        row = self.view()["conditions"]["A"]
        self.assertIn("paired_leg_below_minimum", row["reasons"])
        self.assertEqual(row["action"], "cancel_buy_proposal")
        with self.assertRaises(BudgetError):
            self.ledger.assert_current(identity()[0], row["binding"], now=NOW)

    def test_paired_one_leg_fully_filled_rechecks_remaining_leg(self):
        self.paired()
        self.submit([order(price="0.50"), order("n", "A-NO", price="0.40")], revision=2)
        self.apply("fill", self.fill_data("70", price="0.50"))
        self.assertIn("paired_leg_missing", self.view()["conditions"]["A"]["reasons"])

    def test_mock_scoring_failed_and_unknown_block_submit_and_binding(self):
        for watermark, scoring, reason in ((2, None, "mock_scoring_unknown"), (3, False, "mock_scoring_failed")):
            with self.subTest(scoring=scoring):
                data = books(watermark=watermark)
                data["samples"]["A-YES"]["scoring"] = scoring
                self.apply("books", data)
                result = self.submit()
                self.assertIn(reason, result["receipt"]["reasons"])
                row = result["current"]["conditions"]["A"]
                self.assertIsNone(row["reward_eligible"])
                with self.assertRaises(BudgetError):
                    self.ledger.assert_current(identity()[0], row["binding"], now=NOW)

    def test_boolean_revision_scoring_and_account_version_not_accepted(self):
        for kind in ("books", "submit"):
            data = books() if kind == "books" else {"condition_id": "A", "account_version": self.view()["account_version"], "orders": [order()]}
            data["assignment_revision"] = True
            with self.subTest(kind=kind), self.assertRaises(BudgetError):
                self.apply(kind, data)
        data = books(watermark=2)
        data["samples"]["A-YES"]["scoring"] = 1
        with self.assertRaises(BudgetError):
            self.apply("books", data)
        data = {"condition_id": "A", "assignment_revision": 1, "account_version": True, "orders": [order()]}
        with self.assertRaises(BudgetError):
            self.apply("submit", data)

    def test_budget_after_yes94_fill_rechecks_no4_and_new_yes94(self):
        for idx, tier in ((1, "100"), (2, "150"), (3, "200")):
            with self.subTest(tier=tier):
                if idx != 1:
                    self.initialize(idx, tier=tier, capital=tier)
                self.paired(index=idx)
                self.submit([order(qty="100", price="0.94"), order("n", "A-NO", "100", "0.04")], index=idx, revision=2)
                fill = self.fill_data("100", price="0.94")
                fill["exchange_order_id"] = "0x" + str(idx) * 64
                self.apply("fill", fill, index=idx)
                view = self.view(index=idx)
                self.assert_amount(view["adjusted_cash_usdc"], Decimal(tier) - 94)
                self.assert_amount(view["conditions"]["A"]["actual_buy_notional_usdc"], 4)
                result = self.submit([order("yes-again", qty="100", price="0.94")], index=idx, revision=2)
                self.assertEqual(result["receipt"]["accepted"], idx == 3)

    def test_prior_sample_id_cannot_be_reused_after_an_intermediate_sample(self):
        self.apply("sources", {"cash": evidence("cash", "100", 2)})
        new = evidence("cash", "100", 3)
        new["sample_id"] = "cash-1"
        with self.assertRaises(BudgetError):
            self.apply("sources", {"cash": new})

    def test_assignment_change_cannot_relabel_prior_books(self):
        self.apply("configure", configuration(revision=2))
        with self.assertRaises(BudgetError):
            self.apply("books", books(revision=2, watermark=1))
        self.apply("books", books(revision=2))
        self.assertTrue(self.submit(revision=2)["receipt"]["accepted"])

    def test_cancel_confirmation_cannot_predate_cancel_request(self):
        self.submit()
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(1))
        with self.assertRaises(BudgetError):
            self.apply("cancel_confirmed", self.proof_data("70"), now=later(1))
        self.apply("cancel_confirmed", self.proof_data("70", when=later(1)), now=later(1))

    def test_second_cancel_requires_evidence_after_latest_attempt(self):
        self.submit()
        self.ack()
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(1))
        proof = self.proof_data("70", when=later(2))
        self.apply("reconcile_order", {**proof, "resolved_state": "live"}, now=later(2))
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(3))
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        before = self.view(later(3))
        with self.assertRaisesRegex(BudgetError, "order_proof_before_transition"):
            self.apply("cancel_confirmed", proof, now=later(3))
        self.assertEqual(self.view(later(3)), before)
        self.assertEqual(before["orders"]["o1"]["cancel_requested_at"], later(3))
        self.assert_amount(before["conditions"]["A"]["actual_buy_notional_usdc"], 49)
        fresh = self.proof_data("70", watermark=2, when=later(3))
        self.apply("cancel_confirmed", fresh, now=later(3))
        self.assert_amount(self.view(later(3))["conditions"]["A"]["actual_buy_notional_usdc"], 0)

    def test_reconcile_order_cannot_bypass_cancel_evidence_floor(self):
        self.submit()
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(2))
        old = self.proof_data("70", when=later(1))
        before = self.view(later(2))
        for resolved in ("live", "cancelled"):
            with self.subTest(resolved=resolved), self.assertRaisesRegex(BudgetError, "order_proof_before_transition"):
                self.apply("reconcile_order", {**old, "resolved_state": resolved}, now=later(2))
            self.assertEqual(self.view(later(2)), before)
        fresh = self.proof_data("70", when=later(2))
        self.apply("reconcile_order", {**fresh, "resolved_state": "cancelled"}, now=later(2))

    def test_same_timestamp_unknown_requires_new_proof_watermark(self):
        self.submit()
        proof = {**self.proof_data("70"), "resolved_state": "live"}
        self.apply("reconcile_order", proof)
        self.apply("unknown", {"intent_id": "o1"})
        before = self.view()
        with self.assertRaisesRegex(BudgetError, "order_proof_reused_after_transition"):
            self.apply("reconcile_order", proof)
        self.assertEqual(self.view(), before)
        newer = {**self.proof_data("70", watermark=2), "resolved_state": "live"}
        self.apply("reconcile_order", newer)
        self.assertEqual(self.view()["orders"]["o1"]["state"], "live")

    def test_pre_unknown_proof_rejected_even_with_new_watermark(self):
        self.submit()
        self.apply("unknown", {"intent_id": "o1"}, now=later(2))
        old = {**self.proof_data("70", watermark=2, when=later(1)), "resolved_state": "live"}
        with self.assertRaisesRegex(BudgetError, "order_proof_before_transition"):
            self.apply("reconcile_order", old, now=later(2))
        fresh = {**self.proof_data("70", watermark=2, when=later(2)), "resolved_state": "live"}
        self.apply("reconcile_order", fresh, now=later(2))

    def test_cancel_retry_does_not_postpone_same_attempt_proof(self):
        self.submit()
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(1))
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(3))
        self.apply("cancel_confirmed", self.proof_data("70", when=later(2)), now=later(3))
        self.assertEqual(self.view(later(3))["orders"]["o1"]["state"], "cancelled")

    def _assert_consumed_proof_outcome_is_immutable(self, side):
        if side == "SELL":
            self.apply("sources", {"inventory": evidence("inventory", {"A-YES": {"shares": "100", "cost_usdc": "20"}}, 2)})
        self.assertTrue(self.submit([order(qty="100", side=side)])["receipt"]["accepted"])
        self.ack()
        self.apply("cancel_requested", {"intent_id": "o1"}, now=later(1))
        proof = self.proof_data("100", when=later(2))
        live = {**proof, "resolved_state": "live"}
        first = self.apply("reconcile_order", live, key="live-proof", now=later(2))
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        replay = self.apply("reconcile_order", live, key="live-proof", now=later(3))
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["receipt"], first["receipt"])
        self.assertTrue(self.apply("reconcile_order", live, now=later(3))["receipt"]["accepted"])
        before = self.view(later(3))
        conflicting = {**proof, "resolved_state": "cancelled"}
        with self.assertRaisesRegex(BudgetError, "order_proof_outcome_conflict"):
            self.apply("reconcile_order", conflicting, key="conflicting-outcome", now=later(3))
        self.assertEqual(self.view(later(3)), before)
        self.assertEqual(self.ledger.db.execute("SELECT count(*) FROM events WHERE key='conflicting-outcome'").fetchone()[0], 0)
        with self.assertRaisesRegex(BudgetError, "idempotency_conflict"):
            self.apply("reconcile_order", conflicting, key="live-proof", now=later(3))
        self.assertFalse(self.submit([order("replacement", qty="100", side=side)], now=later(3))["receipt"]["accepted"])
        fresh = {**self.proof_data("100", watermark=2, when=later(4)), "resolved_state": "cancelled"}
        self.apply("reconcile_order", fresh, now=later(4))
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        replay = self.apply("reconcile_order", live, key="live-proof", now=later(4))
        self.assertTrue(replay["replayed"])
        self.assertEqual(replay["current"]["orders"]["o1"]["state"], "cancelled")
        self.assertEqual(replay["current"]["orders"]["o1"]["proof_resolved_state"], "cancelled")
        self.assertTrue(self.submit([order("replacement", qty="100", side=side)], now=later(4))["receipt"]["accepted"])

    def test_live_proof_cannot_release_buy_with_conflicting_outcome(self):
        self._assert_consumed_proof_outcome_is_immutable("BUY")

    def test_live_proof_cannot_release_sell_with_conflicting_outcome(self):
        self._assert_consumed_proof_outcome_is_immutable("SELL")

    def test_pending_cancel_leg_reserves_risk_but_cannot_qualify_pair(self):
        self.paired()
        pair = [order(price="0.50"), order("n", "A-NO", price="0.40")]
        self.assertTrue(self.submit(pair, revision=2)["receipt"]["accepted"])
        self.apply("cancel_requested", {"intent_id": "n"})
        row = self.view()["conditions"]["A"]
        self.assert_amount(row["no_shares"], 70)
        self.assert_amount(row["actual_buy_notional_usdc"], 63)
        self.assertIn("paired_leg_missing", row["reasons"])
        self.assertEqual(row["action"], "cancel_buy_proposal")
        result = self.submit([order("y2", qty="10", price="0.50")], revision=2)
        self.assertFalse(result["receipt"]["accepted"])
        self.assertIn("paired_leg_missing", result["receipt"]["reasons"])
        self.assertNotIn("y2", self.view()["orders"])

    def test_replacement_pair_still_counts_cancelling_leg_capacity(self):
        self.paired()
        self.submit([order(price="0.50"), order("n", "A-NO", price="0.40")], revision=2)
        self.apply("cancel_requested", {"intent_id": "n"})
        blocked = self.submit([order("large-n", "A-NO", qty="40", price="0.40")], revision=2)
        self.assertIn("legacy_paired_capacity", blocked["receipt"]["reasons"])
        allowed = self.submit([order("small-n", "A-NO", qty="20", price="0.40")], revision=2)
        self.assertTrue(allowed["receipt"]["accepted"])
        self.assert_amount(allowed["current"]["conditions"]["A"]["no_shares"], 90)

    def test_filled_sell_no_longer_blocks_buy_after_inventory_reconciliation(self):
        self.apply("sources", {"inventory": evidence("inventory", {"A-YES": {"shares": "20", "cost_usdc": "14"}}, 2)})
        self.assertTrue(self.submit([order("sell", qty="20", side="SELL")])["receipt"]["accepted"])
        fill = self.fill_data("20", intent="sell", status="CONFIRMED")
        fill["inventory_cost_usdc"] = "14"
        fill_id = self.apply("fill", fill)["receipt"]["fill_id"]
        self.apply("sources", sources(watermark=3, coverage=[fill_id]))
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assertEqual(self.view()["orders"]["sell"]["remaining"], "0")
        self.assertTrue(self.submit([order("after-exit", qty="10")])["receipt"]["accepted"])

    def test_partial_sell_still_has_exit_priority(self):
        self.apply("sources", {"inventory": evidence("inventory", {"A-YES": {"shares": "20", "cost_usdc": "14"}}, 2)})
        self.submit([order("sell", qty="20", side="SELL")])
        fill = self.fill_data("10", intent="sell", status="CONFIRMED")
        fill["inventory_cost_usdc"] = "7"
        self.apply("fill", fill)
        self.assert_amount(self.view()["orders"]["sell"]["remaining"], 10)
        self.assertIn("exit_sell_priority", self.submit([order("after-partial", qty="10")])["receipt"]["reasons"])

    def test_full_scale_decimal_product_and_fill_survive_reopen(self):
        qty, price = "1.123456789012", "0.13"
        result = self.submit([order(qty=qty, price=price)])
        self.assertTrue(result["receipt"]["accepted"])
        self.assert_amount(result["current"]["displayed_cross_condition_notional_usdc"], "0.14604938257156")
        self.apply("fill", self.fill_data("0.123456789012", price=price))
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assert_amount(self.view()["inventory_cost_usdc"], "0.01604938257156")
        self.assert_amount(self.view()["adjusted_cash_usdc"], "99.98395061742844")
        self.assert_amount(self.view()["displayed_cross_condition_notional_usdc"], "0.13")

    def test_unknown_fees_remain_null_in_diagnostics(self):
        data = books(watermark=2)
        data["samples"]["A-YES"]["fee_rate"] = None
        self.apply("books", data)
        self.assertIsNone(self.view()["conditions"]["A"]["fee_reserve_usdc"])
        self.apply("books", books(watermark=3))
        self.submit()
        self.apply("fill", self.fill_data(fee=None))
        self.assertIsNone(self.view()["adjusted_cash_usdc"])
        self.assertIsNone(self.view()["adjusted_allowance_usdc"])

    def test_book_probabilities_bounds_and_interval_order(self):
        for changes in ({"best_ask": "100"}, {"best_ask": "0"},
                        {"reward_low": "0.8", "reward_high": "0.2"},
                        {"reward_high": "1.01"}, {"reward_low": "-0.1"}):
            with self.subTest(changes=changes):
                data = books(watermark=2)
                data["samples"]["A-YES"].update(changes)
                before = self.view()
                with self.assertRaises(BudgetError):
                    self.apply("books", data)
                self.assertEqual(self.view(), before)
        data = books(watermark=2)
        data["samples"]["A-YES"].update(best_ask="1", reward_low="0", reward_high="1")
        self.apply("books", data)
        self.assertTrue(self.submit()["receipt"]["accepted"])
        data = books(watermark=3)
        data["samples"]["A-YES"]["best_ask"] = None
        self.apply("books", data)
        self.assertIn("book_or_fee_unknown:A-YES", self.view()["conditions"]["A"]["reasons"])

    def test_balance_change_reassesses_all_tracked_conditions_in_same_commit(self):
        self.submit()
        self.submit([order("b", "B-YES")], "B")
        old = self.view()
        self.apply("sources", {"cash": evidence("cash", "30", 2)})
        view = self.view()
        for cid in ("A", "B"):
            row = view["conditions"][cid]
            self.assertEqual(row["action"], "cancel_buy_proposal")
            self.assertIn("event_cash_notional", row["reasons"])
            self.assertEqual(row["binding"]["account_version"], view["account_version"])
            self.assertGreater(view["account_version"], old["account_version"])
        state = json.loads(self.ledger.db.execute("SELECT state FROM makers").fetchone()[0])
        self.assertEqual(state["proposals"], view["conditions"])
        self.assertEqual(view["orders"]["o1"]["state"], "pending")

    def test_fill_change_reassesses_other_condition_without_timer(self):
        self.submit([order(qty="100")])
        self.submit([order("b", "B-YES", "100")], "B")
        result = self.apply("fill", self.fill_data("100"))
        row = result["current"]["conditions"]["B"]
        self.assertEqual(row["action"], "cancel_buy_proposal")
        self.assertIn("event_cash_notional", row["reasons"])

    def test_below_minimum_remaining_never_claims_reward(self):
        self.apply("configure", configuration(revision=2, minimum="100"))
        self.apply("books", books(revision=2))
        self.submit([order(qty="100")], revision=2)
        self.apply("fill", self.fill_data("20"))
        row = self.view()["conditions"]["A"]
        self.assertIn("below_minimum_shares", row["reasons"])
        self.assertIsNone(row["reward_eligible"])

    def test_identity_alias_or_rebinding_cannot_duplicate_cash(self):
        bads = []
        for field, value in (("account_id", "new-alias"), ("account_index", 21), ("host_id", "elsewhere"), ("signature_type", 1)):
            event = envelope("register", {"tier_usdc": "100", "margin_usdc": "0"})
            event["route"][field] = value
            if field == "signature_type":
                event["route"]["account_uid"] = canonical_maker(event["maker"]) + ":1"
            bads.append(event)
        event = envelope("register", {"tier_usdc": "100", "margin_usdc": "0"}, 2)
        event["route"]["account_id"] = identity()[1]["account_id"]
        bads.append(event)
        for idx, event in enumerate(bads):
            with self.subTest(idx=idx), self.assertRaises(BudgetError):
                self.ledger.apply(f"alias-{idx}", event, now=NOW)
        event = envelope("sources", sources())
        event["route"]["host_id"] = "new-host"
        with self.assertRaises(BudgetError):
            self.ledger.apply("route-change", event, now=NOW)

    def test_condition_pair_identity_fixed_and_revision_resets_evidence(self):
        config = configuration(revision=2)
        bad = deepcopy(config)
        bad["yes_token"] = "new-token"
        with self.assertRaises(BudgetError):
            self.apply("configure", bad)
        self.apply("configure", config)
        self.assertFalse(self.submit(revision=2)["receipt"]["accepted"])
        with self.assertRaises(BudgetError):
            self.apply("books", books(revision=1))
        self.apply("books", books(revision=2))
        self.assertTrue(self.submit(revision=2)["receipt"]["accepted"])

    def test_shared_tokens_cannot_move_between_conditions(self):
        config = configuration("C")
        config["yes_token"] = "A-YES"
        with self.assertRaises(BudgetError):
            self.apply("configure", config)

    def test_idempotency_same_payload_and_conflict(self):
        data = {"cash": evidence("cash", "90", 2)}
        first = self.apply("sources", data, key="repeat")
        second = self.apply("sources", data, key="repeat", now=later(1))
        self.assertTrue(second["replayed"])
        self.assertEqual(first["receipt"], second["receipt"])
        self.assertEqual(first["current"]["account_version"], second["current"]["account_version"])
        data["cash"]["value"] = "80"
        with self.assertRaises(BudgetError):
            self.apply("sources", data, key="repeat", now=later(1))

    def test_replay_returns_historical_receipt_but_expired_current_view(self):
        data = {"cash": evidence("cash", "90", 2)}
        self.apply("sources", data, key="repeat")
        result = self.apply("sources", data, key="repeat", now=later(301))
        self.assertIsNone(result["current"]["event_quote_ceiling_usdc"])
        self.assertEqual(result["receipt"]["generated_at"], NOW)

    def test_old_proposal_cannot_overwrite_new_state(self):
        binding = self.view()["conditions"]["A"]["binding"]
        self.assertTrue(self.ledger.assert_current(identity()[0], binding, now=NOW))
        self.apply("sources", {"cash": evidence("cash", "90", 2)})
        with self.assertRaises(BudgetError):
            self.ledger.assert_current(identity()[0], binding, now=NOW)

    def test_current_binding_does_not_bypass_expiry(self):
        binding = self.view()["conditions"]["A"]["binding"]
        with self.assertRaises(BudgetError):
            self.ledger.assert_current(identity()[0], binding, now=later(301))

    def test_source_regression_cached_read_and_future_rejected(self):
        for alter in (lambda s: s.update(value="99"),
                      lambda s: s.update(watermark=2, value="99"),
                      lambda s: s.update(observed_at=later(1)),
                      lambda s: s.update(source_id="someone-else")):
            sample = sources()["cash"]
            alter(sample)
            with self.subTest(sample=sample), self.assertRaises(BudgetError):
                self.apply("sources", {"cash": sample})

    def test_fill_coverage_cannot_regress_or_name_unknown_fill(self):
        with self.assertRaises(BudgetError):
            self.apply("sources", {"cash": evidence("cash", "80", 2, coverage=["invented"])})
        self.submit()
        fill_id = self.apply("fill", self.fill_data())["receipt"]["fill_id"]
        self.apply("sources", {"cash": evidence("cash", "86", 2, coverage=[fill_id])})
        with self.assertRaises(BudgetError):
            self.apply("sources", {"cash": evidence("cash", "86", 3)})

    def test_fresh_rest_book_old_event_time_is_valid_per_token(self):
        self.assertTrue(self.submit()["receipt"]["accepted"])
        self.apply("sources", sources(watermark=2, when=later(301)), now=later(301))
        partial = books(watermark=2, when=later(301))
        del partial["samples"]["A-NO"]
        self.apply("books", partial, now=later(301))
        row = self.view(later(301))["conditions"]["A"]
        self.assertIn("book_unknown_or_stale:A-NO", row["reasons"])
        self.assertNotIn("book_unknown_or_stale:A-YES", row["reasons"])

    def test_book_reread_does_not_refresh_and_cached_edit_rejected(self):
        self.apply("books", books(), now=later(301))
        row = self.view(later(301))["conditions"]["A"]
        self.assertIn("book_unknown_or_stale:A-YES", row["reasons"])
        bad = books(when=later(301))
        with self.assertRaises(BudgetError):
            self.apply("books", bad, now=later(301))

    def test_mock_market_guards_and_unknown_fee(self):
        for field, value, reason in (("category", "weather", "market_policy_block"),
                                     ("category", "up_down", "market_policy_block"),
                                     ("end_at", later(60), "near_or_after_end")):
            with self.subTest(field=field, value=value):
                rev = self.view()["conditions"]["A"]["binding"]["assignment_revision"] + 1
                config = configuration(revision=rev)
                config[field] = value
                self.apply("configure", config)
                self.apply("books", books(revision=rev))
                self.assertIn(reason, self.submit(revision=rev)["receipt"]["reasons"])
        self.apply("configure", configuration(revision=5))
        data = books(revision=5)
        data["samples"]["A-YES"]["fee_rate"] = None
        self.apply("books", data)
        self.assertIn("book_or_fee_unknown:A-YES", self.submit(revision=5)["receipt"]["reasons"])

    def test_off_tick_and_crossing_quote_rejected(self):
        with self.assertRaises(BudgetError):
            self.submit([order(price="0.701")])
        data = books(watermark=2)
        data["samples"]["A-YES"]["best_ask"] = "0.70"
        self.apply("books", data)
        self.assertIn("would_cross_book", self.submit()["receipt"]["reasons"])

    def test_wire_v1_live_and_float_rejected(self):
        for field, value in (("schema_version", 1), ("source", "live"), ("budget_model", "old")):
            e = envelope("sources", sources())
            e[field] = value
            with self.subTest(field=field), self.assertRaises(BudgetError):
                self.ledger.apply("bad", e, now=NOW)
        e = envelope("sources", sources())
        e["data"]["cash"]["value"] = 100.0
        with self.assertRaises(BudgetError):
            self.ledger.apply("bad-float", e, now=NOW)

    def test_no_real_money_columns_and_exact_roundtrip(self):
        self.submit([order(qty="1.123456", price="0.13")])
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assert_amount(self.view()["displayed_cross_condition_notional_usdc"], "0.14604928")
        sql = " ".join(r[0] for r in self.ledger.db.execute("SELECT sql FROM sqlite_master WHERE type='table'"))
        self.assertNotIn("REAL", sql.upper())

    def test_atomic_error_leaves_no_input_or_state_change(self):
        before = self.view()
        broken = sources(watermark=2)
        broken["inventory"]["value"] = {"A-YES": {"shares": "NaN", "cost_usdc": "0"}}
        with self.assertRaises(BudgetError):
            self.apply("sources", broken, key="rollback-all")
        self.assertEqual(self.view(), before)
        self.assertEqual(self.ledger.db.execute("SELECT count(*) FROM events WHERE key='rollback-all'").fetchone()[0], 0)

    def test_two_connection_write_timeout_no_partial_journal(self):
        other = BudgetLedger(self.path, timeout=0.02)
        try:
            self.ledger.db.execute("BEGIN IMMEDIATE")
            with self.assertRaises(BudgetBusy):
                other.apply("locked", envelope("sources", sources(watermark=2)), now=NOW)
            self.ledger.db.execute("ROLLBACK")
            self.assertEqual(other.db.execute("SELECT count(*) FROM events WHERE key='locked'").fetchone()[0], 0)
        finally:
            if self.ledger.db.in_transaction:
                self.ledger.db.execute("ROLLBACK")
            other.close()

    def concurrent_submits(self, same_condition):
        barrier = threading.Barrier(2)
        version = self.view()["account_version"]
        def worker(i):
            ledger = BudgetLedger(self.path, timeout=2)
            try:
                cid = "A" if same_condition or i == 0 else "B"
                data = {"condition_id": cid, "assignment_revision": 1, "account_version": version,
                        "orders": [order(f"parallel-{i}", cid + "-YES", qty="100")]}
                barrier.wait(timeout=5)
                try:
                    return ledger.apply(f"parallel-key-{i}", envelope("submit", data), now=NOW)
                except BudgetError as exc:
                    if str(exc) != "stale_account_version":
                        raise
                    data["account_version"] = ledger.snapshot(identity()[0], now=NOW)["account_version"]
                    return ledger.apply(f"parallel-retry-{i}", envelope("submit", data), now=NOW)
            finally:
                ledger.close()
        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(worker, (0, 1)))

    def test_concurrent_same_condition_serializes_and_prevents_overcapacity(self):
        results = self.concurrent_submits(True)
        self.assertEqual(sum(r["receipt"]["accepted"] for r in results), 1)
        blocked = [r for r in results if not r["receipt"]["accepted"]][0]
        self.assertIn("event_cash_notional", blocked["receipt"]["reasons"])
        self.assert_amount(self.view()["conditions"]["A"]["capacity"], 70)

    def test_concurrent_independent_conditions_can_both_reserve_after_version_retry(self):
        results = self.concurrent_submits(False)
        self.assertTrue(all(r["receipt"]["accepted"] for r in results))
        for cid in ("A", "B"):
            self.assert_amount(self.view()["conditions"][cid]["capacity"], 70)

    def crash(self, phase, event):
        script = """
import json, os, sys
from platforms.polymarket.maker.small_cap_budget import BudgetLedger
phase = sys.argv[2]
def fault(point):
    if point == phase:
        os._exit(91 if point == 'before_commit' else 92)
ledger = BudgetLedger(sys.argv[1], fault_hook=fault)
ledger.apply('crash-event', json.loads(sys.argv[3]), now=sys.argv[4])
raise SystemExit('fault hook was not reached')
"""
        return subprocess.run([sys.executable, "-c", script, str(self.path), phase, json.dumps(event), NOW],
                              cwd=ROOT, capture_output=True, text=True, timeout=20)

    def test_real_process_death_before_commit_reopen_replay_once(self):
        before = self.view()
        event = envelope("submit", {"condition_id": "A", "assignment_revision": 1,
                                    "account_version": before["account_version"], "orders": [order()]})
        self.assertEqual(self.crash("before_commit", event).returncode, 91)
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assertEqual(self.view(), before)
        result = self.ledger.apply("crash-event", event, now=NOW)
        self.assertFalse(result["replayed"])
        self.assertTrue(result["receipt"]["accepted"])

    def test_real_process_death_after_commit_reopen_replay_no_duplicate(self):
        before = self.view()
        event = envelope("submit", {"condition_id": "A", "assignment_revision": 1,
                                    "account_version": before["account_version"], "orders": [order()]})
        self.assertEqual(self.crash("after_commit", event).returncode, 92)
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        result = self.ledger.apply("crash-event", event, now=NOW)
        self.assertTrue(result["replayed"])
        self.assertEqual(result["current"]["account_version"], before["account_version"] + 1)
        self.assertEqual(len(result["current"]["orders"]), 1)

    def test_unknown_survives_real_crash_after_commit(self):
        self.submit()
        event = envelope("unknown", {"intent_id": "o1"})
        self.assertEqual(self.crash("after_commit", event).returncode, 92)
        self.ledger.close()
        self.ledger = BudgetLedger(self.path)
        self.assertIn("unknown_submission_reconcile_required", self.view()["reconcile_reasons"])
        self.assertTrue(self.ledger.apply("crash-event", event, now=NOW)["replayed"])

    def test_storage_version_does_not_silently_migrate(self):
        wrong = Path(self.tmp.name) / "wrong.sqlite3"
        conn = sqlite3.connect(wrong)
        conn.execute("PRAGMA user_version=2")
        conn.close()
        with self.assertRaises(BudgetError):
            BudgetLedger(wrong)


if __name__ == "__main__":
    unittest.main()
