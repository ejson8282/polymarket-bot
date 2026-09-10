"""Fabricated API-shaped responses only; no accounts, files or network access."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import localcontext
import json
from types import SimpleNamespace
import unittest

from platforms.polymarket.maker.small_cap_observation import (
    CLOB_HOST, END_CURSOR, INITIAL_CURSOR, ClobReadTransport, ObservationError,
    collect_account_observation, observation_at,
)


NOW = datetime(2026, 9, 10, 8, tzinfo=timezone.utc)
IDENTITY = {"chain_id": 137, "signature_type": 2, "maker_address": "0x" + "1" * 40}
OTHER = "0x" + "2" * 40
SPENDER = "0x" + "3" * 40


def page(rows, cursor=END_CURSOR):
    return {"data": rows, "next_cursor": cursor, "count": len(rows)}


def order(oid="order-1", **changes):
    return {"id": oid, "maker_address": IDENTITY["maker_address"], "market": "condition-1",
            "asset_id": "token-no", "side": "BUY", "status": "LIVE", "price": "0.10",
            "original_size": "100", "size_matched": "5.25", **changes}


def trade(**changes):
    return {"id": "trade-1", "trader_side": "MAKER", "status": "CONFIRMED", "market": "condition-1",
            "match_time": str(int(NOW.timestamp()) - 30), "side": "BUY", "price": "0.90", "size": "74.22",
            "maker_orders": [
                {"order_id": "order-1", "maker_address": IDENTITY["maker_address"], "asset_id": "token-no",
                 "side": "BUY", "matched_amount": "54.22", "price": "0.10"},
                {"order_id": "not-ours", "maker_address": OTHER, "asset_id": "token-yes",
                 "side": "SELL", "matched_amount": "20", "price": "0.90"}], **changes}


class FakeTransport:
    source = "synthetic"

    def __init__(self):
        self.identity = deepcopy(IDENTITY)
        self.calls = []
        self.responses = {
            "/balance-allowance": {"balance": "123456789", "allowances": {SPENDER: "90000000", OTHER: "999999999999999"}},
            "/data/orders": page([order()]), "/data/trades": page([trade()]),
            "/order-scoring": {"scoring": True},
        }

    def get(self, path, params):
        self.calls.append((path, deepcopy(params)))
        response = self.responses[path]
        if isinstance(response, Exception):
            raise response
        return deepcopy(response(params) if callable(response) else response)


class ObservationTests(unittest.TestCase):
    def setUp(self):
        self.transport = FakeTransport()

    def collect(self, **kwargs):
        return collect_account_observation(self.transport, IDENTITY, collateral_spender=SPENDER,
            trade_after=int(NOW.timestamp()) - 60, trade_before=int(NOW.timestamp()), clock=lambda: NOW, **kwargs)

    def test_real_shapes_separate_own_components_and_units(self):
        result = self.collect()
        self.assertEqual(result["source"], "synthetic")
        self.assertEqual(result["account_uid"], "137:2:" + IDENTITY["maker_address"])
        collateral = result["samples"]["collateral"]["current"]
        self.assertEqual(collateral["balance_usdc"], "123.456789")
        self.assertEqual(collateral["allowance_usdc"], "90.000000")
        self.assertIsNone(collateral["fill_coverage"])
        self.assertIsNone(collateral["effective_capital_usdc"])
        row = result["samples"]["trades"]["current"]["rows"][0]
        self.assertEqual((row["quantity"], row["price"], row["notional_usdc"]), ("54.22", "0.10", "5.4220"))
        self.assertEqual(row["token_id"], "token-no")
        self.assertIsNone(row["fee_usdc"])
        self.assertEqual(result["samples"]["orders"]["current"]["rows"][0]["remaining_size"], "94.75")
        self.assertIs(result["scoring"]["order-1"]["current"]["scoring"], True)
        for capability in ("live_enabled", "mutation_enabled", "budget_admission_enabled", "atomic_snapshot"):
            self.assertIs(result[capability], False)

    def test_no_selected_spender_does_not_take_largest_allowance(self):
        del self.transport.responses["/balance-allowance"]["allowances"][SPENDER]
        data = self.collect()["samples"]["collateral"]["current"]
        self.assertIsNone(data["allowance_usdc"])
        self.assertEqual(data["allowance_reason"], "selected_spender_missing")

    def test_collateral_does_not_guess_units_or_zero_missing(self):
        for balance in (1.5, 123, "123.5", "1e6", "NaN", None, True, "-1"):
            with self.subTest(balance=balance):
                self.transport.responses["/balance-allowance"]["balance"] = balance
                sample = self.collect()["samples"]["collateral"]
                self.assertIsNone(sample["current"])
                self.assertEqual(sample["status"], "unknown")

    def test_uint256_allowance_and_decimal_context_are_exact(self):
        raw = str(2**256 - 1)
        self.transport.responses["/balance-allowance"]["allowances"][SPENDER] = raw
        with localcontext() as context:
            context.prec = 3
            data = self.collect()["samples"]["collateral"]["current"]
        self.assertEqual(data["allowance_usdc"], raw[:-6] + "." + raw[-6:])

    def test_pages_dedupe_identical_rows(self):
        self.transport.responses["/data/orders"] = lambda p: page([order()], "next") if p["next_cursor"] == INITIAL_CURSOR else page([order(), order("order-2")])
        result = self.collect()
        data = result["samples"]["orders"]["current"]
        self.assertEqual(len(data["rows"]), 2)
        self.assertEqual(data["pages"], 2)
        self.assertTrue(data["pagination_complete"])
        self.assertFalse(data["atomic_snapshot"])

    def test_pagination_cycle_is_unknown_not_partial_success(self):
        self.transport.responses["/data/orders"] = page([order()], INITIAL_CURSOR)
        result = self.collect()
        self.assertEqual(result["samples"]["orders"]["reason"], "cursor_cycle")
        self.assertIsNone(result["samples"]["orders"]["current"])
        self.assertEqual(result["scoring"], {})

    def test_missing_cursor_and_count_mismatch_fail_closed(self):
        for bad in ({"data": []}, {"data": [], "next_cursor": ""}, {**page([]), "count": 1}):
            with self.subTest(bad=bad):
                self.transport.responses["/data/orders"] = bad
                self.assertIsNone(self.collect()["samples"]["orders"]["current"])

    def test_page_and_row_limits(self):
        self.transport.responses["/data/orders"] = lambda p: page([order()], p["next_cursor"] + "n")
        self.assertEqual(self.collect(max_pages=2)["samples"]["orders"]["reason"], "page_limit")
        self.transport.responses["/data/orders"] = page([order(), order("order-2")])
        self.assertEqual(self.collect(max_rows=1)["samples"]["orders"]["reason"], "row_limit")

    def test_changed_duplicate_does_not_silently_overwrite(self):
        self.transport.responses["/data/orders"] = page([order(), order(price="0.11")])
        self.assertEqual(self.collect()["samples"]["orders"]["reason"], "duplicate_conflict")

    def test_transport_identity_mismatch_makes_no_calls(self):
        self.transport.identity["maker_address"] = OTHER
        with self.assertRaisesRegex(ObservationError, "transport_identity_mismatch"):
            self.collect()
        self.assertEqual(self.transport.calls, [])

    def test_wrong_owner_orders_not_filtered_into_false_empty(self):
        self.transport.responses["/data/orders"] = page([order(maker_address=OTHER)])
        self.assertEqual(self.collect()["samples"]["orders"]["reason"], "order_maker_mismatch")

    def test_overmatched_unknown_status_and_float_orders_fail_closed(self):
        for changes in ({"size_matched": "101"}, {"status": "NEW_UNKNOWN_STATUS"}, {"price": 0.1}):
            with self.subTest(changes=changes):
                self.transport.responses["/data/orders"] = page([order(**changes)])
                self.assertIsNone(self.collect()["samples"]["orders"]["current"])

    def test_matched_retrying_failed_trades_remain_visible_not_refunded(self):
        for status in ("MATCHED", "MINED", "RETRYING", "FAILED", "TRADE_STATUS_CONFIRMED"):
            with self.subTest(status=status):
                self.transport.responses["/data/trades"] = page([trade(status=status)])
                result = self.collect()
                row = result["samples"]["trades"]["current"]["rows"][0]
                self.assertEqual(row["status"], status.removeprefix("TRADE_STATUS_"))
                self.assertFalse(result["budget_admission_enabled"])

    def test_trade_identity_and_component_fields_are_required(self):
        for changes in ({"maker_orders": []}, {"trader_side": "OTHER"}, {"status": "NEW"}):
            with self.subTest(changes=changes):
                self.transport.responses["/data/trades"] = page([trade(**changes)])
                self.assertIsNone(self.collect()["samples"]["trades"]["current"])
        raw = trade()
        del raw["maker_orders"][0]["side"]
        self.transport.responses["/data/trades"] = page([raw])
        self.assertEqual(self.collect()["samples"]["trades"]["reason"], "own_side_unknown")

    def test_taker_requires_matching_public_account(self):
        raw = trade(trader_side="TAKER", maker_address=IDENTITY["maker_address"], taker_order_id="taker-1", asset_id="token-yes")
        self.transport.responses["/data/trades"] = page([raw])
        rows = self.collect()["samples"]["trades"]["current"]["rows"]
        self.assertEqual((rows[0]["role"], rows[0]["quantity"]), ("TAKER", "74.22"))
        raw["maker_address"] = OTHER
        self.transport.responses["/data/trades"] = page([raw])
        self.assertEqual(self.collect()["samples"]["trades"]["reason"], "taker_maker_mismatch")

    def test_two_own_components_are_distinct_not_whole_trade_doubled(self):
        raw = trade()
        raw["maker_orders"].append({**raw["maker_orders"][0], "order_id": "order-2", "matched_amount": "10"})
        self.transport.responses["/data/trades"] = page([raw, deepcopy(raw)])
        rows = self.collect()["samples"]["trades"]["current"]["rows"]
        self.assertEqual(len(rows), 2)
        self.assertEqual({row["quantity"] for row in rows}, {"54.22", "10"})

    def test_conflicting_trade_component_and_window_are_not_silently_ignored(self):
        changed = trade()
        changed["maker_orders"][0]["matched_amount"] = "55"
        self.transport.responses["/data/trades"] = page([trade(), changed])
        self.assertEqual(self.collect()["samples"]["trades"]["reason"], "duplicate_conflict")
        self.transport.responses["/data/trades"] = page([trade(match_time=str(int(NOW.timestamp()) - 61))])
        self.assertEqual(self.collect()["samples"]["trades"]["reason"], "trade_outside_window")

    def test_scoring_bool_only_false_is_not_unknown(self):
        self.transport.responses["/order-scoring"] = {"scoring": False}
        row = self.collect()["scoring"]["order-1"]
        self.assertEqual(row["status"], "current")
        self.assertIs(row["current"]["scoring"], False)
        for value in ("true", 1, None):
            with self.subTest(value=value):
                self.transport.responses["/order-scoring"] = {"scoring": value}
                self.assertIsNone(self.collect()["scoring"]["order-1"]["current"])

    def test_only_live_remaining_buy_is_scored_and_cap_is_explicit(self):
        self.transport.responses["/data/orders"] = page([order(), order("2"), order("3", side="SELL"), order("4", status="DELAYED"), order("5", size_matched="100")])
        result = self.collect(max_scoring=1)
        self.assertEqual(len(result["scoring"]), 1)
        self.assertEqual(len(result["scoring_unchecked_order_ids"]), 1)
        self.assertEqual(sum(path == "/order-scoring" for path, _ in self.transport.calls), 1)

    def test_order_change_or_read_failure_after_scoring_invalidates_true(self):
        for after in (page([]), page([order(size_matched="10")]), page([order(price="0.11")]), RuntimeError("secret")):
            with self.subTest(after=after):
                count = [0]
                def sequence(_):
                    count[0] += 1
                    if count[0] == 1:
                        return page([order()])
                    if isinstance(after, Exception):
                        raise after
                    return after
                self.transport.responses["/data/orders"] = sequence
                row = self.collect()["scoring"]["order-1"]
                self.assertIsNone(row["current"])
                self.assertEqual(row["reason"], "order_changed_or_unverified")

    def test_new_order_after_scoring_is_explicitly_unchecked(self):
        count = [0]
        def sequence(_):
            count[0] += 1
            return page([order()]) if count[0] == 1 else page([order(), order("new")])
        self.transport.responses["/data/orders"] = sequence
        self.assertEqual(self.collect()["scoring_unchecked_order_ids"], ["new"])

    def test_stale_consumption_keeps_last_known_without_refresh(self):
        result = self.collect()
        original = deepcopy(result)
        expired = observation_at(result, now=NOW + timedelta(seconds=61))
        self.assertEqual(result, original)
        self.assertEqual(expired["generated_at"], original["generated_at"])
        for sample in list(expired["samples"].values()) + list(expired["scoring"].values()):
            self.assertEqual(sample["status"], "stale")
            self.assertIsNone(sample["current"])
            self.assertIsNotNone(sample["last_known"])
        with self.assertRaisesRegex(ObservationError, "view_before_report"):
            observation_at(result, now=NOW - timedelta(seconds=1))

    def test_slow_collection_expires_early_samples(self):
        tick = [NOW]
        def clock():
            result = tick[0]
            tick[0] += timedelta(seconds=15)
            return result
        result = collect_account_observation(self.transport, IDENTITY, collateral_spender=SPENDER,
            trade_after=int(NOW.timestamp()) - 60, trade_before=int(NOW.timestamp()), clock=clock)
        self.assertEqual(result["samples"]["collateral"]["status"], "stale")
        self.assertEqual(result["samples"]["orders"]["status"], "current")

    def test_transport_errors_and_extra_secret_fields_are_not_echoed(self):
        marker = "DO_NOT_PRINT_CREDENTIAL"
        self.transport.responses["/data/trades"] = ObservationError(marker)
        self.transport.responses["/data/orders"]["data"][0]["owner"] = marker
        self.transport.responses["/balance-allowance"]["api_key"] = marker
        result = self.collect()
        self.assertNotIn(marker, json.dumps(result))
        self.assertEqual(result["samples"]["trades"]["reason"], "read_failed")
        self.assertEqual(result["samples"]["orders"]["status"], "current")

    def test_invalid_bounds_and_naive_clocks_rejected(self):
        for option in ({"max_pages": True}, {"max_pages": 0}, {"max_rows": 10001}, {"max_scoring": 0}, {"max_age_sec": 301}):
            with self.subTest(option=option), self.assertRaises(ObservationError):
                self.collect(**option)
        with self.assertRaisesRegex(ObservationError, "utc_clock_required"):
            collect_account_observation(self.transport, IDENTITY, collateral_spender=SPENDER,
                trade_after=int(NOW.timestamp()) - 60, trade_before=int(NOW.timestamp()),
                clock=lambda: NOW.replace(tzinfo=None))

    def test_observation_view_rejects_changed_capabilities(self):
        report = self.collect()
        for flag in ("live_enabled", "mutation_enabled", "budget_admission_enabled", "atomic_snapshot"):
            changed = deepcopy(report)
            changed[flag] = True
            with self.subTest(flag=flag), self.assertRaisesRegex(ObservationError, "observation_capabilities"):
                observation_at(changed, now=NOW)

    def test_observation_view_rejects_future_sample_and_unknown_schema(self):
        report = self.collect()
        report["samples"]["orders"]["observed_to"] = (NOW + timedelta(seconds=1)).isoformat()
        with self.assertRaisesRegex(ObservationError, "sample_time_order"):
            observation_at(report, now=NOW)
        report = self.collect()
        report["schema_version"] = True
        with self.assertRaisesRegex(ObservationError, "observation_schema"):
            observation_at(report, now=NOW)

    def test_regressing_clock_cannot_produce_fresh_observation(self):
        times = iter([NOW, NOW, NOW - timedelta(seconds=1)])
        with self.assertRaisesRegex(ObservationError, "clock_regression"):
            collect_account_observation(self.transport, IDENTITY, collateral_spender=SPENDER,
                trade_after=int(NOW.timestamp()) - 60, trade_before=int(NOW.timestamp()), clock=lambda: next(times))

    def test_one_scoring_failure_does_not_mark_another_order_false(self):
        self.transport.responses["/data/orders"] = page([order("a"), order("b")])
        def score(params):
            if params["order_id"] == "a":
                raise RuntimeError("private-detail")
            return {"scoring": True}
        self.transport.responses["/order-scoring"] = score
        report = self.collect()
        self.assertIsNone(report["scoring"]["a"]["current"])
        self.assertIs(report["scoring"]["b"]["current"]["scoring"], True)

    def test_empty_orders_is_observation_not_cancellation_receipt(self):
        self.transport.responses["/data/orders"] = page([])
        report = self.collect()
        self.assertEqual(report["samples"]["orders"]["current"]["rows"], [])
        self.assertEqual(report["scoring"], {})
        self.assertIn("source_fill_coverage_unproven", report["blocked_reasons"])
        self.assertNotIn("cancel_confirmed", json.dumps(report))

    def test_repeated_identical_http_reads_do_not_create_fill_coverage(self):
        one, two = self.collect(), self.collect()
        self.assertEqual(one, two)
        self.assertIsNone(two["samples"]["collateral"]["current"]["fill_coverage"])


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        def headers(method, path):
            self.calls.append((method, path))
            return {"secret": "DO_NOT_PRINT"}
        def get(url, **kwargs):
            self.calls.append(("get", url))
            return {"ok": True}
        self.client = SimpleNamespace(host=CLOB_HOST, chain_id=137,
            builder=SimpleNamespace(signature_type=2, funder=IDENTITY["maker_address"]),
            _l2_headers=headers, _get=get)
        self.transport = ClobReadTransport(self.client, IDENTITY)

    def test_exact_get_path_only(self):
        self.assertEqual(self.transport.get("/data/orders", {"next_cursor": INITIAL_CURSOR}), {"ok": True})
        self.assertEqual(self.calls, [("GET", "/data/orders"), ("get", CLOB_HOST + "/data/orders")])

    def test_write_and_credential_endpoints_blocked_before_headers(self):
        for path in ("/order", "/cancel-all", "/auth/derive-api-key", "/balance-allowance/update", "https://elsewhere.invalid/data/orders"):
            with self.subTest(path=path), self.assertRaises(ObservationError):
                self.transport.get(path, {})
        self.assertEqual(self.calls, [])

    def test_unapproved_parameters_and_identity_drift_blocked(self):
        with self.assertRaises(ObservationError):
            self.transport.get("/data/orders", {"maker_address": OTHER})
        self.client.builder.funder = OTHER
        with self.assertRaises(ObservationError):
            self.transport.get("/data/orders", {})
        self.assertEqual(self.calls, [])

    def test_host_change_is_not_followed(self):
        self.client.host = "https://elsewhere.invalid"
        with self.assertRaises(ObservationError):
            self.transport.get("/order-scoring", {"order_id": "order-1"})
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()
