"""Wire-contract tests only; no trading, allocation, or production acceptance."""

import copy
from datetime import datetime, timedelta, timezone
from decimal import localcontext
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from platforms.polymarket.maker.account_roster import market_universe_sha256
from platforms.polymarket.maker.small_cap_contract import (
    ACCOUNTING_TYPES, BUDGET_DERIVED, BUDGET_INPUTS, COMMANDS, DISABLED_REASONS,
    FIXTURE_TIME, SmallCapContractError, business_day, freshness_status_at, reject_command,
    synthetic_command, synthetic_state, validate_command, validate_receipt,
    validate_state, validate_transition,
)


FIXTURES = Path(__file__).parent / "fixtures" / "polymarket_small_cap_contract"


def stamp(age=0, ttl=300, absent=None):
    observed = datetime(2026, 9, 9, 2, tzinfo=timezone.utc) - timedelta(seconds=age)
    return {
        "status": absent or ("fresh" if age <= ttl else "stale"),
        "observed_at": None if absent else observed.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "max_age_sec": ttl, "age_sec": None if absent else age,
        "reason": "not_observed" if absent else ("sample_expired" if age > ttl else None),
    }


def metric(value=None, age=0):
    f = stamp(age, absent="missing" if value is None else None)
    return {"current": value if f["status"] == "fresh" else None,
            "last_known": value if f["status"] == "stale" else None, "freshness": f}


class SmallCapContractTests(unittest.TestCase):
    def setUp(self):
        self.state = synthetic_state("shadow")

    def validate(self, state=None):
        return validate_state(self.state if state is None else state, now=FIXTURE_TIME)

    def assignment(self, state=None):
        return (self.state if state is None else state)["accounts"][0]["assignments"][0]

    def assert_bad(self, state, code=None):
        with self.assertRaises(SmallCapContractError) as error:
            self.validate(state)
        if code:
            self.assertEqual(error.exception.code, code)

    def revision_two_state(self):
        changed = synthetic_state("shadow", now="2026-09-09T02:03:01Z")
        a = changed["accounts"][0]
        a["assignment_revision"] = 2
        for assignment in a["assignments"]:
            assignment["assignment_revision"] = 2
            for i, sample in enumerate(assignment["scoring"]["samples"]):
                sample.update(assignment_revision=2, sample_id=f"rev2-{i}", source_snapshot_id=f"rev2-source-{i}")
        changed["accounts"][1]["assignment_effective_at"] = self.state["accounts"][1]["assignment_effective_at"]
        return changed

    def transition(self, changed):
        return validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=changed["generated_at"])

    def test_all_synthetic_scenarios_validate_and_roundtrip(self):
        for scenario in ("unavailable", "proposed", "shadow", "stale"):
            with self.subTest(scenario=scenario):
                state = synthetic_state(scenario)
                self.assertEqual(state, validate_state(json.loads(json.dumps(state)), now=FIXTURE_TIME))
                self.assertEqual(state["source"], "synthetic")
                self.assertIsNone(state["transport"]["service_url"])
                self.assertIsNone(state["transport"]["authentication"])
                self.assertEqual(state["receipts"], [])
                self.assertFalse(state["accounting_complete"])

    def test_checked_in_fixtures_match_producer(self):
        for scenario in ("unavailable", "proposed", "shadow", "stale"):
            with self.subTest(scenario=scenario):
                expected = json.loads((FIXTURES / f"{scenario}.json").read_text())
                self.assertEqual(expected, synthetic_state(scenario))
        rejection = json.loads((FIXTURES / "command_rejected.json").read_text())
        command = synthetic_command(self.state)
        self.assertEqual(rejection, reject_command(command, self.state, now=FIXTURE_TIME))
        validate_receipt(rejection, command, now=FIXTURE_TIME)

    def test_functions_have_no_network_or_file_side_effects(self):
        with patch("builtins.open", side_effect=AssertionError("file IO")), \
             patch("socket.socket", side_effect=AssertionError("network IO")), \
             patch("subprocess.Popen", side_effect=AssertionError("process IO")):
            state = synthetic_state("shadow")
            command = synthetic_command(state)
            rejected = reject_command(command, state, now=FIXTURE_TIME)
            validate_receipt(rejected, command, now=FIXTURE_TIME)

    def test_nonlive_flags_cannot_be_promoted(self):
        changes = (("mode", "live"), ("source", "production"), ("accounting_complete", True),
                   ("receipts", [{"status": "success"}]))
        for field, value in changes:
            with self.subTest(field=field):
                state = copy.deepcopy(self.state)
                state[field] = value
                self.assert_bad(state)
        for action in COMMANDS:
            state = copy.deepcopy(self.state)
            state["capabilities"]["mutation_commands"][action] = True
            self.assert_bad(state, "live_capability_forbidden")
        self.assertEqual(self.state["capabilities"]["disabled_reasons"], list(DISABLED_REASONS))

    def test_transport_is_explicitly_unavailable(self):
        for field, value in (("service_url", "http://localhost:9999"), ("authentication", "bearer"),
                             ("status", "ready"), ("dashboard_route_family", "/api/pm/engine")):
            state = copy.deepcopy(self.state)
            state["transport"][field] = value
            self.assert_bad(state, "live_capability_forbidden")

    def test_unknown_fields_and_secret_fields_rejected_without_echo(self):
        for target in ("root", "identity", "quote", "budget"):
            state = copy.deepcopy(self.state)
            obj = {"root": state, "identity": state["accounts"][0]["identity"],
                   "quote": self.assignment(state)["quote"], "budget": state["accounts"][0]["budget"]}[target]
            obj["private_key"] = "sensitive-value-must-not-echo"
            with self.assertRaises(SmallCapContractError) as error:
                self.validate(state)
            self.assertNotIn("sensitive-value", str(error.exception))

    def test_missing_required_field_rejected(self):
        del self.state["accounts"][0]["budget"]["unknown_buy_reserved_usdc"]
        self.assert_bad(self.state, "invalid_fields")

    def test_schema_and_revision_not_coerced(self):
        for value in (True, "1", 1.0, None, -1):
            for field in ("schema_version", "assignment_revision"):
                state = copy.deepcopy(self.state)
                target = state if field == "schema_version" else state["accounts"][0]
                target[field] = value
                self.assert_bad(state)

    def test_identity_range_and_canonical_uid(self):
        for field, value in (("account_index", 31), ("account_index", 0), ("account_index", True),
                             ("host_id", "../vps1"), ("host_id", "VPS1"), ("account_id", ""),
                             ("account_uid", "wrong"), ("account_uid_key", "0" * 16),
                             ("signature_type", True), ("signature_type", 9), ("chain_id", "137"),
                             ("maker_address", "0xABC")):
            with self.subTest(field=field, value=value):
                state = copy.deepcopy(self.state)
                state["accounts"][0]["identity"][field] = value
                self.assert_bad(state)

    def test_duplicate_host_cache_cannot_duplicate_maker_account(self):
        duplicate = copy.deepcopy(self.state["accounts"][0])
        duplicate["identity"].update(account_index=22, account_id="fixture-copy", host_id="another-host")
        self.state["accounts"].append(duplicate)
        self.assert_bad(self.state, "duplicate_account")

    def test_same_market_across_makers_and_multiple_markets_per_account_allowed(self):
        accounts = self.validate()["accounts"]
        self.assertEqual([len(a["assignments"]) for a in accounts], [2, 1])
        self.assertEqual(accounts[0]["assignments"][0]["condition_id"], accounts[1]["assignments"][0]["condition_id"])
        self.assertNotEqual(accounts[0]["identity"]["account_uid"], accounts[1]["identity"]["account_uid"])
        self.assertEqual(accounts[0]["market_universe_sha256"], accounts[1]["market_universe_sha256"])

    def test_account_universe_and_assignment_identities_cannot_diverge(self):
        for field, value in (("account_uid", self.state["accounts"][1]["identity"]["account_uid"]),
                             ("condition_id", "0x" + "f" * 64), ("assignment_revision", 2)):
            state = copy.deepcopy(self.state)
            self.assignment(state)[field] = value
            self.assert_bad(state)
        self.state["accounts"][1]["market_universe_sha256"] = "0" * 64
        self.assert_bad(self.state, "identity_mismatch")

    def test_universe_hash_reuses_roster_algorithm(self):
        self.state["universe"]["markets"].reverse()
        self.validate()
        self.state["universe"]["markets"][0]["token_id"] = "999"
        self.assert_bad(self.state, "universe_hash_mismatch")

    def test_duplicate_market_or_token_rejected_even_with_updated_hash(self):
        self.state["universe"]["markets"][1]["paired_token_id"] = "101"
        self.state["group"]["market_universe_sha256"] = market_universe_sha256(self.state["universe"])
        self.assert_bad(self.state, "duplicate_market")

    def test_duplicate_assignment_rejected(self):
        self.state["accounts"][0]["assignments"].append(copy.deepcopy(self.assignment()))
        self.assert_bad(self.state, "duplicate_assignment")

    def test_money_must_be_decimal_string_not_unknown_or_float(self):
        for value in (0, 0.0, True, None, "NaN", "Infinity", "1e2", "", "01", "-0", "-1", " 100 "):
            with self.subTest(value=value):
                state = copy.deepcopy(self.state)
                state["accounts"][0]["budget"]["cash_usdc"]["current"] = value
                self.assert_bad(state)

    def test_budget_tiers_are_fixed_strings(self):
        for value in ("50", "100.00", "300", 100, True):
            state = copy.deepcopy(self.state)
            state["accounts"][0]["budget"]["tier_usdc"] = value
            self.assert_bad(state)

    def test_missing_money_is_not_zero_and_cannot_produce_headroom(self):
        b = self.state["accounts"][0]["budget"]
        b["unknown_buy_reserved_usdc"] = metric()
        self.assert_bad(self.state, "unknown_budget")
        for field in BUDGET_DERIVED:
            b[field] = metric()
        self.validate()
        b["unknown_buy_reserved_usdc"]["current"] = "0"
        self.assert_bad(self.state, "unknown_money")

    def test_budget_report_arithmetic_includes_inventory_pending_unknown_fees(self):
        b = self.state["accounts"][0]["budget"]
        for field, value in {"inventory_cost_usdc": "21", "remaining_buy_usdc": "49",
                             "pending_buy_reserved_usdc": "10", "unknown_buy_reserved_usdc": "20",
                             "fee_reserve_usdc": "1", "occupied_usdc": "101", "available_usdc": "99"}.items():
            b[field] = metric(value)
        self.validate()
        b["available_usdc"] = metric("119")
        self.assert_bad(self.state, "budget_arithmetic_mismatch")

    def test_decimal_context_does_not_change_quote_validation(self):
        with localcontext() as context:
            context.prec = 2
            self.validate()
        self.assignment()["quote"]["notional_usdc"] = "100"
        self.assert_bad(self.state, "notional_mismatch")

    def test_quote_prices_and_integer_shares_validated(self):
        for field, value in (("yes_price", "1"), ("no_price", "0"), ("yes_shares", "100.5"),
                             ("yes_shares", "0"), ("yes_shares", 100), ("yes_price", "0.95")):
            state = copy.deepcopy(self.state)
            self.assignment(state)["quote"][field] = value
            self.assert_bad(state)

    def test_freshness_never_refreshed_by_validation(self):
        original = copy.deepcopy(self.state)
        self.validate()
        self.assertEqual(self.state, original)
        with self.assertRaises(SmallCapContractError):
            validate_state(self.state, now="2026-09-09T02:10:00Z")
        stale = synthetic_state("stale")
        self.assertIsNone(stale["accounts"][0]["budget"]["cash_usdc"]["current"])
        self.assertEqual(stale["accounts"][0]["budget"]["cash_usdc"]["last_known"], "200")

    def test_false_fresh_future_and_invalid_time_rejected(self):
        for field, value in (("age_sec", 0), ("observed_at", "2026-09-09T02:00:01Z"),
                             ("observed_at", "2026-02-30T01:00:00Z"), ("observed_at", "2026-09-09T02:00:00+08:00")):
            state = synthetic_state("stale")
            state["freshness"][field] = value
            self.assert_bad(state)

    def test_old_book_event_and_ws_silence_with_fresh_complete_rest_allowed(self):
        book = self.assignment()["books"][0]
        self.assertEqual(book["ws_received"]["status"], "stale")
        self.assertEqual(book["usable_source"], "rest")
        self.validate()

    def test_one_token_rest_success_cannot_refresh_other_token(self):
        books = self.assignment()["books"]
        books[1]["rest_fetched"] = stamp(31, ttl=30)
        self.assert_bad(self.state, "stale_book")
        books[1]["usable_source"] = "none"
        self.validate()
        self.assertEqual(books[0]["rest_fetched"]["age_sec"], 0)

    def test_incomplete_rest_or_stale_ws_cannot_be_usable(self):
        for update in ({"rest_complete": False}, {"usable_source": "ws"}):
            state = copy.deepcopy(self.state)
            self.assignment(state)["books"][0].update(update)
            self.assert_bad(state, "stale_book")

    def test_external_depth_requires_all_own_orders_excluded(self):
        book = self.assignment()["books"][0]
        book["external_front_depth_usdc"] = metric("100")
        self.assert_bad(self.state, "external_depth_unverified")
        book["own_orders_scope_complete"] = True
        self.validate()

    def test_fresh_scoring_does_not_make_stale_percentage_valid(self):
        self.state["accounts"][0]["assignment_effective_at"] = "2026-09-09T01:49:00Z"
        scoring = self.assignment()["scoring"]
        scoring["samples"][-1]["percentage"] = metric("0.05", age=600)
        self.assert_bad(self.state)
        scoring.update(status="stale", independent_valid_count=0)
        # Preserve source ordering while invalidating only percentage freshness.
        for sample in scoring["samples"][:-1]:
            sample["percentage"] = metric()
        self.validate()

    def test_repeated_cache_cannot_be_independent_even_with_new_id(self):
        for field in ("sample_id", "source_snapshot_id", "scoring_freshness", "percentage", "q_min"):
            with self.subTest(field=field):
                state = copy.deepcopy(self.state)
                samples = self.assignment(state)["scoring"]["samples"]
                samples[-1][field] = copy.deepcopy(samples[-2][field])
                self.assert_bad(state, "duplicate_evidence")

    def test_source_order_checked_even_if_other_source_missing(self):
        samples = self.assignment()["scoring"]["samples"]
        samples[1]["percentage"] = metric()
        samples[2]["scoring_freshness"] = copy.deepcopy(samples[1]["scoring_freshness"])
        self.assert_bad(self.state, "duplicate_evidence")

    def test_stale_percentage_still_requires_valid_fraction(self):
        state = synthetic_state("stale")
        self.assignment(state)["scoring"]["samples"][0]["percentage"]["last_known"] = "50"
        self.assert_bad(state, "metric_out_of_range")

    def test_scoring_invalid_sample_resets_consecutive_count(self):
        scoring = self.assignment()["scoring"]
        scoring["samples"][1]["scoring"] = False
        self.assert_bad(self.state, "evidence_count_mismatch")
        scoring.update(status="unverified", independent_valid_count=1)
        self.validate()

    def test_q_zero_or_zero_share_does_not_count(self):
        for field in ("q_min", "percentage"):
            state = copy.deepcopy(self.state)
            scoring = self.assignment(state)["scoring"]
            scoring["samples"][-1][field]["current"] = "0"
            self.assert_bad(state, "evidence_count_mismatch")
            scoring.update(status="unverified", independent_valid_count=0)
            self.validate(state)

    def test_reallocation_requires_new_revision_scoring_evidence(self):
        self.assignment()["scoring"]["samples"][0]["assignment_revision"] = 0
        self.assert_bad(self.state, "revision_mismatch")

    def test_three_synthetic_samples_do_not_enable_live_or_expansion(self):
        assignment = self.assignment()
        self.assertEqual(assignment["scoring"]["independent_valid_count"], 3)
        self.assertFalse(assignment["economics"]["expansion_allowed"])
        assignment["economics"]["expansion_allowed"] = True
        self.assert_bad(self.state, "live_capability_forbidden")

    def test_unknown_economics_cannot_be_reported_as_zero_cost_profit(self):
        economic = self.assignment()["economics"]
        economic["net_increment_usdc"] = metric("0")
        self.assert_bad(self.state, "unknown_economics")
        economic["net_increment_usdc"] = metric()
        economic["missing_reasons"] = []
        self.assert_bad(self.state, "unknown_economics")

    def test_shadow_economic_report_checks_arithmetic_not_execution(self):
        e = self.assignment()["economics"]
        e.update(status="shadow_only", history_calibration="synthetic_only", missing_reasons=[],
                 horizon_end="2026-09-10T02:00:00Z")
        for key, value in {"lower_reward_increment_usdc": "4", "scoring_uptime": "0.8",
                           "yes_exit_stress_usdc": "1", "no_exit_stress_usdc": "2", "verified_history_stress_usdc": "1.5",
                           "fees_usdc": "0.1", "switching_cost_usdc": "0.2", "net_increment_usdc": "0.9"}.items():
            e[key] = metric(value)
        self.validate()
        self.assertFalse(e["expansion_allowed"])
        e["net_increment_usdc"] = metric("3.9")
        self.assert_bad(self.state, "economics_arithmetic_mismatch")

    def test_economic_horizon_expires_before_metric_ttl_at_consumer(self):
        e = self.assignment()["economics"]
        e.update(status="shadow_only", history_calibration="synthetic_only", missing_reasons=[],
                 horizon_end="2026-09-09T02:00:10Z")
        for key, value in {"lower_reward_increment_usdc": "4", "scoring_uptime": "0.8",
                           "yes_exit_stress_usdc": "1", "no_exit_stress_usdc": "2",
                           "verified_history_stress_usdc": "1.5", "fees_usdc": "0.1",
                           "switching_cost_usdc": "0.2", "net_increment_usdc": "0.9"}.items():
            e[key] = metric(value)
        original = copy.deepcopy(self.state)
        self.assertEqual(validate_state(self.state, now="2026-09-09T02:00:09.999999Z"), original)
        command = synthetic_command(self.state, action="resume_account", now=FIXTURE_TIME)
        for now in ("2026-09-09T02:00:10Z", "2026-09-09T02:00:10.000001Z",
                    "2026-09-09T02:00:11Z"):
            with self.subTest(now=now):
                with self.assertRaisesRegex(SmallCapContractError, "horizon_expired"):
                    validate_state(self.state, now=now)
                with self.assertRaisesRegex(SmallCapContractError, "horizon_expired"):
                    validate_command(command, self.state, now=now)
        self.assertEqual(self.state, original)

    def test_cancel_latency_ends_at_confirmation_not_request(self):
        c = self.assignment()["cancellation"]
        c.update(status="synthetic_confirmed", protection_triggered_at="2026-09-09T01:59:50Z",
                 cancel_requested_at="2026-09-09T01:59:52Z", cancel_confirmed_at="2026-09-09T01:59:55Z",
                 confirmation_latency_ms=5000, freshness=stamp())
        self.validate()
        c["confirmation_latency_ms"] = 2000
        self.assert_bad(self.state)

    def test_subsecond_cancel_latency_and_stale_book_boundary(self):
        c = self.assignment()["cancellation"]
        c.update(status="synthetic_confirmed", protection_triggered_at="2026-09-09T01:59:50.001Z",
                 cancel_requested_at="2026-09-09T01:59:50.004Z", cancel_confirmed_at="2026-09-09T01:59:50.026Z",
                 confirmation_latency_ms=25, freshness=stamp())
        self.validate()
        book = self.assignment()["books"][0]
        book["rest_fetched"].update(observed_at="2026-09-09T01:59:29.999Z", age_sec=31)
        self.assert_bad(self.state, "freshness_mismatch")

    def test_advancing_revision_cannot_relabel_old_evidence(self):
        changed = self.revision_two_state()
        a = changed["accounts"][0]
        for assignment in a["assignments"]:
            for i, sample in enumerate(assignment["scoring"]["samples"]):
                sample["sample_id"] = f"sample-{i}"
        validate_state(changed, now=changed["generated_at"])
        with self.assertRaisesRegex(SmallCapContractError, "evidence_from_prior_revision"):
            self.transition(changed)

    def test_unconfirmed_cancel_has_no_claimed_latency(self):
        c = self.assignment()["cancellation"]
        c.update(status="pending", protection_triggered_at="2026-09-09T01:59:50Z",
                 cancel_requested_at="2026-09-09T01:59:52Z", freshness=stamp())
        self.validate()
        c["confirmation_latency_ms"] = 0
        self.assert_bad(self.state)

    def test_accounting_types_dates_and_maker_deduplication(self):
        rows = self.state["accounts"][0]["accounting"]
        self.assertEqual([r["accounting_type"] for r in rows], list(ACCOUNTING_TYPES))
        rows[3]["amount_usdc"] = metric("-1.25")
        self.validate()
        rows.append(copy.deepcopy(rows[0]))
        self.assert_bad(self.state, "duplicate_accounting_record")

    def test_unknown_total_not_synthesized_from_partial_records(self):
        rows = self.state["accounts"][0]["accounting"]
        rows[0]["amount_usdc"] = metric("5")
        result = self.validate()
        self.assertFalse(result["accounting_complete"])
        self.assertNotIn("total", result)
        self.state["accounts"][0]["accounting"][0]["accounting_type"] = "net_profit"
        self.assert_bad(self.state)

    def test_bjt_0800_business_day_boundary(self):
        self.assertEqual(business_day("2026-09-08T23:59:59Z"), "2026-09-08")
        self.assertEqual(business_day("2026-09-09T00:00:00Z"), "2026-09-09")
        self.assertEqual(business_day("2026-09-09T16:00:00Z"), "2026-09-09")

    def test_transition_is_idempotent_and_fixed_host(self):
        self.assertEqual(validate_transition(self.state, self.state, previous_now=FIXTURE_TIME, now=FIXTURE_TIME), self.state)
        changed = copy.deepcopy(self.state)
        changed["accounts"][0]["identity"]["host_id"] = "other-host"
        with self.assertRaisesRegex(SmallCapContractError, "fixed_ownership_mismatch"):
            validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=FIXTURE_TIME)

    def test_unavailable_report_is_not_an_empty_authoritative_roster(self):
        with self.assertRaisesRegex(SmallCapContractError, "state_not_available"):
            validate_transition(self.state, synthetic_state("unavailable"), previous_now=FIXTURE_TIME, now=FIXTURE_TIME)

    def test_assignment_changes_require_revision_advance_but_samples_do_not(self):
        changed = copy.deepcopy(self.state)
        changed["accounts"][0]["assignments"].pop()
        with self.assertRaisesRegex(SmallCapContractError, "revision_not_advanced"):
            validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=FIXTURE_TIME)
        changed = self.revision_two_state()
        a = changed["accounts"][0]
        a["assignments"].pop()
        for assignment in a["assignments"]:
            assignment["assignment_revision"] = 2
            assignment["scoring"] = {"status": "unverified", "samples": [], "independent_valid_count": 0}
        self.transition(changed)
        changed = copy.deepcopy(self.state)
        self.assignment(changed)["scoring"]["samples"][-1]["q_min"]["current"] = "2"
        validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=FIXTURE_TIME)

    def test_roster_or_account_change_requires_group_revision(self):
        changed = copy.deepcopy(self.state)
        changed["accounts"].pop()
        with self.assertRaisesRegex(SmallCapContractError, "revision_not_advanced"):
            validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=FIXTURE_TIME)

    def test_commands_all_rejected_locally_not_dispatched(self):
        for action in COMMANDS:
            with self.subTest(action=action):
                command = synthetic_command(self.state, action=action)
                original = copy.deepcopy(command)
                receipt = reject_command(command, self.state, now=FIXTURE_TIME)
                self.assertEqual(command, original)
                self.assertEqual(receipt, reject_command(command, self.state, now=FIXTURE_TIME))
                self.assertEqual(receipt["status"], "rejected")
                self.assertEqual(receipt["reason"], "runtime_not_deployed")
                self.assertFalse(receipt["dispatched"])
                self.assertIsNone(receipt["result"])
                self.assertEqual(receipt["runtime_receipt_freshness"]["status"], "unavailable")
                validate_receipt(receipt, command, now=FIXTURE_TIME)

    def test_commands_reject_identity_revision_and_extra_trade_payload(self):
        for field, value in (("host_id", "another-host"), ("account_index", 30), ("account_uid_key", "0" * 16),
                             ("group_revision", 2), ("expected_assignment_revision", 0),
                             ("market_universe_sha256", "0" * 64), ("schema_version", 2),
                             ("payload", {"token_id": "101"}), ("action", "cancel_all"),
                             ("command_id", "not-a-uuid"), ("idempotency_key", True)):
            command = synthetic_command(self.state)
            command[field] = value
            with self.subTest(field=field), self.assertRaises(SmallCapContractError):
                validate_command(command, self.state, now=FIXTURE_TIME)

    def test_expired_or_future_commands_rejected(self):
        command = synthetic_command(self.state)
        for now in ("2026-09-09T01:59:59Z", "2026-09-09T02:02:00Z"):
            with self.assertRaises(SmallCapContractError):
                validate_command(command, self.state, now=now)

    def test_stale_report_cannot_validate_command(self):
        command = synthetic_command(self.state)
        with self.assertRaisesRegex(SmallCapContractError, "state_not_fresh"):
            validate_command(command, synthetic_state("stale"), now=FIXTURE_TIME)

    def test_receipt_success_identity_or_applied_revision_rejected(self):
        command = synthetic_command(self.state)
        for field, value in (("status", "success"), ("dispatched", True), ("result", {}),
                             ("applied_assignment_revision", 2), ("command_sha256", "0" * 64)):
            receipt = reject_command(command, self.state, now=FIXTURE_TIME)
            receipt[field] = value
            with self.assertRaises(SmallCapContractError):
                validate_receipt(receipt, command, now=FIXTURE_TIME)
        receipt = reject_command(command, self.state, now=FIXTURE_TIME)
        receipt["command"]["host_id"] = "wrong-host"
        with self.assertRaisesRegex(SmallCapContractError, "receipt_identity_mismatch"):
            validate_receipt(receipt, command, now=FIXTURE_TIME)

    def test_receipt_embedded_command_cannot_coerce_schema_to_bool(self):
        command = synthetic_command(self.state)
        receipt = reject_command(command, self.state, now=FIXTURE_TIME)
        receipt["command"]["schema_version"] = True
        with self.assertRaises(SmallCapContractError):
            validate_receipt(receipt, command, now=FIXTURE_TIME)

    def test_synthetic_pending_receipt_goes_unknown_when_stale(self):
        command = synthetic_command(self.state)
        receipt = reject_command(command, self.state, now=FIXTURE_TIME)
        receipt.update(source="synthetic_fixture", status="pending", reason="awaiting_receipt")
        validate_receipt(receipt, command, now=FIXTURE_TIME)
        receipt["freshness"].update(status="stale", age_sec=600, reason="sample_expired")
        receipt["generated_at"] = "2026-09-09T02:10:00Z"
        with self.assertRaisesRegex(SmallCapContractError, "stale_receipt"):
            validate_receipt(receipt, command, now="2026-09-09T02:10:00Z")
        receipt.update(status="unknown", last_known_status="pending", reason="receipt_stale")
        validate_receipt(receipt, command, now="2026-09-09T02:10:00Z")

    def test_old_local_rejection_is_last_known_not_current_runtime_status(self):
        command = synthetic_command(self.state)
        receipt = reject_command(command, self.state, now=FIXTURE_TIME)
        receipt["freshness"].update(status="stale", age_sec=600, reason="sample_expired")
        receipt["generated_at"] = "2026-09-09T02:10:00Z"
        receipt.update(status="unknown", last_known_status="rejected", reason="receipt_stale")
        validate_receipt(receipt, command, now="2026-09-09T02:10:00Z")

    def test_review_unchanged_payload_valid_after_one_millisecond(self):
        command = synthetic_command(self.state)
        receipt = reject_command(command, self.state, now=FIXTURE_TIME)
        later = "2026-09-09T02:00:00.001Z"
        for label, validate, args in (
            ("state", validate_state, (self.state,)),
            ("command", validate_command, (command, self.state)),
            ("receipt", validate_receipt, (receipt, command)),
        ):
            with self.subTest(label=label):
                original = copy.deepcopy(args)
                validate(*args, now=later)
                self.assertEqual(args, original)

    def test_review_new_ids_cannot_validate_older_revision_evidence(self):
        changed = copy.deepcopy(self.state)
        account = changed["accounts"][0]
        account["assignment_revision"] = 2
        for assignment in account["assignments"]:
            assignment["assignment_revision"] = 2
            for i, sample in enumerate(assignment["scoring"]["samples"]):
                sample.update(assignment_revision=2, sample_id=f"older-{i}", source_snapshot_id=f"older-source-{i}",
                              scoring_freshness=stamp(150-i*10), percentage=metric("0.05", age=150-i*10),
                              q_min=metric("1", age=150-i*10))
        self.validate(changed)
        with self.assertRaisesRegex(SmallCapContractError, "revision_boundary_not_advanced"):
            validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=FIXTURE_TIME)

    def test_review_condition_remap_cannot_reuse_revision_and_scoring(self):
        changed = copy.deepcopy(self.state)
        changed["universe"]["markets"][0]["token_id"] = "999"
        digest = market_universe_sha256(changed["universe"])
        changed["group"].update(revision=2, market_universe_sha256=digest)
        for account in changed["accounts"]:
            account["market_universe_sha256"] = digest
            for assignment in account["assignments"]:
                for book in assignment["books"]:
                    if book["token_id"] == "101":
                        book["token_id"] = "999"
        self.validate(changed)
        with self.assertRaises(SmallCapContractError):
            validate_transition(self.state, changed, previous_now=FIXTURE_TIME, now=FIXTURE_TIME)

    def test_review_clob_66_character_order_ids_are_valid(self):
        for sample in self.assignment()["scoring"]["samples"]:
            sample.update(yes_order_id="0x"+"d"*64, no_order_id="0x"+"e"*64)
        self.validate()

    def test_review_same_chain_maker_with_different_signature_type_rejected(self):
        a = self.state["accounts"][1]
        ident = a["identity"]
        ident["maker_address"] = self.state["accounts"][0]["identity"]["maker_address"]
        ident["signature_type"] = 0
        ident["account_uid"] = f'{ident["chain_id"]}:0:{ident["maker_address"]}'
        ident["account_uid_key"] = hashlib.sha256(ident["account_uid"].encode()).hexdigest()[:16]
        for row in a["assignments"] + a["accounting"]:
            row["account_uid"] = ident["account_uid"]
        self.assert_bad(self.state)

    def test_consumer_delay_does_not_change_serialized_generation_ages(self):
        original = copy.deepcopy(self.state)
        for now in ("2026-09-09T02:00:00.1Z", "2026-09-09T02:00:01Z", "2026-09-09T02:00:30Z"):
            with self.subTest(now=now):
                self.assertEqual(validate_state(self.state, now=now), original)
        with self.assertRaisesRegex(SmallCapContractError, "evidence_expired"):
            validate_state(self.state, now="2026-09-09T02:00:30.000001Z")
        self.assertEqual(self.state, original)

    def test_age_remains_bound_to_generation_not_consumer(self):
        self.state["freshness"]["age_sec"] = 1
        with self.assertRaisesRegex(SmallCapContractError, "invalid_age"):
            validate_state(self.state, now="2026-09-09T02:00:01Z")

    def test_consumer_timestamp_supports_one_through_six_fraction_digits(self):
        for fraction in ("1", "01", "001", "0001", "00001", "000001"):
            with self.subTest(fraction=fraction):
                self.assertEqual(validate_state(self.state, now=f"2026-09-09T02:00:00.{fraction}Z"), self.state)

    def test_effective_freshness_expires_without_rewriting_nested_payload(self):
        book = self.assignment()["books"][0]["rest_fetched"]
        original = copy.deepcopy(book)
        for now, expected in (("2026-09-09T02:00:00.001Z", "fresh"),
                              ("2026-09-09T02:00:30Z", "fresh"),
                              ("2026-09-09T02:00:30.000001Z", "stale")):
            self.assertEqual(freshness_status_at(book, generated_at=FIXTURE_TIME, now=now), expected)
        self.assertEqual(book, original)
        for absent in ("missing", "unavailable"):
            self.assertEqual(freshness_status_at(stamp(absent=absent), generated_at=FIXTURE_TIME,
                                                now="2026-09-09T02:10:00Z"), absent)
        with self.assertRaisesRegex(SmallCapContractError, "future_generation"):
            freshness_status_at(book, generated_at=FIXTURE_TIME, now="2026-09-09T01:59:59.999Z")

    def test_published_stale_values_remain_last_known_at_later_consumption(self):
        state = synthetic_state("stale")
        self.assertEqual(validate_state(state, now="2026-09-09T02:10:00Z"), state)
        self.assertEqual(state["generated_at"], FIXTURE_TIME)
        self.assertEqual(state["freshness"]["observed_at"], "2026-09-09T01:50:00Z")

    def test_command_uses_consumer_time_for_expiry_not_generation_time(self):
        state = synthetic_state("proposed")
        for account in state["accounts"]:
            account["assignments"] = []
        command = synthetic_command(state)
        validate_command(command, state, now="2026-09-09T02:01:59.999Z")
        with self.assertRaisesRegex(SmallCapContractError, "command_expired"):
            validate_command(command, state, now="2026-09-09T02:02:00Z")

    def test_pending_receipt_expires_at_consumer_without_mutation(self):
        command = synthetic_command(self.state)
        receipt = reject_command(command, self.state, now=FIXTURE_TIME)
        receipt.update(source="synthetic_fixture", status="pending", reason="awaiting_receipt")
        original = copy.deepcopy(receipt)
        validate_receipt(receipt, command, now="2026-09-09T02:05:00Z")
        with self.assertRaisesRegex(SmallCapContractError, "evidence_expired"):
            validate_receipt(receipt, command, now="2026-09-09T02:05:00.000001Z")
        self.assertEqual(freshness_status_at(receipt["freshness"], generated_at=receipt["generated_at"],
                                            now="2026-09-09T02:05:00.000001Z"), "stale")
        self.assertEqual(receipt, original)

    def test_new_revision_accepts_three_strictly_post_effective_samples(self):
        changed = self.revision_two_state()
        self.assertEqual(changed["accounts"][0]["assignment_effective_at"], "2026-09-09T02:00:01Z")
        self.assertEqual(self.transition(changed), changed)
        for assignment in changed["accounts"][0]["assignments"]:
            self.assertEqual(assignment["scoring"]["status"], "synthetic_validated")

    def test_each_independent_source_must_be_strictly_after_revision_boundary(self):
        for field in ("scoring_freshness", "percentage", "q_min"):
            for observed, age in (("2026-09-09T02:00:01Z", 180), ("2026-09-09T02:00:00Z", 181)):
                with self.subTest(field=field, observed=observed):
                    changed = self.revision_two_state()
                    sample = self.assignment(changed)["scoring"]["samples"][0]
                    f = sample[field] if field == "scoring_freshness" else sample[field]["freshness"]
                    f.update(observed_at=observed, age_sec=age)
                    with self.assertRaisesRegex(SmallCapContractError, "evidence_before_revision"):
                        validate_state(changed, now=changed["generated_at"])

    def test_future_or_missing_revision_effective_boundary_rejected(self):
        self.state["accounts"][0]["assignment_effective_at"] = "2026-09-09T02:00:00.001Z"
        self.assert_bad(self.state, "future_revision")
        del self.state["accounts"][0]["assignment_effective_at"]
        self.assert_bad(self.state, "invalid_fields")

    def test_revision_effective_boundary_cannot_be_rewritten_or_backdated(self):
        changed = copy.deepcopy(self.state)
        changed["accounts"][0]["assignment_effective_at"] = "2026-09-09T01:57:01Z"
        with self.assertRaisesRegex(SmallCapContractError, "revision_boundary_mismatch"):
            self.transition(changed)
        for effective in ("2026-09-09T01:59:59Z", FIXTURE_TIME):
            changed = self.revision_two_state()
            changed["accounts"][0]["assignment_effective_at"] = effective
            validate_state(changed, now=changed["generated_at"])
            with self.assertRaisesRegex(SmallCapContractError, "revision_boundary_not_advanced"):
                self.transition(changed)

    def test_condition_token_swap_rejected_even_with_new_assignment_revision(self):
        changed = self.revision_two_state()
        market = changed["universe"]["markets"][0]
        market["token_id"], market["paired_token_id"] = market["paired_token_id"], market["token_id"]
        digest = market_universe_sha256(changed["universe"])
        changed["group"].update(revision=2, market_universe_sha256=digest)
        for account in changed["accounts"]:
            account["market_universe_sha256"] = digest
        validate_state(changed, now=changed["generated_at"])
        with self.assertRaisesRegex(SmallCapContractError, "condition_token_remap"):
            self.transition(changed)

    def test_order_references_require_exact_canonical_clob_shape(self):
        for value in ("synthetic-yes", "d"*64, "0x"+"d"*63, "0x"+"d"*65,
                      "0x"+"g"*64, "0x"+"D"*64, "0x"+"d"*64+"\n", None, 123):
            with self.subTest(value=value):
                state = copy.deepcopy(self.state)
                self.assignment(state)["scoring"]["samples"][0]["yes_order_id"] = value
                self.assert_bad(state)
        sample = self.assignment()["scoring"]["samples"][0]
        sample["no_order_id"] = sample["yes_order_id"]
        self.assert_bad(self.state, "duplicate_order")

    def test_same_maker_address_on_other_chain_is_not_duplicate(self):
        a = self.state["accounts"][1]
        ident = a["identity"]
        ident.update(chain_id=138, maker_address=self.state["accounts"][0]["identity"]["maker_address"])
        ident["account_uid"] = f'138:{ident["signature_type"]}:{ident["maker_address"]}'
        ident["account_uid_key"] = hashlib.sha256(ident["account_uid"].encode()).hexdigest()[:16]
        for row in a["assignments"] + a["accounting"]:
            row["account_uid"] = ident["account_uid"]
        self.validate()

    def test_maker_cannot_change_signature_representation_and_index_across_reports(self):
        changed = copy.deepcopy(self.state)
        a = changed["accounts"][0]
        ident = a["identity"]
        ident.update(account_index=21, account_id="fixture-new", signature_type=0)
        ident["account_uid"] = f'{ident["chain_id"]}:0:{ident["maker_address"]}'
        ident["account_uid_key"] = hashlib.sha256(ident["account_uid"].encode()).hexdigest()[:16]
        for row in a["assignments"] + a["accounting"]:
            row["account_uid"] = ident["account_uid"]
        changed["group"]["revision"] = 2
        self.validate(changed)
        with self.assertRaisesRegex(SmallCapContractError, "fixed_ownership_mismatch"):
            self.transition(changed)


if __name__ == "__main__":
    unittest.main()
