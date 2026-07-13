from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from core.maker_shadow_export import (
    export_polymarket_snapshot,
    export_predictfun_snapshot,
)
from core.maker_shadow_reference import evaluate_case


NOW = datetime(2026, 7, 13, 8, 0, 1, tzinfo=timezone.utc)


class MakerShadowExportTests(unittest.TestCase):
    def test_predictfun_state_exports_desired_actual_and_book_age(self) -> None:
        snapshot = export_predictfun_snapshot(
            intents_state={
                "ts": "2026-07-13T08:00:00Z",
                "intents": [
                    {
                        "intent_id": "pf-a:42:YES:BUY:l0",
                        "account_id": "pf-a",
                        "market_id": 42,
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.48",
                        "size": "20",
                    }
                ],
            },
            actual_state={
                "ts": "2026-07-13T08:00:00Z",
                "active_orders": [
                    {
                        "intent_id": "pf-a:42:YES:BUY:l0",
                        "account_id": "pf-a",
                        "market_id": 42,
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.48",
                        "size": "20",
                    }
                ]
            },
            plans_state={"ts": "2026-07-13T08:00:00Z"},
            now=NOW,
        )

        self.assertEqual(len(snapshot["desired"]), 1)
        self.assertEqual(len(snapshot["actual"]), 1)
        self.assertEqual(snapshot["books"][0]["age_ms"], 1000)
        result = evaluate_case(snapshot)
        self.assertTrue(result["can_execute"])
        self.assertEqual(result["plan"]["actions"][0]["action"], "keep")

    def test_polymarket_states_combine_accounts_and_preserve_exit_orders(self) -> None:
        state_one = {
            "ts": "2026-07-13T08:00:00Z",
            "account_index": 1,
            "markets": {
                "token-yes": {
                    "condition_id": "condition-1",
                    "desired_plan_sig": "0.48:25|0.47:20",
                    "snapshot_age_ms": 100,
                    "orders": [
                        {
                            "id": "maker-1",
                            "price_raw": "0.48",
                            "size_raw": "25",
                            "side": "buy",
                        },
                        {
                            "id": "exit-1",
                            "price_raw": "0.51",
                            "size_raw": "2",
                            "side": "sell",
                            "is_exit": True,
                        },
                    ],
                }
            },
        }
        state_two = {
            "ts": "2026-07-13T08:00:00Z",
            "account_index": 2,
            "markets": {
                "token-yes": {
                    "condition_id": "condition-1",
                    "desired_plan_sig": "0.46:10",
                    "snapshot_age_ms": 200,
                    "orders": [
                        {
                            "id": "maker-2",
                            "price_raw": "0.46",
                            "size_raw": "10",
                            "side": "buy",
                        }
                    ],
                }
            },
        }

        snapshot = export_polymarket_snapshot(
            engine_states=[state_one, state_two],
            now=NOW,
        )

        self.assertEqual(len(snapshot["desired"]), 3)
        self.assertEqual(len(snapshot["actual"]), 3)
        self.assertEqual(snapshot["books"][0]["age_ms"], 1200)
        exit_order = next(row for row in snapshot["actual"] if row["order_id"] == "exit-1")
        self.assertNotIn("managed_slot", exit_order)
        self.assertEqual(snapshot["metadata"]["accounts"], 2)

        result = evaluate_case(snapshot)
        self.assertTrue(result["can_execute"])
        actions = [row["action"] for row in result["plan"]["actions"]]
        self.assertEqual(actions.count("keep"), 2)
        self.assertEqual(actions.count("create"), 1)
        self.assertEqual(result["plan"]["unmanaged_order_ids"], ["exit-1"])

    def test_missing_timestamp_keeps_book_freshness_unknown(self) -> None:
        snapshot = export_predictfun_snapshot(
            intents_state={
                "intents": [
                    {
                        "intent_id": "slot-1",
                        "account_id": "pf-a",
                        "market_id": 42,
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.48",
                        "size": "20",
                    }
                ]
            },
            actual_state={},
            plans_state={},
            now=NOW,
        )

        self.assertEqual(snapshot["books"], [])
        result = evaluate_case(snapshot)
        self.assertFalse(result["can_execute"])
        self.assertEqual(result["risk"]["violations"][0]["code"], "missing_book_age")

    def test_stale_predictfun_actual_state_blocks_a_fresh_plan(self) -> None:
        snapshot = export_predictfun_snapshot(
            intents_state={
                "ts": "2026-07-13T08:00:00Z",
                "intents": [
                    {
                        "intent_id": "slot-1",
                        "account_id": "pf-a",
                        "market_id": 42,
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.48",
                        "size": "20",
                    }
                ],
            },
            actual_state={
                "ts": "2026-07-13T07:59:00Z",
                "active_orders": [
                    {
                        "intent_id": "slot-1",
                        "account_id": "pf-a",
                        "market_id": 42,
                        "outcome": "YES",
                        "side": "BUY",
                        "price": "0.48",
                        "size": "20",
                    }
                ],
            },
            plans_state={"ts": "2026-07-13T08:00:00Z"},
            now=NOW,
        )

        self.assertEqual(snapshot["books"][0]["age_ms"], 61000)
        result = evaluate_case(snapshot)
        self.assertFalse(result["can_execute"])
        self.assertEqual(result["risk"]["violations"][0]["code"], "stale_book")

    def test_legacy_polymarket_state_never_marks_orders_as_managed(self) -> None:
        snapshot = export_polymarket_snapshot(
            engine_states=[
                {
                    "account_index": 1,
                    "markets": {
                        "token-yes": {
                            "orders": [{"id": "legacy-1", "price": 0.48, "size": 25}]
                        }
                    },
                }
            ],
            now=NOW,
        )

        self.assertEqual(snapshot["desired"], [])
        self.assertNotIn("managed_slot", snapshot["actual"][0])
        result = evaluate_case(snapshot)
        self.assertEqual(result["plan"]["actions"], [])
        self.assertEqual(result["plan"]["unmanaged_order_ids"], ["legacy-1"])

    def test_predictfun_cli_writes_a_rust_compatible_snapshot(self) -> None:
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            intents = temporary / "intents.json"
            actual = temporary / "actual.json"
            plans = temporary / "plans.json"
            output = temporary / "shadow.json"
            intents.write_text(
                json.dumps(
                    {
                        "ts": "2026-07-13T08:00:00Z",
                        "intents": [
                            {
                                "intent_id": "slot-1",
                                "account_id": "pf-a",
                                "market_id": 42,
                                "outcome": "YES",
                                "side": "BUY",
                                "price": "0.48",
                                "size": "20",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            actual.write_text(json.dumps({"active_orders": []}), encoding="utf-8")
            plans.write_text(
                json.dumps({"ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}),
                encoding="utf-8",
            )

            completed = subprocess.run(
                [
                    sys.executable,
                    str(root / "scripts" / "maker_shadow_export.py"),
                    "--output",
                    str(output),
                    "predictfun",
                    "--intents",
                    str(intents),
                    "--actual",
                    str(actual),
                    "--plans",
                    str(plans),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            snapshot = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(snapshot["metadata"]["mode"], "read_only_shadow")
            self.assertEqual(snapshot["desired"][0]["venue"], "predict_fun")

            rust_binary = root / "rust-maker" / "target" / "debug" / "maker-dry-run"
            if rust_binary.exists():
                rust_result = subprocess.run(
                    [str(rust_binary), str(output)],
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(rust_result.returncode, 0, rust_result.stderr)
                rust_output = json.loads(rust_result.stdout)
                self.assertIn("can_execute", rust_output)


if __name__ == "__main__":
    unittest.main()
