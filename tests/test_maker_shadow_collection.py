from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from core.maker_shadow_collection import (
    record_comparison,
    record_error,
    source_fingerprint,
    summary,
)
from core.maker_shadow_compare import comparison_result


class MakerShadowCollectionTests(unittest.TestCase):
    def test_collector_systemd_units_are_network_denied(self) -> None:
        root = Path(__file__).resolve().parents[1]
        for name in (
            "maker-shadow-predictfun.service",
            "maker-shadow-polymarket.service",
        ):
            unit = (root / "deploy" / "systemd" / name).read_text(encoding="utf-8")
            self.assertIn("IPAddressDeny=any", unit)
            self.assertIn("RestrictAddressFamilies=AF_UNIX", unit)
            self.assertIn("--run-seconds 86400", unit)

    def test_polymarket_public_observer_is_separate_from_network_denied_collector(self) -> None:
        root = Path(__file__).resolve().parents[1]
        observer = (
            root / "deploy" / "systemd" / "polymarket-readonly-observer.service"
        ).read_text(encoding="utf-8")
        collector = (
            root / "deploy" / "systemd" / "maker-shadow-polymarket.service"
        ).read_text(encoding="utf-8")

        self.assertIn("read_only_observer", observer)
        self.assertIn("config_1.json", observer)
        self.assertIn("config_2.json", observer)
        self.assertNotIn("signer", observer.lower())
        self.assertNotIn("IPAddressDeny=any", observer)
        self.assertIn("polymarket_observer_state_1.json", collector)
        self.assertIn("polymarket_observer_state_2.json", collector)
        self.assertIn("--interval-seconds 2", collector)
        self.assertNotIn("data/engine_state_1.json", collector)

    def test_predictfun_timer_runs_the_dry_simulation_runner(self) -> None:
        root = Path(__file__).resolve().parents[1]
        unit = (
            root / "deploy" / "systemd" / "predictfun-dryrun.service"
        ).read_text(encoding="utf-8")
        self.assertIn("platforms.predictfun.maker.runner", unit)
        self.assertIn("--once", unit)
        self.assertNotIn("live_executor", unit.lower())

    def test_source_fingerprint_is_stable_across_input_order(self) -> None:
        forward = source_fingerprint([("a", b"one"), ("b", b"two")])
        reverse = source_fingerprint([("b", b"two"), ("a", b"one")])
        changed = source_fingerprint([("a", b"one"), ("b", b"three")])

        self.assertEqual(forward, reverse)
        self.assertNotEqual(forward, changed)

    def test_comparison_result_classifies_safety_and_action_differences(self) -> None:
        python = _canonical(can_execute=True, actions=[("keep",)])
        rust_action_difference = _canonical(can_execute=True, actions=[("create",)])
        action_result = comparison_result(
            case_name="action.json",
            python_canonical=python,
            rust_canonical=rust_action_difference,
        )
        self.assertFalse(action_result["matched"])
        self.assertTrue(action_result["safety_matched"])
        self.assertFalse(action_result["actions_matched"])

        rust_safety_difference = _canonical(can_execute=False, actions=[])
        safety_result = comparison_result(
            case_name="safety.json",
            python_canonical=python,
            rust_canonical=rust_safety_difference,
        )
        self.assertFalse(safety_result["safety_matched"])

    def test_database_deduplicates_states_and_reports_rates(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            database = temporary / "shadow.sqlite3"
            snapshot_path = temporary / "sample.json"
            snapshot_path.write_text("{}", encoding="utf-8")
            snapshot = _snapshot()
            matched = comparison_result(
                case_name="sample.json",
                python_canonical=_canonical(can_execute=True, actions=[("keep",)]),
                rust_canonical=_canonical(can_execute=True, actions=[("keep",)]),
            )
            inserted = record_comparison(
                database=database,
                venue="predictfun",
                fingerprint="a" * 64,
                snapshot=snapshot,
                comparison=matched,
                snapshot_path=snapshot_path,
            )
            duplicate = record_comparison(
                database=database,
                venue="predictfun",
                fingerprint="a" * 64,
                snapshot=snapshot,
                comparison=matched,
                snapshot_path=snapshot_path,
            )

            mismatch = comparison_result(
                case_name="sample-2.json",
                python_canonical=_canonical(can_execute=True, actions=[("keep",)]),
                rust_canonical=_canonical(can_execute=False, actions=[]),
            )
            record_comparison(
                database=database,
                venue="predictfun",
                fingerprint="b" * 64,
                snapshot=snapshot,
                comparison=mismatch,
                snapshot_path=snapshot_path,
            )
            record_error(database=database, venue="predictfun", error="temporary read error")

            report = summary(database)
            row = report["venues"][0]
            self.assertTrue(inserted)
            self.assertFalse(duplicate)
            self.assertEqual(row["samples"], 2)
            self.assertEqual(row["fresh_samples"], 2)
            self.assertEqual(row["mismatched_samples"], 1)
            self.assertEqual(row["difference_rate"], 0.5)
            self.assertEqual(row["safety_mismatches"], 1)
            self.assertEqual(row["errors"], 1)

    def test_one_shot_collector_and_report(self) -> None:
        root = Path(__file__).resolve().parents[1]
        rust_binary = root / "rust-maker" / "target" / "debug" / "maker-dry-run"
        if not rust_binary.exists():
            self.skipTest("Rust maker binary is not built")

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            intents = temporary / "intents.json"
            actual = temporary / "actual.json"
            plans = temporary / "plans.json"
            database = temporary / "shadow.sqlite3"
            snapshots = temporary / "snapshots"
            timestamp = "2099-01-01T00:00:00Z"
            intents.write_text(
                json.dumps(
                    {
                        "ts": timestamp,
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
            actual.write_text(
                json.dumps({"ts": timestamp, "active_orders": []}),
                encoding="utf-8",
            )
            plans.write_text(json.dumps({"ts": timestamp}), encoding="utf-8")

            collect = subprocess.run(
                [
                    sys.executable,
                    str(root / "scripts" / "maker_shadow_collect.py"),
                    "--database",
                    str(database),
                    "--snapshot-dir",
                    str(snapshots),
                    "--rust-bin",
                    str(rust_binary),
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
            self.assertEqual(collect.returncode, 0, collect.stderr)
            self.assertIn("MATCH NEW predictfun", collect.stdout)

            report = subprocess.run(
                [
                    sys.executable,
                    str(root / "scripts" / "maker_shadow_report.py"),
                    "--database",
                    str(database),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(report.returncode, 0, report.stderr)
            self.assertIn("difference=0.0000%", report.stdout)

    def test_one_shot_polymarket_collector_accepts_multiple_states(self) -> None:
        root = Path(__file__).resolve().parents[1]
        rust_binary = root / "rust-maker" / "target" / "debug" / "maker-dry-run"
        if not rust_binary.exists():
            self.skipTest("Rust maker binary is not built")

        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            database = temporary / "shadow.sqlite3"
            snapshots = temporary / "snapshots"
            state_paths = []
            for account_index in (1, 2):
                path = temporary / f"engine_state_{account_index}.json"
                path.write_text(
                    json.dumps(
                        {
                            "ts": "2099-01-01T00:00:00Z",
                            "account_index": account_index,
                            "markets": {
                                "token-yes": {
                                    "condition_id": "condition-1",
                                    "desired_plan_sig": "0.48:20",
                                    "snapshot_age_ms": 100,
                                    "orders": [],
                                }
                            },
                        }
                    ),
                    encoding="utf-8",
                )
                state_paths.append(path)

            command = [
                sys.executable,
                str(root / "scripts" / "maker_shadow_collect.py"),
                "--database",
                str(database),
                "--snapshot-dir",
                str(snapshots),
                "--rust-bin",
                str(rust_binary),
                "polymarket",
            ]
            for path in state_paths:
                command.extend(("--state", str(path)))
            collect = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(collect.returncode, 0, collect.stderr)
            self.assertIn("MATCH NEW polymarket", collect.stdout)
            report = summary(database, venue="polymarket")
            self.assertEqual(report["venues"][0]["samples"], 1)
            self.assertEqual(report["venues"][0]["fresh_samples"], 1)


def _canonical(*, can_execute: bool, actions: list[tuple[str, ...]]) -> dict:
    return {
        "can_execute": can_execute,
        "risk_allowed": can_execute,
        "risk_violations": [],
        "actions": actions,
        "unmanaged_order_ids": [],
        "warnings": [],
        "error": "",
    }


def _snapshot() -> dict:
    return {
        "metadata": {"source_ts": "2026-07-13T00:00:00Z"},
        "desired": [{"slot_id": "slot-1"}],
        "actual": [],
        "books": [{"age_ms": 100}],
    }


if __name__ == "__main__":
    unittest.main()
