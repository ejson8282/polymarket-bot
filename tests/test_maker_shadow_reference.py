from __future__ import annotations

import json
import unittest
from pathlib import Path

from core.maker_shadow_reference import canonical_result, evaluate_case


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "rust-maker" / "fixtures"


class MakerShadowReferenceTests(unittest.TestCase):
    def load_case(self, name: str) -> dict:
        return json.loads((FIXTURES / name).read_text(encoding="utf-8"))

    def test_shared_plan_replaces_and_creates_without_touching_manual_order(self) -> None:
        result = evaluate_case(self.load_case("shared_plan.json"))
        self.assertTrue(result["can_execute"])
        actions = result["plan"]["actions"]
        self.assertEqual({action["action"] for action in actions}, {"create", "replace"})
        self.assertEqual(result["plan"]["unmanaged_order_ids"], ["external-order"])

    def test_cross_account_case_is_blocked(self) -> None:
        result = evaluate_case(self.load_case("risk_blocked_cross_account.json"))
        self.assertFalse(result["can_execute"])
        self.assertIn(
            "self_trade_risk",
            {violation["code"] for violation in result["risk"]["violations"]},
        )

    def test_duplicate_actual_orders_are_cleaned_deterministically(self) -> None:
        result = evaluate_case(self.load_case("duplicate_actual_orders.json"))
        canonical = canonical_result(result)
        self.assertTrue(canonical["can_execute"])
        self.assertEqual(
            [action[0] for action in canonical["actions"]],
            ["cancel", "cancel", "keep"],
        )
        self.assertEqual(canonical["unmanaged_order_ids"], ["manual-order"])


if __name__ == "__main__":
    unittest.main()

