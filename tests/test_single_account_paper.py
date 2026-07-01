from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from platforms.single_account.scorer import score_candidates
from platforms.single_account.signals import load_market_signals


def _config(snapshot_path: str) -> dict:
    return {
        "input": {"market_snapshot_path": snapshot_path},
        "risk": {
            "max_quote_age_seconds": 60,
            "max_spread_bps": 10,
            "min_volume_24h_usdc": 1_000_000,
            "min_score_to_trade": 60,
            "max_notional_usdc": 100,
            "max_leverage": 3,
        },
        "strategies": {
            "funding_carry_rotation": {
                "enabled": True,
                "label": "Funding Carry Rotation",
                "target_hold_hours": [8, 36],
                "preferred_categories": ["major"],
                "weight": 1,
            }
        },
        "universe": [{"symbol": "BTC", "category": "major"}],
    }


class SingleAccountPaperTest(unittest.TestCase):
    def test_missing_snapshot_is_not_actionable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "config.json"
            cfg = _config("missing.json")
            config_path.write_text(json.dumps(cfg), encoding="utf-8")

            signals = load_market_signals(config_path, cfg)
            decisions = score_candidates(cfg, signals)

            self.assertTrue(decisions)
            self.assertEqual({row.decision for row in decisions}, {"skip"})
            self.assertEqual({row.reason for row in decisions}, {"waiting_for_market_snapshot"})
            self.assertEqual({row.score for row in decisions}, {0.0})

    def test_fresh_snapshot_can_produce_paper_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            snapshot_path = Path(tmp) / "snapshot.json"
            snapshot_path.write_text(
                json.dumps(
                    {
                        "candidates": [
                            {
                                "symbol": "BTC",
                                "category": "major",
                                "price": 66000,
                                "quote_age_seconds": 3,
                                "spread_bps": 1.2,
                                "volume_24h_usdc": 50000000,
                                "funding_bps_8h": 4,
                                "trend_score": 0.4,
                                "volatility_score": 0.4,
                                "liquidity_score": 0.9,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )
            config_path = Path(tmp) / "config.json"
            cfg = _config(str(snapshot_path))
            config_path.write_text(json.dumps(cfg), encoding="utf-8")

            signals = load_market_signals(config_path, cfg)
            decisions = score_candidates(cfg, signals)

            self.assertEqual(len(decisions), 1)
            self.assertEqual(decisions[0].decision, "allow")
            self.assertEqual(decisions[0].reason, "paper_candidate_passed")
            self.assertGreaterEqual(decisions[0].score, 60)


if __name__ == "__main__":
    unittest.main()

