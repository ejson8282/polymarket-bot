from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "deploy" / "latitude-console" / "console_app.py"
HTML_PATH = ROOT / "deploy" / "latitude-console" / "console.html"


def _load_module():
    spec = importlib.util.spec_from_file_location("latitude_console_btc_test", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _report_payload() -> dict:
    strategies = []
    values = {
        "adaptive_mean_reversion": (28, "838.18", "-0.23", "2.77", "3.15"),
        "trend_follow": (28, "838.03", "-0.14", "1.77", "2.15"),
        "funding_carry": (0, "0", "0", "0", "0"),
        "regime_switch": (37, "1107.36", "-0.09", "0.87", "1.25"),
    }
    for name, (cycles, volume, pnl, rebate, stress_rebate) in values.items():
        strategies.append(
            {
                "strategy": name,
                "selected_candidate_id": None,
                "diagnostic_candidate_id": f"{name}:diagnostic",
                "blockers": ["not_ready"],
                "holdout": {
                    "started_at": "2026-07-25T00:00:00+00:00",
                    "ended_at": "2026-07-27T00:00:00+00:00",
                    "completed_cycles": cycles,
                    "total_volume_usdc": volume,
                    "net_pnl_usdc": pnl,
                    "break_even_rebate_bps_on_actual_volume": rebate,
                },
                "holdout_spread_stress": {
                    "break_even_rebate_bps_on_actual_volume": stress_rebate,
                },
            }
        )
    return {
        "mode": "read_only_research",
        "writes_possible": False,
        "execution_authorized": False,
        "symbol": "BTC",
        "promotion_ready": False,
        "quotes_loaded": 2204,
        "scenario": {
            "position_sizing_mode": "fixed_notional",
            "leverage": "1",
            "target_notional_usdc": "15",
            "contract": {
                "multiplier_btc_per_contract": "1",
                "contract_step": "0.000001",
                "verified_against_live_venue": False,
            },
        },
        "evaluation": {"strategies": strategies},
    }


def test_reviewed_reference_is_explicitly_non_executable() -> None:
    module = _load_module()
    report = module._btc_single_side_reference()

    assert report["execution_authorized"] is False
    assert report["promotion_ready"] is False
    assert report["source_kind"] == "reviewed_reference_snapshot"
    assert len(report["strategies"]) == 4
    funding = next(row for row in report["strategies"] if row["strategy"] == "funding_carry")
    assert funding["completed_cycles"] == 0
    assert funding["evaluable"] is False
    assert funding["break_even_rebate_bps"] is None


def test_generated_report_is_fail_closed_and_funding_zero_cycles_is_unevaluable() -> None:
    module = _load_module()
    report = module._btc_single_side_report(_report_payload(), age=75)

    assert report is not None
    assert report["execution_authorized"] is False
    assert report["promotion_ready"] is False
    assert report["closest_to_break_even"] == "regime_switch"
    assert report["window_start"] == "2026-07-25T00:00:00+00:00"
    assert report["window_end"] == "2026-07-27T00:00:00+00:00"
    funding = next(row for row in report["strategies"] if row["strategy"] == "funding_carry")
    assert funding["evaluable"] is False
    assert funding["break_even_rebate_bps"] is None
    assert funding["stress_break_even_rebate_bps"] is None


def test_generated_report_rejects_any_execution_authorization() -> None:
    module = _load_module()
    payload = _report_payload()
    payload["execution_authorized"] = True

    assert module._btc_single_side_report(payload, age=0) is None


def test_malformed_numbers_only_degrade_the_research_panel(tmp_path: Path) -> None:
    module = _load_module()

    for bad_cycles in ("NaN", "Infinity", "-Infinity", -1, 1.5, 10_000_001):
        payload = _report_payload()
        payload["evaluation"]["strategies"][0]["holdout"]["completed_cycles"] = bad_cycles
        assert module._btc_single_side_report(payload, age=0) is None

    payload = _report_payload()
    payload["evaluation"]["strategies"][0]["holdout"]["net_pnl_usdc"] = float("nan")
    report_path = tmp_path / "btc-single-side.json"
    report_path.write_text(json.dumps(payload), encoding="utf-8")
    module.BTC_SINGLE_SIDE_REPORT_PATH = report_path

    report = module._btc_single_side_research()
    assert report["source_kind"] == "reviewed_reference_snapshot"
    assert report["execution_authorized"] is False
    assert report["promotion_ready"] is False


def test_single_account_html_has_read_only_four_strategy_panel() -> None:
    source = HTML_PATH.read_text(encoding="utf-8")

    assert 'id="sa-btc-research"' in source
    assert 'id="sa-btc-strategies"' in source
    assert "NO PAPER / NO LIVE" in source
    assert "function bindBTCSingleSide(report)" in source
    assert "bindBTCSingleSide(sa.btc_single_side_research||{});" in source
    assert "Funding carry 零周期按不可评估处理" in MODULE_PATH.read_text(encoding="utf-8")
