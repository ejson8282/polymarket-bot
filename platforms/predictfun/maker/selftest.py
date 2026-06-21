from __future__ import annotations

import json
import sys
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.predictfun.maker.dry_run import build_quote_plan, plan_to_jsonable
from platforms.predictfun.maker.intents import build_intent_state
from platforms.predictfun.maker.reconcile import reconcile_once
from platforms.predictfun.maker.simulator import update_simulation
from platforms.predictfun.scanner import PredictMarket


def _fake_market() -> PredictMarket:
    return PredictMarket(
        id=999001,
        title="PF self-test market",
        question="PF self-test market?",
        status="OPEN",
        trading_status="TRADING",
        market_variant="BINARY",
        category_slug="self-test",
        decimal_precision=2,
        fee_rate_bps=0,
        spread_threshold=Decimal("0.08"),
        share_threshold=Decimal("10"),
        hourly_rate=Decimal("100"),
        reward_starts_at="2026-01-01T00:00:00Z",
        reward_ends_at="2099-01-01T00:00:00Z",
        starts_at="2026-01-01T00:00:00Z",
        ends_at="2099-01-01T00:00:00Z",
        is_neg_risk=False,
        is_yield_bearing=False,
        yes_token_id="selftest-yes",
        no_token_id="selftest-no",
        best_yes_bid=Decimal("0.44"),
        best_yes_ask=Decimal("0.56"),
        mid=Decimal("0.50"),
        quoted_spread=Decimal("0.12"),
        score=Decimal("1"),
        risk_note="self-test",
    )


def run_selftest() -> dict:
    market = _fake_market()
    orderbook = {
        "_source": "selftest",
        "data": {
            "bids": [["0.44", "100"]],
            "asks": [["0.56", "100"]],
        },
    }
    plan = build_quote_plan(
        market,
        orderbook,
        quote_size=Decimal("10"),
        edge_ticks=1,
        backoff_ticks=2,
        max_quote_levels=2,
        seed_empty_books=False,
        seed_mid_price=Decimal("0.50"),
        seed_distance_ticks=5,
        min_seconds_to_expiry=3600,
        avoid_mid_band_low=Decimal("0.00"),
        avoid_mid_band_high=Decimal("0.00"),
        allow_crypto_updown_quotes=False,
    )
    empty_market = _fake_market()
    empty_market.best_yes_bid = Decimal("0")
    empty_market.best_yes_ask = Decimal("0")
    empty_seed_plan = build_quote_plan(
        empty_market,
        {"_source": "selftest", "data": {"bids": [], "asks": []}},
        quote_size=Decimal("10"),
        edge_ticks=1,
        backoff_ticks=2,
        max_quote_levels=2,
        seed_empty_books=True,
        seed_mid_price=Decimal("0.50"),
        seed_distance_ticks=5,
        min_seconds_to_expiry=3600,
        avoid_mid_band_low=Decimal("0.35"),
        avoid_mid_band_high=Decimal("0.65"),
        allow_crypto_updown_quotes=False,
    )
    plan_json = plan_to_jsonable(plan)
    first_intents = build_intent_state(
        environment="selftest",
        plans=[plan_json],
        previous_intents=[],
    )
    second_intents = build_intent_state(
        environment="selftest",
        plans=[plan_json],
        previous_intents=first_intents["intents"],
    )
    multi_account_intents = build_intent_state(
        environment="selftest",
        plans=[plan_json, plan_json],
        previous_intents=[],
        accounts_config={"ids": ["acct01", "acct02"], "max_active_accounts": 2},
    )
    exit_intents = build_intent_state(
        environment="selftest",
        plans=[plan_json],
        previous_intents=[],
        accounts_config={"ids": ["acct01"], "max_active_accounts": 1},
        inventory_positions=[
            {
                "account_id": "acct01",
                "market_id": "999001",
                "outcome": "YES",
                "size": "5",
            }
        ],
        inventory_config={
            "max_long_size_per_outcome": "30",
            "exit_quote_size_pct_of_position": "1",
            "min_exit_size": "1",
            "exit_edge_ticks": 1,
        },
    )
    reserved_cap_intents = build_intent_state(
        environment="selftest",
        plans=[plan_to_jsonable(empty_seed_plan)],
        previous_intents=[],
        inventory_config={"max_long_size_per_outcome": "10"},
    )
    report = reconcile_once(first_intents)
    crossed = dict(first_intents["intents"][0])
    crossed["intent_id"] = "pf-selftest-crossed"
    crossed["outcome"] = "YES"
    crossed["side"] = "BUY"
    crossed["price"] = "0.56"
    crossed["notional"] = str(Decimal(crossed["price"]) * Decimal(str(crossed["size"])))
    crossed_state = {
        "ts": first_intents["ts"],
        "environment": "selftest",
        "mode": "dry_run",
        "summary": {
            "desired": 1,
            "create": 1,
            "keep": 0,
            "cancel": 0,
            "total_notional": crossed["notional"],
            "accounts": 1,
        },
        "diff": {"create": [crossed], "keep": [], "cancel": []},
        "intents": [crossed],
    }
    crossed_report = reconcile_once(crossed_state)
    filled_sim = update_simulation(
        previous_state={},
        plan_state={"ts": plan_json.get("ts"), "plans": [plan_json]},
        intents_state=crossed_state,
        execution_report=crossed_report,
        max_fill_size=Decimal("10"),
    )
    refill_after_fill = build_intent_state(
        environment="selftest",
        plans=[plan_json],
        previous_intents=filled_sim["active_orders"],
    )

    checks = {
        "can_quote": plan.can_quote,
        "quote_count": len(plan.yes_quotes) + len(plan.no_quotes),
        "first_create_count": first_intents["summary"]["create"],
        "second_keep_count": second_intents["summary"]["keep"],
        "multi_account_count": multi_account_intents["summary"]["accounts"],
        "exit_sell_count": sum(1 for item in exit_intents["intents"] if item.get("side") == "SELL"),
        "report_action_count": report["summary"]["actions"],
        "fill_new_count": filled_sim["summary"]["fills_new"],
        "refill_create_count": refill_after_fill["summary"]["create"],
        "empty_seed_quote_count": len(empty_seed_plan.yes_quotes) + len(empty_seed_plan.no_quotes),
        "reserved_cap_intent_count": reserved_cap_intents["summary"]["desired"],
    }
    ok = (
        checks["can_quote"]
        and checks["quote_count"] == 2
        and checks["first_create_count"] == 2
        and checks["second_keep_count"] == 2
        and checks["multi_account_count"] == 2
        and checks["exit_sell_count"] >= 1
        and checks["report_action_count"] == 2
        and checks["fill_new_count"] == 1
        and checks["refill_create_count"] == 2
        and checks["empty_seed_quote_count"] == 4
        and checks["reserved_cap_intent_count"] == 2
    )
    return {
        "ok": bool(ok),
        "checks": checks,
        "plan": plan_json,
        "first_intents": first_intents,
        "second_intents": second_intents,
        "multi_account_intents": multi_account_intents,
        "exit_intents": exit_intents,
        "filled_simulation": filled_sim,
        "refill_after_fill": refill_after_fill,
        "empty_seed_plan": plan_to_jsonable(empty_seed_plan),
        "reserved_cap_intents": reserved_cap_intents,
        "execution_report": report,
    }


def main() -> None:
    result = run_selftest()
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
