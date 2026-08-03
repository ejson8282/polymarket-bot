from __future__ import annotations

from decimal import Decimal
import time
from types import SimpleNamespace

import pytest

from platforms.predictfun.maker.admission import select_stable_markets
from platforms.predictfun.maker.dry_run import (
    _book_from_ws_state,
    _load_fresh_ws_state,
    build_quote_plan,
    plan_to_jsonable,
)
from platforms.predictfun.maker.executor import (
    ExecutableOrder,
    ExecutionResult,
    PredictFunLiveExecutor,
)
from platforms.predictfun.maker.intents import build_intent_state, utc_now
from platforms.predictfun.maker.liquidity_sentinel import LiquiditySentinel
from platforms.predictfun.maker.managed_orders import ManagedOrderRegistry
from platforms.predictfun.maker.reconcile import (
    reconcile_cancel_only,
    reconcile_once,
    reconcile_reduce_only,
)
from platforms.predictfun.maker.risk import evaluate_risk
from platforms.predictfun.maker.simulator import update_simulation
from platforms.predictfun.maker.validation import validate_final_order
from platforms.predictfun.scanner import (
    PredictMarket,
    UnsupportedPredictMarket,
    normalize_market,
    scan_markets,
    score_market,
)
from platforms.predictfun.ws_watch import (
    _apply_market_message,
    _write_state_if_due,
    normalize_orderbook_payload,
)


def _market() -> PredictMarket:
    return PredictMarket(
        id=42,
        title="Binary test",
        question="Binary test?",
        status="OPEN",
        trading_status="OPEN",
        market_variant="BINARY",
        category_slug="test",
        decimal_precision=2,
        fee_rate_bps=10,
        spread_threshold=Decimal("0.10"),
        share_threshold=Decimal("10"),
        hourly_rate=Decimal("10"),
        reward_starts_at="2026-01-01T00:00:00Z",
        reward_ends_at="2099-01-01T00:00:00Z",
        starts_at="2026-01-01T00:00:00Z",
        ends_at="2099-01-01T00:00:00Z",
        is_neg_risk=False,
        is_yield_bearing=True,
        yes_token_id="yes-token",
        no_token_id="no-token",
        yes_label="YES",
        no_label="NO",
        best_yes_bid=Decimal("0.40"),
        best_yes_ask=Decimal("0.60"),
        mid=Decimal("0.50"),
        quoted_spread=Decimal("0.20"),
        score=Decimal("2"),
        risk_note="normal",
    )


def _plan(*, can_quote: bool = True) -> dict:
    market = _market()
    plan = build_quote_plan(
        market,
        {"_source": "test", "data": {"bids": [["0.40", "100"]], "asks": [["0.60", "100"]]}},
        quote_size=Decimal("10"),
        edge_ticks=1,
        backoff_ticks=2,
        max_quote_levels=1,
        seed_empty_books=False,
        seed_mid_price=Decimal("0.50"),
        seed_distance_ticks=5,
        min_seconds_to_expiry=3600,
        avoid_mid_band_low=Decimal("0"),
        avoid_mid_band_high=Decimal("0"),
        allow_crypto_updown_quotes=False,
    )
    data = plan_to_jsonable(plan)
    if not can_quote:
        data["can_quote"] = False
        data["skip_reason"] = "liquidity sentinel"
        data["yes_quotes"] = []
        data["no_quotes"] = []
    return data


def _raw_market(*, outcomes: int = 2) -> dict:
    rows = [
        {"name": "YES", "onChainId": "yes-token", "bestBid": {"price": "0.4"}, "bestAsk": {"price": "0.6"}},
        {"name": "NO", "onChainId": "no-token", "bestBid": {"price": "0.4"}, "bestAsk": {"price": "0.6"}},
        {"name": "MAYBE", "onChainId": "maybe-token", "bestBid": {"price": "0.1"}, "bestAsk": {"price": "0.2"}},
    ][:outcomes]
    return {
        "id": 42 + outcomes,
        "title": "Market",
        "question": "Market?",
        "status": "OPEN",
        "tradingStatus": "OPEN",
        "marketVariant": "BINARY" if outcomes == 2 else "MULTI_OUTCOME",
        "categorySlug": "test",
        "decimalPrecision": 2,
        "feeRateBps": 10,
        "isNegRisk": False,
        "isYieldBearing": True,
        "spreadThreshold": "0.1",
        "shareThreshold": "10",
        "rewards": {"current": {"hourlyRate": "10"}},
        "endsAt": "2099-01-01T00:00:00Z",
        "outcomes": rows,
    }


class RecordingExecutor:
    def __init__(self) -> None:
        self.cancelled: list[tuple[str, str, str]] = []
        self.created: list[ExecutableOrder] = []

    def create(self, order: ExecutableOrder) -> ExecutionResult:
        self.created.append(order)
        return ExecutionResult(
            intent_id=order.intent_id,
            account_id=order.account_id,
            action="create",
            ok=True,
            message="accepted",
            order_id=f"official:{order.intent_id}",
            status="open",
        )

    def cancel(self, order_id: str, *, intent_id: str = "", account_id: str = "") -> ExecutionResult:
        self.cancelled.append((order_id, intent_id, account_id))
        return ExecutionResult(
            intent_id=intent_id,
            account_id=account_id,
            action="cancel",
            ok=True,
            message="cancelled",
            order_id=order_id,
            status="cancelled",
        )


def test_empty_book_seed_precedes_depth_guard() -> None:
    market = _market()
    market.best_yes_bid = Decimal("0")
    market.best_yes_ask = Decimal("0")
    plan = build_quote_plan(
        market,
        {"_source": "test", "data": {"bids": [], "asks": []}},
        quote_size=Decimal("10"),
        edge_ticks=1,
        backoff_ticks=2,
        max_quote_levels=1,
        seed_empty_books=True,
        seed_mid_price=Decimal("0.50"),
        seed_distance_ticks=5,
        min_seconds_to_expiry=3600,
        avoid_mid_band_low=Decimal("0.35"),
        avoid_mid_band_high=Decimal("0.65"),
        allow_crypto_updown_quotes=False,
        min_depth_notional=Decimal("2"),
    )
    assert plan.can_quote is True
    assert len(plan.yes_quotes) == 1
    assert len(plan.no_quotes) == 1


def test_orderbook_error_never_becomes_empty_book_seed() -> None:
    market = _market()
    market.best_yes_bid = Decimal("0")
    market.best_yes_ask = Decimal("0")
    plan = build_quote_plan(
        market,
        {
            "_source": "rest_error",
            "error": "request failed",
            "data": {"bids": [], "asks": []},
        },
        quote_size=Decimal("10"),
        edge_ticks=1,
        backoff_ticks=2,
        max_quote_levels=1,
        seed_empty_books=True,
        seed_mid_price=Decimal("0.50"),
        seed_distance_ticks=5,
        min_seconds_to_expiry=3600,
        avoid_mid_band_low=Decimal("0.35"),
        avoid_mid_band_high=Decimal("0.65"),
        allow_crypto_updown_quotes=False,
    )
    assert plan.can_quote is False
    assert plan.skip_reason == "orderbook unavailable"


def test_market_level_ws_timestamp_blocks_stale_book() -> None:
    state = {
        "orderbooks": {"42": {"bids": [["0.4", "10"]], "asks": [["0.6", "10"]]}},
        "orderbook_updated_at": {"42": "2020-01-01T00:00:00Z"},
    }
    assert _book_from_ws_state(state, 42, max_age_sec=120) == {}


def test_market_level_ws_timestamp_is_required() -> None:
    state = {
        "orderbooks": {"42": {"bids": [["0.4", "10"]], "asks": [["0.6", "10"]]}},
        "orderbook_updated_at": {},
    }
    assert _book_from_ws_state(state, 42, max_age_sec=120) == {}


def test_disconnected_ws_state_is_rejected_even_with_fresh_timestamp(tmp_path) -> None:
    state_path = tmp_path / "ws.json"
    state_path.write_text(
        '{"connected": false, "ts": "2099-01-01T00:00:00Z", "orderbooks": {}}',
        encoding="utf-8",
    )
    assert _load_fresh_ws_state(state_path, max_age_sec=120) == {}


def test_ws_state_requires_explicit_connected_true(tmp_path) -> None:
    state_path = tmp_path / "ws.json"
    state_path.write_text(
        '{"ts": "2099-01-01T00:00:00Z", "orderbooks": {}}',
        encoding="utf-8",
    )
    assert _load_fresh_ws_state(state_path, max_age_sec=120) == {}


def test_ws_orderbook_payload_is_normalized_and_sorted() -> None:
    payload = normalize_orderbook_payload(
        "predictOrderbook/42",
        {
            "version": 1,
            "marketId": 42,
            "updateTimestampMs": int(time.time() * 1000),
            "orderCount": 4,
            "bids": [[0.40, 10], [0.41, 5]],
            "asks": [[0.60, 10], [0.59, 5]],
        },
    )
    assert payload["bids"] == [["0.41", "5"], ["0.4", "10"]]
    assert payload["asks"] == [["0.59", "5"], ["0.6", "10"]]


def test_ws_orderbook_rejects_market_mismatch_and_crossed_book() -> None:
    now_ms = int(time.time() * 1000)
    with pytest.raises(ValueError, match="payload_market_id_mismatch"):
        normalize_orderbook_payload(
            "predictOrderbook/42",
            {"marketId": 41, "updateTimestampMs": now_ms, "bids": [], "asks": []},
        )
    with pytest.raises(ValueError, match="orderbook_crossed"):
        normalize_orderbook_payload(
            "predictOrderbook/42",
            {
                "marketId": 42,
                "updateTimestampMs": now_ms,
                "bids": [[0.60, 1]],
                "asks": [[0.59, 1]],
            },
        )


def test_ws_orderbook_rejects_out_of_order_update() -> None:
    now = time.time()
    state = {
        "orderbooks": {},
        "orderbook_updated_at": {},
        "orderbook_upstream_updated_at_ms": {"42": int(now * 1000)},
        "orderbook_latency_ms": {},
        "orderbook_errors": {},
        "trading_statuses": {},
        "market_statuses": {},
        "liquidity": {},
        "liquidity_alerts": {},
    }
    with pytest.raises(ValueError, match="orderbook_update_out_of_order"):
        _apply_market_message(
            state,
            {
                "topic": "predictOrderbook/42",
                "data": {
                    "marketId": 42,
                    "updateTimestampMs": int(now * 1000) - 1,
                    "bids": [[0.40, 10]],
                    "asks": [[0.60, 10]],
                },
            },
            sentinel=LiquiditySentinel.from_config({"enabled": False}),
            now=now,
        )


def test_ws_book_rejects_non_open_trading_status_and_stale_upstream() -> None:
    now = time.time()
    state = {
        "schema_version": 2,
        "orderbooks": {"42": {"bids": [["0.4", "10"]], "asks": [["0.6", "10"]]}},
        "orderbook_updated_at": {"42": utc_now()},
        "orderbook_upstream_updated_at_ms": {"42": int(now * 1000)},
        "trading_statuses": {"42": {"status": "CANCEL_ONLY"}},
        "market_statuses": {"42": {"status": "REGISTERED"}},
        "orderbook_errors": {},
    }
    assert _book_from_ws_state(state, 42, max_age_sec=5) == {}

    state["trading_statuses"]["42"]["status"] = "OPEN"
    state["orderbook_upstream_updated_at_ms"]["42"] = int((now - 10) * 1000)
    assert _book_from_ws_state(state, 42, max_age_sec=5) == {}


def test_ws_book_requires_all_schema_v2_safety_statuses() -> None:
    now = time.time()
    state = {
        "schema_version": 2,
        "orderbooks": {"42": {"bids": [["0.4", "10"]], "asks": [["0.6", "10"]]}},
        "orderbook_updated_at": {"42": utc_now()},
        "orderbook_upstream_updated_at_ms": {"42": int(now * 1000)},
        "trading_statuses": {"42": {"status": "OPEN"}},
        "market_statuses": {"42": {"status": "REGISTERED"}},
        "orderbook_errors": {},
    }
    assert _book_from_ws_state(state, 42, max_age_sec=5)["_source"] == "ws"

    state["market_statuses"] = {}
    assert _book_from_ws_state(state, 42, max_age_sec=5) == {}
    state["market_statuses"] = {"42": {"status": "REGISTERED"}}
    state["orderbook_errors"] = {"42": "orderbook_crossed"}
    assert _book_from_ws_state(state, 42, max_age_sec=5) == {}


def test_points_profile_allows_midpoint_without_hiding_risk_profile() -> None:
    conservative_score, _ = score_market(
        hourly_rate=Decimal("10"),
        share_threshold=Decimal("10"),
        spread_threshold=Decimal("0.1"),
        quoted_spread=Decimal("0.2"),
        mid=Decimal("0.5"),
        market_variant="BINARY",
        ends_at="2099-01-01T00:00:00Z",
        scoring_profile="conservative",
    )
    points_score, note = score_market(
        hourly_rate=Decimal("10"),
        share_threshold=Decimal("10"),
        spread_threshold=Decimal("0.1"),
        quoted_spread=Decimal("0.2"),
        mid=Decimal("0.5"),
        market_variant="BINARY",
        ends_at="2099-01-01T00:00:00Z",
        scoring_profile="points",
    )
    unknown_score, _ = score_market(
        hourly_rate=Decimal("10"),
        share_threshold=Decimal("10"),
        spread_threshold=Decimal("0.1"),
        quoted_spread=Decimal("0.2"),
        mid=Decimal("0.5"),
        market_variant="BINARY",
        ends_at="2099-01-01T00:00:00Z",
        scoring_profile="typo",
    )
    assert points_score > conservative_score
    assert unknown_score == conservative_score
    assert "points" in note

    plan = build_quote_plan(
        _market(),
        {"_source": "test", "data": {"bids": [["0.45", "100"]], "asks": [["0.55", "100"]]}},
        quote_size=Decimal("10"),
        edge_ticks=1,
        backoff_ticks=2,
        max_quote_levels=1,
        seed_empty_books=False,
        seed_mid_price=Decimal("0.50"),
        seed_distance_ticks=5,
        min_seconds_to_expiry=3600,
        avoid_mid_band_low=Decimal("0.35"),
        avoid_mid_band_high=Decimal("0.65"),
        allow_crypto_updown_quotes=False,
        avoid_mid_band=False,
    )
    assert plan.can_quote is True


def test_position_halts_new_buys_but_keeps_inventory_exit() -> None:
    state = build_intent_state(
        environment="test",
        plans=[_plan()],
        previous_intents=[],
        accounts_config={"ids": ["account_01"], "max_active_accounts": 1},
        inventory_positions=[{"account_id": "account_01", "market_id": 42, "outcome": "YES", "size": "5"}],
        inventory_config={
            "halt_market_buys_while_position": True,
            "exit_quote_size_pct_of_position": "1",
            "min_exit_size": "1",
            "exit_edge_ticks": 1,
        },
    )
    assert not [row for row in state["intents"] if row["side"] == "BUY"]
    exits = [row for row in state["intents"] if row["purpose"] == "inventory_exit"]
    assert len(exits) == 1
    assert exits[0]["side"] == "SELL"


def test_exit_follows_position_owner_not_round_robin_quote_owner() -> None:
    plan = _plan()
    plan["can_quote"] = True
    plan["yes_quotes"] = [
        {
            "outcome": "YES",
            "side": "BUY",
            "price": "0.4",
            "size": "5",
            "reason": "test quote",
        }
    ]
    state = build_intent_state(
        environment="test",
        plans=[plan],
        previous_intents=[],
        accounts_config={
            "ids": ["account_01", "account_02"],
            "max_active_accounts": 2,
            "assignment": "round_robin",
        },
        inventory_positions=[
            {
                "account_id": "account_02",
                "market_id": 42,
                "outcome": "YES",
                "size": "5",
            }
        ],
        inventory_config={"halt_market_buys_while_position": True},
    )
    buys = [row for row in state["intents"] if row["side"] == "BUY"]
    exits = [row for row in state["intents"] if row["purpose"] == "inventory_exit"]
    assert {row["account_id"] for row in buys} == {"account_01"}
    assert {row["account_id"] for row in exits} == {"account_02"}


def test_partial_fill_halts_replenishment_and_sizes_exit_to_position() -> None:
    plan = _plan()
    crossed = {
        "intent_id": "partial-fill",
        "account_id": "account_01",
        "market_id": 42,
        "outcome": "YES",
        "side": "BUY",
        "price": "0.60",
        "size": "10",
        "notional": "6",
        "purpose": "maker_quote",
    }
    intents = {
        "ts": "2026-08-03T00:00:00Z",
        "diff": {"create": [crossed], "keep": [], "cancel": []},
        "intents": [crossed],
    }
    simulation = update_simulation(
        previous_state={},
        plan_state={"ts": "2026-08-03T00:00:00Z", "plans": [plan]},
        intents_state=intents,
        execution_report={
            "ts": "2026-08-03T00:00:00Z",
            "results": [
                {
                    "action": "create",
                    "intent_id": "partial-fill",
                    "ok": True,
                }
            ],
        },
        max_fill_size=Decimal("2"),
    )
    assert simulation["new_fills"][0]["size"] == "2"
    next_state = build_intent_state(
        environment="test",
        plans=[plan],
        previous_intents=simulation["active_orders"],
        accounts_config={"ids": ["account_01"], "max_active_accounts": 1},
        inventory_positions=simulation["positions"],
        inventory_config={"halt_market_buys_while_position": True},
    )
    assert not [row for row in next_state["intents"] if row["side"] == "BUY"]
    exits = [row for row in next_state["intents"] if row["purpose"] == "inventory_exit"]
    assert len(exits) == 1
    assert exits[0]["size"] == "2.000000"


def test_inventory_exit_survives_quote_liquidity_block() -> None:
    state = build_intent_state(
        environment="test",
        plans=[_plan(can_quote=False)],
        previous_intents=[],
        inventory_positions=[{"account_id": "acct01", "market_id": 42, "outcome": "YES", "size": "3"}],
        inventory_config={"exit_quote_size_pct_of_position": "1", "min_exit_size": "1"},
    )
    assert [row for row in state["intents"] if row["purpose"] == "inventory_exit"]


def test_continuous_scanner_rejects_multi_outcome_market() -> None:
    with pytest.raises(UnsupportedPredictMarket):
        normalize_market(_raw_market(outcomes=3))

    class Client:
        def list_markets(self, **kwargs):
            del kwargs
            return {"data": [_raw_market(outcomes=3), _raw_market(outcomes=2)], "cursor": None}

    markets = scan_markets(Client(), max_markets=10)
    assert len(markets) == 1
    assert markets[0].market_variant == "BINARY"


def test_yes_no_tokens_are_mapped_by_name_not_array_order() -> None:
    raw = _raw_market(outcomes=2)
    raw["outcomes"] = list(reversed(raw["outcomes"]))
    market = normalize_market(raw)
    assert market.yes_token_id == "yes-token"
    assert market.no_token_id == "no-token"

    raw["outcomes"][0]["name"] = "UP"
    raw["outcomes"][1]["name"] = "DOWN"
    with pytest.raises(UnsupportedPredictMarket, match="indexSet 1/2 or canonical YES/NO"):
        normalize_market(raw)


def test_live_registered_market_maps_index_sets_and_preserves_display_labels() -> None:
    raw = {
        **_raw_market(outcomes=2),
        "id": 58416,
        "title": "Will Solana hit $60 or $140 first?",
        "status": "REGISTERED",
        "tradingStatus": "OPEN",
        "outcomes": [
            {
                "name": "$140",
                "indexSet": 2,
                "onChainId": "high-token",
                "bestBid": {"price": "0.18"},
                "bestAsk": {"price": "0.19"},
            },
            {
                "name": "$60",
                "indexSet": 1,
                "onChainId": "low-token",
                "bestBid": {"price": "0.81"},
                "bestAsk": {"price": "0.82"},
            },
        ],
    }

    market = normalize_market(raw)

    assert market.status == "REGISTERED"
    assert market.trading_status == "OPEN"
    assert market.yes_token_id == "low-token"
    assert market.no_token_id == "high-token"
    assert market.yes_label == "$60"
    assert market.no_label == "$140"
    assert market.best_yes_bid == Decimal("0.81")
    assert market.best_yes_ask == Decimal("0.82")


def test_scanner_uses_open_filter_but_accepts_registered_lifecycle_response() -> None:
    raw = {
        **_raw_market(outcomes=2),
        "status": "REGISTERED",
        "tradingStatus": "OPEN",
        "outcomes": [
            {**_raw_market(outcomes=2)["outcomes"][0], "indexSet": 1},
            {**_raw_market(outcomes=2)["outcomes"][1], "indexSet": 2},
        ],
    }

    class Client:
        status = ""

        def list_markets(self, **kwargs):
            self.status = kwargs.get("status")
            return {"data": [raw], "cursor": None}

    client = Client()
    markets = scan_markets(client, max_markets=10)

    assert client.status == "OPEN"
    assert [market.id for market in markets] == [44]


def test_ws_orderbook_preserves_exact_decimal_values() -> None:
    normalized = normalize_orderbook_payload(
        "predictOrderbook/44",
        {
            "marketId": 44,
            "updateTimestampMs": 1,
            "bids": [["0.4000000000000000001", "10.2500"]],
            "asks": [["0.6000000000000000001", "11"]],
        },
    )

    assert normalized["bids"] == [["0.4000000000000000001", "10.2500"]]
    assert normalized["asks"] == [["0.6000000000000000001", "11"]]


@pytest.mark.parametrize("bad_value", ["NaN", "Infinity", "-Infinity"])
def test_ws_orderbook_rejects_non_finite_numbers(bad_value: str) -> None:
    with pytest.raises(ValueError, match="orderbook_level_out_of_range"):
        normalize_orderbook_payload(
            "predictOrderbook/44",
            {
                "marketId": 44,
                "updateTimestampMs": 1,
                "bids": [[bad_value, "10"]],
                "asks": [["0.60", "10"]],
            },
        )


def test_ws_market_status_rejects_unknown_values() -> None:
    state = {
        "orderbooks": {},
        "orderbook_updated_at": {},
        "orderbook_upstream_updated_at_ms": {},
        "orderbook_latency_ms": {},
        "orderbook_errors": {},
        "trading_statuses": {},
        "market_statuses": {},
        "liquidity": {},
        "liquidity_alerts": {},
    }
    with pytest.raises(ValueError, match="market_status_invalid"):
        _apply_market_message(
            state,
            {
                "topic": "predictMarketStatus/44",
                "data": {"marketId": 44, "status": "SURPRISE"},
            },
            sentinel=LiquiditySentinel.from_config({}),
            now=time.time(),
        )


def test_ws_state_writes_are_coalesced_but_forceable(tmp_path) -> None:
    path = tmp_path / "ws.json"
    state = {"connected": True}

    last = _write_state_if_due(path, state, 10.0, now_monotonic=10.1)
    assert last == 10.0
    assert not path.exists()

    last = _write_state_if_due(path, state, last, now_monotonic=10.3)
    assert last == 10.3
    assert path.exists()

    state["connected"] = False
    _write_state_if_due(
        path,
        state,
        last,
        force=True,
        now_monotonic=10.31,
    )
    assert path.read_text(encoding="utf-8").find('"connected": false') >= 0


def test_market_normalization_parses_string_mode_flags() -> None:
    raw = {**_raw_market(outcomes=2), "isNegRisk": "false", "isYieldBearing": "true"}
    market = normalize_market(raw)
    assert market.is_neg_risk is False
    assert market.is_yield_bearing is True

    with pytest.raises(UnsupportedPredictMarket, match="boolean isNegRisk"):
        normalize_market({**raw, "isNegRisk": "unknown"})


def test_admission_hysteresis_prevents_small_rank_churn() -> None:
    incumbent = SimpleNamespace(id=1, score=Decimal("10"))
    challenger = SimpleNamespace(id=2, score=Decimal("9"))
    selected, state = select_stable_markets(
        [incumbent, challenger],
        previous_state={},
        max_markets=1,
        min_dwell_sec=100,
        replacement_score_margin=Decimal("0.5"),
        now_ts=1000,
    )
    assert [row.id for row in selected] == [1]

    incumbent.score = Decimal("9.5")
    challenger.score = Decimal("9.7")
    selected, state = select_stable_markets(
        [challenger, incumbent],
        previous_state=state,
        max_markets=1,
        min_dwell_sec=100,
        replacement_score_margin=Decimal("0.5"),
        now_ts=1050,
    )
    assert [row.id for row in selected] == [1]

    challenger.score = Decimal("10.2")
    selected, _ = select_stable_markets(
        [challenger, incumbent],
        previous_state=state,
        max_markets=1,
        min_dwell_sec=100,
        replacement_score_margin=Decimal("0.5"),
        now_ts=1200,
    )
    assert [row.id for row in selected] == [2]


def test_position_market_is_pinned_in_admission() -> None:
    high = SimpleNamespace(id=1, score=Decimal("100"))
    position_market = SimpleNamespace(id=2, score=Decimal("1"))
    selected, state = select_stable_markets(
        [high, position_market],
        previous_state={},
        max_markets=1,
        min_dwell_sec=0,
        replacement_score_margin=Decimal("0"),
        pinned_market_ids={2},
        now_ts=1000,
    )
    assert [row.id for row in selected] == [2]
    assert state["summary"]["pinned"] == 1


def test_managed_registry_never_adopts_unseen_manual_order() -> None:
    registry = ManagedOrderRegistry()
    order = ExecutableOrder(
        intent_id="intent-1",
        account_id="account_01",
        market_id=42,
        outcome="YES",
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("5"),
    )
    registry.record_create(
        order,
        ExecutionResult(
            intent_id="intent-1",
            account_id="account_01",
            action="create",
            ok=True,
            message="accepted",
            order_id="official-1",
            status="open",
        ),
    )
    assert registry.owns_order_id("official-1") is True
    assert registry.owns_order_id("manual-website-order") is False
    registry.record_cancel(
        "official-1",
        ExecutionResult(
            intent_id="intent-1",
            account_id="account_01",
            action="cancel",
            ok=True,
            message="cancelled",
            order_id="official-1",
            status="cancelled",
        ),
    )
    assert registry.active() == []


def test_risk_cancel_only_targets_managed_orders() -> None:
    registry = ManagedOrderRegistry()
    order = ExecutableOrder(
        intent_id="intent-risk",
        account_id="account_01",
        market_id=42,
        outcome="YES",
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("5"),
    )
    registry.record_create(
        order,
        ExecutionResult(
            intent_id=order.intent_id,
            account_id=order.account_id,
            action="create",
            ok=True,
            message="accepted",
            order_id="managed-risk-order",
            status="open",
        ),
    )
    executor = RecordingExecutor()
    report = reconcile_cancel_only(
        managed_state=registry.to_state(),
        executor=executor,
        reason="stale_data",
    )
    assert report["summary"] == {
        "actions": 1,
        "create": 0,
        "cancel": 1,
        "failed": 0,
        "blocked": 1,
    }
    assert executor.cancelled == [("managed-risk-order", "intent-risk", "account_01")]
    assert report["managed_orders"]["summary"]["active"] == 0


def test_normal_reconcile_ignores_cancel_for_unmanaged_manual_order() -> None:
    executor = RecordingExecutor()
    report = reconcile_once(
        {
            "diff": {
                "cancel": [
                    {
                        "intent_id": "manual-or-stale-intent",
                        "account_id": "account_01",
                    }
                ]
            }
        },
        executor=executor,
        managed_state={},
    )

    assert executor.cancelled == []
    assert report["summary"]["actions"] == 0


def test_continuous_live_executor_fails_closed_without_balance_contract() -> None:
    executor = PredictFunLiveExecutor(
        signer_url="http://signer.invalid",
        account_id="account_01",
    )
    order = ExecutableOrder(
        intent_id="not-submitted",
        account_id="account_01",
        market_id=42,
        outcome="YES",
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("1"),
    )
    assert executor.list_balances() == []
    assert executor.create(order).ok is False


def test_exposure_limit_enters_reduce_only_and_preserves_exit_path() -> None:
    intents = {
        "summary": {"total_notional": "52"},
        "intents": [
            {
                "intent_id": "new-buy",
                "account_id": "account_01",
                "market_id": 42,
                "outcome": "NO",
                "side": "BUY",
                "notional": "2",
                "purpose": "maker_quote",
            },
            {
                "intent_id": "position-exit",
                "account_id": "account_01",
                "market_id": 42,
                "outcome": "YES",
                "side": "SELL",
                "price": "0.5",
                "size": "100",
                "notional": "50",
                "purpose": "inventory_exit",
            },
        ],
        "diff": {
            "create": [
                {
                    "intent_id": "position-exit",
                    "account_id": "account_01",
                    "market_id": 42,
                    "outcome": "YES",
                    "side": "SELL",
                    "price": "0.5",
                    "size": "100",
                    "notional": "50",
                    "purpose": "inventory_exit",
                }
            ]
        },
    }
    risk = evaluate_risk(
        cfg={
            "accounts": {"max_active_accounts": 1},
            "risk": {
                "max_plan_state_age_sec": 180,
                "max_total_desired_notional": "1",
                "max_account_desired_notional": "1",
                "max_account_market_desired_notional": "1",
                "max_market_desired_notional": "1",
                "max_market_position_size": "10",
                "max_account_market_position_size": "10",
            },
        },
        plan_state={"ts": utc_now()},
        intents_state=intents,
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={
            "positions": [
                {
                    "account_id": "account_01",
                    "market_id": 42,
                    "outcome": "YES",
                    "size": "100",
                }
            ]
        },
        kill_switch_state={},
    )
    assert risk["blocked"] is True
    assert risk["hard_blocked"] is False
    assert risk["execution_mode"] == "reduce_only"
    assert risk["summary"]["desired_total_notional"] == "52"
    assert risk["summary"]["risk_increasing_notional"] == "2"

    registry = ManagedOrderRegistry()
    maker_order = ExecutableOrder(
        intent_id="old-maker",
        account_id="account_01",
        market_id=7,
        outcome="YES",
        side="BUY",
        price=Decimal("0.4"),
        size=Decimal("5"),
    )
    registry.record_create(
        maker_order,
        ExecutionResult(
            intent_id=maker_order.intent_id,
            account_id=maker_order.account_id,
            action="create",
            ok=True,
            message="accepted",
            order_id="official-maker",
            status="open",
        ),
    )
    executor = RecordingExecutor()
    report = reconcile_reduce_only(
        intents,
        managed_state=registry.to_state(),
        executor=executor,
        risk_state=risk,
    )
    assert executor.cancelled == [("official-maker", "old-maker", "account_01")]
    assert [order.intent_id for order in executor.created] == ["position-exit"]
    assert report["mode"] == "reduce_only"


def test_inventory_exit_notional_does_not_consume_buy_risk_budget() -> None:
    risk = evaluate_risk(
        cfg={
            "accounts": {"max_active_accounts": 1},
            "risk": {
                "max_plan_state_age_sec": 180,
                "max_total_desired_notional": "1",
                "max_account_desired_notional": "1",
                "max_account_market_desired_notional": "1",
                "max_market_desired_notional": "1",
                "max_market_position_size": "100",
                "max_account_market_position_size": "100",
            },
        },
        plan_state={"ts": utc_now()},
        intents_state={
            "summary": {"total_notional": "50"},
            "intents": [
                {
                    "account_id": "account_01",
                    "market_id": 42,
                    "side": "SELL",
                    "notional": "50",
                    "purpose": "inventory_exit",
                }
            ],
        },
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={"positions": []},
        kill_switch_state={},
    )
    assert risk["blocked"] is False
    assert risk["summary"]["risk_increasing_notional"] == "0"


def test_stale_plan_and_notional_limit_fail_closed() -> None:
    risk = evaluate_risk(
        cfg={
            "accounts": {"max_active_accounts": 1},
            "risk": {
                "max_plan_state_age_sec": 1,
                "max_total_desired_notional": "1",
                "max_account_desired_notional": "0",
                "max_account_market_desired_notional": "0",
                "max_market_desired_notional": "0",
            },
        },
        plan_state={"ts": "2020-01-01T00:00:00Z"},
        intents_state={
            "summary": {"total_notional": "2"},
            "intents": [
                {"account_id": "account_01", "market_id": 42, "notional": "2", "side": "BUY"}
            ],
        },
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={"positions": []},
        kill_switch_state={},
    )
    assert risk["blocked"] is True
    blocked = {row["name"] for row in risk["checks"] if row["status"] == "BLOCK"}
    assert {"plan_state_fresh", "desired_total_notional"}.issubset(blocked)


def test_required_ws_is_a_hard_risk_gate() -> None:
    cfg = {
        "accounts": {"max_active_accounts": 1},
        "data": {
            "use_ws_orderbook_cache": True,
            "require_ws_for_quotes": True,
            "ws_state_max_age_sec": 5,
        },
        "risk": {"max_plan_state_age_sec": 180},
    }
    risk = evaluate_risk(
        cfg=cfg,
        plan_state={"ts": utc_now()},
        intents_state={"summary": {}, "intents": []},
        runner_state={"error_count": 0},
        ws_state={},
        simulation_state={"positions": []},
        kill_switch_state={},
    )
    assert risk["hard_blocked"] is True
    blocked = {row["name"] for row in risk["checks"] if row["status"] == "BLOCK"}
    assert {
        "ws_connected",
        "ws_message_fresh",
        "ws_orderbooks_present",
    }.issubset(blocked)

    healthy = {
        "connected": True,
        "last_message_at": utc_now(),
        "orderbooks": {"42": {"bids": [], "asks": []}},
        "orderbook_errors": {},
    }
    risk = evaluate_risk(
        cfg=cfg,
        plan_state={"ts": utc_now()},
        intents_state={"summary": {}, "intents": []},
        runner_state={"error_count": 0},
        ws_state=healthy,
        simulation_state={"positions": []},
        kill_switch_state={},
    )
    assert risk["hard_blocked"] is False

    healthy["orderbook_errors"] = {"43": "orderbook_crossed"}
    risk = evaluate_risk(
        cfg=cfg,
        plan_state={"ts": utc_now()},
        intents_state={"summary": {}, "intents": []},
        runner_state={"error_count": 0},
        ws_state=healthy,
        simulation_state={"positions": []},
        kill_switch_state={},
    )
    assert risk["hard_blocked"] is False
    assert risk["status"] == "WARN"
    error_check = next(
        row for row in risk["checks"] if row["name"] == "ws_orderbook_errors"
    )
    assert error_check["block_scope"] == "market"


def test_final_preflight_blocks_crossing_and_mode_changes() -> None:
    original = _raw_market(outcomes=2)
    safe = validate_final_order(
        original_market=original,
        fresh_market=original,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.59"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert safe["ok"] is True

    crossing = validate_final_order(
        original_market=original,
        fresh_market=original,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.60"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert crossing == {
        "ok": False,
        "reason": "post_only_buy_would_cross",
        "best_bid": "0.4",
        "best_ask": "0.6",
        "notional": "0.60",
    }

    changed = {**original, "feeRateBps": 20}
    mode_change = validate_final_order(
        original_market=original,
        fresh_market=changed,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.59"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert mode_change["ok"] is False
    assert "feeRateBps" in mode_change["reason"]

    missing_status = dict(original)
    missing_status.pop("tradingStatus")
    status_check = validate_final_order(
        original_market=original,
        fresh_market=missing_status,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.59"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert status_check["ok"] is False
    assert status_check["reason"] == "market_status_missing"

    no_cap = validate_final_order(
        original_market=original,
        fresh_market=original,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.59"),
        size=Decimal("1"),
        max_notional=Decimal("0"),
    )
    assert no_cap["ok"] is False
    assert no_cap["reason"] == "invalid_max_notional"


def test_final_preflight_rejects_invalid_modes_token_remap_and_tick() -> None:
    original = _raw_market(outcomes=2)

    invalid_mode = {**original, "feeRateBps": "not-a-number"}
    result = validate_final_order(
        original_market=invalid_mode,
        fresh_market=invalid_mode,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.59"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert result["ok"] is False
    assert result["reason"] == "market_execution_mode_invalid field=feeRateBps"

    remapped = dict(original)
    remapped["outcomes"] = [
        {**original["outcomes"][0], "name": "NO"},
        {**original["outcomes"][1], "name": "YES"},
    ]
    result = validate_final_order(
        original_market=original,
        fresh_market=remapped,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.59"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert result["ok"] is False
    assert result["reason"] == "outcome_token_changed"

    result = validate_final_order(
        original_market=original,
        fresh_market=original,
        token_id="yes-token",
        side="BUY",
        price=Decimal("0.591"),
        size=Decimal("1"),
        max_notional=Decimal("1"),
    )
    assert result["ok"] is False
    assert result["reason"] == "invalid_price_tick"
