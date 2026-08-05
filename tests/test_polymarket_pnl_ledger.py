from __future__ import annotations

from datetime import datetime, timezone

from platforms.polymarket.maker.pnl_ledger import (
    calculate_realized_pnl,
    fetch_realized_pnl,
    normalized_account_fills,
)


ADDRESS = "0x" + "a" * 40


def test_maker_fill_uses_own_nested_order_not_taker_summary() -> None:
    trades = [
        {
            "id": "trade-1",
            "market": "condition-1",
            "asset_id": "taker-asset",
            "side": "SELL",
            "size": "213.88",
            "price": "0.18",
            "status": "CONFIRMED",
            "match_time": "1785859999",
            "trader_side": "MAKER",
            "maker_orders": [
                {
                    "order_id": "ours",
                    "maker_address": ADDRESS,
                    "asset_id": "our-asset",
                    "side": "SELL",
                    "matched_amount": "28.3",
                    "price": "0.82",
                },
                {
                    "order_id": "someone-else",
                    "maker_address": "0x" + "b" * 40,
                    "asset_id": "other-asset",
                    "side": "BUY",
                    "matched_amount": "185.58",
                    "price": "0.18",
                },
            ],
        }
    ]

    fills = normalized_account_fills(trades, [ADDRESS])

    assert fills == [
        {
            "fill_id": "trade-1:maker:ours",
            "market": "condition-1",
            "asset_id": "our-asset",
            "side": "SELL",
            "size": "28.3",
            "price": "0.82",
            "role": "MAKER",
            "epoch": 1785859999,
            "trade_id": "trade-1",
            "transaction_hash": None,
        }
    ]


def test_fifo_realized_pnl_includes_entry_and_exit_taker_fees() -> None:
    fills = [
        {
            "fill_id": "buy",
            "market": "condition-1",
            "asset_id": "asset-1",
            "side": "BUY",
            "size": "100",
            "price": "0.40",
            "role": "TAKER",
            "epoch": 1785880000,
        },
        {
            "fill_id": "sell-maker",
            "market": "condition-1",
            "asset_id": "asset-1",
            "side": "SELL",
            "size": "60",
            "price": "0.50",
            "role": "MAKER",
            "epoch": 1785880100,
        },
        {
            "fill_id": "sell-taker",
            "market": "condition-1",
            "asset_id": "asset-1",
            "side": "SELL",
            "size": "40",
            "price": "0.60",
            "role": "TAKER",
            "epoch": 1785880200,
        },
    ]

    result = calculate_realized_pnl(
        fills,
        {"condition-1": {"r": 0.04, "e": 1, "to": True}},
        now=datetime.fromtimestamp(1785880300, timezone.utc),
    )

    assert result["status"] == "ok"
    assert result["complete"] is True
    assert result["fees_usd"] == 1.344
    assert result["realized_pnl_usd"] == 12.656
    assert result["realized_pnl_today_utc_usd"] == 12.656
    assert result["open_inventory_size"] == 0
    older, newer = reversed(result["realized_exits"])
    assert older["gross_pnl_usd"] == 6.0
    assert older["entry_fee_usd"] == 0.576
    assert older["exit_fee_usd"] == 0.0
    assert older["net_pnl_usd"] == 5.424
    assert newer["gross_pnl_usd"] == 8.0
    assert newer["entry_fee_usd"] == 0.384
    assert newer["exit_fee_usd"] == 0.384
    assert newer["net_pnl_usd"] == 7.232


def test_unmatched_sell_is_never_reported_as_zero_pnl() -> None:
    result = calculate_realized_pnl(
        [
            {
                "fill_id": "sell-only",
                "market": "fee-free",
                "asset_id": "asset-1",
                "side": "SELL",
                "size": "10",
                "price": "0.80",
                "role": "MAKER",
                "epoch": 1785880000,
            }
        ],
        {},
    )

    assert result["status"] == "partial"
    assert result["complete"] is False
    assert result["unmatched_sell_count"] == 1
    assert result["realized_exits"][0]["net_pnl_usd"] is None
    assert result["realized_exits"][0]["status"] == "needs_review"


def test_missing_taker_fee_parameters_make_exit_unverified() -> None:
    result = calculate_realized_pnl(
        [
            {
                "fill_id": "buy",
                "market": "unknown-fee",
                "asset_id": "asset-1",
                "side": "BUY",
                "size": "10",
                "price": "0.40",
                "role": "TAKER",
                "epoch": 1785880000,
            },
            {
                "fill_id": "sell",
                "market": "unknown-fee",
                "asset_id": "asset-1",
                "side": "SELL",
                "size": "10",
                "price": "0.50",
                "role": "MAKER",
                "epoch": 1785880100,
            },
        ],
        {"unknown-fee": "unavailable"},
    )

    assert result["fee_unverified_count"] == 1
    assert result["realized_exits"][0]["net_pnl_usd"] is None


def test_fetch_queries_market_fee_details_only_for_taker_fills() -> None:
    class Client:
        def __init__(self) -> None:
            self.markets = []

        def get_trades(self):
            return [
                {
                    "id": "buy",
                    "market": "condition-1",
                    "asset_id": "asset-1",
                    "side": "BUY",
                    "size": "10",
                    "price": "0.40",
                    "status": "CONFIRMED",
                    "match_time": "1785880000",
                    "trader_side": "TAKER",
                },
                {
                    "id": "sell",
                    "market": "condition-1",
                    "status": "CONFIRMED",
                    "match_time": "1785880100",
                    "trader_side": "MAKER",
                    "maker_orders": [
                        {
                            "order_id": "ours",
                            "maker_address": ADDRESS,
                            "asset_id": "asset-1",
                            "side": "SELL",
                            "matched_amount": "10",
                            "price": "0.50",
                        }
                    ],
                },
            ]

        def get_clob_market_info(self, market):
            self.markets.append(market)
            return {"fd": {"r": 0.04, "e": 1, "to": True}}

    client = Client()
    result = fetch_realized_pnl(
        client,
        [ADDRESS],
        now=datetime.fromtimestamp(1785880200, timezone.utc),
    )

    assert client.markets == ["condition-1"]
    assert result["complete"] is True
    assert result["realized_pnl_usd"] == 0.904
