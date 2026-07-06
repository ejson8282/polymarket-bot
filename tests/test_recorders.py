"""施工包01 · §A6 recorder 单测(全 mock 网络,不需要 API key)。"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from platforms.single_account.recorders.basis_report import compute_report, p95
from platforms.single_account.recorders.common import (
    Backoff,
    DecibelPublicClient,
    open_market_db,
    upsert_klines,
)
from platforms.single_account.recorders.funding_recorder import (
    FundingRecorder,
    parse_funding_row,
    period_start_ts,
)
from platforms.single_account.recorders.kline_recorder import KlineRecorder
from platforms.single_account.recorders.rwa_basis_recorder import (
    RwaBasisRecorder,
    best_bid_ask,
    is_rth,
    should_sample,
)

NOW = 1_760_000_000  # 任意固定 epoch


def _db(tmp_path):
    return open_market_db(tmp_path / "market.db")


def _client(**overrides):
    client = MagicMock()
    client.market_addr_for.return_value = "0xaddr"
    client.find_price_item = DecibelPublicClient.find_price_item.__get__(client)
    for key, value in overrides.items():
        setattr(client, key, value)
    return client


# ---------- upsert 幂等(§A6:同一根K线写两次仍一行) ----------

def test_kline_upsert_idempotent(tmp_path):
    conn = _db(tmp_path)
    row = ("decibel", "BTC", "1m", 60, 1.0, 2.0, 0.5, 1.5, 10.0)
    upsert_klines(conn, [row])
    upsert_klines(conn, [row])
    assert conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0] == 1
    # REPLACE 更新未收盘那根:同主键新值覆盖
    upsert_klines(conn, [("decibel", "BTC", "1m", 60, 1.0, 2.5, 0.5, 1.8, 12.0)])
    count, close = conn.execute("SELECT COUNT(*), MAX(close) FROM klines").fetchone()
    assert (count, close) == (1, 1.8)


def test_funding_upsert_same_period_single_row(tmp_path):
    conn = _db(tmp_path)
    client = _client()
    client.get_prices.return_value = [{
        "market": "0xaddr", "symbol": "BTC/USD", "funding_rate_bps": 2.0,
        "is_funding_positive": True, "funding_period_s": 3600,
        "transaction_unix_ms": NOW * 1000,
    }]
    cfg = {"venue": "decibel", "funding": {"symbols": ["BTC"], "poll_sec": 300}}
    rec = FundingRecorder(client, conn, cfg, MagicMock(), now_fn=lambda: NOW)
    rec.iterate()
    rec.iterate()  # 同一结算周期第二轮 → upsert 同一行
    rows = conn.execute("SELECT ts, rate, interval_hours, predicted_next FROM funding").fetchall()
    assert len(rows) == 1
    ts, rate, hours, predicted = rows[0]
    assert ts % 3600 == 0 and rate == 0.0002 and hours == 1.0 and predicted is None


def test_basis_tick_insert_and_nulls(tmp_path):
    conn = _db(tmp_path)
    client = _client()
    client.get_prices.return_value = [{"market": "0xaddr", "symbol": "XAU/USD",
                                       "mark_px": 100.2, "oracle_px": 100.1}]
    client.get_orderbook.side_effect = RuntimeError("book down")  # 个别字段缺失→NULL 不丢行
    cfg = {"venue": "decibel", "basis": {"symbols": [
        {"platform": "XAU", "ref": "GC=F", "session": "24h"}], "sample_sec": 5}}
    ref = MagicMock()
    ref.get_price.return_value = (100.0, NOW, "yfinance_delayed")
    rec = RwaBasisRecorder(client, conn, cfg, MagicMock(), ref_adapter=ref, now_fn=lambda: NOW)
    rec.iterate()
    row = conn.execute("SELECT platform_mark, platform_bid, platform_ask, platform_index, "
                       "ref_price, ref_source FROM basis_ticks").fetchone()
    assert row == (100.2, None, None, 100.1, 100.0, "yfinance_delayed")


# ---------- RTH 判断(§A6:给定美东时间用例) ----------

def test_rth_weekday_open_close_boundaries():
    # 2026-07-06 是周一;夏令时 ET=UTC-4
    assert is_rth(datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc))       # 09:30 开盘含
    assert not is_rth(datetime(2026, 7, 6, 13, 29, tzinfo=timezone.utc))   # 09:29 盘前
    assert is_rth(datetime(2026, 7, 6, 19, 59, tzinfo=timezone.utc))       # 15:59
    assert not is_rth(datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc))    # 16:00 收盘不含
    # 冬令时(EST=UTC-5):2026-01-05 周一,15:00 UTC = 10:00 ET
    assert is_rth(datetime(2026, 1, 5, 15, 0, tzinfo=timezone.utc))
    assert not is_rth(datetime(2026, 1, 5, 14, 29, tzinfo=timezone.utc))   # 09:29 EST


def test_rth_weekend_and_sessions():
    saturday = datetime(2026, 7, 4, 15, 0, tzinfo=timezone.utc)
    assert not is_rth(saturday)
    assert not should_sample("rth", saturday)
    assert should_sample("24h", saturday)


# ---------- 退避序列(§A6:1,2,4,…,60 封顶) ----------

def test_backoff_sequence_caps_at_60():
    backoff = Backoff()
    seq = [backoff.next() for _ in range(8)]
    assert seq == [1, 2, 4, 8, 16, 32, 60, 60]
    backoff.reset()
    assert backoff.next() == 1


# ---------- funding 换算与周期对齐 ----------

def test_period_start_ts_alignment():
    assert period_start_ts(NOW + 3599, 3600) == (NOW + 3599) // 3600 * 3600
    assert period_start_ts(28800 * 5 + 1, 28800) == 28800 * 5


def test_parse_funding_row_sign_and_interval():
    row = parse_funding_row({"funding_rate_bps": 1.5, "is_funding_positive": False,
                             "funding_period_s": 28800,
                             "transaction_unix_ms": NOW * 1000}, "decibel", "BTC", 0)
    venue, symbol, ts, rate, hours, predicted = row
    assert (venue, symbol) == ("decibel", "BTC")
    assert rate == -0.00015 and hours == 8.0 and predicted is None and ts % 28800 == 0
    assert parse_funding_row({"mark_px": 1.0}, "decibel", "BTC", 0) is None  # 缺费率→跳过


# ---------- kline 分页回补(mock 分页响应)+ 重启不重复 ----------

def test_kline_backfill_pagination_and_restart(tmp_path):
    conn = _db(tmp_path)
    tf_ms = 60_000
    start = (NOW - 2000 * 60) * 1000
    page1 = [{"t": start + i * tf_ms, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}
             for i in range(1000)]
    page2 = [{"t": page1[-1]["t"] + tf_ms + i * tf_ms, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 10}
             for i in range(5)]
    client = _client()
    client.get_candlesticks.side_effect = [page1, page2]
    cfg = {"venue": "decibel",
           "kline": {"symbols": ["BTC"], "timeframes": ["1m"], "poll_sec": 60, "backfill_days": 2}}
    rec = KlineRecorder(client, conn, cfg, MagicMock(), now_fn=lambda: NOW)
    assert rec.backfill("BTC", "1m") == 1005
    assert conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0] == 1005
    # 第二页请求应从第一页最新 open_ts 的下一根开始
    second_call = client.get_candlesticks.call_args_list[1]
    assert second_call.args[2] == page1[-1]["t"] + tf_ms

    # 模拟重启:新实例从库中最大 open_ts 续,端点返回重叠数据 → 行数不变
    client.get_candlesticks.side_effect = [page2[-3:]]
    rec2 = KlineRecorder(client, conn, cfg, MagicMock(), now_fn=lambda: NOW)
    rec2.backfill("BTC", "1m")
    assert conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0] == 1005


# ---------- basis_report 统计 ----------

def test_basis_report_stats(tmp_path):
    conn = _db(tmp_path)
    for i, mark in enumerate([100.1, 100.2, 100.3]):
        conn.execute("INSERT INTO basis_ticks VALUES(?,?,?,?,?,?,?,?,?,?)",
                     (NOW + i, "decibel", "XAU", mark, None, None, None,
                      100.0, NOW + i, "yfinance_delayed"))
    conn.commit()
    report = compute_report(conn, days=1, now=NOW + 10)
    stats = report["XAU"]
    assert stats["samples"] == 3
    assert abs(stats["basis_bps"]["mean"] - 20.0) < 1e-6
    assert abs(stats["basis_bps"]["median"] - 20.0) < 1e-6
    assert stats["ref_source_share"] == {"yfinance_delayed": 1.0}
    assert p95([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) == 10
