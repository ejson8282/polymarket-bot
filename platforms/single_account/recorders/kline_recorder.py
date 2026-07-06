"""施工包01 · §A2 K线记录器。

启动时对每个 (symbol, tf) 从库中最大 open_ts 回补到当前(/candlesticks 支持
startTime/endTime 分页,单页≤1000);之后每 poll_sec 拉最近 3 根 upsert。
未收盘K线按 ENDPOINTS.md 的选择「存且持续更新最后一根」。
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path
from typing import Any, Optional

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.single_account.recorders.common import (
    DEFAULT_CONFIG_PATH,
    DecibelPublicClient,
    JsonlLogger,
    load_config,
    open_market_db,
    run_recorder_loop,
    upsert_klines,
)

TF_SECONDS = {
    "1m": 60, "5m": 300, "15m": 900, "30m": 1800,
    "1h": 3600, "2h": 7200, "4h": 14400, "8h": 28800,
    "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800,
}
PAGE_LIMIT = 1000
POLL_LAST_N = 3  # 覆盖未收盘那根的更新(§A2)


def parse_candle_rows(items: list, venue: str, symbol: str, tf: str) -> list:
    rows = []
    for item in items:
        if not isinstance(item, dict) or "t" not in item:
            continue
        rows.append((
            venue, symbol, tf, int(item["t"]) // 1000,
            _num(item.get("o")), _num(item.get("h")), _num(item.get("l")),
            _num(item.get("c")), _num(item.get("v")),
        ))
    return rows


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class KlineRecorder:
    def __init__(self, client: DecibelPublicClient, conn, cfg: dict,
                 logger: JsonlLogger, now_fn=time.time) -> None:
        self.client = client
        self.conn = conn
        self.logger = logger
        self.now_fn = now_fn
        self.venue = str(cfg.get("venue") or "decibel")
        kline_cfg = cfg.get("kline") or {}
        self.symbols = list(kline_cfg.get("symbols") or [])
        self.timeframes = list(kline_cfg.get("timeframes") or [])
        self.poll_sec = float(kline_cfg.get("poll_sec") or 60)
        self.backfill_days = float(kline_cfg.get("backfill_days") or 7)
        self._backfilled: set = set()
        self._addr_cache: dict = {}

    def _market_addr(self, symbol: str) -> str:
        addr = self._addr_cache.get(symbol)
        if not addr:
            addr = self.client.market_addr_for(symbol)
            if not addr:
                raise RuntimeError(f"market_addr not found for {symbol}")
            self._addr_cache[symbol] = addr
        return addr

    def _max_open_ts(self, symbol: str, tf: str) -> Optional[int]:
        row = self.conn.execute(
            "SELECT MAX(open_ts) FROM klines WHERE venue=? AND symbol=? AND tf=?",
            (self.venue, symbol, tf),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def backfill(self, symbol: str, tf: str) -> int:
        tf_sec = TF_SECONDS[tf]
        now_ms = int(self.now_fn() * 1000)
        last = self._max_open_ts(symbol, tf)
        start_ms = (last * 1000) if last is not None else now_ms - int(self.backfill_days * 86400 * 1000)
        addr = self._market_addr(symbol)
        total = 0
        while start_ms < now_ms:
            items = self.client.get_candlesticks(addr, tf, start_ms, now_ms)
            rows = parse_candle_rows(items, self.venue, symbol, tf)
            if not rows:
                break
            total += len(rows)
            upsert_klines(self.conn, rows)
            newest_ms = max(int(item["t"]) for item in items if isinstance(item, dict) and "t" in item)
            next_start = newest_ms + tf_sec * 1000
            if len(items) < PAGE_LIMIT or next_start <= start_ms:
                break
            start_ms = next_start
        return total

    def poll_pair(self, symbol: str, tf: str) -> int:
        tf_sec = TF_SECONDS[tf]
        now_ms = int(self.now_fn() * 1000)
        start_ms = now_ms - POLL_LAST_N * tf_sec * 1000
        items = self.client.get_candlesticks(self._market_addr(symbol), tf, start_ms, now_ms)
        rows = parse_candle_rows(items, self.venue, symbol, tf)
        if rows:
            upsert_klines(self.conn, rows)
        return len(rows)

    def iterate(self) -> None:
        for symbol in self.symbols:
            for tf in self.timeframes:
                key = (symbol, tf)
                if key not in self._backfilled:
                    n = self.backfill(symbol, tf)
                    self._backfilled.add(key)
                    self.logger.log("backfill", symbol=symbol, tf=tf, rows=n)
                else:
                    self.poll_pair(symbol, tf)
        self.logger.log("poll", pairs=len(self.symbols) * len(self.timeframes))


_STOP = False


def _handle_stop(signum: int, frame: object) -> None:
    global _STOP
    _STOP = True


def main() -> None:
    parser = argparse.ArgumentParser(description="SA kline recorder (paper/read-only).")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--once", action="store_true", help="只跑一轮(人工验证用)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    cfg = load_config(Path(args.config).resolve())
    logger = JsonlLogger("kline")
    conn = open_market_db()
    client = DecibelPublicClient(str(cfg.get("venue_base_url") or ""))
    recorder = KlineRecorder(client, conn, cfg, logger)
    logger.log("start", symbols=recorder.symbols, timeframes=recorder.timeframes)

    if args.once:
        recorder.iterate()
        return
    run_recorder_loop("kline", recorder.iterate, recorder.poll_sec, logger,
                      should_stop=lambda: _STOP)


if __name__ == "__main__":
    main()
