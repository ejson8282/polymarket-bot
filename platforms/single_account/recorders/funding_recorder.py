"""施工包01 · §A3 资金费率记录器。

每 poll_sec 拉 /prices,把当前费率按结算周期起点落库(一周期一行,upsert)。
市场级历史 funding 端点不存在(见 ENDPOINTS.md),不做 30 天回补。
换算口径:rate = funding_rate_bps/10000(小时费率,符号由 is_funding_positive),
interval_hours = funding_period_s/3600,predicted_next = NULL(端点不提供)。
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
    upsert_funding,
)

DEFAULT_PERIOD_S = 3600  # /prices 的 funding_rate_bps 是小时费率,缺 funding_period_s 时按小时对齐


def period_start_ts(epoch_sec: float, period_s: int) -> int:
    """把时间戳向下对齐到结算周期起点(§A3:ts 用结算周期时间戳,非抓取时间)。"""
    period = max(int(period_s), 1)
    return (int(epoch_sec) // period) * period


def parse_funding_row(item: dict, venue: str, symbol: str,
                      fallback_now: float) -> Optional[tuple]:
    bps = _num(item.get("funding_rate_bps"))
    if bps is None:
        return None
    positive = item.get("is_funding_positive")
    if isinstance(positive, bool):
        rate = abs(bps) / 10000.0 * (1.0 if positive else -1.0)
    else:
        rate = bps / 10000.0
    period_s = int(_num(item.get("funding_period_s")) or DEFAULT_PERIOD_S)
    tx_ms = _num(item.get("transaction_unix_ms"))
    ref_sec = (tx_ms / 1000.0) if tx_ms else fallback_now
    ts = period_start_ts(ref_sec, period_s)
    return (venue, symbol, ts, rate, period_s / 3600.0, None)


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class FundingRecorder:
    def __init__(self, client: DecibelPublicClient, conn, cfg: dict,
                 logger: JsonlLogger, now_fn=time.time) -> None:
        self.client = client
        self.conn = conn
        self.logger = logger
        self.now_fn = now_fn
        self.venue = str(cfg.get("venue") or "decibel")
        funding_cfg = cfg.get("funding") or {}
        self.symbols = list(funding_cfg.get("symbols") or [])
        self.poll_sec = float(funding_cfg.get("poll_sec") or 300)
        self._addr_cache: dict = {}

    def iterate(self) -> None:
        prices = self.client.get_prices()
        now = self.now_fn()
        rows = []
        misses = []
        for symbol in self.symbols:
            addr = self._addr_cache.get(symbol)
            if addr is None:
                try:
                    addr = self.client.market_addr_for(symbol)
                except Exception:
                    addr = ""
                self._addr_cache[symbol] = addr
            item = self.client.find_price_item(prices, symbol, market_addr=addr)
            row = parse_funding_row(item, self.venue, symbol, now) if item else None
            if row is None:
                misses.append(symbol)
            else:
                rows.append(row)
        if rows:
            upsert_funding(self.conn, rows)
        self.logger.log("poll", rows=len(rows), misses=misses)


_STOP = False


def _handle_stop(signum: int, frame: object) -> None:
    global _STOP
    _STOP = True


def main() -> None:
    parser = argparse.ArgumentParser(description="SA funding recorder (paper/read-only).")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--once", action="store_true", help="只跑一轮(人工验证用)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    cfg = load_config(Path(args.config).resolve())
    logger = JsonlLogger("funding")
    conn = open_market_db()
    client = DecibelPublicClient(str(cfg.get("venue_base_url") or ""))
    recorder = FundingRecorder(client, conn, cfg, logger)
    logger.log("start", symbols=recorder.symbols)

    if args.once:
        recorder.iterate()
        return
    run_recorder_loop("funding", recorder.iterate, recorder.poll_sec, logger,
                      should_stop=lambda: _STOP)


if __name__ == "__main__":
    main()
