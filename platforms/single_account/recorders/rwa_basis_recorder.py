"""施工包01 · §A4 RWA basis 记录器(本包最重要的一个)。

每 sample_sec 采一行 basis_ticks:platform mark/bid/ask/index(mark/index 自
/prices,bid/ask 自 /orderbook 顶档)+ ref 价(RefAdapter)。
- platform 个别字段缺失 → 填 NULL,不丢整行;platform 请求整体失败 → 抛异常
  由主循环退避(无 key 时即此路径,不落行)。
- ref 抓不到 → RefAdapter 抛异常,同样由主循环退避(规格约定)。
- session="rth" 品种只在美东 09:30–16:00(周一至五)采样;节假日不处理,TODO(下一批)。
"""
from __future__ import annotations

import argparse
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Tuple
from zoneinfo import ZoneInfo

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from platforms.single_account.recorders.common import (
    DEFAULT_CONFIG_PATH,
    DecibelPublicClient,
    JsonlLogger,
    insert_basis_tick,
    load_config,
    open_market_db,
    run_recorder_loop,
)

NY_TZ = ZoneInfo("America/New_York")


class RefAdapter:
    """参考价适配器接口(规格 §A4 原文,Protocol 语义)。"""

    def get_price(self, ref_symbol: str) -> Tuple[float, int, str]:
        """返回 (price, price_ts_epoch, source_tag)。抓不到时抛异常,由主循环退避。"""
        raise NotImplementedError


class YFinanceDelayed(RefAdapter):
    """默认实现:yfinance 最新价。明确是【延迟数据】,足够测 basis 水平/分布,
    不足以测秒级滞后;price_ts 为抓取时刻(yfinance fast_info 不含行情时间戳)。"""

    source_tag = "yfinance_delayed"

    def __init__(self) -> None:
        import yfinance  # 延迟导入,避免其他 recorder/测试依赖它

        self._yf = yfinance
        self._tickers: dict = {}

    def get_price(self, ref_symbol: str) -> Tuple[float, int, str]:
        ticker = self._tickers.get(ref_symbol)
        if ticker is None:
            ticker = self._yf.Ticker(ref_symbol)
            self._tickers[ref_symbol] = ticker
        info = ticker.fast_info
        price = None
        for key in ("last_price", "lastPrice"):
            try:
                price = info[key]
            except (KeyError, TypeError):
                price = getattr(info, key, None)
            if price is not None:
                break
        if price is None:
            raise RuntimeError(f"yfinance no last_price for {ref_symbol}")
        return float(price), int(time.time()), self.source_tag


class AlpacaIEX(RefAdapter):
    """TODO(本批次不实现,防止引入新凭证管理问题):
    仅当环境变量 ALPACA_API_KEY_ID / ALPACA_API_SECRET_KEY 存在时可选启用,
    用 Alpaca IEX 实时行情替代 yfinance 延迟价,以支持秒级滞后估计(下一批)。"""

    source_tag = "alpaca_iex"

    def get_price(self, ref_symbol: str) -> Tuple[float, int, str]:
        raise NotImplementedError("AlpacaIEX 适配器留待下一批实现(施工包01 §A4)")


def make_ref_adapter(name: str) -> RefAdapter:
    if name == "yfinance_delayed":
        return YFinanceDelayed()
    raise ValueError(f"unknown ref_adapter: {name}")


def is_rth(now_utc: datetime) -> bool:
    """美东常规交易时段判断:周一至五 09:30–16:00(America/New_York)。
    交易所节假日本批次不处理(TODO 下一批)。"""
    local = now_utc.astimezone(NY_TZ)
    if local.weekday() >= 5:
        return False
    minutes = local.hour * 60 + local.minute
    return 9 * 60 + 30 <= minutes < 16 * 60


def should_sample(session: str, now_utc: datetime) -> bool:
    return session != "rth" or is_rth(now_utc)


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def best_bid_ask(book: dict) -> Tuple[Optional[float], Optional[float]]:
    def first_price(levels: Any) -> Optional[float]:
        if isinstance(levels, list) and levels and isinstance(levels[0], (list, tuple)) and levels[0]:
            return _num(levels[0][0])
        return None

    return first_price(book.get("bids")), first_price(book.get("asks"))


class RwaBasisRecorder:
    def __init__(self, client: DecibelPublicClient, conn, cfg: dict,
                 logger: JsonlLogger, ref_adapter: Optional[RefAdapter] = None,
                 now_fn=time.time) -> None:
        self.client = client
        self.conn = conn
        self.logger = logger
        self.now_fn = now_fn
        self.venue = str(cfg.get("venue") or "decibel")
        basis_cfg = cfg.get("basis") or {}
        self.symbols = list(basis_cfg.get("symbols") or [])
        self.sample_sec = float(basis_cfg.get("sample_sec") or 5)
        self.ref = ref_adapter or make_ref_adapter(str(basis_cfg.get("ref_adapter") or "yfinance_delayed"))
        self._addr_cache: dict = {}
        self._pool = ThreadPoolExecutor(max_workers=8)

    def _addr(self, symbol: str) -> str:
        addr = self._addr_cache.get(symbol)
        if addr is None:
            try:
                addr = self.client.market_addr_for(symbol)
            except Exception:
                addr = ""
            self._addr_cache[symbol] = addr
        return addr

    def iterate(self) -> None:
        now_utc = datetime.now(timezone.utc)
        active = [s for s in self.symbols if should_sample(str(s.get("session") or "24h"), now_utc)]
        if not active:
            self.logger.log("idle", reason="all symbols outside session")
            return
        prices = self.client.get_prices()  # 一轮一次,失败→主循环退避
        # orderbook 与 ref 价并行取(§A4「并行取」)
        book_futs = {s["platform"]: self._pool.submit(self.client.get_orderbook, f"{s['platform']}-PERP")
                     for s in active}
        ref_futs = {s["platform"]: self._pool.submit(self.ref.get_price, str(s["ref"])) for s in active}
        ts = int(self.now_fn())
        written = 0
        for sym_cfg in active:
            platform_sym = str(sym_cfg["platform"])
            ref_price, ref_ts, ref_source = ref_futs[platform_sym].result()  # ref 失败→抛→退避
            item = self.client.find_price_item(prices, platform_sym, market_addr=self._addr(platform_sym))
            mark = _num(item.get("mark_px")) if item else None
            index = _num(item.get("oracle_px")) if item else None
            try:
                bid, ask = best_bid_ask(book_futs[platform_sym].result())
            except Exception:
                bid, ask = None, None  # platform 个别字段缺失填 NULL,不丢整行
            insert_basis_tick(self.conn, (
                ts, self.venue, platform_sym, mark, bid, ask, index,
                float(ref_price), int(ref_ts), str(ref_source),
            ))
            written += 1
        self.logger.log("sample", rows=written, ts=ts)


_STOP = False


def _handle_stop(signum: int, frame: object) -> None:
    global _STOP
    _STOP = True


def main() -> None:
    parser = argparse.ArgumentParser(description="SA RWA basis recorder (paper/read-only).")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--once", action="store_true", help="只跑一轮(人工验证用)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    cfg = load_config(Path(args.config).resolve())
    logger = JsonlLogger("basis")
    conn = open_market_db()
    client = DecibelPublicClient(str(cfg.get("venue_base_url") or ""))
    recorder = RwaBasisRecorder(client, conn, cfg, logger)
    logger.log("start", symbols=[s.get("platform") for s in recorder.symbols])

    if args.once:
        recorder.iterate()
        return
    run_recorder_loop("basis", recorder.iterate, recorder.sample_sec, logger,
                      should_stop=lambda: _STOP)


if __name__ == "__main__":
    main()
