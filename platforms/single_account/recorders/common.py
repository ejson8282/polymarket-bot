"""施工包01 · recorders 公共基建。

数据库/日志/心跳/退避/Decibel 只读客户端。安全约定:
- 只访问公开只读行情端点;鉴权 key 仅从环境变量 DECIBEL_API_BEARER 读取,
  绝不读 .env、绝不写入日志或异常信息。
- 所有落库使用 INSERT OR REPLACE,主键防重,进程重启不产生重复行。
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import requests

REPO_ROOT = Path(__file__).resolve().parents[3]
RECORDERS_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = RECORDERS_DIR / "config.json"
SCHEMA_PATH = RECORDERS_DIR / "schema_market.sql"
DATA_DIR = REPO_ROOT / "data"
LOGS_DIR = REPO_ROOT / "logs"
MARKET_DB_PATH = DATA_DIR / "single_account_market.db"


def load_config(path: Optional[Path] = None) -> dict[str, Any]:
    config_path = path or DEFAULT_CONFIG_PATH
    return json.loads(config_path.read_text(encoding="utf-8"))


def open_market_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = db_path or MARKET_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0)
    # 三个 recorder 进程共享同一个库,WAL + busy_timeout 避免写锁冲突。
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    conn.commit()
    return conn


def upsert_klines(conn: sqlite3.Connection, rows: Iterable[tuple]) -> int:
    cur = conn.executemany(
        "INSERT OR REPLACE INTO klines(venue, symbol, tf, open_ts, open, high, low, close, volume) "
        "VALUES(?,?,?,?,?,?,?,?,?)",
        list(rows),
    )
    conn.commit()
    return cur.rowcount


def upsert_funding(conn: sqlite3.Connection, rows: Iterable[tuple]) -> int:
    cur = conn.executemany(
        "INSERT OR REPLACE INTO funding(venue, symbol, ts, rate, interval_hours, predicted_next) "
        "VALUES(?,?,?,?,?,?)",
        list(rows),
    )
    conn.commit()
    return cur.rowcount


def insert_basis_tick(conn: sqlite3.Connection, row: tuple) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO basis_ticks(ts, venue, symbol, platform_mark, platform_bid, "
        "platform_ask, platform_index, ref_price, ref_ts, ref_source) VALUES(?,?,?,?,?,?,?,?,?,?)",
        row,
    )
    conn.commit()


class JsonlLogger:
    """JSON 行日志:{ts, name, event, detail}。"""

    def __init__(self, name: str, log_dir: Optional[Path] = None) -> None:
        self.name = name
        directory = log_dir or LOGS_DIR
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / f"recorder_{name}.jsonl"

    def log(self, event: str, **detail: Any) -> None:
        payload = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "name": self.name,
            "event": event,
            "detail": detail,
        }
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def touch_heartbeat(name: str, data_dir: Optional[Path] = None) -> Path:
    directory = data_dir or DATA_DIR
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f".recorder_{name}.heartbeat"
    path.touch()
    os.utime(path, None)
    return path


class Backoff:
    """退避序列 1→2→4→…封顶 cap;成功后 reset。"""

    def __init__(self, base: float = 1.0, cap: float = 60.0) -> None:
        self.base = base
        self.cap = cap
        self._current = base

    def next(self) -> float:
        value = self._current
        self._current = min(self._current * 2.0, self.cap)
        return value

    def reset(self) -> None:
        self._current = self.base


class RecorderHTTPError(Exception):
    def __init__(self, status: int, url_path: str, body_snippet: str) -> None:
        super().__init__(f"HTTP {status} on {url_path}: {body_snippet}")
        self.status = status


class DecibelPublicClient:
    """Decibel 公开行情只读客户端。

    端点与字段兼容逻辑参考自只读仓库 varia-decibel-farming 的
    src/exchanges/decibel.py(仅复制最小必要部分),端点参数以
    docs.decibel.trade 官方文档为准,见同目录 ENDPOINTS.md。
    """

    def __init__(self, base_url: str, timeout: float = 15.0,
                 session: Optional[requests.Session] = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self._markets_cache: Optional[list[dict[str, Any]]] = None

    def _headers(self) -> dict[str, str]:
        headers = {"accept": "application/json"}
        bearer = os.getenv("DECIBEL_API_BEARER", "").strip()
        if bearer:
            headers["authorization"] = f"Bearer {bearer}"
            headers["x-api-key"] = bearer
        origin = os.getenv("DECIBEL_ORIGIN", "").strip()
        if origin:
            headers["origin"] = origin
        return headers

    def _get(self, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, params=params, headers=self._headers(), timeout=self.timeout)
        if resp.status_code != 200:
            raise RecorderHTTPError(resp.status_code, path, resp.text[:200])
        return resp.json()

    def get_markets(self) -> list[dict[str, Any]]:
        if self._markets_cache is not None:
            return self._markets_cache
        data = self._get("/markets")
        markets: Optional[list[dict[str, Any]]] = None
        if isinstance(data, list):
            markets = data
        elif isinstance(data, dict):
            for key in ("markets", "data", "result"):
                value = data.get(key)
                if isinstance(value, list):
                    markets = value
                    break
        if markets is None:
            raise RecorderHTTPError(200, "/markets", "unexpected response shape")
        self._markets_cache = markets
        return markets

    def get_prices(self) -> Any:
        return self._get("/prices")

    def get_candlesticks(self, market_addr: str, interval: str,
                         start_ms: int, end_ms: int) -> list[dict[str, Any]]:
        data = self._get("/candlesticks", params={
            "market": market_addr,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        })
        return data if isinstance(data, list) else []

    def get_orderbook(self, ticker_id: str) -> dict[str, Any]:
        data = self._get("/orderbook", params={"ticker_id": ticker_id})
        return data if isinstance(data, dict) else {}

    @staticmethod
    def symbol_names(symbol: str) -> set:
        upper = symbol.upper()
        return {upper, f"{upper}-USD", f"{upper}/USD", f"{upper}-PERP"}

    def market_addr_for(self, symbol: str) -> str:
        wanted = self.symbol_names(symbol)
        for item in self.get_markets():
            if not isinstance(item, dict):
                continue
            names = {str(item.get(k, "")) for k in ("symbol", "market", "marketName", "market_name", "name")}
            if wanted & names:
                return str(item.get("market_addr") or item.get("market") or "")
        return ""

    def find_price_item(self, prices: Any, symbol: str, market_addr: str = "") -> dict[str, Any]:
        rows = prices if isinstance(prices, list) else []
        if isinstance(prices, dict):
            for key in ("prices", "data", "result"):
                value = prices.get(key)
                if isinstance(value, list):
                    rows = value
                    break
        wanted = self.symbol_names(symbol)
        for item in rows:
            if not isinstance(item, dict):
                continue
            names = {str(item.get(k, "")) for k in ("symbol", "market", "marketName", "market_name", "name")}
            if (wanted & names) or (market_addr and str(item.get("market") or item.get("market_addr") or "") == market_addr):
                return item
        return {}


def run_recorder_loop(name: str, iterate: Callable[[], None], poll_sec: float,
                      logger: JsonlLogger,
                      should_stop: Callable[[], bool] = lambda: False,
                      sleep: Callable[[float], None] = time.sleep) -> None:
    """通用主循环:每轮 touch 心跳;异常→日志→退避重试,永不退出。"""
    backoff = Backoff()
    while not should_stop():
        touch_heartbeat(name)
        try:
            iterate()
        except Exception as exc:  # noqa: BLE001 —— 规格要求捕获一切异常持续运行
            delay = backoff.next()
            logger.log("error", error=type(exc).__name__, message=str(exc)[:300], retry_in_sec=delay)
            sleep(delay)
            continue
        backoff.reset()
        sleep(poll_sec)
