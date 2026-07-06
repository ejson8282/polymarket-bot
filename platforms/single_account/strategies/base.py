"""施工包02 · §1 策略统一接口。"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol

import pandas as pd

from platforms.single_account.sim.orders import Bar
from platforms.single_account.sim.position import Position

try:
    from typing import Literal

    Action = Literal["open_long", "open_short", "close"]
except ImportError:  # pragma: no cover
    Action = str  # type: ignore[misc]


@dataclass
class Signal:
    strategy: str
    symbol: str
    action: "Action"
    qty: float                      # 由策略按风险预算算好
    stop_price: Optional[float]
    tp_price: Optional[float]
    reason: str                     # 人话一句:入场依据快照
    tags: Dict[str, Any] = field(default_factory=dict)
    limit_price: Optional[float] = None  # 非空=限价入场(§2.3 下轨挂限价;§1 兼容扩展)


class Strategy(Protocol):
    name: str

    def on_bar(self, ctx: "Context") -> List[Signal]: ...


def risk_qty(risk_pct: float, equity: float, entry_price: float, stop_price: float,
             notional_cap_pct: Optional[float] = None) -> float:
    """§1 风险预算统一函数:qty = risk_pct × equity / |entry − stop|;
    可选名义上限:qty×entry ≤ notional_cap_pct × equity。"""
    dist = abs(entry_price - stop_price)
    if dist <= 0 or equity <= 0:
        return 0.0
    qty = risk_pct * equity / dist
    if notional_cap_pct is not None and entry_price > 0:
        qty = min(qty, notional_cap_pct * equity / entry_price)
    return max(qty, 0.0)


class MarketData:
    """策略侧只读数据访问(market.db:klines/funding/basis_ticks)。"""

    def __init__(self, conn: sqlite3.Connection, venue: str = "decibel") -> None:
        self.conn = conn
        self.venue = venue

    def funding_history(self, symbol: str, end_ts: int, days: float = 30.0) -> pd.DataFrame:
        rows = self.conn.execute(
            "SELECT ts, rate, interval_hours FROM funding "
            "WHERE venue=? AND symbol=? AND ts>? AND ts<=? ORDER BY ts",
            (self.venue, symbol, int(end_ts - days * 86400), int(end_ts))).fetchall()
        return pd.DataFrame(rows, columns=["ts", "rate", "interval_hours"])

    def basis_ticks(self, symbol: str, start_ts: int, end_ts: int) -> pd.DataFrame:
        rows = self.conn.execute(
            "SELECT ts, platform_mark, platform_bid, platform_ask, ref_price, ref_ts "
            "FROM basis_ticks WHERE venue=? AND symbol=? AND ts>? AND ts<=? ORDER BY ts",
            (self.venue, symbol, int(start_ts), int(end_ts))).fetchall()
        return pd.DataFrame(rows, columns=["ts", "platform_mark", "platform_bid",
                                           "platform_ask", "ref_price", "ref_ts"])

    def basis_data_span_days(self, symbol: str, end_ts: int) -> float:
        row = self.conn.execute(
            "SELECT MIN(ts), MAX(ts) FROM basis_ticks WHERE venue=? AND symbol=? AND ts<=?",
            (self.venue, symbol, int(end_ts))).fetchone()
        if not row or row[0] is None:
            return 0.0
        return (row[1] - row[0]) / 86400.0


@dataclass
class Context:
    """runner 每 bar 传给策略(§1)。bars:该 symbol 截至当前的历史(含当前bar),
    DataFrame(index=open_ts epoch 秒,列 open/high/low/close/volume)。"""

    bar: Bar
    bars: pd.DataFrame
    position: Optional[Position]
    equity: float
    funding_rate: Optional[float]
    event_gate: Any                 # EventGate:blocked(symbol, ts) -> (bool, reason)
    data: MarketData
    now_ts: int
    extras: Dict[str, Any] = field(default_factory=dict)  # 策略自留状态(连亏拉黑等)
