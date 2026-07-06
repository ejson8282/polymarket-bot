"""施工包01 · §B3.4 资金费计提。

在 funding 表的结算时间点(无数据则按配置 funding_interval_hours 的整点)执行:
cash -= signed_qty × mark × rate(多头付正 funding)。
落 funding_events:amount 符号约定 **负 = 支出**(多头、rate>0 时 amount<0),
并累加进持仓的 funding_paid(支出为正累计)。
"""
from __future__ import annotations

import sqlite3
from typing import Callable, Dict, List, Optional, Tuple

from platforms.single_account.sim import persistence
from platforms.single_account.sim.account import PaperAccount
from platforms.single_account.sim.position import Position

RateProvider = Callable[[str, int], float]  # (symbol, period_ts) -> rate


def accrue(conn: sqlite3.Connection, account: PaperAccount, ts: int,
           marks: Dict[str, float], rate_for: RateProvider) -> List[Tuple[str, float]]:
    """对全部持仓按 rate_for(symbol, ts) 计提一次。返回 [(symbol, amount)]。"""
    events: List[Tuple[str, float]] = []
    with conn:
        for pos in account.positions.values():
            rate = float(rate_for(pos.symbol, ts) or 0.0)
            if rate == 0.0:
                continue
            mark = marks.get(pos.symbol, pos.entry_price)
            amount = -pos.signed_qty * mark * rate  # 负=支出(多头付正 funding)
            account.cash += amount
            pos.funding_paid += -amount
            persistence.insert_funding_event(conn, ts, pos.symbol, rate, pos.signed_qty, amount)
            events.append((pos.symbol, amount))
        persistence.save_runtime_meta(conn, account.positions, [])
    return events


def market_db_rate_provider(market_conn: sqlite3.Connection, venue: str = "decibel") -> RateProvider:
    """从任务A的 funding 表取结算周期费率:优先精确匹配周期起点,否则取 ≤ts 最近一行;无数据→0。"""

    def rate_for(symbol: str, ts: int) -> float:
        row = market_conn.execute(
            "SELECT rate FROM funding WHERE venue=? AND symbol=? AND ts<=? ORDER BY ts DESC LIMIT 1",
            (venue, symbol, ts)).fetchone()
        return float(row[0]) if row and row[0] is not None else 0.0

    return rate_for


class FundingEngine:
    """按固定周期整点触发 accrue(§B3.4;funding 表提供费率,缺省 0 即无计提)。"""

    def __init__(self, conn: sqlite3.Connection, account: PaperAccount,
                 rate_for: RateProvider, interval_hours: float = 8.0) -> None:
        self.conn = conn
        self.account = account
        self.rate_for = rate_for
        self.period_s = int(interval_hours * 3600)

    def accrue_if_due(self, close_ts: int, marks: Dict[str, float]) -> int:
        """结算 (last_funding_ts, close_ts] 内的所有周期整点,返回计提次数。"""
        last_raw = persistence.get_meta(self.conn, "last_funding_ts")
        if last_raw is None:
            # 首次调用:从当前周期起点开始计,不补历史
            with self.conn:
                persistence.set_meta(self.conn, "last_funding_ts",
                                     str(close_ts // self.period_s * self.period_s))
            return 0
        last = int(last_raw)
        count = 0
        due = (last // self.period_s + 1) * self.period_s
        while due <= close_ts:
            accrue(self.conn, self.account, due, marks, self.rate_for)
            last = due
            count += 1
            due += self.period_s
        if count:
            with self.conn:
                persistence.set_meta(self.conn, "last_funding_ts", str(last))
        return count
