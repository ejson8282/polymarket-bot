"""施工包01 · §B4 指标(公式写死)。

- dd_t = 1 − equity_t / max(equity_0..t);MDD = max(dd_t)(基于 equity_snapshots)。
- 期望R = mean(r_multiple);PF = Σ盈利 / |Σ亏损|;胜率 = wins/trades;
  Sharpe = mean(日收益)/std(日收益, ddof=1) × √365(不足 2 个交易日返回 None)。
"""
from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone
from statistics import mean, stdev
from typing import Iterable, List, Optional


def max_drawdown(equity_series: Iterable[float]) -> float:
    peak = -math.inf
    mdd = 0.0
    for equity in equity_series:
        peak = max(peak, equity)
        if peak > 0:
            mdd = max(mdd, 1.0 - equity / peak)
    return mdd


def expectancy_r(r_multiples: Iterable[Optional[float]]) -> Optional[float]:
    values = [r for r in r_multiples if r is not None]
    return mean(values) if values else None


def profit_factor(net_pnls: Iterable[float]) -> Optional[float]:
    gains = sum(v for v in net_pnls if v > 0)
    losses = sum(v for v in net_pnls if v < 0)
    if losses == 0:
        return None
    return gains / abs(losses)


def win_rate(net_pnls: Iterable[float]) -> Optional[float]:
    values = list(net_pnls)
    if not values:
        return None
    return sum(1 for v in values if v > 0) / len(values)


def sharpe(daily_returns: Iterable[float]) -> Optional[float]:
    values = list(daily_returns)
    if len(values) < 2:
        return None
    sd = stdev(values)  # ddof=1
    if sd == 0:
        return None
    return mean(values) / sd * math.sqrt(365.0)


def _utc_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def aggregate_strategy_daily(conn: sqlite3.Connection) -> int:
    """从 positions_closed(+已折入其中的 funding)聚合写 strategy_daily。
    mdd_intraday 取该日 equity_snapshots 的组合级 MDD(一期不按策略拆分,
    单策略场景两者一致;多策略拆分留待二期)。"""
    rows = conn.execute(
        "SELECT exit_ts, strategy, gross_pnl, fees, funding, net_pnl FROM positions_closed"
    ).fetchall()
    daily: dict = {}
    for exit_ts, strategy, gross, fees, funding, net in rows:
        key = (_utc_date(int(exit_ts)), strategy)
        agg = daily.setdefault(key, {"trades": 0, "wins": 0, "gross": 0.0,
                                     "fees": 0.0, "funding": 0.0, "net": 0.0})
        agg["trades"] += 1
        agg["wins"] += 1 if (net or 0) > 0 else 0
        agg["gross"] += gross or 0.0
        agg["fees"] += fees or 0.0
        agg["funding"] += funding or 0.0
        agg["net"] += net or 0.0

    mdd_by_date: dict = {}
    for (date, _), _agg in daily.items():
        if date in mdd_by_date:
            continue
        start = int(datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        series = [row[0] for row in conn.execute(
            "SELECT equity FROM equity_snapshots WHERE ts>=? AND ts<? ORDER BY ts",
            (start, start + 86400))]
        mdd_by_date[date] = max_drawdown(series) if series else 0.0

    with conn:
        for (date, strategy), agg in daily.items():
            conn.execute(
                "INSERT OR REPLACE INTO strategy_daily(date, strategy, trades, wins, gross, "
                "fees, funding, net, mdd_intraday) VALUES(?,?,?,?,?,?,?,?,?)",
                (date, strategy, agg["trades"], agg["wins"], agg["gross"], agg["fees"],
                 agg["funding"], agg["net"], mdd_by_date[date]))
    return len(daily)
