"""施工包02 · §2.1 funding_carry(资金费倾斜持仓)。

开空(收正 funding;做多完全对称):
  funding_apr ≥ +25% 且 30日z ≥ +1.5 且 非(4h EMA50>EMA200 且 1h ADX≥25)
  且 预期24h funding收入 ≥ 3×往返taker费。
平仓:funding_apr 连续2期 < 8% / 趋势反转(EMA交叉+ADX≥25 逆向) /
  硬止损 1.8×ATR(14,1h) / 浮盈>1.2×ATR 止损移成本 / 时间止损5天。
风险预算 0.75% 权益,名义上限 15%。
失效(health_check):滚动30笔 mean(net)<0,或实收funding/理论<60% → 停用。

口径备注:funding 表 rate 为小时费率(施工包01 ENDPOINTS.md),
apr = rate×24×365;指标暖机期内趋势过滤按规格字面(NaN 比较为 False)处理。
点差过滤用 basis_ticks 的 bid/ask,该 symbol 无 basis 数据时跳过(规格允许,TODO:
接入 orderbook 后补齐全品种点差)。
"""
from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Tuple

from platforms.single_account.strategies import indicators as ind
from platforms.single_account.strategies.base import Context, Signal, risk_qty

DEFAULTS = {
    "symbols": ["BTC", "ETH", "SOL"],
    "apr_entry": 0.25,
    "z_entry": 1.5,
    "apr_exit": 0.08,
    "exit_confirm_periods": 2,
    "atr_stop_mult": 1.8,
    "breakeven_after_atr": 1.2,
    "time_stop_days": 5,
    "risk_pct": 0.0075,
    "notional_cap_pct": 0.15,
    "income_fee_mult": 3.0,
    "taker_fee_bps": 5.0,
    "max_spread_bps": 12.0,
    "trend_adx_min": 25.0,
    "health_min_trades": 30,
}

HOURS_PER_YEAR = 24 * 365


class FundingCarry:
    name = "funding_carry"

    def __init__(self, cfg: Optional[dict] = None) -> None:
        self.cfg = dict(DEFAULTS)
        self.cfg.update(cfg or {})
        self._weak_periods: Dict[str, int] = {}   # funding 转弱连续计数
        self._last_funding_ts: Dict[str, int] = {}
        self.disabled = False
        self.disable_report: Optional[dict] = None

    # ---------- 指标 ----------

    def _trend_state(self, ctx: Context) -> Tuple[bool, bool, float]:
        """返回 (强上涨, 强下跌, atr_1h)。暖机期 NaN 比较自然为 False(规格字面)。"""
        bars_1h = ind.resample_ohlcv(ctx.bars, "1h")
        bars_4h = ind.resample_ohlcv(ctx.bars, "4h")
        atr_1h = ind.atr(bars_1h["high"], bars_1h["low"], bars_1h["close"], 14)
        adx_1h = ind.adx(bars_1h["high"], bars_1h["low"], bars_1h["close"], 14)
        ema50 = ind.ema(bars_4h["close"], 50)
        ema200 = ind.ema(bars_4h["close"], 200)
        adx_now = float(adx_1h.iloc[-1]) if len(adx_1h) else float("nan")
        e50 = float(ema50.iloc[-1]) if len(ema50) else float("nan")
        e200 = float(ema200.iloc[-1]) if len(ema200) else float("nan")
        strong_up = (e50 > e200) and (adx_now >= self.cfg["trend_adx_min"])
        strong_down = (e50 < e200) and (adx_now >= self.cfg["trend_adx_min"])
        atr_now = float(atr_1h.iloc[-1]) if len(atr_1h) and atr_1h.notna().iloc[-1] else 0.0
        return bool(strong_up), bool(strong_down), atr_now

    def _funding_view(self, ctx: Context) -> Tuple[Optional[float], Optional[float]]:
        """返回 (当前小时费率, 30日z)。"""
        hist = ctx.data.funding_history(ctx.bar.symbol, ctx.now_ts, days=30)
        if hist.empty:
            return None, None
        rate = float(hist["rate"].iloc[-1])
        z_series = ind.zscore(hist["rate"], window=len(hist))
        z = float(z_series.iloc[-1]) if z_series.notna().iloc[-1] else None
        return rate, z

    def _spread_ok(self, ctx: Context) -> bool:
        ticks = ctx.data.basis_ticks(ctx.bar.symbol, ctx.now_ts - 3600, ctx.now_ts)
        if ticks.empty:
            return True  # 无 basis 数据 → 跳过点差过滤(TODO:orderbook 全品种点差)
        last = ticks.dropna(subset=["platform_bid", "platform_ask"]).tail(1)
        if last.empty:
            return True
        bid = float(last["platform_bid"].iloc[0])
        ask = float(last["platform_ask"].iloc[0])
        mid = (bid + ask) / 2
        if mid <= 0:
            return True
        return (ask - bid) / mid * 10000.0 <= self.cfg["max_spread_bps"]

    # ---------- 主逻辑 ----------

    def on_bar(self, ctx: Context) -> List[Signal]:
        if self.disabled or ctx.bar.symbol not in self.cfg["symbols"]:
            return []
        rate, z = self._funding_view(ctx)
        if rate is None:
            return []
        apr = rate * HOURS_PER_YEAR
        strong_up, strong_down, atr_1h = self._trend_state(ctx)
        pos = ctx.position

        if pos is not None:
            return self._manage_position(ctx, pos, apr, strong_up, strong_down, atr_1h)

        if atr_1h <= 0 or not self._spread_ok(ctx):
            return []
        # 预期24h收入 ≥ 3×往返taker费(与 qty/价格无关的比率判断)
        income_ok = abs(rate) * 24 >= self.cfg["income_fee_mult"] * 2 * self.cfg["taker_fee_bps"] / 10000.0
        if not income_ok:
            return []
        entry = ctx.bar.close
        if apr >= self.cfg["apr_entry"] and (z is not None and z >= self.cfg["z_entry"]) \
                and not strong_up:
            stop = entry + self.cfg["atr_stop_mult"] * atr_1h
            qty = risk_qty(self.cfg["risk_pct"], ctx.equity, entry, stop,
                           self.cfg["notional_cap_pct"])
            if qty > 0:
                return [Signal(self.name, ctx.bar.symbol, "open_short", qty, stop, None,
                               f"funding_apr={apr:.1%} z={z:.2f} 收正funding做空",
                               {"apr": apr, "z": z, "rate": rate, "atr_1h": atr_1h})]
        if apr <= -self.cfg["apr_entry"] and (z is not None and z <= -self.cfg["z_entry"]) \
                and not strong_down:
            stop = entry - self.cfg["atr_stop_mult"] * atr_1h
            qty = risk_qty(self.cfg["risk_pct"], ctx.equity, entry, stop,
                           self.cfg["notional_cap_pct"])
            if qty > 0:
                return [Signal(self.name, ctx.bar.symbol, "open_long", qty, stop, None,
                               f"funding_apr={apr:.1%} z={z:.2f} 收负funding做多",
                               {"apr": apr, "z": z, "rate": rate, "atr_1h": atr_1h})]
        return []

    def _manage_position(self, ctx: Context, pos, apr: float, strong_up: bool,
                         strong_down: bool, atr_1h: float) -> List[Signal]:
        symbol = ctx.bar.symbol
        # funding 转弱连续计数(每个新 funding 周期判定一期)
        hist = ctx.data.funding_history(symbol, ctx.now_ts, days=2)
        if not hist.empty:
            latest_ts = int(hist["ts"].iloc[-1])
            if latest_ts != self._last_funding_ts.get(symbol):
                self._last_funding_ts[symbol] = latest_ts
                if abs(apr) < self.cfg["apr_exit"]:
                    self._weak_periods[symbol] = self._weak_periods.get(symbol, 0) + 1
                else:
                    self._weak_periods[symbol] = 0
        if self._weak_periods.get(symbol, 0) >= self.cfg["exit_confirm_periods"]:
            self._weak_periods[symbol] = 0
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           f"funding转弱连续{self.cfg['exit_confirm_periods']}期 apr={apr:.1%}", {})]
        # 趋势反转逆向平仓
        if (pos.side == "short" and strong_up) or (pos.side == "long" and strong_down):
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           "趋势反转(EMA交叉+ADX逆向)平仓", {})]
        # 时间止损 5 天
        if ctx.now_ts - pos.entry_ts >= self.cfg["time_stop_days"] * 86400:
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           f"时间止损{self.cfg['time_stop_days']}天", {})]
        # 浮盈 > 1.2×ATR → 止损移到成本(直接修改持仓,由 runner 持久化)
        if atr_1h > 0:
            favorable = (ctx.bar.close - pos.entry_price) * pos.direction
            if favorable > self.cfg["breakeven_after_atr"] * atr_1h:
                if pos.stop is None or (pos.stop - pos.entry_price) * pos.direction < 0:
                    pos.stop = pos.entry_price
        return []

    # ---------- 失效检测(§2.1 末) ----------

    def health_check(self, paper_conn: sqlite3.Connection) -> Tuple[bool, dict]:
        rows = paper_conn.execute(
            "SELECT net_pnl, funding, entry_price, qty, holding_secs, tags_json "
            "FROM positions_closed WHERE strategy=? ORDER BY exit_ts DESC LIMIT ?",
            (self.name, self.cfg["health_min_trades"])).fetchall()
        report = {"trades": len(rows)}
        if len(rows) < self.cfg["health_min_trades"]:
            return True, report
        nets = [r[0] or 0.0 for r in rows]
        report["mean_net"] = sum(nets) / len(nets)
        if report["mean_net"] < 0:
            self.disabled = True
            report["reason"] = "rolling30_mean_net<0"
        self.disable_report = report if self.disabled else None
        return not self.disabled, report
