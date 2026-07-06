"""施工包02 · §2.2 momentum_breakout(趋势突破)。

开多(空完全对称):15m收盘 > 近55根最高(不含当前根)
  且 量 ≥ 1.5×SMA20 且 ATR(14,15m) > 其50期均线 且 4h EMA50 > EMA200。
止损:初始 = 入场 − 1.2×ATR;吊灯跟踪 = 22根最高 − 2.5×ATR(只收紧不放松);
  +2R 后减仓 1/3(一次性;用01扩展的部分平仓);备用离场:15m收盘跌破 EMA20。
风险 0.6%;同时最多3仓(跨 symbol,由 runner 经 extras 提供计数);
当日累计亏损 ≥2% 权益 → 当日停开(runner 经 extras 提供当日已实现亏损)。
失效:滚动30笔 胜率<30% 且 均R<0;或 快速失败(≤5根内止损离场)比例>60%。
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple

from platforms.single_account.strategies import indicators as ind
from platforms.single_account.strategies.base import Context, Signal, risk_qty

DEFAULTS = {
    "symbols": ["BTC", "ETH", "SOL"],
    "donchian_period": 55,
    "volume_sma": 20,
    "volume_mult": 1.5,
    "atr_period": 14,
    "atr_sma": 50,
    "stop_atr_mult": 1.2,
    "chandelier_period": 22,
    "chandelier_atr_mult": 2.5,
    "scale_out_r": 2.0,
    "scale_out_fraction": 1 / 3,
    "ema_backup_period": 20,
    "risk_pct": 0.006,
    "max_concurrent": 3,
    "daily_loss_stop_pct": 0.02,
    "notional_cap_pct": None,
    "health_min_trades": 30,
}


class MomentumBreakout:
    name = "momentum_breakout"

    def __init__(self, cfg: Optional[dict] = None) -> None:
        self.cfg = dict(DEFAULTS)
        self.cfg.update(cfg or {})
        self.disabled = False
        self.disable_report: Optional[dict] = None

    # ---------- 指标 ----------

    def _indicators(self, ctx: Context):
        bars = ctx.bars
        atr = ind.atr(bars["high"], bars["low"], bars["close"], self.cfg["atr_period"])
        atr_ma = ind.sma(atr, self.cfg["atr_sma"])
        don_high = ind.donchian_high(bars["high"], self.cfg["donchian_period"])
        don_low = ind.donchian_low(bars["low"], self.cfg["donchian_period"])
        vol_ma = ind.sma(bars["volume"], self.cfg["volume_sma"])
        ema20 = ind.ema(bars["close"], self.cfg["ema_backup_period"])
        bars_4h = ind.resample_ohlcv(bars, "4h")
        ema50 = ind.ema(bars_4h["close"], 50)
        ema200 = ind.ema(bars_4h["close"], 200)
        return atr, atr_ma, don_high, don_low, vol_ma, ema20, ema50, ema200

    # ---------- 主逻辑 ----------

    def on_bar(self, ctx: Context) -> List[Signal]:
        if self.disabled or ctx.bar.symbol not in self.cfg["symbols"]:
            return []
        atr, atr_ma, don_high, don_low, vol_ma, ema20, ema50, ema200 = self._indicators(ctx)
        atr_now = float(atr.iloc[-1]) if atr.notna().iloc[-1] else 0.0
        pos = ctx.position
        if pos is not None:
            return self._manage_position(ctx, pos, atr, ema20)

        # 开仓约束:当日亏损停开 / 并发仓位上限(数据由 runner 提供)
        if ctx.extras.get("strategy_daily_loss_pct", 0.0) >= self.cfg["daily_loss_stop_pct"]:
            return []
        if ctx.extras.get("open_positions_count", 0) >= self.cfg["max_concurrent"]:
            return []
        if atr_now <= 0 or not atr_ma.notna().iloc[-1]:
            return []

        close = ctx.bar.close
        volume = ctx.bar.volume or 0.0
        vol_ok = vol_ma.notna().iloc[-1] and volume >= self.cfg["volume_mult"] * float(vol_ma.iloc[-1])
        atr_expanding = atr_now > float(atr_ma.iloc[-1])
        e50 = float(ema50.iloc[-1]) if len(ema50) else float("nan")
        e200 = float(ema200.iloc[-1]) if len(ema200) else float("nan")

        if don_high.notna().iloc[-1] and close > float(don_high.iloc[-1]) \
                and vol_ok and atr_expanding and e50 > e200:
            stop = close - self.cfg["stop_atr_mult"] * atr_now
            qty = risk_qty(self.cfg["risk_pct"], ctx.equity, close, stop,
                           self.cfg["notional_cap_pct"])
            if qty > 0:
                return [Signal(self.name, ctx.bar.symbol, "open_long", qty, stop, None,
                               f"突破55高 {float(don_high.iloc[-1]):.4g} 放量 ATR扩张 4h多头",
                               {"initial_stop": stop, "breakout_level": float(don_high.iloc[-1]),
                                "atr": atr_now, "side": "long"})]
        if don_low.notna().iloc[-1] and close < float(don_low.iloc[-1]) \
                and vol_ok and atr_expanding and e50 < e200:
            stop = close + self.cfg["stop_atr_mult"] * atr_now
            qty = risk_qty(self.cfg["risk_pct"], ctx.equity, close, stop,
                           self.cfg["notional_cap_pct"])
            if qty > 0:
                return [Signal(self.name, ctx.bar.symbol, "open_short", qty, stop, None,
                               f"跌破55低 {float(don_low.iloc[-1]):.4g} 放量 ATR扩张 4h空头",
                               {"initial_stop": stop, "breakout_level": float(don_low.iloc[-1]),
                                "atr": atr_now, "side": "short"})]
        return []

    def _manage_position(self, ctx: Context, pos, atr, ema20) -> List[Signal]:
        bars = ctx.bars
        close = ctx.bar.close
        atr_now = float(atr.iloc[-1]) if atr.notna().iloc[-1] else 0.0

        # 备用离场:15m 收盘跌破 EMA20(空头对称:收盘上破)
        if ema20.notna().iloc[-1]:
            e20 = float(ema20.iloc[-1])
            if (pos.side == "long" and close < e20) or (pos.side == "short" and close > e20):
                return [Signal(self.name, pos.symbol, "close", pos.qty, None, None,
                               "备用离场:收盘穿越EMA20", {})]

        # 吊灯止损:22根最高 − 2.5×ATR(只向有利方向收紧)
        if atr_now > 0:
            period = self.cfg["chandelier_period"]
            if pos.side == "long":
                anchor = float(bars["high"].tail(period).max())
                chandelier = anchor - self.cfg["chandelier_atr_mult"] * atr_now
                if pos.stop is None or chandelier > pos.stop:
                    pos.stop = chandelier
            else:
                anchor = float(bars["low"].tail(period).min())
                chandelier = anchor + self.cfg["chandelier_atr_mult"] * atr_now
                if pos.stop is None or chandelier < pos.stop:
                    pos.stop = chandelier

        # +2R 一次性减仓 1/3(部分平仓,01扩展)
        initial_stop = pos.tags.get("initial_stop")
        if initial_stop is not None and not pos.tags.get("scaled_out"):
            risk_per_unit = abs(pos.entry_price - float(initial_stop))
            favorable = (close - pos.entry_price) * pos.direction
            if risk_per_unit > 0 and favorable >= self.cfg["scale_out_r"] * risk_per_unit:
                pos.tags["scaled_out"] = True
                return [Signal(self.name, pos.symbol, "close",
                               pos.qty * self.cfg["scale_out_fraction"], None, None,
                               "+2R 减仓 1/3", {"partial": True})]
        return []

    # ---------- 失效检测 ----------

    def health_check(self, paper_conn: sqlite3.Connection) -> Tuple[bool, dict]:
        rows = paper_conn.execute(
            "SELECT net_pnl, r_multiple, holding_secs, exit_reason FROM positions_closed "
            "WHERE strategy=? ORDER BY exit_ts DESC LIMIT ?",
            (self.name, self.cfg["health_min_trades"])).fetchall()
        report = {"trades": len(rows)}
        if len(rows) < self.cfg["health_min_trades"]:
            return True, report
        nets = [r[0] or 0.0 for r in rows]
        rs = [r[1] for r in rows if r[1] is not None]
        win_rate = sum(1 for n in nets if n > 0) / len(nets)
        mean_r = sum(rs) / len(rs) if rs else 0.0
        quick_fail = sum(1 for r in rows
                         if (r[2] or 0) <= 5 * 900 and (r[0] or 0) < 0) / len(rows)
        report.update({"win_rate": win_rate, "mean_r": mean_r, "quick_fail_ratio": quick_fail})
        if (win_rate < 0.30 and mean_r < 0) or quick_fail > 0.60:
            self.disabled = True
            report["reason"] = "win<30%且均R<0" if quick_fail <= 0.60 else "5根内失败比例>60%"
            self.disable_report = report
        return not self.disabled, report
