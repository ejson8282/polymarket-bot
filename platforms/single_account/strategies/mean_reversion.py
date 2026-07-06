"""施工包02 · §2.3 mean_reversion(均值回归/区间)。

政权过滤(比参数重要):仅当 1h ADX<18 且 15m 布林带宽处 20 日分位 <30% 时启用;
任一破坏立即停用(不再产生新入场信号)。
开多(空对称):15m 收盘下穿布林(20,2.2)下轨 且 RSI(3)<8 → 下轨挂限价买。
平仓:回中轨或日内VWAP即离场;硬止损 入场外 1×ATR;时间止损 16 根(4h)无论盈亏。
禁止:触发K线振幅>2.5×ATR 或 量>2.5×均量(爆发不是回归);同品种连亏3笔拉黑48h。
失效:入场后 ADX 快速>25 的比例>40%(经持仓 tags 标记统计)。

规格未定的参数(config 可调,PR 已注明):risk_pct 默认 0.5%;
限价单未成交 4 根 bar 后由组合层撤单。
"""
from __future__ import annotations

import sqlite3
from typing import List, Optional, Tuple

from platforms.single_account.strategies import indicators as ind
from platforms.single_account.strategies.base import Context, Signal, risk_qty

DEFAULTS = {
    "symbols": ["BTC", "ETH", "SOL"],
    "adx_max": 18.0,
    "bandwidth_pctile_max": 0.30,
    "bandwidth_window_days": 20,
    "boll_period": 20,
    "boll_std": 2.2,
    "rsi_period": 3,
    "rsi_max": 8.0,
    "stop_atr_mult": 1.0,
    "time_stop_bars": 16,
    "trigger_range_atr_mult": 2.5,
    "trigger_volume_mult": 2.5,
    "loss_streak_blacklist": 3,
    "blacklist_hours": 48,
    "adx_spike_level": 25.0,
    "risk_pct": 0.005,
    "cancel_after_bars": 4,
    "health_min_trades": 30,
}


class MeanReversion:
    name = "mean_reversion"

    def __init__(self, cfg: Optional[dict] = None) -> None:
        self.cfg = dict(DEFAULTS)
        self.cfg.update(cfg or {})
        self.disabled = False
        self.disable_report: Optional[dict] = None

    # ---------- 过滤与指标 ----------

    def _regime_ok(self, ctx: Context) -> bool:
        bars_1h = ind.resample_ohlcv(ctx.bars, "1h")
        adx_1h = ind.adx(bars_1h["high"], bars_1h["low"], bars_1h["close"], 14)
        if not len(adx_1h) or not adx_1h.notna().iloc[-1] \
                or float(adx_1h.iloc[-1]) >= self.cfg["adx_max"]:
            return False  # ADX 不可得或 ≥18 → 保守停用(政权过滤比参数重要)
        _, _, _, bw = ind.bollinger(ctx.bars["close"], self.cfg["boll_period"],
                                    self.cfg["boll_std"])
        window = int(self.cfg["bandwidth_window_days"] * 96)  # 15m → 每日96根
        pct = ind.rolling_percentile_rank(bw, window)
        if not pct.notna().iloc[-1] or float(pct.iloc[-1]) >= self.cfg["bandwidth_pctile_max"]:
            return False
        return True

    def _blacklisted(self, ctx: Context) -> bool:
        conn: Optional[sqlite3.Connection] = ctx.extras.get("paper_conn")
        if conn is None:
            return False
        rows = conn.execute(
            "SELECT net_pnl, exit_ts FROM positions_closed WHERE strategy=? AND symbol=? "
            "ORDER BY exit_ts DESC LIMIT ?",
            (self.name, ctx.bar.symbol, self.cfg["loss_streak_blacklist"])).fetchall()
        if len(rows) < self.cfg["loss_streak_blacklist"]:
            return False
        if all((r[0] or 0) < 0 for r in rows):
            last_exit = max(int(r[1]) for r in rows)
            return ctx.now_ts - last_exit < self.cfg["blacklist_hours"] * 3600
        return False

    # ---------- 主逻辑 ----------

    def on_bar(self, ctx: Context) -> List[Signal]:
        if self.disabled or ctx.bar.symbol not in self.cfg["symbols"]:
            return []
        bars = ctx.bars
        atr = ind.atr(bars["high"], bars["low"], bars["close"], 14)
        atr_now = float(atr.iloc[-1]) if atr.notna().iloc[-1] else 0.0
        pos = ctx.position
        if pos is not None:
            return self._manage_position(ctx, pos)
        if atr_now <= 0 or not self._regime_ok(ctx) or self._blacklisted(ctx):
            return []

        mid, upper, lower, _ = ind.bollinger(bars["close"], self.cfg["boll_period"],
                                             self.cfg["boll_std"])
        rsi = ind.rsi(bars["close"], self.cfg["rsi_period"])
        if len(bars) < 2 or not lower.notna().iloc[-1] or not rsi.notna().iloc[-1]:
            return []
        close, prev_close = float(bars["close"].iloc[-1]), float(bars["close"].iloc[-2])
        lo_now, lo_prev = float(lower.iloc[-1]), float(lower.iloc[-2])
        up_now, up_prev = float(upper.iloc[-1]), float(upper.iloc[-2])
        rsi_now = float(rsi.iloc[-1])

        # 禁止爆发K线:振幅>2.5×ATR 或 量>2.5×均量
        bar_range = ctx.bar.high - ctx.bar.low
        vol_ma = ind.sma(bars["volume"], 20)
        volume = ctx.bar.volume or 0.0
        if bar_range > self.cfg["trigger_range_atr_mult"] * atr_now:
            return []
        if vol_ma.notna().iloc[-1] and volume > self.cfg["trigger_volume_mult"] * float(vol_ma.iloc[-1]):
            return []

        signals: List[Signal] = []
        crossed_down = prev_close >= lo_prev and close < lo_now
        crossed_up = prev_close <= up_prev and close > up_now
        if crossed_down and rsi_now < self.cfg["rsi_max"]:
            limit = lo_now
            stop = limit - self.cfg["stop_atr_mult"] * atr_now
            qty = risk_qty(self.cfg["risk_pct"], ctx.equity, limit, stop)
            if qty > 0:
                signals.append(Signal(
                    self.name, ctx.bar.symbol, "open_long", qty, stop, None,
                    f"下穿布林下轨 RSI3={rsi_now:.1f} 下轨限价买",
                    {"entry_kind": "limit", "band_mid": float(mid.iloc[-1]),
                     "cancel_after_bars": self.cfg["cancel_after_bars"]},
                    limit_price=limit))
        elif crossed_up and rsi_now > 100 - self.cfg["rsi_max"]:
            limit = up_now
            stop = limit + self.cfg["stop_atr_mult"] * atr_now
            qty = risk_qty(self.cfg["risk_pct"], ctx.equity, limit, stop)
            if qty > 0:
                signals.append(Signal(
                    self.name, ctx.bar.symbol, "open_short", qty, stop, None,
                    f"上穿布林上轨 RSI3={rsi_now:.1f} 上轨限价卖",
                    {"entry_kind": "limit", "band_mid": float(mid.iloc[-1]),
                     "cancel_after_bars": self.cfg["cancel_after_bars"]},
                    limit_price=limit))
        return signals

    def _manage_position(self, ctx: Context, pos) -> List[Signal]:
        bars = ctx.bars
        close = ctx.bar.close
        # 失效统计:入场后 ADX 快速上穿 25 → 打标(health_check 统计比例)
        bars_1h = ind.resample_ohlcv(bars, "1h")
        adx_1h = ind.adx(bars_1h["high"], bars_1h["low"], bars_1h["close"], 14)
        if len(adx_1h) and adx_1h.notna().iloc[-1] \
                and float(adx_1h.iloc[-1]) > self.cfg["adx_spike_level"]:
            pos.tags["adx_spike"] = True

        mid, _, _, _ = ind.bollinger(bars["close"], self.cfg["boll_period"],
                                     self.cfg["boll_std"])
        vwap = ind.vwap_daily(bars.index.to_series(), bars["close"], bars["volume"])
        targets = []
        if mid.notna().iloc[-1]:
            targets.append(float(mid.iloc[-1]))
        if vwap.notna().iloc[-1]:
            targets.append(float(vwap.iloc[-1]))
        if targets:
            if pos.side == "long" and close >= min(targets):
                return [Signal(self.name, pos.symbol, "close", pos.qty, None, None,
                               "回中轨/VWAP 离场", {})]
            if pos.side == "short" and close <= max(targets):
                return [Signal(self.name, pos.symbol, "close", pos.qty, None, None,
                               "回中轨/VWAP 离场", {})]
        if ctx.now_ts - pos.entry_ts >= self.cfg["time_stop_bars"] * 900:
            return [Signal(self.name, pos.symbol, "close", pos.qty, None, None,
                           f"时间止损{self.cfg['time_stop_bars']}根", {})]
        return []

    # ---------- 失效检测 ----------

    def health_check(self, paper_conn: sqlite3.Connection) -> Tuple[bool, dict]:
        rows = paper_conn.execute(
            "SELECT tags_json FROM positions_closed WHERE strategy=? "
            "ORDER BY exit_ts DESC LIMIT ?",
            (self.name, self.cfg["health_min_trades"])).fetchall()
        report = {"trades": len(rows)}
        if len(rows) < self.cfg["health_min_trades"]:
            return True, report
        import json

        spikes = sum(1 for (tags_json,) in rows
                     if json.loads(tags_json or "{}").get("adx_spike"))
        ratio = spikes / len(rows)
        report["adx_spike_ratio"] = ratio
        if ratio > 0.40:
            self.disabled = True
            report["reason"] = "入场后ADX快速>25比例>40%"
            self.disable_report = report
        return not self.disabled, report
