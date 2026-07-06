"""施工包02 · §2.4 rwa_basis(RWA/美股映射)——先测量后交易。

准入门(不可绕过):该 symbol 的 basis_ticks 数据跨度 ≥7 天,否则 on_bar 返回空
并记 skip 原因 "insufficient_basis_data"(经 ctx.extras['skip_events'] 交 runner
写 decisions)。

开仓:ref 1分钟内变动 > x_bps(配置,起点30bps,且必须 > 手续费+点差成本线)
且 platform 在 lag_window_sec 内未跟随(跟随幅度 < follow_frac×ref变动)
→ 向 ref 方向开仓。
平仓:basis 收敛 <10bps;或 5 分钟时间止损(15m 回放下于下一根 bar 生效);
basis 反向扩大 2× → 立即止损(另设价格硬止损)。
硬规则:只在标的 RTH 内(复用01 is_rth);收盘前10分钟强制平净、禁开新仓;
绝不持仓过周末(RTH+强平规则天然保证,再加显式防御);单笔最大亏损 0.3% 权益;
名义上限(流动性薄)默认 5% 权益。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from platforms.single_account.recorders.rwa_basis_recorder import NY_TZ, is_rth
from platforms.single_account.strategies.base import Context, Signal, risk_qty

DEFAULTS = {
    "symbols": ["XAU", "QQQ", "NVDA"],
    "min_data_days": 7.0,
    "x_bps": 30.0,
    "follow_frac": 0.3,
    "lag_window_sec": 60,
    "tick_staleness_sec": 90,
    "converge_bps": 10.0,
    "time_stop_sec": 300,
    "adverse_mult": 2.0,
    "risk_pct": 0.003,
    "notional_cap_pct": 0.05,
    "flatten_before_close_min": 10,
    "cost_floor_bps": None,   # 手续费+点差成本线;None 时按 2×taker+点差估计
    "taker_fee_bps": 5.0,
    "assumed_spread_bps": 10.0,
}


def _skip(ctx: Context, strategy: str, reason: str) -> List[Signal]:
    ctx.extras.setdefault("skip_events", []).append(
        {"strategy": strategy, "symbol": ctx.bar.symbol, "reason": reason})
    return []


def minutes_to_ny_close(ts: int) -> float:
    local = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(NY_TZ)
    return (16 * 60) - (local.hour * 60 + local.minute + local.second / 60.0)


class RwaBasis:
    name = "rwa_basis"

    def __init__(self, cfg: Optional[dict] = None) -> None:
        self.cfg = dict(DEFAULTS)
        self.cfg.update(cfg or {})

    def _cost_floor_bps(self) -> float:
        if self.cfg["cost_floor_bps"] is not None:
            return float(self.cfg["cost_floor_bps"])
        return 2 * self.cfg["taker_fee_bps"] + self.cfg["assumed_spread_bps"]

    # ---------- 主逻辑 ----------

    def on_bar(self, ctx: Context) -> List[Signal]:
        symbol = ctx.bar.symbol
        if symbol not in self.cfg["symbols"]:
            return []
        pos = ctx.position
        now = ctx.now_ts
        now_dt = datetime.fromtimestamp(now, tz=timezone.utc)

        # 准入门:basis 数据 ≥7 天(§2.4,不许绕过)
        span_days = ctx.data.basis_data_span_days(symbol, now)
        if span_days < self.cfg["min_data_days"]:
            if pos is not None:
                return [Signal(self.name, symbol, "close", pos.qty, None, None,
                               "insufficient_basis_data(数据门失效防御性平仓)", {})]
            return _skip(ctx, self.name, "insufficient_basis_data")

        in_rth = is_rth(now_dt)
        to_close_min = minutes_to_ny_close(now) if in_rth else 0.0
        is_friday_late = now_dt.astimezone(NY_TZ).weekday() == 4 and to_close_min <= 30

        if pos is not None:
            return self._manage_position(ctx, pos, in_rth, to_close_min, is_friday_late)

        # 硬规则:RTH 内、收盘前 10 分钟禁开
        if not in_rth:
            return _skip(ctx, self.name, "outside_rth")
        if to_close_min <= self.cfg["flatten_before_close_min"]:
            return _skip(ctx, self.name, "near_session_close")

        # x_bps 必须高于成本线
        x_bps = float(self.cfg["x_bps"])
        if x_bps <= self._cost_floor_bps():
            return _skip(ctx, self.name, "x_bps_below_cost_floor")

        ticks = ctx.data.basis_ticks(symbol, now - (120 + self.cfg["tick_staleness_sec"]), now)
        ticks = ticks.dropna(subset=["ref_price", "platform_mark"])
        if ticks.empty or now - int(ticks["ts"].iloc[-1]) > self.cfg["tick_staleness_sec"]:
            return _skip(ctx, self.name, "stale_or_missing_ticks")
        latest = ticks.iloc[-1]
        past = ticks[ticks["ts"] <= int(latest["ts"]) - 60]
        if past.empty:
            return _skip(ctx, self.name, "insufficient_recent_ticks")
        base = past.iloc[-1]
        ref_move_bps = (float(latest["ref_price"]) / float(base["ref_price"]) - 1.0) * 10000.0
        mark_move_bps = (float(latest["platform_mark"]) / float(base["platform_mark"]) - 1.0) * 10000.0

        if abs(ref_move_bps) <= x_bps:
            return []
        if abs(mark_move_bps) >= self.cfg["follow_frac"] * abs(ref_move_bps):
            return []  # platform 已跟随,无滞后可套

        mark_now = float(latest["platform_mark"])
        ref_now = float(latest["ref_price"])
        entry_gap_bps = (ref_now / mark_now - 1.0) * 10000.0
        if abs(entry_gap_bps) < 1e-9:
            return []
        stop_dist = mark_now * self.cfg["adverse_mult"] * abs(entry_gap_bps) / 10000.0
        action = "open_long" if ref_move_bps > 0 else "open_short"
        stop = mark_now - stop_dist if action == "open_long" else mark_now + stop_dist
        qty = risk_qty(self.cfg["risk_pct"], ctx.equity, mark_now, stop,
                       self.cfg["notional_cap_pct"])
        if qty <= 0:
            return []
        return [Signal(self.name, symbol, action, qty, stop, None,
                       f"ref 1min {ref_move_bps:+.1f}bps platform 仅 {mark_move_bps:+.1f}bps 滞后",
                       {"entry_gap_bps": entry_gap_bps, "ref_move_bps": ref_move_bps,
                        "entry_ts": now})]

    def _manage_position(self, ctx: Context, pos, in_rth: bool, to_close_min: float,
                         is_friday_late: bool) -> List[Signal]:
        symbol = pos.symbol
        now = ctx.now_ts
        # 强制平净:RTH 外(不该发生但防御)/ 收盘前10分钟 / 周五临收盘
        if not in_rth or to_close_min <= self.cfg["flatten_before_close_min"] or is_friday_late:
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           "收盘前/周末强制平净", {})]
        # 5 分钟时间止损
        if now - pos.entry_ts >= self.cfg["time_stop_sec"]:
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           "5分钟时间止损", {})]
        ticks = ctx.data.basis_ticks(symbol, now - self.cfg["tick_staleness_sec"], now)
        ticks = ticks.dropna(subset=["ref_price", "platform_mark"])
        if ticks.empty:
            return []
        latest = ticks.iloc[-1]
        gap_bps = (float(latest["ref_price"]) / float(latest["platform_mark"]) - 1.0) * 10000.0
        entry_gap = float(pos.tags.get("entry_gap_bps") or 0.0)
        if abs(gap_bps) < self.cfg["converge_bps"]:
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           f"basis 收敛 {gap_bps:+.1f}bps", {})]
        if entry_gap and abs(gap_bps) >= self.cfg["adverse_mult"] * abs(entry_gap):
            return [Signal(self.name, symbol, "close", pos.qty, None, None,
                           f"basis 反向扩大 {gap_bps:+.1f}bps ≥2×入场 立即止损", {})]
        return []
