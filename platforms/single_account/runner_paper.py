"""施工包01 · §B6 模拟器主循环 + 施工包02 · §4 组合层扩展。

CLI:
  python -m platforms.single_account.runner_paper --replay --symbol BTC --tf 15m --days 7 [--smoke]
  python -m platforms.single_account.runner_paper --replay --days 30 \
      --strategies funding_carry,momentum_breakout [--symbols BTC,ETH,SOL]
  python -m platforms.single_account.runner_paper --seed-demo --days 30 --symbols BTC,QQQ

- --smoke:第 3 根 bar 注入一个市价买单、第 20 根注入平仓单(仅单 symbol 路径)。
- --seed-demo:经 recorder 同一 upsert 路径写合成 K线/funding/basis 进 market.db。
- --strategies:施工包02 组合层;信号经 §4 优先级链过滤后交 broker。
- 断点续跑:回放从 last_replay_close_ts 之后继续(kill -9 重启不产生重复行)。
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Sequence, Union

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from platforms.single_account.recorders.common import (
    MARKET_DB_PATH,
    open_market_db,
    upsert_klines,
)
from platforms.single_account.recorders.kline_recorder import TF_SECONDS
from platforms.single_account.signals import load_config
from platforms.single_account.sim import persistence
from platforms.single_account.sim.broker import PaperBroker
from platforms.single_account.sim.funding import FundingEngine, market_db_rate_provider
from platforms.single_account.sim.metrics import (
    aggregate_strategy_daily,
    expectancy_r,
    max_drawdown,
    profit_factor,
    win_rate,
)
from platforms.single_account.sim.orders import Bar, Order

CONFIG_PATH = Path(__file__).with_name("config.json")
SMOKE_STRATEGY = "smoke"
SMOKE_QTY = 0.01
SMOKE_ENTRY_BAR = 2   # 第 3 根(0 起)
SMOKE_EXIT_BAR = 19   # 第 20 根


class ReplayFeed:
    """从任务A的 market.db klines 读已收盘K线;支持多 symbol 按时间归并(§4)。"""

    def __init__(self, market_conn: sqlite3.Connection, venue: str,
                 symbol: Union[str, Sequence[str]], tf: str,
                 start_ts: int, end_ts: int) -> None:
        self.conn = market_conn
        self.venue = venue
        self.symbols = [symbol] if isinstance(symbol, str) else list(symbol)
        self.tf = tf
        self.start_ts = start_ts
        self.end_ts = end_ts

    def __iter__(self) -> Iterator[Bar]:
        tf_sec = TF_SECONDS[self.tf]
        placeholders = ",".join("?" for _ in self.symbols)
        rows = self.conn.execute(
            f"SELECT symbol, open_ts, open, high, low, close, volume FROM klines "
            f"WHERE venue=? AND symbol IN ({placeholders}) AND tf=? "
            f"AND open_ts>=? AND open_ts+?<=? ORDER BY open_ts, symbol",
            (self.venue, *self.symbols, self.tf, self.start_ts, tf_sec, self.end_ts))
        for symbol, open_ts, o, h, l, c, v in rows:
            if None in (o, h, l, c):
                continue
            yield Bar(symbol=str(symbol), tf=self.tf, open_ts=int(open_ts),
                      open=float(o), high=float(h), low=float(l), close=float(c),
                      volume=v)


def seed_demo_klines(market_conn: sqlite3.Connection, venue: str, symbol: str,
                     tf: str, days: float, now: Optional[float] = None,
                     base_price: float = 100.0, drift_rate: float = 0.0004) -> int:
    """合成K线(正弦+可选趋势漂移),走与 recorder 相同的 upsert 路径。"""
    tf_sec = TF_SECONDS[tf]
    end = int(now if now is not None else time.time()) // tf_sec * tf_sec
    n = int(days * 86400 // tf_sec)
    rows = []
    bars_per_block = int(3 * 86400 // tf_sec)  # 3 天一段的静/动政权交替
    for i in range(n):
        open_ts = end - (n - i) * tf_sec
        calm = (i // bars_per_block) % 2 == 0
        amp = 0.006 if calm else 0.02
        drift = base_price * drift_rate * i
        wave = base_price * amp * math.sin(i / 9.0)
        o = base_price + drift + wave
        c = base_price + drift + base_price * amp * math.sin((i + 1) / 9.0)
        if calm and i % 67 < 8:                  # 静区偶发 4 根渐进回落+4 根修复(均值回归演示)
            k = i % 67
            dip = base_price * 0.004 * ((k + 1) if k < 4 else (8 - k))
            c -= dip
        h = max(o, c) * 1.003
        l = min(o, c) * 0.997
        volume = 10.0 + 3000.0 * abs(c - o) / base_price  # 量随波动放大(动量突破演示)
        rows.append((venue, symbol, tf, open_ts, o, h, l, c, volume))
    upsert_klines(market_conn, rows)
    return len(rows)


def seed_demo_market(market_conn: sqlite3.Connection, venue: str, symbols: List[str],
                     tf: str, days: float, now: Optional[float] = None) -> dict:
    """§4 验收用合成数据:K线 + funding(带高费率片段)+ RWA basis(RTH 内含跳动)。"""
    from platforms.single_account.recorders.rwa_basis_recorder import is_rth

    end = int(now if now is not None else time.time())
    counts = {"klines": 0, "funding": 0, "basis": 0}
    rwa = {"XAU", "QQQ", "NVDA"}
    for i, symbol in enumerate(symbols):
        # 偶数位品种带趋势(动量演示),奇数位震荡(funding/均值回归演示)
        counts["klines"] += seed_demo_klines(market_conn, venue, symbol, tf, days,
                                             now=end, base_price=100.0 + 10 * i,
                                             drift_rate=0.0004 if i % 2 == 0 else 0.0)
        funding_rows = []
        for h in range(int(days * 24), 0, -1):
            ts = (end - h * 3600) // 3600 * 3600
            episode = (h // 12) % 6 == 0        # 每 72h 一段 12h 高费率
            rate = 0.0002 if episode else 0.00001
            funding_rows.append((venue, symbol, ts, rate, 1.0, None))
        market_conn.executemany(
            "INSERT OR REPLACE INTO funding(venue, symbol, ts, rate, interval_hours, "
            "predicted_next) VALUES(?,?,?,?,?,?)", funding_rows)
        counts["funding"] += len(funding_rows)
        if symbol in rwa:
            basis_rows = []
            base = 100.0 + 10 * i
            for m in range(int(days * 1440), 0, -1):
                ts = end - m * 60
                if not is_rth(datetime.fromtimestamp(ts, tz=timezone.utc)):
                    continue
                mark = base + 0.3 * math.sin(m / 30.0)
                ref = mark * (1.006 if m % 59 < 2 else 1.0)   # 约每小时 2 分钟 ref 上跳 60bps
                basis_rows.append((ts, venue, symbol, mark, mark - 0.05, mark + 0.05,
                                   None, ref, ts, "demo"))
            market_conn.executemany(
                "INSERT OR REPLACE INTO basis_ticks(ts, venue, symbol, platform_mark, "
                "platform_bid, platform_ask, platform_index, ref_price, ref_ts, ref_source) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)", basis_rows)
            counts["basis"] += len(basis_rows)
    market_conn.commit()
    return counts


# ---------------------------------------------------------------------------
# 施工包02 · §4 组合层
# ---------------------------------------------------------------------------

def _strategy_registry() -> dict:
    from platforms.single_account.strategies.funding_carry import FundingCarry
    from platforms.single_account.strategies.mean_reversion import MeanReversion
    from platforms.single_account.strategies.momentum_breakout import MomentumBreakout
    from platforms.single_account.strategies.rwa_basis import RwaBasis

    return {cls.name: cls for cls in (FundingCarry, MomentumBreakout, MeanReversion, RwaBasis)}


class PortfolioEngine:
    """§4 优先级链:event_gate → 反向互斥 → 全局当日亏损 → 总敞口 → 每策略名义上限。
    平仓信号不经任何闸门(平仓照常)。"""

    def __init__(self, broker: PaperBroker, paper_conn: sqlite3.Connection,
                 market_conn: sqlite3.Connection, strategy_names: List[str],
                 sim_cfg: dict, venue: str = "decibel") -> None:
        import pandas as pd

        from platforms.single_account.strategies.base import MarketData
        from platforms.single_account.strategies.event_gate import EventGate

        self._pd = pd
        self.broker = broker
        self.paper_conn = paper_conn
        registry = _strategy_registry()
        unknown = [n for n in strategy_names if n not in registry]
        if unknown:
            raise ValueError(f"未知策略: {unknown};可用: {sorted(registry)}")
        strat_cfgs = sim_cfg.get("strategies") if isinstance(sim_cfg.get("strategies"), dict) else {}
        self.strategies = [registry[n](strat_cfgs.get(n) or {}) for n in strategy_names]
        self.strat_cfgs = strat_cfgs
        self.gate = EventGate(paper_conn=paper_conn)
        self.data = MarketData(market_conn, venue)
        self.daily_loss_stop_pct = float(sim_cfg.get("daily_loss_stop_pct") or 0.02)
        self.max_gross_exposure_mult = float(sim_cfg.get("max_gross_exposure_mult") or 1.5)
        self._history: dict = {}
        self._bar_index = 0
        self._day: Optional[str] = None
        self._day_start_equity: float = 0.0

    # ---------- 数据与记录 ----------

    def _append_history(self, bar: Bar) -> None:
        hist = self._history.setdefault(bar.symbol, {"ts": [], "open": [], "high": [],
                                                     "low": [], "close": [], "volume": []})
        hist["ts"].append(bar.open_ts)
        hist["open"].append(bar.open)
        hist["high"].append(bar.high)
        hist["low"].append(bar.low)
        hist["close"].append(bar.close)
        hist["volume"].append(bar.volume if bar.volume is not None else 0.0)

    def _bars_df(self, symbol: str):
        hist = self._history[symbol]
        frame = self._pd.DataFrame({k: hist[k] for k in ("open", "high", "low", "close", "volume")},
                                   index=hist["ts"], dtype=float)
        return frame

    def _decision(self, ts: int, strategy: str, symbol: str, action: str,
                  taken: bool, skip_reason: str, tags: Optional[dict] = None) -> None:
        with self.paper_conn:
            persistence.insert_decision(
                self.paper_conn, ts, strategy, symbol, action,
                json.dumps(tags or {}, ensure_ascii=False, default=str),
                1 if taken else 0, skip_reason)

    # ---------- 风险量度 ----------

    def _gross_exposure(self) -> float:
        total = 0.0
        for pos in self.broker.account.positions.values():
            mark = self.broker.account.last_marks.get(pos.symbol, pos.entry_price)
            total += abs(pos.signed_qty) * mark
        return total

    def _strategy_notional(self, strategy: str) -> float:
        total = 0.0
        for pos in self.broker.account.positions.values():
            if pos.strategy == strategy:
                mark = self.broker.account.last_marks.get(pos.symbol, pos.entry_price)
                total += abs(pos.signed_qty) * mark
        return total

    def _strategy_daily_loss_pct(self, strategy: str, day_start_ts: int) -> float:
        row = self.paper_conn.execute(
            "SELECT COALESCE(SUM(net_pnl),0) FROM positions_closed "
            "WHERE strategy=? AND exit_ts>=?", (strategy, day_start_ts)).fetchone()
        net = float(row[0] or 0.0)
        if net >= 0 or self._day_start_equity <= 0:
            return 0.0
        return -net / self._day_start_equity

    # ---------- 主入口 ----------

    def process_bar(self, bar: Bar, equity: float) -> None:
        self._bar_index += 1
        self._append_history(bar)
        day = datetime.fromtimestamp(bar.close_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if day != self._day:
            self._day = day
            self._day_start_equity = equity
        day_start_ts = int(datetime.strptime(day, "%Y-%m-%d")
                           .replace(tzinfo=timezone.utc).timestamp())

        self._cancel_stale_entries(bar)
        self._force_flat_for_events(bar)

        signals = []
        bars_df = self._bars_df(bar.symbol)
        from platforms.single_account.strategies.base import Context

        for strat in self.strategies:
            if bar.symbol not in (strat.cfg.get("symbols") or []):
                continue
            extras = {
                "paper_conn": self.paper_conn,
                "open_positions_count": sum(1 for p in self.broker.account.positions.values()
                                            if p.strategy == strat.name),
                "strategy_daily_loss_pct": self._strategy_daily_loss_pct(strat.name, day_start_ts),
            }
            ctx = Context(bar=bar, bars=bars_df,
                          position=self.broker.account.position_for(strat.name, bar.symbol),
                          equity=equity, funding_rate=None, event_gate=self.gate,
                          data=self.data, now_ts=bar.close_ts, extras=extras)
            try:
                signals.extend(strat.on_bar(ctx) or [])
            except Exception as exc:  # 策略异常不炸 runner,记 decisions
                self._decision(bar.close_ts, strat.name, bar.symbol, "error", False,
                               f"strategy_exception:{type(exc).__name__}:{str(exc)[:120]}")
            for skip in ctx.extras.get("skip_events", []):
                self._decision(bar.close_ts, skip["strategy"], skip["symbol"],
                               "open", False, skip["reason"])

        for sig in signals:
            self._route_signal(sig, bar, equity)

        # 策略可能直接收紧了持仓止损 → 持久化运行时元数据
        with self.paper_conn:
            persistence.save_runtime_meta(self.paper_conn, self.broker.account.positions,
                                          self.broker.pending)

    # ---------- 信号路由(§4 优先级链) ----------

    def _route_signal(self, sig, bar: Bar, equity: float) -> None:
        ts = bar.close_ts
        if sig.action == "close":
            pos = self.broker.account.position_for(sig.strategy, sig.symbol)
            if pos is None:
                self._decision(ts, sig.strategy, sig.symbol, "close", False, "no_position")
                return
            order = Order(strategy=sig.strategy, symbol=sig.symbol,
                          side="sell" if pos.side == "long" else "buy",
                          type="market", qty=min(sig.qty, pos.qty), created_ts=ts,
                          reason=sig.reason, reduce_only=True, tags=dict(sig.tags))
            self.broker.submit_order(order)
            self._decision(ts, sig.strategy, sig.symbol, "close",
                           order.status != "rejected", order.reason if order.status == "rejected" else "",
                           sig.tags)
            return

        # 1) event_gate
        blocked, reason = self.gate.blocked(sig.symbol, ts)
        if blocked:
            self._decision(ts, sig.strategy, sig.symbol, sig.action, False, reason, sig.tags)
            return
        # 2) 同 symbol 反向互斥(跨策略;后到 reject)
        want_long = sig.action == "open_long"
        for pos in self.broker.account.positions.values():
            if pos.symbol == sig.symbol and (pos.side == "long") != want_long:
                self._decision(ts, sig.strategy, sig.symbol, sig.action, False,
                               f"opposite_position_exists:{pos.strategy}", sig.tags)
                return
        for pending in self.broker.pending:
            if pending.symbol == sig.symbol and not pending.reduce_only \
                    and (pending.side == "buy") != want_long:
                self._decision(ts, sig.strategy, sig.symbol, sig.action, False,
                               f"opposite_pending_order:{pending.strategy}", sig.tags)
                return
        # 3) 全局当日亏损 ≥2% 起始权益 → 拒绝所有 open
        if self._day_start_equity > 0 and \
                1.0 - equity / self._day_start_equity >= self.daily_loss_stop_pct:
            self._decision(ts, sig.strategy, sig.symbol, sig.action, False,
                           "global_daily_loss_stop", sig.tags)
            return
        # 4) 总名义敞口 > 1.5×权益
        entry_ref = sig.limit_price or bar.close
        if self._gross_exposure() + sig.qty * entry_ref > self.max_gross_exposure_mult * equity:
            self._decision(ts, sig.strategy, sig.symbol, sig.action, False,
                           "gross_exposure_cap", sig.tags)
            return
        # 5) 每策略独立名义上限
        cap_pct = (self.strat_cfgs.get(sig.strategy) or {}).get("max_notional_pct")
        if cap_pct is not None and \
                self._strategy_notional(sig.strategy) + sig.qty * entry_ref > float(cap_pct) * equity:
            self._decision(ts, sig.strategy, sig.symbol, sig.action, False,
                           "strategy_notional_cap", sig.tags)
            return

        tags = dict(sig.tags)
        tags["submitted_bar"] = self._bar_index
        order = Order(strategy=sig.strategy, symbol=sig.symbol,
                      side="buy" if want_long else "sell",
                      type="limit" if sig.limit_price is not None else "market",
                      qty=sig.qty, limit_price=sig.limit_price, created_ts=ts,
                      reason=sig.reason, stop=sig.stop_price, tp=sig.tp_price, tags=tags)
        self.broker.submit_order(order)
        self._decision(ts, sig.strategy, sig.symbol, sig.action,
                       order.status != "rejected",
                       order.reason if order.status == "rejected" else "", sig.tags)

    # ---------- 辅助规则 ----------

    def _cancel_stale_entries(self, bar: Bar) -> None:
        for order in list(self.broker.pending):
            if order.reduce_only or order.symbol != bar.symbol:
                continue
            limit = order.tags.get("cancel_after_bars")
            submitted = order.tags.get("submitted_bar")
            if limit is not None and submitted is not None \
                    and self._bar_index - int(submitted) >= int(limit):
                self.broker.cancel_order(order.order_id)
                self._decision(bar.close_ts, order.strategy, order.symbol, "cancel",
                               True, f"entry_unfilled_{limit}bars")

    def _force_flat_for_events(self, bar: Bar) -> None:
        for pos in list(self.broker.account.positions.values()):
            if pos.symbol != bar.symbol:
                continue
            flat, reason = self.gate.force_flat(pos.symbol, bar.close_ts)
            if not flat:
                continue
            already = any(o.reduce_only and o.strategy == pos.strategy
                          and o.symbol == pos.symbol for o in self.broker.pending)
            if already:
                continue
            order = Order(strategy=pos.strategy, symbol=pos.symbol,
                          side="sell" if pos.side == "long" else "buy",
                          type="market", qty=pos.qty, created_ts=bar.close_ts,
                          reason=reason, reduce_only=True)
            self.broker.submit_order(order)
            self._decision(bar.close_ts, pos.strategy, pos.symbol, "close", True, reason)


def strategy_performance(paper_conn: sqlite3.Connection,
                         strategies: Optional[List[str]] = None) -> dict:
    """§4:每策略绩效摘要(笔数/胜率/PF/期望R/净额/费用占比;MDD 为组合级)。
    零成交策略也输出条目(trades=0)。"""
    out: dict = {}
    equity_series = [row[0] for row in paper_conn.execute(
        "SELECT equity FROM equity_snapshots ORDER BY ts")]
    portfolio_mdd = max_drawdown(equity_series) if equity_series else 0.0
    names = set(strategies or [])
    names.update(s for (s,) in paper_conn.execute(
        "SELECT DISTINCT strategy FROM positions_closed"))
    for strategy in sorted(names):
        rows = paper_conn.execute(
            "SELECT net_pnl, r_multiple, gross_pnl, fees, funding FROM positions_closed "
            "WHERE strategy=?", (strategy,)).fetchall()
        nets = [r[0] or 0.0 for r in rows]
        gross_abs = sum(abs(r[2] or 0.0) for r in rows)
        fees = sum(r[3] or 0.0 for r in rows)
        out[strategy] = {
            "trades": len(rows),
            "win_rate": round(win_rate(nets), 4) if nets else None,
            "profit_factor": (round(profit_factor(nets), 4)
                              if profit_factor(nets) is not None else None),
            "expectancy_r": (round(expectancy_r([r[1] for r in rows]), 4)
                             if expectancy_r([r[1] for r in rows]) is not None else None),
            "net_pnl": round(sum(nets), 6),
            "fees": round(fees, 6),
            "funding": round(sum(r[4] or 0.0 for r in rows), 6),
            "fees_ratio_of_gross": round(fees / gross_abs, 4) if gross_abs else None,
            "portfolio_mdd": round(portfolio_mdd, 6),
        }
    return out


def run_replay(market_db: Optional[Path] = None, paper_db: Optional[Path] = None,
               symbol: str = "BTC", tf: str = "15m", days: float = 7.0,
               smoke: bool = False, venue: str = "decibel",
               now: Optional[float] = None, sim_cfg: Optional[dict] = None,
               strategies: Optional[List[str]] = None,
               symbols: Optional[List[str]] = None) -> dict:
    market_conn = open_market_db(market_db or MARKET_DB_PATH)
    paper_conn = persistence.open_paper_db(paper_db)

    if sim_cfg is None:
        cfg = load_config(CONFIG_PATH)
        sim_cfg = cfg.get("sim") if isinstance(cfg.get("sim"), dict) else {}
    broker = PaperBroker.from_db(paper_conn, sim_cfg=sim_cfg)
    engine = FundingEngine(paper_conn, broker.account,
                           market_db_rate_provider(market_conn, venue),
                           interval_hours=float(sim_cfg.get("funding_interval_hours") or 8))
    replay_symbols = symbols or [symbol]
    portfolio = (PortfolioEngine(broker, paper_conn, market_conn, strategies,
                                 sim_cfg, venue=venue) if strategies else None)

    end_ts = int(now if now is not None else time.time())
    start_ts = end_ts - int(days * 86400)
    resume_after = int(persistence.get_meta(paper_conn, "last_replay_close_ts") or 0)

    smoke_orders = paper_conn.execute(
        "SELECT COUNT(*) FROM orders WHERE strategy=?", (SMOKE_STRATEGY,)).fetchone()[0]

    bars = processed = 0
    equity = broker.account.mark_to_market({p.symbol: p.entry_price
                                            for p in broker.account.positions.values()})[0]
    for index, bar in enumerate(ReplayFeed(market_conn, venue, replay_symbols, tf,
                                           start_ts, end_ts)):
        bars += 1
        if bar.close_ts <= resume_after:
            if portfolio is not None:
                portfolio._append_history(bar)   # 指标需要完整历史
                portfolio._bar_index += 1
            continue  # 断点续跑:已回放过的 bar 跳过(§B9 无重复行)
        processed += 1

        broker.check_stops(bar)
        broker.process_pending(bar)
        engine.accrue_if_due(bar.close_ts, {bar.symbol: bar.close})
        equity, unrealized = broker.account.mark_to_market({bar.symbol: bar.close})

        if portfolio is not None:
            portfolio.process_bar(bar, equity)
            equity, unrealized = broker.account.mark_to_market({bar.symbol: bar.close})

        peak = float(persistence.get_meta(paper_conn, "equity_peak") or equity)
        peak = max(peak, equity)
        drawdown = 1.0 - equity / peak if peak > 0 else 0.0
        with paper_conn:
            persistence.snapshot_equity(paper_conn, bar.close_ts, equity,
                                        broker.account.cash, unrealized, drawdown)
            persistence.set_meta(paper_conn, "equity_peak", str(peak))
            persistence.set_meta(paper_conn, "last_replay_close_ts", str(bar.close_ts))

        if smoke:
            if index == SMOKE_ENTRY_BAR and smoke_orders == 0:
                broker.submit_order(Order(strategy=SMOKE_STRATEGY, symbol=symbol,
                                          side="buy", type="market", qty=SMOKE_QTY,
                                          created_ts=bar.close_ts, reason="smoke entry"))
                smoke_orders += 1
            elif index == SMOKE_EXIT_BAR and smoke_orders == 1:
                broker.submit_order(Order(strategy=SMOKE_STRATEGY, symbol=symbol,
                                          side="sell", type="market", qty=SMOKE_QTY,
                                          created_ts=bar.close_ts, reason="smoke close",
                                          reduce_only=True))
                smoke_orders += 1

    aggregate_strategy_daily(paper_conn)
    summary = {
        "symbols": replay_symbols, "tf": tf, "days": days, "bars_total": bars,
        "bars_processed": processed, "equity": round(equity, 6),
        "cash": round(broker.account.cash, 6),
        "open_positions": len(broker.account.positions),
        "orders": paper_conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0],
        "fills": paper_conn.execute("SELECT COUNT(*) FROM fills").fetchone()[0],
        "positions_closed": paper_conn.execute("SELECT COUNT(*) FROM positions_closed").fetchone()[0],
        "equity_snapshots": paper_conn.execute("SELECT COUNT(*) FROM equity_snapshots").fetchone()[0],
        "decisions": paper_conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
    }
    if strategies:
        summary["strategies"] = strategies
        summary["performance"] = strategy_performance(paper_conn, strategies)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="SA paper simulator runner (replay).")
    parser.add_argument("--replay", action="store_true")
    parser.add_argument("--seed-demo", action="store_true",
                        help="写合成K线/funding/basis 进 market.db(本地验证用)")
    parser.add_argument("--symbol", default="BTC")
    parser.add_argument("--symbols", default="",
                        help="逗号分隔多品种(组合层回放);留空用 --symbol")
    parser.add_argument("--tf", default="15m", choices=sorted(TF_SECONDS))
    parser.add_argument("--days", type=float, default=7.0)
    parser.add_argument("--smoke", action="store_true",
                        help="第3根bar注入市价买、第20根注入平仓单(单 symbol 路径)")
    parser.add_argument("--strategies", default="",
                        help="逗号分隔策略名,或 all(§4 组合层);留空=不接策略(01 行为)")
    parser.add_argument("--venue", default="decibel")
    args = parser.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()] or None
    if args.seed_demo:
        conn = open_market_db()
        counts = seed_demo_market(conn, args.venue, symbols or [args.symbol],
                                  args.tf, args.days)
        print(json.dumps({"seeded": counts, "symbols": symbols or [args.symbol],
                          "tf": args.tf}))
        return
    if not args.replay:
        raise SystemExit("仅支持 --replay(或 --seed-demo);实时行情接入不属于本批")
    strategies: Optional[List[str]] = None
    if args.strategies:
        strategies = (sorted(_strategy_registry())
                      if args.strategies.strip() == "all"
                      else [s.strip() for s in args.strategies.split(",") if s.strip()])
    summary = run_replay(symbol=args.symbol, tf=args.tf, days=args.days,
                         smoke=args.smoke, venue=args.venue,
                         strategies=strategies, symbols=symbols)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
