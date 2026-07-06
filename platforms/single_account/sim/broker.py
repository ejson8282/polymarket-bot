"""施工包01 · §B3 PaperBroker:撮合与止损检查(核心,规则精确执行)。

1. 市价单:下一根 bar 的 open 成交;slip_bps = default_spread_bps/2 + fixed_impact_bps
   (封顶 slip_cap_bps);买入成交价 = open×(1+slip/10000),卖出对称;taker 费。
2. 限价单严格穿越:买 bar.low < limit(触碰不算)成交价=限价 maker 费;卖对称。不部分成交。
3. 每根 bar 先 check_stops 再 process_pending;多头 low<=stop 触发止损(价=stop)、
   high>=tp 止盈;同 bar 双触发按止损结算(最坏假设)且 sim_meta.ambiguous_bars+=1;空头对称。
4. 平仓:gross=(exit−entry)×qty×方向;net=gross−双边fees−funding_paid;
   r_multiple=(exit−entry)/(entry−stop)(长短通用:方向因子在分子分母同时出现相互抵消,
   长仓与规格公式逐字一致;无 stop 记 NULL)。
5. 每 (strategy,symbol) 至多一个持仓,同向加仓/反向对锁 reject。
"""
from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional, Tuple

from platforms.single_account.sim import persistence
from platforms.single_account.sim.account import PaperAccount
from platforms.single_account.sim.orders import Bar, Fill, Order
from platforms.single_account.sim.position import Position

DEFAULT_SIM_CFG = {
    "initial_cash": 10000,
    "taker_fee_bps": 5,
    "maker_fee_bps": 2,
    "default_spread_bps": 10,
    "fixed_impact_bps": 2,
    "slip_cap_bps": 25,
    "funding_interval_hours": 8,
}


class PaperBroker:
    def __init__(self, conn: sqlite3.Connection, sim_cfg: Optional[dict] = None,
                 account: Optional[PaperAccount] = None,
                 pending: Optional[List[Order]] = None) -> None:
        self.conn = conn
        cfg = dict(DEFAULT_SIM_CFG)
        cfg.update(sim_cfg or {})
        self.cfg = cfg
        self.account = account or PaperAccount(float(cfg["initial_cash"]))
        self.pending: List[Order] = pending or []
        with conn:
            if persistence.get_meta(conn, "initial_cash") is None:
                persistence.set_meta(conn, "initial_cash", str(self.account.initial_cash))

    @classmethod
    def from_db(cls, conn: sqlite3.Connection, sim_cfg: Optional[dict] = None) -> "PaperBroker":
        cfg = dict(DEFAULT_SIM_CFG)
        cfg.update(sim_cfg or {})
        initial = float(persistence.get_meta(conn, "initial_cash") or cfg["initial_cash"])
        cash, positions, pending = persistence.load_state(conn, initial)
        account = PaperAccount(initial)
        account.cash = cash
        account.positions = positions
        return cls(conn, sim_cfg=cfg, account=account, pending=pending)

    # ---------- 下单(§B3.6 单持仓规则) ----------

    def submit_order(self, order: Order) -> Order:
        key = (order.strategy, order.symbol)
        existing = self.account.positions.get(key)
        with self.conn:
            if order.reduce_only:
                if existing is None:
                    order.status, order.reason = "rejected", "reduce_only 无持仓可平"
                elif (existing.side == "long") != (order.side == "sell"):
                    order.status, order.reason = "rejected", "reduce_only 方向须与持仓相反"
            elif existing is not None:
                order.status = "rejected"
                order.reason = ("加仓不支持(每 strategy+symbol 至多一个持仓)"
                                if (existing.side == "long") == (order.side == "buy")
                                else "反向对锁不支持(先平仓)")
            elif any(o for o in self.pending
                     if (o.strategy, o.symbol) == key and not o.reduce_only):
                order.status, order.reason = "rejected", "已有同 strategy+symbol 挂单"
            persistence.insert_order(self.conn, order)
            if order.status == "new":
                self.pending.append(order)
            persistence.save_runtime_meta(self.conn, self.account.positions, self.pending)
        return order

    def cancel_order(self, order_id: str) -> None:
        with self.conn:
            for order in list(self.pending):
                if order.order_id == order_id:
                    order.status = "canceled"
                    self.pending.remove(order)
                    persistence.update_order_status(self.conn, order_id, "canceled", order.reason)
            persistence.save_runtime_meta(self.conn, self.account.positions, self.pending)

    # ---------- 撮合(§B3.1/§B3.2) ----------

    def slip_bps(self) -> float:
        raw = self.cfg["default_spread_bps"] / 2.0 + self.cfg["fixed_impact_bps"]
        return min(raw, float(self.cfg["slip_cap_bps"]))

    def process_pending(self, bar: Bar) -> List[Fill]:
        fills: List[Fill] = []
        for order in list(self.pending):
            if order.symbol != bar.symbol:
                continue
            if order.type == "market":
                slip = self.slip_bps()
                sign = 1.0 if order.side == "buy" else -1.0
                price = bar.open * (1.0 + sign * slip / 10000.0)
                fills.append(self._execute(order, price, bar.open_ts,
                                           self.cfg["taker_fee_bps"], slip))
            elif order.type == "limit":
                assert order.limit_price is not None
                crossed = (bar.low < order.limit_price) if order.side == "buy" \
                    else (bar.high > order.limit_price)  # 严格穿越,触碰不算
                if crossed:
                    fills.append(self._execute(order, float(order.limit_price), bar.open_ts,
                                               self.cfg["maker_fee_bps"], 0.0))
        return fills

    def _execute(self, order: Order, price: float, ts: int,
                 fee_bps: float, slip_bps: float) -> Fill:
        qty = order.qty
        position = self.account.positions.get((order.strategy, order.symbol))
        partial = False
        if order.reduce_only and position is not None:
            # 施工包02扩展:qty 明确小于持仓量 → 部分平仓;否则按01语义全平
            if 0 < order.qty < position.qty - 1e-12:
                partial = True
            else:
                qty = position.qty
        fee = price * qty * fee_bps / 10000.0
        fill = Fill(order_id=order.order_id, ts=ts, price=price, qty=qty,
                    fee=fee, slippage_bps=slip_bps)
        with self.conn:
            if order.side == "buy":
                self.account.cash -= price * qty + fee
            else:
                self.account.cash += price * qty - fee
            persistence.insert_fill(self.conn, fill)
            order.status = "filled"
            persistence.update_order_status(self.conn, order.order_id, "filled", order.reason)
            if order in self.pending:
                self.pending.remove(order)
            if order.reduce_only and position is not None:
                if partial:
                    self._settle_partial_close(position, qty, price, ts,
                                               order.reason or "partial_close", exit_fee=fee)
                else:
                    self._settle_close(position, price, ts, order.reason or "close_order",
                                       exit_fee=fee)
            else:
                self.account.add_position(Position(
                    strategy=order.strategy, symbol=order.symbol,
                    side="long" if order.side == "buy" else "short",
                    qty=qty, entry_ts=ts, entry_price=price,
                    stop=order.stop, tp=order.tp, entry_fee=fee,
                    tags=dict(order.tags),
                ))
            persistence.save_runtime_meta(self.conn, self.account.positions, self.pending)
        return fill

    def _settle_partial_close(self, pos: Position, close_qty: float, exit_price: float,
                              exit_ts: int, exit_reason: str, exit_fee: float) -> dict:
        """施工包02扩展:部分平仓——按比例分摊入场费与 funding,剩余持仓继续。"""
        fraction = close_qty / pos.qty
        entry_fee_part = pos.entry_fee * fraction
        funding_part = pos.funding_paid * fraction
        direction = pos.direction
        gross = (exit_price - pos.entry_price) * close_qty * direction
        fees = entry_fee_part + exit_fee
        net = gross - fees - funding_part
        r_multiple = None
        if pos.stop is not None and pos.entry_price != pos.stop:
            r_multiple = (exit_price - pos.entry_price) / (pos.entry_price - pos.stop)
        row = {
            "strategy": pos.strategy, "symbol": pos.symbol, "side": pos.side,
            "qty": close_qty, "entry_ts": pos.entry_ts, "entry_price": pos.entry_price,
            "exit_ts": exit_ts, "exit_price": exit_price,
            "gross_pnl": gross, "fees": fees, "funding": funding_part, "net_pnl": net,
            "r_multiple": r_multiple, "exit_reason": exit_reason,
            "holding_secs": max(0, exit_ts - pos.entry_ts),
            "tags_json": _tags_json(pos.tags),
        }
        persistence.insert_position_closed(self.conn, row)
        pos.qty -= close_qty
        pos.entry_fee -= entry_fee_part
        pos.funding_paid -= funding_part
        return row

    # ---------- 止损/止盈(§B3.3:先于新信号检查,同 bar 双触发按止损) ----------

    def check_stops(self, bar: Bar) -> List[dict]:
        closed: List[dict] = []
        for key, pos in list(self.account.positions.items()):
            if pos.symbol != bar.symbol:
                continue
            if pos.side == "long":
                stop_hit = pos.stop is not None and bar.low <= pos.stop
                tp_hit = pos.tp is not None and bar.high >= pos.tp
            else:
                stop_hit = pos.stop is not None and bar.high >= pos.stop
                tp_hit = pos.tp is not None and bar.low <= pos.tp
            if not stop_hit and not tp_hit:
                continue
            with self.conn:
                if stop_hit and tp_hit:
                    persistence.incr_meta(self.conn, "ambiguous_bars")
                exit_price = float(pos.stop) if stop_hit else float(pos.tp)
                reason = "stop_loss" if stop_hit else "take_profit"
                exit_order = Order(strategy=pos.strategy, symbol=pos.symbol,
                                   side="sell" if pos.side == "long" else "buy",
                                   type="market", qty=pos.qty, created_ts=bar.open_ts,
                                   status="filled", reason=reason, reduce_only=True)
                fee = exit_price * pos.qty * self.cfg["taker_fee_bps"] / 10000.0
                persistence.insert_order(self.conn, exit_order)
                persistence.insert_fill(self.conn, Fill(
                    order_id=exit_order.order_id, ts=bar.open_ts, price=exit_price,
                    qty=pos.qty, fee=fee, slippage_bps=0.0))
                if pos.side == "long":
                    self.account.cash += exit_price * pos.qty - fee
                else:
                    self.account.cash -= exit_price * pos.qty + fee
                closed.append(self._settle_close(pos, exit_price, bar.open_ts, reason,
                                                 exit_fee=fee))
                persistence.save_runtime_meta(self.conn, self.account.positions, self.pending)
        return closed

    # ---------- 平仓结算(§B3.5;现金流由调用方先行入账) ----------

    def _settle_close(self, pos: Position, exit_price: float, exit_ts: int,
                      exit_reason: str, exit_fee: float) -> dict:
        direction = pos.direction
        gross = (exit_price - pos.entry_price) * pos.qty * direction
        fees = pos.entry_fee + exit_fee
        net = gross - fees - pos.funding_paid
        r_multiple = None
        if pos.stop is not None and pos.entry_price != pos.stop:
            # 方向因子在分子分母同现相消,长仓与规格公式逐字一致,空头按对称原则
            r_multiple = (exit_price - pos.entry_price) / (pos.entry_price - pos.stop)
        row = {
            "strategy": pos.strategy, "symbol": pos.symbol, "side": pos.side,
            "qty": pos.qty, "entry_ts": pos.entry_ts, "entry_price": pos.entry_price,
            "exit_ts": exit_ts, "exit_price": exit_price,
            "gross_pnl": gross, "fees": fees, "funding": pos.funding_paid, "net_pnl": net,
            "r_multiple": r_multiple, "exit_reason": exit_reason,
            "holding_secs": max(0, exit_ts - pos.entry_ts),
            "tags_json": _tags_json(pos.tags),
        }
        persistence.insert_position_closed(self.conn, row)
        self.account.remove_position(pos.key())
        return row


def _tags_json(tags: dict) -> str:
    import json

    try:
        return json.dumps(tags or {}, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        return "{}"
