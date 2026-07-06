"""施工包01 · §B1 PaperAccount:cash、positions、equity、mark_to_market。

记账口径(与 §B8 测试5 一致):
- 买入开仓:cash -= 成交价×qty + fee;equity = cash + Σ signed_qty×mark。
- 卖出开仓(空头):cash += 成交价×qty − fee;equity 同上(signed_qty 为负)。
- unrealized = Σ (mark − entry)×signed_qty(未实现盈亏)。
"""
from __future__ import annotations

from typing import Dict, Optional, Tuple

from platforms.single_account.sim.position import Position


class PaperAccount:
    def __init__(self, initial_cash: float) -> None:
        self.initial_cash = float(initial_cash)
        self.cash = float(initial_cash)
        self.positions: Dict[Tuple[str, str], Position] = {}
        self.last_marks: Dict[str, float] = {}

    def add_position(self, position: Position) -> None:
        self.positions[position.key()] = position

    def remove_position(self, key: Tuple[str, str]) -> Optional[Position]:
        return self.positions.pop(key, None)

    def position_for(self, strategy: str, symbol: str) -> Optional[Position]:
        return self.positions.get((strategy, symbol))

    def mark_to_market(self, marks) -> Tuple[float, float]:
        """marks: {symbol: price} 或单一 float(单品种回放)。返回 (equity, unrealized)。"""
        if isinstance(marks, (int, float)):
            marks = {pos.symbol: float(marks) for pos in self.positions.values()}
        self.last_marks.update(marks)
        equity = self.cash
        unrealized = 0.0
        for pos in self.positions.values():
            mark = self.last_marks.get(pos.symbol)
            if mark is None:
                mark = pos.entry_price
            equity += pos.signed_qty * mark
            unrealized += (mark - pos.entry_price) * pos.signed_qty
        return equity, unrealized
