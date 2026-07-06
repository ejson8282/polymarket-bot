"""施工包01 · §B1 持仓。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Position:
    strategy: str
    symbol: str
    side: str                 # 'long' | 'short'
    qty: float
    entry_ts: int
    entry_price: float
    stop: Optional[float] = None
    tp: Optional[float] = None
    entry_fee: float = 0.0
    funding_paid: float = 0.0  # 累计资金费支出(正=累计付出)
    tags: dict = field(default_factory=dict)  # 入场依据快照(施工包02扩展)

    @property
    def direction(self) -> int:
        return 1 if self.side == "long" else -1

    @property
    def signed_qty(self) -> float:
        return self.qty * self.direction

    def key(self) -> tuple:
        return (self.strategy, self.symbol)
