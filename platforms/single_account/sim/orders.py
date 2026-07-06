"""施工包01 · §B1 订单/成交/K线原语。"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional


def new_id() -> str:
    return uuid.uuid4().hex


@dataclass
class Bar:
    """回放K线(撮合的最小市场数据单元)。"""

    symbol: str
    tf: str
    open_ts: int
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None

    @property
    def close_ts(self) -> int:
        from platforms.single_account.recorders.kline_recorder import TF_SECONDS

        return self.open_ts + TF_SECONDS[self.tf]


@dataclass
class Order:
    strategy: str
    symbol: str
    side: str                    # 'buy' | 'sell'
    type: str                    # 'market' | 'limit'
    qty: float
    limit_price: Optional[float] = None
    created_ts: int = 0
    status: str = "new"          # new/filled/canceled/rejected
    reason: str = ""
    order_id: str = field(default_factory=new_id)
    # 以下为内存态属性(orders 表无对应列;开仓单携带,落到 Position 上)
    stop: Optional[float] = None
    tp: Optional[float] = None
    reduce_only: bool = False    # True = 平仓单;qty<持仓量时为部分平仓(施工包02扩展)
    tags: dict = field(default_factory=dict)  # 入场依据快照,随持仓落 tags_json(02扩展)


@dataclass
class Fill:
    order_id: str
    ts: int
    price: float
    qty: float
    fee: float
    slippage_bps: float
    fill_id: str = field(default_factory=new_id)
