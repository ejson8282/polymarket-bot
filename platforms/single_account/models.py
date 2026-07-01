from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class MarketSignal:
    symbol: str
    category: str
    price: float = 0.0
    quote_age_seconds: float = 999999.0
    spread_bps: float = 999999.0
    volume_24h_usdc: float = 0.0
    funding_bps_8h: float = 0.0
    trend_score: float = 0.0
    volatility_score: float = 0.5
    liquidity_score: float = 0.0
    open_interest_usdc: float = 0.0
    data_source: str = "config_fallback"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class StrategyDecision:
    symbol: str
    category: str
    strategy: str
    strategy_label: str
    decision: str
    reason: str
    score: float
    side: str
    target_hold_hours: str
    max_notional_usdc: float
    max_leverage: float
    estimated_entry_cost_bps: float
    expected_funding_bps_8h: float
    quote_age_seconds: float
    spread_bps: float
    data_source: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

