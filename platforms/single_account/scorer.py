from __future__ import annotations

from typing import Any

from platforms.single_account.models import MarketSignal, StrategyDecision


def score_candidates(cfg: dict[str, Any], signals: list[MarketSignal]) -> list[StrategyDecision]:
    strategies = cfg.get("strategies") if isinstance(cfg.get("strategies"), dict) else {}
    decisions: list[StrategyDecision] = []
    for signal in signals:
        for strategy_name, strategy_cfg in strategies.items():
            if not isinstance(strategy_cfg, dict) or not bool(strategy_cfg.get("enabled", True)):
                continue
            decisions.append(_score_one(cfg, signal, strategy_name, strategy_cfg))
    return sorted(decisions, key=lambda row: (row.decision != "allow", -row.score, row.symbol, row.strategy))


def _score_one(
    cfg: dict[str, Any],
    signal: MarketSignal,
    strategy_name: str,
    strategy_cfg: dict[str, Any],
) -> StrategyDecision:
    risk = cfg.get("risk") if isinstance(cfg.get("risk"), dict) else {}
    max_quote_age = _float(risk.get("max_quote_age_seconds"), 60.0)
    max_spread = _float(risk.get("max_spread_bps"), 12.0)
    min_volume = _float(risk.get("min_volume_24h_usdc"), 1_000_000.0)
    min_score = _float(risk.get("min_score_to_trade"), 65.0)
    max_notional = _float(risk.get("max_notional_usdc"), 100.0)
    max_leverage = _float(risk.get("max_leverage"), 3.0)

    gate_reason = _gate_reason(signal, max_quote_age=max_quote_age, max_spread=max_spread, min_volume=min_volume)
    raw_score = _strategy_score(signal, strategy_name, strategy_cfg, max_spread=max_spread, min_volume=min_volume)
    score = 0.0 if gate_reason else max(0.0, min(100.0, raw_score * _float(strategy_cfg.get("weight"), 1.0)))
    decision = "skip"
    reason = gate_reason
    if not reason:
        if score >= min_score:
            decision = "allow"
            reason = "paper_candidate_passed"
        else:
            reason = "score_below_threshold"

    side = _side_for(signal, strategy_name)
    hold_range = strategy_cfg.get("target_hold_hours") if isinstance(strategy_cfg.get("target_hold_hours"), list) else []
    target_hold_hours = "-".join(str(x) for x in hold_range[:2]) if hold_range else "unset"

    return StrategyDecision(
        symbol=signal.symbol,
        category=signal.category,
        strategy=strategy_name,
        strategy_label=str(strategy_cfg.get("label") or strategy_name),
        decision=decision,
        reason=reason,
        score=round(score, 2),
        side=side,
        target_hold_hours=target_hold_hours,
        max_notional_usdc=max_notional,
        max_leverage=max_leverage,
        estimated_entry_cost_bps=round(max(signal.spread_bps, 0.0), 4),
        expected_funding_bps_8h=round(signal.funding_bps_8h, 4),
        quote_age_seconds=round(signal.quote_age_seconds, 2),
        spread_bps=round(signal.spread_bps, 4),
        data_source=signal.data_source,
    )


def _gate_reason(
    signal: MarketSignal,
    *,
    max_quote_age: float,
    max_spread: float,
    min_volume: float,
) -> str:
    if not signal.symbol:
        return "missing_symbol"
    if signal.data_source == "config_fallback":
        return "waiting_for_market_snapshot"
    if signal.price <= 0:
        return "missing_price"
    if signal.quote_age_seconds > max_quote_age:
        return "stale_quote"
    if signal.spread_bps > max_spread:
        return "spread_too_wide"
    if signal.volume_24h_usdc < min_volume:
        return "low_volume"
    return ""


def _strategy_score(
    signal: MarketSignal,
    strategy_name: str,
    strategy_cfg: dict[str, Any],
    *,
    max_spread: float,
    min_volume: float,
) -> float:
    spread_score = 1.0 - min(max(signal.spread_bps, 0.0), max_spread) / max(max_spread, 0.0001)
    liquidity_score = max(signal.liquidity_score, min(signal.volume_24h_usdc / max(min_volume * 20, 1.0), 1.0))
    category_bonus = 8.0 if signal.category in set(strategy_cfg.get("preferred_categories") or []) else 0.0

    if strategy_name == "funding_carry_rotation":
        funding_score = max(-1.0, min(1.0, signal.funding_bps_8h / 5.0))
        return 45.0 + 25.0 * funding_score + 15.0 * spread_score + 15.0 * liquidity_score + category_bonus
    if strategy_name == "trend_momentum_breakout":
        trend = max(-1.0, min(1.0, signal.trend_score))
        return 42.0 + 32.0 * abs(trend) + 12.0 * spread_score + 14.0 * liquidity_score + category_bonus
    if strategy_name == "mean_reversion_range":
        low_vol = 1.0 - max(0.0, min(1.0, signal.volatility_score))
        return 38.0 + 28.0 * low_vol + 20.0 * spread_score + 10.0 * liquidity_score + category_bonus
    if strategy_name == "rwa_stock_rotation":
        trend = max(-1.0, min(1.0, signal.trend_score))
        return 35.0 + 22.0 * max(trend, 0.0) + 14.0 * spread_score + 12.0 * liquidity_score + category_bonus
    if strategy_name == "event_new_listing_momentum":
        volatility = max(0.0, min(1.0, signal.volatility_score))
        return 30.0 + 18.0 * volatility + 15.0 * spread_score + 10.0 * liquidity_score + category_bonus
    return 0.0


def _side_for(signal: MarketSignal, strategy_name: str) -> str:
    if strategy_name == "funding_carry_rotation":
        return "long" if signal.funding_bps_8h >= 0 else "short"
    if strategy_name == "mean_reversion_range":
        return "short" if signal.trend_score > 0.2 else "long"
    return "long" if signal.trend_score >= 0 else "short"


def _float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default
