from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from platforms.single_account.models import MarketSignal


def resolve_path(config_path: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (config_path.parent / path).resolve()


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_market_signals(config_path: Path, cfg: dict[str, Any]) -> list[MarketSignal]:
    input_cfg = cfg.get("input") if isinstance(cfg.get("input"), dict) else {}
    snapshot_path = resolve_path(
        config_path,
        str(input_cfg.get("market_snapshot_path") or "../../data/single_account_market_snapshot.json"),
    )
    snapshot_rows = _load_snapshot_rows(snapshot_path)
    if snapshot_rows:
        return [_signal_from_row(row) for row in snapshot_rows if isinstance(row, dict)]
    return _fallback_universe(cfg)


def _load_snapshot_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    rows = payload.get("candidates") if isinstance(payload, dict) else None
    if isinstance(rows, list):
        return [row for row in rows if isinstance(row, dict)]
    return []


def _signal_from_row(row: dict[str, Any]) -> MarketSignal:
    return MarketSignal(
        symbol=str(row.get("symbol") or "").upper(),
        category=str(row.get("category") or "unknown"),
        price=_float(row.get("price")),
        quote_age_seconds=_float(row.get("quote_age_seconds"), 999999.0),
        spread_bps=_float(row.get("spread_bps"), 999999.0),
        volume_24h_usdc=_float(row.get("volume_24h_usdc")),
        funding_bps_8h=_float(row.get("funding_bps_8h")),
        trend_score=_clamp(_float(row.get("trend_score")), -1.0, 1.0),
        volatility_score=_clamp(_float(row.get("volatility_score"), 0.5), 0.0, 1.0),
        liquidity_score=_clamp(_float(row.get("liquidity_score")), 0.0, 1.0),
        open_interest_usdc=_float(row.get("open_interest_usdc")),
        data_source=str(row.get("data_source") or "snapshot"),
    )


def _fallback_universe(cfg: dict[str, Any]) -> list[MarketSignal]:
    rows = cfg.get("universe") if isinstance(cfg.get("universe"), list) else []
    out: list[MarketSignal] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("symbol") or "").upper()
        if not symbol:
            continue
        out.append(
            MarketSignal(
                symbol=symbol,
                category=str(row.get("category") or "unknown"),
                data_source="config_fallback",
            )
        )
    return out


def _float(value: object, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))

