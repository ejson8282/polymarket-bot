"""Persistent account-local guardrails for the isolated aggressive LP runtime."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo


RISK_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _decimal(value: object, default: Decimal = Decimal("0")) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


def risk_day_key(timestamp: float, cutoff_hour: int = 8) -> str:
    """Return the Beijing risk day containing ``timestamp``."""
    local = datetime.fromtimestamp(timestamp, tz=RISK_TIMEZONE)
    if local.hour < cutoff_hour:
        local -= timedelta(days=1)
    return local.date().isoformat()


@dataclass
class AggressiveGuardrailState:
    day_key: str = ""
    baseline_equity_usdc: str = ""
    last_equity_usdc: str = ""
    last_collateral_usdc: str = ""
    last_position_value_usdc: str = ""
    daily_loss_usdc: str = "0"
    last_success_ts: float = 0.0
    first_failure_ts: float = 0.0
    latched: bool = False
    reason: str = ""
    triggered_at: float = 0.0

    @classmethod
    def load(cls, path: Path) -> "AggressiveGuardrailState":
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            return cls()
        if not isinstance(payload, Mapping):
            return cls()
        allowed = cls.__dataclass_fields__
        return cls(**{key: payload[key] for key in allowed if key in payload})

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(
            json.dumps(asdict(self), ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)

    def public_dict(self) -> dict[str, object]:
        return asdict(self)

    def observe(
        self,
        *,
        equity: Decimal,
        collateral: Decimal,
        position_value: Decimal,
        now: float,
        cutoff_hour: int,
        baseline_cap: Decimal,
        pause_equity: Decimal,
        daily_loss_limit: Decimal,
    ) -> str:
        current_day = risk_day_key(now, cutoff_hour)
        if self.day_key != current_day or not self.baseline_equity_usdc:
            self.day_key = current_day
            self.baseline_equity_usdc = str(min(equity, baseline_cap))

        baseline = _decimal(self.baseline_equity_usdc, equity)
        daily_loss = max(Decimal("0"), baseline - equity)
        self.last_equity_usdc = str(equity)
        self.last_collateral_usdc = str(collateral)
        self.last_position_value_usdc = str(position_value)
        self.daily_loss_usdc = str(daily_loss)
        self.last_success_ts = now
        self.first_failure_ts = 0.0

        if pause_equity > 0 and equity <= pause_equity:
            return f"equity_floor:{equity}<={pause_equity}"
        if daily_loss_limit > 0 and daily_loss >= daily_loss_limit:
            return f"daily_loss:{daily_loss}>={daily_loss_limit}"
        return ""

    def observe_failure(self, *, now: float, stale_after_sec: float) -> str:
        if self.first_failure_ts <= 0:
            self.first_failure_ts = now
        reference = self.last_success_ts or self.first_failure_ts
        if now - reference >= stale_after_sec:
            return f"equity_unavailable:{int(now - reference)}s"
        return ""

    def latch(self, reason: str, now: float) -> None:
        self.latched = True
        self.reason = reason
        if self.triggered_at <= 0:
            self.triggered_at = now

    def reset(
        self,
        *,
        equity: Decimal,
        now: float,
        cutoff_hour: int,
        baseline_cap: Decimal,
    ) -> None:
        self.day_key = risk_day_key(now, cutoff_hour)
        self.baseline_equity_usdc = str(min(equity, baseline_cap))
        self.last_equity_usdc = str(equity)
        self.daily_loss_usdc = "0"
        self.last_success_ts = now
        self.first_failure_ts = 0.0
        self.latched = False
        self.reason = ""
        self.triggered_at = 0.0
