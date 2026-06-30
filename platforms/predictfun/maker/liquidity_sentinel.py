from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Deque


@dataclass
class _DepthSnap:
    ts: float
    bid_notional: Decimal
    ask_notional: Decimal
    bid_shares: Decimal
    ask_shares: Decimal


@dataclass
class _MarketDepth:
    snaps: Deque[_DepthSnap] = field(default_factory=deque)
    stable_since: float = 0.0


class LiquiditySentinel:
    def __init__(
        self,
        *,
        enabled: bool = False,
        depth_window_sec: float = 30.0,
        depth_levels: int = 3,
        min_snaps: int = 3,
        warmup_sec: float = 5.0,
        max_snap_gap_sec: float = 10.0,
        min_baseline_notional: Decimal = Decimal("5"),
        depth_consumed_notional: Decimal = Decimal("3"),
        depth_consumed_pct: Decimal = Decimal("0.50"),
        cooldown_sec: float = 60.0,
    ) -> None:
        self.enabled = enabled
        self.depth_window_sec = float(depth_window_sec)
        self.depth_levels = max(1, int(depth_levels))
        self.min_snaps = max(2, int(min_snaps))
        self.warmup_sec = float(warmup_sec)
        self.max_snap_gap_sec = float(max_snap_gap_sec)
        self.min_baseline_notional = min_baseline_notional
        self.depth_consumed_notional = depth_consumed_notional
        self.depth_consumed_pct = depth_consumed_pct
        self.cooldown_sec = float(cooldown_sec)
        self._depth: dict[str, _MarketDepth] = {}
        self._cooldowns: dict[str, dict[str, Any]] = {}

    @classmethod
    def from_config(cls, cfg: dict[str, Any]) -> "LiquiditySentinel":
        return cls(
            enabled=bool(cfg.get("enabled", False)),
            depth_window_sec=float(cfg.get("depth_window_sec") or 30),
            depth_levels=int(cfg.get("depth_levels") or 3),
            min_snaps=int(cfg.get("min_snaps") or 3),
            warmup_sec=float(cfg.get("warmup_sec") or 5),
            max_snap_gap_sec=float(cfg.get("max_snap_gap_sec") or 10),
            min_baseline_notional=_dec(cfg.get("min_baseline_notional"), "5"),
            depth_consumed_notional=_dec(cfg.get("depth_consumed_notional"), "3"),
            depth_consumed_pct=_dec(cfg.get("depth_consumed_pct"), "0.50"),
            cooldown_sec=float(cfg.get("cooldown_sec") or 60),
        )

    def record(self, market_id: int | str, book: dict[str, Any], *, ts: float) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        market_key = str(market_id)
        bids = _levels(book.get("bids"))
        asks = _levels(book.get("asks"))
        snap = _DepthSnap(
            ts=ts,
            bid_notional=_depth_notional(bids, self.depth_levels),
            ask_notional=_depth_notional(asks, self.depth_levels),
            bid_shares=_depth_shares(bids, self.depth_levels),
            ask_shares=_depth_shares(asks, self.depth_levels),
        )
        md = self._depth.get(market_key)
        if md is None:
            md = _MarketDepth(stable_since=ts)
            self._depth[market_key] = md
        if md.snaps and ts - md.snaps[-1].ts > self.max_snap_gap_sec:
            md.snaps.clear()
            md.stable_since = ts
        md.snaps.append(snap)
        self._gc(md, ts)

        alert = self._existing_alert(market_key, ts)
        if alert:
            return alert
        trigger = self._trigger(market_key, md, ts)
        if trigger:
            alert = {
                "active": True,
                "market_id": market_key,
                "ts": _iso(ts),
                "cooldown_until_ts": ts + self.cooldown_sec,
                "cooldown_until": _iso(ts + self.cooldown_sec),
                **trigger,
            }
            self._cooldowns[market_key] = alert
            return alert
        return None

    def alerts_json(self, *, now: float) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for market_id, alert in list(self._cooldowns.items()):
            if float(alert.get("cooldown_until_ts") or 0) <= now:
                del self._cooldowns[market_id]
                continue
            out[market_id] = dict(alert)
        return out

    def metrics_json(self, *, now: float) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for market_id, md in self._depth.items():
            self._gc(md, now)
            if not md.snaps:
                continue
            snap = md.snaps[-1]
            out[market_id] = {
                "bid_notional": str(snap.bid_notional),
                "ask_notional": str(snap.ask_notional),
                "bid_shares": str(snap.bid_shares),
                "ask_shares": str(snap.ask_shares),
                "samples": len(md.snaps),
                "updated_at": _iso(snap.ts),
            }
        return out

    def _gc(self, md: _MarketDepth, now: float) -> None:
        cutoff = now - self.depth_window_sec
        while md.snaps and md.snaps[0].ts < cutoff:
            md.snaps.popleft()

    def _existing_alert(self, market_key: str, now: float) -> dict[str, Any] | None:
        alert = self._cooldowns.get(market_key)
        if not alert:
            return None
        if float(alert.get("cooldown_until_ts") or 0) <= now:
            self._cooldowns.pop(market_key, None)
            return None
        return alert

    def _trigger(self, market_key: str, md: _MarketDepth, now: float) -> dict[str, Any] | None:
        self._gc(md, now)
        if len(md.snaps) < self.min_snaps:
            return None
        if now - md.stable_since < self.warmup_sec:
            return None
        latest = md.snaps[-1]
        bid_max = max(s.bid_notional for s in md.snaps)
        ask_max = max(s.ask_notional for s in md.snaps)
        candidates = [
            _depletion("bid", bid_max, latest.bid_notional),
            _depletion("ask", ask_max, latest.ask_notional),
        ]
        candidates = [c for c in candidates if c]
        if not candidates:
            return None
        candidates.sort(key=lambda row: row["consumed_pct"], reverse=True)
        for row in candidates:
            baseline = row["baseline_notional"]
            consumed = row["consumed_notional"]
            pct = row["consumed_pct"]
            if baseline < self.min_baseline_notional:
                continue
            if consumed >= self.depth_consumed_notional or pct >= self.depth_consumed_pct:
                return {
                    "side": row["side"],
                    "reason": "depth_depletion",
                    "baseline_notional": str(baseline),
                    "current_notional": str(row["current_notional"]),
                    "consumed_notional": str(consumed),
                    "consumed_pct": str(pct),
                }
        return None


def _depletion(side: str, baseline: Decimal, current: Decimal) -> dict[str, Any] | None:
    consumed = baseline - current
    if consumed <= 0 or baseline <= 0:
        return None
    return {
        "side": side,
        "baseline_notional": baseline,
        "current_notional": current,
        "consumed_notional": consumed,
        "consumed_pct": consumed / baseline,
    }


def _levels(raw: Any) -> list[tuple[Decimal, Decimal]]:
    if not isinstance(raw, list):
        return []
    out: list[tuple[Decimal, Decimal]] = []
    for row in raw:
        if not isinstance(row, list) or len(row) < 2:
            continue
        price = _dec(row[0])
        size = _dec(row[1])
        if price <= 0 or size <= 0:
            continue
        out.append((price, size))
    return out


def _depth_notional(levels: list[tuple[Decimal, Decimal]], limit: int) -> Decimal:
    return sum((price * size for price, size in levels[:limit]), Decimal("0"))


def _depth_shares(levels: list[tuple[Decimal, Decimal]], limit: int) -> Decimal:
    return sum((size for _, size in levels[:limit]), Decimal("0"))


def _dec(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
