"""
Cross-side sentinel: monitor depth depletion on the OPPOSITE token of each
conditionId. When the opposite token's top-N ASK depth drains rapidly
(BUY pressure consuming asks), the same-game arbitrage relationship
(Yes+No≈$1) will drag our token's price down through our resting BID,
causing an indirect-cross fill. To prevent this, we cancel our orders on
the at-risk side BEFORE arbitrageurs cross our quote.

Signal source: market-WS `book` events (full depth snapshots).
Trigger model: per-token rolling window of (top-N ask depth, top-N bid
depth). When ASK depth drops by ≥ shares_threshold OR ≥ pct_threshold
relative to the window's max → trigger cancel on PAIRED token.

Why depth (not mid): a sweep eats ask depth in the first few seconds
before mid moves materially. Reacting on mid is too late — depth
depletion gives a 5-10s head start.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple


@dataclass
class _DepthSnap:
    ts: float
    ask_depth: float
    bid_depth: float


@dataclass
class _TokenDepth:
    snaps: Deque[_DepthSnap] = field(default_factory=deque)
    # set when a snapshot gap > max_snap_gap_sec is detected; trigger blocked
    # until (now - stable_since) >= warmup_sec
    stable_since: float = 0.0


class CrossSideSentinel:
    def __init__(
        self,
        *,
        enabled: bool = False,
        dry_run: bool = True,
        # Depth-based trigger params
        depth_window_sec: float = 30.0,
        depth_levels: int = 3,
        # Absolute threshold = max(quote_size_proxy * abs_quote_multiple, baseline * abs_baseline_ratio)
        # Both conditions (absolute AND pct) must fail for no-trigger.
        quote_size_proxy: float = 1500.0,
        abs_quote_multiple: float = 3.0,
        abs_baseline_ratio: float = 0.20,
        depth_consumed_pct: float = 0.30,
        # High-liquidity tier: when baseline > high_liq_baseline_shares,
        # require a STRICTER pct threshold. Big books (whales/political
        # markets) churn 30-50% on normal MM rebalancing — only treat
        # as real pressure when consumed >= high_liq_pct.
        high_liq_baseline_shares: float = 50000.0,
        high_liq_pct: float = 0.60,
        cooldown_sec: float = 60.0,
        min_baseline_shares: float = 2000.0,
        # Reconnect/gap protection: a snapshot gap > max_snap_gap_sec means
        # market-WS reconnected; clear stable_since so the rolling window has
        # warmup_sec of consistent data before triggers can fire.
        warmup_sec: float = 15.0,
        max_snap_gap_sec: float = 5.0,
        min_snaps: int = 4,
        # Backwards-compat fields (ignored but retained so old configs don't error)
        depth_consumed_shares: Optional[float] = None,
        window_sec: Optional[float] = None,
        threshold_usd: Optional[float] = None,
        threshold_shares: Optional[float] = None,
        monitor_side: Optional[str] = None,
    ):
        self.enabled = enabled
        self.dry_run = dry_run
        self.depth_window_sec = float(depth_window_sec)
        self.depth_levels = int(depth_levels)
        self.quote_size_proxy = float(quote_size_proxy)
        self.abs_quote_multiple = float(abs_quote_multiple)
        self.abs_baseline_ratio = float(abs_baseline_ratio)
        self.depth_consumed_pct = float(depth_consumed_pct)
        self.high_liq_baseline_shares = float(high_liq_baseline_shares)
        self.high_liq_pct = float(high_liq_pct)
        self.cooldown_sec = float(cooldown_sec)
        self.min_baseline_shares = float(min_baseline_shares)
        self.warmup_sec = float(warmup_sec)
        self.max_snap_gap_sec = float(max_snap_gap_sec)
        self.min_snaps = int(min_snaps)

        # token_id → rolling depth snapshots
        self._depth: Dict[str, _TokenDepth] = {}
        # paired_token_id → cooldown_until_ts
        self._cancel_cooldown: Dict[str, float] = {}
        # totals
        self.triggers_total: int = 0
        self.triggers_dry_total: int = 0

    def _gc(self, td: _TokenDepth, now: float) -> None:
        cutoff = now - self.depth_window_sec
        while td.snaps and td.snaps[0].ts < cutoff:
            td.snaps.popleft()

    def record_depth(
        self,
        token_id: str,
        ask_depth: float,
        bid_depth: float,
        ts: Optional[float] = None,
    ) -> None:
        if not self.enabled or not token_id:
            return
        now = ts if ts is not None else time.time()
        td = self._depth.get(token_id)
        if td is None:
            td = _TokenDepth(stable_since=now)
            self._depth[token_id] = td
        # Detect WS reconnect / data gap: if last snapshot is too old, this is
        # a discontinuity — reset the stability clock so warmup applies again
        # before any trigger can fire on a fresh baseline.
        if td.snaps:
            last_ts = td.snaps[-1].ts
            if (now - last_ts) > self.max_snap_gap_sec:
                td.snaps.clear()
                td.stable_since = now
        td.snaps.append(_DepthSnap(ts=now, ask_depth=float(ask_depth), bid_depth=float(bid_depth)))
        self._gc(td, now)

    def should_trigger(
        self, token_id: str, now: Optional[float] = None
    ) -> Tuple[bool, str, float, float, float]:
        """Detect ask depletion (BUY pressure on this token).
        Returns (trigger, reason, max_ask, current_ask, consumed_pct)."""
        if not self.enabled:
            return (False, "", 0.0, 0.0, 0.0)
        td = self._depth.get(token_id)
        if not td or len(td.snaps) < self.min_snaps:
            return (False, "warmup_snaps", 0.0, 0.0, 0.0)
        t = now if now is not None else time.time()
        self._gc(td, t)
        if len(td.snaps) < self.min_snaps:
            return (False, "warmup_snaps", 0.0, 0.0, 0.0)
        # Block triggers until token has been observed steadily for warmup_sec
        if (t - td.stable_since) < self.warmup_sec:
            return (False, "warmup_time", 0.0, 0.0, 0.0)

        max_ask = max(s.ask_depth for s in td.snaps)
        current_ask = td.snaps[-1].ask_depth
        if max_ask < self.min_baseline_shares:
            # Too thin to interpret depletion meaningfully; skip
            return (False, "thin_baseline", max_ask, current_ask, 0.0)
        consumed = max_ask - current_ask
        if consumed <= 0:
            return (False, "no_depletion", max_ask, current_ask, 0.0)
        pct = consumed / max_ask if max_ask > 0 else 0.0

        # Absolute threshold = max(quote_size_proxy * 3, baseline * 0.20)
        # Captures both "≥3x of our fill risk" AND "≥20% of book"
        abs_threshold = max(
            self.quote_size_proxy * self.abs_quote_multiple,
            max_ask * self.abs_baseline_ratio,
        )
        # High-liquidity tier: thick books churn 30-50% on normal MM activity
        # so the abs trigger and 30% pct trigger both fire as noise. Use pct
        # ONLY for these (60% required) — pure depth-ratio signal.
        if max_ask >= self.high_liq_baseline_shares:
            if pct >= self.high_liq_pct:
                return (True, f"pct>={self.high_liq_pct:.0%}(high_liq)", max_ask, current_ask, pct)
            return (False, "below_threshold(high_liq)", max_ask, current_ask, pct)

        # Normal tier: combined abs + pct
        if consumed >= abs_threshold:
            return (
                True,
                f"abs>={int(abs_threshold)}(q×{self.abs_quote_multiple:g}|book×{self.abs_baseline_ratio:.0%})",
                max_ask,
                current_ask,
                pct,
            )
        if pct >= self.depth_consumed_pct:
            return (True, f"pct>={self.depth_consumed_pct:.0%}(normal)", max_ask, current_ask, pct)
        return (False, "below_threshold", max_ask, current_ask, pct)

    def in_cooldown(self, paired_token_id: str, now: Optional[float] = None) -> bool:
        if not paired_token_id:
            return False
        until = self._cancel_cooldown.get(paired_token_id, 0.0)
        t = now if now is not None else time.time()
        return until > t

    def mark_cancelled(self, paired_token_id: str, now: Optional[float] = None) -> None:
        t = now if now is not None else time.time()
        self._cancel_cooldown[paired_token_id] = t + self.cooldown_sec
        if self.dry_run:
            self.triggers_dry_total += 1
        else:
            self.triggers_total += 1

    def cooldown_remaining(self, paired_token_id: str, now: Optional[float] = None) -> float:
        t = now if now is not None else time.time()
        until = self._cancel_cooldown.get(paired_token_id, 0.0)
        return max(0.0, until - t)
