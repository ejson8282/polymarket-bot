"""Sponsored-reward risk signals for the Polymarket maker.

Polymarket's official rewards endpoint is authoritative for live reward
configuration. Betmoar is used only as an advisory source for sponsor
withdrawals and scheduled reward endings; an advisory can cancel quotes, but
can never place an order.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Optional

import requests


OFFICIAL_REWARDS_URL = "https://clob.polymarket.com/rewards/markets/current"
BETMOAR_SPONSORED_URL = "https://www.betmoar.fun/api/sponsored-rewards"
DONE_CURSOR = "LTE="


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value if value is not None else default)
    except (TypeError, ValueError):
        return default


def _parse_ts(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            raw = float(value)
            return raw / 1000.0 if raw > 10_000_000_000 else raw
        raw = str(value).strip()
        if not raw:
            return None
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _condition_id(value: Any) -> str:
    raw = str(value or "").strip().lower()
    return raw if raw.startswith("0x") and len(raw) == 66 else ""


@dataclass(frozen=True)
class SponsoredRiskPolicy:
    enabled: bool = True
    poll_interval_sec: float = 30.0
    caution_ratio: float = 0.40
    block_new_single_sponsor_ratio: float = 0.70
    caution_size_cap: float = 0.50
    concentrated_size_cap: float = 0.25
    reward_drop_cancel_pct: float = 0.20
    min_remaining_hours: float = 2.0
    cooldown_sec: float = 1800.0
    source_stale_reduce_after_sec: float = 180.0
    betmoar_advisory_enabled: bool = True
    betmoar_early_withdraw_window_sec: float = 1800.0
    betmoar_cancel_min_total_ratio: float = 0.20

    @classmethod
    def from_config(cls, raw: Optional[dict[str, Any]]) -> "SponsoredRiskPolicy":
        cfg = raw if isinstance(raw, dict) else {}
        return cls(
            enabled=bool(cfg.get("enabled", True)),
            poll_interval_sec=max(10.0, _as_float(cfg.get("poll_interval_sec"), 30.0)),
            caution_ratio=min(1.0, max(0.0, _as_float(cfg.get("caution_ratio"), 0.40))),
            block_new_single_sponsor_ratio=min(
                1.0,
                max(0.0, _as_float(cfg.get("block_new_single_sponsor_ratio"), 0.70)),
            ),
            caution_size_cap=min(
                1.0,
                max(0.0, _as_float(cfg.get("caution_size_cap"), 0.50)),
            ),
            concentrated_size_cap=min(
                1.0,
                max(0.0, _as_float(cfg.get("concentrated_size_cap"), 0.25)),
            ),
            reward_drop_cancel_pct=min(
                1.0,
                max(0.0, _as_float(cfg.get("reward_drop_cancel_pct"), 0.20)),
            ),
            min_remaining_hours=max(
                0.0,
                _as_float(cfg.get("min_remaining_hours"), 2.0),
            ),
            cooldown_sec=max(60.0, _as_float(cfg.get("cooldown_sec"), 1800.0)),
            source_stale_reduce_after_sec=max(
                30.0,
                _as_float(cfg.get("source_stale_reduce_after_sec"), 180.0),
            ),
            betmoar_advisory_enabled=bool(
                cfg.get("betmoar_advisory_enabled", True)
            ),
            betmoar_early_withdraw_window_sec=max(
                60.0,
                _as_float(cfg.get("betmoar_early_withdraw_window_sec"), 1800.0),
            ),
            betmoar_cancel_min_total_ratio=min(
                1.0,
                max(
                    0.0,
                    _as_float(cfg.get("betmoar_cancel_min_total_ratio"), 0.20),
                ),
            ),
        )


class SponsoredRiskGuard:
    """Fetch, compare, and classify sponsored reward configurations."""

    def __init__(
        self,
        config: Optional[dict[str, Any]] = None,
        *,
        request_get: Callable[..., Any] = requests.get,
    ) -> None:
        self.policy = SponsoredRiskPolicy.from_config(config)
        self._request_get = request_get
        self._refresh_lock = asyncio.Lock()
        self._official: dict[str, dict[str, Any]] = {}
        self._betmoar_active: dict[str, dict[str, Any]] = {}
        self._betmoar_early: dict[str, dict[str, Any]] = {}
        self._blocked_until: dict[str, float] = {}
        self._blocked_reasons: dict[str, list[str]] = {}
        self._last_refresh_attempt_at = 0.0
        self._official_last_success_at = 0.0
        self._betmoar_last_success_at = 0.0
        self._official_ok = False
        self._betmoar_ok = False
        self._errors: list[str] = []

    @property
    def enabled(self) -> bool:
        return self.policy.enabled

    @property
    def poll_interval_sec(self) -> float:
        return self.policy.poll_interval_sec

    async def refresh(
        self,
        *,
        force: bool = False,
        proxies: Optional[dict[str, str]] = None,
    ) -> bool:
        if not self.enabled:
            return False
        async with self._refresh_lock:
            now = time.time()
            if (
                not force
                and self._last_refresh_attempt_at
                and now - self._last_refresh_attempt_at < self.poll_interval_sec
            ):
                return self._official_ok
            self._last_refresh_attempt_at = now
            return await asyncio.to_thread(self._refresh_sync, proxies, now)

    def _refresh_sync(
        self,
        proxies: Optional[dict[str, str]],
        now: float,
    ) -> bool:
        errors: list[str] = []
        prior_official = self._official
        official_ok = False
        try:
            current_official = self._fetch_official(proxies)
            self._detect_official_changes(prior_official, current_official, now)
            self._official = current_official
            self._official_last_success_at = now
            official_ok = True
        except Exception as exc:
            errors.append(f"official:{exc.__class__.__name__}")

        betmoar_ok = not self.policy.betmoar_advisory_enabled
        if self.policy.betmoar_advisory_enabled:
            try:
                active, early = self._fetch_betmoar(proxies)
                self._betmoar_active = active
                self._betmoar_early = early
                self._betmoar_last_success_at = now
                self._apply_betmoar_blocks(active, early, now)
                betmoar_ok = True
            except Exception as exc:
                errors.append(f"betmoar:{exc.__class__.__name__}")

        self._official_ok = official_ok
        self._betmoar_ok = betmoar_ok
        self._errors = errors
        self._prune_blocks(now)
        return official_ok

    def _fetch_official(
        self,
        proxies: Optional[dict[str, str]],
    ) -> dict[str, dict[str, Any]]:
        cursor = ""
        out: dict[str, dict[str, Any]] = {}
        seen_cursors: set[str] = set()
        for _ in range(20):
            params: dict[str, Any] = {"sponsored": "true", "limit": 500}
            if cursor:
                params["next_cursor"] = cursor
            response = self._request_get(
                OFFICIAL_REWARDS_URL,
                params=params,
                timeout=12,
                proxies=proxies,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("official payload is not an object")
            for item in payload.get("data") or []:
                if not isinstance(item, dict):
                    continue
                cid = _condition_id(item.get("condition_id"))
                if not cid:
                    continue
                sponsored = max(0.0, _as_float(item.get("sponsored_daily_rate")))
                native = max(0.0, _as_float(item.get("native_daily_rate")))
                total = max(
                    sponsored + native,
                    _as_float(item.get("total_daily_rate")),
                )
                out[cid] = {
                    "condition_id": cid,
                    "sponsored_daily_rate": sponsored,
                    "native_daily_rate": native,
                    "total_daily_rate": total,
                    "sponsors_count": max(0, _as_int(item.get("sponsors_count"))),
                    "rewards_max_spread": _as_float(item.get("rewards_max_spread")),
                    "rewards_min_size": _as_float(item.get("rewards_min_size")),
                }
            next_cursor = str(payload.get("next_cursor") or "")
            if not next_cursor or next_cursor == DONE_CURSOR or next_cursor in seen_cursors:
                break
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        return out

    def _fetch_betmoar(
        self,
        proxies: Optional[dict[str, str]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        response = self._request_get(
            BETMOAR_SPONSORED_URL,
            timeout=12,
            proxies=proxies,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Betmoar payload is not an object")

        grouped: dict[str, dict[str, Any]] = {}
        for item in payload.get("active") or []:
            if not isinstance(item, dict):
                continue
            cid = _condition_id(item.get("market_id"))
            if not cid:
                continue
            entry = grouped.setdefault(
                cid,
                {
                    "condition_id": cid,
                    "market_question": str(item.get("market_question") or ""),
                    "market_slug": str(item.get("market_slug") or ""),
                    "sponsors": set(),
                    "is_cancelled": False,
                    "withdrawn_at": None,
                    "next_end_at": None,
                    "cancelled_daily_rate": 0.0,
                    "sponsorships": [],
                },
            )
            sponsor = str(item.get("sponsor") or "").lower()
            daily_rate = max(
                0.0,
                _as_float(item.get("rate_per_minute_usdc")) * 1440.0,
            )
            if sponsor:
                entry["sponsors"].add(sponsor)
            entry["sponsorships"].append({
                "sponsor": sponsor,
                "daily_rate": daily_rate,
                "is_cancelled": bool(item.get("is_cancelled")),
                "end_at": _parse_ts(item.get("rewards_end_at") or item.get("end_at")),
            })
            entry["is_cancelled"] = bool(
                entry["is_cancelled"] or item.get("is_cancelled")
            )
            if item.get("is_cancelled"):
                entry["cancelled_daily_rate"] += daily_rate
            withdrawn_at = _parse_ts(item.get("withdrawn_at"))
            if withdrawn_at:
                entry["withdrawn_at"] = max(
                    float(entry.get("withdrawn_at") or 0),
                    withdrawn_at,
                )
            end_at = _parse_ts(item.get("rewards_end_at") or item.get("end_at"))
            if end_at and (
                entry["next_end_at"] is None or end_at < entry["next_end_at"]
            ):
                entry["next_end_at"] = end_at

        active: dict[str, dict[str, Any]] = {}
        for cid, entry in grouped.items():
            active[cid] = {
                **entry,
                "sponsors": sorted(entry["sponsors"]),
            }

        early: dict[str, dict[str, Any]] = {}
        for item in payload.get("recentWithdrawals") or []:
            if not isinstance(item, dict) or not item.get("is_early_withdraw"):
                continue
            cid = _condition_id(item.get("market_id"))
            if not cid:
                continue
            event_ts = _parse_ts(item.get("block_timestamp"))
            if event_ts is None:
                continue
            prior = early.get(cid)
            if prior is None or event_ts > float(prior.get("event_ts") or 0):
                early[cid] = {
                    "condition_id": cid,
                    "event_ts": event_ts,
                    "market_question": str(item.get("market_question") or ""),
                    "market_slug": str(item.get("market_slug") or ""),
                    "sponsor": str(item.get("sponsor") or "").lower(),
                }
        return active, early

    def _latch_block(
        self,
        condition_id: str,
        reason: str,
        now: float,
        *,
        until: Optional[float] = None,
    ) -> None:
        cid = _condition_id(condition_id)
        if not cid:
            return
        block_until = max(now + self.policy.cooldown_sec, float(until or 0))
        self._blocked_until[cid] = max(
            self._blocked_until.get(cid, 0.0),
            block_until,
        )
        reasons = self._blocked_reasons.setdefault(cid, [])
        if reason not in reasons:
            reasons.append(reason)

    def _detect_official_changes(
        self,
        previous: dict[str, dict[str, Any]],
        current: dict[str, dict[str, Any]],
        now: float,
    ) -> None:
        if not self._official_last_success_at:
            return
        for cid, old in previous.items():
            old_rate = max(0.0, _as_float(old.get("sponsored_daily_rate")))
            if old_rate <= 0:
                continue
            new = current.get(cid)
            if new is None:
                self._latch_block(cid, "official_sponsor_removed", now)
                continue
            new_rate = max(0.0, _as_float(new.get("sponsored_daily_rate")))
            drop_ratio = (old_rate - new_rate) / old_rate if new_rate < old_rate else 0.0
            if drop_ratio >= self.policy.reward_drop_cancel_pct:
                self._latch_block(
                    cid,
                    f"official_reward_drop_{drop_ratio:.0%}",
                    now,
                )
            old_count = max(0, _as_int(old.get("sponsors_count")))
            new_count = max(0, _as_int(new.get("sponsors_count")))
            if old_count > 0 and new_count < old_count:
                self._latch_block(
                    cid,
                    f"official_sponsor_count_{old_count}_to_{new_count}",
                    now,
                )

    def _apply_betmoar_blocks(
        self,
        active: dict[str, dict[str, Any]],
        early: dict[str, dict[str, Any]],
        now: float,
    ) -> None:
        min_remaining_sec = self.policy.min_remaining_hours * 3600.0
        for cid, item in active.items():
            official = self._official.get(cid) or {}
            official_total = max(
                _as_float(official.get("total_daily_rate")),
                _as_float(official.get("sponsored_daily_rate"))
                + _as_float(official.get("native_daily_rate")),
            )
            cancelled_share = (
                _as_float(item.get("cancelled_daily_rate")) / official_total
                if official_total > 0 else 0.0
            )
            next_end_at = _as_float(item.get("next_end_at"), 0.0)
            if (
                item.get("is_cancelled")
                and cancelled_share >= self.policy.betmoar_cancel_min_total_ratio
            ):
                self._latch_block(
                    cid,
                    f"betmoar_sponsor_cancelled_{cancelled_share:.0%}",
                    now,
                    until=next_end_at + 60 if next_end_at else None,
                )
            ending_daily_rate = sum(
                _as_float(sponsorship.get("daily_rate"))
                for sponsorship in (item.get("sponsorships") or [])
                if (
                    _as_float(sponsorship.get("end_at"), 0.0) > now
                    and _as_float(sponsorship.get("end_at"), 0.0) - now
                    <= min_remaining_sec
                )
            )
            ending_share = (
                ending_daily_rate / official_total
                if official_total > 0 else 0.0
            )
            if ending_share >= self.policy.betmoar_cancel_min_total_ratio:
                self._latch_block(
                    cid,
                    f"betmoar_reward_ending_soon_{ending_share:.0%}",
                    now,
                    until=next_end_at + 60,
                )
        for cid, item in early.items():
            event_ts = _as_float(item.get("event_ts"), 0.0)
            official = self._official.get(cid) or {}
            sponsored = _as_float(official.get("sponsored_daily_rate"))
            total = max(
                _as_float(official.get("total_daily_rate")),
                sponsored + _as_float(official.get("native_daily_rate")),
            )
            sponsor_share = sponsored / total if total > 0 else 0.0
            if (
                event_ts
                and now - event_ts <= self.policy.betmoar_early_withdraw_window_sec
                and sponsor_share >= self.policy.betmoar_cancel_min_total_ratio
            ):
                self._latch_block(
                    cid,
                    f"betmoar_early_withdrawal_{sponsor_share:.0%}",
                    now,
                )

    def _prune_blocks(self, now: float) -> None:
        for cid, until in list(self._blocked_until.items()):
            if now >= until:
                self._blocked_until.pop(cid, None)
                self._blocked_reasons.pop(cid, None)

    def assess(
        self,
        condition_id: str,
        *,
        for_admission: bool = False,
        now: Optional[float] = None,
    ) -> dict[str, Any]:
        now_ts = float(now if now is not None else time.time())
        cid = _condition_id(condition_id)
        if not self.enabled:
            return self._assessment(cid, "disabled", 1.0, ["guard_disabled"], now_ts)
        if not cid:
            return self._assessment(cid, "unknown", 1.0, ["condition_id_missing"], now_ts)

        official = self._official.get(cid, {})
        sponsored = max(0.0, _as_float(official.get("sponsored_daily_rate")))
        native = max(0.0, _as_float(official.get("native_daily_rate")))
        total = max(
            sponsored + native,
            _as_float(official.get("total_daily_rate")),
        )
        ratio = sponsored / total if total > 0 else 0.0
        sponsors_count = max(0, _as_int(official.get("sponsors_count")))
        reasons: list[str] = []
        status = "safe"
        size_cap = 1.0

        block_until = self._blocked_until.get(cid, 0.0)
        if block_until > now_ts:
            status = "blocked"
            size_cap = 0.0
            reasons.extend(self._blocked_reasons.get(cid, ["sponsor_change"]))
        elif not self._official_last_success_at:
            status = "blocked" if for_admission else "unknown"
            size_cap = 0.0 if for_admission else 1.0
            reasons.append("official_source_unavailable")
        elif (
            for_admission
            and sponsors_count == 1
            and ratio >= self.policy.block_new_single_sponsor_ratio
        ):
            status = "blocked"
            size_cap = 0.0
            reasons.append("single_sponsor_dependency")
        elif sponsors_count == 1 and ratio >= self.policy.block_new_single_sponsor_ratio:
            status = "caution"
            size_cap = self.policy.concentrated_size_cap
            reasons.append("single_sponsor_concentrated")
        elif ratio >= self.policy.caution_ratio:
            status = "caution"
            size_cap = self.policy.caution_size_cap
            reasons.append("sponsored_share_high")

        source_age_sec = (
            now_ts - self._official_last_success_at
            if self._official_last_success_at
            else None
        )
        if (
            status not in {"blocked", "disabled"}
            and source_age_sec is not None
            and source_age_sec >= self.policy.source_stale_reduce_after_sec
        ):
            status = "caution"
            size_cap = min(size_cap, self.policy.concentrated_size_cap)
            reasons.append("official_source_stale")

        betmoar = self._betmoar_active.get(cid, {})
        early = self._betmoar_early.get(cid, {})
        return {
            **self._assessment(cid, status, size_cap, reasons or ["ok"], now_ts),
            "official_present": bool(official),
            "sponsored_daily_rate": round(sponsored, 6),
            "native_daily_rate": round(native, 6),
            "total_daily_rate": round(total, 6),
            "sponsor_ratio": round(ratio, 6),
            "sponsors_count": sponsors_count,
            "block_until": block_until or None,
            "market_question": str(betmoar.get("market_question") or ""),
            "market_slug": str(betmoar.get("market_slug") or ""),
            "sponsor_addresses": list(betmoar.get("sponsors") or []),
            "reward_end_at": betmoar.get("next_end_at"),
            "betmoar_cancelled": bool(betmoar.get("is_cancelled")),
            "betmoar_early_withdrawal": bool(early),
        }

    def _assessment(
        self,
        condition_id: str,
        status: str,
        size_cap: float,
        reasons: list[str],
        now: float,
    ) -> dict[str, Any]:
        return {
            "condition_id": condition_id,
            "status": status,
            "size_cap": round(size_cap, 4),
            "reasons": list(reasons),
            "assessed_at": now,
        }

    def state_payload(
        self,
        assessments: dict[str, dict[str, Any]],
    ) -> dict[str, Any]:
        counts = {"safe": 0, "caution": 0, "blocked": 0, "unknown": 0}
        for assessment in assessments.values():
            status = str(assessment.get("status") or "unknown")
            counts[status if status in counts else "unknown"] += 1
        if counts["blocked"]:
            overall = "blocked"
        elif counts["caution"] or counts["unknown"]:
            overall = "caution"
        else:
            overall = "safe"
        return {
            "enabled": self.enabled,
            "status": overall if self.enabled else "disabled",
            "counts": counts,
            "last_refresh_attempt_at": self._last_refresh_attempt_at or None,
            "official_last_success_at": self._official_last_success_at or None,
            "betmoar_last_success_at": self._betmoar_last_success_at or None,
            "official_ok": self._official_ok,
            "betmoar_ok": self._betmoar_ok,
            "errors": list(self._errors),
            "policy": {
                "caution_ratio": self.policy.caution_ratio,
                "block_new_single_sponsor_ratio": self.policy.block_new_single_sponsor_ratio,
                "reward_drop_cancel_pct": self.policy.reward_drop_cancel_pct,
                "min_remaining_hours": self.policy.min_remaining_hours,
                "poll_interval_sec": self.policy.poll_interval_sec,
                "betmoar_cancel_min_total_ratio": (
                    self.policy.betmoar_cancel_min_total_ratio
                ),
            },
            "markets": assessments,
        }
