"""auto_curator — periodic scan of Polymarket sports markets, filter to
qualifying pre-game markets, and hot-add them to the running engine via
`engine.add_market_runtime()`.

Two branches, dispatched by the engine's current session:
  • day   → route to market_cfg, filter game_start > now + 3h
  • night → route to _night_market_cfg, filter game_start ≥ next BJT 08:00,
            scan every 30 min

Day and night pools are fully separate (no inheritance across the switch).
T-2h hard cutoff is enforced by the engine's `start_guard_sweep_loop`, so this
module only cares about admission, not exit.
"""

import asyncio
import json as _json
import re
import time
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

try:
    from .engine import log  # package-qualified import
except ImportError:
    from engine import log  # engine.py is launched as a top-level script (cwd=maker/)

# Switched from /events to /markets — matches scanner.py/dashboard coverage.
# /events with tag_slug=sports misses markets that don't carry an event-level
# sports tag but are clearly sports by slug (what Kevin noticed on dashboard).
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"

# Sports slugs always carry an explicit game date (YYYY-MM-DD). Non-sports
# markets with gameStartTime populated (e.g. geopolitical resolution dates)
# don't — this guards against false positives like
# "russia-x-ukraine-ceasefire-before-2027".
_SPORTS_SLUG_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")

# Approved league tag labels (lowercased, matched substring-style against each
# market's nested events[].tags or top-level tags). Adjust here if Polymarket
# renames a tag or we want broader coverage.
APPROVED_LEAGUE_TAGS: Tuple[str, ...] = (
    # Soccer — leagues
    "epl", "premier league", "la liga", "bundesliga", "serie a", "ligue 1",
    "champions league", "europa league", "conference league",
    "mls", "eredivisie", "primeira liga", "copa libertadores", "sudamericana",
    "championship", "efl",
    # Soccer — international / generic
    "world cup", "copa america", "euros", "euro 2024", "euro 2028",
    "nations league", "fifa", "qualifiers", "soccer", "football",
    # Basketball
    "nba", "wnba", "ncaab", "ncaa basketball", "college basketball",
    "euroleague", "eurobasket", "fiba",
    # American football
    "nfl", "ncaaf", "ncaa football", "college football", "cfp", "super bowl",
    # Tennis
    "atp", "wta", "tennis", "grand slam",
    "us open", "wimbledon", "french open", "australian open", "roland garros",
    # Baseball
    "mlb", "world series", "alcs", "nlcs",
    # Hockey
    "nhl", "stanley cup",
    # Combat
    "ufc", "mma", "boxing", "bellator", "pfl",
    # Cricket
    "cricket", "ipl", "t20", "test cricket", "icc",
    # Rugby
    "rugby", "six nations", "world rugby",
    # Golf
    "pga", "liv golf", "masters", "us open golf", "the open", "ryder cup",
    # Motorsport
    "f1", "formula 1", "formula one", "nascar", "motogp", "indycar",
    # Olympics
    "olympics", "summer olympics", "winter olympics",
    # Esports
    "esports", "cs2", "counter-strike", "league of legends", "lol",
    "dota", "valorant", "overwatch",
    # Others
    "afl", "aussie rules", "darts", "snooker", "cycling",
)

HIGH_RETIRE_RISK_TAGS: Tuple[str, ...] = (
    "atp", "wta", "tennis",
    "ufc", "mma", "boxing", "bellator", "pfl",
)
HIGH_RETIRE_RISK_PRE_START_STOP_SEC = 12 * 3600

# Depth check: bid-side total notional (sum of all bid levels' price×size)
# must be ≥ this threshold. Matches scanner.py's `fetch_bid_depth`.
DEPTH_MIN_BID_NOTIONAL_USD = Decimal("100000")

# Rewards gate: market must have ≥ $100/day in LP rewards (sum of clobRewards[].rewardsDailyRate).
# This is a cheap in-memory check — prune BEFORE the expensive depth HTTP call.
# 2026-04-23 Kevin: lowered 300 → 100 to catch mid-reward markets.
REWARD_MIN_DAILY_USD = Decimal("100")

# Day-session admission window: only consider markets starting > 3h from now.
# (Engine's start_guard_sweep_loop cancels at T-2h, leaving a 1h safety margin.)
MIN_TIME_TO_START_SEC = 3 * 3600

# Default day-loop interval. Can be overridden via config.json auto_curator.interval_sec.
CURATOR_INTERVAL_SEC = 15 * 60
# Night-loop interval (fixed per spec — every 30 min new-timed markets pick up).
NIGHT_SCAN_INTERVAL_SEC = 30 * 60
# Timezone used to compute next 08:00 cutoff for night admission.
_BJT = ZoneInfo("Asia/Shanghai")


def _next_bjt_8am_ts() -> float:
    """Unix ts of the next 08:00 Asia/Shanghai that is still in the future.
    During a typical night session (22:00–08:00 BJT) this returns tomorrow
    08:00 while it's still evening, and today 08:00 after midnight.
    """
    now = datetime.now(_BJT)
    today_8 = now.replace(hour=8, minute=0, second=0, microsecond=0)
    if now < today_8:
        return today_8.timestamp()
    return (today_8 + timedelta(days=1)).timestamp()


def _parse_ts(value: Any) -> Optional[float]:
    """Accept unix int/float (ms or s), ISO string, or numeric string."""
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            x = float(value)
            return x / 1000.0 if x > 10_000_000_000 else x
        s = str(value).strip()
        if not s:
            return None
        if s.isdigit():
            x = float(s)
            return x / 1000.0 if x > 10_000_000_000 else x
        return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


class AutoCurator:
    def __init__(self, engine: Any, interval_sec: float = CURATOR_INTERVAL_SEC) -> None:
        self.engine = engine
        self.interval_sec = float(interval_sec)
        self._last_scan_ts: float = 0.0
        self._last_enabled_state: Optional[bool] = None
        self._stats: Dict[str, int] = {"scans": 0, "added_total": 0, "rejected_total": 0, "errors": 0}
        # token_id → (ts, reason) cache so we don't re-log the same rejection each cycle
        self._reject_cache: Dict[str, Tuple[float, str]] = {}
        self._reject_cache_ttl_sec: float = 3600.0
        # Path to config.json (for live-toggle reads) and state file (for dashboard)
        try:
            cfg_path = getattr(engine, "_config_path", None)
            if cfg_path:
                self._config_path: Path = Path(cfg_path).resolve()
            else:
                self._config_path = Path(__file__).resolve().parent / "config.json"
        except Exception:
            self._config_path = Path(__file__).resolve().parent / "config.json"
        self._state_path: Path = self._config_path.parent.parent.parent.parent / "data" / "auto_curator_state.json"
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

    # ── live config flag ─────────────────────────────────────────────────────

    def _read_enabled_flag(self) -> bool:
        """Check auto_curator.enabled from config.json on each tick so dashboard
        toggles take effect without a restart. Returns False on any read error.
        """
        try:
            cfg = _json.loads(self._config_path.read_text(encoding="utf-8"))
            return bool((cfg.get("auto_curator") or {}).get("enabled", False))
        except Exception:
            return False

    def _write_state(self, enabled: bool, last_scan_added: int = 0) -> None:
        """Persist stats to data/auto_curator_state.json so the dashboard can show
        last-scan time, total added, reject count, enabled state.
        """
        try:
            payload = {
                "enabled": enabled,
                "last_scan_ts": self._last_scan_ts,
                "last_scan_added": last_scan_added,
                "interval_sec": self.interval_sec,
                "markets_in_engine": len(getattr(self.engine, "market_cfg", {}) or {}),
                "runtime_added_count": len(getattr(self.engine, "_runtime_added_tokens", set()) or set()),
                **self._stats,
                "written_at": time.time(),
            }
            self._state_path.write_text(_json.dumps(payload, indent=2), encoding="utf-8")
        except Exception as e:
            log(f"[auto_curator] state write err: {e}")

    # ── main loop ────────────────────────────────────────────────────────────

    async def run(self) -> None:
        log(f"[auto_curator] starting (live-toggle via config.json, "
            f"day_interval={self.interval_sec:.0f}s "
            f"night_interval={NIGHT_SCAN_INTERVAL_SEC}s "
            f"day_min_time_to_start={MIN_TIME_TO_START_SEC/3600:.1f}h "
            f"bid_depth_min=${int(DEPTH_MIN_BID_NOTIONAL_USD)})")
        disabled_sleep_sec = 30.0
        while getattr(self.engine, "_running", False):
            enabled = self._read_enabled_flag()
            # Log transitions so the journal records operator flips
            if enabled != self._last_enabled_state:
                if self._last_enabled_state is not None:
                    log(f"[auto_curator] enabled: {self._last_enabled_state} → {enabled}")
                self._last_enabled_state = enabled

            if not enabled:
                self._write_state(enabled=False)
                await asyncio.sleep(disabled_sleep_sec)
                continue

            is_night = self._is_night_session()
            session_label = "night" if is_night else "day"
            sleep_sec = NIGHT_SCAN_INTERVAL_SEC if is_night else self.interval_sec

            try:
                scan_result = await self._scan_once(session_label=session_label)
                self._last_scan_ts = time.time()
                self._stats["scans"] += 1
                added = int(scan_result.get("added", 0))
                self._write_state(enabled=True, last_scan_added=added)
                log(f"[auto_curator] scan#{self._stats['scans']} session={session_label} "
                    f"markets_total={scan_result['events_total']} "
                    f"league_matched={scan_result['events_league_matched']} "
                    f"markets_considered={scan_result['markets_considered']} "
                    f"added={added} total_added={self._stats['added_total']}")
                self._send_scan_discord(scan_result, session_label=session_label)
            except Exception as e:
                self._stats["errors"] += 1
                log(f"[auto_curator] scan err ({session_label}): {e}")
                self._write_state(enabled=True)
            await asyncio.sleep(sleep_sec)

    # ── Discord notify ───────────────────────────────────────────────────────

    def _send_scan_discord(self, scan_result: Dict[str, Any], session_label: str = "day") -> None:
        """Send a concise summary after each successful scan so the operator
        has visibility that the curator is alive and sees what it added.
        """
        try:
            send = getattr(self.engine, "send_discord", None)
            if not callable(send):
                return
            added = int(scan_result.get("added", 0))
            rejected = int(scan_result.get("rejected", 0))
            slugs = scan_result.get("added_slugs") or []
            rejected_samples = scan_result.get("rejected_samples") or []
            # Truncate slug list in the message to stay under Discord limits
            slug_preview = ", ".join(str(s)[:40] for s in slugs[:5])
            if len(slugs) > 5:
                slug_preview += f" … (+{len(slugs) - 5})"
            session_tag = "夜盘" if session_label == "night" else "日盘"
            header = f"✅ {session_tag}扫描完成" if added > 0 else f"🔍 {session_tag}扫描完成（无新增）"
            parts = [
                f"市场总数={scan_result.get('events_total', 0)}",
                f"联赛匹配={scan_result.get('events_league_matched', 0)}",
                f"候选={scan_result.get('markets_considered', 0)}",
                f"✅新增={added}",
                f"❌驳回={rejected}",
                f"累计新增={self._stats.get('added_total', 0)}",
            ]
            msg = f"[auto_curator] {header} | " + " | ".join(parts)
            if slugs:
                msg += f"\n✅ 已加入: {slug_preview}"
            # 驳回样本明细按 Kevin 要求不再贴进 Discord（`❌驳回=N` 汇总已包含
            # 在 parts 里够用），详情需要的话看 engine.log 里 `[auto_curator] REJECT`。
            send(msg)
        except Exception as e:
            log(f"[auto_curator] discord notify err: {e}")

    def _is_night_session(self) -> bool:
        try:
            if not getattr(self.engine, "_session_enabled", False):
                return False
            current = self.engine._current_session()
            return current == "night"
        except Exception:
            return False

    # ── scan + filter pipeline ───────────────────────────────────────────────

    async def _scan_once(self, session_label: str = "day") -> Dict[str, Any]:
        markets = await asyncio.to_thread(self._fetch_sports_markets)
        result: Dict[str, Any] = {
            "events_total": len(markets or []),
            "events_league_matched": 0,
            "markets_considered": 0,
            "added": 0,
            "added_slugs": [],
            "rejected": 0,
            "rejected_samples": [],  # up to 5 (slug, reason) for Discord summary
        }
        if not markets:
            log(f"[auto_curator] fetched 0 markets — skipping")
            return result
        # Night cutoff is stable per scan — compute once to avoid drift across a
        # large result set.
        night_cutoff_ts = _next_bjt_8am_ts() if session_label == "night" else 0.0
        for market in markets:
            try:
                league_tags = self._extract_league_tags_from_market(market)
                matched = [t for t in league_tags if self._league_matches(t)]
                if not matched:
                    continue
                result["events_league_matched"] += 1
                result["markets_considered"] += 1
                added = await self._try_add_market(
                    market, matched,
                    session_label=session_label,
                    night_cutoff_ts=night_cutoff_ts,
                )
                if added:
                    result["added"] += 1
                    self._stats["added_total"] += 1
                    slug = str(market.get("slug") or "")
                    if slug:
                        result["added_slugs"].append(slug)
                else:
                    result["rejected"] += 1
                    if len(result["rejected_samples"]) < 5:
                        slug = str(market.get("slug") or "")[:40]
                        # Pull the latest reason from _reject_cache (keyed by YES token_id)
                        reason = ""
                        raw_ids = market.get("clobTokenIds") or "[]"
                        try:
                            if isinstance(raw_ids, str):
                                raw_ids = _json.loads(raw_ids)
                            yes_tid = str((raw_ids or [None])[0])
                            entry = self._reject_cache.get(yes_tid)
                            if entry:
                                reason = str(entry[1])[:40]
                        except Exception:
                            pass
                        result["rejected_samples"].append((slug, reason))
            except Exception as e:
                log(f"[auto_curator] market iter err: {e}")
        return result

    @staticmethod
    def _league_matches(tag_label: str) -> bool:
        low = (tag_label or "").lower()
        return any(approved in low for approved in APPROVED_LEAGUE_TAGS)

    @staticmethod
    def _is_high_retire_risk_market(tags: List[str]) -> bool:
        lowered = [str(tag or "").lower() for tag in (tags or [])]
        return any(risk_tag in tag for tag in lowered for risk_tag in HIGH_RETIRE_RISK_TAGS)

    @staticmethod
    def _extract_league_tags_from_market(market: Dict[str, Any]) -> List[str]:
        """Collect league-tag labels from a /markets response row.
        Priority: nested events[0].tags → top-level market.tags (fallback).
        """
        out: List[str] = []

        def _pull(raw: Any) -> None:
            if not raw:
                return
            # /markets can return tags as a JSON-encoded string on some fields
            if isinstance(raw, str):
                try:
                    raw = _json.loads(raw)
                except Exception:
                    return
            if not isinstance(raw, list):
                return
            for t in raw:
                if isinstance(t, dict):
                    label = t.get("label") or t.get("slug") or t.get("name") or ""
                else:
                    label = str(t)
                label = str(label).strip()
                if label:
                    out.append(label)

        events_field = market.get("events") or []
        if isinstance(events_field, str):
            try:
                events_field = _json.loads(events_field)
            except Exception:
                events_field = []
        if isinstance(events_field, list):
            for ev in events_field:
                if isinstance(ev, dict):
                    _pull(ev.get("tags"))
        _pull(market.get("tags"))
        return out

    async def _try_add_market(
        self,
        market: Dict[str, Any],
        league_tags: List[str],
        session_label: str = "day",
        night_cutoff_ts: float = 0.0,
    ) -> bool:
        # Parse dual token ids (YES at index 0, NO at index 1)
        raw_ids = market.get("clobTokenIds") or "[]"
        if isinstance(raw_ids, str):
            try:
                ids = _json.loads(raw_ids)
            except Exception:
                return False
        else:
            ids = raw_ids
        if not isinstance(ids, list) or len(ids) < 2:
            return False
        yes_token_id = str(ids[0])
        no_token_id = str(ids[1])
        if not yes_token_id.isdigit() or not no_token_id.isdigit():
            return False

        # Already in engine (manual config OR previously added OR dual-side-injected).
        # Dedup against BOTH pools — day and night must not share a market.
        night_cfg = getattr(self.engine, "_night_market_cfg", {}) or {}
        if (yes_token_id in self.engine.market_cfg
                or no_token_id in self.engine.market_cfg
                or yes_token_id in night_cfg
                or no_token_id in night_cfg):
            return False

        # Closed / inactive
        if market.get("closed") or market.get("archived"):
            self._reject(yes_token_id, "closed_or_archived")
            return False
        if market.get("active") is False:
            self._reject(yes_token_id, "inactive")
            return False

        # Slug must carry YYYY-MM-DD (same guard engine._is_sports_market uses)
        slug = str(market.get("slug") or "")
        if not _SPORTS_SLUG_DATE_RE.search(slug):
            self._reject(yes_token_id, f"slug_no_date:{slug[:30]}")
            return False

        # Game start time
        game_start_raw = market.get("gameStartTime") or market.get("game_start_time")
        game_start_ts = _parse_ts(game_start_raw)
        if game_start_ts is None:
            self._reject(yes_token_id, "no_game_start_ts")
            return False
        now = time.time()
        seconds_to_start = game_start_ts - now
        is_high_retire_risk = self._is_high_retire_risk_market(league_tags)
        min_time_to_start_sec = HIGH_RETIRE_RISK_PRE_START_STOP_SEC if is_high_retire_risk else MIN_TIME_TO_START_SEC
        if session_label == "night":
            # Night only admits games starting at/after next BJT 08:00.
            # Day markets are fully dropped at the switch — no inheritance.
            if game_start_ts < float(night_cutoff_ts):
                hours_short = (float(night_cutoff_ts) - game_start_ts) / 3600.0
                self._reject(yes_token_id, f"night_before_8am:{hours_short:.1f}h_short")
                return False
        if seconds_to_start < min_time_to_start_sec:
            cutoff_h = min_time_to_start_sec / 3600.0
            self._reject(yes_token_id, f"too_soon:{int(seconds_to_start/60)}min<cutoff_{cutoff_h:.0f}h")
            return False

        # Rewards gate — prune BEFORE depth HTTP call. Sum all clobRewards entries' daily rate.
        clob_rewards = market.get("clobRewards")
        daily_reward = Decimal("0")
        if isinstance(clob_rewards, list):
            for item in clob_rewards:
                if isinstance(item, dict):
                    try:
                        daily_reward += Decimal(str(item.get("rewardsDailyRate") or 0))
                    except Exception:
                        pass
        if daily_reward < REWARD_MIN_DAILY_USD:
            self._reject(yes_token_id, f"reward=${daily_reward}<{int(REWARD_MIN_DAILY_USD)}")
            return False

        # Depth check — total bid-side notional ≥ $100k (scanner.py-style).
        ok, detail = await self._check_bid_depth(yes_token_id)
        if not ok:
            self._reject(yes_token_id, f"depth:{detail}")
            return False
        no_ok, no_detail = await self._check_orderbook_exists(no_token_id)
        if not no_ok:
            self._reject(yes_token_id, f"paired_no_book:{no_detail}")
            return False

        # Reward-spread bounty (used as our max_incentive_spread)
        spread_raw = (market.get("rewardsMaxSpread")
                      or market.get("maxIncentiveSpread")
                      or market.get("rewardsMaxSpreadCent"))
        try:
            spread = Decimal(str(spread_raw)) if spread_raw is not None else Decimal("3.0")
        except Exception:
            spread = Decimal("3.0")
        if spread <= 0:
            spread = Decimal("3.0")

        question = str(market.get("question") or "")
        league_display = ",".join(league_tags[:3]) if league_tags else ""
        try:
            added = self.engine.add_market_runtime(
                token_id=yes_token_id,
                paired_token_id=no_token_id,
                spread=spread,
                tick=None,
                min_distance=None,
                min_distance_ticks=None,
                risk="mid",
                session=session_label,
                source=f"auto_curator:{session_label}",
                game_start_ts=float(game_start_ts),
                slug=slug,
                league=league_display,
                question=question,
                pre_start_stop_sec_override=(HIGH_RETIRE_RISK_PRE_START_STOP_SEC if is_high_retire_risk else None),
                league_tags=league_tags,
            )
        except Exception as e:
            log(f"[auto_curator] add_market_runtime err token={yes_token_id[:16]}: {e}")
            return False

        if added:
            leagues_str = ",".join(league_tags[:3])
            game_in_h = (game_start_ts - now) / 3600.0
            risk_flag = " high_retire_risk=1" if is_high_retire_risk else ""
            log(f"[auto_curator] ADDED session={session_label} token={yes_token_id[:16]} "
                f"slug={slug[:40]} leagues={leagues_str} spread={spread} "
                f"reward=${daily_reward}/d game_in={game_in_h:.1f}h depth={detail}{risk_flag}")
        return added

    # ── depth check ──────────────────────────────────────────────────────────

    async def _load_book(self, token_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        try:
            book = await asyncio.to_thread(self._fetch_book, token_id)
        except Exception as e:
            return None, f"book_err:{str(e)[:40]}"
        if not book:
            return None, "empty_book"
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids and not asks:
            return None, "no_bids_or_asks"
        return book, "ok"

    async def _check_orderbook_exists(self, token_id: str) -> Tuple[bool, str]:
        book, detail = await self._load_book(token_id)
        return book is not None, detail

    async def _check_bid_depth(self, token_id: str) -> Tuple[bool, str]:
        """Total bid-side notional across all levels (matches scanner.py
        fetch_bid_depth / dashboard default). Must be ≥ DEPTH_MIN_BID_NOTIONAL_USD.
        """
        book, detail = await self._load_book(token_id)
        if not book:
            return False, detail
        bids = book.get("bids") or []
        bid_notional = Decimal("0")
        for lv in bids:
            try:
                p = Decimal(str(lv.get("price", 0)))
                s = Decimal(str(lv.get("size", 0)))
                if p > 0 and s > 0:
                    bid_notional += p * s
            except Exception:
                continue
        if bid_notional < DEPTH_MIN_BID_NOTIONAL_USD:
            return False, f"bid=${int(bid_notional)}<{int(DEPTH_MIN_BID_NOTIONAL_USD)}"
        return True, f"bid=${int(bid_notional)}"

    # ── HTTP ─────────────────────────────────────────────────────────────────

    def _fetch_sports_markets(self) -> List[Dict[str, Any]]:
        """Paginated GET on gamma /markets sorted by 24h volume desc.
        Matches scanner.py / dashboard default coverage — no event-level tag
        filter; sport-ness is decided downstream by slug date regex + nested
        league-tag match.

        NOTE: `include_tag=true` is required. Without it gamma returns
        `tags: null` and `events[*].tags: []` for every market, which silently
        strips the league info _extract_league_tags_from_market relies on —
        leading to `league_matched=0` across the board.
        """
        out: List[Dict[str, Any]] = []
        # Gamma currently caps this endpoint at 100 rows even when a larger
        # limit is requested. Using 500 made the first 100-row response look
        # like the final page, so the curator silently ignored the rest.
        page_size = 100
        offset = 0
        max_pages = 100  # 100 * 100 = 10k markets, same ceiling as scanner.py
        for _ in range(max_pages):
            try:
                r = requests.get(
                    GAMMA_MARKETS_URL,
                    params={
                        "limit": page_size,
                        "offset": offset,
                        "active": "true",
                        "closed": "false",
                        "archived": "false",
                        "order": "volume24hr",
                        "ascending": "false",
                        "include_tag": "true",
                    },
                    timeout=20,
                )
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                log(f"[auto_curator] markets fetch err offset={offset}: {e}")
                break
            if isinstance(data, dict):
                data = data.get("data") or data.get("markets") or []
            if not isinstance(data, list) or not data:
                break
            out.extend(data)
            if len(data) < page_size:
                break
            offset += len(data)
        return out

    def _fetch_book(self, token_id: str) -> Optional[Dict[str, Any]]:
        try:
            r = requests.get(CLOB_BOOK_URL, params={"token_id": token_id}, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception:
            return None

    # ── diagnostics ──────────────────────────────────────────────────────────

    def _reject(self, token_id: str, reason: str) -> None:
        self._stats["rejected_total"] += 1
        prev = self._reject_cache.get(token_id)
        now = time.time()
        if prev and (now - prev[0]) < self._reject_cache_ttl_sec and prev[1] == reason:
            return  # de-dup chatty rejections
        self._reject_cache[token_id] = (now, reason)
        # Only log on first sighting / reason change; keeps log volume sane
        log(f"[auto_curator] REJECT token={token_id[:16]} reason={reason}")

    def stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "last_scan_ts": self._last_scan_ts,
            "reject_cache_size": len(self._reject_cache),
        }
