import asyncio
import json
import os
import random
import time
from datetime import datetime
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import websockets
from py_clob_client.client import ClobClient
from scanner import normalize_market


HTTP_PROXIES = None       # read operations: book queries, gamma API, meta — routed through proxy pool
HTTP_PROXIES_WRITE = None  # write operations: cancel, place order — always direct (None)
WS_PROXY = None
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL


@dataclass
class TopOfBook:
    best_bid: Decimal
    best_ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.best_bid + self.best_ask) / Decimal("2")


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        safe = line.encode("gbk", errors="ignore").decode("gbk", errors="ignore")
        print(safe, flush=True)


def _choose_proxy(cfg: dict, for_ws: bool, shard_key: str = "") -> str | None:
    """Select a proxy from the pool.

    for_ws=True  → WS connections (long-lived)
    for_ws=False → HTTP read operations (book queries, gamma API)

    Write operations (cancel, place) always use None (direct) — callers must NOT
    pass HTTP_PROXIES for these; use HTTP_PROXIES_WRITE which is always None.

    Proxy rotation uses hash(shard_key) for stable per-token assignment.
    This distributes different markets across different proxies rather than
    switching all markets to the same proxy simultaneously.
    """
    pp = cfg.get("proxy_pool") or {}
    if not pp.get("enabled"):
        return None
    if for_ws and not pp.get("use_for_ws", False):
        return None
    if (not for_ws) and not pp.get("use_for_reads", True):
        return None

    # enabled semantics: only explicit False means disabled; None/omitted are treated as enabled
    items = [
        x for x in (pp.get("items") or [])
        if x and x.get("enabled") is not False and str(x.get("url", "")).strip()
    ]
    if not items:
        return None

    weighted = []
    for it in items:
        try:
            w = int(it.get("weight") or 1)
        except Exception:
            w = 1
        weighted.extend([it] * max(1, w))

    # Stable per-token hash assignment — different markets use different proxies
    if shard_key:
        idx = abs(hash(shard_key)) % len(weighted)
    else:
        idx = int(time.time() // 300) % len(weighted)
    return str(weighted[idx].get("url", "")).strip() or None


def _init_proxy_settings(cfg: dict):
    """Initialise global proxy references.

    HTTP_PROXIES       → read operations (book queries, gamma API calls)
    HTTP_PROXIES_WRITE → write operations (cancel, place order) — always None/direct
    WS_PROXY           → WebSocket connections
    """
    global HTTP_PROXIES, HTTP_PROXIES_WRITE, WS_PROXY
    read_proxy = _choose_proxy(cfg, for_ws=False)
    ws_proxy = _choose_proxy(cfg, for_ws=True)

    WS_PROXY = ws_proxy
    HTTP_PROXIES_WRITE = None  # writes always go direct — never proxy

    if read_proxy:
        HTTP_PROXIES = {"http": read_proxy, "https": read_proxy}
    else:
        HTTP_PROXIES = None


class PolyLPSMulti:
    """
    PolyLPS-Multi (single-account, multi-market) using py-clob-client.

    - Multi-market book polling (official client)
    - Per-market passive quoting
    - Global kill-switch on detected fill notifications
    - Optional Discord webhook alerts
    """

    def __init__(self, config_path: str = "config.json") -> None:
        cfg_path = Path(config_path)
        self.cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        _init_proxy_settings(self.cfg)

        account = self.cfg.get("account", {})
        host = self.cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
        chain_id = int(account.get("chain_id", 137))
        signature_type = int(account.get("signature_type", 0))
        self.signature_type = signature_type
        funder = str(account.get("funder", "")).strip()
        env_key = os.getenv("POLY_PRIVATE_KEY", "").strip()
        private_key = env_key or str(account.get("private_key", "")).strip()
        if not private_key or "REPLACE" in private_key or "REDACTED" in private_key:
            raise ValueError("Private key missing. Set POLY_PRIVATE_KEY env var or config.account.private_key")

        client_kwargs = {
            "host": host,
            "chain_id": chain_id,
            "key": private_key,
            "signature_type": signature_type,
        }
        if funder:
            client_kwargs["funder"] = funder
        self.client = ClobClient(**client_kwargs)
        self.api_creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(self.api_creds)

        strategy = self.cfg.get("strategy", {})
        risk = self.cfg.get("risk", {})

        self.requote_interval_ms = int(strategy.get("requote_interval_ms", 500))
        self.default_tick = Decimal(str(strategy.get("default_price_tick", 0.1)))
        self.default_min_distance = Decimal(str(strategy.get("default_min_distance_from_best_bid", 0.1)))
        self.min_order_size = Decimal(str(strategy.get("min_order_size", 5)))
        self.quote_balance_pct_min = Decimal(str(strategy.get("quote_balance_pct_min", 0.90)))
        self.quote_balance_pct_max = Decimal(str(strategy.get("quote_balance_pct_max", 0.99)))
        # risk-tier percentage ranges (based on TOTAL principal)
        self.quote_balance_pct_ranges = {
            "low": (
                Decimal(str(strategy.get("quote_balance_pct_min_low", 0.95))),
                Decimal(str(strategy.get("quote_balance_pct_max_low", 0.99))),
            ),
            "mid": (
                Decimal(str(strategy.get("quote_balance_pct_min_mid", 0.80))),
                Decimal(str(strategy.get("quote_balance_pct_max_mid", 0.95))),
            ),
            "high": (
                Decimal(str(strategy.get("quote_balance_pct_min_high", 0.50))),
                Decimal(str(strategy.get("quote_balance_pct_max_high", 0.70))),
            ),
        }
        self.post_only = bool(strategy.get("post_only", True))
        self.auto_tick = bool(strategy.get("auto_tick", True))

        self.kill_switch_on_fill = bool(risk.get("kill_switch_on_fill", True))
        self.cooldown_seconds = int(risk.get("cooldown_seconds", 60))

        reporting = self.cfg.get("reporting", {})
        self.discord_webhook = str(reporting.get("discord_webhook", "")).strip()
        self.hourly_summary = bool(reporting.get("hourly_summary", True))

        self.market_cfg: Dict[str, Dict[str, Any]] = {}
        for m in self.cfg.get("markets", []):
            if not m.get("enabled", True):
                continue
            token_id = str(m.get("token_id", ""))
            if not token_id.isdigit():
                continue
            self.market_cfg[token_id] = {
                "spread": Decimal(str(m.get("max_incentive_spread", 0.02))),
                "tick": Decimal(str(m.get("price_tick", self.default_tick))),
                "min_distance": Decimal(str(m.get("min_distance_from_best_bid", self.default_min_distance))),
                "risk": str(m.get("risk", "mid")).lower(),
            }

        if not self.market_cfg:
            raise ValueError("No valid enabled markets in config.markets")

        self.market_states: Dict[str, TopOfBook] = {}
        self.last_quote_ts: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}
        self._tick_resolved: set[str] = set()

        self._cooldown_until = 0.0
        self._running = True
        self._fills_seen = 0
        self._quotes_sent = 0
        self._balance_fail_streak = 0
        self.max_balance_fail_streak = int(risk.get("max_balance_fail_streak", 8))
        # {token_id: (anchor_value, timestamp)} — TTL-based
        self._anchor_cache: Dict[str, tuple] = {}

        # per-market failure isolation (do not nuke all events on single-market balance issues)
        self._market_balance_fail_streak: Dict[str, int] = {tid: 0 for tid in self.market_cfg}
        self._market_skip_until: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}

        # execution pacing: risk actions immediate, normal posting lightly paced
        execution = self.cfg.get("execution", {})
        self.post_delay_min_sec = float(execution.get("post_delay_min_sec", 1))
        self.post_delay_max_sec = float(execution.get("post_delay_max_sec", 3))

        # market reward-health auto offlining
        self.health_check_interval_sec = int(execution.get("health_check_interval_sec", 600))
        self.health_fail_threshold = int(execution.get("health_fail_threshold", 2))
        self.health_near_expiry_hours = int(execution.get("health_near_expiry_hours", 24))
        self._health_fail_streak: Dict[str, int] = {tid: 0 for tid in self.market_cfg}
        self._book_req_exc_streak: Dict[str, int] = {tid: 0 for tid in self.market_cfg}
        self.net_degraded_fail_threshold = int(execution.get("net_degraded_fail_threshold", 10))
        # request-exception storm gate (anti-jitter tuned)
        self.global_req_exc_window_sec = int(execution.get("global_req_exc_window_sec", 45))
        self.global_req_exc_events_threshold = int(execution.get("global_req_exc_events_threshold", 8))
        self.req_exc_confirm_trade_poll = int(execution.get("req_exc_confirm_trade_poll", 2))
        self.req_exc_confirm_guard_loop = int(execution.get("req_exc_confirm_guard_loop", 2))
        self._req_exc_recent: Dict[str, float] = {}
        self._trade_poll_req_exc_streak: int = 0
        self._guard_loop_req_exc_streak: int = 0
        self._poll_err_ts: float = 0.0
        self._last_ws_ok_ts: float = 0.0
        self._last_http_ok_ts: float = 0.0
        self.ws_down_trigger_sec = int(execution.get("ws_down_trigger_sec", 10))
        self.cancel_retry_window_sec = int(execution.get("cancel_retry_window_sec", 300))
        self.cancel_retry_step_sec = int(execution.get("cancel_retry_step_sec", 5))
        self.recovery_quiet_sec = int(execution.get("recovery_quiet_sec", 30))
        self._require_recovery_gate = False
        self.min_front_bid_notional_usdc = Decimal(str(execution.get("min_front_bid_notional_usdc", 2000)))
        self._book_loop_concurrency: int = int(execution.get("book_loop_concurrency", 5))
        self.blocked_slug_keywords = [
            "crude-oil-cl-hit",
            "presidential-election-winner-2028",
            "2028-us-presidential-election",
        ]
        self._token_slug_cache: Dict[str, str] = {}
        # {token_id: (meta_dict, timestamp)} — TTL prevents stale reward/spread data
        self._market_meta_cache: Dict[str, tuple] = {}
        self._meta_cache_ttl_sec: int = int(execution.get("meta_cache_ttl_sec", 300))
        # {token_id: anchor_ttl_sec}
        self._anchor_cache_ttl_sec: int = int(execution.get("anchor_cache_ttl_sec", 120))
        # YES/NO paired token map: {token_id: paired_token_id}
        self._paired_token_cache: Dict[str, str] = {}

        # aggressive fill guard (event-level offlining)
        self.fill_size_threshold = Decimal(str(risk.get("fill_size_threshold", 0.01)))
        self.fill_debounce_sec = float(risk.get("fill_debounce_sec", 1.0))
        self.event_ban_ttl_sec = int(risk.get("event_ban_ttl_sec", 24 * 3600))
        self._event_banned_until: Dict[str, float] = {}
        self._event_last_trigger_ts: Dict[str, float] = {}
        self._signal_seen_ts: Dict[str, float] = {}
        self._last_remaining_by_order: Dict[str, Decimal] = {}
        self._last_plan_sig: Dict[str, str] = {}
        self._seen_trade_ids: set[str] = set()
        # ordered insertion list — used for correct FIFO truncation (set is unordered)
        self._seen_trade_ids_order: list[str] = []
        # pending unwind SELL orders: [{token_id, fill_price, fill_size, order_id, placed_at}]
        self._pending_unwinds: list[dict] = []
        self._unwind_check_interval_sec: int = int(execution.get("unwind_check_interval_sec", 1800))
        self._unwind_max_age_sec: int = int(execution.get("unwind_max_age_sec", 14400))

        # state writer
        self._state_write_interval_sec: int = int(execution.get("state_write_interval_sec", 5))
        _maker_dir = Path(config_path).resolve().parent
        _cfg_stem = Path(config_path).stem  # "config", "config_1", "config_2", ...
        if _cfg_stem.startswith("config_") and _cfg_stem[7:].isdigit():
            _state_fname = f"engine_state_{_cfg_stem[7:]}.json"
        else:
            _state_fname = "engine_state.json"
        self._state_path: Path = _maker_dir.parent.parent.parent / "data" / _state_fname
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._fills_record: list[dict] = []
        self._market_live_orders: Dict[str, list] = {}
        self._last_balance: Optional[Decimal] = None

        # multi-account shared book cache (set by multi_runner; None in single-account mode)
        self._shared_book_cache: Optional[Any] = None
        # multi-account cycle sleep (random interval between full book-loop cycles)
        self._multi_cycle_sleep_min: float = float(execution.get("multi_cycle_sleep_min_sec", 3.0))
        self._multi_cycle_sleep_max: float = float(execution.get("multi_cycle_sleep_max_sec", 20.0))

    @staticmethod
    def _floor_to_tick(px: Decimal, tick: Decimal) -> Decimal:
        return (px / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    async def _action_delay(self, label: str) -> None:
        # emergency/risk/control actions should be immediate
        return

    async def _post_delay(self, label: str) -> None:
        lo = max(0.0, self.post_delay_min_sec)
        hi = max(lo, self.post_delay_max_sec)
        d = random.uniform(lo, hi)
        log(f"[pace] {label} sleep={d:.2f}s")
        await asyncio.sleep(d)

    @staticmethod
    def _infer_tick_from_book(best_bid: Decimal, best_ask: Decimal) -> Decimal:
        # Common Polymarket price grids are 0.01 (1¢) or 0.001 (0.1¢)
        for px in (best_bid, best_ask):
            # normalize exponent, e.g. Decimal('0.201') => -3
            exp = px.normalize().as_tuple().exponent
            if exp <= -3:
                return Decimal("0.001")
        return Decimal("0.01")

    async def _resolve_market_tick(self, token_id: str, best_bid: Decimal, best_ask: Decimal) -> None:
        if (not self.auto_tick) or (token_id in self._tick_resolved):
            return

        resolved: Optional[Decimal] = None
        # 1) Try official tick-size endpoint via py-clob-client
        try:
            t = await asyncio.to_thread(self.client.get_tick_size, token_id)
            # compatible with dict/object/string
            if isinstance(t, dict):
                val = t.get("tick_size") or t.get("tickSize") or t.get("minimumTickSize")
                if val is not None:
                    resolved = Decimal(str(val))
            elif hasattr(t, "tick_size"):
                resolved = Decimal(str(getattr(t, "tick_size")))
            elif t is not None:
                resolved = Decimal(str(t))
        except Exception:
            resolved = None

        # 2) Fallback infer from visible book precision
        if resolved is None or resolved <= 0:
            resolved = self._infer_tick_from_book(best_bid, best_ask)

        if resolved not in (Decimal("0.01"), Decimal("0.001")):
            # clamp to known-safe grids in this strategy
            resolved = Decimal("0.001") if resolved < Decimal("0.01") else Decimal("0.01")

        self.market_cfg[token_id]["tick"] = resolved
        # keep min distance at least one tick
        self.market_cfg[token_id]["min_distance"] = max(self.market_cfg[token_id]["min_distance"], resolved)
        self._tick_resolved.add(token_id)
        log(f"[tick-auto] {token_id}: tick={resolved}")

    async def _normalize_guard_best_bid(self, token_id: str, book_now: Any) -> Optional[Decimal]:
        """Use the same anti-placeholder top-of-book normalization as quote path."""
        if not book_now or not getattr(book_now, "bids", None) or not getattr(book_now, "asks", None):
            return None
        try:
            best_bid = Decimal(str(book_now.bids[0].price))
            best_ask = Decimal(str(book_now.asks[0].price))
        except Exception:
            return None

        if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            return None

        # align with quote-path fallback for placeholder books like 0.001/0.999
        if best_bid <= Decimal("0.02") and best_ask >= Decimal("0.98"):
            anchor = await self._get_anchor_bid_from_gamma(token_id)
            if anchor is None or anchor <= 0:
                log(f"[guard-loop] skip token={token_id} reason=placeholder_book_no_anchor bid={best_bid} ask={best_ask}")
                return None
            best_bid = anchor

        return best_bid

    async def _check_not_at_best_bid(self, token_id: str) -> None:
        """Cancel any of our orders that are at or above current best_bid."""
        try:
            book_now = await asyncio.to_thread(self.client.get_order_book, token_id)
            current_best_bid = await self._normalize_guard_best_bid(token_id, book_now)
            if current_best_bid is None:
                return

            orders = await asyncio.to_thread(self.client.get_orders)
            at_risk = [
                o for o in orders
                if str(o.get("status", "")).lower() in ("live", "open", "active")
                and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
                and Decimal(str(o.get("price", 0) or 0)) >= current_best_bid
            ]
            if not at_risk:
                return
            ids = [o.get("id") or o.get("orderID") for o in at_risk if (o.get("id") or o.get("orderID"))]
            if ids:
                await asyncio.to_thread(self.client.cancel_orders, ids)
                log(f"[safety] best_bid_guard cancelled {len(ids)} orders at/above best_bid={current_best_bid} token={token_id}")
                self._last_plan_sig[token_id] = ""
                # do not just cancel-and-idle: force next pass to rebuild quote from latest book
                self.last_quote_ts[token_id] = 0.0
        except Exception as e:
            log(f"[safety] best_bid_guard error token={token_id} err={e}")
            if self._is_req_exc(e):
                self._log_req_diag("best-bid-guard-check", e, token_id)

    async def best_bid_guard_loop(self) -> None:
        """Continuous background loop: scan all live orders across all markets.
        If any order is at or above the current best_bid, cancel it immediately.
        Runs independently of the quote cycle — covers cooldowns, skips, and gaps.

        Per-token check interval: each token_id is only book-fetched every
        guard_per_token_interval_sec to limit API load when many markets are active.
        """
        guard_interval = 2.0          # seconds between outer loop ticks
        per_token_interval = 10.0     # min seconds between book fetches per token
        _last_guard_ts: dict[str, float] = {}

        while self._running:
            try:
                orders = await asyncio.to_thread(self.client.get_orders)
                live = [
                    o for o in orders
                    if str(o.get("status", "")).lower() in ("live", "open", "active")
                ]
                if not live:
                    await asyncio.sleep(guard_interval)
                    continue

                # Group by token_id
                by_token: dict[str, list] = {}
                for o in live:
                    tid = str(o.get("asset_id") or o.get("token_id") or "")
                    if tid in self.market_cfg:
                        by_token.setdefault(tid, []).append(o)

                now = time.time()
                cancel_ids = []
                for tid, tok_orders in by_token.items():
                    # Skip if checked recently
                    if now - _last_guard_ts.get(tid, 0.0) < per_token_interval:
                        continue
                    _last_guard_ts[tid] = now
                    try:
                        book_now = await asyncio.to_thread(self.client.get_order_book, tid)
                        best_bid = await self._normalize_guard_best_bid(tid, book_now)
                        if best_bid is None:
                            continue
                        for o in tok_orders:
                            op = Decimal(str(o.get("price", 0) or 0))
                            if op >= best_bid:
                                oid = o.get("id") or o.get("orderID")
                                if oid:
                                    cancel_ids.append((tid, best_bid, oid))
                    except Exception:
                        continue

                if cancel_ids:
                    ids = [oid for _, _, oid in cancel_ids]
                    await asyncio.to_thread(self.client.cancel_orders, ids)

                    touched_tokens: set[str] = set()
                    for tid, bb, oid in cancel_ids:
                        log(f"[guard-loop] cancelled order at best_bid={bb} token={tid} oid={oid}")
                        self._last_plan_sig[tid] = ""
                        self.last_quote_ts[tid] = 0.0
                        touched_tokens.add(tid)

                    # user-requested behavior: after post-check cancel, immediately rebuild quote
                    # from latest best-bid/ask (same quoting logic), instead of cancel-and-wait only.
                    for tid in touched_tokens:
                        try:
                            await self.update_and_quote_market(tid)
                        except Exception as e:
                            log(f"[guard-loop] requote error token={tid} err={e}")

                # healthy iteration resets guard req-exception streak
                self._guard_loop_req_exc_streak = 0

            except Exception as e:
                log(f"[guard-loop] error: {e}")
                if self._is_req_exc(e):
                    self._log_req_diag("guard-loop", e)
                    self._guard_loop_req_exc_streak += 1
                    if self._guard_loop_req_exc_streak >= self.req_exc_confirm_guard_loop:
                        await self._mark_req_exc_and_maybe_storm("guard-loop", "global_request_exception_storm")
                else:
                    self._guard_loop_req_exc_streak = 0
            await asyncio.sleep(guard_interval)

    def _build_price_legs(self, token_id: str, book: TopOfBook, live_spread: Optional[Decimal] = None) -> list[Decimal]:
        cfg = self.market_cfg[token_id]
        tick = cfg["tick"]

        # Prefer live rewardsMaxSpread from API; fall back to config value
        spread = live_spread if live_spread is not None else cfg["spread"]
        if spread > Decimal("1"):
            spread = spread / Decimal("100")

        # Valid range: [reward_lower, best_bid - 1 tick]
        # best_bid itself is NEVER included — it's a fill-risk boundary
        reward_lower = max(tick, book.mid - spread)
        safe_top = book.best_bid - tick  # ceiling: best_bid - 1 tick

        if safe_top < reward_lower or safe_top < tick:
            # No valid position exists in reward zone; skip this market
            return []

        # Number of qualifying positions below best_bid that remain in the reward zone
        # e.g. best_bid=0.30, reward_lower=0.27, tick=0.01 → range_ticks=3 → legs at -1/-2/-3
        range_ticks = int((book.best_bid - reward_lower) / tick)

        # Max legs by tick granularity:
        #   1 cent  (0.01) markets: reward zone is narrow, use up to 3 legs
        #   0.1 cent (0.001) markets: fine granularity, use up to 5 legs
        if tick >= Decimal("0.01"):
            max_legs = 3
        else:
            max_legs = 5

        n_legs = min(range_ticks, max_legs)
        if n_legs <= 0:
            return []

        prices = []
        for i in range(1, n_legs + 1):
            p = self._floor_to_tick(book.best_bid - tick * Decimal(i), tick)
            if p >= reward_lower and p >= tick:
                prices.append(p)

        return prices

    def _front_bid_notional(self, book_obj: Any, my_price: Decimal) -> Decimal:
        total = Decimal("0")
        try:
            bids = getattr(book_obj, "bids", None) or []
            for b in bids:
                bp = Decimal(str(getattr(b, "price", 0) or 0))
                bs = Decimal(str(getattr(b, "size", 0) or 0))
                # depth at/above our quote price (include target level liquidity)
                if bp >= my_price and bs > 0:
                    total += bp * bs
        except Exception:
            return Decimal("0")
        return total

    @staticmethod
    def _alloc_weights(n_legs: int) -> list[Decimal]:
        if n_legs <= 1:
            return [Decimal("1")]
        if n_legs == 2:
            return [Decimal("0.5"), Decimal("0.5")]
        return [Decimal("0.3"), Decimal("0.3"), Decimal("0.4")]

    @staticmethod
    def _norm_usdc(v: Optional[Decimal]) -> Optional[Decimal]:
        if v is None:
            return None
        # Normalize likely raw 6-decimal USDC integer units.
        # Examples: 160626629 -> 160.626629 ; 821009 -> 0.821009
        if v == v.to_integral_value():
            av = abs(v)
            if av >= Decimal("1000"):
                return v / Decimal("1000000")
        if abs(v) >= Decimal("1000000"):
            return v / Decimal("1000000")
        return v

    def _to_end_ts(self, m: dict) -> Optional[float]:
        keys = ["endDate", "end_date", "endTime", "end_time", "expiration", "resolveBy", "endTimestamp", "end_timestamp"]
        for k in keys:
            v = m.get(k)
            if v is None:
                continue
            try:
                if isinstance(v, (int, float)):
                    x = float(v)
                    return x / 1000.0 if x > 10_000_000_000 else x
                s = str(v).strip()
                if not s:
                    continue
                if s.isdigit():
                    x = float(s)
                    return x / 1000.0 if x > 10_000_000_000 else x
                s = s.replace("Z", "+00:00")
                return datetime.fromisoformat(s).timestamp()
            except Exception:
                continue
        return None

    async def _get_market_meta(self, token_id: str) -> Dict[str, Any]:
        cached = self._market_meta_cache.get(token_id)
        if cached and (time.time() - cached[1]) < self._meta_cache_ttl_sec:
            return cached[0]
        try:
            r = await asyncio.to_thread(
                requests.get,
                "https://gamma-api.polymarket.com/markets",
                params={"clob_token_ids": token_id, "limit": 1},
                timeout=20,
                proxies=self._read_proxies_for_token(token_id),
            )
            if r.status_code == 200:
                arr = r.json()
                if isinstance(arr, list) and arr:
                    raw = arr[0]
                    nm = normalize_market(raw)
                    self._market_meta_cache[token_id] = (nm, time.time())
                    # extract paired YES/NO token for event-level ban
                    ids = raw.get("clobTokenIds")
                    if isinstance(ids, str):
                        try:
                            ids = json.loads(ids)
                        except Exception:
                            ids = []
                    if isinstance(ids, list):
                        for other in [str(x) for x in ids]:
                            if other != token_id and other.isdigit():
                                self._paired_token_cache[token_id] = other
                                break
                    return nm
        except Exception:
            pass
        return {}

    async def _is_blocked_market(self, token_id: str) -> tuple[bool, str]:
        try:
            meta = await self._get_market_meta(token_id)
            slug = str(meta.get("slug") or self._token_slug_cache.get(token_id, ""))
            if slug:
                self._token_slug_cache[token_id] = slug
            s = slug.lower()
            for kw in self.blocked_slug_keywords:
                if kw in s:
                    return True, f"blocked_slug:{kw}"
            return False, ""
        except Exception:
            return False, ""

    async def _check_market_reward_health(self, token_id: str) -> tuple[bool, str]:
        try:
            r = await asyncio.to_thread(
                requests.get,
                "https://gamma-api.polymarket.com/markets",
                params={"clob_token_ids": token_id, "limit": 3},
                timeout=20,
                proxies=self._read_proxies_for_token(token_id),
            )
            if r.status_code != 200:
                return True, "api_unstable_skip"
            arr = r.json()
            if not isinstance(arr, list) or not arr:
                return False, "market_not_found"
            nm = normalize_market(arr[0])
            reward = Decimal(str(nm.get("reward") or 0))
            if reward <= 0:
                return False, "reward_zero"
            msp = Decimal(str(nm.get("maxIncentiveSpread") or 0))
            if msp <= 0:
                return False, "incentive_spread_zero"
            end_ts = self._to_end_ts(arr[0])
            if end_ts and (end_ts - time.time()) < self.health_near_expiry_hours * 3600:
                return False, f"near_expiry_lt_{self.health_near_expiry_hours}h"
            return True, "ok"
        except Exception:
            return True, "api_error_skip"

    async def _deactivate_market(self, token_id: str, reason: str) -> None:
        self._event_banned_until[self._event_key(token_id)] = time.time() + self.event_ban_ttl_sec
        self._market_skip_until[token_id] = time.time() + self.event_ban_ttl_sec

        try:
            orders = await asyncio.to_thread(self.client.get_orders)
            live = [
                o for o in orders
                if str(o.get("status", "")).lower() in ("live", "open", "active")
                and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
            ]
            ids = [o.get("id") or o.get("orderID") for o in live if (o.get("id") or o.get("orderID"))]
            if ids:
                await self._action_delay(f"health-cancel token={token_id}")
                await asyncio.to_thread(self.client.cancel_orders, ids)
        except Exception as e:
            log(f"[health] cancel fail token={token_id} err={e}")

        # persist disabled in config markets
        try:
            cfg_path = Path("config.json")
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            for m in cfg.get("markets", []):
                if str(m.get("token_id", "")) == str(token_id):
                    m["enabled"] = False
            cfg_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            log(f"[health] config disable fail token={token_id} err={e}")

        msg = f"[ALERT] Reward invalid -> market offlined token={token_id} reason={reason}"
        log(f"[health] {msg}")
        self.send_discord(msg)

    async def market_health_loop(self) -> None:
        while self._running:
            for token_id in list(self.market_cfg.keys()):
                if self._event_is_banned(token_id):
                    continue

                blocked, breason = await self._is_blocked_market(token_id)
                if blocked:
                    await self._deactivate_market(token_id, breason)
                    continue

                ok, reason = await self._check_market_reward_health(token_id)
                if ok:
                    self._health_fail_streak[token_id] = 0
                else:
                    self._health_fail_streak[token_id] = self._health_fail_streak.get(token_id, 0) + 1
                    log(f"[health] token={token_id} fail={self._health_fail_streak[token_id]} reason={reason}")
                    if self._health_fail_streak[token_id] >= self.health_fail_threshold:
                        await self._deactivate_market(token_id, reason)
                        self._health_fail_streak[token_id] = 0
            await asyncio.sleep(max(60, self.health_check_interval_sec))

    def _event_key(self, token_id: str) -> str:
        return str(token_id)

    def _clean_signal_cache(self) -> None:
        now = time.time()
        for k, ts in list(self._signal_seen_ts.items()):
            if now - ts > 120:
                self._signal_seen_ts.pop(k, None)
        for k, ts in list(self._event_banned_until.items()):
            if now > ts:
                self._event_banned_until.pop(k, None)

    def _event_is_banned(self, token_id: str) -> bool:
        self._clean_signal_cache()
        exp = self._event_banned_until.get(self._event_key(token_id), 0.0)
        return time.time() < exp

    def _allow_signal(self, token_id: str, signal_key: str) -> bool:
        self._clean_signal_cache()
        ek = self._event_key(token_id)
        now = time.time()
        if self._event_is_banned(token_id):
            return False
        if signal_key in self._signal_seen_ts:
            return False
        last = self._event_last_trigger_ts.get(ek, 0.0)
        if now - last < self.fill_debounce_sec:
            return False
        self._signal_seen_ts[signal_key] = now
        self._event_last_trigger_ts[ek] = now
        return True

    async def _trigger_event_offline(
        self,
        token_id: str,
        reason: str,
        matched_size: Optional[Decimal] = None,
        matched_price: Optional[Decimal] = None,
    ) -> None:
        if self._event_is_banned(token_id):
            return

        log(f"[risk] SUSPECT_FILL token={token_id} reason={reason}")
        self._fills_record.append({
            "token_id": token_id,
            "price": float(matched_price) if matched_price is not None else None,
            "size": float(matched_size) if matched_size is not None else None,
            "reason": reason,
            "ts": time.time(),
        })
        if len(self._fills_record) > 200:
            self._fills_record = self._fills_record[-100:]
        self._event_banned_until[self._event_key(token_id)] = time.time() + self.event_ban_ttl_sec

        try:
            orders = await asyncio.to_thread(self.client.get_orders)
            live = [
                o for o in orders
                if str(o.get("status", "")).lower() in ("live", "open", "active")
                and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
            ]
            ids = [o.get("id") or o.get("orderID") for o in live if (o.get("id") or o.get("orderID"))]

            canceled = 0
            for i in range(3):
                if not ids:
                    break
                try:
                    await self._action_delay(f"cancel token={token_id} try={i+1}")
                    r = await asyncio.to_thread(self.client.cancel_orders, ids)
                    c = len((r or {}).get("canceled", [])) if isinstance(r, dict) else 0
                    canceled = max(canceled, c)
                    if c >= len(ids):
                        break
                except Exception as e:
                    log(f"[risk] cancel retry error token={token_id} try={i+1} err={e}")
                await asyncio.sleep(0.2 if i == 0 else 0.5)

            log(f"[risk] EVENT_BANNED token={token_id} canceled={canceled}/{len(ids)} ttl={self.event_ban_ttl_sec}s")

            # Unwind strategy: always post-only SELL at fill price (break-even exit).
            # All fills regardless of size get an unwind order.
            # unwind_tracking_loop monitors progress; if still open after unwind_max_age_sec,
            # sends a Discord alert for manual review — no automatic re-pricing.
            if matched_size is not None and matched_price is not None and matched_size > 0 and matched_price > 0:
                try:
                    await self._action_delay(f"unwind-post token={token_id}")
                    s_args = OrderArgs(token_id=token_id, price=float(matched_price), size=float(matched_size), side=SELL)
                    s_signed = await asyncio.to_thread(self.client.create_order, s_args)
                    s_resp = await asyncio.to_thread(self.client.post_order, s_signed, OrderType.GTC)
                    unwind_order_id = ""
                    if isinstance(s_resp, dict):
                        unwind_order_id = str(s_resp.get("orderID") or s_resp.get("id") or "")
                    notional = matched_size * matched_price
                    self._pending_unwinds.append({
                        "token_id": token_id,
                        "fill_price": float(matched_price),
                        "fill_size": float(matched_size),
                        "order_id": unwind_order_id,
                        "placed_at": time.time(),
                        "reason": reason,
                    })
                    log(
                        f"[risk] UNWIND_POSTED token={token_id} price={matched_price} size={matched_size} "
                        f"notional={notional:.2f} order_id={unwind_order_id}"
                    )
                except Exception as ue:
                    log(f"[risk] UNWIND_POST_FAIL token={token_id} err={ue}")

            # Also ban the paired YES/NO token to prevent correlated fills
            paired = self._paired_token_cache.get(token_id)
            if paired and paired in self.market_cfg and not self._event_is_banned(paired):
                log(f"[risk] banning paired token={paired} due to fill on token={token_id}")
                await self._trigger_event_offline(paired, f"paired_fill_from:{token_id}")

            self.send_discord(
                f"[ALERT] Event offlined token={token_id} reason={reason} canceled={canceled}/{len(ids)}"
            )
        except Exception as e:
            log(f"[risk] event offline failed token={token_id} err={e}")
            await self.trigger_global_kill_switch(f"event_offline_failed:{token_id}")

    async def _get_collateral_available(self) -> Optional[Decimal]:
        """Best-effort fetch of available collateral (normalized to USDC units)."""
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, token_id="", signature_type=self.signature_type)
            data = await asyncio.to_thread(self.client.get_balance_allowance, params)
            log(f"[debug-bal] raw={data} sig={self.signature_type}")
            if not isinstance(data, dict):
                return None

            bal = data.get("balance")
            alw = data.get("allowance")
            allowances_map = data.get("allowances") if isinstance(data.get("allowances"), dict) else None

            if isinstance(bal, dict):
                bal = bal.get("available") or bal.get("raw") or bal.get("value")
            if isinstance(alw, dict):
                alw = alw.get("available") or alw.get("raw") or alw.get("value")

            bal_d = self._norm_usdc(Decimal(str(bal))) if bal is not None else None
            alw_d = self._norm_usdc(Decimal(str(alw))) if alw is not None else None

            # handle allowances map + unlimited approvals
            allowance_unlimited = False
            if allowances_map:
                vals = []
                raw_ints = []
                for v in allowances_map.values():
                    try:
                        s = str(v).strip()
                        if s.lstrip('-').isdigit():
                            raw_ints.append(int(s))
                        vals.append(self._norm_usdc(Decimal(s)))
                    except Exception:
                        pass
                vals = [x for x in vals if x is not None]
                if raw_ints and min(raw_ints) >= 10**30:
                    allowance_unlimited = True
                    alw_d = None
                elif vals:
                    alw_d = min(vals)

            # if allowance looks absurdly tiny while balance is healthy, treat as parse artifact and trust balance
            if bal_d is not None and alw_d is not None and bal_d >= Decimal('50') and alw_d < Decimal('2'):
                alw_d = None

            if bal_d is not None and alw_d is not None and not allowance_unlimited:
                return min(bal_d, alw_d)
            if bal_d is not None:
                return bal_d
            if alw_d is not None:
                return alw_d
            return None
        except Exception:
            return None

    async def run(self) -> None:
        tasks = [
            asyncio.create_task(self.book_loop(), name="book_loop"),
            asyncio.create_task(self.fill_watch_loop(), name="fill_watch_loop"),
            asyncio.create_task(self.market_health_loop(), name="market_health_loop"),
            asyncio.create_task(self.unwind_tracking_loop(), name="unwind_tracking_loop"),
            asyncio.create_task(self.best_bid_guard_loop(), name="best_bid_guard_loop"),
            asyncio.create_task(self.state_write_loop(), name="state_write_loop"),
        ]
        if self.hourly_summary:
            tasks.append(asyncio.create_task(self.summary_loop(), name="summary_loop"))

        try:
            await asyncio.gather(*tasks)
        finally:
            self._running = False
            for t in tasks:
                t.cancel()

    def _read_proxies_for_token(self, token_id: str = "") -> Optional[dict]:
        p = _choose_proxy(self.cfg, for_ws=False, shard_key=str(token_id or ""))
        return {"http": p, "https": p} if p else None

    def _is_req_exc(self, e: Exception) -> bool:
        em = str(e)
        return (
            "Request exception" in em
            or isinstance(e, requests.exceptions.RequestException)
        )

    def _log_req_diag(self, scope: str, e: Exception, token_id: str = "") -> None:
        em = str(e).replace("\n", " ")[:240]
        cause = repr(getattr(e, "__cause__", None))[:180]
        read_proxy = "on" if HTTP_PROXIES else "off"
        ws_proxy = "on" if WS_PROXY else "off"
        log(
            f"[netdiag] scope={scope} token={token_id or '-'} etype={type(e).__name__} "
            f"read_proxy={read_proxy} ws_proxy={ws_proxy} msg={em} cause={cause}"
        )

    async def _mark_req_exc_and_maybe_storm(self, key: str, reason: str) -> None:
        now = time.time()
        self._req_exc_recent[key] = now
        active_exc = [
            k for k, ts in list(self._req_exc_recent.items())
            if now - ts <= self.global_req_exc_window_sec
        ]
        if len(active_exc) >= self.global_req_exc_events_threshold:
            await self.trigger_global_kill_switch(reason)

    async def book_loop(self) -> None:
        sem = asyncio.Semaphore(self._book_loop_concurrency)

        async def _process(token_id: str) -> None:
            async with sem:
                try:
                    await self.update_and_quote_market(token_id)
                    self._book_req_exc_streak[token_id] = 0
                except Exception as e:
                    em = str(e)
                    log(f"[book-loop] token={token_id} error: {em}")
                    if self._is_req_exc(e):
                        self._log_req_diag("book-loop", e, token_id)
                        self._book_req_exc_streak[token_id] = self._book_req_exc_streak.get(token_id, 0) + 1

                        # event-level hard protect
                        if self._book_req_exc_streak[token_id] >= self.net_degraded_fail_threshold:
                            await self._deactivate_market(token_id, "net_degraded_request_exception")
                            self._book_req_exc_streak[token_id] = 0

                        # global storm hard protect (distinct events in last window)
                        await self._mark_req_exc_and_maybe_storm(token_id, "global_request_exception_storm")
                    else:
                        self._book_req_exc_streak[token_id] = 0

        while self._running:
            token_ids = list(self.market_cfg.keys())
            random.shuffle(token_ids)
            await asyncio.gather(*[_process(tid) for tid in token_ids])
            if self._shared_book_cache is not None:
                # multi-account mode: random cycle interval to stagger accounts
                cycle_sleep = random.uniform(self._multi_cycle_sleep_min, self._multi_cycle_sleep_max)
            else:
                cycle_sleep = max(self.requote_interval_ms / 1000.0, 0.1)
            await asyncio.sleep(cycle_sleep)

    async def _get_anchor_bid_from_gamma(self, token_id: str) -> Optional[Decimal]:
        try:
            cached = self._anchor_cache.get(token_id)
            if cached and (time.time() - cached[1]) < self._anchor_cache_ttl_sec:
                return cached[0]
            r = await asyncio.to_thread(
                requests.get,
                "https://gamma-api.polymarket.com/markets",
                params={"clob_token_ids": token_id, "limit": 2},
                timeout=20,
                proxies=self._read_proxies_for_token(token_id),
            )
            # requests.get positional args above: url, params, timeout
            if r.status_code != 200:
                return None
            arr = r.json()
            if not isinstance(arr, list) or not arr:
                return None
            m = arr[0]
            ids = m.get("clobTokenIds")
            ops = m.get("outcomePrices")
            if isinstance(ids, str):
                ids = json.loads(ids)
            if isinstance(ops, str):
                ops = json.loads(ops)
            if not isinstance(ids, list) or not isinstance(ops, list):
                return None
            if token_id not in [str(x) for x in ids]:
                return None
            idx = [str(x) for x in ids].index(token_id)
            if idx >= len(ops):
                return None
            anchor = Decimal(str(ops[idx]))
            if anchor > 0:
                self._anchor_cache[token_id] = (anchor, time.time())
                return anchor
            return None
        except Exception:
            return None

    async def update_and_quote_market(self, token_id: str) -> None:
        now_ts = time.time()
        if now_ts < self._cooldown_until:
            return

        # after global kill-switch, auto-resume only when recovery gate fully passes
        if self._require_recovery_gate:
            if not self._recovery_ready():
                return
            self._require_recovery_gate = False
            log("[recovery] recovery gate passed, auto-resuming quoting")
            self.send_discord("[ALERT] Recovery conditions satisfied. Auto-resuming quoting.")
        if self._event_is_banned(token_id):
            return
        if time.time() < self._market_skip_until.get(token_id, 0.0):
            return

        blocked, breason = await self._is_blocked_market(token_id)
        if blocked:
            await self._deactivate_market(token_id, breason)
            return

        now = time.time()
        if (now - self.last_quote_ts[token_id]) * 1000 < self.requote_interval_ms:
            return

        # Use shared book cache in multi-account mode to avoid redundant API calls
        if self._shared_book_cache is not None:
            book = self._shared_book_cache.get(token_id)
        else:
            book = None
        if book is None:
            book = await asyncio.to_thread(self.client.get_order_book, token_id)
        if not book or not getattr(book, "bids", None) or not getattr(book, "asks", None):
            return

        best_bid = Decimal(str(book.bids[0].price))
        best_ask = Decimal(str(book.asks[0].price))
        if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            return

        # fallback for placeholder books like 0.001/0.999 -> use gamma outcomePrices anchor
        if best_bid <= Decimal("0.02") and best_ask >= Decimal("0.98"):
            anchor = await self._get_anchor_bid_from_gamma(token_id)
            if anchor is not None and anchor > 0:
                best_bid = anchor
                best_ask = min(Decimal("1"), anchor + Decimal("0.01"))

        await self._resolve_market_tick(token_id, best_bid, best_ask)

        tob = TopOfBook(best_bid=best_bid, best_ask=best_ask)
        self.market_states[token_id] = tob

        # Fetch live market meta first — needed for real rewardsMaxSpread and rewardsMinSize
        meta = await self._get_market_meta(token_id)
        reward_min_size = Decimal(str(meta.get("rewardsMinSize") or 0))

        # Use live incentive spread from API; fall back to config
        live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
        live_spread: Optional[Decimal] = None
        if live_spread_raw is not None:
            try:
                live_spread = Decimal(str(live_spread_raw))
            except Exception:
                live_spread = None

        prices = self._build_price_legs(token_id, tob, live_spread=live_spread)
        market_risk = str(self.market_cfg[token_id].get("risk", "mid")).lower()
        required_min_size = max(self.min_order_size, reward_min_size)

        avail = await self._get_collateral_available()
        if avail is not None:
            self._last_balance = avail

        # event budget: balance_pct mode only
        if avail is not None and avail > 0:
            lo, hi = self.quote_balance_pct_ranges.get(market_risk, (self.quote_balance_pct_min, self.quote_balance_pct_max))
            lo = max(Decimal("0"), min(lo, Decimal("1")))
            hi = max(lo, min(hi, Decimal("1")))
            pct = Decimal(str(random.uniform(float(lo), float(hi))))
            event_budget = avail * pct
            event_budget = min(event_budget, avail * Decimal("0.98"))
        else:
            log(f"[quote-skip] token={token_id} reason=no_balance_available")
            return

        weights = self._alloc_weights(len(prices))
        plan = []
        for p, w in zip(prices, weights):
            front_notional = self._front_bid_notional(book, p)
            if front_notional < self.min_front_bid_notional_usdc:
                log(
                    f"[quote-skip-leg] token={token_id} price={p} "
                    f"reason=front_bid_notional_lt_threshold front={front_notional} threshold={self.min_front_bid_notional_usdc}"
                )
                continue

            leg_notional = event_budget * w
            size = self._floor_to_tick(leg_notional / p, Decimal("0.001")) if p > 0 else Decimal("0")
            notional = p * size
            if size >= required_min_size and size > 0 and notional > 0:
                plan.append((p, size, notional))
            else:
                log(
                    f"[quote-skip-leg] token={token_id} price={p} reason=below_min_size "
                    f"size={size} required={required_min_size} (exchange/reward)"
                )

        if not plan:
            log(f"[quote-skip] token={token_id} reason=empty_plan avail={avail} budget={event_budget}")
            return

        plan_sig = "|".join([f"{p}:{s}" for p, s, _ in plan])
        live = await asyncio.to_thread(self.client.get_orders)
        live_token = [
            o for o in live
            if str(o.get("status", "")).lower() in ("live", "open", "active")
            and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
        ]
        self._market_live_orders[token_id] = live_token
        if self._last_plan_sig.get(token_id) == plan_sig and len(live_token) >= len(plan):
            return

        # reprice: cancel old token orders first
        ids = [o.get("id") or o.get("orderID") for o in live_token if (o.get("id") or o.get("orderID"))]
        if ids:
            await self._action_delay(f"cancel-before-reprice token={token_id}")
            await asyncio.to_thread(self.client.cancel_orders, ids)

        log(
            f"[quote] token={token_id} risk={market_risk} "
            f"legs={len(plan)} budget={event_budget} avail_usdc={avail} "
            f"plan={[ (str(p), str(s)) for p,s,_ in plan ]}"
        )

        try:
            for p, size, _ in plan:
                await self.place_post_only_order(token_id, p, size)
            self._last_plan_sig[token_id] = plan_sig
            self._balance_fail_streak = 0
            self._market_balance_fail_streak[token_id] = 0

            # Safety check: after placing, verify none of our orders became best_bid.
            # The book can shift during the post_delay between price calculation and placement.
            await self._check_not_at_best_bid(token_id)
        except Exception as e:
            em = str(e).lower()
            if "not enough balance" in em or "allowance" in em:
                self._balance_fail_streak += 1
                self._market_balance_fail_streak[token_id] = self._market_balance_fail_streak.get(token_id, 0) + 1
                log(
                    f"[risk] balance/allowance token={token_id} "
                    f"market_streak={self._market_balance_fail_streak[token_id]} global_streak={self._balance_fail_streak} "
                    f"err={e}"
                )

                # isolate to market-level cooldown; do not cancel all events
                if self._market_balance_fail_streak[token_id] >= self.max_balance_fail_streak:
                    self._market_skip_until[token_id] = time.time() + self.cooldown_seconds
                    self._market_balance_fail_streak[token_id] = 0
                    log(f"[risk] market-skip token={token_id} cooldown={self.cooldown_seconds}s")
                return
            raise
        self.last_quote_ts[token_id] = now
        self._quotes_sent += 1

    async def place_post_only_order(self, token_id: str, price: Decimal, size: Decimal) -> None:
        await self._post_delay(f"post token={token_id}")
        args = OrderArgs(token_id=token_id, price=float(price), size=float(size), side=BUY)
        signed = await asyncio.to_thread(self.client.create_order, args)
        # GTC = passive resting order in normal use; post-only behavior is exchange-enforced by price placement.
        await asyncio.to_thread(self.client.post_order, signed, OrderType.GTC)

    def _count_live_orders(self, orders: list[dict]) -> int:
        return sum(1 for o in orders if str(o.get("status", "")).lower() in ("live", "open", "active"))

    def _recovery_ready(self) -> bool:
        now = time.time()
        # no recent poll errors
        if self._poll_err_ts and (now - self._poll_err_ts) < self.recovery_quiet_sec:
            return False
        # ws must be recently healthy
        if not self._last_ws_ok_ts or (now - self._last_ws_ok_ts) > 20:
            return False
        # no recent request-exception storm markers
        if any((now - ts) < self.recovery_quiet_sec for ts in self._req_exc_recent.values()):
            return False
        return True

    async def trigger_global_kill_switch(self, reason: str) -> None:
        deadline = time.time() + self.cancel_retry_window_sec
        canceled_ok = False
        while time.time() < deadline:
            try:
                await asyncio.to_thread(self.client.cancel_all)
                orders = await asyncio.to_thread(self.client.get_orders)
                if self._count_live_orders(orders if isinstance(orders, list) else []) == 0:
                    canceled_ok = True
                    break
            except Exception as e:
                log(f"[kill-switch] cancel_all failed: {e}")
            await asyncio.sleep(max(1, self.cancel_retry_step_sec))

        if not canceled_ok:
            log("[kill-switch] cancel_all retry window ended; live orders may remain")

        self._cooldown_until = time.time() + self.cooldown_seconds
        self._require_recovery_gate = True
        msg = f"[ALERT] PolyLPS-Multi kill-switch: {reason}; cooldown={self.cooldown_seconds}s"
        log(msg)
        self.send_discord(msg)

    async def _ws_user_watch(self) -> None:
        if not self.kill_switch_on_fill:
            while self._running:
                await asyncio.sleep(5)
            return

        urls = ["wss://ws-subscriptions-clob.polymarket.com/ws/user"]
        token_ids = list(self.market_cfg.keys())
        auth = {
            "apiKey": getattr(self.api_creds, "api_key", ""),
            "secret": getattr(self.api_creds, "api_secret", ""),
            "passphrase": getattr(self.api_creds, "api_passphrase", ""),
        }

        def _payloads() -> list[dict]:
            return [
                {"type": "user", "markets": token_ids, "auth": auth},
                {"type": "user", "assets_ids": token_ids, "auth": auth},
                {"type": "subscribe", "channel": "user", "markets": token_ids, "auth": auth},
            ]

        backoff = 1
        ws_down_since = 0.0
        while self._running:
            url = urls[0]
            try:
                async with websockets.connect(url, proxy=WS_PROXY, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                    for p in _payloads():
                        try:
                            await ws.send(json.dumps(p))
                        except Exception:
                            pass
                    log("[fill-ws] connected")
                    self._last_ws_ok_ts = time.time()
                    ws_down_since = 0.0
                    backoff = 1

                    while self._running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        self._last_ws_ok_ts = time.time()
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue

                        payloads = msg if isinstance(msg, list) else [msg]
                        for it in payloads:
                            if not isinstance(it, dict):
                                continue

                            typ = str(it.get("type") or it.get("event_type") or "").lower()
                            status = str(it.get("status") or "").upper()
                            token = str(it.get("asset_id") or it.get("token_id") or it.get("market") or "")
                            if token and token not in self.market_cfg:
                                continue

                            size_matched = Decimal(str(it.get("size_matched", 0) or 0))
                            if isinstance(it.get("maker_orders"), list):
                                for mo in it.get("maker_orders"):
                                    try:
                                        size_matched = max(size_matched, Decimal(str((mo or {}).get("matched_amount", 0) or 0)))
                                    except Exception:
                                        pass

                            if not token:
                                continue

                            hit = False
                            reason = ""
                            if typ in ("trade", "order") and size_matched > self.fill_size_threshold:
                                hit = True
                                reason = f"WS_{typ.upper()}_MATCH:{size_matched}"
                            elif typ in ("trade", "order") and status in ("MATCHED", "FILLED", "PARTIALLY_FILLED", "MINED", "CONFIRMED", "RETRYING"):
                                hit = True
                                reason = f"WS_{typ.upper()}_{status}"

                            if hit:
                                signal_key = str(it.get("id") or it.get("order_id") or f"{token}:{typ}:{status}:{size_matched}")
                                if self._allow_signal(token, signal_key):
                                    self._fills_seen += 1
                                    m_price = Decimal(str(it.get("price", 0) or 0))
                                    if m_price <= 0 and isinstance(it.get("maker_orders"), list) and it.get("maker_orders"):
                                        try:
                                            m_price = Decimal(str((it.get("maker_orders")[0] or {}).get("price", 0) or 0))
                                        except Exception:
                                            m_price = Decimal("0")
                                    await self._trigger_event_offline(token, reason, size_matched, m_price)
            except Exception as e:
                now = time.time()
                if ws_down_since <= 0:
                    ws_down_since = now
                log(f"[fill-ws] err={e}")
                if self._is_req_exc(e):
                    self._log_req_diag("fill-ws", e)

                ws_down = now - ws_down_since
                poll_recent_bad = (now - self._poll_err_ts) <= 15 if self._poll_err_ts > 0 else False
                if ws_down > self.ws_down_trigger_sec and poll_recent_bad:
                    await self.trigger_global_kill_switch("ws_down_and_poll_degraded")

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 20)

    async def _poll_fill_watch(self) -> None:
        while self._running:
            try:
                # open orders delta/matched fallback
                orders = await asyncio.to_thread(self.client.get_orders)
                for o in orders:
                    st = str(o.get("status", "")).lower()
                    if st not in ("live", "open", "active"):
                        continue
                    token = str(o.get("asset_id") or o.get("token_id") or "")
                    if token not in self.market_cfg:
                        continue

                    oid = str(o.get("id") or o.get("orderID") or "")
                    matched = max(
                        Decimal(str(o.get("size_matched", 0) or 0)),
                        Decimal(str(o.get("matched", 0) or 0)),
                        Decimal(str(o.get("filled_size", 0) or 0)),
                    )
                    if matched > self.fill_size_threshold:
                        if self._allow_signal(token, f"poll_matched:{oid}:{matched}"):
                            self._fills_seen += 1
                            o_price = Decimal(str(o.get("price", 0) or 0))
                            await self._trigger_event_offline(token, f"POLL_MATCHED:{matched}", matched, o_price)
                            continue

                    remain = Decimal(str(o.get("remaining_size", o.get("size", 0)) or 0))
                    last = self._last_remaining_by_order.get(oid)
                    self._last_remaining_by_order[oid] = remain
                    if last is not None and remain < (last - self.fill_size_threshold):
                        if self._allow_signal(token, f"poll_remaining_drop:{oid}:{last}->{remain}"):
                            self._fills_seen += 1
                            await self._trigger_event_offline(token, f"POLL_REMAINING_DROP:{last}->{remain}")

                # notifications audit fallback (low weight)
                notes = await asyncio.to_thread(self.client.get_notifications)
                if isinstance(notes, list):
                    for n in notes:
                        token = str(n.get("asset_id") or n.get("token_id") or n.get("market") or "")
                        if token not in self.market_cfg:
                            continue
                        et = str(n.get("eventType", n.get("type", ""))).lower()
                        if "fill" in et or et == "2":
                            nid = str(n.get("id") or f"notif:{token}:{et}")
                            if self._allow_signal(token, nid):
                                self._fills_seen += 1
                                await self._trigger_event_offline(token, f"NOTIF_{et}")

                await asyncio.sleep(5)
            except Exception as e:
                self._poll_err_ts = time.time()
                log(f"[fill-poll] err={e}")
                if self._is_req_exc(e):
                    self._log_req_diag("fill-poll", e)
                await asyncio.sleep(3)

    async def _trade_poll_watch(self) -> None:
        """Hard fallback: poll account trades and trigger offlining/unwind on newly seen fills."""
        seeded = False
        while self._running:
            try:
                data = await asyncio.to_thread(self.client.get_trades)
                items = []
                if isinstance(data, list):
                    items = data
                elif isinstance(data, dict):
                    for k in ("trades", "data", "items"):
                        v = data.get(k)
                        if isinstance(v, list):
                            items = v
                            break

                for t in items:
                    if not isinstance(t, dict):
                        continue
                    token = str(t.get("asset") or t.get("asset_id") or t.get("token_id") or "")
                    if token not in self.market_cfg:
                        continue

                    tid = str(t.get("id") or t.get("trade_id") or t.get("transactionHash") or "")
                    if not tid:
                        tid = f"{token}:{t.get('price')}:{t.get('size')}:{t.get('timestamp')}"

                    # first pass: baseline only, avoid replaying historical fills as new alerts
                    if not seeded:
                        self._seen_trade_ids.add(tid)
                        self._seen_trade_ids_order.append(tid)
                        continue

                    if tid in self._seen_trade_ids:
                        continue
                    self._seen_trade_ids.add(tid)
                    self._seen_trade_ids_order.append(tid)

                    try:
                        sz = Decimal(str(t.get("size") or t.get("matched_amount") or 0))
                    except Exception:
                        sz = Decimal("0")
                    try:
                        px = Decimal(str(t.get("price") or 0))
                    except Exception:
                        px = Decimal("0")

                    if sz > self.fill_size_threshold:
                        if self._allow_signal(token, f"trade_poll:{tid}"):
                            self._fills_seen += 1
                            await self._trigger_event_offline(token, f"TRADES_POLL:{tid}", sz, px)

                if not seeded:
                    seeded = True
                    log("[trade-poll] baseline seeded")

                # keep memory bounded — use insertion-ordered list for correct FIFO truncation
                if len(self._seen_trade_ids_order) > 5000:
                    keep = self._seen_trade_ids_order[-2500:]
                    self._seen_trade_ids = set(keep)
                    self._seen_trade_ids_order = keep

                # healthy iteration resets req-exception streak
                self._trade_poll_req_exc_streak = 0
                await asyncio.sleep(2)
            except Exception as e:
                log(f"[trade-poll] err={e}")
                if self._is_req_exc(e):
                    self._log_req_diag("trade-poll", e)
                    self._trade_poll_req_exc_streak += 1
                    if self._trade_poll_req_exc_streak >= self.req_exc_confirm_trade_poll:
                        await self._mark_req_exc_and_maybe_storm("trade-poll", "global_request_exception_storm")
                else:
                    self._trade_poll_req_exc_streak = 0
                await asyncio.sleep(3)

    async def fill_watch_loop(self) -> None:
        await asyncio.gather(self._ws_user_watch(), self._poll_fill_watch(), self._trade_poll_watch())

    async def summary_loop(self) -> None:
        while self._running:
            await asyncio.sleep(3600)
            msg = (
                "📊 PolyLPS-Multi hourly summary\n"
                f"markets={len(self.market_cfg)}\n"
                f"quotes_sent={self._quotes_sent}\n"
                f"fills_seen={self._fills_seen}\n"
                f"cooldown_active={time.time() < self._cooldown_until}"
            )
            self.send_discord(msg)

    async def unwind_tracking_loop(self) -> None:
        """Periodically check pending unwind SELL orders.
        - If the order is no longer in live orders → assume filled, remove.
        - If age > unwind_max_age_sec and still open → cancel and escalate (re-post at lower price).
        """
        while self._running:
            await asyncio.sleep(self._unwind_check_interval_sec)
            if not self._pending_unwinds:
                continue
            try:
                orders = await asyncio.to_thread(self.client.get_orders)
                live_ids = {
                    str(o.get("id") or o.get("orderID") or "")
                    for o in orders
                    if str(o.get("status", "")).lower() in ("live", "open", "active")
                }
                now = time.time()
                still_pending = []
                for uw in self._pending_unwinds:
                    oid = str(uw.get("order_id") or "")
                    token_id = str(uw.get("token_id") or "")
                    fill_price = Decimal(str(uw.get("fill_price") or 0))
                    fill_size = Decimal(str(uw.get("fill_size") or 0))
                    placed_at = float(uw.get("placed_at") or 0)
                    age = now - placed_at

                    if oid and oid not in live_ids:
                        # Order no longer open — filled or externally cancelled; consider done
                        log(f"[unwind] completed token={token_id} order_id={oid} age={age:.0f}s")
                        continue

                    if age > self._unwind_max_age_sec:
                        # Timed out — notify via Discord for manual review, keep order alive
                        hours = age / 3600
                        msg = (
                            f"[UNWIND ALERT] Unwind order not filled after {hours:.1f}h\n"
                            f"token={token_id}\n"
                            f"fill_price={fill_price} size={fill_size} notional={float(fill_price * fill_size):.2f}\n"
                            f"order_id={oid}\n"
                            f"reason={uw.get('reason', '')}\n"
                            f"Action required: check market and decide manually."
                        )
                        log(f"[unwind] timeout alert token={token_id} age={hours:.1f}h order_id={oid}")
                        self.send_discord(msg)
                        # Keep in pending list — alert will repeat next check cycle until resolved
                        still_pending.append(uw)
                    else:
                        still_pending.append(uw)

                self._pending_unwinds = still_pending
            except Exception as e:
                log(f"[unwind] tracking loop error: {e}")

    async def state_write_loop(self) -> None:
        """Periodically write engine state to data/engine_state.json for dashboard consumption."""
        while self._running:
            try:
                now = time.time()
                markets_out: dict = {}
                for tid, mcfg in self.market_cfg.items():
                    tob = self.market_states.get(tid)
                    spread = mcfg.get("spread")
                    mid = float(tob.mid) if tob else None
                    best_bid = float(tob.best_bid) if tob else None
                    best_ask = float(tob.best_ask) if tob else None
                    reward_lower = reward_upper = None
                    if tob and spread is not None:
                        s = Decimal(str(spread))
                        if s > Decimal("1"):
                            s = s / Decimal("100")
                        tick = mcfg["tick"]
                        rl = max(tick, tob.mid - s)
                        ru = tob.best_bid - tick
                        reward_lower = float(rl)
                        reward_upper = float(ru)

                    live_orders = self._market_live_orders.get(tid, [])
                    orders_out = [
                        {
                            "id": str(o.get("id") or o.get("orderID") or ""),
                            "price": float(str(o.get("price", 0) or 0)),
                            "size": float(str(o.get("size", 0) or o.get("original_size", 0) or 0)),
                        }
                        for o in live_orders
                    ]

                    banned = self._event_is_banned(tid)
                    skipped = now < self._market_skip_until.get(tid, 0.0)
                    if banned:
                        status = "banned"
                    elif skipped:
                        status = "skipped"
                    elif tob:
                        status = "active"
                    else:
                        status = "waiting"

                    markets_out[tid] = {
                        "mid": mid,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "reward_lower": reward_lower,
                        "reward_upper": reward_upper,
                        "orders": orders_out,
                        "last_quote_ts": self.last_quote_ts.get(tid),
                        "status": status,
                    }

                state = {
                    "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "balance": float(self._last_balance) if self._last_balance is not None else None,
                    "quotes_sent": self._quotes_sent,
                    "fills_seen": self._fills_seen,
                    "cooldown_active": now < self._cooldown_until,
                    "markets": markets_out,
                    "fills": list(self._fills_record[-100:]),
                    "pending_unwinds": list(self._pending_unwinds),
                    "banned_tokens": [
                        tid for tid in self.market_cfg if self._event_is_banned(tid)
                    ],
                }

                tmp = self._state_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
                tmp.replace(self._state_path)
            except Exception as e:
                log(f"[state-writer] error: {e}")
            await asyncio.sleep(self._state_write_interval_sec)

    def send_discord(self, message: str) -> None:
        if not self.discord_webhook:
            return
        try:
            requests.post(self.discord_webhook, json={"content": message}, timeout=8)
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(PolyLPSMulti(config_path="config.json").run())
