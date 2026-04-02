import asyncio
import json
import os
import random
import time
import urllib.request
from datetime import datetime
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

import requests
import websockets
from py_clob_client.client import ClobClient
from scanner import normalize_market


HTTP_PROXIES = None       # read operations: book queries, gamma API, meta 闂?routed through proxy pool
HTTP_PROXIES_WRITE = None  # write operations: cancel, place order 闂?always direct (None)
WS_PROXY = None
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams, OrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL
from remote_signer import AddressStub, BuilderStub, RemoteSignerClient


@dataclass
class TopOfBook:
    best_bid: Decimal
    best_ask: Decimal

    @property
    def mid(self) -> Decimal:
        return (self.best_bid + self.best_ask) / Decimal("2")


@dataclass
class MarketSnapshot:
    best_bid: Decimal = Decimal("0")
    best_ask: Decimal = Decimal("0")
    bids: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    asks: list[tuple[Decimal, Decimal]] = field(default_factory=list)
    last_update_ts: float = 0.0
    last_book_ts_ms: int = 0
    source: str = "rest"


EVENT_ACTIVE = "ACTIVE"
EVENT_DEFENSIVE = "DEFENSIVE"
EVENT_CANCELING = "CANCELING"
EVENT_HALTED_ON_FILL = "HALTED_ON_FILL"
EVENT_HALTED_ON_DATA = "HALTED_ON_DATA"
EVENT_COOLDOWN = "COOLDOWN"
EVENT_STARTED_BLOCKED = "START_BLOCKED"
EVENT_WATCH = "WATCH"
EVENT_QUARANTINE = "QUARANTINE"
EVENT_EXIT_PENDING = "EXIT_PENDING"
EVENT_PENDING_MANUAL_EXIT = "PENDING_MANUAL_EXIT"


class EventHaltPreempted(RuntimeError):
    pass


class SoftQuoteSkip(RuntimeError):
    pass


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        safe = line.encode("gbk", errors="ignore").decode("gbk", errors="ignore")
        print(safe, flush=True)


def _format_exc(exc: Exception, limit: int = 220) -> str:
    msg = str(exc or "").strip()
    if not msg:
        return exc.__class__.__name__
    compact = " ".join(msg.split())
    lower = compact.lower()
    if "cloudflare" in lower and "502" in lower:
        return "Cloudflare 502 Bad Gateway"
    if len(compact) > limit:
        return compact[: limit - 3] + "..."
    return compact


def _contains_any_ci(text: str, needles: list[str]) -> bool:
    hay = str(text or "").lower()
    return any(str(n or "").strip().lower() in hay for n in needles if str(n or "").strip())


def _ws_proxy_diag() -> str:
    sys_proxies = urllib.request.getproxies() or {}
    sys_proxy = (
        sys_proxies.get("https")
        or sys_proxies.get("http")
        or sys_proxies.get("all")
        or sys_proxies.get("ftp")
    )
    effective = WS_PROXY if WS_PROXY else "direct"
    forced_direct = WS_PROXY is None
    detected = sys_proxy or "none"
    return f"system_proxy={detected} effective_ws_proxy={effective} ws_direct_forced={forced_direct}"


def _choose_proxy(cfg: dict, for_ws: bool, shard_key: str = "") -> str | None:
    """Select a proxy from the pool.

    for_ws=True  闂?WS connections (long-lived)
    for_ws=False 闂?HTTP read operations (book queries, gamma API)

    Write operations (cancel, place) always use None (direct) 闂?callers must NOT
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

    # Stable per-token hash assignment 闂?different markets use different proxies
    if shard_key:
        idx = abs(hash(shard_key)) % len(weighted)
    else:
        idx = int(time.time() // 300) % len(weighted)
    return str(weighted[idx].get("url", "")).strip() or None


def _init_proxy_settings(cfg: dict):
    """Initialise global proxy references.

    HTTP_PROXIES       闂?read operations (book queries, gamma API calls)
    HTTP_PROXIES_WRITE 闂?write operations (cancel, place order) 闂?always None/direct
    WS_PROXY           闂?WebSocket connections
    """
    global HTTP_PROXIES, HTTP_PROXIES_WRITE, WS_PROXY
    read_proxy = _choose_proxy(cfg, for_ws=False)
    ws_proxy = _choose_proxy(cfg, for_ws=True)

    WS_PROXY = ws_proxy
    HTTP_PROXIES_WRITE = None  # writes always go direct 闂?never proxy

    if read_proxy:
        HTTP_PROXIES = {"http": read_proxy, "https": read_proxy}
    else:
        # Fall back to system proxy env vars when proxy_pool is disabled
        sys_proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""
        if sys_proxy:
            HTTP_PROXIES = {"http": sys_proxy, "https": sys_proxy}
        else:
            HTTP_PROXIES = None

    # Patch py_clob_client's httpx client to use the resolved proxy
    # Patch py_clob_client's httpx client:
    # - Use http2=False to avoid WinError 10035 with asyncio.to_thread on Windows
    # - Apply proxy if available
    _final_proxy = read_proxy or (os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or "")
    try:
        import httpx
        from py_clob_client.http_helpers import helpers as _hh
        kwargs = {"http2": False}
        if _final_proxy:
            kwargs["proxy"] = _final_proxy
        _hh._http_client = httpx.Client(**kwargs)
    except Exception:
        pass


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
        self._config_path = cfg_path.resolve()
        self.cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        _init_proxy_settings(self.cfg)

        account = self.cfg.get("account", {})
        host = self.cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
        chain_id = int(account.get("chain_id", 137))
        signature_type = int(account.get("signature_type", 0))
        self.signature_type = signature_type
        funder = str(account.get("funder", "")).strip()

        signer_server_url = os.getenv("POLY_SIGNER_SERVER_URL", "").strip() or str(account.get("signer_server_url", "")).strip()
        self.remote_signer: RemoteSignerClient | None = None

        if signer_server_url:
            # --- Remote signer mode: private key lives on Mac Mini ---
            log(f">>> REMOTE SIGNER MODE: using Mac Mini at {signer_server_url} (private key is NOT on this machine)")
            signer_token = (
                os.getenv("SIGNER_TOKEN", "").strip()
                or str(account.get("signer_token", "")).strip()
            )
            self.remote_signer = RemoteSignerClient(signer_server_url, signer_token)
            creds_data = self.remote_signer.derive_creds()
            address = creds_data["address"]
            log(f">>> Remote signer connected, address: {address}")

            # Create ClobClient without private key (L0 read-only + L2 via api_creds)
            self.client = ClobClient(host=host, chain_id=chain_id)
            # Inject AddressStub so client.signer.address() works for HMAC headers
            self.client.signer = AddressStub(address, chain_id)
            # Inject a stub builder so code that accesses self.client.builder won't crash
            self.client.builder = BuilderStub(sig_type=signature_type, funder=funder)

            from py_clob_client.clob_types import ApiCreds
            self.api_creds = ApiCreds(
                api_key=creds_data["api_key"],
                api_secret=creds_data["api_secret"],
                api_passphrase=creds_data["api_passphrase"],
            )
            self.client.set_api_creds(self.api_creds)
        else:
            # --- Local signer mode: private key must come from environment ---
            env_key = os.getenv("POLY_PRIVATE_KEY", "").strip()
            private_key = env_key
            if not private_key or "REPLACE" in private_key or "REDACTED" in private_key:
                raise ValueError("Private key missing. Set POLY_PRIVATE_KEY env var.")

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
            # some py_clob_client builds may not expose .builder in local mode
            # but downstream polling paths still reference it indirectly.
            if not hasattr(self.client, "builder"):
                self.client.builder = BuilderStub(sig_type=signature_type, funder=funder)

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
        self.start_freeze_seconds = int(risk.get("start_freeze_seconds", 120))

        reporting = self.cfg.get("reporting", {})
        self.discord_webhook = os.getenv("POLY_DISCORD_WEBHOOK", "").strip() or str(reporting.get("discord_webhook", "")).strip()
        self.fill_discord_webhook = os.getenv("POLY_FILL_DISCORD_WEBHOOK", "").strip() or str(reporting.get("fill_discord_webhook", "")).strip()
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
                "session": str(m.get("session", "both")).lower(),
                "paired_token_id": str(m.get("paired_token_id", "")),
            }

        if not self.market_cfg:
            raise ValueError("No valid enabled markets in config.markets")

        self.market_states: Dict[str, TopOfBook] = {}
        self._market_snapshots: Dict[str, MarketSnapshot] = {}
        self._market_depth_snapshots: Dict[str, MarketSnapshot] = {}
        self._event_states: Dict[str, Dict[str, Any]] = {
            tid: {"state": EVENT_ACTIVE, "reason": "init", "updated_at": time.time()}
            for tid in self.market_cfg
        }
        self._event_locks: Dict[str, asyncio.Lock] = {tid: asyncio.Lock() for tid in self.market_cfg}
        self._latency_marks: Dict[str, Dict[str, float]] = {tid: {} for tid in self.market_cfg}
        self._latency_records: list[dict] = []
        self._halt_requested: Dict[str, Optional[str]] = {tid: None for tid in self.market_cfg}
        self._top_leg_defense_tasks: Dict[str, asyncio.Task] = {}
        self._gate_decisions: Dict[str, Dict[str, Any]] = {}
        self._last_top_plan_sig: Dict[str, str] = {}
        self._last_back_plan_sig: Dict[str, str] = {}
        self.last_quote_ts: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}
        self._market_budget_pct: Dict[str, Decimal] = {}
        self._size_requote_tolerance_pct = Decimal(str(strategy.get("size_requote_tolerance_pct", 0.05)))
        self._max_reward_levels = int(strategy.get("max_reward_levels", 12))
        self._level_weight_decay = Decimal(str(strategy.get("level_weight_decay", "0.82")))
        self._level_distance_penalty = Decimal(str(strategy.get("level_distance_penalty", "0.08")))
        self._level_depth_bonus_cap = Decimal(str(strategy.get("level_depth_bonus_cap", "0.25")))
        self._level_depth_bonus_scale = Decimal(str(strategy.get("level_depth_bonus_scale", "0.10")))
        self._level_bba_penalty = Decimal(str(strategy.get("level_bba_penalty", "0.12")))
        self._level_defense_storm_penalty = Decimal(str(strategy.get("level_defense_storm_penalty", "0.18")))
        self._repeat_defense_ban_count = int(strategy.get("repeat_defense_ban_count", 3))
        self._tick_resolved: set[str] = set()

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Dual-side (low-price) quoting config 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?
        # When enabled, the engine auto-registers the paired (NO) token for
        # markets where YES mid <= max_mid.  Both tokens are then quoted as
        # independent BUY-side markets 闂?no SELL orders needed.
        dual = strategy.get("dual_side", {})
        self._dual_side_enabled = bool(dual.get("enabled", False))
        self._dual_side_max_mid = Decimal(str(dual.get("max_mid", "0.10")))
        self._dual_side_no_risk = str(dual.get("no_side_risk", "low")).lower()
        self._dual_side_min_book_depth = Decimal(str(dual.get("min_book_depth_usdc", "10000")))
        self._dual_side_injected: set[str] = set()  # NO tokens auto-added

        # execution pacing: risk actions immediate, normal posting lightly paced
        execution = self.cfg.get("execution", {})

        self._cooldown_until = 0.0
        self._running = True
        self._kill_switch_lock = asyncio.Lock()
        self._fills_seen = 0
        self._quotes_sent = 0
        self._balance_fail_streak = 0
        self._balance_cache_ttl_sec = float(execution.get("balance_cache_ttl_sec", 3.0))
        self._balance_cache: tuple[Optional[Decimal], float] = (None, 0.0)
        self.max_balance_fail_streak = int(risk.get("max_balance_fail_streak", 8))
        # {token_id: (anchor_value, timestamp)} 闂?TTL-based
        self._anchor_cache: Dict[str, tuple] = {}

        # per-market failure isolation (do not nuke all events on single-market balance issues)
        self._market_balance_fail_streak: Dict[str, int] = {tid: 0 for tid in self.market_cfg}
        self._market_skip_until: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}
        self._market_stale_fail_streak: Dict[str, int] = {tid: 0 for tid in self.market_cfg}
        self._market_budget_skip_until: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}
        self._paired_event_budget_reserve: Dict[str, Decimal] = {}
        self.stale_skip_threshold = int(execution.get("stale_skip_threshold", 2))
        self.stale_skip_cooldown_sec = float(execution.get("stale_skip_cooldown_sec", 20))
        self.budget_skip_cooldown_sec = float(execution.get("budget_skip_cooldown_sec", 45))

        self.post_delay_min_sec = float(execution.get("post_delay_min_sec", 1))
        self.post_delay_max_sec = float(execution.get("post_delay_max_sec", 3))
        self.signer_max_concurrency = max(1, int(execution.get("signer_max_concurrency", 1)))
        self.signer_requote_gap_sec = float(execution.get("signer_requote_gap_sec", 1.2))
        self._signer_sem = asyncio.Semaphore(self.signer_max_concurrency)
        self._signer_gap_lock = asyncio.Lock()
        self._last_signer_post_ts = 0.0

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Global + per-token order throttle 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕?
        self._global_order_lock = asyncio.Lock()
        self._global_last_order_ts = 0.0
        self._global_order_min_sec = float(execution.get("global_order_min_sec", 10))
        self._global_order_max_sec = float(execution.get("global_order_max_sec", 30))
        self._per_token_order_min_sec = float(execution.get("per_token_order_min_sec", 15))
        self._per_token_last_order_ts: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}

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
        self._last_market_ws_ok_ts: float = 0.0
        self._market_ws_down_cancel_sec: float = float(execution.get("market_ws_down_cancel_sec", 30))
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
            "democratic-presidential-nominee-2028",
        ]
        self._token_slug_cache: Dict[str, str] = {}
        # {token_id: (meta_dict, timestamp)} 闂?TTL prevents stale reward/spread data
        self._market_meta_cache: Dict[str, tuple] = {}
        self._meta_cache_ttl_sec: int = int(execution.get("meta_cache_ttl_sec", 300))
        # {token_id: anchor_ttl_sec}
        self._anchor_cache_ttl_sec: int = int(execution.get("anchor_cache_ttl_sec", 120))
        # YES/NO paired token map: {token_id: paired_token_id}
        self._paired_token_cache: Dict[str, str] = {}
        self._market_condition_ids: Dict[str, str] = {}

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
        # ordered insertion list 闂?used for correct FIFO truncation (set is unordered)
        self._seen_trade_ids_order: list[str] = []
        # pending unwind SELL orders: [{token_id, fill_price, fill_size, order_id, placed_at}]
        self._pending_unwinds: list[dict] = []
        self._unwind_check_interval_sec: int = int(execution.get("unwind_check_interval_sec", 300))
        self._unwind_max_age_sec: int = int(execution.get("unwind_max_age_sec", 14400))

        # --- P0: watch/quarantine volatility tracker ---
        volatility_cfg = self.cfg.get("volatility", {})
        self._vol_front_notional_drop_pct: float = float(volatility_cfg.get("front_notional_drop_pct", 0.30))
        self._vol_bba_jump_ticks: int = int(volatility_cfg.get("bba_jump_ticks", 2))
        self._vol_defense_action_window_sec: float = float(volatility_cfg.get("defense_action_window_sec", 60))
        self._vol_defense_action_threshold: int = int(volatility_cfg.get("defense_action_threshold", 2))
        self._vol_watch_duration_sec: float = float(volatility_cfg.get("watch_duration_sec", 120))
        self._vol_quarantine_duration_sec: float = float(volatility_cfg.get("quarantine_duration_sec", 300))
        # per-token rolling window: {token_id: {"front_notional_history": [(ts, val)], "defense_actions": [(ts, action)], "watch_enter_ts": float, "quarantine_enter_ts": float, "watch_count": int}}
        self._volatility_tracker: Dict[str, Dict[str, Any]] = {
            tid: {"front_notional_history": [], "defense_actions": [], "bba_prev": None, "watch_count": 0}
            for tid in self.market_cfg
        }

        # --- automatic Clash proxy failover ---
        pf_cfg = self.cfg.get("proxy_failover", {})
        self._proxy_failover_enabled: bool = bool(pf_cfg.get("enabled", False))
        self._proxy_failover_controller_url: str = str(pf_cfg.get("controller_url", "http://127.0.0.1:9097")).rstrip("/")
        self._proxy_failover_group_name: str = str(pf_cfg.get("group_name", "Proxy")).strip() or "Proxy"
        self._proxy_failover_whitelist_keywords: list[str] = [
            str(x).strip() for x in (pf_cfg.get("allowed_keywords") or []) if str(x).strip()
        ]
        self._proxy_failover_observe_sec: float = float(pf_cfg.get("observe_sec", 120))
        self._proxy_failover_bad_node_ttl_sec: float = float(pf_cfg.get("bad_node_ttl_sec", 600))
        self._proxy_failover_max_switches_per_window: int = int(pf_cfg.get("max_switches_per_window", 3))
        self._proxy_failover_switch_window_sec: float = float(pf_cfg.get("switch_window_sec", 600))
        self._proxy_failover_min_switch_gap_sec: float = float(pf_cfg.get("min_switch_gap_sec", 60))
        self._proxy_failover_request_exception_threshold: int = int(pf_cfg.get("request_exception_count", 5))
        self._proxy_failover_req_exc_window_sec: float = float(pf_cfg.get("req_exc_window_sec", 120))
        self._proxy_failover_ws_handshake_fail_threshold: int = int(pf_cfg.get("ws_handshake_fail_count", 3))
        self._proxy_failover_ws_down_trigger_sec: float = float(pf_cfg.get("ws_down_trigger_sec", 30))
        self._proxy_failover_lock = asyncio.Lock()
        self._proxy_failover_observe_until: float = 0.0
        self._proxy_failover_switch_history: list[float] = []
        self._proxy_failover_node_bad_until: Dict[str, float] = {}
        self._proxy_failover_last_switch_from: str = ""
        self._proxy_failover_last_switch_to: str = ""
        self._proxy_failover_last_switch_reason: str = ""
        self._proxy_failover_last_switch_ts: float = 0.0
        self._proxy_failover_req_exc_count: int = 0
        self._proxy_failover_req_exc_recent: list[float] = []
        self._proxy_failover_ws_handshake_fail_count: int = 0
        self._proxy_failover_halt_until: float = 0.0

        # --- P1: fill闂備礁鎲￠懝楣冩煀閿濆拋鐎堕柣鎴烆焽椤╅鈧懓瀚妯肩矆瀹€鍕厱?---
        exit_cfg = self.cfg.get("exit_strategy", {})
        self._exit_delay_sec: float = float(exit_cfg.get("exit_delay_sec", 5))
        self._exit_timeout_sec: float = float(exit_cfg.get("exit_timeout_sec", 300))
        self._exit_retry_count: int = int(exit_cfg.get("retry_count", 2))

        # --- P2: 闂備礁鎼崯銊╁磿閸愯?濠电姰鍨奸崺鏍垝鎼粹埗?session mode (redesigned: day=scan markets, night=night_markets) ---
        session_cfg = self.cfg.get("session", {})
        self._session_enabled: bool = bool(session_cfg.get("enabled", False))
        self._session_night_start: str = str(session_cfg.get("night_start", "00:00"))
        self._session_night_end: str = str(session_cfg.get("night_end", "06:00"))
        self._session_tz: str = str(session_cfg.get("tz", "Asia/Shanghai"))
        self._session_switch_gap_sec: float = float(session_cfg.get("switch_gap_sec", 5))
        self._last_session: str = "unknown"  # track session transitions

        # night_markets config: separate market list for night session
        self._night_market_cfg: Dict[str, Dict[str, Any]] = {}
        for m in self.cfg.get("night_markets", []):
            if not m.get("enabled", True):
                continue
            token_id = str(m.get("token_id", ""))
            if not token_id.isdigit():
                continue
            self._night_market_cfg[token_id] = {
                "spread": Decimal(str(m.get("max_incentive_spread", 0.02))),
                "tick": Decimal(str(m.get("price_tick", self.default_tick))),
                "min_distance": Decimal(str(m.get("min_distance_from_best_bid", self.default_min_distance))),
                "risk": str(m.get("risk", "mid")).lower(),
            }

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
        self._market_ws_backoff_cap_sec: int = int(execution.get("market_ws_backoff_cap_sec", 20))
        self._market_snapshot_stale_sec: float = float(execution.get("market_snapshot_stale_sec", 5.0))
        self._ws_recv_idle_timeout_sec: float = float(execution.get("ws_recv_idle_timeout_sec", 90.0))
        self._ws_pong_timeout_sec: float = float(execution.get("ws_pong_timeout_sec", 10.0))
        self._gate_send_accept_budget_ms: float = float(execution.get("gate_send_accept_budget_ms", 2500))
        self._gate_halt_clear_budget_ms: float = float(execution.get("gate_halt_clear_budget_ms", 5000))

        # multi-account shared book cache (set by multi_runner; None in single-account mode)
        self._shared_book_cache: Optional[Any] = None
        # multi-account cycle sleep (random interval between full book-loop cycles)
        self._multi_cycle_sleep_min: float = float(execution.get("multi_cycle_sleep_min_sec", 3.0))
        self._multi_cycle_sleep_max: float = float(execution.get("multi_cycle_sleep_max_sec", 20.0))

    @staticmethod
    def _floor_to_tick(px: Decimal, tick: Decimal) -> Decimal:
        return (px / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    def _event_state_entry(self, token_id: str) -> Dict[str, Any]:
        return self._event_states.setdefault(
            token_id,
            {"state": EVENT_ACTIVE, "reason": "init", "updated_at": time.time()},
        )

    def _event_state_name(self, token_id: str) -> str:
        return str(self._event_state_entry(token_id).get("state") or EVENT_ACTIVE)

    def _event_blocks_quote(self, token_id: str) -> bool:
        return self._event_state_name(token_id) in {
            EVENT_CANCELING,
            EVENT_HALTED_ON_FILL,
            EVENT_HALTED_ON_DATA,
            EVENT_COOLDOWN,
            EVENT_STARTED_BLOCKED,
            EVENT_WATCH,
            EVENT_QUARANTINE,
            EVENT_EXIT_PENDING,
            EVENT_PENDING_MANUAL_EXIT,
        }

    def _latency_flow_reset(self, token_id: str, preserve: Optional[set[str]] = None) -> None:
        marks = self._latency_marks.setdefault(token_id, {})
        if not marks:
            return
        keep = preserve or set()
        self._latency_marks[token_id] = {k: v for k, v in marks.items() if k in keep}

    def _arm_halt_preemption(self, token_id: str, reason: str) -> None:
        self._halt_requested[token_id] = reason
        task = self._top_leg_defense_tasks.get(token_id)
        if task and not task.done():
            task.cancel()

    def _clear_halt_preemption(self, token_id: str) -> None:
        self._halt_requested[token_id] = None

    def _halt_preemption_reason(self, token_id: str) -> Optional[str]:
        reason = self._halt_requested.get(token_id)
        if reason:
            return reason
        state = self._event_state_name(token_id)
        if state in {EVENT_CANCELING, EVENT_HALTED_ON_FILL, EVENT_HALTED_ON_DATA, EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT}:
            return f"event_state={state}"
        return None

    def _ensure_order_path_open(self, token_id: str, label: str) -> None:
        reason = self._halt_preemption_reason(token_id)
        if reason:
            raise EventHaltPreempted(f"{label}:{reason}")

    def _set_event_state(self, token_id: str, state: str, reason: str) -> None:
        entry = self._event_state_entry(token_id)
        prev = str(entry.get("state") or EVENT_ACTIVE)
        if prev == state and str(entry.get("reason") or "") == reason:
            return
        entry.update({"state": state, "reason": reason, "updated_at": time.time()})
        log(f"[event-state] token={token_id} {prev}->{state} reason={reason}")

    def _is_sports_market(self, meta: Optional[Dict[str, Any]] = None) -> bool:
        info = meta or {}
        text_parts = [
            str(info.get("slug") or ""),
            str(info.get("question") or ""),
            str(info.get("title") or ""),
            str(info.get("category") or ""),
            str(info.get("groupItemTitle") or ""),
            str(info.get("seriesSlug") or ""),
        ]
        hay = " ".join(text_parts).lower()
        sports_markers = [
            "sports", "nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball",
            "baseball", "tennis", "golf", "ufc", "mma", "match", "game", "vs ", " vs ",
            "champions league", "premier league", "la liga", "serie a", "bundesliga", "world cup",
            "super bowl", "playoffs", "final", "semi-final", "semifinal", "quarterfinal",
        ]
        return any(marker in hay for marker in sports_markers)

    def _market_start_guard_status(
        self,
        token_id: str,
        meta: Optional[Dict[str, Any]] = None,
        now_ts: Optional[float] = None,
    ) -> tuple[bool, str, Optional[float]]:
        info = meta or {}
        start_ts_raw = info.get("gameStartTs")
        start_ts: Optional[float] = None
        try:
            if start_ts_raw is not None:
                start_ts = float(start_ts_raw)
        except Exception:
            start_ts = None
        if not self._is_sports_market(info):
            return False, "", start_ts
        now = now_ts if now_ts is not None else time.time()
        if start_ts is not None:
            freeze_at = start_ts - max(self.start_freeze_seconds, 0)
            if now >= freeze_at:
                if now >= start_ts:
                    return True, "market_started", start_ts
                return True, f"market_near_start:{self.start_freeze_seconds}s", start_ts
        if bool(info.get("isInPlay")):
            return True, "market_in_play", start_ts
        return False, "", start_ts

    async def _enforce_start_guard(
        self,
        token_id: str,
        meta: Optional[Dict[str, Any]] = None,
        trigger: str = "quote_loop",
    ) -> bool:
        blocked, reason, start_ts = self._market_start_guard_status(token_id, meta=meta)
        if not blocked:
            return False
        reason_parts = [reason]
        if start_ts is not None:
            reason_parts.append(f"game_start_ts={int(start_ts)}")
        reason_parts.append(f"trigger={trigger}")
        final_reason = "|".join(reason_parts)
        self._set_event_state(token_id, EVENT_STARTED_BLOCKED, final_reason)
        live_orders = await self._get_live_orders_fast(token_id)
        if live_orders:
            await self._cancel_order_ids(
                token_id,
                [self._order_id(o) for o in live_orders],
                f"start_guard:{trigger}",
            )
            self._market_live_orders[token_id] = await self._get_live_orders_fast(token_id)
        self.last_quote_ts[token_id] = 0.0
        self._last_plan_sig[token_id] = ""
        self._last_top_plan_sig[token_id] = ""
        self._last_back_plan_sig[token_id] = ""
        return True

    def _mark_latency(self, token_id: str, key: str, ts: Optional[float] = None) -> float:
        when = ts if ts is not None else time.time()
        self._latency_marks.setdefault(token_id, {})[key] = when
        return when

    def _emit_latency_record(self, token_id: str, label: str, extra: Optional[Dict[str, Any]] = None) -> None:
        marks = dict(self._latency_marks.get(token_id, {}))
        if not marks:
            return

        def delta(start: str, end: str) -> Optional[float]:
            if start in marks and end in marks:
                diff = marks[end] - marks[start]
                if diff < 0:
                    return None
                return round(diff * 1000.0, 1)
            return None

        record = {
            "token_id": token_id,
            "label": label,
            "ts": time.time(),
            "detect_to_decision_ms": delta("t_detect", "t_decision"),
            "decision_to_sign_start_ms": delta("t_decision", "t_sign_start"),
            "sign_ms": delta("t_sign_start", "t_sign_done"),
            "send_to_accept_ms": delta("t_send", "t_exchange_accept"),
            "fill_to_halt_ms": delta("t_fill_seen", "t_halt_entered"),
            "halt_to_cleared_ms": delta("t_halt_entered", "t_orders_cleared"),
        }
        if extra:
            record.update(extra)
        self._latency_records.append(record)
        if len(self._latency_records) > 200:
            self._latency_records = self._latency_records[-100:]
        compact = {k: v for k, v in record.items() if k.endswith("_ms") and v is not None}
        log(f"[latency] token={token_id} label={label} metrics={compact}")

    @staticmethod
    def _coerce_levels(levels: Any) -> list[tuple[Decimal, Decimal]]:
        out: list[tuple[Decimal, Decimal]] = []
        for level in levels or []:
            try:
                if isinstance(level, dict):
                    px = Decimal(str(level.get("price", 0) or 0))
                    sz = Decimal(str(level.get("size", 0) or 0))
                else:
                    px = Decimal(str(getattr(level, "price", 0) or 0))
                    sz = Decimal(str(getattr(level, "size", 0) or 0))
                if px > 0 and sz > 0:
                    out.append((px, sz))
            except Exception:
                continue
        return out

    @staticmethod
    def _sort_book_levels(
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]]:
        return (
            sorted(bids, key=lambda x: x[0], reverse=True),
            sorted(asks, key=lambda x: x[0]),
        )

    @classmethod
    def _best_prices_from_levels(
        cls,
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
    ) -> tuple[Decimal, Decimal]:
        bids_sorted, asks_sorted = cls._sort_book_levels(bids, asks)
        best_bid = bids_sorted[0][0] if bids_sorted else Decimal("0")
        best_ask = asks_sorted[0][0] if asks_sorted else Decimal("0")
        return best_bid, best_ask

    @staticmethod
    def _snapshot_values_valid(best_bid: Decimal, best_ask: Decimal) -> bool:
        return best_bid > 0 and best_ask > 0 and best_ask >= best_bid

    @staticmethod
    def _snapshot_is_placeholder(best_bid: Decimal, best_ask: Decimal) -> bool:
        return best_bid <= Decimal("0.02") and best_ask >= Decimal("0.98")

    def _fresh_valid_snapshot(self, token_id: str) -> Optional[MarketSnapshot]:
        snap = self._market_snapshots.get(token_id)
        if not snap:
            return None
        if self._snapshot_is_stale(token_id, snap):
            return None
        if not self._snapshot_values_valid(snap.best_bid, snap.best_ask):
            return None
        return snap

    def _fresh_depth_snapshot(self, token_id: str) -> Optional[MarketSnapshot]:
        snap = self._market_depth_snapshots.get(token_id)
        if not snap:
            return None
        if self._snapshot_is_stale(token_id, snap):
            return None
        if not self._snapshot_values_valid(snap.best_bid, snap.best_ask):
            return None
        if self._snapshot_is_placeholder(snap.best_bid, snap.best_ask):
            return None
        if not snap.bids or not snap.asks:
            return None
        return snap

    async def _recv_ws_message(self, ws: Any, scope: str) -> Any:
        idle_timeout = self._ws_recv_idle_timeout_sec
        if idle_timeout <= 0:
            return await ws.recv()
        while self._running:
            try:
                return await asyncio.wait_for(ws.recv(), timeout=idle_timeout)
            except asyncio.TimeoutError:
                try:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self._ws_pong_timeout_sec)
                    if scope == "fill-ws":
                        self._last_ws_ok_ts = time.time()
                except Exception as ping_exc:
                    raise TimeoutError(
                        f"{scope} idle>{idle_timeout:.0f}s and ping failed: {_format_exc(ping_exc)}"
                    ) from ping_exc
        raise asyncio.CancelledError()

    def _update_market_snapshot(
        self,
        token_id: str,
        *,
        best_bid: Decimal,
        best_ask: Decimal,
        bids: Optional[list[tuple[Decimal, Decimal]]] = None,
        asks: Optional[list[tuple[Decimal, Decimal]]] = None,
        source: str,
        ts_ms: int = 0,
    ) -> Optional[MarketSnapshot]:
        prev = self._market_snapshots.get(token_id)
        if not self._snapshot_values_valid(best_bid, best_ask):
            log(
                f"[snapshot-drop] token={token_id} source={source} "
                f"bid={best_bid} ask={best_ask} reason=invalid_quote"
            )
            return prev

        now = time.time()
        incoming_bids, incoming_asks = self._sort_book_levels(
            list(bids) if bids else [],
            list(asks) if asks else [],
        )
        incoming_depth_trusted = (
            bool(incoming_bids)
            and bool(incoming_asks)
            and not self._snapshot_is_placeholder(best_bid, best_ask)
        )
        if incoming_depth_trusted:
            depth_snap = MarketSnapshot(
                best_bid=best_bid,
                best_ask=best_ask,
                bids=list(incoming_bids),
                asks=list(incoming_asks),
                last_update_ts=now,
                last_book_ts_ms=ts_ms,
                source=source,
            )
            self._market_depth_snapshots[token_id] = depth_snap
        else:
            depth_snap = self._fresh_depth_snapshot(token_id)

        effective_best_bid = best_bid
        effective_best_ask = best_ask
        if depth_snap is not None and self._snapshot_is_placeholder(best_bid, best_ask):
            effective_best_bid = depth_snap.best_bid
            effective_best_ask = depth_snap.best_ask

        snap = prev or MarketSnapshot()
        snap.best_bid = effective_best_bid
        snap.best_ask = effective_best_ask
        if incoming_depth_trusted:
            snap.bids = list(incoming_bids)
            snap.asks = list(incoming_asks)
        elif depth_snap is not None:
            snap.bids = list(depth_snap.bids)
            snap.asks = list(depth_snap.asks)
        elif bids is not None:
            snap.bids = list(incoming_bids)
            snap.asks = list(incoming_asks)
        snap.last_update_ts = now
        snap.last_book_ts_ms = ts_ms
        snap.source = source
        self._market_snapshots[token_id] = snap
        self.market_states[token_id] = TopOfBook(best_bid=best_bid, best_ask=best_ask)
        return snap

    def _snapshot_is_stale(self, token_id: str, snapshot: Optional[MarketSnapshot] = None) -> bool:
        snap = snapshot or self._market_snapshots.get(token_id)
        if not snap:
            return True
        return (time.time() - snap.last_update_ts) > self._market_snapshot_stale_sec

    def _trusted_depth_for_snapshot(
        self,
        token_id: str,
        snapshot: Optional[MarketSnapshot],
    ) -> Optional[MarketSnapshot]:
        if snapshot is not None and snapshot.bids and snapshot.asks and not self._snapshot_is_placeholder(snapshot.best_bid, snapshot.best_ask):
            # Reject depth data that is too old for order decisions
            if self._snapshot_is_stale(token_id, snapshot):
                return None
            return snapshot
        return self._fresh_depth_snapshot(token_id)

    def _effective_snapshot_for_gate(
        self,
        token_id: str,
        snapshot: Optional[MarketSnapshot],
    ) -> Optional[MarketSnapshot]:
        if snapshot is None:
            return None
        depth_snapshot = self._trusted_depth_for_snapshot(token_id, snapshot)
        if depth_snapshot is not None and self._snapshot_is_placeholder(snapshot.best_bid, snapshot.best_ask):
            return depth_snapshot
        # If both snapshot and depth exist, check BBA divergence
        if depth_snapshot is not None and snapshot.best_bid > 0 and depth_snapshot.best_bid > 0:
            divergence = abs(snapshot.best_bid - depth_snapshot.best_bid)
            if divergence > Decimal("0.03"):
                slug = self._token_slug_cache.get(token_id, token_id[:16])
                log(f"[safety] snapshot_divergence slug={slug} token={token_id[:16]} "
                    f"snap_bid={snapshot.best_bid} depth_bid={depth_snapshot.best_bid} "
                    f"diff={divergence}")
                return None  # force skip 闂?data mismatch
        return snapshot

    def _quote_gate(self, token_id: str, snapshot: Optional[MarketSnapshot]) -> tuple[bool, str]:
        effective_snapshot = self._effective_snapshot_for_gate(token_id, snapshot)
        if effective_snapshot is None:
            return False, "no_snapshot"
        if self._snapshot_is_stale(token_id, effective_snapshot):
            return False, "snapshot_stale"
        if effective_snapshot.best_bid <= 0 or effective_snapshot.best_ask <= 0:
            return False, "crossed_or_empty_book"
        if effective_snapshot.best_ask < effective_snapshot.best_bid:
            return False, "crossed_or_empty_book"
        depth_snapshot = self._trusted_depth_for_snapshot(token_id, effective_snapshot)
        if depth_snapshot is not None and depth_snapshot.bids:
            # Depth-thin is handled later in planner/feasibility so we can back off price
            # instead of hard-failing at best_bid immediately.
            if not self._snapshot_is_stale(token_id, depth_snapshot) and len(depth_snapshot.bids) >= 2:
                pass
            # If depth is stale or too shallow, skip depth gate rather than act on bad data
        return True, "ok"

    def _market_quote_budget_pct(self, token_id: str, market_risk: str) -> Decimal:
        cached = self._market_budget_pct.get(token_id)
        if cached is not None and cached > 0:
            return cached
        lo, hi = self.quote_balance_pct_ranges.get(market_risk, (self.quote_balance_pct_min, self.quote_balance_pct_max))
        lo = max(Decimal("0"), min(lo, Decimal("1")))
        hi = max(lo, min(hi, Decimal("1")))
        pct = Decimal(str(random.uniform(float(lo), float(hi))))
        self._market_budget_pct[token_id] = pct
        return pct

    def _paired_budget_key(self, token_id: str, paired_token: Optional[str] = None) -> str:
        other = str(paired_token or self._paired_token_cache.get(token_id) or "")
        ids = sorted([str(token_id), other]) if other else [str(token_id)]
        return "|".join(ids)

    def _event_token_ids(self, token_id: str) -> list[str]:
        token = str(token_id)
        mcfg = self._get_mcfg(token)
        paired = str(mcfg.get("paired_token_id", "") or self._paired_token_cache.get(token, "")).strip()
        ids = [token]
        if paired and paired.isdigit() and paired != token:
            ids.append(paired)
        return sorted(set(ids))

    def _reserve_paired_event_budget(self, token_id: str, paired_token: str, reserve: Decimal) -> None:
        key = self._paired_budget_key(token_id, paired_token)
        self._paired_event_budget_reserve[key] = max(Decimal("0"), reserve)

    def _paired_event_reserved_budget(self, token_id: str, paired_token: Optional[str] = None) -> Decimal:
        key = self._paired_budget_key(token_id, paired_token)
        return max(Decimal("0"), self._paired_event_budget_reserve.get(key, Decimal("0")))

    def _calc_active_orders_reserved(self, exclude_tokens: Optional[set] = None) -> Decimal:
        """Sum notional (price * remaining_size) of ALL active orders across all markets.

        This gives the true global capital commitment so the planner can
        compute: real_available = balance/allowance - active_reserved - safety_buffer.
        """
        total = Decimal("0")
        exclude = exclude_tokens or set()
        for tid, orders in self._market_live_orders.items():
            if tid in exclude:
                continue
            for o in orders:
                p = self._order_price(o)
                s = self._order_size(o)
                if p > 0 and s > 0:
                    total += p * s
        return total

    def _latency_capability(self, token_id: str) -> Dict[str, Any]:
        send_accept_ms = None
        halt_clear_ms = None
        for rec in reversed(self._latency_records):
            if rec.get("token_id") != token_id:
                continue
            if send_accept_ms is None and rec.get("send_to_accept_ms") is not None:
                send_accept_ms = float(rec.get("send_to_accept_ms"))
            if halt_clear_ms is None and rec.get("halt_to_cleared_ms") is not None:
                halt_clear_ms = float(rec.get("halt_to_cleared_ms"))
            if send_accept_ms is not None and halt_clear_ms is not None:
                break
        level = "healthy"
        if (
            (send_accept_ms is not None and send_accept_ms > self._gate_send_accept_budget_ms)
            or (halt_clear_ms is not None and halt_clear_ms > self._gate_halt_clear_budget_ms)
        ):
            level = "degraded"
        return {
            "send_accept_ms": send_accept_ms,
            "halt_clear_ms": halt_clear_ms,
            "level": level,
        }

    def _feasibility_gate(
        self,
        token_id: str,
        meta: Dict[str, Any],
        snapshot: Optional[MarketSnapshot],
        top_price: Optional[Decimal] = None,
    ) -> Dict[str, Any]:
        reasons: list[str] = []
        latency = self._latency_capability(token_id)
        decision: Dict[str, Any] = {
            "can_quote": True,
            "size_cap": 1.0,
            "top_leg_action": "keep",
            "risk_grade": "A",
            "reason": reasons,
            "latency": latency,
        }
        effective_snapshot = self._effective_snapshot_for_gate(token_id, snapshot)
        if effective_snapshot is None:
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "halt", "risk_grade": "BLOCK"})
            reasons.append("no_snapshot")
            return decision
        if self._snapshot_is_stale(token_id, effective_snapshot):
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "halt", "risk_grade": "BLOCK"})
            reasons.append("snapshot_stale")
            return decision
        if effective_snapshot.best_bid <= 0 or effective_snapshot.best_ask <= 0 or effective_snapshot.best_ask < effective_snapshot.best_bid:
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "halt", "risk_grade": "BLOCK"})
            reasons.append("book_invalid")
            return decision
        if self._snapshot_is_placeholder(effective_snapshot.best_bid, effective_snapshot.best_ask):
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "halt", "risk_grade": "BLOCK"})
            reasons.append("placeholder_book")
            return decision
        reward = Decimal(str(meta.get("reward") or 0))
        if reward <= 0:
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "cancel", "risk_grade": "BLOCK"})
            reasons.append("reward_zero")
            return decision
        probe_price = top_price if top_price is not None and top_price > 0 else max(effective_snapshot.best_bid, Decimal("0.01"))
        depth_snapshot = self._trusted_depth_for_snapshot(token_id, effective_snapshot)
        # Only evaluate depth-based gates when depth data is trustworthy
        _depth_trustworthy = (
            depth_snapshot is not None
            and not self._snapshot_is_stale(token_id, depth_snapshot)
            and len(depth_snapshot.bids) >= 2
        )
        if _depth_trustworthy:
            front_notional = self._front_notional_from_snapshot(depth_snapshot, probe_price)
        else:
            front_notional = self._front_notional_from_snapshot(effective_snapshot, probe_price) if effective_snapshot.bids else self.min_front_bid_notional_usdc  # assume OK if no depth
        decision["front_notional"] = float(front_notional)
        fill_risk = float(meta.get("fill_risk") or 0.0)
        decision["fill_risk"] = fill_risk
        if latency["level"] == "degraded":
            decision["size_cap"] = min(float(decision["size_cap"]), 0.5)
            decision["risk_grade"] = "B"
            reasons.append("latency_degraded")
        if _depth_trustworthy and front_notional < (self.min_front_bid_notional_usdc * Decimal("0.50")):
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "cancel", "risk_grade": "BLOCK"})
            reasons.append("front_depth_critical")
            return decision
        if _depth_trustworthy and front_notional < self.min_front_bid_notional_usdc:
            decision.update({"size_cap": min(float(decision["size_cap"]), 0.25), "top_leg_action": "move_back", "risk_grade": "C"})
            reasons.append("front_depth_thin")
        elif not _depth_trustworthy and depth_snapshot is not None:
            reasons.append("depth_data_untrusted")
        if fill_risk >= 75:
            decision.update({"size_cap": min(float(decision["size_cap"]), 0.25), "top_leg_action": "move_back", "risk_grade": "C"})
            reasons.append("fill_risk_high")
        elif fill_risk >= 55:
            if decision["risk_grade"] == "A":
                decision["risk_grade"] = "B"
            decision["size_cap"] = min(float(decision["size_cap"]), 0.5)
            reasons.append("fill_risk_mid")
        if not reasons:
            reasons.append("ok")
        return decision

    # ---------------------------------------------------------------
    # P0: Watch / Quarantine 闂?rapid-change market detection
    # ---------------------------------------------------------------

    def _vol_tracker(self, token_id: str) -> Dict[str, Any]:
        return self._volatility_tracker.setdefault(
            token_id, {"front_notional_history": [], "defense_actions": [], "bba_prev": None}
        )

    def _vol_record_front_notional(self, token_id: str, notional: Decimal) -> None:
        tracker = self._vol_tracker(token_id)
        now = time.time()
        tracker["front_notional_history"].append((now, float(notional)))
        # keep 30s window
        tracker["front_notional_history"] = [
            (ts, v) for ts, v in tracker["front_notional_history"] if now - ts <= 30
        ]

    def _vol_record_defense_action(self, token_id: str, action: str) -> None:
        if action == "KEEP":
            return
        tracker = self._vol_tracker(token_id)
        now = time.time()
        tracker["defense_actions"].append((now, action))
        tracker["defense_actions"] = [
            (ts, a) for ts, a in tracker["defense_actions"]
            if now - ts <= self._vol_defense_action_window_sec
        ]

    def _vol_check_bba_jump(self, token_id: str, best_bid: Decimal, best_ask: Decimal) -> bool:
        """Return True if BBA jumped >= threshold ticks."""
        tracker = self._vol_tracker(token_id)
        prev = tracker.get("bba_prev")
        tracker["bba_prev"] = (best_bid, best_ask)
        if prev is None:
            return False
        prev_bid, prev_ask = prev
        tick = self._get_mcfg(token_id).get("tick", Decimal("0.01"))
        threshold = tick * self._vol_bba_jump_ticks
        if abs(best_bid - prev_bid) >= threshold or abs(best_ask - prev_ask) >= threshold:
            return True
        return False

    def _vol_check_front_notional_drop(self, token_id: str) -> bool:
        """Return True if front_notional dropped >30% within the 30s rolling window."""
        tracker = self._vol_tracker(token_id)
        history = tracker.get("front_notional_history", [])
        if len(history) < 2:
            return False
        oldest_val = history[0][1]
        newest_val = history[-1][1]
        if oldest_val <= 0:
            return False
        drop_pct = (oldest_val - newest_val) / oldest_val
        return drop_pct >= self._vol_front_notional_drop_pct

    def _vol_check_defense_action_storm(self, token_id: str) -> bool:
        """Return True if >= threshold non-KEEP defense actions in the rolling window."""
        tracker = self._vol_tracker(token_id)
        return len(tracker.get("defense_actions", [])) >= self._vol_defense_action_threshold

    def _vol_should_watch_or_quarantine(self, token_id: str) -> Optional[str]:
        """Check all volatility signals. Returns 'watch', 'quarantine', or None."""
        state = self._event_state_name(token_id)
        if state in {EVENT_HALTED_ON_FILL, EVENT_HALTED_ON_DATA, EVENT_CANCELING,
                      EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT, EVENT_STARTED_BLOCKED}:
            return None
        triggered = (
            self._vol_check_front_notional_drop(token_id)
            or self._vol_check_defense_action_storm(token_id)
        )
        if not triggered:
            return None
        if state == EVENT_WATCH:
            return "quarantine"
        return "watch"

    async def _enter_watch(self, token_id: str, reason: str) -> None:
        """Enter WATCH state: cancel all orders, start observation timer. After 2 WATCH entries, forbid the event."""
        tracker = self._vol_tracker(token_id)
        tracker["watch_count"] = int(tracker.get("watch_count", 0)) + 1
        tracker["defense_repeat_count"] = int(tracker.get("defense_repeat_count", 0)) + 1
        if tracker["watch_count"] >= 2 or tracker.get("defense_repeat_count", 0) >= self._repeat_defense_ban_count:
            self._event_banned_until[self._event_key(token_id)] = time.time() + self.event_ban_ttl_sec
            self._set_event_state(token_id, EVENT_QUARANTINE, f"watch_limit_forbid:{reason}")
            live = await self._get_live_orders_fast(token_id)
            ids = [self._order_id(o) for o in live]
            if ids:
                await self._cancel_order_ids(token_id, ids, f"forbid:{reason}")
            log(f"[forbid] token={token_id} watch_count={tracker.get('watch_count', 0)} defense_repeat_count={tracker.get('defense_repeat_count', 0)} reason={reason} ttl={self.event_ban_ttl_sec}s")
            slug = self._token_slug_cache.get(token_id, token_id[:16])
            self.send_discord(
                f"禁挂提醒\n市场: {slug}\n原因: {reason}\n进入观察次数: {tracker.get('watch_count', 0)}\n重复防御次数: {tracker.get('defense_repeat_count', 0)}\n禁挂时长: {self.event_ban_ttl_sec} 秒"
            )
            return
        tracker["watch_enter_ts"] = time.time()
        self._set_event_state(token_id, EVENT_WATCH, reason)
        live = await self._get_live_orders_fast(token_id)
        ids = [self._order_id(o) for o in live]
        if ids:
            await self._cancel_order_ids(token_id, ids, f"watch:{reason}")
        log(f"[watch] token={token_id} entered WATCH reason={reason} duration={self._vol_watch_duration_sec}s")

    async def _enter_quarantine(self, token_id: str, reason: str) -> None:
        """Enter QUARANTINE state: cancel all orders, longer cooldown."""
        tracker = self._vol_tracker(token_id)
        tracker["quarantine_enter_ts"] = time.time()
        tracker["defense_repeat_count"] = int(tracker.get("defense_repeat_count", 0)) + 1
        self._set_event_state(token_id, EVENT_QUARANTINE, reason)
        live = await self._get_live_orders_fast(token_id)
        ids = [self._order_id(o) for o in live]
        if ids:
            await self._cancel_order_ids(token_id, ids, f"quarantine:{reason}")
        log(f"[quarantine] token={token_id} entered QUARANTINE reason={reason} duration={self._vol_quarantine_duration_sec}s")
        self.send_discord(f"[QUARANTINE] token={token_id} reason={reason}")

    def _vol_check_recovery(self, token_id: str) -> bool:
        """Check if WATCH/QUARANTINE timer has expired and can auto-recover."""
        state = self._event_state_name(token_id)
        tracker = self._vol_tracker(token_id)
        now = time.time()
        if state == EVENT_WATCH:
            enter_ts = tracker.get("watch_enter_ts", 0)
            if now - enter_ts >= self._vol_watch_duration_sec:
                return True
        elif state == EVENT_QUARANTINE:
            enter_ts = tracker.get("quarantine_enter_ts", 0)
            if now - enter_ts >= self._vol_quarantine_duration_sec:
                return True
        return False

    # ---------------------------------------------------------------
    # P1: Fill闂備礁鎲￠懝楣冩煀閿濆拋鐎堕柣鎴烆焽椤╅鈧懓瀚妯肩矆瀹€鍕厱?闂?exit strategy
    # ---------------------------------------------------------------

    async def _delayed_balance_drop_reconcile(self, token_id: str, reason: str, delay_sec: float = 30.0) -> None:
        await asyncio.sleep(delay_sec)
        try:
            state = self._event_state_name(token_id)
            if state not in {EVENT_HALTED_ON_FILL, EVENT_PENDING_MANUAL_EXIT, EVENT_EXIT_PENDING}:
                return
            pos = self._get_position_size(token_id)
            if pos is None or pos <= 0:
                return
            try:
                price = self._book(token_id).best_bid if self._book(token_id) else Decimal("0")
            except Exception:
                price = Decimal("0")
            if price <= 0:
                price = self._get_mcfg(token_id).get("mid", Decimal("0")) if isinstance(self._get_mcfg(token_id), dict) else Decimal("0")
            log(f"[balance-drop-reconcile] token={token_id} pos={pos} retry_exit_after={delay_sec}s")
            self._spawn_bg(self._attempt_exit_sell(token_id, Decimal(str(price or 0)), Decimal(str(pos)), f"balance_drop_reconcile:{reason}"), name=f"balance_drop_reconcile:{token_id}")
        except Exception as e:
            log(f"[balance-drop-reconcile] token={token_id} err={e}")

    async def _attempt_exit_sell(self, token_id: str, fill_price: Decimal, fill_size: Decimal, reason: str) -> None:
        """After fill halt completes, wait then place a limit SELL order at >= fill_price."""
        await asyncio.sleep(self._exit_delay_sec)

        state = self._event_state_name(token_id)
        if state != EVENT_HALTED_ON_FILL:
            log(f"[exit] token={token_id} skip exit, state changed to {state}")
            return

        self._set_event_state(token_id, EVENT_EXIT_PENDING, f"exit_sell:{reason}")

        position = await self._get_token_position(token_id)
        if position <= 0:
            log(f"[exit] token={token_id} no position to sell, position={position}")
            self._set_event_state(token_id, EVENT_COOLDOWN, "exit_no_position")
            return

        sell_size = Decimal(str(position)) if position > 0 else fill_size
        sell_price = fill_price
        if sell_price <= 0:
            sell_price = Decimal("0.01")

        for attempt in range(1, self._exit_retry_count + 1):
            try:
                log(f"[exit] token={token_id} placing SELL attempt={attempt} price={sell_price} size={sell_size}")
                resp = await self._place_sell_order(token_id, sell_price, sell_size)
                order_id = str((resp or {}).get("orderID") or (resp or {}).get("id") or "")
                log(f"[exit] token={token_id} SELL order placed order_id={order_id}")
                self._pending_unwinds.append({
                    "token_id": token_id,
                    "fill_price": float(fill_price),
                    "fill_size": float(fill_size),
                    "sell_price": float(sell_price),
                    "order_id": order_id,
                    "placed_at": time.time(),
                    "reason": reason,
                })
                self.send_discord(
                    f"[EXIT] SELL order placed\n"
                    f"token={token_id}\n"
                    f"fill_price={fill_price} sell_price={sell_price} size={sell_size}\n"
                    f"order_id={order_id}"
                )
                # start monitoring the sell order
                self._spawn_bg(self._monitor_exit_order(token_id, order_id, sell_price, sell_size, reason), name=f"monitor_exit:{token_id}")
                return
            except Exception as e:
                log(f"[exit] token={token_id} SELL attempt={attempt} failed: {e}")
                if attempt < self._exit_retry_count:
                    await asyncio.sleep(3)

        # all retries failed
        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, f"exit_sell_failed:{reason}")
        self.send_discord(
            f"[EXIT FAILED] Could not place SELL order after {self._exit_retry_count} attempts\n"
            f"token={token_id}\n"
            f"fill_price={fill_price} size={fill_size}\n"
            f"ACTION REQUIRED: manual exit"
        )

    async def _place_sell_order(self, token_id: str, price: Decimal, size: Decimal) -> Any:
        """Place a SELL limit order."""
        if self.remote_signer:
            signed = await asyncio.to_thread(
                self.remote_signer.sign_order, token_id, float(price), float(size), "SELL"
            )
            if isinstance(signed, dict):
                class _SignedOrderWrap:
                    def __init__(self, d: dict):
                        self._d = d
                    def dict(self):
                        return self._d
                signed = _SignedOrderWrap(signed)
        else:
            args = OrderArgs(token_id=token_id, price=float(price), size=float(size), side=SELL)
            signed = await asyncio.to_thread(self.client.create_order, args)
        resp = await asyncio.to_thread(self.client.post_order, signed, OrderType.GTC)
        return resp

    async def _monitor_exit_order(self, token_id: str, order_id: str, sell_price: Decimal, sell_size: Decimal, reason: str) -> None:
        """Monitor the exit SELL order until filled or timeout."""
        deadline = time.time() + self._exit_timeout_sec
        check_interval = 15
        while self._running and time.time() < deadline:
            await asyncio.sleep(check_interval)
            try:
                position = await self._get_token_position(token_id)
                if position == 0.0:
                    log(f"[exit] token={token_id} position=0 exit complete")
                    self._set_event_state(token_id, EVENT_COOLDOWN, "exit_complete")
                    self.send_discord(f"[EXIT COMPLETE] token={token_id} position sold")
                    return

                orders = await asyncio.to_thread(self.client.get_orders)
                live_ids = {
                    str(o.get("id") or o.get("orderID") or "")
                    for o in orders
                    if str(o.get("status", "")).lower() in ("live", "open", "active")
                }
                if order_id and order_id not in live_ids:
                    new_position = await self._get_token_position(token_id)
                    if new_position == 0.0:
                        log(f"[exit] token={token_id} order gone + position=0, exit complete")
                        self._set_event_state(token_id, EVENT_COOLDOWN, "exit_complete")
                        self.send_fill_discord(f"[EXIT COMPLETE] token={token_id} position sold")
                        return
                    else:
                        log(f"[exit] token={token_id} order gone but position={new_position}, needs manual review")
                        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, "exit_order_gone_position_remains")
                        self.send_fill_discord(
                            f"[EXIT ALERT] SELL order disappeared but position remains\n"
                            f"token={token_id} position={new_position}\n"
                            f"ACTION REQUIRED: manual exit"
                        )
                        return
            except Exception as e:
                log(f"[exit] token={token_id} monitor error: {e}")

        # timeout
        log(f"[exit] token={token_id} SELL order timeout after {self._exit_timeout_sec}s")
        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, f"exit_timeout:{reason}")
        self.send_discord(
            f"[EXIT TIMEOUT] SELL order not filled after {self._exit_timeout_sec}s\n"
            f"token={token_id}\n"
            f"sell_price={sell_price} size={sell_size}\n"
            f"ACTION REQUIRED: manual review"
        )

    # ---------------------------------------------------------------
    # P2: 闂備礁鎼崯銊╁磿閸愯?濠电姰鍨奸崺鏍垝鎼粹埗?session mode (redesigned)
    # ---------------------------------------------------------------
    # Day: run normal markets list.  Night: cancel all day orders, gap,
    # then run night_markets list.  Transition is automatic.

    def _get_mcfg(self, token_id: str) -> Dict[str, Any]:
        """Resolve market config for a token_id from day or night markets."""
        if token_id in self.market_cfg:
            return self.market_cfg[token_id]
        if token_id in self._night_market_cfg:
            return self._night_market_cfg[token_id]
        return {}

    def _current_session(self) -> str:
        """Return 'night' or 'day' based on current time in configured timezone."""
        if not self._session_enabled:
            return "day"
        try:
            nh_start_h, nh_start_m = map(int, self._session_night_start.split(":"))
            nh_end_h, nh_end_m = map(int, self._session_night_end.split(":"))
            now = datetime.now(ZoneInfo(self._session_tz))
            current_minutes = now.hour * 60 + now.minute

            night_start = nh_start_h * 60 + nh_start_m
            night_end = nh_end_h * 60 + nh_end_m

            if night_start <= night_end:
                if night_start <= current_minutes < night_end:
                    return "night"
                return "day"
            else:
                if current_minutes >= night_start or current_minutes < night_end:
                    return "night"
                return "day"
        except Exception as e:
            log(f"[session] error determining session: {e}")
            return "day"

    def _active_market_cfg(self) -> Dict[str, Dict[str, Any]]:
        """Return the market config dict for the current session."""
        if not self._session_enabled:
            return self.market_cfg
        current = self._current_session()
        if current == "night" and self._night_market_cfg:
            return self._night_market_cfg
        return self.market_cfg

    def _session_allows(self, token_id: str) -> bool:
        """Check if token_id belongs to the current session's active markets."""
        if not self._session_enabled:
            return True
        return token_id in self._active_market_cfg()

    async def _session_switch_cleanup(self) -> None:
        """Cancel ALL orders when session switches, wait gap, then let new session start."""
        current = self._current_session()
        if current == self._last_session:
            return

        prev = self._last_session
        self._last_session = current
        if prev == "unknown":
            log(f"[session] initial session: {current}")
            return

        log(f"[session] === SESSION SWITCH: {prev} 闂?{current} ===")
        self.send_discord(f"[SESSION] Switching from {prev} to {current}")
        self.send_fill_discord(f"[SESSION] Switching from {prev} to {current}")

        # Cancel all orders from the previous session's markets
        prev_markets = self._night_market_cfg if prev == "night" else self.market_cfg
        for token_id in list(prev_markets.keys()):
            state = self._event_state_name(token_id)
            if state in {EVENT_HALTED_ON_FILL, EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT}:
                continue
            try:
                live = await self._get_live_orders_fast(token_id)
                ids = [self._order_id(o) for o in live]
                if ids:
                    await self._cancel_order_ids(token_id, ids, "session_switch")
                    log(f"[session] token={token_id} cancelled {len(ids)} orders for session switch")
                self._set_event_state(token_id, EVENT_COOLDOWN, "session_switch")
            except Exception as e:
                log(f"[session] error cancelling token={token_id}: {e}")

        # Gap period before starting new session
        gap = self._session_switch_gap_sec
        log(f"[session] waiting {gap}s gap before starting {current} session...")
        await asyncio.sleep(gap)

        # Initialize event states for new session's markets if not already tracked
        new_markets = self._active_market_cfg()
        for token_id in new_markets:
            if token_id not in self._event_states:
                self._event_states[token_id] = {"state": EVENT_ACTIVE, "reason": "session_switch_init", "updated_at": time.time()}
            if token_id not in self._event_locks:
                self._event_locks[token_id] = asyncio.Lock()
            if token_id not in self.last_quote_ts:
                self.last_quote_ts[token_id] = 0.0
            if token_id not in self._per_token_last_order_ts:
                self._per_token_last_order_ts[token_id] = 0.0
            if token_id not in self._market_balance_fail_streak:
                self._market_balance_fail_streak[token_id] = 0
            if token_id not in self._market_skip_until:
                self._market_skip_until[token_id] = 0.0
            if token_id not in self._health_fail_streak:
                self._health_fail_streak[token_id] = 0
            if token_id not in self._book_req_exc_streak:
                self._book_req_exc_streak[token_id] = 0
            # Reset to ACTIVE for fresh start
            self._set_event_state(token_id, EVENT_ACTIVE, f"session_switch_to_{current}")

        log(f"[session] {current} session started with {len(new_markets)} markets")

    async def _action_delay(self, label: str) -> None:
        # emergency/risk/control actions should be immediate
        return

    @staticmethod
    def _order_id(order: dict) -> str:
        return str(order.get("id") or order.get("orderID") or "")

    @staticmethod
    def _order_price(order: dict) -> Decimal:
        return Decimal(str(order.get("price", 0) or 0))

    @staticmethod
    def _order_size(order: dict) -> Decimal:
        return Decimal(str(order.get("size", 0) or order.get("original_size", 0) or 0))

    def _size_change_within_tolerance(self, current_size: Decimal, desired_size: Decimal) -> bool:
        if current_size <= 0 or desired_size <= 0:
            return False
        base = max(abs(current_size), abs(desired_size))
        if base <= 0:
            return False
        diff_ratio = abs(current_size - desired_size) / base
        return diff_ratio <= self._size_requote_tolerance_pct

    def _sorted_live_orders(self, orders: list[dict]) -> list[dict]:
        return sorted(
            orders,
            key=lambda o: (
                self._order_price(o),
                self._order_id(o),
            ),
            reverse=True,
        )

    def _cached_live_orders(self, token_id: str) -> list[dict]:
        return self._sorted_live_orders(list(self._market_live_orders.get(token_id, [])))

    async def _get_live_orders_fast(self, token_id: str) -> list[dict]:
        cached = self._cached_live_orders(token_id)
        if cached:
            return cached
        return await self._get_live_orders_fast(token_id)

    async def _refresh_live_orders(self, token_id: str) -> list[dict]:
        orders = await asyncio.to_thread(self.client.get_orders)
        live = [
            o for o in orders
            if str(o.get("status", "")).lower() in ("live", "open", "active")
            and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
        ]
        live = self._sorted_live_orders(live)
        self._market_live_orders[token_id] = live
        return live

    async def _cancel_order_ids(self, token_id: str, ids: list[str], reason: str) -> bool:
        ids = [str(x) for x in ids if x]
        if not ids:
            self._mark_latency(token_id, "t_orders_cleared")
            return True
        cancel_kind = "sync"
        if reason.startswith("quote_gate:") or reason.startswith("feasibility_gate:"):
            cancel_kind = "risk_gate"
        elif "top_leg_defense" in reason or reason.endswith(":move_top") or reason.endswith(":cancel_top"):
            cancel_kind = "top_leg_defense"
        elif reason.startswith("empty_plan"):
            cancel_kind = "plan_empty"
        elif reason.startswith("guard-loop"):
            cancel_kind = "guard"
        log(f"[cancel] token={token_id} kind={cancel_kind} reason={reason} ids={len(ids)}")
        self._mark_latency(token_id, "t_send")
        try:
            await self._action_delay(f"cancel token={token_id} reason={reason}")
            await asyncio.to_thread(self.client.cancel_orders, ids)
            self._mark_latency(token_id, "t_cancel_ack")
        except Exception as e:
            log(f"[cancel] token={token_id} kind={cancel_kind} reason={reason} err={e}")
            return False
        live = await self._get_live_orders_fast(token_id)
        live_ids = {self._order_id(o) for o in live}
        cleared = all(oid not in live_ids for oid in ids)
        if cleared:
            self._mark_latency(token_id, "t_orders_cleared")
        return cleared

    def _front_notional_from_snapshot(self, snapshot: MarketSnapshot, my_price: Decimal) -> Decimal:
        total = Decimal("0")
        for bp, bs in snapshot.bids:
            if bp >= my_price and bs > 0:
                total += bp * bs
        return total

    async def _request_event_halt(
        self,
        token_id: str,
        final_state: str,
        reason: str,
        matched_size: Optional[Decimal] = None,
        matched_price: Optional[Decimal] = None,
        halt_key: str = "t_fill_seen",
    ) -> None:
        self._arm_halt_preemption(token_id, reason)
        lock = self._event_locks[token_id]
        async with lock:
            state = self._event_state_name(token_id)
            if state in {EVENT_HALTED_ON_FILL, EVENT_HALTED_ON_DATA}:
                return
            preserve = {halt_key} if halt_key else set()
            self._latency_flow_reset(token_id, preserve=preserve)
            self._mark_latency(token_id, halt_key)
            self._mark_latency(token_id, "t_detect")
            self._mark_latency(token_id, "t_decision")
            self._set_event_state(token_id, EVENT_CANCELING, reason)
            self._mark_latency(token_id, "t_halt_entered")
            self._fills_record.append({
                "token_id": token_id,
                "price": float(matched_price) if matched_price is not None else None,
                "size": float(matched_size) if matched_size is not None else None,
                "reason": reason,
                "ts": time.time(),
                "final_state": final_state,
            })
            if len(self._fills_record) > 200:
                self._fills_record = self._fills_record[-100:]
            live = await self._get_live_orders_fast(token_id)
            ids = [self._order_id(o) for o in live]
            cleared = await self._cancel_order_ids(token_id, ids, f"halt:{reason}") if ids else True
            if cleared:
                self._set_event_state(token_id, final_state, reason)
            else:
                log(f"[event-state] token={token_id} state={EVENT_CANCELING} reason=cancel_not_cleared")
            self._emit_latency_record(
                token_id,
                "event_halt",
                {"reason": reason, "final_state": final_state, "orders_targeted": len(ids)},
            )
            paired = self._paired_token_cache.get(token_id)
            if paired and paired in self.market_cfg and self._event_state_name(paired) == EVENT_ACTIVE:
                asyncio.create_task(
                    self._request_event_halt(
                        paired,
                        final_state,
                        f"paired_halt_from:{token_id}",
                        halt_key="t_detect",
                    )
                )


    async def _post_delay(self, label: str) -> None:
        # Unified pace 闂?no fast path for defense, everything goes through same rhythm
        lo = max(0.0, self.post_delay_min_sec)
        hi = max(lo, self.post_delay_max_sec)
        d = random.uniform(lo, hi)
        log(f"[pace] {label} sleep={d:.2f}s")
        await asyncio.sleep(d)

    async def _acquire_order_throttle(self, token_id: str, label: str) -> None:
        """Unified order throttle: per-token + global.
        Must be called before every real order placement."""
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        async with self._global_order_lock:
            now = time.time()

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Per-token cooldown 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸
            per_token_last = self._per_token_last_order_ts.get(token_id, 0.0)
            per_token_elapsed = now - per_token_last
            per_token_wait = max(0.0, self._per_token_order_min_sec - per_token_elapsed)

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Global cooldown 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁?
            global_elapsed = now - self._global_last_order_ts
            global_min = random.uniform(self._global_order_min_sec, self._global_order_max_sec)
            global_wait = max(0.0, global_min - global_elapsed)

            # Take the larger wait
            wait = max(per_token_wait, global_wait)

            if wait > 0:
                await asyncio.sleep(wait)

            # Record timestamps
            order_ts = time.time()
            self._global_last_order_ts = order_ts
            self._per_token_last_order_ts[token_id] = order_ts


    @staticmethod
    def _infer_tick_from_book(best_bid: Decimal, best_ask: Decimal) -> Decimal:
        # Common Polymarket price grids are 0.01 (1闂? or 0.001 (0.1闂?
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

        mcfg = self._get_mcfg(token_id)
        mcfg["tick"] = resolved
        # keep min distance at least one tick
        mcfg["min_distance"] = max(mcfg["min_distance"], resolved)
        self._tick_resolved.add(token_id)

    async def _normalize_guard_best_bid(self, token_id: str, book_now: Any) -> Optional[Decimal]:
        """Use the same anti-placeholder top-of-book normalization as quote path."""
        if not book_now or not getattr(book_now, "bids", None) or not getattr(book_now, "asks", None):
            return None
        try:
            bids = self._coerce_levels(getattr(book_now, "bids", None))
            asks = self._coerce_levels(getattr(book_now, "asks", None))
            best_bid, best_ask = self._best_prices_from_levels(bids, asks)
        except Exception:
            return None

        if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            return None

        # align with quote-path fallback for placeholder books like 0.001/0.999
        if self._snapshot_is_placeholder(best_bid, best_ask):
            anchor = await self._get_anchor_bid_from_gamma(token_id)
            if anchor is None or anchor <= 0:
                log(f"[guard-loop] skip token={token_id} reason=placeholder_book_no_anchor bid={best_bid} ask={best_ask}")
                return None
            best_bid = anchor

        return best_bid

    async def _legal_top_price_from_book(self, token_id: str, book_now: Any) -> Optional[Decimal]:
        if not book_now or not getattr(book_now, "bids", None) or not getattr(book_now, "asks", None):
            return None
        try:
            bids = self._coerce_levels(getattr(book_now, "bids", None))
            asks = self._coerce_levels(getattr(book_now, "asks", None))
            best_bid, best_ask = self._best_prices_from_levels(bids, asks)
        except Exception:
            return None
        if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
            return None
        if self._snapshot_is_placeholder(best_bid, best_ask):
            anchor = await self._get_anchor_bid_from_gamma(token_id)
            if anchor is None or anchor <= 0:
                return None
            best_bid = anchor
        meta = await self._get_market_meta(token_id)
        live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
        live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
        prices = self._build_price_legs(token_id, TopOfBook(best_bid=best_bid, best_ask=best_ask), live_spread=live_spread)
        return prices[0] if prices else None

    async def _check_not_at_best_bid(self, token_id: str) -> None:
        """Cancel any of our orders that sit above the current legal top quote."""
        try:
            book_now = await asyncio.to_thread(self.client.get_order_book, token_id)
            current_best_bid = await self._normalize_guard_best_bid(token_id, book_now)
            if current_best_bid is None:
                return
            legal_top = await self._legal_top_price_from_book(token_id, book_now)
            risk_limit = legal_top if legal_top is not None else current_best_bid

            orders = await asyncio.to_thread(self.client.get_orders)
            at_risk = [
                o for o in orders
                if str(o.get("status", "")).lower() in ("live", "open", "active")
                and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
                and Decimal(str(o.get("price", 0) or 0)) > risk_limit
            ]
            if not at_risk:
                return
            ids = [o.get("id") or o.get("orderID") for o in at_risk if (o.get("id") or o.get("orderID"))]
            if ids:
                await asyncio.to_thread(self.client.cancel_orders, ids)
                log(f"[safety] legal_top_guard cancelled {len(ids)} orders above legal_top={risk_limit} token={token_id}")
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
        Runs independently of the quote cycle 闂?covers cooldowns, skips, and gaps.

        Per-token check interval: each token_id is only book-fetched every
        guard_per_token_interval_sec to limit API load when many markets are active.
        """
        guard_interval = 3.0          # seconds between outer loop ticks
        per_token_interval = 15.0     # min seconds between book fetches per token
        _last_guard_ts: dict[str, float] = {}

        while self._running:
            try:
                # Market-WS down detection: if no message received for too long,
                # cancel all orders 闂?we are blind to market changes.
                if self._last_market_ws_ok_ts > 0:
                    market_ws_age = time.time() - self._last_market_ws_ok_ts
                    if market_ws_age > self._market_ws_down_cancel_sec:
                        if market_ws_age >= self._proxy_failover_ws_down_trigger_sec:
                            asyncio.create_task(self._maybe_failover_proxy("market_ws_down"))
                        try:
                            await asyncio.to_thread(self.client.cancel_all)
                            log(f"[guard-loop] market-ws down {market_ws_age:.0f}s > {self._market_ws_down_cancel_sec:.0f}s 闂?cancelled all orders")
                            self.send_discord(f"[ALERT] market-ws down {market_ws_age:.0f}s, cancelled all orders for safety")
                            for tid in self.market_cfg:
                                self._last_plan_sig[tid] = ""
                                self.last_quote_ts[tid] = 0.0
                        except Exception as e:
                            log(f"[guard-loop] market-ws-down cancel_all failed: {e}")
                        await asyncio.sleep(guard_interval)
                        continue

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
                        legal_top = await self._legal_top_price_from_book(tid, book_now)
                        risk_limit = legal_top if legal_top is not None else best_bid
                        for o in tok_orders:
                            op = Decimal(str(o.get("price", 0) or 0))
                            if op > risk_limit:
                                oid = o.get("id") or o.get("orderID")
                                if oid:
                                    cancel_ids.append((tid, risk_limit, oid))
                    except Exception:
                        continue

                if cancel_ids:
                    ids = [oid for _, _, oid in cancel_ids]
                    await asyncio.to_thread(self.client.cancel_orders, ids)

                    touched_tokens: set[str] = set()
                    for tid, bb, oid in cancel_ids:
                        log(f"[guard-loop] cancelled order above legal_top={bb} token={tid} oid={oid}")
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
        cfg = self._get_mcfg(token_id)
        tick = cfg["tick"]
        _slug = self._token_slug_cache.get(token_id, token_id[:16])

        # Prefer live rewardsMaxSpread from API; fall back to config value
        spread = live_spread if live_spread is not None else cfg["spread"]
        if spread > Decimal("1"):
            spread = spread / Decimal("100")

        # Valid range: [reward_lower, best_bid - 1 tick]
        # best_bid itself is NEVER included 闂?it's a fill-risk boundary
        reward_lower = max(tick, book.mid - spread)
        safe_top = book.best_bid - tick  # ceiling: best_bid - 1 tick

        if safe_top < reward_lower or safe_top < tick:
            if book.best_bid >= reward_lower and book.best_bid >= tick:
                return [self._floor_to_tick(book.best_bid, tick)]
            # No valid position exists in reward zone; skip this market
            log(f"[price-legs-skip] slug={_slug} token={token_id[:16]}... bid={book.best_bid} ask={book.best_ask} mid={book.mid} spread={spread} reward_lower={reward_lower} safe_top={safe_top} tick={tick}")
            return []

        reward_zone_width = safe_top - reward_lower

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Fine-tick markets (tick < 0.01): percentage-based distribution 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?
        if tick < Decimal("0.01"):
            return self._build_fine_tick_legs(
                token_id, book, tick, reward_lower, safe_top, reward_zone_width, _slug,
            )

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Regular 1-cent markets: original mechanical logic 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍?
        range_ticks = int((book.best_bid - reward_lower) / tick)

        max_legs = 3

        if range_ticks <= 1 and book.best_bid >= reward_lower and book.best_bid >= tick:
            return [self._floor_to_tick(book.best_bid, tick)]

        n_legs = min(range_ticks, max_legs)
        if n_legs <= 0:
            return []

        prices = []
        for i in range(1, n_legs + 1):
            p = self._floor_to_tick(book.best_bid - tick * Decimal(i), tick)
            # Liquidity rewards score falls to zero exactly at the max-spread boundary.
            if p >= reward_lower and p >= tick:
                prices.append(p)

        return prices

    def _build_fine_tick_legs(
        self,
        token_id: str,
        book: TopOfBook,
        tick: Decimal,
        reward_lower: Decimal,
        safe_top: Decimal,
        reward_zone_width: Decimal,
        slug: str,
    ) -> list[Decimal]:
        """Generate price legs for fine-tick (0.001) markets using reward-zone
        percentage distribution instead of mechanical best_bid - N*tick.

        The legs are spread across the reward zone with a configurable retreat
        bias, so they sit further from best_bid and are less likely to be hit.

        Config knobs (in strategy section):
          fine_tick_max_legs      闂?max number of legs (default 5)
          fine_tick_retreat_pct   闂?how far into the reward zone to start,
                                   as a fraction of zone width (default 0.35,
                                   meaning skip the top 35% of the zone)
          fine_tick_zone_use_pct  闂?what fraction of the remaining zone to
                                   spread legs across (default 0.50)
        """
        strategy = self.cfg.get("strategy", {})
        max_legs = int(strategy.get("fine_tick_max_legs", 5))
        retreat_pct = Decimal(str(strategy.get("fine_tick_retreat_pct", "0.35")))
        zone_use_pct = Decimal(str(strategy.get("fine_tick_zone_use_pct", "0.50")))

        # Clamp config values to sane ranges
        retreat_pct = max(Decimal("0.05"), min(retreat_pct, Decimal("0.80")))
        zone_use_pct = max(Decimal("0.10"), min(zone_use_pct, Decimal("0.80")))

        # The usable band within the reward zone:
        #   top_start = safe_top - retreat_pct * zone_width   (retreat from top)
        #   bottom    = top_start - zone_use_pct * zone_width (spread downward)
        retreat_amount = reward_zone_width * retreat_pct
        top_start = safe_top - retreat_amount
        use_band = reward_zone_width * zone_use_pct
        bottom = max(reward_lower, top_start - use_band)
        top_start = max(bottom + tick, top_start)  # ensure at least 1 tick band

        # Snap to tick grid
        top_start = self._floor_to_tick(top_start, tick)
        bottom = self._floor_to_tick(bottom, tick)

        if top_start < reward_lower or top_start < tick:
            # Fallback: zone too narrow after retreat, place at safe_top
            log(f"[fine-tick-fallback] slug={slug} zone_too_narrow top_start={top_start} reward_lower={reward_lower}")
            if safe_top >= reward_lower and safe_top >= tick:
                return [self._floor_to_tick(safe_top, tick)]
            return []

        band_width = top_start - bottom
        if band_width <= 0 or max_legs <= 0:
            return [top_start] if top_start >= reward_lower and top_start >= tick else []

        # Distribute legs evenly across [bottom, top_start]
        if max_legs == 1:
            prices = [top_start]
        else:
            step = band_width / Decimal(max_legs - 1)
            # Ensure step is at least 1 tick
            step = max(step, tick)
            prices = []
            for i in range(max_legs):
                p = self._floor_to_tick(top_start - step * Decimal(i), tick)
                if p >= reward_lower and p >= tick and p not in prices:
                    prices.append(p)

        if not prices and safe_top >= reward_lower and safe_top >= tick:
            prices = [self._floor_to_tick(safe_top, tick)]

        return prices

    # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Dual-side: paired mode helpers for <10c markets 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸

    def _is_low_price_paired_mode(self, token_id: str) -> tuple[bool, str]:
        """Check whether token_id should enter paired (both-or-none) mode.

        Returns (paired_mode: bool, paired_token_id: str).
        Only activates when:
          - dual_side is enabled globally
          - the token is NOT itself an auto-injected NO token
          - YES top price <= _dual_side_max_mid (0.10)
        For auto-injected NO tokens the gate at the top of
        update_and_quote_market already handles eligibility.
        """
        if not self._dual_side_enabled:
            return False, ""
        mcfg = self._get_mcfg(token_id)
        if mcfg.get("_dual_side_auto"):
            return False, ""  # NO side handled by the early gate
        paired_token = mcfg.get("paired_token_id", "")
        if not paired_token:
            return False, ""
        return True, paired_token

    def _paired_side_ready(
        self,
        token_id: str,
        paired_token: str,
        yes_top_price: Decimal,
    ) -> tuple[bool, str]:
        """Validate that the paired side can also produce a valid plan.

        Called when paired mode is active 闂?either YES or NO is below
        max_mid.  Returns (ready: bool, skip_reason: str).
        """
        yes_is_low = yes_top_price <= self._dual_side_max_mid
        no_is_low = yes_top_price >= (Decimal("1") - self._dual_side_max_mid)
        if not yes_is_low and not no_is_low:
            # Neither side is in low-price territory 闂?paired mode not needed
            return True, ""

        pair_snap = self._market_snapshots.get(paired_token)
        if pair_snap is None:
            return False, "paired_side_unavailable"

        cached = self._market_meta_cache.get(paired_token)
        if cached is None:
            return False, "paired_side_no_meta"
        pair_meta = cached[0]  # (meta_dict, timestamp)

        pair_live_spread_raw = (
            pair_meta.get("maxIncentiveSpread")
            or pair_meta.get("rewardsMaxSpread")
        )
        pair_live_spread = (
            Decimal(str(pair_live_spread_raw))
            if pair_live_spread_raw is not None
            else None
        )
        pair_tob = TopOfBook(best_bid=pair_snap.best_bid, best_ask=pair_snap.best_ask)
        pair_prices = self._build_price_legs(paired_token, pair_tob, live_spread=pair_live_spread)
        if not pair_prices:
            return False, "paired_side_no_plan"

        pair_gate = self._feasibility_gate(
            paired_token, pair_meta, pair_snap, top_price=pair_prices[0]
        )
        if not pair_gate.get("can_quote", False):
            return False, "paired_side_gate_failed"

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Unified paired budget pre-check 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?
        # Compute real available capital: balance/allowance minus ALL active
        # orders across every market, minus a safety buffer.  Both YES and NO
        # notional must fit within this single envelope.
        pair_top_price = pair_prices[0]
        pair_reward_min = Decimal(str(pair_meta.get("rewardsMinSize") or 0))
        pair_min_size = max(self.min_order_size, pair_reward_min, Decimal("0.001"))
        pair_min_notional = pair_top_price * pair_min_size
        this_min_notional = yes_top_price * pair_min_size

        avail = self._last_balance
        if avail is not None and avail > 0:
            # Per-event budgeting only: do NOT deduct active orders from other
            # markets. Cross-event capital is intentionally reusable. The shared
            # cap applies only inside this paired event (YES+NO combined).
            active_reserved = Decimal("0")
            safety_buffer = avail * Decimal("0.02")  # 2% safety cushion
            real_avail = max(Decimal("0"), avail - safety_buffer)

            pair_risk = str(self._get_mcfg(paired_token).get("risk", "mid")).lower()
            pair_pct = self._market_quote_budget_pct(paired_token, pair_risk)
            pair_size_cap = Decimal(str(pair_gate.get("size_cap", 1.0) or 0.0))
            this_risk = str(self._get_mcfg(token_id).get("risk", "mid")).lower()
            this_pct = self._market_quote_budget_pct(token_id, this_risk)

            # Combined budget cap is the MINIMUM of pct-based allocation and real available
            combined_budget_cap = min(
                real_avail,
                min(avail * max(this_pct, pair_pct), avail * Decimal("0.98")) * pair_size_cap,
            )
            combined_budget_cap = max(Decimal("0"), combined_budget_cap)
            combined_min_required = pair_min_notional + this_min_notional

            self._reserve_paired_event_budget(token_id, paired_token, combined_budget_cap)

            slug = self._token_slug_cache.get(token_id, token_id[:16])
            if combined_budget_cap < combined_min_required:
                log(
                    f"[paired-budget] slug={slug} token={token_id[:16]} "
                    f"real_avail={real_avail} combined_cap={combined_budget_cap} "
                    f"yes_min={this_min_notional} no_min={pair_min_notional} combined_min={combined_min_required}"
                )

            if combined_budget_cap < pair_min_notional:
                return False, "paired_side_budget_insufficient"
            if combined_budget_cap < combined_min_required:
                return False, "paired_side_combined_budget_insufficient"
        elif avail is not None and avail <= 0:
            return False, "paired_side_budget_insufficient"

        return True, ""

    @staticmethod
    def _total_bid_notional(snapshot) -> Decimal:
        """Sum price*size across ALL bid levels of a snapshot."""
        total = Decimal("0")
        if snapshot is None:
            return total
        for bp, bs in snapshot.bids:
            if bs > 0:
                total += bp * bs
        return total

    def _cheap_side_depth_ok(
        self,
        yes_top_price: Decimal,
        yes_token_id: str,
        no_token_id: str,
    ) -> tuple[bool, Decimal, str]:
        """Check that the cheap (<10c) side has sufficient book depth.

        Returns (ok, depth_usdc, reason).
        If neither side is cheap, returns (True, 0, "").
        """
        yes_is_low = yes_top_price <= self._dual_side_max_mid
        no_is_low = yes_top_price >= (Decimal("1") - self._dual_side_max_mid)

        if not yes_is_low and not no_is_low:
            return True, Decimal("0"), ""

        # Determine which token is the cheap side
        cheap_tid = yes_token_id if yes_is_low else no_token_id
        cheap_snap = self._market_snapshots.get(cheap_tid)
        if cheap_snap is None:
            return False, Decimal("0"), "cheap_side_no_snapshot"

        depth = self._total_bid_notional(cheap_snap)
        if depth < self._dual_side_min_book_depth:
            return False, depth, "cheap_side_depth_insufficient"

        return True, depth, ""

    # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Dual-side: auto-inject paired NO token for low-price markets 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?

    def _maybe_inject_dual_side_tokens(self) -> None:
        """For markets where YES mid is low, auto-register the paired NO
        token so the engine quotes BUY on both sides.

        Called once during startup after market metadata is resolved.
        The NO token is added to market_cfg as a regular market entry
        with risk = dual_side.no_side_risk (default "low").
        """
        if not self._dual_side_enabled:
            return

        to_inject: list[tuple[str, str, Dict[str, Any]]] = []
        for token_id, mcfg in list(self.market_cfg.items()):
            if token_id in self._dual_side_injected:
                continue
            # Check if this market's config has a paired_token_id
            paired = mcfg.get("paired_token_id", "")
            if not paired or paired in self.market_cfg:
                continue  # already present or no pair known
            # We'll check mid later at quote time; inject speculatively
            # and let the normal quoting gate handle eligibility.
            to_inject.append((token_id, paired, mcfg))

        for yes_tid, no_tid, yes_cfg in to_inject:
            self.market_cfg[no_tid] = {
                "spread": yes_cfg["spread"],
                "tick": yes_cfg["tick"],
                "min_distance": yes_cfg.get("min_distance", self.default_min_distance),
                "risk": self._dual_side_no_risk,
                "session": yes_cfg.get("session", "both"),
                "paired_token_id": yes_tid,
                "_dual_side_auto": True,
            }
            # Initialise runtime state for the new token
            self._event_states[no_tid] = {"state": EVENT_ACTIVE, "reason": "dual_side_inject", "updated_at": time.time()}
            self._event_locks.setdefault(no_tid, asyncio.Lock())
            self._latency_marks.setdefault(no_tid, {})
            self._halt_requested.setdefault(no_tid, None)
            self.last_quote_ts.setdefault(no_tid, 0.0)
            self._dual_side_injected.add(no_tid)
            slug = self._token_slug_cache.get(yes_tid, yes_tid[:16])

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

    def _alloc_weights(self, n_legs: int) -> list[Decimal]:
        if n_legs <= 0:
            return []
        decay = self._level_weight_decay
        weights: list[Decimal] = []
        cur = Decimal("1")
        for _ in range(n_legs):
            weights.append(cur)
            cur *= decay
        s = sum(weights)
        return [w / s for w in weights]

    def _score_price_levels(
        self,
        token_id: str,
        prices: list[Decimal],
        depth_snapshot: Optional[MarketSnapshot],
        reward_lower: Decimal,
        legal_top: Decimal,
    ) -> list[tuple[Decimal, Decimal, Decimal]]:
        scored: list[tuple[Decimal, Decimal, Decimal]] = []
        if not prices:
            return scored
        width = max(Decimal("0.000001"), legal_top - reward_lower)
        for idx, p in enumerate(prices):
            front_notional = self._front_notional_from_snapshot(depth_snapshot, p) if depth_snapshot is not None else Decimal("0")
            reward_score = max(Decimal("0"), min(Decimal("1"), (p - reward_lower) / width))
            distance_penalty = Decimal(idx) * self._level_distance_penalty
            depth_bonus = Decimal("0")
            if front_notional > 0 and self.min_front_bid_notional_usdc > 0:
                depth_bonus = min(self._level_depth_bonus_cap, front_notional / self.min_front_bid_notional_usdc * self._level_depth_bonus_scale)
            vol_penalty = Decimal("0")
            if self._vol_check_bba_jump(token_id, self._market_snapshots.get(token_id).best_bid if self._market_snapshots.get(token_id) else p, self._market_snapshots.get(token_id).best_ask if self._market_snapshots.get(token_id) else p):
                vol_penalty += self._level_bba_penalty
            if self._vol_check_defense_action_storm(token_id):
                vol_penalty += self._level_defense_storm_penalty
            final_score = reward_score - distance_penalty + depth_bonus - vol_penalty
            scored.append((p, front_notional, final_score))
        scored.sort(key=lambda x: (x[2], x[0]), reverse=True)
        return scored

    def _adapt_prices_for_front_depth(
        self,
        token_id: str,
        prices: list[Decimal],
        depth_snapshot: Optional[MarketSnapshot],
    ) -> list[tuple[Decimal, Decimal]]:
        if not prices:
            return []
        adapted: list[tuple[Decimal, Decimal]] = []
        for p in prices:
            front_notional = self._front_notional_from_snapshot(depth_snapshot, p) if depth_snapshot is not None else Decimal("0")
            adapted.append((p, front_notional))
        if depth_snapshot is None:
            return adapted
        for idx, (p, front_notional) in enumerate(adapted):
            if front_notional >= self.min_front_bid_notional_usdc:
                return adapted[idx:]
        return []

    async def _sync_top_leg(self, token_id: str, desired: Optional[tuple[Decimal, Decimal, Decimal]], live_orders: list[dict]) -> list[dict]:
        self._ensure_order_path_open(token_id, "planner_top_leg_sync")
        current_top = live_orders[0] if live_orders else None
        desired_sig = f"{desired[0]}:{desired[1]}" if desired is not None else ""
        current_sig = f"{self._order_price(current_top)}:{self._order_size(current_top)}" if current_top else ""
        if desired_sig == current_sig:
            self._last_top_plan_sig[token_id] = desired_sig
            return live_orders
        if desired is not None and current_top is not None:
            current_price = self._order_price(current_top)
            current_size = self._order_size(current_top)
            desired_price, desired_size, _ = desired
            if current_price == desired_price and self._size_change_within_tolerance(current_size, desired_size):
                self._last_top_plan_sig[token_id] = current_sig
                return live_orders
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        if current_top is not None:
            log(f"[cancel] slug={slug} token={token_id[:16]} kind=sync reason=planner_top_leg_sync "
                f"old_price={self._order_price(current_top)} ids=1")
            await self._cancel_order_ids(token_id, [self._order_id(current_top)], "planner_top_leg_sync")
            live_orders = await self._get_live_orders_fast(token_id)
        if desired is not None:
            price, size, _ = desired
            await self.place_post_only_order(token_id, price, size, label="top_leg_sync")
            live_orders = await self._get_live_orders_fast(token_id)
        self._last_top_plan_sig[token_id] = desired_sig
        return live_orders

    async def _sync_back_legs(self, token_id: str, desired_back: list[tuple[Decimal, Decimal, Decimal]], live_orders: list[dict]) -> list[dict]:
        self._ensure_order_path_open(token_id, "planner_back_legs_sync")
        live_back = live_orders[1:] if len(live_orders) > 1 else []
        desired_sig = "|".join(f"{p}:{s}" for p, s, _ in desired_back)
        live_sig = "|".join(f"{self._order_price(o)}:{self._order_size(o)}" for o in live_back)
        if desired_sig == live_sig:
            self._last_back_plan_sig[token_id] = desired_sig
            return live_orders
        if len(desired_back) == len(live_back) and desired_back:
            within_tolerance = True
            for (dp, ds, _), live_order in zip(desired_back, live_back):
                lp = self._order_price(live_order)
                ls = self._order_size(live_order)
                if lp != dp or not self._size_change_within_tolerance(ls, ds):
                    within_tolerance = False
                    break
            if within_tolerance:
                self._last_back_plan_sig[token_id] = live_sig
                return live_orders
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        ids = [self._order_id(o) for o in live_back]
        if ids:
            log(f"[cancel] slug={slug} token={token_id[:16]} kind=sync reason=planner_back_legs_sync ids={len(ids)}")
            await self._cancel_order_ids(token_id, ids, "planner_back_legs_sync")
            live_orders = await self._get_live_orders_fast(token_id)
        for price, size, _ in desired_back:
            await self.place_post_only_order(token_id, price, size, label="back_leg_sync")
        live_orders = await self._get_live_orders_fast(token_id)
        self._last_back_plan_sig[token_id] = desired_sig
        return live_orders

    def _spawn_bg(self, coro, name: str = "bg"):
        task = asyncio.create_task(coro, name=name)

        def _done(t: asyncio.Task):
            try:
                exc = t.exception()
                if exc:
                    log(f"[task-error] name={t.get_name()} err={exc.__class__.__name__}: {exc}")
            except asyncio.CancelledError:
                pass

        task.add_done_callback(_done)
        return task

    async def _maybe_run_top_leg_defense(
        self,
        token_id: str,
        trigger: str,
        snapshot: Optional[MarketSnapshot] = None,
    ) -> None:
        current_task = asyncio.current_task()
        if current_task is not None:
            self._top_leg_defense_tasks[token_id] = current_task
        try:
            if self._event_blocks_quote(token_id):
                return
            snap = snapshot or self._market_snapshots.get(token_id)
            snap = self._effective_snapshot_for_gate(token_id, snap)
            if not snap:
                return
            if self._snapshot_is_stale(token_id, snap):
                await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, f"stale_snapshot:{trigger}", halt_key="t_detect")
                return
            best_bid = snap.best_bid
            best_ask = snap.best_ask
            if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
                await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, f"bad_market_snapshot:{trigger}", halt_key="t_detect")
                return

            # --- P0: BBA jump detection (before any gate/order logic) ---
            bba_jumped = self._vol_check_bba_jump(token_id, best_bid, best_ask)
            if bba_jumped:
                vol_decision = self._vol_should_watch_or_quarantine(token_id)
                if vol_decision is None:
                    vol_decision = "watch"
                if vol_decision == "quarantine":
                    await self._enter_quarantine(token_id, f"bba_jump:{trigger}")
                    return
                elif vol_decision == "watch":
                    await self._enter_watch(token_id, f"bba_jump:{trigger}")
                    return

            meta = await self._get_market_meta(token_id)
            lock = self._event_locks[token_id]
            halt_reason: Optional[str] = None
            self._ensure_order_path_open(token_id, "top_leg_defense_enter")
            live_orders = self._cached_live_orders(token_id)
            if not live_orders:
                live_orders = await self._get_live_orders_fast(token_id)
            if not live_orders:
                return
            top_order = live_orders[0]
            if lock.locked():
                # Defense gets priority: if we already know the top order and market moved,
                # attempt fast cancel from cache instead of giving up behind planner work.
                tick = self._get_mcfg(token_id).get("tick", Decimal("0.01"))
                live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
                live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
                legal_prices = self._build_price_legs(token_id, TopOfBook(best_bid=best_bid, best_ask=best_ask), live_spread=live_spread)
                depth_snapshot = self._trusted_depth_for_snapshot(token_id, snap)
                adapted_legal_prices = self._adapt_prices_for_front_depth(token_id, legal_prices, depth_snapshot)
                legal_top = adapted_legal_prices[0][0] if adapted_legal_prices else (legal_prices[0] if legal_prices else None)
                top_price = self._order_price(top_order)
                gate = self._feasibility_gate(token_id, meta, snap, top_price=top_price)
                if gate.get("top_leg_action") in {"cancel", "move_back", "halt"} or legal_top is None or (legal_top is not None and top_price > legal_top):
                    await self._cancel_order_ids(token_id, [self._order_id(top_order)], f"top_leg_defense:{trigger}:fast_cancel_locked")
                return
            async with lock:
                live_orders = self._cached_live_orders(token_id)
                if not live_orders:
                    return
                top_order = live_orders[0]
                tick = self._get_mcfg(token_id).get("tick", Decimal("0.01"))
                top_price = self._order_price(top_order)
                top_size = self._order_size(top_order)
                live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
                live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
                legal_prices = self._build_price_legs(token_id, TopOfBook(best_bid=best_bid, best_ask=best_ask), live_spread=live_spread)
                depth_snapshot = self._trusted_depth_for_snapshot(token_id, snap)
                adapted_legal_prices = self._adapt_prices_for_front_depth(token_id, legal_prices, depth_snapshot)
                legal_top = adapted_legal_prices[0][0] if adapted_legal_prices else (legal_prices[0] if legal_prices else None)
                front_notional = self._front_notional_from_snapshot(depth_snapshot or snap, top_price)

                # --- P0: record front notional for rolling window ---
                self._vol_record_front_notional(token_id, front_notional)

                gate = self._feasibility_gate(token_id, meta, snap, top_price=top_price)
                self._gate_decisions[token_id] = gate
                action = "KEEP"
                if gate.get("top_leg_action") == "halt":
                    halt_reason = f"feasibility_gate:{'|'.join(gate.get('reason', []))}"
                    action = "HALT_EVENT"
                elif gate.get("top_leg_action") == "cancel":
                    action = "CANCEL_TOP_LEG"
                elif gate.get("top_leg_action") == "move_back":
                    action = "MOVE_BACK_TOP_LEG"
                elif legal_top is None:
                    action = "CANCEL_TOP_LEG"
                elif legal_top is not None and top_price > legal_top:
                    action = "MOVE_BACK_TOP_LEG" if legal_top > 0 and legal_top < best_ask else "CANCEL_TOP_LEG"
                # --- P0: record defense action for volatility tracker ---
                self._vol_record_defense_action(token_id, action)

                # Detailed defense decision log
                _slug = self._token_slug_cache.get(token_id, token_id[:16])
                _spread_val = live_spread if live_spread is not None else "?"
                _reward_lower = "?"
                if live_spread is not None:
                    _mid = (best_bid + best_ask) / Decimal("2")
                    _reward_lower = max(tick, _mid - live_spread) if live_spread <= Decimal("1") else max(tick, _mid - live_spread / Decimal("100"))
                if action != "KEEP":
                    log(f"[risk] defense slug={_slug} token={token_id[:16]} action={action} "
                        f"top_price={top_price} legal_top={legal_top} bid={best_bid} ask={best_ask} "
                        f"spread={_spread_val} reward_lower={_reward_lower} "
                        f"front_notional={front_notional} gate={','.join(gate.get('reason', []))} "
                        f"trigger={trigger}")

                if action == "KEEP":
                    if self._event_state_name(token_id) == EVENT_DEFENSIVE:
                        self._set_event_state(token_id, EVENT_ACTIVE, f"defense_keep:{trigger}")
                    return

                # --- P0: check if defense action storm or depth drop triggers watch/quarantine ---
                vol_decision = self._vol_should_watch_or_quarantine(token_id)
                if vol_decision == "quarantine":
                    await self._enter_quarantine(token_id, f"defense_storm:{trigger}:{action}")
                    return
                elif vol_decision == "watch":
                    await self._enter_watch(token_id, f"defense_storm:{trigger}:{action}")
                    return

                self._latency_flow_reset(token_id)
                self._mark_latency(token_id, "t_detect")
                self._mark_latency(token_id, "t_decision")
                self._set_event_state(token_id, EVENT_DEFENSIVE, f"{trigger}:{action}")
                if action == "HALT_EVENT":
                    pass
                elif action == "CANCEL_TOP_LEG":
                    await self._cancel_order_ids(token_id, [self._order_id(top_order)], f"{trigger}:cancel_top")
                    self._market_live_orders[token_id] = await self._get_live_orders_fast(token_id)
                else:
                    await self._cancel_order_ids(token_id, [self._order_id(top_order)], f"{trigger}:move_top")
                    self._ensure_order_path_open(token_id, "top_leg_defense_after_cancel")
                    if legal_top is None or legal_top <= 0 or legal_top >= best_ask:
                        halt_reason = f"unsafe_move_back:{trigger}"
                    else:
                        await self.place_post_only_order(token_id, legal_top, top_size, label="top_leg_defense")
                        self._market_live_orders[token_id] = await self._get_live_orders_fast(token_id)
                self._emit_latency_record(token_id, "top_leg_defense", {"trigger": trigger, "action": action})
                if halt_reason is None:
                    self._set_event_state(token_id, EVENT_ACTIVE, f"defense_complete:{trigger}")
            if halt_reason is not None:
                await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, halt_reason, halt_key="t_detect")
        except EventHaltPreempted as exc:
            log(f"[preempt] token={token_id} path=top_leg_defense reason={exc}")
        except SoftQuoteSkip:
            return
        except asyncio.CancelledError:
            log(f"[preempt] token={token_id} path=top_leg_defense reason=task_cancelled")
            raise
        except Exception as e:
            log(f"[top-leg-defense] token={token_id} trigger={trigger} err={e.__class__.__name__}: {e}")
            return
        finally:
            if self._top_leg_defense_tasks.get(token_id) is current_task:
                self._top_leg_defense_tasks.pop(token_id, None)

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
                    condition_id = str(raw.get("market") or raw.get("conditionId") or "").strip()
                    if condition_id:
                        self._market_condition_ids[token_id] = condition_id
                        nm["condition_id"] = condition_id
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
            if (
                self.health_near_expiry_hours > 0
                and end_ts
                and (end_ts - time.time()) < self.health_near_expiry_hours * 3600
            ):
                return False, f"near_expiry_lt_{self.health_near_expiry_hours}h"
            return True, "ok"
        except Exception:
            return True, "api_error_skip"

    async def _deactivate_market(self, token_id: str, reason: str) -> None:
        event_token_ids = self._event_token_ids(token_id)
        self._event_banned_until[self._event_key(token_id)] = time.time() + self.event_ban_ttl_sec
        for tid in event_token_ids:
            self._market_skip_until[tid] = time.time() + self.event_ban_ttl_sec

        try:
            orders = await asyncio.to_thread(self.client.get_orders)
            live = [
                o for o in orders
                if str(o.get("status", "")).lower() in ("live", "open", "active")
                and str(o.get("asset_id") or o.get("token_id") or "") in event_token_ids
            ]
            ids = [o.get("id") or o.get("orderID") for o in live if (o.get("id") or o.get("orderID"))]
            if ids:
                await self._action_delay(f"health-cancel token={token_id}")
                await asyncio.to_thread(self.client.cancel_orders, ids)
        except Exception as e:
            log(f"[health] cancel fail token={token_id} err={e}")

        # persist disabled in config markets
        try:
            cfg = json.loads(self._config_path.read_text(encoding="utf-8"))
            for section in ("markets", "night_markets"):
                for m in cfg.get(section, []):
                    if str(m.get("token_id", "")) in event_token_ids:
                        m["enabled"] = False
            self._config_path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as e:
            log(f"[health] config disable fail token={token_id} err={e}")

        slug = self._token_slug_cache.get(token_id, token_id[:16])
        msg = f"市场已下线\n市场: {slug}\n原因: {reason}"
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
        return "|".join(self._event_token_ids(token_id))

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
        if self._event_state_name(token_id) in {
            EVENT_CANCELING,
            EVENT_HALTED_ON_FILL,
            EVENT_HALTED_ON_DATA,
            EVENT_STARTED_BLOCKED,
            EVENT_WATCH,
            EVENT_QUARANTINE,
            EVENT_EXIT_PENDING,
            EVENT_PENDING_MANUAL_EXIT,
        }:
            return True
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
        log(f"[risk] FILL_HALT token={token_id} reason={reason} size={matched_size} price={matched_price}")
        await self.trigger_global_kill_switch(f"fill_detected token={token_id} reason={reason}")
        await self._request_event_halt(
            token_id,
            EVENT_HALTED_ON_FILL,
            reason,
            matched_size=matched_size,
            matched_price=matched_price,
            halt_key="t_fill_seen",
        )
        # --- P1: auto exit sell after fill halt ---
        fill_price = matched_price if matched_price is not None and matched_price > 0 else Decimal("0")
        fill_size = matched_size if matched_size is not None and matched_size > 0 else Decimal("0")
        if fill_price > 0:
            self._spawn_bg(self._attempt_exit_sell(token_id, fill_price, fill_size, reason), name=f"attempt_exit_sell:{token_id}")

    async def _get_collateral_available(self, force_refresh: bool = False) -> Optional[Decimal]:
        """Best-effort fetch of available collateral (normalized to USDC units)."""
        now = time.time()
        cached_value, cached_at = self._balance_cache
        if (not force_refresh) and cached_at > 0 and (now - cached_at) < self._balance_cache_ttl_sec:
            return cached_value
        try:
            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, token_id="", signature_type=self.signature_type)
            data = await asyncio.to_thread(self.client.get_balance_allowance, params)
            if not isinstance(data, dict):
                self._balance_cache = (None, now)
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

            result: Optional[Decimal]
            if bal_d is not None and alw_d is not None and not allowance_unlimited:
                result = min(bal_d, alw_d)
            elif bal_d is not None:
                result = bal_d
            elif alw_d is not None:
                result = alw_d
            else:
                result = None
            self._balance_cache = (result, now)
            return result
        except Exception:
            self._balance_cache = (None, now)
            return None

    async def run(self) -> None:
        # Inject paired NO tokens before any task starts (so WS subscribes to them)
        try:
            self._maybe_inject_dual_side_tokens()
        except Exception as e:
            log(f"[dual-side-inject] startup error: {e}")

        tasks = [
            asyncio.create_task(self.book_loop(), name="book_loop"),
            asyncio.create_task(self._ws_market_watch(), name="market_ws_watch"),
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
        if not p:
            # Fall back to global HTTP_PROXIES (which may come from system env)
            return HTTP_PROXIES
        return {"http": p, "https": p}

    def _proxy_failover_is_enabled(self) -> bool:
        return self._proxy_failover_enabled and bool(self._proxy_failover_controller_url) and bool(self._proxy_failover_group_name)

    def _proxy_failover_reset_req_exc(self) -> None:
        self._proxy_failover_req_exc_count = 0
        self._proxy_failover_req_exc_recent = []

    def _proxy_failover_reset_ws_fail(self) -> None:
        self._proxy_failover_ws_handshake_fail_count = 0

    def _proxy_failover_reset_counters(self) -> None:
        self._proxy_failover_reset_req_exc()
        self._proxy_failover_reset_ws_fail()

    def _proxy_failover_record_success(self, source: str) -> None:
        if not self._proxy_failover_is_enabled():
            return
        if time.time() < self._proxy_failover_observe_until:
            msg = f"[PROXY] recovered source={source} node={self._proxy_failover_last_switch_to or '-'}"
            log(msg)
            self.send_fill_discord(msg)
        self._proxy_failover_observe_until = 0.0
        self._proxy_failover_halt_until = 0.0
        self._proxy_failover_reset_counters()

    def _proxy_failover_record_req_exc(self, source: str) -> None:
        if not self._proxy_failover_is_enabled():
            return
        now = time.time()
        self._proxy_failover_req_exc_recent = [ts for ts in self._proxy_failover_req_exc_recent if now - ts <= self._proxy_failover_req_exc_window_sec]
        self._proxy_failover_req_exc_recent.append(now)
        self._proxy_failover_req_exc_count = len(self._proxy_failover_req_exc_recent)
        if self._proxy_failover_req_exc_count >= self._proxy_failover_request_exception_threshold:
            asyncio.create_task(self._maybe_failover_proxy(f"request_exception_storm:{source}"))

    def _proxy_failover_record_ws_handshake_failure(self, source: str) -> None:
        if not self._proxy_failover_is_enabled():
            return
        self._proxy_failover_ws_handshake_fail_count += 1
        if self._proxy_failover_ws_handshake_fail_count >= self._proxy_failover_ws_handshake_fail_threshold:
            asyncio.create_task(self._maybe_failover_proxy(f"ws_handshake_timeout:{source}"))

    def _proxy_failover_should_halt(self) -> bool:
        now = time.time()
        self._proxy_failover_switch_history = [
            ts for ts in self._proxy_failover_switch_history
            if now - ts <= self._proxy_failover_switch_window_sec
        ]
        return len(self._proxy_failover_switch_history) >= self._proxy_failover_max_switches_per_window

    def _proxy_failover_mark_bad(self, node_name: str) -> None:
        if not node_name:
            return
        self._proxy_failover_node_bad_until[node_name] = time.time() + self._proxy_failover_bad_node_ttl_sec

    def _proxy_failover_allowed_node(self, node_name: str) -> bool:
        if not node_name or node_name in {"DIRECT", "REJECT", "REJECT-DROP", "PASS", "COMPATIBLE"}:
            return False
        if self._proxy_failover_whitelist_keywords and not _contains_any_ci(node_name, self._proxy_failover_whitelist_keywords):
            return False
        bad_until = self._proxy_failover_node_bad_until.get(node_name, 0.0)
        return time.time() >= bad_until

    def _clash_request(self, method: str, path: str, payload: Optional[dict] = None) -> Any:
        url = f"{self._proxy_failover_controller_url}{path}"
        kwargs: dict[str, Any] = {"timeout": 10}
        if payload is not None:
            kwargs["json"] = payload
        resp = requests.request(method, url, **kwargs)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return None

    def _get_clash_proxy_candidates(self) -> tuple[str, list[str], str]:
        data = self._clash_request("GET", "/proxies") or {}
        proxies = data.get("proxies") or {}
        group_names = [self._proxy_failover_group_name, "GLOBAL", "Proxies"]
        seen: set[str] = set()
        for group_name in group_names:
            if not group_name or group_name in seen:
                continue
            seen.add(group_name)
            group = proxies.get(group_name) or {}
            all_nodes = [str(x).strip() for x in (group.get("all") or []) if str(x).strip()]
            allowed = [x for x in all_nodes if self._proxy_failover_allowed_node(x)]
            if allowed:
                current = str(group.get("now") or "")
                return current, allowed, group_name
        return "", [], self._proxy_failover_group_name

    def _switch_clash_proxy(self, group_name: str, node_name: str) -> None:
        encoded_group = requests.utils.quote(group_name, safe="")
        self._clash_request("PUT", f"/proxies/{encoded_group}", {"name": node_name})

    async def _maybe_failover_proxy(self, reason: str) -> None:
        if not self._proxy_failover_is_enabled():
            return
        async with self._proxy_failover_lock:
            now = time.time()
            if now < self._proxy_failover_observe_until:
                return
            if now < self._proxy_failover_halt_until:
                return
            if self._proxy_failover_last_switch_ts and (now - self._proxy_failover_last_switch_ts) < self._proxy_failover_min_switch_gap_sec:
                return
            if self._proxy_failover_should_halt():
                self._proxy_failover_halt_until = now + self._proxy_failover_switch_window_sec
                log(
                    f"[PROXY-HALT] reason={reason} switches={len(self._proxy_failover_switch_history)} "
                    f"window={self._proxy_failover_switch_window_sec:.0f}s"
                )
                self.send_discord(
                    f"[ALERT] Proxy failover halted: too many switches ({len(self._proxy_failover_switch_history)}) in "
                    f"{int(self._proxy_failover_switch_window_sec)}s"
                )
                return
            try:
                current, candidates, active_group = await asyncio.to_thread(self._get_clash_proxy_candidates)
            except Exception as e:
                log(f"[PROXY] candidate_fetch_failed reason={reason} err={_format_exc(e)}")
                return
            if not candidates:
                log(f"[PROXY] no_allowed_candidates reason={reason} group={self._proxy_failover_group_name}")
                return
            if current in candidates:
                idx = candidates.index(current)
                ordered = candidates[idx + 1 :] + candidates[:idx]
            else:
                ordered = candidates
            if not ordered:
                log(f"[PROXY] no_switch_target reason={reason} current={current or '-'}")
                return
            target = ordered[0]
            try:
                await asyncio.to_thread(self._switch_clash_proxy, active_group, target)
            except Exception as e:
                log(f"[PROXY] switch_failed reason={reason} group={active_group} from={current or '-'} to={target} err={_format_exc(e)}")
                return
            if current:
                self._proxy_failover_mark_bad(current)
            self._proxy_failover_last_switch_from = current
            self._proxy_failover_last_switch_to = target
            self._proxy_failover_last_switch_reason = reason
            self._proxy_failover_last_switch_ts = now
            self._proxy_failover_cursor_node = target
            self._proxy_failover_switch_history.append(now)
            self._proxy_failover_observe_until = now + self._proxy_failover_observe_sec
            self._proxy_failover_reset_counters()
            log(
                f"[PROXY] switch group={active_group} from={current or '-'} to={target} "
                f"reason={reason} observe={self._proxy_failover_observe_sec:.0f}s"
            )
            self.send_fill_discord(
                f"[PROXY] switch group={active_group} from={current or '-'} to={target} "
                f"reason={reason} observe={self._proxy_failover_observe_sec:.0f}s"
            )

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
        self._proxy_failover_record_req_exc(key)
        active_exc = [
            k for k, ts in list(self._req_exc_recent.items())
            if now - ts <= self.global_req_exc_window_sec
        ]
        if len(active_exc) >= self.global_req_exc_events_threshold:
            await self._maybe_failover_proxy(reason)
            await self.trigger_global_kill_switch(reason)

    async def book_loop(self) -> None:
        sem = asyncio.Semaphore(self._book_loop_concurrency)

        async def _process(token_id: str) -> None:
            async with sem:
                try:
                    await self.update_and_quote_market(token_id)
                    self._book_req_exc_streak[token_id] = 0
                except Exception as e:
                    em = _format_exc(e)
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
            # P2: check for session switch and handle cleanup/gap
            await self._session_switch_cleanup()
            active_cfg = self._active_market_cfg()
            token_ids = list(active_cfg.keys())
            random.shuffle(token_ids)
            await asyncio.gather(*[_process(tid) for tid in token_ids])
            if self._shared_book_cache is not None:
                # multi-account mode: random cycle interval to stagger accounts
                cycle_sleep = random.uniform(self._multi_cycle_sleep_min, self._multi_cycle_sleep_max)
            else:
                cycle_sleep = max(self.requote_interval_ms / 1000.0, 0.1)
            await asyncio.sleep(cycle_sleep)

    async def _ws_market_watch(self) -> None:
        url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        # Subscribe to both day and night markets so WS data is ready for session switches
        all_token_ids = list(set(list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())))
        payload = {
            "assets_ids": all_token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(url, proxy=WS_PROXY, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                    await ws.send(json.dumps(payload))
                    log(f"[market-ws] netpath {_ws_proxy_diag()}")
                    log(f"[market-ws] connected assets={len(payload['assets_ids'])}")
                    backoff = 1
                    self._last_market_ws_ok_ts = time.time()
                    self._proxy_failover_record_success("market-ws")
                    while self._running:
                        raw = await self._recv_ws_message(ws, "market-ws")
                        self._last_market_ws_ok_ts = time.time()
                        self._proxy_failover_record_success("market-ws")
                        msgs = json.loads(raw)
                        payloads = msgs if isinstance(msgs, list) else [msgs]
                        for msg in payloads:
                            if not isinstance(msg, dict):
                                continue
                            event_type = str(msg.get("event_type") or msg.get("type") or "").lower()
                            if event_type == "book":
                                token_id = str(msg.get("asset_id") or "")
                                if token_id not in self.market_cfg:
                                    continue
                                bids = self._coerce_levels(msg.get("bids"))
                                asks = self._coerce_levels(msg.get("asks"))
                                bids, asks = self._sort_book_levels(bids, asks)
                                if not bids or not asks:
                                    continue
                                best_bid, best_ask = self._best_prices_from_levels(bids, asks)
                                if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
                                    continue
                                snap = self._update_market_snapshot(
                                    token_id,
                                    best_bid=best_bid,
                                    best_ask=best_ask,
                                    bids=bids,
                                    asks=asks,
                                    source="market_ws_book",
                                    ts_ms=int(str(msg.get("timestamp") or 0) or 0),
                                )
                                self._spawn_bg(self._maybe_run_top_leg_defense(token_id, "market_ws:book", snap), name=f"top_leg_defense:{token_id}:book")
                            elif event_type == "best_bid_ask":
                                token_id = str(msg.get("asset_id") or "")
                                if token_id not in self.market_cfg:
                                    continue
                                try:
                                    best_bid = Decimal(str(msg.get("best_bid", 0) or 0))
                                    best_ask = Decimal(str(msg.get("best_ask", 0) or 0))
                                except Exception:
                                    continue
                                snap = self._update_market_snapshot(
                                    token_id,
                                    best_bid=best_bid,
                                    best_ask=best_ask,
                                    source="market_ws_bba",
                                    ts_ms=int(str(msg.get("timestamp") or 0) or 0),
                                )
                                self._spawn_bg(self._maybe_run_top_leg_defense(token_id, "market_ws:best_bid_ask", snap), name=f"top_leg_defense:{token_id}:bba")
                            elif event_type == "price_change":
                                for change in msg.get("price_changes") or []:
                                    if not isinstance(change, dict):
                                        continue
                                    token_id = str(change.get("asset_id") or "")
                                    if token_id not in self.market_cfg:
                                        continue
                                    try:
                                        best_bid = Decimal(str(change.get("best_bid", 0) or 0))
                                        best_ask = Decimal(str(change.get("best_ask", 0) or 0))
                                    except Exception:
                                        continue
                                    if best_bid <= 0 or best_ask <= 0:
                                        continue
                                    snap = self._update_market_snapshot(
                                        token_id,
                                        best_bid=best_bid,
                                        best_ask=best_ask,
                                        source="market_ws_price_change",
                                        ts_ms=int(str(msg.get("timestamp") or 0) or 0),
                                    )
                                    self._spawn_bg(self._maybe_run_top_leg_defense(token_id, "market_ws:price_change", snap), name=f"top_leg_defense:{token_id}:price_change")
            except Exception as e:
                em = _format_exc(e)
                log(f"[market-ws] err={em}")
                if "opening handshake" in em.lower() and "timed out" in em.lower():
                    self._proxy_failover_record_ws_handshake_failure("market-ws")
                if self._is_req_exc(e):
                    self._log_req_diag("market-ws", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._market_ws_backoff_cap_sec)

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
        lock = self._event_locks[token_id]
        async with lock:
            now_ts = time.time()
            if now_ts < self._cooldown_until:
                self._set_event_state(token_id, EVENT_COOLDOWN, "global_cooldown")
                return
            if self._require_recovery_gate:
                if not self._recovery_ready():
                    return
                self._require_recovery_gate = False
                self.send_discord("[ALERT] Recovery conditions satisfied. Auto-resuming quoting.")
            if self._event_is_banned(token_id):
                # --- P0: auto-recover from WATCH/QUARANTINE if timer expired ---
                if self._vol_check_recovery(token_id):
                    prev_state = self._event_state_name(token_id)
                    self._vol_tracker(token_id)["watch_count"] = 0
                    self._set_event_state(token_id, EVENT_ACTIVE, f"vol_recovery_from_{prev_state.lower()}")
                    log(f"[vol-recovery] token={token_id} recovered from {prev_state}")
                else:
                    return
            # --- P2: session mode check ---
            if not self._session_allows(token_id):
                return
            if time.time() < self._market_skip_until.get(token_id, 0.0):
                self._set_event_state(token_id, EVENT_COOLDOWN, "market_skip_ttl")
                return
            if time.time() < self._market_budget_skip_until.get(token_id, 0.0):
                return
            blocked, breason = await self._is_blocked_market(token_id)
            if blocked:
                await self._deactivate_market(token_id, breason)
                return
            meta = await self._get_market_meta(token_id)
            if await self._enforce_start_guard(token_id, meta=meta, trigger="update_and_quote_market"):
                return
            if self._event_blocks_quote(token_id):
                return

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Dual-side gate: auto-injected NO tokens only quote when
            #    either side of the market is below max_mid (10c).
            #    Case A: YES price <= max_mid  闂?YES is cheap, NO is auto-injected
            #    Case B: YES price >= 1 - max_mid 闂?NO is cheap (~1-YES <= max_mid)
            mcfg = self._get_mcfg(token_id)
            if mcfg.get("_dual_side_auto"):
                yes_tid = mcfg.get("paired_token_id", "")
                yes_snap = self._market_snapshots.get(yes_tid)
                if yes_snap is not None:
                    yes_tob = TopOfBook(best_bid=yes_snap.best_bid, best_ask=yes_snap.best_ask)
                    yes_meta = await self._get_market_meta(yes_tid)
                    yes_live_spread_raw = yes_meta.get("maxIncentiveSpread") or yes_meta.get("rewardsMaxSpread") if yes_meta else None
                    yes_live_spread = Decimal(str(yes_live_spread_raw)) if yes_live_spread_raw is not None else None
                    yes_prices = self._build_price_legs(yes_tid, yes_tob, live_spread=yes_live_spread)
                    yes_top_price = yes_prices[0] if yes_prices else yes_snap.best_bid
                    # Either side must be below threshold for dual-side to activate:
                    #   YES cheap: yes_top_price <= max_mid
                    #   NO cheap:  yes_top_price >= 1 - max_mid  (i.e. NO 闂?1-YES <= max_mid)
                    yes_is_low = yes_top_price <= self._dual_side_max_mid
                    no_is_low = yes_top_price >= (Decimal("1") - self._dual_side_max_mid)
                    if not yes_is_low and not no_is_low:
                        return  # Neither side is below threshold; skip paired quoting
                    # Depth gate: cheap side must have >= min_book_depth_usdc
                    depth_ok, depth_val, depth_reason = self._cheap_side_depth_ok(
                        yes_top_price, yes_tid, token_id,
                    )
                    if not depth_ok:
                        slug = self._token_slug_cache.get(yes_tid, yes_tid[:16])
                        if time.time() - self.last_quote_ts.get(token_id, 0) > 60:
                            log(
                                f"[dual-side-skip] slug={slug} token={token_id[:16]} "
                                f"reason={depth_reason} depth={depth_val} "
                                f"min={self._dual_side_min_book_depth}"
                            )
                        return
                    slug = self._token_slug_cache.get(yes_tid, yes_tid[:16])
                    if time.time() - self.last_quote_ts.get(token_id, 0) > 60:
                        side_label = "YES_low" if yes_is_low else "NO_low"
                        log(
                            f"[dual-side-active] slug={slug} yes_top_price={yes_top_price} "
                            f"max_mid={self._dual_side_max_mid} side={side_label} "
                            f"depth={depth_val} 闂?paired mode active"
                        )
                else:
                    return  # No snapshot for YES side yet; wait

            now = time.time()
            if (now - self.last_quote_ts[token_id]) * 1000 < self.requote_interval_ms:
                return
            book = self._shared_book_cache.get(token_id) if self._shared_book_cache is not None else None
            quote_source = "shared" if book is not None else "rest"
            used_ws_fallback = False
            if book is None:
                try:
                    book = await asyncio.to_thread(self.client.get_order_book, token_id)
                    quote_source = "rest"
                except Exception as e:
                    depth_snap = self._fresh_depth_snapshot(token_id)
                    if depth_snap is not None and str(depth_snap.source).startswith("market_ws"):
                        bids = list(depth_snap.bids)
                        asks = list(depth_snap.asks)
                        best_bid = depth_snap.best_bid
                        best_ask = depth_snap.best_ask
                        used_ws_fallback = True
                        quote_source = depth_snap.source
                        age_ms = round((time.time() - depth_snap.last_update_ts) * 1000.0, 1)
                        log(
                            f"[book-loop] token={token_id} using_market_ws_fallback source={depth_snap.source} "
                            f"age_ms={age_ms} cause={_format_exc(e)}"
                        )
                    else:
                        snap = self._fresh_valid_snapshot(token_id)
                        if snap is not None:
                            age_ms = round((time.time() - snap.last_update_ts) * 1000.0, 1)
                            log(
                                f"[book-loop] token={token_id} keep_last_snapshot source={snap.source} "
                                f"age_ms={age_ms} cause={_format_exc(e)}"
                            )
                            return
                        raise
            if not used_ws_fallback:
                if not book or not getattr(book, "bids", None) or not getattr(book, "asks", None):
                    return
                bids = self._coerce_levels(getattr(book, "bids", None))
                asks = self._coerce_levels(getattr(book, "asks", None))
                bids, asks = self._sort_book_levels(bids, asks)
                best_bid, best_ask = self._best_prices_from_levels(bids, asks)
                if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
                    return
            if best_bid <= Decimal("0.02") and best_ask >= Decimal("0.98"):
                ws_snap = self._fresh_valid_snapshot(token_id)
                if ws_snap is not None and str(ws_snap.source).startswith("market_ws"):
                    best_bid = ws_snap.best_bid
                    best_ask = ws_snap.best_ask
                    bids = list(ws_snap.bids)
                    asks = list(ws_snap.asks)
                    log(
                        f"[book-loop] token={token_id} using_market_ws_snapshot "
                        f"source={ws_snap.source} age_ms={round((time.time() - ws_snap.last_update_ts) * 1000.0, 1)}"
                    )
                else:
                    anchor = await self._get_anchor_bid_from_gamma(token_id)
                    if anchor is None or anchor <= 0:
                        log(f"[quote-skip] token={token_id} reason=placeholder_book_unresolved bid={best_bid} ask={best_ask}")
                        return
                    best_bid = anchor
                    best_ask = min(Decimal("1"), anchor + Decimal("0.01"))
                    bids = []
                    asks = []
            await self._resolve_market_tick(token_id, best_bid, best_ask)
            tob = TopOfBook(best_bid=best_bid, best_ask=best_ask)
            snapshot = self._update_market_snapshot(
                token_id,
                best_bid=best_bid,
                best_ask=best_ask,
                bids=bids,
                asks=asks,
                source=quote_source,
            )
            effective_snapshot = self._effective_snapshot_for_gate(token_id, snapshot)
            if effective_snapshot is not None:
                tob = TopOfBook(best_bid=effective_snapshot.best_bid, best_ask=effective_snapshot.best_ask)
            depth_snapshot = self._trusted_depth_for_snapshot(token_id, effective_snapshot)
            can_quote, gate_reason = self._quote_gate(token_id, effective_snapshot)
            if not can_quote:
                live_token = await self._get_live_orders_fast(token_id)
                if live_token:
                    self._mark_latency(token_id, "t_detect")
                    self._mark_latency(token_id, "t_decision")
                    self._set_event_state(token_id, EVENT_DEFENSIVE, f"quote_gate:{gate_reason}")
                    await self._cancel_order_ids(token_id, [self._order_id(o) for o in live_token], f"quote_gate:{gate_reason}")
                    self._market_live_orders[token_id] = await self._get_live_orders_fast(token_id)
                if gate_reason in {"snapshot_stale", "crossed_or_empty_book"}:
                    await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, f"quote_gate:{gate_reason}", halt_key="t_detect")
                return
            reward_min_size = Decimal(str(meta.get("rewardsMinSize") or 0))
            live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
            live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
            prices = self._build_price_legs(token_id, tob, live_spread=live_spread)

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Paired (both-or-none) gate for <10c markets 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍?
            # When either side is below max_mid, enforce:
            #   1) Cheap-side book depth >= min_book_depth_usdc, else skip BOTH
            #   2) Both-or-none: paired side must also have a valid plan
            paired_mode, paired_token = self._is_low_price_paired_mode(token_id)
            if paired_mode:
                current_top_price = prices[0] if prices else best_bid
                yes_is_low = current_top_price <= self._dual_side_max_mid
                no_is_low = current_top_price >= (Decimal("1") - self._dual_side_max_mid)
                if yes_is_low or no_is_low:
                    # Depth gate: cheap side must have enough liquidity
                    depth_ok, depth_val, depth_reason = self._cheap_side_depth_ok(
                        current_top_price, token_id, paired_token,
                    )
                    if not depth_ok:
                        slug = self._token_slug_cache.get(token_id, token_id[:16])
                        log(
                            f"[dual-side-skip] token={slug} reason={depth_reason} "
                            f"depth={depth_val} min={self._dual_side_min_book_depth} "
                            f"闂?skipping entire event"
                        )
                        return
                    # Both-or-none: paired side must be ready
                    ready, skip_reason = self._paired_side_ready(
                        token_id, paired_token, current_top_price,
                    )
                    if not ready:
                        slug = self._token_slug_cache.get(token_id, token_id[:16])
                        log(
                            f"[dual-side-skip] token={slug} reason={skip_reason} "
                            f"paired_token={paired_token[:16]} both_or_none=1 top={current_top_price}"
                        )
                        return
                    slug = self._token_slug_cache.get(token_id, token_id[:16])
                    if time.time() - self.last_quote_ts.get(token_id, 0) > 60:
                        side_label = "YES_low" if yes_is_low else "NO_low"
                        log(
                            f"[dual-side-ok] token={slug} paired_token={paired_token[:16]} "
                            f"yes_top={current_top_price} side={side_label} depth={depth_val}"
                        )
            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?End paired gate 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?

            gate = self._feasibility_gate(token_id, meta, effective_snapshot, top_price=prices[0] if prices else None)
            self._gate_decisions[token_id] = gate
            if not gate.get("can_quote", False):
                live_token = await self._get_live_orders_fast(token_id)
                if live_token:
                    self._mark_latency(token_id, "t_detect")
                    self._mark_latency(token_id, "t_decision")
                    self._set_event_state(token_id, EVENT_DEFENSIVE, f"feasibility_gate:{'|'.join(gate.get('reason', []))}")
                    await self._cancel_order_ids(token_id, [self._order_id(o) for o in live_token], f"feasibility_gate:{'|'.join(gate.get('reason', []))}")
                if gate.get("top_leg_action") == "halt":
                    await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, f"feasibility_gate:{'|'.join(gate.get('reason', []))}", halt_key="t_detect")
                return
            market_risk = str(self._get_mcfg(token_id).get("risk", "mid")).lower()
            required_min_size = max(self.min_order_size, reward_min_size)
            size_cap = Decimal(str(gate.get("size_cap", 1.0) or 0.0))
            pct = self._market_quote_budget_pct(token_id, market_risk)
            reward_lower = max(self._get_mcfg(token_id)["tick"], tob.mid - (live_spread if live_spread is not None else self._get_mcfg(token_id)["spread"]))
            legal_top = tob.best_bid - self._get_mcfg(token_id)["tick"]
            adapted_prices = self._adapt_prices_for_front_depth(token_id, prices, depth_snapshot)
            requested_legs_raw = len(prices)
            scored_levels = self._score_price_levels(token_id, [p for p, _ in adapted_prices], depth_snapshot, reward_lower, legal_top)
            filtered_levels = [(p, front_notional, score) for p, front_notional, score in scored_levels if score > Decimal("0")]
            if not filtered_levels:
                filtered_levels = scored_levels
            raw_weights = self._alloc_weights(len(filtered_levels))
            viable_legs = []
            total_weight = Decimal("0")
            for (p, front_notional, score), w in zip(filtered_levels, raw_weights):
                if front_notional < self.min_front_bid_notional_usdc:
                    continue
                score_boost = max(Decimal("0.15"), min(Decimal("1.5"), score + Decimal("0.25")))
                adj_w = w * score_boost
                viable_legs.append((p, adj_w))
                total_weight += adj_w
            if not viable_legs or total_weight <= 0:
                live_token = await self._get_live_orders_fast(token_id)
                self._gate_decisions[token_id] = {
                    **gate,
                    "can_quote": False,
                    "top_leg_action": "cancel",
                    "reason": list(gate.get("reason", [])) + ["empty_plan_after_gate"],
                }
                if live_token:
                    await self._cancel_order_ids(token_id, [self._order_id(o) for o in live_token], "empty_plan")
                self._last_plan_sig[token_id] = ""
                self._last_top_plan_sig[token_id] = ""
                self._last_back_plan_sig[token_id] = ""
                return
            # 闂備礁纾崕銈夊礉韫囨稑鐤鹃柡灞诲劚閻撴盯鏌熼懜顒€濡芥繛鍛矒閺屽秹鎮滃Ο鍝勵潊闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺?
            # UNIFIED BUDGET PLANNER 闂?global capital-aware
            # 闂備礁纾崕銈夊礉韫囨稑鐤鹃柡灞诲劚閻撴盯鏌熼懜顒€濡芥繛鍛矒閺屽秹鎮滃Ο鍝勵潊闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺鏌ュ蓟閵夈儳鍘搁梺纭呭焽閸斿秴鈻嶅鍫熺厓闁绘垶锚婵偓闂佸搫鎳忛悡锟犲春閿熺姴绠涢柕濠忛檮閻濇娊姊哄畷鍥у妺闁告柨绻樺?
            min_size_needed = max(required_min_size, Decimal("0.001"))
            avail = await self._get_collateral_available()
            if avail is not None:
                self._last_balance = avail
            if avail is None or avail <= 0:
                log(
                    f"[quote-skip] token={token_id} reason=no_balance_available "
                    f"event_state={self._event_state_name(token_id)} pct={pct} size_cap={size_cap}"
                )
                return

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Step 1: compute real available capital 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸
            paired_token_for_budget = (
                self._paired_token_cache.get(token_id)
                or str(self._get_mcfg(token_id).get("paired_token_id", "") or "")
            )
            current_top_price_for_budget = viable_legs[0][0] if viable_legs else Decimal("0")
            is_paired = bool(
                paired_token_for_budget
                and current_top_price_for_budget > 0
                and current_top_price_for_budget <= self._dual_side_max_mid
            )
            # Per-event budgeting only: do NOT deduct active orders from other
            # markets. Capital is reusable across events; only this event's paired
            # sides should share the same envelope.
            active_order_reserved = Decimal("0")
            safety_buffer = avail * Decimal("0.02")
            real_avail = max(Decimal("0"), avail - safety_buffer)

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Step 2: compute this side's budget 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹?
            raw_event_budget = min(avail * pct, avail * Decimal("0.98")) * size_cap
            event_budget = min(raw_event_budget, real_avail)

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Step 3: if paired, enforce combined budget constraint 闂備礁鍟块崢婊堝磻?
            paired_reserved_budget = Decimal("0")
            yes_required = Decimal("0")
            no_required = Decimal("0")
            combined_required = Decimal("0")
            fallback_to_single_side = False
            skip_reason = ""

            if is_paired:
                paired_reserved_budget = self._paired_event_reserved_budget(
                    token_id, paired_token_for_budget
                )
                # Estimate the OTHER side's minimum notional requirement
                pair_snap = self._market_snapshots.get(paired_token_for_budget)
                pair_top_price = Decimal("0")
                if pair_snap is not None:
                    pair_top_price = pair_snap.best_bid
                pair_reward_min = Decimal("0")
                pair_meta_cached = self._market_meta_cache.get(paired_token_for_budget)
                if pair_meta_cached:
                    pair_reward_min = Decimal(str(
                        pair_meta_cached[0].get("rewardsMinSize", 0) or 0
                    ))
                pair_min_size = max(self.min_order_size, pair_reward_min, Decimal("0.001"))

                # YES / NO required = top_price * min_size for each side
                this_top_price = viable_legs[0][0] if viable_legs else Decimal("0")
                yes_required = this_top_price * min_size_needed
                no_required = pair_top_price * pair_min_size if pair_top_price > 0 else Decimal("0")
                combined_required = yes_required + no_required

                # The paired budget cap is the envelope for BOTH sides.
                # Each side gets at most half, but capped to real_avail.
                if paired_reserved_budget > 0:
                    half_budget = paired_reserved_budget / Decimal("2")
                    event_budget = min(event_budget, half_budget)
                else:
                    # No pre-reserve yet 闂?just split real_avail
                    event_budget = min(event_budget, real_avail / Decimal("2"))

                # Check: can combined fit?
                if combined_required > real_avail and combined_required > 0:
                    # Degradation: try single-side only
                    if yes_required <= real_avail:
                        fallback_to_single_side = True
                        event_budget = min(raw_event_budget, real_avail)
                        skip_reason = "paired_over_budget_fallback_single_side"
                    else:
                        slug = self._token_slug_cache.get(token_id, token_id[:16])
                        log(
                            f"[budget-plan] slug={slug} token={token_id[:16]} "
                            f"SKIP avail={avail} active_order_reserved={active_order_reserved} "
                            f"real_avail={real_avail} event_budget={event_budget} "
                            f"yes_required={yes_required} no_required={no_required} "
                            f"combined_required={combined_required} "
                            f"skip_reason=both_sides_over_budget"
                        )
                        self._market_budget_skip_until[token_id] = time.time() + self.budget_skip_cooldown_sec
                        return

            event_budget = max(Decimal("0"), event_budget)

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Step 4: plan generation with degradation cascade 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍?
            # Try: full legs 闂?fewer back legs 闂?shrink sizes 闂?single fallback 闂?skip
            plan = []
            requested_legs = requested_legs_raw if requested_legs_raw > 0 else len(viable_legs)
            planned_legs = 0
            degrade_reason = ""
            top_price = viable_legs[0][0] if viable_legs else Decimal("0")
            single_leg_required_notional = top_price * min_size_needed if top_price > 0 else Decimal("0")

            # 4a: try fitting as many legs as possible within event_budget
            for keep_count in range(len(viable_legs), 0, -1):
                subset = viable_legs[:keep_count]
                subset_weight = sum(w for _, w in subset)
                if subset_weight <= 0:
                    continue
                candidate = []
                candidate_ok = True
                for p, w in subset:
                    normalized_weight = w / subset_weight
                    leg_notional = event_budget * normalized_weight
                    size = self._floor_to_tick(leg_notional / p, Decimal("0.001")) if p > 0 else Decimal("0")
                    notional = p * size
                    if size < required_min_size or size <= 0 or notional <= 0:
                        candidate_ok = False
                        break
                    candidate.append((p, size, notional))
                if candidate_ok and candidate:
                    # Verify total plan notional doesn't exceed real_avail
                    plan_total = sum(n for _, _, n in candidate)
                    if is_paired and not fallback_to_single_side:
                        # Must leave room for the other side's minimum
                        if plan_total + no_required > real_avail:
                            continue  # try fewer legs
                    plan = candidate
                    planned_legs = keep_count
                    if keep_count < requested_legs:
                        degrade_reason = f"budget_limited_degrade requested={requested_legs} planned={planned_legs} paired={is_paired}"
                    break

            # 4b: fallback 闂?single top leg at minimum size
            if not plan and top_price > 0 and event_budget >= single_leg_required_notional:
                fallback_size = self._floor_to_tick(min_size_needed, Decimal("0.001"))
                fallback_notional = top_price * fallback_size
                if fallback_size >= required_min_size and fallback_notional > 0 and fallback_notional <= real_avail:
                    plan = [(top_price, fallback_size, fallback_notional)]
                    planned_legs = 1
                    degrade_reason = "single_leg_fallback"

            # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Step 5: comprehensive budget log 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍?
            final_planned_notional = sum(n for _, _, n in plan) if plan else Decimal("0")
            slug = self._token_slug_cache.get(token_id, token_id[:16])
            if plan and degrade_reason:
                levels = ",".join([f"{p}:{s}" for p, s, _ in plan[:8]])
                log(f"[plan] slug={slug} token={token_id[:16]} levels={levels} planned_legs={planned_legs} final_notional={final_planned_notional} paired={is_paired} {degrade_reason}".strip())

            if not plan:
                log(
                    f"[quote-skip] token={token_id} reason=no_viable_plan "
                    f"event_state={self._event_state_name(token_id)} avail={avail} "
                    f"real_avail={real_avail} event_budget={event_budget} "
                    f"single_leg_required_notional={single_leg_required_notional} "
                    f"required_min_size={required_min_size} top_price={top_price} "
                    f"pct={pct} size_cap={size_cap} requested_legs={requested_legs} "
                    f"is_paired={is_paired}"
                )
                self._market_budget_skip_until[token_id] = time.time() + self.budget_skip_cooldown_sec
                return

            self._market_budget_skip_until[token_id] = 0.0
            self._market_stale_fail_streak[token_id] = 0
            live_token = await self._get_live_orders_fast(token_id)
            if not plan:
                self._gate_decisions[token_id] = {
                    **gate,
                    "can_quote": False,
                    "top_leg_action": "cancel",
                    "reason": list(gate.get("reason", [])) + ["empty_plan_after_gate"],
                }
                if live_token:
                    await self._cancel_order_ids(token_id, [self._order_id(o) for o in live_token], "empty_plan")
                self._last_plan_sig[token_id] = ""
                self._last_top_plan_sig[token_id] = ""
                self._last_back_plan_sig[token_id] = ""
                return
            desired_top = plan[0]
            desired_back = plan[1:] if len(plan) > 1 else []
            try:
                live_token = await self._sync_top_leg(token_id, desired_top, live_token)
                live_token = await self._sync_back_legs(token_id, desired_back, live_token)
                self._market_live_orders[token_id] = live_token
                self._last_plan_sig[token_id] = "|".join([f"{p}:{s}" for p, s, _ in plan])
                self._balance_fail_streak = 0
                self._market_balance_fail_streak[token_id] = 0
                self.last_quote_ts[token_id] = now
                self._quotes_sent += 1
                if self._event_state_name(token_id) in {EVENT_DEFENSIVE, EVENT_COOLDOWN}:
                    self._set_event_state(token_id, EVENT_ACTIVE, "planner_sync_complete")
                # Removed: immediate _check_not_at_best_bid() call here caused a
                # race condition 闂?it fetches a fresh book milliseconds after placing,
                # and micro-movements make _build_price_legs compute a different
                # legal_top, triggering instant cancellation. The background
                # best_bid_guard_loop (every 10s per token) already provides
                # the same safety check with a stable book.

                # (dual-side: NO token is auto-injected as separate market)
            except Exception as e:
                if isinstance(e, EventHaltPreempted):
                    log(f"[preempt] token={token_id} path=planner reason={e}")
                    return
                if isinstance(e, SoftQuoteSkip):
                    em = str(e).lower()
                    if "stale_snapshot" in em:
                        self._market_stale_fail_streak[token_id] = self._market_stale_fail_streak.get(token_id, 0) + 1
                        if self._market_stale_fail_streak[token_id] >= self.stale_skip_threshold:
                            self._market_skip_until[token_id] = time.time() + self.stale_skip_cooldown_sec
                            self._market_stale_fail_streak[token_id] = 0
                        return
                    return
                em = str(e).lower()
                if "not enough balance" in em or "allowance" in em:
                    self._balance_fail_streak += 1
                    self._market_balance_fail_streak[token_id] = self._market_balance_fail_streak.get(token_id, 0) + 1
                    log(
                        f"[risk] balance/allowance token={token_id} "
                        f"market_streak={self._market_balance_fail_streak[token_id]} global_streak={self._balance_fail_streak} "
                        f"err={e}"
                    )
                    if self._market_balance_fail_streak[token_id] >= self.max_balance_fail_streak:
                        self._market_skip_until[token_id] = time.time() + self.cooldown_seconds
                        self._market_balance_fail_streak[token_id] = 0
                        self._set_event_state(token_id, EVENT_COOLDOWN, "balance_or_allowance")
                        log(f"[risk] market-skip token={token_id} cooldown={self.cooldown_seconds}s")
                    return
                raise

    async def place_post_only_order(self, token_id: str, price: Decimal, size: Decimal, label: str = "post") -> Any:
        self._ensure_order_path_open(token_id, f"place_pre_meta:{label}")
        meta = await self._get_market_meta(token_id)
        if await self._enforce_start_guard(token_id, meta=meta, trigger=f"place_post_only_order:{label}"):
            raise RuntimeError(f"market_start_blocked token={token_id}")

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Unified throttle: global + per-token (replaces _post_delay) 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁?
        await self._acquire_order_throttle(token_id, label)

        self._ensure_order_path_open(token_id, f"place_post_throttle:{label}")
        meta = await self._get_market_meta(token_id)
        if await self._enforce_start_guard(token_id, meta=meta, trigger=f"post_throttle_complete:{label}"):
            raise RuntimeError(f"market_start_blocked token={token_id}")

        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?Final pre-order reward-zone validation 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸
        snap = self._market_snapshots.get(token_id)
        effective = self._effective_snapshot_for_gate(token_id, snap)
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        if effective and not self._snapshot_is_stale(token_id, effective):
            fresh_bid = effective.best_bid
            fresh_ask = effective.best_ask
            live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
            live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
            cfg = self._get_mcfg(token_id)
            spread = live_spread if live_spread is not None else cfg["spread"]
            if spread > Decimal("1"):
                spread = spread / Decimal("100")
            mid = (fresh_bid + fresh_ask) / Decimal("2")
            tick = cfg["tick"]
            reward_lower = max(tick, mid - spread)
            legal_top = fresh_bid - tick
            # Reject: price above legal_top
            if price > legal_top and legal_top > 0:
                log(f"[safety] REJECT price>{legal_top} slug={slug} token={token_id[:16]} "
                    f"target={price} legal_top={legal_top} bid={fresh_bid} ask={fresh_ask} "
                    f"spread={spread} reward_lower={reward_lower} label={label}")
                raise RuntimeError(f"pre_order_reject:price_above_legal_top token={token_id[:16]}")
            # Reject: price below reward zone
            if price < reward_lower:
                log(f"[safety] REJECT price<reward_lower slug={slug} token={token_id[:16]} "
                    f"target={price} reward_lower={reward_lower} bid={fresh_bid} ask={fresh_ask} "
                    f"spread={spread} mid={mid} label={label}")
                raise RuntimeError(f"pre_order_reject:price_below_reward_zone token={token_id[:16]}")
            # Reject: price at or above best_ask (would cross spread)
            if price >= fresh_ask:
                log(f"[safety] REJECT price>=ask slug={slug} token={token_id[:16]} "
                    f"target={price} ask={fresh_ask} bid={fresh_bid} label={label}")
                raise RuntimeError(f"pre_order_reject:price_crosses_spread token={token_id[:16]}")
        elif effective is None or self._snapshot_is_stale(token_id, effective):
            raise SoftQuoteSkip(f"stale_snapshot token={token_id[:16]} label={label}")
        # 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴?End pre-order validation 闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍楣冩⒑閸愭彃甯ㄩ柛瀣崌閺屽秹宕楁径濠佸闂備礁鍟块崢婊堝磻閹剧粯鐓冮柛蹇擃槸娴滈箖姊洪崘鎻掑辅闁稿鎹囬弻宥夊礂婢跺﹣澹曢梻浣稿暱閸樻粓宕戦幘缁樼厓闁稿繐顦禍?

        async with self._signer_sem:
            async with self._signer_gap_lock:
                now = time.time()
                wait_sec = max(0.0, self.signer_requote_gap_sec - (now - self._last_signer_post_ts))
                if wait_sec > 0:
                    await asyncio.sleep(wait_sec)
                self._last_signer_post_ts = time.time()

            self._ensure_order_path_open(token_id, f"place_pre_sign:{label}")
            self._mark_latency(token_id, "t_sign_start")
            if self.remote_signer:
                signed = await asyncio.to_thread(
                    self.remote_signer.sign_order, token_id, float(price), float(size), "BUY"
                )
                if isinstance(signed, dict):
                    class _SignedOrderWrap:
                        def __init__(self, d: dict):
                            self._d = d

                        def dict(self):
                            return self._d

                    signed = _SignedOrderWrap(signed)
            else:
                args = OrderArgs(token_id=token_id, price=float(price), size=float(size), side=BUY)
                signed = await asyncio.to_thread(self.client.create_order, args)
            self._mark_latency(token_id, "t_sign_done")
            self._ensure_order_path_open(token_id, f"place_pre_send:{label}")
            self._mark_latency(token_id, "t_send")
            resp = await asyncio.to_thread(self.client.post_order, signed, OrderType.GTC)
            self._mark_latency(token_id, "t_exchange_accept")
            self._last_signer_post_ts = time.time()
            return resp

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
        async with self._kill_switch_lock:
            now = time.time()
            if now < self._cooldown_until and self._require_recovery_gate:
                log(f"[kill-switch] already active reason={reason}")
                return

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
        for token_id in list(self.market_cfg.keys()):
            if token_id not in self._market_condition_ids:
                await self._get_market_meta(token_id)
        condition_ids = [cid for cid in self._market_condition_ids.values() if cid]
        auth = {
            "apiKey": getattr(self.api_creds, "api_key", ""),
            "secret": getattr(self.api_creds, "api_secret", ""),
            "passphrase": getattr(self.api_creds, "api_passphrase", ""),
        }

        def _payloads() -> list[dict]:
            payloads = []
            if condition_ids:
                payloads.append({"type": "user", "markets": condition_ids, "auth": auth})
            payloads.append({"type": "user", "assets_ids": list(self.market_cfg.keys()), "auth": auth})
            return payloads

        backoff = 1
        ws_down_since = 0.0
        while self._running:
            url = urls[0]
            try:
                async with websockets.connect(url, proxy=WS_PROXY, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                    log(f"[fill-ws] netpath {_ws_proxy_diag()}")
                    for p in _payloads():
                        try:
                            await ws.send(json.dumps(p))
                        except Exception:
                            pass
                    # muted noisy fill-ws connected log
                    self._last_ws_ok_ts = time.time()
                    self._proxy_failover_record_success("fill-ws")
                    ws_down_since = 0.0
                    backoff = 1

                    while self._running:
                        raw = await self._recv_ws_message(ws, "fill-ws")
                        self._last_ws_ok_ts = time.time()
                        self._proxy_failover_record_success("fill-ws")
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
                            token = str(it.get("asset_id") or it.get("token_id") or "")
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
                em = _format_exc(e)
                log(f"[fill-ws] err={em}")
                if "opening handshake" in em.lower() and "timed out" in em.lower():
                    self._proxy_failover_record_ws_handshake_failure("fill-ws")
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
                    token = str(o.get("asset_id") or o.get("token_id") or "")
                    if token not in self.market_cfg:
                        continue
                    oid = str(o.get("id") or o.get("orderID") or "")

                    # Detect fully matched/filled orders (any status)
                    matched = max(
                        Decimal(str(o.get("size_matched", 0) or 0)),
                        Decimal(str(o.get("matched", 0) or 0)),
                        Decimal(str(o.get("filled_size", 0) or 0)),
                    )
                    if matched > self.fill_size_threshold:
                        if self._allow_signal(token, f"poll_matched:{oid}:{matched}"):
                            self._fills_seen += 1
                            o_price = Decimal(str(o.get("price", 0) or 0))
                            await self._trigger_event_offline(token, f"POLL_MATCHED:{matched}:status={st}", matched, o_price)
                            continue

                    # Also trigger on status transition to matched/filled
                    if st in ("matched", "filled", "closed"):
                        if self._allow_signal(token, f"poll_status_filled:{oid}"):
                            self._fills_seen += 1
                            o_price = Decimal(str(o.get("price", 0) or 0))
                            o_size = Decimal(str(o.get("original_size", o.get("size", 0)) or 0))
                            await self._trigger_event_offline(token, f"POLL_STATUS_{st.upper()}:{oid}", o_size, o_price)
                            continue

                    if st not in ("live", "open", "active"):
                        continue

                    remain = Decimal(str(o.get("remaining_size", o.get("size", 0)) or 0))
                    last = self._last_remaining_by_order.get(oid)
                    self._last_remaining_by_order[oid] = remain
                    if last is not None and remain < (last - self.fill_size_threshold):
                        if self._allow_signal(token, f"poll_remaining_drop:{oid}:{last}->{remain}"):
                            self._fills_seen += 1
                            await self._trigger_event_offline(token, f"POLL_REMAINING_DROP:{last}->{remain}")

                # notifications audit fallback (low weight)
                # keep this non-fatal: some client builds may not support this path consistently.
                try:
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
                except Exception as e:
                    log(f"[fill-poll] notifications-path-skip err={e}")

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
                    log(f"[trade-poll] baseline seeded trades_count={len(items)} seen_ids={len(self._seen_trade_ids)}")
                elif items:
                    new_count = sum(1 for t in items if isinstance(t, dict) and str(t.get("id") or t.get("trade_id") or t.get("transactionHash") or "") not in self._seen_trade_ids)
                    if new_count > 0:
                        log(f"[trade-poll] new_trades={new_count} total={len(items)}")

                # keep memory bounded 闂?use insertion-ordered list for correct FIFO truncation
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

    async def _balance_drop_watch(self) -> None:
        """Fallback fill detector: trigger alert when balance drops significantly between polls."""
        BALANCE_DROP_PCT = Decimal("0.10")  # 10% drop triggers alert
        BALANCE_DROP_ABS = Decimal("20")    # or $20 absolute drop
        prev_balance: Optional[Decimal] = None
        while self._running:
            try:
                avail = await self._get_collateral_available(force_refresh=True)
                if avail is not None and prev_balance is not None and prev_balance > 0:
                    drop = prev_balance - avail
                    drop_pct = drop / prev_balance if prev_balance > 0 else Decimal("0")
                    if drop > BALANCE_DROP_ABS or drop_pct > BALANCE_DROP_PCT:
                        log(
                            f"[balance-drop] ALERT prev={prev_balance} now={avail} "
                            f"drop={drop} pct={drop_pct:.2%} 闂?possible undetected fill"
                        )
                        # Try to identify which market lost balance by checking
                        # if any previously tracked orders disappeared
                        if self._fills_seen == 0 or drop > BALANCE_DROP_ABS:
                            # Trigger global defensive: cancel all and investigate
                            for token_id in list(self.market_cfg.keys()):
                                if self._allow_signal(token_id, f"balance_drop:{prev_balance}->{avail}"):
                                    self._fills_seen += 1
                                    reason = f"BALANCE_DROP:{prev_balance}->{avail}:drop={drop}"
                                    await self._trigger_event_offline(
                                        token_id,
                                        reason,
                                        drop,
                                        Decimal("0"),
                                    )
                                    self._spawn_bg(self._delayed_balance_drop_reconcile(token_id, reason, delay_sec=30.0), name=f"balance_drop_reconcile:{token_id}")
                                    break  # one trigger is enough to halt
                if avail is not None:
                    prev_balance = avail
                await asyncio.sleep(10)
            except Exception as e:
                log(f"[balance-drop] err={e}")
                await asyncio.sleep(10)

    async def fill_watch_loop(self) -> None:
        await asyncio.gather(self._ws_user_watch(), self._poll_fill_watch(), self._trade_poll_watch(), self._balance_drop_watch())

    async def summary_loop(self) -> None:
        while self._running:
            await asyncio.sleep(3600)
            msg = (
                "Hourly summary\n"
                f"Markets monitored: {len(self.market_cfg)}\n"
                f"Quotes sent this hour: {self._quotes_sent}\n"
                f"Fills detected this hour: {self._fills_seen}\n"
                f"Cooldown active: {'Yes' if time.time() < self._cooldown_until else 'No'}"
            )
            self.send_discord(msg)

    async def _get_token_position(self, token_id: str) -> float:
        """Check how many conditional tokens we hold for a given token_id."""
        try:
            params = BalanceAllowanceParams(
                asset_type=AssetType.CONDITIONAL, token_id=token_id, signature_type=self.signature_type
            )
            data = await asyncio.to_thread(self.client.get_balance_allowance, params)
            if not isinstance(data, dict):
                return -1.0  # unknown
            bal = data.get("balance")
            if isinstance(bal, dict):
                bal = bal.get("available") or bal.get("raw") or bal.get("value")
            if bal is not None:
                return float(self._norm_usdc(Decimal(str(bal))))
            return -1.0
        except Exception as e:
            log(f"[unwind] position check failed token={token_id} err={e}")
            return -1.0  # unknown 闂?don't act on error

    async def unwind_tracking_loop(self) -> None:
        """Periodically check pending unwind SELL orders.
        - If position is 0 闂?already sold (manually or filled), cancel residual order, remove.
        - If the order is no longer in live orders 闂?assume filled, remove.
        - If age > unwind_max_age_sec and still open 闂?Discord alert for manual review.
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

                    # Check if position has been closed (manually sold or filled)
                    position = await self._get_token_position(token_id)
                    if position == 0.0:
                        # Position gone 闂?cancel any residual sell order and clear
                        if oid and oid in live_ids:
                            try:
                                await asyncio.to_thread(self.client.cancel, oid)
                                log(f"[unwind] position=0, canceled residual order={oid}")
                            except Exception as ce:
                                log(f"[unwind] cancel residual failed order={oid} err={ce}")
                        log(f"[unwind] cleared token={token_id} position=0 age={age:.0f}s (manually sold or filled)")
                        continue

                    if oid and oid not in live_ids:
                        # Order no longer open 闂?filled or externally cancelled; consider done
                        log(f"[unwind] completed token={token_id} order_id={oid} age={age:.0f}s")
                        continue

                    if age > self._unwind_max_age_sec:
                        # Timed out 闂?notify via Discord for manual review, keep order alive
                        hours = age / 3600
                        msg = (
                            f"[UNWIND ALERT] Unwind order not filled after {hours:.1f}h\n"
                            f"token={token_id}\n"
                            f"fill_price={fill_price} size={fill_size} notional={float(fill_price * fill_size):.2f}\n"
                            f"order_id={oid}\n"
                            f"position={position}\n"
                            f"reason={uw.get('reason', '')}\n"
                            f"Action required: check market and decide manually."
                        )
                        log(f"[unwind] timeout alert token={token_id} age={hours:.1f}h order_id={oid} position={position}")
                        self.send_discord(msg)
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
                # Combine day + night markets for state output
                all_markets = dict(self.market_cfg)
                all_markets.update(self._night_market_cfg)
                for tid, mcfg in all_markets.items():
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
                        if ru <= rl and tob.best_bid >= rl and tob.best_bid >= tick:
                            ru = tob.best_bid
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

                    event_state = self._event_state_name(tid)
                    banned = self._event_is_banned(tid)
                    skipped = now < self._market_skip_until.get(tid, 0.0)
                    cached_meta = self._market_meta_cache.get(tid)
                    market_meta = cached_meta[0] if cached_meta else {}
                    start_blocked, start_reason, start_ts = self._market_start_guard_status(
                        tid,
                        meta=market_meta,
                        now_ts=now,
                    )
                    if banned:
                        status = "banned"
                    elif skipped:
                        status = "skipped"
                    elif tob:
                        status = "active"
                    else:
                        status = "waiting"

                    snap = self._market_snapshots.get(tid)
                    gate = self._gate_decisions.get(tid)
                    vol_tracker = self._volatility_tracker.get(tid, {})
                    markets_out[tid] = {
                        "mid": mid,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "reward_lower": reward_lower,
                        "reward_upper": reward_upper,
                        "orders": orders_out,
                        "last_quote_ts": self.last_quote_ts.get(tid),
                        "status": status,
                        "event_state": event_state,
                        "event_reason": self._event_state_entry(tid).get("reason"),
                        "game_start_ts": start_ts,
                        "seconds_to_start": round(start_ts - now, 1) if start_ts is not None else None,
                        "start_guard_blocked": start_blocked,
                        "start_guard_reason": start_reason or None,
                        "legacy_offline_until": self._event_banned_until.get(self._event_key(tid)),
                        "snapshot_source": snap.source if snap else None,
                        "snapshot_age_ms": round((now - snap.last_update_ts) * 1000.0, 1) if snap else None,
                        "gate": gate,
                        "is_night_market": tid in self._night_market_cfg,
                        "vol_defense_actions_60s": len(vol_tracker.get("defense_actions", [])),
                        "vol_watch_enter_ts": vol_tracker.get("watch_enter_ts"),
                        "vol_quarantine_enter_ts": vol_tracker.get("quarantine_enter_ts"),
                    }

                current_session = self._current_session()
                state = {
                    "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "balance": float(self._last_balance) if self._last_balance is not None else None,
                    "quotes_sent": self._quotes_sent,
                    "fills_seen": self._fills_seen,
                    "cooldown_active": now < self._cooldown_until,
                    "current_session": current_session,
                    "session_enabled": self._session_enabled,
                    "markets": markets_out,
                    "fills": list(self._fills_record[-100:]),
                    "pending_unwinds": list(self._pending_unwinds),
                    "night_markets_count": len(self._night_market_cfg),
                    "banned_tokens": [
                        tid for tid in all_markets if self._event_is_banned(tid)
                    ],
                    "latency_records": list(self._latency_records[-50:]),
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

    def send_fill_discord(self, message: str) -> None:
        webhook = getattr(self, "fill_discord_webhook", "") or getattr(self, "discord_webhook", "")
        if not webhook:
            return
        try:
            requests.post(webhook, json={"content": message}, timeout=8)
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(PolyLPSMulti(config_path="config.json").run())
