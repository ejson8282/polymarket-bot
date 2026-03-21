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

import requests
import websockets
from py_clob_client.client import ClobClient
from scanner import normalize_market


HTTP_PROXIES = None       # read operations: book queries, gamma API, meta — routed through proxy pool
HTTP_PROXIES_WRITE = None  # write operations: cancel, place order — always direct (None)
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
        self.cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        _init_proxy_settings(self.cfg)

        account = self.cfg.get("account", {})
        host = self.cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
        chain_id = int(account.get("chain_id", 137))
        signature_type = int(account.get("signature_type", 0))
        self.signature_type = signature_type
        funder = str(account.get("funder", "")).strip()

        signer_server_url = str(account.get("signer_server_url", "")).strip()
        self.remote_signer: RemoteSignerClient | None = None

        if signer_server_url:
            # --- Remote signer mode: private key lives on Mac Mini ---
            log(f">>> REMOTE SIGNER MODE: using Mac Mini at {signer_server_url} (private key is NOT on this machine)")
            signer_token = os.getenv("SIGNER_TOKEN", "").strip() or str(account.get("signer_token", "")).strip()
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
            # --- Local signer mode: backward-compatible ---
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
                "session": str(m.get("session", "both")).lower(),
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
        self._tick_resolved: set[str] = set()

        # execution pacing: risk actions immediate, normal posting lightly paced
        execution = self.cfg.get("execution", {})

        self._cooldown_until = 0.0
        self._running = True
        self._fills_seen = 0
        self._quotes_sent = 0
        self._balance_fail_streak = 0
        self._balance_cache_ttl_sec = float(execution.get("balance_cache_ttl_sec", 3.0))
        self._balance_cache: tuple[Optional[Decimal], float] = (None, 0.0)
        self.max_balance_fail_streak = int(risk.get("max_balance_fail_streak", 8))
        # {token_id: (anchor_value, timestamp)} — TTL-based
        self._anchor_cache: Dict[str, tuple] = {}

        # per-market failure isolation (do not nuke all events on single-market balance issues)
        self._market_balance_fail_streak: Dict[str, int] = {tid: 0 for tid in self.market_cfg}
        self._market_skip_until: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}

        self.post_delay_min_sec = float(execution.get("post_delay_min_sec", 1))
        self.post_delay_max_sec = float(execution.get("post_delay_max_sec", 3))
        self.signer_max_concurrency = max(1, int(execution.get("signer_max_concurrency", 1)))
        self.signer_requote_gap_sec = float(execution.get("signer_requote_gap_sec", 1.2))
        self._signer_sem = asyncio.Semaphore(self.signer_max_concurrency)
        self._signer_gap_lock = asyncio.Lock()
        self._last_signer_post_ts = 0.0

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
        # {token_id: (meta_dict, timestamp)} — TTL prevents stale reward/spread data
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
        # ordered insertion list — used for correct FIFO truncation (set is unordered)
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
        # per-token rolling window: {token_id: {"front_notional_history": [(ts, val)], "defense_actions": [(ts, action)], "watch_enter_ts": float, "quarantine_enter_ts": float}}
        self._volatility_tracker: Dict[str, Dict[str, Any]] = {
            tid: {"front_notional_history": [], "defense_actions": [], "bba_prev": None}
            for tid in self.market_cfg
        }

        # --- P1: fill后限价卖出 ---
        exit_cfg = self.cfg.get("exit_strategy", {})
        self._exit_delay_sec: float = float(exit_cfg.get("exit_delay_sec", 5))
        self._exit_timeout_sec: float = float(exit_cfg.get("exit_timeout_sec", 300))
        self._exit_retry_count: int = int(exit_cfg.get("retry_count", 2))

        # --- P2: 日盘/夜盘 session mode (redesigned: day=scan markets, night=night_markets) ---
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
        live_orders = await self._refresh_live_orders(token_id)
        if live_orders:
            await self._cancel_order_ids(
                token_id,
                [self._order_id(o) for o in live_orders],
                f"start_guard:{trigger}",
            )
            self._market_live_orders[token_id] = await self._refresh_live_orders(token_id)
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
            front_notional = self._front_notional_from_snapshot(depth_snapshot, effective_snapshot.best_bid)
            if front_notional < self.min_front_bid_notional_usdc:
                return False, "front_depth_thin"
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
        front_notional = self._front_notional_from_snapshot(depth_snapshot or effective_snapshot, probe_price)
        decision["front_notional"] = float(front_notional)
        fill_risk = float(meta.get("fill_risk") or 0.0)
        decision["fill_risk"] = fill_risk
        if latency["level"] == "degraded":
            decision["size_cap"] = min(float(decision["size_cap"]), 0.5)
            decision["risk_grade"] = "B"
            reasons.append("latency_degraded")
        if front_notional < (self.min_front_bid_notional_usdc * Decimal("0.50")):
            decision.update({"can_quote": False, "size_cap": 0.0, "top_leg_action": "cancel", "risk_grade": "BLOCK"})
            reasons.append("front_depth_critical")
            return decision
        if front_notional < self.min_front_bid_notional_usdc:
            decision.update({"size_cap": min(float(decision["size_cap"]), 0.25), "top_leg_action": "move_back", "risk_grade": "C"})
            reasons.append("front_depth_thin")
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
    # P0: Watch / Quarantine — rapid-change market detection
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
        """Enter WATCH state: cancel all orders, start observation timer."""
        tracker = self._vol_tracker(token_id)
        tracker["watch_enter_ts"] = time.time()
        self._set_event_state(token_id, EVENT_WATCH, reason)
        live = await self._refresh_live_orders(token_id)
        ids = [self._order_id(o) for o in live]
        if ids:
            await self._cancel_order_ids(token_id, ids, f"watch:{reason}")
        log(f"[watch] token={token_id} entered WATCH reason={reason} duration={self._vol_watch_duration_sec}s")

    async def _enter_quarantine(self, token_id: str, reason: str) -> None:
        """Enter QUARANTINE state: cancel all orders, longer cooldown."""
        tracker = self._vol_tracker(token_id)
        tracker["quarantine_enter_ts"] = time.time()
        self._set_event_state(token_id, EVENT_QUARANTINE, reason)
        live = await self._refresh_live_orders(token_id)
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
    # P1: Fill后限价卖出 — exit strategy
    # ---------------------------------------------------------------

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
                asyncio.create_task(self._monitor_exit_order(token_id, order_id, sell_price, sell_size, reason))
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
                        self.send_discord(f"[EXIT COMPLETE] token={token_id} position sold")
                        return
                    else:
                        log(f"[exit] token={token_id} order gone but position={new_position}, needs manual review")
                        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, "exit_order_gone_position_remains")
                        self.send_discord(
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
    # P2: 日盘/夜盘 session mode (redesigned)
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
            from datetime import timezone, timedelta
            nh_start_h, nh_start_m = map(int, self._session_night_start.split(":"))
            nh_end_h, nh_end_m = map(int, self._session_night_end.split(":"))

            if "shanghai" in self._session_tz.lower() or "beijing" in self._session_tz.lower():
                tz_offset = timezone(timedelta(hours=8))
            else:
                tz_offset = timezone(timedelta(hours=8))
            now = datetime.now(tz_offset)
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

        log(f"[session] === SESSION SWITCH: {prev} → {current} ===")
        self.send_discord(f"[SESSION] Switching from {prev} to {current}")

        # Cancel all orders from the previous session's markets
        prev_markets = self._night_market_cfg if prev == "night" else self.market_cfg
        for token_id in list(prev_markets.keys()):
            state = self._event_state_name(token_id)
            if state in {EVENT_HALTED_ON_FILL, EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT}:
                continue
            try:
                live = await self._refresh_live_orders(token_id)
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
        live = await self._refresh_live_orders(token_id)
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
            live = await self._refresh_live_orders(token_id)
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
        pace_label = label.lower()
        if "top_leg_defense" in pace_label:
            lo, hi = 0.0, 1.0
        else:
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

        mcfg = self._get_mcfg(token_id)
        mcfg["tick"] = resolved
        # keep min distance at least one tick
        mcfg["min_distance"] = max(mcfg["min_distance"], resolved)
        self._tick_resolved.add(token_id)
        log(f"[tick-auto] {token_id}: tick={resolved}")

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
        Runs independently of the quote cycle — covers cooldowns, skips, and gaps.

        Per-token check interval: each token_id is only book-fetched every
        guard_per_token_interval_sec to limit API load when many markets are active.
        """
        guard_interval = 2.0          # seconds between outer loop ticks
        per_token_interval = 10.0     # min seconds between book fetches per token
        _last_guard_ts: dict[str, float] = {}

        while self._running:
            try:
                # Market-WS down detection: if no message received for too long,
                # cancel all orders — we are blind to market changes.
                if self._last_market_ws_ok_ts > 0:
                    market_ws_age = time.time() - self._last_market_ws_ok_ts
                    if market_ws_age > self._market_ws_down_cancel_sec:
                        try:
                            await asyncio.to_thread(self.client.cancel_all)
                            log(f"[guard-loop] market-ws down {market_ws_age:.0f}s > {self._market_ws_down_cancel_sec:.0f}s — cancelled all orders")
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

        # Prefer live rewardsMaxSpread from API; fall back to config value
        spread = live_spread if live_spread is not None else cfg["spread"]
        if spread > Decimal("1"):
            spread = spread / Decimal("100")

        # Valid range: [reward_lower, best_bid - 1 tick]
        # best_bid itself is NEVER included — it's a fill-risk boundary
        reward_lower = max(tick, book.mid - spread)
        safe_top = book.best_bid - tick  # ceiling: best_bid - 1 tick

        if safe_top < reward_lower or safe_top < tick:
            if book.best_bid >= reward_lower and book.best_bid >= tick:
                return [self._floor_to_tick(book.best_bid, tick)]
            # No valid position exists in reward zone; skip this market
            log(f"[price-legs-skip] token={token_id[:16]}... bid={book.best_bid} ask={book.best_ask} mid={book.mid} spread={spread} reward_lower={reward_lower} safe_top={safe_top} tick={tick}")
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
        if current_top is not None:
            await self._cancel_order_ids(token_id, [self._order_id(current_top)], "planner_top_leg_sync")
            live_orders = await self._refresh_live_orders(token_id)
        if desired is not None:
            price, size, _ = desired
            await self.place_post_only_order(token_id, price, size, label="top_leg_sync")
            live_orders = await self._refresh_live_orders(token_id)
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
        ids = [self._order_id(o) for o in live_back]
        if ids:
            await self._cancel_order_ids(token_id, ids, "planner_back_legs_sync")
            live_orders = await self._refresh_live_orders(token_id)
        for price, size, _ in desired_back:
            await self.place_post_only_order(token_id, price, size, label="back_leg_sync")
        live_orders = await self._refresh_live_orders(token_id)
        self._last_back_plan_sig[token_id] = desired_sig
        return live_orders

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
            if lock.locked():
                return
            halt_reason: Optional[str] = None
            self._ensure_order_path_open(token_id, "top_leg_defense_enter")
            async with lock:
                live_orders = self._sorted_live_orders(self._market_live_orders.get(token_id, []))
                if not live_orders:
                    return
                top_order = live_orders[0]
                tick = self._get_mcfg(token_id).get("tick", Decimal("0.01"))
                top_price = self._order_price(top_order)
                top_size = self._order_size(top_order)
                live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
                live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
                legal_prices = self._build_price_legs(token_id, TopOfBook(best_bid=best_bid, best_ask=best_ask), live_spread=live_spread)
                legal_top = legal_prices[0] if legal_prices else None
                depth_snapshot = self._trusted_depth_for_snapshot(token_id, snap)
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
                elif top_price > legal_top:
                    action = "MOVE_BACK_TOP_LEG" if legal_top > 0 and legal_top < best_ask else "CANCEL_TOP_LEG"
                # --- P0: record defense action for volatility tracker ---
                self._vol_record_defense_action(token_id, action)

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
                    self._market_live_orders[token_id] = await self._refresh_live_orders(token_id)
                else:
                    await self._cancel_order_ids(token_id, [self._order_id(top_order)], f"{trigger}:move_top")
                    self._ensure_order_path_open(token_id, "top_leg_defense_after_cancel")
                    if legal_top is None or legal_top <= 0 or legal_top >= best_ask:
                        halt_reason = f"unsafe_move_back:{trigger}"
                    else:
                        await self.place_post_only_order(token_id, legal_top, top_size, label="top_leg_defense")
                        self._market_live_orders[token_id] = await self._refresh_live_orders(token_id)
                self._emit_latency_record(token_id, "top_leg_defense", {"trigger": trigger, "action": action})
                if halt_reason is None:
                    self._set_event_state(token_id, EVENT_ACTIVE, f"defense_complete:{trigger}")
            if halt_reason is not None:
                await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, halt_reason, halt_key="t_detect")
        except EventHaltPreempted as exc:
            log(f"[preempt] token={token_id} path=top_leg_defense reason={exc}")
        except asyncio.CancelledError:
            log(f"[preempt] token={token_id} path=top_leg_defense reason=task_cancelled")
            raise
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
            asyncio.create_task(self._attempt_exit_sell(token_id, fill_price, fill_size, reason))

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
                    while self._running:
                        raw = await self._recv_ws_message(ws, "market-ws")
                        self._last_market_ws_ok_ts = time.time()
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
                                asyncio.create_task(self._maybe_run_top_leg_defense(token_id, "market_ws:book", snap))
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
                                asyncio.create_task(self._maybe_run_top_leg_defense(token_id, "market_ws:best_bid_ask", snap))
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
                                    asyncio.create_task(self._maybe_run_top_leg_defense(token_id, "market_ws:price_change", snap))
            except Exception as e:
                log(f"[market-ws] err={_format_exc(e)}")
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
                log("[recovery] recovery gate passed, auto-resuming quoting")
                self.send_discord("[ALERT] Recovery conditions satisfied. Auto-resuming quoting.")
            if self._event_is_banned(token_id):
                # --- P0: auto-recover from WATCH/QUARANTINE if timer expired ---
                if self._vol_check_recovery(token_id):
                    prev_state = self._event_state_name(token_id)
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
            blocked, breason = await self._is_blocked_market(token_id)
            if blocked:
                await self._deactivate_market(token_id, breason)
                return
            meta = await self._get_market_meta(token_id)
            if await self._enforce_start_guard(token_id, meta=meta, trigger="update_and_quote_market"):
                return
            if self._event_blocks_quote(token_id):
                return
            now = time.time()
            if (now - self.last_quote_ts[token_id]) * 1000 < self.requote_interval_ms:
                return
            book = self._shared_book_cache.get(token_id) if self._shared_book_cache is not None else None
            if book is None:
                try:
                    book = await asyncio.to_thread(self.client.get_order_book, token_id)
                except Exception as e:
                    snap = self._fresh_valid_snapshot(token_id)
                    if snap is not None:
                        age_ms = round((time.time() - snap.last_update_ts) * 1000.0, 1)
                        log(
                            f"[book-loop] token={token_id} keep_last_snapshot source={snap.source} "
                            f"age_ms={age_ms} cause={_format_exc(e)}"
                        )
                        return
                    raise
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
                source="rest",
            )
            effective_snapshot = self._effective_snapshot_for_gate(token_id, snapshot)
            if effective_snapshot is not None:
                tob = TopOfBook(best_bid=effective_snapshot.best_bid, best_ask=effective_snapshot.best_ask)
            depth_snapshot = self._trusted_depth_for_snapshot(token_id, effective_snapshot)
            can_quote, gate_reason = self._quote_gate(token_id, effective_snapshot)
            if not can_quote:
                live_token = await self._refresh_live_orders(token_id)
                if live_token:
                    self._mark_latency(token_id, "t_detect")
                    self._mark_latency(token_id, "t_decision")
                    self._set_event_state(token_id, EVENT_DEFENSIVE, f"quote_gate:{gate_reason}")
                    await self._cancel_order_ids(token_id, [self._order_id(o) for o in live_token], f"quote_gate:{gate_reason}")
                    self._market_live_orders[token_id] = await self._refresh_live_orders(token_id)
                if gate_reason in {"snapshot_stale", "crossed_or_empty_book"}:
                    await self._request_event_halt(token_id, EVENT_HALTED_ON_DATA, f"quote_gate:{gate_reason}", halt_key="t_detect")
                return
            reward_min_size = Decimal(str(meta.get("rewardsMinSize") or 0))
            live_spread_raw = meta.get("maxIncentiveSpread") or meta.get("rewardsMaxSpread")
            live_spread = Decimal(str(live_spread_raw)) if live_spread_raw is not None else None
            prices = self._build_price_legs(token_id, tob, live_spread=live_spread)
            gate = self._feasibility_gate(token_id, meta, effective_snapshot, top_price=prices[0] if prices else None)
            self._gate_decisions[token_id] = gate
            if not gate.get("can_quote", False):
                live_token = await self._refresh_live_orders(token_id)
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
            weights = self._alloc_weights(len(prices))
            viable_legs = []
            total_weight = Decimal("0")
            for p, w in zip(prices, weights):
                front_notional = self._front_notional_from_snapshot(depth_snapshot, p) if depth_snapshot is not None else self._front_bid_notional(book, p)
                if front_notional < self.min_front_bid_notional_usdc:
                    continue
                viable_legs.append((p, w))
                total_weight += w
            if not viable_legs or total_weight <= 0:
                live_token = await self._refresh_live_orders(token_id)
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
            min_weight = min(w for _, w in viable_legs)
            min_price = min(p for p, _ in viable_legs)
            min_size_needed = max(required_min_size, Decimal("0.001"))
            min_budget_needed = (min_price * min_size_needed / min_weight) if min_weight > 0 else Decimal("0")
            budget_divisor = min(max(pct * size_cap, Decimal("0.0001")), Decimal("0.98"))
            avail = await self._get_collateral_available()
            if avail is not None:
                self._last_balance = avail
            if avail is None or avail <= 0:
                log(f"[quote-skip] token={token_id} reason=no_balance_available")
                return
            event_budget = min(avail * pct, avail * Decimal("0.98")) * size_cap
            if event_budget <= 0 or avail < (min_budget_needed / budget_divisor):
                log(f"[quote-skip] token={token_id} reason=insufficient_budget_for_min_size")
                return
            plan = []
            for p, w in viable_legs:
                leg_notional = event_budget * w
                size = self._floor_to_tick(leg_notional / p, Decimal("0.001")) if p > 0 else Decimal("0")
                notional = p * size
                if size >= required_min_size and size > 0 and notional > 0:
                    plan.append((p, size, notional))
            live_token = await self._refresh_live_orders(token_id)
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
                # race condition — it fetches a fresh book milliseconds after placing,
                # and micro-movements make _build_price_legs compute a different
                # legal_top, triggering instant cancellation. The background
                # best_bid_guard_loop (every 10s per token) already provides
                # the same safety check with a stable book.
            except Exception as e:
                if isinstance(e, EventHaltPreempted):
                    log(f"[preempt] token={token_id} path=planner reason={e}")
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
        await self._post_delay(f"{label} token={token_id}")
        self._ensure_order_path_open(token_id, f"place_post_delay:{label}")
        meta = await self._get_market_meta(token_id)
        if await self._enforce_start_guard(token_id, meta=meta, trigger=f"post_delay_complete:{label}"):
            raise RuntimeError(f"market_start_blocked token={token_id}")

        async with self._signer_sem:
            async with self._signer_gap_lock:
                now = time.time()
                wait_sec = max(0.0, self.signer_requote_gap_sec - (now - self._last_signer_post_ts))
                if wait_sec > 0:
                    log(f"[signer-pace] token={token_id} label={label} sleep={wait_sec:.2f}s")
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
                    log(f"[fill-ws] connected markets={len(condition_ids)} assets={len(self.market_cfg)}")
                    self._last_ws_ok_ts = time.time()
                    ws_down_since = 0.0
                    backoff = 1

                    while self._running:
                        raw = await self._recv_ws_message(ws, "fill-ws")
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
                log(f"[fill-ws] err={_format_exc(e)}")
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
            return -1.0  # unknown — don't act on error

    async def unwind_tracking_loop(self) -> None:
        """Periodically check pending unwind SELL orders.
        - If position is 0 → already sold (manually or filled), cancel residual order, remove.
        - If the order is no longer in live orders → assume filled, remove.
        - If age > unwind_max_age_sec and still open → Discord alert for manual review.
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
                        # Position gone — cancel any residual sell order and clear
                        if oid and oid in live_ids:
                            try:
                                await asyncio.to_thread(self.client.cancel, oid)
                                log(f"[unwind] position=0, canceled residual order={oid}")
                            except Exception as ce:
                                log(f"[unwind] cancel residual failed order={oid} err={ce}")
                        log(f"[unwind] cleared token={token_id} position=0 age={age:.0f}s (manually sold or filled)")
                        continue

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


if __name__ == "__main__":
    asyncio.run(PolyLPSMulti(config_path="config.json").run())
