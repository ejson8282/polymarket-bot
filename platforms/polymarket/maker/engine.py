import asyncio
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
import random
import re
import socket
import threading
import time
import urllib.request
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_CEILING, ROUND_DOWN
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional
from zoneinfo import ZoneInfo

import contextvars
import httpx
import requests
import websockets
from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.http_helpers import helpers as _hh
from scanner import normalize_market


HTTP_PROXIES = None       # legacy fallback; per-engine self._http_proxies_dict takes precedence
HTTP_PROXIES_WRITE = None  # unused; retained for backward-compat with external imports
WS_PROXY = None            # legacy fallback; per-engine self._ws_proxy takes precedence
from py_clob_client_v2.clob_types import AssetType, BalanceAllowanceParams, OrderArgs, OrderType
from py_clob_client_v2.order_builder.constants import BUY, SELL
from remote_signer import AddressStub, BuilderStub, RemoteSignerClient
from event_bus import EventBus
from cross_side_sentinel import CrossSideSentinel
from sibling_registry import SiblingOrderRegistry, resolve_conflict
try:
    from .account_profiles import (
        LPAccountProfile,
        parse_lp_account_profile,
        shared_event_owner,
    )
except ImportError:
    from account_profiles import (
        LPAccountProfile,
        parse_lp_account_profile,
        shared_event_owner,
    )
try:
    from .sponsored_guard import SponsoredRiskGuard
except ImportError:
    from sponsored_guard import SponsoredRiskGuard
try:
    from .aggressive_guardrails import AggressiveGuardrailState
except ImportError:
    from aggressive_guardrails import AggressiveGuardrailState
try:
    from .aggressive_recovery import evaluate_recovery_sample
except ImportError:
    from aggressive_recovery import evaluate_recovery_sample
try:
    from .order_scoring_observer import OrderScoringObserver
except ImportError:
    from order_scoring_observer import OrderScoringObserver
try:
    from .quote_feasibility import (
        compute_quote_target_shares as _shared_compute_quote_target_shares,
        distance_score as _shared_distance_score,
        executable_quote_boundary,
    )
except ImportError:
    from quote_feasibility import (
        compute_quote_target_shares as _shared_compute_quote_target_shares,
        distance_score as _shared_distance_score,
        executable_quote_boundary,
    )
try:
    from .stable_rotation_commands import (
        confirmation_for_replacement,
        find_confirmed_replacement,
    )
except ImportError:
    from stable_rotation_commands import (
        confirmation_for_replacement,
        find_confirmed_replacement,
    )
try:
    from .stable_lifecycle_commands import (
        normalized_lifecycle_config,
        validate_stable_lifecycle_command,
    )
except ImportError:
    from stable_lifecycle_commands import (
        normalized_lifecycle_config,
        validate_stable_lifecycle_command,
    )
try:
    from .stable_market_lifecycle import (
        MAX_ACTIVE_CANARIES_LIMIT,
        MAX_CANARY_PRINCIPAL_FRACTION,
        MAX_CANARY_USDC,
        MAX_PROMOTION_SCORING_SAMPLE_AGE_SEC,
        MIN_PROMOTION_SCORING_SAMPLES,
        STATE_VERSION as STABLE_LIFECYCLE_STATE_VERSION,
        account_admission as stable_lifecycle_account_admission,
        build_lifecycle_plan,
        candidate_is_executable_for_account,
    )
except ImportError:
    from stable_market_lifecycle import (
        MAX_ACTIVE_CANARIES_LIMIT,
        MAX_CANARY_PRINCIPAL_FRACTION,
        MAX_CANARY_USDC,
        MAX_PROMOTION_SCORING_SAMPLE_AGE_SEC,
        MIN_PROMOTION_SCORING_SAMPLES,
        STATE_VERSION as STABLE_LIFECYCLE_STATE_VERSION,
        account_admission as stable_lifecycle_account_admission,
        build_lifecycle_plan,
        candidate_is_executable_for_account,
    )
try:
    from .exchange_maintenance import (
        ExchangeMaintenanceGuard,
        PHASE_MAINTENANCE,
    )
except ImportError:
    from exchange_maintenance import (
        ExchangeMaintenanceGuard,
        PHASE_MAINTENANCE,
    )


# Per-engine httpx routing. py_clob_client uses a single module-global
# httpx.Client; when multiple engines run in one process (multi_runner) they'd
# all share it — last-writer-wins on proxy config, all traffic leaks out of
# one IP. We install a dispatcher in its place that resolves to a contextvar
# so each engine's call routes to its own httpx.Client (and hence its own
# proxy/IP).
_current_httpx_client: contextvars.ContextVar = contextvars.ContextVar(
    "_poly_current_httpx_client", default=None
)


class _DispatchingHttpxClient:
    def __init__(self, default: httpx.Client):
        self._default = default

    def _pick(self) -> httpx.Client:
        return _current_httpx_client.get() or self._default

    def request(self, *a, **kw):
        return self._pick().request(*a, **kw)

    def get(self, *a, **kw):
        return self._pick().get(*a, **kw)

    def post(self, *a, **kw):
        return self._pick().post(*a, **kw)

    def put(self, *a, **kw):
        return self._pick().put(*a, **kw)

    def delete(self, *a, **kw):
        return self._pick().delete(*a, **kw)

    def close(self) -> None:
        # Lifecycle is owned by individual engines; ignore library-side closes.
        pass


_hh._http_client = _DispatchingHttpxClient(httpx.Client(http2=False))


class _ProxiedClobClient:
    """Wraps a ClobClient so every method call binds a per-engine httpx.Client
    into py_clob_client's module-global via contextvars for the duration of
    the call. Attribute reads/writes pass through to the inner client."""

    def __init__(self, inner: ClobClient, httpx_client: httpx.Client):
        object.__setattr__(self, "_inner", inner)
        object.__setattr__(self, "_httpx", httpx_client)

    def __getattr__(self, name: str):
        attr = getattr(object.__getattribute__(self, "_inner"), name)
        if not callable(attr) or name.startswith("_"):
            return attr
        httpx_client = object.__getattribute__(self, "_httpx")

        def wrapped(*args, **kwargs):
            token = _current_httpx_client.set(httpx_client)
            try:
                result = attr(*args, **kwargs)
                # V2 SDK returns dicts for both book endpoints; engine code
                # consumes OrderBookSummary attributes.
                if name == "get_order_book" and isinstance(result, dict):
                    from py_clob_client_v2.clob_types import OrderBookSummary
                    return OrderBookSummary(**result)
                if name == "get_order_books" and isinstance(result, list):
                    from py_clob_client_v2.clob_types import OrderBookSummary
                    return [
                        OrderBookSummary(**book) if isinstance(book, dict) else book
                        for book in result
                    ]
                return result
            finally:
                _current_httpx_client.reset(token)

        return wrapped

    def __setattr__(self, name: str, value) -> None:
        setattr(object.__getattribute__(self, "_inner"), name, value)


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

_TERMINAL_ORDER_STATUSES = frozenset(
    {
        "cancelled",
        "canceled",
        "closed",
        "expired",
        "filled",
        "matched",
    }
)


def _order_is_live(order: Any) -> bool:
    """Treat every non-terminal record from get_open_orders as still live."""
    if not isinstance(order, dict):
        return False
    status = str(order.get("status") or "").strip().lower()
    return status not in _TERMINAL_ORDER_STATUSES


class EventHaltPreempted(RuntimeError):
    pass


# Sports game slugs reliably contain an explicit game date (YYYY-MM-DD).
# Non-sports prediction markets with gameStartTime populated (e.g. geopolitical
# resolution deadlines) don't — they use phrases like "before-2027".
_SPORTS_SLUG_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


class SoftQuoteSkip(RuntimeError):
    pass


# Account-id context var — set by PolyLPSMulti.run() so log/notify calls
# made anywhere in that engine's async task tree inherit the prefix. Module
# multi_runner reads it too when emitting its own messages for a given account.
_current_account_idx_ctx: contextvars.ContextVar[int] = contextvars.ContextVar(
    "polylps_current_account_idx", default=0
)


def _account_prefix() -> str:
    try:
        idx = _current_account_idx_ctx.get()
    except LookupError:
        return ""
    return f"[{idx}号] " if idx > 0 else ""


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {_account_prefix()}{msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        safe = line.encode("gbk", errors="ignore").decode("gbk", errors="ignore")
        print(safe, flush=True)


# ---------------------------------------------------------------------------
# Discord webhook notification system (fire-and-forget, rate-limited)
# ---------------------------------------------------------------------------
_notify_cooldowns: dict[str, float] = {}
_NOTIFY_COOLDOWN_SEC = 60  # max 1 message per event type per 60s
_discord_normal_webhook_file: Optional[Path] = None
_discord_important_webhook_file: Optional[Path] = None


def _read_webhook_file(path: Path) -> str:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""
    return value if value.startswith("https://") else ""


def _is_important_discord_message(message: str) -> bool:
    text = str(message or "").lower()
    markers = (
        " failed",
        "failure",
        "失败",
        "需手动",
        "manual review",
        "资金不足",
        "balance drop",
        "kill switch",
        "安全超时",
        "拒绝低价",
        "dust remains",
        "recovered",
        "resolved",
        "恢复",
        "解除",
    )
    return any(marker in text for marker in markers)


def _discord_webhook_for(channel: str) -> str:
    path = (
        _discord_important_webhook_file
        if channel == "important"
        else _discord_normal_webhook_file
    )
    return _read_webhook_file(path) if path is not None else ""


def _discord_embed_color(level: str) -> int:
    return {"info": 0x2ECC71, "warning": 0xF1C40F, "danger": 0xE74C3C}.get(level, 0x95A5A6)


def _discord_description(message: Any) -> str:
    """Render structured payloads as readable lines, never raw JSON."""
    if isinstance(message, str):
        return message
    if isinstance(message, dict):
        return "\n".join(
            f"{key}：{value}"
            for key, value in message.items()
            if value is not None and value != ""
        )
    if isinstance(message, (list, tuple, set)):
        return "\n".join(f"- {value}" for value in message)
    return str(message)


def _send_discord_webhook(url: str, title: str, message: Any, level: str) -> None:
    """Blocking HTTP POST to Discord webhook — runs in daemon thread."""
    try:
        description = _discord_description(message)
        payload = {
            "embeds": [{
                "title": title,
                "description": description[:3900],
                "color": _discord_embed_color(level),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }]
        }
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass  # silently ignore — never disrupt the engine


def notify_discord(title: str, message: str, level: str = "info") -> None:
    """Fire-and-forget Discord notification with per-event-type rate limiting.

    Safe to call from anywhere — instant no-op if webhook is not configured
    or if the same event type was notified within the cooldown window.
    """
    important = level in {"warning", "danger"}
    webhook = _discord_webhook_for("important" if important else "normal")
    if not webhook:
        return
    now = time.time()
    cooldown_key = f"{title}:{level}"
    last_sent = _notify_cooldowns.get(cooldown_key, 0.0)
    if now - last_sent < _NOTIFY_COOLDOWN_SEC:
        return
    _notify_cooldowns[cooldown_key] = now
    t = threading.Thread(
        target=_send_discord_webhook,
        args=(webhook, title, message, level),
        daemon=True,
    )
    t.start()


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


def _restore_activity_records(
    prior_state: object,
    account_index: int,
    *,
    limit: int = 100,
) -> tuple[list[dict], list[dict]]:
    """Restore display-only fill/exit history without reviving pending actions."""
    if not isinstance(prior_state, dict):
        return [], []
    prior_account = prior_state.get("account_index")
    if prior_account is not None:
        try:
            if int(prior_account) != account_index:
                return [], []
        except (TypeError, ValueError):
            return [], []

    def recent_dicts(value: object) -> list[dict]:
        if not isinstance(value, list):
            return []
        return [dict(row) for row in value if isinstance(row, dict)][-limit:]

    fills = recent_dicts(prior_state.get("fills"))
    exits = recent_dicts(prior_state.get("exit_records"))

    # Before account-order attribution was added, TRADES_POLL stored the
    # aggregate transaction size. Do not restore those display-only records or
    # a matching exit computed from the same untrusted size.
    tainted_fills = [
        row
        for row in fills
        if str(row.get("reason") or "").startswith("TRADES_POLL:")
        and row.get("size_source") != "account_order_fill"
    ]
    fills = [row for row in fills if row not in tainted_fills]

    def matches_tainted_fill(exit_row: dict) -> bool:
        if exit_row.get("size_source"):
            return False
        try:
            exit_size = float(exit_row.get("size") or 0)
            exit_ts = float(exit_row.get("ts") or 0)
            exit_fill_price = float(exit_row.get("fill_price") or 0)
        except (TypeError, ValueError):
            return False
        for fill_row in tainted_fills:
            try:
                fill_size = float(fill_row.get("size") or 0)
                fill_ts = float(fill_row.get("ts") or 0)
                fill_price = float(fill_row.get("price") or 0)
            except (TypeError, ValueError):
                continue
            if fill_size <= 0:
                continue
            size_tolerance = max(0.000001, abs(fill_size) * 0.000000001)
            if abs(exit_size - fill_size) > size_tolerance:
                continue
            same_token = str(exit_row.get("token_id") or "") == str(
                fill_row.get("token_id") or ""
            )
            price_tolerance = 0.000001
            related_price = (
                exit_fill_price > 0
                and fill_price > 0
                and (
                    abs(exit_fill_price - fill_price) <= price_tolerance
                    or abs(exit_fill_price - (1.0 - fill_price))
                    <= price_tolerance
                )
            )
            if not same_token and not related_price:
                continue
            if fill_ts <= 0 or exit_ts <= 0 or (
                fill_ts - 60 <= exit_ts <= fill_ts + 7 * 24 * 60 * 60
            ):
                return True
        return False

    exits = [row for row in exits if not matches_tainted_fill(row)]
    return fills, exits


def _compute_quote_target_shares(
    *,
    available: Decimal,
    rewards_min: Decimal,
    min_order_size: Decimal,
    budget_pct: Decimal,
    size_cap: Decimal,
    max_quote_shares: Decimal,
) -> tuple[Decimal, str]:
    """Backward-compatible wrapper around the shared pure calculation."""
    return _shared_compute_quote_target_shares(
        available=available,
        rewards_min=rewards_min,
        min_order_size=min_order_size,
        budget_pct=budget_pct,
        size_cap=size_cap,
        max_quote_shares=max_quote_shares,
    )


def _stable_lifecycle_safety_limits(
    lifecycle_cfg: Mapping[str, Any],
) -> tuple[int, Decimal, Decimal, int]:
    """Clamp operator config to the stable-LP canary safety contract."""

    return (
        max(
            0,
            min(
                MAX_ACTIVE_CANARIES_LIMIT,
                int(lifecycle_cfg.get("max_active_canaries", 10)),
            ),
        ),
        max(
            Decimal("0"),
            min(
                Decimal(str(MAX_CANARY_PRINCIPAL_FRACTION)),
                Decimal(
                    str(
                        lifecycle_cfg.get(
                            "canary_principal_fraction",
                            "0.10",
                        )
                    )
                ),
            ),
        ),
        max(
            Decimal("0"),
            min(
                Decimal(str(MAX_CANARY_USDC)),
                Decimal(str(lifecycle_cfg.get("canary_max_usdc", "100"))),
            ),
        ),
        max(
            MIN_PROMOTION_SCORING_SAMPLES,
            int(lifecycle_cfg.get("promotion_scoring_threshold", 3)),
        ),
    )


def _stable_order_scoring_observer_enabled(
    *,
    profile_type: str,
    lifecycle_enabled: bool,
    scoring_cfg: Mapping[str, Any],
) -> bool:
    """Stable lifecycle always requires its read-only scoring producer."""

    return bool(
        lifecycle_enabled
        or (
            str(profile_type or "").strip().lower() == "aggressive"
            and scoring_cfg.get("enabled", True)
        )
    )


def _stable_runtime_host_id(config: Mapping[str, Any]) -> str:
    lifecycle = config.get("stable_market_lifecycle")
    if not isinstance(lifecycle, Mapping):
        lifecycle = {}
    runtime = config.get("runtime")
    if not isinstance(runtime, Mapping):
        runtime = {}
    candidate = (
        os.getenv("POLYMARKET_HOST_ID", "").strip()
        or str(lifecycle.get("host_id") or "").strip()
        or str(runtime.get("host_id") or "").strip()
        or socket.gethostname().strip()
    ).lower()
    return candidate if re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,127}", candidate) else ""


def _ws_proxy_diag(ws_proxy: Optional[str] = None) -> str:
    sys_proxies = urllib.request.getproxies() or {}
    sys_proxy = (
        sys_proxies.get("https")
        or sys_proxies.get("http")
        or sys_proxies.get("all")
        or sys_proxies.get("ftp")
    )
    effective_proxy = ws_proxy if ws_proxy is not None else WS_PROXY
    effective = effective_proxy if effective_proxy else "direct"
    forced_direct = effective_proxy is None
    detected = sys_proxy or "none"
    return f"system_proxy={detected} effective_ws_proxy={effective} ws_direct_forced={forced_direct}"


def _choose_proxy(cfg: dict, for_ws: bool, shard_key: str = "") -> str | None:
    """Select a proxy from the pool.

    for_ws=True — WS connections (long-lived)
    for_ws=False — HTTP read operations (book queries, gamma API)

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

    HTTP_PROXIES — read operations (book queries, gamma API calls)
    HTTP_PROXIES_WRITE — write operations (cancel, place order) — always None/direct
    WS_PROXY — WebSocket connections
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

    # NOTE: py_clob_client's module-global `_hh._http_client` is NOT patched
    # here. It is installed once at import as a `_DispatchingHttpxClient` that
    # resolves per-engine via contextvars, so multiple engines in the same
    # process each route REST traffic through their own httpx.Client / proxy.


class PolyLPSMulti:
    """
    PolyLPS-Multi (single-account, multi-market) using py-clob-client.

    - Multi-market book polling (official client)
    - Per-market passive quoting
    - Global kill-switch on detected fill notifications
    - Optional Discord webhook alerts
    """

    def __init__(
        self,
        config_path: str = "config.json",
        *,
        allow_paused_zero_markets: bool = False,
    ) -> None:
        cfg_path = Path(config_path)
        self._config_path = cfg_path.resolve()
        self._config_lock = threading.RLock()
        self._config_lock_state = threading.local()
        self.cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
        self._allow_paused_zero_markets = bool(allow_paused_zero_markets)
        _cfg_stem = cfg_path.stem
        self._account_idx = (
            int(_cfg_stem[7:])
            if _cfg_stem.startswith("config_") and _cfg_stem[7:].isdigit()
            else 0
        )
        self.lp_account_profile: LPAccountProfile = parse_lp_account_profile(
            self.cfg,
            self._account_idx or 1,
        )
        self._shared_account_profiles: Dict[int, LPAccountProfile] = {
            self._account_idx or 1: self.lp_account_profile
        }
        _init_proxy_settings(self.cfg)

        account = self.cfg.get("account", {})
        host = self.cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
        chain_id = int(account.get("chain_id", 137))
        signature_type = int(account.get("signature_type", 0))
        self.signature_type = signature_type
        funder = str(account.get("funder", "")).strip()
        # Persist funder lowercase for fill-ws own-order filtering — Polymarket
        # user channel broadcasts market-wide `trade` events so we need to
        # distinguish "our maker order matched" from "someone else's order
        # matched in a market we subscribe to".
        self._funder_lc: str = funder.lower()
        account_uid = f"{chain_id}:{signature_type}:{self._funder_lc}"
        self._stable_lifecycle_account_uid_key = (
            hashlib.sha256(account_uid.encode("utf-8")).hexdigest()[:16]
            if re.fullmatch(r"0x[0-9a-f]{40}", self._funder_lc)
            else ""
        )
        self._runtime_host_id = _stable_runtime_host_id(self.cfg)

        # 施工包04:跨账号自成交防线。multi_runner 会用共享实例覆盖此默认值;
        # 单账号直跑时是自建空注册表(无兄弟订单,行为不变)。
        _sib_raw = self.cfg.get("sibling_registry") if isinstance(self.cfg.get("sibling_registry"), dict) else {}
        self._sibling_cfg: Dict[str, Any] = {
            "enabled": bool(_sib_raw.get("enabled", True)),
            "mode": str(_sib_raw.get("mode", "observe")).lower(),   # observe|adjust|block
            "adjust_ticks": int(_sib_raw.get("adjust_ticks", 1)),
        }
        self._sibling_registry: SiblingOrderRegistry = SiblingOrderRegistry()

        signer_server_url = os.getenv("POLY_SIGNER_SERVER_URL", "").strip() or str(account.get("signer_server_url", "")).strip()
        self.remote_signer: RemoteSignerClient | None = None

        if signer_server_url:
            # --- Remote signer mode: private key lives on Mac Mini ---
            log(f">>> REMOTE SIGNER MODE: using Mac Mini at {signer_server_url} (private key is NOT on this machine)")
            signer_token = (
                os.getenv("SIGNER_TOKEN", "").strip()
                or str(account.get("signer_token", "")).strip()
            )
            self.remote_signer = RemoteSignerClient(signer_server_url, signer_token, funder=funder or None)
            creds_data = self.remote_signer.derive_creds()
            address = creds_data["address"]
            log(f">>> Remote signer connected, address: {address}")

            # Create ClobClient without private key (L0 read-only + L2 via api_creds)
            self.client = ClobClient(host=host, chain_id=chain_id)
            # Inject AddressStub so client.signer.address() works for HMAC headers
            self.client.signer = AddressStub(address, chain_id)
            # Inject a stub builder so code that accesses self.client.builder won't crash
            self.client.builder = BuilderStub(sig_type=signature_type, funder=funder)

            from py_clob_client_v2.clob_types import ApiCreds
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
            self.api_creds = self.client.create_or_derive_api_key()
            self.client.set_api_creds(self.api_creds)
            # some py_clob_client builds may not expose .builder in local mode
            # but downstream polling paths still reference it indirectly.
            if not hasattr(self.client, "builder"):
                self.client.builder = BuilderStub(sig_type=signature_type, funder=funder)

        # Per-engine HTTP/WS proxy binding. Each engine gets its own
        # httpx.Client routed through its proxy_pool item (typically one
        # dedicated Clash local port). The ClobClient is wrapped so every
        # call binds this engine's client via contextvars — multiple engines
        # in the same process no longer share a single module-global client.
        _read_proxy_url = _choose_proxy(self.cfg, for_ws=False, shard_key=str(funder or ""))
        self._ws_proxy: Optional[str] = _choose_proxy(self.cfg, for_ws=True, shard_key=str(funder or ""))
        if not _read_proxy_url:
            _read_proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""
        _read_proxy_url = _read_proxy_url or None
        self._read_proxy_url: Optional[str] = _read_proxy_url
        self._http_proxies_dict: Optional[dict] = (
            {"http": _read_proxy_url, "https": _read_proxy_url} if _read_proxy_url else None
        )
        _httpx_kwargs: dict = {"http2": False}
        if _read_proxy_url:
            _httpx_kwargs["proxy"] = _read_proxy_url
        self._httpx_client = httpx.Client(**_httpx_kwargs)
        self.client = _ProxiedClobClient(self.client, self._httpx_client)
        log(
            f"[proxy] engine bound read_proxy={_read_proxy_url or 'direct'} "
            f"ws_proxy={self._ws_proxy or 'direct'} funder={str(funder or '-')[:10]}"
        )

        strategy = self.cfg.get("strategy", {})
        risk = self.cfg.get("risk", {})

        self.requote_interval_ms = int(strategy.get("requote_interval_ms", 500))
        self.default_tick = Decimal(str(strategy.get("default_price_tick", 0.1)))
        self.default_min_distance = Decimal(str(strategy.get("default_min_distance_from_best_bid", 0.1)))
        self.default_min_distance_ticks = int(strategy.get("min_distance_ticks", 1))
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
        # Maker BUYs are always exchange-enforced post-only. This is a safety
        # invariant, not a runtime strategy toggle.
        self.post_only = True
        self.auto_tick = bool(strategy.get("auto_tick", True))

        # --- Dynamic budget allocation ---
        self._dynamic_budget_enabled = bool(strategy.get("dynamic_budget_enabled", False))
        self._budget_rebalance_interval_sec = float(strategy.get("budget_rebalance_interval_sec", 60))
        self._budget_min_pct = Decimal(str(strategy.get("budget_min_pct", 0.02)))
        self._budget_max_pct = Decimal(str(strategy.get("budget_max_pct", 0.25)))
        self._last_budget_rebalance_ts: float = 0.0
        self._dynamic_budget_scores: Dict[str, float] = {}

        self.kill_switch_on_fill = bool(risk.get("kill_switch_on_fill", True))
        # Runtime floor: after a fill, if remaining USDC ≥ this, only halt the
        # filled token (and let other markets keep quoting). If below, fall back
        # to the default global halt — protects against cascading fills when
        # liquid balance gets thin. Set to 0 to always do global halt.
        self.runtime_floor_usdc = Decimal(str(risk.get("runtime_floor_usdc", 0)))
        self.max_quote_shares_per_market = Decimal(
            str(risk.get("max_quote_shares_per_market", 0))
        )
        self.max_notional_usdc_per_order = Decimal(
            str(risk.get("max_notional_usdc_per_order", 0))
        )

        # Cross-side sentinel: monitor opposite-token DEPTH depletion (top-N
        # ask depth shrinkage in a short window signals incoming BUY pressure;
        # arbitrageurs will then cross our paired-side BIDs). Pre-emptively
        # cancel our orders on the paired token before they get hit.
        css_cfg = self.cfg.get("cross_side_sentinel", {}) or {}
        self.cross_side_sentinel = CrossSideSentinel(
            enabled=bool(css_cfg.get("enabled", False)),
            dry_run=bool(css_cfg.get("dry_run", True)),
            depth_window_sec=float(css_cfg.get("depth_window_sec", 30.0)),
            depth_levels=int(css_cfg.get("depth_levels", 3)),
            depth_consumed_shares=float(css_cfg.get("depth_consumed_shares", 5000.0)),
            depth_consumed_pct=float(css_cfg.get("depth_consumed_pct", 0.5)),
            cooldown_sec=float(css_cfg.get("cooldown_sec", 60.0)),
            min_baseline_shares=float(css_cfg.get("min_baseline_shares", 2000.0)),
        )
        self._cross_side_cancel_inflight: set[str] = set()

        self.cooldown_seconds = int(risk.get("cooldown_seconds", 60))
        self.start_freeze_seconds = int(risk.get("start_freeze_seconds", 120))
        # Hard pre-start stop: cancel orders this many seconds before event start (default 3h)
        self.pre_start_stop_sec = int(risk.get("pre_start_stop_sec", 3 * 3600))

        default_data_dir = (
            Path(config_path).resolve().parent.parent.parent.parent / "data"
        )
        normal_file = default_data_dir / "discord_normal_webhook.txt"
        important_file = default_data_dir / "discord_important_webhook.txt"
        reporting = self.cfg.get("reporting", {})
        self.hourly_summary = bool(reporting.get("hourly_summary", True))

        # Dashboard > 通知 is the only Discord configuration source. Both files
        # are re-read for every send so a saved change takes effect immediately.
        global _discord_normal_webhook_file, _discord_important_webhook_file
        _discord_normal_webhook_file = normal_file
        _discord_important_webhook_file = important_file

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
                "min_distance_ticks": int(m.get("min_distance_ticks", self.default_min_distance_ticks)),
                "risk": str(m.get("risk", "mid")).lower(),
                "base_risk": str(
                    m.get("eligibility_base_risk") or m.get("risk", "mid")
                ).lower(),
                "session": str(m.get("session", "both")).lower(),
                "paired_token_id": str(m.get("paired_token_id", "")),
                "condition_id": str(m.get("condition_id", "")).strip().lower(),
                "source": str(m.get("source") or "manual"),
                "eligibility_managed": bool(m.get("eligibility_managed", False)),
                "lifecycle_stage": str(m.get("lifecycle_stage") or ""),
                "lifecycle_retire_pending": bool(
                    m.get("lifecycle_retire_pending", False)
                ),
            }

        if not self.market_cfg and not self._allow_paused_zero_markets:
            raise ValueError("No valid enabled markets in config.markets")

        self.market_states: Dict[str, TopOfBook] = {}
        self._market_snapshots: Dict[str, MarketSnapshot] = {}
        self._market_depth_snapshots: Dict[str, MarketSnapshot] = {}
        self._event_states: Dict[str, Dict[str, Any]] = {}
        for tid, market in self.market_cfg.items():
            retire_pending = bool(market.get("lifecycle_retire_pending"))
            self._event_states[tid] = {
                "state": EVENT_WATCH if retire_pending else EVENT_ACTIVE,
                "reason": (
                    "stable_lifecycle_retire_pending" if retire_pending else "init"
                ),
                "updated_at": time.time(),
            }
        self._event_locks: Dict[str, asyncio.Lock] = {tid: asyncio.Lock() for tid in self.market_cfg}
        self._latency_marks: Dict[str, Dict[str, float]] = {tid: {} for tid in self.market_cfg}
        self._latency_records: list[dict] = []
        self._halt_requested: Dict[str, Optional[str]] = {tid: None for tid in self.market_cfg}
        self._top_leg_defense_tasks: Dict[str, asyncio.Task] = {}
        self._top_leg_defense_active: set[str] = set()
        self._top_leg_defense_pending: dict[str, tuple[str, object]] = {}
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
        self._defense_requote_block_sec = float(strategy.get("defense_requote_block_sec", 15))
        # Separate, longer cooldown for MOVE_BACK specifically — when the bid
        # has moved down against us, re-entering at the new lower price within
        # seconds puts us in the path of a sweep (see 2026-04-24 WTA 11:52 fill).
        # Default 240s = 4 min; override via strategy.move_back_requote_block_sec.
        self._move_back_requote_block_sec = float(strategy.get("move_back_requote_block_sec", 240))
        self._tick_resolved: set[str] = set()

        # Churn-lock: planner_*_sync cancels happen when _last_plan_sig jitters (float precision,
        # depth re-weighting, etc). A market in this state cancels+reposts continuously without
        # real price movement, burning gas & gate quota. Count those cancels per token, and if
        # they cross the threshold in the rolling window, lock the planner for this token.
        self._planner_churn_window_sec = float(strategy.get("planner_churn_window_sec", 60.0))
        self._planner_churn_threshold = int(strategy.get("planner_churn_threshold", 3))
        self._planner_churn_lock_sec = float(strategy.get("planner_churn_lock_sec", 600.0))
        self._planner_churn_cancels: Dict[str, list[float]] = {}
        self._planner_churn_locked_until: Dict[str, float] = {}

        # # Dual-side quoting config
        # The engine auto-registers the paired (NO) token for ALL markets
        # that have a paired_token_id.  Both tokens are quoted as independent
        # BUY-side markets — NO BUY = synthetic YES SELL.
        # Q_min optimization: dual-side ≈ +49-200% LP rewards vs single-side.
        dual = strategy.get("dual_side", {})
        self._dual_side_enabled = bool(dual.get("enabled", True))  # default ON
        # Stable LP rewards are event-level: a YES bid without the paired NO
        # bid is materially penalized (and scores zero in the price tails).
        # Keep both quote legs as one operational unit unless explicitly
        # disabled for a non-production experiment.
        self._dual_side_require_both_sides = bool(
            dual.get("require_both_sides", True)
        )
        self._dual_side_max_mid = Decimal(str(dual.get("max_mid", "0.10")))  # low-price threshold for strict both-or-none
        self._dual_side_no_risk = str(dual.get("no_side_risk", "mid")).lower()
        self._dual_side_min_book_depth = Decimal(str(dual.get("min_book_depth_usdc", "500")))
        self._dual_side_injected: set[str] = set()  # NO tokens auto-added
        self._dual_side_insufficient_warned: set[str] = set()  # tokens warned about insufficient funds
        # set() by add_market_runtime / remove_market_runtime to force market WS to re-send
        # subscribe payload with the latest token list (lazily created on first set()).
        self._market_ws_resubscribe_evt: Optional[asyncio.Event] = None
        self._user_ws_resubscribe_evt: Optional[asyncio.Event] = None
        self._runtime_added_tokens: set[str] = set()  # tokens added via add_market_runtime

        # execution pacing: risk actions immediate, normal posting lightly paced
        execution = self.cfg.get("execution", {})

        self._cooldown_until = 0.0
        self._running = True
        self._kill_switch_lock = asyncio.Lock()
        self._exchange_maintenance = ExchangeMaintenanceGuard(
            min_hold_sec=max(
                0.0,
                float(execution.get("exchange_maintenance_min_hold_sec", 60.0)),
            ),
            recovery_read_successes=max(
                1,
                int(execution.get("exchange_maintenance_recovery_reads", 3)),
            ),
            recovery_read_spacing_sec=max(
                1.0,
                float(
                    execution.get(
                        "exchange_maintenance_recovery_read_spacing_sec",
                        10.0,
                    )
                ),
            ),
            cancel_backoff_initial_sec=max(
                5.0,
                float(
                    execution.get(
                        "exchange_maintenance_cancel_backoff_initial_sec",
                        30.0,
                    )
                ),
            ),
            cancel_backoff_max_sec=max(
                30.0,
                float(
                    execution.get(
                        "exchange_maintenance_cancel_backoff_max_sec",
                        300.0,
                    )
                ),
            ),
            buy_probe_retry_sec=max(
                5.0,
                float(
                    execution.get(
                        "exchange_maintenance_buy_probe_retry_sec",
                        30.0,
                    )
                ),
            ),
        )
        self._exchange_maintenance_cancel_task: Optional[asyncio.Task] = None
        self._fills_seen = 0
        self._quotes_sent = 0
        self._balance_fail_streak = 0
        self._balance_cache_ttl_sec = float(execution.get("balance_cache_ttl_sec", 3.0))
        self._balance_cache: tuple[Optional[Decimal], float] = (None, 0.0)
        self._balance_resize_lock = asyncio.Lock()
        self.max_balance_fail_streak = int(risk.get("max_balance_fail_streak", 8))
        # {token_id: (anchor_value, timestamp)} — TTL-based
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
        # Hard reserve guard: refuse a new BUY whose notional would push
        # (live BUY notional + new notional + margin) past available USDC/allowance.
        # Margin absorbs balance-cache lag and signer-side rounding so we don't
        # bounce off "not enough balance / allowance" at the exchange.
        self.budget_reserve_safety_margin_usdc = Decimal(str(execution.get("budget_reserve_safety_margin_usdc", 1.0)))
        self.budget_reserve_enabled = bool(execution.get("budget_reserve_enabled", True))
        self._budget_reserve_lock = asyncio.Lock()
        self._pending_order_reserve: Dict[str, tuple[str, Decimal]] = {}
        self._budget_reserve_seq = 0

        self.post_delay_min_sec = float(execution.get("post_delay_min_sec", 1))
        self.post_delay_max_sec = float(execution.get("post_delay_max_sec", 3))
        self.signer_max_concurrency = max(1, int(execution.get("signer_max_concurrency", 1)))
        self.signer_requote_gap_sec = float(execution.get("signer_requote_gap_sec", 1.2))
        self._signer_sem = asyncio.Semaphore(self.signer_max_concurrency)
        self._signer_gap_lock = asyncio.Lock()
        self._last_signer_post_ts = 0.0
        self.signer_fail_safe_after_sec = float(execution.get("signer_fail_safe_after_sec", 30))
        self.signer_fail_safe_cooldown_sec = float(execution.get("signer_fail_safe_cooldown_sec", 120))
        self._signer_failure_since = 0.0
        self._signer_fail_safe_fired_at = 0.0

        # # Global per-token order throttle
        self._global_order_lock = asyncio.Lock()
        self._global_last_order_ts = 0.0
        self._global_order_min_sec = float(execution.get("global_order_min_sec", 10))
        self._global_order_max_sec = float(execution.get("global_order_max_sec", 30))
        self._per_token_order_min_sec = float(execution.get("per_token_order_min_sec", 15))
        self._per_token_last_order_ts: Dict[str, float] = {tid: 0.0 for tid in self.market_cfg}
        self._aggressive_pair_submit_timeout_sec = max(
            1.0,
            float(
                execution.get(
                    "paired_submit_timeout_sec",
                    execution.get("aggressive_pair_submit_timeout_sec", 5.0),
                )
            ),
        )
        self._aggressive_pair_retry_cooldown_sec = max(
            self._aggressive_pair_submit_timeout_sec,
            float(
                execution.get(
                    "paired_retry_cooldown_sec",
                    execution.get("aggressive_pair_retry_cooldown_sec", 15.0),
                )
            ),
        )
        self._aggressive_pair_submit_lock = asyncio.Lock()
        self._aggressive_pair_submit_seq = 0
        self._aggressive_pair_submit_attempts: Dict[str, Dict[str, Any]] = {}
        self._aggressive_pair_submit_watchdogs: Dict[str, asyncio.Task] = {}
        self._paired_single_leg_grace_sec = max(
            self._aggressive_pair_submit_timeout_sec + 1.0,
            float(execution.get("paired_single_leg_grace_sec", 6.0)),
        )
        self._paired_reconcile_interval_sec = max(
            0.5,
            float(execution.get("paired_reconcile_interval_sec", 1.0)),
        )
        self._paired_single_leg_since: Dict[str, float] = {}

        # market reward-health auto offlining
        self.health_check_interval_sec = int(execution.get("health_check_interval_sec", 600))
        self.health_fail_threshold = int(execution.get("health_fail_threshold", 2))
        self.health_near_expiry_hours = int(execution.get("health_near_expiry_hours", 12))
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
        # Gamma parent event groups can contain several related binary markets.
        # A shock in one child (for example one Fed outcome) must protect the
        # other children too, not only the YES/NO pair in the same condition.
        self._market_parent_event_ids: Dict[str, str] = {}
        self._parent_event_tokens: Dict[str, set[str]] = {}
        self._parent_event_cooldown_until: Dict[str, float] = {}
        self._parent_event_last_shock_ts: Dict[str, float] = {}
        self._sponsored_guard = SponsoredRiskGuard(
            self.cfg.get("sponsored_risk_guard")
        )
        self._sponsored_guard_by_token: Dict[str, Dict[str, Any]] = {}
        self._sponsored_guard_assessments: Dict[str, Dict[str, Any]] = {}
        self._sponsored_guard_last_action: Dict[str, Dict[str, Any]] = {}
        self._sponsored_guard_summary: Dict[str, Any] = (
            self._sponsored_guard.state_payload({})
        )
        for _tid, _mcfg in self.market_cfg.items():
            _cid = str(_mcfg.get("condition_id") or "").strip().lower()
            if _cid:
                self._market_condition_ids[_tid] = _cid

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
        # Orders created by this engine. Account-level trade feeds also include
        # manual website orders, so order ownership must be explicit before an
        # observed trade is allowed to trigger automated inventory disposal.
        self._managed_buy_order_ids: set[str] = set()
        self._managed_buy_order_ids_order: list[str] = []
        self._managed_order_history_limit: int = int(
            execution.get("managed_order_history_limit", 1_000)
        )
        # A manual SELL means the user is taking control of that event's exit.
        # Keep maker BUYs away while it is live and for a short grace period
        # after it disappears so the engine cannot immediately buy it back.
        self._manual_exit_cooldown_sec: float = float(
            execution.get("manual_exit_cooldown_sec", 900)
        )
        self._manual_exit_event_until: Dict[str, float] = {}
        self._manual_exit_last_notice: Dict[str, float] = {}
        # pending unwind SELL orders: [{token_id, fill_price, fill_size, order_id, placed_at}]
        self._pending_unwinds: list[dict] = []
        self._active_exit_orders: Dict[str, str] = {}  # {token_id: order_id} — protected from cancel_all
        # Completed exit records: [{token_id, fill_price, sell_price, size, loss, ts}]
        self._exit_records: list[dict] = []
        self._fills_record: list[dict] = []
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
        self._parent_event_shock_guard_enabled: bool = bool(
            volatility_cfg.get("parent_event_shock_guard_enabled", True)
        )
        self._parent_event_shock_cooldown_sec: float = float(
            volatility_cfg.get("parent_event_shock_cooldown_sec", 1800)
        )
        self._parent_event_shock_debounce_sec: float = float(
            volatility_cfg.get("parent_event_shock_debounce_sec", 2)
        )
        # per-token rolling window: {token_id: {"front_notional_history": [(ts, val)], "defense_actions": [(ts, action)], "watch_enter_ts": float, "quarantine_enter_ts": float, "watch_count": int}}
        self._volatility_tracker: Dict[str, Dict[str, Any]] = {
            tid: {"front_notional_history": [], "defense_actions": [], "bba_prev": None, "watch_count": 0}
            for tid in self.market_cfg
        }
        aggressive_defense_cfg = self.cfg.get("aggressive_defense", {}) or {}
        self._aggressive_recovery_enabled = bool(
            self.lp_account_profile.profile_type == "aggressive"
            and aggressive_defense_cfg.get("enabled", True)
        )
        self._aggressive_recovery_required_samples = max(
            1, int(aggressive_defense_cfg.get("recovery_required_samples", 3))
        )
        self._aggressive_recovery_sample_interval_sec = max(
            5.0,
            float(aggressive_defense_cfg.get("recovery_sample_interval_sec", 30.0)),
        )

        # --- automatic Clash proxy failover ---
        pf_cfg = self.cfg.get("proxy_failover", {})
        self._proxy_failover_enabled: bool = bool(pf_cfg.get("enabled", False))
        self._proxy_failover_controller_url: str = str(pf_cfg.get("controller_url", "http://127.0.0.1:9097")).rstrip("/")
        # Clash Verge Rev runs mihomo in service mode and exposes the controller via Windows
        # named pipe ONLY (yaml `external-controller` is ignored). When pipe_path is non-empty,
        # _clash_request talks raw HTTP/1.1 over the pipe instead of TCP.
        self._proxy_failover_pipe_path: str = str(pf_cfg.get("pipe_path", "")).strip()
        self._proxy_failover_group_name: str = str(pf_cfg.get("group_name", "Proxy")).strip() or "Proxy"
        self._proxy_failover_whitelist_keywords: list[str] = [
            str(x).strip() for x in (pf_cfg.get("allowed_keywords") or []) if str(x).strip()
        ]
        self._proxy_failover_blocked_keywords: list[str] = [
            str(x).strip() for x in (pf_cfg.get("blocked_keywords") or ["Premium"]) if str(x).strip()
        ]
        # Set of node names already tried during the current recovery round.
        # Cleared by _proxy_failover_record_success when any WS/HTTP recovers.
        self._proxy_failover_tried_this_round: set[str] = set()
        self._proxy_failover_observe_sec: float = float(pf_cfg.get("observe_sec", 10))
        self._proxy_failover_bad_node_ttl_sec: float = float(pf_cfg.get("bad_node_ttl_sec", 600))
        self._proxy_failover_max_switches_per_window: int = int(pf_cfg.get("max_switches_per_window", 3))
        self._proxy_failover_switch_window_sec: float = float(pf_cfg.get("switch_window_sec", 600))
        self._proxy_failover_min_switch_gap_sec: float = float(pf_cfg.get("min_switch_gap_sec", 10))
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
        self._defense_block_until: dict[str, float] = {}
        self._proxy_failover_req_exc_recent: list[float] = []
        self._proxy_failover_ws_handshake_fail_count: int = 0
        self._proxy_failover_halt_until: float = 0.0

        # # P1: fill
        exit_cfg = self.cfg.get("exit_strategy", {})
        self._exit_delay_sec: float = float(exit_cfg.get("exit_delay_sec", 5))
        self._exit_timeout_sec: float = float(exit_cfg.get("exit_timeout_sec", 300))
        self._exit_max_loss_usd: Decimal = Decimal(str(exit_cfg.get("max_loss_usd", 10)))
        self._exit_reprice_interval: int = int(exit_cfg.get("reprice_interval_sec", 30))
        self._exit_stop_loss_wait_sec: float = float(exit_cfg.get("stop_loss_wait_sec", 10800))  # 3 hours
        self._exit_retry_count: int = int(exit_cfg.get("retry_count", 2))
        self._exit_dust_threshold: float = float(exit_cfg.get("dust_threshold", 0.5))
        self._balance_stability_checks: int = int(exit_cfg.get("balance_stability_checks", 3))
        self._balance_stability_interval_sec: float = float(exit_cfg.get("balance_stability_interval_sec", 3.0))
        # Exit recovery protection: after exit_complete_resume, skip global_cooldown for a window
        self._exit_recovery_protection_sec: float = float(exit_cfg.get("recovery_protection_sec", 20))
        self._exit_recovery_protection_until: Dict[str, float] = {}

        reconcile_cfg = self.cfg.get("position_reconcile", {}) or {}
        stable_profile = self.lp_account_profile.profile_type != "aggressive"
        self._position_reconcile_enabled: bool = bool(
            reconcile_cfg.get("enabled", stable_profile)
        )
        self._position_reconcile_interval_sec: float = max(
            30.0, float(reconcile_cfg.get("interval_sec", 60.0))
        )
        self._position_reconcile_managed_max_age_sec: float = max(
            300.0,
            float(reconcile_cfg.get("managed_evidence_max_age_sec", 24 * 3600)),
        )
        self._position_reconcile_inflight: set[str] = set()
        self._position_reconcile_managed_tokens: set[str] = set()
        self._position_reconcile_manual_sell_ts: Dict[str, float] = {}
        self._position_reconcile_alerted: Dict[str, str] = {}
        self._position_reconcile_state: Dict[str, Any] = {
            "enabled": self._position_reconcile_enabled,
            "last_checked_at": None,
            "status": "waiting" if self._position_reconcile_enabled else "disabled",
            "configured_positions": 0,
            "managed_positions": 0,
            "manual_review_positions": 0,
            "last_error": None,
            "positions": [],
        }

        # # session mode (redesigned: day=scan markets, night=night_markets)
        session_cfg = self.cfg.get("session", {})
        self._session_enabled: bool = bool(session_cfg.get("enabled", False))
        self._session_night_start: str = str(session_cfg.get("night_start", "00:00"))
        self._session_night_end: str = str(session_cfg.get("night_end", "06:00"))
        self._session_tz: str = str(session_cfg.get("tz", "Asia/Shanghai"))
        self._session_switch_gap_sec: float = float(session_cfg.get("switch_gap_sec", 5))
        self._session_confirm_required: bool = bool(session_cfg.get("confirm_required", True))
        self._session_confirm_ttl_sec: int = int(session_cfg.get("confirm_ttl_sec", 86400))  # 24h
        self._session_confirm_path: Path = Path(config_path).resolve().parent.parent.parent.parent / "data" / "session_confirm.json"
        self._session_confirm_window_start: str = str(session_cfg.get("confirm_window_start", "22:00"))
        self._session_confirm_window_end: str = str(session_cfg.get("confirm_window_end", "00:00"))
        # Overnight safety is fail-closed by default. Day markets must not
        # silently survive the switch when their start time cannot be verified.
        self._session_carry_day_markets_to_night: bool = bool(
            session_cfg.get("carry_day_markets_to_night", False)
        )
        self._last_session: str = "unknown"  # track session transitions
        self._session_halted_no_confirm: bool = False  # True when switch was blocked due to no confirm

        # Persistent log of auto_curator-added markets (both day and night).
        # Entries survive removal from the pool so the operator can see
        # recently added markets and what ran last night. Pruned after 48h.
        # Reloaded from prior engine_state on startup so restarts don't wipe it.
        self._curator_events_log: List[Dict[str, Any]] = []
        self._curator_events_ttl_sec: float = 48 * 3600

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
                "min_distance_ticks": int(m.get("min_distance_ticks", self.default_min_distance_ticks)),
                "risk": str(m.get("risk", "mid")).lower(),
                "base_risk": str(
                    m.get("eligibility_base_risk") or m.get("risk", "mid")
                ).lower(),
                "condition_id": str(m.get("condition_id", "")).strip().lower(),
                "source": str(m.get("source") or "manual"),
                "eligibility_managed": bool(m.get("eligibility_managed", False)),
                "lifecycle_stage": str(m.get("lifecycle_stage") or ""),
                "lifecycle_retire_pending": bool(
                    m.get("lifecycle_retire_pending", False)
                ),
            }
            if self._night_market_cfg[token_id]["condition_id"]:
                self._market_condition_ids[token_id] = self._night_market_cfg[token_id]["condition_id"]
            if self._night_market_cfg[token_id]["lifecycle_retire_pending"]:
                self._event_states[token_id] = {
                    "state": EVENT_WATCH,
                    "reason": "stable_lifecycle_retire_pending",
                    "updated_at": time.time(),
                }

        # state writer
        self._state_write_interval_sec: int = int(execution.get("state_write_interval_sec", 3))
        _maker_dir = Path(config_path).resolve().parent
        _cfg_stem = Path(config_path).stem  # "config", "config_1", "config_2", ...
        if self._account_idx:
            _state_fname = f"engine_state_{self._account_idx}.json"
        else:
            _state_fname = "engine_state.json"
        self._state_path: Path = _maker_dir.parent.parent.parent / "data" / _state_fname
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        # Pause flag file — touched by dashboard to pause quoting on this account
        # without stopping the process. Engine cancels open orders on entry to
        # paused state and resumes quoting when the file is removed.
        self._pause_flag_path: Path = self._state_path.parent / f".account_{self._account_idx}.paused"
        self._was_paused: bool = False
        _guard_cfg = self.cfg.get("aggressive_guardrails", {}) or {}
        self._aggressive_guardrails_enabled = bool(
            self.lp_account_profile.managed
            and self.lp_account_profile.profile_type == "aggressive"
        )
        self._aggressive_guardrail_interval_sec = max(
            5.0, float(_guard_cfg.get("check_interval_sec", 15.0))
        )
        self._aggressive_guardrail_stale_after_sec = max(
            self._aggressive_guardrail_interval_sec,
            float(_guard_cfg.get("stale_after_sec", 90.0)),
        )
        self._aggressive_guardrail_cutoff_hour = int(
            _guard_cfg.get("risk_day_cutoff_hour_cst", 8)
        )
        if not 0 <= self._aggressive_guardrail_cutoff_hour <= 23:
            raise ValueError("aggressive_guardrails.risk_day_cutoff_hour_cst must be 0..23")
        self._aggressive_guardrail_state_path = (
            self._state_path.parent
            / f"aggressive_guardrail_state_{self._account_idx or 1}.json"
        )
        self._aggressive_guardrail_latch_path = (
            self._state_path.parent
            / f".account_{self._account_idx or 1}.aggressive_guardrail"
        )
        self._aggressive_guardrail_reset_path = (
            self._state_path.parent
            / f".account_{self._account_idx or 1}.aggressive_guardrail_reset"
        )
        self._aggressive_guardrail_reset_paused_path = (
            self._state_path.parent
            / f".account_{self._account_idx or 1}.aggressive_guardrail_reset_paused"
        )
        self._aggressive_guardrail_state = AggressiveGuardrailState.load(
            self._aggressive_guardrail_state_path
        )
        self._aggressive_guardrail_cancel_complete = False
        if self._aggressive_guardrail_latch_path.exists():
            self._aggressive_guardrail_state.latched = True
        # Heartbeat file — touched every second by heartbeat_loop so the dashboard
        # can detect liveness across machines (PID check fails for rsynced state
        # from other VPS). Dashboard falls back to mtime > now-10s = alive.
        if self._account_idx == 0:
            self._heartbeat_path: Path = self._state_path.parent / ".engine.heartbeat"
        else:
            self._heartbeat_path = self._state_path.parent / f".engine_{self._account_idx}.heartbeat"
        self._runtime_command_dir = (
            self._state_path.parent / f"runtime_commands_{self._account_idx}"
        )
        self._runtime_result_dir = (
            self._state_path.parent / f"runtime_results_{self._account_idx}"
        )
        self._runtime_command_dir.mkdir(parents=True, exist_ok=True)
        self._runtime_result_dir.mkdir(parents=True, exist_ok=True)
        self._runtime_command_parse_failures: Dict[str, int] = {}
        self._runtime_command_parse_retry_limit = 5
        self._eligibility_observer_path = (
            self._state_path.parent / "reward_observer_state.json"
        )
        self._stable_rotation_proposal_path = (
            self._state_path.parent / "stable_rotation_proposal.json"
        )
        lifecycle_cfg = self.cfg.get("stable_market_lifecycle", {}) or {}
        if not isinstance(lifecycle_cfg, dict):
            lifecycle_cfg = {}
        self._stable_market_lifecycle_desired_enabled = bool(
            lifecycle_cfg.get("enabled", False)
            and self.lp_account_profile.profile_type != "aggressive"
        )
        self._stable_market_lifecycle_enabled = (
            self._stable_market_lifecycle_desired_enabled
        )
        self._stable_lifecycle_action_lock = asyncio.Lock()
        self._stable_lifecycle_max_proposal_age_sec = max(
            60.0,
            float(lifecycle_cfg.get("max_proposal_age_sec", 900.0)),
        )
        self._stable_lifecycle_max_add_per_cycle = max(
            0,
            int(lifecycle_cfg.get("max_add_per_cycle", 5)),
        )
        (
            self._stable_lifecycle_max_active_canaries,
            self._stable_lifecycle_canary_principal_fraction,
            self._stable_lifecycle_canary_max_usdc,
            self._stable_lifecycle_promotion_scoring_threshold,
        ) = _stable_lifecycle_safety_limits(
            lifecycle_cfg
        )
        self._stable_lifecycle_soft_failure_threshold = max(
            1,
            int(lifecycle_cfg.get("soft_failure_threshold", 3)),
        )
        self._stable_lifecycle_hard_failure_threshold = max(
            1,
            int(lifecycle_cfg.get("hard_failure_threshold", 1)),
        )
        self._stable_lifecycle_state_path = (
            self._state_path.parent
            / f"stable_market_lifecycle_state_{self._account_idx}.json"
        )
        self._stable_lifecycle_state: Dict[str, Any] = {}
        try:
            lifecycle_state = json.loads(
                self._stable_lifecycle_state_path.read_text(encoding="utf-8")
            )
            if (
                isinstance(lifecycle_state, dict)
                and int(lifecycle_state.get("account_index") or 0)
                == int(self._account_idx)
                and (
                    int(lifecycle_state.get("version") or 0)
                    != STABLE_LIFECYCLE_STATE_VERSION
                    or (
                        str(
                            lifecycle_state.get("account_uid_key") or ""
                        ).strip()
                        == self._stable_lifecycle_account_uid_key
                        and (
                            str(lifecycle_state.get("host_id") or "")
                            .strip()
                            .lower()
                            == self._runtime_host_id
                        )
                    )
                )
            ):
                self._stable_lifecycle_state = lifecycle_state
        except Exception:
            pass
        self._eligibility_state: Dict[str, Dict[str, Any]] = {}
        eligibility_cfg = self.cfg.get("eligibility_guard", {}) or {}
        aggressive_profile = self.lp_account_profile.profile_type == "aggressive"
        self._eligibility_check_interval_sec = max(
            15.0,
            float(
                eligibility_cfg.get(
                    "check_interval_sec",
                    60.0 if aggressive_profile else 300.0,
                )
            ),
        )
        self._eligibility_failure_threshold = max(
            1,
            int(
                eligibility_cfg.get(
                    "failure_threshold",
                    2 if aggressive_profile else 3,
                )
            ),
        )
        self._eligibility_stale_after_sec = max(
            self._eligibility_check_interval_sec,
            float(eligibility_cfg.get("stale_after_sec", 660.0)),
        )
        scoring_cfg = self.cfg.get("order_scoring_observer", {}) or {}
        if not isinstance(scoring_cfg, Mapping):
            scoring_cfg = {}
        self._order_scoring_observer_enabled = (
            _stable_order_scoring_observer_enabled(
                profile_type=self.lp_account_profile.profile_type,
                lifecycle_enabled=self._stable_market_lifecycle_enabled,
                scoring_cfg=scoring_cfg,
            )
        )
        checkpoints = scoring_cfg.get("checkpoints_sec") or (10, 30, 60, 120, 180, 300)
        self._order_scoring_observer_interval_sec = max(
            5.0, float(scoring_cfg.get("poll_interval_sec", 10.0))
        )
        self._order_scoring_observer = OrderScoringObserver(
            self._state_path.parent
            / f"order_scoring_state_{self._account_idx or 1}.json",
            checkpoints_sec=tuple(checkpoints),
            retention_sec=float(scoring_cfg.get("retention_sec", 7 * 24 * 60 * 60)),
            steady_state_interval_sec=float(
                scoring_cfg.get(
                    "steady_state_interval_sec",
                    300.0 if self._stable_market_lifecycle_enabled else 0.0,
                )
            ),
            account_uid_key=self._stable_lifecycle_account_uid_key,
            host_id=self._runtime_host_id,
        )

        # Rehydrate _curator_events_log from prior engine_state so a restart
        # doesn't wipe recently added records. Reads new key "curator_events"
        # with fallback to legacy "night_events". Entries older than TTL are
        # pruned on first state_write tick.
        try:
            if self._state_path.exists():
                _prior = json.loads(self._state_path.read_text(encoding="utf-8"))
                _prior_events = (
                    _prior.get("curator_events")
                    or _prior.get("night_events")
                    or []
                ) if isinstance(_prior, dict) else []
                _prior_managed_ids = (
                    _prior.get("managed_buy_order_ids") or []
                ) if isinstance(_prior, dict) else []
                if isinstance(_prior_managed_ids, list):
                    restored_ids = [
                        str(order_id)
                        for order_id in _prior_managed_ids[
                            -self._managed_order_history_limit:
                        ]
                        if order_id
                    ]
                    self._managed_buy_order_ids_order = restored_ids
                    self._managed_buy_order_ids = set(restored_ids)
                _prior_managed_positions = (
                    _prior.get("managed_position_tokens") or []
                ) if isinstance(_prior, dict) else []
                if isinstance(_prior_managed_positions, list):
                    self._position_reconcile_managed_tokens = {
                        str(token_id)
                        for token_id in _prior_managed_positions
                        if str(token_id).isdigit()
                    }
                _prior_manual_sell_ts = (
                    _prior.get("position_reconcile_manual_sell_ts") or {}
                ) if isinstance(_prior, dict) else {}
                if isinstance(_prior_manual_sell_ts, dict):
                    now_ts = time.time()
                    cutoff = now_ts - self._position_reconcile_managed_max_age_sec
                    for token_id, raw_ts in _prior_manual_sell_ts.items():
                        try:
                            sell_ts = float(raw_ts)
                        except (TypeError, ValueError):
                            continue
                        if str(token_id).isdigit() and cutoff <= sell_ts <= now_ts + 30:
                            self._position_reconcile_manual_sell_ts[
                                str(token_id)
                            ] = sell_ts
                restored_fills, restored_exits = _restore_activity_records(
                    _prior,
                    self._account_idx,
                )
                self._fills_record = restored_fills
                self._exit_records = restored_exits
                if isinstance(_prior_events, list):
                    _now_ts = time.time()
                    _ttl_cutoff = _now_ts - self._curator_events_ttl_sec
                    restored = []
                    for e in _prior_events:
                        if not isinstance(e, dict):
                            continue
                        if float(e.get("added_at", 0) or 0) < _ttl_cutoff:
                            continue
                        # Drop computed fields — will be recomputed each state write
                        restored.append({
                            k: v for k, v in e.items()
                            if k not in ("live_status", "in_pool")
                        })
                    self._curator_events_log = restored
                    if restored:
                        log(f"[engine] restored {len(restored)} curator_events from prior state")
                if restored_fills or restored_exits:
                    log(
                        "[engine] restored activity history "
                        f"fills={len(restored_fills)} exits={len(restored_exits)}"
                    )
        except Exception as _re:
            log(f"[engine] prior state rehydrate err: {_re}")

        self._market_live_orders: Dict[str, list] = {}
        self._last_balance: Optional[Decimal] = None
        self._market_ws_backoff_cap_sec: int = int(execution.get("market_ws_backoff_cap_sec", 30))
        self._market_snapshot_stale_sec: float = float(execution.get("market_snapshot_stale_sec", 5.0))
        self._ws_recv_idle_timeout_sec: float = float(execution.get("ws_recv_idle_timeout_sec", 90.0))
        self._ws_pong_timeout_sec: float = float(execution.get("ws_pong_timeout_sec", 10.0))
        # WS reconnection robustness settings
        self._ws_backoff_base_sec: float = float(execution.get("ws_backoff_base_sec", 1.0))
        self._ws_backoff_cap_sec: float = float(execution.get("ws_backoff_cap_sec", 30.0))
        self._ws_full_restart_after_n: int = int(execution.get("ws_full_restart_after_n", 5))
        self._ws_heartbeat_interval_sec: float = float(execution.get("ws_heartbeat_interval_sec", 30.0))
        self._market_ws_reconnect_count: int = 0
        self._fill_ws_reconnect_count: int = 0
        self._gate_send_accept_budget_ms: float = float(execution.get("gate_send_accept_budget_ms", 2500))
        self._gate_halt_clear_budget_ms: float = float(execution.get("gate_halt_clear_budget_ms", 5000))

        # multi-account shared book cache (set by multi_runner; None in single-account mode)
        self._shared_book_cache: Optional[Any] = None
        self._shared_book_cache_ttl_sec: float = max(
            0.5,
            float(execution.get("shared_book_cache_ttl_sec", 2.0)),
        )
        self._shared_book_chunk_size: int = max(
            2,
            int(execution.get("shared_book_chunk_size", 12)),
        )
        self._shared_book_chunk_concurrency: int = max(
            1,
            min(4, int(execution.get("shared_book_chunk_concurrency", 2))),
        )
        self._shared_book_direct_rest_burst: int = max(
            1,
            int(execution.get("shared_book_direct_rest_burst", 4)),
        )
        self._shared_book_direct_rest_window_sec: float = max(
            0.1,
            float(execution.get("shared_book_direct_rest_window_sec", 1.0)),
        )
        # multi-account cycle sleep (random interval between full book-loop cycles)
        self._multi_cycle_sleep_min: float = float(execution.get("multi_cycle_sleep_min_sec", 3.0))
        self._multi_cycle_sleep_max: float = float(execution.get("multi_cycle_sleep_max_sec", 20.0))

        # Global all-orders cache — coalesces concurrent client.get_open_orders()
        # calls (primarily fired by top-leg-defense) into a single REST request
        # per TTL window. Invalidated on successful place/cancel.
        self._all_orders_cache: Optional[tuple[list, float]] = None
        self._all_orders_cache_ttl_sec: float = float(execution.get("all_orders_cache_ttl_sec", 0.5))
        self._all_orders_refresh_lock: Optional[asyncio.Lock] = None

        # --- Redis event bus ---
        bus_cfg = self.cfg.get("event_bus", {})
        self._event_bus = EventBus(
            redis_url=os.getenv("POLY_REDIS_URL", "").strip() or str(bus_cfg.get("redis_url", "")).strip(),
            enabled=bool(bus_cfg.get("enabled", False)),
        )
        self._runtime_mode = "single"
        self._runtime_scope = ""
        self._routing_roster_sha256 = ""
        self._routing_market_universe_sha256 = ""
        self._routing_account_count = 1
        self._local_account_count = 1
        self._runtime_market_updates_enabled = True
        self._stable_lifecycle_runtime_updates_enabled = bool(
            self.lp_account_profile.profile_type == "standard"
            and self._account_idx > 0
            and self._stable_lifecycle_account_uid_key
            and self._runtime_host_id
        )
        self._paused_zero_market_startup_hold = False
        if not self.market_cfg:
            # Keep the legacy night-only boundary unchanged. This exception is
            # only for a genuinely empty direct stable runtime that must stay
            # paused while a reviewed lifecycle command provisions canaries.
            if self._night_market_cfg:
                raise ValueError("No valid enabled markets in config.markets")
            rejection = self._paused_zero_market_startup_rejection()
            if rejection:
                raise ValueError(
                    "No valid enabled markets in config.markets; "
                    f"paused zero-market startup rejected: {rejection}"
                )
            self._paused_zero_market_startup_hold = True
            log(
                "[init] paused zero-market stable runtime accepted; "
                "BUY creation remains blocked until a market is provisioned "
                "while the account pause flag is present"
            )
        if self._event_bus.is_enabled:
            log("[init] Redis event bus enabled")

    @staticmethod
    def _floor_to_tick(px: Decimal, tick: Decimal) -> Decimal:
        return (px / tick).to_integral_value(rounding=ROUND_DOWN) * tick

    @staticmethod
    def _ceil_to_tick(px: Decimal, tick: Decimal) -> Decimal:
        if tick <= 0:
            return px
        return (px / tick).to_integral_value(rounding=ROUND_CEILING) * tick

    def _event_state_entry(self, token_id: str) -> Dict[str, Any]:
        return self._event_states.setdefault(
            token_id,
            {"state": EVENT_ACTIVE, "reason": "init", "updated_at": time.time()},
        )

    def _event_state_name(self, token_id: str) -> str:
        return str(self._event_state_entry(token_id).get("state") or EVENT_ACTIVE)

    def _paired_token_id(self, token_id: str) -> str:
        paired = str(self._paired_token_cache.get(token_id, "") or "").strip()
        if paired:
            return paired
        try:
            return str(
                self._get_mcfg(token_id).get("paired_token_id", "") or ""
            ).strip()
        except Exception:
            return ""

    def _aggressive_pair_token(self, token_id: str) -> str:
        if str(getattr(self, "_runtime_scope", "") or "").lower() != "aggressive":
            return ""
        return self._coordinated_pair_token(token_id)

    def _coordinated_pair_token(self, token_id: str) -> str:
        """Return the sibling outcome that must quote with ``token_id``.

        Aggressive LP has always treated its two legs atomically. Stable LP
        now uses the same invariant whenever dual-side quoting is enabled, so
        independent depth, defense, budget, or submit failures cannot leave a
        reward-inefficient single BUY leg resting on the venue.
        """
        aggressive = (
            str(getattr(self, "_runtime_scope", "") or "").lower()
            == "aggressive"
        )
        if not aggressive and not (
            bool(getattr(self, "_dual_side_enabled", True))
            and bool(getattr(self, "_dual_side_require_both_sides", True))
        ):
            return ""
        paired = self._paired_token_id(token_id)
        if (
            not paired
            or paired == token_id
            or (
                paired not in self.market_cfg
                and paired not in self._night_market_cfg
            )
        ):
            return ""
        return paired

    def _event_quote_block_reason(self, token_id: str) -> Optional[str]:
        state = self._event_state_name(token_id)
        if state in {
            EVENT_CANCELING,
            EVENT_HALTED_ON_FILL,
            EVENT_HALTED_ON_DATA,
            EVENT_COOLDOWN,
            EVENT_STARTED_BLOCKED,
            EVENT_WATCH,
            EVENT_QUARANTINE,
            EVENT_PENDING_MANUAL_EXIT,
            EVENT_EXIT_PENDING,
        }:
            return f"event_state={state}"

        parent_event_id = str(
            getattr(self, "_market_parent_event_ids", {}).get(token_id, "")
            or ""
        )
        parent_cooldown_until = float(
            getattr(self, "_parent_event_cooldown_until", {}).get(
                parent_event_id,
                0.0,
            )
            or 0.0
        )
        if parent_event_id and time.time() < parent_cooldown_until:
            return (
                f"parent_event_cooldown={parent_event_id}:"
                f"{int(parent_cooldown_until)}"
            )

        # YES and NO share one event. Once either side has inventory to unwind,
        # do not let the other side add fresh exposure while the exit is pending.
        paired = self._paired_token_id(token_id)
        if paired and paired != token_id:
            paired_state = self._event_state_name(paired)
            if paired_state in {
                EVENT_CANCELING,
                EVENT_HALTED_ON_FILL,
                EVENT_EXIT_PENDING,
                EVENT_PENDING_MANUAL_EXIT,
            }:
                return f"paired_event_state={paired_state}:{paired}"
        return None

    def _event_blocks_quote(self, token_id: str) -> bool:
        return self._event_quote_block_reason(token_id) is not None

    def _defense_blocks_requote(self, token_id: str) -> bool:
        return time.time() < float(self._defense_block_until.get(token_id, 0.0))

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

    def _pending_unwind_token_ids(self) -> set[str]:
        pending = getattr(self, "_pending_unwinds", [])
        if isinstance(pending, dict):
            token_ids = {str(token_id) for token_id in pending if token_id}
            values = pending.values()
        elif isinstance(pending, list):
            token_ids = set()
            values = pending
        else:
            return set()
        for unwind in values:
            if not isinstance(unwind, dict):
                continue
            for key in ("token_id", "source_token_id"):
                token_id = str(unwind.get(key) or "")
                if token_id:
                    token_ids.add(token_id)
        return token_ids

    def _event_has_unresolved_exit(
        self,
        token_id: str,
        *,
        ignore_state_tokens: Optional[set[str]] = None,
    ) -> bool:
        try:
            event_tokens = set(self._event_token_ids(token_id))
        except Exception:
            event_tokens = {str(token_id)}
        ignored_states = {str(tid) for tid in (ignore_state_tokens or set())}
        active_exits = getattr(self, "_active_exit_orders", {})
        if any(tid in active_exits for tid in event_tokens):
            return True
        if event_tokens & self._pending_unwind_token_ids():
            return True
        unresolved_states = {
            EVENT_CANCELING,
            EVENT_EXIT_PENDING,
            EVENT_PENDING_MANUAL_EXIT,
        }
        return any(
            tid not in ignored_states
            and self._event_state_name(tid) in unresolved_states
            for tid in event_tokens
        )

    def _clear_stale_active_halt_preemptions(self, trigger: str) -> int:
        """Clear pause-era preemption only for otherwise active, safe markets."""
        token_ids = set(self.market_cfg) | set(self._night_market_cfg)
        cleared = 0
        for token_id in token_ids:
            if self._event_state_name(token_id) != EVENT_ACTIVE:
                continue
            if self._event_has_unresolved_exit(token_id):
                continue
            if self._halt_requested.get(token_id):
                cleared += 1
            self._clear_halt_preemption(token_id)
        if cleared:
            log(
                f"[pause] cleared {cleared} stale active preemptions "
                f"trigger={trigger}"
            )
        return cleared

    def _consume_resolved_unwinds(self, token_id: str) -> set[str]:
        """Remove completed unwind tracking and return every state it owns."""
        resolved_tokens = {str(token_id)}
        pending = getattr(self, "_pending_unwinds", [])
        if not isinstance(pending, list):
            return resolved_tokens
        remaining = []
        for unwind in pending:
            if not isinstance(unwind, dict) or str(
                unwind.get("token_id") or ""
            ) != str(token_id):
                remaining.append(unwind)
                continue
            source_token = str(unwind.get("source_token_id") or "")
            if source_token:
                resolved_tokens.add(source_token)
        self._pending_unwinds = remaining
        return resolved_tokens

    def _activate_resolved_exit_tokens(
        self,
        token_ids: set[str],
        trigger: str,
    ) -> int:
        """Reactivate only tokens covered by an authoritatively completed exit."""
        resolved = {str(token_id) for token_id in token_ids if token_id}
        if not resolved:
            return 0
        managed_positions = getattr(
            self, "_position_reconcile_managed_tokens", set()
        )
        managed_positions.difference_update(resolved)
        resumed = 0
        protection_deadline = time.time() + self._exit_recovery_protection_sec
        resolvable_states = {
            EVENT_ACTIVE,
            EVENT_CANCELING,
            EVENT_COOLDOWN,
            EVENT_HALTED_ON_FILL,
            EVENT_EXIT_PENDING,
            EVENT_PENDING_MANUAL_EXIT,
        }
        for token_id in sorted(resolved):
            state = self._event_state_name(token_id)
            if state not in resolvable_states:
                log(
                    f"[exit] token={token_id[:16]} retains intentional "
                    f"state={state}"
                )
                continue
            if self._event_has_unresolved_exit(
                token_id,
                ignore_state_tokens=resolved,
            ):
                log(
                    f"[exit] token={token_id[:16]} remains halted; "
                    "another exit is unresolved"
                )
                continue
            self._clear_halt_preemption(token_id)
            self._set_event_state(token_id, EVENT_ACTIVE, trigger)
            self._exit_recovery_protection_until[token_id] = protection_deadline
            resumed += 1
        return resumed

    def _halt_preemption_reason(self, token_id: str) -> Optional[str]:
        reason = self._halt_requested.get(token_id)
        if reason:
            return reason
        return self._event_quote_block_reason(token_id)

    def _exchange_maintenance_guard(self) -> ExchangeMaintenanceGuard:
        guard = getattr(self, "_exchange_maintenance", None)
        if guard is None:
            guard = ExchangeMaintenanceGuard()
            self._exchange_maintenance = guard
        return guard

    def _record_exchange_maintenance_error(
        self,
        error: BaseException,
        action: str,
    ) -> bool:
        guard = self._exchange_maintenance_guard()
        update = guard.observe_error(error, action)
        if update is None:
            return False

        self._apply_exchange_maintenance_update(update, action)
        return True

    def _force_exchange_maintenance(
        self,
        reason: str,
        action: str,
        *,
        cancel_failure: bool = False,
        immediate_cancel: bool = False,
        cancel_already_verified: bool = False,
    ) -> None:
        update = self._exchange_maintenance_guard().force_maintenance(
            reason,
            action,
            cancel_failure=cancel_failure,
            immediate_cancel=immediate_cancel,
        )
        self._apply_exchange_maintenance_update(
            update,
            action,
            cancel_already_verified=cancel_already_verified,
        )

    def _apply_exchange_maintenance_update(
        self,
        update: Any,
        action: str,
        *,
        cancel_already_verified: bool = False,
    ) -> None:

        if update.entered or update.reason_changed:
            log(
                f"[exchange-maintenance] phase={PHASE_MAINTENANCE} "
                f"reason={update.reason} action={action}"
            )
            event_bus = getattr(self, "_event_bus", None)
            if event_bus is not None:
                try:
                    event_bus.publish(
                        "exchange_maintenance",
                        {
                            "phase": PHASE_MAINTENANCE,
                            "reason": update.reason,
                            "action": action,
                        },
                    )
                except Exception:
                    pass
            notify = getattr(self, "notify_discord", None)
            if callable(notify):
                try:
                    paused_verified = bool(
                        self._is_account_paused()
                        and getattr(self, "_was_paused", False)
                    )
                    market_data_outage = action == "market_data_guard"
                    if market_data_outage and paused_verified:
                        title = "行情暂时不可用（账户已暂停）"
                        message = (
                            "行情源：WebSocket 与 REST 同时陈旧\n"
                            "系统处理：维持零买单，保留退出卖单；等待连续健康确认"
                        )
                        level = "info"
                    elif market_data_outage:
                        title = "行情不可用，已进入安全模式"
                        message = (
                            "行情源：WebSocket 与 REST 同时陈旧\n"
                            "系统处理：停止新增买单，保留退出卖单；等待连续健康确认"
                        )
                        level = "warning"
                    else:
                        title = "交易所维护模式已启用"
                        message = (
                            f"原因：{update.reason}\n"
                            "系统处理：停止新增买单，保留退出卖单，降低撤单重试频率"
                        )
                        level = "info" if paused_verified else "warning"
                    notify(
                        title,
                        message,
                        level,
                    )
                except Exception:
                    pass

        if update.entered:
            if cancel_already_verified:
                self._exchange_maintenance_guard().note_cancel_success()
                return
            spawn = getattr(self, "_spawn_bg", None)
            current = getattr(self, "_exchange_maintenance_cancel_task", None)
            if callable(spawn) and (current is None or current.done()):
                self._exchange_maintenance_cancel_task = spawn(
                    self.trigger_global_kill_switch(
                        f"exchange_maintenance:{update.reason}"
                    ),
                    name="exchange_maintenance_cancel",
                )

    def _note_exchange_authenticated_read_success(self, action: str) -> None:
        guard = self._exchange_maintenance_guard()
        if not guard.note_authenticated_read_success():
            return
        log(
            "[exchange-maintenance] phase=recovering "
            f"action={action} reads={guard.read_success_streak}"
        )
        event_bus = getattr(self, "_event_bus", None)
        if event_bus is not None:
            try:
                event_bus.publish(
                    "exchange_maintenance",
                    {
                        "phase": "recovering",
                        "reason": guard.reason,
                        "action": action,
                    },
                )
            except Exception:
                pass
        if guard.reason == "market_data_unavailable":
            notify = getattr(self, "notify_discord", None)
            if callable(notify):
                try:
                    notify(
                        "行情读取已恢复",
                        (
                            f"连续认证读取：{guard.read_success_streak}/"
                            f"{guard.recovery_read_successes}\n"
                            "系统处理：继续保持安全锁，等待测试挂单与精确撤单核验"
                        ),
                        "info",
                    )
                except Exception:
                    pass

    def _begin_exchange_buy_attempt(self, token_id: str, label: str) -> str:
        claim = self._exchange_maintenance_guard().claim_buy_attempt()
        if claim is None:
            raise SoftQuoteSkip(
                f"exchange_maintenance_block token={token_id[:16]} label={label}"
            )
        return claim

    def _finish_exchange_buy_attempt(
        self,
        claim: Optional[str],
        *,
        success: bool,
    ) -> None:
        guard = self._exchange_maintenance_guard()
        recovered = guard.finish_buy_attempt(claim, success=success)
        if not recovered:
            return
        log(
            "[exchange-maintenance] phase=normal "
            "recovery_post_cancel_verify=success"
        )
        event_bus = getattr(self, "_event_bus", None)
        if event_bus is not None:
            try:
                event_bus.publish(
                    "exchange_maintenance",
                    {
                        "phase": "normal",
                        "reason": "probe_canceled_and_verified",
                    },
                )
            except Exception:
                pass
        notify = getattr(self, "notify_discord", None)
        if callable(notify):
            try:
                notify(
                    "交易所维护模式已解除",
                    "认证读取、测试挂单、精确撤单与未成交核验均已恢复，允许继续正常做市",
                    "success",
                )
            except Exception:
                pass

    def _ensure_order_path_open(self, token_id: str, label: str) -> None:
        if self._is_account_paused():
            raise EventHaltPreempted(f"{label}:account_paused")
        guard = self._exchange_maintenance_guard()
        if guard.blocks_new_buys:
            raise EventHaltPreempted(
                f"{label}:exchange_maintenance={guard.reason or 'unknown'}"
            )
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
        self._event_bus.publish("state_change", {
            "token_id": token_id, "prev": prev, "state": state, "reason": reason,
        })

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
        # Primary: category field is the most reliable signal
        category = str(info.get("category") or "").lower()
        if category in ("sports", "esports"):
            return True
        # Secondary: gameStartTime / gameStartTs populated is a sports signal, BUT
        # some non-sports markets (e.g. geopolitical resolution dates like
        # "russia-x-ukraine-ceasefire-before-2027") also have gameStartTime set.
        # Require a corroborating signal: slug contains a specific date (YYYY-MM-DD),
        # which sports game slugs always have and long-window prediction markets don't.
        has_game_ts = bool(
            info.get("gameStartTime") or info.get("game_start_time") or info.get("gameStartTs")
        )
        if has_game_ts:
            slug_for_date = str(info.get("slug") or "")
            if _SPORTS_SLUG_DATE_RE.search(slug_for_date):
                return True
        # Tertiary: keyword heuristic (use specific terms, avoid ambiguous words
        # like "final", "match", "game" which appear in political/macro markets)
        sports_markers = [
            "sports", "nba", "nfl", "mlb", "nhl", "soccer", "football", "basketball",
            "baseball", "tennis", "golf", "ufc", "mma", " vs ",
            "champions league", "premier league", "la liga", "serie a", "bundesliga", "world cup",
            "super bowl", "playoffs", "playoff", "semifinal", "quarterfinal",
        ]
        return any(marker in hay for marker in sports_markers)

    def _extract_market_tags(self, meta: Optional[Dict[str, Any]] = None, token_id: str = "") -> list[str]:
        info = meta or {}
        out: list[str] = []

        def _pull(raw: Any) -> None:
            if not raw:
                return
            if isinstance(raw, str):
                try:
                    raw = json.loads(raw)
                except Exception:
                    raw = [raw]
            if not isinstance(raw, list):
                return
            for item in raw:
                if isinstance(item, dict):
                    label = item.get("label") or item.get("slug") or item.get("name") or ""
                else:
                    label = str(item)
                label = str(label).strip()
                if label:
                    out.append(label)

        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(token_id) or {}
        _pull(cfg.get("league_tags"))
        events_field = info.get("events") or []
        if isinstance(events_field, str):
            try:
                events_field = json.loads(events_field)
            except Exception:
                events_field = []
        if isinstance(events_field, list):
            for ev in events_field:
                if isinstance(ev, dict):
                    _pull(ev.get("tags"))
        _pull(info.get("tags"))
        return out

    def _market_pre_start_stop_sec(self, token_id: str, meta: Optional[Dict[str, Any]] = None) -> int:
        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(token_id) or {}
        try:
            override = int(cfg.get("pre_start_stop_sec_override") or 0)
        except Exception:
            override = 0
        if override > 0:
            return override
        high_risk_tags = ("atp", "wta", "tennis", "ufc", "mma", "boxing", "bellator", "pfl")
        tags = [str(tag or "").lower() for tag in self._extract_market_tags(meta, token_id)]
        if any(risk_tag in tag for tag in tags for risk_tag in high_risk_tags):
            return 12 * 3600
        return self.pre_start_stop_sec

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
        start_guard_sec = max(self._market_pre_start_stop_sec(token_id, info), 0)
        if start_ts is not None:
            freeze_at = start_ts - start_guard_sec
            if now >= freeze_at:
                if now >= start_ts:
                    return True, "market_started", start_ts
                return True, f"market_near_start:{start_guard_sec}s", start_ts
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
        was_blocked = self._event_state_name(token_id) == EVENT_STARTED_BLOCKED
        self._set_event_state(token_id, EVENT_STARTED_BLOCKED, final_reason)
        live_orders = await self._get_live_orders_fast(token_id)
        cancelled_n = len(live_orders) if live_orders else 0
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
        if not was_blocked:
            slug = self._token_slug_cache.get(token_id, token_id[:16])
            if start_ts:
                start_hm = datetime.fromtimestamp(start_ts).strftime("%m-%d %H:%M")
                self.send_discord(
                    f"赛前下架\n市场：{slug}\n开赛：{start_hm}\n"
                    f"撤单：{cancelled_n} 笔\n原因：{self._discord_reason(reason)}\n来源：{trigger}"
                )
            else:
                self.send_discord(
                    f"赛前下架\n市场：{slug}\n撤单：{cancelled_n} 笔\n"
                    f"原因：{self._discord_reason(reason)}\n来源：{trigger}"
                )
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
            "cancel_ack_ms": delta("t_send", "t_cancel_ack"),
            "detect_to_cancel_ack_ms": delta("t_detect", "t_cancel_ack"),
            "cancel_ack_to_cleared_ms": delta("t_cancel_ack", "t_orders_cleared"),
            "send_to_cleared_ms": delta("t_send", "t_orders_cleared"),
            "detect_to_cleared_ms": delta("t_detect", "t_orders_cleared"),
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
        heartbeat_interval = self._ws_heartbeat_interval_sec
        if idle_timeout <= 0:
            return await ws.recv()
        # Use the shorter of heartbeat_interval and idle_timeout as the recv wait
        # so we can send proactive pings even when no data arrives
        check_interval = min(heartbeat_interval, idle_timeout) if heartbeat_interval > 0 else idle_timeout
        last_activity = time.time()
        while self._running:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=check_interval)
                last_activity = time.time()
                return msg
            except asyncio.TimeoutError:
                now = time.time()
                since_activity = now - last_activity
                # Proactive heartbeat: send ping if no data within heartbeat interval
                try:
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=self._ws_pong_timeout_sec)
                    last_activity = time.time()
                    if scope == "fill-ws":
                        self._last_ws_ok_ts = time.time()
                    elif scope == "market-ws":
                        self._last_market_ws_ok_ts = time.time()
                except Exception as ping_exc:
                    raise TimeoutError(
                        f"{scope} stale connection: no data for {since_activity:.0f}s, ping failed: {_format_exc(ping_exc)}"
                    ) from ping_exc
                # If total silence exceeds idle_timeout even though pings succeed,
                # raise to force reconnect (server may have stopped sending data)
                if since_activity > idle_timeout:
                    raise TimeoutError(
                        f"{scope} idle>{idle_timeout:.0f}s despite successful pings — forcing reconnect"
                    )
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
        observed_ts: Optional[float] = None,
    ) -> Optional[MarketSnapshot]:
        prev = self._market_snapshots.get(token_id)
        if not self._snapshot_values_valid(best_bid, best_ask):
            log(
                f"[snapshot-drop] token={token_id} source={source} "
                f"bid={best_bid} ask={best_ask} reason=invalid_quote"
            )
            return prev

        now = (
            float(observed_ts)
            if observed_ts is not None and float(observed_ts) > 0
            else time.time()
        )
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
                return None  # force skip — data mismatch
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
        # When dynamic budget is enabled, trigger periodic rebalance before lookup.
        if self._dynamic_budget_enabled:
            self._rebalance_market_budgets()
        cached = self._market_budget_pct.get(token_id)
        if cached is not None and cached > 0:
            return cached
        lo, hi = self.quote_balance_pct_ranges.get(market_risk, (self.quote_balance_pct_min, self.quote_balance_pct_max))
        lo = max(Decimal("0"), min(lo, Decimal("1")))
        hi = max(lo, min(hi, Decimal("1")))
        pct = Decimal(str(random.uniform(float(lo), float(hi))))
        self._market_budget_pct[token_id] = pct
        return pct

    # ------------------------------------------------------------------
    # Dynamic budget scoring & rebalancing
    # ------------------------------------------------------------------

    def _compute_market_score(self, token_id: str) -> float:
        """Score a market from 0.0 to 1.0 for dynamic budget allocation.

        Components (weights sum to 1.0):
        - reward_score  (0.4): higher rewardsMaxSpread = more attractive
        - depth_score   (0.3): lower front depth (less competition) = better
        - fill_risk     (0.2): defensive/watch/quarantine/cooldown or recent fills penalised
        - event_state   (0.1): only ACTIVE markets get full bonus
        """
        score = 0.0

        # -- Reward rate component (0..0.4) --
        reward_weight = 0.0
        meta_entry = self._market_meta_cache.get(token_id)
        if meta_entry:
            meta_dict = meta_entry[0] if isinstance(meta_entry, tuple) else meta_entry
            spread_raw = meta_dict.get("maxIncentiveSpread") or meta_dict.get("rewardsMaxSpread")
            if spread_raw is not None:
                try:
                    spread_val = float(spread_raw)
                except (TypeError, ValueError):
                    spread_val = 0.0
                # Typical rewardsMaxSpread: 0.01 - 0.10.  Map to 0..1 linearly, clamp.
                reward_weight = min(1.0, max(0.0, spread_val / 0.10))
        score += reward_weight * 0.4

        # -- Depth / crowding component (0..0.3) --
        depth_score = 0.5  # neutral default when no data
        tracker = self._volatility_tracker.get(token_id)
        if tracker:
            history = tracker.get("front_notional_history", [])
            if history:
                latest_notional = history[-1][1]
                # Lower front notional = less competition = higher score.
                # Typical range: 0 - 500 USDC.  Map inversely.
                if latest_notional <= 0:
                    depth_score = 1.0
                elif latest_notional >= 500:
                    depth_score = 0.0
                else:
                    depth_score = 1.0 - (latest_notional / 500.0)
        score += depth_score * 0.3

        # -- Fill risk component (0..0.2) --
        fill_penalty = 0.0
        state = self._event_state_name(token_id)
        if state in (EVENT_WATCH, EVENT_QUARANTINE, EVENT_COOLDOWN):
            fill_penalty = 0.8
        elif state in (EVENT_CANCELING, EVENT_HALTED_ON_FILL, EVENT_HALTED_ON_DATA):
            fill_penalty = 1.0
        elif state == EVENT_DEFENSIVE:
            fill_penalty = 0.4
        # Penalise markets with recent fills (last 120s)
        now = time.time()
        recent_fill_count = 0
        for fr in self._fills_record:
            if fr.get("token_id") == token_id and (now - fr.get("ts", 0)) < 120:
                recent_fill_count += 1
        if recent_fill_count > 0:
            fill_penalty = max(fill_penalty, min(1.0, recent_fill_count * 0.3))
        score += (1.0 - fill_penalty) * 0.2

        # -- Event state component (0..0.1) --
        if state == EVENT_ACTIVE:
            score += 0.1
        elif state == EVENT_DEFENSIVE:
            score += 0.05

        return min(1.0, max(0.0, score))

    def _rebalance_market_budgets(self) -> None:
        """Recompute dynamic budget percentages for all enabled markets.

        Each market gets a score, scores are normalized into budget pct values,
        then clamped to [budget_min_pct, budget_max_pct].

        The resulting percentages are written into ``_market_budget_pct`` so that
        ``_market_quote_budget_pct()`` picks them up on the next quote cycle.

        Paired markets (YES/NO on the same event) share a single score — the max
        of both sides — so their budgets stay aligned.
        """
        if not self._dynamic_budget_enabled:
            return
        now = time.time()
        if now - self._last_budget_rebalance_ts < self._budget_rebalance_interval_sec:
            return
        self._last_budget_rebalance_ts = now

        token_ids = list(self.market_cfg.keys())
        if not token_ids:
            return

        # Step 1: compute raw scores
        raw_scores: Dict[str, float] = {}
        for tid in token_ids:
            raw_scores[tid] = self._compute_market_score(tid)
        self._dynamic_budget_scores = dict(raw_scores)

        # Step 2: unify paired markets — both sides get the max score of the pair
        for tid in token_ids:
            paired = self._paired_token_cache.get(tid) or str(
                self.market_cfg[tid].get("paired_token_id", "") or ""
            ).strip()
            if paired and paired in raw_scores:
                unified = max(raw_scores[tid], raw_scores[paired])
                raw_scores[tid] = unified
                raw_scores[paired] = unified

        # Step 3: weight scores by risk tier baseline so risk tier C still gets less
        weighted: Dict[str, float] = {}
        for tid in token_ids:
            risk = str(self.market_cfg[tid].get("risk", "mid")).lower()
            lo, hi = self.quote_balance_pct_ranges.get(
                risk, (self.quote_balance_pct_min, self.quote_balance_pct_max)
            )
            tier_midpoint = float((lo + hi) / Decimal("2"))
            weighted[tid] = raw_scores[tid] * tier_midpoint

        # Step 4: normalize to sum ~ 1, then clamp
        total_weight = sum(weighted.values())
        if total_weight <= 0:
            return  # all scores zero — keep previous budgets

        min_pct = float(self._budget_min_pct)
        max_pct = float(self._budget_max_pct)

        new_pcts: Dict[str, Decimal] = {}
        for tid in token_ids:
            raw_pct = weighted[tid] / total_weight
            clamped = max(min_pct, min(max_pct, raw_pct))
            new_pcts[tid] = Decimal(str(round(clamped, 6)))

        # Step 5: rescale so total doesn't exceed 1.0
        pct_sum = sum(new_pcts.values())
        if pct_sum > Decimal("1"):
            for tid in new_pcts:
                new_pcts[tid] = max(
                    self._budget_min_pct,
                    (new_pcts[tid] / pct_sum).quantize(Decimal("0.000001")),
                )

        # Step 6: write into the budget cache
        for tid, pct in new_pcts.items():
            self._market_budget_pct[tid] = pct

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

    @staticmethod
    def _order_token_id(order: dict) -> str:
        return str(order.get("asset_id") or order.get("token_id") or "")

    @staticmethod
    def _order_side(order: dict) -> str:
        return str(order.get("side", "") or "").upper()

    def _uses_shared_event_collateral(self, token_id: str) -> bool:
        mcfg = self._get_mcfg(str(token_id))
        return bool(mcfg.get("paired_token_id") or mcfg.get("_dual_side_auto"))

    def _collateral_required_for_order(self, token_id: str, price: Decimal, size: Decimal) -> Decimal:
        if price <= 0 or size <= 0:
            return Decimal("0")
        if self._uses_shared_event_collateral(token_id):
            return size
        return price * size

    def _event_reserved_collateral(
        self,
        token_id: str,
        extra_entries: Optional[list[tuple[str, Decimal]]] = None,
    ) -> Decimal:
        """Return reserved collateral for one event only.

        Polymarket validates BUY capacity independently for each condition, so
        orders in other events must not consume this event's quote budget.
        Within a dual-side event, reserve the max outstanding YES/NO shares;
        equal-sized complementary bids cost approximately one dollar per pair.
        """
        event_key = self._paired_budget_key(str(token_id))
        total = Decimal("0")
        paired_side_reserve: Dict[str, Dict[str, Decimal]] = {}

        def add_entry(entry_token_id: str, amount: Decimal) -> None:
            nonlocal total
            if amount <= 0:
                return
            token = str(entry_token_id or "")
            if self._paired_budget_key(token) != event_key:
                return
            if token and self._uses_shared_event_collateral(token):
                key = self._paired_budget_key(token)
                bucket = paired_side_reserve.setdefault(key, {})
                bucket[token] = bucket.get(token, Decimal("0")) + amount
                return
            total += amount

        for fallback_tid, orders in self._market_live_orders.items():
            for order in orders:
                side = self._order_side(order)
                if side and side != "BUY":
                    continue
                if not _order_is_live(order):
                    continue
                token_id = self._order_token_id(order) or str(fallback_tid)
                price = self._order_price(order)
                size = self._order_size(order)
                add_entry(token_id, self._collateral_required_for_order(token_id, price, size))

        for token_id, amount in self._pending_order_reserve.values():
            add_entry(token_id, amount)

        for token_id, amount in extra_entries or []:
            add_entry(token_id, amount)

        for side_amounts in paired_side_reserve.values():
            total += max(side_amounts.values(), default=Decimal("0"))
        return total

    def _calc_active_orders_reserved(
        self,
        exclude_tokens: Optional[set] = None,
        only_tokens: Optional[set] = None,
    ) -> Decimal:
        """Sum notional (price * remaining_size) of active orders.

        exclude_tokens: tokens to skip
        only_tokens: if provided, only count these tokens
        """
        total = Decimal("0")
        exclude = exclude_tokens or set()
        only = only_tokens or set()
        for tid, orders in self._market_live_orders.items():
            if tid in exclude:
                continue
            if only and tid not in only:
                continue
            for o in orders:
                p = self._order_price(o)
                s = self._order_size(o)
                if p > 0 and s > 0:
                    total += p * s
        return total

    async def _acquire_budget_reserve(
        self,
        token_id: str,
        price: Decimal,
        size: Decimal,
        label: str,
    ) -> Optional[str]:
        """Reserve collateral for an in-flight BUY before we hit the signer."""
        if not self.budget_reserve_enabled:
            return None
        if price <= 0 or size <= 0:
            return None

        reserve_needed = self._collateral_required_for_order(token_id, price, size)
        if reserve_needed <= 0:
            return None

        async with self._budget_reserve_lock:
            last_diag: Optional[tuple[Decimal, Decimal, Decimal, Decimal]] = None
            for force_refresh in (False, True):
                avail = await self._get_collateral_available(force_refresh=force_refresh)
                if avail is None or avail <= 0:
                    continue
                margin = self.budget_reserve_safety_margin_usdc
                allowed = max(Decimal("0"), avail - margin)
                projected = self._event_reserved_collateral(
                    token_id,
                    extra_entries=[(token_id, reserve_needed)],
                )
                if projected <= allowed:
                    self._budget_reserve_seq += 1
                    reserve_id = f"{token_id}:{self._budget_reserve_seq}"
                    self._pending_order_reserve[reserve_id] = (str(token_id), reserve_needed)
                    return reserve_id
                last_diag = (avail, allowed, projected, margin)

            if last_diag is None:
                return None

            avail, allowed, projected, margin = last_diag
            reserved = self._event_reserved_collateral(token_id)
            slug = self._token_slug_cache.get(token_id, token_id[:16])
            self._market_budget_skip_until[token_id] = time.time() + self.budget_skip_cooldown_sec
            log(
                f"[budget-reserve-block] slug={slug} token={token_id[:16]} label={label} "
                f"price={price} size={size} reserve_needed={reserve_needed:.4f} "
                f"event_reserved={reserved:.4f} margin={margin:.4f} avail={avail:.4f} "
                f"allowed={allowed:.4f} projected={projected:.4f}"
            )
            raise SoftQuoteSkip(
                f"budget_reserve_block token={token_id[:16]} label={label} "
                f"projected={projected:.4f}>allowed={allowed:.4f}"
            )

    async def _release_budget_reserve(self, reserve_id: Optional[str]) -> None:
        if not reserve_id:
            return
        async with self._budget_reserve_lock:
            self._pending_order_reserve.pop(reserve_id, None)

    def _ensure_runtime_token_state(self, token_id: str, reason: str = "runtime_state") -> None:
        now = time.time()
        self._event_states.setdefault(token_id, {"state": EVENT_ACTIVE, "reason": reason, "updated_at": now})
        self._event_locks.setdefault(token_id, asyncio.Lock())
        self._latency_marks.setdefault(token_id, {})
        self._halt_requested.setdefault(token_id, None)
        self.last_quote_ts.setdefault(token_id, 0.0)
        self._market_balance_fail_streak.setdefault(token_id, 0)
        self._market_skip_until.setdefault(token_id, 0.0)
        self._market_stale_fail_streak.setdefault(token_id, 0)
        self._market_budget_skip_until.setdefault(token_id, 0.0)
        self._health_fail_streak.setdefault(token_id, 0)
        self._book_req_exc_streak.setdefault(token_id, 0)
        self._per_token_last_order_ts.setdefault(token_id, 0.0)
        self._market_live_orders.setdefault(token_id, [])

    def _mark_signer_recovered(self) -> None:
        if self._signer_failure_since > 0:
            outage = time.time() - self._signer_failure_since
            log(f"[signer] recovered after {outage:.1f}s")
        self._signer_failure_since = 0.0
        self._signer_fail_safe_fired_at = 0.0

    async def _handle_signer_failure(self, token_id: str, exc: Exception, phase: str) -> None:
        now = time.time()
        if self._signer_failure_since <= 0:
            self._signer_failure_since = now
            log(f"[signer] outage_started phase={phase} token=*** err={_format_exc(exc)}")
        elapsed = now - self._signer_failure_since
        if elapsed < self.signer_fail_safe_after_sec:
            return
        if (now - self._signer_fail_safe_fired_at) < self.signer_fail_safe_cooldown_sec:
            return
        self._signer_fail_safe_fired_at = now
        log(
            f"[signer] fail_safe trigger elapsed={elapsed:.1f}s token=*** "
            f"phase={phase} cooldown={self.cooldown_seconds}s"
        )
        await self.trigger_global_kill_switch("remote_signer_unreachable")

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
        sponsored_risk = getattr(self, "_sponsored_guard_by_token", {}).get(token_id)
        if sponsored_risk:
            decision["sponsored_risk"] = sponsored_risk
            sponsor_status = str(sponsored_risk.get("status") or "unknown")
            sponsor_reasons = [
                f"sponsor:{reason}"
                for reason in (sponsored_risk.get("reasons") or [])
                if reason != "ok"
            ]
            if sponsor_status == "blocked":
                decision.update({
                    "can_quote": False,
                    "size_cap": 0.0,
                    "top_leg_action": "cancel",
                    "risk_grade": "BLOCK",
                })
                reasons.extend(sponsor_reasons or ["sponsor:blocked"])
                return decision
            if sponsor_status == "caution":
                sponsor_cap = float(sponsored_risk.get("size_cap", 1.0) or 0.0)
                decision["size_cap"] = min(float(decision["size_cap"]), sponsor_cap)
                decision["risk_grade"] = "B" if sponsor_cap >= 0.5 else "C"
                reasons.extend(sponsor_reasons or ["sponsor:caution"])
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
        # Full markets keep the hard trusted-depth floor. Lifecycle canaries
        # may probe positive shallow depth because their capital is bounded
        # separately and promotion still requires three scoring samples.
        market_cfg = self._get_mcfg(token_id)
        lifecycle_stage = str(
            market_cfg.get("lifecycle_stage") or "full"
        ).strip().lower()
        decision["lifecycle_stage"] = lifecycle_stage
        front_depth_floor = self.min_front_bid_notional_usdc
        if lifecycle_stage == "canary":
            canary_budget = self._stable_lifecycle_canary_budget_usdc(
                self._last_balance
            )
            if canary_budget is None or canary_budget <= 0:
                decision.update(
                    {
                        "can_quote": False,
                        "size_cap": 0.0,
                        "top_leg_action": "cancel",
                        "risk_grade": "BLOCK",
                    }
                )
                reasons.append("canary_budget_unavailable")
                return decision
            front_depth_floor = min(front_depth_floor, canary_budget)
        decision["front_depth_floor_usdc"] = float(front_depth_floor)
        if lifecycle_stage == "canary" and not _depth_trustworthy:
            decision.update(
                {
                    "can_quote": False,
                    "size_cap": 0.0,
                    "top_leg_action": "cancel",
                    "risk_grade": "BLOCK",
                }
            )
            reasons.append("front_depth_canary_untrusted")
            return decision
        if _depth_trustworthy and front_notional < front_depth_floor:
            decision.update(
                {
                    "can_quote": False,
                    "size_cap": 0.0,
                    "top_leg_action": "cancel",
                    "risk_grade": "BLOCK",
                }
            )
            reasons.append(
                "front_depth_empty"
                if lifecycle_stage == "canary" and front_notional <= 0
                else (
                    "front_depth_canary_insufficient"
                    if lifecycle_stage == "canary"
                    else "front_depth_critical"
                )
            )
            return decision
        if (
            _depth_trustworthy
            and lifecycle_stage == "canary"
            and front_notional < self.min_front_bid_notional_usdc
        ):
            if front_notional > 0:
                # Canary admission intentionally tests shallow but executable
                # reward pools with a separately bounded account-level budget.
                # Full markets continue to require the normal depth floor.
                decision["risk_grade"] = "C"
                reasons.append("front_depth_canary_probe")
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

    def _vol_check_bba_jump(
        self,
        token_id: str,
        best_bid: Decimal,
        best_ask: Decimal,
        *,
        update_baseline: bool = True,
    ) -> bool:
        """Return True if BBA jumped >= threshold ticks."""
        tracker = self._vol_tracker(token_id)
        prev = tracker.get("bba_prev")
        if update_baseline:
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
        self._latency_flow_reset(token_id)
        self._mark_latency(token_id, "t_detect")
        self._mark_latency(token_id, "t_decision")
        tracker = self._vol_tracker(token_id)
        tracker["watch_count"] = int(tracker.get("watch_count", 0)) + 1
        tracker["defense_repeat_count"] = int(tracker.get("defense_repeat_count", 0)) + 1
        if tracker["watch_count"] >= 2 or tracker.get("defense_repeat_count", 0) >= self._repeat_defense_ban_count:
            self._event_banned_until[self._event_key(token_id)] = time.time() + self.event_ban_ttl_sec
            self._set_event_state(token_id, EVENT_QUARANTINE, f"watch_limit_forbid:{reason}")
            ids = [
                self._order_id(o)
                for o in self._cached_live_orders(token_id)
            ]
            cleared = await self._cancel_coordinated_pair_quotes(
                token_id,
                f"forbid:{reason}",
            )
            if not cleared:
                log(
                    f"[forbid] token={token_id} cancellation remains "
                    "unconfirmed after global escalation"
                )
            self._emit_latency_record(
                token_id,
                "volatility_forbid",
                {"reason": reason, "orders_targeted": len(ids)},
            )
            log(f"[forbid] token={token_id} watch_count={tracker.get('watch_count', 0)} defense_repeat_count={tracker.get('defense_repeat_count', 0)} reason={reason} ttl={self.event_ban_ttl_sec}s")
            slug = self._token_slug_cache.get(token_id, token_id[:16])
            self.send_discord(
                f"禁挂提醒\n市场：{slug}\n原因：{self._discord_reason(reason)}\n"
                f"进入观察：{tracker.get('watch_count', 0)} 次\n"
                f"重复防御：{tracker.get('defense_repeat_count', 0)} 次\n"
                f"禁挂时长：{self.event_ban_ttl_sec:.0f} 秒"
            )
            return
        tracker["watch_enter_ts"] = time.time()
        self._set_event_state(token_id, EVENT_WATCH, reason)
        ids = [
            self._order_id(o)
            for o in self._cached_live_orders(token_id)
        ]
        cleared = await self._cancel_coordinated_pair_quotes(
            token_id,
            f"watch:{reason}",
        )
        if not cleared:
            log(
                f"[watch] token={token_id} cancellation remains "
                "unconfirmed after global escalation"
            )
        self._emit_latency_record(
            token_id,
            "volatility_watch",
            {"reason": reason, "orders_targeted": len(ids)},
        )
        log(f"[watch] token={token_id} entered WATCH reason={reason} duration={self._vol_watch_duration_sec}s")

    async def _enter_quarantine(self, token_id: str, reason: str) -> None:
        """Enter QUARANTINE state: cancel all orders, longer cooldown."""
        self._latency_flow_reset(token_id)
        self._mark_latency(token_id, "t_detect")
        self._mark_latency(token_id, "t_decision")
        tracker = self._vol_tracker(token_id)
        tracker["quarantine_enter_ts"] = time.time()
        tracker["defense_repeat_count"] = int(tracker.get("defense_repeat_count", 0)) + 1
        self._set_event_state(token_id, EVENT_QUARANTINE, reason)
        ids = [
            self._order_id(o)
            for o in self._cached_live_orders(token_id)
        ]
        cleared = await self._cancel_coordinated_pair_quotes(
            token_id,
            f"quarantine:{reason}",
        )
        if not cleared:
            log(
                f"[quarantine] token={token_id} cancellation remains "
                "unconfirmed after global escalation"
            )
        self._emit_latency_record(
            token_id,
            "volatility_quarantine",
            {"reason": reason, "orders_targeted": len(ids)},
        )
        log(f"[quarantine] token={token_id} entered QUARANTINE reason={reason} duration={self._vol_quarantine_duration_sec}s")
        self._notify_risk("市场已暂停观察", token=token_id, reason=reason)

    def _remember_parent_event(
        self,
        token_ids: list[str],
        raw: Dict[str, Any],
        normalized: Dict[str, Any],
    ) -> str:
        events = raw.get("events")
        event = (
            events[0]
            if isinstance(events, list)
            and events
            and isinstance(events[0], dict)
            else {}
        )
        parent_event_id = str(
            event.get("id")
            or raw.get("eventId")
            or raw.get("event_id")
            or ""
        ).strip()
        if not parent_event_id:
            return ""

        parent_event_slug = str(
            event.get("slug")
            or raw.get("eventSlug")
            or raw.get("event_slug")
            or ""
        ).strip()
        normalized["parent_event_id"] = parent_event_id
        if parent_event_slug:
            normalized["parent_event_slug"] = parent_event_slug

        parent_ids = self._market_parent_event_ids
        parent_tokens = self._parent_event_tokens
        for candidate in token_ids:
            token_id = str(candidate or "").strip()
            if not token_id:
                continue
            prior = parent_ids.get(token_id)
            if prior and prior != parent_event_id:
                parent_tokens.get(prior, set()).discard(token_id)
            parent_ids[token_id] = parent_event_id
            parent_tokens.setdefault(parent_event_id, set()).add(token_id)
        return parent_event_id

    def _parent_event_members(self, token_id: str) -> tuple[str, list[str]]:
        parent_event_id = str(
            getattr(self, "_market_parent_event_ids", {}).get(token_id, "")
            or ""
        )
        if not parent_event_id:
            return "", [token_id]
        configured = set(self.market_cfg) | set(self._night_market_cfg)
        members = sorted(
            tid
            for tid in getattr(self, "_parent_event_tokens", {}).get(
                parent_event_id,
                set(),
            )
            if tid in configured
        )
        if token_id not in members:
            members.append(token_id)
        return parent_event_id, members

    async def _enter_parent_event_shock_watch(
        self,
        token_id: str,
        reason: str,
        *,
        primary_decision: str = "watch",
    ) -> bool:
        """Cancel and cool down every configured market in one Gamma event."""
        if not getattr(self, "_parent_event_shock_guard_enabled", True):
            if primary_decision == "quarantine":
                await self._enter_quarantine(token_id, reason)
            elif primary_decision != "skip":
                await self._enter_watch(token_id, reason)
            return False

        parent_event_id, members = self._parent_event_members(token_id)
        if not parent_event_id:
            if primary_decision == "quarantine":
                await self._enter_quarantine(token_id, reason)
            elif primary_decision != "skip":
                await self._enter_watch(token_id, reason)
            return False

        now = time.time()
        last_shock = float(
            self._parent_event_last_shock_ts.get(parent_event_id, 0.0) or 0.0
        )
        if now - last_shock < self._parent_event_shock_debounce_sec:
            return True

        self._parent_event_last_shock_ts[parent_event_id] = now
        cooldown_until = now + self._parent_event_shock_cooldown_sec
        self._parent_event_cooldown_until[parent_event_id] = max(
            cooldown_until,
            float(self._parent_event_cooldown_until.get(parent_event_id, 0.0) or 0.0),
        )
        terminal_states = {
            EVENT_CANCELING,
            EVENT_HALTED_ON_FILL,
            EVENT_HALTED_ON_DATA,
            EVENT_EXIT_PENDING,
            EVENT_PENDING_MANUAL_EXIT,
            EVENT_STARTED_BLOCKED,
        }
        primary_condition = (
            set(self._event_token_ids(token_id))
            if primary_decision == "skip"
            else set()
        )

        async def _protect(member: str) -> None:
            if member in primary_condition:
                return
            if self._event_state_name(member) in terminal_states:
                return
            member_reason = (
                reason
                if member == token_id
                else f"parent_event_shock:{token_id}:{reason}"
            )
            if member == token_id and primary_decision == "quarantine":
                await self._enter_quarantine(member, member_reason)
            else:
                await self._enter_watch(member, member_reason)

        results = await asyncio.gather(
            *(_protect(member) for member in members),
            return_exceptions=True,
        )
        for member, result in zip(members, results):
            if isinstance(result, Exception):
                log(
                    f"[parent-event-guard] member={member} "
                    f"err={result.__class__.__name__}:{result}"
                )

        log(
            f"[parent-event-guard] parent_event={parent_event_id} "
            f"trigger={token_id} members={len(members)} "
            f"cooldown={self._parent_event_shock_cooldown_sec:.0f}s reason={reason}"
        )
        self._notify_status(
            "关联市场已暂停",
            parent_event=parent_event_id,
            trigger=token_id,
            markets=len(members),
            cooldown_sec=self._parent_event_shock_cooldown_sec,
            reason=reason,
        )
        return True

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

    def _aggressive_recovery_ready(self, token_id: str) -> bool:
        """Require several fresh, healthy samples before aggressive re-entry."""
        now = time.time()
        timer_expired = self._vol_check_recovery(token_id)
        if not getattr(self, "_aggressive_recovery_enabled", False):
            return timer_expired

        snapshot = self._market_snapshots.get(token_id)
        snapshot_fresh = bool(
            snapshot is not None and not self._snapshot_is_stale(token_id, snapshot)
        )
        book_valid = bool(
            snapshot is not None
            and snapshot.best_bid > 0
            and snapshot.best_ask > snapshot.best_bid
        )
        cfg = self._get_mcfg(token_id)
        eligibility_ok = True
        if cfg.get("eligibility_managed"):
            eligibility_token = (
                str(cfg.get("paired_token_id") or token_id)
                if cfg.get("_dual_side_auto")
                else token_id
            )
            eligibility_ok = bool(
                (self._eligibility_state.get(eligibility_token) or {}).get(
                    "qualified"
                )
            )

        tracker = self._vol_tracker(token_id)
        decision = evaluate_recovery_sample(
            now=now,
            timer_expired=timer_expired,
            snapshot_fresh=snapshot_fresh,
            book_valid=book_valid,
            eligibility_ok=eligibility_ok,
            last_sample_at=float(tracker.get("recovery_last_sample_at") or 0.0),
            good_samples=int(tracker.get("recovery_good_samples") or 0),
            required_samples=self._aggressive_recovery_required_samples,
            sample_interval_sec=self._aggressive_recovery_sample_interval_sec,
        )
        if decision.reason == "waiting_next_sample":
            return False
        tracker["recovery_good_samples"] = decision.good_samples
        tracker["recovery_last_sample_at"] = now
        tracker["recovery_reason"] = decision.reason
        if decision.ready:
            tracker["recovery_good_samples"] = 0
            tracker["recovery_last_sample_at"] = 0.0
        return decision.ready

    # ---------------------------------------------------------------
    # # P1: Fill exit strategy
    # ---------------------------------------------------------------

    @staticmethod
    def _position_order_remaining(order: Mapping[str, Any]) -> Decimal:
        raw_remaining = order.get("remaining_size")
        if raw_remaining not in (None, ""):
            try:
                return max(Decimal("0"), Decimal(str(raw_remaining)))
            except Exception:
                return Decimal("0")
        try:
            original = Decimal(
                str(order.get("original_size") or order.get("size") or 0)
            )
            matched = max(
                Decimal(str(order.get("size_matched") or 0)),
                Decimal(str(order.get("matched") or 0)),
                Decimal(str(order.get("filled_size") or 0)),
            )
            return max(Decimal("0"), original - matched)
        except Exception:
            return Decimal("0")

    def _position_cost_basis(
        self,
        row: Mapping[str, Any],
        size: Decimal,
    ) -> tuple[Decimal, str]:
        """Return a conservative per-share basis without trusting one API alias."""

        candidates: list[tuple[Decimal, str]] = []
        for key in ("grossInitialValue", "initialValue"):
            try:
                value = Decimal(str(row.get(key) or 0))
            except Exception:
                continue
            if value.is_finite() and value > 0 and size > 0:
                candidates.append((value / size, key))
        try:
            avg_price = Decimal(str(row.get("avgPrice") or row.get("avg_price") or 0))
        except Exception:
            avg_price = Decimal("0")
        if avg_price.is_finite() and avg_price > 0:
            candidates.append((avg_price, "avgPrice"))
        if not candidates:
            return Decimal("0"), "unavailable"

        basis, source = max(candidates, key=lambda item: item[0])
        for key in ("entryFeesUsdc", "entry_fee_usdc", "feesPaid"):
            try:
                fees = Decimal(str(row.get(key) or 0))
            except Exception:
                continue
            if fees.is_finite() and fees > 0 and size > 0:
                basis += fees / size
                source = f"{source}+{key}"
                break
        if not basis.is_finite() or basis <= 0 or basis >= 1:
            return Decimal("0"), "invalid"
        return basis, source

    def _normalize_configured_positions(
        self,
        payload: Any,
    ) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            raise RuntimeError("invalid positions response")
        configured = set(self.market_cfg) | set(self._night_market_cfg)
        funder = str(self._funder_lc or "").strip().lower()
        positions: list[dict[str, Any]] = []
        for row in payload:
            if not isinstance(row, Mapping):
                raise RuntimeError("invalid position row")
            row_account = str(
                row.get("proxyWallet") or row.get("proxy_wallet") or row.get("user") or ""
            ).strip().lower()
            if row_account and row_account != funder:
                continue
            redeemable = row.get("redeemable") is True or str(
                row.get("redeemable") or ""
            ).strip().lower() == "true"
            if redeemable:
                continue
            token_id = str(
                row.get("asset") or row.get("token_id") or row.get("tokenId") or ""
            ).strip()
            if token_id not in configured:
                continue
            try:
                size = Decimal(str(row.get("size") or 0))
            except Exception:
                continue
            if not size.is_finite() or size <= Decimal(str(self._exit_dust_threshold)):
                continue
            cost_basis, cost_source = self._position_cost_basis(row, size)
            positions.append(
                {
                    "token_id": token_id,
                    "size": size,
                    "cost_basis": cost_basis,
                    "cost_source": cost_source,
                    "title": str(row.get("title") or row.get("question") or "")[:160],
                }
            )
        positions.sort(key=lambda item: item["token_id"])
        return positions

    async def _fetch_configured_positions(self) -> list[dict[str, Any]]:
        funder = str(self._funder_lc or "").strip().lower()
        if not re.fullmatch(r"0x[a-f0-9]{40}", funder):
            raise RuntimeError("invalid funder for position reconcile")
        data_api = str(
            self.cfg.get("data_api_url") or "https://data-api.polymarket.com"
        ).rstrip("/")

        def fetch() -> Any:
            response = requests.get(
                f"{data_api}/positions",
                params={"user": funder, "sizeThreshold": "0", "limit": 500},
                timeout=8,
                proxies=self._read_proxies_for_token(),
            )
            response.raise_for_status()
            return response.json()

        payload = await asyncio.to_thread(fetch)
        return self._normalize_configured_positions(payload)

    def _position_has_managed_evidence(self, token_id: str) -> bool:
        token_id = str(token_id)
        if token_id in self._pending_unwind_token_ids():
            return True
        if token_id in self._active_exit_orders:
            return True

        cutoff = time.time() - self._position_reconcile_managed_max_age_sec
        latest_fill = 0.0
        for fill in self._fills_record:
            if not isinstance(fill, Mapping):
                continue
            if str(fill.get("token_id") or "") != token_id:
                continue
            if str(fill.get("final_state") or "") != EVENT_HALTED_ON_FILL:
                continue
            try:
                size = Decimal(str(fill.get("size") or 0))
                fill_ts = float(fill.get("ts") or 0)
            except Exception:
                continue
            if size > Decimal(str(self._exit_dust_threshold)):
                latest_fill = max(latest_fill, fill_ts)
        latest_exit = 0.0
        for exit_row in self._exit_records:
            if not isinstance(exit_row, Mapping):
                continue
            if str(exit_row.get("token_id") or "") != token_id:
                continue
            try:
                latest_exit = max(latest_exit, float(exit_row.get("ts") or 0))
            except Exception:
                continue
        manual_sell_ts = float(
            getattr(self, "_position_reconcile_manual_sell_ts", {}).get(
                token_id, 0.0
            )
            or 0.0
        )
        return latest_fill >= cutoff and latest_fill > max(
            latest_exit, manual_sell_ts
        )

    def _position_managed_capacity(self, token_id: str) -> Decimal:
        """Return the largest exact-token inventory size supported by bot records."""

        token_id = str(token_id)
        cutoff = time.time() - self._position_reconcile_managed_max_age_sec
        latest_exit = 0.0
        for exit_row in self._exit_records:
            if not isinstance(exit_row, Mapping):
                continue
            if str(exit_row.get("token_id") or "") != token_id:
                continue
            try:
                latest_exit = max(latest_exit, float(exit_row.get("ts") or 0))
            except Exception:
                continue
        manual_sell_ts = float(
            getattr(self, "_position_reconcile_manual_sell_ts", {}).get(
                token_id, 0.0
            )
            or 0.0
        )
        evidence_after = max(cutoff, latest_exit, manual_sell_ts)
        capacities: list[Decimal] = []

        for unwind in self._pending_unwinds:
            if not isinstance(unwind, Mapping):
                continue
            if str(unwind.get("token_id") or "") != token_id:
                continue
            try:
                placed_at = float(unwind.get("placed_at") or 0.0)
            except Exception:
                placed_at = 0.0
            if placed_at < evidence_after:
                continue
            for key in ("exit_size", "fill_size", "reported_fill_size"):
                try:
                    size = Decimal(str(unwind.get(key) or 0))
                except Exception:
                    continue
                if size.is_finite() and size > 0:
                    capacities.append(size)

        for fill in self._fills_record:
            if not isinstance(fill, Mapping):
                continue
            if str(fill.get("token_id") or "") != token_id:
                continue
            if str(fill.get("final_state") or "") != EVENT_HALTED_ON_FILL:
                continue
            try:
                fill_ts = float(fill.get("ts") or 0.0)
                size = Decimal(str(fill.get("size") or 0))
            except Exception:
                continue
            if fill_ts >= evidence_after and size.is_finite() and size > 0:
                capacities.append(size)

        return max(capacities, default=Decimal("0"))

    def _position_reconcile_notice(
        self,
        token_id: str,
        status: str,
        **details: Any,
    ) -> None:
        signature = json.dumps(
            {"status": status, **details},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )
        if self._position_reconcile_alerted.get(token_id) == signature:
            return
        self._position_reconcile_alerted[token_id] = signature
        self._notify_attention("持仓对账需要检查", market=token_id, status=status, **details)

    async def _position_reconcile_once(
        self,
        *,
        positions: Optional[list[dict[str, Any]]] = None,
        open_orders: Optional[list[dict[str, Any]]] = None,
    ) -> None:
        positions = (
            await self._fetch_configured_positions()
            if positions is None
            else positions
        )
        if open_orders is None:
            raw_orders = await self._read_open_orders("position_reconcile")
            if not isinstance(raw_orders, list):
                raise RuntimeError("invalid open orders response")
            open_orders = raw_orders

        results: list[dict[str, Any]] = []
        managed_count = 0
        manual_count = 0
        position_token_ids = {
            str(position.get("token_id") or "") for position in positions
        }
        tracked_exit_tokens = (
            self._pending_unwind_token_ids() | set(self._active_exit_orders)
        )
        self._position_reconcile_managed_tokens.intersection_update(
            position_token_ids | tracked_exit_tokens
        )
        for position in positions:
            token_id = str(position.get("token_id") or "")
            size = Decimal(str(position.get("size") or 0))
            basis = Decimal(str(position.get("cost_basis") or 0))
            tick = Decimal(str(self._get_mcfg(token_id).get("tick") or "0.01"))
            no_loss_price = self._ceil_to_tick(basis, tick) if basis > 0 else Decimal("0")
            managed_evidence = self._position_has_managed_evidence(token_id)
            managed_capacity = self._position_managed_capacity(token_id)
            size_explained = bool(
                managed_capacity > 0
                and size
                <= managed_capacity + Decimal(str(self._exit_dust_threshold))
            )
            managed = managed_evidence and size_explained
            if managed:
                managed_count += 1
                self._position_reconcile_managed_tokens.add(token_id)

            live_sells = [
                order
                for order in open_orders
                if isinstance(order, dict)
                and _order_is_live(order)
                and self._order_token_id(order) == token_id
                and self._order_side(order) == "SELL"
                and self._position_order_remaining(order) > 0
            ]
            coverage = sum(
                (self._position_order_remaining(order) for order in live_sells),
                Decimal("0"),
            )
            result = {
                "token_id": token_id,
                "size": float(size),
                "cost_basis": float(basis) if basis > 0 else None,
                "no_loss_price": float(no_loss_price) if no_loss_price > 0 else None,
                "managed": managed,
                "managed_evidence": managed_evidence,
                "managed_capacity": (
                    float(managed_capacity) if managed_capacity > 0 else None
                ),
                "live_sell_orders": len(live_sells),
                "live_sell_coverage": float(coverage),
                "status": "checking",
            }

            if not managed or no_loss_price <= 0:
                manual_count += 1
                await self._cancel_token_orders(
                    token_id,
                    reason="position_reconcile_unmanaged",
                )
                self._set_event_state(
                    token_id,
                    EVENT_PENDING_MANUAL_EXIT,
                    "unmanaged_position_detected",
                )
                result["status"] = (
                    "manual_review_unexplained_size"
                    if managed_evidence and not size_explained
                    else "manual_review_unmanaged"
                )
                self._position_reconcile_notice(
                    token_id,
                    result["status"],
                    position=f"{float(size):,.4f} 份",
                    managed_capacity=(
                        f"{float(managed_capacity):,.4f} 份"
                        if managed_capacity > 0
                        else "无可核验记录"
                    ),
                    action="已停止该事件继续买入；数量或来源不能完全核验，不自动卖出",
                )
                results.append(result)
                continue

            if live_sells:
                prices = [self._order_price(order) for order in live_sells]
                sufficient = coverage + Decimal(str(self._exit_dust_threshold)) >= size
                prices_safe = all(price >= no_loss_price for price in prices)
                if len(live_sells) == 1 and sufficient and prices_safe:
                    order = live_sells[0]
                    order_id = self._order_id(order)
                    self._active_exit_orders[token_id] = order_id
                    if not any(
                        isinstance(row, dict)
                        and str(row.get("token_id") or "") == token_id
                        for row in self._pending_unwinds
                    ):
                        self._pending_unwinds.append(
                            {
                                "token_id": token_id,
                                "source_token_id": token_id,
                                "fill_price": float(basis),
                                "fill_size": float(size),
                                "exit_size": float(coverage),
                                "reported_fill_size": float(size),
                                "sell_price": float(prices[0]),
                                "order_id": order_id,
                                "placed_at": time.time(),
                                "reason": "position_reconcile_rehydrated",
                                "reconciled": True,
                                "strict_no_loss": True,
                            }
                        )
                    self._set_event_state(
                        token_id,
                        EVENT_EXIT_PENDING,
                        "position_reconcile_rehydrated",
                    )
                    result["status"] = "exit_rehydrated"
                    results.append(result)
                    continue

                manual_count += 1
                await self._cancel_token_orders(
                    token_id,
                    reason="position_reconcile_ambiguous_sell",
                )
                self._set_event_state(
                    token_id,
                    EVENT_PENDING_MANUAL_EXIT,
                    "ambiguous_exit_sell_coverage",
                )
                result["status"] = (
                    "manual_review_sell_below_cost"
                    if not prices_safe
                    else "manual_review_partial_or_multiple_sells"
                )
                self._position_reconcile_notice(
                    token_id,
                    result["status"],
                    position=f"{float(size):,.4f} 份",
                    sell_coverage=f"{float(coverage):,.4f} 份",
                    no_loss_price=f"${float(no_loss_price):.4f}",
                    action="保留现有卖单并停止继续买入；需人工检查",
                )
                results.append(result)
                continue

            if token_id in self._position_reconcile_inflight:
                result["status"] = "exit_request_inflight"
                results.append(result)
                continue

            self._position_reconcile_inflight.add(token_id)
            try:
                self._set_event_state(
                    token_id,
                    EVENT_CANCELING,
                    "position_reconcile_uncovered",
                )
                await self._attempt_exit_sell(
                    token_id,
                    basis,
                    size,
                    "position_reconcile_uncovered",
                    strict_no_loss=True,
                    allow_position_fallback=False,
                )
                exit_tracked = token_id in self._active_exit_orders
                if exit_tracked:
                    result["status"] = "exit_requested"
                else:
                    manual_count += 1
                    result["status"] = "exit_not_placed"
                    self._position_reconcile_notice(
                        token_id,
                        result["status"],
                        position=f"{float(size):,.4f} 份",
                        no_loss_price=f"${float(no_loss_price):.4f}",
                        action="未确认退出单已挂出；保持停止买入并等待下轮对账",
                    )
            finally:
                self._position_reconcile_inflight.discard(token_id)
            results.append(result)

        self._position_reconcile_state = {
            "enabled": True,
            "last_checked_at": time.time(),
            "status": "ready",
            "configured_positions": len(positions),
            "managed_positions": managed_count,
            "manual_review_positions": manual_count,
            "last_error": None,
            "positions": results,
        }

    async def position_reconcile_loop(self) -> None:
        await asyncio.sleep(min(10.0, self._position_reconcile_interval_sec))
        while self._running:
            try:
                await self._position_reconcile_once()
            except Exception as exc:
                self._position_reconcile_state.update(
                    {
                        "last_checked_at": time.time(),
                        "status": "error",
                        "last_error": f"{exc.__class__.__name__}: {exc}",
                    }
                )
                log(f"[position-reconcile] error={exc.__class__.__name__}:{exc}")
            await asyncio.sleep(self._position_reconcile_interval_sec)

    async def _delayed_balance_drop_reconcile(self, token_id: str, reason: str, delay_sec: float = 30.0) -> None:
        await asyncio.sleep(delay_sec)
        try:
            # Check if exit is already in progress
            if token_id in self._active_exit_orders:
                log(f"[balance-drop-reconcile] token={token_id} exit already in progress, skipping")
                return
            all_tokens = list(set(list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())))
            found_tid, pos = await self._scan_for_position(all_tokens)
            if not found_tid:
                log(f"[balance-drop-reconcile] no position found on any token, skipping")
                return
            # Get best ask for sell price
            price = Decimal("0")
            snap = self._market_snapshots.get(found_tid)
            if snap and snap.best_ask > 0:
                price = snap.best_ask
            if price <= 0:
                tob = self.market_states.get(found_tid)
                if tob and tob.best_ask > 0:
                    price = tob.best_ask
            log(f"[balance-drop-reconcile] token={found_tid} pos={pos} price={price} retry_exit_after={delay_sec}s")
            self._spawn_bg(self._attempt_exit_sell(found_tid, Decimal(str(price or 0)), Decimal(str(pos)), f"balance_drop_reconcile:{reason}"), name=f"balance_drop_reconcile:{found_tid}")
        except Exception as e:
            log(f"[balance-drop-reconcile] token={token_id} err={e}")

    def _calc_exit_price(self, token_id: str, fill_price: Decimal, stop_loss_floor: Decimal,
                         market_bid: Decimal, market_ask: Decimal, tick: Decimal,
                         elapsed_sec: float = 0) -> Decimal:
        """Calculate best exit SELL price given market conditions and stop-loss floor.
        Policy: rest as maker (avoid taker fee). Only cross the book as taker
        after stop_loss_wait_sec has elapsed AND bid is within stop_loss_floor.
        Night-pool tokens NEVER auto-cross — a long-running unfilled night exit
        rolls over to PENDING_MANUAL_EXIT via the safety deadline instead.
        """
        sell_price = fill_price  # default: breakeven
        stop_loss_wait = float(self._exit_stop_loss_wait_sec)
        is_night_token = token_id in self._night_market_cfg

        if market_ask > 0 and market_ask >= fill_price:
            # Rest as maker at best_ask (tied with or above fill) — no taker fee
            sell_price = market_ask
            log(f"[exit-price] {token_id[:16]} maker at ask={market_ask} >= fill={fill_price}")
        elif (market_bid > 0 and market_bid >= stop_loss_floor
              and elapsed_sec >= stop_loss_wait and not is_night_token):
            # Stop-loss triggered: cross the book (taker) to cut loss. DAY-POOL ONLY.
            sell_price = market_bid
            loss_est = (fill_price - market_bid)
            log(f"[exit-price] {token_id[:16]} stop-loss taker: bid={market_bid} loss/share={loss_est} waited={elapsed_sec/3600:.1f}h")
        elif fill_price > 0:
            # Market moved against us OR stop-loss not elapsed — post at fill_price as maker.
            # Our SELL sits above current best_ask, waits for buyers to sweep up to fill.
            # No taker fee; may take time in thin markets but guarantees no price loss.
            sell_price = fill_price
            log(f"[exit-price] {token_id[:16]} maker breakeven: fill={fill_price} bid={market_bid} ask={market_ask}")
        elif market_ask > 0:
            sell_price = market_ask
        elif market_bid > 0:
            sell_price = market_bid

        # Floor: never sell below stop_loss_floor
        if stop_loss_floor > 0 and sell_price < stop_loss_floor:
            sell_price = stop_loss_floor
            log(f"[exit-price] {token_id[:16]} clamped to stop_loss_floor={stop_loss_floor}")

        return sell_price

    async def _attempt_exit_sell(
        self,
        token_id: str,
        fill_price: Decimal,
        fill_size: Decimal,
        reason: str,
        *,
        strict_no_loss: bool = False,
        allow_position_fallback: bool = True,
    ) -> None:
        """After fill detected:
        1. Cancel all BUY orders for this token (keep only the upcoming SELL)
        2. Place a SELL order at fill_price to recover position
        3. Monitor until sold, then resume normal quoting

        Dedup: paired-halt path and balance_drop_watch path can both trigger
        exit for the same fill (seen 2026-04-24 23:47-23:49). If another exit
        is already in flight for this token we return early — the first exit
        monitor handles the full lifecycle; a second _place_sell_order would
        over-commit tokens and break reprice with 'not enough balance'.
        """
        source_token_id = token_id

        # Brief delay to let fill-ws / trade-poll settle
        await asyncio.sleep(max(1, self._exit_delay_sec))

        # Allow exit from multiple states (kill switch may change state concurrently)
        state = self._event_state_name(token_id)
        _exit_allowed_states = {EVENT_HALTED_ON_FILL, EVENT_CANCELING, EVENT_COOLDOWN, EVENT_ACTIVE}
        if state not in _exit_allowed_states:
            log(f"[exit] token={token_id} skip exit, state={state} not in allowed states")
            return

        # Dedup guard — a concurrent exit is already live for this token.
        existing_oid = self._active_exit_orders.get(token_id)
        if existing_oid:
            log(f"[exit] token={token_id[:16]} skip exit, already in flight oid={existing_oid[:12]} (reason={reason})")
            return

        self._set_event_state(token_id, EVENT_EXIT_PENDING, f"exit_sell:{reason}")

        # Step 1: Cancel all existing orders for this specific token
        canceled = await self._cancel_token_orders(
            token_id,
            reason=f"exit_before_sell:{reason}",
        )
        if not canceled:
            log(
                f"[exit] token={token_id} token cancel unconfirmed; "
                "falling back to account-wide BUY cancel"
            )
            try:
                canceled = await self._cancel_all_except_exit()
            except Exception as exc:
                log(f"[exit] token={token_id} account-wide cancel failed: {exc}")
                canceled = False
        if not canceled:
            self._set_event_state(
                token_id,
                EVENT_PENDING_MANUAL_EXIT,
                f"buy_cancel_unconfirmed:{reason}",
            )
            self.send_discord(
                f"退出流程暂停\n市场：{self._discord_market_name(token_id)}\n"
                "原因：无法确认买单已经撤净\n"
                "系统处理：已停止自动卖出并启动全局安全撤单"
            )
            self._spawn_bg(
                self.trigger_global_kill_switch(
                    f"exit_buy_cancel_unconfirmed:{token_id}"
                ),
                name=f"exit_cancel_kill_switch:{token_id}",
            )
            return
        log(f"[exit] token={token_id} confirmed all BUY orders canceled")

        # Step 2: Check actual position — scan all tokens if the attributed one has none
        position = await self._get_token_position(token_id)
        if position is None or position <= 0:
            if not allow_position_fallback:
                log(
                    f"[exit] token={token_id} exact position unavailable; "
                    f"cross-token fallback disabled reason={reason}"
                )
                self._set_event_state(
                    token_id,
                    EVENT_PENDING_MANUAL_EXIT,
                    f"exact_position_unavailable:{reason}",
                )
                self.send_discord(
                    f"退出仓位需要复核\n市场：{self._discord_market_name(token_id)}\n"
                    "原因：未能确认该市场的精确仓位\n"
                    "系统处理：不扫描或卖出其他市场仓位，保持停止买入"
                )
                return
            log(f"[exit] token={token_id} position={position}, scanning all tokens...")
            all_tokens = list(set(list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())))
            found_tid, found_pos = await self._scan_for_position([t for t in all_tokens if t != token_id])
            if found_tid:
                log(f"[exit] found position={found_pos} on token={found_tid} (was attributed to {token_id})")
                # When position is on paired token, adjust fill_price:
                # NO fill at P → YES position worth ~(1-P), and vice versa
                if found_tid != token_id and fill_price > 0:
                    old_fp = fill_price
                    fill_price = Decimal("1") - fill_price
                    log(f"[exit] {found_tid[:16]} paired token switch: fill_price {old_fp} → {fill_price}")
                token_id = found_tid
                managed_positions = getattr(
                    self, "_position_reconcile_managed_tokens", set()
                )
                managed_positions.add(token_id)
                self._position_reconcile_managed_tokens = managed_positions
                position = found_pos
            else:
                # Keep scanning until found
                scan_attempt = 0
                while self._running:
                    scan_attempt += 1
                    log(f"[exit] scan attempt {scan_attempt}: no position found, retrying in 60s...")
                    await asyncio.sleep(60)
                    found_tid, found_pos = await self._scan_for_position(all_tokens)
                    if found_tid:
                        log(f"[exit] found position={found_pos} on token={found_tid} after {scan_attempt} retries")
                        # Paired token fill_price adjustment
                        if found_tid != token_id and fill_price > 0:
                            old_fp = fill_price
                            fill_price = Decimal("1") - fill_price
                            log(f"[exit] {found_tid[:16]} paired token switch: fill_price {old_fp} → {fill_price}")
                        token_id = found_tid
                        managed_positions = getattr(
                            self, "_position_reconcile_managed_tokens", set()
                        )
                        managed_positions.add(token_id)
                        self._position_reconcile_managed_tokens = managed_positions
                        position = found_pos
                        break
                    if scan_attempt >= 10:
                        self.send_fill_discord(
                            f"正在查找成交后的仓位\n已尝试：{scan_attempt} 次\n"
                            "系统处理：继续查询，暂不恢复挂单"
                        )
                if not position or position <= 0:
                    return

        # Position stability loop: keep re-checking until position stops changing
        POS_STABLE_REQUIRED = 3   # consecutive stable reads required
        POS_CHECK_INTERVAL = 2.0  # seconds between reads
        POS_MAX_CHECKS = 10       # hard cap to avoid infinite loop
        stable_count = 0
        prev_pos = float(position)
        for _chk in range(POS_MAX_CHECKS):
            await asyncio.sleep(POS_CHECK_INTERVAL)
            cur = await self._get_token_position(token_id)
            if cur is None or cur < 0:
                continue  # API error — skip, don't count
            delta = abs(cur - prev_pos)
            threshold = max(0.5, prev_pos * 0.05)
            if delta <= threshold:
                stable_count += 1
            else:
                log(f"[exit] {token_id[:16]} pos shifting {prev_pos}->{cur} (check {_chk+1})")
                stable_count = 0
            prev_pos = cur
            if stable_count >= POS_STABLE_REQUIRED:
                break
        position = prev_pos
        log(f"[exit] {token_id[:16]} pos confirmed={position} stable_checks={stable_count}/{POS_STABLE_REQUIRED}")

        # Dust guard: skip sell for tiny positions
        # Policy: on any fill, keep global halt until exit is FULLY complete.
        # Dust position is NOT a complete exit — stay halted, require manual clear + restart.
        if position is not None and 0 < position <= self._exit_dust_threshold:
            log(f"[exit] token={token_id[:16]} dust_pos={position} thr={self._exit_dust_threshold} — holding global halt")
            self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, "dust_position")
            self.send_discord(
                f"发现微量剩余仓位\n市场：{self._discord_market_name(token_id)}\n"
                f"剩余：{float(position):,.4f} 份\n"
                "需手动清仓；处理完成前其他市场保持暂停"
            )
            return

        sell_size = Decimal(str(position)) if position and position > 0 else fill_size
        tick = Decimal("0.01")

        # Determine the best available market price for selling
        market_bid = Decimal("0")
        market_ask = Decimal("0")
        snap = self._market_snapshots.get(token_id)
        if snap:
            if snap.best_bid > 0:
                market_bid = snap.best_bid
            if snap.best_ask > 0:
                market_ask = snap.best_ask
        if market_bid <= 0 or market_ask <= 0:
            tob = self.market_states.get(token_id)
            if tob:
                if market_bid <= 0 and tob.best_bid > 0:
                    market_bid = tob.best_bid
                if market_ask <= 0 and tob.best_ask > 0:
                    market_ask = tob.best_ask

        # Exit pricing strategy: try to profit, accept stop-loss within max_loss_usd
        stop_loss_floor = Decimal("0")
        if fill_price > 0 and sell_size > 0:
            if strict_no_loss:
                stop_loss_floor = self._ceil_to_tick(fill_price, tick)
            else:
                stop_loss_floor = fill_price - self._exit_max_loss_usd / sell_size
                if stop_loss_floor < Decimal("0.01"):
                    stop_loss_floor = Decimal("0.01")
            log(f"[exit] {token_id[:16]} stop_loss_floor={stop_loss_floor} (fill={fill_price} max_loss={self._exit_max_loss_usd} sz={sell_size})")

        sell_price = self._calc_exit_price(token_id, fill_price, stop_loss_floor, market_bid, market_ask, tick)

        # Hard guard: NEVER sell at absurdly low price — go manual instead
        if sell_price <= Decimal("0.02"):
            log(f"[exit] {token_id[:16]} sell_price={sell_price} too low, refusing to sell at loss — MANUAL EXIT")
            self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, f"sell_price_too_low:{sell_price}")
            self.send_discord(
                f"已拒绝异常低价卖出\n市场：{self._discord_market_name(token_id)}\n"
                f"价格：${float(sell_price):.4f}\n数量：{float(sell_size):,.2f} 份\n"
                "需手动处理；其他市场仍暂停"
            )
            return

        # Step 3: Place SELL order
        for attempt in range(1, self._exit_retry_count + 1):
            try:
                log(f"[exit] token={token_id} placing SELL attempt={attempt} price={sell_price} size={sell_size}")
                resp = await self._place_sell_order(token_id, sell_price, sell_size)
                order_id = str((resp or {}).get("orderID") or (resp or {}).get("id") or "")
                log(f"[exit] token={token_id} SELL order placed order_id={order_id}")
                # Track the exit sell order so kill switch won't cancel it
                self._active_exit_orders[token_id] = order_id
                self._pending_unwinds.append({
                    "token_id": token_id,
                    "source_token_id": source_token_id,
                    "fill_price": float(fill_price),
                    "fill_size": float(sell_size),
                    "exit_size": float(sell_size),
                    "reported_fill_size": float(fill_size),
                    "sell_price": float(sell_price),
                    "order_id": order_id,
                    "placed_at": time.time(),
                    "reason": reason,
                    "strict_no_loss": strict_no_loss,
                })
                self.notify_discord(
                    "退出单已提交",
                    (
                        f"市场：{self._discord_market_name(token_id)}\n"
                        f"卖出：{float(sell_size):,.2f} 份 × ${float(sell_price):.4f}\n"
                        "系统处理：等待成交并确认仓位归零"
                    ),
                    "warning",
                )
                # Step 4: Monitor until sold, then resume
                self._spawn_bg(self._monitor_exit_order(token_id, order_id, sell_price, sell_size, fill_price, stop_loss_floor, reason), name=f"monitor_exit:{token_id}")
                return
            except Exception as e:
                log(f"[exit] token={token_id} SELL attempt={attempt} failed: {e}")
                if attempt < self._exit_retry_count:
                    await asyncio.sleep(3)

        # all retries failed — don't resume, wait for manual
        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, f"exit_sell_failed:{reason}")
        self.send_discord(
            f"退出单提交失败\n市场：{self._discord_market_name(token_id)}\n"
            f"已尝试：{self._exit_retry_count} 次\n"
            f"成交价：${float(fill_price):.4f}\n数量：{float(sell_size):,.2f} 份\n"
            "需手动处理；其他市场仍暂停"
        )

    async def _cancel_token_orders(
        self,
        token_id: str,
        *,
        reason: str = "token_buy_cleanup",
        max_attempts: int = 3,
    ) -> bool:
        """Cancel and remotely confirm every live BUY for one token.

        py-clob-client-v2 has no reliable single-order ``cancel`` method. The
        previous implementation swallowed those per-order failures and could
        report cleanup even though orders remained live. Use the reviewed
        batch path and re-read the official open-order endpoint instead.
        """
        def _live_buys(orders: list[dict]) -> tuple[list[str], bool]:
            ids: list[str] = []
            unknown_side = False
            for order in orders:
                if not isinstance(order, dict):
                    continue
                asset = str(
                    order.get("asset_id") or order.get("token_id") or ""
                )
                if asset != token_id or not _order_is_live(order):
                    continue
                side = str(order.get("side") or "").upper()
                if side == "SELL":
                    continue
                if side != "BUY":
                    unknown_side = True
                    continue
                order_id = str(order.get("id") or order.get("orderID") or "")
                if order_id:
                    ids.append(order_id)
            return ids, unknown_side

        attempts = max(1, int(max_attempts))
        for attempt in range(1, attempts + 1):
            try:
                orders = await self._read_open_orders("cancel_token_read")
            except Exception as exc:
                log(
                    f"[cancel-token] token={token_id} reason={reason} "
                    f"attempt={attempt}/{attempts} read_error={exc}"
                )
                if attempt < attempts:
                    await asyncio.sleep(min(0.25, 0.05 * attempt))
                    continue
                return False
            if not isinstance(orders, list):
                log(
                    f"[cancel-token] token={token_id} reason={reason} "
                    "invalid_open_orders_response"
                )
                return False

            ids, unknown_side = _live_buys(orders)
            if unknown_side:
                log(
                    f"[cancel-token] token={token_id} reason={reason} "
                    "active_order_with_unknown_side"
                )
                return False
            if not ids:
                return True

            try:
                cleared = await self._cancel_order_ids(
                    token_id,
                    ids,
                    f"{reason}:attempt_{attempt}",
                )
            except Exception as exc:
                log(
                    f"[cancel-token] token={token_id} reason={reason} "
                    f"attempt={attempt}/{attempts} cancel_error={exc}"
                )
                cleared = False
            log(
                f"[cancel-token] token={token_id} reason={reason} "
                f"attempt={attempt}/{attempts} ids={len(ids)} ack={cleared}"
            )
            await asyncio.sleep(min(0.25, 0.05 * attempt))

        try:
            remaining = await self._read_open_orders("cancel_token_verify")
        except Exception as exc:
            log(
                f"[cancel-token] token={token_id} reason={reason} "
                f"final_read_error={exc}"
            )
            return False
        if not isinstance(remaining, list):
            return False
        remaining_ids, unknown_side = _live_buys(remaining)
        if unknown_side or remaining_ids:
            log(
                f"[cancel-token] token={token_id} reason={reason} "
                f"unconfirmed_remaining={len(remaining_ids)} "
                f"unknown_side={unknown_side}"
            )
            return False
        return True

    async def _cancel_risk_buys(self, token_id: str, reason: str) -> bool:
        """Cancel BUY liquidity and fail closed until the venue confirms it."""
        # Risk paths already know the order IDs they are protecting. Dispatch
        # those cancellations before spending another network round-trip on
        # get_open_orders(), then use the official endpoint below to verify
        # and catch any order missing from the local cache.
        cached_ids = [
            self._order_id(order)
            for order in self._cached_live_orders(token_id)
            if _order_is_live(order) and self._order_side(order) == "BUY"
        ]
        if cached_ids:
            fast_ack = await self._cancel_order_ids(
                token_id,
                cached_ids,
                f"{reason}:fast_cached",
            )
            log(
                f"[risk-cancel] token={token_id} reason={reason} "
                f"fast_cached={len(cached_ids)} ack={fast_ack}"
            )

        confirmed = await self._cancel_token_orders(
            token_id,
            reason=reason,
        )
        if confirmed:
            self._mark_latency(token_id, "t_orders_cleared")
            return True

        log(
            f"[risk-cancel] token={token_id} reason={reason} "
            "token cancellation unconfirmed; escalating global cancel"
        )
        try:
            self._notify_risk(
                "风险挂单撤销未确认",
                token=token_id,
                reason=reason,
            )
        except Exception:
            pass
        try:
            await self.trigger_global_kill_switch(
                f"risk_cancel_unconfirmed:{token_id}:{reason}"
            )
        except Exception as exc:
            log(
                f"[risk-cancel] token={token_id} reason={reason} "
                f"global_cancel_error={exc}"
            )

        confirmed = await self._cancel_token_orders(
            token_id,
            reason=f"{reason}:post_global_verify",
            max_attempts=1,
        )
        if confirmed:
            self._mark_latency(token_id, "t_orders_cleared")
        else:
            log(
                f"[risk-cancel] token={token_id} reason={reason} "
                "still_unconfirmed"
            )
        return confirmed

    async def _cancel_coordinated_pair_quotes(
        self,
        token_id: str,
        reason: str,
    ) -> bool:
        """Cancel both coordinated YES/NO BUY quote legs as one risk unit."""
        paired = self._coordinated_pair_token(token_id)
        if not paired:
            return await self._cancel_risk_buys(token_id, reason)

        pair_key = "|".join(sorted((str(token_id), str(paired))))
        inflight = getattr(self, "_aggressive_pair_cancel_inflight", None)
        if inflight is None:
            inflight = set()
            self._aggressive_pair_cancel_inflight = inflight
        if pair_key in inflight:
            return True

        inflight.add(pair_key)
        pair_reason = f"paired_quote:{reason}"
        targets = (paired, token_id)
        defense_block_sec = float(
            getattr(self, "_defense_requote_block_sec", 0.0)
        )
        block_sec = (
            float(
                getattr(
                    self,
                    "_move_back_requote_block_sec",
                    defense_block_sec,
                )
            )
            if "move_back" in str(reason).lower()
            else defense_block_sec
        )
        block_until = time.time() + max(0.0, block_sec)
        results: list[bool] = []
        try:
            # Preempt both planners before the first network round-trip so a
            # sibling task cannot recreate the opposite leg during cleanup.
            for target in targets:
                self._arm_halt_preemption(target, pair_reason)
                self._defense_block_until[target] = max(
                    float(self._defense_block_until.get(target, 0.0)),
                    block_until,
                )
                if self._event_state_name(target) not in {
                    EVENT_CANCELING,
                    EVENT_HALTED_ON_FILL,
                    EVENT_HALTED_ON_DATA,
                    EVENT_COOLDOWN,
                    EVENT_STARTED_BLOCKED,
                    EVENT_WATCH,
                    EVENT_QUARANTINE,
                    EVENT_PENDING_MANUAL_EXIT,
                    EVENT_EXIT_PENDING,
                }:
                    self._set_event_state(target, EVENT_DEFENSIVE, pair_reason)

            # Cancel the opposite leg first: it is the leg otherwise left live
            # when the triggering side fails its feasibility gate.
            for target in targets:
                results.append(
                    await self._cancel_risk_buys(target, pair_reason)
                )

            cleared = all(results)
            if not cleared:
                for target in targets:
                    self._set_event_state(
                        target,
                        EVENT_CANCELING,
                        f"{pair_reason}:cancel_unconfirmed",
                    )
                return False

            log(
                f"[paired-quote] token={token_id[:16]} "
                f"paired={paired[:16]} action=cancel_both reason={reason}"
            )
            return True
        finally:
            if len(results) == len(targets) and all(results):
                for target in targets:
                    self._clear_halt_preemption(target)
            inflight.discard(pair_key)

    async def _cancel_aggressive_pair_quotes(
        self,
        token_id: str,
        reason: str,
    ) -> bool:
        """Backward-compatible wrapper for aggressive pair callers/tests."""
        if not self._aggressive_pair_token(token_id):
            return await self._cancel_risk_buys(token_id, reason)
        return await self._cancel_coordinated_pair_quotes(token_id, reason)

    def _live_buy_count(self, orders: Iterable[dict]) -> int:
        return sum(
            1
            for order in orders
            if _order_is_live(order) and self._order_side(order) == "BUY"
        )

    def _cached_live_buy_count(self, token_id: str) -> int:
        return self._live_buy_count(self._market_live_orders.get(token_id, []))

    async def paired_quote_invariant_loop(self) -> None:
        """Remove any stable/aggressive LP BUY leg left live without its pair.

        Pair submission is necessarily sequential at the signer, so a short
        grace period is allowed. After that window, a locally observed single
        leg is canceled as a fail-closed event-level unit. Exit SELL orders are
        intentionally ignored and remain owned by the unwind workflow.
        """
        while self._running:
            try:
                now = time.time()
                seen: set[str] = set()
                configured_tokens = set(self.market_cfg) | set(self._night_market_cfg)
                for token_id in configured_tokens:
                    paired = self._coordinated_pair_token(token_id)
                    if not paired:
                        continue
                    pair_key = self._aggressive_pair_key(token_id, paired)
                    if pair_key in seen:
                        continue
                    seen.add(pair_key)

                    first_count = self._cached_live_buy_count(token_id)
                    second_count = self._cached_live_buy_count(paired)
                    if (first_count > 0) == (second_count > 0):
                        self._paired_single_leg_since.pop(pair_key, None)
                        continue

                    single_since = self._paired_single_leg_since.setdefault(
                        pair_key,
                        now,
                    )
                    if now - single_since < self._paired_single_leg_grace_sec:
                        continue

                    # Cache updates for sibling tokens are not atomic. Confirm
                    # the apparent mismatch against the venue before taking a
                    # destructive action, otherwise a normal refresh skew can
                    # cause needless churn of a healthy pair.
                    try:
                        self._invalidate_all_orders_cache()
                        first_count = self._live_buy_count(
                            await self._refresh_live_orders(token_id)
                        )
                        second_count = self._live_buy_count(
                            await self._refresh_live_orders(paired)
                        )
                    except Exception as exc:
                        log(
                            f"[paired-invariant] pair={pair_key} "
                            "action=defer_refresh_failed "
                            f"error={exc.__class__.__name__}:{exc}"
                        )
                        continue

                    if (first_count > 0) == (second_count > 0):
                        self._paired_single_leg_since.pop(pair_key, None)
                        continue

                    live_token = token_id if first_count > 0 else paired
                    log(
                        f"[paired-invariant] pair={pair_key} "
                        f"counts={first_count}/{second_count} "
                        f"age={now - single_since:.1f}s action=cancel_both"
                    )
                    await self._cancel_coordinated_pair_quotes(
                        live_token,
                        "single_leg_invariant_timeout",
                    )
                    self._paired_single_leg_since.pop(pair_key, None)

                for pair_key in list(self._paired_single_leg_since):
                    if pair_key not in seen:
                        self._paired_single_leg_since.pop(pair_key, None)
            except Exception as exc:
                log(
                    "[paired-invariant] loop error: "
                    f"{exc.__class__.__name__}: {exc}"
                )
            await asyncio.sleep(self._paired_reconcile_interval_sec)

    @staticmethod
    def _aggressive_pair_key(token_id: str, paired_token_id: str) -> str:
        return "|".join(sorted((str(token_id), str(paired_token_id))))

    async def _claim_aggressive_pair_submit(
        self,
        token_id: str,
    ) -> Optional[tuple[str, int, str]]:
        paired = self._coordinated_pair_token(token_id)
        if not paired:
            return None

        pair_key = self._aggressive_pair_key(token_id, paired)
        async with self._aggressive_pair_submit_lock:
            attempt = self._aggressive_pair_submit_attempts.get(pair_key)
            if attempt is None:
                self._aggressive_pair_submit_seq += 1
                attempt = {
                    "id": self._aggressive_pair_submit_seq,
                    "leader": token_id,
                    "follower": paired,
                    "leader_posted": asyncio.Event(),
                    "posted": set(),
                    "failed": False,
                    "created_at": time.time(),
                    "first_post_at": 0.0,
                    "follower_claimed": False,
                }
                self._aggressive_pair_submit_attempts[pair_key] = attempt
                return pair_key, int(attempt["id"]), "leader"

            if (
                token_id == attempt.get("follower")
                and not attempt.get("follower_claimed")
            ):
                attempt["follower_claimed"] = True
                return pair_key, int(attempt["id"]), "follower"

        return None

    async def _aggressive_pair_attempt(
        self,
        claim: tuple[str, int, str],
    ) -> Optional[Dict[str, Any]]:
        pair_key, attempt_id, _role = claim
        async with self._aggressive_pair_submit_lock:
            attempt = self._aggressive_pair_submit_attempts.get(pair_key)
            if attempt is None or int(attempt.get("id", -1)) != attempt_id:
                return None
            return attempt

    async def _clear_aggressive_pair_submit(
        self,
        pair_key: str,
        attempt_id: int,
    ) -> None:
        watchdog: Optional[asyncio.Task] = None
        async with self._aggressive_pair_submit_lock:
            attempt = self._aggressive_pair_submit_attempts.get(pair_key)
            if attempt is None or int(attempt.get("id", -1)) != attempt_id:
                return
            self._aggressive_pair_submit_attempts.pop(pair_key, None)
            watchdog = self._aggressive_pair_submit_watchdogs.pop(pair_key, None)
        if watchdog is not None and watchdog is not asyncio.current_task():
            watchdog.cancel()

    def _cooldown_aggressive_pair(self, token_id: str, paired_token_id: str) -> None:
        retry_at = time.time() + self._aggressive_pair_retry_cooldown_sec
        for target in (token_id, paired_token_id):
            self._market_budget_skip_until[target] = max(
                float(self._market_budget_skip_until.get(target, 0.0)),
                retry_at,
            )
            self._last_plan_sig[target] = ""
            self._last_top_plan_sig[target] = ""
            self._last_back_plan_sig[target] = ""

    async def _aggressive_pair_submit_failed(
        self,
        token_id: str,
        claim: Optional[tuple[str, int, str]],
        reason: str,
    ) -> None:
        if claim is None:
            return
        pair_key, attempt_id, _role = claim
        async with self._aggressive_pair_submit_lock:
            attempt = self._aggressive_pair_submit_attempts.get(pair_key)
            if attempt is None or int(attempt.get("id", -1)) != attempt_id:
                return
            paired = str(
                attempt.get("follower")
                if token_id == attempt.get("leader")
                else attempt.get("leader")
            )
            attempt["failed"] = True
            attempt["leader_posted"].set()
        self._cooldown_aggressive_pair(token_id, paired)
        try:
            await self._cancel_coordinated_pair_quotes(
                token_id,
                f"paired_submit_failed:{reason}",
            )
        finally:
            await self._clear_aggressive_pair_submit(pair_key, attempt_id)

    async def _aggressive_pair_submit_succeeded(
        self,
        token_id: str,
        claim: Optional[tuple[str, int, str]],
    ) -> None:
        if claim is None:
            return
        pair_key, attempt_id, _role = claim
        start_watchdog = False
        complete = False
        async with self._aggressive_pair_submit_lock:
            attempt = self._aggressive_pair_submit_attempts.get(pair_key)
            if attempt is None or int(attempt.get("id", -1)) != attempt_id:
                return
            attempt["posted"].add(token_id)
            if token_id == attempt.get("leader"):
                attempt["first_post_at"] = time.time()
                attempt["leader_posted"].set()
            expected = {str(attempt.get("leader")), str(attempt.get("follower"))}
            complete = expected.issubset(attempt["posted"])
            if not complete and pair_key not in self._aggressive_pair_submit_watchdogs:
                start_watchdog = True

        if complete:
            log(
                f"[paired-submit] pair={pair_key} "
                f"attempt={attempt_id} action=both_posted"
            )
            await self._clear_aggressive_pair_submit(pair_key, attempt_id)
            return

        if start_watchdog:
            watchdog = asyncio.create_task(
                self._aggressive_pair_submit_watchdog(
                    pair_key,
                    attempt_id,
                    token_id,
                ),
                name=f"paired_submit:{attempt_id}",
            )
            async with self._aggressive_pair_submit_lock:
                attempt = self._aggressive_pair_submit_attempts.get(pair_key)
                if attempt is not None and int(attempt.get("id", -1)) == attempt_id:
                    self._aggressive_pair_submit_watchdogs[pair_key] = watchdog
                else:
                    watchdog.cancel()

    async def _aggressive_pair_submit_watchdog(
        self,
        pair_key: str,
        attempt_id: int,
        trigger_token_id: str,
    ) -> None:
        await asyncio.sleep(self._aggressive_pair_submit_timeout_sec)
        async with self._aggressive_pair_submit_lock:
            attempt = self._aggressive_pair_submit_attempts.get(pair_key)
            if attempt is None or int(attempt.get("id", -1)) != attempt_id:
                return
            leader = str(attempt.get("leader"))
            follower = str(attempt.get("follower"))

        live_counts: Dict[str, int] = {}
        refresh_error = ""
        try:
            self._invalidate_all_orders_cache()
            for target in (leader, follower):
                orders = await self._refresh_live_orders(target)
                live_counts[target] = self._live_buy_count(orders)
        except Exception as exc:
            refresh_error = f"{exc.__class__.__name__}:{exc}"

        if not refresh_error and all(live_counts.get(target, 0) > 0 for target in (leader, follower)):
            log(
                f"[paired-submit] pair={pair_key} "
                f"attempt={attempt_id} action=confirmed_live"
            )
            await self._clear_aggressive_pair_submit(pair_key, attempt_id)
            return

        reason = (
            f"paired_submit_timeout:{self._aggressive_pair_submit_timeout_sec:.1f}s:"
            f"leader_live={live_counts.get(leader, -1)}:"
            f"follower_live={live_counts.get(follower, -1)}"
        )
        if refresh_error:
            reason = f"{reason}:refresh_error={refresh_error}"
        self._cooldown_aggressive_pair(leader, follower)
        try:
            cleared = await self._cancel_coordinated_pair_quotes(
                trigger_token_id,
                reason,
            )
            result_text = (
                "已确认撤回两腿"
                if cleared
                else "撤单尚未确认，事件保持阻断"
            )
            profile_label = (
                "激进 LP"
                if str(getattr(self, "_runtime_scope", "") or "").lower()
                == "aggressive"
                else "稳定 LP"
            )
            self.notify_discord(
                f"{profile_label} 双腿建单失败",
                f"{reason}\n{result_text}，冷却 "
                f"{self._aggressive_pair_retry_cooldown_sec:.0f} 秒",
                "warning" if cleared else "error",
            )
        finally:
            await self._clear_aggressive_pair_submit(pair_key, attempt_id)

    async def _cancel_infeasible_quote_target(
        self,
        token_id: str,
        paired_token_id: str,
        gate: Dict[str, Any],
        warning: str,
    ) -> None:
        """Cancel live quotes when risk caps make the reward minimum impossible."""
        reason = f"minimum_quote_infeasible:{warning}"
        self._gate_decisions[token_id] = {
            **gate,
            "can_quote": False,
            "top_leg_action": "cancel",
            "reason": list(gate.get("reason", [])) + [reason],
        }
        retry_at = time.time() + self.budget_skip_cooldown_sec
        self._market_budget_skip_until[token_id] = retry_at
        if paired_token_id:
            self._market_budget_skip_until[paired_token_id] = retry_at
            await self._cancel_coordinated_pair_quotes(token_id, reason)
            return

        live_token = await self._get_live_orders_fast(token_id)
        if not live_token:
            return
        self._set_event_state(token_id, EVENT_DEFENSIVE, reason)
        await self._cancel_order_ids(
            token_id,
            [self._order_id(order) for order in live_token],
            reason,
        )
        self._market_live_orders[token_id] = await self._get_live_orders_fast(
            token_id
        )

    async def _execute_cross_side_cancel(
        self,
        trigger_token: str,
        paired_token: str,
        reason: str,
        *,
        max_ask: float,
        current_ask: float,
        consumed_pct: float,
    ) -> bool:
        """Block requotes, cancel the at-risk side, and confirm the result."""
        inflight = getattr(self, "_cross_side_cancel_inflight", None)
        if inflight is None:
            inflight = set()
            self._cross_side_cancel_inflight = inflight
        inflight.add(paired_token)
        try:
            tracker = self._vol_tracker(paired_token)
            tracker["watch_enter_ts"] = time.time()
            self._set_event_state(
                paired_token,
                EVENT_WATCH,
                f"cross_side_sentinel:{trigger_token}:{reason}",
            )
            canceled = await self._cancel_risk_buys(
                paired_token,
                f"cross_side_sentinel:{trigger_token}:{reason}",
            )
            if not canceled:
                log(
                    f"[cross-side-sentinel] LIVE CANCEL UNCONFIRMED "
                    f"trigger_token={trigger_token[:14]}.. "
                    f"paired={paired_token[:14]}.. reason={reason}"
                )
                try:
                    self._notify_risk(
                        "对侧挂单撤销未确认",
                        trigger_token=trigger_token,
                        paired_token=paired_token,
                        reason=reason,
                    )
                except Exception:
                    pass
                # _cancel_risk_buys already escalates to the global cancel
                # path and rechecks the official order endpoint. Do not
                # launch a duplicate account-wide cancellation here.
                return False

            self.cross_side_sentinel.mark_cancelled(paired_token)
            log(
                f"[cross-side-sentinel] LIVE CANCEL CONFIRMED "
                f"trigger_token={trigger_token[:14]}.. "
                f"paired={paired_token[:14]}.. reason={reason}"
            )
            try:
                self.notify_discord(
                    "对侧风险保护已触发",
                    (
                        f"触发市场：{self._discord_market_name(trigger_token)}\n"
                        f"已撤市场：{self._discord_market_name(paired_token)}\n"
                        f"触发原因：{self._discord_reason(reason)}\n"
                        f"盘口深度：{current_ask:,.0f} / 峰值 {max_ask:,.0f}\n"
                        f"深度下降：{consumed_pct:.0%}\n"
                        "系统处理：已撤销同一事件另一侧买单"
                    ),
                    "warning",
                )
            except Exception:
                pass
            return True
        finally:
            inflight.discard(paired_token)

    async def _place_sell_order(self, token_id: str, price: Decimal, size: Decimal) -> Any:
        """Place a SELL limit order."""
        price = self._sibling_gate(token_id, "SELL", price, "exit_sell")
        if self.remote_signer:
            try:
                signed = await asyncio.to_thread(
                    self.remote_signer.sign_order, token_id, float(price), float(size), "SELL"
                )
                self._mark_signer_recovered()
            except Exception as e:
                await self._handle_signer_failure(token_id, e, "sell")
                raise
            if isinstance(signed, dict):
                from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2
                signed = SignedOrderV2(**signed)
        else:
            signed = await asyncio.to_thread(self.client.create_order, OrderArgs(token_id=token_id, price=float(price), size=float(size), side=SELL))
        try:
            resp = await asyncio.to_thread(self.client.post_order, signed, OrderType.GTC)
        except Exception as exc:
            self._record_exchange_maintenance_error(exc, "post_exit_sell")
            raise
        self._invalidate_all_orders_cache()
        self._sibling_register_resp(token_id, "SELL", price, size, resp)
        return resp

    def _resume_halted_markets(self, trigger: str) -> None:
        """Resume markets that were halted by fill kill-switch (HALTED_ON_FILL / COOLDOWN).
        Does NOT touch WATCH, QUARANTINE, PENDING_MANUAL_EXIT, banned — those are intentional.
        Sets an exit-recovery protection window so the quote loop won't immediately push
        recovered tokens back into global_cooldown.
        """
        _resumable = {EVENT_HALTED_ON_FILL, EVENT_COOLDOWN}
        # Clear global cooldown state — this fill incident is resolved
        now = time.time()
        if self._cooldown_until > now:
            log(f"[exit] clearing global cooldown (was {int(self._cooldown_until - now)}s remaining)")
            self._cooldown_until = 0.0
        self._require_recovery_gate = False

        all_tokens = list(set(list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())))
        protection_deadline = now + self._exit_recovery_protection_sec
        resumed = 0
        for tid in all_tokens:
            st = self._event_state_name(tid)
            if st in _resumable:
                if self._event_has_unresolved_exit(tid):
                    continue
                self._clear_halt_preemption(tid)
                self._set_event_state(tid, EVENT_ACTIVE, trigger)
                self._exit_recovery_protection_until[tid] = protection_deadline
                resumed += 1
        log(f"[exit] resumed {resumed} markets | trigger={trigger} | protect={int(self._exit_recovery_protection_sec)}s")

    async def _await_balance_stable(self, token_id: str) -> bool:
        """Poll balance N times; return True when non-decreasing (stable/recovering)."""
        n = self._balance_stability_checks
        interval = self._balance_stability_interval_sec
        deadline = time.time() + n * interval * 3  # generous timeout

        prev_bal = await self._get_collateral_available(force_refresh=True)
        if prev_bal is None:
            return True  # can't gate on unknown balance

        stable_count = 0
        while stable_count < n and time.time() < deadline and self._running:
            await asyncio.sleep(interval)
            cur_bal = await self._get_collateral_available(force_refresh=True)
            if cur_bal is None:
                continue
            if cur_bal >= prev_bal:
                stable_count += 1
            else:
                stable_count = 0
                log(f"[exit-gate] {token_id[:16]} bal dropped {prev_bal}->{cur_bal}, reset")
            prev_bal = cur_bal

        ok = stable_count >= n
        log(f"[exit-gate] {token_id[:16]} stable={ok} checks={stable_count}/{n} bal={prev_bal}")
        return ok

    def _record_exit(self, token_id: str) -> None:
        """Write an exit_record entry from the matching pending_unwind. Safe to
        call once per completed exit; dedups recent records by token_id.
        `loss` is signed: positive = loss, negative = benefit (profitable exit).
        """
        # The monitor and unwind tracker may observe completion concurrently.
        # Inspect the whole recent tail because another token can be recorded
        # between those two observations.
        now = time.time()
        for rec in reversed(self._exit_records):
            age = now - float(rec.get("ts", 0) or 0)
            if age >= 60:
                break
            if rec.get("token_id") == token_id:
                return
        for uw in self._pending_unwinds:
            if uw.get("token_id") == token_id:
                fp = float(uw.get("fill_price", 0) or 0)
                sp = float(uw.get("sell_price", 0) or 0)
                sz = float(
                    uw.get("exit_size", uw.get("fill_size", 0)) or 0
                )
                reported_sz = float(uw.get("reported_fill_size", sz) or 0)
                loss = (fp - sp) * sz if fp > 0 and sp > 0 else 0.0
                self._exit_records.append({
                    "token_id": token_id,
                    "fill_price": fp,
                    "sell_price": sp,
                    "size": sz,
                    "reported_size": reported_sz,
                    "size_source": "position",
                    "reason": str(uw.get("reason") or ""),
                    "loss": round(loss, 6),
                    "ts": now,
                })
                if len(self._exit_records) > 200:
                    self._exit_records = self._exit_records[-100:]
                break

    async def _finalize_exit_resume(self, token_id: str) -> None:
        """Balance-stability gate then resume only the affected event's markets."""
        self._active_exit_orders.pop(token_id, None)
        stable = await self._await_balance_stable(token_id)
        # Re-verify position hasn't reappeared during stability wait
        recheck = await self._get_token_position(token_id)
        if recheck is None or recheck < 0:
            log(f"[exit] {token_id[:16]} position recheck unknown; keeping halt")
            return
        if recheck > self._exit_dust_threshold:
            log(f"[exit] {token_id[:16]} pos reappeared={recheck} during stability wait")
            return  # don't resume — position came back (new fill?)
        self._record_exit(token_id)
        resolved_tokens = self._consume_resolved_unwinds(token_id)
        self._activate_resolved_exit_tokens(
            resolved_tokens,
            "exit_complete_resume",
        )
        self._resume_halted_markets("exit_complete_resume")
        self.send_fill_discord(
            f"仓位退出完成\n市场：{self._discord_market_name(token_id)}\n"
            f"仓位：0\n余额状态：{'已稳定' if stable else '尚未稳定'}"
        )

    async def _monitor_exit_order(self, token_id: str, order_id: str, sell_price: Decimal,
                                  sell_size: Decimal, fill_price: Decimal, stop_loss_floor: Decimal,
                                  reason: str) -> None:
        """Monitor exit SELL with dynamic repricing.
        - Tracks market and adjusts SELL price to stay competitive
        - Accepts loss up to max_loss_usd (stop-loss)
        - Never resumes other markets until exit completes
        - Safety timeout goes to MANUAL_EXIT but does NOT resume others
        """
        tick = Decimal("0.01")
        # Detect tick size from market config
        mcfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(token_id) or {}
        if mcfg.get("tick_size"):
            tick = Decimal(str(mcfg["tick_size"]))

        check_interval = 15
        reprice_interval = self._exit_reprice_interval
        exit_start_time = time.time()
        last_reprice = exit_start_time
        # Safety deadline must be longer than stop-loss wait (3h default) + buffer
        safety_deadline = exit_start_time + max(self._exit_timeout_sec, self._exit_stop_loss_wait_sec + 3600)
        below_floor_since: float = 0  # track how long market is below stop-loss floor

        while self._running and time.time() < safety_deadline:
            await asyncio.sleep(check_interval)
            try:
                # Check if position is gone (filled)
                position = await self._get_token_position(token_id)
                if position is None or position < 0:
                    log(
                        f"[exit] {token_id[:16]} position unavailable; "
                        "keeping halt"
                    )
                    continue
                if position == 0.0:
                    log(f"[exit] {token_id[:16]} pos=0 — running balance stability gate")
                    await self._finalize_exit_resume(token_id)
                    return

                # Check if our order is still live
                orders = await self._read_open_orders("exit_monitor")
                live_ids = {
                    str(o.get("id") or o.get("orderID") or "")
                    for o in orders
                    if _order_is_live(o)
                }

                order_gone = order_id and order_id not in live_ids

                if order_gone:
                    new_position = await self._get_token_position(token_id)
                    if new_position is None or new_position < 0:
                        log(
                            f"[exit] {token_id[:16]} order gone but position "
                            "is unavailable; keeping halt"
                        )
                        continue
                    if new_position == 0.0:
                        log(f"[exit] {token_id[:16]} order gone + pos=0 — balance stability gate")
                        await self._finalize_exit_resume(token_id)
                        return
                    elif new_position <= self._exit_dust_threshold:
                        log(f"[exit] {token_id[:16]} dust_remains={new_position} — manual exit")
                        self._active_exit_orders.pop(token_id, None)
                        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, "dust_after_partial")
                        # The exit record is written only after position reaches
                        # zero. Until then the unwind tracker remains authoritative.
                        self._resume_halted_markets("exit_dust_resume")
                        self.send_discord(
                            f"退出后仍有微量仓位\n市场：{self._discord_market_name(token_id)}\n"
                            f"剩余：{float(new_position):,.4f} 份\n需手动检查"
                        )
                        return
                    # Order disappeared but still have position — re-place with current pricing
                    sell_size = Decimal(str(new_position))

                # Dynamic reprice: periodically re-evaluate sell price
                now = time.time()
                should_reprice = order_gone or (now - last_reprice >= reprice_interval)

                if should_reprice:
                    last_reprice = now
                    # Get current market prices
                    market_bid = Decimal("0")
                    market_ask = Decimal("0")
                    snap = self._market_snapshots.get(token_id)
                    if snap:
                        if snap.best_bid > 0:
                            market_bid = snap.best_bid
                        if snap.best_ask > 0:
                            market_ask = snap.best_ask
                    if market_bid <= 0 or market_ask <= 0:
                        tob = self.market_states.get(token_id)
                        if tob:
                            if market_bid <= 0 and tob.best_bid > 0:
                                market_bid = tob.best_bid
                            if market_ask <= 0 and tob.best_ask > 0:
                                market_ask = tob.best_ask

                    elapsed = now - exit_start_time
                    new_price = self._calc_exit_price(token_id, fill_price, stop_loss_floor,
                                                      market_bid, market_ask, tick,
                                                      elapsed_sec=elapsed)

                    # Track if market is below stop-loss floor
                    if market_bid > 0 and market_bid < stop_loss_floor:
                        if below_floor_since == 0:
                            below_floor_since = now
                            self.send_discord(
                                f"退出价格低于止损底线\n市场：{self._discord_market_name(token_id)}\n"
                                f"市场价：${float(market_bid):.4f}\n"
                                f"止损底线：${float(stop_loss_floor):.4f}\n"
                                f"成交价：${float(fill_price):.4f}\n数量：{float(sell_size):,.2f} 份\n"
                                "系统处理：等待价格回升"
                            )
                    else:
                        below_floor_since = 0

                    if order_gone or new_price != sell_price:
                        # Cancel old order if still live; must succeed before we
                        # can place the new one because the tokens are still
                        # locked by the old order (Polymarket returns 400
                        # "not enough balance / allowance" otherwise).
                        cancel_ok = True
                        if not order_gone and order_id:
                            cancel_ok = False
                            try:
                                cancel_ok = await self._execute_exchange_cancel(
                                    "cancel_exit_reprice",
                                    self.client.cancel,
                                    order_id,
                                )
                                if cancel_ok:
                                    log(f"[exit] {token_id[:16]} canceled old SELL for reprice {sell_price}->{new_price}")
                            except Exception as ce:
                                log(f"[exit] {token_id[:16]} cancel-before-reprice err: {ce} — skipping reprice this cycle")
                                if self._is_req_exc(ce):
                                    await self._mark_req_exc_and_maybe_storm(
                                        f"exit_cancel:{token_id}", "global_request_exception_storm"
                                    )
                        if not cancel_ok:
                            # Keep old order alive, retry next cycle. Do NOT place
                            # a new SELL — the old one still has the tokens.
                            continue

                        sell_price = new_price
                        try:
                            resp = await self._place_sell_order(token_id, sell_price, sell_size)
                            new_oid = str((resp or {}).get("orderID") or (resp or {}).get("id") or "")
                            order_id = new_oid
                            self._active_exit_orders[token_id] = new_oid
                            # Keep pending_unwinds in sync so _record_exit uses the
                            # actual final sell_price, not the initial one.
                            for uw in self._pending_unwinds:
                                if uw.get("token_id") == token_id:
                                    uw["sell_price"] = float(sell_price)
                                    uw["order_id"] = new_oid
                                    break
                            action = "re-placed" if not order_gone else "repriced"
                            log(f"[exit] {token_id[:16]} SELL {action} oid={new_oid} p={sell_price}")
                        except Exception as e2:
                            log(f"[exit] {token_id[:16]} SELL reprice err: {e2}")

            except Exception as e:
                log(f"[exit] {token_id[:16]} monitor err: {e}")

        # Safety timeout — the stuck token goes to PENDING_MANUAL_EXIT for operator review,
        # but resume unrelated markets that were mass-halted by balance_drop_global_halt:
        # the halt was a safety catch that already found its target (this token).
        self._active_exit_orders.pop(token_id, None)
        log(f"[exit] {token_id[:16]} safety timeout | p={sell_price} sz={sell_size}")
        self._set_event_state(token_id, EVENT_PENDING_MANUAL_EXIT, f"exit_safety_timeout:{reason}")
        self._resume_halted_markets("exit_safety_timeout_resume")
        self.send_discord(
            f"退出流程超时\n市场：{self._discord_market_name(token_id)}\n"
            f"价格：${float(sell_price):.4f}\n数量：{float(sell_size):,.2f} 份\n"
            "需手动处理；其他市场已恢复"
        )

    # ---------------------------------------------------------------
    # # session mode (redesigned)
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
        if current == "night":
            return self._night_market_cfg
        return self.market_cfg

    def _should_carry_day_market_to_night(
        self,
        game_start_ts: Optional[float],
        night_cutoff_ts: Optional[float],
    ) -> bool:
        """Only carry a day market when explicitly enabled and fully verified."""
        if not self._session_carry_day_markets_to_night:
            return False
        if game_start_ts is None or night_cutoff_ts is None:
            return False
        try:
            return float(game_start_ts) >= float(night_cutoff_ts)
        except (TypeError, ValueError):
            return False

    def _session_allows(self, token_id: str) -> bool:
        """Check if token_id belongs to the current session's active markets."""
        if not self._session_enabled:
            return True
        return token_id in self._active_market_cfg()

    def _is_session_confirmed(self) -> bool:
        """Check if session confirmation is valid (within 24h TTL).

        Behavior:
        - Confirmation is only accepted if it was given during the confirm window
          (default 22:00-00:00 Beijing time).
        - Valid for 24 hours from confirmed_at, regardless of date boundaries.
        - Without confirmation, the bot keeps running but does NOT auto-switch.
        """
        try:
            if not self._session_confirm_path.exists():
                return False
            data = json.loads(self._session_confirm_path.read_text())
            confirmed_at = float(data.get("confirmed_at", 0) or 0)
            if confirmed_at <= 0:
                return False

            now = time.time()

            # 24h TTL from confirmed_at
            if now - confirmed_at >= self._session_confirm_ttl_sec:
                return False

            # Check that confirmation was given during the confirm window (e.g. 22:00-00:00)
            confirmed_local = datetime.fromtimestamp(confirmed_at, ZoneInfo(self._session_tz))
            try:
                cw_start_h, cw_start_m = map(int, self._session_confirm_window_start.split(":"))
                cw_end_h, cw_end_m = map(int, self._session_confirm_window_end.split(":"))
                confirm_minutes = confirmed_local.hour * 60 + confirmed_local.minute
                window_start = cw_start_h * 60 + cw_start_m
                window_end = cw_end_h * 60 + cw_end_m

                # For cross-midnight windows like 22:00-00:00, treat end=0:00 as 1:00
                # to include the full midnight hour (00:00-00:59)
                if window_end == 0:
                    window_end = 60  # 00:00 → include up to 01:00
                if window_start <= window_end:
                    in_window = window_start <= confirm_minutes < window_end
                else:
                    # Cross-midnight window (e.g. 22:00-02:00)
                    in_window = confirm_minutes >= window_start or confirm_minutes < window_end

                if not in_window:
                    log(f"[session] confirmation at {confirmed_local.strftime('%H:%M')} outside window {self._session_confirm_window_start}-{self._session_confirm_window_end}, ignoring")
                    return False
            except Exception as e:
                log(f"[session] error parsing confirm window: {e}")

            return True
        except Exception as e:
            log(f"[session] error reading session confirmation: {e}")
            return False

    async def _session_switch_halt(self) -> None:
        """Session switch without confirmation — cancel all orders but keep engine alive.

        The engine stays running so that if the user later confirms, it can
        resume on the next switch check.  Only orders are canceled.
        """
        log("[session] *** SESSION SWITCH BLOCKED — no valid confirmation, canceling orders ***")
        self.notify_discord(
            "日夜盘切换已暂停",
            "原因：未收到切换确认\n系统处理：已撤销全部挂单，引擎继续运行并等待确认",
            "warning",
        )
        try:
            canceled = await self._execute_exchange_cancel(
                "cancel_all_session_switch",
                self.client.cancel_all,
            )
            if canceled:
                self._sibling_registry.clear_funder(self._funder_lc)
                log("[session] cancel_all done (switch blocked, no confirmation)")
            else:
                log("[session] cancel_all deferred by exchange maintenance")
        except Exception as e:
            log(f"[session] cancel_all error during switch halt: {e}")
        self._session_halted_no_confirm = True

    async def _session_switch_cleanup(self) -> None:
        """Cancel ALL orders when session switches, wait gap, then let new session start.

        If confirm_required and no valid confirmation at switch time:
        - Cancel all orders (stop quoting)
        - Engine stays alive, keeps checking each cycle
        - Once confirmation appears, the next cycle will complete the switch
        """
        current = self._current_session()

        # If previously halted due to no confirmation, check if confirm arrived
        if self._session_halted_no_confirm:
            if self._session_confirm_required and not self._is_session_confirmed():
                return  # still no confirmation, stay halted (no orders)
            # Confirmation arrived! Resume by allowing the switch to proceed
            log(f"[session] confirmation received — resuming switch to {current}")
            session_name = "夜盘" if current == "night" else "日盘"
            self.send_fill_discord(f"日夜盘确认已收到\n系统处理：正在切换到{session_name}")
            self._session_halted_no_confirm = False
            # Fall through to do the actual switch setup below
            prev = self._last_session
            self._last_session = current
            # Skip the "same session" check since we need to initialize
        else:
            if current == self._last_session:
                return

            prev = self._last_session
            self._last_session = current
            if prev == "unknown":
                log(f"[session] initial session: {current}")
                return

            # Check session confirmation before allowing switch
            if self._session_confirm_required and not self._is_session_confirmed():
                log(f"[session] session switch {prev} → {current} BLOCKED — no valid confirmation")
                await self._session_switch_halt()
                return

        log(f"[session] === SESSION SWITCH: {prev} — {current} ===")
        self._notify_status("日夜盘切换", previous=prev, current=current)

        # Day → Night: selective migration (Kevin 2026-04-26).
        # Don't blow away every order — only cancel markets whose game starts
        # before the next 8am BJT cutoff (those are the risky overnight ones).
        # Markets starting after 8am tomorrow get carried into night_market_cfg
        # with their orders intact, so reward accrual is continuous.
        # Night → Day still uses the original full-cancel path.
        if (
            prev == "day"
            and current == "night"
            and self._session_carry_day_markets_to_night
        ):
            try:
                try:
                    from .auto_curator import _next_bjt_8am_ts
                except ImportError:
                    from auto_curator import _next_bjt_8am_ts
                night_cutoff_ts = _next_bjt_8am_ts()
            except Exception as _exc:
                log(f"[session] could not import _next_bjt_8am_ts: {_exc} — falling back to full cancel")
                night_cutoff_ts = None

            if night_cutoff_ts is not None:
                cancel_tokens: list[str] = []
                carry_tokens: list[tuple[str, Dict[str, Any]]] = []
                for token_id, mcfg in list(self.market_cfg.items()):
                    # Resolve gs_ts with multiple fallbacks. Bug fix 2026-04-27:
                    # cache-only read returned None for many tokens at switch
                    # time, putting markets that should have been carried into
                    # CANCEL. Now we (a) try existing snapshot's game_start_ts,
                    # (b) fall back to live meta fetch, (c) if BOTH still
                    # unavailable, DEFAULT TO CARRY (safer than CANCEL).
                    gs_ts = None
                    snap = self._market_snapshots.get(token_id)
                    if snap is not None:
                        snap_gs = getattr(snap, "game_start_ts", None) or getattr(snap, "gameStartTs", None)
                        if snap_gs:
                            try:
                                gs_ts = float(snap_gs)
                            except Exception:
                                pass
                    if gs_ts is None:
                        cached = self._market_meta_cache.get(token_id)
                        meta_dict = cached[0] if cached else None
                        if meta_dict:
                            gs_ts = self._to_end_ts(meta_dict)
                    if gs_ts is None:
                        try:
                            meta_dict = await self._get_market_meta(token_id)
                            if meta_dict:
                                gs_ts = self._to_end_ts(meta_dict)
                        except Exception as _exc:
                            log(f"[session] meta fetch failed for {token_id[:14]}..: {_exc}")
                    if self._should_carry_day_market_to_night(gs_ts, night_cutoff_ts):
                        carry_tokens.append((token_id, mcfg))
                    else:
                        if gs_ts is None:
                            log(f"[session] gs_ts unknown for {token_id[:14]}.. — fail-closed CANCEL")
                        cancel_tokens.append(token_id)

                log(f"[session] day→night migration: cancel={len(cancel_tokens)} carry={len(carry_tokens)} cutoff={night_cutoff_ts:.0f}")

                # Cancel only the too-early markets
                for token_id in cancel_tokens:
                    try:
                        live = await self._get_live_orders_fast(token_id)
                        ids = [self._order_id(o) for o in live]
                        if ids:
                            await self._cancel_order_ids(token_id, ids, "session_switch_pre_8am")
                    except Exception:
                        pass
                    state = self._event_state_name(token_id)
                    if state not in {EVENT_HALTED_ON_FILL, EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT}:
                        self._set_event_state(token_id, EVENT_COOLDOWN, "session_switch_pre_8am")

                # Carry surviving markets into night_market_cfg, keep state ACTIVE
                for token_id, mcfg in carry_tokens:
                    self._night_market_cfg[token_id] = mcfg

                gap = self._session_switch_gap_sec
                log(f"[session] day→night migration done; waiting {gap}s gap")
                await asyncio.sleep(gap)

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
                    if (
                        self._event_state_name(token_id) == EVENT_ACTIVE
                        and not self._event_has_unresolved_exit(token_id)
                    ):
                        self._clear_halt_preemption(token_id)
                log(f"[session] {current} session started with {len(new_markets)} markets (incl. {len(carry_tokens)} carried over)")
                return

        # Default path (Night → Day, or fallback if 8am cutoff unavailable):
        # Cancel ALL orders globally first (fast, reliable), then set states
        try:
            await self._cancel_all_except_exit()
            log(f"[session] cancel_all done (protecting {len(self._active_exit_orders)} exit orders)")
        except Exception as e:
            log(f"[session] cancel_all error: {e}, falling back to per-token cancel")
            # Fallback: cancel per-token
            prev_markets = self._night_market_cfg if prev == "night" else self.market_cfg
            for token_id in list(prev_markets.keys()):
                try:
                    live = await self._get_live_orders_fast(token_id)
                    ids = [self._order_id(o) for o in live]
                    if ids:
                        await self._cancel_order_ids(token_id, ids, "session_switch")
                except Exception:
                    pass

        # Set all previous session's markets to COOLDOWN
        prev_markets = self._night_market_cfg if prev == "night" else self.market_cfg
        for token_id in list(prev_markets.keys()):
            state = self._event_state_name(token_id)
            if state not in {EVENT_HALTED_ON_FILL, EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT}:
                self._set_event_state(token_id, EVENT_COOLDOWN, "session_switch")

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
            state = self._event_state_name(token_id)
            if state == EVENT_ACTIVE:
                if not self._event_has_unresolved_exit(token_id):
                    self._clear_halt_preemption(token_id)
            elif state == EVENT_COOLDOWN and not self._event_has_unresolved_exit(token_id):
                self._clear_halt_preemption(token_id)
                self._set_event_state(
                    token_id,
                    EVENT_ACTIVE,
                    f"session_switch_to_{current}",
                )
            else:
                log(
                    f"[session] retaining token={token_id[:16]} state={state}; "
                    "not safe to reactivate"
                )

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

    def _track_managed_buy_order(self, order_id: str) -> None:
        order_id = str(order_id or "")
        if not order_id or order_id in self._managed_buy_order_ids:
            return
        self._managed_buy_order_ids.add(order_id)
        self._managed_buy_order_ids_order.append(order_id)
        overflow = (
            len(self._managed_buy_order_ids_order)
            - self._managed_order_history_limit
        )
        if overflow > 0:
            stale = self._managed_buy_order_ids_order[:overflow]
            self._managed_buy_order_ids_order = (
                self._managed_buy_order_ids_order[overflow:]
            )
            self._managed_buy_order_ids.difference_update(stale)

    @staticmethod
    def _trade_order_ids(trade: dict) -> set[str]:
        ids = {
            str(
                trade.get("order_id")
                or trade.get("orderID")
                or trade.get("taker_order_id")
                or ""
            )
        }
        maker_orders = trade.get("maker_orders")
        if isinstance(maker_orders, list):
            ids.update(
                str(order.get("order_id") or order.get("id") or "")
                for order in maker_orders
                if isinstance(order, dict)
            )
        ids.discard("")
        return ids

    @staticmethod
    def _trade_decimal(item: object, *keys: str) -> Decimal:
        if not isinstance(item, dict):
            return Decimal("0")
        for key in keys:
            value = item.get(key)
            if value in (None, ""):
                continue
            try:
                return Decimal(str(value))
            except Exception:
                continue
        return Decimal("0")

    def _managed_inventory_increase_details(
        self,
        trade: dict,
    ) -> tuple[bool, str, Decimal, Decimal, str]:
        """Classify account trades before automatic exit handling.

        SELL trades reduce inventory and are always user-safe. BUY trades are
        actionable only when they reference an order created by this engine.
        Maker fills use the matching maker-order asset, amount and price, never
        the aggregate top-level fields, which describe the taker side.
        """
        raw_token = str(
            trade.get("asset")
            or trade.get("asset_id")
            or trade.get("token_id")
            or ""
        )
        maker_orders = trade.get("maker_orders")
        if isinstance(maker_orders, list):
            matched_size = Decimal("0")
            matched_notional = Decimal("0")
            matched_assets: set[str] = set()
            maker_asset_missing = False
            managed_order_seen = False
            managed_buy_seen = False
            seen_order_ids: set[str] = set()
            for maker_order in maker_orders:
                if not isinstance(maker_order, dict):
                    continue
                order_id = str(
                    maker_order.get("order_id")
                    or maker_order.get("id")
                    or ""
                )
                if order_id not in self._managed_buy_order_ids:
                    continue
                if order_id in seen_order_ids:
                    continue
                seen_order_ids.add(order_id)
                managed_order_seen = True
                maker_side = str(maker_order.get("side") or "").upper()
                if maker_side != "BUY":
                    continue
                managed_buy_seen = True
                order_size = self._trade_decimal(
                    maker_order,
                    "matched_amount",
                    "size_matched",
                    "matched",
                    "filled_size",
                )
                order_price = self._trade_decimal(maker_order, "price")
                if order_price <= 0:
                    order_price = self._trade_decimal(trade, "price")
                order_token = str(
                    maker_order.get("asset")
                    or maker_order.get("asset_id")
                    or maker_order.get("token_id")
                    or ""
                )
                if order_token:
                    matched_assets.add(order_token)
                else:
                    maker_asset_missing = True
                if order_size > 0:
                    matched_size += order_size
                    if order_price > 0:
                        matched_notional += order_size * order_price
            if managed_buy_seen:
                if maker_asset_missing:
                    return (
                        False,
                        "managed_maker_missing_asset",
                        Decimal("0"),
                        Decimal("0"),
                        "",
                    )
                if len(matched_assets) > 1:
                    return (
                        False,
                        "managed_maker_multiple_assets",
                        Decimal("0"),
                        Decimal("0"),
                        "",
                    )
                matched_price = (
                    matched_notional / matched_size
                    if matched_size > 0 and matched_notional > 0
                    else self._trade_decimal(trade, "price")
                )
                matched_token = next(iter(matched_assets), raw_token)
                return (
                    True,
                    "managed_maker_buy",
                    matched_size,
                    matched_price,
                    matched_token,
                )
            if managed_order_seen:
                return (
                    False,
                    "managed_maker_non_buy",
                    Decimal("0"),
                    Decimal("0"),
                    raw_token,
                )

        taker_order_id = str(
            trade.get("taker_order_id")
            or trade.get("order_id")
            or trade.get("orderID")
            or ""
        )
        side = str(trade.get("side") or "").upper()
        if taker_order_id in self._managed_buy_order_ids:
            if side == "BUY":
                return (
                    True,
                    "managed_taker_buy",
                    self._trade_decimal(trade, "size", "matched_amount"),
                    self._trade_decimal(trade, "price"),
                    raw_token,
                )
            return (
                False,
                "managed_taker_non_buy",
                Decimal("0"),
                Decimal("0"),
                raw_token,
            )
        if side == "SELL":
            return (
                False,
                "manual_or_external_sell",
                Decimal("0"),
                Decimal("0"),
                raw_token,
            )
        if side == "BUY":
            return False, "unmanaged_buy", Decimal("0"), Decimal("0"), raw_token
        return False, "unknown_side", Decimal("0"), Decimal("0"), raw_token

    def _trade_is_managed_inventory_increase(
        self, trade: dict
    ) -> tuple[bool, str]:
        managed, classification, _size, _price, _token = (
            self._managed_inventory_increase_details(trade)
        )
        return managed, classification

    def _manual_sell_orders_for_event(self, orders: list[dict], token_id: str) -> list[dict]:
        event_tokens = set(self._event_token_ids(token_id))
        active_exit_ids = {
            str(order_id)
            for order_id in self._active_exit_orders.values()
            if order_id
        }
        return [
            order
            for order in orders
            if _order_is_live(order)
            and str(order.get("asset_id") or order.get("token_id") or "")
            in event_tokens
            and str(order.get("side") or "").upper() == "SELL"
            and self._order_id(order) not in active_exit_ids
        ]

    async def _manual_exit_blocks_quote(self, token_id: str) -> bool:
        """Pause bot BUYs while a user-managed SELL is active or cooling down."""
        event_key = self._event_key(token_id)
        now = time.time()
        try:
            orders = await self._get_all_orders_cached()
            manual_sells = self._manual_sell_orders_for_event(orders, token_id)
        except Exception as exc:
            log(
                f"[manual-exit] token={token_id[:16]} order read failed: {exc}"
            )
            return now < self._manual_exit_event_until.get(event_key, 0.0)

        if manual_sells:
            self._manual_exit_event_until[event_key] = (
                now + self._manual_exit_cooldown_sec
            )
            managed_buy_ids = [
                self._order_id(order)
                for order in orders
                if _order_is_live(order)
                and str(order.get("asset_id") or order.get("token_id") or "")
                in set(self._event_token_ids(token_id))
                and str(order.get("side") or "").upper() == "BUY"
                and self._order_id(order) in self._managed_buy_order_ids
            ]
            if managed_buy_ids:
                await self._cancel_order_ids(
                    token_id,
                    managed_buy_ids,
                    "manual_exit_protection",
                )
            last_notice = self._manual_exit_last_notice.get(event_key, 0.0)
            if now - last_notice >= 60:
                self._manual_exit_last_notice[event_key] = now
                log(
                    f"[manual-exit] event={event_key[:16]} "
                    f"manual_sell_orders={len(manual_sells)} "
                    f"managed_buys_cancelled={len(managed_buy_ids)} "
                    f"cooldown={int(self._manual_exit_cooldown_sec)}s"
                )
            return True

        return now < self._manual_exit_event_until.get(event_key, 0.0)

    async def _register_manual_sell_trade(
        self,
        token_id: str,
        trade_id: str,
    ) -> None:
        """Keep a completed website SELL from being bought back immediately."""
        event_key = self._event_key(token_id)
        now = time.time()
        self._manual_exit_event_until[event_key] = (
            now + self._manual_exit_cooldown_sec
        )
        event_tokens = set(self._event_token_ids(token_id))
        manual_sell_ts = getattr(
            self, "_position_reconcile_manual_sell_ts", {}
        )
        managed_positions = getattr(
            self, "_position_reconcile_managed_tokens", set()
        )
        for event_token in event_tokens:
            manual_sell_ts[str(event_token)] = now
        managed_positions.difference_update(event_tokens)
        self._position_reconcile_manual_sell_ts = manual_sell_ts
        self._position_reconcile_managed_tokens = managed_positions
        try:
            self._invalidate_all_orders_cache()
            orders = await self._get_all_orders_cached()
            managed_by_token: Dict[str, list[str]] = {}
            for order in orders:
                order_token = self._order_token_id(order)
                order_id = self._order_id(order)
                if (
                    order_token in event_tokens
                    and self._order_side(order) == "BUY"
                    and order_id in self._managed_buy_order_ids
                ):
                    managed_by_token.setdefault(order_token, []).append(
                        order_id
                    )
            for order_token, order_ids in managed_by_token.items():
                await self._cancel_order_ids(
                    order_token,
                    order_ids,
                    "manual_sell_trade_protection",
                )
            log(
                f"[manual-exit] completed_sell trade={trade_id[:16]} "
                f"event={event_key[:24]} "
                f"managed_buys_cancelled="
                f"{sum(len(ids) for ids in managed_by_token.values())} "
                f"cooldown={int(self._manual_exit_cooldown_sec)}s"
            )
        except Exception as exc:
            log(
                f"[manual-exit] completed_sell trade={trade_id[:16]} "
                f"event={event_key[:24]} protection_error={_format_exc(exc)}"
            )

    async def _adopt_legacy_live_buy_orders(self) -> None:
        """Reconcile live BUYs created before the current process started."""
        try:
            orders = await self._read_open_orders("managed_order_adoption")
        except Exception as exc:
            log(f"[managed-orders] legacy adoption skipped: {exc}")
            return
        adopted = 0
        for order in orders if isinstance(orders, list) else []:
            if (
                not _order_is_live(order)
                or str(order.get("side") or "").upper() != "BUY"
            ):
                continue
            order_id = self._order_id(order)
            if not order_id:
                continue
            already_known = order_id in self._managed_buy_order_ids
            self._track_managed_buy_order(order_id)
            if not already_known:
                adopted += 1
        log(f"[managed-orders] adopted_legacy_live_buys={adopted}")

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
        return await self._refresh_live_orders(token_id)

    @classmethod
    def _valid_open_orders_response(cls, orders: Any) -> bool:
        if not isinstance(orders, list):
            return False
        for order in orders:
            if not isinstance(order, dict):
                return False
            if not cls._order_id(order):
                return False
            if not str(order.get("status") or "").strip():
                return False
            if str(order.get("side") or "").strip().upper() not in {"BUY", "SELL"}:
                return False
        return True

    async def _read_open_orders(self, action: str) -> Any:
        try:
            orders = await asyncio.to_thread(self.client.get_open_orders)
        except Exception as exc:
            self._record_exchange_maintenance_error(exc, action)
            raise
        if self._valid_open_orders_response(orders):
            self._note_exchange_authenticated_read_success(action)
        return orders

    @classmethod
    def _recovery_post_order_id(cls, response: Any) -> str:
        if not isinstance(response, dict):
            return ""
        if response.get("success") is not True:
            return ""
        if str(response.get("errorMsg") or "").strip():
            return ""
        if str(response.get("status") or "").strip().lower() != "live":
            return ""
        if response.get("tradeIDs") or response.get("transactionsHashes"):
            return ""
        return cls._order_id(response)

    @staticmethod
    def _recovery_cancel_confirmed(response: Any, order_id: str) -> bool:
        if not isinstance(response, dict):
            return False
        canceled = response.get("canceled")
        not_canceled = response.get("not_canceled")
        if not isinstance(canceled, list) or not isinstance(not_canceled, dict):
            return False
        canceled_ids = {str(value) for value in canceled}
        failed_ids = {str(value) for value in not_canceled}
        return order_id in canceled_ids and order_id not in failed_ids

    @classmethod
    def _recovery_order_canceled_unmatched(
        cls,
        response: Any,
        order_id: str,
    ) -> bool:
        if not isinstance(response, dict):
            return False
        if cls._order_id(response) != order_id:
            return False
        status = str(response.get("status") or "").strip().upper()
        if status != "ORDER_STATUS_CANCELED":
            return False
        if "size_matched" not in response:
            return False
        try:
            matched = Decimal(str(response.get("size_matched")))
        except Exception:
            return False
        if not matched.is_finite() or matched != 0:
            return False
        trades = response.get("associate_trades")
        return isinstance(trades, list) and not trades

    async def _verify_exchange_recovery_probe(
        self,
        token_id: str,
        order_id: str,
    ) -> bool:
        try:
            cancel_response = await asyncio.to_thread(
                self.client.cancel_orders,
                [order_id],
            )
        except Exception as exc:
            handled = self._record_exchange_maintenance_error(
                exc,
                "recovery_probe_cancel",
            )
            if not handled:
                self._force_exchange_maintenance(
                    "recovery_probe_cancel_failed",
                    "recovery_probe_cancel",
                    cancel_failure=True,
                )
            return False
        if not self._recovery_cancel_confirmed(cancel_response, order_id):
            self._force_exchange_maintenance(
                "recovery_probe_cancel_unconfirmed",
                "recovery_probe_cancel",
                cancel_failure=True,
            )
            return False
        self._invalidate_all_orders_cache()

        try:
            order_state = await asyncio.to_thread(self.client.get_order, order_id)
        except Exception as exc:
            handled = self._record_exchange_maintenance_error(
                exc,
                "recovery_probe_order_verify",
            )
            if not handled:
                self._force_exchange_maintenance(
                    "recovery_probe_order_unverified",
                    "recovery_probe_order_verify",
                )
            return False
        if not self._recovery_order_canceled_unmatched(order_state, order_id):
            self._force_exchange_maintenance(
                "recovery_probe_order_unverified",
                "recovery_probe_order_verify",
            )
            return False

        try:
            open_orders = await self._read_open_orders(
                "recovery_probe_open_orders_verify"
            )
        except Exception as exc:
            if self._exchange_maintenance_guard().phase != PHASE_MAINTENANCE:
                self._force_exchange_maintenance(
                    "recovery_probe_open_orders_unverified",
                    "recovery_probe_open_orders_verify",
                )
            return False
        if not self._valid_open_orders_response(open_orders):
            self._force_exchange_maintenance(
                "recovery_probe_open_orders_unverified",
                "recovery_probe_open_orders_verify",
            )
            return False
        if any(self._order_id(order) == order_id for order in open_orders):
            self._force_exchange_maintenance(
                "recovery_probe_still_open",
                "recovery_probe_open_orders_verify",
            )
            return False

        cached = self._market_live_orders.get(token_id, [])
        self._market_live_orders[token_id] = [
            order for order in cached if self._order_id(order) != order_id
        ]
        return True

    async def _execute_exchange_cancel(
        self,
        action: str,
        method: Any,
        *args: Any,
    ) -> bool:
        guard = self._exchange_maintenance_guard()
        claim = guard.claim_cancel_attempt()
        if claim is None:
            return False
        try:
            await asyncio.to_thread(method, *args)
        except Exception as exc:
            self._record_exchange_maintenance_error(exc, action)
            guard.finish_cancel_attempt(claim, success=False)
            raise
        guard.finish_cancel_attempt(claim, success=True)
        return True

    async def _get_all_orders_cached(self) -> list[dict]:
        """Return all open orders, coalescing concurrent refreshes via a short TTL cache.

        Collapses the per-token-defense fan-out (one get_orders per WS event
        per token) into one REST request per TTL window, eliminating the
        Cloudflare 429 storm on CLOB. Invalidated on successful place/cancel
        via _invalidate_all_orders_cache.
        """
        cached = self._all_orders_cache
        if cached and (time.time() - cached[1]) < self._all_orders_cache_ttl_sec:
            return cached[0]
        if self._all_orders_refresh_lock is None:
            self._all_orders_refresh_lock = asyncio.Lock()
        async with self._all_orders_refresh_lock:
            cached = self._all_orders_cache
            if cached and (time.time() - cached[1]) < self._all_orders_cache_ttl_sec:
                return cached[0]
            orders = await self._read_open_orders("orders_cache_refresh")
            self._all_orders_cache = (list(orders) if orders else [], time.time())
            return self._all_orders_cache[0]

    def _invalidate_all_orders_cache(self) -> None:
        """Clear the shared orders cache. Call after any write (place/cancel)."""
        self._all_orders_cache = None

    async def _refresh_live_orders(self, token_id: str) -> list[dict]:
        orders = await self._get_all_orders_cached()
        live = [
            o for o in orders
            if _order_is_live(o)
            and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
        ]
        live = self._sorted_live_orders(live)
        self._market_live_orders[token_id] = live
        self._sibling_sync_token(token_id, live)
        return live

    def _sibling_sync_token(self, token_id: str, live: list) -> None:
        """施工包04:refresh 对账——注册表与引擎本地清单同点同步(整体重建)。"""
        registry = getattr(self, "_sibling_registry", None)
        if registry is None:
            return
        entries = []
        for o in live:
            oid = self._order_id(o)
            side = str(o.get("side", "")).upper()
            if not oid or side not in ("BUY", "SELL"):
                continue
            try:
                price = float(o.get("price") or 0)
            except (TypeError, ValueError):
                continue
            try:
                size = float(o.get("original_size") or o.get("size") or 0)
            except (TypeError, ValueError):
                size = 0.0
            entries.append((oid, side, price, size))
        registry.sync_token(self._funder_lc, token_id, entries)

    async def _cancel_order_ids(self, token_id: str, ids: list[str], reason: str) -> bool:
        ids = [str(x) for x in ids if x]
        # Quote/risk paths manage BUY liquidity. A SELL is an inventory exit
        # (including one placed manually on the website) and must never be
        # swept up by a generic planner cancellation.
        sell_ids = {
            self._order_id(order)
            for orders in self._market_live_orders.values()
            for order in orders
            if str(order.get("side") or "").upper() == "SELL"
        }
        protected = [order_id for order_id in ids if order_id in sell_ids]
        if protected:
            log(
                f"[manual-exit] preserved {len(protected)} SELL order(s) "
                f"during cancel reason={reason}"
            )
            ids = [order_id for order_id in ids if order_id not in sell_ids]
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
            canceled = await self._execute_exchange_cancel(
                f"cancel_orders:{cancel_kind}",
                self.client.cancel_orders,
                ids,
            )
            if not canceled:
                return False
            self._mark_latency(token_id, "t_cancel_ack")
            self._invalidate_all_orders_cache()
        except Exception as e:
            log(f"[cancel] token={token_id} kind={cancel_kind} reason={reason} err={e}")
            # Cancel failures on request-exception storms were invisible to the
            # global kill-switch counter before (2026-04-24 14:30-15:15 window
            # had 180/min cancel exceptions but kill-switch only fired at 15:17).
            # Feed cancel exceptions into the counter so the kill-switch fires
            # within a minute of the storm starting, not 47 minutes later.
            if self._is_req_exc(e):
                self._log_req_diag(f"cancel:{cancel_kind}", e, token_id)
                await self._mark_req_exc_and_maybe_storm(
                    f"cancel:{token_id}", "global_request_exception_storm"
                )
            return False
        # Evict canceled order IDs from cache immediately to prevent ghost orders.
        # _get_live_orders_fast returns cached data; without eviction the stale
        # entries persist and cause the planner to cancel already-gone orders
        # while placing new ones, accumulating orphans on the exchange.
        canceled_set = set(ids)
        cached = self._market_live_orders.get(token_id, [])
        self._market_live_orders[token_id] = [
            o for o in cached if self._order_id(o) not in canceled_set
        ]
        self._sibling_registry.unregister_many(self._funder_lc, ids)  # 施工包04:与剔除同点
        live = self._market_live_orders[token_id]
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
        matched_size_source: str = "",
        halt_key: str = "t_fill_seen",
    ) -> None:
        self._arm_halt_preemption(token_id, reason)
        lock = self._event_locks[token_id]
        async with lock:
            state = self._event_state_name(token_id)
            if state == EVENT_HALTED_ON_FILL:
                return
            if state == EVENT_HALTED_ON_DATA and final_state != EVENT_HALTED_ON_FILL:
                return
            if state in {EVENT_EXIT_PENDING, EVENT_PENDING_MANUAL_EXIT}:
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
                "size_source": matched_size_source or None,
                "reason": reason,
                "ts": time.time(),
                "final_state": final_state,
            })
            if final_state == EVENT_HALTED_ON_FILL:
                managed_positions = getattr(
                    self, "_position_reconcile_managed_tokens", set()
                )
                managed_positions.add(str(token_id))
                self._position_reconcile_managed_tokens = managed_positions
            if len(self._fills_record) > 200:
                self._fills_record = self._fills_record[-100:]
            ids = [
                self._order_id(o)
                for o in self._cached_live_orders(token_id)
            ]
            cleared = await self._cancel_risk_buys(
                token_id,
                f"halt:{reason}",
            )
            if cleared:
                self._set_event_state(token_id, final_state, reason)
            else:
                log(f"[event-state] token={token_id} state={EVENT_CANCELING} reason=cancel_not_cleared")
            self._emit_latency_record(
                token_id,
                "event_halt",
                {"reason": reason, "final_state": final_state, "orders_targeted": len(ids)},
            )
            paired = self._paired_token_id(token_id)
            if (
                paired
                and (
                    paired in self.market_cfg
                    or paired in self._night_market_cfg
                )
                and self._event_state_name(paired) == EVENT_ACTIVE
            ):
                asyncio.create_task(
                    self._request_event_halt(
                        paired,
                        final_state,
                        f"paired_halt_from:{token_id}",
                        halt_key="t_detect",
                    )
                )


    async def _post_delay(self, label: str) -> None:
        # Unified pace — no fast path for defense, everything goes through same rhythm
        lo = max(0.0, self.post_delay_min_sec)
        hi = max(lo, self.post_delay_max_sec)
        d = random.uniform(lo, hi)
        log(f"[pace] {label} sleep={d:.2f}s")
        await asyncio.sleep(d)

    async def _acquire_order_throttle(
        self,
        token_id: str,
        label: str,
    ) -> Optional[tuple[str, int, str]]:
        """Unified order throttle: per-token + global.
        Must be called before every real order placement."""
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        # Only the front reward leg needs atomic YES/NO admission. Back legs
        # can differ in count and price across outcomes; pairing every layer
        # would manufacture false failures after the event already has both
        # required front BUY legs live.
        claim = (
            None
            if "back_leg" in str(label).lower()
            else await self._claim_aggressive_pair_submit(token_id)
        )
        if claim is not None and claim[2] == "follower":
            attempt = await self._aggressive_pair_attempt(claim)
            if attempt is None:
                raise SoftQuoteSkip(
                    f"paired_submit_missing token={token_id[:16]} label={label}"
                )
            leader_wait_sec = max(
                self._global_order_max_sec,
                self._per_token_order_min_sec,
            ) + self._aggressive_pair_submit_timeout_sec + 10.0
            try:
                await asyncio.wait_for(
                    attempt["leader_posted"].wait(),
                    timeout=leader_wait_sec,
                )
            except asyncio.TimeoutError:
                await self._aggressive_pair_submit_failed(
                    token_id,
                    claim,
                    "leader_wait_timeout",
                )
                raise SoftQuoteSkip(
                    f"paired_submit_leader_timeout token={token_id[:16]} label={label}"
                )

            attempt = await self._aggressive_pair_attempt(claim)
            if (
                attempt is None
                or attempt.get("failed")
                or str(attempt.get("leader")) not in attempt.get("posted", set())
            ):
                raise SoftQuoteSkip(
                    f"paired_submit_leader_failed token={token_id[:16]} label={label}"
                )

            per_token_last = self._per_token_last_order_ts.get(token_id, 0.0)
            per_token_wait = max(
                0.0,
                self._per_token_order_min_sec - (time.time() - per_token_last),
            )
            deadline = float(attempt.get("first_post_at", 0.0)) + self._aggressive_pair_submit_timeout_sec
            if per_token_wait > 0:
                if time.time() + per_token_wait >= deadline:
                    await self._aggressive_pair_submit_failed(
                        token_id,
                        claim,
                        "follower_per_token_throttle_timeout",
                    )
                    raise SoftQuoteSkip(
                        f"paired_submit_follower_throttled token={token_id[:16]} label={label}"
                    )
                await asyncio.sleep(per_token_wait)
            if time.time() >= deadline:
                await self._aggressive_pair_submit_failed(
                    token_id,
                    claim,
                    "follower_deadline_elapsed",
                )
                raise SoftQuoteSkip(
                    f"paired_submit_follower_late token={token_id[:16]} label={label}"
                )

            order_ts = time.time()
            self._global_last_order_ts = max(self._global_last_order_ts, order_ts)
            self._per_token_last_order_ts[token_id] = order_ts
            log(
                f"[paired-submit] token={slug} role=follower "
                f"action=fast_follow label={label}"
            )
            return claim

        async with self._global_order_lock:
            global_min = random.uniform(self._global_order_min_sec, self._global_order_max_sec)
            while True:
                now = time.time()
                per_token_last = self._per_token_last_order_ts.get(token_id, 0.0)
                per_token_wait = max(
                    0.0,
                    self._per_token_order_min_sec - (now - per_token_last),
                )
                global_wait = max(
                    0.0,
                    global_min - (now - self._global_last_order_ts),
                )
                wait = max(per_token_wait, global_wait)
                if wait <= 0:
                    break
                await asyncio.sleep(wait)

            # Record timestamps
            order_ts = time.time()
            self._global_last_order_ts = order_ts
            self._per_token_last_order_ts[token_id] = order_ts
        return claim


    @staticmethod
    def _infer_tick_from_book(best_bid: Decimal, best_ask: Decimal) -> Decimal:
        # Common Polymarket price grids are 0.01 (1c) or 0.001 (0.1c)
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

            orders = await self._read_open_orders("best_bid_guard_token")
            protected_exit_ids = set(self._active_exit_orders.values())
            at_risk = [
                o for o in orders
                if _order_is_live(o)
                and str(o.get("asset_id") or o.get("token_id") or "") == str(token_id)
                and Decimal(str(o.get("price", 0) or 0)) > risk_limit
                and str(o.get("side", "")).upper() != "SELL"
                and str(o.get("id") or o.get("orderID") or "") not in protected_exit_ids
            ]
            if not at_risk:
                return
            ids = [o.get("id") or o.get("orderID") for o in at_risk if (o.get("id") or o.get("orderID"))]
            if ids:
                canceled = await self._execute_exchange_cancel(
                    "cancel_best_bid_guard",
                    self.client.cancel_orders,
                    ids,
                )
                if not canceled:
                    return
                self._sibling_registry.unregister_many(self._funder_lc, [str(x) for x in ids])
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
        guard_interval = 3.0          # seconds between outer loop ticks
        per_token_interval = 15.0     # min seconds between book fetches per token
        _last_guard_ts: dict[str, float] = {}

        while self._running:
            try:
                # Market-WS down detection: if no message received for too long,
                # cancel all orders — we are blind to market changes.
                if self._last_market_ws_ok_ts > 0:
                    market_ws_age = time.time() - self._last_market_ws_ok_ts
                    if market_ws_age > self._market_ws_down_cancel_sec:
                        shared_cache_fresh = bool(
                            self._shared_book_cache is not None
                            and self._shared_book_cache.has_fresh_books(self.market_cfg.keys())
                        )
                        if shared_cache_fresh:
                            # Quiet markets may not emit WS events for longer than the
                            # cancel threshold. A complete fresh REST batch means the
                            # guard still has current prices and can continue its
                            # normal per-order checks below.
                            market_ws_age = 0.0
                        else:
                            if market_ws_age >= self._proxy_failover_ws_down_trigger_sec:
                                asyncio.create_task(self._maybe_failover_proxy("market_ws_down"))
                            maintenance_guard = self._exchange_maintenance_guard()
                            same_market_data_incident = bool(
                                maintenance_guard.phase == PHASE_MAINTENANCE
                                and maintenance_guard.last_action
                                == "market_data_guard"
                            )
                            maintenance_reason = (
                                maintenance_guard.reason
                                if maintenance_guard.active
                                and maintenance_guard.reason
                                and maintenance_guard.reason
                                != "market_data_unavailable"
                                else "market_data_unavailable"
                            )
                            paused_verified = bool(
                                self._is_account_paused()
                                and getattr(self, "_was_paused", False)
                            )
                            self._force_exchange_maintenance(
                                maintenance_reason,
                                "market_data_guard",
                                immediate_cancel=(
                                    not paused_verified
                                    and not same_market_data_incident
                                ),
                                cancel_already_verified=paused_verified,
                            )
                            action_text = (
                                "account already paused; BUYs previously verified absent"
                                if paused_verified
                                else "BUY cancellation owned by safety lane; SELL exits preserved"
                            )
                            log(
                                f"[guard-loop] market-ws down {market_ws_age:.0f}s > "
                                f"{self._market_ws_down_cancel_sec:.0f}s and REST books stale "
                                f"— {action_text}"
                            )
                            for tid in self.market_cfg:
                                self._last_plan_sig[tid] = ""
                                self.last_quote_ts[tid] = 0.0
                            await asyncio.sleep(guard_interval)
                            continue

                orders = await self._read_open_orders("best_bid_guard_loop")
                live = [
                    o for o in orders
                    if _order_is_live(o)
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

                # Sync cache with reality: update _market_live_orders from
                # exchange data so stale entries don't persist.
                for tid, tok_orders in by_token.items():
                    self._market_live_orders[tid] = self._sorted_live_orders(tok_orders)
                # Also clear cache for tokens that have no live orders on exchange
                for tid in list(self._market_live_orders.keys()):
                    if tid in self.market_cfg and tid not in by_token:
                        self._market_live_orders[tid] = []

                now = time.time()
                cancel_ids = []
                for tid, tok_orders in by_token.items():
                    # Skip tokens in PENDING_MANUAL_EXIT — user may be placing
                    # manual sell orders; guard must not interfere.
                    if self._event_state_name(tid) == EVENT_PENDING_MANUAL_EXIT:
                        continue
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
                        protected_exit_ids = set(self._active_exit_orders.values())
                        for o in tok_orders:
                            oid = o.get("id") or o.get("orderID") or ""
                            if oid in protected_exit_ids:
                                continue  # never cancel active exit SELL orders
                            side = str(o.get("side", "")).upper()
                            if side == "SELL":
                                continue  # guard loop only manages BUY orders
                            op = Decimal(str(o.get("price", 0) or 0))
                            if op > risk_limit:
                                if oid:
                                    cancel_ids.append((tid, risk_limit, oid))
                    except Exception:
                        continue

                if cancel_ids:
                    ids = [oid for _, _, oid in cancel_ids]
                    canceled = await self._execute_exchange_cancel(
                        "cancel_best_bid_guard_batch",
                        self.client.cancel_orders,
                        ids,
                    )
                    if not canceled:
                        await asyncio.sleep(guard_interval)
                        continue
                    self._sibling_registry.unregister_many(self._funder_lc, [str(x) for x in ids])

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

        boundary = executable_quote_boundary(
            best_bid=book.best_bid,
            midpoint=book.mid,
            tick=tick,
            max_spread=spread,
            min_distance_ticks=cfg.get("min_distance_ticks", 1),
        )
        reward_lower = boundary.reward_lower
        safe_top = boundary.safe_top

        if not boundary.executable:
            # No valid position exists in reward zone; skip this market
            log(f"[price-legs-skip] slug={_slug} token={token_id[:16]}... bid={book.best_bid} ask={book.best_ask} mid={book.mid} spread={spread} reward_lower={reward_lower} safe_top={safe_top} tick={tick} reason={boundary.blocked_reason}")
            return []

        reward_zone_width = safe_top - reward_lower

        # # Fine-tick markets (tick < 0.01): percentage-based distribution
        if tick < Decimal("0.01"):
            return self._build_fine_tick_legs(
                token_id, book, tick, reward_lower, safe_top, reward_zone_width, _slug,
            )

        # # Regular 1-cent markets: original mechanical logic
        range_ticks = int((safe_top - reward_lower) / tick) + 1

        max_legs = 3

        if range_ticks <= 1 and safe_top >= reward_lower and safe_top >= tick:
            return [self._floor_to_tick(safe_top, tick)]

        n_legs = min(range_ticks, max_legs)
        if n_legs <= 0:
            return []

        prices = []
        for i in range(n_legs):
            p = self._floor_to_tick(safe_top - tick * Decimal(i), tick)
            # Liquidity rewards score falls to zero exactly at the max-spread boundary.
            if p >= reward_lower and p >= tick and p not in prices:
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
        """Generate price legs for fine-tick (0.001) markets.

        Top leg starts at safe_top (best_bid - min_distance_ticks * tick),
        then remaining legs are spread downward across a portion of the
        reward zone.

        Config knobs (in strategy section):
          fine_tick_max_legs — max number of legs (default 5)
          fine_tick_zone_use_pct — what fraction of the reward zone to
                                   spread legs across (default 0.50)
        """
        strategy = self.cfg.get("strategy", {})
        max_legs = int(strategy.get("fine_tick_max_legs", 5))
        zone_use_pct = Decimal(str(strategy.get("fine_tick_zone_use_pct", "0.50")))

        # Clamp config values to sane ranges
        zone_use_pct = max(Decimal("0.10"), min(zone_use_pct, Decimal("0.80")))

        # Top leg at safe_top (already min_distance_ticks behind best_bid).
        # Remaining legs spread across zone_use_pct of the reward zone below safe_top.
        top_start = safe_top
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

    # # Share-based sizing for Q_min optimization

    def _lp_effective_available(
        self,
        available: Optional[Decimal],
    ) -> Optional[Decimal]:
        if available is None:
            return None
        profile = getattr(self, "lp_account_profile", None)
        if profile is None:
            return available
        return profile.effective_available(available)

    def _stable_lifecycle_canary_budget_usdc(
        self,
        available: Optional[Decimal],
    ) -> Optional[Decimal]:
        """Return the per-event canary cap without expanding account capital.

        The lifecycle hard limit is ten active canaries. Therefore ten markets
        at ``min(10% of principal, $100)`` can never target more than the
        account principal in aggregate.
        """

        profile = getattr(self, "lp_account_profile", None)
        principal: Optional[Decimal] = None
        if profile is not None and getattr(profile, "managed", False):
            principal = Decimal(str(profile.target_principal_usdc))
        elif available is not None:
            principal = max(Decimal("0"), Decimal(str(available)))
        if principal is None:
            return None
        fraction = Decimal(
            str(
                getattr(
                    self,
                    "_stable_lifecycle_canary_principal_fraction",
                    Decimal("0.10"),
                )
            )
        )
        absolute_cap = Decimal(
            str(
                getattr(
                    self,
                    "_stable_lifecycle_canary_max_usdc",
                    Decimal("100"),
                )
            )
        )
        return max(
            Decimal("0"),
            min(principal * fraction, absolute_cap),
        )

    async def _compute_target_shares(
        self,
        token_id: str,
        *,
        budget_pct: Decimal = Decimal("1"),
        size_cap: Decimal = Decimal("1"),
    ) -> tuple[Decimal, Decimal, str]:
        """Compute target bid/ask shares for Q_min maximization.

        Returns (target_bid_shares, target_ask_shares, warning).
        - Both sides equal: target = max(floor(balance), rewardsMinSize)
        - If balance < rewardsMinSize: single-side only + warning
        - 1 share = $1 collateral (YES + NO on same event share the collateral)
        """
        meta = await self._get_market_meta(token_id)
        rewards_min = Decimal(str(meta.get("rewardsMinSize", 0) or 0))
        if rewards_min <= 0:
            rewards_min = Decimal(str(self.min_order_size))

        avail = self._lp_effective_available(
            await self._get_collateral_available()
        )
        if avail is None or avail <= 0:
            return Decimal("0"), Decimal("0"), "no_balance"

        market_cfg = (
            getattr(self, "market_cfg", {}).get(token_id)
            or getattr(self, "_night_market_cfg", {}).get(token_id)
            or {}
        )
        if str(market_cfg.get("lifecycle_stage") or "").lower() == "canary":
            canary_cap = self._stable_lifecycle_canary_budget_usdc(avail)
            if canary_cap is None or canary_cap <= 0:
                return Decimal("0"), Decimal("0"), "canary_budget_unavailable"
            avail = min(avail, canary_cap)
            budget_pct = Decimal("1")

        target, warning = _compute_quote_target_shares(
            available=avail,
            rewards_min=rewards_min,
            min_order_size=self.min_order_size,
            budget_pct=budget_pct,
            size_cap=size_cap,
            max_quote_shares=self.max_quote_shares_per_market,
        )
        return target, target, warning

    def _is_low_price_market(self, token_id: str) -> bool:
        """Check if market midpoint is in low-price territory (<0.10 or >0.90)."""
        snap = self._market_snapshots.get(token_id)
        if not snap:
            return False
        mid = (snap.best_bid + snap.best_ask) / Decimal("2") if snap.best_bid > 0 and snap.best_ask > 0 else Decimal("0")
        return mid < Decimal("0.10") or mid > Decimal("0.90")

    def calculate_q_min_efficiency(self, token_id: str) -> tuple[Decimal, Dict[str, Any]]:
        """Calculate Q_min efficiency for a token (0~1).

        Returns (efficiency, details_dict).
        - Low-price: Q_min = min(Q_one, Q_two) → efficiency = min/max
        - Normal: Q_min = max(min(Q_one,Q_two), max(Q_one/3,Q_two/3)) → efficiency = Q_min/max
        - Orders below rewardsMinSize are treated as invalid (Q=0).
        """
        paired_tid = (
            self._paired_token_cache.get(token_id)
            or str(self._get_mcfg(token_id).get("paired_token_id", "") or "")
        )
        # Get reward min size
        meta_cached = self._market_meta_cache.get(token_id)
        rewards_min = Decimal("0")
        max_spread = Decimal("0.03")
        if meta_cached:
            rewards_min = Decimal(str(meta_cached[0].get("rewardsMinSize", 0) or 0))
            spread_raw = meta_cached[0].get("maxIncentiveSpread") or meta_cached[0].get("rewardsMaxSpread")
            if spread_raw is not None:
                max_spread = Decimal(str(spread_raw))

        snap = self._market_snapshots.get(token_id)
        midpoint = Decimal("0")
        if snap and snap.best_bid > 0 and snap.best_ask > 0:
            midpoint = (snap.best_bid + snap.best_ask) / Decimal("2")

        # Compute Q for this side (YES or NO)
        live_orders = self._cached_live_orders(token_id)
        q_this = Decimal("0")
        this_shares = Decimal("0")
        for o in live_orders:
            size = self._order_size(o)
            price = self._order_price(o)
            if size < rewards_min:
                continue  # below minimum, doesn't count
            ds = self._distance_score(price, midpoint, max_spread)
            q_this += ds * size
            this_shares += size

        # Compute Q for paired side
        q_paired = Decimal("0")
        paired_shares = Decimal("0")
        if paired_tid:
            paired_orders = self._cached_live_orders(paired_tid)
            paired_meta = self._market_meta_cache.get(paired_tid)
            paired_min = Decimal("0")
            paired_spread = max_spread
            if paired_meta:
                paired_min = Decimal(str(paired_meta[0].get("rewardsMinSize", 0) or 0))
                ps_raw = paired_meta[0].get("maxIncentiveSpread") or paired_meta[0].get("rewardsMaxSpread")
                if ps_raw is not None:
                    paired_spread = Decimal(str(ps_raw))
            paired_snap = self._market_snapshots.get(paired_tid)
            paired_mid = Decimal("0")
            if paired_snap and paired_snap.best_bid > 0 and paired_snap.best_ask > 0:
                paired_mid = (paired_snap.best_bid + paired_snap.best_ask) / Decimal("2")
            for o in paired_orders:
                size = self._order_size(o)
                price = self._order_price(o)
                if size < paired_min:
                    continue
                ds = self._distance_score(price, paired_mid, paired_spread)
                q_paired += ds * size
                paired_shares += size

        # Compute Q_min and efficiency
        is_low = self._is_low_price_market(token_id)
        q_max = max(q_this, q_paired) if max(q_this, q_paired) > 0 else Decimal("1")

        if is_low:
            q_min = min(q_this, q_paired)
        else:
            q_min = max(min(q_this, q_paired), max(q_this / Decimal("3"), q_paired / Decimal("3")))

        efficiency = q_min / q_max if q_max > 0 else Decimal("0")

        details = {
            "q_this": float(q_this),
            "q_paired": float(q_paired),
            "q_min": float(q_min),
            "efficiency": float(efficiency),
            "is_low_price": is_low,
            "this_shares": float(this_shares),
            "paired_shares": float(paired_shares),
            "rewards_min_size": float(rewards_min),
            "has_dual_side": bool(paired_tid and q_paired > 0),
        }
        return efficiency, details

    # # Dual-side: paired mode helpers

    def _is_low_price_paired_mode(self, token_id: str) -> tuple[bool, str]:
        """Check whether token_id should enter paired (both-or-none) mode.

        Returns (paired_mode: bool, paired_token_id: str).
        Only activates when:
          - dual_side is enabled globally
          - the token is NOT itself an auto-injected NO token
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
        *,
        enforce_all_pairs: bool = False,
    ) -> tuple[bool, str]:
        """Validate that the paired side can also produce a valid plan.

        Low-price mode checks only the risky tail by default. Coordinated
        stable/aggressive LP sets ``enforce_all_pairs`` so every YES/NO pair
        is validated before either leg is allowed to quote.
        """
        yes_is_low = yes_top_price <= self._dual_side_max_mid
        no_is_low = yes_top_price >= (Decimal("1") - self._dual_side_max_mid)
        if not enforce_all_pairs and not yes_is_low and not no_is_low:
            # Neither side is in low-price territory — paired mode not needed
            return True, ""

        pair_snap = self._market_snapshots.get(paired_token)
        if pair_snap is None:
            return False, "paired_side_unavailable"

        pair_snap = self._effective_snapshot_for_gate(paired_token, pair_snap)
        pair_quote_ready, pair_quote_reason = self._quote_gate(
            paired_token,
            pair_snap,
        )
        if not pair_quote_ready:
            return False, f"paired_side_quote_gate:{pair_quote_reason}"

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

        # # Unified paired budget pre-check
        # Compute real available capital: balance/allowance minus ALL active
        # orders across every market, minus a safety buffer.  Both YES and NO
        # notional must fit within this single envelope.
        pair_top_price = pair_prices[0]
        pair_reward_min = Decimal(str(pair_meta.get("rewardsMinSize") or 0))
        pair_min_size = max(self.min_order_size, pair_reward_min, Decimal("0.001"))

        own_cached = self._market_meta_cache.get(token_id)
        own_meta = own_cached[0] if own_cached else {}
        own_reward_min = Decimal(str(own_meta.get("rewardsMinSize") or 0))
        own_min_size = max(self.min_order_size, own_reward_min, Decimal("0.001"))

        own_stage = str(
            self._get_mcfg(token_id).get("lifecycle_stage") or "full"
        ).strip().lower()
        pair_stage = str(
            self._get_mcfg(paired_token).get("lifecycle_stage") or "full"
        ).strip().lower()
        if "canary" in {own_stage, pair_stage}:
            canary_budget = self._stable_lifecycle_canary_budget_usdc(
                self._last_balance
            )
            if canary_budget is None or canary_budget <= 0:
                return False, "canary_budget_unavailable"
            if max(own_min_size, pair_min_size) > canary_budget:
                return False, "canary_reward_min_above_budget"

        avail = self._last_balance
        if avail is not None and avail > 0:
            # Each resting BUY reserves its actual price * size. Requiring one
            # full dollar per YES/NO share pair makes a $200 account unable to
            # quote a valid 0.41 + 0.56 pair even though it costs only $194.
            safety_buffer = avail * Decimal("0.02")  # 2% safety cushion
            real_avail = max(Decimal("0"), avail - safety_buffer)
            combined_min_notional = (
                yes_top_price * own_min_size
                + pair_top_price * pair_min_size
            )

            slug = self._token_slug_cache.get(token_id, token_id[:16])
            if real_avail < combined_min_notional:
                log(
                    f"[paired-budget] slug={slug} token={token_id[:16]} "
                    f"real_avail={real_avail} required={combined_min_notional} "
                    f"legs={yes_top_price}x{own_min_size}+{pair_top_price}x{pair_min_size}"
                )
                return False, "paired_side_budget_insufficient"
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

    # # Dual-side: auto-inject paired NO token for ALL markets

    def _maybe_inject_dual_side_tokens(self) -> None:
        """Auto-register the paired NO token for all markets that have a
        paired_token_id, so the engine quotes BUY on both sides.

        Called once during startup after market metadata is resolved.
        The NO token is added to market_cfg as a regular market entry.
        Both-side quoting maximizes Q_min for LP rewards.
        """
        if not self._dual_side_enabled:
            return

        # Process day and night pools separately — the NO side is injected into
        # the same pool as the YES side so session routing stays clean.
        for pool in (self.market_cfg, self._night_market_cfg):
            to_inject: list[tuple[str, str, Dict[str, Any]]] = []
            for token_id, mcfg in list(pool.items()):
                if token_id in self._dual_side_injected:
                    continue
                paired = mcfg.get("paired_token_id", "")
                if not paired or paired in pool:
                    continue  # already present or no pair known
                to_inject.append((token_id, paired, mcfg))

            for yes_tid, no_tid, yes_cfg in to_inject:
                pool[no_tid] = {
                    "spread": yes_cfg["spread"],
                    "tick": yes_cfg["tick"],
                    "min_distance": yes_cfg.get("min_distance", self.default_min_distance),
                    "min_distance_ticks": yes_cfg.get("min_distance_ticks", self.default_min_distance_ticks),
                    "risk": self._dual_side_no_risk,
                    "base_risk": self._dual_side_no_risk,
                    "session": yes_cfg.get("session", "both"),
                    "paired_token_id": yes_tid,
                    "source": yes_cfg.get("source", "manual"),
                    "eligibility_managed": bool(
                        yes_cfg.get("eligibility_managed", False)
                    ),
                    "lifecycle_stage": str(
                        yes_cfg.get("lifecycle_stage") or ""
                    ),
                    "lifecycle_retire_pending": bool(
                        yes_cfg.get("lifecycle_retire_pending", False)
                    ),
                    "_dual_side_auto": True,
                    "game_start_ts_override": float(yes_cfg.get("game_start_ts_override", 0.0) or 0.0),
                    "pre_start_stop_sec_override": int(yes_cfg.get("pre_start_stop_sec_override", 0) or 0),
                    "league_tags": list(yes_cfg.get("league_tags") or []),
                    "condition_id": str(yes_cfg.get("condition_id") or "").strip().lower(),
                }
                if pool[no_tid]["condition_id"]:
                    self._market_condition_ids[no_tid] = pool[no_tid]["condition_id"]
                # Initialise runtime state for the new token
                self._ensure_runtime_token_state(no_tid, reason="dual_side_inject")
                retire_pending = bool(
                    pool[no_tid].get("lifecycle_retire_pending")
                )
                self._event_states[no_tid] = {
                    "state": EVENT_WATCH if retire_pending else EVENT_ACTIVE,
                    "reason": (
                        "stable_lifecycle_retire_pending"
                        if retire_pending
                        else "dual_side_inject"
                    ),
                    "updated_at": time.time(),
                }
                self._dual_side_injected.add(no_tid)
                slug = self._token_slug_cache.get(yes_tid, yes_tid[:16])

    # # Runtime market management (auto_curator hot-add / T-2h hot-remove)

    def add_market_runtime(
        self,
        token_id: str,
        paired_token_id: str,
        spread: Any,
        tick: Any = None,
        min_distance: Any = None,
        min_distance_ticks: Any = None,
        risk: str = "mid",
        session: str = "day",
        source: str = "auto_curator",
        game_start_ts: Optional[float] = None,
        slug: Optional[str] = None,
        league: Optional[str] = None,
        question: Optional[str] = None,
        pre_start_stop_sec_override: Optional[int] = None,
        league_tags: Optional[list[str]] = None,
        condition_id: Optional[str] = None,
        eligibility_managed: bool = False,
        eligibility_base_risk: Optional[str] = None,
        lifecycle_stage: str = "",
        persist: bool = True,
        notify: bool = True,
        require_new_persisted_pair: bool = False,
    ) -> bool:
        """Register a new market at runtime (no restart). Mirrors the init-time
        market_cfg schema + minimum per-token state (same 5 dicts that dual-side
        inject populates; the rest tolerate missing keys via .get(tid, default)).
        Triggers dual-side paired-token injection and requests a market WS
        resubscribe so book updates flow in for the new token.
        Returns True if newly added, False if token was already registered.
        """
        token_id = str(token_id).strip()
        if not token_id.isdigit():
            raise ValueError(f"invalid token_id: {token_id}")
        # Dedup across BOTH pools — a market must not appear in day and night at once.
        if token_id in self.market_cfg or token_id in self._night_market_cfg:
            return False
        paired_token_id = str(paired_token_id).strip()
        if not paired_token_id.isdigit() or paired_token_id == token_id:
            raise ValueError(f"invalid paired_token_id: {paired_token_id}")
        if (
            paired_token_id in self.market_cfg
            or paired_token_id in self._night_market_cfg
        ):
            raise ValueError(
                f"paired token is already registered: {paired_token_id}"
            )
        preexisting_runtime_tokens = set(self.market_cfg) | set(
            self._night_market_cfg
        )

        session_label = str(session).lower()
        target_cfg = self._night_market_cfg if session_label == "night" else self.market_cfg

        target_cfg[token_id] = {
            "spread": Decimal(str(spread)),
            "tick": Decimal(str(tick)) if tick is not None else self.default_tick,
            "min_distance": Decimal(str(min_distance)) if min_distance is not None else self.default_min_distance,
            "min_distance_ticks": int(min_distance_ticks) if min_distance_ticks is not None else self.default_min_distance_ticks,
            "risk": str(risk).lower(),
            "base_risk": str(eligibility_base_risk or risk).lower(),
            "session": session_label,
            "paired_token_id": paired_token_id,
            "source": source,
            "eligibility_managed": bool(eligibility_managed),
            "lifecycle_stage": str(lifecycle_stage or ""),
            "lifecycle_retire_pending": False,
            "_runtime_added": True,
            "game_start_ts_override": float(game_start_ts) if game_start_ts else 0.0,
            "pre_start_stop_sec_override": int(pre_start_stop_sec_override or 0),
            "league_tags": list(league_tags or []),
            "condition_id": str(condition_id or "").strip().lower(),
        }
        if target_cfg[token_id]["condition_id"]:
            self._market_condition_ids[token_id] = target_cfg[token_id]["condition_id"]
        self._ensure_runtime_token_state(token_id, reason=f"runtime_add:{source}")
        self._event_states[token_id] = {
            "state": EVENT_ACTIVE,
            "reason": f"runtime_add:{source}",
            "updated_at": time.time(),
        }
        self._runtime_added_tokens.add(token_id)

        # Inject paired NO token for dual-side quoting (idempotent)
        try:
            self._maybe_inject_dual_side_tokens()
        except Exception as e:
            log(f"[runtime-add] dual-side inject err: {e}")

        self._request_market_ws_resubscribe()

        slug_display = slug or self._token_slug_cache.get(token_id, token_id[:16])

        # Persist to config.json so a restart (or crash) doesn't drop this market.
        # Mirrors the symmetric prune path in start_guard_sweep_loop at T-2h cutoff.
        try:
            section = "night_markets" if session_label == "night" else "markets"
            persisted_entry = {
                "token_id": token_id,
                "paired_token_id": paired_token_id,
                "side": "YES",
                "max_incentive_spread": float(spread) if spread is not None else 2.5,
                "price_tick": 0.01,
                "min_distance_from_best_bid": 0.01,
                "quote_size": 100.0,
                "risk": str(risk).lower(),
                "enabled": True,
                "source": source,
                "eligibility_managed": bool(eligibility_managed),
                "eligibility_base_risk": str(
                    eligibility_base_risk or risk
                ).lower(),
                "lifecycle_stage": str(lifecycle_stage or ""),
                "lifecycle_retire_pending": False,
                "slug": slug or "",
                "question": question or "",
                "league": league or "",
                "league_tags": list(league_tags or []),
                "condition_id": str(condition_id or "").strip().lower(),
                "game_start_ts": float(game_start_ts) if game_start_ts else 0.0,
                "pre_start_stop_sec_override": int(pre_start_stop_sec_override or 0),
            }
            if persist:
                with self._config_transaction_lock():
                    cfg_disk = json.loads(
                        self._config_path.read_text(encoding="utf-8")
                    )
                    if not isinstance(cfg_disk, dict):
                        raise ValueError("account config is invalid")
                    persisted_tokens = set()
                    for sec in ("markets", "night_markets"):
                        rows = cfg_disk.get(sec) or []
                        if not isinstance(rows, list):
                            raise ValueError(
                                f"account config {sec} is invalid"
                            )
                        for market in rows:
                            if not isinstance(market, dict):
                                raise ValueError(
                                    f"account config {sec} contains invalid market"
                                )
                            persisted_token = str(
                                market.get("token_id") or ""
                            )
                            if persisted_token:
                                persisted_tokens.add(persisted_token)
                    if require_new_persisted_pair and {
                        token_id,
                        paired_token_id,
                    } & persisted_tokens:
                        raise ValueError(
                            "candidate conflicts with existing persisted market"
                        )

                    persisted = False
                    for sec in ("markets", "night_markets"):
                        for market in cfg_disk.get(sec) or []:
                            if str(market.get("token_id") or "") != token_id:
                                continue
                            market.update(persisted_entry)
                            market.pop("pending_activation", None)
                            market.pop("pending_command_id", None)
                            persisted = True
                    if not persisted:
                        entries = cfg_disk.setdefault(section, []) or []
                        entries.append(persisted_entry)
                        cfg_disk[section] = entries
                    self._write_config_atomic(cfg_disk)
                    log(
                        f"[runtime-add] persisted to config.json "
                        f"section={section} token={token_id[:16]}"
                    )
        except Exception as e:
            log(f"[runtime-add] config.json persist err: {e}")
            self._drop_market_runtime_state(
                token_id,
                preserve_tokens=preexisting_runtime_tokens,
            )
            self._request_market_ws_resubscribe()
            raise RuntimeError(
                f"runtime market persistence failed for {token_id[:16]}"
            ) from e

        log(f"[runtime-add] token={token_id[:16]} paired={paired_token_id[:16]} spread={spread} side=YES src={source}")
        if persist:
            try:
                self._curator_events_log.append({
                    "token_id": token_id,
                    "paired_token_id": paired_token_id,
                    "slug": slug_display,
                    "question": question or "",
                    "league": league or "",
                    "game_start_ts": float(game_start_ts) if game_start_ts else 0.0,
                    "added_at": time.time(),
                    "source": source,
                    "session": session_label,
                    "spread": float(spread) if spread is not None else 0.0,
                })
            except Exception as event_exc:
                log(f"[runtime-add] curator_events append err: {event_exc}")
        if notify:
            self.send_discord(
                f"市场已自动加入\n市场：{slug_display}\n"
                f"价差：{spread}\n来源：{source}"
            )

        return True

    @contextmanager
    def _config_transaction_lock(self) -> Iterator[None]:
        lock = getattr(self, "_config_lock", None)
        if lock is None:
            lock = threading.RLock()
            self._config_lock = lock
        state = getattr(self, "_config_lock_state", None)
        if state is None:
            state = threading.local()
            self._config_lock_state = state

        with lock:
            depth = int(getattr(state, "depth", 0))
            if depth:
                state.depth = depth + 1
                try:
                    yield
                finally:
                    state.depth = depth
                return

            lock_path = self._config_path.with_name(
                f".{self._config_path.name}.lock"
            )
            lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_EX)
                state.depth = 1
                yield
            finally:
                state.depth = 0
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)

    def _write_config_atomic(self, config: Mapping[str, Any]) -> None:
        with self._config_transaction_lock():
            temporary = self._config_path.with_name(
                f".{self._config_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
            )
            try:
                temporary.write_text(
                    json.dumps(config, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                temporary.replace(self._config_path)
            finally:
                temporary.unlink(missing_ok=True)

    def _drop_market_runtime_state(
        self,
        token_id: str,
        *,
        preserve_tokens: Iterable[str] = (),
    ) -> bool:
        token_id = str(token_id).strip()
        if token_id in self.market_cfg:
            source_cfg = self.market_cfg
        elif token_id in self._night_market_cfg:
            source_cfg = self._night_market_cfg
        else:
            return False
        paired = str(source_cfg[token_id].get("paired_token_id", ""))
        preserved = {str(tid) for tid in preserve_tokens if str(tid)}
        for tid in (token_id, paired):
            if not tid or tid in preserved:
                continue
            self.market_cfg.pop(tid, None)
            self._night_market_cfg.pop(tid, None)
            self._event_states.pop(tid, None)
            self._event_locks.pop(tid, None)
            self._latency_marks.pop(tid, None)
            self._halt_requested.pop(tid, None)
            self.last_quote_ts.pop(tid, None)
            self._market_balance_fail_streak.pop(tid, None)
            self._market_skip_until.pop(tid, None)
            self._market_stale_fail_streak.pop(tid, None)
            self._market_budget_skip_until.pop(tid, None)
            self._per_token_last_order_ts.pop(tid, None)
            self._health_fail_streak.pop(tid, None)
            self._book_req_exc_streak.pop(tid, None)
            self._volatility_tracker.pop(tid, None)
            self._market_condition_ids.pop(tid, None)
            self._sponsored_guard_by_token.pop(tid, None)
            self._eligibility_state.pop(tid, None)
            self._dual_side_injected.discard(tid)
            self._runtime_added_tokens.discard(tid)
        return True

    async def remove_market_runtime(self, token_id: str, reason: str = "runtime_remove") -> bool:
        """Remove a market at runtime: cancel live orders, drop market_cfg entry
        (plus the dual-side-injected paired NO entry) and all per-token state.
        Works against whichever pool (day or night) the token is in.
        Positions (if any) are NOT touched — operator must clear separately.
        Returns True if removed, False if token was not registered.
        """
        token_id = str(token_id).strip()
        if token_id in self.market_cfg:
            source_cfg = self.market_cfg
        elif token_id in self._night_market_cfg:
            source_cfg = self._night_market_cfg
        else:
            return False
        try:
            live = await self._get_live_orders_fast(token_id)
            if live:
                await self._cancel_order_ids(
                    token_id,
                    [self._order_id(o) for o in live],
                    f"runtime_remove:{reason}",
                )
        except Exception as e:
            log(f"[runtime-remove] cancel err token={token_id[:16]}: {e}")

        self._drop_market_runtime_state(token_id)

        self._request_market_ws_resubscribe()
        log(f"[runtime-remove] token={token_id[:16]} reason={reason}")
        return True

    def _write_runtime_result(self, command_id: str, payload: Dict[str, Any]) -> None:
        result = {
            "command_id": command_id,
            "account": self._account_idx,
            "finished_at": time.time(),
            **payload,
        }
        path = self._runtime_result_dir / f"{command_id}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        tmp.replace(path)

    def _mark_runtime_pending_failed(
        self,
        token_id: str,
        command_id: str,
        error: str,
    ) -> None:
        if not token_id and not command_id:
            return
        try:
            with self._config_transaction_lock():
                config = json.loads(
                    self._config_path.read_text(encoding="utf-8")
                )
                changed = False
                for section in ("markets", "night_markets"):
                    for market in config.get(section) or []:
                        pending_command_id = str(
                            market.get("pending_command_id") or ""
                        )
                        if token_id:
                            if str(market.get("token_id") or "") != token_id:
                                continue
                            if (
                                pending_command_id
                                and pending_command_id != command_id
                            ):
                                continue
                        elif pending_command_id != command_id:
                            continue
                        market["enabled"] = False
                        market["pending_activation"] = False
                        market["activation_error"] = error[:180]
                        market["activation_failed_at"] = time.time()
                        changed = True
                if changed:
                    self._write_config_atomic(config)
        except Exception as exc:
            log(f"[runtime-command] failed-state persist error: {exc}")

    def _defer_runtime_command_parse_error(
        self,
        processing: Path,
        exc: Exception,
    ) -> bool:
        """Restore a partially copied command so the next loop can retry it."""
        if not isinstance(exc, (json.JSONDecodeError, UnicodeDecodeError)):
            return False

        command_id = processing.stem
        failures = getattr(self, "_runtime_command_parse_failures", None)
        if failures is None:
            failures = {}
            self._runtime_command_parse_failures = failures
        attempt = failures.get(command_id, 0) + 1
        limit = max(
            1,
            int(getattr(self, "_runtime_command_parse_retry_limit", 5)),
        )
        if attempt > limit:
            failures.pop(command_id, None)
            return False

        retry_path = processing.with_suffix(".json")
        try:
            processing.replace(retry_path)
        except OSError as restore_exc:
            log(
                f"[runtime-command] id={command_id[:12]} retry restore failed="
                f"{type(restore_exc).__name__}:{str(restore_exc)[:120]}"
            )
            return False

        failures[command_id] = attempt
        log(
            f"[runtime-command] id={command_id[:12]} partial file deferred "
            f"retry={attempt}/{limit}"
        )
        return True

    def _reward_observer_snapshot(
        self,
    ) -> tuple[Optional[Dict[str, Any]], Dict[str, Dict[str, Any]], Optional[float]]:
        try:
            state = json.loads(
                self._eligibility_observer_path.read_text(encoding="utf-8")
            )
        except Exception:
            return None, {}, None
        if not isinstance(state, dict):
            return None, {}, None
        generated_at = state.get("generated_at")
        try:
            age = max(0.0, time.time() - float(generated_at))
        except (TypeError, ValueError):
            age = None
        candidates: Dict[str, Dict[str, Any]] = {}
        for row in state.get("candidates") or []:
            if not isinstance(row, dict):
                continue
            token_id = str(row.get("token_id") or "")
            if token_id.isdigit():
                candidates[token_id] = row
        return state, candidates, age

    def _validate_stable_replacement_candidate(
        self,
        market: Mapping[str, Any],
        policy: Mapping[str, Any],
        *,
        allow_canary: bool = False,
        require_account_execution: bool = False,
    ) -> Dict[str, Any]:
        token_id = str(market.get("token_id") or "").strip()
        paired_token_id = str(market.get("paired_token_id") or "").strip()
        if not token_id.isdigit() or not paired_token_id.isdigit():
            raise ValueError("invalid replacement token pair")

        _, candidates, observer_age = self._reward_observer_snapshot()
        candidate = candidates.get(token_id)
        max_observer_age = float(policy.get("max_observer_age_sec") or 900.0)
        if observer_age is None or observer_age > max_observer_age:
            raise ValueError("reward observer snapshot is stale")
        if not candidate:
            raise ValueError("replacement market is no longer eligible")
        admission_level = "full"
        if allow_canary or require_account_execution:
            eligible, admission_level, admission_reasons = (
                candidate_is_executable_for_account(
                    candidate,
                    int(self._account_idx),
                    allow_canary=allow_canary,
                    expected_account_uid_key=str(
                        getattr(
                            self,
                            "_stable_lifecycle_account_uid_key",
                            "",
                        )
                        or ""
                    ),
                    expected_host_id=str(
                        getattr(self, "_runtime_host_id", "") or ""
                    ),
                )
            )
            if not eligible:
                reason = admission_reasons[0] if admission_reasons else "ineligible"
                raise ValueError(
                    f"replacement market is not account-executable: {reason}"
                )
        elif candidate.get("stable_lp_recommended") is not True:
            raise ValueError("replacement market is no longer eligible")
        if candidate.get("weather_market") is True or str(
            candidate.get("market_type") or ""
        ).lower() == "weather":
            raise ValueError("weather market is observe-only for stable LP")
        if str(candidate.get("paired_token_id") or "") != paired_token_id:
            raise ValueError("replacement token pair changed")
        expected_condition = str(market.get("condition_id") or "").strip().lower()
        current_condition = str(candidate.get("condition_id") or "").strip().lower()
        if expected_condition and current_condition != expected_condition:
            raise ValueError("replacement condition changed")
        if str(candidate.get("market_phase") or "").lower() == "live":
            raise ValueError("live market is observe-only")
        if candidate.get("market_active") is not True:
            raise ValueError("replacement market is not active")
        if candidate.get("market_closed") is not False:
            raise ValueError("replacement market is closed or unknown")
        if candidate.get("market_archived") is not False:
            raise ValueError("replacement market is archived or unknown")
        if candidate.get("accepting_orders") is not True:
            raise ValueError("replacement market is not accepting orders")
        market_end_ts = float(candidate.get("market_end_ts") or 0.0)
        if market_end_ts <= time.time():
            raise ValueError("replacement market has expired or has no end time")
        min_time_to_end = float(
            policy.get("min_market_time_to_end_sec") or 43200.0
        )
        if market_end_ts < time.time() + min_time_to_end:
            raise ValueError("replacement market ends too soon")
        if str(candidate.get("market_phase") or "").lower() == "pregame":
            game_start_ts = float(candidate.get("game_start_ts") or 0.0)
            min_sports_lead = float(policy.get("min_sports_lead_sec") or 10800.0)
            if game_start_ts < time.time() + min_sports_lead:
                raise ValueError("replacement sports market starts too soon")

        if str(candidate.get("front_depth_status") or "") != "verified":
            raise ValueError("replacement depth is not verified")
        depth_observed_at = float(candidate.get("front_depth_observed_at") or 0.0)
        max_depth_age = float(policy.get("max_depth_age_sec") or 600.0)
        depth_age = time.time() - depth_observed_at
        if depth_observed_at <= 0 or depth_age < -30 or depth_age > max_depth_age:
            raise ValueError("replacement depth snapshot is stale")
        yes_depth = Decimal(
            str(candidate.get("yes_front_bid_notional_usd") or -1)
        )
        no_depth = Decimal(
            str(candidate.get("no_front_bid_notional_usd") or -1)
        )
        if (
            admission_level != "canary"
            and min(yes_depth, no_depth) < self.min_front_bid_notional_usdc
        ):
            raise ValueError("replacement depth is below account minimum")

        stability = float(candidate.get("stability_score") or 0.0)
        fill_risk = float(candidate.get("fill_risk") or 100.0)
        roi = float(candidate.get("risk_adjusted_daily_roi_pct") or 0.0)
        if admission_level != "canary":
            if stability < float(policy.get("min_stability_score") or 70.0):
                raise ValueError("replacement stability is below minimum")
            if fill_risk >= float(policy.get("max_fill_risk") or 35.0):
                raise ValueError("replacement fill risk is above stable limit")
            if roi < float(
                policy.get("min_risk_adjusted_daily_roi_pct") or 0.1
            ):
                raise ValueError("replacement expected return is below minimum")
        validated = dict(candidate)
        validated["account_admission_level"] = admission_level
        return validated

    def _load_stable_replacement_command(
        self,
        command: Mapping[str, Any],
    ) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        try:
            proposal = json.loads(
                self._stable_rotation_proposal_path.read_text(encoding="utf-8")
            )
        except Exception as exc:
            raise ValueError("stable rotation proposal is unavailable") from exc
        if not isinstance(proposal, dict):
            raise ValueError("stable rotation proposal is invalid")

        account_index = int(command.get("account_index") or 0)
        if account_index != int(self._account_idx):
            raise ValueError("replacement command targets another account")
        replacement_id = str(command.get("replacement_id") or "")
        expected_confirmation = confirmation_for_replacement(replacement_id)
        if str(command.get("confirm") or "") != expected_confirmation:
            raise ValueError("replacement confirmation does not match")
        replacement = find_confirmed_replacement(
            proposal,
            account_index=account_index,
            replacement_id=replacement_id,
        )
        selection = replacement.get("selection")
        if not isinstance(selection, Mapping):
            raise ValueError("replacement selection is invalid")
        target_section = str(
            selection.get("target_config_section") or ""
        ).strip()
        if target_section not in {"markets", "night_markets"}:
            raise ValueError("replacement target section is invalid")
        if str(command.get("target_config_section") or "") != target_section:
            raise ValueError("replacement target section does not match proposal")
        command_generated_at = float(command.get("proposal_generated_at") or 0.0)
        if abs(command_generated_at - float(proposal.get("generated_at") or 0.0)) > 0.001:
            raise ValueError("replacement command references another proposal")

        retire = replacement["retire"]
        add = replacement["add"]
        market = command.get("market")
        if not isinstance(market, Mapping):
            raise ValueError("missing replacement market payload")
        if str(command.get("retire_token_id") or "") != str(
            retire.get("token_id") or ""
        ):
            raise ValueError("retirement token does not match proposal")
        if str(market.get("token_id") or "") != str(add.get("token_id") or ""):
            raise ValueError("replacement token does not match proposal")
        if str(market.get("paired_token_id") or "") != str(
            add.get("paired_token_id") or ""
        ):
            raise ValueError("replacement pair does not match proposal")
        candidate = self._validate_stable_replacement_candidate(
            market,
            proposal.get("policy") or {},
        )
        return proposal, replacement, candidate

    async def _require_no_live_orders_for_tokens(
        self,
        token_ids: tuple[str, ...],
    ) -> None:
        try:
            orders = await self._read_open_orders("runtime_no_live_order_check")
        except Exception as exc:
            raise ValueError("cannot confirm retirement market cancellation") from exc
        if not isinstance(orders, list):
            raise ValueError("retirement market order response is invalid")
        token_set = set(token_ids)
        for order in orders:
            if not isinstance(order, dict) or not _order_is_live(order):
                continue
            asset = str(order.get("asset_id") or order.get("token_id") or "")
            if asset in token_set:
                raise ValueError("retirement market still has a live order")

    def _add_runtime_candidate(
        self,
        market: Mapping[str, Any],
        candidate: Mapping[str, Any],
        *,
        session: str = "day",
        source: str = "dashboard_confirmed",
        lifecycle_stage: str = "",
        risk_override: Optional[str] = None,
        persist: bool,
        notify: bool,
        require_new_persisted_pair: bool = False,
    ) -> bool:
        token_id = str(market.get("token_id") or "").strip()
        paired_token_id = str(market.get("paired_token_id") or "").strip()
        spread = candidate.get("rewards_max_spread")
        if spread is None:
            spread = market.get("max_incentive_spread")
        risk = str(
            risk_override
            or ("low" if float(candidate.get("fill_risk") or 100) < 35 else "mid")
        ).lower()
        return self.add_market_runtime(
            token_id=token_id,
            paired_token_id=paired_token_id,
            spread=spread,
            tick=market.get("price_tick", 0.01),
            min_distance=market.get("min_distance_from_best_bid", 0.01),
            risk=risk,
            session=session,
            source=source,
            game_start_ts=candidate.get("game_start_ts"),
            slug=str(candidate.get("slug") or market.get("slug") or ""),
            question=str(candidate.get("question") or market.get("question") or ""),
            condition_id=str(candidate.get("condition_id") or ""),
            eligibility_managed=True,
            eligibility_base_risk=risk,
            lifecycle_stage=lifecycle_stage,
            persist=persist,
            notify=notify,
            require_new_persisted_pair=require_new_persisted_pair,
        )

    def _runtime_add_from_command(self, market: Dict[str, Any]) -> str:
        if not getattr(self, "_runtime_market_updates_enabled", True):
            raise ValueError(
                "runtime market additions require the multi-account market coordinator"
            )
        token_id = str(market.get("token_id") or "").strip()
        paired_token_id = str(market.get("paired_token_id") or "").strip()
        if not token_id.isdigit() or not paired_token_id.isdigit():
            raise ValueError("invalid token pair")
        if token_id in self.market_cfg or token_id in self._night_market_cfg:
            return "already_configured"

        admission_cfg = self.cfg.get("stable_lp_admission", {}) if isinstance(
            getattr(self, "cfg", None), dict
        ) else {}
        if not isinstance(admission_cfg, dict):
            admission_cfg = {}
        candidate = self._validate_stable_replacement_candidate(
            market,
            admission_cfg,
        )

        added = self._add_runtime_candidate(
            market,
            candidate,
            persist=True,
            notify=True,
        )
        return "added" if added else "already_configured"

    def _stable_replacement_config_entry(
        self,
        market: Mapping[str, Any],
        candidate: Mapping[str, Any],
        *,
        replacement_id: str,
        retire_token_id: str,
    ) -> Dict[str, Any]:
        spread = candidate.get("rewards_max_spread")
        if spread is None:
            spread = market.get("max_incentive_spread")
        risk = "low" if float(candidate.get("fill_risk") or 100) < 35 else "mid"
        return {
            "token_id": str(market.get("token_id") or ""),
            "paired_token_id": str(market.get("paired_token_id") or ""),
            "side": "YES",
            "max_incentive_spread": float(spread),
            "price_tick": float(market.get("price_tick") or 0.01),
            "min_distance_from_best_bid": float(
                market.get("min_distance_from_best_bid") or 0.01
            ),
            "quote_size": float(market.get("quote_size") or 100.0),
            "risk": risk,
            "enabled": True,
            "source": "dashboard_confirmed",
            "eligibility_managed": True,
            "eligibility_base_risk": risk,
            "condition_id": str(candidate.get("condition_id") or ""),
            "slug": str(candidate.get("slug") or market.get("slug") or ""),
            "question": str(
                candidate.get("question") or market.get("question") or ""
            ),
            "game_start_ts": float(candidate.get("game_start_ts") or 0.0),
            "rotation_replacement_id": replacement_id,
            "replaced_token_id": retire_token_id,
        }

    def _stable_rotation_position_is_clear(self, position: float) -> bool:
        """Treat exchange-defined dust as clear while failing closed on unknowns."""
        return 0 <= position <= self._exit_dust_threshold

    @staticmethod
    def _stable_lifecycle_position_is_flat(position: float) -> bool:
        """Require an exact zero before lifecycle removal from the config."""
        try:
            value = float(position)
        except (TypeError, ValueError):
            return False
        return math.isfinite(value) and value == 0.0

    async def _runtime_replace_from_command(
        self,
        command: Mapping[str, Any],
    ) -> str:
        if not getattr(self, "_runtime_market_updates_enabled", True):
            raise ValueError(
                "runtime market replacements require the multi-account market coordinator"
            )
        _proposal, _replacement, candidate = self._load_stable_replacement_command(
            command
        )
        market = command["market"]
        retire_token_id = str(command.get("retire_token_id") or "")
        new_token_id = str(market.get("token_id") or "")
        new_pair_id = str(market.get("paired_token_id") or "")
        target_section = str(command.get("target_config_section") or "")
        target_session = "night" if target_section == "night_markets" else "day"

        new_configured = (
            new_token_id in self.market_cfg
            or new_token_id in self._night_market_cfg
        )
        new_pair_configured = (
            new_pair_id in self.market_cfg
            or new_pair_id in self._night_market_cfg
        )
        old_configured = (
            retire_token_id in self.market_cfg
            or retire_token_id in self._night_market_cfg
        )
        if new_configured and not old_configured:
            return "already_replaced"
        if new_configured or new_pair_configured:
            raise ValueError("replacement market is already configured")
        if not old_configured:
            raise ValueError("retirement market is no longer configured")

        old_cfg = self.market_cfg.get(retire_token_id) or self._night_market_cfg.get(
            retire_token_id
        )
        old_pair_id = str((old_cfg or {}).get("paired_token_id") or "")
        old_tokens = tuple(
            token for token in (retire_token_id, old_pair_id) if token
        )
        if any(token in self._active_exit_orders for token in old_tokens):
            raise ValueError("retirement market has an active exit order")
        pending_unwinds = getattr(self, "_pending_unwinds", {})
        if any(token in pending_unwinds for token in old_tokens):
            raise ValueError("retirement market has a pending unwind")

        try:
            open_orders = await self._read_open_orders("runtime_replace_precheck")
        except Exception as exc:
            raise ValueError("cannot verify retirement market orders") from exc
        if not isinstance(open_orders, list):
            raise ValueError("retirement market order response is invalid")
        for order in open_orders:
            if not isinstance(order, dict) or not _order_is_live(order):
                continue
            asset = str(order.get("asset_id") or order.get("token_id") or "")
            if asset not in old_tokens:
                continue
            if str(order.get("side") or "").upper() != "BUY":
                raise ValueError("retirement market has a live non-BUY order")

        for token in old_tokens:
            position = await self._get_token_position(token)
            if position < 0:
                raise ValueError("retirement market position is unknown")
            if not self._stable_rotation_position_is_clear(position):
                raise ValueError("retirement market still has a position")
            if position > 0:
                log(
                    f"[stable-rotation] token={token[:16]} "
                    f"ignoring_dust={position} threshold={self._exit_dust_threshold}"
                )

        for token in old_tokens:
            self._set_event_state(
                token,
                EVENT_CANCELING,
                f"stable_rotation:{command.get('replacement_id')}",
            )
        added = self._add_runtime_candidate(
            market,
            candidate,
            session=target_session,
            persist=False,
            notify=False,
        )
        if not added:
            for token in old_tokens:
                self._set_event_state(token, EVENT_ACTIVE, "stable_rotation_add_failed")
            raise ValueError("replacement market could not be staged")
        for token in (new_token_id, new_pair_id):
            if token in self.market_cfg or token in self._night_market_cfg:
                self._set_event_state(
                    token,
                    EVENT_CANCELING,
                    f"stable_rotation_staged:{command.get('replacement_id')}",
                )

        cancellation_confirmed = True
        for token in old_tokens:
            if not await self._cancel_token_orders(
                token,
                reason=f"stable_rotation:{command.get('replacement_id')}",
            ):
                cancellation_confirmed = False
        if not cancellation_confirmed:
            self._drop_market_runtime_state(new_token_id)
            self._request_market_ws_resubscribe()
            self.send_discord(
                "市场替换已停止\n"
                f"旧市场：{self._discord_market_name(retire_token_id)}\n"
                "原因：旧挂单撤销未确认；未启用新市场"
            )
            raise ValueError("retirement market cancellation is unconfirmed")

        try:
            await self._require_no_live_orders_for_tokens(old_tokens)
        except Exception:
            self._drop_market_runtime_state(new_token_id)
            self._request_market_ws_resubscribe()
            raise

        for token in old_tokens:
            position = await self._get_token_position(token)
            if not self._stable_rotation_position_is_clear(position):
                self._drop_market_runtime_state(new_token_id)
                self._request_market_ws_resubscribe()
                raise ValueError("retirement position changed during replacement")

        replacement_id = str(command.get("replacement_id") or "")
        new_entry = self._stable_replacement_config_entry(
            market,
            candidate,
            replacement_id=replacement_id,
            retire_token_id=retire_token_id,
        )
        try:
            with self._config_transaction_lock():
                config = json.loads(
                    self._config_path.read_text(encoding="utf-8")
                )
                if not isinstance(config, dict):
                    raise ValueError("account config is invalid")
                drop_ids = set(old_tokens) | {
                    token for token in (new_token_id, new_pair_id) if token
                }
                for section in ("markets", "night_markets"):
                    rows = config.get(section) or []
                    if not isinstance(rows, list):
                        raise ValueError(f"account config {section} is invalid")
                    kept = []
                    for row in rows:
                        if not isinstance(row, Mapping):
                            raise ValueError(
                                f"account config {section} contains invalid market"
                            )
                        if str(row.get("token_id") or "") not in drop_ids:
                            kept.append(row)
                    config[section] = kept
                config.setdefault(target_section, []).append(new_entry)
                self._write_config_atomic(config)
        except Exception:
            self._drop_market_runtime_state(new_token_id)
            for token in old_tokens:
                self._set_event_state(token, EVENT_ACTIVE, "stable_rotation_rollback")
            self._request_market_ws_resubscribe()
            raise

        self._drop_market_runtime_state(retire_token_id)
        for token in (new_token_id, new_pair_id):
            if token in self.market_cfg or token in self._night_market_cfg:
                self._set_event_state(
                    token,
                    EVENT_ACTIVE,
                    f"stable_rotation_complete:{replacement_id}",
                )
        self._request_market_ws_resubscribe()
        log(
            f"[stable-rotation] id={replacement_id[:12]} "
            f"retired={retire_token_id[:16]} added={new_token_id[:16]}"
        )
        self.send_discord(
            "市场替换完成\n"
            f"退出：{self._discord_market_name(retire_token_id)}\n"
            f"加入：{str(candidate.get('question') or candidate.get('slug') or new_token_id)[:120]}\n"
            "选择依据：预期收益率、竞争度与吃单风险"
        )
        return "replaced"

    def _stable_lifecycle_runtime_can_update(self) -> bool:
        """Allow only unambiguous reviewed stable-account runtimes."""

        if not getattr(
            self,
            "_stable_lifecycle_runtime_updates_enabled",
            False,
        ):
            return False
        mode = str(getattr(self, "_runtime_mode", "single") or "single")
        if mode == "multi_roster":
            return True
        if mode != "single":
            return False
        try:
            account_index = int(getattr(self, "_account_idx", 0) or 0)
            routing_count = int(
                getattr(self, "_routing_account_count", 0) or 0
            )
            local_count = int(getattr(self, "_local_account_count", 0) or 0)
        except (TypeError, ValueError):
            return False
        profile_type = str(
            getattr(
                getattr(self, "lp_account_profile", None),
                "profile_type",
                "",
            )
            or ""
        ).strip().lower()
        return bool(
            profile_type == "standard"
            and 1 <= account_index <= 30
            and routing_count == 1
            and local_count == 1
            and getattr(self, "_runtime_market_updates_enabled", False)
            and str(getattr(self, "_runtime_scope", "") or "") in {"", "stable"}
            and re.fullmatch(
                r"[0-9a-f]{16}",
                str(
                    getattr(self, "_stable_lifecycle_account_uid_key", "")
                    or ""
                ).strip().lower(),
            )
            and re.fullmatch(
                r"[a-z0-9][a-z0-9._-]{0,127}",
                str(getattr(self, "_runtime_host_id", "") or "")
                .strip()
                .lower(),
            )
        )

    def _paused_zero_market_startup_rejection(self) -> str:
        """Return why the narrow direct-runtime empty start is not allowed."""

        if not getattr(self, "_allow_paused_zero_markets", False):
            return "direct_runtime_gate_disabled"
        if str(getattr(self, "_runtime_mode", "") or "") != "single":
            return "runtime_mode_not_single"
        try:
            pause_present = self._pause_flag_path.is_file()
        except Exception:
            pause_present = False
        if not pause_present:
            return "account_pause_flag_missing"
        if not self._stable_lifecycle_runtime_can_update():
            return "stable_runtime_identity_or_update_gate_invalid"
        return ""

    async def _runtime_set_stable_lifecycle(
        self,
        command: Mapping[str, Any],
    ) -> Dict[str, Any]:
        """Persist the operator gate and apply it when this process is ready."""

        if self.lp_account_profile.profile_type == "aggressive":
            raise ValueError("stable lifecycle cannot target an aggressive account")
        if not self._stable_lifecycle_runtime_can_update():
            raise ValueError(
                "stable lifecycle requires an unambiguous reviewed stable runtime"
            )

        requested_enabled = command.get("enabled") is True
        proposal: Optional[Dict[str, Any]] = None
        if requested_enabled:
            try:
                raw_proposal = json.loads(
                    self._stable_rotation_proposal_path.read_text(encoding="utf-8")
                )
            except Exception as exc:
                raise ValueError("stable lifecycle proposal is unavailable") from exc
            if not isinstance(raw_proposal, dict):
                raise ValueError("stable lifecycle proposal is invalid")
            proposal = raw_proposal
        validate_stable_lifecycle_command(
            command,
            proposal=proposal,
            expected_account_index=int(self._account_idx),
            expected_account_uid_key=str(
                self._stable_lifecycle_account_uid_key or ""
            ),
            expected_host_id=str(self._runtime_host_id or ""),
        )

        lock = getattr(self, "_stable_lifecycle_action_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._stable_lifecycle_action_lock = lock
        async with lock:
            with self._config_transaction_lock():
                config = json.loads(
                    self._config_path.read_text(encoding="utf-8")
                )
                if not isinstance(config, dict):
                    raise ValueError("account config is invalid")
                lifecycle_config = normalized_lifecycle_config(
                    config.get("stable_market_lifecycle"),
                    enabled=requested_enabled,
                )
                config["stable_market_lifecycle"] = lifecycle_config
                self._write_config_atomic(config)

            self.cfg["stable_market_lifecycle"] = lifecycle_config
            self._stable_market_lifecycle_desired_enabled = requested_enabled
            restart_required = bool(
                requested_enabled
                and not getattr(self, "_order_scoring_observer_enabled", False)
            )
            applied_enabled = bool(requested_enabled and not restart_required)
            self._stable_market_lifecycle_enabled = applied_enabled
            state = dict(getattr(self, "_stable_lifecycle_state", {}) or {})
            state.update(
                {
                    "version": STABLE_LIFECYCLE_STATE_VERSION,
                    "account_index": int(self._account_idx),
                    "account_uid_key": str(
                        self._stable_lifecycle_account_uid_key or ""
                    ),
                    "host_id": str(self._runtime_host_id or ""),
                    "status": (
                        "enabled"
                        if applied_enabled
                        else (
                            "restart_required"
                            if restart_required
                            else "disabled_by_operator"
                        )
                    ),
                    "reason": (
                        "order_scoring_observer_requires_restart"
                        if restart_required
                        else "operator_runtime_command"
                    ),
                    "desired_enabled": requested_enabled,
                    "applied_enabled": applied_enabled,
                    "restart_required": restart_required,
                    "updated_at": time.time(),
                }
            )
            self._stable_lifecycle_state = state
            state_persisted = True
            try:
                self._write_stable_lifecycle_state(state)
            except Exception as exc:
                state_persisted = False
                log(
                    "[stable-lifecycle] control state persist failed: "
                    f"{type(exc).__name__}: {str(exc)[:120]}"
                )

        return {
            "status": (
                "restart_required"
                if restart_required
                else ("enabled" if applied_enabled else "disabled")
            ),
            "desired_enabled": requested_enabled,
            "applied_enabled": applied_enabled,
            "restart_required": restart_required,
            "state_persisted": state_persisted,
        }

    async def runtime_command_loop(self) -> None:
        """Apply dashboard commands without restarting the engine."""
        while self._running:
            try:
                command_paths = sorted(self._runtime_command_dir.glob("*.json"))
                for path in command_paths[:20]:
                    processing = path.with_suffix(".processing")
                    try:
                        path.replace(processing)
                    except FileNotFoundError:
                        continue
                    file_command_id = processing.stem
                    command_id = file_command_id
                    market: Dict[str, Any] = {}
                    try:
                        command = json.loads(processing.read_text(encoding="utf-8"))
                        self._runtime_command_parse_failures.pop(
                            file_command_id,
                            None,
                        )
                        command_id = str(command.get("command_id") or command_id)
                        action = str(command.get("action") or "")
                        if action not in {
                            "add_market",
                            "replace_market",
                            "set_stable_market_lifecycle",
                        }:
                            raise ValueError(f"unsupported action: {action}")
                        if action == "set_stable_market_lifecycle":
                            result_payload = {
                                "ok": True,
                                "action": action,
                                **await self._runtime_set_stable_lifecycle(command),
                            }
                        else:
                            raw_market = command.get("market")
                            if not isinstance(raw_market, dict):
                                raise ValueError("missing market payload")
                            market = raw_market
                            if action == "replace_market":
                                status = await self._runtime_replace_from_command(command)
                            else:
                                status = self._runtime_add_from_command(market)
                            result_payload = {
                                "ok": True,
                                "status": status,
                                "token_id": str(market.get("token_id") or ""),
                            }
                        if action == "replace_market":
                            result_payload["retired_token_id"] = str(
                                command.get("retire_token_id") or ""
                            )
                            result_payload["replacement_id"] = str(
                                command.get("replacement_id") or ""
                            )
                        self._write_runtime_result(
                            command_id,
                            result_payload,
                        )
                        log(
                            f"[runtime-command] id={command_id[:12]} "
                            f"action={action} status={result_payload.get('status')}"
                        )
                    except Exception as exc:
                        if self._defer_runtime_command_parse_error(
                            processing,
                            exc,
                        ):
                            continue
                        error = f"{type(exc).__name__}: {str(exc)[:180]}"
                        self._mark_runtime_pending_failed(
                            str(market.get("token_id") or ""),
                            command_id,
                            error,
                        )
                        self._write_runtime_result(
                            command_id,
                            {
                                "ok": False,
                                "status": "rejected",
                                "error": error,
                            },
                        )
                        log(
                            f"[runtime-command] id={command_id[:12]} "
                            f"rejected={type(exc).__name__}:{str(exc)[:120]}"
                        )
                    finally:
                        processing.unlink(missing_ok=True)
            except Exception as exc:
                log(f"[runtime-command] loop error: {type(exc).__name__}: {exc}")
            await asyncio.sleep(1.0)

    def _set_eligibility_risk(
        self,
        token_id: str,
        risk: str,
        *,
        paired_token_id: str = "",
    ) -> None:
        for tid in (token_id, paired_token_id):
            if not tid:
                continue
            cfg = self.market_cfg.get(tid) or self._night_market_cfg.get(tid)
            if not cfg:
                continue
            cfg["risk"] = risk
            self._market_budget_pct.pop(tid, None)
        self._last_budget_rebalance_ts = 0.0

    async def _hold_aggressive_ineligible_market(
        self,
        token_id: str,
        paired_token_id: str,
        reason: str,
    ) -> None:
        """Cancel both event legs and hold them until slow recovery passes."""
        now = time.time()
        for tid in sorted({token_id, paired_token_id} - {""}):
            state = self._event_state_name(tid)
            if state in {
                EVENT_HALTED_ON_FILL,
                EVENT_EXIT_PENDING,
                EVENT_PENDING_MANUAL_EXIT,
                EVENT_STARTED_BLOCKED,
            }:
                continue
            tracker = self._vol_tracker(tid)
            tracker["watch_enter_ts"] = now
            tracker["recovery_good_samples"] = 0
            tracker["recovery_last_sample_at"] = 0.0
            tracker["recovery_reason"] = reason
            self._set_event_state(tid, EVENT_WATCH, reason)
            cleared = await self._cancel_risk_buys(
                tid,
                f"aggressive_eligibility:{reason}",
            )
            if not cleared:
                log(
                    f"[eligibility] token={tid[:16]} cancellation remains "
                    "unconfirmed after global escalation"
                )

    def _natural_managed_scoring_orders(
        self,
        orders: Iterable[Mapping[str, Any]],
    ) -> list[Mapping[str, Any]]:
        """Return only live maker BUYs created and tracked by this engine."""
        return [
            order
            for order in orders
            if _order_is_live(order)
            and self._order_side(order) == "BUY"
            and self._order_id(order) in self._managed_buy_order_ids
        ]

    @staticmethod
    def _live_order_ids_sha256(order_ids: Iterable[str]) -> str:
        encoded = json.dumps(
            sorted(str(order_id) for order_id in order_ids if str(order_id)),
            ensure_ascii=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    async def _validate_stable_lifecycle_promotion(
        self,
        row: Mapping[str, Any],
    ) -> tuple[bool, str]:
        """Rebind a promotion proposal to the currently live managed BUY set."""

        token_id = str(row.get("token_id") or "").strip()
        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(
            token_id
        )
        paired_token_id = str((cfg or {}).get("paired_token_id") or "").strip()
        if not token_id.isdigit() or not paired_token_id.isdigit():
            return False, "promotion_market_pair_missing"

        try:
            observed_at = float(row.get("scoring_sample_observed_at"))
        except (TypeError, ValueError):
            return False, "promotion_scoring_sample_time_invalid"
        sample_age = time.time() - observed_at
        if (
            not math.isfinite(observed_at)
            or observed_at <= 0
            or sample_age < -30
            or sample_age > MAX_PROMOTION_SCORING_SAMPLE_AGE_SEC
        ):
            return False, "promotion_scoring_sample_stale"

        expected_raw = row.get("scoring_live_order_ids_sha256_by_token")
        if not isinstance(expected_raw, Mapping):
            return False, "promotion_live_order_set_missing"
        expected = {
            str(key): str(value).strip().lower()
            for key, value in expected_raw.items()
        }
        event_tokens = {token_id, paired_token_id}
        if set(expected) != event_tokens or any(
            not re.fullmatch(r"[0-9a-f]{64}", digest)
            for digest in expected.values()
        ):
            return False, "promotion_live_order_set_invalid"

        try:
            orders = await self._read_open_orders("lifecycle_promotion_check")
        except Exception:
            return False, "promotion_live_orders_unavailable"
        if not isinstance(orders, list):
            return False, "promotion_live_orders_invalid"

        current_ids = {tid: [] for tid in event_tokens}
        for order in self._natural_managed_scoring_orders(orders):
            asset = str(order.get("asset_id") or order.get("token_id") or "")
            if asset in current_ids:
                current_ids[asset].append(self._order_id(order))
        if any(not order_ids for order_ids in current_ids.values()):
            return False, "promotion_live_order_pair_missing"
        current = {
            tid: self._live_order_ids_sha256(order_ids)
            for tid, order_ids in current_ids.items()
        }
        if current != expected:
            return False, "promotion_live_order_set_changed"
        return True, "promotion_live_order_set_verified"

    async def order_scoring_observer_loop(self) -> None:
        """Sample official scoring status for natural managed BUY orders only."""
        from py_clob_client_v2.clob_types import OrderScoringParams

        while self._running:
            try:
                orders = await self._get_all_orders_cached()
                natural_orders = self._natural_managed_scoring_orders(orders)

                def _query(order_id: str) -> object:
                    return self.client.is_order_scoring(
                        OrderScoringParams(orderId=order_id)
                    )

                await asyncio.to_thread(
                    self._order_scoring_observer.poll,
                    natural_orders,
                    _query,
                    now=time.time(),
                )
            except Exception as exc:
                log(
                    "[order-scoring-observer] read-only poll failed: "
                    f"{type(exc).__name__}: {str(exc)[:160]}"
                )
            await asyncio.sleep(self._order_scoring_observer_interval_sec)

    def _write_stable_lifecycle_state(self, state: Mapping[str, Any]) -> None:
        payload = dict(state)
        temporary = self._stable_lifecycle_state_path.with_suffix(
            ".json.tmp"
        )
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(self._stable_lifecycle_state_path)
        self._stable_lifecycle_state = payload

    def _stable_lifecycle_managed_tokens(self) -> set[str]:
        return {
            str(token_id)
            for token_id, config in (
                list(self.market_cfg.items())
                + list(self._night_market_cfg.items())
            )
            if not config.get("_dual_side_auto")
        }

    def _stable_lifecycle_managed_stages(self) -> dict[str, str]:
        return {
            str(token_id): str(
                config.get("lifecycle_stage") or "full"
            ).lower()
            for token_id, config in (
                list(self.market_cfg.items())
                + list(self._night_market_cfg.items())
            )
            if not config.get("_dual_side_auto")
        }

    def _promote_stable_lifecycle_market(
        self,
        token_id: str,
        target_risk: str,
    ) -> str:
        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(
            token_id
        )
        if not cfg:
            return "market_missing"
        if cfg.get("_dual_side_auto"):
            return "paired_side_ignored"
        if str(cfg.get("lifecycle_stage") or "full").lower() == "full":
            return "already_full"
        risk = str(target_risk or "mid").lower()
        if risk not in {"low", "mid"}:
            risk = "mid"
        paired_token_id = str(cfg.get("paired_token_id") or "")
        promoted_at = time.time()
        with self._config_transaction_lock():
            config = json.loads(self._config_path.read_text(encoding="utf-8"))
            if not isinstance(config, dict):
                raise ValueError("account config is invalid")
            changed = False
            for section in ("markets", "night_markets"):
                rows = config.get(section) or []
                if not isinstance(rows, list):
                    raise ValueError(f"account config {section} is invalid")
                for row in rows:
                    if not isinstance(row, dict):
                        raise ValueError(
                            f"account config {section} contains invalid market"
                        )
                    if str(row.get("token_id") or "") not in {
                        token_id,
                        paired_token_id,
                    }:
                        continue
                    row["lifecycle_stage"] = "full"
                    row["risk"] = risk
                    row["eligibility_base_risk"] = risk
                    row["lifecycle_promoted_at"] = promoted_at
                    changed = True
            if not changed:
                raise ValueError("managed market is missing from account config")
            self._write_config_atomic(config)
        for tid in (token_id, paired_token_id):
            runtime = self.market_cfg.get(tid) or self._night_market_cfg.get(tid)
            if runtime is None:
                continue
            runtime["lifecycle_stage"] = "full"
            runtime["risk"] = risk
            runtime["base_risk"] = risk
            runtime["lifecycle_promoted_at"] = promoted_at
            self._market_budget_pct.pop(tid, None)
        self._last_budget_rebalance_ts = 0.0
        log(
            f"[stable-lifecycle] promoted token={token_id[:16]} "
            f"risk={risk}"
        )
        self.send_discord(
            "市场已升级为完整挂单\n"
            f"市场：{self._discord_market_name(token_id)}\n"
            f"风险档位：{risk}"
        )
        return "promoted"

    def _mark_stable_lifecycle_retire_pending(
        self,
        token_id: str,
        reason_codes: Iterable[str],
    ) -> None:
        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(
            token_id
        )
        paired_token_id = str((cfg or {}).get("paired_token_id") or "")
        reason_list = list(dict.fromkeys(str(reason) for reason in reason_codes))
        requested_at = time.time()
        with self._config_transaction_lock():
            config = json.loads(self._config_path.read_text(encoding="utf-8"))
            if not isinstance(config, dict):
                raise ValueError("account config is invalid")
            changed = False
            for section in ("markets", "night_markets"):
                rows = config.get(section) or []
                if not isinstance(rows, list):
                    raise ValueError(f"account config {section} is invalid")
                for row in rows:
                    if not isinstance(row, dict):
                        raise ValueError(
                            f"account config {section} contains invalid market"
                        )
                    if str(row.get("token_id") or "") not in {
                        token_id,
                        paired_token_id,
                    }:
                        continue
                    row["lifecycle_retire_pending"] = True
                    row["lifecycle_retire_reason_codes"] = reason_list
                    if not row.get("lifecycle_retire_requested_at"):
                        row["lifecycle_retire_requested_at"] = requested_at
                    changed = True
            if changed:
                self._write_config_atomic(config)
        for tid in (token_id, paired_token_id):
            runtime = self.market_cfg.get(tid) or self._night_market_cfg.get(tid)
            if runtime is not None:
                runtime["lifecycle_retire_pending"] = True

    def _remove_stable_lifecycle_config(
        self,
        token_id: str,
        *,
        preserve_startup_market: bool = False,
    ) -> bool:
        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(
            token_id
        )
        paired_token_id = str((cfg or {}).get("paired_token_id") or "")
        drop_ids = {token_id, paired_token_id} - {""}
        with self._config_transaction_lock():
            config = json.loads(self._config_path.read_text(encoding="utf-8"))
            if not isinstance(config, dict):
                raise ValueError("account config is invalid")
            for section in ("markets", "night_markets"):
                rows = config.get(section) or []
                if not isinstance(rows, list):
                    raise ValueError(f"account config {section} is invalid")
                kept = []
                for row in rows:
                    if not isinstance(row, Mapping):
                        raise ValueError(
                            f"account config {section} contains invalid market"
                        )
                    if str(row.get("token_id") or "") not in drop_ids:
                        kept.append(row)
                config[section] = kept
            if preserve_startup_market:
                loaded_primary_survives = False
                for row in config.get("markets") or []:
                    token = str(row.get("token_id") or "")
                    pair = str(row.get("paired_token_id") or "")
                    runtime = self.market_cfg.get(token)
                    paired_runtime = (
                        self.market_cfg.get(pair)
                        or self._night_market_cfg.get(pair)
                    )
                    if (
                        token.isdigit()
                        and pair.isdigit()
                        and row.get("enabled") is not False
                        and isinstance(runtime, Mapping)
                        and not runtime.get("_dual_side_auto")
                        and str(runtime.get("paired_token_id") or "") == pair
                        and isinstance(paired_runtime, Mapping)
                        and str(paired_runtime.get("paired_token_id") or "")
                        == token
                    ):
                        loaded_primary_survives = True
                        break
                if not loaded_primary_survives:
                    return False
            self._write_config_atomic(config)
        self._drop_market_runtime_state(token_id)
        self._request_market_ws_resubscribe()
        return True

    def _rollback_stable_lifecycle_add(
        self,
        token_id: str,
        *,
        config_before: Mapping[str, Any],
        runtime_tokens_before: Iterable[str],
    ) -> None:
        """Undo this lifecycle add without erasing concurrent config edits."""

        write_error: Optional[Exception] = None
        try:
            with self._config_transaction_lock():
                current = json.loads(
                    self._config_path.read_text(encoding="utf-8")
                )
                if not isinstance(current, dict):
                    raise ValueError("account config is invalid")
                for section in ("markets", "night_markets"):
                    current_rows = current.get(section) or []
                    original_rows = config_before.get(section) or []
                    if not isinstance(current_rows, list) or not isinstance(
                        original_rows,
                        list,
                    ):
                        raise ValueError(
                            f"account config {section} is invalid"
                        )
                    if any(not isinstance(row, Mapping) for row in current_rows):
                        raise ValueError(
                            f"account config {section} contains invalid market"
                        )
                    if any(not isinstance(row, Mapping) for row in original_rows):
                        raise ValueError(
                            f"original config {section} contains invalid market"
                        )
                    original_tokens = {
                        str(row.get("token_id") or "")
                        for row in original_rows
                        if str(row.get("token_id") or "")
                    }
                    kept = [
                        row
                        for row in current_rows
                        if not (
                            str(row.get("token_id") or "") == token_id
                            and str(row.get("source") or "")
                            == "stable_lifecycle_auto"
                            and token_id not in original_tokens
                        )
                    ]
                    current[section] = kept
                self._write_config_atomic(current)
        except Exception as exc:
            write_error = exc
        finally:
            self._drop_market_runtime_state(
                token_id,
                preserve_tokens=runtime_tokens_before,
            )
            self._request_market_ws_resubscribe()
        if write_error is not None:
            raise RuntimeError(
                "stable lifecycle config rollback failed"
            ) from write_error

    async def _retire_stable_lifecycle_market(
        self,
        token_id: str,
        reason_codes: Iterable[str],
    ) -> str:
        cfg = self.market_cfg.get(token_id) or self._night_market_cfg.get(
            token_id
        )
        if not cfg:
            return "already_removed"
        if cfg.get("_dual_side_auto"):
            return "paired_side_ignored"
        paired_token_id = str(cfg.get("paired_token_id") or "")
        paired_cfg = self.market_cfg.get(
            paired_token_id
        ) or self._night_market_cfg.get(paired_token_id)
        if (
            not token_id.isdigit()
            or not paired_token_id.isdigit()
            or paired_token_id == token_id
            or not isinstance(paired_cfg, Mapping)
            or str(paired_cfg.get("paired_token_id") or "") != token_id
        ):
            return "paired_market_invalid"
        event_tokens = (token_id, paired_token_id)
        reason_list = tuple(dict.fromkeys(str(reason) for reason in reason_codes))
        for tid in event_tokens:
            if not self._event_has_unresolved_exit(
                tid,
                ignore_state_tokens=set(event_tokens),
            ):
                self._set_event_state(
                    tid,
                    EVENT_WATCH,
                    "stable_lifecycle_retire_pending",
                )

        cancellation_confirmed = True
        for tid in event_tokens:
            if not await self._cancel_risk_buys(
                tid,
                "stable_lifecycle_retire",
            ):
                cancellation_confirmed = False
        self._mark_stable_lifecycle_retire_pending(token_id, reason_list)
        if not cancellation_confirmed:
            return "buy_cancellation_unconfirmed"

        if any(
            self._event_has_unresolved_exit(
                tid,
                ignore_state_tokens=set(event_tokens),
            )
            for tid in event_tokens
        ):
            return "position_or_exit_pending"
        for tid in event_tokens:
            position = await self._get_token_position(tid)
            if position < 0:
                return "position_unknown"
            if not self._stable_lifecycle_position_is_flat(position):
                return "position_not_flat"

        try:
            open_orders = await self._read_open_orders("lifecycle_retirement_check")
        except Exception:
            return "open_orders_unknown"
        if not isinstance(open_orders, list):
            return "open_orders_unknown"
        for order in open_orders:
            if not isinstance(order, Mapping) or not _order_is_live(order):
                continue
            asset = str(order.get("asset_id") or order.get("token_id") or "")
            if asset in event_tokens:
                return (
                    "buy_cancellation_unconfirmed"
                    if self._order_side(order) == "BUY"
                    else "exit_sell_preserved"
                )

        if not self._remove_stable_lifecycle_config(
            token_id,
            preserve_startup_market=True,
        ):
            return "minimum_market_guard"
        log(
            f"[stable-lifecycle] retired token={token_id[:16]} "
            f"reasons={','.join(reason_list)[:180]}"
        )
        self.send_discord(
            "市场已自动移出\n"
            f"市场：{self._discord_market_name(token_id)}\n"
            f"原因：{', '.join(reason_list)[:220]}"
        )
        return "removed"

    async def _stable_market_lifecycle_once(self) -> None:
        if not self._stable_market_lifecycle_enabled:
            return
        if not self._stable_lifecycle_runtime_can_update():
            return
        lock = getattr(self, "_stable_lifecycle_action_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._stable_lifecycle_action_lock = lock
        async with lock:
            if not self._stable_market_lifecycle_enabled:
                return
            await self._stable_market_lifecycle_apply_once()

    async def _stable_market_lifecycle_apply_once(self) -> None:
        try:
            proposal = json.loads(
                self._stable_rotation_proposal_path.read_text(encoding="utf-8")
            )
        except Exception:
            return
        if not isinstance(proposal, dict):
            return
        runtime_host_id = str(getattr(self, "_runtime_host_id", "") or "").strip().lower()
        account_uid_key = str(
            getattr(self, "_stable_lifecycle_account_uid_key", "") or ""
        ).strip()
        if not runtime_host_id or not account_uid_key:
            self._stable_lifecycle_state = {
                "version": STABLE_LIFECYCLE_STATE_VERSION,
                "account_index": int(self._account_idx),
                "account_uid_key": account_uid_key,
                "host_id": runtime_host_id,
                "status": "blocked",
                "reason": "runtime_identity_unavailable",
                "generated_at": time.time(),
                "add": [],
                "promote": [],
                "retire": [],
            }
            self._write_stable_lifecycle_state(self._stable_lifecycle_state)
            return

        configured_tokens = set(self.market_cfg) | set(self._night_market_cfg)
        lifecycle_available = getattr(self, "_last_balance", None)
        if lifecycle_available is None:
            try:
                lifecycle_available = self._lp_effective_available(
                    await self._get_collateral_available(force_refresh=True)
                )
            except Exception:
                lifecycle_available = None
            if lifecycle_available is not None:
                self._last_balance = lifecycle_available
        canary_budget = self._stable_lifecycle_canary_budget_usdc(
            lifecycle_available
        )
        plan = build_lifecycle_plan(
            proposal,
            account_index=int(self._account_idx),
            configured_token_ids=configured_tokens,
            managed_token_ids=self._stable_lifecycle_managed_tokens(),
            managed_market_stages=self._stable_lifecycle_managed_stages(),
            previous_state=self._stable_lifecycle_state,
            now_ts=time.time(),
            max_proposal_age_sec=self._stable_lifecycle_max_proposal_age_sec,
            max_add_per_cycle=self._stable_lifecycle_max_add_per_cycle,
            max_active_canaries=getattr(
                self,
                "_stable_lifecycle_max_active_canaries",
                10,
            ),
            canary_budget_usdc=(
                float(canary_budget) if canary_budget is not None else None
            ),
            promotion_scoring_threshold=getattr(
                self,
                "_stable_lifecycle_promotion_scoring_threshold",
                3,
            ),
            soft_failure_threshold=(
                self._stable_lifecycle_soft_failure_threshold
            ),
            hard_failure_threshold=(
                self._stable_lifecycle_hard_failure_threshold
            ),
            expected_account_uid_key=account_uid_key,
            expected_host_id=runtime_host_id,
        )
        retire_results = []
        pending_tokens = [
            token_id
            for token_id in sorted(self._stable_lifecycle_managed_tokens())
            if bool(
                (
                    self.market_cfg.get(token_id)
                    or self._night_market_cfg.get(token_id)
                    or {}
                ).get("lifecycle_retire_pending")
            )
        ]
        retire_rows = {
            str(row.get("token_id") or ""): row
            for row in plan.get("retire") or []
            if isinstance(row, Mapping)
        }
        for token_id in pending_tokens:
            retire_rows.setdefault(
                token_id,
                {
                    "token_id": token_id,
                    "reason_codes": ["retirement_already_pending"],
                },
            )
        for token_id, row in retire_rows.items():
            if not token_id:
                continue
            try:
                result = await self._retire_stable_lifecycle_market(
                    token_id,
                    row.get("reason_codes") or [],
                )
            except Exception as exc:
                result = f"rejected:{type(exc).__name__}:{str(exc)[:120]}"
            retire_results.append({"token_id": token_id, "status": result})

        promote_results = []
        for row in plan.get("promote") or []:
            if not isinstance(row, Mapping):
                continue
            token_id = str(row.get("token_id") or "")
            if not token_id:
                continue
            try:
                valid, reason = await self._validate_stable_lifecycle_promotion(
                    row
                )
                if not valid:
                    result = f"rejected:{reason}"
                else:
                    result = self._promote_stable_lifecycle_market(
                        token_id,
                        str(row.get("target_risk") or "mid"),
                    )
            except Exception as exc:
                result = f"rejected:{type(exc).__name__}:{str(exc)[:120]}"
            promote_results.append({"token_id": token_id, "status": result})

        add_results = []
        policy = proposal.get("policy") or {}
        for action in plan.get("add") or []:
            if not isinstance(action, Mapping):
                continue
            market = action.get("market")
            if not isinstance(market, Mapping):
                continue
            stage = str(action.get("stage") or "")
            token_id = str(market.get("token_id") or "")
            try:
                paired_token_id = str(market.get("paired_token_id") or "")
                with self._config_transaction_lock():
                    runtime_tokens_before = set(self.market_cfg) | set(
                        self._night_market_cfg
                    )
                    config_before = json.loads(
                        self._config_path.read_text(encoding="utf-8")
                    )
                    if not isinstance(config_before, dict):
                        raise ValueError("account config is invalid")
                    persisted_tokens_before = set()
                    for section in ("markets", "night_markets"):
                        rows = config_before.get(section) or []
                        if not isinstance(rows, list):
                            raise ValueError(
                                f"account config {section} is invalid"
                            )
                        for row in rows:
                            if not isinstance(row, Mapping):
                                raise ValueError(
                                    f"account config {section} contains invalid market"
                                )
                            persisted_token_id = str(row.get("token_id") or "")
                            if persisted_token_id:
                                persisted_tokens_before.add(persisted_token_id)
                    if {token_id, paired_token_id} & persisted_tokens_before:
                        raise ValueError(
                            "candidate conflicts with existing persisted market"
                        )
                    candidate = self._validate_stable_replacement_candidate(
                        market,
                        policy,
                        allow_canary=True,
                        require_account_execution=True,
                    )
                    current_admission = stable_lifecycle_account_admission(
                        candidate,
                        int(self._account_idx),
                    )
                    if (
                        current_admission is None
                        or current_admission[0] != stage
                    ):
                        raise ValueError("candidate admission changed")
                    try:
                        added = self._add_runtime_candidate(
                            market,
                            candidate,
                            source="stable_lifecycle_auto",
                            lifecycle_stage=stage,
                            risk_override=(
                                "high" if stage == "canary" else None
                            ),
                            persist=True,
                            notify=False,
                            require_new_persisted_pair=True,
                        )
                        if added:
                            configured_after_add = set(self.market_cfg) | set(
                                self._night_market_cfg
                            )
                            if not {token_id, paired_token_id}.issubset(
                                configured_after_add
                            ):
                                raise RuntimeError(
                                    "paired market injection did not complete"
                                )
                    except Exception:
                        self._rollback_stable_lifecycle_add(
                            token_id,
                            config_before=config_before,
                            runtime_tokens_before=runtime_tokens_before,
                        )
                        raise
                if added:
                    try:
                        self.send_discord(
                            "市场已自动加入\n"
                            f"市场：{self._discord_market_name(token_id)}\n"
                            f"阶段：{stage}"
                        )
                    except Exception as notify_exc:
                        log(
                            "[stable-lifecycle] add notification failed: "
                            f"{type(notify_exc).__name__}: {notify_exc}"
                        )
                status = "added" if added else "already_configured"
            except Exception as exc:
                status = f"rejected:{type(exc).__name__}:{str(exc)[:120]}"
            add_results.append({"token_id": token_id, "status": status})

        plan["retire_results"] = retire_results
        plan["promote_results"] = promote_results
        plan["add_results"] = add_results
        plan["active_canaries"] = sum(
            stage == "canary"
            for stage in self._stable_lifecycle_managed_stages().values()
        )
        plan["canary_budget_usdc"] = (
            float(canary_budget) if canary_budget is not None else None
        )
        self._write_stable_lifecycle_state(plan)
        if retire_results or promote_results or add_results:
            log(
                f"[stable-lifecycle] account={self._account_idx} "
                f"add={add_results} promote={promote_results} "
                f"retire={retire_results}"
            )

    async def eligibility_guard_loop(self) -> None:
        """Keep dashboard-added markets useful without creating an all-off cliff.

        Fresh qualifying observations restore the configured budget tier.
        Stable profiles retain and reduce soft failures. Aggressive profiles
        withdraw both event legs after repeated or stale failures, then rely on
        slow recovery before quoting again. Hard market, sponsor and pre-start
        exits remain owned by their dedicated safety guards.
        """
        while self._running:
            try:
                observer_state, candidates, observer_age = self._reward_observer_snapshot()
                now = time.time()
                try:
                    observer_generated_at = (
                        float(observer_state.get("generated_at") or 0.0)
                        if isinstance(observer_state, dict)
                        else 0.0
                    )
                except (TypeError, ValueError):
                    observer_generated_at = 0.0
                for token_id, cfg in list(self.market_cfg.items()) + list(
                    self._night_market_cfg.items()
                ):
                    if not cfg.get("eligibility_managed") or cfg.get(
                        "_dual_side_auto"
                    ):
                        continue
                    candidate = candidates.get(token_id)
                    qualified = bool(
                        observer_age is not None
                        and observer_age <= 900
                        and candidate
                        and candidate.get("verification_recommended") is True
                        and str(candidate.get("market_phase") or "").lower()
                        != "live"
                    )
                    previous = self._eligibility_state.get(token_id, {})
                    sample_changed = bool(
                        (
                            observer_generated_at
                            and observer_generated_at
                            != float(previous.get("observer_generated_at") or 0.0)
                        )
                        or (
                            not observer_generated_at
                            and now - float(previous.get("updated_at") or 0.0)
                            >= self._eligibility_check_interval_sec * 0.9
                        )
                    )
                    previous_failures = int(
                        previous.get("consecutive_failures") or 0
                    )
                    failures = (
                        0
                        if qualified
                        else previous_failures + (1 if sample_changed else 0)
                    )
                    paired = str(cfg.get("paired_token_id") or "")
                    if qualified:
                        status = "qualified"
                        target_risk = str(cfg.get("base_risk") or "mid")
                    elif (
                        self.lp_account_profile.profile_type == "aggressive"
                        and observer_age is not None
                        and observer_age > self._eligibility_stale_after_sec
                    ):
                        status = "withdrawn"
                        target_risk = "high"
                    elif failures < self._eligibility_failure_threshold:
                        status = "watch"
                        target_risk = "high"
                    elif self.lp_account_profile.profile_type == "aggressive":
                        status = "withdrawn"
                        target_risk = "high"
                    else:
                        status = "retained_reduced"
                        target_risk = "high"
                    self._set_eligibility_risk(
                        token_id,
                        target_risk,
                        paired_token_id=paired,
                    )
                    state = {
                        "status": status,
                        "qualified": qualified,
                        "consecutive_failures": failures,
                        "observer_age_sec": round(observer_age, 1)
                        if observer_age is not None
                        else None,
                        "observer_generated_at": observer_generated_at or None,
                        "updated_at": now,
                        "reason": (
                            "eligible"
                            if qualified
                            else (
                                "aggressive eligibility failed; orders held"
                                if status == "withdrawn"
                                else "soft eligibility failed; budget reduced"
                            )
                        ),
                    }
                    self._eligibility_state[token_id] = state
                    if (
                        status == "withdrawn"
                        and previous.get("status") != "withdrawn"
                    ):
                        await self._hold_aggressive_ineligible_market(
                            token_id,
                            paired,
                            "eligibility_withdrawn",
                        )
                    if previous.get("status") != status:
                        log(
                            f"[eligibility] token={token_id[:16]} "
                            f"status={status} failures={failures} risk={target_risk}"
                        )
                await self._stable_market_lifecycle_once()
            except Exception as exc:
                log(f"[eligibility] loop error: {type(exc).__name__}: {exc}")
            await asyncio.sleep(self._eligibility_check_interval_sec)

    def _request_market_ws_resubscribe(self) -> None:
        # Runtime market changes affect both public books and the authenticated
        # fill stream. Keep separate events because either watcher may clear
        # its own event while the other is still reconnecting.
        for attr in (
            "_market_ws_resubscribe_evt",
            "_user_ws_resubscribe_evt",
        ):
            evt = getattr(self, attr, None)
            if evt is not None:
                evt.set()

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

    @staticmethod
    def _distance_score(price: Decimal, midpoint: Decimal, max_spread: Decimal) -> Decimal:
        """Polymarket LP reward distance score: S(v,s) = ((v-s)/v)²
        v = max_incentive_spread, s = distance from midpoint.
        Returns 0-1, higher = closer to midpoint = more reward per share.
        """
        return _shared_distance_score(price, midpoint, max_spread)

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

        # Compute midpoint and spread for distance score
        snap = self._market_snapshots.get(token_id)
        midpoint = (snap.best_bid + snap.best_ask) / Decimal("2") if snap and snap.best_bid > 0 and snap.best_ask > 0 else Decimal("0")
        mcfg = self._get_mcfg(token_id)
        max_spread = mcfg.get("spread", Decimal("0.03"))

        width = max(Decimal("0.000001"), legal_top - reward_lower)
        for idx, p in enumerate(prices):
            front_notional = self._front_notional_from_snapshot(depth_snapshot, p) if depth_snapshot is not None else Decimal("0")

            # S(v,s) = ((v-s)/v)² — LP reward distance score (primary signal)
            ds = self._distance_score(p, midpoint, max_spread) if midpoint > 0 else Decimal("0")

            # Legacy reward zone position score (secondary)
            reward_score = max(Decimal("0"), min(Decimal("1"), (p - reward_lower) / width))

            # Combine: distance score is the dominant factor (weight 0.7)
            # reward_score captures "how far into the zone" (weight 0.3)
            combined = ds * Decimal("0.7") + reward_score * Decimal("0.3")

            distance_penalty = Decimal(idx) * self._level_distance_penalty
            depth_bonus = Decimal("0")
            if front_notional > 0 and self.min_front_bid_notional_usdc > 0:
                depth_bonus = min(self._level_depth_bonus_cap, front_notional / self.min_front_bid_notional_usdc * self._level_depth_bonus_scale)
            vol_penalty = Decimal("0")
            if self._vol_check_bba_jump(
                token_id,
                self._market_snapshots.get(token_id).best_bid
                if self._market_snapshots.get(token_id)
                else p,
                self._market_snapshots.get(token_id).best_ask
                if self._market_snapshots.get(token_id)
                else p,
                update_baseline=False,
            ):
                vol_penalty += self._level_bba_penalty
            if self._vol_check_defense_action_storm(token_id):
                vol_penalty += self._level_defense_storm_penalty
            final_score = combined - distance_penalty + depth_bonus - vol_penalty
            scored.append((p, front_notional, final_score))
        scored.sort(key=lambda x: (x[2], x[0]), reverse=True)
        return scored

    def _adapt_prices_for_front_depth(
        self,
        token_id: str,
        prices: list[Decimal],
        depth_snapshot: Optional[MarketSnapshot],
        *,
        front_depth_floor_usdc: Optional[Decimal] = None,
    ) -> list[tuple[Decimal, Decimal]]:
        if not prices:
            return []
        depth_floor = (
            self.min_front_bid_notional_usdc
            if front_depth_floor_usdc is None
            else max(Decimal("0"), Decimal(str(front_depth_floor_usdc)))
        )
        adapted: list[tuple[Decimal, Decimal]] = []
        for p in prices:
            front_notional = self._front_notional_from_snapshot(depth_snapshot, p) if depth_snapshot is not None else Decimal("0")
            adapted.append((p, front_notional))
        if depth_snapshot is None:
            return adapted
        for idx, (p, front_notional) in enumerate(adapted):
            if front_notional >= depth_floor:
                return adapted[idx:]
        return []

    def _planner_churn_is_locked(self, token_id: str) -> bool:
        until = self._planner_churn_locked_until.get(token_id, 0.0)
        if until <= 0:
            return False
        if time.time() < until:
            return True
        # lock expired — clean up so next churn starts fresh
        self._planner_churn_locked_until.pop(token_id, None)
        self._planner_churn_cancels.pop(token_id, None)
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        log(f"[churn-lock] token={token_id[:16]} slug={slug} lock_expired unlocked")
        return False

    async def _planner_churn_record(self, token_id: str, kind: str) -> None:
        # Record a planner_*_sync cancel; if rate crosses threshold, engage lock.
        now = time.time()
        hist = self._planner_churn_cancels.setdefault(token_id, [])
        hist.append(now)
        cutoff = now - self._planner_churn_window_sec
        self._planner_churn_cancels[token_id] = [t for t in hist if t >= cutoff]
        count = len(self._planner_churn_cancels[token_id])
        if count < self._planner_churn_threshold:
            return
        # Engage lock: cancel all live orders once, then freeze planner for this token.
        self._planner_churn_locked_until[token_id] = now + self._planner_churn_lock_sec
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        log(
            f"[churn-lock] token={token_id[:16]} slug={slug} ENGAGED "
            f"cancels={count}/{self._planner_churn_threshold} window={int(self._planner_churn_window_sec)}s "
            f"lock={int(self._planner_churn_lock_sec)}s last_kind={kind}"
        )
        try:
            live = await self._get_live_orders_fast(token_id)
            ids = [self._order_id(o) for o in live]
            if ids:
                await self._cancel_order_ids(token_id, ids, "churn_lock_engage")
        except Exception as e:
            log(f"[churn-lock] token={token_id[:16]} cancel_all err={e}")
        self.send_discord(
            f"[锁仓] {slug} | planner 频繁调价\n"
            f"{count} 次撤单 / {int(self._planner_churn_window_sec)} 秒\n"
            f"锁 {int(self._planner_churn_lock_sec / 60)} 分钟"
        )

    async def _sync_top_leg(self, token_id: str, desired: Optional[tuple[Decimal, Decimal, Decimal]], live_orders: list[dict]) -> list[dict]:
        if self._planner_churn_is_locked(token_id):
            return live_orders
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
        old_price = self._order_price(current_top) if current_top else None
        if current_top is not None:
            log(f"[cancel] slug={slug} token={token_id[:16]} kind=sync reason=planner_top_leg_sync "
                f"old_price={old_price} ids=1")
            await self._cancel_order_ids(token_id, [self._order_id(current_top)], "planner_top_leg_sync")
            await self._planner_churn_record(token_id, "top_leg")
            if self._planner_churn_is_locked(token_id):
                return await self._get_live_orders_fast(token_id)
            live_orders = await self._get_live_orders_fast(token_id)
        if desired is not None:
            price, size, _ = desired
            await self._place_post_only_order_fast(token_id, price, size, label="top_leg_sync")
            # Force refresh from exchange after placing — cache is stale after mutations
            self._market_live_orders.pop(token_id, None)
            live_orders = await self._refresh_live_orders(token_id)
            if old_price is not None and old_price != price:
                self.send_discord(f"调价\n市场：{slug}\n价格：{old_price} → {price}\n数量：{size}")
            elif old_price is None:
                self.send_discord(f"挂单\n市场：{slug}\n价格：{price}\n数量：{size}")
        self._last_top_plan_sig[token_id] = desired_sig
        return live_orders

    async def _sync_back_legs(self, token_id: str, desired_back: list[tuple[Decimal, Decimal, Decimal]], live_orders: list[dict]) -> list[dict]:
        if self._planner_churn_is_locked(token_id):
            return live_orders
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
        old_back_prices = [self._order_price(o) for o in live_back]
        ids = [self._order_id(o) for o in live_back]
        if ids:
            log(f"[cancel] slug={slug} token={token_id[:16]} kind=sync reason=planner_back_legs_sync ids={len(ids)}")
            await self._cancel_order_ids(token_id, ids, "planner_back_legs_sync")
            await self._planner_churn_record(token_id, "back_legs")
            if self._planner_churn_is_locked(token_id):
                return await self._get_live_orders_fast(token_id)
            live_orders = await self._get_live_orders_fast(token_id)
        for price, size, _ in desired_back:
            await self._place_post_only_order_fast(token_id, price, size, label="back_leg_sync")
        # Force refresh from exchange after placing back legs
        self._market_live_orders.pop(token_id, None)
        live_orders = await self._refresh_live_orders(token_id)
        new_back_prices = [p for p, _, _ in desired_back]
        if old_back_prices != new_back_prices:
            old_str = ",".join(str(p) for p in old_back_prices) if old_back_prices else "-"
            new_str = ",".join(str(p) for p in new_back_prices) if new_back_prices else "-"
            self.send_discord(f"对侧调价\n市场：{slug}\n价格：{old_str} → {new_str}")
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
        if self._is_account_paused():
            return
        if token_id in self._top_leg_defense_active:
            self._top_leg_defense_pending[token_id] = (trigger, snapshot)
            return
        self._top_leg_defense_active.add(token_id)
        current_task = asyncio.current_task()
        if current_task is not None:
            self._top_leg_defense_tasks[token_id] = current_task
        try:
            # Websocket updates can keep scheduling defense after a fill has
            # already preempted this token. Skip that known-closed path before
            # touching snapshots or order APIs; the exception handler below
            # remains for a halt that races with an in-flight defense task.
            if self._halt_preemption_reason(token_id) or self._defense_blocks_requote(token_id):
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
                await self._enter_parent_event_shock_watch(
                    token_id,
                    f"bba_jump:{trigger}",
                    primary_decision=vol_decision,
                )
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
                top_price = self._order_price(top_order)
                gate = self._feasibility_gate(token_id, meta, snap, top_price=top_price)
                front_depth_floor = Decimal(
                    str(
                        gate.get(
                            "front_depth_floor_usdc",
                            self.min_front_bid_notional_usdc,
                        )
                    )
                )
                adapted_legal_prices = self._adapt_prices_for_front_depth(
                    token_id,
                    legal_prices,
                    depth_snapshot,
                    front_depth_floor_usdc=front_depth_floor,
                )
                legal_top = adapted_legal_prices[0][0] if adapted_legal_prices else (legal_prices[0] if legal_prices else None)
                if gate.get("top_leg_action") in {"cancel", "move_back", "halt"} or legal_top is None or (legal_top is not None and top_price > legal_top):
                    if self._coordinated_pair_token(token_id):
                        await self._cancel_coordinated_pair_quotes(
                            token_id,
                            f"top_leg_defense:{trigger}:fast_cancel_locked",
                        )
                        return
                    # Mirror the main defense path: MOVE_BACK gets the longer cooldown.
                    _fast_is_move_back = (
                        gate.get("top_leg_action") == "move_back"
                        or (legal_top is not None and top_price > legal_top
                            and legal_top > 0 and legal_top < best_ask)
                    )
                    block_sec = (self._move_back_requote_block_sec
                                 if _fast_is_move_back
                                 else self._defense_requote_block_sec)
                    self._defense_block_until[token_id] = time.time() + block_sec
                    cleared = await self._cancel_risk_buys(
                        token_id,
                        f"top_leg_defense:{trigger}:fast_cancel_locked",
                    )
                    if not cleared:
                        self._set_event_state(
                            token_id,
                            EVENT_CANCELING,
                            f"risk_cancel_unconfirmed:{trigger}",
                        )
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
                gate = self._feasibility_gate(token_id, meta, snap, top_price=top_price)
                front_depth_floor = Decimal(
                    str(
                        gate.get(
                            "front_depth_floor_usdc",
                            self.min_front_bid_notional_usdc,
                        )
                    )
                )
                adapted_legal_prices = self._adapt_prices_for_front_depth(
                    token_id,
                    legal_prices,
                    depth_snapshot,
                    front_depth_floor_usdc=front_depth_floor,
                )
                legal_top = adapted_legal_prices[0][0] if adapted_legal_prices else (legal_prices[0] if legal_prices else None)
                front_notional = self._front_notional_from_snapshot(depth_snapshot or snap, top_price)

                # --- P0: record front notional for rolling window ---
                self._vol_record_front_notional(token_id, front_notional)

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
                elif action in ("CANCEL_TOP_LEG", "MOVE_BACK_TOP_LEG"):
                    if self._coordinated_pair_token(token_id):
                        await self._cancel_coordinated_pair_quotes(
                            token_id,
                            f"top_leg_defense:{trigger}:{action.lower()}",
                        )
                        self._emit_latency_record(
                            token_id,
                            "top_leg_defense",
                            {"trigger": trigger, "action": action},
                        )
                        return
                    # STRUCTURAL FIX: defense NEVER reposts/requotes.
                    # Both CANCEL and MOVE_BACK only cancel the top order here.
                    # The regular planner loop will repost on its next cycle
                    # after the defense_requote_block cooldown expires.
                    # MOVE_BACK uses a longer cooldown: bid moved down against
                    # us, re-posting at the new lower price within seconds is
                    # what got us filled on 2026-04-24 11:52 (WTA sweep race).
                    block_sec = (self._move_back_requote_block_sec
                                 if action == "MOVE_BACK_TOP_LEG"
                                 else self._defense_requote_block_sec)
                    self._defense_block_until[token_id] = time.time() + block_sec
                    cleared = await self._cancel_risk_buys(
                        token_id,
                        f"{trigger}:cancel_top",
                    )
                    if not cleared:
                        self._set_event_state(
                            token_id,
                            EVENT_CANCELING,
                            f"risk_cancel_unconfirmed:{trigger}",
                        )
                        return
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
            self._top_leg_defense_active.discard(token_id)
            # STRUCTURAL FIX: discard any pending coalesced re-invocations.
            # The regular planner/book loop will handle requoting on its next
            # cycle. Allowing defense to chain-spawn itself from the finally
            # block was the root cause of unbounded task fan-out under
            # high-frequency WS events, leading to RecursionError storms.
            self._top_leg_defense_pending.pop(token_id, None)

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
        # Prefer gameStartTime for sports markets (actual game start, more accurate than endDate)
        keys = ["gameStartTime", "endDate", "end_date", "endTime", "end_time", "expiration", "resolveBy", "endTimestamp", "end_timestamp"]
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
                    parent_tokens = [token_id]
                    if isinstance(ids, list):
                        parent_tokens.extend(str(x) for x in ids)
                    self._remember_parent_event(parent_tokens, raw, nm)
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

    async def get_sponsored_risk(
        self,
        condition_id: str,
        *,
        for_admission: bool = False,
    ) -> Dict[str, Any]:
        """Return the latest sponsor assessment for auto-curator admission."""
        if not self._sponsored_guard.enabled:
            return self._sponsored_guard.assess(
                condition_id,
                for_admission=for_admission,
            )
        await self._sponsored_guard.refresh(
            proxies=self._http_proxies_dict,
        )
        return self._sponsored_guard.assess(
            condition_id,
            for_admission=for_admission,
        )

    async def _resolve_sponsored_condition_ids(
        self,
        token_ids: list[str],
    ) -> None:
        """Resolve missing token -> condition mappings with bounded concurrency."""
        unresolved = [
            tid for tid in token_ids
            if tid not in self._market_condition_ids
            and not self._get_mcfg(tid).get("_dual_side_auto")
        ][:40]
        if not unresolved:
            return
        semaphore = asyncio.Semaphore(8)

        async def _resolve(token_id: str) -> None:
            async with semaphore:
                try:
                    await self._get_market_meta(token_id)
                except Exception:
                    return

        await asyncio.gather(*(_resolve(tid) for tid in unresolved))

    async def _refresh_sponsored_guard_once(
        self,
        *,
        force: bool = False,
        resolve_missing: bool = True,
    ) -> None:
        guard = self._sponsored_guard
        if not guard.enabled:
            self._sponsored_guard_summary = guard.state_payload({})
            return

        await guard.refresh(force=force, proxies=self._http_proxies_dict)
        token_ids = list(
            dict.fromkeys(
                list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())
            )
        )
        if resolve_missing:
            await self._resolve_sponsored_condition_ids(token_ids)

        by_condition: Dict[str, list[str]] = {}
        for token_id in token_ids:
            condition_id = (
                self._market_condition_ids.get(token_id)
                or str(self._get_mcfg(token_id).get("condition_id") or "").strip().lower()
            )
            if not condition_id:
                paired = str(
                    self._get_mcfg(token_id).get("paired_token_id")
                    or self._paired_token_cache.get(token_id)
                    or ""
                )
                condition_id = self._market_condition_ids.get(paired, "")
            if condition_id:
                by_condition.setdefault(condition_id, []).extend(
                    self._event_token_ids(token_id)
                )

        assessments: Dict[str, Dict[str, Any]] = {}
        for condition_id, event_tokens_raw in by_condition.items():
            event_tokens = list(dict.fromkeys(event_tokens_raw))
            assessment = guard.assess(condition_id)
            assessment["token_ids"] = event_tokens
            if not assessment.get("market_slug"):
                for token_id in event_tokens:
                    slug = self._token_slug_cache.get(token_id, "")
                    if slug:
                        assessment["market_slug"] = slug
                        break
            assessments[condition_id] = assessment
            for token_id in event_tokens:
                self._sponsored_guard_by_token[token_id] = assessment

            if assessment.get("status") != "blocked":
                continue
            root_token = event_tokens[0] if event_tokens else ""
            prior_action = self._sponsored_guard_last_action.get(condition_id) or {}
            prior_action_at = float(prior_action.get("at") or 0)
            if (
                not root_token
                or time.time() - prior_action_at
                < self._sponsored_guard.policy.cooldown_sec
            ):
                continue
            reasons = assessment.get("reasons") or ["sponsored_risk"]
            reason = f"sponsored_guard:{'|'.join(str(x) for x in reasons)}"
            await self._deactivate_market(root_token, reason)
            self._sponsored_guard_last_action[condition_id] = {
                "action": "cancel_and_disable_event",
                "reason": reason,
                "at": time.time(),
            }

        self._sponsored_guard_assessments = assessments
        summary = guard.state_payload(assessments)
        summary["last_actions"] = dict(self._sponsored_guard_last_action)
        self._sponsored_guard_summary = summary

    async def sponsored_guard_loop(self) -> None:
        while self._running:
            try:
                await self._refresh_sponsored_guard_once(
                    force=True,
                    resolve_missing=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log(
                    f"[sponsored-guard] refresh error "
                    f"{exc.__class__.__name__}: {exc}"
                )
            await asyncio.sleep(self._sponsored_guard.poll_interval_sec)

    async def _deactivate_market(self, token_id: str, reason: str) -> None:
        event_token_ids = self._event_token_ids(token_id)
        self._event_banned_until[self._event_key(token_id)] = time.time() + self.event_ban_ttl_sec
        for tid in event_token_ids:
            self._market_skip_until[tid] = time.time() + self.event_ban_ttl_sec

        try:
            orders = await self._read_open_orders("market_deactivation_check")
            live = [
                o for o in orders
                if _order_is_live(o)
                and str(o.get("asset_id") or o.get("token_id") or "") in event_token_ids
                and str(o.get("side") or "").upper() != "SELL"
            ]
            ids = [o.get("id") or o.get("orderID") for o in live if (o.get("id") or o.get("orderID"))]
            if ids:
                await self._action_delay(f"health-cancel token={token_id}")
                canceled = await self._execute_exchange_cancel(
                    "cancel_market_deactivation",
                    self.client.cancel_orders,
                    ids,
                )
                if canceled:
                    self._sibling_registry.unregister_many(self._funder_lc, [str(x) for x in ids])
        except Exception as e:
            log(f"[health] cancel fail token={token_id} err={e}")

        # persist disabled in config markets
        try:
            with self._config_transaction_lock():
                cfg = json.loads(
                    self._config_path.read_text(encoding="utf-8")
                )
                for section in ("markets", "night_markets"):
                    for m in cfg.get(section, []):
                        if str(m.get("token_id", "")) in event_token_ids:
                            m["enabled"] = False
                self._write_config_atomic(cfg)
        except Exception as e:
            log(f"[health] config disable fail token={token_id} err={e}")

        # A persisted `enabled=false` only takes effect on the next process
        # start. Remove the event from the current runtime as well so a
        # long-lived process cannot resume it after the temporary ban expires.
        try:
            await self.remove_market_runtime(
                token_id,
                reason=f"deactivated:{reason}",
            )
        except Exception as e:
            log(f"[health] runtime remove fail token={token_id} err={e}")

        slug = self._token_slug_cache.get(token_id, token_id[:16])
        msg = f"Health check failed: {slug}\nReason: {reason}"
        log(f"[health] {msg}")
        self.notify_discord(
            "市场健康检查异常",
            (
                f"市场：{slug}\n"
                f"原因：{self._discord_reason(reason)}\n"
                "系统处理：已停止该市场并撤销相关挂单"
            ),
            "warning",
        )
        self._event_bus.publish("health_fail", {"token_id": token_id, "slug": slug, "reason": reason})

    async def market_health_loop(self) -> None:
        while self._running:
            all_tokens = list(set(list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())))
            for token_id in all_tokens:
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
                    # Near-expiry is definitive — deactivate immediately, don't wait for streak
                    immediate = reason.startswith("near_expiry")
                    if immediate or self._health_fail_streak[token_id] >= self.health_fail_threshold:
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
        # A confirmed fill may arrive after a defensive WATCH/QUARANTINE or
        # cancellation transition. Those states and their ban TTL block new
        # quotes, not inventory reconciliation. Only an existing fill/exit
        # workflow owns the position strongly enough to suppress this signal.
        if self._event_state_name(token_id) in {
            EVENT_HALTED_ON_FILL,
            EVENT_EXIT_PENDING,
            EVENT_PENDING_MANUAL_EXIT,
        }:
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
        matched_size_source: str = "",
    ) -> None:
        log(f"[risk] FILL_HALT token={token_id} reason={reason} size={matched_size} price={matched_price}")
        self.notify_discord("检测到成交", self._format_fill_alert(token_id, reason, matched_size, matched_price), "danger")
        self._event_bus.publish("fill", {
            "token_id": token_id, "reason": reason,
            "size": str(matched_size), "price": str(matched_price),
        })
        # Keep unrelated markets quoting when the account retains the configured
        # collateral floor. Unknown/thin collateral falls back to the original
        # account-wide BUY cancellation before the event-level halt below.
        event_scoped_halt = False
        collateral_available: Optional[Decimal] = None
        if self.runtime_floor_usdc > 0:
            try:
                collateral_available = await self._get_collateral_available(
                    force_refresh=True,
                )
            except Exception as balance_error:
                log(
                    f"[risk] fill balance refresh failed token={token_id} "
                    f"err={balance_error}; using global BUY cancel"
                )
            event_scoped_halt = (
                collateral_available is not None
                and collateral_available >= self.runtime_floor_usdc
            )

        if event_scoped_halt:
            log(
                f"[risk] fill event-scoped halt token={token_id} "
                f"available={collateral_available} "
                f"floor={self.runtime_floor_usdc}; unrelated markets stay live"
            )
        else:
            balance_reason = (
                "disabled"
                if self.runtime_floor_usdc <= 0
                else "unknown"
                if collateral_available is None
                else f"below_floor:{collateral_available}"
            )
            log(
                f"[risk] fill global BUY cancel token={token_id} "
                f"balance={balance_reason} floor={self.runtime_floor_usdc}"
            )
            try:
                await self._cancel_all_except_exit()
            except Exception as _ce:
                log(f"[risk] fill cancel warn: {_ce}")

        # Event-level handling always freezes the filled token and its YES/NO
        # pair. global_cooldown remains reserved for system-level risk.
        halt_kwargs = {
            "matched_size": matched_size,
            "matched_price": matched_price,
            "halt_key": "t_fill_seen",
        }
        if matched_size_source:
            halt_kwargs["matched_size_source"] = matched_size_source
        await self._request_event_halt(
            token_id,
            EVENT_HALTED_ON_FILL,
            reason,
            **halt_kwargs,
        )
        await self._enter_parent_event_shock_watch(
            token_id,
            f"fill:{reason}",
            primary_decision="skip",
        )
        # --- P1: auto exit sell after fill halt ---
        # Keep the exchange-reported matched price as the immutable cost basis.
        # The actual inventory can land on the paired YES/NO token; that
        # conversion happens only after _attempt_exit_sell resolves which token
        # is held. Using this token's current ask here corrupts the paired cost
        # basis (for example 0.53 -> ask 0.56 -> paired 0.44 instead of 0.47).
        fill_price = matched_price if matched_price is not None and matched_price > 0 else Decimal("0")
        fill_size = matched_size if matched_size is not None and matched_size > 0 else Decimal("0")
        if fill_price <= 0:
            log(
                f"[exit] token={token_id} no confirmed fill price; "
                "held-token book will determine the exit quote"
            )
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

    async def _get_collateral_balance(self) -> Decimal:
        """Return wallet collateral balance without treating allowance as equity."""
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            token_id="",
            signature_type=self.signature_type,
        )
        data = await asyncio.to_thread(self.client.get_balance_allowance, params)
        if not isinstance(data, dict):
            raise RuntimeError("invalid collateral response")
        raw = data.get("balance")
        if isinstance(raw, dict):
            raw = raw.get("available") or raw.get("raw") or raw.get("value")
        if raw is None:
            raise RuntimeError("collateral balance missing")
        balance = self._norm_usdc(Decimal(str(raw)))
        if balance is None or not balance.is_finite() or balance < 0:
            raise RuntimeError("invalid collateral balance")
        return balance

    async def _get_total_position_value(self) -> Decimal:
        """Return official Data API mark value for every open account position."""
        funder = self._funder_lc.strip()
        if not re.fullmatch(r"0x[a-f0-9]{40}", funder):
            raise RuntimeError("invalid funder for position value")
        data_api = str(
            self.cfg.get("data_api_url") or "https://data-api.polymarket.com"
        ).rstrip("/")

        def fetch() -> object:
            response = requests.get(
                f"{data_api}/value",
                params={"user": funder},
                timeout=8,
                proxies=self._read_proxies_for_token(),
            )
            response.raise_for_status()
            return response.json()

        payload = await asyncio.to_thread(fetch)
        if not isinstance(payload, list):
            raise RuntimeError("invalid position value response")
        total = Decimal("0")
        matched = False
        for row in payload:
            if not isinstance(row, dict):
                raise RuntimeError("invalid position value row")
            row_user = str(row.get("user") or "").strip().lower()
            if row_user != funder:
                continue
            matched = True
            value = Decimal(str(row.get("value", 0)))
            if not value.is_finite() or value < 0:
                raise RuntimeError("invalid position value")
            total += value
        if payload and not matched:
            raise RuntimeError("position value user mismatch")
        return total

    async def _get_aggressive_equity_snapshot(self) -> tuple[Decimal, Decimal, Decimal]:
        collateral, position_value = await asyncio.gather(
            self._get_collateral_balance(),
            self._get_total_position_value(),
        )
        return collateral + position_value, collateral, position_value

    def _write_aggressive_guardrail_latch(self, reason: str, now: float) -> None:
        payload = {
            "account_index": self._account_idx or 1,
            "account_id": self.lp_account_profile.account_id,
            "reason": reason,
            "triggered_at": now,
        }
        temporary = self._aggressive_guardrail_latch_path.with_name(
            f".{self._aggressive_guardrail_latch_path.name}.tmp"
        )
        temporary.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self._aggressive_guardrail_latch_path)

    async def _trigger_aggressive_guardrail(self, reason: str, now: float) -> None:
        first_trigger = not self._aggressive_guardrail_latch_path.exists()
        self._aggressive_guardrail_state.latch(reason, now)
        self._write_aggressive_guardrail_latch(reason, now)
        self._pause_flag_path.touch(exist_ok=True)
        self._aggressive_guardrail_state.save(self._aggressive_guardrail_state_path)
        canceled = self._aggressive_guardrail_cancel_complete
        if not self._aggressive_guardrail_cancel_complete:
            canceled = await self._cancel_all_except_exit()
            self._aggressive_guardrail_cancel_complete = canceled
        if first_trigger:
            state = self._aggressive_guardrail_state
            self._event_bus.publish(
                "aggressive_guardrail_triggered",
                {
                    "account_index": self._account_idx or 1,
                    "reason": reason,
                    "equity_usdc": state.last_equity_usdc or None,
                    "daily_loss_usdc": state.daily_loss_usdc,
                    "maker_orders_canceled": canceled,
                },
            )
            self.notify_discord(
                "激进 LP 风控已暂停账户",
                (
                    f"原因：{reason}\n"
                    f"总权益：${state.last_equity_usdc or '未知'}\n"
                    f"当日损失：${state.daily_loss_usdc}\n"
                    "做市单已撤销，退出 SELL 保留；需人工复位"
                ),
                "danger",
            )

    async def _reset_aggressive_guardrail(self, *, keep_paused: bool = False) -> bool:
        if not (
            self._aggressive_guardrail_latch_path.exists()
            or self._aggressive_guardrail_state.latched
        ):
            log("[aggressive-guardrail] reset ignored: account is not guard-latched")
            return False
        try:
            equity, collateral, position_value = (
                await self._get_aggressive_equity_snapshot()
            )
        except Exception as exc:
            log(f"[aggressive-guardrail] reset rejected: equity unavailable: {exc}")
            return False
        if equity <= self.lp_account_profile.pause_equity_usdc:
            log(
                "[aggressive-guardrail] reset rejected: "
                f"equity={equity} floor={self.lp_account_profile.pause_equity_usdc}"
            )
            return False
        now = time.time()
        self._aggressive_guardrail_state.reset(
            equity=equity,
            now=now,
            cutoff_hour=self._aggressive_guardrail_cutoff_hour,
            baseline_cap=self.lp_account_profile.target_principal_usdc,
        )
        self._aggressive_guardrail_state.last_collateral_usdc = str(collateral)
        self._aggressive_guardrail_state.last_position_value_usdc = str(position_value)
        self._aggressive_guardrail_state.save(self._aggressive_guardrail_state_path)
        self._aggressive_guardrail_latch_path.unlink(missing_ok=True)
        self._aggressive_guardrail_reset_path.unlink(missing_ok=True)
        self._aggressive_guardrail_reset_paused_path.unlink(missing_ok=True)
        if keep_paused:
            self._pause_flag_path.touch(exist_ok=True)
        else:
            self._pause_flag_path.unlink(missing_ok=True)
        self._aggressive_guardrail_cancel_complete = False
        self._event_bus.publish(
            "aggressive_guardrail_reset",
            {
                "account_index": self._account_idx or 1,
                "equity_usdc": str(equity),
                "paused": keep_paused,
            },
        )
        self.notify_discord(
            "激进 LP 风控已人工复位",
            (
                f"当前总权益：${equity}\n"
                "当日损失基线已重新建立\n"
                f"运行状态：{'继续暂停' if keep_paused else '解除暂停'}"
            ),
            "warning",
        )
        return True

    async def aggressive_guardrail_loop(self) -> None:
        """Enforce isolated aggressive-account equity and daily-loss limits."""
        while self._running:
            try:
                keep_paused = self._aggressive_guardrail_reset_paused_path.exists()
                resume_requested = self._aggressive_guardrail_reset_path.exists()
                if keep_paused or resume_requested:
                    reset = await self._reset_aggressive_guardrail(
                        keep_paused=keep_paused
                    )
                    if not reset:
                        self._aggressive_guardrail_reset_path.unlink(missing_ok=True)
                        self._aggressive_guardrail_reset_paused_path.unlink(
                            missing_ok=True
                        )

                if (
                    self._aggressive_guardrail_latch_path.exists()
                    or self._aggressive_guardrail_state.latched
                ):
                    reason = self._aggressive_guardrail_state.reason or "persisted_latch"
                    await self._trigger_aggressive_guardrail(reason, time.time())
                else:
                    now = time.time()
                    try:
                        equity, collateral, position_value = (
                            await self._get_aggressive_equity_snapshot()
                        )
                        reason = self._aggressive_guardrail_state.observe(
                            equity=equity,
                            collateral=collateral,
                            position_value=position_value,
                            now=now,
                            cutoff_hour=self._aggressive_guardrail_cutoff_hour,
                            baseline_cap=self.lp_account_profile.target_principal_usdc,
                            pause_equity=self.lp_account_profile.pause_equity_usdc,
                            daily_loss_limit=self.lp_account_profile.daily_loss_limit_usdc,
                        )
                    except Exception as exc:
                        log(f"[aggressive-guardrail] equity refresh failed: {exc}")
                        reason = self._aggressive_guardrail_state.observe_failure(
                            now=now,
                            stale_after_sec=self._aggressive_guardrail_stale_after_sec,
                        )
                    self._aggressive_guardrail_state.save(
                        self._aggressive_guardrail_state_path
                    )
                    if reason:
                        await self._trigger_aggressive_guardrail(reason, now)
            except Exception as exc:
                log(f"[aggressive-guardrail] loop error: {exc}")
            await asyncio.sleep(self._aggressive_guardrail_interval_sec)

    async def run(self) -> None:
        # Stamp this engine's account id into the per-task context so every
        # downstream log/notify call carries the [N号] prefix automatically.
        _current_account_idx_ctx.set(self._account_idx)
        self._order_scoring_observer.bind_identity(
            account_uid_key=self._stable_lifecycle_account_uid_key,
            host_id=self._runtime_host_id,
        )
        await self._enforce_paused_zero_market_startup()

        n_markets = len(self._active_market_cfg())
        log(f"[engine] starting with {n_markets} active markets")
        self.notify_discord("做市引擎已启动", f"运行市场：{n_markets} 个", "info")
        self._event_bus.publish("engine_start", {"n_markets": n_markets})

        # Inject paired NO tokens before any task starts (so WS subscribes to them)
        try:
            self._maybe_inject_dual_side_tokens()
        except Exception as e:
            log(f"[dual-side-inject] startup error: {e}")

        try:
            await asyncio.wait_for(
                self._adopt_legacy_live_buy_orders(),
                timeout=15,
            )
        except Exception as e:
            log(f"[managed-orders] startup adoption error: {e}")

        if self._sponsored_guard.enabled:
            try:
                await asyncio.wait_for(
                    self._refresh_sponsored_guard_once(
                        force=True,
                        resolve_missing=True,
                    ),
                    timeout=25,
                )
                _sg = self._sponsored_guard_summary
                _counts = _sg.get("counts") or {}
                log(
                    "[sponsored-guard] startup "
                    f"status={_sg.get('status')} "
                    f"safe={_counts.get('safe', 0)} "
                    f"caution={_counts.get('caution', 0)} "
                    f"blocked={_counts.get('blocked', 0)}"
                )
            except Exception as exc:
                log(
                    f"[sponsored-guard] startup degraded "
                    f"{exc.__class__.__name__}: {exc}"
                )

        if self._aggressive_guardrails_enabled and self._runtime_scope != "aggressive":
            raise RuntimeError(
                "aggressive LP guardrails require runtime_scope=aggressive"
            )

        tasks = [
            asyncio.create_task(self.book_loop(), name="book_loop"),
            asyncio.create_task(self._ws_market_watch(), name="market_ws_watch"),
            asyncio.create_task(self.fill_watch_loop(), name="fill_watch_loop"),
            asyncio.create_task(self.market_health_loop(), name="market_health_loop"),
            asyncio.create_task(self.unwind_tracking_loop(), name="unwind_tracking_loop"),
            asyncio.create_task(self.best_bid_guard_loop(), name="best_bid_guard_loop"),
            asyncio.create_task(
                self.paired_quote_invariant_loop(),
                name="paired_quote_invariant_loop",
            ),
            asyncio.create_task(self.state_write_loop(), name="state_write_loop"),
            asyncio.create_task(self.start_guard_sweep_loop(), name="start_guard_sweep_loop"),
            asyncio.create_task(self.heartbeat_loop(), name="heartbeat_loop"),
            asyncio.create_task(
                self.runtime_command_loop(),
                name="runtime_command_loop",
            ),
            asyncio.create_task(
                self.eligibility_guard_loop(),
                name="eligibility_guard_loop",
            ),
        ]
        if self._aggressive_guardrails_enabled:
            tasks.append(
                asyncio.create_task(
                    self.aggressive_guardrail_loop(),
                    name="aggressive_guardrail_loop",
                )
            )
        if self._position_reconcile_enabled:
            tasks.append(
                asyncio.create_task(
                    self.position_reconcile_loop(),
                    name="position_reconcile_loop",
                )
            )
        if self._order_scoring_observer_enabled:
            tasks.append(
                asyncio.create_task(
                    self.order_scoring_observer_loop(),
                    name="order_scoring_observer_loop",
                )
            )
        if self._sponsored_guard.enabled:
            tasks.append(
                asyncio.create_task(
                    self.sponsored_guard_loop(),
                    name="sponsored_guard_loop",
                )
            )
        if self.hourly_summary:
            tasks.append(asyncio.create_task(self.summary_loop(), name="summary_loop"))

        # Auto-curator: always spawned; run() reads auto_curator.enabled from
        # config.json on each tick so the dashboard toggle takes effect live.
        if not self._runtime_market_updates_enabled:
            log(
                "[engine] auto_curator disabled: multi-account roster "
                "requires coordinated market updates"
            )
        else:
            try:
                try:
                    from .auto_curator import AutoCurator, CURATOR_INTERVAL_SEC
                except ImportError:
                    from auto_curator import AutoCurator, CURATOR_INTERVAL_SEC  # engine launched as top-level script
                ac_cfg = self.cfg.get("auto_curator", {}) or {}
                interval = float(ac_cfg.get("interval_sec", CURATOR_INTERVAL_SEC))
                self._auto_curator = AutoCurator(self, interval_sec=interval)
                tasks.append(asyncio.create_task(self._auto_curator.run(), name="auto_curator"))
                log(f"[engine] auto_curator spawned (interval={interval:.0f}s, live-toggle via config)")
            except Exception as e:
                log(f"[engine] auto_curator spawn err: {e}")

        # Shared book fetcher — only spawn if not already set externally
        # (multi_runner.py sets self._shared_book_cache before engine.run()).
        # Batch POST /books cuts per-token REST requests ~55× and is the
        # primary mitigation for Cloudflare 429 storms on read endpoints.
        if self._shared_book_cache is None:
            try:
                try:
                    from .multi_runner import SharedBookCache, _shared_book_fetcher
                except ImportError:
                    from multi_runner import SharedBookCache, _shared_book_fetcher
                _bf_cache = SharedBookCache(
                    ttl_sec=self._shared_book_cache_ttl_sec,
                    direct_rest_burst=self._shared_book_direct_rest_burst,
                    direct_rest_window_sec=self._shared_book_direct_rest_window_sec,
                )
                self._shared_book_cache = _bf_cache
                tasks.append(asyncio.create_task(
                    _shared_book_fetcher(
                        self,
                        lambda: list({**self.market_cfg, **self._night_market_cfg}.keys()),
                        _bf_cache,
                    ),
                    name="shared_book_fetcher",
                ))
                log(
                    "[engine] shared_book_fetcher spawned "
                    f"(single-account, {len({**self.market_cfg, **self._night_market_cfg})} tokens)"
                )
            except Exception as e:
                log(f"[engine] shared_book_fetcher spawn err: {e}")

        try:
            await asyncio.gather(*tasks)
        finally:
            self._running = False
            for t in tasks:
                t.cancel()
            for watchdog in self._aggressive_pair_submit_watchdogs.values():
                watchdog.cancel()
            self._aggressive_pair_submit_watchdogs.clear()
            self._aggressive_pair_submit_attempts.clear()
            self._paired_single_leg_since.clear()

    def _read_proxies_for_token(self, token_id: str = "") -> Optional[dict]:
        p = _choose_proxy(self.cfg, for_ws=False, shard_key=str(token_id or ""))
        if not p:
            # Per-engine dict (picked up at __init__) beats the legacy global
            return self._http_proxies_dict or HTTP_PROXIES
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
            msg = (
                "网络代理已恢复\n"
                f"检测来源：{source}\n"
                f"当前节点：{self._proxy_failover_last_switch_to or '-'}"
            )
            log(msg)
            self.send_fill_discord(msg)
        self._proxy_failover_observe_until = 0.0
        self._proxy_failover_halt_until = 0.0
        self._proxy_failover_tried_this_round.clear()
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
        if self._proxy_failover_blocked_keywords and _contains_any_ci(node_name, self._proxy_failover_blocked_keywords):
            return False
        bad_until = self._proxy_failover_node_bad_until.get(node_name, 0.0)
        return time.time() >= bad_until

    def _clash_request(self, method: str, path: str, payload: Optional[dict] = None) -> Any:
        if self._proxy_failover_pipe_path:
            return self._clash_request_pipe(method, path, payload)
        url = f"{self._proxy_failover_controller_url}{path}"
        kwargs: dict[str, Any] = {"timeout": 10}
        if payload is not None:
            kwargs["json"] = payload
        resp = requests.request(method, url, **kwargs)
        resp.raise_for_status()
        if resp.content:
            return resp.json()
        return None

    def _clash_request_pipe(self, method: str, path: str, payload: Optional[dict] = None) -> Any:
        # Raw HTTP/1.1 over Windows named pipe (Clash Verge Rev service mode).
        import win32file, pywintypes  # type: ignore

        body_bytes = b""
        headers = ["Host: localhost", "Connection: close", "Accept: application/json"]
        if payload is not None:
            body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers.append("Content-Type: application/json")
            headers.append(f"Content-Length: {len(body_bytes)}")
        else:
            headers.append("Content-Length: 0")
        req = (
            f"{method} {path} HTTP/1.1\r\n"
            + "\r\n".join(headers)
            + "\r\n\r\n"
        ).encode("utf-8") + body_bytes

        h = win32file.CreateFile(
            self._proxy_failover_pipe_path,
            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
            0, None, win32file.OPEN_EXISTING, 0, None,
        )
        try:
            win32file.WriteFile(h, req)
            buf = b""
            while True:
                try:
                    _rc, chunk = win32file.ReadFile(h, 8192)
                    if not chunk:
                        break
                    buf += chunk
                    if len(buf) > 4 * 1024 * 1024:
                        break
                except pywintypes.error as e:
                    if e.winerror in (109, 232):  # ERROR_BROKEN_PIPE / ERROR_NO_DATA
                        break
                    raise
        finally:
            try:
                win32file.CloseHandle(h)
            except Exception:
                pass

        head_end = buf.find(b"\r\n\r\n")
        if head_end < 0:
            raise RuntimeError(f"clash pipe: malformed response, no header terminator (len={len(buf)})")
        head = buf[:head_end].decode("iso-8859-1", "replace")
        body = buf[head_end + 4 :]
        status_line = head.split("\r\n", 1)[0]
        parts = status_line.split(" ", 2)
        if len(parts) < 2 or not parts[1].isdigit():
            raise RuntimeError(f"clash pipe: bad status line: {status_line!r}")
        status = int(parts[1])
        # Mihomo's controller uses Transfer-Encoding: chunked for /proxies and others.
        if "transfer-encoding: chunked" in head.lower():
            body = self._dechunk(body)
        if status >= 400:
            raise RuntimeError(f"clash pipe: HTTP {status} {parts[2] if len(parts) > 2 else ''} body={body[:200]!r}")
        if not body:
            return None
        return json.loads(body.decode("utf-8", "replace"))

    @staticmethod
    def _dechunk(data: bytes) -> bytes:
        out = bytearray()
        i = 0
        n = len(data)
        while i < n:
            crlf = data.find(b"\r\n", i)
            if crlf < 0:
                break
            size_hex = data[i:crlf].split(b";", 1)[0].strip()
            try:
                size = int(size_hex, 16)
            except ValueError:
                break
            i = crlf + 2
            if size == 0:
                break
            if i + size > n:
                break
            out += data[i : i + size]
            i += size + 2  # skip trailing CRLF after chunk
        return bytes(out)

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
            try:
                current, candidates, active_group = await asyncio.to_thread(self._get_clash_proxy_candidates)
            except Exception as e:
                log(f"[PROXY] candidate_fetch_failed reason={reason} err={_format_exc(e)}")
                return
            if not candidates:
                log(f"[PROXY] no_allowed_candidates reason={reason} group={self._proxy_failover_group_name}")
                return
            # Exclude already-tried nodes from the current recovery round.
            untried = [c for c in candidates if c not in self._proxy_failover_tried_this_round]
            if not untried:
                # Round complete — cycled through every candidate and none recovered. Halt.
                tried_count = len(self._proxy_failover_tried_this_round)
                total = len(candidates)
                self._proxy_failover_halt_until = now + self._proxy_failover_switch_window_sec
                log(
                    f"[PROXY-HALT] reason={reason} round_exhausted tried={tried_count}/{total} "
                    f"halt={self._proxy_failover_switch_window_sec:.0f}s"
                )
                self.send_discord(
                    "网络代理切换已暂停\n"
                    f"已轮换全部 {total} 个节点，均未恢复\n"
                    f"暂停时间：{int(self._proxy_failover_switch_window_sec)} 秒"
                )
                return
            if current in untried:
                idx = untried.index(current)
                ordered = untried[idx + 1 :] + untried[:idx]
            else:
                ordered = untried
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
                self._proxy_failover_tried_this_round.add(current)
            self._proxy_failover_tried_this_round.add(target)
            self._proxy_failover_last_switch_from = current
            self._proxy_failover_last_switch_to = target
            self._proxy_failover_last_switch_reason = reason
            self._proxy_failover_last_switch_ts = now
            self._proxy_failover_cursor_node = target
            self._proxy_failover_switch_history.append(now)
            self._proxy_failover_observe_until = now + self._proxy_failover_observe_sec
            self._proxy_failover_reset_counters()
            tried = len(self._proxy_failover_tried_this_round)
            total = len(candidates)
            log(
                f"[PROXY] switch group={active_group} from={current or '-'} to={target} "
                f"reason={reason} observe={self._proxy_failover_observe_sec:.0f}s round={tried}/{total}"
            )
            self.send_fill_discord(
                f"网络代理已切换\n节点：{current or '-'} → {target}\n"
                f"原因：{self._discord_reason(reason)}\n"
                f"观察：{int(self._proxy_failover_observe_sec)} 秒\n"
                f"本轮：{tried}/{total}"
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
        read_proxy = "on" if (self._http_proxies_dict or HTTP_PROXIES) else "off"
        ws_proxy = "on" if (self._ws_proxy or WS_PROXY) else "off"
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

    def _zero_market_startup_state(self) -> Dict[str, Any]:
        hold = bool(
            getattr(self, "_paused_zero_market_startup_hold", False)
        )
        return {
            "allowed": bool(
                getattr(self, "_allow_paused_zero_markets", False)
            ),
            "hold": hold,
            "reason": "paused_zero_market_startup" if hold else None,
        }

    async def _enforce_paused_zero_market_startup(self) -> None:
        """Verify zero BUY liquidity before the empty runtime starts loops."""

        if not getattr(self, "_paused_zero_market_startup_hold", False):
            return
        try:
            canceled = await asyncio.wait_for(
                self._cancel_all_except_exit(),
                timeout=20.0,
            )
        except Exception as exc:
            raise RuntimeError(
                "paused zero-market startup could not verify BUY cancellation"
            ) from exc
        if not canceled:
            raise RuntimeError(
                "paused zero-market startup could not verify BUY cancellation"
            )
        self._was_paused = True
        log(
            "[init] paused zero-market hold verified: maker BUYs absent, "
            "SELL exits preserved"
        )

    def _is_account_paused(self) -> bool:
        """Return True when the dashboard has pause-flagged this account.

        Filesystem check — cheap enough to call every book-loop cycle. The
        flag file is touched by the dashboard and cleared by removing it.
        """
        try:
            pause_present = self._pause_flag_path.exists()
        except Exception:
            pause_present = False
        if getattr(self, "_paused_zero_market_startup_hold", False):
            has_market = bool(
                getattr(self, "market_cfg", {})
                or getattr(self, "_night_market_cfg", {})
            )
            if has_market and pause_present:
                self._paused_zero_market_startup_hold = False
                log(
                    "[init] zero-market hold released while account remains "
                    "pause-flagged"
                )
            else:
                return True
        return pause_present

    def _prime_cycle_snapshots(self, cycle_books: Optional[Mapping[str, Any]]) -> int:
        """Publish one coherent shared-book generation before paired planning."""
        if not cycle_books:
            return 0

        primed = 0
        now = time.time()
        for token_id, cycle_entry in cycle_books.items():
            book = getattr(cycle_entry, "book", cycle_entry)
            observed_ts = float(getattr(cycle_entry, "fetched_at", 0.0) or now)
            if observed_ts <= 0 or (now - observed_ts) > self._market_snapshot_stale_sec:
                continue
            if not book or not getattr(book, "bids", None) or not getattr(book, "asks", None):
                continue
            bids = self._coerce_levels(getattr(book, "bids", None))
            asks = self._coerce_levels(getattr(book, "asks", None))
            bids, asks = self._sort_book_levels(bids, asks)
            best_bid, best_ask = self._best_prices_from_levels(bids, asks)
            if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
                continue
            if self._update_market_snapshot(
                str(token_id),
                best_bid=best_bid,
                best_ask=best_ask,
                bids=bids,
                asks=asks,
                source="shared_batch",
                observed_ts=observed_ts,
            ) is not None:
                primed += 1
        return primed

    def _book_loop_token_groups(self, token_ids: list[str]) -> list[tuple[str, ...]]:
        """Keep coordinated outcomes in the same scheduler slot."""
        configured = set(token_ids)
        grouped: set[str] = set()
        groups: list[tuple[str, ...]] = []
        for token_id in token_ids:
            if token_id in grouped:
                continue
            paired = self._coordinated_pair_token(token_id)
            if paired in configured and paired not in grouped:
                groups.append((token_id, paired))
                grouped.update((token_id, paired))
            else:
                groups.append((token_id,))
                grouped.add(token_id)
        return groups

    def _refresh_group_cycle_books(
        self,
        token_ids: tuple[str, ...],
        fallback: Optional[Mapping[str, Any]],
    ) -> Optional[Mapping[str, Any]]:
        """Read and prime the newest cached books when a scheduler slot starts."""
        if self._shared_book_cache is None:
            return fallback
        snapshot_fn = getattr(self._shared_book_cache, "snapshot", None)
        if not callable(snapshot_fn):
            return fallback
        latest = snapshot_fn(list(token_ids))
        if not latest:
            return fallback
        self._prime_cycle_snapshots(latest)
        return latest

    async def book_loop(self) -> None:
        # A coordinated group can run two outcome tasks at once. Cap group
        # concurrency so the change does not double the configured token load.
        group_concurrency = max(1, self._book_loop_concurrency // 2)
        sem = asyncio.Semaphore(group_concurrency)

        async def _process(token_id: str, cycle_books: Optional[Mapping[str, Any]]) -> None:
            try:
                await self.update_and_quote_market(token_id, cycle_books=cycle_books)
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

        async def _process_group(
            token_group: tuple[str, ...],
            cycle_books: Optional[Mapping[str, Any]],
        ) -> None:
            async with sem:
                group_books = self._refresh_group_cycle_books(token_group, cycle_books)
                await asyncio.gather(
                    *[_process(token_id, group_books) for token_id in token_group]
                )

        while self._running:
            # P2: check for session switch and handle cleanup/gap
            await self._session_switch_cleanup()
            if not self._running:
                break
            # If session switch was blocked (no confirm), skip quoting until confirmed
            if self._session_halted_no_confirm:
                await self._session_switch_cleanup()  # re-check for confirmation
                if self._session_halted_no_confirm:
                    await asyncio.sleep(5)  # poll every 5s
                    continue
            # Dashboard-controlled soft pause. On entry we cancel open orders once
            # (exit SELLs preserved for safety); on exit we fall through to the
            # normal quote path. WS loops, fill watcher and state writer keep running.
            if self._is_account_paused():
                if not self._was_paused:
                    log(f"[pause] account {self._account_idx} paused via dashboard — cancelling open orders")
                    try:
                        canceled = await self._cancel_all_except_exit()
                    except Exception as _e:
                        log(f"[pause] cancel error: {_e}")
                        canceled = False
                    if not canceled:
                        log(f"[pause] account {self._account_idx} cancel not verified — retrying")
                        await asyncio.sleep(3)
                        continue
                    self._was_paused = True
                await asyncio.sleep(3)
                continue
            if self._was_paused:
                cleared = self._clear_stale_active_halt_preemptions(
                    "dashboard_resume"
                )
                log(
                    f"[pause] account {self._account_idx} resumed via dashboard "
                    f"— quoting will restart, cleared_preemptions={cleared}"
                )
                self._was_paused = False
            active_cfg = self._active_market_cfg()
            token_ids = list(active_cfg.keys())
            random.shuffle(token_ids)
            cycle_books: Optional[Mapping[str, Any]] = None
            if self._shared_book_cache is not None:
                snapshot_fn = getattr(self._shared_book_cache, "snapshot", None)
                if callable(snapshot_fn):
                    cycle_books = snapshot_fn(token_ids)
                    self._prime_cycle_snapshots(cycle_books)
            token_groups = self._book_loop_token_groups(token_ids)
            random.shuffle(token_groups)
            await asyncio.gather(
                *[_process_group(token_group, cycle_books) for token_group in token_groups]
            )
            if self._shared_book_cache is not None:
                # multi-account mode: random cycle interval to stagger accounts
                cycle_sleep = random.uniform(self._multi_cycle_sleep_min, self._multi_cycle_sleep_max)
            else:
                cycle_sleep = max(self.requote_interval_ms / 1000.0, 0.1)
            await asyncio.sleep(cycle_sleep)

    def _ws_backoff_with_jitter(self, attempt: int) -> float:
        """Exponential backoff with jitter: base * 2^attempt, capped, plus random jitter."""
        exp = min(self._ws_backoff_base_sec * (2 ** attempt), self._ws_backoff_cap_sec)
        jitter = random.uniform(0, exp * 0.3)
        return exp + jitter

    def _invalidate_all_market_snapshots(self) -> None:
        """Clear all market snapshots to force fresh data on WS reconnect."""
        for token_id in list(self._market_snapshots.keys()):
            snap = self._market_snapshots.get(token_id)
            if snap is not None:
                # Set last_update_ts to 0 so snapshots are treated as stale
                snap.last_update_ts = 0.0
        log(f"[ws-reconnect] invalidated {len(self._market_snapshots)} market snapshots")

    async def _ws_market_watch(self) -> None:
        url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
        # Lazily create the resubscribe event inside the running event loop
        if self._market_ws_resubscribe_evt is None:
            self._market_ws_resubscribe_evt = asyncio.Event()
        consecutive_failures = 0
        while self._running:
            # Rebuild subscription payload each connect so runtime-added tokens are included
            all_token_ids = list(set(list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())))
            if not all_token_ids:
                # A narrowly admitted paused zero-market runtime has nothing
                # valid to subscribe to yet. Poll locally until lifecycle
                # provisioning adds its first market instead of sending an
                # empty WS subscription and generating reconnect noise.
                await asyncio.sleep(1.0)
                continue
            payload = {
                "assets_ids": all_token_ids,
                "type": "market",
                "custom_feature_enabled": True,
            }
            resub_task: Optional[asyncio.Task] = None
            try:
                # Full restart: if too many consecutive failures, log and reset counter
                if consecutive_failures >= self._ws_full_restart_after_n:
                    log(f"[market-ws] {consecutive_failures} consecutive failures — performing full WS restart")
                    self._notify_attention("Market WS full restart", failures=str(consecutive_failures))
                    consecutive_failures = 0
                    # Brief extra pause before full restart
                    await asyncio.sleep(2.0)

                self._market_ws_reconnect_count += 1
                if self._market_ws_reconnect_count > 1:
                    log(f"[market-ws] reconnect attempt #{self._market_ws_reconnect_count} (consecutive_failures={consecutive_failures})")
                    # Invalidate snapshots to avoid stale data during reconnection gap
                    self._invalidate_all_market_snapshots()

                async with websockets.connect(url, proxy=self._ws_proxy, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=8 * 1024 * 1024) as ws:
                    await ws.send(json.dumps(payload))
                    log(f"[market-ws] netpath {_ws_proxy_diag(self._ws_proxy)}")
                    log(f"[market-ws] connected assets={len(payload['assets_ids'])} reconnect_total={self._market_ws_reconnect_count}")
                    consecutive_failures = 0
                    self._last_market_ws_ok_ts = time.time()
                    self._proxy_failover_record_success("market-ws")

                    # Watchdog: close WS when a resubscribe is requested so the outer
                    # loop reconnects with a payload containing the latest token list.
                    self._market_ws_resubscribe_evt.clear()

                    async def _resubscribe_watchdog() -> None:
                        try:
                            await self._market_ws_resubscribe_evt.wait()
                            log(f"[market-ws] resubscribe requested — closing WS to reconnect")
                            await ws.close()
                        except Exception:
                            pass

                    resub_task = asyncio.create_task(_resubscribe_watchdog(), name="market_ws_resubscribe_watch")
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

                                # Cross-side sentinel: feed top-N depth and check
                                # for ASK-depletion on this token; if triggered,
                                # cancel our orders on the PAIRED (opposite) token
                                # because arbitrageurs will cross those next.
                                if self.cross_side_sentinel.enabled:
                                    try:
                                        _N = self.cross_side_sentinel.depth_levels
                                        _ask_d = float(sum(sz for (_px, sz) in asks[:_N]))
                                        _bid_d = float(sum(sz for (_px, sz) in bids[:_N]))
                                        self.cross_side_sentinel.record_depth(token_id, _ask_d, _bid_d)
                                        _trig, _reason, _max_ask, _cur_ask, _pct = self.cross_side_sentinel.should_trigger(token_id)
                                        if _trig:
                                            _paired = self._paired_token_cache.get(token_id) or str(
                                                self.market_cfg.get(token_id, {}).get("paired_token_id", "") or ""
                                            )
                                            if _paired and not self.cross_side_sentinel.in_cooldown(_paired):
                                                _mode = "DRY_RUN" if self.cross_side_sentinel.dry_run else "LIVE"
                                                log(
                                                    f"[cross-side-sentinel] {_mode} TRIGGER trigger_token={token_id[:14]}.. "
                                                    f"paired_to_cancel={_paired[:14]}.. reason={_reason} "
                                                    f"window={self.cross_side_sentinel.depth_window_sec:.0f}s "
                                                    f"top{_N}_ask: max={_max_ask:.0f} cur={_cur_ask:.0f} consumed={_max_ask-_cur_ask:.0f}({_pct:.0%})"
                                                )
                                                if self.cross_side_sentinel.dry_run:
                                                    self.cross_side_sentinel.mark_cancelled(_paired)
                                                elif _paired not in self._cross_side_cancel_inflight:
                                                    self._cross_side_cancel_inflight.add(_paired)
                                                    self._spawn_bg(
                                                        self._execute_cross_side_cancel(
                                                            token_id,
                                                            _paired,
                                                            _reason,
                                                            max_ask=_max_ask,
                                                            current_ask=_cur_ask,
                                                            consumed_pct=_pct,
                                                        ),
                                                        name=f"cross_side_cancel:{_paired}",
                                                    )
                                    except Exception as _ex:
                                        log(f"[cross-side-sentinel] err: {_ex}")
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
                consecutive_failures += 1
                em = _format_exc(e)
                backoff = self._ws_backoff_with_jitter(consecutive_failures)
                log(f"[market-ws] err={em} consecutive_failures={consecutive_failures} backoff={backoff:.1f}s")
                if "opening handshake" in em.lower() and "timed out" in em.lower():
                    self._proxy_failover_record_ws_handshake_failure("market-ws")
                if self._is_req_exc(e):
                    self._log_req_diag("market-ws", e)
                await asyncio.sleep(backoff)
            finally:
                # Cancel the resubscribe watchdog so it doesn't leak between reconnects
                if resub_task is not None and not resub_task.done():
                    resub_task.cancel()

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

    async def update_and_quote_market(
        self,
        token_id: str,
        *,
        cycle_books: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self._ensure_runtime_token_state(token_id, reason="quote_loop")
        mcfg = self._get_mcfg(token_id)
        owner = shared_event_owner(
            self._account_idx or 1,
            token_id,
            mcfg,
            self._shared_account_profiles,
        )
        if owner != (self._account_idx or 1):
            return
        lock = self._event_locks[token_id]
        async with lock:
            now_ts = time.time()
            if now_ts < self._cooldown_until:
                # Exit recovery protection: skip global_cooldown for tokens just resumed from exit
                if now_ts < self._exit_recovery_protection_until.get(token_id, 0.0):
                    pass  # fall through to normal quoting
                elif self._event_state_name(token_id) != EVENT_HALTED_ON_FILL or not self._active_exit_orders:
                    self._set_event_state(token_id, EVENT_COOLDOWN, "global_cooldown")
                    return
                else:
                    return
            if self._require_recovery_gate:
                if not self._recovery_ready():
                    return
                self._require_recovery_gate = False
                self._notify_status("Recovery", action="auto resume quoting")
            self._resume_expired_global_cooldown_markets(
                "global_cooldown_recovered"
            )
            if self._event_is_banned(token_id):
                # --- P0: auto-recover from WATCH/QUARANTINE if timer expired ---
                if self._aggressive_recovery_ready(token_id):
                    prev_state = self._event_state_name(token_id)
                    self._vol_tracker(token_id)["watch_count"] = 0
                    self._vol_tracker(token_id)["defense_repeat_count"] = 0
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
            if self._defense_blocks_requote(token_id):
                return
            if await self._manual_exit_blocks_quote(token_id):
                return

            # # Dual-side gate: auto-injected NO tokens quote alongside YES.
            #    Only gated by: snapshot availability and minimum book depth.
            mcfg = self._get_mcfg(token_id)
            if mcfg.get("_dual_side_auto"):
                paired_tid = mcfg.get("paired_token_id", "")
                my_snap = self._market_snapshots.get(token_id)
                if my_snap is None:
                    return  # No snapshot for this side yet; wait
                my_bid = my_snap.best_bid
                if my_bid <= 0:
                    return  # No valid bid; wait

            now = time.time()
            if (now - self.last_quote_ts[token_id]) * 1000 < self.requote_interval_ms:
                return
            quote_observed_ts: Optional[float] = None
            if cycle_books is not None:
                cycle_entry = cycle_books.get(token_id)
                if cycle_entry is not None and hasattr(cycle_entry, "book"):
                    quote_observed_ts = float(getattr(cycle_entry, "fetched_at", 0.0) or 0.0)
                    if (
                        quote_observed_ts <= 0
                        or (time.time() - quote_observed_ts) > self._market_snapshot_stale_sec
                    ):
                        book = None
                    else:
                        book = cycle_entry.book
                else:
                    book = cycle_entry
            else:
                book = self._shared_book_cache.get(token_id) if self._shared_book_cache is not None else None
            quote_source = "shared_batch" if book is not None else "rest"
            used_ws_fallback = False
            if book is None:
                # Prefer the already-streaming full-depth WS snapshot over
                # multiplying one failed batch into many direct REST calls.
                depth_snap = self._fresh_depth_snapshot(token_id)
                if depth_snap is not None and str(depth_snap.source).startswith("market_ws"):
                    bids = list(depth_snap.bids)
                    asks = list(depth_snap.asks)
                    best_bid = depth_snap.best_bid
                    best_ask = depth_snap.best_ask
                    used_ws_fallback = True
                    quote_source = depth_snap.source
                    quote_observed_ts = depth_snap.last_update_ts
                    note_ws = getattr(self._shared_book_cache, "note_ws_depth_fallback", None)
                    if callable(note_ws):
                        note_ws()
                elif self._shared_book_cache is not None:
                    allow_direct = getattr(self._shared_book_cache, "allow_direct_rest", None)
                    if callable(allow_direct) and not allow_direct():
                        # Fail closed for this cycle. The shared fetcher or WS
                        # will make the market eligible again without a REST storm.
                        return

            if book is None and not used_ws_fallback:
                try:
                    book = await asyncio.to_thread(self.client.get_order_book, token_id)
                    quote_source = "rest"
                    quote_observed_ts = time.time()
                except Exception as e:
                    depth_snap = self._fresh_depth_snapshot(token_id)
                    if depth_snap is not None and str(depth_snap.source).startswith("market_ws"):
                        bids = list(depth_snap.bids)
                        asks = list(depth_snap.asks)
                        best_bid = depth_snap.best_bid
                        best_ask = depth_snap.best_ask
                        used_ws_fallback = True
                        quote_source = depth_snap.source
                        quote_observed_ts = depth_snap.last_update_ts
                        note_ws = getattr(self._shared_book_cache, "note_ws_depth_fallback", None)
                        if callable(note_ws):
                            note_ws()
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
                    await self._request_event_halt(
                        token_id,
                        EVENT_HALTED_ON_DATA,
                        "order_book:empty_or_unparsed",
                        halt_key="t_detect",
                    )
                    return
                bids = self._coerce_levels(getattr(book, "bids", None))
                asks = self._coerce_levels(getattr(book, "asks", None))
                bids, asks = self._sort_book_levels(bids, asks)
                best_bid, best_ask = self._best_prices_from_levels(bids, asks)
                if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
                    await self._request_event_halt(
                        token_id,
                        EVENT_HALTED_ON_DATA,
                        "order_book:crossed_or_invalid",
                        halt_key="t_detect",
                    )
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
                        await self._request_event_halt(
                            token_id,
                            EVENT_HALTED_ON_DATA,
                            "order_book:placeholder_unresolved",
                            halt_key="t_detect",
                        )
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
                observed_ts=quote_observed_ts,
            )
            effective_snapshot = self._effective_snapshot_for_gate(token_id, snapshot)
            if effective_snapshot is not None:
                tob = TopOfBook(best_bid=effective_snapshot.best_bid, best_ask=effective_snapshot.best_ask)
            depth_snapshot = self._trusted_depth_for_snapshot(token_id, effective_snapshot)
            coordinated_paired = self._coordinated_pair_token(token_id)
            can_quote, gate_reason = self._quote_gate(token_id, effective_snapshot)
            if not can_quote:
                live_token = await self._get_live_orders_fast(token_id)
                if coordinated_paired:
                    await self._cancel_coordinated_pair_quotes(
                        token_id,
                        f"quote_gate:{gate_reason}",
                    )
                elif live_token:
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

            # # Paired (both-or-none) gate for <10c markets
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
                            f" — skipping entire event"
                        )
                        await self._cancel_coordinated_pair_quotes(
                            token_id,
                            f"cheap_side_depth:{depth_reason}",
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
                        await self._cancel_coordinated_pair_quotes(
                            token_id,
                            f"paired_preflight:{skip_reason}",
                        )
                        return
                    slug = self._token_slug_cache.get(token_id, token_id[:16])
                    if time.time() - self.last_quote_ts.get(token_id, 0) > 60:
                        side_label = "YES_low" if yes_is_low else "NO_low"
                        log(
                            f"[dual-side-ok] token={slug} paired_token={paired_token[:16]} "
                            f"yes_top={current_top_price} side={side_label} depth={depth_val}"
                        )
            # End paired gate

            if coordinated_paired:
                paired_ready, paired_reason = self._paired_side_ready(
                    token_id,
                    coordinated_paired,
                    prices[0] if prices else best_bid,
                    enforce_all_pairs=True,
                )
                if not paired_ready:
                    await self._cancel_coordinated_pair_quotes(
                        token_id,
                        f"paired_preflight:{paired_reason}",
                    )
                    return

            gate = self._feasibility_gate(token_id, meta, effective_snapshot, top_price=prices[0] if prices else None)
            self._gate_decisions[token_id] = gate
            if not gate.get("can_quote", False):
                live_token = await self._get_live_orders_fast(token_id)
                gate_reason = f"feasibility_gate:{'|'.join(gate.get('reason', []))}"
                if coordinated_paired:
                    await self._cancel_coordinated_pair_quotes(
                        token_id,
                        gate_reason,
                    )
                elif live_token:
                    self._mark_latency(token_id, "t_detect")
                    self._mark_latency(token_id, "t_decision")
                    self._set_event_state(token_id, EVENT_DEFENSIVE, gate_reason)
                    await self._cancel_order_ids(
                        token_id,
                        [self._order_id(o) for o in live_token],
                        gate_reason,
                    )
                if gate.get("top_leg_action") == "halt":
                    await self._request_event_halt(
                        token_id,
                        EVENT_HALTED_ON_DATA,
                        gate_reason,
                        halt_key="t_detect",
                    )
                return
            market_risk = str(self._get_mcfg(token_id).get("risk", "mid")).lower()
            required_min_size = max(self.min_order_size, reward_min_size)
            size_cap = Decimal(str(gate.get("size_cap", 1.0) or 0.0))
            pct = self._market_quote_budget_pct(token_id, market_risk)
            reward_lower = max(self._get_mcfg(token_id)["tick"], tob.mid - (live_spread if live_spread is not None else self._get_mcfg(token_id)["spread"]))
            legal_top = tob.best_bid - self._get_mcfg(token_id)["tick"]
            front_depth_floor = Decimal(
                str(
                    gate.get(
                        "front_depth_floor_usdc",
                        self.min_front_bid_notional_usdc,
                    )
                )
            )
            adapted_prices = self._adapt_prices_for_front_depth(
                token_id,
                prices,
                depth_snapshot,
                front_depth_floor_usdc=front_depth_floor,
            )
            requested_legs_raw = len(prices)
            scored_levels = self._score_price_levels(
                token_id,
                [p for p, _ in adapted_prices],
                depth_snapshot,
                reward_lower,
                legal_top,
            )
            filtered_levels = [
                (p, front_notional, score)
                for p, front_notional, score in scored_levels
                if score > Decimal("0")
            ]
            if not filtered_levels:
                filtered_levels = scored_levels
            raw_weights = self._alloc_weights(len(filtered_levels))
            viable_legs = []
            total_weight = Decimal("0")
            for (p, front_notional, score), w in zip(filtered_levels, raw_weights):
                if front_notional < front_depth_floor:
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
                if coordinated_paired:
                    await self._cancel_coordinated_pair_quotes(
                        token_id,
                        "empty_plan_after_gate",
                    )
                elif live_token:
                    await self._cancel_order_ids(token_id, [self._order_id(o) for o in live_token], "empty_plan")
                self._last_plan_sig[token_id] = ""
                self._last_top_plan_sig[token_id] = ""
                self._last_back_plan_sig[token_id] = ""
                return
            # # SHARE-BASED PLANNER — Q_min optimized
            # LP rewards only count share quantities, not USD notional.
            # target_shares = max(floor(balance), rewardsMinSize)
            # Same event YES+NO share the collateral: 1 share = $1.
            min_size_needed = max(required_min_size, Decimal("0.001"))
            target_bid, target_ask, share_warning = await self._compute_target_shares(
                token_id,
                budget_pct=pct,
                size_cap=size_cap,
            )
            if target_bid <= 0:
                log(
                    f"[quote-cancel] token={token_id} "
                    f"reason=minimum_quote_infeasible warning={share_warning}"
                )
                await self._cancel_infeasible_quote_target(
                    token_id,
                    coordinated_paired,
                    gate,
                    share_warning,
                )
                return

            avail = await self._get_collateral_available()
            if avail is not None:
                self._last_balance = avail
            if avail is None or avail <= 0:
                log(
                    f"[quote-skip] token={token_id} reason=no_balance_available "
                    f"event_state={self._event_state_name(token_id)} pct={pct} size_cap={size_cap}"
                )
                return

            # For dual-side markets, collateral is shares × $1 (not shares × price).
            # The notional check below uses price-based notional which overstates
            # the actual capital needed for high-price sides. Use a collateral-aware
            # available amount so the planner doesn't over-allocate.
            is_dual = bool(mcfg.get("paired_token_id") or mcfg.get("_dual_side_auto"))
            avail_for_plan = avail

            # Warn once if balance insufficient for dual-side minimum
            if share_warning and token_id not in self._dual_side_insufficient_warned:
                self._dual_side_insufficient_warned.add(token_id)
                slug = self._token_slug_cache.get(token_id, token_id[:16])
                self.send_discord(f"[资金不足双边] {slug} | {share_warning}")
                log(f"[dual-side-warn] {slug} {share_warning}")

            # # Step 1: share-based plan generation
            plan = []
            requested_legs = requested_legs_raw if requested_legs_raw > 0 else len(viable_legs)
            planned_legs = 0
            degrade_reason = ""
            top_price = viable_legs[0][0] if viable_legs else Decimal("0")

            # Distribute target_bid shares across legs by weight
            total_weight = sum(w for _, w in viable_legs)
            if total_weight > 0 and top_price > 0:
                for keep_count in range(len(viable_legs), 0, -1):
                    subset = viable_legs[:keep_count]
                    subset_weight = sum(w for _, w in subset)
                    if subset_weight <= 0:
                        continue
                    candidate = []
                    remaining_shares = target_bid
                    remaining_weight = subset_weight
                    for idx, (p, w) in enumerate(subset):
                        if p <= 0 or remaining_shares <= 0 or remaining_weight <= 0:
                            break
                        normalized_weight = w / remaining_weight
                        leg_shares = self._floor_to_tick(remaining_shares * normalized_weight, Decimal("0.001"))
                        if self.max_notional_usdc_per_order > 0 and p > 0:
                            max_leg_shares = self._floor_to_tick(
                                self.max_notional_usdc_per_order / p,
                                Decimal("0.001"),
                            )
                            leg_shares = min(leg_shares, max_leg_shares)
                        notional = p * leg_shares
                        if leg_shares < required_min_size or leg_shares <= 0:
                            if idx == 0:
                                candidate = []
                            break
                        candidate.append((p, leg_shares, notional))
                        remaining_shares -= leg_shares
                        remaining_weight -= w
                    if candidate:
                        # Verify total doesn't exceed available balance
                        plan_total_notional = sum(n for _, _, n in candidate)
                        # For dual-side: collateral = shares × $1, not price-based notional
                        if is_dual:
                            plan_total_shares = sum(s for _, s, _ in candidate)
                            cost_ok = plan_total_shares <= avail
                        else:
                            cost_ok = plan_total_notional <= avail
                        if cost_ok:
                            plan = candidate
                            planned_legs = len(candidate)
                            if planned_legs < requested_legs:
                                degrade_reason = f"share_limited_degrade requested={requested_legs} planned={planned_legs}"
                            break

            # Fallback: single top leg at minimum size
            if not plan and top_price > 0:
                fallback_size = max(min_size_needed, required_min_size)
                if self.max_notional_usdc_per_order > 0:
                    fallback_size = min(
                        fallback_size,
                        self._floor_to_tick(
                            self.max_notional_usdc_per_order / top_price,
                            Decimal("0.001"),
                        ),
                    )
                fallback_notional = top_price * fallback_size
                fallback_cost_ok = (fallback_size <= avail) if is_dual else (fallback_notional <= avail)
                if fallback_cost_ok and fallback_size >= required_min_size:
                    plan = [(top_price, fallback_size, fallback_notional)]
                    planned_legs = 1
                    degrade_reason = "single_leg_fallback"

            # # Step 2: comprehensive plan log
            final_planned_notional = sum(n for _, _, n in plan) if plan else Decimal("0")
            slug = self._token_slug_cache.get(token_id, token_id[:16])
            if plan and degrade_reason:
                levels = ",".join([f"{p}:{s}" for p, s, _ in plan[:8]])
                log(f"[plan] slug={slug} token={token_id[:16]} levels={levels} planned_legs={planned_legs} final_notional={final_planned_notional} target_shares={target_bid} {degrade_reason}".strip())

            if not plan:
                log(
                    f"[quote-skip] token={token_id} reason=no_viable_plan "
                    f"event_state={self._event_state_name(token_id)} avail={avail} "
                    f"target_shares={target_bid} "
                    f"required_min_size={required_min_size} top_price={top_price} "
                    f"requested_legs={requested_legs}"
                )
                self._market_budget_skip_until[token_id] = time.time() + self.budget_skip_cooldown_sec
                if coordinated_paired:
                    self._market_budget_skip_until[coordinated_paired] = max(
                        float(
                            self._market_budget_skip_until.get(
                                coordinated_paired,
                                0.0,
                            )
                        ),
                        self._market_budget_skip_until[token_id],
                    )
                    await self._cancel_coordinated_pair_quotes(
                        token_id,
                        "no_viable_plan",
                    )
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
                # race condition — it fetches a fresh book milliseconds after placing,
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

    # ---- 施工包04:跨账号自成交防线(下单前单点检查) -------------------------

    def _sibling_tick(self, token_id: str) -> Decimal:
        try:
            return Decimal(str(self._get_mcfg(token_id)["tick"]))
        except Exception:
            return Decimal("0.01")

    def _sibling_buy_floor(self, token_id: str) -> Optional[Decimal]:
        """adjust 模式的买单退让下限:复用 preflight 的 reward_lower 口径
        (mid − spread,快照不可得则返回 None → 调用方按 skip 处理)。"""
        try:
            snap = self._market_snapshots.get(token_id)
            effective = self._effective_snapshot_for_gate(token_id, snap)
            if not effective or self._snapshot_is_stale(token_id, effective):
                return None
            cfg = self._get_mcfg(token_id)
            spread = cfg["spread"]
            if spread > Decimal("1"):
                spread = spread / Decimal("100")
            mid = (effective.best_bid + effective.best_ask) / Decimal("2")
            return max(self._sibling_tick(token_id), mid - spread)
        except Exception:
            return None

    def _sibling_gate(self, token_id: str, side: str, price: Decimal, label: str) -> Decimal:
        """下单前交叉检查。返回(可能退让后的)价格;block/退让越限 → 抛 SoftQuoteSkip。
        只加防线,不改报价/exit 定价逻辑;observe 模式零干预。"""
        registry = getattr(self, "_sibling_registry", None)
        scfg = getattr(self, "_sibling_cfg", None) or {}
        if registry is None or not scfg.get("enabled", True):
            return price
        # §2.3 v1:配对 token 互补 BUY 只观察不拦截
        if side == "BUY":
            paired = str(self._paired_token_cache.get(token_id, "") or "")
            if not paired:
                try:
                    paired = str(self._get_mcfg(token_id).get("paired_token_id", "") or "")
                except Exception:
                    paired = ""
            if paired and paired != token_id:
                matched, comp_hit = registry.complement_would_match(
                    self._funder_lc, paired, float(price))
                if matched:
                    log(f"[sibling_complement_observe] token={token_id[:16]} "
                        f"my_buy={price} hit={comp_hit}")
        crossed, hit = registry.would_cross(self._funder_lc, token_id, side, float(price))
        if not crossed:
            return price
        mode = str(scfg.get("mode", "observe"))
        log(f"[sibling_conflict] mode={mode} side={side} token={token_id[:16]} "
            f"my_funder={self._funder_lc[:10]} my_price={price} hit={hit} label={label}")
        if mode == "observe":
            return price
        floor = self._sibling_buy_floor(token_id) if side == "BUY" else None
        if mode == "adjust" and side == "BUY" and floor is None:
            # 快照不可得 → 无法核对报价下限,保守跳过(等同 block)
            registry.note_skipped()
            log(f"[sibling_skip] token={token_id[:16]} side={side} price={price} "
                f"label={label} reason=no_floor_snapshot")
            raise SoftQuoteSkip(f"sibling_conflict_no_floor token={token_id[:16]} label={label}")
        action, new_price = resolve_conflict(
            mode, side, float(price), float(self._sibling_tick(token_id)),
            adjust_ticks=int(scfg.get("adjust_ticks", 1)),
            floor=float(floor) if floor is not None else None)
        if action == "adjust" and new_price is not None:
            registry.note_adjusted()
            adjusted = Decimal(str(new_price))
            log(f"[sibling_adjust] token={token_id[:16]} side={side} {price} -> {adjusted} label={label}")
            return adjusted
        registry.note_skipped()
        log(f"[sibling_skip] token={token_id[:16]} side={side} price={price} label={label}")
        raise SoftQuoteSkip(f"sibling_conflict token={token_id[:16]} side={side} label={label}")

    def _sibling_register_resp(self, token_id: str, side: str, price: Decimal,
                               size: Decimal, resp: Any) -> None:
        registry = getattr(self, "_sibling_registry", None)
        if registry is None:
            return
        try:
            oid = str((resp or {}).get("orderID") or (resp or {}).get("id") or "") \
                if isinstance(resp, dict) else str(getattr(resp, "orderID", "") or getattr(resp, "id", "") or "")
        except Exception:
            oid = ""
        if oid:
            if str(side).upper() == "BUY":
                self._track_managed_buy_order(oid)
            registry.register(self._funder_lc, token_id, side, float(price), float(size), oid)

    async def _submit_post_order(self, token_id: str, price: Decimal, size: Decimal, label: str) -> Any:
        self._ensure_order_path_open(token_id, f"submit_pre_sign:{label}")
        price = self._sibling_gate(token_id, "BUY", price, label)
        maintenance_claim = self._begin_exchange_buy_attempt(token_id, label)
        reserve_id: Optional[str] = None
        post_attempted = False
        try:
            reserve_id = await self._acquire_budget_reserve(token_id, price, size, label)
            self._mark_latency(token_id, "t_send")
            args = OrderArgs(token_id=token_id, price=float(price), size=float(size), side=BUY)
            if self.remote_signer:
                try:
                    signed = await asyncio.to_thread(self.remote_signer.sign_order, token_id, float(price), float(size), "BUY")
                    self._mark_signer_recovered()
                except Exception as e:
                    await self._handle_signer_failure(token_id, e, label)
                    raise
                if isinstance(signed, dict):
                    from py_clob_client_v2.order_utils.model.order_data_v2 import SignedOrderV2
                    signed = SignedOrderV2(**signed)
            else:
                signed = await asyncio.to_thread(self.client.create_order, args)
            # State may change while remote signing is in flight. Re-check at
            # the last possible point so an exit transition or a moving book
            # cannot leak a new maker BUY into the same YES/NO event.
            self._ensure_order_path_open(token_id, f"submit_pre_post:{label}")
            await self._validate_passive_buy_quote(
                token_id,
                price,
                f"submit_final:{label}",
            )
            post_attempted = True
            resp = await asyncio.to_thread(
                self.client.post_order,
                signed,
                OrderType.GTC,
                post_only=True,
            )
            self._invalidate_all_orders_cache()
            if maintenance_claim.startswith("recovery_probe:"):
                candidate_order_id = (
                    self._order_id(resp) if isinstance(resp, dict) else ""
                )
                if candidate_order_id:
                    # If the venue matched before the cancel proof completes,
                    # the normal fill/exit paths must still recognize it as an
                    # engine-managed BUY.
                    self._track_managed_buy_order(candidate_order_id)
                recovery_order_id = self._recovery_post_order_id(resp)
                if not recovery_order_id:
                    self._force_exchange_maintenance(
                        "recovery_probe_receipt_invalid",
                        "recovery_probe_post_verify",
                        immediate_cancel=True,
                    )
                    raise SoftQuoteSkip(
                        "exchange_recovery_probe_receipt_invalid"
                    )
                if not await self._verify_exchange_recovery_probe(
                    token_id,
                    recovery_order_id,
                ):
                    raise SoftQuoteSkip(
                        "exchange_recovery_probe_cancel_unverified"
                    )
                self._finish_exchange_buy_attempt(
                    maintenance_claim,
                    success=True,
                )
                return resp
            self._sibling_register_resp(token_id, "BUY", price, size, resp)
            try:
                await self._refresh_live_orders(token_id)
            except Exception as refresh_exc:
                log(f"[budget-reserve] token={token_id[:16]} live refresh err: {refresh_exc}")
            self._finish_exchange_buy_attempt(maintenance_claim, success=True)
            return resp
        except asyncio.CancelledError:
            if (
                maintenance_claim.startswith("recovery_probe:")
                and post_attempted
                and self._exchange_maintenance_guard().phase
                != PHASE_MAINTENANCE
            ):
                self._force_exchange_maintenance(
                    "recovery_probe_post_ambiguous",
                    "recovery_probe_post_interrupted",
                    immediate_cancel=True,
                )
            self._finish_exchange_buy_attempt(maintenance_claim, success=False)
            raise
        except Exception as exc:
            self._record_exchange_maintenance_error(exc, "post_only_buy")
            if (
                maintenance_claim.startswith("recovery_probe:")
                and post_attempted
                and self._exchange_maintenance_guard().phase
                != PHASE_MAINTENANCE
            ):
                self._force_exchange_maintenance(
                    "recovery_probe_post_ambiguous",
                    "recovery_probe_post_no_receipt",
                    immediate_cancel=True,
                )
            self._finish_exchange_buy_attempt(maintenance_claim, success=False)
            raise
        finally:
            if reserve_id is not None:
                await self._release_budget_reserve(reserve_id)

    async def _validate_passive_buy_quote(
        self,
        token_id: str,
        price: Decimal,
        label: str,
    ) -> None:
        self._ensure_order_path_open(token_id, f"passive_quote:{label}")
        meta = await self._get_market_meta(token_id)
        if await self._enforce_start_guard(
            token_id,
            meta=meta,
            trigger=f"passive_quote:{label}",
        ):
            raise RuntimeError(f"market_start_blocked token={token_id}")
        self._ensure_order_path_open(token_id, f"passive_quote_post_start:{label}")

        snap = self._market_snapshots.get(token_id)
        gate_ok, gate_reason = self._quote_gate(token_id, snap)
        effective = self._effective_snapshot_for_gate(token_id, snap)
        slug = self._token_slug_cache.get(token_id, token_id[:16])
        if not gate_ok or effective is None:
            raise SoftQuoteSkip(
                f"{gate_reason}_snapshot token={token_id[:16]} label={label}"
            )

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
        if price > legal_top and legal_top > 0:
            log(f"[safety] REJECT price>{legal_top} slug={slug} token={token_id[:16]} target={price} legal_top={legal_top} bid={fresh_bid} ask={fresh_ask} spread={spread} reward_lower={reward_lower} label={label}")
            raise RuntimeError(f"pre_order_reject:price_above_legal_top token={token_id[:16]}")
        if price < reward_lower:
            log(f"[safety] REJECT price<reward_lower slug={slug} token={token_id[:16]} target={price} reward_lower={reward_lower} bid={fresh_bid} ask={fresh_ask} spread={spread} mid={mid} label={label}")
            raise RuntimeError(f"pre_order_reject:price_below_reward_zone token={token_id[:16]}")
        if price >= fresh_ask:
            log(f"[safety] REJECT price>=ask slug={slug} token={token_id[:16]} target={price} ask={fresh_ask} bid={fresh_bid} label={label}")
            raise RuntimeError(f"pre_order_reject:price_crosses_spread token={token_id[:16]}")

    async def _preflight_post_order(
        self,
        token_id: str,
        price: Decimal,
        label: str,
    ) -> Optional[tuple[str, int, str]]:
        await self._validate_passive_buy_quote(
            token_id,
            price,
            f"pre_throttle:{label}",
        )
        claim = await self._acquire_order_throttle(token_id, label)
        try:
            await self._validate_passive_buy_quote(
                token_id,
                price,
                f"post_throttle:{label}",
            )
        except Exception as exc:
            await self._aggressive_pair_submit_failed(
                token_id,
                claim,
                f"post_throttle:{label}:{exc.__class__.__name__}",
            )
            raise
        return claim

    async def _place_post_only_order_fast(self, token_id: str, price: Decimal, size: Decimal, label: str = "post_fast") -> Any:
        """Fast repost path that retains the full passive-order safety gate."""
        claim: Optional[tuple[str, int, str]] = None
        try:
            claim = await self._preflight_post_order(token_id, price, label)
            response = await self._submit_post_order(token_id, price, size, label)
            await self._aggressive_pair_submit_succeeded(token_id, claim)
            return response
        except Exception as exc:
            await self._aggressive_pair_submit_failed(
                token_id,
                claim,
                f"{label}:{exc.__class__.__name__}",
            )
            raise

    async def place_post_only_order(self, token_id: str, price: Decimal, size: Decimal, label: str = "post") -> Any:
        claim: Optional[tuple[str, int, str]] = None
        try:
            claim = await self._preflight_post_order(token_id, price, label)
            async with self._signer_sem:
                async with self._signer_gap_lock:
                    now = time.time()
                    wait_sec = max(0.0, self.signer_requote_gap_sec - (now - self._last_signer_post_ts))
                    if wait_sec > 0:
                        await asyncio.sleep(wait_sec)
                    self._last_signer_post_ts = time.time()
                response = await self._submit_post_order(token_id, price, size, label)
            await self._aggressive_pair_submit_succeeded(token_id, claim)
            return response
        except Exception as exc:
            await self._aggressive_pair_submit_failed(
                token_id,
                claim,
                f"{label}:{exc.__class__.__name__}",
            )
            raise

    def _count_live_orders(self, orders: list[dict]) -> int:
        return sum(1 for order in orders if _order_is_live(order))

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

    def _resume_expired_global_cooldown_markets(self, trigger: str) -> int:
        """Resume only markets stopped by an expired global cooldown.

        Market-specific cooldowns have their own recovery rules and must
        remain untouched here.
        """
        now = time.time()
        if now < self._cooldown_until or self._require_recovery_gate:
            return 0

        resumed = 0
        all_tokens = set(self.market_cfg) | set(self._night_market_cfg)
        for token_id in all_tokens:
            entry = self._event_state_entry(token_id)
            if (
                str(entry.get("state") or EVENT_ACTIVE) != EVENT_COOLDOWN
                or str(entry.get("reason") or "") != "global_cooldown"
            ):
                continue
            self._set_event_state(token_id, EVENT_ACTIVE, trigger)
            resumed += 1

        if resumed:
            log(
                f"[recovery] resumed {resumed} market(s) after global cooldown "
                f"trigger={trigger}"
            )
        return resumed

    async def _cancel_all_except_exit(self) -> bool:
        """Cancel all live quote orders while preserving every SELL order.

        The strategy quotes with BUY orders on YES/NO tokens, so any live SELL
        is risk-reducing inventory disposal. Preserve SELLs even when the
        in-memory exit registry was lost after restart or manual recovery.
        Returns True if all non-exit orders were successfully canceled.
        """
        guard = self._exchange_maintenance_guard()
        if guard.cancel_retry_delay() > 0:
            return False
        orders = await self._read_open_orders("cancel_all_read")
        if not isinstance(orders, list):
            return False
        protected_ids = {
            str(order_id) for order_id in self._active_exit_orders.values() if order_id
        }
        protected_ids.update(
            str(o.get("id") or o.get("orderID") or "")
            for o in orders
            if _order_is_live(o)
            and str(o.get("side", "")).upper() == "SELL"
            and (o.get("id") or o.get("orderID"))
        )
        cancel_ids = []
        for o in orders:
            oid = str(o.get("id") or o.get("orderID") or "")
            if _order_is_live(o) and oid and oid not in protected_ids:
                cancel_ids.append(oid)
        if cancel_ids:
            try:
                canceled = await self._execute_exchange_cancel(
                    "cancel_all_except_exit",
                    self.client.cancel_orders,
                    cancel_ids,
                )
                if not canceled:
                    return False
                self._invalidate_all_orders_cache()
            except Exception as exc:
                log(
                    f"[cancel_all_except_exit] cancel_orders failed "
                    f"count={len(cancel_ids)} err={exc}"
                )
                return False
        # Clear cache after canceling
        self._market_live_orders.clear()
        self._sibling_registry.clear_funder(self._funder_lc,
                                            keep_order_ids=protected_ids)  # 施工包04
        # Verify: only protected orders remain
        remaining = await self._read_open_orders("cancel_all_verify")
        if not isinstance(remaining, list):
            log(
                "[cancel_all_except_exit] invalid verification response; "
                "cancellation remains unconfirmed"
            )
            return False
        live_non_exit = [
            o for o in remaining
            if _order_is_live(o)
            and str(o.get("id") or o.get("orderID") or "") not in protected_ids
        ]
        verified = len(live_non_exit) == 0
        if verified:
            guard.note_cancel_success()
        return verified

    async def trigger_global_kill_switch(self, reason: str) -> None:
        async with self._kill_switch_lock:
            now = time.time()
            if now < self._cooldown_until and self._require_recovery_gate:
                log(f"[kill-switch] already active reason={reason}")
                return

            protected = set(self._active_exit_orders.values())
            if protected:
                log(f"[kill-switch] protecting {len(protected)} exit SELL order(s)")

            # Retry cancel_all indefinitely — Kevin's 2026-04-23 directive is
            # that kill-switch keeps trying forever, never gives up. The old
            # `cancel_retry_window_sec` deadline (default 300s) was ending the
            # loop and leaving live orders during sustained Polymarket / proxy
            # outages (observed 2026-04-24: 7556 Request exception errors in
            # 2h, fill through unattended orders).
            canceled_ok = False
            while self._running:
                try:
                    canceled_ok = await self._cancel_all_except_exit()
                    if canceled_ok:
                        break
                except Exception as e:
                    log(f"[kill-switch] cancel failed: {e}")
                retry_delay = max(1.0, float(self.cancel_retry_step_sec))
                maintenance_delay = self._exchange_maintenance_guard().cancel_retry_delay()
                if maintenance_delay > retry_delay:
                    retry_delay = maintenance_delay
                    log(
                        "[kill-switch] exchange maintenance; "
                        f"next cancel retry in {retry_delay:.1f}s"
                    )
                await asyncio.sleep(retry_delay)

            self._cooldown_until = time.time() + self.cooldown_seconds
            self._require_recovery_gate = True
            msg = f"[ALERT] PolyLPS-Multi kill-switch: {reason}; cooldown={self.cooldown_seconds}s"
            log(msg)
            if not str(reason).startswith("exchange_maintenance:"):
                self.notify_discord(
                    "安全暂停已触发",
                    (
                        f"原因：{self._discord_reason(reason)}\n"
                        f"冷静期：{self.cooldown_seconds:.0f} 秒\n"
                        "系统处理：持续撤单，确认安全后自动恢复"
                    ),
                    "danger",
                )
            self._event_bus.publish("kill_switch", {"reason": reason, "cooldown_seconds": self.cooldown_seconds})

    async def _ws_user_watch(self) -> None:
        if not self.kill_switch_on_fill:
            while self._running:
                await asyncio.sleep(5)
            return

        if self._user_ws_resubscribe_evt is None:
            self._user_ws_resubscribe_evt = asyncio.Event()

        urls = ["wss://ws-subscriptions-clob.polymarket.com/ws/user"]
        # Subscribe BOTH day (market_cfg) and night (_night_market_cfg) pools so
        # fills on night-added markets also reach the WS kill-switch.
        all_subscribed_tokens = list(
            set(self.market_cfg.keys()) | set(self._night_market_cfg.keys())
        )
        for token_id in all_subscribed_tokens:
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
            # Union asset_ids across both pools on every payload build so pool
            # changes at runtime (auto_curator adds/removes) reflect on reconnect.
            payloads.append({
                "type": "user",
                "assets_ids": list(set(self.market_cfg.keys()) | set(self._night_market_cfg.keys())),
                "auth": auth,
            })
            return payloads

        consecutive_failures = 0
        ws_down_since = 0.0
        while self._running:
            if not (self.market_cfg or self._night_market_cfg):
                # No maker BUY can exist after the zero-market startup
                # verification. Wait locally until lifecycle provisioning adds
                # the first market, then subscribe with the current token set.
                consecutive_failures = 0
                ws_down_since = 0.0
                await asyncio.sleep(1.0)
                continue
            url = urls[0]
            resub_task: Optional[asyncio.Task] = None
            resubscribe_requested = False
            try:
                # Full restart: if too many consecutive failures, log and reset
                if consecutive_failures >= self._ws_full_restart_after_n:
                    log(f"[fill-ws] {consecutive_failures} consecutive failures — performing full WS restart")
                    self._notify_attention("Fill WS full restart", failures=str(consecutive_failures))
                    consecutive_failures = 0
                    await asyncio.sleep(2.0)

                self._fill_ws_reconnect_count += 1
                if self._fill_ws_reconnect_count > 1:
                    log(f"[fill-ws] reconnect attempt #{self._fill_ws_reconnect_count} (consecutive_failures={consecutive_failures})")

                async with websockets.connect(url, proxy=self._ws_proxy, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=8 * 1024 * 1024) as ws:
                    log(f"[fill-ws] netpath {_ws_proxy_diag(self._ws_proxy)}")
                    for p in _payloads():
                        try:
                            await ws.send(json.dumps(p))
                        except Exception:
                            pass
                    # On successful (re)connect, surface the downtime so we can
                    # tell when balance_drop_watch was the only fill-detection
                    # path. >30s gap is significant (balance_drop takes ~40s to
                    # localize a fill) — warn loudly.
                    now_ok = time.time()
                    if ws_down_since > 0:
                        down_sec = now_ok - ws_down_since
                        if down_sec > 30:
                            log(f"[fill-ws] ⚠ reconnect after {down_sec:.0f}s down "
                                f"— balance_drop_watch was primary fill detection during gap "
                                f"(total_reconnects={self._fill_ws_reconnect_count})")
                            try:
                                self.send_discord(
                                    f"成交连接已恢复\n断线时间：{down_sec:.0f} 秒\n"
                                    f"累计重连：{self._fill_ws_reconnect_count} 次")
                            except Exception:
                                pass
                        else:
                            log(f"[fill-ws] reconnected after {down_sec:.1f}s down "
                                f"total_reconnects={self._fill_ws_reconnect_count}")
                    else:
                        log(f"[fill-ws] connected reconnect_total={self._fill_ws_reconnect_count}")
                    self._last_ws_ok_ts = now_ok
                    self._proxy_failover_record_success("fill-ws")
                    ws_down_since = 0.0
                    consecutive_failures = 0

                    self._user_ws_resubscribe_evt.clear()

                    async def _resubscribe_watchdog() -> None:
                        nonlocal resubscribe_requested
                        try:
                            await self._user_ws_resubscribe_evt.wait()
                            resubscribe_requested = True
                            log(
                                "[fill-ws] resubscribe requested — closing WS "
                                "to reconnect"
                            )
                            await ws.close()
                        except Exception:
                            pass

                    resub_task = asyncio.create_task(
                        _resubscribe_watchdog(),
                        name="fill_ws_resubscribe_watch",
                    )

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

                            # Account streams also contain website/manual
                            # orders. Only engine-created BUY IDs may trigger
                            # automated inventory disposal.
                            event_order_id = str(
                                it.get("order_id") or it.get("id") or ""
                            )
                            size_matched = Decimal("0")
                            if typ == "order":
                                if (
                                    not event_order_id
                                    or event_order_id
                                    not in self._managed_buy_order_ids
                                ):
                                    continue
                                try:
                                    size_matched = Decimal(str(it.get("size_matched", 0) or 0))
                                except Exception:
                                    size_matched = Decimal("0")
                            if isinstance(it.get("maker_orders"), list):
                                for mo in it.get("maker_orders"):
                                    if not isinstance(mo, (dict,)):
                                        continue
                                    mo_id = str(mo.get("order_id") or mo.get("id") or "")
                                    if (
                                        not mo_id
                                        or mo_id
                                        not in self._managed_buy_order_ids
                                    ):
                                        continue
                                    try:
                                        size_matched = max(
                                            size_matched,
                                            Decimal(str(mo.get("matched_amount", 0) or 0)),
                                        )
                                    except Exception:
                                        pass

                            if not token:
                                continue

                            # Cross-side sentinel: feed every "trade" event
                            # (regardless of who the maker was) into the
                            # opposite-token monitor. Aggressor BUY pressure
                            # on token A signals incoming arb cross on the
                            # paired (opposite) token of the same conditionId,
                            # so we pre-emptively cancel our BIDs there.
                            hit = False
                            reason = ""
                            if typ in ("trade", "order") and size_matched > self.fill_size_threshold:
                                hit = True
                                reason = f"WS_{typ.upper()}_MATCH:{size_matched}"
                            elif typ == "order" and status in ("MATCHED", "FILLED", "PARTIALLY_FILLED", "MINED", "CONFIRMED", "RETRYING"):
                                # `trade` status strings are broadcast, so the
                                # status-based trigger only fires for "order"
                                # events (those are per-user by Polymarket's WS
                                # contract).
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
                if resubscribe_requested:
                    consecutive_failures = 0
                    ws_down_since = 0.0
                    continue
                consecutive_failures += 1
                now = time.time()
                if ws_down_since <= 0:
                    ws_down_since = now
                em = _format_exc(e)
                backoff = self._ws_backoff_with_jitter(consecutive_failures)
                log(f"[fill-ws] err={em} consecutive_failures={consecutive_failures} backoff={backoff:.1f}s")
                if "opening handshake" in em.lower() and "timed out" in em.lower():
                    self._proxy_failover_record_ws_handshake_failure("fill-ws")
                if self._is_req_exc(e):
                    self._log_req_diag("fill-ws", e)

                ws_down = now - ws_down_since
                poll_recent_bad = (now - self._poll_err_ts) <= 15 if self._poll_err_ts > 0 else False
                if ws_down > self.ws_down_trigger_sec and poll_recent_bad:
                    await self.trigger_global_kill_switch("ws_down_and_poll_degraded")

                await asyncio.sleep(backoff)
            finally:
                if resub_task is not None and not resub_task.done():
                    resub_task.cancel()

    async def _poll_fill_watch(self) -> None:
        while self._running:
            try:
                # open orders delta/matched fallback
                orders = await self._read_open_orders("fill_poll")
                for o in orders:
                    st = str(o.get("status", "")).lower()
                    token = str(o.get("asset_id") or o.get("token_id") or "")
                    if token not in self.market_cfg and token not in self._night_market_cfg:
                        continue
                    oid = str(o.get("id") or o.get("orderID") or "")
                    if not oid or oid not in self._managed_buy_order_ids:
                        continue

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

                    if not _order_is_live(o):
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
                            if token not in self.market_cfg and token not in self._night_market_cfg:
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

                new_count = 0
                for t in items:
                    if not isinstance(t, dict):
                        continue
                    managed_increase, classification, sz, px, token = (
                        self._managed_inventory_increase_details(t)
                    )
                    if token not in self.market_cfg and token not in self._night_market_cfg:
                        continue

                    tid = str(
                        t.get("id")
                        or t.get("trade_id")
                        or t.get("transaction_hash")
                        or t.get("transactionHash")
                        or ""
                    )
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
                    new_count += 1

                    if not managed_increase:
                        if classification == "manual_or_external_sell":
                            await self._register_manual_sell_trade(token, tid)
                        log(
                            f"[trade-poll] ignored account trade id={tid[:16]} "
                            f"token={token[:16]} side={str(t.get('side') or '').upper()} "
                            f"class={classification}"
                        )
                        continue

                    if sz <= self.fill_size_threshold:
                        log(
                            f"[trade-poll] ignored managed trade id={tid[:16]} "
                            f"token={token[:16]} class={classification} "
                            f"attributed_size={sz}"
                        )
                        continue

                    if self._allow_signal(token, f"trade_poll:{tid}"):
                        self._fills_seen += 1
                        await self._trigger_event_offline(
                            token,
                            f"TRADES_POLL:{tid}",
                            sz,
                            px,
                            matched_size_source="account_order_fill",
                        )

                if not seeded:
                    seeded = True
                    log(f"[trade-poll] baseline seeded trades_count={len(items)} seen_ids={len(self._seen_trade_ids)}")
                elif new_count > 0:
                    log(f"[trade-poll] new_trades={new_count} total={len(items)}")

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

    def _balance_resize_plan(
        self,
        available: Decimal,
        *,
        include_within_limit: bool = False,
    ) -> list[dict]:
        """Plan event-local quote trims after available collateral falls."""
        allowed = max(
            Decimal("0"),
            available - self.budget_reserve_safety_margin_usdc,
        )
        active_tokens = set(self._active_market_cfg())
        events: Dict[str, set[str]] = {}
        for token_id in active_tokens:
            managed_buys = [
                order
                for order in self._market_live_orders.get(token_id, [])
                if self._order_side(order) == "BUY"
                and self._order_id(order) in self._managed_buy_order_ids
            ]
            if managed_buys or include_within_limit:
                events.setdefault(self._event_key(token_id), set()).update(
                    self._event_token_ids(token_id)
                )

        plan = []
        for event_key, event_tokens in events.items():
            event_tokens &= active_tokens
            if not event_tokens:
                continue
            probe_token = sorted(event_tokens)[0]
            reserved = self._event_reserved_collateral(probe_token)
            oversized = reserved > allowed
            if not oversized and not include_within_limit:
                continue

            trim_by_token: Dict[str, list[str]] = {}
            for token_id in sorted(event_tokens) if oversized else []:
                live_buys = [
                    order
                    for order in self._market_live_orders.get(token_id, [])
                    if self._order_side(order) == "BUY"
                    and _order_is_live(order)
                ]
                side_reserved = sum(
                    (
                        self._collateral_required_for_order(
                            token_id,
                            self._order_price(order),
                            self._order_size(order),
                        )
                        for order in live_buys
                    ),
                    Decimal("0"),
                )
                if side_reserved <= allowed:
                    continue

                managed = [
                    order
                    for order in live_buys
                    if self._order_id(order) in self._managed_buy_order_ids
                ]
                # Lowest-priced/back layers leave first. The front quote stays
                # live whenever it fits inside the reduced balance.
                managed.sort(
                    key=lambda order: (
                        self._order_price(order),
                        self._order_id(order),
                    )
                )
                trim_ids = []
                remaining = side_reserved
                for order in managed:
                    if remaining <= allowed:
                        break
                    order_id = self._order_id(order)
                    if not order_id:
                        continue
                    trim_ids.append(order_id)
                    remaining -= self._collateral_required_for_order(
                        token_id,
                        self._order_price(order),
                        self._order_size(order),
                    )
                if trim_ids:
                    trim_by_token[token_id] = trim_ids

            plan.append(
                {
                    "event_key": event_key,
                    "token_ids": sorted(event_tokens),
                    "reserved": reserved,
                    "allowed": allowed,
                    "excess": max(Decimal("0"), reserved - allowed),
                    "oversized": oversized,
                    "trim_by_token": trim_by_token,
                }
            )

        return sorted(
            plan,
            key=lambda item: (
                not item["oversized"],
                -item["excess"],
                item["event_key"],
            ),
        )

    async def _rebalance_quotes_after_balance_change(
        self,
        previous: Decimal,
        available: Decimal,
    ) -> dict:
        """Shrink oversized events one at a time, then immediately re-quote."""
        if not hasattr(self, "_balance_resize_lock"):
            self._balance_resize_lock = asyncio.Lock()
        if self._balance_resize_lock.locked():
            log(
                f"[balance-resize] coalesced prev={previous} now={available}; "
                "another resize is active"
            )
            return {"status": "coalesced", "events": 0, "tokens": 0}

        async with self._balance_resize_lock:
            self._invalidate_all_orders_cache()
            orders = await self._get_all_orders_cached()
            active_tokens = set(self._active_market_cfg())
            by_token: Dict[str, list] = {}
            for order in orders:
                token_id = self._order_token_id(order)
                status = str(order.get("status", "") or "").lower()
                if token_id in active_tokens and status in (
                    "",
                    "live",
                    "open",
                    "active",
                ):
                    by_token.setdefault(token_id, []).append(order)
            for token_id in active_tokens:
                self._market_live_orders[token_id] = self._sorted_live_orders(
                    by_token.get(token_id, [])
                )

            resize_plan = self._balance_resize_plan(
                available,
                include_within_limit=True,
            )
            resized_events = 0
            resized_tokens = 0
            skipped_events = 0
            for item in resize_plan:
                trim_by_token = item["trim_by_token"]
                if item["oversized"] and not trim_by_token:
                    skipped_events += 1
                    log(
                        f"[balance-resize] event={item['event_key'][:24]} "
                        f"reserved={item['reserved']} allowed={item['allowed']} "
                        "managed_trim=0; unmanaged orders preserved"
                    )
                    continue

                if trim_by_token:
                    # Trim both sides of this event before replacing either
                    # side, so the replacement cannot be rejected by the old
                    # reserve.
                    for token_id, order_ids in trim_by_token.items():
                        await self._cancel_order_ids(
                            token_id,
                            order_ids,
                            "balance_hot_resize_trim",
                        )

                event_resized = False
                for token_id in item["token_ids"]:
                    self.last_quote_ts[token_id] = 0.0
                    self._last_plan_sig[token_id] = ""
                    self._last_top_plan_sig[token_id] = ""
                    self._last_back_plan_sig[token_id] = ""
                    self._market_budget_skip_until[token_id] = 0.0
                    try:
                        await self.update_and_quote_market(token_id)
                        resized_tokens += 1
                        event_resized = True
                    except Exception as exc:
                        log(
                            f"[balance-resize] token={token_id[:16]} "
                            f"requote_error={_format_exc(exc)}"
                        )
                if event_resized:
                    resized_events += 1
                    log(
                        f"[balance-rebalance] event={item['event_key'][:24]} "
                        f"reserved={item['reserved']} allowed={item['allowed']} "
                        f"trimmed={sum(len(ids) for ids in trim_by_token.values())} "
                        "requote=done"
                    )

            result = {
                "status": "complete",
                "events": resized_events,
                "tokens": resized_tokens,
                "skipped_events": skipped_events,
                "candidates": len(resize_plan),
                "previous": str(previous),
                "available": str(available),
            }
            self._event_bus.publish("balance_quotes_rebalanced", result)
            log(
                f"[balance-rebalance] complete prev={previous} now={available} "
                f"candidates={len(resize_plan)} events={resized_events} "
                f"tokens={resized_tokens} skipped={skipped_events}"
            )
            return result

    async def _balance_drop_watch(self) -> None:
        """Rebalance engine-owned quotes whenever collateral changes."""
        BALANCE_CHANGE_EPS = Decimal("0.01")
        BALANCE_DROP_PCT = Decimal("0.10")  # 10% drop triggers alert
        BALANCE_DROP_ABS = Decimal("20")    # or $20 absolute drop
        prev_balance: Optional[Decimal] = None
        while self._running:
            try:
                avail = self._lp_effective_available(
                    await self._get_collateral_available(force_refresh=True)
                )
                if avail is not None:
                    self._last_balance = avail
                if avail is not None and prev_balance is not None:
                    change = avail - prev_balance
                    drop = prev_balance - avail
                    drop_pct = drop / prev_balance if prev_balance > 0 else Decimal("0")
                    if abs(change) >= BALANCE_CHANGE_EPS:
                        log(
                            f"[balance-change] prev={prev_balance} now={avail} "
                            f"change={change}"
                        )
                        self._event_bus.publish("balance_change", {
                            "prev": str(prev_balance),
                            "now": str(avail),
                            "change": str(change),
                        })
                        if drop > BALANCE_DROP_ABS or drop_pct > BALANCE_DROP_PCT:
                            self.notify_discord(
                                "可用余额下降",
                                (
                                    f"原余额：${float(prev_balance):,.2f}\n"
                                    f"现余额：${float(avail):,.2f}\n"
                                    f"减少：${float(drop):,.2f}（{drop_pct:.2%}）\n"
                                    "系统处理：正在按新余额调整挂单"
                                ),
                                "warning",
                            )
                            self._event_bus.publish("balance_drop", {
                                "prev": str(prev_balance), "now": str(avail),
                                "drop": str(drop),
                                "drop_pct": f"{drop_pct:.4f}",
                            })
                        # Never inspect or dispose of inventory here. A balance
                        # change can come from a manual website action. Only
                        # engine-owned BUY liquidity is resized, one event at a
                        # time, and manual SELL exits remain protected.
                        await self._rebalance_quotes_after_balance_change(
                            prev_balance,
                            avail,
                        )
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
                "每小时运行汇总\n"
                f"监控市场：{len(self.market_cfg)} 个\n"
                f"本小时挂单：{self._quotes_sent} 笔\n"
                f"本小时成交：{self._fills_seen} 笔\n"
                f"安全冷静期：{'生效中' if time.time() < self._cooldown_until else '未触发'}"
            )
            self._notify_attention("Message", text=msg)

    async def start_guard_sweep_loop(self) -> None:
        """Periodically scan all markets and enforce hard pre-start stop.
        Cutoff = event_start_time - pre_start_stop_sec (default 3 hours).
        At/after cutoff: cancel live orders, clear plan cache, set START_BLOCKED.
        Runs regardless of current state (COOLDOWN, WATCH, banned, session, etc.).
        Only applies to sports markets (detected via _is_sports_market).
        """
        interval = 30  # seconds — start time is coarse, no need to hammer
        while self._running:
            try:
                all_tokens = list(set(
                    list(self.market_cfg.keys()) + list(self._night_market_cfg.keys())
                ))
                now = time.time()
                for tid in all_tokens:
                    try:
                        if self._event_state_name(tid) == EVENT_STARTED_BLOCKED:
                            continue  # already handled

                        meta = None

                        # Prefer the locally-stored override (populated by
                        # auto_curator at admission). This makes T-2h cutoff
                        # resilient to a gamma API outage — we never need to
                        # re-fetch meta for markets we admitted ourselves.
                        mcfg = self.market_cfg.get(tid) or self._night_market_cfg.get(tid) or {}
                        override_ts = float(mcfg.get("game_start_ts_override", 0.0) or 0.0)
                        start_ts: float = 0.0
                        if override_ts > 0:
                            start_ts = override_ts
                        else:
                            # Fallback: gamma meta (manual config entries / older runtime adds)
                            meta = await self._get_market_meta(tid)
                            if not self._is_sports_market(meta):
                                continue  # only enforce start guard on sports markets
                            start_ts_raw = meta.get("gameStartTs") if meta else None
                            if start_ts_raw is None:
                                continue
                            try:
                                start_ts = float(start_ts_raw)
                            except Exception:
                                continue
                        if start_ts <= 0:
                            continue

                        cutoff_sec = self._market_pre_start_stop_sec(tid, meta if 'meta' in locals() else None)
                        cutoff = start_ts - cutoff_sec
                        if now < cutoff:
                            continue  # still safe to quote

                        # At/after cutoff — hard stop
                        live = await self._get_live_orders_fast(tid)
                        cancelled_n = len(live) if live else 0
                        if live:
                            await self._cancel_order_ids(
                                tid,
                                [self._order_id(o) for o in live],
                                "pre_start_stop",
                            )
                            log(f"[start-guard] {tid[:16]} cancelled {cancelled_n} orders | start_ts={int(start_ts)} cutoff_reached")
                        reason = f"pre_start_stop|start_ts={int(start_ts)}|cutoff_sec={cutoff_sec}"
                        self._set_event_state(tid, EVENT_STARTED_BLOCKED, reason)
                        self.last_quote_ts[tid] = 0.0
                        self._last_plan_sig[tid] = ""
                        self._last_top_plan_sig[tid] = ""
                        self._last_back_plan_sig[tid] = ""
                        slug = self._token_slug_cache.get(tid, tid[:16])
                        start_hm = datetime.fromtimestamp(start_ts).strftime("%m-%d %H:%M")
                        self.send_discord(
                            f"赛前下架\n市场：{slug}\n开赛：{start_hm}\n"
                            f"撤单：{cancelled_n} 笔\n提前：{cutoff_sec} 秒\n来源：定时检查"
                        )

                        # Drop the market from market_cfg entirely at cutoff so we
                        # stop receiving WS/REST data for it (avoids snapshot-drop
                        # noise and eliminates any chance of post-start accidental
                        # quoting). Applies to both auto_curator and manual entries —
                        # each sports game has unique token_ids that never recur.
                        paired_for_cfg = str(
                            (self.market_cfg.get(tid) or self._night_market_cfg.get(tid) or {})
                            .get("paired_token_id", "")
                        )
                        try:
                            await self.remove_market_runtime(tid, reason="pre_start_stop")
                        except Exception as e:
                            log(f"[start-guard] remove_market_runtime err token={tid[:16]}: {e}")

                        # Also prune from config.json so a restart doesn't re-register
                        # the dead game. Remove both YES and paired NO entries across
                        # markets / night_markets.
                        try:
                            with self._config_transaction_lock():
                                cfg_disk = json.loads(
                                    self._config_path.read_text(encoding="utf-8")
                                )
                                drop_ids = {tid}
                                if paired_for_cfg:
                                    drop_ids.add(paired_for_cfg)
                                removed = 0
                                for section in ("markets", "night_markets"):
                                    before = cfg_disk.get(section, []) or []
                                    after = [
                                        m
                                        for m in before
                                        if str(m.get("token_id", ""))
                                        not in drop_ids
                                    ]
                                    removed += len(before) - len(after)
                                    cfg_disk[section] = after
                                if removed > 0:
                                    self._write_config_atomic(cfg_disk)
                                    log(
                                        "[start-guard] config.json pruned "
                                        f"{removed} entries for token={tid[:16]}"
                                    )
                        except Exception as e:
                            log(f"[start-guard] config.json prune err token={tid[:16]}: {e}")
                    except Exception as e:
                        log(f"[start-guard] sweep err token={tid[:16]}: {e}")
            except Exception as e:
                log(f"[start-guard] sweep loop err: {e}")
            await asyncio.sleep(interval)

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

    async def _scan_for_position(self, token_ids: list[str]) -> tuple[str | None, float]:
        """Scan a list of tokens and return the one with the LARGEST position.

        Previous bug: returned the first token with pos > 0, which could be a
        dust remnant (e.g. 0.007 shares) while the actual filled token held 236
        shares. This caused the exit flow to target the wrong token.
        """
        best_tid: str | None = None
        best_pos: float = 0.0
        for tid in token_ids:
            try:
                pos = await self._get_token_position(tid)
                if pos is not None and pos > best_pos:
                    best_tid = tid
                    best_pos = pos
            except Exception:
                pass
        return best_tid, best_pos

    async def unwind_tracking_loop(self) -> None:
        """Periodically check pending unwind SELL orders.
        - If position is zero or dust, cancel any residual order and resume unrelated markets.
        - If the order is no longer live but material inventory remains, keep the halt.
        - If age > unwind_max_age_sec and still open — Discord alert for manual review.
        """
        while self._running:
            await asyncio.sleep(self._unwind_check_interval_sec)
            if not self._pending_unwinds:
                continue
            try:
                orders = await self._read_open_orders("unwind_tracking")
                live_ids = {
                    str(o.get("id") or o.get("orderID") or "")
                    for o in orders
                    if _order_is_live(o)
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

                    # Treat exchange-defined dust as economic completion. A
                    # negative value is the position API's error sentinel and
                    # must never be interpreted as a closed position.
                    position = await self._get_token_position(token_id)
                    position_is_known = position is not None and position >= 0
                    position_is_dust = (
                        position_is_known
                        and position <= self._exit_dust_threshold
                    )
                    if position_is_dust:
                        # Position is gone or too small to exit. Cancel any
                        # residual SELL before releasing unrelated markets.
                        if oid and oid in live_ids:
                            try:
                                canceled = await self._execute_exchange_cancel(
                                    "cancel_unwind_residual",
                                    self.client.cancel_orders,
                                    [oid],
                                )
                                if not canceled:
                                    still_pending.append(uw)
                                    continue
                                self._invalidate_all_orders_cache()
                                log(
                                    f"[unwind] position={position}, "
                                    f"canceled residual order={oid}"
                                )
                            except Exception as ce:
                                log(f"[unwind] cancel residual failed order={oid} err={ce}")
                                still_pending.append(uw)
                                continue
                        self._active_exit_orders.pop(token_id, None)
                        if position > 0:
                            self._set_event_state(
                                token_id,
                                EVENT_PENDING_MANUAL_EXIT,
                                "dust_position_after_unwind",
                            )
                            log(
                                f"[unwind] cleared dust token={token_id} "
                                f"position={position} threshold={self._exit_dust_threshold} "
                                f"age={age:.0f}s"
                            )
                            if not uw.get("dust_alerted"):
                                uw["dust_alerted"] = True
                                self._notify_attention(
                                    "退出后仅剩微量仓位",
                                    market=token_id,
                                    position=f"{float(position):,.4f} 份",
                                    action="该市场等待人工检查；其他市场已自动恢复挂单",
                                )
                            still_pending.append(uw)
                            self._resume_halted_markets("unwind_dust_position")
                        else:
                            self._record_exit(token_id)
                            resolved_tokens = self._consume_resolved_unwinds(
                                token_id
                            )
                            self._activate_resolved_exit_tokens(
                                resolved_tokens,
                                "unwind_position_zero",
                            )
                            log(
                                f"[unwind] cleared token={token_id} position=0 "
                                f"age={age:.0f}s (manually sold or filled)"
                            )
                            self._resume_halted_markets("unwind_position_zero")
                        continue

                    if oid and oid not in live_ids:
                        # Missing order is not proof of a fill: it may have
                        # been externally canceled or removed by a guard.
                        # Only position==0 above is authoritative completion.
                        if not uw.get("missing_order_alerted"):
                            uw["missing_order_alerted"] = True
                            uw["missing_order_detected_at"] = now
                            log(
                                f"[unwind] order missing but position={position}; "
                                f"keeping halt token={token_id} order_id={oid}"
                            )
                            if position_is_known and position > 0:
                                self._set_event_state(
                                    token_id,
                                    EVENT_PENDING_MANUAL_EXIT,
                                    "exit_order_missing_with_inventory",
                                )
                            position_display = (
                                f"{float(position):,.4f} 份"
                                if position_is_known
                                else "读取失败"
                            )
                            self._notify_attention(
                                "退出单异常",
                                market=token_id,
                                position=position_display,
                                action="退出单已不在挂单中；账户保持暂停，等待重新提交退出单",
                            )
                        self._active_exit_orders.pop(token_id, None)
                        still_pending.append(uw)
                        continue

                    if age > self._unwind_max_age_sec:
                        # Timed out — notify via Discord for manual review, keep order alive
                        hours = age / 3600
                        log(f"[unwind] timeout alert token={token_id} age={hours:.1f}h order_id={oid} position={position}")
                        self._notify_attention(
                            "退出单等待超时",
                            market=token_id,
                            waiting=f"{hours:.1f} 小时",
                            fill_price=f"${float(fill_price):.4f}",
                            size=f"{float(fill_size):,.2f} 份",
                            notional=f"${float(fill_price * fill_size):,.2f}",
                            position=f"{float(position):,.4f} 份",
                            reason=uw.get("reason", ""),
                            action="请检查市场并人工决定是否调整退出价格",
                        )
                        still_pending.append(uw)
                    else:
                        still_pending.append(uw)

                self._pending_unwinds = still_pending
            except Exception as e:
                log(f"[unwind] tracking loop error: {e}")

    async def heartbeat_loop(self) -> None:
        """Touch data/.engine_N.heartbeat every second so the dashboard can
        detect liveness across machines (PID-based check breaks once we rsync
        state files from VPS2 → VPS1; the remote PID means nothing locally).
        Dashboard falls back to: mtime > now-10s = alive.
        """
        while self._running:
            try:
                self._heartbeat_path.touch(exist_ok=True)
            except Exception as e:
                log(f"[heartbeat] touch err: {e}")
            await asyncio.sleep(1.0)

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
                        boundary = executable_quote_boundary(
                            best_bid=tob.best_bid,
                            midpoint=tob.mid,
                            tick=tick,
                            max_spread=s,
                            min_distance_ticks=mcfg.get("min_distance_ticks", 1),
                        )
                        reward_lower = float(boundary.reward_lower)
                        reward_upper = (
                            float(boundary.quote) if boundary.executable else None
                        )

                    live_orders = self._market_live_orders.get(tid, [])
                    active_exit_order_ids = {
                        str(order_id) for order_id in self._active_exit_orders.values() if order_id
                    }
                    orders_out = [
                        {
                            "id": str(o.get("id") or o.get("orderID") or ""),
                            "price": float(str(o.get("price", 0) or 0)),
                            "size": float(str(o.get("size", 0) or o.get("original_size", 0) or 0)),
                            "price_raw": str(o.get("price", 0) or 0),
                            "size_raw": str(o.get("size", 0) or o.get("original_size", 0) or 0),
                            "size_matched_raw": str(o.get("size_matched", 0) or 0),
                            "side": str(o.get("side") or "BUY").lower(),
                            "status": str(o.get("status") or "open").lower(),
                            "post_only": (
                                self._order_side(o) == "BUY" and self.post_only
                            ),
                            "is_exit": self._order_id(o) in active_exit_order_ids,
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
                    # Q_min efficiency
                    q_eff, q_details = self.calculate_q_min_efficiency(tid)
                    coordinated_pair = self._coordinated_pair_token(tid)
                    own_live_buys = self._cached_live_buy_count(tid)
                    pair_live_buys = (
                        self._cached_live_buy_count(coordinated_pair)
                        if coordinated_pair
                        else 0
                    )
                    if not coordinated_pair:
                        paired_quote_status = "not_required"
                    elif own_live_buys > 0 and pair_live_buys > 0:
                        paired_quote_status = "both_live"
                    elif own_live_buys == 0 and pair_live_buys == 0:
                        paired_quote_status = "both_empty"
                    else:
                        paired_quote_status = "single_live"
                    markets_out[tid] = {
                        "mid": mid,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "reward_lower": reward_lower,
                        "reward_upper": reward_upper,
                        "orders": orders_out,
                        "desired_plan_sig": self._last_plan_sig.get(tid, ""),
                        "condition_id": self._market_condition_ids.get(tid, ""),
                        "parent_event_id": self._market_parent_event_ids.get(tid, ""),
                        "parent_event_cooldown_until": self._parent_event_cooldown_until.get(
                            self._market_parent_event_ids.get(tid, ""),
                        ),
                        "paired_token_id": str(mcfg.get("paired_token_id") or ""),
                        "price_tick": str(mcfg.get("tick") or ""),
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
                        "q_min_efficiency": float(q_eff),
                        "q_bid_shares": q_details.get("this_shares", 0),
                        "q_ask_shares": q_details.get("paired_shares", 0),
                        "q_min": q_details.get("q_min", 0),
                        "rewards_min_size": q_details.get("rewards_min_size", 0),
                        "has_dual_side": q_details.get("has_dual_side", False),
                        "paired_quote_required": bool(coordinated_pair),
                        "paired_quote_status": paired_quote_status,
                        "paired_quote_live_buys": own_live_buys,
                        "paired_quote_other_live_buys": pair_live_buys,
                        "sponsored_risk": self._sponsored_guard_by_token.get(tid),
                        "source": str(mcfg.get("source") or "manual"),
                        "eligibility": self._eligibility_state.get(tid),
                        "lifecycle_stage": str(
                            mcfg.get("lifecycle_stage") or ""
                        ),
                        "lifecycle_retire_pending": bool(
                            mcfg.get("lifecycle_retire_pending")
                        ),
                    }

                # Prune curator_events_log entries older than TTL
                try:
                    ttl_cutoff = now - self._curator_events_ttl_sec
                    self._curator_events_log = [
                        e for e in self._curator_events_log
                        if float(e.get("added_at", 0) or 0) >= ttl_cutoff
                    ]
                except Exception:
                    pass
                # Decorate each entry with current state (still in pool? already started?)
                curator_events_out = []
                for e in self._curator_events_log:
                    tid = str(e.get("token_id", ""))
                    gst = float(e.get("game_start_ts", 0) or 0)
                    in_pool = tid in self._night_market_cfg or tid in self.market_cfg
                    if gst > 0 and now >= gst:
                        live_status = "started"
                    elif in_pool:
                        live_status = "in_pool"
                    else:
                        live_status = "removed"
                    curator_events_out.append({
                        **e,
                        "session": e.get("session", "night"),  # default for legacy entries
                        "live_status": live_status,
                        "in_pool": in_pool,
                    })

                current_session = self._current_session()
                order_scoring_state = self._order_scoring_observer.public_state(now)
                market_data_state: Dict[str, Any] = {"shared_fetcher_enabled": False}
                if self._shared_book_cache is not None:
                    stats_fn = getattr(self._shared_book_cache, "stats", None)
                    if callable(stats_fn):
                        try:
                            market_data_state = {
                                "shared_fetcher_enabled": True,
                                **stats_fn(),
                            }
                        except Exception as stats_exc:
                            market_data_state = {
                                "shared_fetcher_enabled": True,
                                "stats_error": type(stats_exc).__name__,
                            }
                state = {
                    "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "account_index": self._account_idx,
                    "account_uid_key": (
                        self._stable_lifecycle_account_uid_key or None
                    ),
                    "account_id": (
                        self.lp_account_profile.account_id
                        if self.lp_account_profile.managed
                        else f"pm-account-{self._account_idx or 1}"
                    ),
                    "lp_account": self.lp_account_profile.public_dict(),
                    "aggressive_guardrails": {
                        "enabled": self._aggressive_guardrails_enabled,
                        "active": (
                            self._aggressive_guardrails_enabled
                            and self._runtime_scope == "aggressive"
                        ),
                        "state": self._aggressive_guardrail_state.public_dict(),
                    },
                    "aggressive_defense": {
                        "enabled": self._aggressive_recovery_enabled,
                        "recovery_required_samples": self._aggressive_recovery_required_samples,
                        "recovery_sample_interval_sec": self._aggressive_recovery_sample_interval_sec,
                    },
                    "order_scoring_observer": {
                        "enabled": self._order_scoring_observer_enabled,
                        **order_scoring_state["summary"],
                        "last_error": order_scoring_state.get("last_error"),
                    },
                    "release_sha": os.getenv("POLYMARKET_RELEASE_SHA") or None,
                    "release_required": os.getenv(
                        "POLYMARKET_REQUIRE_RELEASE", ""
                    ).strip().lower() in {"1", "true", "yes", "on"},
                    "runtime": {
                        "mode": self._runtime_mode,
                        "scope": self._runtime_scope or None,
                        "host_id": self._runtime_host_id or None,
                        "routing_roster_sha256": self._routing_roster_sha256 or None,
                        "market_universe_sha256": self._routing_market_universe_sha256 or None,
                        "routing_account_count": self._routing_account_count,
                        "local_account_count": self._local_account_count,
                        "dynamic_market_updates": self._runtime_market_updates_enabled,
                        "zero_market_startup": self._zero_market_startup_state(),
                    },
                    "market_data": market_data_state,
                    "stable_market_lifecycle": {
                        "enabled": self._stable_market_lifecycle_enabled,
                        "desired_enabled": getattr(
                            self,
                            "_stable_market_lifecycle_desired_enabled",
                            self._stable_market_lifecycle_enabled,
                        ),
                        "restart_required": bool(
                            getattr(
                                self,
                                "_stable_market_lifecycle_desired_enabled",
                                self._stable_market_lifecycle_enabled,
                            )
                            != self._stable_market_lifecycle_enabled
                        ),
                        "status": self._stable_lifecycle_state.get("status"),
                        "reason": self._stable_lifecycle_state.get("reason"),
                        "proposal_generated_at": self._stable_lifecycle_state.get(
                            "proposal_generated_at"
                        ),
                        "last_add_results": self._stable_lifecycle_state.get(
                            "add_results", []
                        ),
                        "last_promote_results": self._stable_lifecycle_state.get(
                            "promote_results", []
                        ),
                        "last_retire_results": self._stable_lifecycle_state.get(
                            "retire_results", []
                        ),
                        "active_canaries": self._stable_lifecycle_state.get(
                            "active_canaries", 0
                        ),
                        "max_active_canaries": getattr(
                            self,
                            "_stable_lifecycle_max_active_canaries",
                            10,
                        ),
                        "canary_budget_usdc": self._stable_lifecycle_state.get(
                            "canary_budget_usdc"
                        ),
                    },
                    "balance": float(self._last_balance) if self._last_balance is not None else None,
                    "quotes_sent": self._quotes_sent,
                    "fills_seen": self._fills_seen,
                    "cooldown_active": now < self._cooldown_until,
                    "exchange_maintenance": self._exchange_maintenance_guard().snapshot(
                        now=now
                    ),
                    "paused": self._is_account_paused(),
                    "current_session": current_session,
                    "session_enabled": self._session_enabled,
                    "markets": markets_out,
                    "fills": list(self._fills_record[-100:]),
                    "pending_unwinds": list(self._pending_unwinds),
                    "managed_position_tokens": sorted(
                        self._position_reconcile_managed_tokens
                    ),
                    "position_reconcile_manual_sell_ts": dict(
                        self._position_reconcile_manual_sell_ts
                    ),
                    "position_reconcile": dict(self._position_reconcile_state),
                    "exit_records": list(self._exit_records[-100:]),
                    "managed_buy_order_ids": list(
                        self._managed_buy_order_ids_order[
                            -self._managed_order_history_limit:
                        ]
                    ),
                    "night_markets_count": len(self._night_market_cfg),
                    "sponsored_risk_guard": self._sponsored_guard_summary,
                    "parent_event_shock_guard": {
                        "enabled": self._parent_event_shock_guard_enabled,
                        "cooldown_sec": self._parent_event_shock_cooldown_sec,
                        "active": {
                            event_id: until
                            for event_id, until in self._parent_event_cooldown_until.items()
                            if until > now
                        },
                    },
                    "cross_side_sentinel": {
                        "enabled": self.cross_side_sentinel.enabled,
                        "dry_run": self.cross_side_sentinel.dry_run,
                        "live_protection": (
                            self.cross_side_sentinel.enabled
                            and not self.cross_side_sentinel.dry_run
                        ),
                    },
                    "curator_events": curator_events_out,
                    # Legacy key — kept for dashboards that haven't been refreshed.
                    "night_events": [e for e in curator_events_out if e.get("session") == "night"],
                    "banned_tokens": [
                        tid for tid in all_markets if self._event_is_banned(tid)
                    ],
                    "latency_records": list(self._latency_records[-50:]),
                    # 施工包04:跨账号自成交防线统计(§2.5)
                    "sibling_registry": {
                        "mode": self._sibling_cfg.get("mode", "observe"),
                        "enabled": self._sibling_cfg.get("enabled", True),
                        **self._sibling_registry.stats(),
                    },
                }
                # 心跳日志:统计有变化时记一行(避免每个写周期刷屏)
                _sib_stats = state["sibling_registry"]
                _sib_sig = (_sib_stats["checked"], _sib_stats["conflicts_detected"],
                            _sib_stats["adjusted"], _sib_stats["skipped"],
                            _sib_stats["complement_observed"])
                if _sib_sig != getattr(self, "_sibling_last_logged", None):
                    self._sibling_last_logged = _sib_sig
                    log(f"[sibling_stats] mode={_sib_stats['mode']} checked={_sib_sig[0]} "
                        f"conflicts={_sib_sig[1]} adjusted={_sib_sig[2]} skipped={_sib_sig[3]} "
                        f"complement={_sib_sig[4]} live={_sib_stats['live_orders']}")

                tmp = self._state_path.with_suffix(".tmp")
                tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
                tmp.replace(self._state_path)
                self._event_bus.set_state(state)
            except Exception as e:
                log(f"[state-writer] error: {e}")
            await asyncio.sleep(self._state_write_interval_sec)

    def _discord_market_name(self, token_id: Any) -> str:
        token = str(token_id or "")
        cache = getattr(self, "_token_slug_cache", {})
        return cache.get(token, token[:16] or "未知市场")

    @staticmethod
    def _discord_reason(reason: Any) -> str:
        raw = str(reason or "").strip()
        lowered = raw.lower()
        translations = (
            ("ws_trade_match", "WebSocket 成交回报"),
            ("remote_signer_unreachable", "Mac mini 签名器不可达"),
            ("bba_jump", "盘口价格快速变化"),
            ("near_expiry", "市场临近到期"),
            ("request_exception_storm", "网络请求连续异常"),
            ("balance_drop", "可用余额明显下降"),
            ("price_change", "盘口价格变化"),
            ("high_liq", "盘口深度快速下降"),
        )
        for marker, label in translations:
            if marker in lowered:
                return label
        return raw or "系统风险信号"

    def _format_discord_fields(self, title: str, fields: Dict[str, Any]) -> str:
        title_map = {
            "Market WS down": "行情连接中断",
            "Market WS full restart": "行情连接正在重建",
            "Fill WS full restart": "成交连接正在重建",
            "Recovery": "系统已恢复",
            "Message": "系统消息",
        }
        labels = {
            "token": "市场",
            "market": "市场",
            "trigger": "触发市场",
            "trigger_token": "触发市场",
            "paired_token": "关联市场",
            "parent_event": "事件编号",
            "markets": "关联市场数",
            "reason": "原因",
            "cooldown_sec": "暂停时间",
            "age_sec": "中断时间",
            "action": "系统处理",
            "failures": "连续失败次数",
            "previous": "原时段",
            "current": "新时段",
            "text": "详情",
            "waiting": "已等待",
            "fill_price": "成交价",
            "size": "成交数量",
            "notional": "成交金额",
            "position": "当前仓位",
        }
        market_keys = {"token", "market", "trigger", "trigger_token", "paired_token"}
        session_names = {"day": "日盘", "night": "夜盘", "unknown": "未知"}
        body = [title_map.get(title, title)]
        for key, value in fields.items():
            if value is None or value == "":
                continue
            if key in market_keys:
                value = self._discord_market_name(value)
            elif key == "reason":
                value = self._discord_reason(value)
            elif key in {"previous", "current"}:
                value = session_names.get(str(value), value)
            elif key in {"cooldown_sec", "age_sec"}:
                value = f"{float(value):.0f} 秒"
            body.append(f"{labels.get(key, key)}：{value}")
        return "\n".join(body)

    def _notify_risk(self, title: str, **fields) -> None:
        headline, *details = self._format_discord_fields(title, fields).splitlines()
        self.notify_discord(headline, "\n".join(details), "warning")

    def _format_fill_alert(self, token_id: str, reason: str,
                           matched_size: Optional[Decimal],
                           matched_price: Optional[Decimal]) -> str:
        """Build a concise operator-facing fill message."""
        slug = self._discord_market_name(token_id)
        lines = [f"市场：{slug}"]
        if matched_size is not None:
            lines.append(f"成交数量：{float(matched_size):,.2f} 份")
        if matched_price is not None:
            try:
                _val = float(matched_size or 0) * float(matched_price or 0)
                lines.append(
                    f"成交价格：${float(matched_price):.4f}（金额 ${_val:,.2f}）"
                )
            except Exception:
                lines.append(f"成交价格：{matched_price}")
        lines.append(f"来源：{self._discord_reason(reason)}")
        lines.append("系统处理：已撤销相关买单，正在退出仓位")
        return "\n".join(lines)


    def _notify_fill(self, title: str, **fields) -> None:
        headline, *details = self._format_discord_fields(title, fields).splitlines()
        self.notify_discord(headline, "\n".join(details), "danger")

    def _notify_status(self, title: str, **fields) -> None:
        headline, *details = self._format_discord_fields(title, fields).splitlines()
        self.notify_discord(headline, "\n".join(details), "info")

    def _notify_attention(self, title: str, **fields) -> None:
        headline, *details = self._format_discord_fields(title, fields).splitlines()
        self.notify_discord(headline, "\n".join(details), "warning")

    def _discord_prefix(self) -> str:
        """Multi-account Discord tag (empty string in single-account mode)."""
        if self._account_idx > 0:
            return f"[{self._account_idx}号] "
        return ""

    def notify_discord(self, title: str, message: str, level: str = "info") -> None:
        """Prefix the account label, then use the shared two-channel router."""
        full_title = f"{self._discord_prefix()}{title}"
        notify_discord(full_title, message, level)

    def send_discord(self, message: str) -> None:
        channel = (
            "important"
            if _is_important_discord_message(message)
            else "normal"
        )
        webhook = _discord_webhook_for(channel)
        if not webhook:
            return
        try:
            requests.post(
                webhook,
                json={"content": f"{self._discord_prefix()}{message}"},
                timeout=8,
            )
        except Exception:
            pass

    def send_fill_discord(self, message: str) -> None:
        self.send_discord(message)


async def _cancel_maker_orders_for_shutdown(engine) -> None:
    """Cancel maker BUYs after workers stop while preserving every SELL exit."""
    verified = await asyncio.wait_for(
        engine._cancel_all_except_exit(),
        timeout=20.0,
    )
    if not verified:
        raise RuntimeError("maker-order cancellation was not verified")


async def _main_with_shutdown(_cfg):
    import signal
    engine = PolyLPSMulti(
        config_path=_cfg,
        allow_paused_zero_markets=True,
    )
    loop = asyncio.get_running_loop()
    shutdown_evt = asyncio.Event()

    def _trigger():
        try:
            log("[shutdown] SIGTERM/SIGINT received, cancelling all orders before exit")
        except Exception:
            pass
        shutdown_evt.set()

    for s in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(s, _trigger)
        except (NotImplementedError, RuntimeError):
            pass

    run_task = asyncio.create_task(engine.run())
    wait_task = asyncio.create_task(shutdown_evt.wait())
    failure = None
    shutdown_verified = False
    try:
        done, _ = await asyncio.wait(
            {run_task, wait_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if run_task in done and not shutdown_evt.is_set():
            try:
                run_task.result()
            except BaseException as exc:
                failure = RuntimeError(f"engine worker failed: {exc}")
                failure.__cause__ = exc
            else:
                failure = RuntimeError("engine worker exited unexpectedly")
    finally:
        engine._running = False
        for task in (run_task, wait_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(run_task, wait_task, return_exceptions=True)

        client = getattr(engine, "client", None)
        if client is not None:
            try:
                await _cancel_maker_orders_for_shutdown(engine)
                shutdown_verified = True
                try:
                    log("[shutdown] maker BUYs cancelled; SELL exits preserved")
                except Exception:
                    pass
            except Exception as exc:
                try:
                    log(f"[shutdown] maker-order cancellation failed: {exc}")
                except Exception:
                    pass

    if client is not None and not shutdown_verified:
        raise RuntimeError("shutdown could not verify maker-order cancellation")
    if failure is not None:
        raise failure


if __name__ == "__main__":
    import sys as _sys
    from release_guard import verify_release

    _release = verify_release(Path(__file__))
    if _release:
        log(f"[release] verified commit={_release['commit']}")
    _cfg = _sys.argv[1] if len(_sys.argv) > 1 else "config.json"
    try:
        asyncio.run(_main_with_shutdown(_cfg))
    except KeyboardInterrupt:
        pass
