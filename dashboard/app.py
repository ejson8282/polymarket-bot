"""
PolyMatrix Dashboard v2
- Dark terminal theme
- Live balance / order utilization
- Per-market order table with status coloring
- Fill / Unwind panel
- Proxy manager (bulk import + connectivity test)
- Control panel with test-mode private key (session only, never written to disk)
"""

from __future__ import annotations

import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta

# Beijing timezone (UTC+8)
_BJT = timezone(timedelta(hours=8))
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ── paths ──────────────────────────────────────────────────────────────────────
REPO_DIR            = Path(__file__).resolve().parent.parent
MAKER_DIR           = REPO_DIR / "platforms/polymarket/maker"
DATA_DIR            = REPO_DIR / "data"
CONFIG_PATH         = MAKER_DIR / "config.json"
ENGINE_PATH         = MAKER_DIR / "engine.py"
SCAN_PATH           = MAKER_DIR / "scanner.py"
PID_PATH            = DATA_DIR / ".engine.pid"
LOG_PATH            = DATA_DIR / "engine.log"
NOTIFY_PATH         = DATA_DIR / "notifications.json"
ENGINE_STATE_PATH   = DATA_DIR / "engine_state.json"
MULTI_RUNNER_PATH   = MAKER_DIR / "multi_runner.py"

# legacy alias — some helpers still use BASE_DIR for cwd
BASE_DIR            = MAKER_DIR

# ── page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Latitude Alpha",
    page_icon="▲",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── CSS ────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* global */
.stApp { background-color: #0d1117 !important; }
section[data-testid="stSidebar"] { background-color: #161b22; }

/* metric cards */
div[data-testid="stMetric"] {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 14px 16px;
}
div[data-testid="stMetric"] label  { color: #8b949e !important; font-size: 11px; letter-spacing: .05em; text-transform: uppercase; }
div[data-testid="stMetric"] .css-1wivap2 { color: #e6edf3 !important; font-size: 22px; font-weight: 600; }

/* tab bar */
div[data-testid="stTabs"] button { color: #8b949e; border-radius: 0; }
div[data-testid="stTabs"] button[aria-selected="true"] {
    color: #58a6ff !important;
    border-bottom: 2px solid #58a6ff;
    background: transparent;
}

/* status pills */
.pill-green { background:#1a3a1a; color:#3fb950; border:1px solid #238636; border-radius:4px; padding:2px 9px; font-size:12px; font-weight:600; }
.pill-red   { background:#3a1a1a; color:#f85149; border:1px solid #da3633; border-radius:4px; padding:2px 9px; font-size:12px; font-weight:600; }
.pill-gray  { background:#21262d; color:#8b949e; border:1px solid #30363d; border-radius:4px; padding:2px 9px; font-size:12px; }
.pill-yellow{ background:#2d2a1a; color:#d29922; border:1px solid #9e6a03; border-radius:4px; padding:2px 9px; font-size:12px; font-weight:600; }

/* section headers */
.section-title { color:#8b949e; font-size:11px; letter-spacing:.1em; text-transform:uppercase; margin-bottom:6px; }

/* emergency button */
div[data-testid="stButton"] button[kind="primary"] {
    background: #da3633 !important;
    border-color: #f85149 !important;
    color: #fff !important;
    font-weight: 700;
}

/* dataframe */
div[data-testid="stDataFrame"] { border: 1px solid #30363d; border-radius: 6px; }
</style>
""", unsafe_allow_html=True)


# ── config helpers ─────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ── process helpers ────────────────────────────────────────────────────────────

def engine_pid() -> int | None:
    if not PID_PATH.exists():
        return None
    try:
        return int(PID_PATH.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def _pid_alive(pid: int) -> bool:
    if platform.system() == "Windows":
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {pid}"], text=True, errors="ignore"
        )
        return str(pid) in out
    else:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False


def engine_running() -> bool:
    pid = engine_pid()
    return bool(pid and _pid_alive(pid))


def start_engine() -> str:
    if engine_running():
        return "Engine already running."
    LOG_PATH.touch(exist_ok=True)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if platform.system() == "Windows" else 0
    env = os.environ.copy()
    test_key = st.session_state.get("test_private_key", "").strip()
    if test_key:
        env["POLY_PRIVATE_KEY"] = test_key
    with LOG_PATH.open("a", encoding="utf-8") as lf:
        proc = subprocess.Popen(
            [sys.executable, str(ENGINE_PATH)],
            cwd=str(BASE_DIR),
            stdout=lf,
            stderr=lf,
            creationflags=flags,
            env=env,
        )
    PID_PATH.write_text(str(proc.pid), encoding="utf-8")
    return f"Engine started — PID {proc.pid}"


def stop_engine() -> str:
    pid = engine_pid()
    if pid:
        if platform.system() == "Windows":
            subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                           capture_output=True, check=False)
        else:
            try:
                import signal
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass
        try:
            PID_PATH.unlink()
        except Exception:
            pass
    msg = emergency_cancel_all()
    return f"Engine stopped. {msg}"


def emergency_cancel_all() -> str:
    key = st.session_state.get("test_private_key", "").strip() or os.getenv("POLY_PRIVATE_KEY", "")
    signer_server_url = str(acc.get("signer_server_url", "")).strip()
    signer_token = os.getenv("SIGNER_TOKEN", "").strip() or str(acc.get("signer_token", "")).strip()

    if key and "REDACTED" not in key:
        code = """
import json, os, sys
from pathlib import Path
from py_clob_client.client import ClobClient
cfg = json.loads(Path("config.json").read_text(encoding="utf-8"))
acc = cfg.get("account", {})
host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
if not key or "REDACTED" in key:
    print("ERR:NO_KEY"); sys.exit(2)
client = ClobClient(host, chain_id=int(acc.get("chain_id", 137)),
                    key=key, signature_type=int(acc.get("signature_type", 0)),
                    funder=acc.get("funder"))
client.set_api_creds(client.create_or_derive_api_creds())
client.cancel_all()
print("OK")
"""
        env = os.environ.copy()
        env["POLY_PRIVATE_KEY"] = key
        p = subprocess.run([sys.executable, "-c", code],
                           cwd=str(BASE_DIR), capture_output=True, text=True, env=env)
        if p.returncode == 0:
            return "cancel_all OK (local key)."
        return f"cancel_all failed (local key, code={p.returncode})."

    if signer_server_url and signer_token:
        code = """
import json
from pathlib import Path
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
from platforms.polymarket.maker.remote_signer import AddressStub, BuilderStub, RemoteSignerClient
cfg = json.loads(Path("platforms/polymarket/maker/config.json").read_text(encoding="utf-8"))
acc = cfg.get("account", {})
host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
chain_id = int(acc.get("chain_id", 137))
signature_type = int(acc.get("signature_type", 0))
funder = acc.get("funder")
signer = RemoteSignerClient(acc.get("signer_server_url"), acc.get("signer_token"))
creds = signer.derive_creds()
client = ClobClient(host=host, chain_id=chain_id)
client.signer = AddressStub(creds["address"], chain_id)
client.builder = BuilderStub(sig_type=signature_type, funder=funder)
client.set_api_creds(ApiCreds(
    api_key=creds["api_key"],
    api_secret=creds["api_secret"],
    api_passphrase=creds["api_passphrase"],
))
client.cancel_all()
print("OK")
"""
        p = subprocess.run([sys.executable, "-c", code],
                           cwd=str(REPO_DIR), capture_output=True, text=True, env=os.environ.copy())
        if p.returncode == 0:
            return "cancel_all OK (remote signer)."
        return f"cancel_all failed (remote signer, code={p.returncode})."

    return "cancel_all skipped: no local key or remote signer credentials."


@st.cache_data(ttl=30)
def fetch_balance_info(host: str, key: str, chain_id: int, sig_type: int, funder: str | None) -> dict:
    """Returns {balance, allowance, error}."""
    code = f"""
import json, os, sys
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
client = ClobClient("{host}", chain_id={chain_id}, key=os.environ["_KEY_"],
                    signature_type={sig_type}, funder={repr(funder)})
client.set_api_creds(client.create_or_derive_api_creds())
r = client.get_balance_allowance(params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
print(json.dumps({{"balance": r.get("balance","0"), "allowance": r.get("allowance","0")}}))
"""
    env = os.environ.copy()
    env["_KEY_"] = key
    p = subprocess.run([sys.executable, "-c", code],
                       cwd=str(BASE_DIR), capture_output=True, text=True, env=env)
    if p.returncode == 0:
        try:
            return json.loads(p.stdout.strip().splitlines()[-1])
        except Exception:
            pass
    return {"balance": None, "allowance": None, "error": p.stderr[:200]}


@st.cache_data(ttl=30)
def fetch_open_orders(host: str, key: str, chain_id: int, sig_type: int, funder: str | None) -> list[dict]:
    code = f"""
import json, os
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OpenOrderParams
client = ClobClient("{host}", chain_id={chain_id}, key=os.environ["_KEY_"],
                    signature_type={sig_type}, funder={repr(funder)})
client.set_api_creds(client.create_or_derive_api_creds())
orders = client.get_orders(OpenOrderParams())
print(json.dumps(orders if isinstance(orders, list) else []))
"""
    env = os.environ.copy()
    env["_KEY_"] = key
    p = subprocess.run([sys.executable, "-c", code],
                       cwd=str(BASE_DIR), capture_output=True, text=True, env=env)
    if p.returncode == 0:
        try:
            return json.loads(p.stdout.strip().splitlines()[-1])
        except Exception:
            pass
    return []


@st.cache_data(ttl=300)
def resolve_market_names_batch(token_ids: tuple) -> dict[str, str]:
    """Fetch all market names in batches of 10 instead of one per token."""
    result = {tid: tid[:16] + "..." for tid in token_ids}
    if not token_ids:
        return result
    batch_size = 10
    for i in range(0, len(token_ids), batch_size):
        batch = token_ids[i:i + batch_size]
        try:
            params = [("clob_token_ids", tid) for tid in batch]
            r = requests.get(
                "https://gamma-api.polymarket.com/markets",
                params=params,
                timeout=8,
            )
            for m in (r.json() if isinstance(r.json(), list) else []):
                ids = m.get("clobTokenIds", "")
                if isinstance(ids, str):
                    try:
                        ids = json.loads(ids)
                    except Exception:
                        ids = [ids]
                q = m.get("question", "")
                label = q[:55] + "..." if len(q) > 55 else q
                for tid in (ids or []):
                    if str(tid) in result:
                        result[str(tid)] = label
        except Exception:
            pass
    return result


def resolve_market_name(token_id: str) -> str:
    """Single token lookup — uses batch cache internally."""
    cfg_tids = tuple(
        str(m.get("token_id", ""))
        for m in load_config().get("markets", [])
        if m.get("token_id")
    )
    names = resolve_market_names_batch(cfg_tids)
    return names.get(token_id, token_id[:16] + "...")


def test_proxy(proxy_str: str) -> bool:
    parts = proxy_str.strip().split(":")
    try:
        if len(parts) == 4:
            host, port, user, pw = parts
            proxy_url = f"http://{user}:{pw}@{host}:{port}"
        elif len(parts) == 2:
            host, port = parts
            proxy_url = f"http://{host}:{port}"
        else:
            return False
        r = requests.get("https://clob.polymarket.com", proxies={"https": proxy_url}, timeout=4)
        return r.status_code < 500
    except Exception:
        return False


# ── engine state (written by engine in future) ─────────────────────────────────

def load_engine_state() -> dict:
    if ENGINE_STATE_PATH.exists():
        try:
            s = json.loads(ENGINE_STATE_PATH.read_text(encoding="utf-8"))
            s["_loaded_at"] = time.time()
            return s
        except Exception:
            pass
    return {"fills": [], "pending_unwinds": [], "banned_tokens": [], "latency_records": [], "markets": {}}


def load_all_engine_states() -> dict[int, dict]:
    """Load engine_state_N.json for N=1..30, plus engine_state.json as account 0."""
    result: dict[int, dict] = {}
    # Single-account state
    if ENGINE_STATE_PATH.exists():
        try:
            s = json.loads(ENGINE_STATE_PATH.read_text(encoding="utf-8"))
            s["_loaded_at"] = time.time()
            result[0] = s
        except Exception:
            pass
    # Multi-account states
    for i in range(1, 31):
        p = DATA_DIR / f"engine_state_{i}.json"
        if p.exists():
            try:
                s = json.loads(p.read_text(encoding="utf-8"))
                s["_loaded_at"] = time.time()
                result[i] = s
            except Exception:
                pass
    return result


def _format_countdown(seconds: Any) -> str:
    try:
        if seconds is None:
            return "-"
        secs = int(round(float(seconds)))
    except Exception:
        return "-"
    if secs <= 0:
        return "started"
    hours, rem = divmod(secs, 3600)
    minutes, secs = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m"
    if minutes > 0:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _pretty_event_state(state: Any, reason: Any = None) -> str:
    raw = str(state or "").upper()
    reason_text = str(reason or "")
    if raw == "START_BLOCKED":
        if "market_started" in reason_text or "market_in_play" in reason_text:
            return "???"
        return "????"
    mapping = {
        "ACTIVE": "ACTIVE",
        "DEFENSIVE": "DEFENSIVE",
        "CANCELING": "CANCELING",
        "HALTED_ON_FILL": "HALTED_ON_FILL",
        "HALTED_ON_DATA": "HALTED_ON_DATA",
        "COOLDOWN": "COOLDOWN",
    }
    return mapping.get(raw, str(state or "waiting"))


def multi_engine_running() -> dict[int, bool]:
    """Check which account engine processes are alive via PID files."""
    result: dict[int, bool] = {}
    # Single-account
    if PID_PATH.exists():
        try:
            pid = int(PID_PATH.read_text(encoding="utf-8").strip())
            result[0] = _pid_alive(pid)
        except Exception:
            result[0] = False
    # Multi-account PIDs: .engine_1.pid, .engine_2.pid, ...
    for i in range(1, 31):
        p = DATA_DIR / f".engine_{i}.pid"
        if p.exists():
            try:
                pid = int(p.read_text(encoding="utf-8").strip())
                result[i] = _pid_alive(pid)
            except Exception:
                result[i] = False
    return result


# ── log tail ───────────────────────────────────────────────────────────────────

def tail_log(n: int = 60) -> str:
    if not LOG_PATH.exists():
        return "(no log file)"
    lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    return "\n".join(lines[-n:])


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### Latitude Alpha")
    st.markdown("---")

    PLATFORMS = {
        "Polymarket": ["Market Making"],
        # future: "Hyperliquid": ["Perps", "Vaults"],
    }

    nav_platform = None
    nav_feature = None
    for p, features in PLATFORMS.items():
        st.markdown(f"<span style='color:#8b949e; font-size:11px; letter-spacing:.08em; text-transform:uppercase'>{p}</span>", unsafe_allow_html=True)
        for f in features:
            if st.button(
                f"  {f}",
                key=f"nav_{p}_{f}",
                use_container_width=True,
            ):
                st.session_state["nav_feature"] = f"{p}/{f}"
            if nav_platform is None:
                nav_platform = p
                nav_feature = f

    # default selection
    _nav = st.session_state.get("nav_feature", "Polymarket/Market Making")
    nav_platform, nav_feature = _nav.split("/", 1)

    st.markdown("---")
    st.caption(f"{nav_platform} / {nav_feature}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════════════

cfg = load_config()
acc = cfg.get("account", {})
host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")

# active key: test session key > env var > config (REDACTED)
active_key = (
    st.session_state.get("test_private_key", "").strip()
    or os.getenv("POLY_PRIVATE_KEY", "").strip()
)
has_key = bool(active_key and "REDACTED" not in active_key)

chain_id  = int(acc.get("chain_id", 137))
sig_type  = int(acc.get("signature_type", 0))
funder    = acc.get("funder")

# ── header ─────────────────────────────────────────────────────────────────────
col_title, col_status, col_stop = st.columns([4, 2, 1])
with col_title:
    st.markdown("## Latitude Alpha")
    st.caption(f"{nav_platform}  /  {nav_feature}")
with col_status:
    running = engine_running()
    badge = '<span class="pill-green">● RUNNING</span>' if running else '<span class="pill-red">● STOPPED</span>'
    key_badge = '<span class="pill-green">KEY OK</span>' if has_key else '<span class="pill-yellow">NO KEY</span>'
    st.markdown(f"{badge}&nbsp;&nbsp;{key_badge}", unsafe_allow_html=True)
    st.caption(f"Refresh: {datetime.now(_BJT).strftime('%H:%M:%S')} 北京时间")
with col_stop:
    if st.button("EMERGENCY STOP", type="primary", use_container_width=True):
        msg = stop_engine()
        st.error(msg)

st.divider()

# ── metric cards ───────────────────────────────────────────────────────────────
engine_state = load_engine_state()
_now_unix = time.time()

# Prefer engine_state.json (written every 5s by engine) over slow subprocess calls.
# Only fall back to CLOB API subprocess when engine state is missing.
_es_balance = engine_state.get("balance")
_es_markets = engine_state.get("markets", {})
_es_has_data = _es_balance is not None and bool(_es_markets)

if _es_has_data:
    # Fast path: all data from engine_state.json — no subprocess needed
    balance_raw = float(_es_balance)
    bal_info = {"balance": None, "allowance": None}
    open_orders = []
    # Compute order stats from engine_state markets
    _total_order_notional = 0.0
    _total_order_count = 0
    for _ms in _es_markets.values():
        for _o in _ms.get("orders", []):
            _total_order_notional += float(_o.get("price", 0) or 0) * float(_o.get("size", 0) or 0)
            _total_order_count += 1
    order_size_sum = _total_order_notional
    allowance_raw = 0.0
    utilization = (order_size_sum / balance_raw * 100) if balance_raw > 0 else 0.0
elif has_key:
    # Slow path: engine not running or no state file, use subprocess
    bal_info   = fetch_balance_info(host, active_key, chain_id, sig_type, funder)
    open_orders = fetch_open_orders(host, active_key, chain_id, sig_type, funder)
    balance_raw = float(bal_info.get("balance") or 0) / 1e6
    allowance_raw = float(bal_info.get("allowance") or 0) / 1e6
    order_size_sum = sum(float(o.get("size_matched", 0) or 0) * float(o.get("price", 0) or 0)
                         for o in open_orders)
    utilization = (order_size_sum / allowance_raw * 100) if allowance_raw > 0 else 0.0
else:
    bal_info   = {"balance": None, "allowance": None}
    open_orders = []
    balance_raw = 0.0
    allowance_raw = 0.0
    order_size_sum = 0.0
    utilization = 0.0

fills_today  = [f for f in engine_state.get("fills", [])
                if _now_unix - float(f.get("ts", 0) or 0) < 86400]
unwinds      = engine_state.get("pending_unwinds", [])
overdue_unwinds = [u for u in unwinds
                   if _now_unix - float(u.get("placed_at", _now_unix) or _now_unix) > 4 * 3600]

m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    _show_bal = _es_has_data or has_key
    st.metric("USDC Balance",
              f"${balance_raw:,.2f}" if _show_bal else "—",
              help="Polygon USDC collateral balance")
with m2:
    st.metric("Order Utilization",
              f"{utilization:.1f}%" if _show_bal else "—",
              delta=f"${order_size_sum:,.0f} deployed" if _show_bal else None)
with m3:
    _order_count = _total_order_count if _es_has_data else len(open_orders)
    st.metric("Open Orders",
              str(_order_count) if _show_bal else "—",
              help="All live BUY limit orders")
with m4:
    st.metric("Fills Today", str(len(fills_today)))
with m5:
    overdue_label = f"{len(overdue_unwinds)} overdue" if overdue_unwinds else "clear"
    st.metric("Pending Unwinds", str(len(unwinds)), delta=overdue_label if overdue_unwinds else None,
              delta_color="inverse" if overdue_unwinds else "off")

st.markdown("")

# ── tabs ───────────────────────────────────────────────────────────────────────
tab_control, tab_markets, tab_fills, tab_scan, tab_proxy, tab_accounts = st.tabs(
    ["Control", "Markets", "Fill / Unwind", "Scan", "Proxy", "Accounts"]
)


# ══ TAB: MARKETS ══════════════════════════════════════════════════════════════
with tab_markets:
    markets_cfg = cfg.get("markets", [])
    es_markets  = engine_state.get("markets", {})   # from engine_state.json
    es_ts       = engine_state.get("ts", "")
    es_balance  = engine_state.get("balance")

    has_engine_state = bool(es_markets)

    if has_engine_state:
        age_s = _now_unix - float(engine_state.get("_loaded_at", _now_unix) or _now_unix)
        # Convert UTC timestamp to Beijing time for display
        es_ts_bjt = es_ts
        try:
            if es_ts:
                _utc_dt = datetime.strptime(es_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                es_ts_bjt = _utc_dt.astimezone(_BJT).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        current_session = engine_state.get("current_session", "—")
        session_label = f"  |  session={current_session}" if engine_state.get("session_enabled") else ""
        st.caption(f"Engine state: {es_ts_bjt} 北京时间  |  quotes_sent={engine_state.get('quotes_sent',0)}  "
                   f"fills_seen={engine_state.get('fills_seen',0)}  "
                   f"cooldown={'YES' if engine_state.get('cooldown_active') else 'no'}{session_label}")
    else:
        st.caption("Engine state file not found — showing config + CLOB API data only.")

    # build per-token order lookup from live open orders (CLOB API fallback)
    orders_by_token: dict[str, list] = {}
    for o in open_orders:
        tid = o.get("asset_id", "")
        orders_by_token.setdefault(tid, []).append(o)

    rows = []
    for _i, m in enumerate(markets_cfg, 1):
        tid     = m["token_id"]
        name    = resolve_market_name(tid)
        risk    = m.get("risk", "—")
        enabled = m.get("enabled", True)
        event_state = "waiting"
        event_reason = ""
        countdown = "-"
        gate_grade = ""
        gate_action = ""
        gate_reason = ""

        if has_engine_state and tid in es_markets:
            ms = es_markets[tid]
            status = ms.get("status", "waiting")
            event_reason = ms.get("event_reason", "")
            event_state = _pretty_event_state(ms.get("event_state", status), event_reason)
            countdown = _format_countdown(ms.get("seconds_to_start"))
            gate = ms.get("gate") or {}
            gate_grade = gate.get("risk_grade", "")
            gate_action = gate.get("top_leg_action", "")
            gate_reason = ",".join(gate.get("reason", [])[:2]) if isinstance(gate.get("reason"), list) else ""
            mid    = ms.get("mid")
            bb     = ms.get("best_bid")
            ba     = ms.get("best_ask")
            rl     = ms.get("reward_lower")
            ru     = ms.get("reward_upper")
            es_orders = ms.get("orders", [])
            n_orders  = len(es_orders)
            total_size = sum(float(o.get("size", 0) or 0) for o in es_orders)
            avg_price  = (
                sum(float(o.get("price", 0) or 0) * float(o.get("size", 0) or 0)
                    for o in es_orders) / total_size
                if total_size > 0 else None
            )
            lqt = ms.get("last_quote_ts")
            last_quote = (
                f"{int(_now_unix - lqt)}s ago" if lqt and (_now_unix - lqt) < 3600
                else datetime.fromtimestamp(lqt, tz=_BJT).strftime("%H:%M:%S") if lqt else "—"
            )
            # in-zone check: are orders within [reward_lower, reward_upper]?
            if es_orders and rl is not None and ru is not None:
                in_zone = all(float(rl) <= float(o.get("price", 0)) <= float(ru)
                              for o in es_orders)
                zone_str = "in zone" if in_zone else "out of zone"
            else:
                zone_str = "—"
        else:
            # fallback: CLOB API data
            tok_orders = orders_by_token.get(tid, [])
            n_orders   = len(tok_orders)
            total_size = sum(float(o.get("original_size", 0) or 0) for o in tok_orders)
            avg_price  = (
                sum(float(o.get("price", 0) or 0) * float(o.get("original_size", 0) or 0)
                    for o in tok_orders) / total_size
                if total_size > 0 else None
            )
            mid = ba = rl = ru = None
            bb  = avg_price
            last_quote = "—"
            zone_str   = "—"
            if not enabled:
                status = "disabled"
                event_state = status
            elif not has_key:
                status = "no key"
                event_state = status
            elif n_orders == 0:
                status = "no orders"
                event_state = status
            else:
                status = "active"
                event_state = status

        rows.append({
            "#":          _i,
            "Market":     name,
            "Status":     status,
            "Event":      event_state,
            "Event ETA":  countdown,
            "Gate":       gate_grade,
            "Action":     gate_action,
            "Reason":     gate_reason,
            "Zone":       zone_str,
            "Mid":        f"{mid:.4f}" if mid is not None else "—",
            "Best Bid":   f"{bb:.4f}"  if bb  is not None else "—",
            "Rew Lower":  f"{rl:.4f}"  if rl  is not None else "—",
            "Rew Upper":  f"{ru:.4f}"  if ru  is not None else "—",
            "Orders":     n_orders,
            "Avg Price":  f"{avg_price:.4f}" if avg_price else "—",
            "Size":       f"{total_size:.0f}",
            "Risk":       risk,
            "Last Quote": last_quote,
        })

    if rows:
        STATUS_COLORS = {
            "active":   "color:#3fb950; font-weight:600",
            "waiting":  "color:#8b949e",
            "skipped":  "color:#d29922",
            "banned":   "color:#f85149; font-weight:600",
            "no orders":"color:#f85149",
            "disabled": "color:#8b949e",
            "no key":   "color:#d29922",
        }
        ZONE_COLORS = {
            "in zone":     "color:#3fb950",
            "out of zone": "color:#d29922",
        }
        RISK_COLORS = {"low": "color:#3fb950", "mid": "color:#d29922", "high": "color:#f85149"}
        EVENT_COLORS = {"ACTIVE": "color:#3fb950; font-weight:600", "DEFENSIVE": "color:#d29922; font-weight:600", "CANCELING": "color:#f85149; font-weight:600", "HALTED_ON_FILL": "color:#f85149; font-weight:600", "HALTED_ON_DATA": "color:#f85149; font-weight:600", "COOLDOWN": "color:#8b949e", "????": "color:#d29922; font-weight:600", "???": "color:#f85149; font-weight:600"}
        GATE_COLORS = {"A": "color:#3fb950", "B": "color:#d29922", "C": "color:#f85149", "BLOCK": "color:#f85149; font-weight:600"}

        df = pd.DataFrame(rows)
        styled = (
            df.style
            .map(lambda v: STATUS_COLORS.get(v, ""), subset=["Status"])
            .map(lambda v: EVENT_COLORS.get(v, ""),  subset=["Event"])
            .map(lambda v: GATE_COLORS.get(v, ""),   subset=["Gate"])
            .map(lambda v: ZONE_COLORS.get(v, ""),   subset=["Zone"])
            .map(lambda v: RISK_COLORS.get(v, ""),   subset=["Risk"])
            .set_properties(**{"background-color": "#0d1117", "color": "#e6edf3"})
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("No markets configured.")

    # ── Night Markets ──────────────────────────────────────────────────────────
    night_markets_cfg = cfg.get("night_markets", [])
    if night_markets_cfg:
        st.divider()
        current_session = engine_state.get("current_session", "day")
        session_indicator = " (ACTIVE)" if current_session == "night" else ""
        st.markdown(f'<p class="section-title">Night Markets 夜盘{session_indicator}</p>', unsafe_allow_html=True)

        night_rows = []
        for nm in night_markets_cfg:
            tid = str(nm.get("token_id", ""))
            enabled = nm.get("enabled", True)
            ms = es_markets.get(tid, {})

            label = resolve_market_name(tid)
            risk = nm.get("risk", "mid")
            quote_size = nm.get("quote_size", "—")
            spread = nm.get("max_incentive_spread", "—")
            min_dist = nm.get("min_distance_from_best_bid", "—")

            if ms:
                mid = ms.get("mid")
                bb = ms.get("best_bid")
                n_orders = len(ms.get("orders", []))
                event_state = ms.get("event_state", "—")
                status = ms.get("status", "—")
            else:
                mid = bb = None
                n_orders = 0
                event_state = "—"
                status = "disabled" if not enabled else "waiting"

            night_rows.append({
                "Market": label,
                "Status": status,
                "Event": event_state,
                "Mid": f"{mid:.4f}" if mid is not None else "—",
                "Best Bid": f"{bb:.4f}" if bb is not None else "—",
                "Size": str(quote_size),
                "Spread": str(spread),
                "Risk": risk,
            })

        if night_rows:
            NIGHT_STATUS_COLORS = {
                "active": "color:#3fb950; font-weight:600",
                "waiting": "color:#8b949e",
                "disabled": "color:#8b949e",
                "banned": "color:#f85149; font-weight:600",
            }
            df_night = pd.DataFrame(night_rows)
            styled_night = (
                df_night.style
                .map(lambda v: NIGHT_STATUS_COLORS.get(v, ""), subset=["Status"])
                .map(lambda v: {"low": "color:#3fb950", "mid": "color:#d29922", "high": "color:#f85149"}.get(v, ""), subset=["Risk"])
                .set_properties(**{"background-color": "#0d1117", "color": "#e6edf3"})
            )
            st.dataframe(styled_night, use_container_width=True, hide_index=True)
        else:
            st.info("No night markets configured.")

    if st.button("Refresh Markets", key="refresh_markets"):
        fetch_balance_info.clear()
        fetch_open_orders.clear()
        st.rerun()


# ══ TAB: FILL / UNWIND ════════════════════════════════════════════════════════
with tab_fills:
    st.markdown('<p class="section-title">Pending Unwinds</p>', unsafe_allow_html=True)

    if unwinds:
        unwind_rows = []
        for u in unwinds:
            placed_at = float(u.get("placed_at", _now_unix) or _now_unix)
            age_h = (_now_unix - placed_at) / 3600
            flag  = "OVERDUE" if age_h >= 4 else ""
            notional = float(u.get("fill_price", 0) or 0) * float(u.get("fill_size", 0) or 0)
            unwind_rows.append({
                "Token":       u.get("token_id", "")[:16] + "...",
                "Fill Price":  f"{u.get('fill_price', 0):.4f}",
                "Fill Size":   f"{u.get('fill_size', 0):.1f}",
                "Notional $":  f"{notional:.2f}",
                "Age (h)":     f"{age_h:.1f}",
                "Sell Order":  (u.get("order_id", "") or "—")[:16] + "...",
                "Reason":      u.get("reason", "")[:30],
                "Flag":        flag,
            })
        df_u = pd.DataFrame(unwind_rows)

        def highlight_overdue(row):
            if row["Flag"] == "OVERDUE":
                return ["background-color:#3a1a1a; color:#f85149"] * len(row)
            return [""] * len(row)

        st.dataframe(
            df_u.style.apply(highlight_overdue, axis=1)
                .set_properties(**{"background-color": "#0d1117", "color": "#e6edf3"}),
            use_container_width=True, hide_index=True,
        )
    else:
        st.success("No pending unwinds.")

    st.markdown('<p class="section-title" style="margin-top:24px">Fill History</p>',
                unsafe_allow_html=True)

    all_fills = engine_state.get("fills", [])

    if all_fills:
        fill_rows = []
        for f in reversed(all_fills[-50:]):
            ts_unix = float(f.get("ts", 0) or 0)
            ts_str  = datetime.fromtimestamp(ts_unix, tz=_BJT).strftime("%Y-%m-%d %H:%M:%S") if ts_unix else "—"
            notional = (float(f.get("price", 0) or 0) * float(f.get("size", 0) or 0))
            fill_rows.append({
                "Time (BJT)": ts_str,
                "Token":      f.get("token_id", "")[:16] + "...",
                "Price":      f"{f.get('price', 0):.4f}" if f.get("price") else "—",
                "Size":       f"{f.get('size', 0):.1f}"  if f.get("size")  else "—",
                "Notional $": f"{notional:.2f}",
                "Reason":     f.get("reason", "")[:40],
            })
        st.dataframe(pd.DataFrame(fill_rows), use_container_width=True, hide_index=True)
    else:
        st.info("No fill history yet. Fill events are recorded by the engine while running.")


# ══ TAB: SCAN ════════════════════════════════════════════════════════════════
with tab_scan:
    st.markdown('<p class="section-title">Market Scanner</p>', unsafe_allow_html=True)

    row1_c1, row1_c2, row1_c3, row1_c4 = st.columns(4)
    with row1_c1:
        min_reward = st.number_input("Min Daily Reward ($)", value=10, step=10)
    with row1_c2:
        max_reward = st.number_input("Max Daily Reward ($)", value=8888, step=10, help="0 = no limit")
    with row1_c3:
        min_spread = st.number_input("Min Spread", value=1, step=1, help="maxIncentiveSpread lower bound")
    with row1_c4:
        max_spread = st.number_input("Max Spread", value=10, step=1, help="0 = no limit")

    row2_c1, row2_c2, row2_c3 = st.columns(3)
    with row2_c1:
        min_vol = st.number_input("Min 24h Volume ($)", value=10_000, step=10_000)
    with row2_c2:
        sort_by = st.selectbox("Sort by", ["reward", "reward_score", "volume", "score"], index=1)
    with row2_c3:
        top_n = st.number_input("Top N", value=50, min_value=5, max_value=200)

    if st.button("Run Scan", use_container_width=False):
        with st.spinner("Scanning Polymarket... (30-60s)"):
            scan_cmd = [
                sys.executable, str(SCAN_PATH),
                "--min-volume", str(int(min_vol)),
                "--min-reward", str(int(min_reward)),
                "--max-reward", str(int(max_reward)),
                "--min-spread", str(min_spread),
                "--max-spread", str(max_spread),
                "--sort-by", sort_by,
                "--top", str(int(top_n)),
                "--json",
            ]
            proc_json = subprocess.run(
                scan_cmd,
                cwd=str(BASE_DIR), capture_output=True, text=True,
            )
        # find the JSON line robustly (first line starting with "[")
        json_line = ""
        for line in proc_json.stdout.splitlines():
            line = line.strip()
            if line.startswith("["):
                json_line = line
                break
        if json_line:
            try:
                st.session_state["scan_results"] = json.loads(json_line)
                st.session_state["scan_error"] = ""
            except Exception as e:
                st.session_state["scan_results"] = []
                st.session_state["scan_error"] = f"Parse error: {e}"
        else:
            st.session_state["scan_results"] = []
            st.session_state["scan_error"] = proc_json.stderr[-300:] or "No output."
        st.rerun()

    scan_results = st.session_state.get("scan_results", [])
    scan_error   = st.session_state.get("scan_error", "")
    if scan_error:
        st.error(scan_error)

    # B/D filter — applied to table only, scatter still shows all
    only_ac = st.checkbox("只显示 A/C 区（排除 B/D）", value=True)
    table_results = [m for m in scan_results
                     if not only_ac or m.get("quadrant", "").startswith(("A", "C"))]

    # ── Scatter plot ──────────────────────────────────────────────────────────
    if scan_results:
        try:
            import plotly.express as px

            df_scan = pd.DataFrame(scan_results)
            df_scan["label"] = df_scan["question"].str[:50] + "..."
            df_scan["size_marker"] = df_scan["volume24h"].clip(upper=5e6) / 5e4 + 4

            QUAD_COLORS = {
                "A: high reward, low risk":  "#3fb950",
                "B: high reward, high risk": "#d29922",
                "C: low reward, low risk":   "#58a6ff",
                "D: low reward, high risk":  "#8b949e",
            }

            fig = px.scatter(
                df_scan,
                x="fill_risk",
                y="reward_score",
                color="quadrant",
                color_discrete_map=QUAD_COLORS,
                size="size_marker",
                hover_name="label",
                hover_data={
                    "slug": True,
                    "reward": ":.1f",
                    "volume24h": ":,.0f",
                    "fill_risk": ":.1f",
                    "reward_score": ":.1f",
                    "size_marker": False,
                },
                labels={
                    "fill_risk": "Fill Risk (0=safe, 100=risky)",
                    "reward_score": "Reward Score (0-100)",
                },
                title="",
            )
            fig.update_layout(
                paper_bgcolor="#0d1117",
                plot_bgcolor="#161b22",
                font_color="#e6edf3",
                legend_title_text="Quadrant",
                xaxis=dict(gridcolor="#30363d", zeroline=False, range=[-2, 102]),
                yaxis=dict(gridcolor="#30363d", zeroline=False, range=[-2, 102]),
                margin=dict(l=20, r=20, t=20, b=20),
                height=460,
            )
            # quadrant guide lines
            fig.add_hline(y=50, line_dash="dot", line_color="#30363d", line_width=1)
            fig.add_vline(x=50, line_dash="dot", line_color="#30363d", line_width=1)

            # quadrant labels
            for txt, x, y in [
                ("A: best", 15, 95), ("B: risky", 85, 95),
                ("C: low reward", 15, 5), ("D: avoid", 85, 5),
            ]:
                fig.add_annotation(text=txt, x=x, y=y, showarrow=False,
                                   font=dict(color="#8b949e", size=11))

            st.plotly_chart(fig)

            # ── Merged table: results + config toggle ─────────────────────────
            st.markdown(
                f'<p class="section-title">Markets — click In Config to add / remove'
                f'&nbsp;&nbsp;<span style="color:#58a6ff">{len(table_results)} results'
                f'{" / " + str(len(scan_results)) + " total" if only_ac and len(table_results) != len(scan_results) else ""}'
                f'</span></p>',
                unsafe_allow_html=True,
            )

            existing_tokens = {m["token_id"] for m in cfg.get("markets", [])}
            existing_night_tokens = {m["token_id"] for m in cfg.get("night_markets", [])}

            df_edit = pd.DataFrame([{
                "#":         idx + 1,
                "In Config": (item.get("token_id", "") in existing_tokens
                              or item.get("quadrant", "").startswith(("A", "C"))),
                "夜盘":      item.get("token_id", "") in existing_night_tokens,
                "Market":    item.get("question", "")[:60],
                "Zone":      item.get("quadrant", "?")[0],
                "Daily $":   round(item.get("reward", 0), 0),
                "Risk":      round(item.get("fill_risk", 0), 1),
                "Crowd":     item.get("crowd", "?"),
                "Vol 24h":   round(item.get("volume24h", 0), 0),
                "Spread":    round(item.get("maxIncentiveSpread", 0), 3),
                "打开链接":   item.get("market_url", f"https://polymarket.com/event/{item.get('slug','')}"),
                "_token_id": item.get("token_id", ""),
                "_item":     json.dumps({k: v for k, v in item.items()
                                         if not k.startswith("_")}),
            } for idx, item in enumerate(table_results) if item.get("token_id")])

            edited = st.data_editor(
                df_edit.drop(columns=["_token_id", "_item"]),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "#":         st.column_config.NumberColumn("#", width="small"),
                    "In Config": st.column_config.CheckboxColumn(
                        "In Config", help="Check to add, uncheck to remove", width="small"
                    ),
                    "夜盘": st.column_config.CheckboxColumn(
                        "夜盘", help="勾选添加到夜盘 config", width="small"
                    ),
                    "Market":  st.column_config.TextColumn("Market", width="large"),
                    "打开链接": st.column_config.LinkColumn("打开链接", display_text="打开链接", width="small"),
                    "Zone":    st.column_config.TextColumn("Zone", width="small"),
                    "Daily $": st.column_config.NumberColumn("Daily $", format="$%.0f"),
                    "Risk":    st.column_config.NumberColumn("Risk",    format="%.1f"),
                    "Vol 24h": st.column_config.NumberColumn("Vol 24h", format="$%,.0f"),
                    "Spread":  st.column_config.NumberColumn("Spread",  format="%.3f"),
                },
                disabled=["#", "Market", "Zone", "Daily $", "Risk", "Crowd", "Vol 24h", "Spread", "打开链接"],
                # Note: "In Config" and "夜盘" columns are editable
                key=f"scan_editor_{len(scan_results)}",
            )

            # apply changes when In Config / 夜盘 columns differ from original
            changed = False
            for i, row in edited.iterrows():
                tid  = df_edit.at[i, "_token_id"]
                item = json.loads(df_edit.at[i, "_item"])
                # --- day market toggle ---
                want_in = bool(row["In Config"])
                is_in   = tid in existing_tokens
                if want_in and not is_in:
                    cfg.setdefault("markets", []).append({
                        "token_id": tid,
                        "max_incentive_spread": round(item.get("maxIncentiveSpread", 3.5), 4),
                        "price_tick": 0.01,
                        "min_distance_from_best_bid": 0.01,
                        "quote_size": 100.0,
                        "risk": "low" if item.get("quadrant","").startswith("A") else "mid",
                        "enabled": True,
                    })
                    existing_tokens.add(tid)
                    changed = True
                elif not want_in and is_in:
                    cfg["markets"] = [m for m in cfg.get("markets", [])
                                      if m["token_id"] != tid]
                    existing_tokens.discard(tid)
                    changed = True
                # --- night market toggle ---
                want_night = bool(row["夜盘"])
                is_night   = tid in existing_night_tokens
                if want_night and not is_night:
                    cfg.setdefault("night_markets", []).append({
                        "token_id": tid,
                        "max_incentive_spread": round(item.get("maxIncentiveSpread", 3.5), 4),
                        "price_tick": 0.01,
                        "min_distance_from_best_bid": 0.02,
                        "quote_size": 80.0,
                        "risk": "low",
                        "enabled": True,
                    })
                    existing_night_tokens.add(tid)
                    changed = True
                elif not want_night and is_night:
                    cfg["night_markets"] = [m for m in cfg.get("night_markets", [])
                                            if m["token_id"] != tid]
                    existing_night_tokens.discard(tid)
                    changed = True
            if changed:
                save_config(cfg)
                st.rerun()

            # ── Replace all button ────────────────────────────────────────────
            st.markdown("")
            btn_col1, btn_col2 = st.columns(2)
            with btn_col1:
                if st.button("应用到 Config（替换全部旧市场）", type="primary"):
                    checked_items = [
                        item for i, (row, item) in enumerate(
                            zip(edited.itertuples(), table_results)
                        )
                        if row._1  # "In Config" is first column
                    ]
                    new_markets = [{
                        "token_id": item.get("token_id", ""),
                        "max_incentive_spread": round(item.get("maxIncentiveSpread", 3.5), 4),
                        "price_tick": 0.01,
                        "min_distance_from_best_bid": 0.01,
                        "quote_size": 100.0,
                        "risk": "low" if item.get("quadrant", "").startswith("A") else "mid",
                        "enabled": True,
                    } for item in checked_items if item.get("token_id")]
                    cfg["markets"] = new_markets
                    save_config(cfg)
                    st.success(f"已替换：写入 {len(new_markets)} 个市场，旧配置已清除。")
                    st.rerun()
            with btn_col2:
                if st.button("应用到夜盘 Config（替换全部旧夜盘市场）", type="secondary"):
                    checked_night = []
                    for i, row in edited.iterrows():
                        if bool(row["夜盘"]):
                            item = json.loads(df_edit.at[i, "_item"])
                            if item.get("token_id"):
                                checked_night.append({
                                    "token_id": item["token_id"],
                                    "max_incentive_spread": round(item.get("maxIncentiveSpread", 3.5), 4),
                                    "price_tick": 0.01,
                                    "min_distance_from_best_bid": 0.02,
                                    "quote_size": 80.0,
                                    "risk": "low",
                                    "enabled": True,
                                })
                    cfg["night_markets"] = checked_night
                    save_config(cfg)
                    st.success(f"夜盘已更新：写入 {len(checked_night)} 个市场。")
                    st.rerun()

            # ── Night market summary ───────────────────────────────────────────
            night_selected = [df_edit.at[i, "Market"] for i, row in edited.iterrows()
                              if bool(row.get("夜盘", False))]
            night_cfg_count = len(cfg.get("night_markets", []))
            if night_selected or night_cfg_count:
                st.markdown(
                    f'<p class="section-title">已选夜盘市场'
                    f'&nbsp;&nbsp;<span style="color:#58a6ff">'
                    f'{len(night_selected)} 个已勾选 / {night_cfg_count} 个在 config'
                    f'</span></p>',
                    unsafe_allow_html=True,
                )
                if night_selected:
                    for idx, name in enumerate(night_selected, 1):
                        st.markdown(f"&ensp;{idx}. {name}")
                else:
                    st.caption("当前表格中无勾选夜盘，config 中已有的夜盘市场不在本次 scan 结果中。")

        except ImportError:
            st.warning("pip install plotly  to enable scatter chart.")


# ══ TAB: PROXY ════════════════════════════════════════════════════════════════
with tab_proxy:
    proxy_cfg    = cfg.get("proxy_pool", {})
    current_list = proxy_cfg.get("proxies", [])

    st.markdown(
        f'<p class="section-title">Proxy Pool — {len(current_list)} proxies loaded</p>',
        unsafe_allow_html=True,
    )

    # bulk import
    with st.expander("Bulk Import (one per line: host:port or host:port:user:pass)"):
        raw_input = st.text_area("Paste proxies", height=180, placeholder="1.2.3.4:8080\n1.2.3.5:8080:user:pass")
        col_imp, col_rep = st.columns(2)
        with col_imp:
            if st.button("Append to config"):
                new_proxies = [line.strip() for line in raw_input.strip().splitlines() if line.strip()]
                existing_set = set(current_list)
                added = [p for p in new_proxies if p not in existing_set]
                current_list.extend(added)
                proxy_cfg["proxies"] = current_list
                cfg["proxy_pool"] = proxy_cfg
                save_config(cfg)
                st.success(f"Added {len(added)} proxies ({len(new_proxies)-len(added)} duplicates skipped).")
                st.rerun()
        with col_rep:
            if st.button("Replace all"):
                new_proxies = [line.strip() for line in raw_input.strip().splitlines() if line.strip()]
                proxy_cfg["proxies"] = new_proxies
                cfg["proxy_pool"] = proxy_cfg
                save_config(cfg)
                st.success(f"Replaced with {len(new_proxies)} proxies.")
                st.rerun()

    # connectivity test
    with st.expander("Connectivity Test (sample 5 random proxies)"):
        import random
        sample = random.sample(current_list, min(5, len(current_list))) if current_list else []
        if st.button("Run Test", key="proxy_test"):
            results = []
            for p in sample:
                ok = test_proxy(p)
                results.append({"Proxy": p[:40], "Status": "OK" if ok else "FAIL"})
            df_p = pd.DataFrame(results)
            def color_proxy_status(val):
                return "color:#3fb950" if val == "OK" else "color:#f85149"
            st.dataframe(
                df_p.style.map(color_proxy_status, subset=["Status"])
                    .set_properties(**{"background-color": "#0d1117", "color": "#e6edf3"}),
                use_container_width=True, hide_index=True,
            )

    # current proxy list preview
    if current_list:
        with st.expander(f"View all {len(current_list)} proxies"):
            masked = [p.split(":")[0] + ":***" for p in current_list]
            st.code("\n".join(masked), language="text")


# ══ TAB: CONTROL ═════════════════════════════════════════════════════════════
with tab_control:
    col_eng, col_key = st.columns([1, 1])

    with col_eng:
        st.markdown('<p class="section-title">Engine Control</p>', unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Start", use_container_width=True):
                st.info(start_engine())
        with c2:
            if st.button("Stop", use_container_width=True):
                st.info(stop_engine())
        with c3:
            if st.button("Reload", use_container_width=True):
                stop_engine()
                time.sleep(0.5)
                st.info(start_engine())

        st.markdown("")
        pid = engine_pid()
        st.markdown(
            f"PID: `{pid}`  |  Status: "
            + ('<span class="pill-green">RUNNING</span>' if running else '<span class="pill-red">STOPPED</span>'),
            unsafe_allow_html=True,
        )

    with col_key:
        st.markdown('<p class="section-title">Test Mode — Local Private Key</p>', unsafe_allow_html=True)
        st.caption("Stored in session memory only. Never written to disk. For testing without Mac Mini signer.")

        key_input = st.text_input(
            "Private Key (0x...)",
            value=st.session_state.get("test_private_key", ""),
            type="password",
            placeholder="0x...",
            key="key_input_field",
        )
        c1, c2 = st.columns(2)
        with c1:
            if st.button("Set Key (session)", use_container_width=True):
                st.session_state["test_private_key"] = key_input.strip()
                st.success("Key loaded into session.")
        with c2:
            if st.button("Clear Key", use_container_width=True):
                st.session_state["test_private_key"] = ""
                st.success("Key cleared.")

        if st.session_state.get("test_private_key"):
            st.markdown('<span class="pill-green">LOCAL KEY ACTIVE</span>', unsafe_allow_html=True)
        elif os.getenv("POLY_PRIVATE_KEY"):
            st.markdown('<span class="pill-yellow">ENV KEY ACTIVE</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="pill-gray">NO KEY — API calls disabled</span>', unsafe_allow_html=True)

        # future: remote signer status
        st.markdown("")
        signer_url = acc.get("signer_server_url", "")
        if signer_url:
            st.markdown(
                f'Remote Signer: <span class="pill-gray">{signer_url}</span> '
                f'<span class="pill-yellow">not tested</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<span class="pill-gray">Remote Signer: not configured</span>',
                        unsafe_allow_html=True)

    st.divider()
    st.markdown('<p class="section-title">Engine Log (last 60 lines)</p>', unsafe_allow_html=True)
    log_text = tail_log(60)
    st.code(log_text, language="text")
    if st.button("Refresh Log"):
        st.rerun()

# ══ TAB: ACCOUNTS (D-6 multi-account aggregated view) ════════════════════════
with tab_accounts:
    all_states = load_all_engine_states()
    alive_map  = multi_engine_running()

    st.markdown('<p class="section-title">Multi-Account Status</p>', unsafe_allow_html=True)

    if not all_states:
        st.info(
            "No engine state files found. "
            "Start multi_runner.py with config_1.json, config_2.json, ... "
            "or start a single-account engine to see state here."
        )
    else:
        acc_rows = []
        for acc_id in sorted(all_states.keys()):
            s = all_states[acc_id]
            label = f"Account {acc_id}" if acc_id > 0 else "Account (single)"
            cfg_name = f"config_{acc_id}.json" if acc_id > 0 else "config.json"

            is_running = alive_map.get(acc_id, False)
            _raw_ts = s.get("ts", "—")
            try:
                if _raw_ts and _raw_ts != "—":
                    _utc_dt = datetime.strptime(_raw_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                    state_ts = _utc_dt.astimezone(_BJT).strftime("%Y-%m-%d %H:%M:%S")
                else:
                    state_ts = "—"
            except Exception:
                state_ts = _raw_ts
            balance    = s.get("balance")
            n_markets  = len(s.get("markets", {}))
            n_orders   = sum(
                len(ms.get("orders", []))
                for ms in s.get("markets", {}).values()
            )
            n_active   = sum(
                1 for ms in s.get("markets", {}).values()
                if ms.get("status") == "active"
            )
            n_banned   = len(s.get("banned_tokens", []))
            fills_24h  = sum(
                1 for f in s.get("fills", [])
                if _now_unix - float(f.get("ts", 0) or 0) < 86400
            )
            pending_uw = len(s.get("pending_unwinds", []))
            cooldown   = s.get("cooldown_active", False)

            status_str = "RUNNING" if is_running else "STOPPED"
            acc_rows.append({
                "Account":     label,
                "Config":      cfg_name,
                "Status":      status_str,
                "Balance $":   f"{balance:,.2f}" if balance is not None else "—",
                "Markets":     n_markets,
                "Active":      n_active,
                "Orders":      n_orders,
                "Fills 24h":   fills_24h,
                "Unwinds":     pending_uw,
                "Banned":      n_banned,
                "Cooldown":    "YES" if cooldown else "—",
                "State TS":    state_ts,
            })

        df_acc = pd.DataFrame(acc_rows)

        def acc_color_status(val: str) -> str:
            return "color:#3fb950; font-weight:600" if val == "RUNNING" else "color:#f85149"

        def acc_color_cooldown(val: str) -> str:
            return "color:#d29922; font-weight:600" if val == "YES" else ""

        st.dataframe(
            df_acc.style
                .map(acc_color_status, subset=["Status"])
                .map(acc_color_cooldown, subset=["Cooldown"])
                .set_properties(**{"background-color": "#0d1117", "color": "#e6edf3"}),
            use_container_width=True,
            hide_index=True,
        )

    # Multi-runner control
    st.markdown('<p class="section-title" style="margin-top:24px">Multi-Runner Control</p>',
                unsafe_allow_html=True)

    st.caption(
        "multi_runner.py auto-discovers config_1.json ... config_30.json in the maker directory. "
        "Copy config.json to config_1.json, config_2.json, etc. and set different private_key per file."
    )

    col_mr1, col_mr2 = st.columns(2)
    with col_mr1:
        if st.button("Start Multi-Runner", use_container_width=True):
            if not MULTI_RUNNER_PATH.exists():
                st.error("multi_runner.py not found.")
            else:
                multi_log = DATA_DIR / "multi_runner.log"
                multi_log.touch(exist_ok=True)
                multi_pid_path = DATA_DIR / ".multi_runner.pid"
                flags = subprocess.CREATE_NEW_PROCESS_GROUP if platform.system() == "Windows" else 0
                env = os.environ.copy()
                test_key = st.session_state.get("test_private_key", "").strip()
                if test_key:
                    env["POLY_PRIVATE_KEY"] = test_key
                with multi_log.open("a", encoding="utf-8") as lf:
                    proc = subprocess.Popen(
                        [sys.executable, str(MULTI_RUNNER_PATH)],
                        cwd=str(BASE_DIR),
                        stdout=lf, stderr=lf,
                        creationflags=flags,
                        env=env,
                    )
                multi_pid_path.write_text(str(proc.pid), encoding="utf-8")
                st.success(f"Multi-runner started — PID {proc.pid}")

    with col_mr2:
        if st.button("Stop Multi-Runner", use_container_width=True):
            multi_pid_path = DATA_DIR / ".multi_runner.pid"
            if multi_pid_path.exists():
                try:
                    pid = int(multi_pid_path.read_text(encoding="utf-8").strip())
                    if platform.system() == "Windows":
                        subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                                       capture_output=True, check=False)
                    else:
                        import signal as _signal
                        os.kill(pid, _signal.SIGTERM)
                    multi_pid_path.unlink(missing_ok=True)
                    st.success(f"Multi-runner PID {pid} stopped.")
                except Exception as e:
                    st.error(f"Stop failed: {e}")
            else:
                st.warning("No multi-runner PID file found.")

    # Multi-runner log tail
    multi_log_path = DATA_DIR / "multi_runner.log"
    if multi_log_path.exists():
        with st.expander("Multi-Runner Log (last 40 lines)"):
            lines = multi_log_path.read_text(encoding="utf-8", errors="replace").splitlines()
            st.code("\n".join(lines[-40:]), language="text")

    # Config file setup helper
    st.markdown('<p class="section-title" style="margin-top:24px">Config Setup</p>',
                unsafe_allow_html=True)
    st.caption("Existing config files in maker directory:")
    import glob as _glob
    cfgs_found = sorted(_glob.glob(str(MAKER_DIR / "config*.json")))
    if cfgs_found:
        st.code("\n".join(Path(p).name for p in cfgs_found), language="text")
    else:
        st.info("No config*.json files found.")


# ── auto-refresh disabled ──────────────────────────────────────────────────────
# st_autorefresh(interval=10000, key="auto_refresh")

st.markdown(
    "<p style='color:#30363d; font-size:11px; text-align:right; margin-top:24px;'>"
    "Latitude Alpha v2 — 2026</p>",
    unsafe_allow_html=True,
)
