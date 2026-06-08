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
import ast
import sqlite3
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
SESSION_CONFIRM_PATH = DATA_DIR / "session_confirm.json"
MULTI_RUNNER_PATH   = MAKER_DIR / "multi_runner.py"
REMOTE_ACCOUNTS_PATH = REPO_DIR / "dashboard" / "remote_accounts.json"
VAR_DECIBEL_DIR      = Path(os.getenv("VAR_DECIBEL_HEDGE_BOT_DIR", str(REPO_DIR.parent / "var_decibel_hedge_bot")))
VAR_DECIBEL_CONFIG_PATH = VAR_DECIBEL_DIR / "config.yaml"

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
.muted-note { color:#8b949e; font-size:12px; line-height:1.55; }
.var-panel {
    background:#161b22;
    border:1px solid #30363d;
    border-radius:8px;
    padding:14px 16px;
    min-height:92px;
}
.var-panel-title {
    color:#8b949e;
    font-size:11px;
    letter-spacing:.08em;
    text-transform:uppercase;
    margin-bottom:6px;
}
.var-panel-value { color:#e6edf3; font-size:20px; font-weight:650; }
.var-panel-small { color:#8b949e; font-size:12px; margin-top:4px; }
.var-block {
    border:1px solid #30363d;
    border-radius:8px;
    padding:14px 16px;
    background:#0d1117;
}

/* emergency button */
div[data-testid="stButton"] button[kind="primary"] {
    background: #da3633 !important;
    border-color: #f85149 !important;
    color: #fff !important;
    font-weight: 700;
}

/* dataframe */
div[data-testid="stDataFrame"] { border: 1px solid #30363d; border-radius: 6px; }

/* ── Log Monitor Panel ─────────────────────────────────────────── */
.log-panel {
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 8px;
    max-height: 600px;
    overflow-y: auto;
    padding: 0;
    font-family: 'JetBrains Mono', 'Cascadia Code', 'Consolas', monospace;
    font-size: 12.5px;
    scroll-behavior: smooth;
}
.log-entry {
    display: flex;
    align-items: flex-start;
    padding: 6px 14px;
    border-bottom: 1px solid #161b22;
    gap: 10px;
    line-height: 1.5;
}
.log-entry:hover { background: #161b22; }
.log-time {
    color: #484f58;
    font-size: 11px;
    white-space: nowrap;
    min-width: 55px;
    padding-top: 1px;
}
.log-tag {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: .03em;
    border-radius: 3px;
    padding: 1px 7px;
    white-space: nowrap;
    min-width: 52px;
    text-align: center;
}
.log-msg { color: #c9d1d9; flex: 1; word-break: break-word; }
.log-detail { color: #484f58; font-size: 11px; margin-top: 2px; word-break: break-all; }

/* tag colors */
.tag-success  { background: #1a3a1a; color: #3fb950; border: 1px solid #238636; }
.tag-info     { background: #161b22; color: #8b949e; border: 1px solid #30363d; }
.tag-warning  { background: #2d2a1a; color: #d29922; border: 1px solid #9e6a03; }
.tag-danger   { background: #3a1a1a; color: #f85149; border: 1px solid #da3633; }
.tag-cooldown { background: #1a2a3a; color: #58a6ff; border: 1px solid #1f6feb; }

/* summary bar */
.log-summary {
    display: flex;
    gap: 16px;
    align-items: center;
    padding: 10px 16px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    margin-bottom: 10px;
    font-size: 12px;
    flex-wrap: wrap;
}
.log-summary-item {
    display: flex;
    align-items: center;
    gap: 5px;
}
.log-summary-dot {
    width: 8px; height: 8px;
    border-radius: 50%;
    display: inline-block;
}
.dot-green  { background: #3fb950; }
.dot-red    { background: #f85149; }
.dot-yellow { background: #d29922; }
.dot-blue   { background: #58a6ff; }
.dot-gray   { background: #484f58; }

/* new entry animation */
@keyframes logSlideIn {
    from { opacity: 0; transform: translateY(-6px); background: #1a2332; }
    to   { opacity: 1; transform: translateY(0);    background: transparent; }
}
.log-entry-new {
    animation: logSlideIn 0.6s ease-out;
}

/* live dot pulse */
@keyframes livePulse {
    0%, 100% { opacity: 1; }
    50%      { opacity: 0.3; }
}
.live-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: #3fb950;
    display: inline-block;
    animation: livePulse 1.5s ease-in-out infinite;
    margin-right: 6px;
    vertical-align: middle;
}
</style>
""", unsafe_allow_html=True)


# ── flash message helper (survives st.rerun) ──────────────────────────────────
def _flash(msg: str, level: str = "info") -> None:
    """Store a message in session_state so it displays after rerun."""
    st.session_state["_flash"] = (msg, level)

def _show_flash() -> None:
    """Display and clear any pending flash message."""
    flash = st.session_state.pop("_flash", None)
    if flash:
        msg, level = flash
        getattr(st, level, st.info)(msg)


# ── config helpers ─────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(
        json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _is_public_access() -> bool:
    """True when dashboard accessed via the public Cloudflare URL (nginx sets
    X-Dashboard-Source: cloudflare). False for direct Tailscale access or local.

    Used to downgrade Cloudflare-exposed sessions to read-only for config
    writes (save markets, proxy pool, auto_curator toggle) so an attacker who
    got the public URL + basic auth password cannot inject markets that the
    engine would then sign. Process-control actions (start/stop/pause/resume)
    are still allowed from public per Kevin 2026-04-24.
    """
    try:
        hdr = st.context.headers.get("X-Dashboard-Source", "")
        return str(hdr).lower() == "cloudflare"
    except Exception:
        return False


def _gate_write(action: str) -> bool:
    """Return True when the write is authorized in the current session.
    Currently always True — Kevin 2026-04-24 chose convenience (scan markets
    over the fast Cloudflare path) over the tailscale-only write restriction,
    judging the attack value-at-risk as below the usability cost of slow DERP.
    Keep the function here so we can re-enable a gate later without touching
    call sites.
    """
    return True


def _load_account_config() -> tuple[dict, dict]:
    cfg = load_config()
    return cfg, cfg.get("account", {})


@st.cache_resource(ttl=600)
def _build_remote_signer_client():
    """Cached (10 min) — building this does a Tailscale-round-trip to the Mac
    Mini signer for `derive_creds`, adding ~100-300 ms to every call that
    needs a ClobClient (balance, orders, rewards). Caching across reruns keeps
    the dashboard snappy from remote devices."""
    cfg, acc = _load_account_config()
    signer_server_url = (
        os.getenv("POLY_SIGNER_SERVER_URL", "").strip()
        or str(acc.get("signer_server_url", "")).strip()
    )
    signer_token = (
        os.getenv("SIGNER_TOKEN", "").strip()
        or str(acc.get("signer_token", "")).strip()
    )
    if not signer_server_url or not signer_token:
        return None, None, cfg, acc, "Remote signer is not configured."

    if str(REPO_DIR) not in sys.path:
        sys.path.insert(0, str(REPO_DIR))

    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from platforms.polymarket.maker.remote_signer import AddressStub, BuilderStub, RemoteSignerClient

    host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(acc.get("chain_id", 137))
    signature_type = int(acc.get("signature_type", 0))
    funder = acc.get("funder")

    signer = RemoteSignerClient(signer_server_url, signer_token)
    creds = signer.derive_creds()
    client = ClobClient(host=host, chain_id=chain_id)
    client.signer = AddressStub(creds["address"], chain_id)
    client.builder = BuilderStub(sig_type=signature_type, funder=funder)
    client.set_api_creds(ApiCreds(
        api_key=creds["api_key"],
        api_secret=creds["api_secret"],
        api_passphrase=creds["api_passphrase"],
    ))
    return client, signer, cfg, acc, None


def _clear_runtime_caches() -> None:
    for fn in (fetch_balance_info, fetch_open_orders, load_engine_state, load_all_engine_states, _fetch_rewards_for_account):
        clear = getattr(fn, "clear", None)
        if callable(clear):
            clear()
    st.session_state.pop("_rewards_loaded", None)


def _decode_log_bytes(raw: bytes) -> str:
    for encoding in ("utf-8", "utf-8-sig", "gb18030", "cp936"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


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
        # Use ctypes instead of slow tasklist subprocess
        import ctypes
        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return exit_code.value == STILL_ACTIVE
            return False
        finally:
            kernel32.CloseHandle(handle)
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
    try:
        _, _, _, _, signer_error = _build_remote_signer_client()
    except Exception as exc:
        return f"Engine start blocked: signer server error — {exc}"
    if signer_error:
        return "Engine start blocked: configure the remote signer first."
    LOG_PATH.touch(exist_ok=True)
    flags = subprocess.CREATE_NEW_PROCESS_GROUP if platform.system() == "Windows" else 0
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    with LOG_PATH.open("a", encoding="utf-8") as lf:
        proc = subprocess.Popen(
            [sys.executable, "-X", "utf8", str(ENGINE_PATH)],
            cwd=str(BASE_DIR),
            stdout=lf,
            stderr=lf,
            creationflags=flags,
            env=env,
        )
    PID_PATH.write_text(str(proc.pid), encoding="utf-8")
    _clear_runtime_caches()
    return f"Engine started. PID {proc.pid}"


def stop_engine() -> str:
    pid = engine_pid()
    msg = emergency_cancel_all()
    if pid:
        if platform.system() == "Windows":
            subprocess.run(["taskkill", "/PID", str(pid)],
                           capture_output=True, check=False)
            for _ in range(20):
                if not _pid_alive(pid):
                    break
                time.sleep(0.25)
            if _pid_alive(pid):
                subprocess.run(["taskkill", "/PID", str(pid), "/F"],
                               capture_output=True, check=False)
        else:
            try:
                import signal
                os.kill(pid, signal.SIGTERM)
            except Exception:
                pass

        if _pid_alive(pid):
            return f"Engine stop incomplete. {msg}"

    try:
        PID_PATH.unlink()
    except Exception:
        pass
    _clear_runtime_caches()
    return f"Engine stopped. {msg}"


def _is_multi_mode() -> bool:
    """Detect multi-account mode: True if config_1.json or higher exists."""
    import glob as _g
    multi_cfgs = _g.glob(str(MAKER_DIR / "config_*.json"))
    return len(multi_cfgs) > 0


# ── Per-account pause flag (Phase B soft-pause) ────────────────────────────────
# A flag file `.account_N.paused` in data/ signals the engine to cancel open
# orders and skip quoting for account N without stopping the process.

def _pause_flag_path(acc_id: int) -> Path:
    return DATA_DIR / f".account_{acc_id}.paused"


def _is_account_paused(acc_id: int) -> bool:
    return _pause_flag_path(acc_id).exists()


def _set_account_paused(acc_id: int, paused: bool) -> None:
    p = _pause_flag_path(acc_id)
    if paused:
        p.touch(exist_ok=True)
    else:
        try:
            p.unlink()
        except FileNotFoundError:
            pass


def _load_remote_accounts() -> dict[int, dict]:
    """Map of account_id → {ssh_host, ssh_key, systemd_unit} for accounts that
    run on a different VPS than this dashboard. Lets Start/Stop buttons SSH to
    the correct host instead of trying (and silently failing) locally.
    """
    if not REMOTE_ACCOUNTS_PATH.exists():
        return {}
    try:
        raw = json.loads(REMOTE_ACCOUNTS_PATH.read_text(encoding="utf-8"))
        return {int(k): v for k, v in raw.items() if isinstance(v, dict)}
    except Exception:
        return {}


def _remote_systemctl(acc_id: int, action: str) -> str:
    remotes = _load_remote_accounts()
    if acc_id not in remotes:
        return f"acc{acc_id}: not remote"
    r = remotes[acc_id]
    cmd = [
        "ssh", "-i", str(r["ssh_key"]),
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", "ConnectTimeout=8",
        str(r["ssh_host"]),
        f"sudo -n systemctl {action} {r['systemd_unit']}",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            return f"acc{acc_id} {action}:OK"
        err = (result.stderr or result.stdout or "").strip()[:60]
        return f"acc{acc_id} {action}:rc={result.returncode} {err}"
    except subprocess.TimeoutExpired:
        return f"acc{acc_id} {action}:timeout"
    except Exception as e:
        return f"acc{acc_id} {action}:{type(e).__name__}({str(e)[:30]})"


def _configured_account_ids(max_n: int = 30) -> list[int]:
    local = [i for i in range(1, max_n + 1) if (MAKER_DIR / f"config_{i}.json").exists()]
    remote = list(_load_remote_accounts().keys())
    return sorted(set(local + remote))


def _multi_runner_pid() -> int | None:
    # Prefer .multi_runner.pid (legacy multi_runner.py model). Fall back to
    # .engine_1.pid which polymarket-engine.service writes via ExecStartPost.
    for name in (".multi_runner.pid", ".engine_1.pid"):
        p = DATA_DIR / name
        if p.exists():
            try:
                return int(p.read_text(encoding="utf-8").strip())
            except Exception:
                continue
    return None


def _multi_runner_running() -> bool:
    # Optimistic UI: if Start was clicked very recently, treat as running so
    # the badge flips to RUNNING immediately instead of waiting for engine to
    # write its first heartbeat. Cleared as soon as a real heartbeat appears.
    try:
        _just = st.session_state.get("_engine_just_started_at", 0.0)
    except Exception:
        _just = 0.0
    if _just and (time.time() - _just) < 30.0:
        return True

    pid = _multi_runner_pid()
    if pid and _pid_alive(pid):
        return True
    # Fallback: any *local* engine heartbeat fresh. Remote accounts (whose
    # heartbeats arrive via rsync) are excluded — otherwise stop_multi_runner
    # could appear to leave the runner running because account 2's rsynced
    # heartbeat is still recent, breaking the Reload pattern.
    remote_ids = set(_load_remote_accounts().keys())
    alive = multi_engine_running()
    return any(v for k, v in alive.items() if k not in remote_ids)


LOCAL_ENGINE_UNIT = "polymarket-engine.service"


def start_multi_runner() -> str:
    """Start the local engine via systemd (polymarket-engine.service). Using
    systemd instead of subprocess.Popen means the engine survives dashboard
    restarts (which would otherwise SIGTERM child processes via the streamlit
    cgroup)."""
    if _multi_runner_running():
        return "Engine already running."
    cmd = ["sudo", "-n", "systemctl", "start", LOCAL_ENGINE_UNIT]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        if result.returncode == 0:
            _clear_runtime_caches()
            # Optimistic-UI marker: read by _multi_runner_running so the
            # status badge flips to RUNNING immediately instead of lingering
            # on STOPPED for the ~3-5s the engine takes to write its first
            # heartbeat after systemd start returns.
            try:
                st.session_state["_engine_just_started_at"] = time.time()
            except Exception:
                pass
            return f"Engine started via systemd ({LOCAL_ENGINE_UNIT})"
        err = (result.stderr or result.stdout or "").strip()[:80]
        return f"Engine start failed: rc={result.returncode} {err}"
    except subprocess.TimeoutExpired:
        return "Engine start: systemctl timeout"
    except Exception as e:
        return f"Engine start: {type(e).__name__}({e})"


def stop_multi_runner() -> str:
    # Cancel each account's open orders FIRST (the engine process is about to
    # be killed, so it won't get a chance to clean up via its own pause/exit
    # handlers). Do this best-effort per-account — one failure doesn't block
    # the others or the process kill.
    cancel_summary: list[str] = []
    for _acc_id in _configured_account_ids():
        _cfg_path = MAKER_DIR / f"config_{_acc_id}.json"
        if not _cfg_path.exists():
            continue
        try:
            _client, _addr, _err = _build_client_for_config(_cfg_path)
            if _err:
                cancel_summary.append(f"acc{_acc_id}:skip({_err[:40]})")
                continue
            _client.cancel_all()
            cancel_summary.append(f"acc{_acc_id}:cancelled")
        except Exception as _exc:
            cancel_summary.append(f"acc{_acc_id}:fail({type(_exc).__name__})")

    # Stop the systemd unit; engine receives SIGTERM and graceful-cancels its
    # own orders before exiting (REST cancel above is belt-and-suspenders).
    cmd = ["sudo", "-n", "systemctl", "stop", LOCAL_ENGINE_UNIT]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        if result.returncode == 0:
            stop_msg = f"Engine stopped via systemd ({LOCAL_ENGINE_UNIT})"
        else:
            err = (result.stderr or result.stdout or "").strip()[:60]
            stop_msg = f"systemctl stop rc={result.returncode} {err}"
    except subprocess.TimeoutExpired:
        stop_msg = "systemctl stop: timeout"
    except Exception as e:
        stop_msg = f"systemctl stop: {type(e).__name__}({e})"

    # Cleanup pid + heartbeat files. Removing the heartbeat is essential
    # for the Reload pattern (stop → sleep → start) to work — otherwise
    # _multi_runner_running()'s heartbeat-fallback still reports the engine
    # as alive within ~10s of stop, and start_multi_runner() bails with
    # "already running".
    for name in (".multi_runner.pid", ".engine_1.pid", ".engine_1.heartbeat",
                 ".engine.pid", ".engine.heartbeat"):
        try:
            (DATA_DIR / name).unlink(missing_ok=True)
        except Exception:
            pass
    _clear_runtime_caches()
    _cancel_line = " | ".join(cancel_summary) if cancel_summary else "no accounts"
    return f"{stop_msg}. Orders: {_cancel_line}"


def emergency_cancel_all() -> str:
    try:
        client, _, _, _, err = _build_remote_signer_client()
        if err:
            return f"cancel_all skipped: {err}"
        client.cancel_all()
        _clear_runtime_caches()
        return "cancel_all OK (remote signer)."
    except Exception as exc:
        return f"cancel_all failed: {exc.__class__.__name__}: {exc}"


@st.cache_data(ttl=15)
def fetch_balance_info(host: str, key: str, chain_id: int, sig_type: int, funder: str | None) -> dict:
    """Returns {balance, allowance, error}."""
    del host, key, chain_id, sig_type, funder
    try:
        client, _, _, _, err = _build_remote_signer_client()
        if err:
            return {"balance": None, "allowance": None, "error": err}
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
        resp = client.get_balance_allowance(
            params=BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
        return {"balance": resp.get("balance", "0"), "allowance": resp.get("allowance", "0")}
    except Exception as exc:
        return {"balance": None, "allowance": None, "error": f"{exc.__class__.__name__}: {exc}"}


@st.cache_data(ttl=15)
def fetch_open_orders(host: str, key: str, chain_id: int, sig_type: int, funder: str | None) -> list[dict]:
    del host, key, chain_id, sig_type, funder
    try:
        client, _, _, _, err = _build_remote_signer_client()
        if err:
            return []
        from py_clob_client.clob_types import OpenOrderParams
        orders = client.get_orders(OpenOrderParams())
        return orders if isinstance(orders, list) else []
    except Exception:
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


def _all_config_token_ids() -> tuple[str, ...]:
    """Union of token_ids across config.json + config_1..30.json + every
    engine_state_*.json. Engine state is included because auto_curator runtime
    additions and dual-side NO-token injections never land in config files,
    so names would otherwise fall back to the raw 70-digit id on the Markets
    tab.
    """
    tids: set[str] = set()
    for p in [CONFIG_PATH] + [MAKER_DIR / f"config_{i}.json" for i in range(1, 31)]:
        if not p.exists():
            continue
        try:
            cfg = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for m in cfg.get("markets", []) or []:
            tid = str(m.get("token_id", "") or "")
            if tid:
                tids.add(tid)
            ptid = str(m.get("paired_token_id", "") or "")
            if ptid:
                tids.add(ptid)
    # Pull in tokens the engine knows about at runtime (dual-side injects,
    # auto_curator runtime adds, etc.) via engine_state_*.json.
    for i in range(0, 31):
        p = DATA_DIR / ("engine_state.json" if i == 0 else f"engine_state_{i}.json")
        if not p.exists():
            continue
        try:
            s = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for tid in (s.get("markets", {}) or {}).keys():
            if tid:
                tids.add(str(tid))
    return tuple(sorted(tids))


def resolve_market_name(token_id: str) -> str:
    """Single token lookup — uses batch cache internally."""
    cfg_tids = _all_config_token_ids()
    if token_id and token_id not in cfg_tids:
        cfg_tids = tuple(sorted(set(cfg_tids) | {token_id}))
    names = resolve_market_names_batch(cfg_tids)
    return names.get(token_id, token_id[:16] + "...")


@st.cache_resource(ttl=600)
def _build_client_for_config(config_path: Path):
    """Build a ClobClient + address from any config_N.json file.
    Returns (client, address, error_str).

    Cached per config path (10 min) to avoid the Tailscale signer round-trip
    on every render — that's ~100-300 ms per account that was stacking up on
    the Control tab (rewards fetch + stop handler + status bar).
    """
    try:
        cfg = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        return None, None, f"Cannot read {config_path.name}: {e}"
    acc = cfg.get("account", {})
    signer_url = str(acc.get("signer_server_url", "")).strip()
    signer_token = str(acc.get("signer_token", "")).strip()
    if not signer_url or not signer_token:
        return None, None, f"{config_path.name}: no signer configured"

    if str(REPO_DIR) not in sys.path:
        sys.path.insert(0, str(REPO_DIR))
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from platforms.polymarket.maker.remote_signer import AddressStub, BuilderStub, RemoteSignerClient

    host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(acc.get("chain_id", 137))
    sig_type = int(acc.get("signature_type", 0))
    funder = acc.get("funder")

    # Pass funder so the multi-key signer routes the request to the right key
    # — without it the signer would 400 whenever more than one funder is loaded.
    signer = RemoteSignerClient(signer_url, signer_token, funder=funder or None)
    creds = signer.derive_creds()
    address = creds["address"]
    client = ClobClient(host=host, chain_id=chain_id)
    client.signer = AddressStub(address, chain_id)
    client.builder = BuilderStub(sig_type=sig_type, funder=funder)
    client.set_api_creds(ApiCreds(
        api_key=creds["api_key"],
        api_secret=creds["api_secret"],
        api_passphrase=creds["api_passphrase"],
    ))
    return client, address, None


# Polymarket taker fee on the CTF Exchange. Currently ~0 on most sports
# markets, but exposed as a config knob so we can dial it in if accounting
# diverges from on-chain settlement. Maker side pays nothing.
_TAKER_FEE_PCT = 0.0


@st.cache_data(ttl=300, show_spinner=False)
def _fetch_realized_pnl_for_funder(funder: str, window_sec: int = 86400) -> dict:
    """Compute realized fill P&L from Polymarket data-api trade history.
    Returns {'benefit_usd': X, 'loss_usd': Y, 'count_24h': N, 'positions_closed_24h': M, 'fee_paid': F, 'error': None|msg}.

    Method: pull the user's trades, group by asset (token_id). Within each
    group, order by timestamp; pair BUYs against SELLs (FIFO) to compute
    realized P&L per close. Net positive across closes in the window is
    Benefit; net negative magnitude is Loss. Only counts CLOSED legs whose
    SELL ts is within the window — open positions don't show here (they're
    in the position table).
    """
    import requests as _req
    if not funder:
        return {"benefit_usd": 0.0, "loss_usd": 0.0, "count_24h": 0, "positions_closed_24h": 0, "fee_paid": 0.0, "error": "no_funder"}
    out = {"benefit_usd": 0.0, "loss_usd": 0.0, "count_24h": 0, "positions_closed_24h": 0, "fee_paid": 0.0, "error": None}
    try:
        all_trades: list = []
        for offset in range(0, 5000, 500):
            r = _req.get(
                f"https://data-api.polymarket.com/trades?user={funder}&limit=500&offset={offset}",
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                timeout=8,
            )
            if not r.ok:
                break
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            all_trades.extend(batch)
            if len(batch) < 500:
                break
    except Exception as e:
        out["error"] = f"fetch_failed:{type(e).__name__}"
        return out

    # Group by asset (token_id), order chronologically within each group
    from collections import defaultdict
    by_asset: dict[str, list] = defaultdict(list)
    for t in all_trades:
        a = t.get("asset")
        if not a:
            continue
        by_asset[a].append(t)
    for a in by_asset:
        by_asset[a].sort(key=lambda t: float(t.get("timestamp", 0) or 0))

    # FIFO pair within each asset group
    cutoff = time.time() - window_sec
    realized_in_window: list[tuple[float, float]] = []  # (sell_ts, pnl)
    fee_paid_total = 0.0
    for asset, trades in by_asset.items():
        # FIFO queue of (size_remaining, cost_per_share)
        buy_queue: list[list[float]] = []  # each entry [remaining_size, price]
        for t in trades:
            try:
                sz = float(t.get("size", 0) or 0)
                px = float(t.get("price", 0) or 0)
                ts = float(t.get("timestamp", 0) or 0)
                side = (t.get("side") or "").upper()
            except Exception:
                continue
            if sz <= 0 or px <= 0:
                continue
            # Per-side fee (taker only — Polymarket maker side is fee-free).
            # We don't always know maker/taker from the data-api response,
            # so apply the configured rate uniformly. Set _TAKER_FEE_PCT=0 to disable.
            fee = px * sz * _TAKER_FEE_PCT
            if side == "BUY":
                # Cost = price * size + fee  →  effective price = price + fee/size
                eff_px = px + (fee / sz if sz > 0 else 0.0)
                buy_queue.append([sz, eff_px])
                fee_paid_total += fee
            elif side == "SELL":
                # Match against earliest BUYs (FIFO)
                remaining = sz
                trade_pnl = -fee  # subtract fee paid on this sell
                fee_paid_total += fee
                while remaining > 1e-9 and buy_queue:
                    bsz, bpx = buy_queue[0]
                    take = min(bsz, remaining)
                    trade_pnl += (px - bpx) * take
                    bsz -= take
                    remaining -= take
                    if bsz <= 1e-9:
                        buy_queue.pop(0)
                    else:
                        buy_queue[0][0] = bsz
                # If SELL exceeds buy_queue (shouldn't happen for our maker
                # strategy unless we manually shorted), treat as flat —
                # don't double count the unmatched remainder.
                if ts >= cutoff:
                    realized_in_window.append((ts, trade_pnl))

    out["positions_closed_24h"] = len(realized_in_window)
    for ts, pnl in realized_in_window:
        if pnl >= 0:
            out["benefit_usd"] += pnl
        else:
            out["loss_usd"] += -pnl
    out["fee_paid"] = fee_paid_total
    return out


@st.cache_data(ttl=120)
def _fetch_rewards_for_account(config_name: str, date_str: str) -> dict:
    """Fetch rewards data for one account. Returns dict with earnings, totals, percentages."""
    config_path = MAKER_DIR / config_name
    client, address, err = _build_client_for_config(config_path)
    if err:
        return {"error": err, "address": None}

    from py_clob_client.headers.headers import create_level_2_headers
    from py_clob_client.clob_types import RequestArgs
    from py_clob_client.http_helpers.helpers import get as clob_get

    host = client.host
    sig_type = int(json.loads(config_path.read_text(encoding="utf-8")).get("account", {}).get("signature_type", 0))
    result = {"address": address, "error": None}

    # 1) Total earnings for date
    try:
        req = RequestArgs(method="GET", request_path="/rewards/user/total")
        headers = create_level_2_headers(client.signer, client.creds, req)
        url = f"{host}/rewards/user/total?date={date_str}&signature_type={sig_type}"
        resp = clob_get(url, headers=headers)
        result["totals"] = resp if isinstance(resp, list) else []
    except Exception as e:
        result["totals"] = []
        result["totals_error"] = str(e)

    # 2) Per-market earnings
    try:
        req = RequestArgs(method="GET", request_path="/rewards/user/markets")
        headers = create_level_2_headers(client.signer, client.creds, req)
        all_market_earnings = []
        cursor = "MA=="
        for _ in range(10):  # max 10 pages
            url = f"{host}/rewards/user/markets?date={date_str}&signature_type={sig_type}&next_cursor={cursor}&page_size=500"
            resp = clob_get(url, headers=headers)
            if isinstance(resp, dict):
                all_market_earnings.extend(resp.get("data", []))
                cursor = resp.get("next_cursor", "LTE=")
                if cursor == "LTE=":
                    break
            else:
                break
        result["markets"] = all_market_earnings
    except Exception as e:
        result["markets"] = []
        result["markets_error"] = str(e)

    # 3) Reward percentages
    try:
        req = RequestArgs(method="GET", request_path="/rewards/user/percentages")
        headers = create_level_2_headers(client.signer, client.creds, req)
        url = f"{host}/rewards/user/percentages?signature_type={sig_type}"
        resp = clob_get(url, headers=headers)
        result["percentages"] = resp if isinstance(resp, dict) else {}
    except Exception as e:
        result["percentages"] = {}
        result["percentages_error"] = str(e)

    return result


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

def _try_redis_state() -> dict | None:
    """Try to load engine state from Redis event bus (faster than file I/O)."""
    try:
        sys.path.insert(0, str(MAKER_DIR))
        from event_bus import EventBus
        cfg_path = CONFIG_PATH
        if cfg_path.exists():
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            bus_cfg = cfg.get("event_bus", {})
            redis_url = os.environ.get("POLY_REDIS_URL", "").strip() or str(bus_cfg.get("redis_url", "")).strip()
            if redis_url and bus_cfg.get("enabled"):
                bus = EventBus(redis_url=redis_url, connect_timeout=1.0)
                return bus.get_state()
    except Exception:
        pass
    return None


@st.cache_data(ttl=5)
def load_engine_state() -> dict:
    redis_state = _try_redis_state()
    if redis_state:
        redis_state["_loaded_at"] = time.time()
        redis_state["_source"] = "redis"
        markets = redis_state.get("markets", {}) if isinstance(redis_state.get("markets"), dict) else {}
        global_protection_active = bool(redis_state.get("cooldown_active")) or any(
            str(m.get("event_state", "")).upper() == "HALTED_ON_FILL"
            for m in markets.values() if isinstance(m, dict)
        )
        redis_state["global_protection_active"] = global_protection_active
        redis_state["global_trading_enabled"] = not global_protection_active
        return redis_state
    if ENGINE_STATE_PATH.exists():
        try:
            s = json.loads(ENGINE_STATE_PATH.read_text(encoding="utf-8"))
            s["_loaded_at"] = time.time()
            markets = s.get("markets", {}) if isinstance(s.get("markets"), dict) else {}
            global_protection_active = bool(s.get("cooldown_active"))
            if not global_protection_active:
                global_protection_active = any(
                    str(m.get("event_state", "")).upper() == "HALTED_ON_FILL"
                    for m in markets.values()
                    if isinstance(m, dict)
                )
            s["global_protection_active"] = global_protection_active
            s["global_trading_enabled"] = not global_protection_active
            return s
        except Exception:
            pass
    return {
        "fills": [],
        "pending_unwinds": [],
        "banned_tokens": [],
        "latency_records": [],
        "markets": {},
        "global_protection_active": False,
        "global_trading_enabled": True,
    }

@st.cache_data(ttl=5)
def load_all_engine_states() -> dict[int, dict]:
    """Load engine_state_N.json for N=1..30, plus engine_state.json as account 0."""
    result: dict[int, dict] = {}
    # Single-account state
    if ENGINE_STATE_PATH.exists():
        try:
            s = json.loads(ENGINE_STATE_PATH.read_text(encoding="utf-8"))
            s["_loaded_at"] = time.time()
            markets = s.get("markets", {}) if isinstance(s.get("markets"), dict) else {}
            global_protection_active = bool(s.get("cooldown_active")) or any(
                str(m.get("event_state", "")).upper() == "HALTED_ON_FILL"
                for m in markets.values()
                if isinstance(m, dict)
            )
            s["global_protection_active"] = global_protection_active
            s["global_trading_enabled"] = not global_protection_active
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
                markets = s.get("markets", {}) if isinstance(s.get("markets"), dict) else {}
                global_protection_active = bool(s.get("cooldown_active")) or any(
                    str(m.get("event_state", "")).upper() == "HALTED_ON_FILL"
                    for m in markets.values()
                    if isinstance(m, dict)
                )
                s["global_protection_active"] = global_protection_active
                s["global_trading_enabled"] = not global_protection_active
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
    """Check which account engine processes are alive.

    Uses PID file first; falls back to .engine_N.heartbeat mtime because PID
    checks break when state files are rsynced from another VPS (the remote
    PID means nothing on the local machine). Heartbeat is touched every
    second by engine.heartbeat_loop; mtime within last 10 s = alive.
    """
    import time as _time

    def _is_alive(i: int) -> bool:
        # PID-based check (works when engine is local to this dashboard)
        pid_path = PID_PATH if i == 0 else DATA_DIR / f".engine_{i}.pid"
        if pid_path.exists():
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip())
                if _pid_alive(pid):
                    return True
            except Exception:
                pass
        # Heartbeat mtime fallback (works for rsynced state from other VPS)
        hb_path = (DATA_DIR / ".engine.heartbeat") if i == 0 else DATA_DIR / f".engine_{i}.heartbeat"
        if hb_path.exists():
            try:
                age = _time.time() - hb_path.stat().st_mtime
                if age <= 10.0:
                    return True
            except Exception:
                pass
        return False

    result: dict[int, bool] = {}
    if PID_PATH.exists() or (DATA_DIR / ".engine.heartbeat").exists():
        result[0] = _is_alive(0)
    for i in range(1, 31):
        if (DATA_DIR / f".engine_{i}.pid").exists() or (DATA_DIR / f".engine_{i}.heartbeat").exists():
            result[i] = _is_alive(i)
    return result


# ── log tail ───────────────────────────────────────────────────────────────────

def tail_log(n: int = 60) -> str:
    if not LOG_PATH.exists():
        return "(no log file)"
    # Read only the tail of the file to avoid loading large logs into memory
    try:
        size = LOG_PATH.stat().st_size
        # ~200 bytes per line estimate; read a generous chunk from the end
        chunk = min(size, n * 300)
        with LOG_PATH.open("rb") as f:
            if size > chunk:
                f.seek(size - chunk)
            raw = _decode_log_bytes(f.read())
        lines = raw.splitlines()
        # If we seeked mid-file, drop the first (possibly partial) line
        if size > chunk and len(lines) > 1:
            lines = lines[1:]
        return "\n".join(lines[-n:])
    except Exception:
        return "(error reading log)"


# ── log parsing & Chinese mapping ─────────────────────────────────────────────
import re as _re

# Tag → (Chinese label, CSS class, level category)
_TAG_MAP: dict[str, tuple[str, str, str]] = {
    # danger
    "kill-switch":    ("风控熔断",   "tag-danger",   "异常"),
    "ALERT":          ("严重警报",   "tag-danger",   "异常"),
    "error":          ("错误",       "tag-danger",   "异常"),
    "exception":      ("异常",       "tag-danger",   "异常"),
    "safety":         ("安全检查",   "tag-danger",   "异常"),
    "risk":           ("风控",       "tag-danger",   "异常"),
    # warning / cooldown
    "watch":          ("监控模式",   "tag-cooldown", "冷却"),
    "cooldown":       ("冷却中",     "tag-cooldown", "冷却"),
    "netdiag":        ("网络诊断",   "tag-warning",  "冷却"),
    "signer-pace":    ("签名限速",   "tag-warning",  "冷却"),
    "pace":           ("节奏控制",   "tag-warning",  "冷却"),
    "snapshot-drop":  ("快照丢弃",   "tag-warning",  "冷却"),
    "latency":        ("延迟监测",   "tag-warning",  "冷却"),
    "preempt":        ("抢占执行",   "tag-warning",  "冷却"),
    # skip
    "quote-skip":     ("报价跳过",   "tag-warning",  "跳过"),
    "quote-skip-leg": ("分腿跳过",   "tag-warning",  "跳过"),
    "price-legs-skip":("价格跳过",   "tag-warning",  "跳过"),
    # cancel
    "cancel":         ("撤单",       "tag-danger",   "撤单"),
    "cancel_all":     ("全部撤单",   "tag-danger",   "撤单"),
    # recovery / success
    "recovery":       ("系统恢复",   "tag-success",  "恢复"),
    "vol-recovery":   ("波动恢复",   "tag-success",  "恢复"),
    "event-state":    ("状态变更",   "tag-success",  "恢复"),
    # info
    "quote":          ("报价更新",   "tag-info",     "信息"),
    "plan":           ("策略规划",   "tag-info",     "信息"),
    "health":         ("健康检查",   "tag-info",     "信息"),
    "exit":           ("退出操作",   "tag-info",     "信息"),
    "fill-ws":        ("成交推送",   "tag-info",     "信息"),
    "fill-poll":      ("成交轮询",   "tag-info",     "信息"),
    "trade-poll":     ("交易轮询",   "tag-info",     "信息"),
    "book-loop":      ("盘口更新",   "tag-info",     "信息"),
    "market-ws":      ("行情推送",   "tag-info",     "信息"),
    "guard-loop":     ("守护循环",   "tag-info",     "信息"),
    "debug-bal":      ("余额调试",   "tag-info",     "信息"),
    "tick-auto":      ("自动报价",   "tag-info",     "信息"),
    "session":        ("会话管理",   "tag-info",     "信息"),
    "unwind":         ("平仓操作",   "tag-info",     "信息"),
    "state-writer":   ("状态写入",   "tag-info",     "信息"),
    "dual-side-inject": ("双边注入", "tag-info",     "信息"),
    "dual-side-ok":   ("双边就绪", "tag-info",     "信息"),
    "dual-side-skip": ("双边跳过", "tag-warning",  "跳过"),
    # warning - additional
    "task-error":     ("任务错误",   "tag-danger",   "异常"),
    "quarantine":     ("隔离模式",   "tag-cooldown", "冷却"),
    "forbid":         ("禁止交易",   "tag-cooldown", "冷却"),
    "balance-drop":   ("余额下降",   "tag-warning",  "冷却"),
    "balance-drop-reconcile": ("余额调节", "tag-warning", "冷却"),
    "fine-tick-fallback": ("精细报价回退", "tag-warning", "跳过"),
    "top-leg-defense": ("头腿防御",  "tag-warning",  "冷却"),
}

# Reason snippets → Chinese description
_REASON_MAP: dict[str, str] = {
    "insufficient_budget_for_min_size": "预算不足",
    "blocked_slug":                     "市场已屏蔽",
    "Request exception":                "请求异常",
    "vol_recovery_from_watch":          "波动恢复，退出监控",
    "bba_jump":                         "盘口跳变",
    "planner_top_leg_sync":             "策略同步撤单",
    "planner_back_legs_sync":           "后腿同步撤单",
    "Reward invalid":                   "奖励无效，市场下线",
    "REJECT price>":                    "拒绝下单：价格超出合法上限",
    "REJECT price<reward_lower":        "拒绝下单：价格低于奖励区间",
    "REJECT price>=ask":                "拒绝下单：价格穿越卖盘",
    "REJECT stale_data":                "拒绝下单：数据过期",
    "snapshot_divergence":              "快照分歧过大，跳过",
    "CANCEL_TOP_LEG":                   "撤销头腿",
    "MOVE_BACK_TOP_LEG":                "头腿后移",
    "HALT_EVENT":                       "事件暂停",
    "front_depth_critical":             "前方深度严重不足",
    "front_depth_thin":                 "前方深度不足",
    "depth_data_untrusted":             "深度数据不可信",
    "market offlined":                  "市场已下线",
    "auto-resuming":                    "自动恢复报价",
    "recovery gate passed":             "恢复检查通过",
    "cancel_all failed":                "全部撤单失败",
}

## (duplicate English maps removed — using Chinese versions above)
_LOG_LINE_RE = _re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\]\s+\[([^\]]+)\]\s*(.*)"
)

def _translate_reason(detail: str) -> str:
    """Map known reason snippets in detail text to Chinese."""
    for eng, chn in _REASON_MAP.items():
        if eng in detail:
            return chn
    return ""


def _parse_log_entry(line: str) -> dict | None:
    """Parse a single log line into structured dict."""
    m = _LOG_LINE_RE.match(line.strip())
    if not m:
        return None
    ts_str, raw_tag, detail = m.group(1), m.group(2).strip(), m.group(3).strip()

    # Handle nested tags like "[health] [ALERT] ..."
    inner = _re.match(r"\[([^\]]+)\]\s*(.*)", detail)
    if inner:
        raw_tag = inner.group(1).strip()
        detail = inner.group(2).strip()

    tag_info = _TAG_MAP.get(raw_tag, ("其他", "tag-info", "信息"))
    cn_label, css_class, category = tag_info

    # Build human-readable Chinese description
    reason_cn = _translate_reason(detail)
    if reason_cn:
        main_msg = f"{cn_label}：{reason_cn}"
    else:
        main_msg = cn_label

    # Extract full token ID for name resolution
    token_full_match = _re.search(r"token=(\d{20,})", detail)
    token_full = token_full_match.group(1) if token_full_match else ""
    token_short = token_full[:8] + "…" if token_full else ""

    # Compact detail: strip long token/oid for readability
    compact_detail = _re.sub(r"(token=)\d{20,}", r"\1…", detail)
    compact_detail = _re.sub(r"(oid=|ids=)[0-9a-fA-F]{8,}", r"\1…", compact_detail)

    return {
        "time": ts_str[-8:],  # HH:MM:SS
        "tag": raw_tag,
        "cn_label": cn_label,
        "css_class": css_class,
        "category": category,
        "main_msg": main_msg,
        "detail": compact_detail,
        "token": token_short,
        "token_full": token_full,
        "market_name": "",  # filled later by tail_log_parsed
    }


def _get_token_name_cache() -> dict[str, str]:
    """Get or initialize the token→name cache in session_state."""
    if "_token_name_cache" not in st.session_state:
        st.session_state["_token_name_cache"] = {}
    return st.session_state["_token_name_cache"]


def _resolve_token_names(token_ids: set[str]) -> dict[str, str]:
    """Resolve token IDs to market names, using session cache + batch API."""
    cache = _get_token_name_cache()
    missing = [tid for tid in token_ids if tid and tid not in cache]
    if missing:
        # Batch resolve via existing function (max ~30 to avoid slow API)
        batch = tuple(missing[:30])
        try:
            resolved = resolve_market_names_batch(batch)
            for tid, name in resolved.items():
                # Only cache if we got a real name (not the fallback "xxxx...")
                if name and not name.endswith("...") or len(name) > 20:
                    cache[tid] = name
        except Exception:
            pass
    return cache


def tail_log_parsed(n: int = 200) -> list[dict]:
    """Read last n lines, parse into structured entries, resolve market names."""
    raw = tail_log(n)
    if raw.startswith("("):
        return []
    hidden_tags = {"trade-poll"}
    hidden_detail_markers = ["cheap_side_depth_insufficient"]
    entries = []
    for line in raw.splitlines():
        entry = _parse_log_entry(line)
        if entry:
            if entry["tag"] in hidden_tags:
                continue
            detail_text = str(entry.get("detail", ""))
            if any(marker in detail_text for marker in hidden_detail_markers):
                continue
            entries.append(entry)

    # Collect unique token IDs and batch-resolve names
    all_tokens = {e["token_full"] for e in entries if e["token_full"]}
    if all_tokens:
        name_map = _resolve_token_names(all_tokens)
        for e in entries:
            if e["token_full"] and e["token_full"] in name_map:
                e["market_name"] = name_map[e["token_full"]]

    return entries


def _render_log_html(entries: list[dict], new_count: int = 0) -> str:
    """Render parsed log entries into styled HTML.
    new_count: how many entries at the tail are 'new' (get slide-in animation).
    """
    if not entries:
        return '<div class="log-panel" style="padding:24px;color:#484f58;text-align:center;">暂无日志</div>'
    rows = []
    new_start = max(0, len(entries) - new_count) if new_count > 0 else len(entries)
    for idx, e in enumerate(entries):
        # Market name badge
        name_html = ""
        if e.get("market_name"):
            name_html = (
                f'<span style="color:#58a6ff;font-size:11px;background:#1a2332;'
                f'border:1px solid #1f3a5f;border-radius:3px;padding:0 5px;'
                f'margin-left:4px;white-space:nowrap;">{e["market_name"]}</span>'
            )
        # Detail line
        detail_html = ""
        if e["detail"]:
            detail_html = f'<div class="log-detail">{e["detail"]}</div>'
        extra_cls = " log-entry-new" if idx >= new_start else ""
        rows.append(
            f'<div class="log-entry{extra_cls}">'
            f'  <span class="log-time">{e["time"]}</span>'
            f'  <span class="log-tag {e["css_class"]}">{e["cn_label"]}</span>'
            f'  <div class="log-msg">{e["main_msg"]}{name_html}{detail_html}</div>'
            f'</div>'
        )
    # Auto-scroll: put anchor at bottom and use JS
    html = (
        '<div class="log-panel" id="logPanel">'
        + "\n".join(rows)
        + '<div id="logBottom"></div>'
        + '</div>'
        + '<script>var lp=document.getElementById("logPanel");if(lp)lp.scrollTop=lp.scrollHeight;</script>'
    )
    return html


def _render_log_summary(entries: list[dict]) -> str:
    """Render top summary bar."""
    from collections import Counter
    cat_counts = Counter(e["category"] for e in entries)

    # Latest notable event
    latest_notable = ""
    for e in reversed(entries):
        if e["category"] in ("异常", "撤单", "冷却", "恢复"):
            latest_notable = f'{e["cn_label"]}（{e["time"]}）'
            break

    # Current status inference
    status = "正常运行"
    status_dot = "dot-green"
    for e in entries[-10:]:
        if e["category"] == "异常":
            status = "异常告警"
            status_dot = "dot-red"
            break
        elif e["category"] == "冷却":
            status = "冷却等待"
            status_dot = "dot-blue"

    items = [
        f'<span class="log-summary-item"><span class="log-summary-dot {status_dot}"></span><b>{status}</b></span>',
    ]
    if latest_notable:
        items.append(f'<span class="log-summary-item" style="color:#8b949e;">最近事件：{latest_notable}</span>')
    if cat_counts.get("异常", 0):
        items.append(f'<span class="log-summary-item"><span class="log-summary-dot dot-red"></span>异常 {cat_counts["异常"]}</span>')
    if cat_counts.get("撤单", 0):
        items.append(f'<span class="log-summary-item"><span class="log-summary-dot dot-yellow"></span>撤单 {cat_counts["撤单"]}</span>')
    if cat_counts.get("冷却", 0) + cat_counts.get("跳过", 0):
        items.append(f'<span class="log-summary-item"><span class="log-summary-dot dot-blue"></span>冷却/跳过 {cat_counts.get("冷却", 0) + cat_counts.get("跳过", 0)}</span>')
    if cat_counts.get("恢复", 0):
        items.append(f'<span class="log-summary-item"><span class="log-summary-dot dot-green"></span>恢复 {cat_counts["恢复"]}</span>')

    return f'<div class="log-summary">{" ".join(items)}</div>'


# ══════════════════════════════════════════════════════════════════════════════
# AIRDROP FARMING / VAR-DECIBEL HEDGE LAB
# ══════════════════════════════════════════════════════════════════════════════

def _vd_nested_get(data: dict, path: tuple[str, ...], default: Any = None) -> Any:
    cur: Any = data
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _vd_parse_scalar(value: str) -> Any:
    value = value.strip()
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    if value in ('""', "''"):
        return ""
    if value.startswith("[") or value.startswith("{"):
        try:
            return ast.literal_eval(value)
        except Exception:
            return value
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value


def _vd_load_config() -> dict:
    if not VAR_DECIBEL_CONFIG_PATH.exists():
        return {}
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw_line in VAR_DECIBEL_CONFIG_PATH.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        key, sep, raw_value = raw_line.strip().partition(":")
        if not sep:
            continue
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        value = raw_value.strip()
        if value == "":
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _vd_parse_scalar(value)
    return root


def _vd_db_path(cfg: dict) -> Path:
    raw = str(cfg.get("database_url") or "sqlite:///data/hedge_bot.sqlite3")
    if raw.startswith("sqlite:///"):
        raw = raw[len("sqlite:///"):]
    path = Path(raw)
    return path if path.is_absolute() else VAR_DECIBEL_DIR / path


def _vd_fetch_rows(db_path: Path, table: str, order_col: str, limit: int = 250) -> list[dict[str, Any]]:
    if table not in {"trades", "market_snapshots"} or not db_path.exists():
        return []
    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                f"select * from {table} order by {order_col} desc limit ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
    except Exception:
        return []


def _vd_fmt_bool(value: Any) -> str:
    return "true" if bool(value) else "false"


def _vd_live_enabled(cfg: dict) -> bool:
    return (
        not bool(cfg.get("dry_run", True))
        and bool(cfg.get("live_trading", False))
        and str(cfg.get("confirm_live_trading_text", "")) == "I_UNDERSTAND_THIS_CAN_LOSE_MONEY"
    )


def _vd_bot_status(cfg: dict, kill_switch: Path) -> str:
    if kill_switch.exists():
        return "HALTED"
    if _vd_live_enabled(cfg):
        return "LIVE ARMED"
    if bool(cfg.get("live_trading", False)):
        return "LIVE LOCKED"
    return "MONITOR / DRY-RUN"


def _vd_latest_snapshot(snapshots: pd.DataFrame, symbol: str) -> dict[str, Any]:
    if snapshots.empty or "symbol" not in snapshots:
        return {}
    matched = snapshots[snapshots["symbol"].astype(str) == symbol]
    if matched.empty:
        return {}
    order_col = "timestamp" if "timestamp" in matched else "id"
    return dict(matched.sort_values(order_col).iloc[-1])


def _vd_mid(row: dict[str, Any]) -> str:
    try:
        bid = float(row.get("decibel_bid"))
        ask = float(row.get("decibel_ask"))
    except (TypeError, ValueError):
        return "—"
    return f"{(bid + ask) / 2:.4f}"


def _vd_capture_row(label: str, path: Path, kind: str) -> dict[str, str]:
    return {
        "Item": label,
        "Status": "present" if path.exists() else "missing",
        "Kind": kind,
        "Path": str(path),
    }


def _vd_panel(title: str, value: str, detail: str, color_class: str = "pill-gray") -> None:
    st.markdown(
        f"""
        <div class="var-panel">
          <div class="var-panel-title">{title}</div>
          <div class="var-panel-value">{value}</div>
          <div class="var-panel-small"><span class="{color_class}">{detail}</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_airdrop_farming_dashboard() -> None:
    cfg = _vd_load_config()
    project_ok = VAR_DECIBEL_DIR.exists()
    config_ok = VAR_DECIBEL_CONFIG_PATH.exists()
    db_path = _vd_db_path(cfg)
    kill_switch = VAR_DECIBEL_DIR / str(_vd_nested_get(cfg, ("risk", "kill_switch_file"), "KILL_SWITCH"))
    live_enabled = _vd_live_enabled(cfg)
    status = _vd_bot_status(cfg, kill_switch)

    trades = pd.DataFrame(_vd_fetch_rows(db_path, "trades", "id", 100))
    snapshots = pd.DataFrame(_vd_fetch_rows(db_path, "market_snapshots", "id", 500))

    st.markdown('<p class="section-title">Airdrop Farming / Hedge Research</p>', unsafe_allow_html=True)
    st.markdown("## Var/Decibel Hedge Lab")
    st.caption("Small-notional Variational Omni + Decibel delta-neutral experimenter. This panel is embedded in Latitude Alpha and stays monitor/dry-run first.")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _vd_panel("Bot status", status, "kill switch active" if kill_switch.exists() else "research mode", "pill-red" if kill_switch.exists() else "pill-yellow")
    with c2:
        _vd_panel("Live gate", "ARMED" if live_enabled else "LOCKED", f"dry_run={_vd_fmt_bool(cfg.get('dry_run', True))}", "pill-green" if live_enabled else "pill-yellow")
    with c3:
        _vd_panel("Data store", "READY" if db_path.exists() else "EMPTY", db_path.name, "pill-green" if db_path.exists() else "pill-gray")
    with c4:
        _vd_panel("Project", "FOUND" if project_ok else "MISSING", str(VAR_DECIBEL_DIR), "pill-green" if project_ok else "pill-red")

    if not project_ok or not config_ok:
        st.warning(f"Var/Decibel project or config not found. Expected `{VAR_DECIBEL_CONFIG_PATH}`.")
        return

    st.markdown("")
    tabs = st.tabs(["Overview", "Monitor", "Cost Model", "Risk", "RFQ Captures", "Replay"])

    with tabs[0]:
        left, right = st.columns([1.2, 1])
        with left:
            st.markdown('<p class="section-title">Scope</p>', unsafe_allow_html=True)
            auto = _vd_nested_get(cfg, ("symbols", "auto_trade_whitelist"), ["BTC", "ETH"])
            monitor_only = _vd_nested_get(cfg, ("symbols", "monitor_only"), ["AAPL", "QQQ", "XAU", "CL", "SPCX"])
            st.markdown(
                f"""
                <div class="var-block">
                  <div class="muted-note">Default flow: monitor → dry-run → very-small live-run. No live order controls are exposed here yet.</div>
                  <br>
                  <span class="pill-green">Auto whitelist: {", ".join(map(str, auto))}</span>
                  <span class="pill-yellow">Monitor-only: {", ".join(map(str, monitor_only))}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        with right:
            summary = pd.DataFrame(
                [
                    {"Metric": "Recent trades", "Value": str(len(trades))},
                    {"Metric": "Market snapshots", "Value": str(len(snapshots))},
                    {"Metric": "Decibel network", "Value": str(_vd_nested_get(cfg, ("decibel", "network"), "testnet"))},
                    {"Metric": "Variational API", "Value": str(_vd_nested_get(cfg, ("variational", "readonly_base_url"), "—"))},
                ]
            )
            st.dataframe(summary, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.markdown('<p class="section-title">Opportunity Matrix</p>', unsafe_allow_html=True)
        rows = []
        for symbol in _vd_nested_get(cfg, ("symbols", "auto_trade_whitelist"), ["BTC", "ETH"]):
            latest = _vd_latest_snapshot(snapshots, str(symbol))
            rows.append(
                {
                    "Symbol": symbol,
                    "Var mark": latest.get("var_mark", "—") if latest else "—",
                    "Decibel mid": _vd_mid(latest) if latest else "—",
                    "Basis bp": latest.get("basis_bp", "—") if latest else "—",
                    "Var funding": latest.get("var_funding", "—") if latest else "—",
                    "Decibel funding": latest.get("decibel_funding", "—") if latest else "—",
                    "Quote state": "no snapshot" if not latest else "freshness unknown",
                    "Decision": "locked" if not live_enabled else "eligible",
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.markdown('<p class="muted-note">Live values will appear after the monitor process writes snapshots into the Var/Decibel SQLite database.</p>', unsafe_allow_html=True)

    with tabs[2]:
        st.markdown('<p class="section-title">Expected Cost</p>', unsafe_allow_html=True)
        max_notional = float(_vd_nested_get(cfg, ("risk", "max_order_notional"), "25"))
        max_expected = float(_vd_nested_get(cfg, ("risk", "max_expected_cost_bp"), "45"))
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            notional = st.number_input("Notional USDC", min_value=1.0, max_value=max_notional, value=min(10.0, max_notional), step=1.0, key="vd_notional")
            var_slippage = st.number_input("Var slippage bp", min_value=0.0, value=5.0, step=0.5, key="vd_var_slippage")
        with col_b:
            decibel_spread = st.number_input("Decibel spread bp", min_value=0.0, value=6.0, step=0.5, key="vd_decibel_spread")
            close_cost = st.number_input("Close cost bp", min_value=0.0, value=8.0, step=0.5, key="vd_close_cost")
        with col_c:
            funding_cost = st.number_input("Funding cost bp", value=0.0, step=0.5, key="vd_funding_cost")
            risk_buffer = st.number_input("Risk buffer bp", min_value=0.0, value=10.0, step=0.5, key="vd_risk_buffer")
        total = var_slippage + decibel_spread + close_cost + funding_cost + risk_buffer
        m1, m2, m3 = st.columns(3)
        m1.metric("Target notional", f"{notional:.2f} U")
        m2.metric("Total expected cost", f"{total:.2f} bp")
        m3.metric("Configured max", f"{max_expected:.2f} bp")
        st.dataframe(
            pd.DataFrame(
                [
                    {"Direction": "Var long + Decibel short", "Expected cost bp": total, "Decision": "blocked" if total >= max_expected or not live_enabled else "eligible"},
                    {"Direction": "Var short + Decibel long", "Expected cost bp": total, "Decision": "blocked" if total >= max_expected or not live_enabled else "eligible"},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tabs[3]:
        st.markdown('<p class="section-title">Risk Controls</p>', unsafe_allow_html=True)
        risk = cfg.get("risk", {}) if isinstance(cfg.get("risk"), dict) else {}
        risk_rows = [{"Limit": k, "Value": str(v)} for k, v in risk.items()]
        left, right = st.columns([1.2, 1])
        with left:
            st.dataframe(pd.DataFrame(risk_rows), use_container_width=True, hide_index=True)
        with right:
            st.markdown("Kill switch")
            st.write(f"`{kill_switch}`")
            st.write("Active" if kill_switch.exists() else "Inactive")
            if st.button("Create Var/Decibel kill switch", use_container_width=True, key="vd_create_kill"):
                kill_switch.write_text("stop\n", encoding="utf-8")
                st.rerun()
            if kill_switch.exists() and st.button("Remove Var/Decibel kill switch", use_container_width=True, key="vd_remove_kill"):
                kill_switch.unlink()
                st.rerun()

    with tabs[4]:
        st.markdown('<p class="section-title">Variational RFQ Captures</p>', unsafe_allow_html=True)
        rfq_local = VAR_DECIBEL_DIR / str(_vd_nested_get(cfg, ("variational", "rfq_request_capture_path"), "captures/variational_rfq_request.local.json"))
        accept_local = VAR_DECIBEL_DIR / str(_vd_nested_get(cfg, ("variational", "accept_quote_capture_path"), "captures/variational_accept_quote.local.json"))
        rows = [
            _vd_capture_row("RFQ request local", rfq_local, "ignored local template"),
            _vd_capture_row("Accept quote local", accept_local, "ignored local template"),
            _vd_capture_row("RFQ request example", VAR_DECIBEL_DIR / "captures/variational_rfq_request.example.json", "tracked example"),
            _vd_capture_row("Accept quote example", VAR_DECIBEL_DIR / "captures/variational_accept_quote.example.json", "tracked example"),
            _vd_capture_row("Last schema mismatch", VAR_DECIBEL_DIR / "captures/schema_change_response.redacted.json", "redacted artifact"),
        ]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        st.warning("Never upload real cookies, bearer tokens, wallet signatures, private keys, or session tokens. Local capture files stay git-ignored.")

    with tabs[5]:
        st.markdown('<p class="section-title">Replay</p>', unsafe_allow_html=True)
        if snapshots.empty:
            st.info("No market snapshots yet.")
        else:
            ordered = snapshots.sort_values("timestamp" if "timestamp" in snapshots else "id")
            if "basis_bp" in ordered:
                st.line_chart(ordered.set_index("timestamp" if "timestamp" in ordered else "id")[["basis_bp"]])
        st.dataframe(trades, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION
# ══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("### Latitude Alpha")
    st.markdown("---")

    PLATFORMS = {
        "Market Making": ["Polymarket"],
        "airdrop_farming": [],
        # future: "Hyperliquid": ["Perps", "Vaults"],
    }

    def _select_nav(value: str) -> None:
        st.session_state["nav_feature"] = value
        st.rerun()

    nav_platform = None
    nav_feature = None
    for p, features in PLATFORMS.items():
        st.markdown(f"<span style='color:#8b949e; font-size:11px; letter-spacing:.08em; text-transform:uppercase'>{p}</span>", unsafe_allow_html=True)
        if not features:
            if st.button(
                f"  {p}",
                key=f"nav_{p}",
                use_container_width=True,
            ):
                _select_nav(f"{p}/{p}")
            if nav_platform is None:
                nav_platform = p
                nav_feature = p
        for f in features:
            if st.button(
                f"  {f}",
                key=f"nav_{p}_{f}",
                use_container_width=True,
            ):
                _select_nav(f"{p}/{f}")
            if nav_platform is None:
                nav_platform = p
                nav_feature = f

    # default selection
    _nav = st.session_state.get("nav_feature", "Market Making/Polymarket")
    if _nav == "Polymarket/Market Making":
        _nav = "Market Making/Polymarket"
        st.session_state["nav_feature"] = _nav
    nav_platform, nav_feature = _nav.split("/", 1)

    st.markdown("---")
    st.caption(f"{nav_platform} / {nav_feature}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ══════════════════════════════════════════════════════════════════════════════

cfg = load_config()
acc = cfg.get("account", {})
host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
scan_defaults = cfg.get("dashboard", {}).get("scan_defaults", {})

# active key: test session key > env var > config (REDACTED)
active_key = (
    st.session_state.get("test_private_key", "").strip()
    or os.getenv("POLY_PRIVATE_KEY", "").strip()
)
has_key = bool(active_key and "REDACTED" not in active_key) or bool(acc.get("signer_server_url"))

chain_id  = int(acc.get("chain_id", 137))
sig_type  = int(acc.get("signature_type", 0))
funder    = acc.get("funder")

# ── header ─────────────────────────────────────────────────────────────────────
col_title, col_status, col_stop = st.columns([4, 2, 1])
with col_title:
    st.markdown("## Latitude Alpha")
    st.caption(f"{nav_platform}  /  {nav_feature}")
with col_status:
    running = _multi_runner_running()
    badge = '<span class="pill-green">● RUNNING</span>' if running else '<span class="pill-red">● STOPPED</span>'
    key_badge = '<span class="pill-green">KEY OK</span>' if has_key else '<span class="pill-yellow">NO KEY</span>'
    st.markdown(f"{badge}&nbsp;&nbsp;{key_badge}", unsafe_allow_html=True)
    st.caption(f"Refresh: {datetime.now(_BJT).strftime('%H:%M:%S')} 北京时间")
with col_stop:
    if st.button("EMERGENCY STOP", type="primary", use_container_width=True):
        _flash(stop_multi_runner(), "error")
        st.rerun()

_show_flash()
st.divider()

if nav_platform == "airdrop_farming":
    _render_airdrop_farming_dashboard()
    st.stop()

# ── metric cards (auto-refresh fragment — only this section re-renders) ────────
engine_state = load_engine_state()

@st.fragment(run_every=timedelta(seconds=5))
def _status_bar():
    _now = time.time()
    # Aggregate across every engine_state_N.json that exists (multi-account
    # view). Falls back to single engine_state.json when multi-runner isn't
    # active so the status bar still works standalone.
    _all_states = load_all_engine_states()
    _running_states = {k: v for k, v in _all_states.items() if k > 0 and v.get("balance") is not None}
    if not _running_states and _all_states.get(0) and _all_states[0].get("balance") is not None:
        _running_states = {0: _all_states[0]}

    _balance_raw = 0.0
    _order_size_sum = 0.0
    _total_order_count = 0
    _fills_today: list = []
    _unwinds: list = []
    _es_has_data = False
    for _acc_id, _es in _running_states.items():
        _es_has_data = True
        _balance_raw += float(_es.get("balance") or 0)
        for _ms in (_es.get("markets", {}) or {}).values():
            for _o in _ms.get("orders", []) or []:
                _order_size_sum += float(_o.get("price", 0) or 0) * float(_o.get("size", 0) or 0)
                _total_order_count += 1
        _fills_today.extend(
            f for f in (_es.get("fills", []) or [])
            if _now - float(f.get("ts", 0) or 0) < 86400
        )
        _unwinds.extend(_es.get("pending_unwinds", []) or [])

    _utilization = (_order_size_sum / _balance_raw * 100) if _balance_raw > 0 else 0.0

    if not _es_has_data and has_key:
        # No engine state files yet — fall back to a direct CLOB read for the
        # config.json account so the status bar has something to show pre-start.
        _bal_info = fetch_balance_info(host, active_key, chain_id, sig_type, funder)
        _open_orders = fetch_open_orders(host, active_key, chain_id, sig_type, funder)
        _balance_raw = float(_bal_info.get("balance") or 0) / 1e6
        _allowance_raw = float(_bal_info.get("allowance") or 0) / 1e6
        _order_size_sum = sum(float(o.get("size_matched", 0) or 0) * float(o.get("price", 0) or 0)
                              for o in _open_orders)
        _utilization = (_order_size_sum / _allowance_raw * 100) if _allowance_raw > 0 else 0.0
        _total_order_count = len(_open_orders)

    _overdue_unwinds = [u for u in _unwinds
                        if _now - float(u.get("placed_at", _now) or _now) > 4 * 3600]
    _n_accounts = len(_running_states)

    _show_bal = _es_has_data or has_key
    _acc_suffix = (
        f" ({_n_accounts} 账号合计)" if _n_accounts > 1
        else (" (1 账号)" if _n_accounts == 1 else "")
    )
    m1, m2, m3, m4, m5 = st.columns(5)
    with m1:
        st.metric(f"USDC Balance{_acc_suffix}",
                  f"${_balance_raw:,.2f}" if _show_bal else "—",
                  help="所有在跑账号 collateral USDC 总和")
    with m2:
        st.metric(f"Order Utilization{_acc_suffix}",
                  f"{_utilization:.1f}%" if _show_bal else "—",
                  delta=f"${_order_size_sum:,.0f} deployed" if _show_bal else None)
    with m3:
        st.metric(f"Open Orders{_acc_suffix}",
                  str(_total_order_count) if _show_bal else "—",
                  help="所有在跑账号的 live BUY limit orders 合计")
    with m4:
        st.metric(f"Fills Today{_acc_suffix}", str(len(_fills_today)),
                  help="过去 24h 内所有账号的 fill 事件合计")
    with m5:
        _overdue_label = f"{len(_overdue_unwinds)} overdue" if _overdue_unwinds else None
        st.metric(f"Pending Unwinds{_acc_suffix}", str(len(_unwinds)),
                  delta=_overdue_label,
                  delta_color="inverse" if _overdue_unwinds else "off",
                  help="所有在跑账号的 pending_unwinds 合计")
    st.caption(f"状态更新: {datetime.now(_BJT).strftime('%H:%M:%S')} 北京时间")

_status_bar()

# Provide variables needed by tabs below (from initial engine_state load)
_now_unix = time.time()
_es_balance = engine_state.get("balance")
_es_markets = engine_state.get("markets", {})
_es_has_data = _es_balance is not None and bool(_es_markets)
if _es_has_data:
    balance_raw = float(_es_balance)
    bal_info = {"balance": None, "allowance": None}
    open_orders = []
    _total_order_notional = 0.0
    _total_order_count = 0
    for _ms in _es_markets.values():
        for _o in _ms.get("orders", []):
            _total_order_notional += float(_o.get("price", 0) or 0) * float(_o.get("size", 0) or 0)
            _total_order_count += 1
    order_size_sum = _total_order_notional
    utilization = (order_size_sum / balance_raw * 100) if balance_raw > 0 else 0.0
elif has_key:
    bal_info = fetch_balance_info(host, active_key, chain_id, sig_type, funder)
    open_orders = fetch_open_orders(host, active_key, chain_id, sig_type, funder)
    balance_raw = float(bal_info.get("balance") or 0) / 1e6
    order_size_sum = sum(float(o.get("size_matched", 0) or 0) * float(o.get("price", 0) or 0)
                         for o in open_orders)
    _total_order_count = len(open_orders)
    utilization = 0.0
else:
    bal_info = {"balance": None, "allowance": None}
    open_orders = []
    balance_raw = 0.0
    order_size_sum = 0.0
    _total_order_count = 0
    utilization = 0.0
fills_today = [f for f in engine_state.get("fills", [])
               if _now_unix - float(f.get("ts", 0) or 0) < 86400]
unwinds = engine_state.get("pending_unwinds", [])
overdue_unwinds = [u for u in unwinds
                   if _now_unix - float(u.get("placed_at", _now_unix) or _now_unix) > 4 * 3600]

st.markdown("")

# ── tabs ───────────────────────────────────────────────────────────────────────
tab_control, tab_markets, tab_fills, tab_scan, tab_proxy = st.tabs(
    ["Control", "Markets", "Fill / Unwind", "Scan", "Proxy"]
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

        # Q_min data from engine state
        q_eff = ms.get("q_min_efficiency", 0) if has_engine_state and tid in es_markets else 0
        q_bid_sh = ms.get("q_bid_shares", 0) if has_engine_state and tid in es_markets else 0
        q_ask_sh = ms.get("q_ask_shares", 0) if has_engine_state and tid in es_markets else 0
        rw_min_sz = ms.get("rewards_min_size", 0) if has_engine_state and tid in es_markets else 0
        has_dual = ms.get("has_dual_side", False) if has_engine_state and tid in es_markets else False

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
            "Bid Sh":     f"{q_bid_sh:.0f}" if q_bid_sh else "—",
            "Ask Sh":     f"{q_ask_sh:.0f}" if q_ask_sh else "—",
            "MinSh":      f"{rw_min_sz:.0f}" if rw_min_sz else "—",
            "Q Eff":      f"{q_eff:.0%}" if q_eff else "—",
            "Dual":       "✓" if has_dual else "—",
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

    # ── 自动扫入事件 (auto_curator additions, last 48h, day + night) ────────
    st.divider()
    st.markdown('<p class="section-title">🤖 自动扫入事件 — 最近 48 小时 Auto Curator 加入的 markets（日盘 + 夜盘）</p>',
                unsafe_allow_html=True)

    _ne_all = load_all_engine_states()
    _ne_merged: dict[str, dict] = {}
    for _acc_id, _acc_state in _ne_all.items():
        # New key `curator_events` (both sessions); fallback to legacy `night_events`.
        _events = _acc_state.get("curator_events")
        if _events is None:
            _events = _acc_state.get("night_events") or []
        for _ev in (_events or []):
            tid = str(_ev.get("token_id", ""))
            if not tid:
                continue
            existing = _ne_merged.get(tid)
            if existing is None:
                _ne_merged[tid] = {**_ev, "accounts": [_acc_id]}
            else:
                if _acc_id not in existing["accounts"]:
                    existing["accounts"].append(_acc_id)
                # Keep earliest added_at, latest live_status priority: started > in_pool > removed
                if float(_ev.get("added_at", 0) or 0) < float(existing.get("added_at", 0) or 0):
                    existing["added_at"] = _ev.get("added_at")
                _priority = {"started": 2, "in_pool": 1, "removed": 0}
                if _priority.get(_ev.get("live_status"), 0) > _priority.get(existing.get("live_status"), 0):
                    existing["live_status"] = _ev.get("live_status")
                    existing["in_pool"] = _ev.get("in_pool")

    # Session filter (All / Day / Night)
    _ne_filter = st.radio(
        "过滤",
        options=["全部", "日盘", "夜盘"],
        horizontal=True,
        key="curator_events_filter",
    )
    _filter_map = {"日盘": "day", "夜盘": "night"}
    _filter_session = _filter_map.get(_ne_filter)
    _ne_view = {
        tid: ev for tid, ev in _ne_merged.items()
        if (_filter_session is None) or (ev.get("session") == _filter_session)
    }

    if not _ne_view:
        if not _ne_merged:
            st.caption("尚无自动扫入事件记录。Auto Curator 启用后，每次加入 market（日盘或夜盘）都会在这里显示，保留 48h。")
        else:
            st.caption(f"当前过滤 ({_ne_filter}) 下无记录。")
    else:
        _now_ts = time.time()
        ne_rows = []
        for tid, ev in _ne_view.items():
            gst = float(ev.get("game_start_ts", 0) or 0)
            added_at = float(ev.get("added_at", 0) or 0)
            if gst > 0:
                gst_bjt = datetime.fromtimestamp(gst, tz=_BJT).strftime("%m-%d %H:%M")
                delta_h = (gst - _now_ts) / 3600.0
                if delta_h > 0:
                    delta_str = f"T-{delta_h:.1f}h"
                else:
                    delta_str = f"已开 {abs(delta_h):.1f}h"
            else:
                gst_bjt = "—"
                delta_str = "—"
            added_bjt = datetime.fromtimestamp(added_at, tz=_BJT).strftime("%m-%d %H:%M") if added_at else "—"
            accs = sorted(ev.get("accounts", []))
            acc_label = f"{len(accs)} ({','.join(str(a) for a in accs[:5])}{'...' if len(accs) > 5 else ''})"
            _sess = str(ev.get("session", "")).lower()
            sess_badge = "🌙 夜盘" if _sess == "night" else ("☀️ 日盘" if _sess == "day" else "—")
            ne_rows.append({
                "盘": sess_badge,
                "Slug": (ev.get("slug") or "")[:48],
                "Question": (ev.get("question") or "")[:40],
                "League": (ev.get("league") or "")[:24],
                "开赛 BJT": gst_bjt,
                "距开赛": delta_str,
                "加入时间": added_bjt,
                "状态": ev.get("live_status", "—"),
                "账户": acc_label,
                "Token": tid[:12],
            })
        # Sort by game start time ascending (next games first)
        ne_rows.sort(key=lambda r: r["开赛 BJT"] if r["开赛 BJT"] != "—" else "9999")
        df_ne = pd.DataFrame(ne_rows)
        NE_STATUS_COLORS = {
            "in_pool": "color:#3fb950; font-weight:600",
            "started": "color:#8b949e",
            "removed": "color:#d29922",
        }
        NE_SESSION_COLORS = {
            "☀️ 日盘": "color:#d29922; font-weight:600",
            "🌙 夜盘": "color:#58a6ff; font-weight:600",
        }
        styled_ne = (
            df_ne.style
            .map(lambda v: NE_STATUS_COLORS.get(v, ""), subset=["状态"])
            .map(lambda v: NE_SESSION_COLORS.get(v, ""), subset=["盘"])
            .set_properties(**{"background-color": "#0d1117", "color": "#e6edf3"})
        )
        st.dataframe(styled_ne, use_container_width=True, hide_index=True)
        _day_n  = sum(1 for e in _ne_view.values() if e.get("session") == "day")
        _night_n = sum(1 for e in _ne_view.values() if e.get("session") == "night")
        st.caption(
            f"合计 {len(_ne_view)} 个事件（日盘 {_day_n} · 夜盘 {_night_n}） · "
            f"在池中 {sum(1 for e in _ne_view.values() if e.get('live_status') == 'in_pool')} · "
            f"已开赛 {sum(1 for e in _ne_view.values() if e.get('live_status') == 'started')} · "
            f"已移出 {sum(1 for e in _ne_view.values() if e.get('live_status') == 'removed')}"
        )

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

    # Replay toast(s) queued by the previous "应用选择" submission (the rerun
    # wipes any in-flight st.success, so we use session_state as a mailbox).
    for _ic, _msg in st.session_state.pop("_scan_toasts", []):
        st.toast(_msg, icon=_ic)
        if _ic == "✅":
            st.success(_msg)
        else:
            st.warning(_msg)

    # ── Session Confirmation ──────────────────────────────────────────────────
    _confirm_data = {}
    _confirm_active = False
    try:
        if SESSION_CONFIRM_PATH.exists():
            _confirm_data = json.loads(SESSION_CONFIRM_PATH.read_text())
            _confirmed_at = float(_confirm_data.get("confirmed_at", 0) or 0)
            _expires_at = float(_confirm_data.get("expires_at", 0) or 0)
            if _confirmed_at > 0 and (not _expires_at or time.time() < _expires_at):
                _confirmed_dt = datetime.fromtimestamp(_confirmed_at, tz=_BJT)
                _now_dt = datetime.now(_BJT)
                _confirm_active = (_confirmed_dt.date() == _now_dt.date())
    except Exception:
        pass

    _confirm_col1, _confirm_col2 = st.columns([1, 3])
    with _confirm_col1:
        if st.button(
            "✅ 确认今晚切盘（覆盖夜盘+次日日盘）" if not _confirm_active else "🔄 重新确认今晚切盘",
            type="primary" if not _confirm_active else "secondary",
            use_container_width=True,
        ):
            _now_dt = datetime.now(_BJT)
            _next_midnight = (_now_dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            _confirm_payload = {
                "confirmed_at": _now_dt.timestamp(),
                "confirmed_at_human": _now_dt.strftime("%Y-%m-%d %H:%M:%S 北京时间"),
                "confirm_date": _now_dt.strftime("%Y-%m-%d"),
                "expires_at": _next_midnight.timestamp(),
                "expires_at_human": _next_midnight.strftime("%Y-%m-%d %H:%M:%S 北京时间"),
                "scope": "night_and_next_day",
            }
            SESSION_CONFIRM_PATH.parent.mkdir(parents=True, exist_ok=True)
            SESSION_CONFIRM_PATH.write_text(json.dumps(_confirm_payload, indent=2))
            st.success(f"已确认！本次确认覆盖今晚夜盘和次日日盘，有效至 {_confirm_payload['expires_at_human']}。")
            st.rerun()
    with _confirm_col2:
        if _confirm_active:
            _exp_str = _confirm_data.get("expires_at_human", "")
            _conf_str = _confirm_data.get("confirmed_at_human", "")
            st.success(f"今晚切盘已授权 — 确认于 {_conf_str}，覆盖夜盘+次日日盘，有效至 {_exp_str}")
        else:
            st.error("⚠️ 今晚切盘未授权 — 到 00:00 后若无今日确认，引擎将撤单并停止；次日日盘也不会自动恢复。请 Scan 后点击确认按钮。")
    st.markdown("---")

    # ── Auto Curator (自动扫入体育赛事) ──────────────────────────────────────
    st.markdown('<p class="section-title">🤖 Auto Curator — 自动扫入体育赛事（日盘）</p>', unsafe_allow_html=True)
    try:
        _ac_cfg_cur = load_config()
        _ac_enabled_cur = bool((_ac_cfg_cur.get("auto_curator") or {}).get("enabled", False))
        _ac_interval_cur = int((_ac_cfg_cur.get("auto_curator") or {}).get("interval_sec", 900))
    except Exception:
        _ac_enabled_cur = False
        _ac_interval_cur = 900

    _ac_state: dict = {}
    _ac_state_path = DATA_DIR / "auto_curator_state.json"
    if _ac_state_path.exists():
        try:
            _ac_state = json.loads(_ac_state_path.read_text(encoding="utf-8"))
        except Exception:
            _ac_state = {}

    _ac_col_toggle, _ac_col_interval, _ac_col_status = st.columns([1, 1, 3])
    with _ac_col_toggle:
        _ac_new_enabled = st.toggle(
            "启用自动扫入",
            value=_ac_enabled_cur,
            key="auto_curator_enabled_toggle",
            help="开启后每 interval_sec 自动扫 Polymarket 体育赛事并热加入日盘 market_cfg。撤单由 T-2h 开赛守护完成。",
        )
    with _ac_col_interval:
        _ac_new_interval = st.number_input(
            "扫描间隔 (秒)",
            min_value=60,
            max_value=3600,
            value=_ac_interval_cur,
            step=60,
            key="auto_curator_interval_input",
            help="默认 900s (15分钟)。修改后下次重启生效；enable 切换无需重启。",
        )
    with _ac_col_status:
        if _ac_state:
            _last_scan = float(_ac_state.get("last_scan_ts") or 0)
            _last_scan_str = (
                datetime.fromtimestamp(_last_scan, tz=_BJT).strftime("%m-%d %H:%M:%S")
                if _last_scan > 0 else "尚未扫描"
            )
            _ac_status_color = "🟢" if _ac_state.get("enabled") else "⚪"
            st.markdown(
                f"{_ac_status_color} **状态**: {'启用' if _ac_state.get('enabled') else '停用'} &nbsp;|&nbsp; "
                f"**上次扫描**: {_last_scan_str} &nbsp;|&nbsp; "
                f"**累计扫描**: {_ac_state.get('scans', 0)} 次 &nbsp;|&nbsp; "
                f"**累计加入**: {_ac_state.get('added_total', 0)} 市场 &nbsp;|&nbsp; "
                f"**拒绝**: {_ac_state.get('rejected_total', 0)} &nbsp;|&nbsp; "
                f"**当前运行态加入**: {_ac_state.get('runtime_added_count', 0)}",
                unsafe_allow_html=True,
            )
        else:
            st.caption("引擎尚未写入 auto_curator_state.json — 启动引擎并等一个扫描周期后会出现。")

    # Persist toggle / interval changes back to config.json
    if (_ac_new_enabled != _ac_enabled_cur) or (int(_ac_new_interval) != _ac_interval_cur):
        if _gate_write("auto_curator toggle"):
            try:
                _ac_cfg_save = load_config()
                _ac_cfg_save.setdefault("auto_curator", {})
                _ac_cfg_save["auto_curator"]["enabled"] = bool(_ac_new_enabled)
                _ac_cfg_save["auto_curator"]["interval_sec"] = int(_ac_new_interval)
                save_config(_ac_cfg_save)
                if _ac_new_enabled != _ac_enabled_cur:
                    st.success(f"Auto Curator {'已启用' if _ac_new_enabled else '已停用'}（下次扫描周期内生效，最多等 {_ac_interval_cur}s）")
                else:
                    st.info("扫描间隔已保存（需重启引擎生效）")
            except Exception as _ac_save_err:
                st.error(f"保存失败: {_ac_save_err}")

    st.markdown("---")

    row1_c1, row1_c2, row1_c3, row1_c4 = st.columns(4)
    scan_min_reward = int(scan_defaults.get("min_reward", 10) or 0)
    scan_max_reward = int(scan_defaults.get("max_reward", 88888) or 0)
    scan_min_spread = int(scan_defaults.get("min_spread", 1) or 0)
    scan_max_spread = int(scan_defaults.get("max_spread", 10) or 0)
    scan_min_vol = int(scan_defaults.get("min_volume", 10_000) or 0)
    scan_min_bid_depth = int(scan_defaults.get("min_bid_depth", 10_000) or 0)
    scan_sort_by = str(scan_defaults.get("sort_by", "reward_score") or "reward_score")
    scan_top_n = int(scan_defaults.get("top_n", 50) or 50)
    sort_options = ["reward", "reward_score", "share_balance", "volume", "score"]
    sort_index = sort_options.index(scan_sort_by) if scan_sort_by in sort_options else 1

    with row1_c1:
        min_reward = st.number_input("Min Daily Reward ($)", value=scan_min_reward, step=10)
    with row1_c2:
        max_reward = st.number_input("Max Daily Reward ($)", value=scan_max_reward, step=10, help="0 = no limit")
    with row1_c3:
        min_spread = st.number_input("Min Spread", value=scan_min_spread, step=1, help="maxIncentiveSpread lower bound")
    with row1_c4:
        max_spread = st.number_input("Max Spread", value=scan_max_spread, step=1, help="0 = no limit")

    row2_c1, row2_c2, row2_c3, row2_c4 = st.columns(4)
    with row2_c1:
        min_vol = st.number_input("Min 24h Volume ($)", value=scan_min_vol, step=10_000)
    with row2_c2:
        min_bid_depth = st.number_input("Min Bid Depth ($)", value=scan_min_bid_depth, step=10_000, help="Bid-side order book total notional (USDC)")
    with row2_c3:
        sort_by = st.selectbox("Sort by", sort_options, index=sort_index)
    with row2_c4:
        top_n = st.number_input("Top N", value=scan_top_n, min_value=5, max_value=500)

    if st.button("Run Scan", use_container_width=False):
        with st.spinner("Scanning Polymarket... (fetching books may take 60-120s)"):
            scan_cmd = [
                sys.executable, str(SCAN_PATH),
                "--min-volume", str(int(min_vol)),
                "--min-reward", str(int(min_reward)),
                "--max-reward", str(int(max_reward)),
                "--min-spread", str(min_spread),
                "--max-spread", str(max_spread),
                "--min-bid-depth", str(int(min_bid_depth)),
                "--sort-by", sort_by,
                "--top", str(int(top_n)),
                "--json",
            ]
            proc_json = subprocess.run(
                scan_cmd,
                cwd=str(BASE_DIR), capture_output=True, text=True,
                timeout=300,
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
                    "share_balance": ":.1f",
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
            # Also track sides already in config (lookup by both YES and NO token)
            existing_sides: dict[str, str] = {}  # token_id -> side stored in config
            for m in cfg.get("markets", []):
                existing_sides[m["token_id"]] = m.get("side", "YES")
            for m in cfg.get("night_markets", []):
                existing_sides[m["token_id"]] = m.get("side", "YES")

            # Config version hash — forces data_editor to refresh after saves
            _cfg_sig = hash(frozenset(existing_tokens | existing_night_tokens))

            def _detect_in_config(item: dict) -> bool:
                yes_tid = item.get("token_id", "")
                no_tid = item.get("paired_token_id", "")
                return yes_tid in existing_tokens or no_tid in existing_tokens

            def _detect_in_night(item: dict) -> bool:
                yes_tid = item.get("token_id", "")
                no_tid = item.get("paired_token_id", "")
                return yes_tid in existing_night_tokens or no_tid in existing_night_tokens

            def _detect_side(item: dict) -> str:
                """Detect which side is already in config; default YES."""
                no_tid = item.get("paired_token_id", "")
                if no_tid in existing_tokens or no_tid in existing_night_tokens:
                    return "NO"
                return existing_sides.get(item.get("token_id", ""), "YES")

            def _fmt_start_time(it: dict) -> str:
                ts = it.get("gameStartTs")
                if not ts:
                    return "—"
                try:
                    return datetime.fromtimestamp(float(ts), _BJT).strftime("%m-%d %H:%M")
                except Exception:
                    return "—"

            def _is_sport_item(it: dict) -> str:
                # Match engine.py _is_sports_market secondary check:
                # gameStartTs populated AND slug contains an explicit YYYY-MM-DD
                # (rules out non-sports markets like geopolitical resolution dates
                # that also have gameStartTime set, e.g. "...-before-2027").
                if not it.get("gameStartTs"):
                    return "—"
                slug = str(it.get("slug") or "")
                return "✓" if _re.search(r"\b\d{4}-\d{2}-\d{2}\b", slug) else "—"

            df_edit = pd.DataFrame([{
                "#":         idx + 1,
                "In Config": _detect_in_config(item),
                "夜盘":      _detect_in_night(item),
                "Market":    item.get("question", "")[:60],
                "打开链接":   item.get("market_url", f"https://polymarket.com/event/{item.get('slug','')}"),
                "是否体育":  _is_sport_item(item),
                "开赛时间":  _fmt_start_time(item),
                "Daily $":   round(item.get("reward", 0), 0),
                "Risk":      round(item.get("fill_risk", 0), 1),
                "Bal":       round(item.get("share_balance", 0), 1),
                "Crowd":     item.get("crowd", "?"),
                "Vol 24h":   round(item.get("volume24h", 0), 0),
                "Bid Depth": round(item.get("bidDepth", 0), 0),
                "Spread":    round(item.get("maxIncentiveSpread", 0), 3),
                "_token_id": item.get("token_id", ""),
                "_item":     json.dumps({k: v for k, v in item.items()
                                         if not k.startswith("_")}),
            } for idx, item in enumerate(table_results) if item.get("token_id")])

            # Bulk select: choose all / clear all per session.
            _sel_col1, _sel_col2 = st.columns(2)
            with _sel_col1:
                _day_action = st.radio(
                    "📌 日盘批量",
                    ["不变", "全选", "全清"],
                    horizontal=True,
                    index=0,
                    key=f"bulk_day_{len(scan_results)}_{_cfg_sig}",
                    help="选全选/全清后下方 In Config 列被全部 ✓ / 全清，再点应用选择生效",
                )
            with _sel_col2:
                _night_action = st.radio(
                    "🌙 夜盘批量",
                    ["不变", "全选", "全清"],
                    horizontal=True,
                    index=0,
                    key=f"bulk_night_{len(scan_results)}_{_cfg_sig}",
                )
            if _day_action == "全选":
                df_edit["In Config"] = True
            elif _day_action == "全清":
                df_edit["In Config"] = False
            if _night_action == "全选":
                df_edit["夜盘"] = True
            elif _night_action == "全清":
                df_edit["夜盘"] = False

            # Wrap in st.form so checkbox clicks do NOT trigger reruns —
            # changes are only applied when a submit button is pressed.
            with st.form(key=f"scan_form_{len(scan_results)}_{_cfg_sig}"):
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
                        "是否体育": st.column_config.TextColumn("是否体育", width="small", help="✓ = 体育赛事（会触发赛前 freeze/sweep）"),
                        "开赛时间": st.column_config.TextColumn("开赛时间", width="small", help="北京时间，体育赛事才会有"),
                        "Daily $": st.column_config.NumberColumn("Daily $", format="$%.0f"),
                        "Risk":    st.column_config.NumberColumn("Risk",    format="%.1f"),
                        "Vol 24h":   st.column_config.NumberColumn("Vol 24h", format="$%,.0f"),
                        "Bid Depth": st.column_config.NumberColumn("Bid Depth", format="$%,.0f", help="Bid-side order book total USDC"),
                        "Spread":    st.column_config.NumberColumn("Spread",  format="%.3f"),
                    },
                    disabled=["#", "Market", "是否体育", "开赛时间", "Daily $", "Risk", "Crowd", "Vol 24h", "Bid Depth", "Spread", "打开链接"],
                    key=f"scan_editor_{len(scan_results)}_{_cfg_sig}",
                )

                st.markdown("")
                submit_all = st.form_submit_button("应用选择（同时更新日盘 + 夜盘 Config）", type="primary")

            # ── Handle form submission (outside the form block) ───────────────
            if submit_all and _gate_write("scan/apply market selection"):
                new_markets = []
                checked_night = []
                for i, row in edited.iterrows():
                    item = json.loads(df_edit.at[i, "_item"])
                    yes_tid = item.get("token_id", "")
                    no_tid = item.get("paired_token_id", "")
                    if not yes_tid:
                        continue
                    # Side column removed from UI (dual-side always injected).
                    # Preserve existing side for markets already in config; default YES for new.
                    side = _detect_side(item)
                    tid = no_tid if side == "NO" and no_tid else yes_tid
                    # Determine paired token for dual-side injection
                    paired = no_tid if side == "YES" else yes_tid
                    if bool(row["In Config"]):
                        entry = {
                            "token_id": tid,
                            "side": side,
                            "max_incentive_spread": round(item.get("maxIncentiveSpread", 3.5), 4),
                            "price_tick": 0.01,
                            "min_distance_from_best_bid": 0.01,
                            "quote_size": 100.0,
                            "risk": "low" if item.get("quadrant", "").startswith("A") else "mid",
                            "enabled": True,
                        }
                        if paired:
                            entry["paired_token_id"] = paired
                        new_markets.append(entry)
                    if bool(row["夜盘"]):
                        entry = {
                            "token_id": tid,
                            "side": side,
                            "max_incentive_spread": round(item.get("maxIncentiveSpread", 3.5), 4),
                            "price_tick": 0.01,
                            "min_distance_from_best_bid": 0.02,
                            "quote_size": 80.0,
                            "risk": "low",
                            "enabled": True,
                        }
                        if paired:
                            entry["paired_token_id"] = paired
                        checked_night.append(entry)
                cfg["markets"] = new_markets
                cfg["night_markets"] = checked_night
                save_config(cfg)

                # In multi-account mode, propagate the markets/night_markets
                # sections to every config_N.json so the running engines pick up
                # the change. Without this, the scanner save would only touch
                # config.json (which multi_runner never reads), silently doing
                # nothing to live quoting.
                synced_configs: list[str] = ["config.json"]
                failed_configs: list[tuple[str, str]] = []
                for _i in range(1, 31):
                    _multi_cfg_path = MAKER_DIR / f"config_{_i}.json"
                    if not _multi_cfg_path.exists():
                        continue
                    try:
                        _mc = json.loads(_multi_cfg_path.read_text(encoding="utf-8"))
                        _mc["markets"] = new_markets
                        _mc["night_markets"] = checked_night
                        _multi_cfg_path.write_text(
                            json.dumps(_mc, ensure_ascii=False, indent=2),
                            encoding="utf-8",
                        )
                        synced_configs.append(_multi_cfg_path.name)
                    except Exception as _mcerr:
                        failed_configs.append((_multi_cfg_path.name, str(_mcerr)[:80]))

                # Persist success toast across the rerun below (otherwise the
                # message vanishes the moment the page reloads).
                _msg_parts = [
                    f"✅ 已保存：日盘 {len(new_markets)} 个，夜盘 {len(checked_night)} 个",
                    f"同步到 {len(synced_configs)} 个 config 文件: {', '.join(synced_configs)}",
                ]
                if failed_configs:
                    _msg_parts.append(
                        f"⚠️ {len(failed_configs)} 个 config 写入失败: "
                        + "; ".join(f"{n}({e})" for n, e in failed_configs)
                    )
                if len(synced_configs) > 1:  # multi-mode
                    _msg_parts.append("⚠️ engine 需要 Stop+Start 重启才会读取新 markets 列表")
                st.session_state.setdefault("_scan_toasts", []).append((
                    "✅" if not failed_configs else "⚠️",
                    " | ".join(_msg_parts),
                ))
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
                if _gate_write("proxy pool append"):
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
                if _gate_write("proxy pool replace"):
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
    _show_flash()

    # ── Engine Control & Accounts (merged) ────────────────────────────────────
    all_states = load_all_engine_states()
    alive_map  = multi_engine_running()

    st.markdown('<p class="section-title">Engine Control & Accounts</p>', unsafe_allow_html=True)

    _configured_ids = _configured_account_ids()
    _mr_running = _multi_runner_running()
    _mr_pid = _multi_runner_pid()
    _GRID_N = 10  # always render a 10-row planning grid for accounts 1..10

    # Read the data_editor's current "Selected" column from session_state.
    # Users check rows in the grid *without* triggering any action; the Start /
    # Stop buttons up top then apply to whatever is currently checked.
    def _selected_account_ids() -> list[int]:
        delta = st.session_state.get("multi_account_editor") or {}
        edited = delta.get("edited_rows", {}) if isinstance(delta, dict) else {}
        out: list[int] = []
        for idx in range(_GRID_N):
            acc_id = idx + 1
            if acc_id not in _configured_ids:
                continue
            if isinstance(edited, dict) and idx in edited and "Selected" in edited[idx]:
                if bool(edited[idx]["Selected"]):
                    out.append(acc_id)
        return out

    # Toolbar: Start / Stop apply to the rows you've checked (explicit — the
    # checkboxes themselves no longer auto-fire). Reload restarts the whole
    # multi_runner process. Load Rewards forces a rewards-cache refresh.
    _b1, _b2, _b3, _bspacer, _b6 = st.columns([1, 1, 1, 0.2, 1])
    with _b1:
        if st.button("Start", use_container_width=True, key="mr_start"):
            _sel = _selected_account_ids()
            _remote_map = _load_remote_accounts()
            _local_sel  = [i for i in _sel if i not in _remote_map]
            _remote_sel = [i for i in _sel if i in _remote_map]
            if not _sel:
                # No rows checked → local runner-level Start. Remote engines
                # are not auto-started here — explicit selection required.
                _flash(start_multi_runner(), "info")
            else:
                _msg_parts: list[str] = []
                # Remote accounts: SSH systemctl start; also clear any local
                # .account_N.paused flag so rsync doesn't re-pause it on VPS.
                for _i in _remote_sel:
                    _set_account_paused(_i, False)
                    _msg_parts.append(_remote_systemctl(_i, "start"))
                # Local accounts: existing pause/unpause + start runner logic.
                _newly_paused: list[int] = []
                if _local_sel:
                    for _i in _local_sel:
                        _set_account_paused(_i, False)
                    _local_others = [
                        i for i in _configured_ids
                        if i not in _local_sel and i not in _remote_map
                    ]
                    for _i in _local_others:
                        already_running = _mr_running and not _is_account_paused(_i)
                        if not already_running and not _is_account_paused(_i):
                            _set_account_paused(_i, True)
                            _newly_paused.append(_i)
                    if not _mr_running:
                        _msg_parts.append(start_multi_runner())
                _msg = f"▶️ 启动 {len(_sel)} 个: local={_local_sel} remote={_remote_sel}"
                if _newly_paused:
                    _msg += f" · 锁定停止 {_newly_paused}"
                if _msg_parts:
                    _msg += " | " + " · ".join(_msg_parts)
                st.session_state.setdefault("_pause_toasts", []).append(("▶️", _msg))
            st.rerun()
    with _b2:
        if st.button("Stop", use_container_width=True, key="mr_stop"):
            _sel = _selected_account_ids()
            _remote_map = _load_remote_accounts()
            _local_sel  = [i for i in _sel if i not in _remote_map]
            _remote_sel = [i for i in _sel if i in _remote_map]
            if not _sel:
                # No rows checked → cancel everything + kill local runner +
                # systemctl stop every remote engine.
                _local_msg = stop_multi_runner()
                _remote_msgs: list[str] = []
                for _i in _remote_map.keys():
                    _remote_msgs.append(_remote_systemctl(_i, "stop"))
                _full = _local_msg
                if _remote_msgs:
                    _full += " | " + " · ".join(_remote_msgs)
                _flash(_full, "info")
            else:
                # Stop selected: cancel orders + stop the engine for each
                # selected account. (Engines cancel their own orders on
                # SIGTERM during graceful shutdown.)
                _msg_parts: list[str] = []
                if _local_sel:
                    # stop_multi_runner cancels orders for every local
                    # configured account before killing the multi_runner
                    # PID. Surgical per-account local stop would need IPC
                    # into multi_runner; with one local account it's moot.
                    _msg_parts.append(stop_multi_runner())
                for _i in _remote_sel:
                    _msg_parts.append(_remote_systemctl(_i, "stop"))
                _flash(
                    f"⏹ 停止 {len(_sel)} 个: local={_local_sel} remote={_remote_sel}"
                    + (" | " + " · ".join(_msg_parts) if _msg_parts else ""),
                    "info",
                )
            st.rerun()
    with _b3:
        if st.button("Reload", use_container_width=True, key="mr_reload"):
            stop_multi_runner()
            time.sleep(1)
            _flash(start_multi_runner(), "info")
            st.rerun()
    with _b6:
        _load_rewards = st.button("📊 Load Rewards", key="load_rewards_btn", use_container_width=True)

    # Status line — engine + signer + account roster, all one row.
    # Cache signer /health for 15 s so every rerun doesn't do a Tailscale RTT.
    @st.cache_data(ttl=15, show_spinner=False)
    def _signer_health(signer_url: str, signer_token: str) -> dict:
        import requests as _req
        _headers = {"Authorization": f"Bearer {signer_token}"} if signer_token else {}
        r = _req.get(f"{signer_url.rstrip('/')}/health", headers=_headers, timeout=8)
        r.raise_for_status()
        return r.json()

    signer_url = os.getenv("POLY_SIGNER_SERVER_URL", "").strip() or acc.get("signer_server_url", "")
    signer_token = os.getenv("SIGNER_TOKEN", "").strip() or acc.get("signer_token", "")
    if signer_url:
        try:
            _hdata = _signer_health(signer_url, signer_token)
            _signer_locked = _hdata.get("locked", False)
            if _signer_locked:
                _signer_pill = '<span class="pill-yellow">● LOCKED</span>'
            else:
                _funders_n = _hdata.get("funders_configured", 0)
                _signer_pill = f'<span class="pill-green">● ONLINE</span> <span class="pill-gray">{_funders_n} keys</span>'
        except Exception as _sigerr:
            _signer_pill = f'<span class="pill-red">● OFFLINE ({str(_sigerr)[:40]})</span>'
    else:
        _signer_pill = '<span class="pill-gray">not configured</span>'

    # Top engine pill reflects ANY live engine (single or multi VPS split).
    # multi_runner.pid only exists when a single-host multi_runner orchestrator
    # is used; in a VPS-per-account layout each engine runs directly via
    # systemd and only populates .engine_N.pid / .engine_N.heartbeat.
    _engine_pill = ('<span class="pill-green">● RUNNING</span>'
                    if (_mr_running or any(alive_map.values()))
                    else '<span class="pill-red">● STOPPED</span>')
    _roster_text = (
        f"{len(_configured_ids)} 个账号：config_{', config_'.join(str(i) for i in _configured_ids)}.json"
        if _configured_ids else "⚠️ 未检测到 config_N.json"
    )
    st.markdown(
        f"Engine {_engine_pill} PID `{_mr_pid}`  &nbsp;&nbsp;|&nbsp;&nbsp; "
        f"Signer {_signer_pill}  &nbsp;&nbsp;|&nbsp;&nbsp; {_roster_text}",
        unsafe_allow_html=True,
    )

    if not _configured_ids:
        st.warning(
            "未检测到任何 `config_N.json`——请先在终端跑："
            "`python scripts/generate_configs.py`（需要 `scripts/accounts.json`）"
        )

    if _load_rewards:
        st.caption("Loading rewards data...")

    # Dummy placeholder kept to minimise diff vs the old `_ctl1..4` block; the
    # real button wiring is in the single toolbar row above.
    if True:
        # Rewards: opt-in. Fetching is expensive (3 signed REST calls × N
        # accounts, each going through the Mac Mini signer over Tailscale), so
        # we only hit the API after the user clicks 📊 Load Rewards. Once
        # loaded, subsequent renders within 120 s cache hit instantly. Clicking
        # the button again forces a cache bust for a fresh fetch.
        # Polymarket reward epochs roll on UTC midnight, so the "today" date
        # we send the API must be UTC, not BJT — otherwise after 00:00 BJT
        # (= 16:00 UTC of previous calendar day) we query a future UTC day and
        # get $0 back even though the current UTC day still has live earnings.
        _today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        _rewards_by_acc: dict[int, dict] = {}
        _rewards_ids = _configured_account_ids()
        if _load_rewards:
            try:
                _fetch_rewards_for_account.clear()
            except Exception:
                pass
            st.session_state["_rewards_loaded"] = True
        if st.session_state.get("_rewards_loaded"):
            for acc_id in _rewards_ids:
                cfg_name = f"config_{acc_id}.json" if acc_id > 0 else "config.json"
                cfg_path = MAKER_DIR / cfg_name
                if cfg_path.exists():
                    try:
                        rw = _fetch_rewards_for_account(cfg_name, _today_str)
                        _rewards_by_acc[acc_id] = rw
                    except Exception:
                        _rewards_by_acc[acc_id] = {"error": "fetch failed"}

        # Load cumulative rewards from every rewards_cumulative*.json file in
        # DATA_DIR. The local engine writes rewards_cumulative.json; rsync from
        # other VPSes lands as rewards_cumulative_<host>.json. Per-account
        # entries from all files are merged (later files override earlier on
        # the same acc_id key — so make sure remote snapshots only contain the
        # accounts they own). The legacy per-address merge below then unifies
        # historical data across wallet rotations.
        _cum_accounts_raw: dict = {}
        for _cum_path in sorted(DATA_DIR.glob("rewards_cumulative*.json")):
            try:
                _state = json.loads(_cum_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            _accounts = _state.get("accounts", {}) if isinstance(_state, dict) else {}
            if isinstance(_accounts, dict):
                _cum_accounts_raw.update(_accounts)

        # Build address -> merged daily map, then we'll look up per-account by addr.
        _cum_by_address: dict[str, dict[str, float]] = {}
        for _ak, _av in (_cum_accounts_raw or {}).items():
            if not isinstance(_av, dict):
                continue
            _addr = str(_av.get("address", "") or "").lower()
            if not _addr:
                continue
            _daily = _cum_by_address.setdefault(_addr, {})
            for _d, _usd in (_av.get("daily", {}) or {}).items():
                try:
                    _v = float(_usd)
                except Exception:
                    _v = 0.0
                # If the same date appears in both "0" and "N", prefer the larger
                # (full-day snapshot beats a partial single-mode sample).
                if _v > _daily.get(_d, 0.0):
                    _daily[_d] = _v

        def _cum_usd_for_account(acc_id: int) -> tuple[float, int]:
            entry = _cum_accounts_raw.get(str(acc_id)) if isinstance(_cum_accounts_raw, dict) else None
            if not isinstance(entry, dict):
                return 0.0, 0
            addr = str(entry.get("address", "") or "").lower()
            daily = _cum_by_address.get(addr, {}) if addr else (entry.get("daily") or {})
            total = 0.0
            try:
                total = sum(float(v) for v in daily.values())
            except Exception:
                total = float(entry.get("cumulative_usd", 0) or 0)
            return round(total, 6), len(daily)

        # Always render the full 10-row planning grid (account slots 1..10).
        # Single-mode account 0 is surfaced in the caption above, not in the grid.
        _iter_ids = list(range(1, _GRID_N + 1))

        acc_rows = []
        _grand_rewards = 0.0
        _grand_fill_loss = 0.0
        _grand_fill_benefit = 0.0
        _grand_cumulative = 0.0
        for acc_id in _iter_ids:
            cfg_exists = (MAKER_DIR / (f"config_{acc_id}.json" if acc_id > 0 else "config.json")).exists()
            s = all_states.get(acc_id, {}) or {}
            label = f"Account {acc_id}" if acc_id > 0 else "Account (single)"

            # NOT CONFIGURED: placeholder row — checkbox ignored by Start/Stop handlers.
            if not cfg_exists and not s:
                acc_rows.append({
                    "Selected":     False,
                    "Account":      label,
                    "Status":       "NOT CONFIGURED",
                    "Balance $":    "—",
                    "Rewards $":    "—",
                    "Benefit $":    "—",
                    "Loss $":       "—",
                    "Total P&L $":  "—",
                    "累计 $":       "—",
                    "Markets":      0,
                    "Active":       0,
                    "Orders":       0,
                    "Fills 24h":    0,
                    "Unwinds":      0,
                    "Cooldown":     "—",
                    "State TS":     "—",
                })
                continue

            is_running = alive_map.get(acc_id, False)
            is_paused = _is_account_paused(acc_id) if acc_id > 0 else False
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
            # Fill P&L (24h window). Split into benefit (profitable exits) and
            # loss (unprofitable exits) so the dashboard can show them separately.
            # Data sources:
            #   1. `exit_records` are completed exits — their signed `loss` is the
            #      truth: positive = loss, negative = benefit.
            #   2. `pending_unwinds` are still unresolved; only their worst-case
            #      LOSS is surfaced (max(0, fp-sp)*sz). Unrealized benefit is NOT
            #      counted so we don't credit profit that hasn't settled yet.
            # Dedup by token_id so an unwind still in-flight isn't double-counted
            # once a matching exit_record is written for the same fill.
            fill_loss_24h = 0.0
            fill_benefit_24h = 0.0
            fills_24h_count = 0
            _counted_tokens: set[str] = set()
            for f in s.get("fills", []):
                if _now_unix - float(f.get("ts", 0) or 0) < 86400:
                    fills_24h_count += 1
            for ex in s.get("exit_records", []):
                if _now_unix - float(ex.get("ts", 0) or 0) < 86400:
                    _loss = float(ex.get("loss", 0) or 0)
                    if _loss > 0:
                        fill_loss_24h += _loss
                    elif _loss < 0:
                        fill_benefit_24h += -_loss  # benefit = |negative loss|
                    tid = str(ex.get("token_id", "") or "")
                    if tid:
                        _counted_tokens.add(tid)
            for uw in s.get("pending_unwinds", []):
                if _now_unix - float(uw.get("placed_at", 0) or 0) < 86400:
                    tid = str(uw.get("token_id", "") or "")
                    if tid and tid in _counted_tokens:
                        continue  # already reflected in exit_records
                    fp = float(uw.get("fill_price", 0) or 0)
                    sp = float(uw.get("sell_price", 0) or 0)
                    sz = float(uw.get("fill_size", 0) or 0)
                    if fp > 0 and sp > 0 and sz > 0:
                        fill_loss_24h += max(0.0, (fp - sp) * sz)

            # Override with realized P&L from on-chain trade history (data-api).
            # The engine's exit_records pipeline has been observed to drop entries
            # (esp. for manual SELLs that bypass _attempt_exit_sell), so this is
            # the more reliable source. Falls back to engine state on API failure.
            try:
                _cfg_path_for_acc = MAKER_DIR / (f"config_{acc_id}.json" if acc_id > 0 else "config.json")
                _funder_for_acc = ""
                if _cfg_path_for_acc.exists():
                    _cfg_for_acc = json.loads(_cfg_path_for_acc.read_text(encoding="utf-8"))
                    _funder_for_acc = (_cfg_for_acc.get("account", {}) or {}).get("funder", "") or ""
                if _funder_for_acc:
                    _pnl = _fetch_realized_pnl_for_funder(_funder_for_acc, 86400)
                    if not _pnl.get("error"):
                        fill_benefit_24h = float(_pnl.get("benefit_usd", 0) or 0)
                        fill_loss_24h = float(_pnl.get("loss_usd", 0) or 0)
                        fills_24h_count = max(fills_24h_count, int(_pnl.get("positions_closed_24h", 0) or 0))
            except Exception:
                pass  # keep engine-state-derived values on any failure
            pending_uw = len(s.get("pending_unwinds", []))
            cooldown   = s.get("cooldown_active", False)

            # Rewards data
            rw = _rewards_by_acc.get(acc_id, {})
            today_rewards = sum(
                float(t.get("earnings", 0) or 0) * float(t.get("asset_rate", 1) or 1)
                for t in rw.get("totals", [])
            ) if not rw.get("error") else 0.0
            n_reward_markets = len(rw.get("markets", []))

            _grand_rewards += today_rewards
            _grand_fill_loss += fill_loss_24h
            _grand_fill_benefit += fill_benefit_24h
            # Total P&L = passive rewards + realized exit benefit − realized exit loss
            net_pnl = today_rewards + fill_benefit_24h - fill_loss_24h

            # Cumulative rewards snapshot (from rewards_cumulative.json). Uses
            # the per-address merge so the pre-multi-mode history is included.
            cumulative_usd, _cum_days = _cum_usd_for_account(acc_id)
            _grand_cumulative += cumulative_usd

            # Two-state UX per Kevin's ask: STOPPED if not actively quoting
            # (either runner is down or the account has its pause flag set),
            # else RUNNING. The internal "pause flag" mechanism stays as-is —
            # just the label in the Status column merges both "flag set" and
            # "runner not running" into STOPPED so the button (Stop) matches
            # the status text (STOPPED) instead of confusing PAUSED.
            status_str = "RUNNING" if (is_running and not is_paused) else "STOPPED"
            acc_rows.append({
                # Checkbox is pure selection — default False every render so it
                # stays a deliberate user action. Current running state is
                # shown in the Status column (RUNNING / STOPPED).
                "Selected":     False,
                "Account":      label,
                "Status":       status_str,
                "Balance $":    f"{balance:,.2f}" if balance is not None else "—",
                "Rewards $":    f"{today_rewards:,.4f}" if today_rewards > 0 else "—",
                "Benefit $":    f"{fill_benefit_24h:,.2f}" if fill_benefit_24h > 0 else "—",
                "Loss $":       f"{fill_loss_24h:,.2f}" if fill_loss_24h > 0 else "—",
                "Total P&L $":  f"{net_pnl:+,.4f}" if (today_rewards > 0 or fill_loss_24h > 0 or fill_benefit_24h > 0) else "—",
                "累计 $":       f"{cumulative_usd:,.4f}" if cumulative_usd > 0 else "—",
                "Markets":      n_markets,
                "Active":       n_active,
                "Orders":       n_orders,
                "Fills 24h":    fills_24h_count,
                "Unwinds":      pending_uw,
                "Cooldown":     "YES" if cooldown else "—",
                "State TS":     state_ts,
            })

        df_acc = pd.DataFrame(acc_rows)

        # 10-row grid with a "Selected" checkbox column. **The checkbox is
        # passive** — ticking it doesn't start/stop anything on its own; the
        # top Start/Stop buttons apply to whichever rows are currently checked.
        # Live account state (RUNNING / PAUSED / STOPPED) is in the Status col.
        # Wrapped in @st.fragment so checkbox clicks rerun ONLY this widget,
        # not the whole page (which would re-fetch all expensive data and lock
        # the Start/Stop buttons during the spinner).
        @st.fragment
        def _account_editor_fragment(_df):
            st.data_editor(
                _df,
                column_config={
                    "Selected": st.column_config.CheckboxColumn(
                        "Selected",
                        help="勾选要批量操作的账号；再按顶部 Start / Stop 按钮才真正执行。"
                             " Start/Stop 不选任何行 = 作用于整个 multi_runner 进程。",
                        default=False,
                    ),
                },
                disabled=[c for c in _df.columns if c != "Selected"],
                hide_index=True,
                use_container_width=True,
                key="multi_account_editor",
            )

        _account_editor_fragment(df_acc)

        # Replay toasts queued by Start/Stop button handlers (they run before
        # the editor on rerun, so toasts live in session_state until this point).
        for _ic, _msg in st.session_state.pop("_pause_toasts", []):
            st.toast(_msg, icon=_ic)

        # Grand totals row. Streamlit's markdown parser treats paired `$` as
        # LaTeX math and eats content between them; that's why the previous
        # `<span>` showed up as raw text (reported 2026-04-23 and 2026-04-24).
        # Escape every dollar sign as the `&#36;` HTML entity so the parser
        # leaves the row alone.
        _grand_net = _grand_rewards + _grand_fill_benefit - _grand_fill_loss
        _pnl_color = "#3fb950" if _grand_net >= 0 else "#f85149"
        _usd = "&#36;"
        st.markdown(
            f'<b>Total: Rewards {_usd}{_grand_rewards:,.4f} | '
            f'Benefit {_usd}{_grand_fill_benefit:,.2f} | '
            f'Loss {_usd}{_grand_fill_loss:,.2f} | '
            f'当日总和 <span style="color:{_pnl_color}">'
            f'{_usd}{_grand_net:+,.4f}</span> | '
            f'累计 {_usd}{_grand_cumulative:,.4f}</b>',
            unsafe_allow_html=True,
        )

    # Real-time log monitor removed 2026-04-23 — Kevin: "不发挥作用，不如看 discord log"
    # Engine logs still go to data/engine.log; Discord webhook delivers the
    # important events with [N号] prefix.

# ── auto-refresh disabled (status bar uses st.fragment for partial refresh) ────
# st_autorefresh(interval=5000, key="auto_refresh")

st.markdown(
    "<p style='color:#30363d; font-size:11px; text-align:right; margin-top:24px;'>"
    "Latitude Alpha v2 — 2026</p>",
    unsafe_allow_html=True,
)
