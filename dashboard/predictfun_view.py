from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


REPO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_DIR / "data"
DRY_RUN_STATE = DATA_DIR / "predictfun_mainnet_state.json"
WS_STATE = DATA_DIR / "predictfun_mainnet_ws_state.json"
INTENTS_STATE = DATA_DIR / "predictfun_mainnet_desired_orders.json"
EXECUTION_REPORT = DATA_DIR / "predictfun_mainnet_execution_report.json"
RUNNER_STATE = DATA_DIR / "predictfun_mainnet_runner_state.json"
SIMULATION_STATE = DATA_DIR / "predictfun_mainnet_simulation_state.json"
RISK_STATE = DATA_DIR / "predictfun_mainnet_risk_state.json"
KILL_SWITCH_STATE = DATA_DIR / "predictfun_mainnet_kill_switch.json"
RESEARCH_STATE = DATA_DIR / "predictfun_mainnet_market_research.json"
CONFIG_PATH = REPO_DIR / "platforms/predictfun/maker/config.mainnet.json"
PID_PATH = DATA_DIR / "predictfun_mainnet_dry_run.pid"
LOG_PATH = DATA_DIR / "predictfun_mainnet_dry_run.log"


def apply_predictfun_styles() -> None:
    st.markdown(
        """
<style>
div[data-testid="stMetric"] {
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 8px;
    padding: 12px 14px;
}
div[data-testid="stMetric"] label {
    color: #8b949e !important;
    font-size: 11px;
    letter-spacing: .05em;
    text-transform: uppercase;
}
div[data-testid="stDataFrame"] { border: 1px solid #30363d; border-radius: 6px; }
.pf-muted { color:#8b949e; font-size:12px; }
.pf-ok { color:#3fb950; font-weight:700; }
.pf-warn { color:#d29922; font-weight:700; }
.pf-bad { color:#f85149; font-weight:700; }
.pf-pill {
    border-radius: 4px;
    padding: 3px 9px;
    font-size: 12px;
    font-weight: 700;
    display: inline-block;
    margin-right: 8px;
}
.pf-pill-ok { background:#1a3a1a; color:#3fb950; border:1px solid #238636; }
.pf-pill-warn { background:#2d2a1a; color:#d29922; border:1px solid #9e6a03; }
.pf-pill-bad { background:#3a1a1a; color:#f85149; border:1px solid #da3633; }
.pf-pill-gray { background:#21262d; color:#8b949e; border:1px solid #30363d; }
.pf-panel {
    background:#0d1117;
    border:1px solid #30363d;
    border-radius:8px;
    padding:14px 16px;
    margin-bottom:12px;
}
.pf-panel-title {
    color:#8b949e;
    font-size:11px;
    letter-spacing:.1em;
    text-transform:uppercase;
    margin-bottom:8px;
}
.pf-kv { color:#c9d1d9; font-size:13px; line-height:1.7; }
.pf-kv span { color:#8b949e; display:inline-block; min-width:130px; }
</style>
""",
        unsafe_allow_html=True,
    )


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_config() -> dict[str, Any]:
    return load_json(CONFIG_PATH)


def _configured_path(cfg: dict[str, Any], key: str, default: Path) -> Path:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    raw = str(out.get(key) or "").strip()
    if not raw:
        return default
    return (CONFIG_PATH.parent / raw).resolve()


def _read_pid(pid_path: Path) -> int | None:
    try:
        raw = pid_path.read_text(encoding="utf-8").strip()
        return int(raw)
    except Exception:
        return None


def _pid_running(pid: int | None) -> bool:
    if not pid or pid <= 0:
        return False
    if sys.platform.startswith("win"):
        proc = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return str(pid) in proc.stdout
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _start_dry_run_loop(config_path: Path, pid_path: Path, log_path: Path, interval_sec: int) -> str:
    pid = _read_pid(pid_path)
    if _pid_running(pid):
        return f"PF runner already running pid={pid}"

    pid_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("a", encoding="utf-8")
    kwargs: dict[str, Any] = {}
    if sys.platform.startswith("win") and hasattr(subprocess, "CREATE_NO_WINDOW"):
        kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "platforms.predictfun.maker.runner",
            "--config",
            str(config_path),
            "--interval-sec",
            str(interval_sec),
        ],
        cwd=str(REPO_DIR),
        stdout=log_file,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        close_fds=False,
        **kwargs,
    )
    log_file.close()
    pid_path.write_text(str(proc.pid), encoding="utf-8")
    return f"Started PF runner pid={proc.pid}"


def _stop_dry_run_loop(pid_path: Path) -> str:
    pid = _read_pid(pid_path)
    if not _pid_running(pid):
        if pid_path.exists():
            pid_path.unlink()
        return "PF runner is not running"
    assert pid is not None
    try:
        if sys.platform.startswith("win"):
            subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], capture_output=True, text=True, timeout=8)
        else:
            os.kill(pid, signal.SIGTERM)
    except OSError as exc:
        return f"Failed to stop PF runner pid={pid}: {exc}"
    if pid_path.exists():
        pid_path.unlink()
    return f"Stopped PF runner pid={pid}"


def _tail_text(path: Path, lines: int = 120) -> str:
    if not path.exists():
        return ""
    try:
        raw = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    return "\n".join(raw.splitlines()[-lines:])


def state_age(ts: str) -> str:
    if not ts:
        return "n/a"
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        seconds = max(0, (datetime.now(timezone.utc) - dt).total_seconds())
        if seconds < 60:
            return f"{seconds:.0f}s"
        if seconds < 3600:
            return f"{seconds / 60:.1f}m"
        return f"{seconds / 3600:.1f}h"
    except Exception:
        return "n/a"


def run_command(args: list[str], timeout: int = 90) -> tuple[int, str]:
    proc = subprocess.run(
        [sys.executable, *args],
        cwd=str(REPO_DIR),
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    output = "\n".join(x for x in [proc.stdout.strip(), proc.stderr.strip()] if x)
    return proc.returncode, output[-10000:]


def _as_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _fmt_ts(ts: str) -> str:
    if not ts:
        return "n/a"
    return ts.replace("T", " ").replace("Z", " UTC")


def _pill(label: str, tone: str) -> str:
    return f'<span class="pf-pill pf-pill-{tone}">{label}</span>'


def plans_frame(state: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for plan in state.get("plans", []):
        market = plan.get("market", {})
        yes_quotes = plan.get("yes_quotes", []) or []
        no_quotes = plan.get("no_quotes", []) or []
        rows.append(
            {
                "Status": "QUOTE" if plan.get("can_quote") else "SKIP",
                "Market ID": market.get("id"),
                "Title": market.get("title"),
                "Hourly": _as_float(market.get("hourly_rate")),
                "Mid": _as_float(plan.get("mid")),
                "YES Bid": _as_float(plan.get("best_yes_bid")),
                "YES Ask": _as_float(plan.get("best_yes_ask")),
                "Spread": _as_float(plan.get("best_yes_ask")) - _as_float(plan.get("best_yes_bid")),
                "YES Quotes": len(yes_quotes),
                "NO Quotes": len(no_quotes),
                "Source": plan.get("orderbook_source") or "unknown",
                "Reason": plan.get("skip_reason") or "ok",
            }
        )
    return pd.DataFrame(rows)


def intents_frame(state: dict[str, Any]) -> pd.DataFrame:
    intents = state.get("intents") if isinstance(state.get("intents"), list) else []
    rows = []
    for item in intents:
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "Account": item.get("account_id") or "acct01",
                "Intent ID": item.get("intent_id"),
                "Market ID": item.get("market_id"),
                "Outcome": item.get("outcome"),
                "Side": item.get("side"),
                "Price": _as_float(item.get("price")),
                "Size": _as_float(item.get("size")),
                "Notional": _as_float(item.get("notional")),
                "Reason": item.get("reason"),
            }
        )
    return pd.DataFrame(rows)


def accounts_frame(state: dict[str, Any]) -> pd.DataFrame:
    accounts = state.get("accounts") if isinstance(state.get("accounts"), dict) else {}
    rows = []
    for account_id, item in sorted(accounts.items()):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "Account": account_id,
                "Desired Orders": int(item.get("desired") or 0),
                "Desired Notional": _as_float(item.get("total_notional")),
            }
        )
    return pd.DataFrame(rows)


def ws_books_frame(state: dict[str, Any]) -> pd.DataFrame:
    books = state.get("orderbooks") if isinstance(state.get("orderbooks"), dict) else {}
    rows = []
    for market_id, book in books.items():
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        rows.append(
            {
                "Market ID": market_id,
                "Best Bid": bids[0][0] if bids else None,
                "Best Ask": asks[0][0] if asks else None,
                "Bid Levels": len(bids),
                "Ask Levels": len(asks),
                "Update Ms": book.get("updateTimestampMs"),
            }
        )
    return pd.DataFrame(rows)


def liquidity_frame(state: dict[str, Any]) -> pd.DataFrame:
    liquidity = state.get("liquidity") if isinstance(state.get("liquidity"), dict) else {}
    rows = []
    for market_id, item in sorted(liquidity.items()):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "Market ID": market_id,
                "Bid Depth $": _as_float(item.get("bid_notional")),
                "Ask Depth $": _as_float(item.get("ask_notional")),
                "Bid Shares": _as_float(item.get("bid_shares")),
                "Ask Shares": _as_float(item.get("ask_shares")),
                "Samples": int(item.get("samples") or 0),
                "Updated": item.get("updated_at"),
            }
        )
    return pd.DataFrame(rows)


def liquidity_alerts_frame(state: dict[str, Any]) -> pd.DataFrame:
    alerts = state.get("liquidity_alerts") if isinstance(state.get("liquidity_alerts"), dict) else {}
    rows = []
    for market_id, item in sorted(alerts.items()):
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "Market ID": market_id,
                "Active": bool(item.get("active")),
                "Side": item.get("side"),
                "Reason": item.get("reason"),
                "Consumed %": _as_float(item.get("consumed_pct")) * 100,
                "Consumed $": _as_float(item.get("consumed_notional")),
                "Current $": _as_float(item.get("current_notional")),
                "Cooldown Until": item.get("cooldown_until"),
            }
        )
    return pd.DataFrame(rows)


def _cap_text(value: Any) -> str:
    amount = _as_float(value)
    return "unlimited" if amount <= 0 else f"${amount:,.2f}"

def _config_rows(title: str, values: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for key, value in values.items():
        if isinstance(value, bool):
            display = "enabled" if value else "disabled"
        else:
            display = str(value)
        rows.append({"Section": title, "Setting": key, "Value": display})
    return pd.DataFrame(rows)


def _output_rows(cfg: dict[str, Any]) -> pd.DataFrame:
    out = cfg.get("output") if isinstance(cfg.get("output"), dict) else {}
    rows = []
    for key, value in out.items():
        rows.append(
            {
                "Artifact": key,
                "Path": str(_configured_path({"output": {key: value}}, key, DATA_DIR / key)),
            }
        )
    return pd.DataFrame(rows)


def _dict_rows(data: dict[str, Any], *, nested_style: str = "summary") -> pd.DataFrame:
    rows = []
    for key, value in data.items():
        if isinstance(value, dict):
            display = f"{len(value)} fields" if nested_style == "summary" else json.dumps(value, ensure_ascii=False)
        elif isinstance(value, list):
            display = f"{len(value)} items" if nested_style == "summary" else json.dumps(value, ensure_ascii=False)
        else:
            display = value
        rows.append({"Field": key, "Value": display})
    return pd.DataFrame(rows)


def _list_frame(items: Any) -> pd.DataFrame:
    if not isinstance(items, list) or not items:
        return pd.DataFrame()
    if all(isinstance(item, dict) for item in items):
        return pd.DataFrame(items)
    return pd.DataFrame({"Value": [str(item) for item in items]})


def simulation_orders_frame(state: dict[str, Any]) -> pd.DataFrame:
    orders = state.get("active_orders") if isinstance(state.get("active_orders"), list) else []
    rows = []
    for item in orders:
        if isinstance(item, dict):
            rows.append(
                {
                    "Account": item.get("account_id") or "acct01",
                    "Intent ID": item.get("intent_id"),
                    "Market ID": item.get("market_id"),
                    "Outcome": item.get("outcome"),
                    "Side": item.get("side"),
                    "Price": _as_float(item.get("price")),
                    "Size": _as_float(item.get("size")),
                    "Notional": _as_float(item.get("notional")),
                    "Created": item.get("created_at"),
                }
            )
    return pd.DataFrame(rows)


def simulation_positions_frame(state: dict[str, Any]) -> pd.DataFrame:
    positions = state.get("positions") if isinstance(state.get("positions"), list) else []
    rows = []
    for item in positions:
        if isinstance(item, dict):
            rows.append(
                {
                    "Account": item.get("account_id") or "acct01",
                    "Market ID": item.get("market_id"),
                    "Outcome": item.get("outcome"),
                    "Size": _as_float(item.get("size")),
                    "Avg Cost": _as_float(item.get("avg_cost")),
                    "Mark": _as_float(item.get("mark")),
                    "Cost": _as_float(item.get("cost")),
                    "Mark Value": _as_float(item.get("mark_value")),
                    "uPnL": _as_float(item.get("unrealized_pnl")),
                }
            )
    return pd.DataFrame(rows)


def risk_checks_frame(state: dict[str, Any]) -> pd.DataFrame:
    checks = state.get("checks") if isinstance(state.get("checks"), list) else []
    return pd.DataFrame([row for row in checks if isinstance(row, dict)])


def research_frame(state: dict[str, Any]) -> pd.DataFrame:
    markets = state.get("markets") if isinstance(state.get("markets"), list) else []
    rows = []
    for item in markets:
        if isinstance(item, dict):
            rows.append(
                {
                    "Bucket": item.get("bucket"),
                    "Score": _as_float(item.get("suitability_score")),
                    "Market ID": item.get("market_id"),
                    "Title": item.get("title"),
                    "Variant": item.get("variant"),
                    "Hourly": _as_float(item.get("hourly_rate")),
                    "Mid": _as_float(item.get("mid")),
                    "Spread": _as_float(item.get("book_spread")),
                    "Quote Legs": item.get("quote_legs"),
                    "Can Quote": item.get("can_quote"),
                    "Notes": item.get("notes"),
                }
            )
    return pd.DataFrame(rows)


def _write_kill_switch(enabled: bool, reason: str = "") -> None:
    KILL_SWITCH_STATE.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "enabled": enabled,
        "reason": reason,
    }
    tmp = KILL_SWITCH_STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(KILL_SWITCH_STATE)


def _render_command_result() -> None:
    last = st.session_state.get("pf_last_command")
    if not last:
        return
    name, code, output = last
    if code == 0:
        st.success(f"{name} completed.")
    else:
        st.error(f"{name} failed with exit code {code}.")
    with st.expander("Command output", expanded=code != 0):
        st.code(output or "(no output)", language="text")


def render_predictfun_dashboard(*, embedded: bool = False) -> None:
    apply_predictfun_styles()

    cfg = load_config()
    scan_cfg = cfg.get("scan") if isinstance(cfg.get("scan"), dict) else {}
    strategy_cfg = cfg.get("strategy") if isinstance(cfg.get("strategy"), dict) else {}
    data_cfg = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
    risk_cfg = cfg.get("risk") if isinstance(cfg.get("risk"), dict) else {}
    liquidity_cfg = cfg.get("liquidity") if isinstance(cfg.get("liquidity"), dict) else {}
    sentinel_cfg = cfg.get("liquidity_sentinel") if isinstance(cfg.get("liquidity_sentinel"), dict) else {}
    dry_state_path = _configured_path(cfg, "state_path", DRY_RUN_STATE)
    ws_state_path = _configured_path(cfg, "ws_state_path", WS_STATE)
    intents_state_path = _configured_path(cfg, "intents_path", INTENTS_STATE)
    execution_report_path = _configured_path(cfg, "execution_report_path", EXECUTION_REPORT)
    runner_state_path = _configured_path(cfg, "runner_state_path", RUNNER_STATE)
    simulation_state_path = _configured_path(cfg, "simulation_state_path", SIMULATION_STATE)
    risk_state_path = _configured_path(cfg, "risk_state_path", RISK_STATE)
    kill_switch_state_path = _configured_path(cfg, "kill_switch_path", KILL_SWITCH_STATE)
    research_state_path = _configured_path(cfg, "research_state_path", RESEARCH_STATE)
    pid_path = _configured_path(cfg, "pid_path", PID_PATH)
    log_path = _configured_path(cfg, "log_path", LOG_PATH)
    interval_sec = int((cfg.get("runner") or {}).get("interval_sec") or 30)
    pid = _read_pid(pid_path)
    loop_running = _pid_running(pid)

    dry_state = load_json(dry_state_path)
    ws_state = load_json(ws_state_path)
    intents_state = load_json(intents_state_path)
    execution_report = load_json(execution_report_path)
    runner_state = load_json(runner_state_path)
    simulation_state = load_json(simulation_state_path)
    risk_state = load_json(risk_state_path)
    kill_switch_state = load_json(kill_switch_state_path)
    research_state = load_json(research_state_path)

    plans = dry_state.get("plans", []) if isinstance(dry_state.get("plans"), list) else []
    intent_summary = intents_state.get("summary") if isinstance(intents_state.get("summary"), dict) else {}
    exec_summary = execution_report.get("summary") if isinstance(execution_report.get("summary"), dict) else {}
    runner_plan = runner_state.get("last_plan_summary") if isinstance(runner_state.get("last_plan_summary"), dict) else {}
    runner_error = str(runner_state.get("last_error") or "")
    risk_summary = risk_state.get("summary") if isinstance(risk_state.get("summary"), dict) else {}
    simulation_summary = simulation_state.get("summary") if isinstance(simulation_state.get("summary"), dict) else {}
    research_summary = research_state.get("summary") if isinstance(research_state.get("summary"), dict) else {}
    risk_status = str(risk_state.get("status") or "UNKNOWN")

    quotable = sum(1 for p in plans if p.get("can_quote"))
    quote_legs = sum(len(p.get("yes_quotes", []) or []) + len(p.get("no_quotes", []) or []) for p in plans)
    desired = int(intent_summary.get("desired") or 0)
    total_notional = _as_float(intent_summary.get("total_notional"))
    active_accounts = int(intent_summary.get("accounts") or risk_summary.get("active_accounts") or 0)
    ws_books = len(ws_state.get("orderbooks") or {})
    sim_pnl = _as_float(simulation_summary.get("unrealized_pnl"))
    risk_blocked = bool(risk_state.get("blocked"))
    kill_enabled = bool(kill_switch_state.get("enabled"))

    if embedded:
        st.markdown("## Latitude Alpha")
        st.caption("Market Making / Predict.fun")
    else:
        st.title("Predict.fun Maker")
        st.caption("Market Making / Predict.fun")

    ws_ok = bool(ws_state.get("connected") or ws_state.get("last_connected") or ws_state.get("completed"))
    status_html = "".join(
        [
            _pill(str(cfg.get("environment") or "mainnet").upper(), "ok"),
            _pill("RUNNER ON" if loop_running else "RUNNER OFF", "ok" if loop_running else "gray"),
            _pill("WS OK" if ws_ok else "WS IDLE", "ok" if ws_ok else "gray"),
            _pill(f"RISK {risk_status}", "bad" if risk_blocked else "warn" if risk_status == "WARN" else "ok" if risk_status == "OK" else "gray"),
            _pill("ERROR" if runner_error else "NO ERRORS", "bad" if runner_error else "ok"),
            _pill("CAPITAL REUSE", "ok"),
        ]
    )
    st.markdown(status_html, unsafe_allow_html=True)
    st.caption(
        f"PF mainnet maker console | API {cfg.get('base_url', 'n/a')} | "
        f"runner interval {interval_sec}s | refresh {datetime.now().strftime('%H:%M:%S')}"
    )
    st.divider()

    m1, m2, m3, m4, m5, m6, m7, m8 = st.columns(8)
    m1.metric("Runner", "RUNNING" if loop_running else "STOPPED", f"pid={pid}" if loop_running else None)
    m2.metric("Cycles", int(runner_state.get("cycle_count") or 0), f"errors={runner_state.get('error_count', 0)}")
    m3.metric("Markets", len(plans), f"quotable={quotable}")
    m4.metric("Quote Legs", quote_legs)
    m5.metric("Desired Orders", desired, f"${total_notional:,.2f}")
    m6.metric("Accounts", active_accounts, f"ws books={ws_books}")
    m7.metric("Risk", risk_status, f"blocked={risk_summary.get('blocked', 0)}")
    m8.metric("Sim uPnL", f"${sim_pnl:,.2f}", f"fills={simulation_summary.get('fills_total', 0)}")

    a1, a2, a3, a4 = st.columns([2, 2, 2, 2])
    with a1:
        st.markdown(
            f"""
<div class="pf-panel">
  <div class="pf-panel-title">Runner</div>
  <div class="pf-kv"><span>last cycle</span>{_fmt_ts(str(runner_state.get("last_cycle_finished_at") or ""))}</div>
  <div class="pf-kv"><span>last plan</span>{runner_plan.get("plans", 0)} markets / {runner_plan.get("quotable", 0)} quotable</div>
  <div class="pf-kv"><span>fast requote</span>{"yes" if runner_state.get("fast_requote") else "no"}</div>
  <div class="pf-kv"><span>last error</span>{runner_error or "none"}</div>
</div>
""",
            unsafe_allow_html=True,
        )
    with a2:
        st.markdown(
            f"""
<div class="pf-panel">
  <div class="pf-panel-title">Orders</div>
  <div class="pf-kv"><span>create</span>{intent_summary.get("create", 0)}</div>
  <div class="pf-kv"><span>keep</span>{intent_summary.get("keep", 0)}</div>
  <div class="pf-kv"><span>cancel</span>{intent_summary.get("cancel", 0)}</div>
  <div class="pf-kv"><span>capital cap</span>{_cap_text(risk_cfg.get("max_account_desired_notional"))}</div>
  <div class="pf-kv"><span>market cap</span>{_cap_text(risk_cfg.get("max_account_market_desired_notional"))}</div>
</div>
""",
            unsafe_allow_html=True,
        )
    with a3:
        st.markdown(
            f"""
<div class="pf-panel">
  <div class="pf-panel-title">Execution</div>
  <div class="pf-kv"><span>actions</span>{exec_summary.get("actions", 0)}</div>
  <div class="pf-kv"><span>failed</span>{exec_summary.get("failed", 0)}</div>
  <div class="pf-kv"><span>source</span>{_fmt_ts(str(execution_report.get("source_ts") or ""))}</div>
</div>
""",
            unsafe_allow_html=True,
        )
    with a4:
        st.markdown(
            f"""
<div class="pf-panel">
  <div class="pf-panel-title">Inventory</div>
  <div class="pf-kv"><span>active orders</span>{simulation_summary.get("active_orders", 0)}</div>
  <div class="pf-kv"><span>position legs</span>{simulation_summary.get("position_legs", 0)}</div>
  <div class="pf-kv"><span>fills new</span>{simulation_summary.get("fills_new", 0)}</div>
</div>
""",
            unsafe_allow_html=True,
        )

    _render_command_result()

    tab_control, tab_markets, tab_orders, tab_sim, tab_risk, tab_research, tab_ws, tab_runner, tab_config = st.tabs(
        ["Control", "Markets", "Orders", "Simulation", "Risk", "Research", "WebSocket", "Runner / Logs", "Config"]
    )

    with tab_control:
        st.markdown("#### Control")
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        with c1:
            if st.button("Start Runner", use_container_width=True, disabled=loop_running):
                msg = _start_dry_run_loop(CONFIG_PATH, pid_path, log_path, interval_sec)
                st.session_state["pf_last_command"] = ("start-runner", 0, msg)
                st.rerun()
        with c2:
            if st.button("Stop Runner", use_container_width=True, disabled=not loop_running):
                msg = _stop_dry_run_loop(pid_path)
                st.session_state["pf_last_command"] = ("stop-runner", 0, msg)
                st.rerun()
        with c3:
            if st.button("One Cycle", use_container_width=True):
                code, output = run_command(["-m", "platforms.predictfun.maker.runner", "--config", str(CONFIG_PATH), "--once"])
                st.session_state["pf_last_command"] = ("runner-once", code, output)
                st.rerun()
        with c4:
            if st.button("WS Smoke", use_container_width=True):
                code, output = run_command(["-m", "platforms.predictfun.ws_watch", "--config", str(CONFIG_PATH), "--max-messages", "5", "--timeout-sec", "8"])
                st.session_state["pf_last_command"] = ("ws-smoke", code, output)
                st.rerun()
        with c5:
            if st.button("Reconcile", use_container_width=True):
                code, output = run_command(["-m", "platforms.predictfun.maker.reconcile", "--config", str(CONFIG_PATH)])
                st.session_state["pf_last_command"] = ("reconcile", code, output)
                st.rerun()
        with c6:
            if st.button("Self-Test", use_container_width=True):
                code, output = run_command(["-m", "platforms.predictfun.maker.selftest"])
                st.session_state["pf_last_command"] = ("self-test", code, output)
                st.rerun()

        if kill_enabled:
            st.warning(f"Manual halt file is active: {kill_switch_state.get('reason') or 'no reason'}")

        st.markdown("#### State Files")
        st.dataframe(
            pd.DataFrame(
                [
                    {"Name": "plans", "Path": str(dry_state_path), "Age": state_age(dry_state.get("ts", ""))},
                    {"Name": "intents", "Path": str(intents_state_path), "Age": state_age(intents_state.get("ts", ""))},
                    {"Name": "execution", "Path": str(execution_report_path), "Age": state_age(execution_report.get("ts", ""))},
                    {"Name": "simulation", "Path": str(simulation_state_path), "Age": state_age(simulation_state.get("ts", ""))},
                    {"Name": "risk", "Path": str(risk_state_path), "Age": state_age(risk_state.get("ts", ""))},
                    {"Name": "research", "Path": str(research_state_path), "Age": state_age(research_state.get("ts", ""))},
                    {"Name": "runner", "Path": str(runner_state_path), "Age": state_age(runner_state.get("ts", ""))},
                    {"Name": "websocket", "Path": str(ws_state_path), "Age": state_age(ws_state.get("ts", ""))},
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

    with tab_markets:
        df = plans_frame(dry_state)
        if df.empty:
            st.info("No PF plan state yet. Run One Cycle.")
        else:
            reason_counts = Counter(df["Reason"].fillna("ok").tolist())
            r1, r2, r3 = st.columns(3)
            r1.metric("Skipped", int((df["Status"] == "SKIP").sum()))
            r2.metric("Quoted Markets", int((df["Status"] == "QUOTE").sum()))
            r3.metric("Top Skip Reason", reason_counts.most_common(1)[0][0] if reason_counts else "n/a")

            def status_style(value: str) -> str:
                if value == "QUOTE":
                    return "color:#3fb950; font-weight:700"
                return "color:#d29922; font-weight:700"

            st.dataframe(
                df.style.map(status_style, subset=["Status"]),
                use_container_width=True,
                hide_index=True,
            )

    with tab_orders:
        st.info("Capital reuse mode: open-order notional is not capped by account principal. Guards focus on per-order size, per-market cap, and book quality.")
        o1, o2, o3, o4, o5 = st.columns(5)
        o1.metric("Desired", intent_summary.get("desired", 0))
        o2.metric("Create", intent_summary.get("create", 0))
        o3.metric("Keep", intent_summary.get("keep", 0))
        o4.metric("Cancel", intent_summary.get("cancel", 0))
        o5.metric("Notional", f"${total_notional:,.2f}")

        accounts_df = accounts_frame(intents_state)
        if not accounts_df.empty:
            st.markdown("#### Accounts")
            st.dataframe(accounts_df, use_container_width=True, hide_index=True)

        intents_df = intents_frame(intents_state)
        if intents_df.empty:
            st.info("No desired PF orders under current risk rules.")
        else:
            st.dataframe(intents_df, use_container_width=True, hide_index=True)

        diff = intents_state.get("diff") if isinstance(intents_state.get("diff"), dict) else {}
        d1, d2 = st.columns(2)
        with d1:
            st.markdown("#### Diff")
            diff_rows = pd.DataFrame(
                [
                    {"Action": "create", "Count": len(diff.get("create") or [])},
                    {"Action": "keep", "Count": len(diff.get("keep") or [])},
                    {"Action": "cancel", "Count": len(diff.get("cancel") or [])},
                ]
            )
            st.dataframe(diff_rows, use_container_width=True, hide_index=True)
            with st.expander("Diff detail", expanded=False):
                st.code(json.dumps(diff, indent=2, ensure_ascii=False), language="json")
        with d2:
            st.markdown("#### Execution Report")
            report_rows = _dict_rows(execution_report)
            if report_rows.empty:
                st.info("No execution report yet.")
            else:
                st.dataframe(report_rows, use_container_width=True, hide_index=True)
            with st.expander("Execution report detail", expanded=False):
                st.code(json.dumps(execution_report, indent=2, ensure_ascii=False), language="json")

    with tab_sim:
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("Active Orders", simulation_summary.get("active_orders", 0))
        s2.metric("Fills Total", simulation_summary.get("fills_total", 0), f"new={simulation_summary.get('fills_new', 0)}")
        s3.metric("Position Legs", simulation_summary.get("position_legs", 0))
        s4.metric("Marked Value", f"${_as_float(simulation_summary.get('marked_value')):,.2f}")
        s5.metric("Sim uPnL", f"${sim_pnl:,.2f}")

        sim_left, sim_right = st.columns(2)
        with sim_left:
            st.markdown("#### Simulated Active Orders")
            sim_orders = simulation_orders_frame(simulation_state)
            if sim_orders.empty:
                st.info("No simulated active orders yet.")
            else:
                st.dataframe(sim_orders, use_container_width=True, hide_index=True)
        with sim_right:
            st.markdown("#### Simulated Positions")
            sim_positions = simulation_positions_frame(simulation_state)
            if sim_positions.empty:
                st.info("No simulated positions yet.")
            else:
                st.dataframe(sim_positions, use_container_width=True, hide_index=True)

        st.markdown("#### Recent Simulated Fills")
        fills_df = _list_frame(simulation_state.get("fills", [])[-50:] if isinstance(simulation_state.get("fills"), list) else [])
        if fills_df.empty:
            st.info("No simulated fills yet. Passive orders only fill in the simulator when the book crosses the order price.")
        else:
            st.dataframe(fills_df, use_container_width=True, hide_index=True)
        with st.expander("Simulation detail", expanded=False):
            st.code(json.dumps(simulation_state, indent=2, ensure_ascii=False), language="json")

    with tab_risk:
        r1, r2, r3, r4 = st.columns(4)
        r1.metric("Status", risk_status)
        r2.metric("Blocked Checks", risk_summary.get("blocked", 0))
        r3.metric("Warnings", risk_summary.get("warn", 0))
        r4.metric("Desired Notional", f"${_as_float(risk_summary.get('desired_total_notional')):,.2f}")

        if risk_blocked:
            st.error("Risk gate is blocking PF execution.")
        elif risk_status == "WARN":
            st.warning("Risk gate has warnings, but execution is not blocked.")
        elif risk_status == "OK":
            st.success("Risk gate is clear.")
        else:
            st.info("No risk state yet. Run One Cycle.")

        risk_df = risk_checks_frame(risk_state)
        if risk_df.empty:
            st.info("No risk checks yet.")
        else:
            def risk_style(value: str) -> str:
                if value == "BLOCK":
                    return "color:#f85149; font-weight:700"
                if value == "WARN":
                    return "color:#d29922; font-weight:700"
                if value == "OK":
                    return "color:#3fb950; font-weight:700"
                return ""

            st.dataframe(risk_df.style.map(risk_style, subset=["status"]), use_container_width=True, hide_index=True)

        st.markdown("#### Manual Halt State")
        st.dataframe(_dict_rows(kill_switch_state), use_container_width=True, hide_index=True)
        with st.expander("Risk detail", expanded=False):
            st.code(json.dumps(risk_state, indent=2, ensure_ascii=False), language="json")

    with tab_research:
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Markets", research_summary.get("markets", 0))
        q2.metric("Tradable Now", research_summary.get("tradable_now", 0))
        q3.metric("Watchlist", research_summary.get("watchlist", 0))
        q4.metric("Avoid", research_summary.get("avoid", 0))

        research_df = research_frame(research_state)
        if research_df.empty:
            st.info("No PF market research state yet. Run One Cycle.")
        else:
            def bucket_style(value: str) -> str:
                if value == "tradable":
                    return "color:#3fb950; font-weight:700"
                if value == "watchlist":
                    return "color:#d29922; font-weight:700"
                if value == "avoid":
                    return "color:#f85149; font-weight:700"
                return ""

            st.dataframe(
                research_df.style.map(bucket_style, subset=["Bucket"]),
                use_container_width=True,
                hide_index=True,
            )
        with st.expander("Research detail", expanded=False):
            st.code(json.dumps(research_state, indent=2, ensure_ascii=False), language="json")

    with tab_ws:
        st.markdown(
            f"Status: {'CONNECTED' if ws_state.get('connected') else 'SUBSCRIBED' if ws_ok else 'IDLE'} | "
            f"age {state_age(ws_state.get('ts', ''))}"
        )
        if ws_state.get("error"):
            st.error(str(ws_state.get("error")))
        elif ws_state.get("note"):
            st.info(str(ws_state.get("note")))

        books_df = ws_books_frame(ws_state)
        if books_df.empty:
            st.info("No live WS orderbook payloads captured yet. Subscription ack is still shown in recent messages.")
        else:
            st.dataframe(books_df, use_container_width=True, hide_index=True)
        st.markdown("#### Liquidity Sentinel")
        liq_alerts = liquidity_alerts_frame(ws_state)
        if liq_alerts.empty:
            st.success("No active liquidity alerts.")
        else:
            st.warning("Liquidity alerts are active; affected markets are paused by planner until cooldown expires.")
            st.dataframe(liq_alerts, use_container_width=True, hide_index=True)
        liq_df = liquidity_frame(ws_state)
        if not liq_df.empty:
            st.dataframe(liq_df, use_container_width=True, hide_index=True)

        with st.expander("Recent WS Messages", expanded=False):
            messages_df = _list_frame(ws_state.get("messages", []))
            if messages_df.empty:
                st.info("No recent WS messages.")
            else:
                st.dataframe(messages_df, use_container_width=True, hide_index=True)
            st.code(json.dumps(ws_state.get("messages", []), indent=2, ensure_ascii=False), language="json")

    with tab_runner:
        st.markdown("#### Runner State")
        runner_rows = _dict_rows(runner_state)
        if runner_rows.empty:
            st.info("No runner state yet.")
        else:
            st.dataframe(runner_rows, use_container_width=True, hide_index=True)
        with st.expander("Runner state detail", expanded=False):
            st.code(json.dumps(runner_state, indent=2, ensure_ascii=False), language="json")
        st.markdown("#### Runner Log")
        st.code(_tail_text(log_path) or "No runner log yet.", language="text")
        st.markdown("#### Files")
        st.code(f"pid: {pid_path}\nlog: {log_path}\nconfig: {CONFIG_PATH}", language="text")

    with tab_config:
        st.markdown("#### PF Mainnet Config")
        env = str(cfg.get("environment") or "unknown").upper()
        api_key_env = str(cfg.get("api_key_env") or "not configured")
        live_ready = "planner/sim; live executor requires explicit command"

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Environment", env)
        k2.metric("Live Orders", "MANUAL")
        k3.metric("API Key Source", api_key_env)
        k4.metric("Mode", "planner", live_ready)

        st.markdown(
            f"""
<div class="pf-panel">
  <div class="pf-panel-title">Endpoint</div>
  <div class="pf-kv"><span>REST API</span>{cfg.get("base_url", "n/a")}</div>
  <div class="pf-kv"><span>WebSocket</span>{cfg.get("ws_url", "n/a")}</div>
  <div class="pf-kv"><span>API key</span>read from env var <code>{api_key_env}</code>; value hidden</div>
</div>
""",
            unsafe_allow_html=True,
        )

        simulation_cfg = cfg.get("simulation") if isinstance(cfg.get("simulation"), dict) else {}
        runner_cfg = cfg.get("runner") if isinstance(cfg.get("runner"), dict) else {}

        cfg_left, cfg_right = st.columns(2)
        with cfg_left:
            st.markdown("#### Market Selection")
            st.dataframe(_config_rows("Scan", scan_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Data Source")
            st.dataframe(_config_rows("Data", data_cfg), use_container_width=True, hide_index=True)
        with cfg_right:
            st.markdown("#### Quote Strategy")
            st.dataframe(_config_rows("Strategy", strategy_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Risk Guardrails")
            st.dataframe(_config_rows("Risk", risk_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Liquidity Guards")
            st.dataframe(_config_rows("Liquidity", liquidity_cfg), use_container_width=True, hide_index=True)
            st.dataframe(_config_rows("Liquidity Sentinel", sentinel_cfg), use_container_width=True, hide_index=True)

        st.markdown("#### Runner")
        st.dataframe(_config_rows("Runner", runner_cfg), use_container_width=True, hide_index=True)

        st.markdown("#### Simulation")
        st.dataframe(_config_rows("Simulation", simulation_cfg), use_container_width=True, hide_index=True)

        st.markdown("#### State Outputs")
        output_df = _output_rows(cfg)
        if output_df.empty:
            st.info("No output paths configured.")
        else:
            st.dataframe(output_df, use_container_width=True, hide_index=True)

        with st.expander("Debug JSON", expanded=False):
            raw_a, raw_b = st.columns(2)
            with raw_a:
                st.markdown("Config")
                st.code(json.dumps(cfg, indent=2, ensure_ascii=False), language="json")
                st.markdown("Plans")
                st.code(json.dumps(dry_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("Intents")
                st.code(json.dumps(intents_state, indent=2, ensure_ascii=False), language="json")
            with raw_b:
                st.markdown("Execution")
                st.code(json.dumps(execution_report, indent=2, ensure_ascii=False), language="json")
                st.markdown("Simulation")
                st.code(json.dumps(simulation_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("Risk")
                st.code(json.dumps(risk_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("Research")
                st.code(json.dumps(research_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("WebSocket")
                st.code(json.dumps(ws_state, indent=2, ensure_ascii=False), language="json")

    st.markdown(
        "<p class='pf-muted' style='text-align:right'>Predict.fun maker console - mainnet planner / simulated execution</p>",
        unsafe_allow_html=True,
    )
