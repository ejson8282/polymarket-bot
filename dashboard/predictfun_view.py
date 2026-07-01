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
import requests
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
MARKET_MODE_LABELS = {
    "standard": "Standard",
    "neg_risk": "Neg Risk",
    "yield_bearing": "Yield Bearing",
    "neg_risk_yield_bearing": "Neg Risk + Yield",
}


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
.section-title { color:#f0f6fc; font-size:18px; font-weight:700; margin:10px 0 8px; }
.pf-soft { background:#161b22; border:1px solid #30363d; border-radius:8px; padding:12px 14px; margin-bottom:10px; }
.pf-small { color:#8b949e; font-size:12px; }
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


@st.cache_data(ttl=20)
def fetch_allowances(base_url: str, account_id: str) -> dict[str, Any]:
    if not base_url or not account_id:
        return {"ok": False, "error": "missing_signer_or_account"}
    try:
        resp = requests.post(
            f"{base_url.rstrip()}/predictfun/accounts/{account_id}/allowances",
            timeout=12,
        )
        payload = resp.json() if resp.content else {}
    except Exception as exc:
        return {"ok": False, "error": f"{exc.__class__.__name__}: {exc}"}
    if not isinstance(payload, dict):
        return {"ok": False, "error": "non_object_response"}
    payload["status"] = resp.status_code
    return payload


def allowances_frame(payload: dict[str, Any], intents_state: dict[str, Any] | None = None) -> pd.DataFrame:
    modes = payload.get("modes") if isinstance(payload.get("modes"), dict) else {}
    planned = planned_buy_notional_by_mode(intents_state or {})
    rows: list[dict[str, Any]] = []
    for key, label in MARKET_MODE_LABELS.items():
        row = modes.get(key) if isinstance(modes.get(key), dict) else {}
        allowance = float(row.get("allowance") or 0)
        planned_buy = float(planned.get(key) or 0)
        headroom = allowance - planned_buy
        if planned_buy <= 0 and allowance <= 0:
            status = "Idle"
        elif allowance <= 0:
            status = "Needs approval"
        elif planned_buy > allowance:
            status = "Plan > allowance"
        else:
            status = "OK"
        rows.append(
            {
                "Mode": label,
                "Planned Buy": planned_buy,
                "Allowance": allowance,
                "Headroom": headroom,
                "Status": status,
            }
        )
    return pd.DataFrame(rows)


def planned_buy_notional_by_mode(intents_state: dict[str, Any]) -> dict[str, float]:
    out = {key: 0.0 for key in MARKET_MODE_LABELS}
    summary = intents_state.get("summary") if isinstance(intents_state.get("summary"), dict) else {}
    market_modes = summary.get("market_modes") if isinstance(summary.get("market_modes"), dict) else {}
    if market_modes:
        for key in MARKET_MODE_LABELS:
            row = market_modes.get(key) if isinstance(market_modes.get(key), dict) else {}
            out[key] = _as_float(row.get("buy_notional"))
        return out
    for item in intents_state.get("intents") or []:
        if not isinstance(item, dict) or str(item.get("side") or "").upper() != "BUY":
            continue
        mode = _intent_market_mode(item)
        out[mode] = out.get(mode, 0.0) + _as_float(item.get("notional"))
    return out


def _intent_market_mode(item: dict[str, Any]) -> str:
    mode = str(item.get("market_mode") or "").strip()
    if mode in MARKET_MODE_LABELS:
        return mode
    neg = _truthy(item.get("is_neg_risk"))
    yb = _truthy(item.get("is_yield_bearing"))
    if neg and yb:
        return "neg_risk_yield_bearing"
    if neg:
        return "neg_risk"
    if yb:
        return "yield_bearing"
    return "standard"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


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
    risk_cfg = cfg.get("risk") if isinstance(cfg.get("risk"), dict) else {}
    liquidity_cfg = cfg.get("liquidity") if isinstance(cfg.get("liquidity"), dict) else {}
    sentinel_cfg = cfg.get("liquidity_sentinel") if isinstance(cfg.get("liquidity_sentinel"), dict) else {}
    data_cfg = cfg.get("data") if isinstance(cfg.get("data"), dict) else {}
    simulation_cfg = cfg.get("simulation") if isinstance(cfg.get("simulation"), dict) else {}
    runner_cfg = cfg.get("runner") if isinstance(cfg.get("runner"), dict) else {}
    accounts_cfg = cfg.get("accounts") if isinstance(cfg.get("accounts"), dict) else {}
    signer_cfg = cfg.get("signer") if isinstance(cfg.get("signer"), dict) else {}
    account_ids = accounts_cfg.get("ids") if isinstance(accounts_cfg.get("ids"), list) else []
    primary_account = str(account_ids[0] if account_ids else "account_01")
    allowance_state = fetch_allowances(
        str(signer_cfg.get("base_url") or cfg.get("base_url") or ""),
        primary_account,
    )

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

    dry_state = load_json(dry_state_path)
    ws_state = load_json(ws_state_path)
    intents_state = load_json(intents_state_path)
    execution_report = load_json(execution_report_path)
    runner_state = load_json(runner_state_path)
    simulation_state = load_json(simulation_state_path)
    risk_state = load_json(risk_state_path)
    kill_switch_state = load_json(kill_switch_state_path)
    research_state = load_json(research_state_path)

    interval_sec = int(runner_cfg.get("interval_sec") or 30)
    pid = _read_pid(pid_path)
    loop_running = _pid_running(pid)

    plans = dry_state.get("plans", []) if isinstance(dry_state.get("plans"), list) else []
    intent_summary = intents_state.get("summary") if isinstance(intents_state.get("summary"), dict) else {}
    exec_summary = execution_report.get("summary") if isinstance(execution_report.get("summary"), dict) else {}
    runner_plan = runner_state.get("last_plan_summary") if isinstance(runner_state.get("last_plan_summary"), dict) else {}
    runner_error = str(runner_state.get("last_error") or "")
    risk_summary = risk_state.get("summary") if isinstance(risk_state.get("summary"), dict) else {}
    simulation_summary = simulation_state.get("summary") if isinstance(simulation_state.get("summary"), dict) else {}
    research_summary = research_state.get("summary") if isinstance(research_state.get("summary"), dict) else {}
    risk_status = str(risk_state.get("status") or "UNKNOWN")
    risk_blocked = bool(risk_state.get("blocked"))
    kill_enabled = bool(kill_switch_state.get("enabled"))
    ws_ok = bool(ws_state.get("connected") or ws_state.get("last_connected") or ws_state.get("completed"))
    liquidity_alerts = ws_state.get("liquidity_alerts") if isinstance(ws_state.get("liquidity_alerts"), dict) else {}
    active_liquidity_alerts = sum(1 for row in liquidity_alerts.values() if isinstance(row, dict) and row.get("active"))

    quotable = sum(1 for plan in plans if plan.get("can_quote"))
    quote_legs = sum(len(plan.get("yes_quotes", []) or []) + len(plan.get("no_quotes", []) or []) for plan in plans)
    desired = int(intent_summary.get("desired") or 0)
    total_notional = _as_float(intent_summary.get("total_notional"))
    active_accounts = int(intent_summary.get("accounts") or risk_summary.get("active_accounts") or 0)
    ws_books = len(ws_state.get("orderbooks") or {})
    sim_pnl = _as_float(simulation_summary.get("unrealized_pnl"))

    if embedded:
        st.markdown("## Predict.fun Maker")
    else:
        st.title("Predict.fun Maker")
    st.caption("Mainnet maker operations. Same layout as the Polymarket bot: control, markets, orders, scan, settings.")

    status_html = "".join(
        [
            _pill(str(cfg.get("environment") or "mainnet").upper(), "ok"),
            _pill("RUNNING" if loop_running else "STOPPED", "ok" if loop_running else "gray"),
            _pill("WS OK" if ws_ok else "WS IDLE", "ok" if ws_ok else "gray"),
            _pill(f"RISK {risk_status}", "bad" if risk_blocked else "warn" if risk_status == "WARN" else "ok" if risk_status == "OK" else "gray"),
            _pill("LIQ ALERT" if active_liquidity_alerts else "LIQ CLEAR", "warn" if active_liquidity_alerts else "ok"),
            _pill("CAPITAL REUSE", "ok"),
            _pill("ERROR" if runner_error else "NO ERRORS", "bad" if runner_error else "ok"),
        ]
    )
    st.markdown(status_html, unsafe_allow_html=True)

    if kill_enabled:
        st.warning(f"Manual halt file is active: {kill_switch_state.get('reason') or 'no reason'}")
    if runner_error:
        st.error(runner_error)

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Runner", "ON" if loop_running else "OFF", f"cycle {runner_state.get('cycle_count', 0)}")
    m2.metric("Markets", len(plans), f"quotable {quotable}")
    m3.metric("Orders", desired, f"${total_notional:,.2f}")
    m4.metric("WebSocket", ws_books, f"alerts {active_liquidity_alerts}")
    m5.metric("Sim PnL", f"${sim_pnl:,.2f}", f"fills {simulation_summary.get('fills_total', 0)}")

    _render_command_result()

    tab_control, tab_markets, tab_orders, tab_scan, tab_settings = st.tabs(
        ["Control", "Markets", "Orders / Fills", "Scan", "Settings"]
    )

    with tab_control:
        left, right = st.columns([1.05, 1.25])
        with left:
            st.markdown('<p class="section-title">Engine Control & Account</p>', unsafe_allow_html=True)
            b1, b2, b3, b4 = st.columns(4)
            with b1:
                if st.button("Start", use_container_width=True, disabled=loop_running):
                    msg = _start_dry_run_loop(CONFIG_PATH, pid_path, log_path, interval_sec)
                    st.session_state["pf_last_command"] = ("start-runner", 0, msg)
                    st.rerun()
            with b2:
                if st.button("Stop", use_container_width=True, disabled=not loop_running):
                    msg = _stop_dry_run_loop(pid_path)
                    st.session_state["pf_last_command"] = ("stop-runner", 0, msg)
                    st.rerun()
            with b3:
                if st.button("One Cycle", use_container_width=True):
                    code, output = run_command(["-m", "platforms.predictfun.maker.runner", "--config", str(CONFIG_PATH), "--once"])
                    st.session_state["pf_last_command"] = ("runner-once", code, output)
                    st.rerun()
            with b4:
                if st.button("WS Check", use_container_width=True):
                    code, output = run_command(["-m", "platforms.predictfun.ws_watch", "--config", str(CONFIG_PATH), "--max-messages", "5", "--timeout-sec", "8"])
                    st.session_state["pf_last_command"] = ("ws-check", code, output)
                    st.rerun()

            accounts = accounts_frame(intents_state)
            if accounts.empty:
                st.info("No account plan yet. Run One Cycle.")
            else:
                st.dataframe(accounts, use_container_width=True, hide_index=True)

            st.markdown('<p class="section-title">Collateral Allowance</p>', unsafe_allow_html=True)
            if allowance_state.get("ok"):
                st.caption(
                    f"{primary_account} · USDT balance {allowance_state.get('balance', '0')} · "
                    "live orders only work on modes with allowance."
                )
                allowance_df = allowances_frame(allowance_state, intents_state)

                def allowance_style(value: str) -> str:
                    if value == "OK":
                        return "color:#3fb950; font-weight:700"
                    if value in {"Idle", "Plan > allowance"}:
                        return "color:#d29922; font-weight:700"
                    return "color:#f85149; font-weight:700"

                st.dataframe(
                    allowance_df.style.map(allowance_style, subset=["Status"]).format(
                        {"Planned Buy": "${:,.2f}", "Allowance": "${:,.2f}", "Headroom": "${:,.2f}"}
                    ),
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.warning(f"Allowance check failed: {allowance_state.get('error') or allowance_state.get('status')}")

            st.markdown(
                f"""
<div class="pf-soft">
  <div class="pf-kv"><span>capital mode</span>reuse principal across maker orders</div>
  <div class="pf-kv"><span>account cap</span>{_cap_text(risk_cfg.get('max_account_desired_notional'))}</div>
  <div class="pf-kv"><span>market cap</span>{_cap_text(risk_cfg.get('max_account_market_desired_notional'))}</div>
  <div class="pf-kv"><span>single order</span>{_cap_text(strategy_cfg.get('max_order_notional'))}</div>
</div>
""",
                unsafe_allow_html=True,
            )

        with right:
            st.markdown('<p class="section-title">Status & Recent Log</p>', unsafe_allow_html=True)
            c1, c2, c3 = st.columns(3)
            c1.metric("Risk", risk_status, f"blocked {risk_summary.get('blocked', 0)}")
            c2.metric("Execution", exec_summary.get("actions", 0), f"failed {exec_summary.get('failed', 0)}")
            c3.metric("Freshness", state_age(runner_state.get("ts", "")), f"interval {interval_sec}s")
            st.markdown(
                f"""
<div class="pf-soft">
  <div class="pf-kv"><span>last cycle</span>{_fmt_ts(str(runner_state.get('last_cycle_finished_at') or ''))}</div>
  <div class="pf-kv"><span>last plan</span>{runner_plan.get('plans', 0)} markets / {runner_plan.get('quotable', 0)} quotable</div>
  <div class="pf-kv"><span>fast requote</span>{'yes' if runner_state.get('fast_requote') else 'no'}</div>
  <div class="pf-kv"><span>ws age</span>{state_age(ws_state.get('ts', ''))}</div>
</div>
""",
                unsafe_allow_html=True,
            )
            st.code(_tail_text(log_path, lines=35) or "No runner log yet.", language="text")

    with tab_markets:
        st.markdown('<p class="section-title">Markets</p>', unsafe_allow_html=True)
        df = plans_frame(dry_state)
        if df.empty:
            st.info("No plan state yet. Run One Cycle.")
        else:
            a, b, c, d = st.columns(4)
            a.metric("Quoted", int((df["Status"] == "QUOTE").sum()))
            b.metric("Skipped", int((df["Status"] == "SKIP").sum()))
            c.metric("Quote Legs", quote_legs)
            top_reason = Counter(df["Reason"].fillna("ok").tolist()).most_common(1)[0][0]
            d.metric("Top Skip", top_reason[:28])

            show = df[["Status", "Market ID", "Title", "Hourly", "Mid", "YES Bid", "YES Ask", "YES Quotes", "NO Quotes", "Reason"]]

            def status_style(value: str) -> str:
                if value == "QUOTE":
                    return "color:#3fb950; font-weight:700"
                return "color:#d29922; font-weight:700"

            st.dataframe(show.style.map(status_style, subset=["Status"]), use_container_width=True, hide_index=True)

        st.markdown('<p class="section-title">Liquidity Watch</p>', unsafe_allow_html=True)
        alerts_df = liquidity_alerts_frame(ws_state)
        if alerts_df.empty:
            st.success("No active liquidity alerts.")
        else:
            st.warning("Liquidity alert active: affected markets are paused until cooldown ends.")
            st.dataframe(alerts_df, use_container_width=True, hide_index=True)
        liq_df = liquidity_frame(ws_state)
        if not liq_df.empty:
            st.dataframe(liq_df, use_container_width=True, hide_index=True)

    with tab_orders:
        st.markdown('<p class="section-title">Orders / Fills</p>', unsafe_allow_html=True)
        o1, o2, o3, o4, o5 = st.columns(5)
        o1.metric("Desired", intent_summary.get("desired", 0))
        o2.metric("Create", intent_summary.get("create", 0))
        o3.metric("Keep", intent_summary.get("keep", 0))
        o4.metric("Cancel", intent_summary.get("cancel", 0))
        o5.metric("Notional", f"${total_notional:,.2f}")

        orders_df = intents_frame(intents_state)
        if orders_df.empty:
            st.info("No desired orders under current rules.")
        else:
            st.dataframe(orders_df[["Account", "Market ID", "Outcome", "Side", "Price", "Size", "Notional", "Reason"]], use_container_width=True, hide_index=True)

        sim_left, sim_right = st.columns(2)
        with sim_left:
            st.markdown("#### Simulated Active Orders")
            sim_orders = simulation_orders_frame(simulation_state)
            if sim_orders.empty:
                st.info("No simulated active orders yet.")
            else:
                st.dataframe(sim_orders, use_container_width=True, hide_index=True)
        with sim_right:
            st.markdown("#### Simulated Positions / Fills")
            pos_df = simulation_positions_frame(simulation_state)
            if pos_df.empty:
                st.info("No simulated positions.")
            else:
                st.dataframe(pos_df, use_container_width=True, hide_index=True)
            fills = simulation_state.get("fills", []) if isinstance(simulation_state.get("fills"), list) else []
            if fills:
                st.dataframe(_list_frame(fills[-30:]), use_container_width=True, hide_index=True)

    with tab_scan:
        st.markdown('<p class="section-title">Scan</p>', unsafe_allow_html=True)
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Markets", research_summary.get("markets", 0))
        q2.metric("Tradable", research_summary.get("tradable_now", 0))
        q3.metric("Watchlist", research_summary.get("watchlist", 0))
        q4.metric("Avoid", research_summary.get("avoid", 0))

        research_df = research_frame(research_state)
        if research_df.empty:
            st.info("No market research state yet. Run One Cycle.")
        else:
            def bucket_style(value: str) -> str:
                if value == "tradable":
                    return "color:#3fb950; font-weight:700"
                if value == "watchlist":
                    return "color:#d29922; font-weight:700"
                if value == "avoid":
                    return "color:#f85149; font-weight:700"
                return ""

            st.dataframe(research_df.style.map(bucket_style, subset=["Bucket"]), use_container_width=True, hide_index=True)

        with st.expander("Risk checks", expanded=False):
            risk_df = risk_checks_frame(risk_state)
            if risk_df.empty:
                st.info("No risk checks yet.")
            else:
                st.dataframe(risk_df, use_container_width=True, hide_index=True)

    with tab_settings:
        st.markdown('<p class="section-title">Settings</p>', unsafe_allow_html=True)
        cfg_left, cfg_right = st.columns(2)
        with cfg_left:
            st.markdown("#### Market Selection")
            st.dataframe(_config_rows("Scan", scan_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Quote Strategy")
            st.dataframe(_config_rows("Strategy", strategy_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Runner")
            st.dataframe(_config_rows("Runner", runner_cfg), use_container_width=True, hide_index=True)
        with cfg_right:
            st.markdown("#### Guardrails")
            st.dataframe(_config_rows("Risk", risk_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Liquidity")
            st.dataframe(_config_rows("Liquidity", liquidity_cfg), use_container_width=True, hide_index=True)
            st.dataframe(_config_rows("Liquidity Sentinel", sentinel_cfg), use_container_width=True, hide_index=True)
            st.markdown("#### Data")
            st.dataframe(_config_rows("Data", data_cfg), use_container_width=True, hide_index=True)

        with st.expander("State files", expanded=False):
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

        with st.expander("Debug JSON", expanded=False):
            raw_a, raw_b = st.columns(2)
            with raw_a:
                st.markdown("Plans")
                st.code(json.dumps(dry_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("Intents")
                st.code(json.dumps(intents_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("Execution")
                st.code(json.dumps(execution_report, indent=2, ensure_ascii=False), language="json")
            with raw_b:
                st.markdown("Runner")
                st.code(json.dumps(runner_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("Risk")
                st.code(json.dumps(risk_state, indent=2, ensure_ascii=False), language="json")
                st.markdown("WebSocket")
                st.code(json.dumps(ws_state, indent=2, ensure_ascii=False), language="json")

    st.markdown(
        "<p class='pf-muted' style='text-align:right'>Predict.fun maker console - compact operations layout</p>",
        unsafe_allow_html=True,
    )
