"""Latitude Alpha 统一控制台(HTML shell + 只读数据 API)。

- GET /            → console.html(前端每 15s 拉 /api/state 覆盖真数据)
- GET /api/state   → 只读现有状态文件/库;缺失字段返回 null,前端保留模板示例值。

四源不混算铁律:vps1/decibel、vps1/var、vps2/decibel、vps2/var 逐源独立读取,
总计=真实来源相加;某源缺失就标缺失,绝不复制他源顶替。
只读:绝不写任何交易/worker/signer 文件。

环境变量:
  LATITUDE_DATA_DIR  pmbot 数据目录(默认仓库 data/;VPS=/home/ubuntu/polymarket-bot/data)
  VARIA_DATA_DIR     varia 数据目录(默认 /home/ubuntu/varia-decibel-farming-live/data)
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

APP_DIR = Path(__file__).resolve().parent
CONSOLE_HTML = APP_DIR / "console.html"
DATA_DIR = Path(os.getenv("LATITUDE_DATA_DIR", APP_DIR.parents[1] / "data"))
VARIA_DIR = Path(os.getenv("VARIA_DATA_DIR", "/home/ubuntu/varia-decibel-farming-live/data"))

STALE_SEC = 600  # 状态文件超过 10 分钟视为过期(展示但标注)

app = FastAPI(title="Latitude Alpha Console", docs_url=None, redoc_url=None)


# ---------- 只读辅助 ----------

def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _mtime_age(path: Path) -> Optional[int]:
    try:
        return int(time.time() - path.stat().st_mtime)
    except OSError:
        return None


def _age_text(secs: Optional[int]) -> Optional[str]:
    if secs is None:
        return None
    if secs < 90:
        return f"{secs}s 前"
    if secs < 5400:
        return f"{secs // 60}m 前"
    return f"{secs // 3600}h 前"


def _iso_age(value: Any) -> Optional[int]:
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        secs = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
        return max(0, int(secs))  # 时钟偏差防负数
    except Exception:
        return None


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------- Polymarket(engine_state_N 逐账号,不混算) ----------

def _polymarket() -> Dict[str, Any]:
    accounts: List[dict] = []
    running = live_orders = 0
    volume_today = pnl_today = 0.0
    quotes_sent = fills_seen = 0
    cooldown = False
    pm_fill_events: List[dict] = []
    for idx in range(1, 31):
        state = _read_json(DATA_DIR / f"engine_state_{idx}.json")
        if state is None and idx == 1:
            state = _read_json(DATA_DIR / "engine_state.json")
        if state is None:
            continue
        alive = (DATA_DIR / f".engine_{idx}.pid").exists() or (DATA_DIR / ".engine.pid").exists()
        paused = (DATA_DIR / f".account_{idx}.paused").exists()
        markets = state.get("markets") if isinstance(state.get("markets"), dict) else {}
        acct_orders = 0
        for m in markets.values():
            if isinstance(m, dict):
                orders = m.get("live_orders") or m.get("orders")
                acct_orders += len(orders) if isinstance(orders, list) else int(_num(m.get("live_order_count")) or 0)
        fills = state.get("fills") if isinstance(state.get("fills"), list) else []
        cutoff = time.time() - 86400
        ft = [f for f in fills if isinstance(f, dict) and (_num(f.get("ts")) or 0) >= cutoff]
        vol = sum(abs((_num(f.get("price")) or 0) * (_num(f.get("size")) or 0)) for f in ft)
        pnl = sum(_num(f.get("pnl")) or 0 for f in ft if f.get("pnl") is not None)
        for f in ft[-3:]:
            ts = _num(f.get("ts")) or 0
            pm_fill_events.append({
                "t": datetime.fromtimestamp(ts).strftime("%H:%M") if ts else "",
                "epoch": ts, "sev": "info",
                "msg": f"[PM·#{idx}] 成交 {f.get('side','')} {f.get('size','')} @{f.get('price','')}",
            })
        sibling = state.get("sibling_registry") if isinstance(state.get("sibling_registry"), dict) else {}
        running += 1 if (alive and not paused) else 0
        live_orders += acct_orders
        volume_today += vol
        pnl_today += pnl
        quotes_sent += int(_num(state.get("quotes_sent")) or 0)
        fills_seen += int(_num(state.get("fills_seen")) or 0)
        cooldown = cooldown or bool(state.get("cooldown_active"))
        funder = str(state.get("funder") or "")
        accounts.append({
            "idx": idx,
            "funder": (funder[:6] + "…" + funder[-3:]) if len(funder) > 12 else (funder or f"acct{idx}"),
            "status": "已暂停" if paused else ("运行中" if alive else "已停止"),
            "status_cls": "warn" if paused else ("ok" if alive else "danger"),
            "balance": _num(state.get("balance")),
            "orders": acct_orders,
            "fills_today": len(ft),
            "volume_today": round(vol, 2),
            "pnl_today": round(pnl, 2),
            "sibling_conflicts": sibling.get("conflicts_detected"),
            "sibling_mode": sibling.get("mode"),
            "age": _age_text(_mtime_age(DATA_DIR / (f"engine_state_{idx}.json" if (DATA_DIR / f"engine_state_{idx}.json").exists() else "engine_state.json"))),
        })
    rewards_total = None
    rewards = _read_json(DATA_DIR / "rewards_cumulative.json")
    if isinstance(rewards, dict) and isinstance(rewards.get("accounts"), (list, dict)):
        acc = rewards["accounts"]
        rows = acc if isinstance(acc, list) else list(acc.values())
        vals = [_num(r.get("total") or r.get("cumulative") or r.get("earnings")) for r in rows if isinstance(r, dict)]
        vals = [v for v in vals if v is not None]
        rewards_total = round(sum(vals), 2) if vals else None
    return {
        "present": bool(accounts), "accounts": accounts,
        "running": running, "total": len(accounts), "live_orders": live_orders,
        "volume_today": round(volume_today, 2), "pnl_today": round(pnl_today, 2),
        "quotes_sent": quotes_sent, "fills_seen": fills_seen,
        "cooldown": cooldown, "rewards_total": rewards_total,
        "fill_events": pm_fill_events[-6:],
    }


# ---------- Predict.fun(runner/risk 状态文件) ----------

def _predictfun() -> Dict[str, Any]:
    out: Dict[str, Any] = {"present": False}
    for prefix in ("predictfun_mainnet", "predictfun"):
        runner = _read_json(DATA_DIR / f"{prefix}_runner_state.json")
        if runner is None:
            continue
        out = {
            "present": True,
            "running": bool(runner.get("running")),
            "mode": runner.get("mode"),
            "environment": runner.get("environment"),
            "cycles": runner.get("cycle_count"),
            "errors": runner.get("error_count"),
            "last_cycle_age": _age_text(_iso_age(runner.get("last_cycle_finished_at"))),
            "last_error": (str(runner.get("last_error"))[:120] if runner.get("last_error") else None),
        }
        break
    return out


# ---------- Var/Decibel(四源:逐 host 逐 venue) ----------

def _pos_open(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    pos = payload.get("position") if isinstance(payload.get("position"), dict) else payload
    if not isinstance(pos, dict):
        return False
    for key in ("size", "position_size", "qty", "amount", "notional"):
        v = _num(pos.get(key))
        if v is not None and abs(v) > 1e-9:
            return True
    side = str(pos.get("side") or "").lower()
    return side in {"long", "short", "buy", "sell"}


def _var_decibel() -> Dict[str, Any]:
    peer_dir = VARIA_DIR / "ops_peer_state"
    hosts: Dict[str, dict] = {}
    # 口径同 varia 自家 _state_map:peer 目录打底,本机 ops_state.json 覆盖自己
    # 那台(peer 副本可能陈旧,本机最了解自己)。四源仍逐 host 逐 venue 独立。
    by_host: Dict[str, dict] = {}
    for path in (sorted(peer_dir.glob("*.json")) if peer_dir.exists() else []):
        state = _read_json(path)
        if isinstance(state, dict):
            h = str(state.get("host_id") or path.stem).lower()
            by_host["vps1" if h.startswith("vm-") else h] = state
    local = _read_json(VARIA_DIR / "ops_state.json")
    if isinstance(local, dict) and local.get("host_id"):
        h = str(local["host_id"]).lower()
        by_host["vps1" if h.startswith("vm-") else h] = local
    sources = list(by_host.values())
    equity_total = 0.0
    equity_found = False
    points_dec = points_var = None
    vol_weekly = vol_total = 0.0
    vol_found = False
    single_leg: List[str] = []
    for state in sources:
        if not isinstance(state, dict):
            continue
        host = str(state.get("host_id") or "").lower() or "unknown"
        if host.startswith("vm-"):
            host = "vps1"
        age = _iso_age(state.get("generated_at"))
        exchanges = state.get("exchanges") if isinstance(state.get("exchanges"), dict) else {}
        h: Dict[str, Any] = {"age_sec": age, "age": _age_text(age),
                             "stale": (age is None or age > STALE_SEC)}
        dec_syms = {}
        var_syms = {}
        for venue in ("decibel", "variational"):
            payload = exchanges.get(venue) if isinstance(exchanges.get(venue), dict) else {}
            bal = payload.get("balance") if isinstance(payload.get("balance"), dict) else {}
            eq = _num(bal.get("total_equity"))
            h[f"equity_{venue[:3]}"] = eq
            h[f"ok_{venue[:3]}"] = payload.get("ok")
            if eq is not None and not h["stale"]:
                equity_total += eq
                equity_found = True
            if venue == "decibel":
                dec_syms = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
                pts = payload.get("points") if isinstance(payload.get("points"), dict) else {}
                rows = pts.get("breakdown") if isinstance(pts.get("breakdown"), list) else []
                vals = [_num(r.get("points")) for r in rows if isinstance(r, dict)]
                vals = [v for v in vals if v is not None]
                if vals:
                    points_dec = (points_dec or 0.0) + sum(vals)
            else:
                var_syms = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
                pts = payload.get("points") if isinstance(payload.get("points"), dict) else {}
                tp = _num(pts.get("total_points"))
                if tp is not None:
                    points_var = (points_var or 0.0) + tp
        # 单腿检测(轻量,口径同 varia _host_exposure_status 的核心判断)
        for symbol in sorted(set(dec_syms) | set(var_syms)):
            d_open = _pos_open(dec_syms.get(symbol))
            v_open = _pos_open(var_syms.get(symbol))
            if d_open != v_open:
                single_leg.append(f"{host.upper()}·{symbol}")
                h["single_leg"] = True
        tv = state.get("trade_volume") if isinstance(state.get("trade_volume"), dict) else {}
        for venue_data in (tv.get("venues") or {}).values():
            if isinstance(venue_data, dict):
                w = _num(venue_data.get("weekly_notional_usdc"))
                t = _num(venue_data.get("total_notional_usdc"))
                if w is not None:
                    vol_weekly += w
                    vol_found = True
                if t is not None:
                    vol_total += t
        hosts[host] = h
    return {
        "present": bool(hosts), "hosts": hosts,
        "equity_total": round(equity_total, 2) if equity_found else None,
        "points_decibel": round(points_dec, 1) if points_dec is not None else None,
        "points_variational": round(points_var, 1) if points_var is not None else None,
        "volume_weekly": round(vol_weekly, 2) if vol_found else None,
        "volume_total": round(vol_total, 2) if vol_found else None,
        "single_leg": single_leg,
    }


# ---------- Single Account ----------

def _single_account() -> Dict[str, Any]:
    state = _read_json(DATA_DIR / "single_account_paper_state.json")
    out: Dict[str, Any] = {"present": state is not None}
    if state:
        summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        out.update({
            "signals": summary.get("signals"), "actionable": summary.get("actionable"),
            "top_symbol": summary.get("top_symbol"), "top_strategy": summary.get("top_strategy"),
            "top_score": summary.get("top_score"),
            "age": _age_text(_mtime_age(DATA_DIR / "single_account_paper_state.json")),
        })
        skip = summary.get("skip_reasons") if isinstance(summary.get("skip_reasons"), dict) else {}
        total = sum(int(_num(v) or 0) for v in skip.values()) or 0
        out["skip_reasons"] = [
            {"reason": k, "count": int(_num(v) or 0),
             "pct": round(100 * (_num(v) or 0) / total) if total else 0}
            for k, v in sorted(skip.items(), key=lambda kv: -(_num(kv[1]) or 0))[:6]
        ]
        rows = state.get("decisions") if isinstance(state.get("decisions"), list) else []
        out["recent_decisions"] = [
            {"msg": f"{r.get('strategy','')} · {r.get('symbol','')} {r.get('decision','')} "
                    f"{r.get('score','')}({str(r.get('reason',''))[:24]})"}
            for r in rows[:4] if isinstance(r, dict)
        ]
    sim_db = DATA_DIR / "single_account_paper.db"
    if sim_db.exists():
        try:
            conn = sqlite3.connect(f"file:{sim_db}?mode=ro", uri=True)
            row = conn.execute("SELECT equity, drawdown FROM equity_snapshots ORDER BY ts DESC LIMIT 1").fetchone()
            if row:
                out["sim_equity"], out["sim_drawdown"] = row[0], row[1]
            curve = conn.execute("SELECT equity FROM equity_snapshots ORDER BY ts DESC LIMIT 96").fetchall()
            out["equity_curve"] = [r[0] for r in reversed(curve)]
            out["closed_trades"] = conn.execute("SELECT COUNT(*) FROM positions_closed").fetchone()[0]
            conn.close()
        except Exception:
            pass
    return out


# ---------- 记录器 / 事件流 / 告警 ----------

def _recorders() -> Dict[str, Any]:
    heartbeats = sorted(DATA_DIR.glob(".recorder_*.heartbeat"))
    market_db = DATA_DIR / "single_account_market.db"
    latest = None
    if market_db.exists() or heartbeats:
        latest = _age_text(_mtime_age(max([p for p in [market_db, *heartbeats] if p.exists()],
                                          key=lambda p: p.stat().st_mtime)))
    return {"present": bool(heartbeats) or market_db.exists(),
            "recorders": [p.stem.replace(".recorder_", "") for p in heartbeats],
            "market_db": market_db.exists(), "latest": latest}


_SEV = {"error": "crit", "failed": "crit", "critical": "crit", "warning": "warn", "warn": "warn"}


def _events(pm_fills: List[dict]) -> List[dict]:
    merged: List[dict] = list(pm_fills)
    path = VARIA_DIR / "ops_events.ndjson"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()[-15:]
    except Exception:
        lines = []
    for line in lines:
        try:
            ev = json.loads(line)
        except Exception:
            continue
        status = str(ev.get("status") or ev.get("kind") or "").lower()
        sev = _SEV.get(status, "crit" if ev.get("error") else "info")
        ts = str(ev.get("finished_at") or ev.get("timestamp") or "")
        age = _iso_age(ts)
        msg = str(ev.get("message") or ev.get("reason_label") or ev.get("job_kind") or ev.get("kind") or "")[:90]
        merged.append({
            "t": ts[11:16] if len(ts) >= 16 else ts,
            "epoch": (time.time() - age) if age is not None else 0,
            "sev": sev,
            "msg": f"[VAR/DEC·{str(ev.get('host') or '').upper()}] {msg}",
        })
    merged.sort(key=lambda e: -(e.get("epoch") or 0))
    return merged[:12]


def _alerts(vd: Dict[str, Any], pm: Dict[str, Any]) -> List[dict]:
    alerts: List[dict] = []
    for item in vd.get("single_leg") or []:
        alerts.append({"tag": "VAR/DEC", "msg": f"<b>{item} 单腿</b>:双腿不对称,janitor 应在处置", "page": "vardec"})
    for host, h in (vd.get("hosts") or {}).items():
        if h.get("age_sec") is not None and h["age_sec"] > STALE_SEC:
            alerts.append({"tag": "VAR/DEC", "msg": f"<b>{host.upper()} ops 心跳过期</b>:{_age_text(h['age_sec'])}", "page": "vardec"})
    if pm.get("cooldown"):
        alerts.append({"tag": "PM", "msg": "<b>Polymarket 冷却中</b>:kill-switch/冷却激活,暂停开新单", "page": "pm"})
    return alerts


@app.get("/api/state")
def api_state() -> JSONResponse:
    pm = _polymarket()
    vd = _var_decibel()
    return JSONResponse({
        "ts": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M"),
        "polymarket": pm,
        "predictfun": _predictfun(),
        "var_decibel": vd,
        "single_account": _single_account(),
        "recorders": _recorders(),
        "events": _events(pm.pop("fill_events", [])),
        "alerts": _alerts(vd, pm),
    })


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(CONSOLE_HTML.read_text(encoding="utf-8"))


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "data_dir": str(DATA_DIR), "varia_dir": str(VARIA_DIR),
            "data_dir_exists": DATA_DIR.exists(), "varia_dir_exists": VARIA_DIR.exists()}
