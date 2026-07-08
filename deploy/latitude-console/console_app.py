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
# 跨机只读数据源(tailnet 内):打新核算台已构建 JSON、router ipo 状态、mac-mini 状态导出器
ACCOUNT_OPS_URL = os.getenv("ACCOUNT_OPS_URL", "http://100.82.86.62:8081/data/dashboard.json")
IPO_STATE_URL = os.getenv("IPO_STATE_URL", "http://100.82.86.62:8080/dashboard/ipo/state")
MACMINI_STATUS_URL = os.getenv("MACMINI_STATUS_URL", "http://100.91.159.54:8620/status")

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
    rewards_by_addr: Dict[str, float] = {}
    rewards = _read_json(DATA_DIR / "rewards_cumulative.json")
    if isinstance(rewards, dict) and isinstance(rewards.get("accounts"), dict):
        for row in rewards["accounts"].values():
            if isinstance(row, dict) and row.get("address"):
                v = _num(row.get("cumulative_usd"))
                if v is not None:
                    rewards_by_addr[str(row["address"]).lower()] = v
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
            "rewards": rewards_by_addr.get(funder.lower()),
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
    rewards_total = round(sum(rewards_by_addr.values()), 2) if rewards_by_addr else None
    curator = _read_json(DATA_DIR / "auto_curator_state.json")
    curator_out = None
    if isinstance(curator, dict):
        mie = curator.get("markets_in_engine")
        curator_out = {
            "enabled": bool(curator.get("enabled")),
            "markets": (len(mie) if isinstance(mie, (list, dict)) else int(_num(mie) or 0)),
            "added_total": curator.get("added_total"),
            "rejected_total": curator.get("rejected_total"),
            "last_scan_age": _age_text(_iso_age(curator.get("last_scan_ts"))
                                       if isinstance(curator.get("last_scan_ts"), str)
                                       else (int(time.time() - curator["last_scan_ts"])
                                             if _num(curator.get("last_scan_ts")) else None)),
        }
    return {
        "curator": curator_out,
        "present": bool(accounts), "accounts": accounts,
        "running": running, "total": len(accounts), "live_orders": live_orders,
        "volume_today": round(volume_today, 2), "pnl_today": round(pnl_today, 2),
        "quotes_sent": quotes_sent, "fills_seen": fills_seen,
        "cooldown": cooldown, "rewards_total": rewards_total,
        "fill_events": pm_fill_events[-6:],
    }


# ---------- varia trades:今日量 / 损耗分解(本地 sqlite + peer,(host,id) 去重) ----------

def _parse_ts(value: Any) -> Optional[float]:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _varia_trades_today() -> Dict[str, Any]:
    rows: List[dict] = []
    db = VARIA_DIR / "hedge_bot.sqlite3"
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        cur = conn.execute(
            "SELECT id, host, status, timestamp_close, target_notional, funding_var, "
            "funding_decibel, realized_cost_bp, estimated_cost_bp, var_slippage_bp, "
            "decibel_slippage_bp, realized_pnl_usdc FROM trades "
            "WHERE timestamp_close >= datetime('now','-7 day')")
        names = [d[0] for d in cur.description]
        rows += [dict(zip(names, r)) for r in cur.fetchall()]
        conn.close()
    except Exception:
        pass
    peer_dir = VARIA_DIR / "peer_trades"
    cutoff_7d = time.time() - 7 * 86400
    for path in (sorted(peer_dir.glob("*.json")) if peer_dir.exists() else []):
        raw = _read_json(path)
        if not isinstance(raw, list):
            continue
        for r in raw:
            if not isinstance(r, dict):
                continue
            ts = _parse_ts(r.get("timestamp_close") or r.get("timestamp_open"))
            if ts is None or ts < cutoff_7d:
                continue
            r = dict(r)
            r.setdefault("host", path.stem)
            rows.append(r)
    # (host,id) 去重 + 只算 executed(口径同 varia dashboard);24h 与 7日双窗口
    seen = set()
    cutoff_24h = time.time() - 86400
    volume = pnl = fee = funding = slip = loss = 0.0
    loss_7d = 0.0
    loss_7d_by_host: Dict[str, float] = {}
    count = 0
    for r in rows:
        status = str(r.get("status") or "").strip().lower()
        if status not in ("", "executed"):
            continue
        host = str(r.get("host") or "").lower() or "unknown"
        key = (host, r.get("id"))
        if r.get("id") is not None and key in seen:
            continue
        seen.add(key)
        notional = abs(_num(r.get("target_notional")) or 0.0)
        r_pnl = _num(r.get("realized_pnl_usdc"))
        cost_bp = _num(r.get("realized_cost_bp"))
        if cost_bp is None:
            cost_bp = _num(r.get("estimated_cost_bp"))
        row_loss = (-r_pnl if (r_pnl is not None and r_pnl < 0) else
                    (abs(cost_bp) * notional / 10000.0 if (r_pnl is None and cost_bp is not None) else 0.0))
        loss_7d += row_loss
        loss_7d_by_host[host] = loss_7d_by_host.get(host, 0.0) + row_loss
        ts = _parse_ts(r.get("timestamp_close") or r.get("timestamp_open"))
        if ts is None or ts < cutoff_24h:
            continue
        volume += notional
        count += 1
        loss += row_loss
        if r_pnl is not None:
            pnl += r_pnl
        if cost_bp is not None:
            fee += abs(cost_bp) * notional / 10000.0
        funding += (_num(r.get("funding_var")) or 0.0) + (_num(r.get("funding_decibel")) or 0.0)
        slip += (abs(_num(r.get("var_slippage_bp")) or 0.0)
                 + abs(_num(r.get("decibel_slippage_bp")) or 0.0)) * notional / 10000.0
    return {
        "present": count > 0,
        "trades": count, "volume": round(volume, 2), "pnl": round(pnl, 2),
        "loss": round(loss, 2), "loss_7d": round(loss_7d, 2),
        "loss_7d_by_host": {h: round(v, 4) for h, v in loss_7d_by_host.items()},
        "loss_bps_wan": round(loss / volume * 10000.0, 2) if volume else None,
        "fee": round(fee, 2), "funding": round(funding, 2), "slip": round(slip, 2),
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
        risk = _read_json(DATA_DIR / f"{prefix}_risk_state.json")
        if isinstance(risk, dict):
            summary = risk.get("summary") if isinstance(risk.get("summary"), dict) else {}
            checks = risk.get("checks") if isinstance(risk.get("checks"), list) else []
            gates = []
            not_ok = 0
            for c in checks:
                if not isinstance(c, dict):
                    continue
                ok = str(c.get("status") or "").upper() == "OK"
                not_ok += 0 if ok else 1
                v, lim = _num(c.get("value")), _num(c.get("limit"))
                if v is not None and lim not in (None, 0):
                    gates.append({"name": str(c.get("name") or "")[:28], "value": v,
                                  "limit": lim, "pct": round(min(100.0, abs(v) / abs(lim) * 100)),
                                  "ok": ok})
            gates.sort(key=lambda g: -g["pct"])
            out["risk"] = {
                "blocked": summary.get("blocked"), "warn": summary.get("warn"),
                "checks_total": summary.get("checks"), "checks_not_ok": not_ok,
                "desired_notional": _num(summary.get("desired_total_notional")),
                "active_accounts": summary.get("active_accounts"),
                "sim_positions": summary.get("sim_positions"),
                "gates": gates[:8],
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
    pairs: List[dict] = []
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
        # 单腿检测 + 配对腿行(口径同 varia _host_exposure_status 的核心判断)
        for symbol in sorted(set(dec_syms) | set(var_syms)):
            d_pos = (dec_syms.get(symbol) or {}).get("position") if isinstance(dec_syms.get(symbol), dict) else {}
            v_pos = (var_syms.get(symbol) or {}).get("position") if isinstance(var_syms.get(symbol), dict) else {}
            d_pos = d_pos if isinstance(d_pos, dict) else {}
            v_pos = v_pos if isinstance(v_pos, dict) else {}
            d_open = _pos_open(dec_syms.get(symbol))
            v_open = _pos_open(var_syms.get(symbol))
            if not d_open and not v_open:
                continue
            if d_open != v_open:
                single_leg.append(f"{host.upper()}·{symbol}")
                h["single_leg"] = True

            def _leg(p: dict, is_open: bool) -> dict:
                sign = -1.0 if str(p.get("side") or "").lower() in ("short", "sell") else 1.0
                notional = _num(p.get("notional"))
                entry, liq = _num(p.get("entry_price")), _num(p.get("liquidation_price"))
                size = _num(p.get("size"))
                if (notional is None or notional == 0) and size and entry:
                    notional = abs(size) * entry  # 数据源 notional 缺失/为0时按 |size|×entry 推算
                liq_pct = (round(abs(entry - liq) / entry * 100) if entry and liq else None)
                return {"open": is_open, "side": str(p.get("side") or ""),
                        "notional": notional, "signed": (sign * notional) if (is_open and notional) else 0.0,
                        "liq_pct": liq_pct}

            var_leg, dec_leg = _leg(v_pos, v_open), _leg(d_pos, d_open)
            pairs.append({
                "host": host, "symbol": symbol, "var": var_leg, "dec": dec_leg,
                "net": round(var_leg["signed"] + dec_leg["signed"], 2),
                "status": ("HEDGED" if (d_open and v_open) else
                           ("DEC 裸腿" if d_open else "VAR 裸腿")),
            })
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
    auto = _read_json(VARIA_DIR / "auto_strategy_state.json")
    auto_ctl = None
    if isinstance(auto, dict):
        auto_ctl = {"enabled": bool(auto.get("enabled")), "mode": auto.get("mode"),
                    "hosts": auto.get("hosts") if isinstance(auto.get("hosts"), dict) else {}}
    return {
        "present": bool(hosts), "hosts": hosts, "auto": auto_ctl,
        "equity_total": round(equity_total, 2) if equity_found else None,
        "points_decibel": round(points_dec, 1) if points_dec is not None else None,
        "points_variational": round(points_var, 1) if points_var is not None else None,
        "volume_weekly": round(vol_weekly, 2) if vol_found else None,
        "volume_total": round(vol_total, 2) if vol_found else None,
        "single_leg": single_leg,
        "pairs": pairs[:12],
        "today": (today := _varia_trades_today()),
        "budget": _varia_budget({h: today.get("loss_7d_by_host", {}).get(h, 0.0)
                                 for h in (hosts or {"vps1": {}})}),
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


# ---------- 原生子视图明细(只读)----------

def _varia_detail() -> Dict[str, Any]:
    """varia 二级页原生数据:近期成交明细 + 统计聚合(替代 iframe)。"""
    trades: List[dict] = []
    db = VARIA_DIR / "hedge_bot.sqlite3"
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        cur = conn.execute(
            "SELECT id, host, symbol, timestamp_open, timestamp_close, target_notional, "
            "var_side, decibel_side, basis_open_bp, basis_close_bp, realized_pnl_usdc, "
            "realized_cost_bp, status, strategy FROM trades "
            "ORDER BY timestamp_close DESC LIMIT 40")
        names = [d[0] for d in cur.description]
        rows = [dict(zip(names, r)) for r in cur.fetchall()]
        conn.close()
    except Exception:
        rows = []
    for r in rows:
        tc = str(r.get("timestamp_close") or "")
        trades.append({
            "id": r.get("id"), "host": str(r.get("host") or "").upper(),
            "symbol": r.get("symbol"), "strategy": r.get("strategy"),
            "close": tc[5:16].replace("T", " ") if len(tc) >= 16 else tc,
            "notional": _num(r.get("target_notional")),
            "side": f"{r.get('var_side') or '?'}/{r.get('decibel_side') or '?'}",
            "basis": (f"{_num(r.get('basis_open_bp')):.1f}→{_num(r.get('basis_close_bp')):.1f}bp"
                      if _num(r.get("basis_open_bp")) is not None else "—"),
            "pnl": _num(r.get("realized_pnl_usdc")),
            "cost_bp": _num(r.get("realized_cost_bp")),
            "status": r.get("status"),
        })
    # 统计聚合(按 host / 按 symbol)
    by_host: Dict[str, dict] = {}
    by_symbol: Dict[str, dict] = {}
    for t in trades:
        for bucket, keyname in ((by_host, t["host"]), (by_symbol, str(t["symbol"] or "?"))):
            b = bucket.setdefault(keyname, {"trades": 0, "notional": 0.0, "pnl": 0.0, "wins": 0})
            b["trades"] += 1
            b["notional"] += t["notional"] or 0.0
            b["pnl"] += t["pnl"] or 0.0
            b["wins"] += 1 if (t["pnl"] or 0) > 0 else 0
    def _agg(d):
        return [{"name": k, "trades": v["trades"], "notional": round(v["notional"], 2),
                 "pnl": round(v["pnl"], 2),
                 "win_rate": round(v["wins"] / v["trades"] * 100) if v["trades"] else 0}
                for k, v in sorted(d.items(), key=lambda kv: -kv[1]["notional"])]
    return {"present": bool(trades), "trades": trades,
            "by_host": _agg(by_host), "by_symbol": _agg(by_symbol)}


def _pm_detail() -> Dict[str, Any]:
    """pm 二级页原生数据:各账号在做市场明细 + 成交流(engine_state)。"""
    markets: List[dict] = []
    fills: List[dict] = []
    for idx in range(1, 31):
        state = _read_json(DATA_DIR / f"engine_state_{idx}.json")
        if state is None and idx == 1:
            state = _read_json(DATA_DIR / "engine_state.json")
        if not isinstance(state, dict):
            continue
        mk = state.get("markets") if isinstance(state.get("markets"), dict) else {}
        for tid, m in mk.items():
            if not isinstance(m, dict):
                continue
            orders = m.get("orders")
            markets.append({
                "account": idx, "token": str(tid)[:10],
                "mid": _num(m.get("mid")), "bid": _num(m.get("best_bid")),
                "ask": _num(m.get("best_ask")),
                "orders": len(orders) if isinstance(orders, list) else 0,
                "status": m.get("status") or m.get("event_state") or "—",
            })
        for f in (state.get("fills") if isinstance(state.get("fills"), list) else [])[-15:]:
            if not isinstance(f, dict):
                continue
            ts = _num(f.get("ts")) or 0
            fills.append({
                "account": idx,
                "t": datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts else "—",
                "epoch": ts,
                "side": f.get("side"), "price": _num(f.get("price")),
                "size": _num(f.get("size")), "pnl": _num(f.get("pnl")),
                "market": str(f.get("market") or f.get("slug") or f.get("asset_id") or "")[:18],
            })
    fills.sort(key=lambda x: -(x["epoch"] or 0))
    return {"present": bool(markets or fills), "markets": markets[:40], "fills": fills[:30]}


# ---------- 跨机只读拉取(带缓存,拉不到显示离线不阻塞) ----------

_HTTP_CACHE: Dict[str, tuple] = {}


def _do_fetch(url: str, timeout: float = 4.0) -> Optional[dict]:
    import urllib.request

    try:
        # tailnet 内网直连,显式绕过系统 HTTP 代理(本机 Clash 等会把内网 IP 打成 503)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def _fetch_json(url: str, ttl: float = 60.0, timeout: float = 4.0) -> Optional[dict]:
    """请求路径永不阻塞:命中缓存直接返回(哪怕已过期,后台线程会刷新);
    仅冷启动(缓存全空)时同步拉一次(短超时)。跨机源慢/断都不拖慢 /api/state。"""
    cached = _HTTP_CACHE.get(url)
    if cached is not None:
        return cached[0]  # 有缓存(含 None=上次拉失败)就立即返回,不阻塞
    data = _do_fetch(url, timeout=2.0)  # 冷启动:短超时同步一次
    _HTTP_CACHE[url] = (data, time.time())
    return data


_PREFETCH_URLS = [ACCOUNT_OPS_URL, IPO_STATE_URL, MACMINI_STATUS_URL]


def _prefetch_loop() -> None:
    """后台守护线程:每 20s 主动刷新跨机只读源到缓存,使请求路径始终命中热缓存。"""
    while True:
        for url in _PREFETCH_URLS:
            _HTTP_CACHE[url] = (_do_fetch(url, timeout=6.0), time.time())
        time.sleep(20)


@app.on_event("startup")
def _start_prefetch() -> None:
    import threading

    threading.Thread(target=_prefetch_loop, name="latitude-prefetch", daemon=True).start()


def _account_ops() -> Dict[str, Any]:
    d = _fetch_json(ACCOUNT_OPS_URL)
    if not isinstance(d, dict):
        return {"present": False}
    accounts = d.get("accounts") if isinstance(d.get("accounts"), list) else []
    capital = sum(_num(a.get("capital")) or 0.0 for a in accounts if isinstance(a, dict))
    income = sum(_num(a.get("income")) or 0.0 for a in accounts if isinstance(a, dict))
    wear = sum(_num(a.get("wear")) or 0.0 for a in accounts if isinstance(a, dict))
    reminders = ((d.get("reminders") or {}).get("summary")
                 if isinstance(d.get("reminders"), dict) else {}) or {}
    risks = d.get("risks") if isinstance(d.get("risks"), list) else []
    meta = d.get("meta") if isinstance(d.get("meta"), dict) else {}
    # ④ 人员明细:accounts 按 owner 聚合,share 从 people 表补
    share_by_name = {str(p.get("name") or ""): _num(p.get("share"))
                     for p in (d.get("people") or []) if isinstance(p, dict)}
    owners: Dict[str, dict] = {}
    for a in accounts:
        if not isinstance(a, dict):
            continue
        name = str(a.get("owner") or "未分配")
        o = owners.setdefault(name, {"name": name, "capital": 0.0, "income": 0.0,
                                     "wear": 0.0, "accounts": []})
        o["capital"] += _num(a.get("capital")) or 0.0
        o["income"] += _num(a.get("income")) or 0.0
        o["wear"] += _num(a.get("wear")) or 0.0
        if len(o["accounts"]) < 6:
            o["accounts"].append({"id": a.get("id"), "platform": a.get("platform"),
                                  "status": a.get("status"),
                                  "capital": round(_num(a.get("capital")) or 0.0, 2),
                                  "income": round(_num(a.get("income")) or 0.0, 2)})
    owner_rows = []
    for o in sorted(owners.values(), key=lambda x: -x["capital"])[:8]:
        owner_rows.append({
            **{k: (round(v, 2) if isinstance(v, float) else v) for k, v in o.items()},
            "roi_pct": round(o["income"] / o["capital"] * 100, 2) if o["capital"] else None,
            "wear_pct": round(o["wear"] / o["capital"] * 100, 2) if o["capital"] else None,
            "share": share_by_name.get(o["name"]),
        })
    return {
        "owners": owner_rows,
        "present": True,
        "accounts": len(accounts),
        "people": len(d.get("people") or []),
        "capital": round(capital, 2),
        "income": round(income, 2),
        "roi_pct": round(income / capital * 100, 2) if capital else None,
        "wear": round(wear, 2),
        "wear_pct": round(wear / capital * 100, 2) if capital else None,
        "pending": reminders.get("open"),
        "overdue": reminders.get("overdue"),
        "risk_count": len(risks),
        "risks": [{"level": str(r.get("level") or ""), "title": str(r.get("title") or "")[:40],
                   "meta": str(r.get("meta") or "")[:50], "value": str(r.get("value") or "")}
                  for r in risks[:5] if isinstance(r, dict)],
        "as_of": str(meta.get("as_of") or ""),
        "as_of_age": _age_text(_iso_age(meta.get("as_of"))),
    }


def _ipo() -> Dict[str, Any]:
    """① 打新工作台:router /dashboard/ipo/state(只读)。"""
    d = _fetch_json(IPO_STATE_URL, ttl=120.0)
    inner = (d or {}).get("ipo") if isinstance(d, dict) else None
    if not isinstance(inner, dict):
        return {"present": False}
    rnd = inner.get("round") if isinstance(inner.get("round"), dict) else {}
    stocks = inner.get("stocks") if isinstance(inner.get("stocks"), list) else []
    entries = inner.get("entries") if isinstance(inner.get("entries"), list) else []

    def _stock(s: dict) -> dict:
        return {"name": str(s.get("name") or s.get("title") or s.get("code") or "")[:24],
                "code": str(s.get("code") or ""),
                "score": s.get("score"),
                "fee": s.get("fee") or s.get("entryFee") or s.get("entry_fee"),
                "risk": str(s.get("risk") or s.get("riskLabel") or "")[:12],
                "note": str(s.get("note") or s.get("summary") or s.get("comment") or "")[:48]}

    def _entry(e: dict) -> dict:
        return {"account": str(e.get("account") or e.get("accountId") or "")[:14],
                "person": str(e.get("person") or e.get("owner") or "")[:10],
                "stock": str(e.get("stock") or e.get("code") or e.get("suggestion") or "")[:20],
                "fee": e.get("fee") or e.get("entryFee"),
                "due": str(e.get("due") or e.get("lockUntil") or e.get("deadline") or "")[:12],
                "status": str(e.get("status") or "")[:14],
                "reason": str(e.get("reason") or e.get("explain") or e.get("note") or "")[:40]}

    return {
        "present": True, "mode": inner.get("mode"),
        "round": {"title": rnd.get("title"), "code": rnd.get("code"),
                  "deadline": rnd.get("deadline"), "currency": rnd.get("currency")},
        "updated_age": _age_text(_iso_age(inner.get("updated_at"))),
        "stocks": [_stock(s) for s in stocks[:6] if isinstance(s, dict)],
        "entries": [_entry(e) for e in entries[:8] if isinstance(e, dict)],
        "stocks_total": len(stocks), "entries_total": len(entries),
    }


def _pf_intents() -> Dict[str, Any]:
    """② PF 模拟持仓&意向:desired_orders + execution_report(只读)。"""
    out: Dict[str, Any] = {"present": False}
    desired = _read_json(DATA_DIR / "predictfun_mainnet_desired_orders.json") \
        or _read_json(DATA_DIR / "predictfun_desired_orders.json")
    if isinstance(desired, dict):
        summary = desired.get("summary") if isinstance(desired.get("summary"), dict) else {}
        intents = desired.get("intents") if isinstance(desired.get("intents"), list) else []

        def _intent(i: dict) -> dict:
            market = str(i.get("market") or i.get("market_title") or i.get("market_id") or "")[:26]
            outcome = str(i.get("outcome") or "")[:10]
            return {"market": (market + (" · " + outcome if outcome else "")),
                    "side": str(i.get("side") or "")[:6],
                    "price": i.get("price"), "size": i.get("size") or i.get("quantity"),
                    "action": str(i.get("action") or i.get("op") or i.get("reason") or "")[:14],
                    "account": str(i.get("account") or i.get("account_id") or "")[:10]}
        out = {"present": True, "ts_age": _age_text(_iso_age(desired.get("ts"))),
               "summary": {k: summary.get(k) for k in list(summary)[:6]},
               "intents": [_intent(i) for i in intents[:6] if isinstance(i, dict)],
               "intents_total": len(intents)}
    report = _read_json(DATA_DIR / "predictfun_mainnet_execution_report.json")
    if isinstance(report, dict):
        rs = report.get("summary") if isinstance(report.get("summary"), dict) else {}
        out["exec_summary"] = {k: rs.get(k) for k in list(rs)[:6]}
    return out


def _budget_cap_for_host() -> Optional[float]:
    """生效的每周预算:优先 auto_strategy_state.json(varia dashboard 控件写入的
    生效值),回退 config.yaml。每 VPS 独立(各自 $5)。"""
    state = _read_json(VARIA_DIR / "auto_strategy_state.json") \
        or _read_json(VARIA_DIR / "auto_strategy_runtime.json")
    if isinstance(state, dict) and _num(state.get("weekly_loss_cap_usdc")) is not None:
        return _num(state.get("weekly_loss_cap_usdc"))
    try:
        for line in (VARIA_DIR.parent / "config.yaml").read_text(encoding="utf-8").splitlines():
            if "weekly_loss_cap_usdc" in line and ":" in line:
                return _num(line.split(":", 1)[1].strip().strip('"').strip("'"))
    except Exception:
        pass
    return None


def _varia_budget(loss_by_host: Dict[str, float]) -> Dict[str, Any]:
    """③ 本周预算:每 VPS 各自 cap − 各自近7日损耗(每VPS独立,合计=各源相加)。
    本机 cap 可读到生效值;peer VPS 的 cap 目前取同值(两台配置一致),标注口径。"""
    cap = _budget_cap_for_host()
    if cap is None:
        return {"present": False}
    hosts = {}
    total_remaining = total_cap = 0.0
    for host, loss in sorted(loss_by_host.items()):
        rem = max(0.0, cap - loss)
        hosts[host] = {"cap": cap, "loss_7d": round(loss, 2), "remaining": round(rem, 2)}
        total_remaining += rem
        total_cap += cap
    return {"present": True, "per_vps": True, "hosts": hosts,
            "cap_each": cap,
            "total_cap": round(total_cap, 2), "total_remaining": round(total_remaining, 2)}


def _macmini() -> Dict[str, Any]:
    d = _fetch_json(MACMINI_STATUS_URL, ttl=30.0)
    if not isinstance(d, dict):
        return {"present": False}
    services = d.get("services") if isinstance(d.get("services"), dict) else {}
    out = {"present": True, "age": _age_text(max(0, int(time.time() - (_num(d.get("ts")) or 0))))}
    for label, key in (("ai.codex.var-decibel-signer", "var_signer"),
                       ("ai.codex.predictfun-api-proxy", "pf_proxy"),
                       ("ai.codex.var-decibel-chrome-health", "chrome_health")):
        svc = services.get(label) if isinstance(services.get(label), dict) else {}
        out[key] = {"running": bool(svc.get("running")), "last_exit": svc.get("last_exit")}
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
    seen = set()
    sources = [VARIA_DIR / "ops_events.ndjson"]
    peer_dir = VARIA_DIR / "ops_peer_events"
    if peer_dir.exists():
        sources += sorted(peer_dir.glob("*.ndjson"))
    for path in sources:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()[-15:]
        except Exception:
            continue
        for line in lines:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            status = str(ev.get("status") or ev.get("kind") or "").lower()
            sev = _SEV.get(status, "crit" if ev.get("error") else "info")
            ts = str(ev.get("finished_at") or ev.get("timestamp") or "")
            key = (str(ev.get("host") or ""), ts, str(ev.get("kind") or ""))
            if key in seen:
                continue  # 本机文件与 peer 副本重叠时去重
            seen.add(key)
            age = _iso_age(ts)
            # 事件时间戳为 UTC(无时区后缀),显示统一转北京时间
            t_disp = ts[11:16] if len(ts) >= 16 else ts
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                from datetime import timedelta as _td

                t_disp = (dt.astimezone(timezone(_td(hours=8)))).strftime("%H:%M")
            except Exception:
                pass
            msg = str(ev.get("message") or ev.get("reason_label") or ev.get("job_kind")
                      or ev.get("kind") or "").replace("\n", " · ")[:150]
            merged.append({
                "t": t_disp,
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
        "account_ops": _account_ops(),
        "ipo": _ipo(),
        "pf_intents": _pf_intents(),
        "varia_detail": _varia_detail(),
        "pm_detail": _pm_detail(),
        "macmini": _macmini(),
        "events": _events(pm.pop("fill_events", [])),
        "alerts": _alerts(vd, pm),
        "writes_enabled": WRITES_ENABLED,
    })


# ---------- 受控写:varia 周预算(默认关闭,LATITUDE_ENABLE_WRITES=1 启用) ----------
# 写路径与 varia dashboard 自身控件完全一致:改 VPS1 的 auto_strategy_state.json,
# 由 varia 既有的 state 同步机制传播到 VPS2。带备份 + 审计日志 + 范围校验。

WRITES_ENABLED = os.getenv("LATITUDE_ENABLE_WRITES", "0") == "1"
AUDIT_LOG = DATA_DIR / "console_write_audit.jsonl"


@app.post("/api/varia/budget")
async def set_varia_budget(payload: dict) -> JSONResponse:
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用:待 Kevin 审阅后在服务环境设 "
                                                   "LATITUDE_ENABLE_WRITES=1(见 README)"}, status_code=403)
    cap = _num((payload or {}).get("cap"))
    if cap is None or not (0 <= cap <= 500):
        return JSONResponse({"ok": False, "error": "cap 需为 0–500 之间的数字"}, status_code=400)
    path = VARIA_DIR / "auto_strategy_state.json"
    state = _read_json(path)
    if not isinstance(state, dict):
        return JSONResponse({"ok": False, "error": "auto_strategy_state.json 不可读"}, status_code=500)
    old = state.get("weekly_loss_cap_usdc")
    backup = path.with_suffix(f".json.bak-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    backup.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    state["weekly_loss_cap_usdc"] = str(cap)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    with AUDIT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                             "action": "set_weekly_loss_cap", "old": old, "new": str(cap),
                             "backup": backup.name}, ensure_ascii=False) + "\n")
    return JSONResponse({"ok": True, "old": old, "new": str(cap),
                         "note": "已写入 VPS1 生效值;VPS2 由 varia 既有 state 同步机制传播"})


def _write_auto_strategy(updates: Dict[str, Any]) -> Dict[str, Any]:
    """A 类操作:部分更新 auto_strategy_state.json(保留其余字段),读改写+备份+审计。
    与 varia dashboard 自身 _write_auto_strategy_state 同一文件、同一 worker 消费路径。"""
    path = VARIA_DIR / "auto_strategy_state.json"
    state = _read_json(path)
    if not isinstance(state, dict):
        return {"ok": False, "error": "auto_strategy_state.json 不可读", "code": 500}
    before = {k: state.get(k) for k in updates}
    backup = path.with_suffix(f".json.bak-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    backup.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    state.update(updates)
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    with AUDIT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                             "action": "set_auto_strategy", "before": before,
                             "after": updates, "backup": backup.name}, ensure_ascii=False) + "\n")
    return {"ok": True, "before": before, "after": updates}


@app.post("/api/varia/auto")
async def set_varia_auto(payload: dict) -> JSONResponse:
    """A 类:自动运行总开关 / 半-全自动模式(文件写,worker 生效,可逆)。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    updates: Dict[str, Any] = {}
    if "enabled" in (payload or {}):
        updates["enabled"] = bool(payload["enabled"])
    if "mode" in (payload or {}):
        mode = str(payload["mode"])
        if mode not in ("semi_auto", "full_auto"):
            return JSONResponse({"ok": False, "error": "mode 须为 semi_auto/full_auto"}, status_code=400)
        updates["mode"] = mode
    if not updates:
        return JSONResponse({"ok": False, "error": "无有效字段"}, status_code=400)
    res = _write_auto_strategy(updates)
    return JSONResponse(res, status_code=res.pop("code", 200 if res.get("ok") else 500))


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(CONSOLE_HTML.read_text(encoding="utf-8"))


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "data_dir": str(DATA_DIR), "varia_dir": str(VARIA_DIR),
            "data_dir_exists": DATA_DIR.exists(), "varia_dir_exists": VARIA_DIR.exists()}
