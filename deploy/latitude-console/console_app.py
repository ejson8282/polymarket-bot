"""Latitude Alpha 统一控制台(HTML shell + 只读数据 API)。

方向修正(施工包05 呈现层):原 5A/5B 在 Streamlit 里模仿模板,受框架限制无法
还原 latitude_console_full.html 的样子。本服务直接以该模板为前端,配一个只读
数据 API 喂真数据——所见即模板,数字变真。

- GET /            → console.html(模板本体,前端每 15s 拉 /api/state 覆盖真数据)
- GET /api/state   → 读现有状态文件/库,返回 JSON;缺的字段返回 null,前端保留
                     模板示例值并打「示例」标记(有真数据的先接,其余占位)。

只读:本服务绝不写任何交易/worker/signer 文件,只读 data/ 下的状态快照与 sqlite。
数据目录用环境变量 LATITUDE_DATA_DIR 指定(默认仓库 data/);部署到 VPS 时指向
/home/ubuntu/polymarket-bot/data。
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

app = FastAPI(title="Latitude Alpha Console", docs_url=None, redoc_url=None)


# ---------- 只读辅助 ----------

def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _age_text(path: Path) -> Optional[str]:
    try:
        secs = time.time() - path.stat().st_mtime
    except OSError:
        return None
    if secs < 90:
        return f"{int(secs)}s 前"
    if secs < 5400:
        return f"{int(secs // 60)}m 前"
    return f"{int(secs // 3600)}h 前"


def _num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------- 各系统真数据(缺失即 None,前端占位) ----------

def _polymarket() -> Dict[str, Any]:
    """PM 账号矩阵聚合:读 engine_state_N.json + .engine_N.pid + .account_N.paused。"""
    accounts: List[dict] = []
    running = 0
    live_orders = 0
    volume_today = 0.0
    pnl_today = 0.0
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
        sibling = state.get("sibling_registry") if isinstance(state.get("sibling_registry"), dict) else {}
        running += 1 if (alive and not paused) else 0
        live_orders += acct_orders
        volume_today += vol
        pnl_today += pnl
        funder = str(state.get("funder") or "")
        accounts.append({
            "idx": idx,
            "funder": (funder[:6] + "…" + funder[-3:]) if len(funder) > 12 else (funder or f"acct{idx}"),
            "status": "已暂停" if paused else ("运行中" if alive else "已停止"),
            "status_cls": "warn" if paused else ("ok" if alive else "danger"),
            "balance": state.get("balance"),
            "orders": acct_orders,
            "volume_today": round(vol, 2),
            "pnl_today": round(pnl, 2),
            "sibling_conflicts": sibling.get("conflicts_detected"),
            "sibling_mode": sibling.get("mode"),
        })
    return {
        "present": bool(accounts),
        "accounts": accounts,
        "running": running,
        "total": len(accounts),
        "live_orders": live_orders,
        "volume_today": round(volume_today, 2),
        "pnl_today": round(pnl_today, 2),
    }


def _single_account() -> Dict[str, Any]:
    state = _read_json(DATA_DIR / "single_account_paper_state.json")
    out: Dict[str, Any] = {"present": state is not None}
    if state:
        summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
        out.update({
            "signals": summary.get("signals"),
            "actionable": summary.get("actionable"),
            "top_symbol": summary.get("top_symbol"),
            "top_strategy": summary.get("top_strategy"),
            "top_score": summary.get("top_score"),
            "age": _age_text(DATA_DIR / "single_account_paper_state.json"),
        })
    # 模拟器权益(施工包01):paper 库存在则取最新快照
    sim_db = DATA_DIR / "single_account_paper.db"
    if sim_db.exists():
        try:
            conn = sqlite3.connect(f"file:{sim_db}?mode=ro", uri=True)
            row = conn.execute("SELECT equity, drawdown FROM equity_snapshots "
                               "ORDER BY ts DESC LIMIT 1").fetchone()
            conn.close()
            if row:
                out["sim_equity"] = row[0]
                out["sim_drawdown"] = row[1]
        except Exception:
            pass
    return out


def _var_decibel() -> Dict[str, Any]:
    """varia 状态:读 ops_state.json(本机)。四源不混算:只读本机 ops,
    peer host 的聚合留待接入 peer 快照目录(占位)。"""
    ops = _read_json(DATA_DIR / "ops_state.json")
    return {
        "present": ops is not None,
        "host_id": (ops or {}).get("host_id"),
        "generated_at": (ops or {}).get("generated_at"),
    }


def _recorders() -> Dict[str, Any]:
    """Research 数据:记录器心跳 + market.db 存在性。"""
    heartbeats = sorted(DATA_DIR.glob(".recorder_*.heartbeat"))
    market_db = DATA_DIR / "single_account_market.db"
    return {
        "present": bool(heartbeats) or market_db.exists(),
        "recorders": [p.stem.replace(".recorder_", "") for p in heartbeats],
        "market_db": market_db.exists(),
        "latest": _age_text(max([market_db, *heartbeats], key=lambda p: p.stat().st_mtime)
                            if (market_db.exists() or heartbeats) else market_db)
        if (market_db.exists() or heartbeats) else None,
    }


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse({
        "ts": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M"),
        "polymarket": _polymarket(),
        "single_account": _single_account(),
        "var_decibel": _var_decibel(),
        "recorders": _recorders(),
    })


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(CONSOLE_HTML.read_text(encoding="utf-8"))


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "data_dir": str(DATA_DIR), "data_dir_exists": DATA_DIR.exists()}
