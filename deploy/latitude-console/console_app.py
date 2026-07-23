"""Latitude Alpha 统一控制台(HTML shell + 状态与受控操作 API)。

- GET /            → console.html(前端每 15s 拉 /api/state 覆盖真数据)
- GET /api/state   → 只读现有状态文件/库;缺失或过期字段返回 null/未知,前端不展示样例值。
- POST /api/varia/control/* → Tailscale 内网受控人工任务；复用原队列和一次性 worker。

四源不混算铁律:每台 VPS 独立读取 Variational 与其指定对冲平台,
当前 VPS1=Var/Decibel、VPS2=Var/Ondo；总计只加真实来源,某源缺失绝不复制顶替。
签名密钥不进入本服务；真实操作仍由原对冲执行环境完成。

环境变量:
  LATITUDE_DATA_DIR  pmbot 数据目录(默认仓库 data/;VPS=/home/ubuntu/polymarket-bot/data)
  VARIA_DATA_DIR     varia 数据目录(默认 /home/ubuntu/varia-decibel-farming-live/data)
"""
from __future__ import annotations

import ast
import hashlib
import io
import json
import os
import re
import shlex
import sqlite3
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import unquote
from xml.etree import ElementTree as ET

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

APP_DIR = Path(__file__).resolve().parent
CONSOLE_HTML = APP_DIR / "console.html"
DATA_DIR = Path(os.getenv("LATITUDE_DATA_DIR", APP_DIR.parents[1] / "data"))
VARIA_DIR = Path(os.getenv("VARIA_DATA_DIR", "/home/ubuntu/varia-decibel-farming-live/data"))
VARIA_CAPITAL_LEDGER = VARIA_DIR / "home_equity_principal.json"
VARIA_RECONCILED_PNL_HISTORY = VARIA_DIR / "reconciled_pnl_history.json"
VARIA_MANUAL_JOB_UNIT = os.getenv(
    "VARIA_MANUAL_JOB_UNIT", "varia-decibel-manual-job.service"
)
VARIA_VPS2_SSH = os.getenv("VARIA_VPS2_SSH", "ubuntu@100.101.50.40")
VARIA_AUTO_WORKER_TEMPLATE = os.getenv(
    "VARIA_AUTO_WORKER_TEMPLATE", "var-decibel-worker@{host}.service"
)
VARIA_VPS2_REPO = os.getenv(
    "VARIA_VPS2_REPO", "/home/ubuntu/varia-decibel-farming-live"
)
VARIA_HEDGE_VENUE_BY_HOST = {"vps1": "decibel", "vps2": "ondo"}
VARIA_STRATEGY_BY_HOST = {"vps1": "A", "vps2": "B"}
VARIA_VENUE_LABELS = {"variational": "Var", "decibel": "Decibel", "ondo": "Ondo"}
VARIA_ONDO_MUTATION_LABELS = (
    ("leverage_sync", "杠杆同步"),
    ("post_only_cancel", "挂单撤单"),
    ("partial_fill_reconcile", "部分成交对账"),
    ("reduce_only_close", "只减仓平仓"),
    ("paired_micro_hedge", "微量双腿"),
)
VARIA_ONDO_MUTATION_CHECKS = tuple(key for key, _ in VARIA_ONDO_MUTATION_LABELS)
VARIA_MARKET_CANDIDATES = tuple(
    item.strip().upper() for item in os.getenv(
        "VARIA_MARKET_CANDIDATES",
        "BTC,ETH,HYPE,XAU,SPCX,SOL,MU,XRP,QQQ,CL,ZEC,XPL,NEAR,TAO,ADA,BNB,"
        "NVDA,TSLA,SNDK,SUI,FARTCOIN,COPPER,NATGAS,EWY,AAVE,DOGE,APT,LINK,MEGA,"
        "TRUMP,WLFI,CBRS,ZRO,CHIP",
    ).split(",") if item.strip()
)
VARIA_RWA_SYMBOLS = frozenset(
    item.strip().upper() for item in os.getenv(
        "VARIA_RWA_SYMBOLS",
        "XAU,SPCX,MU,QQQ,CL,NVDA,TSLA,SNDK,COPPER,NATGAS,EWY,CHIP",
    ).split(",") if item.strip()
)
# 跨机只读数据源(tailnet 内):打新核算台已构建 JSON、router ipo 状态、mac-mini 状态导出器
ACCOUNT_OPS_URL = os.getenv("ACCOUNT_OPS_URL", "http://100.82.86.62:8081/data/dashboard.json")
ACCOUNT_OPS_SNAPSHOT_PATH = Path(
    os.getenv("ACCOUNT_OPS_SNAPSHOT_PATH", DATA_DIR / "account_ops_last_good.json")
)
IPO_STATE_URL = os.getenv("IPO_STATE_URL", "http://100.82.86.62:8080/dashboard/ipo/state")
IPO_PACK_URL = os.getenv(
    "IPO_PACK_URL",
    "http://100.82.86.62:8080/dashboard/ipo/judgment-pack",
)
MACMINI_STATUS_URL = os.getenv("MACMINI_STATUS_URL", "http://100.91.159.54:8620/status")
GRID_CONSOLE_URL = os.getenv("GRID_CONSOLE_URL", "http://127.0.0.1:8610/api/state")  # varxyz-grid 独立控制台(本机)
DEWU_INVENTORY_PATH = Path(
    os.getenv("DEWU_INVENTORY_PATH", DATA_DIR / "dewu_inventory.json")
)
DEWU_EXECUTOR_URL = os.getenv(
    "DEWU_EXECUTOR_URL", "http://100.91.159.54:8621"
).rstrip("/")

STALE_SEC = 600  # 状态文件超过 10 分钟视为过期(展示但标注)
PM_STATE_STALE_SEC = int(os.getenv("PM_STATE_STALE_SEC", "300"))

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


# ---------- 得物库存(Excel 为唯一入库来源) ----------

def _dewu_empty() -> dict:
    return {"version": 1, "items": [], "imports": [], "exceptions": [], "updated_at": None}


def _dewu_load() -> dict:
    state = _read_json(DEWU_INVENTORY_PATH)
    if not isinstance(state, dict):
        return _dewu_empty()
    for key in ("items", "imports", "exceptions"):
        if not isinstance(state.get(key), list):
            state[key] = []
    return state


def _dewu_save(state: dict) -> None:
    DEWU_INVENTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = DEWU_INVENTORY_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, DEWU_INVENTORY_PATH)


def _dewu_executor(path: str, payload: Optional[dict] = None) -> tuple[dict, int]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{DEWU_EXECUTOR_URL}{path}",
        data=body,
        headers={"Content-Type": "application/json"},
        method="GET" if payload is None else "POST",
    )
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        with opener.open(request, timeout=25) as response:
            return json.loads(response.read()), response.status
    except urllib.error.HTTPError as exc:
        try:
            return json.loads(exc.read()), exc.code
        except Exception:
            return {"ok": False, "error": f"执行器返回 HTTP {exc.code}"}, exc.code
    except Exception as exc:
        return {"ok": False, "error": f"Mac mini 执行器不可达：{exc}"}, 502


def _xlsx_rows(blob: bytes) -> List[List[str]]:
    """用标准库读取首个工作表，避免为控制台引入额外运行依赖。"""
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rel_ns = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    pkg_rel_ns = "http://schemas.openxmlformats.org/package/2006/relationships"
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        shared: List[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall(f"{{{ns}}}si"):
                shared.append("".join(t.text or "" for t in si.iter(f"{{{ns}}}t")))
        workbook = ET.fromstring(zf.read("xl/workbook.xml"))
        sheet = workbook.find(f".//{{{ns}}}sheet")
        if sheet is None:
            return []
        rel_id = sheet.attrib.get(f"{{{rel_ns}}}id")
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = None
        for rel in rels.findall(f"{{{pkg_rel_ns}}}Relationship"):
            if rel.attrib.get("Id") == rel_id:
                target = rel.attrib.get("Target")
                break
        if not target:
            return []
        path = target.lstrip("/")
        if not path.startswith("xl/"):
            path = "xl/" + path
        root = ET.fromstring(zf.read(path))
        rows: List[List[str]] = []
        for row in root.findall(f".//{{{ns}}}row"):
            values: Dict[int, str] = {}
            for cell in row.findall(f"{{{ns}}}c"):
                ref = cell.attrib.get("r", "A1")
                letters = re.match(r"[A-Z]+", ref)
                if not letters:
                    continue
                col = 0
                for char in letters.group(0):
                    col = col * 26 + ord(char) - 64
                typ = cell.attrib.get("t")
                value = ""
                if typ == "inlineStr":
                    value = "".join(t.text or "" for t in cell.iter(f"{{{ns}}}t"))
                else:
                    node = cell.find(f"{{{ns}}}v")
                    raw = node.text if node is not None and node.text is not None else ""
                    if typ == "s" and raw:
                        value = shared[int(raw)]
                    else:
                        value = raw
                values[col - 1] = str(value).strip()
            if values:
                width = max(values) + 1
                rows.append([values.get(i, "") for i in range(width)])
        return rows


def _dewu_parse_xlsx(blob: bytes) -> tuple[List[dict], List[dict]]:
    rows = _xlsx_rows(blob)
    header_idx = -1
    columns: Dict[str, int] = {}
    aliases = {
        "sku": ("sku", "货号"),
        "name": ("名称", "商品名称", "品名"),
        "color": ("颜色", "配色"),
        "size": ("尺码", "尺码（eur/服装）", "eur尺码", "size"),
        "quantity": ("数量", "库存", "qty"),
    }
    for idx, row in enumerate(rows):
        normalized = [re.sub(r"\s+", "", str(v)).lower() for v in row]
        found: Dict[str, int] = {}
        for key, names in aliases.items():
            for col, value in enumerate(normalized):
                if any(value == re.sub(r"\s+", "", name).lower() or
                       (key == "size" and "尺码" in value) for name in names):
                    found[key] = col
                    break
        if all(key in found for key in ("sku", "name", "size", "quantity")):
            header_idx, columns = idx, found
            break
    if header_idx < 0:
        raise ValueError("找不到表头：需要 SKU、名称、尺码（EUR/服装）、数量")
    merged: Dict[str, dict] = {}
    errors: List[dict] = []
    for excel_row, row in enumerate(rows[header_idx + 1:], start=header_idx + 2):
        get = lambda key: row[columns[key]].strip() if columns.get(key, -1) < len(row) else ""
        sku, name, size = get("sku").upper(), get("name"), get("size")
        color = get("color") if "color" in columns else ""
        raw_qty = get("quantity")
        if not any((sku, name, size, raw_qty)):
            continue
        if sku and not any((name, size, raw_qty)):
            continue  # 表尾“整理说明”等单格文字
        if not sku or not size:
            errors.append({"row": excel_row, "sku": sku, "reason": "缺少 SKU 或尺码"})
            continue
        try:
            quantity = int(float(raw_qty))
            if quantity <= 0:
                raise ValueError
        except (TypeError, ValueError):
            errors.append({"row": excel_row, "sku": sku, "reason": "数量必须是正整数"})
            continue
        normalized_size = re.sub(r"\s+", " ", size).upper()
        key = f"{sku}|{normalized_size}"
        item = merged.setdefault(key, {
            "key": key, "sku": sku, "name": name, "color": color, "size": size,
            "quantity": 0, "status": "待处理", "lowest_price": None,
            "listing_price": None, "updated_at": None,
        })
        item["quantity"] += quantity
        if name:
            item["name"] = name
        if color:
            item["color"] = color
    return list(merged.values()), errors


@app.get("/api/dewu/inventory")
def dewu_inventory_get() -> JSONResponse:
    state = _dewu_load()
    total = sum(int(item.get("quantity") or 0) for item in state["items"])
    return JSONResponse({**state, "total_quantity": total, "spec_count": len(state["items"])})


@app.get("/api/dewu/executor")
def dewu_executor_get() -> JSONResponse:
    result, code = _dewu_executor("/status")
    return JSONResponse(result, status_code=code)


@app.post("/api/dewu/listings/start")
async def dewu_listings_start(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    if payload.get("confirmation") != "START_REAL_LISTING":
        return JSONResponse({"ok": False, "error": "请确认真实上架操作"}, status_code=400)
    state = _dewu_load()
    items = [
        {
            "sku": item.get("sku"),
            "name": item.get("name"),
            "size": item.get("size"),
            "quantity": item.get("quantity"),
        }
        for item in state["items"]
        if int(item.get("quantity") or 0) > 0
        and str(item.get("status") or "待处理") not in {"已上架", "已售出"}
    ]
    result, code = _dewu_executor(
        "/jobs/listings",
        {
            "confirmation": "START_REAL_LISTING",
            "items": items,
        },
    )
    return JSONResponse(result, status_code=code)


@app.post("/api/dewu/inventory/import")
async def dewu_inventory_import(request: Request) -> JSONResponse:
    blob = await request.body()
    if not blob or len(blob) > 15 * 1024 * 1024:
        return JSONResponse({"ok": False, "error": "文件为空或超过 15 MB"}, status_code=400)
    digest = hashlib.sha256(blob).hexdigest()
    state = _dewu_load()
    if any(row.get("sha256") == digest for row in state["imports"]):
        return JSONResponse({"ok": False, "duplicate": True,
                             "error": "这个文件已经导入过，库存未重复累加"}, status_code=409)
    try:
        incoming, errors = _dewu_parse_xlsx(blob)
    except (ValueError, zipfile.BadZipFile, KeyError, ET.ParseError) as exc:
        return JSONResponse({"ok": False, "error": f"Excel 解析失败：{exc}"}, status_code=400)
    now = datetime.now(timezone.utc).isoformat()
    by_key = {str(item.get("key")): item for item in state["items"]}
    added_specs = added_quantity = 0
    for item in incoming:
        existing = by_key.get(item["key"])
        if existing:
            existing["quantity"] = int(existing.get("quantity") or 0) + item["quantity"]
            existing["name"] = item["name"] or existing.get("name") or ""
            existing["color"] = item["color"] or existing.get("color") or ""
            existing["updated_at"] = now
        else:
            item["updated_at"] = now
            state["items"].append(item)
            by_key[item["key"]] = item
            added_specs += 1
        added_quantity += item["quantity"]
    filename = unquote(request.headers.get("x-filename", "库存.xlsx"))[:180]
    record = {"id": digest[:12], "sha256": digest, "filename": filename, "at": now,
              "specs": len(incoming), "quantity": added_quantity, "errors": len(errors)}
    state["imports"].insert(0, record)
    state["imports"] = state["imports"][:50]
    state["exceptions"] = [{"import_id": record["id"], **row} for row in errors] + state["exceptions"]
    state["exceptions"] = state["exceptions"][:200]
    state["updated_at"] = now
    _dewu_save(state)
    return JSONResponse({"ok": True, "import": record, "added_specs": added_specs,
                         "merged_specs": len(incoming) - added_specs, "errors": errors,
                         "total_specs": len(state["items"]),
                         "total_quantity": sum(int(x.get("quantity") or 0)
                                               for x in state["items"])})


# ---------- Polymarket(engine_state_N 逐账号,不混算) ----------

def _pm_pnl() -> Dict[str, Any]:
    """PM 收益核算(诚实粒度=账户-日)。奖励:rewards_cumulative.json 的 daily(持久,
    Polymarket /rewards/user/total 只到账户-日,无法按市场拆);损耗:各账户当前会话
    fills 的实现 pnl(引擎重启即清,标"本次运行")。净=近7日奖励 − 本次运行损耗。
    远程账号读 peer 目录。"""
    rc = _read_json(DATA_DIR / "rewards_cumulative.json")
    accts_rc = rc.get("accounts") if isinstance(rc, dict) and isinstance(rc.get("accounts"), dict) else {}
    # rewards_cumulative 按账户序号存(dict 键 "0"/"1"…),按序号 join(funder 地址会随迁移变)
    by_idx: Dict[int, dict] = {}
    for k, a in accts_rc.items():
        if isinstance(a, dict):
            try:
                by_idx[int(k)] = a
            except (TypeError, ValueError):
                pass
    remotes = _load_pm_remotes()
    rows: List[dict] = []
    total_reward_7d = total_loss_session = 0.0
    any_reward = False
    for idx in _pm_all_accounts():
        is_remote = idx in remotes
        base = PM_PEER_DIR if is_remote else DATA_DIR
        st = _read_json(base / f"engine_state_{idx}.json") or {}
        a = by_idx.get(idx, {})
        daily = a.get("daily") if isinstance(a.get("daily"), dict) else {}
        days = sorted(daily.keys())[-7:]
        series = [{"d": d, "v": round(_num(daily.get(d)) or 0, 2)} for d in days]
        reward_7d = round(sum(x["v"] for x in series), 2)
        # 本次运行损耗:fills 的负 pnl 求和(绝对值),及费用(若引擎写了)
        fills = st.get("fills") if isinstance(st.get("fills"), list) else []
        realized = sum(_num(f.get("pnl")) or 0 for f in fills if f.get("pnl") is not None)
        loss_session = round(-realized, 2) if realized < 0 else 0.0
        if series:
            any_reward = True
        total_reward_7d += reward_7d
        total_loss_session += loss_session
        rows.append({
            "idx": idx, "host": (remotes[idx].get("label") or "远程") if is_remote else "VPS1",
            "cumulative": _num(a.get("cumulative_usd")), "last_date": a.get("last_snapshot_date"),
            "recent": series, "reward_7d": reward_7d,
            "realized_session": round(realized, 2), "fills_session": len(fills),
            "net_est": round(reward_7d - loss_session, 2),
        })
    return {
        "present": any_reward or bool(rows),
        "has_reward_data": any_reward,
        "accounts": rows,
        "total_reward_7d": round(total_reward_7d, 2),
        "total_loss_session": round(total_loss_session, 2),
        "total_net_est": round(total_reward_7d - total_loss_session, 2),
        "note": "奖励=账户-日粒度(Polymarket 不提供按市场拆分);损耗=本次运行 fills 实现盈亏",
    }


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
    orders_unknown = False
    fresh_states = 0
    volume_today = pnl_today = 0.0
    quotes_sent = fills_seen = 0
    cooldown = False
    pm_fill_events: List[dict] = []
    remotes = _load_pm_remotes()
    for idx in range(1, 31):
        is_remote = idx in remotes
        base = PM_PEER_DIR if is_remote else DATA_DIR
        state_path = base / f"engine_state_{idx}.json"
        state = _read_json(state_path)
        if state is None and idx == 1 and not is_remote:
            fallback = DATA_DIR / "engine_state.json"
            state = _read_json(fallback)
            if state is not None:
                state_path = fallback
        if state is None:
            if is_remote:
                state = {}          # 远程账号已配置但还没跑出状态:仍显示一行
            else:
                continue
        if is_remote:
            alive = _REMOTE_STATUS.get(idx, False)
            paused = (PM_PEER_DIR / f".account_{idx}.paused").exists()
        else:
            alive = _pid_file_alive(DATA_DIR / f".engine_{idx}.pid",
                                    DATA_DIR / ".engine.pid")
            paused = (DATA_DIR / f".account_{idx}.paused").exists()
        state_age = _mtime_age(state_path)
        state_fresh = state_age is not None and state_age <= PM_STATE_STALE_SEC
        fresh_states += 1 if state_fresh else 0
        # 停止的引擎无法证明交易所当前仍有/已无挂单。只有运行中且状态新鲜时，
        # engine_state 的挂单数才作为“当前活跃挂单”展示。
        orders_verified = bool(alive and state_fresh)
        markets = state.get("markets") if isinstance(state.get("markets"), dict) else {}
        last_seen_orders = 0
        for m in markets.values():
            if isinstance(m, dict):
                orders = m.get("live_orders") or m.get("orders")
                last_seen_orders += len(orders) if isinstance(orders, list) else int(_num(m.get("live_order_count")) or 0)
        acct_orders = last_seen_orders if orders_verified else None
        fills = state.get("fills") if isinstance(state.get("fills"), list) else []
        cutoff = time.time() - 86400
        ft = ([f for f in fills if isinstance(f, dict) and (_num(f.get("ts")) or 0) >= cutoff]
              if state_fresh else [])
        vol = sum(abs((_num(f.get("price")) or 0) * (_num(f.get("size")) or 0)) for f in ft)
        pnl = sum(_num(f.get("pnl")) or 0 for f in ft if f.get("pnl") is not None)
        for f in ft[-3:]:
            ts = _num(f.get("ts")) or 0
            pm_fill_events.append({
                "t": datetime.fromtimestamp(ts).strftime("%H:%M") if ts else "",
                "epoch": ts, "sev": "info",
                "msg": f"[PM·#{idx}] 成交 {f.get('side','')} {f.get('size','')} @{f.get('price','')}",
            })
        sibling = (state.get("sibling_registry")
                   if state_fresh and isinstance(state.get("sibling_registry"), dict) else {})
        running += 1 if (alive and not paused) else 0
        if acct_orders is None:
            orders_unknown = True
        else:
            live_orders += acct_orders
        volume_today += vol
        pnl_today += pnl
        if state_fresh:
            quotes_sent += int(_num(state.get("quotes_sent")) or 0)
            fills_seen += int(_num(state.get("fills_seen")) or 0)
            cooldown = cooldown or bool(state.get("cooldown_active"))
        funder = str(state.get("funder") or "")
        if paused:
            status, status_cls = "已暂停", "warn"
        elif alive and not state_fresh:
            status, status_cls = "运行中·状态迟滞", "danger"
        elif alive:
            status, status_cls = "运行中", "ok"
        else:
            status, status_cls = "已停止", "danger"
        accounts.append({
            "idx": idx,
            "paused": paused,
            "host": (remotes[idx].get("label") or "远程") if is_remote else "VPS1",
            "funder": (funder[:6] + "…" + funder[-3:]) if len(funder) > 12 else (funder or f"acct{idx}"),
            "rewards": rewards_by_addr.get(funder.lower()),
            "status": status,
            "status_cls": status_cls,
            "balance": _num(state.get("balance")) if state_fresh else None,
            "balance_last_seen": _num(state.get("balance")),
            "orders": acct_orders,
            "orders_last_seen": last_seen_orders,
            "orders_verified": orders_verified,
            "fills_today": len(ft) if state_fresh else None,
            "volume_today": round(vol, 2) if state_fresh else None,
            "pnl_today": round(pnl, 2) if state_fresh else None,
            "sibling_conflicts": sibling.get("conflicts_detected"),
            "sibling_mode": sibling.get("mode"),
            "state_stale": not state_fresh,
            "age_sec": state_age,
            "age": _age_text(state_age),
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
        "running": running, "total": len(accounts),
        "live_orders": None if orders_unknown else live_orders,
        "orders_unknown": orders_unknown,
        "fresh_state_count": fresh_states,
        "volume_today": round(volume_today, 2) if fresh_states else None,
        "pnl_today": round(pnl_today, 2) if fresh_states else None,
        "quotes_sent": quotes_sent if fresh_states else None,
        "fills_seen": fills_seen if fresh_states else None,
        "cooldown": cooldown if fresh_states else None,
        "rewards_total": rewards_total,
        "fill_events": pm_fill_events[-6:],
    }


def _pid_file_alive(*paths: Path) -> bool:
    """PID 文件只是提示；只有对应进程仍存在才算运行中。"""
    for path in paths:
        try:
            pid = int(path.read_text(encoding="utf-8").strip())
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except (FileNotFoundError, ProcessLookupError, ValueError, OSError):
            continue
    return False


# ---------- varia trades:今日量 / 损耗分解(本地 sqlite + peer,(host,id) 去重) ----------

def _parse_ts(value: Any) -> Optional[float]:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        # SQLite timestamps written by the Varia recorder are UTC but do not
        # carry an offset. Treating them as server-local time makes fresh
        # quotes look eight hours old on hosts configured for China time.
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except Exception:
        return None


def _farming_week_cutoff_epoch(now: Optional[datetime] = None) -> float:
    china_tz = timezone(timedelta(hours=8))
    current = (now or datetime.now(timezone.utc)).astimezone(china_tz)
    days_back = (current.weekday() - 4) % 7  # Friday
    cutoff = current.replace(hour=8, minute=0, second=0, microsecond=0) - timedelta(days=days_back)
    if cutoff > current:
        cutoff -= timedelta(days=7)
    return cutoff.timestamp()


def _varia_trades_today() -> Dict[str, Any]:
    rows: List[dict] = []
    db = VARIA_DIR / "hedge_bot.sqlite3"
    try:
        conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        cur = conn.execute(
            "SELECT id, host, status, timestamp_close, target_notional, funding_var, "
            "funding_decibel, realized_cost_bp, estimated_cost_bp, var_slippage_bp, "
            "decibel_slippage_bp, realized_pnl_usdc FROM trades "
            "WHERE timestamp_close >= datetime('now','-8 day')")
        names = [d[0] for d in cur.description]
        rows += [dict(zip(names, r)) for r in cur.fetchall()]
        conn.close()
    except Exception:
        pass
    peer_dir = VARIA_DIR / "peer_trades"
    cutoff_week = _farming_week_cutoff_epoch()
    for path in (sorted(peer_dir.glob("*.json")) if peer_dir.exists() else []):
        raw = _read_json(path)
        if not isinstance(raw, list):
            continue
        for r in raw:
            if not isinstance(r, dict):
                continue
            ts = _parse_ts(r.get("timestamp_close") or r.get("timestamp_open"))
            if ts is None or ts < cutoff_week:
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
    net_week_by_host: Dict[str, float] = {}
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
        ts = _parse_ts(r.get("timestamp_close") or r.get("timestamp_open"))
        if ts is not None and ts >= cutoff_week:
            loss_7d += row_loss
            loss_7d_by_host[host] = loss_7d_by_host.get(host, 0.0) + row_loss
            net_week_by_host[host] = net_week_by_host.get(host, 0.0) + (
                r_pnl if r_pnl is not None else -row_loss
            )
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
        "net_week_by_host": {h: round(v, 4) for h, v in net_week_by_host.items()},
        "week_cutoff": datetime.fromtimestamp(cutoff_week, tz=timezone.utc).isoformat(),
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


# ---------- Var hedge (each host owns an independent venue pair) ----------

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


def _venue_read_error(payload: dict) -> str:
    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message") or error.get("detail") or error.get("type")
    else:
        message = error
    text = str(message or "状态未返回").strip()
    return text[:180]


def _host_hedge_venue(host: str) -> str:
    return VARIA_HEDGE_VENUE_BY_HOST.get(str(host).lower(), "decibel")


def _venue_label(venue: str) -> str:
    return VARIA_VENUE_LABELS.get(venue, venue.title())


def _incentive_accounting(hosts: Dict[str, dict]) -> Dict[str, Any]:
    """Aggregate settled incentives without adding them to equity a second time."""
    ondo_total = ondo_week = ondo_24h = 0.0
    var_total = var_week = var_24h = 0.0
    var_own = var_other = var_estimate = 0.0
    current_pool = None
    ondo_pool = None
    complete = bool(hosts)
    host_rows: Dict[str, dict] = {}
    for host, host_state in sorted(hosts.items()):
        incentives = host_state.get("incentives") if isinstance(host_state.get("incentives"), dict) else {}
        var_row = incentives.get("variational") if isinstance(incentives.get("variational"), dict) else None
        ondo_row = incentives.get("ondo") if isinstance(incentives.get("ondo"), dict) else None
        row: Dict[str, Any] = {}
        if var_row is None:
            complete = False
            row["variational"] = {"available": False}
        else:
            observed = {
                "available": True,
                "settled_total_usdc": round(_num(var_row.get("settled_total_usdc")) or 0.0, 6),
                "settled_week_usdc": round(_num(var_row.get("settled_week_usdc")) or 0.0, 6),
                "own_refunds_total_usdc": round(_num(var_row.get("own_refunds_total_usdc")) or 0.0, 6),
                "other_rewards_total_usdc": round(_num(var_row.get("other_rewards_total_usdc")) or 0.0, 6),
                "estimated_refund_usdc": round(_num(var_row.get("estimated_refund_usdc")) or 0.0, 6),
                "estimated_hit_probability_pct": round(_num(var_row.get("estimated_hit_probability_pct")) or 0.0, 4),
                "pool_usdc": round(_num(var_row.get("pool_usdc")) or 0.0, 2),
                "pool_eligible": var_row.get("pool_eligible") is True,
                "tier": var_row.get("tier"),
                "odds_pct": _num(var_row.get("odds_pct")),
                "estimate_status": var_row.get("estimate_status"),
                "eligible_loss_count": int(_num(var_row.get("eligible_loss_count")) or 0),
                "coverage_start": var_row.get("coverage_start"),
                "complete": var_row.get("complete") is True,
            }
            row["variational"] = observed
            var_total += observed["settled_total_usdc"]
            var_week += observed["settled_week_usdc"]
            var_24h += _num(var_row.get("settled_24h_usdc")) or 0.0
            var_own += observed["own_refunds_total_usdc"]
            var_other += observed["other_rewards_total_usdc"]
            var_estimate += observed["estimated_refund_usdc"]
            current_pool = max(current_pool or 0.0, observed["pool_usdc"])
            if observed["complete"] is not True:
                complete = False
        if host_state.get("hedge_venue") == "ondo":
            if ondo_row is None:
                complete = False
                row["ondo"] = {"available": False}
            else:
                observed = {
                    "available": True,
                    "settled_total_usdc": round(_num(ondo_row.get("settled_total_usdc")) or 0.0, 6),
                    "settled_week_usdc": round(_num(ondo_row.get("settled_week_usdc")) or 0.0, 6),
                    "current_week": ondo_row.get("current_week"),
                    "current_week_pool_usdc": _num(ondo_row.get("current_week_pool_usdc")),
                    "current_week_status": ondo_row.get("current_week_status"),
                    "pending_account_reward_usdc": _num(ondo_row.get("pending_account_reward_usdc")),
                    "complete": ondo_row.get("complete") is True,
                }
                row["ondo"] = observed
                ondo_total += observed["settled_total_usdc"]
                ondo_week += observed["settled_week_usdc"]
                ondo_24h += _num(ondo_row.get("settled_24h_usdc")) or 0.0
                if observed["current_week_pool_usdc"] is not None:
                    ondo_pool = max(ondo_pool or 0.0, observed["current_week_pool_usdc"])
                if observed["complete"] is not True:
                    complete = False
        host_rows[host] = row
    settled_total = ondo_total + var_total
    settled_week = ondo_week + var_week
    return {
        "present": bool(hosts),
        "complete": complete,
        "settled_total_usdc": round(settled_total, 6),
        "settled_week_usdc": round(settled_week, 6),
        "settled_24h_usdc": round(ondo_24h + var_24h, 6),
        "ondo": {
            "settled_total_usdc": round(ondo_total, 6),
            "settled_week_usdc": round(ondo_week, 6),
            "current_week_pool_usdc": round(ondo_pool, 2) if ondo_pool is not None else None,
        },
        "variational": {
            "settled_total_usdc": round(var_total, 6),
            "settled_week_usdc": round(var_week, 6),
            "own_refunds_total_usdc": round(var_own, 6),
            "other_rewards_total_usdc": round(var_other, 6),
            "estimated_refund_usdc": round(var_estimate, 6),
            "pool_usdc": round(current_pool, 2) if current_pool is not None else None,
        },
        "hosts": host_rows,
    }


def _pnl_attribution(capital: Dict[str, Any], incentives: Dict[str, Any]) -> Dict[str, Any]:
    net_pnl = _num(capital.get("pnl")) if capital.get("complete") else None
    settled = _num(incentives.get("settled_total_usdc"))
    complete = net_pnl is not None and incentives.get("complete") is True and settled is not None
    return {
        "complete": complete,
        "net_pnl_usdc": round(net_pnl, 2) if net_pnl is not None else None,
        "settled_incentives_usdc": round(settled, 2) if settled is not None else None,
        "trading_funding_fees_usdc": round(net_pnl - settled, 2) if complete else None,
        "note": "Settled incentives are already inside equity and are not added twice.",
    }


def _var_decibel() -> Dict[str, Any]:
    peer_dir = VARIA_DIR / "ops_peer_state"
    hosts: Dict[str, dict] = {}
    # peer 目录打底,本机 ops_state.json 覆盖自己。每台主机只计算
    # Variational + 该主机指定的 hedge venue,不把 VPS2 的旧 Decibel 数据混入。
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
    volume_weekly_hosts: set[str] = set()
    volume_total_hosts: set[str] = set()
    single_leg: List[str] = []
    pairs: List[dict] = []
    verified_hosts: List[str] = []
    unverified_hosts: List[dict] = []
    for state in sources:
        if not isinstance(state, dict):
            continue
        host = str(state.get("host_id") or "").lower() or "unknown"
        if host.startswith("vm-"):
            host = "vps1"
        hedge_venue = _host_hedge_venue(host)
        hedge_label = _venue_label(hedge_venue)
        age = _iso_age(state.get("generated_at"))
        exchanges = state.get("exchanges") if isinstance(state.get("exchanges"), dict) else {}
        h: Dict[str, Any] = {"age_sec": age, "age": _age_text(age),
                             "stale": (age is None or age > STALE_SEC),
                             "hedge_venue": hedge_venue, "hedge_label": hedge_label,
                             "points_dec_available": False, "points_decibel": None,
                             "points_var_available": False, "points_variational": None}
        venue_symbols: Dict[str, dict] = {}
        venue_reads: Dict[str, dict] = {}
        for venue in ("variational", hedge_venue):
            payload = exchanges.get(venue) if isinstance(exchanges.get(venue), dict) else {}
            bal = payload.get("balance") if isinstance(payload.get("balance"), dict) else {}
            eq = _num(bal.get("total_equity"))
            venue_ok = payload.get("ok") is True
            venue_reads[venue] = {
                "ok": venue_ok,
                "error": None if venue_ok else _venue_read_error(payload),
            }
            trusted_eq = eq if (venue_ok and not h["stale"]) else None
            alias = {"variational": "var", "decibel": "dec", "ondo": "ondo"}.get(venue, venue)
            h[f"equity_{alias}"] = trusted_eq
            h[f"equity_{alias}_last_seen"] = eq
            h[f"ok_{alias}"] = payload.get("ok")
            if trusted_eq is not None:
                equity_total += eq
                equity_found = True
            venue_symbols[venue] = payload.get("symbols") if isinstance(payload.get("symbols"), dict) else {}
            if venue == "decibel":
                pts = payload.get("points") if isinstance(payload.get("points"), dict) else {}
                rows = pts.get("breakdown") if isinstance(pts.get("breakdown"), list) else []
                vals = [_num(r.get("points")) for r in rows if isinstance(r, dict)]
                vals = [v for v in vals if v is not None]
                total = _num(pts.get("total_points"))
                if total is None and vals:
                    total = sum(vals)
                h["points_dec_available"] = bool(
                    total is not None and venue_ok and not h["stale"]
                )
                h["points_decibel"] = total if h["points_dec_available"] else None
                if h["points_dec_available"]:
                    points_dec = (points_dec or 0.0) + total
            elif venue == "variational":
                pts = payload.get("points") if isinstance(payload.get("points"), dict) else {}
                tp = _num(pts.get("total_points"))
                h["points_var_available"] = bool(
                    tp is not None and venue_ok and not h["stale"]
                )
                h["points_variational"] = tp if h["points_var_available"] else None
                if h["points_var_available"]:
                    points_var = (points_var or 0.0) + tp
        var_payload = exchanges.get("variational") if isinstance(exchanges.get("variational"), dict) else {}
        hedge_payload = exchanges.get(hedge_venue) if isinstance(exchanges.get(hedge_venue), dict) else {}
        h["incentives"] = {}
        if not h["stale"] and var_payload.get("ok") is True and isinstance(var_payload.get("loss_refunds"), dict):
            h["incentives"]["variational"] = var_payload["loss_refunds"]
        if (
            hedge_venue == "ondo"
            and not h["stale"]
            and hedge_payload.get("ok") is True
            and isinstance(hedge_payload.get("rewards"), dict)
        ):
            h["incentives"]["ondo"] = hedge_payload["rewards"]
        hedge_alias = {"decibel": "dec", "ondo": "ondo"}.get(hedge_venue, hedge_venue)
        h["equity_hedge"] = h.get(f"equity_{hedge_alias}")
        h["equity_hedge_last_seen"] = h.get(f"equity_{hedge_alias}_last_seen")
        h["ok_hedge"] = h.get(f"ok_{hedge_alias}")
        if hedge_venue == "ondo":
            acceptance = state.get("ondo_acceptance")
            h["ondo_acceptance"] = acceptance if isinstance(acceptance, dict) else {"present": False}
        h["venue_reads"] = venue_reads
        var_syms = venue_symbols.get("variational", {})
        hedge_syms = venue_symbols.get(hedge_venue, {})
        # 只有快照新鲜且两家指定交易所读取都明确成功，才能判断空仓/对冲/单腿。
        # 任何一源过期或失败都标“仓位未知”，不拿上次快照冒充当前状态。
        symbols = sorted(set(hedge_syms) | set(var_syms))
        positions_verified = (not h["stale"] and h.get("ok_hedge") is True
                              and h.get("ok_var") is True)
        h["positions_verified"] = positions_verified
        if positions_verified:
            verified_hosts.append(host)
        else:
            last_seen = [symbol for symbol in symbols
                         if _pos_open(hedge_syms.get(symbol)) or _pos_open(var_syms.get(symbol))]
            reason = "快照过期" if h["stale"] else "交易所读取不完整"
            failed_venues = [
                {
                    "venue": venue,
                    "label": _venue_label(venue),
                    "error": read.get("error") or "状态未返回",
                }
                for venue, read in venue_reads.items()
                if read.get("ok") is not True
            ]
            summary = reason if h["stale"] else (
                "、".join(row["label"] for row in failed_venues) + " 读取失败"
                if failed_venues else reason
            )
            unverified_hosts.append({
                "host": host, "age": h.get("age"), "reason": reason,
                "summary": summary, "failed_venues": failed_venues,
                "last_seen_symbols": last_seen,
            })
        # 单腿检测 + 配对腿行(口径同 varia _host_exposure_status 的核心判断)
        for symbol in (symbols if positions_verified else []):
            d_pos = (hedge_syms.get(symbol) or {}).get("position") if isinstance(hedge_syms.get(symbol), dict) else {}
            v_pos = (var_syms.get(symbol) or {}).get("position") if isinstance(var_syms.get(symbol), dict) else {}
            d_pos = d_pos if isinstance(d_pos, dict) else {}
            v_pos = v_pos if isinstance(v_pos, dict) else {}
            d_open = _pos_open(hedge_syms.get(symbol))
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
                "host": host, "symbol": symbol, "var": var_leg,
                "hedge": dec_leg, "dec": dec_leg,
                "hedge_venue": hedge_venue, "hedge_label": hedge_label,
                "net": round(var_leg["signed"] + dec_leg["signed"], 2),
                "status": ("HEDGED" if (d_open and v_open) else
                           (f"{hedge_label.upper()} 裸腿" if d_open else "VAR 裸腿")),
            })
        if positions_verified:
            tv = state.get("trade_volume") if isinstance(state.get("trade_volume"), dict) else {}
            host_weekly_complete = tv.get("ok") is True
            host_total_complete = tv.get("ok") is True
            venue_rows = tv.get("venues") if isinstance(tv.get("venues"), dict) else {}
            for venue in ("variational", hedge_venue):
                venue_data = venue_rows.get(venue)
                if isinstance(venue_data, dict):
                    w = _num(venue_data.get("weekly_notional_usdc"))
                    t = _num(venue_data.get("total_notional_usdc"))
                    if w is not None:
                        vol_weekly += w
                        vol_found = True
                    else:
                        host_weekly_complete = False
                    if t is not None:
                        vol_total += t
                    else:
                        host_total_complete = False
                else:
                    host_weekly_complete = host_total_complete = False
            if host_weekly_complete:
                volume_weekly_hosts.add(host)
            if host_total_complete:
                volume_total_hosts.add(host)
        hosts[host] = h
    auto = _read_json(VARIA_DIR / "auto_strategy_state.json")
    auto_ctl = None
    if isinstance(auto, dict):
        auto_ctl = {"enabled": bool(auto.get("enabled")), "mode": auto.get("mode"),
                    "hosts": auto.get("hosts") if isinstance(auto.get("hosts"), dict) else {}}
    equity_complete = bool(hosts) and all(
        h.get("positions_verified") is True
        and h.get("equity_var") is not None
        and h.get("equity_hedge") is not None
        for h in hosts.values()
    )
    decibel_hosts = {
        host: data for host, data in hosts.items()
        if data.get("hedge_venue") == "decibel"
    }
    points_dec_complete = bool(decibel_hosts) and all(
        h.get("points_dec_available") is True for h in decibel_hosts.values()
    )
    points_var_complete = bool(hosts) and all(
        h.get("points_var_available") is True for h in hosts.values()
    )
    volume_weekly_complete = bool(hosts) and len(volume_weekly_hosts) == len(hosts)
    volume_total_complete = bool(hosts) and len(volume_total_hosts) == len(hosts)
    capital = _capital_accounting(hosts)
    incentives = _incentive_accounting(hosts)
    pnl_attribution = _pnl_attribution(capital, incentives)
    equity_history = _reconciled_pnl_history(capital)
    points_by_venue = {
        "decibel": {
            "total": round(points_dec, 4) if points_dec_complete and points_dec is not None else None,
            "hosts": {
                host: round(_num(data.get("points_decibel")), 4)
                if data.get("points_decibel") is not None else None
                for host, data in sorted(decibel_hosts.items())
            },
            "complete": points_dec_complete,
        },
        "variational": {
            "total": round(points_var, 4) if points_var_complete and points_var is not None else None,
            "hosts": {
                host: round(_num(data.get("points_variational")), 4)
                if data.get("points_variational") is not None else None
                for host, data in sorted(hosts.items())
            },
            "complete": points_var_complete,
        },
    }
    today = _varia_trades_today()
    weekly_net_by_host: Dict[str, float] = {}
    for host in (hosts or {"vps1": {}}):
        host_incentives = incentives.get("hosts", {}).get(host, {})
        var_incentive = host_incentives.get("variational") or {}
        ondo_incentive = host_incentives.get("ondo") or {}
        weekly_net_by_host[host] = (
            (_num(today.get("net_week_by_host", {}).get(host)) or 0.0)
            + (_num(var_incentive.get("settled_week_usdc")) or 0.0)
            + (_num(ondo_incentive.get("settled_week_usdc")) or 0.0)
        )
    return {
        "present": bool(hosts), "hosts": hosts, "auto": auto_ctl,
        "host_venues": {
            host: {"var": "variational", "hedge": data.get("hedge_venue"),
                   "hedge_label": data.get("hedge_label")}
            for host, data in sorted(hosts.items())
        },
        "equity_total": round(equity_total, 2) if equity_found and equity_complete else None,
        "equity_complete": equity_complete,
        # Legacy totals remain for older readers. New clients should use the
        # source-auditable points_by_venue structure below.
        "points_decibel": round(points_dec, 4) if points_dec_complete and points_dec is not None else None,
        "points_variational": round(points_var, 4) if points_var_complete and points_var is not None else None,
        "points_complete": {"decibel": points_dec_complete, "variational": points_var_complete},
        "points_by_venue": points_by_venue,
        "volume_weekly": round(vol_weekly, 2) if vol_found and volume_weekly_complete else None,
        "volume_total": round(vol_total, 2) if vol_found and volume_total_complete else None,
        "volume_complete": {"weekly": volume_weekly_complete, "total": volume_total_complete},
        "single_leg": single_leg,
        "pairs": pairs[:12],
        "position_sources": {
            "verified_hosts": verified_hosts,
            "unverified": unverified_hosts,
        },
        "today": today,
        "budget": _varia_budget(weekly_net_by_host),
        "capital": capital,
        "incentives": incentives,
        "pnl_attribution": pnl_attribution,
        "equity_history": equity_history,
    }


def _equity_history() -> Dict[str, Any]:
    """Deprecated aggregate series; retained only for extension compatibility.

    It predates the reconciled cashflow ledger and must never be shown as
    investment performance. `_reconciled_pnl_history` is the only UI source.
    """
    return {"present": False, "valid": False, "reason": "legacy aggregate series disabled"}


def _reconciled_pnl_history(capital: Dict[str, Any]) -> Dict[str, Any]:
    """Return only snapshots written after four-source cashflow reconciliation.

    The old aggregate-equity file remains preserved for operations debugging, but
    is intentionally excluded from investment reporting because it predates the
    verified cashflow ledger.
    """
    if not capital.get("complete"):
        return {"present": False}
    raw = _read_json(VARIA_RECONCILED_PNL_HISTORY)
    points: List[dict] = []
    if isinstance(raw, list):
        for row in raw:
            if not isinstance(row, dict):
                continue
            pnl = _num(row.get("pnl"))
            equity = _num(row.get("equity"))
            principal = _num(row.get("principal"))
            timestamp = str(row.get("timestamp") or "")
            if pnl is None or equity is None or principal is None or not timestamp:
                continue
            try:
                display_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(
                    timezone(timedelta(hours=8))
                ).strftime("%m-%d %H:%M")
            except ValueError:
                display_time = timestamp[:16].replace("T", " ")
            points.append({
                "t": display_time,
                "v": round(pnl, 2),
                "equity": round(equity, 2),
                "principal": round(principal, 2),
            })
    current = {
        "t": datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%m-%d %H:%M"),
        "v": round(_num(capital.get("pnl")) or 0.0, 2),
        "equity": round(_num(capital.get("current_equity")) or 0.0, 2),
        "principal": round(_num(capital.get("principal_total")) or 0.0, 2),
    }
    if not points or points[-1]["v"] != current["v"] or points[-1]["equity"] != current["equity"]:
        points.append(current)
    values = [point["v"] for point in points]
    first, last = values[0], values[-1]
    return {
        "present": True,
        "valid": True,
        "recording": True,
        "points": points[-144:],
        "first": first,
        "last": last,
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "change": round(last - first, 2),
        "change_pct": round((last - first) / first * 100, 2) if first else None,
        "metric": "reconciled_pnl",
        "change_basis": "recorded_interval",
    }


def record_reconciled_pnl_snapshot() -> Dict[str, Any]:
    """Persist one verified four-source PnL sample for the read-only timer."""
    # Reuse the exact four-source state mapping used by the console so a timer
    # can never record a different accounting view from the one the user sees.
    capital = (_var_decibel().get("capital") or {})
    if not capital.get("complete"):
        return {"ok": False, "reason": capital.get("reason", "两台 VPS 的指定交易账户尚未完成对账")}
    point = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pnl": round(_num(capital.get("pnl")) or 0.0, 6),
        "equity": round(_num(capital.get("current_equity")) or 0.0, 6),
        "principal": round(_num(capital.get("principal_total")) or 0.0, 6),
    }
    raw = _read_json(VARIA_RECONCILED_PNL_HISTORY)
    rows = raw if isinstance(raw, list) else []
    last = rows[-1] if rows and isinstance(rows[-1], dict) else {}
    last_time = None
    try:
        last_time = datetime.fromisoformat(str(last.get("timestamp", "")).replace("Z", "+00:00"))
    except ValueError:
        pass
    last_pnl = _num(last.get("pnl"))
    due = last_time is None or (datetime.now(timezone.utc) - last_time.astimezone(timezone.utc)).total_seconds() >= 300
    changed = last_pnl is None or abs(point["pnl"] - last_pnl) >= 0.005
    if due or changed:
        rows.append(point)
        VARIA_RECONCILED_PNL_HISTORY.parent.mkdir(parents=True, exist_ok=True)
        VARIA_RECONCILED_PNL_HISTORY.write_text(json.dumps(rows[-2016:], ensure_ascii=False, indent=2), encoding="utf-8")
    return {"ok": True, "point": point, "written": bool(due or changed)}


def _capital_flow_amount(flow: Any) -> Optional[float]:
    """Return a signed external cashflow without treating it as trading PnL."""
    if not isinstance(flow, dict):
        return None
    amount = _num(flow.get("amount", flow.get("amount_usdc")))
    if amount is None:
        return None
    kind = str(flow.get("type") or "").strip().lower()
    if kind in {"withdraw", "withdrawal", "out"} and amount > 0:
        return -amount
    return amount


def _capital_accounting(hosts: Dict[str, dict]) -> Dict[str, Any]:
    """Separate capital flows from equity so deposits can never be reported as PnL.

    The ledger is intentionally append-only and only contains reconciled external
    transfers. A missing source makes PnL unavailable instead of guessing.
    """
    ledger = _read_json(VARIA_CAPITAL_LEDGER)
    if not isinstance(ledger, dict):
        return {"present": False, "complete": False, "reason": "本金账本不存在"}

    initial_total = net_cashflow = current_total = 0.0
    complete = bool(hosts)
    rows: List[dict] = []
    missing: List[str] = []
    for host in sorted(hosts):
        host_ledger = ledger.get(host) if isinstance(ledger.get(host), dict) else {}
        hedge_venue = str(hosts.get(host, {}).get("hedge_venue") or _host_hedge_venue(host))
        values = (
            ("variational", "equity_var", "Var"),
            (hedge_venue, "equity_hedge", _venue_label(hedge_venue)),
        )
        for venue, equity_key, label in values:
            entry = host_ledger.get(venue) if isinstance(host_ledger, dict) else None
            current = _num(hosts.get(host, {}).get(equity_key))
            initial = _num(entry.get("initial")) if isinstance(entry, dict) else None
            flows = entry.get("cashflows") if isinstance(entry, dict) else None
            reconciled = bool(entry.get("reconciled")) if isinstance(entry, dict) else False
            if current is None or initial is None or not isinstance(flows, list) or not reconciled:
                complete = False
                missing.append(f"{host.upper()} {label}")
                continue
            flow_total = sum(
                amount for amount in (_capital_flow_amount(flow) for flow in flows)
                if amount is not None
            )
            principal = initial + flow_total
            initial_total += initial
            net_cashflow += flow_total
            current_total += current
            rows.append({
                "host": host,
                "venue": venue,
                "initial": round(initial, 6),
                "cashflow": round(flow_total, 6),
                "principal": round(principal, 6),
                "current": round(current, 6),
                "pnl": round(current - principal, 6),
            })

    if not complete:
        return {
            "present": True, "complete": False,
            "reason": "待对账：" + "、".join(missing), "sources": rows,
        }
    principal_total = initial_total + net_cashflow
    return {
        "present": True,
        "complete": True,
        "initial_principal": round(initial_total, 2),
        "net_cashflow": round(net_cashflow, 2),
        "principal_total": round(principal_total, 2),
        "current_equity": round(current_total, 2),
        "pnl": round(current_total - principal_total, 2),
        "pnl_pct": round((current_total - principal_total) / principal_total * 100, 2)
        if principal_total else None,
        "sources": rows,
    }


# ---------- Single Account ----------

def _sa_worker_pid() -> Optional[int]:
    """先查 systemd 单元(sa-paper-worker.service,2026-07-08 起的正规跑法——
    dashboard Popen 子进程会被 dashboard 重启连坐杀死,7/1 停摆即此因),
    再退回 dashboard 遗留的 pid 文件。"""
    import subprocess
    try:
        r = subprocess.run(["systemctl", "show", "sa-paper-worker.service",
                            "-p", "ActiveState", "-p", "MainPID"],
                           capture_output=True, text=True, timeout=5)
        props = dict(ln.split("=", 1) for ln in r.stdout.splitlines() if "=" in ln)
        if props.get("ActiveState") == "active" and props.get("MainPID", "0") != "0":
            return int(props["MainPID"])
    except Exception:
        pass
    pid_path = DATA_DIR / ".single_account_paper.pid"
    if not pid_path.exists():
        return None
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return pid
    except Exception:
        return None


def _single_account() -> Dict[str, Any]:
    state = _read_json(DATA_DIR / "single_account_paper_state.json")
    out: Dict[str, Any] = {"present": state is not None,
                           "worker_pid": _sa_worker_pid(),
                           "automation_draft": _read_json(DATA_DIR / "single_account_automation_draft.json")}
    out["worker_running"] = out["worker_pid"] is not None
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


def _varia_decibel_scan_state() -> dict:
    state = _varia_raw_states().get("vps1", {})
    embedded = state.get("var_decibel_market_scan") if isinstance(state, dict) else None
    direct = _read_json(VARIA_DIR / "var_decibel_market_scan.json")
    candidates = [item for item in (embedded, direct) if isinstance(item, dict) and item]
    if not candidates:
        return {}

    def generated_at(item: dict) -> float:
        parsed = _parse_ts(item.get("generated_at"))
        return parsed if parsed is not None else 0.0

    return max(candidates, key=generated_at)


def _varia_quotes_from_readonly_scan(scan: dict) -> List[dict]:
    rows = scan.get("rows") if isinstance(scan.get("rows"), list) else []
    generated_at = str(scan.get("generated_at") or "")
    quotes: List[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        quote = {
            "symbol": str(row.get("symbol") or "").upper(),
            "timestamp": str(row.get("timestamp") or generated_at),
            "var_bid": _num(row.get("var_bid_1k")),
            "var_ask": _num(row.get("var_ask_1k")),
            "decibel_bid": _num(row.get("decibel_bid")),
            "decibel_ask": _num(row.get("decibel_ask")),
            "source": "read_only_market_scan",
        }
        if not quote["symbol"] or None in (
            quote["var_bid"], quote["var_ask"], quote["decibel_bid"], quote["decibel_ask"]
        ):
            continue
        ts = _parse_ts(quote["timestamp"])
        quote["age_sec"] = max(0, int(time.time() - ts)) if ts is not None else None
        quote.update(_varia_quote_direction(quote))
        quotes.append(quote)
    return quotes


def _varia_latest_quotes() -> List[dict]:
    """每个 symbol 最近一条完整双边报价；优先使用独立只读扫描。"""
    scan_quotes = _varia_quotes_from_readonly_scan(_varia_decibel_scan_state())
    if scan_quotes:
        return scan_quotes
    path = VARIA_DIR / "hedge_bot.sqlite3"
    rows: List[dict] = []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        cur = conn.execute(
            "SELECT m.symbol, m.timestamp, m.var_bid_1k, m.var_ask_1k, "
            "m.decibel_bid, m.decibel_ask FROM market_snapshots m "
            "JOIN (SELECT symbol, MAX(id) AS max_id FROM market_snapshots "
            "WHERE var_bid_1k IS NOT NULL AND var_ask_1k IS NOT NULL "
            "AND decibel_bid IS NOT NULL AND decibel_ask IS NOT NULL GROUP BY symbol) latest "
            "ON m.id = latest.max_id ORDER BY m.symbol"
        )
        for symbol, timestamp, var_bid, var_ask, dec_bid, dec_ask in cur.fetchall():
            quote = {
                "symbol": str(symbol or "").upper(),
                "timestamp": str(timestamp or ""),
                "var_bid": _num(var_bid), "var_ask": _num(var_ask),
                "decibel_bid": _num(dec_bid), "decibel_ask": _num(dec_ask),
            }
            ts = _parse_ts(timestamp)
            quote["age_sec"] = max(0, int(time.time() - ts)) if ts is not None else None
            quote.update(_varia_quote_direction(quote))
            rows.append(quote)
        conn.close()
    except Exception:
        return []
    return rows


def _varia_quote_direction(quote: dict) -> Dict[str, Any]:
    vb, va = _num(quote.get("var_bid")), _num(quote.get("var_ask"))
    db, da = _num(quote.get("decibel_bid")), _num(quote.get("decibel_ask"))
    if None in (vb, va, db, da):
        return {"recommended": None, "costs": {}}
    mid = (vb + va + db + da) / 4
    if mid <= 0:
        return {"recommended": None, "costs": {}}
    costs = {
        "var_buy": round((va - db) / mid * 10000, 4),
        "var_sell": round((da - vb) / mid * 10000, 4),
    }
    return {"recommended": min(costs, key=costs.get), "costs": costs}


def _varia_recent_jobs(limit: int = 8) -> List[dict]:
    path = VARIA_DIR / "hedge_bot.sqlite3"
    jobs: List[dict] = []
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        cur = conn.execute(
            "SELECT id, kind, status, created_at, started_at, finished_at, "
            "payload_json, result_json, error_message FROM dashboard_jobs "
            "WHERE kind IN ('manual_live','close_all') ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        for row in cur.fetchall():
            payload = _json_object(row[6])
            result = _json_object(row[7])
            status = str(row[2] or "")
            jobs.append({
                "id": row[0], "kind": row[1], "status": status,
                "created_at": str(row[3] or ""), "started_at": str(row[4] or ""),
                "finished_at": str(row[5] or ""), "symbol": payload.get("symbol"),
                "host": payload.get("host"), "error": _varia_job_error(row[8], result),
                "summary": _varia_job_summary(status, result),
            })
        conn.close()
    except Exception:
        return []
    return jobs


def _varia_active_job() -> Optional[dict]:
    path = VARIA_DIR / "hedge_bot.sqlite3"
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            row = conn.execute(
                "SELECT id, kind, status FROM dashboard_jobs "
                "WHERE status IN ('queued','running') ORDER BY id LIMIT 1"
            ).fetchone()
        return {"id": row[0], "kind": row[1], "status": row[2]} if row else None
    except Exception:
        return None


def _json_object(value: Any) -> dict:
    try:
        parsed = json.loads(str(value or "{}"))
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _varia_job_error(error: Any, result: dict) -> str:
    raw = str(error or result.get("stderr") or "").strip()
    return raw[:240]


def _varia_job_summary(status: str, result: dict) -> str:
    if status in {"queued", "running"}:
        return "已提交，后台执行中" if status == "queued" else "正在执行"
    if status == "succeeded":
        return "执行成功"
    if status == "blocked":
        return "预检拦截，未下单"
    if status == "failed":
        return "执行失败"
    return status or "未知"


def _varia_control_state(vd: Optional[dict] = None) -> Dict[str, Any]:
    vd = vd if isinstance(vd, dict) else _var_decibel()
    quotes = _varia_latest_quotes()
    symbols: List[str] = []
    for symbol in (
        list(VARIA_MARKET_CANDIDATES)
        + [str(q.get("symbol") or "").upper() for q in quotes]
        + [str(p.get("symbol") or "").upper() for p in vd.get("pairs", [])]
    ):
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    jobs = _varia_recent_jobs()
    host_controls = {}
    for host in ("vps1", "vps2"):
        hedge_venue = _host_hedge_venue(host)
        host_controls[host] = {
            "hedge_venue": hedge_venue,
            "hedge_label": _venue_label(hedge_venue),
            # The current manual quote/submit endpoint still consumes the Decibel
            # market snapshot. Keep VPS2 disabled until its Ondo-native manual
            # quote and one-click confirmation path is separately verified.
            "manual_open_supported": hedge_venue == "decibel",
            "reason": None if hedge_venue == "decibel" else "VPS2 Var/Ondo 手动开仓入口尚未验收",
        }
    return {
        "symbols": symbols, "quotes": quotes, "pairs": vd.get("pairs", []),
        "single_leg": vd.get("single_leg", []), "hosts": vd.get("hosts", {}),
        "host_controls": host_controls,
        "jobs": jobs, "active_job": _varia_active_job(),
        "max_leverage": 40,
    }


def _normalize_varia_auto_state(raw: Any) -> Dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    raw_hosts = raw.get("hosts") if isinstance(raw.get("hosts"), dict) else {}
    pressure = raw.get("pressure_test") if isinstance(raw.get("pressure_test"), dict) else {}
    mode = str(raw.get("mode") or "semi_auto")
    if mode not in {"semi_auto", "full_auto"}:
        mode = "semi_auto"
    raw_min_minutes = _num(pressure.get("min_open_interval_minutes"))
    raw_max_minutes = _num(pressure.get("max_open_interval_minutes"))
    min_minutes = int(raw_min_minutes if raw_min_minutes is not None else 30)
    max_minutes = int(raw_max_minutes if raw_max_minutes is not None else 180)
    cap = _num(raw.get("weekly_loss_cap_usdc"))
    max_spread = _num(raw.get("max_auto_spread_bps"))
    ratio = _num(raw.get("major_ratio"))
    hosts: Dict[str, dict] = {}
    for host, default_strategy in (("vps1", "A"), ("vps2", "B")):
        configured = raw_hosts.get(host) if isinstance(raw_hosts.get(host), dict) else {}
        strategy = str(configured.get("strategy") or default_strategy).upper()
        hosts[host] = {
            "enabled": bool(configured.get("enabled")),
            "strategy": "B" if strategy == "B" else "A",
        }
    return {
        "enabled": bool(raw.get("enabled")),
        "mode": mode,
        "weekly_loss_cap_usdc": str(cap if cap is not None else 5),
        "max_auto_spread_bps": str(max(0.0, max_spread if max_spread is not None else 2)),
        "major_ratio": str(min(1.0, max(0.0, ratio if ratio is not None else 0.8))),
        "pressure_test": {
            "enabled": bool(pressure.get("enabled")),
            "min_open_interval_minutes": max(1, min_minutes),
            "max_open_interval_minutes": max(max(1, min_minutes), max_minutes),
        },
        "hosts": hosts,
        "updated_at": str(raw.get("updated_at") or ""),
    }


def _varia_auto_state_file() -> Path:
    return VARIA_DIR / "auto_strategy_state.json"


def _varia_auto_unit(host: str) -> str:
    return VARIA_AUTO_WORKER_TEMPLATE.format(host=host)


def _systemd_user_env() -> Dict[str, str]:
    env = dict(os.environ)
    runtime_dir = f"/run/user/{os.getuid()}"
    env.setdefault("XDG_RUNTIME_DIR", runtime_dir)
    env.setdefault("DBUS_SESSION_BUS_ADDRESS", f"unix:path={runtime_dir}/bus")
    return env


def _varia_worker_action(host: str, action: str) -> Dict[str, Any]:
    if host not in {"vps1", "vps2"} or action not in {"start", "stop", "is-active"}:
        return {"rc": -1, "out": "", "err": "invalid worker action"}
    unit = _varia_auto_unit(host)
    if host == "vps1":
        return _run_cmd(
            ["systemctl", "--user", action, unit], timeout=20,
            env=_systemd_user_env(),
        )
    remote_action = f"systemctl --user {action} {shlex.quote(unit)}"
    if action == "is-active":
        remote_action += " || true"
    return _run_cmd([
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", "StrictHostKeyChecking=yes", VARIA_VPS2_SSH, remote_action,
    ], timeout=25)


def _varia_worker_status(host: str) -> str:
    result = _varia_worker_action(host, "is-active")
    status = str(result.get("out") or result.get("err") or "unknown").splitlines()
    value = status[-1].strip() if status else "unknown"
    return value if value in {
        "active", "activating", "deactivating", "failed", "inactive", "reloading"
    } else "unknown"


def _varia_auto_runtime(host: str) -> Dict[str, Any]:
    path = (VARIA_DIR / "auto_strategy_runtime.json" if host == "vps1" else
            VARIA_DIR / "auto_strategy_peer_runtime" / "vps2.json")
    candidates = [_read_json(path)]
    if host == "vps2":
        peer_state = _varia_raw_states().get("vps2", {})
        embedded = peer_state.get("auto_strategy_runtime") if isinstance(peer_state, dict) else None
        if isinstance(embedded, dict):
            candidates.append(embedded)
    runtime = max(
        (item for item in candidates if isinstance(item, dict)),
        key=lambda item: _parse_ts(item.get("updated_at") or item.get("last_checked_at")) or 0,
        default={},
    )
    if not runtime:
        return {"present": False, "age_sec": None}
    updated = runtime.get("updated_at") or runtime.get("last_checked_at")
    plan = dict(runtime.get("next_open_plan")) if isinstance(runtime.get("next_open_plan"), dict) else {}
    plan_updated = plan.get("generated_at") or updated
    plan_age = _iso_age(plan_updated)
    if plan:
        market_age = _iso_age(plan.get("market_scan_generated_at"))
        for leg in ("var", "hedge"):
            explicit = _num(plan.get(f"{leg}_one_way_spread_bps"))
            full = _num(
                plan.get(f"{leg}_full_spread_bps")
                if plan.get(f"{leg}_full_spread_bps") not in (None, "")
                else plan.get(f"{leg}_spread_bps")
            )
            plan[f"{leg}_one_way_spread_bps"] = (
                explicit if explicit is not None else full / 2 if full is not None else None
            )
        plan["age_sec"] = plan_age
        plan["market_data_age_sec"] = market_age
        plan["stale"] = (
            plan_age is None
            or plan_age > STALE_SEC
            or (market_age is not None and market_age > STALE_SEC)
        )
    return {
        "present": True,
        "age_sec": _iso_age(updated),
        "status": str(runtime.get("status") or ""),
        "message": str(runtime.get("message") or "")[:180],
        "last_checked_at": runtime.get("last_checked_at"),
        "last_action_at": runtime.get("last_action_at"),
        "pressure_test": runtime.get("pressure_test") if isinstance(runtime.get("pressure_test"), dict) else {},
        "next_open_plan": plan,
    }


def _varia_strategy_symbol_config() -> Dict[str, List[str]]:
    """Read the worker's configured pools without importing the trading process."""
    fallback = {
        "major_symbols": ["BTC", "ETH"],
        "opportunity_symbols": [
            symbol for symbol in VARIA_MARKET_CANDIDATES
            if symbol not in {"BTC", "ETH", "HYPE"}
        ],
    }
    path = VARIA_DIR.parent / "config.yaml"
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return fallback
    result = dict(fallback)
    for key in ("major_symbols", "opportunity_symbols"):
        prefix = f"{key}:"
        line = next((item.strip() for item in lines if item.strip().startswith(prefix)), "")
        if not line:
            continue
        try:
            parsed = ast.literal_eval(line.split(":", 1)[1].strip())
        except (SyntaxError, ValueError):
            continue
        if isinstance(parsed, list):
            symbols = []
            for value in parsed:
                symbol = str(value or "").strip().upper()
                if symbol and symbol not in symbols:
                    symbols.append(symbol)
            if symbols:
                result[key] = symbols
    return result


def _varia_strategy_pools(max_spread_bps: float = 2.0) -> Dict[str, Any]:
    """Expose the real worker pools and fresh-quote readiness to the console."""
    configured = _varia_strategy_symbol_config()
    all_symbols = list(dict.fromkeys(VARIA_MARKET_CANDIDATES))
    all_set = set(all_symbols)
    majors = [symbol for symbol in configured["major_symbols"] if symbol in all_set]
    opportunities = [
        symbol for symbol in configured["opportunity_symbols"]
        if symbol in all_set and symbol not in majors
    ]
    others = [
        symbol for symbol in all_symbols
        if symbol not in set(majors) and symbol not in set(opportunities)
    ]
    opportunity_pool = opportunities + others
    ready: List[str] = []
    allowed: List[str] = []
    blocked: List[str] = []
    metrics: Dict[str, dict] = {}
    for quote in _varia_latest_quotes():
        symbol = str(quote.get("symbol") or "").upper()
        age = quote.get("age_sec")
        if not symbol or age is None or int(age) > 600:
            continue
        ready.append(symbol)
        var_bid, var_ask = _num(quote.get("var_bid")), _num(quote.get("var_ask"))
        dec_bid, dec_ask = _num(quote.get("decibel_bid")), _num(quote.get("decibel_ask"))
        if None in (var_bid, var_ask, dec_bid, dec_ask):
            continue
        var_mid = (var_bid + var_ask) / 2
        dec_mid = (dec_bid + dec_ask) / 2
        if var_mid <= 0 or dec_mid <= 0:
            continue
        var_spread = (var_ask - var_bid) / var_mid * 10000 / 2
        dec_spread = (dec_ask - dec_bid) / dec_mid * 10000 / 2
        costs = quote.get("costs") if isinstance(quote.get("costs"), dict) else {}
        numeric_costs = [value for value in (_num(item) for item in costs.values()) if value is not None]
        # Keep the sign for like-for-like route comparison. A negative value
        # means the current executable cross-venue quotes are favorable; the
        # spread guard below still uses max(), so a favorable basis never
        # bypasses either venue's own spread limit.
        entry_cost = min(numeric_costs) if numeric_costs else None
        observed = [var_spread, dec_spread] + ([entry_cost] if entry_cost is not None else [])
        worst = max(observed)
        can_trade = worst <= max_spread_bps
        recommended = {
            "var_buy": "Var 买 / Decibel 卖",
            "var_sell": "Var 卖 / Decibel 买",
        }.get(str(quote.get("recommended") or ""), "方向待定")
        metrics[symbol] = {
            "category": "rwa" if symbol in VARIA_RWA_SYMBOLS else "crypto",
            "var_spread_bps": round(var_spread, 2),
            "decibel_spread_bps": round(dec_spread, 2),
            "entry_cost_bps": round(entry_cost, 2) if entry_cost is not None else None,
            "display_bps": round(worst, 2),
            "allowed": can_trade,
            "age_sec": int(age),
            "recommended": recommended,
        }
        (allowed if can_trade else blocked).append(symbol)
    scan = _varia_decibel_scan_state()
    scan_generated = str(scan.get("generated_at") or "")
    scan_ts = _parse_ts(scan_generated)
    scan_age = max(0, int(time.time() - scan_ts)) if scan_ts is not None else None
    scan_summary = scan.get("summary") if isinstance(scan.get("summary"), dict) else {}
    scan_rows = scan.get("rows") if isinstance(scan.get("rows"), list) else []
    common_symbols = list(dict.fromkeys(
        str(row.get("symbol") or "").strip().upper()
        for row in scan_rows if isinstance(row, dict)
        and str(row.get("symbol") or "").strip().upper() in all_set
    ))
    decibel_pool = {
        "host": "vps1",
        "pair": "Var/Decibel",
        "present": bool(scan.get("present") is True or scan.get("rows")),
        "read_only": scan.get("read_only") is True,
        "mutations_sent": scan.get("mutations_sent"),
        "age_sec": scan_age,
        "stale": scan_age is None or scan_age > STALE_SEC,
        "scan_summary": scan_summary,
        "total": len(all_symbols),
        "symbols": all_symbols,
        "common": common_symbols,
        "categories": {
            symbol: ("rwa" if symbol in VARIA_RWA_SYMBOLS else "crypto")
            for symbol in all_symbols
        },
        "quote_ready": [symbol for symbol in all_symbols if symbol in set(ready)],
        "allowed": [symbol for symbol in all_symbols if symbol in set(allowed)],
        "blocked": [symbol for symbol in all_symbols if symbol in set(blocked)],
        "metrics": metrics,
        "max_spread_bps": max_spread_bps,
    }
    ondo_pool = _varia_ondo_strategy_pool()
    route_comparison = _varia_route_comparison(decibel_pool, ondo_pool)
    return {
        "total": len(all_symbols),
        "major": majors,
        "opportunity": opportunity_pool,
        "quote_ready": [symbol for symbol in all_symbols if symbol in set(ready)],
        "allowed": [symbol for symbol in all_symbols if symbol in set(allowed)],
        "blocked": [symbol for symbol in all_symbols if symbol in set(blocked)],
        "metrics": metrics,
        "max_spread_bps": max_spread_bps,
        "venues": {"decibel": decibel_pool, "ondo": ondo_pool},
        "route_comparison": route_comparison,
        "strategy_a": {
            "eligible": all_symbols,
            "major": majors,
            "opportunity": opportunity_pool,
        },
        "strategy_b": {
            "priority": opportunity_pool,
            "fallback": majors,
            "eligible": opportunity_pool + majors,
        },
    }


def _varia_route_comparison(decibel_pool: dict, ondo_pool: dict) -> List[dict]:
    """Compare like-for-like entry friction without crossing account ownership."""
    decibel_metrics = decibel_pool.get("metrics") if isinstance(decibel_pool.get("metrics"), dict) else {}
    ondo_metrics = ondo_pool.get("metrics") if isinstance(ondo_pool.get("metrics"), dict) else {}
    ordered_symbols = [
        str(symbol).upper() for symbol in (decibel_pool.get("symbols") or [])
        if str(symbol).upper() in decibel_metrics and str(symbol).upper() in ondo_metrics
    ]
    rows: List[dict] = []
    for symbol in ordered_symbols:
        decibel = decibel_metrics[symbol]
        ondo = ondo_metrics[symbol]
        decibel_cost = _num(decibel.get("entry_cost_bps"))
        ondo_cost = _num(ondo.get("entry_cost_bps"))
        decibel_allowed = decibel.get("allowed") is True
        ondo_allowed = ondo.get("allowed") is True
        preferred: Optional[str]
        reason: str
        if decibel_allowed and ondo_allowed:
            if decibel_cost is None and ondo_cost is None:
                preferred, reason = None, "两边入场价差均待定"
            elif ondo_cost is not None and (decibel_cost is None or ondo_cost < decibel_cost):
                preferred, reason = "ondo", "两边均通过，Ondo 入场价差更低"
            else:
                preferred, reason = "decibel", "两边均通过，Decibel 入场价差更低"
        elif decibel_allowed:
            preferred, reason = "decibel", "仅 Decibel 当前通过门槛"
        elif ondo_allowed:
            preferred, reason = "ondo", "仅 Ondo 当前通过门槛"
        else:
            preferred, reason = None, "两条路线当前均未通过门槛"
        savings = (
            abs(decibel_cost - ondo_cost)
            if decibel_cost is not None and ondo_cost is not None else None
        )
        rows.append({
            "symbol": symbol,
            "preferred": preferred,
            "preferred_label": {"decibel": "Var/Decibel", "ondo": "Var/Ondo"}.get(preferred, "暂不交易"),
            "reason": reason,
            "entry_savings_bps": round(savings, 4) if savings is not None else None,
            "decibel": {
                "allowed": decibel_allowed,
                "direction": str(decibel.get("recommended") or "方向待定"),
                "entry_cost_bps": decibel_cost,
                "spread_bps": _num(decibel.get("display_bps")),
            },
            "ondo": {
                "allowed": ondo_allowed,
                "direction": str(ondo.get("recommended") or "方向待定"),
                "entry_cost_bps": ondo_cost,
                "net_funding_24h_bps": _num(ondo.get("net_funding_24h_bps")),
                "spread_bps": max(
                    value for value in (
                        _num(ondo.get("var_spread_bps")),
                        _num(ondo.get("ondo_spread_bps")),
                    ) if value is not None
                ) if any(
                    value is not None for value in (
                        _num(ondo.get("var_spread_bps")),
                        _num(ondo.get("ondo_spread_bps")),
                    )
                ) else None,
            },
        })
    rows.sort(key=lambda row: (
        row["preferred"] is None,
        not (row["decibel"]["allowed"] and row["ondo"]["allowed"]),
        min(
            value for value in (
                row["decibel"]["entry_cost_bps"], row["ondo"]["entry_cost_bps"]
            ) if value is not None
        ) if any(
            value is not None for value in (
                row["decibel"]["entry_cost_bps"], row["ondo"]["entry_cost_bps"]
            )
        ) else float("inf"),
        row["symbol"],
    ))
    return rows


def _varia_ondo_strategy_pool() -> Dict[str, Any]:
    """Expose only VPS2's own read-only Var/Ondo scan; never reuse VPS1 quotes."""
    state = _varia_raw_states().get("vps2", {})
    scan = state.get("var_ondo_market_scan") if isinstance(state, dict) else None
    scan = scan if isinstance(scan, dict) else {}
    generated_at = str(scan.get("generated_at") or "")
    generated_ts = _parse_ts(generated_at)
    age_sec = max(0, int(time.time() - generated_ts)) if generated_ts is not None else None
    stale = age_sec is None or age_sec > STALE_SEC
    source_rows = scan.get("rows") if isinstance(scan.get("rows"), list) else []
    rows: List[dict] = []
    metrics: Dict[str, dict] = {}
    allowed: List[str] = []
    blocked: List[str] = []
    for raw in source_rows:
        if not isinstance(raw, dict):
            continue
        symbol = str(raw.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        reasons = [str(item) for item in (raw.get("block_reasons") or []) if item]
        eligible = bool(raw.get("eligible") is True and not stale)
        if stale and "scan_stale" not in reasons:
            reasons.append("scan_stale")
        metric = {
            "category": str(raw.get("category") or "opportunity"),
            "var_spread_bps": _num(raw.get("var_half_spread_bps")),
            "ondo_spread_bps": _num(raw.get("ondo_half_spread_bps")),
            "entry_cost_bps": _num(raw.get("recommended_entry_cost_bps")),
            "net_funding_24h_bps": _num(raw.get("recommended_net_funding_24h_bps")),
            "expected_24h_cost_bps": _num(raw.get("recommended_expected_24h_cost_bps")),
            "funding_projection_note": str(raw.get("funding_projection_note") or ""),
            "direction_selection_policy": str(raw.get("direction_selection_policy") or ""),
            "minimum_net_funding_24h_bps": _num(raw.get("minimum_net_funding_24h_bps")),
            "basis_bps": _num(raw.get("midpoint_basis_bps")),
            "recommended": _varia_direction_cn(raw.get("recommended"), "Ondo"),
            "allowed": eligible,
            "block_reasons": reasons,
            "age_sec": _num(raw.get("quote_age_seconds")),
            "volume_24h": _num(raw.get("volume_24h")),
            "max_spread_bps": _num(raw.get("max_spread_bps")),
        }
        rows.append({"symbol": symbol, **metric})
        metrics[symbol] = metric
        (allowed if eligible else blocked).append(symbol)
    summary = scan.get("summary") if isinstance(scan.get("summary"), dict) else {}
    return {
        "host": "vps2",
        "pair": "Var/Ondo",
        "present": bool(scan.get("present") is True or scan.get("rows")),
        "ok": bool(scan.get("ok")) and not stale,
        "stale": stale,
        "generated_at": generated_at or None,
        "age_sec": age_sec,
        "total": int(summary.get("common_markets") or len(rows)),
        "common": [row["symbol"] for row in rows],
        "categories": {row["symbol"]: row["category"] for row in rows},
        "quote_ready": [row["symbol"] for row in rows if "ondo_quote_failed" not in row["block_reasons"]],
        "allowed": allowed,
        "blocked": blocked,
        "metrics": metrics,
        "rows": rows,
        "thresholds_bps": scan.get("thresholds_bps") if isinstance(scan.get("thresholds_bps"), dict) else {},
        "funding_horizon_hours": int(scan.get("funding_horizon_hours") or 24),
    }


def _varia_direction_cn(value: Any, hedge_label: str) -> str:
    text = str(value or "")
    if text == f"Var buy / {hedge_label} sell":
        return f"Var 买 / {hedge_label} 卖"
    if text == f"Var sell / {hedge_label} buy":
        return f"Var 卖 / {hedge_label} 买"
    return text


def _varia_host_live_readiness(
    host: str, raw_state: Optional[dict] = None, strategy: Optional[str] = None,
) -> Dict[str, Any]:
    hedge_venue = _host_hedge_venue(host)
    expected_strategy = VARIA_STRATEGY_BY_HOST.get(host)
    strategy_ok = strategy is None or strategy == expected_strategy
    if hedge_venue != "ondo":
        return {
            "ready": strategy_ok, "hedge_venue": hedge_venue,
            "hedge_label": _venue_label(hedge_venue),
            "expected_strategy": expected_strategy,
            "reason": (None if strategy_ok else
                       f"当前只验收策略 {expected_strategy}（Var/{_venue_label(hedge_venue)}）"),
            "acceptance": None,
        }
    state = raw_state if isinstance(raw_state, dict) else _varia_raw_states().get(host, {})
    acceptance = state.get("ondo_acceptance") if isinstance(state, dict) else None
    acceptance = acceptance if isinstance(acceptance, dict) else {"present": False}
    read_only = acceptance.get("read_only") if isinstance(acceptance.get("read_only"), dict) else {}
    mutation = acceptance.get("mutation") if isinstance(acceptance.get("mutation"), dict) else {}
    policy = acceptance.get("policy") if isinstance(acceptance.get("policy"), dict) else {}
    pending = [
        label for name, label in VARIA_ONDO_MUTATION_LABELS
        if mutation.get(name) is not True
    ]
    read_only_ok = read_only.get("passed") is True and read_only.get("mutations_sent") is False
    policy_ok = policy.get("variational_automated_trading_authorized") is True
    ready = bool(
        strategy_ok
        and read_only_ok
        and policy_ok
        and acceptance.get("live_ready") is True
        and not pending
    )
    if not strategy_ok:
        reason = f"当前只验收策略 {expected_strategy}（Var/{_venue_label(hedge_venue)}）"
    elif not read_only_ok:
        reason = "Ondo 正式环境只读验收未通过"
    elif pending:
        reason = "Ondo 真实交易验收待完成：" + "、".join(pending)
    elif not policy_ok:
        reason = "Variational 自动交易书面授权尚未记录"
    elif not ready:
        reason = "Ondo 尚未标记为可实盘"
    else:
        reason = None
    return {
        "ready": ready, "hedge_venue": hedge_venue,
        "hedge_label": _venue_label(hedge_venue),
        "expected_strategy": expected_strategy, "reason": reason,
        "variational_policy_ready": policy_ok,
        "acceptance": acceptance,
    }


def _varia_selected_start_blocks(state: dict) -> List[str]:
    raw_states = _varia_raw_states()
    blocks: List[str] = []
    for host, configured in (state.get("hosts") or {}).items():
        if not isinstance(configured, dict) or not configured.get("enabled"):
            continue
        readiness = _varia_host_live_readiness(
            host, raw_states.get(host), str(configured.get("strategy") or ""),
        )
        if not readiness.get("ready"):
            blocks.append(f"{host.upper()}：{readiness.get('reason') or '实盘验收未完成'}")
    return blocks


def _varia_automation_state(vd: Optional[dict] = None) -> Dict[str, Any]:
    state = _normalize_varia_auto_state(_read_json(_varia_auto_state_file()))
    vd = vd if isinstance(vd, dict) else _var_decibel()
    budget = vd.get("budget") if isinstance(vd.get("budget"), dict) else {}
    host_budget = budget.get("hosts") if isinstance(budget.get("hosts"), dict) else {}
    raw_states = _varia_raw_states()
    hosts: Dict[str, dict] = {}
    for host in ("vps1", "vps2"):
        configured = state["hosts"][host]
        service = _varia_worker_status(host)
        runtime = _varia_auto_runtime(host)
        readiness = _varia_host_live_readiness(
            host, raw_states.get(host), str(configured.get("strategy") or ""),
        )
        hosts[host] = {
            **configured,
            "service": service,
            "running": bool(state["enabled"] and configured["enabled"] and service == "active"),
            "runtime": runtime,
            "budget": host_budget.get(host) if isinstance(host_budget.get(host), dict) else None,
            "hedge_venue": readiness["hedge_venue"],
            "hedge_label": readiness["hedge_label"],
            "expected_strategy": readiness["expected_strategy"],
            "live_ready": readiness["ready"],
            "start_blocked": not readiness["ready"],
            "start_block_reason": readiness["reason"],
            "acceptance": readiness["acceptance"],
        }
    selected = [host for host, item in state["hosts"].items() if item.get("enabled")]
    running = [host for host, item in hosts.items() if item.get("running")]
    starting = [host for host, item in hosts.items() if item.get("service") == "activating"]
    if not state["enabled"]:
        status = "stopped"
    elif starting:
        status = "starting"
    elif selected and len(running) == len(selected):
        status = "running"
    elif running:
        status = "partial"
    else:
        status = "attention"
    start_blocks = [
        f"{host.upper()}：{item.get('start_block_reason')}"
        for host, item in hosts.items()
        if item.get("enabled") and item.get("start_blocked")
    ]
    return {
        **state,
        "status": status,
        "selected_hosts": selected,
        "running_hosts": running,
        "hosts": hosts,
        "start_blocked": bool(start_blocks),
        "start_block_reasons": start_blocks,
        "budget": budget,
        "strategy_pools": _varia_strategy_pools(float(state["max_auto_spread_bps"])),
    }


def _varia_root() -> Path:
    return VARIA_DIR.parent


def _varia_python() -> str:
    return str(_varia_root() / ".venv" / "bin" / "python")


def _varia_raw_states() -> Dict[str, dict]:
    """Return the newest raw ops snapshot per host without copying one host to another."""
    states: Dict[str, dict] = {}
    peer_dir = VARIA_DIR / "ops_peer_state"
    for path in (sorted(peer_dir.glob("*.json")) if peer_dir.exists() else []):
        state = _read_json(path)
        if not isinstance(state, dict):
            continue
        host = str(state.get("host_id") or path.stem).lower()
        host = "vps1" if host.startswith("vm-") else host
        states[host] = state
    local = _read_json(VARIA_DIR / "ops_state.json")
    if isinstance(local, dict) and local.get("host_id"):
        host = str(local["host_id"]).lower()
        host = "vps1" if host.startswith("vm-") else host
        states[host] = local
    return states


def _position_payload(value: Any) -> dict:
    if not isinstance(value, dict):
        return {}
    payload = value.get("position")
    return payload if isinstance(payload, dict) else value


def _signed_position_size(position: dict) -> float:
    size = _num(position.get("size"))
    if size is None:
        size = _num(position.get("position_size"))
    if size is None:
        size = _num(position.get("qty"))
    size = size or 0.0
    side = str(position.get("side") or "").lower()
    if side in {"short", "sell"}:
        return -abs(size)
    if side in {"long", "buy"}:
        return abs(size)
    return size


def _varia_live_command(
    *, host: str, symbol: str, var_side: str, quantity: float, leverage: float,
    notional: Optional[float] = None, reduce_only: bool = False,
    take_profit: Optional[float] = None, stop_loss: Optional[float] = None,
    hedge_venue: str = "decibel",
) -> List[str]:
    command = [
        _varia_python(), "-m", "src.main", "--mode", "live", "--config", "config.yaml",
        "--symbol", symbol.upper(), "--side", var_side,
        "--quantity", f"{quantity:.10f}".rstrip("0").rstrip("."),
        "--leverage", f"{leverage:g}", "--leverage-cap", "40",
    ]
    if hedge_venue != "decibel":
        command.extend(["--hedge-venue", hedge_venue])
    if notional is not None and not reduce_only:
        command.extend(["--notional", f"{notional:.8f}".rstrip("0").rstrip(".")])
    if reduce_only:
        command.append("--reduce-only")
    if take_profit is not None and not reduce_only:
        command.extend(["--take-profit", f"{take_profit:.10f}".rstrip("0").rstrip(".")])
    if stop_loss is not None and not reduce_only:
        command.extend(["--stop-loss", f"{stop_loss:.10f}".rstrip("0").rstrip(".")])
    if host == "vps1":
        return command
    if host != "vps2":
        raise ValueError("host must be vps1 or vps2")
    remote_command = command.copy()
    remote_command[0] = ".venv/bin/python"
    shell = (
        "cd /home/ubuntu/varia-decibel-farming-live && "
        "set -a && . ./.env.dashboard && set +a && exec " + shlex.join(remote_command)
    )
    return [
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", "StrictHostKeyChecking=yes", VARIA_VPS2_SSH, shell,
    ]


def _varia_close_commands() -> tuple[List[dict], List[str]]:
    commands: List[dict] = []
    blocked: List[str] = []
    for host, state in _varia_raw_states().items():
        age = _iso_age(state.get("generated_at"))
        exchanges = state.get("exchanges") if isinstance(state.get("exchanges"), dict) else {}
        hedge_venue = _host_hedge_venue(host)
        dec = exchanges.get(hedge_venue) if isinstance(exchanges.get(hedge_venue), dict) else {}
        var = exchanges.get("variational") if isinstance(exchanges.get("variational"), dict) else {}
        verified = age is not None and age <= STALE_SEC and dec.get("ok") is True and var.get("ok") is True
        if not verified:
            blocked.append(f"{host.upper()} 仓位快照未核验")
            continue
        ds = dec.get("symbols") if isinstance(dec.get("symbols"), dict) else {}
        vs = var.get("symbols") if isinstance(var.get("symbols"), dict) else {}
        for symbol in sorted(set(ds) | set(vs)):
            dp, vp = _position_payload(ds.get(symbol)), _position_payload(vs.get(symbol))
            d_open, v_open = _pos_open(ds.get(symbol)), _pos_open(vs.get(symbol))
            if not d_open and not v_open:
                continue
            if d_open != v_open:
                blocked.append(f"{host.upper()}·{symbol} 是单腿仓位")
                continue
            if hedge_venue == "ondo":
                readiness = _varia_host_live_readiness(host, state)
                mutation = (readiness.get("acceptance") or {}).get("mutation") or {}
                if mutation.get("reduce_only_close") is not True:
                    blocked.append(f"{host.upper()}·{symbol} Ondo 平仓链路尚未验收")
                    continue
            d_size, v_size = _signed_position_size(dp), _signed_position_size(vp)
            quantity = min(abs(d_size), abs(v_size))
            if quantity <= 0:
                blocked.append(f"{host.upper()}·{symbol} 数量不可读")
                continue
            var_side = "sell" if v_size > 0 else "buy"
            command = _varia_live_command(
                host=host, symbol=symbol, var_side=var_side, quantity=quantity,
                leverage=1, reduce_only=True, hedge_venue=hedge_venue,
            )
            commands.append({
                "command": command, "host": host, "symbol": symbol.upper(),
                "planned_var_side": var_side, "planned_quantity": f"{quantity:.10f}",
                "hedge_venue": hedge_venue,
            })
    return commands, blocked


def _enqueue_varia_job(*, kind: str, command: dict, payload: dict) -> int:
    path = VARIA_DIR / "hedge_bot.sqlite3"
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    conn = sqlite3.connect(path, timeout=5)
    try:
        conn.execute("BEGIN IMMEDIATE")
        active = conn.execute(
            "SELECT id FROM dashboard_jobs WHERE status IN ('queued','running') ORDER BY id LIMIT 1"
        ).fetchone()
        if active:
            raise RuntimeError(f"已有任务 #{active[0]} 正在执行")
        cur = conn.execute(
            "INSERT INTO dashboard_jobs "
            "(created_at, updated_at, kind, status, payload_json, command_json, attempts) "
            "VALUES (?, ?, ?, 'queued', ?, ?, 0)",
            (now, now, kind, json.dumps(payload, ensure_ascii=False),
             json.dumps(command, ensure_ascii=False)),
        )
        job_id = int(cur.lastrowid)
        conn.commit()
        return job_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _start_varia_manual_worker() -> Dict[str, Any]:
    return _run_cmd(
        ["sudo", "-n", "systemctl", "start", "--no-block", VARIA_MANUAL_JOB_UNIT],
        timeout=8,
    )


def _refresh_varia_quote(symbol: str) -> Dict[str, Any]:
    return _run_cmd(
        [_varia_python(), "src/tools/sample_var_spread.py", "--once", "--symbol", symbol,
         "--db", "sqlite:///data/hedge_bot.sqlite3"],
        timeout=150, cwd=_varia_root(),
    )


def _quote_for_symbol(symbol: str) -> Optional[dict]:
    wanted = symbol.upper()
    return next((q for q in _varia_latest_quotes() if q.get("symbol") == wanted), None)


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


def _maker_shadow() -> Dict[str, Any]:
    """Public observer health plus long-running Python/Rust parity metrics."""
    database = DATA_DIR / "maker_shadow.sqlite3"
    observer = _read_json(DATA_DIR / "polymarket_observer_status.json") or {}
    rows_by_venue: Dict[str, dict] = {}
    status_by_venue: Dict[str, dict] = {}
    errors_by_venue: Dict[str, int] = {}

    if database.exists():
        try:
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=2) as connection:
                connection.row_factory = sqlite3.Row
                rows = connection.execute(
                    """
                    SELECT venue, COUNT(*) AS samples, SUM(fresh) AS fresh_samples,
                           SUM(CASE WHEN matched = 0 THEN 1 ELSE 0 END) AS mismatches,
                           SUM(CASE WHEN safety_matched = 0 THEN 1 ELSE 0 END) AS safety_mismatches,
                           SUM(CASE WHEN actions_matched = 0 THEN 1 ELSE 0 END) AS action_mismatches,
                           SUM(CASE WHEN fresh = 1 AND matched = 0 THEN 1 ELSE 0 END) AS fresh_mismatches,
                           MIN(observed_at) AS first_observed_at,
                           MAX(observed_at) AS last_observed_at
                    FROM shadow_samples GROUP BY venue ORDER BY venue
                    """
                ).fetchall()
                statuses = connection.execute(
                    """
                    SELECT venue, last_poll_at, last_new_state_at, last_error
                    FROM shadow_collector_status ORDER BY venue
                    """
                ).fetchall()
                error_rows = connection.execute(
                    "SELECT venue, COUNT(*) AS errors FROM shadow_errors GROUP BY venue"
                ).fetchall()
            rows_by_venue = {str(row["venue"]): dict(row) for row in rows}
            status_by_venue = {str(row["venue"]): dict(row) for row in statuses}
            errors_by_venue = {
                str(row["venue"]): int(row["errors"] or 0) for row in error_rows
            }
        except (OSError, sqlite3.Error):
            rows_by_venue = {}
            status_by_venue = {}
            errors_by_venue = {}

    venues = []
    for venue in ("polymarket", "predictfun"):
        row = rows_by_venue.get(venue, {})
        status = status_by_venue.get(venue, {})
        samples = int(row.get("samples") or 0)
        fresh_samples = int(row.get("fresh_samples") or 0)
        mismatches = int(row.get("mismatches") or 0)
        fresh_mismatches = int(row.get("fresh_mismatches") or 0)
        safety_mismatches = int(row.get("safety_mismatches") or 0)
        action_mismatches = int(row.get("action_mismatches") or 0)
        poll_age = _iso_age(status.get("last_poll_at"))
        last_error = str(status.get("last_error") or "")
        if safety_mismatches or last_error or (poll_age is not None and poll_age > 120):
            tier, label = "danger", "需要检查"
        elif action_mismatches or fresh_mismatches or (poll_age is not None and poll_age > 45):
            tier, label = "warn", "存在差异"
        elif samples and fresh_samples:
            tier, label = "ok", "一致"
        else:
            tier, label = "warn", "等待样本"
        venues.append({
            "venue": venue,
            "label": "Polymarket" if venue == "polymarket" else "Predict.fun",
            "tier": tier,
            "status": label,
            "samples": samples,
            "fresh_samples": fresh_samples,
            "mismatches": mismatches,
            "difference_rate": (mismatches / samples) if samples else None,
            "fresh_difference_rate": (fresh_mismatches / fresh_samples) if fresh_samples else None,
            "safety_mismatches": safety_mismatches,
            "action_mismatches": action_mismatches,
            "errors": errors_by_venue.get(venue, 0),
            "last_error": last_error[:200],
            "last_poll_at": status.get("last_poll_at"),
            "last_poll_age": poll_age,
            "last_new_state_at": status.get("last_new_state_at"),
            "last_observed_at": row.get("last_observed_at"),
        })

    observer_age = _iso_age(observer.get("last_poll_at"))
    observer_summary = observer.get("summary") if isinstance(observer.get("summary"), dict) else {}
    observer_present = bool(observer)
    observer_errors = int(observer_summary.get("errors") or 0)
    configured_markets = int(observer_summary.get("markets") or 0)
    ready_markets = int(observer_summary.get("ready_markets") or 0)
    if not observer_present or observer_age is None or observer_age > 120 or configured_markets <= 0 or not ready_markets:
        observer_tier, observer_label = "danger", "公共行情不可用"
    elif observer_errors or observer_age > 45 or ready_markets < configured_markets:
        observer_tier, observer_label = "warn", "部分行情异常"
    else:
        observer_tier, observer_label = "ok", "公共行情正常"

    markets = []
    for path in sorted(DATA_DIR.glob("polymarket_observer_state_*.json")):
        state = _read_json(path)
        if not isinstance(state, dict):
            continue
        account = int(state.get("account_index") or 0)
        state_age = _iso_age(state.get("ts"))
        state_markets = state.get("markets") if isinstance(state.get("markets"), dict) else {}
        for token_id, market in state_markets.items():
            if not isinstance(market, dict):
                continue
            plan = market.get("reference_plan") if isinstance(market.get("reference_plan"), list) else []
            plan_text = " / ".join(
                f"{item.get('price')} x {item.get('quantity')}"
                for item in plan if isinstance(item, dict)
            )
            token_text = str(token_id)
            if len(token_text) > 18:
                token_text = f"{token_text[:8]}...{token_text[-6:]}"
            markets.append({
                "account": account,
                "token": token_text,
                "name": str(market.get("display_name") or token_text),
                "bid": market.get("best_bid"),
                "ask": market.get("best_ask"),
                "mid": market.get("mid"),
                "reference_plan": plan_text,
                "plan_levels": len(plan),
                "status": market.get("status") or "unknown",
                "state_age": state_age,
                "state_at": state.get("ts"),
                "actual_orders_available": False,
            })

    tiers = [observer_tier] + [row["tier"] for row in venues]
    if "danger" in tiers:
        overall_tier, overall_label = "danger", "需要检查"
    elif "warn" in tiers:
        overall_tier, overall_label = "warn", "正在采样"
    else:
        overall_tier, overall_label = "ok", "观察正常"
    return {
        "present": observer_present or database.exists(),
        "mode": "read_only",
        "overall_tier": overall_tier,
        "overall_label": overall_label,
        "observer": {
            "present": observer_present,
            "tier": observer_tier,
            "status": observer_label,
            "last_poll_at": observer.get("last_poll_at"),
            "last_poll_age": observer_age,
            "accounts": int(observer_summary.get("accounts") or 0),
            "markets": configured_markets,
            "ready_markets": ready_markets,
            "plans": int(observer_summary.get("plans") or 0),
            "errors": observer_errors,
        },
        "venues": venues,
        "markets": markets[:60],
    }


# ---------- 跨机只读拉取(带缓存,拉不到显示离线不阻塞) ----------

_HTTP_CACHE: Dict[str, tuple] = {}


def _persist_account_ops_snapshot(data: dict) -> None:
    """Keep a local last-known-good copy so a slow Windows/Tailnet hop cannot blank the UI."""
    try:
        ACCOUNT_OPS_SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        temp = ACCOUNT_OPS_SNAPSHOT_PATH.with_suffix(
            ACCOUNT_OPS_SNAPSHOT_PATH.suffix + ".tmp"
        )
        temp.write_text(
            json.dumps(data, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temp, ACCOUNT_OPS_SNAPSHOT_PATH)
    except OSError:
        pass


def _account_ops_version(data: Any) -> Optional[float]:
    if not isinstance(data, dict):
        return None
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    raw = str(meta.get("as_of") or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _store_http_cache(url: str, data: Optional[dict]) -> None:
    if url == ACCOUNT_OPS_URL and isinstance(data, dict):
        cached = _HTTP_CACHE.get(url)
        current = cached[0] if cached is not None else None
        if not isinstance(current, dict):
            current = _read_json(ACCOUNT_OPS_SNAPSHOT_PATH)
        current_version = _account_ops_version(current)
        incoming_version = _account_ops_version(data)
        if (
            current_version is not None
            and incoming_version is not None
            and incoming_version < current_version
        ):
            return
    _HTTP_CACHE[url] = (data, time.time())
    if url == ACCOUNT_OPS_URL and isinstance(data, dict):
        _persist_account_ops_snapshot(data)


def _merge_account_ops_cache(section: str, value: Any) -> None:
    """Apply successful writes immediately without forcing a fragile Windows refetch."""
    if not isinstance(value, dict):
        return
    cached = _HTTP_CACHE.get(ACCOUNT_OPS_URL)
    base = cached[0] if cached is not None else _read_json(ACCOUNT_OPS_SNAPSHOT_PATH)
    if not isinstance(base, dict):
        return
    merged = dict(base)
    merged[section] = value
    _store_http_cache(ACCOUNT_OPS_URL, merged)


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
    if url == ACCOUNT_OPS_URL:
        snapshot = _read_json(ACCOUNT_OPS_SNAPSHOT_PATH)
        if isinstance(snapshot, dict):
            _HTTP_CACHE[url] = (snapshot, time.time())
            return snapshot
    data = _do_fetch(url, timeout=2.0)  # 冷启动:短超时同步一次
    _store_http_cache(url, data)
    return data


# Every remote document that can be cached by ``_fetch_json`` must also be
# refreshed here.  IPO_PACK_URL used to be omitted, so the first judgment pack
# read after service startup remained frozen in memory even after a new GPT
# judgment completed.
_PREFETCH_URLS = [
    ACCOUNT_OPS_URL,
    IPO_STATE_URL,
    IPO_PACK_URL,
    MACMINI_STATUS_URL,
    GRID_CONSOLE_URL,
]


def _grid() -> Dict[str, Any]:
    """varxyz-grid 网格系统(独立仓库,本机 :8610 只读控制台)的状态转发。"""
    data = _fetch_json(GRID_CONSOLE_URL, ttl=30.0)
    if not isinstance(data, dict) or "runners" not in data:
        return {"present": False}
    out = dict(data)
    out["present"] = True
    return out


PM_SIGNER_HOSTPORT = os.getenv("PM_SIGNER_HOSTPORT", "100.91.159.54:8420")
VAR_SIGNER_HOSTPORT = os.getenv("VAR_SIGNER_HOSTPORT", "100.91.159.54:8787")


def _probe_tcp(hostport: str) -> bool:
    """只做 TCP 可达性探测，不发应用请求、不读取密钥。"""
    import socket
    host, port = hostport.rsplit(":", 1)
    try:
        with socket.create_connection((host, int(port)), timeout=4):
            return True
    except Exception:
        return False


def _probe_pm_signer() -> bool:
    return _probe_tcp(PM_SIGNER_HOSTPORT)


def _probe_var_signer() -> bool:
    return _probe_tcp(VAR_SIGNER_HOSTPORT)


# ---------- 多 VPS PM 账号(一 VPS 一账号):远程账号路由 ----------
# remote_accounts.json:{"2":{"ssh_host":"ubuntu@100.101.50.40","ssh_key":"/home/ubuntu/.ssh/id_ed25519",
#                            "systemd_unit":"polymarket-engine.service","label":"VPS2"}}
# 账号1在本机(VPS1),账号2在 VPS2;启停按账号路由到对应机器的 systemctl。
REMOTE_ACCOUNTS_PATH = DATA_DIR / "remote_accounts.json"
PM_PEER_DIR = DATA_DIR / "pm_peer"          # rsync 回来的远程账号只读状态
_REMOTE_STATUS: Dict[int, bool] = {}         # idx -> 远程单元 is-active(prefetch 刷新)
REMOTE_REPO_DATA = "/home/ubuntu/polymarket-bot/data"


def _load_pm_remotes() -> Dict[int, dict]:
    d = _read_json(REMOTE_ACCOUNTS_PATH)
    if not isinstance(d, dict):
        return {}
    out: Dict[int, dict] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            try:
                out[int(k)] = v
            except (TypeError, ValueError):
                pass
    return out


def _remote_ssh(remote: dict, cmd: str, timeout: float = 20.0) -> Dict[str, Any]:
    ssh = ["ssh", "-i", str(remote.get("ssh_key", "")), "-o", "BatchMode=yes",
           "-o", "StrictHostKeyChecking=accept-new", "-o", "ConnectTimeout=8",
           str(remote.get("ssh_host", "")), cmd]
    return _run_cmd(ssh, timeout)


def _refresh_pm_remotes() -> None:
    """prefetch 调用:刷新远程账号单元活跃态 + rsync 其只读状态文件到本地 peer 目录。"""
    import subprocess
    remotes = _load_pm_remotes()
    if not remotes:
        return
    PM_PEER_DIR.mkdir(parents=True, exist_ok=True)
    for idx, r in remotes.items():
        unit = r.get("systemd_unit", "polymarket-engine.service")
        act = _remote_ssh(r, f"systemctl is-active {unit}", timeout=12)
        _REMOTE_STATUS[idx] = (act.get("out") == "active")
        for fn in (f"engine_state_{idx}.json", f".engine_{idx}.pid", f".account_{idx}.paused"):
            src = f"{r.get('ssh_host')}:{REMOTE_REPO_DATA}/{fn}"
            try:
                subprocess.run(["scp", "-i", str(r.get("ssh_key", "")), "-o", "BatchMode=yes",
                                "-o", "ConnectTimeout=8", src, str(PM_PEER_DIR / fn)],
                               capture_output=True, timeout=15)
            except Exception:
                pass


def _prefetch_loop() -> None:
    """后台守护线程:每 20s 主动刷新跨机只读源到缓存,使请求路径始终命中热缓存。"""
    while True:
        for url in _PREFETCH_URLS:
            data = _do_fetch(url, timeout=10.0)  # 放宽超时:Windows 源偶尔慢到 5-6s
            if data is None:
                if url == ACCOUNT_OPS_URL:
                    # 账号数据宁可展示本地最后成功快照并标记迟滞，也不因链路抖动遮住内容。
                    prev = _HTTP_CACHE.get(url)
                    if prev is not None and isinstance(prev[0], dict):
                        continue
                    snapshot = _read_json(ACCOUNT_OPS_SNAPSHOT_PATH)
                    if isinstance(snapshot, dict):
                        _HTTP_CACHE[url] = (snapshot, time.time())
                        continue
                # 单次拉取失败别急着翻成"不可达":保留 5 分钟内的上一次好值,
                # 慢源/瞬时抖动不再造成 present=false 闪断(配合告警防抖彻底消除假警)
                prev = _HTTP_CACHE.get(url)
                if prev is not None and prev[0] is not None and (time.time() - prev[1]) < 300:
                    continue
            _store_http_cache(url, data)
        _HTTP_CACHE["pm_signer_up"] = (_probe_pm_signer(), time.time())
        _HTTP_CACHE["var_signer_up"] = (_probe_var_signer(), time.time())
        try:
            _refresh_pm_remotes()
        except Exception:
            pass
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
    broker_ready = sum(1 for a in accounts if isinstance(a, dict) and str(a.get("broker") or "").strip())
    cash_ready = sum(
        1
        for a in accounts
        if isinstance(a, dict)
        and any(a.get(key) not in (None, "") for key in ("availableCash", "available_cash", "available"))
    )
    margin_ready = sum(
        1
        for a in accounts
        if isinstance(a, dict)
        and any(
            (_num(a.get(key)) or 0) > 0
            for key in ("financingLimit", "financing_limit", "marginLimit", "margin_limit")
        )
    )
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
                                  "broker": a.get("broker"),
                                  "status": a.get("status"),
                                  "capital": round(_num(a.get("capital")) or 0.0, 2),
                                  "income": round(_num(a.get("income")) or 0.0, 2),
                                  "available_cash": _num(
                                      a.get("availableCash")
                                      or a.get("available_cash")
                                      or a.get("available")
                                  ),
                                  "financing_limit": _num(
                                      a.get("financingLimit")
                                      or a.get("financing_limit")
                                      or a.get("marginLimit")
                                      or a.get("margin_limit")
                                  )})
    owner_rows = []
    for o in sorted(owners.values(), key=lambda x: -x["capital"])[:8]:
        owner_rows.append({
            **{k: (round(v, 2) if isinstance(v, float) else v) for k, v in o.items()},
            "roi_pct": round(o["income"] / o["capital"] * 100, 2) if o["capital"] else None,
            "wear_pct": round(o["wear"] / o["capital"] * 100, 2) if o["capital"] else None,
            "share": share_by_name.get(o["name"]),
        })
    alpha_accounts_raw = [
        a
        for a in accounts
        if isinstance(a, dict)
        and (
            str(a.get("platform") or "").strip().lower() in {"binance alpha", "alpha", "binance"}
            or str(a.get("id") or "").upper().startswith("BN-")
        )
    ]
    ledger = d.get("ledger") if isinstance(d.get("ledger"), list) else []
    alpha_account_ids = {str(a.get("id") or "") for a in alpha_accounts_raw}
    alpha_rows = []
    for account in alpha_accounts_raw:
        account_id = str(account.get("id") or "")
        account_ledger = [
            row
            for row in ledger
            if isinstance(row, dict) and str(row.get("account") or "") == account_id
        ]
        deposits = 0.0
        withdrawals = 0.0
        rewards = 0.0
        for row in account_ledger:
            amount = _num(row.get("amount")) or 0.0
            row_type = str(row.get("type") or "")
            if row_type.startswith("本金"):
                if amount >= 0:
                    deposits += amount
                else:
                    withdrawals += abs(amount)
            elif row_type.startswith("奖励"):
                rewards += max(0.0, amount)
        account_income = _num(account.get("income")) or 0.0
        account_wear = _num(account.get("wear")) or 0.0
        alpha_rows.append(
            {
                "id": account_id,
                "owner": str(account.get("owner") or ""),
                "status": str(account.get("status") or ""),
                "currency": str(account.get("currency") or "USDT"),
                "capital": round(_num(account.get("capital")) or 0.0, 2),
                "deposits": round(deposits, 2),
                "withdrawals": round(withdrawals, 2),
                "wear": round(account_wear, 2),
                "rewards": round(rewards, 2),
                "profit": round(account_income - rewards, 2),
                "net": round(account_income - account_wear, 2),
            }
        )
    booster_state = (
        d.get("alpha_booster")
        if isinstance(d.get("alpha_booster"), dict)
        else {"tasks": [], "updated_at": ""}
    )
    booster_tasks = [
        task
        for task in booster_state.get("tasks", [])
        if isinstance(task, dict) and not task.get("archived")
    ]
    booster_accounts = [
        item
        for task in booster_tasks
        for item in (task.get("accounts") or [])
        if isinstance(item, dict)
        and (not alpha_account_ids or str(item.get("accountId") or "") in alpha_account_ids)
    ]
    alpha = {
        "accounts": alpha_rows,
        "account_count": len(alpha_rows),
        "capital": round(sum(row["capital"] for row in alpha_rows), 2),
        "wear": round(sum(row["wear"] for row in alpha_rows), 2),
        "rewards": round(sum(row["rewards"] for row in alpha_rows), 2),
        "profit": round(sum(row["profit"] for row in alpha_rows), 2),
        "net": round(sum(row["net"] for row in alpha_rows), 2),
        "tasks": booster_tasks,
        "active_tasks": sum(
            1
            for item in booster_accounts
            if str(item.get("status") or "待完成") in {"待完成", "已完成", "可领取"}
        ),
        "claimable": sum(
            1 for item in booster_accounts if str(item.get("status") or "") == "可领取"
        ),
        "claimed": sum(
            1 for item in booster_accounts if str(item.get("status") or "") == "已领取"
        ),
        "updated_at": str(booster_state.get("updated_at") or ""),
    }
    onboarding_state = (
        d.get("onboarding")
        if isinstance(d.get("onboarding"), dict)
        else {"records": [], "updated_at": ""}
    )
    onboarding_records = [
        item
        for item in onboarding_state.get("records", [])
        if isinstance(item, dict)
    ]
    today = datetime.now().astimezone().date()

    def _days_until(value: Any) -> Optional[int]:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            return (datetime.fromisoformat(raw.replace("Z", "+00:00")).date() - today).days
        except ValueError:
            try:
                return (datetime.strptime(raw[:10], "%Y-%m-%d").date() - today).days
            except ValueError:
                return None

    normalized_onboarding = []
    for item in onboarding_records:
        status = str(item.get("status") or "待准备")
        deadline_days = _days_until(item.get("deadline"))
        reward_days = _days_until(item.get("rewardDue"))
        normalized_onboarding.append(
            {
                "id": str(item.get("id") or ""),
                "person": str(item.get("person") or ""),
                "account_id": str(item.get("accountId") or ""),
                "institution": str(item.get("institution") or ""),
                "institution_type": str(item.get("institutionType") or "券商"),
                "activity_name": str(item.get("activityName") or ""),
                "status": status,
                "opened_at": str(item.get("openedAt") or ""),
                "deadline": str(item.get("deadline") or ""),
                "deadline_days": deadline_days,
                "deposit_amount": _num(item.get("depositAmount")),
                "currency": str(item.get("currency") or "HKD"),
                "hold_days": item.get("holdDays"),
                "reward_value": _num(item.get("rewardValue")),
                "reward_currency": str(item.get("rewardCurrency") or "HKD"),
                "reward_due": str(item.get("rewardDue") or ""),
                "reward_days": reward_days,
                "actual_reward": _num(item.get("actualReward")),
                "funding_path": str(item.get("fundingPath") or ""),
                "source_url": str(item.get("sourceUrl") or ""),
                "notes": str(item.get("notes") or ""),
                "reward_tiers": [
                    tier
                    for tier in item.get("rewardTiers", [])
                    if isinstance(tier, dict)
                ],
                "updated_at": str(item.get("updatedAt") or ""),
            }
        )
    active_onboarding = [
        item
        for item in normalized_onboarding
        if item["status"] not in {"已完成", "失败"}
    ]
    funding_plans = [
        item
        for item in onboarding_state.get("fundingPlans", [])
        if isinstance(item, dict)
    ]
    active_plan_statuses = {"计划中", "待转入", "锁资中", "可释放"}
    capital_batches: Dict[str, Dict[str, Any]] = {}
    for plan in funding_plans:
        if str(plan.get("status") or "计划中") not in active_plan_statuses:
            continue
        amount = _num(plan.get("amount"))
        if not amount:
            continue
        currency = str(plan.get("currency") or "HKD").upper()
        batch_id = str(plan.get("batchId") or "").strip()
        if not batch_id:
            batch_id = "|".join(
                [str(plan.get("person") or ""), currency, f"{amount:.2f}"]
            )
        if batch_id not in capital_batches:
            capital_batches[batch_id] = {
                "id": batch_id,
                "name": str(plan.get("batchName") or ""),
                "person": str(plan.get("person") or ""),
                "amount": amount,
                "currency": currency,
            }
    locked_capital_by_currency: Dict[str, float] = {}
    expected_rewards_by_currency: Dict[str, float] = {}
    if capital_batches:
        for batch in capital_batches.values():
            currency = batch["currency"]
            locked_capital_by_currency[currency] = (
                locked_capital_by_currency.get(currency, 0.0)
                + batch["amount"]
            )
    else:
        for item in normalized_onboarding:
            if item["status"] not in {"待入金", "锁资中", "待交易", "待领奖"}:
                continue
            currency = item["currency"] or "HKD"
            locked_capital_by_currency[currency] = (
                locked_capital_by_currency.get(currency, 0.0)
                + (item["deposit_amount"] or 0.0)
            )
    for item in normalized_onboarding:
        if item["status"] not in {"已完成", "失败"}:
            reward_currency = item["reward_currency"] or "HKD"
            expected_rewards_by_currency[reward_currency] = (
                expected_rewards_by_currency.get(reward_currency, 0.0)
                + (item["reward_value"] or 0.0)
            )
    onboarding = {
        "records": normalized_onboarding,
        "profiles": [
            item
            for item in onboarding_state.get("profiles", [])
            if isinstance(item, dict)
        ],
        "funding_plans": funding_plans,
        "capital_batches": list(capital_batches.values()),
        "updated_at": str(onboarding_state.get("updated_at") or ""),
        "total": len(normalized_onboarding),
        "active": len(active_onboarding),
        "expiring_7d": sum(
            1
            for item in active_onboarding
            if item["deadline_days"] is not None and 0 <= item["deadline_days"] <= 7
        ),
        "overdue": sum(
            1
            for item in active_onboarding
            if item["deadline_days"] is not None and item["deadline_days"] < 0
        ),
        "locked_capital": round(sum(locked_capital_by_currency.values()), 2),
        "locked_capital_by_currency": {
            key: round(value, 2) for key, value in sorted(locked_capital_by_currency.items())
        },
        "pending_rewards": sum(
            1 for item in normalized_onboarding if item["status"] == "待领奖"
        ),
        "expected_rewards": round(
            sum(
                item["reward_value"] or 0.0
                for item in normalized_onboarding
                if item["status"] not in {"已完成", "失败"}
            ),
            2,
        ),
        "expected_rewards_by_currency": {
            key: round(value, 2) for key, value in sorted(expected_rewards_by_currency.items())
        },
        "people": sorted(
            {
                str(person.get("name") or "").strip()
                for person in (d.get("people") or [])
                if isinstance(person, dict) and str(person.get("name") or "").strip()
            }
            | {
                str(account.get("owner") or "").strip()
                for account in accounts
                if isinstance(account, dict) and str(account.get("owner") or "").strip()
            }
        ),
        "accounts": [
            {
                "id": str(account.get("id") or ""),
                "owner": str(account.get("owner") or ""),
                "platform": str(account.get("platform") or ""),
                "broker": str(account.get("broker") or ""),
            }
            for account in accounts
            if isinstance(account, dict) and str(account.get("id") or "")
        ],
    }
    return {
        "owners": owner_rows,
        "alpha": alpha,
        "onboarding": onboarding,
        "present": True,
        "accounts": len(accounts),
        "people": len(d.get("people") or []),
        "broker_ready": broker_ready,
        "cash_ready": cash_ready,
        "margin_ready": margin_ready,
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
        "age_sec": _iso_age(meta.get("as_of")),
    }


def _parse_ipo_datetime(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw or raw in {"待确认", "—"}:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
        return parsed
    except ValueError:
        pass
    normalized = (
        raw.replace(" noon ", " PM ")
        .replace(" a.m. ", " AM ")
        .replace(" p.m. ", " PM ")
        .replace("a.m.", "AM")
        .replace("p.m.", "PM")
    )
    for pattern in (
        "%I:%M %p on %A, %d %B %Y",
        "%I:%M %p on %A, %B %d, %Y",
        "%A, %d %B %Y",
        "%A, %B %d, %Y",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            parsed = datetime.strptime(normalized, pattern)
            return parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
        except ValueError:
            continue
    return None


def _ipo_stock_is_open(stock: dict, now: Optional[datetime] = None) -> bool:
    code = str(stock.get("code") or "").strip().upper()
    if not code or code.startswith(("IPO-", "STR-", "SIM")):
        return False
    if str(stock.get("status") or "").strip() not in {"申购中", "招股中", "待申购"}:
        return False
    current = now or datetime.now().astimezone()
    close_at = _parse_ipo_datetime(stock.get("closeAt") or stock.get("deadlineAt"))
    if close_at is not None and close_at <= current:
        return False
    listing_at = _parse_ipo_datetime(stock.get("listingAt"))
    if listing_at is not None and listing_at <= current:
        return False
    return True


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
        name_zh = str(
            s.get("nameZh") or s.get("name_zh") or s.get("chineseName") or ""
        ).strip()
        name_en = str(s.get("nameEn") or s.get("name_en") or "").strip()
        name = str(s.get("name") or s.get("title") or s.get("code") or "").strip()
        status = str(s.get("status") or "")[:10]
        # 真字段修正(2026-07-15):router 原始数据里入场费=minCapital、评分=expectedScore,
        # 此前读的 fee/score 键不存在故全 null。一并带出状态/日期/评分拆解/招股书链接。
        return {"name": (name_zh or name or name_en)[:40],
                "name_zh": name_zh[:40],
                "name_en": name_en[:72],
                "code": str(s.get("code") or ""),
                "score": s.get("expectedScore") if s.get("expectedScore") is not None else s.get("score"),
                "hit_rate": s.get("hitRateScore"),
                "turnover": s.get("turnoverScore"),
                "data_completeness": s.get("dataCompletenessScore"),
                "capital_efficiency": s.get("capitalEfficiencyScore"),
                "fee": _num(s.get("minCapital")) or _num(s.get("fee")) or _num(s.get("entryFee")),
                "risk": str(s.get("risk") or s.get("riskLabel") or "")[:12],
                "status": status,
                "close_at": str(s.get("closeAt") or s.get("deadlineAt") or "")[:16],
                "listing_at": str(s.get("listingAt") or "")[:16],
                "refund_days": s.get("refundDays"),
                "prospectus": str(s.get("prospectusUrl") or "")[:200],
                "note": str(s.get("note") or s.get("view") or s.get("summary") or "")[:60]}

    def _entry(e: dict) -> dict:
        return {"account": str(e.get("account") or e.get("accountId") or "")[:14],
                "person": str(e.get("person") or e.get("owner") or "")[:10],
                "stock": str(e.get("stockName") or e.get("stock") or e.get("stockCode")
                             or e.get("code") or e.get("suggestion") or "")[:20],
                "fee": e.get("fee") or e.get("entryFee"),
                "due": str(e.get("due") or e.get("lockUntil") or e.get("deadline") or "")[:12],
                "status": str(e.get("status") or "")[:14],
                "broker": str(e.get("broker") or "")[:20],
                "method": str(e.get("method") or "")[:8],
                "financing_cost": _num(e.get("financingCost") if e.get("financingCost") is not None else e.get("financing_cost")) or 0,
                "fee_rule_version": str(e.get("feeRuleVersion") or e.get("fee_rule_version") or "")[:40],
                "strategy_source": str(e.get("strategySource") or e.get("strategy_source") or "")[:16],
                "strategy_override": bool(e.get("strategyOverride") or e.get("strategy_override")),
                "suggested_action": str(e.get("suggestedAction") or e.get("suggested_action") or "")[:40],
                "suggested_method": str(e.get("suggestedMethod") or e.get("suggested_method") or "")[:12],
                "suggested_reason": str(e.get("suggestedReason") or e.get("suggested_reason") or "")[:140],
                "suggestion_verdict": str(e.get("suggestionVerdict") or e.get("suggestion_verdict") or "")[:12],
                "suggestion_score": e.get("suggestionScore") if e.get("suggestionScore") is not None else e.get("suggestion_score"),
                "available_capital": _num(e.get("availableCapital") if e.get("availableCapital") is not None else e.get("available_capital")) or 0,
                "required_capital": _num(e.get("requiredCapital") if e.get("requiredCapital") is not None else e.get("required_capital")) or 0,
                "trade_pnl": _num(e.get("tradePnl") if e.get("tradePnl") is not None else e.get("trade_pnl")) or 0,
                "net_pnl": _num(e.get("netPnl") if e.get("netPnl") is not None else e.get("net_pnl")) or 0,
                "settlement_note": str(e.get("settlementNote") or e.get("settlement_note") or "")[:80],
                "settled_at": str(e.get("settledAt") or e.get("settled_at") or "")[:24],
                "reason": str(e.get("reason") or e.get("explain") or e.get("note") or "")[:40]}

    stock_rows = [
        _stock(s)
        for s in stocks
        if isinstance(s, dict) and _ipo_stock_is_open(s)
    ][:20]
    # AI 判研由 Windows OpenClaw/GPT 写入 router judgment-pack,按代码贴到确定性事实旁。
    pack = _fetch_json(IPO_PACK_URL, ttl=120.0)
    judged_at = None
    if isinstance(pack, dict):
        judged_at = pack.get("judged_at")
        jmap = {str(s.get("code")): s for s in (pack.get("stocks") or []) if isinstance(s, dict)}
        for row in stock_rows:
            j = jmap.get(row["code"])
            if j and j.get("verdict"):
                row["ai_verdict"] = j.get("verdict")          # 打 / 跳 / 观望
                row["ai_grade"] = str(j.get("grade") or "")[:1]
                row["ai_expected"] = j.get("expected_net")    # 期望净收益
                row["ai_reason"] = str(j.get("reason") or "")[:80]
                row["ai_score"] = j.get("overall_score")
                row["ai_confidence"] = j.get("confidence")
                row["ai_scores"] = j.get("score_breakdown") if isinstance(j.get("score_breakdown"), dict) else {}
                row["ai_sources"] = j.get("sources") if isinstance(j.get("sources"), list) else []
                row["ai_gaps"] = j.get("evidence_gaps") if isinstance(j.get("evidence_gaps"), list) else []
    # 上游 status 偶尔没有随招股截止更新。AI 明确判为“已过期”时，不能继续在
    # “当前可申购”里展示，避免同一行同时出现“申购中 / 已过期”的自相矛盾。
    stock_rows = [row for row in stock_rows if "已过期" not in str(row.get("ai_verdict") or "")]
    active_n = len(stock_rows)
    return {
        "present": True, "mode": inner.get("mode"),
        "round": {"title": rnd.get("title"), "code": rnd.get("code"),
                  "deadline": rnd.get("deadline"), "currency": rnd.get("currency")},
        "updated_age": _age_text(_iso_age(inner.get("updated_at"))),
        "stocks": stock_rows,
        "entries": [_entry(e) for e in entries[:50] if isinstance(e, dict)],
        "round_strategy": inner.get("round_strategy") if isinstance(inner.get("round_strategy"), dict) else {},
        "stocks_total": active_n, "entries_total": len(entries),
        "active_stocks": active_n,
        "ai_judged_at": judged_at,
        "ai_judged_age": _age_text(_iso_age(judged_at)) if judged_at else None,
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


def _varia_budget(net_by_host: Dict[str, float]) -> Dict[str, Any]:
    """Weekly available budget = base cap + settled net result.

    Settled venue incentives are part of the net result. Pending or estimated
    rewards never increase the available budget.
    """
    cap = _budget_cap_for_host()
    if cap is None:
        return {"present": False}
    hosts = {}
    total_remaining = total_cap = 0.0
    for host, net_result in sorted(net_by_host.items()):
        rem = max(0.0, cap + net_result)
        hosts[host] = {
            "cap": cap,
            "net_result_week": round(net_result, 2),
            "remaining": round(rem, 2),
        }
        total_remaining += rem
        total_cap += cap
    return {"present": True, "per_vps": True, "hosts": hosts,
            "cap_each": cap,
            "total_cap": round(total_cap, 2), "total_remaining": round(total_remaining, 2),
            "basis": "friday_0800_settled_net_including_incentives"}


def _macmini() -> Dict[str, Any]:
    d = _fetch_json(MACMINI_STATUS_URL, ttl=30.0)
    if not isinstance(d, dict):
        return {"present": False}
    services = d.get("services") if isinstance(d.get("services"), dict) else {}
    out = {"present": True, "age": _age_text(max(0, int(time.time() - (_num(d.get("ts")) or 0))))}
    for label, key in (("ai.codex.var-decibel-signer", "var_signer"),
                       ("ai.codex.predictfun-api-proxy", "pf_proxy"),
                       ("ai.codex.var-decibel-chrome-health", "chrome_health"),
                       # PM 签名器常驻化后(ai.codex.polymarket-signer)导出器一上报这里就接住
                       ("ai.codex.polymarket-signer", "pm_signer")):
        svc = services.get(label) if isinstance(services.get(label), dict) else {}
        out[key] = {"running": bool(svc.get("running")), "last_exit": svc.get("last_exit")}
    for cache_key, key in (("pm_signer_up", "pm_signer"),
                           ("var_signer_up", "var_signer")):
        cached = _HTTP_CACHE.get(cache_key)
        out[key]["reachable"] = cached[0] if cached is not None else None
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


IPO_IMPORT_SUCCESS_STAMP = Path(
    os.getenv("IPO_IMPORT_SUCCESS_STAMP", "/home/ubuntu/ipo_import.success")
)


def _tier(age: Optional[int]) -> str:
    if age is None:
        return "unknown"
    if age < 900:
        return "ok"
    if age < 86400:
        return "warn"
    return "danger"


def _freshness(vd: Dict[str, Any], ao: Dict[str, Any]) -> Dict[str, Any]:
    """每页核心源年龄徽章:🟢 <15m / 🟡 迟滞 / 🔴 停摆(附停摆日期)。纯只读。"""
    from datetime import timedelta
    out: Dict[str, Any] = {}

    def entry(key: str, age: Optional[int]) -> None:
        t = _tier(age)
        if age is None:
            lbl = "无数据"
        elif t == "danger":
            stop = (datetime.now(timezone.utc) - timedelta(seconds=age)).astimezone()
            lbl = f"停摆 · 停于 {stop.strftime('%m-%d')}"
        else:
            lbl = _age_text(age) or ""
        out[key] = {"age": age, "tier": t, "label": lbl}

    pm_p = DATA_DIR / "engine_state_1.json"
    entry("pm", _mtime_age(pm_p if pm_p.exists() else DATA_DIR / "engine_state.json"))
    hosts = [h.get("age_sec") for h in (vd.get("hosts") or {}).values() if h.get("age_sec") is not None]
    entry("vardec", min(hosts) if hosts else None)
    # dry-run 每轮更新 state/desired_orders;execution_report 只有 executor 写,不能当心跳
    pf_ages = [a for a in (_mtime_age(DATA_DIR / n) for n in
                           ("predictfun_mainnet_state.json", "predictfun_mainnet_desired_orders.json",
                            "predictfun_mainnet_execution_report.json")) if a is not None]
    entry("pf", min(pf_ages) if pf_ages else None)
    entry("sa", _mtime_age(DATA_DIR / "single_account_paper_state.json"))
    entry("hk", ao.get("age_sec") if ao.get("present") else None)
    return out


def _alerts(vd: Dict[str, Any], pm: Dict[str, Any], sa: Dict[str, Any],
            ao: Dict[str, Any], mm: Dict[str, Any], fresh: Dict[str, Any],
            grid: Optional[Dict[str, Any]] = None) -> List[dict]:
    alerts: List[dict] = []
    # 网格(varxyz-grid):KILL/停机/失联/agent 到期
    g = grid or {}
    if g.get("present"):
        if g.get("kill"):
            alerts.append({"tag": "GRID", "msg": "<b>网格 KILL 急停已触发</b>:引擎全撤停机", "page": "grid", "sev": "crit"})
        for r in g.get("runners") or []:
            if r.get("halted"):
                alerts.append({"tag": "GRID", "msg": f"<b>网格 {r.get('coin')} 停机</b>:{r.get('halt_reason') or '未知原因'}", "page": "grid", "sev": "crit"})
            elif not r.get("running"):
                alerts.append({"tag": "GRID", "msg": f"<b>网格 {r.get('coin')} 失联</b>:state 心跳 {r.get('state_age_s')}s 未更新", "page": "grid", "sev": "warn"})
            if r.get("frozen"):
                alerts.append({"tag": "GRID", "msg": f"<b>网格 {r.get('coin')} 冻结格线 {r['frozen']}</b>:挂单无故消失,待人工 resume", "page": "grid", "sev": "warn"})
        for a in (g.get("account") or {}).get("agents") or []:
            if isinstance(a.get("days_left"), (int, float)) and a["days_left"] < 30:
                alerts.append({"tag": "GRID", "msg": f"<b>HL agent「{a.get('name')}」{a['days_left']:.0f} 天后到期</b>:需在 app.hyperliquid.xyz/API 续签", "page": "grid", "sev": "warn"})
    for item in vd.get("single_leg") or []:
        alerts.append({"tag": "VAR/DEC", "msg": f"<b>{item} 单腿</b>:双腿不对称,janitor 应在处置", "page": "vardec", "sev": "crit"})
    for host, h in (vd.get("hosts") or {}).items():
        if h.get("age_sec") is not None and h["age_sec"] > STALE_SEC:
            alerts.append({"tag": "VAR/DEC", "msg": f"<b>{host.upper()} ops 心跳过期</b>:{_age_text(h['age_sec'])}", "page": "vardec", "sev": "warn"})
    if pm.get("cooldown"):
        alerts.append({"tag": "PM", "msg": "<b>Polymarket 冷却中</b>:kill-switch/冷却激活,暂停开新单", "page": "pm", "sev": "warn"})
    # 补全(2026-07-08):account-ops 迟滞 / IPO 导入超期 / macmini 失联 / 权益曲线断更 / 预算告急 / SA worker 停
    if not ao.get("present"):
        alerts.append({"tag": "HK/US", "msg": "<b>account-ops 数据源不可达</b>(Windows :8081)", "page": "hk", "sev": "warn"})
    elif ao.get("age_sec") is not None and ao["age_sec"] > 1800:
        alerts.append({"tag": "HK/US", "msg": f"<b>account-ops 数据迟滞</b>:{_age_text(ao['age_sec'])} 未更新(>30m)", "page": "hk", "sev": "warn"})
    # Only a verified successful import updates this stamp.  A silent curl timeout must
    # remain visible as a failure instead of looking like a cron job that never existed.
    imp_age = _mtime_age(IPO_IMPORT_SUCCESS_STAMP)
    if imp_age is not None and imp_age > 26 * 3600:
        alerts.append({"tag": "IPO", "msg": f"<b>每日新股导入超期</b>:{_age_text(imp_age)} 未跑(cron 每日 01:00,错过一轮即报)", "page": "hk", "sev": "warn"})
    if not mm.get("present"):
        alerts.append({"tag": "INFRA", "msg": "<b>mac-mini 状态导出器失联</b>(:8620;影响 signer/pf-proxy 可见性)", "page": "vardec", "sev": "warn"})
    eq = vd.get("equity_history") or {}
    auto_active = bool((vd.get("auto") or {}).get("enabled"))
    exposure_active = bool(vd.get("pairs"))
    if eq.get("present") and (auto_active or exposure_active):
        try:
            last_d = datetime.fromisoformat(str(eq["points"][-1]["t"]))
            if (datetime.now() - last_d).total_seconds() > 2 * 86400:
                alerts.append({"tag": "VAR/DEC", "msg": f"<b>权益曲线断更</b>:最后一点 {eq['points'][-1]['t']}", "page": "vardec", "sev": "warn"})
        except Exception:
            pass
    for host, h in ((vd.get("budget") or {}).get("hosts") or {}).items():
        cap, rem = _num(h.get("cap")) or 0.0, _num(h.get("remaining")) or 0.0
        if cap and rem < max(1.0, 0.2 * cap):
            alerts.append({"tag": "VAR/DEC", "msg": f"<b>{host} 周预算告急</b>:剩 ${rem:.2f} / ${cap:.2f}", "page": "vardec", "sev": "crit"})
    if sa.get("present") and not sa.get("worker_running"):
        alerts.append({"tag": "SA", "msg": "<b>SA paper worker 未运行</b>(sa-paper-worker.service)", "page": "sa", "sev": "warn"})
    # PM 引擎停摆不再作为推送告警:启停是用户主动控制的状态,dashboard 徽章已显示,
    # 常态是"故意没开",当故障天天报纯噪音。真崩溃需告警可在实盘后加"曾运行→掉线"探测。
    # PM 签名器不可达只在"引擎在跑却连不上签名器"时才是真问题;引擎没跑时签名器闲置正常。
    signer_cached = _HTTP_CACHE.get("pm_signer_up")
    if signer_cached is not None and signer_cached[0] is False and (pm.get("engine_ctl") or {}).get("active"):
        alerts.append({"tag": "PM", "msg": f"<b>PM 签名器不可达</b>(mac-mini :{PM_SIGNER_HOSTPORT.rsplit(':', 1)[1]})"
                                           ":引擎在跑却连不上签名器,急停撤单不可用", "page": "pm", "sev": "crit"})
    return alerts


@app.get("/api/state")
def api_state() -> JSONResponse:
    pm = _polymarket()
    pm["engine_ctl"] = _engine_ctl()
    pm["pnl"] = _pm_pnl()
    _accs = pm.get("accounts") or []
    pm["engine_summary"] = {"running": sum(1 for a in _accs if a.get("status") == "运行中"),
                            "total": len(_accs)}
    vd = _var_decibel()
    sa = _single_account()
    ao = _account_ops()
    mm = _macmini()
    fresh = _freshness(vd, ao)
    grid = _grid()
    return JSONResponse({
        "ts": datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M"),
        "polymarket": pm,
        "predictfun": _predictfun(),
        "var_decibel": vd,
        "single_account": sa,
        "recorders": _recorders(),
        "account_ops": ao,
        "ipo": _ipo(),
        "pf_intents": _pf_intents(),
        "varia_detail": _varia_detail(),
        "varia_control": _varia_control_state(vd),
        "varia_automation": _varia_automation_state(vd),
        "pm_detail": _pm_detail(),
        "maker_shadow": _maker_shadow(),
        "macmini": mm,
        "freshness": fresh,
        "events": _events(pm.pop("fill_events", [])),
        "grid": grid,
        "alerts": _alerts(vd, pm, sa, ao, mm, fresh, grid),
        "writes_enabled": WRITES_ENABLED,
    })


# ---------- 受控写:varia 周预算(默认关闭,LATITUDE_ENABLE_WRITES=1 启用) ----------
# 写路径与 varia dashboard 自身控件完全一致:改 VPS1 的 auto_strategy_state.json,
# 由 varia 既有的 state 同步机制传播到 VPS2。带备份 + 审计日志 + 范围校验。

WRITES_ENABLED = os.getenv("LATITUDE_ENABLE_WRITES", "0") == "1"
AUDIT_LOG = DATA_DIR / "console_write_audit.jsonl"

# pmbot 操作迁移(自 /alpha/ Streamlit,写路径一一对应 dashboard/app.py):
REPO_ROOT = DATA_DIR.parent
MAKER_DIR = REPO_ROOT / "platforms" / "polymarket" / "maker"
ENGINE_UNIT = "polymarket-engine.service"
CANCEL_CLI = MAKER_DIR / "cancel_all_cli.py"
SA_CONFIG = REPO_ROOT / "platforms" / "single_account" / "config.json"
SA_PID = DATA_DIR / ".single_account_paper.pid"
SA_LOG = DATA_DIR / "single_account_paper_worker.log"


def _audit(action: str, **fields: Any) -> None:
    with AUDIT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                             "action": action, **fields}, ensure_ascii=False) + "\n")


def _is_cloudflare(request: Request) -> bool:
    """公网 Cloudflare 入口(nginx 注 X-Dashboard-Source 头)。与 dashboard/app.py::
    _is_public_access 同一规则:配置类写降级只读,进程控制类放行(Kevin 2026-04-24)。"""
    return str(request.headers.get("X-Dashboard-Source", "")).lower() == "cloudflare"


def _run_cmd(
    cmd: List[str], timeout: float, cwd: Optional[Path] = None,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    import subprocess
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                           cwd=str(cwd) if cwd else None, env=env)
        return {"rc": r.returncode, "out": (r.stdout or "").strip()[:400],
                "err": (r.stderr or "").strip()[:200]}
    except subprocess.TimeoutExpired:
        return {"rc": -1, "out": "", "err": "timeout"}
    except Exception as e:
        return {"rc": -1, "out": "", "err": f"{type(e).__name__}: {str(e)[:80]}"}


def _fail_varia_job(job_id: int, error: str) -> None:
    path = VARIA_DIR / "hedge_bot.sqlite3"
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")
    try:
        with sqlite3.connect(path, timeout=5) as conn:
            conn.execute(
                "UPDATE dashboard_jobs SET status='failed', updated_at=?, finished_at=?, "
                "error_message=? WHERE id=? AND status='queued'",
                (now, now, error[:500], job_id),
            )
    except Exception:
        pass


def _varia_write_guard(request: Request) -> Optional[JSONResponse]:
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    if _is_cloudflare(request):
        return JSONResponse(
            {"ok": False, "error": "真实交易只允许通过 Tailscale 内网页面操作"},
            status_code=403,
        )
    return None


@app.get("/api/varia/control")
def varia_control_get() -> JSONResponse:
    return JSONResponse(_varia_control_state())


@app.post("/api/varia/control/refresh")
async def varia_control_refresh(payload: dict, request: Request) -> JSONResponse:
    blocked = _varia_write_guard(request)
    if blocked is not None:
        return blocked
    symbol = str((payload or {}).get("symbol") or "").strip().upper()
    if not symbol or len(symbol) > 16 or not symbol.replace("-", "").isalnum():
        return JSONResponse({"ok": False, "error": "交易对格式不正确"}, status_code=400)
    result = _refresh_varia_quote(symbol)
    quote = _quote_for_symbol(symbol)
    ok = result.get("rc") == 0 and bool(quote) and (quote.get("age_sec") or 10**9) <= 120
    return JSONResponse({
        "ok": ok, "quote": quote, "state": _varia_control_state(),
        "error": None if ok else (result.get("err") or result.get("out") or "双边报价刷新失败"),
    }, status_code=200 if ok else 502)


@app.post("/api/varia/control/open")
async def varia_control_open(payload: dict, request: Request) -> JSONResponse:
    blocked = _varia_write_guard(request)
    if blocked is not None:
        return blocked
    data = payload or {}
    host = str(data.get("host") or "").lower()
    symbol = str(data.get("symbol") or "").strip().upper()
    direction = str(data.get("direction") or "auto").lower()
    leverage = _num(data.get("leverage"))
    notional = _num(data.get("notional"))
    take_profit = _num(data.get("take_profit")) if data.get("take_profit") not in (None, "") else None
    stop_loss = _num(data.get("stop_loss")) if data.get("stop_loss") not in (None, "") else None
    if host not in {"vps1", "vps2"}:
        return JSONResponse({"ok": False, "error": "请选择 VPS1 或 VPS2"}, status_code=400)
    if _host_hedge_venue(host) != "decibel":
        return JSONResponse({
            "ok": False,
            "error": "VPS2 Var/Ondo 手动开仓尚未完成独立报价与确认验收，未提交订单",
        }, status_code=409)
    auto_state = _normalize_varia_auto_state(_read_json(_varia_auto_state_file()))
    auto_host = auto_state.get("hosts", {}).get(host, {})
    if (auto_state.get("enabled") and auto_host.get("enabled")
            and _varia_worker_status(host) in {"active", "activating"}):
        return JSONResponse({
            "ok": False,
            "error": f"{host.upper()} 自动运行中；请先停止自动化，再进行手动开仓",
        }, status_code=409)
    if not symbol or len(symbol) > 16 or not symbol.replace("-", "").isalnum():
        return JSONResponse({"ok": False, "error": "交易对格式不正确"}, status_code=400)
    if direction not in {"auto", "var_buy", "var_sell"}:
        return JSONResponse({"ok": False, "error": "方向参数不正确"}, status_code=400)
    if leverage is None or not (1 <= leverage <= 40):
        return JSONResponse({"ok": False, "error": "杠杆须为 1–40 倍"}, status_code=400)
    if notional is None or not (5 <= notional <= 10000):
        return JSONResponse({"ok": False, "error": "名义金额须为 5–10000 USDC"}, status_code=400)
    if any(value is not None and value <= 0 for value in (take_profit, stop_loss)):
        return JSONResponse({"ok": False, "error": "止盈止损价格必须大于 0"}, status_code=400)

    refresh_result = _refresh_varia_quote(symbol)
    quote = _quote_for_symbol(symbol)
    if refresh_result.get("rc") != 0 or not quote or (quote.get("age_sec") or 10**9) > 120:
        return JSONResponse({
            "ok": False,
            "error": refresh_result.get("err") or refresh_result.get("out") or "没有最新双边报价，未提交订单",
        }, status_code=409)
    chosen = quote.get("recommended") if direction == "auto" else direction
    if chosen not in {"var_buy", "var_sell"}:
        return JSONResponse({"ok": False, "error": "无法计算低成本方向，未提交订单"}, status_code=409)
    var_side = "buy" if chosen == "var_buy" else "sell"
    reference_price = _num(quote.get("var_ask" if chosen == "var_buy" else "var_bid"))
    if reference_price is None or reference_price <= 0:
        return JSONResponse({"ok": False, "error": "Var 报价不可用，未提交订单"}, status_code=409)
    quantity = notional / reference_price
    command = _varia_live_command(
        host=host, symbol=symbol, var_side=var_side, quantity=quantity,
        leverage=leverage, notional=notional, take_profit=take_profit, stop_loss=stop_loss,
    )
    spec = {"mode": "single", "commands": [{
        "command": command, "host": host, "symbol": symbol,
        "planned_var_side": var_side, "planned_quantity": f"{quantity:.10f}",
    }]}
    job_payload = {
        "host": host, "symbol": symbol, "var_side": var_side,
        "direction": chosen, "leverage": leverage, "notional": notional,
        "take_profit": take_profit, "stop_loss": stop_loss,
        "quote_timestamp": quote.get("timestamp"), "estimated_cost_bps": (quote.get("costs") or {}).get(chosen),
    }
    try:
        job_id = _enqueue_varia_job(kind="manual_live", command=spec, payload=job_payload)
    except RuntimeError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"任务写入失败：{type(exc).__name__}"}, status_code=500)
    started = _start_varia_manual_worker()
    if started.get("rc") != 0:
        _fail_varia_job(job_id, started.get("err") or "一次性 worker 启动失败")
        return JSONResponse({"ok": False, "error": "任务未启动，未执行订单"}, status_code=500)
    _audit("varia_manual_open", job_id=job_id, host=host, symbol=symbol,
           direction=chosen, notional=notional, leverage=leverage, source="tailnet")
    return JSONResponse({
        "ok": True, "job_id": job_id, "note": f"开仓任务 #{job_id} 已提交，正在后台执行",
        "direction": chosen, "quantity": quantity, "costs": quote.get("costs"),
    })


@app.post("/api/varia/control/close-all")
async def varia_control_close_all(request: Request) -> JSONResponse:
    blocked = _varia_write_guard(request)
    if blocked is not None:
        return blocked
    commands, safety_blocks = _varia_close_commands()
    if safety_blocks:
        return JSONResponse({
            "ok": False, "error": "；".join(safety_blocks) + "。为避免误平，未提交任务。"
        }, status_code=409)
    if not commands:
        return JSONResponse({"ok": False, "error": "当前没有已核验的双腿仓位"}, status_code=409)
    payload = {"host": "all", "command_count": len(commands),
               "symbols": [f"{c['host']}:{c['symbol']}" for c in commands]}
    try:
        job_id = _enqueue_varia_job(
            kind="close_all", command={"mode": "close_all", "commands": commands}, payload=payload,
        )
    except RuntimeError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=409)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"任务写入失败：{type(exc).__name__}"}, status_code=500)
    started = _start_varia_manual_worker()
    if started.get("rc") != 0:
        _fail_varia_job(job_id, started.get("err") or "一次性 worker 启动失败")
        return JSONResponse({"ok": False, "error": "任务未启动，未执行平仓"}, status_code=500)
    _audit("varia_close_all", job_id=job_id, commands=len(commands), source="tailnet")
    return JSONResponse({"ok": True, "job_id": job_id,
                         "note": f"一键平仓任务 #{job_id} 已提交，正在后台执行"})


def _engine_ctl() -> Dict[str, Any]:
    r = _run_cmd(["systemctl", "is-active", ENGINE_UNIT], timeout=5)
    return {"unit": ENGINE_UNIT, "active": r["out"] == "active", "state": r["out"] or r["err"]}


def _cancel_all_orders() -> Dict[str, Any]:
    """EMERGENCY STOP 第一步:REST 撤销全部挂单(cancel_all_cli 镜像 dashboard 同一
    signer 路径;引擎 SIGTERM 时自身也会撤单,此为双保险)。best-effort,不阻塞停机。"""
    import sys as _sys
    r = _run_cmd([_sys.executable, str(CANCEL_CLI)], timeout=90)
    try:
        return {"rc": r["rc"], **json.loads(r["out"])}
    except Exception:
        return {"rc": r["rc"], "raw": r["out"], "err": r["err"]}


def _configured_pm_accounts() -> List[int]:
    """已配置的 PM 本地账户 idx(config_N.json 存在)。"""
    return [i for i in range(1, 31) if (MAKER_DIR / f"config_{i}.json").exists()]


def _pm_all_accounts() -> List[int]:
    """全部 PM 账户 = 本地已配置 ∪ 远程(remote_accounts.json)。"""
    return sorted(set(_configured_pm_accounts()) | set(_load_pm_remotes().keys()))


def _cancel_account_orders(idx: int) -> Dict[str, Any]:
    """撤单只针对账号 idx(cancel_all_cli --account N)。在 VPS1 上跑即可——VPS1 有全部
    config + 签名器白名单,能派生任一账户 creds 去 REST 撤单,与引擎在哪台机无关。"""
    import sys as _sys
    r = _run_cmd([_sys.executable, str(CANCEL_CLI), "--account", str(idx)], timeout=60)
    try:
        return {"rc": r["rc"], **json.loads(r["out"])}
    except Exception:
        return {"rc": r["rc"], "raw": r["out"], "err": r["err"]}


@app.post("/api/pm/engine")
async def pm_engine(payload: dict, request: Request) -> JSONResponse:
    """按账号路由的进程控制(一 VPS 一账号):每个账号 = 对应机器上一个
    polymarket-engine.service。start/stop/reload 带 accounts=[idx…] 对选中账号逐个
    在其所属机器 systemctl(本地账号本地跑,远程账号 SSH 到对应 VPS);省略 accounts=全部。
    stop/reload 先按账号 REST 撤单(只撤该账号)再停;省略 accounts 的 stop = EMERGENCY(停全部)。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    action = str((payload or {}).get("action") or "")
    if action not in ("start", "stop", "reload"):
        return JSONResponse({"ok": False, "error": "action 须为 start/stop/reload"}, status_code=400)
    if not (payload or {}).get("confirm"):
        return JSONResponse({"ok": False, "error": "缺少 confirm 确认标记"}, status_code=400)
    remotes = _load_pm_remotes()
    all_acc = _pm_all_accounts()
    req_acc = (payload or {}).get("accounts")
    if isinstance(req_acc, list):
        try:
            targets = [int(x) for x in req_acc]
        except (TypeError, ValueError):
            return JSONResponse({"ok": False, "error": "accounts 须为账户号列表"}, status_code=400)
    else:
        targets = all_acc
    targets = [i for i in targets if i in all_acc]
    if not targets:
        return JSONResponse({"ok": False, "error": "无有效账户(检查是否已配置)"}, status_code=400)

    per: Dict[int, Any] = {}
    for idx in targets:
        is_remote = idx in remotes
        r = remotes.get(idx, {})
        unit = r.get("systemd_unit", "polymarket-engine.service") if is_remote else ENGINE_UNIT
        entry: Dict[str, Any] = {"host": (r.get("label") or "远程") if is_remote else "VPS1",
                                 "remote": is_remote}
        if action in ("stop", "reload"):
            entry["cancel"] = _cancel_account_orders(idx)
            entry["stop"] = (_remote_ssh(r, f"sudo -n systemctl stop {unit}", timeout=25) if is_remote
                             else _run_cmd(["sudo", "-n", "systemctl", "stop", unit], timeout=25))
        if action == "reload":
            time.sleep(1)
        if action in ("start", "reload"):
            entry["start"] = (_remote_ssh(r, f"sudo -n systemctl start {unit}", timeout=20) if is_remote
                              else _run_cmd(["sudo", "-n", "systemctl", "start", unit], timeout=20))
        per[idx] = entry

    ok = True
    for v in per.values():
        for k in ("start", "stop"):
            if k in v and v[k].get("rc") != 0:
                ok = False
    _audit("pm_engine", request_action=action, targets=targets, per=per,
           source="cloudflare" if _is_cloudflare(request) else "tailnet")
    note = "、".join(f"账号{i}@{per[i]['host']}" for i in targets) + f" 已{action}"
    return JSONResponse({"ok": ok, "note": note, "per": per,
                         "error": None if ok else json.dumps(per, ensure_ascii=False)[:300]})


@app.post("/api/pm/precheck")
async def pm_precheck(request: Request) -> JSONResponse:
    """启动预检(只读探测,不撤单不下单):
    ① signer TCP 可达 ② 每账号 derive-creds 可签(cancel_all_cli --check)
    ③ markets 配置新鲜度。全绿才建议 Start —— 6/8 盲启失败(signer 500)的疫苗。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    import sys as _sys
    checks: List[dict] = []
    signer_up = _probe_pm_signer()
    checks.append({"name": f"签名器 TCP({PM_SIGNER_HOSTPORT})", "ok": signer_up,
                   "note": "可达" if signer_up else "不可达 — 先修 mac-mini 上的 PM signer"})
    if signer_up:
        remotes = _load_pm_remotes()
        for idx in _pm_all_accounts():
            if idx in remotes:
                # 远程账号:在其所属 VPS 上验签(用远程 venv),更贴近真实运行环境
                rc = remotes[idx]
                py = rc.get("python", "/home/ubuntu/.venv2/bin/python")
                cmd = (f"cd /home/ubuntu/polymarket-bot && {py} "
                       f"platforms/polymarket/maker/cancel_all_cli.py --check --account {idx}")
                r = _remote_ssh(rc, cmd, timeout=40)
                host = rc.get("label") or "远程"
            else:
                r = _run_cmd([_sys.executable, str(CANCEL_CLI), "--check", "--account", str(idx)], timeout=60)
                host = "VPS1"
            try:
                res = json.loads(r["out"]).get("results", [])
            except Exception:
                res = []
            if res:
                a = res[0]
                checks.append({"name": f"账号 {idx}@{host} derive-creds", "ok": a["status"] == "ok",
                               "note": a.get("note") or a["status"]})
            else:
                checks.append({"name": f"账号 {idx}@{host} derive-creds", "ok": False,
                               "note": (r.get("err") or "无输出")[:80]})
    cfg = _pm_cfg()
    n_day, n_night = len(cfg.get("markets") or []), len(cfg.get("night_markets") or [])
    cfg_age = _mtime_age(PM_CONFIG)
    cfg_fresh = cfg_age is not None and cfg_age < 7 * 86400
    checks.append({"name": f"markets 配置(日 {n_day} · 夜 {n_night})", "ok": bool(n_day) and cfg_fresh,
                   "note": ("最后修改 " + (_age_text(cfg_age) or "?")) +
                           ("" if cfg_fresh else " — 陈旧,先 Run Scan 应用新市场")})
    all_ok = all(c["ok"] for c in checks)
    _audit("pm_precheck", ok=all_ok, checks=checks,
           source="cloudflare" if _is_cloudflare(request) else "tailnet")
    return JSONResponse({"ok": all_ok, "checks": checks,
                         "note": "全绿,可以 Start" if all_ok else "有红项,先处理再启动"})


@app.post("/api/pm/account")
async def pm_account(payload: dict, request: Request) -> JSONResponse:
    """账号级软暂停/恢复:touch/unlink data/.account_N.paused,引擎在跑时自行撤单停报价。
    与 dashboard/app.py::_set_account_paused 同一旗标文件。进程控制类,Cloudflare 放行。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    idx = (payload or {}).get("idx")
    action = str((payload or {}).get("action") or "")
    if not isinstance(idx, int) or not (1 <= idx <= 30) or action not in ("pause", "resume"):
        return JSONResponse({"ok": False, "error": "需 idx 1-30 且 action pause/resume"}, status_code=400)
    remotes = _load_pm_remotes()
    if idx not in remotes and not (MAKER_DIR / f"config_{idx}.json").exists():
        return JSONResponse({"ok": False, "error": f"账号 {idx} 未配置"}, status_code=404)
    if idx in remotes:
        # 远程账号:旗标在其所属 VPS 上,SSH touch/unlink(引擎在那台机上读它)
        r = remotes[idx]
        fpath = f"{REMOTE_REPO_DATA}/.account_{idx}.paused"
        cmd = f"touch {fpath}" if action == "pause" else f"rm -f {fpath}"
        res = _remote_ssh(r, cmd, timeout=15)
        paused_now = action == "pause" and res.get("rc") == 0
        _audit("pm_account", idx=idx, request_action=action, remote=True, rc=res.get("rc"),
               source="cloudflare" if _is_cloudflare(request) else "tailnet")
        return JSONResponse({"ok": res.get("rc") == 0, "idx": idx, "paused": paused_now,
                             "host": r.get("label") or "远程"})
    flag = DATA_DIR / f".account_{idx}.paused"
    if action == "pause":
        flag.touch(exist_ok=True)
    else:
        try:
            flag.unlink()
        except FileNotFoundError:
            pass
    _audit("pm_account", idx=idx, request_action=action,
           source="cloudflare" if _is_cloudflare(request) else "tailnet")
    return JSONResponse({"ok": True, "idx": idx, "paused": flag.exists(), "host": "VPS1"})


@app.post("/api/sa/paper")
async def sa_paper(payload: dict, request: Request) -> JSONResponse:
    """SA paper worker 控制(纯 paper,不下单):once/start/stop。
    与 dashboard/app.py::_run/_start/_stop_single_account_paper_worker 同一命令、
    同一 pid/log 文件。进程控制类,Cloudflare 放行。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    import subprocess
    import sys as _sys
    action = str((payload or {}).get("action") or "")
    if action not in ("once", "start", "stop"):
        return JSONResponse({"ok": False, "error": "action 须为 once/start/stop"}, status_code=400)
    pid = _sa_worker_pid()
    result: Dict[str, Any]
    if action == "once":
        r = _run_cmd([_sys.executable, "-m", "platforms.single_account.paper_worker",
                      "--config", str(SA_CONFIG), "--once"], timeout=60, cwd=REPO_ROOT)
        result = {"ok": r["rc"] == 0,
                  "msg": "paper worker 已完成一次评分;未下单。" if r["rc"] == 0 else (r["err"] or r["out"] or "失败")}
    elif action == "start":
        # systemd 单元跑法:不再 Popen(dashboard/console 重启会 cgroup 连坐杀掉子进程,
        # 7/1 停摆即此因;sa-paper-worker.service 独立于两者存活)
        if pid:
            result = {"ok": True, "msg": f"paper worker 已在运行,pid={pid}"}
        else:
            r = _run_cmd(["sudo", "-n", "systemctl", "start", "sa-paper-worker.service"], timeout=20)
            result = {"ok": r["rc"] == 0,
                      "msg": "paper worker 已启动(systemd)" if r["rc"] == 0 else (r["err"] or "启动失败")}
    else:
        legacy_pid_file = SA_PID.exists()
        r = _run_cmd(["sudo", "-n", "systemctl", "stop", "sa-paper-worker.service"], timeout=20)
        if legacy_pid_file and pid:  # dashboard 遗留 Popen 进程,单元停了也要补刀
            import signal as _signal
            try:
                os.kill(pid, _signal.SIGTERM)
            except Exception:
                pass
        SA_PID.unlink(missing_ok=True)
        result = {"ok": r["rc"] == 0,
                  "msg": "paper worker 已停止" if r["rc"] == 0 else (r["err"] or "停止失败")}
    _audit("sa_paper", request_action=action, ok=result["ok"], msg=result["msg"],
           source="cloudflare" if _is_cloudflare(request) else "tailnet")
    return JSONResponse({**result, "worker_pid": _sa_worker_pid()})


# ---------- 打新 & Alpha:代理 router 打新操作(原生内嵌,不跳转不走飞书 Bot) ----------
# router(Windows :8080)是操作真源;控制台受控写端点代理到它,带闸门+审计。
# 关键安全:router 的 /ipo/action 是"提交完整状态+动作"模式——精简 payload 会冲空排班。
# 故 set_status/set_mode/finish_round 等一律走"拉当前状态→改一处→回传完整状态"。
IPO_ROUTER_BASE = os.getenv("IPO_ROUTER_BASE", "http://100.82.86.62:8080")


def _http_post_json(url: str, body: dict, timeout: float = 20.0) -> Dict[str, Any]:
    import urllib.error
    import urllib.request
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req = urllib.request.Request(url, data=json.dumps(body).encode("utf-8"),
                                     headers={"Content-Type": "application/json"}, method="POST")
        with opener.open(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            try:
                return {"ok": True, "status": resp.status, "data": json.loads(raw)}
            except Exception:
                return {"ok": True, "status": resp.status, "data": raw[:300]}
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            detail = payload.get("detail") or payload.get("error")
        except Exception:
            detail = None
        return {"ok": False, "status": exc.code, "error": str(detail or exc.reason)}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:160]}"}


def _http_post_bytes(
    url: str,
    body: bytes,
    *,
    headers: Optional[dict] = None,
    timeout: float = 240.0,
) -> Dict[str, Any]:
    import urllib.error
    import urllib.request

    request_headers = {"Content-Type": "application/pdf", **(headers or {})}
    try:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req = urllib.request.Request(url, data=body, headers=request_headers, method="POST")
        with opener.open(req, timeout=timeout) as resp:
            return {"ok": True, "status": resp.status, "data": json.loads(resp.read().decode("utf-8"))}
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8"))
            detail = payload.get("detail") or payload.get("error")
        except Exception:
            detail = None
        return {"ok": False, "status": exc.code, "error": str(detail or exc.reason)}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:160]}"}


def _ipo_current_state() -> Optional[dict]:
    """拉 router 当前打新状态(内层 ipo dict),回传式操作的基底。
    Windows 源偶尔慢:先较长超时新拉;失败回退到 prefetch 热缓存(~20s 新鲜,IPO 手动操作
    频率低,可接受),两者皆空才中止(绝不空提交)。"""
    d = _do_fetch(f"{IPO_ROUTER_BASE}/dashboard/ipo/state", timeout=15.0)
    if not isinstance(d, dict):
        cached = _HTTP_CACHE.get(IPO_STATE_URL)
        d = cached[0] if cached and isinstance(cached[0], dict) else None
    if not isinstance(d, dict):
        return None
    return d.get("ipo") if isinstance(d.get("ipo"), dict) else d


@app.post("/api/ipo/import")
async def ipo_import(payload: dict, request: Request) -> JSONResponse:
    """立即导入新股(HKEX)——独立端点、无状态、最安全。等价日 cron 的 on-demand 版。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    body = {"include_pdf_details": bool((payload or {}).get("include_pdf_details", True))}
    r = _http_post_json(f"{IPO_ROUTER_BASE}/dashboard/ipo/import/hkex", body, timeout=240.0)
    _audit("ipo_import", ok=r.get("ok"), body=body,
           source="cloudflare" if _is_cloudflare(request) else "tailnet")
    if not r.get("ok"):
        return JSONResponse({"ok": False, "error": r.get("error")}, status_code=502)
    data = r.get("data") if isinstance(r.get("data"), dict) else {}
    n = len((data.get("ipo") or data).get("stocks") or []) if isinstance(data, dict) else 0
    return JSONResponse({"ok": True, "msg": f"HKEX 新股已导入(新股池 {n} 只)", "stocks": n})


@app.post("/api/ipo/judgment")
async def ipo_judgment(payload: dict, request: Request) -> JSONResponse:
    """让 Windows OpenClaw 使用固定的 OpenAI 模型生成当前轮次判研包。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    state = _ipo_current_state()
    if state is None:
        return JSONResponse({"ok": False, "error": "当前新股状态不可达"}, status_code=502)
    active_stocks = [
        item
        for item in state.get("stocks", [])
        if isinstance(item, dict) and _ipo_stock_is_open(item)
    ]
    if not active_stocks:
        return JSONResponse(
            {"ok": False, "error": "当前没有申购中的真实新股，无需运行 GPT 判研"},
            status_code=400,
        )
    body = {
        "research_text": str((payload or {}).get("research_text") or "")[:48000],
        "stocks": active_stocks,
    }
    r = _http_post_json(
        f"{IPO_ROUTER_BASE}/dashboard/ipo/openclaw/judgment",
        body,
        timeout=160.0,
    )
    _audit(
        "ipo_judgment",
        ok=r.get("ok"),
        research_chars=len(body["research_text"]),
        source="cloudflare" if _is_cloudflare(request) else "tailnet",
    )
    if not r.get("ok"):
        return JSONResponse({"ok": False, "error": r.get("error")}, status_code=502)
    data = r.get("data") if isinstance(r.get("data"), dict) else {}
    judgment = data.get("judgment") if isinstance(data.get("judgment"), dict) else {}
    _HTTP_CACHE.pop(IPO_PACK_URL, None)
    _HTTP_CACHE.pop(IPO_STATE_URL, None)
    return JSONResponse(
        {
            "ok": True,
            "msg": f"GPT 判研已更新({len(judgment.get('stocks') or [])} 只)",
            "judged_at": judgment.get("judged_at"),
        }
    )


@app.post("/api/ipo/research-pdf")
async def ipo_research_pdf(request: Request) -> JSONResponse:
    """Proxy a PDF to Windows where selected pages are rendered for GPT vision."""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    content_length = request.headers.get("content-length")
    try:
        declared_size = int(content_length) if content_length else 0
    except ValueError:
        declared_size = 0
    max_bytes = 20 * 1024 * 1024
    if declared_size > max_bytes:
        return JSONResponse({"ok": False, "error": "单个 PDF 不能超过 20MB"}, status_code=413)

    pdf_bytes = await request.body()
    if len(pdf_bytes) > max_bytes:
        return JSONResponse({"ok": False, "error": "单个 PDF 不能超过 20MB"}, status_code=413)
    if not pdf_bytes:
        return JSONResponse({"ok": False, "error": "没有收到 PDF 文件"}, status_code=400)

    filename = str(request.headers.get("x-file-name") or "研究材料.pdf")[:240]
    if not filename.lower().endswith(".pdf"):
        return JSONResponse({"ok": False, "error": "只支持 PDF 文件"}, status_code=415)
    page_range = str(request.headers.get("x-page-range") or "").strip()[:120]
    forwarded = _http_post_bytes(
        f"{IPO_ROUTER_BASE}/dashboard/ipo/research/pdf-vision",
        pdf_bytes,
        headers={"X-File-Name": filename, "X-Page-Range": page_range},
        timeout=240.0,
    )
    if not forwarded.get("ok"):
        return JSONResponse(
            {"ok": False, "error": forwarded.get("error") or "PDF 视觉分析失败"},
            status_code=int(forwarded.get("status") or 502),
        )
    result = forwarded.get("data") if isinstance(forwarded.get("data"), dict) else {}

    _audit(
        "ipo_research_pdf",
        ok=True,
        filename=filename,
        bytes=len(pdf_bytes),
        pages=result.get("pages"),
        mode="page_images",
        source="cloudflare" if _is_cloudflare(request) else "tailnet",
    )
    return JSONResponse(
        {
            "ok": True,
            "filename": result.get("filename") or filename,
            "text": str(result.get("visual_summary") or ""),
            "pages": result.get("pages") or [],
            "total_pages": result.get("total_pages"),
            "model": result.get("model"),
            "mode": "page_images",
        }
    )


@app.post("/api/ipo/action")
async def ipo_action(payload: dict, request: Request) -> JSONResponse:
    """打新操作:set_mode / set_status / subscribe_active / subscribe_all / finish_round。
    回传式安全:拉当前完整状态 → 按 action 改动一处 → 整体回传 router,绝不冲空排班。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    action = str((payload or {}).get("action") or "")
    allowed = {"set_mode", "set_status", "set_strategy", "apply_round_strategy", "settle_result", "subscribe_active", "subscribe_all", "finish_round"}
    if action not in allowed:
        return JSONResponse({"ok": False, "error": f"action 须为 {sorted(allowed)}"}, status_code=400)
    st = _ipo_current_state()
    if st is None:
        return JSONResponse({"ok": False, "error": "拉取 router 当前打新状态失败,已中止(不冒险空提交)"}, status_code=502)
    mode = st.get("mode")
    round_ = st.get("round") or {}
    stocks = st.get("stocks") or []
    entries = [dict(e) for e in (st.get("entries") or []) if isinstance(e, dict)]
    settlements = [dict(e) for e in (st.get("settlements") or []) if isinstance(e, dict)]

    detail = ""
    if action == "set_mode":
        new_mode = str((payload or {}).get("mode") or "")
        if new_mode not in ("conservative", "balanced", "aggressive"):
            return JSONResponse({"ok": False, "error": "mode 须为 conservative/balanced/aggressive"}, status_code=400)
        mode = new_mode
        detail = f"策略模式 → {new_mode}"
    elif action == "set_status":
        acc = str((payload or {}).get("account_id") or "")
        status = str((payload or {}).get("status") or "")
        if not acc or not status:
            return JSONResponse({"ok": False, "error": "set_status 需 account_id + status"}, status_code=400)
        hit = False
        for e in entries:
            if str(e.get("accountId")) == acc:
                e["status"] = status
                hit = True
        if not hit:
            return JSONResponse({"ok": False, "error": f"排班里没有账号 {acc}"}, status_code=404)
        detail = f"{acc} 状态 → {status}"
    elif action == "set_strategy":
        acc = str((payload or {}).get("account_id") or "")
        method = str((payload or {}).get("method") or "")
        if not acc or method not in ("现金", "融资"):
            return JSONResponse({"ok": False, "error": "设置策略需要账号及现金/融资方式"}, status_code=400)
        if not any(str(e.get("accountId")) == acc for e in entries):
            return JSONResponse({"ok": False, "error": f"排班里没有账号 {acc}"}, status_code=404)
        detail = f"{acc} 策略已锁定：{payload.get('broker') or '未填券商'} · {method}"
    elif action == "apply_round_strategy":
        strategy = (payload or {}).get("strategy") if isinstance((payload or {}).get("strategy"), dict) else {}
        method = str(strategy.get("method") or "自动")
        if method not in ("自动", "现金", "融资"):
            return JSONResponse({"ok": False, "error": "统一策略须为自动、现金或融资"}, status_code=400)
        eligible = [e for e in entries if e.get("status") not in ("中签", "未中签", "跳过", "未申购", "已卖出")]
        overrides = [e for e in eligible if e.get("strategyOverride")]
        applied = len(eligible) if (payload or {}).get("force") else len(eligible) - len(overrides)
        detail = f"统一方案已应用到 {applied} 个账号"
        if overrides and not (payload or {}).get("force"):
            detail += f"；保留 {len(overrides)} 个人工调整"
    elif action == "settle_result":
        acc = str((payload or {}).get("account_id") or "")
        status = str((payload or {}).get("status") or "")
        if not acc or status not in ("中签", "未中签"):
            return JSONResponse({"ok": False, "error": "结算需要账号及中签/未中签结果"}, status_code=400)
        if not any(str(e.get("accountId")) == acc for e in entries):
            return JSONResponse({"ok": False, "error": f"排班里没有账号 {acc}"}, status_code=404)
        try:
            float((payload or {}).get("trade_pnl") or 0)
            float((payload or {}).get("financing_cost") or 0)
        except (TypeError, ValueError):
            return JSONResponse({"ok": False, "error": "盈亏和融资成本必须是数字"}, status_code=400)
        detail = f"{acc} 已结算：{status}"
    elif action in ("subscribe_active", "subscribe_all"):
        target_all = action == "subscribe_all"
        n = 0
        for e in entries:
            if e.get("status") == "跳过" and not target_all:
                continue
            if e.get("status") in (None, "", "待申购") or target_all:
                if e.get("status") != "跳过" or target_all:
                    e["status"] = "已申购"
                    n += 1
        detail = f"标记{'全部' if target_all else '活跃'}为已申购({n} 个)"
    elif action == "finish_round":
        detail = "结束本轮(router 生成手续费流水)"

    body = {
        "action": action, "mode": mode, "round": round_, "stocks": stocks,
        "entries": entries, "settlements": settlements,
        "account_id": (payload or {}).get("account_id"),
        "status": (payload or {}).get("status"),
        "broker": (payload or {}).get("broker") or "",
        "method": (payload or {}).get("method") or "",
        "financing_cost": (payload or {}).get("financing_cost"),
        "fee_rule_version": (payload or {}).get("fee_rule_version") or "",
        "trade_pnl": (payload or {}).get("trade_pnl"),
        "settlement_note": (payload or {}).get("settlement_note") or "",
        "strategy": (payload or {}).get("strategy") or {},
        "force": bool((payload or {}).get("force")),
    }
    r = _http_post_json(f"{IPO_ROUTER_BASE}/dashboard/ipo/action", body, timeout=40.0)
    _audit("ipo_action", request_action=action, detail=detail, ok=r.get("ok"),
           source="cloudflare" if _is_cloudflare(request) else "tailnet")
    if not r.get("ok"):
        return JSONResponse({"ok": False, "error": r.get("error")}, status_code=502)
    return JSONResponse({"ok": True, "msg": detail or (action + " 已提交")})


@app.post("/api/alpha/action")
async def alpha_action(payload: dict, request: Request) -> JSONResponse:
    """Persist Binance Alpha Booster task progress through the account router."""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    action = str((payload or {}).get("action") or "")
    if action not in {"add_task", "set_status", "archive_task"}:
        return JSONResponse({"ok": False, "error": "不支持的 Alpha 操作"}, status_code=400)
    body = dict(payload or {})
    body["action"] = action
    r = _http_post_json(
        f"{IPO_ROUTER_BASE}/dashboard/alpha/action",
        body,
        timeout=40.0,
    )
    _audit(
        "alpha_action",
        request_action=action,
        task_id=str(body.get("task_id") or ""),
        account_id=str(body.get("account_id") or ""),
        ok=r.get("ok"),
        source="cloudflare" if _is_cloudflare(request) else "tailnet",
    )
    if not r.get("ok"):
        return JSONResponse({"ok": False, "error": r.get("error")}, status_code=502)
    _merge_account_ops_cache("alpha_booster", r.get("alpha"))
    detail = {
        "add_task": "Booster 任务已保存",
        "set_status": "Booster 状态已更新",
        "archive_task": "Booster 任务已归档",
    }[action]
    return JSONResponse({"ok": True, "msg": detail})


@app.post("/api/onboarding/action")
async def onboarding_action(payload: dict, request: Request) -> JSONResponse:
    """Persist broker/bank onboarding, funding and reward progress through the router."""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    action = str((payload or {}).get("action") or "")
    if action not in {
        "upsert_record",
        "set_status",
        "delete_record",
        "set_requirement_progress",
        "upsert_profile",
        "delete_profile",
        "upsert_funding_plan",
        "delete_funding_plan",
    }:
        return JSONResponse({"ok": False, "error": "不支持的开户操作"}, status_code=400)
    body = dict(payload or {})
    body["action"] = action
    r = _http_post_json(
        f"{IPO_ROUTER_BASE}/dashboard/onboarding/action",
        body,
        timeout=40.0,
    )
    _audit(
        "onboarding_action",
        request_action=action,
        record_id=str(body.get("record_id") or ""),
        institution=str(body.get("institution") or ""),
        ok=r.get("ok"),
        source="cloudflare" if _is_cloudflare(request) else "tailnet",
    )
    if not r.get("ok"):
        return JSONResponse({"ok": False, "error": r.get("error")}, status_code=502)
    _merge_account_ops_cache("onboarding", r.get("onboarding"))
    detail = {
        "upsert_record": "开户记录已保存",
        "set_status": "开户进度已更新",
        "delete_record": "开户记录已删除",
        "set_requirement_progress": "奖励条件进度已更新",
        "upsert_profile": "账号档案已保存",
        "delete_profile": "账号档案已删除",
        "upsert_funding_plan": "资金排期已保存",
        "delete_funding_plan": "资金排期已删除",
    }[action]
    return JSONResponse({"ok": True, "msg": detail})


# ---------- 二期:Scan 后台任务 / 市场配置应用 / 代理池 / SA 草稿 ----------

PM_CONFIG = MAKER_DIR / "config.json"
SCANNER = MAKER_DIR / "scanner.py"
SA_DRAFT_PATH = DATA_DIR / "single_account_automation_draft.json"
_SCAN_JOB: Dict[str, Any] = {"status": "idle"}  # idle|running|done|error


def _pm_cfg() -> dict:
    return _read_json(PM_CONFIG) or {}


def _cfg_token_sides(cfg: dict, key: str) -> Dict[str, str]:
    return {str(m.get("token_id")): str(m.get("side", "YES"))
            for m in (cfg.get(key) or []) if isinstance(m, dict) and m.get("token_id")}


def _backup_pm_config(cfg: dict) -> str:
    backup = PM_CONFIG.with_suffix(f".json.bak-{datetime.now().strftime('%Y%m%d%H%M%S')}")
    backup.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    return backup.name


@app.get("/api/pm/scan")
def pm_scan_get() -> JSONResponse:
    cfg = _pm_cfg()
    out = {k: v for k, v in _SCAN_JOB.items()}
    out["config_day"] = _cfg_token_sides(cfg, "markets")
    out["config_night"] = _cfg_token_sides(cfg, "night_markets")
    out["proxy_count"] = len((cfg.get("proxy_pool") or {}).get("proxies") or [])
    return JSONResponse(out)


@app.post("/api/pm/scan")
async def pm_scan_run(payload: dict, request: Request) -> JSONResponse:
    """跑 scanner.py --json(与 /alpha/ Run Scan 同一脚本同一参数;只查公开行情,60-120s)。
    后台线程执行,前端轮询 GET /api/pm/scan。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    if _SCAN_JOB.get("status") == "running":
        return JSONResponse({"ok": False, "error": "已有 scan 在跑"}, status_code=409)
    sd = (_pm_cfg().get("dashboard") or {}).get("scan_defaults") or {}
    p = payload or {}

    def _n(key: str, default: float) -> float:
        v = _num(p.get(key))
        return v if v is not None else (_num(sd.get(key)) if _num(sd.get(key)) is not None else default)

    sort_by = str(p.get("sort_by") or sd.get("sort_by") or "reward")[:32]
    args = [str(SCANNER),
            "--min-volume", str(int(_n("min_volume", 0))),
            "--min-reward", str(int(_n("min_reward", 0))),
            "--max-reward", str(int(_n("max_reward", 0))),
            "--min-spread", str(_n("min_spread", 0)),
            "--max-spread", str(_n("max_spread", 0)),
            "--min-bid-depth", str(int(_n("min_bid_depth", 0))),
            "--sort-by", sort_by,
            "--top", str(int(_n("top", 50))),
            "--json"]
    _SCAN_JOB.clear()
    _SCAN_JOB.update({"status": "running", "started_at": datetime.now(timezone.utc).isoformat(),
                      "params": {a: b for a, b in zip(args[1::2], args[2::2])}})
    _audit("pm_scan", params=_SCAN_JOB["params"],
           source="cloudflare" if _is_cloudflare(request) else "tailnet")

    # scan 结果可能超出 _run_cmd 的 stdout 截断上限 → 线程内直接 subprocess 拿完整输出
    def _worker_full() -> None:
        import subprocess
        import sys as _sys
        try:
            r = subprocess.run([_sys.executable] + args, capture_output=True, text=True,
                               timeout=300, cwd=str(MAKER_DIR))
            json_line = next((ln.strip() for ln in (r.stdout or "").splitlines()
                              if ln.strip().startswith("[")), "")
            if r.returncode != 0 or not json_line:
                _SCAN_JOB.update({"status": "error",
                                  "error": (r.stderr or "")[-300:] or "no output"})
                return
            results = json.loads(json_line)
            _SCAN_JOB.update({"status": "done", "count": len(results), "results": results,
                              "finished_at": datetime.now(timezone.utc).isoformat()})
        except Exception as e:
            _SCAN_JOB.update({"status": "error", "error": f"{type(e).__name__}: {str(e)[:120]}"})

    import threading
    threading.Thread(target=_worker_full, name="pm-scan", daemon=True).start()
    return JSONResponse({"ok": True, "status": "running"})


@app.post("/api/pm/markets/apply")
async def pm_markets_apply(payload: dict, request: Request) -> JSONResponse:
    """应用选择(日盘+夜盘):与 /alpha/「应用选择」同一写路径 —— 服务器端按 scan 结果
    重建 entry(参数模板逐字段一致),写 config.json 并全量同步 config_1..30 的
    markets/night_markets。配置类写:Cloudflare 入口只读。引擎需 Stop+Start 才读新列表。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    if _is_cloudflare(request):
        return JSONResponse({"ok": False, "error": "公网入口只读:配置类写请走 Tailscale 内网"}, status_code=403)
    if _SCAN_JOB.get("status") != "done" or not _SCAN_JOB.get("results"):
        return JSONResponse({"ok": False, "error": "无 scan 结果:先 Run Scan"}, status_code=400)
    day_ids = {str(t) for t in (payload or {}).get("day_tokens") or []}
    night_ids = {str(t) for t in (payload or {}).get("night_tokens") or []}
    cfg = _pm_cfg()
    if not cfg:
        return JSONResponse({"ok": False, "error": "config.json 不可读"}, status_code=500)
    existing_day = _cfg_token_sides(cfg, "markets")
    existing_night = _cfg_token_sides(cfg, "night_markets")

    def _detect_side(item: dict) -> str:
        no_tid = str(item.get("paired_token_id") or "")
        if no_tid and (no_tid in existing_day or no_tid in existing_night):
            return "NO"
        return existing_day.get(str(item.get("token_id") or ""), "YES")

    new_markets: List[dict] = []
    checked_night: List[dict] = []
    for item in _SCAN_JOB["results"]:
        if not isinstance(item, dict):
            continue
        yes_tid = str(item.get("token_id") or "")
        no_tid = str(item.get("paired_token_id") or "")
        if not yes_tid:
            continue
        side = _detect_side(item)
        tid = no_tid if side == "NO" and no_tid else yes_tid
        paired = no_tid if side == "YES" else yes_tid
        if yes_tid in day_ids:
            entry = {"token_id": tid, "side": side,
                     "max_incentive_spread": round(_num(item.get("maxIncentiveSpread")) or 3.5, 4),
                     "price_tick": 0.01, "min_distance_from_best_bid": 0.01,
                     "quote_size": 100.0,
                     "risk": "low" if str(item.get("quadrant", "")).startswith("A") else "mid",
                     "enabled": True}
            if paired:
                entry["paired_token_id"] = paired
            new_markets.append(entry)
        if yes_tid in night_ids:
            entry = {"token_id": tid, "side": side,
                     "max_incentive_spread": round(_num(item.get("maxIncentiveSpread")) or 3.5, 4),
                     "price_tick": 0.01, "min_distance_from_best_bid": 0.02,
                     "quote_size": 80.0, "risk": "low", "enabled": True}
            if paired:
                entry["paired_token_id"] = paired
            checked_night.append(entry)

    backup = _backup_pm_config(cfg)
    cfg["markets"] = new_markets
    cfg["night_markets"] = checked_night
    tmp = PM_CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, PM_CONFIG)

    synced = ["config.json"]
    failed: List[str] = []
    for i in range(1, 31):
        mp = MAKER_DIR / f"config_{i}.json"
        if not mp.exists():
            continue
        try:
            mc = json.loads(mp.read_text(encoding="utf-8"))
            mc["markets"] = new_markets
            mc["night_markets"] = checked_night
            mp.write_text(json.dumps(mc, ensure_ascii=False, indent=2), encoding="utf-8")
            synced.append(mp.name)
        except Exception as e:
            failed.append(f"{mp.name}({str(e)[:60]})")
    # 远程账号(别的 VPS)的 config 也要推过去,否则它读不到新 markets
    import subprocess
    pushed: List[str] = []
    for idx, r in _load_pm_remotes().items():
        local_cfg = MAKER_DIR / f"config_{idx}.json"
        if not local_cfg.exists():
            continue
        dst = f"{r.get('ssh_host')}:/home/ubuntu/polymarket-bot/platforms/polymarket/maker/config_{idx}.json"
        try:
            pr = subprocess.run(["scp", "-i", str(r.get("ssh_key", "")), "-o", "BatchMode=yes",
                                 "-o", "ConnectTimeout=8", str(local_cfg), dst],
                                capture_output=True, text=True, timeout=20)
            (pushed if pr.returncode == 0 else failed).append(
                f"→{r.get('label') or idx}:config_{idx}" if pr.returncode == 0
                else f"push config_{idx} 失败({(pr.stderr or '')[:40]})")
        except Exception as e:
            failed.append(f"push config_{idx}({str(e)[:40]})")
    _audit("pm_markets_apply", day=len(new_markets), night=len(checked_night),
           synced=synced, pushed=pushed, failed=failed, backup=backup, source="tailnet")
    return JSONResponse({"ok": not failed, "day": len(new_markets), "night": len(checked_night),
                         "synced": synced, "pushed": pushed, "failed": failed,
                         "note": "engine 需 Stop+Start 重启才读新 markets;远程账号 config 已推送对应 VPS"})


@app.post("/api/pm/proxies")
async def pm_proxies(payload: dict, request: Request) -> JSONResponse:
    """代理池 Append/Replace:与 /alpha/ Proxy tab 同一写路径(config.json proxy_pool.proxies)。
    配置类写:Cloudflare 只读。审计只记条数,不落代理凭据。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    if _is_cloudflare(request):
        return JSONResponse({"ok": False, "error": "公网入口只读:配置类写请走 Tailscale 内网"}, status_code=403)
    mode = str((payload or {}).get("mode") or "")
    lines = (payload or {}).get("proxies")
    if mode not in ("append", "replace") or not isinstance(lines, list):
        return JSONResponse({"ok": False, "error": "需 mode append/replace + proxies 列表"}, status_code=400)
    new_proxies = [str(x).strip() for x in lines if str(x).strip()]
    if len(new_proxies) > 500 or any(len(x) > 200 for x in new_proxies):
        return JSONResponse({"ok": False, "error": "条数≤500,单条≤200字符"}, status_code=400)
    cfg = _pm_cfg()
    if not cfg:
        return JSONResponse({"ok": False, "error": "config.json 不可读"}, status_code=500)
    pool = cfg.get("proxy_pool") or {}
    cur = [str(x) for x in (pool.get("proxies") or [])]
    if mode == "append":
        added = [p for p in new_proxies if p not in set(cur)]
        cur.extend(added)
        result = {"added": len(added), "skipped": len(new_proxies) - len(added), "total": len(cur)}
    else:
        cur = new_proxies
        result = {"replaced": len(cur), "total": len(cur)}
    backup = _backup_pm_config(cfg)
    pool["proxies"] = cur
    cfg["proxy_pool"] = pool
    tmp = PM_CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, PM_CONFIG)
    _audit("pm_proxies", mode=mode, counts=result, backup=backup, source="tailnet")
    return JSONResponse({"ok": True, **result})


@app.post("/api/sa/draft")
async def sa_draft(payload: dict, request: Request) -> JSONResponse:
    """SA 自动化草稿保存:与 /alpha/「保存草稿」同一文件同一字段(不启动 worker,不下单)。
    配置类写:Cloudflare 只读。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    if _is_cloudflare(request):
        return JSONResponse({"ok": False, "error": "公网入口只读:配置类写请走 Tailscale 内网"}, status_code=403)
    from datetime import timedelta
    p = payload or {}
    ranges = {"weekly_loss_cap_usdc": (0, 10000), "max_open_positions": (1, 10),
              "max_leverage": (1, 50), "max_notional_usdc": (0, 10000),
              "default_stop_loss_pct": (0.1, 50), "default_take_profit_pct": (0.1, 100)}
    draft: Dict[str, Any] = {}
    for k, (lo, hi) in ranges.items():
        v = _num(p.get(k))
        if v is None or not (lo <= v <= hi):
            return JSONResponse({"ok": False, "error": f"{k} 需在 {lo}–{hi}"}, status_code=400)
        draft[k] = v
    draft["mode"] = str(p.get("mode") or "paper")[:24]
    draft["host"] = str(p.get("host") or "")[:64]
    strategies = p.get("enabled_strategies")
    draft["enabled_strategies"] = [str(s)[:64] for s in strategies][:8] if isinstance(strategies, list) else []
    draft["notes"] = str(p.get("notes") or "")[:2000]
    bjt = timezone(timedelta(hours=8))
    draft["updated_at_bjt"] = datetime.now(bjt).isoformat()
    tmp = SA_DRAFT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(draft, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, SA_DRAFT_PATH)
    _audit("sa_draft", draft={k: v for k, v in draft.items() if k != "notes"}, source="tailnet")
    return JSONResponse({"ok": True, "msg": "已保存单号自动化草稿;未启动 worker,未下单。"})


@app.post("/api/varia/budget")
async def set_varia_budget(payload: dict, request: Request) -> JSONResponse:
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用:待 Kevin 审阅后在服务环境设 "
                                                   "LATITUDE_ENABLE_WRITES=1(见 README)"}, status_code=403)
    if _is_cloudflare(request):
        return JSONResponse({"ok": False, "error": "公网入口只读:配置类写请走 Tailscale 内网"}, status_code=403)
    cap = _num((payload or {}).get("cap"))
    if cap is None or not (0 <= cap <= 500):
        return JSONResponse({"ok": False, "error": "cap 需为 0–500 之间的数字"}, status_code=400)
    path = VARIA_DIR / "auto_strategy_state.json"
    state = _read_json(path)
    if not isinstance(state, dict):
        return JSONResponse({"ok": False, "error": "auto_strategy_state.json 不可读"}, status_code=500)
    old = state.get("weekly_loss_cap_usdc")
    backup = path.with_suffix(f".json.bak-{datetime.now().strftime('%Y%m%d%H%M%S%f')}")
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
    raw = _read_json(path)
    if not isinstance(raw, dict):
        return {"ok": False, "error": "auto_strategy_state.json 不可读", "code": 500}
    state = dict(raw)
    state.update(_normalize_varia_auto_state(raw))
    before = {k: state.get(k) for k in updates}
    backup = path.with_suffix(f".json.bak-{datetime.now().strftime('%Y%m%d%H%M%S%f')}")
    backup.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    state.update(updates)
    state.update(_normalize_varia_auto_state(state))
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    with AUDIT_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"ts": datetime.now(timezone.utc).isoformat(),
                             "action": "set_auto_strategy", "before": before,
                             "after": updates, "backup": backup.name}, ensure_ascii=False) + "\n")
    return {"ok": True, "before": before, "after": updates}


def _sync_varia_auto_state_to_vps2() -> Dict[str, Any]:
    path = _varia_auto_state_file()
    result = _run_cmd([
        "scp", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
        "-o", "StrictHostKeyChecking=yes", str(path),
        f"{VARIA_VPS2_SSH}:{VARIA_VPS2_REPO}/data/auto_strategy_state.json",
    ], timeout=20)
    return {
        "ok": result.get("rc") == 0,
        "error": result.get("err") or result.get("out") or "VPS2 配置同步失败",
    }


def _varia_auto_payload(payload: dict) -> Dict[str, Any]:
    mode = str(payload.get("mode") or "")
    if mode not in {"semi_auto", "full_auto"}:
        raise ValueError("自动模式须为半自动或全自动")
    cap = _num(payload.get("weekly_loss_cap_usdc"))
    max_spread = _num(payload.get("max_auto_spread_bps"))
    ratio = _num(payload.get("major_ratio"))
    pressure = payload.get("pressure_test") if isinstance(payload.get("pressure_test"), dict) else {}
    min_minutes = _num(pressure.get("min_open_interval_minutes"))
    max_minutes = _num(pressure.get("max_open_interval_minutes"))
    hosts = payload.get("hosts") if isinstance(payload.get("hosts"), dict) else {}
    if cap is None or not 0 <= cap <= 500:
        raise ValueError("每周预算须为 0–500 USDC")
    if max_spread is None or not 0.1 <= max_spread <= 100:
        raise ValueError("最高综合点差须为 0.1–100 bps")
    if ratio is None or not 0 <= ratio <= 1:
        raise ValueError("A 策略主流币比例须为 0–100%")
    if min_minutes is None or max_minutes is None or not 1 <= min_minutes <= max_minutes <= 1440:
        raise ValueError("开仓间隔须为 1–1440 分钟，且最长不小于最短")
    normalized_hosts: Dict[str, dict] = {}
    for host, default in (("vps1", "A"), ("vps2", "B")):
        item = hosts.get(host) if isinstance(hosts.get(host), dict) else {}
        strategy = str(item.get("strategy") or default).upper()
        normalized_hosts[host] = {
            "enabled": bool(item.get("enabled")),
            "strategy": "B" if strategy == "B" else "A",
        }
    return {
        "mode": mode,
        "weekly_loss_cap_usdc": str(cap),
        "max_auto_spread_bps": str(max_spread),
        "major_ratio": str(ratio),
        "pressure_test": {
            "enabled": bool(pressure.get("enabled")),
            "min_open_interval_minutes": int(min_minutes),
            "max_open_interval_minutes": int(max_minutes),
        },
        "hosts": normalized_hosts,
    }


def _stop_all_varia_auto_workers() -> Dict[str, Dict[str, Any]]:
    return {host: _varia_worker_action(host, "stop") for host in ("vps1", "vps2")}


@app.get("/api/varia/automation")
def varia_automation_get() -> JSONResponse:
    return JSONResponse(_varia_automation_state())


@app.post("/api/varia/automation/config")
async def varia_automation_config(payload: dict, request: Request) -> JSONResponse:
    blocked = _varia_write_guard(request)
    if blocked is not None:
        return blocked
    try:
        updates = _varia_auto_payload(payload or {})
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    result = _write_auto_strategy(updates)
    if not result.get("ok"):
        return JSONResponse(result, status_code=int(result.pop("code", 500)))
    sync = _sync_varia_auto_state_to_vps2()
    _audit("varia_auto_config", config=updates, vps2_sync=sync.get("ok"), source="tailnet")
    note = "自动策略配置已保存，并同步到 VPS2。" if sync.get("ok") else "VPS1 已保存，但 VPS2 同步失败；未改变后台运行状态。"
    return JSONResponse({
        "ok": bool(sync.get("ok")), "saved": True, "sync_ok": bool(sync.get("ok")),
        "note": note, "error": None if sync.get("ok") else sync.get("error"),
        "state": _varia_automation_state(),
    }, status_code=200 if sync.get("ok") else 502)


@app.post("/api/varia/automation/start")
async def varia_automation_start(request: Request) -> JSONResponse:
    blocked = _varia_write_guard(request)
    if blocked is not None:
        return blocked
    state = _normalize_varia_auto_state(_read_json(_varia_auto_state_file()))
    selected = [host for host, item in state["hosts"].items() if item.get("enabled")]
    if not selected:
        return JSONResponse({"ok": False, "error": "请先至少启用一台 VPS 并保存配置"}, status_code=409)
    raw_states = _varia_raw_states()
    startable: List[str] = []
    start_blocks: List[str] = []
    for host in selected:
        configured = state["hosts"][host]
        readiness = _varia_host_live_readiness(
            host, raw_states.get(host), str(configured.get("strategy") or ""),
        )
        if readiness.get("ready"):
            startable.append(host)
        else:
            start_blocks.append(
                f"{host.upper()}：{readiness.get('reason') or '实盘验收未完成'}"
            )
    if not startable:
        return JSONResponse({
            "ok": False,
            "error": "；".join(start_blocks) + "。未启动任何 worker。",
            "state": _varia_automation_state(),
        }, status_code=409)
    effective_hosts = {
        host: dict(configured)
        for host, configured in state["hosts"].items()
    }
    for host in selected:
        if host not in startable:
            effective_hosts[host]["enabled"] = False
    written = _write_auto_strategy({
        "enabled": True,
        "hosts": effective_hosts,
    })
    if not written.get("ok"):
        return JSONResponse(written, status_code=int(written.pop("code", 500)))
    sync = _sync_varia_auto_state_to_vps2()
    if not sync.get("ok"):
        _write_auto_strategy({"enabled": False})
        _sync_varia_auto_state_to_vps2()
        _stop_all_varia_auto_workers()
        return JSONResponse({"ok": False, "error": "VPS2 配置同步失败，自动化未启动"}, status_code=502)
    results: Dict[str, Dict[str, Any]] = {}
    for host in ("vps1", "vps2"):
        results[host] = _varia_worker_action(host, "start" if host in startable else "stop")
    if any(item.get("rc") != 0 for item in results.values()):
        _stop_all_varia_auto_workers()
        _write_auto_strategy({"enabled": False})
        _sync_varia_auto_state_to_vps2()
        _audit(
            "varia_auto_start_failed", selected=selected, startable=startable,
            blocked=start_blocks, source="tailnet",
        )
        return JSONResponse({
            "ok": False, "error": "后台未能全部按配置启动，已回滚并停止两台 worker",
            "state": _varia_automation_state(),
        }, status_code=502)
    _audit(
        "varia_auto_start", selected=selected, started=startable,
        blocked=start_blocks, source="tailnet",
    )
    started_label = "、".join(host.upper() for host in startable)
    skipped_label = (
        "；" + "；".join(start_blocks) + "，已取消参与并保持停止。"
        if start_blocks else "。"
    )
    return JSONResponse({
        "ok": True,
        "note": f"已向 {started_label} 发送启动命令{skipped_label}",
        "started_hosts": startable,
        "blocked_hosts": start_blocks,
        "state": _varia_automation_state(),
    })


@app.post("/api/varia/automation/stop")
async def varia_automation_stop(request: Request) -> JSONResponse:
    blocked = _varia_write_guard(request)
    if blocked is not None:
        return blocked
    written = _write_auto_strategy({"enabled": False})
    sync = _sync_varia_auto_state_to_vps2() if written.get("ok") else {"ok": False}
    results = _stop_all_varia_auto_workers()
    ok = bool(written.get("ok")) and all(item.get("rc") == 0 for item in results.values())
    _audit("varia_auto_stop", vps2_sync=sync.get("ok"), workers_ok=ok, source="tailnet")
    return JSONResponse({
        "ok": ok, "note": "自动调度已停止；此操作不会自动平掉现有仓位。" if ok else None,
        "error": None if ok else "部分 worker 停止失败，请检查运行状态",
        "state": _varia_automation_state(),
    }, status_code=200 if ok else 502)


@app.post("/api/varia/auto")
async def set_varia_auto(payload: dict, request: Request) -> JSONResponse:
    """A 类:自动运行总开关 / 半-全自动模式(文件写,worker 生效,可逆)。"""
    if not WRITES_ENABLED:
        return JSONResponse({"ok": False, "error": "写通道未启用"}, status_code=403)
    if _is_cloudflare(request):
        return JSONResponse({"ok": False, "error": "公网入口只读:配置类写请走 Tailscale 内网"}, status_code=403)
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
    return HTMLResponse(
        CONSOLE_HTML.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"},
    )


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "data_dir": str(DATA_DIR), "varia_dir": str(VARIA_DIR),
            "data_dir_exists": DATA_DIR.exists(), "varia_dir_exists": VARIA_DIR.exists()}
