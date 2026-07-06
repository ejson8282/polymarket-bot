"""统一导航壳 · 总览聚合器(施工包05·5C)。

FastAPI + 一页静态 HTML(照 docs/dashboard_spec/latitude_console_full.html 总览页
的配色与卡片矩阵)。每 refresh_sec(默认 15s,前端轮询)拉取各系统 status
端点/文件,拉不到显示灰卡「离线」;渲染项目卡矩阵 + 合并事件流。
**只读,无任何操作按钮**;绑定 127.0.0.1:8600。

启动:
    uvicorn overview_app:app --host 127.0.0.1 --port 8600
配置:同目录 unified_shell_config.json(缺省用 examples/ 内示例),格式:
    {"refresh_sec": 15,
     "systems": [{"name": "Var/Decibel", "kind": "file",
                  "target": "/home/ubuntu/varia/data/status.json", "link": "/vardec/"},
                 {"name": "Polymarket", "kind": "url",
                  "target": "http://127.0.0.1:8501/status", "link": "/alpha/"}]}
status.json 字段 schema 见 STATUS_CONVENTION.md;聚合器对缺字段全部容错。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

BASE_DIR = Path(__file__).resolve().parent
CONFIG_CANDIDATES = [BASE_DIR / "unified_shell_config.json",
                     BASE_DIR / "examples" / "unified_shell_config.json"]
FETCH_TIMEOUT_SEC = 3.0
MAX_EVENTS = 50

app = FastAPI(title="Latitude Console Overview", docs_url=None, redoc_url=None)


def _load_config() -> dict:
    for path in CONFIG_CANDIDATES:
        try:
            if path.exists():
                return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
    return {"refresh_sec": 15, "systems": []}


def _fetch_status(system: dict) -> dict:
    """单系统状态:kind=file 读 JSON 文件,kind=url GET;失败 → ok=False(灰卡离线)。"""
    name = str(system.get("name") or "unnamed")
    kind = str(system.get("kind") or "file")
    target = str(system.get("target") or "")
    out: Dict[str, Any] = {"name": name, "link": system.get("link") or "",
                           "ok": False, "status": None, "error": ""}
    try:
        if kind == "url":
            import requests

            resp = requests.get(target, timeout=FETCH_TIMEOUT_SEC)
            resp.raise_for_status()
            payload = resp.json()
        else:
            path = Path(target)
            if not path.is_absolute():
                path = BASE_DIR / path   # 相对路径按本文件目录解析,不依赖 CWD
            payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            out["ok"] = True
            out["status"] = payload
        else:
            out["error"] = "status 非对象"
    except Exception as exc:  # 任何失败都归为「离线」,聚合器绝不因单系统崩溃
        out["error"] = f"{type(exc).__name__}: {str(exc)[:120]}"
    return out


def _merged_events(cards: List[dict]) -> List[dict]:
    events: List[dict] = []
    for card in cards:
        status = card.get("status") or {}
        rows = status.get("events") if isinstance(status.get("events"), list) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            events.append({
                "system": card["name"],
                "ts": str(row.get("ts") or ""),
                "severity": str(row.get("severity") or "info").lower(),
                "message": str(row.get("message") or "")[:300],
            })
    events.sort(key=lambda e: e["ts"], reverse=True)
    return events[:MAX_EVENTS]


@app.get("/api/status")
def api_status() -> JSONResponse:
    config = _load_config()
    cards = [_fetch_status(system) for system in config.get("systems") or []]
    return JSONResponse({
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "refresh_sec": int(config.get("refresh_sec") or 15),
        "systems": cards,
        "events": _merged_events(cards),
    })


_PAGE = """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="UTF-8">
<title>Latitude Console · 总览</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#0E1116;--panel:#151B23;--panel2:#1A222C;--line:#242E3A;--text:#E8EDF2;
--muted:#8B98A9;--faint:#5A6676;--ok:#46B26B;--warn:#E7C547;--danger:#E5484D;
--mono:"IBM Plex Mono",Menlo,monospace;--sans:"IBM Plex Sans","PingFang SC",sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:14px}
.num{font-family:var(--mono);font-variant-numeric:tabular-nums}
header{display:flex;align-items:center;gap:24px;padding:14px 24px;border-bottom:1px solid var(--line)}
.brand{font-weight:700;letter-spacing:.04em}
.brand small{display:block;font-family:var(--mono);font-size:10px;color:var(--faint);letter-spacing:.22em;font-weight:400}
.clock{font-family:var(--mono);font-size:11px;color:var(--faint);margin-left:auto}
main{display:grid;grid-template-columns:1fr 340px;gap:16px;padding:16px 24px}
.cards{display:grid;grid-template-columns:repeat(2,1fr);gap:14px;align-content:start}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px;display:block;
color:var(--text);text-decoration:none}
.card.offline{opacity:.55;border-style:dashed}
.card h3{font-size:14px;margin-bottom:8px;display:flex;justify-content:space-between;align-items:center}
.pill{font-family:var(--mono);font-size:10.5px;padding:2px 8px;border-radius:99px;border:1px solid var(--line);color:var(--muted)}
.pill.ok{color:var(--ok);border-color:rgba(70,178,107,.4)}
.pill.off{color:var(--faint)}
.pill.warn{color:var(--warn);border-color:rgba(231,197,71,.4)}
.kv{display:flex;justify-content:space-between;font-size:12.5px;padding:4px 0;border-top:1px dashed var(--line)}
.kv:first-of-type{border-top:0}
.kv .k{color:var(--muted)} .kv .v{font-family:var(--mono)}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:16px}
.panel h2{font-size:13px;margin-bottom:10px}
.events{list-style:none}
.events li{display:flex;gap:10px;padding:8px 0;border-bottom:1px solid var(--panel2);font-size:12.5px}
.dot{width:8px;height:8px;border-radius:50%;margin-top:5px;flex:none;background:var(--faint)}
.dot.warn{background:var(--warn)} .dot.crit{background:var(--danger)}
.t{font-family:var(--mono);font-size:10.5px;color:var(--faint)}
footer{padding:8px 24px 20px;font-family:var(--mono);font-size:11px;color:var(--faint)}
@media(max-width:1000px){main{grid-template-columns:1fr}.cards{grid-template-columns:1fr}}
</style></head><body>
<header><div class="brand">Latitude Alpha<small>OPS CONSOLE · OVERVIEW</small></div>
<span class="clock num" id="clock">加载中…</span></header>
<main><div class="cards" id="cards"></div>
<aside><section class="panel"><h2>合并事件流</h2><ul class="events" id="events"></ul></section></aside></main>
<footer>只读聚合 · 无操作入口 · 数据来自各系统 status 端点/文件(schema 见 STATUS_CONVENTION.md)</footer>
<script>
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
async function refresh(){
  let data;
  try { data = await (await fetch("/api/status")).json(); }
  catch (e) { document.getElementById("clock").textContent = "聚合器不可达"; return; }
  document.getElementById("clock").textContent = data.generated_at + " · 每 " + data.refresh_sec + "s 刷新";
  const cards = document.getElementById("cards");
  cards.innerHTML = "";
  for (const sys of data.systems){
    const st = sys.status || {};
    const summary = (st.summary && typeof st.summary === "object") ? st.summary : {};
    const alerts = Array.isArray(st.alerts) ? st.alerts : [];
    const pill = sys.ok ? (alerts.length ? '<span class="pill warn">告警 ' + alerts.length + '</span>'
                                         : '<span class="pill ok">在线</span>')
                        : '<span class="pill off">离线</span>';
    let rows = "";
    for (const [k, v] of Object.entries(summary).slice(0, 5))
      rows += '<div class="kv"><span class="k">' + esc(k) + '</span><span class="v">' + esc(v) + '</span></div>';
    if (!sys.ok) rows = '<div class="kv"><span class="k">error</span><span class="v">' + esc(sys.error) + '</span></div>';
    const tag = sys.link ? "a" : "div";
    cards.insertAdjacentHTML("beforeend",
      "<" + tag + (sys.link ? ' href="' + esc(sys.link) + '"' : "") +
      ' class="card' + (sys.ok ? "" : " offline") + '"><h3>' + esc(sys.name) + pill + "</h3>" +
      rows + "</" + tag + ">");
  }
  const events = document.getElementById("events");
  events.innerHTML = "";
  for (const ev of data.events){
    const cls = ["critical","error","crit"].includes(ev.severity) ? "crit"
              : ["warning","warn"].includes(ev.severity) ? "warn" : "";
    events.insertAdjacentHTML("beforeend",
      '<li><span class="dot ' + cls + '"></span><div><div>' + esc(ev.message) +
      '</div><div class="t">' + esc(ev.ts) + " · " + esc(ev.system) + "</div></div></li>");
  }
}
refresh();
fetch("/api/status").then(r => r.json()).then(d => setInterval(refresh, (d.refresh_sec || 15) * 1000));
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return _PAGE


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8600)
