"""Latitude 告警/通知推送 — 双通道路由(Kevin 2026-07-08 定的规矩):

  飞书  = 急报:sev=crit 的告警(单腿、预算告急等)及其解除
  Discord = 日常:warn 级告警及解除、控制台写操作记录、每日早报(--digest)

webhook 文件(均 chmod 600,不入 git;缺文件 → 该通道静默跳过):
  data/feishu_webhook.txt   data/discord_webhook.txt
Discord 未配置时,warn 告警临时降级走飞书(带标注),写操作记录与早报跳过。

普通模式:由 alert-pusher.timer 每 5 分钟跑一次(新告警/解除 + 写操作增量)。
--digest:由 alert-digest.timer 每日 09:00 BJT 跑一次(状态早报)。
去重状态:data/alert_push_state.json(指纹+文本+级别;告警消失清指纹,复发再推)。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path

DATA_DIR = Path(os.getenv("LATITUDE_DATA_DIR", "/home/ubuntu/polymarket-bot/data"))
STATE_URL = os.getenv("LATITUDE_STATE_URL", "http://127.0.0.1:8600/api/state")
FEISHU_FILE = DATA_DIR / "feishu_webhook.txt"
DISCORD_FILE = DATA_DIR / "discord_webhook.txt"
PUSH_STATE = DATA_DIR / "alert_push_state.json"
AUDIT_LOG = DATA_DIR / "console_write_audit.jsonl"
CONSOLE_URL = "http://100.122.255.98:8502/"

_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _webhook(path: Path) -> str:
    try:
        url = path.read_text(encoding="utf-8").strip()
        return url if url.startswith("https://") else ""
    except Exception:
        return ""


def _post(url: str, payload: dict) -> None:
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Content-Type": "application/json"})
    with _opener.open(req, timeout=10) as resp:
        resp.read()


def send_feishu(text: str) -> bool:
    url = _webhook(FEISHU_FILE)
    if not url:
        return False
    try:
        _post(url, {"msg_type": "text", "content": {"text": text}})
        return True
    except Exception as e:  # 单通道故障不拖垮整轮(403=webhook 被删等)
        print(f"feishu push failed: {e}", file=sys.stderr)
        return False


def send_discord(text: str) -> bool:
    url = _webhook(DISCORD_FILE)
    if not url:
        return False
    try:
        _post(url, {"content": text[:1900]})
        return True
    except Exception as e:
        print(f"discord push failed: {e}", file=sys.stderr)
        return False


def send_routed(sev: str, text: str) -> None:
    """crit → 飞书;其余 → Discord,Discord 缺配置时降级飞书(标注临时)。"""
    if sev == "crit":
        send_feishu(text)
        return
    if not send_discord(text):
        send_feishu(text + "\n(临时走飞书:Discord 未配置)")


def _plain(t: str) -> str:
    return re.sub(r"<[^>]+>", "", str(t))


def _load_push_state() -> dict:
    try:
        raw = json.loads(PUSH_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {"alerts": {}, "audit_offset": 0}
    if "alerts" not in raw:  # 旧格式迁移:顶层即指纹表
        return {"alerts": {k: (v if isinstance(v, dict) else {"ts": v}) for k, v in raw.items()},
                "audit_offset": 0}
    return raw


def _fmt_audit(rec: dict) -> str:
    a = rec.get("action", "?")
    t = str(rec.get("ts", ""))[11:16]
    if a == "set_weekly_loss_cap":
        return f"{t} 改周预算 {rec.get('old')} → {rec.get('new')} USDC/台"
    if a == "set_auto_strategy":
        return f"{t} 自动运行设置 {rec.get('after')}"
    if a == "pm_engine":
        return f"{t} PM 引擎 {rec.get('request_action')}"
    if a == "pm_account":
        return f"{t} PM 账号{rec.get('idx')} {rec.get('request_action')}"
    if a == "sa_paper":
        return f"{t} SA paper {rec.get('request_action')}({'ok' if rec.get('ok') else '失败'})"
    if a == "pm_markets_apply":
        return f"{t} 应用市场配置:日 {rec.get('day')} · 夜 {rec.get('night')}"
    if a == "pm_proxies":
        return f"{t} 代理池 {rec.get('mode')} {rec.get('counts')}"
    if a == "sa_draft":
        return f"{t} 保存 SA 自动化草稿"
    return ""  # pm_scan / pm_precheck 等噪音跳过


def run_alerts() -> None:
    with _opener.open(STATE_URL, timeout=10) as resp:
        state = json.load(resp)
    alerts = state.get("alerts") or []
    ps = _load_push_state()
    prev = ps.get("alerts") or {}

    current: dict = {}
    for a in alerts:
        sev = a.get("sev") or "warn"
        text = f"[{a.get('tag', '')}]{'🔴' if sev == 'crit' else '🟡'} {_plain(a.get('msg', ''))}"
        current[hashlib.sha1(text.encode()).hexdigest()[:16]] = {"text": text, "sev": sev}

    stamp = time.strftime("%m-%d %H:%M")
    for sev in ("crit", "warn"):
        new = [v["text"] for k, v in current.items() if k not in prev and v["sev"] == sev]
        if new:
            send_routed(sev, ("🚨" if sev == "crit" else "⚠️") + f" Latitude 新告警({stamp}):\n"
                        + "\n".join("· " + t for t in new)
                        + f"\n共 {len(current)} 条活跃 → {CONSOLE_URL}")
        gone = [v for k, v in prev.items()
                if k not in current and (v.get("sev") or "warn") == sev and v.get("text")]
        if gone:
            send_routed(sev, f"✅ Latitude 告警解除({stamp}):\n"
                        + "\n".join("· " + v["text"] for v in gone)
                        + (f"\n仍有 {len(current)} 条活跃" if current else "\n当前无活跃告警 🎉"))

    out_alerts = {}
    for k, v in current.items():
        old = prev.get(k) or {}
        out_alerts[k] = {"ts": int(old.get("ts") or time.time()), **v}

    # 写操作增量 → Discord(日常通道;无 Discord 则跳过,审计文件本身仍是完整记录)
    offset = int(ps.get("audit_offset") or 0)
    lines: list = []
    if AUDIT_LOG.exists():
        raw = AUDIT_LOG.read_text(encoding="utf-8")
        if offset > len(raw):  # 文件被轮转/截断
            offset = 0
        chunk = raw[offset:]
        offset = len(raw)
        for ln in chunk.splitlines():
            try:
                msg = _fmt_audit(json.loads(ln))
                if msg:
                    lines.append(msg)
            except Exception:
                continue
    if lines and _webhook(DISCORD_FILE):
        send_discord("🛠 控制台写操作(" + stamp + "):\n" + "\n".join("· " + x for x in lines[-8:]))

    PUSH_STATE.write_text(json.dumps({"alerts": out_alerts, "audit_offset": offset},
                                     ensure_ascii=False), encoding="utf-8")


def run_digest() -> None:
    with _opener.open(STATE_URL, timeout=10) as resp:
        s = json.load(resp)
    icon = {"ok": "🟢", "warn": "🟡", "danger": "🔴", "unknown": "⚪"}
    fresh = s.get("freshness") or {}
    names = {"pm": "Polymarket", "vardec": "Var/Decibel", "pf": "Predict.fun",
             "sa": "SingleAccount", "hk": "HK/US 账户"}
    rows = [f"{icon.get(v.get('tier'), '⚪')} {names.get(k, k)} {v.get('label', '')}"
            for k, v in fresh.items()]
    vd = s.get("var_decibel") or {}
    eq = vd.get("equity_history") or {}
    eq_line = (f"权益 ${eq.get('last')}(区间 {('+' if (eq.get('change') or 0) >= 0 else '')}{eq.get('change')})"
               if eq.get("present") else "权益曲线无数据")
    bud = vd.get("budget") or {}
    bud_line = " · ".join(f"{h} 剩 ${v.get('remaining')}/{v.get('cap')}"
                          for h, v in (bud.get("hosts") or {}).items()) or "预算无数据"
    alerts = s.get("alerts") or []
    a_line = ("活跃告警 " + str(len(alerts)) + " 条:" + "、".join(a.get("tag", "") for a in alerts)) \
        if alerts else "无活跃告警 🎉"
    body = ("📋 Latitude 早报 " + time.strftime("%m-%d %H:%M") + "\n"
            + "\n".join(rows) + "\n" + eq_line + "\n" + bud_line + "\n" + a_line
            + "\n" + CONSOLE_URL)
    if not send_discord(body):
        send_feishu(body + "\n(临时走飞书:Discord 未配置)")


def main() -> int:
    if not _webhook(FEISHU_FILE) and not _webhook(DISCORD_FILE):
        return 0  # 两个通道都没配,静默
    if "--digest" in sys.argv:
        run_digest()
    else:
        run_alerts()
    return 0


if __name__ == "__main__":
    sys.exit(main())
