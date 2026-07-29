"""Latitude 告警/通知推送。

  飞书 = 急报:sev=crit 的告警(单腿、预算告急等)及其解除
  Discord 正常频道 = 每日早报、控制台写操作等日常记录
  Discord 重要频道 = warn/crit 告警、错误、bug、风险及其解除

webhook 文件(均 chmod 600,不入 git;缺文件 → 该通道静默跳过):
  data/feishu_webhook.txt
  data/discord_normal_webhook.txt
  data/discord_important_webhook.txt

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
DISCORD_NORMAL_FILE = DATA_DIR / "discord_normal_webhook.txt"
DISCORD_IMPORTANT_FILE = DATA_DIR / "discord_important_webhook.txt"
PUSH_STATE = DATA_DIR / "alert_push_state.json"
AUDIT_LOG = DATA_DIR / "console_write_audit.jsonl"
SYSTEM_EVENT_LOG = DATA_DIR / "system_events.jsonl"
CONSOLE_URL = "http://100.122.255.98:8502/"

_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _webhook(path: Path) -> str:
    try:
        url = path.read_text(encoding="utf-8").strip()
        return url if url.startswith("https://") else ""
    except Exception:
        return ""


def _post(url: str, payload: dict) -> None:
    # 必带 User-Agent:Discord 的 Cloudflare 对无 UA 请求一律 403(2026-07-15 查明——
    # 此前 Discord 推送全 403、误以为 webhook 失效、warn 全涌飞书,根因即缺此头)。
    req = urllib.request.Request(url, data=json.dumps(payload).encode("utf-8"),
                                 headers={"Content-Type": "application/json",
                                          "User-Agent": "Latitude-Alert/1.0"})
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


def _discord_webhook(channel: str) -> str:
    primary = DISCORD_IMPORTANT_FILE if channel == "important" else DISCORD_NORMAL_FILE
    return _webhook(primary)


def send_discord(text: str, *, channel: str = "normal") -> bool:
    url = _discord_webhook(channel)
    if not url:
        return False
    try:
        _post(url, {"content": text[:1900]})
        return True
    except Exception as e:
        print(f"discord {channel} push failed: {e}", file=sys.stderr)
        return False


def send_routed(sev: str, text: str) -> None:
    """All active alerts go to Discord important; crit also goes to Feishu."""
    if sev == "crit":
        send_feishu(text)
    send_discord(text, channel="important")


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
    if a == "discord_webhook":
        channel = "普通通知" if rec.get("channel") == "normal" else "重要通知"
        state = "已配置" if rec.get("configured") else "已清除"
        return f"{t} {channel}频道{state}"
    return ""  # pm_scan / pm_precheck 等噪音跳过


def _alert_project(tag: str) -> tuple[str, str]:
    upper = str(tag or "").upper()
    if upper.startswith(("VAR", "DEC", "ONDO")):
        return "var", "vardec"
    if upper.startswith("PM"):
        return "pm", "pm"
    if upper.startswith(("PF", "PREDICT")):
        return "pf", "pf"
    if upper.startswith(("SA", "SINGLE")):
        return "sa", "sa"
    if upper.startswith("GRID"):
        return "grid", "grid"
    if upper.startswith(("HK", "IPO", "ALPHA")):
        return "hk", "hk"
    return "infra", "overview"


def _legacy_alert_tag(value: dict) -> str:
    tag = str(value.get("tag") or "").strip()
    if tag:
        return tag
    text = str(value.get("text") or "")
    match = re.match(r"^\[([^\]]+)\]", text)
    return match.group(1) if match else ""


def _append_system_event(
    *,
    fingerprint: str,
    tag: str,
    sev: str,
    msg: str,
    resolved: bool,
    page: str = "",
) -> None:
    project, default_page = _alert_project(tag)
    record = {
        "id": f"alert:{fingerprint}:{'resolved' if resolved else 'raised'}",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "project": project,
        "page": page or default_page,
        "sev": "info" if resolved else sev,
        "kind": "alert_resolved" if resolved else "alert_raised",
        "msg": (
            f"{tag} 告警已解除：{_plain(msg)}"
            if resolved else f"{tag} 持续告警：{_plain(msg)}"
        ),
    }
    try:
        SYSTEM_EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with SYSTEM_EVENT_LOG.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as exc:
        print(f"system event write failed: {exc}", file=sys.stderr)


# 防抖:告警必须持续存在 ≥ 这么久才推(kill "闪断一下自己就好"的噪音)。
# 定时器每 5min 跑,600s = 至少熬过一整个周期还在,才算真问题。
DEBOUNCE_SEC = 600
PM_OPPORTUNITY_NOTIFY_COOLDOWN_SEC = 24 * 60 * 60


def _notify_pm_verification_candidates(
    state: dict,
    previous: dict,
    now: int,
) -> dict:
    retained = {}
    for key, value in (previous or {}).items():
        try:
            notified_at = int(value)
        except (TypeError, ValueError):
            continue
        if now - notified_at < PM_OPPORTUNITY_NOTIFY_COOLDOWN_SEC:
            retained[str(key)] = notified_at

    rows = (
        ((state.get("polymarket") or {}).get("curator") or {}).get(
            "opportunities"
        )
        or []
    )
    candidates = []
    for row in rows:
        if not isinstance(row, dict) or row.get("verification_recommended") is not True:
            continue
        if row.get("verification_status") not in {"stable", "confirmed"}:
            continue
        condition_id = str(row.get("condition_id") or "").strip().lower()
        try:
            account = int(row.get("account") or 0)
        except (TypeError, ValueError):
            account = 0
        key = f"{account}:{condition_id}"
        if not condition_id or key in retained:
            continue
        candidates.append((key, row))
    candidates.sort(
        key=lambda item: float(
            item[1].get("risk_adjusted_daily_roi_pct") or 0
        ),
        reverse=True,
    )
    candidates = candidates[:5]
    if not candidates or not _discord_webhook("normal"):
        return retained

    lines = []
    for _, row in candidates:
        question = str(row.get("question") or row.get("slug") or "未命名市场")
        if len(question) > 54:
            question = question[:51] + "..."
        lines.append(
            f"· 账号{int(row.get('account') or 0)} {question}"
            f"｜测试本金 ${float(row.get('probe_capital_usd') or 0):,.2f}"
            f"｜风险调整效率 {float(row.get('risk_adjusted_daily_roi_pct') or 0):.2f}%/日"
        )
    sent = send_discord(
        "🔎 Polymarket 小额验证候选已稳定:\n"
        + "\n".join(lines)
        + f"\n仅生成计划，尚未下单 → {CONSOLE_URL}",
        channel="normal",
    )
    if sent:
        for key, _ in candidates:
            retained[key] = now
    return retained


def run_alerts() -> None:
    with _opener.open(STATE_URL, timeout=10) as resp:
        state = json.load(resp)
    alerts = state.get("alerts") or []
    ps = _load_push_state()
    prev = ps.get("alerts") or {}
    now = int(time.time())

    current: dict = {}
    for a in alerts:
        sev = a.get("sev") or "warn"
        tag = str(a.get("tag") or "")
        raw_msg = _plain(a.get("msg", ""))
        text = f"[{tag}]{'🔴' if sev == 'crit' else '🟡'} {raw_msg}"
        current[hashlib.sha1(text.encode()).hexdigest()[:16]] = {
            "text": text,
            "raw_msg": raw_msg,
            "tag": tag,
            "page": str(a.get("page") or ""),
            "sev": sev,
        }

    # 结转状态:记 first_seen(首次出现)与 pushed(是否已推过),用于防抖 + 只对推过的报解除
    out_alerts = {}
    for k, v in current.items():
        old = prev.get(k) or {}
        out_alerts[k] = {"first_seen": int(old.get("first_seen") or now),
                         "pushed": bool(old.get("pushed")), **v}

    stamp = time.strftime("%m-%d %H:%M")
    # 新告警:仅推送"已持续 ≥DEBOUNCE 且尚未推过"的(自愈的短暂告警永不打扰)
    for sev in ("crit", "warn"):
        ripe = [(k, v) for k, v in out_alerts.items()
                if v["sev"] == sev and not v["pushed"] and (now - v["first_seen"]) >= DEBOUNCE_SEC]
        if ripe:
            send_routed(sev, ("🚨" if sev == "crit" else "⚠️") + f" Latitude 新告警({stamp}):\n"
                        + "\n".join("· " + v["text"] for _, v in ripe)
                        + f"\n共 {len(current)} 条活跃 → {CONSOLE_URL}")
            for k, value in ripe:
                out_alerts[k]["pushed"] = True
                _append_system_event(
                    fingerprint=k,
                    tag=str(value.get("tag") or ""),
                    sev=sev,
                    msg=str(value.get("raw_msg") or value.get("text") or ""),
                    resolved=False,
                    page=str(value.get("page") or ""),
                )
    # 解除:只对"之前真推过"的告警报解除(被防抖挡下、从没推过的,消失也不吭声)
    for sev in ("crit", "warn"):
        gone = [
            (key, value) for key, value in prev.items()
            if key not in current
            and value.get("pushed")
            and (value.get("sev") or "warn") == sev
            and value.get("text")
        ]
        if gone:
            send_routed(sev, f"✅ Latitude 告警解除({stamp}):\n"
                        + "\n".join("· " + value["text"] for _, value in gone)
                        + (f"\n仍有 {len(current)} 条活跃" if current else "\n当前无活跃告警 🎉"))
            for key, value in gone:
                tag = _legacy_alert_tag(value)
                _append_system_event(
                    fingerprint=key,
                    tag=tag,
                    sev=sev,
                    msg=str(value.get("raw_msg") or value.get("text") or ""),
                    resolved=True,
                    page=str(value.get("page") or ""),
                )

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
    if lines and _discord_webhook("normal"):
        send_discord(
            "🛠 控制台写操作(" + stamp + "):\n" + "\n".join("· " + x for x in lines[-8:]),
            channel="normal",
        )

    opportunity_notified = _notify_pm_verification_candidates(
        state,
        ps.get("pm_opportunity_notified") or {},
        now,
    )
    PUSH_STATE.write_text(
        json.dumps(
            {
                "alerts": out_alerts,
                "audit_offset": offset,
                "pm_opportunity_notified": opportunity_notified,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


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
    eq_line = _equity_digest_line(vd)
    bud = vd.get("budget") or {}
    bud_line = " · ".join(f"{h} 剩 ${v.get('remaining')}/{v.get('cap')}"
                          for h, v in (bud.get("hosts") or {}).items()) or "预算无数据"
    alerts = s.get("alerts") or []
    a_line = ("活跃告警 " + str(len(alerts)) + " 条:" + "、".join(a.get("tag", "") for a in alerts)) \
        if alerts else "无活跃告警 🎉"
    sections = [
        "📋 Latitude 早报 " + time.strftime("%m-%d %H:%M"),
        *rows,
        eq_line,
        bud_line,
        "",
        *_ipo_digest_lines(s.get("ipo")),
        "",
        a_line,
        CONSOLE_URL,
    ]
    body = "\n".join(sections)
    send_discord(body, channel="normal")


def _ipo_digest_lines(raw: object, *, limit: int = 5) -> list[str]:
    """Compact the IPO workbench into the shared daily digest."""
    if not isinstance(raw, dict) or raw.get("present") is not True:
        return ["港股打新：数据暂不可用"]

    active_statuses = {"申购中", "招股中", "待申购"}
    stocks = [
        stock for stock in (raw.get("stocks") or [])
        if isinstance(stock, dict) and str(stock.get("status") or "") in active_statuses
    ]
    declared = raw.get("active_stocks")
    try:
        active_count = max(len(stocks), int(declared))
    except (TypeError, ValueError):
        active_count = len(stocks)
    if active_count == 0:
        return ["港股打新：当前无申购中新股"]

    lines = [f"港股打新 · {active_count} 只申购中"]
    for stock in stocks[:max(0, limit)]:
        code = str(stock.get("code") or "—")
        name = str(stock.get("name_zh") or stock.get("name") or stock.get("name_en") or "")[:14]
        parts = [f"· {code} {name}".rstrip()]
        try:
            fee = float(stock.get("fee"))
        except (TypeError, ValueError):
            fee = None
        if fee is not None:
            parts.append(f"HK${fee:,.0f}")
        try:
            lockup = float(stock.get("lockup_cost_hkd"))
        except (TypeError, ValueError):
            lockup = None
        if lockup is not None:
            parts.append(f"锁资磨损~HK${lockup:,.0f}")
        verdict = str(stock.get("ai_verdict") or "").strip()
        if verdict:
            parts.append(f"判研：{verdict}")
        lines.append(" · ".join(parts))

    hidden = max(0, active_count - len(stocks[:max(0, limit)]))
    if hidden:
        lines.append(f"· 另 {hidden} 只见控制台")
    if stocks and not any(str(stock.get("ai_verdict") or "").strip() for stock in stocks):
        lines.append("判研建议尚未生成，当前仅展示确定性申购数据")
    return lines


def _equity_digest_line(vd: dict) -> str:
    """Format only reconciled PnL; deposits and withdrawals are never PnL."""
    capital = vd.get("capital") if isinstance(vd.get("capital"), dict) else {}
    if capital.get("complete") is True:
        try:
            equity = float(capital["current_equity"])
            pnl = float(capital["pnl"])
            pnl_pct = float(capital["pnl_pct"])
        except (KeyError, TypeError, ValueError):
            return "权益对账暂不可用(已对账字段不完整)"
        up = pnl >= 0
        arrow, sign = ("▲", "+") if up else ("▼", "-")
        return (
            f"总权益 ${equity:,.2f} · 相对投入 {arrow}{sign}${abs(pnl):,.2f} "
            f"({sign}{abs(pnl_pct):.2f}%)"
        )

    eq = vd.get("equity_history") if isinstance(vd.get("equity_history"), dict) else {}
    if not eq.get("present"):
        return "权益曲线无数据"
    if eq.get("valid") is False:
        # 曲线标为无效(未逐笔扣充提)→ 不当收益数报,只说明,避免误导
        return f"权益曲线暂不可用({eq.get('reason') or '未扣充提,非真实收益'})"
    return "权益对账暂不可用(本金账本未完成)"


def main() -> int:
    if (
        not _webhook(FEISHU_FILE)
        and not _discord_webhook("normal")
        and not _discord_webhook("important")
    ):
        return 0  # 两个通道都没配,静默
    if "--digest" in sys.argv:
        run_digest()
    else:
        run_alerts()
    return 0


if __name__ == "__main__":
    sys.exit(main())
