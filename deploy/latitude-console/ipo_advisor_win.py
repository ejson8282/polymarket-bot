"""打新判研 · 第一层(确定性简报)+ 观点渠道 + 判研包组装。

设计(与全项目"AI 只提议、人确认"铁律一致):
  - 本脚本只算"能算死的":每只申购中的股 → 真实入场费(minCapital)+ 锁资天数 →
    成本门槛;并入老板观点(data/ipo_boss_views.json)。不做主观打/跳判断(那是第二层
    的 Claude 智能体的活)。
  - 产出两样:①Discord 简报(给人看)②judgment_pack.json(给第二层 Claude 判研当输入)。
  - 零 API、零套餐消耗;第二层才唤起 Claude(消耗订阅额度)。

用法:
  python ipo_advisor.py brief          # 生成简报 + judgment_pack,推 Discord
  python ipo_advisor.py add-view CODE "观点文本"   # 落一条老板观点
  python ipo_advisor.py show-pack      # 只打印 judgment_pack(第二层读它)

数据源:控制台 /api/state 的 ipo(已读对真字段);老板观点本地 json。
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

DATA_DIR = Path(os.getenv("IPO_DATA_DIR", r"C:\ops\ipo-advisor\data"))
ROUTER_URL = os.getenv("IPO_ROUTER_URL", "http://127.0.0.1:8080/dashboard/ipo/state")
DISCORD_FILE = DATA_DIR / "discord_webhook.txt"
VIEWS_FILE = DATA_DIR / "ipo_boss_views.json"          # {code: [{ts, text}]}
PACK_FILE = DATA_DIR / "ipo_judgment_pack.json"        # 第二层判研输入
BJT = timezone(timedelta(hours=8))

# 港股孖展/保证金融资年化利率(粗口径,用于锁资磨损估算;真实按券商,可后续配置化)
MARGIN_APR = float(os.getenv("IPO_MARGIN_APR", "0.055"))

# Windows 控制台默认 GBK,强制 stdout/stderr utf-8,避免 emoji 打印崩溃
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _get_state() -> dict:
    """读本机 router 原始状态,映射成与 VPS1 控制台 _ipo() 同构的字段(fee/score/status...)。"""
    with _opener.open(ROUTER_URL, timeout=12) as r:
        d = json.load(r)
    ipo = d.get("ipo") if isinstance(d, dict) and isinstance(d.get("ipo"), dict) else d
    out_stocks = []
    for s in (ipo.get("stocks") or []):
        if not isinstance(s, dict):
            continue
        nz = str(s.get("nameZh") or s.get("name_zh") or "").strip()
        ne = str(s.get("nameEn") or s.get("name_en") or "").strip()
        nm = str(s.get("name") or "").strip()
        out_stocks.append({
            "code": str(s.get("code") or ""),
            "name": (nz or nm or ne)[:40],
            "fee": _num(s.get("minCapital")) or _num(s.get("fee")),
            "score": s.get("expectedScore") if s.get("expectedScore") is not None else s.get("score"),
            "hit_rate": s.get("hitRateScore"),
            "risk": str(s.get("risk") or "")[:12],
            "status": str(s.get("status") or "")[:10],
            "close_at": str(s.get("closeAt") or s.get("deadlineAt") or "")[:24],
            "listing_at": str(s.get("listingAt") or "")[:24],
            "refund_days": s.get("refundDays"),
            "prospectus": str(s.get("prospectusUrl") or "")[:200],
        })
    return {"ipo": {"mode": ipo.get("mode"), "round": ipo.get("round"), "stocks": out_stocks},
            "account_ops": {}}


def _discord(text: str) -> bool:
    try:
        url = DISCORD_FILE.read_text(encoding="utf-8").strip()
        if not url.startswith("https://"):
            return False
        req = urllib.request.Request(url, data=json.dumps({"content": text[:1900]}).encode("utf-8"),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "Latitude-IPO/1.0"})
        with _opener.open(req, timeout=10) as r:
            r.read()
        return True
    except Exception as e:
        print(f"discord push failed: {e}", file=sys.stderr)
        return False


def _load_views() -> dict:
    try:
        return json.loads(VIEWS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def add_view(code: str, text: str) -> None:
    views = _load_views()
    views.setdefault(str(code).upper(), []).append(
        {"ts": datetime.now(BJT).isoformat(timespec="minutes"), "text": text})
    tmp = VIEWS_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(views, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, VIEWS_FILE)
    print(f"已记录老板观点 · {code}: {text}")


def _lockup_cost(fee: float, refund_days) -> float:
    """锁资磨损:入场费 × 年化 × 锁资天数/365(粗口径,可算)。"""
    try:
        days = float(refund_days) if refund_days is not None else 3.0
    except (TypeError, ValueError):
        days = 3.0
    return (fee or 0) * MARGIN_APR * days / 365.0


AI_FIELDS = ("margin_subscription", "cornerstone", "sector_recent_day1",
             "expected_net", "verdict", "reason")


def _load_pack() -> dict:
    try:
        return json.loads(PACK_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def build_pack() -> dict:
    """组装判研包:每只申购中的股的确定性事实 + 老板观点。给第二层 Claude 当输入。
    重跑时保留已有 AI 判断(按代码 merge),不把第二层的 verdict/reason 冲掉。"""
    st = _get_state()
    ipo = st.get("ipo") or {}
    views = _load_views()
    prev = {str(s.get("code")): s for s in (_load_pack().get("stocks") or [])}
    active = [s for s in (ipo.get("stocks") or []) if s.get("status") in ("申购中", "招股中", "待申购")]
    items = []
    for s in active:
        code = str(s.get("code") or "")
        fee = s.get("fee")
        lock = _lockup_cost(fee, s.get("refund_days"))
        old = prev.get(code) or {}
        item = {
            "code": code, "name": s.get("name"),
            "fee_hkd": fee, "refund_days": s.get("refund_days"),
            "lockup_cost_hkd": round(lock, 2),
            "router_score": s.get("score"), "hit_rate_score": s.get("hit_rate"),
            "risk": s.get("risk"), "close_at": s.get("close_at"),
            "listing_at": s.get("listing_at"), "prospectus": s.get("prospectus"),
            "boss_views": views.get(code.upper()) or [],
        }
        for f in AI_FIELDS:                      # 保留已有 AI 判断(第二层填的)
            item[f] = old.get(f)
        items.append(item)
    # 账户可用资金(排班要用):从 account_ops owners 聚合
    ao = st.get("account_ops") or {}
    pack = {
        "generated_at": datetime.now(BJT).isoformat(timespec="minutes"),
        "round": ipo.get("round"), "mode": ipo.get("mode"),
        "active_count": len(active), "stocks": items,
        "accounts_capital": ao.get("capital"), "accounts_n": ao.get("accounts"),
        "note": "router_score/hit_rate 目前是导入占位默认值,不可信;真实判断以第二层上网查为准。",
    }
    pack["judged_at"] = _load_pack().get("judged_at")   # 保留上次 AI 判研时间
    tmp = PACK_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, PACK_FILE)
    return pack


def apply_verdicts(verdicts: dict) -> None:
    """第二层 Claude 判研完成后调用:把 {code: {verdict, expected_net, reason, cornerstone,
    margin_subscription, sector_recent_day1}} 写回判研包,并盖 judged_at 时间戳。
    这样控制台读同一个包就能把 AI 判断显示在打新页,和确定性事实并排。"""
    pack = _load_pack()
    if not pack.get("stocks"):
        pack = build_pack()
    by_code = {str(s.get("code")): s for s in pack.get("stocks") or []}
    n = 0
    for code, v in (verdicts or {}).items():
        s = by_code.get(str(code))
        if not s:
            continue
        for f in AI_FIELDS:
            if f in v:
                s[f] = v[f]
        n += 1
    pack["judged_at"] = datetime.now(BJT).isoformat(timespec="minutes")
    tmp = PACK_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(pack, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, PACK_FILE)
    print(f"已写回 {n} 只 AI 判断 · judged_at={pack['judged_at']}")


def brief() -> None:
    pack = build_pack()
    stamp = datetime.now(BJT).strftime("%m-%d %H:%M")
    if not pack["stocks"]:
        _discord(f"📋 打新判研简报({stamp}):当前无「申购中」新股。")
        print("无申购中新股")
        return
    lines = [f"📋 打新判研简报({stamp}) · {pack['active_count']} 只申购中"]
    for s in pack["stocks"]:
        fee = f"HK${s['fee_hkd']:,.0f}" if s.get("fee_hkd") else "费待补"
        lock = f"锁资磨损~HK${s['lockup_cost_hkd']:.0f}" if s.get("lockup_cost_hkd") else ""
        vtag = f" · 老板观点{len(s['boss_views'])}条" if s["boss_views"] else ""
        lines.append(f"· {s['code']} {(s['name'] or '')[:12]} | {fee} · {lock}{vtag}")
    lines.append("（评分为导入占位值,不可信;等第二层 Claude 判研出打/跳建议）")
    _discord("\n".join(lines))
    print("\n".join(lines))


def main() -> int:
    cmd = sys.argv[1] if len(sys.argv) > 1 else "brief"
    if cmd == "brief":
        brief()
    elif cmd == "add-view" and len(sys.argv) >= 4:
        add_view(sys.argv[2], sys.argv[3])
    elif cmd == "show-pack":
        print(json.dumps(build_pack(), ensure_ascii=False, indent=2))
    elif cmd == "apply-verdicts":
        # 从 stdin 读 {code:{verdict,expected_net,reason,...}} JSON,写回包
        raw = sys.stdin.read()
        apply_verdicts(json.loads(raw) if raw.strip() else {})
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
