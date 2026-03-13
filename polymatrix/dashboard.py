import json
import os
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from datetime import date

import requests
import streamlit as st
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
from scan_markets import fetch_markets, normalize_market, parse_slug, resolve_by_slug

TARGET_SLUG = "will-the-iranian-regime-fall-by-june-30"

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
ENGINE_PATH = BASE_DIR / "engine.py"
SCAN_PATH = BASE_DIR / "scan_markets.py"
PID_PATH = BASE_DIR / ".engine.pid"
LOG_PATH = BASE_DIR / "engine.log"
OVERNIGHT_LOG_PATH = BASE_DIR / "overnight_reprice.log"
BAT_PATH = BASE_DIR / "start_dashboard.bat"
REWARDS_STATS_PATH = BASE_DIR / "rewards_stats.json"
NOTIFY_PATH = BASE_DIR / "notifications.json"
SCAN_CACHE_PATH = BASE_DIR / "scan_cache.json"


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def save_config(cfg: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def engine_pid() -> int | None:
    if not PID_PATH.exists():
        return None
    try:
        return int(PID_PATH.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def engine_running() -> bool:
    pid = engine_pid()
    if not pid:
        return False
    try:
        out = subprocess.check_output(["tasklist", "/FI", f"PID eq {pid}"], text=True)
        return str(pid) in out
    except Exception:
        return False


def overnight_pid() -> int | None:
    p = BASE_DIR / ".overnight_reprice.pid"
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def overnight_running() -> bool:
    pid = overnight_pid()
    if not pid:
        return False
    try:
        out = subprocess.check_output(["tasklist", "/FI", f"PID eq {pid}"], text=True)
        return str(pid) in out
    except Exception:
        return False


def get_engine_process_info() -> dict:
    pid = engine_pid()
    if not pid:
        return {"running": False, "pid": None, "raw": ""}
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            text=True,
            encoding="utf-8",
            errors="ignore",
        ).strip()
        if not out or "No tasks" in out:
            return {"running": False, "pid": pid, "raw": out}
        # CSV line: "python.exe","3600","Console","1","59,596 K"
        parts = [x.strip('"') for x in out.split(",")]
        name = parts[0] if len(parts) > 0 else ""
        mem = parts[4] if len(parts) > 4 else ""
        return {"running": True, "pid": pid, "name": name, "memory": mem, "raw": out}
    except Exception as e:
        return {"running": False, "pid": pid, "raw": str(e)}


def start_engine() -> str:
    if engine_running():
        return "Engine 已在运行中"
    LOG_PATH.touch(exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as logf:
        proc = subprocess.Popen(
            [sys.executable, str(ENGINE_PATH)],
            cwd=str(BASE_DIR),
            stdout=logf,
            stderr=logf,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform.startswith("win") else 0,
        )
    PID_PATH.write_text(str(proc.pid), encoding="utf-8")
    return f"Engine 已启动，PID={proc.pid}"


def _kill_by_pidfile(pid_file: Path) -> tuple[bool, str]:
    if not pid_file.exists():
        return False, ""
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except Exception:
        pid = None
    try:
        if pid:
            subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True, text=True, check=False)
        return True, str(pid) if pid else "unknown"
    finally:
        try:
            pid_file.unlink()
        except Exception:
            pass


def stop_engine() -> str:
    # stop both engine and overnight bot, then cancel all orders
    stopped = []
    ok1, p1 = _kill_by_pidfile(PID_PATH)
    if ok1:
        stopped.append(f"engine(PID={p1})")

    op = BASE_DIR / ".overnight_reprice.pid"
    ok2, p2 = _kill_by_pidfile(op)
    if ok2:
        stopped.append(f"overnight(PID={p2})")

    cancel_msg = emergency_cancel_all()

    if not stopped:
        return f"已执行停机清仓：{cancel_msg}；本地未检测到运行中的进程"
    return f"已停止 {' + '.join(stopped)}；{cancel_msg}"


def reload_engine() -> str:
    was_running = engine_running()
    if was_running:
        stop_engine()
    return start_engine()


def emergency_cancel_all() -> str:
    """Best-effort cancel_all using py-clob-client without logging secrets."""
    code = r'''
import json, os, sys
from pathlib import Path
from py_clob_client.client import ClobClient
cfg=json.loads(Path("config.json").read_text(encoding="utf-8"))
acc=cfg.get("account",{})
host=cfg.get("rest_base_url","https://clob.polymarket.com").rstrip("/")
key=(os.getenv("POLY_PRIVATE_KEY","").strip() or str(acc.get("private_key","")).strip())
if not key or "REPLACE" in key:
    print("ERR:NO_KEY")
    sys.exit(2)
chain_id=int(acc.get("chain_id",137))
sig_type=int(acc.get("signature_type",0))
funder=acc.get("funder")
client=ClobClient(host, chain_id=chain_id, key=key, signature_type=sig_type, funder=funder)
client.set_api_creds(client.create_or_derive_api_creds())
client.cancel_all()
print("OK")
'''
    p = subprocess.run([sys.executable, "-c", code], cwd=str(BASE_DIR), capture_output=True, text=True)
    if p.returncode == 0:
        return "cancel_all 已执行"
    if "ERR:NO_KEY" in (p.stdout + p.stderr):
        return "急停失败：未配置私钥（请先设置 POLY_PRIVATE_KEY）"
    return f"急停执行失败（code={p.returncode}）"


def run_scan(min_volume: int, sort_by: str, top_n: int) -> str:
    proc = subprocess.run(
        [sys.executable, str(SCAN_PATH), "--min-volume", str(min_volume), "--sort-by", sort_by, "--top", str(top_n)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return f"扫描失败:\n{proc.stderr}"
    return proc.stdout or "(无输出)"


def _normalize_proxy_url(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if s.startswith("http://") or s.startswith("https://"):
        return s

    # support host:port:user:pass input
    parts = s.split(":")
    if len(parts) >= 4:
        host = parts[0]
        port = parts[1]
        user = parts[2]
        pwd = ":".join(parts[3:])
        return f"http://{user}:{pwd}@{host}:{port}"

    # fallback host:port
    if len(parts) == 2 and parts[1].isdigit():
        return f"http://{s}"

    return s


def _load_rewards_stats() -> dict:
    if not REWARDS_STATS_PATH.exists():
        return {}
    try:
        return json.loads(REWARDS_STATS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_rewards_stats(data: dict) -> None:
    REWARDS_STATS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_notifications() -> list[dict]:
    if not NOTIFY_PATH.exists():
        return []
    try:
        x = json.loads(NOTIFY_PATH.read_text(encoding="utf-8"))
        return x if isinstance(x, list) else []
    except Exception:
        return []


def _save_notifications(items: list[dict]) -> None:
    NOTIFY_PATH.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")


def _push_notification(kind: str, title: str, payload: dict | None = None):
    items = _load_notifications()
    items.insert(
        0,
        {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "kind": kind,
            "title": title,
            "payload": payload or {},
            "read": False,
        },
    )
    _save_notifications(items[:200])


def _load_scan_cache() -> dict:
    if not SCAN_CACHE_PATH.exists():
        return {}
    try:
        x = json.loads(SCAN_CACHE_PATH.read_text(encoding="utf-8"))
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def _save_scan_cache(data: dict) -> None:
    SCAN_CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _auto_scan_if_due(cfg: dict, every_hours: int = 3) -> dict:
    cache = _load_scan_cache()
    now_ts = datetime.now().timestamp()
    last_ts = float(cache.get("last_scan_ts") or 0.0)
    if now_ts - last_ts < every_hours * 3600:
        return cache

    rp = build_reward_candidates()
    cands = rp.get("candidates") or []
    current_tokens = [str(x.get("token_id", "")) for x in cands if x.get("token_id")]
    prev_tokens = set(cache.get("tokens") or [])
    new_tokens = [t for t in current_tokens if t not in prev_tokens]
    by_tid = {str(x.get("token_id", "")): x for x in cands}
    new_items = []
    for t in new_tokens:
        m = by_tid.get(str(t)) or {}
        new_items.append(
            {
                "token_id": str(t),
                "slug": m.get("slug", ""),
                "reward": float(m.get("reward") or 0.0),
                "vol24h": float(m.get("volume24h") or 0.0),
                "risk": m.get("risk", "mid"),
            }
        )

    new_cache = {
        "last_scan_ts": now_ts,
        "tokens": current_tokens,
        "new_tokens": new_tokens,
        "new_items": new_items,
        "new_count": len(new_tokens),
    }
    _save_scan_cache(new_cache)
    if len(new_tokens) > 0:
        _push_notification("scan", f"发现 {len(new_tokens)} 个新增候选事件", {"tokens": new_tokens, "items": new_items})
    return new_cache


def _to_end_ts(m: dict) -> float | None:
    keys = ["endDate", "end_date", "endTime", "end_time", "expiration", "resolveBy", "endTimestamp", "end_timestamp"]
    for k in keys:
        v = m.get(k)
        if v is None:
            continue
        try:
            if isinstance(v, (int, float)):
                x = float(v)
                # ms -> s
                return x / 1000.0 if x > 10_000_000_000 else x
            s = str(v).strip()
            if not s:
                continue
            if s.isdigit():
                x = float(s)
                return x / 1000.0 if x > 10_000_000_000 else x
            s = s.replace("Z", "+00:00")
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            continue
    return None


def _risk_level(m: dict) -> str:
    """Simple LP-oriented risk tag: low / mid / high."""
    vol = float(m.get("volume24h") or 0.0)
    spread = float(m.get("quotedSpread") or 0.0)
    reward = float(m.get("reward") or 0.0)
    slug = str(m.get("slug") or "").lower()

    score = 0
    if vol >= 300000:
        score += 2
    elif vol >= 120000:
        score += 1

    if spread <= 0.01:
        score += 2
    elif spread <= 0.02:
        score += 1

    if reward >= 300:
        score += 1

    if any(x in slug for x in ["war", "strike", "iran", "inva", "ceasefire"]):
        score -= 1

    if score >= 4:
        return "low"
    if score >= 2:
        return "mid"
    return "high"


def build_reward_candidates(
    min_reward: float = 100.0,
    min_volume: float = 50000.0,
    exclude_hours: int = 24,
    exclude_topic: bool = True,
    exclude_near_expiry: bool = True,
) -> dict:
    raw = fetch_markets(limit=1000)
    out = []
    rejected = []
    now_ts = datetime.now(timezone.utc).timestamp()

    for rm in raw:
        nm = normalize_market(rm)
        reward = float(nm.get("reward") or 0.0)
        vol = float(nm.get("volume24h") or 0.0)
        slug = str(nm.get("slug") or "").lower()

        reason = None
        if reward < min_reward:
            reason = "reward_too_low"
        elif vol < min_volume:
            reason = "volume_too_low"
        elif exclude_topic and any(x in slug for x in ["spx", "today", "by-march-", "by-end-of-day", "tweets", "truth-social", "crude-oil", "hit-high", "hit-low"]):
            reason = "excluded_topic"
        else:
            end_ts = _to_end_ts(rm)
            if exclude_near_expiry and end_ts and (end_ts - now_ts) < exclude_hours * 3600:
                reason = "near_expiry"

        if reason:
            rejected.append({"slug": slug, "reason": reason, "reward": reward, "volume24h": vol})
            continue

        nm["risk"] = _risk_level(nm)
        out.append(nm)

    out.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    return {"ok": True, "candidates": out, "rejected": rejected}


def _append_tokens_to_markets(cfg: dict, tokens: list[str]) -> int:
    token_set = {str(t) for t in tokens if str(t).strip()}
    if not token_set:
        return 0

    raw = fetch_markets(limit=1000)
    norm = [normalize_market(x) for x in raw]
    by_token = {str(m.get("token_id", "")): m for m in norm if m.get("token_id")}

    existing = {str(x.get("token_id", "")) for x in cfg.get("markets", [])}
    add_count = 0
    for tid in token_set:
        if tid in existing:
            continue
        m = by_token.get(tid)
        if not m:
            continue
        cfg.setdefault("markets", []).append(
            {
                "token_id": tid,
                "max_incentive_spread": float(m.get("maxIncentiveSpread") or 0.02),
                "price_tick": 0.01,
                "min_distance_from_best_bid": 0.01,
                "quote_size": float(cfg.get("strategy", {}).get("default_quote_size", 100)),
                "enabled": True,
            }
        )
        existing.add(tid)
        add_count += 1
    if add_count:
        save_config(cfg)
    return add_count


def fetch_rewards_overview() -> dict:
    """Fetch rewards market count and total daily rewards from rewards page API."""
    base = "https://polymarket.com/api/rewards/markets"
    params = {
        "orderBy": "rate_per_day",
        "position": "DESC",
        "query": "",
        "showFavorites": "false",
        "tagSlug": "all",
        "requestPath": "/rewards/user/markets",
        "onlyMergeable": "false",
        "noCompetition": "false",
        "onlyOpenOrders": "false",
        "onlyPositions": "false",
        "sponsored": "true",
    }

    cursor = ""
    count = 0
    total_rate = 0.0
    first_total_count = None

    for _ in range(60):
        if cursor:
            params["nextCursor"] = cursor
        else:
            params.pop("nextCursor", None)

        r = requests.get(base, params=params, timeout=25)
        r.raise_for_status()
        d = r.json() if r.content else {}
        items = (d.get("data") or []) if isinstance(d, dict) else []

        if first_total_count is None and isinstance(d, dict):
            first_total_count = d.get("total_count")

        if not items:
            break

        count += len(items)
        for it in items:
            rc = it.get("rewards_config") or []
            if rc and isinstance(rc, list):
                try:
                    total_rate += float((rc[0] or {}).get("rate_per_day") or 0)
                except Exception:
                    pass

        cursor = (d.get("next_cursor") or d.get("nextCursor") or "") if isinstance(d, dict) else ""
        if not cursor or cursor in ("LTE=", "-1"):
            break

    final_count = int(first_total_count or count)
    return {"ok": True, "event_count": final_count, "total_rate_per_day": round(total_rate, 6)}


def _extract_event_slug(item: str) -> str:
    v = (item or "").strip()
    if "polymarket.com" not in v:
        return ""
    p = urlparse(v)
    parts = [x for x in p.path.split("/") if x]
    # supports /event/<slug>/... and /zh/event/<slug>/...
    if "event" in parts:
        i = parts.index("event")
        if i + 1 < len(parts):
            return parts[i + 1]
    return ""


def _resolve_event_markets(item: str, default_quote_size: float) -> list[dict[str, Any]]:
    event_slug = _extract_event_slug(item)
    if not event_slug:
        return []
    try:
        r = requests.get("https://gamma-api.polymarket.com/events", params={"slug": event_slug, "limit": 5}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            return []
        event = data[0]
        out = []
        for m in event.get("markets", []) or []:
            nm = normalize_market(m)
            token_id = str(nm.get("token_id", ""))
            if not token_id.isdigit():
                continue
            out.append(
                {
                    "token_id": token_id,
                    "max_incentive_spread": float(nm.get("maxIncentiveSpread", 0.02) or 0.02),
                    "price_tick": 0.01,
                    "min_distance_from_best_bid": 0.01,
                    "quote_size": float(default_quote_size),
                    "enabled": True,
                    "_slug": nm.get("slug", ""),
                    "_reward": nm.get("reward", 0),
                    "_umaReward": nm.get("umaReward", 0),
                    "_clobDailyRate": nm.get("clobDailyRate", 0),
                    "_volume24h": nm.get("volume24h", 0),
                    "_event": event_slug,
                }
            )
        # Keep higher reward first, then higher volume
        out.sort(key=lambda x: (float(x.get("_reward", 0)), float(x.get("_volume24h", 0))), reverse=True)
        return out
    except Exception:
        return []


def resolve_market_inputs(raw_text: str, default_quote_size: float) -> list[dict[str, Any]]:
    """Resolve market URLs/slugs OR event URLs into market rows for config."""
    lines = [x.strip() for x in (raw_text or "").splitlines() if x.strip()]
    if not lines:
        return []

    all_markets = [normalize_market(m) for m in fetch_markets(limit=1000)]
    resolved = []
    for item in lines:
        # 1) event URL may expand to multiple markets
        event_rows = _resolve_event_markets(item, default_quote_size)
        if event_rows:
            resolved.extend(event_rows)
            continue

        # 2) normal market URL/slug -> single market
        slug = parse_slug(item)
        found = resolve_by_slug(all_markets, slug)
        if not found:
            continue
        token_id = str(found.get("token_id", ""))
        if not token_id.isdigit():
            continue
        resolved.append(
            {
                "token_id": token_id,
                "max_incentive_spread": float(found.get("maxIncentiveSpread", 0.02) or 0.02),
                "price_tick": 0.01,
                "min_distance_from_best_bid": 0.01,
                "quote_size": float(default_quote_size),
                "enabled": True,
                "_slug": found.get("slug", ""),
                "_reward": found.get("reward", 0),
                "_umaReward": found.get("umaReward", 0),
                "_clobDailyRate": found.get("clobDailyRate", 0),
                "_volume24h": found.get("volume24h", 0),
                "_event": "",
            }
        )
    return resolved


def ensure_defaults(cfg: dict) -> dict:
    cfg.setdefault("rest_base_url", "https://clob.polymarket.com")
    cfg.setdefault("account", {})
    cfg["account"].setdefault("private_key", "REPLACE_PRIVATE_KEY")
    cfg["account"].setdefault("chain_id", 137)
    cfg["account"].setdefault("signature_type", 0)
    cfg["account"].setdefault("funder", "")

    cfg.setdefault("strategy", {})
    cfg["strategy"].setdefault("requote_interval_ms", 300)
    cfg["strategy"].setdefault("default_price_tick", 0.1)
    cfg["strategy"].setdefault("default_min_distance_from_best_bid", 0.1)
    cfg["strategy"].setdefault("default_quote_size", 100)
    cfg["strategy"].setdefault("min_order_size", 5)
    cfg["strategy"].setdefault("quote_size_mode", "balance_pct")
    cfg["strategy"].setdefault("quote_balance_pct_min", 0.90)
    cfg["strategy"].setdefault("quote_balance_pct_max", 0.99)
    # risk-tier principal percentage ranges (engine mode)
    cfg["strategy"].setdefault("quote_balance_pct_min_low", 0.95)
    cfg["strategy"].setdefault("quote_balance_pct_max_low", 0.99)
    cfg["strategy"].setdefault("quote_balance_pct_min_mid", 0.80)
    cfg["strategy"].setdefault("quote_balance_pct_max_mid", 0.95)
    cfg["strategy"].setdefault("quote_balance_pct_min_high", 0.50)
    cfg["strategy"].setdefault("quote_balance_pct_max_high", 0.70)
    cfg["strategy"].setdefault("post_only", True)

    cfg.setdefault("risk", {})
    cfg["risk"].setdefault("kill_switch_on_fill", True)
    cfg["risk"].setdefault("cooldown_seconds", 60)
    cfg["risk"].setdefault("max_notional_usdc_per_order", 500)
    cfg["risk"].setdefault("max_balance_fail_streak", 8)

    cfg.setdefault("reporting", {})
    cfg["reporting"].setdefault("discord_webhook", "")
    cfg["reporting"].setdefault("hourly_summary", True)

    cfg.setdefault("proxy_pool", {})
    cfg["proxy_pool"].setdefault("enabled", False)
    cfg["proxy_pool"].setdefault("use_for_ws", True)
    cfg["proxy_pool"].setdefault("use_for_accounts", False)
    cfg["proxy_pool"].setdefault("max_ws_per_proxy", 3)
    cfg["proxy_pool"].setdefault("items", [])

    cfg.setdefault("execution", {})
    cfg["execution"].setdefault("batch_size", 8)
    cfg["execution"].setdefault("batch_interval_sec", 30)
    cfg["execution"].setdefault("amount_jitter_pct", 0.08)
    cfg["execution"].setdefault("enable_order_rotation", True)
    cfg["execution"].setdefault("rotation_interval_min", 60)
    cfg["execution"].setdefault("skip_stable_live_orders", True)

    cfg.setdefault("markets", [])
    return cfg


def write_bat() -> None:
    content = f"""@echo off
setlocal
cd /d "{BASE_DIR}"
streamlit run dashboard.py --server.headless true --browser.gatherUsageStats false
"""
    BAT_PATH.write_text(content, encoding="utf-8")


def _normalize_usdc_amount(v: Any) -> float | None:
    """Normalize possible raw/on-chain USDC units to human-readable USDC.

    Common cases:
    - "160447429" -> 160.447429 (raw 6-decimal USDC units)
    - "160.447429" -> 160.447429 (already human)
    """
    if v is None:
        return None
    try:
        if isinstance(v, dict):
            for k in ["available", "value", "formatted", "human", "humanReadable", "raw"]:
                if k in v and v[k] is not None:
                    return _normalize_usdc_amount(v[k])
            return None

        s = str(v).strip()
        if not s:
            return None

        # integer string => very likely raw token units from API
        t = s[1:] if s.startswith("-") else s
        if t.isdigit():
            n = int(s)
            if abs(n) >= 1_000_000:
                return n / 1_000_000.0
            return float(n)

        # decimal string => already human-readable in most responses
        return float(s)
    except Exception:
        return None


def _parse_key_list(cfg: dict) -> list[str]:
    account = cfg.get("account", {})
    keys: list[str] = []

    env_multi = os.getenv("POLY_PRIVATE_KEYS", "").strip()
    if env_multi:
        for part in env_multi.replace(",", "\n").splitlines():
            k = part.strip()
            if k and "REPLACE" not in k:
                keys.append(k)

    env_single = os.getenv("POLY_PRIVATE_KEY", "").strip()
    if env_single and "REPLACE" not in env_single:
        keys.append(env_single)

    cfg_key = str(account.get("private_key", "")).strip()
    if cfg_key and "REPLACE" not in cfg_key:
        keys.append(cfg_key)

    # dedupe while preserving order
    uniq = []
    seen = set()
    for k in keys:
        if k not in seen:
            seen.add(k)
            uniq.append(k)
    return uniq


def _mask_key(k: str) -> str:
    if not k:
        return "(empty)"
    return f"{k[:8]}...{k[-6:]}"


def _single_balance_allowance_snapshot(cfg: dict, key: str) -> dict:
    account = cfg.get("account", {})
    host = str(cfg.get("rest_base_url", "https://clob.polymarket.com")).rstrip("/")
    chain_id = int(account.get("chain_id", 137))
    signature_type = int(account.get("signature_type", 0))
    funder = str(account.get("funder", "")).strip()

    kwargs = {"host": host, "chain_id": chain_id, "key": key, "signature_type": signature_type}
    if funder:
        kwargs["funder"] = funder

    client = ClobClient(**kwargs)
    client.set_api_creds(client.create_or_derive_api_creds())

    params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, token_id="", signature_type=signature_type)
    data = client.get_balance_allowance(params)

    bal = None
    alw = None
    allowance_unlimited = False
    if isinstance(data, dict):
        bal = data.get("balance")
        # some responses use `allowance`, others use `allowances` map
        if "allowance" in data:
            alw = data.get("allowance")
        elif isinstance(data.get("allowances"), dict):
            raw_vals = list((data.get("allowances") or {}).values())
            # ERC20 max approvals appear as huge uint values; treat as unlimited for UI.
            try:
                maxish = [int(str(v)) for v in raw_vals if str(v).strip().lstrip("-").isdigit()]
                if maxish and min(maxish) >= 10**30:
                    allowance_unlimited = True
                    alw = None
                else:
                    vals = [_normalize_usdc_amount(v) for v in raw_vals]
                    vals = [v for v in vals if v is not None]
                    alw = min(vals) if vals else None
            except Exception:
                vals = [_normalize_usdc_amount(v) for v in raw_vals]
                vals = [v for v in vals if v is not None]
                alw = min(vals) if vals else None

    bal_f = _normalize_usdc_amount(bal)
    alw_f = _normalize_usdc_amount(alw)
    eff = bal_f if allowance_unlimited else (min(x for x in [bal_f, alw_f] if x is not None) if (bal_f is not None or alw_f is not None) else None)
    return {"ok": True, "balance": bal_f, "allowance": alw_f, "allowance_unlimited": allowance_unlimited, "effective": eff, "raw": data}


def get_balance_allowance_snapshot(cfg: dict) -> dict:
    try:
        keys = _parse_key_list(cfg)
        if not keys:
            return {"ok": False, "error": "NO_KEY"}

        items = []
        for i, key in enumerate(keys, start=1):
            try:
                snap = _single_balance_allowance_snapshot(cfg, key)
                items.append({"name": f"Key-{i}", "key_mask": _mask_key(key), **snap})
            except Exception as e:
                items.append({"name": f"Key-{i}", "key_mask": _mask_key(key), "ok": False, "error": str(e)})

        ok_items = [x for x in items if x.get("ok")]
        if not ok_items:
            return {"ok": False, "error": "ALL_KEYS_FAILED", "items": items}

        # summary (for metric cards): use totals by effective available
        total_balance = sum(float(x.get("balance") or 0.0) for x in ok_items)
        total_effective = sum(float(x.get("effective") or 0.0) for x in ok_items)
        return {
            "ok": True,
            "balance": total_balance,
            "allowance": None,
            "allowance_unlimited": True,
            "effective": total_effective,
            "items": items,
            "raw": {x.get("name", "key"): x.get("raw") for x in ok_items},
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _build_client_from_cfg(cfg: dict) -> ClobClient:
    account = cfg.get("account", {})
    host = str(cfg.get("rest_base_url", "https://clob.polymarket.com")).rstrip("/")
    chain_id = int(account.get("chain_id", 137))
    signature_type = int(account.get("signature_type", 0))
    funder = str(account.get("funder", "")).strip()
    key = (os.getenv("POLY_PRIVATE_KEY", "").strip() or str(account.get("private_key", "")).strip())
    if not key or "REPLACE" in key:
        raise RuntimeError("NO_KEY")
    kwargs = {"host": host, "chain_id": chain_id, "key": key, "signature_type": signature_type}
    if funder:
        kwargs["funder"] = funder
    c = ClobClient(**kwargs)
    c.set_api_creds(c.create_or_derive_api_creds())
    return c


def get_live_quote_snapshot(cfg: dict) -> dict:
    try:
        client = _build_client_from_cfg(cfg)
        m = requests.get(
            "https://gamma-api.polymarket.com/markets",
            params={"slug": TARGET_SLUG, "limit": 5},
            timeout=20,
        ).json()[0]
        ids = m.get("clobTokenIds")
        if isinstance(ids, str):
            ids = json.loads(ids)
        token = str(ids[0])

        ob = client.get_order_book(token)
        best_bid = float(ob.bids[0].price) if ob and ob.bids else None
        best_ask = float(ob.asks[0].price) if ob and ob.asks else None

        orders = client.get_orders()
        live = [
            o
            for o in orders
            if str(o.get("status", "")).lower() in ("live", "open", "active")
            and str(o.get("asset_id") or o.get("token_id") or "") == token
        ]
        rows = []
        for o in live:
            rows.append(
                {
                    "price": float(o.get("price") or 0),
                    "shares": float(o.get("size") or o.get("original_size") or 0),
                    "status": str(o.get("status") or ""),
                    "order_id": str(o.get("id") or o.get("orderID") or ""),
                }
            )
        rows.sort(key=lambda x: x["price"], reverse=True)
        return {
            "ok": True,
            "token": token,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "live_count": len(rows),
            "live_orders": rows,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_accounts_runtime_snapshot(cfg: dict) -> list[dict[str, Any]]:
    """Per-key runtime summary: balance/effective/live orders/today trades/scoring orders."""
    account = cfg.get("account", {})
    host = str(cfg.get("rest_base_url", "https://clob.polymarket.com")).rstrip("/")
    chain_id = int(account.get("chain_id", 137))
    signature_type = int(account.get("signature_type", 0))
    funder = str(account.get("funder", "")).strip()

    keys = _parse_key_list(cfg)
    out = []

    tz_local = timezone(timedelta(hours=8))
    day_start = datetime.now(tz_local).replace(hour=0, minute=0, second=0, microsecond=0)
    day_start_ts = int(day_start.timestamp())

    for i, key in enumerate(keys, start=1):
        row = {
            "account": f"Key-{i}",
            "key": _mask_key(key),
            "engine": "运行中" if engine_running() else "未启动",
            "balance_usdc": None,
            "effective_usdc": None,
            "live_orders": 0,
            "today_trades": 0,
            "today_notional": 0.0,
            "today_scoring_orders": 0,
            "error": "",
        }
        try:
            kwargs = {"host": host, "chain_id": chain_id, "key": key, "signature_type": signature_type}
            if funder:
                kwargs["funder"] = funder
            client = ClobClient(**kwargs)
            client.set_api_creds(client.create_or_derive_api_creds())

            snap = _single_balance_allowance_snapshot(cfg, key)
            if snap.get("ok"):
                row["balance_usdc"] = snap.get("balance")
                row["effective_usdc"] = snap.get("effective")

            orders = client.get_orders()
            live = [o for o in orders if str(o.get("status", "")).lower() in ("live", "open", "active")]
            row["live_orders"] = len(live)

            # reward-relevance proxy: how many live orders are currently scoring
            scoring_cnt = 0
            for o in live[:50]:  # avoid huge loops
                oid = o.get("id") or o.get("orderID")
                if not oid:
                    continue
                try:
                    if client.is_order_scoring(oid):
                        scoring_cnt += 1
                except Exception:
                    pass
            row["today_scoring_orders"] = scoring_cnt

            trades = client.get_trades()
            if isinstance(trades, dict):
                trades = trades.get("data") or trades.get("trades") or []
            tcnt = 0
            tnot = 0.0
            for t in trades:
                try:
                    mt = int(str(t.get("match_time", "0")))
                except Exception:
                    mt = 0
                if mt < day_start_ts:
                    continue
                tcnt += 1
                try:
                    tnot += float(t.get("price", 0)) * float(t.get("size", 0))
                except Exception:
                    pass
            row["today_trades"] = tcnt
            row["today_notional"] = round(tnot, 4)
        except Exception as e:
            row["error"] = str(e)

        out.append(row)

    return out


st.set_page_config(page_title="Market Ops Console", layout="wide")
st.title("Market Ops Console")

cfg = ensure_defaults(load_config())
scan_cache = _auto_scan_if_due(cfg, every_hours=3)

# Global top-right notification center (not inside Engine tab)
notif_l, notif_r = st.columns([5, 1])
with notif_r:
    items = _load_notifications()
    unread = sum(1 for x in items if not x.get("read"))
    if st.button(f"🔔 消息({unread})", use_container_width=True):
        st.session_state["show_notifications"] = not bool(st.session_state.get("show_notifications", False))

if st.session_state.get("show_notifications", False):
    notis = _load_notifications()
    st.markdown("**通知中心**")
    if not notis:
        st.caption("暂无消息")
    else:
        latest = notis[:20]
        for i, n in enumerate(latest):
            st.write(f"[{n.get('ts')}] {n.get('title')}")
            payload = n.get("payload") or {}
            tokens = payload.get("tokens") or []
            items = payload.get("items") or []

            b1, b2 = st.columns(2)
            with b1:
                if st.button(f"事件明细 ({len(items) if items else len(tokens)})", key=f"detail_noti_{i}", use_container_width=True):
                    st.session_state[f"show_noti_detail_{i}"] = not bool(st.session_state.get(f"show_noti_detail_{i}", False))
            with b2:
                if tokens and st.button(f"一键添加 ({len(tokens)})", key=f"add_noti_{i}", use_container_width=True):
                    added = _append_tokens_to_markets(cfg, tokens)
                    st.success(f"已添加 {added} 个事件到市场列表")

            if st.session_state.get(f"show_noti_detail_{i}", False):
                if items:
                    st.dataframe(items[:50], use_container_width=True, hide_index=True)
                elif tokens:
                    st.code("\n".join(tokens[:80]), language="text")

        if st.button("全部标记已读"):
            for n in notis:
                n["read"] = True
            _save_notifications(notis)
            st.rerun()

pages = st.tabs(["Engine", "Config（表单）", "Rewards筛选"])

with pages[0]:
    st.subheader("Engine 运行控制")

    status = "🟢 运行中" if engine_running() else "⚪ 已停止"
    st.write(f"当前状态：{status}")

    env_key_present = bool(os.getenv("POLY_PRIVATE_KEY", "").strip())
    cfg_key = str(cfg.get("account", {}).get("private_key", "")).strip()
    cfg_key_present = bool(cfg_key and "REPLACE" not in cfg_key)
    if env_key_present:
        st.success("🔐 私钥来源：环境变量 POLY_PRIVATE_KEY（推荐）")
    elif cfg_key_present:
        st.warning("🔐 私钥来源：config.account.private_key（可用但不推荐）")
    else:
        st.error("🔐 未检测到私钥：请设置 POLY_PRIVATE_KEY 或填写 config.account.private_key")

    # show action result from previous rerun
    flash = st.session_state.pop("engine_flash", None)
    if flash:
        lvl = flash.get("level", "info")
        msg = flash.get("msg", "")
        if lvl == "success":
            st.success(msg)
        elif lvl == "warning":
            st.warning(msg)
        elif lvl == "error":
            st.error(msg)
        else:
            st.info(msg)

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("启动 Engine", use_container_width=True):
            st.session_state["engine_flash"] = {"level": "success", "msg": start_engine()}
            st.rerun()
    with c2:
        if st.button("停止 Engine", use_container_width=True):
            st.session_state["engine_flash"] = {"level": "warning", "msg": stop_engine()}
            st.rerun()
    with c3:
        if st.button("保存并重载引擎", use_container_width=True):
            save_config(cfg)
            st.session_state["engine_flash"] = {"level": "success", "msg": reload_engine()}
            st.rerun()

    st.markdown("---")
    st.subheader("🚨 风控急停")
    st.caption("执行 cancel_all（若私钥可用）并停止引擎")
    if st.button("🚨 紧急停止（Cancel All + Stop）", type="primary", use_container_width=True):
        msg = stop_engine()
        st.error(msg)

    st.markdown("---")
    st.subheader("Engine 进程详情")
    pinfo = get_engine_process_info()
    if pinfo.get("running"):
        st.success(f"进程运行中: {pinfo.get('name')} | PID={pinfo.get('pid')} | 内存={pinfo.get('memory')}")
    else:
        st.warning(f"进程未运行 | PID={pinfo.get('pid')}")

    if st.button("刷新 Key 运行状态/当日明细", use_container_width=True):
        st.session_state["runtime_snapshot"] = get_accounts_runtime_snapshot(cfg)

    rt = st.session_state.get("runtime_snapshot")
    if rt:
        st.markdown("**Key 维度状态（Key1 / Key2 / ...）**")

        key_opts = ["全部"] + [str(x.get("account", "")) for x in rt]
        selected_key = st.selectbox("查看账号", key_opts, index=0, key="runtime_key_filter")
        rt_view = rt if selected_key == "全部" else [x for x in rt if str(x.get("account", "")) == selected_key]

        # UI-friendly column aliases
        rows = []
        for x in rt_view:
            rows.append(
                {
                    "账号": x.get("account"),
                    "私钥": x.get("key"),
                    "引擎状态": x.get("engine"),
                    "可用USDC": x.get("effective_usdc"),
                    "挂单数": x.get("live_orders"),
                    "当日成交笔数": x.get("today_trades"),
                    "当日成交额": x.get("today_notional"),
                    "可计分挂单": x.get("today_scoring_orders"),
                    "错误": x.get("error"),
                }
            )

        st.dataframe(rows, use_container_width=True, hide_index=True)
        st.caption("说明：可计分挂单=当前 live 订单中可被 rewards 计分的数量（代理指标）。")

    with st.expander("查看 tasklist 原始输出"):
        st.code(pinfo.get("raw", ""), language="text")

    st.markdown("---")
    st.subheader("Engine 日志")
    c_log1, c_log2 = st.columns([1, 1])
    tail_lines = 50
    with c_log1:
        st.caption("默认展示日志末尾 50 行（需要更早日志可直接让我查）。")
    with c_log2:
        if st.button("刷新日志", use_container_width=True):
            st.rerun()

    log_source = "overnight_reprice.log" if overnight_running() else "engine.log"
    log_path = OVERNIGHT_LOG_PATH if log_source == "overnight_reprice.log" else LOG_PATH
    st.caption(f"当前日志源：{log_source}")

    if log_path.exists():
        txt = log_path.read_text(encoding="utf-8", errors="ignore")
        lines = txt.splitlines()

        show_key_only = st.checkbox(
            "仅显示关键状态（已重挂/守单中/挂单失败）",
            value=True,
            help="开启后会过滤普通噪声日志，只保留关键运行状态。",
        )
        if show_key_only:
            keys = [
                "STATE=POST",
                "STATE=REPRICE",
                "STATE=HOLD",
                "STATE=POST_FAIL",
                "success=True",
                "reprice",
                "hold",
                "post_err",
            ]
            lines = [ln for ln in lines if any(k in ln for k in keys)]

        display = "\n".join(lines[-int(tail_lines):])
        st.code(display if display else "(日志为空)", language="text", height=180)
    else:
        st.info("暂无日志")

    st.markdown("---")
    st.subheader("一键启动脚本")
    if st.button("生成 start_dashboard.bat", use_container_width=True):
        write_bat()
        st.success(f"已生成：{BAT_PATH}")

with pages[1]:
    st.subheader("参数表单（不用直接改 JSON）")

    with st.form("config_form"):
        st.markdown("**市场列表**")
        markets = cfg.get("markets", [])
        market_rows = []
        for i, m in enumerate(markets, 1):
            row = dict(m)
            row["序号"] = i
            market_rows.append(row)

        edited_markets = st.data_editor(
            market_rows,
            num_rows="dynamic",
            use_container_width=True,
            column_config={
                "序号": st.column_config.NumberColumn("序号", disabled=True, width="small"),
                "token_id": st.column_config.TextColumn("token_id（市场Token）"),
                "max_incentive_spread": st.column_config.NumberColumn("max_incentive_spread（奖励最大点差）", format="%.4f"),
                "price_tick": st.column_config.NumberColumn("price_tick（价格步长）", format="%.4f"),
                "min_distance_from_best_bid": st.column_config.NumberColumn("min_distance_from_best_bid（离best bid最小距离）", format="%.4f"),
                "quote_size": st.column_config.NumberColumn("quote_size（下单数量）"),
                "enabled": st.column_config.CheckboxColumn("enabled（启用）"),
            },
        )

        st.markdown("### URL/Slug 一键转 Token 并追加到 markets")
        market_inputs = st.text_area(
            "每行一个 Polymarket 市场 URL 或 slug",
            value="",
            placeholder="https://polymarket.com/event/xxxx\nwill-norway-win-the-2026-fifa-world-cup-893",
            height=100,
            key="market_url_inputs",
        )
        c_resolve, c_append = st.columns(2)
        with c_resolve:
            if st.form_submit_button("解析 URL/Slug", use_container_width=True):
                resolved = resolve_market_inputs(market_inputs, float(cfg["strategy"].get("default_quote_size", 100)))
                st.session_state["resolved_markets"] = resolved
                st.session_state["resolved_selected_tokens"] = [str(x.get("token_id", "")) for x in resolved]

        resolved_markets = st.session_state.get("resolved_markets", [])
        selected_tokens = st.session_state.get("resolved_selected_tokens", [])
        if resolved_markets:
            st.info(f"已解析 {len(resolved_markets)} 个市场，可勾选后追加到配置")
            show = [
                {
                    "selected": (r["token_id"] in selected_tokens) if selected_tokens else True,
                    "token_id": r["token_id"],
                    "event": r.get("_event", ""),
                    "slug": r.get("_slug", ""),
                    "reward_used": r.get("_reward", 0),
                    "clobDailyRate": r.get("_clobDailyRate", 0),
                    "umaReward": r.get("_umaReward", 0),
                    "volume24h": r.get("_volume24h", 0),
                    "max_incentive_spread": r["max_incentive_spread"],
                    "reward_source": "clobDailyRate>umaReward>fallback",
                }
                for r in resolved_markets
            ]
            edited_resolved = st.data_editor(
                show,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "selected": st.column_config.CheckboxColumn("选择"),
                    "reward_used": st.column_config.NumberColumn("reward_used", format="%.4f"),
                    "clobDailyRate": st.column_config.NumberColumn("clobDailyRate", format="%.4f"),
                    "umaReward": st.column_config.NumberColumn("umaReward", format="%.4f"),
                },
                key="resolved_editor",
            )
            st.session_state["resolved_selected_tokens"] = [
                str(row.get("token_id", "")) for row in edited_resolved if row.get("selected", False)
            ]

        with c_append:
            if st.form_submit_button("追加已选到 markets", use_container_width=True):
                resolved = st.session_state.get("resolved_markets", [])
                selected = set(st.session_state.get("resolved_selected_tokens", []))
                if not resolved:
                    st.warning("没有可追加的解析结果")
                elif not selected:
                    st.warning("请先勾选要追加的子项目")
                else:
                    existing = {str(x.get("token_id", "")) for x in cfg.get("markets", [])}
                    add_count = 0
                    for r in resolved:
                        if r["token_id"] not in selected:
                            continue
                        if r["token_id"] in existing:
                            continue
                        item = {k: v for k, v in r.items() if not str(k).startswith("_")}
                        cfg.setdefault("markets", []).append(item)
                        existing.add(r["token_id"])
                        add_count += 1
                    st.session_state["pending_append_count"] = add_count

        st.markdown("**策略参数**")
        requote_interval_ms = st.number_input(
            "Requote Interval (ms)（重报价间隔）",
            min_value=50,
            value=int(cfg["strategy"].get("requote_interval_ms", 300)),
            step=50,
            help="盘口变化后最短多久重发一次报价。太小易触发限频。",
        )
        min_order_size = st.number_input(
            "Min Order Size（最小下单数量）",
            min_value=0.0001,
            value=float(cfg["strategy"].get("min_order_size", 5)),
            step=1.0,
            help="低于该数量直接跳过，避免触发交易所最小单限制。",
        )
        post_only = st.checkbox(
            "Post Only（仅挂单，不吃单）",
            value=bool(cfg["strategy"].get("post_only", True)),
            help="开启后仅作为挂单进入订单簿，避免主动成交。",
        )

        st.markdown("**本金百分比区间（按风险档位）**")
        st.caption("基于本金总额：low 95-99%，mid 80-95%，high 50-70%（可调）")
        low_c1, low_c2 = st.columns(2)
        with low_c1:
            quote_balance_pct_min_low = st.number_input(
                "Low % Min",
                min_value=0.0,
                max_value=1.0,
                value=float(cfg["strategy"].get("quote_balance_pct_min_low", 0.95)),
                step=0.01,
                format="%.2f",
            )
        with low_c2:
            quote_balance_pct_max_low = st.number_input(
                "Low % Max",
                min_value=0.0,
                max_value=1.0,
                value=float(cfg["strategy"].get("quote_balance_pct_max_low", 0.99)),
                step=0.01,
                format="%.2f",
            )

        mid_c1, mid_c2 = st.columns(2)
        with mid_c1:
            quote_balance_pct_min_mid = st.number_input(
                "Mid % Min",
                min_value=0.0,
                max_value=1.0,
                value=float(cfg["strategy"].get("quote_balance_pct_min_mid", 0.80)),
                step=0.01,
                format="%.2f",
            )
        with mid_c2:
            quote_balance_pct_max_mid = st.number_input(
                "Mid % Max",
                min_value=0.0,
                max_value=1.0,
                value=float(cfg["strategy"].get("quote_balance_pct_max_mid", 0.95)),
                step=0.01,
                format="%.2f",
            )

        hi_c1, hi_c2 = st.columns(2)
        with hi_c1:
            quote_balance_pct_min_high = st.number_input(
                "High % Min",
                min_value=0.0,
                max_value=1.0,
                value=float(cfg["strategy"].get("quote_balance_pct_min_high", 0.50)),
                step=0.01,
                format="%.2f",
            )
        with hi_c2:
            quote_balance_pct_max_high = st.number_input(
                "High % Max",
                min_value=0.0,
                max_value=1.0,
                value=float(cfg["strategy"].get("quote_balance_pct_max_high", 0.70)),
                step=0.01,
                format="%.2f",
            )

        st.markdown("**风控参数**")
        cooldown = st.number_input(
            "Cooldown Seconds（熔断冷却秒数）",
            min_value=1,
            value=int(cfg["risk"].get("cooldown_seconds", 60)),
            help="触发熔断后暂停下单的时间。",
        )
        max_notional = st.number_input(
            "Max Notional per Order（单笔最大名义金额）",
            min_value=1.0,
            value=float(cfg["risk"].get("max_notional_usdc_per_order", 500)),
            step=10.0,
            help="每笔下单的价格×数量上限，超出会自动缩量。",
        )
        max_balance_fail_streak = st.number_input(
            "Max Balance Fail Streak（余额/授权连续失败阈值）",
            min_value=1,
            value=int(cfg["risk"].get("max_balance_fail_streak", 8)),
            help="连续出现 not enough balance/allowance 达到该次数后触发熔断。",
        )
        webhook = st.text_input(
            "Discord Webhook（告警回调，可选）",
            value=str(cfg["reporting"].get("discord_webhook", "")),
            help="用于发送熔断/汇总通知；留空则不推送。",
        )

        st.markdown("**执行调度（多市场/多账号）**")
        ex = cfg.get("execution", {})
        ex_c1, ex_c2, ex_c3 = st.columns(3)
        with ex_c1:
            ex_batch_size = st.number_input("批次大小", min_value=1, max_value=100, value=int(ex.get("batch_size", 8)))
        with ex_c2:
            ex_batch_interval = st.number_input("批次间隔(秒)", min_value=1, max_value=600, value=int(ex.get("batch_interval_sec", 30)))
        with ex_c3:
            ex_jitter = st.number_input("金额微调(±%)", min_value=0.0, max_value=0.5, value=float(ex.get("amount_jitter_pct", 0.08)), step=0.01, format="%.2f")

        ex_c4, ex_c5, ex_c6 = st.columns(3)
        with ex_c4:
            ex_rotate = st.checkbox("启用顺序轮换", value=bool(ex.get("enable_order_rotation", True)))
        with ex_c5:
            ex_rotate_min = st.number_input("轮换周期(分钟)", min_value=5, max_value=1440, value=int(ex.get("rotation_interval_min", 60)))
        with ex_c6:
            ex_skip_live = st.checkbox("live稳定单跳过", value=bool(ex.get("skip_stable_live_orders", True)))

        st.markdown("**代理池（Proxy Pool）**")
        pp_enabled = st.checkbox("启用代理池", value=bool(cfg.get("proxy_pool", {}).get("enabled", False)))
        pp_c1, pp_c2, pp_c3 = st.columns(3)
        with pp_c1:
            pp_use_ws = st.checkbox("WS 使用代理", value=bool(cfg.get("proxy_pool", {}).get("use_for_ws", True)))
        with pp_c2:
            pp_use_acc = st.checkbox("账号请求使用代理", value=bool(cfg.get("proxy_pool", {}).get("use_for_accounts", False)))
        with pp_c3:
            pp_max_ws = st.number_input(
                "每个代理最大 WS 数",
                min_value=1,
                max_value=10,
                value=int(cfg.get("proxy_pool", {}).get("max_ws_per_proxy", 3)),
            )

        pp_items = cfg.get("proxy_pool", {}).get("items", [])
        if not pp_items:
            pp_items = [{"name": "proxy-1", "url": "", "enabled": True, "weight": 1}]

        st.caption("可直接在下表 URL 列粘贴代理；支持 host:port:user:pass 自动转换。")
        edited_proxies = st.data_editor(
            pp_items,
            num_rows="dynamic",
            use_container_width=True,
            height=260,
            column_config={
                "name": st.column_config.TextColumn("名称", width="small"),
                "url": st.column_config.TextColumn("代理URL（支持 host:port:user:pass 或 http://user:pass@host:port）", width="large"),
                "enabled": st.column_config.CheckboxColumn("启用", width="small"),
                "weight": st.column_config.NumberColumn("权重", min_value=1, step=1, width="small"),
            },
        )

        submitted = st.form_submit_button("保存表单配置", use_container_width=True)

    if submitted:
        cfg["strategy"]["requote_interval_ms"] = int(requote_interval_ms)
        cfg["strategy"]["min_order_size"] = float(min_order_size)
        cfg["strategy"]["quote_size_mode"] = "balance_pct"
        cfg["strategy"]["quote_balance_pct_min_low"] = float(quote_balance_pct_min_low)
        cfg["strategy"]["quote_balance_pct_max_low"] = float(quote_balance_pct_max_low)
        cfg["strategy"]["quote_balance_pct_min_mid"] = float(quote_balance_pct_min_mid)
        cfg["strategy"]["quote_balance_pct_max_mid"] = float(quote_balance_pct_max_mid)
        cfg["strategy"]["quote_balance_pct_min_high"] = float(quote_balance_pct_min_high)
        cfg["strategy"]["quote_balance_pct_max_high"] = float(quote_balance_pct_max_high)
        cfg["strategy"]["post_only"] = bool(post_only)

        cfg["risk"]["cooldown_seconds"] = int(cooldown)
        cfg["risk"]["max_notional_usdc_per_order"] = float(max_notional)
        cfg["risk"]["max_balance_fail_streak"] = int(max_balance_fail_streak)

        cfg["reporting"]["discord_webhook"] = webhook.strip()

        normalized_proxies = []
        for p in (edited_proxies or []):
            row = dict(p)
            row["url"] = _normalize_proxy_url(str(row.get("url", "")))
            normalized_proxies.append(row)

        cfg["execution"] = {
            "batch_size": int(ex_batch_size),
            "batch_interval_sec": int(ex_batch_interval),
            "amount_jitter_pct": float(ex_jitter),
            "enable_order_rotation": bool(ex_rotate),
            "rotation_interval_min": int(ex_rotate_min),
            "skip_stable_live_orders": bool(ex_skip_live),
        }

        cfg["proxy_pool"] = {
            "enabled": bool(pp_enabled),
            "use_for_ws": bool(pp_use_ws),
            "use_for_accounts": bool(pp_use_acc),
            "max_ws_per_proxy": int(pp_max_ws),
            "items": normalized_proxies,
        }

        # merge appended markets (from URL/Slug resolver) with edited table
        pending_append = int(st.session_state.get("pending_append_count", 0) or 0)
        cleaned_markets = []
        for row in (edited_markets or []):
            r = dict(row)
            r.pop("序号", None)
            cleaned_markets.append(r)
        cfg["markets"] = cleaned_markets

        save_config(cfg)
        if pending_append > 0:
            st.success(f"配置已保存（并追加 {pending_append} 个市场）")
            st.session_state["pending_append_count"] = 0
        else:
            st.success("配置已保存")

    st.markdown("---")
    st.subheader("环境变量操作")
    st.code('setx POLY_PRIVATE_KEY "你的私钥"', language="powershell")
    st.caption("设置后请重开终端/重启面板进程再启动引擎。")

with pages[2]:
    st.subheader("Rewards 市场概览")
    c_rw1, c_rw2 = st.columns([1, 2])
    with c_rw1:
        if st.button("刷新 Rewards 概览", use_container_width=True):
            snap = fetch_rewards_overview()
            st.session_state["rewards_overview"] = snap
            if snap.get("ok"):
                stats = _load_rewards_stats()
                today = date.today().isoformat()
                stats[today] = {
                    "event_count": int(snap.get("event_count") or 0),
                    "total_rate_per_day": float(snap.get("total_rate_per_day") or 0.0),
                }
                _save_rewards_stats(stats)
    with c_rw2:
        st.caption("统计全站 sponsored rewards 市场数量与每日奖励总额（USDC/day）")

    rw = st.session_state.get("rewards_overview")
    if rw and rw.get("ok"):
        stats = _load_rewards_stats()
        today = date.today()
        yday = (today - timedelta(days=1)).isoformat()
        prev = stats.get(yday)
        cur_total = float(rw.get("total_rate_per_day") or 0.0)
        cur_count = int(rw.get("event_count") or 0)
        if prev:
            prev_total = float(prev.get("total_rate_per_day") or 0.0)
            delta = cur_total - prev_total
            pct = (delta / prev_total * 100.0) if prev_total else 0.0
            d_text = f"{delta:+.2f} ({pct:+.2f}%)"
        else:
            d_text = "N/A（暂无昨日快照）"

        r1, r2, r3 = st.columns(3)
        r1.metric("奖励事件总数", f"{cur_count}")
        r2.metric("每日奖励总额 (USDC/day)", f"{cur_total:,.2f}")
        r3.metric("较昨日变化", d_text)
    elif rw and not rw.get("ok"):
        st.warning(f"Rewards 概览读取失败：{rw.get('error')}")

    st.markdown("---")
    st.subheader("市场扫描（scan_markets.py）")
    col1, col2, col3 = st.columns(3)
    with col1:
        min_volume = st.number_input("min-volume", min_value=0, value=100000, step=10000)
    with col2:
        sort_by = st.selectbox("sort-by", ["score", "reward", "volume"], index=0)
    with col3:
        top_n = st.number_input("top", min_value=1, max_value=100, value=10)

    if st.button("执行扫描", use_container_width=True):
        out = run_scan(int(min_volume), sort_by, int(top_n))
        st.code(out, language="text")

    st.markdown("---")
    st.subheader("奖励候选池（按你的偏好过滤）")
    f1, f2, f3 = st.columns(3)
    with f1:
        f_min_reward = st.number_input("最低日奖励", min_value=0.0, value=100.0, step=10.0)
    with f2:
        f_min_vol = st.number_input("最低24h成交量", min_value=0.0, value=50000.0, step=10000.0)
    with f3:
        f_ex_h = st.number_input("排除临期（小时）", min_value=1, max_value=168, value=24)

    cex1, cex2 = st.columns(2)
    with cex1:
        ex_topic = st.checkbox("排除话题类（SPX当日/价格阈值/发帖数）", value=True)
    with cex2:
        ex_near = st.checkbox("排除临期事件", value=True)

    if st.button("生成候选池", use_container_width=True):
        rp_new = build_reward_candidates(
            min_reward=float(f_min_reward),
            min_volume=float(f_min_vol),
            exclude_hours=int(f_ex_h),
            exclude_topic=bool(ex_topic),
            exclude_near_expiry=bool(ex_near),
        )
        st.session_state["reward_pool"] = rp_new
        st.session_state["reward_pool_selected"] = [
            str(m.get("token_id", "")) for m in (rp_new.get("candidates") or []) if m.get("token_id")
        ]

    rp = st.session_state.get("reward_pool")
    if rp and rp.get("ok"):
        cands = rp.get("candidates") or []
        rej = rp.get("rejected") or []
        st.success(f"候选 {len(cands)} 个；排除 {len(rej)} 个")
        st.caption("如果你想看到之前约35个结果：关闭“排除话题类”和“排除临期事件”后再生成。")

        if "reward_pool_selected" not in st.session_state:
            st.session_state["reward_pool_selected"] = [str(m.get("token_id", "")) for m in cands if m.get("token_id")]

        show = []
        selected_set = set(st.session_state.get("reward_pool_selected", []))
        for i, m in enumerate(cands[:120], 1):
            tid = str(m.get("token_id") or "")
            show.append({
                "selected": (tid in selected_set) if selected_set else True,
                "rank": i,
                "risk": m.get("risk", "mid"),
                "reward": round(float(m.get("reward") or 0), 2),
                "vol24h": round(float(m.get("volume24h") or 0), 0),
                "chg24h": round(float(m.get("oneDayPriceChange") or 0), 4),
                "chg1h": round(float(m.get("oneHourPriceChange") or 0), 4),
                "stable_pen": round(float(m.get("stabilityPenalty") or 0), 4),
                "score": round(float(m.get("score") or 0), 2),
                "slug": m.get("slug"),
                "token_id": tid,
            })

        edited_pool = st.data_editor(
            show,
            use_container_width=True,
            hide_index=True,
            column_config={
                "selected": st.column_config.CheckboxColumn("选择"),
                "risk": st.column_config.TextColumn("风险等级"),
                "chg24h": st.column_config.NumberColumn("24h变化", format="%.4f"),
                "chg1h": st.column_config.NumberColumn("1h变化", format="%.4f"),
                "stable_pen": st.column_config.NumberColumn("稳定性惩罚", format="%.4f"),
            },
            key="reward_pool_editor",
        )
        st.session_state["reward_pool_selected"] = [
            str(row.get("token_id", "")) for row in edited_pool if row.get("selected", False)
        ]

        if st.button("一键添加到市场列表", use_container_width=True):
            selected = set(st.session_state.get("reward_pool_selected", []))
            if not selected:
                st.warning("请先勾选要添加的候选")
            else:
                existing = {str(x.get("token_id", "")) for x in cfg.get("markets", [])}
                add_count = 0
                skip_existing = 0
                skip_unselected = 0
                skip_invalid = 0

                for m in cands:
                    tid = str(m.get("token_id") or "").strip()
                    if not tid:
                        skip_invalid += 1
                        continue
                    if tid not in selected:
                        skip_unselected += 1
                        continue
                    if tid in existing:
                        skip_existing += 1
                        continue

                    cfg.setdefault("markets", []).append(
                        {
                            "token_id": tid,
                            "max_incentive_spread": float(m.get("maxIncentiveSpread") or 0.02),
                            "price_tick": 0.01,
                            "min_distance_from_best_bid": 0.01,
                            "quote_size": float(cfg.get("strategy", {}).get("default_quote_size", 100)),
                            "risk": str(m.get("risk") or "mid"),
                            "enabled": True,
                        }
                    )
                    existing.add(tid)
                    add_count += 1

                save_config(cfg)
                st.success(
                    f"新增 {add_count}；已存在跳过 {skip_existing}；未勾选跳过 {skip_unselected}；无效token {skip_invalid}"
                )

        with st.expander("查看被排除列表"):
            st.dataframe(rej[:300], use_container_width=True, hide_index=True)

st.caption(f"Workspace: {BASE_DIR}")
