"""One-shot manual rescan + merge into config_1/2.json.

Purpose: after changing REWARD_MIN_DAILY_USD (auto_curator.py), trigger an
immediate add of qualifying markets without waiting for the engine's next
15-minute scan. Writes the same schema auto_curator uses.
"""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

GAMMA_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK = "https://clob.polymarket.com/book"

APPROVED = (
    "epl", "premier league", "la liga", "bundesliga", "serie a", "ligue 1",
    "champions league", "europa league", "conference league",
    "mls", "eredivisie", "primeira liga", "copa libertadores", "sudamericana",
    "championship", "efl",
    "world cup", "copa america", "euros", "euro 2024", "euro 2028",
    "nations league", "fifa", "qualifiers", "soccer", "football",
    "nba", "wnba", "ncaab", "ncaa basketball", "college basketball",
    "euroleague", "eurobasket", "fiba",
    "nfl", "ncaaf", "ncaa football", "college football", "cfp", "super bowl",
    "atp", "wta", "tennis", "grand slam", "us open", "wimbledon",
    "french open", "australian open", "roland garros",
    "mlb", "world series", "alcs", "nlcs", "nhl", "stanley cup",
    "ufc", "mma", "boxing", "bellator", "pfl",
    "cricket", "ipl", "t20", "test cricket", "icc",
    "rugby", "six nations", "world rugby",
    "pga", "liv golf", "masters", "us open golf", "the open", "ryder cup",
    "f1", "formula 1", "formula one", "nascar", "motogp", "indycar",
    "olympics", "summer olympics", "winter olympics",
    "esports", "cs2", "counter-strike", "league of legends", "lol",
    "dota", "valorant", "overwatch",
    "afl", "aussie rules", "darts", "snooker", "cycling",
)
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

REWARD_MIN = 100          # match auto_curator.REWARD_MIN_DAILY_USD
DEPTH_MIN = 100_000        # match auto_curator.DEPTH_MIN_BID_NOTIONAL_USD
MIN_HOURS_TO_START = 3.0   # match auto_curator.MIN_TIME_TO_START_SEC / 3600

MAKER_DIR = Path(__file__).resolve().parent.parent / "platforms/polymarket/maker"


def fetch_all_markets():
    out, offset = [], 0
    for _ in range(20):
        data = None
        for attempt in range(3):
            try:
                r = requests.get(GAMMA_URL, params={
                    "limit": 500, "offset": offset,
                    "active": "true", "closed": "false", "archived": "false",
                    "order": "volume24hr", "ascending": "false",
                    "include_tag": "true",
                }, timeout=20)
                data = r.json()
                break
            except Exception as e:
                print(f"  offset={offset} attempt {attempt+1}/3 failed: {type(e).__name__}")
                time.sleep(2 * (attempt + 1))
        if not data:
            print(f"  offset={offset} giving up after 3 retries — proceeding with {len(out)} so far")
            break
        out.extend(data)
        if len(data) < 500:
            break
        offset += len(data)
        time.sleep(0.3)  # polite gap between paginated calls
    return out


def parse_market_ts(market):
    raw = market.get("gameStartTime") or market.get("game_start_time")
    if not raw:
        return None
    try:
        if isinstance(raw, str):
            s = raw.replace("Z", "+00:00").replace(" ", "T", 1)
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
    except Exception:
        return None
    return None


def is_approved_league(tags):
    for t in tags or []:
        if not isinstance(t, dict):
            continue
        label = str(t.get("label", "")).lower()
        if any(a in label for a in APPROVED):
            return label
    return None


def prefilter(market):
    tags = market.get("tags") or []
    lg = is_approved_league(tags)
    if not lg:
        return None, "not_sport"
    slug = str(market.get("slug") or "")
    if not DATE_RE.search(slug):
        return None, "slug_no_date"
    ts = parse_market_ts(market)
    if not ts:
        return None, "no_start_ts"
    hours = (ts - time.time()) / 3600
    if hours < MIN_HOURS_TO_START:
        return None, f"too_soon:{hours:.1f}h"
    daily = 0.0
    for item in market.get("clobRewards") or []:
        if isinstance(item, dict):
            try:
                daily += float(item.get("rewardsDailyRate") or 0)
            except Exception:
                pass
    if daily < REWARD_MIN:
        return None, f"reward=${daily:.0f}<{REWARD_MIN}"
    try:
        ids = market.get("clobTokenIds")
        if isinstance(ids, str):
            ids = json.loads(ids)
        if not ids or len(ids) < 2:
            return None, "no_token_ids"
        yes_tok, no_tok = str(ids[0]), str(ids[1])
    except Exception:
        return None, "bad_tokens"
    spread = (market.get("rewardsMaxSpread")
              or market.get("maxIncentiveSpread") or 3.0)
    try:
        spread = float(spread)
    except Exception:
        spread = 3.0
    return {
        "slug": slug, "league": lg,
        "yes_token": yes_tok, "no_token": no_tok,
        "daily": daily, "hours": hours, "spread": spread,
    }, None


def fetch_bid_depth(token_id):
    try:
        r = requests.get(CLOB_BOOK, params={"token_id": token_id}, timeout=8)
        b = r.json()
        return sum(float(x.get("price", 0)) * float(x.get("size", 0))
                   for x in b.get("bids", []))
    except Exception:
        return 0.0


def merge_into_config(cfg_path, passed):
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    existing = {m.get("token_id") for m in cfg.get("markets", []) if m.get("token_id")}
    added = []
    for p in passed:
        if p["yes_token"] in existing:
            continue
        cfg["markets"].append({
            "token_id": p["yes_token"],
            "side": "YES",
            "max_incentive_spread": round(p["spread"], 4),
            "price_tick": 0.01,
            "min_distance_from_best_bid": 0.01,
            "quote_size": 100.0,
            "risk": "mid",
            "enabled": True,
            "paired_token_id": p["no_token"],
        })
        added.append(p["slug"])
    if added:
        cfg_path.write_text(
            json.dumps(cfg, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return added, len(cfg["markets"])


def main():
    print("Fetching markets from gamma …")
    all_markets = fetch_all_markets()
    print(f"  fetched {len(all_markets)} markets")

    candidates, reject = [], {}
    for m in all_markets:
        c, reason = prefilter(m)
        if c:
            candidates.append(c)
        else:
            key = reason.split(":", 1)[0] if reason else "other"
            reject[key] = reject.get(key, 0) + 1

    candidates.sort(key=lambda x: -x["daily"])
    print(f"  pre-depth candidates: {len(candidates)}")
    print(f"  rejected: {reject}")

    passed = []
    for i, c in enumerate(candidates, 1):
        depth = fetch_bid_depth(c["yes_token"])
        c["depth"] = depth
        if depth >= DEPTH_MIN:
            passed.append(c)
        if i % 10 == 0:
            print(f"  depth-checked {i}/{len(candidates)}  passed={len(passed)}")

    print(f"\n=== Passed reward+depth: {len(passed)} ===")
    for p in passed:
        print(f"  ${p['daily']:5.0f}/d  depth=${p['depth']/1000:5.0f}k  "
              f"+{p['hours']:5.1f}h  [{p['league'][:12]:12}]  {p['slug'][:55]}")

    print()
    for cfg_idx in (1, 2):
        path = MAKER_DIR / f"config_{cfg_idx}.json"
        if not path.exists():
            print(f"config_{cfg_idx}.json: not present, skipped")
            continue
        added, total = merge_into_config(path, passed)
        print(f"config_{cfg_idx}.json: +{len(added)} new  (total markets: {total})")

    base = MAKER_DIR / "config.json"
    if base.exists():
        added, total = merge_into_config(base, passed)
        print(f"config.json (base): +{len(added)} new  (total markets: {total})")


if __name__ == "__main__":
    main()
