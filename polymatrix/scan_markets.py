import argparse
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import requests

GAMMA_URL = "https://gamma-api.polymarket.com/markets"


def to_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def pick_first(d: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def parse_slug(value: str) -> str:
    """Accept raw slug or full polymarket URL."""
    v = (value or "").strip()
    if not v:
        return ""
    if "polymarket.com" not in v:
        return v.strip("/")

    p = urlparse(v)
    parts = [x for x in p.path.split("/") if x]
    # Common patterns: /event/<slug>, /market/<slug>
    if len(parts) >= 2 and parts[0] in {"event", "market"}:
        return parts[1]
    if parts:
        return parts[-1]
    return ""


def extract_reward_fields(m: Dict[str, Any]) -> Dict[str, float]:
    # Gamma reward surfaces can include:
    # - umaReward (often static bounty-style value, e.g. 5)
    # - clobRewards[].rewardsDailyRate (actual LP daily reward rate)
    # We prefer daily-rate when available.
    uma_reward = to_float(m.get("umaReward"), 0.0)
    rewards_min_size = to_float(m.get("rewardsMinSize"), 0.0)
    rewards_max_spread = to_float(m.get("rewardsMaxSpread"), 0.0)

    clob_daily_rate = 0.0
    clob_rewards = m.get("clobRewards")
    if isinstance(clob_rewards, list):
        for item in clob_rewards:
            if isinstance(item, dict):
                clob_daily_rate += to_float(item.get("rewardsDailyRate"), 0.0)

    # Fallback aliases (older variants)
    reward_fallback = to_float(
        pick_first(m, ["liquidityReward", "reward", "dailyReward", "rewardsDaily", "incentive"], 0.0),
        0.0,
    )

    reward_value = clob_daily_rate if clob_daily_rate > 0 else (uma_reward if uma_reward > 0 else reward_fallback)

    return {
        "reward": reward_value,
        "umaReward": uma_reward,
        "clobDailyRate": clob_daily_rate,
        "rewardsMinSize": rewards_min_size,
        "rewardsMaxSpread": rewards_max_spread,
    }


def first_token_id(m: Dict[str, Any]) -> str:
    ids = m.get("clobTokenIds")
    if isinstance(ids, str):
        # Usually JSON string list
        try:
            import json

            arr = json.loads(ids)
            if isinstance(arr, list) and arr:
                return str(arr[0])
        except Exception:
            pass
    elif isinstance(ids, list) and ids:
        return str(ids[0])

    # fallback key variants
    return str(pick_first(m, ["tokenId", "token_id"], ""))


def normalize_market(m: Dict[str, Any]) -> Dict[str, Any]:
    volume = to_float(pick_first(m, ["volume24hr", "volume24h", "volume", "vol24h"], 0.0))
    spread_limit = to_float(pick_first(m, ["rewardsMaxSpread", "maxIncentiveSpread", "incentiveSpread", "spread"], 0.02), 0.02)
    best_bid = to_float(pick_first(m, ["bestBid", "bid", "topBid"], 0.0))
    best_ask = to_float(pick_first(m, ["bestAsk", "ask", "topAsk"], 0.0))

    reward_fields = extract_reward_fields(m)
    reward = reward_fields["reward"]

    mid = (best_bid + best_ask) / 2 if (best_bid > 0 and best_ask > 0) else 0.0
    quoted_spread = (best_ask - best_bid) if (best_bid > 0 and best_ask > 0) else 0.0

    # Stability proxies (24h / 1h price change magnitude).
    # Lower absolute change => more stable daily curve => better LP candidate.
    one_day_change = to_float(pick_first(m, ["oneDayPriceChange", "dayPriceChange", "priceChange24h"], 0.0), 0.0)
    one_hour_change = to_float(pick_first(m, ["oneHourPriceChange", "hourPriceChange", "priceChange1h"], 0.0), 0.0)
    stability_penalty = abs(one_day_change) * 6.0 + abs(one_hour_change) * 10.0

    # Scoring:
    # + reward density (reward over 24h volume)
    # + prefer tighter book and valid top-of-book
    # + prefer more stable recent price curve
    reward_eff = reward / max(volume, 1.0)
    spread_penalty = quoted_spread if quoted_spread > 0 else max(spread_limit, 0.02)
    depth_penalty = 0.0 if mid > 0 else 0.01
    score = (reward_eff * 1e6) - (spread_penalty * 10.0) - (depth_penalty * 10.0) - (stability_penalty * 10.0)

    return {
        "id": str(pick_first(m, ["id", "marketId", "slug"], "")),
        "question": str(pick_first(m, ["question", "title", "name"], "")),
        "slug": str(pick_first(m, ["slug"], "")),
        "token_id": first_token_id(m),
        "volume24h": volume,
        "reward": reward,
        "umaReward": reward_fields["umaReward"],
        "clobDailyRate": reward_fields["clobDailyRate"],
        "rewardsMinSize": reward_fields["rewardsMinSize"],
        "maxIncentiveSpread": spread_limit,
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "quotedSpread": quoted_spread,
        "oneDayPriceChange": one_day_change,
        "oneHourPriceChange": one_hour_change,
        "stabilityPenalty": stability_penalty,
        "score": score,
    }


def fetch_markets(limit: int = 1000) -> List[Dict[str, Any]]:
    """Fetch active markets with pagination from Gamma.

    Gamma supports offset pagination. We iterate until an empty page or short page.
    """
    out: List[Dict[str, Any]] = []
    offset = 0
    # Gamma effectively caps page size around 500; use 500 for reliable pagination.
    page_size = 500

    for _ in range(100):  # hard safety cap
        params = {
            "limit": page_size,
            "offset": offset,
            "active": "true",
            "closed": "false",
            "archived": "false",
        }
        r = requests.get(GAMMA_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict):
            data = data.get("data") or data.get("markets") or []
        if not isinstance(data, list) or not data:
            break

        out.extend(data)
        offset += len(data)

    return out


def fetch_market_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    """Direct lookup to avoid pagination misses."""
    try:
        r = requests.get(GAMMA_URL, params={"slug": slug, "limit": 5}, timeout=15)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list) and data:
            return normalize_market(data[0])
    except Exception:
        return None
    return None


def resolve_by_slug(markets: List[Dict[str, Any]], slug: str) -> Optional[Dict[str, Any]]:
    slug = slug.strip().lower()
    if not slug:
        return None

    # exact first
    for m in markets:
        if str(m.get("slug", "")).lower() == slug:
            return m
    # contains fallback
    for m in markets:
        s = str(m.get("slug", "")).lower()
        if slug in s or s in slug:
            return m

    # direct API fallback (handles not-in-page results)
    return fetch_market_by_slug(slug)


def main() -> None:
    ap = argparse.ArgumentParser(description="Scan Polymarket markets and rank candidates.")
    ap.add_argument("--min-volume", type=float, default=100000, help="Minimum 24h volume filter")
    ap.add_argument("--sort-by", choices=["reward", "volume", "score"], default="score")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--market", action="append", default=[], help="Market URL or slug; repeatable")
    args = ap.parse_args()

    raw = fetch_markets()
    normalized = [normalize_market(m) for m in raw]

    # Mode A: resolve specific URLs/slugs to token ids
    if args.market:
        print("Resolve market URL/slug -> token_id\n")
        by_slug = {m["slug"]: m for m in normalized if m.get("slug")}
        for item in args.market:
            slug = parse_slug(item)
            found = resolve_by_slug(normalized, slug)
            if not found:
                print(f"- input={item}\n  slug={slug}\n  status=NOT_FOUND\n")
                continue
            print(
                f"- input={item}\n"
                f"  slug={found['slug']}\n"
                f"  token_id={found['token_id']}\n"
                f"  reward={found['reward']:.2f} (clobDailyRate={found['clobDailyRate']:.2f}, umaReward={found['umaReward']:.2f})\n"
                f"  volume24h={found['volume24h']:.0f}\n"
            )
        return

    # Mode B: ranking scan
    markets = [m for m in normalized if m["volume24h"] >= args.min_volume]

    key_map = {
        "reward": lambda x: x["reward"],
        "volume": lambda x: x["volume24h"],
        "score": lambda x: x["score"],
    }
    markets.sort(key=key_map[args.sort_by], reverse=True)

    topn = markets[: args.top]
    print(f"Found {len(markets)} markets (min_volume={args.min_volume:.0f}), showing top {len(topn)} by {args.sort_by}\n")
    for i, m in enumerate(topn, 1):
        q = (m["question"][:90] + "...") if len(m["question"]) > 93 else m["question"]
        print(
            f"{i:02d}. score={m['score']:.3f} | reward={m['reward']:.2f} | vol24h={m['volume24h']:.0f} | "
            f"spread={m['quotedSpread']:.4f} | inc_spread={m['maxIncentiveSpread']:.4f}\n"
            f"    token_id={m['token_id']} slug={m['slug']}\n"
            f"    {q}\n"
        )


if __name__ == "__main__":
    main()
