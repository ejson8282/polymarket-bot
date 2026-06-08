import argparse
import json
import math
from datetime import datetime, timezone
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


def parse_ts(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            x = float(value)
            return x / 1000.0 if x > 10_000_000_000 else x
        s = str(value).strip()
        if not s:
            return None
        if s.isdigit():
            x = float(s)
            return x / 1000.0 if x > 10_000_000_000 else x
        s = s.replace('Z', '+00:00')
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def is_in_play_market(m: Dict[str, Any]) -> bool:
    # Only use actual game/event start time fields — NOT startDate/startDateIso,
    # which are market creation timestamps and would falsely mark all
    # non-sports markets as "in play".
    game_start = parse_ts(pick_first(m, ['gameStartTime', 'game_start_time']))
    if game_start is None:
        return False
    return datetime.now(timezone.utc).timestamp() >= game_start

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


def parse_token_ids(m: Dict[str, Any]) -> List[str]:
    ids = m.get("clobTokenIds")
    if isinstance(ids, str):
        try:
            arr = json.loads(ids)
            if isinstance(arr, list):
                return [str(x) for x in arr if str(x)]
        except Exception:
            return []
    if isinstance(ids, list):
        return [str(x) for x in ids if str(x)]
    single = str(pick_first(m, ["tokenId", "token_id"], "") or "")
    return [single] if single else []


def infer_tick_size(best_bid: float, best_ask: float) -> float:
    for px in (best_bid, best_ask):
        s = f"{px}"
        if "." in s and len(s.split(".", 1)[1].rstrip("0")) >= 3:
            return 0.001
    return 0.01


def normalize_market(m: Dict[str, Any]) -> Dict[str, Any]:
    volume = to_float(pick_first(m, ["volume24hr", "volume24h", "volume", "vol24h"], 0.0))
    spread_limit = to_float(pick_first(m, ["rewardsMaxSpread", "maxIncentiveSpread", "incentiveSpread", "spread"], 0.02), 0.02)
    best_bid = to_float(pick_first(m, ["bestBid", "bid", "topBid"], 0.0))
    best_ask = to_float(pick_first(m, ["bestAsk", "ask", "topAsk"], 0.0))

    reward_fields = extract_reward_fields(m)
    reward = reward_fields["reward"]
    token_ids = parse_token_ids(m)
    token_id = token_ids[0] if token_ids else ""
    paired_token_id = token_ids[1] if len(token_ids) > 1 else ""

    mid = (best_bid + best_ask) / 2 if (best_bid > 0 and best_ask > 0) else 0.0
    quoted_spread = (best_ask - best_bid) if (best_bid > 0 and best_ask > 0) else 0.0
    tick_size_hint = infer_tick_size(best_bid, best_ask)

    # Stability proxies (24h / 1h price change magnitude).
    # Lower absolute change => more stable daily curve => better LP candidate.
    one_day_change = to_float(pick_first(m, ["oneDayPriceChange", "dayPriceChange", "priceChange24h"], 0.0), 0.0)
    one_hour_change = to_float(pick_first(m, ["oneHourPriceChange", "hourPriceChange", "priceChange1h"], 0.0), 0.0)
    stability_penalty = abs(one_day_change) * 6.0 + abs(one_hour_change) * 10.0

    # ── Legacy single score (kept for backwards-compat) ──────────────────────
    reward_eff = reward / max(volume, 1.0)
    spread_penalty = quoted_spread if quoted_spread > 0 else max(spread_limit, 0.02)
    depth_penalty = 0.0 if mid > 0 else 0.01
    score = (reward_eff * 1e6) - (spread_penalty * 10.0) - (depth_penalty * 10.0) - (stability_penalty * 10.0)

    # ── Dimension 1: Reward Potential ─────────────────────────────────────────
    # clobDailyRate  : daily USDC rewards for this market
    # spread_limit   : wider = more price range to work in = easier to capture
    # rewardsMinSize : smaller = lower barrier to qualify
    clob_daily = reward_fields["clobDailyRate"]
    min_size   = max(reward_fields["rewardsMinSize"], 1.0)
    # sqrt(spread) dampens extreme spreads; divide by min_size for capital efficiency
    reward_score_raw = clob_daily * math.sqrt(max(spread_limit, 0.001)) / min_size

    # ── Dimension 2: Fill Risk ────────────────────────────────────────────────
    # Crowded + stable price = still OK to LP.
    # Fill risk is about PRICE MOVEMENT, not crowding.
    #
    # price_volatility : actual recent movement (day + hour) — primary factor (70%)
    # price_uncertainty: mid near 0.5 = undecided market — secondary factor (30%)
    price_uncertainty = 1.0 - abs(mid - 0.5) * 2.0 if mid > 0 else 1.0
    price_volatility  = min(abs(one_day_change) * 4.0 + abs(one_hour_change) * 8.0, 1.0)
    fill_risk_raw = 0.70 * price_volatility + 0.30 * price_uncertainty

    # ── Crowding indicator (informational only, not a filter) ─────────────────
    # Tight quoted_spread relative to incentive spread = many LPs competing.
    # Low = not crowded, High = crowded (reward may be diluted).
    if spread_limit > 0.001 and quoted_spread > 0:
        crowd_raw = 1.0 - min(quoted_spread / spread_limit, 1.0)
    else:
        crowd_raw = 0.5
    if crowd_raw < 0.33:
        crowd_level = "low"
    elif crowd_raw < 0.66:
        crowd_level = "mid"
    else:
        crowd_level = "high"

    # ── Dimension 3: Share Balance Score ────────────────────────────────────────
    # Favors markets where dual-side quoting is easy:
    #   - mid near 0.5 → balanced collateral (YES+NO ≈ $1 per share pair)
    #   - low rewardsMinSize → easier to meet minimum threshold
    #   - must have paired_token_id for dual-side to work
    if paired_token_id and mid > 0:
        # Balance factor: 1.0 at mid=0.5, drops toward 0 at extremes
        # Using 1 - 2*|mid-0.5| gives linear dropoff; square for sharper penalty
        balance = 1.0 - 2.0 * abs(mid - 0.5)
        balance_factor = max(balance, 0.0) ** 0.5  # sqrt to soften the curve
        # MinSize penalty: lower is better; use 200 as reference baseline
        min_size_factor = min(200.0 / max(min_size, 1.0), 1.0)
        share_balance_raw = balance_factor * min_size_factor
    else:
        share_balance_raw = 0.0

    # Raw scores are normalised to 0-100 percentile ranks by score_markets()
    # after all markets are collected. Store raw here; dashboard uses final values.

    open_interest = to_float(pick_first(m, ["openInterest", "open_interest"], 0.0))
    condition_id = str(pick_first(m, ["market", "conditionId", "condition_id"], ""))
    has_valid_book = best_bid > 0 and best_ask > 0 and best_ask >= best_bid
    is_placeholder_book = best_bid <= 0.02 and best_ask >= 0.98 if has_valid_book else False
    if not has_valid_book:
        book_integrity = "crossed_or_empty"
    elif is_placeholder_book:
        book_integrity = "placeholder"
    else:
        book_integrity = "healthy"

    # extract parent event slug for accurate URL construction
    market_slug = str(pick_first(m, ["slug"], ""))
    event_slug  = ""
    events_field = m.get("events")
    if isinstance(events_field, list) and events_field:
        event_slug = str(events_field[0].get("slug", ""))
    elif isinstance(events_field, str):
        try:
            import json as _json
            ev = _json.loads(events_field)
            if isinstance(ev, list) and ev:
                event_slug = str(ev[0].get("slug", ""))
        except Exception:
            pass

    if event_slug:
        market_url = f"https://polymarket.com/event/{event_slug}/{market_slug}"
    else:
        market_url = f"https://polymarket.com/event/{market_slug}"

    # Only use actual game/event start time fields — NOT startDate/startDateIso,
    # which are market creation timestamps and would falsely mark non-sports
    # markets as having a "game start" time.
    game_start_ts = parse_ts(pick_first(m, ["gameStartTime", "game_start_time"]))

    return {
        "id": str(pick_first(m, ["id", "marketId", "slug"], "")),
        "question": str(pick_first(m, ["question", "title", "name"], "")),
        "slug": market_slug,
        "market_url": market_url,
        "token_id": token_id,
        "paired_token_id": paired_token_id,
        "condition_id": condition_id,
        "volume24h": volume,
        "openInterest": open_interest,
        "reward": reward,
        "umaReward": reward_fields["umaReward"],
        "clobDailyRate": reward_fields["clobDailyRate"],
        "rewardsMinSize": reward_fields["rewardsMinSize"],
        "maxIncentiveSpread": spread_limit,
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "mid": mid,
        "quotedSpread": quoted_spread,
        "tickSizeHint": tick_size_hint,
        "bookIntegrity": book_integrity,
        "oneDayPriceChange": one_day_change,
        "oneHourPriceChange": one_hour_change,
        "stabilityPenalty": stability_penalty,
        "score": score,
        # dimension scores (raw, pre-normalisation)
        "_reward_score_raw": reward_score_raw,
        "_fill_risk_raw": fill_risk_raw,
        "_share_balance_raw": share_balance_raw,
        # final 0-100 values filled in by score_markets()
        "reward_score": 0.0,
        "fill_risk": 0.0,
        "share_balance": 0.0,
        "quadrant": "",
        "crowd": crowd_level,
        "gameStartTs": game_start_ts,
        "isInPlay": is_in_play_market(m),
    }


def score_markets(markets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalise raw scores to 0-100 percentile ranks and assign quadrant labels."""
    if not markets:
        return markets

    raws_r = [m["_reward_score_raw"] for m in markets]
    raws_f = [m["_fill_risk_raw"]    for m in markets]
    raws_s = [m["_share_balance_raw"] for m in markets]

    min_r, max_r = min(raws_r), max(raws_r)
    min_f, max_f = min(raws_f), max(raws_f)
    min_s, max_s = min(raws_s), max(raws_s)

    def norm(v: float, lo: float, hi: float) -> float:
        if hi == lo:
            return 50.0
        return round((v - lo) / (hi - lo) * 100, 1)

    for m in markets:
        r = norm(m["_reward_score_raw"], min_r, max_r)
        f = norm(m["_fill_risk_raw"],    min_f, max_f)
        s = norm(m["_share_balance_raw"], min_s, max_s)
        m["reward_score"]   = r
        m["fill_risk"]      = f
        m["share_balance"]  = s
        # Quadrant: reward >= 50 is "high", fill_risk < 50 is "safe"
        if r >= 50 and f < 50:
            m["quadrant"] = "A: high reward, low risk"
        elif r >= 50 and f >= 50:
            m["quadrant"] = "B: high reward, high risk"
        elif r < 50 and f < 50:
            m["quadrant"] = "C: low reward, low risk"
        else:
            m["quadrant"] = "D: low reward, high risk"

    return markets


def fetch_markets(limit: int = 5000) -> List[Dict[str, Any]]:
    """Fetch active markets with pagination from Gamma.

    Stops on empty page, short page, SSL error, or when limit is reached.
    """
    out: List[Dict[str, Any]] = []
    offset = 0
    page_size = 500

    for _ in range(20):  # max 20 pages = 10000 markets, enough for any scan
        if len(out) >= limit:
            break
        params = {
            "limit": page_size,
            "offset": offset,
            "active": "true",
            "closed": "false",
            "archived": "false",
            "order": "volume24hr",   # highest volume first — ensures important markets appear early
            "ascending": "false",
        }
        try:
            r = requests.get(GAMMA_URL, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception:
            # SSL error or network issue — return what we have so far
            break
        if isinstance(data, dict):
            data = data.get("data") or data.get("markets") or []
        if not isinstance(data, list) or not data:
            break

        out.extend(data)
        if len(data) < page_size:
            break  # last page
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


CLOB_BOOK_URL = "https://clob.polymarket.com/book"


def fetch_bid_depth(token_id: str, timeout: float = 8.0) -> float:
    """Fetch the total bid-side notional (USDC) from the CLOB order book.

    Returns sum(price * size) for all bid levels, or 0.0 on failure.
    """
    if not token_id:
        return 0.0
    try:
        r = requests.get(CLOB_BOOK_URL, params={"token_id": token_id}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        bids = data.get("bids") or []
        total = 0.0
        for level in bids:
            price = to_float(level.get("price"), 0.0)
            size = to_float(level.get("size"), 0.0)
            total += price * size
        return total
    except Exception:
        return 0.0


def enrich_book_depth(
    markets: List[Dict[str, Any]],
    min_bid_depth: float = 0.0,
) -> List[Dict[str, Any]]:
    """Fetch bid-side depth for each market and filter by minimum.

    Adds 'bidDepth' field to each market dict.
    """
    import sys
    result = []
    for idx, m in enumerate(markets):
        tid = m.get("token_id", "")
        depth = fetch_bid_depth(tid)
        m["bidDepth"] = round(depth, 2)
        slug = m.get("slug", "")[:30]
        print(
            f"  [{idx + 1}/{len(markets)}] {slug:<30s} bid_depth=${depth:>12,.2f}"
            f"{'  ✗ SKIP' if min_bid_depth > 0 and depth < min_bid_depth else ''}",
            file=sys.stderr, flush=True,
        )
        if min_bid_depth > 0 and depth < min_bid_depth:
            continue
        result.append(m)
    return result


def main() -> None:
    ap = argparse.ArgumentParser(description="Scan Polymarket markets and rank candidates.")
    ap.add_argument("--min-volume", type=float, default=0, help="Minimum 24h volume filter (default: 0 = no filter)")
    ap.add_argument("--min-reward", type=float, default=0, help="Minimum daily reward ($) filter")
    ap.add_argument("--max-reward", type=float, default=0, help="Maximum daily reward ($) filter (0 = no limit)")
    ap.add_argument("--min-spread", type=float, default=0, help="Minimum maxIncentiveSpread filter (0 = no filter)")
    ap.add_argument("--max-spread", type=float, default=0, help="Maximum maxIncentiveSpread filter (0 = no limit)")
    ap.add_argument("--min-bid-depth", type=float, default=0, help="Minimum bid-side book depth in USDC (0 = no filter)")
    ap.add_argument("--sort-by", choices=["reward", "volume", "score", "reward_score"], default="reward")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--market", action="append", default=[], help="Market URL or slug; repeatable")
    ap.add_argument("--json", action="store_true", help="Output full results as JSON (for dashboard)")
    args = ap.parse_args()

    raw = fetch_markets()
    normalized = score_markets([normalize_market(m) for m in raw])

    # Mode A: resolve specific URLs/slugs to token ids
    if args.market:
        print("Resolve market URL/slug -> token_id\n")
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
                f"  reward_score={found['reward_score']:.1f}  fill_risk={found['fill_risk']:.1f}  [{found['quadrant']}]\n"
            )
        return

    # Mode B: ranking scan
    markets = [
        m for m in normalized
        if m["volume24h"] >= args.min_volume
        and m["reward"] >= args.min_reward
        and (args.max_reward <= 0 or m["reward"] <= args.max_reward)
        and (args.min_spread <= 0 or m["maxIncentiveSpread"] >= args.min_spread)
        and (args.max_spread <= 0 or m["maxIncentiveSpread"] <= args.max_spread)
        and not m.get("isInPlay", False)
    ]

    key_map = {
        "reward":       lambda x: x["reward"],
        "volume":       lambda x: x["volume24h"],
        "score":        lambda x: x["score"],
        "reward_score": lambda x: x["reward_score"] - x["fill_risk"] * 0.3,
    }
    markets.sort(key=key_map[args.sort_by], reverse=True)
    topn = markets[: args.top]

    # ── Book depth filter: fetch CLOB order book for shortlisted markets ──
    min_depth = args.min_bid_depth
    if min_depth > 0:
        import sys
        print(f"\nFetching bid-side depth for {len(topn)} candidates (min ${min_depth:,.0f}) ...", file=sys.stderr, flush=True)
        topn = enrich_book_depth(topn, min_bid_depth=min_depth)
        print(f"Passed depth filter: {len(topn)} markets\n", file=sys.stderr, flush=True)
    else:
        # Still enrich with depth info for display, but don't filter
        import sys
        print(f"\nFetching bid-side depth for {len(topn)} candidates ...", file=sys.stderr, flush=True)
        topn = enrich_book_depth(topn, min_bid_depth=0)

    # JSON mode: used by dashboard scatter plot
    if args.json:
        safe = [{k: v for k, v in m.items() if not k.startswith("_")} for m in topn]
        print(json.dumps(safe))
        return

    # Text mode
    depth_label = f", min_bid_depth=${min_depth:,.0f}" if min_depth > 0 else ""
    print(f"Found {len(markets)} markets (min_volume={args.min_volume:.0f}{depth_label}), showing top {len(topn)} by {args.sort_by}\n")
    for i, m in enumerate(topn, 1):
        q = (m["question"][:90] + "...") if len(m["question"]) > 93 else m["question"]
        bid_depth = m.get("bidDepth", 0.0)
        print(
            f"{i:02d}. reward_score={m['reward_score']:.1f} fill_risk={m['fill_risk']:.1f} [{m['quadrant'][0]}] | "
            f"reward={m['reward']:.2f} | vol24h={m['volume24h']:.0f} | "
            f"bid_depth=${bid_depth:,.0f} | "
            f"inc_spread={m['maxIncentiveSpread']:.4f}\n"
            f"    token_id={m['token_id']} slug={m['slug']}\n"
            f"    {q}\n"
        )


if __name__ == "__main__":
    main()
