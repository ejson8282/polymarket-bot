"""Read-only LP reward opportunity model.

The observer estimates how efficiently a small, balanced YES/NO quote could
compete for a market's daily liquidity reward. It never signs, posts, or
cancels orders. Estimates are deliberately labelled as estimates because the
public order book does not identify which levels belong to the same maker.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


MODEL_VERSION = 2
DEFAULT_PROBE_BUDGET_USDC = Decimal("100")
DEFAULT_CANDIDATE_LIMIT = 100
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
HISTORY_RETENTION_SECONDS = 7 * 24 * 60 * 60
HISTORY_SAMPLES_PER_MARKET = 288
_SPORTS_SLUG_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


def _timestamp(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


def _market_phase(market: Dict[str, Any], now_ts: Optional[float] = None) -> Tuple[str, Optional[float]]:
    if not _is_sports_market(market):
        return "normal", None
    start_ts = _timestamp(
        market.get("gameStartTime")
        or market.get("game_start_time")
        or market.get("game_start_ts")
    )
    if start_ts is None:
        return "pregame", None
    return ("live" if (now_ts or time.time()) >= start_ts else "pregame"), start_ts


def _token_ids(market: Dict[str, Any]) -> List[str]:
    raw = market.get("clobTokenIds")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            return []
    if not isinstance(raw, list):
        return []
    return [str(value) for value in raw if str(value)]


def _daily_reward(market: Dict[str, Any]) -> Decimal:
    total = Decimal("0")
    rewards = market.get("clobRewards")
    if isinstance(rewards, list):
        for row in rewards:
            if isinstance(row, dict):
                total += _decimal(row.get("rewardsDailyRate"))
    if total > 0:
        return total
    for key in ("liquidityReward", "dailyReward", "rewardsDaily", "reward"):
        value = _decimal(market.get(key))
        if value > 0:
            return value
    return Decimal("0")


def _reward_spread_decimal(market: Dict[str, Any]) -> Decimal:
    """Return max qualifying distance as a 0-1 price distance.

    Gamma normally exposes this in cents (for example 4.5 means $0.045), while
    older fixtures sometimes use decimal price units.
    """

    raw = _decimal(
        market.get("rewardsMaxSpread")
        or market.get("maxIncentiveSpread")
        or market.get("rewardsMaxSpreadCent")
    )
    if raw <= 0:
        return Decimal("0")
    return raw / Decimal("100") if raw >= Decimal("1") else raw


def _is_sports_market(market: Dict[str, Any]) -> bool:
    category = str(market.get("category") or "").strip().lower()
    if category in {"sports", "esports"}:
        return True
    slug = str(market.get("slug") or "")
    has_start = bool(
        market.get("gameStartTime")
        or market.get("game_start_time")
        or market.get("gameStartTs")
    )
    return has_start and bool(_SPORTS_SLUG_DATE_RE.search(slug))


def _event_slug(market: Dict[str, Any]) -> str:
    for key in ("eventSlug", "event_slug"):
        value = str(market.get(key) or "").strip()
        if value:
            return value
    events = market.get("events")
    if isinstance(events, str):
        try:
            events = json.loads(events)
        except Exception:
            events = []
    if isinstance(events, list):
        for event in events:
            if not isinstance(event, dict):
                continue
            value = str(event.get("slug") or "").strip()
            if value:
                return value
    return ""


def _market_url(market: Dict[str, Any]) -> str:
    market_slug = str(market.get("slug") or "").strip()
    event_slug = _event_slug(market)
    if event_slug and market_slug:
        return f"https://polymarket.com/event/{event_slug}/{market_slug}"
    if event_slug:
        return f"https://polymarket.com/event/{event_slug}"
    return ""


def _book_levels(book: Optional[Dict[str, Any]], side: str) -> List[Tuple[Decimal, Decimal]]:
    if not isinstance(book, dict):
        return []
    levels: List[Tuple[Decimal, Decimal]] = []
    for row in book.get(side) or []:
        if not isinstance(row, dict):
            continue
        price = _decimal(row.get("price"))
        size = _decimal(row.get("size"))
        if Decimal("0") < price < Decimal("1") and size > 0:
            levels.append((price, size))
    levels.sort(key=lambda item: item[0], reverse=(side == "bids"))
    return levels


def _book_tick_size(book: Optional[Dict[str, Any]]) -> Optional[Decimal]:
    if not isinstance(book, dict):
        return None
    for key in ("tick_size", "tickSize", "minimum_tick_size", "minimumTickSize"):
        tick = _decimal(book.get(key), Decimal("-1"))
        if Decimal("0") < tick < Decimal("1"):
            return tick
    return None


def _book_summary(book: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    bids = _book_levels(book, "bids")
    asks = _book_levels(book, "asks")
    if not bids or not asks:
        return None
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    if best_ask < best_bid:
        return None
    return {
        "bids": bids,
        "asks": asks,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid": (best_bid + best_ask) / Decimal("2"),
        "tick_size": _book_tick_size(book),
    }


def _front_depth_metrics(
    yes: Dict[str, Any],
    no: Dict[str, Any],
    *,
    observed_at: float,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "front_depth_status": "unavailable",
        "front_depth_observed_at": round(observed_at, 3),
        "yes_tick_size": None,
        "no_tick_size": None,
        "yes_safe_quote": None,
        "no_safe_quote": None,
        "yes_front_bid_notional_usd": None,
        "no_front_bid_notional_usd": None,
        "min_front_bid_notional_usd": None,
        "yes_front_bid_levels": 0,
        "no_front_bid_levels": 0,
    }
    yes_tick = yes.get("tick_size")
    no_tick = no.get("tick_size")
    if not isinstance(yes_tick, Decimal) or not isinstance(no_tick, Decimal):
        result["front_depth_status"] = "missing_tick_size"
        return result
    result["yes_tick_size"] = round(float(yes_tick), 6)
    result["no_tick_size"] = round(float(no_tick), 6)
    if yes_tick != no_tick:
        result["front_depth_status"] = "tick_size_mismatch"
        return result
    if len(yes["bids"]) < 2 or len(no["bids"]) < 2:
        result["front_depth_status"] = "insufficient_bid_levels"
        return result

    yes_quote = yes["best_bid"] - yes_tick
    no_quote = no["best_bid"] - no_tick
    if yes_quote < yes_tick or no_quote < no_tick:
        result["front_depth_status"] = "no_safe_quote"
        return result

    yes_front = [(price, size) for price, size in yes["bids"] if price >= yes_quote]
    no_front = [(price, size) for price, size in no["bids"] if price >= no_quote]
    yes_notional = sum((price * size for price, size in yes_front), Decimal("0"))
    no_notional = sum((price * size for price, size in no_front), Decimal("0"))
    result.update(
        {
            "front_depth_status": "verified",
            "yes_safe_quote": round(float(yes_quote), 6),
            "no_safe_quote": round(float(no_quote), 6),
            "yes_front_bid_notional_usd": round(float(yes_notional), 2),
            "no_front_bid_notional_usd": round(float(no_notional), 2),
            "min_front_bid_notional_usd": round(float(min(yes_notional, no_notional)), 2),
            "yes_front_bid_levels": len(yes_front),
            "no_front_bid_levels": len(no_front),
        }
    )
    return result


def _distance_score(max_spread: Decimal, midpoint: Decimal, price: Decimal) -> Decimal:
    if max_spread <= 0:
        return Decimal("0")
    distance = abs(midpoint - price)
    if distance >= max_spread:
        return Decimal("0")
    ratio = (max_spread - distance) / max_spread
    return ratio * ratio


def _aggregate_bid_score(summary: Dict[str, Any], max_spread: Decimal) -> Decimal:
    midpoint = summary["mid"]
    return sum(
        (_distance_score(max_spread, midpoint, price) * size for price, size in summary["bids"]),
        Decimal("0"),
    )


def _fill_risk(market: Dict[str, Any], midpoint: Decimal) -> float:
    day = abs(float(_decimal(market.get("oneDayPriceChange"))))
    hour = abs(float(_decimal(market.get("oneHourPriceChange"))))
    volatility = min(day * 4.0 + hour * 8.0, 1.0)
    uncertainty = max(0.0, 1.0 - abs(float(midpoint) - 0.5) * 2.0)
    return round((volatility * 0.7 + uncertainty * 0.3) * 100.0, 1)


def _risk_label(score: float) -> str:
    if score < 35:
        return "low"
    if score < 65:
        return "medium"
    return "high"


def _rough_candidate(market: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if market.get("closed") or market.get("archived") or market.get("active") is False:
        return None
    reward = _daily_reward(market)
    token_ids = _token_ids(market)
    spread = _reward_spread_decimal(market)
    min_size = _decimal(market.get("rewardsMinSize"))
    if reward <= 0 or len(token_ids) < 2 or spread <= 0 or min_size <= 0:
        return None

    yes_bid = _decimal(market.get("bestBid"))
    yes_ask = _decimal(market.get("bestAsk"))
    if yes_bid <= 0 or yes_ask <= yes_bid:
        return None
    no_bid_estimate = max(Decimal("0.001"), Decimal("1") - yes_ask)
    minimum_capital = min_size * (yes_bid + no_bid_estimate)
    rough_efficiency = reward / max(minimum_capital, Decimal("1"))
    return {
        "market": market,
        "token_ids": token_ids[:2],
        "reward": reward,
        "spread": spread,
        "min_size": min_size,
        "rough_efficiency": rough_efficiency,
    }


def _observe_candidate(
    rough: Dict[str, Any],
    yes_book: Optional[Dict[str, Any]],
    no_book: Optional[Dict[str, Any]],
    probe_budget: Decimal,
) -> Optional[Dict[str, Any]]:
    market = rough["market"]
    yes = _book_summary(yes_book)
    no = _book_summary(no_book)
    if yes is None or no is None:
        return None

    spread = rough["spread"]
    yes_unit_score = _distance_score(spread, yes["mid"], yes["best_bid"])
    no_unit_score = _distance_score(spread, no["mid"], no["best_bid"])
    if yes_unit_score <= 0 or no_unit_score <= 0:
        return None

    pair_cost = yes["best_bid"] + no["best_bid"]
    if pair_cost <= 0:
        return None
    min_size = rough["min_size"]
    probe_shares = max(min_size, probe_budget / pair_cost)
    probe_capital = probe_shares * pair_cost

    own_yes_q = yes_unit_score * probe_shares
    own_no_q = no_unit_score * probe_shares
    own_q_min = min(own_yes_q, own_no_q)
    competition_q = min(
        _aggregate_bid_score(yes, spread),
        _aggregate_bid_score(no, spread),
    )
    estimated_share = (
        own_q_min / (competition_q + own_q_min)
        if own_q_min > 0 else Decimal("0")
    )
    estimated_share = min(Decimal("1"), max(Decimal("0"), estimated_share))
    daily_reward = rough["reward"]
    estimated_daily_gross = daily_reward * estimated_share
    gross_daily_roi = (
        estimated_daily_gross / probe_capital
        if probe_capital > 0 else Decimal("0")
    )
    midpoint = yes["mid"]
    fill_risk = _fill_risk(market, midpoint)
    market_phase, game_start_ts = _market_phase(market)
    front_depth = _front_depth_metrics(yes, no, observed_at=time.time())

    reasons: List[str] = []
    if estimated_share >= Decimal("0.5"):
        reasons.append("estimated_majority_share")
    elif estimated_share >= Decimal("0.2"):
        reasons.append("estimated_meaningful_share")
    else:
        reasons.append("estimated_crowded")
    if probe_capital > probe_budget * Decimal("1.05"):
        reasons.append("minimum_size_raises_capital")
    if fill_risk >= 65:
        reasons.append("high_fill_risk")
    elif fill_risk >= 35:
        reasons.append("medium_fill_risk")

    market_competitiveness = market.get("marketCompetitiveness")
    if market_competitiveness is None:
        market_competitiveness = market.get("market_competitiveness")

    return {
        "condition_id": str(
            market.get("conditionId")
            or market.get("condition_id")
            or market.get("market")
            or ""
        ),
        "question": str(market.get("question") or market.get("title") or ""),
        "slug": str(market.get("slug") or ""),
        "event_slug": _event_slug(market),
        "market_url": _market_url(market),
        "token_id": rough["token_ids"][0],
        "paired_token_id": rough["token_ids"][1],
        "market_type": "sports" if _is_sports_market(market) else "always_on",
        "market_phase": market_phase,
        "game_start_ts": game_start_ts,
        "seconds_to_start": (
            round(game_start_ts - time.time(), 1)
            if game_start_ts is not None
            else None
        ),
        "daily_reward_usd": round(float(daily_reward), 2),
        "rewards_min_size_shares": round(float(min_size), 4),
        "rewards_max_spread": round(float(spread), 6),
        "probe_budget_usd": round(float(probe_budget), 2),
        "probe_capital_usd": round(float(probe_capital), 2),
        "probe_shares_each_side": round(float(probe_shares), 4),
        "yes_quote": round(float(yes["best_bid"]), 4),
        "no_quote": round(float(no["best_bid"]), 4),
        "estimated_reward_share_pct": round(float(estimated_share * 100), 2),
        "estimated_daily_gross_usd": round(float(estimated_daily_gross), 2),
        "estimated_gross_daily_roi_pct": round(float(gross_daily_roi * 100), 2),
        "actual_reward_share_pct": None,
        "actual_daily_gross_usd": None,
        "fill_risk": fill_risk,
        "risk_label": _risk_label(fill_risk),
        "competition_score_estimate": round(float(competition_q), 4),
        "market_competitiveness": (
            round(float(_decimal(market_competitiveness)), 4)
            if market_competitiveness is not None else None
        ),
        "reasons": reasons,
        "status": "observe_only",
        "estimate_confidence": "directional",
        "model_version": MODEL_VERSION,
        **front_depth,
    }


def observe_reward_markets(
    markets: Iterable[Dict[str, Any]],
    fetch_book: Callable[[str], Optional[Dict[str, Any]]],
    *,
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT,
    probe_budget_usdc: Decimal = DEFAULT_PROBE_BUDGET_USDC,
    fetch_workers: int = 8,
) -> Dict[str, Any]:
    """Build a ranked, read-only opportunity list from active reward markets."""

    rough = [
        candidate
        for market in markets
        if (candidate := _rough_candidate(market)) is not None
    ]
    rough.sort(
        key=lambda row: (row["rough_efficiency"], row["reward"]),
        reverse=True,
    )
    selected = rough[: max(1, int(candidate_limit))]

    def load_pair(row: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        yes_token, no_token = row["token_ids"]
        return fetch_book(yes_token), fetch_book(no_token)

    with ThreadPoolExecutor(max_workers=max(1, int(fetch_workers))) as pool:
        books = list(pool.map(load_pair, selected))

    observed = [
        candidate
        for row, (yes_book, no_book) in zip(selected, books)
        if (candidate := _observe_candidate(
            row,
            yes_book,
            no_book,
            _decimal(probe_budget_usdc, DEFAULT_PROBE_BUDGET_USDC),
        )) is not None
    ]
    observed.sort(
        key=lambda row: (
            row["estimated_gross_daily_roi_pct"],
            row["estimated_daily_gross_usd"],
        ),
        reverse=True,
    )
    return {
        "mode": "observe_only",
        "model_version": MODEL_VERSION,
        "probe_budget_usd": round(float(_decimal(probe_budget_usdc)), 2),
        "rewarded_markets_seen": len(rough),
        "candidates_evaluated": len(selected),
        "candidates_ready": len(observed),
        "candidates": observed,
    }


def _fetch_json(url: str, params: Dict[str, Any], timeout: float) -> Any:
    request = Request(
        f"{url}?{urlencode(params)}",
        headers={
            "Accept": "application/json",
            "User-Agent": "polymarket-reward-observer/1.0",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_active_markets(
    *,
    timeout: float = 20.0,
    page_size: int = 100,
    max_pages: int = 100,
) -> List[Dict[str, Any]]:
    """Fetch every active Gamma market, ordered by recent volume."""

    markets: List[Dict[str, Any]] = []
    offset = 0
    for _ in range(max(1, int(max_pages))):
        try:
            payload = _fetch_json(
                GAMMA_MARKETS_URL,
                {
                    "limit": max(1, min(100, int(page_size))),
                    "offset": offset,
                    "active": "true",
                    "closed": "false",
                    "archived": "false",
                    "order": "volume24hr",
                    "ascending": "false",
                    "include_tag": "true",
                },
                timeout,
            )
        except Exception:
            if markets:
                break
            raise
        if isinstance(payload, dict):
            payload = payload.get("data") or payload.get("markets") or []
        if not isinstance(payload, list) or not payload:
            break
        rows = [row for row in payload if isinstance(row, dict)]
        markets.extend(rows)
        if len(payload) < page_size:
            break
        offset += len(payload)
    return markets


def fetch_public_book(
    token_id: str,
    *,
    timeout: float = 10.0,
) -> Optional[Dict[str, Any]]:
    try:
        payload = _fetch_json(
            CLOB_BOOK_URL,
            {"token_id": token_id},
            timeout,
        )
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _observer_settings(config_dir: Optional[Path]) -> Dict[str, Any]:
    settings: Dict[str, Any] = {}
    if config_dir is not None:
        for path in sorted(config_dir.glob("config_*.json")):
            try:
                config = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            curator = config.get("auto_curator")
            if not isinstance(curator, dict):
                continue
            observer = curator.get("reward_observer")
            if isinstance(observer, dict):
                settings = observer
                break
    return {
        "candidate_limit": max(
            1,
            min(100, int(settings.get("candidate_limit") or DEFAULT_CANDIDATE_LIMIT)),
        ),
        "probe_budget_usdc": _decimal(
            settings.get("probe_budget_usdc"),
            DEFAULT_PROBE_BUDGET_USDC,
        ),
    }


def _float_values(samples: List[Dict[str, Any]], key: str) -> List[float]:
    values: List[float] = []
    for sample in samples:
        try:
            values.append(float(sample[key]))
        except (KeyError, TypeError, ValueError):
            continue
    return values


def _stability_fields(
    samples: List[Dict[str, Any]],
    now_ts: float,
) -> Dict[str, Any]:
    if not samples:
        return {
            "observation_samples": 0,
            "observation_span_sec": 0,
            "stability_score": 0,
            "estimated_share_range_pp": None,
            "verification_status": "collecting",
        }
    first_ts = float(samples[0].get("ts") or now_ts)
    span = max(0, int(now_ts - first_ts))
    shares = _float_values(samples, "share")
    risks = _float_values(samples, "risk")
    share_range = max(shares) - min(shares) if shares else 100.0
    risk_range = max(risks) - min(risks) if risks else 100.0
    consistency = max(
        0.0,
        100.0 - min(70.0, share_range * 2.0) - min(20.0, risk_range * 0.5),
    )
    sample_coverage = min(1.0, len(samples) / 12.0)
    time_coverage = min(1.0, span / 3300.0)
    score = round(consistency * (0.4 + 0.6 * min(sample_coverage, time_coverage)))

    latest_risk = risks[-1] if risks else 100.0
    if len(samples) < 3:
        status = "collecting"
    elif len(samples) < 12 or span < 3300:
        status = "warming"
    elif latest_risk >= 65:
        status = "risk_high"
    elif share_range > 20:
        status = "unstable"
    elif span >= 12 * 60 * 60:
        status = "confirmed"
    else:
        status = "stable"
    return {
        "observation_samples": len(samples),
        "observation_span_sec": span,
        "stability_score": int(score),
        "estimated_share_range_pp": round(share_range, 2),
        "verification_status": status,
    }


def _apply_observation_history(
    data_dir: Path,
    state: Dict[str, Any],
    now_ts: float,
) -> None:
    history_path = data_dir / "reward_observer_history.json"
    try:
        history = json.loads(history_path.read_text(encoding="utf-8"))
    except Exception:
        history = {}
    markets = history.get("markets") if isinstance(history, dict) else {}
    if not isinstance(markets, dict):
        markets = {}

    cutoff = now_ts - HISTORY_RETENTION_SECONDS
    for condition_id, row in list(markets.items()):
        if not isinstance(row, dict):
            markets.pop(condition_id, None)
            continue
        samples = row.get("samples")
        if not isinstance(samples, list):
            markets.pop(condition_id, None)
            continue
        fresh: List[Dict[str, Any]] = []
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            try:
                sample_ts = float(sample.get("ts") or 0)
            except (TypeError, ValueError):
                continue
            if sample_ts >= cutoff:
                fresh.append(sample)
        if fresh:
            row["samples"] = fresh[-HISTORY_SAMPLES_PER_MARKET:]
        else:
            markets.pop(condition_id, None)

    for candidate in state.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        condition_id = str(candidate.get("condition_id") or "").strip().lower()
        if not condition_id:
            continue
        row = markets.setdefault(
            condition_id,
            {
                "question": candidate.get("question"),
                "slug": candidate.get("slug"),
                "samples": [],
            },
        )
        samples = row.get("samples")
        if not isinstance(samples, list):
            samples = []
        samples.append(
            {
                "ts": now_ts,
                "share": candidate.get("estimated_reward_share_pct"),
                "gross_roi": candidate.get("estimated_gross_daily_roi_pct"),
                "risk": candidate.get("fill_risk"),
                "capital": candidate.get("probe_capital_usd"),
                "yes_quote": candidate.get("yes_quote"),
                "no_quote": candidate.get("no_quote"),
                "front_depth_status": candidate.get("front_depth_status"),
                "min_front_bid_notional_usd": candidate.get("min_front_bid_notional_usd"),
            }
        )
        samples = samples[-HISTORY_SAMPLES_PER_MARKET:]
        row["samples"] = samples
        row["last_seen_at"] = now_ts
        stability = _stability_fields(samples, now_ts)
        candidate.update(stability)
        gross_roi = float(
            candidate.get("estimated_gross_daily_roi_pct") or 0
        )
        fill_risk = float(candidate.get("fill_risk") or 100)
        stability_ratio = float(stability["stability_score"]) / 100.0
        risk_ratio = max(0.0, 1.0 - fill_risk / 100.0)
        candidate["risk_adjusted_daily_roi_pct"] = round(
            gross_roi * stability_ratio * risk_ratio,
            2,
        )
        candidate["verification_recommended"] = bool(
            stability["verification_status"] in {"stable", "confirmed"}
            and fill_risk < 65
            and stability["stability_score"] >= 70
            and gross_roi > 0
        )

    history_payload = {
        "version": 1,
        "updated_at": now_ts,
        "retention_seconds": HISTORY_RETENTION_SECONDS,
        "samples_per_market": HISTORY_SAMPLES_PER_MARKET,
        "markets": markets,
    }
    tmp = history_path.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(history_payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    tmp.replace(history_path)


def refresh_observer_state(
    data_dir: Path,
    *,
    config_dir: Optional[Path] = None,
    fetch_markets: Callable[[], List[Dict[str, Any]]] = fetch_active_markets,
    fetch_book: Callable[[str], Optional[Dict[str, Any]]] = fetch_public_book,
) -> Dict[str, Any]:
    """Refresh the standalone read-only state used by the dashboard."""

    data_dir.mkdir(parents=True, exist_ok=True)
    settings = _observer_settings(config_dir)
    started = time.time()
    markets = fetch_markets()
    state = observe_reward_markets(
        markets,
        fetch_book,
        candidate_limit=int(settings["candidate_limit"]),
        probe_budget_usdc=settings["probe_budget_usdc"],
    )
    generated_at = time.time()
    _apply_observation_history(data_dir, state, generated_at)
    state.update(
        {
            "status": "ready",
            "generated_at": generated_at,
            "elapsed_sec": round(time.time() - started, 2),
            "markets_seen": len(markets),
            "source": "public_gamma_and_clob",
        }
    )
    output = data_dir / "reward_observer_state.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    tmp = output.with_suffix(".json.tmp")
    tmp.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    tmp.replace(output)
    return state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh the read-only Polymarket reward opportunity cache"
    )
    maker_dir = Path(__file__).resolve().parent
    parser.add_argument(
        "--config-dir",
        type=Path,
        default=maker_dir,
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=maker_dir.parent.parent.parent / "data",
    )
    return parser


def main() -> int:
    try:
        from .release_guard import verify_release
    except ImportError:
        from release_guard import verify_release

    verify_release(Path(__file__))
    args = _parser().parse_args()
    try:
        state = refresh_observer_state(
            args.data_dir,
            config_dir=args.config_dir,
        )
    except Exception as exc:
        print(
            f"[reward-observer] refresh failed: {type(exc).__name__}",
            flush=True,
        )
        return 1
    print(
        "[reward-observer] "
        f"markets={state.get('markets_seen')} "
        f"rewarded={state.get('rewarded_markets_seen')} "
        f"ready={state.get('candidates_ready')} "
        f"elapsed={state.get('elapsed_sec')}s",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
