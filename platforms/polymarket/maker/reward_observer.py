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
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen


MODEL_VERSION = 1
DEFAULT_PROBE_BUDGET_USDC = Decimal("100")
DEFAULT_CANDIDATE_LIMIT = 100
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
_SPORTS_SLUG_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def _decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if value is None or value == "":
            return default
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default


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
    }


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
        "market_url": (
            f"https://polymarket.com/event/{market.get('slug')}"
            if market.get("slug") else ""
        ),
        "token_id": rough["token_ids"][0],
        "paired_token_id": rough["token_ids"][1],
        "market_type": "sports" if _is_sports_market(market) else "always_on",
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


def refresh_observer_state(
    data_dir: Path,
    *,
    config_dir: Optional[Path] = None,
    fetch_markets: Callable[[], List[Dict[str, Any]]] = fetch_active_markets,
    fetch_book: Callable[[str], Optional[Dict[str, Any]]] = fetch_public_book,
) -> Dict[str, Any]:
    """Refresh the standalone read-only state used by the dashboard."""

    settings = _observer_settings(config_dir)
    started = time.time()
    markets = fetch_markets()
    state = observe_reward_markets(
        markets,
        fetch_book,
        candidate_limit=int(settings["candidate_limit"]),
        probe_budget_usdc=settings["probe_budget_usdc"],
    )
    state.update(
        {
            "status": "ready",
            "generated_at": time.time(),
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
