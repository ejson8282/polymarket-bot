"""Read-only LP reward opportunity model.

The observer estimates how efficiently a small, balanced YES/NO quote could
compete for a market's daily liquidity reward. It never signs, posts, or
cancels orders. Estimates are deliberately labelled as estimates because the
public order book does not identify which levels belong to the same maker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen

try:
    from .stable_rotation_planner import (
        refresh_stable_rotation_proposal,
        write_blocked_stable_rotation_proposal,
    )
    from .account_profiles import parse_lp_account_profile
    from .reward_fast_lane import (
        forced_condition_ids as fast_lane_forced_condition_ids,
        update_fast_lane,
    )
    from .reward_shadow_allocator import write_shadow_budget
    from .quote_feasibility import (
        PairExecution,
        aggregate_bid_q,
        distance_score,
        evaluate_paired_quote,
        executable_quote_boundary,
    )
except ImportError:  # pragma: no cover - direct script execution
    from stable_rotation_planner import (
        refresh_stable_rotation_proposal,
        write_blocked_stable_rotation_proposal,
    )
    from account_profiles import parse_lp_account_profile
    from reward_fast_lane import (
        forced_condition_ids as fast_lane_forced_condition_ids,
        update_fast_lane,
    )
    from reward_shadow_allocator import write_shadow_budget
    from quote_feasibility import (
        PairExecution,
        aggregate_bid_q,
        distance_score,
        evaluate_paired_quote,
        executable_quote_boundary,
    )


MODEL_VERSION = 6
DEFAULT_PROBE_BUDGET_USDC = Decimal("100")
DEFAULT_CANDIDATE_LIMIT = 100
DEFAULT_LOWER_REWARD_RESERVE_RATIO = Decimal("0.25")
DEFAULT_MIN_ESTIMATED_DAILY_PAYOUT_USDC = Decimal("1")
DEFAULT_STABLE_MIN_FRONT_BID_NOTIONAL_USDC = Decimal("2000")
DEFAULT_STABLE_MIN_TIME_TO_END_SEC = 12 * 60 * 60
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
HISTORY_RETENTION_SECONDS = 7 * 24 * 60 * 60
HISTORY_SAMPLES_PER_MARKET = 7 * 24 * 12
ACCOUNT_STATE_MAX_AGE_SEC = 900
_SPORTS_SLUG_DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_WEATHER_MARKET_RE = re.compile(
    r"\btemperature\b|"
    r"\bweather\b|\brain(?:fall|y|ing)?\b|\bsnow(?:fall|y|ing)?\b|"
    r"\bprecipitation\b|\bhumidity\b|\bwind[-\s]+(?:speed|gust)\b|"
    r"\bdegrees?[-\s]+(?:celsius|fahrenheit)\b|\b(?:celsius|fahrenheit)\b|"
    r"\d\s*°\s*[cf]\b|"
    r"\bhurricane\b|\btyphoon\b|\btornado\b|\bheat[-\s]*wave\b|"
    r"\bcold[-\s]*wave\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ObserverAccountPolicy:
    account_index: int
    account_id: str
    available_usdc: Optional[Decimal]
    available_source: str
    min_distance_ticks: int
    min_order_size: Decimal
    budget_pct: Decimal
    max_quote_shares: Decimal
    max_notional_per_order: Decimal
    min_front_bid_notional_usdc: Decimal
    configured_tokens: frozenset[str]
    market_runtime: Mapping[str, Mapping[str, Any]]
    scoring_by_token: Mapping[str, Optional[bool]]
    scoring_sample_by_token: Mapping[str, str]
    reward_percentages: Mapping[str, Decimal]
    configured_market_refs: tuple[Mapping[str, Any], ...] = ()
    account_uid: str = ""
    host_id: str = ""


def _default_probe_policy(probe_budget: Decimal) -> ObserverAccountPolicy:
    return ObserverAccountPolicy(
        account_index=0,
        account_id="probe",
        available_usdc=max(Decimal("0"), probe_budget),
        available_source="probe_budget",
        min_distance_ticks=1,
        min_order_size=Decimal("5"),
        budget_pct=Decimal("1"),
        max_quote_shares=Decimal("0"),
        max_notional_per_order=Decimal("0"),
        min_front_bid_notional_usdc=DEFAULT_STABLE_MIN_FRONT_BID_NOTIONAL_USDC,
        configured_tokens=frozenset(),
        market_runtime={},
        scoring_by_token={},
        scoring_sample_by_token={},
        reward_percentages={},
    )


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


def _optional_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    return None


def _read_mapping(path: Path) -> Dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, ValueError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _account_uid_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _canonical_account_uid(config: Mapping[str, Any]) -> str:
    account = config.get("account")
    if not isinstance(account, Mapping):
        return ""
    funder = str(account.get("funder") or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]{40}", funder):
        return ""
    try:
        chain_id = int(account.get("chain_id", 137))
        signature_type = int(account.get("signature_type", 0))
    except (TypeError, ValueError):
        return ""
    return f"{chain_id}:{signature_type}:{funder}"


def _state_is_fresh(state: Mapping[str, Any], now_ts: float) -> bool:
    generated_at = _timestamp(state.get("generated_at") or state.get("ts"))
    return bool(
        generated_at is not None
        and -30 <= now_ts - generated_at <= ACCOUNT_STATE_MAX_AGE_SEC
    )


def _latest_scoring_sample(row: Mapping[str, Any]) -> Dict[str, Any] | None:
    observations = row.get("observations")
    if not isinstance(observations, list):
        return None
    observed = [
        item
        for item in observations
        if isinstance(item, Mapping)
        and item.get("status") == "observed"
        and isinstance(item.get("scoring"), bool)
        and _timestamp(item.get("observed_at")) is not None
    ]
    if not observed:
        return None
    latest = max(observed, key=lambda item: _timestamp(item.get("observed_at")) or 0)
    if latest.get("scoring") is not row.get("last_scoring"):
        return None
    return {
        "order_id": str(row.get("order_id") or "").strip(),
        "observed_at": round(_timestamp(latest.get("observed_at")) or 0, 6),
        "scoring": bool(latest["scoring"]),
    }


def _scoring_evidence_by_token(
    payload: Mapping[str, Any],
) -> tuple[Dict[str, Optional[bool]], Dict[str, str]]:
    grouped: Dict[str, List[tuple[Optional[bool], Dict[str, Any] | None]]] = {}
    orders = payload.get("orders")
    if not isinstance(orders, Mapping):
        return {}, {}
    for row in orders.values():
        if not isinstance(row, Mapping) or row.get("live") is not True:
            continue
        token_id = str(row.get("token_id") or "").strip()
        if not token_id:
            continue
        scoring = row.get("last_scoring")
        grouped.setdefault(token_id, []).append(
            (
                scoring if isinstance(scoring, bool) else None,
                _latest_scoring_sample(row),
            )
        )
    result: Dict[str, Optional[bool]] = {}
    samples: Dict[str, str] = {}
    for token_id, rows in grouped.items():
        values = [value for value, _sample in rows]
        if any(value is False for value in values):
            result[token_id] = False
        elif values and all(value is True for value in values):
            result[token_id] = True
        else:
            result[token_id] = None
        sample_rows = [sample for _value, sample in rows if sample is not None]
        if result[token_id] is not None and len(sample_rows) == len(rows):
            material = json.dumps(
                sorted(sample_rows, key=lambda item: item["order_id"]),
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            samples[token_id] = hashlib.sha256(material.encode("utf-8")).hexdigest()
    return result, samples


def _scoring_by_token(payload: Mapping[str, Any]) -> Dict[str, Optional[bool]]:
    return _scoring_evidence_by_token(payload)[0]


def _load_account_policies(
    config_dir: Optional[Path],
    data_dir: Path,
    *,
    now_ts: float,
) -> List[ObserverAccountPolicy]:
    if config_dir is None:
        return []
    rewards_state = _read_mapping(data_dir / "rewards_live.json")
    reward_accounts = rewards_state.get("accounts")
    if not isinstance(reward_accounts, Mapping) or not _state_is_fresh(
        rewards_state,
        now_ts,
    ):
        reward_accounts = {}

    policies: List[ObserverAccountPolicy] = []
    for path in sorted(config_dir.glob("config_*.json")):
        match = re.fullmatch(r"config_(\d+)\.json", path.name)
        if match is None:
            continue
        config = _read_mapping(path)
        if not config:
            continue
        account_index = int(match.group(1))
        try:
            profile = parse_lp_account_profile(config, account_index)
        except ValueError:
            continue
        state = _read_mapping(data_dir / f"engine_state_{account_index}.json")
        state_fresh = _state_is_fresh(state, now_ts)
        runtime = state.get("runtime")
        if not isinstance(runtime, Mapping):
            runtime = {}
        host_id = (
            str(runtime.get("host_id") or "").strip().lower()
            if state_fresh
            else ""
        )
        balance = _decimal(state.get("balance"), Decimal("-1"))
        if not state_fresh or balance < 0:
            available = None
            available_source = "unavailable"
        else:
            available = profile.effective_available(balance)
            available_source = (
                "engine_balance_capped_by_principal"
                if profile.managed and available < balance
                else "engine_balance"
            )

        strategy = config.get("strategy")
        if not isinstance(strategy, Mapping):
            strategy = {}
        risk = config.get("risk")
        if not isinstance(risk, Mapping):
            risk = {}
        execution = config.get("execution")
        if not isinstance(execution, Mapping):
            execution = {}

        configured_tokens: set[str] = set()
        configured_market_refs: List[Dict[str, Any]] = []
        for section in ("markets", "night_markets"):
            rows = config.get(section)
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, Mapping) or row.get("enabled", True) is False:
                    continue
                for key in ("token_id", "paired_token_id"):
                    token_id = str(row.get(key) or "").strip()
                    if token_id:
                        configured_tokens.add(token_id)
                configured_market_refs.append(
                    {
                        "account_index": account_index,
                        "condition_id": str(
                            row.get("condition_id") or ""
                        ).strip().lower(),
                        "token_id": str(row.get("token_id") or "").strip(),
                        "paired_token_id": str(
                            row.get("paired_token_id") or ""
                        ).strip(),
                        "question": str(row.get("question") or "").strip(),
                        "slug": str(row.get("slug") or "").strip(),
                    }
                )

        market_runtime = state.get("markets")
        if not isinstance(market_runtime, Mapping):
            market_runtime = {}
        scoring_state = _read_mapping(
            data_dir / f"order_scoring_state_{account_index}.json"
        )
        if _state_is_fresh(scoring_state, now_ts):
            scoring, scoring_samples = _scoring_evidence_by_token(scoring_state)
        else:
            scoring, scoring_samples = {}, {}
        reward_row = reward_accounts.get(str(account_index))
        account_uid = _canonical_account_uid(config)
        if (
            not isinstance(reward_row, Mapping)
            or str(reward_row.get("account_uid") or "").strip() != account_uid
        ):
            reward_row = {}
        percentages: Dict[str, Decimal] = {}
        if isinstance(reward_row, Mapping) and str(
            reward_row.get("percentage_status") or ""
        ) == "ok":
            raw_percentages = reward_row.get("reward_percentages")
            if isinstance(raw_percentages, Mapping):
                percentages = {
                    str(condition).strip().lower(): _decimal(value)
                    for condition, value in raw_percentages.items()
                    if str(condition).strip()
                }

        budget_pct = _decimal(
            strategy.get("quote_balance_pct_min_mid"),
            _decimal(strategy.get("quote_balance_pct_min"), Decimal("0.80")),
        )
        policies.append(
            ObserverAccountPolicy(
                account_index=account_index,
                account_id=(
                    profile.account_id
                    if profile.managed
                    else f"pm-account-{account_index}"
                ),
                available_usdc=available,
                available_source=available_source,
                min_distance_ticks=max(
                    1,
                    int(strategy.get("min_distance_ticks") or 1),
                ),
                min_order_size=max(
                    Decimal("0"),
                    _decimal(strategy.get("min_order_size"), Decimal("5")),
                ),
                budget_pct=max(Decimal("0"), min(Decimal("1"), budget_pct)),
                max_quote_shares=max(
                    Decimal("0"),
                    _decimal(risk.get("max_quote_shares_per_market")),
                ),
                max_notional_per_order=max(
                    Decimal("0"),
                    _decimal(risk.get("max_notional_usdc_per_order")),
                ),
                min_front_bid_notional_usdc=max(
                    Decimal("1"),
                    _decimal(
                        execution.get("min_front_bid_notional_usdc"),
                        DEFAULT_STABLE_MIN_FRONT_BID_NOTIONAL_USDC,
                    ),
                ),
                configured_tokens=frozenset(configured_tokens),
                market_runtime={
                    str(token_id): dict(row)
                    for token_id, row in market_runtime.items()
                    if isinstance(row, Mapping)
                },
                scoring_by_token=scoring,
                scoring_sample_by_token=scoring_samples,
                reward_percentages=percentages,
                configured_market_refs=tuple(configured_market_refs),
                account_uid=account_uid,
                host_id=host_id,
            )
        )
    return policies


def _market_end_timestamp(market: Dict[str, Any]) -> Optional[float]:
    for key in ("endDate", "end_date", "endDateIso", "end_date_iso"):
        value = market.get(key)
        if value in (None, ""):
            continue
        text = str(value).strip()
        timestamp = _timestamp(text)
        if timestamp is None:
            continue
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            timestamp += 24 * 60 * 60 - 1
        return timestamp
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


def _public_reward_terms(market: Mapping[str, Any]) -> List[Dict[str, Any]]:
    rows = market.get("clobRewards") or market.get("clob_rewards") or []
    if not isinstance(rows, list):
        return []
    safe_keys = (
        "rewardsDailyRate",
        "rewards_daily_rate",
        "startDate",
        "start_date",
        "endDate",
        "end_date",
        "assetAddress",
        "asset_address",
    )
    return [
        {key: row.get(key) for key in safe_keys if row.get(key) is not None}
        for row in rows
        if isinstance(row, Mapping)
    ]


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


def _is_weather_market(market: Dict[str, Any]) -> bool:
    """Classify weather contracts conservatively for stable LP admission."""

    category = str(market.get("category") or "").strip().lower()
    if category in {"weather", "climate"}:
        return True

    parts = [
        market.get("question"),
        market.get("title"),
        market.get("slug"),
        _event_slug(market),
        market.get("description"),
        market.get("resolutionSource"),
        market.get("resolution_source"),
    ]
    for key in ("tags", "categories"):
        values = market.get(key)
        if isinstance(values, str):
            try:
                values = json.loads(values)
            except Exception:
                values = [values]
        if isinstance(values, list):
            for value in values:
                if isinstance(value, dict):
                    parts.extend((value.get("label"), value.get("name"), value.get("slug")))
                else:
                    parts.append(value)
    text = " ".join(str(part or "") for part in parts).lower()
    return bool(_WEATHER_MARKET_RE.search(text))


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
    max_spread: Decimal,
    min_distance_ticks: int = 1,
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
    yes_boundary = executable_quote_boundary(
        best_bid=yes["best_bid"],
        midpoint=yes["mid"],
        tick=yes_tick,
        max_spread=max_spread,
        min_distance_ticks=min_distance_ticks,
    )
    no_boundary = executable_quote_boundary(
        best_bid=no["best_bid"],
        midpoint=no["mid"],
        tick=no_tick,
        max_spread=max_spread,
        min_distance_ticks=min_distance_ticks,
    )
    if not yes_boundary.executable or not no_boundary.executable:
        result["front_depth_status"] = (
            yes_boundary.blocked_reason
            or no_boundary.blocked_reason
            or "no_safe_quote"
        )
        return result
    yes_quote = yes_boundary.quote
    no_quote = no_boundary.quote

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
    return distance_score(price, midpoint, max_spread)


def _aggregate_bid_score(summary: Dict[str, Any], max_spread: Decimal) -> Decimal:
    return aggregate_bid_q(
        summary["bids"],
        midpoint=summary["mid"],
        max_spread=max_spread,
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
    active = _optional_bool(market.get("active"))
    closed = _optional_bool(market.get("closed"))
    archived = _optional_bool(market.get("archived"))
    accepting_orders = _optional_bool(
        market.get("acceptingOrders")
        if "acceptingOrders" in market
        else market.get("accepting_orders")
    )
    market_end_ts = _market_end_timestamp(market)
    if closed is True or archived is True or active is False or accepting_orders is False:
        return None
    if market_end_ts is not None and market_end_ts <= time.time():
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
        "market_active": active,
        "market_closed": closed,
        "market_archived": archived,
        "accepting_orders": accepting_orders,
        "market_end_ts": market_end_ts,
        "token_ids": token_ids[:2],
        "reward": reward,
        "spread": spread,
        "min_size": min_size,
        "reward_terms": _public_reward_terms(market),
        "rough_efficiency": rough_efficiency,
    }


def _observe_candidate(
    rough: Dict[str, Any],
    yes_book: Optional[Dict[str, Any]],
    no_book: Optional[Dict[str, Any]],
    probe_budget: Decimal,
    min_estimated_daily_payout: Decimal,
    account_policies: Optional[List[ObserverAccountPolicy]] = None,
) -> Optional[Dict[str, Any]]:
    market = rough["market"]
    yes = _book_summary(yes_book)
    no = _book_summary(no_book)
    if yes is None or no is None:
        return None

    spread = rough["spread"]
    min_size = rough["min_size"]
    policies = account_policies or [_default_probe_policy(probe_budget)]
    condition_id = str(
        market.get("conditionId")
        or market.get("condition_id")
        or market.get("market")
        or ""
    ).strip().lower()
    token_ids = tuple(rough["token_ids"][:2])
    account_execution: List[Dict[str, Any]] = []
    executions: List[tuple[ObserverAccountPolicy, PairExecution]] = []
    for policy in policies:
        available = policy.available_usdc
        capital_source = policy.available_source
        if available is None:
            available = probe_budget
            capital_source = "probe_budget_fallback"
        execution = evaluate_paired_quote(
            yes_bids=yes["bids"],
            no_bids=no["bids"],
            yes_best_bid=yes["best_bid"],
            no_best_bid=no["best_bid"],
            yes_midpoint=yes["mid"],
            no_midpoint=no["mid"],
            yes_tick=yes.get("tick_size") or Decimal("0"),
            no_tick=no.get("tick_size") or Decimal("0"),
            max_spread=spread,
            min_distance_ticks=policy.min_distance_ticks,
            available=available,
            rewards_min=min_size,
            min_order_size=policy.min_order_size,
            budget_pct=policy.budget_pct,
            size_cap=Decimal("1"),
            max_quote_shares=policy.max_quote_shares,
            max_notional_per_order=policy.max_notional_per_order,
        )
        executions.append((policy, execution))
        configured = any(token_id in policy.configured_tokens for token_id in token_ids)
        runtime_rows = [policy.market_runtime.get(token_id) for token_id in token_ids]
        runtime_q_values = [
            _decimal(row.get("q_min"), Decimal("-1"))
            for row in runtime_rows
            if isinstance(row, Mapping) and row.get("q_min") is not None
        ]
        runtime_pair_complete = bool(
            len(token_ids) == 2
            and token_ids[0] != token_ids[1]
            and len(runtime_q_values) == 2
        )
        observed_q_min = min(runtime_q_values) if runtime_pair_complete else None
        scoring_values = [policy.scoring_by_token.get(token_id) for token_id in token_ids]
        if any(value is False for value in scoring_values):
            scoring = False
        elif (
            len(token_ids) == 2
            and token_ids[0] != token_ids[1]
            and len(scoring_values) == 2
            and all(value is True for value in scoring_values)
        ):
            scoring = True
        else:
            scoring = None
        scoring_samples = [
            policy.scoring_sample_by_token.get(token_id, "") for token_id in token_ids
        ]
        scoring_sample_id = ""
        if scoring is True and len(scoring_samples) == 2 and all(scoring_samples):
            sample_material = json.dumps(
                {
                    "account_uid_key": _account_uid_key(policy.account_uid),
                    "condition_id": condition_id,
                    "legs": sorted(zip(token_ids, scoring_samples)),
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            scoring_sample_id = hashlib.sha256(
                sample_material.encode("utf-8")
            ).hexdigest()
        actual_percentage = policy.reward_percentages.get(condition_id)
        blocked = list(execution.blocked_reasons)
        if configured and observed_q_min is not None and observed_q_min <= 0:
            blocked.append("observed_q_min_zero")
        if configured and scoring is False:
            blocked.append("official_order_scoring_false")
        account_execution.append(
            {
                "account_index": policy.account_index,
                "account_id": policy.account_id,
                "account_uid_key": _account_uid_key(policy.account_uid),
                "host_id": policy.host_id,
                "configured": configured,
                "capital_source": capital_source,
                "capital_evidence_fresh": policy.available_usdc is not None,
                "available_usdc": (
                    round(float(policy.available_usdc), 2)
                    if policy.available_usdc is not None
                    else None
                ),
                "budget_pct": round(float(policy.budget_pct), 6),
                "min_distance_ticks": policy.min_distance_ticks,
                "min_front_bid_notional_usdc": round(
                    float(policy.min_front_bid_notional_usdc), 2
                ),
                "target_shares": round(float(execution.target_shares), 4),
                "collateral_required_usdc": round(
                    float(execution.collateral_required), 2
                ),
                "yes_quote": round(float(execution.yes_quote), 6),
                "no_quote": round(float(execution.no_quote), 6),
                "theoretical_q_min": round(
                    float(execution.theoretical_q_min), 6
                ),
                "executable_q_min": round(float(execution.executable_q_min), 6),
                "theoretical_share_pct": round(
                    float(execution.theoretical_share * 100), 4
                ),
                "executable_share_pct": round(
                    float(execution.executable_share * 100), 4
                ),
                "estimated_daily_gross_usd": round(
                    float(rough["reward"] * execution.executable_share), 4
                ),
                "yes_front_bid_notional_usd": round(
                    float(execution.yes_front_notional), 2
                ),
                "no_front_bid_notional_usd": round(
                    float(execution.no_front_notional), 2
                ),
                "min_front_bid_notional_usd": round(
                    float(
                        min(
                            execution.yes_front_notional,
                            execution.no_front_notional,
                        )
                    ),
                    2,
                ),
                "observed_q_min": (
                    round(float(observed_q_min), 6)
                    if observed_q_min is not None
                    else None
                ),
                "official_scoring": scoring,
                "official_scoring_legs": {
                    token_id: policy.scoring_by_token.get(token_id)
                    for token_id in token_ids
                },
                "scoring_sample_id": scoring_sample_id or None,
                "actual_reward_share_pct": (
                    round(float(actual_percentage), 6)
                    if actual_percentage is not None
                    else None
                ),
                "blocked_reasons": list(dict.fromkeys(blocked)),
                "executable": execution.executable and not blocked,
            }
        )

    policy, best_execution = max(
        executions,
        key=lambda item: (
            item[1].executable_share,
            item[1].executable_q_min,
        ),
    )
    estimated_share = best_execution.executable_share
    theoretical_share = best_execution.theoretical_share
    probe_shares = best_execution.target_shares
    probe_capital = best_execution.collateral_required
    competition_q = best_execution.competition_q
    daily_reward = rough["reward"]
    estimated_daily_gross = daily_reward * estimated_share
    gross_daily_roi = (
        estimated_daily_gross / probe_capital
        if probe_capital > 0 else Decimal("0")
    )
    midpoint = yes["mid"]
    fill_risk = _fill_risk(market, midpoint)
    market_phase, game_start_ts = _market_phase(market)
    front_depth = _front_depth_metrics(
        yes,
        no,
        observed_at=time.time(),
        max_spread=spread,
        min_distance_ticks=policy.min_distance_ticks,
    )

    reasons: List[str] = []
    if estimated_share >= Decimal("0.5"):
        reasons.append("estimated_majority_share")
    elif estimated_share >= Decimal("0.2"):
        reasons.append("estimated_meaningful_share")
    else:
        reasons.append("estimated_crowded")
    if best_execution.target_shares <= 0:
        reasons.append("minimum_size_exceeds_executable_budget")
    if best_execution.blocked_reasons:
        reasons.extend(best_execution.blocked_reasons)
    if fill_risk >= 65:
        reasons.append("high_fill_risk")
    elif fill_risk >= 35:
        reasons.append("medium_fill_risk")
    if estimated_daily_gross < min_estimated_daily_payout:
        reasons.append("estimated_daily_payout_below_floor")

    market_competitiveness = market.get("marketCompetitiveness")
    if market_competitiveness is None:
        market_competitiveness = market.get("market_competitiveness")

    is_weather = _is_weather_market(market)
    return {
        "condition_id": condition_id,
        "question": str(market.get("question") or market.get("title") or ""),
        "slug": str(market.get("slug") or ""),
        "event_slug": _event_slug(market),
        "market_url": _market_url(market),
        "token_id": rough["token_ids"][0],
        "paired_token_id": rough["token_ids"][1],
        "market_type": (
            "weather"
            if is_weather
            else "sports" if _is_sports_market(market) else "always_on"
        ),
        "weather_market": is_weather,
        "market_phase": market_phase,
        "game_start_ts": game_start_ts,
        "market_active": rough.get("market_active"),
        "market_closed": rough.get("market_closed"),
        "market_archived": rough.get("market_archived"),
        "accepting_orders": rough.get("accepting_orders"),
        "market_end_ts": rough.get("market_end_ts"),
        "seconds_to_end": (
            round(float(rough["market_end_ts"]) - time.time(), 1)
            if rough.get("market_end_ts") is not None
            else None
        ),
        "seconds_to_start": (
            round(game_start_ts - time.time(), 1)
            if game_start_ts is not None
            else None
        ),
        "daily_reward_usd": round(float(daily_reward), 2),
        "reward_terms": list(rough.get("reward_terms") or []),
        "selection_lane": str(
            rough.get("selection_lane") or "overall_efficiency"
        ),
        "rewards_min_size_shares": round(float(min_size), 4),
        "rewards_max_spread": round(float(spread), 6),
        "probe_budget_usd": round(float(probe_budget), 2),
        "probe_capital_usd": round(float(probe_capital), 2),
        "probe_shares_each_side": round(float(probe_shares), 4),
        "theoretical_yes_quote": round(float(yes["best_bid"]), 6),
        "theoretical_no_quote": round(float(no["best_bid"]), 6),
        "yes_quote": round(float(best_execution.yes_quote), 6),
        "no_quote": round(float(best_execution.no_quote), 6),
        "theoretical_reward_share_pct": round(
            float(theoretical_share * 100), 2
        ),
        "executable_reward_share_pct": round(float(estimated_share * 100), 2),
        "theoretical_q_min": round(float(best_execution.theoretical_q_min), 6),
        "executable_q_min": round(float(best_execution.executable_q_min), 6),
        "estimated_reward_share_pct": round(float(estimated_share * 100), 2),
        "estimated_daily_gross_usd": round(float(estimated_daily_gross), 2),
        "estimated_gross_daily_roi_pct": round(float(gross_daily_roi * 100), 2),
        "min_estimated_daily_payout_usd": round(
            float(min_estimated_daily_payout),
            2,
        ),
        "actual_reward_share_pct": next(
            (
                row["actual_reward_share_pct"]
                for row in account_execution
                if row["actual_reward_share_pct"] is not None
            ),
            None,
        ),
        "actual_daily_gross_usd": next(
            (
                round(
                    float(
                        daily_reward
                        * Decimal(str(row["actual_reward_share_pct"]))
                        / Decimal("100")
                    ),
                    4,
                )
                for row in account_execution
                if row["actual_reward_share_pct"] is not None
            ),
            None,
        ),
        "fill_risk": fill_risk,
        "risk_label": _risk_label(fill_risk),
        "competition_score_estimate": round(float(competition_q), 4),
        "market_competitiveness": (
            round(float(_decimal(market_competitiveness)), 4)
            if market_competitiveness is not None else None
        ),
        "reasons": list(dict.fromkeys(reasons)),
        "blocked_reason": (
            ";".join(best_execution.blocked_reasons)
            if best_execution.blocked_reasons
            else None
        ),
        "status": "observe_only",
        "execution_status": (
            "executable_observation"
            if best_execution.executable
            else "blocked_observation"
        ),
        "estimate_confidence": (
            "executable_model"
            if best_execution.executable
            else "blocked"
        ),
        "account_execution": account_execution,
        "model_version": MODEL_VERSION,
        **front_depth,
    }


def _select_rough_candidates(
    rough: List[Dict[str, Any]],
    *,
    candidate_limit: int,
    lower_reward_reserve_ratio: Decimal,
    required_token_ids: Optional[set[str]] = None,
    required_condition_ids: Optional[set[str]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Reserve part of the book budget for efficient smaller reward pools.

    The main lane keeps the existing capital-efficiency ranking. The reserve
    lane samples the lower-reward tail and ranks that tail by the same
    efficiency metric, so absolute reward size alone cannot crowd every small
    pool out before order-book analysis.
    """

    limit = max(1, int(candidate_limit))
    required_token_ids = {
        str(token_id).strip()
        for token_id in (required_token_ids or set())
        if str(token_id).strip()
    }
    required_condition_ids = {
        str(condition_id).strip().lower()
        for condition_id in (required_condition_ids or set())
        if str(condition_id).strip()
    }
    if not rough:
        return [], {
            "configured_or_watchlist": 0,
            "overall_efficiency": 0,
            "lower_reward_efficiency": 0,
            "lower_reward_pool_seen": 0,
        }

    overall = sorted(
        rough,
        key=lambda row: (row["rough_efficiency"], row["reward"]),
        reverse=True,
    )
    reserve_ratio = min(
        Decimal("0.5"),
        max(Decimal("0"), _decimal(lower_reward_reserve_ratio)),
    )
    reserve_count = min(
        max(0, limit - 1),
        int(Decimal(limit) * reserve_ratio),
    )
    lower_pool_size = min(len(rough), max(reserve_count * 2, reserve_count))
    lower_pool = sorted(
        rough,
        key=lambda row: (row["reward"], -row["rough_efficiency"]),
    )[:lower_pool_size]
    lower_ranked = sorted(
        lower_pool,
        key=lambda row: (row["rough_efficiency"], row["reward"]),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    seen: set[int] = set()
    lane_counts = {
        "configured_or_watchlist": 0,
        "overall_efficiency": 0,
        "lower_reward_efficiency": 0,
        "lower_reward_pool_seen": len(lower_pool),
    }

    def add(
        rows: Iterable[Dict[str, Any]],
        count: int,
        lane: str,
        *,
        ignore_total_limit: bool = False,
    ) -> None:
        for row in rows:
            if lane_counts[lane] >= count:
                break
            if not ignore_total_limit and len(selected) >= limit + lane_counts[
                "configured_or_watchlist"
            ]:
                break
            key = id(row)
            if key in seen:
                continue
            copy = dict(row)
            copy["selection_lane"] = lane
            selected.append(copy)
            seen.add(key)
            lane_counts[lane] += 1

    forced = [
        row
        for row in rough
        if required_token_ids.intersection(
            {str(token_id).strip() for token_id in row.get("token_ids") or []}
        )
        or str(
            row["market"].get("conditionId")
            or row["market"].get("condition_id")
            or ""
        ).strip().lower()
        in required_condition_ids
    ]
    add(
        forced,
        len(forced),
        "configured_or_watchlist",
        ignore_total_limit=True,
    )
    add(lower_ranked, reserve_count, "lower_reward_efficiency")
    optional_selected = len(selected) - lane_counts["configured_or_watchlist"]
    add(overall, limit - optional_selected, "overall_efficiency")
    if len(selected) - lane_counts["configured_or_watchlist"] < limit:
        add(overall, limit, "overall_efficiency")
    return selected, lane_counts


def _rough_public_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    market = row["market"]
    token_ids = [str(token_id) for token_id in row.get("token_ids") or []]
    return {
        "condition_id": str(
            market.get("conditionId") or market.get("condition_id") or ""
        ).strip().lower(),
        "token_id": token_ids[0] if token_ids else "",
        "paired_token_id": token_ids[1] if len(token_ids) > 1 else "",
        "question": str(market.get("question") or market.get("title") or ""),
        "slug": str(market.get("slug") or ""),
        "event_slug": _event_slug(market),
        "market_url": _market_url(market),
        "daily_reward_usd": round(float(row["reward"]), 2),
        "reward_terms": list(row.get("reward_terms") or []),
        "rewards_min_size_shares": round(float(row["min_size"]), 4),
        "rewards_max_spread": round(float(row["spread"]), 6),
        "assessment_status": "unassessed",
        "admission_level": "unassessed",
        "reason_codes": ["selection_budget_not_evaluated"],
    }


def _market_ref_aliases(row: Mapping[str, Any]) -> tuple[str, ...]:
    aliases: List[str] = []
    condition_id = str(row.get("condition_id") or "").strip().lower()
    if condition_id:
        aliases.append(f"condition:{condition_id}")
    token_ids = sorted(
        {
            str(row.get("token_id") or "").strip(),
            str(row.get("paired_token_id") or "").strip(),
        }
        - {""}
    )
    if token_ids:
        aliases.append("pair:" + ":".join(token_ids))
    return tuple(aliases)


def _market_ref_key(row: Mapping[str, Any]) -> str:
    aliases = _market_ref_aliases(row)
    return aliases[0] if aliases else ""


def observe_reward_markets(
    markets: Iterable[Dict[str, Any]],
    fetch_book: Callable[[str], Optional[Dict[str, Any]]],
    *,
    candidate_limit: int = DEFAULT_CANDIDATE_LIMIT,
    probe_budget_usdc: Decimal = DEFAULT_PROBE_BUDGET_USDC,
    lower_reward_reserve_ratio: Decimal = DEFAULT_LOWER_REWARD_RESERVE_RATIO,
    min_estimated_daily_payout_usdc: Decimal = (
        DEFAULT_MIN_ESTIMATED_DAILY_PAYOUT_USDC
    ),
    account_policies: Optional[List[ObserverAccountPolicy]] = None,
    forced_condition_ids: Optional[set[str]] = None,
    fetch_workers: int = 8,
) -> Dict[str, Any]:
    """Build a ranked, read-only opportunity list from active reward markets."""

    rough = [
        candidate
        for market in markets
        if (candidate := _rough_candidate(market)) is not None
    ]
    required_token_ids = {
        token_id
        for policy in account_policies or []
        for token_id in policy.configured_tokens
    }
    configured_condition_ids = {
        str(ref.get("condition_id") or "").strip().lower()
        for policy in account_policies or []
        for ref in policy.configured_market_refs
        if isinstance(ref, Mapping)
        and str(ref.get("condition_id") or "").strip()
    }
    selected, selection_lanes = _select_rough_candidates(
        rough,
        candidate_limit=candidate_limit,
        lower_reward_reserve_ratio=lower_reward_reserve_ratio,
        required_token_ids=required_token_ids,
        required_condition_ids=configured_condition_ids
        | set(forced_condition_ids or set()),
    )
    selected_ids = {id(row.get("market")) for row in selected}
    unassessed = [
        _rough_public_row(row)
        for row in rough
        if id(row.get("market")) not in selected_ids
    ]
    rough_public = [_rough_public_row(row) for row in rough]

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
            _decimal(
                min_estimated_daily_payout_usdc,
                DEFAULT_MIN_ESTIMATED_DAILY_PAYOUT_USDC,
            ),
            account_policies,
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
        "candidates_unassessed": len(unassessed),
        "selection_lanes": selection_lanes,
        "candidates": observed,
        "unassessed_candidates": unassessed,
        "configured_market_refs": [
            dict(ref)
            for policy in account_policies or []
            for ref in policy.configured_market_refs
        ],
        "rewarded_market_keys": sorted(
            {
                alias
                for row in rough_public
                for alias in _market_ref_aliases(row)
            }
        ),
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
    stable_depths: List[Decimal] = []
    if config_dir is not None:
        for path in sorted(config_dir.glob("config_*.json")):
            try:
                config = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            curator = config.get("auto_curator")
            if isinstance(curator, dict):
                observer = curator.get("reward_observer")
                if isinstance(observer, dict) and not settings:
                    settings = observer
            profile = config.get("lp_account")
            profile_type = (
                str(profile.get("profile_type") or "standard").strip().lower()
                if isinstance(profile, dict)
                else "standard"
            )
            if profile_type != "aggressive":
                execution = config.get("execution")
                if isinstance(execution, dict):
                    depth = _decimal(
                        execution.get("min_front_bid_notional_usdc"),
                        Decimal("-1"),
                    )
                    if depth > 0:
                        stable_depths.append(depth)
    return {
        "candidate_limit": max(
            1,
            min(100, int(settings.get("candidate_limit") or DEFAULT_CANDIDATE_LIMIT)),
        ),
        "probe_budget_usdc": _decimal(
            settings.get("probe_budget_usdc"),
            DEFAULT_PROBE_BUDGET_USDC,
        ),
        "lower_reward_reserve_ratio": min(
            Decimal("0.5"),
            max(
                Decimal("0"),
                _decimal(
                    settings.get("lower_reward_reserve_ratio"),
                    DEFAULT_LOWER_REWARD_RESERVE_RATIO,
                ),
            ),
        ),
        "min_estimated_daily_payout_usdc": max(
            Decimal("0"),
            _decimal(
                settings.get("min_estimated_daily_payout_usdc"),
                DEFAULT_MIN_ESTIMATED_DAILY_PAYOUT_USDC,
            ),
        ),
        "stable_min_front_bid_notional_usdc": max(
            Decimal("1"),
            min(stable_depths)
            if stable_depths
            else _decimal(
                settings.get("stable_min_front_bid_notional_usdc"),
                DEFAULT_STABLE_MIN_FRONT_BID_NOTIONAL_USDC,
            ),
        ),
        "stable_min_time_to_end_sec": max(
            0,
            int(
                settings.get("stable_min_time_to_end_sec")
                or DEFAULT_STABLE_MIN_TIME_TO_END_SEC
            ),
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


def _finalized_lp_earnings(
    data_dir: Path,
) -> Dict[tuple[str, str, str], Dict[str, Any]]:
    """Return canonical finalized LP earnings without exposing maker addresses."""

    ledger = _read_mapping(data_dir / "reward_ledger.json")
    records = ledger.get("records")
    if not isinstance(records, Mapping):
        return {}
    grouped: Dict[tuple[str, str, str], Dict[str, Any]] = {}
    for raw in records.values():
        if not isinstance(raw, Mapping):
            continue
        reward_type = str(raw.get("reward_type") or "")
        if reward_type not in {"native_lp", "sponsored_lp"}:
            continue
        if raw.get("fresh") is not True or raw.get("finalized") is not True:
            continue
        account_key = _account_uid_key(raw.get("account_uid"))
        business_day = str(raw.get("business_day") or "").strip()
        condition_id = str(raw.get("condition_id") or "").strip().lower()
        if not account_key or not business_day or not condition_id:
            continue
        try:
            usd_amount = float(raw.get("usd_amount") or 0)
        except (TypeError, ValueError):
            continue
        key = (account_key, business_day, condition_id)
        row = grouped.setdefault(
            key,
            {
                "business_day": business_day,
                "condition_id": condition_id,
                "usd_amount": 0.0,
                "usd_by_type": {"native_lp": 0.0, "sponsored_lp": 0.0},
            },
        )
        row["usd_amount"] += usd_amount
        row["usd_by_type"][reward_type] += usd_amount
    for row in grouped.values():
        row["usd_amount"] = round(float(row["usd_amount"]), 6)
        row["usd_by_type"] = {
            key: round(float(value), 6)
            for key, value in row["usd_by_type"].items()
        }
    return grouped


def _apply_finalized_earnings(
    samples: List[Dict[str, Any]],
    *,
    condition_id: str,
    earnings: Mapping[tuple[str, str, str], Mapping[str, Any]],
) -> None:
    for sample in samples:
        business_day = str(sample.get("forecast_business_day") or "").strip()
        if not business_day:
            continue
        for execution in sample.get("account_execution") or []:
            if not isinstance(execution, dict):
                continue
            account_key = str(execution.get("account_uid_key") or "").strip()
            actual = earnings.get((account_key, business_day, condition_id))
            if not isinstance(actual, Mapping):
                continue
            execution["official_finalized_lp_earnings_usd"] = actual.get(
                "usd_amount"
            )
            execution["official_finalized_lp_earnings_by_type"] = dict(
                actual.get("usd_by_type") or {}
            )


def _dedupe_history_samples(
    samples: Iterable[Mapping[str, Any]],
    *,
    cutoff: float,
) -> List[Dict[str, Any]]:
    by_timestamp: Dict[float, Dict[str, Any]] = {}
    for sample in samples:
        if not isinstance(sample, Mapping):
            continue
        try:
            sample_ts = round(float(sample.get("ts") or 0), 3)
        except (TypeError, ValueError):
            continue
        if sample_ts < cutoff:
            continue
        copy = dict(sample)
        copy["ts"] = sample_ts
        by_timestamp[sample_ts] = copy
    return [by_timestamp[key] for key in sorted(by_timestamp)][
        -HISTORY_SAMPLES_PER_MARKET:
    ]


def _stability_fields(
    samples: List[Dict[str, Any]],
    now_ts: float,
) -> Dict[str, Any]:
    valid_samples = [sample for sample in samples if not sample.get("missing_reason")]
    missing_samples = len(samples) - len(valid_samples)
    if not valid_samples:
        return {
            "observation_samples": 0,
            "history_samples": len(samples),
            "missing_samples": missing_samples,
            "observation_span_sec": 0,
            "stability_score": 0,
            "estimated_share_range_pp": None,
            "eligible_sample_ratio": 0.0,
            "scoring_sample_ratio": None,
            "prediction_actual_mae_pp": None,
            "earnings_calibration_scopes": 0,
            "earnings_prediction_mae_usd": None,
            "earnings_prediction_bias_usd": None,
            "earnings_calibration_ratio": None,
            "verification_status": "collecting",
        }
    first_ts = float(valid_samples[0].get("ts") or now_ts)
    span = max(0, int(now_ts - first_ts))
    shares = _float_values(valid_samples, "share")
    risks = _float_values(valid_samples, "risk")
    share_range = max(shares) - min(shares) if shares else 100.0
    risk_range = max(risks) - min(risks) if risks else 100.0
    executable_samples = 0
    scoring_known = 0
    scoring_true = 0
    calibration_errors: List[float] = []
    earnings_scopes: Dict[tuple[str, str], Dict[str, Any]] = {}
    for sample in valid_samples:
        executions = sample.get("account_execution") or []
        if any(
            execution.get("executable") is True
            for execution in executions
            if isinstance(execution, Mapping)
        ):
            executable_samples += 1
        scoring_values = [
            execution.get("official_scoring")
            for execution in executions
            if isinstance(execution, Mapping)
            and isinstance(execution.get("official_scoring"), bool)
        ]
        if scoring_values:
            scoring_known += 1
            if all(value is True for value in scoring_values):
                scoring_true += 1
        try:
            predicted = float(sample.get("executable_share"))
            actual = float(sample.get("actual_share"))
        except (TypeError, ValueError):
            pass
        else:
            calibration_errors.append(abs(predicted - actual))
        for execution in sample.get("account_execution") or []:
            if not isinstance(execution, Mapping):
                continue
            account_key = str(execution.get("account_uid_key") or "").strip()
            business_day = str(sample.get("forecast_business_day") or "").strip()
            try:
                predicted_earnings = float(
                    execution.get("predicted_daily_gross_usd")
                )
                actual_earnings = float(
                    execution.get("official_finalized_lp_earnings_usd")
                )
            except (TypeError, ValueError):
                continue
            if not account_key or not business_day:
                continue
            scope = earnings_scopes.setdefault(
                (account_key, business_day),
                {"predictions": [], "actual": actual_earnings},
            )
            scope["predictions"].append(predicted_earnings)
            scope["actual"] = actual_earnings
    earnings_errors: List[float] = []
    earnings_biases: List[float] = []
    predicted_total = 0.0
    actual_total = 0.0
    for scope in earnings_scopes.values():
        predictions = scope["predictions"]
        if not predictions:
            continue
        predicted = sum(predictions) / len(predictions)
        actual = float(scope["actual"])
        earnings_errors.append(abs(predicted - actual))
        earnings_biases.append(predicted - actual)
        predicted_total += predicted
        actual_total += actual
    consistency = max(
        0.0,
        100.0 - min(70.0, share_range * 2.0) - min(20.0, risk_range * 0.5),
    )
    sample_coverage = min(1.0, len(valid_samples) / 12.0)
    time_coverage = min(1.0, span / 3300.0)
    score = round(consistency * (0.4 + 0.6 * min(sample_coverage, time_coverage)))

    latest_risk = risks[-1] if risks else 100.0
    latest_missing = bool(samples and samples[-1].get("missing_reason"))
    if latest_missing:
        status = "data_missing"
    elif len(valid_samples) < 3:
        status = "collecting"
    elif len(valid_samples) < 12 or span < 3300:
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
        "observation_samples": len(valid_samples),
        "history_samples": len(samples),
        "missing_samples": missing_samples,
        "observation_span_sec": span,
        "stability_score": int(score),
        "estimated_share_range_pp": round(share_range, 2),
        "eligible_sample_ratio": round(
            executable_samples / max(1, len(valid_samples)),
            4,
        ),
        "scoring_sample_ratio": (
            round(scoring_true / scoring_known, 4)
            if scoring_known
            else None
        ),
        "prediction_actual_mae_pp": (
            round(sum(calibration_errors) / len(calibration_errors), 4)
            if calibration_errors
            else None
        ),
        "earnings_calibration_scopes": len(earnings_errors),
        "earnings_prediction_mae_usd": (
            round(sum(earnings_errors) / len(earnings_errors), 6)
            if earnings_errors
            else None
        ),
        "earnings_prediction_bias_usd": (
            round(sum(earnings_biases) / len(earnings_biases), 6)
            if earnings_biases
            else None
        ),
        "earnings_calibration_ratio": (
            round(actual_total / predicted_total, 6)
            if earnings_errors and predicted_total > 0
            else None
        ),
        "verification_status": status,
    }


def _apply_observation_history(
    data_dir: Path,
    state: Dict[str, Any],
    now_ts: float,
    settings: Optional[Dict[str, Any]] = None,
) -> None:
    settings = settings or {}
    history_path = data_dir / "reward_observer_history.json"
    try:
        history = json.loads(history_path.read_text(encoding="utf-8"))
    except Exception:
        history = {}
    markets = history.get("markets") if isinstance(history, dict) else {}
    if not isinstance(markets, dict):
        markets = {}

    cutoff = now_ts - HISTORY_RETENTION_SECONDS
    finalized_earnings = _finalized_lp_earnings(data_dir)
    for condition_id, row in list(markets.items()):
        if not isinstance(row, dict):
            markets.pop(condition_id, None)
            continue
        samples = row.get("samples")
        if not isinstance(samples, list):
            markets.pop(condition_id, None)
            continue
        fresh = _dedupe_history_samples(samples, cutoff=cutoff)
        if fresh:
            row["samples"] = fresh
        else:
            markets.pop(condition_id, None)

    sampled_keys: set[str] = set()
    for candidate in state.get("candidates") or []:
        if not isinstance(candidate, dict):
            continue
        market_key = _market_ref_key(candidate)
        if not market_key:
            continue
        sampled_keys.update(_market_ref_aliases(candidate))
        legacy_key = str(candidate.get("condition_id") or "").strip().lower()
        if legacy_key and legacy_key != market_key and legacy_key in markets:
            legacy_row = markets.pop(legacy_key)
            target_row = markets.get(market_key)
            if isinstance(legacy_row, Mapping) and isinstance(target_row, Mapping):
                merged = dict(target_row)
                merged["samples"] = _dedupe_history_samples(
                    list(legacy_row.get("samples") or [])
                    + list(target_row.get("samples") or []),
                    cutoff=cutoff,
                )
                markets[market_key] = merged
            elif isinstance(legacy_row, Mapping):
                markets[market_key] = dict(legacy_row)
        row = markets.setdefault(
            market_key,
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
                "forecast_business_day": datetime.fromtimestamp(
                    now_ts, timezone.utc
                ).date().isoformat(),
                "share": candidate.get("estimated_reward_share_pct"),
                "gross_roi": candidate.get("estimated_gross_daily_roi_pct"),
                "risk": candidate.get("fill_risk"),
                "capital": candidate.get("probe_capital_usd"),
                "yes_quote": candidate.get("yes_quote"),
                "no_quote": candidate.get("no_quote"),
                "front_depth_status": candidate.get("front_depth_status"),
                "min_front_bid_notional_usd": candidate.get("min_front_bid_notional_usd"),
                "executable_share": candidate.get(
                    "executable_reward_share_pct"
                ),
                "theoretical_share": candidate.get(
                    "theoretical_reward_share_pct"
                ),
                "executable_q_min": candidate.get("executable_q_min"),
                "actual_share": candidate.get("actual_reward_share_pct"),
                "account_execution": [
                    {
                        "account_index": execution.get("account_index"),
                        "account_uid_key": execution.get("account_uid_key"),
                        "executable": execution.get("executable"),
                        "executable_q_min": execution.get("executable_q_min"),
                        "official_scoring": execution.get("official_scoring"),
                        "observed_q_min": execution.get("observed_q_min"),
                        "actual_reward_share_pct": execution.get(
                            "actual_reward_share_pct"
                        ),
                        "predicted_daily_gross_usd": execution.get(
                            "estimated_daily_gross_usd"
                        ),
                    }
                    for execution in candidate.get("account_execution") or []
                    if isinstance(execution, Mapping)
                ],
            }
        )
        samples = _dedupe_history_samples(samples, cutoff=cutoff)
        _apply_finalized_earnings(
            samples,
            condition_id=str(candidate.get("condition_id") or "").strip().lower(),
            earnings=finalized_earnings,
        )
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
            and float(candidate.get("estimated_daily_gross_usd") or 0)
            >= float(candidate.get("min_estimated_daily_payout_usd") or 0)
        )
        stable_rejections: List[str] = []
        if candidate.get("weather_market") is True:
            stable_rejections.append("weather_observe_only")
        if str(candidate.get("front_depth_status") or "") != "verified":
            stable_rejections.append("front_depth_unavailable")
        min_depth = float(
            settings.get("stable_min_front_bid_notional_usdc")
            or DEFAULT_STABLE_MIN_FRONT_BID_NOTIONAL_USDC
        )
        try:
            front_depth = float(candidate.get("min_front_bid_notional_usd"))
        except (TypeError, ValueError):
            front_depth = -1.0
        if front_depth < min_depth:
            stable_rejections.append("front_depth_below_stable_minimum")
        min_time_to_end = float(
            settings.get("stable_min_time_to_end_sec")
            or DEFAULT_STABLE_MIN_TIME_TO_END_SEC
        )
        try:
            seconds_to_end = float(candidate.get("market_end_ts")) - now_ts
        except (TypeError, ValueError):
            seconds_to_end = -1.0
        candidate["seconds_to_end"] = round(seconds_to_end, 1)
        if seconds_to_end < min_time_to_end:
            stable_rejections.append("market_ends_too_soon")
        candidate["stable_lp_min_front_bid_notional_usdc"] = round(min_depth, 2)
        candidate["stable_lp_min_time_to_end_sec"] = round(min_time_to_end, 1)
        candidate["stable_lp_rejection_reasons"] = stable_rejections
        candidate["stable_lp_recommended"] = bool(
            candidate["verification_recommended"] and not stable_rejections
        )
        account_admission: List[Dict[str, Any]] = []
        for execution in candidate.get("account_execution") or []:
            if not isinstance(execution, Mapping):
                continue
            rejection_reasons: List[str] = []
            canary_reasons: List[str] = []
            blocked = [
                str(reason)
                for reason in execution.get("blocked_reasons") or []
                if str(reason)
            ]
            if blocked:
                rejection_reasons.extend(blocked)
            if candidate.get("weather_market") is True:
                rejection_reasons.append("weather_observe_only")
            if seconds_to_end < min_time_to_end:
                rejection_reasons.append("market_ends_too_soon")
            if fill_risk >= 65:
                rejection_reasons.append("fill_risk_high")
            if float(execution.get("executable_q_min") or 0) <= 0:
                rejection_reasons.append("executable_q_min_zero")
            if execution.get("configured") and execution.get("official_scoring") is False:
                rejection_reasons.append("official_order_scoring_false")
            observed_q = execution.get("observed_q_min")
            if (
                execution.get("configured")
                and observed_q is not None
                and float(observed_q) <= 0
            ):
                rejection_reasons.append("observed_q_min_zero")
            if execution.get("configured"):
                if execution.get("official_scoring") is None:
                    canary_reasons.append("official_scoring_evidence_unavailable")
                if observed_q is None:
                    canary_reasons.append("observed_q_evidence_unavailable")
            else:
                canary_reasons.append("requires_canary_scoring_validation")

            try:
                execution_depth = float(
                    execution.get("min_front_bid_notional_usd")
                )
                required_depth = float(
                    execution.get("min_front_bid_notional_usdc")
                )
            except (TypeError, ValueError):
                execution_depth = -1.0
                required_depth = float(
                    DEFAULT_STABLE_MIN_FRONT_BID_NOTIONAL_USDC
                )
            if execution_depth < required_depth:
                canary_reasons.append("front_depth_below_full_minimum")
            if not execution.get("capital_evidence_fresh"):
                canary_reasons.append("capital_evidence_unavailable")
            if stability["verification_status"] not in {"stable", "confirmed"}:
                canary_reasons.append("stability_warming")
            if stability["stability_score"] < 70:
                canary_reasons.append("stability_below_full_minimum")
            if fill_risk >= 35:
                canary_reasons.append("fill_risk_above_full_limit")
            estimated_daily_gross = float(
                execution.get("estimated_daily_gross_usd") or 0
            )
            if estimated_daily_gross > 0:
                if estimated_daily_gross < float(
                    candidate.get("min_estimated_daily_payout_usd") or 0
                ):
                    canary_reasons.append("estimated_daily_payout_below_floor")
            else:
                rejection_reasons.append("estimated_daily_payout_zero")

            rejection_reasons = list(dict.fromkeys(rejection_reasons))
            canary_reasons = list(dict.fromkeys(canary_reasons))
            if rejection_reasons:
                level = "reject"
                reason_codes = rejection_reasons
            elif canary_reasons:
                level = "canary"
                reason_codes = canary_reasons
            else:
                level = "full"
                reason_codes = ["account_executable_and_verified"]
            account_admission.append(
                {
                    "account_index": execution.get("account_index"),
                    "level": level,
                    "reason_codes": reason_codes,
                    "canary_requires_scoring_validation": level == "canary",
                }
            )
        rank = {"reject": 0, "canary": 1, "full": 2}
        best_admission = max(
            (
                str(row.get("level") or "reject")
                for row in account_admission
            ),
            key=lambda level: rank.get(level, 0),
            default="reject",
        )
        candidate["account_admission"] = account_admission
        candidate["admission_level"] = best_admission
        candidate["stable_lp_recommended"] = best_admission == "full"
        candidate["canary_proposal_eligible"] = False

    rewarded_market_keys = {
        str(key) for key in state.get("rewarded_market_keys") or [] if str(key)
    }
    missing_refs: Dict[str, Dict[str, Any]] = {}
    for ref in state.get("configured_market_refs") or []:
        if not isinstance(ref, Mapping):
            continue
        aliases = _market_ref_aliases(ref)
        market_key = aliases[0] if aliases else ""
        if not market_key or sampled_keys.intersection(aliases):
            continue
        current = missing_refs.setdefault(
            market_key,
            {
                "condition_id": str(ref.get("condition_id") or ""),
                "question": str(ref.get("question") or ""),
                "slug": str(ref.get("slug") or ""),
                "account_indexes": [],
            },
        )
        account_index = ref.get("account_index")
        if account_index not in current["account_indexes"]:
            current["account_indexes"].append(account_index)

    for market_key, ref in missing_refs.items():
        missing_reason = (
            "order_book_unavailable_or_invalid"
            if market_key in rewarded_market_keys
            else "not_in_active_reward_market_feed"
        )
        row = markets.setdefault(
            market_key,
            {
                "question": ref.get("question"),
                "slug": ref.get("slug"),
                "samples": [],
            },
        )
        samples = row.get("samples")
        if not isinstance(samples, list):
            samples = []
        samples.append(
            {
                "ts": now_ts,
                "missing_reason": missing_reason,
                "configured_account_indexes": sorted(
                    index
                    for index in ref.get("account_indexes") or []
                    if isinstance(index, int)
                ),
            }
        )
        row["samples"] = _dedupe_history_samples(samples, cutoff=cutoff)
        row["last_seen_at"] = now_ts
        row["last_missing_reason"] = missing_reason

    history_payload = {
        "version": 2,
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
    account_policies = _load_account_policies(
        config_dir,
        data_dir,
        now_ts=started,
    )
    forced_conditions = fast_lane_forced_condition_ids(
        data_dir,
        now_ts=started,
    )
    markets = fetch_markets()
    state = observe_reward_markets(
        markets,
        fetch_book,
        candidate_limit=int(settings["candidate_limit"]),
        probe_budget_usdc=settings["probe_budget_usdc"],
        lower_reward_reserve_ratio=settings["lower_reward_reserve_ratio"],
        min_estimated_daily_payout_usdc=settings[
            "min_estimated_daily_payout_usdc"
        ],
        account_policies=account_policies or None,
        forced_condition_ids=forced_conditions,
    )
    generated_at = time.time()
    state["generated_at"] = generated_at
    _apply_observation_history(data_dir, state, generated_at, settings)
    update_fast_lane(data_dir, state, now_ts=generated_at)
    write_shadow_budget(data_dir, state)
    state.update(
        {
            "status": "ready",
            "generated_at": generated_at,
            "elapsed_sec": round(time.time() - started, 2),
            "markets_seen": len(markets),
            "source": "public_gamma_and_clob",
            "accounts_evaluated": len(account_policies),
        }
    )
    if config_dir is not None:
        try:
            proposal_summary = refresh_stable_rotation_proposal(
                data_dir,
                config_dir,
                state,
                now_ts=generated_at,
            )
        except Exception as exc:
            try:
                proposal_summary = write_blocked_stable_rotation_proposal(
                    data_dir,
                    state,
                    exc,
                )
            except Exception:
                proposal_summary = {
                    "status": "blocked",
                    "error_type": type(exc).__name__,
                }
        if proposal_summary is not None:
            state["stable_rotation_proposal"] = proposal_summary
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
