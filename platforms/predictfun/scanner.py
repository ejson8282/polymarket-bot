from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .client import PredictFunClient, PREDICT_TESTNET_BASE, as_decimal


class UnsupportedPredictMarket(ValueError):
    pass


TRADEABLE_LIFECYCLE_STATUSES = frozenset({"OPEN", "REGISTERED"})


@dataclass
class PredictMarket:
    id: int
    title: str
    question: str
    status: str
    trading_status: str
    market_variant: str
    category_slug: str
    decimal_precision: int
    fee_rate_bps: int
    spread_threshold: Decimal
    share_threshold: Decimal
    hourly_rate: Decimal
    reward_starts_at: str
    reward_ends_at: str
    starts_at: str
    ends_at: str
    is_neg_risk: bool
    is_yield_bearing: bool
    yes_token_id: str
    no_token_id: str
    yes_label: str
    no_label: str
    best_yes_bid: Decimal
    best_yes_ask: Decimal
    mid: Decimal
    quoted_spread: Decimal
    score: Decimal
    risk_note: str


def normalize_market(raw: dict[str, Any], *, scoring_profile: str = "conservative") -> PredictMarket:
    rewards = raw.get("rewards") if isinstance(raw.get("rewards"), dict) else {}
    current = rewards.get("current") if isinstance(rewards.get("current"), dict) else {}
    outcomes = resolve_binary_outcomes(raw)
    yes = outcomes["YES"]
    no = outcomes["NO"]

    best_yes_bid = _level_price(yes.get("bestBid"))
    best_yes_ask = _level_price(yes.get("bestAsk"))
    if best_yes_bid <= 0 and _level_price(no.get("bestAsk")) > 0:
        best_yes_bid = Decimal("1") - _level_price(no.get("bestAsk"))
    if best_yes_ask <= 0 and _level_price(no.get("bestBid")) > 0:
        best_yes_ask = Decimal("1") - _level_price(no.get("bestBid"))

    mid = Decimal("0")
    quoted_spread = Decimal("0")
    if best_yes_bid > 0 and best_yes_ask > 0 and best_yes_ask >= best_yes_bid:
        mid = (best_yes_bid + best_yes_ask) / Decimal("2")
        quoted_spread = best_yes_ask - best_yes_bid

    hourly_rate = as_decimal(current.get("hourlyRate"))
    share_threshold = as_decimal(raw.get("shareThreshold"))
    spread_threshold = as_decimal(raw.get("spreadThreshold"))
    score, risk_note = score_market(
        hourly_rate=hourly_rate,
        share_threshold=share_threshold,
        spread_threshold=spread_threshold,
        quoted_spread=quoted_spread,
        mid=mid,
        market_variant=str(raw.get("marketVariant") or "").upper(),
        ends_at=str(raw.get("endsAt") or ""),
        scoring_profile=scoring_profile,
    )

    return PredictMarket(
        id=int(raw.get("id") or 0),
        title=str(raw.get("title") or ""),
        question=str(raw.get("question") or ""),
        status=str(raw.get("status") or "").upper(),
        trading_status=str(raw.get("tradingStatus") or "").upper(),
        market_variant=str(raw.get("marketVariant") or "").upper(),
        category_slug=str(raw.get("categorySlug") or ""),
        decimal_precision=int(raw.get("decimalPrecision") or 2),
        fee_rate_bps=int(raw.get("feeRateBps") or 0),
        spread_threshold=spread_threshold,
        share_threshold=share_threshold,
        hourly_rate=hourly_rate,
        reward_starts_at=str(current.get("startsAt") or ""),
        reward_ends_at=str(current.get("endsAt") or ""),
        starts_at=str(raw.get("startsAt") or ""),
        ends_at=str(raw.get("endsAt") or ""),
        is_neg_risk=_bool_value(raw.get("isNegRisk"), field="isNegRisk"),
        is_yield_bearing=_bool_value(raw.get("isYieldBearing"), field="isYieldBearing"),
        yes_token_id=str(yes.get("onChainId") or ""),
        no_token_id=str(no.get("onChainId") or ""),
        yes_label=_outcome_label(yes),
        no_label=_outcome_label(no),
        best_yes_bid=best_yes_bid,
        best_yes_ask=best_yes_ask,
        mid=mid,
        quoted_spread=quoted_spread,
        score=score,
        risk_note=risk_note,
    )


def score_market(
    *,
    hourly_rate: Decimal,
    share_threshold: Decimal,
    spread_threshold: Decimal,
    quoted_spread: Decimal,
    mid: Decimal,
    market_variant: str,
    ends_at: str,
    scoring_profile: str = "conservative",
) -> tuple[Decimal, str]:
    min_size = max(share_threshold, Decimal("1"))
    reward_eff = hourly_rate / min_size
    spread_bonus = spread_threshold * Decimal("10")
    crowd_penalty = Decimal("0")
    if quoted_spread > 0 and spread_threshold > 0:
        crowd_penalty = max(Decimal("0"), Decimal("1") - quoted_spread / spread_threshold)

    risk_penalty = Decimal("0")
    notes: list[str] = []
    if market_variant == "CRYPTO_UP_DOWN":
        risk_penalty += Decimal("2")
        notes.append("short-window crypto direction")
    if Decimal("0.35") <= mid <= Decimal("0.65"):
        profile = str(scoring_profile or "conservative").strip().lower()
        if profile not in {"conservative", "balanced", "points"}:
            profile = "conservative"
        if profile == "conservative":
            risk_penalty += Decimal("0.4")
            notes.append("mid near 50/50")
        elif profile == "balanced":
            risk_penalty += Decimal("0.1")
            notes.append("mid liquidity zone")
        else:
            notes.append("mid points zone")
    if _seconds_to(ends_at) is not None and (_seconds_to(ends_at) or 0) < 3600:
        risk_penalty += Decimal("0.8")
        notes.append("near expiry")

    score = reward_eff + spread_bonus - crowd_penalty - risk_penalty
    return score, ", ".join(notes) if notes else "normal"


def scan_markets(
    client: PredictFunClient,
    *,
    max_markets: int = 50,
    first: int = 50,
    has_active_rewards: bool = True,
    min_hourly_rate: Decimal = Decimal("0"),
    include_crypto_updown: bool = False,
    scoring_profile: str = "conservative",
    status_filter: str | None = "OPEN",
) -> list[PredictMarket]:
    out: list[PredictMarket] = []
    cursor: str | None = None
    while len(out) < max_markets:
        page = client.list_markets(
            first=min(first, max_markets),
            after=cursor,
            status=status_filter,
            has_active_rewards=has_active_rewards,
        )
        items = page.get("data") if isinstance(page.get("data"), list) else []
        if not items:
            break
        for item in items:
            try:
                market = normalize_market(item, scoring_profile=scoring_profile)
            except UnsupportedPredictMarket:
                continue
            if not market_is_tradeable(market.status, market.trading_status):
                continue
            if market.hourly_rate < min_hourly_rate:
                continue
            if not include_crypto_updown and market.market_variant == "CRYPTO_UP_DOWN":
                continue
            out.append(market)
            if len(out) >= max_markets:
                break
        next_cursor = page.get("cursor")
        if not next_cursor or next_cursor == cursor:
            break
        cursor = str(next_cursor)
    out.sort(key=lambda m: m.score, reverse=True)
    return out


def market_to_jsonable(market: PredictMarket) -> dict[str, Any]:
    data = asdict(market)
    for key, value in list(data.items()):
        if isinstance(value, Decimal):
            data[key] = str(value)
    return data


def _level_price(value: Any) -> Decimal:
    if isinstance(value, dict):
        return as_decimal(value.get("price"))
    return as_decimal(value)


def market_is_tradeable(status: Any, trading_status: Any) -> bool:
    lifecycle = str(status or "").strip().upper()
    matching = str(trading_status or "").strip().upper()
    return lifecycle in TRADEABLE_LIFECYCLE_STATUSES and matching == "OPEN"


def resolve_binary_outcomes(raw: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Map Predict's two complementary outcomes onto stable internal YES/NO roles.

    Predict uses ``indexSet`` 1 and 2 as the protocol-level outcome identity even
    when the display labels are values such as ``$60`` and ``$140``.  Older test
    fixtures and some legacy markets only expose literal YES/NO labels, so that
    remains a fail-closed compatibility fallback.
    """

    outcomes = raw.get("outcomes") if isinstance(raw.get("outcomes"), list) else []
    if len(outcomes) != 2 or not all(isinstance(outcome, dict) for outcome in outcomes):
        raise UnsupportedPredictMarket(
            f"continuous maker supports exactly two outcomes; market={raw.get('id')} outcomes={len(outcomes)}"
        )

    by_index: dict[int, dict[str, Any]] = {}
    index_complete = True
    for outcome in outcomes:
        index_set = _strict_index_set(outcome.get("indexSet"))
        if index_set is None or index_set in by_index:
            index_complete = False
            break
        by_index[index_set] = outcome
    if index_complete and set(by_index) == {1, 2}:
        resolved = {"YES": by_index[1], "NO": by_index[2]}
    else:
        by_name = {_outcome_label(outcome).upper(): outcome for outcome in outcomes}
        if set(by_name) != {"YES", "NO"}:
            raise UnsupportedPredictMarket(
                f"continuous maker requires indexSet 1/2 or canonical YES/NO outcomes; market={raw.get('id')}"
            )
        resolved = {"YES": by_name["YES"], "NO": by_name["NO"]}

    token_ids = [str(outcome.get("onChainId") or "").strip() for outcome in resolved.values()]
    if not all(token_ids) or len(set(token_ids)) != 2:
        raise UnsupportedPredictMarket(
            f"continuous maker requires two distinct outcome token ids; market={raw.get('id')}"
        )
    return resolved


def _strict_index_set(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed in {1, 2} else None


def _outcome_label(outcome: dict[str, Any]) -> str:
    return str(outcome.get("name") or outcome.get("label") or "").strip()


def _bool_value(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise UnsupportedPredictMarket(f"continuous maker requires boolean {field}")


def _seconds_to(value: str) -> float | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return (dt - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan Predict.fun markets.")
    parser.add_argument("--base-url", default=PREDICT_TESTNET_BASE)
    parser.add_argument("--api-key", default="")
    parser.add_argument("--max-markets", type=int, default=20)
    parser.add_argument("--min-hourly-rate", default="0")
    parser.add_argument("--include-crypto-updown", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    client = PredictFunClient(base_url=args.base_url, api_key=args.api_key)
    markets = scan_markets(
        client,
        max_markets=args.max_markets,
        min_hourly_rate=Decimal(str(args.min_hourly_rate)),
        include_crypto_updown=args.include_crypto_updown,
    )
    if args.json:
        print(json.dumps([market_to_jsonable(m) for m in markets], indent=2))
        return

    for idx, market in enumerate(markets, 1):
        print(
            f"{idx:02d}. score={market.score:.4f} hourly={market.hourly_rate} "
            f"min_size={market.share_threshold} spread_thr={market.spread_threshold} "
            f"mid={market.mid} book_spread={market.quoted_spread} id={market.id}"
        )
        print(f"    {market.title}")
        print(f"    risk={market.risk_note}")


if __name__ == "__main__":
    main()
