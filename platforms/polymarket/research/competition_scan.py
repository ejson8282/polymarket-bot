"""Competition-aware reward share scan over Polymarket sampling markets.

For every reward-eligible market this tool measures the in-range competitor
Q (from the public order book), then simulates our own dual-side executable
quote over a capital grid with the exact scoring the engine and observer
share (``quote_feasibility``). The output answers, per market: how much of
the daily reward pool one marginal dollar earns today, and where that rate
stops improving.

Read-only: public endpoints only, no credentials, no orders, no config
writes. Safe to run repeatedly (``--jsonl``) to build an hourly competition
time series.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence
from zoneinfo import ZoneInfo

from platforms.polymarket.maker.quote_feasibility import (
    aggregate_bid_q,
    evaluate_paired_quote,
    normalize_reward_spread,
)

try:  # CLI convenience; the pure evaluation path never touches the network.
    from .public_data import fetch_books, fetch_sampling_markets, new_session
except ImportError:  # pragma: no cover - direct script execution
    from public_data import fetch_books, fetch_sampling_markets, new_session


ZERO = Decimal("0")
DEFAULT_CAPITAL_GRID = (
    Decimal("100"),
    Decimal("250"),
    Decimal("500"),
    Decimal("1000"),
    Decimal("2500"),
    Decimal("5000"),
)
DEFAULT_MIN_ORDER_SIZE = Decimal("5")
_CST = ZoneInfo("Asia/Shanghai")


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


@dataclass(frozen=True)
class MarketInfo:
    condition_id: str
    question: str
    slug: str
    yes_token: str
    no_token: str
    tick: Decimal
    max_spread: Decimal
    rewards_min_size: Decimal
    daily_rate_usd: Decimal


def parse_sampling_market(row: Mapping[str, Any]) -> Optional[MarketInfo]:
    """Convert one /sampling-markets row into a typed record, or None."""
    tokens = row.get("tokens")
    if not isinstance(tokens, list) or len(tokens) != 2:
        return None
    outcome_map: dict[str, str] = {}
    ordered: list[str] = []
    for token in tokens:
        if not isinstance(token, Mapping):
            return None
        token_id = str(token.get("token_id") or "")
        if not token_id:
            return None
        ordered.append(token_id)
        outcome_map[str(token.get("outcome") or "").strip().lower()] = token_id
    yes_token = outcome_map.get("yes", ordered[0])
    no_token = outcome_map.get("no", ordered[1])
    if yes_token == no_token:
        return None

    rewards = row.get("rewards")
    if not isinstance(rewards, Mapping):
        return None
    daily_rate = ZERO
    rates = rewards.get("rates")
    if isinstance(rates, list):
        for rate in rates:
            if isinstance(rate, Mapping):
                daily_rate += _decimal(rate.get("rewards_daily_rate"))
    max_spread = _decimal(rewards.get("max_spread"))
    min_size = _decimal(rewards.get("min_size"))
    tick = _decimal(row.get("minimum_tick_size"), Decimal("0.01"))
    if daily_rate <= ZERO or max_spread <= ZERO or tick <= ZERO:
        return None
    return MarketInfo(
        condition_id=str(row.get("condition_id") or ""),
        question=str(row.get("question") or ""),
        slug=str(row.get("market_slug") or ""),
        yes_token=yes_token,
        no_token=no_token,
        tick=tick,
        max_spread=max_spread,
        rewards_min_size=min_size,
        daily_rate_usd=daily_rate,
    )


def book_levels(book: Optional[Mapping[str, Any]], side: str) -> list[tuple[Decimal, Decimal]]:
    levels: list[tuple[Decimal, Decimal]] = []
    if not isinstance(book, Mapping):
        return levels
    for row in book.get(side) or []:
        if not isinstance(row, Mapping):
            continue
        price = _decimal(row.get("price"))
        size = _decimal(row.get("size"))
        if price > ZERO and size > ZERO:
            levels.append((price, size))
    reverse = side == "bids"
    levels.sort(key=lambda level: level[0], reverse=reverse)
    return levels


def book_top(book: Optional[Mapping[str, Any]]) -> tuple[Decimal, Decimal]:
    bids = book_levels(book, "bids")
    asks = book_levels(book, "asks")
    best_bid = bids[0][0] if bids else ZERO
    best_ask = asks[0][0] if asks else ZERO
    return best_bid, best_ask


def evaluate_market(
    info: MarketInfo,
    yes_book: Optional[Mapping[str, Any]],
    no_book: Optional[Mapping[str, Any]],
    *,
    capital_grid: Sequence[Decimal] = DEFAULT_CAPITAL_GRID,
    min_order_size: Decimal = DEFAULT_MIN_ORDER_SIZE,
    min_distance_ticks: int = 1,
) -> dict[str, Any]:
    """Score one market: competitor Q plus our simulated share per capital."""
    yes_bids = book_levels(yes_book, "bids")
    no_bids = book_levels(no_book, "bids")
    yes_best_bid, yes_best_ask = book_top(yes_book)
    no_best_bid, no_best_ask = book_top(no_book)

    row: dict[str, Any] = {
        "condition_id": info.condition_id,
        "slug": info.slug,
        "question": info.question,
        "yes_token": info.yes_token,
        "no_token": info.no_token,
        "tick": str(info.tick),
        "rewards_max_spread": str(info.max_spread),
        "rewards_min_size": str(info.rewards_min_size),
        "daily_rate_usd": str(info.daily_rate_usd),
        "yes_best_bid": str(yes_best_bid),
        "yes_best_ask": str(yes_best_ask),
        "no_best_bid": str(no_best_bid),
        "no_best_ask": str(no_best_ask),
        "capital_curve": [],
        "blocked_reason": "",
    }

    if (
        yes_best_bid <= ZERO
        or yes_best_ask <= ZERO
        or yes_best_ask < yes_best_bid
        or no_best_bid <= ZERO
        or no_best_ask <= ZERO
        or no_best_ask < no_best_bid
    ):
        row["blocked_reason"] = "book_empty_or_crossed"
        return row

    yes_mid = (yes_best_bid + yes_best_ask) / Decimal("2")
    no_mid = (no_best_bid + no_best_ask) / Decimal("2")
    spread = normalize_reward_spread(info.max_spread)
    competition_q = min(
        aggregate_bid_q(yes_bids, midpoint=yes_mid, max_spread=spread),
        aggregate_bid_q(no_bids, midpoint=no_mid, max_spread=spread),
    )
    row["yes_mid"] = str(yes_mid)
    row["no_mid"] = str(no_mid)
    row["competition_q_min"] = str(competition_q)

    curve: list[dict[str, Any]] = []
    previous: Optional[dict[str, Any]] = None
    for capital in capital_grid:
        result = evaluate_paired_quote(
            yes_bids=yes_bids,
            no_bids=no_bids,
            yes_best_bid=yes_best_bid,
            no_best_bid=no_best_bid,
            yes_midpoint=yes_mid,
            no_midpoint=no_mid,
            yes_tick=info.tick,
            no_tick=info.tick,
            max_spread=info.max_spread,
            min_distance_ticks=min_distance_ticks,
            available=capital,
            rewards_min=info.rewards_min_size,
            min_order_size=min_order_size,
        )
        expected_daily = info.daily_rate_usd * result.executable_share
        entry: dict[str, Any] = {
            "capital_usd": str(capital),
            "target_shares": str(result.target_shares),
            "executable_share": str(result.executable_share),
            "expected_daily_usd": str(expected_daily),
            "daily_roi_pct": str(
                (expected_daily / capital * Decimal("100")) if capital > ZERO else ZERO
            ),
            "blocked_reasons": list(result.blocked_reasons),
        }
        if previous is not None:
            delta_capital = capital - _decimal(previous["capital_usd"])
            delta_reward = expected_daily - _decimal(previous["expected_daily_usd"])
            entry["marginal_daily_roi_pct"] = str(
                (delta_reward / delta_capital * Decimal("100"))
                if delta_capital > ZERO
                else ZERO
            )
        curve.append(entry)
        previous = entry
    row["capital_curve"] = curve
    return row


def _reference_roi(row: Mapping[str, Any], reference_capital: Decimal) -> Decimal:
    for entry in row.get("capital_curve") or []:
        if _decimal(entry.get("capital_usd")) == reference_capital:
            return _decimal(entry.get("daily_roi_pct"))
    return ZERO


def scan_once(
    *,
    capital_grid: Sequence[Decimal] = DEFAULT_CAPITAL_GRID,
    reference_capital: Decimal = Decimal("1000"),
    min_daily_rate_usd: Decimal = Decimal("5"),
    max_markets: int = 0,
    min_order_size: Decimal = DEFAULT_MIN_ORDER_SIZE,
    save_books_dir: Optional[Path] = None,
    session: Any = None,
) -> dict[str, Any]:
    session = session or new_session()
    raw_markets = fetch_sampling_markets(session)
    infos = [
        info
        for info in (parse_sampling_market(row) for row in raw_markets)
        if info is not None and info.daily_rate_usd >= min_daily_rate_usd
    ]
    infos.sort(key=lambda info: info.daily_rate_usd, reverse=True)
    if max_markets > 0:
        infos = infos[:max_markets]

    token_ids = [token for info in infos for token in (info.yes_token, info.no_token)]
    books = fetch_books(token_ids, session)
    if save_books_dir is not None:
        save_books_dir.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time())
        (save_books_dir / f"books_{stamp}.json").write_text(
            json.dumps(books, ensure_ascii=True), encoding="utf-8"
        )

    rows = [
        evaluate_market(
            info,
            books.get(info.yes_token),
            books.get(info.no_token),
            capital_grid=capital_grid,
            min_order_size=min_order_size,
        )
        for info in infos
    ]
    rows.sort(key=lambda row: _reference_roi(row, reference_capital), reverse=True)
    now = time.time()
    utc = _dt.datetime.fromtimestamp(now, tz=_dt.timezone.utc)
    cst = utc.astimezone(_CST)
    return {
        "generated_at": now,
        "generated_at_utc": utc.isoformat(timespec="seconds"),
        "hour_utc": utc.hour,
        "hour_cst": cst.hour,
        "reference_capital_usd": str(reference_capital),
        "markets_scanned": len(rows),
        "markets": rows,
    }


def _append_jsonl(path: Path, report: Mapping[str, Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("a", encoding="utf-8") as handle:
        for row in report.get("markets") or []:
            record = {
                "generated_at": report.get("generated_at"),
                "hour_utc": report.get("hour_utc"),
                "hour_cst": report.get("hour_cst"),
                **row,
            }
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
            written += 1
    return written


def _print_summary(report: Mapping[str, Any], top: int) -> None:
    reference = report.get("reference_capital_usd")
    print(
        f"scanned={report.get('markets_scanned')} markets "
        f"at {report.get('generated_at_utc')} (ref capital ${reference})"
    )
    header = f"{'slug':<48} {'pool$/d':>8} {'compQ':>10} {'share':>7} {'exp$/d':>8} {'roi%/d':>7}"
    print(header)
    for row in (report.get("markets") or [])[:top]:
        entry = next(
            (
                item
                for item in row.get("capital_curve") or []
                if item.get("capital_usd") == str(reference)
            ),
            None,
        )
        share = entry.get("executable_share", "0") if entry else "0"
        expected = entry.get("expected_daily_usd", "0") if entry else "0"
        roi = entry.get("daily_roi_pct", "0") if entry else "0"
        print(
            f"{(row.get('slug') or row.get('condition_id') or '?')[:48]:<48} "
            f"{_decimal(row.get('daily_rate_usd')):>8.2f} "
            f"{_decimal(row.get('competition_q_min')):>10.1f} "
            f"{_decimal(share):>7.4f} "
            f"{_decimal(expected):>8.2f} "
            f"{_decimal(roi):>7.3f}"
        )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("competition_scan.json"))
    parser.add_argument("--jsonl", type=Path, default=None,
                        help="append per-market rows for time-series sampling")
    parser.add_argument("--capital", type=str,
                        default=",".join(str(c) for c in DEFAULT_CAPITAL_GRID))
    parser.add_argument("--ref-capital", type=str, default="1000")
    parser.add_argument("--min-daily-rate", type=str, default="5")
    parser.add_argument("--max-markets", type=int, default=0)
    parser.add_argument("--save-books", type=Path, default=None)
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args(argv)

    capital_grid = tuple(
        sorted(_decimal(item) for item in args.capital.split(",") if item.strip())
    )
    if not capital_grid or any(value <= ZERO for value in capital_grid):
        parser.error("--capital must be positive amounts")
    report = scan_once(
        capital_grid=capital_grid,
        reference_capital=_decimal(args.ref_capital),
        min_daily_rate_usd=_decimal(args.min_daily_rate),
        max_markets=args.max_markets,
        save_books_dir=args.save_books,
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    if args.jsonl is not None:
        _append_jsonl(args.jsonl, report)
    _print_summary(report, args.top)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
