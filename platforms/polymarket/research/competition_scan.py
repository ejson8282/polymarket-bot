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


def _iso_ts(value: Any) -> Optional[float]:
    """Parse an ISO-8601 timestamp (Z suffix tolerated) to unix seconds."""
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return _dt.datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


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
    end_ts: Optional[float] = None
    game_start_ts: Optional[float] = None


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
        end_ts=_iso_ts(row.get("end_date_iso")),
        game_start_ts=_iso_ts(row.get("game_start_time")),
    )


def market_passes_guards(
    info: MarketInfo,
    *,
    now_ts: float,
    min_hours_to_end: float = 0.0,
    min_hours_to_game_start: float = 0.0,
    exclude_slug_keywords: Sequence[str] = (),
) -> tuple[bool, str]:
    """Observer-style eligibility guards for actionable candidate lists.

    All guards default off so the raw scan stays a neutral measurement.
    """
    slug = info.slug.lower()
    for keyword in exclude_slug_keywords:
        needle = keyword.strip().lower()
        if needle and needle in slug:
            return False, f"slug_excluded:{needle}"
    if min_hours_to_end > 0:
        if info.end_ts is None:
            return False, "end_date_unknown"
        if info.end_ts - now_ts < min_hours_to_end * 3600.0:
            return False, "too_close_to_end"
    if min_hours_to_game_start > 0 and info.game_start_ts is not None:
        if info.game_start_ts - now_ts < min_hours_to_game_start * 3600.0:
            return False, "too_close_to_game_start"
    return True, ""


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
        "end_ts": info.end_ts,
        "hours_to_end": (
            round((info.end_ts - time.time()) / 3600.0, 1)
            if info.end_ts is not None
            else None
        ),
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
    min_hours_to_end: float = 0.0,
    min_hours_to_game_start: float = 0.0,
    exclude_slug_keywords: Sequence[str] = (),
    save_books_dir: Optional[Path] = None,
    session: Any = None,
) -> dict[str, Any]:
    session = session or new_session()
    raw_markets = fetch_sampling_markets(session)
    now_ts = time.time()
    guards_dropped: dict[str, int] = {}
    infos = []
    for row in raw_markets:
        info = parse_sampling_market(row)
        if info is None or info.daily_rate_usd < min_daily_rate_usd:
            continue
        passed, guard_reason = market_passes_guards(
            info,
            now_ts=now_ts,
            min_hours_to_end=min_hours_to_end,
            min_hours_to_game_start=min_hours_to_game_start,
            exclude_slug_keywords=exclude_slug_keywords,
        )
        if not passed:
            guards_dropped[guard_reason] = guards_dropped.get(guard_reason, 0) + 1
            continue
        infos.append(info)
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
        "guards_dropped": guards_dropped,
        "markets": rows,
    }


def portfolio_view(
    report: Mapping[str, Any],
    *,
    principal_usd: Decimal,
    top_n: int = 20,
    min_daily_roi_pct: Decimal = ZERO,
) -> dict[str, Any]:
    """Stack per-market rewards on one reused principal.

    Polymarket BUY orders lock no collateral until they match, so one
    principal can quote many markets at once. This view sums the expected
    daily reward of the top eligible markets, each quoted with the full
    principal, and reports the over-commit multiple: how many times the
    principal would be needed if every quoted market filled at once. The
    over-commit multiple is the risk knob — rewards stack, but so does the
    simultaneous-fill exposure.
    """
    reference = _decimal(report.get("reference_capital_usd"), principal_usd)
    picked: list[dict[str, Any]] = []
    total_expected = ZERO
    total_open_collateral = ZERO
    for row in report.get("markets") or []:
        if row.get("blocked_reason"):
            continue
        entry = next(
            (
                item
                for item in row.get("capital_curve") or []
                if _decimal(item.get("capital_usd")) == reference
            ),
            None,
        )
        if entry is None or entry.get("blocked_reasons"):
            continue
        roi = _decimal(entry.get("daily_roi_pct"))
        if roi < min_daily_roi_pct:
            continue
        expected = _decimal(entry.get("expected_daily_usd"))
        # Dual-side collateral if both legs of the event fill: shares x $1.
        open_collateral = _decimal(entry.get("target_shares"))
        picked.append(
            {
                "slug": row.get("slug"),
                "condition_id": row.get("condition_id"),
                "hours_to_end": row.get("hours_to_end"),
                "daily_rate_usd": row.get("daily_rate_usd"),
                "competition_q_min": row.get("competition_q_min"),
                "executable_share": entry.get("executable_share"),
                "expected_daily_usd": str(expected),
                "open_collateral_if_filled_usd": str(open_collateral),
            }
        )
        total_expected += expected
        total_open_collateral += open_collateral
        if len(picked) >= top_n:
            break
    return {
        "principal_usd": str(principal_usd),
        "per_market_quote_usd": str(reference),
        "markets_quoted": len(picked),
        "total_expected_daily_usd": str(total_expected),
        "stacked_daily_roi_pct": str(
            (total_expected / principal_usd * Decimal("100"))
            if principal_usd > ZERO
            else ZERO
        ),
        "overcommit_multiple": str(
            (total_open_collateral / principal_usd)
            if principal_usd > ZERO
            else ZERO
        ),
        "markets": picked,
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
    parser.add_argument("--min-hours-to-end", type=float, default=0.0,
                        help="drop markets ending sooner (0 = off)")
    parser.add_argument("--min-hours-to-game-start", type=float, default=0.0,
                        help="drop markets whose game starts sooner (0 = off)")
    parser.add_argument("--exclude-slugs", type=str, default="",
                        help="comma-separated slug keywords to drop")
    parser.add_argument("--portfolio-principal", type=str, default="0",
                        help="stacked portfolio view for this principal (0 = off)")
    parser.add_argument("--portfolio-top", type=int, default=20)
    parser.add_argument("--portfolio-min-roi", type=str, default="0.5",
                        help="min per-market daily ROI pct for the portfolio")
    args = parser.parse_args(argv)

    capital_grid = tuple(
        sorted(_decimal(item) for item in args.capital.split(",") if item.strip())
    )
    if not capital_grid or any(value <= ZERO for value in capital_grid):
        parser.error("--capital must be positive amounts")
    reference_capital = _decimal(args.ref_capital)
    if reference_capital not in capital_grid:
        capital_grid = tuple(sorted((*capital_grid, reference_capital)))
    report = scan_once(
        capital_grid=capital_grid,
        reference_capital=reference_capital,
        min_daily_rate_usd=_decimal(args.min_daily_rate),
        max_markets=args.max_markets,
        min_hours_to_end=args.min_hours_to_end,
        min_hours_to_game_start=args.min_hours_to_game_start,
        exclude_slug_keywords=tuple(
            item for item in args.exclude_slugs.split(",") if item.strip()
        ),
        save_books_dir=args.save_books,
    )
    principal = _decimal(args.portfolio_principal)
    if principal > ZERO:
        report["portfolio"] = portfolio_view(
            report,
            principal_usd=principal,
            top_n=args.portfolio_top,
            min_daily_roi_pct=_decimal(args.portfolio_min_roi),
        )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8"
    )
    if args.jsonl is not None:
        _append_jsonl(args.jsonl, report)
    _print_summary(report, args.top)
    portfolio = report.get("portfolio")
    if portfolio:
        print(
            f"portfolio: principal=${portfolio['principal_usd']} "
            f"markets={portfolio['markets_quoted']} "
            f"expected=${_decimal(portfolio['total_expected_daily_usd']):.2f}/d "
            f"stacked_roi={_decimal(portfolio['stacked_daily_roi_pct']):.2f}%/d "
            f"overcommit=x{_decimal(portfolio['overcommit_multiple']):.1f}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
