"""Fill markout analysis from the public trade feed, read-only.

For each fill of a funder (proxy-wallet) address this tool measures how the
market price moved 1/5/30 minutes later. Sign convention: positive markout
means the price moved in our favor after the fill (for a BUY, price rose).
A persistently negative mean markout identifies informed (toxic) flow that
justifies fast exits; a mean near zero identifies noise flow where the
current fire-sale exit pays spread for no protection.

Inputs are public: the data-api trade feed and CLOB prices-history are
keyed by on-chain settlement and need no credentials. Funder addresses are
public chain addresses, not secrets.
"""

from __future__ import annotations

import argparse
import bisect
import json
import statistics
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence

try:
    from .public_data import fetch_prices_history, fetch_user_trades, new_session
except ImportError:  # pragma: no cover - direct script execution
    from public_data import fetch_prices_history, fetch_user_trades, new_session


ZERO = Decimal("0")
DEFAULT_HORIZONS_SEC = (60, 300, 1800)
DEFAULT_PRICE_TOLERANCE_SEC = 150


def _decimal(value: Any, default: Decimal = ZERO) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return default
    return parsed if parsed.is_finite() else default


@dataclass(frozen=True)
class Fill:
    ts: int
    token_id: str
    condition_id: str
    side: str  # BUY or SELL, as reported for this wallet by the data-api
    price: Decimal
    size: Decimal
    slug: str
    outcome: str
    tx: str


def normalize_trades(rows: Iterable[Mapping[str, Any]]) -> list[Fill]:
    fills: list[Fill] = []
    for row in rows:
        side = str(row.get("side") or "").strip().upper()
        token_id = str(row.get("asset") or row.get("token_id") or "")
        price = _decimal(row.get("price"))
        size = _decimal(row.get("size"))
        try:
            ts = int(row.get("timestamp") or 0)
        except (TypeError, ValueError):
            ts = 0
        if side not in {"BUY", "SELL"} or not token_id or ts <= 0:
            continue
        if price <= ZERO or size <= ZERO:
            continue
        fills.append(
            Fill(
                ts=ts,
                token_id=token_id,
                condition_id=str(row.get("conditionId") or row.get("condition_id") or ""),
                side=side,
                price=price,
                size=size,
                slug=str(row.get("slug") or ""),
                outcome=str(row.get("outcome") or ""),
                tx=str(row.get("transactionHash") or ""),
            )
        )
    fills.sort(key=lambda fill: fill.ts)
    return fills


def price_at(
    series: Sequence[tuple[int, float]],
    target_ts: int,
    *,
    tolerance_sec: int = DEFAULT_PRICE_TOLERANCE_SEC,
) -> Optional[Decimal]:
    """Nearest sample within tolerance; None when the series has a gap."""
    if not series:
        return None
    timestamps = [ts for ts, _ in series]
    index = bisect.bisect_left(timestamps, target_ts)
    best: Optional[tuple[int, float]] = None
    for candidate in (index - 1, index):
        if 0 <= candidate < len(series):
            ts, price = series[candidate]
            if abs(ts - target_ts) <= tolerance_sec and (
                best is None or abs(ts - target_ts) < abs(best[0] - target_ts)
            ):
                best = (ts, price)
    return _decimal(best[1]) if best is not None else None


def markout_for_fill(
    fill: Fill,
    series: Sequence[tuple[int, float]],
    horizon_sec: int,
    *,
    tolerance_sec: int = DEFAULT_PRICE_TOLERANCE_SEC,
) -> Optional[Decimal]:
    """Signed per-share markout in USDC; None when prices are missing."""
    future = price_at(series, fill.ts + horizon_sec, tolerance_sec=tolerance_sec)
    if future is None:
        return None
    if fill.side == "BUY":
        return future - fill.price
    return fill.price - future


def _aggregate(samples: list[tuple[Decimal, Decimal]]) -> dict[str, Any]:
    """Aggregate (markout_per_share, size) samples."""
    values = [float(markout) for markout, _ in samples]
    total_size = sum((size for _, size in samples), ZERO)
    weighted = sum((markout * size for markout, size in samples), ZERO)
    wins = sum(1 for markout, _ in samples if markout > ZERO)
    return {
        "n": len(samples),
        "mean_per_share": statistics.fmean(values) if values else 0.0,
        "median_per_share": statistics.median(values) if values else 0.0,
        "weighted_mean_per_share": (
            float(weighted / total_size) if total_size > ZERO else 0.0
        ),
        "total_usd": float(weighted),
        "win_rate": (wins / len(samples)) if samples else 0.0,
    }


def summarize_markouts(
    fills: Sequence[Fill],
    series_by_token: Mapping[str, Sequence[tuple[int, float]]],
    *,
    horizons_sec: Sequence[int] = DEFAULT_HORIZONS_SEC,
    tolerance_sec: int = DEFAULT_PRICE_TOLERANCE_SEC,
) -> dict[str, Any]:
    by_horizon: dict[int, list[tuple[Decimal, Decimal]]] = {h: [] for h in horizons_sec}
    by_market: dict[str, dict[int, list[tuple[Decimal, Decimal]]]] = {}
    skipped = 0
    for fill in fills:
        series = series_by_token.get(fill.token_id) or ()
        market_key = fill.slug or fill.condition_id or fill.token_id
        for horizon in horizons_sec:
            markout = markout_for_fill(
                fill, series, horizon, tolerance_sec=tolerance_sec
            )
            if markout is None:
                skipped += 1
                continue
            sample = (markout, fill.size)
            by_horizon[horizon].append(sample)
            by_market.setdefault(market_key, {h: [] for h in horizons_sec})[
                horizon
            ].append(sample)
    return {
        "fills_total": len(fills),
        "samples_skipped_missing_price": skipped,
        "by_horizon_sec": {
            str(horizon): _aggregate(samples)
            for horizon, samples in by_horizon.items()
        },
        "by_market": {
            market: {
                str(horizon): _aggregate(samples)
                for horizon, samples in horizon_map.items()
                if samples
            }
            for market, horizon_map in sorted(by_market.items())
        },
    }


def collect_price_series(
    fills: Sequence[Fill],
    *,
    horizons_sec: Sequence[int] = DEFAULT_HORIZONS_SEC,
    session: Any = None,
    fidelity_min: int = 1,
) -> dict[str, list[tuple[int, float]]]:
    """Fetch one price-history window per token spanning all its fills."""
    session = session or new_session()
    max_horizon = max(horizons_sec) if horizons_sec else 0
    windows: dict[str, tuple[int, int]] = {}
    for fill in fills:
        start, end = windows.get(fill.token_id, (fill.ts, fill.ts))
        windows[fill.token_id] = (min(start, fill.ts), max(end, fill.ts))
    series: dict[str, list[tuple[int, float]]] = {}
    for token_id, (start, end) in windows.items():
        series[token_id] = fetch_prices_history(
            token_id,
            start_ts=start - 120,
            end_ts=end + max_horizon + 300,
            fidelity_min=fidelity_min,
            session=session,
        )
    return series


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user", action="append", required=True,
                        help="funder (proxy wallet) address; repeatable")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--horizons", type=str, default="60,300,1800",
                        help="markout horizons in seconds")
    parser.add_argument("--side", choices=["BUY", "SELL", "ALL"], default="ALL")
    parser.add_argument("--max-fills", type=int, default=2000)
    parser.add_argument("--out", type=Path, default=Path("markout_report.json"))
    args = parser.parse_args(argv)

    horizons = tuple(
        int(item) for item in args.horizons.split(",") if item.strip()
    )
    if not horizons or any(h <= 0 for h in horizons):
        parser.error("--horizons must be positive seconds")
    cutoff_ts = int(time.time()) - args.days * 86400

    session = new_session()
    reports: dict[str, Any] = {}
    for user in args.user:
        rows = fetch_user_trades(user, session, max_rows=args.max_fills)
        fills = [
            fill
            for fill in normalize_trades(rows)
            if fill.ts >= cutoff_ts
            and (args.side == "ALL" or fill.side == args.side)
        ]
        series = collect_price_series(
            fills, horizons_sec=horizons, session=session
        )
        reports[user.lower()] = summarize_markouts(
            fills, series, horizons_sec=horizons
        )
        top = reports[user.lower()]["by_horizon_sec"]
        line = " ".join(
            f"{horizon}s: mean={stats['mean_per_share']:+.4f} "
            f"win={stats['win_rate']:.2f} n={stats['n']}"
            for horizon, stats in top.items()
        )
        print(f"{user.lower()} fills={len(fills)} {line}")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        json.dumps(
            {"generated_at": time.time(), "days": args.days, "users": reports},
            ensure_ascii=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
