"""Refresh Polymarket liquidity rewards independently of the maker engine.

Polymarket reward days use UTC. Midnight UTC is 08:00 in Asia/Shanghai, so
the live cache naturally rolls over at 08:00 Beijing time. Completed days are
stored in ``rewards_cumulative.json`` while the in-progress day is kept in
``rewards_live.json`` and is never included in the finalized cumulative sum.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    from .rewards_snapshot import (
        _build_snapshot_client,
        _dates_between,
        _fetch_daily_reward_usd,
        _load_state,
        _save_state,
    )
except ImportError:
    from rewards_snapshot import (  # type: ignore
        _build_snapshot_client,
        _dates_between,
        _fetch_daily_reward_usd,
        _load_state,
        _save_state,
    )


_BJT = ZoneInfo("Asia/Shanghai")


def _as_utc(now: Optional[datetime] = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc)


def reward_day_utc(now: Optional[datetime] = None) -> date:
    """Return the current Polymarket reward date."""
    return _as_utc(now).date()


def reward_window_bjt(now: Optional[datetime] = None) -> Tuple[datetime, datetime]:
    """Return the current reward window as Beijing-time datetimes."""
    day = reward_day_utc(now)
    start_utc = datetime.combine(day, datetime_time.min, tzinfo=timezone.utc)
    end_utc = start_utc + timedelta(days=1)
    return start_utc.astimezone(_BJT), end_utc.astimezone(_BJT)


def discover_configs(config_dir: Path) -> List[Tuple[int, Path]]:
    return [
        (idx, path)
        for idx in range(1, 31)
        if (path := config_dir / f"config_{idx}.json").exists()
    ]


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _missing_finalized_dates(account_state: dict, finalized_day: date) -> List[str]:
    last_value = account_state.get("last_snapshot_date")
    if last_value:
        try:
            start = date.fromisoformat(str(last_value)) + timedelta(days=1)
        except ValueError:
            start = finalized_day
    else:
        start = finalized_day
    dates = _dates_between(start, finalized_day) if start <= finalized_day else []
    # Re-read the most recent finalized day on every run. This catches a short
    # settlement delay after 00:00 UTC without rewriting older history.
    final_key = finalized_day.isoformat()
    if final_key not in dates:
        dates.append(final_key)
    return dates


async def refresh_rewards(
    configs: Iterable[Tuple[int, Path]],
    data_dir: Path,
    *,
    now: Optional[datetime] = None,
    build_client: Callable[[Path], tuple] = _build_snapshot_client,
    fetch_daily: Callable[[Any, int, str], float] = _fetch_daily_reward_usd,
) -> dict:
    """Fetch the in-progress day and finalize all missing completed days."""
    data_dir.mkdir(parents=True, exist_ok=True)
    now_utc = _as_utc(now)
    current_day = now_utc.date()
    finalized_day = current_day - timedelta(days=1)
    current_key = current_day.isoformat()
    finalized_key = finalized_day.isoformat()
    generated_at = now_utc.isoformat().replace("+00:00", "Z")

    cumulative_path = data_dir / "rewards_cumulative.json"
    live_path = data_dir / "rewards_live.json"
    cumulative = _load_state(cumulative_path)
    previous_live = {}
    try:
        previous_live = json.loads(live_path.read_text(encoding="utf-8"))
    except Exception:
        previous_live = {}

    cumulative_accounts = cumulative.setdefault("accounts", {})
    prior_live_accounts = (
        previous_live.get("accounts", {})
        if isinstance(previous_live, dict)
        and isinstance(previous_live.get("accounts"), dict)
        else {}
    )
    live_accounts: Dict[str, dict] = {}
    successful = 0

    for account_idx, config_path in configs:
        account_key = str(account_idx)
        prior = prior_live_accounts.get(account_key, {})
        if not isinstance(prior, dict) or previous_live.get("reward_date_utc") != current_key:
            prior = {}
        row = {
            "today_usd": prior.get("today_usd"),
            "previous_day_usd": prior.get("previous_day_usd"),
            "status": "error",
            "updated_at": prior.get("updated_at"),
            "error": "reward API unavailable",
        }

        client, address, signature_type, client_error = await asyncio.to_thread(
            build_client, config_path
        )
        if client_error:
            row["error"] = str(client_error).split(":", 1)[0]
            live_accounts[account_key] = row
            continue

        account_state = cumulative_accounts.get(account_key)
        if not isinstance(account_state, dict):
            account_state = {
                "address": address,
                "daily": {},
                "cumulative_usd": 0.0,
                "last_snapshot_date": None,
            }
            cumulative_accounts[account_key] = account_state
        account_state["address"] = address
        daily = account_state.get("daily")
        if not isinstance(daily, dict):
            daily = {}
            account_state["daily"] = daily

        try:
            for day_key in _missing_finalized_dates(account_state, finalized_day):
                amount = await asyncio.to_thread(
                    fetch_daily, client, signature_type, day_key
                )
                existing = float(daily.get(day_key) or 0.0)
                fetched = float(amount)
                # An empty result is common for a few moments at rollover.
                # Preserve a known non-zero value in that case, while allowing
                # positive API corrections in either direction.
                daily[day_key] = round(
                    existing if fetched == 0.0 and existing > 0.0 else fetched,
                    6,
                )

            today_amount = await asyncio.to_thread(
                fetch_daily, client, signature_type, current_key
            )
            account_state["cumulative_usd"] = round(
                sum(float(value or 0.0) for value in daily.values()), 6
            )
            if daily:
                account_state["last_snapshot_date"] = max(daily)
            row = {
                "today_usd": round(float(today_amount), 6),
                "previous_day_usd": round(float(daily.get(finalized_key) or 0.0), 6),
                "status": "ok",
                "updated_at": generated_at,
                "error": None,
            }
            successful += 1
        except Exception as exc:
            row["error"] = type(exc).__name__

        live_accounts[account_key] = row

    start_bjt, end_bjt = reward_window_bjt(now_utc)
    known_today = [
        float(row["today_usd"])
        for row in live_accounts.values()
        if row.get("today_usd") is not None
    ]
    known_previous = [
        float(row["previous_day_usd"])
        for row in live_accounts.values()
        if row.get("previous_day_usd") is not None
    ]
    live_state = {
        "version": 1,
        "generated_at": generated_at,
        "reward_date_utc": current_key,
        "previous_date_utc": finalized_key,
        "window_start_bjt": start_bjt.isoformat(),
        "window_end_bjt": end_bjt.isoformat(),
        "window_label_bjt": (
            f"{start_bjt:%m-%d %H:%M} - {end_bjt:%m-%d %H:%M}"
        ),
        "next_reset_at_bjt": end_bjt.isoformat(),
        "poll_interval_sec": 300,
        "accounts": live_accounts,
        "total_today_usd": round(sum(known_today), 6) if known_today else None,
        "total_previous_usd": (
            round(sum(known_previous), 6) if known_previous else None
        ),
        "successful_accounts": successful,
        "configured_accounts": len(live_accounts),
    }

    cumulative["version"] = 2
    cumulative["updated_at"] = generated_at
    _save_state(cumulative_path, cumulative)
    _atomic_json(live_path, live_state)
    return live_state


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Polymarket reward cache")
    maker_dir = Path(__file__).resolve().parent
    parser.add_argument("--config-dir", type=Path, default=maker_dir)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=maker_dir.parent.parent.parent / "data",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    configs = discover_configs(args.config_dir)
    if not configs:
        print("[rewards-live] no config_N.json files found", flush=True)
        return 1
    state = asyncio.run(refresh_rewards(configs, args.data_dir))
    ok = int(state.get("successful_accounts") or 0)
    total = int(state.get("configured_accounts") or 0)
    print(
        "[rewards-live] "
        f"date={state.get('reward_date_utc')} accounts={ok}/{total} "
        f"today_usd={state.get('total_today_usd')}",
        flush=True,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
