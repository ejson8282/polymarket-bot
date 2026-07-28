"""Refresh Polymarket maker income independently of the maker engine.

Polymarket reward days use UTC. Midnight UTC is 08:00 in Asia/Shanghai, so
the live cache naturally rolls over at 08:00 Beijing time. Completed days are
stored in ``rewards_cumulative.json`` while the in-progress day is kept in
``rewards_live.json`` and is never included in the finalized cumulative sum.
LP rewards and maker rebates are recorded separately and combined only for
display totals.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
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
_REBATES_URL = "https://clob.polymarket.com/rebates/current"


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


def _maker_address_for_config(
    config_path: Path,
    fallback_address: Optional[str] = None,
) -> Optional[str]:
    """Return the account's maker/funder address without exposing credentials."""
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        config = {}
    account = config.get("account") if isinstance(config, dict) else {}
    if not isinstance(account, dict):
        account = {}
    address = str(account.get("funder") or fallback_address or "").strip()
    if (
        len(address) != 42
        or not address.startswith("0x")
        or any(ch not in "0123456789abcdefABCDEF" for ch in address[2:])
    ):
        return None
    return address


def _fetch_daily_rebate_usd(maker_address: str, date_str: str) -> float:
    """Return official maker rebates for one maker and UTC date."""
    query = urlencode({"date": date_str, "maker_address": maker_address})
    request = Request(
        f"{_REBATES_URL}?{query}",
        headers={
            "Accept": "application/json",
            "User-Agent": "polymarket-maker-income/1.0",
        },
    )
    with urlopen(request, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if isinstance(payload, dict):
        rows = payload.get("data") or payload.get("results") or []
    else:
        rows = payload
    if not isinstance(rows, list):
        return 0.0
    total = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            total += float(row.get("rebated_fees_usdc") or 0.0)
        except (TypeError, ValueError):
            continue
    return total


def _missing_finalized_rebate_dates(
    account_state: dict,
    finalized_day: date,
) -> List[str]:
    """Backfill rebate dates already represented in the LP reward ledger."""
    rebate_daily = account_state.get("rebates_daily")
    if not isinstance(rebate_daily, dict):
        rebate_daily = {}
    reward_daily = account_state.get("daily")
    if not isinstance(reward_daily, dict):
        reward_daily = {}

    final_key = finalized_day.isoformat()
    candidates: List[str] = []
    last_value = account_state.get("rebates_last_snapshot_date")
    if last_value:
        try:
            start = date.fromisoformat(str(last_value)) + timedelta(days=1)
            if start <= finalized_day:
                candidates.extend(_dates_between(start, finalized_day))
        except ValueError:
            pass
    else:
        for value in reward_daily:
            try:
                if date.fromisoformat(str(value)) <= finalized_day:
                    candidates.append(str(value))
            except ValueError:
                continue
    candidates.append(final_key)
    # A first migration may have a long LP history. Bound the initial public
    # endpoint backfill while retaining every already-recorded recent day.
    return sorted(set(candidates))[-90:]


async def refresh_rewards(
    configs: Iterable[Tuple[int, Path]],
    data_dir: Path,
    *,
    now: Optional[datetime] = None,
    build_client: Callable[[Path], tuple] = _build_snapshot_client,
    fetch_daily: Callable[[Any, int, str], float] = _fetch_daily_reward_usd,
    fetch_rebate: Callable[[str, str], float] = _fetch_daily_rebate_usd,
) -> dict:
    """Fetch live and finalized LP rewards plus maker rebates."""
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
    successful_rewards = 0
    successful_rebates = 0

    for account_idx, config_path in configs:
        account_key = str(account_idx)
        prior = prior_live_accounts.get(account_key, {})
        if not isinstance(prior, dict) or previous_live.get("reward_date_utc") != current_key:
            prior = {}
        row = {
            "today_usd": prior.get("today_usd"),
            "previous_day_usd": prior.get("previous_day_usd"),
            "today_rebates_usd": prior.get("today_rebates_usd"),
            "previous_day_rebates_usd": prior.get(
                "previous_day_rebates_usd"
            ),
            "today_total_income_usd": prior.get("today_total_income_usd"),
            "status": "error",
            "reward_status": "error",
            "rebate_status": "error",
            "updated_at": prior.get("updated_at"),
            "error": "income APIs unavailable",
        }

        client, address, signature_type, client_error = await asyncio.to_thread(
            build_client, config_path
        )
        maker_address = _maker_address_for_config(config_path, address)

        account_state = cumulative_accounts.get(account_key)
        if not isinstance(account_state, dict):
            account_state = {
                "address": address,
                "maker_address": maker_address,
                "daily": {},
                "rebates_daily": {},
                "cumulative_usd": 0.0,
                "rebates_cumulative_usd": 0.0,
                "income_cumulative_usd": 0.0,
                "last_snapshot_date": None,
                "rebates_last_snapshot_date": None,
            }
            cumulative_accounts[account_key] = account_state
        if address:
            account_state["address"] = address
        if maker_address:
            account_state["maker_address"] = maker_address
        daily = account_state.get("daily")
        if not isinstance(daily, dict):
            daily = {}
            account_state["daily"] = daily
        rebates_daily = account_state.get("rebates_daily")
        if not isinstance(rebates_daily, dict):
            rebates_daily = {}
            account_state["rebates_daily"] = rebates_daily

        reward_error: Optional[str] = None
        rebate_error: Optional[str] = None
        if client_error:
            reward_error = str(client_error).split(":", 1)[0]
        else:
            try:
                for day_key in _missing_finalized_dates(
                    account_state, finalized_day
                ):
                    amount = await asyncio.to_thread(
                        fetch_daily, client, signature_type, day_key
                    )
                    existing = float(daily.get(day_key) or 0.0)
                    fetched = float(amount)
                    # An empty result is common for a few moments at rollover.
                    # Preserve a known non-zero value in that case, while
                    # allowing positive API corrections in either direction.
                    daily[day_key] = round(
                        existing if fetched == 0.0 and existing > 0.0 else fetched,
                        6,
                    )

                today_amount = await asyncio.to_thread(
                    fetch_daily, client, signature_type, current_key
                )
                row["today_usd"] = round(float(today_amount), 6)
                row["previous_day_usd"] = round(
                    float(daily.get(finalized_key) or 0.0), 6
                )
                row["reward_status"] = "ok"
                successful_rewards += 1
            except Exception as exc:
                reward_error = type(exc).__name__

        if maker_address:
            try:
                rebate_history_errors: List[str] = []
                for day_key in _missing_finalized_rebate_dates(
                    account_state, finalized_day
                ):
                    try:
                        amount = await asyncio.to_thread(
                            fetch_rebate, maker_address, day_key
                        )
                    except Exception as exc:
                        rebate_history_errors.append(
                            f"{day_key}:{type(exc).__name__}"
                        )
                        continue
                    existing = float(rebates_daily.get(day_key) or 0.0)
                    fetched = float(amount)
                    rebates_daily[day_key] = round(
                        existing if fetched == 0.0 and existing > 0.0 else fetched,
                        6,
                    )
                today_rebate = await asyncio.to_thread(
                    fetch_rebate, maker_address, current_key
                )
                row["today_rebates_usd"] = round(float(today_rebate), 6)
                row["previous_day_rebates_usd"] = round(
                    float(rebates_daily.get(finalized_key) or 0.0), 6
                )
                row["rebate_status"] = (
                    "partial" if rebate_history_errors else "ok"
                )
                successful_rebates += 1
                if rebate_history_errors:
                    rebate_error = "history-incomplete"
            except Exception as exc:
                rebate_error = type(exc).__name__
        else:
            rebate_error = "maker-address-unavailable"

        account_state["cumulative_usd"] = round(
            sum(float(value or 0.0) for value in daily.values()), 6
        )
        account_state["rebates_cumulative_usd"] = round(
            sum(float(value or 0.0) for value in rebates_daily.values()), 6
        )
        account_state["income_cumulative_usd"] = round(
            account_state["cumulative_usd"]
            + account_state["rebates_cumulative_usd"],
            6,
        )
        if daily:
            account_state["last_snapshot_date"] = max(daily)
        if rebates_daily:
            account_state["rebates_last_snapshot_date"] = max(rebates_daily)

        today_values = (
            row.get("today_usd"),
            row.get("today_rebates_usd"),
        )
        row["today_total_income_usd"] = (
            round(sum(float(value) for value in today_values), 6)
            if all(value is not None for value in today_values)
            else None
        )
        statuses = (row["reward_status"], row["rebate_status"])
        if statuses == ("ok", "ok"):
            row["status"] = "ok"
        elif "ok" in statuses or "partial" in statuses:
            row["status"] = "partial"
        row["updated_at"] = (
            generated_at
            if "ok" in statuses or "partial" in statuses
            else row["updated_at"]
        )
        errors = [
            f"reward:{reward_error}" if reward_error else "",
            f"rebate:{rebate_error}" if rebate_error else "",
        ]
        row["error"] = ";".join(value for value in errors if value) or None

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
    known_today_rebates = [
        float(row["today_rebates_usd"])
        for row in live_accounts.values()
        if row.get("today_rebates_usd") is not None
    ]
    known_previous_rebates = [
        float(row["previous_day_rebates_usd"])
        for row in live_accounts.values()
        if row.get("previous_day_rebates_usd") is not None
    ]
    known_today_income = [
        float(row["today_total_income_usd"])
        for row in live_accounts.values()
        if row.get("today_total_income_usd") is not None
    ]
    live_state = {
        "version": 2,
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
        "total_today_rebates_usd": (
            round(sum(known_today_rebates), 6)
            if known_today_rebates else None
        ),
        "total_previous_rebates_usd": (
            round(sum(known_previous_rebates), 6)
            if known_previous_rebates else None
        ),
        "total_today_income_usd": (
            round(sum(known_today_income), 6)
            if live_accounts
            and len(known_today_income) == len(live_accounts)
            else None
        ),
        "successful_accounts": successful_rewards,
        "successful_reward_accounts": successful_rewards,
        "successful_rebate_accounts": successful_rebates,
        "configured_accounts": len(live_accounts),
    }

    cumulative["version"] = 3
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
        f"lp_usd={state.get('total_today_usd')} "
        f"rebates_usd={state.get('total_today_rebates_usd')} "
        f"income_usd={state.get('total_today_income_usd')}",
        flush=True,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
