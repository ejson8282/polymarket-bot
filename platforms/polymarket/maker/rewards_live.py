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
    from .pnl_ledger import fetch_realized_pnl
    from .reward_ledger import (
        canonical_account_uid,
        load_reward_ledger,
        mark_reward_scope_stale,
        register_account_alias,
        replace_reward_scope,
        reward_ledger_summary,
        save_reward_ledger,
    )
    from .reward_observer import refresh_observer_state
    from .rewards_snapshot import (
        _build_snapshot_client,
        _dates_between,
        _fetch_daily_reward_usd,
        _load_state,
        _save_state,
    )
except ImportError:
    from pnl_ledger import fetch_realized_pnl  # type: ignore
    from reward_ledger import (  # type: ignore
        canonical_account_uid,
        load_reward_ledger,
        mark_reward_scope_stale,
        register_account_alias,
        replace_reward_scope,
        reward_ledger_summary,
        save_reward_ledger,
    )
    from reward_observer import refresh_observer_state  # type: ignore
    from rewards_snapshot import (  # type: ignore
        _build_snapshot_client,
        _dates_between,
        _fetch_daily_reward_usd,
        _load_state,
        _save_state,
    )


_BJT = ZoneInfo("Asia/Shanghai")
_REBATES_URL = "https://clob.polymarket.com/rebates/current"
_REWARD_EARNINGS_PATH = "/rewards/user"
_USDC_LEDGER_ASSET = "usdc"


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


def _account_identity_for_config(
    config_path: Path,
    maker_address: str,
) -> Tuple[int, int, str]:
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        config = {}
    account = config.get("account") if isinstance(config, dict) else {}
    if not isinstance(account, dict):
        account = {}
    chain_id = int(account.get("chain_id", 137))
    signature_type = int(account.get("signature_type", 0))
    return (
        chain_id,
        signature_type,
        canonical_account_uid(chain_id, signature_type, maker_address),
    )


def _fetch_daily_rebate_rows(maker_address: str, date_str: str) -> List[dict]:
    """Return official per-market maker rebate rows."""
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
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _fetch_daily_rebate_usd(maker_address: str, date_str: str) -> float:
    """Return official maker rebates for one maker and UTC date."""
    total = 0.0
    for row in _fetch_daily_rebate_rows(maker_address, date_str):
        try:
            total += float(row.get("rebated_fees_usdc") or 0.0)
        except (TypeError, ValueError):
            continue
    return total


def _fetch_market_earnings(
    client: Any,
    signature_type: int,
    date_str: str,
    maker_address: str,
    sponsored: bool,
) -> List[dict]:
    """Return all official per-market LP earnings for one account/day/type."""
    from py_clob_client_v2.headers.headers import create_level_2_headers
    from py_clob_client_v2.clob_types import RequestArgs
    from py_clob_client_v2.http_helpers.helpers import get as clob_get

    rows: List[dict] = []
    next_cursor: Optional[str] = None
    seen_cursors: set[str] = set()
    for _page in range(100):
        params = {
            "date": date_str,
            "signature_type": int(signature_type),
            "maker_address": maker_address,
            "sponsored": "true" if sponsored else "false",
        }
        if next_cursor:
            params["next_cursor"] = next_cursor
        request = RequestArgs(method="GET", request_path=_REWARD_EARNINGS_PATH)
        headers = create_level_2_headers(client.signer, client.creds, request)
        response = clob_get(
            f"{client.host}{_REWARD_EARNINGS_PATH}?{urlencode(params)}",
            headers=headers,
        )
        if isinstance(response, dict):
            page_rows = response.get("data") or []
            cursor = str(response.get("next_cursor") or "")
        elif isinstance(response, list):
            page_rows = response
            cursor = ""
        else:
            raise TypeError("invalid per-market rewards response")
        if not isinstance(page_rows, list):
            raise TypeError("invalid per-market rewards rows")
        rows.extend(dict(row) for row in page_rows if isinstance(row, dict))
        if not cursor or cursor == "LTE=":
            break
        if cursor in seen_cursors:
            raise RuntimeError("rewards pagination cursor repeated")
        seen_cursors.add(cursor)
        next_cursor = cursor
    else:
        raise RuntimeError("rewards pagination exceeded safety limit")
    return rows


def _lp_ledger_rows(rows: Iterable[dict], maker_address: str) -> List[dict]:
    out: List[dict] = []
    for row in rows:
        condition_id = str(row.get("condition_id") or "").strip().lower()
        asset_address = str(row.get("asset_address") or "").strip().lower()
        if not condition_id or not asset_address:
            continue
        try:
            earnings = float(row.get("earnings") or 0.0)
            asset_rate = float(row.get("asset_rate") or 0.0)
        except (TypeError, ValueError):
            continue
        out.append(
            {
                "condition_id": condition_id,
                "asset_address": asset_address,
                "maker_address": maker_address.lower(),
                "amount": round(earnings, 12),
                "asset_rate": round(asset_rate, 12),
                "usd_amount": round(earnings * asset_rate, 6),
                "source": "official_rewards_user",
            }
        )
    return out


def _rebate_ledger_rows(rows: Iterable[dict], maker_address: str) -> List[dict]:
    out: List[dict] = []
    for row in rows:
        condition_id = str(row.get("condition_id") or "").strip().lower()
        asset_address = str(
            row.get("asset_address") or _USDC_LEDGER_ASSET
        ).strip().lower()
        if not condition_id:
            continue
        try:
            amount = float(row.get("rebated_fees_usdc") or 0.0)
        except (TypeError, ValueError):
            continue
        out.append(
            {
                "condition_id": condition_id,
                "asset_address": asset_address,
                "maker_address": maker_address.lower(),
                "amount": round(amount, 6),
                "asset_rate": 1.0,
                "usd_amount": round(amount, 6),
                "source": "official_rebates_current",
            }
        )
    return out


def _pnl_ledger_rows(pnl: dict) -> Dict[str, List[dict]]:
    grouped: Dict[Tuple[str, str, str], float] = {}
    exits = pnl.get("realized_exits") if isinstance(pnl, dict) else []
    if not isinstance(exits, list):
        return {}
    for row in exits:
        if not isinstance(row, dict) or row.get("complete") is not True:
            continue
        condition_id = str(row.get("market") or "").strip().lower()
        asset_id = str(row.get("asset_id") or "").strip().lower()
        try:
            epoch = int(row.get("epoch"))
            amount = float(row.get("net_pnl_usd"))
        except (TypeError, ValueError):
            continue
        if not condition_id or not asset_id:
            continue
        day = datetime.fromtimestamp(epoch, timezone.utc).date().isoformat()
        key = (day, condition_id, asset_id)
        grouped[key] = grouped.get(key, 0.0) + amount
    out: Dict[str, List[dict]] = {}
    for (day, condition_id, asset_id), amount in grouped.items():
        out.setdefault(day, []).append(
            {
                "condition_id": condition_id,
                "asset_address": asset_id,
                "amount": round(amount, 6),
                "asset_rate": 1.0,
                "usd_amount": round(amount, 6),
                "source": "confirmed_trades_fifo_v2_market_fees",
            }
        )
    return out


def _fetch_reward_percentages(client: Any, signature_type: int) -> Dict[str, float]:
    """Return the account's live reward share by condition id."""
    from py_clob_client_v2.headers.headers import create_level_2_headers
    from py_clob_client_v2.clob_types import RequestArgs
    from py_clob_client_v2.http_helpers.helpers import get as clob_get

    path = "/rewards/user/percentages"
    request = RequestArgs(method="GET", request_path=path)
    headers = create_level_2_headers(client.signer, client.creds, request)
    response = clob_get(
        f"{client.host}{path}?signature_type={int(signature_type)}",
        headers=headers,
    )
    if not isinstance(response, dict):
        return {}
    out: Dict[str, float] = {}
    for condition_id, percentage in response.items():
        try:
            value = float(percentage)
        except (TypeError, ValueError):
            continue
        if value < 0:
            continue
        out[str(condition_id).strip().lower()] = round(value, 6)
    return out


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
    fetch_percentages: Callable[
        [Any, int], Dict[str, float]
    ] = _fetch_reward_percentages,
    fetch_pnl: Callable[..., dict] = fetch_realized_pnl,
    fetch_market_earnings: Optional[Callable[..., List[dict]]] = None,
    fetch_rebate_rows: Optional[Callable[[str, str], List[dict]]] = None,
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
    ledger_path = data_dir / "reward_ledger.json"
    cumulative = _load_state(cumulative_path)
    ledger = load_reward_ledger(ledger_path)
    ledger["updated_at"] = generated_at
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
    successful_percentages = 0
    successful_pnl = 0
    canonical_accounts: Dict[str, str] = {}
    refreshed_ledger_scopes: set[str] = set()
    detailed_fetch_enabled = fetch_market_earnings is not None or (
        fetch_daily is _fetch_daily_reward_usd
    )
    market_earnings_fetcher = (
        fetch_market_earnings or _fetch_market_earnings
    )
    rebate_rows_enabled = fetch_rebate_rows is not None or (
        fetch_rebate is _fetch_daily_rebate_usd
    )
    rebate_rows_fetcher = fetch_rebate_rows or _fetch_daily_rebate_rows

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
            "percentage_status": "error",
            "pnl_status": "error",
            "pnl": prior.get("pnl") if isinstance(prior.get("pnl"), dict) else None,
            "reward_percentages": prior.get("reward_percentages") or {},
            "updated_at": prior.get("updated_at"),
            "error": "income APIs unavailable",
        }

        client, address, signature_type, client_error = await asyncio.to_thread(
            build_client, config_path
        )
        maker_address = _maker_address_for_config(config_path, address)
        account_uid: Optional[str] = None
        if maker_address:
            try:
                _chain_id, _config_signature_type, account_uid = (
                    _account_identity_for_config(config_path, maker_address)
                )
            except (TypeError, ValueError):
                account_uid = None
        row["account_uid"] = account_uid
        row["canonical_account_index"] = account_key
        row["duplicate_of_account"] = None
        if account_uid:
            register_account_alias(
                ledger,
                account_uid=account_uid,
                account_index=account_idx,
            )
            canonical_index = canonical_accounts.setdefault(
                account_uid, account_key
            )
            row["canonical_account_index"] = canonical_index
            if canonical_index != account_key:
                row["duplicate_of_account"] = canonical_index

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
        percentage_error: Optional[str] = None
        pnl_error: Optional[str] = None
        ledger_errors: List[str] = []
        if client_error:
            reward_error = str(client_error).split(":", 1)[0]
            percentage_error = reward_error
            pnl_error = reward_error
            if detailed_fetch_enabled and account_uid:
                for reward_type in ("native_lp", "sponsored_lp"):
                    scope_id = f"{current_key}|{account_uid}|{reward_type}"
                    if scope_id not in refreshed_ledger_scopes:
                        ledger_errors.append(
                            f"{reward_type}:{current_key}:{reward_error}"
                        )
                        mark_reward_scope_stale(
                            ledger,
                            business_day=current_key,
                            account_uid=account_uid,
                            reward_type=reward_type,
                            observed_at=generated_at,
                            error=reward_error,
                        )
        else:
            try:
                finalized_reward_dates = _missing_finalized_dates(
                    account_state, finalized_day
                )
                for day_key in finalized_reward_dates:
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
                finalized_reward_dates = [finalized_key]

            if (
                detailed_fetch_enabled
                and account_uid
                and maker_address
            ):
                ledger_days = list(dict.fromkeys(
                    [*finalized_reward_dates, current_key]
                ))
                for day_key in ledger_days:
                    for reward_type, sponsored in (
                        ("native_lp", False),
                        ("sponsored_lp", True),
                    ):
                        scope_id = f"{day_key}|{account_uid}|{reward_type}"
                        if scope_id in refreshed_ledger_scopes:
                            continue
                        try:
                            earnings_rows = await asyncio.to_thread(
                                market_earnings_fetcher,
                                client,
                                signature_type,
                                day_key,
                                maker_address,
                                sponsored,
                            )
                            replace_reward_scope(
                                ledger,
                                business_day=day_key,
                                account_uid=account_uid,
                                reward_type=reward_type,
                                records=_lp_ledger_rows(
                                    earnings_rows, maker_address
                                ),
                                observed_at=generated_at,
                                finalized=day_key != current_key,
                            )
                            refreshed_ledger_scopes.add(scope_id)
                        except Exception as exc:
                            error = type(exc).__name__
                            ledger_errors.append(
                                f"{reward_type}:{day_key}:{error}"
                            )
                            mark_reward_scope_stale(
                                ledger,
                                business_day=day_key,
                                account_uid=account_uid,
                                reward_type=reward_type,
                                observed_at=generated_at,
                                error=error,
                            )
            try:
                percentages = await asyncio.to_thread(
                    fetch_percentages, client, signature_type
                )
                row["reward_percentages"] = {
                    str(condition_id).strip().lower(): round(
                        float(percentage), 6
                    )
                    for condition_id, percentage in percentages.items()
                }
                row["percentage_status"] = "ok"
                successful_percentages += 1
            except Exception as exc:
                percentage_error = type(exc).__name__
            try:
                pnl = await asyncio.to_thread(
                    fetch_pnl,
                    client,
                    [value for value in (address, maker_address) if value],
                    now=now_utc,
                )
                if not isinstance(pnl, dict):
                    raise TypeError("invalid-pnl-payload")
                row["pnl"] = pnl
                row["pnl_status"] = str(pnl.get("status") or "error")
                if row["pnl_status"] in {"ok", "partial", "empty"}:
                    successful_pnl += 1
            except Exception as exc:
                pnl_error = type(exc).__name__

        if maker_address:
            try:
                rebate_history_errors: List[str] = []
                finalized_rebate_dates = _missing_finalized_rebate_dates(
                    account_state, finalized_day
                )
                for day_key in finalized_rebate_dates:
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
                finalized_rebate_dates = [finalized_key]

            if (
                rebate_rows_enabled
                and account_uid
            ):
                rebate_days = list(dict.fromkeys(
                    [*finalized_rebate_dates, current_key]
                ))
                for day_key in rebate_days:
                    scope_id = f"{day_key}|{account_uid}|maker_rebate"
                    if scope_id in refreshed_ledger_scopes:
                        continue
                    try:
                        detailed_rebates = await asyncio.to_thread(
                            rebate_rows_fetcher, maker_address, day_key
                        )
                        replace_reward_scope(
                            ledger,
                            business_day=day_key,
                            account_uid=account_uid,
                            reward_type="maker_rebate",
                            records=_rebate_ledger_rows(
                                detailed_rebates, maker_address
                            ),
                            observed_at=generated_at,
                            finalized=day_key != current_key,
                        )
                        refreshed_ledger_scopes.add(scope_id)
                    except Exception as exc:
                        error = type(exc).__name__
                        ledger_errors.append(
                            f"maker_rebate:{day_key}:{error}"
                        )
                        mark_reward_scope_stale(
                            ledger,
                            business_day=day_key,
                            account_uid=account_uid,
                            reward_type="maker_rebate",
                            observed_at=generated_at,
                            error=error,
                        )
        else:
            rebate_error = "maker-address-unavailable"

        if (
            account_uid
            and isinstance(row.get("pnl"), dict)
            and row.get("pnl_status") in {"ok", "partial", "empty"}
        ):
            pnl_by_day = _pnl_ledger_rows(row["pnl"])
            for day_key in sorted(set(pnl_by_day) | {current_key}):
                scope_id = f"{day_key}|{account_uid}|trading_pnl"
                if scope_id in refreshed_ledger_scopes:
                    continue
                replace_reward_scope(
                    ledger,
                    business_day=day_key,
                    account_uid=account_uid,
                    reward_type="trading_pnl",
                    records=pnl_by_day.get(day_key, []),
                    observed_at=generated_at,
                    finalized=day_key != current_key,
                )
                refreshed_ledger_scopes.add(scope_id)
        elif account_uid:
            error = pnl_error or str(row.get("pnl_status") or "unavailable")
            scope_id = f"{current_key}|{account_uid}|trading_pnl"
            if scope_id not in refreshed_ledger_scopes:
                ledger_errors.append(f"trading_pnl:{current_key}:{error}")
                mark_reward_scope_stale(
                    ledger,
                    business_day=current_key,
                    account_uid=account_uid,
                    reward_type="trading_pnl",
                    observed_at=generated_at,
                    error=error,
                )

        row["reward_ledger_status"] = (
            "stale" if ledger_errors else "current"
        )
        row["reward_ledger_errors"] = ledger_errors

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
            (
                f"percentages:{percentage_error}"
                if percentage_error else ""
            ),
            f"pnl:{pnl_error}" if pnl_error else "",
        ]
        row["error"] = ";".join(value for value in errors if value) or None

        live_accounts[account_key] = row

    start_bjt, end_bjt = reward_window_bjt(now_utc)
    representative_by_uid: Dict[str, Tuple[str, dict]] = {}
    for account_key, row in live_accounts.items():
        uid = str(row.get("account_uid") or f"config:{account_key}")
        existing = representative_by_uid.get(uid)
        rank = (
            1 if row.get("status") == "ok" else 0,
            1 if row.get("reward_status") == "ok" else 0,
            1 if row.get("rebate_status") in {"ok", "partial"} else 0,
            str(row.get("updated_at") or ""),
        )
        existing_rank = (
            (
                1 if existing[1].get("status") == "ok" else 0,
                1 if existing[1].get("reward_status") == "ok" else 0,
                1
                if existing[1].get("rebate_status") in {"ok", "partial"}
                else 0,
                str(existing[1].get("updated_at") or ""),
            )
            if existing
            else None
        )
        if existing is None or rank > existing_rank:
            representative_by_uid[uid] = (account_key, row)

    for uid, (canonical_key, _canonical_row) in representative_by_uid.items():
        for account_key, row in live_accounts.items():
            row_uid = str(row.get("account_uid") or f"config:{account_key}")
            if row_uid != uid:
                continue
            row["canonical_account_index"] = canonical_key
            row["duplicate_of_account"] = (
                None if account_key == canonical_key else canonical_key
            )
    canonical_live_accounts = [
        row for _key, row in representative_by_uid.values()
    ]
    known_today = [
        float(row["today_usd"])
        for row in canonical_live_accounts
        if row.get("today_usd") is not None
    ]
    known_previous = [
        float(row["previous_day_usd"])
        for row in canonical_live_accounts
        if row.get("previous_day_usd") is not None
    ]
    known_today_rebates = [
        float(row["today_rebates_usd"])
        for row in canonical_live_accounts
        if row.get("today_rebates_usd") is not None
    ]
    known_previous_rebates = [
        float(row["previous_day_rebates_usd"])
        for row in canonical_live_accounts
        if row.get("previous_day_rebates_usd") is not None
    ]
    known_today_income = [
        float(row["today_total_income_usd"])
        for row in canonical_live_accounts
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
            if canonical_live_accounts
            and len(known_today_income) == len(canonical_live_accounts)
            else None
        ),
        "successful_accounts": successful_rewards,
        "successful_reward_accounts": successful_rewards,
        "successful_rebate_accounts": successful_rebates,
        "successful_percentage_accounts": successful_percentages,
        "successful_pnl_accounts": successful_pnl,
        "configured_accounts": len(live_accounts),
        "canonical_accounts": len(canonical_live_accounts),
        "duplicate_account_aliases": (
            len(live_accounts) - len(canonical_live_accounts)
        ),
    }

    ledger["updated_at"] = generated_at
    save_reward_ledger(ledger_path, ledger)
    live_state["reward_ledger"] = {
        "version": ledger.get("version"),
        "path": ledger_path.name,
        "summary": reward_ledger_summary(ledger, current_key),
        "account_aliases": ledger.get("account_aliases", {}),
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
    observer_state: Dict[str, Any] = {}
    observer_error: Optional[str] = None
    try:
        observer_state = refresh_observer_state(
            args.data_dir,
            config_dir=args.config_dir,
        )
    except Exception as exc:
        observer_error = type(exc).__name__
    ok = int(state.get("successful_accounts") or 0)
    total = int(state.get("configured_accounts") or 0)
    print(
        "[rewards-live] "
        f"date={state.get('reward_date_utc')} accounts={ok}/{total} "
        f"lp_usd={state.get('total_today_usd')} "
        f"rebates_usd={state.get('total_today_rebates_usd')} "
        f"income_usd={state.get('total_today_income_usd')} "
        f"observer_ready={observer_state.get('candidates_ready')} "
        f"observer_error={observer_error}",
        flush=True,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
