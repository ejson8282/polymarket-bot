"""
Cumulative daily LP-rewards snapshot subsystem.

Every day at 07:50 BJT this task queries /rewards/user/total for each
configured account using the prior BJT-calendar-date (a fully settled 24h
window) and appends the USD total to data/rewards_cumulative.json. On
startup it backfills any missing days between the last recorded snapshot
and yesterday_bjt.

Idempotent per (account, date): re-running on a date already recorded is
a no-op. Safe to restart at any time.
"""

import asyncio
import json
import sys
import time
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Tuple
from zoneinfo import ZoneInfo

_BJT = ZoneInfo("Asia/Shanghai")

try:
    from engine import log
except Exception:
    def log(msg: str) -> None:
        print(msg, flush=True)


def _yesterday_bjt() -> date:
    return (datetime.now(_BJT) - timedelta(days=1)).date()


def _next_0750_bjt_ts() -> float:
    now = datetime.now(_BJT)
    target = now.replace(hour=7, minute=50, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return target.timestamp()


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {"version": 1, "accounts": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"[rewards-snap] state read failed: {e} — starting fresh")
        return {"version": 1, "accounts": {}}


def _save_state(path: Path, state: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


def _dates_between(start: date, end: date) -> List[str]:
    out: List[str] = []
    d = start
    while d <= end:
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _build_snapshot_client(cfg_path: Path):
    """Return (client, address, sig_type, err). Mirrors dashboard._build_client_for_config."""
    try:
        cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    except Exception as e:
        return None, None, 0, f"read-fail: {e}"
    acc = cfg.get("account", {})
    signer_url = str(acc.get("signer_server_url", "")).strip()
    signer_token = str(acc.get("signer_token", "")).strip()
    if not signer_url or not signer_token:
        return None, None, 0, "no-signer"

    # Walk up to repo root so py_clob_client is importable
    repo_dir = cfg_path.parent.parent.parent.parent
    if str(repo_dir) not in sys.path:
        sys.path.insert(0, str(repo_dir))

    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    from remote_signer import AddressStub, BuilderStub, RemoteSignerClient

    host = cfg.get("rest_base_url", "https://clob.polymarket.com").rstrip("/")
    chain_id = int(acc.get("chain_id", 137))
    sig_type = int(acc.get("signature_type", 0))
    funder = acc.get("funder")

    signer = RemoteSignerClient(signer_url, signer_token, funder=funder or None)
    creds = signer.derive_creds()
    address = creds["address"]
    client = ClobClient(host=host, chain_id=chain_id)
    client.signer = AddressStub(address, chain_id)
    client.builder = BuilderStub(sig_type=sig_type, funder=funder)
    client.set_api_creds(ApiCreds(
        api_key=creds["api_key"],
        api_secret=creds["api_secret"],
        api_passphrase=creds["api_passphrase"],
    ))
    return client, address, sig_type, None


def _fetch_daily_reward_usd(client, sig_type: int, date_str: str) -> float:
    """Call /rewards/user/total?date=<date_str> and return summed USD."""
    from py_clob_client.headers.headers import create_level_2_headers
    from py_clob_client.clob_types import RequestArgs
    from py_clob_client.http_helpers.helpers import get as clob_get

    host = client.host
    req = RequestArgs(method="GET", request_path="/rewards/user/total")
    headers = create_level_2_headers(client.signer, client.creds, req)
    url = f"{host}/rewards/user/total?date={date_str}&signature_type={sig_type}"
    resp = clob_get(url, headers=headers)
    totals = resp if isinstance(resp, list) else []
    total_usd = 0.0
    for t in totals:
        try:
            total_usd += float(t.get("earnings") or 0) * float(t.get("asset_rate") or 0)
        except Exception:
            continue
    return total_usd


async def _snapshot_account(
    state: dict,
    acc_idx: int,
    cfg_path: Path,
    target_dates: List[str],
) -> bool:
    """Fetch missing target_dates for one account. Returns True if state mutated."""
    client, address, sig_type, err = await asyncio.to_thread(_build_snapshot_client, cfg_path)
    if err:
        log(f"[rewards-snap] acc={acc_idx}: client build failed — {err}")
        return False

    acc_key = str(acc_idx)
    accounts = state.setdefault("accounts", {})
    acc_state = accounts.setdefault(acc_key, {
        "address": address,
        "daily": {},
        "cumulative_usd": 0.0,
        "last_snapshot_date": None,
    })
    acc_state["address"] = address  # refresh

    changed = False
    for d in target_dates:
        if d in acc_state.get("daily", {}):
            continue
        try:
            usd = await asyncio.to_thread(_fetch_daily_reward_usd, client, sig_type, d)
        except Exception as e:
            log(f"[rewards-snap] acc={acc_idx} date={d} fetch failed: {e}")
            continue
        acc_state.setdefault("daily", {})[d] = round(float(usd), 6)
        log(f"[rewards-snap] acc={acc_idx} {d}: ${usd:.4f}")
        changed = True
        await asyncio.sleep(0.2)

    if changed:
        acc_state["cumulative_usd"] = round(
            sum(float(v) for v in acc_state.get("daily", {}).values()), 6
        )
        if acc_state.get("daily"):
            acc_state["last_snapshot_date"] = max(acc_state["daily"].keys())
    return changed


def _targets_for_account(state: dict, acc_idx: int, yesterday: date) -> List[str]:
    """Compute the list of date strings that still need to be fetched."""
    acc_key = str(acc_idx)
    last_str = state.get("accounts", {}).get(acc_key, {}).get("last_snapshot_date")
    if last_str:
        try:
            last_dt = date.fromisoformat(last_str)
        except Exception:
            last_dt = yesterday - timedelta(days=1)
        start = last_dt + timedelta(days=1)
    else:
        # First ever snapshot for this account: take yesterday only
        start = yesterday
    return _dates_between(start, yesterday)


async def _run_pending_snapshots(
    state_path: Path,
    configs: List[Tuple[int, Path]],
) -> None:
    state = _load_state(state_path)
    yesterday = _yesterday_bjt()
    any_changed = False
    for acc_idx, cfg_path in configs:
        targets = _targets_for_account(state, acc_idx, yesterday)
        if not targets:
            continue
        log(f"[rewards-snap] acc={acc_idx}: fetching {len(targets)} day(s) "
            f"{targets[0]}..{targets[-1]}")
        changed = await _snapshot_account(state, acc_idx, cfg_path, targets)
        any_changed = any_changed or changed
    if any_changed:
        _save_state(state_path, state)


async def rewards_snapshot_loop(
    configs: List[Tuple[int, Path]],
    data_dir: Path,
) -> None:
    """Backfill on startup, then snapshot yesterday every day at 07:50 BJT."""
    state_path = data_dir / "rewards_cumulative.json"
    log(f"[rewards-snap] state file: {state_path}")

    try:
        await _run_pending_snapshots(state_path, configs)
    except Exception as e:
        log(f"[rewards-snap] startup backfill error: {e}\n{traceback.format_exc()}")

    while True:
        try:
            target_ts = _next_0750_bjt_ts()
            sleep_sec = max(1.0, target_ts - time.time())
            log(f"[rewards-snap] next snapshot in {sleep_sec / 3600:.2f}h (07:50 BJT)")
            await asyncio.sleep(sleep_sec)
            await _run_pending_snapshots(state_path, configs)
        except asyncio.CancelledError:
            log("[rewards-snap] cancelled")
            raise
        except Exception as e:
            log(f"[rewards-snap] loop error: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(60.0)
