"""
Multi-account runner for Latitude Alpha / PolyLPS.

Usage:
    python multi_runner.py                    # loads config_1.json, config_2.json, ...
    python multi_runner.py --config-dir /path  # custom maker directory

Supports up to 30 accounts (config_1.json ... config_30.json).

Design:
- Each account runs as an independent coroutine inside the same event loop.
- A single SharedBookFetcher polls all market books once and caches results;
  every account engine reads from this cache instead of issuing its own API calls.
- Accounts have random 3-20s startup stagger so they don't hit the API in sync.
- Each account's book-loop uses a random 3-20s inter-cycle sleep (configurable).
- Per-account engine state is written to data/engine_state_N.json.
"""

import argparse
import asyncio
import json
import os
import random
import re
import signal
import sys
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

# Add maker dir to path for engine import
_MAKER_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MAKER_DIR))

if __package__:  # noqa: E402
    from .engine import PolyLPSMulti, log
    from .account_profiles import (
        LPAccountProfile,
        parse_lp_account_profile,
        validate_shared_allocation,
    )
    from .account_roster import (
        RuntimeAccount,
        load_runtime_roster,
        local_runtime_accounts,
        market_universe_sha256,
        roster_hosts,
        routing_profiles,
        routing_roster_sha256,
    )
    from .release_guard import verify_release
    from .sibling_registry import SiblingOrderRegistry
else:  # direct script execution
    from engine import PolyLPSMulti, log
    from account_profiles import (
        LPAccountProfile,
        parse_lp_account_profile,
        validate_shared_allocation,
    )
    from account_roster import (
        RuntimeAccount,
        load_runtime_roster,
        local_runtime_accounts,
        market_universe_sha256,
        roster_hosts,
        routing_profiles,
        routing_roster_sha256,
    )
    from release_guard import verify_release
    from sibling_registry import SiblingOrderRegistry

# NOTE: py_clob_client_v2.get_order_books posts `data=params` straight to httpx
# without dataclass serialization, so passing BookParams instances raises
# "Object of type BookParams is not JSON serializable". v1 hand-converts to
# `[{"token_id": tid}]`; we mirror that and ship plain dicts. The engine client
# wrapper converts the returned dicts into OrderBookSummary objects.


# ── Shared book cache ──────────────────────────────────────────────────────────

class SharedBookCache:
    """
    In-process order book cache shared across all account engines.

    One SharedBookFetcher coroutine writes; all PolyLPSMulti instances read.
    TTL defaults to 500ms — books older than this are considered stale and
    the engine falls back to fetching directly.
    """

    def __init__(self, ttl_sec: float = 0.5):
        self._data: Dict[str, Tuple[Any, float]] = {}
        self._ttl = ttl_sec

    def put(self, token_id: str, book: Any) -> None:
        self._data[token_id] = (book, time.time())

    def get(self, token_id: str) -> Optional[Any]:
        entry = self._data.get(token_id)
        if entry and (time.time() - entry[1]) < self._ttl:
            return entry[0]
        return None


def _is_rate_limit_error(exc: BaseException) -> bool:
    """Detect Cloudflare 429 / rate-limit responses by best-effort string match."""
    s = f"{type(exc).__name__}: {exc}".lower()
    return "429" in s or "too many requests" in s or "rate limit" in s


async def _shared_book_fetcher(
    primary_engine: PolyLPSMulti,
    get_token_ids,
    cache: SharedBookCache,
    fetch_interval_sec: float = 0.3,
) -> None:
    """Continuously fetch all market books via batch POST /books.

    One batch request replaces N per-token GET /book calls, cutting request
    volume ~55× and staying well clear of Cloudflare's per-IP throttle. On
    429 the fetcher backs off exponentially (1→2→4→8s, capped at 30s).
    If the batch endpoint itself fails, falls back to per-token fetches.

    `get_token_ids` is a zero-arg callable returning the current list of
    token ids — supports runtime market changes (auto-curator add/remove).
    """
    initial_tokens = get_token_ids()
    log(f"[book-fetcher] started for {len(initial_tokens)} token(s) (batch mode)")
    prev_tokens: Optional[List[str]] = None
    params: List[Dict[str, str]] = []
    backoff = 0.0
    consecutive_errors = 0

    while primary_engine._running:
        current_tokens = list(get_token_ids())
        if current_tokens != prev_tokens:
            params = [{"token_id": tid} for tid in current_tokens]
            prev_tokens = current_tokens
            if not params:
                await asyncio.sleep(fetch_interval_sec)
                continue

        if backoff > 0:
            await asyncio.sleep(backoff)

        try:
            payload = [asdict(param) if hasattr(param, "__dataclass_fields__") else dict(param) for param in params]
            books = await asyncio.to_thread(primary_engine.client.get_order_books, payload)
            stored = 0
            for book in books or []:
                if not book or not getattr(book, "bids", None) or not getattr(book, "asks", None):
                    continue
                tid = getattr(book, "asset_id", None)
                if tid:
                    cache.put(tid, book)
                    stored += 1
            if stored == 0 and books:
                log(f"[book-fetcher] batch returned {len(books)} book(s), 0 stored (empty bids/asks)")
            consecutive_errors = 0
            backoff = 0.0
        except Exception as e:
            consecutive_errors += 1
            if _is_rate_limit_error(e):
                backoff = min(30.0, max(1.0, backoff * 2 if backoff else 1.0))
                log(f"[book-fetcher] batch 429 — backoff {backoff:.1f}s (consecutive={consecutive_errors})")
            else:
                log(f"[book-fetcher] batch err={type(e).__name__}: {e} — falling back to per-token")
                for tid in current_tokens:
                    try:
                        book = await asyncio.to_thread(primary_engine.client.get_order_book, tid)
                        if book and getattr(book, "bids", None) and getattr(book, "asks", None):
                            cache.put(tid, book)
                    except Exception as e2:
                        if _is_rate_limit_error(e2):
                            backoff = min(30.0, max(1.0, backoff * 2 if backoff else 1.0))
                            log(f"[book-fetcher] per-token 429 token={tid} — backoff {backoff:.1f}s")
                            break
                        log(f"[book-fetcher] token={tid} err={e2}")
                backoff = max(backoff, 0.0)

        await asyncio.sleep(fetch_interval_sec)


# -- Roster and startup validation ---------------------------------------------

def _legacy_config_files(config_dir: Path) -> List[Tuple[int, Path]]:
    return [
        (index, config_dir / f"config_{index}.json")
        for index in range(1, 31)
        if (config_dir / f"config_{index}.json").is_file()
    ]


def _resolve_host_id(
    accounts: Sequence[RuntimeAccount],
    requested_host_id: str,
) -> str:
    requested = requested_host_id.strip().lower()
    hosts = roster_hosts(accounts)
    if requested:
        if requested not in hosts:
            raise ValueError(
                f"host_id {requested!r} has no enabled accounts; roster hosts={list(hosts)}"
            )
        return requested
    if len(hosts) != 1:
        raise ValueError(
            "--host-id is required when the roster contains multiple hosts: "
            + ", ".join(hosts)
        )
    return hosts[0]


def _roster_config_files(
    config_dir: Path,
    accounts: Sequence[RuntimeAccount],
    host_id: str,
) -> tuple[tuple[RuntimeAccount, Path], ...]:
    local_accounts = local_runtime_accounts(accounts, host_id)
    if not local_accounts:
        raise ValueError(f"roster has no enabled accounts for host {host_id!r}")
    rows = tuple(
        (account, config_dir / f"config_{account.account_index}.json")
        for account in local_accounts
    )
    missing = [path.name for _, path in rows if not path.is_file()]
    if missing:
        raise ValueError(
            f"host {host_id!r} is missing generated configs: {', '.join(missing)}"
        )
    return rows


def _read_config(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read {path.name}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return payload


def _verify_roster_config(
    account: RuntimeAccount,
    path: Path,
    roster_sha256: str,
) -> str:
    payload = _read_config(path)
    actual_funder = str((payload.get("account") or {}).get("funder") or "").strip()
    if actual_funder.casefold() != account.funder.casefold():
        raise ValueError(
            f"{path.name} funder does not match roster account {account.account_index}"
        )

    actual_profile = parse_lp_account_profile(payload, account.account_index)
    if actual_profile != account.profile:
        raise ValueError(
            f"{path.name} lp_account does not match the global roster"
        )

    runtime = payload.get("runtime_account")
    market_sha = market_universe_sha256(payload)
    expected_runtime = {
        "account_index": account.account_index,
        "host_id": account.host_id,
        "clash_port": account.clash_port,
        "routing_roster_sha256": roster_sha256,
        "market_universe_sha256": market_sha,
    }
    if runtime != expected_runtime:
        raise ValueError(
            f"{path.name} runtime_account metadata is stale; regenerate it from the roster"
        )

    proxy_pool = payload.get("proxy_pool")
    proxy_items = proxy_pool.get("items") if isinstance(proxy_pool, dict) else None
    enabled_items = [
        item
        for item in (proxy_items or [])
        if isinstance(item, dict) and item.get("enabled", True)
    ]
    if len(enabled_items) != 1:
        raise ValueError(f"{path.name} must contain exactly one enabled account proxy")
    try:
        proxy_port = urlparse(str(enabled_items[0].get("url") or "")).port
    except ValueError as exc:
        raise ValueError(f"{path.name} contains an invalid account proxy URL") from exc
    if proxy_port != account.clash_port:
        raise ValueError(
            f"{path.name} proxy port does not match roster account {account.account_index}"
        )
    return market_sha


def _require_pause_flags(
    data_dir: Path,
    account_indexes: Sequence[int],
) -> None:
    missing = [
        f".account_{index}.paused"
        for index in account_indexes
        if not (data_dir / f".account_{index}.paused").is_file()
    ]
    if missing:
        raise ValueError(
            "multi-account startup requires every local account paused; missing "
            + ", ".join(missing)
        )


def _verify_expected_digest(name: str, expected: str, actual: str) -> None:
    normalized = expected.strip().lower()
    if not normalized:
        return
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise ValueError(f"expected {name} SHA256 must be 64 hex characters")
    if normalized != actual.lower():
        raise ValueError(
            f"{name} SHA256 mismatch: expected {normalized}, got {actual.lower()}"
        )


# ── Per-account wrapper ────────────────────────────────────────────────────────

async def run_account(
    engine: PolyLPSMulti,
    account_idx: int,
    cache: SharedBookCache,
    startup_delay: float,
    data_dir: Path,
) -> None:
    """Run one account engine with shared book cache and startup stagger."""
    # Write per-account PID file so dashboard can track process liveness
    pid_path = data_dir / f".engine_{account_idx}.pid"
    try:
        pid_path.write_text(str(os.getpid()), encoding="utf-8")
    except Exception:
        pass

    log(f"[multi] account {account_idx}: startup delay {startup_delay:.1f}s")
    await asyncio.sleep(startup_delay)
    engine._shared_book_cache = cache
    log(f"[multi] account {account_idx}: starting engine")
    try:
        await engine.run()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log(f"[multi] account {account_idx}: engine exited with error: {e}")
        raise
    finally:
        try:
            pid_path.unlink(missing_ok=True)
        except Exception:
            pass


async def _cancel_accounts_preserving_exits(
    engines: Sequence[Tuple[int, PolyLPSMulti]],
    *,
    timeout_sec: float = 20.0,
) -> bool:
    """Cancel maker orders on shutdown while leaving live exit SELLs alone."""

    async def _cancel_one(account_idx: int, engine: PolyLPSMulti) -> bool:
        try:
            verified = await asyncio.wait_for(
                engine._cancel_all_except_exit(),
                timeout=timeout_sec,
            )
        except Exception as exc:
            log(f"[multi] account {account_idx}: shutdown cancel failed: {exc}")
            return False
        if not verified:
            log(f"[multi] account {account_idx}: shutdown cancel was not verified")
            return False
        log(f"[multi] account {account_idx}: maker orders cancelled; exit SELLs preserved")
        return True

    results = await asyncio.gather(
        *[_cancel_one(index, engine) for index, engine in engines]
    )
    return all(results)


# ── Main entry point ───────────────────────────────────────────────────────────

async def multi_run(
    config_dir: Path,
    *,
    roster_path: Optional[Path] = None,
    host_id: str = "",
    data_dir: Optional[Path] = None,
    require_paused: bool = False,
    validate_only: bool = False,
    expected_roster_sha256: str = "",
    expected_market_sha256: str = "",
) -> None:
    """Run every local account as one fail-closed process.

    Roster mode is the production multi-host path. Legacy discovery remains for
    development compatibility, but it has no cross-host routing guarantees.
    """
    config_dir = config_dir.resolve()
    resolved_data_dir = (
        data_dir.resolve()
        if data_dir is not None
        else config_dir.parent.parent.parent / "data"
    )
    resolved_data_dir.mkdir(parents=True, exist_ok=True)

    accounts: tuple[RuntimeAccount, ...] = ()
    route_sha = ""
    resolved_host_id = host_id.strip().lower()
    config_files: list[tuple[int, Path]] = []
    global_profiles: Dict[int, LPAccountProfile] = {}
    local_market_sha = ""

    if roster_path is not None:
        accounts = load_runtime_roster(roster_path.resolve())
        resolved_host_id = _resolve_host_id(accounts, resolved_host_id)
        if len(roster_hosts(accounts)) > 1 and (
            not expected_roster_sha256.strip()
            or not expected_market_sha256.strip()
        ):
            raise ValueError(
                "multi-host roster mode requires --expected-roster-sha256 and "
                "--expected-market-sha256"
            )
        route_sha = routing_roster_sha256(accounts)
        roster_rows = _roster_config_files(config_dir, accounts, resolved_host_id)
        if require_paused:
            _require_pause_flags(
                resolved_data_dir,
                [account.account_index for account, _ in roster_rows],
            )
        market_shas: set[str] = set()
        for account, path in roster_rows:
            market_shas.add(_verify_roster_config(account, path, route_sha))
            config_files.append((account.account_index, path))
            log(f"[multi] host={resolved_host_id} selected {path.name}")
        if len(market_shas) != 1:
            raise ValueError(
                f"host {resolved_host_id!r} account configs have different market universes"
            )
        local_market_sha = next(iter(market_shas))
        global_profiles = routing_profiles(accounts)
        _verify_expected_digest("roster", expected_roster_sha256, route_sha)
        _verify_expected_digest(
            "market universe",
            expected_market_sha256,
            local_market_sha,
        )
    else:
        if expected_roster_sha256 or expected_market_sha256:
            raise ValueError("expected routing digests require --roster mode")
        config_files = _legacy_config_files(config_dir)
        if require_paused:
            _require_pause_flags(
                resolved_data_dir,
                [index for index, _ in config_files],
            )

    if not config_files:
        raise ValueError(f"no local config_N.json files found in {config_dir}")

    log(
        f"[multi] initializing {len(config_files)} local account(s)"
        + (
            f" on {resolved_host_id}; global_accounts={len(global_profiles)} "
            f"roster_sha={route_sha}"
            if roster_path is not None
            else " in legacy local-discovery mode"
        )
    )

    engines: List[Tuple[int, PolyLPSMulti]] = []
    local_profiles: Dict[int, LPAccountProfile] = {}
    try:
        for idx, cfg_path in config_files:
            eng = PolyLPSMulti(config_path=str(cfg_path))
            if eng._account_idx != idx:
                raise ValueError(
                    f"{cfg_path.name} initialized as account {eng._account_idx}, expected {idx}"
                )
            engines.append((idx, eng))
            local_profiles[idx] = eng.lp_account_profile
            actual_data_dir = eng._state_path.parent.resolve()
            if actual_data_dir != resolved_data_dir:
                raise ValueError(
                    f"{cfg_path.name} writes runtime state to {actual_data_dir}, "
                    f"but --data-dir is {resolved_data_dir}"
                )
            log(f"[multi] account {idx}: initialized ({len(eng.market_cfg)} markets)")
    except Exception as exc:
        log(f"[multi] initialization failed; no account will start: {exc}")
        raise

    if roster_path is None:
        global_profiles = dict(local_profiles)

    markets_by_account = {
        idx: {**eng.market_cfg, **eng._night_market_cfg}
        for idx, eng in engines
    }
    validate_shared_allocation(local_profiles, markets_by_account)

    sibling_registry = SiblingOrderRegistry()
    for _, eng in engines:
        eng._sibling_registry = sibling_registry
        eng._shared_account_profiles = global_profiles
        eng._runtime_mode = "multi_roster" if roster_path is not None else "multi_legacy"
        eng._runtime_host_id = resolved_host_id
        eng._routing_roster_sha256 = route_sha
        eng._routing_market_universe_sha256 = local_market_sha
        eng._routing_account_count = len(global_profiles)
        eng._local_account_count = len(engines)
        eng._runtime_market_updates_enabled = roster_path is None
        eng._event_bus.set_state_namespace(f"account:{eng._account_idx}")
    log(f"[multi] sibling order registry shared across {len(engines)} local account(s)")

    managed_profiles = [profile for profile in global_profiles.values() if profile.managed]
    if managed_profiles:
        summary = ", ".join(
            f"{profile.account_id}=${profile.target_principal_usdc}"
            for profile in sorted(managed_profiles, key=lambda item: item.account_index)
        )
        log(f"[multi] global LP routing profiles: {summary}")

    if validate_only:
        log(
            f"[multi] validation complete: host={resolved_host_id or 'legacy'} "
            f"local_accounts={len(engines)} roster_sha={route_sha or '-'} "
            f"market_sha={local_market_sha or '-'}; no workers started"
        )
        return

    all_token_ids = {
        tid
        for _, eng in engines
        for tid in {**eng.market_cfg, **eng._night_market_cfg}
    }
    log(
        f"[multi] shared book cache: {len(all_token_ids)} unique market(s) "
        f"across {len(engines)} local account(s)"
    )

    cache = SharedBookCache(ttl_sec=0.5)
    primary_engine = engines[0][1]

    def _all_tokens_fn() -> List[str]:
        return list(
            {
                token_id
                for _, engine in engines
                for token_id in {**engine.market_cfg, **engine._night_market_cfg}
            }
        )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        log("[multi] SIGTERM/SIGINT received; stopping all local accounts")
        stop_event.set()

    installed_signals: list[signal.Signals] = []
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    worker_tasks: list[asyncio.Task] = [
        asyncio.create_task(
            _shared_book_fetcher(primary_engine, _all_tokens_fn, cache),
            name="shared_book_fetcher",
        )
    ]
    cumulative_delay = 0.0
    for idx, eng in engines:
        worker_tasks.append(
            asyncio.create_task(
                run_account(
                    eng,
                    idx,
                    cache,
                    startup_delay=cumulative_delay,
                    data_dir=resolved_data_dir,
                ),
                name=f"account_{idx}",
            )
        )
        cumulative_delay += random.uniform(3.0, 20.0)
    stop_task = asyncio.create_task(stop_event.wait(), name="shutdown_signal")

    failure: Optional[BaseException] = None
    shutdown_verified = False
    try:
        done, _ = await asyncio.wait(
            [*worker_tasks, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task not in done:
            finished = next(task for task in done if task is not stop_task)
            try:
                finished.result()
            except asyncio.CancelledError as exc:
                failure = RuntimeError(f"worker {finished.get_name()} was cancelled")
                failure.__cause__ = exc
            except BaseException as exc:
                failure = RuntimeError(f"worker {finished.get_name()} failed: {exc}")
                failure.__cause__ = exc
            else:
                failure = RuntimeError(f"worker {finished.get_name()} exited unexpectedly")
    finally:
        for _, eng in engines:
            eng._running = False
        for task in worker_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        shutdown_verified = await _cancel_accounts_preserving_exits(engines)
        if not stop_task.done():
            stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        for sig in installed_signals:
            loop.remove_signal_handler(sig)
        log("[multi] all local account tasks stopped")

    if not shutdown_verified:
        raise RuntimeError("multi-account shutdown could not verify maker-order cancellation")
    if failure is not None:
        raise failure


def main() -> None:
    verify_release(Path(__file__))
    parser = argparse.ArgumentParser(description="PolyLPS multi-account runner")
    parser.add_argument(
        "--config-dir",
        default=str(_MAKER_DIR),
        help="Directory containing config_1.json, config_2.json, ... (default: same directory as this script)",
    )
    parser.add_argument(
        "--roster",
        default="",
        help="Global non-secret account roster JSON (enables host-aware routing)",
    )
    parser.add_argument(
        "--host-id",
        default=os.getenv("POLYMARKET_HOST_ID", ""),
        help="This runtime host id; required when the roster contains multiple hosts",
    )
    parser.add_argument(
        "--data-dir",
        default="",
        help="Runtime data directory (default: repo data directory derived from config-dir)",
    )
    parser.add_argument(
        "--require-paused",
        action="store_true",
        help="Fail before signer initialization unless every local account pause flag exists",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Initialize and validate every local account, then exit before starting workers",
    )
    parser.add_argument(
        "--expected-roster-sha256",
        default=os.getenv("POLYMARKET_EXPECTED_ROSTER_SHA256", ""),
        help="Fail closed unless the global roster matches this reviewed SHA256",
    )
    parser.add_argument(
        "--expected-market-sha256",
        default=os.getenv("POLYMARKET_EXPECTED_MARKET_SHA256", ""),
        help="Fail closed unless the market universe matches this reviewed SHA256",
    )
    args = parser.parse_args()
    config_dir = Path(args.config_dir).resolve()
    log(f"[multi] config dir: {config_dir}")
    asyncio.run(
        multi_run(
            config_dir,
            roster_path=Path(args.roster).resolve() if args.roster else None,
            host_id=args.host_id,
            data_dir=Path(args.data_dir).resolve() if args.data_dir else None,
            require_paused=args.require_paused,
            validate_only=args.validate_only,
            expected_roster_sha256=args.expected_roster_sha256,
            expected_market_sha256=args.expected_market_sha256,
        )
    )


if __name__ == "__main__":
    main()
