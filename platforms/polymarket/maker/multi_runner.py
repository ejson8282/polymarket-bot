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
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
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
        load_runtime_roster_scope,
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
        load_runtime_roster_scope,
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

@dataclass(frozen=True)
class CachedBookSnapshot:
    book: Any
    fetched_at: float


class SharedBookCache:
    """
    In-process order book cache shared across all account engines.

    One SharedBookFetcher coroutine writes; all PolyLPSMulti instances read.
    Reads can pin one immutable snapshot for a complete quote cycle. This
    prevents a large market universe from expiring halfway through the cycle
    and amplifying one batch failure into dozens of direct REST requests.
    """

    def __init__(
        self,
        ttl_sec: float = 2.0,
        *,
        direct_rest_burst: int = 4,
        direct_rest_window_sec: float = 1.0,
    ):
        self._data: Dict[str, Tuple[Any, float]] = {}
        self._ttl = max(0.1, float(ttl_sec))
        self._generation = 0
        self._direct_rest_burst = max(1, int(direct_rest_burst))
        self._direct_rest_window_sec = max(0.1, float(direct_rest_window_sec))
        self._direct_rest_events: deque[float] = deque()
        self._batch_latencies_ms: deque[float] = deque(maxlen=120)
        self._counters: Dict[str, int] = {
            "full_batch_requests": 0,
            "full_batch_successes": 0,
            "full_batch_failures": 0,
            "chunk_batch_requests": 0,
            "chunk_batch_successes": 0,
            "chunk_batch_failures": 0,
            "books_stored": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "ws_depth_fallbacks": 0,
            "direct_rest_fallbacks": 0,
            "direct_rest_suppressed": 0,
        }
        self._last_success_ts = 0.0
        self._last_failure_ts = 0.0
        self._last_error_type = ""
        self._backoff_sec = 0.0

    def put(self, token_id: str, book: Any) -> None:
        self._data[token_id] = (book, time.time())

    def put_many(self, books: Iterable[Any]) -> int:
        now = time.time()
        staged: Dict[str, Tuple[Any, float]] = {}
        for book in books or []:
            if not book or not getattr(book, "bids", None) or not getattr(book, "asks", None):
                continue
            token_id = str(getattr(book, "asset_id", None) or "").strip()
            if token_id:
                staged[token_id] = (book, now)
        if staged:
            self._data.update(staged)
            self._generation += 1
            self._counters["books_stored"] += len(staged)
            self._last_success_ts = now
        return len(staged)

    def get(self, token_id: str) -> Optional[Any]:
        entry = self._data.get(token_id)
        if entry and (time.time() - entry[1]) < self._ttl:
            self._counters["cache_hits"] += 1
            return entry[0]
        self._counters["cache_misses"] += 1
        return None

    def snapshot(self, token_ids: Iterable[str]) -> Dict[str, CachedBookSnapshot]:
        """Return fresh books using one timestamp for the whole quote cycle."""
        now = time.time()
        result: Dict[str, CachedBookSnapshot] = {}
        requested = [str(token_id) for token_id in token_ids]
        for token_id in requested:
            entry = self._data.get(token_id)
            if entry and (now - entry[1]) < self._ttl:
                result[token_id] = CachedBookSnapshot(
                    book=entry[0],
                    fetched_at=entry[1],
                )
        self._counters["cache_hits"] += len(result)
        self._counters["cache_misses"] += len(requested) - len(result)
        return result

    def allow_direct_rest(self) -> bool:
        """Bound cache-miss REST fan-out across all local account engines."""
        now = time.time()
        cutoff = now - self._direct_rest_window_sec
        while self._direct_rest_events and self._direct_rest_events[0] <= cutoff:
            self._direct_rest_events.popleft()
        if len(self._direct_rest_events) >= self._direct_rest_burst:
            self._counters["direct_rest_suppressed"] += 1
            return False
        self._direct_rest_events.append(now)
        self._counters["direct_rest_fallbacks"] += 1
        return True

    def note_ws_depth_fallback(self) -> None:
        self._counters["ws_depth_fallbacks"] += 1

    def record_batch_result(
        self,
        *,
        kind: str,
        success: bool,
        latency_ms: float,
        error: Optional[BaseException] = None,
    ) -> None:
        prefix = "chunk_batch" if kind == "chunk" else "full_batch"
        self._counters[f"{prefix}_requests"] += 1
        self._counters[f"{prefix}_{'successes' if success else 'failures'}"] += 1
        self._batch_latencies_ms.append(max(0.0, float(latency_ms)))
        if success:
            self._last_success_ts = time.time()
        else:
            self._last_failure_ts = time.time()
            self._last_error_type = type(error).__name__ if error is not None else "unknown"

    def set_backoff(self, seconds: float) -> None:
        self._backoff_sec = max(0.0, float(seconds))

    @staticmethod
    def _percentile(values: Sequence[float], fraction: float) -> Optional[float]:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
        return round(ordered[index], 1)

    def stats(self) -> Dict[str, Any]:
        now = time.time()
        fresh_entries = sum(1 for _, ts in self._data.values() if (now - ts) < self._ttl)
        latencies = list(self._batch_latencies_ms)
        cache_lookups = self._counters["cache_hits"] + self._counters["cache_misses"]
        full_requests = self._counters["full_batch_requests"]
        return {
            **self._counters,
            "ttl_sec": self._ttl,
            "generation": self._generation,
            "entries": len(self._data),
            "fresh_entries": fresh_entries,
            "batch_latency_ms_p50": self._percentile(latencies, 0.50),
            "batch_latency_ms_p95": self._percentile(latencies, 0.95),
            "batch_latency_ms_max": round(max(latencies), 1) if latencies else None,
            "cache_hit_ratio": (
                round(self._counters["cache_hits"] / cache_lookups, 4)
                if cache_lookups
                else None
            ),
            "full_batch_success_ratio": (
                round(self._counters["full_batch_successes"] / full_requests, 4)
                if full_requests
                else None
            ),
            "last_success_age_sec": (
                round(now - self._last_success_ts, 1) if self._last_success_ts else None
            ),
            "last_failure_age_sec": (
                round(now - self._last_failure_ts, 1) if self._last_failure_ts else None
            ),
            "last_error_type": self._last_error_type or None,
            "backoff_sec": self._backoff_sec,
            "direct_rest_burst": self._direct_rest_burst,
            "direct_rest_window_sec": self._direct_rest_window_sec,
        }


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

    One batch request replaces N per-token GET /book calls. When that request
    fails, bounded chunk batches recover partial coverage without starting a
    per-token request storm. Repeated failures enter a short chunk-mode circuit
    with exponential backoff.

    `get_token_ids` is a zero-arg callable returning the current list of
    token ids — supports runtime market changes (auto-curator add/remove).
    """
    initial_tokens = get_token_ids()
    log(f"[book-fetcher] started for {len(initial_tokens)} token(s) (batch mode)")
    prev_tokens: Optional[List[str]] = None
    params: List[Dict[str, str]] = []
    backoff = 0.0
    consecutive_errors = 0
    chunk_mode_until = 0.0
    last_health_log_ts = 0.0
    chunk_size = max(2, int(getattr(primary_engine, "_shared_book_chunk_size", 12)))
    chunk_concurrency = max(
        1,
        min(4, int(getattr(primary_engine, "_shared_book_chunk_concurrency", 2))),
    )

    async def _fetch(payload: List[Dict[str, str]], *, kind: str):
        started = time.perf_counter()
        try:
            books = await asyncio.to_thread(primary_engine.client.get_order_books, payload)
        except Exception as exc:
            cache.record_batch_result(
                kind=kind,
                success=False,
                latency_ms=(time.perf_counter() - started) * 1000.0,
                error=exc,
            )
            return None, exc
        cache.record_batch_result(
            kind=kind,
            success=True,
            latency_ms=(time.perf_counter() - started) * 1000.0,
        )
        return books, None

    async def _fetch_chunks(
        payload: List[Dict[str, str]],
    ) -> tuple[int, int, int, bool]:
        chunks = [payload[i:i + chunk_size] for i in range(0, len(payload), chunk_size)]
        next_chunk = 0
        stop_scheduling = False
        results = []

        async def _worker() -> None:
            nonlocal next_chunk, stop_scheduling
            while not stop_scheduling and next_chunk < len(chunks):
                chunk = chunks[next_chunk]
                next_chunk += 1
                result = await _fetch(chunk, kind="chunk")
                results.append(result)
                _, error = result
                if error is not None and _is_rate_limit_error(error):
                    stop_scheduling = True

        await asyncio.gather(
            *[
                _worker()
                for _ in range(min(chunk_concurrency, len(chunks)))
            ]
        )
        successes = 0
        failures = 0
        stored = 0
        rate_limited = False
        for books, error in results:
            if error is not None:
                failures += 1
                rate_limited = rate_limited or _is_rate_limit_error(error)
                continue
            successes += 1
            stored += cache.put_many(books or [])
        return successes, failures, stored, rate_limited

    while primary_engine._running:
        current_tokens = list(get_token_ids())
        if current_tokens != prev_tokens:
            params = [{"token_id": tid} for tid in current_tokens]
            prev_tokens = current_tokens
            if not params:
                await asyncio.sleep(fetch_interval_sec)
                continue

        payload = [
            asdict(param) if hasattr(param, "__dataclass_fields__") else dict(param)
            for param in params
        ]
        use_chunk_mode = time.time() < chunk_mode_until
        if use_chunk_mode:
            successes, failures, stored, rate_limited = await _fetch_chunks(payload)
            if failures:
                consecutive_errors += 1
                backoff = min(30.0, max(1.0, 2 ** min(consecutive_errors - 1, 5)))
                chunk_mode_until = time.time() + max(5.0, backoff * 2)
                log(
                    f"[book-fetcher] chunk mode partial ok={successes} failed={failures} "
                    f"stored={stored} rate_limited={rate_limited} backoff={backoff:.1f}s"
                )
            else:
                consecutive_errors = max(0, consecutive_errors - 1)
                backoff = 0.0
                if consecutive_errors == 0:
                    chunk_mode_until = 0.0
        else:
            books, error = await _fetch(payload, kind="full")
            if error is None:
                stored = cache.put_many(books or [])
                if stored == 0 and books:
                    log(
                        f"[book-fetcher] batch returned {len(books)} book(s), "
                        "0 stored (empty bids/asks)"
                    )
                consecutive_errors = 0
                backoff = 0.0
            else:
                consecutive_errors += 1
                backoff = min(30.0, max(1.0, 2 ** min(consecutive_errors - 1, 5)))
                if _is_rate_limit_error(error):
                    log(
                        f"[book-fetcher] batch 429 — backoff {backoff:.1f}s "
                        f"(consecutive={consecutive_errors})"
                    )
                else:
                    successes, failures, stored, rate_limited = await _fetch_chunks(payload)
                    chunk_mode_until = time.time() + max(5.0, backoff * 2)
                    log(
                        f"[book-fetcher] batch err={type(error).__name__}; "
                        f"bounded_chunks ok={successes} failed={failures} stored={stored} "
                        f"rate_limited={rate_limited} backoff={backoff:.1f}s"
                    )

        cache.set_backoff(backoff)
        now = time.time()
        if now - last_health_log_ts >= 60.0:
            health = cache.stats()
            mode = "chunk" if now < chunk_mode_until else "full"
            log(
                f"[book-fetcher] health mode={mode} tokens={len(current_tokens)} "
                f"fresh={health['fresh_entries']} hit_ratio={health['cache_hit_ratio']} "
                f"p95_ms={health['batch_latency_ms_p95']} "
                f"full_ok={health['full_batch_successes']}/{health['full_batch_requests']} "
                f"chunk_fail={health['chunk_batch_failures']} backoff={backoff:.1f}s"
            )
            last_health_log_ts = now
        if not primary_engine._running:
            break
        delay = backoff if backoff > 0 else (max(1.0, fetch_interval_sec) if use_chunk_mode else fetch_interval_sec)
        await asyncio.sleep(delay)


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
    runtime_scope: str = "",
    expected_signer_url: str = "",
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
    if runtime_scope:
        expected_runtime["runtime_scope"] = runtime_scope
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
    if expected_signer_url:
        actual_signer_url = str(
            (payload.get("account") or {}).get("signer_server_url") or ""
        ).strip().rstrip("/")
        if actual_signer_url != expected_signer_url.strip().rstrip("/"):
            raise ValueError(
                f"{path.name} signer URL does not match the isolated runtime"
            )
    return market_sha


def _require_isolated_runtime_paths(
    runtime_root: Path,
    paths: Sequence[tuple[str, Path]],
) -> None:
    root = runtime_root.resolve()
    if root == Path(root.anchor):
        raise ValueError("isolated runtime root cannot be the filesystem root")
    for label, path in paths:
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError(
                f"isolated {label} must live below runtime root {root}: {resolved}"
            ) from exc


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


async def _run_aggressive_reward_observer_once(
    data_dir: Path,
    config_dir: Path,
) -> str:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(_MAKER_DIR / "reward_observer.py"),
        "--config-dir",
        str(config_dir),
        "--data-dir",
        str(data_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    try:
        output, _ = await asyncio.wait_for(process.communicate(), timeout=240.0)
    except asyncio.CancelledError:
        if process.returncode is None:
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            if process.returncode is None:
                process.kill()
            await process.wait()
        raise
    except asyncio.TimeoutError:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise RuntimeError("reward observer refresh timed out")
    message = output.decode("utf-8", errors="replace").strip()
    if process.returncode != 0:
        raise RuntimeError(message[:240] or "reward observer refresh failed")
    return message


async def _aggressive_reward_observer_loop(
    data_dir: Path,
    config_dir: Path,
    *,
    interval_sec: float = 300.0,
    refresh_once=None,
) -> None:
    """Maintain an isolated, read-only reward snapshot for aggressive LP."""

    refresh_fn = refresh_once or _run_aggressive_reward_observer_once
    while True:
        try:
            message = await refresh_fn(data_dir, config_dir)
            log(f"[aggressive-observer] {message or 'refresh complete'}")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log(
                "[aggressive-observer] refresh failed: "
                f"{type(exc).__name__}: {str(exc)[:160]}"
            )
        await asyncio.sleep(max(60.0, float(interval_sec)))


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
    runtime_scope: str = "",
    runtime_root: Optional[Path] = None,
    expected_signer_url: str = "",
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
    requested_scope = str(runtime_scope or "").strip().lower()

    accounts: tuple[RuntimeAccount, ...] = ()
    route_sha = ""
    resolved_host_id = host_id.strip().lower()
    config_files: list[tuple[int, Path]] = []
    global_profiles: Dict[int, LPAccountProfile] = {}
    local_market_sha = ""

    if roster_path is not None:
        resolved_roster_path = roster_path.resolve()
        accounts = load_runtime_roster(resolved_roster_path)
        roster_scope = load_runtime_roster_scope(resolved_roster_path)
        if roster_scope != requested_scope:
            raise ValueError(
                "runtime scope mismatch: "
                f"roster={roster_scope or 'legacy'} requested={requested_scope or 'legacy'}"
            )
        resolved_host_id = _resolve_host_id(accounts, resolved_host_id)
        if requested_scope == "aggressive":
            if runtime_root is None:
                raise ValueError("aggressive runtime requires --runtime-root")
            if not require_paused:
                raise ValueError("aggressive runtime must start with --require-paused")
            if not expected_signer_url.strip():
                raise ValueError("aggressive runtime requires --expected-signer-url")
            environment_signer_url = os.getenv("POLY_SIGNER_SERVER_URL", "").strip()
            if (
                not environment_signer_url
                or environment_signer_url.rstrip("/")
                != expected_signer_url.strip().rstrip("/")
            ):
                raise ValueError(
                    "aggressive runtime POLY_SIGNER_SERVER_URL must exactly match "
                    "--expected-signer-url"
                )
            if not resolved_host_id.startswith("aggressive-"):
                raise ValueError(
                    "aggressive runtime host ids must start with 'aggressive-'"
                )
            _require_isolated_runtime_paths(
                runtime_root,
                (
                    ("config directory", config_dir),
                    ("data directory", resolved_data_dir),
                    ("roster", resolved_roster_path),
                ),
            )
            invalid_profiles = [
                account.account_index
                for account in accounts
                if account.enabled
                and (
                    not account.profile.managed
                    or account.profile.profile_type != "aggressive"
                )
            ]
            if invalid_profiles:
                raise ValueError(
                    "aggressive runtime contains non-aggressive accounts: "
                    + ", ".join(map(str, invalid_profiles))
                )
        if len(roster_hosts(accounts)) > 1 and (
            not expected_roster_sha256.strip()
            or not expected_market_sha256.strip()
        ):
            raise ValueError(
                "multi-host roster mode requires --expected-roster-sha256 and "
                "--expected-market-sha256"
            )
        route_sha = routing_roster_sha256(accounts, requested_scope)
        roster_rows = _roster_config_files(config_dir, accounts, resolved_host_id)
        if require_paused:
            _require_pause_flags(
                resolved_data_dir,
                [account.account_index for account, _ in roster_rows],
            )
        market_shas: set[str] = set()
        for account, path in roster_rows:
            market_shas.add(
                _verify_roster_config(
                    account,
                    path,
                    route_sha,
                    requested_scope,
                    expected_signer_url,
                )
            )
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
        if requested_scope or runtime_root is not None or expected_signer_url:
            raise ValueError("isolated runtime options require --roster mode")
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
    resolved_data_dir.mkdir(parents=True, exist_ok=True)

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
        eng._runtime_scope = requested_scope
        eng._runtime_host_id = resolved_host_id
        eng._routing_roster_sha256 = route_sha
        eng._routing_market_universe_sha256 = local_market_sha
        eng._routing_account_count = len(global_profiles)
        eng._local_account_count = len(engines)
        eng._runtime_market_updates_enabled = roster_path is None
        eng._event_bus.set_runtime_namespace(requested_scope)
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

    primary_engine = engines[0][1]
    cache = SharedBookCache(
        ttl_sec=float(getattr(primary_engine, "_shared_book_cache_ttl_sec", 2.0)),
        direct_rest_burst=int(
            getattr(primary_engine, "_shared_book_direct_rest_burst", 4)
        ),
        direct_rest_window_sec=float(
            getattr(primary_engine, "_shared_book_direct_rest_window_sec", 1.0)
        ),
    )

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
    if requested_scope == "aggressive":
        worker_tasks.append(
            asyncio.create_task(
                _aggressive_reward_observer_loop(
                    resolved_data_dir,
                    config_dir,
                ),
                name="aggressive_reward_observer",
            )
        )
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
    parser.add_argument(
        "--runtime-scope",
        default=os.getenv("POLYMARKET_RUNTIME_SCOPE", ""),
        help="Isolated runtime scope declared by the roster",
    )
    parser.add_argument(
        "--runtime-root",
        default=os.getenv("POLYMARKET_RUNTIME_ROOT", ""),
        help="Filesystem root that must contain all isolated runtime inputs and state",
    )
    parser.add_argument(
        "--expected-signer-url",
        default=os.getenv("POLYMARKET_EXPECTED_SIGNER_URL", ""),
        help="Fail closed unless every generated config uses this signer service",
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
            runtime_scope=args.runtime_scope,
            runtime_root=Path(args.runtime_root).resolve() if args.runtime_root else None,
            expected_signer_url=args.expected_signer_url,
        )
    )


if __name__ == "__main__":
    main()
