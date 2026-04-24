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
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Add maker dir to path for engine import
_MAKER_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(_MAKER_DIR))

from engine import PolyLPSMulti, log  # noqa: E402
from rewards_snapshot import rewards_snapshot_loop  # noqa: E402

from py_clob_client.clob_types import BookParams  # noqa: E402


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
    params: List[BookParams] = []
    backoff = 0.0
    consecutive_errors = 0

    while primary_engine._running:
        current_tokens = list(get_token_ids())
        if current_tokens != prev_tokens:
            params = [BookParams(token_id=tid) for tid in current_tokens]
            prev_tokens = current_tokens
            if not params:
                await asyncio.sleep(fetch_interval_sec)
                continue

        if backoff > 0:
            await asyncio.sleep(backoff)

        try:
            books = await asyncio.to_thread(primary_engine.client.get_order_books, params)
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


# ── Per-account wrapper ────────────────────────────────────────────────────────

async def run_account(
    engine: PolyLPSMulti,
    account_idx: int,
    cache: SharedBookCache,
    startup_delay: float,
    data_dir: Path,
) -> None:
    """Run one account engine with shared book cache and startup stagger."""
    import os as _os
    # Write per-account PID file so dashboard can track process liveness
    pid_path = data_dir / f".engine_{account_idx}.pid"
    try:
        pid_path.write_text(str(_os.getpid()), encoding="utf-8")
    except Exception:
        pass

    log(f"[multi] account {account_idx}: startup delay {startup_delay:.1f}s")
    await asyncio.sleep(startup_delay)
    engine._shared_book_cache = cache
    log(f"[multi] account {account_idx}: starting engine")
    try:
        await engine.run()
    except Exception as e:
        log(f"[multi] account {account_idx}: engine exited with error: {e}")
    finally:
        try:
            pid_path.unlink(missing_ok=True)
        except Exception:
            pass


# ── Main entry point ───────────────────────────────────────────────────────────

async def multi_run(config_dir: Path) -> None:
    # Discover config files: config_1.json ... config_30.json
    config_files: List[Tuple[int, Path]] = []
    for i in range(1, 31):
        p = config_dir / f"config_{i}.json"
        if p.exists():
            config_files.append((i, p))
            log(f"[multi] found config_{i}.json")

    if not config_files:
        log(f"[multi] no config_N.json files found in {config_dir}")
        log("[multi] create config_1.json, config_2.json, ... by copying config.json")
        return

    log(f"[multi] initializing {len(config_files)} account(s)")

    # Initialize engines
    engines: List[Tuple[int, PolyLPSMulti]] = []
    for idx, cfg_path in config_files:
        try:
            eng = PolyLPSMulti(config_path=str(cfg_path))
            engines.append((idx, eng))
            log(f"[multi] account {idx}: initialized ({len(eng.market_cfg)} markets)")
        except Exception as e:
            log(f"[multi] account {idx}: init failed — {e}")

    if not engines:
        log("[multi] no accounts could be initialized — exiting")
        return

    # Collect all unique token IDs across all accounts
    all_token_ids = list({
        tid
        for _, eng in engines
        for tid in eng.market_cfg.keys()
    })
    log(f"[multi] shared book cache: {len(all_token_ids)} unique market(s) across all accounts")

    # Derive data directory from config_dir (3 levels up from maker/ → repo root / data/)
    data_dir = config_dir.parent.parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # Shared book cache
    cache = SharedBookCache(ttl_sec=0.5)

    # Primary engine provides the shared book fetcher (uses first account's client)
    primary_engine = engines[0][1]

    def _all_tokens_fn() -> List[str]:
        return list({tid for _, eng in engines for tid in eng.market_cfg.keys()})

    tasks = [
        asyncio.create_task(
            _shared_book_fetcher(primary_engine, _all_tokens_fn, cache),
            name="shared_book_fetcher",
        ),
        asyncio.create_task(
            rewards_snapshot_loop(list(config_files), data_dir),
            name="rewards_snapshot",
        ),
    ]

    # Stagger account startups: 0s for first, then random 3-20s gaps
    cumulative_delay = 0.0
    for idx, eng in engines:
        tasks.append(
            asyncio.create_task(
                run_account(eng, idx, cache, startup_delay=cumulative_delay, data_dir=data_dir),
                name=f"account_{idx}",
            )
        )
        cumulative_delay += random.uniform(3.0, 20.0)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log("[multi] keyboard interrupt — shutting down")
    finally:
        for _, eng in engines:
            eng._running = False
        for t in tasks:
            t.cancel()
        log("[multi] all tasks cancelled")


def main() -> None:
    parser = argparse.ArgumentParser(description="PolyLPS multi-account runner")
    parser.add_argument(
        "--config-dir",
        default=str(_MAKER_DIR),
        help="Directory containing config_1.json, config_2.json, ... (default: same directory as this script)",
    )
    args = parser.parse_args()
    config_dir = Path(args.config_dir).resolve()
    log(f"[multi] config dir: {config_dir}")
    asyncio.run(multi_run(config_dir))


if __name__ == "__main__":
    main()
