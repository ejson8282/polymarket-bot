"""Read-only access to public Polymarket endpoints for research tooling.

Only public, unauthenticated endpoints are used: the CLOB sampling-markets,
books and prices-history APIs plus the public data-api trade feed. This
module never reads credentials or runtime state, never signs anything and
never sends a state-changing request.
"""

from __future__ import annotations

import time
from typing import Any, Iterable, Mapping, Optional, Sequence

import requests

CLOB_BASE = "https://clob.polymarket.com"
DATA_API_BASE = "https://data-api.polymarket.com"

DEFAULT_TIMEOUT_SEC = 15.0
DEFAULT_RETRY_COUNT = 3
DEFAULT_RETRY_BACKOFF_SEC = 1.5
DEFAULT_PAGE_SLEEP_SEC = 0.4
SAMPLING_TERMINAL_CURSOR = "LTE="

_BOOKS_CHUNK_SIZE = 20


class PublicDataError(RuntimeError):
    """A public endpoint returned an unusable response."""


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    json_body: Any = None,
    timeout_sec: float = DEFAULT_TIMEOUT_SEC,
    retry_count: int = DEFAULT_RETRY_COUNT,
    retry_backoff_sec: float = DEFAULT_RETRY_BACKOFF_SEC,
) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(1, retry_count + 1):
        try:
            response = session.request(
                method,
                url,
                params=params,
                json=json_body,
                timeout=timeout_sec,
            )
            if response.status_code == 429 or response.status_code >= 500:
                raise PublicDataError(
                    f"{url} returned HTTP {response.status_code}"
                )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, PublicDataError, ValueError) as exc:
            last_error = exc
            if attempt < retry_count:
                time.sleep(retry_backoff_sec * attempt)
    raise PublicDataError(f"{method} {url} failed after {retry_count} attempts: {last_error}")


def new_session() -> requests.Session:
    session = requests.Session()
    session.headers["User-Agent"] = "polymarket-bot-research/1.0 (read-only)"
    return session


def fetch_sampling_markets(
    session: Optional[requests.Session] = None,
    *,
    max_pages: int = 50,
    page_sleep_sec: float = DEFAULT_PAGE_SLEEP_SEC,
) -> list[dict[str, Any]]:
    """Return every reward-eligible (sampling) market row from the CLOB API."""
    session = session or new_session()
    rows: list[dict[str, Any]] = []
    cursor = ""
    for _ in range(max_pages):
        params: dict[str, Any] = {}
        if cursor:
            params["next_cursor"] = cursor
        payload = _request_json(
            session, "GET", f"{CLOB_BASE}/sampling-markets", params=params
        )
        if not isinstance(payload, Mapping):
            raise PublicDataError("sampling-markets payload is not an object")
        data = payload.get("data")
        if isinstance(data, list):
            rows.extend(row for row in data if isinstance(row, Mapping))
        cursor = str(payload.get("next_cursor") or "")
        if not cursor or cursor == SAMPLING_TERMINAL_CURSOR:
            break
        time.sleep(page_sleep_sec)
    return [dict(row) for row in rows]


def fetch_books(
    token_ids: Sequence[str],
    session: Optional[requests.Session] = None,
    *,
    chunk_size: int = _BOOKS_CHUNK_SIZE,
    page_sleep_sec: float = DEFAULT_PAGE_SLEEP_SEC,
) -> dict[str, dict[str, Any]]:
    """Batch-fetch order books; returns {token_id: book} for resolvable ids."""
    session = session or new_session()
    books: dict[str, dict[str, Any]] = {}
    unique_ids = [str(token_id) for token_id in dict.fromkeys(token_ids) if str(token_id)]
    for start in range(0, len(unique_ids), chunk_size):
        chunk = unique_ids[start : start + chunk_size]
        payload = _request_json(
            session,
            "POST",
            f"{CLOB_BASE}/books",
            json_body=[{"token_id": token_id} for token_id in chunk],
        )
        if not isinstance(payload, list):
            raise PublicDataError("books payload is not a list")
        for book in payload:
            if not isinstance(book, Mapping):
                continue
            asset_id = str(book.get("asset_id") or book.get("token_id") or "")
            if asset_id:
                books[asset_id] = dict(book)
        if start + chunk_size < len(unique_ids):
            time.sleep(page_sleep_sec)
    return books


def fetch_prices_history(
    token_id: str,
    *,
    start_ts: int,
    end_ts: int,
    fidelity_min: int = 1,
    session: Optional[requests.Session] = None,
) -> list[tuple[int, float]]:
    """Return [(unix_ts, price)] samples for one token over a window."""
    session = session or new_session()
    payload = _request_json(
        session,
        "GET",
        f"{CLOB_BASE}/prices-history",
        params={
            "market": str(token_id),
            "startTs": int(start_ts),
            "endTs": int(end_ts),
            "fidelity": int(fidelity_min),
        },
    )
    if not isinstance(payload, Mapping):
        raise PublicDataError("prices-history payload is not an object")
    history = payload.get("history")
    samples: list[tuple[int, float]] = []
    if isinstance(history, list):
        for row in history:
            if not isinstance(row, Mapping):
                continue
            try:
                samples.append((int(row["t"]), float(row["p"])))
            except (KeyError, TypeError, ValueError):
                continue
    samples.sort(key=lambda item: item[0])
    return samples


def fetch_user_trades(
    user_address: str,
    session: Optional[requests.Session] = None,
    *,
    max_rows: int = 2000,
    page_size: int = 500,
    page_sleep_sec: float = DEFAULT_PAGE_SLEEP_SEC,
) -> list[dict[str, Any]]:
    """Return public trade rows for one proxy-wallet (funder) address.

    The data-api trade feed is public, keyed by on-chain settlement, and
    requires no credentials. The address itself is public information.
    """
    session = session or new_session()
    user = str(user_address).strip().lower()
    if not user.startswith("0x") or len(user) != 42:
        raise ValueError("user_address must be a 0x-prefixed 20-byte address")
    rows: list[dict[str, Any]] = []
    offset = 0
    while len(rows) < max_rows:
        limit = min(page_size, max_rows - len(rows))
        payload = _request_json(
            session,
            "GET",
            f"{DATA_API_BASE}/trades",
            params={"user": user, "limit": limit, "offset": offset},
        )
        if not isinstance(payload, list):
            raise PublicDataError("trades payload is not a list")
        page = [dict(row) for row in payload if isinstance(row, Mapping)]
        rows.extend(page)
        if len(page) < limit:
            break
        offset += len(page)
        time.sleep(page_sleep_sec)
    return rows


def iter_token_ids(markets: Iterable[Mapping[str, Any]]) -> list[str]:
    """Collect every token id referenced by sampling-market rows."""
    token_ids: list[str] = []
    for market in markets:
        tokens = market.get("tokens")
        if not isinstance(tokens, list):
            continue
        for token in tokens:
            if isinstance(token, Mapping):
                token_id = str(token.get("token_id") or "")
                if token_id:
                    token_ids.append(token_id)
    return list(dict.fromkeys(token_ids))
