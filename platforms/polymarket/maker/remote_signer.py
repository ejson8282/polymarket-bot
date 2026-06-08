"""
Remote signer client — Windows side.
Calls the Mac Mini signer_server for derive-creds and sign-order.
"""

import os
import time
import random
import logging

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, HTTPError, ReadTimeout
from http.client import RemoteDisconnected

logger = logging.getLogger(__name__)

# HTTP status codes that are transient and worth retrying.
_RETRYABLE_STATUS_CODES = {500, 502, 503, 504}

# Transient exceptions that justify an application-level retry with backoff.
_TRANSIENT_EXCEPTIONS = (ConnectionError, ReadTimeout, RemoteDisconnected, OSError)


class AddressStub:
    """
    Minimal substitute for py_clob_client's signer object.
    Only provides address() and chain_id — enough for L2 HMAC headers
    used by post_order / cancel_orders.
    """

    def __init__(self, address: str, chain_id: int = 137):
        self._address = address
        self._chain_id = chain_id

    def address(self) -> str:
        return self._address

    def get_chain_id(self) -> int:
        return self._chain_id


class BuilderStub:
    """
    Minimal substitute for py_clob_client's OrderBuilder.
    Provides sig_type and funder so read-only ClobClient methods
    that reference self.builder don't crash.
    """

    def __init__(self, sig_type: int = 2, funder: str = ""):
        self.sig_type = sig_type
        self.signature_type = sig_type  # V2 SDK renamed sig_type -> signature_type
        self.funder = funder


class RemoteSignerClient:
    """HTTP client for the signer server running on Mac Mini."""

    TIMEOUT = 15.0  # seconds (generous for Tailscale, covers cold-start)

    def __init__(
        self,
        server_url: str,
        token: str | None = None,
        max_retries: int = 3,
        base_delay: float = 0.5,
        funder: str | None = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.token = token or os.getenv("SIGNER_TOKEN", "").strip()
        if not self.token:
            raise ValueError("Signer token not provided. Set SIGNER_TOKEN env var.")
        # Funder address routes the request to the right key on the multi-key
        # signer. Empty string ⇒ legacy single-key mode on the server.
        self.funder = (funder or "").strip().lower() or None
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._session = self._build_session()

    # ── session / connection-pool management ────────────────────────

    def _build_session(self) -> requests.Session:
        """Create a new Session with connection-pooling and transport-level retries."""
        session = requests.Session()
        session.headers["Authorization"] = f"Bearer {self.token}"
        # Bypass proxy for Tailscale / local signer server
        session.trust_env = False

        # urllib3 Retry handles low-level transport hiccups (resets, DNS
        # blips) transparently before our application-level retry kicks in.
        transport_retry = Retry(
            total=2,
            backoff_factor=0.3,
            status_forcelist=[502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=transport_retry,
            pool_connections=8,
            pool_maxsize=16,
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _reset_session(self) -> None:
        """Close the current session and create a fresh one.

        Called when the connection pool appears exhausted or stale
        (e.g. after repeated RemoteDisconnected errors).
        """
        try:
            self._session.close()
        except Exception:
            pass
        self._session = self._build_session()
        logger.info("RemoteSignerClient: session recreated")

    # ── retry wrapper ───────────────────────────────────────────────

    def _request_with_retry(self, method: str, url: str, **kwargs):
        """Execute an HTTP request with exponential backoff + jitter.

        On transient failures the session is recreated (once per retry cycle)
        so stale / broken pooled connections are discarded.
        """
        kwargs.setdefault("timeout", self.TIMEOUT)
        last_exc: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                resp = self._session.request(method, url, **kwargs)
                resp.raise_for_status()
                return resp
            except HTTPError as exc:
                if exc.response is None or exc.response.status_code not in _RETRYABLE_STATUS_CODES:
                    raise  # 4xx errors (auth, bad request) are not retryable
                last_exc = exc
            except _TRANSIENT_EXCEPTIONS as exc:
                last_exc = exc

            # Shared retry logic for all transient errors
            if attempt < self._max_retries:
                delay = self._base_delay * (2 ** (attempt - 1))
                jitter = random.uniform(0, delay * 0.5)
                sleep_time = delay + jitter
                logger.warning(
                    "RemoteSignerClient: transient error on attempt %d/%d "
                    "(%s: %s) — retrying in %.2fs",
                    attempt,
                    self._max_retries,
                    type(last_exc).__name__,
                    last_exc,
                    sleep_time,
                )
                time.sleep(sleep_time)
                self._reset_session()
            else:
                logger.error(
                    "RemoteSignerClient: all %d attempts exhausted (%s: %s)",
                    self._max_retries,
                    type(last_exc).__name__,
                    last_exc,
                )

        # Re-raise the last transient exception so callers see the real error
        raise last_exc  # type: ignore[misc]

    # ── public API (unchanged signatures) ───────────────────────────

    def health(self) -> dict:
        resp = self._request_with_retry("GET", f"{self.server_url}/health")
        return resp.json()

    def derive_creds(self) -> dict:
        """
        Returns: {api_key, api_secret, api_passphrase, address}
        """
        payload: dict = {}
        if self.funder:
            payload["funder"] = self.funder
        resp = self._request_with_retry(
            "POST",
            f"{self.server_url}/derive-creds",
            json=payload,
        )
        return resp.json()

    def sign_order(self, token_id: str, price: float, size: float, side: str) -> dict:
        """
        Send order params to signer server, returns the signed order dict.
        side: "BUY" or "SELL"
        """
        payload = {
            "token_id": token_id,
            "price": price,
            "size": size,
            "side": side,
        }
        if self.funder:
            payload["funder"] = self.funder
        resp = self._request_with_retry(
            "POST",
            f"{self.server_url}/sign-order",
            json=payload,
        )
        data = resp.json()
        return data["signed_order"]
