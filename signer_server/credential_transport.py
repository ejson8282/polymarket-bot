"""One-shot recovery for a stale shared credential transport."""

from collections.abc import Callable
import threading
from typing import TypeVar


T = TypeVar("T")


def is_request_transport_failure(exc: Exception) -> bool:
    """Return whether py-clob-client lost its transport before an HTTP reply."""
    return (
        getattr(exc, "status_code", None) is None
        and "request exception" in str(exc).lower()
    )


class CredentialTransportRecovery:
    """Reset a shared transport once, then retry the failed operation once.

    Multiple requests can observe the same dead transport concurrently.  The
    generation check ensures only the first waiter replaces it; later waiters
    reuse the replacement instead of closing a newly healthy client.
    """

    def __init__(self, reset_transport: Callable[[], None]) -> None:
        self._reset_transport = reset_transport
        self._generation = 0
        self._lock = threading.Lock()

    def run(self, operation: Callable[[], T]) -> tuple[T, bool]:
        generation = self._generation
        try:
            return operation(), False
        except Exception as exc:
            if not is_request_transport_failure(exc):
                raise

        with self._lock:
            if generation == self._generation:
                self._reset_transport()
                self._generation += 1

        return operation(), True
