"""
Redis-backed event bus for engine ↔ dashboard real-time communication.

Design:
  - Engine publishes events to Redis pub/sub channels + writes latest state to a Redis key.
  - Dashboard (or any consumer) subscribes for real-time updates, or polls the state key.
  - Graceful degradation: if Redis is unavailable, all publish calls are silent no-ops.
  - Connection is lazy (created on first publish) and auto-reconnects on failure.

Channels:
  polymarket:events        — all events (state changes, fills, alerts, quotes)
  polymarket:events:{type} — per-type channels for selective subscription

State keys:
  polymarket:state         — latest full engine state snapshot (JSON)
  polymarket:state:ts      — timestamp of last state write

Usage in engine:
  from event_bus import EventBus
  bus = EventBus(redis_url="redis://localhost:6379")
  bus.publish("fill", {"token_id": "...", "size": 10, "price": 0.55})
  bus.set_state(state_dict)

Usage in dashboard:
  bus = EventBus(redis_url="redis://localhost:6379")
  state = bus.get_state()          # poll latest state
  for event in bus.subscribe():    # blocking iterator
      print(event)
"""

import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Iterator, Optional

logger = logging.getLogger(__name__)

_CHANNEL_PREFIX = "polymarket"
_EVENTS_CHANNEL = f"{_CHANNEL_PREFIX}:events"
_STATE_KEY = f"{_CHANNEL_PREFIX}:state"
_STATE_TS_KEY = f"{_CHANNEL_PREFIX}:state:ts"
_HISTORY_KEY = f"{_CHANNEL_PREFIX}:history"
_HISTORY_MAX_LEN = 500  # keep last 500 events in Redis list
_STATE_NAMESPACE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._-]{0,127}$")
_RUNTIME_NAMESPACE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,31}$")


class EventBus:
    """Redis-backed event bus with graceful degradation."""

    def __init__(
        self,
        redis_url: str = "",
        enabled: bool = True,
        connect_timeout: float = 3.0,
        history_max_len: int = _HISTORY_MAX_LEN,
        state_namespace: str = "",
        runtime_namespace: str = "",
    ):
        self._redis_url = redis_url.strip()
        self._enabled = enabled and bool(self._redis_url)
        self._connect_timeout = connect_timeout
        self._history_max_len = history_max_len

        self._client = None  # redis.Redis instance (lazy)
        self._lock = threading.Lock()
        self._connected = False
        self._last_connect_attempt: float = 0.0
        self._connect_backoff: float = 1.0  # seconds between reconnect attempts
        self._max_backoff: float = 30.0
        self._publish_count: int = 0
        self._publish_errors: int = 0
        self._runtime_namespace = ""
        self._state_namespace = ""
        self._events_channel = _EVENTS_CHANNEL
        self._history_key = _HISTORY_KEY
        self._state_key = _STATE_KEY
        self._state_ts_key = _STATE_TS_KEY
        self.set_runtime_namespace(runtime_namespace)
        self.set_state_namespace(state_namespace)

    def set_runtime_namespace(self, namespace: str = "") -> None:
        """Isolate events, history, and state for one runtime domain."""
        normalized = str(namespace or "").strip().lower()
        if normalized and not _RUNTIME_NAMESPACE_RE.fullmatch(normalized):
            raise ValueError(f"invalid event-bus runtime namespace: {namespace!r}")
        self._runtime_namespace = normalized
        prefix = _CHANNEL_PREFIX + (f":{normalized}" if normalized else "")
        self._events_channel = f"{prefix}:events"
        self._history_key = f"{prefix}:history"
        self._refresh_state_keys()

    def _refresh_state_keys(self) -> None:
        prefix = _CHANNEL_PREFIX + (
            f":{self._runtime_namespace}" if self._runtime_namespace else ""
        )
        suffix = f":{self._state_namespace}" if self._state_namespace else ""
        self._state_key = f"{prefix}:state{suffix}"
        self._state_ts_key = f"{prefix}:state:ts{suffix}"

    def set_state_namespace(self, namespace: str = "") -> None:
        """Select an account-local state key inside the active runtime scope."""
        normalized = str(namespace or "").strip()
        if normalized and not _STATE_NAMESPACE_RE.fullmatch(normalized):
            raise ValueError(f"invalid event-bus state namespace: {namespace!r}")
        self._state_namespace = normalized
        self._refresh_state_keys()

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def stats(self) -> dict:
        return {
            "enabled": self._enabled,
            "connected": self._connected,
            "redis_url": self._redis_url[:30] + "..." if len(self._redis_url) > 30 else self._redis_url,
            "publish_count": self._publish_count,
            "publish_errors": self._publish_errors,
        }

    def _ensure_connection(self) -> bool:
        """Lazy connect to Redis. Returns True if connected."""
        if not self._enabled:
            return False
        if self._connected and self._client is not None:
            return True

        now = time.time()
        if now - self._last_connect_attempt < self._connect_backoff:
            return False

        with self._lock:
            # Double-check after acquiring lock
            if self._connected and self._client is not None:
                return True
            self._last_connect_attempt = now
            try:
                import redis
                self._client = redis.Redis.from_url(
                    self._redis_url,
                    socket_connect_timeout=self._connect_timeout,
                    socket_timeout=self._connect_timeout,
                    decode_responses=True,
                )
                self._client.ping()
                self._connected = True
                self._connect_backoff = 1.0  # reset backoff on success
                logger.info(f"[event-bus] connected to Redis at {self._redis_url}")
                return True
            except Exception as e:
                self._connected = False
                self._client = None
                self._connect_backoff = min(self._connect_backoff * 2, self._max_backoff)
                logger.debug(f"[event-bus] Redis connect failed: {e} (retry in {self._connect_backoff:.0f}s)")
                return False

    def _reset_connection(self):
        """Mark connection as dead so next call retries."""
        with self._lock:
            self._connected = False
            try:
                if self._client:
                    self._client.close()
            except Exception:
                pass
            self._client = None

    def publish(self, event_type: str, payload: dict, *, ts: Optional[float] = None) -> bool:
        """Publish an event to Redis pub/sub.

        Returns True if published, False if Redis is unavailable (silent no-op).
        """
        if not self._ensure_connection():
            return False

        event = {
            "type": event_type,
            "ts": ts or time.time(),
            "ts_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "payload": payload,
        }
        event_json = json.dumps(event, default=str)

        try:
            pipe = self._client.pipeline(transaction=False)
            # Publish to both the general channel and the type-specific channel
            pipe.publish(self._events_channel, event_json)
            pipe.publish(f"{self._events_channel}:{event_type}", event_json)
            # Append to history list (capped)
            pipe.lpush(self._history_key, event_json)
            pipe.ltrim(self._history_key, 0, self._history_max_len - 1)
            pipe.execute()
            self._publish_count += 1
            return True
        except Exception as e:
            self._publish_errors += 1
            logger.debug(f"[event-bus] publish error: {e}")
            self._reset_connection()
            return False

    def set_state(self, state: dict) -> bool:
        """Write the latest engine state snapshot to Redis.

        This replaces the file-based engine_state.json for consumers that prefer Redis.
        """
        if not self._ensure_connection():
            return False

        try:
            now = time.time()
            state_json = json.dumps(state, default=str)
            pipe = self._client.pipeline(transaction=False)
            pipe.set(self._state_key, state_json)
            pipe.set(self._state_ts_key, str(now))
            pipe.execute()
            return True
        except Exception as e:
            logger.debug(f"[event-bus] set_state error: {e}")
            self._reset_connection()
            return False

    def get_state(self) -> Optional[dict]:
        """Read the latest engine state from Redis. Returns None if unavailable."""
        if not self._ensure_connection():
            return None
        try:
            raw = self._client.get(self._state_key)
            if raw:
                return json.loads(raw)
            return None
        except Exception as e:
            logger.debug(f"[event-bus] get_state error: {e}")
            self._reset_connection()
            return None

    def get_history(self, count: int = 50) -> list[dict]:
        """Read recent events from the history list."""
        if not self._ensure_connection():
            return []
        try:
            items = self._client.lrange(self._history_key, 0, count - 1)
            return [json.loads(item) for item in items]
        except Exception as e:
            logger.debug(f"[event-bus] get_history error: {e}")
            self._reset_connection()
            return []

    def subscribe(self, event_types: Optional[list[str]] = None) -> Iterator[dict]:
        """Blocking iterator that yields events from Redis pub/sub.

        Args:
            event_types: if provided, subscribe only to these event types.
                         Otherwise subscribe to the general events channel.

        Yields:
            dict with keys: type, ts, ts_iso, payload
        """
        if not self._ensure_connection():
            return

        try:
            import redis
            pubsub = self._client.pubsub()
            if event_types:
                channels = [f"{self._events_channel}:{t}" for t in event_types]
            else:
                channels = [self._events_channel]
            pubsub.subscribe(*channels)

            for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        yield json.loads(message["data"])
                    except (json.JSONDecodeError, TypeError):
                        continue
        except Exception as e:
            logger.debug(f"[event-bus] subscribe error: {e}")
            self._reset_connection()

    def close(self):
        """Clean shutdown."""
        self._enabled = False
        self._reset_connection()
