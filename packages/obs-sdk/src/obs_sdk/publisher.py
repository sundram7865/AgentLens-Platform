"""Redis Streams publisher.

The hard rule of this file: **a publish failure must never reach the caller.**
SupportPilot answering a customer's ticket is the product; watching it is not.
If Redis is unreachable we drop events, count the drops, log with backoff, and
let the ticket complete normally.

Shape
-----
``publish()`` is a non-blocking ``put_nowait`` onto a bounded queue. A daemon
thread drains that queue and issues pipelined ``XADD``s. The request path pays
a queue append -- microseconds -- and never waits on a socket.

Why a thread and not asyncio: SupportPilot's agent runs are synchronous
LangGraph invocations inside a threadpool. A thread-backed queue works from both
sync and async callers with no event-loop assumptions.
"""

from __future__ import annotations

import atexit
import contextlib
import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from .schema import ObsEvent

logger = logging.getLogger("obs_sdk.publisher")

DEFAULT_STREAM = "obs:events"
DEFAULT_MAXLEN = 100_000
DEFAULT_QUEUE_SIZE = 10_000


@dataclass
class PublisherStats:
    """Cheap self-observability for the SDK. Surfaced on SupportPilot's own /health."""

    queued: int = 0
    published: int = 0
    dropped_queue_full: int = 0
    dropped_error: int = 0
    connect_failures: int = 0
    last_error: str | None = None
    last_published_at: float | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "queued": self.queued,
            "published": self.published,
            "dropped_queue_full": self.dropped_queue_full,
            "dropped_error": self.dropped_error,
            "connect_failures": self.connect_failures,
            "last_error": self.last_error,
            "last_published_at": self.last_published_at,
        }


class Publisher(Protocol):
    """The seam the callback handler emits through."""

    def publish(self, event: ObsEvent) -> bool: ...

    def flush(self, timeout: float = 5.0) -> bool: ...

    def close(self, timeout: float = 5.0) -> None: ...


class NullPublisher:
    """Drops everything. Used when observability is switched off by config."""

    def __init__(self) -> None:
        self.stats = PublisherStats()

    def publish(self, event: ObsEvent) -> bool:
        return False

    def flush(self, timeout: float = 5.0) -> bool:
        return True

    def close(self, timeout: float = 5.0) -> None:
        return None


class InMemoryPublisher:
    """Collects events in a list. The unit-test seam: no Redis, no threads."""

    def __init__(self) -> None:
        self.events: list[ObsEvent] = []
        self.stats = PublisherStats()
        self._lock = threading.Lock()

    def publish(self, event: ObsEvent) -> bool:
        with self._lock:
            self.events.append(event)
            self.stats.published += 1
        return True

    def flush(self, timeout: float = 5.0) -> bool:
        return True

    def close(self, timeout: float = 5.0) -> None:
        return None

    def by_type(self, event_type: str) -> list[ObsEvent]:
        with self._lock:
            return [e for e in self.events if e.type.value == event_type]

    def by_name(self, name: str) -> list[ObsEvent]:
        with self._lock:
            return [e for e in self.events if e.name == name]

    def clear(self) -> None:
        with self._lock:
            self.events.clear()


@dataclass
class RedisPublisherConfig:
    url: str
    stream: str = DEFAULT_STREAM
    maxlen: int = DEFAULT_MAXLEN
    queue_size: int = DEFAULT_QUEUE_SIZE
    batch_size: int = 50
    flush_interval: float = 0.25
    connect_timeout: float = 5.0
    socket_timeout: float = 5.0
    reconnect_backoff_max: float = 30.0
    log_every_n_drops: int = 100
    client_kwargs: dict[str, Any] = field(default_factory=dict)


class RedisStreamPublisher:
    """Buffered, fire-and-forget publisher backed by a Redis Stream."""

    def __init__(self, config: RedisPublisherConfig, client: Any | None = None) -> None:
        self.config = config
        self.stats = PublisherStats()
        self._queue: queue.Queue[ObsEvent] = queue.Queue(maxsize=config.queue_size)
        self._stop = threading.Event()
        self._closed = threading.Event()
        self._client: Any = client
        self._client_lock = threading.Lock()
        # Events pulled off the queue but not yet written. flush() must wait on
        # this too, or it reports success while a batch is still in the air.
        self._in_flight = 0
        self._backoff = 0.5
        self._thread = threading.Thread(target=self._run, name="obs-sdk-publisher", daemon=True)
        self._thread.start()
        # Best-effort flush on normal interpreter exit: a daemon thread would
        # otherwise be killed with events still buffered.
        atexit.register(self._atexit)

    # -- public API --------------------------------------------------------- #
    def publish(self, event: ObsEvent) -> bool:
        """Enqueue an event. Returns False if dropped. Never raises."""
        try:
            self._queue.put_nowait(event)
            self.stats.queued += 1
            return True
        except queue.Full:
            self.stats.dropped_queue_full += 1
            n = self.stats.dropped_queue_full
            # Log the first drop, then every Nth: a sustained outage must not
            # become a second incident via log volume.
            if n == 1 or n % self.config.log_every_n_drops == 0:
                logger.warning(
                    "obs-sdk: publish queue full, %s events dropped so far "
                    "(Redis or the consumer is behind)",
                    n,
                )
            return False
        except Exception as exc:  # pragma: no cover - defensive
            self.stats.dropped_error += 1
            self.stats.last_error = repr(exc)
            return False

    def flush(self, timeout: float = 5.0) -> bool:
        """Block until everything published so far has left for Redis.

        Waits on the queue **and** on the batch the drain thread has already
        taken out of it. Checking only the queue reports "drained" while a batch
        is still mid-``XADD``: the queue empties the instant the thread picks the
        events up, not when the socket write completes. The original code papered
        over that with a 10 ms sleep, which is a guess about network latency
        rather than a guarantee, and it makes this method quietly lie to anyone
        who calls it before asserting delivery.
        """
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._queue.empty() and self._in_flight == 0:
                return True
            time.sleep(0.01)
        return self._queue.empty() and self._in_flight == 0

    def close(self, timeout: float = 5.0) -> None:
        """Flush, stop the drain thread, close the socket. Idempotent."""
        if self._closed.is_set():
            return
        self._closed.set()
        # Drop the exit hook first. It is registered per instance and would
        # otherwise outlive the publisher it belongs to: an application that
        # builds a tracer per worker, per test, or per reload accumulates one
        # dead hook each time, and pays up to `timeout` seconds for every one of
        # them at shutdown. The cost is invisible until it is not -- a suite that
        # created a few dozen publishers appeared to hang after printing its
        # results, and was really just walking an exit-hook list nobody pruned.
        with contextlib.suppress(Exception):
            atexit.unregister(self._atexit)
        self.flush(timeout=timeout)
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=timeout)
        with self._client_lock:
            client, self._client = self._client, None
        if client is not None:
            with contextlib.suppress(Exception):  # pragma: no cover - defensive
                client.close()

    # -- internals ---------------------------------------------------------- #
    def _atexit(self) -> None:  # pragma: no cover - interpreter shutdown
        with contextlib.suppress(Exception):
            self.close(timeout=2.0)

    def _connect(self) -> Any:
        import redis  # lazy: keeps `import obs_sdk` cheap

        return redis.Redis.from_url(
            self.config.url,
            socket_connect_timeout=self.config.connect_timeout,
            socket_timeout=self.config.socket_timeout,
            retry_on_timeout=True,
            health_check_interval=30,
            decode_responses=False,
            **self.config.client_kwargs,
        )

    def _get_client(self) -> Any | None:
        with self._client_lock:
            if self._client is not None:
                return self._client
        try:
            client = self._connect()
            client.ping()
        except Exception as exc:
            self.stats.connect_failures += 1
            self.stats.last_error = repr(exc)
            if self.stats.connect_failures == 1 or self.stats.connect_failures % 20 == 0:
                logger.warning("obs-sdk: cannot reach Redis (%s); events are being dropped", exc)
            return None
        with self._client_lock:
            self._client = client
        self._backoff = 0.5
        return client

    def _drop_client(self) -> None:
        with self._client_lock:
            client, self._client = self._client, None
        if client is not None:
            with contextlib.suppress(Exception):
                client.close()

    def _drain_batch(self) -> list[ObsEvent]:
        batch: list[ObsEvent] = []
        deadline = time.monotonic() + self.config.flush_interval
        while len(batch) < self.config.batch_size:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                batch.append(self._queue.get(timeout=min(remaining, 0.1)))
            except queue.Empty:
                break
        return batch

    def _run(self) -> None:
        while not self._stop.is_set() or not self._queue.empty():
            batch = self._drain_batch()
            if not batch:
                continue
            self._in_flight = len(batch)
            client = self._get_client()
            if client is None:
                # No connection: drop the batch rather than grow memory without bound.
                self.stats.dropped_error += len(batch)
                self._in_flight = 0
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, self.config.reconnect_backoff_max)
                continue
            try:
                pipe = client.pipeline(transaction=False)
                for event in batch:
                    pipe.xadd(
                        self.config.stream,
                        encode_event(event),
                        maxlen=self.config.maxlen,
                        # `~` (approximate) trimming lets Redis trim at a macro-node
                        # boundary. Exact trimming walks the stream and is measurably
                        # more expensive at volume.
                        approximate=True,
                    )
                pipe.execute()
                self.stats.published += len(batch)
                self.stats.last_published_at = time.time()
                self._in_flight = 0
            except Exception as exc:
                self.stats.dropped_error += len(batch)
                self.stats.last_error = repr(exc)
                logger.warning("obs-sdk: XADD failed, dropped %s events: %s", len(batch), exc)
                self._in_flight = 0
                self._drop_client()
                time.sleep(self._backoff)
                self._backoff = min(self._backoff * 2, self.config.reconnect_backoff_max)


def encode_event(event: ObsEvent) -> dict[str, str]:
    """Wire encoding: a version field beside one JSON blob.

    The version is duplicated outside the JSON so a consumer can route or
    dead-letter an event without paying for a full parse.
    """
    return {"v": str(event.schema_version), "data": event.to_json()}


def decode_event_fields(fields: dict[Any, Any]) -> dict[str, Any]:
    """Inverse of :func:`encode_event`, tolerant of bytes-vs-str keys.

    redis-py returns bytes when ``decode_responses=False`` and str when True;
    consumers should not care which client built the connection.
    """

    def _s(v: Any) -> Any:
        return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else v

    decoded = {str(_s(k)): _s(v) for k, v in fields.items()}
    raw = decoded.get("data")
    if raw is None:
        raise ValueError("stream entry has no 'data' field")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("stream entry 'data' is not a JSON object")
    if "schema_version" not in payload and "v" in decoded:
        with contextlib.suppress(TypeError, ValueError):
            payload["schema_version"] = int(decoded["v"])
    return payload


def _falsey(value: str) -> bool:
    return value.strip().lower() in {"0", "false", "no", "off", ""}


def publisher_from_env(env: dict[str, str] | None = None) -> Publisher:
    """Build a publisher from environment variables.

    ``OBS_ENABLED=false`` or a missing ``OBS_REDIS_URL`` yields a
    :class:`NullPublisher`, so an app can import and wire the SDK unconditionally
    and still run with no observability infrastructure present at all. That
    property is what makes the SupportPilot integration a safe one-liner.
    """
    e = dict(os.environ if env is None else env)
    if _falsey(e.get("OBS_ENABLED", "true")):
        return NullPublisher()
    url = e.get("OBS_REDIS_URL") or e.get("REDIS_URL")
    if not url:
        logger.info("obs-sdk: no OBS_REDIS_URL set; observability disabled")
        return NullPublisher()
    return RedisStreamPublisher(
        RedisPublisherConfig(
            url=url,
            stream=e.get("OBS_STREAM", DEFAULT_STREAM),
            maxlen=int(e.get("OBS_STREAM_MAXLEN", str(DEFAULT_MAXLEN))),
            queue_size=int(e.get("OBS_QUEUE_SIZE", str(DEFAULT_QUEUE_SIZE))),
            batch_size=int(e.get("OBS_BATCH_SIZE", "50")),
            flush_interval=float(e.get("OBS_FLUSH_INTERVAL", "0.25")),
        )
    )


__all__ = [
    "DEFAULT_MAXLEN",
    "DEFAULT_QUEUE_SIZE",
    "DEFAULT_STREAM",
    "InMemoryPublisher",
    "NullPublisher",
    "Publisher",
    "PublisherStats",
    "RedisPublisherConfig",
    "RedisStreamPublisher",
    "decode_event_fields",
    "encode_event",
    "publisher_from_env",
]
