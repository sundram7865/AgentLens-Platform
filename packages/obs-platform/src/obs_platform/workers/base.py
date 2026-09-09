"""Stream consumer base class.

Everything hard about running a Redis Streams consumer in production lives here,
once, so each concrete consumer only has to say what to *do* with a batch.

What this base guarantees
-------------------------
**At-least-once, never infinitely.** Messages are acked only after the handler
says they are done. A message that keeps failing is counted (via ``XPENDING``'s
``times-delivered``) and, past ``max_delivery_attempts``, moved to the
``dead_letters`` table and acked. Without that ceiling a single poison message
is a self-inflicted outage: it is redelivered forever, and every redelivery
displaces real work.

**A permanent failure is not retried.** The handler classifies each message.
A payload that will never parse is dead-lettered immediately -- retrying it 5
times first only wastes 5 times as much.

**Ack is separate from the work.** The try/except around the handler is a
different try/except from the ack, precisely so a transient database timeout
leaves the message pending (it retries) while a genuinely bad message gets
acked (it does not).

**SIGTERM finishes the batch.** Render, Fly, Kubernetes and `docker compose
down` all send SIGTERM on every redeploy. A worker that ignores it is killed
mid-write and loses or duplicates a message on *every single deploy*. Here,
SIGTERM stops the loop from fetching more, lets the in-flight batch finish, and
then exits.

**Idle costs one command per block.** ``XREADGROUP`` blocks for
``consumer_block_ms``; a polling loop at 100 ms would burn ~864k commands a day
against Upstash's 10k/day free tier doing nothing at all.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from obs_sdk.publisher import decode_event_fields
from obs_sdk.schema import ObsEvent, SchemaError, parse_event

from ..db import Database, get_db, table_of, upsert
from ..logging import get_logger
from ..models import DeadLetter, WorkerHeartbeat
from ..redis_io import ensure_group, get_redis
from ..settings import Settings, get_settings

log = get_logger("obs_platform.worker")


class PermanentError(Exception):
    """This message will never succeed. Dead-letter it; do not retry."""


@dataclass
class StreamMessage:
    """One entry read from the stream, with its redelivery count."""

    message_id: str
    fields: dict[str, str]
    delivery_count: int = 1

    def payload(self) -> dict[str, Any]:
        return decode_event_fields(self.fields)


@dataclass
class ParsedMessage:
    message: StreamMessage
    event: ObsEvent

    @property
    def message_id(self) -> str:
        return self.message.message_id


@dataclass
class BatchOutcome:
    """What the handler decided for each message in a batch."""

    ack: list[str] = field(default_factory=list)
    retry: list[str] = field(default_factory=list)
    dead: list[tuple[StreamMessage, str, str]] = field(default_factory=list)

    def extend(self, other: BatchOutcome) -> None:
        self.ack.extend(other.ack)
        self.retry.extend(other.retry)
        self.dead.extend(other.dead)


@dataclass
class ConsumerStats:
    processed: int = 0
    failed: int = 0
    dead_lettered: int = 0
    batches: int = 0
    last_success_at: datetime | None = None
    last_error: str | None = None


class StreamConsumer(ABC):
    """Base class for every stream consumer in the platform."""

    #: Short role name; also the suffix of the consumer group.
    role: str = "base"

    def __init__(
        self,
        settings: Settings | None = None,
        redis: Redis | None = None,
        database: Database | None = None,
        consumer_name: str | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._redis = redis
        self._db = database
        self.stream = self.settings.stream
        self.group = f"obs-{self.role}"
        # hostname:pid -- unique per process, so several replicas can share the
        # group and Redis can attribute pending messages to the right consumer.
        self.consumer_name = consumer_name or f"{socket.gethostname()}:{os.getpid()}"
        self.stats = ConsumerStats()
        self._stop = asyncio.Event()
        self._started_at = datetime.now(UTC)
        self._last_claim_at = 0.0
        self._claim_supported = True
        # Upper bound on how long an idle tick waits before looping. Small enough
        # to stay responsive to SIGTERM, large enough not to spin.
        self._idle_yield_seconds = min(0.05, max(self.settings.consumer_block_ms / 1000.0, 0.001))

    # -- wiring -------------------------------------------------------------
    @property
    def redis(self) -> Redis:
        return self._redis if self._redis is not None else get_redis()

    @property
    def db(self) -> Database:
        return self._db if self._db is not None else get_db()

    @property
    def name(self) -> str:
        return f"{self.consumer_name}#{self.role}"

    # -- lifecycle ----------------------------------------------------------
    def request_stop(self) -> None:
        """Ask the loop to finish the current batch and exit."""
        if not self._stop.is_set():
            log.info("worker.stop_requested", role=self.role, consumer=self.consumer_name)
            self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    async def setup(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Hook for subclasses. Called once before the loop starts."""

    async def teardown(self) -> None:  # noqa: B027 - optional hook, not abstract
        """Hook for subclasses. Called once after the loop exits."""

    async def run(self) -> None:
        await ensure_group(self.stream, self.group, self.redis)
        await self.setup()
        await self._heartbeat("running")
        log.info(
            "worker.started",
            role=self.role,
            consumer=self.consumer_name,
            stream=self.stream,
            group=self.group,
        )
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            while not self._stop.is_set():
                try:
                    await self._tick()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # A crash in the loop itself must not kill the worker; back
                    # off so a persistent outage does not become a hot loop.
                    self.stats.last_error = repr(exc)
                    log.exception("worker.tick_failed", role=self.role)
                    await self._sleep(2.0)
        finally:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self.teardown()
            await self._heartbeat("stopped")
            log.info(
                "worker.stopped",
                role=self.role,
                processed=self.stats.processed,
                failed=self.stats.failed,
                dead_lettered=self.stats.dead_lettered,
            )

    async def _sleep(self, seconds: float) -> None:
        """Sleep that wakes immediately on a stop request."""
        with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def _tick(self) -> None:
        messages = await self._reclaim_pending()
        if not messages:
            messages = await self._read_new()
        if not messages:
            # A blocking XREADGROUP normally suspends for consumer_block_ms, which
            # yields the event loop. Some clients (and any server that answers
            # immediately) return without ever suspending -- and an await that
            # completes synchronously does NOT yield. Without this sleep the loop
            # becomes a busy-spin that pegs a core and starves the heartbeat task
            # in the same process, so the worker looks dead while burning CPU.
            await self._sleep(self._idle_yield_seconds)
            return
        await self._process_batch(messages)
        # Guarantee a yield after a batch too: a saturated stream would otherwise
        # let one consumer monopolise the loop and starve its co-resident roles.
        await asyncio.sleep(0)

    # -- reading ------------------------------------------------------------
    async def _read_new(self) -> list[StreamMessage]:
        """One blocking read for up to ``consumer_batch_size`` new messages."""
        try:
            response = await self.redis.xreadgroup(
                groupname=self.group,
                consumername=self.consumer_name,
                streams={self.stream: ">"},
                count=self.settings.consumer_batch_size,
                block=self.settings.consumer_block_ms,
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):
                # The stream was trimmed away entirely, or Redis was flushed.
                await ensure_group(self.stream, self.group, self.redis)
                return []
            raise
        return _flatten(response)

    async def _reclaim_pending(self) -> list[StreamMessage]:
        """Recover messages abandoned by a dead consumer; dead-letter the poison.

        Runs on a timer rather than every loop: ``XPENDING`` on every iteration
        would double the command count for no benefit.
        """
        now = time.monotonic()
        if now - self._last_claim_at < self.settings.consumer_claim_interval_seconds:
            return []
        self._last_claim_at = now
        if not self._claim_supported:
            return []

        try:
            pending = await self.redis.xpending_range(
                name=self.stream,
                groupname=self.group,
                min="-",
                max="+",
                count=self.settings.consumer_batch_size,
            )
        except ResponseError as exc:
            if "NOGROUP" in str(exc):
                await ensure_group(self.stream, self.group, self.redis)
                return []
            raise
        except Exception as exc:  # pragma: no cover - server without XPENDING range
            self._claim_supported = False
            log.warning("worker.xpending_unsupported", role=self.role, error=str(exc))
            return []

        if not pending:
            return []

        min_idle = self.settings.consumer_claim_min_idle_ms
        poison: list[tuple[str, int]] = []
        claimable: list[tuple[str, int]] = []
        for entry in pending:
            message_id = _text(entry.get("message_id"))
            delivered = int(entry.get("times_delivered") or 1)
            idle_ms = int(entry.get("time_since_delivered") or 0)
            if delivered > self.settings.max_delivery_attempts:
                poison.append((message_id, delivered))
            elif idle_ms >= min_idle:
                claimable.append((message_id, delivered))

        if poison:
            await self._dead_letter_ids(poison)

        if not claimable:
            return []

        claimed = await self.redis.xclaim(
            name=self.stream,
            groupname=self.group,
            consumername=self.consumer_name,
            min_idle_time=min_idle,
            message_ids=[mid for mid, _ in claimable],
        )
        counts = dict(claimable)
        messages = _flatten_entries(claimed)
        for message in messages:
            message.delivery_count = counts.get(message.message_id, 1)
        if messages:
            log.info("worker.reclaimed", role=self.role, count=len(messages))
        return messages

    # -- processing ---------------------------------------------------------
    async def _process_batch(self, messages: list[StreamMessage]) -> None:
        self.stats.batches += 1
        started = time.perf_counter()
        try:
            outcome = await self.process(messages)
        except PermanentError as exc:
            # The whole batch is unusable. Rare, but it must not loop.
            outcome = BatchOutcome(dead=[(m, type(exc).__name__, str(exc)) for m in messages])
        except Exception as exc:
            # Transient: leave everything pending so it is redelivered.
            self.stats.failed += len(messages)
            self.stats.last_error = repr(exc)
            log.exception("worker.batch_failed", role=self.role, count=len(messages))
            await self._sleep(1.0)
            return

        if outcome.dead:
            await self._dead_letter(outcome.dead)

        # Ack is deliberately outside the handler's try/except above.
        ack_ids = list(outcome.ack) + [m.message_id for m, _, _ in outcome.dead]
        if ack_ids:
            try:
                await self.redis.xack(self.stream, self.group, *ack_ids)
            except Exception:
                # Failing to ack is safe: the message is redelivered and the
                # handler's idempotent write makes that a no-op.
                log.exception("worker.ack_failed", role=self.role, count=len(ack_ids))

        self.stats.processed += len(outcome.ack)
        self.stats.failed += len(outcome.retry)
        if outcome.ack:
            self.stats.last_success_at = datetime.now(UTC)

        log.debug(
            "worker.batch",
            role=self.role,
            acked=len(outcome.ack),
            retried=len(outcome.retry),
            dead=len(outcome.dead),
            duration_ms=int((time.perf_counter() - started) * 1000),
        )

    @abstractmethod
    async def process(self, messages: list[StreamMessage]) -> BatchOutcome:
        """Handle one batch and say what to ack, retry, or dead-letter."""

    # -- helpers for subclasses --------------------------------------------
    def parse(self, messages: list[StreamMessage]) -> tuple[list[ParsedMessage], BatchOutcome]:
        """Parse a batch, routing unparseable payloads straight to dead letters.

        A malformed payload is permanent by definition: no number of retries
        turns invalid JSON into a valid event.
        """
        parsed: list[ParsedMessage] = []
        outcome = BatchOutcome()
        for message in messages:
            try:
                event = parse_event(message.payload())
            except (SchemaError, ValueError, TypeError) as exc:
                outcome.dead.append((message, type(exc).__name__, str(exc)[:500]))
                continue
            except Exception as exc:  # pydantic ValidationError and friends
                outcome.dead.append((message, type(exc).__name__, str(exc)[:500]))
                continue
            parsed.append(ParsedMessage(message=message, event=event))
        return parsed, outcome

    # -- dead letters -------------------------------------------------------
    async def _dead_letter(self, entries: list[tuple[StreamMessage, str, str]]) -> None:
        rows = []
        for message, error_type, error_message in entries:
            payload: dict[str, Any]
            try:
                payload = message.payload()
            except Exception:
                payload = {"raw_fields": message.fields}
            rows.append(
                {
                    "stream": self.stream,
                    "consumer_group": self.group,
                    "message_id": message.message_id,
                    "tenant_id": str(payload.get("tenant_id") or "")[:64] or None,
                    "trace_id": str(payload.get("trace_id") or "")[:64] or None,
                    "delivery_count": message.delivery_count,
                    "error_type": error_type[:120],
                    "error_message": error_message[:2000],
                    "payload": payload,
                }
            )
        await self._write_dead_letters(rows)

    async def _dead_letter_ids(self, poison: list[tuple[str, int]]) -> None:
        """Dead-letter messages that exhausted their retries, then ack them."""
        ids = [mid for mid, _ in poison]
        counts = dict(poison)
        # One bounded fetch per poison message, not one XRANGE spanning the
        # first to the last. Two poison ids either side of fifty thousand
        # healthy messages would otherwise pull all fifty thousand into memory
        # to look up two payloads. Dead-lettering is a rare path and `poison` is
        # bounded by the batch size, so N small reads is the right trade.
        by_id: dict[str, Any] = {}
        for message_id in ids:
            try:
                entries = await self.redis.xrange(
                    self.stream, min=message_id, max=message_id, count=1
                )
            except Exception:
                # A message trimmed away by MAXLEN has no payload left to
                # recover. Dead-letter it anyway -- losing the record of a
                # poison message is worse than losing its body.
                continue
            for entry_id, fields in entries:
                by_id[_text(entry_id)] = fields
        rows = []
        for message_id in ids:
            fields = {_text(k): _text(v) for k, v in (by_id.get(message_id) or {}).items()}
            try:
                payload = decode_event_fields(fields) if fields else {}
            except Exception:
                payload = {"raw_fields": fields}
            rows.append(
                {
                    "stream": self.stream,
                    "consumer_group": self.group,
                    "message_id": message_id,
                    "tenant_id": str(payload.get("tenant_id") or "")[:64] or None,
                    "trace_id": str(payload.get("trace_id") or "")[:64] or None,
                    "delivery_count": counts.get(message_id, 0),
                    "error_type": "MaxDeliveryAttemptsExceeded",
                    "error_message": f"redelivered {counts.get(message_id, 0)} times without an ack (limit {self.settings.max_delivery_attempts})",
                    "payload": payload,
                }
            )
        await self._write_dead_letters(rows)
        with contextlib.suppress(Exception):
            await self.redis.xack(self.stream, self.group, *ids)

    async def _write_dead_letters(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        from ..db import insert_ignore

        try:
            async with self.db.session() as session:
                await insert_ignore(
                    session,
                    table_of(DeadLetter),
                    rows,
                    ["stream", "consumer_group", "message_id"],
                )
        except Exception:
            log.exception("worker.dead_letter_write_failed", role=self.role, count=len(rows))
            return
        self.stats.dead_lettered += len(rows)
        for row in rows:
            log.warning(
                "worker.dead_letter",
                role=self.role,
                message_id=row["message_id"],
                trace_id=row.get("trace_id"),
                error_type=row["error_type"],
            )

    # -- heartbeat ----------------------------------------------------------
    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            await self._sleep(self.settings.worker_heartbeat_seconds)
            if self._stop.is_set():
                return
            await self._heartbeat("running")

    async def _heartbeat(self, status: str) -> None:
        """Publish liveness. Failing to write a heartbeat must not stop work."""
        now = datetime.now(UTC)
        try:
            async with self.db.session() as session:
                await upsert(
                    session,
                    table_of(WorkerHeartbeat),
                    {
                        "name": self.name,
                        "role": self.role,
                        "host": socket.gethostname()[:200],
                        "pid": os.getpid(),
                        "status": status,
                        "started_at": self._started_at,
                        "last_seen_at": now,
                        "last_success_at": self.stats.last_success_at,
                        "processed": self.stats.processed,
                        "failed": self.stats.failed,
                        "dead_lettered": self.stats.dead_lettered,
                        "detail": {
                            "batches": self.stats.batches,
                            "last_error": self.stats.last_error,
                            "stream": self.stream,
                            "group": self.group,
                        },
                    },
                    index_elements=["name"],
                    update_columns=[
                        "role",
                        "host",
                        "pid",
                        "status",
                        "last_seen_at",
                        "last_success_at",
                        "processed",
                        "failed",
                        "dead_lettered",
                        "detail",
                    ],
                )
        except Exception:
            log.debug("worker.heartbeat_failed", role=self.role, exc_info=True)


# --------------------------------------------------------------------------- #
# Signal handling
# --------------------------------------------------------------------------- #
def install_signal_handlers(stop: Any) -> None:
    """Route SIGTERM/SIGINT to ``stop()``.

    ``loop.add_signal_handler`` is the correct asyncio mechanism but is not
    implemented on Windows, so fall back to ``signal.signal`` there rather than
    leaving local development without a working Ctrl-C.
    """
    handled = (signal.SIGTERM, signal.SIGINT)
    try:
        loop = asyncio.get_running_loop()
        for sig in handled:
            loop.add_signal_handler(sig, stop)
        return
    except (NotImplementedError, RuntimeError, AttributeError):
        pass
    for sig in handled:
        with contextlib.suppress(ValueError, OSError, AttributeError):
            signal.signal(sig, lambda _s, _f: stop())


# --------------------------------------------------------------------------- #
# Reply normalisation
# --------------------------------------------------------------------------- #
def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return "" if value is None else str(value)


def _flatten(response: Any) -> list[StreamMessage]:
    """Normalise an ``XREADGROUP`` reply into ``StreamMessage``s."""
    messages: list[StreamMessage] = []
    for item in response or []:
        if not isinstance(item, (list, tuple)) or len(item) != 2:
            continue
        _stream_name, entries = item
        messages.extend(_flatten_entries(entries))
    return messages


def _flatten_entries(entries: Any) -> list[StreamMessage]:
    out: list[StreamMessage] = []
    for entry in entries or []:
        if not isinstance(entry, (list, tuple)) or len(entry) != 2:
            continue
        message_id, fields = entry
        if fields is None:
            # A message deleted from the stream while still pending.
            continue
        out.append(
            StreamMessage(
                message_id=_text(message_id),
                fields={_text(k): _text(v) for k, v in dict(fields).items()},
            )
        )
    return out


__all__ = [
    "BatchOutcome",
    "ConsumerStats",
    "ParsedMessage",
    "PermanentError",
    "StreamConsumer",
    "StreamMessage",
    "install_signal_handlers",
]
