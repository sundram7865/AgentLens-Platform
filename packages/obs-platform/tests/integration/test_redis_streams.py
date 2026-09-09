"""Real Redis Streams semantics.

fakeredis is close enough for the unit suite, but consumer-group recovery is
exactly the area where "close enough" hides bugs: XPENDING's delivery counter,
XCLAIM's idle-time filter, MAXLEN trimming and how a group distributes messages
across consumers are the mechanics the whole at-least-once story rests on.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select

from obs_platform.models import DeadLetter, Span
from obs_platform.redis_io import ensure_group, group_info, pending_summary, stream_length
from obs_platform.workers.storage_writer import StorageWriter
from obs_sdk.publisher import (
    RedisPublisherConfig,
    RedisStreamPublisher,
    encode_event,
)
from obs_sdk.schema import EventType, ObsEvent, SpanKind, SpanStatus, new_span_id, new_trace_id

pytestmark = pytest.mark.integration

STREAM = "obs:events"


def _event(trace_id: str | None = None) -> ObsEvent:
    now = datetime.now(UTC)
    return ObsEvent(
        trace_id=trace_id or new_trace_id(),
        tenant_id="org_1",
        span_id=new_span_id(),
        type=EventType.SPAN_END,
        kind=SpanKind.LLM,
        name="classify_ticket_step",
        status=SpanStatus.OK,
        started_at=now,
        ended_at=now,
        model="gemini-2.0-flash",
    )


@pytest.fixture
async def worker(settings, database, redis) -> StorageWriter:
    consumer = StorageWriter(
        settings=settings, redis=redis, database=database, consumer_name="itest:1"
    )
    await ensure_group(STREAM, consumer.group, redis)
    return consumer


class TestConsumerGroups:
    async def test_group_creation_is_idempotent(self, redis) -> None:
        await ensure_group(STREAM, "obs-storage", redis)
        await ensure_group(STREAM, "obs-storage", redis)  # BUSYGROUP swallowed
        groups = await group_info(STREAM, redis)
        assert [g["name"] for g in groups] == ["obs-storage"]

    async def test_mkstream_creates_the_stream_before_any_producer(self, redis) -> None:
        """Workers usually start before the first event exists."""
        assert await stream_length("obs:brand-new", redis) == 0
        await ensure_group("obs:brand-new", "obs-storage", redis)
        assert await redis.exists("obs:brand-new")

    async def test_three_groups_each_see_every_message(self, redis) -> None:
        for group in ("obs-storage", "obs-guardrail", "obs-eval"):
            await ensure_group(STREAM, group, redis)
        for _ in range(5):
            await redis.xadd(STREAM, encode_event(_event()))

        for group in ("obs-storage", "obs-guardrail", "obs-eval"):
            response = await redis.xreadgroup(
                groupname=group, consumername="c1", streams={STREAM: ">"}, count=10, block=10
            )
            delivered = sum(len(entries) for _, entries in response)
            assert delivered == 5, f"{group} saw {delivered} of 5"

    async def test_messages_are_split_across_consumers_in_a_group(self, redis) -> None:
        await ensure_group(STREAM, "obs-storage", redis)
        for _ in range(10):
            await redis.xadd(STREAM, encode_event(_event()))

        first = await redis.xreadgroup(
            groupname="obs-storage", consumername="a", streams={STREAM: ">"}, count=4, block=10
        )
        second = await redis.xreadgroup(
            groupname="obs-storage", consumername="b", streams={STREAM: ">"}, count=10, block=10
        )
        a_count = sum(len(entries) for _, entries in first)
        b_count = sum(len(entries) for _, entries in second)
        assert a_count == 4
        assert b_count == 6, "the second consumer must get only what the first did not"


class TestRecovery:
    async def test_xpending_reports_a_real_delivery_count(self, redis, worker) -> None:
        """The dead-letter ceiling depends on `times-delivered` being accurate."""
        message_id = await redis.xadd(STREAM, encode_event(_event()))
        # XCLAIM only touches messages already in the group's pending list, so
        # the message has to be delivered before it can be re-claimed.
        await redis.xreadgroup(
            groupname=worker.group, consumername="first", streams={STREAM: ">"}, count=10, block=10
        )
        for _ in range(3):
            await redis.xclaim(
                name=STREAM,
                groupname=worker.group,
                consumername="crashy",
                min_idle_time=0,
                message_ids=[message_id],
            )
        entries = await redis.xpending_range(
            name=STREAM, groupname=worker.group, min="-", max="+", count=10
        )
        assert entries
        assert int(entries[0]["times_delivered"]) >= 3

    async def test_an_abandoned_message_is_reclaimed_by_another_worker(
        self, settings, database, redis
    ) -> None:
        """A worker that dies mid-batch must not strand its messages."""
        settings.consumer_claim_min_idle_ms = 0
        settings.consumer_claim_interval_seconds = 0
        dead_worker = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="dying:1"
        )
        await ensure_group(STREAM, dead_worker.group, redis)

        trace_id = new_trace_id()
        await redis.xadd(STREAM, encode_event(_event(trace_id)))
        # Delivered but never acked, as if the process was killed here.
        await dead_worker._read_new()
        assert (await pending_summary(STREAM, dead_worker.group, redis))["pending"] == 1

        survivor = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="survivor:2"
        )
        survivor._last_claim_at = 0.0
        reclaimed = await survivor._reclaim_pending()
        assert reclaimed, "the survivor should have claimed the abandoned message"
        await survivor._process_batch(reclaimed)

        async with database.read_session() as session:
            count = (
                await session.execute(
                    select(func.count()).select_from(Span).where(Span.trace_id == trace_id)
                )
            ).scalar()
        assert count == 1
        assert (await pending_summary(STREAM, survivor.group, redis))["pending"] == 0

    async def test_poison_message_is_dead_lettered_after_the_real_ceiling(
        self, settings, database, redis, worker
    ) -> None:
        settings.max_delivery_attempts = 3
        settings.consumer_claim_min_idle_ms = 0
        settings.consumer_claim_interval_seconds = 0

        message_id = await redis.xadd(STREAM, encode_event(_event()))
        await redis.xreadgroup(
            groupname=worker.group, consumername="first", streams={STREAM: ">"}, count=10, block=10
        )
        for _ in range(5):
            await redis.xclaim(
                name=STREAM,
                groupname=worker.group,
                consumername="crashy",
                min_idle_time=0,
                message_ids=[message_id],
            )

        worker._last_claim_at = 0.0
        await worker._reclaim_pending()

        async with database.read_session() as session:
            dead = list((await session.execute(select(DeadLetter))).scalars())
        assert len(dead) == 1
        assert dead[0].error_type == "MaxDeliveryAttemptsExceeded"
        assert (await pending_summary(STREAM, worker.group, redis))["pending"] == 0

    async def test_lag_reporting_matches_reality(self, redis, worker) -> None:
        for _ in range(7):
            await redis.xadd(STREAM, encode_event(_event()))
        groups = {g["name"]: g for g in await group_info(STREAM, redis)}
        assert groups[worker.group]["lag"] == 7

        messages = await worker._read_new()
        await worker._process_batch(messages)

        groups = {g["name"]: g for g in await group_info(STREAM, redis)}
        assert groups[worker.group]["lag"] == 0
        assert groups[worker.group]["pending"] == 0


class TestTrimming:
    async def test_approximate_maxlen_bounds_the_stream(self, redis) -> None:
        """`MAXLEN ~` trims at a macro-node boundary, so the bound is loose."""
        for _ in range(500):
            await redis.xadd(STREAM, encode_event(_event()), maxlen=100, approximate=True)
        length = await stream_length(STREAM, redis)
        assert length >= 100, "approximate trimming must never drop below the target"
        assert length < 500, "but it must actually be trimming"

    async def test_exact_maxlen_is_exact(self, redis) -> None:
        for _ in range(200):
            await redis.xadd(STREAM, encode_event(_event()), maxlen=50, approximate=False)
        assert await stream_length(STREAM, redis) == 50


class TestSdkPublisherAgainstRealRedis:
    async def test_the_sdk_publishes_and_the_consumer_reads_it_back(
        self, settings, database, redis, worker
    ) -> None:
        """The real wire format, end to end, through the actual publisher."""
        publisher = RedisStreamPublisher(
            RedisPublisherConfig(
                url=settings.redis_url, stream=STREAM, flush_interval=0.05, batch_size=10
            )
        )
        trace_ids = [new_trace_id() for _ in range(5)]
        try:
            for trace_id in trace_ids:
                assert publisher.publish(_event(trace_id)) is True
            assert publisher.flush(timeout=10)
        finally:
            publisher.close(timeout=5)

        assert publisher.stats.published == 5
        assert publisher.stats.dropped_error == 0

        for _ in range(5):
            messages = await worker._read_new()
            if not messages:
                break
            await worker._process_batch(messages)

        async with database.read_session() as session:
            stored = {
                row
                for (row,) in (
                    await session.execute(select(Span.trace_id).where(Span.trace_id.in_(trace_ids)))
                ).all()
            }
        assert stored == set(trace_ids)

    async def test_publisher_survives_redis_going_away_mid_flight(self, settings, redis) -> None:
        """Pointed at a dead port, the caller must still never see an error."""
        publisher = RedisStreamPublisher(
            RedisPublisherConfig(
                url="redis://127.0.0.1:9",
                stream=STREAM,
                connect_timeout=0.2,
                socket_timeout=0.2,
                flush_interval=0.05,
            )
        )
        try:
            results = [publisher.publish(_event()) for _ in range(10)]
            await asyncio.sleep(0.5)
        finally:
            publisher.close(timeout=3)
        assert all(results), "publish() must accept the event regardless"
        assert publisher.stats.published == 0
        assert publisher.stats.dropped_error > 0
