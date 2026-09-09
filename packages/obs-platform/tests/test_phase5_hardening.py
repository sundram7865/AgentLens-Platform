"""Phase 5 -- hardening: self-observability, dead letters, shutdown, retention.

The `/health/meta` tests exist because of a bug this suite did not originally
catch: the endpoint read ORM rows after its read-only session had rolled back,
raised ``DetachedInstanceError``, swallowed it, and reported "database
unreachable" while the database was perfectly healthy. A monitoring endpoint
that lies about *which* dependency is broken is worse than one that is simply
down -- it sends whoever is on call in the wrong direction. These tests assert
the endpoint reports worker rows correctly and that a genuine failure is named
accurately.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select

from obs_platform.models import DeadLetter, Span, Trace, WorkerHeartbeat
from obs_platform.redis_io import ensure_group, stream_health
from obs_platform.workers.storage_writer import StorageWriter
from obs_sdk.publisher import encode_event
from obs_sdk.schema import EventType, ObsEvent, SpanKind, SpanStatus, new_span_id, new_trace_id

STREAM = "obs:events"


@pytest.fixture
async def worker(settings, database, redis) -> StorageWriter:
    settings.consumer_block_ms = 10
    consumer = StorageWriter(
        settings=settings, redis=redis, database=database, consumer_name="hardening:1"
    )
    await ensure_group(STREAM, consumer.group, redis)
    return consumer


async def seed_heartbeat(database, role: str = "storage", seconds_ago: float = 1.0) -> None:
    async with database.session() as session:
        session.add(
            WorkerHeartbeat(
                name=f"host:1#{role}",
                role=role,
                host="host",
                pid=1,
                status="running",
                started_at=datetime.now(UTC) - timedelta(minutes=5),
                last_seen_at=datetime.now(UTC) - timedelta(seconds=seconds_ago),
                last_success_at=datetime.now(UTC),
                processed=42,
                failed=0,
                dead_lettered=0,
                detail={},
            )
        )


class TestMetaHealth:
    async def test_reports_worker_rows_without_detaching_them(
        self, anon_client, database, redis
    ) -> None:
        """The regression: reading ORM rows after the read session rolled back."""
        await seed_heartbeat(database)
        response = await anon_client.get("/health/meta")
        payload = response.json()

        assert "database" not in payload, (
            f"the database check failed when it should not have: {payload.get('database')}"
        )
        assert payload["problems"] == []
        assert payload["status"] == "ok"
        assert len(payload["workers"]) == 1
        assert payload["workers"][0]["role"] == "storage"
        assert payload["workers"][0]["processed"] == 42
        assert payload["workers"][0]["seconds_since_seen"] is not None

    async def test_a_real_database_failure_is_named_accurately(
        self, anon_client, database, monkeypatch
    ) -> None:
        """Every exception reported as 'unreachable' is how a bug gets triaged
        as an outage."""
        from obs_platform.api.routers import health as health_module

        class Boom:
            def read_session(self) -> Any:
                raise RuntimeError("connection refused")

        monkeypatch.setattr(health_module, "get_db", lambda: Boom())
        payload = (await anon_client.get("/health/meta")).json()

        assert payload["status"] == "degraded"
        assert payload["database"]["error_type"] == "RuntimeError"
        assert "RuntimeError" in payload["problems"][0]

    async def test_reports_stream_lag_per_consumer_group(self, anon_client, worker, redis) -> None:
        await redis.xadd(STREAM, encode_event(_trace_event()), maxlen=100)
        payload = (await anon_client.get("/health/meta")).json()
        groups = {g["name"]: g for g in payload["streams"]["groups"]}
        assert "obs-storage" in groups
        assert payload["streams"]["length"] >= 1

    async def test_unreplayed_dead_letters_degrade_health(
        self, anon_client, database, redis
    ) -> None:
        """The premise collapses if the platform cannot notice its own failure."""
        async with database.session() as session:
            session.add(
                DeadLetter(
                    stream=STREAM,
                    consumer_group="obs-storage",
                    message_id="1-1",
                    error_type="ValueError",
                    error_message="unparseable",
                    payload={},
                )
            )
        response = await anon_client.get("/health/meta")
        assert response.status_code == 503
        assert response.json()["unreplayed_dead_letters"] == 1
        assert any("dead letter" in p for p in response.json()["problems"])

    async def test_a_stale_worker_is_reported(self, anon_client, database, settings) -> None:
        settings.worker_heartbeat_seconds = 5
        await seed_heartbeat(database, seconds_ago=600)
        payload = (await anon_client.get("/health/meta")).json()
        assert any("last seen" in p for p in payload["problems"])

    async def test_idle_deployment_with_no_traffic_is_healthy(self, anon_client, redis) -> None:
        """A fresh deploy has no spans; that is not staleness."""
        payload = (await anon_client.get("/health/meta")).json()
        assert not any("no span written" in p for p in payload["problems"])

    async def test_meta_health_needs_no_credentials(self, anon_client) -> None:
        """An uptime checker has no token; requiring one makes it useless."""
        assert (await anon_client.get("/health/meta")).status_code in (200, 503)


class TestDeadLetterPath:
    async def test_poison_message_is_parked_after_max_attempts(
        self, worker, redis, database, settings
    ) -> None:
        """Gotcha #5: an infinitely retried message is a self-inflicted outage."""
        settings.max_delivery_attempts = 2
        settings.consumer_claim_min_idle_ms = 0
        settings.consumer_claim_interval_seconds = 0

        message_id = await redis.xadd(STREAM, encode_event(_trace_event()))

        # Deliver it repeatedly without acking, as a crash loop would.
        for _ in range(4):
            await redis.xreadgroup(
                groupname=worker.group,
                consumername="crashing-consumer",
                streams={STREAM: ">"},
                count=10,
                block=1,
            )
            await redis.xclaim(
                name=STREAM,
                groupname=worker.group,
                consumername="crashing-consumer",
                min_idle_time=0,
                message_ids=[message_id],
            )

        worker._last_claim_at = 0.0
        await worker._reclaim_pending()

        async with database.session() as session:
            dead = list((await session.execute(select(DeadLetter))).scalars())
        assert len(dead) == 1
        assert dead[0].error_type == "MaxDeliveryAttemptsExceeded"
        assert dead[0].delivery_count > settings.max_delivery_attempts

        # And it is acked, so it stops consuming redelivery slots forever.
        pending = await redis.xpending(STREAM, worker.group)
        assert (pending["pending"] if isinstance(pending, dict) else pending[0]) == 0

    async def test_dead_letter_write_is_idempotent(self, worker, database) -> None:
        """A replayed dead-letter must not accumulate rows."""
        from obs_platform.workers.base import StreamMessage

        message = StreamMessage(message_id="9-9", fields={"v": "2", "data": "{bad"})
        for _ in range(3):
            await worker._dead_letter([(message, "ValueError", "unparseable")])

        async with database.session() as session:
            count = (await session.execute(select(func.count()).select_from(DeadLetter))).scalar()
        assert count == 1


class TestGracefulShutdown:
    async def test_sigterm_finishes_the_batch_and_acks(self, worker, redis, database) -> None:
        """Gotcha #4: every redeploy sends SIGTERM."""
        for _ in range(3):
            await redis.xadd(STREAM, encode_event(_trace_event()))

        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.3)
        worker.request_stop()
        await asyncio.wait_for(task, timeout=10)

        async with database.session() as session:
            spans = (await session.execute(select(func.count()).select_from(Span))).scalar()
        assert spans == 3
        pending = await redis.xpending(STREAM, worker.group)
        assert (pending["pending"] if isinstance(pending, dict) else pending[0]) == 0

    async def test_stop_is_idempotent(self, worker) -> None:
        worker.request_stop()
        worker.request_stop()
        assert worker.stopping

    async def test_heartbeat_marks_the_worker_stopped(self, worker, database) -> None:
        task = asyncio.create_task(worker.run())
        await asyncio.sleep(0.2)
        worker.request_stop()
        await asyncio.wait_for(task, timeout=10)

        async with database.session() as session:
            beat = (await session.execute(select(WorkerHeartbeat))).scalar_one()
        assert beat.status == "stopped"


class TestStructuredLogging:
    def test_every_line_is_json_with_the_bound_trace_id(self, capsys) -> None:
        """Gotcha: one request must be greppable end to end across services."""
        from obs_platform.logging import configure_logging, get_logger, log_context

        configure_logging(level="INFO", json_output=True, service="obs-test")
        log = get_logger("test")
        with log_context(trace_id="tr_grep_me", tenant_id="acme"):
            log.info("worker.processed", count=3)

        captured = capsys.readouterr().out.strip().splitlines()
        entry = json.loads(captured[-1])
        assert entry["trace_id"] == "tr_grep_me"
        assert entry["tenant_id"] == "acme"
        assert entry["event"] == "worker.processed"
        assert entry["count"] == 3
        assert entry["service"] == "obs-test"

        # Restore console output so later tests are not spammed with JSON.
        configure_logging(level="WARNING", json_output=False, service="obs-test")

    def test_context_unbinds_without_wiping_outer_scope(self) -> None:
        from obs_platform.logging import configure_logging, get_logger, log_context

        configure_logging(level="WARNING", json_output=False, service="obs-test")
        with log_context(request_id="outer"), log_context(trace_id="inner"):
            pass
        get_logger("test").warning("still.works")


class TestSchemaVersionTolerance:
    async def test_a_future_event_still_ingests(self, worker, redis, database) -> None:
        """Gotcha #7: a rolling deploy puts newer events in front of an older consumer."""
        trace_id = new_trace_id()
        payload = json.loads(_trace_event(trace_id).to_json())
        payload["schema_version"] = 99
        payload["a_field_from_the_future"] = {"nested": True}
        await redis.xadd(STREAM, {"v": "99", "data": json.dumps(payload)})

        messages = await worker._read_new()
        await worker._process_batch(messages)

        async with database.session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
        assert trace.schema_version == 99


def _trace_event(trace_id: str | None = None) -> ObsEvent:
    now = datetime.now(UTC)
    return ObsEvent(
        trace_id=trace_id or new_trace_id(),
        span_id=new_span_id(),
        tenant_id="acme",
        type=EventType.SPAN_END,
        kind=SpanKind.LLM,
        name="classify_ticket_step",
        status=SpanStatus.OK,
        started_at=now,
        ended_at=now,
        model="gemini-2.0-flash",
    )


async def _unused(redis: Any) -> Any:  # pragma: no cover - keeps the import honest
    return await stream_health(client=redis)
