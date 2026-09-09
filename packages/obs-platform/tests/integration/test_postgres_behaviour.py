"""Postgres-specific behaviour the SQLite unit suite cannot verify.

Everything here is a place where "it passed on SQLite" is not evidence.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select, text

from obs_platform.db import insert_ignore, table_of, upsert_many
from obs_platform.evals.budget import record_spend
from obs_platform.models import EvalScore, Span, TenantUsage, Trace
from obs_platform.redis_io import ensure_group
from obs_platform.workers.storage_writer import StorageWriter
from obs_sdk.publisher import encode_event
from obs_sdk.schema import (
    EventType,
    ObsEvent,
    SpanKind,
    SpanStatus,
    Usage,
    new_span_id,
    new_trace_id,
)

pytestmark = pytest.mark.integration

STREAM = "obs:events"


def _span_row(trace_id: str, span_id: str, **overrides: object) -> dict:
    now = datetime.now(UTC)
    row = {
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": None,
        "tenant_id": "org_1",
        "kind": "llm",
        "name": "classify",
        "status": "ok",
        "started_at": now,
        "ended_at": now,
        "latency_ms": 100,
        "input": {"prompt": "hello"},
        "output": {"completion": "ORDER_STATUS"},
        "error": None,
        "attributes": {"ticket_id": "T-1"},
        "prompt_tokens": 100,
        "completion_tokens": 20,
        "total_tokens": 120,
        "cost_micros": 30,
        "model": "gemini-2.0-flash",
        "schema_version": 2,
        "created_at": now,
    }
    row.update(overrides)
    return row


class TestOnConflict:
    async def test_insert_ignore_against_a_real_unique_index(self, database) -> None:
        trace_id, span_id = new_trace_id(), new_span_id()
        async with database.session() as session:
            first = await insert_ignore(
                session, table_of(Span), [_span_row(trace_id, span_id)], ["trace_id", "span_id"]
            )
        async with database.session() as session:
            second = await insert_ignore(
                session,
                table_of(Span),
                [_span_row(trace_id, span_id, name="different")],
                ["trace_id", "span_id"],
            )
        assert first == 1
        assert second == 0, "the second insert must be ignored, not applied"

        async with database.session() as session:
            span = (await session.execute(select(Span).where(Span.span_id == span_id))).scalar_one()
        assert span.name == "classify"

    async def test_upsert_many_rejects_duplicate_conflict_keys(self, database) -> None:
        """Postgres raises when one statement touches the same row twice.

        This is exactly why the storage writer deduplicates by (trace_id,
        span_id) before batching, and it is a constraint SQLite does not
        enforce -- so only this test can prove the dedupe is load-bearing.
        """
        trace_id, span_id = new_trace_id(), new_span_id()
        rows = [_span_row(trace_id, span_id), _span_row(trace_id, span_id, name="dupe")]

        with pytest.raises(Exception) as excinfo:
            async with database.session() as session:
                await upsert_many(
                    session,
                    table_of(Span),
                    rows,
                    index_elements=["trace_id", "span_id"],
                    update_columns=["name"],
                )
        assert (
            "second time" in str(excinfo.value).lower()
            or "cardinality" in str(excinfo.value).lower()
        )

    async def test_atomic_increment_under_real_concurrency(self, database) -> None:
        """SQLite serialises writers; Postgres does not.

        The budget cap depends on `SET x = x + excluded.x` being atomic. With a
        read-modify-write it would lose updates here and the cap would
        under-count exactly when traffic is highest.
        """

        async def spend() -> None:
            async with database.session() as session:
                await record_spend(session, "org_conc", 10, 5, cost_micros=100)

        await asyncio.gather(*(spend() for _ in range(25)))

        async with database.session() as session:
            row = (
                await session.execute(
                    select(TenantUsage).where(
                        TenantUsage.tenant_id == "org_conc", TenantUsage.period_kind == "day"
                    )
                )
            ).scalar_one()
        assert row.calls == 25, f"lost updates: {row.calls} of 25"
        assert row.cost_micros == 2500
        assert row.total_tokens == 25 * 15


class TestJsonb:
    async def test_payloads_round_trip_through_jsonb(self, database) -> None:
        trace_id = new_trace_id()
        payload = {
            "question": "Where is my order?",
            "nested": {"list": [1, 2, {"deep": True}], "unicode": "ऑर्डर 🚚"},
            "empty": {},
            "null": None,
        }
        async with database.session() as session:
            session.add(
                Trace(
                    trace_id=trace_id,
                    tenant_id="org_1",
                    name="t",
                    input=payload,
                    output={},
                    attributes={},
                    guardrail_flags={},
                )
            )
        async with database.session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
        assert trace.input == payload

    async def test_json_path_filter_works_on_jsonb(self, database, redis) -> None:
        """The ticket_id filter renders as ->> on Postgres, json_extract on SQLite."""
        async with database.session() as session:
            for index, ticket in enumerate(["T-77", "T-88"]):
                session.add(
                    Trace(
                        trace_id=f"tr_json_{index}",
                        tenant_id="org_1",
                        name="t",
                        input={},
                        output={},
                        attributes={"ticket_id": ticket},
                        guardrail_flags={},
                    )
                )
        async with database.read_session() as session:
            rows = list(
                (
                    await session.execute(
                        select(Trace).where(Trace.attributes["ticket_id"].as_string() == "T-88")
                    )
                ).scalars()
            )
        assert [r.trace_id for r in rows] == ["tr_json_1"]

    async def test_jsonb_column_type_is_actually_jsonb(self, database) -> None:
        async with database.engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_name = 'traces' AND column_name = 'input'"
                )
            )
            assert result.scalar() == "jsonb"


class TestPercentiles:
    async def test_percentile_cont_is_used_and_correct(self, client, database) -> None:
        """SQLite has no percentile_cont, so only this test exercises that branch."""
        now = datetime.now(UTC)
        async with database.session() as session:
            for index, latency in enumerate([100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]):
                session.add(
                    Trace(
                        trace_id=f"tr_p{index}",
                        tenant_id="org_1",
                        name="t",
                        status="ok",
                        latency_ms=latency,
                        started_at=now,
                        ended_at=now,
                        input={},
                        output={},
                        attributes={},
                        guardrail_flags={},
                        created_at=now,
                    )
                )

        body = (await client.get("/v1/metrics/overview?hours=24")).json()
        assert body["traces"] == 10
        # percentile_cont interpolates: p50 of 100..1000 is 550.
        assert body["p50_latency_ms"] == pytest.approx(550, abs=1)
        assert body["p95_latency_ms"] == pytest.approx(955, abs=5)

    async def test_percentiles_ignore_null_latencies(self, client, database) -> None:
        now = datetime.now(UTC)
        async with database.session() as session:
            for index, latency in enumerate([None, 200, None, 400]):
                session.add(
                    Trace(
                        trace_id=f"tr_null{index}",
                        tenant_id="org_1",
                        name="t",
                        latency_ms=latency,
                        input={},
                        output={},
                        attributes={},
                        guardrail_flags={},
                        created_at=now,
                    )
                )
        body = (await client.get("/v1/metrics/overview?hours=24")).json()
        assert body["p50_latency_ms"] is not None
        assert 200 <= body["p50_latency_ms"] <= 400


class TestConcurrentConsumers:
    async def test_two_storage_writers_share_one_group_without_duplicating(
        self, settings, database, redis
    ) -> None:
        """The horizontal-scaling claim, actually exercised.

        Two workers, one consumer group, distinct `hostname:pid` names. Redis
        splits the messages between them; the unique constraint plus ON CONFLICT
        makes any overlap a no-op.
        """
        worker_a = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="hostA:1"
        )
        worker_b = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="hostB:2"
        )
        await ensure_group(STREAM, worker_a.group, redis)

        trace_ids = [new_trace_id() for _ in range(20)]
        now = datetime.now(UTC)
        for trace_id in trace_ids:
            for _ in range(3):
                await redis.xadd(
                    STREAM,
                    encode_event(
                        ObsEvent(
                            trace_id=trace_id,
                            tenant_id="org_1",
                            span_id=new_span_id(),
                            type=EventType.SPAN_END,
                            kind=SpanKind.LLM,
                            name="step",
                            status=SpanStatus.OK,
                            started_at=now,
                            ended_at=now,
                            model="gemini-2.0-flash",
                            usage=Usage(prompt_tokens=100, completion_tokens=10),
                        )
                    ),
                )

        async def drain(worker: StorageWriter) -> None:
            for _ in range(10):
                messages = await worker._read_new()
                if not messages:
                    break
                await worker._process_batch(messages)

        await asyncio.gather(drain(worker_a), drain(worker_b))

        async with database.read_session() as session:
            spans = (await session.execute(select(func.count()).select_from(Span))).scalar()
            traces = (await session.execute(select(func.count()).select_from(Trace))).scalar()
        assert spans == 60, f"expected 60 spans exactly, got {spans}"
        assert traces == 20

        pending = await redis.xpending(STREAM, worker_a.group)
        count = pending["pending"] if isinstance(pending, dict) else pending[0]
        assert count == 0, "both workers must have acked their share"

    async def test_rollups_are_correct_after_concurrent_writes(
        self, settings, database, redis
    ) -> None:
        """Recomputed rollups must converge regardless of who wrote what."""
        worker_a = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="hostA:1"
        )
        worker_b = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="hostB:2"
        )
        await ensure_group(STREAM, worker_a.group, redis)

        trace_id = new_trace_id()
        now = datetime.now(UTC)
        for _ in range(10):
            await redis.xadd(
                STREAM,
                encode_event(
                    ObsEvent(
                        trace_id=trace_id,
                        tenant_id="org_1",
                        span_id=new_span_id(),
                        type=EventType.SPAN_END,
                        kind=SpanKind.LLM,
                        name="step",
                        status=SpanStatus.OK,
                        started_at=now,
                        ended_at=now,
                        model="gemini-2.0-flash",
                        usage=Usage(prompt_tokens=100, completion_tokens=10),
                    )
                ),
            )

        async def drain(worker: StorageWriter) -> None:
            for _ in range(10):
                messages = await worker._read_new()
                if not messages:
                    break
                await worker._process_batch(messages)

        await asyncio.gather(drain(worker_a), drain(worker_b))

        async with database.read_session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
        assert trace.span_count == 10
        assert trace.total_tokens == 1100
        assert trace.llm_calls == 10


class TestRetentionOnPostgres:
    async def test_bulk_delete_removes_children_first(self, settings, database) -> None:
        from obs_platform.jobs import retention

        settings.retention_days = 1
        settings.retention_batch_size = 25
        old = datetime.now(UTC) - timedelta(days=10)

        async with database.session() as session:
            for index in range(60):
                trace_id = f"tr_old_{index}"
                session.add(
                    Trace(
                        trace_id=trace_id,
                        tenant_id="org_1",
                        name="t",
                        input={},
                        output={},
                        attributes={},
                        guardrail_flags={},
                        created_at=old,
                    )
                )
                session.add(
                    Span(
                        trace_id=trace_id,
                        span_id=f"sp_{index}",
                        tenant_id="org_1",
                        kind="llm",
                        name="s",
                        status="ok",
                        input={},
                        output={},
                        attributes={},
                        created_at=old,
                    )
                )
                session.add(
                    EvalScore(
                        trace_id=trace_id,
                        tenant_id="org_1",
                        metric="faithfulness",
                        score=0.9,
                        created_at=old,
                    )
                )

        report = await retention.purge(database, settings)
        assert report.deleted["traces"] == 60
        assert report.deleted["spans"] == 60
        assert report.deleted["eval_scores"] == 60
        assert report.batches >= 3

        async with database.read_session() as session:
            assert (await session.execute(select(func.count()).select_from(Span))).scalar() == 0
