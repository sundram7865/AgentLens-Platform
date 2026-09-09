"""Phase 1 -- the ingestion path.

The three tests the build plan calls for, plus the guarantees they depend on:

(a) the callback handler, driven by a mocked LangChain callback manager -- no
    LLM call, no network,
(b) the storage-writer consumer against a real (fake)redis stream,
(c) end to end: publish a synthetic trace, assert the row lands with the right
    shape,

and then the properties that actually break in production: redelivery must not
duplicate, a poison message must not loop forever, a Redis outage must not
surface in the caller, and two concurrent traces must not braid together.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select

from obs_platform.models import DeadLetter, Span, Trace
from obs_platform.redis_io import ensure_group
from obs_platform.workers.storage_writer import StorageWriter
from obs_sdk import InMemoryPublisher, ObservabilityCallbackHandler, Tracer
from obs_sdk.publisher import (
    RedisPublisherConfig,
    RedisStreamPublisher,
    encode_event,
    publisher_from_env,
)
from obs_sdk.schema import (
    EventType,
    ObsEvent,
    SpanKind,
    SpanStatus,
    Usage,
    new_span_id,
    new_trace_id,
)

STREAM = "obs:events"


# --------------------------------------------------------------------------- #
# Fake LangChain objects -- enough surface for the handler, nothing more
# --------------------------------------------------------------------------- #
class FakeMessage:
    def __init__(self, usage: dict | None = None, model: str | None = None) -> None:
        self.usage_metadata = usage
        self.response_metadata = {"model_name": model} if model else {}
        self.content = "response text"
        self.type = "ai"


class FakeGeneration:
    def __init__(self, text: str = "", usage: dict | None = None, model: str | None = None):
        self.text = text
        self.message = FakeMessage(usage, model)
        self.generation_info = {}


class FakeLLMResult:
    def __init__(self, text: str = "ok", usage: dict | None = None, model: str | None = None):
        self.generations = [[FakeGeneration(text, usage, model)]]
        self.llm_output = {}


class FakeDocument:
    def __init__(self, content: str, metadata: dict | None = None) -> None:
        self.page_content = content
        self.metadata = metadata or {}


# --------------------------------------------------------------------------- #
# (a) Callback handler
# --------------------------------------------------------------------------- #
class TestCallbackHandler:
    def test_emits_a_connected_span_tree(self, publisher: InMemoryPublisher) -> None:
        handler = ObservabilityCallbackHandler(
            publisher, tenant_id="org_1", service="supportpilot", trace_id="tr_fixed"
        )
        root, node, llm = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

        handler.on_chain_start({"name": "LangGraph"}, {"ticket_id": "T-1"}, run_id=root)
        handler.on_chain_start(
            {"name": "step"},
            {},
            run_id=node,
            parent_run_id=root,
            metadata={"langgraph_node": "classify_ticket_step"},
        )
        handler.on_llm_start(
            {"name": "ChatGoogleGenerativeAI"}, ["classify"], run_id=llm, parent_run_id=node
        )
        handler.on_llm_end(
            FakeLLMResult(usage={"input_tokens": 100, "output_tokens": 20}), run_id=llm
        )
        handler.on_chain_end({"category": "REFUND_REQUEST"}, run_id=node)
        handler.on_chain_end({"decision": "AUTO_REPLY_DRAFT"}, run_id=root)

        events = publisher.events
        assert events[0].type.value == "trace.start"
        assert events[-1].type.value == "trace.end"
        assert {e.trace_id for e in events} == {"tr_fixed"}

        spans = {e.span_id: e for e in events if e.type is EventType.SPAN_END}
        llm_span = next(s for s in spans.values() if s.kind is SpanKind.LLM)
        node_span = next(s for s in spans.values() if s.name == "classify_ticket_step")
        root_span = next(s for s in spans.values() if s.name == "LangGraph")
        # This is the OpenTelemetry-shaped guarantee: every span knows its parent.
        assert llm_span.parent_span_id == node_span.span_id
        assert node_span.parent_span_id == root_span.span_id
        assert root_span.parent_span_id is None

    def test_langgraph_node_name_wins_over_class_name(self, publisher) -> None:
        handler = ObservabilityCallbackHandler(publisher)
        root, node = uuid.uuid4(), uuid.uuid4()
        handler.on_chain_start({"name": "LangGraph"}, {}, run_id=root)
        handler.on_chain_start(
            {"name": "RunnableSequence"},
            {},
            run_id=node,
            parent_run_id=root,
            metadata={"langgraph_node": "detect_risk_step"},
        )
        assert any(e.name == "detect_risk_step" for e in publisher.events)

    def test_plumbing_chains_are_skipped_without_orphaning_children(self, publisher) -> None:
        """A skipped span must re-parent its children, not disconnect them."""
        handler = ObservabilityCallbackHandler(publisher)
        root, noise, llm = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        handler.on_chain_start({"name": "LangGraph"}, {}, run_id=root)
        handler.on_chain_start({"name": "ChannelWrite"}, {}, run_id=noise, parent_run_id=root)
        handler.on_llm_start({"name": "llm"}, ["hi"], run_id=llm, parent_run_id=noise)
        handler.on_llm_end(FakeLLMResult(), run_id=llm)

        assert not [e for e in publisher.events if e.name == "ChannelWrite"]
        root_span = next(e for e in publisher.events if e.name == "LangGraph" and e.span_id)
        llm_span = next(e for e in publisher.events if e.name == "llm")
        assert llm_span.parent_span_id == root_span.span_id

    def test_llm_error_still_produces_a_span(self, publisher) -> None:
        """Without on_llm_error the failing run -- the one you need -- is invisible."""
        handler = ObservabilityCallbackHandler(publisher)
        root, llm = uuid.uuid4(), uuid.uuid4()
        handler.on_chain_start({"name": "graph"}, {}, run_id=root)
        handler.on_llm_start({"name": "llm"}, ["hi"], run_id=llm, parent_run_id=root)
        handler.on_llm_error(RuntimeError("gemini 503"), run_id=llm)
        handler.on_chain_error(RuntimeError("gemini 503"), run_id=root)

        errored = [e for e in publisher.events if e.status is SpanStatus.ERROR]
        assert errored, "a failed LLM call produced no error span"
        assert errored[0].error is not None
        assert "gemini 503" in errored[0].error.message
        trace_end = next(e for e in publisher.events if e.type is EventType.TRACE_END)
        assert trace_end.status is SpanStatus.ERROR

    def test_tool_error_is_captured(self, publisher) -> None:
        handler = ObservabilityCallbackHandler(publisher)
        root, tool = uuid.uuid4(), uuid.uuid4()
        handler.on_chain_start({"name": "graph"}, {}, run_id=root)
        handler.on_tool_start(
            {"name": "urbankart_request_refund"},
            '{"order_id": "A-1"}',
            run_id=tool,
            parent_run_id=root,
        )
        handler.on_tool_error(ValueError("provider rejected refund"), run_id=tool)
        handler.on_chain_end({}, run_id=root)

        tool_span = next(
            e for e in publisher.events if e.kind is SpanKind.TOOL and e.type is EventType.SPAN_END
        )
        assert tool_span.status is SpanStatus.ERROR
        assert tool_span.error is not None
        assert "provider rejected refund" in tool_span.error.message

    @pytest.mark.parametrize(
        ("usage", "expected"),
        [
            ({"input_tokens": 10, "output_tokens": 5}, 15),  # Anthropic / LC core
            ({"prompt_tokens": 10, "completion_tokens": 5}, 15),  # OpenAI
            ({"prompt_token_count": 10, "candidates_token_count": 5}, 15),  # Gemini
        ],
    )
    def test_token_usage_across_provider_spellings(self, publisher, usage, expected) -> None:
        """SupportPilot runs Gemini with an optional Groq path; guessing one
        vendor's spelling would zero out the cost chart on the others."""
        handler = ObservabilityCallbackHandler(publisher)
        run = uuid.uuid4()
        handler.on_llm_start({"name": "llm"}, ["hi"], run_id=run)
        handler.on_llm_end(FakeLLMResult(usage=usage), run_id=run)
        span = next(e for e in publisher.events if e.type is EventType.SPAN_END)
        assert span.usage is not None and span.usage.total_tokens == expected

    def test_retriever_documents_are_captured_for_faithfulness_scoring(self, publisher) -> None:
        handler = ObservabilityCallbackHandler(publisher)
        run = uuid.uuid4()
        handler.on_retriever_start({"name": "pgvector"}, "refund policy", run_id=run)
        handler.on_retriever_end(
            [FakeDocument("Refunds within 30 days.", {"doc": "policy"})], run_id=run
        )
        span = next(
            e
            for e in publisher.events
            if e.kind is SpanKind.RETRIEVER and e.type is EventType.SPAN_END
        )
        assert span.output["document_count"] == 1
        assert "Refunds within 30 days." in span.output["documents"][0]["content"]

    def test_concurrent_runs_do_not_braid_traces(self, publisher) -> None:
        """The reason trace identity comes from run_id lineage, not handler state."""
        handler = ObservabilityCallbackHandler(publisher)
        a_root, b_root = uuid.uuid4(), uuid.uuid4()
        a_llm, b_llm = uuid.uuid4(), uuid.uuid4()

        handler.on_chain_start({"name": "a"}, {}, run_id=a_root, metadata={"trace_id": "tr_a"})
        handler.on_chain_start({"name": "b"}, {}, run_id=b_root, metadata={"trace_id": "tr_b"})
        handler.on_llm_start({"name": "llm"}, ["a"], run_id=a_llm, parent_run_id=a_root)
        handler.on_llm_start({"name": "llm"}, ["b"], run_id=b_llm, parent_run_id=b_root)
        handler.on_llm_end(FakeLLMResult(), run_id=b_llm)
        handler.on_llm_end(FakeLLMResult(), run_id=a_llm)
        handler.on_chain_end({}, run_id=b_root)
        handler.on_chain_end({}, run_id=a_root)

        by_trace: dict[str, set[str]] = {}
        for event in publisher.events:
            by_trace.setdefault(event.trace_id, set()).add(event.name)
        assert by_trace["tr_a"] == {"a", "llm"}
        assert by_trace["tr_b"] == {"b", "llm"}
        assert handler.stats()["open_runs"] == 0

    def test_a_handler_bug_never_reaches_the_agent(self, publisher) -> None:
        """Rule 1: observability degrades, the customer's ticket completes."""

        class Exploding(InMemoryPublisher):
            def publish(self, event):  # type: ignore[override]
                raise RuntimeError("publisher exploded")

        handler = ObservabilityCallbackHandler(Exploding())
        run = uuid.uuid4()
        handler.on_llm_start({"name": "llm"}, ["hi"], run_id=run)  # must not raise
        handler.on_llm_end(FakeLLMResult(), run_id=run)
        assert handler.stats()["internal_errors"] > 0

    def test_end_without_start_is_ignored(self, publisher) -> None:
        handler = ObservabilityCallbackHandler(publisher)
        handler.on_llm_end(FakeLLMResult(), run_id=uuid.uuid4())
        assert publisher.events == []

    def test_run_table_is_bounded(self, publisher) -> None:
        """A graph that crashes without closing its runs must not leak memory."""
        handler = ObservabilityCallbackHandler(publisher, max_tracked_runs=10)
        for _ in range(50):
            handler.on_chain_start({"name": "leak"}, {}, run_id=uuid.uuid4())
        assert handler.stats()["open_runs"] <= 10


# --------------------------------------------------------------------------- #
# Publisher resilience
# --------------------------------------------------------------------------- #
class TestPublisherResilience:
    def test_redis_down_never_raises_into_the_caller(self) -> None:
        """The ticket must complete even when the observability stack is gone."""
        publisher = RedisStreamPublisher(
            RedisPublisherConfig(
                url="redis://127.0.0.1:9",  # discard port: connection always refused
                connect_timeout=0.2,
                socket_timeout=0.2,
                flush_interval=0.05,
            )
        )
        try:
            for _ in range(20):
                assert (
                    publisher.publish(ObsEvent(trace_id=new_trace_id(), type=EventType.SPAN_END))
                    is True
                )
            publisher.flush(timeout=2.0)
        finally:
            publisher.close(timeout=2.0)
        assert publisher.stats.published == 0
        assert publisher.stats.dropped_error > 0

    def test_full_queue_drops_rather_than_blocking(self) -> None:
        publisher = RedisStreamPublisher(
            RedisPublisherConfig(url="redis://127.0.0.1:9", queue_size=5, connect_timeout=0.1)
        )
        try:
            results = [
                publisher.publish(ObsEvent(trace_id="t", type=EventType.SPAN_END))
                for _ in range(200)
            ]
        finally:
            publisher.close(timeout=1.0)
        assert results.count(False) > 0, "a bounded queue must shed load, not grow"

    def test_disabled_by_config_is_a_no_op(self) -> None:
        publisher = publisher_from_env({"OBS_ENABLED": "false", "OBS_REDIS_URL": "redis://x"})
        assert publisher.publish(ObsEvent(trace_id="t", type=EventType.SPAN_END)) is False

    def test_missing_url_disables_rather_than_crashing(self) -> None:
        """An app can wire the SDK unconditionally and still run with no infra."""
        assert (
            publisher_from_env({}).publish(ObsEvent(trace_id="t", type=EventType.SPAN_END)) is False
        )


# --------------------------------------------------------------------------- #
# (b) + (c) Storage writer against a real stream
# --------------------------------------------------------------------------- #
async def _publish(redis: Any, events: list[ObsEvent]) -> list[str]:
    ids = []
    for event in events:
        ids.append(await redis.xadd(STREAM, encode_event(event), maxlen=1000, approximate=True))
    return ids


def _agent_trace(trace_id: str, tenant: str = "org_1") -> list[ObsEvent]:
    """A trace shaped like one SupportPilot agent run."""
    now = datetime.now(UTC)
    root_span, llm_span, tool_span = new_span_id(), new_span_id(), new_span_id()
    common = {
        "trace_id": trace_id,
        "tenant_id": tenant,
        "service": "supportpilot",
        "environment": "test",
    }
    return [
        ObsEvent(
            **common,
            type=EventType.TRACE_START,
            kind=SpanKind.AGENT,
            name="agent_run",
            started_at=now,
            input={"question": "Where is my order A-1?"},
            attributes={"ticket_id": "T-77", "category": "ORDER_STATUS"},
        ),
        ObsEvent(
            **common,
            type=EventType.SPAN_END,
            span_id=root_span,
            kind=SpanKind.AGENT,
            name="agent_run",
            status=SpanStatus.OK,
            started_at=now,
            ended_at=now + timedelta(milliseconds=900),
        ),
        ObsEvent(
            **common,
            type=EventType.SPAN_END,
            span_id=llm_span,
            parent_span_id=root_span,
            kind=SpanKind.LLM,
            name="classify_ticket_step",
            status=SpanStatus.OK,
            started_at=now,
            ended_at=now + timedelta(milliseconds=400),
            model="gemini-2.0-flash",
            usage=Usage(prompt_tokens=1000, completion_tokens=200),
            output={"completion": "ORDER_STATUS"},
        ),
        ObsEvent(
            **common,
            type=EventType.SPAN_END,
            span_id=tool_span,
            parent_span_id=root_span,
            kind=SpanKind.TOOL,
            name="urbankart_get_order_context",
            status=SpanStatus.OK,
            started_at=now,
            ended_at=now + timedelta(milliseconds=120),
        ),
        ObsEvent(
            **common,
            type=EventType.TRACE_END,
            kind=SpanKind.AGENT,
            name="agent_run",
            status=SpanStatus.OK,
            ended_at=now + timedelta(milliseconds=950),
            latency_ms=950,
            output={"answer": "Your order ships tomorrow."},
            attributes={"decision": "AUTO_REPLY_DRAFT"},
        ),
    ]


async def _drain(writer: StorageWriter, rounds: int = 3) -> None:
    """Run the consumer's read/process cycle a bounded number of times."""
    for _ in range(rounds):
        messages = await writer._read_new()
        if not messages:
            break
        await writer._process_batch(messages)


@pytest.fixture
async def writer(settings, database, redis) -> StorageWriter:
    settings.consumer_block_ms = 10
    consumer = StorageWriter(
        settings=settings, redis=redis, database=database, consumer_name="test:1"
    )
    await ensure_group(STREAM, consumer.group, redis)
    return consumer


class TestStorageWriter:
    async def test_a_published_trace_lands_with_the_right_shape(self, writer, redis, database):
        trace_id = new_trace_id()
        await _publish(redis, _agent_trace(trace_id))
        await _drain(writer)

        async with database.session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
            spans = list(
                (await session.execute(select(Span).where(Span.trace_id == trace_id))).scalars()
            )

        assert trace.tenant_id == "org_1"
        assert trace.service == "supportpilot"
        assert trace.status == "ok"
        assert trace.latency_ms == 950
        assert trace.input["question"] == "Where is my order A-1?"
        assert trace.output["answer"] == "Your order ships tomorrow."
        assert trace.attributes["ticket_id"] == "T-77"
        assert trace.attributes["decision"] == "AUTO_REPLY_DRAFT"
        # Rollups are recomputed from spans, not trusted from the producer.
        assert trace.total_tokens == 1200
        assert trace.span_count == 3
        assert trace.llm_calls == 1
        assert trace.tool_calls == 1
        assert trace.error_count == 0
        # gemini-2.0-flash at $0.10/$0.40 per 1M -> 1000*0.10 + 200*0.40 = 180 micros
        assert trace.cost_micros == 180
        assert len(spans) == 3
        assert {s.name for s in spans} == {
            "agent_run",
            "classify_ticket_step",
            "urbankart_get_order_context",
        }

    async def test_redelivery_creates_no_duplicates(self, writer, redis, database):
        """Gotcha #2. A crashed-and-restarted consumer WILL see these again."""
        trace_id = new_trace_id()
        message_ids = await _publish(redis, _agent_trace(trace_id))
        await _drain(writer)

        async with database.session() as session:
            first = (
                await session.execute(
                    select(func.count()).select_from(Span).where(Span.trace_id == trace_id)
                )
            ).scalar()

        # Simulate the crash: the work was done but the ACK never happened, so
        # Redis hands the same messages back on restart.
        claimed = await redis.xclaim(
            name=STREAM,
            groupname=writer.group,
            consumername="test:2",
            min_idle_time=0,
            message_ids=message_ids,
        )
        from obs_platform.workers.base import _flatten_entries

        await writer._process_batch(_flatten_entries(claimed))

        async with database.session() as session:
            second = (
                await session.execute(
                    select(func.count()).select_from(Span).where(Span.trace_id == trace_id)
                )
            ).scalar()
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()

        assert first == second == 3, "redelivery duplicated span rows"
        assert trace.total_tokens == 1200, "rollups double-counted on redelivery"

    async def test_span_end_wins_over_a_replayed_span_start(self, writer, redis, database):
        """Order independence: a late start must not revert a finished span."""
        trace_id, span_id = new_trace_id(), new_span_id()
        now = datetime.now(UTC)
        start = ObsEvent(
            trace_id=trace_id,
            span_id=span_id,
            type=EventType.SPAN_START,
            kind=SpanKind.LLM,
            name="call",
            status=SpanStatus.RUNNING,
            started_at=now,
        )
        end = ObsEvent(
            trace_id=trace_id,
            span_id=span_id,
            type=EventType.SPAN_END,
            kind=SpanKind.LLM,
            name="call",
            status=SpanStatus.OK,
            started_at=now,
            ended_at=now + timedelta(milliseconds=50),
            usage=Usage(prompt_tokens=7),
        )
        await _publish(redis, [end])
        await _drain(writer)
        await _publish(redis, [start])
        await _drain(writer)

        async with database.session() as session:
            span = (await session.execute(select(Span).where(Span.span_id == span_id))).scalar_one()
        assert span.status == "ok"
        assert span.prompt_tokens == 7

    async def test_error_span_marks_the_trace_failed(self, writer, redis, database):
        trace_id = new_trace_id()
        now = datetime.now(UTC)
        await _publish(
            redis,
            [
                ObsEvent(trace_id=trace_id, type=EventType.TRACE_START, started_at=now),
                ObsEvent(
                    trace_id=trace_id,
                    span_id=new_span_id(),
                    type=EventType.SPAN_END,
                    kind=SpanKind.TOOL,
                    name="refund",
                    status=SpanStatus.ERROR,
                    error={"type": "HTTPError", "message": "provider 500"},
                    started_at=now,
                    ended_at=now,
                ),
                ObsEvent(
                    trace_id=trace_id,
                    type=EventType.TRACE_END,
                    status=SpanStatus.ERROR,
                    ended_at=now,
                    latency_ms=10,
                ),
            ],
        )
        await _drain(writer)

        async with database.session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
        assert trace.status == "error"
        assert trace.error_count == 1

    async def test_malformed_payload_is_dead_lettered_not_retried(self, writer, redis, database):
        """Gotcha #5. Invalid JSON never becomes valid; retrying it 5 times is waste."""
        await redis.xadd(STREAM, {"v": "2", "data": "{not json"})
        await redis.xadd(STREAM, {"v": "2", "data": json.dumps({"no_trace_id": True})})
        await _drain(writer)

        async with database.session() as session:
            dead = list((await session.execute(select(DeadLetter))).scalars())
        assert len(dead) == 2
        assert all(d.consumer_group == "obs-storage" for d in dead)
        # Dead-lettered messages are acked, so they stop being redelivered.
        pending = await redis.xpending(STREAM, writer.group)
        assert (pending["pending"] if isinstance(pending, dict) else pending[0]) == 0

    async def test_prehistoric_schema_version_is_dead_lettered(self, writer, redis, database):
        await redis.xadd(
            STREAM,
            {
                "v": "0",
                "data": json.dumps({"schema_version": 0, "trace_id": "t", "type": "span.end"}),
            },
        )
        await _drain(writer)
        async with database.session() as session:
            dead = list((await session.execute(select(DeadLetter))).scalars())
        assert len(dead) == 1
        assert "SchemaTooOld" in dead[0].error_type

    async def test_v1_events_still_ingest(self, writer, redis, database):
        """A rolling deploy leaves old-format events in the stream."""
        trace_id = new_trace_id()
        await redis.xadd(
            STREAM,
            {
                "v": "1",
                "data": json.dumps(
                    {
                        "schema_version": 1,
                        "trace_id": trace_id,
                        "span_id": "sp_v1",
                        "tenant_id": "org_1",
                        "event_type": "span.end",
                        "kind": "llm",
                        "node": "legacy_step",
                        "status": "ok",
                        "prompt_tokens": 40,
                        "completion_tokens": 10,
                    }
                ),
            },
        )
        await _drain(writer)
        async with database.session() as session:
            span = (await session.execute(select(Span).where(Span.span_id == "sp_v1"))).scalar_one()
        assert span.name == "legacy_step"
        assert span.total_tokens == 50

    async def test_deterministic_sampling_is_recorded_on_completion(
        self, writer, redis, database, settings
    ):
        settings.eval_sample_rate = 100
        trace_id = new_trace_id()
        await _publish(redis, _agent_trace(trace_id))
        await _drain(writer)
        async with database.session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
        assert trace.eval_status == "pending"

    async def test_heartbeat_is_published(self, writer, database):
        await writer._heartbeat("running")
        from obs_platform.models import WorkerHeartbeat

        async with database.session() as session:
            beat = (await session.execute(select(WorkerHeartbeat))).scalar_one()
        assert beat.role == "storage"
        assert beat.status == "running"

    async def test_graceful_stop_finishes_the_in_flight_batch(self, writer, redis, database):
        """Gotcha #4: SIGTERM on every redeploy must not lose the current batch."""
        trace_id = new_trace_id()
        await _publish(redis, _agent_trace(trace_id))
        task = asyncio.create_task(writer.run())
        await asyncio.sleep(0.25)
        writer.request_stop()
        await asyncio.wait_for(task, timeout=10)

        async with database.session() as session:
            count = (
                await session.execute(
                    select(func.count()).select_from(Span).where(Span.trace_id == trace_id)
                )
            ).scalar()
        assert count == 3
        pending = await redis.xpending(STREAM, writer.group)
        assert (pending["pending"] if isinstance(pending, dict) else pending[0]) == 0


# --------------------------------------------------------------------------- #
# Trace read API
# --------------------------------------------------------------------------- #
class TestTraceApi:
    async def test_list_and_detail(self, client, writer, redis):
        trace_id = new_trace_id()
        await _publish(redis, _agent_trace(trace_id))
        await _drain(writer)

        listing = await client.get("/v1/traces")
        assert listing.status_code == 200
        body = listing.json()
        assert body["items"][0]["trace_id"] == trace_id
        assert body["items"][0]["usage"]["total_tokens"] == 1200
        assert body["items"][0]["usage"]["cost_usd"] == 0.00018

        detail = await client.get(f"/v1/traces/{trace_id}")
        assert detail.status_code == 200
        payload = detail.json()
        assert len(payload["spans"]) == 3
        # The call tree survives the round trip.
        depths = {s["name"]: s["depth"] for s in payload["spans"]}
        assert depths["agent_run"] == 0
        assert depths["classify_ticket_step"] == 1

    async def test_pagination_is_keyset_and_stable(self, client, writer, redis):
        """Gotcha #8. Fine in the demo, dies on real data -- unless it is paged."""
        for _ in range(7):
            await _publish(redis, _agent_trace(new_trace_id()))
        await _drain(writer, rounds=10)

        seen: list[str] = []
        cursor = None
        for _ in range(10):
            url = f"/v1/traces?limit=3{f'&cursor={cursor}' if cursor else ''}"
            page = (await client.get(url)).json()
            seen.extend(item["trace_id"] for item in page["items"])
            cursor = page["next_cursor"]
            if not cursor:
                break
        assert len(seen) == 7
        assert len(set(seen)) == 7, "keyset pagination repeated a row"

    async def test_bad_cursor_is_a_400_not_a_500(self, client):
        assert (await client.get("/v1/traces?cursor=!!!not-base64!!!")).status_code == 400

    async def test_filters(self, client, writer, redis):
        ok_trace, error_trace = new_trace_id(), new_trace_id()
        await _publish(redis, _agent_trace(ok_trace))
        now = datetime.now(UTC)
        await _publish(
            redis,
            [
                ObsEvent(
                    trace_id=error_trace,
                    tenant_id="org_2",
                    type=EventType.TRACE_START,
                    started_at=now,
                ),
                ObsEvent(
                    trace_id=error_trace,
                    tenant_id="org_2",
                    type=EventType.TRACE_END,
                    status=SpanStatus.ERROR,
                    ended_at=now,
                    latency_ms=5,
                ),
            ],
        )
        await _drain(writer, rounds=5)

        assert [
            t["trace_id"] for t in (await client.get("/v1/traces?status=error")).json()["items"]
        ] == [error_trace]
        assert [
            t["trace_id"] for t in (await client.get("/v1/traces?tenant_id=org_1")).json()["items"]
        ] == [ok_trace]
        assert [
            t["trace_id"] for t in (await client.get("/v1/traces?ticket_id=T-77")).json()["items"]
        ] == [ok_trace]

        tenants = {t["tenant_id"] for t in (await client.get("/v1/traces/tenants")).json()}
        assert tenants == {"org_1", "org_2"}

    async def test_unknown_trace_is_404(self, client):
        assert (await client.get("/v1/traces/tr_missing")).status_code == 404


# --------------------------------------------------------------------------- #
# Manual tracer (SupportPilot's non-LangChain code paths)
# --------------------------------------------------------------------------- #
class TestTracer:
    def test_spans_nest_and_join_the_same_trace(self, publisher):
        tracer = Tracer(publisher, tenant_id="org_1", service="supportpilot")
        # Nesting is deliberate here: parent/child span linking is what is under test.
        with tracer.trace("ticket", trace_id="tr_manual", attributes={"ticket_id": "T-9"}):  # noqa: SIM117
            with tracer.span("tool_gateway", kind=SpanKind.TOOL) as outer:
                outer.set_input(tool="urbankart_request_refund")
                with tracer.span("provider_call", kind=SpanKind.OTHER) as inner:
                    inner.set_output(status=200)

        spans = {e.name: e for e in publisher.events if e.type is EventType.SPAN_END}
        assert spans["provider_call"].parent_span_id == spans["tool_gateway"].span_id
        assert {e.trace_id for e in publisher.events} == {"tr_manual"}

    def test_a_raising_block_still_closes_the_trace(self, publisher):
        tracer = Tracer(publisher, service="supportpilot")
        with pytest.raises(ValueError), tracer.trace("ticket"), tracer.span("boom"):
            raise ValueError("provider down")

        trace_end = next(e for e in publisher.events if e.type is EventType.TRACE_END)
        assert trace_end.status is SpanStatus.ERROR
        span_end = next(e for e in publisher.events if e.type is EventType.SPAN_END)
        assert span_end.error is not None and "provider down" in span_end.error.message

    def test_capture_content_off_keeps_shape_but_not_text(self, publisher):
        tracer = Tracer(publisher, capture_content=False)
        with tracer.trace("ticket"), tracer.span("step", input={"email": "a@b.com"}):
            pass  # the span body is irrelevant; only the emitted payload matters
        span = next(e for e in publisher.events if e.type is EventType.SPAN_START and e.span_id)
        assert span.input["__redacted_by_producer__"] is True
        assert "a@b.com" not in json.dumps(span.input)
