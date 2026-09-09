"""Storage writer -- events in Redis become rows in Postgres.

Idempotency is the whole design here, because redelivery is not an edge case:
it happens on every worker restart, every deploy, and every transient database
error. Three rules make redelivery a no-op:

1. ``span.start`` inserts with **ON CONFLICT DO NOTHING**. A replayed start can
   never clobber a span that has already finished.
2. ``span.end`` inserts with **ON CONFLICT DO UPDATE**. The terminal state always
   wins, whatever order the two events arrive in.
3. Trace rollups (tokens, cost, span counts) are **recomputed from the spans
   table**, never incremented. An increment would double-count on redelivery;
   a recomputation converges to the same answer no matter how many times it runs.

Cost is applied here rather than in the SDK so that a vendor price change is a
config edit on this service instead of a redeploy of the agent being watched.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select, update

from obs_sdk.schema import EventType, ObsEvent, SpanKind, SpanStatus

from ..db import insert_ignore, table_of, upsert_many
from ..evals.sampling import should_sample
from ..logging import get_logger
from ..models import Span, Trace
from ..pricing import cost_micros
from .base import BatchOutcome, StreamConsumer, StreamMessage

log = get_logger("obs_platform.storage")

# Trace status precedence. A trace that errored stays errored even if a later
# event reports "ok" -- the failure is the fact worth keeping.
_STATUS_RANK = {"running": 0, "ok": 1, "error": 2}


def _as_utc(value: datetime | None) -> datetime | None:
    """Force a timestamp read back from the database to be timezone-aware.

    Every datetime this platform writes is UTC and aware. Not every database
    gives it back that way: Postgres ``timestamptz`` does, but SQLite has no
    timezone type at all and hands back a naive value however the column was
    declared. That difference is invisible until a trace is merged across two
    batches -- which is the normal case for anything longer than one poll, since
    ``trace.start`` and ``trace.end`` then arrive separately. The merge compares
    the incoming (aware) timestamp with the stored (naive) one and Python raises
    ``TypeError: can't compare offset-naive and offset-aware datetimes``, which
    fails the whole batch and, once the retries run out, dead-letters a
    perfectly good trace.
    """
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


_SPAN_END_COLUMNS = [
    "parent_span_id",
    "kind",
    "name",
    "status",
    "started_at",
    "ended_at",
    "latency_ms",
    "input",
    "output",
    "error",
    "attributes",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "cost_micros",
    "model",
    "schema_version",
]

_TRACE_UPDATE_COLUMNS = [
    "name",
    "service",
    "environment",
    "status",
    "started_at",
    "ended_at",
    "latency_ms",
    "input",
    "output",
    "attributes",
    "session_id",
    "user_ref",
    "model",
    "schema_version",
    "eval_status",
    "updated_at",
]


class StorageWriter(StreamConsumer):
    """Consumer group ``obs-storage``: the durable record of every trace."""

    role = "storage"

    async def process(self, messages: list[StreamMessage]) -> BatchOutcome:
        parsed, outcome = self.parse(messages)
        if not parsed:
            return outcome

        span_starts: dict[tuple[str, str], dict[str, Any]] = {}
        span_ends: dict[tuple[str, str], dict[str, Any]] = {}
        trace_events: dict[str, list[ObsEvent]] = {}

        for item in parsed:
            event = item.event
            trace_events.setdefault(event.trace_id, []).append(event)
            if event.span_id is None:
                continue
            key = (event.trace_id, event.span_id)
            row = _span_row(event)
            if event.type is EventType.SPAN_END:
                span_ends[key] = row
            elif event.type is EventType.SPAN_START and key not in span_ends:
                span_starts[key] = row

        # A span that both started and finished inside this batch only needs the
        # terminal row; inserting the start first would be two writes for one fact.
        for key in span_ends:
            span_starts.pop(key, None)

        async with self.db.session() as session:
            if span_starts:
                await insert_ignore(
                    session, table_of(Span), list(span_starts.values()), ["trace_id", "span_id"]
                )
            if span_ends:
                # One statement for the whole batch. Safe because span_ends is
                # keyed by (trace_id, span_id), so no row is touched twice --
                # which is the condition Postgres rejects. Measured: 292 -> 416
                # rows/s on the durable write path (docs/LOADTEST.md), i.e. ~1.4x.
                # Less than the round-trip count suggests, because at this batch
                # size the JSONB serialisation cost dominates the round trip.
                await upsert_many(
                    session,
                    table_of(Span),
                    list(span_ends.values()),
                    index_elements=["trace_id", "span_id"],
                    update_columns=_SPAN_END_COLUMNS,
                )

            await self._write_traces(session, trace_events)
            await self._recompute_rollups(session, list(trace_events.keys()))

        outcome.ack.extend(item.message_id for item in parsed)
        return outcome

    # ------------------------------------------------------------------ #
    async def _write_traces(self, session: Any, trace_events: dict[str, list[ObsEvent]]) -> None:
        """Merge each trace's events into its row, order-independently."""
        if not trace_events:
            return

        existing = {
            row.trace_id: row
            for row in (
                await session.execute(select(Trace).where(Trace.trace_id.in_(list(trace_events))))
            ).scalars()
        }

        rows = [
            _merge_trace(trace_id, events, existing.get(trace_id), self.settings.eval_sample_rate)
            for trace_id, events in trace_events.items()
        ]
        # One statement, not one per trace. Keyed by trace_id and unique by
        # construction, so no row is touched twice.
        await upsert_many(
            session,
            table_of(Trace),
            rows,
            index_elements=["trace_id"],
            update_columns=_TRACE_UPDATE_COLUMNS,
        )

    async def _recompute_rollups(self, session: Any, trace_ids: list[str]) -> None:
        """Recompute token/cost/count rollups from the spans table.

        Recomputed rather than incremented: an increment is not idempotent, and
        this consumer is guaranteed to see some messages more than once.

        One GROUP BY pass over the batch's spans, then one UPDATE per trace.
        A correlated-subquery form that collapses those UPDATEs into a single
        statement was tried and **measured 25% slower** (121.9 -> 91.4 rows/s,
        docs/LOADTEST.md): it trades eleven round trips for eight aggregate
        passes per trace instead of one shared pass. Round trips are not the
        bottleneck here; scanning spans repeatedly is.
        """
        if not trace_ids:
            return
        rows = (
            await session.execute(
                select(
                    Span.trace_id,
                    func.coalesce(func.sum(Span.prompt_tokens), 0),
                    func.coalesce(func.sum(Span.completion_tokens), 0),
                    func.coalesce(func.sum(Span.total_tokens), 0),
                    func.coalesce(func.sum(Span.cost_micros), 0),
                    func.count(),
                    func.coalesce(func.sum(_case_when(Span.kind == SpanKind.LLM.value)), 0),
                    func.coalesce(func.sum(_case_when(Span.kind == SpanKind.TOOL.value)), 0),
                    func.coalesce(func.sum(_case_when(Span.status == SpanStatus.ERROR.value)), 0),
                )
                .where(Span.trace_id.in_(trace_ids))
                .group_by(Span.trace_id)
            )
        ).all()

        for (
            trace_id,
            prompt_tokens,
            completion_tokens,
            total_tokens,
            cost,
            span_count,
            llm_calls,
            tool_calls,
            error_count,
        ) in rows:
            await session.execute(
                update(Trace)
                .where(Trace.trace_id == trace_id)
                .values(
                    prompt_tokens=int(prompt_tokens),
                    completion_tokens=int(completion_tokens),
                    total_tokens=int(total_tokens),
                    cost_micros=int(cost),
                    span_count=int(span_count),
                    llm_calls=int(llm_calls),
                    tool_calls=int(tool_calls),
                    error_count=int(error_count),
                )
            )


def _case_when(condition: Any) -> Any:
    from sqlalchemy import case

    return case((condition, 1), else_=0)


def _span_row(event: ObsEvent) -> dict[str, Any]:
    usage = event.usage
    prompt = usage.prompt_tokens if usage else 0
    completion = usage.completion_tokens if usage else 0
    total = usage.total_tokens if usage else 0
    return {
        "trace_id": event.trace_id,
        "span_id": event.span_id,
        "parent_span_id": event.parent_span_id,
        "tenant_id": event.tenant_id,
        "kind": event.kind.value,
        "name": event.name[:200],
        "status": event.status.value,
        "started_at": event.started_at,
        "ended_at": event.ended_at,
        "latency_ms": event.computed_latency_ms(),
        "input": event.input or {},
        "output": event.output or {},
        "error": event.error.model_dump(mode="json") if event.error else None,
        "attributes": event.attributes or {},
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": total,
        "cost_micros": cost_micros(event.model, prompt, completion),
        "model": event.model,
        "schema_version": event.schema_version,
        "created_at": datetime.now(UTC),
    }


def _merge_trace(
    trace_id: str,
    events: list[ObsEvent],
    current: Trace | None,
    sample_rate: int,
) -> dict[str, Any]:
    """Fold a batch of events (plus any stored row) into one trace row.

    Convergent by construction: timestamps take min/max, status only escalates,
    and payloads only overwrite when the incoming event actually carries one. Two
    replicas processing different halves of a trace therefore agree.
    """
    row: dict[str, Any] = {
        "trace_id": trace_id,
        "tenant_id": current.tenant_id if current else "default",
        "name": current.name if current else "",
        "service": current.service if current else "unknown",
        "environment": current.environment if current else "local",
        "status": current.status if current else "running",
        "started_at": _as_utc(current.started_at) if current else None,
        "ended_at": _as_utc(current.ended_at) if current else None,
        "latency_ms": current.latency_ms if current else None,
        "input": current.input if current else {},
        "output": current.output if current else {},
        "attributes": dict(current.attributes) if current else {},
        "session_id": current.session_id if current else None,
        "user_ref": current.user_ref if current else None,
        "model": current.model if current else None,
        "schema_version": current.schema_version if current else 2,
        "eval_status": current.eval_status if current else "not_sampled",
        # Set here rather than left to the server default: SQLite's
        # CURRENT_TIMESTAMP has one-second resolution, so a burst of traces would
        # share a timestamp and the keyset cursor would have only the id
        # tiebreak to work with. Python gives microseconds on every backend.
        "created_at": _as_utc(current.created_at) if current else datetime.now(UTC),
    }

    saw_trace_end = False
    for event in sorted(events, key=lambda e: e.emitted_at):
        row["tenant_id"] = event.tenant_id
        row["schema_version"] = max(row["schema_version"], event.schema_version)
        if event.service and event.service != "unknown":
            row["service"] = event.service
        if event.environment:
            row["environment"] = event.environment
        if event.session_id:
            row["session_id"] = event.session_id
        if event.user_ref:
            row["user_ref"] = event.user_ref
        if event.model and not row["model"]:
            row["model"] = event.model

        is_trace_event = event.type in (EventType.TRACE_START, EventType.TRACE_END)
        if (is_trace_event and event.name) or (not row["name"] and event.name):
            row["name"] = event.name[:200]

        if event.started_at and (row["started_at"] is None or event.started_at < row["started_at"]):
            row["started_at"] = event.started_at
        if event.ended_at and (row["ended_at"] is None or event.ended_at > row["ended_at"]):
            row["ended_at"] = event.ended_at

        if event.attributes:
            row["attributes"].update(event.attributes)

        if event.type is EventType.TRACE_START and event.input:
            row["input"] = event.input
        elif not row["input"] and event.input:
            # No trace envelope yet: the first span carrying an input is the
            # best available stand-in for the request. Accepting span.END as
            # well as span.START matters -- a completed span emits both, but a
            # producer that only reports finished work emits only the end, and
            # restricting this to START left those traces with an empty input.
            # The eval scorer then found no question and silently skipped them.
            row["input"] = event.input

        if event.type is EventType.TRACE_END:
            saw_trace_end = True
            if event.output:
                row["output"] = event.output
            if event.latency_ms is not None:
                row["latency_ms"] = event.latency_ms
            if _STATUS_RANK.get(event.status.value, 0) >= _STATUS_RANK.get(row["status"], 0):
                row["status"] = event.status.value
        elif event.status is SpanStatus.ERROR and row["status"] != "error":
            # A failing span means a failing trace, even if the envelope is lost.
            row["status"] = "error"

    if saw_trace_end:
        if row["latency_ms"] is None and row["started_at"] and row["ended_at"]:
            row["latency_ms"] = max(
                0, int((row["ended_at"] - row["started_at"]).total_seconds() * 1000)
            )
        if row["status"] == "running":
            row["status"] = "ok"
        # The sampling decision is taken once, when the trace completes, and is
        # a pure function of the trace id -- so a redelivered trace.end reaches
        # the same answer instead of flipping the trace in or out of the sample.
        if row["eval_status"] in ("not_sampled", "pending"):
            row["eval_status"] = (
                "pending" if should_sample(trace_id, sample_rate) else "not_sampled"
            )

    row["updated_at"] = datetime.now(UTC)
    return row


__all__ = ["StorageWriter"]
