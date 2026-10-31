"""The consumer groups must not deadlock each other on `traces`.

The storage writer and the guardrail scanner read the same stream in different
consumer groups, so at any moment they are writing *different* batches that
mention the *same* traces. Each used to lock those `traces` rows in its own
batch's arrival order -- a lock cycle -- and the paced load test hit
"deadlock detected" eight times in one run (docs/LOADTEST.md). Every victim
batch then sat pending until the reclaim loop got to it.

This drives both consumers concurrently over the same traces, presented in
opposite orders, and calls ``process()`` directly: no retry, no redelivery, so
a deadlock surfaces as an exception instead of being absorbed.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from obs_platform.workers.base import StreamMessage
from obs_platform.workers.guardrail_scanner import GuardrailScanner
from obs_platform.workers.storage_writer import StorageWriter
from obs_sdk.publisher import encode_event
from obs_sdk.schema import EventType, ObsEvent, SpanKind, SpanStatus, new_span_id, new_trace_id

pytestmark = pytest.mark.integration

TRACES = 60
ROUNDS = 3


def _batch(trace_ids: list[str], prefix: str) -> list[StreamMessage]:
    now = datetime.now(UTC)
    messages = []
    for index, trace_id in enumerate(trace_ids):
        event = ObsEvent(
            trace_id=trace_id,
            span_id=new_span_id(),
            tenant_id="acme",
            type=EventType.SPAN_END,
            kind=SpanKind.LLM,
            name="llm:generate_json",
            status=SpanStatus.OK,
            started_at=now,
            ended_at=now,
            input={"prompt": "card 4111 1111 1111 1111"},  # gives the scanner a finding to write
            output={"completion": "ok"},
        )
        messages.append(StreamMessage(f"{prefix}-{index}", encode_event(event)))
    return messages


async def test_storage_and_guardrail_do_not_deadlock_on_shared_traces(
    settings, database, redis
) -> None:
    storage = StorageWriter(settings=settings, redis=redis, database=database, consumer_name="s:1")
    scanner = GuardrailScanner(
        settings=settings, redis=redis, database=database, consumer_name="g:1"
    )

    for round_number in range(ROUNDS):
        trace_ids = [new_trace_id() for _ in range(TRACES)]
        forward = _batch(trace_ids, f"s{round_number}")
        backward = _batch(list(reversed(trace_ids)), f"g{round_number}")

        results = await asyncio.gather(
            storage.process(forward), scanner.process(backward), return_exceptions=True
        )
        errors = [r for r in results if isinstance(r, BaseException)]
        assert not errors, f"round {round_number}: {errors[0]!r}"
