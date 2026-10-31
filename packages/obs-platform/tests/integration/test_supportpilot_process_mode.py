"""SupportPilot's process mode, against a real Redis.

With ``AGENT_EXECUTION_MODE=process`` every agent run executes in a *spawned*
interpreter that exits when the run returns. Two things have to hold for its
spans to survive: the child must build its own publisher (nothing usable can
be handed to it across spawn), and that publisher must flush before the
interpreter exits. Neither is visible to an in-memory test, because the
in-memory publisher lives in the parent.
"""

from __future__ import annotations

import json
import multiprocessing
import uuid

import pytest
import supportpilot_replica as sp

from .conftest import REDIS_URL

pytestmark = pytest.mark.integration


def test_events_from_a_spawned_agent_process_reach_the_stream(
    integration_enabled: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    import redis as redis_sync

    stream = f"obs:events:spawn:{uuid.uuid4().hex[:8]}"
    client = redis_sync.Redis.from_url(REDIS_URL, decode_responses=True)
    client.delete(stream)

    # The child inherits the environment at spawn time, as SupportPilot's
    # process-mode children inherit the API's.
    monkeypatch.setenv("OBS_ENABLED", "true")
    monkeypatch.setenv("OBS_REDIS_URL", REDIS_URL)
    monkeypatch.setenv("OBS_STREAM", stream)

    org, ticket, run = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    state = {"organization_id": str(org), "ticket_id": str(ticket), "agent_run_id": str(run)}

    ctx = multiprocessing.get_context("spawn")
    queue = ctx.Queue()
    child = ctx.Process(target=sp.spawned_graph_entry, args=(state, queue))
    child.start()
    assert queue.get(timeout=60) == {"ok": True, "decision": "AUTO_REPLY_DRAFT"}
    child.join(timeout=30)
    assert child.exitcode == 0

    try:
        entries = client.xrange(stream)
        events = [json.loads(fields["data"]) for _, fields in entries]
    finally:
        client.delete(stream)
        client.close()

    expected_trace = "tr_" + run.hex
    assert events, "the child exited without flushing its publisher"
    assert {e["trace_id"] for e in events} == {expected_trace}
    assert {e["tenant_id"] for e in events} == {str(org)}
    names = {e["name"] for e in events if e["type"] == "span.end"}
    assert {
        "agent_step:load_ticket_context",
        "agent_step:classify_ticket",
        "llm:generate_json",
        "provider:GET:/orders/A-1001",
    } <= names
    llm = next(e for e in events if e["type"] == "span.end" and e["name"] == "llm:generate_json")
    assert llm["usage"]["total_tokens"] == 890
    assert llm["model"] == "gemini-3.5-flash-lite"
