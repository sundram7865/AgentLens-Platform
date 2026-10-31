"""The SupportPilot drop-in module, driven through SupportPilot's real control flow.

``integrations/supportpilot/observability.py`` is copied into another
repository, so this suite is the only CI it gets. Every test runs it through
``supportpilot_replica``: SupportPilot's own ``trace_operation``,
``run_logged_step``, Gemini client, UrbanKart client and graph executor, cut
down to the control flow and annotated with where each piece comes from.

The properties under test are the ones SupportPilot's real execution model
makes hard: the graph runs on a different thread from the request (or in a
spawned process), Gemini is not a LangChain model, and a run can pause for a
human and resume later.
"""

from __future__ import annotations

import multiprocessing
import pickle
import threading
import uuid
from typing import Any

import pytest
import supportpilot_replica as sp

from obs_sdk import InMemoryPublisher, Observability, SpanKind
from obs_sdk.schema import EventType, ObsEvent


@pytest.fixture
def obs(monkeypatch: pytest.MonkeyPatch) -> Any:
    module = sp.load_integration()
    publisher = InMemoryPublisher()
    monkeypatch.setattr(module, "_obs", Observability(publisher=publisher, service="supportpilot"))
    module.captured = publisher  # type: ignore[attr-defined]
    return module


def _ids() -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    return uuid.uuid4(), uuid.uuid4(), uuid.uuid4()


def _ends(events: list[ObsEvent]) -> dict[str, ObsEvent]:
    return {e.name: e for e in events if e.type is EventType.SPAN_END}


class TestOneRunIsOneTrace:
    def test_every_event_lands_in_the_run_trace_across_the_executor_thread(self, obs) -> None:
        """The graph runs on a ThreadPoolExecutor thread, which copies no context."""
        org, ticket, run = _ids()
        sp.run_ticket_agent(org, ticket, run)

        events = obs.captured.events
        assert {e.trace_id for e in events} == {obs.agent_trace_id(run)}
        assert {e.tenant_id for e in events} == {str(org)}
        types = [e.type for e in events]
        assert types[0] is EventType.TRACE_START and types[-1] is EventType.TRACE_END

    def test_steps_llm_calls_and_provider_calls_are_all_recorded(self, obs) -> None:
        org, ticket, run = _ids()
        sp.run_ticket_agent(org, ticket, run)
        ends = _ends(obs.captured.events)

        steps = [n for n in ends if n.startswith("agent_step:")]
        assert steps == [
            "agent_step:load_ticket_context",
            "agent_step:retrieve_knowledge",
            "agent_step:classify_ticket",
            "agent_step:execute_read_only_tools",
            "agent_step:draft_response",
            "agent_step:decision",
        ]
        assert all(ends[n].kind is SpanKind.CHAIN for n in steps)
        assert ends["llm:generate_json"].kind is SpanKind.LLM
        assert ends["provider:GET:/orders/A-1001"].kind is SpanKind.TOOL

    def test_llm_span_is_nested_in_its_step_and_carries_model_and_tokens(self, obs) -> None:
        """The whole reason for hooking trace_operation: Gemini is not LangChain."""
        sp.run_ticket_agent(*_ids())
        ends = _ends(obs.captured.events)
        llm, step = ends["llm:generate_json"], ends["agent_step:classify_ticket"]

        assert llm.parent_span_id == step.span_id
        assert llm.model == "gemini-3.5-flash-lite"
        assert llm.usage is not None
        assert (llm.usage.prompt_tokens, llm.usage.completion_tokens) == (850, 40)
        assert llm.usage.total_tokens == 890

    def test_a_fallback_model_is_the_one_recorded(self, obs) -> None:
        """SupportPilot walks a model chain on quota errors; cost must follow the model that answered."""
        sp.run_ticket_agent(*_ids(), extra_state={"_fail_first_model": True})
        llm = _ends(obs.captured.events)["llm:generate_json"]
        assert llm.model == "gemini-3.1-flash-lite"
        assert llm.usage is not None and llm.usage.total_tokens == 890

    def test_provider_call_inside_asyncio_run_joins_its_step(self, obs) -> None:
        sp.run_ticket_agent(*_ids())
        ends = _ends(obs.captured.events)
        provider = ends["provider:GET:/orders/A-1001"]
        assert provider.parent_span_id == ends["agent_step:execute_read_only_tools"].span_id
        assert provider.output["status"] == "DELIVERED"

    def test_provider_call_from_a_running_loop_still_joins(self, obs) -> None:
        """The resume-after-approval path: _run_async_from_sync falls back to a raw thread."""
        import asyncio

        org, ticket, run = _ids()
        captured: dict[str, Any] = {}

        async def approval_route() -> None:
            # An async route calling down into sync agent code, as approve_approval does.
            captured["state"] = sp.run_graph(
                {"organization_id": str(org), "ticket_id": str(ticket), "agent_run_id": str(run)}
            )

        asyncio.run(approval_route())
        provider = _ends(obs.captured.events)["provider:GET:/orders/A-1001"]
        assert provider.trace_id == obs.agent_trace_id(run)


class TestWhatTheEvalScorerNeeds:
    def test_trace_end_carries_the_question_and_the_answer(self, obs) -> None:
        sp.run_ticket_agent(*_ids())
        end = next(e for e in obs.captured.events if e.type is EventType.TRACE_END)
        assert end.input["question"].startswith("Refund for order A-1001")
        assert "My blender arrived broken" in end.input["question"]
        assert end.output["answer"] == sp.DRAFT
        assert end.output["decision"] == "AUTO_REPLY_DRAFT"
        assert end.attributes["detected_category"] == "REFUND_REQUEST"

    def test_retrieval_step_exposes_documents_for_faithfulness(self, obs) -> None:
        sp.run_ticket_agent(*_ids())
        retrieve = _ends(obs.captured.events)["agent_step:retrieve_knowledge"]
        assert retrieve.output == {
            "documents": [
                {
                    "content": sp.KNOWLEDGE[0]["content"],
                    "title": "Refund policy",
                    "score": 0.91,
                }
            ]
        }

    def test_step_outputs_keep_only_what_the_step_produced(self, obs) -> None:
        """run_logged_step returns the whole state; storing it per step is N copies of the ticket."""
        sp.run_ticket_agent(*_ids())
        ends = _ends(obs.captured.events)
        assert ends["agent_step:classify_ticket"].output == {
            "detected_category": "REFUND_REQUEST",
            "detected_priority": "HIGH",
        }
        assert set(ends["agent_step:draft_response"].output) == {"draft_response", "draft_attempts"}
        # The ticket body is recorded once, on the envelope, not on step inputs.
        assert ends["agent_step:retrieve_knowledge"].input == {"ticket_id": "T-1001"}

    async def test_the_platform_builds_a_complete_eval_sample(self, obs, settings, database, redis):
        """End to end on the platform side: storage writer, then the scorer's sample builder."""
        from sqlalchemy import select

        from obs_platform.models import Span, Trace
        from obs_platform.redis_io import ensure_group
        from obs_platform.workers.eval_scorer import build_sample
        from obs_platform.workers.storage_writer import StorageWriter
        from obs_sdk.publisher import encode_event

        org, ticket, run = _ids()
        sp.run_ticket_agent(org, ticket, run)

        settings.consumer_block_ms = 10
        writer = StorageWriter(
            settings=settings, redis=redis, database=database, consumer_name="t:1"
        )
        await ensure_group(settings.stream, writer.group, redis)
        for event in obs.captured.events:
            await redis.xadd(settings.stream, encode_event(event))
        for _ in range(3):
            messages = await writer._read_new()
            if not messages:
                break
            await writer._process_batch(messages)

        trace_id = obs.agent_trace_id(run)
        async with database.session() as session:
            trace = (
                await session.execute(select(Trace).where(Trace.trace_id == trace_id))
            ).scalar_one()
            spans = list(
                (await session.execute(select(Span).where(Span.trace_id == trace_id))).scalars()
            )

        assert trace.tenant_id == str(org)
        assert trace.status == "ok"
        assert trace.llm_calls == 1 and trace.tool_calls == 1
        assert trace.total_tokens == 890
        # gemini-3.5-flash-lite at $0.30 in / $2.50 out per 1M tokens:
        # 850 * 0.30 + 40 * 2.50 = 255 + 100 = 355 micro-dollars.
        assert trace.cost_micros == 355
        sample = build_sample(trace, spans)
        assert sample.question.startswith("Refund for order A-1001")
        assert sample.answer == sp.DRAFT
        assert sample.contexts == [sp.KNOWLEDGE[0]["content"]]
        assert sample.scorable


class TestSupportPilotPricing:
    """SupportPilot's model chain, priced from Google's published table."""

    @pytest.mark.parametrize(
        ("model", "expected_micros"),
        [
            ("gemini-3.5-flash-lite", 850 * 0.30 + 40 * 2.50),
            ("gemini-3.1-flash-lite", 850 * 0.25 + 40 * 1.50),
            ("gemini-3.6-flash", 850 * 0.75 + 40 * 3.75),
            ("gemini-3.8-flash", 850 * 0.75 + 40 * 3.75),
            # A provider prefix must not change the price.
            ("models/gemini-3.5-flash-lite", 850 * 0.30 + 40 * 2.50),
        ],
    )
    def test_chain_models_are_priced(self, model: str, expected_micros: float) -> None:
        from obs_platform.pricing import cost_micros, reset_pricing_cache

        reset_pricing_cache()
        assert cost_micros(model, 850, 40) == round(expected_micros)

    def test_an_unlisted_chain_model_is_reported_not_silently_free(self) -> None:
        from obs_platform.pricing import cost_micros, reset_pricing_cache, unknown_models

        reset_pricing_cache()
        assert cost_micros("gemini-3.7-flash", 850, 40) == 0
        assert unknown_models() == {"gemini-3.7-flash": 1}


class TestRunLifecycle:
    def test_a_failing_step_marks_the_step_and_the_run_as_errors(self, obs) -> None:
        with pytest.raises(RuntimeError, match="classify_ticket exploded"):
            sp.run_ticket_agent(*_ids(), fail_at="classify_ticket")
        events = obs.captured.events
        step = _ends(events)["agent_step:classify_ticket"]
        assert step.status.value == "error" and step.error is not None
        trace_end = next(e for e in events if e.type is EventType.TRACE_END)
        assert trace_end.status.value == "error"

    def test_a_paused_run_and_its_resume_share_one_trace(self, obs) -> None:
        """agent_run_id is stable across the approval pause, so the trace id is too."""
        org, ticket, run = _ids()
        sp.run_ticket_agent(org, ticket, run, pause=True)
        sp.run_ticket_agent(org, ticket, run, phase="resume")

        events = obs.captured.events
        assert {e.trace_id for e in events} == {obs.agent_trace_id(run)}
        ends = [e for e in events if e.type is EventType.TRACE_END]
        assert ends[0].output["status"] == "waiting_for_approval"
        assert [e.attributes["phase"] for e in ends] == ["run", "resume"]

    def test_concurrent_runs_for_two_organizations_do_not_braid(self, obs) -> None:
        runs = [_ids() for _ in range(6)]
        threads = [threading.Thread(target=sp.run_ticket_agent, args=ids) for ids in runs]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        by_trace: dict[str, set[str]] = {}
        for event in obs.captured.events:
            by_trace.setdefault(event.trace_id, set()).add(event.tenant_id)
        assert set(by_trace) == {obs.agent_trace_id(run) for _, _, run in runs}
        for org, _, run in runs:
            assert by_trace[obs.agent_trace_id(run)] == {str(org)}

    def test_an_operation_outside_any_run_emits_nothing(self, obs) -> None:
        """Knowledge ingestion and friends: no run, no tenant, so no orphan trace."""
        import asyncio

        asyncio.run(sp.urbankart_request("GET", "/health"))
        sp.LLMClient().generate_json("standalone")
        assert obs.captured.events == []

    def test_the_trace_id_is_a_pure_function_of_the_run_id(self, obs) -> None:
        run = uuid.uuid4()
        assert obs.agent_trace_id(run) == obs.agent_trace_id(str(run)) == "tr_" + run.hex


class TestProcessMode:
    def test_nothing_unpicklable_rides_in_the_graph_config(self, obs) -> None:
        """Spawn pickles its arguments. The old integration put a live handler here."""
        state = {"organization_id": "o", "ticket_id": "t", "agent_run_id": str(uuid.uuid4())}
        config = {"configurable": {"thread_id": state["agent_run_id"]}}
        pickle.dumps((state, config))

    @pytest.mark.slow
    def test_a_spawned_child_runs_the_instrumented_graph(self, obs) -> None:
        """SupportPilot's AGENT_EXECUTION_MODE=process: a fresh interpreter per run.

        With observability switched off in the child (no OBS_REDIS_URL), this
        proves the instrumented code imports and runs under spawn. The test that
        the child's events actually reach Redis is in the integration suite,
        because it needs a Redis both processes can see.
        """
        ctx = multiprocessing.get_context("spawn")
        queue = ctx.Queue()
        org, ticket, run = _ids()
        state = {"organization_id": str(org), "ticket_id": str(ticket), "agent_run_id": str(run)}
        child = ctx.Process(target=sp.spawned_graph_entry, args=(state, queue))
        child.start()
        result = queue.get(timeout=60)
        child.join(timeout=30)
        assert result == {"ok": True, "decision": "AUTO_REPLY_DRAFT"}
        assert child.exitcode == 0


class TestSafety:
    def test_disabled_by_config_is_a_total_no_op(self, monkeypatch) -> None:
        """What makes the patch safe to merge before AgentLens is deployed."""
        monkeypatch.setenv("OBS_ENABLED", "false")
        monkeypatch.delenv("OBS_REDIS_URL", raising=False)
        module = sp.load_integration()
        monkeypatch.setattr(module, "_obs", None)

        final = sp.run_ticket_agent(*_ids())
        assert final["decision"] == "AUTO_REPLY_DRAFT"
        stats = module.observability_stats()
        assert stats["published"] == 0 and stats["connect_failures"] == 0
        module.shutdown_observability(timeout=0.1)

    def test_a_broken_publisher_never_reaches_the_agent(self, obs, monkeypatch) -> None:
        def explode(event: Any) -> bool:
            raise RuntimeError("publisher bug")

        monkeypatch.setattr(obs.captured, "publish", explode)
        final = sp.run_ticket_agent(*_ids())
        assert final["decision"] == "AUTO_REPLY_DRAFT"

    def test_usage_outside_an_llm_span_is_ignored(self, obs) -> None:
        obs.record_llm_usage(sp.GeminiResponse("x", 1, 1))
        obs.record_llm_usage(object())
        assert obs.captured.events == []

    def test_shutdown_is_idempotent(self, obs) -> None:
        obs.shutdown_observability(timeout=0.1)
        obs.shutdown_observability(timeout=0.1)
