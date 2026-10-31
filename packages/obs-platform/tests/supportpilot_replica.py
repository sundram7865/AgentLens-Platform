"""A faithful, dependency-free replica of SupportPilot's agent execution path.

``integrations/supportpilot/observability.py`` is copied into another
repository, so it gets no CI there. Testing it against an imagined host proves
nothing -- the previous version of these tests drove a LangChain callback with
a fake ``ChatGoogleGenerativeAI`` model, which SupportPilot never uses, and they
passed while the integration could not have recorded one LLM call.

So this module reproduces the parts of SupportPilot the integration touches,
copied from SupportPilot ``main`` @ ``ca317cd`` and cut down only where a
dependency (SQLAlchemy sessions, LangSmith, httpx, google-generativeai) is not
needed to reproduce the control flow. Each function names its source. The
AgentLens lines are exactly the ones ``integrations/supportpilot/supportpilot.patch``
adds, marked ``# AgentLens``.

Kept at module level, importable by name, so a ``spawn`` child process can
unpickle the entry point the way SupportPilot's process mode does.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import importlib.util
import sys
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

INTEGRATION = Path(__file__).resolve().parents[3] / "integrations/supportpilot/observability.py"
MODULE_NAME = "app_common_observability"


def load_integration() -> Any:
    """Import the drop-in file by path, as SupportPilot would after copying it."""
    if MODULE_NAME in sys.modules:
        return sys.modules[MODULE_NAME]
    spec = importlib.util.spec_from_file_location(MODULE_NAME, INTEGRATION)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[MODULE_NAME] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- #
# app/core/resilience.py :: trace_operation (LangSmith body elided)
# --------------------------------------------------------------------------- #
@contextlib.contextmanager
def _langsmith_trace_operation(*, name: str, run_type: str, inputs: dict, metadata: dict | None):
    result: dict[str, Any] = {}
    yield result


@contextlib.contextmanager
def trace_operation(*, name: str, run_type: str, inputs: dict, metadata: dict | None = None):
    obs = load_integration()  # AgentLens
    with obs.observe_operation(  # AgentLens
        name=name, run_type=run_type, inputs=inputs, metadata=metadata
    ) as agentlens_op:
        with _langsmith_trace_operation(
            name=name, run_type=run_type, inputs=inputs, metadata=metadata
        ) as result:
            yield result
        agentlens_op.finish(result)  # AgentLens


# --------------------------------------------------------------------------- #
# app/modules/agent/step_logger.py :: run_logged_step (DB row writes elided)
# --------------------------------------------------------------------------- #
def run_logged_step(
    organization_id: uuid.UUID,
    agent_run_id: uuid.UUID,
    ticket_id: uuid.UUID,
    step_name: str,
    input_json: dict | None,
    fn: Any,
) -> Any:
    with trace_operation(
        name=f"agent_step:{step_name}",
        run_type="chain",
        inputs=input_json or {},
        metadata={
            "component": "agent_graph",
            "step_name": step_name,
            "organization_id": str(organization_id),
            "agent_run_id": str(agent_run_id),
            "ticket_id": str(ticket_id),
        },
    ) as trace:
        output = fn()
        trace["outputs"] = output if isinstance(output, dict) else {"output": output}
    return output


# --------------------------------------------------------------------------- #
# app/modules/agent/llm.py :: LLMClient (Gemini path only)
# --------------------------------------------------------------------------- #
class GeminiUsage:
    def __init__(self, prompt: int, completion: int) -> None:
        self.prompt_token_count = prompt
        self.candidates_token_count = completion
        self.total_token_count = prompt + completion


class GeminiResponse:
    """The two attributes SupportPilot reads off ``generate_content``'s result."""

    def __init__(self, text: str, prompt: int, completion: int) -> None:
        self.text = text
        self.usage_metadata = GeminiUsage(prompt, completion)


class LLMClient:
    MODEL_CHAIN = ("gemini-3.5-flash-lite", "gemini-3.1-flash-lite")

    def __init__(self, fail_first_model: bool = False) -> None:
        self.fail_first_model = fail_first_model

    def _generate_content(self, prompt: str, model_name: str) -> GeminiResponse:
        if self.fail_first_model and model_name == self.MODEL_CHAIN[0]:
            raise RuntimeError("429 ResourceExhausted: quota")
        return GeminiResponse('{"category": "REFUND_REQUEST"}', prompt=850, completion=40)

    def _generate_gemini_json(self, prompt: str, model_name: str) -> dict[str, Any]:
        response = self._generate_content(prompt, model_name)
        load_integration().record_llm_usage(response, model_name)  # AgentLens
        return {"category": "REFUND_REQUEST"}

    def _run_over_model_chain(self, call: Any) -> tuple[dict[str, Any], str]:
        last: Exception | None = None
        for model_name in self.MODEL_CHAIN:
            try:
                return call(model_name), model_name
            except Exception as exc:
                last = exc
        assert last is not None
        raise last

    def generate_json(self, prompt: str) -> dict[str, Any]:
        with trace_operation(
            name="llm:generate_json",
            run_type="llm",
            inputs={"prompt": prompt},
            metadata={"component": "llm", "operation": "generate_json"},
        ) as trace:
            result, used_model = self._run_over_model_chain(
                lambda name: self._generate_gemini_json(prompt, name)
            )
            trace["outputs"] = result
            trace["model"] = used_model
        return result


# --------------------------------------------------------------------------- #
# app/modules/integrations/urbankart_client.py :: _request (httpx elided)
# --------------------------------------------------------------------------- #
async def urbankart_request(method: str, endpoint: str, payload: dict | None = None) -> dict:
    with trace_operation(
        name=f"provider:{method}:{endpoint}",
        run_type="tool",
        inputs={"method": method, "endpoint": endpoint, "payload": payload or {}},
        metadata={"component": "urbankart_client", "operation": f"{method}:{endpoint}"},
    ) as trace:
        await asyncio.sleep(0)
        response = {"order_id": "A-1001", "status": "DELIVERED", "amount": 1499}
        trace["outputs"] = response
    return response


# app/modules/agent/nodes.py :: _run_async_from_sync
def run_async_from_sync(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    holder: dict[str, Any] = {}

    def _runner() -> None:
        holder["value"] = asyncio.run(coro)

    # AgentLens: copy_context() so spans opened in this thread join the trace.
    thread = threading.Thread(target=contextvars.copy_context().run, args=(_runner,), daemon=True)
    thread.start()
    thread.join()
    return holder["value"]


# --------------------------------------------------------------------------- #
# app/modules/agent/graph.py :: the steps a refund ticket walks through
# --------------------------------------------------------------------------- #
TICKET = {
    "id": "T-1001",
    "subject": "Refund for order A-1001",
    "description": "My blender arrived broken. Please refund order A-1001.",
    "category": "REFUND_REQUEST",
}
KNOWLEDGE = [
    {
        "chunk_id": "c1",
        "document_id": "d1",
        "document_title": "Refund policy",
        "document_type": "policy",
        "content": "Damaged items are refundable within 30 days of delivery.",
        "score": 0.91,
    }
]
DRAFT = "Sorry about the blender. Order A-1001 qualifies for a refund under our 30-day policy."


def run_graph(state: dict[str, Any], *, fail_at: str | None = None, pause: bool = False) -> dict:
    """What ``graph.invoke`` does for one refund ticket, step by step."""
    org = uuid.UUID(state["organization_id"])
    run_id = uuid.UUID(state["agent_run_id"])
    ticket_id = uuid.UUID(state["ticket_id"])
    llm = LLMClient(fail_first_model=state.get("_fail_first_model", False))

    def step(name: str, input_json: dict, fn: Any) -> None:
        def wrapped() -> dict:
            if fail_at == name:
                raise RuntimeError(f"{name} exploded")
            fn()
            return dict(state)

        run_logged_step(org, run_id, ticket_id, name, input_json, wrapped)

    step(
        "load_ticket_context",
        {"ticket_id": state["ticket_id"]},
        lambda: state.update(ticket=dict(TICKET)),
    )
    step(
        "retrieve_knowledge",
        {"ticket": state.get("ticket")},
        lambda: state.update(knowledge_context=list(KNOWLEDGE)),
    )
    step(
        "classify_ticket",
        {"ticket": state.get("ticket")},
        lambda: state.update(
            detected_category=llm.generate_json("Classify: " + TICKET["description"])["category"],
            detected_priority="HIGH",
        ),
    )
    step(
        "execute_read_only_tools",
        {"planned_tools": ["urbankart_get_order_context"]},
        lambda: state.update(
            tool_execution_results=[run_async_from_sync(urbankart_request("GET", "/orders/A-1001"))]
        ),
    )
    step("draft_response", {}, lambda: state.update(draft_response=DRAFT, draft_attempts=1))
    step(
        "decision",
        {},
        lambda: state.update(decision="NEEDS_APPROVAL" if pause else "AUTO_REPLY_DRAFT"),
    )
    return dict(state)


# --------------------------------------------------------------------------- #
# app/modules/agent/service.py :: thread and spawn executors, run envelope
# --------------------------------------------------------------------------- #
def invoke_in_thread(state: dict[str, Any], **kwargs: Any) -> dict:
    """``_invoke_graph_in_thread``: a fresh executor thread, no context copied."""
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="agent-graph")
    try:
        return executor.submit(run_graph, state, **kwargs).result(timeout=30)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def run_ticket_agent(
    organization_id: uuid.UUID,
    ticket_id: uuid.UUID,
    agent_run_id: uuid.UUID,
    *,
    phase: str = "run",
    **kwargs: Any,
) -> dict:
    """``_run_ticket_agent_locked`` around ``_invoke_graph_with_timeout``."""
    obs = load_integration()
    initial_state = {
        "organization_id": str(organization_id),
        "ticket_id": str(ticket_id),
        "agent_run_id": str(agent_run_id),
        **kwargs.pop("extra_state", {}),
    }
    with obs.observe_agent_run(  # AgentLens
        organization_id=organization_id,
        ticket_id=ticket_id,
        agent_run_id=agent_run_id,
        phase=phase,
    ) as run:
        final_state = invoke_in_thread(initial_state, **kwargs)
        run.finish(final_state, paused=kwargs.get("pause", False))  # AgentLens
    return final_state


def spawned_graph_entry(state: dict[str, Any], result_queue: Any) -> None:
    """``_graph_process_entry``: runs in a *spawned* interpreter.

    Nothing AgentLens-specific is passed in. The child imports the integration
    fresh and derives the trace id from ``agent_run_id`` on its own, which is
    the property under test. Returns normally, as SupportPilot's does, so the
    interpreter's exit hooks run -- that is what flushes the child's publisher.
    """
    try:
        final_state = run_graph(state)
        result_queue.put({"ok": True, "decision": final_state.get("decision")})
    except Exception as exc:  # pragma: no cover - reported to the parent
        result_queue.put({"ok": False, "error": repr(exc)})


__all__ = [
    "DRAFT",
    "KNOWLEDGE",
    "TICKET",
    "LLMClient",
    "invoke_in_thread",
    "load_integration",
    "run_graph",
    "run_ticket_agent",
    "spawned_graph_entry",
    "trace_operation",
    "urbankart_request",
]
