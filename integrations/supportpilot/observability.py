"""AgentLens wiring for SupportPilot.

Copy this file to ``apps/api/app/common/observability.py`` in the SupportPilot
repository. ``supportpilot.patch`` next to this file is the complete change,
generated with ``git diff`` against SupportPilot's ``main`` (``ca317cd``): this
module plus four small edits.

Why it hooks ``trace_operation`` and not a LangChain callback
-------------------------------------------------------------
Checked against SupportPilot's real code, not assumed:

* **Gemini is called directly** through ``google.generativeai``
  (``app/modules/agent/llm.py``), not through a LangChain chat model. A LangChain
  callback handler therefore sees the graph's nodes but never a single LLM call:
  no prompts, no models, no tokens, no cost.
* **The graph does not run on the request thread.** ``graph.invoke`` runs on a
  ``ThreadPoolExecutor`` thread (``AGENT_EXECUTION_MODE=thread``) or in a
  *spawned* interpreter (``process``). Spawn pickles the ``config`` it is given;
  a callback handler owns a publisher with a thread, a lock and a queue, none of
  which pickle, so putting one in ``config`` fails every run in process mode.
* **Every agent step, every Gemini call and every UrbanKart HTTP call already
  passes through one function**: ``app.core.resilience.trace_operation`` (via
  ``run_logged_step``, ``LLMClient.generate_*`` and ``UrbanKartClient``).
  Wrapping that one function instruments all three, in whatever thread or
  process they happen to run.

Trace identity crosses threads and processes as data, never as shared state:
the trace id is a pure function of ``agent_run_id``, which every agent step
already carries in its metadata. A step running in a spawned child computes the
same trace id as the request thread that opened the run, with nothing passed
between them. A run paused for approval and resumed later lands in the same
trace for the same reason.

Rules this module keeps, because they are SupportPilot's rules
--------------------------------------------------------------
* **Observe only.** Nothing here changes a decision, blocks a tool call, or
  raises into a request. Every entry point swallows its own failures.
* **Tenant = organization.** Every event carries ``organization_id`` as the
  platform's ``tenant_id``, so the dashboard's RBAC scopes by organization.
* **Off means off.** With ``OBS_REDIS_URL`` unset every function is a no-op:
  no thread, no socket, no queue. Safe to merge before AgentLens is deployed.

Configuration (SupportPilot's ``apps/api/.env``)::

    OBS_ENABLED=true
    OBS_REDIS_URL=rediss://default:<password>@<host>.upstash.io:6379
    OBS_SERVICE=supportpilot
    OBS_ENVIRONMENT=production
    OBS_CAPTURE_CONTENT=true   # false: shapes and sizes only, no ticket text
"""

from __future__ import annotations

import contextlib
import logging
import os
from collections.abc import Iterator
from contextvars import ContextVar
from typing import Any
from uuid import UUID

from obs_sdk import Observability, SpanHandle, SpanKind, Tracer, trace_context
from obs_sdk.context import get_trace_context

logger = logging.getLogger(__name__)

# One publisher per process. It owns a bounded queue and a daemon thread; one
# per request would be a thread per request. A spawned agent process builds its
# own on first use and flushes it from the SDK's exit hook when the run returns.
_obs: Observability | None = None

# The innermost open LLM operation on this thread, so the Gemini call inside it
# can attach token usage without SupportPilot threading a handle through
# _run_over_model_chain and run_with_retry.
_current_llm: ContextVar[SpanHandle | None] = ContextVar("agentlens_current_llm", default=None)

_KIND_BY_RUN_TYPE = {
    "chain": SpanKind.CHAIN,
    "llm": SpanKind.LLM,
    "tool": SpanKind.TOOL,
    "retriever": SpanKind.RETRIEVER,
}

# run_logged_step hands back the *whole* AgentState after every step. Storing it
# fifteen times per run would make every trace mostly duplicate copies of the
# ticket, so each step span keeps only the keys that step is responsible for.
# Keys are SupportPilot's AgentState (app/modules/agent/state.py).
_STEP_OUTPUT_KEYS: dict[str, tuple[str, ...]] = {
    "load_ticket_context": ("injection_signals",),
    "classify_ticket": ("detected_category", "detected_priority", "classification_confidence"),
    "detect_risk": ("risk_level", "risk_reasons"),
    "plan_tools": ("planned_tools",),
    "draft_response": ("draft_response", "reasoning_summary", "draft_attempts"),
    "groundedness_check": ("groundedness_status", "groundedness_reasons"),
    "decision": ("decision", "policy_reasons", "policy_blocked_auto_reply"),
    "approval": ("approval_decision", "approval_reason"),
    "execute_tools": ("tool_execution_results",),
    "execute_read_only_tools": ("tool_execution_results",),
    "merge_results": ("tool_execution_results",),
    "mark_skipped": ("tool_execution_results",),
    "escalate": ("decision", "error_message"),
    "ask_customer": ("decision", "draft_response"),
}


# --------------------------------------------------------------------------- #
# Lifecycle
# --------------------------------------------------------------------------- #
def get_observability() -> Observability:
    global _obs
    if _obs is None:
        _obs = Observability.from_env(service=os.getenv("OBS_SERVICE", "supportpilot"))
    return _obs


def shutdown_observability(timeout: float = 5.0) -> None:
    """Flush buffered events. Call from the FastAPI lifespan shutdown.

    Without it the daemon thread dies with events still queued, and the last
    seconds before every deploy are lost: the window a bad deploy shows up in.
    """
    global _obs
    if _obs is not None:
        with contextlib.suppress(Exception):
            _obs.shutdown(timeout=timeout)
        _obs = None


def observability_stats() -> dict[str, Any]:
    """Publisher counters for SupportPilot's own ``/health`` payload."""
    try:
        return get_observability().stats()
    except Exception:
        return {}


def get_tracer(organization_id: str | UUID) -> Tracer:
    return get_observability().tracer(tenant_id=str(organization_id))


def agent_trace_id(agent_run_id: str | UUID) -> str:
    """The trace id for an agent run: a pure function of its id.

    Deterministic on purpose. The request thread, the graph's worker thread, a
    spawned agent process and a resume after approval all compute the same id
    without being handed it, which is what keeps one run in one trace.
    """
    return "tr_" + UUID(str(agent_run_id)).hex


# --------------------------------------------------------------------------- #
# Call site 1: the run envelope (agent/service.py)
# --------------------------------------------------------------------------- #
class AgentRunRecorder:
    """Collects what the run envelope reports when it closes."""

    def __init__(self, attributes: dict[str, Any]) -> None:
        self._attributes = attributes

    def finish(self, final_state: dict[str, Any] | None, *, paused: bool = False) -> None:
        """Record the question, the answer and the outcome from the final state.

        The question and answer are what the platform's eval scorer grades for
        faithfulness and relevancy, so they are named the way it reads them.
        """
        try:
            state = final_state or {}
            ticket = state.get("ticket") or {}
            question = "\n\n".join(
                part
                for part in (str(ticket.get("subject") or ""), str(ticket.get("description") or ""))
                if part
            )
            if question:
                self._attributes["__input__"] = {"question": question}
            output: dict[str, Any] = {
                "answer": state.get("draft_response") or "",
                "decision": state.get("decision"),
                "risk_level": state.get("risk_level"),
                "category": state.get("detected_category"),
                "groundedness": state.get("groundedness_status"),
            }
            if paused:
                output["status"] = "waiting_for_approval"
            self._attributes["__output__"] = {k: v for k, v in output.items() if v is not None}
            for key in ("decision", "risk_level", "detected_category"):
                if state.get(key):
                    self._attributes[key] = state[key]
        except Exception:
            logger.debug("agentlens: could not record run result", exc_info=True)


@contextlib.contextmanager
def observe_agent_run(
    *,
    organization_id: str | UUID,
    ticket_id: str | UUID,
    agent_run_id: str | UUID,
    phase: str = "run",
) -> Iterator[AgentRunRecorder]:
    """Open the trace for one agent run (``phase="run"``) or its resume.

    Wrap the call to ``_invoke_graph_with_timeout``; call ``run.finish(...)``
    with the final state when it returns. A run that raises is still closed,
    with status ``error``: that is the run you most need to see.
    """
    try:
        manager = get_tracer(organization_id).trace(
            name="ticket_agent",
            trace_id=agent_trace_id(agent_run_id),
            tenant_id=str(organization_id),
            attributes={
                "ticket_id": str(ticket_id),
                "agent_run_id": str(agent_run_id),
                "phase": phase,
            },
        )
        # The tracer copies the attributes it is given; the result has to be
        # written into the live context's dict, which is what trace.end reads.
        recorder = AgentRunRecorder(manager.__enter__().attributes)
    except Exception:
        logger.debug("agentlens: could not open run trace", exc_info=True)
        yield AgentRunRecorder({})
        return

    try:
        yield recorder
    except BaseException as exc:
        with contextlib.suppress(Exception):
            manager.__exit__(type(exc), exc, exc.__traceback__)
        raise
    else:
        with contextlib.suppress(Exception):
            manager.__exit__(None, None, None)


# --------------------------------------------------------------------------- #
# Call site 2: trace_operation (core/resilience.py)
# --------------------------------------------------------------------------- #
class _Operation:
    """What ``trace_operation`` reports back when its body finishes."""

    def __init__(self, handle: SpanHandle | None, step_name: str | None) -> None:
        self.handle = handle
        self.step_name = step_name

    def finish(self, result: dict[str, Any] | None) -> None:
        """Copy ``trace_operation``'s result dict (outputs, model) onto the span."""
        if self.handle is None or not result:
            return
        try:
            if result.get("model"):
                self.handle.set_model(str(result["model"]))
            outputs = result.get("outputs")
            if isinstance(outputs, dict):
                self.handle.set_output(**_shape_output(self.step_name, outputs))
            elif outputs is not None:
                self.handle.set_output(output=outputs)
        except Exception:
            logger.debug("agentlens: could not record operation result", exc_info=True)


@contextlib.contextmanager
def observe_operation(
    *,
    name: str,
    run_type: str,
    inputs: dict[str, Any] | None,
    metadata: dict[str, Any] | None,
) -> Iterator[_Operation]:
    """Mirror one ``trace_operation`` call as an AgentLens span.

    Which trace it joins:

    * metadata carries ``agent_run_id`` (every agent step does): the run's trace,
      and the trace context is set so everything nested inside joins it too;
    * otherwise, whatever trace is open on this thread (a Gemini call or an
      UrbanKart request made inside a step);
    * otherwise nothing is emitted. An operation outside any agent run would
      otherwise become an orphan trace under no tenant.
    """
    meta = metadata or {}
    agent_run_id = meta.get("agent_run_id")
    organization_id = meta.get("organization_id")
    step_name = meta.get("step_name")

    scope: contextlib.AbstractContextManager[Any] = contextlib.nullcontext()
    if agent_run_id and organization_id:
        try:
            scope = trace_context(
                trace_id=agent_trace_id(agent_run_id),
                tenant_id=str(organization_id),
            )
        except Exception:
            scope = contextlib.nullcontext()
    elif get_trace_context() is None:
        yield _Operation(None, None)
        return

    with scope:
        span_manager: contextlib.AbstractContextManager[SpanHandle] | None
        try:
            ctx = get_trace_context()
            tracer = get_tracer(ctx.tenant_id if ctx else (organization_id or "default"))
            attributes = {
                k: str(v)
                for k, v in meta.items()
                if k in ("component", "operation", "step_name", "ticket_id", "agent_run_id")
            }
            span_manager = tracer.span(
                name,
                kind=_KIND_BY_RUN_TYPE.get(run_type, SpanKind.OTHER),
                input=_shape_input(step_name, inputs),
                attributes=attributes,
            )
            handle = span_manager.__enter__()
        except Exception:
            logger.debug("agentlens: could not open span %s", name, exc_info=True)
            yield _Operation(None, step_name)
            return

        token = _current_llm.set(handle) if run_type == "llm" else None
        try:
            yield _Operation(handle, step_name)
        except BaseException as exc:
            _close(span_manager, exc)
            raise
        else:
            _close(span_manager, None)
        finally:
            if token is not None:
                _current_llm.reset(token)


# --------------------------------------------------------------------------- #
# Call site 3: token usage (agent/llm.py)
# --------------------------------------------------------------------------- #
def record_llm_usage(response: Any, model_name: str | None = None) -> None:
    """Attach a Gemini response's token counts to the LLM span that is open.

    Call it right after ``model.generate_content(...)``. ``usage_metadata`` is
    the only place the token counts exist; the text-returning helpers discard
    the response, so without this line every LLM span has a model and no cost.
    """
    handle = _current_llm.get()
    if handle is None:
        return
    try:
        usage = getattr(response, "usage_metadata", None)
        if usage is None:
            return
        prompt = int(getattr(usage, "prompt_token_count", 0) or 0)
        completion = int(getattr(usage, "candidates_token_count", 0) or 0)
        total = int(getattr(usage, "total_token_count", 0) or 0) or prompt + completion
        handle.set_usage(prompt_tokens=prompt, completion_tokens=completion, total_tokens=total)
        if model_name:
            handle.set_model(model_name)
    except Exception:
        logger.debug("agentlens: could not read usage_metadata", exc_info=True)


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #
def _close(manager: contextlib.AbstractContextManager[Any], exc: BaseException | None) -> None:
    with contextlib.suppress(Exception):
        if exc is None:
            manager.__exit__(None, None, None)
        else:
            manager.__exit__(type(exc), exc, exc.__traceback__)


def _shape_input(step_name: str | None, inputs: dict[str, Any] | None) -> dict[str, Any]:
    if not inputs:
        return {}
    if step_name and "ticket" in inputs and isinstance(inputs["ticket"], dict):
        # The ticket is recorded once, on the run envelope. Repeating it on every
        # step's input would store the same customer text a dozen times.
        ticket = inputs["ticket"]
        return {"ticket_id": ticket.get("id") or ticket.get("ticket_id")}
    return dict(inputs)


def _shape_output(step_name: str | None, outputs: dict[str, Any]) -> dict[str, Any]:
    if step_name is None:
        return dict(outputs)
    if step_name == "retrieve_knowledge":
        # The eval scorer grades faithfulness against `documents[].content`;
        # SupportPilot calls the same thing knowledge_context.
        chunks = outputs.get("knowledge_context") or []
        return {
            "documents": [
                {
                    "content": c.get("content", ""),
                    "title": c.get("document_title"),
                    "score": c.get("score"),
                }
                for c in chunks
                if isinstance(c, dict)
            ]
        }
    keys = _STEP_OUTPUT_KEYS.get(step_name)
    if keys is None:
        return {"keys": sorted(str(k) for k in outputs)}
    return {k: outputs[k] for k in keys if k in outputs}


__all__ = [
    "AgentRunRecorder",
    "agent_trace_id",
    "get_observability",
    "get_tracer",
    "observability_stats",
    "observe_agent_run",
    "observe_operation",
    "record_llm_usage",
    "shutdown_observability",
]
