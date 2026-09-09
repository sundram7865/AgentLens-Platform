"""Drop-in observability wiring for SupportPilot.

Copy this file to ``apps/api/app/common/observability.py`` in the SupportPilot
repository. It is the only file you need to add; the rest of the integration is
four call sites listed in the README next to this file.

Design constraints this file respects, because they are SupportPilot's rules:

* **The backend decides what is allowed, the AI only recommends.** This module
  observes; it never changes a decision, never blocks a tool call, and never
  raises into a request. If the observability platform is down, tickets are
  handled exactly as before.
* **Multi-tenant by organization.** Every event carries ``organization_id`` as
  the platform's ``tenant_id``, so one org can never see another's traces
  through the dashboard's RBAC layer.
* **No new heavy dependencies.** ``obs-sdk`` pulls in ``pydantic`` and ``redis``,
  both of which SupportPilot already has.

Configuration (SupportPilot's ``.env``)::

    OBS_ENABLED=true
    OBS_REDIS_URL=rediss://default:...@...upstash.io:6379
    OBS_SERVICE=supportpilot
    OBS_ENVIRONMENT=production
    OBS_CAPTURE_CONTENT=true

Leaving ``OBS_REDIS_URL`` unset disables everything and turns every call in this
module into a no-op, so this can be merged before the platform is deployed.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Iterator
from typing import Any
from uuid import UUID

from obs_sdk import Observability, SpanKind, Tracer, new_trace_id

# One publisher per process. It owns a bounded queue and a daemon thread; making
# one per request would spawn a thread per request.
_obs: Observability | None = None


def get_observability() -> Observability:
    global _obs
    if _obs is None:
        _obs = Observability.from_env(service=os.getenv("OBS_SERVICE", "supportpilot"))
    return _obs


def shutdown_observability(timeout: float = 5.0) -> None:
    """Flush buffered events. Call from the FastAPI lifespan shutdown hook.

    Without this, the daemon thread is killed on exit with events still
    buffered, and the last few seconds of traces before every deploy are lost --
    which is exactly the window where a bad deploy shows up.
    """
    global _obs
    if _obs is not None:
        _obs.shutdown(timeout=timeout)
        _obs = None


def new_agent_trace_id() -> str:
    """Generate once per agent run, in the request handler.

    Deliberately returned rather than stashed in a global or a ContextVar the
    caller does not control: under concurrent ticket processing, shared mutable
    state silently braids two customers' traces into one.
    """
    return new_trace_id()


def agent_config(
    *,
    organization_id: str | UUID,
    ticket_id: str | UUID,
    trace_id: str | None = None,
    agent_run_id: str | UUID | None = None,
    category: str | None = None,
    priority: str | None = None,
    risk_level: str | None = None,
    user_ref: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the ``config`` dict to pass to ``graph.invoke`` / ``graph.ainvoke``.

    Usage in ``app/modules/agent/service.py``::

        from app.common.observability import agent_config, new_agent_trace_id

        trace_id = new_agent_trace_id()
        result = await graph.ainvoke(
            state,
            config=agent_config(
                organization_id=organization.id,
                ticket_id=ticket.id,
                trace_id=trace_id,
                agent_run_id=agent_run.id,
            ),
        )

    If you already pass a ``config``, merge rather than replace -- LangGraph uses
    it for its own ``configurable`` keys too::

        config = {**existing_config, **agent_config(...)}
    """
    obs = get_observability()
    tenant_id = str(organization_id)

    metadata: dict[str, Any] = {
        "trace_id": trace_id or new_trace_id(),
        "tenant_id": tenant_id,
        "organization_id": tenant_id,
        "ticket_id": str(ticket_id),
    }
    if agent_run_id is not None:
        metadata["agent_run_id"] = str(agent_run_id)
    for key, value in (
        ("category", category),
        ("priority", priority),
        ("risk_level", risk_level),
    ):
        if value:
            metadata[key] = value
    if extra:
        metadata.update(extra)

    handler = obs.handler(
        tenant_id=tenant_id,
        trace_id=metadata["trace_id"],
        user_ref=user_ref,
        attributes={k: v for k, v in metadata.items() if k != "trace_id"},
    )
    # Both keys matter: `callbacks` is what LangChain invokes, `metadata` is what
    # a LangGraph node's own callbacks see, so a node that creates a sub-chain
    # still inherits the trace id.
    return {"callbacks": [handler], "metadata": metadata}


def get_tracer(organization_id: str | UUID) -> Tracer:
    """A tracer for code that is not a LangChain run.

    SupportPilot's most security-relevant work is outside the callback surface:
    the tool gateway, approval decisions, provider HTTP calls. Those spans join
    the same trace as the agent's LLM calls, so the trace detail view does not
    show an agent that decided to refund someone with no tool call attached.
    """
    return get_observability().tracer(tenant_id=str(organization_id))


@contextlib.contextmanager
def observe_tool_execution(
    *,
    organization_id: str | UUID,
    tool_name: str,
    scope: str,
    arguments: dict[str, Any] | None = None,
    ticket_id: str | UUID | None = None,
) -> Iterator[Any]:
    """Wrap one tool-gateway execution.

    Drop into ``app/modules/tools/gateway.py`` around the provider call::

        with observe_tool_execution(
            organization_id=org.id,
            tool_name=tool.name,
            scope=tool.scope,
            arguments=normalised_args,
            ticket_id=ticket.id,
        ) as span:
            result = await provider.call(...)
            span.set_output(status=result.status)

    The span records the *scope* (READ_ONLY vs HIGH_RISK_WRITE) and the approval
    outcome, so "which high-risk writes ran, on whose authority" is a dashboard
    filter rather than a log search.
    """
    tracer = get_tracer(organization_id)
    attributes: dict[str, Any] = {"tool_scope": scope}
    if ticket_id is not None:
        attributes["ticket_id"] = str(ticket_id)

    with tracer.span(
        tool_name,
        kind=SpanKind.TOOL,
        input={"arguments": arguments or {}},
        attributes=attributes,
    ) as span:
        yield span


def observability_stats() -> dict[str, Any]:
    """Publisher counters, for SupportPilot's own ``/health`` payload.

    Surfacing ``dropped_queue_full`` there means a full buffer is visible on the
    dashboard you already watch, rather than only in the platform that is by
    definition not receiving the events.
    """
    return get_observability().stats()


__all__ = [
    "agent_config",
    "get_observability",
    "get_tracer",
    "new_agent_trace_id",
    "observability_stats",
    "observe_tool_execution",
    "shutdown_observability",
]
