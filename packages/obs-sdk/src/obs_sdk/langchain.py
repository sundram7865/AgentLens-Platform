"""``ObservabilityCallbackHandler`` -- the LangChain/LangGraph integration.

Design rules, each of which exists because of a specific failure mode:

1. **No hook may raise into the agent.** Every callback body is wrapped. A bug
   in this file must degrade observability, never a customer's ticket.
2. **Trace identity comes from LangChain's own ``run_id``/``parent_run_id``
   lineage**, not from handler state. That makes a single shared handler
   instance safe under concurrency: two simultaneous ticket runs cannot braid
   into one trace, because their run trees are disjoint.
3. **Error hooks are implemented, not skipped.** ``on_llm_error`` and
   ``on_tool_error`` are the whole reason to have this: the failing run is the
   run you need to see.
4. **Every span carries ``parent_span_id``**, so the platform can rebuild the
   call tree the same way OpenTelemetry does.
"""

from __future__ import annotations

import functools
import json
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeVar

from .context import current_span_id, get_trace_context
from .publisher import NullPublisher, Publisher
from .schema import (
    Attr,
    ErrorInfo,
    EventType,
    ObsEvent,
    SpanKind,
    SpanStatus,
    Usage,
    clip,
    new_span_id,
    new_trace_id,
    utcnow,
)
from .tracer import error_info

logger = logging.getLogger("obs_sdk.langchain")

try:  # pragma: no cover - exercised by whichever environment is installed
    from langchain_core.callbacks.base import BaseCallbackHandler as _LCBase

    LANGCHAIN_AVAILABLE = True
except Exception:  # pragma: no cover

    class _LCBase:  # type: ignore[no-redef]
        """Stand-in so obs-sdk imports cleanly without langchain-core installed."""

        raise_error = False
        run_inline = False

    LANGCHAIN_AVAILABLE = False


# LangGraph and LCEL emit a chain run for every internal plumbing step. Tracing
# them buries the six steps a human actually cares about under fifty that they
# do not. Skipped runs still register in the run table, so their children are
# re-parented onto the nearest kept ancestor and the tree stays connected.
DEFAULT_IGNORED_CHAINS: frozenset[str] = frozenset(
    {
        "ChannelWrite",
        "ChannelRead",
        "RunnableSequence",
        "RunnableParallel",
        "RunnableLambda",
        "RunnableAssign",
        "RunnablePassthrough",
        "RunnableWithFallbacks",
        "ChatPromptTemplate",
        "PromptTemplate",
        "StrOutputParser",
        "JsonOutputParser",
        "PydanticOutputParser",
        "_write",
        "_read",
        "__start__",
        "__end__",
        "_execute",
    }
)

MAX_TRACKED_RUNS = 20_000
"""Ceiling on the in-memory run table. A crashed graph leaks entries; evict oldest."""


@dataclass
class _Run:
    """One in-flight LangChain run."""

    run_id: str
    trace_id: str
    tenant_id: str
    span_id: str | None  # None when the run is skipped (plumbing chain)
    parent_span_id: str | None
    name: str
    kind: SpanKind
    started_at: Any
    started_perf: float
    is_root: bool = False
    session_id: str | None = None
    user_ref: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    model: str | None = None


@dataclass
class _TraceAgg:
    """Rollup accumulated across a trace so ``trace.end`` carries totals."""

    trace_id: str
    tenant_id: str
    name: str
    started_at: Any
    started_perf: float
    session_id: str | None = None
    user_ref: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    prompt_tokens: int = 0
    completion_tokens: int = 0
    span_count: int = 0
    error_count: int = 0
    llm_calls: int = 0
    tool_calls: int = 0
    models: list[str] = field(default_factory=list)
    first_input: dict[str, Any] = field(default_factory=dict)
    last_output: dict[str, Any] = field(default_factory=dict)


F = TypeVar("F", bound=Callable[..., Any])


def _guard(fn: F) -> F:
    """Swallow anything a hook raises. Rule 1 of this module, enforced mechanically."""

    @functools.wraps(fn)
    def wrapper(self: ObservabilityCallbackHandler, *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(self, *args, **kwargs)
        except Exception:
            self._internal_errors += 1
            # debug, not warning: a broken handler must not flood the host's logs.
            logger.debug("obs-sdk: callback %s failed", fn.__name__, exc_info=True)
            return None

    return wrapper  # type: ignore[return-value]


class ObservabilityCallbackHandler(_LCBase):
    """Emits one span per LangChain run and one trace per root run.

    Usage inside SupportPilot's agent module::

        handler = ObservabilityCallbackHandler(publisher, tenant_id=str(org_id),
                                               service="supportpilot")
        graph.invoke(state, config={
            "callbacks": [handler],
            "metadata": {"trace_id": trace_id, "ticket_id": str(ticket.id)},
        })
    """

    raise_error = False  # LangChain must never re-raise our exceptions
    run_inline = False

    def __init__(
        self,
        publisher: Publisher | None = None,
        tenant_id: str = "default",
        service: str = "unknown",
        environment: str = "local",
        trace_id: str | None = None,
        session_id: str | None = None,
        user_ref: str | None = None,
        attributes: dict[str, Any] | None = None,
        capture_content: bool = True,
        capture_chains: bool = True,
        ignored_chain_names: Sequence[str] | None = None,
        max_tracked_runs: int = MAX_TRACKED_RUNS,
    ) -> None:
        self.publisher: Publisher = publisher or NullPublisher()
        self.tenant_id = tenant_id
        self.service = service
        self.environment = environment
        self.default_trace_id = trace_id
        self.session_id = session_id
        self.user_ref = user_ref
        self.default_attributes = dict(attributes or {})
        self.capture_content = capture_content
        self.capture_chains = capture_chains
        self.ignored_chain_names = (
            frozenset(ignored_chain_names)
            if ignored_chain_names is not None
            else DEFAULT_IGNORED_CHAINS
        )
        self.max_tracked_runs = max_tracked_runs

        self._runs: OrderedDict[str, _Run] = OrderedDict()
        self._traces: dict[str, _TraceAgg] = {}
        self._lock = threading.RLock()
        self._internal_errors = 0

    # ------------------------------------------------------------------ #
    # Diagnostics
    # ------------------------------------------------------------------ #
    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "open_runs": len(self._runs),
                "open_traces": len(self._traces),
                "internal_errors": self._internal_errors,
                "publisher": getattr(self.publisher, "stats", None)
                and self.publisher.stats.snapshot(),  # type: ignore[attr-defined]
            }

    # ------------------------------------------------------------------ #
    # LLM hooks
    # ------------------------------------------------------------------ #
    @_guard
    def on_llm_start(
        self,
        serialized: dict[str, Any] | None = None,
        prompts: list[str] | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._start_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name_of(serialized, kwargs, default="llm"),
            kind=SpanKind.LLM,
            payload={"prompts": [clip(p) for p in (prompts or [])]},
            metadata=metadata,
            tags=tags,
            model=_model_of(serialized, kwargs, metadata),
        )

    @_guard
    def on_chat_model_start(
        self,
        serialized: dict[str, Any] | None = None,
        messages: list[list[Any]] | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._start_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name_of(serialized, kwargs, default="chat_model"),
            kind=SpanKind.LLM,
            payload={"messages": _messages_to_text(messages)},
            metadata=metadata,
            tags=tags,
            model=_model_of(serialized, kwargs, metadata),
        )

    @_guard
    def on_llm_end(
        self, response: Any = None, *, run_id: Any = None, parent_run_id: Any = None, **kwargs: Any
    ) -> None:
        usage, model = extract_usage(response)
        self._end_run(
            run_id,
            status=SpanStatus.OK,
            payload={"completion": clip(extract_generation_text(response))},
            usage=usage,
            model=model,
        )

    @_guard
    def on_llm_error(
        self,
        error: BaseException | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        # Without this hook a failing model call produces no span at all, and the
        # trace shows a gap exactly where the incident is.
        self._end_run(run_id, status=SpanStatus.ERROR, error=_as_error(error))

    # ------------------------------------------------------------------ #
    # Chain / LangGraph node hooks
    # ------------------------------------------------------------------ #
    @_guard
    def on_chain_start(
        self,
        serialized: dict[str, Any] | None = None,
        inputs: Any = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        node = (metadata or {}).get("langgraph_node")
        name = node or _name_of(serialized, kwargs, default="chain")
        is_root = parent_run_id is None
        # A LangGraph node is always worth a span even if its class name is on
        # the plumbing ignore list; the root always is, or there is no trace.
        skip = (
            not is_root
            and node is None
            and (not self.capture_chains or name in self.ignored_chain_names)
        )
        self._start_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=name,
            kind=SpanKind.AGENT if is_root else SpanKind.CHAIN,
            payload={"inputs": _to_payload(inputs)},
            metadata=metadata,
            tags=tags,
            skip=skip,
        )

    @_guard
    def on_chain_end(
        self, outputs: Any = None, *, run_id: Any = None, parent_run_id: Any = None, **kwargs: Any
    ) -> None:
        self._end_run(run_id, status=SpanStatus.OK, payload={"outputs": _to_payload(outputs)})

    @_guard
    def on_chain_error(
        self,
        error: BaseException | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._end_run(run_id, status=SpanStatus.ERROR, error=_as_error(error))

    # ------------------------------------------------------------------ #
    # Tool hooks
    # ------------------------------------------------------------------ #
    @_guard
    def on_tool_start(
        self,
        serialized: dict[str, Any] | None = None,
        input_str: str | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        inputs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        payload: dict[str, Any] = {"input": clip(input_str)}
        if inputs:
            payload["inputs"] = _to_payload(inputs)
        self._start_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name_of(serialized, kwargs, default="tool"),
            kind=SpanKind.TOOL,
            payload=payload,
            metadata=metadata,
            tags=tags,
        )

    @_guard
    def on_tool_end(
        self, output: Any = None, *, run_id: Any = None, parent_run_id: Any = None, **kwargs: Any
    ) -> None:
        self._end_run(run_id, status=SpanStatus.OK, payload={"output": _to_payload(output)})

    @_guard
    def on_tool_error(
        self,
        error: BaseException | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        # A refund tool that throws is a support incident, not a log line.
        self._end_run(run_id, status=SpanStatus.ERROR, error=_as_error(error))

    # ------------------------------------------------------------------ #
    # Retriever hooks (SupportPilot's pgvector knowledge search)
    # ------------------------------------------------------------------ #
    @_guard
    def on_retriever_start(
        self,
        serialized: dict[str, Any] | None = None,
        query: str | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._start_run(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name_of(serialized, kwargs, default="retriever"),
            kind=SpanKind.RETRIEVER,
            payload={"query": clip(query)},
            metadata=metadata,
            tags=tags,
        )

    @_guard
    def on_retriever_end(
        self,
        documents: Sequence[Any] | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        docs = list(documents or [])
        # The retrieved context is what the faithfulness scorer grades against,
        # so it has to be on the span, not merely counted.
        self._end_run(
            run_id,
            status=SpanStatus.OK,
            payload={
                "document_count": len(docs),
                "documents": [
                    {
                        "content": clip(getattr(d, "page_content", d), 2_000),
                        "metadata": _to_payload(getattr(d, "metadata", None)),
                    }
                    for d in docs[:20]
                ],
            },
        )

    @_guard
    def on_retriever_error(
        self,
        error: BaseException | None = None,
        *,
        run_id: Any = None,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._end_run(run_id, status=SpanStatus.ERROR, error=_as_error(error))

    # ------------------------------------------------------------------ #
    # Agent hooks -- recorded as attributes on the enclosing trace
    # ------------------------------------------------------------------ #
    @_guard
    def on_agent_action(
        self, action: Any = None, *, run_id: Any = None, parent_run_id: Any = None, **kwargs: Any
    ) -> None:
        trace = self._trace_for_run(run_id)
        if trace is not None:
            trace.attributes.setdefault("agent_actions", []).append(
                clip(getattr(action, "tool", str(action)), 200)
            )

    @_guard
    def on_agent_finish(
        self, finish: Any = None, *, run_id: Any = None, parent_run_id: Any = None, **kwargs: Any
    ) -> None:
        trace = self._trace_for_run(run_id)
        if trace is not None:
            values = getattr(finish, "return_values", None)
            if isinstance(values, dict):
                trace.last_output = _to_payload(values)

    def on_text(self, *args: Any, **kwargs: Any) -> None:
        return None

    def on_llm_new_token(self, *args: Any, **kwargs: Any) -> None:
        # Emitting a span per streamed token would be a self-inflicted DoS on a
        # 10k-command/day free tier.
        return None

    # ------------------------------------------------------------------ #
    # Core run bookkeeping
    # ------------------------------------------------------------------ #
    def _start_run(
        self,
        run_id: Any,
        parent_run_id: Any,
        name: str,
        kind: SpanKind,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        model: str | None = None,
        skip: bool = False,
    ) -> None:
        rid = _key(run_id)
        if rid is None:
            return
        parent_key = _key(parent_run_id)
        meta = metadata or {}

        with self._lock:
            parent = self._runs.get(parent_key) if parent_key else None
            is_root = parent is None

            if parent is not None:
                trace_id = parent.trace_id
                tenant_id = parent.tenant_id
                session_id = parent.session_id
                user_ref = parent.user_ref
                # Skipped ancestors have span_id=None and already carry their own
                # nearest kept ancestor, so this stays connected either way.
                effective_parent = parent.span_id or parent.parent_span_id
                attributes = dict(parent.attributes)
            else:
                ctx = get_trace_context()
                trace_id = (
                    meta.get("trace_id")
                    or meta.get("obs_trace_id")
                    or self.default_trace_id
                    or (ctx.trace_id if ctx else None)
                    or new_trace_id()
                )
                tenant_id = str(
                    meta.get("tenant_id")
                    or meta.get(Attr.ORGANIZATION_ID)
                    or (ctx.tenant_id if ctx else None)
                    or self.tenant_id
                )
                session_id = meta.get("session_id") or (ctx.session_id if ctx else self.session_id)
                user_ref = meta.get("user_ref") or (ctx.user_ref if ctx else self.user_ref)
                effective_parent = current_span_id()
                attributes = dict(self.default_attributes)
                if ctx:
                    attributes.update(ctx.attributes)

            attributes.update(_domain_attributes(meta))
            if tags:
                attributes["tags"] = list(tags)

            span_id = None if skip else new_span_id()
            run = _Run(
                run_id=rid,
                trace_id=str(trace_id),
                tenant_id=tenant_id,
                span_id=span_id,
                parent_span_id=effective_parent,
                name=name,
                kind=kind,
                started_at=utcnow(),
                started_perf=time.perf_counter(),
                is_root=is_root,
                session_id=session_id,
                user_ref=user_ref,
                attributes=attributes,
                model=model,
            )
            self._register(run)

            if is_root:
                agg = _TraceAgg(
                    trace_id=run.trace_id,
                    tenant_id=tenant_id,
                    name=name,
                    started_at=run.started_at,
                    started_perf=run.started_perf,
                    session_id=session_id,
                    user_ref=user_ref,
                    attributes=dict(attributes),
                    first_input=self._payload(payload),
                )
                self._traces[run.trace_id] = agg
                self._emit(
                    self._event(
                        run,
                        EventType.TRACE_START,
                        span_id=None,
                        payload_in=agg.first_input,
                    )
                )

            open_trace = self._traces.get(run.trace_id)
            if open_trace is not None and span_id is not None:
                open_trace.span_count += 1
                if kind is SpanKind.LLM:
                    open_trace.llm_calls += 1
                elif kind is SpanKind.TOOL:
                    open_trace.tool_calls += 1

        if skip:
            return
        self._emit(self._event(run, EventType.SPAN_START, payload_in=self._payload(payload)))

    def _end_run(
        self,
        run_id: Any,
        status: SpanStatus,
        payload: dict[str, Any] | None = None,
        usage: Usage | None = None,
        model: str | None = None,
        error: ErrorInfo | None = None,
    ) -> None:
        rid = _key(run_id)
        if rid is None:
            return
        with self._lock:
            run = self._runs.pop(rid, None)
            if run is None:
                # An end without a start: the handler was attached mid-run, or the
                # start hook was filtered. Nothing sane to emit.
                return
            if model:
                run.model = model
            agg = self._traces.get(run.trace_id)
            if agg is not None:
                if usage is not None:
                    agg.prompt_tokens += usage.prompt_tokens
                    agg.completion_tokens += usage.completion_tokens
                if status is SpanStatus.ERROR:
                    agg.error_count += 1
                if run.model and run.model not in agg.models:
                    agg.models.append(run.model)
                if payload:
                    agg.last_output = self._payload(payload)
            is_root = run.is_root

        latency_ms = max(0, int((time.perf_counter() - run.started_perf) * 1000))
        if run.span_id is not None:
            self._emit(
                self._event(
                    run,
                    EventType.SPAN_END,
                    status=status,
                    payload_out=self._payload(payload or {}),
                    usage=usage,
                    error=error,
                    latency_ms=latency_ms,
                )
            )

        if is_root:
            self._close_trace(run, status, latency_ms, error)

    def _close_trace(
        self,
        run: _Run,
        status: SpanStatus,
        latency_ms: int,
        error: ErrorInfo | None,
    ) -> None:
        with self._lock:
            agg = self._traces.pop(run.trace_id, None)
        if agg is None:
            return
        attributes = dict(agg.attributes)
        attributes.update(
            {
                "span_count": agg.span_count,
                "llm_calls": agg.llm_calls,
                "tool_calls": agg.tool_calls,
                "error_count": agg.error_count,
                "models": agg.models,
            }
        )
        self._emit(
            ObsEvent(
                type=EventType.TRACE_END,
                trace_id=agg.trace_id,
                tenant_id=agg.tenant_id,
                name=agg.name,
                kind=SpanKind.AGENT,
                status=status,
                started_at=agg.started_at,
                ended_at=utcnow(),
                latency_ms=latency_ms,
                output=agg.last_output,
                error=error,
                usage=Usage(
                    prompt_tokens=agg.prompt_tokens,
                    completion_tokens=agg.completion_tokens,
                ),
                model=agg.models[0] if agg.models else None,
                attributes=attributes,
                session_id=agg.session_id,
                user_ref=agg.user_ref,
                service=self.service,
                environment=self.environment,
            )
        )

    def _register(self, run: _Run) -> None:
        """Insert into the run table, evicting the oldest if it has grown unbounded."""
        self._runs[run.run_id] = run
        while len(self._runs) > self.max_tracked_runs:
            evicted_id, evicted = self._runs.popitem(last=False)
            logger.debug("obs-sdk: evicting stale run %s (%s)", evicted_id, evicted.name)

    def _trace_for_run(self, run_id: Any) -> _TraceAgg | None:
        rid = _key(run_id)
        with self._lock:
            run = self._runs.get(rid) if rid else None
            if run is None:
                return None
            return self._traces.get(run.trace_id)

    def _event(
        self,
        run: _Run,
        event_type: EventType,
        span_id: str | None = ...,  # type: ignore[assignment]
        status: SpanStatus = SpanStatus.RUNNING,
        payload_in: dict[str, Any] | None = None,
        payload_out: dict[str, Any] | None = None,
        usage: Usage | None = None,
        error: ErrorInfo | None = None,
        latency_ms: int | None = None,
    ) -> ObsEvent:
        resolved_span = run.span_id if span_id is ... else span_id
        is_end = event_type in (EventType.SPAN_END, EventType.TRACE_END)
        return ObsEvent(
            type=event_type,
            trace_id=run.trace_id,
            span_id=resolved_span,
            parent_span_id=run.parent_span_id if resolved_span else None,
            tenant_id=run.tenant_id,
            name=run.name,
            kind=run.kind,
            status=status,
            started_at=run.started_at,
            ended_at=utcnow() if is_end else None,
            latency_ms=latency_ms,
            input=payload_in or {},
            output=payload_out or {},
            error=error,
            usage=usage,
            model=run.model,
            attributes=run.attributes,
            session_id=run.session_id,
            user_ref=run.user_ref,
            service=self.service,
            environment=self.environment,
        )

    def _payload(self, value: dict[str, Any] | None) -> dict[str, Any]:
        if not value:
            return {}
        if self.capture_content:
            return value
        return {
            "__redacted_by_producer__": True,
            "keys": sorted(str(k) for k in value),
            "approx_chars": sum(len(str(v)) for v in value.values()),
        }

    def _emit(self, event: ObsEvent) -> None:
        try:
            self.publisher.publish(event)
        except Exception:  # pragma: no cover - defensive
            self._internal_errors += 1
            logger.debug("obs-sdk: emit failed", exc_info=True)


# --------------------------------------------------------------------------- #
# Extraction helpers -- provider quirks live here, not in the hooks
# --------------------------------------------------------------------------- #
_PROMPT_KEYS = ("prompt_tokens", "input_tokens", "prompt_token_count")
_COMPLETION_KEYS = ("completion_tokens", "output_tokens", "candidates_token_count")
_TOTAL_KEYS = ("total_tokens", "total_token_count")


def normalize_usage(raw: dict[str, Any] | None) -> Usage | None:
    """Map any provider's token dict onto :class:`Usage`.

    OpenAI says ``prompt_tokens``, Anthropic says ``input_tokens``, Gemini says
    ``prompt_token_count``. SupportPilot runs Gemini with an optional Groq
    evaluation path, so guessing one vendor's spelling would zero out the cost
    chart on the other.
    """
    if not isinstance(raw, dict) or not raw:
        return None

    def pick(keys: tuple[str, ...]) -> int:
        for k in keys:
            v = raw.get(k)
            if isinstance(v, (int, float)):
                return int(v)
        return 0

    prompt = pick(_PROMPT_KEYS)
    completion = pick(_COMPLETION_KEYS)
    total = pick(_TOTAL_KEYS)
    if not (prompt or completion or total):
        return None
    return Usage(prompt_tokens=prompt, completion_tokens=completion, total_tokens=total)


def extract_usage(response: Any) -> tuple[Usage | None, str | None]:
    """Pull token usage and model name out of an ``LLMResult``."""
    if response is None:
        return None, None
    llm_output = getattr(response, "llm_output", None)
    llm_output = llm_output if isinstance(llm_output, dict) else {}
    model = llm_output.get("model_name") or llm_output.get("model")

    usage: Usage | None = None
    for key in ("token_usage", "usage", "usage_metadata"):
        usage = normalize_usage(llm_output.get(key))
        if usage:
            break

    if usage is None or model is None:
        for generation_list in getattr(response, "generations", None) or []:
            for generation in generation_list or []:
                message = getattr(generation, "message", None)
                if usage is None:
                    usage = normalize_usage(getattr(message, "usage_metadata", None))
                info = getattr(generation, "generation_info", None) or {}
                if usage is None and isinstance(info, dict):
                    for key in ("usage_metadata", "token_usage", "usage"):
                        usage = normalize_usage(info.get(key))
                        if usage:
                            break
                if model is None:
                    meta = getattr(message, "response_metadata", None) or {}
                    if isinstance(meta, dict):
                        model = meta.get("model_name") or meta.get("model")
                    if model is None and isinstance(info, dict):
                        model = info.get("model_name") or info.get("model")
            if usage is not None and model is not None:
                break
    return usage, (str(model) if model else None)


def extract_generation_text(response: Any) -> str:
    """Flatten an ``LLMResult`` into text, handling both string and block content."""
    if response is None:
        return ""
    parts: list[str] = []
    for generation_list in getattr(response, "generations", None) or []:
        for generation in generation_list or []:
            text = getattr(generation, "text", None)
            if not text:
                message = getattr(generation, "message", None)
                content = getattr(message, "content", None)
                if isinstance(content, str):
                    text = content
                elif content:
                    text = json.dumps(content, default=str)
            if text:
                parts.append(str(text))
    return "\n".join(parts)


def _messages_to_text(messages: list[list[Any]] | None) -> list[str]:
    out: list[str] = []
    for group in messages or []:
        for message in group or []:
            role = (
                getattr(message, "type", None)
                or getattr(message, "role", None)
                or type(message).__name__
            )
            content = getattr(message, "content", message)
            if not isinstance(content, str):
                content = json.dumps(content, default=str)
            out.append(clip(f"{role}: {content}"))
    return out


def _to_payload(value: Any, depth: int = 0) -> Any:
    """Make an arbitrary LangChain payload JSON-safe and bounded."""
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return clip(value)
    if depth >= 4:
        return clip(value)
    if isinstance(value, dict):
        return {str(k): _to_payload(v, depth + 1) for k, v in list(value.items())[:50]}
    if isinstance(value, (list, tuple, set)):
        return [_to_payload(v, depth + 1) for v in list(value)[:50]]
    for attr in ("content", "page_content"):
        inner = getattr(value, attr, None)
        if inner is not None:
            return _to_payload(inner, depth + 1)
    return clip(value)


def _name_of(serialized: dict[str, Any] | None, kwargs: dict[str, Any], default: str) -> str:
    if kwargs.get("name"):
        return str(kwargs["name"])
    if isinstance(serialized, dict):
        if serialized.get("name"):
            return str(serialized["name"])
        ident = serialized.get("id")
        if isinstance(ident, (list, tuple)) and ident:
            return str(ident[-1])
        if isinstance(ident, str):
            return ident
        kwargs_block = serialized.get("kwargs")
        if isinstance(kwargs_block, dict) and kwargs_block.get("name"):
            return str(kwargs_block["name"])
    return default


def _model_of(
    serialized: dict[str, Any] | None,
    kwargs: dict[str, Any],
    metadata: dict[str, Any] | None,
) -> str | None:
    for source in (metadata or {}, kwargs, (serialized or {}).get("kwargs") or {}):
        if not isinstance(source, dict):
            continue
        for key in ("ls_model_name", "model_name", "model", "model_id", "deployment_name"):
            value = source.get(key)
            if isinstance(value, str) and value:
                return value
    invocation = kwargs.get("invocation_params")
    if isinstance(invocation, dict):
        for key in ("model_name", "model", "model_id"):
            value = invocation.get(key)
            if isinstance(value, str) and value:
                return value
    return None


def _domain_attributes(metadata: dict[str, Any]) -> dict[str, Any]:
    """Lift SupportPilot's well-known metadata keys onto the span."""
    keys = (
        Attr.TICKET_ID,
        Attr.AGENT_RUN_ID,
        Attr.ORGANIZATION_ID,
        Attr.CATEGORY,
        Attr.PRIORITY,
        Attr.RISK_LEVEL,
        Attr.DECISION,
        Attr.TOOL_SCOPE,
        Attr.APPROVAL_STATE,
        Attr.CHANNEL,
    )
    return {k: metadata[k] for k in keys if metadata.get(k) is not None}


def _as_error(error: BaseException | None) -> ErrorInfo | None:
    if error is None:
        return None
    if isinstance(error, BaseException):
        return error_info(error)
    return ErrorInfo(type="Unknown", message=clip(str(error), 2_000))


def _key(run_id: Any) -> str | None:
    return None if run_id is None else str(run_id)


__all__ = [
    "DEFAULT_IGNORED_CHAINS",
    "LANGCHAIN_AVAILABLE",
    "MAX_TRACKED_RUNS",
    "ObservabilityCallbackHandler",
    "extract_generation_text",
    "extract_usage",
    "normalize_usage",
]
