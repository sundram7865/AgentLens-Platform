"""Manual tracing API for code that is not a LangChain run.

SupportPilot's most security-relevant work happens *outside* the LangChain
callback surface: the tool gateway, approval decisions, provider HTTP calls, the
SLA job. Those need spans too, and they need to land in the same trace as the
agent's LLM calls -- otherwise the trace detail view shows an agent that
apparently decided to refund someone with no tool call attached.

:class:`Tracer` and :class:`ObservabilityCallbackHandler` share one publisher and
one contextvar-backed span stack, so spans from both sources join the same tree.
"""

from __future__ import annotations

import contextlib
import logging
import time
import traceback
from collections.abc import Iterator
from typing import Any

from .context import (
    TraceContext,
    current_span_id,
    get_trace_context,
    reset_current_span_id,
    reset_trace_context,
    set_current_span_id,
    set_trace_context,
)
from .publisher import NullPublisher, Publisher
from .schema import (
    MAX_STACK_CHARS,
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

logger = logging.getLogger("obs_sdk.tracer")


class SpanHandle:
    """A span that is currently open. Mutate it, then let the context manager close it."""

    __slots__ = (
        "_ended",
        "_started_perf",
        "attributes",
        "error",
        "input",
        "kind",
        "model",
        "name",
        "output",
        "parent_span_id",
        "span_id",
        "started_at",
        "status",
        "trace_id",
        "usage",
    )

    def __init__(
        self,
        trace_id: str,
        span_id: str,
        parent_span_id: str | None,
        name: str,
        kind: SpanKind,
    ) -> None:
        self.trace_id = trace_id
        self.span_id = span_id
        self.parent_span_id = parent_span_id
        self.name = name
        self.kind = kind
        self.input: dict[str, Any] = {}
        self.output: dict[str, Any] = {}
        self.attributes: dict[str, Any] = {}
        self.usage: Usage | None = None
        self.model: str | None = None
        self.error: ErrorInfo | None = None
        self.status = SpanStatus.RUNNING
        self.started_at = utcnow()
        self._started_perf = time.perf_counter()
        self._ended = False

    # -- mutation ----------------------------------------------------------- #
    def set_input(self, **kwargs: Any) -> SpanHandle:
        self.input.update(kwargs)
        return self

    def set_output(self, **kwargs: Any) -> SpanHandle:
        self.output.update(kwargs)
        return self

    def set_attributes(self, **kwargs: Any) -> SpanHandle:
        self.attributes.update(kwargs)
        return self

    def set_usage(
        self, prompt_tokens: int = 0, completion_tokens: int = 0, total_tokens: int = 0
    ) -> SpanHandle:
        self.usage = Usage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
        )
        return self

    def set_model(self, model: str | None) -> SpanHandle:
        self.model = model
        return self

    def set_error(self, exc: BaseException) -> SpanHandle:
        self.error = error_info(exc)
        self.status = SpanStatus.ERROR
        return self

    @property
    def elapsed_ms(self) -> int:
        return max(0, int((time.perf_counter() - self._started_perf) * 1000))


def error_info(exc: BaseException) -> ErrorInfo:
    """Build an :class:`ErrorInfo`, truncating the traceback at the producer."""
    stack = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return ErrorInfo(
        type=type(exc).__name__,
        message=clip(str(exc), 2_000),
        stack=clip(stack, MAX_STACK_CHARS),
    )


class Tracer:
    """Emits trace and span events for hand-instrumented code."""

    def __init__(
        self,
        publisher: Publisher | None = None,
        tenant_id: str = "default",
        service: str = "unknown",
        environment: str = "local",
        capture_content: bool = True,
    ) -> None:
        self.publisher: Publisher = publisher or NullPublisher()
        self.tenant_id = tenant_id
        self.service = service
        self.environment = environment
        self.capture_content = capture_content

    # -- context managers --------------------------------------------------- #
    @contextlib.contextmanager
    def trace(
        self,
        name: str = "request",
        trace_id: str | None = None,
        tenant_id: str | None = None,
        session_id: str | None = None,
        user_ref: str | None = None,
        input: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> Iterator[TraceContext]:
        """Open a trace. Emits ``trace.start`` on entry and ``trace.end`` on exit.

        The ``trace.end`` event is emitted from a ``finally``: a request that
        raises is exactly the request you most need to see in the dashboard.
        """
        ctx = TraceContext(
            trace_id=trace_id or new_trace_id(),
            tenant_id=tenant_id or self.tenant_id,
            session_id=session_id,
            user_ref=user_ref,
            attributes=dict(attributes or {}),
        )
        token = set_trace_context(ctx)
        span_token = set_current_span_id(None)
        started = time.perf_counter()
        self._emit(
            ObsEvent(
                type=EventType.TRACE_START,
                trace_id=ctx.trace_id,
                tenant_id=ctx.tenant_id,
                name=name,
                kind=SpanKind.AGENT,
                status=SpanStatus.RUNNING,
                started_at=utcnow(),
                input=self._payload(input),
                attributes=ctx.attributes,
                session_id=ctx.session_id,
                user_ref=ctx.user_ref,
                service=self.service,
                environment=self.environment,
            )
        )
        status = SpanStatus.OK
        err: ErrorInfo | None = None
        try:
            yield ctx
        except BaseException as exc:
            status = SpanStatus.ERROR
            err = error_info(exc)
            raise
        finally:
            self._emit(
                ObsEvent(
                    type=EventType.TRACE_END,
                    trace_id=ctx.trace_id,
                    tenant_id=ctx.tenant_id,
                    name=name,
                    kind=SpanKind.AGENT,
                    status=status,
                    ended_at=utcnow(),
                    latency_ms=max(0, int((time.perf_counter() - started) * 1000)),
                    output=self._payload(ctx.attributes.get("__output__")),
                    error=err,
                    attributes={k: v for k, v in ctx.attributes.items() if not k.startswith("__")},
                    session_id=ctx.session_id,
                    user_ref=ctx.user_ref,
                    service=self.service,
                    environment=self.environment,
                )
            )
            reset_current_span_id(span_token)
            reset_trace_context(token)

    @contextlib.contextmanager
    def span(
        self,
        name: str,
        kind: SpanKind | str = SpanKind.OTHER,
        input: dict[str, Any] | None = None,
        attributes: dict[str, Any] | None = None,
        trace_id: str | None = None,
        tenant_id: str | None = None,
    ) -> Iterator[SpanHandle]:
        """Open a span, parented to whatever span is currently open."""
        ctx = get_trace_context()
        resolved_trace = trace_id or (ctx.trace_id if ctx else new_trace_id())
        resolved_tenant = tenant_id or (ctx.tenant_id if ctx else self.tenant_id)
        handle = SpanHandle(
            trace_id=resolved_trace,
            span_id=new_span_id(),
            parent_span_id=current_span_id(),
            name=name,
            kind=SpanKind(kind),
        )
        if input:
            handle.input.update(input)
        if attributes:
            handle.attributes.update(attributes)

        self._emit(self._span_event(handle, EventType.SPAN_START, resolved_tenant, ctx))
        span_token = set_current_span_id(handle.span_id)
        try:
            yield handle
            if handle.status is SpanStatus.RUNNING:
                handle.status = SpanStatus.OK
        except BaseException as exc:
            handle.set_error(exc)
            raise
        finally:
            reset_current_span_id(span_token)
            handle._ended = True
            self._emit(self._span_event(handle, EventType.SPAN_END, resolved_tenant, ctx))

    # -- internals ---------------------------------------------------------- #
    def _span_event(
        self,
        handle: SpanHandle,
        event_type: EventType,
        tenant_id: str,
        ctx: TraceContext | None,
    ) -> ObsEvent:
        is_end = event_type is EventType.SPAN_END
        return ObsEvent(
            type=event_type,
            trace_id=handle.trace_id,
            span_id=handle.span_id,
            parent_span_id=handle.parent_span_id,
            tenant_id=tenant_id,
            name=handle.name,
            kind=handle.kind,
            status=handle.status,
            started_at=handle.started_at,
            ended_at=utcnow() if is_end else None,
            latency_ms=handle.elapsed_ms if is_end else None,
            input=self._payload(handle.input),
            output=self._payload(handle.output) if is_end else {},
            error=handle.error if is_end else None,
            usage=handle.usage if is_end else None,
            model=handle.model,
            attributes=handle.attributes,
            session_id=ctx.session_id if ctx else None,
            user_ref=ctx.user_ref if ctx else None,
            service=self.service,
            environment=self.environment,
        )

    def _payload(self, value: Any) -> dict[str, Any]:
        """Normalise a payload and honour ``capture_content``.

        With capture off we keep shape and size but not content, so latency,
        token and error analysis all still work for a tenant that will not let
        prompt text leave their process.
        """
        if value is None:
            return {}
        data = value if isinstance(value, dict) else {"value": value}
        if self.capture_content:
            return {k: _clip_value(v) for k, v in data.items()}
        return {
            "__redacted_by_producer__": True,
            "keys": sorted(str(k) for k in data),
            "approx_chars": sum(len(str(v)) for v in data.values()),
        }

    def _emit(self, event: ObsEvent) -> None:
        # Nothing in this module may raise into the host application.
        try:
            self.publisher.publish(event)
        except Exception:  # pragma: no cover - defensive
            logger.debug("obs-sdk: emit failed", exc_info=True)


def _clip_value(value: Any) -> Any:
    if isinstance(value, str):
        return clip(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_clip_value(v) for v in list(value)[:50]]
    if isinstance(value, dict):
        return {str(k): _clip_value(v) for k, v in list(value.items())[:50]}
    return clip(value)


__all__ = ["Attr", "SpanHandle", "Tracer", "error_info"]
