"""Per-request trace context.

Trace identity lives in :mod:`contextvars`, never a module global and never
``threading.local``. Under an async server one OS thread interleaves many
requests; a thread-local would hand request B the trace id of request A and
silently braid two customers' traces together. A ContextVar is copied into each
task and each thread started from it, so concurrent requests stay isolated.

The LangChain handler does not *depend* on this: it resolves trace identity from
LangChain's own ``run_id``/``parent_run_id`` lineage, which is correct however
the host app manages context. This module is the bridge for code outside a
LangChain run -- HTTP middleware, the tool gateway, a background job -- that
needs to join the same trace.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

from .schema import new_trace_id


@dataclass(frozen=True)
class TraceContext:
    """Identity carried for the duration of one request."""

    trace_id: str
    tenant_id: str = "default"
    session_id: str | None = None
    user_ref: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    def merged_attributes(self, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        out = dict(self.attributes)
        if extra:
            out.update(extra)
        return out


_trace_ctx: ContextVar[TraceContext | None] = ContextVar("obs_trace_context", default=None)
_span_id: ContextVar[str | None] = ContextVar("obs_current_span_id", default=None)


def get_trace_context() -> TraceContext | None:
    return _trace_ctx.get()


def current_trace_id() -> str | None:
    ctx = _trace_ctx.get()
    return ctx.trace_id if ctx else None


def current_span_id() -> str | None:
    """The innermost open span, used as the parent of anything started next."""
    return _span_id.get()


def set_trace_context(ctx: TraceContext) -> Token:
    return _trace_ctx.set(ctx)


def reset_trace_context(token: Token) -> None:
    _trace_ctx.reset(token)


def set_current_span_id(span_id: str | None) -> Token:
    return _span_id.set(span_id)


def reset_current_span_id(token: Token) -> None:
    _span_id.reset(token)


@contextlib.contextmanager
def trace_context(
    trace_id: str | None = None,
    tenant_id: str = "default",
    session_id: str | None = None,
    user_ref: str | None = None,
    **attributes: Any,
) -> Iterator[TraceContext]:
    """Scope a trace context. Restores the previous value even on exception."""
    ctx = TraceContext(
        trace_id=trace_id or new_trace_id(),
        tenant_id=tenant_id,
        session_id=session_id,
        user_ref=user_ref,
        attributes=attributes,
    )
    token = _trace_ctx.set(ctx)
    try:
        yield ctx
    finally:
        _trace_ctx.reset(token)


__all__ = [
    "TraceContext",
    "current_span_id",
    "current_trace_id",
    "get_trace_context",
    "reset_current_span_id",
    "reset_trace_context",
    "set_current_span_id",
    "set_trace_context",
    "trace_context",
]
