"""obs-sdk -- the client half of the AI Observability & Guardrails Platform.

One import, one line of wiring::

    from obs_sdk import Observability

    obs = Observability.from_env(service="supportpilot")

    handler = obs.handler(tenant_id=str(org_id), trace_id=trace_id)
    graph.invoke(state, config={"callbacks": [handler]})

Guarantees this package makes to the app that imports it:

* it never raises into the request path,
* it never blocks on the network in the request path,
* it works, degraded to a no-op, when no observability infrastructure exists.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from .context import (
    TraceContext,
    current_span_id,
    current_trace_id,
    get_trace_context,
    trace_context,
)
from .langchain import (
    LANGCHAIN_AVAILABLE,
    ObservabilityCallbackHandler,
    extract_usage,
    normalize_usage,
)
from .publisher import (
    DEFAULT_MAXLEN,
    DEFAULT_STREAM,
    InMemoryPublisher,
    NullPublisher,
    Publisher,
    PublisherStats,
    RedisPublisherConfig,
    RedisStreamPublisher,
    decode_event_fields,
    encode_event,
    publisher_from_env,
)
from .schema import (
    MIN_SUPPORTED_SCHEMA_VERSION,
    SCHEMA_VERSION,
    SDK_VERSION,
    Attr,
    ErrorInfo,
    EventType,
    ObsEvent,
    SchemaError,
    Severity,
    SpanKind,
    SpanStatus,
    Usage,
    new_span_id,
    new_trace_id,
    parse_event,
    upgrade_payload,
    utcnow,
)
from .tracer import SpanHandle, Tracer

__version__ = SDK_VERSION


@dataclass
class Observability:
    """Facade holding the process-wide publisher plus factories built on it."""

    publisher: Publisher
    service: str = "unknown"
    environment: str = "local"
    tenant_id: str = "default"
    capture_content: bool = True

    @classmethod
    def from_env(
        cls,
        service: str | None = None,
        env: dict[str, str] | None = None,
        **overrides: Any,
    ) -> Observability:
        e = dict(os.environ if env is None else env)
        return cls(
            publisher=publisher_from_env(e),
            service=service or e.get("OBS_SERVICE", "unknown"),
            environment=e.get("OBS_ENVIRONMENT", e.get("ENVIRONMENT", "local")),
            tenant_id=e.get("OBS_TENANT_ID", "default"),
            capture_content=e.get("OBS_CAPTURE_CONTENT", "true").strip().lower()
            not in {"0", "false", "no", "off"},
            **overrides,
        )

    @classmethod
    def disabled(cls, service: str = "unknown") -> Observability:
        return cls(publisher=NullPublisher(), service=service)

    def handler(self, **kwargs: Any) -> ObservabilityCallbackHandler:
        """A callback handler for one agent run. Cheap -- make one per request."""
        kwargs.setdefault("tenant_id", self.tenant_id)
        kwargs.setdefault("service", self.service)
        kwargs.setdefault("environment", self.environment)
        kwargs.setdefault("capture_content", self.capture_content)
        return ObservabilityCallbackHandler(self.publisher, **kwargs)

    def tracer(self, **kwargs: Any) -> Tracer:
        """A tracer for hand-instrumented code (tool gateway, jobs, HTTP calls)."""
        kwargs.setdefault("tenant_id", self.tenant_id)
        kwargs.setdefault("service", self.service)
        kwargs.setdefault("environment", self.environment)
        kwargs.setdefault("capture_content", self.capture_content)
        return Tracer(self.publisher, **kwargs)

    def stats(self) -> dict[str, Any]:
        stats = getattr(self.publisher, "stats", None)
        return stats.snapshot() if stats else {}

    def shutdown(self, timeout: float = 5.0) -> None:
        """Flush buffered events. Call from the app's shutdown hook."""
        self.publisher.close(timeout=timeout)


__all__ = [
    "DEFAULT_MAXLEN",
    "DEFAULT_STREAM",
    "LANGCHAIN_AVAILABLE",
    "MIN_SUPPORTED_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "SDK_VERSION",
    "Attr",
    "ErrorInfo",
    "EventType",
    "InMemoryPublisher",
    "NullPublisher",
    "ObsEvent",
    "Observability",
    "ObservabilityCallbackHandler",
    "Publisher",
    "PublisherStats",
    "RedisPublisherConfig",
    "RedisStreamPublisher",
    "SchemaError",
    "Severity",
    "SpanHandle",
    "SpanKind",
    "SpanStatus",
    "TraceContext",
    "Tracer",
    "Usage",
    "__version__",
    "current_span_id",
    "current_trace_id",
    "decode_event_fields",
    "encode_event",
    "extract_usage",
    "get_trace_context",
    "new_span_id",
    "new_trace_id",
    "normalize_usage",
    "parse_event",
    "publisher_from_env",
    "trace_context",
    "upgrade_payload",
    "utcnow",
]
