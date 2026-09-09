"""Canonical trace-event schema.

This module is the **single source of truth** for the wire format between the
SDK (producer, running inside SupportPilot) and every platform consumer
(reader). Both sides import these exact models; there is no second copy that
can drift.

Versioning contract
-------------------
``SCHEMA_VERSION`` is bumped whenever the payload shape changes. Every event
carries its version, so one stream can hold a mix of old and new events during
a rolling deploy without a consumer crashing.

* Consumers parse with :func:`parse_event`, which walks any payload at or above
  ``MIN_SUPPORTED_SCHEMA_VERSION`` forward to the current shape before validating.
* A payload from the *future* (newer producer, older consumer, mid-deploy) is
  parsed leniently: unknown fields are preserved via ``extra="allow"`` instead
  of raising. That tolerance is precisely what lets two consumer versions run
  side by side during a rollout.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

SCHEMA_VERSION = 2
"""Current wire format version. Bump on any payload shape change."""

MIN_SUPPORTED_SCHEMA_VERSION = 1
"""Oldest version consumers can still upgrade. Below this, events are dead-lettered."""

SDK_VERSION = "1.0.0"

# Hard caps applied by the producer before an event leaves the process. An
# unbounded prompt on a bounded free-tier stream is how you lose every *other*
# event in the buffer.
MAX_TEXT_CHARS = 8_000
MAX_STACK_CHARS = 2_000


def utcnow() -> datetime:
    """Timezone-aware UTC now. Naive datetimes are a portability bug across DB drivers."""
    return datetime.now(UTC)


def new_trace_id() -> str:
    """Identifier for one end-to-end request. Generate once, per incoming request."""
    return "tr_" + uuid.uuid4().hex


def new_span_id() -> str:
    """Identifier for one unit of work inside a trace."""
    return "sp_" + uuid.uuid4().hex[:16]


def new_event_id() -> str:
    return "ev_" + uuid.uuid4().hex[:16]


class EventType(str, Enum):
    """``span.start``/``span.end`` bracket a unit of work; ``trace.*`` brackets the request."""

    TRACE_START = "trace.start"
    TRACE_END = "trace.end"
    SPAN_START = "span.start"
    SPAN_END = "span.end"


class SpanKind(str, Enum):
    """What kind of work a span represents. Mirrors OpenTelemetry's gen-ai span kinds."""

    AGENT = "agent"
    CHAIN = "chain"
    LLM = "llm"
    TOOL = "tool"
    RETRIEVER = "retriever"
    GUARDRAIL = "guardrail"
    OTHER = "other"


class SpanStatus(str, Enum):
    RUNNING = "running"
    OK = "ok"
    ERROR = "error"


class Severity(str, Enum):
    """One severity ladder shared by guardrail findings, alerts and drift signals."""

    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return SEVERITY_RANK[self.value]

    @classmethod
    def at_least(cls, value: Severity | str, floor: Severity | str) -> bool:
        """True when ``value`` is at or above ``floor`` on the ladder."""
        return SEVERITY_RANK[cls(value).value] >= SEVERITY_RANK[cls(floor).value]

    @classmethod
    def max_of(cls, values: list[Severity | str]) -> Severity:
        """Highest severity in a list, tolerating values it does not recognise.

        Inputs reach this from persisted JSON (a trace's accumulated
        ``guardrail_flags``) and from database columns, so one unexpected string
        -- a hand-edited row, or a value written by a newer version mid-rollout
        -- must not raise. Raising would abort the whole guardrail batch, leave
        its messages unacked, and eventually dead-letter perfectly good events
        because of one bad neighbour. Unknown values are ignored.
        """
        best = cls.INFO
        for value in values:
            try:
                candidate = cls(value)
            except ValueError:
                continue
            if SEVERITY_RANK[candidate.value] > SEVERITY_RANK[best.value]:
                best = candidate
        return best


SEVERITY_RANK: dict[str, int] = {"info": 0, "low": 1, "medium": 2, "high": 3, "critical": 4}


class Attr:
    """Well-known ``attributes`` keys.

    The event schema stays domain-agnostic -- it must, so it can watch any agent
    -- but the platform's UI gives these particular keys first-class treatment
    because they are what SupportPilot emits. Anything else still lands in the
    attributes blob and is queryable; it just is not a dedicated column.
    """

    TICKET_ID = "ticket_id"
    AGENT_RUN_ID = "agent_run_id"
    ORGANIZATION_ID = "organization_id"
    CATEGORY = "category"
    PRIORITY = "priority"
    RISK_LEVEL = "risk_level"
    DECISION = "decision"
    TOOL_SCOPE = "tool_scope"
    APPROVAL_STATE = "approval_state"
    CHANNEL = "channel"


def clip(text: Any, limit: int = MAX_TEXT_CHARS) -> str:
    """Coerce to string and truncate, marking the truncation so nobody reads a
    clipped prompt as the whole prompt."""
    if text is None:
        return ""
    s = text if isinstance(text, str) else str(text)
    if len(s) <= limit:
        return s
    return s[:limit] + f"...[truncated {len(s) - limit} chars]"


class Usage(BaseModel):
    """Token accounting for one LLM call.

    Cost is *not* set here. The platform owns the price table so a vendor price
    change never requires redeploying SupportPilot.
    """

    model_config = ConfigDict(extra="allow")

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0

    @field_validator("prompt_tokens", "completion_tokens", "total_tokens", mode="before")
    @classmethod
    def _coerce(cls, v: Any) -> int:
        # Providers omit these or send null. A missing count is zero, not a crash.
        if v is None:
            return 0
        try:
            return max(0, int(v))
        except (TypeError, ValueError):
            return 0

    def model_post_init(self, __context: Any) -> None:
        if not self.total_tokens:
            self.total_tokens = self.prompt_tokens + self.completion_tokens


class ErrorInfo(BaseModel):
    model_config = ConfigDict(extra="allow")

    type: str = "Exception"
    message: str = ""
    stack: str | None = None

    @field_validator("message")
    @classmethod
    def _clip_message(cls, v: str) -> str:
        return clip(v, 2_000)

    @field_validator("stack")
    @classmethod
    def _clip_stack(cls, v: str | None) -> str | None:
        return clip(v, MAX_STACK_CHARS) if v else v


class ObsEvent(BaseModel):
    """One observability event on the wire.

    Flat on purpose. A discriminated union reads better but forces every consumer
    to know every variant -- exactly the coupling that breaks rolling deploys.
    """

    # extra="allow" is load-bearing: it is what lets an old consumer read a newer
    # producer's events instead of dying on an unknown field.
    model_config = ConfigDict(extra="allow")

    schema_version: int = SCHEMA_VERSION
    event_id: str = Field(default_factory=new_event_id)
    emitted_at: datetime = Field(default_factory=utcnow)

    # Identity ---------------------------------------------------------------
    tenant_id: str = "default"
    trace_id: str
    span_id: str | None = None
    parent_span_id: str | None = None

    # Classification ---------------------------------------------------------
    type: EventType
    kind: SpanKind = SpanKind.OTHER
    name: str = ""
    status: SpanStatus = SpanStatus.RUNNING

    # Timing -----------------------------------------------------------------
    started_at: datetime | None = None
    ended_at: datetime | None = None
    latency_ms: int | None = None

    # Payload ----------------------------------------------------------------
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    error: ErrorInfo | None = None
    usage: Usage | None = None
    model: str | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    # Provenance -------------------------------------------------------------
    session_id: str | None = None
    user_ref: str | None = None
    environment: str = "local"
    service: str = "unknown"
    sdk_version: str = SDK_VERSION

    @field_validator("emitted_at", "started_at", "ended_at")
    @classmethod
    def _ensure_tz(cls, v: datetime | None) -> datetime | None:
        if v is not None and v.tzinfo is None:
            return v.replace(tzinfo=UTC)
        return v

    @field_validator("tenant_id")
    @classmethod
    def _tenant_not_blank(cls, v: str) -> str:
        # A blank tenant would silently pool several customers' traces into one
        # bucket, which then leaks across the RBAC boundary. Fail loud instead.
        v = (v or "").strip()
        return v or "default"

    def computed_latency_ms(self) -> int | None:
        if self.latency_ms is not None:
            return self.latency_ms
        if self.started_at and self.ended_at:
            return max(0, int((self.ended_at - self.started_at).total_seconds() * 1000))
        return None

    def to_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), separators=(",", ":"), default=str)


class SchemaError(Exception):
    """Unparseable event. Goes to the dead-letter table, never back onto the retry path."""


class SchemaTooOldError(SchemaError):
    pass


# --------------------------------------------------------------------------- #
# Version upgrades
# --------------------------------------------------------------------------- #
def _upgrade_v1_to_v2(payload: dict[str, Any]) -> dict[str, Any]:
    """v1 carried flat token counts, a ``node`` name and an ``event_type`` key.

    v2 nests tokens under ``usage`` and renames the other two. Keep this function
    forever, even after every v1 producer is gone: replayed history is still v1.
    """
    out = dict(payload)
    if "usage" not in out and any(
        k in out for k in ("prompt_tokens", "completion_tokens", "total_tokens")
    ):
        out["usage"] = {
            "prompt_tokens": out.pop("prompt_tokens", 0),
            "completion_tokens": out.pop("completion_tokens", 0),
            "total_tokens": out.pop("total_tokens", 0),
        }
    if "name" not in out and "node" in out:
        out["name"] = out.pop("node")
    if "type" not in out and "event_type" in out:
        out["type"] = out.pop("event_type")
    out["schema_version"] = 2
    return out


_UPGRADES: dict[int, Any] = {1: _upgrade_v1_to_v2}


def upgrade_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Walk a payload forward to the current schema version.

    Newer-than-current payloads are returned untouched; ``extra="allow"`` absorbs
    fields this consumer has not learned about yet.
    """
    raw_version = payload.get("schema_version", 1)
    try:
        version = int(raw_version)
    except (TypeError, ValueError) as exc:
        raise SchemaError("non-numeric schema_version: " + repr(raw_version)) from exc

    if version < MIN_SUPPORTED_SCHEMA_VERSION:
        raise SchemaTooOldError(
            f"schema_version {version} is below the supported floor {MIN_SUPPORTED_SCHEMA_VERSION}"
        )

    current = payload
    for _ in range(16):
        if version >= SCHEMA_VERSION:
            return current
        upgrade = _UPGRADES.get(version)
        if upgrade is None:
            raise SchemaError(f"no upgrade path from schema_version {version}")
        current = upgrade(current)
        version = int(current.get("schema_version", version + 1))
    raise SchemaError("schema upgrade chain did not converge")


def parse_event(payload: dict[str, Any]) -> ObsEvent:
    """Parse a raw stream payload into an :class:`ObsEvent`, upgrading if needed."""
    return ObsEvent.model_validate(upgrade_payload(payload))


__all__ = [
    "MAX_STACK_CHARS",
    "MAX_TEXT_CHARS",
    "MIN_SUPPORTED_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "SDK_VERSION",
    "SEVERITY_RANK",
    "Attr",
    "ErrorInfo",
    "EventType",
    "ObsEvent",
    "SchemaError",
    "SchemaTooOldError",
    "Severity",
    "SpanKind",
    "SpanStatus",
    "Usage",
    "clip",
    "new_event_id",
    "new_span_id",
    "new_trace_id",
    "parse_event",
    "upgrade_payload",
    "utcnow",
]
