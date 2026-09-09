"""API response models.

Response shapes are declared, not returned ad hoc from ORM objects. That is what
makes redaction enforceable: a viewer's response is built from these models
after the redactor has run over the payload, so there is no path where an
un-redacted column leaks just because someone added it to a table.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    """A keyset-paginated page.

    ``next_cursor`` instead of a page number, and no ``total``. Both are
    deliberate: ``OFFSET n`` makes the database walk and discard n rows (so page
    500 is 500x the work of page 1), and rows shifting under an offset silently
    duplicate or skip entries. ``SELECT count(*)`` over a growing traces table
    is the other half of the same problem -- an unbounded scan on every list
    request. ``has_more`` answers the only question the UI actually needs.
    """

    items: list[T]
    next_cursor: str | None = None
    has_more: bool = False
    limit: int


class UsageOut(BaseModel):
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0


class TraceSummary(BaseModel):
    trace_id: str
    tenant_id: str
    name: str
    service: str
    environment: str
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    latency_ms: int | None = None
    model: str | None = None
    usage: UsageOut = Field(default_factory=UsageOut)
    span_count: int = 0
    llm_calls: int = 0
    tool_calls: int = 0
    error_count: int = 0
    guardrail_status: str = "pending"
    risk_score: int = 0
    max_severity: str | None = None
    eval_status: str = "not_sampled"
    input_preview: str = ""
    output_preview: str = ""
    attributes: dict[str, Any] = Field(default_factory=dict)
    redacted: bool = False


class SpanOut(BaseModel):
    span_id: str
    parent_span_id: str | None = None
    kind: str
    name: str
    status: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    latency_ms: int | None = None
    model: str | None = None
    usage: UsageOut = Field(default_factory=UsageOut)
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)
    depth: int = 0


class GuardrailFindingOut(BaseModel):
    id: int
    span_id: str = ""
    detector: str
    finding_type: str
    severity: str
    score: float
    field: str
    excerpt: str
    created_at: datetime


class EvalScoreOut(BaseModel):
    metric: str
    score: float
    reason: str = ""
    backend: str = ""
    judge_model: str = ""
    cost_usd: float = 0.0
    created_at: datetime


class TraceDetail(TraceSummary):
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    guardrail_flags: dict[str, Any] = Field(default_factory=dict)
    spans: list[SpanOut] = Field(default_factory=list)
    findings: list[GuardrailFindingOut] = Field(default_factory=list)
    scores: list[EvalScoreOut] = Field(default_factory=list)


class AlertOut(BaseModel):
    id: int
    tenant_id: str
    trace_id: str | None = None
    kind: str
    severity: str
    title: str
    detail: dict[str, Any] = Field(default_factory=dict)
    status: str
    acknowledged_by: str | None = None
    acknowledged_at: datetime | None = None
    created_at: datetime


class TimeBucket(BaseModel):
    bucket: datetime
    traces: int = 0
    errors: int = 0
    p50_latency_ms: float | None = None
    p95_latency_ms: float | None = None
    total_tokens: int = 0
    cost_usd: float = 0.0


class MetricsOverview(BaseModel):
    window_hours: int
    tenant_id: str | None = None
    traces: int = 0
    errors: int = 0
    error_rate: float = 0.0
    flagged: int = 0
    flagged_rate: float = 0.0
    p50_latency_ms: float | None = None
    p95_latency_ms: float | None = None
    p99_latency_ms: float | None = None
    total_tokens: int = 0
    cost_usd: float = 0.0
    eval_scores: dict[str, float] = Field(default_factory=dict)
    open_alerts: int = 0


class DriftPointOut(BaseModel):
    metric: str
    window_start: datetime
    window_end: datetime
    mean: float
    sample_count: int
    baseline_mean: float | None = None
    z_score: float | None = None
    drifted: bool = False
    created_at: datetime


class TenantOut(BaseModel):
    tenant_id: str
    traces: int = 0
    last_seen_at: datetime | None = None


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    role: str
    email: str
    tenant_id: str | None = None


class MeResponse(BaseModel):
    id: str
    email: str
    role: str
    tenant_id: str | None = None
    can_view_raw: bool = False


class IngestResponse(BaseModel):
    accepted: int
    dropped: int = 0


__all__ = [
    "AlertOut",
    "DriftPointOut",
    "EvalScoreOut",
    "GuardrailFindingOut",
    "IngestResponse",
    "LoginRequest",
    "MeResponse",
    "MetricsOverview",
    "Page",
    "SpanOut",
    "TenantOut",
    "TimeBucket",
    "TokenResponse",
    "TraceDetail",
    "TraceSummary",
    "UsageOut",
]
