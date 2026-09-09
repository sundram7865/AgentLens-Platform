"""ORM rows -> API models.

Every trace payload passes through here on its way out, and the optional
``redactor`` is applied *while the response body is being built* -- not in the
browser, and not by hiding a field in the UI. An API call that bypasses the
dashboard entirely gets the same redacted bytes, because there is no other path
to a trace payload.
"""

from __future__ import annotations

from typing import Any, Protocol

from ..models import Alert, EvalScore, GuardrailFinding, Span, Trace
from ..pricing import micros_to_usd
from .schemas import (
    AlertOut,
    EvalScoreOut,
    GuardrailFindingOut,
    SpanOut,
    TraceDetail,
    TraceSummary,
    UsageOut,
)

PREVIEW_CHARS = 240


class Redactor(Protocol):
    """Applied to every text and payload leaving the API.

    ``full`` distinguishes "PII was masked for this viewer" from "only secrets
    were masked", which is what the response's ``redacted`` flag reports and
    what the audit row records.
    """

    full: bool

    def redact_text(self, value: str) -> str: ...

    def redact_payload(self, value: dict[str, Any]) -> dict[str, Any]: ...


def _text(value: Any, redactor: Redactor | None) -> str:
    text = value if isinstance(value, str) else str(value)
    return redactor.redact_text(text) if redactor else text


def _payload(value: dict[str, Any] | None, redactor: Redactor | None) -> dict[str, Any]:
    data = value or {}
    return redactor.redact_payload(data) if redactor else data


def preview_text(payload: dict[str, Any] | None, redactor: Redactor | None = None) -> str:
    """A short human-readable stand-in for a payload, for list rows."""
    if not payload:
        return ""
    for key in ("question", "query", "prompt", "prompts", "messages", "input", "text", "value"):
        if key in payload:
            candidate = payload[key]
            break
    else:
        candidate = next(iter(payload.values()), "")

    if isinstance(candidate, list):
        candidate = candidate[0] if candidate else ""
    if isinstance(candidate, dict):
        candidate = next(iter(candidate.values()), "")
    if candidate is None:
        # str(None) is "None", which renders as that literal word in the trace
        # list. A missing preview should be blank, not the repr of nothing.
        return ""
    text = str(candidate).strip().replace("\n", " ")
    if len(text) > PREVIEW_CHARS:
        text = text[:PREVIEW_CHARS] + "..."
    return _text(text, redactor)


def usage_of(prompt: int, completion: int, total: int, cost_micros: int) -> UsageOut:
    return UsageOut(
        prompt_tokens=prompt,
        completion_tokens=completion,
        total_tokens=total,
        cost_usd=micros_to_usd(cost_micros),
    )


def trace_summary(trace: Trace, redactor: Redactor | None = None) -> TraceSummary:
    return TraceSummary(
        trace_id=trace.trace_id,
        tenant_id=trace.tenant_id,
        name=trace.name,
        service=trace.service,
        environment=trace.environment,
        status=trace.status,
        started_at=trace.started_at,
        ended_at=trace.ended_at,
        latency_ms=trace.latency_ms,
        model=trace.model,
        usage=usage_of(
            trace.prompt_tokens, trace.completion_tokens, trace.total_tokens, trace.cost_micros
        ),
        span_count=trace.span_count,
        llm_calls=trace.llm_calls,
        tool_calls=trace.tool_calls,
        error_count=trace.error_count,
        guardrail_status=trace.guardrail_status,
        risk_score=trace.risk_score,
        max_severity=trace.max_severity,
        eval_status=trace.eval_status,
        input_preview=preview_text(trace.input, redactor),
        output_preview=preview_text(trace.output, redactor),
        attributes=_payload(trace.attributes, redactor),
        redacted=bool(getattr(redactor, "full", False)),
    )


def span_out(span: Span, depth: int = 0, redactor: Redactor | None = None) -> SpanOut:
    error = span.error
    if error and redactor:
        error = redactor.redact_payload(error)
    return SpanOut(
        span_id=span.span_id,
        parent_span_id=span.parent_span_id,
        kind=span.kind,
        name=span.name,
        status=span.status,
        started_at=span.started_at,
        ended_at=span.ended_at,
        latency_ms=span.latency_ms,
        model=span.model,
        usage=usage_of(
            span.prompt_tokens, span.completion_tokens, span.total_tokens, span.cost_micros
        ),
        input=_payload(span.input, redactor),
        output=_payload(span.output, redactor),
        error=error,
        attributes=_payload(span.attributes, redactor),
        depth=depth,
    )


def order_spans(spans: list[Span]) -> list[tuple[Span, int]]:
    """Depth-first order with nesting depth, so the UI can render a call tree.

    Orphans (a span whose parent was dropped, trimmed, or never arrived) are
    emitted at the end at depth 0 rather than discarded -- losing a tool call
    because its parent chain is incomplete would hide exactly the runs that went
    wrong.
    """
    by_parent: dict[str | None, list[Span]] = {}
    known = {span.span_id for span in spans}
    for span in spans:
        parent = span.parent_span_id if span.parent_span_id in known else None
        by_parent.setdefault(parent, []).append(span)
    for children in by_parent.values():
        children.sort(key=lambda s: (s.started_at is None, s.started_at, s.span_id))

    ordered: list[tuple[Span, int]] = []
    visited: set[str] = set()

    def walk(parent: str | None, depth: int) -> None:
        for span in by_parent.get(parent, []):
            if span.span_id in visited:  # defensive: a cycle would hang the walk
                continue
            visited.add(span.span_id)
            ordered.append((span, depth))
            walk(span.span_id, depth + 1)

    walk(None, 0)
    for span in spans:
        if span.span_id not in visited:
            ordered.append((span, 0))
    return ordered


def finding_out(finding: GuardrailFinding) -> GuardrailFindingOut:
    # `excerpt` is stored already masked, so it needs no redactor here.
    return GuardrailFindingOut(
        id=finding.id,
        span_id=finding.span_id,
        detector=finding.detector,
        finding_type=finding.finding_type,
        severity=finding.severity,
        score=finding.score,
        field=finding.field,
        excerpt=finding.excerpt,
        created_at=finding.created_at,
    )


def score_out(score: EvalScore) -> EvalScoreOut:
    return EvalScoreOut(
        metric=score.metric,
        score=score.score,
        reason=score.reason,
        backend=score.backend,
        judge_model=score.judge_model,
        cost_usd=micros_to_usd(score.cost_micros),
        created_at=score.created_at,
    )


def alert_out(alert: Alert) -> AlertOut:
    return AlertOut(
        id=alert.id,
        tenant_id=alert.tenant_id,
        trace_id=alert.trace_id,
        kind=alert.kind,
        severity=alert.severity,
        title=alert.title,
        detail=alert.detail or {},
        status=alert.status,
        acknowledged_by=alert.acknowledged_by,
        acknowledged_at=alert.acknowledged_at,
        created_at=alert.created_at,
    )


def trace_detail(
    trace: Trace,
    spans: list[Span],
    findings: list[GuardrailFinding],
    scores: list[EvalScore],
    redactor: Redactor | None = None,
) -> TraceDetail:
    summary = trace_summary(trace, redactor)
    return TraceDetail(
        **summary.model_dump(),
        input=_payload(trace.input, redactor),
        output=_payload(trace.output, redactor),
        guardrail_flags=trace.guardrail_flags or {},
        spans=[span_out(span, depth, redactor) for span, depth in order_spans(spans)],
        findings=[finding_out(f) for f in findings],
        scores=[score_out(s) for s in scores],
    )


__all__ = [
    "Redactor",
    "alert_out",
    "finding_out",
    "order_spans",
    "preview_text",
    "score_out",
    "span_out",
    "trace_detail",
    "trace_summary",
    "usage_of",
]
