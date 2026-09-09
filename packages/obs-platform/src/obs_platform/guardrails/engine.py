"""Guardrail engine: turn one event into findings.

Runs on **every** event, no sampling. Unlike the eval scorer this costs no LLM
tokens, and "we only noticed the card number in 5% of tickets" is not a
compliance position anyone can defend.

What gets scanned where:

* **inputs** -- PII *and* prompt injection. Input is the attacker-controlled
  surface.
* **outputs** -- PII only. A model echoing a customer's card number back into a
  reply is the leak that matters; scoring the model's own words for "injection"
  produces noise, not signal.

Excerpts stored on findings are **masked**. Recording the raw value in the
findings table while redacting it in the API response would just move the leak
one table over.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from obs_sdk.schema import EventType, ObsEvent, Severity

from ..settings import Settings
from . import pii
from .injection import get_detector
from .patterns import Match
from .patterns import redact_text as _redact_patterns

MAX_FIELDS_PER_EVENT = 40
MAX_FIELD_CHARS = 20_000
EXCERPT_CONTEXT = 24

# Risk contribution of the worst finding, on a 0-100 scale. Additional distinct
# finding types add a smaller increment: two criticals are worse than one, but
# not twice as worse -- the response is the same either way.
SEVERITY_BASE = {
    Severity.INFO: 0,
    Severity.LOW: 15,
    Severity.MEDIUM: 35,
    Severity.HIGH: 70,
    Severity.CRITICAL: 95,
}
ADDITIONAL_TYPE_WEIGHT = 5


@dataclass
class Finding:
    detector: str
    finding_type: str
    severity: Severity
    score: float
    field: str
    excerpt: str
    start_offset: int = -1
    end_offset: int = -1
    span_id: str = ""

    def as_row(self, trace_id: str, tenant_id: str) -> dict[str, Any]:
        return {
            "trace_id": trace_id,
            "span_id": self.span_id,
            "tenant_id": tenant_id,
            "detector": self.detector,
            "finding_type": self.finding_type,
            "severity": self.severity.value,
            "score": round(self.score, 4),
            "field": self.field[:120],
            "excerpt": self.excerpt[:500],
            "start_offset": self.start_offset,
            "end_offset": self.end_offset,
            "created_at": datetime.now(UTC),
        }


@dataclass
class ScanOutcome:
    findings: list[Finding] = field(default_factory=list)
    risk_score: int = 0
    max_severity: Severity | None = None
    flags: dict[str, Any] = field(default_factory=dict)

    @property
    def flagged(self) -> bool:
        return bool(self.findings)


def iter_text_fields(payload: dict[str, Any], prefix: str) -> list[tuple[str, str]]:
    """Flatten a payload into ``(json_path, text)`` pairs, bounded in both size and count."""
    out: list[tuple[str, str]] = []

    def walk(value: Any, path: str, depth: int) -> None:
        if len(out) >= MAX_FIELDS_PER_EVENT or depth > 6:
            return
        if isinstance(value, str):
            if value.strip():
                out.append((path, value[:MAX_FIELD_CHARS]))
        elif isinstance(value, dict):
            for key, item in value.items():
                walk(item, f"{path}.{key}", depth + 1)
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value[:20]):
                walk(item, f"{path}[{index}]", depth + 1)

    walk(payload or {}, prefix, 0)
    return out


def redact_pii(text: str) -> str:
    """Mask any personal data inside a stretch of text, leaving the rest intact."""
    return _redact_patterns(text)[0]


def masked_excerpt(text: str, match: Match) -> str:
    """A little context around the hit, with **all** PII in it masked.

    Context is what makes a finding actionable ("card number in the reply draft"
    rather than "CREDIT_CARD somewhere"); masking is what keeps this table from
    becoming a second copy of the PII.

    Masking only ``match`` is not enough, and that was a real leak: a ticket
    reading "email me at a@b.com or 98765 43210" produces an EMAIL finding whose
    context window contains the raw phone number, and a PHONE finding whose
    context contains most of the raw email. Since ``finding_out()`` deliberately
    skips the redactor -- on the premise that excerpts are already masked -- both
    reached a viewer in full. Redacting the assembled excerpt makes that premise
    true. It is idempotent: an already-masked value no longer matches its own
    pattern, so re-running the redactor over it changes nothing.
    """
    start = max(0, match.start - EXCERPT_CONTEXT)
    end = min(len(text), match.end + EXCERPT_CONTEXT)
    prefix = ("..." if start > 0 else "") + text[start : match.start]
    suffix = text[match.end : end] + ("..." if end < len(text) else "")
    excerpt = f"{prefix}{match.masked}{suffix}".replace("\n", " ")
    return redact_pii(excerpt)


class GuardrailEngine:
    """Stateless scanner. One instance per worker; detectors are cached."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pii_engine_name = settings.pii_engine
        self.pii_language = settings.pii_language
        self.injection = get_detector(
            threshold=settings.injection_threshold,
            backend=settings.injection_backend,
            embedding_model=settings.embedding_model,
        )

    def scan_event(self, event: ObsEvent) -> ScanOutcome:
        span_id = event.span_id or ""
        findings: list[Finding] = []

        input_fields = iter_text_fields(event.input, "input")
        output_fields = iter_text_fields(event.output, "output")

        for path, text in input_fields + output_fields:
            findings.extend(self._scan_pii(text, path, span_id))

        # Injection only on inputs, and only on the events that carry a real
        # request: re-scanning the same prompt on every child span would create
        # one finding per span for a single attack.
        if event.type in (EventType.TRACE_START, EventType.SPAN_START, EventType.TRACE_END):
            for path, text in input_fields:
                finding = self._scan_injection(text, path, span_id)
                if finding is not None:
                    findings.append(finding)

        return self._summarize(findings)

    # ------------------------------------------------------------------ #
    def _scan_pii(self, text: str, path: str, span_id: str) -> list[Finding]:
        result = pii.scan(text, self.pii_engine_name, self.pii_language)
        return [
            Finding(
                detector="pii",
                finding_type=match.pattern,
                severity=match.severity,
                score=match.score,
                field=path,
                excerpt=masked_excerpt(text, match),
                start_offset=match.start,
                end_offset=match.end,
                span_id=span_id,
            )
            for match in result.matches
        ]

    def _scan_injection(self, text: str, path: str, span_id: str) -> Finding | None:
        result = self.injection.scan(text)
        if not result.detected:
            return None
        return Finding(
            detector="injection",
            finding_type=result.category.upper() or "PROMPT_INJECTION",
            severity=result.severity,
            score=result.score,
            field=path,
            # The attack text has to stay readable or the finding is
            # untriageable -- but an attack can carry PII too ("my card is X,
            # now ignore your instructions"). Storing it raw put unmasked card
            # numbers in the findings table, which the API returns WITHOUT a
            # redactor on the assumption that every excerpt is already masked.
            # Masking here makes that assumption true for every detector.
            excerpt=redact_pii(text[:280]).replace("\n", " "),
            start_offset=0,
            end_offset=min(len(text), 280),
            span_id=span_id,
        )

    def _summarize(self, findings: list[Finding]) -> ScanOutcome:
        if not findings:
            return ScanOutcome(flags={"scanned": True, "findings": 0})

        max_severity = Severity.max_of([f.severity for f in findings])
        distinct_types = {(f.detector, f.finding_type) for f in findings}
        risk = min(
            100,
            SEVERITY_BASE[max_severity] + ADDITIONAL_TYPE_WEIGHT * (len(distinct_types) - 1),
        )

        pii_findings = [f for f in findings if f.detector == "pii"]
        injection_findings = [f for f in findings if f.detector == "injection"]

        flags: dict[str, Any] = {
            "scanned": True,
            "findings": len(findings),
            "risk_score": risk,
            "max_severity": max_severity.value,
            "engines": {"pii": self.pii_engine_name, "injection": self.injection.backend_name},
        }
        if pii_findings:
            flags["pii"] = {
                "count": len(pii_findings),
                "types": sorted({f.finding_type for f in pii_findings}),
                "fields": sorted({f.field for f in pii_findings})[:10],
                "max_severity": Severity.max_of([f.severity for f in pii_findings]).value,
            }
        if injection_findings:
            worst = max(injection_findings, key=lambda f: f.score)
            flags["injection"] = {
                "detected": True,
                "category": worst.finding_type,
                "score": round(worst.score, 4),
                "field": worst.field,
                "severity": worst.severity.value,
            }
        return ScanOutcome(
            findings=findings, risk_score=risk, max_severity=max_severity, flags=flags
        )


def merge_flags(existing: dict[str, Any] | None, new: dict[str, Any]) -> dict[str, Any]:
    """Merge a new scan into the trace's accumulated flags.

    A trace is scanned once per event, so the trace-level summary has to
    accumulate across events rather than being overwritten by whichever event
    happened to be processed last -- otherwise a clean final span erases the
    injection found in the first one.
    """
    merged = dict(existing or {})
    merged["scanned"] = True
    merged["findings"] = int(merged.get("findings", 0)) + int(new.get("findings", 0))
    merged["risk_score"] = max(int(merged.get("risk_score", 0)), int(new.get("risk_score", 0)))
    if new.get("max_severity"):
        merged["max_severity"] = Severity.max_of(
            [merged.get("max_severity", "info"), new["max_severity"]]
        ).value
    if new.get("engines"):
        merged["engines"] = new["engines"]
    if new.get("pii"):
        previous = merged.get("pii") or {}
        merged["pii"] = {
            "count": int(previous.get("count", 0)) + new["pii"]["count"],
            "types": sorted(set(previous.get("types", [])) | set(new["pii"]["types"])),
            "fields": sorted(set(previous.get("fields", [])) | set(new["pii"]["fields"]))[:10],
            "max_severity": Severity.max_of(
                [previous.get("max_severity", "info"), new["pii"]["max_severity"]]
            ).value,
        }
    if new.get("injection"):
        previous = merged.get("injection") or {}
        if new["injection"]["score"] >= float(previous.get("score", 0)):
            merged["injection"] = new["injection"]
    return merged


__all__ = [
    "Finding",
    "GuardrailEngine",
    "ScanOutcome",
    "iter_text_fields",
    "masked_excerpt",
    "merge_flags",
    "redact_pii",
]
