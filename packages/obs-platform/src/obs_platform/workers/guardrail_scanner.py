"""Guardrail scanner -- PII and prompt injection on every event.

No sampling here. Scanning costs CPU, not tokens, and "we checked 5% of tickets
for card numbers" is not a position anyone can defend to a compliance reviewer.

Error handling is split deliberately, and this is the part worth reading:

* A failure **inside the scan** of one event is that event's problem. It is
  dead-lettered and acked, because a payload that breaks the scanner will break
  it again on every redelivery.
* A failure **writing to the database** is transient. Those messages are left
  unacked so Redis redelivers them once the database is back.

Collapsing those two into one ``except`` gives you either a poison message that
retries forever, or a database blip that silently drops a batch of findings.

The scanner also **upserts the trace row** rather than only updating it. It has
its own consumer group and can easily finish before the storage writer has
created the trace, in which case a plain UPDATE would match zero rows and the
findings would be orphaned from their trace.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select

from obs_sdk.schema import ObsEvent, Severity

from ..alerts import AlertKind, guardrail_dedupe_key, raise_alert
from ..db import insert_ignore, table_of, upsert
from ..guardrails.engine import Finding, GuardrailEngine, ScanOutcome, merge_flags
from ..logging import get_logger
from ..models import GuardrailFinding, Trace
from ..settings import Settings
from .base import BatchOutcome, StreamConsumer, StreamMessage

log = get_logger("obs_platform.guardrail")

_FINDING_CONFLICT = ["trace_id", "span_id", "detector", "finding_type", "field", "start_offset"]


class GuardrailScanner(StreamConsumer):
    """Consumer group ``obs-guardrail``."""

    role = "guardrail"

    def __init__(self, settings: Settings | None = None, **kwargs: Any) -> None:
        super().__init__(settings=settings, **kwargs)
        self.engine = GuardrailEngine(self.settings)

    async def process(self, messages: list[StreamMessage]) -> BatchOutcome:
        parsed, outcome = self.parse(messages)
        if not parsed:
            return outcome
        if not self.settings.guardrails_enabled:
            outcome.ack.extend(item.message_id for item in parsed)
            return outcome

        # -- scan: per-message failures are permanent, not transient ---------
        per_trace: dict[str, list[tuple[ObsEvent, ScanOutcome]]] = {}
        scannable = []
        for item in parsed:
            try:
                result = self.engine.scan_event(item.event)
            except Exception as exc:
                log.exception("guardrail.scan_failed", trace_id=item.event.trace_id)
                outcome.dead.append((item.message, type(exc).__name__, str(exc)[:500]))
                continue
            per_trace.setdefault(item.event.trace_id, []).append((item.event, result))
            scannable.append(item)

        if not scannable:
            return outcome

        # -- persist: a failure here is transient, so nothing is acked -------
        async with self.db.session() as session:
            for trace_id, results in per_trace.items():
                await self._persist(session, trace_id, results)

        outcome.ack.extend(item.message_id for item in scannable)
        return outcome

    # ------------------------------------------------------------------ #
    async def _persist(
        self, session: Any, trace_id: str, results: list[tuple[ObsEvent, ScanOutcome]]
    ) -> None:
        tenant_id = results[0][0].tenant_id
        findings: list[Finding] = []
        combined: dict[str, Any] = {}
        risk = 0
        severities: list[str] = []

        for _event, result in results:
            findings.extend(result.findings)
            combined = merge_flags(combined, result.flags)
            risk = max(risk, result.risk_score)
            if result.max_severity:
                severities.append(result.max_severity.value)

        if findings:
            await insert_ignore(
                session,
                table_of(GuardrailFinding),
                [f.as_row(trace_id, tenant_id) for f in findings],
                _FINDING_CONFLICT,
            )

        existing = (
            await session.execute(select(Trace).where(Trace.trace_id == trace_id))
        ).scalar_one_or_none()

        merged_flags = merge_flags(existing.guardrail_flags if existing else {}, combined)
        merged_risk = max(risk, existing.risk_score if existing else 0)
        merged_severity = (
            Severity.max_of([*severities, existing.max_severity or "info"]).value
            if existing and existing.max_severity
            else (Severity.max_of(severities).value if severities else None)
        )
        status = "flagged" if merged_flags.get("findings") else "clean"

        # Upsert, not update: this consumer group can outrun the storage writer,
        # and an UPDATE matching zero rows would strand the findings.
        await upsert(
            session,
            table_of(Trace),
            {
                "trace_id": trace_id,
                "tenant_id": tenant_id,
                "guardrail_status": status,
                "guardrail_flags": merged_flags,
                "risk_score": merged_risk,
                "max_severity": merged_severity,
                "input": {},
                "output": {},
                "attributes": {},
                "created_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            },
            index_elements=["trace_id"],
            update_columns=[
                "guardrail_status",
                "guardrail_flags",
                "risk_score",
                "max_severity",
                "updated_at",
            ],
        )

        await self._maybe_alert(session, trace_id, tenant_id, findings)

    async def _maybe_alert(
        self, session: Any, trace_id: str, tenant_id: str, findings: list[Finding]
    ) -> None:
        """Raise an alert for anything at or above the configured severity floor."""
        floor = self.settings.alert_severity_floor
        serious = [f for f in findings if Severity.at_least(f.severity, floor)]
        if not serious:
            return
        worst = max(serious, key=lambda f: (f.severity.rank, f.score))
        await raise_alert(
            session,
            tenant_id=tenant_id,
            kind=AlertKind.GUARDRAIL,
            severity=worst.severity,
            title=f"{worst.detector.upper()}: {worst.finding_type} in {worst.field}",
            detail={
                "detector": worst.detector,
                "finding_type": worst.finding_type,
                "field": worst.field,
                "excerpt": worst.excerpt,
                "score": round(worst.score, 4),
                "total_findings": len(findings),
                "all_types": sorted({f.finding_type for f in serious}),
            },
            trace_id=trace_id,
            dedupe_key=guardrail_dedupe_key(trace_id, worst.detector, worst.finding_type),
        )


__all__ = ["GuardrailScanner"]
