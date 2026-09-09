"""Retention.

``MAXLEN ~`` bounds Redis. Nothing bounds Postgres unless something deletes, so
without this job the traces table grows forever and the Neon free tier's 0.5 GB
runs out quietly -- writes start failing, and the observability platform is the
last thing anyone thinks to check.

Two implementation choices worth stating:

**It lives in the worker, not in the database.** Neon's free tier scales compute
to zero when idle, and ``pg_cron`` jobs only fire while the compute is awake --
so a suspended instance silently skips every scheduled run and nobody finds out
until the disk is full. A job inside a process we control does not have that
failure mode.

**It deletes in bounded batches.** ``DELETE FROM traces WHERE created_at < ...``
in one statement takes a long transaction and a lot of locks the first time it
runs against months of accumulated data. Batching keeps each transaction short
and leaves the ingestion path responsive while it works.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..db import Database
from ..logging import get_logger
from ..models import (
    Alert,
    AuditLog,
    DeadLetter,
    DriftSnapshot,
    EvalScore,
    GuardrailFinding,
    Span,
    Trace,
)
from ..settings import Settings

log = get_logger("obs_platform.retention")


@dataclass
class RetentionReport:
    cutoff: datetime
    deleted: dict[str, int] = field(default_factory=dict)
    batches: int = 0

    @property
    def total(self) -> int:
        return sum(self.deleted.values())

    def as_dict(self) -> dict[str, Any]:
        return {
            "cutoff": self.cutoff.isoformat(),
            "deleted": self.deleted,
            "total": self.total,
            "batches": self.batches,
        }


# Child rows first: a trace's spans and findings are meaningless once the trace
# is gone, and deleting the parent first would strand them (there is no FK
# cascade -- the ingestion path must be able to write a span before its trace
# row exists).
CHILD_TABLES = (
    ("spans", Span, Span.trace_id),
    ("guardrail_findings", GuardrailFinding, GuardrailFinding.trace_id),
    ("eval_scores", EvalScore, EvalScore.trace_id),
)

# Tables aged independently of any trace.
STANDALONE_TABLES = (
    ("drift_snapshots", DriftSnapshot, DriftSnapshot.created_at),
    ("dead_letters", DeadLetter, DeadLetter.created_at),
    ("alerts", Alert, Alert.created_at),
    ("audit_log", AuditLog, AuditLog.created_at),
)

# The audit trail outlives the data it describes: "who looked at this trace" is
# the compliance artifact, and it is small.
AUDIT_RETENTION_MULTIPLIER = 3


async def purge(
    database: Database, settings: Settings, now: datetime | None = None
) -> RetentionReport:
    """Delete everything older than the retention window, in bounded batches."""
    moment = now or datetime.now(UTC)
    cutoff = moment - timedelta(days=settings.retention_days)
    audit_cutoff = moment - timedelta(days=settings.retention_days * AUDIT_RETENTION_MULTIPLIER)
    report = RetentionReport(cutoff=cutoff)
    batch_size = settings.retention_batch_size

    while True:
        async with database.session() as session:
            trace_ids = list(
                (
                    await session.execute(
                        select(Trace.trace_id).where(Trace.created_at < cutoff).limit(batch_size)
                    )
                ).scalars()
            )
            if not trace_ids:
                break
            for name, model, column in CHILD_TABLES:
                result = await session.execute(delete(model).where(column.in_(trace_ids)))
                report.deleted[name] = report.deleted.get(name, 0) + int(result.rowcount or 0)
            result = await session.execute(delete(Trace).where(Trace.trace_id.in_(trace_ids)))
            report.deleted["traces"] = report.deleted.get("traces", 0) + int(result.rowcount or 0)
            report.batches += 1

        if len(trace_ids) < batch_size:
            break
        # A pathological clock or misconfiguration should not spin forever.
        if report.batches > 1000:
            log.warning("retention.batch_limit_reached", batches=report.batches)
            break

    async with database.session() as session:
        for table_name, table_model, age_column in STANDALONE_TABLES:
            table_cutoff = audit_cutoff if table_name == "audit_log" else cutoff
            result = await session.execute(delete(table_model).where(age_column < table_cutoff))
            report.deleted[table_name] = report.deleted.get(table_name, 0) + int(
                result.rowcount or 0
            )

    log.info("retention.completed", **report.as_dict())
    return report


async def estimate(session: AsyncSession, settings: Settings) -> dict[str, int]:
    """How many rows the next run would remove. Used by the admin endpoint."""
    from sqlalchemy import func

    cutoff = datetime.now(UTC) - timedelta(days=settings.retention_days)
    traces = (
        await session.execute(
            select(func.count()).select_from(Trace).where(Trace.created_at < cutoff)
        )
    ).scalar() or 0
    spans = (
        await session.execute(
            select(func.count()).select_from(Span).where(Span.created_at < cutoff)
        )
    ).scalar() or 0
    return {"traces": int(traces), "spans": int(spans), "retention_days": settings.retention_days}


__all__ = ["AUDIT_RETENTION_MULTIPLIER", "RetentionReport", "estimate", "purge"]
