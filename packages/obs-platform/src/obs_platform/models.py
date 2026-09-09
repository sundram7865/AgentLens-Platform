"""SQLAlchemy models.

Conventions this file commits to, because each one prevents a specific outage:

* **Every table carries ``created_at``** with a server default, so the retention
  job has one predicate that works everywhere.
* **Money is integer micro-dollars**, never a float. Summing 40,000 float costs
  and comparing the result against a budget cap is how a guard rail drifts.
* **Constraints are named** via a naming convention. Alembic cannot drop an
  anonymous constraint on SQLite, and unnamed constraints are the reason so many
  projects end up with irreversible migrations.
* **New columns are nullable or carry a server default.** A consumer built
  before the column exists must still be able to insert during a rolling deploy.
* ``spans`` has a unique constraint on ``(trace_id, span_id)``. Redis Streams
  guarantee at-least-once delivery; a restarted consumer *will* redeliver, and
  this constraint plus ``ON CONFLICT DO NOTHING`` is what makes that a non-event.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

# JSONB on Postgres (indexable, typed); plain JSON on SQLite so the test suite
# runs without a container.
JSONType = JSON().with_variant(JSONB(), "postgresql")

# SQLite only auto-increments a column declared exactly ``INTEGER PRIMARY KEY``;
# a BIGINT primary key there is an ordinary column and inserts fail without an
# explicit id. The variant keeps Postgres on BIGINT and the test suite working.
PkBigInt = BigInteger().with_variant(Integer(), "sqlite")

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


# --------------------------------------------------------------------------- #
# Phase 1 -- ingestion
# --------------------------------------------------------------------------- #
class Trace(Base):
    """One end-to-end agent request. The row the dashboard's list page shows."""

    __tablename__ = "traces"

    trace_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False, server_default="")
    service: Mapped[str] = mapped_column(String(100), nullable=False, server_default="unknown")
    environment: Mapped[str] = mapped_column(String(32), nullable=False, server_default="local")
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="running")

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    latency_ms: Mapped[int | None] = mapped_column(Integer)

    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    cost_micros: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    model: Mapped[str | None] = mapped_column(String(120))

    span_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    llm_calls: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    tool_calls: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    input: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    output: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)

    session_id: Mapped[str | None] = mapped_column(String(200))
    user_ref: Mapped[str | None] = mapped_column(String(200))
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="2")

    # -- Phase 2 columns, added by a later migration as nullable/defaulted ----
    guardrail_status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="pending"
    )
    guardrail_flags: Mapped[dict[str, Any]] = mapped_column(
        JSONType, nullable=False, default=dict, server_default="{}"
    )
    risk_score: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    max_severity: Mapped[str | None] = mapped_column(String(16))
    eval_status: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="not_sampled"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        # The dashboard's default query: one tenant, newest first. Ordering and
        # time filters both use created_at (ingestion time, never NULL, always
        # increasing) rather than started_at, so one index serves both and the
        # keyset cursor can never hit a NULL sort key.
        Index("ix_traces_tenant_created", "tenant_id", "created_at"),
        Index("ix_traces_tenant_guardrail", "tenant_id", "guardrail_status"),
        Index("ix_traces_tenant_status", "tenant_id", "status"),
    )


class Span(Base):
    """One unit of work inside a trace: an LLM call, a tool call, a graph node."""

    __tablename__ = "spans"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    span_id: Mapped[str] = mapped_column(String(64), nullable=False)
    parent_span_id: Mapped[str | None] = mapped_column(String(64))
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)

    kind: Mapped[str] = mapped_column(String(24), nullable=False, server_default="other")
    name: Mapped[str] = mapped_column(String(200), nullable=False, server_default="")
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="running")

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    latency_ms: Mapped[int | None] = mapped_column(Integer)

    input: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    output: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSONType)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)

    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    cost_micros: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    model: Mapped[str | None] = mapped_column(String(120))

    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="2")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        # THE idempotency key. Without it, every consumer restart duplicates rows.
        UniqueConstraint("trace_id", "span_id", name="uq_spans_trace_id_span_id"),
        Index("ix_spans_tenant_created", "tenant_id", "created_at"),
    )


class DeadLetter(Base):
    """A message that failed too many times. Parked here instead of retried forever."""

    __tablename__ = "dead_letters"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    stream: Mapped[str] = mapped_column(String(120), nullable=False)
    consumer_group: Mapped[str] = mapped_column(String(120), nullable=False)
    message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    tenant_id: Mapped[str | None] = mapped_column(String(64))
    trace_id: Mapped[str | None] = mapped_column(String(64))
    delivery_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    error_type: Mapped[str] = mapped_column(String(120), nullable=False, server_default="")
    error_message: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    payload: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    replayed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        UniqueConstraint(
            "stream", "consumer_group", "message_id", name="uq_dead_letters_stream_group_message"
        ),
    )


class WorkerHeartbeat(Base):
    """Liveness for each consumer. Feeds /health/meta -- the platform watching itself."""

    __tablename__ = "worker_heartbeats"

    name: Mapped[str] = mapped_column(String(200), primary_key=True)
    role: Mapped[str] = mapped_column(String(32), nullable=False)
    host: Mapped[str] = mapped_column(String(200), nullable=False, server_default="")
    pid: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="starting")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    failed: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    dead_lettered: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    detail: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)


class JobRun(Base):
    """Last-run bookkeeping for scheduled jobs.

    Persisted rather than kept in memory precisely because the free tier sleeps:
    on wake, a job that is overdue runs immediately instead of silently skipping
    the window it slept through.
    """

    __tablename__ = "job_runs"

    job_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="never")
    last_error: Mapped[str | None] = mapped_column(Text)
    run_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    detail: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)


# --------------------------------------------------------------------------- #
# Phase 2 -- guardrails, evals, alerts, budget
# --------------------------------------------------------------------------- #
class GuardrailFinding(Base):
    """One detection on one field of one span.

    ``excerpt`` stores the **masked** text. Storing raw PII in the findings table
    while redacting it in the API response would just move the leak.
    """

    __tablename__ = "guardrail_findings"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    # Empty string, never NULL: NULL never equals NULL, which would silently
    # disable the dedupe constraint below on every trace-level finding.
    span_id: Mapped[str] = mapped_column(String(64), nullable=False, server_default="")
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)

    detector: Mapped[str] = mapped_column(String(32), nullable=False)
    finding_type: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, server_default="low")
    score: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    field: Mapped[str] = mapped_column(String(120), nullable=False, server_default="")
    excerpt: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    start_offset: Mapped[int] = mapped_column(Integer, nullable=False, server_default="-1")
    end_offset: Mapped[int] = mapped_column(Integer, nullable=False, server_default="-1")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        UniqueConstraint(
            "trace_id",
            "span_id",
            "detector",
            "finding_type",
            "field",
            "start_offset",
            name="uq_guardrail_findings_dedupe",
        ),
        Index("ix_guardrail_findings_tenant_created", "tenant_id", "created_at"),
    )


class EvalScore(Base):
    """One metric score for one trace, plus what the judging itself cost."""

    __tablename__ = "eval_scores"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    backend: Mapped[str] = mapped_column(String(24), nullable=False, server_default="judge")
    judge_model: Mapped[str] = mapped_column(String(120), nullable=False, server_default="")
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    cost_micros: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        UniqueConstraint("trace_id", "metric", name="uq_eval_scores_trace_id_metric"),
        Index("ix_eval_scores_tenant_metric_created", "tenant_id", "metric", "created_at"),
    )


class Alert(Base):
    """Something a human should look at."""

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), index=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, server_default="medium")
    title: Mapped[str] = mapped_column(String(300), nullable=False)
    detail: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="open")
    # Collapses an alert storm into one row: a tenant blowing its budget every
    # minute for six hours should page once, not 360 times.
    dedupe_key: Mapped[str | None] = mapped_column(String(200))
    acknowledged_by: Mapped[str | None] = mapped_column(String(320))
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "dedupe_key", name="uq_alerts_tenant_id_dedupe_key"),
        Index("ix_alerts_tenant_status_created", "tenant_id", "status", "created_at"),
    )


class TenantUsage(Base):
    """Running LLM spend per tenant per period. The budget cap reads this."""

    __tablename__ = "tenant_usage"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    period_kind: Mapped[str] = mapped_column(String(8), nullable=False)  # day | month
    period_key: Mapped[str] = mapped_column(String(10), nullable=False)  # 2026-09-08 | 2026-09
    purpose: Mapped[str] = mapped_column(String(32), nullable=False, server_default="eval_judge")
    prompt_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    completion_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    total_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    cost_micros: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    calls: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "period_kind",
            "period_key",
            "purpose",
            name="uq_tenant_usage_period",
        ),
    )


# --------------------------------------------------------------------------- #
# Phase 3 -- drift
# --------------------------------------------------------------------------- #
class DriftBaseline(Base):
    """A measured healthy period, captured deliberately -- not a guessed constant."""

    __tablename__ = "drift_baselines"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    mean: Mapped[float] = mapped_column(Float, nullable=False)
    stddev: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="1")
    note: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("ix_drift_baselines_tenant_metric_active", "tenant_id", "metric", "is_active"),
    )


class DriftSnapshot(Base):
    """One drift evaluation of a rolling window against the active baseline."""

    __tablename__ = "drift_snapshots"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    mean: Mapped[float] = mapped_column(Float, nullable=False)
    stddev: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    baseline_mean: Mapped[float | None] = mapped_column(Float)
    baseline_stddev: Mapped[float | None] = mapped_column(Float)
    z_score: Mapped[float | None] = mapped_column(Float)
    drifted: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        Index("ix_drift_snapshots_tenant_metric_created", "tenant_id", "metric", "created_at"),
    )


# --------------------------------------------------------------------------- #
# Phase 4 -- auth, RBAC, audit
# --------------------------------------------------------------------------- #
class User(Base):
    """A dashboard operator. ``role`` drives server-side redaction, not the UI."""

    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True)
    password_hash: Mapped[str | None] = mapped_column(Text)
    subject: Mapped[str | None] = mapped_column(String(255), unique=True)  # OIDC `sub`
    role: Mapped[str] = mapped_column(String(16), nullable=False, server_default="viewer")
    # NULL means "every tenant"; a value scopes the operator to one tenant.
    tenant_id: Mapped[str | None] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="1")
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class AuditLog(Base):
    """Who looked at what, and when. The artifact behind the compliance story."""

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(PkBigInt, primary_key=True, autoincrement=True)
    actor_id: Mapped[str | None] = mapped_column(String(36))
    actor_email: Mapped[str] = mapped_column(String(320), nullable=False, server_default="")
    actor_role: Mapped[str] = mapped_column(String(16), nullable=False, server_default="")
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(64), nullable=False, server_default="")
    resource_id: Mapped[str] = mapped_column(String(128), nullable=False, server_default="")
    tenant_id: Mapped[str | None] = mapped_column(String(64))
    ip: Mapped[str] = mapped_column(String(64), nullable=False, server_default="")
    user_agent: Mapped[str] = mapped_column(String(400), nullable=False, server_default="")
    redacted: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="0")
    detail: Mapped[dict[str, Any]] = mapped_column(JSONType, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), index=True
    )

    __table_args__ = (
        Index("ix_audit_log_actor_created", "actor_email", "created_at"),
        Index("ix_audit_log_resource", "resource_type", "resource_id"),
    )


__all__ = [
    "Alert",
    "AuditLog",
    "Base",
    "DeadLetter",
    "DriftBaseline",
    "DriftSnapshot",
    "EvalScore",
    "GuardrailFinding",
    "JSONType",
    "JobRun",
    "PkBigInt",
    "Span",
    "TenantUsage",
    "Trace",
    "User",
    "WorkerHeartbeat",
]
