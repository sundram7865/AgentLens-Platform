"""Phase 1: ingestion core -- traces, spans, dead letters, worker health, job runs.

Revision ID: 0001_ingestion_core
Revises:
Create Date: 2026-09-08

The load-bearing line in this file is the unique constraint on
``spans (trace_id, span_id)``. Redis Streams are at-least-once: a consumer that
dies between doing the work and sending XACK gets the same message again on
restart. With this constraint plus ON CONFLICT DO NOTHING that redelivery is a
no-op; without it, every restart silently duplicates rows.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001_ingestion_core"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# JSONB where it exists, JSON where it does not. Declared per-migration rather
# than imported from models.py: migrations must describe the schema as it was at
# this revision, and models.py describes the schema as it is now.
JSON_TYPE = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

# func.now() renders now() on Postgres and CURRENT_TIMESTAMP on SQLite. A
# hardcoded string would freeze one dialect's spelling into every environment.
NOW = sa.func.now()


def upgrade() -> None:
    op.create_table(
        "traces",
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=200), server_default="", nullable=False),
        sa.Column("service", sa.String(length=100), server_default="unknown", nullable=False),
        sa.Column("environment", sa.String(length=32), server_default="local", nullable=False),
        sa.Column("status", sa.String(length=16), server_default="running", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("prompt_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("completion_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("total_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("cost_micros", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("span_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("llm_calls", sa.Integer(), server_default="0", nullable=False),
        sa.Column("tool_calls", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("input", JSON_TYPE, nullable=False),
        sa.Column("output", JSON_TYPE, nullable=False),
        sa.Column("attributes", JSON_TYPE, nullable=False),
        sa.Column("session_id", sa.String(length=200), nullable=True),
        sa.Column("user_ref", sa.String(length=200), nullable=True),
        sa.Column("schema_version", sa.Integer(), server_default="2", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("trace_id", name=op.f("pk_traces")),
    )
    op.create_index(op.f("ix_traces_tenant_id"), "traces", ["tenant_id"])
    op.create_index(op.f("ix_traces_created_at"), "traces", ["created_at"])
    op.create_index("ix_traces_tenant_created", "traces", ["tenant_id", "created_at"])
    op.create_index("ix_traces_tenant_status", "traces", ["tenant_id", "status"])

    op.create_table(
        "spans",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("span_id", sa.String(length=64), nullable=False),
        sa.Column("parent_span_id", sa.String(length=64), nullable=True),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("kind", sa.String(length=24), server_default="other", nullable=False),
        sa.Column("name", sa.String(length=200), server_default="", nullable=False),
        sa.Column("status", sa.String(length=16), server_default="running", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("input", JSON_TYPE, nullable=False),
        sa.Column("output", JSON_TYPE, nullable=False),
        sa.Column("error", JSON_TYPE, nullable=True),
        sa.Column("attributes", JSON_TYPE, nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("completion_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("total_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("cost_micros", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("model", sa.String(length=120), nullable=True),
        sa.Column("schema_version", sa.Integer(), server_default="2", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_spans")),
        # ---- the idempotency guarantee for the whole ingestion path ----
        sa.UniqueConstraint("trace_id", "span_id", name="uq_spans_trace_id_span_id"),
    )
    op.create_index(op.f("ix_spans_trace_id"), "spans", ["trace_id"])
    op.create_index(op.f("ix_spans_created_at"), "spans", ["created_at"])
    op.create_index("ix_spans_tenant_created", "spans", ["tenant_id", "created_at"])

    op.create_table(
        "dead_letters",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("stream", sa.String(length=120), nullable=False),
        sa.Column("consumer_group", sa.String(length=120), nullable=False),
        sa.Column("message_id", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=True),
        sa.Column("trace_id", sa.String(length=64), nullable=True),
        sa.Column("delivery_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("error_type", sa.String(length=120), server_default="", nullable=False),
        sa.Column("error_message", sa.Text(), server_default="", nullable=False),
        sa.Column("payload", JSON_TYPE, nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_dead_letters")),
        sa.UniqueConstraint(
            "stream", "consumer_group", "message_id", name="uq_dead_letters_stream_group_message"
        ),
    )
    op.create_index(op.f("ix_dead_letters_created_at"), "dead_letters", ["created_at"])

    op.create_table(
        "worker_heartbeats",
        sa.Column("name", sa.String(length=200), nullable=False),
        sa.Column("role", sa.String(length=32), nullable=False),
        sa.Column("host", sa.String(length=200), server_default="", nullable=False),
        sa.Column("pid", sa.Integer(), server_default="0", nullable=False),
        sa.Column("status", sa.String(length=16), server_default="starting", nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processed", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("failed", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("dead_lettered", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("detail", JSON_TYPE, nullable=False),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_worker_heartbeats")),
    )
    op.create_index(
        op.f("ix_worker_heartbeats_last_seen_at"), "worker_heartbeats", ["last_seen_at"]
    )

    op.create_table(
        "job_runs",
        sa.Column("job_name", sa.String(length=100), nullable=False),
        sa.Column("last_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_status", sa.String(length=16), server_default="never", nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("run_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("detail", JSON_TYPE, nullable=False),
        sa.PrimaryKeyConstraint("job_name", name=op.f("pk_job_runs")),
    )


def downgrade() -> None:
    op.drop_table("job_runs")
    op.drop_index(op.f("ix_worker_heartbeats_last_seen_at"), table_name="worker_heartbeats")
    op.drop_table("worker_heartbeats")
    op.drop_index(op.f("ix_dead_letters_created_at"), table_name="dead_letters")
    op.drop_table("dead_letters")
    op.drop_index("ix_spans_tenant_created", table_name="spans")
    op.drop_index(op.f("ix_spans_created_at"), table_name="spans")
    op.drop_index(op.f("ix_spans_trace_id"), table_name="spans")
    op.drop_table("spans")
    op.drop_index("ix_traces_tenant_status", table_name="traces")
    op.drop_index("ix_traces_tenant_created", table_name="traces")
    op.drop_index(op.f("ix_traces_created_at"), table_name="traces")
    op.drop_index(op.f("ix_traces_tenant_id"), table_name="traces")
    op.drop_table("traces")
