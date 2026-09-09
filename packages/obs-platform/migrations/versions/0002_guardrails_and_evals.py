"""Phase 2: guardrail findings, eval scores, alerts, per-tenant budget usage.

Revision ID: 0002_guardrails_and_evals
Revises: 0001_ingestion_core
Create Date: 2026-09-08

Every column added to the existing ``traces`` table here is NOT NULL **with a
server default**. That is the zero-downtime rule made concrete: while this
migration is applied, the already-running storage writer -- which does not know
these columns exist -- keeps inserting successfully, because the database fills
them in. A NOT NULL column with no default would have failed every insert from
the old consumer until the new one finished rolling out.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002_guardrails_and_evals"
down_revision: Union[str, None] = "0001_ingestion_core"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

JSON_TYPE = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
NOW = sa.func.now()


def upgrade() -> None:
    # -- additive columns on traces ----------------------------------------
    op.add_column(
        "traces",
        sa.Column("guardrail_status", sa.String(length=16), server_default="pending", nullable=False),
    )
    op.add_column(
        "traces",
        sa.Column("guardrail_flags", JSON_TYPE, server_default="{}", nullable=False),
    )
    op.add_column("traces", sa.Column("risk_score", sa.Integer(), server_default="0", nullable=False))
    op.add_column("traces", sa.Column("max_severity", sa.String(length=16), nullable=True))
    op.add_column(
        "traces",
        sa.Column("eval_status", sa.String(length=16), server_default="not_sampled", nullable=False),
    )
    op.create_index("ix_traces_tenant_guardrail", "traces", ["tenant_id", "guardrail_status"])

    op.create_table(
        "guardrail_findings",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        # Empty string rather than NULL: NULL never equals NULL, which would
        # quietly disable the dedupe constraint for every trace-level finding.
        sa.Column("span_id", sa.String(length=64), server_default="", nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("detector", sa.String(length=32), nullable=False),
        sa.Column("finding_type", sa.String(length=64), nullable=False),
        sa.Column("severity", sa.String(length=16), server_default="low", nullable=False),
        sa.Column("score", sa.Float(), server_default="0", nullable=False),
        sa.Column("field", sa.String(length=120), server_default="", nullable=False),
        sa.Column("excerpt", sa.Text(), server_default="", nullable=False),
        sa.Column("start_offset", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("end_offset", sa.Integer(), server_default="-1", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_guardrail_findings")),
        sa.UniqueConstraint(
            "trace_id",
            "span_id",
            "detector",
            "finding_type",
            "field",
            "start_offset",
            name="uq_guardrail_findings_dedupe",
        ),
    )
    op.create_index(op.f("ix_guardrail_findings_trace_id"), "guardrail_findings", ["trace_id"])
    op.create_index(op.f("ix_guardrail_findings_created_at"), "guardrail_findings", ["created_at"])
    op.create_index(
        "ix_guardrail_findings_tenant_created", "guardrail_findings", ["tenant_id", "created_at"]
    )

    op.create_table(
        "eval_scores",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("metric", sa.String(length=32), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("reason", sa.Text(), server_default="", nullable=False),
        sa.Column("backend", sa.String(length=24), server_default="judge", nullable=False),
        sa.Column("judge_model", sa.String(length=120), server_default="", nullable=False),
        sa.Column("prompt_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("completion_tokens", sa.Integer(), server_default="0", nullable=False),
        sa.Column("cost_micros", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_eval_scores")),
        # One score per metric per trace: a redelivered scoring job overwrites
        # rather than accumulating duplicate opinions.
        sa.UniqueConstraint("trace_id", "metric", name="uq_eval_scores_trace_id_metric"),
    )
    op.create_index(op.f("ix_eval_scores_trace_id"), "eval_scores", ["trace_id"])
    op.create_index(op.f("ix_eval_scores_created_at"), "eval_scores", ["created_at"])
    op.create_index(
        "ix_eval_scores_tenant_metric_created", "eval_scores", ["tenant_id", "metric", "created_at"]
    )

    op.create_table(
        "alerts",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("trace_id", sa.String(length=64), nullable=True),
        sa.Column("kind", sa.String(length=32), nullable=False),
        sa.Column("severity", sa.String(length=16), server_default="medium", nullable=False),
        sa.Column("title", sa.String(length=300), nullable=False),
        sa.Column("detail", JSON_TYPE, nullable=False),
        sa.Column("status", sa.String(length=16), server_default="open", nullable=False),
        sa.Column("dedupe_key", sa.String(length=200), nullable=True),
        sa.Column("acknowledged_by", sa.String(length=320), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_alerts")),
        # Collapses an alert storm: a tenant over budget every minute for six
        # hours produces one row, not 360.
        sa.UniqueConstraint("tenant_id", "dedupe_key", name="uq_alerts_tenant_id_dedupe_key"),
    )
    op.create_index(op.f("ix_alerts_tenant_id"), "alerts", ["tenant_id"])
    op.create_index(op.f("ix_alerts_trace_id"), "alerts", ["trace_id"])
    op.create_index(op.f("ix_alerts_created_at"), "alerts", ["created_at"])
    op.create_index("ix_alerts_tenant_status_created", "alerts", ["tenant_id", "status", "created_at"])

    op.create_table(
        "tenant_usage",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("period_kind", sa.String(length=8), nullable=False),
        sa.Column("period_key", sa.String(length=10), nullable=False),
        sa.Column("purpose", sa.String(length=32), server_default="eval_judge", nullable=False),
        sa.Column("prompt_tokens", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("completion_tokens", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("total_tokens", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("cost_micros", sa.BigInteger(), server_default="0", nullable=False),
        sa.Column("calls", sa.Integer(), server_default="0", nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tenant_usage")),
        sa.UniqueConstraint(
            "tenant_id", "period_kind", "period_key", "purpose", name="uq_tenant_usage_period"
        ),
    )
    op.create_index(op.f("ix_tenant_usage_tenant_id"), "tenant_usage", ["tenant_id"])


def downgrade() -> None:
    op.drop_index(op.f("ix_tenant_usage_tenant_id"), table_name="tenant_usage")
    op.drop_table("tenant_usage")
    op.drop_index("ix_alerts_tenant_status_created", table_name="alerts")
    op.drop_index(op.f("ix_alerts_created_at"), table_name="alerts")
    op.drop_index(op.f("ix_alerts_trace_id"), table_name="alerts")
    op.drop_index(op.f("ix_alerts_tenant_id"), table_name="alerts")
    op.drop_table("alerts")
    op.drop_index("ix_eval_scores_tenant_metric_created", table_name="eval_scores")
    op.drop_index(op.f("ix_eval_scores_created_at"), table_name="eval_scores")
    op.drop_index(op.f("ix_eval_scores_trace_id"), table_name="eval_scores")
    op.drop_table("eval_scores")
    op.drop_index("ix_guardrail_findings_tenant_created", table_name="guardrail_findings")
    op.drop_index(op.f("ix_guardrail_findings_created_at"), table_name="guardrail_findings")
    op.drop_index(op.f("ix_guardrail_findings_trace_id"), table_name="guardrail_findings")
    op.drop_table("guardrail_findings")
    op.drop_index("ix_traces_tenant_guardrail", table_name="traces")
    op.drop_column("traces", "eval_status")
    op.drop_column("traces", "max_severity")
    op.drop_column("traces", "risk_score")
    op.drop_column("traces", "guardrail_flags")
    op.drop_column("traces", "guardrail_status")
