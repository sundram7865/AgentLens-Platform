"""Phase 3: drift baselines and snapshots.

Revision ID: 0003_drift
Revises: 0002_guardrails_and_evals
Create Date: 2026-09-08

``drift_baselines`` stores a *measured* healthy window rather than a constant
somebody guessed. The drift monitor compares a rolling window against the active
baseline for that tenant and metric, so "the model got worse" is a statement
about this deployment's own history, not about a number from a blog post.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0003_drift"
down_revision: Union[str, None] = "0002_guardrails_and_evals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

NOW = sa.func.now()


def upgrade() -> None:
    op.create_table(
        "drift_baselines",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("metric", sa.String(length=32), nullable=False),
        sa.Column("mean", sa.Float(), nullable=False),
        sa.Column("stddev", sa.Float(), server_default="0", nullable=False),
        sa.Column("sample_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default="1", nullable=False),
        sa.Column("note", sa.Text(), server_default="", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_drift_baselines")),
    )
    op.create_index(op.f("ix_drift_baselines_tenant_id"), "drift_baselines", ["tenant_id"])
    op.create_index(
        "ix_drift_baselines_tenant_metric_active",
        "drift_baselines",
        ["tenant_id", "metric", "is_active"],
    )

    op.create_table(
        "drift_snapshots",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("metric", sa.String(length=32), nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("mean", sa.Float(), nullable=False),
        sa.Column("stddev", sa.Float(), server_default="0", nullable=False),
        sa.Column("sample_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("baseline_mean", sa.Float(), nullable=True),
        sa.Column("baseline_stddev", sa.Float(), nullable=True),
        sa.Column("z_score", sa.Float(), nullable=True),
        sa.Column("drifted", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_drift_snapshots")),
    )
    op.create_index(op.f("ix_drift_snapshots_tenant_id"), "drift_snapshots", ["tenant_id"])
    op.create_index(op.f("ix_drift_snapshots_created_at"), "drift_snapshots", ["created_at"])
    op.create_index(
        "ix_drift_snapshots_tenant_metric_created",
        "drift_snapshots",
        ["tenant_id", "metric", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_drift_snapshots_tenant_metric_created", table_name="drift_snapshots")
    op.drop_index(op.f("ix_drift_snapshots_created_at"), table_name="drift_snapshots")
    op.drop_index(op.f("ix_drift_snapshots_tenant_id"), table_name="drift_snapshots")
    op.drop_table("drift_snapshots")
    op.drop_index("ix_drift_baselines_tenant_metric_active", table_name="drift_baselines")
    op.drop_index(op.f("ix_drift_baselines_tenant_id"), table_name="drift_baselines")
    op.drop_table("drift_baselines")
