"""Phase 4: operators, roles and the audit trail.

Revision ID: 0004_auth_and_audit
Revises: 0003_drift
Create Date: 2026-09-08

``users.tenant_id`` NULL means "every tenant"; a value scopes an operator to one.
``audit_log.redacted`` records whether the response that operator actually
received had PII stripped -- which is the difference between an audit trail and
a list of page views.
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0004_auth_and_audit"
down_revision: Union[str, None] = "0003_drift"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

JSON_TYPE = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")
NOW = sa.func.now()


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("email", sa.String(length=320), nullable=False),
        sa.Column("password_hash", sa.Text(), nullable=True),
        sa.Column("subject", sa.String(length=255), nullable=True),
        sa.Column("role", sa.String(length=16), server_default="viewer", nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default="1", nullable=False),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_users")),
        sa.UniqueConstraint("email", name=op.f("uq_users_email")),
        sa.UniqueConstraint("subject", name=op.f("uq_users_subject")),
    )

    op.create_table(
        "audit_log",
        sa.Column("id", sa.BigInteger().with_variant(sa.Integer(), "sqlite"), nullable=False),
        sa.Column("actor_id", sa.String(length=36), nullable=True),
        sa.Column("actor_email", sa.String(length=320), server_default="", nullable=False),
        sa.Column("actor_role", sa.String(length=16), server_default="", nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=64), server_default="", nullable=False),
        sa.Column("resource_id", sa.String(length=128), server_default="", nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=True),
        sa.Column("ip", sa.String(length=64), server_default="", nullable=False),
        sa.Column("user_agent", sa.String(length=400), server_default="", nullable=False),
        sa.Column("redacted", sa.Boolean(), server_default="0", nullable=False),
        sa.Column("detail", JSON_TYPE, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=NOW, nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_audit_log")),
    )
    op.create_index(op.f("ix_audit_log_created_at"), "audit_log", ["created_at"])
    op.create_index("ix_audit_log_actor_created", "audit_log", ["actor_email", "created_at"])
    op.create_index("ix_audit_log_resource", "audit_log", ["resource_type", "resource_id"])


def downgrade() -> None:
    op.drop_index("ix_audit_log_resource", table_name="audit_log")
    op.drop_index("ix_audit_log_actor_created", table_name="audit_log")
    op.drop_index(op.f("ix_audit_log_created_at"), table_name="audit_log")
    op.drop_table("audit_log")
    op.drop_table("users")
