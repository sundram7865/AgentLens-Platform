"""Audit log.

Who looked at which trace, when, from where, and -- the field that makes this an
audit trail rather than a page-view log -- **whether what they received was
redacted**. "Alice opened trace tr_9f2c" is trivia; "Alice opened trace tr_9f2c
and the response contained unmasked customer PII" is the answer to the question
a compliance review actually asks.

Writing it must never fail the request it is recording. A failed audit write is
logged loudly and the read proceeds: refusing to serve a trace because the audit
table is unreachable would turn a bookkeeping outage into a product outage.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from ..db import get_db
from ..logging import get_logger
from ..models import AuditLog

if TYPE_CHECKING:  # pragma: no cover
    from fastapi import Request

    from ..api.deps import Principal

log = get_logger("obs_platform.audit")

MAX_USER_AGENT = 400


def client_ip(request: Any) -> str:
    """Best-effort client address, honouring one proxy hop.

    ``X-Forwarded-For`` is client-controlled, so this is evidence, not proof --
    the left-most entry is recorded because that is what a reviewer expects to
    see, not because it can be trusted.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()[:64]
    client = getattr(request, "client", None)
    return (getattr(client, "host", "") or "")[:64]


async def record_access(
    request: Request,
    principal: Principal,
    action: str,
    resource_type: str = "",
    resource_id: str = "",
    tenant_id: str | None = None,
    redacted: bool = True,
    detail: dict[str, Any] | None = None,
) -> None:
    """Append one audit row. Swallows its own failures by design."""
    try:
        async with get_db().session() as session:
            session.add(
                AuditLog(
                    actor_id=principal.id,
                    actor_email=principal.email[:320],
                    actor_role=getattr(principal.role, "value", str(principal.role))[:16],
                    action=action[:64],
                    resource_type=resource_type[:64],
                    resource_id=str(resource_id)[:128],
                    tenant_id=tenant_id,
                    ip=client_ip(request),
                    user_agent=request.headers.get("user-agent", "")[:MAX_USER_AGENT],
                    redacted=redacted,
                    detail=detail or {},
                    created_at=datetime.now(UTC),
                )
            )
    except Exception:
        log.warning(
            "audit.write_failed",
            action=action,
            resource_id=str(resource_id),
            actor=principal.email,
            exc_info=True,
        )


__all__ = ["client_ip", "record_access"]
