"""Alert creation.

Every alert carries a ``dedupe_key``. Without one, a tenant that trips the same
condition on every request produces thousands of identical rows, the alerts page
becomes unusable, and the operator learns to ignore it -- which is strictly worse
than having no alerts, because now the failure is invisible *and* everyone
believes it is being watched.

The unique constraint on ``(tenant_id, dedupe_key)`` makes de-duplication a
database guarantee rather than an application convention, so it holds even when
two workers detect the same condition simultaneously.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from obs_sdk.schema import Severity

from .db import table_of, upsert
from .logging import get_logger
from .models import Alert

log = get_logger("obs_platform.alerts")


class AlertKind:
    GUARDRAIL = "guardrail"
    DRIFT = "drift"
    BUDGET = "budget"
    DEAD_LETTER = "dead_letter"
    SYSTEM = "system"


async def raise_alert(
    session: AsyncSession,
    tenant_id: str,
    kind: str,
    severity: Severity | str,
    title: str,
    detail: dict[str, Any] | None = None,
    trace_id: str | None = None,
    dedupe_key: str | None = None,
) -> None:
    """Create an alert, or refresh the existing one with the same dedupe key.

    On conflict the detail and timestamp are updated rather than a second row
    inserted, so the alerts list shows "this is still happening, most recently
    at X" instead of a wall of duplicates.
    """
    severity_value = Severity(severity).value
    await upsert(
        session,
        table_of(Alert),
        {
            "tenant_id": tenant_id,
            "trace_id": trace_id,
            "kind": kind,
            "severity": severity_value,
            "title": title[:300],
            "detail": detail or {},
            "status": "open",
            "dedupe_key": dedupe_key,
            "created_at": datetime.now(UTC),
        },
        index_elements=["tenant_id", "dedupe_key"],
        update_columns=["severity", "title", "detail", "created_at"],
    )
    log.info(
        "alert.raised",
        tenant_id=tenant_id,
        kind=kind,
        severity=severity_value,
        title=title[:120],
        trace_id=trace_id,
        dedupe_key=dedupe_key,
    )


def guardrail_dedupe_key(trace_id: str, detector: str, finding_type: str) -> str:
    """One alert per (trace, detector, finding type).

    Per-trace rather than per-tenant: each flagged trace is a separate thing a
    human has to look at. Collapsing across traces would hide the second victim.
    """
    return f"guardrail:{trace_id}:{detector}:{finding_type}"


def budget_dedupe_key(tenant_id: str, period_key: str, kind: str = "daily") -> str:
    """One budget alert per tenant per period.

    A tenant over its cap trips this on every scoring batch for the rest of the
    day; the operator needs to be told once.
    """
    return f"budget:{kind}:{tenant_id}:{period_key}"


def drift_dedupe_key(tenant_id: str, metric: str, day: date | None = None) -> str:
    """One drift alert per tenant, metric and day."""
    return f"drift:{tenant_id}:{metric}:{(day or datetime.now(UTC).date()).isoformat()}"


def dead_letter_dedupe_key(consumer_group: str, day: date | None = None) -> str:
    return f"dead_letter:{consumer_group}:{(day or datetime.now(UTC).date()).isoformat()}"


__all__ = [
    "AlertKind",
    "budget_dedupe_key",
    "dead_letter_dedupe_key",
    "drift_dedupe_key",
    "guardrail_dedupe_key",
    "raise_alert",
]
