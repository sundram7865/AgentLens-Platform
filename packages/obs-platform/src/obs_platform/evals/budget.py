"""Per-tenant LLM spend cap.

The eval scorer is the only thing in this platform that spends money. A
misconfigured agent flooding the stream would otherwise turn into a surprise
invoice, so the cap is enforced **before** each scoring batch, not observed
afterwards in a log line.

Two properties make this a guard rail rather than a metric:

* Spend is accumulated in Postgres with an atomic ``ON CONFLICT DO UPDATE ...
  SET total = total + excluded``, so concurrent scorer replicas cannot lose an
  increment to a read-modify-write race and drift under the real number.
* Amounts are integer micro-dollars. Summing forty thousand floats and comparing
  the result to a cap is how a budget silently stops binding.

Exceeding the cap drops the tenant's effective sample rate to zero and writes an
alert. That turns "a surprise bill" into "a logged, expected event", which is
the whole point.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from obs_sdk.schema import Severity

from ..alerts import AlertKind, budget_dedupe_key, raise_alert
from ..db import table_of
from ..logging import get_logger
from ..models import TenantUsage
from ..pricing import micros_to_usd, usd_to_micros
from ..settings import Settings

log = get_logger("obs_platform.budget")

PURPOSE_EVAL = "eval_judge"


def period_keys(now: datetime | None = None) -> tuple[str, str]:
    moment = now or datetime.now(UTC)
    return moment.strftime("%Y-%m-%d"), moment.strftime("%Y-%m")


@dataclass
class BudgetStatus:
    allowed: bool
    reason: str = ""
    daily_cost_usd: float = 0.0
    monthly_cost_usd: float = 0.0
    daily_tokens: int = 0
    daily_cap_usd: float = 0.0
    monthly_cap_usd: float = 0.0
    daily_token_cap: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "reason": self.reason,
            "daily_cost_usd": self.daily_cost_usd,
            "monthly_cost_usd": self.monthly_cost_usd,
            "daily_tokens": self.daily_tokens,
            "daily_cap_usd": self.daily_cap_usd,
            "monthly_cap_usd": self.monthly_cap_usd,
            "daily_token_cap": self.daily_token_cap,
        }


async def _usage_row(
    session: AsyncSession, tenant_id: str, period_kind: str, period_key: str
) -> TenantUsage | None:
    return (
        await session.execute(
            select(TenantUsage).where(
                TenantUsage.tenant_id == tenant_id,
                TenantUsage.period_kind == period_kind,
                TenantUsage.period_key == period_key,
                TenantUsage.purpose == PURPOSE_EVAL,
            )
        )
    ).scalar_one_or_none()


async def check_budget(session: AsyncSession, tenant_id: str, settings: Settings) -> BudgetStatus:
    """Is this tenant still allowed to spend judge tokens today?"""
    day_key, month_key = period_keys()
    daily = await _usage_row(session, tenant_id, "day", day_key)
    monthly = await _usage_row(session, tenant_id, "month", month_key)

    daily_cost = micros_to_usd(daily.cost_micros if daily else 0)
    monthly_cost = micros_to_usd(monthly.cost_micros if monthly else 0)
    daily_tokens = int(daily.total_tokens if daily else 0)

    status = BudgetStatus(
        allowed=True,
        daily_cost_usd=daily_cost,
        monthly_cost_usd=monthly_cost,
        daily_tokens=daily_tokens,
        daily_cap_usd=settings.budget_daily_usd_per_tenant,
        monthly_cap_usd=settings.budget_monthly_usd_per_tenant,
        daily_token_cap=settings.budget_daily_tokens_per_tenant,
    )

    if daily_cost >= settings.budget_daily_usd_per_tenant:
        status.allowed = False
        status.reason = (
            f"daily judge spend ${daily_cost:.4f} reached the "
            f"${settings.budget_daily_usd_per_tenant:.2f} cap"
        )
    elif monthly_cost >= settings.budget_monthly_usd_per_tenant:
        status.allowed = False
        status.reason = (
            f"monthly judge spend ${monthly_cost:.4f} reached the "
            f"${settings.budget_monthly_usd_per_tenant:.2f} cap"
        )
    elif daily_tokens >= settings.budget_daily_tokens_per_tenant:
        status.allowed = False
        status.reason = (
            f"daily judge tokens {daily_tokens} reached the "
            f"{settings.budget_daily_tokens_per_tenant} cap"
        )
    return status


async def record_spend(
    session: AsyncSession,
    tenant_id: str,
    prompt_tokens: int,
    completion_tokens: int,
    cost_micros: int,
    calls: int = 1,
    purpose: str = PURPOSE_EVAL,
) -> None:
    """Atomically add this batch's spend to the daily and monthly totals."""
    day_key, month_key = period_keys()
    for period_kind, period_key in (("day", day_key), ("month", month_key)):
        await _increment(
            session,
            {
                "tenant_id": tenant_id,
                "period_kind": period_kind,
                "period_key": period_key,
                "purpose": purpose,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "total_tokens": prompt_tokens + completion_tokens,
                "cost_micros": cost_micros,
                "calls": calls,
                "updated_at": datetime.now(UTC),
                "created_at": datetime.now(UTC),
            },
        )


async def _increment(session: AsyncSession, row: dict[str, Any]) -> None:
    """``INSERT ... ON CONFLICT DO UPDATE SET total = total + excluded.total``.

    Done in SQL rather than read-then-write so two scorer replicas incrementing
    the same tenant in the same second cannot lose one of the updates -- which
    would make the cap under-count and stop binding exactly when traffic is
    highest.
    """
    table = table_of(TenantUsage)
    dialect = session.bind.dialect.name if session.bind is not None else "postgresql"
    insert = pg_insert if dialect == "postgresql" else sqlite_insert
    stmt = insert(table).values(row)
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=["tenant_id", "period_kind", "period_key", "purpose"],
            set_={
                "prompt_tokens": table.c.prompt_tokens + stmt.excluded.prompt_tokens,
                "completion_tokens": table.c.completion_tokens + stmt.excluded.completion_tokens,
                "total_tokens": table.c.total_tokens + stmt.excluded.total_tokens,
                "cost_micros": table.c.cost_micros + stmt.excluded.cost_micros,
                "calls": table.c.calls + stmt.excluded.calls,
                "updated_at": stmt.excluded.updated_at,
            },
        )
    )


async def report_exhausted(session: AsyncSession, tenant_id: str, status: BudgetStatus) -> None:
    """Write the alert that turns a surprise bill into an expected event."""
    day_key, _ = period_keys()
    await raise_alert(
        session,
        tenant_id=tenant_id,
        kind=AlertKind.BUDGET,
        severity=Severity.HIGH,
        title=f"Eval budget exhausted for {tenant_id}: sampling paused",
        detail={
            **status.as_dict(),
            "action": "eval sample rate dropped to 0 for this tenant until the period rolls over",
        },
        dedupe_key=budget_dedupe_key(tenant_id, day_key),
    )
    log.warning("budget.exhausted", tenant_id=tenant_id, reason=status.reason)


def estimated_batch_cost_usd(model: str, traces: int, avg_prompt_tokens: int = 1200) -> float:
    """Rough pre-flight cost estimate, used to refuse a batch that would blow the cap."""
    from ..pricing import cost_micros

    return micros_to_usd(cost_micros(model, avg_prompt_tokens * traces, 150 * traces))


def usd(micros: int) -> float:
    return micros_to_usd(micros)


def to_micros(value: float) -> int:
    return usd_to_micros(value)


__all__ = [
    "PURPOSE_EVAL",
    "BudgetStatus",
    "check_budget",
    "estimated_batch_cost_usd",
    "period_keys",
    "record_spend",
    "report_exhausted",
]
