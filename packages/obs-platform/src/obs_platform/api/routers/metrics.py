"""Latency, cost and quality metrics.

Percentiles are computed **in the database on Postgres** (``percentile_cont``)
and in Python on SQLite, which has no such aggregate. That split is deliberate
rather than lazy: shipping the Python path everywhere would mean streaming every
latency value in the window to the API process, which is fine for the test suite
and wrong for the deployment. The row cap on the fallback path exists so that if
someone does point this at SQLite with real data, it degrades to an approximate
answer instead of an out-of-memory kill.
"""

from __future__ import annotations

import statistics
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import Alert, EvalScore, Trace
from ...pricing import micros_to_usd
from ..deps import Principal, get_principal, read_session
from ..schemas import MetricsOverview, TimeBucket

router = APIRouter(prefix="/v1/metrics", tags=["metrics"])

SessionDep = Annotated[AsyncSession, Depends(read_session)]
PrincipalDep = Annotated[Principal, Depends(get_principal)]

# Ceiling on the SQLite percentile fallback. Beyond this the answer is computed
# from the most recent N traces and is approximate -- stated, not hidden.
MAX_LATENCY_SAMPLE = 50_000

BUCKET_SECONDS = {"minute": 60, "hour": 3600, "day": 86400}


def _dialect(session: AsyncSession) -> str:
    return session.bind.dialect.name if session.bind is not None else "postgresql"


async def _percentiles(
    session: AsyncSession, where: list[Any], quantiles: tuple[float, ...] = (0.5, 0.95, 0.99)
) -> dict[float, float | None]:
    """Latency percentiles, natively on Postgres and in Python elsewhere."""
    if _dialect(session) == "postgresql":
        columns = [func.percentile_cont(q).within_group(Trace.latency_ms.asc()) for q in quantiles]
        row = (await session.execute(select(*columns).where(*where))).first()
        if row is None:
            return dict.fromkeys(quantiles)
        return {
            q: (round(float(value), 1) if value is not None else None)
            for q, value in zip(quantiles, row, strict=True)
        }

    values = list(
        (
            await session.execute(
                select(Trace.latency_ms)
                .where(*where, Trace.latency_ms.is_not(None))
                .order_by(Trace.created_at.desc())
                .limit(MAX_LATENCY_SAMPLE)
            )
        ).scalars()
    )
    # latency_ms is nullable: a trace whose envelope never closed has none, and
    # a None in the sort would raise rather than simply not counting.
    return _percentiles_from([v for v in values if v is not None], quantiles)


def _percentiles_from(values: list[int], quantiles: tuple[float, ...]) -> dict[float, float | None]:
    if not values:
        return dict.fromkeys(quantiles)
    ordered = sorted(values)
    result: dict[float, float | None] = {}
    for q in quantiles:
        # Nearest-rank: no interpolation between two real observations, which
        # keeps a reported p99 an actual measured latency.
        index = min(len(ordered) - 1, max(0, round(q * (len(ordered) - 1))))
        result[q] = round(float(ordered[index]), 1)
    return result


@router.get("/overview", response_model=MetricsOverview, summary="Headline metrics")
async def overview(
    session: SessionDep,
    principal: PrincipalDep,
    tenant_id: str | None = Query(None),
    hours: int = Query(24, ge=1, le=24 * 90),
) -> MetricsOverview:
    scoped = principal.scope_tenant(tenant_id)
    since = datetime.now(UTC) - timedelta(hours=hours)
    where: list[Any] = [Trace.created_at >= since]
    if scoped:
        where.append(Trace.tenant_id == scoped)

    totals = (
        await session.execute(
            select(
                func.count(),
                func.coalesce(func.sum(case((Trace.status == "error", 1), else_=0)), 0),
                func.coalesce(func.sum(case((Trace.guardrail_status == "flagged", 1), else_=0)), 0),
                func.coalesce(func.sum(Trace.total_tokens), 0),
                func.coalesce(func.sum(Trace.cost_micros), 0),
            ).where(*where)
        )
    ).one()
    traces, errors, flagged, tokens, cost = (int(v) for v in totals)

    percentiles = await _percentiles(session, where)

    score_where: list[Any] = [EvalScore.created_at >= since]
    if scoped:
        score_where.append(EvalScore.tenant_id == scoped)
    score_rows = (
        await session.execute(
            select(EvalScore.metric, func.avg(EvalScore.score))
            .where(*score_where)
            .group_by(EvalScore.metric)
        )
    ).all()

    alert_where: list[Any] = [Alert.status == "open"]
    if scoped:
        alert_where.append(Alert.tenant_id == scoped)
    open_alerts = (
        await session.execute(select(func.count()).select_from(Alert).where(*alert_where))
    ).scalar() or 0

    return MetricsOverview(
        window_hours=hours,
        tenant_id=scoped,
        traces=traces,
        errors=errors,
        error_rate=round(errors / traces, 4) if traces else 0.0,
        flagged=flagged,
        flagged_rate=round(flagged / traces, 4) if traces else 0.0,
        p50_latency_ms=percentiles.get(0.5),
        p95_latency_ms=percentiles.get(0.95),
        p99_latency_ms=percentiles.get(0.99),
        total_tokens=tokens,
        cost_usd=micros_to_usd(cost),
        eval_scores={
            str(metric): round(float(value), 4) for metric, value in score_rows if value is not None
        },
        open_alerts=int(open_alerts),
    )


@router.get("/timeseries", response_model=list[TimeBucket], summary="Latency and cost over time")
async def timeseries(
    session: SessionDep,
    principal: PrincipalDep,
    tenant_id: str | None = Query(None),
    hours: int = Query(24, ge=1, le=24 * 30),
    bucket: str = Query("hour", pattern="^(minute|hour|day)$"),
) -> list[TimeBucket]:
    scoped = principal.scope_tenant(tenant_id)
    since = datetime.now(UTC) - timedelta(hours=hours)
    where: list[Any] = [Trace.created_at >= since]
    if scoped:
        where.append(Trace.tenant_id == scoped)

    rows = list(
        (
            await session.execute(
                select(
                    Trace.created_at,
                    Trace.latency_ms,
                    Trace.total_tokens,
                    Trace.cost_micros,
                    Trace.status,
                )
                .where(*where)
                .order_by(Trace.created_at.desc())
                .limit(MAX_LATENCY_SAMPLE)
            )
        ).all()
    )

    width = BUCKET_SECONDS[bucket]
    buckets: dict[datetime, dict[str, Any]] = {}
    for created_at, latency, tokens, cost, status in rows:
        moment = created_at if created_at.tzinfo else created_at.replace(tzinfo=UTC)
        key = datetime.fromtimestamp((int(moment.timestamp()) // width) * width, tz=UTC)
        entry = buckets.setdefault(
            key, {"traces": 0, "errors": 0, "latencies": [], "tokens": 0, "cost": 0}
        )
        entry["traces"] += 1
        entry["errors"] += 1 if status == "error" else 0
        entry["tokens"] += int(tokens or 0)
        entry["cost"] += int(cost or 0)
        if latency is not None:
            entry["latencies"].append(int(latency))

    series = []
    for key in sorted(buckets):
        entry = buckets[key]
        latencies = entry["latencies"]
        series.append(
            TimeBucket(
                bucket=key,
                traces=entry["traces"],
                errors=entry["errors"],
                p50_latency_ms=round(statistics.median(latencies), 1) if latencies else None,
                p95_latency_ms=_percentiles_from(latencies, (0.95,))[0.95] if latencies else None,
                total_tokens=entry["tokens"],
                cost_usd=micros_to_usd(entry["cost"]),
            )
        )
    return series


__all__ = ["router"]
