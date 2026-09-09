"""Alerts and drift.

The one place the dashboard is allowed to write, and only to acknowledge or
resolve an alert -- never to touch a trace. That keeps the "dashboard reads
Postgres, ingestion writes it" boundary intact while still letting an operator
clear a queue, and every acknowledgement is attributed to a named principal.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, status
from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session
from ...drift.monitor import DriftMonitor
from ...models import Alert, DriftBaseline, DriftSnapshot
from ...settings import Settings
from ..deps import (
    ListParams,
    Principal,
    Role,
    get_principal,
    get_settings_dep,
    list_params,
    read_session,
)
from ..pagination import build_page, decode_cursor, encode_cursor
from ..schemas import AlertOut, DriftPointOut, Page
from ..serializers import alert_out

router = APIRouter(prefix="/v1", tags=["alerts"])

SessionDep = Annotated[AsyncSession, Depends(read_session)]
WriteSessionDep = Annotated[AsyncSession, Depends(get_session)]
PrincipalDep = Annotated[Principal, Depends(get_principal)]
ParamsDep = Annotated[ListParams, Depends(list_params)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]


@router.get("/alerts", response_model=Page[AlertOut], summary="List alerts")
async def list_alerts(
    session: SessionDep,
    principal: PrincipalDep,
    params: ParamsDep,
    status_filter: str | None = Query("open", alias="status", description="open | resolved | all"),
    kind: str | None = Query(None, description="guardrail | drift | budget | dead_letter | system"),
    severity: str | None = Query(None),
) -> Page[AlertOut]:
    tenant_id = principal.scope_tenant(params.tenant_id)
    query = select(Alert)
    if tenant_id:
        query = query.where(Alert.tenant_id == tenant_id)
    if status_filter and status_filter != "all":
        query = query.where(Alert.status == status_filter)
    if kind:
        query = query.where(Alert.kind == kind)
    if severity:
        query = query.where(Alert.severity == severity)
    if params.cursor:
        cursor_created, cursor_id = decode_cursor(params.cursor)
        query = query.where(
            tuple_(Alert.created_at, Alert.id) < (cursor_created, int(cursor_id or 0))
        )

    query = query.order_by(Alert.created_at.desc(), Alert.id.desc()).limit(params.limit + 1)
    rows = list((await session.execute(query)).scalars())
    page, next_cursor, has_more = build_page(
        rows, params.limit, lambda a: encode_cursor(a.created_at, str(a.id))
    )
    return Page[AlertOut](
        items=[alert_out(a) for a in page],
        next_cursor=next_cursor,
        has_more=has_more,
        limit=params.limit,
    )


@router.post("/alerts/{alert_id}/acknowledge", response_model=AlertOut, summary="Acknowledge")
async def acknowledge(
    request: Request,
    alert_id: int,
    session: WriteSessionDep,
    principal: PrincipalDep,
    resolve: bool = Query(False, description="Also mark the alert resolved"),
) -> AlertOut:
    alert = (await session.execute(select(Alert).where(Alert.id == alert_id))).scalar_one_or_none()
    if alert is None or (principal.tenant_id and alert.tenant_id != principal.tenant_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alert not found")

    alert.acknowledged_by = principal.email
    alert.acknowledged_at = datetime.now(UTC)
    if resolve:
        alert.status = "resolved"

    from ...security.audit import record_access

    await record_access(
        request=request,
        principal=principal,
        action="alert.resolve" if resolve else "alert.acknowledge",
        resource_type="alert",
        resource_id=str(alert_id),
        tenant_id=alert.tenant_id,
        redacted=False,
    )
    return alert_out(alert)


# --------------------------------------------------------------------------- #
# Drift
# --------------------------------------------------------------------------- #
@router.get("/drift", response_model=list[DriftPointOut], summary="Drift history")
async def drift_history(
    session: SessionDep,
    principal: PrincipalDep,
    tenant_id: str | None = Query(None),
    metric: str | None = Query(None),
    hours: int = Query(72, ge=1, le=24 * 90),
    limit: int = Query(200, ge=1, le=1000),
) -> list[DriftPointOut]:
    scoped = principal.scope_tenant(tenant_id)
    query = select(DriftSnapshot).where(
        DriftSnapshot.created_at >= datetime.now(UTC) - timedelta(hours=hours)
    )
    if scoped:
        query = query.where(DriftSnapshot.tenant_id == scoped)
    if metric:
        query = query.where(DriftSnapshot.metric == metric)
    rows = list(
        (
            await session.execute(query.order_by(DriftSnapshot.created_at.desc()).limit(limit))
        ).scalars()
    )
    return [
        DriftPointOut(
            metric=row.metric,
            window_start=row.window_start,
            window_end=row.window_end,
            mean=row.mean,
            sample_count=row.sample_count,
            baseline_mean=row.baseline_mean,
            z_score=row.z_score,
            drifted=row.drifted,
            created_at=row.created_at,
        )
        for row in rows
    ]


@router.get("/drift/baselines", summary="Active drift baselines")
async def list_baselines(
    session: SessionDep, principal: PrincipalDep, tenant_id: str | None = Query(None)
) -> list[dict[str, Any]]:
    scoped = principal.scope_tenant(tenant_id)
    query = select(DriftBaseline).where(DriftBaseline.is_active.is_(True))
    if scoped:
        query = query.where(DriftBaseline.tenant_id == scoped)
    rows = list((await session.execute(query)).scalars())
    return [
        {
            "tenant_id": row.tenant_id,
            "metric": row.metric,
            "mean": row.mean,
            "stddev": row.stddev,
            "sample_count": row.sample_count,
            "window_start": row.window_start.isoformat(),
            "window_end": row.window_end.isoformat(),
            "note": row.note,
            "captured_at": row.created_at.isoformat(),
        }
        for row in rows
    ]


@router.post("/drift/baseline", summary="Capture a healthy baseline")
async def capture_baseline(
    request: Request,
    session: WriteSessionDep,
    principal: PrincipalDep,
    settings: SettingsDep,
    tenant_id: str = Body(..., embed=True),
    metric: str = Body(..., embed=True),
    hours: int = Body(24, embed=True),
    note: str = Body("", embed=True),
) -> dict[str, Any]:
    """Freeze the last N hours as the reference this tenant is compared against.

    Deliberately a human action, not something the monitor does for itself. An
    auto-refreshing baseline tracks whatever the model is doing now and can
    therefore never detect that it got worse.
    """
    if principal.role is not Role.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only an admin can capture a drift baseline",
        )
    scoped = principal.scope_tenant(tenant_id) or tenant_id

    monitor = DriftMonitor(settings)
    baseline = await monitor.capture_baseline(
        session, tenant_id=scoped, metric=metric, hours=hours, note=note
    )
    if baseline is None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Need at least {settings.drift_min_samples} scores in the last {hours}h "
                "to capture a baseline. A baseline built on a handful of scores is noise."
            ),
        )

    from ...security.audit import record_access

    await record_access(
        request=request,
        principal=principal,
        action="drift.baseline_captured",
        resource_type="drift_baseline",
        resource_id=f"{scoped}:{metric}",
        tenant_id=scoped,
        redacted=False,
        detail={"mean": baseline.mean, "samples": baseline.sample_count, "hours": hours},
    )
    return {
        "tenant_id": scoped,
        "metric": metric,
        "mean": baseline.mean,
        "stddev": baseline.stddev,
        "sample_count": baseline.sample_count,
        "window_hours": hours,
    }


__all__ = ["router"]
