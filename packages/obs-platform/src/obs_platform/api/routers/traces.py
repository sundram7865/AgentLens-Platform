"""Trace read API.

Read-only by construction: the session comes from ``get_read_session``, which
rolls back on exit, so a stray write in this layer cannot commit. The dashboard
talks only to endpoints in this file and its siblings, and never to Redis.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from ...models import EvalScore, GuardrailFinding, Span, Trace
from ...settings import Settings
from ..deps import ListParams, Principal, get_principal, get_settings_dep, list_params, read_session
from ..pagination import build_page, decode_cursor, encode_cursor
from ..schemas import Page, TenantOut, TraceDetail, TraceSummary
from ..serializers import trace_detail, trace_summary

router = APIRouter(prefix="/v1/traces", tags=["traces"])

SessionDep = Annotated[AsyncSession, Depends(read_session)]
PrincipalDep = Annotated[Principal, Depends(get_principal)]
ParamsDep = Annotated[ListParams, Depends(list_params)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]


def _redactor_for(principal: Principal) -> Any:
    """Server-side redaction level, chosen by role before the body is built.

    Never returns None. An admin still gets secrets masked; only the PII layer
    is role-dependent.
    """
    from ...security.redaction import redactor_for_role

    return redactor_for_role(principal.can_view_raw)


@router.get("", response_model=Page[TraceSummary], summary="List traces")
async def list_traces(
    request: Request,
    session: SessionDep,
    principal: PrincipalDep,
    params: ParamsDep,
    status_filter: str | None = Query(None, alias="status", description="ok | error | running"),
    service: str | None = Query(None),
    flagged: bool | None = Query(None, description="Only traces with guardrail findings"),
    has_errors: bool | None = Query(None),
    ticket_id: str | None = Query(None, description="SupportPilot ticket id"),
    since_hours: int | None = Query(None, ge=1, le=24 * 90),
    search: str | None = Query(None, max_length=200, description="Match trace id or name"),
) -> Page[TraceSummary]:
    tenant_id = principal.scope_tenant(params.tenant_id)

    query = select(Trace)
    if tenant_id:
        query = query.where(Trace.tenant_id == tenant_id)
    if status_filter:
        query = query.where(Trace.status == status_filter)
    if service:
        query = query.where(Trace.service == service)
    if flagged is not None:
        query = (
            query.where(Trace.guardrail_status == "flagged")
            if flagged
            else query.where(Trace.guardrail_status != "flagged")
        )
    if has_errors is not None:
        query = query.where(Trace.error_count > 0 if has_errors else Trace.error_count == 0)
    if ticket_id:
        # JSON path indexing renders as ->> on Postgres and json_extract on
        # SQLite, so one expression serves both.
        query = query.where(Trace.attributes["ticket_id"].as_string() == ticket_id)
    if since_hours:
        query = query.where(Trace.created_at >= datetime.now(UTC) - timedelta(hours=since_hours))
    if search:
        pattern = f"%{search.strip()}%"
        query = query.where(Trace.trace_id.ilike(pattern) | Trace.name.ilike(pattern))

    if params.cursor:
        cursor_created, cursor_id = decode_cursor(params.cursor)
        query = query.where(tuple_(Trace.created_at, Trace.trace_id) < (cursor_created, cursor_id))

    # Sorted by created_at (ingestion time): never NULL and always increasing,
    # which is what makes the keyset cursor total and stable. started_at is
    # shown for display but is not the sort key -- a NULL sort key would break
    # pagination for exactly the malformed traces you most want to inspect.
    query = query.order_by(Trace.created_at.desc(), Trace.trace_id.desc()).limit(params.limit + 1)

    rows = list((await session.execute(query)).scalars())
    page, next_cursor, has_more = build_page(
        rows, params.limit, lambda t: encode_cursor(t.created_at, t.trace_id)
    )

    redactor = _redactor_for(principal)
    await _audit(request, principal, "trace.list", "trace", "", tenant_id, redactor.full)
    return Page[TraceSummary](
        items=[trace_summary(trace, redactor) for trace in page],
        next_cursor=next_cursor,
        has_more=has_more,
        limit=params.limit,
    )


@router.get("/tenants", response_model=list[TenantOut], summary="Tenants with traces")
async def list_tenants(session: SessionDep, principal: PrincipalDep) -> list[TenantOut]:
    query = select(
        Trace.tenant_id, func.count().label("traces"), func.max(Trace.created_at)
    ).group_by(Trace.tenant_id)
    if principal.tenant_id:
        query = query.where(Trace.tenant_id == principal.tenant_id)
    rows = (await session.execute(query.order_by(func.count().desc()).limit(200))).all()
    return [
        TenantOut(tenant_id=tenant, traces=int(count), last_seen_at=last_seen)
        for tenant, count, last_seen in rows
    ]


@router.get("/{trace_id}", response_model=TraceDetail, summary="One trace with its call tree")
async def get_trace(
    request: Request,
    trace_id: str,
    session: SessionDep,
    principal: PrincipalDep,
) -> TraceDetail:
    trace = (
        await session.execute(select(Trace).where(Trace.trace_id == trace_id))
    ).scalar_one_or_none()
    if trace is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Trace not found")
    if principal.tenant_id and trace.tenant_id != principal.tenant_id:
        # 404, not 403: a 403 would confirm the trace exists to someone with no
        # right to know that.
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Trace not found")

    spans = list(
        (
            await session.execute(
                select(Span).where(Span.trace_id == trace_id).order_by(Span.started_at, Span.id)
            )
        ).scalars()
    )
    findings = list(
        (
            await session.execute(
                select(GuardrailFinding)
                .where(GuardrailFinding.trace_id == trace_id)
                .order_by(GuardrailFinding.id)
            )
        ).scalars()
    )
    scores = list(
        (
            await session.execute(
                select(EvalScore).where(EvalScore.trace_id == trace_id).order_by(EvalScore.metric)
            )
        ).scalars()
    )

    redactor = _redactor_for(principal)
    await _audit(
        request, principal, "trace.read", "trace", trace_id, trace.tenant_id, redactor.full
    )
    return trace_detail(trace, spans, findings, scores, redactor)


async def _audit(
    request: Request,
    principal: Principal,
    action: str,
    resource_type: str,
    resource_id: str,
    tenant_id: str | None,
    redacted: bool,
) -> None:
    """Record who read what. Imported lazily to keep this module import-cheap."""
    from ...security.audit import record_access

    await record_access(
        request=request,
        principal=principal,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        tenant_id=tenant_id,
        redacted=redacted,
    )


__all__ = ["router"]
