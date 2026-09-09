"""Shared FastAPI dependencies: the caller's principal and the tenant scope.

The role model has exactly two roles, and the distinction between them is a
single question with a single answer: **can this person see raw customer data?**
``admin`` yes, ``viewer`` no. Everything downstream -- which redactor runs, what
the audit row records -- derives from that one bit.

Tenant scoping is enforced here rather than in each endpoint. A scoped principal
asking for another tenant's data gets a 403, not a silently-ignored parameter
that returns their own data and looks like a bug.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from enum import Enum
from typing import Any

from fastapi import Depends, HTTPException, Query, Request, status

from ..db import get_db
from ..logging import get_logger
from ..security.tokens import TokenError, verify_token
from ..settings import Settings, get_settings

log = get_logger("obs_platform.deps")

UNAUTHENTICATED = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer"},
)


class Role(str, Enum):
    ADMIN = "admin"
    VIEWER = "viewer"

    @property
    def can_view_raw(self) -> bool:
        """Only an admin sees unredacted trace payloads."""
        return self is Role.ADMIN

    @classmethod
    def parse(cls, value: str | None) -> Role:
        """Unknown or missing role means viewer.

        Fail closed: a token from a misconfigured identity provider must not be
        able to reach customer PII by accident.
        """
        try:
            return cls((value or "viewer").lower())
        except ValueError:
            return cls.VIEWER


@dataclass(frozen=True)
class Principal:
    """Who is calling, and what they are allowed to see."""

    id: str
    email: str
    role: Role
    #: None means "every tenant". A value scopes every query to that tenant.
    tenant_id: str | None = None
    source: str = "local"

    @property
    def can_view_raw(self) -> bool:
        return self.role.can_view_raw

    def scope_tenant(self, requested: str | None) -> str | None:
        if self.tenant_id is None:
            return requested
        if requested and requested != self.tenant_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not permitted to read another tenant's data",
            )
        return self.tenant_id


def extract_token(request: Request) -> str | None:
    """Bearer header first, then the httpOnly cookie the dashboard sets."""
    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return request.cookies.get("obs_token")


async def get_principal(request: Request) -> Principal:
    """Resolve and verify the caller. Every read endpoint depends on this."""
    cached = getattr(request.state, "principal", None)
    if cached is not None:
        return cached

    token = extract_token(request)
    if not token:
        raise UNAUTHENTICATED

    settings = get_settings()
    try:
        claims = verify_token(token, settings)
    except TokenError as exc:
        log.info("auth.token_rejected", reason=str(exc)[:200], path=request.url.path)
        raise UNAUTHENTICATED from exc

    principal = Principal(
        id=claims.subject,
        email=claims.email or claims.subject,
        role=Role.parse(claims.role),
        tenant_id=claims.tenant_id,
        source="oidc" if claims.issuer != "obs-platform" else "local",
    )
    request.state.principal = principal
    return principal


async def require_admin(principal: Principal = Depends(get_principal)) -> Principal:
    if principal.role is not Role.ADMIN:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin role required")
    return principal


async def get_settings_dep() -> Settings:
    return get_settings()


async def read_session() -> AsyncIterator[Any]:
    """Yield a read-only session, and give it back even when the route raises.

    This used to re-wrap the dependency as ``async for s in get_read_session():
    yield s``, which looks like a harmless re-export and is not. When a route
    raises -- ``raise HTTPException(404)`` is the common case -- FastAPI throws
    that exception into *this* generator at the ``yield``. The exception
    propagates out, and the inner generator is left suspended inside its own
    ``async with``: its cleanup runs whenever the garbage collector or the
    loop's ``shutdown_asyncgens`` gets to it, not now. That cleanup is what
    returns the connection to the pool, so every 404 parked a connection for an
    unbounded time. Against Neon's pooled connection limit that is a slow
    strangle under exactly the traffic you cannot control -- clients asking for
    things that are not there.

    Awaiting the context manager directly makes the release deterministic:
    ``__aexit__`` runs on the way out, exception or not. It was found by the
    test suite refusing to exit -- a leaked connection kept aiosqlite's
    non-daemon worker thread alive, and the interpreter waited for it forever.
    """
    async with get_db().read_session() as session:
        yield session


@dataclass
class ListParams:
    """Query parameters shared by every list endpoint."""

    tenant_id: str | None
    limit: int
    cursor: str | None


async def list_params(
    tenant_id: str | None = Query(None, description="Filter to one tenant"),
    limit: int | None = Query(None, ge=1, le=500, description="Page size"),
    cursor: str | None = Query(None, description="Opaque keyset cursor from a previous page"),
    settings: Settings = Depends(get_settings_dep),
) -> ListParams:
    from .pagination import clamp_limit

    return ListParams(
        tenant_id=tenant_id,
        limit=clamp_limit(limit, settings.default_page_size, settings.max_page_size),
        cursor=cursor,
    )


__all__ = [
    "UNAUTHENTICATED",
    "ListParams",
    "Principal",
    "Role",
    "extract_token",
    "get_principal",
    "get_settings_dep",
    "list_params",
    "read_session",
    "require_admin",
]
