"""Authentication endpoints.

Login is rate-limited harder than everything else (10 attempts per 5 minutes by
default). It is the one endpoint where an attacker gets unlimited free guesses,
and the general API limit is far too generous for credential stuffing.

Failures return one generic message regardless of cause. "No such user" versus
"wrong password" is an account-enumeration oracle, and combined with a password
reset flow it is how attackers build a target list.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from ...db import get_session
from ...logging import get_logger
from ...security import users as user_service
from ...security.audit import record_access
from ...security.passwords import validate_password_strength
from ...security.ratelimit import check_rate_limit
from ...security.tokens import issue_token
from ...settings import Settings
from ..deps import Principal, Role, get_principal, get_settings_dep
from ..schemas import LoginRequest, MeResponse, TokenResponse

router = APIRouter(prefix="/v1/auth", tags=["auth"])
log = get_logger("obs_platform.auth")

SessionDep = Annotated[AsyncSession, Depends(get_session)]
PrincipalDep = Annotated[Principal, Depends(get_principal)]
SettingsDep = Annotated[Settings, Depends(get_settings_dep)]

GENERIC_LOGIN_FAILURE = "Invalid email or password"
COOKIE_NAME = "obs_token"


@router.post("/login", response_model=TokenResponse, summary="Exchange credentials for a token")
async def login(
    request: Request,
    response: Response,
    payload: LoginRequest,
    session: SessionDep,
    settings: SettingsDep,
) -> TokenResponse:
    if settings.rate_limit_enabled:
        identity = f"login:{request.client.host if request.client else 'unknown'}"
        limit = await check_rate_limit(
            identity,
            settings.rate_limit_login_requests,
            settings.rate_limit_login_window_seconds,
            scope="login",
        )
        if not limit.allowed:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Too many login attempts. Try again shortly.",
                headers=limit.headers(),
            )

    user = await user_service.authenticate(session, payload.email, payload.password)
    if user is None:
        log.info(
            "auth.login_failed",
            email=user_service.normalize_email(payload.email),
            ip=request.client.host if request.client else None,
        )
        # One message for every failure mode: no such user, wrong password,
        # deactivated account.
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=GENERIC_LOGIN_FAILURE,
            headers={"WWW-Authenticate": "Bearer"},
        )

    token, expires_in = issue_token(
        settings, subject=user.id, email=user.email, role=user.role, tenant_id=user.tenant_id
    )
    # Also set an httpOnly cookie so the dashboard's server components can read
    # it without the token ever being reachable from browser JavaScript.
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=expires_in,
        httponly=True,
        samesite="lax",
        secure=settings.environment in ("production", "staging"),
        path="/",
    )
    log.info("auth.login_succeeded", email=user.email, role=user.role)
    return TokenResponse(
        access_token=token,
        expires_in=expires_in,
        role=user.role,
        email=user.email,
        tenant_id=user.tenant_id,
    )


@router.post("/logout", summary="Clear the session cookie")
async def logout(response: Response) -> dict[str, str]:
    response.delete_cookie(COOKIE_NAME, path="/")
    return {"status": "ok"}


@router.get("/me", response_model=MeResponse, summary="Who am I")
async def me(principal: PrincipalDep) -> MeResponse:
    return MeResponse(
        id=principal.id,
        email=principal.email,
        role=principal.role.value,
        tenant_id=principal.tenant_id,
        can_view_raw=principal.can_view_raw,
    )


@router.post("/users", summary="Create an operator account", status_code=201)
async def create_user(
    request: Request,
    session: SessionDep,
    principal: PrincipalDep,
    email: str,
    password: str,
    role: str = "viewer",
    tenant_id: str | None = None,
) -> dict[str, Any]:
    if principal.role is not Role.ADMIN:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Only an admin can create accounts"
        )
    if role not in user_service.ROLES:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=f"role must be one of {user_service.ROLES}",
        )
    problem = validate_password_strength(password)
    if problem:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=problem)
    if await user_service.get_by_email(session, email):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")

    user = await user_service.create_user(
        session, email=email, password=password, role=role, tenant_id=tenant_id
    )
    await record_access(
        request=request,
        principal=principal,
        action="user.create",
        resource_type="user",
        resource_id=user.id,
        tenant_id=tenant_id,
        redacted=False,
        detail={"email": user.email, "role": role},
    )
    return {"id": user.id, "email": user.email, "role": user.role, "tenant_id": user.tenant_id}


__all__ = ["COOKIE_NAME", "router"]
