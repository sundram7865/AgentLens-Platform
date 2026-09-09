"""HTTP middleware: request identity, structured access logs, safety headers."""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ..logging import get_logger, log_context

log = get_logger("obs_platform.api")

REQUEST_ID_HEADER = "X-Request-ID"

# Paths that would otherwise dominate the log volume with no information value.
_QUIET_PATHS = {"/health", "/health/live", "/metrics", "/favicon.ico"}


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Attach a request id, bind it to the log context, emit one access line.

    An inbound ``X-Request-ID`` is honoured so a request can be followed across
    the dashboard, the API and the workers with a single grep.
    """

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request.headers.get(REQUEST_ID_HEADER) or uuid.uuid4().hex[:16]
        request.state.request_id = request_id
        started = time.perf_counter()

        with log_context(request_id=request_id):
            try:
                response = await call_next(request)
            except Exception:
                duration_ms = int((time.perf_counter() - started) * 1000)
                log.exception(
                    "request.failed",
                    method=request.method,
                    path=request.url.path,
                    duration_ms=duration_ms,
                )
                raise
            duration_ms = int((time.perf_counter() - started) * 1000)
            response.headers[REQUEST_ID_HEADER] = request_id
            if request.url.path not in _QUIET_PATHS:
                log.info(
                    "request.completed",
                    method=request.method,
                    path=request.url.path,
                    status=response.status_code,
                    duration_ms=duration_ms,
                )
            return response


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Conservative response headers. The API serves JSON to a separate origin."""

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        response = await call_next(request)
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        # Trace payloads can contain customer data; caching them at a proxy or in
        # a browser would put PII somewhere the redaction layer cannot reach.
        if request.url.path.startswith("/v1/"):
            response.headers.setdefault("Cache-Control", "no-store")
        return response


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-caller quota on the data API.

    Only ``/v1/*`` is limited. Health checks must never be throttled -- a load
    balancer that gets a 429 from /health takes the instance out of rotation,
    turning a rate limit into an outage. Login has its own, much tighter limit
    inside the endpoint, where the identity is known before any work is done.
    """

    EXEMPT_PREFIXES = ("/health", "/docs", "/openapi.json", "/v1/auth/login")

    def __init__(self, app: Any, limit: int, window_seconds: int, enabled: bool = True) -> None:
        super().__init__(app)
        self.limit = limit
        self.window_seconds = window_seconds
        self.enabled = enabled

    async def dispatch(
        self, request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        path = request.url.path
        if not self.enabled or not path.startswith("/v1/") or path.startswith(self.EXEMPT_PREFIXES):
            return await call_next(request)

        from ..security.ratelimit import check_rate_limit, client_identity

        # Resolve the caller BEFORE choosing a rate-limit key. Middleware runs
        # ahead of dependency resolution, so request.state.principal is unset at
        # this point unless we set it -- which meant every authenticated caller
        # was silently keyed by IP, and one noisy account throttled everyone
        # behind the same NAT or proxy. get_principal() reuses what we cache
        # here, so the token is verified once per request, not twice.
        _resolve_principal(request)

        result = await check_rate_limit(client_identity(request), self.limit, self.window_seconds)
        if not result.allowed:
            log.warning("ratelimit.exceeded", path=path, identity=client_identity(request))
            return JSONResponse(
                status_code=429,
                content={
                    "error": {
                        "type": "rate_limited",
                        "message": "Too many requests",
                        "retry_after_seconds": result.reset_after,
                    }
                },
                headers=result.headers(),
            )
        response = await call_next(request)
        for key, value in result.headers().items():
            response.headers.setdefault(key, value)
        return response


def _resolve_principal(request: Request) -> None:
    """Best-effort identity for rate limiting. Never rejects: a bad or missing
    token simply falls back to IP keying, and the endpoint's own dependency is
    what returns 401."""
    if getattr(request.state, "principal", None) is not None:
        return
    from ..api.deps import Principal, Role, extract_token
    from ..security.tokens import verify_token
    from ..settings import get_settings

    token = extract_token(request)
    if not token:
        return
    try:
        claims = verify_token(token, get_settings())
    except Exception:
        # Any failure here just means "anonymous for rate-limiting purposes".
        # TokenError is the expected one; anything else (a JWKS fetch blowing
        # up, say) must not turn into a 500 in middleware, because the endpoint
        # dependency is what owns rejecting the request.
        return
    request.state.principal = Principal(
        id=claims.subject,
        email=claims.email or claims.subject,
        role=Role.parse(claims.role),
        tenant_id=claims.tenant_id,
        source="oidc" if claims.issuer != "obs-platform" else "local",
    )


__all__ = [
    "REQUEST_ID_HEADER",
    "RateLimitMiddleware",
    "RequestContextMiddleware",
    "SecurityHeadersMiddleware",
]
