"""FastAPI application factory."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..db import dispose_db, get_db
from ..logging import configure_logging, get_logger
from ..redis_io import close_redis
from ..settings import Settings, get_settings
from .middleware import RateLimitMiddleware, RequestContextMiddleware, SecurityHeadersMiddleware
from .routers import alerts, auth, health, metrics, traces

log = get_logger("obs_platform.api")


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(
        level=settings.log_level, json_output=settings.log_json, service=settings.service_name
    )

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        log.info(
            "api.starting",
            environment=settings.environment,
            version=settings.version,
            embed_workers=settings.embed_workers_in_api,
        )
        await _bootstrap(settings)
        supervisor = None
        if settings.embed_workers_in_api:
            # Render's free tier has no background-worker service type, so the
            # consumers can be hosted inside the API process. Same code, same
            # graceful shutdown, one less service to pay for.
            from ..workers.supervisor import WorkerSupervisor

            supervisor = WorkerSupervisor(settings=settings)
            await supervisor.start()
            app.state.supervisor = supervisor
        try:
            yield
        finally:
            if supervisor is not None:
                await supervisor.stop()
            await dispose_db()
            await close_redis()
            log.info("api.stopped")

    app = FastAPI(
        title="AI Observability & Guardrails Platform",
        version=settings.version,
        description=(
            "Trace, guardrail and evaluate any LLM agent. Read-only API over "
            "Postgres; ingestion happens through Redis Streams."
        ),
        lifespan=lifespan,
        root_path=settings.api_root_path,
        docs_url="/docs" if settings.docs_enabled else None,
        redoc_url=None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
    )

    app.add_middleware(SecurityHeadersMiddleware)
    app.add_middleware(
        RateLimitMiddleware,
        limit=settings.rate_limit_requests,
        window_seconds=settings.rate_limit_window_seconds,
        enabled=settings.rate_limit_enabled,
    )
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
        expose_headers=["X-Request-ID"],
        max_age=600,
    )

    app.include_router(health.router)
    app.include_router(auth.router)
    app.include_router(traces.router)
    app.include_router(metrics.router)
    app.include_router(alerts.router)
    _register_error_handlers(app)

    @app.get("/", include_in_schema=False)
    async def root() -> dict[str, Any]:
        return {
            "service": settings.service_name,
            "version": settings.version,
            "docs": "/docs" if settings.docs_enabled else None,
            "health": "/health",
        }

    app.state.settings = settings
    app.state.db = get_db
    return app


async def _bootstrap(settings: Settings) -> None:
    """Create the first admin account on an empty deployment.

    Failing this must not stop the API from serving: a database that is briefly
    unreachable at boot should delay logins, not take the whole service down.
    """
    from ..security.users import ensure_bootstrap_admin

    try:
        async with get_db().session() as session:
            await ensure_bootstrap_admin(session, settings)
    except Exception:
        log.warning("api.bootstrap_failed", exc_info=True)


def _register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"type": "http_error", "message": exc.detail}},
            headers=getattr(exc, "headers", None),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content={
                "error": {
                    "type": "validation_error",
                    "message": "Request validation failed",
                    "details": exc.errors()[:20],
                }
            },
        )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        # Never leak an internal message to a caller: a stack trace or a DSN in
        # an error body is a genuine information disclosure.
        log.exception("request.unhandled", path=request.url.path)
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "type": "internal_error",
                    "message": "Internal server error",
                    "request_id": getattr(request.state, "request_id", None),
                }
            },
        )


__all__ = ["create_app"]
