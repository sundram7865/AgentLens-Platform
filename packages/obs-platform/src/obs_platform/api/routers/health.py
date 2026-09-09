"""Health endpoints.

Three levels, because they answer three different questions:

``/health``      Is the process alive? Never touches a dependency, so a Redis
                 outage cannot make the platform look dead to the load balancer.
``/health/ready``Can it serve traffic? Checks Postgres and Redis; returns 503
                 when it cannot, which is what a deploy gate should read.
``/health/meta`` Is the observability platform itself healthy? Consumer lag,
                 last successful write, worker heartbeats, dead-letter count.
                 A tool that watches other systems and cannot notice its own
                 failure is worse than no tool, because it is trusted.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Response
from sqlalchemy import func, select

from ...db import get_db
from ...logging import get_logger
from ...models import DeadLetter, Span, WorkerHeartbeat
from ...redis_io import get_redis, stream_health
from ...settings import get_settings

router = APIRouter(tags=["health"])
log = get_logger("obs_platform.health")

_STARTED_AT = time.time()


@router.get("/health", summary="Liveness")
async def health() -> dict[str, Any]:
    settings = get_settings()
    return {
        "status": "ok",
        "service": settings.service_name,
        "version": settings.version,
        "environment": settings.environment,
        "uptime_seconds": round(time.time() - _STARTED_AT, 1),
    }


@router.get("/health/live", include_in_schema=False)
async def live() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/health/ready", summary="Readiness")
async def ready(response: Response) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    ok = True

    started = time.perf_counter()
    try:
        await get_db().ping()
        checks["database"] = {
            "status": "ok",
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        }
    except Exception as exc:
        ok = False
        checks["database"] = {"status": "error", "error": str(exc)[:300]}

    started = time.perf_counter()
    try:
        client = get_redis()
        await client.ping()
        checks["redis"] = {
            "status": "ok",
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
        }
    except Exception as exc:
        ok = False
        checks["redis"] = {"status": "error", "error": str(exc)[:300]}

    if not ok:
        response.status_code = 503
    return {"status": "ok" if ok else "degraded", "checks": checks}


@router.get("/health/meta", summary="Self-observability")
async def meta(response: Response) -> dict[str, Any]:
    """Report on the platform's own pipeline: lag, freshness, workers, dead letters."""
    settings = get_settings()
    now = datetime.now(UTC)
    problems: list[str] = []
    payload: dict[str, Any] = {"checked_at": now.isoformat()}

    # -- stream lag ---------------------------------------------------------
    try:
        streams = await stream_health()
        payload["streams"] = streams
        for group in streams.get("groups", []):
            if group.get("lag", 0) > settings.meta_health_max_lag:
                problems.append(
                    "consumer group {} is {} messages behind".format(
                        group.get("name"), group.get("lag")
                    )
                )
    except Exception as exc:
        payload["streams"] = {"status": "error", "error": str(exc)[:300]}
        problems.append("redis unreachable")

    # -- write freshness and workers ---------------------------------------
    try:
        async with get_db().read_session() as session:
            last_write = (await session.execute(select(func.max(Span.created_at)))).scalar()
            dead_letters = (
                await session.execute(
                    select(func.count())
                    .select_from(DeadLetter)
                    .where(DeadLetter.replayed_at.is_(None))
                )
            ).scalar() or 0
            # Selected as columns, not ORM objects, and consumed inside the
            # session. read_session() rolls back on exit, which expires any
            # ORM instance; touching one afterwards raises DetachedInstanceError,
            # gets caught below, and reports "database unreachable" while the
            # database is perfectly healthy -- sending whoever is on call in
            # exactly the wrong direction.
            worker_rows = (
                await session.execute(
                    select(
                        WorkerHeartbeat.name,
                        WorkerHeartbeat.role,
                        WorkerHeartbeat.status,
                        WorkerHeartbeat.processed,
                        WorkerHeartbeat.failed,
                        WorkerHeartbeat.dead_lettered,
                        WorkerHeartbeat.last_seen_at,
                    )
                )
            ).all()

        write_age = None
        if last_write is not None:
            if last_write.tzinfo is None:
                last_write = last_write.replace(tzinfo=UTC)
            write_age = round((now - last_write).total_seconds(), 1)

        payload["last_span_write_at"] = last_write.isoformat() if last_write else None
        payload["last_span_write_age_seconds"] = write_age
        payload["unreplayed_dead_letters"] = int(dead_letters)
        payload["workers"] = [
            {
                "name": name,
                "role": role,
                "status": status_value,
                "processed": processed,
                "failed": failed,
                "dead_lettered": dead_lettered,
                "last_seen_at": last_seen.isoformat() if last_seen else None,
                "seconds_since_seen": _age(now, last_seen),
            }
            for name, role, status_value, processed, failed, dead_lettered, last_seen in (
                worker_rows
            )
        ]

        stale_after = settings.worker_heartbeat_seconds * 4
        for worker in payload["workers"]:
            age = worker["seconds_since_seen"]
            if age is not None and age > stale_after:
                problems.append("worker {} last seen {}s ago".format(worker["name"], int(age)))

        # Only complain about write freshness once traffic has actually started;
        # a brand-new deployment with zero traffic is healthy, not stale.
        if (
            write_age is not None
            and write_age > settings.meta_health_max_write_age_seconds
            and payload.get("streams", {}).get("length", 0) > 0
        ):
            problems.append(f"no span written for {int(write_age)}s")
        if dead_letters:
            problems.append(f"{dead_letters} unreplayed dead letters")
    except Exception as exc:
        # Report what actually failed. "database unreachable" for every
        # exception is how a serialisation bug gets triaged as an outage.
        payload["database"] = {
            "status": "error",
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
        }
        problems.append(f"database check failed: {type(exc).__name__}")

    payload["status"] = "ok" if not problems else "degraded"
    payload["problems"] = problems
    if problems:
        response.status_code = 503
    return payload


def _age(now: datetime, value: datetime | None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return round((now - value).total_seconds(), 1)


__all__ = ["router"]
