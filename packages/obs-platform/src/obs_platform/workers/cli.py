"""Worker entrypoint: ``python -m obs_platform.workers.cli --roles storage,guardrail``.

Owns process-level concerns only: argument parsing, logging setup, signal
wiring, and clean disposal of the database and Redis connections. The actual
shutdown sequencing lives in the supervisor and the consumer base, so it behaves
identically whether workers run standalone or embedded in the API.
"""

from __future__ import annotations

import argparse
import asyncio
import socket
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

from ..db import dispose_db
from ..logging import configure_logging, get_logger
from ..redis_io import close_redis
from ..settings import get_settings
from .base import install_signal_handlers
from .supervisor import ROLES, WorkerSupervisor


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="obs-worker",
        description="Run AI Observability Platform stream consumers and scheduled jobs.",
    )
    parser.add_argument(
        "--roles",
        default=None,
        help="Comma-separated roles to run. Default: OBS_WORKER_ROLES. Known: "
        + ", ".join(sorted(ROLES)),
    )
    parser.add_argument(
        "--shutdown-timeout",
        type=float,
        default=30.0,
        help="Seconds to let in-flight work finish after SIGTERM (default: 30).",
    )
    parser.add_argument(
        "--healthcheck",
        action="store_true",
        help="Exit 0 if every role running on this host heartbeated recently, else 1. "
        "Used by the container HEALTHCHECK; starts no workers.",
    )
    return parser.parse_args(argv)


async def healthcheck(database: Any | None = None, host: str | None = None) -> int:
    """Liveness for a worker container, from the heartbeats its roles write.

    The image's HEALTHCHECK used to curl the API port for every role. A worker
    serves no HTTP, so every worker container was permanently "unhealthy" --
    which hides a worker that has really stopped behind one that never looked
    alive. Staleness uses the same rule as /health/meta: four missed beats.
    """
    from sqlalchemy import select

    from ..db import get_db
    from ..models import WorkerHeartbeat

    settings = get_settings()
    db = database or get_db()
    host = (host or socket.gethostname())[:200]
    cutoff = datetime.now(UTC) - timedelta(seconds=settings.worker_heartbeat_seconds * 4)
    try:
        async with db.session() as session:
            rows = (
                await session.execute(
                    select(WorkerHeartbeat.last_seen_at).where(
                        WorkerHeartbeat.host == host, WorkerHeartbeat.status == "running"
                    )
                )
            ).all()
    except Exception as exc:
        sys.stderr.write(f"unhealthy: cannot read heartbeats: {exc!r}\n")
        return 1
    finally:
        if database is None:
            await dispose_db()
    if not rows:
        sys.stderr.write(f"unhealthy: no running role has heartbeated from {host}\n")
        return 1
    stale = [
        r.last_seen_at
        for r in rows
        if (r.last_seen_at if r.last_seen_at.tzinfo else r.last_seen_at.replace(tzinfo=UTC))
        < cutoff
    ]
    if stale:
        sys.stderr.write(f"unhealthy: {len(stale)} of {len(rows)} roles are stale\n")
        return 1
    return 0


async def run(roles: list[str] | None, shutdown_timeout: float) -> int:
    settings = get_settings()
    configure_logging(
        level=settings.log_level,
        json_output=settings.log_json,
        service="obs-worker",
    )
    log = get_logger("obs_platform.worker.cli")

    supervisor = WorkerSupervisor(settings=settings, roles=roles)
    stop_event = asyncio.Event()

    def _stop() -> None:
        # SIGTERM arrives on every redeploy. Set the flag; the supervisor lets
        # each worker finish its current batch before the process exits.
        supervisor.request_stop()
        stop_event.set()

    install_signal_handlers(_stop)

    await supervisor.start()
    if not supervisor.workers:
        log.error("worker.no_roles", requested=roles or settings.worker_role_list)
        return 2

    try:
        await asyncio.wait(
            [asyncio.create_task(stop_event.wait()), *supervisor._tasks],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        await supervisor.stop(timeout=shutdown_timeout)
        await dispose_db()
        await close_redis()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.healthcheck:
        return asyncio.run(healthcheck())
    roles = [r.strip().lower() for r in args.roles.split(",") if r.strip()] if args.roles else None
    try:
        return asyncio.run(run(roles, args.shutdown_timeout))
    except KeyboardInterrupt:  # pragma: no cover - interactive
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
