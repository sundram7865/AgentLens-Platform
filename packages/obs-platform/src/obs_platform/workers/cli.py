"""Worker entrypoint: ``python -m obs_platform.workers.cli --roles storage,guardrail``.

Owns process-level concerns only: argument parsing, logging setup, signal
wiring, and clean disposal of the database and Redis connections. The actual
shutdown sequencing lives in the supervisor and the consumer base, so it behaves
identically whether workers run standalone or embedded in the API.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

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
    return parser.parse_args(argv)


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
    roles = [r.strip().lower() for r in args.roles.split(",") if r.strip()] if args.roles else None
    try:
        return asyncio.run(run(roles, args.shutdown_timeout))
    except KeyboardInterrupt:  # pragma: no cover - interactive
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
