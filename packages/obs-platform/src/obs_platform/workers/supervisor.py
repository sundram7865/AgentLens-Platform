"""Worker supervisor.

Runs one or more consumer roles inside a single process as asyncio tasks.

Why one process can host several roles: Render's free tier has no
background-worker service type -- only web services. Rather than pretend
otherwise (or quietly require a paid plan for the deploy the README promises is
free), the same consumers can run either as their own service, several to a
process, or embedded in the API. The code path is identical in all three; only
``OBS_WORKER_ROLES`` and ``OBS_EMBED_WORKERS_IN_API`` change.

Each role keeps its **own consumer group**, so they read the same stream
independently and one falling behind never starves another.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

from ..db import Database
from ..logging import get_logger
from ..settings import Settings, get_settings
from .base import StreamConsumer

log = get_logger("obs_platform.supervisor")

ConsumerFactory = Callable[[Settings], Any]


def _storage(settings: Settings) -> StreamConsumer:
    from .storage_writer import StorageWriter

    return StorageWriter(settings=settings)


def _guardrail(settings: Settings) -> StreamConsumer:
    from .guardrail_scanner import GuardrailScanner

    return GuardrailScanner(settings=settings)


def _eval(settings: Settings) -> StreamConsumer:
    from .eval_scorer import EvalScorer

    return EvalScorer(settings=settings)


def _scheduler(settings: Settings) -> Any:
    from .scheduler import Scheduler

    return Scheduler(settings=settings)


#: Role name -> factory. Roles are opt-in via OBS_WORKER_ROLES.
ROLES: dict[str, ConsumerFactory] = {
    "storage": _storage,
    "guardrail": _guardrail,
    "eval": _eval,
    "scheduler": _scheduler,
}


class WorkerSupervisor:
    """Starts, supervises and gracefully stops a set of workers."""

    def __init__(
        self,
        settings: Settings | None = None,
        roles: list[str] | None = None,
        database: Database | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.roles = roles if roles is not None else self.settings.worker_role_list
        self.database = database
        self.workers: list[Any] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._stopped = False

    def build(self) -> list[Any]:
        workers: list[Any] = []
        for role in self.roles:
            factory = ROLES.get(role)
            if factory is None:
                log.warning("supervisor.unknown_role", role=role, known=sorted(ROLES))
                continue
            worker = factory(self.settings)
            if self.database is not None:
                worker._db = self.database
            workers.append(worker)
        return workers

    async def start(self) -> None:
        self.workers = self.build()
        if not self.workers:
            log.warning("supervisor.no_workers", requested=self.roles)
            return
        for worker in self.workers:
            self._tasks.append(asyncio.create_task(worker.run(), name=f"obs-{worker.role}"))
        log.info("supervisor.started", roles=[w.role for w in self.workers])

    async def stop(self, timeout: float = 30.0) -> None:  # noqa: ASYNC109 - grace period, passed to asyncio.wait
        """Ask every worker to finish its in-flight batch, then wait.

        Only after the grace period do we cancel -- cancelling first is what
        turns a redeploy into a lost or duplicated message.
        """
        if self._stopped:
            return
        self._stopped = True
        for worker in self.workers:
            worker.request_stop()
        if not self._tasks:
            return
        done, pending = await asyncio.wait(self._tasks, timeout=timeout)
        for task in pending:
            log.warning("supervisor.force_cancel", task=task.get_name())
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exc = task.exception() if not task.cancelled() else None
            if exc is not None:
                log.error("supervisor.worker_crashed", task=task.get_name(), error=repr(exc))
        log.info("supervisor.stopped")

    async def run_forever(self) -> None:
        """Start, then block until every worker exits (or a signal stops them)."""
        await self.start()
        if not self._tasks:
            return
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def request_stop(self) -> None:
        for worker in self.workers:
            worker.request_stop()


__all__ = ["ROLES", "WorkerSupervisor"]
