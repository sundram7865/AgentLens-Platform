"""Structured logging.

Every line is JSON with a ``trace_id`` when one is in scope, across the API and
every worker. That is what makes one request greppable end to end:

    grep tr_9f2c application.log | jq -s 'sort_by(.timestamp)'

Bindings live in ``structlog.contextvars``, so they follow an asyncio task
without being passed through every function signature, and cannot leak between
concurrently-served requests the way a module global would.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Iterator
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, unbind_contextvars

_configured = False


def configure_logging(
    level: str = "INFO", json_output: bool = True, service: str = "obs-platform"
) -> None:
    """Configure structlog and route stdlib logging (uvicorn, sqlalchemy) through it."""
    global _configured

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    structlog.configure(
        processors=[
            *shared,
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    # Replace rather than append: uvicorn installs its own handlers and we would
    # otherwise emit every line twice, once JSON and once not.
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    root.setLevel(level.upper())

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"):
        stdlib_logger = logging.getLogger(name)
        stdlib_logger.handlers = []
        stdlib_logger.propagate = True

    # SQLAlchemy's engine logger is deafening at INFO once echo is on.
    logging.getLogger("sqlalchemy.engine").setLevel("WARNING")
    logging.getLogger("asyncio").setLevel("WARNING")

    structlog.contextvars.bind_contextvars(service=service)
    _configured = True


def get_logger(name: str | None = None) -> Any:
    if not _configured:
        configure_logging()
    return structlog.get_logger(name)


class log_context:
    """Bind fields for the duration of a block, then unbind exactly those fields.

    ``clear_contextvars()`` would also wipe the service binding and anything an
    outer scope added, so we unbind by name instead.
    """

    def __init__(self, **fields: Any) -> None:
        self._fields = {k: v for k, v in fields.items() if v is not None}

    def __enter__(self) -> log_context:
        bind_contextvars(**self._fields)
        return self

    def __exit__(self, *exc: Any) -> None:
        unbind_contextvars(*self._fields.keys())


def bind_trace(trace_id: str | None = None, **fields: Any) -> None:
    """Bind trace identity onto every subsequent log line in this task."""
    payload = {k: v for k, v in {"trace_id": trace_id, **fields}.items() if v is not None}
    if payload:
        bind_contextvars(**payload)


def clear_trace() -> None:
    clear_contextvars()


def iter_handlers() -> Iterator[logging.Handler]:  # pragma: no cover - debugging aid
    yield from logging.getLogger().handlers


__all__ = [
    "bind_trace",
    "clear_trace",
    "configure_logging",
    "get_logger",
    "log_context",
]
