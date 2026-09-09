"""Database engine, sessions, and the dialect-aware upsert helpers.

The upsert helpers matter more than they look. Redis Streams are at-least-once:
a consumer that crashes between doing the work and sending ``XACK`` will be
handed the same message again. Every write on the ingestion path therefore has
to be idempotent, and ``ON CONFLICT DO NOTHING`` / ``DO UPDATE`` against a real
unique constraint is how that is expressed in SQL rather than in hope.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator, Sequence
from typing import Any, cast

from sqlalchemy import Table, event, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool, StaticPool

from .logging import get_logger
from .settings import Settings, get_settings

log = get_logger("obs_platform.db")


class Database:
    """Owns one engine and its session factory for the life of a process."""

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self.engine: AsyncEngine = self._create_engine()
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self.engine, expire_on_commit=False, autoflush=False
        )

    def _create_engine(self) -> AsyncEngine:
        s = self.settings
        url = s.async_database_url
        kwargs: dict[str, Any] = {
            "echo": s.db_echo,
            "pool_pre_ping": True,
            "future": True,
        }

        if url.startswith("sqlite"):
            # SQLite is the test/dev fallback only. In-memory needs one shared
            # connection or every session gets its own empty database.
            kwargs["poolclass"] = StaticPool if ":memory:" in url else NullPool
            kwargs["connect_args"] = {"check_same_thread": False}
        else:
            kwargs.update(
                pool_size=s.db_pool_size,
                max_overflow=s.db_max_overflow,
                pool_recycle=s.db_pool_recycle_seconds,
                pool_timeout=s.db_pool_timeout_seconds,
                connect_args=s.async_connect_args,
            )

        engine = create_async_engine(url, **kwargs)
        if url.startswith("sqlite"):
            _enable_sqlite_foreign_keys(engine)
        return engine

    @contextlib.asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """A session that commits on success and rolls back on any exception."""
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    @contextlib.asynccontextmanager
    async def read_session(self) -> AsyncIterator[AsyncSession]:
        """A session for reads. Rolls back at the end so nothing can leak a write.

        Rows loaded here stay readable after the block exits. That needs saying,
        because the obvious implementation -- just ``rollback()`` -- does the
        opposite: rollback *expires* every ORM instance, so the next attribute
        access tries to refresh from a closed session and raises
        ``DetachedInstanceError``. Code that reads rows inside the block and
        renders them just outside it looks completely correct and fails at
        runtime.

        That trap already produced one production bug in this codebase: the
        /health/meta endpoint caught the resulting exception in a broad
        ``except`` and reported "database unreachable" while the database was
        perfectly healthy. Expunging first detaches the instances with their
        loaded values intact, which is exactly what a read-only snapshot should
        be, and removes the trap rather than documenting it.
        """
        async with self.session_factory() as session:
            try:
                yield session
            finally:
                session.expunge_all()
                await session.rollback()

    async def ping(self) -> bool:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True

    async def create_all(self) -> None:
        """Schema straight from the models. Tests only -- production uses Alembic."""
        from .models import Base

        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def dispose(self) -> None:
        await self.engine.dispose()


def _enable_sqlite_foreign_keys(engine: AsyncEngine) -> None:
    @event.listens_for(engine.sync_engine, "connect")
    def _set_pragma(dbapi_connection: Any, _record: Any) -> None:  # pragma: no cover - driver hook
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


# --------------------------------------------------------------------------- #
# Process-wide instance
# --------------------------------------------------------------------------- #
_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


def set_db(db: Database | None) -> None:
    """Swap the process-wide database. Used by the test fixtures."""
    global _db
    _db = db


async def dispose_db() -> None:
    global _db
    if _db is not None:
        await _db.dispose()
        _db = None


# --------------------------------------------------------------------------- #
# Dialect-aware upserts
# --------------------------------------------------------------------------- #
def table_of(model: Any) -> Table:
    """Narrow a declarative model's ``__table__`` to ``Table``.

    SQLAlchemy 2.0 types ``DeclarativeBase.__table__`` as ``FromClause`` because
    a mapper can be built on a join or a subquery. Ours never are, so the cast is
    safe -- done once here rather than as a ``type: ignore`` at every call site.
    """
    return cast(Table, model.__table__)


def _dialect_insert(session: AsyncSession, table: Table) -> Any:
    name = session.bind.dialect.name if session.bind is not None else "postgresql"
    if name == "postgresql":
        return pg_insert(table)
    if name == "sqlite":
        return sqlite_insert(table)
    return insert(table)


def _supports_on_conflict(session: AsyncSession) -> bool:
    name = session.bind.dialect.name if session.bind is not None else "postgresql"
    return name in {"postgresql", "sqlite"}


async def insert_ignore(
    session: AsyncSession,
    table: Table,
    rows: Sequence[dict[str, Any]],
    index_elements: Sequence[str],
) -> int:
    """``INSERT ... ON CONFLICT DO NOTHING``. Returns the number of rows actually written.

    This is the ingestion path's idempotency guarantee. Redelivery of an already
    stored span is a no-op instead of a duplicate row.
    """
    if not rows:
        return 0
    stmt = _dialect_insert(session, table).values(list(rows))
    if _supports_on_conflict(session):
        stmt = stmt.on_conflict_do_nothing(index_elements=list(index_elements))
    result = await session.execute(stmt)
    return int(result.rowcount or 0)


async def upsert(
    session: AsyncSession,
    table: Table,
    row: dict[str, Any],
    index_elements: Sequence[str],
    update_columns: Sequence[str] | None = None,
    update_values: dict[str, Any] | None = None,
) -> None:
    """``INSERT ... ON CONFLICT DO UPDATE``, updating only the named columns."""
    stmt = _dialect_insert(session, table).values(row)
    if not _supports_on_conflict(session):  # pragma: no cover - only exotic dialects
        await session.execute(stmt)
        return
    setters: dict[str, Any] = dict(update_values or {})
    for column in update_columns or ():
        if column not in setters:
            setters[column] = getattr(stmt.excluded, column)
    if not setters:
        stmt = stmt.on_conflict_do_nothing(index_elements=list(index_elements))
    else:
        stmt = stmt.on_conflict_do_update(index_elements=list(index_elements), set_=setters)
    await session.execute(stmt)


async def upsert_many(
    session: AsyncSession,
    table: Table,
    rows: Sequence[dict[str, Any]],
    index_elements: Sequence[str],
    update_columns: Sequence[str],
) -> None:
    """Multi-row ``INSERT ... ON CONFLICT DO UPDATE`` in a single statement.

    **The caller must have deduplicated `rows` by the conflict key.** Postgres
    raises "ON CONFLICT DO UPDATE command cannot affect row a second time" if one
    statement touches the same row twice, and that is a runtime error, not a
    silent one.

    Worth the caveat: one statement instead of N is the difference between one
    network round trip per batch and one per span, which on a pooled Neon
    connection dominates the wall time of the whole ingestion path.
    """
    if not rows:
        return
    stmt = _dialect_insert(session, table).values(list(rows))
    if not _supports_on_conflict(session):  # pragma: no cover - exotic dialects
        await session.execute(stmt)
        return
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=list(index_elements),
            set_={column: getattr(stmt.excluded, column) for column in update_columns},
        )
    )


def excluded(table_insert: Any, column: str) -> Any:
    """Reference the would-be-inserted value inside an ON CONFLICT update clause."""
    return getattr(table_insert.excluded, column)


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a read/write session."""
    async with get_db().session() as session:
        yield session


async def get_read_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding a session that cannot commit.

    The dashboard is read-only by contract; this makes that structural rather
    than a rule someone has to remember.
    """
    async with get_db().read_session() as session:
        yield session


__all__ = [
    "Database",
    "dispose_db",
    "excluded",
    "get_db",
    "get_read_session",
    "get_session",
    "insert_ignore",
    "set_db",
    "table_of",
    "upsert",
    "upsert_many",
]
