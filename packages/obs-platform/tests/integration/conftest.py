"""Integration fixtures: real Postgres, real Redis.

The unit suite runs on SQLite and fakeredis so it needs no containers and stays
a sub-minute job. That is the right default, but it means a whole class of bug
is invisible to it -- anything where the real engine behaves differently:

* ``ON CONFLICT`` against a real unique index, and Postgres's refusal to touch
  the same row twice in one statement
* JSONB storage, round-tripping and containment operators
* ``percentile_cont``, which SQLite does not have at all
* real ``XAUTOCLAIM`` / ``XPENDING`` semantics and message distribution across
  several consumers in one group
* actual concurrency, rather than the serialised writers SQLite gives you

These tests are opt-in. Without ``OBS_INTEGRATION=1`` and reachable services
they skip, so `pytest` on a laptop with no Docker still passes cleanly.

    docker compose up -d postgres redis
    OBS_INTEGRATION=1 pytest -m integration
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from typing import Any

import pytest

pytestmark = pytest.mark.integration

PG_URL = os.environ.get("OBS_TEST_DATABASE_URL", "postgresql+asyncpg://obs:obs@localhost:5432/obs")
REDIS_URL = os.environ.get("OBS_TEST_REDIS_URL", "redis://localhost:6379/1")

ENABLED = os.environ.get("OBS_INTEGRATION", "").strip().lower() in {"1", "true", "yes"}


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    """Mark and skip everything in this package unless integration is requested."""
    skip = pytest.mark.skip(
        reason="integration tests need OBS_INTEGRATION=1 plus a running Postgres and Redis"
    )
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
            if not ENABLED:
                item.add_marker(skip)


@pytest.fixture(scope="session")
def integration_enabled() -> bool:
    if not ENABLED:
        pytest.skip("set OBS_INTEGRATION=1 to run integration tests")
    return True


@pytest.fixture
def settings(integration_enabled: bool, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Settings pointed at the real services, overriding the unit fixture."""
    from obs_platform.settings import get_settings, reset_settings_cache

    monkeypatch.setenv("OBS_DATABASE_URL", PG_URL)
    monkeypatch.setenv("OBS_DATABASE_DIRECT_URL", PG_URL.replace("+asyncpg", ""))
    monkeypatch.setenv("OBS_REDIS_URL", REDIS_URL)
    monkeypatch.setenv("OBS_ENVIRONMENT", "test")
    reset_settings_cache()
    settings = get_settings()
    settings.consumer_block_ms = 50
    try:
        yield settings
    finally:
        reset_settings_cache()


@pytest.fixture
async def database(settings: Any) -> AsyncIterator[Any]:
    """A real Postgres database with a clean schema.

    Tables are created from the models and truncated between tests rather than
    dropped and recreated: TRUNCATE is far faster, and a schema that survives
    the whole session is closer to how the thing actually runs.
    """
    from sqlalchemy import text

    from obs_platform import db as db_module
    from obs_platform.db import Database
    from obs_platform.models import Base

    try:
        database = Database(settings)
        async with database.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"Postgres unreachable at {PG_URL}: {exc}")

    tables = ", ".join(f'"{name}"' for name in Base.metadata.tables)
    async with database.engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))

    db_module.set_db(database)
    try:
        yield database
    finally:
        db_module.set_db(None)
        await database.dispose()


@pytest.fixture
async def redis(settings: Any) -> AsyncIterator[Any]:
    """A real Redis, on a dedicated database index, flushed between tests."""
    from redis.asyncio import Redis

    from obs_platform import redis_io

    client = Redis.from_url(REDIS_URL, decode_responses=True)
    try:
        await client.ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        await client.aclose()
        pytest.skip(f"Redis unreachable at {REDIS_URL}: {exc}")

    await client.flushdb()
    redis_io.set_redis(client)
    try:
        yield client
    finally:
        await client.flushdb()
        redis_io.set_redis(None)
        await client.aclose()


@pytest.fixture
async def app(settings: Any, database: Any, redis: Any) -> Any:
    from obs_platform.api.app import create_app

    return create_app(settings)
