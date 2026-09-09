"""Shared fixtures.

The whole suite runs with **no containers**: SQLite via aiosqlite stands in for
Postgres and fakeredis stands in for Redis. That is a deliberate constraint --
a test suite that needs Docker is a test suite that stops being run, and CI on
the free tier stays a sub-minute job.

Where behaviour genuinely differs between SQLite and Postgres (``ON CONFLICT``
targeting, JSONB, ``percentile_cont``), the code paths are dialect-aware and the
Postgres path is covered by the opt-in tests marked ``integration``.

The default ``client`` is authenticated as an **admin**, because that is the
baseline every non-auth test wants. ``anon_client`` and ``viewer_client`` exist
so the authorisation tests can prove the difference rather than assume it.
"""

from __future__ import annotations

import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

# Test-only environment must be in place before Settings is first constructed.
os.environ.setdefault("OBS_ENVIRONMENT", "test")
os.environ.setdefault("OBS_LOG_LEVEL", "WARNING")
os.environ.setdefault("OBS_LOG_JSON", "false")
os.environ.setdefault("OBS_JWT_SECRET", "test-secret-key-that-is-definitely-long-enough-123456")
os.environ.setdefault("OBS_RATE_LIMIT_ENABLED", "false")
os.environ.setdefault("OBS_EVAL_PROVIDER", "none")
os.environ.setdefault("OBS_DOCS_ENABLED", "true")
os.environ.setdefault("OBS_BOOTSTRAP_ADMIN_EMAIL", "")
os.environ.setdefault("OBS_BOOTSTRAP_ADMIN_PASSWORD", "")

ROOT = Path(__file__).resolve().parents[3]
for src in (ROOT / "packages/obs-sdk/src", ROOT / "packages/obs-platform/src"):
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))

import fakeredis.aioredis
from httpx import ASGITransport, AsyncClient

from obs_platform import db as db_module
from obs_platform import redis_io
from obs_platform.db import Database
from obs_platform.models import Base
from obs_platform.security.tokens import issue_token
from obs_platform.security.users import create_user
from obs_platform.settings import Settings, get_settings, reset_settings_cache

ADMIN_EMAIL = "admin@obs.test"
VIEWER_EMAIL = "viewer@obs.test"
TEST_PASSWORD = "correct-horse-battery-staple"


@pytest.fixture(scope="session", autouse=True)
def _configure_logging_once() -> None:
    from obs_platform.logging import configure_logging

    configure_logging(level="WARNING", json_output=False, service="obs-test")


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    """Fresh settings pointed at a per-test SQLite file."""
    os.environ["OBS_DATABASE_URL"] = "sqlite+aiosqlite:///{}".format(
        (tmp_path / "obs_test.sqlite3").as_posix()
    )
    reset_settings_cache()
    return get_settings()


@pytest.fixture
async def database(settings: Settings) -> AsyncIterator[Database]:
    """A created database, installed as the process-wide one."""
    database = Database(settings)
    async with database.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    db_module.set_db(database)
    try:
        yield database
    finally:
        db_module.set_db(None)
        await database.dispose()


@pytest.fixture
async def session(database: Database) -> AsyncIterator[Any]:
    async with database.session() as s:
        yield s


@pytest.fixture
async def redis(settings: Settings) -> AsyncIterator[Any]:
    """fakeredis, installed as the process-wide client. Supports streams."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    redis_io.set_redis(client)
    try:
        yield client
    finally:
        redis_io.set_redis(None)
        await client.aclose()


@pytest.fixture
async def app(settings: Settings, database: Database, redis: Any) -> Any:
    from obs_platform.api.app import create_app

    return create_app(settings)


@pytest.fixture
async def users(database: Database) -> dict[str, Any]:
    """One admin and one tenant-scoped viewer."""
    async with database.session() as session:
        admin = await create_user(session, ADMIN_EMAIL, TEST_PASSWORD, role="admin")
        viewer = await create_user(
            session, VIEWER_EMAIL, TEST_PASSWORD, role="viewer", tenant_id="org_1"
        )
        return {
            "admin": {"id": admin.id, "email": admin.email, "role": "admin", "tenant_id": None},
            "viewer": {
                "id": viewer.id,
                "email": viewer.email,
                "role": "viewer",
                "tenant_id": "org_1",
            },
        }


def _token(settings: Settings, user: dict[str, Any]) -> str:
    token, _ = issue_token(
        settings,
        subject=user["id"],
        email=user["email"],
        role=user["role"],
        tenant_id=user["tenant_id"],
    )
    return token


@pytest.fixture
async def rate_limited_app(settings: Settings, database: Database, redis: Any) -> Any:
    """An app with rate limiting ON.

    The default `app` fixture has it off, because 250 tests hammering the same
    endpoints would throttle each other. Rate-limit tests must therefore build
    their own app -- mutating settings after construction does nothing, since
    the middleware snapshots its limits at __init__.
    """
    from obs_platform.api.app import create_app

    settings.rate_limit_enabled = True
    return create_app(settings)


@pytest.fixture
async def anon_client(app: Any) -> AsyncIterator[AsyncClient]:
    """No credentials. Used to prove endpoints actually require them."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client


@pytest.fixture
async def client(app: Any, settings: Settings, users: dict[str, Any]) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"Authorization": f"Bearer {_token(settings, users['admin'])}"},
    ) as client:
        yield client


@pytest.fixture
async def viewer_client(
    app: Any, settings: Settings, users: dict[str, Any]
) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"Authorization": f"Bearer {_token(settings, users['viewer'])}"},
    ) as client:
        yield client


@pytest.fixture
def publisher() -> Any:
    from obs_sdk import InMemoryPublisher

    return InMemoryPublisher()


@pytest.fixture(autouse=True)
def _reset_caches_after_test() -> AsyncIterator[None]:
    yield
    reset_settings_cache()
