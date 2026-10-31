"""The database the real-infrastructure suites run against.

Integration and end-to-end tests TRUNCATE every table, and the e2e suite drops
the whole schema to prove a cold migration works. Pointed at the database
``docker compose`` serves for development, that wipes whatever you were
looking at in the dashboard. So the default is a separate ``obs_test`` database
on the same server, created on first use.
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit, urlunsplit

DEFAULT_TEST_URL = "postgresql+asyncpg://obs:obs@localhost:5432/obs_test"

TEST_DATABASE_URL = os.environ.get("OBS_TEST_DATABASE_URL", DEFAULT_TEST_URL)


def ensure_database(url: str = TEST_DATABASE_URL) -> None:
    """Create the database named in ``url`` if the server does not have it yet.

    Raises whatever psycopg raises when the server itself is unreachable, so the
    caller can turn that into a skip.
    """
    import psycopg
    from psycopg import sql

    parts = urlsplit(url.replace("+asyncpg", "").replace("+psycopg", ""))
    name = parts.path.lstrip("/")
    if not name:
        return
    admin = urlunsplit(parts._replace(path="/postgres"))
    with psycopg.connect(admin, autocommit=True, connect_timeout=5) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
