"""Alembic environment.

Two decisions worth spelling out:

1. **The URL comes from Settings, never from alembic.ini.** A connection string
   in a committed ini file is a credential in git.
2. **Migrations use the DIRECT connection string** (``OBS_DATABASE_DIRECT_URL``),
   not the pooled one. Alembic takes a session-level advisory lock and runs DDL
   in a transaction; PgBouncer in transaction mode can hand the next statement a
   different backend connection, at which point the lock is held by a session
   nobody is talking to any more and the migration hangs. If only a pooled URL
   is configured we proceed but warn loudly, because failing the deploy of a
   portfolio project over this would be worse than the risk.
"""

from __future__ import annotations

import sys
import warnings
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
SDK_SRC = Path(__file__).resolve().parents[3] / "packages/obs-sdk/src"
if SDK_SRC.exists() and str(SDK_SRC) not in sys.path:
    sys.path.insert(0, str(SDK_SRC))

from obs_platform.models import Base  # noqa: E402
from obs_platform.settings import get_settings  # noqa: E402

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata

settings = get_settings()

# A URL set programmatically on the Config wins. That is how the migration test
# points Alembic at a throwaway SQLite file without touching the environment.
DATABASE_URL = config.get_main_option("sqlalchemy.url") or settings.migration_database_url

if not config.get_main_option("sqlalchemy.url") and settings.migration_url_is_pooled:
    warnings.warn(
        "Running migrations through a POOLED connection string. Set "
        "OBS_DATABASE_DIRECT_URL to the non '-pooler' Neon host: PgBouncer's "
        "transaction mode can strand Alembic's advisory lock and hang the migration.",
        RuntimeWarning,
        stacklevel=1,
    )

config.set_main_option("sqlalchemy.url", DATABASE_URL.replace("%", "%%"))

IS_SQLITE = DATABASE_URL.startswith("sqlite")


def run_migrations_offline() -> None:
    context.configure(
        url=DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        render_as_batch=IS_SQLITE,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            # SQLite cannot ALTER most things; batch mode rebuilds the table.
            render_as_batch=IS_SQLITE,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
