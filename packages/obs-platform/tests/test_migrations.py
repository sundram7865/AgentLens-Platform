"""Migrations must describe exactly the schema the models describe.

The drift this catches is the classic one: someone adds a column to ``models.py``,
the test suite (which uses ``create_all``) passes happily, and production -- which
only ever runs Alembic -- is missing the column. This test runs the real
migration chain and then asks Alembic's autogenerate comparator whether anything
is left over. Anything at all is a failure.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, inspect

from obs_platform.models import Base

PACKAGE_ROOT = Path(__file__).resolve().parents[1]


def _alembic_config(url: str) -> Config:
    config = Config(str(PACKAGE_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(PACKAGE_ROOT / "migrations"))
    config.set_main_option("sqlalchemy.url", url)
    return config


@pytest.fixture
def migrated_url(tmp_path: Path) -> str:
    url = "sqlite:///{}".format((tmp_path / "migrated.sqlite3").as_posix())
    command.upgrade(_alembic_config(url), "head")
    return url


def test_upgrade_head_creates_every_table(migrated_url: str) -> None:
    engine = create_engine(migrated_url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    expected = set(Base.metadata.tables) | {"alembic_version"}
    assert expected - tables == set(), f"migrations are missing tables: {expected - tables}"


def test_migrations_have_no_drift_from_models(migrated_url: str) -> None:
    """The whole point of the file: models and migrations agree, or CI fails."""
    engine = create_engine(migrated_url)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(
                connection,
                opts={"compare_type": True, "compare_server_default": False},
            )
            diff = compare_metadata(context, Base.metadata)
    finally:
        engine.dispose()

    # Ignore index-ordering noise that SQLite reports but Postgres does not.
    meaningful = [d for d in diff if not _is_ignorable(d)]
    assert meaningful == [], "models and migrations have drifted:\n{}".format(
        "\n".join(repr(d) for d in meaningful)
    )


def test_full_downgrade_then_upgrade_round_trips(tmp_path: Path) -> None:
    """A migration you cannot reverse is a migration you cannot safely deploy."""
    url = "sqlite:///{}".format((tmp_path / "roundtrip.sqlite3").as_posix())
    config = _alembic_config(url)
    command.upgrade(config, "head")
    command.downgrade(config, "base")

    engine = create_engine(url)
    try:
        remaining = set(inspect(engine).get_table_names()) - {"alembic_version"}
    finally:
        engine.dispose()
    assert remaining == set(), f"downgrade left tables behind: {remaining}"

    command.upgrade(config, "head")


def test_migrations_apply_one_at_a_time(tmp_path: Path) -> None:
    """Each revision must stand on its own; a broken middle step blocks a rollback."""
    url = "sqlite:///{}".format((tmp_path / "stepwise.sqlite3").as_posix())
    config = _alembic_config(url)
    for revision in (
        "0001_ingestion_core",
        "0002_guardrails_and_evals",
        "0003_drift",
        "0004_auth_and_audit",
    ):
        command.upgrade(config, revision)

    engine = create_engine(url)
    try:
        tables = set(inspect(engine).get_table_names())
    finally:
        engine.dispose()
    assert {"traces", "spans", "eval_scores", "drift_snapshots", "users"} <= tables


def test_added_columns_are_backfill_safe() -> None:
    """Gotcha #11: a NOT NULL column with no default breaks a half-finished rollout.

    Every migration, not just the one that existed when this test was written.
    Naming a single file here is how a guard quietly stops guarding: the next
    migration adds a NOT NULL column, nothing complains, and the failure only
    shows up as a consumer crashing mid-rollout against a half-migrated
    database. Whatever is in ``versions/`` is what gets checked.
    """
    migrations = sorted((PACKAGE_ROOT / "migrations/versions").glob("[0-9]*.py"))
    assert migrations, "expected at least one migration to check"

    checked = 0
    for path in migrations:
        source = path.read_text(encoding="utf-8")
        for block in source.split("op.add_column(")[1:]:
            if 'sa.Column("' not in block:
                continue
            head = block.split(")\n")[0]
            checked += 1
            if "nullable=False" in head:
                assert "server_default" in head, (
                    f"{path.name}: NOT NULL column added without a server default:\n{head}"
                )
    assert checked, "expected additive columns somewhere in the migration history"


def _is_ignorable(diff: object) -> bool:
    if isinstance(diff, tuple) and diff:
        kind = diff[0]
        # SQLite reflects unique constraints as unique indexes, so the comparator
        # reports a phantom pair for every UniqueConstraint we declared.
        if kind in {"add_index", "remove_index"}:
            index = diff[-1]
            return bool(getattr(index, "unique", False))
    return False
