"""Phase 0 -- foundations.

Covers the contracts everything later depends on: the shared event schema and
its version tolerance, connection-string normalisation for Neon, the production
config guards, and /health.
"""

from __future__ import annotations

import json
from datetime import UTC

import pytest

from obs_platform.settings import (
    DEV_JWT_SECRET,
    Settings,
    is_pooled_host,
    normalize_async_dsn,
    normalize_sync_dsn,
)
from obs_sdk.schema import (
    SCHEMA_VERSION,
    ErrorInfo,
    EventType,
    ObsEvent,
    SchemaTooOldError,
    Severity,
    SpanKind,
    Usage,
    clip,
    new_trace_id,
    parse_event,
)

NEON_POOLED = (
    "postgresql://neondb_owner:pw@ep-cool-name-123456-pooler.us-east-2.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)
NEON_DIRECT = (
    "postgresql://neondb_owner:pw@ep-cool-name-123456.us-east-2.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)


# --------------------------------------------------------------------------- #
# Event schema
# --------------------------------------------------------------------------- #
class TestEventSchema:
    def test_every_event_carries_a_schema_version(self) -> None:
        event = ObsEvent(trace_id=new_trace_id(), type=EventType.SPAN_END)
        assert event.schema_version == SCHEMA_VERSION
        assert "schema_version" in json.loads(event.to_json())

    def test_json_roundtrip_is_lossless(self) -> None:
        original = ObsEvent(
            trace_id=new_trace_id(),
            span_id="sp_1",
            parent_span_id="sp_0",
            type=EventType.SPAN_END,
            kind=SpanKind.LLM,
            name="classify",
            usage=Usage(prompt_tokens=10, completion_tokens=4),
            attributes={"ticket_id": "T-1"},
        )
        restored = parse_event(json.loads(original.to_json()))
        assert restored.trace_id == original.trace_id
        assert restored.parent_span_id == "sp_0"
        assert restored.usage is not None and restored.usage.total_tokens == 14
        assert restored.attributes["ticket_id"] == "T-1"

    def test_v1_payload_is_upgraded_not_rejected(self) -> None:
        """Replayed history is still v1. It has to keep parsing forever."""
        event = parse_event(
            {
                "schema_version": 1,
                "trace_id": "tr_old",
                "event_type": "span.end",
                "node": "classify_ticket_step",
                "prompt_tokens": 30,
                "completion_tokens": 12,
            }
        )
        assert event.schema_version == 2
        assert event.name == "classify_ticket_step"
        assert event.usage is not None and event.usage.total_tokens == 42

    def test_future_payload_survives_an_older_consumer(self) -> None:
        """The rolling-deploy case: a new producer, this old consumer."""
        event = parse_event(
            {
                "schema_version": SCHEMA_VERSION + 5,
                "trace_id": "tr_new",
                "type": "span.end",
                "name": "x",
                "field_from_the_future": {"nested": True},
            }
        )
        assert event.trace_id == "tr_new"
        assert event.field_from_the_future == {"nested": True}  # type: ignore[attr-defined]

    def test_prehistoric_payload_is_rejected_for_dead_lettering(self) -> None:
        with pytest.raises(SchemaTooOldError):
            parse_event({"schema_version": 0, "trace_id": "t", "type": "span.end"})

    def test_non_numeric_version_is_a_schema_error(self) -> None:
        from obs_sdk.schema import SchemaError

        with pytest.raises(SchemaError):
            parse_event({"schema_version": "banana", "trace_id": "t", "type": "span.end"})

    @pytest.mark.parametrize(
        ("raw", "expected_total"),
        [
            ({"prompt_tokens": 5, "completion_tokens": 5}, 10),
            ({"prompt_tokens": None, "completion_tokens": 3}, 3),
            ({"prompt_tokens": "7", "completion_tokens": 1}, 8),
            ({"prompt_tokens": -4, "completion_tokens": 2}, 2),
        ],
    )
    def test_usage_coerces_provider_junk(self, raw: dict, expected_total: int) -> None:
        assert Usage(**raw).total_tokens == expected_total

    def test_blank_tenant_falls_back_rather_than_pooling_tenants(self) -> None:
        assert (
            ObsEvent(trace_id="t", type=EventType.SPAN_END, tenant_id="   ").tenant_id == "default"
        )

    def test_naive_datetimes_are_made_utc_aware(self) -> None:
        from datetime import datetime

        event = ObsEvent(
            trace_id="t", type=EventType.SPAN_END, started_at=datetime(2026, 1, 1, 12, 0, 0)
        )
        assert event.started_at is not None and event.started_at.tzinfo is not None

    def test_latency_is_computed_when_absent(self) -> None:
        from datetime import datetime, timedelta

        start = datetime(2026, 1, 1, tzinfo=UTC)
        event = ObsEvent(
            trace_id="t",
            type=EventType.SPAN_END,
            started_at=start,
            ended_at=start + timedelta(milliseconds=250),
        )
        assert event.computed_latency_ms() == 250

    def test_clip_marks_truncation(self) -> None:
        clipped = clip("x" * 100, 10)
        assert clipped.startswith("x" * 10)
        assert "truncated" in clipped

    def test_error_info_truncates_stack(self) -> None:
        info = ErrorInfo(type="ValueError", message="m", stack="y" * 10_000)
        assert info.stack is not None and len(info.stack) < 3_000

    def test_severity_ladder(self) -> None:
        assert Severity.at_least("critical", "high")
        assert not Severity.at_least("low", "high")
        assert Severity.max_of(["low", "critical", "medium"]) is Severity.CRITICAL
        assert Severity.max_of([]) is Severity.INFO


# --------------------------------------------------------------------------- #
# Connection strings -- gotcha #1
# --------------------------------------------------------------------------- #
class TestConnectionStrings:
    def test_neon_pooled_url_is_made_asyncpg_safe(self) -> None:
        """asyncpg raises on sslmode/channel_binding. They must not survive."""
        url, connect_args = normalize_async_dsn(NEON_POOLED)
        assert url.startswith("postgresql+asyncpg://")
        assert "sslmode" not in url
        assert "channel_binding" not in url
        assert connect_args["ssl"] is True

    def test_pooled_host_disables_the_prepared_statement_cache(self) -> None:
        """PgBouncer transaction mode does not keep a session between statements."""
        _, pooled_args = normalize_async_dsn(NEON_POOLED)
        _, direct_args = normalize_async_dsn(NEON_DIRECT)
        assert pooled_args["statement_cache_size"] == 0
        assert "statement_cache_size" not in direct_args

    def test_pooled_host_detection(self) -> None:
        assert is_pooled_host("ep-x-123-pooler.aws.neon.tech")
        assert is_pooled_host("my-pgbouncer.internal")
        assert not is_pooled_host("ep-x-123.aws.neon.tech")

    def test_migrations_use_the_sync_driver_and_keep_libpq_params(self) -> None:
        url = normalize_sync_dsn(NEON_DIRECT)
        assert url.startswith("postgresql+psycopg://")
        assert "sslmode=require" in url

    def test_migration_url_prefers_the_direct_string(self) -> None:
        s = Settings(database_url=NEON_POOLED, database_direct_url=NEON_DIRECT)
        assert "-pooler" not in s.migration_database_url
        assert s.migration_url_is_pooled is False

    def test_migration_url_falls_back_and_flags_the_risk(self) -> None:
        """Falling back is survivable; doing it silently is not."""
        s = Settings(database_url=NEON_POOLED, database_direct_url=None)
        assert s.migration_url_is_pooled is True

    def test_sqlite_urls_get_the_async_driver(self) -> None:
        url, args = normalize_async_dsn("sqlite:///./x.sqlite3")
        assert url == "sqlite+aiosqlite:///./x.sqlite3"
        assert args == {}


# --------------------------------------------------------------------------- #
# Settings guards
# --------------------------------------------------------------------------- #
class TestSettingsGuards:
    def test_production_refuses_the_dev_signing_key(self) -> None:
        with pytest.raises(ValueError, match="OBS_JWT_SECRET"):
            Settings(
                environment="production",
                jwt_secret=DEV_JWT_SECRET,
                database_url=NEON_POOLED,
            )

    def test_production_refuses_a_short_signing_key(self) -> None:
        with pytest.raises(ValueError, match="OBS_JWT_SECRET"):
            Settings(environment="production", jwt_secret="short", database_url=NEON_POOLED)

    def test_production_refuses_sqlite(self) -> None:
        with pytest.raises(ValueError, match="Postgres"):
            Settings(
                environment="production",
                jwt_secret="a" * 40,
                database_url="sqlite+aiosqlite:///./x.sqlite3",
            )

    def test_local_environment_stays_convenient(self) -> None:
        """The guards are production-only; local must not need ceremony."""
        s = Settings(environment="local", jwt_secret=DEV_JWT_SECRET)
        assert s.jwt_secret == DEV_JWT_SECRET
        assert s.is_sqlite

    @pytest.mark.parametrize("bad", [-1, 101])
    def test_sample_rate_must_be_a_percentage(self, bad: int) -> None:
        with pytest.raises(ValueError):
            Settings(eval_sample_rate=bad)

    def test_injection_threshold_bounds(self) -> None:
        with pytest.raises(ValueError):
            Settings(injection_threshold=1.5)

    def test_list_parsing(self) -> None:
        s = Settings(cors_origins="http://a.com, http://b.com ,", worker_roles="storage, eval")
        assert s.cors_origin_list == ["http://a.com", "http://b.com"]
        assert s.worker_role_list == ["storage", "eval"]

    def test_consumer_name_is_host_and_pid(self) -> None:
        assert ":" in Settings().consumer_name()


# --------------------------------------------------------------------------- #
# Health -- Phase 0 definition of done
# --------------------------------------------------------------------------- #
class TestHealth:
    async def test_health_returns_200(self, client) -> None:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    async def test_health_does_not_touch_dependencies(self, client) -> None:
        """Liveness must stay green during a Redis outage, or the LB kills a healthy box."""
        from obs_platform import redis_io

        broken = redis_io.get_redis()
        redis_io.set_redis(None)
        try:
            assert (await client.get("/health")).status_code == 200
        finally:
            redis_io.set_redis(broken)

    async def test_ready_checks_database_and_redis(self, client) -> None:
        response = await client.get("/health/ready")
        assert response.status_code == 200
        checks = response.json()["checks"]
        assert checks["database"]["status"] == "ok"
        assert checks["redis"]["status"] == "ok"

    async def test_request_id_is_echoed(self, client) -> None:
        response = await client.get("/health", headers={"X-Request-ID": "abc123"})
        assert response.headers["X-Request-ID"] == "abc123"

    async def test_security_headers_present(self, client) -> None:
        response = await client.get("/health")
        assert response.headers["X-Content-Type-Options"] == "nosniff"

    async def test_root_endpoint(self, client) -> None:
        assert (await client.get("/")).status_code == 200
