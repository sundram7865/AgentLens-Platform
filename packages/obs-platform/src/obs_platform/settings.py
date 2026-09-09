"""Configuration.

Everything is read from the environment through ``pydantic-settings``. Nothing
is hardcoded, and ``.env.example`` is the committed contract -- ``.env`` never is.

Two things in here are not boilerplate and are worth reading:

``normalize_async_dsn`` / ``normalize_sync_dsn``
    Neon hands you a libpq URL with ``sslmode=require&channel_binding=require``.
    asyncpg does not understand either parameter and raises on connect, so the
    async driver needs them stripped and TLS re-expressed as a connect arg,
    while Alembic's sync driver wants them left alone. Getting this wrong is the
    "works locally, fails on deploy" bug that eats an afternoon.

``pooled vs direct``
    ``database_url`` (the ``-pooler`` host) is for the API and workers.
    ``database_direct_url`` is reserved for Alembic. PgBouncer in transaction
    mode does not keep a session across statements, which breaks both prepared
    statements and the session-level locking migration tooling relies on. When
    a pooled host is detected the async engine also disables asyncpg's prepared
    statement cache, because cached plans do not survive a pooler handing you a
    different backend connection between statements.
"""

from __future__ import annotations

import functools
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    InitSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

Environment = Literal["local", "test", "dev", "staging", "production"]

DEV_JWT_SECRET = "dev-only-insecure-secret-change-me"

# libpq parameters asyncpg rejects outright. Stripped from the async DSN and
# re-expressed through connect_args.
_LIBPQ_ONLY_PARAMS = {
    "sslmode",
    "channel_binding",
    "target_session_attrs",
    "options",
    "connect_timeout",
    "application_name",
    "gssencmode",
}


def _split_scheme(url: str) -> tuple[str, str]:
    scheme, _, rest = url.partition("://")
    return scheme, rest


def normalize_async_dsn(url: str) -> tuple[str, dict[str, Any]]:
    """Return ``(sqlalchemy_url, connect_args)`` for the async engine.

    Handles the three shapes that actually show up: a Neon libpq URL, a plain
    local Postgres URL, and a SQLite URL used by the test suite.
    """
    if url.startswith("sqlite"):
        if "+aiosqlite" not in url:
            url = url.replace("sqlite:", "sqlite+aiosqlite:", 1)
        return url, {}

    scheme, rest = _split_scheme(url)
    base = scheme.split("+", 1)[0]
    if base not in {"postgres", "postgresql"}:
        return url, {}

    parts = urlsplit("postgresql://" + rest)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    sslmode = query.get("sslmode")
    stripped = {k: v for k, v in query.items() if k not in _LIBPQ_ONLY_PARAMS}

    connect_args: dict[str, Any] = {}
    if sslmode and sslmode not in {"disable", "allow"}:
        # asyncpg's own spelling. `True` means "verify against the system store",
        # which is what Neon's public certificate needs.
        connect_args["ssl"] = True

    host = (parts.hostname or "").lower()
    if is_pooled_host(host):
        # PgBouncer transaction mode: a prepared statement created on one backend
        # connection is not there on the next statement. Disabling the cache is
        # the documented fix, and asyncpg still works, just without plan reuse.
        connect_args["statement_cache_size"] = 0

    rebuilt = urlunsplit(
        ("postgresql+asyncpg", parts.netloc, parts.path, urlencode(stripped), parts.fragment)
    )
    return rebuilt, connect_args


def normalize_sync_dsn(url: str) -> str:
    """Return a psycopg (sync) URL. Used by Alembic only; libpq params are kept."""
    if url.startswith("sqlite"):
        return url.replace("+aiosqlite", "")
    scheme, rest = _split_scheme(url)
    base = scheme.split("+", 1)[0]
    if base not in {"postgres", "postgresql"}:
        return url
    return "postgresql+psycopg://" + rest


def is_pooled_host(host: str) -> bool:
    """True for a connection-pooler endpoint (Neon's ``-pooler``, PgBouncer, RDS proxy)."""
    host = host.lower()
    return "-pooler" in host or "pgbouncer" in host or "proxy" in host


class _AliasAwareInitSource(InitSettingsSource):
    """Make explicit ``Settings(field=...)`` arguments actually win.

    Fields like ``database_url`` carry a ``validation_alias`` so a host can set
    ``DATABASE_URL`` without the ``OBS_`` prefix. The side effect is that the env
    source emits the value under the *alias* key while the init source emits it
    under the *field name*, so both end up in the merged data -- and pydantic
    resolves by alias, which means the environment silently beat an explicit
    argument.

    That is backwards, and it is a landmine: anyone constructing Settings
    programmatically (a test, an embedding application) gets whatever happens to
    be in the environment instead of what they passed. Re-keying init values
    onto the primary alias puts them in the same slot the env source uses, where
    being the higher-priority source actually decides the outcome.
    """

    def __call__(self) -> dict[str, Any]:
        data = dict(super().__call__())
        for name, field in self.settings_cls.model_fields.items():
            if name not in data:
                continue
            alias = getattr(field, "validation_alias", None)
            choices = getattr(alias, "choices", None)
            if choices:
                data[str(choices[0])] = data[name]
        return data


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="OBS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        # Fields with a validation_alias (DATABASE_URL, REDIS_URL, PORT) are
        # otherwise only settable by alias, which silently ignores the field name
        # in tests and in any code that constructs Settings directly.
        populate_by_name=True,
    )

    # -- service -------------------------------------------------------------
    environment: Environment = "local"
    service_name: str = "obs-platform"
    version: str = "1.0.0"
    log_level: str = "INFO"
    log_json: bool = True
    port: int = Field(default=8000, validation_alias=AliasChoices("OBS_PORT", "PORT"))

    # -- database ------------------------------------------------------------
    database_url: str = Field(
        default="sqlite+aiosqlite:///./obs_local.sqlite3",
        validation_alias=AliasChoices("OBS_DATABASE_URL", "DATABASE_URL"),
        description="Pooled connection string. Used by the API and every worker.",
    )
    database_direct_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "OBS_DATABASE_DIRECT_URL", "DATABASE_DIRECT_URL", "DIRECT_DATABASE_URL"
        ),
        description="Un-pooled connection string. Alembic only. Never used at runtime.",
    )
    db_pool_size: int = 5
    db_max_overflow: int = 5
    db_pool_recycle_seconds: int = 1_800
    db_pool_timeout_seconds: int = 30
    db_echo: bool = False

    # -- redis / streams -----------------------------------------------------
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        validation_alias=AliasChoices("OBS_REDIS_URL", "REDIS_URL"),
    )
    stream: str = "obs:events"
    stream_maxlen: int = 100_000
    consumer_block_ms: int = 5_000
    consumer_batch_size: int = 100
    consumer_claim_min_idle_ms: int = 60_000
    consumer_claim_interval_seconds: int = 30
    max_delivery_attempts: int = 5
    """After this many redeliveries a message goes to dead_letters instead of looping forever."""

    # -- API -----------------------------------------------------------------
    cors_origins: str = "http://localhost:3000,http://127.0.0.1:3000"
    api_root_path: str = ""
    docs_enabled: bool = True
    max_page_size: int = 200
    default_page_size: int = 25

    # -- auth ----------------------------------------------------------------
    jwt_secret: str = DEV_JWT_SECRET
    jwt_algorithm: str = "HS256"
    access_token_ttl_minutes: int = 720
    bootstrap_admin_email: str | None = None
    bootstrap_admin_password: str | None = None
    # Optional external OIDC (Keycloak, Auth0, Clerk). When set, bearer tokens
    # signed by this issuer are accepted alongside locally issued ones.
    oidc_issuer: str | None = None
    oidc_jwks_url: str | None = None
    oidc_audience: str | None = None
    oidc_role_claim: str = "obs_role"

    # -- rate limiting -------------------------------------------------------
    rate_limit_enabled: bool = True
    rate_limit_requests: int = 120
    rate_limit_window_seconds: int = 60
    rate_limit_login_requests: int = 10
    rate_limit_login_window_seconds: int = 300

    # -- guardrails ----------------------------------------------------------
    guardrails_enabled: bool = True
    pii_engine: Literal["auto", "presidio", "builtin"] = "auto"
    pii_language: str = "en"
    injection_backend: Literal["auto", "embeddings", "lexical"] = "auto"
    injection_threshold: float = 0.61
    """Measured, not guessed: the midpoint of the plateau where precision and
    recall are both 1.00 on held-out attacks. See scripts/tune_injection_threshold.py
    and docs/TUNING.md for the sweep and the caveats."""
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    alert_severity_floor: Literal["info", "low", "medium", "high", "critical"] = "high"

    # -- evals ---------------------------------------------------------------
    evals_enabled: bool = True
    eval_sample_rate: int = 5
    """Percent of traces scored. Deterministic on hash(trace_id), so retries agree."""
    eval_backend: Literal["auto", "ragas", "judge", "heuristic"] = "auto"
    eval_provider: Literal["anthropic", "openai", "google", "none"] = "none"
    eval_model: str = "claude-haiku-4-5"
    eval_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "OBS_EVAL_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "GOOGLE_API_KEY"
        ),
    )
    eval_batch_size: int = 10
    eval_batch_interval_seconds: int = 30
    eval_ready_timeout_seconds: int = 45
    """How long the judge waits for the storage writer to persist a trace.

    The two consumers read the same stream in different groups with no ordering
    between them, so ``trace.end`` can reach the judge first. Rather than call
    such a trace ungradable -- which is permanent, and wrong -- it is retried for
    this long. Keep it below ``consumer_claim_min_idle_ms`` so a trace reaches a
    final answer before its own message becomes eligible for reclaim.
    """
    eval_timeout_seconds: float = 30.0
    eval_max_context_chars: int = 6_000

    # -- budget guard --------------------------------------------------------
    budget_daily_usd_per_tenant: float = 1.0
    budget_monthly_usd_per_tenant: float = 10.0
    budget_daily_tokens_per_tenant: int = 2_000_000

    # -- drift ---------------------------------------------------------------
    drift_enabled: bool = True
    drift_interval_seconds: int = 300
    drift_window_hours: int = 6
    drift_min_samples: int = 20
    drift_z_threshold: float = 2.5
    drift_absolute_floor: float = 0.60

    # -- retention / jobs ----------------------------------------------------
    retention_days: int = 30
    retention_interval_seconds: int = 3_600
    retention_batch_size: int = 5_000
    job_catchup_enabled: bool = True
    """Run an overdue job at startup. Render free tier sleeps; timers do not fire while asleep."""

    # -- workers -------------------------------------------------------------
    worker_roles: str = "storage,guardrail,eval,scheduler"
    embed_workers_in_api: bool = False
    """Render's free tier has no background-worker type. Set true to run consumers
    inside the API process; keep false when workers get their own service."""
    worker_heartbeat_seconds: int = 15
    meta_health_max_lag: int = 1_000
    meta_health_max_write_age_seconds: int = 900

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Priority: explicit arguments, then env, then .env, then secrets."""
        return (
            _AliasAwareInitSource(settings_cls, getattr(init_settings, "init_kwargs", {})),
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )

    # -- validators ----------------------------------------------------------
    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("eval_sample_rate")
    @classmethod
    def _sample_range(cls, v: int) -> int:
        if not 0 <= v <= 100:
            raise ValueError("eval_sample_rate must be a percentage between 0 and 100")
        return v

    @field_validator("injection_threshold")
    @classmethod
    def _threshold_range(cls, v: float) -> float:
        if not 0.0 < v < 1.0:
            raise ValueError("injection_threshold must be between 0 and 1 exclusive")
        return v

    @model_validator(mode="after")
    def _production_guards(self) -> Settings:
        if self.environment == "production":
            # Refusing to boot beats booting with a signing key that is on GitHub.
            if self.jwt_secret == DEV_JWT_SECRET or len(self.jwt_secret) < 32:
                raise ValueError(
                    "OBS_JWT_SECRET must be set to a unique value of at least 32 characters "
                    'in production (generate: python -c "import secrets;print(secrets.token_urlsafe(48))")'
                )
            if self.database_url.startswith("sqlite"):
                raise ValueError("OBS_DATABASE_URL must point at Postgres in production")
        return self

    # -- derived -------------------------------------------------------------
    @property
    def async_database_url(self) -> str:
        return normalize_async_dsn(self.database_url)[0]

    @property
    def async_connect_args(self) -> dict[str, Any]:
        return normalize_async_dsn(self.database_url)[1]

    @property
    def migration_database_url(self) -> str:
        """Direct URL when configured, else the pooled one with a loud caveat.

        Alembic against PgBouncer in transaction mode can hang on the migration
        advisory lock. We fall back rather than crash, and warn at run time.
        """
        return normalize_sync_dsn(self.database_direct_url or self.database_url)

    @property
    def migration_url_is_pooled(self) -> bool:
        url = self.database_direct_url or self.database_url
        try:
            host = urlsplit(url.replace("+asyncpg", "").replace("+psycopg", "")).hostname or ""
        except ValueError:
            return False
        return is_pooled_host(host)

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")

    @property
    def cors_origin_list(self) -> list[str]:
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    @property
    def worker_role_list(self) -> list[str]:
        return [r.strip().lower() for r in self.worker_roles.split(",") if r.strip()]

    @property
    def testing(self) -> bool:
        return self.environment == "test"

    def consumer_name(self) -> str:
        """``hostname:pid`` -- unique per process so several workers can share a group."""
        import os
        import socket

        return f"{socket.gethostname()}:{os.getpid()}"


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """Tests mutate the environment between cases; the cache has to follow."""
    get_settings.cache_clear()


__all__ = [
    "DEV_JWT_SECRET",
    "Settings",
    "get_settings",
    "is_pooled_host",
    "normalize_async_dsn",
    "normalize_sync_dsn",
    "reset_settings_cache",
]
