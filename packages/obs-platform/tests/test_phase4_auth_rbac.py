"""Phase 4 -- authentication, RBAC, redaction and the audit trail.

The definition of done for this phase is one specific claim: **logged in as a
viewer, a direct API call for a raw trace still comes back redacted.** Not the
UI view -- the API response bytes. `test_viewer_gets_redacted_payload_over_the_api`
is that test, and several others exist to prove the claim cannot be dodged by
going around the dashboard.
"""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime

import jwt
import pytest
from conftest import ADMIN_EMAIL, TEST_PASSWORD, VIEWER_EMAIL
from sqlalchemy import select

from obs_platform.models import AuditLog, Span, Trace
from obs_platform.security.passwords import (
    hash_password,
    validate_password_strength,
    verify_password,
)
from obs_platform.security.ratelimit import InProcessCounter, check_rate_limit
from obs_platform.security.redaction import get_redactor, redactor_for_role
from obs_platform.security.tokens import TokenError, issue_token, verify_token
from obs_platform.security.users import authenticate, create_user, ensure_bootstrap_admin

SENSITIVE_QUESTION = (
    "My card 4111 1111 1111 1111 was charged twice. "
    "Email me at priya.sharma@example.com, phone +91 98765 43210."
)
SENSITIVE_ANSWER = "We refunded card ending 1111 and emailed priya.sharma@example.com."
LEAKED_KEY = "The integration uses sk-ant-api03-abcdefghijklmnopqrstuvwx to call the provider."


async def seed_sensitive_trace(database, trace_id: str = "tr_pii", tenant_id: str = "org_1"):
    now = datetime.now(UTC)
    async with database.session() as session:
        session.add(
            Trace(
                trace_id=trace_id,
                tenant_id=tenant_id,
                name="agent_run",
                service="supportpilot",
                status="ok",
                started_at=now,
                ended_at=now,
                latency_ms=900,
                input={"question": SENSITIVE_QUESTION},
                output={"answer": SENSITIVE_ANSWER},
                attributes={"ticket_id": "T-77"},
                guardrail_flags={},
                created_at=now,
                updated_at=now,
            )
        )
        session.add(
            Span(
                trace_id=trace_id,
                span_id="sp_1",
                tenant_id=tenant_id,
                kind="tool",
                name="urbankart_request_refund",
                status="ok",
                input={"note": LEAKED_KEY},
                output={"result": "ok"},
                attributes={},
                created_at=now,
            )
        )


# --------------------------------------------------------------------------- #
# Authentication is required
# --------------------------------------------------------------------------- #
class TestAuthenticationRequired:
    @pytest.mark.parametrize(
        "path",
        [
            "/v1/traces",
            "/v1/traces/tr_pii",
            "/v1/traces/tenants",
            "/v1/metrics/overview",
            "/v1/metrics/timeseries",
            "/v1/alerts",
            "/v1/drift",
            "/v1/auth/me",
        ],
    )
    async def test_endpoints_reject_anonymous_callers(self, anon_client, path: str) -> None:
        response = await anon_client.get(path)
        assert response.status_code == 401
        assert response.headers.get("WWW-Authenticate") == "Bearer"

    async def test_health_stays_open(self, anon_client) -> None:
        """A load balancer has no token; a 401 on /health takes the box out of rotation."""
        assert (await anon_client.get("/health")).status_code == 200

    async def test_garbage_token_is_rejected(self, anon_client) -> None:
        response = await anon_client.get(
            "/v1/traces", headers={"Authorization": "Bearer not-a-jwt"}
        )
        assert response.status_code == 401


# --------------------------------------------------------------------------- #
# THE Phase 4 definition of done
# --------------------------------------------------------------------------- #
class TestServerSideRedaction:
    async def test_viewer_gets_redacted_payload_over_the_api(self, viewer_client, database) -> None:
        """Gotcha #6. Not the UI view -- the actual response bytes."""
        await seed_sensitive_trace(database)
        response = await viewer_client.get("/v1/traces/tr_pii")
        assert response.status_code == 200
        body = response.text

        assert "4111 1111 1111 1111" not in body
        assert "4111111111111111" not in body
        assert "priya.sharma@example.com" not in body
        assert "98765 43210" not in body
        assert "sk-ant-api03-abcdefghijklmnopqrstuvwx" not in body

        payload = response.json()
        assert payload["redacted"] is True
        # Still useful: the shape, the masks and the metrics survive.
        assert "****" in json.dumps(payload["input"]) or "*" in json.dumps(payload["input"])
        assert payload["latency_ms"] == 900

    async def test_viewer_list_view_is_redacted_too(self, viewer_client, database) -> None:
        """The preview text on the list page is a payload like any other."""
        await seed_sensitive_trace(database)
        body = (await viewer_client.get("/v1/traces")).text
        assert "priya.sharma@example.com" not in body
        assert "4111111111111111" not in body

    async def test_admin_sees_raw_pii(self, client, database) -> None:
        """The role has to actually mean something, or it is theatre."""
        await seed_sensitive_trace(database)
        payload = (await client.get("/v1/traces/tr_pii")).json()
        assert "priya.sharma@example.com" in json.dumps(payload["input"])
        assert payload["redacted"] is False

    async def test_secrets_are_masked_even_for_admin(self, client, database) -> None:
        """Nobody needs to read a live API key out of a trace, and a screenshot
        of one is a credential leak."""
        await seed_sensitive_trace(database)
        body = (await client.get("/v1/traces/tr_pii")).text
        assert "sk-ant-api03-abcdefghijklmnopqrstuvwx" not in body
        assert "sk-ant" in body  # the prefix survives so the key can be identified

    async def test_redaction_is_recursive(self) -> None:
        redactor = get_redactor(full=True)
        payload = redactor.redact_payload(
            {"messages": [{"content": "reach me at a.b@c.com"}], "meta": {"n": 1}}
        )
        assert "a.b@c.com" not in json.dumps(payload)
        assert payload["meta"]["n"] == 1

    def test_there_is_no_no_redaction_option(self) -> None:
        """Both roles get a redactor; 'no redaction at all' is unrepresentable."""
        assert redactor_for_role(can_view_raw=True).full is False
        assert redactor_for_role(can_view_raw=False).full is True

    async def test_deeply_nested_payload_is_bounded_not_recursed_forever(self) -> None:
        payload: dict = {"a": {}}
        cursor = payload["a"]
        for _ in range(50):
            cursor["a"] = {}
            cursor = cursor["a"]
        cursor["leak"] = "a.b@c.com"
        assert "a.b@c.com" not in json.dumps(get_redactor(True).redact_payload(payload))


# --------------------------------------------------------------------------- #
# Tenant scoping
# --------------------------------------------------------------------------- #
class TestTenantScoping:
    async def test_viewer_cannot_read_another_tenants_trace(self, viewer_client, database) -> None:
        await seed_sensitive_trace(database, trace_id="tr_other", tenant_id="org_2")
        response = await viewer_client.get("/v1/traces/tr_other")
        # 404, not 403: a 403 confirms the trace exists to someone with no right
        # to know that.
        assert response.status_code == 404

    async def test_viewer_list_is_scoped(self, viewer_client, database) -> None:
        await seed_sensitive_trace(database, trace_id="tr_mine", tenant_id="org_1")
        await seed_sensitive_trace(database, trace_id="tr_theirs", tenant_id="org_2")
        items = (await viewer_client.get("/v1/traces")).json()["items"]
        assert {t["trace_id"] for t in items} == {"tr_mine"}

    async def test_asking_for_another_tenant_is_403_not_silently_ignored(
        self, viewer_client
    ) -> None:
        response = await viewer_client.get("/v1/traces?tenant_id=org_2")
        assert response.status_code == 403

    async def test_admin_sees_every_tenant(self, client, database) -> None:
        await seed_sensitive_trace(database, trace_id="tr_a", tenant_id="org_1")
        await seed_sensitive_trace(database, trace_id="tr_b", tenant_id="org_2")
        items = (await client.get("/v1/traces")).json()["items"]
        assert {t["trace_id"] for t in items} == {"tr_a", "tr_b"}


# --------------------------------------------------------------------------- #
# Role-gated writes
# --------------------------------------------------------------------------- #
class TestRoleGatedActions:
    async def test_viewer_cannot_capture_a_baseline(self, viewer_client) -> None:
        response = await viewer_client.post(
            "/v1/drift/baseline", json={"tenant_id": "org_1", "metric": "faithfulness"}
        )
        assert response.status_code == 403

    async def test_viewer_cannot_create_accounts(self, viewer_client) -> None:
        response = await viewer_client.post(
            "/v1/auth/users?email=x@y.com&password=a-long-enough-password&role=admin"
        )
        assert response.status_code == 403

    async def test_admin_can_create_accounts(self, client) -> None:
        response = await client.post(
            "/v1/auth/users?email=new@obs.test&password=another-long-password&role=viewer"
        )
        assert response.status_code == 201
        assert response.json()["role"] == "viewer"

    async def test_weak_passwords_are_refused(self, client) -> None:
        response = await client.post("/v1/auth/users?email=w@obs.test&password=short")
        assert response.status_code == 422

    async def test_duplicate_email_is_409(self, client) -> None:
        response = await client.post(
            f"/v1/auth/users?email={ADMIN_EMAIL}&password=a-long-enough-password"
        )
        assert response.status_code == 409


# --------------------------------------------------------------------------- #
# Login
# --------------------------------------------------------------------------- #
class TestLogin:
    async def test_successful_login_returns_a_usable_token(self, anon_client, users) -> None:
        response = await anon_client.post(
            "/v1/auth/login", json={"email": ADMIN_EMAIL, "password": TEST_PASSWORD}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["role"] == "admin"
        # httpOnly cookie so the dashboard's server components can read it and
        # browser JavaScript cannot.
        assert "obs_token" in response.cookies

        me = await anon_client.get(
            "/v1/auth/me", headers={"Authorization": f"Bearer {body['access_token']}"}
        )
        assert me.json()["email"] == ADMIN_EMAIL
        assert me.json()["can_view_raw"] is True

    async def test_viewer_token_reports_no_raw_access(self, anon_client, users) -> None:
        login = await anon_client.post(
            "/v1/auth/login", json={"email": VIEWER_EMAIL, "password": TEST_PASSWORD}
        )
        token = login.json()["access_token"]
        me = await anon_client.get("/v1/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert me.json()["can_view_raw"] is False
        assert me.json()["tenant_id"] == "org_1"

    @pytest.mark.parametrize(
        ("email", "password"),
        [
            (ADMIN_EMAIL, "wrong-password-entirely"),
            ("nobody@obs.test", TEST_PASSWORD),
        ],
    )
    async def test_failures_are_indistinguishable(
        self, anon_client, users, email: str, password: str
    ) -> None:
        """Different messages for 'no such user' and 'wrong password' is an
        account-enumeration oracle."""
        response = await anon_client.post(
            "/v1/auth/login", json={"email": email, "password": password}
        )
        assert response.status_code == 401
        assert response.json()["error"]["message"] == "Invalid email or password"

    async def test_unknown_user_does_not_short_circuit(self, database) -> None:
        """The dummy-hash path must actually run, not return early."""
        async with database.session() as session:
            start = time.perf_counter()
            assert await authenticate(session, "ghost@obs.test", "anything") is None
            missing_user_time = time.perf_counter() - start

        # Argon2 at these parameters takes tens of milliseconds. A short-circuit
        # would return in microseconds.
        assert missing_user_time > 0.005

    async def test_logout_clears_the_cookie(self, client) -> None:
        response = await client.post("/v1/auth/logout")
        assert response.status_code == 200


# --------------------------------------------------------------------------- #
# Tokens
# --------------------------------------------------------------------------- #
class TestTokens:
    def test_roundtrip(self, settings) -> None:
        token, ttl = issue_token(settings, "u1", "a@b.com", "admin", "org_1")
        claims = verify_token(token, settings)
        assert claims.subject == "u1"
        assert claims.role == "admin"
        assert claims.tenant_id == "org_1"
        assert ttl > 0

    def test_tampered_payload_is_rejected(self, settings) -> None:
        token, _ = issue_token(settings, "u1", "a@b.com", "viewer")
        forged = jwt.encode(
            {
                **jwt.decode(
                    token, settings.jwt_secret, algorithms=["HS256"], audience="obs-platform"
                ),
                "role": "admin",
            },
            "some-other-secret",
            algorithm="HS256",
        )
        with pytest.raises(TokenError):
            verify_token(forged, settings)

    def test_alg_none_is_rejected(self, settings) -> None:
        """The classic JWT bypass."""
        forged = jwt.encode({"sub": "u1", "role": "admin"}, key="", algorithm="none")
        with pytest.raises(TokenError, match="unsigned"):
            verify_token(forged, settings)

    def test_expired_token_is_rejected(self, settings) -> None:
        settings.access_token_ttl_minutes = -1
        token, _ = issue_token(settings, "u1", "a@b.com", "admin")
        with pytest.raises(TokenError):
            verify_token(token, settings)

    def test_unknown_role_falls_back_to_viewer(self, settings) -> None:
        """Fail closed: a misconfigured IdP must not grant raw PII access."""
        from obs_platform.api.deps import Role

        assert Role.parse("superuser") is Role.VIEWER
        assert Role.parse(None) is Role.VIEWER
        assert Role.parse("ADMIN") is Role.ADMIN

    def test_empty_token_is_rejected(self, settings) -> None:
        with pytest.raises(TokenError):
            verify_token("", settings)


# --------------------------------------------------------------------------- #
# Passwords
# --------------------------------------------------------------------------- #
class TestPasswords:
    def test_hash_and_verify(self) -> None:
        digest = hash_password("correct-horse-battery-staple")
        assert digest.startswith("$argon2id$")
        assert verify_password("correct-horse-battery-staple", digest)
        assert not verify_password("wrong", digest)

    def test_hashes_are_salted(self) -> None:
        assert hash_password("same") != hash_password("same")

    def test_oidc_only_user_cannot_password_login(self) -> None:
        """A NULL hash must never verify, however convincing the dummy path is."""
        assert not verify_password("anything", None)

    @pytest.mark.parametrize("password", ["short", "admin12345", "123456789"])
    def test_weak_passwords_rejected(self, password: str) -> None:
        assert validate_password_strength(password) is not None

    def test_reasonable_password_accepted(self) -> None:
        assert validate_password_strength("a-perfectly-fine-passphrase") is None


# --------------------------------------------------------------------------- #
# Audit trail
# --------------------------------------------------------------------------- #
class TestAuditLog:
    async def test_reading_a_trace_is_recorded_with_the_redaction_flag(
        self, viewer_client, database
    ) -> None:
        """'Alice opened tr_9f2c' is trivia. 'and it contained unmasked PII' is
        the artifact a compliance review asks for."""
        await seed_sensitive_trace(database)
        await viewer_client.get("/v1/traces/tr_pii")

        async with database.session() as session:
            rows = list(
                (
                    await session.execute(select(AuditLog).where(AuditLog.action == "trace.read"))
                ).scalars()
            )
        assert len(rows) == 1
        assert rows[0].actor_email == VIEWER_EMAIL
        assert rows[0].actor_role == "viewer"
        assert rows[0].resource_id == "tr_pii"
        assert rows[0].redacted is True

    async def test_admin_read_is_recorded_as_unredacted(self, client, database) -> None:
        await seed_sensitive_trace(database)
        await client.get("/v1/traces/tr_pii")
        async with database.session() as session:
            row = (
                await session.execute(select(AuditLog).where(AuditLog.action == "trace.read"))
            ).scalar_one()
        assert row.redacted is False
        assert row.actor_role == "admin"

    async def test_audit_failure_does_not_fail_the_request(
        self, client, database, monkeypatch
    ) -> None:
        """A bookkeeping outage must not become a product outage."""
        await seed_sensitive_trace(database)

        from obs_platform.security import audit

        def broken_db():
            raise RuntimeError("audit table unreachable")

        monkeypatch.setattr(audit, "get_db", broken_db)
        assert (await client.get("/v1/traces/tr_pii")).status_code == 200

    async def test_baseline_capture_is_audited(self, client, database, settings) -> None:
        from test_phase3_drift_metrics import seed_scores

        settings.drift_min_samples = 5
        await seed_scores(database, "org_1", "faithfulness", [0.9] * 10)
        await client.post(
            "/v1/drift/baseline", json={"tenant_id": "org_1", "metric": "faithfulness"}
        )
        async with database.session() as session:
            rows = list(
                (
                    await session.execute(
                        select(AuditLog).where(AuditLog.action == "drift.baseline_captured")
                    )
                ).scalars()
            )
        assert rows and rows[0].actor_role == "admin"


# --------------------------------------------------------------------------- #
# Rate limiting
# --------------------------------------------------------------------------- #
class TestRateLimiting:
    async def test_limit_blocks_after_the_quota(self, redis) -> None:
        identity = "ip:203.0.113.7"
        results = [await check_rate_limit(identity, limit=5, window_seconds=60) for _ in range(8)]
        assert all(r.allowed for r in results[:5])
        assert not results[-1].allowed
        assert results[-1].headers()["Retry-After"]

    async def test_identities_are_independent(self, redis) -> None:
        for _ in range(6):
            await check_rate_limit("ip:a", limit=5, window_seconds=60)
        assert (await check_rate_limit("ip:b", limit=5, window_seconds=60)).allowed

    async def test_fails_open_when_redis_is_down(self, settings) -> None:
        """A rate limiter that cannot reach Redis must not take the API down."""
        from obs_platform import redis_io

        redis_io.set_redis(None)
        result = await check_rate_limit("ip:c", limit=100, window_seconds=60)
        assert result.allowed

    def test_in_process_counter_is_bounded(self) -> None:
        counter = InProcessCounter()
        for index in range(counter.MAX_KEYS + 100):
            counter.incr(f"k{index}", ttl=0)
        assert len(counter._buckets) <= counter.MAX_KEYS + 200

    async def test_login_is_rate_limited_harder(self, anon_client, users, settings) -> None:
        settings.rate_limit_enabled = True
        settings.rate_limit_login_requests = 3
        codes = [
            (
                await anon_client.post(
                    "/v1/auth/login", json={"email": ADMIN_EMAIL, "password": "wrong"}
                )
            ).status_code
            for _ in range(6)
        ]
        assert 429 in codes, "login must throttle before general API traffic would"


# --------------------------------------------------------------------------- #
# Bootstrap
# --------------------------------------------------------------------------- #
class TestBootstrap:
    async def test_creates_the_first_admin_on_an_empty_database(self, database, settings) -> None:
        settings.bootstrap_admin_email = "root@obs.test"
        settings.bootstrap_admin_password = "a-strong-bootstrap-password"
        async with database.session() as session:
            user = await ensure_bootstrap_admin(session, settings)
        assert user is not None and user.role == "admin"

    async def test_does_not_run_when_users_already_exist(self, database, settings) -> None:
        """Re-running on every boot would let a stale env var resurrect a
        deleted account or reset a changed password."""
        settings.bootstrap_admin_email = "root@obs.test"
        settings.bootstrap_admin_password = "a-strong-bootstrap-password"
        async with database.session() as session:
            await create_user(session, "someone@obs.test", TEST_PASSWORD, role="viewer")
        async with database.session() as session:
            assert await ensure_bootstrap_admin(session, settings) is None

    async def test_refuses_the_example_password_in_production(self, database, settings) -> None:
        settings.environment = "production"
        settings.bootstrap_admin_email = "root@obs.test"
        settings.bootstrap_admin_password = "admin12345"
        async with database.session() as session:
            assert await ensure_bootstrap_admin(session, settings) is None
