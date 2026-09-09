"""Bearer tokens.

Two accepted issuers:

**This platform** (HS256, signed with ``OBS_JWT_SECRET``). The default. No extra
infrastructure, which is what makes the free-tier deployment actually free --
Keycloak alone wants more memory than the whole target box has.

**An external OIDC provider** (RS256 via JWKS), when ``OBS_OIDC_ISSUER`` and
``OBS_OIDC_JWKS_URL`` are configured. That covers Keycloak, Auth0 and Clerk
without the platform having to care which. SupportPilot already uses Clerk, so
an operator can front this with the same identity provider rather than managing
a second set of credentials.

Verification is strict on both paths: signature, expiry, issuer and audience are
all checked. ``jwt.decode(..., options={"verify_signature": False})`` appears
nowhere in this codebase, which is the single most common way a JWT integration
turns into "any string is a valid admin token".
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import jwt

try:
    from jwt import InvalidTokenError, PyJWKClient
except ImportError as _exc:  # pragma: no cover - environment misconfiguration
    # `pip install jwt` installs a DIFFERENT, unrelated package that occupies the
    # same `jwt` module name and shadows PyJWT. The failure is an obscure
    # ImportError deep in this module; say what is actually wrong instead.
    raise ImportError(
        "The 'jwt' package is shadowing PyJWT. This project needs PyJWT: "
        "run `pip uninstall -y jwt && pip install 'PyJWT[crypto]'`, or use a "
        "virtualenv built from packages/obs-platform/requirements.txt, which "
        "avoids the collision entirely."
    ) from _exc

from ..logging import get_logger
from ..settings import Settings

log = get_logger("obs_platform.tokens")

ISSUER = "obs-platform"
LEEWAY_SECONDS = 30  # tolerate small clock skew between the API and the client

_jwks_client: PyJWKClient | None = None
_jwks_url: str | None = None


@dataclass
class TokenClaims:
    subject: str
    email: str
    role: str
    tenant_id: str | None = None
    issuer: str = ISSUER
    expires_at: int = 0
    raw: dict[str, Any] | None = None


class TokenError(Exception):
    """Any failure to produce a trustworthy identity from a token."""


def issue_token(
    settings: Settings,
    subject: str,
    email: str,
    role: str,
    tenant_id: str | None = None,
) -> tuple[str, int]:
    """Issue an access token. Returns ``(token, expires_in_seconds)``."""
    now = int(time.time())
    ttl = settings.access_token_ttl_minutes * 60
    payload = {
        "sub": subject,
        "email": email,
        "role": role,
        "tenant_id": tenant_id,
        "iss": ISSUER,
        "aud": ISSUER,
        "iat": now,
        "nbf": now,
        "exp": now + ttl,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm), ttl


def verify_token(token: str, settings: Settings) -> TokenClaims:
    """Verify a bearer token from either accepted issuer."""
    if not token:
        raise TokenError("missing token")

    try:
        header = jwt.get_unverified_header(token)
    except InvalidTokenError as exc:
        raise TokenError("malformed token") from exc

    algorithm = header.get("alg", "")
    if algorithm == "none":
        # The classic JWT attack. Explicit rather than implied.
        raise TokenError("unsigned tokens are not accepted")

    if algorithm.startswith("HS"):
        return _verify_local(token, settings)
    if settings.oidc_issuer and settings.oidc_jwks_url:
        return _verify_oidc(token, settings)
    raise TokenError(f"unsupported token algorithm {algorithm}")


def _verify_local(token: str, settings: Settings) -> TokenClaims:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            audience=ISSUER,
            issuer=ISSUER,
            leeway=LEEWAY_SECONDS,
            options={"require": ["exp", "sub", "iss"]},
        )
    except InvalidTokenError as exc:
        raise TokenError(str(exc)) from exc
    return TokenClaims(
        subject=str(payload.get("sub", "")),
        email=str(payload.get("email", "")),
        role=str(payload.get("role", "viewer")),
        tenant_id=payload.get("tenant_id"),
        issuer=ISSUER,
        expires_at=int(payload.get("exp", 0)),
        raw=payload,
    )


def _verify_oidc(token: str, settings: Settings) -> TokenClaims:
    global _jwks_client, _jwks_url
    if _jwks_client is None or _jwks_url != settings.oidc_jwks_url:
        # PyJWKClient caches keys internally, so this is not a fetch per request.
        _jwks_client = PyJWKClient(settings.oidc_jwks_url or "", cache_keys=True)
        _jwks_url = settings.oidc_jwks_url

    try:
        signing_key = _jwks_client.get_signing_key_from_jwt(token)
        payload = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "RS384", "RS512", "ES256"],
            audience=settings.oidc_audience or None,
            issuer=settings.oidc_issuer,
            leeway=LEEWAY_SECONDS,
            options={
                "require": ["exp", "sub", "iss"],
                "verify_aud": bool(settings.oidc_audience),
            },
        )
    except InvalidTokenError as exc:
        raise TokenError(str(exc)) from exc
    except Exception as exc:  # JWKS fetch failure
        log.warning("token.jwks_failed", error=str(exc)[:200])
        raise TokenError("could not verify token against the identity provider") from exc

    return TokenClaims(
        subject=str(payload.get("sub", "")),
        email=str(payload.get("email") or payload.get("preferred_username") or ""),
        role=_role_from_claims(payload, settings),
        tenant_id=payload.get("tenant_id") or payload.get("org_id"),
        issuer=str(payload.get("iss", "")),
        expires_at=int(payload.get("exp", 0)),
        raw=payload,
    )


def _role_from_claims(payload: dict[str, Any], settings: Settings) -> str:
    """Map an external provider's claims onto this platform's two roles.

    Defaults to ``viewer``. An identity provider that does not say "admin"
    must not accidentally grant unredacted access to customer data.
    """
    claim = payload.get(settings.oidc_role_claim)
    if isinstance(claim, str) and claim.lower() in {"admin", "viewer"}:
        return claim.lower()
    if isinstance(claim, list) and any(str(v).lower() == "admin" for v in claim):
        return "admin"
    # Keycloak's default shape.
    realm_roles = (payload.get("realm_access") or {}).get("roles") or []
    if any(str(role).lower() in {"obs-admin", "admin"} for role in realm_roles):
        return "admin"
    return "viewer"


def reset_jwks_cache() -> None:
    global _jwks_client, _jwks_url
    _jwks_client, _jwks_url = None, None


__all__ = ["ISSUER", "TokenClaims", "TokenError", "issue_token", "reset_jwks_cache", "verify_token"]
