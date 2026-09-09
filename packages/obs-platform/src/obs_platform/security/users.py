"""Operator accounts.

Two roles, and only two: ``admin`` sees raw trace payloads, ``viewer`` sees them
redacted. Resisting a third role is a deliberate choice -- the moment there are
five, nobody can say from memory which one can read customer PII, and that is the
only question this system's authorisation model has to answer clearly.

``tenant_id`` scopes an operator to one tenant; NULL means all of them.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..logging import get_logger
from ..models import User
from ..settings import Settings
from .passwords import hash_password, needs_rehash, verify_password

log = get_logger("obs_platform.users")

ROLES = ("admin", "viewer")


def normalize_email(email: str) -> str:
    return email.strip().lower()


async def get_by_email(session: AsyncSession, email: str) -> User | None:
    return (
        await session.execute(select(User).where(User.email == normalize_email(email)))
    ).scalar_one_or_none()


async def get_by_subject(session: AsyncSession, subject: str) -> User | None:
    return (await session.execute(select(User).where(User.subject == subject))).scalar_one_or_none()


async def create_user(
    session: AsyncSession,
    email: str,
    password: str | None,
    role: str = "viewer",
    tenant_id: str | None = None,
    subject: str | None = None,
) -> User:
    if role not in ROLES:
        raise ValueError(f"role must be one of {ROLES}")
    user = User(
        id=str(uuid.uuid4()),
        email=normalize_email(email),
        password_hash=hash_password(password) if password else None,
        subject=subject,
        role=role,
        tenant_id=tenant_id,
        is_active=True,
    )
    session.add(user)
    await session.flush()
    log.info("user.created", email=user.email, role=role, tenant_id=tenant_id)
    return user


async def authenticate(session: AsyncSession, email: str, password: str) -> User | None:
    """Verify credentials. Constant-time with respect to whether the user exists."""
    user = await get_by_email(session, email)
    # verify_password always does the work, even with no user, so the response
    # time cannot be used to enumerate registered accounts.
    if not verify_password(password, user.password_hash if user else None):
        return None
    if user is None or not user.is_active:
        return None

    user.last_login_at = datetime.now(UTC)
    if user.password_hash and needs_rehash(user.password_hash):
        # Cost parameters can be raised later and applied transparently, rather
        # than freezing today's factor into the database forever.
        user.password_hash = hash_password(password)
        log.info("user.password_rehashed", email=user.email)
    return user


async def ensure_bootstrap_admin(session: AsyncSession, settings: Settings) -> User | None:
    """Create the first admin on an empty deployment.

    Only when the users table is empty. Re-running it on every boot would let a
    stale environment variable silently resurrect a deleted account or reset a
    changed password.
    """
    if not settings.bootstrap_admin_email or not settings.bootstrap_admin_password:
        return None

    count = (await session.execute(select(func.count()).select_from(User))).scalar() or 0
    if count:
        return None

    if settings.environment == "production" and settings.bootstrap_admin_password == "admin12345":
        log.error(
            "user.bootstrap_refused",
            reason="OBS_BOOTSTRAP_ADMIN_PASSWORD is still the example value",
        )
        return None

    user = await create_user(
        session,
        email=settings.bootstrap_admin_email,
        password=settings.bootstrap_admin_password,
        role="admin",
    )
    log.warning(
        "user.bootstrap_admin_created",
        email=user.email,
        hint="change this password immediately after first login",
    )
    return user


async def upsert_oidc_user(
    session: AsyncSession, subject: str, email: str, role: str, tenant_id: str | None
) -> User:
    """Mirror an external identity locally so audit rows have a stable actor id."""
    user = await get_by_subject(session, subject) or await get_by_email(session, email)
    if user is None:
        return await create_user(
            session, email=email, password=None, role=role, tenant_id=tenant_id, subject=subject
        )
    user.subject = subject
    # The identity provider is authoritative for role and tenant on each login,
    # so revoking admin there takes effect here on the next request.
    user.role = role
    user.tenant_id = tenant_id
    user.last_login_at = datetime.now(UTC)
    return user


__all__ = [
    "ROLES",
    "authenticate",
    "create_user",
    "ensure_bootstrap_admin",
    "get_by_email",
    "get_by_subject",
    "normalize_email",
    "upsert_oidc_user",
]
