"""Password hashing.

Argon2id, via ``argon2-cffi``. Not bcrypt (72-byte silent truncation), not
PBKDF2 (cheap to attack on GPUs), and obviously not a bare SHA.

Two details that matter more than the algorithm choice:

* ``verify_password`` runs a **dummy hash** when the user does not exist, so the
  response time does not reveal which emails are registered. Without it, the
  login endpoint is a free account-enumeration oracle.
* ``needs_rehash`` lets parameters be raised later and applied transparently on
  next login, so today's cost factor is not frozen into the database forever.
"""

from __future__ import annotations

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerifyMismatchError

from ..logging import get_logger

log = get_logger("obs_platform.passwords")

# OWASP's second recommended Argon2id configuration (2024): 19 MiB, 2 passes,
# 1 degree of parallelism. Chosen to be defensible on a 512 MB box -- the
# higher-memory profile would make a burst of logins an out-of-memory risk.
_hasher = PasswordHasher(time_cost=2, memory_cost=19_456, parallelism=1, hash_len=32, salt_len=16)

# Precomputed so the "no such user" path does the same work as a real check.
_DUMMY_HASH = _hasher.hash("dummy-password-for-constant-time-comparison")

MIN_PASSWORD_LENGTH = 10


def hash_password(password: str) -> str:
    return _hasher.hash(password)


def verify_password(password: str, password_hash: str | None) -> bool:
    """Verify a password. Takes the same time whether or not the user exists."""
    target = password_hash or _DUMMY_HASH
    try:
        _hasher.verify(target, password)
    except (VerifyMismatchError, InvalidHashError):
        return False
    except Exception:  # pragma: no cover - malformed stored hash
        log.warning("password.verify_error", exc_info=True)
        return False
    # A user row with no hash is OIDC-only; a correct-looking verify against the
    # dummy hash must still fail.
    return password_hash is not None


def needs_rehash(password_hash: str) -> bool:
    try:
        return _hasher.check_needs_rehash(password_hash)
    except Exception:  # pragma: no cover
        return True


def validate_password_strength(password: str) -> str | None:
    """Return an error message, or None if acceptable.

    Deliberately minimal: length is the only rule that reliably correlates with
    strength. Composition rules ("must contain a symbol") push people toward
    Password1! and are worse than useless.
    """
    if len(password) < MIN_PASSWORD_LENGTH:
        return f"Password must be at least {MIN_PASSWORD_LENGTH} characters"
    if password.lower() in {"password12", "changeme12", "admin12345", "letmein123"}:
        return "Password is too common"
    return None


__all__ = [
    "MIN_PASSWORD_LENGTH",
    "hash_password",
    "needs_rehash",
    "validate_password_strength",
    "verify_password",
]
