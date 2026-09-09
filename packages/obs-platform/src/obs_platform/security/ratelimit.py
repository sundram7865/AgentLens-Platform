"""Rate limiting.

An authenticated-but-unthrottled API is the gap nobody notices until it is
abused. A single valid token can otherwise page through every trace a tenant
has ever produced as fast as the network allows -- which is both a denial of
service against Neon's free tier connection budget and the most efficient way to
exfiltrate the data the redaction layer is protecting.

**Sliding window over two fixed buckets.** A plain fixed window lets a caller
send the full quota at 0:59 and again at 1:01 -- double the intended rate across
the boundary. Weighting the previous bucket by how much of it still overlaps the
current window costs one extra counter and removes that.

**Redis when available, in-process otherwise.** Redis makes the limit hold
across replicas; the in-process fallback means a Redis outage degrades the limit
to per-process rather than removing it. Failing *open* on a Redis error is
deliberate: an observability API that stops serving because its rate limiter
cannot reach Redis has turned a dependency blip into an outage.
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass

from ..logging import get_logger
from ..redis_io import get_redis

log = get_logger("obs_platform.ratelimit")

KEY_PREFIX = "obs:rl"


@dataclass
class RateLimitResult:
    allowed: bool
    limit: int
    remaining: int
    reset_after: int
    used: float = 0.0

    def headers(self) -> dict[str, str]:
        headers = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(max(0, self.remaining)),
            "X-RateLimit-Reset": str(self.reset_after),
        }
        if not self.allowed:
            headers["Retry-After"] = str(self.reset_after)
        return headers


class InProcessCounter:
    """Fallback store. Bounded so a flood of distinct IPs cannot exhaust memory."""

    MAX_KEYS = 20_000

    def __init__(self) -> None:
        self._buckets: dict[str, float] = defaultdict(float)
        self._expiry: dict[str, float] = {}

    def incr(self, key: str, ttl: int) -> float:
        now = time.time()
        if len(self._buckets) > self.MAX_KEYS:
            self._evict(now)
        if self._expiry.get(key, 0) < now:
            self._buckets[key] = 0.0
        self._buckets[key] += 1
        self._expiry[key] = now + ttl
        return self._buckets[key]

    def get(self, key: str) -> float:
        if self._expiry.get(key, 0) < time.time():
            return 0.0
        return self._buckets.get(key, 0.0)

    def _evict(self, now: float) -> None:
        for key in [k for k, expiry in self._expiry.items() if expiry < now]:
            self._buckets.pop(key, None)
            self._expiry.pop(key, None)


_fallback = InProcessCounter()


def _bucket_keys(identity: str, scope: str, window: int, now: float) -> tuple[str, str, float]:
    current_index = int(now // window)
    elapsed = (now % window) / window
    return (
        f"{KEY_PREFIX}:{scope}:{identity}:{current_index}",
        f"{KEY_PREFIX}:{scope}:{identity}:{current_index - 1}",
        elapsed,
    )


async def check_rate_limit(
    identity: str, limit: int, window_seconds: int, scope: str = "api"
) -> RateLimitResult:
    """Consume one unit of quota for ``identity``. Fails open on a Redis error."""
    now = time.time()
    current_key, previous_key, elapsed = _bucket_keys(identity, scope, window_seconds, now)
    ttl = window_seconds * 2

    try:
        redis = get_redis()
        pipe = redis.pipeline(transaction=False)
        pipe.incr(current_key)
        pipe.expire(current_key, ttl)
        pipe.get(previous_key)
        current, _, previous_raw = await pipe.execute()
        current = float(current or 0)
        previous = float(previous_raw or 0)
    except Exception as exc:
        log.debug("ratelimit.redis_unavailable", error=str(exc)[:200])
        current = _fallback.incr(current_key, ttl)
        previous = _fallback.get(previous_key)

    # Sliding window: the previous bucket still counts, weighted by overlap.
    used = current + previous * (1.0 - elapsed)
    reset_after = int(window_seconds - (now % window_seconds)) or window_seconds
    return RateLimitResult(
        allowed=used <= limit,
        limit=limit,
        remaining=int(max(0, limit - used)),
        reset_after=reset_after,
        used=round(used, 2),
    )


def client_identity(request: object) -> str:
    """Identify the caller: authenticated subject first, then IP.

    Keying on the token subject when present means one noisy account cannot
    consume the quota of everyone behind the same NAT or corporate proxy.
    """
    principal = getattr(getattr(request, "state", None), "principal", None)
    if principal is not None and getattr(principal, "id", None):
        return f"user:{principal.id}"
    headers = getattr(request, "headers", {})
    forwarded = headers.get("x-forwarded-for", "") if hasattr(headers, "get") else ""
    if forwarded:
        return f"ip:{forwarded.split(',')[0].strip()}"
    client = getattr(request, "client", None)
    return f"ip:{getattr(client, 'host', 'unknown')}"


__all__ = ["InProcessCounter", "RateLimitResult", "check_rate_limit", "client_identity"]
