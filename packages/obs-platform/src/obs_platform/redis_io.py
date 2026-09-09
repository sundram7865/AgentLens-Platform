"""Redis client and stream primitives.

Everything that talks to Redis Streams goes through here so the command budget
is visible in one file. Upstash's free tier bills per command (10k/day), which
rules out the naive "poll every 100 ms" consumer loop -- a single idle worker
polling at that rate burns 864,000 commands a day doing nothing at all.

The consumer therefore uses a *blocking* ``XREADGROUP`` with a multi-second
timeout and a batch ``COUNT``: one command returns up to N messages, and an idle
worker issues roughly one command per block interval.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from .logging import get_logger
from .settings import get_settings

log = get_logger("obs_platform.redis")

_client: Redis | None = None


def get_redis() -> Redis:
    """Process-wide async client. ``decode_responses=True`` -- we only store text."""
    global _client
    if _client is None:
        settings = get_settings()
        _client = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_keepalive=True,
            health_check_interval=30,
            retry_on_timeout=True,
        )
    return _client


def set_redis(client: Redis | None) -> None:
    """Swap the process-wide client. Used by tests to inject fakeredis."""
    global _client
    _client = client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def ensure_group(stream: str, group: str, client: Redis | None = None) -> None:
    """Create the consumer group, and the stream with it, if either is missing.

    ``mkstream=True`` matters on a cold deploy: the workers usually start before
    the first event is ever published, and without it every consumer would crash
    on a missing key until traffic happened to arrive.
    """
    redis = client or get_redis()
    try:
        await redis.xgroup_create(name=stream, groupname=group, id="0", mkstream=True)
        log.info("stream.group_created", stream=stream, group=group)
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


async def stream_length(stream: str, client: Redis | None = None) -> int:
    redis = client or get_redis()
    try:
        return int(await redis.xlen(stream))
    except ResponseError:
        return 0


async def group_info(stream: str, client: Redis | None = None) -> list[dict[str, Any]]:
    """``XINFO GROUPS``, normalised, with ``lag`` filled in on older servers."""
    redis = client or get_redis()
    try:
        raw = await redis.xinfo_groups(stream)
    except ResponseError:
        return []

    length = await stream_length(stream, redis)
    groups: list[dict[str, Any]] = []
    for entry in raw:
        item = {_s(k): _s(v) for k, v in entry.items()}
        lag = item.get("lag")
        if lag is None:
            # Redis < 7 has no `lag`; entries-read is the next best estimate.
            entries_read = _int(item.get("entries-read"))
            lag = max(0, length - entries_read) if entries_read is not None else None
        groups.append(
            {
                "name": item.get("name"),
                "consumers": _int(item.get("consumers")) or 0,
                "pending": _int(item.get("pending")) or 0,
                "last_delivered_id": item.get("last-delivered-id"),
                "lag": _int(lag) or 0,
            }
        )
    return groups


async def stream_health(stream: str | None = None, client: Redis | None = None) -> dict[str, Any]:
    """Length plus per-group lag. Feeds ``/health/meta``."""
    settings = get_settings()
    target = stream or settings.stream
    redis = client or get_redis()
    length = await stream_length(target, redis)
    return {
        "stream": target,
        "length": length,
        "maxlen": settings.stream_maxlen,
        "groups": await group_info(target, redis),
    }


async def pending_summary(stream: str, group: str, client: Redis | None = None) -> dict[str, Any]:
    """``XPENDING`` summary: how many messages are claimed but not acked."""
    redis = client or get_redis()
    try:
        raw = await redis.xpending(stream, group)
    except ResponseError:
        return {"pending": 0, "consumers": []}
    if not raw:
        return {"pending": 0, "consumers": []}
    if isinstance(raw, dict):
        return {
            "pending": _int(raw.get("pending")) or 0,
            "min_id": _s(raw.get("min")),
            "max_id": _s(raw.get("max")),
            "consumers": [
                {"name": _s(c.get("name")), "pending": _int(c.get("pending")) or 0}
                for c in (raw.get("consumers") or [])
            ],
        }
    return {"pending": _int(raw[0]) or 0, "consumers": []}


def _s(value: Any) -> Any:
    return value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else value


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_entries(entries: Iterable[Any]) -> list[tuple[str, dict[str, Any]]]:
    """Flatten an ``XREADGROUP`` reply into ``[(message_id, fields), ...]``."""
    out: list[tuple[str, dict[str, Any]]] = []
    for item in entries or []:
        # redis-py returns [(stream_name, [(id, {..}), ...]), ...]
        if isinstance(item, (list, tuple)) and len(item) == 2 and isinstance(item[1], list):
            for message_id, fields in item[1]:
                out.append((_s(message_id), {_s(k): _s(v) for k, v in (fields or {}).items()}))
        elif isinstance(item, (list, tuple)) and len(item) == 2:
            message_id, fields = item
            if isinstance(fields, dict):
                out.append((_s(message_id), {_s(k): _s(v) for k, v in fields.items()}))
    return out


__all__ = [
    "close_redis",
    "ensure_group",
    "get_redis",
    "group_info",
    "normalize_entries",
    "pending_summary",
    "set_redis",
    "stream_health",
    "stream_length",
]
