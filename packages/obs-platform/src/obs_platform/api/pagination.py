"""Keyset pagination.

Offset pagination is the default everyone reaches for and it fails twice over:
``OFFSET 10000`` makes the database walk and throw away ten thousand rows, and
because new traces keep arriving at the head of the list, the rows under an
offset shift between requests -- so page 2 silently repeats or skips entries.

A cursor encodes the last row's sort key. The next page is
``WHERE (created_at, trace_id) < (cursor)``, which is an index range scan of
constant cost no matter how deep the reader has gone, and is stable under
concurrent inserts.
"""

from __future__ import annotations

import base64
import binascii
from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException, status

CURSOR_SEPARATOR = "|"


def encode_cursor(sort_value: datetime, tiebreak: str) -> str:
    # Drivers differ: Postgres hands back an aware datetime, SQLite a naive one.
    # `.timestamp()` on a naive value silently assumes *local* time, which would
    # shift the cursor by the UTC offset and quietly return an empty second page.
    if sort_value.tzinfo is None:
        sort_value = sort_value.replace(tzinfo=UTC)
    raw = f"{sort_value.timestamp()}{CURSOR_SEPARATOR}{tiebreak}"
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, str]:
    """Decode a cursor, rejecting anything malformed with a 400.

    A bad cursor is a client error, not a 500 -- and definitely not an
    unbounded query that quietly ignores the cursor and returns page one.
    """
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        timestamp_text, _, tiebreak = raw.partition(CURSOR_SEPARATOR)
        return datetime.fromtimestamp(float(timestamp_text), tz=UTC), tiebreak
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid cursor"
        ) from exc


def clamp_limit(limit: int | None, default: int, maximum: int) -> int:
    """Bound the page size. An unbounded ``limit`` is a denial-of-service knob."""
    if limit is None:
        return default
    return max(1, min(int(limit), maximum))


def build_page(rows: list[Any], limit: int, cursor_of: Any) -> tuple[list[Any], str | None, bool]:
    """Trim an over-fetched result set into a page plus a next cursor.

    The caller queries ``limit + 1`` rows; the extra row is how we know whether
    more exist without a second COUNT query.
    """
    has_more = len(rows) > limit
    page = rows[:limit]
    next_cursor = cursor_of(page[-1]) if (has_more and page) else None
    return page, next_cursor, has_more


__all__ = ["build_page", "clamp_limit", "decode_cursor", "encode_cursor"]
