"""Deterministic sampling.

``random.random() < rate`` is the obvious implementation and the wrong one. A
redelivered message would roll the dice again: the same trace can be scored
twice (double spend, two conflicting scores) or scored once and then skipped on
the retry that was supposed to complete it. Coverage becomes unrepeatable, which
also makes "why was this trace not scored?" unanswerable.

Hashing the trace id makes the decision a pure function of the trace. A retry
reaches the same answer, every consumer replica reaches the same answer, and you
can check by hand whether a specific trace should have been scored.

blake2b, not the built-in ``hash()``: Python randomises string hashing per
process (PYTHONHASHSEED), so ``hash(trace_id) % 100`` would give two workers --
or the same worker after a restart -- different answers for the same trace.
"""

from __future__ import annotations

import hashlib

BUCKETS = 100


def bucket_of(trace_id: str, salt: str = "") -> int:
    """Stable bucket in [0, 100) for a trace id."""
    digest = hashlib.blake2b((salt + trace_id).encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") % BUCKETS


def should_sample(trace_id: str, rate_percent: int, salt: str = "") -> bool:
    """True when this trace falls inside a ``rate_percent`` sample.

    ``salt`` lets an independent sampler (say, a second metric) select a
    different subset at the same rate instead of always grading the same traces.
    """
    if rate_percent <= 0:
        return False
    if rate_percent >= BUCKETS:
        return True
    return bucket_of(trace_id, salt) < rate_percent


__all__ = ["BUCKETS", "bucket_of", "should_sample"]
