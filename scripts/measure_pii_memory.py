#!/usr/bin/env python
"""Measure what each PII engine actually costs in memory.

The build plan says: test Presidio's footprint locally before assuming it fits a
free-tier deploy box. This is that test. It reports resident set size before and
after loading each engine and scanning a sample ticket, in a **subprocess per
engine** -- measuring both in one process would attribute spaCy's allocations to
whichever engine happened to load second.

    python scripts/measure_pii_memory.py
    python scripts/measure_pii_memory.py --engine presidio   # internal, one engine

Render's free web service has 512 MB total, shared with FastAPI, SQLAlchemy,
asyncpg and redis. That is the budget the numbers below have to fit inside.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [
    str(ROOT / "packages/obs-sdk/src"),
    str(ROOT / "packages/obs-platform/src"),
]

SAMPLE = (
    "Hi, I am Priya Sharma from Bengaluru. My card 4111 1111 1111 1111 was "
    "charged twice for order A-10294. Reach me at priya.sharma@example.com or "
    "+91 98765 43210. My PAN is ABCDE1234F."
)

FREE_TIER_MB = 512


def rss_mb() -> float:
    """Resident set size in MB, by whatever mechanism this platform offers."""
    try:
        import psutil

        return psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        pass
    try:  # POSIX
        import resource

        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # Linux reports kB, macOS bytes.
        return peak / 1024 if sys.platform != "darwin" else peak / (1024 * 1024)
    except ImportError:
        return -1.0


def measure_one(engine: str) -> dict[str, object]:
    baseline = rss_mb()
    try:
        from obs_platform.guardrails.pii import build_engine

        detector = build_engine(engine)
        matches = detector.analyze(SAMPLE)
        loaded = rss_mb()
        return {
            "engine": engine,
            "resolved_to": detector.name,
            "available": True,
            "baseline_rss_mb": round(baseline, 1),
            "loaded_rss_mb": round(loaded, 1),
            "delta_mb": round(loaded - baseline, 1),
            "matches": sorted({m.pattern for m in matches}),
        }
    except Exception as exc:
        return {
            "engine": engine,
            "available": False,
            "error": f"{type(exc).__name__}: {exc}"[:300],
            "baseline_rss_mb": round(baseline, 1),
        }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["builtin", "presidio"])
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.engine:
        print(json.dumps(measure_one(args.engine)))
        return 0

    results = []
    for engine in ("builtin", "presidio"):
        # Subprocess per engine: spaCy's allocations must not be charged to the
        # built-in engine just because it loaded first in the same interpreter.
        proc = subprocess.run(
            [sys.executable, __file__, "--engine", engine],
            capture_output=True,
            text=True,
            cwd=str(ROOT),
        )
        try:
            results.append(json.loads(proc.stdout.strip().splitlines()[-1]))
        except (ValueError, IndexError):
            results.append({"engine": engine, "available": False, "error": proc.stderr[-300:]})

    if args.json:
        print(json.dumps(results, indent=2))
        return 0

    print(f"PII engine memory footprint (free-tier budget: {FREE_TIER_MB} MB total)\n")
    print(f"{'engine':>10} {'resolved':>10} {'baseline':>10} {'loaded':>10} {'delta':>9}  detects")
    for row in results:
        if not row.get("available"):
            print(f"{row['engine']:>10} {'-':>10} {'-':>10} {'-':>10} {'-':>9}  NOT INSTALLED")
            print(f"           {str(row.get('error', ''))[:140]}")
            continue
        print(
            f"{row['engine']:>10} {row['resolved_to']:>10} "
            f"{row['baseline_rss_mb']:>9.1f}M {row['loaded_rss_mb']:>9.1f}M "
            f"{row['delta_mb']:>8.1f}M  {', '.join(row['matches'])}"
        )

    print()
    for row in results:
        if not row.get("available"):
            continue
        loaded = float(row["loaded_rss_mb"])
        verdict = "fits" if loaded < FREE_TIER_MB * 0.6 else "DOES NOT FIT with the app"
        print(f"  {row['engine']:>8}: {loaded:.0f} MB resident -> {verdict}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
