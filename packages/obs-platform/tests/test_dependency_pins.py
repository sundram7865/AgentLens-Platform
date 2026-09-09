"""The pinned versions must satisfy the ranges the packages declare.

Two files describe the same dependencies for different reasons. ``pyproject.toml``
states the range this code is *compatible with*, which is what anyone installing
the package resolves against. ``requirements*.txt`` pins the exact versions CI
installs and ``pip-audit`` scans. Nothing links them, so they drift -- and they
have, twice: ``structlog`` was pinned at 26.1.0 while the range said ``<26``, and
``aiosqlite`` at 0.22.1 against ``<0.22``.

Drift in that direction is quiet and nasty. CI keeps passing, because CI installs
the pins. What breaks is the person who runs ``pip install obs-platform`` and
resolves against the range instead: they get a version this project has never
run a single test against, and the failure surfaces as their problem, on their
machine, with no obvious cause. Gotcha #14 is about a CVE hiding in a pin; this
is the same shape of gap in the other direction.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest
from packaging.requirements import Requirement
from packaging.version import Version

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = PACKAGE_ROOT.parents[1]


def _declared_ranges(pyproject: Path) -> dict[str, Requirement]:
    """Every requirement the package declares, runtime and extras alike."""
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    project = data["project"]
    specs: list[str] = list(project.get("dependencies", []))
    for extra in project.get("optional-dependencies", {}).values():
        specs.extend(extra)

    ranges: dict[str, Requirement] = {}
    for spec in specs:
        requirement = Requirement(spec)
        ranges[requirement.name.lower().replace("_", "-")] = requirement
    return ranges


def _pins(requirements: Path) -> dict[str, tuple[str, Version]]:
    """The exact ``name==version`` pins in a requirements file."""
    pins: dict[str, tuple[str, Version]] = {}
    for raw in requirements.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-"):
            continue
        requirement = Requirement(line)
        pinned = [s for s in requirement.specifier if s.operator == "=="]
        if not pinned:
            continue
        name = requirement.name.lower().replace("_", "-")
        pins[name] = (line, Version(pinned[0].version))
    return pins


CASES = [
    ("obs-platform", PACKAGE_ROOT / "pyproject.toml", PACKAGE_ROOT / "requirements.txt"),
    ("obs-platform-dev", PACKAGE_ROOT / "pyproject.toml", PACKAGE_ROOT / "requirements-dev.txt"),
    (
        "obs-sdk",
        REPO_ROOT / "packages/obs-sdk/pyproject.toml",
        REPO_ROOT / "packages/obs-sdk/requirements.txt",
    ),
]


@pytest.mark.parametrize(("name", "pyproject", "requirements"), CASES, ids=[c[0] for c in CASES])
def test_every_pin_satisfies_its_declared_range(
    name: str, pyproject: Path, requirements: Path
) -> None:
    ranges = _declared_ranges(pyproject)
    pins = _pins(requirements)
    assert pins, f"{requirements.name} has no pins to check"

    mismatched = []
    for package, (line, version) in pins.items():
        declared = ranges.get(package)
        if declared is None:
            # A transitive pin with no declared range of its own is fine.
            continue
        if not declared.specifier.contains(version, prereleases=True):
            mismatched.append(f"  {line}  does not satisfy  {declared}")

    assert not mismatched, (
        f"{requirements.name} pins versions outside the range {pyproject.name} declares.\n"
        "Anyone installing by range gets an untested version:\n" + "\n".join(mismatched)
    )
