"""Marks this directory as a package so its conftest does not shadow the unit one.

Without it, pytest inserts both ``tests/`` and ``tests/integration/`` at the
front of ``sys.path`` and imports each conftest as the top-level module
``conftest``. Which one wins depends on collection order, so
``from conftest import ADMIN_EMAIL`` in a unit test resolved -- intermittently --
to the integration conftest and failed the whole run at collection time. As a
package, this conftest is ``integration.conftest`` and the ambiguity is gone.
"""
