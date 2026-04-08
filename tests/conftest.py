"""Root conftest.py for the test suite.

Registers the ``--postgres`` CLI flag and skips ``@pytest.mark.postgres``
tests by default.  Pass ``--postgres`` to opt in::

    uv run pytest --postgres
"""
from __future__ import annotations

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--postgres",
        action="store_true",
        default=False,
        help="Run tests that require a live PostgreSQL instance.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip @pytest.mark.postgres tests unless --postgres is given."""
    if config.getoption("--postgres"):
        return

    skip_marker = pytest.mark.skip(
        reason="PostgreSQL tests are opt-in; pass --postgres to run them"
    )
    for item in items:
        if any(mark.name == "postgres" for mark in item.iter_markers()):
            item.add_marker(skip_marker)
