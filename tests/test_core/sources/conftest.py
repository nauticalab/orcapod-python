"""conftest.py for test_core/sources.

Installs a lightweight mock psycopg module into sys.modules so that:
  - The module-level ``import psycopg`` in integration test files does not
    fail collection in environments where libpq / psycopg is not installed.
  - Tests marked ``@pytest.mark.postgres`` are skipped when the real psycopg
    is absent (they require a live PostgreSQL instance to pass).
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

# Only install the stub if psycopg is not already importable.
try:
    import psycopg  # noqa: F401
except ImportError:
    _psycopg_stub = types.ModuleType("psycopg")
    _psycopg_stub.connect = MagicMock()  # type: ignore[attr-defined]
    # Mark this module as a stub so we can distinguish it from the real psycopg.
    _psycopg_stub.__psycopg_stub__ = True  # type: ignore[attr-defined]
    sys.modules["psycopg"] = _psycopg_stub


def _is_real_psycopg_available() -> bool:
    """Return True if the real psycopg package (backed by libpq) is available.

    This distinguishes between the lightweight stub installed above and a
    genuine psycopg installation.
    """
    try:
        import psycopg as _psycopg  # type: ignore[import]
    except ImportError:
        return False
    # If the imported module carries the stub marker, treat it as unavailable.
    return not getattr(_psycopg, "__psycopg_stub__", False)


def pytest_collection_modifyitems(config, items):
    """Skip @pytest.mark.postgres tests when real psycopg is not available."""
    if _is_real_psycopg_available():
        return

    skip_marker = pytest.mark.skip(
        reason="psycopg not available; skipping PostgreSQL integration tests"
    )
    for item in items:
        # item.iter_markers() is available on pytest.Item and returns all markers.
        if any(mark.name == "postgres" for mark in item.iter_markers()):
            item.add_marker(skip_marker)
