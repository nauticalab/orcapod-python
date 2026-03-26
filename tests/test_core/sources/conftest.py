"""conftest.py for test_core/sources.

Installs a lightweight mock psycopg module into sys.modules so that:
  - The module-level ``import psycopg`` in integration test files does not
    fail collection in environments where libpq / psycopg is not installed.
  - Tests marked ``@pytest.mark.postgres`` will still be skipped if the real
    psycopg is absent (they require a live PostgreSQL instance to pass).
"""
from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

# Only install the stub if psycopg is not already importable.
try:
    import psycopg  # noqa: F401
except ImportError:
    _psycopg_stub = types.ModuleType("psycopg")
    _psycopg_stub.connect = MagicMock()  # type: ignore[attr-defined]
    sys.modules["psycopg"] = _psycopg_stub
