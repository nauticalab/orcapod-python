"""conftest.py for test_databases.

Installs a lightweight mock psycopg module into sys.modules so that:
  - ``patch("psycopg.connect")`` works even when libpq is not installed.
  - The LazyModule("psycopg") in postgresql_connector.py resolves without
    trying to load the real psycopg (which requires libpq).
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
