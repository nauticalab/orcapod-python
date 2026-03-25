"""SpiralDBConnector — DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is handled externally via the ``spiral login`` CLI command,
which stores credentials in ``~/.config/pyspiral/auth.json``.

The connector is dataset-scoped: all tables are read from and written to
a single ``(project_id, dataset)`` pair.

Example::

    connector = SpiralDBConnector(project_id="my-project-123456", dataset="default")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""
from __future__ import annotations

import logging
import re
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa
    import spiral as sp
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")
    sp = LazyModule("spiral")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _parse_table_name(query: str) -> str:
    """Extract the plain table name from a SELECT * FROM ... query string.

    Supports double-quoted identifiers (``"table_name"``) and unquoted bare
    identifiers (``table_name``). Always returns the plain table name — not
    a ``dataset.table`` qualified form.

    Args:
        query: SQL-style query string, e.g. ``'SELECT * FROM "my_table"'``.

    Returns:
        Plain table name string.

    Raises:
        ValueError: If no table name can be parsed from the query.
    """
    m = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not m:
        m = re.search(r"FROM\s+(\w+)", query, re.IGNORECASE)
    if not m:
        raise ValueError(f"Cannot parse table name from query: {query!r}")
    return m.group(1)


# ---------------------------------------------------------------------------
# SpiralDBConnector (stub — methods implemented task by task)
# ---------------------------------------------------------------------------


class SpiralDBConnector:
    """DBConnectorProtocol implementation backed by SpiralDB (pyspiral).

    Scoped to a single dataset within a SpiralDB project. Auth is handled
    externally — run ``spiral login`` once to store credentials in
    ``~/.config/pyspiral/auth.json``.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        dataset: Dataset within the project. Defaults to ``"default"``.
        overrides: Optional pyspiral client config overrides, e.g.
            ``{"server.url": "http://api.spiraldb.dev"}`` for the dev
            environment. See the pyspiral config docs for full options.
    """

    def __init__(
        self,
        project_id: str,
        dataset: str = "default",
        overrides: dict[str, str] | None = None,
    ) -> None:
        self._project_id = project_id
        self._dataset = dataset
        self._overrides = overrides
        self._spiral = sp.Spiral(overrides=overrides)
        self._project = self._spiral.project(project_id)
        self._closed = False

    def _require_open(self) -> None:
        """Raise RuntimeError if this connector has been closed."""
        if self._closed:
            raise RuntimeError("SpiralDBConnector is closed")

    def _table_id(self, table_name: str) -> str:
        """Return the dataset-qualified table identifier for a plain table name."""
        return f"{self._dataset}.{table_name}"

    def get_table_names(self) -> list[str]:
        self._require_open()
        raise NotImplementedError

    def get_pk_columns(self, table_name: str) -> list[str]:
        self._require_open()
        raise NotImplementedError

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        self._require_open()
        raise NotImplementedError

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        self._require_open()
        raise NotImplementedError

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        self._require_open()
        raise NotImplementedError

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        self._require_open()
        raise NotImplementedError

    def close(self) -> None:
        """Mark this connector as closed. Idempotent. No network teardown needed."""
        self._closed = True

    def __enter__(self) -> SpiralDBConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def to_config(self) -> dict[str, Any]:
        self._require_open()
        raise NotImplementedError

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SpiralDBConnector:
        raise NotImplementedError
