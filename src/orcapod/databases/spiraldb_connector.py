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
        self._closed = False
        self._spiral = sp.Spiral(overrides=overrides)
        self._project = self._spiral.project(project_id)

    def _require_open(self) -> None:
        """Raise RuntimeError if this connector has been closed."""
        if self._closed:
            raise RuntimeError("SpiralDBConnector is closed")

    def _table_id(self, table_name: str) -> str:
        """Return the dataset-qualified table identifier for a plain table name."""
        return f"{self._dataset}.{table_name}"

    def get_table_names(self) -> list[str]:
        """Return all table names in this connector's dataset, sorted alphabetically.

        Filters ``project.list_tables()`` to the connector's dataset. Only the
        plain ``.table`` field is returned; the opaque ``.id`` handle is never
        exposed.

        Returns:
            Sorted list of plain table name strings (no ``dataset.`` prefix).
        """
        self._require_open()
        resources = self._project.list_tables()
        return sorted(r.table for r in resources if r.dataset == self._dataset)

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in declaration order.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of PK column names; empty list if the table has no key schema.
        """
        self._require_open()
        return list(self._project.table(self._table_id(table_name)).key_schema.names)

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow types.

        SpiralDB is Arrow-native; no type mapping layer is needed. Types are
        taken directly from the table's Arrow schema. Note: SpiralDB silently
        drops timezone information from timestamp columns at storage time —
        ``timestamp[us, tz=UTC]`` is returned as ``timestamp[us]``.

        Args:
            table_name: Plain table name (no dataset prefix).

        Returns:
            List of ColumnInfo objects. Propagates pyspiral exception if the
            table does not exist (no empty-list fallback).
        """
        self._require_open()
        arrow_schema = (
            self._project.table(self._table_id(table_name)).schema().to_arrow()
        )
        return [
            ColumnInfo(name=field.name, arrow_type=field.type, nullable=field.nullable)
            for field in arrow_schema
        ]

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a full-table scan and yield results as Arrow RecordBatches.

        SpiralDB has no SQL engine. The table name is parsed from the query's
        FROM clause (double-quoted or unquoted). Only ``SELECT * FROM "table"``
        patterns are supported; WHERE clauses and projections are silently ignored.

        The query must use a plain (non-qualified) table name — not
        ``"dataset.table"``. Passing a qualified name would cause a pyspiral
        lookup failure.

        Args:
            query: SQL-style query. Must contain ``FROM "table_name"``.
            params: Accepted for protocol compliance. SpiralDB has no parameterised
                interface — if not ``None``, a warning is logged and params are
                not used.
            batch_size: Passed to ``Scan.to_record_batches()``; actual batch
                sizing is controlled by the Spiral execution engine.

        Yields:
            Arrow RecordBatch objects. Yields nothing for an empty table.
        """
        self._require_open()
        if params is not None:
            logger.warning(
                "SpiralDBConnector does not support query parameters; "
                "ignoring params=%r",
                params,
            )
        table_name = _parse_table_name(query)
        tbl = self._project.table(self._table_id(table_name))
        reader = self._spiral.scan(tbl.select()).to_record_batches(batch_size=batch_size)
        yield from reader

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a SpiralDB table with a single-column key schema, idempotently.

        Only the primary-key column's type is registered with Spiral at creation
        time (via ``key_schema``). Non-PK columns in ``columns`` are ignored —
        Spiral infers value column schemas from the first write. A second call
        with the same table name is a no-op (``exist_ok=True``).

        Only single-column primary keys are supported via this method. Tables
        with composite primary keys must be created outside this connector but
        can be read and written through it.

        Args:
            table_name: Plain table name (no dataset prefix).
            columns: Full column list; must include ``pk_column``. Non-PK entries
                are accepted but not used at creation time.
            pk_column: Name of the single primary-key column.

        Raises:
            ValueError: If ``pk_column`` is not found in ``columns`` (including
                when ``columns`` is empty).
        """
        self._require_open()
        col_names = [c.name for c in columns]
        if pk_column not in col_names:
            raise ValueError(
                f"pk_column {pk_column!r} not found in columns: {col_names}"
            )
        pk_arrow_type = next(c.arrow_type for c in columns if c.name == pk_column)
        self._project.create_table(
            self._table_id(table_name),
            key_schema=[(pk_column, pk_arrow_type)],
            exist_ok=True,
        )

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
