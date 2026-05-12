"""SQLiteTableSource — a read-only RootSource backed by a SQLite table.

Wraps a SQLite table as an OrcaPod Source. Primary-key columns are used
as key columns by default. For tables with no explicit primary key
(ROWID-only tables), the implicit ``rowid`` integer column is used
automatically.

Example::

    # File-backed (supports from_config round-trip)
    source = SQLiteTableSource("/path/to/my.db", "measurements")

    # In-memory (for tests / throwaway pipelines; cannot round-trip)
    source = SQLiteTableSource(":memory:", "events", key_columns=["session_id"])

Note:
    ``:memory:`` sources cannot be reconstructed via ``from_config`` because
    each new ``SQLiteConnector(":memory:")`` opens a fresh empty database.
    File-backed sources (including ROWID-only tables) round-trip correctly.
"""
from __future__ import annotations

import os
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.databases.sqlite_connector import SQLiteConnector

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import Config


class SQLiteTableSource(DBTableSource):
    """A read-only Source backed by a table in a SQLite database.

    At construction time the source:
    1. Opens a ``SQLiteConnector`` for *db_path*.
    2. Validates the table exists.
    3. Resolves key columns:
       - If *key_columns* is provided, uses them as-is.
       - Otherwise uses the table's primary-key columns.
       - If the table has no explicit PK (ROWID-only), falls back to the
         implicit ``rowid`` integer column.
    4. Determines the fetch query: injects ``SELECT rowid, *`` when
       ``"rowid"`` is a resolved key column and not a normal table column
       (handles both auto-detection and ``from_config`` reconstruction).
    5. Delegates to ``DBTableSource.__init__`` for fetching and stream building.
    6. Closes the connector — all data is eagerly loaded into memory, so the
       connection is released immediately to avoid holding file locks.

    Args:
        db_path: Path to the SQLite database file, or ``":memory:"`` for an
            in-process in-memory database.
        table_name: Name of the table to expose as a source.
        key_columns: Columns to use as key columns. If ``None`` (default),
            the table's primary-key columns are used; ROWID-only tables fall
            back to ``["rowid"]``.
        system_key_columns: Additional system-level key columns.
        record_id_column: Column for stable per-row record IDs in provenance.
        source_id: Canonical source name. Defaults to *table_name*.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.

    Raises:
        ValueError: If the table is not found or is empty.
        sqlite3.OperationalError: If *db_path* cannot be opened.
    """

    def __init__(
        self,
        db_path: str | os.PathLike,
        table_name: str,
        key_columns: Collection[str] | None = None,
        system_key_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: "Config | None" = None,
    ) -> None:
        self._db_path = db_path
        connector = SQLiteConnector(db_path)

        try:
            # Step 3: Resolve key columns.
            if key_columns is None:
                pk_cols = connector.get_pk_columns(table_name)
                resolved_keys: list[str] = pk_cols if pk_cols else ["rowid"]
            else:
                resolved_keys = list(key_columns)

            # Step 4: Determine the fetch query.
            # If "rowid" is in resolved_keys but not a real column, we need
            # SELECT rowid, * to include it.  This also handles from_config
            # reconstruction where key_columns=["rowid"] is passed explicitly.
            normal_cols = {ci.name for ci in connector.get_column_info(table_name)}
            if "rowid" in resolved_keys and "rowid" not in normal_cols:
                _query: str | None = f'SELECT rowid, * FROM "{table_name}"'
            else:
                _query = None

            super().__init__(
                connector,
                table_name,
                key_columns=resolved_keys,
                system_key_columns=system_key_columns,
                record_id_column=record_id_column,
                source_id=source_id,
                label=label,
                data_context=data_context,
                config=config,
                _query=_query,
            )
        finally:
            try:
                connector.close()
            except Exception:
                # Suppress connector close errors to avoid masking __init__ failures.
                pass

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        base = super().to_config()
        base.pop("connector", None)
        return {**base, "source_type": "sqlite_table", "db_path": str(self._db_path)}

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> "SQLiteTableSource":
        """Reconstruct a SQLiteTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``SQLiteTableSource`` instance.

        Note:
            ``:memory:`` sources cannot be reconstructed — the new in-memory
            database will be empty and ``ValueError`` will be raised.
        """
        return cls(
            db_path=config["db_path"],
            table_name=config["table_name"],
            key_columns=config.get("key_columns"),
            system_key_columns=config.get("system_key_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
            label=config.get("label"),
            data_context=config.get("data_context"),
        )
