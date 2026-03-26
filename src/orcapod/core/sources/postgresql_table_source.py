"""PostgreSQLTableSource — a read-only RootSource backed by a PostgreSQL table.

Wraps a PostgreSQL table as an OrcaPod Source. Primary-key columns are used
as tag columns by default.

Example::

    source = PostgreSQLTableSource(
        "postgresql://user:pass@localhost:5432/mydb", "measurements"
    )
"""
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.db_table_source import DBTableSource
from orcapod.databases.postgresql_connector import PostgreSQLConnector

if TYPE_CHECKING:
    from orcapod import contexts
    from orcapod.config import Config


class PostgreSQLTableSource(DBTableSource):
    """A read-only Source backed by a table in a PostgreSQL database.

    At construction time the source:
    1. Stores the DSN for serialisation.
    2. Opens a ``PostgreSQLConnector`` for *dsn*.
    3. Delegates to ``DBTableSource.__init__``, which validates the table,
       resolves tag columns (defaults to PK columns), fetches all rows as
       Arrow batches, and builds the stream.
    4. Closes the connector — all data is eagerly loaded into memory, so the
       connection is released immediately.

    PostgreSQL PK columns are always ``NOT NULL``, so NULL tag values can
    only arise when *tag_columns* is overridden to point at a nullable
    column. Such NULLs are passed through as-is (Arrow supports nulls).

    Args:
        dsn: libpq connection string.
            URI form: ``"postgresql://user:pass@host:5432/dbname"``
            Keyword form: ``"host=localhost dbname=mydb user=alice"``
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns. If ``None`` (default),
            the table's primary-key columns are used. Raises ``ValueError``
            if the table has no primary key and no explicit columns are given.
        system_tag_columns: Additional system-level tag columns.
        record_id_column: Column for stable per-row record IDs in provenance.
        source_id: Canonical source name. Defaults to *table_name*.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.

    Raises:
        ValueError: If the table is not found, is empty, or has no PK and
            no *tag_columns* are given.
        psycopg.OperationalError: If the DSN is invalid or connection fails.
    """

    def __init__(
        self,
        dsn: str,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        self._dsn = dsn  # store before try — needed by to_config even if super() raises
        connector = PostgreSQLConnector(dsn)  # outside try — if this raises, finally never runs
        try:
            super().__init__(
                connector,
                table_name,
                tag_columns=tag_columns,
                system_tag_columns=system_tag_columns,
                record_id_column=record_id_column,
                source_id=source_id,
                label=label,
                data_context=data_context,
                config=config,
            )
        finally:
            try:
                connector.close()
            except Exception:
                pass  # suppress close errors; don't mask original __init__ failure

    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        # super().to_config() calls self._connector.to_config(), which reads only
        # self._connector._dsn (a stored string) — safe even though the connector
        # was closed after construction.
        base = super().to_config()
        base.pop("connector", None)
        return {**base, "source_type": "postgresql_table", "dsn": self._dsn}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PostgreSQLTableSource:
        """Reconstruct a PostgreSQLTableSource from a config dict.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``PostgreSQLTableSource`` instance.
        """
        return cls(
            dsn=config["dsn"],
            table_name=config["table_name"],
            tag_columns=config.get("tag_columns"),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
            label=config.get("label"),
            data_context=config.get("data_context"),
        )
