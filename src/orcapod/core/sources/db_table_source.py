"""DBTableSource — a read-only RootSource backed by any DBConnectorProtocol.

Uses the table's primary-key columns as tag columns by default.
Type mapping (DB-native → Arrow) is fully delegated to the connector.

Example::

    connector = SQLiteConnector(":memory:")   # PLT-1076
    source = DBTableSource(connector, "measurements")          # PKs → tags
    source = DBTableSource(connector, "events", tag_columns=["session_id"])
"""
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod import contexts
    from orcapod.config import Config
    from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
else:
    pa = LazyModule("pyarrow")


class DBTableSource(RootSource):
    """A read-only Source backed by a table in any DBConnectorProtocol database.

    At construction time the source:
    1. Validates the table exists in the connector.
    2. Resolves tag columns (defaults to the table's primary-key columns).
    3. Fetches all rows as Arrow batches and assembles a PyArrow table.
    4. Enriches via ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

    Args:
        connector: A ``DBConnectorProtocol`` providing DB access.
        table_name: Name of the table to expose as a source.
        tag_columns: Columns to use as tag columns.  If ``None`` (default),
            the table's primary-key columns are used.  Raises ``ValueError``
            if the table has no primary key and no explicit columns are given.
        system_tag_columns: Additional system-level tag columns (passed through
            to ``SourceStreamBuilder``; mirrors ``DeltaTableSource`` API).
        record_id_column: Column for stable per-row record IDs in provenance
            strings.  If ``None``, row indices are used.
        source_id: Canonical source name for the registry and provenance tokens.
            Defaults to ``table_name``.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: Orcapod configuration (controls hash character counts, etc.).

    Raises:
        ValueError: If the table is not found, has no PK columns and none are
            provided, or is empty.
    """

    def __init__(
        self,
        connector: DBConnectorProtocol,
        table_name: str,
        tag_columns: Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
        *,
        _query: str | None = None,
    ) -> None:
        if source_id is None:
            source_id = table_name
        super().__init__(
            source_id=source_id,
            label=label,
            data_context=data_context,
            config=config,
        )

        self._connector = connector
        self._table_name = table_name
        self._record_id_column = record_id_column

        # Step 1: Validate the table exists first so the error is always
        # "not found" rather than a misleading "no primary key" for missing tables.
        if table_name not in connector.get_table_names():
            raise ValueError(f"Table {table_name!r} not found in database.")

        # Step 2: Resolve tag columns — default to PK columns
        if tag_columns is None:
            resolved_tag_columns: list[str] = connector.get_pk_columns(table_name)
            if not resolved_tag_columns:
                raise ValueError(
                    f"Table {table_name!r} has no primary key columns. "
                    "Provide explicit tag_columns."
                )
        else:
            resolved_tag_columns = list(tag_columns)

        # Step 3: Fetch the full table as Arrow.
        # _query allows subclasses (e.g. SQLiteTableSource) to inject a custom
        # SELECT (e.g. SELECT rowid, * FROM "t") without duplicating the rest of init.
        query = _query if _query is not None else f'SELECT * FROM "{table_name}"'
        batches = list(connector.iter_batches(query))
        if not batches:
            raise ValueError(f"Table {table_name!r} is empty.")
        table: pa.Table = pa.Table.from_batches(batches)

        # Step 4: Enrich via SourceStreamBuilder (same pipeline as all other RootSources)
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=resolved_tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        return {
            "source_type": "db_table",
            "connector": self._connector.to_config(),
            "table_name": self._table_name,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> DBTableSource:
        """Not yet implemented — requires a connector factory registry.

        Raises:
            NotImplementedError: Always, until connector implementations add
                a registry helper (``build_db_connector_from_config``) in
                PLT-1074/1075/1076.
        """
        raise NotImplementedError(
            "DBTableSource.from_config requires a registered connector factory. "
            "Implement build_db_connector_from_config in PLT-1074/1075/1076."
        )
