"""DBTableSource — a read-only RootSource backed by any DBConnectorProtocol.

Uses the table's primary-key columns as tag columns by default.
Type mapping (DB-native → Arrow) is fully delegated to the connector.

Example::

    connector = SQLiteConnector(":memory:")   # PLT-1076
    source = DBTableSource(connector, "measurements")          # PKs → tags
    source = DBTableSource(connector, "events", tag_columns=["session_id"])
"""

from __future__ import annotations

from collections.abc import Collection, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.reload import keep_nullable_columns, same_rows
from orcapod.core.sources.stream_builder import (
    SourceStreamBuilder,
    SourceStreamResult,
)
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod import contexts
    from orcapod.config import OrcapodConfig
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
        tag_columns: Columns to use as tag columns.  A bare string is
            accepted as a single column name.  If ``None`` (default),
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
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
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
            resolved_tag_columns = _normalize_column_list(tag_columns)

        # Step 3: Fetch the full table as Arrow.
        # _query allows subclasses (e.g. SQLiteTableSource) to inject a custom
        # SELECT (e.g. SELECT rowid, * FROM "t") without duplicating the rest of init.
        self._query = _query if _query is not None else f'SELECT * FROM "{table_name}"'
        table = self._read_table(connector)
        # The rows as read, columns and types included: a check compares a new read
        # with them, and refuses a table whose columns changed.
        self._table: pa.Table = table

        # Step 4: Enrich via SourceStreamBuilder (same pipeline as all other RootSources)
        self._tag_columns: Collection[str] = resolved_tag_columns
        self._system_tag_columns: Collection[str] = system_tag_columns
        result = self._build(table)
        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def _read_table(
        self, connector: DBConnectorProtocol, held: pa.Schema | None = None
    ) -> pa.Table:
        """Read every row of the table through ``connector``, with its nullability.

        Args:
            connector: The connector to read through.
            held: The schema of the table the source holds, when the read is a check's.
                Where nullability is inferred from the rows (the connector gives no
                column information), a column ``held`` has nullable stays nullable
                (``keep_nullable_columns``).

        Raises:
            ValueError: If the table is not found or is empty.
        """
        if self._table_name not in connector.get_table_names():
            raise ValueError(f"Table {self._table_name!r} not found in database.")
        batches = list(connector.iter_batches(self._query))
        if not batches:
            raise ValueError(f"Table {self._table_name!r} is empty.")
        table: pa.Table = pa.Table.from_batches(batches)
        # Apply database-defined nullability: the connector's get_column_info
        # returns the authoritative nullable flag per column from the DB schema.
        col_infos = connector.get_column_info(self._table_name)
        if col_infos:
            db_nullable = {ci.name: ci.nullable for ci in col_infos}
            db_schema = pa.schema(
                [
                    pa.field(f.name, f.type, nullable=db_nullable.get(f.name, True))
                    for f in table.schema
                ]
            )
            return pa.Table.from_arrays(
                [table.column(i) for i in range(table.num_columns)],
                schema=db_schema,
            )
        # No column info available — fall back to inferring nullable flags
        # from actual data so columns without nulls are not spuriously marked
        # nullable=True (Arrow's default).
        inferred = table.cast(arrow_utils.infer_schema_nullable(table))
        return inferred if held is None else keep_nullable_columns(inferred, held)

    def _build(self, table: pa.Table) -> SourceStreamResult:
        """Enrich ``table`` as this source's stream (source info, system tags)."""
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        return builder.build(
            table,
            tag_columns=self._tag_columns,
            source_id=self._source_id,
            record_id_column=self._record_id_column,
            system_tag_columns=self._system_tag_columns,
        )

    @contextmanager
    def _open_connector(self) -> Iterator[DBConnectorProtocol]:
        """Give the connector a check reads through: the one the source was given.

        A subclass that closed its connector after construction opens a new one.
        """
        yield self._connector

    @override
    def check_for_new_data(self) -> bool:
        """Read the table again and take in its rows if they changed (ITL-1102).

        A database table has no cheap exact sign of a change, so every check reads the
        whole table, as building the source did, and compares it with the rows held,
        in any order when a ``record_id_column`` gives the record ids: a table that
        returns the same rows in another order (an
        unordered ``SELECT``) is unchanged. The rows are compared as read, every column
        included (a ``system_tag_columns`` column too; ``same_rows``).

        Returns:
            ``True`` if the source now holds new rows.

        Raises:
            ValueError: If the table is gone or empty, or its columns or their types
                changed: the source's schema is part of its identity; build a new
                source to read it.
        """
        with self._open_connector() as connector:
            table = self._read_table(connector, held=self._table.schema)
        if not table.schema.equals(self._table.schema, check_metadata=True):
            raise ValueError(
                f"Table {self._table_name!r} changed its columns: the source was "
                f"built with {self._table.schema}, the table now has {table.schema}. "
                "Build a new source to read it"
            )
        # Row positions are record ids without a record_id_column: order counts.
        if same_rows(self._table, table, ordered=self._record_id_column is None):
            return False
        self._stream = self._build(table).stream
        self._table = table
        self._update_modified_time()
        return True

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
