"""DBTableSource — a read-only RootSource backed by any DBConnectorProtocol.

Uses the table's primary-key columns as tag columns by default.
Type mapping (DB-native → Arrow) is fully delegated to the connector.

Example::

    connector = SQLiteConnector(":memory:")   # PLT-1076
    source = DBTableSource(connector, "measurements", source_id="lab.measurements")
    source = DBTableSource(
        connector, "events", tag_columns=["session_id"], source_id="lab.events"
    )
"""

from __future__ import annotations

from collections.abc import Collection, Iterator
from contextlib import contextmanager
from typing import Any, ClassVar, TYPE_CHECKING

from typing_extensions import override

from orcapod.core.referential_tags import TagOnlyColumns, check_tag_only_columns
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.source_ids import location_source_id
from orcapod.core.sources.reload import keep_nullable_columns, same_rows
from orcapod.core.sources.stream_builder import (
    normalize_record_id_columns,
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
        record_id_columns: The key columns whose values identify a row (the record
            id), in key order. ``None`` (default): the table's primary key; a table
            without one is refused. ``()``: the row's position, ``row_<n>``.
        source_id: Canonical source name for the registry and provenance tokens.
            Required here: a generic connector exposes no database address, and the
            table name alone does not say which database the table is in
            (ITL-1154). A subclass that knows its database's address
            (``SQLiteTableSource``, ``PostgreSQLTableSource``) defaults it to
            ``<kind>:<table address>`` (``_table_address``).
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: Orcapod configuration (controls hash character counts, etc.).
        tag_only_columns: Tag columns kept as tags only, not data, exactly as in
            v0.3; ``True`` for all of them. Keyword-only. ``True`` is the default and
            the only value v0.3 accepts: any other value is refused, because
            referential tags arrive in v0.4 (tags then become referential by default
            and ``tag_only_columns`` keeps a tag plain).

    Raises:
        ValueError: If no ``source_id`` is given and the source knows no address of
            its table's database, or the table is not found, has no PK columns and
            none are provided, or is empty.
    """

    SOURCE_KIND: ClassVar[str] = "db_table"
    """Config type key and default source-id kind of this source (one vocabulary)."""

    def __init__(
        self,
        connector: DBConnectorProtocol,
        table_name: str,
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        record_id_columns: str | Collection[str] | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
        *,
        tag_only_columns: TagOnlyColumns = True,
        _query: str | None = None,
    ) -> None:
        check_tag_only_columns(tag_only_columns)
        if source_id is None:
            address = self._table_address(connector, table_name)
            if address is None:
                raise ValueError(
                    f"{type(self).__name__} over table {table_name!r} needs a "
                    "source_id: the connector gives no address of its database, so "
                    "the table name alone does not say which database the table is "
                    "in, and two databases with a table of that name would share one "
                    "source id. Pass source_id='<a name for this table>', or use the "
                    "source of your database (SQLiteTableSource, "
                    "PostgreSQLTableSource), whose default id holds the address."
                )
            source_id = location_source_id(self.SOURCE_KIND, address)
        super().__init__(
            source_id=source_id,
            label=label,
            data_context=data_context,
            config=config,
        )

        self._connector = connector
        self._table_name = table_name
        self._record_id_columns = normalize_record_id_columns(record_id_columns)

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

        # A row is identified by its primary key unless key columns are named, or
        # () asks for the position (ITL-1154). No silent fallback to position.
        if self._record_id_columns is None:
            self._key_columns: tuple[str, ...] = tuple(
                connector.get_pk_columns(table_name)
            )
            if not self._key_columns:
                raise ValueError(
                    f"Table {table_name!r} has no primary key, so its rows have no "
                    "identifier: name key columns with record_id_columns=[...], or "
                    "pass record_id_columns=() to identify rows by position."
                )
        else:
            self._key_columns = self._record_id_columns

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

    def _table_address(
        self, connector: DBConnectorProtocol, table_name: str
    ) -> str | None:
        """Return the table's full address in the database's own convention.

        The location part of the default source id (``<kind>:<address>``). A generic
        connector exposes no database address, so there is none and the source needs
        a ``source_id``; a subclass that knows its database's convention overrides
        this (ITL-1154).

        Args:
            connector: The connector the source reads through.
            table_name: The table's name.

        Returns:
            The table's address, or ``None`` when the source knows none.
        """
        del connector, table_name
        return None

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
            record_id_columns=self._key_columns,
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
        in any order when a ``record_id_columns`` gives the record ids: a table that
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
        # Row positions are record ids without key columns: order counts.
        if same_rows(self._table, table, ordered=not self._key_columns):
            return False
        self._stream = self._build(table).stream
        self._table = table
        self._update_modified_time()
        return True

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        return {
            "source_type": self.SOURCE_KIND,
            "connector": self._connector.to_config(),
            "table_name": self._table_name,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_columns": None
            if self._record_id_columns is None
            else list(self._record_id_columns),
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
