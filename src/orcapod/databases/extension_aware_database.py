"""ExtensionAwareDatabase — ArrowDatabaseProtocol wrapper that handles extension type registration.

Wraps any ``ArrowDatabaseProtocol`` backend and transparently applies the
register → cast pattern on every read result:

1. Call ``register_discovered_extensions(converter, table.schema)`` to ensure
   all Arrow extension types found in the returned table's field metadata are
   registered with the converter.
2. Call ``converter.apply_extension_types(table)`` to re-wrap columns that
   were loaded as plain storage types into their correct extension types.
   This operation is zero-copy (``pa.ExtensionArray.from_storage`` per chunk).

Write operations pass through to the underlying database unchanged.

Example::

    db = DeltaTableDatabase("/path/to/store")
    ext_db = ExtensionAwareDatabase(db, converter=type_converter)
    table = ext_db.get_all_records(("results", "my_fn"))
    # table columns have proper extension types applied
"""
from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.database_hooks import register_discovered_extensions
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.extension_types.protocols import TypeConverterProtocol


class ExtensionAwareDatabase:
    """``ArrowDatabaseProtocol`` wrapper that auto-registers and applies extension types.

    All read methods delegate to the wrapped *db*, then:

    1. Walk the returned table's schema to find any extension types (from
       preserved ``ARROW:extension:*`` field metadata).
    2. Register any newly discovered types with *converter* via
       ``register_discovered_extensions``.
    3. Re-wrap columns that were loaded as plain storage types into their
       correct Arrow extension types via ``converter.apply_extension_types``
       (zero-copy).

    Write methods and ``flush`` delegate directly without modification.

    Args:
        db: Any ``ArrowDatabaseProtocol`` backend.
        converter: The ``TypeConverterProtocol`` to use for registration and
            lookup.
    """

    def __init__(
        self,
        db: ArrowDatabaseProtocol,
        converter: TypeConverterProtocol,
    ) -> None:
        self._db = db
        self._converter = converter

    # ── Internal helper ───────────────────────────────────────────────────────

    def _process(self, table: pa.Table | None) -> pa.Table | None:
        """Register extension types and re-wrap columns, or return None unchanged."""
        if table is None:
            return None
        register_discovered_extensions(self._converter, table.schema)
        return self._converter.apply_extension_types(table)

    # ── Read methods ──────────────────────────────────────────────────────────

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: bytes,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        return self._process(
            self._db.get_record_by_id(
                record_path,
                record_id,
                record_id_column=record_id_column,
                flush=flush,
            )
        )

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> pa.Table | None:
        return self._process(
            self._db.get_all_records(record_path, record_id_column=record_id_column)
        )

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[bytes],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        return self._process(
            self._db.get_records_by_ids(
                record_path,
                record_ids,
                record_id_column=record_id_column,
                flush=flush,
            )
        )

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        return self._process(
            self._db.get_records_with_column_value(
                record_path,
                column_values,
                record_id_column=record_id_column,
                flush=flush,
            )
        )

    # ── Write methods (pass-through) ──────────────────────────────────────────

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: bytes,
        record: pa.Table,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        self._db.add_record(
            record_path,
            record_id,
            record,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        self._db.add_records(
            record_path,
            records,
            record_id_column=record_id_column,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def flush(self) -> None:
        self._db.flush()

    # ── Structural delegation ─────────────────────────────────────────────────

    @property
    def base_path(self) -> tuple[str, ...]:
        return self._db.base_path

    def at(self, *path_components: str) -> ExtensionAwareDatabase:
        """Return a scoped view, preserving the extension-aware wrapper."""
        return ExtensionAwareDatabase(
            self._db.at(*path_components),
            converter=self._converter,
        )

    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a table exists at ``record_path``.

        Delegates directly to the wrapped database without any extension-type
        processing — this is a pure existence check, not a data read.

        Args:
            record_path: Path components identifying the table, relative to
                ``self.base_path``.

        Returns:
            ``True`` if a table exists at the given path, ``False`` otherwise.
        """
        return self._db.table_exists(record_path)

    def to_config(self) -> dict[str, Any]:
        return self._db.to_config()
