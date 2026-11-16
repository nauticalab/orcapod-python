from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from orcapod.core.referential_tags import TagOnlyColumns, check_tag_only_columns
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.reload import same_rows
from orcapod.core.sources.stream_builder import (
    SourceStreamBuilder,
    SourceStreamResult,
)
from orcapod.databases.storage_utils import delta_to_pyarrow_dataset
from orcapod.types import PathLike
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    import pyarrow as pa
    import deltalake
else:
    pa = LazyModule("pyarrow")
    deltalake = LazyModule("deltalake")


class DeltaTableSource(RootSource):
    """A source backed by a Delta Lake table.

    The table is read once at construction time using ``deltalake``'s
    PyArrow integration. The resulting Arrow table is enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

    At the start of every run (``check_for_new_data``, ITL-1102) the source checks the
    table's Delta version. When the version moved, it reads the table again and, if
    the rows changed (in order without a ``record_id_column``, in any order with one: a
    version that only compacted files or
    changed metadata changes nothing), holds the new rows and moves its modified time.
    Record ids keep their rule: the ``record_id_column`` value, or the row's position.

    Plain tags: ``tag_only_columns`` (keyword-only) is ``True``, the default, and keeps
    every tag column a tag only, not data, as every source does in v0.3. Any other value
    is refused: referential tags arrive in v0.4, where tags become referential by default
    and ``tag_only_columns`` names the tags kept plain.
    """

    def __init__(
        self,
        delta_table_path: PathLike,
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        *,
        tag_only_columns: TagOnlyColumns = True,
        **kwargs: Any,
    ) -> None:
        check_tag_only_columns(tag_only_columns)
        resolved = Path(delta_table_path).resolve()

        if source_id is None:
            source_id = resolved.name
        super().__init__(source_id=source_id, **kwargs)

        self._delta_table_path = resolved

        try:
            self._delta_table = deltalake.DeltaTable(str(self._delta_table_path))
        except deltalake.exceptions.TableNotFoundError:
            raise ValueError(f"Delta table not found at {self._delta_table_path}")

        self._tag_columns: Collection[str] = _normalize_column_list(tag_columns)
        self._system_tag_columns: Collection[str] = system_tag_columns
        self._record_id_column = record_id_column
        self._version: int = self._delta_table.version()
        table = self._read_table()
        # The rows as read, columns and types included: a check compares a new read
        # with them, and refuses a table whose columns changed.
        self._table: pa.Table = table
        result = self._build(table)
        self._stream = result.stream
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def _read_table(self) -> "pa.Table":
        """Read the table at the Delta version ``self._delta_table`` is loaded at."""
        # The Delta log stores field-level nullable flags; to_pyarrow_dataset()
        # preserves them exactly — no inference needed.
        return delta_to_pyarrow_dataset(
            self._delta_table, as_large_types=True
        ).to_table()

    def _build(self, table: "pa.Table") -> SourceStreamResult:
        """Enrich ``table`` as this source's stream (source info, system tags)."""
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        return builder.build(
            table,
            tag_columns=self._tag_columns,
            source_id=self._source_id,
            record_id_column=self._record_id_column,
            system_tag_columns=self._system_tag_columns,
        )

    @override
    def check_for_new_data(self) -> bool:
        """Take in the table's rows if its Delta version moved and its rows changed.

        Only the Delta log is read when the version did not move. A version that moved
        without changing the rows (``OPTIMIZE``, a metadata change) leaves the held rows
        and the modified time as they are. The rows are compared as read from the
        table, every column included (a ``system_tag_columns`` column too), in any
        order (``same_rows``).

        Returns:
            ``True`` if the source now holds new rows.

        Raises:
            ValueError: If the table's columns or their types changed: the source's
                schema is part of its identity; build a new source to read it.
        """
        self._delta_table.update_incremental()
        version = self._delta_table.version()
        if version == self._version:
            return False
        table = self._read_table()
        if not table.schema.equals(self._table.schema, check_metadata=True):
            raise ValueError(
                f"Delta table {str(self._delta_table_path)!r} changed its columns at "
                f"version {version}: the source was built with {self._table.schema}, "
                f"the table now has {table.schema}. Build a new DeltaTableSource to "
                "read it"
            )
        # Row positions are record ids without a record_id_column: order counts.
        if same_rows(self._table, table, ordered=self._record_id_column is None):
            self._version = version
            return False
        stream = self._build(table).stream
        self._version = version
        self._stream = stream
        self._table = table
        self._update_modified_time()
        return True

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict."""
        return {
            "source_type": "delta_table",
            "delta_table_path": str(self._delta_table_path),
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry=None
    ) -> "DeltaTableSource":
        """Reconstruct a DeltaTableSource from a config dict."""
        return cls(
            delta_table_path=config["delta_table_path"],
            tag_columns=config.get("tag_columns", ()),
            tag_only_columns=config.get("tag_only_columns", True),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
        )
