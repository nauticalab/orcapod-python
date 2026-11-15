from __future__ import annotations

import os
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.reload import keep_nullable_columns, same_rows
from orcapod.core.sources.stream_builder import (
    SourceStreamBuilder,
    SourceStreamResult,
)
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import _normalize_column_list
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def _file_state(path: str) -> tuple[int, int]:
    """Return a file's modification time (nanoseconds) and size: what tells a change."""
    stat = os.stat(path)
    return (stat.st_mtime_ns, stat.st_size)


class CSVSource(RootSource):
    """A source backed by a CSV file.

    The file is read once at construction time using PyArrow's CSV reader,
    converted to an Arrow table, and enriched by ``SourceStreamBuilder``
    (source-info, schema-hash, system tags).

    At the start of every run (``check_for_new_data``, ITL-1102) the source looks at
    the file's modification time and size. When either changed, it reads the file
    again and, if the rows changed (in order without a ``record_id_column``: row
    positions are record ids; in any order with one), holds the new rows and
    moves its modified time. Record ids keep their rule: the ``record_id_column``
    value, or the row's position.
    """

    def __init__(
        self,
        file_path: str,
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        schema: pa.Schema | None = None,
        **kwargs: Any,
    ) -> None:
        if source_id is None:
            source_id = file_path
        super().__init__(source_id=source_id, **kwargs)

        self._file_path = file_path
        # Every later read (a check, ITL-1102) opens the file found now, whatever the
        # working directory is then.
        self._access_path = os.path.abspath(file_path)
        self._schema = schema
        self._tag_columns: Collection[str] = _normalize_column_list(tag_columns)
        self._system_tag_columns: Collection[str] = system_tag_columns
        self._record_id_column = record_id_column
        self._file_state = _file_state(self._access_path)
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

    def _read_table(self) -> pa.Table:
        """Read the file into an Arrow table, typed as at construction."""
        import pyarrow.csv as pa_csv

        table: pa.Table = pa_csv.read_csv(self._access_path)
        if self._schema is not None:
            return table.cast(self._schema)
        return table.cast(arrow_utils.infer_schema_nullable(table))

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

    @override
    def check_for_new_data(self) -> bool:
        """Take in the file's rows if the file changed and its rows changed.

        Only the file's modification time and size are read when neither changed. A
        file written again with the same rows (in another order too, given a
        ``record_id_column``) leaves the held rows and
        the modified time as they are. The rows are compared as read from the file,
        every column included (a ``system_tag_columns`` column too), in order unless a
        ``record_id_column`` gives the record ids
        (``same_rows``).

        Returns:
            ``True`` if the source now holds new rows.

        Raises:
            ValueError: If the file's columns or their types changed: the source's
                schema is part of its identity; build a new source to read it.
        """
        state = _file_state(self._access_path)
        if state == self._file_state:
            return False
        table = self._read_table()
        if self._schema is None:
            # Nullability is inferred from the rows: a column that lost its nulls is
            # still the nullable column the source was built with.
            table = keep_nullable_columns(table, self._table.schema)
        if not table.schema.equals(self._table.schema, check_metadata=True):
            raise ValueError(
                f"CSV file {self._file_path!r} changed its columns: the source was "
                f"built with {self._table.schema}, the file now has {table.schema}. "
                "Build a new CSVSource to read it"
            )
        # Row positions are record ids without a record_id_column: order counts.
        if same_rows(self._table, table, ordered=self._record_id_column is None):
            self._file_state = state
            return False
        stream = self._build(table).stream
        self._file_state = state
        self._stream = stream
        self._table = table
        self._update_modified_time()
        return True

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict."""
        return {
            "source_type": "csv",
            "file_path": self._file_path,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> CSVSource:
        """Reconstruct a CSVSource from a config dict."""
        return cls(
            file_path=config["file_path"],
            tag_columns=config.get("tag_columns", ()),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
        )
