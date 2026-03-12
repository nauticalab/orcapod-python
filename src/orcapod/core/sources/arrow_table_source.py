from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any, Self

from orcapod.core.sources.base import RootSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import FieldNotResolvableError
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def _make_record_id(record_id_column: str | None, row_index: int, row: dict) -> str:
    """
    Build the record-ID token for a single row.

    When *record_id_column* is given the token is ``"{column}={value}"``,
    giving a stable, human-readable key that survives row reordering.
    When no column is specified the fallback is ``"row_{index}"``.
    """
    if record_id_column is not None:
        return f"{record_id_column}={row[record_id_column]}"
    return f"row_{row_index}"


class ArrowTableSource(RootSource):
    """
    A source backed by an in-memory PyArrow Table.

    Strips system columns from the input table, adds per-row source-info
    provenance columns and a system tag column encoding the schema hash, then
    wraps the result in a ``ArrowTableStream``.  Because the table is immutable the
    same ``ArrowTableStream`` is returned from every ``process()`` call.

    Parameters
    ----------
    table:
        The PyArrow table to expose as a stream.
    tag_columns:
        Column names whose values form the tag for each row.
    system_tag_columns:
        Additional system-level tag columns.
    record_id_column:
        Column whose values serve as stable record identifiers in provenance
        strings and ``resolve_field`` lookups.  When ``None`` (default) the
        positional row index (``row_0``, ``row_1``, …) is used instead.
    source_id:
        Canonical registry name for this source (passed to ``RootSource``).
    auto_register:
        Whether to auto-register with the source registry on construction.
    """

    def __init__(
        self,
        table: pa.Table,
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # Drop system columns from the raw input.
        table = arrow_data_utils.drop_system_columns(table)

        missing_tags = set(tag_columns) - set(table.column_names)
        if missing_tags:
            raise ValueError(
                f"tag_columns not found in table: {missing_tags}. "
                f"Available columns: {list(table.column_names)}"
            )
        self._tag_columns = tuple(tag_columns)
        self._system_tag_columns = tuple(system_tag_columns)

        # Validate record_id_column early.
        if record_id_column is not None and record_id_column not in table.column_names:
            raise ValueError(
                f"record_id_column {record_id_column!r} not found in table columns: "
                f"{table.column_names}"
            )
        self._record_id_column = record_id_column

        # Derive a schema hash from the tag/packet python schemas.
        non_sys = arrow_data_utils.drop_system_columns(table)
        tag_schema = non_sys.select(self._tag_columns).schema
        packet_schema = non_sys.drop(list(self._tag_columns)).schema
        tag_python = self.data_context.type_converter.arrow_schema_to_python_schema(
            tag_schema
        )
        packet_python = self.data_context.type_converter.arrow_schema_to_python_schema(
            packet_schema
        )
        self._schema_hash = self.data_context.semantic_hasher.hash_object(
            (tag_python, packet_python)
        ).to_hex(char_count=self.orcapod_config.schema_hash_n_char)

        # Derive a stable table hash for data identity.
        self._table_hash = self.data_context.arrow_hasher.hash_table(table)

        # Default source_id to table hash when not explicitly provided.
        if self._source_id is None:
            self._source_id = self._table_hash.to_hex(
                char_count=self.orcapod_config.path_hash_n_char
            )

        # Keep a clean copy for resolve_field lookups (no system columns).
        self._data_table = table

        # Build per-row source-info strings using stable record IDs.
        rows_as_dicts = table.to_pylist()
        source_info = [
            f"{self.source_id}{constants.BLOCK_SEPARATOR}"
            f"{_make_record_id(record_id_column, i, row)}"
            for i, row in enumerate(rows_as_dicts)
        ]

        table = arrow_data_utils.add_source_info(
            table, source_info, exclude_columns=self._tag_columns
        )

        # System tags: paired source_id and record_id columns
        record_id_values = [
            _make_record_id(record_id_column, i, row)
            for i, row in enumerate(rows_as_dicts)
        ]
        table = arrow_data_utils.add_system_tag_columns(
            table,
            self._schema_hash,
            self.source_id,
            record_id_values,
        )

        self._table = table
        self._stream = ArrowTableStream(
            table=self._table,
            tag_columns=self._tag_columns,
            system_tag_columns=self._system_tag_columns,
        )

    # -------------------------------------------------------------------------
    # Field resolution
    # -------------------------------------------------------------------------

    def resolve_field(self, record_id: str, field_name: str) -> Any:
        """
        Return the value of *field_name* for the row identified by *record_id*.

        *record_id* is the token embedded in provenance strings:
        - ``"row_3"`` — positional index (when no ``record_id_column`` was set)
        - ``"user_id=abc123"`` — column/value pair (when ``record_id_column``
          was set)

        Raises
        ------
        FieldNotResolvableError
            When the record or field cannot be found.
        """
        if field_name not in self._data_table.column_names:
            raise FieldNotResolvableError(
                f"Field {field_name!r} not found in source {self.source_id!r}. "
                f"Available columns: {self._data_table.column_names}"
            )

        if self._record_id_column is not None:
            # record_id format: "{column}={value}"
            expected_prefix = f"{self._record_id_column}="
            if not record_id.startswith(expected_prefix):
                raise FieldNotResolvableError(
                    f"record_id {record_id!r} does not match expected format "
                    f"'{expected_prefix}<value>' for source {self.source_id!r}."
                )
            value_str = record_id[len(expected_prefix) :]
            matches = [
                i
                for i, v in enumerate(
                    self._data_table.column(self._record_id_column).to_pylist()
                )
                if str(v) == value_str
            ]
            if not matches:
                raise FieldNotResolvableError(
                    f"No row with {self._record_id_column}={value_str!r} found in "
                    f"source {self.source_id!r}."
                )
            row_index = matches[0]
        else:
            # record_id format: "row_{index}"
            if not record_id.startswith("row_"):
                raise FieldNotResolvableError(
                    f"record_id {record_id!r} does not match expected format "
                    f"'row_<index>' for source {self.source_id!r}."
                )
            try:
                row_index = int(record_id[4:])
            except ValueError:
                raise FieldNotResolvableError(
                    f"Cannot parse row index from record_id {record_id!r}."
                )
            if row_index < 0 or row_index >= self._data_table.num_rows:
                raise FieldNotResolvableError(
                    f"Row index {row_index} is out of range for source "
                    f"{self.source_id!r} ({self._data_table.num_rows} rows)."
                )

        return self._data_table.column(field_name)[row_index].as_py()

    # -------------------------------------------------------------------------
    # RootSource protocol
    # -------------------------------------------------------------------------

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (in-memory table is not serializable).

        Returns:
            Dict with source metadata. Cannot be used to reconstruct the source
            since the original Arrow table data is not preserved.
        """
        return {
            "source_type": "arrow_table",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Not supported — ArrowTableSource cannot be reconstructed from config.

        Args:
            config: Config dict (ignored).

        Raises:
            NotImplementedError: Always, because the in-memory Arrow table
                cannot be recovered from config.
        """
        raise NotImplementedError(
            "ArrowTableSource cannot be reconstructed from config — "
            "the in-memory Arrow table is not serializable."
        )

    @property
    def table(self) -> pa.Table:
        return self._table

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.output_schema(), self.source_id)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._stream.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        return self._stream.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._stream.as_table(columns=columns, all_info=all_info)
