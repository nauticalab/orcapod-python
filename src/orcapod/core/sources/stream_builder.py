"""Compositional builder for enriching raw Arrow tables into source streams.

Extracts the enrichment pipeline that was previously embedded in
``ArrowTableSource.__init__``: dropping system columns, validating keys,
computing schema/table hashes, adding source-info provenance, adding system
key columns, and wrapping the result in an ``ArrowTableStream``.
"""

from __future__ import annotations

from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING

from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.config import Config
    from orcapod.contexts import DataContext
else:
    pa = LazyModule("pyarrow")


def _make_record_id(record_id_column: str | None, row_index: int, row: dict) -> str:
    """Build the record-ID token for a single row.

    When *record_id_column* is given the token is ``"{column}={value}"``,
    giving a stable, human-readable key that survives row reordering.
    When no column is specified the fallback is ``"row_{index}"``.
    """
    if record_id_column is not None:
        return f"{record_id_column}={row[record_id_column]}"
    return f"row_{row_index}"


@dataclass(frozen=True)
class SourceStreamResult:
    """Artifacts produced by ``SourceStreamBuilder.build()``."""

    stream: ArrowTableStream
    schema_hash: str
    table_hash: ContentHash
    source_id: str
    key_columns: tuple[str, ...]
    system_key_columns: tuple[str, ...]


class SourceStreamBuilder:
    """Builds an enriched ``ArrowTableStream`` from a raw Arrow table.

    Args:
        data_context: Provides type_converter, semantic_hasher, arrow_hasher.
        config: Orcapod config (controls hash character counts).
    """

    def __init__(self, data_context: DataContext, config: Config) -> None:
        self._data_context = data_context
        self._config = config

    def build(
        self,
        table: pa.Table,
        key_columns: Collection[str],
        source_id: str | None = None,
        record_id_column: str | None = None,
        system_key_columns: Collection[str] = (),
    ) -> SourceStreamResult:
        """Run the full enrichment pipeline.

        The builder trusts the incoming table's nullable flags as-is.
        Callers are responsible for setting nullable correctly before calling
        this method — use ``arrow_utils.infer_schema_nullable(table)`` when
        receiving a raw Arrow table whose schema has not been set deliberately
        (e.g. from Polars or plain ``pa.table({})``, which default all fields
        to ``nullable=True`` regardless of content).

        Args:
            table: Arrow table with nullable flags already set correctly.
            key_columns: Column names forming the key for each row.
            source_id: Canonical source name. Defaults to table hash.
            record_id_column: Column for stable record IDs in provenance.
            system_key_columns: Additional system-level key columns.

        Returns:
            SourceStreamResult with enriched stream and metadata.

        Raises:
            ValueError: If key_columns or record_id_column are not in table.
        """
        key_columns_tuple = tuple(key_columns)
        system_key_columns_tuple = tuple(system_key_columns)

        # 1. Drop system columns from raw input.
        table = arrow_utils.drop_system_columns(table)

        # 2. Validate key_columns.
        missing_keys = set(key_columns_tuple) - set(table.column_names)
        if missing_keys:
            raise ValueError(
                f"key_columns not found in table: {missing_keys}. "
                f"Available columns: {list(table.column_names)}"
            )

        # 3. Validate record_id_column.
        if record_id_column is not None and record_id_column not in table.column_names:
            raise ValueError(
                f"record_id_column {record_id_column!r} not found in table columns: "
                f"{table.column_names}"
            )

        # 4. Compute schema hash from key/data python schemas.
        # Nullable flags in the incoming table are trusted as-is — callers must
        # set them correctly before calling build().
        non_sys = arrow_utils.drop_system_columns(table)
        key_schema = non_sys.select(list(key_columns_tuple)).schema
        data_schema = non_sys.drop(list(key_columns_tuple)).schema
        key_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            key_schema
        )
        data_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            data_schema
        )
        schema_hash = self._data_context.semantic_hasher.hash_object(
            (key_python, data_python)
        ).to_hex(char_count=self._config.schema_hash_n_char)

        # 5. Compute table hash for data identity.
        table_hash = self._data_context.arrow_hasher.hash_table(table)

        # 6. Default source_id to table hash.
        if source_id is None:
            source_id = table_hash.to_hex(char_count=self._config.path_hash_n_char)

        # 7. Build per-row source-info strings.
        rows_as_dicts = table.to_pylist()
        source_info = [
            f"{source_id}{constants.BLOCK_SEPARATOR}"
            f"{_make_record_id(record_id_column, i, row)}"
            for i, row in enumerate(rows_as_dicts)
        ]

        # 8. Add source-info provenance columns.
        table = arrow_utils.add_source_info(
            table, source_info, exclude_columns=key_columns_tuple
        )

        # 9. Add system key columns.
        record_id_values = [
            _make_record_id(record_id_column, i, row)
            for i, row in enumerate(rows_as_dicts)
        ]
        table = arrow_utils.add_system_key_columns(
            table,
            schema_hash,
            source_id,
            record_id_values,
        )

        # 10. Wrap in ArrowTableStream. Nullable flags are already correct —
        # the caller set them before calling build().
        stream = ArrowTableStream(
            table=table,
            key_columns=key_columns_tuple,
            system_key_columns=system_key_columns_tuple,
        )

        return SourceStreamResult(
            stream=stream,
            schema_hash=schema_hash,
            table_hash=table_hash,
            source_id=source_id,
            key_columns=key_columns_tuple,
            system_key_columns=system_key_columns_tuple,
        )
