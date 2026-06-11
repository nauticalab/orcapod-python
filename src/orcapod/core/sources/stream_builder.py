"""Compositional builder for enriching raw Arrow tables into source streams.

Extracts the enrichment pipeline that was previously embedded in
``ArrowTableSource.__init__``: dropping system columns, validating tags,
computing schema/table hashes, adding source-info provenance, adding system
tag columns, and wrapping the result in an ``ArrowTableStream``.
"""

from __future__ import annotations

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from typing import TYPE_CHECKING

from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import compute_schema_hash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.config import OrcapodConfig
    from orcapod.contexts import DataContext
else:
    pa = LazyModule("pyarrow")

# Namespace UUID for source record IDs — all source record IDs are derived
# from this namespace and the row's source_id + provenance token.
_SOURCE_RECORD_ID_NAMESPACE = uuid.UUID("7f8e9d0c-1b2a-3c4d-5e6f-7a8b9c0d1e2f")


def _make_provenance_token(record_id_column: str | None, row_index: int, row: dict) -> str:
    """Build the human-readable provenance token for a single row.

    When *record_id_column* is given the token is ``"{column}={value}"``,
    giving a stable, human-readable key that survives row reordering.
    When no column is specified the fallback is ``"row_{index}"``.
    """
    if record_id_column is not None:
        return f"{record_id_column}={row[record_id_column]}"
    return f"row_{row_index}"


def _make_record_id_bytes(source_id: str, provenance_token: str) -> bytes:
    """Build a stable 16-byte record ID from source_id and provenance token.

    Uses UUID v5 (name-based, SHA-1) to produce a deterministic UUID from
    the source identity and row provenance token. The result is stable across
    runs for the same source_id and provenance_token.

    Args:
        source_id: The canonical source identifier.
        provenance_token: Human-readable per-row provenance key
            (e.g. ``"row_0"`` or ``"col=value"``).

    Returns:
        16 raw bytes suitable for ``UUID_ARROW_TYPE`` storage.
    """
    name = f"{source_id}::{provenance_token}"
    return uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, name).bytes


@dataclass(frozen=True)
class SourceStreamResult:
    """Artifacts produced by ``SourceStreamBuilder.build()``."""

    stream: ArrowTableStream
    schema_hash: str
    table_hash: ContentHash
    source_id: str
    tag_columns: tuple[str, ...]
    system_tag_columns: tuple[str, ...]


class SourceStreamBuilder:
    """Builds an enriched ``ArrowTableStream`` from a raw Arrow table.

    Args:
        data_context: Provides type_converter, semantic_hasher, arrow_hasher.
        config: Orcapod config (controls hash character counts).
    """

    def __init__(self, data_context: DataContext, config: OrcapodConfig) -> None:
        self._data_context = data_context
        self._config = config

    def build(
        self,
        table: pa.Table,
        tag_columns: Collection[str],
        source_id: str | None = None,
        record_id_column: str | None = None,
        system_tag_columns: Collection[str] = (),
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
            tag_columns: Column names forming the tag for each row.
            source_id: Canonical source name. Defaults to table hash.
            record_id_column: Column for stable record IDs in provenance.
            system_tag_columns: Additional system-level tag columns.

        Returns:
            SourceStreamResult with enriched stream and metadata.

        Raises:
            ValueError: If tag_columns or record_id_column are not in table.
        """
        tag_columns_tuple = tuple(tag_columns)
        system_tag_columns_tuple = tuple(system_tag_columns)

        # 1. Drop system columns from raw input.
        table = arrow_utils.drop_system_columns(table)

        # 2. Validate tag_columns.
        missing_tags = set(tag_columns_tuple) - set(table.column_names)
        if missing_tags:
            raise ValueError(
                f"tag_columns not found in table: {missing_tags}. "
                f"Available columns: {list(table.column_names)}"
            )

        # 3. Validate record_id_column.
        if record_id_column is not None and record_id_column not in table.column_names:
            raise ValueError(
                f"record_id_column {record_id_column!r} not found in table columns: "
                f"{table.column_names}"
            )

        # 4. Compute schema hash from tag/data python schemas.
        # Nullable flags in the incoming table are trusted as-is — callers must
        # set them correctly before calling build().
        non_sys = arrow_utils.drop_system_columns(table)
        tag_schema = non_sys.select(list(tag_columns_tuple)).schema
        data_schema = non_sys.drop(list(tag_columns_tuple)).schema
        tag_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            tag_schema
        )
        data_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            data_schema
        )
        schema_hash = compute_schema_hash(
            tag_python,
            data_python,
            self._data_context.semantic_hasher,
            self._config.hashing.schema_n_char,
        )

        # 5. Compute table hash for data identity.
        table_hash = self._data_context.arrow_hasher.hash_table(table)

        # 6. Default source_id to table hash.
        if source_id is None:
            source_id = table_hash.to_hex(char_count=self._config.hashing.path_n_char)

        # 7. Build per-row source-info strings and deterministic UUID record IDs.
        rows_as_dicts = table.to_pylist()
        provenance_tokens = [
            _make_provenance_token(record_id_column, i, row)
            for i, row in enumerate(rows_as_dicts)
        ]
        source_info = [
            f"{source_id}{constants.BLOCK_SEPARATOR}{token}"
            for token in provenance_tokens
        ]
        # Record IDs stored in the system tag column are stable UUID v5 bytes —
        # deterministically derived from source_id and per-row provenance token.
        # Same source_id + same row position/content → same record_id bytes.
        record_id_values = [
            _make_record_id_bytes(source_id, token) for token in provenance_tokens
        ]

        # 8. Add source-info provenance columns.
        table = arrow_utils.add_source_info(
            table, source_info, exclude_columns=tag_columns_tuple
        )

        # 9. Add system tag columns.
        table = arrow_utils.add_system_tag_columns(
            table,
            schema_hash,
            source_id,
            record_id_values,
        )

        # 10. Wrap in ArrowTableStream. Nullable flags are already correct —
        # the caller set them before calling build().
        stream = ArrowTableStream(
            table=table,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
        )

        return SourceStreamResult(
            stream=stream,
            schema_hash=schema_hash,
            table_hash=table_hash,
            source_id=source_id,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
        )
