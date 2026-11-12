"""Compositional builder for enriching raw Arrow tables into source streams.

Extracts the enrichment pipeline that was previously embedded in
``ArrowTableSource.__init__``: dropping system columns, validating tags,
computing schema/table hashes, adding source-info provenance, adding system
tag columns, and wrapping the result in an ``ArrowTableStream``.
"""

from __future__ import annotations

import uuid
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from orcapod.core.sources.source_ids import hash_source_id
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import compute_source_schema_hash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.config import OrcapodConfig
    from orcapod.contexts import DataContext
    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
else:
    pa = LazyModule("pyarrow")

# Namespace UUID for Orcapod source record IDs.
# Derived as UUID v5 of NAMESPACE_URL + a stable Orcapod-specific URL so the
# value is principled rather than an opaque hex literal.  This value is fixed for
# the lifetime of the project — changing it would invalidate all existing stored
# record IDs.
# Computed value: uuid.UUID('877ec89b-6645-5852-ba37-a94604043f5e')
_SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL,
    "https://orcapod.org/namespaces/source-record-id",
)


def _make_provenance_token(
    record_id_column: str | None, row_index: int, value: object
) -> str:
    """Build the human-readable provenance token for a single row.

    When *record_id_column* is given the token is ``"{column}={value}"``,
    giving a stable, human-readable key that survives row reordering.
    When no column is specified the fallback is ``"row_{index}"``.

    The fallback is positional: the token names a row position, not a row.
    If the same rows are supplied in a different order under the same
    ``source_id``, a moved row gets a different record ID. Downstream record
    identities (``FunctionJobNode`` pipeline entries, ``SideEffectPod``
    completion records) are derived from the system tags plus the row's data
    hash, so the moved row is treated as a new entry: it is recorded again,
    giving two entries with identical tag values but distinct record IDs, and
    a ``SideEffectPod`` delivers it again. Because every identity includes the
    row's own data hash, no result is ever paired with the wrong row. Pass
    ``record_id_column`` (with a stable ``source_id``) whenever row order is
    not guaranteed to be identical between runs.
    """
    if record_id_column is not None:
        return f"{record_id_column}={value}"
    return f"row_{row_index}"


def _unhashable_columns(
    hasher: ArrowHasherProtocol, table: pa.Table
) -> list[tuple[str, pa.DataType]]:
    """Return the columns of ``table`` that ``hasher`` cannot hash, with their types.

    Each column is hashed on its own. A zero-row slice is not enough, because the hasher
    rejects a type only when it visits a value, so the whole column is probed. This runs
    only after hashing the whole table has failed.

    Args:
        hasher: The data context's Arrow hasher.
        table: The table whose hash failed.

    Returns:
        ``(column name, Arrow type)`` for every column whose hash raises
        ``ValueError``, in column order.
    """
    failing: list[tuple[str, pa.DataType]] = []
    for name in table.column_names:
        try:
            hasher.hash_table(table.select([name]))
        except ValueError:
            failing.append((name, table.schema.field(name).type))
    return failing


def _make_record_id(source_id: str, provenance_token: str) -> uuid.UUID:
    """Build a stable record ID UUID from source_id and provenance token.

    Uses UUID v5 (name-based, SHA-1) to produce a deterministic UUID from
    the source identity and row provenance token. The result is stable across
    runs for the same source_id and provenance_token.

    Callers that need raw bytes for Arrow storage (``pa.binary(16)``)
    should call ``.bytes`` on the returned UUID.

    Args:
        source_id: The canonical source identifier.
        provenance_token: Human-readable per-row provenance key
            (e.g. ``"row_0"`` or ``"col=value"``).

    Returns:
        A deterministic ``uuid.UUID`` identifying this (source, row) pair.
    """
    name = f"{source_id}::{provenance_token}"
    return uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, name)


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
        source_kind: str | None = None,
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
            source_id: Canonical source name. When not given, the default is
                ``<source_kind>:<algorithm label>:<hex>`` of the table hash
                (``hash_source_id``), which changes when rows are reordered;
                pass an explicit value when record IDs must survive reordering.
            record_id_column: Column whose value identifies a row, giving
                record IDs that follow the row regardless of its position
                (given a stable ``source_id``). When ``None`` the record ID is
                derived from the row's position (``row_{index}``), so it is
                stable only while row order is.
            system_tag_columns: Additional system-level tag columns.
            source_kind: Kind prefix of the default source id, for example
                ``dict``. Required when ``source_id`` is not given; not used
                otherwise.

        Returns:
            SourceStreamResult with enriched stream and metadata.

        Raises:
            ValueError: If the table has no columns ("Table is empty"), if
                tag_columns or record_id_column are not in the table, or if
                neither ``source_id`` nor ``source_kind`` is given.
        """
        tag_columns_tuple = tuple(tag_columns)
        system_tag_columns_tuple = tuple(system_tag_columns)

        # 1. Drop system columns from raw input.
        table = arrow_utils.drop_system_columns(table)

        # 2. Validate tag_columns. A table with no columns at all (e.g. a
        # DictSource built from no rows and no schema) gets its own error, since
        # "tag column not found" would point at the wrong problem.
        if not table.column_names:
            raise ValueError(
                "Table is empty: the input has no columns, so its tag and data "
                "columns cannot be found. To build a source with no rows, give it "
                "an explicit schema (for example data_schema= on DictSource)."
            )
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
        schema_hash = compute_source_schema_hash(
            tag_python,
            data_python,
            self._data_context,
            self._config,
        )

        # 5. Compute table hash for data identity.
        hasher = self._data_context.arrow_hasher
        try:
            table_hash = hasher.hash_table(table)
        except ValueError as exc:
            # The hasher (starfix 0.5) raises ValueError for a type it does not hash
            # and names the first such column only; name every column it refuses.
            unhashable = _unhashable_columns(hasher, table)
            if not unhashable:
                raise
            described = ", ".join(
                f"{name!r} of type {kind}" for name, kind in unhashable
            )
            noun = "column" if len(unhashable) == 1 else "columns"
            raise ValueError(
                f"cannot hash {noun} {described}: the Arrow hasher refused "
                f"({exc}). Drop or convert before building the source."
            ) from exc

        # 6. Default source_id: the source kind and the labelled table hash. A
        # source id is the source's name, so a default built from a hash says
        # what kind of source it is and which algorithm made the hash. Only the
        # hex is shortened by ``hashing.path_n_char``.
        if source_id is None:
            if source_kind is None:
                raise ValueError(
                    "build() needs a source_id or a source_kind to make a default "
                    "source id from."
                )
            source_id = hash_source_id(
                source_kind, table_hash, hexdigits=self._config.hashing.path_n_char
            )

        # 7. Build per-row source-info strings and deterministic UUID record IDs.
        # Only the record-ID column's values are needed (none for positional
        # tokens), so convert that one column to Python, never the whole table.
        record_values: Sequence[object] = (
            table.column(record_id_column).to_pylist()
            if record_id_column is not None
            else [None] * table.num_rows
        )
        provenance_tokens = [
            _make_provenance_token(record_id_column, i, value)
            for i, value in enumerate(record_values)
        ]
        source_info = [
            f"{source_id}{constants.BLOCK_SEPARATOR}{token}"
            for token in provenance_tokens
        ]
        # Record IDs stored in the system tag column are stable UUID v5 bytes —
        # deterministically derived from source_id and per-row provenance token.
        # Same source_id + same row position/content → same record_id bytes.
        record_id_values = [
            _make_record_id(source_id, token).bytes for token in provenance_tokens
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
