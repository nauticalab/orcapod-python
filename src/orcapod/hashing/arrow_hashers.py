from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pyarrow as pa
from starfix import ArrowDigester

from orcapod.hashing.schema_cleaner import clean_schema_for_hashing, has_extension_metadata
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.utils.arrow_utils import normalize_extension_columns
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


class StarfixArrowHasher:
    """Arrow table hasher backed by the starfix-python ``ArrowDigester``.

    Pipeline
    --------
    1. **Semantic pre-processing** — the ``SemanticHashingVisitor`` traverses
       every column. Extension-typed columns whose Python type has a registered
       semantic hasher are replaced with ``pa.large_binary()`` hash tokens
       (e.g. ``Path`` columns are replaced by their file-content hash).
       Extension-typed columns without a registered hasher pass through with
       their full extension metadata intact.
    2. **Starfix hashing** — ``ArrowDigester.hash_table`` produces a 35-byte
       versioned SHA-256 digest that is byte-for-byte identical to the Rust
       ``starfix`` crate output.

    Parameters
    ----------
    type_converter:
        ``UniversalTypeConverter`` used to resolve extension types to Python
        types and convert storage values back to Python objects.
    semantic_hasher:
        ``SemanticHasherProtocol`` used to hash Python objects extracted
        from extension-typed columns.
    hasher_id:
        String identifier embedded in every ``ContentHash`` produced by this
        hasher.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        semantic_hasher: "SemanticHasherProtocol",
        hasher_id: str,
    ) -> None:
        self._type_converter = type_converter
        self._semantic_hasher = semantic_hasher
        self._hasher_id = hasher_id

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    def _process_table_columns(self, table: "pa.Table | pa.RecordBatch") -> "pa.Table":
        """Replace semantic-typed columns with content-hash bytes; normalize extension columns.

        For columns whose Python type has a registered semantic handler (e.g. ``Path``),
        the extension-typed column is replaced by a ``pa.large_binary()`` column of
        content-hash tokens.  For all other extension-typed columns (visitor passthrough),
        the column is normalized to IPC storage representation via
        ``normalize_extension_columns`` — storage type for the data, extension identity
        in field metadata — so that ``ArrowDigester`` can hash them without encountering
        a live ``pa.ExtensionType``, which is unhashable.
        """
        new_columns: list[pa.Array | pa.ChunkedArray] = []
        new_fields: list[pa.Field] = []

        for i, field in enumerate(table.schema):
            # Short-circuit: columns that cannot contain semantic types skip
            # the costly Python round-trip. Extension types must pass through
            # so visit_extension can process them.
            if not (
                isinstance(field.type, pa.ExtensionType)
                or pa.types.is_struct(field.type)
                or pa.types.is_list(field.type)
                or pa.types.is_large_list(field.type)
                or pa.types.is_fixed_size_list(field.type)
                or pa.types.is_map(field.type)
            ):
                new_columns.append(table.column(i))
                new_fields.append(field)
                continue

            column_data = table.column(i).to_pylist()
            visitor = SemanticHashingVisitor(self._type_converter, self._semantic_hasher)

            try:
                new_type: pa.DataType | None = None
                processed_data: list[Any] = []
                for value in column_data:
                    processed_type, processed_value = visitor.visit(field.type, value)
                    if new_type is None and processed_value is not None:
                        new_type = processed_type
                    processed_data.append(processed_value)

                if new_type is None:
                    new_type = field.type

                new_columns.append(pa.array(processed_data, type=new_type))
                new_fields.append(field.with_type(new_type))

            except Exception as exc:
                raise RuntimeError(
                    f"Failed to process column '{field.name}': {exc}"
                ) from exc

        intermediate = pa.table(
            new_columns,
            schema=pa.schema(new_fields, metadata=table.schema.metadata),
        )
        # Normalize any remaining extension-typed columns to their IPC storage
        # representation (storage type + ARROW:extension:* field metadata).
        # This handles the visitor passthrough case — extension types with no
        # registered semantic handler — so that ArrowDigester never receives a
        # live pa.ExtensionType, which is unhashable and would crash starfix.
        return normalize_extension_columns(intermediate)

    def hash_schema(self, schema: "pa.Schema") -> ContentHash:
        """Hash an Arrow schema using the starfix canonical algorithm."""
        include_meta = has_extension_metadata(schema)
        if include_meta:
            schema = clean_schema_for_hashing(schema)
        digest = ArrowDigester.hash_schema(schema, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)

    def hash_table(self, table: "pa.Table | pa.RecordBatch") -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) using starfix."""
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        processed_table = self._process_table_columns(table)
        include_meta = has_extension_metadata(processed_table.schema)
        if include_meta:
            clean_schema = clean_schema_for_hashing(processed_table.schema)
            clean_table = pa.Table.from_arrays(
                processed_table.columns, schema=clean_schema
            )
        else:
            clean_table = processed_table
        digest = ArrowDigester.hash_table(clean_table, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)
