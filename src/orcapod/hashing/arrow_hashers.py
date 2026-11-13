from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
from starfix import ArrowDigester

from orcapod.hashing.schema_cleaner import (
    clean_schema_for_hashing,
    has_extension_metadata,
    has_extension_type,
)
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.utils.arrow_utils import normalize_extension_columns
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.protocols.hashing_protocols import SemanticHasherProtocol


ARROW_HASH_LABEL = "arrow_v1"
"""``ContentHash.method`` of an Arrow table or schema hash: the single definition.

It names the whole procedure of ``StarfixArrowHasher``: the replacement of
extension-typed cells by their value digests, then the starfix digest (hash format 2,
starfix 0.5). A change that gives an existing table a different digest takes a new
label (``arrow_v2``). The number is not kept equal to the starfix hash format number.
The data context file names the hasher class and does not repeat the label.
"""


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
       digest: the starfix hash format number ``00 00 02`` (hash format 2,
       starfix 0.5) followed by a SHA-256 digest, byte-for-byte identical to
       the Rust ``starfix`` crate output. In ``hash_table`` the order of columns
       and of struct fields is not part of the hash (starfix's order options stay
       off); ``hash_table_ordered`` turns both options on. Row order and list
       element order are always part of it. Extension identity
       (``ARROW:extension:name`` and ``ARROW:extension:metadata``) is hashed
       through starfix's ``include_metadata`` whenever the table has an
       extension column, including pyarrow's built-in canonical extension types
       such as ``arrow.uuid``.

    starfix raises ``ValueError`` for inputs it cannot hash: data of ``null``,
    ``map``, interval and view types (whatever the row count); union and
    run-end-encoded types; two schema nodes sharing a path (duplicate column
    names, or a ``/`` in a name aliasing a struct child); and a non-nullable
    field holding a null. Those errors propagate unchanged; they name the
    column path and type.

    Parameters
    ----------
    type_converter:
        ``UniversalTypeConverter`` used to resolve extension types to Python
        types and convert storage values back to Python objects.
    semantic_hasher:
        ``SemanticHasherProtocol`` used to hash Python objects extracted
        from extension-typed columns.
    hasher_id:
        The label (``ContentHash.method``) of every hash produced. Defaults to
        ``ARROW_HASH_LABEL``, the label of this algorithm.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        semantic_hasher: "SemanticHasherProtocol",
        hasher_id: str = ARROW_HASH_LABEL,
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
            visitor = SemanticHashingVisitor(
                self._type_converter, self._semantic_hasher
            )

            try:
                # The hashed type is derived from the declared type alone, never
                # from a row: nested nullability, field metadata and list item
                # fields are kept, and a column hashes the same way whether it
                # holds data, nulls or no rows (ITL-942 I).
                # A value-hashed logical type keeps its identifier on the field, so
                # it is part of the hash also when the column holds no value
                # (ITL-967).
                new_field = visitor.hashed_field(field)
                processed_data = [
                    visitor.visit(field.type, value)[1] for value in column_data
                ]
                new_columns.append(pa.array(processed_data, type=new_field.type))
                new_fields.append(new_field)

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
        """Hash an Arrow schema using the starfix canonical algorithm.

        Each field is hashed under the same rule as in ``hash_table``
        (``SemanticHashingVisitor.hashed_field``). A logical type whose values
        are hashed by a value handler is a ``large_binary`` field carrying the
        type's identifier (extension name and metadata), whatever its storage
        type: how such a type is stored is not part of its identity. Any other
        extension type is normalized to its storage type with the extension
        identity in field metadata, because its stored values are what is
        hashed.
        """
        visitor = SemanticHashingVisitor(self._type_converter, self._semantic_hasher)
        hashed = pa.schema([visitor.hashed_field(field) for field in schema])
        if schema.metadata is not None:
            hashed = hashed.with_metadata(dict(schema.metadata))
        schema = hashed
        schema = normalize_extension_columns(schema.empty_table()).schema
        include_meta = has_extension_metadata(schema) or has_extension_type(schema)
        if include_meta:
            schema = clean_schema_for_hashing(schema)
        digest = ArrowDigester.hash_schema(schema, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)

    def hash_table(self, table: "pa.Table | pa.RecordBatch") -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) using starfix.

        Column order and struct field order are not part of the hash: this is the hash of
        Orcapod's own tables (streams, datagrams, rows), which ignore order on purpose.
        """
        return ContentHash(
            method=self._hasher_id,
            digest=self._digest(
                table, ordered_columns=False, ordered_struct_fields=False
            ),
        )

    def hash_table_ordered(
        self, table: "pa.Table | pa.RecordBatch", *, struct_field_order: bool = True
    ) -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) as a value, order included.

        The same as ``hash_table`` (the same semantic preprocessing, so a ``File`` column is
        hashed by its content), except that the order of the columns and, by default, of the
        fields of every struct at every depth is part of the hash (starfix's
        ``ordered_columns`` and ``ordered_struct_fields`` options). The options are an input
        of the digest, so the hashes of one table under different options never coincide,
        even for a one-column table. Value types whose own equality depends on order build
        their hash on this digest (pandas ``DataFrame`` and ``Series``); the ``ContentHash``
        returned here carries this hasher's label, and such a type uses the digest bytes
        only, under its own label.

        Args:
            table: The table or record batch to hash.
            struct_field_order: Whether the order of struct fields is part of the hash.
                ``False`` is for a value type configured to ignore struct field order;
                column order stays part of the hash.
        """
        return ContentHash(
            method=self._hasher_id,
            digest=self._digest(
                table, ordered_columns=True, ordered_struct_fields=struct_field_order
            ),
        )

    def _digest(
        self,
        table: "pa.Table | pa.RecordBatch",
        *,
        ordered_columns: bool,
        ordered_struct_fields: bool,
    ) -> bytes:
        """Preprocess ``table`` and return its starfix digest; the only starfix table call.

        Args:
            table: The table or record batch to hash.
            ordered_columns: Whether column order is part of the digest.
            ordered_struct_fields: Whether struct field order is part of the digest.

        Returns:
            The starfix digest (version prefix included).
        """
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        processed_table = self._process_table_columns(table)
        include_meta = has_extension_metadata(
            processed_table.schema
        ) or has_extension_type(processed_table.schema)
        if include_meta:
            clean_schema = clean_schema_for_hashing(processed_table.schema)
            clean_table = pa.Table.from_arrays(
                processed_table.columns, schema=clean_schema
            )
        else:
            clean_table = processed_table
        return ArrowDigester.hash_table(
            clean_table,
            include_metadata=include_meta,
            ordered_columns=ordered_columns,
            ordered_struct_fields=ordered_struct_fields,
        )
