import hashlib
import json
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from starfix import ArrowDigester

from orcapod.hashing import arrow_serialization
from orcapod.hashing.schema_cleaner import clean_schema_for_hashing, has_extension_metadata
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.semantic_types import SemanticTypeRegistry
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter
    from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher

SERIALIZATION_METHOD_LUT: dict[str, Callable[[pa.Table], bytes]] = {
    "logical": arrow_serialization.serialize_table_logical,
}


def json_pyarrow_table_serialization(table: pa.Table) -> str:
    """
    Serialize a PyArrow table to a stable JSON string by converting to dictionary of lists.

    Args:
        table: PyArrow table to serialize

    Returns:
        JSON string representation with sorted keys and no whitespace
    """
    # Convert table to dictionary of lists using to_pylist()
    data_dict = {}

    for column_name in table.column_names:
        # Convert Arrow column to Python list, which visits all elements
        data_dict[column_name] = table.column(column_name).to_pylist()

    # Serialize to JSON with sorted keys and no whitespace
    return json.dumps(
        data_dict,
        separators=(",", ":"),
        sort_keys=True,
    )


class SemanticArrowHasher:
    """
    Stable hasher for Arrow tables with semantic type support.

    This hasher:
    1. Uses visitor pattern to recursively process nested data structures
    2. Replaces semantic types with their hash strings using registered converters
    3. Sorts columns by name for deterministic ordering
    4. Uses Arrow serialization for stable binary representation
    5. Computes final hash of the processed table
    """

    def __init__(
        self,
        semantic_registry: SemanticTypeRegistry,
        hasher_id: str | None = None,
        hash_algorithm: str = "sha256",
        chunk_size: int = 8192,
        handle_missing: str = "error",
        serialization_method: str = "logical",
        # TODO: consider passing options for serialization method
    ):
        """
        Initialize SemanticArrowHasher.

        Args:
            semantic_registry: Registry containing semantic type converters with hashing
            hash_algorithm: Hash algorithm to use for final table hash
            chunk_size: Size of chunks to read files in bytes (legacy, may be removed)
            hasher_id: Unique identifier for this hasher instance
            handle_missing: How to handle missing files ('error', 'skip', 'null_hash')
            serialization_method: Method for serializing Arrow table
        """
        if hasher_id is None:
            hasher_id = f"semantic_arrow_hasher:{hash_algorithm}:{serialization_method}"

        self._hasher_id = hasher_id
        self.semantic_registry = semantic_registry
        self.chunk_size = chunk_size
        self.handle_missing = handle_missing
        self.hash_algorithm = hash_algorithm

        if serialization_method not in SERIALIZATION_METHOD_LUT:
            raise ValueError(
                f"Invalid serialization method '{serialization_method}'. "
                f"Supported methods: {list(SERIALIZATION_METHOD_LUT.keys())}"
            )
        self.serialization_method = serialization_method

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    def _process_table_columns(self, table: pa.Table | pa.RecordBatch) -> pa.Table:
        """Process table columns using the semantic registry to hash struct-typed semantic columns.

        Traverses each column and replaces recognised semantic struct types (detected by
        struct signature via ``SemanticTypeRegistry``) with their content-hash strings.
        """
        # TODO: Process in batchwise/chunk-wise fashion for memory efficiency
        # Currently using to_pylist() for simplicity but this loads entire table into memory

        new_columns = []
        new_fields = []

        for i, field in enumerate(table.schema):
            column_data = table.column(i).to_pylist()

            try:
                if pa.types.is_struct(field.type):
                    converter = self.semantic_registry.get_converter_for_struct_signature(field.type)
                    if converter is not None:
                        # Semantic struct — replace with hash strings
                        processed_data = [
                            converter.hash_struct_dict(row) if row is not None else None
                            for row in column_data
                        ]
                        new_type = pa.large_string()
                        new_columns.append(pa.array(processed_data, type=new_type))
                        new_fields.append(pa.field(field.name, new_type))
                        continue

                # Not a semantic type — pass through unchanged
                new_columns.append(table.column(i))
                new_fields.append(field)

            except Exception as e:
                raise RuntimeError(
                    f"Failed to process column '{field.name}': {str(e)}"
                ) from e

        # Return new table with processed columns
        return pa.table(new_columns, schema=pa.schema(new_fields))

    def _sort_table_columns(self, table: pa.Table) -> pa.Table:
        """Sort table columns by field name for deterministic ordering."""
        # Get sorted column names
        sorted_column_names = sorted(table.column_names)

        # Use select to reorder columns - much cleaner!
        return table.select(sorted_column_names)

    def serialize_arrow_table(self, table: pa.Table) -> bytes:
        """
        Serialize Arrow table using the configured serialization method.

        Args:
            table: Arrow table to serialize

        Returns:
            Serialized bytes of the table
        """
        serialization_method_function = SERIALIZATION_METHOD_LUT[
            self.serialization_method
        ]
        return serialization_method_function(table)

    def hash_table(self, table: pa.Table | pa.RecordBatch) -> ContentHash:
        """
        Compute stable hash of Arrow table with semantic type processing.

        Args:
            table: Arrow table to hash
            prefix_hasher_id: Whether to prefix hash with hasher ID

        Returns:
            Hex string of the computed hash
        """

        # Step 1: Process columns with semantic types using visitor pattern
        processed_table = self._process_table_columns(table)

        # Step 2: Sort columns by name for deterministic ordering
        sorted_table = self._sort_table_columns(processed_table)

        # normalize all string to large strings (for compatibility with Polars)
        normalized_table = arrow_utils.normalize_table_to_large_types(sorted_table)

        # Step 3: Serialize using configured serialization method
        serialized_bytes = self.serialize_arrow_table(normalized_table)

        # Step 4: Compute final hash
        hasher = hashlib.new(self.hash_algorithm)
        hasher.update(serialized_bytes)

        return ContentHash(method=self.hasher_id, digest=hasher.digest())

    def hash_table_with_metadata(self, table: pa.Table) -> dict[str, Any]:  # noqa: C901
        """
        Compute hash with additional metadata about the process.

        Returns:
            Dictionary containing hash, metadata, and processing info
        """
        # Process table to see what transformations were made
        processed_table = self._process_table_columns(table)

        # Track processing steps
        processed_columns = []
        for i, (original_field, processed_field) in enumerate(
            zip(table.schema, processed_table.schema)
        ):
            column_info = {
                "name": original_field.name,
                "original_type": str(original_field.type),
                "processed_type": str(processed_field.type),
                "was_processed": str(original_field.type) != str(processed_field.type),
            }
            processed_columns.append(column_info)

        # Compute hash
        table_hash = self.hash_table(table)

        return {
            "hash": table_hash,
            "hasher_id": self.hasher_id,
            "serialization_method": self.serialization_method,
            "hash_algorithm": self.hash_algorithm,
            "num_rows": len(table),
            "num_columns": len(table.schema),
            "processed_columns": processed_columns,
            "column_order": [field.name for field in table.schema],
        }


class StarfixArrowHasher:
    """
    Arrow table hasher backed by the starfix-python ``ArrowDigester``.

    This hasher produces cross-language-compatible, deterministic content
    addresses for Arrow tables and schemas by delegating to the canonical
    StarFix specification (``starfix-python``).

    Pipeline
    --------
    1. **Semantic pre-processing** — the ``SemanticHashingVisitor`` traverses
       every column and replaces recognised extension-typed columns (e.g. ``Path``)
       with their content-addressed hash bytes.  This step runs before the Arrow
       bytes are ever touched by starfix, so the final hash captures *file content*
       for path-typed columns rather than the raw path string.
    2. **Starfix hashing** — ``ArrowDigester.hash_table`` (or
       ``ArrowDigester.hash_schema``) is called on the pre-processed table /
       schema.  The digester is column-order-independent and normalises
       ``Utf8`` → ``LargeUtf8``, ``Binary`` → ``LargeBinary``, etc.,
       producing a 35-byte versioned SHA-256 digest that is byte-for-byte
       identical to the Rust ``starfix`` crate output.

    Parameters
    ----------
    type_converter:
        ``UniversalTypeConverter`` used by ``SemanticHashingVisitor`` to resolve
        Arrow extension types to Python types and convert storage values.
    python_hasher:
        ``SemanticAwarePythonHasher`` used by ``SemanticHashingVisitor`` to hash
        Python objects produced from extension-typed columns.
    hasher_id:
        String identifier embedded in every ``ContentHash`` produced by
        this hasher.  Bump this value whenever the hash algorithm changes
        so that stored hashes remain distinguishable.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        hasher_id: str,
        python_hasher: "SemanticAwarePythonHasher | None" = None,
    ) -> None:
        self._hasher_id = hasher_id
        self._type_converter = type_converter
        self._python_hasher = python_hasher

    @property
    def hasher_id(self) -> str:
        return self._hasher_id

    def _get_python_hasher(self) -> "SemanticAwarePythonHasher":
        """Return the python_hasher, lazily resolving from default context if not set.

        Lazy resolution breaks the circular dependency that would arise if ``arrow_hasher``
        were constructed before ``semantic_hasher`` in the context JSON spec (which is the
        natural order since ``type_handler_registry`` references ``arrow_hasher`` for
        ``ArrowTableSemanticHasher``).
        """
        if self._python_hasher is not None:
            return self._python_hasher
        from orcapod.contexts import get_default_context
        return get_default_context().semantic_hasher  # type: ignore[return-value]

    def _process_table_columns(self, table: pa.Table | pa.RecordBatch) -> pa.Table:
        """Replace extension-typed columns with their content-hash bytes."""
        new_columns: list[pa.Array] = []
        new_fields: list[pa.Field] = []

        python_hasher = self._get_python_hasher()

        for i, field in enumerate(table.schema):
            # Short-circuit: primitive columns (non-extension, non-struct, non-list, non-map)
            # cannot contain extension semantic types, so skip the costly Python round-trip
            # and reuse the original Arrow array directly.
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
            visitor = SemanticHashingVisitor(self._type_converter, python_hasher)

            try:
                new_type: pa.DataType | None = None
                processed_data: list[Any] = []
                for value in column_data:
                    processed_type, processed_value = visitor.visit(field.type, value)
                    # Infer the output type from the first non-null processed value.
                    # When the first row is null, visit_struct returns the original
                    # struct type rather than the converted type (e.g. large_string),
                    # which would cause pa.array() to fail for subsequent non-null rows.
                    if new_type is None and processed_value is not None:
                        new_type = processed_type
                    processed_data.append(processed_value)

                # For empty or all-null columns there are no non-null values to infer
                # the type from; fall back to the field's declared type.
                if new_type is None:
                    new_type = field.type
                new_columns.append(pa.array(processed_data, type=new_type))
                # Preserve original field attributes (nullable, metadata) while
                # updating only the type, so the schema fed to starfix remains faithful.
                new_fields.append(field.with_type(new_type))

            except Exception as exc:
                raise RuntimeError(
                    f"Failed to process column '{field.name}': {exc}"
                ) from exc

        # Preserve the original schema-level metadata while using updated fields.
        return pa.table(new_columns, schema=pa.schema(new_fields, metadata=table.schema.metadata))

    def hash_schema(self, schema: pa.Schema) -> ContentHash:
        """Hash an Arrow schema using the starfix canonical algorithm.

        ``has_extension_metadata`` is checked first on the raw schema. When
        no extension metadata is found, ``include_metadata=False`` is passed
        to ``ArrowDigester`` directly without rebuilding the schema (starfix
        ignores metadata when ``include_metadata=False``, so the hash is
        identical). When extension metadata is present, ``clean_schema_for_hashing``
        strips non-``ARROW:extension:*`` keys before hashing with
        ``include_metadata=True``, preserving byte-for-byte hash stability
        with pre-v0.3.0 output for extension-free schemas.

        Parameters
        ----------
        schema:
            The ``pa.Schema`` to hash.

        Returns
        -------
        ContentHash
            A ``ContentHash`` whose ``digest`` is the 35-byte versioned
            SHA-256 produced by ``ArrowDigester.hash_schema``.
        """
        include_meta = has_extension_metadata(schema)
        if include_meta:
            schema = clean_schema_for_hashing(schema)
        digest = ArrowDigester.hash_schema(schema, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)

    def hash_table(self, table: pa.Table | pa.RecordBatch) -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) using starfix.

        Semantic types are resolved to their content-hash strings first.
        ``has_extension_metadata`` is then checked on the processed table's
        schema. When no extension metadata is found, the processed table is
        passed to ``ArrowDigester.hash_table`` directly with
        ``include_metadata=False``, avoiding a schema rebuild and new table
        allocation. When extension metadata is present,
        ``clean_schema_for_hashing`` strips non-``ARROW:extension:*`` keys
        before hashing with ``include_metadata=True``.

        Parameters
        ----------
        table:
            The ``pa.Table`` or ``pa.RecordBatch`` to hash.

        Returns
        -------
        ContentHash
            A ``ContentHash`` whose ``digest`` is the 35-byte versioned
            SHA-256 produced by ``ArrowDigester.hash_table``.
        """
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        processed_table = self._process_table_columns(table)
        include_meta = has_extension_metadata(processed_table.schema)
        if include_meta:
            clean_schema = clean_schema_for_hashing(processed_table.schema)
            # clean_schema_for_hashing only strips metadata; physical types and
            # column order are unchanged, so from_arrays is safe without a cast.
            clean_table = pa.Table.from_arrays(
                processed_table.columns, schema=clean_schema
            )
        else:
            clean_table = processed_table
        digest = ArrowDigester.hash_table(clean_table, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)
