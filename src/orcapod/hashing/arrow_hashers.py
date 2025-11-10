import hashlib
import json
from collections.abc import Callable
from typing import Any

import pyarrow as pa

from orcapod.hashing import arrow_serialization
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.protocols.hashing_protocols import ContentHash
from orcapod.semantic_types import SemanticTypeRegistry
from orcapod.utils import arrow_utils

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
        """
        Process table columns using visitor pattern to handle nested semantic types.

        This replaces the old column-by-column processing with a visitor-based approach
        that can handle semantic types nested inside complex data structures.
        """
        # TODO: Process in batchwise/chunk-wise fashion for memory efficiency
        # Currently using to_pylist() for simplicity but this loads entire table into memory

        new_columns = []
        new_fields = []

        # Import here to avoid circular dependencies
        for i, field in enumerate(table.schema):
            # Convert column to struct dicts for processing
            column_data = table.column(i).to_pylist()

            # TODO: verify the functioning of the visitor pattern
            # Create fresh visitor for each column (stateless approach)
            visitor = SemanticHashingVisitor(self.semantic_registry)

            try:
                # Use visitor to transform both type and data
                new_type = None
                processed_data = []
                for c in column_data:
                    processed_type, processed_value = visitor.visit(field.type, c)
                    if new_type is None:
                        new_type = processed_type
                    processed_data.append(processed_value)

                # Create new Arrow column from processed data
                assert new_type is not None, "Failed to infer new column type"
                # TODO: revisit this logic
                new_column = pa.array(processed_data, type=new_type)
                new_field = pa.field(field.name, new_type)

                new_columns.append(new_column)
                new_fields.append(new_field)

            except Exception as e:
                # Add context about which column failed
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

    def hash_table_with_metadata(self, table: pa.Table) -> dict[str, Any]:
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
