"""Schema-walking utilities for extension type auto-registration and post-load casting.

Two entry points:

``register_discovered_logical_types(converter, schema)``
    Walk an Arrow schema and register any extension types not yet known to
    *converter*.  No-op when *converter* is ``None`` or the schema has no
    extension types.

``apply_logical_types(table, registry)``
    Re-wrap columns of *table* that carry ``ARROW:extension:*`` field metadata
    into their registered extension types.  Operates per-chunk so no data is
    copied — each chunk is wrapped with ``pa.ExtensionArray.from_storage()``.
    Nested struct fields are reconstructed recursively.

These two functions are typically called in sequence via ``UniversalTypeConverter``:

    register_discovered_logical_types(converter, table.schema)
    table = converter.apply_logical_types(table)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from orcapod.logical_types.registry import LogicalTypeRegistry
from orcapod.logical_types.schema_walker import walk_schema

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.logical_types.protocols import TypeConverterProtocol

logger = logging.getLogger(__name__)


def register_discovered_logical_types(
    converter: "TypeConverterProtocol | None",
    schema: "pa.Schema",
) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively via ``walk_schema`` to discover all Arrow extension
    types at any nesting depth (both in-memory and field-metadata channels).
    For each discovered type, delegates to ``converter.register_logical_type_from_arrow_metadata``.

    Already-registered types are detected and skipped inside the converter —
    this function itself is stateless beyond the converter it operates on.

    Args:
        converter: The ``TypeConverterProtocol`` to use for registration.
            If ``None``, this call is a no-op.
        schema: The Arrow schema to inspect. May contain no extension types,
            in which case this call is a no-op.

    Raises:
        ValueError: Propagated from the converter if an extension type's metadata
            has no registered factory or is malformed.
    """
    if converter is None:
        logger.debug("register_discovered_logical_types: no converter provided, skipping")
        return

    found = walk_schema(schema)
    if not found:
        logger.debug("register_discovered_logical_types: no extension types in schema")
        return
    logger.debug(
        "register_discovered_logical_types: found %d extension type(s) in schema: %s",
        len(found),
        [info.extension_name for info in found],
    )
    for info in found:
        # Bottom-up resolve the storage type first, then register the extension
        resolved_storage = converter.register_storage_type(info.storage_type)
        converter.register_logical_type_from_arrow_metadata(
            info.extension_name,
            info.extension_metadata,
            resolved_storage,
        )


def apply_logical_types(
    table: pa.Table,
    registry: LogicalTypeRegistry,
) -> pa.Table:
    """Re-wrap *table* columns into their registered Arrow extension types.

    Arrow preserves ``ARROW:extension:name`` / ``ARROW:extension:metadata``
    field metadata even when an extension type was not registered at read
    time, in which case the column is stored as a plain storage type (e.g.
    ``large_utf8``).  Once the extension type has been registered (via
    ``register_discovered_logical_types``), this function reconstructs the
    correct extension-typed columns using ``pa.ExtensionArray.from_storage``.

    The operation is zero-copy per chunk: each chunk in a ``ChunkedArray``
    is individually wrapped without rechunking or data movement.  Struct
    columns are handled recursively so nested extension type fields are also
    reconstructed.

    Columns whose field has no ``ARROW:extension:name`` metadata (plain Arrow
    types) are left untouched.

    Args:
        table: Arrow table whose columns may contain extension type metadata
            but were loaded as storage types.
        registry: Registry that holds the registered ``LogicalTypeProtocol``
            instances.  Must already contain every extension type referenced
            by ``table.schema`` — call ``register_discovered_logical_types``
            first.

    Returns:
        A new ``pa.Table`` with extension-typed columns re-wrapped.  Columns
        with no extension type metadata are shared with *table* unchanged.
    """
    import pyarrow as pa

    new_columns: list[pa.ChunkedArray] = []
    new_fields: list[pa.Field] = []
    changed = False

    for i, field in enumerate(table.schema):
        col = table.column(i)
        new_col, new_field = _apply_field(col, field, registry)
        new_columns.append(new_col)
        new_fields.append(new_field)
        if new_field is not field:
            changed = True

    if not changed:
        return table

    # Preserve any schema-level metadata (e.g. pandas metadata) from the original.
    new_schema = pa.schema(new_fields, metadata=table.schema.metadata)
    return pa.table(dict(zip(new_schema.names, new_columns)), schema=new_schema)


def _apply_field(
    col: pa.ChunkedArray,
    field: pa.Field,
    registry: LogicalTypeRegistry,
) -> tuple[pa.ChunkedArray, pa.Field]:
    """Return *(new_col, new_field)* with extension type applied if needed.

    Handles three cases:
    - Field already has an extension type → return as-is.
    - Field has extension metadata and a registered type → wrap per-chunk.
    - Field is a struct with extension-typed children → recurse.
    """
    import pyarrow as pa

    field_meta = field.metadata or {}
    ext_name_bytes = field_meta.get(b"ARROW:extension:name")

    # ── Case 1: field is already an extension type (registered at read time) ──
    if hasattr(field.type, "extension_name"):
        return col, field

    # ── Case 2: field has extension metadata and a matching registered type ───
    if ext_name_bytes is not None:
        ext_name = ext_name_bytes.decode("utf-8")
        lt = registry.get_by_arrow_extension_name(ext_name)
        if lt is not None:
            ext_type = lt.get_arrow_extension_type()
            wrapped_chunks = [
                pa.ExtensionArray.from_storage(ext_type, chunk)
                for chunk in col.chunks
            ]
            new_col = pa.chunked_array(wrapped_chunks, type=ext_type)
            new_field = field.with_type(ext_type)
            logger.debug("apply_logical_types: wrapped column %r as %r", field.name, ext_name)
            return new_col, new_field

    # ── Case 3: struct — recurse only if children carry extension metadata ──────
    if pa.types.is_struct(field.type):
        if _has_nested_extension_fields(field.type):
            return _apply_struct_field(col, field, registry)

    return col, field


def _has_nested_extension_fields(arrow_type: pa.DataType) -> bool:
    """Return True if any child field at any nesting depth carries extension metadata.

    Used to guard struct recursion: structs whose children carry no
    ``ARROW:extension:name`` metadata are returned as-is without rebuilding.
    """
    import pyarrow as pa

    for i in range(arrow_type.num_fields):
        child = arrow_type.field(i)
        meta = child.metadata or {}
        if b"ARROW:extension:name" in meta:
            return True
        if pa.types.is_struct(child.type) and _has_nested_extension_fields(child.type):
            return True
    return False


def _apply_struct_field(
    col: pa.ChunkedArray,
    field: pa.Field,
    registry: LogicalTypeRegistry,
) -> tuple[pa.ChunkedArray, pa.Field]:
    """Recursively apply extension types to children of a struct column."""
    import pyarrow as pa

    struct_type = field.type
    child_fields = [struct_type.field(i) for i in range(struct_type.num_fields)]

    # Process each chunk: rebuild StructArray with re-wrapped children.
    new_chunks: list[pa.StructArray] = []
    new_child_fields: list[pa.Field] | None = None

    for chunk in col.chunks:
        new_child_arrays: list[pa.Array] = []
        resolved_fields: list[pa.Field] = []

        for child_field in child_fields:
            child_arr = chunk.field(child_field.name)
            # Wrap child array into a single-chunk ChunkedArray for _apply_field.
            child_chunked = pa.chunked_array([child_arr], type=child_arr.type)
            new_child_chunked, new_child_field = _apply_field(
                child_chunked, child_field, registry
            )
            # from_storage produces a non-chunked Array; use combine_chunks for single chunk.
            new_child_arrays.append(new_child_chunked.combine_chunks())
            resolved_fields.append(new_child_field)

        # Preserve the original null bitmap so struct-level nulls survive wrapping.
        # StructArray.from_arrays() defaults to all-valid without an explicit mask.
        null_mask = chunk.is_null() if chunk.null_count > 0 else None
        new_struct = pa.StructArray.from_arrays(
            new_child_arrays, fields=resolved_fields, mask=null_mask
        )
        new_chunks.append(new_struct)
        if new_child_fields is None:
            new_child_fields = resolved_fields

    assert new_child_fields is not None  # col.chunks is non-empty if we reach here
    new_struct_type = pa.struct(new_child_fields)
    new_field = field.with_type(new_struct_type)
    new_col = pa.chunked_array(new_chunks, type=new_struct_type)
    return new_col, new_field
