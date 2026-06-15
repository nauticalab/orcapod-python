"""Schema-walking utilities for extension type auto-registration and post-load casting.

Two entry points:

``register_discovered_extensions(registry, schema)``
    Walk an Arrow schema and register any extension types not yet known to
    *registry*.  No-op when *registry* is ``None`` or the schema has no
    extension types.

``apply_extension_types(table, registry)``
    Re-wrap columns of *table* that carry ``ARROW:extension:*`` field metadata
    into their registered extension types.  Operates per-chunk so no data is
    copied — each chunk is wrapped with ``pa.ExtensionArray.from_storage()``.
    Nested struct fields are reconstructed recursively.

These two functions are typically called in sequence:

    register_discovered_extensions(registry, table.schema)
    table = apply_extension_types(table, registry)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from orcapod.extension_types.registry import LogicalTypeRegistry
from orcapod.extension_types.schema_walker import walk_schema

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


def register_discovered_extensions(
    registry: LogicalTypeRegistry | None,
    schema: pa.Schema,
) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively to discover all Arrow extension types at any
    nesting depth. For each discovered type, delegates to
    ``registry.ensure_extension_type``.

    Already-registered types are detected and skipped inside the registry —
    this function itself is stateless beyond the registry it operates on.

    Args:
        registry: The ``LogicalTypeRegistry`` to use for lookup and registration.
            If ``None``, this call is a no-op — no extension types will be
            registered. Callers that want auto-registration must supply a registry
            explicitly; the typical source is
            ``data_context.logical_type_registry``.
        schema: The Arrow schema to inspect. May contain no extension types,
            in which case this call is a no-op.

    Raises:
        ValueError: Propagated from the registry if an extension type's metadata
            has no registered factory or is malformed.
    """
    if registry is None:
        logger.debug("register_discovered_extensions: no registry provided, skipping")
        return

    found = walk_schema(schema)
    if not found:
        logger.debug("register_discovered_extensions: no extension types in schema")
        return
    logger.debug(
        "register_discovered_extensions: found %d extension type(s) in schema: %s",
        len(found),
        [info.extension_name for info in found],
    )
    for info in found:
        registry.ensure_extension_type(
            info.extension_name,
            info.extension_metadata,
            info.storage_type,
        )


def apply_extension_types(
    table: pa.Table,
    registry: LogicalTypeRegistry,
) -> pa.Table:
    """Re-wrap *table* columns into their registered Arrow extension types.

    Arrow preserves ``ARROW:extension:name`` / ``ARROW:extension:metadata``
    field metadata even when an extension type was not registered at read
    time, in which case the column is stored as a plain storage type (e.g.
    ``large_utf8``).  Once the extension type has been registered (via
    ``register_discovered_extensions``), this function reconstructs the
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
            by ``table.schema`` — call ``register_discovered_extensions``
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

    new_schema = pa.schema(new_fields)
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
            logger.debug("apply_extension_types: wrapped column %r as %r", field.name, ext_name)
            return new_col, new_field

    # ── Case 3: struct — recurse into children ────────────────────────────────
    if pa.types.is_struct(field.type):
        new_col, new_field = _apply_struct_field(col, field, registry)
        return new_col, new_field

    return col, field


def _apply_struct_field(
    col: pa.ChunkedArray,
    field: pa.Field,
    registry: LogicalTypeRegistry,
) -> tuple[pa.ChunkedArray, pa.Field]:
    """Recursively apply extension types to children of a struct column."""
    import pyarrow as pa

    struct_type = field.type
    child_names = [struct_type.field(i).name for i in range(struct_type.num_fields)]
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

        new_struct = pa.StructArray.from_arrays(new_child_arrays, fields=resolved_fields)
        new_chunks.append(new_struct)
        if new_child_fields is None:
            new_child_fields = resolved_fields

    assert new_child_fields is not None  # col.chunks is non-empty if we reach here
    new_struct_type = pa.struct(new_child_fields)
    new_field = field.with_type(new_struct_type)
    new_col = pa.chunked_array(new_chunks, type=new_struct_type)
    return new_col, new_field
