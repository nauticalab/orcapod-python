"""Schema metadata cleaner for Arrow hashing.

This module provides utilities for preprocessing Arrow schemas before
content-addressed hashing. Only ``ARROW:extension:*`` metadata keys are
identity-bearing in Orcapod; all other keys (comments, vendor annotations,
source-file provenance, etc.) are stripped so they cannot affect the hash.

Contract
--------
Only ``ARROW:extension:*`` metadata keys are treated as identity-bearing.
If an extension type were to stamp extra metadata under a non-``ARROW:extension:*``
key, that metadata would be silently stripped by ``clean_schema_for_hashing``.
Orcapod does not currently rely on any such pattern.

Public API
----------
- ``clean_schema_for_hashing(schema)`` — returns a cleaned copy of the schema.
- ``has_extension_metadata(schema)`` — returns True if the (cleaned) schema has
  any ``ARROW:extension:name`` key on any field at any nesting depth, or in the
  schema-level metadata.
"""

from __future__ import annotations

import pyarrow as pa

_EXTENSION_PREFIX: bytes = b"ARROW:extension:"
_EXTENSION_NAME_KEY: bytes = b"ARROW:extension:name"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _filter_metadata(metadata: dict[bytes, bytes] | None) -> dict[bytes, bytes]:
    """Return only the entries whose key starts with ``b'ARROW:extension:'``."""
    if not metadata:
        return {}
    return {k: v for k, v in metadata.items() if k.startswith(_EXTENSION_PREFIX)}


def _clean_type(arrow_type: pa.DataType) -> pa.DataType:
    """Recursively rebuild a composite type with cleaned child-field metadata.

    For primitive (leaf) types, returns the type unchanged. For composite types
    (struct, list, large_list, fixed_size_list, map), rebuilds the type using
    cleaned versions of all child fields.
    """
    if pa.types.is_struct(arrow_type):
        return pa.struct([
            _clean_field(arrow_type.field(i))
            for i in range(arrow_type.num_fields)
        ])
    if pa.types.is_list(arrow_type):
        return pa.list_(_clean_field(arrow_type.value_field))
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(_clean_field(arrow_type.value_field))
    if pa.types.is_fixed_size_list(arrow_type):
        # pa.list_(field, size) produces FixedSizeListType when size is an int
        return pa.list_(_clean_field(arrow_type.value_field), arrow_type.list_size)
    if pa.types.is_map(arrow_type):
        return pa.map_(
            _clean_field(arrow_type.key_field),
            _clean_field(arrow_type.item_field),
            arrow_type.keys_sorted,
        )
    return arrow_type


def _clean_field(field: pa.Field) -> pa.Field:
    """Return a copy of ``field`` with metadata and child-type metadata cleaned.

    The field name, type identity (Arrow type), and nullability flag are
    preserved exactly. Only the metadata dict is filtered.
    """
    return pa.field(
        field.name,
        _clean_type(field.type),
        nullable=field.nullable,
        metadata=_filter_metadata(field.metadata),
    )


def _has_extension_in_type(arrow_type: pa.DataType) -> bool:
    """Return True if any child field of ``arrow_type`` has extension metadata."""
    if pa.types.is_struct(arrow_type):
        return any(
            _has_extension_in_field(arrow_type.field(i))
            for i in range(arrow_type.num_fields)
        )
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _has_extension_in_field(arrow_type.value_field)
    if pa.types.is_map(arrow_type):
        return (
            _has_extension_in_field(arrow_type.key_field)
            or _has_extension_in_field(arrow_type.item_field)
        )
    return False


def _has_extension_in_field(field: pa.Field) -> bool:
    """Return True if ``field`` or any of its nested fields carries extension metadata."""
    if field.metadata and _EXTENSION_NAME_KEY in field.metadata:
        return True
    return _has_extension_in_type(field.type)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def clean_schema_for_hashing(schema: pa.Schema) -> pa.Schema:
    """Return a copy of ``schema`` with all non-extension metadata stripped.

    Walks the schema and every field recursively (through struct, list,
    large_list, fixed_size_list, and map child fields). Retains only metadata
    keys that start with ``b'ARROW:extension:'`` — at both schema level and
    per-field level. All other metadata is dropped.

    Names, types, and nullability flags are untouched.

    Args:
        schema: The ``pa.Schema`` to clean.

    Returns:
        A new ``pa.Schema`` with the same structure but with non-extension
        metadata removed at every level. Schema-level metadata becomes an
        empty dict if no extension keys were present.
    """
    return pa.schema(
        [_clean_field(schema.field(i)) for i in range(len(schema))],
        metadata=_filter_metadata(schema.metadata),
    )


def has_extension_metadata(schema: pa.Schema) -> bool:
    """Return True if ``schema`` has any ``ARROW:extension:name`` key at any depth.

    Intended to be called on the output of ``clean_schema_for_hashing`` so
    that the check is purely a key-presence test (no re-filtering needed).

    Recurses through struct, list, large_list, fixed_size_list, and map child
    fields identically to ``clean_schema_for_hashing``.

    Args:
        schema: A ``pa.Schema`` (typically the output of
            ``clean_schema_for_hashing``).

    Returns:
        True if any field at any nesting depth carries ``ARROW:extension:name``
        in its metadata, or if the schema-level metadata contains
        ``ARROW:extension:name``; False otherwise.
    """
    if schema.metadata and _EXTENSION_NAME_KEY in schema.metadata:
        return True
    return any(
        _has_extension_in_field(schema.field(i)) for i in range(len(schema))
    )
