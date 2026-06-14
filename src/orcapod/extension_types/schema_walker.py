"""Recursive Arrow schema walker for extension type discovery.

Given a ``pa.Schema`` or a single ``pa.Field``, walks the Arrow type tree
recursively and returns all extension-typed fields found at any depth of
nesting (struct, list, map, etc.).

This is a pure discovery utility — it never triggers any registration.
"""

from __future__ import annotations

import dataclasses

import pyarrow as pa


@dataclasses.dataclass(frozen=True)
class ExtensionTypeInfo:
    """Metadata for a single Arrow extension type found in a schema.

    Attributes:
        extension_name: The extension type's unique name stored as
            ``ARROW:extension:name`` (e.g. ``"pathlib.Path"``).
        extension_metadata: The category tag stored as
            ``ARROW:extension:metadata`` (e.g. ``b"orcapod.dataclass"``).
            ``None`` when absent or serialised as empty bytes.
        storage_type: The underlying Arrow storage type
            (e.g. ``pa.large_string()``).
    """

    extension_name: str
    extension_metadata: bytes | None
    storage_type: pa.DataType


def walk_schema(schema: pa.Schema) -> list[ExtensionTypeInfo]:
    """Walk *schema* and return all extension types found, deduplicated.

    Iterates every top-level field and descends recursively into struct,
    list, and map container types. The result is deduplicated by
    ``(extension_name, extension_metadata)``; the first occurrence of each
    pair is kept.

    Args:
        schema: A PyArrow schema to inspect.

    Returns:
        Deduplicated list of ``ExtensionTypeInfo`` in depth-first,
        first-seen order.
    """
    seen: set[tuple[str, bytes | None]] = set()
    results: list[ExtensionTypeInfo] = []
    for i in range(len(schema)):
        _collect(schema.field(i), seen, results)
    return results


def walk_field(field: pa.Field) -> list[ExtensionTypeInfo]:
    """Walk *field*'s type tree and return all extension types found, deduplicated.

    Args:
        field: A PyArrow field to inspect.

    Returns:
        Deduplicated list of ``ExtensionTypeInfo`` in depth-first,
        first-seen order.
    """
    seen: set[tuple[str, bytes | None]] = set()
    results: list[ExtensionTypeInfo] = []
    _collect(field, seen, results)
    return results


def _collect(
    field: pa.Field,
    seen: set[tuple[str, bytes | None]],
    results: list[ExtensionTypeInfo],
) -> None:
    """Recursively walk *field* and accumulate ``ExtensionTypeInfo`` into *results*.

    Mutates *seen* and *results* in place. Stops descending once a field is
    identified as extension-typed — the storage type of an extension type is
    not descended into.

    Args:
        field: The field to inspect.
        seen: Deduplication set of ``(extension_name, extension_metadata)``
            pairs already appended to *results*.
        results: Accumulator list.
    """
    info = _detect_extension(field)
    if info is not None:
        key = (info.extension_name, info.extension_metadata)
        if key not in seen:
            seen.add(key)
            results.append(info)
        return

    t = field.type
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            _collect(t.field(i), seen, results)
    elif (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
        or pa.types.is_list_view(t)
        or pa.types.is_large_list_view(t)
    ):
        _collect(t.value_field, seen, results)
    elif pa.types.is_map(t):
        key_field = getattr(t, "key_field", None)
        item_field = getattr(t, "item_field", None)
        if key_field is not None:
            _collect(key_field, seen, results)
        if item_field is not None:
            _collect(item_field, seen, results)


def _detect_extension(field: pa.Field) -> ExtensionTypeInfo | None:
    """Extract ``ExtensionTypeInfo`` from *field*, or ``None`` if not extension-typed.

    Checks two channels in order:

    1. **Registered channel** — ``pa.types.is_extension(field.type)`` is
       true. The Python type object carries the name, serialised metadata,
       and storage type.
    2. **Unregistered channel** — ``field.metadata`` contains
       ``b"ARROW:extension:name"``. The type survived a Parquet/IPC
       round-trip without being registered in this process.

    In both cases empty bytes metadata (``b""``) is normalised to ``None``.

    Args:
        field: The field to inspect.

    Returns:
        ``ExtensionTypeInfo`` if the field is extension-typed, else ``None``.
    """
    if isinstance(field.type, pa.ExtensionType):
        ext_type = field.type
        raw_meta = ext_type.__arrow_ext_serialize__()
        return ExtensionTypeInfo(
            extension_name=ext_type.extension_name,
            extension_metadata=raw_meta or None,
            storage_type=ext_type.storage_type,
        )

    if field.metadata and b"ARROW:extension:name" in field.metadata:
        name = field.metadata[b"ARROW:extension:name"].decode("utf-8")
        raw_meta = field.metadata.get(b"ARROW:extension:metadata")
        return ExtensionTypeInfo(
            extension_name=name,
            extension_metadata=raw_meta or None,
            storage_type=field.type,
        )

    return None
