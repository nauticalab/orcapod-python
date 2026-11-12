"""The nullability record of a SpiralDB table (ITL-936).

SpiralDB reports every top-level column (key columns included) and every struct field as
nullable, whatever was written; only list items keep a non-null flag. Orcapod therefore
records the non-nullable column paths of a table in the table's key-value metadata, and its
Spiral connector restores and enforces them on read.

Format, version 1:

* Header key ``__orcapod_nullability__``: compact JSON
  ``{"format": "orcapod.nullability", "version": 1, "chunks": N, "sha256": "<hex>"}``.
* Chunk keys ``__orcapod_nullability__/<first 16 hex digits of sha256>/<i>`` for ``i`` in
  ``0..N-1``: their values, concatenated, are the payload, whose SHA-256 is ``sha256``.
* Payload: a compact JSON list of the non-nullable paths, sorted, for example
  ``[["g"],["g","x"],["k"],["l"],["l","[]"]]``. A path starts with a top-level column name;
  each further segment is a struct field name, or ``"[]"`` for the item of a ``list``,
  ``large_list`` or ``fixed_size_list``.

A listed path asserts that the field is non-nullable in Arrow's sense: its array holds no
null. A path that is not listed keeps the nullability SpiralDB reports. Map interiors are
not recorded. Chunks are written before the header, and chunk keys are named by the payload
digest, so a reader sees either the previous record or the new one; chunks without a header
are no record.

This module does not import pyspiral: ``write_nullability_record`` works on any object with
pyspiral's ``Table`` metadata methods.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

from orcapod.errors import NullabilityRecordError, NullInNonNullableColumnError
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

RECORD_KEY = "__orcapod_nullability__"
_FORMAT = "orcapod.nullability"
_VERSION = 1
ITEM = "[]"
# SpiralDB limits a metadata key + value pair to 8 KiB; leave room for the chunk key.
_MAX_PAIR_BYTES = 8192
_CHUNK_BYTES = _MAX_PAIR_BYTES - 128

ColumnPath = tuple[str, ...]


class MetadataTable(Protocol):
    """The metadata methods of a pyspiral ``Table`` that the record uses."""

    def get_metadata(self) -> dict[str, bytes]:
        """Return the table's key-value metadata."""
        ...

    def set_metadata(self, metadata: dict[str, bytes]) -> None:
        """Upsert entries into the table's key-value metadata."""
        ...

    def drop_metadata(self, keys: list[str]) -> None:
        """Remove keys from the table's key-value metadata."""
        ...


def render_path(path: ColumnPath) -> str:
    """Return ``path`` as text: ``g.x`` for a struct field, ``l[]`` for a list item."""
    out = path[0]
    for segment in path[1:]:
        out += ITEM if segment == ITEM else f".{segment}"
    return out


# ------------------------------------------------------------------- schema -> paths


def _storage(t: pa.DataType) -> pa.DataType:
    """Return the storage type of an extension type (SpiralDB stores the storage type)."""
    return t.storage_type if isinstance(t, pa.ExtensionType) else t


def _children(t: pa.DataType) -> list[tuple[str, pa.Field[Any]]]:
    """Return ``(segment, field)`` for each child the record can address."""
    if pa.types.is_struct(t):
        return [(t.field(i).name, t.field(i)) for i in range(t.num_fields)]
    if (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
    ):
        return [(ITEM, t.value_field)]
    return []


def _has_non_nullable(field: pa.Field[Any]) -> bool:
    """Return whether ``field`` or any field nested in it is non-nullable."""
    if not field.nullable:
        return True
    return any(_has_non_nullable(child) for _, child in _children(_storage(field.type)))


def _collect(field: pa.Field[Any], path: ColumnPath, out: set[ColumnPath]) -> None:
    """Add the non-nullable paths at and below ``field`` to ``out``.

    Args:
        field: The field to walk.
        path: The field's path.
        out: The set collecting non-nullable paths.

    Raises:
        NullabilityRecordError: If a map holds a non-nullable field.
    """
    if not field.nullable:
        out.add(path)
    t = _storage(field.type)
    if pa.types.is_map(t):
        # Map keys are non-nullable by definition and are not recorded.
        if _has_non_nullable(t.item_field):
            raise NullabilityRecordError(
                f"column {render_path(path)!r} has a non-nullable field inside a map, "
                "which the record cannot express"
            )
        return
    for segment, child in _children(t):
        _collect(child, (*path, segment), out)


def schema_paths(schema: pa.Schema) -> frozenset[ColumnPath]:
    """Return the non-nullable paths of ``schema``, nested fields included.

    Raises:
        NullabilityRecordError: If a map holds a non-nullable field.
    """
    out: set[ColumnPath] = set()
    for field in schema:
        _collect(field, (field.name,), out)
    return frozenset(out)


# ------------------------------------------------------------------- encode / decode


def _encode(paths: Iterable[ColumnPath]) -> tuple[dict[str, bytes], bytes]:
    """Return the chunk entries and the header value for ``paths``."""
    payload = json.dumps(
        sorted(list(p) for p in paths), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    pieces = [
        payload[i : i + _CHUNK_BYTES] for i in range(0, len(payload), _CHUNK_BYTES)
    ]
    chunks = {_chunk_key(digest, i): piece for i, piece in enumerate(pieces)}
    header = json.dumps(
        {
            "format": _FORMAT,
            "version": _VERSION,
            "chunks": len(pieces),
            "sha256": digest,
        },
        separators=(",", ":"),
    ).encode("utf-8")
    return chunks, header


def _chunk_key(digest: str, index: int) -> str:
    """Return the metadata key of chunk ``index`` of the payload with SHA-256 ``digest``.

    Args:
        digest: The payload's SHA-256, as hex.
        index: The chunk's position, from 0.

    Returns:
        ``__orcapod_nullability__/<first 16 hex digits>/<index>``.
    """
    return f"{RECORD_KEY}/{digest[:16]}/{index}"


def read_nullability_record(
    metadata: Mapping[str, bytes],
) -> frozenset[ColumnPath] | None:
    """Return the non-nullable paths recorded in a table's key-value metadata.

    Args:
        metadata: The table's key-value metadata (pyspiral ``Table.get_metadata()``).

    Returns:
        The recorded paths, or ``None`` if the table has no record (no header key).

    Raises:
        NullabilityRecordError: If the record cannot be read in full: the header is not
            JSON or has the wrong shape, the format or version is unknown, a chunk is
            missing, the digest does not match, or the payload is not a list of paths.
    """
    raw = metadata.get(RECORD_KEY)
    if raw is None:
        return None
    try:
        header = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NullabilityRecordError(f"the header is not JSON ({exc})") from exc
    if not isinstance(header, dict):
        raise NullabilityRecordError("the header is not a JSON object")
    if header.get("format") != _FORMAT:
        raise NullabilityRecordError(f"unknown format {header.get('format')!r}")
    if header.get("version") != _VERSION:
        raise NullabilityRecordError(
            f"unsupported version {header.get('version')!r} (this Orcapod reads "
            f"version {_VERSION})"
        )
    count, digest = header.get("chunks"), header.get("sha256")
    if not isinstance(count, int) or isinstance(count, bool) or count < 1:
        raise NullabilityRecordError(f"invalid chunk count {count!r}")
    if not isinstance(digest, str) or len(digest) != 64:
        raise NullabilityRecordError(f"invalid digest {digest!r}")
    pieces = []
    for i in range(count):
        piece = metadata.get(_chunk_key(digest, i))
        if piece is None:
            raise NullabilityRecordError(f"chunk {i} of {count} is missing")
        pieces.append(piece)
    payload = b"".join(pieces)
    if hashlib.sha256(payload).hexdigest() != digest:
        raise NullabilityRecordError("the chunks do not match the header's digest")
    try:
        paths = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise NullabilityRecordError(f"the payload is not JSON ({exc})") from exc
    if not isinstance(paths, list) or not all(
        isinstance(p, list) and p and all(isinstance(s, str) for s in p) for p in paths
    ):
        raise NullabilityRecordError(
            "the payload is not a list of column paths (lists of strings)"
        )
    return frozenset(tuple(p) for p in paths)


def write_nullability_record(table: MetadataTable, schema: pa.Schema) -> bool:
    """Record the non-nullable paths of ``schema`` in ``table``'s key-value metadata.

    For writers other than Orcapod's connector (and for replacing a wrong record): pass the
    Arrow schema the table's data was written with. Chunks are written first, then the
    header, then chunks of an earlier record are dropped, so a reader never sees a header
    whose chunks are missing. Other metadata keys are left alone. The helper reads no data:
    check the table holds no null in the schema's non-nullable columns before recording it.

    Replacing a record changes the schema Orcapod reads, and so the table's
    ``pipeline_hash`` and ``content_hash``.

    Args:
        table: A pyspiral ``Table`` (anything with ``get_metadata``, ``set_metadata`` and
            ``drop_metadata``).
        schema: The Arrow schema the table was written with.

    Returns:
        ``True`` if a record was written, ``False`` if the table already held this record.

    Raises:
        NullabilityRecordError: If ``schema`` has a non-nullable field inside a map.
    """
    paths = schema_paths(schema)
    current_metadata = table.get_metadata()
    try:
        current = read_nullability_record(current_metadata)
    except NullabilityRecordError:
        current = None  # a broken record is replaced
    if current == paths:
        return False
    chunks, header = _encode(paths)
    table.set_metadata(chunks)
    table.set_metadata({RECORD_KEY: header})
    stale = [
        k
        for k in current_metadata
        if k.startswith(RECORD_KEY + "/") and k not in chunks
    ]
    if stale:
        table.drop_metadata(stale)
    return True


def remove_nullability_record(table: MetadataTable) -> None:
    """Remove ``table``'s nullability record: the header first, then every chunk.

    Used by the connector to undo a record whose rows failed to write. Chunks left
    behind by a failure after the header is gone are no record.

    Args:
        table: A pyspiral ``Table`` (anything with the ``MetadataTable`` methods).
    """
    metadata = table.get_metadata()
    if RECORD_KEY in metadata:
        table.drop_metadata([RECORD_KEY])
    chunks = [k for k in metadata if k.startswith(RECORD_KEY + "/")]
    if chunks:
        table.drop_metadata(chunks)


# ------------------------------------------------------------------- apply / enforce


def _restrict_field(
    field: pa.Field[Any],
    path: ColumnPath,
    paths: frozenset[ColumnPath],
    matched: set[ColumnPath],
) -> pa.Field[Any]:
    """Return ``field`` with every recorded path at or below ``path`` non-nullable."""
    out = field
    if path in paths:
        matched.add(path)
        out = out.with_nullable(False)
    if not any(p[: len(path)] == path and len(p) > len(path) for p in paths):
        return out
    t = field.type
    if pa.types.is_struct(t):
        new_type: pa.DataType = pa.struct(
            [
                _restrict_field(t.field(i), (*path, t.field(i).name), paths, matched)
                for i in range(t.num_fields)
            ]
        )
    elif pa.types.is_large_list(t):
        new_type = pa.large_list(
            _restrict_field(t.value_field, (*path, ITEM), paths, matched)
        )
    elif pa.types.is_list(t):
        new_type = pa.list_(
            _restrict_field(t.value_field, (*path, ITEM), paths, matched)
        )
    elif pa.types.is_fixed_size_list(t):
        new_type = pa.list_(
            _restrict_field(t.value_field, (*path, ITEM), paths, matched), t.list_size
        )
    else:
        return out
    return out.with_type(new_type)


def apply_nullability_record(
    schema: pa.Schema, paths: frozenset[ColumnPath]
) -> pa.Schema:
    """Return ``schema`` with every recorded path non-nullable; nothing else changes.

    Raises:
        NullabilityRecordError: If a recorded path is not in ``schema``.
    """
    matched: set[ColumnPath] = set()
    fields = [_restrict_field(field, (field.name,), paths, matched) for field in schema]
    missing = sorted(paths - matched)
    if missing:
        raise NullabilityRecordError(
            "recorded column(s) not in the table: "
            + ", ".join(repr(render_path(p)) for p in missing)
        )
    restricted = pa.schema(fields)
    return restricted.with_metadata(schema.metadata) if schema.metadata else restricted


def _descend(arrays: list[Any], segment: str) -> list[Any]:
    """Return the arrays one path segment below ``arrays`` (a struct field or list items)."""
    arrays = [a.storage if isinstance(a, pa.ExtensionArray) else a for a in arrays]
    if segment == ITEM:
        return [a.flatten() for a in arrays]
    return [a.field(segment) for a in arrays]


def check_no_nulls(
    data: pa.Table | pa.RecordBatch, paths: Iterable[ColumnPath]
) -> None:
    """Raise if ``data`` holds a null at any of ``paths``.

    A struct field is checked over its whole array (Arrow's meaning of non-nullable), a
    list item over the items of the list values. A top-level column costs one null-count
    lookup; a nested path one vectorized pass over its values.

    Raises:
        NullInNonNullableColumnError: Naming the first path, in sorted order, with a null.
    """
    for path in sorted(paths):
        column = data.column(path[0])
        arrays: list[Any] = (
            list(column.chunks) if isinstance(column, pa.ChunkedArray) else [column]
        )
        for segment in path[1:]:
            arrays = _descend(arrays, segment)
        if any(a.null_count for a in arrays):
            raise NullInNonNullableColumnError(render_path(path))


def enforce_nullability_record(
    batch: pa.RecordBatch, paths: frozenset[ColumnPath]
) -> pa.RecordBatch:
    """Check ``batch`` against the record and return it with the recorded nullability.

    This is the per-batch step of every Spiral read path (the eager connector scan, and the
    streaming source of ITL-186).

    Raises:
        NullabilityRecordError: If a recorded path is not in the batch.
        NullInNonNullableColumnError: If a recorded path holds a null.
    """
    target = apply_nullability_record(batch.schema, paths)
    check_no_nulls(batch, paths)
    if target.equals(batch.schema, check_metadata=True):
        return batch
    return batch.cast(target)
