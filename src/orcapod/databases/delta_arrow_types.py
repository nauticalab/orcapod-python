"""Keep a column's Arrow type through a Delta table (ITL-928).

Delta Lake has no unsigned integer, no nanosecond, second or millisecond timestamp,
no zone other than UTC, and no ``date64``, ``time`` or ``duration`` type. The Delta
writer converts the first kinds (unsigned to the signed type of the same width, which
fails for a value above the signed maximum; every timestamp to microseconds, dropping
nanosecond digits; a zone to UTC) and cannot store the others at all.

A Delta database must give back the type it was given: a tag read back with another type
changes every record id computed from it. So before a write, a column whose type Delta
would not keep is recorded in its top-level field metadata (``ARROW_TYPE_KEY``: the
column's whole Arrow type, IPC-serialized; Delta keeps top-level field metadata but not
that of nested fields), and the values Delta cannot hold losslessly are written as
their integer form:

* an unsigned integer as the signed integer of the same width, bit for bit;
* ``timestamp[ns]``, ``date64``, ``time32``, ``time64`` and ``duration`` as their
  integer value (``int64``, or ``int32`` for ``time32``).

``timestamp[s]``, ``timestamp[ms]`` and a zone other than UTC are left to Delta's own
conversion, which is lossless. A read casts each recorded column back to its recorded
type (large-normalized). A column whose type has an extension (logical) type anywhere is
left as it is: its type is restored from its extension metadata.

A table written before this existed has no recorded type and reads back with Delta's
types; such a database must be rebuilt.
"""

from __future__ import annotations

import base64
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

# Field metadata key holding a column's original Arrow type.
ARROW_TYPE_KEY = b"orcapod:arrow_type"


def _has_extension_type(arrow_type: pa.DataType) -> bool:
    """Return whether ``arrow_type`` is or contains an extension type."""
    if isinstance(arrow_type, pa.ExtensionType):
        return True
    return any(
        _has_extension_type(arrow_type.field(i).type)
        for i in range(arrow_type.num_fields)
    )


def _stored_form(arrow_type: pa.DataType) -> pa.DataType:
    """Return the type a column of ``arrow_type`` is written as, at every depth.

    Args:
        arrow_type: A type with no extension type in it.

    Returns:
        ``arrow_type`` with each unsigned integer replaced by the signed integer of
        the same width and each ``timestamp[ns]``, ``date64``, ``time`` and
        ``duration`` by its integer type; every other part unchanged.
    """
    t = pa.types
    if t.is_unsigned_integer(arrow_type):
        return {8: pa.int8(), 16: pa.int16(), 32: pa.int32(), 64: pa.int64()}[
            arrow_type.bit_width
        ]
    if (t.is_timestamp(arrow_type) and arrow_type.unit == "ns") or (
        t.is_date64(arrow_type) or t.is_time64(arrow_type) or t.is_duration(arrow_type)
    ):
        return pa.int64()
    if t.is_time32(arrow_type):
        return pa.int32()
    if t.is_large_list(arrow_type):
        return pa.large_list(_stored_field(arrow_type.value_field))
    if t.is_list(arrow_type):
        return pa.list_(_stored_field(arrow_type.value_field))
    if t.is_fixed_size_list(arrow_type):
        return pa.list_(_stored_field(arrow_type.value_field), arrow_type.list_size)
    if t.is_struct(arrow_type):
        return pa.struct([_stored_field(f) for f in arrow_type])
    if t.is_map(arrow_type):
        # pyarrow accepts key and item Fields here; its stubs only list types.
        return pa.map_(  # pyright: ignore[reportCallIssue]
            _stored_field(arrow_type.key_field),
            _stored_field(arrow_type.item_field),
            keys_sorted=arrow_type.keys_sorted,
        )
    return arrow_type


def _stored_field(field: "pa.Field[Any]") -> "pa.Field[Any]":
    """Return ``field`` with its type in the form it is written as."""
    return field.with_type(_stored_form(field.type))


def _serialize(arrow_type: pa.DataType) -> bytes:
    """Return ``arrow_type`` as field metadata text (base64 of an IPC schema)."""
    return base64.b64encode(
        pa.schema([pa.field("type", arrow_type)]).serialize().to_pybytes()
    )


def _deserialize(text: bytes) -> pa.DataType:
    """Return the Arrow type ``_serialize`` wrote as ``text``."""
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(text))).field(0).type


def _is_recorded(arrow_type: pa.DataType) -> bool:
    """Return whether a column of ``arrow_type`` needs its type recorded.

    It does when it has, at any depth, a part Delta would not give back: an unsigned
    integer, a timestamp in another unit than microseconds or with a zone other than
    UTC, or a ``date64``, ``time`` or ``duration``.
    """
    t = pa.types
    if (
        t.is_unsigned_integer(arrow_type)
        or t.is_date64(arrow_type)
        or t.is_time(arrow_type)
        or t.is_duration(arrow_type)
    ):
        return True
    if t.is_timestamp(arrow_type):
        return arrow_type.unit != "us" or arrow_type.tz not in (None, "UTC")
    return any(
        _is_recorded(arrow_type.field(i).type) for i in range(arrow_type.num_fields)
    )


def to_delta_layout(table: pa.Table) -> pa.Table:
    """Return ``table`` as it is written to Delta, each changed column's type recorded.

    Args:
        table: The rows about to be written.

    Returns:
        The table with each recorded column (``_is_recorded``) converted to its
        stored form (``_stored_form``, values unchanged bit for bit) and its original
        type in its field metadata; every other column unchanged.
    """
    for index, field in enumerate(table.schema):
        if _has_extension_type(field.type) or not _is_recorded(field.type):
            continue
        metadata = dict(field.metadata or {})
        metadata[ARROW_TYPE_KEY] = _serialize(field.type)
        stored_type = _stored_form(field.type)
        # An integer view of the same values: a wrap-around cast keeps the bits.
        column = table.column(index).cast(stored_type, safe=False)
        table = table.set_column(
            index,
            pa.field(field.name, stored_type, field.nullable, metadata),
            column,
        )
    return table


def from_delta_layout(table: pa.Table) -> pa.Table:
    """Return rows read from Delta with each recorded column cast back to its type.

    Args:
        table: Rows as read from the Delta table.

    Returns:
        The rows with every column that carries ``ARROW_TYPE_KEY`` in its type
        (large-normalized) and without that key; other columns unchanged.
    """
    for index, field in enumerate(table.schema):
        metadata = field.metadata or {}
        if ARROW_TYPE_KEY not in metadata:
            continue
        original = arrow_utils.normalize_to_large_types(
            _deserialize(metadata[ARROW_TYPE_KEY])
        )
        rest = {k: v for k, v in metadata.items() if k != ARROW_TYPE_KEY}
        table = table.set_column(
            index,
            pa.field(field.name, original, field.nullable, rest or None),
            # Values were written from this type, so the cast back is exact.
            table.column(index).cast(original, safe=False),
        )
    return table


def recorded_type(
    metadata: "Mapping[bytes, bytes] | Mapping[str, str] | None",
) -> pa.DataType | None:
    """Return the Arrow type recorded in a field's metadata, or ``None``.

    Args:
        metadata: The field metadata, as pyarrow gives it (bytes) or as the Delta
            log gives it (text).

    Returns:
        The recorded type, or ``None`` when the field has none.
    """
    for key, value in (metadata or {}).items():
        if (key.encode() if isinstance(key, str) else key) == ARROW_TYPE_KEY:
            return _deserialize(value.encode() if isinstance(value, str) else value)
    return None


def first_column_out_of_stored_range(
    table: pa.Table, names: "Collection[str]"
) -> str | None:
    """Return the first named column with a value its written form cannot hold.

    An unsigned integer is written as the signed integer of the same width, bit for
    bit; a column that records its type reads it back exactly. A column whose stored
    type is not recorded (written before types were recorded) would read such a value
    back as a negative number, so the caller refuses it.

    Args:
        table: The rows about to be written, with their own types.
        names: The columns to check: those stored without a recorded type.

    Returns:
        The first such column's name, or ``None``.
    """
    for name in names:
        index = table.schema.get_field_index(name)
        if index < 0:
            continue
        arrow_type = table.schema.field(index).type
        if _has_extension_type(arrow_type) or not _is_recorded(arrow_type):
            continue
        try:
            table.column(index).cast(_stored_form(arrow_type), safe=True)
        except pa.ArrowInvalid:
            return name
    return None
