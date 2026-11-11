"""Array storage struct and value digest shared by array-like logical types.

This module defines the parts of Orcapod's array format that do not depend on
any array library: the Arrow struct an array value is stored as, and the
version-1 value digest computed from a stored row. ``numpy.ndarray`` uses them
today (``numpy_type.LogicalNumpyArray``); other array types (such as
``torch.Tensor``) reuse them under their own extension name and hash label.

Storage struct (one value per row):

    struct<
      dtype: large_string,              # Orcapod dtype tag
      shape: large_list<item: int64>,   # dimensions, outermost first
      data:  large_binary,              # element bytes, C order
    >

The children are nullable at the Arrow level only because Polars drops nested
non-null flags when it converts back to Arrow (ITL-842), and the extension type
would then no longer match. A stored array value never has a null child: the
logical type refuses one on write and on read. A null array is a null struct.

Value digest, version 1. For a row with dtype tag ``T``, shape
``(d0, ..., d{n-1})`` and data bytes ``D``:

    P = u64le(len(utf8(T))) || utf8(T)
        || u64le(n) || i64le(d0) || ... || i64le(d{n-1})
        || u64le(len(D)) || D
    digest = SHA-256(P)

``u64le`` and ``i64le`` are 8-byte little-endian unsigned and signed integers.
Every field is length-prefixed, so distinct rows never share a preimage. The
digest depends only on the three stored fields, so it can be recomputed from a
stored table with any SHA-256 implementation.

This module must not import NumPy.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence

import pyarrow as pa

ARRAY_FORMAT_VERSION = 1
"""Version of the array storage struct; recorded in the extension metadata."""

ARRAY_FORMAT_METADATA = b'{"version":1}'
"""``ARROW:extension:metadata`` of every array-format extension type (canonical JSON).

A reader rejects a stored type whose metadata differs, so a later storage version
is never decoded as this one.
"""


ARRAY_STORAGE_TYPE: pa.StructType = pa.struct(
    [
        pa.field("dtype", pa.large_string()),
        pa.field("shape", pa.large_list(pa.int64())),
        pa.field("data", pa.large_binary()),
    ]
)
"""The Arrow struct one array value is stored as (see the module docstring)."""

_U64 = struct.Struct("<Q")
_I64 = struct.Struct("<q")


def array_digest_v1(
    tag: str, shape: Sequence[int], data: bytes | bytearray | memoryview
) -> bytes:
    """Compute the version-1 value digest of a stored array row.

    The buffer is fed to SHA-256 directly, so a large array is not copied.

    Args:
        tag: The Orcapod dtype tag, as stored in the ``dtype`` field.
        shape: The dimensions, outermost first, as stored in the ``shape`` field.
        data: The element bytes, as stored in the ``data`` field. Any object
            exposing a C-contiguous buffer is accepted.

    Returns:
        The 32-byte SHA-256 digest defined in the module docstring.

    Raises:
        ValueError: If a dimension is negative or not an integer.
    """
    tag_bytes = tag.encode("utf-8")
    h = hashlib.sha256()
    h.update(_U64.pack(len(tag_bytes)))
    h.update(tag_bytes)
    h.update(_U64.pack(len(shape)))
    for dim in shape:
        if isinstance(dim, bool) or not isinstance(dim, int) or dim < 0:
            raise ValueError(
                f"array_digest_v1: every dimension must be a non-negative int, got {dim!r} "
                f"in shape {tuple(shape)!r}"
            )
        h.update(_I64.pack(dim))
    view = memoryview(data)
    h.update(_U64.pack(view.nbytes))
    if view.nbytes:
        h.update(view)
    return h.digest()
