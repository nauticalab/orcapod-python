"""Element digest shared by array-like value types, and the ndarray constants.

An array-like value (a ``numpy.ndarray``, a tensor) is hashed in two steps.
This module holds the first, which does not depend on any array library: the
*element digest* of a dtype tag, a shape and the element bytes in a fixed
layout. Each type builds its value hash on top of it (``numpy_type`` for
``numpy.ndarray``), adding its own leading type text and one field per property
the element digest does not cover.

Element digest, version 1. For a dtype tag ``T``, shape ``(d0, ..., d{n-1})``
and element bytes ``D`` (C order, little-endian, structured fields packed in
declared order):

    P = u64le(len(utf8(T))) || utf8(T)
        || u64le(n) || i64le(d0) || ... || i64le(d{n-1})
        || u64le(len(D)) || D
    E = SHA-256(P)

``u64le`` and ``i64le`` are 8-byte little-endian unsigned and signed integers.
Every field is length-prefixed, so distinct inputs never share a preimage.

This module must not import NumPy or Arrow.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence

NDARRAY_EXTENSION_NAME = "orcapod.numpy.ndarray"
"""Arrow extension name (and logical type name) of ``numpy.ndarray`` values.

It is also the leading type text of the ndarray value hash.
"""

NDARRAY_OPTIONS = frozenset({"byte_order", "memory_order", "struct_layout"})
"""The property names the ndarray value hash can ignore (see ``numpy_type``)."""

_U64 = struct.Struct("<Q")
_I64 = struct.Struct("<q")


def length_prefixed(text: str) -> bytes:
    """Return ``u64le(len(utf8(text))) || utf8(text)``.

    Args:
        text: The text of one preimage field.

    Returns:
        The length-prefixed UTF-8 bytes.
    """
    data = text.encode("utf-8")
    return _U64.pack(len(data)) + data


def array_digest_v1(
    tag: str, shape: Sequence[int], data: bytes | bytearray | memoryview
) -> bytes:
    """Compute the version-1 element digest of an array.

    The buffer is fed to SHA-256 directly, so a large array is not copied.

    Args:
        tag: The Orcapod dtype tag.
        shape: The dimensions, outermost first.
        data: The element bytes in C order, little-endian, packed. Any object
            exposing a C-contiguous buffer is accepted.

    Returns:
        The 32-byte SHA-256 digest defined in the module docstring.

    Raises:
        ValueError: If a dimension is negative or not an integer.
    """
    h = hashlib.sha256()
    h.update(length_prefixed(tag))
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
