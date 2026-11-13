"""Tests for the element digest shared by array-like value types (ITL-888, ITL-981).

The reference digest below is written independently of Orcapod, with ``struct`` and
``hashlib`` only, from the byte-level definition in ``docs/reference/hashing.md``
(section 8). It is the first step of the ``numpy.ndarray`` value hash, and the
definition other array types (torch) must reproduce.
"""

from __future__ import annotations

import hashlib
import struct

import pytest

from orcapod.logical_types.array_format import (
    NDARRAY_EXTENSION_NAME,
    NDARRAY_OPTIONS,
    array_digest_v1,
    length_prefixed,
)


def reference_digest(tag: str, shape: tuple[int, ...], data: bytes) -> bytes:
    """Independent implementation of the v1 element digest."""
    t = tag.encode("utf-8")
    preimage = struct.pack("<Q", len(t)) + t
    preimage += struct.pack("<Q", len(shape))
    preimage += b"".join(struct.pack("<q", d) for d in shape)
    preimage += struct.pack("<Q", len(data)) + data
    return hashlib.sha256(preimage).digest()


CASES = [
    ("float32", (2, 3), bytes(range(24))),
    ("float64", (), b"\x00" * 8),
    ("int8", (0, 3), b""),
    ("int8", (3, 0), b""),
    ('[["a","int32",[2]],["b","float64"]]', (1,), bytes(16)),
    ("ucs4[2]", (1,), "hé".encode("utf-32-le")),
]


@pytest.mark.parametrize(("tag", "shape", "data"), CASES)
def test_digest_matches_independent_reference(
    tag: str, shape: tuple[int, ...], data: bytes
) -> None:
    """The digest matches an independent stdlib reference implementation."""
    assert array_digest_v1(tag, shape, data) == reference_digest(tag, shape, data)


def test_digest_is_32_bytes() -> None:
    """The digest is 32 bytes long."""
    assert len(array_digest_v1("float32", (1,), b"\x00" * 4)) == 32


def test_digest_accepts_any_buffer() -> None:
    """The digest accepts bytes, memoryview and bytearray data and any shape sequence."""
    data = bytes(range(24))
    expected = array_digest_v1("float32", (6,), data)
    assert array_digest_v1("float32", (6,), memoryview(data)) == expected
    assert array_digest_v1("float32", [6], bytearray(data)) == expected


def test_digest_separates_every_field() -> None:
    """Every field of the preimage contributes to the digest."""
    assert array_digest_v1("int8", (0, 3), b"") != array_digest_v1("int8", (3, 0), b"")
    assert array_digest_v1("int8", (), b"\x01") != array_digest_v1(
        "int8", (1,), b"\x01"
    )
    assert array_digest_v1("int8", (2,), b"\x01\x02") != array_digest_v1(
        "uint8", (2,), b"\x01\x02"
    )
    assert array_digest_v1("int8", (2,), b"\x01\x02") != array_digest_v1(
        "int8", (2,), b"\x02\x01"
    )


def test_digest_rejects_negative_dimension() -> None:
    """The digest rejects a negative dimension."""
    with pytest.raises(ValueError, match="dimension"):
        _ = array_digest_v1("int8", (-1,), b"")


def test_length_prefixed_text() -> None:
    """A text field is its UTF-8 bytes after their 8-byte little-endian length."""
    assert length_prefixed("") == struct.pack("<Q", 0)
    assert length_prefixed("hé") == struct.pack("<Q", 3) + "hé".encode()


def test_ndarray_constants() -> None:
    """The array type's extension name and the properties its hash can ignore."""
    assert NDARRAY_EXTENSION_NAME == "orcapod.numpy.ndarray"
    assert NDARRAY_OPTIONS == {"byte_order", "memory_order", "struct_layout"}
