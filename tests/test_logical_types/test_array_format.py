"""Tests for the shared array storage struct and value digest (ITL-888).

The reference digest below is written independently of Orcapod, with ``struct`` and
``hashlib`` only, from the byte-level definition in
``superpowers/specs/2026-11-10-array-format-v0.3.md`` (section C1). It is the
definition other implementations (starfix, torch) must reproduce.
"""

from __future__ import annotations

import hashlib
import struct

import pyarrow as pa
import pytest

from orcapod.logical_types.array_format import ARRAY_STORAGE_TYPE, array_digest_v1


def reference_digest(tag: str, shape: tuple[int, ...], data: bytes) -> bytes:
    """Independent implementation of the v1 array digest."""
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
    """Every field of the stored row contributes to the digest."""
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


def test_storage_type() -> None:
    """The storage type is the dtype, shape and data struct of the spec."""
    # Children are nullable at the Arrow level: Polars drops nested non-null flags
    # (ITL-842), so non-null is enforced by the logical type instead.
    assert ARRAY_STORAGE_TYPE == pa.struct(
        [
            pa.field("dtype", pa.large_string()),
            pa.field("shape", pa.large_list(pa.int64())),
            pa.field("data", pa.large_binary()),
        ]
    )
