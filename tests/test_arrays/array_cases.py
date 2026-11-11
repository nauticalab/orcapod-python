"""Shared arrays and assertions for the ``numpy.ndarray`` value tests (ITL-888).

Every array here is little-endian, C-ordered, a plain ``np.ndarray`` and, when
structured, packed with its fields in declared order (field order is part of an
array's identity). Such arrays read back unchanged.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import numpy.typing as npt

ARRAYS: dict[str, npt.NDArray[Any]] = {
    "bool": np.array([[True, False, True], [False, False, True]]),
    "int8": np.array([-128, -1, 0, 1, 127], dtype=np.int8),
    "uint64": np.array([0, 1, 2**64 - 1], dtype=np.uint64),
    "float16": np.array([0.5, -2.0, 65504.0], dtype=np.float16),
    "float32": np.arange(6, dtype=np.float32).reshape(2, 3),
    "float64_special": np.array([np.nan, np.inf, -np.inf, -0.0, 0.0, 1e-308]),
    "complex64": np.array([1 + 2j, -3.5 - 0.5j], dtype=np.complex64),
    "complex128": np.array([[1 + 2j], [3 - 4j]], dtype=np.complex128),
    "datetime64_ns": np.array(
        ["2026-11-10T12:00:00.000000001", "1970-01-01"], dtype="datetime64[ns]"
    ),
    "datetime64_D": np.array(["2026-11-16", "2000-02-29"], dtype="datetime64[D]"),
    "timedelta64_s": np.array([0, -5, 86400], dtype="timedelta64[s]"),
    "unicode": np.array(["héllo", "", "ok"], dtype="<U5"),
    "bytes": np.array([b"abc", b"", b"z"], dtype="S3"),
    "void": np.array([b"\x00\x01\x02\x03", b"\xff\xfe\xfd\xfc"], dtype="V4"),
    "structured": np.array([(1, 0.5), (-2, 1.5)], dtype=[("a", "<i4"), ("b", "<f8")]),
    "structured_unsorted": np.array(
        [(0.5, 1), (1.5, -2)], dtype=[("b", "<f8"), ("a", "<i4")]
    ),
    "structured_nested": np.array(
        [((1, 2), 3.5)], dtype=[("s", [("y", "<i2"), ("x", "<i2")]), ("a", "<f8")]
    ),
    "zero_d": np.array(2.5),
    "empty_0x3": np.zeros((0, 3), dtype=np.float32),
    "empty_3x0": np.zeros((3, 0), dtype=np.float32),
    "three_d": np.arange(24, dtype=np.int32).reshape(2, 3, 4),
}
"""One array per dtype class and shape class."""

NUMERIC = ("int8", "float32", "complex128", "zero_d", "empty_0x3", "three_d")
"""Keys of arrays that support ``x * 2`` and ``x.sum()``."""


def assert_arrays_identical(got: object, expected: npt.NDArray[Any]) -> None:
    """Assert ``got`` is a plain C-ordered ndarray equal to ``expected`` bit for bit.

    Args:
        got: The value read back.
        expected: The value written.
    """
    assert type(got) is np.ndarray, f"expected np.ndarray, got {type(got)!r}"
    assert got.dtype == expected.dtype, f"dtype {got.dtype!r} != {expected.dtype!r}"
    assert got.shape == expected.shape, f"shape {got.shape} != {expected.shape}"
    assert got.flags.c_contiguous
    assert got.tobytes() == expected.tobytes()


def assert_same_arrays_any_order(
    got: list[Any], expected: list[npt.NDArray[Any]]
) -> None:
    """Assert two lists hold the same arrays, each bit for bit, in any order.

    Args:
        got: Arrays read back.
        expected: Arrays written.
    """
    assert len(got) == len(expected)
    remaining = list(expected)
    for g in got:
        for i, e in enumerate(remaining):
            if (
                type(g) is np.ndarray
                and g.dtype == e.dtype
                and g.shape == e.shape
                and g.tobytes() == e.tobytes()
            ):
                del remaining[i]
                break
        else:
            raise AssertionError(f"unexpected array {g!r}")
