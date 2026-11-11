"""Array values round trip exactly through sources and function pods (ITL-888)."""

from __future__ import annotations

from typing import Any

import numpy as np
import numpy.typing as npt
import pytest

from orcapod import function_pod
from orcapod.sources import DictSource
from tests.test_arrays.array_cases import ARRAYS, assert_arrays_identical


def _source(arrays: dict[str, Any]) -> DictSource:
    return DictSource(
        data=[{"k": k, "x": v} for k, v in arrays.items()],
        tag_columns=["k"],
        source_id="arrays",
    )


def _read(stream: Any, column: str) -> dict[str, Any]:
    return {t.as_dict()["k"]: d.as_dict()[column] for t, d in stream.iter_data()}


@pytest.mark.parametrize("name", sorted(ARRAYS))
def test_source_round_trip(name: str) -> None:
    """Each array case reads back from a source exactly."""
    got = _read(_source({name: ARRAYS[name]}), "x")
    assert_arrays_identical(got[name], ARRAYS[name])


def test_one_column_holds_every_dtype_and_shape() -> None:
    """One column holding every dtype and shape reads back exactly."""
    got = _read(_source(ARRAYS), "x")
    for name, expected in ARRAYS.items():
        assert_arrays_identical(got[name], expected)


def test_function_pod_input_and_output() -> None:
    """A function pod receives and returns arrays exactly."""
    seen: dict[str, npt.NDArray[Any]] = {}

    @function_pod(output_keys="y")
    def identity(x: np.ndarray) -> np.ndarray:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
        seen[str(x.dtype) + str(x.shape)] = x
        return x.copy()

    got = _read(identity.pod(_source(ARRAYS)), "y")
    for name, expected in ARRAYS.items():
        assert_arrays_identical(got[name], expected)
    assert len(seen) == len({str(a.dtype) + str(a.shape) for a in ARRAYS.values()})


def test_optional_array_column_keeps_none() -> None:
    """An optional array output keeps ``None`` and the returned arrays."""

    @function_pod(output_keys="y")
    def maybe(x: np.ndarray) -> np.ndarray | None:  # pyright: ignore[reportMissingTypeArgument]  # the bare np.ndarray annotation is under test
        return None if x.size == 0 else x

    got = _read(maybe.pod(_source(ARRAYS)), "y")
    for name, expected in ARRAYS.items():
        if expected.size == 0:
            assert got[name] is None
        else:
            assert_arrays_identical(got[name], expected)


def test_equal_arrays_have_equal_content_hashes() -> None:
    """Equal arrays in separate sources have equal content hashes."""
    a = _source({"r": np.arange(6, dtype=np.float32).reshape(2, 3)})
    b = _source({"r": np.arange(6, dtype=np.float32).reshape(2, 3).copy()})
    (_, da), (_, db) = next(iter(a.iter_data())), next(iter(b.iter_data()))
    assert da.content_hash() == db.content_hash()


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("float32", "three_d"),
        ("empty_0x3", "empty_3x0"),
        ("int8", "uint64"),
        ("float64_special", "zero_d"),
    ],
)
def test_different_arrays_have_different_content_hashes(left: str, right: str) -> None:
    """Different arrays have different content hashes."""
    (_, da), (_, db) = (
        next(iter(_source({"r": ARRAYS[n]}).iter_data())) for n in (left, right)
    )
    assert da.content_hash() != db.content_hash()


def test_dtype_is_identity() -> None:
    """The dtype is part of an array's identity."""
    a = np.arange(4, dtype=np.int32)
    (_, da), (_, db) = (
        next(iter(_source({"r": v}).iter_data())) for v in (a, a.astype(np.int64))
    )
    assert da.content_hash() != db.content_hash()


def test_shape_is_identity() -> None:
    """The shape is part of an array's identity."""
    a = np.arange(6, dtype=np.int32)
    (_, da), (_, db) = (
        next(iter(_source({"r": v}).iter_data()))
        for v in (a.reshape(2, 3), a.reshape(3, 2))
    )
    assert da.content_hash() != db.content_hash()
