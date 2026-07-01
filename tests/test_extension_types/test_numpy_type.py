"""Tests for LogicalNumpyArray."""
from __future__ import annotations

import io

import numpy as np
import pyarrow as pa
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol


class TestLogicalNumpyArrayProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert isinstance(LogicalNumpyArray(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().logical_type_name == "numpy.ndarray"

    def test_python_type(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().python_type is np.ndarray

    def test_arrow_ext_name(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().get_arrow_extension_type().extension_name == "numpy.ndarray"

    def test_arrow_ext_storage_type(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        assert LogicalNumpyArray().get_arrow_extension_type().storage_type == pa.large_binary()

    def test_arrow_ext_is_cached(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        lt = LogicalNumpyArray()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_cached(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        lt = LogicalNumpyArray()
        assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


class TestLogicalNumpyArrayStorage:
    def test_python_to_storage_returns_bytes(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        arr = np.array([1.0, 2.0, 3.0])
        result = LogicalNumpyArray().python_to_storage(arr)
        assert isinstance(result, bytes)

    def test_object_dtype_raises_before_save(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        arr = np.array([1, "hello", None], dtype=object)
        with pytest.raises(ValueError, match="object-dtype"):
            LogicalNumpyArray().python_to_storage(arr)

    def test_structured_with_object_field_raises(self):
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        arr = np.zeros(3, dtype=[("label", object), ("value", np.int32)])
        with pytest.raises(ValueError):
            LogicalNumpyArray().python_to_storage(arr)


class TestLogicalNumpyArrayRoundTrip:
    def _rt(self, arr: np.ndarray) -> np.ndarray:
        from orcapod.extension_types.numpy_type import LogicalNumpyArray
        lt = LogicalNumpyArray()
        return lt.storage_to_python(lt.python_to_storage(arr))

    def test_1d_float64(self):
        arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
        assert np.array_equal(self._rt(arr), arr)
        assert self._rt(arr).dtype == arr.dtype

    def test_2d_int32(self):
        arr = np.array([[1, 2], [3, 4]], dtype=np.int32)
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.shape == arr.shape
        assert result.dtype == arr.dtype

    def test_3d_uint8(self):
        arr = np.zeros((2, 3, 4), dtype=np.uint8)
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.shape == arr.shape

    def test_zero_size(self):
        arr = np.array([], dtype=np.float32)
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.dtype == arr.dtype

    def test_single_element(self):
        arr = np.array([42], dtype=np.int64)
        result = self._rt(arr)
        assert np.array_equal(result, arr)

    def test_structured_record_array(self):
        dt = np.dtype([("x", np.float64), ("y", np.int32)])
        arr = np.array([(1.0, 2), (3.0, 4)], dtype=dt)
        result = self._rt(arr)
        assert result.dtype == arr.dtype
        assert np.array_equal(result["x"], arr["x"])
        assert np.array_equal(result["y"], arr["y"])

    def test_fortran_order(self):
        arr = np.asfortranarray(np.array([[1, 2], [3, 4]], dtype=np.float64))
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.flags["F_CONTIGUOUS"]
