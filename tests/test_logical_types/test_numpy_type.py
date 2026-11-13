"""Tests for LogicalNumpyArray."""

from __future__ import annotations

import numpy as np
import pytest

from orcapod.logical_types.protocols import LogicalTypeProtocol


class TestLogicalNumpyArrayProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        assert isinstance(LogicalNumpyArray(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        """The logical type name is ``orcapod.numpy.ndarray``."""
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        assert LogicalNumpyArray().logical_type_name == "orcapod.numpy.ndarray"

    def test_python_type(self):
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        assert LogicalNumpyArray().python_type is np.ndarray

    def test_arrow_ext_name(self):
        """The Arrow extension name is ``orcapod.numpy.ndarray``."""
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        assert (
            LogicalNumpyArray().get_arrow_extension_type().extension_name
            == "orcapod.numpy.ndarray"
        )

    def test_arrow_ext_storage_type(self):
        """The Arrow storage type is ``large_binary``: one ``.npy`` file per value."""
        import pyarrow as pa

        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        assert (
            LogicalNumpyArray().get_arrow_extension_type().storage_type
            == pa.large_binary()
        )

    def test_ignore_list_is_the_extension_metadata(self):
        """The ignore list is written into the extension metadata and makes another type."""
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        strict = LogicalNumpyArray().get_arrow_extension_type()
        lenient = LogicalNumpyArray(
            ["memory_order", "byte_order"]
        ).get_arrow_extension_type()
        assert strict.__arrow_ext_serialize__() == b'{"ignore":[],"version":1}'
        assert (
            lenient.__arrow_ext_serialize__()
            == b'{"ignore":["byte_order","memory_order"],"version":1}'
        )
        assert strict != lenient

    def test_unknown_ignore_name_raises(self):
        """A property name the array hash cannot ignore is refused."""
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        with pytest.raises(ValueError, match="'strides'"):
            LogicalNumpyArray(["strides"])

    def test_arrow_ext_is_cached(self):
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        lt = LogicalNumpyArray()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_cached(self):
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        lt = LogicalNumpyArray()
        assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


class TestLogicalNumpyArrayStorage:
    def test_python_to_storage_returns_a_npy_file(self):
        """``python_to_storage`` returns a ``.npy`` file of format 3.0 that NumPy loads."""
        import io

        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        arr = np.array([1.0, 2.0, 3.0])
        result = LogicalNumpyArray().python_to_storage(arr)
        assert isinstance(result, bytes)
        assert result[:8] == b"\x93NUMPY\x03\x00"
        assert np.array_equal(np.load(io.BytesIO(result), allow_pickle=False), arr)

    def test_object_dtype_raises(self):
        """``python_to_storage`` refuses object arrays."""
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        arr = np.array([1, "hello", None], dtype=object)
        with pytest.raises(ValueError, match="object arrays"):
            LogicalNumpyArray().python_to_storage(arr)

    def test_structured_with_object_field_raises(self):
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

        arr = np.zeros(3, dtype=[("label", object), ("value", np.int32)])
        with pytest.raises(ValueError):
            LogicalNumpyArray().python_to_storage(arr)


class TestLogicalNumpyArrayRoundTrip:
    def _rt(self, arr: np.ndarray) -> np.ndarray:
        from orcapod.logical_types.numpy_type import LogicalNumpyArray

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

    def test_fortran_order_reads_back_fortran_order(self):
        """A Fortran-ordered array reads back Fortran-contiguous with the same values."""
        arr = np.asfortranarray(np.array([[1, 2], [3, 4]], dtype=np.float64))
        result = self._rt(arr)
        assert np.array_equal(result, arr)
        assert result.flags["F_CONTIGUOUS"] and not result.flags["C_CONTIGUOUS"]
