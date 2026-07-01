"""Tests for NumpyArrayHandler."""
from __future__ import annotations

import numpy as np
import pytest

from orcapod.hashing.semantic_hashing.builtin_handlers import NumpyArrayHandler
from orcapod.types import ContentHash


class TestNumpyArrayHandler:
    def test_returns_content_hash(self):
        handler = NumpyArrayHandler()
        arr = np.array([1.0, 2.0, 3.0])
        result = handler.handle(arr, hasher=None)
        assert isinstance(result, ContentHash)
        assert result.method == "sha256"

    def test_same_content_same_hash(self):
        handler = NumpyArrayHandler()
        arr = np.array([1, 2, 3], dtype=np.int32)
        assert handler.handle(arr, None) == handler.handle(arr.copy(), None)

    def test_different_dtype_different_hash(self):
        handler = NumpyArrayHandler()
        a = np.array([1, 2, 3], dtype=np.int32)
        b = np.array([1, 2, 3], dtype=np.float64)
        assert handler.handle(a, None) != handler.handle(b, None)

    def test_same_values_different_shape_different_hash(self):
        handler = NumpyArrayHandler()
        a = np.array([1, 2, 3, 4], dtype=np.int32)
        b = np.array([[1, 2], [3, 4]], dtype=np.int32)
        assert handler.handle(a, None) != handler.handle(b, None)

    def test_structured_array_field_names_affect_hash(self):
        handler = NumpyArrayHandler()
        dt_xy = np.dtype([("x", np.float64), ("y", np.int32)])
        dt_ab = np.dtype([("a", np.float64), ("b", np.int32)])
        arr_xy = np.array([(1.0, 2)], dtype=dt_xy)
        arr_ab = np.array([(1.0, 2)], dtype=dt_ab)
        # Same values, same field types, different field names → different hash
        assert handler.handle(arr_xy, None) != handler.handle(arr_ab, None)

    def test_raises_type_error_for_non_ndarray(self):
        handler = NumpyArrayHandler()
        with pytest.raises(TypeError, match="numpy.ndarray"):
            handler.handle([1, 2, 3], hasher=None)

    def test_raises_value_error_for_object_dtype(self):
        handler = NumpyArrayHandler()
        arr = np.array([1, "hello", None], dtype=object)
        with pytest.raises(ValueError, match="object-dtype"):
            handler.handle(arr, hasher=None)
