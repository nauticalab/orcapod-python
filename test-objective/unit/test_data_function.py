"""Specification-derived tests for PythonDataFunction and CachedDataFunction.

Tests based on DataFunctionProtocol and documented behaviors.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams.tag_data import Data
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import Schema


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def add(x: int, y: int) -> int:
    return x + y


def to_upper(name: str) -> str:
    return name.upper()


def return_none(x: int) -> int:
    return None  # type: ignore[return-value]


def variadic_func(*args: int) -> int:
    return sum(args)


def kwargs_func(**kwargs: int) -> int:
    return sum(kwargs.values())


def no_annotations(x, y):
    return x + y


# ---------------------------------------------------------------------------
# PythonDataFunction construction
# ---------------------------------------------------------------------------


class TestPythonDataFunctionConstruction:
    """Per design, PythonDataFunction wraps a plain Python function."""

    def test_from_simple_function(self):
        pf = PythonDataFunction(double, output_keys="result")
        assert pf.canonical_function_name == "double"

    def test_infers_input_schema_from_signature(self):
        pf = PythonDataFunction(double, output_keys="result")
        input_schema = pf.input_data_schema
        assert "x" in input_schema
        assert input_schema["x"] is int

    def test_infers_output_schema(self):
        pf = PythonDataFunction(double, output_keys="result")
        output_schema = pf.output_data_schema
        assert "result" in output_schema

    def test_multi_input_schema(self):
        pf = PythonDataFunction(add, output_keys="result")
        input_schema = pf.input_data_schema
        assert "x" in input_schema
        assert "y" in input_schema

    def test_rejects_variadic_args(self):
        with pytest.raises((ValueError, TypeError)):
            PythonDataFunction(variadic_func, output_keys="result")

    def test_rejects_variadic_kwargs(self):
        with pytest.raises((ValueError, TypeError)):
            PythonDataFunction(kwargs_func, output_keys="result")

    def test_explicit_function_name(self):
        pf = PythonDataFunction(
            double, output_keys="result", function_name="my_doubler"
        )
        assert pf.canonical_function_name == "my_doubler"

    def test_version_parsing(self):
        pf = PythonDataFunction(double, output_keys="result", version="v1.2")
        assert pf.major_version == 1
        assert pf.minor_version_string == "2"

    def test_default_version(self):
        pf = PythonDataFunction(double, output_keys="result")
        assert pf.major_version == 0


# ---------------------------------------------------------------------------
# PythonDataFunction execution
# ---------------------------------------------------------------------------


class TestPythonDataFunctionExecution:
    """Per DataFunctionProtocol, call() applies function to data data."""

    def test_call_transforms_data(self):
        pf = PythonDataFunction(double, output_keys="result")
        data = Data({"x": 5})
        result = pf.call(data)
        assert result is not None
        assert result["result"] == 10

    def test_call_multi_input(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = Data({"x": 3, "y": 7})
        result = pf.call(data)
        assert result is not None
        assert result["result"] == 10

    def test_call_returns_none_propagates(self):
        pf = PythonDataFunction(return_none, output_keys="result")
        data = Data({"x": 5})
        result = pf.call(data)
        # When function returns None, it's wrapped: {"result": None}
        assert result["result"] is None

    def test_direct_call_bypasses_executor(self):
        pf = PythonDataFunction(double, output_keys="result")
        data = Data({"x": 5})
        result = pf.direct_call(data)
        assert result is not None
        assert result["result"] == 10


# ---------------------------------------------------------------------------
# PythonDataFunction hashing
# ---------------------------------------------------------------------------


class TestPythonDataFunctionHashing:
    """Per ContentIdentifiableProtocol, hash is deterministic and changes
    with function content."""

    def test_content_hash_deterministic(self):
        pf1 = PythonDataFunction(double, output_keys="result")
        pf2 = PythonDataFunction(double, output_keys="result")
        assert pf1.content_hash() == pf2.content_hash()

    def test_content_hash_changes_with_different_function(self):
        pf1 = PythonDataFunction(double, output_keys="result")
        pf2 = PythonDataFunction(to_upper, output_keys="result")
        assert pf1.content_hash() != pf2.content_hash()

    def test_pipeline_hash_schema_based(self):
        pf = PythonDataFunction(double, output_keys="result")
        ph = pf.pipeline_hash()
        assert ph is not None


# ---------------------------------------------------------------------------
# CachedDataFunction
# ---------------------------------------------------------------------------


class TestCachedDataFunction:
    """Per design, CachedDataFunction wraps a DataFunction and caches
    results in an ArrowDatabaseProtocol."""

    def test_cache_miss_computes_and_stores(self):
        db = InMemoryArrowDatabase()
        inner_pf = PythonDataFunction(double, output_keys="result")
        cached_pf = CachedDataFunction(inner_pf, result_database=db)
        data = Data({"x": 5})
        result = cached_pf.call(data)
        assert result is not None
        assert result["result"] == 10
        # After flush, record should be in DB
        db.flush()

    def test_cache_hit_returns_stored(self):
        db = InMemoryArrowDatabase()
        inner_pf = PythonDataFunction(double, output_keys="result")
        cached_pf = CachedDataFunction(inner_pf, result_database=db)
        cached_pf.set_auto_flush(True)
        data = Data({"x": 5})
        # First call computes
        result1 = cached_pf.call(data)
        # Second call should return cached
        result2 = cached_pf.call(data)
        assert result1 is not None
        assert result2 is not None
        assert result1["result"] == result2["result"]

    def test_skip_cache_lookup_always_computes(self):
        db = InMemoryArrowDatabase()
        inner_pf = PythonDataFunction(double, output_keys="result")
        cached_pf = CachedDataFunction(inner_pf, result_database=db)
        cached_pf.set_auto_flush(True)
        data = Data({"x": 5})
        cached_pf.call(data)
        # With skip_cache_lookup, should recompute
        result = cached_pf.call(data, skip_cache_lookup=True)
        assert result is not None
        assert result["result"] == 10

    def test_skip_cache_insert_doesnt_store(self):
        db = InMemoryArrowDatabase()
        inner_pf = PythonDataFunction(double, output_keys="result")
        cached_pf = CachedDataFunction(inner_pf, result_database=db)
        data = Data({"x": 5})
        cached_pf.call(data, skip_cache_insert=True)
        db.flush()
        # Should not be cached
        cached_output = cached_pf.get_cached_output_for_data(data)
        assert cached_output is None

    def test_get_all_cached_outputs(self):
        db = InMemoryArrowDatabase()
        inner_pf = PythonDataFunction(double, output_keys="result")
        cached_pf = CachedDataFunction(inner_pf, result_database=db)
        cached_pf.set_auto_flush(True)
        cached_pf.call(Data({"x": 1}))
        cached_pf.call(Data({"x": 2}))
        all_outputs = cached_pf.get_all_cached_outputs()
        assert all_outputs is not None
        assert all_outputs.num_rows == 2
