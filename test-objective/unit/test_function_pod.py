"""Specification-derived tests for FunctionPod and FunctionPodStream.

Tests based on FunctionPodProtocol and documented behaviors:
- FunctionPod wraps a DataFunction for per-data transformation
- Never inspects or modifies keys
- Exactly one input stream
- output_schema() prediction matches actual output
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams.key_data import Data, Key
from orcapod.core.function_pod import FunctionPod, FunctionPodStream, function_pod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _add(x: int, y: int) -> int:
    return x + y


def _make_stream(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableSource(table, key_columns=["id"], infer_nullable=True)


def _make_two_col_stream(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, key_columns=["id"], infer_nullable=True)


# ---------------------------------------------------------------------------
# FunctionPod construction and processing
# ---------------------------------------------------------------------------


class TestFunctionPodProcess:
    """Per FunctionPodProtocol, process() accepts exactly one stream and
    returns a FunctionPodStream."""

    def test_process_returns_function_pod_stream(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        assert isinstance(result, FunctionPodStream)

    def test_callable_alias(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod(stream)
        assert isinstance(result, FunctionPodStream)

    def test_validate_inputs_rejects_multiple_streams(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        s1 = _make_stream()
        s2 = _make_stream()
        with pytest.raises(Exception):
            pod.validate_inputs(s1, s2)

    def test_validate_inputs_accepts_single_stream(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        pod.validate_inputs(stream)  # Should not raise


class TestFunctionPodKeyInvariant:
    """Per the strict boundary: function pods NEVER inspect or modify keys."""

    def test_keys_pass_through_unchanged(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)

        input_keys = [key for key, _ in stream.iter_data()]
        output_keys = [key for key, _ in result.iter_data()]

        for in_key, out_key in zip(input_keys, output_keys):
            # Key data columns should be identical
            assert in_key.keys() == out_key.keys()
            for key in in_key.keys():
                assert in_key[key] == out_key[key]

    def test_data_are_transformed(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)

        for key, data in result.iter_data():
            assert "result" in data.keys()


class TestFunctionPodOutputSchema:
    """Per PodProtocol, output_schema() must match the actual output."""

    def test_output_schema_matches_actual(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()

        predicted_key_schema, predicted_data_schema = pod.output_schema(stream)
        result = pod.process(stream)
        actual_key_schema, actual_data_schema = result.output_schema()

        # Key schemas should match
        assert set(predicted_key_schema.keys()) == set(actual_key_schema.keys())
        # Data schemas should match
        assert set(predicted_data_schema.keys()) == set(actual_data_schema.keys())


# ---------------------------------------------------------------------------
# FunctionPodStream
# ---------------------------------------------------------------------------


class TestFunctionPodStream:
    """Per design, FunctionPodStream is lazy — computation happens on iteration."""

    def test_producer_is_function_pod(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        assert result.producer is pod

    def test_upstreams_contains_input_stream(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        assert stream in result.upstreams

    def test_keys_matches_output_schema(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        key_keys, data_keys = result.keys()
        key_schema, data_schema = result.output_schema()
        assert set(key_keys) == set(key_schema.keys())
        assert set(data_keys) == set(data_schema.keys())

    def test_as_table_materialization(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream(3)
        result = pod.process(stream)
        table = result.as_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3

    def test_iter_data_yields_correct_count(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream(5)
        result = pod.process(stream)
        data = list(result.iter_data())
        assert len(data) == 5

    def test_clear_cache_forces_recompute(self):
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        # Materialize
        list(result.iter_data())
        # Clear and re-iterate
        result.clear_cache()
        data = list(result.iter_data())
        assert len(data) == 3


# ---------------------------------------------------------------------------
# @function_pod decorator
# ---------------------------------------------------------------------------


class TestFunctionPodDecorator:
    """Per design, the @function_pod decorator adds a .pod attribute."""

    def test_decorator_creates_pod_attribute(self):
        @function_pod(output_keys="result")
        def my_double(x: int) -> int:
            return x * 2

        assert hasattr(my_double, "pod")
        assert isinstance(my_double.pod, FunctionPod)

    def test_decorated_function_still_callable(self):
        @function_pod(output_keys="result")
        def my_double(x: int) -> int:
            return x * 2

        # The pod can process streams
        stream = _make_stream()
        result = my_double.pod.process(stream)
        data = list(result.iter_data())
        assert len(data) == 3
