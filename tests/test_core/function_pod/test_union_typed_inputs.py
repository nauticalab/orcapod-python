"""
Tests for union-typed function inputs (ITL-452).

Verifies:
- FunctionPod construction succeeds when an input arg is declared with a union type.
- Binding a concrete stream of a matching type succeeds and the input schema
  reflects the concrete branch (accessible via pod_stream.upstreams[0].output_schema()).
- Binding a stream whose type is not a member of the union raises ValueError.
- Data processing works correctly for each concrete branch.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.streams import ArrowTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_str_stream(n: int = 2) -> ArrowTableStream:
    """Stream with tag=id (int64), data=x (large_string / str)."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.large_string(), nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array([f"value_{i}" for i in range(n)], type=pa.large_string()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_path_stream(n: int = 2) -> ArrowTableStream:
    """Stream with tag=id (int64), data=x (orcapod.path / Path)."""
    ctx = get_default_context()
    path_arrow_type = ctx.type_converter.python_type_to_arrow_type(Path)
    storage = pa.array([f"/tmp/test_{i}" for i in range(n)], type=pa.large_string())
    path_array = pa.ExtensionArray.from_storage(path_arrow_type, storage)
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", path_arrow_type, nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": path_array,
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_int_stream(n: int = 2) -> ArrowTableStream:
    """Stream with tag=id (int64), data=x (int64) — incompatible with str | Path."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_union_pod() -> FunctionPod:
    """FunctionPod whose input x accepts str | Path."""
    def union_fn(x: str | Path) -> str:
        return str(x)

    return FunctionPod(PythonDataFunction(union_fn, output_keys="result"))


# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------


class TestUnionInputConstruction:
    def test_pod_construction_succeeds(self):
        """Creating a FunctionPod with a union-typed input must not raise."""
        def union_fn(x: str | Path) -> str:
            return str(x)

        # Must not raise ValueError about complex unions
        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        assert pod is not None

    def test_input_schema_preserves_union_type(self):
        """The data function's input_data_schema must record the full union type."""
        def union_fn(x: str | Path) -> str:
            return str(x)

        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        assert pod.data_function.input_data_schema["x"] == str | Path


# ---------------------------------------------------------------------------
# Stream binding tests
# ---------------------------------------------------------------------------


class TestUnionInputStreamBinding:
    def test_bind_str_stream_succeeds(self):
        """Binding a str-typed stream to a str | Path pod must succeed."""
        pod = make_union_pod()
        pod_stream = pod.process(make_str_stream())
        # The upstream stream's data schema must report the concrete str type
        _, input_data_schema = pod_stream.upstreams[0].output_schema()
        assert input_data_schema["x"] == str

    def test_bind_path_stream_succeeds(self):
        """Binding a Path-typed stream to a str | Path pod must succeed."""
        pod = make_union_pod()
        pod_stream = pod.process(make_path_stream())
        # The upstream stream's data schema must report the concrete Path type
        _, input_data_schema = pod_stream.upstreams[0].output_schema()
        assert input_data_schema["x"] == Path

    def test_bind_incompatible_type_raises(self):
        """Binding a stream whose type is not in the union must raise ValueError."""
        pod = make_union_pod()
        with pytest.raises(ValueError, match="not compatible"):
            pod.process(make_int_stream())


# ---------------------------------------------------------------------------
# Data processing tests
# ---------------------------------------------------------------------------


class TestUnionInputDataProcessing:
    def test_process_str_input_yields_correct_output(self):
        """Processing a str-typed stream through a str | Path pod gives correct results."""
        def union_fn(x: str | Path) -> str:
            return str(x).upper()

        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        pod_stream = pod.process(make_str_stream(n=2))
        results = list(pod_stream.iter_data())
        assert len(results) == 2
        _, out_data = results[0]
        assert out_data.as_dict()["result"] == "VALUE_0"

    def test_process_path_input_yields_correct_output(self):
        """Processing a Path-typed stream through a str | Path pod gives correct results."""
        def union_fn(x: str | Path) -> str:
            return str(x)

        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        pod_stream = pod.process(make_path_stream(n=2))
        results = list(pod_stream.iter_data())
        assert len(results) == 2
        _, out_data = results[0]
        assert out_data.as_dict()["result"] == "/tmp/test_0"
