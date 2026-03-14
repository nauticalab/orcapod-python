"""Tests for FunctionNode with optional database backing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase


def _make_pod():
    def double(x: int) -> int:
        return x * 2

    return FunctionPod(
        packet_function=PythonPacketFunction(double, output_keys="result"),
    )


def _make_stream(n=3):
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


class TestFunctionNodeWithoutDatabase:
    def test_construction_without_database(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        assert node._pipeline_database is None

    def test_iter_packets_without_database(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream(n=3))
        results = list(node.iter_packets())
        assert len(results) == 3
        assert results[0][1]["result"] == 0

    def test_get_all_records_without_database_returns_none(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        assert node.get_all_records() is None

    def test_as_source_without_database_raises(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        with pytest.raises(RuntimeError):
            node.as_source()


class TestFunctionNodeAttachDatabases:
    def test_attach_databases_sets_pipeline_db(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert node._pipeline_database is db

    def test_attach_databases_creates_cached_function_pod(self):
        from orcapod.core.cached_function_pod import CachedFunctionPod

        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert isinstance(node._cached_function_pod, CachedFunctionPod)

    def test_attach_databases_clears_caches(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        list(node.iter_packets())  # populate cache
        assert len(node._cached_output_packets) > 0
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert len(node._cached_output_packets) == 0

    def test_attach_databases_computes_pipeline_path(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert node.pipeline_path is not None
        assert len(node.pipeline_path) > 0

    def test_double_attach_does_not_double_wrap(self):
        from orcapod.core.cached_function_pod import CachedFunctionPod

        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert isinstance(node._cached_function_pod, CachedFunctionPod)
        # Second attach wraps the original function_pod, not the cached one
        node.attach_databases(pipeline_database=db, result_database=db)
        assert isinstance(node._cached_function_pod, CachedFunctionPod)
        assert not isinstance(
            node._cached_function_pod._function_pod, CachedFunctionPod
        )

    def test_iter_packets_after_attach_works(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream(n=2))
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        results = list(node.iter_packets())
        assert len(results) == 2


class TestFunctionNodeWithDatabase:
    def test_construction_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(),
            pipeline_database=db,
            result_database=db,
        )
        assert node._pipeline_database is db

    def test_pipeline_path_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(),
            pipeline_database=db,
            result_database=db,
        )
        assert len(node.pipeline_path) > 0

    def test_iter_packets_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(n=3),
            pipeline_database=db,
            result_database=db,
        )
        results = list(node.iter_packets())
        assert len(results) == 3
