"""Tests for CachedFunctionPod — pod-level caching wrapper."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod, function_pod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def _make_stream(rows: list[dict] | None = None) -> ArrowTableStream:
    if rows is None:
        rows = [{"id": 0, "x": 10}, {"id": 1, "x": 20}]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    return ArrowTableStream(table, tag_columns=["id"])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def cache_db():
    return InMemoryArrowDatabase()


@pytest.fixture
def double_pod():
    pf = PythonPacketFunction(double, output_keys="result")
    return FunctionPod(pf)


@pytest.fixture
def cached_pod(double_pod, cache_db):
    return CachedFunctionPod(
        function_pod=double_pod,
        result_database=cache_db,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_record_path_ends_with_inner_uri(self, cached_pod, double_pod):
        assert cached_pod.record_path[-len(double_pod.uri) :] == double_pod.uri

    def test_record_path_prefix_empty_by_default(self, cached_pod, double_pod):
        assert cached_pod.record_path == double_pod.uri

    def test_record_path_prefix_prepended(self, double_pod, cache_db):
        pod = CachedFunctionPod(
            double_pod,
            result_database=cache_db,
            record_path_prefix=("my", "prefix"),
        )
        assert pod.record_path[:2] == ("my", "prefix")


# ---------------------------------------------------------------------------
# Cache miss
# ---------------------------------------------------------------------------


class TestCacheMiss:
    def test_returns_non_none_result(self, cached_pod):
        stream = _make_stream()
        output = cached_pod.process(stream)
        results = list(output.iter_packets())
        assert len(results) == 2

    def test_result_values_correct(self, cached_pod):
        stream = _make_stream()
        output = cached_pod.process(stream)
        results = list(output.iter_packets())
        assert results[0][1].as_dict()["result"] == 20
        assert results[1][1].as_dict()["result"] == 40

    def test_result_stored_in_database(self, cached_pod, cache_db):
        stream = _make_stream()
        output = cached_pod.process(stream)
        list(output.iter_packets())  # exhaust the iterator

        records = cache_db.get_all_records(cached_pod.record_path)
        assert records is not None
        assert records.num_rows == 2


# ---------------------------------------------------------------------------
# Cache hit
# ---------------------------------------------------------------------------


class TestCacheHit:
    def test_second_call_returns_same_result(self, cached_pod):
        stream = _make_stream()

        # First call: cache miss
        output1 = cached_pod.process(stream)
        first = [(t.as_dict(), p.as_dict()) for t, p in output1.iter_packets()]

        # Second call: cache hit
        output2 = cached_pod.process(_make_stream())
        second = [(t.as_dict(), p.as_dict()) for t, p in output2.iter_packets()]

        assert len(first) == len(second)
        for (_, p1), (_, p2) in zip(first, second):
            assert p1["result"] == p2["result"]

    def test_second_call_does_not_add_new_records(self, cached_pod, cache_db):
        stream = _make_stream()
        output = cached_pod.process(stream)
        list(output.iter_packets())

        records_after_first = cache_db.get_all_records(cached_pod.record_path)
        assert records_after_first is not None
        count_first = records_after_first.num_rows

        output2 = cached_pod.process(_make_stream())
        list(output2.iter_packets())

        records_after_second = cache_db.get_all_records(cached_pod.record_path)
        assert records_after_second is not None
        assert records_after_second.num_rows == count_first


# ---------------------------------------------------------------------------
# Tag-aware caching
# ---------------------------------------------------------------------------


class TestTagAwareCaching:
    def test_different_tags_same_packet_cached_separately(self, double_pod, cache_db):
        """Same packet data with different tag values should be cached separately."""
        cached_pod = CachedFunctionPod(double_pod, result_database=cache_db)

        # Stream with tag=0, x=10
        stream1 = _make_stream([{"id": 0, "x": 10}])
        list(cached_pod.process(stream1).iter_packets())

        # Stream with tag=1, x=10 (same packet data, different tag)
        stream2 = _make_stream([{"id": 1, "x": 10}])
        list(cached_pod.process(stream2).iter_packets())

        records = cache_db.get_all_records(cached_pod.record_path)
        assert records is not None
        # Should have 2 records since tags differ
        assert records.num_rows == 2


# ---------------------------------------------------------------------------
# Decorator integration
# ---------------------------------------------------------------------------


class TestDecoratorIntegration:
    def test_decorator_with_pod_cache_database(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", pod_cache_database=db)
        def my_double(x: int) -> int:
            return x * 2

        assert isinstance(my_double.pod, CachedFunctionPod)

    def test_decorator_pod_cache_produces_correct_results(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", pod_cache_database=db)
        def my_double(x: int) -> int:
            return x * 2

        stream = _make_stream()
        output = my_double.pod.process(stream)
        results = list(output.iter_packets())
        assert len(results) == 2
        assert results[0][1].as_dict()["result"] == 20

    def test_decorator_without_pod_cache_returns_plain_pod(self):
        @function_pod(output_keys="result")
        def my_double(x: int) -> int:
            return x * 2

        assert isinstance(my_double.pod, FunctionPod)
