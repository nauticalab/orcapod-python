"""
Tests for PersistentFunctionNode's stream interface covering:
- iter_packets: correctness, repeatability, __iter__
- as_table: correctness, ColumnConfig (content_hash, sort_by_tags)
- output_schema and keys
- source / upstreams properties
- Inactive packet function behaviour
- DB-backed Phase 1: cached results served without recomputation
- DB Phase 2: only missing entries computed
- is_stale: freshly created, after upstream modified
- clear_cache: resets state, produces same results on re-iteration
- Automatic staleness detection in iter_packets / as_table
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from collections.abc import Mapping

from orcapod.core.function_pod import PersistentFunctionNode, FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol

from ..conftest import make_int_stream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    pf: PythonPacketFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> PersistentFunctionNode:
    if db is None:
        db = InMemoryArrowDatabase()
    return PersistentFunctionNode(
        packet_function=pf,
        input_stream=make_int_stream(n=n),
        pipeline_database=db,
    )


def _fill_node(node: PersistentFunctionNode) -> None:
    """Process all packets so the DB is populated."""
    node.run()


# ---------------------------------------------------------------------------
# 1. Basic iter_packets and as_table correctness
# ---------------------------------------------------------------------------


class TestFunctionNodeStreamBasic:
    @pytest.fixture
    def node(self, double_pf) -> PersistentFunctionNode:
        db = InMemoryArrowDatabase()
        return PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_iter_packets_yields_correct_count(self, node):
        assert len(list(node.iter_packets())) == 3

    def test_iter_packets_correct_values(self, node):
        for i, (_, packet) in enumerate(node.iter_packets()):
            assert packet["result"] == i * 2

    def test_iter_is_repeatable(self, node):
        first = [(t["id"], p["result"]) for t, p in node.iter_packets()]
        second = [(t["id"], p["result"]) for t, p in node.iter_packets()]
        assert first == second

    def test_dunder_iter_delegates_to_iter_packets(self, node):
        assert len(list(node)) == len(list(node.iter_packets()))

    def test_as_table_returns_pyarrow_table(self, node):
        assert isinstance(node.as_table(), pa.Table)

    def test_as_table_has_correct_row_count(self, node):
        assert len(node.as_table()) == 3

    def test_as_table_contains_tag_columns(self, node):
        assert "id" in node.as_table().column_names

    def test_as_table_contains_packet_columns(self, node):
        assert "result" in node.as_table().column_names

    def test_producer_is_function_pod(self, node, double_pf):
        assert isinstance(node.producer, FunctionPod)

    def test_upstreams_contains_input_stream(self, node):
        upstreams = node.upstreams
        assert isinstance(upstreams, tuple)
        assert len(upstreams) == 1

    def test_output_schema_has_result_in_packet_schema(self, node):
        tag_schema, packet_schema = node.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)
        assert "result" in packet_schema


# ---------------------------------------------------------------------------
# 2. ColumnConfig — content_hash and sort_by_tags
# ---------------------------------------------------------------------------


class TestFunctionNodeColumnConfig:
    def test_as_table_content_hash_column(self, double_pf):
        node = _make_node(double_pf, n=3)
        table = node.as_table(columns={"content_hash": True})
        assert "_content_hash" in table.column_names
        assert len(table.column("_content_hash")) == 3

    def test_as_table_sort_by_tags(self, double_pf):
        db = InMemoryArrowDatabase()
        reversed_table = pa.table(
            {
                "id": pa.array([4, 3, 2, 1, 0], type=pa.int64()),
                "x": pa.array([4, 3, 2, 1, 0], type=pa.int64()),
            }
        )
        input_stream = ArrowTableStream(reversed_table, tag_columns=["id"])
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        result = node.as_table(columns={"sort_by_tags": True})
        ids: list[int] = result.column("id").to_pylist()  # type: ignore[assignment]
        assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# 3. Inactive packet function behaviour
# ---------------------------------------------------------------------------


class TestFunctionNodeInactive:
    def test_as_table_returns_empty_when_packet_function_inactive(self, double_pf):
        double_pf.set_active(False)
        node = _make_node(double_pf, n=3)
        table = node.as_table()
        assert isinstance(table, pa.Table)
        assert len(table) == 0

    def test_as_table_returns_cached_results_when_packet_function_inactive(
        self, double_pf
    ):
        """
        Cache filled by node1 (active) is shared with node2 (inactive).
        node2.as_table() must return full results from Phase 1 (DB).
        """
        n = 3
        db = InMemoryArrowDatabase()
        node1 = _make_node(double_pf, n=n, db=db)
        table1 = node1.as_table()
        assert len(table1) == n

        double_pf.set_active(False)

        node2 = _make_node(double_pf, n=n, db=db)
        table2 = node2.as_table()

        assert isinstance(table2, pa.Table)
        assert len(table2) == n
        assert (
            table2.column("result").to_pylist() == table1.column("result").to_pylist()
        )

    def test_inactive_fresh_db_yields_no_packets(self, double_pf):
        double_pf.set_active(False)
        node = _make_node(double_pf, n=3)
        assert list(node.iter_packets()) == []

    def test_inactive_filled_db_serves_cached_results(self, double_pf):
        n = 3
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=n, db=db))

        double_pf.set_active(False)
        node2 = _make_node(double_pf, n=n, db=db)
        packets = list(node2.iter_packets())
        assert len(packets) == n

    def test_inactive_node_with_separate_fresh_db_yields_empty(self, double_pf):
        n = 3
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=n, db=db))

        double_pf.set_active(False)
        node3 = _make_node(double_pf, n=n, db=InMemoryArrowDatabase())
        table = node3.as_table()
        assert isinstance(table, pa.Table)
        assert len(table) == 0


# ---------------------------------------------------------------------------
# 4. DB-backed Phase 1: results served without recomputation
# ---------------------------------------------------------------------------


class TestIterPacketsDbPhase:
    def test_cached_results_served_without_recomputation(self, double_pf):
        """
        After node1 fills the DB, node2 (sharing the same DB) serves all results
        from Phase 1 without calling the inner function.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")
        n = 3
        db = InMemoryArrowDatabase()

        node1 = _make_node(counting_pf, n=n, db=db)
        _fill_node(node1)
        calls_after_first_pass = call_count

        node2 = _make_node(counting_pf, n=n, db=db)
        _fill_node(node2)

        assert call_count == calls_after_first_pass

    def test_db_served_results_have_correct_values(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()

        table1 = _make_node(double_pf, n=n, db=db).as_table()
        table2 = _make_node(double_pf, n=n, db=db).as_table()

        assert sorted(table1.column("result").to_pylist()) == sorted(
            table2.column("result").to_pylist()
        )

    def test_db_served_results_have_correct_row_count(self, double_pf):
        n = 5
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=n, db=db))
        packets = list(_make_node(double_pf, n=n, db=db).iter_packets())
        assert len(packets) == n

    def test_fresh_db_always_computes(self, double_pf):
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")
        n = 3
        for _ in range(2):
            _fill_node(_make_node(counting_pf, n=n, db=InMemoryArrowDatabase()))

        assert call_count == n * 2


# ---------------------------------------------------------------------------
# 5. DB Phase 2: only missing entries computed
# ---------------------------------------------------------------------------


class TestIterPacketsMissingEntriesOnly:
    def test_partial_fill_computes_only_missing(self, double_pf):
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")
        n = 4
        db = InMemoryArrowDatabase()

        _fill_node(_make_node(counting_pf, n=2, db=db))
        calls_after_prefill = call_count

        _fill_node(_make_node(counting_pf, n=n, db=db))
        assert call_count == calls_after_prefill + 2

    def test_partial_fill_total_row_count_correct(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=2, db=db))
        packets = list(_make_node(double_pf, n=n, db=db).iter_packets())
        assert len(packets) == n

    def test_partial_fill_all_values_correct(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=2, db=db))
        table = _make_node(double_pf, n=n, db=db).as_table()
        assert sorted(table.column("result").to_pylist()) == [0, 2, 4, 6]

    def test_already_full_db_zero_additional_calls(self, double_pf):
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")
        n = 3
        db = InMemoryArrowDatabase()

        _fill_node(_make_node(counting_pf, n=n, db=db))
        calls_after_fill = call_count

        _fill_node(_make_node(counting_pf, n=n, db=db))
        assert call_count == calls_after_fill


# ---------------------------------------------------------------------------
# 6. is_stale and clear_cache
# ---------------------------------------------------------------------------


class TestFunctionNodeStaleness:
    # --- is_stale ---

    def test_is_stale_false_immediately_after_creation(self, double_pf):
        """A freshly created PersistentFunctionNode whose upstream has not changed is not stale."""
        node = _make_node(double_pf, n=3)
        assert not node.is_stale

    def test_is_stale_true_after_upstream_modified(self, double_pf):
        import time

        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        list(node.iter_packets())

        time.sleep(0.01)
        input_stream._update_modified_time()

        assert node.is_stale

    def test_is_stale_false_after_clear_cache(self, double_pf):
        import time

        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        list(node.iter_packets())

        time.sleep(0.01)
        input_stream._update_modified_time()
        assert node.is_stale

        node.clear_cache()
        assert not node.is_stale

    # --- clear_cache ---

    def test_clear_cache_resets_output_packets(self, double_pf):
        node = _make_node(double_pf, n=3)
        list(node.iter_packets())
        assert len(node._cached_output_packets) == 3

        node.clear_cache()
        assert len(node._cached_output_packets) == 0
        assert node._cached_output_table is None

    def test_clear_cache_produces_same_results_on_re_iteration(self, double_pf):
        node = _make_node(double_pf, n=3)
        table_before = node.as_table()

        node.clear_cache()
        table_after = node.as_table()

        assert sorted(table_before.column("result").to_pylist()) == sorted(
            table_after.column("result").to_pylist()
        )

    # --- automatic staleness detection ---

    def test_iter_packets_auto_detects_stale_and_repopulates(self, double_pf):
        import time

        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        first = list(node.iter_packets())

        time.sleep(0.01)
        input_stream._update_modified_time()
        assert node.is_stale

        second = list(node.iter_packets())
        assert len(second) == len(first)
        assert [p["result"] for _, p in second] == [p["result"] for _, p in first]

    def test_as_table_auto_detects_stale_and_repopulates(self, double_pf):
        import time

        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        table_before = node.as_table()
        assert len(table_before) == 3

        time.sleep(0.01)
        input_stream._update_modified_time()

        table_after = node.as_table()
        assert len(table_after) == 3
        assert sorted(table_after.column("result").to_pylist()) == sorted(
            table_before.column("result").to_pylist()
        )

    def test_no_auto_clear_when_not_stale(self, double_pf):
        node = _make_node(double_pf, n=3)
        list(node.iter_packets())
        cached_count = len(node._cached_output_packets)

        list(node.iter_packets())
        assert len(node._cached_output_packets) == cached_count

    def test_as_table_no_auto_clear_when_not_stale(self, double_pf):
        node = _make_node(double_pf, n=3)
        table_before = node.as_table()
        table_after = node.as_table()
        assert table_before.equals(table_after)
