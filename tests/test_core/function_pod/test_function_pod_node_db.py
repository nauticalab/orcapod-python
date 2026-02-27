"""
Tests for FunctionPodNode.get_all_records and the DB-backed iter_packets
behaviour of FunctionPodNodeStream.

Covers:
- get_all_records: empty DB → None
- get_all_records: default (data-only) columns
- get_all_records: meta columns included/excluded
- get_all_records: source columns included/excluded
- get_all_records: system_tags columns included/excluded
- get_all_records: all_info=True includes everything
- get_all_records: row count and values are correct
- iter_packets Phase 1: already-stored results served from DB without recomputation
- iter_packets Phase 2: only missing entries are passed to process_packet
- iter_packets: partial fill — some stored, some new
- iter_packets: second node sharing same DB skips all computation
- iter_packets: node with fresh DB always computes
- iter_packets: results from DB and from compute agree in values
- iter_packets: call-count proof that the inner function is not re-called for cached rows
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams import DictPacket, DictTag
from orcapod.core.function_pod import FunctionPodNode, FunctionPodNodeStream
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import TableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants

from ..conftest import double, make_int_stream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    pf: PythonPacketFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> FunctionPodNode:
    if db is None:
        db = InMemoryArrowDatabase()
    return FunctionPodNode(
        packet_function=pf,
        input_stream=make_int_stream(n=n),
        pipeline_database=db,
    )


def _make_node_with_system_tags(
    pf: PythonPacketFunction,
    n: int = 3,
    db: InMemoryArrowDatabase | None = None,
) -> FunctionPodNode:
    """Build a node whose input stream has an explicit system-tag column ('run')."""
    if db is None:
        db = InMemoryArrowDatabase()
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "run": pa.array([f"r{i}" for i in range(n)]),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    stream = TableStream(table, tag_columns=["id"], system_tag_columns=["run"])
    return FunctionPodNode(
        packet_function=pf,
        input_stream=stream,
        pipeline_database=db,
    )


def _fill_node(node: FunctionPodNode) -> None:
    """Process all packets so the DB is populated."""
    list(node.process().iter_packets())


# ---------------------------------------------------------------------------
# 1. FunctionPodNode.get_all_records — empty database
# ---------------------------------------------------------------------------


class TestGetAllRecordsEmpty:
    def test_returns_none_when_db_is_empty(self, double_pf):
        node = _make_node(double_pf, n=3)
        assert node.get_all_records() is None

    def test_returns_none_after_no_processing(self, double_pf):
        node = _make_node(double_pf, n=5)
        # process() is never called, so both DBs are empty
        assert node.get_all_records(all_info=True) is None


# ---------------------------------------------------------------------------
# 2. FunctionPodNode.get_all_records — basic correctness after population
# ---------------------------------------------------------------------------


class TestGetAllRecordsValues:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionPodNode:
        node = _make_node(double_pf, n=4)
        _fill_node(node)
        return node

    def test_returns_pyarrow_table(self, filled_node):
        result = filled_node.get_all_records()
        assert isinstance(result, pa.Table)

    def test_row_count_matches_input(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert result.num_rows == 4

    def test_contains_tag_column(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert "id" in result.column_names

    def test_contains_output_packet_column(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert "result" in result.column_names

    def test_output_values_are_correct(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        # double(x) = x*2, x in {0,1,2,3}
        assert sorted(result.column("result").to_pylist()) == [0, 2, 4, 6]

    def test_tag_values_are_correct(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        assert sorted(result.column("id").to_pylist()) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# 3. FunctionPodNode.get_all_records — ColumnConfig: meta columns
# ---------------------------------------------------------------------------


class TestGetAllRecordsMetaColumns:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionPodNode:
        node = _make_node(double_pf, n=3)
        _fill_node(node)
        return node

    def test_default_excludes_meta_columns(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        meta_cols = [
            c for c in result.column_names if c.startswith(constants.META_PREFIX)
        ]
        assert meta_cols == [], f"Unexpected meta columns: {meta_cols}"

    def test_meta_true_includes_packet_record_id(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert constants.PACKET_RECORD_ID in result.column_names

    def test_meta_true_includes_input_packet_hash(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert constants.INPUT_PACKET_HASH_COL in result.column_names

    def test_meta_true_still_has_data_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names

    def test_input_packet_hash_values_are_non_empty_strings(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        hashes = result.column(constants.INPUT_PACKET_HASH_COL).to_pylist()
        assert all(isinstance(h, str) and len(h) > 0 for h in hashes)

    def test_packet_record_id_values_are_non_empty_strings(self, filled_node):
        result = filled_node.get_all_records(columns={"meta": True})
        assert result is not None
        ids = result.column(constants.PACKET_RECORD_ID).to_pylist()
        assert all(isinstance(rid, str) and len(rid) > 0 for rid in ids)


# ---------------------------------------------------------------------------
# 4. FunctionPodNode.get_all_records — ColumnConfig: source columns
# ---------------------------------------------------------------------------


class TestGetAllRecordsSourceColumns:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionPodNode:
        node = _make_node(double_pf, n=3)
        _fill_node(node)
        return node

    def test_default_excludes_source_columns(self, filled_node):
        result = filled_node.get_all_records()
        assert result is not None
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert source_cols == [], f"Unexpected source columns: {source_cols}"

    def test_source_true_includes_source_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"source": True})
        assert result is not None
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0

    def test_source_true_still_has_data_columns(self, filled_node):
        result = filled_node.get_all_records(columns={"source": True})
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names


# ---------------------------------------------------------------------------
# 5. FunctionPodNode.get_all_records — ColumnConfig: system_tags columns
# ---------------------------------------------------------------------------


class TestGetAllRecordsSystemTagColumns:
    @pytest.fixture
    def filled_node_with_sys_tags(self, double_pf) -> FunctionPodNode:
        """Node whose input stream has an explicit 'run' system-tag column."""
        node = _make_node_with_system_tags(double_pf, n=3)
        _fill_node(node)
        return node

    def test_default_excludes_system_tag_columns(self, filled_node_with_sys_tags):
        result = filled_node_with_sys_tags.get_all_records()
        assert result is not None
        sys_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert sys_cols == [], f"Unexpected system tag columns: {sys_cols}"

    def test_system_tags_true_includes_system_tag_columns(
        self, filled_node_with_sys_tags
    ):
        result = filled_node_with_sys_tags.get_all_records(
            columns={"system_tags": True}
        )
        assert result is not None
        sys_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_cols) > 0

    def test_system_tags_true_still_has_data_columns(self, filled_node_with_sys_tags):
        result = filled_node_with_sys_tags.get_all_records(
            columns={"system_tags": True}
        )
        assert result is not None
        assert "id" in result.column_names
        assert "result" in result.column_names


# ---------------------------------------------------------------------------
# 6. FunctionPodNode.get_all_records — all_info=True
# ---------------------------------------------------------------------------


class TestGetAllRecordsAllInfo:
    @pytest.fixture
    def filled_node(self, double_pf) -> FunctionPodNode:
        node = _make_node(double_pf, n=3)
        _fill_node(node)
        return node

    @pytest.fixture
    def filled_node_with_sys_tags(self, double_pf) -> FunctionPodNode:
        node = _make_node_with_system_tags(double_pf, n=3)
        _fill_node(node)
        return node

    def test_all_info_includes_meta_columns(self, filled_node):
        result = filled_node.get_all_records(all_info=True)
        assert result is not None
        meta_cols = [
            c for c in result.column_names if c.startswith(constants.META_PREFIX)
        ]
        assert len(meta_cols) > 0

    def test_all_info_includes_source_columns(self, filled_node):
        result = filled_node.get_all_records(all_info=True)
        assert result is not None
        source_cols = [
            c for c in result.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0

    def test_all_info_includes_system_tag_columns(self, filled_node_with_sys_tags):
        """System-tag columns appear in all_info only when the input stream has them."""
        result = filled_node_with_sys_tags.get_all_records(all_info=True)
        assert result is not None
        sys_cols = [
            c for c in result.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_cols) > 0

    def test_all_info_has_more_columns_than_default(self, filled_node):
        default_result = filled_node.get_all_records()
        full_result = filled_node.get_all_records(all_info=True)
        assert full_result is not None
        assert default_result is not None
        assert full_result.num_columns > default_result.num_columns

    def test_all_info_data_columns_match_default(self, filled_node):
        """Data columns (id, result) are present and identical under both configs."""
        default_result = filled_node.get_all_records()
        full_result = filled_node.get_all_records(all_info=True)
        assert default_result is not None
        assert full_result is not None
        assert sorted(default_result.column("id").to_pylist()) == sorted(
            full_result.column("id").to_pylist()
        )
        assert sorted(default_result.column("result").to_pylist()) == sorted(
            full_result.column("result").to_pylist()
        )


# ---------------------------------------------------------------------------
# 7. FunctionPodNodeStream.iter_packets — Phase 1: DB-served results
# ---------------------------------------------------------------------------


class TestIterPacketsDbPhase:
    def test_cached_results_served_without_recomputation(self, double_pf):
        """
        After node1 fills the DB, node2 (sharing the same DB) should serve all
        results from Phase 1 (DB lookup) without calling the inner function.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")

        n = 3
        db = InMemoryArrowDatabase()

        # node1 — first pass populates result DB and pipeline DB
        node1 = FunctionPodNode(
            packet_function=counting_pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=db,
        )
        _fill_node(node1)
        calls_after_first_pass = call_count

        # node2 — same packet_function, same DB → all entries pre-exist
        node2 = FunctionPodNode(
            packet_function=counting_pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=db,
        )
        _fill_node(node2)

        # No additional calls should have happened
        assert call_count == calls_after_first_pass

    def test_db_served_results_have_correct_values(self, double_pf):
        """Values from DB Phase equal the originally computed values."""
        n = 4
        db = InMemoryArrowDatabase()

        node1 = FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=db,
        )
        table1 = node1.process().as_table()

        node2 = FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=db,
        )
        table2 = node2.process().as_table()

        # Both passes must produce the same result values
        assert sorted(table1.column("result").to_pylist()) == sorted(
            table2.column("result").to_pylist()
        )

    def test_db_served_results_have_correct_row_count(self, double_pf):
        n = 5
        db = InMemoryArrowDatabase()

        node1 = _make_node(double_pf, n=n, db=db)
        _fill_node(node1)

        node2 = _make_node(double_pf, n=n, db=db)
        packets = list(node2.process().iter_packets())
        assert len(packets) == n

    def test_fresh_db_always_computes(self, double_pf):
        """A node with a fresh (empty) DB always falls through to Phase 2."""
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")

        n = 3
        # Each node gets its own fresh DB → no cross-node cache sharing
        for _ in range(2):
            node = FunctionPodNode(
                packet_function=counting_pf,
                input_stream=make_int_stream(n=n),
                pipeline_database=InMemoryArrowDatabase(),
            )
            _fill_node(node)

        # Both passes must have computed from scratch
        assert call_count == n * 2


# ---------------------------------------------------------------------------
# 8. FunctionPodNodeStream.iter_packets — Phase 2: only missing entries computed
# ---------------------------------------------------------------------------


class TestIterPacketsMissingEntriesOnly:
    def test_partial_fill_computes_only_missing(self, double_pf):
        """
        Manually populate the DB for a subset of the input, then verify that
        iter_packets only computes the remaining entries.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")

        n = 4
        db = InMemoryArrowDatabase()

        # Pre-fill 2 out of 4 entries using a node over a smaller input stream
        node_pre = FunctionPodNode(
            packet_function=counting_pf,
            input_stream=make_int_stream(n=2),  # only first 2 rows
            pipeline_database=db,
        )
        _fill_node(node_pre)
        calls_after_prefill = call_count  # should be 2

        # Now process all 4 rows — the 2 already in DB should not be recomputed
        node_full = FunctionPodNode(
            packet_function=counting_pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=db,
        )
        _fill_node(node_full)

        # Only 2 additional calls expected for the 2 missing rows
        assert call_count == calls_after_prefill + 2

    def test_partial_fill_total_row_count_correct(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()

        # Pre-fill first 2
        node_pre = _make_node(double_pf, n=2, db=db)
        _fill_node(node_pre)

        # Full run over n=4
        node_full = _make_node(double_pf, n=n, db=db)
        packets = list(node_full.process().iter_packets())
        assert len(packets) == n

    def test_partial_fill_all_values_correct(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()

        node_pre = _make_node(double_pf, n=2, db=db)
        _fill_node(node_pre)

        node_full = _make_node(double_pf, n=n, db=db)
        table = node_full.process().as_table()
        assert sorted(table.column("result").to_pylist()) == [0, 2, 4, 6]

    def test_already_full_db_zero_additional_calls(self, double_pf):
        """Once every entry is in the DB, a new node makes zero inner-function calls."""
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        counting_pf = PythonPacketFunction(counting_double, output_keys="result")
        n = 3
        db = InMemoryArrowDatabase()

        # Fill completely
        node1 = _make_node(counting_pf, n=n, db=db)
        _fill_node(node1)
        calls_after_fill = call_count

        # Second node — same DB, same inputs → Phase 1 covers everything
        node2 = _make_node(counting_pf, n=n, db=db)
        _fill_node(node2)
        assert call_count == calls_after_fill


# ---------------------------------------------------------------------------
# 9. FunctionPodNodeStream.iter_packets — inactive packet function + DB
# ---------------------------------------------------------------------------


class TestIterPacketsInactiveWithDb:
    def test_inactive_with_empty_db_yields_no_packets(self, double_pf):
        double_pf.set_active(False)
        node = _make_node(double_pf, n=3)
        packets = list(node.process().iter_packets())
        assert packets == []

    def test_inactive_with_filled_db_serves_cached_results(self, double_pf):
        """
        Fill the DB while active, then deactivate and verify results still come
        from Phase 1 (DB) without calling the inner function.
        """
        n = 3
        db = InMemoryArrowDatabase()

        # Fill while active
        node1 = _make_node(double_pf, n=n, db=db)
        _fill_node(node1)

        # Deactivate and use a new node with the same DB
        double_pf.set_active(False)
        node2 = _make_node(double_pf, n=n, db=db)
        packets = list(node2.process().iter_packets())

        assert len(packets) == n

    def test_inactive_with_filled_db_values_correct(self, double_pf):
        n = 3
        db = InMemoryArrowDatabase()

        node1 = _make_node(double_pf, n=n, db=db)
        table1 = node1.process().as_table()

        double_pf.set_active(False)
        node2 = _make_node(double_pf, n=n, db=db)
        table2 = node2.process().as_table()

        assert sorted(table2.column("result").to_pylist()) == sorted(
            table1.column("result").to_pylist()
        )
