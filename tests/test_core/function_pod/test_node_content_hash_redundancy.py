"""Tests proving NODE_CONTENT_HASH_COL is redundant in the record_id preimage.

This module is an investigation artefact for ITL-533. It demonstrates two
concrete properties:

1. base_entry_id (hash of system_tags + INPUT_DATA_HASH_COL) already uniquely
   identifies distinct input rows — different inputs produce different base_entry_ids
   without any help from NODE_CONTENT_HASH_COL.

2. _filter_by_content_hash() is a no-op for correctness: patching it to pass all
   rows through leaves pipeline results identical (same call counts, same output values).

The deeper insight tying both properties together:

NODE_CONTENT_HASH_COL is always in lockstep with base_entry_id. There is no
scenario where base_entry_id is the same but NODE_CONTENT_HASH_COL differs, because:

  - base_entry_id = hash(system_tags + INPUT_DATA_HASH_COL)
  - system_tags encode source_id, which defaults to table_hash (content-addressed)
  - ArrowTableSource.identity_structure() = (class_name, schema, source_id)
  - Same source_id + same schema → same source content_hash → same node content_hash
    → same NODE_CONTENT_HASH_COL

So the two cases are:
  a) Different inputs → different base_entry_id → records already distinct.
     NODE_CONTENT_HASH_COL adds nothing.
  b) Same inputs → same base_entry_id AND same NODE_CONTENT_HASH_COL.
     Same logical record should have the same record_id (idempotency is correct).

Together these confirm: removing NODE_CONTENT_HASH_COL from the preimage is safe.
The combination of system_tags + INPUT_DATA_HASH_COL + recomputation_index is
sufficient for unique record identification.
"""

from __future__ import annotations

from typing import cast
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants

from ..conftest import make_int_stream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source_stream(values: list[int]) -> ArrowTableSource:
    """Return an ArrowTableSource with tag=id, data=x for the given int values."""
    table = pa.table(
        {
            "id": pa.array(list(range(len(values))), type=pa.int64()),
            "x": pa.array(values, type=pa.int64()),
        }
    )
    return ArrowTableSource(table=table, tag_columns=["id"], infer_nullable=True)


def _all_base_entry_ids(node: FunctionJobNode, stream) -> set[bytes]:
    """Return the set of base_entry_ids for all (tag, data) pairs in stream."""
    return {
        node.compute_base_entry_id(tag, data)
        for tag, data in stream.iter_data()
    }


# ---------------------------------------------------------------------------
# Property 1: base_entry_id uniqueness across distinct inputs
#
# Two nodes sharing the same pipeline table (same pipeline_hash, different
# content_hash) produce *different* base_entry_ids when processing different
# input rows.  system_tags (content-addressed source_id) alone ensures this.
# ---------------------------------------------------------------------------


class TestBaseEntryIdUniqueness:
    """base_entry_id uniquely identifies distinct input rows across node instances."""

    def test_different_source_data_different_base_entry_id(self, double_pf):
        """Rows from two sources with different data → different base_entry_ids.

        Even though node1 and node2 share the same pipeline_hash (same function +
        same schema), their inputs come from sources with different table_hash-derived
        source_ids.  system_tags therefore differ → different base_entry_id.
        """
        db = InMemoryArrowDatabase()
        src1 = _make_source_stream([10, 20, 30])  # source_id = hash(this table)
        src2 = _make_source_stream([40, 50, 60])  # source_id = hash(different table)

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src1,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src2,
            pipeline_database=db,
        )

        # Sanity: they share the same pipeline table path
        assert node1.node_identity_path == node2.node_identity_path
        # But have different content_hashes (different upstream data)
        assert node1.content_hash() != node2.content_hash()

        ids1 = _all_base_entry_ids(node1, src1)
        ids2 = _all_base_entry_ids(node2, src2)

        # No overlap: different rows → different base_entry_ids
        assert ids1.isdisjoint(ids2), (
            "base_entry_ids from different sources must not collide; "
            "NODE_CONTENT_HASH_COL is therefore not needed to distinguish them."
        )

    def test_different_row_within_source_different_base_entry_id(self, double_pf):
        """Distinct rows within the same source always have different base_entry_ids."""
        db = InMemoryArrowDatabase()
        src = _make_source_stream([10, 20, 30])

        node = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )

        ids = [node.compute_base_entry_id(tag, data) for tag, data in src.iter_data()]
        assert len(ids) == len(set(ids)), "Every row in a source must have a unique base_entry_id."

    def test_no_collision_across_six_distinct_inputs(self, double_pf):
        """base_entry_ids for all six rows across two sources are pairwise distinct."""
        db = InMemoryArrowDatabase()
        src1 = _make_source_stream([1, 2, 3])
        src2 = _make_source_stream([4, 5, 6])

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src1,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src2,
            pipeline_database=db,
        )

        all_ids = (
            list(_all_base_entry_ids(node1, src1))
            + list(_all_base_entry_ids(node2, src2))
        )
        assert len(all_ids) == len(set(all_ids)), (
            "All six base_entry_ids must be distinct — no collisions without "
            "NODE_CONTENT_HASH_COL."
        )


# ---------------------------------------------------------------------------
# Property 2 (forward-looking): preimage shape after ITL-533
#
# Documents the intended preimage structure after the fix lands.
# The test is expected to FAIL until the implementation is updated.
# ---------------------------------------------------------------------------


class TestPreimageShape:
    """Preimage column membership — documents the post-ITL-533 target state."""

    def test_node_content_hash_col_not_in_preimage_keys(self, double_pf):
        """After ITL-533: preimage = system_tags + INPUT_DATA_HASH_COL only.

        NODE_CONTENT_HASH_COL must be absent.  This test currently FAILS
        (confirming the pre-condition) and will pass once the implementation
        is updated.
        """
        src = _make_source_stream([10])
        node = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=InMemoryArrowDatabase(),
        )
        tag, data = next(iter(src.iter_data()))
        preimage = node._build_entry_id_preimage(tag, data)

        assert constants.INPUT_DATA_HASH_COL in preimage.column_names, (
            "INPUT_DATA_HASH_COL must be in the preimage."
        )
        assert constants.NODE_CONTENT_HASH_COL not in preimage.column_names, (
            "NODE_CONTENT_HASH_COL must NOT be in the preimage after ITL-533. "
            "This test fails until the implementation is updated."
        )

    def test_node_content_hash_lockstep_with_base_entry_id(self, double_pf):
        """NODE_CONTENT_HASH_COL is always in lockstep with base_entry_id.

        Same input (same source_id, same data) → same base_entry_id AND
        same NODE_CONTENT_HASH_COL.  There is no case where one differs
        without the other differing too: ArrowTableSource.identity_structure()
        = (class_name, schema, source_id), so equal source_ids + equal schemas
        always produce equal source content_hashes and therefore equal node
        content_hashes.
        """
        db = InMemoryArrowDatabase()
        src = _make_source_stream([10, 20, 30])

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=double_pf),
            input_stream=src,
            pipeline_database=db,
        )

        # Both nodes have the same content_hash (same source object → same upstream)
        assert node1.content_hash() == node2.content_hash()

        for tag, data in src.iter_data():
            eid1 = node1.compute_base_entry_id(tag, data)
            eid2 = node2.compute_base_entry_id(tag, data)
            # Same input → same base_entry_id ...
            assert eid1 == eid2
            # ... and same NODE_CONTENT_HASH_COL — the two are inseparable.
            p1 = node1._build_entry_id_preimage(tag, data)
            p2 = node2._build_entry_id_preimage(tag, data)
            nch1 = p1.column(constants.NODE_CONTENT_HASH_COL)[0].as_py()
            nch2 = p2.column(constants.NODE_CONTENT_HASH_COL)[0].as_py()
            assert nch1 == nch2, (
                "NODE_CONTENT_HASH_COL must be identical when base_entry_id is identical."
            )


# ---------------------------------------------------------------------------
# Property 3: _filter_by_content_hash() is a correctness no-op
#
# Disabling the filter (making it a pass-through) leaves pipeline results
# identical — same call counts, same output values.  This proves that
# base_entry_id filtering already provides all necessary isolation.
# ---------------------------------------------------------------------------


class TestFilterByContentHashIsNoOp:
    """_filter_by_content_hash() does not affect correctness — removing it is safe."""

    def _run_two_node_scenario(self, double_pf, patch_filter: bool) -> tuple[int, list[int]]:
        """Run node1 (n=3) then node2 (n=5, superset) sharing a pipeline DB.

        Returns (total_function_calls, sorted_node2_results).
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonDataFunction(counting_double, output_keys="result")
        db = InMemoryArrowDatabase()

        src1 = _make_source_stream(list(range(3)))  # rows x=0,1,2
        src2 = _make_source_stream(list(range(5)))  # rows x=0,1,2,3,4 (same source)

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=pf),
            input_stream=src1,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=pf),
            input_stream=src2,
            pipeline_database=db,
        )

        if patch_filter:
            # Disable _filter_by_content_hash: always return the table unchanged.
            with patch.object(
                FunctionJobNode,
                "_filter_by_content_hash",
                lambda self, table: table,
            ):
                node1.run()
                node2.run()
                results = sorted(
                    cast(int, p["result"]) for _, p in node2.iter_data()
                )
        else:
            node1.run()
            node2.run()
            results = sorted(
                cast(int, p["result"]) for _, p in node2.iter_data()
            )

        return call_count, results

    def test_filter_disabled_same_call_count(self, double_pf):
        """Disabling _filter_by_content_hash does not change the total function call count.

        With the filter enabled (current code):
          - node1 processes rows x={0,1,2}: 3 calls.
          - node2 Phase-1 finds no records (content_hash mismatch) → Phase-2 runs
            all 5; result cache hits for x={0,1,2}, new calls for x={3,4}: 2 more calls.
          - Total: 5 calls.

        With the filter disabled (proposed code):
          - node1 processes rows x={0,1,2}: 3 calls.
          - node2 Phase-1 finds node1's 3 records via matching base_entry_id →
            replays them directly; Phase-2 only runs x={3,4}: 2 more calls.
          - Total: 5 calls.

        Same total either way — the filter is a no-op for call count.
        """
        calls_with_filter, _ = self._run_two_node_scenario(double_pf, patch_filter=False)
        calls_without_filter, _ = self._run_two_node_scenario(double_pf, patch_filter=True)
        assert calls_with_filter == calls_without_filter, (
            f"Expected identical call counts; "
            f"with filter={calls_with_filter}, without={calls_without_filter}."
        )

    def test_filter_disabled_same_output_values(self, double_pf):
        """Disabling _filter_by_content_hash produces identical output values."""
        _, results_with = self._run_two_node_scenario(double_pf, patch_filter=False)
        _, results_without = self._run_two_node_scenario(double_pf, patch_filter=True)
        assert results_with == results_without, (
            f"Output values differ: with filter={results_with}, without={results_without}."
        )

    def test_filter_disabled_zero_recomputation_when_all_cached(self, double_pf):
        """When all inputs are already cached by node1, disabling the filter still
        yields zero additional function calls for node2.

        With filter enabled: node2 Phase-1 skips everything → Phase-2 runs 3,
        all result-cache hits → 0 new calls.
        With filter disabled: node2 Phase-1 finds all 3 records from node1 →
        replays directly → 0 new calls.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonDataFunction(counting_double, output_keys="result")
        db = InMemoryArrowDatabase()

        src = _make_source_stream(list(range(3)))  # shared source — same source_id

        node1 = FunctionJobNode(
            function_pod=FunctionPod(data_function=pf),
            input_stream=src,
            pipeline_database=db,
        )
        node2 = FunctionJobNode(
            function_pod=FunctionPod(data_function=pf),
            input_stream=src,
            pipeline_database=db,
        )

        with patch.object(
            FunctionJobNode,
            "_filter_by_content_hash",
            lambda self, table: table,
        ):
            node1.run()
            calls_after_node1 = call_count

            node2.run()
            assert call_count == calls_after_node1, (
                "With filter disabled, node2 must still produce zero additional "
                "function calls when all inputs were already computed by node1."
            )
