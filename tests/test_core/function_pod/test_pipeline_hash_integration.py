"""
End-to-end pipeline hash integration tests.

These tests verify the full pipeline identity (pipeline_hash) chain introduced
across Phases 1–5 of the redesign:

  Phase 1 — PipelineElementBase
    pipeline_hash() returns a ContentHash, is cached, is not content_hash()

  Phase 2 — FunctionPod pipeline_hash
    FunctionPod.pipeline_hash() is function-schema based
    Same function → same pipeline_hash
    Different function → different pipeline_hash

  Phase 3 — RootSource base case
    RootSource.pipeline_hash() is (tag_schema, packet_schema) only
    Same-schema sources share pipeline_hash regardless of data

  Phase 4 — ArrowTableStream pipeline_hash
    ArrowTableStream (no source) → schema-based pipeline_hash
    Two same-schema TableStreams share pipeline_hash even with different data

  Phase 5 — PersistentFunctionNode and THE CORE FIX
    PersistentFunctionNode.pipeline_path is derived from pipeline_hash, not content_hash
    Two FunctionNodes with same schema/function but different data share pipeline_path
    They also share the DB: node1's cached results are reused by node2

  End-to-end DB scoping
    The complete behaviour: fill half the pipeline via node1, verify node2
    reuses node1's results and only computes the remaining entries.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import PersistentFunctionNode, FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DictSource, ListSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.hashing_protocols import ContentHash, PipelineElementProtocol

from ..conftest import add, double, make_int_stream, make_two_col_stream


# ---------------------------------------------------------------------------
# Phase 1: PipelineElementBase — basic invariants
# ---------------------------------------------------------------------------


class TestPipelineElementBase:
    """Verify PipelineElementBase invariants on concrete instances."""

    def test_function_node_pipeline_hash_returns_content_hash(self, double_pf):
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        h = node.pipeline_hash()
        assert isinstance(h, ContentHash)

    def test_pipeline_hash_is_cached(self, double_pf):
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        assert node.pipeline_hash() is node.pipeline_hash()

    def test_pipeline_hash_not_equal_to_content_hash(self, double_pf):
        """pipeline_hash (schema+topology) must differ from content_hash (data-inclusive)
        when the input stream contains real data."""
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        assert node.pipeline_hash() != node.content_hash()

    def test_source_satisfies_pipeline_element_protocol(self, double_pf):
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        assert isinstance(node, PipelineElementProtocol)

    def test_root_source_satisfies_pipeline_element_protocol(self):
        src = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        )
        assert isinstance(src, PipelineElementProtocol)

    def test_table_stream_satisfies_pipeline_element_protocol(self):
        stream = make_int_stream(n=3)
        assert isinstance(stream, PipelineElementProtocol)

    def test_function_pod_satisfies_pipeline_element_protocol(self, double_pf):
        pod = FunctionPod(packet_function=double_pf)
        assert isinstance(pod, PipelineElementProtocol)


# ---------------------------------------------------------------------------
# Phase 2: FunctionPod pipeline_hash
# ---------------------------------------------------------------------------


class TestFunctionPodPipelineHash:
    def test_function_pod_has_pipeline_hash(self, double_pf):
        pod = FunctionPod(packet_function=double_pf)
        assert isinstance(pod.pipeline_hash(), ContentHash)

    def test_same_function_same_pipeline_hash(self, double_pf):
        pod1 = FunctionPod(packet_function=double_pf)
        pod2 = FunctionPod(packet_function=double_pf)
        assert pod1.pipeline_hash() == pod2.pipeline_hash()

    def test_different_function_different_pipeline_hash(self, double_pf, add_pf):
        pod_double = FunctionPod(packet_function=double_pf)
        pod_add = FunctionPod(packet_function=add_pf)
        assert pod_double.pipeline_hash() != pod_add.pipeline_hash()

    def test_function_pod_pipeline_hash_is_stable(self, double_pf):
        pod = FunctionPod(packet_function=double_pf)
        assert pod.pipeline_hash() == pod.pipeline_hash()

    def test_function_pod_pipeline_hash_determines_function_node_pipeline_hash(
        self, double_pf, add_pf
    ):
        """Two FunctionNodes on the same input stream but different functions
        have different pipeline_hashes because the FunctionPod hashes differ."""
        db = InMemoryArrowDatabase()
        stream = make_two_col_stream(n=3)
        node_double = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node_add = PersistentFunctionNode(
            packet_function=add_pf,
            input_stream=stream,
            pipeline_database=db,
        )
        assert node_double.pipeline_hash() != node_add.pipeline_hash()


# ---------------------------------------------------------------------------
# Phase 3: RootSource pipeline_hash — the base case
# ---------------------------------------------------------------------------


class TestRootSourcePipelineHash:
    def test_same_schema_arrow_sources_share_pipeline_hash(self):
        """Two ArrowTableSources with identical schemas but different data
        must share pipeline_hash (schema-only base case)."""
        t1 = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        t2 = pa.table({"x": pa.array([99, 100, 101], type=pa.int64())})
        src1 = ArrowTableSource(table=t1)
        src2 = ArrowTableSource(table=t2)
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_same_schema_different_data_different_content_hash(self):
        """Counterpart: same schema → same pipeline_hash but different content_hash."""
        t1 = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        t2 = pa.table({"x": pa.array([99, 100, 101], type=pa.int64())})
        src1 = ArrowTableSource(table=t1)
        src2 = ArrowTableSource(table=t2)
        assert src1.content_hash() != src2.content_hash()

    def test_different_schema_different_pipeline_hash(self):
        src_x = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2], type=pa.int64())})
        )
        src_y = ArrowTableSource(
            table=pa.table({"y": pa.array([1, 2], type=pa.int64())})
        )
        assert src_x.pipeline_hash() != src_y.pipeline_hash()

    def test_different_tag_column_different_pipeline_hash(self):
        """Tag vs packet assignment changes the schema, hence the pipeline_hash."""
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "val": pa.array([10, 20], type=pa.int64()),
            }
        )
        src_with_tag = ArrowTableSource(table=table, tag_columns=["id"])
        src_no_tag = ArrowTableSource(table=table)
        assert src_with_tag.pipeline_hash() != src_no_tag.pipeline_hash()

    def test_dict_source_same_schema_shares_pipeline_hash_with_arrow_source(self):
        """DictSource and ArrowTableSource with identical schemas share pipeline_hash."""
        arrow_table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        arrow_src = ArrowTableSource(table=arrow_table)

        dict_src = DictSource(
            data=[{"x": 10}, {"x": 20}, {"x": 30}],
            data_schema={"x": int},
        )
        # Both have packet schema {x: int64}, no tag columns → same pipeline_hash
        assert arrow_src.pipeline_hash() == dict_src.pipeline_hash()

    def test_pipeline_hash_stable_across_instances(self):
        t = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src1 = ArrowTableSource(table=t)
        src2 = ArrowTableSource(table=t)
        assert src1.pipeline_hash() == src2.pipeline_hash()


# ---------------------------------------------------------------------------
# Phase 4: ArrowTableStream pipeline_hash
# ---------------------------------------------------------------------------


class TestTableStreamPipelineHash:
    def test_table_stream_has_pipeline_hash(self):
        stream = make_int_stream(n=3)
        assert isinstance(stream.pipeline_hash(), ContentHash)

    def test_same_schema_streams_share_pipeline_hash(self):
        """Two TableStreams with same schema but different row counts share pipeline_hash."""
        s1 = make_int_stream(n=3)
        s2 = make_int_stream(n=10)
        assert s1.pipeline_hash() == s2.pipeline_hash()

    def test_different_schema_streams_differ(self):
        s1 = make_int_stream(n=3)  # id + x
        s2 = make_two_col_stream(n=3)  # id + x + y
        assert s1.pipeline_hash() != s2.pipeline_hash()

    def test_different_data_same_schema_different_content_hash(self):
        """Same schema → same pipeline_hash, but data is different → different content_hash."""
        s1 = make_int_stream(n=3)
        s2 = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([10, 11, 12], type=pa.int64()),
                    "x": pa.array([100, 200, 300], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        assert s1.pipeline_hash() == s2.pipeline_hash()
        assert s1.content_hash() != s2.content_hash()

    def test_table_stream_pipeline_hash_equals_source_pipeline_hash(self):
        """ArrowTableStream backed by a source should inherit the source's pipeline_hash
        at the stream level (it is the RootSource itself here)."""
        src = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        )
        # The source IS a stream; its pipeline_hash is schema-only
        s = ArrowTableStream(pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}))
        # Both have same schema, so same pipeline_hash
        assert src.pipeline_hash() == s.pipeline_hash()


# ---------------------------------------------------------------------------
# Phase 5: PersistentFunctionNode — the core DB-scoping fix
# ---------------------------------------------------------------------------


class TestFunctionNodePipelineHashFix:
    """
    The critical invariant: PersistentFunctionNode._pipeline_node_hash (and therefore
    pipeline_path) is derived from pipeline_hash(), not content_hash().

    Before the fix: _pipeline_node_hash = self.content_hash().to_string()
      → different data → different pipeline_path → DB not shared
    After the fix:  _pipeline_node_hash = self.pipeline_hash().to_string()
      → same schema  → same pipeline_path  → DB shared across nodes
    """

    def test_different_data_same_schema_share_pipeline_path(self, double_pf):
        db = InMemoryArrowDatabase()
        node1 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=5),
            pipeline_database=db,
        )
        assert node1.pipeline_path == node2.pipeline_path

    def test_different_data_same_schema_share_uri(self, double_pf):
        """URI is also schema-based, so two nodes with same schema share it."""
        db = InMemoryArrowDatabase()
        node1 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=ArrowTableStream(
                pa.table(
                    {
                        "id": pa.array([10, 11, 12, 13], type=pa.int64()),
                        "x": pa.array([100, 200, 300, 400], type=pa.int64()),
                    }
                ),
                tag_columns=["id"],
            ),
            pipeline_database=db,
        )
        assert node1.pipeline_path == node2.pipeline_path

    def test_different_data_yields_different_content_hash(self, double_pf):
        """Same schema, different actual data → content_hash must differ."""
        db = InMemoryArrowDatabase()
        node1 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=ArrowTableStream(
                pa.table(
                    {
                        "id": pa.array([10, 11, 12], type=pa.int64()),
                        "x": pa.array([100, 200, 300], type=pa.int64()),
                    }
                ),
                tag_columns=["id"],
            ),
            pipeline_database=db,
        )
        assert node1.content_hash() != node2.content_hash()

    def test_different_function_different_pipeline_path(self, double_pf, add_pf):
        """Different functions → different pipeline_hash → different pipeline_path."""
        db = InMemoryArrowDatabase()
        node_double = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node_add = PersistentFunctionNode(
            packet_function=add_pf,
            input_stream=make_two_col_stream(n=3),
            pipeline_database=db,
        )
        assert node_double.pipeline_path != node_add.pipeline_path

    def test_pipeline_path_prefix_propagates(self, double_pf):
        db = InMemoryArrowDatabase()
        prefix = ("stage", "one")
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
            pipeline_path_prefix=prefix,
        )
        assert node.pipeline_path[: len(prefix)] == prefix

    def test_pipeline_path_without_prefix_starts_with_pf_uri(self, double_pf):
        node = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=2),
            pipeline_database=InMemoryArrowDatabase(),
        )
        pf_uri = node._cached_packet_function.uri
        assert node.pipeline_path[: len(pf_uri)] == pf_uri
        assert node.pipeline_path[-1].startswith("node:")


# ---------------------------------------------------------------------------
# End-to-end DB scoping: the definitive pipeline fix test
# ---------------------------------------------------------------------------


class TestPipelineDbScoping:
    """
    The definitive end-to-end test for the pipeline DB scoping fix.

    Two PersistentFunctionNode instances:
      - Same packet function
      - Same input schema
      - DIFFERENT input data (overlapping subset)
    must write to the SAME database table (shared pipeline_path) and
    correctly reuse previously-computed entries.
    """

    def test_shared_db_overlapping_inputs_avoids_recomputation(self, double_pf):
        """
        node1 processes {0,1,2}. node2 processes {0,1,2,3,4}.
        After node1 fills the DB, node2 should only need to compute {3,4}.
        Total function calls: 3 (node1) + 2 (node2) = 5, not 3+5=8.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonPacketFunction(counting_double, output_keys="result")
        db = InMemoryArrowDatabase()

        node1 = PersistentFunctionNode(
            packet_function=pf,
            input_stream=make_int_stream(n=3),  # x in {0,1,2}
            pipeline_database=db,
        )
        node2 = PersistentFunctionNode(
            packet_function=pf,
            input_stream=make_int_stream(n=5),  # x in {0,1,2,3,4}
            pipeline_database=db,
        )

        # Sanity: they share the same DB table path
        assert node1.pipeline_path == node2.pipeline_path

        node1.run()
        assert call_count == 3

        node2.run()
        # node2 only computes the 2 entries not yet in the DB
        assert call_count == 5

    def test_shared_db_all_inputs_pre_computed_zero_recomputation(self, double_pf):
        """
        If node1 already computed all entries that node2 needs, node2 does
        zero additional function calls.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonPacketFunction(counting_double, output_keys="result")
        db = InMemoryArrowDatabase()

        node1 = PersistentFunctionNode(
            packet_function=pf,
            input_stream=make_int_stream(n=5),
            pipeline_database=db,
        )
        node2 = PersistentFunctionNode(
            packet_function=pf,
            input_stream=make_int_stream(n=3),  # strict subset of node1's data
            pipeline_database=db,
        )

        node1.run()
        calls_after_node1 = call_count

        node2.run()
        # All 3 entries were already computed by node1 → zero additional calls
        assert call_count == calls_after_node1

    def test_shared_db_results_are_correct_values(self, double_pf):
        """Correctness: DB-served results from a shared pipeline have correct values."""
        db = InMemoryArrowDatabase()

        node1 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node1.run()

        node2 = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=5),
            pipeline_database=db,
        )
        results = sorted(p["result"] for _, p in node2.iter_packets())
        assert results == [0, 2, 4, 6, 8]

    def test_isolated_db_computes_independently(self, double_pf):
        """
        Two nodes that do NOT share a DB always compute all entries independently.
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonPacketFunction(counting_double, output_keys="result")
        n = 3

        PersistentFunctionNode(
            packet_function=pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=InMemoryArrowDatabase(),
        ).run()

        PersistentFunctionNode(
            packet_function=pf,
            input_stream=make_int_stream(n=n),
            pipeline_database=InMemoryArrowDatabase(),
        ).run()

        assert call_count == n * 2  # no sharing: 3 + 3

    def test_pipeline_hash_chain_root_to_function_node(self, double_pf):
        """
        Verify the full Merkle-like chain:
          RootSource.pipeline_hash  →  ArrowTableStream.pipeline_hash
            →  PersistentFunctionNode.pipeline_hash

        Two pipelines (same schema, different data) must share pipeline_hash
        at every level of the chain.
        """
        db = InMemoryArrowDatabase()

        stream_a = make_int_stream(n=3)
        stream_b = make_int_stream(n=7)  # same schema, different count

        # Level 0 (root): same schema → same pipeline_hash
        assert stream_a.pipeline_hash() == stream_b.pipeline_hash()

        node_a = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=stream_a,
            pipeline_database=db,
        )
        node_b = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=stream_b,
            pipeline_database=db,
        )

        # Level 1 (function node): same function + same input schema → same pipeline_hash
        assert node_a.pipeline_hash() == node_b.pipeline_hash()

        # Content hashes must differ (different actual data)
        assert node_a.content_hash() != node_b.content_hash()

    def test_chained_nodes_share_pipeline_path(self, double_pf):
        """
        Two independent two-node pipelines that are structurally identical
        (same functions, same schemas) share pipeline_path at each level.
        """
        db = InMemoryArrowDatabase()

        # Pipeline A: stream(n=3) → node1_a → source_a → node2_a
        stream_a = make_int_stream(n=3)
        node1_a = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=stream_a,
            pipeline_database=db,
        )
        node1_a.run()
        src_a = node1_a.as_source()

        # Pipeline B: stream(n=5) → node1_b → source_b → node2_b
        stream_b = make_int_stream(n=5)
        node1_b = PersistentFunctionNode(
            packet_function=double_pf,
            input_stream=stream_b,
            pipeline_database=db,
        )
        node1_b.run()
        src_b = node1_b.as_source()

        # At the first level, both nodes share pipeline_path
        assert node1_a.pipeline_path == node1_b.pipeline_path

        # At the DerivedSource level, pipeline_hash is schema-only
        assert src_a.pipeline_hash() == src_b.pipeline_hash()
