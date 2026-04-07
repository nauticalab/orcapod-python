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

  Phase 5 — FunctionNode and THE CORE FIX
    FunctionNode.pipeline_path is derived from pipeline_hash, not content_hash
    Two FunctionNodes with same schema/function but different data share pipeline_path
    They also share the DB: node1's cached results are reused by node2

  End-to-end DB scoping
    The complete behaviour: fill half the pipeline via node1, verify node2
    reuses node1's results and only computes the remaining entries.
"""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import ContentHash

from ..conftest import make_int_stream, make_two_col_stream

# ---------------------------------------------------------------------------
# Phase 1: PipelineElementBase — basic invariants
# ---------------------------------------------------------------------------


class TestPipelineElementBase:
    """Verify PipelineElementBase invariants on concrete instances."""

    def test_function_node_pipeline_hash_returns_content_hash(self, double_pf):
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        h = node.pipeline_hash()
        assert isinstance(h, ContentHash)

    def test_pipeline_hash_is_cached(self, double_pf):
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        assert node.pipeline_hash() is node.pipeline_hash()

    def test_pipeline_hash_not_equal_to_content_hash(self, double_pf):
        """pipeline_hash (schema+topology) must differ from content_hash (data-inclusive)
        when the input stream contains real data."""
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        assert node.pipeline_hash() != node.content_hash()

    def test_source_satisfies_pipeline_element_protocol(self, double_pf):
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=InMemoryArrowDatabase(),
        )
        assert isinstance(node, PipelineElementProtocol)

    def test_root_source_satisfies_pipeline_element_protocol(self):
        src = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}),
            infer_nullable=True,
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
        node_double = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node_add = FunctionNode(
            function_pod=FunctionPod(packet_function=add_pf),
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
        src1 = ArrowTableSource(table=t1, infer_nullable=True)
        src2 = ArrowTableSource(table=t2, infer_nullable=True)
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_same_schema_different_data_different_content_hash(self):
        """Counterpart: same schema → same pipeline_hash but different content_hash."""
        t1 = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        t2 = pa.table({"x": pa.array([99, 100, 101], type=pa.int64())})
        src1 = ArrowTableSource(table=t1, infer_nullable=True)
        src2 = ArrowTableSource(table=t2, infer_nullable=True)
        assert src1.content_hash() != src2.content_hash()

    def test_different_schema_different_pipeline_hash(self):
        src_x = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2], type=pa.int64())}),
            infer_nullable=True,
        )
        src_y = ArrowTableSource(
            table=pa.table({"y": pa.array([1, 2], type=pa.int64())}),
            infer_nullable=True,
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
        src_with_tag = ArrowTableSource(table=table, tag_columns=["id"], infer_nullable=True)
        src_no_tag = ArrowTableSource(table=table, infer_nullable=True)
        assert src_with_tag.pipeline_hash() != src_no_tag.pipeline_hash()

    def test_dict_source_same_schema_shares_pipeline_hash_with_arrow_source(self):
        """DictSource and ArrowTableSource with identical schemas share pipeline_hash."""
        arrow_table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        arrow_src = ArrowTableSource(table=arrow_table, infer_nullable=True)

        dict_src = DictSource(
            data=[{"x": 10}, {"x": 20}, {"x": 30}],
            data_schema={"x": int},
        )
        # Both have packet schema {x: int64}, no tag columns → same pipeline_hash
        assert arrow_src.pipeline_hash() == dict_src.pipeline_hash()

    def test_pipeline_hash_stable_across_instances(self):
        t = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src1 = ArrowTableSource(table=t, infer_nullable=True)
        src2 = ArrowTableSource(table=t, infer_nullable=True)
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
                },
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("x", pa.int64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["id"],
        )
        assert s1.pipeline_hash() == s2.pipeline_hash()
        assert s1.content_hash() != s2.content_hash()

    def test_table_stream_pipeline_hash_equals_source_pipeline_hash(self):
        """ArrowTableStream backed by a source should inherit the source's pipeline_hash
        at the stream level (it is the RootSource itself here)."""
        _schema = pa.schema([pa.field("x", pa.int64(), nullable=False)])
        src = ArrowTableSource(
            table=pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}, schema=_schema)
        )
        # The source IS a stream; its pipeline_hash is schema-only
        s = ArrowTableStream(pa.table({"x": pa.array([1, 2, 3], type=pa.int64())}, schema=_schema))
        # Both have same schema, so same pipeline_hash
        assert src.pipeline_hash() == s.pipeline_hash()


# ---------------------------------------------------------------------------
# Phase 5: FunctionNode — the core DB-scoping fix
# ---------------------------------------------------------------------------


class TestFunctionNodePipelineHashFix:
    """
    With ``table_scope="pipeline_hash"`` (the default), nodes sharing the same
    pipeline structure (same function + same input schema) route to a single
    shared DB table.  The path formula is:

      prefix + pf.uri + (f"schema:{pipeline_hash}",)

    Per-run isolation is achieved via the ``_node_content_hash`` row-level
    column stored in every pipeline record.
    """

    def test_different_data_same_schema_share_pipeline_path(self, double_pf):
        db = InMemoryArrowDatabase()
        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=5),
            pipeline_database=db,
        )
        # Same schema → same pipeline_hash → SAME node_identity_path
        assert node1.node_identity_path == node2.node_identity_path
        assert node1.node_identity_path[-1].startswith("schema:")
        # Per-run disambiguation via content_hash (not in the path)
        assert node1.content_hash() != node2.content_hash()

    def test_different_data_same_schema_share_uri(self, double_pf):
        """With pipeline_hash scope, two nodes with same schema share the full path."""
        db = InMemoryArrowDatabase()
        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=ArrowTableStream(
                pa.table(
                    {
                        "id": pa.array([10, 11, 12, 13], type=pa.int64()),
                        "x": pa.array([100, 200, 300, 400], type=pa.int64()),
                    },
                    schema=pa.schema(
                        [
                            pa.field("id", pa.int64(), nullable=False),
                            pa.field("x", pa.int64(), nullable=False),
                        ]
                    ),
                ),
                tag_columns=["id"],
            ),
            pipeline_database=db,
        )
        # With pipeline_hash scope, same function + schema → identical full path
        assert node1.node_identity_path == node2.node_identity_path

    def test_different_data_yields_different_content_hash(self, double_pf):
        """Same schema, different actual data → content_hash must differ."""
        db = InMemoryArrowDatabase()
        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=ArrowTableStream(
                pa.table(
                    {
                        "id": pa.array([10, 11, 12], type=pa.int64()),
                        "x": pa.array([100, 200, 300], type=pa.int64()),
                    },
                    schema=pa.schema(
                        [
                            pa.field("id", pa.int64(), nullable=False),
                            pa.field("x", pa.int64(), nullable=False),
                        ]
                    ),
                ),
                tag_columns=["id"],
            ),
            pipeline_database=db,
        )
        assert node1.content_hash() != node2.content_hash()

    def test_different_function_different_pipeline_path(self, double_pf, add_pf):
        """Different functions → different pipeline_hash → different pipeline_path."""
        db = InMemoryArrowDatabase()
        node_double = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node_add = FunctionNode(
            function_pod=FunctionPod(packet_function=add_pf),
            input_stream=make_two_col_stream(n=3),
            pipeline_database=db,
        )
        assert node_double.node_identity_path != node_add.node_identity_path

    def test_node_identity_path_starts_with_pf_uri(self, double_pf):
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=2),
            pipeline_database=InMemoryArrowDatabase(),
        )
        pf_uri = node._packet_function.uri
        assert node.node_identity_path[: len(pf_uri)] == pf_uri
        # With pipeline_hash scope (default): path ends with schema:{hash} only
        assert node.node_identity_path[-1].startswith("schema:")
        assert not any(
            seg.startswith("instance:") for seg in node.node_identity_path
        )


# ---------------------------------------------------------------------------
# End-to-end DB scoping: the definitive pipeline fix test
# ---------------------------------------------------------------------------


class TestPipelineDbScoping:
    """
    With the two-level pipeline_path formula, each data-distinct FunctionNode
    writes to its own DB table (different instance: suffix). Nodes with the same
    data (identical content_hash) share a path; nodes with different data do not.
    """

    def test_shared_db_overlapping_inputs_avoids_recomputation(self, double_pf):
        """
        node1 processes {0,1,2}. node2 processes {0,1,2,3,4}.
        With pipeline_hash scope (default), node1 and node2 share the SAME
        node_identity_path (same function + schema).  Per-run isolation happens
        via the ``_node_content_hash`` row column.  node2 Phase 1 finds no
        matching records (different content_hash), so Phase 2 executes all 5;
        however the result DB (CachedFunctionPod) is shared, so only x=3 and
        x=4 require actual function invocations.
        Total function calls: 3 (node1) + 2 (node2).
        """
        call_count = 0

        def counting_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        pf = PythonPacketFunction(counting_double, output_keys="result")
        db = InMemoryArrowDatabase()

        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=pf),
            input_stream=make_int_stream(n=3),  # x in {0,1,2}
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=pf),
            input_stream=make_int_stream(n=5),  # x in {0,1,2,3,4}
            pipeline_database=db,
        )

        # Same schema → same pipeline_hash → same node_identity_path (shared table)
        assert node1.node_identity_path == node2.node_identity_path
        assert node1.node_identity_path[-1].startswith("schema:")

        node1.run()
        assert call_count == 3

        node2.run()
        # node2 Phase 1 finds no records for its content_hash → Phase 2 runs all 5.
        # CachedFunctionPod only invokes the function for novel packets (x=3, x=4).
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

        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=pf),
            input_stream=make_int_stream(n=5),
            pipeline_database=db,
        )
        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=pf),
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

        node1 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node1.run()

        node2 = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=5),
            pipeline_database=db,
        )
        node2.run()
        results = sorted(cast(int, p["result"]) for _, p in node2.iter_packets())
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

        FunctionNode(
            function_pod=FunctionPod(packet_function=pf),
            input_stream=make_int_stream(n=n),
            pipeline_database=InMemoryArrowDatabase(),
        ).run()

        FunctionNode(
            function_pod=FunctionPod(packet_function=pf),
            input_stream=make_int_stream(n=n),
            pipeline_database=InMemoryArrowDatabase(),
        ).run()

        assert call_count == n * 2  # no sharing: 3 + 3

    def test_pipeline_hash_chain_root_to_function_node(self, double_pf):
        """
        Verify the full Merkle-like chain:
          RootSource.pipeline_hash  →  ArrowTableStream.pipeline_hash
            →  FunctionNode.pipeline_hash

        Two pipelines (same schema, different data) must share pipeline_hash
        at every level of the chain.
        """
        db = InMemoryArrowDatabase()

        stream_a = make_int_stream(n=3)
        stream_b = make_int_stream(n=7)  # same schema, different count

        # Level 0 (root): same schema → same pipeline_hash
        assert stream_a.pipeline_hash() == stream_b.pipeline_hash()

        node_a = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream_a,
            pipeline_database=db,
        )
        node_b = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream_b,
            pipeline_database=db,
        )

        # Level 1 (function node): same function + same input schema → same pipeline_hash
        assert node_a.pipeline_hash() == node_b.pipeline_hash()

        # Content hashes must differ (different actual data)
        assert node_a.content_hash() != node_b.content_hash()

    def test_chained_nodes_share_pipeline_path(self, double_pf):
        """
        Two independent two-node pipelines with same schema but different data
        share the same node_identity_path (pipeline_hash scope default).
        Per-run isolation is via _node_content_hash at row level.
        """
        db = InMemoryArrowDatabase()

        # Pipeline A: stream(n=3) → node1_a → source_a → node2_a
        stream_a = make_int_stream(n=3)
        node1_a = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream_a,
            pipeline_database=db,
        )
        node1_a.run()
        src_a = node1_a.as_source()

        # Pipeline B: stream(n=5) → node1_b → source_b → node2_b
        stream_b = make_int_stream(n=5)
        node1_b = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=stream_b,
            pipeline_database=db,
        )
        node1_b.run()
        src_b = node1_b.as_source()

        # Same schema → same pipeline_hash → SAME full path (shared table)
        assert node1_a.node_identity_path == node1_b.node_identity_path
        assert node1_a.node_identity_path[-1].startswith("schema:")

        # At the DerivedSource level, pipeline_hash is schema-only
        assert src_a.pipeline_hash() == src_b.pipeline_hash()
