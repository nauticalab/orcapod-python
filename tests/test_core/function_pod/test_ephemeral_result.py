"""Tests for FunctionJobNode ephemeral result store — ITL-507."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.datagrams import Data, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def double(x: int) -> int:
    return x * 2


def _make_pod(config: NodeConfig | None = None):
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf, node_config=config)


def _make_stream(rows: list[dict], tag_columns: list[str] | None = None) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    keys = list(rows[0].keys())
    schema = pa.schema([pa.field(k, pa.int64(), nullable=False) for k in keys])
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in keys},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=tag_columns)


def _make_source_stream(rows: list[dict], tag_columns: list[str] | None = None) -> ArrowTableStream:
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    source = ArrowTableSource(table, tag_columns=tag_columns, source_id="test_src", infer_nullable=True)
    return source


def _make_node(stream, pipeline_db=None, result_db=None, ephemeral_result: bool = False):
    """Create a FunctionJobNode with given DB configuration."""
    cfg = NodeConfig(ephemeral_result=ephemeral_result)
    pod = _make_pod(config=cfg)
    if pipeline_db is None:
        pipeline_db = InMemoryArrowDatabase()
    return FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db if result_db is not None else pipeline_db,
    ), pipeline_db


# ---------------------------------------------------------------------------
# Task 4 test: no-op set_ephemeral_store on blueprint node classes
# ---------------------------------------------------------------------------

class TestNoOpSetEphemeralStore:
    def test_function_node_has_set_ephemeral_store(self):
        """FunctionNode (blueprint) must have set_ephemeral_store as a no-op."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)

    def test_operator_node_has_set_ephemeral_store(self):
        """OperatorNode (blueprint) must have set_ephemeral_store as a no-op."""
        from orcapod.core.nodes import OperatorNode
        from orcapod.core.operators import Join

        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 0, "y": 20}])
        op = Join()
        node = OperatorNode(operator=op, input_streams=(stream_a, stream_b))
        store = InMemoryArrowDatabase()
        # Must not raise
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)


# ---------------------------------------------------------------------------
# Task 5 test: FunctionJobNode set_ephemeral_store real override
# ---------------------------------------------------------------------------

class TestSetEphemeralStore:
    def test_set_ephemeral_store_assigns_store(self):
        """set_ephemeral_store(store) assigns the ephemeral_result_store attribute."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_store(store)
        assert node.ephemeral_result_store is store

    def test_set_ephemeral_store_none_detaches(self):
        """set_ephemeral_store(None) removes the ephemeral store."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        store = InMemoryArrowDatabase()
        node.set_ephemeral_store(store)
        node.set_ephemeral_store(None)
        assert node.ephemeral_result_store is None


# ---------------------------------------------------------------------------
# Task 6 test: IS_EPHEMERAL_COL written to pipeline DB
# ---------------------------------------------------------------------------

class TestAddPipelineRecord:
    def test_is_ephemeral_false_written_to_pipeline_db(self):
        """add_pipeline_record(is_ephemeral=False) stores IS_EPHEMERAL_COL = False."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        node, db = _make_node(stream)
        results = node.execute(stream)
        assert len(results) == 1

        all_records = db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is False for v in vals)

    def test_is_ephemeral_true_written_to_pipeline_db(self):
        """When ephemeral_result=True, IS_EPHEMERAL_COL=True is stored in the tag table."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db, ephemeral_result=True)
        node.set_ephemeral_store(ephemeral_store)

        results = node.execute(stream)
        assert len(results) == 1

        all_records = pipeline_db.get_all_records(node.node_identity_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in vals)


# ---------------------------------------------------------------------------
# Task 7 tests: two-store join
# ---------------------------------------------------------------------------

class TestBulkResolution:
    def test_ephemeral_false_unchanged(self):
        """ephemeral_result=False: execute() behaves identically to current implementation."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node, _ = _make_node(stream)
        results = node.execute(stream)
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_ephemeral_result_written_to_memory_not_persistent_db(self):
        """With ephemeral_result=True, persistent DB has no result rows; ephemeral store does."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        results = node.execute(stream)

        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

        # Persistent result DB must be empty (no writes there)
        eph_cache = node._ephemeral_cached_pod
        assert eph_cache is not None
        assert eph_cache.result_database.get_all_records(eph_cache.record_path) is not None
        assert result_db.get_all_records(node._cached_function_pod.record_path) is None

    def test_within_session_ephemeral_hit(self):
        """Same node called twice: second call hits ephemeral store — no recomputation."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, node_config=cfg)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)

        node.execute(stream)
        assert call_count["n"] == 1

        # Second execution — same entry_id — must hit cache
        node._cached_output_datas.clear()  # clear in-memory cache to force DB lookup
        node.execute(stream)
        assert call_count["n"] == 1  # function must NOT have been called again

    def test_cross_session_miss_recomputes(self):
        """Fresh InMemoryArrowDatabase (new session): ephemeral miss triggers recomputation."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1: execute with ephemeral store
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, node_config=cfg)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh in-memory node with a fresh ephemeral store
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2, node_config=cfg)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.set_ephemeral_store(InMemoryArrowDatabase())  # fresh store
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed

    def test_persistent_hit_served_when_ephemeral_true(self):
        """A persistent result is still served from cache when ephemeral store is also set."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)  # ephemeral_result=False (default)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        # Attach an ephemeral store — must NOT break persistent reads
        node.set_ephemeral_store(InMemoryArrowDatabase())

        # Run 1: writes to persistent DB (ephemeral_result=False)
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1

        # Clear in-memory cache to force DB lookup on Run 2
        node._cached_output_datas.clear()

        # Run 2: Phase 1 must find persistent result — no recompute
        results2 = node.execute(stream)
        assert len(results2) == 1
        assert results2[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1  # NOT recomputed

    def test_bulk_resolution_mixed_stores(self):
        """Tag table has both persistent and ephemeral entries; both resolve correctly."""
        stream_a = _make_stream([{"id": 0, "x": 10}])
        stream_b = _make_stream([{"id": 1, "x": 20}])
        pipeline_db = InMemoryArrowDatabase()

        # id=0 → persistent
        node_p, _ = _make_node(stream_a, pipeline_db=pipeline_db, ephemeral_result=False)
        node_p.execute(stream_a)

        # id=1 → ephemeral, reusing same pipeline_db
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, node_config=cfg)
        node_e = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_b,
            pipeline_database=pipeline_db,
        )
        node_e.set_ephemeral_store(ephemeral_store)
        node_e.execute(stream_b)

        # Verify id=1 result is in ephemeral_store (not persistent)
        eph_cache = node_e._ephemeral_cached_pod
        assert eph_cache is not None
        assert eph_cache.result_database.get_all_records(eph_cache.record_path) is not None

        # Now load both results via a combined stream
        stream_both = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node_both = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_both,
            pipeline_database=pipeline_db,
        )
        node_both.set_ephemeral_store(ephemeral_store)
        node_both._cached_output_datas.clear()

        results = node_both.execute(stream_both)
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_persistent_result_outcompetes_ephemeral(self):
        """When both persistent and ephemeral entries exist for the same entry_id, persistent wins.

        This tests the anti-join merge: a tag table row with IS_EPHEMERAL=True that shares
        an entry_id with an IS_EPHEMERAL=False row should be excluded from available_results.
        """
        # Use a single node to write both a persistent AND an ephemeral entry for id=0.
        # We do this by:
        #   1. Execute with ephemeral store attached but ephemeral_result=False → writes persistent entry
        #   2. Manually insert an ephemeral tag-table row + ephemeral store result to simulate
        #      the scenario where both exist (e.g. after a recovery run).
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()

        # Write a persistent entry
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        node.execute(stream)  # writes IS_EPHEMERAL=False persistent entry
        assert call_count["n"] == 1

        # Simulate an ephemeral entry also existing for the same input
        # by calling add_pipeline_record directly with is_ephemeral=True.
        # (This represents the "recovery scenario" from the spec.)
        import uuid as _uuid
        tag = Tag({"id": 0})
        data = Data({"x": 10})
        fake_uuid = _uuid.UUID(int=0)  # dummy UUID, no actual result stored
        node.add_pipeline_record(
            tag, data,
            data_record_id=fake_uuid,
            computed=True,
            skip_cache_lookup=True,
            is_ephemeral=True,
        )

        # Clear in-memory cache to force fresh DB lookup
        node._cached_output_datas.clear()

        # Phase 1: both persistent (result=20) and ephemeral (fake UUID, no actual result)
        # entries exist. Persistent should win — ephemeral entry excluded by anti-join.
        # Since fake_uuid has no actual result in ephemeral_store, the ephemeral join
        # produces no row for it. The persistent join wins, and result=20 is returned.
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 1  # NOT recomputed


# ---------------------------------------------------------------------------
# Task 8 tests: ephemeral write path
# ---------------------------------------------------------------------------

class TestEphemeralWritePath:
    def test_store_not_assigned_raises(self):
        """ephemeral_result=True but ephemeral_result_store=None raises RuntimeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)
        pipeline_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        # set_ephemeral_store never called → ephemeral_result_store is None
        with pytest.raises(RuntimeError, match="ephemeral_result=True"):
            node.execute(stream, error_policy="fail_fast")

    def test_ephemeral_only_node(self):
        """result_database=None, ephemeral_result=True: end-to-end works."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)
        # Pass pipeline_database but no result_database — ephemeral only
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20

    def test_recompute_after_ephemeral_miss_no_infinite_cycle(self):
        """Cross-session ephemeral miss → recomputed → served on next call (no cycle)."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1
        pf = PythonDataFunction(counting_double, output_keys="result")
        cfg = NodeConfig(ephemeral_result=True)
        pod = FunctionPod(pf, node_config=cfg)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.set_ephemeral_store(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh store → miss → recompute
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2, node_config=cfg)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.set_ephemeral_store(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2

        # Session 3: same ephemeral store as session 2 → hit → no recompute
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3, node_config=cfg)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node3.set_ephemeral_store(ephemeral2)  # reuse session 2's store
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed


# ---------------------------------------------------------------------------
# Task 9 tests: pipeline propagation
# ---------------------------------------------------------------------------

class TestPipelineInjectsStore:
    def test_pipeline_job_set_ephemeral_store_propagates(self):
        """PipelineJob.set_ephemeral_store propagates to all compiled nodes."""
        from orcapod.pipeline.job import PipelineJob

        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)

        job = PipelineJob(name="test_pipeline")
        with job:
            output = pod(stream)

        ephemeral_store = InMemoryArrowDatabase()
        job.set_ephemeral_store(ephemeral_store)

        # Every function node in the compiled pipeline should now have the store
        for label, node in job.function_pods.items():
            assert node.ephemeral_result_store is ephemeral_store, (
                f"Node '{label}' did not receive the ephemeral store"
            )

    def test_pipeline_job_set_ephemeral_store_none_detaches(self):
        """PipelineJob.set_ephemeral_store(None) detaches the store from all nodes."""
        from orcapod.pipeline.job import PipelineJob

        stream = _make_stream([{"id": 0, "x": 10}])
        cfg = NodeConfig(ephemeral_result=True)
        pod = _make_pod(config=cfg)

        job = PipelineJob(name="test_pipeline")
        with job:
            pod(stream)

        store = InMemoryArrowDatabase()
        job.set_ephemeral_store(store)
        job.set_ephemeral_store(None)

        for label, node in job.function_pods.items():
            assert node.ephemeral_result_store is None, (
                f"Node '{label}' ephemeral store was not detached"
            )
