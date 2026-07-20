"""Tests for FunctionJobNode ephemeral result store — ITL-507."""
from __future__ import annotations

import asyncio
import uuid

import pyarrow as pa
import pytest

from orcapod.channels import Channel
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


def _make_pod():
    pf = PythonDataFunction(double, output_keys="result")
    return FunctionPod(pf)


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


def _make_source_stream(rows: list[dict], tag_columns: list[str] | None = None) -> ArrowTableSource:
    if tag_columns is None:
        tag_columns = ["id"]
    table = pa.table(
        {k: pa.array([r[k] for r in rows], type=pa.int64()) for k in rows[0]}
    )
    source = ArrowTableSource(table, tag_columns=tag_columns, source_id="test_src", infer_nullable=True)
    return source


def _make_node(stream, pipeline_db=None, result_db=None, is_result_ephemeral: bool = False):
    """Create a FunctionJobNode with given DB configuration."""
    pod = _make_pod()
    if pipeline_db is None:
        pipeline_db = InMemoryArrowDatabase()
    node = FunctionJobNode(
        function_pod=pod,
        input_stream=stream,
        pipeline_database=pipeline_db,
        result_database=result_db if result_db is not None else pipeline_db,
    )
    if is_result_ephemeral:
        node.node_config = NodeConfig(is_result_ephemeral=True)
    return node, pipeline_db


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

        all_records = db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is False for v in vals)

    def test_is_ephemeral_true_written_to_pipeline_db(self):
        """When is_result_ephemeral=True, IS_EPHEMERAL_COL=True is stored in the tag table."""
        from orcapod.system_constants import constants

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db, is_result_ephemeral=True)
        node.set_ephemeral_store(ephemeral_store)

        results = node.execute(stream)
        assert len(results) == 1

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert constants.IS_EPHEMERAL_COL in all_records.column_names
        vals = all_records.column(constants.IS_EPHEMERAL_COL).to_pylist()
        assert all(v is True for v in vals)


# ---------------------------------------------------------------------------
# Task 7 tests: two-store join
# ---------------------------------------------------------------------------

class TestBulkResolution:
    def test_ephemeral_false_unchanged(self):
        """is_result_ephemeral=False: execute() behaves identically to current implementation."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        node, _ = _make_node(stream)
        results = node.execute(stream)
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_ephemeral_result_written_to_memory_not_persistent_db(self):
        """With is_result_ephemeral=True, persistent DB has no result rows; ephemeral store does."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pod = _make_pod()
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
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
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
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
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(InMemoryArrowDatabase())
        node.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh in-memory node with a fresh ephemeral store
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)
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
        pod = FunctionPod(pf)  # is_result_ephemeral=False (default)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        # Attach an ephemeral store — must NOT break persistent reads
        node.set_ephemeral_store(InMemoryArrowDatabase())

        # Run 1: writes to persistent DB (is_result_ephemeral=False)
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
        node_p, _ = _make_node(stream_a, pipeline_db=pipeline_db, is_result_ephemeral=False)
        node_p.execute(stream_a)

        # id=1 → ephemeral, reusing same pipeline_db
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node_e = FunctionJobNode(
            function_pod=pod,
            input_stream=stream_b,
            pipeline_database=pipeline_db,
        )
        node_e.node_config = NodeConfig(is_result_ephemeral=True)
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
        node_both.node_config = NodeConfig(is_result_ephemeral=True)
        node_both.set_ephemeral_store(ephemeral_store)
        node_both._cached_output_datas.clear()

        results = node_both.execute(stream_both)
        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}

    def test_persistent_result_outcompetes_ephemeral(self):
        """Persistent result wins when both persistent and ephemeral rows share the same entry_id.

        Verifies the anti-join priority merge in ``_fetch_joined_records``:
        when both ``persistent_df`` and ``ephemeral_df`` have a row for the same
        ``_PIPELINE_ENTRY_ID_COL`` value, the ephemeral row is excluded by anti-join,
        and the persistent result is returned (not recomputed).

        Because ``add_pipeline_record`` with ``skip_cache_lookup=True`` deduplicates
        at the DB level, constructing this scenario requires direct DB manipulation:
        - write the persistent row normally
        - ``flush()`` pipeline_db so it moves to committed tables (not pending)
        - insert the ephemeral row via ``add_record(skip_duplicates=False)`` — succeeds
          because the committed row is not in ``_pending_record_ids``
        """
        import pyarrow as pa
        from orcapod.core.nodes.function_node import _PIPELINE_ENTRY_ID_COL
        from orcapod.system_constants import constants

        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()

        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)  # is_result_ephemeral=False (default)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.set_ephemeral_store(ephemeral_store)

        # Step 1: Write the persistent result (IS_EPHEMERAL=False)
        node.execute(stream)
        assert call_count["n"] == 1

        # Step 2: Write a real result to ephemeral_store so the ephemeral inner-join
        # produces a row (this makes ephemeral_df.height > 0, triggering the anti-join branch).
        # This computation increments call_count — capture it here for the later assertion.
        tag_obj = Tag({"id": 0})
        data_obj = Data({"x": 10})
        _, eph_output = node._ephemeral_cached_pod.process_data(tag_obj, data_obj)
        assert eph_output is not None
        eph_data_record_id = eph_output.datagram_uuid
        count_after_setup = call_count["n"]  # 2: once from step 1, once from this call

        # Step 3: Flush pipeline_db — moves the persistent row from _pending to _tables
        # After this, _pending_record_ids is cleared for this path
        pipeline_db.flush()

        # Step 4: Get the committed row and its entry_id bytes
        existing = pipeline_db.get_all_records(
            node._versioned_pipeline_path,
            record_id_column=_PIPELINE_ENTRY_ID_COL,
        )
        assert existing is not None and existing.num_rows == 1
        entry_id_bytes = existing[_PIPELINE_ENTRY_ID_COL][0].as_py()

        # Step 5: Build an ephemeral copy of the row with IS_EPHEMERAL=True and the
        # ephemeral_store's data_record_id
        row_without_id = existing.drop([_PIPELINE_ENTRY_ID_COL])
        eph_col_idx = row_without_id.schema.get_field_index(constants.IS_EPHEMERAL_COL)
        rid_col_idx = row_without_id.schema.get_field_index(constants.DATA_RECORD_ID)
        ephemeral_row = row_without_id.set_column(
            eph_col_idx,
            constants.IS_EPHEMERAL_COL,
            pa.array([True], type=pa.bool_()),
        )
        ephemeral_row = ephemeral_row.set_column(
            rid_col_idx,
            constants.DATA_RECORD_ID,
            pa.array([eph_data_record_id.bytes], type=pa.large_binary()),
        )

        # Step 6: Insert the ephemeral row with the SAME entry_id, skip_duplicates=False.
        # This succeeds because the flushed row is in _tables (not _pending_record_ids).
        pipeline_db.add_record(
            node._versioned_pipeline_path,
            entry_id_bytes,
            ephemeral_row,
            skip_duplicates=False,
        )

        # Now pipeline_db has two rows for the same entry_id:
        #   IS_EPHEMERAL=False → persistent result (result=20)
        #   IS_EPHEMERAL=True  → ephemeral result (result=20, same value but different store)
        both = pipeline_db.get_all_records(
            node._versioned_pipeline_path,
            record_id_column=_PIPELINE_ENTRY_ID_COL,
        )
        assert both is not None and both.num_rows == 2

        # Step 7: Clear in-memory cache to force _fetch_joined_records lookup
        node._cached_output_datas.clear()

        # The anti-join should exclude the ephemeral row (entry_id clash with persistent row),
        # returning only the persistent result. No recomputation.
        results = node.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        # call_count did not increase — persistent hit from anti-join merge, no recomputation
        assert call_count["n"] == count_after_setup


# ---------------------------------------------------------------------------
# Task 8 tests: ephemeral write path
# ---------------------------------------------------------------------------

class TestEphemeralWritePath:
    def test_store_not_assigned_raises(self):
        """is_result_ephemeral=True but ephemeral_result_store=None raises RuntimeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pod = _make_pod()
        pipeline_db = InMemoryArrowDatabase()
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        # set_ephemeral_store never called → ephemeral_result_store is None
        with pytest.raises(RuntimeError, match="is_result_ephemeral=True"):
            node.execute(stream, error_policy="fail_fast")

    def test_ephemeral_only_node(self):
        """result_database=None, is_result_ephemeral=True: end-to-end works."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pod = _make_pod()
        # Pass pipeline_database but no result_database — ephemeral only
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
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
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.set_ephemeral_store(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh store → miss → recompute
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)
        node2.set_ephemeral_store(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2

        # Session 3: same ephemeral store as session 2 → hit → no recompute
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node3.node_config = NodeConfig(is_result_ephemeral=True)
        node3.set_ephemeral_store(ephemeral2)  # reuse session 2's store
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed

    def test_iter_data_serves_result_after_cross_session_recompute(self):
        """After a cross-session ephemeral miss triggers recompute via execute(),
        iter_data() serves the fresh result without triggering additional computation.

        Verifies that:
        - after clearing the in-memory cache, iter_data() hot-loads the recomputed
          result from the pipeline + ephemeral DBs (exercises _load_cached_entries)
        - repeated iter_data() calls return the same result without recomputing
        """
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()

        # Session 1: compute with ephemeral store 1 → pipeline DB gets index-0 record
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node1.node_config = NodeConfig(is_result_ephemeral=True)
        node1.set_ephemeral_store(InMemoryArrowDatabase())
        node1.execute(stream)
        assert call_count["n"] == 1

        # Session 2: fresh ephemeral store → cross-session miss → recompute → index-1 record written
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        ephemeral2 = InMemoryArrowDatabase()
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node2.node_config = NodeConfig(is_result_ephemeral=True)
        node2.set_ephemeral_store(ephemeral2)
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed once due to cross-session miss

        # execute() calls _update_modified_time() at the end, so is_stale is False here.
        # Manually clearing forces the iter_data() hot-load branch (_load_cached_entries →
        # _fetch_joined_records) without relying on staleness, so this tests the DB join
        # path explicitly rather than the in-memory dict fast-path.
        node2._cached_output_datas.clear()

        # iter_data() must serve the recomputed result — no additional computation
        results = list(node2.iter_data())
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 2  # NOT recomputed again

        # A second iter_data() call must also return the same result without recomputing
        results2 = list(node2.iter_data())
        assert len(results2) == 1
        assert results2[0][1].as_dict()["result"] == 20
        assert call_count["n"] == 2  # still NOT recomputed


# ---------------------------------------------------------------------------
# Task 9 tests: pipeline propagation
# ---------------------------------------------------------------------------

class TestPipelineInjectsStore:
    def test_pipeline_job_set_ephemeral_store_propagates(self):
        """PipelineJob.set_ephemeral_store propagates to all compiled nodes."""
        from orcapod.pipeline.job import PipelineJob

        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        pod = _make_pod()

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
        pod = _make_pod()

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


# ---------------------------------------------------------------------------
# Task 10 tests: persistent miss warning and ephemeral-only node
# ---------------------------------------------------------------------------

class TestPersistentMissWarning:
    def test_persistent_miss_warns_and_recomputes(self, caplog):
        """Tag table has a regular record_id but persistent DB was trimmed: WARNING emitted."""
        import logging

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Write a persistent record
        node, _ = _make_node(stream, pipeline_db=pipeline_db, result_db=result_db, is_result_ephemeral=False)
        node.execute(stream)

        # Wipe the result DB to simulate data loss
        result_db._tables.clear()
        result_db._pending_batches.clear()

        # Recreate node with same pipeline_db (tag entry still there) but empty result_db
        node2, _ = _make_node(stream, pipeline_db=pipeline_db, result_db=result_db, is_result_ephemeral=False)

        with caplog.at_level(logging.WARNING, logger="orcapod.core.nodes.function_node"):
            results = node2.execute(stream)

        assert len(results) == 1  # recomputed
        assert any("have no match in persistent result DB" in msg for msg in caplog.messages)

    def test_recompute_after_persistent_miss_appends_new_pipeline_record(self):
        """After persistent miss and recompute, tag table has two rows; next call hits."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: write persistent record
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node1 = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.execute(stream)
        assert call_count["n"] == 1

        # Simulate data loss
        result_db._tables.clear()
        result_db._pending_batches.clear()

        # Session 2: miss → recompute → appends new record to pipeline_db
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node2.execute(stream)
        assert call_count["n"] == 2  # recomputed

        # Session 3: tag table now has two rows (stale + new); inner join resolves correctly
        pf3 = PythonDataFunction(counting_double, output_keys="result")
        pod3 = FunctionPod(pf3)
        node3 = FunctionJobNode(
            function_pod=pod3,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node3._cached_output_datas.clear()
        node3.execute(stream)
        assert call_count["n"] == 2  # NOT recomputed — new row was found


class TestEphemeralOnlyNode:
    def test_ephemeral_only_node_no_persistent_db(self):
        """NodeConfig(is_result_ephemeral=True) with no result_database works end-to-end."""
        stream = _make_stream([{"id": 0, "x": 10}, {"id": 1, "x": 20}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)

        # No result_database — pipeline_db doubles as both
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(ephemeral_store)
        results = node.execute(stream)

        assert len(results) == 2
        vals = {tag.as_dict()["id"]: data.as_dict()["result"] for tag, data in results}
        assert vals == {0: 20, 1: 40}


# ---------------------------------------------------------------------------
# Task 11 tests: async ephemeral execution path
# ---------------------------------------------------------------------------


class TestAsyncEphemeralExecution:
    @pytest.mark.asyncio
    async def test_async_execute_ephemeral_happy_path(self):
        """async_execute with is_result_ephemeral=True writes to ephemeral store and emits results."""
        stream = _make_stream([{"id": 0, "x": 5}, {"id": 1, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        ephemeral_store = InMemoryArrowDatabase()

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(ephemeral_store)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, data in stream.iter_data():
            await input_ch.writer.send((tag, data))
        await input_ch.writer.close()

        await node.async_execute(input_ch.reader, output_ch.writer)

        results = await output_ch.reader.collect()
        assert len(results) == 2
        values = sorted(data.as_dict()["result"] for _, data in results)
        assert values == [10, 20]

        # Result records must be in the ephemeral store, not the persistent store
        eph_records = ephemeral_store.get_all_records(
            node._ephemeral_cached_pod.record_path,
        )
        assert eph_records is not None
        assert eph_records.num_rows == 2

    @pytest.mark.asyncio
    async def test_async_process_data_internal_raises_when_no_store(self):
        """_async_process_data_internal raises RuntimeError when is_result_ephemeral=True but no store."""
        stream = _make_stream([{"id": 0, "x": 5}])
        pipeline_db = InMemoryArrowDatabase()

        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
        )
        node.node_config = NodeConfig(is_result_ephemeral=True)
        # set_ephemeral_store never called → _ephemeral_cached_pod is None

        tag, data = next(iter(stream.iter_data()))
        with pytest.raises(RuntimeError, match="is_result_ephemeral=True"):
            await node._async_process_data_internal(tag, data)


# ---------------------------------------------------------------------------
# Task 2 tests: redesigned add_pipeline_record (indexed, no skip_cache_lookup)
# ---------------------------------------------------------------------------


class TestAddPipelineRecordIndexed:
    def test_first_call_writes_at_index_zero(self):
        """add_pipeline_record writes recomputation_index=0 on the first call."""
        from orcapod.core.nodes.function_node import _PIPELINE_RECOMPUTATION_INDEX_COL
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 1
        assert all_records.column(_PIPELINE_RECOMPUTATION_INDEX_COL)[0].as_py() == 0

    def test_second_call_writes_at_index_one(self):
        """Second add_pipeline_record call for the same base_entry_id writes at index 1."""
        from orcapod.core.nodes.function_node import _PIPELINE_RECOMPUTATION_INDEX_COL
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()
        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 2
        indices = all_records.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
        assert sorted(indices) == [0, 1]

    def test_base_entry_id_column_written(self):
        """add_pipeline_record writes _PIPELINE_BASE_ENTRY_ID_COL to the pipeline DB row."""
        from orcapod.core.nodes.function_node import _PIPELINE_BASE_ENTRY_ID_COL
        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert _PIPELINE_BASE_ENTRY_ID_COL in all_records.column_names
        expected_base_id = node.compute_base_entry_id(tag, data)
        assert all_records.column(_PIPELINE_BASE_ENTRY_ID_COL)[0].as_py() == expected_base_id

    def test_skip_cache_lookup_parameter_removed(self):
        """add_pipeline_record no longer accepts skip_cache_lookup — raises TypeError."""
        stream = _make_stream([{"id": 0, "x": 10}])
        node, _ = _make_node(stream)
        tag, data = next(iter(stream.iter_data()))
        with pytest.raises(TypeError, match="skip_cache_lookup"):
            node.add_pipeline_record(
                tag, data, data_record_id=uuid.uuid4(), computed=True,
                skip_cache_lookup=False,  # removed parameter
            )


# ---------------------------------------------------------------------------
# Task 13 tests: _fetch_joined_records guard and backward-compat branch
# ---------------------------------------------------------------------------


class TestFetchJoinedRecordsGuards:
    def test_get_all_records_no_db_returns_none(self):
        """get_all_records() on a FunctionJobNode with no pipeline_database returns None."""
        stream = _make_stream([{"id": 0, "x": 10}])
        pf = PythonDataFunction(double, output_keys="result")
        pod = FunctionPod(pf)
        # Intentionally no pipeline_database → _cached_function_pod is None
        node = FunctionJobNode(function_pod=pod, input_stream=stream)

        result = node.get_all_records()
        assert result is None

    def test_legacy_records_without_ephemeral_col_treated_as_persistent(self):
        """Records lacking IS_EPHEMERAL_COL are treated as persistent (backward compat)."""
        from orcapod import system_constants as sc

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # Session 1: write a normal persistent record
        node1, _ = _make_node(
            stream,
            pipeline_db=pipeline_db,
            result_db=result_db,
            is_result_ephemeral=False,
        )
        node1.execute(stream)
        pipeline_db.flush()

        # Drop IS_EPHEMERAL_COL from the committed table to simulate a legacy record
        is_eph_col = sc.constants.IS_EPHEMERAL_COL
        record_key = "/".join(node1._versioned_pipeline_path)
        old_table = pipeline_db._tables[record_key]
        col_idx = old_table.schema.get_field_index(is_eph_col)
        assert col_idx >= 0, "IS_EPHEMERAL_COL must exist before we drop it"
        pipeline_db._tables[record_key] = old_table.remove_column(col_idx)

        # Session 2: new node with same DBs — should handle missing column gracefully
        pf2 = PythonDataFunction(double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        results = node2.execute(stream)

        # Result must be served (from result DB cache or recomputed)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20


# ---------------------------------------------------------------------------
# Task 6 tests: concurrent asyncio Phase 2 serialisation
# ---------------------------------------------------------------------------


class TestConcurrentMissSerialization:
    @pytest.mark.asyncio
    async def test_two_concurrent_phase2_misses_produce_valid_pipeline_records(self):
        """Two asyncio coroutines that simultaneously execute Phase 2 for the same input
        each produce a valid pipeline record. A subsequent Phase 1 lookup finds a result
        and does NOT recompute."""
        call_count = {"n": 0}

        def counting_double(x: int) -> int:
            call_count["n"] += 1
            return x * 2

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        pf = PythonDataFunction(counting_double, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionJobNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )

        tag, data = next(iter(stream.iter_data()))

        # Two concurrent _async_process_data_internal calls on the same (tag, data).
        # In asyncio cooperative multitasking, these serialise at add_pipeline_record
        # (synchronous), so each gets a distinct recomputation_index.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(node._async_process_data_internal(tag, data))
            tg.create_task(node._async_process_data_internal(tag, data))

        # At least one pipeline record must exist for this base_entry_id
        pipeline_db.flush()
        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

        # Session 2: new node with the same DBs — Phase 1 must find a valid result
        pf2 = PythonDataFunction(counting_double, output_keys="result")
        pod2 = FunctionPod(pf2)
        node2 = FunctionJobNode(
            function_pod=pod2,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        count_before_session2 = call_count["n"]
        results = node2.execute(stream)
        assert len(results) == 1
        assert results[0][1].as_dict()["result"] == 20
        assert call_count["n"] == count_before_session2  # NOT recomputed

    @pytest.mark.asyncio
    async def test_sequential_add_pipeline_record_increments_index_each_time(self):
        """Two sequential add_pipeline_record calls for the same base_entry_id
        write at indices 0 and 1 respectively (not blocked by the existing row)."""
        from orcapod.core.nodes.function_node import (
            _PIPELINE_BASE_ENTRY_ID_COL,
            _PIPELINE_RECOMPUTATION_INDEX_COL,
        )

        stream = _make_stream([{"id": 0, "x": 10}])
        pipeline_db = InMemoryArrowDatabase()
        node, _ = _make_node(stream, pipeline_db=pipeline_db)
        tag, data = next(iter(stream.iter_data()))

        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        node.add_pipeline_record(tag, data, data_record_id=uuid.uuid4(), computed=True)
        pipeline_db.flush()

        all_records = pipeline_db.get_all_records(node._versioned_pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 2

        base_ids = all_records.column(_PIPELINE_BASE_ENTRY_ID_COL).to_pylist()
        assert base_ids[0] == base_ids[1]  # same base_entry_id

        indices = all_records.column(_PIPELINE_RECOMPUTATION_INDEX_COL).to_pylist()
        assert sorted(indices) == [0, 1]  # distinct indices
