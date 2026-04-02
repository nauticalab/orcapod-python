"""
Integration tests: all three pod caching strategies working end-to-end.

Covers:
1. CachedSource — always-on cache scoped to content_hash()
   - DeltaTableSource with canonical source_id (defaults to dir name)
   - Named sources: same name + same schema = same identity (data-independent)
   - Unnamed sources: identity determined by table hash (data-dependent)
   - Cumulative caching across data updates
2. FunctionNode — pipeline_hash()-scoped cache, cross-source sharing
   - Two pipelines with different source identities but same schema share one cache table
3. OperatorNode — content_hash()-scoped with CacheMode (OFF/LOG/REPLAY)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DeltaTableSource, CachedSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_delta(path: Path, table: pa.Table, *, mode: str = "error") -> None:
    write_deltalake(str(path), table, mode=mode)


def _make_patients(path: Path, ids: list[str], ages: list[int]) -> pa.Table:
    t = pa.table(
        {
            "patient_id": pa.array(ids, type=pa.large_string()),
            "age": pa.array(ages, type=pa.int64()),
        }
    )
    _write_delta(path, t)
    return t


def _make_labs(path: Path, ids: list[str], chols: list[int]) -> pa.Table:
    t = pa.table(
        {
            "patient_id": pa.array(ids, type=pa.large_string()),
            "cholesterol": pa.array(chols, type=pa.int64()),
        }
    )
    _write_delta(path, t)
    return t


def risk_score(age: int, cholesterol: int) -> float:
    return age * 0.5 + cholesterol * 0.3


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def source_db():
    return InMemoryArrowDatabase()


@pytest.fixture
def pipeline_db():
    return InMemoryArrowDatabase()


@pytest.fixture
def result_db():
    return InMemoryArrowDatabase()


@pytest.fixture
def operator_db():
    return InMemoryArrowDatabase()


@pytest.fixture
def delta_dir(tmp_path):
    return tmp_path


@pytest.fixture
def clinic_a(delta_dir):
    """Create clinic A's Delta tables and return (patients_path, labs_path)."""
    p = delta_dir / "clinic_a_patients"
    l = delta_dir / "clinic_a_labs"
    _make_patients(p, ["p1", "p2", "p3"], [30, 45, 60])
    _make_labs(l, ["p1", "p2", "p3"], [180, 220, 260])
    return p, l


@pytest.fixture
def clinic_b(delta_dir):
    """Create clinic B's Delta tables and return (patients_path, labs_path)."""
    p = delta_dir / "clinic_b_patients"
    l = delta_dir / "clinic_b_labs"
    _make_patients(p, ["x1", "x2"], [28, 72])
    _make_labs(l, ["x1", "x2"], [160, 290])
    return p, l


@pytest.fixture
def pod():
    pf = PythonPacketFunction(risk_score, output_keys="risk")
    return FunctionPod(packet_function=pf)


# ---------------------------------------------------------------------------
# 1. CachedSource — source pod caching
# ---------------------------------------------------------------------------


class TestSourcePodCaching:
    def test_delta_source_id_defaults_to_dir_name(self, clinic_a):
        patients_path, labs_path = clinic_a
        ps = DeltaTableSource(patients_path, tag_columns=["patient_id"])
        ls = DeltaTableSource(labs_path, tag_columns=["patient_id"])
        assert ps.source_id == patients_path.name
        assert ls.source_id == labs_path.name

    def test_different_sources_get_different_cache_paths(self, clinic_a, source_db):
        patients_path, labs_path = clinic_a
        patients = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        labs = CachedSource(
            DeltaTableSource(labs_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        assert patients.cache_path != labs.cache_path

    def test_cache_populates_on_flow(self, clinic_a, source_db):
        patients_path, _ = clinic_a
        ps = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        ps.flow()
        records = ps.get_all_records()
        assert records is not None
        assert records.num_rows == 3

    def test_dedup_on_rerun(self, clinic_a, source_db):
        patients_path, _ = clinic_a
        ps1 = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        ps1.flow()
        ps2 = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        ps2.flow()
        assert ps2.get_all_records().num_rows == 3

    def test_named_source_same_name_same_schema_same_identity(
        self, clinic_a, source_db
    ):
        """Same dir name + same schema = same content_hash regardless of data."""
        patients_path, _ = clinic_a
        src1 = DeltaTableSource(patients_path, tag_columns=["patient_id"])
        ps1 = CachedSource(src1, cache_database=source_db)

        # Overwrite with different data, same schema
        _write_delta(
            patients_path,
            pa.table(
                {
                    "patient_id": pa.array(
                        ["p1", "p2", "p3", "p4"], type=pa.large_string()
                    ),
                    "age": pa.array([30, 45, 60, 25], type=pa.int64()),
                }
            ),
            mode="overwrite",
        )
        src2 = DeltaTableSource(patients_path, tag_columns=["patient_id"])
        ps2 = CachedSource(src2, cache_database=source_db)

        assert src1.source_id == src2.source_id
        assert ps1.content_hash() == ps2.content_hash()
        assert ps1.cache_path == ps2.cache_path

    def test_cumulative_caching_across_data_updates(self, clinic_a, source_db):
        """New rows from updated data accumulate in the same cache table."""
        patients_path, _ = clinic_a
        ps1 = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        ps1.flow()
        assert ps1.get_all_records().num_rows == 3

        # Update Delta table: add p4
        _write_delta(
            patients_path,
            pa.table(
                {
                    "patient_id": pa.array(
                        ["p1", "p2", "p3", "p4"], type=pa.large_string()
                    ),
                    "age": pa.array([30, 45, 60, 25], type=pa.int64()),
                }
            ),
            mode="overwrite",
        )
        ps2 = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        ps2.flow()
        # 3 original + 1 new, existing rows deduped
        assert ps2.get_all_records().num_rows == 4

    def test_unnamed_source_different_data_different_identity(self):
        t1 = pa.table(
            {
                "k": pa.array(["a"], type=pa.large_string()),
                "v": pa.array([1], type=pa.int64()),
            }
        )
        t2 = pa.table(
            {
                "k": pa.array(["b"], type=pa.large_string()),
                "v": pa.array([2], type=pa.int64()),
            }
        )
        s1 = ArrowTableSource(t1, tag_columns=["k"], infer_nullable=True)
        s2 = ArrowTableSource(t2, tag_columns=["k"], infer_nullable=True)
        assert s1.source_id != s2.source_id
        assert s1.content_hash() != s2.content_hash()

    def test_unnamed_source_same_data_same_identity(self):
        t = pa.table(
            {
                "k": pa.array(["a"], type=pa.large_string()),
                "v": pa.array([1], type=pa.int64()),
            }
        )
        s1 = ArrowTableSource(t, tag_columns=["k"], infer_nullable=True)
        s2 = ArrowTableSource(t, tag_columns=["k"], infer_nullable=True)
        assert s1.source_id == s2.source_id
        assert s1.content_hash() == s2.content_hash()


# ---------------------------------------------------------------------------
# 2. FunctionNode — function pod caching + cross-source sharing
# ---------------------------------------------------------------------------


class TestFunctionPodCaching:
    def test_function_node_stores_records(
        self, clinic_a, source_db, pipeline_db, result_db, pod
    ):
        patients_path, labs_path = clinic_a
        patients = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        labs = CachedSource(
            DeltaTableSource(labs_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        joined = Join()(patients, labs)

        fn_node = FunctionNode(
            function_pod=pod,
            input_stream=joined,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        fn_node.run()

        records = fn_node.get_all_records()
        assert records is not None
        assert records.num_rows == 3

    def test_cross_source_sharing_same_pipeline_path(
        self, clinic_a, clinic_b, source_db, pipeline_db, result_db, pod
    ):
        """Different source identities, same schema → same schema: component but
        different instance: components (different data → different content_hash)."""
        patients_a, labs_a = clinic_a
        patients_b, labs_b = clinic_b

        # Pipeline A
        pa_src = CachedSource(
            DeltaTableSource(patients_a, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        la_src = CachedSource(
            DeltaTableSource(labs_a, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        fn_a = FunctionNode(
            function_pod=pod,
            input_stream=Join()(pa_src, la_src),
            pipeline_database=pipeline_db,
            result_database=result_db,
        )

        # Pipeline B
        pb_src = CachedSource(
            DeltaTableSource(patients_b, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        lb_src = CachedSource(
            DeltaTableSource(labs_b, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        fn_b = FunctionNode(
            function_pod=pod,
            input_stream=Join()(pb_src, lb_src),
            pipeline_database=pipeline_db,
            result_database=result_db,
        )

        # Same schema → same schema: component
        assert fn_a.node_identity_path[-2] == fn_b.node_identity_path[-2]
        assert fn_a.node_identity_path[-2].startswith("schema:")
        # Different data → different full path
        assert fn_a.node_identity_path != fn_b.node_identity_path

    def test_cross_source_records_accumulate_in_shared_table(
        self, clinic_a, clinic_b, source_db, pipeline_db, result_db, pod
    ):
        """With the two-level formula, each pipeline writes to its own DB path.
        Records from pipeline A and B are stored separately."""
        patients_a, labs_a = clinic_a
        patients_b, labs_b = clinic_b

        # Pipeline A: 3 patients
        fn_a = FunctionNode(
            function_pod=pod,
            input_stream=Join()(
                CachedSource(
                    DeltaTableSource(patients_a, tag_columns=["patient_id"]),
                    cache_database=source_db,
                ),
                CachedSource(
                    DeltaTableSource(labs_a, tag_columns=["patient_id"]),
                    cache_database=source_db,
                ),
            ),
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        fn_a.run()
        assert fn_a.get_all_records().num_rows == 3

        # Pipeline B: 2 patients, different source identity, same schema
        fn_b = FunctionNode(
            function_pod=pod,
            input_stream=Join()(
                CachedSource(
                    DeltaTableSource(patients_b, tag_columns=["patient_id"]),
                    cache_database=source_db,
                ),
                CachedSource(
                    DeltaTableSource(labs_b, tag_columns=["patient_id"]),
                    cache_database=source_db,
                ),
            ),
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        fn_b.run()
        # Different pipeline_path: fn_b has its own 2 records
        assert fn_b.get_all_records().num_rows == 2


# ---------------------------------------------------------------------------
# 3. OperatorNode — operator pod caching with CacheMode
# ---------------------------------------------------------------------------


class TestOperatorPodCaching:
    def _make_joined_streams(self, clinic_a, source_db):
        patients_path, labs_path = clinic_a
        patients = CachedSource(
            DeltaTableSource(patients_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        labs = CachedSource(
            DeltaTableSource(labs_path, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        return patients, labs

    def test_off_computes_without_db_writes(self, clinic_a, source_db, operator_db):
        patients, labs = self._make_joined_streams(clinic_a, source_db)
        node = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=operator_db,
            cache_mode=CacheMode.OFF,
        )
        node.run()
        assert node.as_table().num_rows == 3
        assert operator_db.get_all_records(node.node_identity_path) is None

    def test_log_computes_and_writes(self, clinic_a, source_db, operator_db):
        patients, labs = self._make_joined_streams(clinic_a, source_db)
        node = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=operator_db,
            cache_mode=CacheMode.LOG,
        )
        node.run()
        assert node.as_table().num_rows == 3
        records = operator_db.get_all_records(node.node_identity_path)
        assert records is not None
        assert records.num_rows == 3

    def test_replay_loads_from_cache(self, clinic_a, source_db, operator_db):
        patients, labs = self._make_joined_streams(clinic_a, source_db)

        # First LOG to populate
        log_node = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=operator_db,
            cache_mode=CacheMode.LOG,
        )
        log_node.run()

        # Then REPLAY from cache
        replay_node = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=operator_db,
            cache_mode=CacheMode.REPLAY,
        )
        replay_node.run()
        assert replay_node.as_table().num_rows == 3

    def test_replay_empty_cache_returns_empty_stream(self, clinic_a, source_db):
        patients, labs = self._make_joined_streams(clinic_a, source_db)
        node = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=InMemoryArrowDatabase(),
            cache_mode=CacheMode.REPLAY,
        )
        node.run()
        table = node.as_table()
        assert table.num_rows == 0
        # Schema is preserved
        tag_keys, packet_keys = node.keys()
        assert set(tag_keys).issubset(set(table.column_names))
        assert set(packet_keys).issubset(set(table.column_names))

    def test_content_hash_scoping_isolates_source_combinations(
        self, clinic_a, clinic_b, source_db, operator_db
    ):
        """Different source combinations → different pipeline_paths."""
        patients_a, labs_a = clinic_a
        patients_b, labs_b = clinic_b

        pa_src = CachedSource(
            DeltaTableSource(patients_a, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        la_src = CachedSource(
            DeltaTableSource(labs_a, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        pb_src = CachedSource(
            DeltaTableSource(patients_b, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        lb_src = CachedSource(
            DeltaTableSource(labs_b, tag_columns=["patient_id"]),
            cache_database=source_db,
        )

        node_a = OperatorNode(
            operator=Join(),
            input_streams=[pa_src, la_src],
            pipeline_database=operator_db,
            cache_mode=CacheMode.LOG,
        )
        node_b = OperatorNode(
            operator=Join(),
            input_streams=[pb_src, lb_src],
            pipeline_database=operator_db,
            cache_mode=CacheMode.LOG,
        )
        assert node_a.node_identity_path != node_b.node_identity_path


# ---------------------------------------------------------------------------
# 4. End-to-end: all three pods in one pipeline
# ---------------------------------------------------------------------------


class TestEndToEndPipeline:
    def test_full_pipeline_source_to_function_to_operator(
        self, clinic_a, clinic_b, source_db, pipeline_db, result_db, operator_db, pod
    ):
        """
        Full pipeline: DeltaTableSource → CachedSource → Join →
        FunctionNode → OperatorNode (LOG + REPLAY).
        """
        patients_a, labs_a = clinic_a

        # Step 1: CachedSource
        patients = CachedSource(
            DeltaTableSource(patients_a, tag_columns=["patient_id"]),
            cache_database=source_db,
        )
        labs = CachedSource(
            DeltaTableSource(labs_a, tag_columns=["patient_id"]),
            cache_database=source_db,
        )

        # Step 2: Join + FunctionNode
        joined = Join()(patients, labs)
        fn_node = FunctionNode(
            function_pod=pod,
            input_stream=joined,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        fn_node.run()
        assert fn_node.get_all_records().num_rows == 3

        # Step 3: OperatorNode (LOG)
        # Use fn_node output as input to an operator
        op_node = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=operator_db,
            cache_mode=CacheMode.LOG,
        )
        op_node.run()
        assert operator_db.get_all_records(op_node.node_identity_path).num_rows == 3

        # Step 4: REPLAY from operator cache
        op_replay = OperatorNode(
            operator=Join(),
            input_streams=[patients, labs],
            pipeline_database=operator_db,
            cache_mode=CacheMode.REPLAY,
        )
        op_replay.run()
        assert op_replay.as_table().num_rows == 3

        # Step 5: Second clinic uses same function but different data → own pipeline_path
        patients_b, labs_b = clinic_b
        fn_node_b = FunctionNode(
            function_pod=pod,
            input_stream=Join()(
                CachedSource(
                    DeltaTableSource(patients_b, tag_columns=["patient_id"]),
                    cache_database=source_db,
                ),
                CachedSource(
                    DeltaTableSource(labs_b, tag_columns=["patient_id"]),
                    cache_database=source_db,
                ),
            ),
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        # Same schema → same schema: component; different data → different full path
        assert fn_node.node_identity_path[-2] == fn_node_b.node_identity_path[-2]
        assert fn_node.node_identity_path != fn_node_b.node_identity_path
        fn_node_b.run()
        # fn_node_b has its own path: 2 records from clinic B only
        assert fn_node_b.get_all_records().num_rows == 2

        # Verify source caches are populated
        assert patients.get_all_records().num_rows == 3
        assert labs.get_all_records().num_rows == 3
