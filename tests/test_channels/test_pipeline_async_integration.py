"""
Integration test — end-to-end async pipeline.

Shows the recommended workflow in a single, linear example:

1. Define domain functions with ``@function_pod``.
2. Build a pipeline job with the ``PipelineJob`` context manager.
3. Run the pipeline asynchronously via ``AsyncPipelineOrchestrator``.
4. Retrieve persisted results synchronously from the compiled nodes.

Pipeline::

    students ──┐
               ├── Join ──► compute_letter_grade
    grades  ───┘

Tags:   student_id
Data: name, score  →  letter_grade
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.sources import ArrowTableSource
from orcapod.core.operators import Join
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob


# ── 1. Define domain functions ───────────────────────────────────────────


@function_pod(output_keys="letter_grade")
def compute_letter_grade(name: str, score: int) -> str:
    """Assign a letter grade based on numeric score."""
    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    else:
        return "F"


# ── 2. Source data ───────────────────────────────────────────────────────


STUDENTS = pa.table(
    {
        "student_id": pa.array(["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()),
        "name": pa.array(
            ["Alice", "Bob", "Carol", "Dave", "Eve"], type=pa.large_string()
        ),
    }
)

GRADES = pa.table(
    {
        "student_id": pa.array(["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()),
        "score": pa.array([95, 82, 67, 73, 55], type=pa.int64()),
    }
)

EXPECTED = {
    "s1": "A",  # 95
    "s2": "B",  # 82
    "s3": "F",  # 67
    "s4": "C",  # 73
    "s5": "F",  # 55
}


# ── 3. Build pipeline job ───────────────────────────────────────────────


def _build_job() -> PipelineJob:
    """Construct the pipeline job."""
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="grades_pipeline", store=db)

    with job:
        students = ArrowTableSource(STUDENTS, tag_columns=["student_id"], infer_nullable=True)
        grades = ArrowTableSource(GRADES, tag_columns=["student_id"], infer_nullable=True)

        joined = Join()(students, grades, label="join")
        compute_letter_grade.pod(joined, label="letter_grade")

    return job


def _grades_from_table(table: pa.Table) -> dict[str, str]:
    """Extract {student_id: letter_grade} from a PyArrow table."""
    return {
        table.column("student_id")[i].as_py(): table.column("letter_grade")[i].as_py()
        for i in range(table.num_rows)
    }


# ── Tests ────────────────────────────────────────────────────────────────


class TestAsyncPipelineIntegration:
    """Build → run async → retrieve from persistent nodes → verify."""

    def test_orchestrator_then_db_retrieval(self):
        """Run via orchestrator, then retrieve results from the persistent node."""
        job = _build_job()
        AsyncPipelineOrchestrator().run(job.dag)
        job.store.at(*job._name).flush()
        job.store.at(*job._name).at("_result").flush()

        records = job.nodes["letter_grade"].get_all_records()
        assert records is not None
        assert records.num_rows == 5
        assert _grades_from_table(records) == EXPECTED

    def test_pipeline_run_with_async_executor(self):
        """Test async execution via AsyncPipelineOrchestrator directly."""
        job = _build_job()
        AsyncPipelineOrchestrator().run(job.dag)
        job.store.at(*job._name).flush()
        job.store.at(*job._name).at("_result").flush()

        records = job.nodes["letter_grade"].get_all_records()
        assert records is not None
        assert records.num_rows == 5
        assert _grades_from_table(records) == EXPECTED

    @pytest.mark.asyncio
    async def test_orchestrator_run_async_from_event_loop(self):
        """run_async() works when an event loop is already running."""
        job = _build_job()
        orchestrator = AsyncPipelineOrchestrator()
        await orchestrator.run_async(job.dag)
        job.store.at(*job._name).flush()
        job.store.at(*job._name).at("_result").flush()

        records = job.nodes["letter_grade"].get_all_records()
        assert records is not None
        assert records.num_rows == 5
        assert _grades_from_table(records) == EXPECTED

    def test_sync_run_then_db_retrieval(self):
        """Baseline: sync run() populates the DB for later retrieval."""
        job = _build_job()
        result_job = job.run()

        records = result_job.nodes["letter_grade"].get_all_records()
        assert records is not None
        assert records.num_rows == 5
        assert _grades_from_table(records) == EXPECTED

    def test_sync_and_async_produce_identical_results(self):
        """Sync and async execution paths yield the same grades."""
        # Sync path
        sync_job = _build_job()
        sync_result = sync_job.run()
        sync_records = sync_result.nodes["letter_grade"].get_all_records()
        assert sync_records is not None
        sync_grades = _grades_from_table(sync_records)

        # Async path
        async_job = _build_job()
        AsyncPipelineOrchestrator().run(async_job.dag)
        async_job.store.at(*async_job._name).flush()
        async_job.store.at(*async_job._name).at("_result").flush()
        async_records = async_job.nodes["letter_grade"].get_all_records()
        assert async_records is not None
        async_grades = _grades_from_table(async_records)

        assert sync_grades == async_grades == EXPECTED


class TestSyncAsyncSystemTagEquivalence:
    """Verify that sync and async pipeline execution produce identical
    system-tag column names and values in the persisted DB records."""

    def _get_system_tag_columns(self, table: pa.Table) -> list[str]:
        """Return sorted system-tag column names from a table."""
        from orcapod.system_constants import constants

        return sorted(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

    def _system_tag_data(self, table: pa.Table) -> dict[str, list]:
        """Extract system-tag columns as {col_name: sorted_values}."""
        sys_cols = self._get_system_tag_columns(table)
        return {c: sorted(table.column(c).to_pylist()) for c in sys_cols}

    def test_join_pipeline_system_tags_identical(self):
        """Join pipeline: sync and async produce the same system-tag columns."""
        sync_job = _build_job()
        sync_result = sync_job.run()
        sync_records = sync_result.nodes["letter_grade"].get_all_records(
            columns={"system_tags": True}
        )
        assert sync_records is not None

        async_job = _build_job()
        AsyncPipelineOrchestrator().run(async_job.dag)
        async_job.store.at(*async_job._name).flush()
        async_job.store.at(*async_job._name).at("_result").flush()
        async_records = async_job.nodes["letter_grade"].get_all_records(
            columns={"system_tags": True}
        )
        assert async_records is not None

        # System-tag column names must match
        sync_sys_cols = self._get_system_tag_columns(sync_records)
        async_sys_cols = self._get_system_tag_columns(async_records)
        assert sync_sys_cols, "Expected system-tag columns in output"
        assert sync_sys_cols == async_sys_cols

        # System-tag values must match
        sync_sys_data = self._system_tag_data(sync_records)
        async_sys_data = self._system_tag_data(async_records)
        assert sync_sys_data == async_sys_data

    def test_join_pipeline_system_tag_column_names_contain_pipeline_hash(self):
        """System-tag columns should follow the name-extending convention."""

        job = _build_job()
        AsyncPipelineOrchestrator().run(job.dag)
        job.store.at(*job._name).flush()
        job.store.at(*job._name).at("_result").flush()
        records = job.nodes["letter_grade"].get_all_records(
            columns={"system_tags": True}
        )
        assert records is not None

        sys_cols = self._get_system_tag_columns(records)
        assert len(sys_cols) > 0

        # Each system-tag column should end with :N (canonical position)
        for col in sys_cols:
            assert col[-2:] in (":0", ":1"), (
                f"System-tag column {col!r} missing canonical position suffix"
            )

    def test_all_system_tag_columns_match_between_sync_and_async(self):
        """Every system-tag column name and value in the terminal node's
        DB records should be identical between sync and async.

        Source-info columns contain run-specific UUIDs and are excluded
        from this comparison.
        """
        sync_job = _build_job()
        sync_result = sync_job.run()
        sync_records = sync_result.nodes["letter_grade"].get_all_records(
            columns={"system_tags": True}
        )
        assert sync_records is not None

        async_job = _build_job()
        AsyncPipelineOrchestrator().run(async_job.dag)
        async_job.store.at(*async_job._name).flush()
        async_job.store.at(*async_job._name).at("_result").flush()
        async_records = async_job.nodes["letter_grade"].get_all_records(
            columns={"system_tags": True}
        )
        assert async_records is not None

        # System-tag column names must match
        sync_sys_cols = self._get_system_tag_columns(sync_records)
        async_sys_cols = self._get_system_tag_columns(async_records)
        assert sync_sys_cols == async_sys_cols

        # System-tag column values must match (sort by student_id)
        sync_sorted = sync_records.sort_by("student_id")
        async_sorted = async_records.sort_by("student_id")
        for col in sync_sys_cols:
            assert (
                sync_sorted.column(col).to_pylist()
                == async_sorted.column(col).to_pylist()
            ), f"System-tag column {col!r} differs between sync and async"
