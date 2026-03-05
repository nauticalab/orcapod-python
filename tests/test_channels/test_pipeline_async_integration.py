"""
Integration test — end-to-end async pipeline.

Shows the recommended workflow in a single, linear example:

1. Define domain functions with ``@function_pod``.
2. Build a pipeline with the ``Pipeline`` context manager.
3. Run the pipeline asynchronously via ``AsyncPipelineOrchestrator``.
4. Retrieve persisted results synchronously from the pipeline database.

Pipeline::

    students ──┐
               ├── Join ──► compute_letter_grade
    grades  ───┘

Tags:   student_id
Packet: name, score  →  letter_grade
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod import ArrowTableSource, function_pod
from orcapod.core.operators import Join
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, Pipeline
from orcapod.types import ExecutorType, PipelineConfig


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
        "student_id": pa.array(
            ["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()
        ),
        "name": pa.array(
            ["Alice", "Bob", "Carol", "Dave", "Eve"], type=pa.large_string()
        ),
    }
)

GRADES = pa.table(
    {
        "student_id": pa.array(
            ["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()
        ),
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


# ── 3. Build, run async, retrieve sync ───────────────────────────────────


def _build_pipeline() -> Pipeline:
    """Construct and auto-compile the pipeline."""
    db = InMemoryArrowDatabase()
    pipeline = Pipeline(
        name="grades_pipeline",
        pipeline_database=db,
        auto_compile=True,
    )

    with pipeline:
        students = ArrowTableSource(STUDENTS, tag_columns=["student_id"])
        grades = ArrowTableSource(GRADES, tag_columns=["student_id"])

        joined = Join()(students, grades, label="join")
        compute_letter_grade.pod(joined, label="letter_grade")

    return pipeline


def _grades_from_stream(stream) -> dict[str, str]:
    """Extract {student_id: letter_grade} from any iterable stream."""
    return {
        tag.as_dict()["student_id"]: packet.as_dict()["letter_grade"]
        for tag, packet in stream.iter_packets()
    }


def _grades_from_table(table: pa.Table) -> dict[str, str]:
    """Extract {student_id: letter_grade} from a PyArrow table."""
    return {
        table.column("student_id")[i].as_py(): table.column("letter_grade")[i].as_py()
        for i in range(table.num_rows)
    }


# ── Tests ────────────────────────────────────────────────────────────────


class TestAsyncPipelineIntegration:
    """Single narrative: build → run async → retrieve sync → verify."""

    def test_orchestrator_produces_correct_streamed_output(self):
        """AsyncPipelineOrchestrator returns a stream with expected grades."""
        pipeline = _build_pipeline()

        # Run asynchronously — returns an ArrowTableStream
        orchestrator = AsyncPipelineOrchestrator()
        result_stream = orchestrator.run(pipeline)

        assert _grades_from_stream(result_stream) == EXPECTED

    def test_pipeline_run_with_async_executor(self):
        """Pipeline.run() with ASYNC_CHANNELS delegates to the orchestrator."""
        pipeline = _build_pipeline()

        config = PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS)
        pipeline.run(config=config)

    @pytest.mark.asyncio
    async def test_orchestrator_run_async_from_event_loop(self):
        """run_async() works when an event loop is already running."""
        pipeline = _build_pipeline()

        orchestrator = AsyncPipelineOrchestrator()
        result_stream = await orchestrator.run_async(pipeline)

        assert _grades_from_stream(result_stream) == EXPECTED

    def test_sync_run_then_db_retrieval(self):
        """Baseline: sync run() populates the DB for later retrieval."""
        pipeline = _build_pipeline()
        pipeline.run()

        records = pipeline.letter_grade.get_all_records()
        assert records is not None
        assert records.num_rows == 5
        assert _grades_from_table(records) == EXPECTED

    def test_sync_and_async_produce_identical_results(self):
        """Sync and async execution paths yield the same grades."""
        # Sync path — results come from the pipeline database
        sync_pipeline = _build_pipeline()
        sync_pipeline.run()
        sync_records = sync_pipeline.letter_grade.get_all_records()
        assert sync_records is not None
        sync_grades = _grades_from_table(sync_records)

        # Async path — results come from the returned stream
        async_pipeline = _build_pipeline()
        orchestrator = AsyncPipelineOrchestrator()
        async_stream = orchestrator.run(async_pipeline)
        async_grades = _grades_from_stream(async_stream)

        assert sync_grades == async_grades == EXPECTED
