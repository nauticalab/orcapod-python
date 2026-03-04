"""
Integration test: Pipeline + @function_pod decorator + async orchestrator.

Demonstrates the recommended workflow:

1. **Define** domain functions with the ``@function_pod`` decorator.
2. **Build** a pipeline using the ``Pipeline`` context manager, which
   records the graph and auto-compiles persistent nodes on exit.
3. **Execute** the compiled pipeline asynchronously via the channel-based
   async orchestrator (``asyncio.TaskGroup`` + ``async_execute``).
4. **Retrieve** results synchronously from the pipeline databases.

Pipeline under test::

    students ──┐
               ├── Join ──► compute_letter_grade ──► results
    grades  ───┘

Sources:
    students: {student_id, name}
    grades:   {student_id, score}

After join:     {student_id | name, score}
After function: {student_id | letter_grade}  (failing students filtered out)
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.function_pod import PersistentFunctionNode, function_pod
from orcapod.core.operator_node import PersistentOperatorNode
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.protocols.core_protocols import (
    PacketProtocol,
    TagProtocol,
)


# ---------------------------------------------------------------------------
# Domain functions (decorated the recommended way)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------


def make_students() -> ArrowTableSource:
    table = pa.table(
        {
            "student_id": pa.array(
                ["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()
            ),
            "name": pa.array(
                ["Alice", "Bob", "Carol", "Dave", "Eve"], type=pa.large_string()
            ),
        }
    )
    return ArrowTableSource(table, tag_columns=["student_id"])


def make_grades() -> ArrowTableSource:
    table = pa.table(
        {
            "student_id": pa.array(
                ["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()
            ),
            "score": pa.array([95, 82, 67, 73, 55], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["student_id"])


EXPECTED_GRADES = {
    "s1": "A",   # 95
    "s2": "B",   # 82
    "s3": "F",   # 67
    "s4": "C",   # 73
    "s5": "F",   # 55
}


# ---------------------------------------------------------------------------
# Async orchestrator helper
# ---------------------------------------------------------------------------


async def push_source_to_channel(
    source: ArrowTableSource,
    ch: Channel,
) -> None:
    """Push all (tag, packet) pairs from a source into a channel, then close."""
    for tag, packet in source.iter_packets():
        await ch.writer.send((tag, packet))
    await ch.writer.close()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPipelineAsyncIntegration:
    """Build a Pipeline with @function_pod, run it async, retrieve from DB."""

    def _build_pipeline(self) -> Pipeline:
        """Build and compile the pipeline using the Pipeline context manager."""
        db = InMemoryArrowDatabase()
        pipeline = Pipeline(
            name="grades_pipeline",
            pipeline_database=db,
            auto_compile=True,
        )

        with pipeline:
            students = make_students()
            grades = make_grades()

            # Step 1: Join on student_id
            joined = Join()(students, grades, label="join")

            # Step 2: Compute letter grades (using the @function_pod decorator)
            compute_letter_grade.pod(joined, label="letter_grade")

        return pipeline

    def test_pipeline_compiles_correct_node_types(self):
        """Verify that compile() creates the correct persistent node types."""
        pipeline = self._build_pipeline()

        assert pipeline._compiled
        nodes = pipeline.compiled_nodes
        assert "join" in nodes
        assert "letter_grade" in nodes

        assert isinstance(nodes["join"], PersistentOperatorNode)
        assert isinstance(nodes["letter_grade"], PersistentFunctionNode)

    def test_sync_pipeline_produces_expected_results(self):
        """Baseline: sync run() produces the expected letter grades."""
        pipeline = self._build_pipeline()
        pipeline.run()

        records = pipeline.letter_grade.get_all_records()
        assert records is not None
        assert records.num_rows == 5

        results = {
            records.column("student_id")[i].as_py(): records.column("letter_grade")[i].as_py()
            for i in range(records.num_rows)
        }
        assert results == EXPECTED_GRADES

    @pytest.mark.asyncio
    async def test_async_orchestrator_produces_expected_results(self):
        """Run the compiled pipeline asynchronously and verify streaming results."""
        pipeline = self._build_pipeline()

        join_node = pipeline.join
        grade_node = pipeline.letter_grade

        # Channels for each edge:
        #   students → join, grades → join, join → letter_grade, letter_grade → output
        ch_students = Channel(buffer_size=16)
        ch_grades = Channel(buffer_size=16)
        ch_joined = Channel(buffer_size=16)
        ch_output = Channel(buffer_size=16)

        async with asyncio.TaskGroup() as tg:
            # Source producers
            tg.create_task(push_source_to_channel(make_students(), ch_students))
            tg.create_task(push_source_to_channel(make_grades(), ch_grades))

            # Join (barrier: collects both inputs, then emits)
            tg.create_task(
                join_node.async_execute(
                    [ch_students.reader, ch_grades.reader],
                    ch_joined.writer,
                )
            )

            # Function pod (streaming: processes packets as they arrive)
            tg.create_task(
                grade_node.async_execute(
                    [ch_joined.reader],
                    ch_output.writer,
                )
            )

        output_rows = await ch_output.reader.collect()
        results = {
            tag.as_dict()["student_id"]: packet.as_dict()["letter_grade"]
            for tag, packet in output_rows
        }
        assert results == EXPECTED_GRADES

    @pytest.mark.asyncio
    async def test_async_then_sync_db_retrieval(self):
        """Run pipeline async, then retrieve results synchronously from DB.

        This is the key use-case: async streaming execution populates the
        pipeline database, and later callers can retrieve results without
        re-running the pipeline.
        """
        pipeline = self._build_pipeline()

        join_node = pipeline.join
        grade_node = pipeline.letter_grade

        # --- Async execution ---
        ch_students = Channel(buffer_size=16)
        ch_grades = Channel(buffer_size=16)
        ch_joined = Channel(buffer_size=16)
        ch_output = Channel(buffer_size=16)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push_source_to_channel(make_students(), ch_students))
            tg.create_task(push_source_to_channel(make_grades(), ch_grades))
            tg.create_task(
                join_node.async_execute(
                    [ch_students.reader, ch_grades.reader],
                    ch_joined.writer,
                )
            )
            tg.create_task(
                grade_node.async_execute(
                    [ch_joined.reader],
                    ch_output.writer,
                )
            )

        # Drain the output channel
        await ch_output.reader.collect()

        # --- Synchronous DB retrieval (no re-computation) ---
        records = grade_node.get_all_records()
        assert records is not None
        assert records.num_rows == 5

        results = {
            records.column("student_id")[i].as_py(): records.column("letter_grade")[i].as_py()
            for i in range(records.num_rows)
        }
        assert results == EXPECTED_GRADES

    @pytest.mark.asyncio
    async def test_sync_and_async_produce_identical_results(self):
        """Run both sync and async pipelines, verify identical output."""
        # --- Sync ---
        sync_pipeline = self._build_pipeline()
        sync_pipeline.run()

        sync_records = sync_pipeline.letter_grade.get_all_records()
        assert sync_records is not None
        sync_results = {
            sync_records.column("student_id")[i].as_py(): sync_records.column("letter_grade")[i].as_py()
            for i in range(sync_records.num_rows)
        }

        # --- Async ---
        async_pipeline = self._build_pipeline()
        join_node = async_pipeline.join
        grade_node = async_pipeline.letter_grade

        ch_s = Channel(buffer_size=16)
        ch_g = Channel(buffer_size=16)
        ch_j = Channel(buffer_size=16)
        ch_o = Channel(buffer_size=16)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push_source_to_channel(make_students(), ch_s))
            tg.create_task(push_source_to_channel(make_grades(), ch_g))
            tg.create_task(
                join_node.async_execute(
                    [ch_s.reader, ch_g.reader], ch_j.writer
                )
            )
            tg.create_task(
                grade_node.async_execute([ch_j.reader], ch_o.writer)
            )

        async_streamed = await ch_o.reader.collect()
        async_results = {
            tag.as_dict()["student_id"]: packet.as_dict()["letter_grade"]
            for tag, packet in async_streamed
        }

        assert sync_results == async_results
        assert sync_results == EXPECTED_GRADES
