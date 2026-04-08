"""
Concrete example: build a pipeline and run it both synchronously and asynchronously.

Demonstrates that the same graph of sources, operators, and function pods
produces identical results regardless of execution strategy.

Pipeline under test
-------------------

    students ──┐
               ├── Join ──► filter(grade >= 70) ──► compute_letter_grade ──► results
    grades  ───┘

Sources:
    students: {student_id, name}
    grades:   {student_id, score}

After join:  {student_id | name, score}
After filter: only passing students (score >= 70)
After function: {student_id | letter_grade}
"""

from __future__ import annotations

import asyncio

import polars as pl
import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join, PolarsFilter
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.types import NodeConfig, PipelineConfig


# ---------------------------------------------------------------------------
# Domain functions
# ---------------------------------------------------------------------------


def compute_letter_grade(name: str, score: int) -> str:
    if score >= 90:
        return "A"
    elif score >= 80:
        return "B"
    elif score >= 70:
        return "C"
    else:
        return "F"


# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------


def make_students() -> ArrowTableStream:
    schema = pa.schema(
        [
            pa.field("student_id", pa.large_string(), nullable=False),
            pa.field("name", pa.large_string(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "student_id": pa.array(["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()),
            "name": pa.array(["Alice", "Bob", "Carol", "Dave", "Eve"], type=pa.large_string()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["student_id"])


def make_grades() -> ArrowTableStream:
    schema = pa.schema(
        [
            pa.field("student_id", pa.large_string(), nullable=False),
            pa.field("score", pa.int64(), nullable=False),
        ]
    )
    table = pa.table(
        {
            "student_id": pa.array(["s1", "s2", "s3", "s4", "s5"], type=pa.large_string()),
            "score": pa.array([95, 82, 67, 73, 55], type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["student_id"])


# The expected output: only students with score >= 70, with letter grades.
# FunctionPod replaces the packet with the function's output only.
EXPECTED = {
    "s1": "A",
    "s2": "B",
    "s4": "C",
}


# ---------------------------------------------------------------------------
# 1. Synchronous execution (existing pull-based model)
# ---------------------------------------------------------------------------


class TestSynchronousPipeline:
    """Build the pipeline with the existing sync API and verify results."""

    def test_sync_pipeline_full(self):
        # --- Build pipeline (declarative) ---
        students = make_students()
        grades = make_grades()

        # Step 1: Join on student_id
        joined = Join()(students, grades)

        # Step 2: Filter passing students (score >= 70)
        passing = PolarsFilter(predicates=(pl.col("score") >= 70,))(joined)

        # Step 3: Compute letter grade
        grade_pf = PythonPacketFunction(
            compute_letter_grade, output_keys="letter_grade"
        )
        grade_pod = FunctionPod(grade_pf)
        with_grades = grade_pod.process(passing)

        # --- Execute (pull-based: iter_packets triggers computation) ---
        results = {}
        for tag, packet in with_grades.iter_packets():
            sid = tag.as_dict()["student_id"]
            results[sid] = packet.as_dict()["letter_grade"]

        # --- Verify ---
        assert results == EXPECTED

    def test_sync_pipeline_as_table(self):
        students = make_students()
        grades = make_grades()

        joined = Join()(students, grades)
        passing = PolarsFilter(predicates=(pl.col("score") >= 70,))(joined)

        grade_pf = PythonPacketFunction(
            compute_letter_grade, output_keys="letter_grade"
        )
        with_grades = FunctionPod(grade_pf).process(passing)

        table = with_grades.as_table()
        assert table.num_rows == 3
        assert "student_id" in table.column_names
        assert "letter_grade" in table.column_names


# ---------------------------------------------------------------------------
# 2. Asynchronous execution (new push-based channel model)
# ---------------------------------------------------------------------------


class TestAsynchronousPipeline:
    """Wire the same pipeline nodes with channels and run via async_execute."""

    @pytest.mark.asyncio
    async def test_async_pipeline_full(self):
        # --- Nodes (same objects as sync) ---
        join_op = Join()
        filter_op = PolarsFilter(predicates=(pl.col("score") >= 70,))
        grade_pod = FunctionPod(
            PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
        )

        # --- Create channels for each edge in the DAG ---
        ch_students = Channel(buffer_size=16)  # source → join
        ch_grades = Channel(buffer_size=16)  # source → join
        ch_joined = Channel(buffer_size=16)  # join → filter
        ch_filtered = Channel(buffer_size=16)  # filter → function pod
        ch_output = Channel(buffer_size=16)  # function pod → sink

        # --- Source tasks push data into channels ---
        async def push_source(stream: ArrowTableStream, ch: Channel):
            for tag, packet in stream.iter_packets():
                await ch.writer.send((tag, packet))
            await ch.writer.close()

        # --- Run all stages concurrently via TaskGroup ---
        async with asyncio.TaskGroup() as tg:
            # Sources (push data into channels)
            tg.create_task(push_source(make_students(), ch_students))
            tg.create_task(push_source(make_grades(), ch_grades))

            # Join (barrier: collects both inputs, then emits)
            tg.create_task(
                join_op.async_execute(
                    [ch_students.reader, ch_grades.reader],
                    ch_joined.writer,
                )
            )

            # Filter (barrier: collects input, applies predicate, emits)
            tg.create_task(
                filter_op.async_execute(
                    [ch_joined.reader],
                    ch_filtered.writer,
                )
            )

            # Function pod (streaming: processes packets as they arrive)
            tg.create_task(
                grade_pod.async_execute(
                    [ch_filtered.reader],
                    ch_output.writer,
                )
            )

        # --- Collect and verify ---
        output_rows = await ch_output.reader.collect()

        results = {}
        for tag, packet in output_rows:
            sid = tag.as_dict()["student_id"]
            results[sid] = packet.as_dict()["letter_grade"]

        assert results == EXPECTED

    @pytest.mark.asyncio
    async def test_async_pipeline_with_concurrency_control(self):
        """Same pipeline but with max_concurrency=1 on the function pod."""
        join_op = Join()
        filter_op = PolarsFilter(predicates=(pl.col("score") >= 70,))
        grade_pod = FunctionPod(
            PythonPacketFunction(compute_letter_grade, output_keys="letter_grade"),
            node_config=NodeConfig(max_concurrency=1),
        )

        ch_students = Channel(buffer_size=16)
        ch_grades = Channel(buffer_size=16)
        ch_joined = Channel(buffer_size=16)
        ch_filtered = Channel(buffer_size=16)
        ch_output = Channel(buffer_size=16)

        async def push_source(stream, ch):
            for tag, packet in stream.iter_packets():
                await ch.writer.send((tag, packet))
            await ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push_source(make_students(), ch_students))
            tg.create_task(push_source(make_grades(), ch_grades))
            tg.create_task(
                join_op.async_execute(
                    [ch_students.reader, ch_grades.reader],
                    ch_joined.writer,
                )
            )
            tg.create_task(
                filter_op.async_execute(
                    [ch_joined.reader],
                    ch_filtered.writer,
                )
            )
            tg.create_task(
                grade_pod.async_execute(
                    [ch_filtered.reader],
                    ch_output.writer,
                    pipeline_config=PipelineConfig(channel_buffer_size=16),
                )
            )

        output_rows = await ch_output.reader.collect()
        results = {
            tag.as_dict()["student_id"]: packet.as_dict()["letter_grade"]
            for tag, packet in output_rows
        }
        assert results == EXPECTED


# ---------------------------------------------------------------------------
# 3. Side-by-side: sync vs async produce identical output
# ---------------------------------------------------------------------------


class TestSyncAsyncEquivalence:
    """Run both modes on the same input and compare results."""

    def _run_sync(self) -> dict[str, str]:
        students = make_students()
        grades = make_grades()
        joined = Join()(students, grades)
        passing = PolarsFilter(predicates=(pl.col("score") >= 70,))(joined)
        grade_pod = FunctionPod(
            PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
        )
        with_grades = grade_pod.process(passing)

        return {
            tag.as_dict()["student_id"]: packet.as_dict()["letter_grade"]
            for tag, packet in with_grades.iter_packets()
        }

    async def _run_async(self) -> dict[str, str]:
        join_op = Join()
        filter_op = PolarsFilter(predicates=(pl.col("score") >= 70,))
        grade_pod = FunctionPod(
            PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
        )

        ch_s = Channel(buffer_size=16)
        ch_g = Channel(buffer_size=16)
        ch_j = Channel(buffer_size=16)
        ch_f = Channel(buffer_size=16)
        ch_o = Channel(buffer_size=16)

        async def push(stream, ch):
            for tag, packet in stream.iter_packets():
                await ch.writer.send((tag, packet))
            await ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push(make_students(), ch_s))
            tg.create_task(push(make_grades(), ch_g))
            tg.create_task(
                join_op.async_execute([ch_s.reader, ch_g.reader], ch_j.writer)
            )
            tg.create_task(filter_op.async_execute([ch_j.reader], ch_f.writer))
            tg.create_task(grade_pod.async_execute([ch_f.reader], ch_o.writer))

        return {
            tag.as_dict()["student_id"]: packet.as_dict()["letter_grade"]
            for tag, packet in await ch_o.reader.collect()
        }

    @pytest.mark.asyncio
    async def test_sync_and_async_produce_same_results(self):
        sync_results = self._run_sync()
        async_results = await self._run_async()
        assert sync_results == async_results

    @pytest.mark.asyncio
    async def test_both_produce_three_passing_students(self):
        sync_results = self._run_sync()
        async_results = await self._run_async()
        assert len(sync_results) == 3
        assert len(async_results) == 3

    @pytest.mark.asyncio
    async def test_both_have_same_student_ids(self):
        sync_results = self._run_sync()
        async_results = await self._run_async()
        assert set(sync_results.keys()) == set(async_results.keys())
        assert set(sync_results.keys()) == {"s1", "s2", "s4"}
