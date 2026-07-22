"""Integration tests for error_policy='continue' + Join schema compatibility.

Regression for ITL-563: failed functions produced empty tables with wrong
nullable flags, causing Join to raise InputValidationError and abort the pipeline.
"""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators.join import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator


def _make_source_with_required_field(subjects: list[str], values: list[int]) -> ArrowTableSource:
    """Source with tag=subject (str, non-nullable) and data=value (int, non-nullable)."""
    schema = pa.schema([
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {"subject": subjects, "value": values},
        schema=schema,
    )
    return ArrowTableSource(table, tag_columns=["subject"])


def test_failed_function_with_join_does_not_abort_pipeline():
    """Topology: source → failing_function → Join ← source.

    Under error_policy='continue', the failing function should be logged,
    the Join should produce zero rows (empty × non-empty = empty), and
    orchestration should complete without raising.
    """
    src = _make_source_with_required_field(["a", "b"], [1, 2])

    def always_fails(value: int) -> int:
        raise RuntimeError("intentional failure")

    pf = PythonDataFunction(always_fails, output_keys="transformed")
    failing_pod = FunctionPod(pf)

    job = PipelineJob(name="test_join_continue", store=InMemoryArrowDatabase())
    with job:
        failing_out = failing_pod(src, label="failing")
        Join()(failing_out, src, label="joined")

    # Should complete without raising despite the failing function.
    # error_policy is passed via the SyncPipelineOrchestrator constructor.
    job.run(orchestrator=SyncPipelineOrchestrator(error_policy="continue"))

    joined_records = job.nodes["joined"].get_all_records()
    # Join of empty × non-empty = empty (zero rows), not an error
    assert joined_records is None or joined_records.num_rows == 0


def test_empty_buffer_schema_preserves_nullability():
    """_materialize_as_stream on an empty buffer preserves required field nullability."""
    src = _make_source_with_required_field(["x"], [10])

    def identity(value: int) -> int:
        return value

    pf = PythonDataFunction(identity, output_keys="result")
    pod = FunctionPod(pf)

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="test_empty_schema", store=db)
    with job:
        pod(src, label="fn")

    # Don't run — buffer will be empty
    node = job.nodes["fn"]
    stream = SyncPipelineOrchestrator._materialize_as_stream([], node)
    tag_schema, data_schema = stream.output_schema()

    # "result" is declared as int (required), must NOT become int | None
    assert data_schema["result"] == int
