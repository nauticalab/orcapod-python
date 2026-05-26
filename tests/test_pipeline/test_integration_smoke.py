"""Smoke tests for the PipelineJob API — basic lifecycle validation."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline, PipelineJob


def _make_source() -> ArrowTableSource:
    tbl = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "x": pa.array([10, 20, 30], type=pa.int64()),
    })
    return ArrowTableSource(tbl, tag_columns=["id"], infer_nullable=True)


def _double_x(x: int) -> int:
    return x * 2


def _increment_x(x: int) -> int:
    return x + 1


def test_smoke_run_completes():
    """PipelineJob.run() completes without error for a simple function pipeline."""
    pf = PythonDataFunction(_double_x, output_keys="doubled")
    pod = FunctionPod(pf)
    db = InMemoryArrowDatabase()

    job = PipelineJob(name="smoke", store=db)
    with job:
        pod(_make_source(), label="doubler")

    completed = job.run()
    assert completed._has_run is True


def test_smoke_nodes_accessible_after_run():
    """job.pipeline.nodes returns nodes with database attached after run."""
    pf = PythonDataFunction(_increment_x, output_keys="incremented")
    pod = FunctionPod(pf)
    db = InMemoryArrowDatabase()

    job = PipelineJob(name="smoke2", store=db)
    with job:
        pod(_make_source(), label="adder")

    completed = job.run()
    assert "adder" in completed.pipeline.nodes


def test_smoke_run_raises_without_pipeline():
    """run() raises RuntimeError when no pipeline has been recorded."""
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="empty", store=db)
    # Don't enter context — no pipeline recorded
    # run() should raise RuntimeError("No compiled pipeline")
    with pytest.raises(RuntimeError, match="No compiled pipeline"):
        job.run()
