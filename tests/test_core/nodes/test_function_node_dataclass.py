"""End-to-end tests: FunctionNode with a dataclass return type → as_df().

Fixtures are defined at module level so that get_type_hints() can resolve
annotations (local-scope classes are not reachable via __globals__) and so
DataclassLogicalTypeFactory can build a stable FQCN for registration.
"""
from __future__ import annotations

import dataclasses

import polars as pl

import orcapod as op
from orcapod.core.sources import DictSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob


# ---------------------------------------------------------------------------
# Module-level fixtures
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _SumResult:
    total: int
    delta: int


@op.function_pod("result")
def take_sum(a: int, b: int) -> _SumResult:
    return _SumResult(a + b, a - b)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_as_df_dataclass_column_populated():
    """FunctionNode with a dataclass return type produces a correct Polars DataFrame."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(store=store)
    source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

    with job:
        take_sum.pod(source)

    job.run()
    df = job.nodes["take_sum"].as_df()

    # Shape: one row, two visible columns (tag + data)
    assert df.shape[0] == 1
    assert "id" in df.columns
    assert "result" in df.columns

    # result column must NOT be Null/Any — it should be a struct or extension type
    assert df["result"].dtype != pl.Null

    # Values are correct
    row = df["result"][0]
    assert row["total"] == 8
    assert row["delta"] == 2


def test_as_df_dataclass_column_empty_node():
    """Unrun FunctionNode returns a zero-row DataFrame with the dataclass column present."""
    job = PipelineJob(store=InMemoryArrowDatabase())
    source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

    with job:
        take_sum.pod(source)

    # Deliberately do NOT call job.run() — node has no computed results yet.
    df = job.nodes["take_sum"].as_df()

    assert df.shape[0] == 0          # zero rows
    assert "id" in df.columns        # tag column present
    assert "result" in df.columns    # data column present even with no data
    assert df["result"].dtype != pl.Null


def test_as_df_empty_schema_matches_nonempty_schema():
    """Empty and populated as_df() for the same node type share identical schemas."""
    # Populated node
    source_full = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])
    job_full = PipelineJob(store=InMemoryArrowDatabase())
    with job_full:
        take_sum.pod(source_full)
    job_full.run()
    full_df = job_full.nodes["take_sum"].as_df()

    # Unrun node — same pod, fresh job
    source_empty = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])
    job_empty = PipelineJob(store=InMemoryArrowDatabase())
    with job_empty:
        take_sum.pod(source_empty)
    empty_df = job_empty.nodes["take_sum"].as_df()

    assert full_df.shape[0] > 0
    assert empty_df.shape[0] == 0
    # Column names and types must match exactly
    assert empty_df.schema == full_df.schema
