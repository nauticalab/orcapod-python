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
