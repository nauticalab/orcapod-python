"""One store, two orchestrators: a run on one extends a store written by the other (ITL-885).

A job run by ``SyncPipelineOrchestrator`` and then by ``AsyncPipelineOrchestrator`` on the same
store, or the other way round, must find every stored result: the second run computes
nothing, and every node (source, operator and function nodes) reads back the rows of the
first run. Before ITL-906 the async path named the system-tag columns of ``GroupBy``,
``Batch`` and ``MergeJoin`` after a non-source input differently from sync, so the function
node downstream wrote a second set of system-tag columns into the same pipeline table and
the second run failed.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.operators import Batch, GroupBy, MergeJoin, SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

CALLS: list[str] = []


def _source(name: str, x: list[int]) -> ArrowTableSource:
    """Tags ``id: int8`` and ``grp: large_string``; non-nullable ``x: int8``."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], type=pa.int8()),
            "grp": pa.array(["g1", "g1", "g2", "g2"], type=pa.large_string()),
            "x": pa.array(x, type=pa.int8()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("grp", pa.large_string(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id=name)


@function_pod(output_keys="y")
def double(x: int) -> int:
    """The function node upstream of the operator."""
    CALLS.append("double")
    return x * 2


@function_pod(output_keys="first")
def first(y: list[int]) -> int:
    """A function node after a reducing operator."""
    CALLS.append("first")
    return y[0]


@function_pod(output_keys="total")
def total(x: list[int]) -> int:
    """A function node after ``MergeJoin`` or ``Batch``."""
    CALLS.append("total")
    return sum(x)


def _group_by_after_function_pod(a: StreamProtocol, b: StreamProtocol) -> None:
    """ITL-885's graph: source, function node, ``GroupBy``, function node."""
    grouped = GroupBy(by=["grp"])(double.pod(a, label="double"), label="grouped")
    first.pod(grouped, label="first")


def _merge_join_after_operator(a: StreamProtocol, b: StreamProtocol) -> None:
    """``MergeJoin`` of an operator's output and a source, then a function node."""
    merged = MergeJoin()(
        SelectDataColumns(["x"])(a, label="selected"),
        SelectDataColumns(["x"])(b, label="selected_b"),
        label="merged",
    )
    total.pod(merged, label="total")


def _batch_after_operator(a: StreamProtocol, b: StreamProtocol) -> None:
    """``Batch`` of an operator's output, then a function node."""
    batched = Batch(batch_size=2)(SelectDataColumns(["x"])(a, label="selected"))
    total.pod(batched, label="total")


def _function_pod_over_source(a: StreamProtocol, b: StreamProtocol) -> None:
    """A function node reading a source directly: both orchestrators always agreed."""
    double.pod(a, label="double")


GRAPHS: dict[str, Callable[[StreamProtocol, StreamProtocol], None]] = {
    "function_pod_over_source": _function_pod_over_source,
    "group_by_after_function_pod": _group_by_after_function_pod,
    "merge_join_after_operator": _merge_join_after_operator,
    "batch_after_operator": _batch_after_operator,
}


def _run(
    graph: str, orchestrator: str, store: ArrowDatabaseProtocol
) -> tuple[PipelineJob, list[str]]:
    """Build ``graph`` in a new job on ``store``, run it; return the job and the pod calls."""
    CALLS.clear()
    job = PipelineJob(name="mixed", database=store)
    with job:
        GRAPHS[graph](_source("a", [1, 2, 3, 4]), _source("b", [5, 6, 7, 8]))
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    return job, list(CALLS)


def _read_back(job: PipelineJob) -> dict[str, list[str]]:
    """Every node's rows: tag and data content hashes and system tags, sorted."""
    return {
        label: sorted(
            repr(
                (
                    tag.content_hash().to_string(),
                    data.content_hash().to_string(),
                    sorted(tag.system_tags().items()),
                )
            )
            for tag, data in node.iter_data()
        )
        for label, node in job.nodes.items()
    }


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("orders", [("sync", "async"), ("async", "sync")])
@pytest.mark.parametrize("graph", list(GRAPHS))
def test_second_orchestrator_finds_every_stored_result(
    graph: str, orders: tuple[str, str], store_kind: str, tmp_path: Path
) -> None:
    """The second run on the other orchestrator computes nothing and reads back the same rows."""
    store = _store(store_kind, tmp_path)
    first_job, first_calls = _run(graph, orders[0], store)
    second_job, second_calls = _run(graph, orders[1], store)

    assert first_calls
    assert second_calls == []
    first_rows = _read_back(first_job)
    assert all(first_rows.values())
    assert _read_back(second_job) == first_rows
