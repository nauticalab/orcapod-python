"""Sync and async runs store the same pipeline records after a select or drop (ITL-1040).

The async paths of ``SelectDataColumns`` and ``DropDataColumns`` drop columns datagram by
datagram. Before ITL-1040 the dropped columns' source info survived there, so an async run
stored an extra ``_source_<dropped column>`` in the function node's records that a sync run
did not: the stored table's schema depended on the orchestrator.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import DropDataColumns, SelectDataColumns
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource

STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path),
}

# Columns that differ between two runs by design: the output datagram's uuid, the token
# that carries it, and the time the row was computed.
PER_RUN_COLUMNS = ("__data_id", "_source_y", "__pod_ts")


@function_pod(output_keys="y")
def times_ten(x: int) -> int:
    """Return ``10 * x``."""
    return 10 * x


def source() -> ArrowTableSource:
    """Tag ``k``; data ``x`` (the pod's input) and ``s``, ``m`` (not its input)."""
    table = pa.table(
        {
            "k": ["a", "b", "c"],
            "x": [1, 2, 3],
            "s": ["p", "q", "r"],
            "m": [0.5, 1.5, 2.5],
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
                pa.field("s", pa.large_string(), nullable=False),
                pa.field("m", pa.float64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="src")


def automatic_select(src: ArrowTableSource) -> Any:
    """``pod(src)`` selects the pod's input ``x`` itself."""
    return times_ten.pod(src, label="node")


def explicit_select(src: ArrowTableSource) -> Any:
    """``SelectDataColumns(["x"])`` before the pod."""
    return times_ten.pod(SelectDataColumns(["x"])(src), label="node")


def explicit_drop(src: ArrowTableSource) -> Any:
    """``DropDataColumns(["s", "m"])`` before the pod."""
    return times_ten.pod(DropDataColumns(["s", "m"])(src), label="node")


PIPELINES = {
    "automatic_select": automatic_select,
    "explicit_select": explicit_select,
    "explicit_drop": explicit_drop,
}


def stored_records(
    pipeline: str, orchestrator: str, store_kind: str, directory: Path
) -> pa.Table:
    """The function node's stored records after a run, rows in tag order."""
    job = PipelineJob(name="itl1040", store=STORES[store_kind](directory))
    with job:
        PIPELINES[pipeline](source())
    job.run(
        orchestrator=SyncPipelineOrchestrator()
        if orchestrator == "sync"
        else AsyncPipelineOrchestrator()
    )
    records = job.nodes["node"].get_all_records(all_info=True)
    assert records is not None
    return records.sort_by("k")


@pytest.mark.parametrize("store_kind", list(STORES))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_async_stores_the_records_a_sync_run_stores(
    pipeline: str, store_kind: str, tmp_path: Path
) -> None:
    sync = stored_records(pipeline, "sync", store_kind, tmp_path / "sync")
    async_ = stored_records(pipeline, "async", store_kind, tmp_path / "async")

    assert async_.schema == sync.schema
    assert "_source_s" not in async_.column_names
    assert "_source_m" not in async_.column_names
    assert async_.drop_columns(list(PER_RUN_COLUMNS)).equals(
        sync.drop_columns(list(PER_RUN_COLUMNS))
    )


@pytest.mark.parametrize("pipeline", ["explicit_select", "explicit_drop"])
def test_the_operator_node_table_is_the_same_on_both_orchestrators(
    pipeline: str,
) -> None:
    """The select or drop node itself shows the same columns after either run."""
    tables = []
    for orchestrator in (SyncPipelineOrchestrator, AsyncPipelineOrchestrator):
        job = PipelineJob(name="itl1040", store=InMemoryArrowDatabase())
        with job:
            src = source()
            if pipeline == "explicit_select":
                SelectDataColumns(["x"])(src, label="op")
            else:
                DropDataColumns(["s", "m"])(src, label="op")
        job.run(orchestrator=orchestrator())
        tables.append(job.nodes["op"].as_table(all_info=True).sort_by("k"))
    assert tables[1].schema == tables[0].schema
    assert tables[1].equals(tables[0])
