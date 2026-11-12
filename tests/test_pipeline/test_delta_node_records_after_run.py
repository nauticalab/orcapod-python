"""A function node on a Delta store returns every row right after its first run (ITL-864).

``PipelineJob.run`` flushes through handles other than the node's own, so a node
handle that cached the table during the run used to keep reading an old snapshot
and return only some of the rows it had just written.
"""

from __future__ import annotations

from pathlib import Path

import pytest

import orcapod as op
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator


@pytest.mark.parametrize(
    "orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator]
)
@pytest.mark.parametrize("n_rows", [2, 3])
def test_node_returns_every_row_right_after_first_run(
    tmp_path: Path,
    orchestrator: type[SyncPipelineOrchestrator | AsyncPipelineOrchestrator],
    n_rows: int,
) -> None:
    source = op.sources.DictSource(
        [{"id": i, "x": 10 * i} for i in range(n_rows)], tag_columns=["id"]
    )

    @op.function_pod(output_keys=["result"])
    def double(x: int) -> int:
        return x * 2

    job = PipelineJob(store=DeltaTableDatabase(tmp_path))
    with job:
        double.pod(source, label="double")
    job.run(orchestrator=orchestrator())

    records = job.nodes["double"].get_all_records()
    assert records is not None
    assert sorted(records.column("result").to_pylist()) == [
        20 * i for i in range(n_rows)
    ]
