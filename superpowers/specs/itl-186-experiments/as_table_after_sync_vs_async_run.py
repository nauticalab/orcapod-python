"""On main: ``node.as_table()`` after a run returns different rows on the two orchestrators.

Run from the repository root:

    uv run python superpowers/specs/itl-186-experiments/as_table_after_sync_vs_async_run.py

Two jobs with the same pipeline run one after the other on one store. Their sources have
the same schema and ``source_id`` (so the same content hash) and different rows: job A has
rows k=0..4, job B rows k=0..1. After B's run:

* sync orchestrator: ``B.nodes["double"].as_table()`` returns B's 2 rows;
* async orchestrator: it returns 5 rows, every row stored for the node.

``get_all_records()`` returns 5 rows on both. Found while measuring ITL-186 (decision D2);
not caused by it and not changed there.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import DictSource


def run(keys: list[int], store: Path, use_async: bool) -> PipelineJob:
    @function_pod(output_keys="w")
    def double(v: int) -> int:
        return 2 * v

    job = PipelineJob(name="job", database=DeltaTableDatabase(store))
    with job:
        source = DictSource(
            [{"k": k, "v": k} for k in keys], tag_columns=["k"], source_id="same"
        )
        double.pod(source, label="double")
    job.run(orchestrator=AsyncPipelineOrchestrator() if use_async else None)
    return job


for use_async in (False, True):
    store = Path(tempfile.mkdtemp()) / "store"
    run([0, 1, 2, 3, 4], store, use_async)
    node = run([0, 1], store, use_async).nodes["double"]
    print(
        f"{'async' if use_async else 'sync '}: B's source has 2 rows; "
        f"as_table() -> {node.as_table().num_rows} rows, "
        f"get_all_records() -> {node.get_all_records().num_rows} rows"
    )
