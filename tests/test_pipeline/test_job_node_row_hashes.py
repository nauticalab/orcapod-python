"""A job node's table gives every row its own content hash, in any row order (ITL-1029).

``node.as_table`` sorts its rows by tag when asked to (``all_info=True`` asks). The content
hash of a row must stay with that row. A node holds its rows in the order they were
computed: input order on the sync path, completion order on the async path. Both orders can
differ from tag order, and these tests force each of them to.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource

N_ROWS = 5

# Set by the last row of ``first_row_last`` once it is computed; the first row waits for it.
# Bound to the running loop on its first wait, so each async run gets a fresh one.
last_row_computed: asyncio.Event | None = None


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


@function_pod(output_keys="y")
async def first_row_last(x: int) -> int:
    """Return twice ``x``; row 0 finishes only after the last row is computed.

    The wait is bounded: if the last row never comes, row 0 fails and the test sees a
    missing row instead of hanging.
    """
    assert last_row_computed is not None
    if x == 0:
        await asyncio.wait_for(last_row_computed.wait(), timeout=60)
    elif x == N_ROWS - 1:
        last_row_computed.set()
    return 2 * x


def run_job(pod: Any, rows: list[dict[str, Any]], orchestrator: Any) -> Any:
    """Run ``pod`` over ``rows`` in a fresh job; return its node."""
    job = PipelineJob(name="row_hashes", database=InMemoryArrowDatabase())
    with job:
        pod.pod(
            DictSource(data=rows, tag_columns=["k"], source_id="rows"), label="node"
        )
    job.run(orchestrator=orchestrator)
    return job.nodes["node"]


def sync_with_input_out_of_tag_order() -> Any:
    """A sync run whose input rows come in reverse tag order."""
    rows = [{"k": f"r{i}", "x": i} for i in reversed(range(N_ROWS))]
    return run_job(double, rows, SyncPipelineOrchestrator())


def async_with_first_row_finishing_last() -> Any:
    """An async run whose input is in tag order but whose first row finishes last."""
    global last_row_computed
    last_row_computed = asyncio.Event()
    rows = [{"k": f"r{i}", "x": i} for i in range(N_ROWS)]
    return run_job(first_row_last, rows, AsyncPipelineOrchestrator())


RUNS = {
    "sync-input-out-of-tag-order": sync_with_input_out_of_tag_order,
    "async-completion-out-of-tag-order": async_with_first_row_finishing_last,
}

SORTED_WITH_HASHES = {
    "all_info": {"all_info": True},
    "content_hash-sort_by_tags": {
        "columns": {"content_hash": True, "sort_by_tags": True}
    },
    "custom-name-sort_by_tags": {
        "columns": {"content_hash": "row_hash", "sort_by_tags": True}
    },
}


@pytest.mark.parametrize("run", list(RUNS))
@pytest.mark.parametrize("table_args", list(SORTED_WITH_HASHES))
def test_each_row_of_a_sorted_table_has_its_own_content_hash(
    run: str, table_args: str
) -> None:
    """Every row of the tag-sorted table carries the content hash of its own data."""
    node = RUNS[run]()
    own_hash = {
        tag.as_dict()["k"]: data.content_hash().to_string()
        for tag, data in node.iter_data()
    }
    # The run must really hold its rows out of tag order, or the test proves nothing.
    assert len(own_hash) == N_ROWS
    assert list(own_hash) != sorted(own_hash)

    table = node.as_table(**SORTED_WITH_HASHES[table_args])
    hash_column = "row_hash" if table_args.startswith("custom") else "_content_hash"
    tags = table.column("k").to_pylist()
    assert tags == sorted(own_hash)
    assert table.column(hash_column).to_pylist() == [own_hash[k] for k in tags]
    # The rest of each row stays with its tag as well.
    assert table.column("y").to_pylist() == [2 * int(k[1:]) for k in tags]


@pytest.mark.parametrize("run", list(RUNS))
def test_unsorted_table_keeps_hashes_with_rows(run: str) -> None:
    """Without sorting, the table and its hashes are both in the node's own row order."""
    node = RUNS[run]()
    pairs = list(node.iter_data())

    table = node.as_table(columns={"content_hash": True})

    assert table.column("k").to_pylist() == [tag.as_dict()["k"] for tag, _ in pairs]
    assert table.column("_content_hash").to_pylist() == [
        data.content_hash().to_string() for _, data in pairs
    ]
