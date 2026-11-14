"""A job node's ``head()`` sorts the way ``as_table`` does, among the rows it has (ITL-1053).

``head(n, cfg)`` is ``as_table(cfg).slice(0, n)`` for every column configuration, whatever
order the rows were computed in: input order on the sync path, and on the async path an
order forced here (events, no sleeps) to be the reverse of tag order. Operator nodes are
tested after a sync run, or from a store a sync run filled: an operator node's head after
an async run says NOT_COMPUTED until ITL-1042 is fixed.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.streams import ViewStatus
from orcapod.types import CacheMode

N_ROWS = 6

CONFIGS: dict[str, dict[str, Any]] = {
    "default": {},
    "sort_by_tags": {"columns": {"sort_by_tags": True}},
    "all_info": {"all_info": True},
    "meta": {"columns": {"meta": True}},
    "context": {"columns": {"context": True}},
    "source": {"columns": {"source": True}},
    "system_tags": {"columns": {"system_tags": True}},
    "content_hash": {"columns": {"content_hash": True}},
    "content_hash_sorted": {"columns": {"content_hash": True, "sort_by_tags": True}},
}

CALLS: list[int] = []

# Rows are released in reverse tag order: row i waits for row i + 1. Bound to the running
# loop on first use, so each async run builds its own.
gates: list[asyncio.Event] = []


@function_pod(output_keys="y")
def triple(x: int) -> int:
    """Triple ``x``, counting every call."""
    CALLS.append(x)
    return 3 * x


@function_pod(output_keys="y")
async def triple_last_row_first(x: int) -> int:
    """Triple ``x``; row ``i`` finishes only after row ``i + 1``, so rows complete in
    reverse tag order. The wait is bounded so a failure is a missing row, not a hang."""
    CALLS.append(x)
    if x + 1 < N_ROWS:
        await asyncio.wait_for(gates[x + 1].wait(), timeout=60)
    gates[x].set()
    return 3 * x


def _source(order: list[int]) -> ArrowTableSource:
    """A source of rows ``id`` and ``x`` in the given order (non-nullable ``int64``)."""
    table = pa.table(
        {"id": order, "x": order},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="nums", label="nums")


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _build(pod: Any, store: ArrowDatabaseProtocol, order: list[int]) -> PipelineJob:
    job = PipelineJob(name="itl1053", database=store)
    with job:
        pod.pod(_source(order), label="node")
    return job


def _run(kind: str, store: ArrowDatabaseProtocol) -> PipelineJob:
    """Run a job so that its rows are stored out of tag order; return the job.

    ``sync``: the input is in reverse tag order. ``async``: the input is in tag order and
    the rows complete in reverse tag order.
    """
    gates.clear()
    if kind == "sync":
        job = _build(triple, store, list(reversed(range(N_ROWS))))
        job.run(orchestrator=SyncPipelineOrchestrator())
        return job
    gates.extend(asyncio.Event() for _ in range(N_ROWS))
    job = _build(triple_last_row_first, store, list(range(N_ROWS)))
    job.run(orchestrator=AsyncPipelineOrchestrator())
    return job


def _held_order(node: Any) -> list[int]:
    """The tag order the node holds its rows in (unsorted ``as_table``)."""
    return node.as_table().column("id").to_pylist()


@pytest.mark.parametrize("config", list(CONFIGS))
@pytest.mark.parametrize("n", [0, 3, N_ROWS, 50])
@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("run_kind", ["sync", "async"])
def test_a_function_nodes_head_is_the_prefix_of_as_table_in_any_completion_order(
    run_kind: str, store_kind: str, n: int, config: str, tmp_path: Path
) -> None:
    """After a run, and from the store in a new job, head(n, cfg) == as_table(cfg)[:n]."""
    store = _store(store_kind, tmp_path)
    job = _run(run_kind, store)
    assert _held_order(job.nodes["node"]) != sorted(_held_order(job.nodes["node"]))
    CALLS.clear()
    fresh = _build(
        triple if run_kind == "sync" else triple_last_row_first,
        store,
        list(range(N_ROWS)),
    )
    for node in (job.nodes["node"], fresh.nodes["node"]):
        head = node.head(n, **CONFIGS[config])
        assert head.status is ViewStatus.ROWS
        assert head.table.equals(node.as_table(**CONFIGS[config]).slice(0, n))
    assert CALLS == []


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("run_kind", ["sync", "async"])
def test_head_sorts_only_when_asked_to(
    run_kind: str, store_kind: str, tmp_path: Path
) -> None:
    """The default head keeps the node's order; sort_by_tags and all_info sort by tag."""
    job = _run(run_kind, _store(store_kind, tmp_path))
    node = job.nodes["node"]
    held = _held_order(node)
    assert held != sorted(held)
    assert node.head(N_ROWS).table.column("id").to_pylist() == held
    for config in ({"columns": {"sort_by_tags": True}}, {"all_info": True}):
        ids = node.head(N_ROWS, **config).table.column("id").to_pylist()
        assert ids == sorted(held)
    # The first n of the sorted rows, not the sorted first n of the held rows.
    assert node.head(2, all_info=True).table.column("id").to_pylist() == [0, 1]


@pytest.mark.parametrize("config", list(CONFIGS))
@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
def test_an_operator_nodes_head_is_the_prefix_of_as_table(
    store_kind: str, config: str, tmp_path: Path
) -> None:
    """An operator node's stored rows, out of tag order, sort as ``as_table`` sorts."""
    store = _store(store_kind, tmp_path)
    order = list(reversed(range(N_ROWS)))

    def node(mode: CacheMode) -> OperatorJobNode:
        return OperatorJobNode(
            operator=SelectDataColumns(["x"]),
            input_streams=[_source(order)],
            pipeline_database=store,
            cache_mode=mode,
        )

    ran = node(CacheMode.LOG)
    ran.execute(_source(order))
    for each in (ran, node(CacheMode.REPLAY)):
        head = each.head(3, **CONFIGS[config])
        assert head.status is ViewStatus.ROWS
        assert head.table.equals(each.as_table(**CONFIGS[config]).slice(0, 3))
    sorted_ids = node(CacheMode.REPLAY).head(3, all_info=True).table.column("id")
    assert sorted_ids.to_pylist() == [0, 1, 2]


# ---------------------------------------------------------------------------
# Equal and null tags: one deterministic sort (ITL-1053)
# ---------------------------------------------------------------------------

TIE_KEYS: list[int | None] = [2, 1, None, 2, 1, None, 0, 2]


def _tie_source() -> ArrowTableSource:
    """Rows with duplicate and null tags ``k``, each with its own ``x``."""
    table = pa.table(
        {"k": pa.array(TIE_KEYS, pa.int64()), "x": pa.array(range(N_ROWS + 2))},
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=True),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="ties", label="ties")


def _tie_run(kind: str) -> Any:
    """Run ``_tie_source`` through a function node; ``async`` completes rows in reverse."""
    gates.clear()
    job = PipelineJob(name="ties", database=InMemoryArrowDatabase())
    if kind == "sync":
        with job:
            triple.pod(_tie_source(), label="node")
        job.run(orchestrator=SyncPipelineOrchestrator())
    else:
        gates.extend(asyncio.Event() for _ in range(len(TIE_KEYS)))
        with job:
            tie_last_row_first.pod(_tie_source(), label="node")
        job.run(orchestrator=AsyncPipelineOrchestrator())
    return job.nodes["node"]


@function_pod(output_keys="y")
async def tie_last_row_first(x: int) -> int:
    """Triple ``x``; row ``i`` finishes after row ``i + 1`` (reverse completion)."""
    if x + 1 < len(TIE_KEYS):
        await asyncio.wait_for(gates[x + 1].wait(), timeout=60)
    gates[x].set()
    return 3 * x


@pytest.mark.parametrize("config", ["sort_by_tags", "all_info", "content_hash_sorted"])
def test_a_sorted_table_with_equal_and_null_tags_does_not_depend_on_completion_order(
    config: str,
) -> None:
    """Rows with equal tags are ordered by their system tags, nulls last, in any order."""
    sync_node, async_node = _tie_run("sync"), _tie_run("async")
    assert _held_order_k(sync_node) != _held_order_k(async_node)
    tables = {
        name: node.as_table(**CONFIGS[config])
        for name, node in (("sync", sync_node), ("async", async_node))
    }

    def without_source_info(table: pa.Table) -> pa.Table:
        """The table without ``_source_`` columns: their tokens name each run's rows."""
        return table.drop_columns(
            [c for c in table.column_names if c.startswith("_source_")]
        )

    assert without_source_info(tables["sync"]).equals(
        without_source_info(tables["async"])
    )
    assert tables["sync"].column("k").to_pylist() == [0, 1, 1, 2, 2, 2, None, None]
    for name, node in (("sync", sync_node), ("async", async_node)):
        for n in (0, 3, 5, 20):
            head = node.head(n, **CONFIGS[config])
            assert head.table.equals(tables[name].slice(0, n))


def _held_order_k(node: Any) -> list[int | None]:
    return node.as_table().column("k").to_pylist()


# ---------------------------------------------------------------------------
# The notebook preview sorts by tags (developer, November 12, 2026)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
def test_a_nodes_preview_is_the_same_after_any_completion_order(
    store_kind: str, tmp_path: Path
) -> None:
    """The preview of a node shows tag order: reversed completion looks like a sync run."""
    # A sync run over input in tag order holds its rows in tag order.
    ascending = _build(triple, _store(store_kind, tmp_path / "a"), list(range(N_ROWS)))
    ascending.run(orchestrator=SyncPipelineOrchestrator())
    sync_node = ascending.nodes["node"]
    async_node = _run("async", _store(store_kind, tmp_path / "b")).nodes["node"]
    assert _held_order(sync_node) != _held_order(async_node)
    assert sync_node._repr_html_() == async_node._repr_html_()
    html = async_node._repr_html_()
    assert "all 6 rows, sorted by tags" in html
    assert html.index(">0</td>") < html.index(">5</td>")
