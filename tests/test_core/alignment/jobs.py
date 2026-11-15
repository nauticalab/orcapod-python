"""Jobs the row-alignment tests run, and the tables they read from them (ITL-1034).

The job: source ``A`` feeds the gated function node ``scaled`` (``y = 10 * x``); ``scaled``
feeds an operator node ``joined`` (``Join`` with source ``B``, cached in the store so it can
be read back) and an operator node ``grouped`` (``GroupBy`` on ``g``, which drops the tag
``n``: ``DROPPED_TAGS``). A run is made with one
of four entry points and, on the async ones, with the gate forcing a completion order.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa

from orcapod import PipelineJob
from orcapod.core.operators import GroupBy, Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.run_summary import RunSummary
from orcapod.system_constants import constants
from orcapod.types import CacheMode

from .checker import Layout
from .fixtures import Sources, content_hash_column, keys_a, make_sources
from .gate import GATE, gated_fragile, gated_scale, scale
from .oracle import Origin

JOB_NAME = "alignment"

# How a job is run. "sync" runs rows in input order; the others run them concurrently.
MODES = ("sync", "async_run", "async_run_async", "async_in_loop")

STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path),
}

# Columns that differ between two runs by design: a function output's token carries the
# output datagram's uuid, made when the row is computed.
PER_RUN_COLUMNS = ("_source_y",)


def run_job(job: PipelineJob, mode: str) -> RunSummary:
    """Run ``job`` with the entry point ``mode`` names and return its run summary."""
    if mode == "sync":
        return job.run(orchestrator=SyncPipelineOrchestrator())
    if mode == "async_run":
        return job.run(orchestrator=AsyncPipelineOrchestrator())
    if mode == "async_run_async":
        return asyncio.run(job.run_async())

    async def notebook_cell() -> RunSummary:
        """A notebook cell: the job run inside the kernel's running loop."""
        return job.run(orchestrator=AsyncPipelineOrchestrator())

    if mode == "async_in_loop":
        return asyncio.run(notebook_cell())
    raise ValueError(f"unknown mode {mode!r}")


def derive_y(sources: Sources) -> None:
    """Tell the oracle that ``y`` is ``gated_scale`` of an ``A`` row."""

    def y_of(origin: Origin) -> Any:
        """``gated_scale``'s output for ``origin``."""
        return scale(origin.values["x"])

    sources.oracle.derive("y", "A", y_of)


def build_job(
    scenario: str,
    input_order: str,
    store: Any,
    *,
    fragile: bool = False,
    cache: CacheMode = CacheMode.LOG,
) -> tuple[PipelineJob, Sources]:
    """Record the alignment job of ``scenario`` on ``store``; the oracle knows ``y``."""
    sources = make_sources(scenario, input_order)
    derive_y(sources)
    job = PipelineJob(name=JOB_NAME, database=store)
    pod = gated_fragile if fragile else gated_scale
    with job:
        scaled = pod.pod(sources.a, label="scaled")
        Join()(scaled, sources.b, label="joined")
        GroupBy(by=["g"])(scaled, label="grouped")
    # OperatorNode.attach_databases is the public way to store an operator's rows.
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=cache
    )
    return job, sources


@dataclass
class Run:
    """A finished run: the job, its sources and what the gate saw."""

    job: PipelineJob
    sources: Sources
    store: Any
    summary: RunSummary
    completed: list[Any]


def make_run(
    scenario: str,
    input_order: str,
    mode: str,
    order: list[Any] | None,
    store_kind: str,
    directory: Path,
    *,
    fragile: bool = False,
) -> Run:
    """Run the alignment job; on an async mode the gate forces ``order``."""
    store = STORES[store_kind](directory / "store")
    job, sources = build_job(scenario, input_order, store, fragile=fragile)
    if order is None:
        GATE.disarm()
    else:
        GATE.arm(order)
    try:
        summary = run_job(job, mode)
    finally:
        completed = list(GATE.completed)
        GATE.disarm()
    return Run(job, sources, store, summary, completed)


def function_tokens(job: PipelineJob) -> dict[tuple[str, bytes], str]:
    """The stored ``_source_y`` of every ``scaled`` record, keyed by its input record id."""
    records = job.nodes["scaled"].get_all_records(all_info=True)
    assert records is not None, "the function node stored no records"
    (record_id,) = [
        c
        for c in records.column_names
        if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
    ]
    return {
        ("y", row[record_id]): row["_source_y"]
        for row in records.select([record_id, "_source_y"]).to_pylist()
    }


# ---------------------------------------------------------------------------
# Views: the tables one node gives, and their layouts
# ---------------------------------------------------------------------------


# The input tags each node drops: GroupBy keeps only its by tags (ITL-1070).
DROPPED_TAGS: dict[str, tuple[str, ...]] = {"grouped": ("n",)}


@dataclass(frozen=True)
class View:
    """One table-producing call on one node of the job.

    Attributes:
        node: The node's label.
        call: ``as_table``, ``head`` or ``as_source``.
    """

    node: str
    call: str

    @property
    def name(self) -> str:
        """``<node>.<call>``, the test id."""
        return f"{self.node}.{self.call}"


VIEWS = (
    View("scaled", "as_table"),
    View("scaled", "head"),
    View("scaled", "as_source"),
    View("joined", "as_table"),
    View("joined", "head"),
    View("grouped", "as_table"),
    View("grouped", "head"),
)


def produce(job: PipelineJob, view: View, config: dict[str, Any]) -> pa.Table:
    """The table ``view`` gives with the column configuration ``config``."""
    node = job.nodes[view.node]
    if view.call == "as_table":
        return node.as_table(**config)
    if view.call == "head":
        return node.head(100, **config).as_table()
    if view.call == "as_source":
        return node.as_source().as_table(**config)
    raise ValueError(f"unknown call {view.call!r}")


def layout(job: PipelineJob, view: View, config: dict[str, Any]) -> Layout:
    """How the columns of ``view``'s table relate to the origin rows."""
    tag_keys, data_keys = job.nodes[view.node].keys()
    members: frozenset[str] = frozenset()
    if view.node == "grouped":
        # The key g is scalar; the other tag is dropped (ITL-1070); data are members.
        members = frozenset(data_keys)
    return Layout(
        tag_keys=tuple(tag_keys),
        data_keys=tuple(data_keys),
        members=members,
        content_hash_column=content_hash_column(config),
        # ITL-1059: a plain DerivedSource has null source info and no system tags by
        # design (for now).
        null_tokens=view.call == "as_source",
        no_system_tags=view.call == "as_source",
    )


def expected_rows(view: View, oracle: Any) -> list[Any]:
    """The rows ``view`` must hold, from the fixtures by each node's rule."""
    from .expectations import groups, pairs, rows

    if view.node == "scaled":
        return rows(oracle, "A")
    if view.node == "joined":
        return pairs(oracle, "A", "B")
    if view.node == "grouped":
        return groups(oracle, "A", ["g"])
    raise ValueError(f"unknown node {view.node!r}")


def natural_keys(scenario: str, input_order: str) -> list[int]:
    """The keys of A's rows in input order: the order a sync run computes them in."""
    return keys_a(scenario, input_order)


def stream_function_tokens(
    stream: Any, column: str = "y"
) -> dict[tuple[str, bytes], str]:
    """Each row's function token for ``column``, keyed by the row's record id.

    Read from the ``Data`` objects ``iter_data`` yields (``source_info()``), a path that
    does not go through table assembly: the reference a table's tokens must match.
    """
    return pairs_function_tokens(stream.iter_data(), column)


def pairs_function_tokens(
    pairs: Iterable[tuple[Any, Any]], column: str = "y"
) -> dict[tuple[str, bytes], str]:
    """The function token of ``column`` of each ``(tag, data)`` pair, keyed by record id."""
    tokens: dict[tuple[str, bytes], str] = {}
    for tag, data in pairs:
        system_tags = tag.as_dict(columns={"system_tags": True})
        (record_id,) = [
            v
            for k, v in system_tags.items()
            if k.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        ]
        token = data.source_info()[column]
        assert isinstance(token, str) and isinstance(record_id, bytes)
        tokens[(column, record_id)] = token
    return tokens
