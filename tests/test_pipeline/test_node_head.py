"""A job node's ``head()`` shows its in-memory or stored rows and never runs it (ITL-991).

A node is associated with its storage (developer, November 12, 2026): its preview is the
first rows of what it holds after a run, or of its stored table, and showing it never
computes. These tests run jobs on both orchestrators and both stores, and load saved jobs.
"""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.function_pod import FunctionPodStream
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.streams import HeadStatus
from orcapod.types import CacheMode, Schema

CALLS: list[int] = []

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


@function_pod(output_keys="y")
def triple(x: int) -> int:
    """Triple ``x``, counting every call."""
    CALLS.append(x)
    return 3 * x


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    """Clear the record of calls before each test."""
    CALLS.clear()


def _source(rows: int = 25) -> ArrowTableSource:
    """A source of ``rows`` rows with ``id`` and ``x``, both non-nullable ``int64``."""
    table = pa.table(
        {"id": list(range(rows)), "x": list(range(rows))},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="nums", label="nums")


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    """A fresh store of the given kind (``in_memory`` or ``delta``)."""
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _job(store: ArrowDatabaseProtocol) -> PipelineJob:
    """A job of ``triple`` over the source, then a select of ``y``."""
    job = PipelineJob(name="itl991", database=store)
    with job:
        tripled = triple.pod(_source(), label="triple")
        _ = SelectDataColumns(["y"])(tripled, label="sel")
    return job


def test_nodes_before_a_run_show_no_rows_and_compute_nothing() -> None:
    """Before a run, nodes show no rows and run neither the function nor the operator."""
    job = _job(InMemoryArrowDatabase())
    # The operator must not run: either entry point raises if it is called.
    with (
        mock.patch.object(
            SelectDataColumns, "process", side_effect=AssertionError("process ran")
        ),
        mock.patch.object(
            SelectDataColumns,
            "static_process",
            side_effect=AssertionError("static_process ran"),
        ),
    ):
        fn_head = job.nodes["triple"].head()
        op_head = job.nodes["sel"].head()
        _ = job.nodes["triple"]._repr_html_()
        op_html = job.nodes["sel"]._repr_html_()
    assert "could not read" not in op_html
    assert CALLS == []
    assert fn_head.status is HeadStatus.NOTHING_STORED
    assert fn_head.table.column_names == ["id", "y"]
    assert op_head.status is HeadStatus.NOT_COMPUTED
    assert "cache mode OFF" in op_head.header()


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_nodes_after_a_run_show_their_first_rows(
    orchestrator: str, store_kind: str, tmp_path: Path
) -> None:
    """Right after a run, nodes show their first rows from memory."""
    store = _store(store_kind, tmp_path)
    job = _job(store)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert len(CALLS) == 25
    for label in ("triple", "sel"):
        node = job.nodes[label]
        # Right after a run the rows are in memory: the store is not read at all.
        with mock.patch.object(
            type(store), "get_all_records", side_effect=AssertionError("store read")
        ):
            head = node.head()
        assert head.status is HeadStatus.ROWS, label
        assert head.total_rows == 25
        assert head.table.equals(node.as_table().slice(0, 10)), label
        assert "first 10 of 25" in node._repr_html_()
    assert len(CALLS) == 25


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
def test_a_new_job_over_a_filled_store_reads_stored_rows_without_computing(
    store_kind: str, tmp_path: Path
) -> None:
    """A new job over a filled store shows stored rows without computing."""
    store = _store(store_kind, tmp_path)
    _ = _job(store).run()
    CALLS.clear()

    node = _job(store).nodes["triple"]
    head = node.head(4)
    assert CALLS == []
    assert head.status is HeadStatus.ROWS
    # The whole stored slice is read, so the header states the exact count and
    # makes no claim of a bounded read.
    assert head.header() == "FunctionJobNode[triple] — first 4 of 25 stored rows"
    assert head.table.equals(node.as_table().slice(0, 4))


def test_a_loaded_job_shows_stored_rows_and_never_raises(tmp_path: Path) -> None:
    """A loaded job's nodes show stored rows, or say why there are none."""
    store = DeltaTableDatabase(base_path=tmp_path / "store")
    job = _job(store)
    _ = job.run()
    saved = tmp_path / "job.json"
    job.save(saved)
    CALLS.clear()

    loaded = PipelineJob.load(str(saved), database=store)
    fn_head = loaded.nodes["triple"].head()
    op_head = loaded.nodes["sel"].head()
    assert CALLS == []
    assert fn_head.status is HeadStatus.ROWS
    assert fn_head.header().endswith("first 10 of 25 stored rows")
    assert op_head.status is HeadStatus.UNAVAILABLE
    assert "cache mode OFF" in op_head.header()


def test_blueprint_nodes_are_unavailable() -> None:
    """Blueprint nodes have no rows to show."""
    job = _job(InMemoryArrowDatabase())
    for label in ("triple", "sel"):
        blueprint = job.nodes[label].as_node()
        head = blueprint.head()
        assert head.status is HeadStatus.UNAVAILABLE
        assert "pipeline blueprint" in blueprint._repr_html_()


def test_source_nodes() -> None:
    """Input slots and unbound source nodes show no rows; bound ones delegate."""
    slot = SourceNode(
        name="nums", tag_schema=Schema({"id": int}), data_schema=Schema({"x": int})
    )
    assert slot.head().status is HeadStatus.UNAVAILABLE
    assert "input slot" in slot._repr_html_()
    unbound = SourceJobNode(
        name="nums", tag_schema=Schema({"id": int}), data_schema=Schema({"x": int})
    )
    assert unbound.head().status is HeadStatus.UNAVAILABLE
    assert "no source bound" in unbound.head().header()
    source = _source()
    bound = SourceJobNode(name="nums", bound_source=source)
    assert bound.head().table.equals(source.head().table)
    assert bound.head().header() == "SourceJobNode[nums] — first 10 of 25 rows"


def _operator_node(
    cache_mode: CacheMode, store: InMemoryArrowDatabase
) -> OperatorJobNode:
    """An operator node selecting ``x`` from the source, on ``store``."""
    return OperatorJobNode(
        operator=SelectDataColumns(["x"]),
        input_streams=[_source()],
        pipeline_database=store,
        cache_mode=cache_mode,
    )


def test_operator_node_cache_modes() -> None:
    """Operator nodes show stored rows in REPLAY and none in LOG before a run."""
    store = InMemoryArrowDatabase()
    assert _operator_node(CacheMode.REPLAY, store).head().status is (
        HeadStatus.NOTHING_STORED
    )
    seed = _operator_node(CacheMode.LOG, store)
    _ = seed.execute(_source())

    replay = _operator_node(CacheMode.REPLAY, store)
    head = replay.head(3)
    assert head.status is HeadStatus.ROWS
    assert (
        head.header()
        == "OperatorJobNode[SelectDataColumns] — first 3 of 25 stored rows"
    )
    assert head.table.equals(replay.as_table().slice(0, 3))

    log = _operator_node(CacheMode.LOG, store)
    log_head = log.head()
    assert log_head.status is HeadStatus.NOT_COMPUTED
    assert "not replayed" in log_head.header()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_an_operator_node_with_an_empty_result_reads_as_computed(
    orchestrator: str,
) -> None:
    """After a run that produced no rows, an operator node is computed and empty."""
    from orcapod.core.operators import PolarsFilter

    job = PipelineJob(name="itl991_empty", database=InMemoryArrowDatabase())
    with job:
        _ = PolarsFilter(constraints={"x": -1})(_source(), label="none")
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    head = job.nodes["none"].head()
    assert head.status is HeadStatus.ROWS
    assert len(head) == 0
    assert head.table.column_names == ["id", "x"]


def test_an_operator_node_with_changed_inputs_shows_no_stale_rows() -> None:
    """After an input changed, an operator node's kept rows are not shown."""
    fn_stream = triple.pod(_source())
    assert isinstance(fn_stream, FunctionPodStream)
    node = OperatorJobNode(operator=SelectDataColumns(["y"]), input_streams=[fn_stream])
    _ = node.execute(fn_stream)
    assert node.head().status is HeadStatus.ROWS
    fn_stream.clear_cache()  # the input is modified after the run
    head = node.head()
    assert head.status is HeadStatus.NOT_COMPUTED
    assert "inputs changed" in head.header()


@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_head_keeps_the_tag_arrow_type_of_the_input(
    orchestrator: str, store_kind: str, tmp_path: Path
) -> None:
    """A node's head keeps an ``int8`` tag ``int8`` and is the prefix of ``as_table``.

    ``head`` and ``as_table`` share one table build (ITL-928 typing), in memory after a
    run and from the store in a new job.
    """
    table = pa.table(
        {"id": pa.array(range(12), pa.int8()), "x": pa.array(range(12), pa.int64())},
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )

    def build(store: ArrowDatabaseProtocol) -> PipelineJob:
        """Build a job of ``triple`` over the narrow-tag source on ``store``."""
        job = PipelineJob(name="itl991_types", database=store)
        with job:
            _ = triple.pod(
                ArrowTableSource(table, tag_columns=["id"], source_id="narrow"),
                label="triple",
            )
        return job

    store = _store(store_kind, tmp_path)
    job = build(store)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    for node in (job.nodes["triple"], build(store).nodes["triple"]):
        head = node.head(5, all_info=True)
        assert head.status is HeadStatus.ROWS
        assert head.table.schema.field("id").type == pa.int8()
        assert head.table.equals(node.as_table(all_info=True).slice(0, 5))
