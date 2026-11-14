"""An operator node's zero-row table has the Arrow types its rows have (ITL-912).

Before a run, after ``clear_cache()`` and after a run that produced no row, an operator node
reads as a zero-row table. That table must have exactly the columns, Arrow types and
nullability the node's table has after a run with rows: an ``int8`` stays ``int8``, a
non-nullable column stays non-nullable, a list keeps its item type and the system-tag
columns are present. The node gets those types without computing or reading a row: it
applies its operator to zero-row inputs, as the sync path does.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any
from unittest import mock
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import pyarrow as pa
import pytest

from orcapod import contexts, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.operators import (
    Batch,
    Join,
    PolarsFilter,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.core.sources import ArrowTableSource, DeltaTableSource
from orcapod.core.sources.cached_source import CachedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import typed_zero_row_table
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.side_effects import SideEffectPod
from orcapod.types import ColumnConfig

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

WITH_ROWS = ("g1", "g1", "g2")
NO_MATCH = ("g2", "g2", "g3")


def _path_column() -> pa.ChunkedArray[Any]:
    """Three ``Path`` values in the logical type the data context stores them with."""
    converter = contexts.resolve_context(None).type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"p": Path("/a")}, {"p": Path("/b")}, {"p": Path("/c")}],
        python_schema={"p": Path},
    )
    return table.column("p")


def _table(groups: tuple[str, ...]) -> pa.Table:
    """Non-nullable ``int8`` tag and data, a logical ``Path`` column and a list column."""
    path = _path_column()
    schema = pa.schema(
        [
            pa.field("id", pa.int8(), nullable=False),
            pa.field("grp", pa.large_string(), nullable=False),
            pa.field("x", pa.int8(), nullable=False),
            pa.field("p", path.type, nullable=False),
            pa.field(
                "l",
                pa.large_list(pa.field("item", pa.int8(), nullable=False)),
                nullable=False,
            ),
        ]
    )
    return pa.table(
        [
            pa.array([1, 2, 3], pa.int8()),
            pa.array(list(groups), pa.large_string()),
            pa.array([10, 20, 30], pa.int8()),
            path.combine_chunks(),
            pa.array([[1], [2, 3], []], schema.field("l").type),
        ],
        schema=schema,
    )


def _source(groups: tuple[str, ...] = WITH_ROWS) -> ArrowTableSource:
    """The source every pipeline here reads; same schema whatever ``groups`` is."""
    return ArrowTableSource(_table(groups), tag_columns=["id", "grp"], source_id="a")


def _g1(stream: StreamProtocol) -> StreamProtocol:
    """``stream`` behind a filter keeping group ``g1``."""
    return PolarsFilter(constraints={"grp": "g1"})(stream)


PIPELINES: dict[str, Callable[[StreamProtocol], object]] = {
    "filter": lambda a: PolarsFilter(constraints={"grp": "g1"})(a, label="op"),
    "select_after_filter": lambda a: SelectDataColumns(["x", "p", "l"])(
        _g1(a), label="op"
    ),
    "batch_after_filter": lambda a: Batch(batch_size=2)(_g1(a), label="op"),
}


def _job(pipeline: str, groups: tuple[str, ...] = WITH_ROWS) -> PipelineJob:
    """A job holding ``pipeline`` over a source with the given groups; not run."""
    job = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
    with job:
        _ = PIPELINES[pipeline](_source(groups))
    return job


def _op(job: PipelineJob) -> OperatorJobNode:
    """The job's operator node labelled ``op``."""
    node = job.nodes["op"]
    assert isinstance(node, OperatorJobNode)
    return node


def _schema_after_run(pipeline: str, orchestrator: str) -> pa.Schema:
    """The node's full schema after a run with rows: the reference."""
    job = _job(pipeline)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    table = _op(job).as_table(all_info=True)
    assert table.num_rows > 0
    return table.schema


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_not_yet_run_table_has_the_run_types(pipeline: str, orchestrator: str) -> None:
    """Before a run, the node's zero-row table has the types of its rows."""
    expected = _schema_after_run(pipeline, orchestrator)

    table = _op(_job(pipeline)).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_cleared_table_has_the_run_types(pipeline: str, orchestrator: str) -> None:
    """After ``clear_cache()``, the node's zero-row table has the run's types."""
    job = _job(pipeline)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    node = _op(job)
    expected = node.as_table(all_info=True).schema

    node.clear_cache()
    table = node.as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pipeline", list(PIPELINES))
def test_zero_row_run_table_has_the_run_types(pipeline: str, orchestrator: str) -> None:
    """A run that produces no row reads with the types of a run with rows.

    ``select_after_filter`` and ``batch_after_filter`` take their input from a node that
    produced no row, so they check the empty input each orchestrator builds as well.
    """
    expected = _schema_after_run(pipeline, orchestrator)
    job = _job(pipeline, NO_MATCH)
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())

    table = _op(job).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


class _RowsMustNotBeRead(ArrowTableSource):
    """A source whose rows must not be read: reading them raises."""

    def iter_data(self):
        raise AssertionError("the source's rows were read")

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        raise AssertionError("the source's rows were read")


def test_not_yet_run_table_reads_no_source_row() -> None:
    """The node types its zero-row table from the source's schema, not its rows."""
    expected = _schema_after_run("select_after_filter", "sync")
    job = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
    with job:
        _ = PIPELINES["select_after_filter"](
            _RowsMustNotBeRead(
                _table(WITH_ROWS), tag_columns=["id", "grp"], source_id="a"
            )
        )

    table = _op(job).as_table(all_info=True)

    assert table.num_rows == 0
    assert table.schema == expected


@dataclass(frozen=True)
class Point:
    """A user class a function returns."""

    a: int
    b: str


CALLS: list[int] = []


@function_pod(output_keys=("n", "maybe", "where", "items", "point"))
def describe_x(x: int) -> tuple[int, int | None, Path, list[int], Point]:
    """Outputs of each kind: plain, optional, logical, list and user class."""
    CALLS.append(x)
    return x, None, Path(f"/{x}"), [x, x], Point(x, str(x))


def _function_stream(groups: tuple[str, ...]) -> StreamProtocol:
    """``describe_x`` over the ``g1`` rows of a source with the given groups."""
    return describe_x.pod(_g1(_source(groups)))


def _cached_function_stream(groups: tuple[str, ...]) -> StreamProtocol:
    """``describe_x`` through a ``CachedFunctionPod`` over the ``g1`` rows."""
    # The decorated function's pod, as the hashing tests pass it (typed loosely).
    inner: Any = describe_x.pod
    pod = CachedFunctionPod(inner, result_database=InMemoryArrowDatabase())
    return pod(_g1(_source(groups)))


@pytest.mark.parametrize(
    "make", [_function_stream, _cached_function_stream], ids=["plain", "cached"]
)
def test_function_pod_stream_zero_rows_has_the_computed_types(
    make: Callable[[tuple[str, ...]], StreamProtocol],
) -> None:
    """A function pod over zero rows gives the table of its computed rows, no row."""
    expected = make(WITH_ROWS).as_table(all_info=True)
    assert expected.num_rows > 0
    CALLS.clear()

    table = make(NO_MATCH).as_table(all_info=True)

    assert CALLS == []
    assert table.num_rows == 0
    assert table.schema == expected.schema


def _function_job(groups: tuple[str, ...] = WITH_ROWS) -> PipelineJob:
    """A job: ``describe_x`` over the ``g1`` rows, then an operator labelled ``op``."""
    job = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
    with job:
        _ = SelectDataColumns(["n", "where", "items"])(
            describe_x.pod(_g1(_source(groups)), label="fn"), label="op"
        )
    return job


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_function_node_zero_row_tables_have_the_computed_types(
    orchestrator: str,
) -> None:
    """A function node, and an operator after it, type their zero-row tables.

    Before a run and after a run with no row, both tables equal the tables of a run
    with rows, and the function is never called for them.
    """
    ran = _function_job()
    _ = ran.run(orchestrator=ORCHESTRATORS[orchestrator]())
    expected_fn = ran.nodes["fn"].as_table(all_info=True).schema
    expected_op = _op(ran).as_table(all_info=True).schema
    CALLS.clear()

    not_run = _function_job()
    assert not_run.nodes["fn"].as_table(all_info=True).schema == expected_fn
    assert _op(not_run).as_table(all_info=True).schema == expected_op
    assert CALLS == []

    no_rows = _function_job(NO_MATCH)
    _ = no_rows.run(orchestrator=ORCHESTRATORS[orchestrator]())
    fn_table = no_rows.nodes["fn"].as_table(all_info=True)
    op_table = _op(no_rows).as_table(all_info=True)
    assert fn_table.num_rows == op_table.num_rows == 0
    assert fn_table.schema == expected_fn
    assert op_table.schema == expected_op
    assert CALLS == []


def _int8_table() -> pa.Table:
    """A small table of non-nullable ``int8`` tag and data columns."""
    return pa.table(
        [pa.array([1, 2], pa.int8()), pa.array([10, 20], pa.int8())],
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
            ]
        ),
    )


def _arrow_table_source(tmp_path: Path) -> ArrowTableSource:
    """An in-memory source; ``tmp_path`` is unused (every maker takes it)."""
    del tmp_path
    return ArrowTableSource(_int8_table(), tag_columns=["id"], source_id="kinds")


def _cached_source(tmp_path: Path) -> StreamProtocol:
    """A cached source over an in-memory source; its cache is never read here."""
    del tmp_path
    return CachedSource(
        ArrowTableSource(_int8_table(), tag_columns=["id"], source_id="kinds"),
        cache_database=InMemoryArrowDatabase(),
    )


def _delta_source(tmp_path: Path) -> StreamProtocol:
    """A Delta table source; its table is read when it is built."""
    from deltalake import write_deltalake

    write_deltalake(tmp_path / "delta", _int8_table(), mode="overwrite")
    return DeltaTableSource(tmp_path / "delta", tag_columns=["id"], source_id="kinds")


@pytest.mark.parametrize(
    "make_source",
    [_arrow_table_source, _cached_source, _delta_source],
    ids=["arrow_table", "cached", "delta"],
)
def test_operator_node_over_each_source_kind_types_its_zero_row_table(
    make_source: Callable[[Path], StreamProtocol], tmp_path: Path
) -> None:
    """Before a run, the node's zero-row table has the types of the run's rows."""

    def job() -> PipelineJob:
        """A job selecting ``x`` from a fresh source of this kind."""
        new = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
        with new:
            _ = SelectDataColumns(["x"])(make_source(tmp_path), label="op")
        return new

    table = _op(job()).as_table(all_info=True)
    ran = job()
    _ = ran.run(orchestrator=SyncPipelineOrchestrator())
    expected = _op(ran).as_table(all_info=True)

    assert expected.num_rows == 2
    assert table.num_rows == 0
    assert table.schema == expected.schema


def test_a_filter_after_a_cached_function_pod_has_no_meta_column_with_zero_rows() -> (
    None
):
    """A zero-row input carries no meta column, as a row passed downstream does not.

    A cached function pod's own table has ``__computed``; an operator after it never
    shows it, with rows or without.
    """

    def job(groups: tuple[str, ...]) -> PipelineJob:
        """A filter keeping every column, after a cached function pod."""
        new = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
        with new:
            _ = PolarsFilter(constraints={"grp": "g1"})(
                _cached_function_stream(groups), label="op"
            )
        return new

    ran = job(WITH_ROWS)
    _ = ran.run(orchestrator=SyncPipelineOrchestrator())
    in_job = _op(ran).as_table(all_info=True)
    not_run = _op(job(WITH_ROWS)).as_table(all_info=True)

    def plain(groups: tuple[str, ...]) -> pa.Table:
        """The same filter over the cached function pod's stream, outside a job."""
        stream = PolarsFilter(constraints={"grp": "g1"})(
            _cached_function_stream(groups)
        )
        return stream.as_table(all_info=True)

    plain_rows = plain(WITH_ROWS)
    plain_zero_rows = plain(NO_MATCH)

    assert in_job.num_rows > 0 and plain_rows.num_rows > 0
    assert not_run.num_rows == plain_zero_rows.num_rows == 0
    assert not_run.schema == in_job.schema
    assert plain_zero_rows.schema == plain_rows.schema
    assert not any(n.startswith("__") for n in not_run.column_names)


def test_a_zero_row_table_never_carries_a_meta_column() -> None:
    """The zero-row table of a cached function pod's stream has no ``__computed``.

    Non-public on purpose: the test is about ``typed_zero_row_table``'s contract (user
    tags, system tags, data, source info; no meta), which no public read shows today.
    """
    stream = _cached_function_stream(NO_MATCH)
    assert "__computed" in stream.as_table(all_info=True).column_names

    table = typed_zero_row_table(stream)

    assert table is not None
    assert not [name for name in table.column_names if name.startswith("__")]


def _side_effect_job() -> tuple[PipelineJob, list[int]]:
    """A job with a side-effect node ``delivered`` over ``x`` and ``p``; not run."""
    sent: list[int] = []

    def deliver(ctx: Any, x: int, p: Path) -> None:
        sent.append(x)

    job = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
    with job:
        selected = SelectDataColumns(["x", "p"])(_g1(_source()))
        _ = SideEffectPod(deliver)(selected, label="delivered")
    return job, sent


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_side_effect_node_not_run_reads_as_zero_rows_typed_as_its_rows(
    orchestrator: str,
) -> None:
    """A side-effect job node with no rows from a run has its input's zero-row table.

    It holds the rows of its last run (ITL-1091); without them it reads as zero rows
    with the types its rows have (ITL-912), and nothing is delivered to get them.
    """
    ran, _ = _side_effect_job()
    _ = ran.run(orchestrator=ORCHESTRATORS[orchestrator]())
    after_run = ran.nodes["delivered"].as_table(all_info=True)
    assert after_run.num_rows > 0

    job, sent = _side_effect_job()
    not_run = job.nodes["delivered"].as_table(all_info=True)

    assert not_run.num_rows == 0
    assert not_run.schema == after_run.schema
    assert sent == []


@function_pod(output_keys=("y", "q"))
def must_not_run(x: int, p: Path) -> tuple[int, Path]:
    """A function that must not be called: calling it raises."""
    raise AssertionError("the function was called")


def must_not_deliver(ctx: Any, x: int, p: Path) -> None:
    """A side effect that must not be delivered: delivering it raises."""
    raise AssertionError("the side effect was delivered")


def _every_kind_job(source: ArrowTableSource) -> PipelineJob:
    """A job not run: source ``a``, operator ``op``, function ``fn``, side effect ``se``."""
    job = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
    with job:
        selected = SelectDataColumns(["x", "p"])(_g1(source), label="op")
        _ = must_not_run.pod(selected, label="fn")
        _ = SideEffectPod(must_not_deliver)(selected, label="se")
    return job


HEAD_CONFIGS: dict[str, dict[str, Any] | None] = {
    "default": {},
    "system_tags": {"system_tags": True},
    "source": {"source": True},
    "context": {"context": True},
    "meta": {"meta": True},
    "content_hash": {"content_hash": True},
    "sort_by_tags": {"sort_by_tags": True},
    "all_info": None,
}


@pytest.mark.parametrize("config", list(HEAD_CONFIGS))
@pytest.mark.parametrize("node", ["op", "fn", "se"])
def test_head_of_a_node_not_run_has_the_types_of_its_table(
    node: str, config: str
) -> None:
    """A head with no rows has the columns and Arrow types ``as_table`` gives.

    Under every column configuration, for an operator, function and side-effect job
    node (a source node shows the source's rows): neither read calls the function or
    delivers the side effect.
    """
    columns = HEAD_CONFIGS[config]
    options: dict[str, Any] = (
        {"all_info": True} if columns is None else {"columns": columns}
    )
    stream = _every_kind_job(_source(NO_MATCH)).nodes[node]

    head = stream.head(**options).table
    table = stream.as_table(**options)

    assert head.num_rows == 0
    assert head.schema == table.schema


@pytest.mark.parametrize("node", ["op", "fn", "se"])
def test_head_of_a_node_not_run_reads_no_source_row(node: str) -> None:
    """A head with no rows takes its types from the source's schema, not its rows."""
    source = _RowsMustNotBeRead(
        _table(WITH_ROWS), tag_columns=["id", "grp"], source_id="a"
    )

    head = _every_kind_job(source).nodes[node].head(all_info=True).table

    assert head.num_rows == 0
    assert head.schema.field("id").type == pa.int8()
    assert any(name.startswith("_tag_") for name in head.column_names)


UDF_CALLS: list[int] = []


def _record_and_keep(batch: pl.Series) -> pl.Series:
    """A user predicate: records the batch length and keeps every row."""
    UDF_CALLS.append(len(batch))
    return pl.Series([True] * len(batch))


def _udf_filter(groups: tuple[str, ...]) -> StreamProtocol:
    """A standalone filter whose predicate is user code (a job refuses to hash it)."""
    return PolarsFilter([pl.col("x").map_batches(_record_and_keep, pl.Boolean)])(
        _g1(_source(groups))
    )


def test_head_of_a_filter_never_calls_its_predicate() -> None:
    """A filter's zero-row table is its input's: the predicate is not evaluated."""
    UDF_CALLS.clear()

    head = _udf_filter(WITH_ROWS).head(all_info=True).table

    assert UDF_CALLS == []
    assert head.num_rows == 0
    assert head.schema == _g1(_source(WITH_ROWS)).as_table(all_info=True).schema


def test_a_zero_row_run_of_a_filter_still_evaluates_its_predicate() -> None:
    """A run with no input row evaluates the predicate once, over an empty batch."""
    UDF_CALLS.clear()

    table = _udf_filter(NO_MATCH).as_table(all_info=True)

    assert UDF_CALLS == [0]
    assert table.num_rows == 0


def test_an_ill_typed_predicate_raises_at_run_not_at_head() -> None:
    """``head()`` shows the input's columns; the run, even with no row, raises."""
    stream = PolarsFilter([pl.col("grp") > 1])(_g1(_source(NO_MATCH)))

    head = stream.head()

    assert head.table.num_rows == 0
    with pytest.raises(pl.exceptions.PolarsError):
        _ = stream.as_table()


def test_head_of_a_join_whose_keys_disagree_raises_as_its_run_does() -> None:
    """Inputs that disagree on a key's Arrow type: ``head`` raises the run's error.

    The notebook preview (``_repr_html_``, the notebook's display hook) never raises: it
    shows the error.
    """
    wide = pa.table(
        {
            "id": pa.array([1, 2, 3], pa.int64()),
            "grp": pa.array(list(WITH_ROWS), pa.large_string()),
            "y": pa.array([4, 5, 6], pa.int8()),
        }
    )
    joined = Join()(
        _source(), ArrowTableSource(wide, tag_columns=["id", "grp"], source_id="b")
    )

    with pytest.raises(InputValidationError, match="'id'") as at_head:
        _ = joined.head()
    with pytest.raises(InputValidationError) as at_run:
        _ = joined.run()

    assert str(at_head.value) == str(at_run.value)
    assert "could not read" in joined._repr_html_()


@function_pod(output_keys="y", result_database=InMemoryArrowDatabase())
def cached_double(x: int) -> int:
    """A function pod with a result cache: its rows carry ``__computed``."""
    return 2 * x


def test_a_cached_data_function_has_its_meta_column_with_zero_rows() -> None:
    """With and without rows, a cached data function's table has the same columns."""
    with_rows = cached_double.pod(_g1(_source(WITH_ROWS))).as_table(all_info=True)
    no_rows = cached_double.pod(_g1(_source(NO_MATCH))).as_table(all_info=True)

    assert with_rows.num_rows > 0
    assert no_rows.num_rows == 0
    assert no_rows.schema == with_rows.schema


def _string_stream(groups: tuple[str, ...]) -> StreamProtocol:
    """A raw stream whose ``grp`` tag is a ``string`` (not ``large_string``) column."""
    return ArrowTableStream(
        pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int8()),
                "grp": pa.array(list(groups), pa.string()),
                "x": pa.array([10, 20, 30], pa.int8()),
            }
        ),
        tag_columns=["id", "grp"],
    )


def test_head_of_a_filter_has_the_large_types_its_run_gives() -> None:
    """A filter's run normalizes ``string`` to ``large_string``; its head does too."""
    keep_g1 = PolarsFilter(constraints={"grp": "g1"})

    head = keep_g1(_string_stream(WITH_ROWS)).head(all_info=True).table
    ran = keep_g1(_string_stream(WITH_ROWS)).as_table(all_info=True)

    assert head.schema.field("grp").type == pa.large_string()
    assert head.schema == ran.schema


def test_an_input_with_a_meta_data_column_gives_a_zero_row_table() -> None:
    """A raw stream lists a ``__`` column as data; an operator node over it still
    reads as zero rows before a run, without that column."""
    raw = ArrowTableStream(
        pa.table(
            {
                "id": pa.array([1, 2], pa.int8()),
                "x": pa.array([1, 2], pa.int8()),
                "__debug": pa.array(["a", "b"], pa.large_string()),
            }
        ),
        tag_columns=["id"],
    )
    job = PipelineJob(name="itl912", database=InMemoryArrowDatabase())
    with job:
        _ = SelectDataColumns(["x"])(raw, label="op")
    node = _op(job)

    head = node.head(all_info=True).table
    table = node.as_table(all_info=True)

    assert head.num_rows == table.num_rows == 0
    assert head.schema == table.schema
    assert table.schema.field("x").type == pa.int8()
    assert not any("__debug" in name for name in table.column_names)


# Both branches share the data columns, which SemiJoin ignores: not under test here.
@pytest.mark.filterwarnings("ignore::orcapod.errors.SemiJoinIgnoredColumnWarning")
def test_a_shared_upstream_is_derived_once_per_zero_row_table() -> None:
    """Ten stacked diamonds derive the source's zero-row table once, not 2**10 times."""
    stream: StreamProtocol = _source()
    for _ in range(10):
        stream = SemiJoin()(
            PolarsFilter(constraints={"grp": "g1"})(stream),
            PolarsFilter(constraints={"grp": "g2"})(stream),
        )
    original = ArrowTableSource._typed_zero_row_table
    # A spy on the non-public hook: the test is about how often it is asked.
    with mock.patch.object(
        ArrowTableSource, "_typed_zero_row_table", autospec=True, side_effect=original
    ) as spy:
        head = stream.head(all_info=True).table
        assert spy.call_count == 1
        _ = stream.head(all_info=True)
        # A new outermost call derives again: nothing is kept between calls.
        assert spy.call_count == 2
    assert head.schema.field("id").type == pa.int8()


class _PerThreadTable(ArrowTableSource):
    """A source whose zero-row table names the thread that asked for it."""

    def _typed_zero_row_table(self) -> pa.Table:
        """A zero-row table with an ``id`` column and a column named after the thread.

        Returns:
            The table, which tells which thread's derivation made it.
        """
        name = threading.current_thread().name
        return pa.table({"id": pa.array([], pa.int8()), name: pa.array([], pa.int8())})


class _AsksTwice(ArrowTableSource):
    """Asks for ``inner``'s zero-row table, waits for the other thread, asks again."""

    def __init__(self, inner: StreamProtocol, gate: threading.Barrier, **kwargs: Any):
        """Hold the stream to ask and the barrier the two threads meet at.

        Args:
            inner: The stream whose zero-row table each thread asks for twice.
            gate: Synchronizes the two threads: each waits there between its two
                asks, so one thread's first ask happens before the other's second.
            **kwargs: As for ``ArrowTableSource``.
        """
        super().__init__(**kwargs)
        self.inner = inner
        self.gate = gate
        # The column names each thread got from its two asks, by thread name.
        self.seen: dict[str, list[list[str]]] = {}

    def _typed_zero_row_table(self) -> pa.Table | None:
        """Ask ``inner`` twice around the barrier and record what came back.

        Returns:
            ``None`` on purpose: the answers are recorded in ``seen``.
        """
        name = threading.current_thread().name
        first = typed_zero_row_table(self.inner)
        self.gate.wait()
        second = typed_zero_row_table(self.inner)
        assert first is not None and second is not None
        self.seen[name] = [first.column_names, second.column_names]
        return None


def test_two_concurrent_zero_row_derivations_never_share_a_memo() -> None:
    """Each thread's outermost call has its own memo: no table crosses threads."""
    gate = threading.Barrier(2, timeout=10)
    inner = _PerThreadTable(_int8_table(), tag_columns=["id"], source_id="inner")
    outer = _AsksTwice(
        inner, gate, table=_int8_table(), tag_columns=["id"], source_id="outer"
    )
    errors: list[BaseException] = []

    def derive() -> None:
        try:
            _ = typed_zero_row_table(outer)
        except BaseException as error:  # noqa: BLE001 - reported by the test below
            errors.append(error)
            gate.abort()  # the other thread stops waiting at once

    threads = [threading.Thread(target=derive, name=name) for name in ("t1", "t2")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert outer.seen == {
        "t1": [["id", "t1"], ["id", "t1"]],
        "t2": [["id", "t2"], ["id", "t2"]],
    }
