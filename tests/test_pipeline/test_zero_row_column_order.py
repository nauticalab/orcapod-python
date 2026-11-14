"""A zero-row table has exactly its populated table's schema, column order included (ITL-1131).

ITL-912 made a table with no rows carry the Arrow types and nullability of the rows. This
sweep checks the rest of that rule for every operator and for a function pod: the zero-row
table has the populated table's columns in the populated table's order. It compares
``as_table(all_info=True)`` with ``pa.Schema.equals`` (names, order, types, nullability):

- a plain stream, ``op(stream)``, over an input with no rows, and its ``head()`` before it
  computes, against the same stream over an input with rows;
- a job node before a run, and after a sync or async run whose input had no rows, against the
  same job node after a run with rows by the same orchestrator.

Inputs carry one or two system-tag pairs (a source, or a join of two sources).

An operator node also has one populated order on every path (ITL-1133): the row order of a job
run (user tags, system tags, data, source info). A node run on its own (``node.run()`` over
streams outside a job, without a store, in ``LOG`` and read back in ``REPLAY``) keeps and
stores that order, so a store written by a job run and by a node run on its own holds one
order and accepts both.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import contexts, function_pod
from orcapod.core.operators import (
    Batch,
    DropColumns,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.core.nodes import OperatorJobNode
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.core_protocols.operator_pod import OperatorPodProtocol
from orcapod.types import CacheMode

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

WITH_ROWS = ("g1", "g1", "g2")
"""Groups of the sources' rows: the filter on ``g1`` keeps two rows."""
NO_ROWS = ("g2", "g2", "g3")
"""Groups of the sources' rows: the filter on ``g1`` keeps none."""


def _dict_column() -> "pa.Array[Any]":
    """A ``dict[str, int]`` column in the type the data context stores it with."""
    converter = contexts.resolve_context(None).type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"m": {"k": i}} for i in (1, 2, 3)], python_schema={"m": dict[str, int]}
    )
    return table.column("m").combine_chunks()


def _table(groups: tuple[str, ...], data: dict[str, "pa.Array[Any]"]) -> pa.Table:
    """Tags ``g`` and ``id`` and ``data``; every column non-nullable except ``n``."""
    table = pa.table(
        {
            "g": pa.array(list(groups), pa.large_string()),
            "id": pa.array([1, 2, 3], pa.int8()),
            **data,
        }
    )
    return table.cast(
        pa.schema(
            [pa.field(f.name, f.type, nullable=f.name == "n") for f in table.schema]
        )
    )


def _source(
    name: str, groups: tuple[str, ...], data: dict[str, "pa.Array[Any]"]
) -> StreamProtocol:
    """A source of ``data`` behind a filter keeping group ``g1``."""
    source = ArrowTableSource(
        _table(groups, data), tag_columns=["g", "id"], source_id=name
    )
    return PolarsFilter(constraints={"g": "g1"})(source)


def _main(groups: tuple[str, ...]) -> StreamProtocol:
    item = pa.field("item", pa.int8(), nullable=False)
    return _source(
        "a",
        groups,
        {
            "x": pa.array([10, 20, 30], pa.int8()),
            "l": pa.array([[1], [2, 3], [4]], pa.large_list(item)),
            "n": pa.array([1, None, 3], pa.int8()),
            "m": _dict_column(),
        },
    )


def _other(groups: tuple[str, ...]) -> StreamProtocol:
    return _source("b", groups, {"z": pa.array([7, 8, 9], pa.int16())})


def _colliding(groups: tuple[str, ...]) -> StreamProtocol:
    return _source("c", groups, {"x": pa.array([1, 2, 3], pa.int8())})


def _input(groups: tuple[str, ...], pairs: int) -> StreamProtocol:
    """The operator's input: one source, or a join of two (two system-tag pairs)."""
    stream = _main(groups)
    if pairs == 2:
        extra = _source("d", groups, {"w": pa.array([5, 6, 7], pa.int32())})
        stream = Join()(stream, extra)
    return stream


@function_pod(output_keys="y")
def _plus_one(x: int) -> int:
    return x + 1


Step = Callable[[StreamProtocol, tuple[str, ...]], StreamProtocol]

STEPS: dict[str, Step] = {
    "join": lambda a, g: Join()(a, _other(g), label="op"),
    "merge_join": lambda a, g: MergeJoin()(
        SelectDataColumns(["x"])(a), _colliding(g), label="op"
    ),
    "semijoin": lambda a, g: SemiJoin()(a, _other(g), label="op"),
    "batch": lambda a, g: Batch(batch_size=2)(a, label="op"),
    "group_by": lambda a, g: GroupBy(by=["g"])(a, label="op"),
    "select_data": lambda a, g: SelectDataColumns(["x", "l"])(a, label="op"),
    "drop_data": lambda a, g: DropDataColumns(["l"])(a, label="op"),
    "select_tags": lambda a, g: SelectTagColumns(["g"])(a, label="op"),
    "drop_tags": lambda a, g: DropTagColumns(["id"])(a, label="op"),
    "select_columns": lambda a, g: SelectColumns(["g", "x"])(a, label="op"),
    "drop_columns": lambda a, g: DropColumns(["id", "l"])(a, label="op"),
    "map_tags": lambda a, g: MapTags({"id": "ident"})(a, label="op"),
    "map_data": lambda a, g: MapData({"x": "xx"})(a, label="op"),
    "filter": lambda a, g: PolarsFilter(constraints={"id": 1})(a, label="op"),
    "drop_nulls": lambda a, g: DropNulls(["n"])(a, label="op"),
    "drop_nulls_all": lambda a, g: DropNulls()(a, label="op"),
    "index": lambda a, g: Index("l", 0)(a, label="op"),
    "index_out": lambda a, g: Index("l", 0, out="first")(a, label="op"),
    "pick": lambda a, g: Pick("m", "k")(a, label="op"),
    "pick_out": lambda a, g: Pick("m", "k", out="k")(a, label="op"),
    "function_pod": lambda a, g: _plus_one.pod(a, label="op"),
}


def _stream(step: str, pairs: int, groups: tuple[str, ...]) -> StreamProtocol:
    return STEPS[step](_input(groups, pairs), groups)


def _job(step: str, pairs: int, groups: tuple[str, ...]) -> PipelineJob:
    job = PipelineJob(name="itl1131", database=InMemoryArrowDatabase())
    with job:
        _ = STEPS[step](_input(groups, pairs), groups)
    return job


def _populated(table: pa.Table) -> pa.Schema:
    """The reference schema: a table that has rows."""
    assert table.num_rows > 0
    return table.schema


def _assert_same_schema(table: pa.Table, expected: pa.Schema) -> None:
    assert table.num_rows == 0
    assert table.schema.equals(expected), (
        f"zero-row columns {table.schema.names}\n"
        f"populated columns {expected.names}\n"
        f"zero-row schema:\n{table.schema}\npopulated schema:\n{expected}"
    )


PAIRS = [1, 2]


@pytest.mark.parametrize("pairs", PAIRS)
@pytest.mark.parametrize("step", list(STEPS))
class TestPlainStream:
    """``op(stream)`` with no rows has the schema of ``op(stream)`` with rows."""

    def test_over_an_input_with_no_rows(self, step: str, pairs: int) -> None:
        expected = _populated(_stream(step, pairs, WITH_ROWS).as_table(all_info=True))
        table = _stream(step, pairs, NO_ROWS).as_table(all_info=True)
        _assert_same_schema(table, expected)

    def test_head_before_it_computes(self, step: str, pairs: int) -> None:
        expected = _populated(_stream(step, pairs, WITH_ROWS).as_table(all_info=True))
        view = _stream(step, pairs, WITH_ROWS).head(all_info=True)
        _assert_same_schema(view.as_table(), expected)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("pairs", PAIRS)
@pytest.mark.parametrize("step", list(STEPS))
class TestJobNode:
    """A job node with no rows has the schema of the same node after a run with rows."""

    def _expected(self, step: str, pairs: int, orchestrator: str) -> pa.Schema:
        job = _job(step, pairs, WITH_ROWS)
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        return _populated(job.nodes["op"].as_table(all_info=True))

    def test_before_a_run(self, step: str, pairs: int, orchestrator: str) -> None:
        expected = self._expected(step, pairs, orchestrator)
        job = _job(step, pairs, WITH_ROWS)
        _assert_same_schema(job.nodes["op"].as_table(all_info=True), expected)

    def test_after_a_run_whose_input_had_no_rows(
        self, step: str, pairs: int, orchestrator: str
    ) -> None:
        expected = self._expected(step, pairs, orchestrator)
        job = _job(step, pairs, NO_ROWS)
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        _assert_same_schema(job.nodes["op"].as_table(all_info=True), expected)

    @pytest.mark.parametrize("groups", [WITH_ROWS, NO_ROWS], ids=["rows", "no_rows"])
    def test_load_only_run(
        self, step: str, pairs: int, orchestrator: str, groups: tuple[str, ...]
    ) -> None:
        """A load-only run (ITL-1095) gives the node the schema of a full run."""
        expected = self._expected(step, pairs, orchestrator)
        job = _job(step, pairs, groups)
        _ = job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
        assert job.nodes["op"].as_table(all_info=True).schema.equals(expected)

    @pytest.mark.parametrize("groups", [WITH_ROWS, NO_ROWS], ids=["rows", "no_rows"])
    def test_node_run_runs_its_upstreams(
        self, step: str, pairs: int, orchestrator: str, groups: tuple[str, ...]
    ) -> None:
        """``node.run()`` on a job node, which runs the nodes it depends on first."""
        expected = self._expected(step, pairs, orchestrator)
        job = _job(step, pairs, groups)
        _ = job.nodes["op"].run()
        assert job.nodes["op"].as_table(all_info=True).schema.equals(expected)


OPERATOR_STEPS = [step for step in STEPS if step != "function_pod"]


def _node(
    step: str, pairs: int, groups: tuple[str, ...], **kwargs: Any
) -> OperatorJobNode:
    """An operator node of ``step`` over the plain streams the step reads, outside a job."""
    stream = _stream(step, pairs, groups)
    operator = cast(OperatorPodProtocol, stream.producer)
    return OperatorJobNode(operator, stream.upstreams, **kwargs)


@pytest.mark.parametrize("pairs", PAIRS)
@pytest.mark.parametrize("step", OPERATOR_STEPS)
class TestNodeRunOnItsOwn:
    """A node run on its own has the schema a job run gives it, on every path (ITL-1133)."""

    def _expected(self, step: str, pairs: int) -> pa.Schema:
        job = _job(step, pairs, WITH_ROWS)
        _ = job.run(orchestrator=SyncPipelineOrchestrator())
        return _populated(job.nodes["op"].as_table(all_info=True))

    def test_run_without_a_store(self, step: str, pairs: int) -> None:
        node = _node(step, pairs, WITH_ROWS)
        _ = node.run()
        table = node.as_table(all_info=True)
        assert table.num_rows > 0
        assert table.schema.equals(self._expected(step, pairs))

    def test_before_a_run(self, step: str, pairs: int) -> None:
        node = _node(step, pairs, WITH_ROWS)
        _assert_same_schema(node.as_table(all_info=True), self._expected(step, pairs))

    def test_run_whose_input_had_no_rows(self, step: str, pairs: int) -> None:
        node = _node(step, pairs, NO_ROWS)
        _ = node.run()
        _assert_same_schema(node.as_table(all_info=True), self._expected(step, pairs))

    def test_log_run_and_replay(self, step: str, pairs: int) -> None:
        expected = self._expected(step, pairs)
        store = InMemoryArrowDatabase()
        node = _node(
            step, pairs, WITH_ROWS, pipeline_database=store, cache_mode=CacheMode.LOG
        )
        _ = node.run()
        assert node.as_table(all_info=True).schema.equals(expected)
        replay = _node(
            step, pairs, WITH_ROWS, pipeline_database=store, cache_mode=CacheMode.REPLAY
        )
        # A REPLAY run over lazy inputs that it does not compute reads as stale once
        # they compute (seen on main e4b4319c, reported); computed first, they do not.
        for upstream in replay.upstreams:
            _ = upstream.as_table()
        # A node shows its current run (ITL-1095): a REPLAY run loads the stored rows.
        _ = replay.run()
        table = replay.as_table(all_info=True)
        assert table.num_rows > 0
        assert table.schema.equals(expected)


def _join_job(ids: list[int]) -> PipelineJob:
    """A job joining two sources whose rows have ``ids``; same source ids every time."""

    def source(name: str, column: str) -> ArrowTableSource:
        table = pa.table(
            {"id": pa.array(ids, pa.int64()), column: pa.array(ids, pa.int64())}
        )
        return ArrowTableSource(table, tag_columns=["id"], source_id=name)

    job = PipelineJob(name="itl1133", database=InMemoryArrowDatabase())
    with job:
        _ = Join()(source("a", "x"), source("b", "z"), label="joined")
    return job


STORES = ["in_memory", "delta", "delta_flushed"]


@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("order", [("job", "node"), ("node", "job")])
def test_a_job_run_and_a_node_run_share_one_stored_table(
    store_kind: str, order: tuple[str, str], tmp_path: Any
) -> None:
    """A job run and a node run on its own store one column order in one table (ITL-1133).

    Each run has new rows under the same source ids, so both write the same node table.
    """
    store = (
        InMemoryArrowDatabase()
        if store_kind == "in_memory"
        else DeltaTableDatabase(tmp_path)
    )
    schemas = []
    nodes = []
    for how, ids in zip(order, ([1, 2], [3, 4]), strict=True):
        job = _join_job(ids)
        node = job.nodes["joined"]
        nodes.append(node)
        node.attach_databases(pipeline_database=store, cache_mode=CacheMode.LOG)
        if how == "job":
            _ = job.run(orchestrator=SyncPipelineOrchestrator())
        else:
            _ = node.run()
        schemas.append(node.as_table(all_info=True).schema)
        if store_kind == "delta_flushed":
            store.flush()
    assert schemas[0].equals(schemas[1])
    stored = nodes[-1].get_all_records(all_info=True)
    assert stored is not None
    assert stored.num_rows == 4


def _write_in_operator_order(source_dir: Any, target_dir: Any) -> tuple[str, ...]:
    """Copy the one operator cache table of ``source_dir`` as the old writer wrote it.

    Test-only stand-in for an operator cache written before ITL-1133 by a node run on its
    own: the same stored rows, with the user columns in a join's own order (user tags,
    data, system tags, source info) and the bookkeeping (``__``) columns where they were.
    """
    source = DeltaTableDatabase(source_dir)
    (path,) = [p for p in source.list_table_paths() if p[-1] == "odb_v1"]
    rows = source.get_all_records(
        path, record_id_column=OperatorJobNode.HASH_COLUMN_NAME
    )
    assert rows is not None
    names = rows.column_names
    user = [n for n in names if not n.startswith("__")]
    system_tags = [n for n in user if n.startswith("_tag")]
    source_info = [n for n in user if n.startswith("_source_")]
    tags = [n for n in user if n == "id"]
    data = [n for n in user if n not in {*tags, *system_tags, *source_info}]
    old_order = iter([*tags, *data, *system_tags, *source_info])
    reordered = [n if n.startswith("__") else next(old_order) for n in names]
    target = DeltaTableDatabase(target_dir)
    target.add_records(
        path,
        rows.select(reordered),
        record_id_column=OperatorJobNode.HASH_COLUMN_NAME,
    )
    target.flush()
    return path


class TestOperatorCacheWrittenInOperatorOrder:
    """A stored operator cache in a join's own order reads in row order (ITL-1133, B)."""

    def _expected(self) -> pa.Schema:
        job = _join_job([1, 2])
        _ = job.run(orchestrator=SyncPipelineOrchestrator())
        return _populated(job.nodes["joined"].as_table(all_info=True))

    def _old_table(self, tmp_path: Any) -> Any:
        written = tmp_path / "written"
        store = DeltaTableDatabase(written)
        job = _join_job([1, 2])
        node = job.nodes["joined"]
        node.attach_databases(pipeline_database=store, cache_mode=CacheMode.LOG)
        _ = job.run(orchestrator=SyncPipelineOrchestrator())
        store.flush()
        old = tmp_path / "old"
        _ = _write_in_operator_order(written, old)
        return old

    def _replay(self, store: DeltaTableDatabase, ids: list[int]) -> OperatorJobNode:
        """A REPLAY node over ``store`` after its run, which loads the stored rows."""
        node = _join_job(ids).nodes["joined"]
        node.attach_databases(pipeline_database=store, cache_mode=CacheMode.REPLAY)
        _ = node.run()  # a node shows its current run (ITL-1095)
        return node

    def test_reads_in_row_order(self, tmp_path: Any) -> None:
        old = self._old_table(tmp_path)
        table = self._replay(DeltaTableDatabase(old), [1, 2]).as_table(all_info=True)
        assert table.num_rows == 2
        assert table.schema.equals(self._expected())

    def test_accepts_a_row_order_append(self, tmp_path: Any) -> None:
        old = self._old_table(tmp_path)
        store = DeltaTableDatabase(old)
        job = _join_job([3, 4])
        node = job.nodes["joined"]
        node.attach_databases(pipeline_database=store, cache_mode=CacheMode.LOG)
        _ = node.run()
        store.flush()
        stored = node.get_all_records(all_info=True)
        assert stored is not None
        assert stored.num_rows == 4
        # Both runs bind the same source ids, so the node's stored rows are all four.
        table = self._replay(DeltaTableDatabase(old), [3, 4]).as_table(all_info=True)
        assert table.num_rows == 4
        assert table.schema.equals(self._expected())


COLUMN_CONFIGS: dict[str, dict[str, Any]] = {
    "default": {},
    "system_tags": {"columns": {"system_tags": True}},
    "source": {"columns": {"source": True}},
    "context": {"columns": {"context": True}},
    "content_hash": {"columns": {"content_hash": True}},
    "meta": {"columns": {"meta": True}},
    "all_info": {"all_info": True},
}


def _schema_items(
    schemas: tuple[Any, Any],
) -> tuple[list[tuple[str, Any]], list[tuple[str, Any]]]:
    """The tag and data schemas as ordered (name, Python type) lists."""
    tag_schema, data_schema = schemas
    return list(tag_schema.items()), list(data_schema.items())


def _computed(step: str, pairs: int) -> Any:
    """The operator's computed result over the step's inputs.

    A schema prediction is verified against ``static_process(...).output_schema(...)``
    (CLAUDE.md's standing exception to the public-interface rule).
    """
    stream = _stream(step, pairs, WITH_ROWS)
    return stream.producer.static_process(*stream.upstreams)  # pyright: ignore[reportOptionalMemberAccess, reportAttributeAccessIssue]  # an operator's stream has a producer with static_process


_NULLABLE_MEMBER = pytest.mark.xfail(
    strict=True,
    reason=(
        "Batch and GroupBy predict list[int | None] for the nullable column n; their "
        "computed table's large_list<item: int8> (nullable items) reads as list[int] "
        "(ITL-1136: the Arrow-to-Python reading drops list item nullability)"
    ),
)
PREDICTION_STEPS = [
    pytest.param(step, marks=_NULLABLE_MEMBER)
    if step in ("batch", "group_by")
    else step
    for step in OPERATOR_STEPS
]


@pytest.mark.parametrize("config", list(COLUMN_CONFIGS))
@pytest.mark.parametrize("pairs", PAIRS)
@pytest.mark.parametrize("step", PREDICTION_STEPS)
class TestOutputSchemaPrediction:
    """Every operator predicts the schema of its computed result (ITL-1135)."""

    def test_plain_stream(self, step: str, pairs: int, config: str) -> None:
        kwargs = COLUMN_CONFIGS[config]
        predicted = _stream(step, pairs, WITH_ROWS).output_schema(**kwargs)
        computed = _computed(step, pairs).output_schema(**kwargs)
        assert _schema_items(predicted) == _schema_items(computed)

    def test_job_node(self, step: str, pairs: int, config: str) -> None:
        kwargs = COLUMN_CONFIGS[config]
        job = _job(step, pairs, WITH_ROWS)
        _ = job.run(orchestrator=SyncPipelineOrchestrator())
        predicted = job.nodes["op"].output_schema(**kwargs)
        computed = _computed(step, pairs).output_schema(**kwargs)
        assert _schema_items(predicted) == _schema_items(computed)
