"""After a job run, an operator node reads back the same rows on both orchestrators (ITL-896).

``job.nodes[label]`` is a stream: ``as_table()`` and ``iter_data()`` must return the rows the
operator produced, whether the job ran on ``SyncPipelineOrchestrator`` or
``AsyncPipelineOrchestrator``, with either store and either ``materialize_results`` setting.
An operator node in a job runs with the default ``cache_mode=OFF`` and stores nothing, so
``get_all_records()`` returns ``None`` on both.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    DropNulls,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod import function_pod
from orcapod.channels import Channel
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.types import CacheMode

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


def _source_a() -> ArrowTableSource:
    """Two tags and two data columns: non-nullable ``int8`` and a nullable ``string``."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int8()),
            "grp": pa.array(["g1", "g1", "g2"], type=pa.large_string()),
            "x": pa.array([10, 20, 30], type=pa.int8()),
            "s": pa.array(["p", None, "r"], type=pa.string()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("grp", pa.large_string(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
                pa.field("s", pa.string(), nullable=True),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id="a")


def _source_b() -> ArrowTableSource:
    """Shares the ``id`` tag with ``_source_a`` and adds an ``int16`` column for joins."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int8()),
            "z": pa.array([5, 6, 7], type=pa.int16()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("z", pa.int16(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"], source_id="b")


def _source_c() -> ArrowTableSource:
    """Same tags and ``x`` column as ``_source_a``, so ``MergeJoin`` merges ``x``."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int8()),
            "grp": pa.array(["g1", "g1", "g2"], type=pa.large_string()),
            "x": pa.array([11, 21, 31], type=pa.int8()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int8(), nullable=False),
                pa.field("grp", pa.large_string(), nullable=False),
                pa.field("x", pa.int8(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id="c")


@function_pod(output_keys="x2")
def double_x(x: int) -> int:
    """Double ``x``; the function pod downstream of an operator node."""
    return x * 2


@dataclass(frozen=True)
class Case:
    """An operator applied inside a job; the operator node under test is labelled ``op``."""

    name: str
    build: Callable[[StreamProtocol, StreamProtocol, StreamProtocol], object]
    expected_rows: int


CASES = [
    Case(
        "select_data_columns",
        lambda a, b, c: SelectDataColumns(["x"])(a, label="op"),
        3,
    ),
    Case("drop_data_columns", lambda a, b, c: DropDataColumns(["s"])(a, label="op"), 3),
    Case("drop_nulls_named", lambda a, b, c: DropNulls(["s"])(a, label="op"), 2),
    Case("drop_nulls_all", lambda a, b, c: DropNulls()(a, label="op"), 2),
    Case(
        "select_tag_columns",
        lambda a, b, c: SelectTagColumns(["id"])(a, label="op"),
        3,
    ),
    Case("drop_tag_columns", lambda a, b, c: DropTagColumns(["grp"])(a, label="op"), 3),
    Case("map_data", lambda a, b, c: MapData({"x": "w"})(a, label="op"), 3),
    Case("map_tags", lambda a, b, c: MapTags({"grp": "group"})(a, label="op"), 3),
    Case(
        "polars_filter",
        lambda a, b, c: PolarsFilter(constraints={"grp": "g1"})(a, label="op"),
        2,
    ),
    Case("join", lambda a, b, c: Join()(a, b, label="op"), 3),
    Case("merge_join", lambda a, b, c: MergeJoin()(a, c, label="op"), 3),
    Case("semi_join", lambda a, b, c: SemiJoin()(a, b, label="op"), 3),
    Case("batch", lambda a, b, c: Batch(batch_size=2)(a, label="op"), 2),
    Case("group_by", lambda a, b, c: GroupBy(by=["grp"])(a, label="op"), 2),
    Case(
        "operator_feeding_function_pod",
        lambda a, b, c: double_x.pod(
            SelectDataColumns(["x"])(a, label="op"), label="double"
        ),
        3,
    ),
]


def _make_store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    """A fresh store of the given kind (``in_memory`` or ``delta``)."""
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _run_job(
    case: Case,
    orchestrator: str,
    store_kind: str,
    tmp_path: Path,
    materialize_results: bool = True,
) -> PipelineJob:
    """Build ``case`` in a new job on a fresh store and run it on ``orchestrator``."""
    job = PipelineJob(
        name="itl896", store=_make_store(store_kind, tmp_path / orchestrator)
    )
    with job:
        _ = case.build(_source_a(), _source_b(), _source_c())
    _ = job.run(
        orchestrator=ORCHESTRATORS[orchestrator](),
        materialize_results=materialize_results,
    )
    return job


def _op_node(job: PipelineJob) -> OperatorJobNode:
    """The job's operator node labelled ``op``."""
    node: object = job.nodes["op"]
    assert isinstance(node, OperatorJobNode)
    return node


def _table_rows(table: pa.Table) -> list[dict[str, object]]:
    """Rows of ``table`` in a canonical order, so arrival order does not matter."""
    return sorted(table.to_pylist(), key=repr)


def _row_hashes(node: OperatorJobNode) -> list[tuple[str, str]]:
    """Sorted (tag, data) content hashes of the node's rows from ``iter_data()``."""
    return sorted(
        (tag.content_hash().to_string(), data.content_hash().to_string())
        for tag, data in node.iter_data()
    )


@pytest.mark.parametrize("materialize_results", [True, False])
@pytest.mark.parametrize("store_kind", ["in_memory", "delta"])
@pytest.mark.parametrize("case", CASES, ids=[case.name for case in CASES])
def test_operator_node_reads_back_same_rows_after_async_run(
    case: Case, store_kind: str, materialize_results: bool, tmp_path: Path
) -> None:
    """An async run reads back exactly the rows, types and system tags of a sync run."""
    sync_node = _op_node(
        _run_job(case, "sync", store_kind, tmp_path, materialize_results)
    )
    async_node = _op_node(
        _run_job(case, "async", store_kind, tmp_path, materialize_results)
    )

    sync_table = sync_node.as_table(all_info=True)
    async_table = async_node.as_table(all_info=True)

    assert sync_table.num_rows == case.expected_rows
    assert async_table.num_rows == case.expected_rows
    # Types, nullability and column order, including system tags and source info.
    assert async_table.schema == sync_table.schema
    assert _table_rows(async_table) == _table_rows(sync_table)
    assert _table_rows(async_node.as_table()) == _table_rows(sync_node.as_table())

    assert len(list(async_node.iter_data())) == case.expected_rows
    assert _row_hashes(async_node) == _row_hashes(sync_node)


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_operator_node_stores_nothing_so_get_all_records_is_none(
    orchestrator: str, tmp_path: Path
) -> None:
    """With the default ``cache_mode=OFF`` nothing is stored, on either orchestrator."""
    node = _op_node(_run_job(CASES[0], orchestrator, "in_memory", tmp_path))

    assert node.as_table().num_rows == 3
    assert node.get_all_records() is None


@pytest.mark.parametrize(
    ("cache_mode", "with_database"),
    [
        pytest.param(CacheMode.OFF, True, id="off"),
        pytest.param(CacheMode.LOG, True, id="log_with_database"),
        pytest.param(CacheMode.LOG, False, id="log_without_database"),
        pytest.param(CacheMode.REPLAY, True, id="replay"),
    ],
)
def test_operator_node_cache_modes_keep_sync_async_readback_equivalent(
    cache_mode: CacheMode, with_database: bool
) -> None:
    """Cache mode controls persistence; both execution paths retain emitted rows."""
    source = _source_a()

    def make_node() -> OperatorJobNode:
        """Build a node, seeding a populated cache for REPLAY."""
        store = InMemoryArrowDatabase() if with_database else None
        if cache_mode == CacheMode.REPLAY:
            seed = OperatorJobNode(
                operator=SelectDataColumns(["x"]),
                input_streams=[source],
                pipeline_database=store,
                cache_mode=CacheMode.LOG,
            )
            _ = seed.execute(source)
        return OperatorJobNode(
            operator=SelectDataColumns(["x"]),
            input_streams=[source],
            pipeline_database=store,
            cache_mode=cache_mode,
        )

    sync_node, async_node = make_node(), make_node()
    sync_rows = sync_node.execute(source)

    async def run() -> list[tuple[TagProtocol, DataProtocol]]:
        """Drive the node's public channel interface and collect forwarded rows."""
        inputs: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
        outputs: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
        # REPLAY must emit the stored rows even with no upstream rows to compute.
        if cache_mode != CacheMode.REPLAY:
            for item in source.iter_data():
                await inputs.writer.send(item)
        await inputs.writer.close()
        await async_node.async_execute([inputs.reader], outputs.writer)
        return await outputs.reader.collect()

    async_rows = asyncio.run(run())
    assert len(sync_rows) == len(async_rows) == 3
    sync_table = sync_node.as_table(all_info=True)
    async_table = async_node.as_table(all_info=True)
    assert async_table.schema == sync_table.schema
    assert _table_rows(async_table) == _table_rows(sync_table)
    assert _row_hashes(async_node) == _row_hashes(sync_node)
    for node in (sync_node, async_node):
        records = node.get_all_records()
        if with_database and cache_mode in (CacheMode.LOG, CacheMode.REPLAY):
            assert records is not None
            assert records.num_rows == 3
        else:
            assert records is None


@pytest.mark.parametrize("cache_mode", [None, CacheMode.OFF, CacheMode.LOG])
def test_operator_failure_preserves_exception_type_and_message(
    cache_mode: CacheMode | None,
) -> None:
    """A sole operator error reaches callers unchanged on either execution path."""
    source = ArrowTableSource(
        pa.table(
            {"x": pa.array([[1]], type=pa.large_list(pa.int8()))},
            schema=pa.schema([pa.field("x", pa.large_list(pa.int8()), nullable=False)]),
        ),
        source_id="index_failure",
    )

    def make_node() -> OperatorJobNode:
        """Create a fresh Index node with the requested persistence mode."""
        return OperatorJobNode(
            operator=Index("x", 5, fail_on_miss=True),
            input_streams=[source],
            pipeline_database=None if cache_mode is None else InMemoryArrowDatabase(),
            cache_mode=cache_mode or CacheMode.OFF,
        )

    with pytest.raises(RuntimeError) as sync_error:
        _ = make_node().execute(source)

    async def run() -> None:
        """Send the source through the node's public async execution interface."""
        inputs: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
        outputs: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
        for item in source.iter_data():
            await inputs.writer.send(item)
        await inputs.writer.close()
        await make_node().async_execute([inputs.reader], outputs.writer)

    with pytest.raises(RuntimeError) as async_error:
        asyncio.run(run())
    assert type(async_error.value) is type(sync_error.value)
    assert str(async_error.value) == str(sync_error.value)


def test_concurrent_operator_and_forwarding_failures_stay_grouped() -> None:
    """Unwrapping a sole failure must never discard concurrent channel failures."""
    source = ArrowTableSource(
        pa.table(
            {"x": pa.array([[1], []], type=pa.large_list(pa.int8()))},
            schema=pa.schema([pa.field("x", pa.large_list(pa.int8()), nullable=False)]),
        ),
        source_id="concurrent_failure",
    )
    node = OperatorJobNode(
        operator=Index("x", 0, fail_on_miss=True), input_streams=[source]
    )

    class FailingWriter:
        """An output sink that fails when the first valid row is forwarded."""

        async def send(self, item: tuple[TagProtocol, DataProtocol]) -> None:
            """Reject the forwarded row."""
            raise LookupError("output unavailable")

        async def close(self) -> None:
            """Allow the node to close its output even after a failure."""

    async def run() -> None:
        """Queue a valid row followed by an invalid row before starting both tasks."""
        inputs: Channel[tuple[TagProtocol, DataProtocol]] = Channel(buffer_size=16)
        for item in source.iter_data():
            await inputs.writer.send(item)
        await inputs.writer.close()
        await node.async_execute([inputs.reader], FailingWriter())

    with pytest.raises(ExceptionGroup) as info:
        asyncio.run(run())
    assert {type(error) for error in info.value.exceptions} == {
        RuntimeError,
        LookupError,
    }
    assert len(info.value.exceptions) == 2


NO_MATCH = Case(
    "polars_filter_no_match",
    lambda a, b, c: PolarsFilter(constraints={"grp": "none"})(a, label="op"),
    0,
)

MERGE_JOIN_AFTER_OPERATOR = Case(
    "merge_join_after_operator",
    lambda a, b, c: MergeJoin()(SelectDataColumns(["x"])(a), c, label="op"),
    3,
)


def _no_match(stream: StreamProtocol) -> StreamProtocol:
    """``stream`` behind a filter that matches no row."""
    return PolarsFilter(constraints={"grp": "none"})(stream)


AFTER_NO_ROWS_CASES = [
    Case(
        "group_by_after_no_rows",
        lambda a, b, c: GroupBy(by=["grp"])(_no_match(a), label="op"),
        0,
    ),
    Case(
        "polars_filter_after_no_rows",
        lambda a, b, c: PolarsFilter(constraints={"grp": "g1"})(
            _no_match(a), label="op"
        ),
        0,
    ),
    Case(
        "batch_after_no_rows",
        lambda a, b, c: Batch(batch_size=2)(_no_match(a), label="op"),
        0,
    ),
    Case(
        "batch_all_after_no_rows", lambda a, b, c: Batch()(_no_match(a), label="op"), 0
    ),
    # Both inputs empty: a zero-row operator node reads back types built from Python
    # types (ITL-912), so one empty and one non-empty input disagree on ``id`` on sync.
    Case(
        "merge_join_after_no_rows",
        lambda a, b, c: MergeJoin()(_no_match(a), _no_match(c), label="op"),
        0,
    ),
    Case(
        "function_pod_after_no_rows",
        lambda a, b, c: double_x.pod(_no_match(c), label="op"),
        0,
    ),
]


@pytest.mark.parametrize(
    "case", AFTER_NO_ROWS_CASES, ids=[case.name for case in AFTER_NO_ROWS_CASES]
)
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_node_after_an_input_with_no_rows_completes_the_run(
    case: Case, orchestrator: str, tmp_path: Path
) -> None:
    """A filter that matches no row upstream: the job completes with zero rows (ITL-865).

    On the async path a barrier operator whose input delivered no rows emits none, as the
    sync path returns an empty stream; it does not raise.
    """
    job = _run_job(case, orchestrator, "in_memory", tmp_path)
    node: object = job.nodes["op"]
    assert isinstance(node, StreamProtocol)
    assert list(node.iter_data()) == []
    assert node.as_table().num_rows == 0


def _sync_and_async_nodes(
    case: Case, tmp_path: Path
) -> tuple[OperatorJobNode, OperatorJobNode]:
    """The ``op`` node after a sync run and after an async run of ``case``."""
    sync_node = _op_node(_run_job(case, "sync", "in_memory", tmp_path))
    async_node = _op_node(_run_job(case, "async", "in_memory", tmp_path))
    return sync_node, async_node


def test_operator_node_with_no_rows_reads_back_empty_after_async_run(
    tmp_path: Path,
) -> None:
    """A run with no rows reads back a zero-row table with the sync path's columns."""
    sync_node, async_node = _sync_and_async_nodes(NO_MATCH, tmp_path)

    sync_table = sync_node.as_table()
    async_table = async_node.as_table()

    assert sync_table.num_rows == 0
    assert async_table.num_rows == 0
    assert async_table.column_names == sync_table.column_names
    assert list(async_node.iter_data()) == []


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ITL-858/ITL-912: no row reaches the async node, so it reads back the table "
        "built from the Python output schema: int8 becomes int64, and the system-tag "
        "and source-info columns are missing"
    ),
)
def test_operator_node_with_no_rows_keeps_columns_and_types_after_async_run(
    tmp_path: Path,
) -> None:
    """A zero-row async result should keep the sync path's columns and Arrow types."""
    sync_node, async_node = _sync_and_async_nodes(NO_MATCH, tmp_path)

    assert async_node.as_table(all_info=True).schema == (
        sync_node.as_table(all_info=True).schema
    )


AFTER_OPERATOR_CASES = [
    MERGE_JOIN_AFTER_OPERATOR,
    Case(
        "batch_after_operator",
        lambda a, b, c: Batch(batch_size=2)(SelectDataColumns(["x"])(a), label="op"),
        2,
    ),
    Case(
        "group_by_after_operator",
        lambda a, b, c: GroupBy(by=["grp"])(SelectDataColumns(["x"])(a), label="op"),
        2,
    ),
    Case(
        "group_by_after_function_pod",
        lambda a, b, c: GroupBy(by=["grp"])(double_x.pod(c), label="op"),
        2,
    ),
]


@pytest.mark.parametrize(
    "case", AFTER_OPERATOR_CASES, ids=[case.name for case in AFTER_OPERATOR_CASES]
)
def test_operator_after_an_operator_reads_back_same_system_tags_after_async_run(
    case: Case, tmp_path: Path
) -> None:
    """An async run names system tags as sync does when the input is not a source (ITL-906).

    ``MergeJoin``, ``Batch`` and ``GroupBy`` put their inputs' pipeline hashes into
    system-tag column names; the async path must use the upstream node's hash.
    """
    sync_node, async_node = _sync_and_async_nodes(case, tmp_path)

    sync_table = sync_node.as_table(all_info=True)
    async_table = async_node.as_table(all_info=True)
    assert async_table.column_names == sync_table.column_names
    assert async_table.num_rows == sync_table.num_rows == case.expected_rows
    assert _row_hashes(async_node) == _row_hashes(sync_node)
    assert sorted(
        repr(sorted(tag.system_tags().items())) for tag, _ in async_node.iter_data()
    ) == sorted(
        repr(sorted(tag.system_tags().items())) for tag, _ in sync_node.iter_data()
    )


def _cleared_table(orchestrator: str, tmp_path: Path) -> tuple[pa.Table, pa.Table]:
    """The node's table after a run, and after ``clear_cache()``."""
    node = _op_node(_run_job(CASES[0], orchestrator, "in_memory", tmp_path))
    after_run = node.as_table()

    # An operator node keeps its output in memory only: clear_cache() discards it, and
    # the node reads as a zero-row table, as before the first run.
    node.clear_cache()

    assert list(node.iter_data()) == []
    return after_run, node.as_table()


def test_clear_cache_returns_operator_node_to_not_yet_run(tmp_path: Path) -> None:
    """``clear_cache()`` gives a zero-row table on both orchestrators."""
    sync_after_run, sync_cleared = _cleared_table("sync", tmp_path / "sync")
    async_after_run, async_cleared = _cleared_table("async", tmp_path / "async")

    assert sync_after_run.num_rows == async_after_run.num_rows == 3
    assert sync_cleared.num_rows == async_cleared.num_rows == 0
    assert sync_cleared.column_names == sync_after_run.column_names
    assert async_cleared.schema == sync_cleared.schema


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ITL-912: the zero-row table built from the Python output schema widens "
        "int8 to int64 and changes nullability"
    ),
)
@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_clear_cache_keeps_operator_node_arrow_types(
    orchestrator: str, tmp_path: Path
) -> None:
    """The zero-row table after ``clear_cache()`` should keep the run's Arrow types."""
    after_run, cleared = _cleared_table(orchestrator, tmp_path)

    assert cleared.schema == after_run.schema


def _rerun_source(ids: list[int], groups: list[str]) -> ArrowTableSource:
    """A one-tag-pair source for the rerun tests; ``x`` is ``10 * id``."""
    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int8()),
            "grp": pa.array(groups, type=pa.large_string()),
            "x": pa.array([10 * i for i in ids], type=pa.int8()),
        }
    )
    return ArrowTableSource(table, tag_columns=["id", "grp"], source_id="a")


def _table_ids(node: OperatorJobNode) -> list[int]:
    """Sorted ``id`` values of the node's table."""
    ids = cast(list[int], node.as_table().column("id").to_pylist())
    return sorted(ids)


@pytest.mark.parametrize(
    ("second_ids", "second_groups", "expected_ids"),
    [
        pytest.param([1, 2], ["g2", "g2"], [], id="second_run_has_no_rows"),
        pytest.param([3, 4], ["g1", "g1"], [3, 4], id="second_run_has_other_rows"),
    ],
)
@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_operator_node_reads_back_only_the_latest_run(
    orchestrator: str,
    second_ids: list[int],
    second_groups: list[str],
    expected_ids: list[int],
) -> None:
    """A second run replaces the node's rows, even when it produces none."""
    job = PipelineJob(name="itl896_rerun", store=InMemoryArrowDatabase())
    with job:
        _ = PolarsFilter(constraints={"grp": "g1"})(
            _rerun_source([1, 2], ["g1", "g1"]), label="op"
        )
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert _table_ids(_op_node(job)) == [1, 2]

    job.bind(sources={"a": _rerun_source(second_ids, second_groups)})
    _ = job.run(orchestrator=ORCHESTRATORS[orchestrator]())

    node = _op_node(job)
    assert _table_ids(node) == expected_ids
    assert sorted(repr(tag.as_dict()["id"]) for tag, _ in node.iter_data()) == [
        repr(i) for i in expected_ids
    ]
