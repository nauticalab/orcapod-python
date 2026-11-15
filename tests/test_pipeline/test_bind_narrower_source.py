"""A slot declared ``T | None`` accepts a source of ``T``, presented as ``T | None`` (ITL-1112).

Developer decision, November 14, 2026: a source slot accepts a source that is stricter than
it declares (a column ``x: int`` for a slot declaring ``x: int | None``) and refuses the
reverse. The slot gives the rows of its declared schema: the bound source is treated as
``x: int | None``. Its rows are exactly those of a source with a nullable ``x``, the same
values and the same source id: Arrow fields, system-tag column names, row hashes and record
ids alike, so both write to one stored path and append to one table. Nothing downstream
moves: every pipeline hash and predicted schema is the declared one.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

import orcapod
from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
from orcapod.core.operators import Join, MapData
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    ZERO_ROW_INPUT_COLUMNS,
    StreamBase,
    typed_zero_row_table,
)
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, CSVSource, DictSource
from orcapod.types import Schema


@function_pod(output_keys="z")
def inc(y: int | None) -> int:
    """Return ``y + 1``, or 0 for a missing ``y``."""
    return 0 if y is None else y + 1


def _arrow_source(
    xs: list[int | None], *, x_nullable: bool, ids: list[int] | None = None
) -> ArrowTableSource:
    """Return a source of tag ``id`` (non-null) and data ``x`` with the given flag."""
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), x_nullable)]
    )
    ids = list(range(len(xs))) if ids is None else ids
    return ArrowTableSource(
        pa.table({"id": ids, "x": xs}, schema=schema),
        tag_columns=["id"],
        source_id="same",
    )


def _slot(x_type: Any) -> SourceNode:
    """Return slot ``src`` declaring tag ``id: int`` and data ``x`` of ``x_type``."""
    return SourceNode(
        name="src", tag_schema=Schema({"id": int}), data_schema=Schema({"x": x_type})
    )


def _job(tmp_path: pathlib.Path, x_type: Any = int | None) -> PipelineJob:
    """Return an unbound job: slot ``src`` -> MapData x->y ``renamed`` -> ``inc``."""
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = inc.pod(MapData({"x": "y"})(_slot(x_type), label="renamed"), label="inc")
    return job


def _fresh(tmp_path: pathlib.Path) -> PipelineJob:
    """Return the job bound to a nullable source, as recorded."""
    job = _job(tmp_path)
    job.bind(sources={"src": _arrow_source([1, None], x_nullable=True)})
    return job


def _loaded(tmp_path: pathlib.Path) -> PipelineJob:
    """Return the job, run with a nullable source and saved, loaded again."""
    job = _fresh(tmp_path)
    _ = job.run()
    path = tmp_path / "job.json"
    job.save(str(path))
    return PipelineJob.load(str(path))


JOBS: dict[str, Callable[[pathlib.Path], PipelineJob]] = {
    "fresh": _fresh,
    "loaded": _loaded,
}


def _bind(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` with ``PipelineJob.bind``."""
    job.bind(sources={"src": source})


def _assign(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` by assigning the node's ``bound_source``."""
    job.nodes["src"].bound_source = source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode


BINDS: dict[str, Callable[[PipelineJob, Any], None]] = {
    "bind": _bind,
    "bound_source": _assign,
}


def _pipeline_hashes(job: PipelineJob) -> dict[str, str]:
    """Return every node's pipeline hash, by node label."""
    return {name: node.pipeline_hash().to_string() for name, node in job.nodes.items()}


def _row_hashes(pairs: Any) -> list[tuple[str, str]]:
    """Return the tag and data content hashes of each ``(tag, data)`` pair."""
    return [
        (t.content_hash().to_string(), d.content_hash().to_string()) for t, d in pairs
    ]


async def _collect(node: Any) -> list[Any]:
    """Return every pair of ``node.async_iter_data()``."""
    return [pair async for pair in node.async_iter_data()]


# ---------------------------------------------------------------------------
# Accepted and refused
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_narrower_source_is_accepted_and_moves_nothing(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """``x: int`` binds to a slot of ``x: int | None``; schemas and pipeline hashes stay."""
    job = make_job(tmp_path)
    before = _pipeline_hashes(job)
    bind(job, DictSource([{"id": 0, "x": 1}], tag_columns=["id"], source_id="d"))
    assert _pipeline_hashes(job) == before
    assert dict(job.nodes["src"].output_schema()[1]) == {"x": int | None}
    assert dict(job.nodes["renamed"].output_schema()[1]) == {"y": int | None}
    assert job.nodes["src"].as_table().schema.field("x").nullable


@pytest.mark.parametrize("make_job", JOBS.values(), ids=JOBS.keys())
@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_nullable_source_is_refused_by_a_non_null_slot(
    tmp_path: pathlib.Path,
    make_job: Callable[[pathlib.Path], PipelineJob],
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """A source with ``x: int | None`` is refused by a slot declaring ``x: int``."""
    job = _job(tmp_path, x_type=int)
    job.bind(sources={"src": _arrow_source([1, 2], x_nullable=False)})
    if make_job is _loaded:
        _ = job.run()
        job.save(str(tmp_path / "job.json"))
        job = PipelineJob.load(str(tmp_path / "job.json"))
    with pytest.raises(
        SourceSpecMismatchError,
        match=r"'x' is declared int but the source has int \| None",
    ):
        bind(job, _arrow_source([1, None], x_nullable=True))


def test_a_narrower_tag_column_is_accepted(tmp_path: pathlib.Path) -> None:
    """A tag ``id: int`` binds to a slot declaring ``id: int | None`` and reads as one."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int | None}),
        data_schema=Schema({"x": int}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(slot, label="renamed")
    job.bind(sources={"src": DictSource([{"id": 0, "x": 1}], tag_columns=["id"])})
    assert dict(job.nodes["src"].output_schema()[0]) == {"id": int | None}
    assert job.nodes["src"].as_table().schema.field("id").nullable


@pytest.mark.parametrize(
    "declared, given",
    [(int | None, str), (int | str | None, int), (int | None, float)],
    ids=["other-type", "other-union-member", "numeric"],
)
def test_only_the_none_member_may_be_missing(
    tmp_path: pathlib.Path, declared: Any, given: type
) -> None:
    """``T | None`` accepts ``T`` only: no other type, union member or numeric type."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(declared), label="renamed")
    value: Any = {str: "a", int: 1, float: 1.5}[given]
    with pytest.raises(SourceSpecMismatchError, match="'x'"):
        job.bind(
            sources={"src": DictSource([{"id": 0, "x": value}], tag_columns=["id"])}
        )


# ---------------------------------------------------------------------------
# The rows are those of the declared schema
# ---------------------------------------------------------------------------


def _bound_slot(tmp_path: pathlib.Path, source: ArrowTableSource) -> Any:
    """Return slot ``src`` of a fresh ``x: int | None`` job, bound to *source*."""
    job = _job(tmp_path)
    job.bind(sources={"src": source})
    return job.nodes["src"]


def test_rows_equal_those_of_a_nullable_source(tmp_path: pathlib.Path) -> None:
    """Narrower rows equal a nullable source's: fields, system tags, hashes, record ids."""
    narrower = _bound_slot(tmp_path / "a", _arrow_source([1, 2], x_nullable=False))
    nullable = _bound_slot(tmp_path / "b", _arrow_source([1, 2], x_nullable=True))
    assert narrower.as_table(all_info=True).equals(
        nullable.as_table(all_info=True), check_metadata=True
    )
    assert _row_hashes(narrower.iter_data()) == _row_hashes(nullable.iter_data())
    assert _row_hashes(asyncio.run(_collect(narrower))) == _row_hashes(
        nullable.iter_data()
    )
    assert narrower.keys(all_info=True) == nullable.keys(all_info=True)


def test_rows_of_each_read_carry_the_declared_flags(tmp_path: pathlib.Path) -> None:
    """``iter_data``, ``async_iter_data`` and ``head`` give nullable ``x``."""
    slot = _bound_slot(tmp_path, _arrow_source([1, 2], x_nullable=False))
    for _, data in slot.iter_data():
        assert data.as_table().schema.field("x").nullable
    for _, data in asyncio.run(_collect(slot)):
        assert data.as_table().schema.field("x").nullable
    view = slot.head(1, all_info=True)
    head = view.table
    assert head.schema.field("x").nullable
    assert view.tag_columns == slot.keys(columns={"system_tags": True})[0]
    assert head.equals(slot.as_table(all_info=True).slice(0, 1))


def test_zero_rows_have_the_types_of_rows(tmp_path: pathlib.Path) -> None:
    """A narrower source with no rows reads with the types it has with rows."""

    @function_pod(output_keys="x")
    def same(v: int) -> int:
        """Return ``v``."""
        return v

    origin = PipelineJob(name="origin", database=InMemoryArrowDatabase())
    with origin:
        _ = same.pod(
            DictSource([{"id": 0, "v": 1}], tag_columns=["id"], source_id="o"),
            label="same",
        )
    empty = origin.nodes["same"].as_source()  # pyright: ignore[reportAttributeAccessIssue]  # a function job node; as_source before a run has no rows
    job = _job(tmp_path)
    job.bind(sources={"src": empty})
    with_rows = _bound_slot(tmp_path / "rows", _arrow_source([1], x_nullable=False))
    table = job.nodes["src"].as_table()
    assert table.num_rows == 0
    assert table.schema == with_rows.as_table().schema


# ---------------------------------------------------------------------------
# One stored path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "orchestrator",
    [SyncPipelineOrchestrator, AsyncPipelineOrchestrator],
    ids=["sync", "async"],
)
def test_narrower_then_nullable_runs_append_to_one_path(
    tmp_path: pathlib.Path, orchestrator: type
) -> None:
    """A run of a narrower source, then of a nullable one, append under the same path."""
    job = _job(tmp_path)
    job.bind(sources={"src": _arrow_source([1, 2], x_nullable=False)})
    database = job.database
    assert database is not None
    first = job.run(orchestrator=orchestrator())
    assert first.ok
    paths = set(database.list_table_paths(()))
    job.bind(sources={"src": _arrow_source([None, 3], x_nullable=True, ids=[2, 3])})
    second = job.run(orchestrator=orchestrator())
    assert second.ok
    assert set(database.list_table_paths(())) == paths
    assert sorted(job.nodes["inc"].as_table().column("z").to_pylist()) == [0, 4]
    stored = [database.get_all_records(path) for path in paths if path[-1] == "pdb_v2"]
    assert [None if table is None else table.num_rows for table in stored] == [4]


# ---------------------------------------------------------------------------
# A loaded job's rebuilt file source
# ---------------------------------------------------------------------------


def _csv_job(tmp_path: pathlib.Path, text: str) -> str:
    """Save a job over a CSV source of *text*; return the job file's path."""
    csv = tmp_path / "data.csv"
    _ = csv.write_text(text)
    job = PipelineJob(name="c", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = MapData({"x": "y"})(
            CSVSource(str(csv), tag_columns=["id"], source_id="csv"), label="renamed"
        )
    _ = job.run()
    path = tmp_path / "job.json"
    job.save(str(path))
    return str(path)


def _csv_slot(job: PipelineJob) -> Any:
    """Return the CSV source's slot of *job*."""
    return job.nodes["csv"]


def test_loaded_csv_of_another_type_is_left_unbound(
    tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A CSV whose column changed type loads unbound, with a warning naming the slot."""
    path = _csv_job(tmp_path, "id,x\n0,1\n1,2\n")
    _ = (tmp_path / "data.csv").write_text("id,x\n0,a\n1,b\n")
    with caplog.at_level(logging.WARNING):
        job = PipelineJob.load(path)
    assert any(
        "'csv'" in r.getMessage() and "declared int" in r.getMessage()
        for r in caplog.records
    )
    assert job.unbound_sources == ["csv"]
    with pytest.raises(UnboundSourceError):
        _ = _csv_slot(job).as_table()
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _ = job.run()
    _ = (tmp_path / "fixed.csv").write_text("id,x\n0,7\n")
    job.bind(
        sources={
            "csv": CSVSource(
                str(tmp_path / "fixed.csv"), tag_columns=["id"], source_id="csv"
            )
        }
    )
    assert _csv_slot(job).as_table().column("x").to_pylist() == [7]


def test_loaded_csv_now_narrower_is_bound_as_declared(tmp_path: pathlib.Path) -> None:
    """A CSV saved with an empty cell (``x: int | None``) and now without loads bound."""
    path = _csv_job(tmp_path, "id,x\n0,1\n1,\n")
    _ = (tmp_path / "data.csv").write_text("id,x\n0,1\n1,2\n")
    job = PipelineJob.load(path)
    slot = _csv_slot(job)
    assert dict(slot.output_schema()[1]) == {"x": int | None}
    assert slot.as_table().schema.field("x").nullable
    assert slot.as_table().column("x").to_pylist() == [1, 2]


# ---------------------------------------------------------------------------
# System-tag values are the source's own
# ---------------------------------------------------------------------------


def test_system_tag_values_and_source_info_are_the_sources_own(
    tmp_path: pathlib.Path,
) -> None:
    """Names follow the slot; source id, record ids and source-info tokens the source.

    The source is given no source id, so its default hash-based id is computed from its
    true table (``x: int``) and is carried unchanged.
    """
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), False)]
    )
    source = ArrowTableSource(
        pa.table({"id": [0, 1], "x": [1, 2]}, schema=schema), tag_columns=["id"]
    )
    direct = source.as_table(all_info=True)
    slot = _bound_slot(tmp_path, source)
    bound = slot.as_table(all_info=True)

    def system_tag(table: pa.Table, prefix: str) -> list[Any]:
        """Return the values of the one column of *table* whose name starts with *prefix*."""
        (name,) = [c for c in table.column_names if c.startswith(prefix)]
        return table.column(name).to_pylist()

    for prefix in ("_tag_source_id::", "_tag_record_id::"):
        assert system_tag(bound, prefix) == system_tag(direct, prefix)
    assert system_tag(bound, "_tag_source_id::") == [source.source_id] * 2
    assert (
        bound.column("_source_x").to_pylist() == direct.column("_source_x").to_pylist()
    )
    # The names are the slot's, not the source's.
    names = [c for c in bound.column_names if c.startswith("_tag_")]
    assert names == list(slot.keys(columns={"system_tags": True})[0][1:])
    assert not set(names) & set(direct.column_names)


# ---------------------------------------------------------------------------
# Column order, transformed streams and meta columns (CodeRabbit round 1)
# ---------------------------------------------------------------------------


def _abc_job(a_type: Any) -> PipelineJob:
    """Return a job whose slot declares tag ``id`` and data ``a``, ``b``, ``c`` in order."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"a": a_type, "b": str, "c": float}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"a": "a2"})(slot, label="renamed")
    return job


@pytest.mark.parametrize("a_type", [int, int | None], ids=["equal", "narrower"])
def test_columns_follow_the_declared_order(a_type: Any) -> None:
    """A source with its columns in another order is shown in the declared order.

    A convenience, not part of the bind invariant: column order is not (developer,
    November 14, 2026).
    """
    job = _abc_job(a_type)
    job.bind(
        sources={
            "src": DictSource(
                [{"id": 0, "c": 1.5, "b": "s", "a": 1}], tag_columns=["id"]
            )
        }
    )
    node = job.nodes["src"]
    assert list(node.output_schema()[1]) == ["a", "b", "c"]
    table = node.as_table(columns={"source": True})
    assert table.column_names == [
        "id",
        "a",
        "b",
        "c",
        "_source_a",
        "_source_b",
        "_source_c",
    ]
    for _, data in node.iter_data():
        assert data.as_table().column_names == ["a", "b", "c"]
    for _, data in asyncio.run(_collect(node)):
        assert data.as_table().column_names == ["a", "b", "c"]
    assert node.head(1).table.column_names == ["id", "a", "b", "c"]
    assert job.nodes["renamed"].as_table().column_names == ["id", "a2", "b", "c"]


def _mapped_stream() -> Any:
    """Return a stream (not a root source): a DictSource with ``q`` renamed to ``x``."""
    source = DictSource([{"id": 0, "q": 1}], tag_columns=["id"], source_id="m")
    return MapData({"q": "x"})(source)


@pytest.mark.parametrize("x_type", [int, int | None], ids=["equal", "narrower"])
def test_a_transformed_stream_gets_the_slots_system_tag_names(
    tmp_path: pathlib.Path, x_type: Any
) -> None:
    """A stream's one system-tag pair takes the slot's names; its values stay its own."""
    stream = _mapped_stream()
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(x_type), label="renamed")
    job.bind(sources={"src": stream})
    node = job.nodes["src"]
    predicted = node.keys(columns={"system_tags": True})[0][1:]
    table = node.as_table(columns={"system_tags": True})
    names = [c for c in table.column_names if c.startswith("_tag_")]
    assert names == list(predicted)
    direct = stream.as_table(columns={"system_tags": True})
    direct_names = [c for c in direct.column_names if c.startswith("_tag_")]
    assert not set(direct_names) & set(names)
    for mine, theirs in zip(names, direct_names, strict=True):
        assert table.column(mine).to_pylist() == direct.column(theirs).to_pylist()


def _joined_stream(x_nullable: bool) -> Any:
    """Return a Join of two sources: two system-tag pairs, data ``x`` and ``w``."""
    left = _arrow_source([1, 2], x_nullable=x_nullable)
    right = DictSource([{"id": 0, "w": "p"}, {"id": 1, "w": "q"}], tag_columns=["id"])
    return Join()(left, right)


def _xw_job() -> PipelineJob:
    """Return a job whose slot declares tag ``id`` and data ``x: int | None``, ``w``."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int | None, "w": str}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(slot, label="renamed")
    return job


def test_meta_columns_of_a_narrower_stream_are_kept() -> None:
    """The ``__`` columns a job node's rows carry reach the rows of a narrower bind."""

    @function_pod(output_keys="x")
    def same(v: int) -> int:
        """Return ``v``."""
        return v

    origin = PipelineJob(name="origin", database=InMemoryArrowDatabase())
    with origin:
        _ = same.pod(
            DictSource([{"id": 0, "v": 1}], tag_columns=["id"], source_id="o"),
            label="same",
        )
    _ = origin.run()
    stream = origin.nodes["same"]
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int | None), label="renamed")
    job.bind(sources={"src": stream})
    node = job.nodes["src"]
    own = [
        sorted(data.as_table(columns={"meta": True}).column_names)
        for _, data in stream.iter_data()
    ]
    assert any("__computed" in names for names in own)
    for rows in (list(node.iter_data()), asyncio.run(_collect(node))):
        assert [
            sorted(data.as_table(columns={"meta": True}).column_names)
            for _, data in rows
        ] == own


# ---------------------------------------------------------------------------
# Zero-row tables (ITL-912): the declared form, as the populated table has it
# ---------------------------------------------------------------------------


def _declared_bind() -> PipelineJob:
    """Return the ``x: int`` job bound to a source of exactly the declared form."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int), label="renamed")
    job.bind(sources={"src": _arrow_source([1, 2], x_nullable=False)})
    return job


def _narrower_bind() -> PipelineJob:
    """Return the ``x: int | None`` job bound to a source of ``x: int``."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int | None), label="renamed")
    job.bind(sources={"src": _arrow_source([1, 2], x_nullable=False)})
    return job


def _reordered_bind() -> PipelineJob:
    """Return the ``a, b, c`` job bound to a source whose data come as ``c, b, a``."""
    job = _abc_job(int)
    source = DictSource([{"id": 0, "c": 1.5, "b": "s", "a": 1}], tag_columns=["id"])
    job.bind(sources={"src": source})
    return job


def _transformed_bind() -> PipelineJob:
    """Return the ``x: int`` job bound to a stream whose system tags have other names."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int), label="renamed")
    job.bind(sources={"src": _mapped_stream()})
    return job


DECLARED_FORM_BINDS: dict[str, Callable[[], PipelineJob]] = {
    "declared": _declared_bind,
    "narrower": _narrower_bind,
    "reordered": _reordered_bind,
    "transformed": _transformed_bind,
}


@pytest.mark.parametrize(
    "make_job", DECLARED_FORM_BINDS.values(), ids=DECLARED_FORM_BINDS.keys()
)
def test_the_zero_row_table_of_a_slot_is_its_populated_declared_form(
    make_job: Callable[[], PipelineJob],
) -> None:
    """A slot's zero-row table has exactly the schema of its populated table."""
    job = make_job()
    node = job.nodes["src"]
    populated = node.as_table(columns=ZERO_ROW_INPUT_COLUMNS)
    assert populated.num_rows > 0
    table = typed_zero_row_table(node)
    assert table is not None
    assert table.num_rows == 0
    assert table.schema == populated.schema


@pytest.mark.parametrize(
    "make_job", DECLARED_FORM_BINDS.values(), ids=DECLARED_FORM_BINDS.keys()
)
def test_a_downstream_node_before_a_run_has_the_types_of_its_rows(
    make_job: Callable[[], PipelineJob],
) -> None:
    """The node after the slot reads, before a run, with the schema it has after one."""
    job = make_job()
    before = job.nodes["renamed"].as_table(all_info=True)
    head = job.nodes["renamed"].head(all_info=True).table
    assert before.num_rows == 0
    _ = job.run()
    after = job.nodes["renamed"].as_table(all_info=True)
    assert after.num_rows > 0
    assert before.schema == after.schema
    assert head.schema == after.schema


# ---------------------------------------------------------------------------
# Not exactly one system-tag pair: refused (developer, November 14, 2026, ITL-1126)
# ---------------------------------------------------------------------------


def _raw_stream(x_nullable: bool) -> Any:
    """Return an ``ArrowTableStream`` of tag ``id`` and data ``x``, ``w``: no system tags."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("x", pa.int64(), x_nullable),
            pa.field("w", pa.large_string(), False),
        ]
    )
    return ArrowTableStream(
        pa.table({"id": [0], "x": [1], "w": ["p"]}, schema=schema), tag_columns=["id"]
    )


def _construct(job: PipelineJob, source: Any) -> None:
    """Build a node for slot ``src`` with *source* bound at construction."""
    slot = job.nodes["src"]
    _ = SourceJobNode(
        name="src",
        tag_schema=slot.output_schema()[0],
        data_schema=slot.output_schema()[1],
        bound_source=source,
    )


ALL_BINDS: dict[str, Callable[[PipelineJob, Any], None]] = {
    **BINDS,
    "construction": _construct,
}

ONE_PAIR_REQUIRED: dict[str, Callable[[bool], Any]] = {
    "joined": _joined_stream,
    "no_system_tags": _raw_stream,
}


@pytest.mark.parametrize("x_nullable", [True, False], ids=["declared", "narrower"])
@pytest.mark.parametrize(
    "make_stream", ONE_PAIR_REQUIRED.values(), ids=ONE_PAIR_REQUIRED.keys()
)
@pytest.mark.parametrize("bind", ALL_BINDS.values(), ids=ALL_BINDS.keys())
def test_a_stream_without_exactly_one_system_tag_pair_is_refused(
    bind: Callable[[PipelineJob, Any], None],
    make_stream: Callable[[bool], Any],
    x_nullable: bool,
) -> None:
    """A joined stream, or one with no system tags, cannot take the slot's one pair.

    The slot's rows carry one source-id and one record-id column, under the slot's
    names. Refused whatever the column types; the error names the slot and the
    system-tag columns found.
    """
    stream = make_stream(x_nullable)
    found = [
        name
        for name in stream.keys(columns={"system_tags": True})[0]
        if name.startswith("_tag")
    ]
    job = _xw_job()
    with pytest.raises(SourceSpecMismatchError) as raised:
        bind(job, stream)
    message = str(raised.value)
    assert "'src'" in message
    assert "exactly one source-id and one record-id" in message
    assert str(sorted(found)) in message
    assert job.nodes["src"].bound_source is None


def test_a_refused_joined_stream_keeps_the_previous_binding() -> None:
    """After the refusal the slot keeps the source it had."""
    job = _xw_job()
    before = DictSource([{"id": 0, "x": 1, "w": "p"}], tag_columns=["id"])
    job.bind(sources={"src": before})
    with pytest.raises(SourceSpecMismatchError):
        job.bind(sources={"src": _joined_stream(x_nullable=True)})
    assert job.nodes["src"].bound_source is before


def test_a_pipeline_built_over_a_raw_stream_is_refused() -> None:
    """A raw stream as a pipeline's input is refused, telling the user what to do.

    Developer, November 14, 2026: refuse a pipeline built directly over a stream
    without system tags until a raw stream is captured into a source (ITL-1127).
    """
    raw = _raw_stream(x_nullable=True)
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with pytest.raises(SourceSpecMismatchError) as raised:
        with job:
            _ = MapData({"x": "y"})(raw, label="renamed")
    message = str(raised.value)
    assert "exactly one source-id and one record-id" in message
    assert "ArrowTableSource(stream.as_table(), tag_columns=[...])" in message
    assert "ITL-1127" in message


def test_a_raw_streams_table_wrapped_as_the_error_advises_runs() -> None:
    """``ArrowTableSource(stream.as_table(), tag_columns=[...])`` builds and runs."""
    raw = _raw_stream(x_nullable=True)
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(
            ArrowTableSource(raw.as_table(), tag_columns=["id"]), label="renamed"
        )
    _ = job.run()
    table = job.nodes["renamed"].as_table()
    assert table.column("y").to_pylist() == [1]
    assert table.column("w").to_pylist() == ["p"]


# ---------------------------------------------------------------------------
# The slot's predicted schema says what its rows are, bound or unbound
# ---------------------------------------------------------------------------


def _x_job() -> PipelineJob:
    """Return an unbound job: slot ``src`` (``id: int``, ``x: int | None``) -> rename."""
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(int | None), label="renamed")
    return job


def _abc_unbound() -> PipelineJob:
    """Return the unbound ``a, b, c`` job."""
    return _abc_job(int)


def _abc_bound(order: str) -> Callable[[], PipelineJob]:
    """Return a maker of the ``a, b, c`` job bound to a source with data in *order*."""

    def make() -> PipelineJob:
        job = _abc_job(int)
        values = {"a": 1, "b": "s", "c": 1.5}
        row = {"id": 0, **{name: values[name] for name in order}}
        job.bind(sources={"src": DictSource([row], tag_columns=["id"])})
        return job

    return make


def _x_bound(source: Callable[[], Any]) -> Callable[[], PipelineJob]:
    """Return a maker of the ``x: int | None`` job bound to ``source()``."""

    def make() -> PipelineJob:
        job = _x_job()
        job.bind(sources={"src": source()})
        return job

    return make


PREDICTION_CASES: dict[str, tuple[Callable[[], PipelineJob], ...]] = {
    "x": (
        _x_job,
        _x_bound(lambda: _arrow_source([1, None], x_nullable=True)),
        _x_bound(lambda: _arrow_source([1, 2], x_nullable=False)),
        _x_bound(_mapped_stream),
    ),
    "abc": (_abc_unbound, _abc_bound("abc"), _abc_bound("cba")),
}


@pytest.mark.parametrize("config", [{"system_tags": True}, "all_info"])
@pytest.mark.parametrize(
    "makers", PREDICTION_CASES.values(), ids=PREDICTION_CASES.keys()
)
def test_the_predicted_schema_is_that_of_the_rows_bound_or_unbound(
    makers: tuple[Callable[[], PipelineJob], ...], config: Any
) -> None:
    """Unbound, declared, narrower, reordered and transformed binds predict alike.

    The prediction (system tags included: the record id is ``bytes``) equals the
    Python schema of the populated rows, for the slot and the node after it.
    """
    predictions = []
    for make in makers:
        job = make()
        predictions.append(
            {
                label: (
                    job.nodes[label].output_schema(all_info=True)
                    if config == "all_info"
                    else job.nodes[label].output_schema(columns=config)
                )
                for label in ("src", "renamed")
            }
        )
        if job.nodes["src"].bound_source is None:  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode
            continue
        for label, (tag, data) in predictions[-1].items():
            node = job.nodes[label]
            table = node.as_table(columns={"system_tags": True})
            rows = node.data_context.type_converter.arrow_schema_to_python_schema(
                table.schema
            )
            assert {**tag, **data} == dict(rows), label
    assert all(p == predictions[0] for p in predictions), predictions


# ---------------------------------------------------------------------------
# CodeRabbit round 2: structural classes, extra system tags, system-tag types
# ---------------------------------------------------------------------------


@orcapod.dataclass(name="BindPoint")
class _PointA:
    """A structural class, identified by its name and fields."""

    x: int
    y: int


@orcapod.dataclass(name="BindPoint")
class _PointB:
    """Another class object of the same structural identity as ``_PointA``."""

    x: int
    y: int


@dataclasses.dataclass
class _ExactA:
    """An exact class: one type only with itself."""

    x: int
    y: int


@dataclasses.dataclass
class _ExactB:
    """An exact class with the same fields as ``_ExactA``, yet another type."""

    x: int
    y: int


def _rows(*rows: dict[str, Any]) -> list[dict[str, Any]]:
    """Return *rows* as a ``DictSource`` takes them (user-class values included)."""
    return list(rows)


def _point_job(point_type: Any) -> PipelineJob:
    """Return an unbound job: slot ``src`` (``id: int``, ``p: point_type``) -> rename."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"p": point_type}),
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"p": "q"})(slot, label="renamed")
    return job


@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_a_structural_class_of_the_same_identity_is_accepted(
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """A column of another class object of the declared structural identity fits.

    Two structural classes with equal identity records are one type in the schema
    check and in the pipeline hash, so the bind moves no hash or schema.
    """
    job = _point_job(_PointA)
    source = DictSource(
        _rows({"id": 0, "p": _PointB(1, 2)}),
        tag_columns=["id"],
        data_schema={"id": int, "p": _PointB},
        source_id="b",
    )
    assert source.output_schema()[1]["p"] is not _PointA
    hashes = _pipeline_hashes(job)
    schemas = {
        label: node.output_schema(all_info=True) for label, node in job.nodes.items()
    }
    bind(job, source)
    assert job.nodes["src"].bound_source is source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode
    assert _pipeline_hashes(job) == hashes
    for label, node in job.nodes.items():
        if isinstance(node, StreamBase):
            # Predict afresh: the test is about the schema, not the memo.
            node._forget_output_schema()  # pyright: ignore[reportPrivateUsage]
        assert node.output_schema(all_info=True) == schemas[label], label
    assert job.run().ok
    assert job.nodes["renamed"].as_table().num_rows == 1


@pytest.mark.parametrize("bind", BINDS.values(), ids=BINDS.keys())
def test_an_exact_class_with_the_same_fields_is_refused(
    bind: Callable[[PipelineJob, Any], None],
) -> None:
    """Two exact classes with the same fields are two types."""
    job = _point_job(_ExactA)
    source = DictSource(
        _rows({"id": 0, "p": _ExactB(1, 2)}),
        tag_columns=["id"],
        data_schema={"id": int, "p": _ExactB},
    )
    with pytest.raises(SourceSpecMismatchError, match="data column 'p'"):
        bind(job, source)


def _stream_with_system_tags(columns: dict[str, pa.Array[Any]]) -> ArrowTableStream:
    """Return a stream of tag ``id``, data ``x`` and the given system-tag columns."""
    table = pa.table(
        {
            "id": pa.array([0], pa.int64()),
            "x": pa.array([1], pa.int64()),
            **columns,
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), False),
                pa.field("x", pa.int64(), False),
                *(pa.field(name, array.type, False) for name, array in columns.items()),
            ]
        ),
    )
    return ArrowTableStream(table, tag_columns=["id"])


SOURCE_ID = "_tag_source_id::abc"
RECORD_ID = "_tag_record_id::abc"


@pytest.mark.parametrize("x_type", [int, int | None], ids=["declared", "narrower"])
@pytest.mark.parametrize("bind", ALL_BINDS.values(), ids=ALL_BINDS.keys())
def test_an_extra_system_tag_column_is_refused(
    bind: Callable[[PipelineJob, Any], None], x_type: Any
) -> None:
    """A system-tag column besides the one pair is not in the slot's schema."""
    stream = _stream_with_system_tags(
        {
            SOURCE_ID: pa.array(["s"], pa.large_string()),
            RECORD_ID: pa.array([b"r"], pa.large_binary()),
            "_tag_extra": pa.array(["e"], pa.large_string()),
        }
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(x_type), label="renamed")
    with pytest.raises(SourceSpecMismatchError, match="_tag_extra"):
        bind(job, stream)


@pytest.mark.parametrize("x_type", [int, int | None], ids=["declared", "narrower"])
@pytest.mark.parametrize("bind", ALL_BINDS.values(), ids=ALL_BINDS.keys())
def test_a_system_tag_pair_of_other_types_is_refused(
    bind: Callable[[PipelineJob, Any], None], x_type: Any
) -> None:
    """The source id must be ``str`` and the record id ``bytes``, as the slot predicts."""
    stream = _stream_with_system_tags(
        {
            SOURCE_ID: pa.array(["s"], pa.large_string()),
            RECORD_ID: pa.array(["r"], pa.large_string()),
        }
    )
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        _ = MapData({"x": "y"})(_slot(x_type), label="renamed")
    with pytest.raises(SourceSpecMismatchError, match=r"declared bytes but the source"):
        bind(job, stream)
