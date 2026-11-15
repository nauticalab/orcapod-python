"""No accepted bind changes a node's stored path or its columns (ITL-1112).

Developer, November 14, 2026: "for pipeline, all node's path/table schema should NOT change
regardless of what source you bind to the source nodes in the pipeline". One job is run over
every kind of accepted bind: a source of the declared schema, a narrower source into a
``T | None`` slot, sources of different kinds, through ``bind``, the ``bound_source`` setter
and ``PipelineJob.load``. After each, the stored table paths, the Arrow schema of every stored
table and of every node's ``as_table(all_info=True)`` (each column by name, system tags
included, with its type and nullable flag) are those of the first bind: only values differ.
Column order is not part of that (developer, November 14, 2026), so the snapshots map names to
fields; the order a slot presents is tested on its own (``test_order_convenience_*``).

The bind check compares Python types, so two Arrow widths of one Python type (``int32`` and
``int64``) are not covered here (ITL-949).
"""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from typing import Any, cast

import pyarrow as pa

import orcapod
from orcapod import PipelineJob, function_pod
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import MapData
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase
from orcapod.sources import ArrowTableSource, CSVSource, DictSource
from orcapod.system_constants import constants
from orcapod.types import Schema


@function_pod(output_keys="z")
def inc(y: int | None) -> int:
    """Return ``y + 1``, or 0 for a missing ``y``."""
    return 0 if y is None else y + 1


def _arrow(
    ids: list[int], xs: list[int | None], *, x_nullable: bool
) -> ArrowTableSource:
    """Return an Arrow source of tag ``id`` (non-null) and data ``x``."""
    schema = pa.schema(
        [pa.field("id", pa.int64(), False), pa.field("x", pa.int64(), x_nullable)]
    )
    return ArrowTableSource(
        pa.table({"id": ids, "x": xs}, schema=schema), tag_columns=["id"]
    )


def _csv(tmp_path: pathlib.Path, name: str, text: str) -> CSVSource:
    """Return a CSV source over *text*, written to *name* in *tmp_path*."""
    path = tmp_path / name
    _ = path.write_text(text)
    return CSVSource(str(path), tag_columns=["id"], source_id=name)


def _record_id_first(source: ArrowTableSource) -> ArrowTableStream:
    """Return a stream of *source*'s rows with its system-tag pair record-id first.

    The pair swaps places; every other column keeps its position.
    """
    table = source.as_table(
        columns={"system_tags": True, "source": True, "context": True}
    )
    names = table.column_names
    pair = [n for n in names if n.startswith(constants.SYSTEM_TAG_PREFIX)]
    assert len(pair) == 2 and constants.SYSTEM_TAG_SOURCE_ID_PREFIX in pair[0]
    swap = {pair[0]: pair[1], pair[1]: pair[0]}
    reordered = table.select([swap.get(name, name) for name in names])
    return ArrowTableStream(reordered, tag_columns=["id"])


NODES = ("src", "renamed", "inc")
# A loaded job reads stored results only; its uncached operator has none to read.
LOADED_NODES = ("src", "inc")


def _fields(schema: pa.Schema) -> dict[str, Any]:
    """Return *schema*'s fields by name: the columns, whatever their order."""
    return {field.name: field for field in schema}


def _snapshot(job: PipelineJob, nodes: tuple[str, ...] = NODES) -> dict[str, Any]:
    """Return the stored paths, and the columns of the stored tables and *nodes*' tables."""
    database = job.database
    assert database is not None
    paths = sorted(database.list_table_paths(()))
    stored = {}
    for path in paths:
        table = database.get_all_records(path)
        assert table is not None
        stored[path] = _fields(table.schema)
    return {
        "paths": paths,
        "stored": stored,
        "nodes": {
            name: _fields(job.nodes[name].as_table(all_info=True).schema)
            for name in nodes
        },
    }


def _assign(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` by assigning the node's ``bound_source``."""
    job.nodes["src"].bound_source = source  # pyright: ignore[reportAttributeAccessIssue]  # "src" is a SourceJobNode


def _bind(job: PipelineJob, source: Any) -> None:
    """Bind *source* to slot ``src`` with ``PipelineJob.bind``."""
    job.bind(sources={"src": source})


def test_no_accepted_bind_changes_a_path_or_a_table_schema(
    tmp_path: pathlib.Path,
) -> None:
    """Every accepted bind leaves paths and every stored and node table schema as they were."""
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int | None}),
    )
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = inc.pod(MapData({"x": "y"})(slot, label="renamed"), label="inc")

    binds: list[tuple[str, Callable[[PipelineJob, Any], None], Any]] = [
        ("declared schema", _bind, _arrow([0, 1], [1, None], x_nullable=True)),
        ("narrower arrow", _bind, _arrow([2, 3], [2, 3], x_nullable=False)),
        (
            "narrower dict",
            _assign,
            DictSource([{"id": 4, "x": 4}], tag_columns=["id"], source_id="d"),
        ),
        (
            "dict with a null",
            _assign,
            DictSource([{"id": 5, "x": None}, {"id": 6, "x": 6}], tag_columns=["id"]),
        ),
        ("csv with an empty cell", _bind, _csv(tmp_path, "a.csv", "id,x\n7,\n8,8\n")),
        (
            "stream over a source",
            _assign,
            MapData({"q": "x"})(
                DictSource([{"id": 10, "q": 10}], tag_columns=["id"], source_id="m")
            ),
        ),
        (
            "system-tag pair record-id first",
            _assign,
            _record_id_first(_arrow([11, 12], [11, None], x_nullable=True)),
        ),
        (
            "narrower, system-tag pair record-id first",
            _bind,
            _record_id_first(_arrow([13], [13], x_nullable=False)),
        ),
        ("narrower csv", _bind, _csv(tmp_path, "b.csv", "id,x\n9,9\n")),
    ]
    first: dict[str, Any] | None = None
    for label, bind, source in binds:
        bind(job, source)
        assert job.run().ok, label
        snapshot = _snapshot(job)
        if first is None:
            first = snapshot
        assert snapshot == first, label

    assert first is not None
    # The last bound source, the narrower CSV, is rebuilt from its file on load.
    job.save(str(tmp_path / "job.json"))
    loaded = PipelineJob.load(str(tmp_path / "job.json"))
    before = _snapshot(loaded, LOADED_NODES)
    assert before["paths"] == first["paths"]
    assert before["stored"] == first["stored"]
    assert before["nodes"]["src"] == first["nodes"]["src"]
    _bind(loaded, _arrow([0, 1], [1, None], x_nullable=True))
    # A loaded function node orders its meta columns unlike a fresh one, bind or not
    # (reported separately), so the loaded job is compared with itself.
    assert _snapshot(loaded, LOADED_NODES) == before


@orcapod.dataclass(name="KeepsPoint")
class _PointA:
    """A structural class, identified by its name and fields."""

    x: int
    y: int


@orcapod.dataclass(name="KeepsPoint")
class _PointB:
    """Another class object of the same structural identity as ``_PointA``."""

    x: int
    y: int


def test_a_structural_class_of_the_same_identity_changes_no_path_or_schema(
    tmp_path: pathlib.Path,
) -> None:
    """A source of another class object of the slot's structural identity moves nothing.

    The sources are built before the baseline hashes are taken: on main too, the
    pipeline hash of a schema holding a structural class differs before and after a
    source of that class is first built, bind or no bind (reported separately).
    """
    sources = [
        DictSource(
            cast(list[dict[str, Any]], [{"id": 0, "p": point_type(1, 2)}]),
            tag_columns=["id"],
            data_schema={"id": int, "p": point_type},
            source_id="points",
        )
        for point_type in (_PointA, _PointB, _PointB)
    ]
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"p": _PointA}),
    )
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = MapData({"p": "q"})(slot, label="renamed")
    hashes = {name: node.pipeline_hash() for name, node in job.nodes.items()}

    snapshots = []
    for source, bind in zip(sources, (_bind, _bind, _assign), strict=True):
        bind(job, source)
        assert {name: node.pipeline_hash() for name, node in job.nodes.items()} == (
            hashes
        )
        assert job.run().ok
        snapshots.append(_snapshot(job, ("src", "renamed")))
    assert snapshots[1] == snapshots[0]
    assert snapshots[2] == snapshots[0]


def test_order_convenience_a_record_id_first_pair_is_shown_source_id_first() -> None:
    """A source whose system-tag pair comes record-id first is shown source-id first.

    A convenience, not part of the bind invariant (column order is not): the slot shows
    the order it predicts (``output_schema``) and a source of the declared order has, in
    the schema, the table, the head and the zero-row table, for a slot of the source's
    own types and for a ``T | None`` slot the source is narrower than.
    """
    for x_type, x_nullable in ((int, False), (int | None, True)):
        slot = SourceNode(
            name="src",
            tag_schema=Schema({"id": int}),
            data_schema=Schema({"x": x_type}),
        )
        job = PipelineJob(name="j")
        with job:
            _ = MapData({"x": "y"})(slot, label="renamed")
        node = job.nodes["src"]
        declared = list(node.output_schema(all_info=True)[0])
        reference = _arrow([0], [0], x_nullable=x_nullable)
        job.bind(sources={"src": reference})
        order = node.as_table(all_info=True).column_names
        zero_row_order = node.head(0, all_info=True).table.column_names

        for rows in ([1, 2], []):
            # A non-null ``x``: the slot's own type, or narrower than ``int | None``.
            xs: list[int | None] = list(rows)
            source = _record_id_first(_arrow(rows, xs, x_nullable=False))
            job.bind(sources={"src": source})
            assert list(node.output_schema(all_info=True)[0]) == declared, x_type
            assert node.as_table(all_info=True).column_names == order, x_type
            assert node.head(all_info=True).table.column_names == (
                order if rows else zero_row_order
            ), x_type


@function_pod(output_keys="x")
def passes_on(w: int | None) -> int | None:
    """Return ``w`` unchanged."""
    return w


def test_a_bound_function_stream_moves_no_stored_or_downstream_schema(
    tmp_path: pathlib.Path,
) -> None:
    """A function pod's stream (one system-tag pair) moves no path or column.

    Stored paths, stored tables' columns and every downstream node's columns are those of
    a source bind, as are the source node's user, system-tag and context columns. The
    source node's own table keeps the nullable flags the stream gives its
    ``_content_hash`` and source-info columns; that table is not stored and no downstream
    node sees those flags.
    """
    slot = SourceNode(
        name="src",
        tag_schema=Schema({"id": int}),
        data_schema=Schema({"x": int | None}),
    )
    job = PipelineJob(name="j", database=DeltaTableDatabase(str(tmp_path / "store")))
    with job:
        _ = inc.pod(MapData({"x": "y"})(slot, label="renamed"), label="inc")
    _bind(job, _arrow([0, 1], [1, None], x_nullable=True))
    assert job.run().ok
    first = _snapshot(job)

    upstream = MapData({"x": "w"})(_arrow([5, 6], [5, None], x_nullable=True))
    _bind(job, passes_on.pod(upstream))
    assert job.run().ok
    second = _snapshot(job)
    assert second["paths"] == first["paths"]
    assert second["stored"] == first["stored"]
    for name in ("renamed", "inc"):
        assert second["nodes"][name] == first["nodes"][name], name
    src_first, src_second = first["nodes"]["src"], second["nodes"]["src"]
    assert src_second.keys() == src_first.keys()
    compared = [
        name
        for name in src_first
        if name in ("id", "x", constants.CONTEXT_KEY)
        or name.startswith(constants.SYSTEM_TAG_PREFIX)
    ]
    assert len(compared) == 5
    for name in compared:
        assert src_second[name] == src_first[name], name
