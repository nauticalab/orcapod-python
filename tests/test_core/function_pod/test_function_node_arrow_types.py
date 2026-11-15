"""A function node in a job keeps the Arrow type of every tag column (ITL-928).

A function pod never touches tags, so a function node's tag columns must have the
input's exact Arrow types (large-normalized) whatever the database, the orchestrator and
the read: ``as_table()``, ``iter_data()``, ``get_all_records()``, a fresh job on the same
store, and a re-run. Before ITL-928 a node's ``as_table()`` rebuilt its table from Python
values typed by the node's Python output schema, so an ``int8`` tag came back ``int64``
and a naive ``timestamp[us]`` came back ``timestamp[us, tz=UTC]``.
"""

from __future__ import annotations

import datetime as dt
import decimal
import pathlib
import uuid
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import resolve_context
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError
from orcapod.pipeline import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource
from orcapod.utils import arrow_utils

_T0 = dt.datetime(2026, 3, 24, 17, 54, 17)
_DAYS = [_T0 + dt.timedelta(days=i) for i in range(3)]


def _logical(py_type: type, values: list[Any]) -> pa.Array[Any]:
    """A column of a logical type, built by the type converter."""
    converter = resolve_context(None).type_converter
    table = converter.python_dicts_to_arrow_table(
        [{"v": v} for v in values], python_schema={"v": py_type}
    )
    return table.column("v").combine_chunks()


# One column of three distinct, non-null values per Arrow type.
TAG_COLUMNS: dict[str, Callable[[], pa.Array[Any]]] = {
    "int8": lambda: pa.array([1, 2, 3], pa.int8()),
    "int16": lambda: pa.array([1, 2, 3], pa.int16()),
    "int32": lambda: pa.array([1, 2, 3], pa.int32()),
    "int64": lambda: pa.array([1, 2, 3], pa.int64()),
    "uint8": lambda: pa.array([1, 2, 3], pa.uint8()),
    "uint16": lambda: pa.array([1, 2, 3], pa.uint16()),
    "uint32": lambda: pa.array([1, 2, 3], pa.uint32()),
    "uint64": lambda: pa.array([1, 2, 3], pa.uint64()),
    "float32": lambda: pa.array([1.5, 2.5, 3.5], pa.float32()),
    "float64": lambda: pa.array([1.5, 2.5, 3.5], pa.float64()),
    "bool": lambda: pa.array([True, False, True], pa.bool_()),
    "timestamp[s]": lambda: pa.array(_DAYS, pa.timestamp("s")),
    "timestamp[ms]": lambda: pa.array(_DAYS, pa.timestamp("ms")),
    "timestamp[us]": lambda: pa.array(_DAYS, pa.timestamp("us")),
    "timestamp[ns]": lambda: pa.array(_DAYS, pa.timestamp("ns")),
    "timestamp[us, UTC]": lambda: pa.array(_DAYS, pa.timestamp("us", tz="UTC")),
    "timestamp[us, New_York]": lambda: pa.array(
        _DAYS, pa.timestamp("us", tz="America/New_York")
    ),
    "date32": lambda: pa.array([d.date() for d in _DAYS], pa.date32()),
    "date64": lambda: pa.array([d.date() for d in _DAYS], pa.date64()),
    "time32[s]": lambda: pa.array([dt.time(1), dt.time(2), dt.time(3)], pa.time32("s")),
    "time64[us]": lambda: pa.array(
        [dt.time(1), dt.time(2), dt.time(3)], pa.time64("us")
    ),
    "duration[s]": lambda: pa.array(
        [dt.timedelta(seconds=i) for i in (1, 2, 3)], pa.duration("s")
    ),
    "duration[us]": lambda: pa.array(
        [dt.timedelta(seconds=i) for i in (1, 2, 3)], pa.duration("us")
    ),
    "string": lambda: pa.array(["a", "b", "c"], pa.string()),
    "large_string": lambda: pa.array(["a", "b", "c"], pa.large_string()),
    "binary": lambda: pa.array([b"a", b"b", b"c"], pa.binary()),
    "decimal128(10, 2)": lambda: pa.array(
        [decimal.Decimal(v) for v in ("1.25", "2.50", "3.75")], pa.decimal128(10, 2)
    ),
    "uuid": lambda: _logical(uuid.UUID, [uuid.UUID(int=i) for i in (1, 2, 3)]),
    "Path": lambda: _logical(pathlib.Path, [pathlib.Path(p) for p in "abc"]),
}


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Twice ``x``."""
    return 2 * x


@function_pod(output_keys="z")
def increment(y: int) -> int:
    """``y`` plus one."""
    return y + 1


def _source_table(name: str, data_column: str = "x") -> pa.Table:
    """A three-row table with tag ``k`` of the named type and an ``int64`` data column."""
    k = TAG_COLUMNS[name]()
    schema = pa.schema(
        [
            pa.field("k", k.type, nullable=False),
            pa.field(data_column, pa.int64(), nullable=False),
        ]
    )
    return pa.Table.from_arrays([k, pa.array([1, 2, 3], pa.int64())], schema=schema)


def _expected(name: str) -> pa.DataType:
    """The type a pass-through tag column must have: the large-normalized input type."""
    return arrow_utils.normalize_to_large_types(TAG_COLUMNS[name]().type)


def _new_store(kind: str, tmp_path: pathlib.Path):
    """A store of the given kind (``memory`` or ``delta``); a Delta database under ``tmp_path``."""
    if kind == "delta":
        return DeltaTableDatabase(tmp_path / "store")
    return InMemoryArrowDatabase()


def _run(job: PipelineJob, mode: str) -> None:
    """Run ``job`` with the orchestrator named by ``mode`` (``sync`` or ``async``)."""
    if mode == "async":
        job.run(orchestrator=AsyncPipelineOrchestrator())
    else:
        job.run()


def _build(store, table: pa.Table, *, chained: bool = False) -> PipelineJob:
    """A job: source -> ``double`` (-> ``increment`` when chained)."""
    with PipelineJob(name="types", database=store) as job:
        src = ArrowTableSource(table, tag_columns=["k"], source_id="src")
        out = double.pod(src, label="double")
        if chained:
            increment.pod(out, label="increment")
    return job


# ---------------------------------------------------------------------------
# as_table() after a run: the node's own rows (part A)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("name", list(TAG_COLUMNS))
def test_as_table_after_run_keeps_tag_type(
    name: str, store: str, mode: str, tmp_path: pathlib.Path
) -> None:
    """After a run, the node's ``as_table()`` has the input's tag type."""
    job = _build(_new_store(store, tmp_path), _source_table(name))
    _run(job, mode)

    table = job.nodes["double"].as_table()

    assert table.schema.field("k").type == _expected(name)
    assert table.num_rows == 3


@pytest.mark.parametrize("name", list(TAG_COLUMNS))
def test_pod_stream_outside_a_job_keeps_tag_type(name: str) -> None:
    """The same pod outside a job: its stream's table keeps the tag type too."""
    stream = double.pod(
        ArrowTableSource(_source_table(name), tag_columns=["k"], source_id="src")
    )

    table = stream.as_table()

    assert table.schema.field("k").type == _expected(name)
    assert table.num_rows == 3


@pytest.mark.parametrize("name", ["int64", "uint32", "timestamp[us]", "uuid", "Path"])
def test_as_table_all_info_matches_the_pod_outside_a_job(name: str) -> None:
    """With every column group, the node's table equals the pod's stream outside a job."""
    table = _source_table(name)
    outside = double.pod(
        ArrowTableSource(table, tag_columns=["k"], source_id="src")
    ).as_table(all_info=True)
    job = _build(InMemoryArrowDatabase(), table)
    job.run()

    inside = job.nodes["double"].as_table(all_info=True)

    assert inside.schema == outside.schema
    # A source-info token names the result record, which each computation makes anew.
    source_info = [c for c in inside.column_names if c.startswith("_source_")]
    assert source_info == ["_source_y"]
    assert inside.drop(source_info).equals(outside.drop(source_info))


@pytest.mark.parametrize("name", ["uint32", "timestamp[us]"])
def test_join_after_a_function_node_on_a_typed_tag(name: str) -> None:
    """The ITL-990 case: join a function node's output with another source on the tag."""
    job = _build(InMemoryArrowDatabase(), _source_table(name))
    job.run()
    other = ArrowTableSource(_source_table(name, "w"), tag_columns=["k"], source_id="o")

    joined = job.nodes["double"].join(other).as_table()

    assert joined.num_rows == 3
    assert joined.schema.field("k").type == _expected(name)


def test_as_table_mixing_stored_and_new_rows_keeps_tag_type() -> None:
    """Rows served from the database and rows computed now make one correctly typed table."""
    store = InMemoryArrowDatabase()
    first = _build(store, _source_table("uint32").slice(0, 2))
    first.run()
    second = _build(store, _source_table("uint32"))
    second.run()

    table = second.nodes["double"].as_table()

    assert table.num_rows == 3
    assert table.schema.field("k").type == pa.uint32()
    assert sorted(table.column("k").to_pylist()) == [1, 2, 3]


# ---------------------------------------------------------------------------
# Reads from the database: get_all_records() and a fresh job (part B on Delta)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("store", ["memory", "delta"])
@pytest.mark.parametrize("name", list(TAG_COLUMNS))
def test_store_read_back_keeps_tag_type(
    name: str, store: str, tmp_path: pathlib.Path
) -> None:
    """Reads from the database (``get_all_records``, a fresh job's run) keep the tag type."""
    database = _new_store(store, tmp_path)
    table = _source_table(name)
    _build(database, table).run()
    if store == "delta":
        # A new store object sees the committed tables, as a new process would.
        database = _new_store(store, tmp_path)

    fresh_job = _build(database, table)
    fresh = fresh_job.nodes["double"]

    records = fresh.get_all_records()
    assert records is not None
    assert records.schema.field("k").type == _expected(name)
    # The node shows rows once its run has read them from the database (ITL-1095).
    fresh_job.run()
    tag, _ = next(iter(fresh.iter_data()))
    assert tag.as_table().schema.field("k").type == _expected(name)
    assert fresh.as_table().schema.field("k").type == _expected(name)


def _as_written_before_the_fix(root: pathlib.Path) -> None:
    """Rewrite every Delta table under ``root`` as a build before ITL-928 left it.

    Such a build stored a ``uint32`` column as Delta's ``int32`` with no recorded type.
    """
    import deltalake

    # The recorded-type key is what this test removes; it is not public API.
    from orcapod.databases.delta_arrow_types import ARROW_TYPE_KEY

    for log in root.rglob("_delta_log"):
        table = deltalake.DeltaTable(str(log.parent)).to_pyarrow_table()
        fields = [
            f.with_metadata(
                {k: v for k, v in (f.metadata or {}).items() if k != ARROW_TYPE_KEY}
            )
            for f in table.schema
        ]
        deltalake.write_deltalake(
            str(log.parent),
            pa.Table.from_arrays(table.columns, schema=pa.schema(fields)),
            mode="overwrite",
            schema_mode="overwrite",
        )


def test_new_rows_over_a_store_written_before_the_fix_raise_naming_the_column(
    tmp_path: pathlib.Path,
) -> None:
    """Rows of one node never make a column of a type none of them has.

    A Delta database written before ITL-928 gives a ``uint32`` tag back as ``int32``:
    rows served from it and rows computed now disagree, and the node says so instead
    of picking a type. Such a database must be rebuilt.
    """
    _build(DeltaTableDatabase(tmp_path), _source_table("uint32").slice(0, 2)).run()
    _as_written_before_the_fix(tmp_path)
    job = _build(DeltaTableDatabase(tmp_path), _source_table("uint32"))
    job.run()

    with pytest.raises(InputValidationError) as raised:
        job.nodes["double"].as_table()

    message = str(raised.value)
    assert "'k'" in message
    assert "int32" in message
    assert "uint32" in message


def test_new_rows_over_a_store_written_after_the_fix_keep_the_tag_type(
    tmp_path: pathlib.Path,
) -> None:
    """The same run over a database this build wrote: one table, typed as the input."""
    _build(DeltaTableDatabase(tmp_path), _source_table("uint32").slice(0, 2)).run()
    job = _build(DeltaTableDatabase(tmp_path), _source_table("uint32"))
    job.run()

    table = job.nodes["double"].as_table()

    assert table.num_rows == 3
    assert table.schema.field("k").type == pa.uint32()


def test_a_column_first_seen_after_row_one_is_still_checked() -> None:
    """Two later rows that disagree on a column row one lacks still raise, naming it."""
    # About the private helper itself: no public input gives row one fewer columns.
    from orcapod.core.nodes.function_node import _concat_row_tables

    rows = [
        pa.table({"a": pa.array([1], pa.int64())}),
        pa.table({"a": pa.array([2], pa.int64()), "k": pa.array([1], pa.int8())}),
        pa.table({"a": pa.array([3], pa.int64()), "k": pa.array([1], pa.int16())}),
    ]

    with pytest.raises(InputValidationError, match="'k': int8 and int16"):
        _concat_row_tables(rows)


def test_rows_in_two_layouts_of_one_type_concatenate() -> None:
    """``string`` and ``large_string`` rows are one type: they make one large column."""
    # About the private helper itself: a node's rows rarely differ only in layout.
    from orcapod.core.nodes.function_node import _concat_row_tables

    rows = [
        pa.table({"k": pa.array(["a"], pa.string())}),
        pa.table({"k": pa.array(["b"], pa.large_string())}),
    ]

    table = _concat_row_tables(rows)

    assert table.schema.field("k").type == pa.large_string()
    assert table.column("k").to_pylist() == ["a", "b"]


def test_a_null_typed_row_is_compared_as_null() -> None:
    """Large normalization keeps a ``null`` column ``null``: the error names it so."""
    # About the private helper itself: Orcapod rows seldom carry a null-typed column.
    from orcapod.core.nodes.function_node import _concat_row_tables

    rows = [
        pa.table({"k": pa.array([None], pa.null())}),
        pa.table({"k": pa.array([1], pa.int64())}),
    ]

    with pytest.raises(InputValidationError, match="'k': null and int64"):
        _concat_row_tables(rows)
