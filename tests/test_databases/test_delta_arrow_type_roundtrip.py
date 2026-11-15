"""The Delta database gives back the Arrow types it was given (ITL-928, part B).

Delta Lake has no unsigned integer, nanosecond, second or millisecond timestamp, or
zoned (other than UTC) timestamp type, and the Delta writer converts such columns. A
read must still return the column's written type (large-normalized) and values: a tag
read back with another type changes the record ids computed from it, so a re-run
writes its pipeline rows a second time.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
import pathlib
import subprocess
import sys
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources import CachedSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import InputValidationError
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode
from orcapod.utils import arrow_utils


_T = [
    dt.datetime(2026, 3, 24, 17, 54, 17, 123456) + dt.timedelta(days=i)
    for i in range(3)
]

# Column name -> three values of that type, chosen within what the type holds.
COLUMNS: dict[str, pa.Array[Any]] = {
    "int8": pa.array([-1, 0, 1], pa.int8()),
    "int64": pa.array([-1, 0, 1], pa.int64()),
    "uint8": pa.array([0, 1, 2], pa.uint8()),
    "uint16": pa.array([0, 1, 2], pa.uint16()),
    "uint32": pa.array([0, 1, 2], pa.uint32()),
    "uint64": pa.array([0, 1, 2], pa.uint64()),
    "float32": pa.array([0.5, 1.5, 2.5], pa.float32()),
    "bool": pa.array([True, False, True], pa.bool_()),
    "timestamp[s]": pa.array([t.replace(microsecond=0) for t in _T], pa.timestamp("s")),
    "timestamp[ms]": pa.array(
        [t.replace(microsecond=123000) for t in _T], pa.timestamp("ms")
    ),
    "timestamp[us]": pa.array(_T, pa.timestamp("us")),
    "timestamp[us, UTC]": pa.array(_T, pa.timestamp("us", tz="UTC")),
    "timestamp[us, New_York]": pa.array(_T, pa.timestamp("us", tz="America/New_York")),
    "date32": pa.array([t.date() for t in _T], pa.date32()),
    "large_string": pa.array(["a", "b", "c"], pa.large_string()),
    "large_binary": pa.array([b"a", b"b", b"c"], pa.large_binary()),
    "decimal128(10, 2)": pa.array(
        [decimal.Decimal(v) for v in ("1.25", "2.50", "3.75")], pa.decimal128(10, 2)
    ),
    "large_list<uint32>": pa.array([[0], [1, 2], []], pa.large_list(pa.uint32())),
    "struct<u: uint16, t: timestamp[ms]>": pa.array(
        [{"u": i, "t": _T[i].replace(microsecond=0)} for i in range(3)],
        pa.struct([("u", pa.uint16()), ("t", pa.timestamp("ms"))]),
    ),
}


def _written_and_read(tmp_path: pathlib.Path, column: pa.Array[Any]) -> pa.Table:
    """Write one three-row column to a Delta database and read it back from a new store object."""
    DeltaTableDatabase(tmp_path).add_records(
        ("t",),
        pa.table({"id": pa.array([b"a", b"b", b"c"], pa.large_binary()), "c": column}),
        record_id_column="id",
        flush=True,
    )
    read = DeltaTableDatabase(tmp_path).get_all_records(("t",), record_id_column="id")
    assert read is not None
    return read.sort_by("id")


@pytest.mark.parametrize("name", list(COLUMNS))
def test_a_read_returns_the_written_type_and_values(
    name: str, tmp_path: pathlib.Path
) -> None:
    """A read gives back the written type (large-normalized) and values."""
    column = COLUMNS[name]

    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == arrow_utils.normalize_to_large_types(
        column.type
    )
    assert read.column("c").to_pylist() == column.to_pylist()


@pytest.mark.parametrize(
    "column",
    [
        pa.array([0, 2**31, 2**32 - 1], pa.uint32()),
        pa.array([0, 2**63, 2**64 - 1], pa.uint64()),
    ],
    ids=["uint32", "uint64"],
)
def test_an_unsigned_value_above_the_signed_range_round_trips(
    column: pa.Array[Any], tmp_path: pathlib.Path
) -> None:
    """An unsigned value above the signed maximum is stored and read back exactly."""
    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == column.type
    assert read.column("c").to_pylist() == column.to_pylist()


def test_nanosecond_timestamps_keep_their_digits(tmp_path: pathlib.Path) -> None:
    """``timestamp[ns]`` keeps its sub-microsecond digits."""
    column = pa.array(
        [1_700_000_000_123_456_789, 1, 1_700_000_000_000_000_001], pa.timestamp("ns")
    )

    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == pa.timestamp("ns")
    assert read.column("c").to_pylist() == column.to_pylist()


@pytest.mark.parametrize(
    "column",
    [
        pa.array([dt.date(2026, 1, i) for i in (1, 2, 3)], pa.date64()),
        pa.array([dt.time(i) for i in (1, 2, 3)], pa.time32("s")),
        pa.array([dt.time(i) for i in (1, 2, 3)], pa.time64("us")),
        pa.array([dt.timedelta(seconds=i) for i in (1, 2, 3)], pa.duration("us")),
    ],
    ids=["date64", "time32[s]", "time64[us]", "duration[us]"],
)
def test_types_delta_has_no_equivalent_for_are_stored(
    column: pa.Array[Any], tmp_path: pathlib.Path
) -> None:
    """``date64``, ``time`` and ``duration`` columns are stored and read back."""
    read = _written_and_read(tmp_path, column)

    assert read.schema.field("c").type == column.type
    assert read.column("c").to_pylist() == column.to_pylist()


# ---------------------------------------------------------------------------
# Through a job: re-runs, operator replay, cached sources, a loaded job
# ---------------------------------------------------------------------------


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Twice ``x``."""
    return 2 * x


@function_pod(output_keys="z")
def increment(y: int) -> int:
    """``y`` plus one."""
    return y + 1


def _source_table(k: pa.Array[Any]) -> pa.Table:
    """A three-row table with tag ``k`` (the given column) and an ``int64`` column ``x``."""
    schema = pa.schema(
        [
            pa.field("k", k.type, nullable=False),
            pa.field("x", pa.int64(), nullable=False),
        ]
    )
    return pa.Table.from_arrays([k, pa.array([1, 2, 3], pa.int64())], schema=schema)


TAGS = {
    "int8": pa.array([1, 2, 3], pa.int8()),
    "uint32": pa.array([1, 2, 3], pa.uint32()),
    "timestamp[s]": pa.array(_T, pa.timestamp("s")),
    "timestamp[ns]": pa.array(_T, pa.timestamp("ns")),
    "timestamp[us, New_York]": pa.array(_T, pa.timestamp("us", tz="America/New_York")),
}


def _chained_job(root: pathlib.Path, table: pa.Table) -> PipelineJob:
    """A job on a Delta database at ``root``: source -> ``double`` -> ``increment``."""
    with PipelineJob(name="j", database=DeltaTableDatabase(root)) as job:
        src = ArrowTableSource(table, tag_columns=["k"], source_id="src")
        increment.pod(double.pod(src, label="double"), label="increment")
    return job


@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize("name", list(TAGS))
def test_a_rerun_on_the_same_store_writes_no_pipeline_rows(
    name: str, mode: str, tmp_path: pathlib.Path
) -> None:
    """The ITL-990 case: source -> pod -> pod, run twice; a fresh job's run shows 3 rows
    and the database holds 3 records per node (a node shows rows only after a run,
    ITL-1095)."""
    from orcapod.pipeline import AsyncPipelineOrchestrator

    table = _source_table(TAGS[name])
    for _ in range(2):
        job = _chained_job(tmp_path, table)
        if mode == "async":
            job.run(orchestrator=AsyncPipelineOrchestrator())
        else:
            job.run()

    fresh = _chained_job(tmp_path, table)
    fresh.run()
    for label in ("double", "increment"):
        node = fresh.nodes[label]
        assert node.as_table().num_rows == 3
        assert node.as_table().schema.field("k").type == TAGS[name].type
        records = node.get_all_records(all_info=True)
        assert records is not None
        assert records.num_rows == 3


@pytest.mark.parametrize("name", list(TAGS))
def test_a_fresh_job_reads_the_tag_type(name: str, tmp_path: pathlib.Path) -> None:
    """A new job on the same database, run, reads the tag type through every read of a
    node (a node shows rows only after a run, ITL-1095)."""
    table = _source_table(TAGS[name])
    _chained_job(tmp_path, table).run()

    fresh_job = _chained_job(tmp_path, table)
    fresh_job.run()
    fresh = fresh_job.nodes["double"]

    assert fresh.as_table().schema.field("k").type == TAGS[name].type
    tag, _ = next(iter(fresh.iter_data()))
    assert tag.as_table().schema.field("k").type == TAGS[name].type
    records = fresh.get_all_records()
    assert records is not None
    assert records.schema.field("k").type == TAGS[name].type


# Run in a child process by ``test_a_loaded_job_reads_the_tag_type``: load the job
# saved under ``argv[1]`` and print each function node's stored tag type as JSON (a
# loaded node is empty until it runs, ITL-1095: its stored rows are its stored view).
_READ_LOADED_TAG_TYPES = """
import json, pathlib, sys
from orcapod import PipelineJob
from orcapod.databases import DeltaTableDatabase
root = pathlib.Path(sys.argv[1])
job = PipelineJob.load(root / "job.json", database=DeltaTableDatabase(root / "store"))
print(json.dumps({
    label: str(job.nodes[label].as_source().as_table().schema.field("k").type)
    for label in ("double", "increment")
}))
"""


@pytest.mark.parametrize("name", list(TAGS))
def test_a_loaded_job_reads_the_tag_type(name: str, tmp_path: pathlib.Path) -> None:
    """A job saved after its run and loaded in a new process reads the tag type.

    The load runs in a child process, so nothing of the run (pods, sources, store
    objects) can stand in for what the saved job and the database hold.
    """
    job = _chained_job(tmp_path / "store", _source_table(TAGS[name]))
    job.run()
    job.save(tmp_path / "job.json")

    read = subprocess.run(
        [sys.executable, "-c", _READ_LOADED_TAG_TYPES, str(tmp_path)],
        capture_output=True,
        text=True,
        check=True,
    )

    assert json.loads(read.stdout.strip().splitlines()[-1]) == {
        label: str(TAGS[name].type) for label in ("double", "increment")
    }


@pytest.mark.parametrize("name", list(TAGS))
def test_an_operator_replay_reads_the_tag_type(
    name: str, tmp_path: pathlib.Path
) -> None:
    """An operator node's ``REPLAY`` of its logged output reads the tag type."""
    table = pa.Table.from_arrays(
        [TAGS[name], pa.array([1, 2, 3], pa.int64()), pa.array([4, 5, 6], pa.int64())],
        schema=pa.schema(
            [
                pa.field("k", TAGS[name].type, nullable=False),
                pa.field("x", pa.int64(), nullable=False),
                pa.field("w", pa.int64(), nullable=False),
            ]
        ),
    )

    def build(mode: CacheMode) -> PipelineJob:
        """A job with operator node ``sel`` in the given cache mode."""
        store = DeltaTableDatabase(tmp_path)
        with PipelineJob(name="o", database=store) as job:
            src = ArrowTableSource(table, tag_columns=["k"], source_id="src")
            SelectDataColumns(["x"])(src, label="sel")
        # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at("o"), cache_mode=mode
        )
        return job

    build(CacheMode.LOG).run()

    replay_job = build(CacheMode.REPLAY)
    replay_job.run()  # a node shows rows only after a run (ITL-1095)
    replayed = replay_job.nodes["sel"].as_table()

    assert replayed.schema.field("k").type == TAGS[name].type


@pytest.mark.parametrize("name", list(TAGS))
def test_a_cached_source_reads_the_tag_type(name: str, tmp_path: pathlib.Path) -> None:
    """A ``CachedSource`` served from its Delta cache reads the tag type."""
    table = _source_table(TAGS[name])

    def cached() -> CachedSource:
        """A ``CachedSource`` of the table, cached in a new Delta database object."""
        return CachedSource(
            ArrowTableSource(table, tag_columns=["k"], source_id="src"),
            cache_database=DeltaTableDatabase(tmp_path),
        )

    cached().as_table()

    assert cached().as_table().schema.field("k").type == TAGS[name].type


@pytest.mark.parametrize(
    "name",
    [
        "uint32",
        "timestamp[ms]",
        "timestamp[us, New_York]",
        "large_list<uint32>",
        "struct<u: uint16, t: timestamp[ms]>",
    ],
)
def test_appends_and_merges_keep_the_type(name: str, tmp_path: pathlib.Path) -> None:
    """A second commit (append) and a third with a stored id (merge) keep the type."""
    column = COLUMNS[name]
    ids = pa.array([b"a", b"b", b"c"], pa.large_binary())
    rows = pa.table({"id": ids, "c": column})
    store = DeltaTableDatabase(tmp_path)
    store.add_records(("t",), rows.slice(0, 1), record_id_column="id", flush=True)
    store.add_records(("t",), rows.slice(1, 1), record_id_column="id", flush=True)
    # Holds an id already stored, so the commit is a MERGE.
    store.add_records(
        ("t",),
        rows.slice(1, 2),
        record_id_column="id",
        skip_duplicates=True,
        flush=True,
    )

    for reader in (store, DeltaTableDatabase(tmp_path)):
        read = reader.get_all_records(("t",), record_id_column="id")
        assert read is not None
        read = read.sort_by("id")
        assert read.schema.field("c").type == arrow_utils.normalize_to_large_types(
            column.type
        )
        assert read.column("c").to_pylist() == column.to_pylist()
        one = reader.get_record_by_id(("t",), b"b", record_id_column="id")
        assert one is not None
        assert one.schema.field("c").type == read.schema.field("c").type


def test_nanoseconds_cannot_be_appended_to_a_table_written_before_the_fix(
    tmp_path: pathlib.Path,
) -> None:
    """A build before ITL-928 stored ``timestamp[ns]`` as ``timestamp[us]``.

    The integer form this build writes differs from that stored type, so the append
    is refused (nothing is written) and the database must be rebuilt.
    """
    from deltalake import write_deltalake

    column = pa.array([1_700_000_000_123_456_000], pa.timestamp("ns"))
    write_deltalake(
        str(tmp_path / "t"),
        pa.table({"__record_id": pa.array([b"a"], pa.large_binary()), "c": column}),
    )
    store = DeltaTableDatabase(tmp_path)
    store.add_records(
        ("t",),
        pa.table({"id": pa.array([b"b"], pa.large_binary()), "c": column}),
        record_id_column="id",
    )

    with pytest.raises(InputValidationError, match="'c'"):
        store.flush()


@pytest.mark.parametrize(
    ("stored", "incoming"),
    [
        (pa.array([1], pa.uint32()), pa.array([2], pa.int32())),
        (pa.array([1], pa.timestamp("ns")), pa.array([2], pa.int64())),
        (pa.array([1], pa.int64()), pa.array([2], pa.timestamp("ns"))),
        (pa.array([1], pa.timestamp("ms")), pa.array([2], pa.timestamp("us"))),
    ],
    ids=["uint32<-int32", "ns<-int64", "int64<-ns", "ms<-us"],
)
def test_an_append_of_another_type_is_refused(
    stored: pa.Array[Any], incoming: pa.Array[Any], tmp_path: pathlib.Path
) -> None:
    """Written in the same Delta form, two types are still two types."""
    store = DeltaTableDatabase(tmp_path)
    store.add_records(
        ("t",),
        pa.table({"id": pa.array([b"a"], pa.large_binary()), "c": stored}),
        record_id_column="id",
        flush=True,
    )
    store.add_records(
        ("t",),
        pa.table({"id": pa.array([b"b"], pa.large_binary()), "c": incoming}),
        record_id_column="id",
    )

    with pytest.raises(InputValidationError, match="'c'"):
        store.flush()


def test_unsigned_rows_append_to_a_signed_column_without_a_recorded_type(
    tmp_path: pathlib.Path,
) -> None:
    """A column with no recorded type may hold an unsigned column a build before
    ITL-928 wrote (Delta's ``int32`` for ``uint32``): appending to it is not refused,
    as before. Such a database must be rebuilt (developer, November 12, 2026)."""
    store = DeltaTableDatabase(tmp_path)
    store.add_records(
        ("t",),
        pa.table(
            {"id": pa.array([b"a"], pa.large_binary()), "c": pa.array([1], pa.int32())}
        ),
        record_id_column="id",
        flush=True,
    )
    store.add_records(
        ("t",),
        pa.table(
            {"id": pa.array([b"b"], pa.large_binary()), "c": pa.array([2], pa.uint32())}
        ),
        record_id_column="id",
        flush=True,
    )

    read = DeltaTableDatabase(tmp_path).get_all_records(("t",), record_id_column="id")
    assert read is not None
    assert read.schema.field("c").type == pa.int32()
    assert read.sort_by("id").column("c").to_pylist() == [1, 2]


def test_an_unsigned_value_beyond_an_unrecorded_signed_column_is_refused(
    tmp_path: pathlib.Path,
) -> None:
    """Into a column with no recorded type, only values the stored type holds go.

    Such a column may hold what a build before ITL-928 wrote for ``uint32``, Delta's
    ``int32``. A value above its maximum would be written as a negative number, so the
    append is refused and nothing is written.
    """
    from deltalake import write_deltalake

    write_deltalake(
        str(tmp_path / "t"),
        pa.table(
            {
                "__record_id": pa.array([b"a"], pa.large_binary()),
                "c": pa.array([1], pa.int32()),
            }
        ),
    )
    store = DeltaTableDatabase(tmp_path)
    store.add_records(
        ("t",),
        pa.table(
            {
                "id": pa.array([b"b"], pa.large_binary()),
                "c": pa.array([3_000_000_000], pa.uint32()),
            }
        ),
        record_id_column="id",
    )

    with pytest.raises(InputValidationError, match="'c'"):
        store.flush()
    read = DeltaTableDatabase(tmp_path).get_all_records(("t",))
    assert read is not None
    assert read.column("c").to_pylist() == [1]
