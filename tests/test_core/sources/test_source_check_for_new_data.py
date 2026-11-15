"""What each table-backed source does when a run checks it for new data (ITL-1102).

``check_for_new_data()`` is called once per source at the start of every run. A source
that finds new data takes it in and moves its modified time; one that finds nothing
moves nothing. Tested through the source and through ``PipelineJob.run()`` on both
orchestrators.
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.databases.spiraldb_nullability import write_nullability_record
from orcapod.databases.sqlite_connector import SQLiteConnector
from orcapod.errors import SourceCheckFailedWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import (
    CachedSource,
    CSVSource,
    DBTableSource,
    DeltaTableSource,
    DerivedSource,
    PostgreSQLTableSource,
    SourceProxy,
    SpiralDBTableSource,
    SQLiteTableSource,
)
from orcapod.types import ColumnInfo
from tests.test_databases import fake_pyspiral

CALLS: list[int] = []
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    CALLS.append(x)
    return x + 1


@pytest.fixture(autouse=True)
def _reset() -> None:
    CALLS.clear()


_SCHEMA = pa.schema(
    [pa.field("k", pa.int64(), False), pa.field("x", pa.int64(), False)]
)


def _rows(keys: list[int]) -> pa.Table:
    return pa.table(
        {
            "k": pa.array(keys, pa.int64()),
            "x": pa.array([10 * k for k in keys], pa.int64()),
        },
        schema=_SCHEMA,
    )


def _job(source: Any) -> PipelineJob:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        inc.pod(source, label="rowwise")
    return job


def _run(job: PipelineJob, orchestrator: str) -> Any:
    return job.run(orchestrator=ORCHESTRATORS[orchestrator]())


def _values(source: Any, column: str) -> list[int]:
    return sorted(
        v for v in source.as_table().column(column).to_pylist() if v is not None
    )


def _keys(source: Any) -> list[int]:
    return _values(source, "k")


def _ys(job: PipelineJob) -> list[int]:
    return sorted(r["y"] for r in job.nodes["rowwise"].as_table().to_pylist())


# ---------------------------------------------------------------------------
# DeltaTableSource
# ---------------------------------------------------------------------------


@pytest.fixture()
def delta_path(tmp_path: Path) -> Path:
    path = tmp_path / "table"
    deltalake.write_deltalake(str(path), _rows([0, 1, 2]))
    return path


def _delta(path: Path) -> DeltaTableSource:
    return DeltaTableSource(path, tag_columns="k", record_id_column="k")


def test_delta_unchanged_finds_nothing(delta_path: Path) -> None:
    source = _delta(delta_path)
    before = source.last_modified
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_delta_appended_rows_are_taken_in(delta_path: Path) -> None:
    source = _delta(delta_path)
    before = source.last_modified
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert source.check_for_new_data() is True
    assert source.last_modified is not None and before is not None
    assert source.last_modified > before
    assert _keys(source) == [0, 1, 2, 3]


def test_delta_new_version_with_the_same_rows_changes_nothing(
    delta_path: Path,
) -> None:
    """A version that only compacts files (OPTIMIZE) holds the same rows."""
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    source = _delta(delta_path)
    before = source.last_modified
    deltalake.DeltaTable(str(delta_path)).optimize.compact()
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_delta_rows_with_nan_read_again_change_nothing(tmp_path: Path) -> None:
    """NaN equals NaN when rows are compared: a NaN must not make every run a change."""
    path = tmp_path / "floats"
    schema = pa.schema(
        [pa.field("k", pa.int64(), False), pa.field("x", pa.float64(), False)]
    )
    rows = pa.table({"k": [0, 1], "x": [float("nan"), 1.5]}, schema=schema)
    deltalake.write_deltalake(str(path), rows)
    # A second file, so that compaction has something to do.
    deltalake.write_deltalake(
        str(path), pa.table({"k": [2], "x": [2.5]}, schema=schema), mode="append"
    )
    source = DeltaTableSource(path, tag_columns="k", record_id_column="k")
    before = source.last_modified
    deltalake.DeltaTable(str(path)).optimize.compact()  # a new version, the same rows
    assert source.check_for_new_data() is False
    assert source.last_modified == before
    deltalake.write_deltalake(
        str(path),
        pa.table({"k": [3], "x": [float("nan")]}, schema=schema),
        mode="append",
    )
    assert source.check_for_new_data() is True


def test_delta_changed_columns_fail_the_check(delta_path: Path) -> None:
    source = _delta(delta_path)
    wider = _rows([3]).append_column(
        pa.field("z", pa.int64(), False), pa.array([1], pa.int64())
    )
    deltalake.write_deltalake(
        str(delta_path), wider, mode="append", schema_mode="merge"
    )
    with pytest.raises(ValueError, match="changed its columns"):
        source.check_for_new_data()
    assert _keys(source) == [0, 1, 2]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_delta_job_runs_only_the_new_rows(delta_path: Path, orchestrator: str) -> None:
    job = _job(_delta(delta_path))
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    assert CALLS == []
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    summary = _run(job, orchestrator)
    assert CALLS == [30]
    assert summary.nodes["rowwise"].computed == 1
    assert _ys(job) == [1, 11, 21, 31]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_delta_failed_check_warns_and_runs_on_held_rows(
    delta_path: Path, orchestrator: str
) -> None:
    job = _job(_delta(delta_path))
    _run(job, orchestrator)
    CALLS.clear()
    wider = _rows([3]).append_column(
        pa.field("z", pa.int64(), False), pa.array([1], pa.int64())
    )
    deltalake.write_deltalake(
        str(delta_path), wider, mode="append", schema_mode="merge"
    )
    with pytest.warns(SourceCheckFailedWarning, match="changed its columns"):
        summary = _run(job, orchestrator)
    assert CALLS == []
    assert summary.unchecked_sources == ("table",)
    assert _ys(job) == [1, 11, 21]


# ---------------------------------------------------------------------------
# SpiralDBTableSource (disk-backed pyspiral fake; no live SpiralDB)
# ---------------------------------------------------------------------------

_PROJECT = "fake-project"
_KEYS = pa.schema([pa.field("k", pa.int64())])


@pytest.fixture()
def spiral(tmp_path: Path):
    """A fake project with table ``default.t`` holding k = 0, 1, 2."""
    with fake_pyspiral.install(tmp_path / "spiral"):
        project = fake_pyspiral.FakeSpiral().project(_PROJECT)
        table = project.create_table("default.t", key_schema=_KEYS)
        table.write(_rows([0, 1, 2]))
        # SpiralDB reports every column nullable; the record says k and x are not.
        write_nullability_record(table, _SCHEMA)
        yield project


def _spiral(**kwargs: Any) -> SpiralDBTableSource:
    return SpiralDBTableSource(project_id=_PROJECT, table_name="t", **kwargs)


def test_spiral_unchanged_finds_nothing(spiral: Any) -> None:
    source = _spiral()
    before = source.last_modified
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_spiral_new_version_is_pinned_without_reading_rows(spiral: Any) -> None:
    source = _spiral(max_rows=2)  # the table holds more rows than one read may return
    before = source.last_modified
    spiral.table("default.t").write(_rows([3]))
    scans = len(fake_pyspiral.scan_log)
    assert source.check_for_new_data() is True  # no SourceTooLargeError: no row read
    assert len(fake_pyspiral.scan_log) == scans
    assert source.last_modified is not None and before is not None
    assert source.last_modified > before
    assert source.check_for_new_data() is False


def test_spiral_reads_the_new_version_after_the_check(spiral: Any) -> None:
    source = _spiral()
    spiral.table("default.t").write(_rows([3]))
    assert source.check_for_new_data() is True
    _ = source.as_table()
    assert (
        fake_pyspiral.scan_log[-1].kwargs["asof"]
        == spiral.table("default.t").last_modified_at()
    )


def test_spiral_explicit_asof_never_moves(spiral: Any) -> None:
    version = spiral.table("default.t").last_modified_at()
    source = _spiral(asof=version)
    before = source.last_modified
    spiral.table("default.t").write(_rows([3]))
    assert source.check_for_new_data() is False
    assert source.last_modified == before
    _ = source.as_table()
    assert fake_pyspiral.scan_log[-1].kwargs["asof"] == version


def test_spiral_recreated_table_fails_the_check(spiral: Any) -> None:
    source = _spiral()
    spiral.drop_table("default.t")
    recreated = spiral.create_table("default.t", key_schema=_KEYS)
    recreated.write(_rows([0, 1, 2, 3]))
    write_nullability_record(recreated, _SCHEMA)
    with pytest.raises(ValueError, match="dropped and created again"):
        source.check_for_new_data()


def test_spiral_changed_columns_fail_the_check(spiral: Any) -> None:
    source = _spiral()
    # Recording x as nullable changes the schema the source reads.
    write_nullability_record(
        spiral.table("default.t"),
        pa.schema([pa.field("k", pa.int64(), False), pa.field("x", pa.int64(), True)]),
    )
    with pytest.raises(ValueError, match="Build the source again"):
        source.check_for_new_data()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_spiral_job_runs_only_the_new_rows(spiral: Any, orchestrator: str) -> None:
    job = _job(_spiral())
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    assert CALLS == []
    spiral.table("default.t").write(_rows([3]))
    summary = _run(job, orchestrator)
    assert CALLS == [30]  # keyed record ids: the other rows' entries are stored
    assert summary.nodes["rowwise"].computed == 1
    assert _ys(job) == [1, 11, 21, 31]


# ---------------------------------------------------------------------------
# CSVSource
# ---------------------------------------------------------------------------


def _write_csv(path: Path, keys: list[int]) -> None:
    lines = ["k,x", *(f"{k},{10 * k}" for k in keys)]
    path.write_text("\n".join(lines) + "\n")


@pytest.fixture()
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "rows.csv"
    _write_csv(path, [0, 1, 2])
    return path


def _csv(path: Path) -> CSVSource:
    return CSVSource(str(path), tag_columns="k", record_id_column="k")


def test_csv_unchanged_finds_nothing(csv_path: Path) -> None:
    source = _csv(csv_path)
    before = source.last_modified
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_csv_touched_or_reordered_with_the_same_rows_changes_nothing(
    csv_path: Path,
) -> None:
    source = _csv(csv_path)
    before = source.last_modified
    os.utime(csv_path, ns=(time.time_ns(), time.time_ns() + 10**9))
    assert source.check_for_new_data() is False
    _write_csv(csv_path, [2, 0, 1])  # the same rows in another order
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_csv_appended_rows_are_taken_in(csv_path: Path) -> None:
    source = _csv(csv_path)
    _write_csv(csv_path, [0, 1, 2, 3])
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]
    assert source.check_for_new_data() is False


def test_csv_changed_columns_fail_the_check(csv_path: Path) -> None:
    source = _csv(csv_path)
    csv_path.write_text("k,x,z\n0,0,1\n")
    with pytest.raises(ValueError, match="changed its columns"):
        source.check_for_new_data()
    assert _keys(source) == [0, 1, 2]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_csv_job_runs_only_the_new_rows(csv_path: Path, orchestrator: str) -> None:
    job = _job(_csv(csv_path))
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    _write_csv(csv_path, [0, 1, 2, 3])
    _run(job, orchestrator)
    assert CALLS == [30]
    assert _ys(job) == [1, 11, 21, 31]


# ---------------------------------------------------------------------------
# SQLiteTableSource and DBTableSource
# ---------------------------------------------------------------------------


@pytest.fixture()
def sqlite_path(tmp_path: Path) -> Path:
    path = tmp_path / "data.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE t (k INTEGER PRIMARY KEY NOT NULL, x INTEGER NOT NULL)"
        )
        conn.executemany("INSERT INTO t VALUES (?, ?)", [(k, 10 * k) for k in range(3)])
    return path


def _insert(path: Path, k: int) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute("INSERT INTO t VALUES (?, ?)", (k, 10 * k))


def test_sqlite_unchanged_finds_nothing(sqlite_path: Path) -> None:
    source = SQLiteTableSource(sqlite_path, "t", record_id_column="k")
    before = source.last_modified
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def test_sqlite_new_rows_are_taken_in(sqlite_path: Path) -> None:
    source = SQLiteTableSource(sqlite_path, "t", record_id_column="k")
    _insert(sqlite_path, 3)
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]


def test_sqlite_dropped_table_fails_the_check(sqlite_path: Path) -> None:
    source = SQLiteTableSource(sqlite_path, "t", record_id_column="k")
    with sqlite3.connect(sqlite_path) as conn:
        conn.execute("DROP TABLE t")
    with pytest.raises(ValueError, match="not found"):
        source.check_for_new_data()
    assert _keys(source) == [0, 1, 2]


def test_db_table_source_reads_again_through_its_connector() -> None:
    connector = SQLiteConnector(":memory:")
    connector.create_table_if_not_exists(
        "t",
        [ColumnInfo("k", pa.int64(), False), ColumnInfo("x", pa.int64(), False)],
        pk_column="k",
    )
    connector.upsert_records("t", _rows([0, 1, 2]), id_column="k")
    source = DBTableSource(connector, "t", record_id_column="k")
    connector.upsert_records("t", _rows([3]), id_column="k")
    # A DBTableSource reads through the connector it was given, which is still open.
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_sqlite_job_runs_only_the_new_rows(
    sqlite_path: Path, orchestrator: str
) -> None:
    job = _job(SQLiteTableSource(sqlite_path, "t", record_id_column="k"))
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    _insert(sqlite_path, 3)
    _run(job, orchestrator)
    assert CALLS == [30]
    assert _ys(job) == [1, 11, 21, 31]


# ---------------------------------------------------------------------------
# PostgreSQLTableSource (connector mocked; no server)
# ---------------------------------------------------------------------------

_PG_PATCH = "orcapod.core.sources.postgresql_table_source.PostgreSQLConnector"


def _pg_connector(keys: list[int]) -> MagicMock:
    connector = MagicMock()
    connector.get_table_names.return_value = ["t"]
    connector.get_pk_columns.return_value = ["k"]
    connector.get_column_info.return_value = [
        ColumnInfo("k", pa.int64(), False),
        ColumnInfo("x", pa.int64(), False),
    ]
    connector.iter_batches.return_value = iter(_rows(keys).to_batches())
    return connector


def test_postgresql_reads_again_and_compares_in_any_order() -> None:
    with patch(_PG_PATCH) as connector_class:
        connector_class.return_value = _pg_connector([0, 1, 2])
        source = PostgreSQLTableSource("dsn", "t", record_id_column="k")
        before = source.last_modified
        connector_class.return_value = _pg_connector([2, 0, 1])
        assert source.check_for_new_data() is False
        assert source.last_modified == before
        connector_class.return_value = _pg_connector([0, 1, 2, 3])
        assert source.check_for_new_data() is True
        connector_class.return_value.close.assert_called_once()
    assert _keys(source) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# CachedSource, DerivedSource, SourceProxy
# ---------------------------------------------------------------------------


def test_cached_source_moves_only_when_the_cache_gains_rows(delta_path: Path) -> None:
    source = CachedSource(_delta(delta_path), cache_database=InMemoryArrowDatabase())
    assert _keys(source) == [0, 1, 2]
    before = source.last_modified
    assert source.check_for_new_data() is False
    deltalake.DeltaTable(str(delta_path)).delete("k = 2")
    assert source.check_for_new_data() is False  # the cache keeps every row it had
    assert source.last_modified == before
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_cached_source_job_runs_only_the_new_rows(
    delta_path: Path, orchestrator: str
) -> None:
    job = _job(CachedSource(_delta(delta_path), cache_database=InMemoryArrowDatabase()))
    _run(job, orchestrator)
    CALLS.clear()
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    _run(job, orchestrator)
    assert CALLS == [30]


def test_derived_source_follows_its_source_origin(delta_path: Path) -> None:
    source = DerivedSource(_delta(delta_path), tags_as_data={"k": "k_value"})
    assert _values(source, "k_value") == [0, 1, 2]
    assert source.check_for_new_data() is False
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert source.check_for_new_data() is True
    assert _values(source, "k_value") == [0, 1, 2, 3]


def test_derived_source_follows_its_job_node(csv_path: Path) -> None:
    job = _job(_csv(csv_path))
    stored = job.nodes["rowwise"].as_source()
    assert stored.as_table().num_rows == 0  # nothing stored before the first run
    before = stored.last_modified
    _run(job, "sync")
    assert stored.check_for_new_data() is True
    assert _values(stored, "y") == [1, 11, 21]
    assert stored.last_modified is not None and before is not None
    assert stored.last_modified > before
    assert stored.check_for_new_data() is False


def test_source_proxy_follows_its_delegate(delta_path: Path) -> None:
    live = _delta(delta_path)
    proxy = SourceProxy(
        source_id=live.source_id,
        expected_class_name="DeltaTableSource",
        tag_schema=live.output_schema()[0],
        data_schema=live.output_schema()[1],
        content_hash_str=live.content_hash().to_string(),
        pipeline_hash_str=live.pipeline_hash().to_string(),
    )
    built = proxy.last_modified
    proxy.bind(live)
    bound = proxy.last_modified
    assert bound is not None and built is not None and bound > built
    assert proxy.check_for_new_data() is False
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert proxy.check_for_new_data() is True
    assert proxy.last_modified == live.last_modified
    assert _keys(proxy) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# Review round (ITL-1102): system-tag columns, shared and failed checks
# ---------------------------------------------------------------------------

_REV_SCHEMA = pa.schema(
    [
        pa.field("k", pa.int64(), False),
        pa.field("x", pa.int64(), False),
        pa.field("rev", pa.large_string(), False),
    ]
)


def _rev_rows(rev: str) -> pa.Table:
    return pa.table(
        {"k": [0, 1], "x": [0, 10], "rev": pa.array([rev, rev], pa.large_string())},
        schema=_REV_SCHEMA,
    )


def _system_tag_values(source: Any) -> list[Any]:
    table = source.as_table(columns={"system_tags": True})
    names = [n for n in table.column_names if "rev" in n and n != "rev"]
    names = names or ["rev"]
    return sorted(str(v) for n in names for v in table.column(n).to_pylist())


def test_delta_a_change_in_a_system_tag_column_only_is_taken_in(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rev"
    deltalake.write_deltalake(str(path), _rev_rows("a"))
    source = DeltaTableSource(
        path, tag_columns="k", record_id_column="k", system_tag_columns=["rev"]
    )
    deltalake.write_deltalake(str(path), _rev_rows("b"), mode="overwrite")
    assert source.check_for_new_data() is True
    assert "a" not in _system_tag_values(source)


def test_csv_a_change_in_a_system_tag_column_only_is_taken_in(tmp_path: Path) -> None:
    path = tmp_path / "rev.csv"
    path.write_text("k,x,rev\n0,0,a\n1,10,a\n")
    source = CSVSource(
        str(path), tag_columns="k", record_id_column="k", system_tag_columns=["rev"]
    )
    path.write_text("k,x,rev\n0,0,b\n1,10,b\n")
    assert source.check_for_new_data() is True
    assert "a" not in _system_tag_values(source)


def test_sqlite_a_change_in_a_system_tag_column_only_is_taken_in(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rev.db"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE t (k INTEGER PRIMARY KEY NOT NULL, x INTEGER NOT NULL, "
            "rev TEXT NOT NULL)"
        )
        conn.executemany("INSERT INTO t VALUES (?, ?, 'a')", [(0, 0), (1, 10)])
    source = SQLiteTableSource(
        path, "t", record_id_column="k", system_tag_columns=["rev"]
    )
    with sqlite3.connect(path) as conn:
        conn.execute("UPDATE t SET rev = 'b'")
    assert source.check_for_new_data() is True
    assert "a" not in _system_tag_values(source)


def test_two_derived_sources_over_one_source_both_take_in_its_change(
    delta_path: Path,
) -> None:
    origin = _delta(delta_path)
    first = DerivedSource(origin, tags_as_data={"k": "k_value"})
    second = DerivedSource(origin, tags_as_data={"k": "k_value"})
    assert _values(first, "k_value") == _values(second, "k_value") == [0, 1, 2]
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert first.check_for_new_data() is True
    assert second.check_for_new_data() is True  # the origin's change is consumed
    assert _values(second, "k_value") == [0, 1, 2, 3]


def test_a_derived_source_retries_a_read_that_raised(csv_path: Path) -> None:
    job = _job(_csv(csv_path))
    _run(job, "sync")
    stored = job.nodes["rowwise"].as_source()
    assert _values(stored, "y") == [1, 11, 21]
    _write_csv(csv_path, [0, 1, 2, 3])
    _run(job, "sync")
    with patch.object(
        DerivedSource, "_origin_records", side_effect=OSError("store unreachable")
    ):
        with pytest.raises(OSError, match="store unreachable"):
            stored.check_for_new_data()
    assert _values(stored, "y") == [1, 11, 21]
    assert stored.check_for_new_data() is True
    assert _values(stored, "y") == [1, 11, 21, 31]


def test_two_cached_sources_over_one_source_both_move(delta_path: Path) -> None:
    origin = _delta(delta_path)
    first = CachedSource(origin, cache_database=InMemoryArrowDatabase())
    second = CachedSource(origin, cache_database=InMemoryArrowDatabase())
    assert _keys(first) == _keys(second) == [0, 1, 2]
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert first.check_for_new_data() is True
    before = second.last_modified
    assert second.check_for_new_data() is True  # the origin's change is consumed
    assert second.last_modified != before
    assert _keys(second) == [0, 1, 2, 3]


def test_a_cached_source_retries_a_comparison_that_raised(delta_path: Path) -> None:
    source = CachedSource(_delta(delta_path), cache_database=InMemoryArrowDatabase())
    assert _keys(source) == [0, 1, 2]
    before = source.last_modified
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    with patch.object(
        CachedSource, "_live_records", side_effect=OSError("cache unreachable")
    ):
        with pytest.raises(OSError, match="cache unreachable"):
            source.check_for_new_data()
    assert source.last_modified == before
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]


# ---------------------------------------------------------------------------
# Review round 2 (ITL-1102): inferred nullability, a derived source holding nothing
# ---------------------------------------------------------------------------


def test_csv_a_column_that_loses_its_nulls_keeps_its_nullable_type(
    tmp_path: Path,
) -> None:
    path = tmp_path / "nulls.csv"
    path.write_text("k,x\n0,\n1,10\n")
    source = _csv(path)
    before = source.output_schema()
    path.write_text("k,x\n0,0\n1,10\n")  # no null left: still the nullable column
    assert source.check_for_new_data() is True
    assert _values(source, "x") == [0, 10]
    path.write_text("k,x\n0,\n1,10\n2,20\n")  # a null again
    assert source.check_for_new_data() is True
    assert _values(source, "x") == [10, 20]
    assert source.output_schema() == before


def test_csv_a_null_in_a_column_built_without_one_fails_the_check(
    csv_path: Path,
) -> None:
    source = _csv(csv_path)
    csv_path.write_text("k,x\n0,\n1,10\n")
    with pytest.raises(ValueError, match="changed its columns"):
        source.check_for_new_data()
    assert _keys(source) == [0, 1, 2]


def _connector_without_column_info(x: list[int | None]) -> MagicMock:
    connector = MagicMock()
    connector.get_table_names.return_value = ["t"]
    connector.get_pk_columns.return_value = ["k"]
    connector.get_column_info.return_value = []
    table = pa.table(
        {"k": pa.array(range(len(x)), pa.int64()), "x": pa.array(x, pa.int64())}
    )
    connector.iter_batches.side_effect = lambda _query: iter(table.to_batches())
    return connector


def test_db_table_source_inferred_nullability_keeps_a_column_nullable() -> None:
    connector = _connector_without_column_info([None, 10])
    source = DBTableSource(connector, "t", record_id_column="k")
    before = source.output_schema()
    filled = _connector_without_column_info([0, 10])
    connector.iter_batches.side_effect = filled.iter_batches.side_effect
    assert source.check_for_new_data() is True
    assert _values(source, "x") == [0, 10]
    assert source.output_schema() == before


@function_pod(output_keys="z")
def twice(y: int) -> int:
    """Return ``2 * y``."""
    return 2 * y


def test_a_derived_source_that_held_no_rows_moves_when_its_origin_runs(
    csv_path: Path,
) -> None:
    upstream = _job(_csv(csv_path))
    derived = DerivedSource(upstream.nodes["rowwise"], tags_as_data={"k": "k_value"})
    downstream = PipelineJob(name="down", database=InMemoryArrowDatabase())
    with downstream:
        twice.pod(derived, label="doubled")
    _run(downstream, "sync")
    assert downstream.nodes["doubled"].as_table().num_rows == 0
    _run(upstream, "sync")
    _run(downstream, "sync")
    zs = sorted(r["z"] for r in downstream.nodes["doubled"].as_table().to_pylist())
    assert zs == [2, 22, 42]


# ---------------------------------------------------------------------------
# Review round 3 (ITL-1102): a shared cache, relative paths
# ---------------------------------------------------------------------------


def test_two_cached_sources_sharing_one_cache_both_move(delta_path: Path) -> None:
    """The second wrapper's rows change though the first already wrote the new row."""
    origin = _delta(delta_path)
    database = InMemoryArrowDatabase()
    first = CachedSource(origin, cache_database=database)
    second = CachedSource(origin, cache_database=database)
    assert _keys(first) == _keys(second) == [0, 1, 2]
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert first.check_for_new_data() is True
    assert _keys(first) == [0, 1, 2, 3]  # this read writes the row to the shared cache
    before = second.last_modified
    assert second.check_for_new_data() is True
    assert second.last_modified != before
    assert _keys(second) == [0, 1, 2, 3]


def test_csv_a_relative_path_is_checked_where_it_was_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    _write_csv(tmp_path / "a" / "rows.csv", [0, 1, 2])
    _write_csv(tmp_path / "b" / "rows.csv", [5, 6, 7, 8])
    monkeypatch.chdir(tmp_path / "a")
    source = CSVSource("rows.csv", tag_columns="k", record_id_column="k")
    monkeypatch.chdir(tmp_path / "b")
    assert source.check_for_new_data() is False
    _write_csv(tmp_path / "a" / "rows.csv", [0, 1, 2, 3])
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]


def test_sqlite_a_relative_path_is_checked_where_it_was_read(
    sqlite_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "elsewhere").mkdir()
    monkeypatch.chdir(sqlite_path.parent)
    source = SQLiteTableSource(sqlite_path.name, "t", record_id_column="k")
    monkeypatch.chdir(tmp_path / "elsewhere")
    _insert(sqlite_path, 3)
    assert source.check_for_new_data() is True
    assert _keys(source) == [0, 1, 2, 3]
    assert not (tmp_path / "elsewhere" / sqlite_path.name).exists()


# ---------------------------------------------------------------------------
# Review round 4 (ITL-1102): two wrappers sharing one cache, every order
# ---------------------------------------------------------------------------
#
# The invariant a CachedSource's check keeps: its modified time moves exactly when
# its next read would present a different set of rows. A shared cache keeps every row
# any wrapper wrote, so a wrapper's next read presents the cache's rows and the live
# rows together.


def _shared_wrappers(delta_path: Path) -> tuple[CachedSource, CachedSource]:
    """Two CachedSources over one Delta source, sharing one cache, both read once."""
    origin = _delta(delta_path)
    database = InMemoryArrowDatabase()
    pair = (
        CachedSource(origin, cache_database=database),
        CachedSource(origin, cache_database=database),
    )
    for wrapper in pair:
        _ = wrapper.as_table()
    return pair


def _moved(wrapper: CachedSource) -> bool:
    """Check ``wrapper``; return whether the check said so AND its time really moved."""
    before = wrapper.last_modified
    found = wrapper.check_for_new_data()
    assert found is (wrapper.last_modified != before)
    return found


@pytest.mark.parametrize("leader", [0, 1])
def test_shared_cache_added_rows_move_both_wrappers(
    delta_path: Path, leader: int
) -> None:
    wrappers = _shared_wrappers(delta_path)
    first, second = wrappers[leader], wrappers[1 - leader]
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert _moved(first) is True
    assert _keys(first) == [0, 1, 2, 3]  # writes the row to the shared cache
    assert _moved(second) is True
    assert _keys(second) == [0, 1, 2, 3]
    assert _moved(first) is False
    assert _moved(second) is False


@pytest.mark.parametrize("leader", [0, 1])
def test_shared_cache_rows_added_then_deleted_still_move_the_other_wrapper(
    delta_path: Path, leader: int
) -> None:
    wrappers = _shared_wrappers(delta_path)
    first, second = wrappers[leader], wrappers[1 - leader]
    deltalake.write_deltalake(str(delta_path), _rows([3]), mode="append")
    assert _moved(first) is True
    assert _keys(first) == [0, 1, 2, 3]  # the shared cache now holds key 3
    deltalake.DeltaTable(str(delta_path)).delete("k = 3")
    assert _moved(second) is True  # its next read presents the cached key 3
    assert _keys(second) == [0, 1, 2, 3]
    assert _moved(first) is False  # it already presents key 3, which stays cached
    assert _keys(first) == [0, 1, 2, 3]


@pytest.mark.parametrize("leader", [0, 1])
def test_shared_cache_deleted_rows_move_neither_wrapper(
    delta_path: Path, leader: int
) -> None:
    wrappers = _shared_wrappers(delta_path)
    first, second = wrappers[leader], wrappers[1 - leader]
    deltalake.DeltaTable(str(delta_path)).delete("k = 2")
    assert _moved(first) is False  # the cache keeps every row it held
    assert _moved(second) is False
    assert _keys(first) == _keys(second) == [0, 1, 2]


# ---------------------------------------------------------------------------
# Row order (developer, November 14, 2026, ITL-1102): without a record_id_column a
# row's record id is its position, so a change of row order is a change; with one,
# only content counts.
# ---------------------------------------------------------------------------


def _record_ids(source: Any) -> dict[int, str]:
    """Each row's record id (its system tag), by key ``k``."""
    table = source.as_table(columns={"system_tags": True})
    (column,) = [n for n in table.column_names if "record_id" in n]
    return dict(zip(table.column("k").to_pylist(), table.column(column).to_pylist()))


def test_csv_positional_ids_a_reorder_is_a_change(csv_path: Path) -> None:
    source = CSVSource(str(csv_path), tag_columns="k")
    before = _record_ids(source)
    _write_csv(csv_path, [2, 0, 1])
    assert source.check_for_new_data() is True
    after = _record_ids(source)
    assert after[2] == before[0]  # the ids follow the new positions
    assert after[0] == before[1]
    assert source.check_for_new_data() is False


def test_csv_explicit_record_ids_a_reorder_is_no_change(csv_path: Path) -> None:
    source = _csv(csv_path)  # record_id_column="k"
    before = source.last_modified
    _write_csv(csv_path, [2, 0, 1])
    assert source.check_for_new_data() is False
    assert source.last_modified == before


def _delta_positional(path: Path) -> DeltaTableSource:
    return DeltaTableSource(path, tag_columns="k")


def test_delta_positional_ids_a_reorder_is_a_change(delta_path: Path) -> None:
    source = _delta_positional(delta_path)
    before = _record_ids(source)
    deltalake.write_deltalake(str(delta_path), _rows([2, 0, 1]), mode="overwrite")
    assert source.check_for_new_data() is True
    assert _record_ids(source)[2] == before[0]


def test_delta_explicit_record_ids_a_reorder_is_no_change(delta_path: Path) -> None:
    source = _delta(delta_path)  # record_id_column="k"
    before = source.last_modified
    deltalake.write_deltalake(str(delta_path), _rows([2, 0, 1]), mode="overwrite")
    assert source.check_for_new_data() is False
    assert source.last_modified == before


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("kind", ["csv", "delta"])
def test_a_reorder_with_positional_ids_reruns_and_reuses_every_result(
    csv_path: Path, delta_path: Path, kind: str, orchestrator: str
) -> None:
    source: Any = (
        CSVSource(str(csv_path), tag_columns="k")
        if kind == "csv"
        else _delta_positional(delta_path)
    )
    job = _job(source)
    _run(job, orchestrator)
    CALLS.clear()
    if kind == "csv":
        _write_csv(csv_path, [2, 0, 1])
    else:
        deltalake.write_deltalake(str(delta_path), _rows([2, 0, 1]), mode="overwrite")
    summary = _run(job, orchestrator)
    assert summary.nodes["rowwise"].held == 0  # the node ran again
    assert CALLS == []  # every function result was reused
    assert _ys(job) == [1, 11, 21]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("kind", ["csv", "delta"])
def test_a_reorder_with_explicit_record_ids_keeps_the_job_current(
    csv_path: Path, delta_path: Path, kind: str, orchestrator: str
) -> None:
    source: Any = _csv(csv_path) if kind == "csv" else _delta(delta_path)
    job = _job(source)
    _run(job, orchestrator)
    if kind == "csv":
        _write_csv(csv_path, [2, 0, 1])
    else:
        deltalake.write_deltalake(str(delta_path), _rows([2, 0, 1]), mode="overwrite")
    assert _run(job, orchestrator).nodes["rowwise"].held == 3
