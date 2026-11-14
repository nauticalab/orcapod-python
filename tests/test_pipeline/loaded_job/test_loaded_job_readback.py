"""Reading the nodes of a job loaded with ``PipelineJob.load`` (ITL-904).

The main job is saved in a separate process (``save_job_script.py``) and loaded here, so
these tests see exactly what a user sees when loading a job in a new session: the function
pods and the in-memory sources of the saving process do not exist. Tests of one specific
shape (a datetime or ``Path`` output, ephemeral results) save their own small job in this
process; loading never touches the objects that built it.
"""

from __future__ import annotations

import datetime
import json
import logging
import pathlib
import subprocess
import sys
import uuid
import warnings
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import StoreContentWarning
from orcapod.pipeline.serialization import LoadStatus
from orcapod.sources import DictSource
from orcapod.types import NodeConfig

SAVE_SCRIPT = pathlib.Path(__file__).with_name("save_job_script.py")
FUNCTION_NODES = ("dbl", "inc", "half", "sq")
TAG_OF = {"dbl": "id", "inc": "id", "half": "id", "sq": "element_index"}


@pytest.fixture(scope="module")
def saved(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Directory holding a job saved by a separate Python process."""
    root = tmp_path_factory.mktemp("saved_job")
    _ = subprocess.run([sys.executable, str(SAVE_SCRIPT), str(root)], check=True)
    return root


def _load(saved: pathlib.Path, **kwargs: Any) -> PipelineJob:
    """Load the job saved in *saved*."""
    return PipelineJob.load(str(saved / "job.json"), **kwargs)


def _sorted(table: pa.Table, tag: str) -> pa.Table:
    """Return *table* sorted by the tag column *tag*."""
    return table.sort_by([(tag, "ascending")])


def _rewrite_job_file(
    saved: pathlib.Path,
    tmp_path: pathlib.Path,
    edit: Callable[[dict[str, Any]], None],
) -> str:
    """Write a copy of the saved job file, changed by *edit*, and return its path."""
    data: dict[str, Any] = json.loads((saved / "job.json").read_text())
    edit(data)
    path = tmp_path / "edited_job.json"
    _ = path.write_text(json.dumps(data))
    return str(path)


def _dict_source() -> DictSource:
    """Return a two-row in-memory source with tag ``id`` and data ``x``."""
    return DictSource(
        [{"id": 0, "x": 5}, {"id": 1, "x": 10}], tag_columns=["id"], source_id="dsrc"
    )


def _save_small_job(
    tmp_path: pathlib.Path,
    record: Callable[[], None],
    configure: Callable[[PipelineJob], None] | None = None,
) -> str:
    """Record, run and save a job on a Delta store in *tmp_path*; return its path.

    Args:
        tmp_path: Directory for the store and the saved file.
        record: Called inside the job's ``with`` block to record the pipeline.
        configure: Called after recording and before the run, to configure nodes.
    """
    job = PipelineJob(
        name="small", database=DeltaTableDatabase(str(tmp_path / "store"))
    )
    with job:
        record()
    if configure is not None:
        configure(job)
    _ = job.run()
    path = tmp_path / "small_job.json"
    job.save(str(path))
    return str(path)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def test_load_does_not_crash_when_an_operator_consumes_a_function_node(
    saved: pathlib.Path,
) -> None:
    """A job whose Join consumes a function node loads."""
    job = _load(saved)
    assert {"dbl", "joined", "inc", "half", "sq"} <= set(job.nodes)


def test_loaded_function_node_output_schema_has_python_types(
    saved: pathlib.Path,
) -> None:
    """A loaded node reports Python types, not saved type strings."""
    tag_schema, data_schema = _load(saved).nodes["dbl"].output_schema()
    assert dict(tag_schema) == {"id": int}
    assert dict(data_schema) == {"dbl": int}


def test_datetime_output_feeding_an_operator_loads_with_its_type(
    tmp_path: pathlib.Path,
) -> None:
    """A ``datetime`` output, saved as ``timestamp[us, tz=UTC]``, loads as ``datetime``."""

    @function_pod(output_keys="t")
    def stamp(x: int) -> datetime.datetime:
        """Return a UTC timestamp derived from *x*."""
        return datetime.datetime(2026, 1, x % 28 + 1, tzinfo=datetime.timezone.utc)

    other = DictSource([{"id": 0, "y": 1}, {"id": 1, "y": 2}], tag_columns=["id"])

    def record() -> None:
        """Record the pipeline under test."""
        _ = Join()(stamp.pod(_dict_source(), label="stamp"), other, label="joined")

    path = _save_small_job(tmp_path, record)
    node = PipelineJob.load(path).nodes["stamp"]
    assert dict(node.output_schema()[1]) == {"t": datetime.datetime}
    assert node.as_table().num_rows == 2


def test_datetime_output_with_nothing_stored_returns_typed_zero_rows(
    tmp_path: pathlib.Path,
) -> None:
    """The zero-row table of a ``datetime`` output has a timestamp column."""

    @function_pod(output_keys="t")
    def stamp(x: int) -> datetime.datetime:
        """Return a UTC timestamp derived from *x*."""
        return datetime.datetime(2026, 1, x % 28 + 1, tzinfo=datetime.timezone.utc)

    def record() -> None:
        """Record the pipeline under test."""
        _ = stamp.pod(_dict_source(), label="stamp")

    path = _save_small_job(tmp_path, record)
    data = json.loads(pathlib.Path(path).read_text())
    # Without the advisory count, an empty store is read without any check.
    data["run"].pop("expected_rows", None)
    _ = pathlib.Path(path).write_text(json.dumps(data))
    table = (
        PipelineJob.load(path, database=InMemoryArrowDatabase())
        .nodes["stamp"]
        .as_table()
    )
    assert table.num_rows == 0
    assert pa.types.is_timestamp(table.schema.field("t").type)


def test_logical_type_output_schema_fails_naming_the_column_and_saved_type(
    tmp_path: pathlib.Path,
) -> None:
    """A registered ``Path`` output restores its Python schema and stored logical rows."""

    @function_pod(output_keys="p")
    def to_path(x: int) -> pathlib.Path:
        """Return a path derived from *x*."""
        return pathlib.Path(f"/data/{x}")

    def record() -> None:
        """Record the pipeline under test."""
        _ = to_path.pod(_dict_source(), label="paths")

    node = PipelineJob.load(_save_small_job(tmp_path, record)).nodes["paths"]
    assert node.output_schema()[1]["p"] is pathlib.Path
    # The stored rows are still readable, with their stored logical type.
    table = node.as_table()
    assert table.num_rows == 2
    assert isinstance(table.schema.field("p").type, pa.ExtensionType)


# ---------------------------------------------------------------------------
# Function nodes: stored rows
# ---------------------------------------------------------------------------


def test_path_output_feeding_join_still_allows_job_load(tmp_path: pathlib.Path) -> None:
    """An unrestorable input schema must not prevent reading stored function rows."""

    @function_pod(output_keys="p")
    def to_path(x: int) -> pathlib.Path:
        """Return a path derived from the input."""
        return pathlib.Path(f"/data/{x}")

    def record() -> None:
        """Record a Path-returning function feeding a Join."""
        paths = to_path.pod(_dict_source(), label="paths")
        other = DictSource(
            [{"id": 0, "y": 1}, {"id": 1, "y": 2}],
            tag_columns=["id"],
            source_id="other",
        )
        Join()(paths, other, label="joined")

    loaded = PipelineJob.load(_save_small_job(tmp_path, record))
    assert loaded.nodes["paths"].as_table().num_rows == 2


@pytest.mark.parametrize("label", FUNCTION_NODES)
def test_loaded_function_node_as_table_returns_stored_rows(
    saved: pathlib.Path, label: str
) -> None:
    """A loaded node's table equals the live table: schema and values."""
    expected = pq.read_table(saved / "expected" / f"{label}.parquet")
    table = _load(saved).nodes[label].as_table()
    assert table.schema == expected.schema
    assert _sorted(table, TAG_OF[label]).equals(_sorted(expected, TAG_OF[label]))


@pytest.mark.parametrize("label", FUNCTION_NODES)
def test_loaded_function_node_as_table_all_info_has_stream_columns(
    saved: pathlib.Path, label: str
) -> None:
    """With ``all_info``, a loaded node's table has every live column and more."""
    # The loaded table follows the documented stream semantics: every column of the
    # live table, with the same values and types, plus the data context and the
    # source-info columns. (The live FunctionJobNode.as_table() drops those two
    # groups -- a separate, pre-existing bug the loaded node does not copy.)
    expected = pq.read_table(saved / "expected" / f"{label}.all_info.parquet")
    table = _load(saved).nodes[label].as_table(all_info=True)
    data_column = next(iter(_load(saved).nodes[label].output_schema()[1]))
    assert "_context_key" in table.column_names
    assert f"_source_{data_column}" in table.column_names
    for name in expected.column_names:
        assert table.schema.field(name).type == expected.schema.field(name).type
    common = _sorted(table, TAG_OF[label]).select(expected.column_names)
    assert common.to_pylist() == _sorted(expected, TAG_OF[label]).to_pylist()


def test_loaded_function_node_iter_data_yields_stored_rows(saved: pathlib.Path) -> None:
    """``iter_data()`` yields the stored (tag, data) pairs."""
    rows = sorted(
        (
            (tag.as_dict(), data.as_dict())
            for tag, data in _load(saved).nodes["dbl"].iter_data()
        ),
        key=lambda row: row[0]["id"],
    )
    assert rows == [({"id": 0}, {"dbl": 10}), ({"id": 1}, {"dbl": 20})]


def test_loaded_function_node_get_all_records_returns_stored_rows(
    saved: pathlib.Path,
) -> None:
    """``get_all_records()`` still returns the stored rows."""
    records = _load(saved).nodes["dbl"].get_all_records()
    assert records is not None
    assert records.num_rows == 2


def test_loaded_function_node_with_store_is_read_only(saved: pathlib.Path) -> None:
    """With a store attached, a loaded node is READ_ONLY."""
    assert _load(saved).nodes["dbl"].load_status == LoadStatus.READ_ONLY


def _append_recomputed_record(
    node: Any, entry_id: int, recomputation_index: int, value: int
) -> None:
    """Store a recomputed result for the entry tagged ``id == entry_id``.

    Non-public on purpose: no public call recomputes a stored entry in a test, so
    this writes what a recomputation writes -- a result record with *value* and a
    pipeline record for the same base entry with *recomputation_index* -- straight
    into the node's store.
    """
    from orcapod.system_constants import constants

    record_id_col = constants.DATA_RECORD_ID
    new_record_id = uuid.uuid4().bytes

    reader = node._cached_function_pod
    results = reader.result_database.get_all_records(
        reader.record_path, record_id_column=record_id_col
    )
    pipeline_db = node._pipeline_database
    table_path = node._versioned_pipeline_path
    entry_id_col = "__pipeline_entry_id"
    records = pipeline_db.get_all_records(table_path, record_id_column=entry_id_col)
    # The entry's original record: selected by its tag value, never by row order.
    index_col = "__pipeline_recomputation_index"
    id_type = records.schema.field("id").type
    index_type = records.schema.field(index_col).type
    record = records.filter(
        pc.and_(
            pc.equal(records.column("id"), pa.scalar(entry_id, type=id_type)),
            pc.equal(records.column(index_col), pa.scalar(0, type=index_type)),
        )
    )
    assert record.num_rows == 1
    old_record_id = record.column(record_id_col)[0]
    result = results.filter(pc.equal(results.column(record_id_col), old_record_id))

    def replaced(table: pa.Table, column: str, values: list[Any]) -> pa.Table:
        """Return *table* with *column* set to *values*, keeping its type."""
        field = table.schema.field(column)
        return table.set_column(
            table.schema.get_field_index(column),
            field,
            pa.array(values, type=field.type),
        )

    result = replaced(result, record_id_col, [new_record_id])
    result = replaced(result, "dbl", [value])
    reader.result_database.add_records(
        reader.record_path, result, record_id_column=record_id_col, flush=True
    )
    record = replaced(record, entry_id_col, [uuid.uuid4().bytes])
    record = replaced(record, "__pipeline_recomputation_index", [recomputation_index])
    record = replaced(record, record_id_col, [new_record_id])
    pipeline_db.add_records(
        table_path, record, record_id_column=entry_id_col, flush=True
    )


def _record_double() -> None:
    """Record ``double`` over the two-row in-memory source, labelled ``dbl``."""

    @function_pod(output_keys="dbl")
    def double(x: int) -> int:
        """Double *x*."""
        return 2 * x

    _ = double.pod(_dict_source(), label="dbl")


@pytest.mark.parametrize("read_method", ["as_table", "iter_data", "get_all_records"])
def test_other_context_records_do_not_satisfy_saved_expectation(
    tmp_path: pathlib.Path, read_method: str
) -> None:
    """Records of another data context are not the run's: none is read, with a warning."""
    path = _save_small_job(tmp_path, _record_double)
    written = DeltaTableDatabase(str(tmp_path / "store"))
    other = InMemoryArrowDatabase()
    for table_path in written.list_sources():
        rows = written.get_all_records(table_path, record_id_column="__rid")
        assert rows is not None
        index = rows.schema.get_field_index("__data_context")
        field = rows.schema.field(index)
        rows = rows.set_column(
            index,
            field,
            pa.array(["std:v0.9:other"] * rows.num_rows, type=field.type),
        )
        other.add_records(table_path, rows, record_id_column="__rid")
    other.flush()

    node = PipelineJob.load(path, database=other).nodes["dbl"]
    # The store is the source of truth (ITL-1043): it holds none of the run's
    # records, so none is read; the job file's count makes it a warning.
    with pytest.warns(StoreContentWarning, match="records 2 rows .* holds 0"):
        result = getattr(node, read_method)()
        rows = list(result) if read_method == "iter_data" else result
    assert rows is None or len(rows) == 0


def test_recomputed_entry_is_read_once_with_its_latest_result(
    tmp_path: pathlib.Path,
) -> None:
    """A loaded node reads one row per entry: the greatest recomputation index wins."""
    path = _save_small_job(tmp_path, _record_double)
    node = PipelineJob.load(path).nodes["dbl"]
    entry_id = next(iter(node.iter_data()))[0].as_dict()["id"]
    # Index 2 is stored before index 1, so "last record wins" would pick 111.
    _append_recomputed_record(node, entry_id, recomputation_index=2, value=222)
    _append_recomputed_record(node, entry_id, recomputation_index=1, value=111)

    reloaded = PipelineJob.load(path).nodes["dbl"]
    table = reloaded.as_table()
    assert table.num_rows == 2
    rows = {row["id"]: row["dbl"] for row in table.to_pylist()}
    assert rows[entry_id] == 222
    iterated = {t.as_dict()["id"]: d.as_dict()["dbl"] for t, d in reloaded.iter_data()}
    assert iterated == rows


def test_live_node_hot_load_reads_the_latest_recomputed_result(
    tmp_path: pathlib.Path,
) -> None:
    """A live node hot-loading from its store also keeps the greatest index."""
    # Behaviour change (ITL-904): the hot-load used to keep whichever record of an
    # entry the join returned last; it now keeps the latest recomputation.
    store = DeltaTableDatabase(str(tmp_path / "store"))
    job = PipelineJob(name="small", database=store)
    with job:
        _record_double()
    _ = job.run()
    node = job.nodes["dbl"]
    entry_id = next(iter(node.iter_data()))[0].as_dict()["id"]
    _append_recomputed_record(node, entry_id, recomputation_index=2, value=222)
    _append_recomputed_record(node, entry_id, recomputation_index=1, value=111)

    fresh = PipelineJob(name="small", database=store)
    with fresh:
        _record_double()
    values = {
        t.as_dict()["id"]: d.as_dict()["dbl"] for t, d in fresh.nodes["dbl"].iter_data()
    }
    assert len(values) == 2
    assert values[entry_id] == 222


def test_loaded_node_with_ephemeral_results_does_not_crash(
    tmp_path: pathlib.Path,
) -> None:
    """Ephemeral results come back as placeholders; the table says why it has none."""

    @function_pod(output_keys="dbl")
    def double(x: int) -> int:
        """Double *x*."""
        return 2 * x

    def record() -> None:
        """Record the pipeline under test."""
        _ = double.pod(_dict_source(), label="dbl")

    def make_ephemeral(job: PipelineJob) -> None:
        """Keep the node's results in an in-memory ephemeral store."""
        node = job.nodes["dbl"]
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.set_ephemeral_store(InMemoryArrowDatabase())

    path = _save_small_job(tmp_path, record, configure=make_ephemeral)
    node = PipelineJob.load(path).nodes["dbl"]
    assert [type(data).__name__ for _, data in node.iter_data()] == [
        "EmptyData",
        "EmptyData",
    ]
    with pytest.raises(RuntimeError) as excinfo:
        _ = node.as_table()
    assert str(excinfo.value) == (
        "FunctionJobNode 'dbl': the saved run kept 2 of its results in an ephemeral "
        "store, which is not saved with the job, so they cannot be read into a table. "
        "iter_data() yields them as EmptyData placeholders."
    )


def test_newer_unavailable_result_does_not_expose_old_persistent_result(
    tmp_path: pathlib.Path,
) -> None:
    """A later ephemeral recomputation wins even after its ephemeral store is gone."""
    from orcapod.system_constants import constants

    path = _save_small_job(tmp_path, _record_double)
    store = DeltaTableDatabase(str(tmp_path / "store"))
    (table_path,) = [p for p in store.list_sources() if p[-1] == "pdb_v2"]
    records = store.get_all_records(table_path, record_id_column="__rid")
    assert records is not None
    # Public database writes reproduce historical recomputations without executing a pod.
    for column, values in {
        "__rid": [uuid.uuid4().bytes for _ in range(records.num_rows)],
        constants.IS_EPHEMERAL_COL: [True] * records.num_rows,
        "__pipeline_recomputation_index": [1] * records.num_rows,
    }.items():
        index = records.schema.get_field_index(column)
        field = records.schema.field(index)
        records = records.set_column(index, field, pa.array(values, type=field.type))
    store.add_records(table_path, records, record_id_column="__rid", flush=True)

    node = PipelineJob.load(path).nodes["dbl"]
    assert [type(data).__name__ for _, data in node.iter_data()] == ["EmptyData"] * 2
    with pytest.raises(RuntimeError, match="ephemeral store"):
        node.as_table()


# ---------------------------------------------------------------------------
# Function nodes: the store
# ---------------------------------------------------------------------------


def test_loaded_function_node_warns_once_on_a_store_without_its_runs_records(
    saved: pathlib.Path,
) -> None:
    """A store lacking the run's records is read as it is, with one warning.

    The store is the source of truth (ITL-1043): every read returns what it holds.
    The job file's advisory count makes the difference a ``StoreContentWarning``,
    issued once per node.
    """
    node = _load(saved, database=InMemoryArrowDatabase()).nodes["dbl"]
    with pytest.warns(StoreContentWarning) as record:
        table = node.as_table()
        rows = list(node.iter_data())
        records = node.get_all_records()
        head = node.head()
    assert table.num_rows == 0
    assert table.column_names == ["id", "dbl"]
    assert rows == []
    assert records is None
    assert head.table.num_rows == 0
    messages = [str(w.message) for w in record if w.category is StoreContentWarning]
    assert len(messages) == 1
    assert messages[0].startswith(
        "FunctionJobNode 'dbl': the job file records 2 rows for its run, but the "
        "in_memory store (table 'loaded_demo/double/"
    )
    assert messages[0].endswith(
        "holds 0 under the run's key. The read returns what the store holds; the "
        "store may not be the one the run wrote, or it may have changed since."
    )


def test_store_content_warning_names_a_delta_stores_location(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The warning names a Delta store's location."""
    other = tmp_path / "other_store"
    node = _load(saved, database=DeltaTableDatabase(str(other))).nodes["dbl"]
    with pytest.warns(StoreContentWarning, match="the store at '") as record:
        _ = node.as_table()
    assert str(other) in str(record[0].message)


def test_store_with_only_another_runs_records_reads_none_with_a_warning(
    tmp_path: pathlib.Path,
) -> None:
    """A shared table holding another run's records, not this node's, reads none."""

    def record_other_source() -> None:
        """Record ``double`` over another source: same table, other records."""

        @function_pod(output_keys="dbl")
        def double(x: int) -> int:
            """Double *x*."""
            return 2 * x

        # A node's records are scoped by its source's identity (``source_id``), so
        # "another run" here is a run over a different source. Its records share
        # the pipeline table, which is keyed by the schema-only pipeline hash.
        other = DictSource(
            [{"id": 0, "x": 7}, {"id": 1, "x": 8}],
            tag_columns=["id"],
            source_id="other",
        )
        _ = double.pod(other, label="dbl")

    path = _save_small_job(tmp_path / "this_run", _record_double)
    _ = _save_small_job(tmp_path / "other_run", record_other_source)
    other_store = DeltaTableDatabase(str(tmp_path / "other_run" / "store"))
    node = PipelineJob.load(path, database=other_store).nodes["dbl"]
    # Non-public on purpose: confirm the shared table exists, so this test checks
    # the per-record count, not the missing-table case.
    assert node._pipeline_database is not None
    assert node._pipeline_database.table_exists(node._versioned_pipeline_path)
    with pytest.warns(StoreContentWarning, match="records 2 rows .* holds 0"):
        assert node.as_table().num_rows == 0


def test_saved_file_records_the_rows_each_node_had_in_the_store(
    saved: pathlib.Path,
) -> None:
    """``save()`` records, per job node, the rows its run left in the store."""
    run = json.loads((saved / "job.json").read_text())["run"]
    # The join is not cached (cache mode OFF): its run stored nothing.
    assert run["expected_rows"] == {
        "dbl": 2,
        "inc": 2,
        "half": 2,
        "sq": 2,
        "joined": 0,
    }
    assert "node_has_stored_records" not in run


def test_saving_a_loaded_job_keeps_the_expected_rows(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Re-saving a job loaded with another store keeps the saved counts."""
    resaved = tmp_path / "resaved.json"
    _load(saved, database=InMemoryArrowDatabase()).save(str(resaved))
    original = json.loads((saved / "job.json").read_text())["run"]["expected_rows"]
    run = json.loads(resaved.read_text())["run"]
    assert run["expected_rows"] == original
    node = PipelineJob.load(str(resaved), database=InMemoryArrowDatabase()).nodes["dbl"]
    with pytest.warns(StoreContentWarning, match="records 2 rows"):
        assert node.as_table().num_rows == 0


def test_loaded_function_node_with_nothing_stored_returns_zero_rows(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A node saved with nothing stored reads zero rows with its columns."""

    def nothing_stored(data: dict[str, Any]) -> None:
        """Mark the node as saved with nothing stored."""
        data["run"]["expected_rows"]["dbl"] = 0

    path = _rewrite_job_file(saved, tmp_path, nothing_stored)
    node = PipelineJob.load(path, database=InMemoryArrowDatabase()).nodes["dbl"]
    with warnings.catch_warnings():
        # Zero expected and zero found: nothing to warn about.
        warnings.simplefilter("error", StoreContentWarning)
        table = node.as_table()
    assert table.num_rows == 0
    assert table.column_names == ["id", "dbl"]
    assert list(node.iter_data()) == []


def test_file_without_the_expected_rows_section_reads_without_a_check(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The section is optional: deleting it changes nothing but the check."""

    def drop_section(data: dict[str, Any]) -> None:
        """Remove the advisory section, by hand."""
        del data["run"]["expected_rows"]

    path = _rewrite_job_file(saved, tmp_path, drop_section)
    with warnings.catch_warnings():
        warnings.simplefilter("error", StoreContentWarning)
        job = PipelineJob.load(path, database=InMemoryArrowDatabase())
        assert job.nodes["dbl"].as_table().num_rows == 0
        assert PipelineJob.load(path).nodes["dbl"].as_table().num_rows == 2


def test_a_file_with_the_old_stored_records_flag_loads_without_a_check(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The ITL-904 flag of an earlier file is not used: it never raises or warns."""

    def old_flag(data: dict[str, Any]) -> None:
        """Write the file as before ITL-1043: the flag, no section."""
        del data["run"]["expected_rows"]
        data["run"]["node_has_stored_records"] = {"dbl": True}

    path = _rewrite_job_file(saved, tmp_path, old_flag)
    with warnings.catch_warnings():
        warnings.simplefilter("error", StoreContentWarning)
        node = PipelineJob.load(path, database=InMemoryArrowDatabase()).nodes["dbl"]
        assert node.as_table().num_rows == 0


def test_loaded_function_node_without_store_says_how_to_attach_one(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Without a store, every read method says how to attach one."""

    def drop_store(data: dict[str, Any]) -> None:
        """Remove the saved store configuration."""
        data["bindings"]["database"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    node = job.nodes["dbl"]
    assert node.load_status == LoadStatus.UNAVAILABLE
    reads = (node.as_table, lambda: list(node.iter_data()), node.get_all_records)
    for read in reads:
        with pytest.raises(RuntimeError) as excinfo:
            _ = read()
        message = str(excinfo.value)
        assert "was loaded without a database" in message
        assert "PipelineJob.load(path, database=...)" in message
        assert "job.bind(database=...)" in message
        assert "no database attached" not in message


def test_binding_a_store_makes_a_storeless_loaded_node_readable(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """``job.bind(database=...)`` makes a loaded node readable."""

    def drop_store(data: dict[str, Any]) -> None:
        """Remove the saved store configuration."""
        data["bindings"]["database"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    job.bind(database=DeltaTableDatabase(str(saved / "store")))
    assert job.nodes["dbl"].as_table().num_rows == 2


def test_loaded_job_still_cannot_run(saved: pathlib.Path) -> None:
    """A loaded job is read-only: ``run()`` raises."""
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _ = _load(saved).run()


def _loaded_operator_only_job(tmp_path: pathlib.Path) -> PipelineJob:
    """Load a job whose two CSV inputs can both be reconstructed."""
    from orcapod.sources import CSVSource

    (tmp_path / "a.csv").write_text("id,a\n0,1\n1,2\n")
    (tmp_path / "b.csv").write_text("id,b\n0,3\n1,4\n")

    def record() -> None:
        """Join two CSV sources, which are rebuilt on load."""
        left = CSVSource(str(tmp_path / "a.csv"), tag_columns=["id"], source_id="a")
        right = CSVSource(str(tmp_path / "b.csv"), tag_columns=["id"], source_id="b")
        _ = Join()(left, right, label="joined")

    job = PipelineJob.load(_save_small_job(tmp_path, record))
    assert job.unbound_sources == []
    return job


def test_loaded_operator_only_job_cannot_run(tmp_path: pathlib.Path) -> None:
    """A loaded job whose operators were rebuilt and sources bound still never runs."""
    job = _loaded_operator_only_job(tmp_path)
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _ = job.run()


@pytest.mark.parametrize("method", ["run", "execute", "async_execute"])
def test_loaded_operator_rejects_direct_execution(
    tmp_path: pathlib.Path, method: str
) -> None:
    """Every operator execution entry point respects the loaded-job read-only contract."""
    import asyncio
    from orcapod.channels import Channel

    job = _loaded_operator_only_job(tmp_path)
    node = job.nodes["joined"]
    if method == "async_execute":
        output: Channel[Any] = Channel()

        async def run() -> list[Any]:
            """Reject before consuming inputs and close the output on rejection."""
            with pytest.raises(RuntimeError, match="loaded.*cannot compute"):
                await node.async_execute([], output.writer)
            return [item async for item in output.reader]

        assert asyncio.run(asyncio.wait_for(run(), timeout=5)) == []
    else:
        args = (job.nodes["a"], job.nodes["b"]) if method == "execute" else ()
        with pytest.raises(RuntimeError, match="loaded.*cannot compute"):
            getattr(node, method)(*args)


def test_loaded_job_without_a_store_cannot_run(
    saved: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A loaded job without a store gets the read-only error, not 'requires a database'."""

    def drop_store(data: dict[str, Any]) -> None:
        """Remove the saved store configuration."""
        data["bindings"]["database"] = None

    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, drop_store))
    with pytest.raises(RuntimeError, match="cannot execute a loaded job"):
        _ = job.run()


def test_loaded_function_node_cannot_execute(saved: pathlib.Path) -> None:
    """``execute()`` on a loaded node raises: it has no function pod."""
    job = _load(saved)
    with pytest.raises(RuntimeError) as excinfo:
        _ = job.nodes["dbl"].execute(job.nodes["dsrc"])
    assert str(excinfo.value) == (
        "FunctionJobNode 'dbl' was loaded by PipelineJob.load and has no function "
        "pod, so it cannot compute; read its stored results with as_table() or "
        "iter_data()."
    )


def test_loaded_function_node_cannot_async_execute(saved: pathlib.Path) -> None:
    """``async_execute()`` on a loaded node raises and closes its output."""
    import asyncio

    from orcapod.channels import Channel

    node = _load(saved).nodes["dbl"]
    inputs: Channel[Any] = Channel()
    output: Channel[Any] = Channel()

    async def run() -> list[Any]:
        """Run the node, then drain its output (which must be closed)."""
        with pytest.raises(RuntimeError, match="has no function pod, so it cannot"):
            await node.async_execute(inputs.reader, output.writer)
        return [item async for item in output.reader]

    assert asyncio.run(asyncio.wait_for(run(), timeout=10)) == []


# ---------------------------------------------------------------------------
# Operator nodes
# ---------------------------------------------------------------------------


def test_loaded_operator_node_raises_instead_of_returning_empty(
    saved: pathlib.Path,
) -> None:
    """A loaded operator node, with nothing stored, raises and names its inputs."""
    node = _load(saved).nodes["joined"]
    expected = (
        "OperatorJobNode 'joined' has no stored results available for this read. "
        "Operator outputs require a stored cache and replay configuration. Read the stored results of "
        "its input nodes ('dbl', 'asrc') instead; an input source whose data was not "
        "saved must be bound first with job.bind(sources=...)."
    )
    for read in (node.as_table, lambda: list(node.iter_data())):
        with pytest.raises(RuntimeError) as excinfo:
            _ = read()
        assert str(excinfo.value) == expected


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------


def test_in_memory_sources_are_logged_at_info_with_the_remedy(
    saved: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """In-memory sources are logged at INFO with the consequence and remedy."""
    caplog.set_level(logging.DEBUG, logger="orcapod.pipeline.job")
    _ = _load(saved)
    job_records = [r for r in caplog.records if r.name == "orcapod.pipeline.job"]
    assert not [r for r in job_records if r.levelno >= logging.WARNING]
    messages = {r.getMessage() for r in job_records if r.levelno == logging.INFO}
    assert (
        "Source 'dsrc' (dict) keeps its data in memory and is not saved with the job; "
        "the loaded job has no data for it. Stored function-node results are still "
        "readable from the store. To read source node 'dsrc' itself, bind a source: "
        "job.bind(sources={'dsrc': <source>})."
    ) in messages
    assert any(m.startswith("Source 'asrc' (arrow_table) keeps") for m in messages)
    assert any(m.startswith("Source 'lsrc' (list) keeps") for m in messages)


def test_unreconstructable_file_source_is_a_warning_with_the_reason(
    saved: pathlib.Path, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A source that fails to rebuild is a WARNING carrying the reason."""
    missing = str(tmp_path / "gone.csv")

    def move_csv(data: dict[str, Any]) -> None:
        """Point the CSV source at a file that does not exist."""
        data["bindings"]["sources"]["csrc"]["file_path"] = missing

    caplog.set_level(logging.INFO, logger="orcapod.pipeline.job")
    job = PipelineJob.load(_rewrite_job_file(saved, tmp_path, move_csv))
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name == "orcapod.pipeline.job" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].startswith(
        "Could not reconstruct source 'csrc' (csv) from its saved config: "
    )
    assert "gone.csv" in warnings[0]
    assert warnings[0].endswith(
        "Stored function-node results are still readable from the store. To read "
        "source node 'csrc' itself, bind a source: job.bind(sources={'csrc': <source>})."
    )
    # The stored results downstream of the missing source are still readable.
    assert job.nodes["inc"].as_table().num_rows == 2


def test_unreconstructable_store_is_a_warning_with_the_remedy(
    saved: pathlib.Path, tmp_path: pathlib.Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A store that fails to rebuild is a WARNING with the remedy."""

    def break_store(data: dict[str, Any]) -> None:
        """Replace the store configuration with an unknown type."""
        data["bindings"]["database"] = {"type": "no_such_store_type"}

    caplog.set_level(logging.INFO, logger="orcapod.pipeline.job")
    _ = PipelineJob.load(_rewrite_job_file(saved, tmp_path, break_store))
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.name == "orcapod.pipeline.job" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert warnings[0].startswith(
        "Could not reconstruct the database from its saved config: "
    )
    assert warnings[0].endswith(
        "Node results cannot be read until a database is given: "
        "PipelineJob.load(path, database=...) or job.bind(database=...)."
    )


def test_file_source_nodes_are_readable_after_load(saved: pathlib.Path) -> None:
    """CSV and Delta sources are rebuilt, so their nodes are readable."""
    job = _load(saved)
    assert job.nodes["csrc"].as_table().num_rows == 2
    assert job.nodes["tsrc"].as_table().num_rows == 2


def test_in_memory_source_node_is_readable_after_bind(saved: pathlib.Path) -> None:
    """An in-memory source node is readable once a source is bound to it."""
    job = _load(saved)
    job.bind(sources={"dsrc": _dict_source()})
    assert job.nodes["dsrc"].as_table().num_rows == 2


@pytest.mark.parametrize("method", ["as_table", "iter_data"])
def test_loaded_replay_operator_reads_an_absent_cache_table_as_no_rows(
    tmp_path: pathlib.Path, method: str
) -> None:
    """The store is the source of truth: no table under the node's key is no rows.

    The job file records the operator's run content hash (ITL-1043), so the loaded node
    knows its key and reads as a running node does. A file without that hash raises
    instead; see ``test_loaded_operator_cache.py``.
    """
    from orcapod.types import CacheMode

    node = _loaded_operator_only_job(tmp_path).nodes["joined"]
    node.attach_databases(InMemoryArrowDatabase(), cache_mode=CacheMode.REPLAY)
    result = getattr(node, method)()
    if method == "iter_data":
        assert list(result) == []
    else:
        assert result.num_rows == 0


def test_loaded_job_is_never_runnable(tmp_path: pathlib.Path) -> None:
    """Bound file sources cannot make a loaded read-only job executable."""
    job = _loaded_operator_only_job(tmp_path)
    assert not job.is_runnable("joined")


def test_loaded_function_run_rejects_computation(saved: pathlib.Path) -> None:
    """A loaded function's run entry point rejects before setting up a run."""
    with pytest.raises(RuntimeError, match="loaded.*cannot compute"):
        _load(saved).nodes["dbl"].run()


@pytest.mark.parametrize(
    "ephemeral_modes", [(False,), (True,), (False, True), (True, False)]
)
def test_latest_readable_result_wins_across_both_stores(
    ephemeral_modes: tuple[bool, ...],
) -> None:
    """Readback picks the newest recomputation regardless of its result store."""
    import time
    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.protocols.core_protocols import FunctionPodProtocol

    def timestamp(x: int) -> int:
        """Produce a distinct result for each explicitly requested computation."""
        return time.monotonic_ns() + x

    source = _dict_source()
    pod = function_pod(output_keys="value")(timestamp).pod
    assert isinstance(pod, FunctionPodProtocol)
    node = FunctionJobNode(
        pod,
        source,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )
    node.set_ephemeral_store(InMemoryArrowDatabase())
    tag, data = next(source.iter_data())
    expected = None
    for ephemeral in ephemeral_modes:
        node.node_config = NodeConfig(is_result_ephemeral=ephemeral)
        _, result = node.execute_data(tag, data)
        assert result is not None
        expected = result["value"]
    node.clear_cache()
    assert node.as_table().column("value").to_pylist() == [expected]
    assert [result["value"] for _, result in node.iter_data()] == [expected]
