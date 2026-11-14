"""Saving and loading a job keeps every node's identity; a loaded operator reads its cache.

ITL-1043. The job joins a function node over one in-memory ``ArrowTableSource`` with a
second one and stores the join's output (``CacheMode.LOG``). Columns are declared nullable
or not. A job file records each column's nullability and the run content hash of every
operator node, so the loaded job reproduces each node's pipeline hash, content hash, table
path and output schema, and a loaded operator node attached in ``CacheMode.REPLAY`` reads
the rows its run stored. A file without those fields (``0.1.0``, or a ``0.2.0`` file written
before them) loads as before, lossy: the operator node still looks under its run's path,
and raises rather than show the stored rows as none.
"""

from __future__ import annotations

import json
import warnings
import pathlib
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.streams import HeadStatus
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import StoreContentWarning
from orcapod.pipeline import Pipeline, SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode

JOB_NAME = "loaded_operator_cache"
NULLABILITY = {
    "non_null": (False, False),
    "nullable_tag": (True, False),
    "nullable_data": (False, True),
    "nullable_tag_and_data": (True, True),
}
STORES = ["memory", "delta"]


@function_pod(output_keys="a1")
def inc(a: int) -> int:
    """Add one."""
    return a + 1


@function_pod(output_keys="a1")
def inc_nullable(a: int | None) -> int | None:
    """Add one to a value that may be missing."""
    return None if a is None else a + 1


def _source(column: str, tag_nullable: bool, data_nullable: bool) -> ArrowTableSource:
    """A two-row source with tag ``id`` and data ``column``, nulls allowed as declared."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=tag_nullable),
            pa.field(column, pa.int64(), nullable=data_nullable),
        ]
    )
    table = pa.table({"id": [0, 1], column: [1, 2]}, schema=schema)
    return ArrowTableSource(table, tag_columns=["id"], source_id=f"src_{column}")


def _sources(nullability: str) -> dict[str, StreamProtocol]:
    """The job's two sources, keyed by source node name."""
    tag_nullable, data_nullable = NULLABILITY[nullability]
    return {
        "src_a": _source("a", tag_nullable, data_nullable),
        "src_b": _source("b", tag_nullable, data_nullable),
    }


def _store(kind: str, tmp_path: pathlib.Path) -> ArrowDatabaseProtocol:
    """An empty store of the given kind."""
    if kind == "delta":
        return DeltaTableDatabase(base_path=tmp_path / "store")
    return InMemoryArrowDatabase()


def _run(
    store: ArrowDatabaseProtocol, nullability: str, with_function: bool = True
) -> PipelineJob:
    """Run the job with the join stored (``LOG``)."""
    _, data_nullable = NULLABILITY[nullability]
    sources = _sources(nullability)
    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        left: Any = sources["src_a"]
        if with_function:
            pod = inc_nullable if data_nullable else inc
            left = pod.pod(left, label="inc")
        _ = Join()(left, sources["src_b"], label="joined")
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    return job


def _save(
    job: PipelineJob,
    tmp_path: pathlib.Path,
    edit: Callable[[dict[str, Any]], None] | None = None,
) -> pathlib.Path:
    """Save ``job``, optionally rewriting the saved file with ``edit``."""
    saved = tmp_path / "job.json"
    job.save(saved)
    if edit is not None:
        data = json.loads(saved.read_text())
        edit(data)
        saved.write_text(json.dumps(data))
    return saved


def _load_replaying(saved: pathlib.Path, store: ArrowDatabaseProtocol) -> PipelineJob:
    """Load the job and attach its operator node in ``REPLAY`` mode."""
    loaded = PipelineJob.load(str(saved), database=store)
    loaded.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
    )
    return loaded


def _sorted(table: pa.Table) -> pa.Table:
    """``table`` with its columns in name order and its rows in ``id`` order."""
    return table.select(sorted(table.column_names)).sort_by("id")


def _drop_saved_nullability(data: dict[str, Any]) -> None:
    """Remove the recorded nullability, as in a file written before it existed."""
    for descriptor in data["pipeline"]["nodes"].values():
        del descriptor["output_schema"]["nullable"]
        if "source_config" in descriptor:
            del descriptor["source_config"]["nullable"]


def _drop_saved_operator_content_hash(data: dict[str, Any]) -> None:
    """Remove the join's run content hash, as in a file written before it existed."""
    del data["run"]["node_job_content_hashes"]["joined"]


# ---------------------------------------------------------------------------
# A file with the fields: every node round-trips, the operator reads its rows
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_every_node_keeps_its_identity_and_schema_through_save_and_load(
    store_kind: str, nullability: str, tmp_path: pathlib.Path
) -> None:
    """Pipeline hash, content hash, table path and output schema are the run's."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    loaded = PipelineJob.load(str(_save(job, tmp_path)), database=store)
    # In-memory sources are not saved: a source node's content hash is its bound
    # source's, so bind the same sources before comparing.
    loaded.bind(sources=_sources(nullability))
    assert set(loaded.nodes) == set(job.nodes)
    for label, run_node in job.nodes.items():
        loaded_node = loaded.nodes[label]
        assert loaded_node.pipeline_hash() == run_node.pipeline_hash(), label
        assert loaded_node.content_hash() == run_node.content_hash(), label
        assert loaded_node.output_schema() == run_node.output_schema(), label
        if label in ("inc", "joined"):
            assert loaded_node.node_identity_path == run_node.node_identity_path


@pytest.mark.parametrize("read", ["as_table", "iter_data", "head"])
@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_loaded_replay_operator_reads_the_rows_its_run_stored(
    store_kind: str, nullability: str, read: str, tmp_path: pathlib.Path
) -> None:
    """A loaded operator node in REPLAY reads exactly the rows read before saving."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    expected = _sorted(job.nodes["joined"].as_table())
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    if read == "as_table":
        assert _sorted(node.as_table()).equals(expected)
    elif read == "iter_data":
        assert len(list(node.iter_data())) == expected.num_rows
    else:
        assert _sorted(node.head().table).equals(expected)


@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_a_pipeline_loaded_and_bound_to_its_sources_reuses_the_operator_cache(
    nullability: str, tmp_path: pathlib.Path
) -> None:
    """``Pipeline.load`` + ``from_pipeline(sources=...)`` reads and writes the run's table."""
    store = InMemoryArrowDatabase()
    # Pipeline.load rebuilds operators but not function pods: an operator-only pipeline.
    job = _run(store, nullability, with_function=False)
    expected = _sorted(job.nodes["joined"].as_table())
    saved = tmp_path / "pipeline.json"
    job.pipeline.save(saved)
    tables_after_run = set(store.at(JOB_NAME).list_table_paths())

    def rebuilt(cache_mode: CacheMode) -> PipelineJob:
        """The saved pipeline bound to the same sources, its join in ``cache_mode``."""
        again = PipelineJob.from_pipeline(
            Pipeline.load(saved), database=store, sources=_sources(nullability)
        )
        again.nodes["joined"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=cache_mode
        )
        return again

    assert _sorted(rebuilt(CacheMode.REPLAY).nodes["joined"].as_table()).equals(
        expected
    )
    _ = rebuilt(CacheMode.LOG).run()
    assert set(store.at(JOB_NAME).list_table_paths()) == tables_after_run


# ---------------------------------------------------------------------------
# A file without the fields: lossy, but never stored rows shown as none
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_without_saved_nullability_the_operator_still_reads_its_runs_table(
    store_kind: str, nullability: str, tmp_path: pathlib.Path
) -> None:
    """The saved pipeline hash, not the lossy schemas, names the operator's table."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    saved = _save(job, tmp_path, _drop_saved_nullability)
    node = _load_replaying(saved, store).nodes["joined"]
    assert node.pipeline_hash() == job.nodes["joined"].pipeline_hash()
    assert node.node_identity_path == job.nodes["joined"].node_identity_path
    assert _sorted(node.as_table()).equals(_sorted(job.nodes["joined"].as_table()))


def _drop_both(data: dict[str, Any]) -> None:
    """Remove every field this fix adds, as in a file written before them."""
    _drop_saved_nullability(data)
    _drop_saved_operator_content_hash(data)


UNKNOWN_KEY = (
    r"'joined': the job file does not record the content hash of this node's run "
    r".*ITL-1048"
)


@pytest.mark.parametrize("read", ["as_table", "iter_data"])
@pytest.mark.parametrize("store_kind", STORES)
@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_without_the_run_content_hash_the_loaded_operator_raises(
    store_kind: str, nullability: str, read: str, tmp_path: pathlib.Path
) -> None:
    """A node that cannot name its run's rows never shows the stored rows as none."""
    store = _store(store_kind, tmp_path)
    job = _run(store, nullability)
    node = _load_replaying(_save(job, tmp_path, _drop_both), store).nodes["joined"]
    with pytest.raises(RuntimeError, match=UNKNOWN_KEY) as excinfo:
        result = getattr(node, read)()
        if read == "iter_data":
            _ = list(result)
    assert "holds 2 stored rows, none of them under the content hash" in str(
        excinfo.value
    )


def test_without_the_run_content_hash_an_absent_table_raises_too(
    tmp_path: pathlib.Path,
) -> None:
    """Without its run's content hash, an absent table is no answer either."""
    saved = _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path, _drop_both)
    node = _load_replaying(saved, InMemoryArrowDatabase()).nodes["joined"]
    with pytest.raises(RuntimeError, match=UNKNOWN_KEY) as excinfo:
        _ = node.as_table()
    assert "is absent from the attached store" in str(excinfo.value)


@pytest.mark.parametrize("store_kind", STORES)
def test_without_the_run_content_hash_head_says_why_it_shows_no_rows(
    store_kind: str, tmp_path: pathlib.Path
) -> None:
    """``head()`` never raises on a loaded node: it shows no rows and names the cause."""
    store = _store(store_kind, tmp_path)
    job = _run(store, "nullable_tag")
    node = _load_replaying(_save(job, tmp_path, _drop_both), store).nodes["joined"]
    head = node.head()
    assert head.status is HeadStatus.UNAVAILABLE
    assert head.table.num_rows == 0
    assert "does not record its run's content hash" in head.header()
    assert "ITL-1048" in head.header()


# ---------------------------------------------------------------------------
# The store is the source of truth: a known key with no rows is an empty result
# ---------------------------------------------------------------------------


def _live_replaying(store: ArrowDatabaseProtocol, job: PipelineJob) -> Any:
    """The join of ``job``'s definition rebuilt from code, in REPLAY over ``store``."""
    rebuilt = PipelineJob.from_pipeline(job.pipeline, database=store)
    rebuilt.bind(sources=job.sources)
    node = rebuilt.nodes["joined"]
    node.attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.REPLAY
    )
    return node


def _assert_reads_as_live_with_no_rows(loaded_node: Any, live_node: Any) -> None:
    """The loaded node reads what a running node reads from the same store: no rows."""
    for config in ({}, {"all_info": True}):
        table = loaded_node.as_table(**config)
        live = live_node.as_table(**config)
        assert table.num_rows == 0
        assert table.schema.equals(live.schema), (table.schema, live.schema)
    assert list(loaded_node.iter_data()) == []
    head = loaded_node.head()
    assert head.status is HeadStatus.NOTHING_STORED
    assert head.table.num_rows == 0
    assert "the store holds no rows for its run" in head.header()
    assert live_node.head().status is HeadStatus.NOTHING_STORED


def test_a_store_holding_only_other_runs_rows_gives_an_empty_result(
    tmp_path: pathlib.Path,
) -> None:
    """With its key known, a store without the run's rows answers: no rows."""
    saved_store = InMemoryArrowDatabase()
    job = _run(saved_store, "non_null")
    saved = _save(job, tmp_path)
    # Another store whose table at the same path holds another run's rows.
    other_store = InMemoryArrowDatabase()
    other = PipelineJob(name=JOB_NAME, database=other_store)
    with other:
        other_a = ArrowTableSource(
            pa.table(
                {"id": [0], "a": [9]},
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("a", pa.int64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["id"],
            source_id="other_a",
        )
        _ = Join()(
            inc.pod(other_a, label="inc"),
            _sources("non_null")["src_b"],
            label="joined",
        )
    other.nodes["joined"].attach_databases(
        pipeline_database=other_store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = other.run()
    node = _load_replaying(saved, other_store).nodes["joined"]
    # The job file records the run's 2 rows: an advisory warning, not an error.
    with pytest.warns(StoreContentWarning, match="records 2 rows .* holds 0"):
        _assert_reads_as_live_with_no_rows(node, _live_replaying(other_store, job))


@pytest.mark.parametrize("nullability", list(NULLABILITY))
def test_an_absent_cache_table_gives_an_empty_result(
    nullability: str, tmp_path: pathlib.Path
) -> None:
    """With its key known, a store without the table answers: no rows, typed."""
    job = _run(InMemoryArrowDatabase(), nullability)
    empty_store = InMemoryArrowDatabase()
    node = _load_replaying(_save(job, tmp_path), empty_store).nodes["joined"]
    # The job file records the run's 2 rows: an advisory warning, not an error.
    with pytest.warns(StoreContentWarning, match="records 2 rows .* holds 0"):
        _assert_reads_as_live_with_no_rows(node, _live_replaying(empty_store, job))


def _run_empty_join(store: ArrowDatabaseProtocol) -> PipelineJob:
    """Run the operator-only job over inputs with no common ``id``: an empty join."""

    def source(column: str, ids: list[int], source_id: str) -> ArrowTableSource:
        """A non-null source with tag ``id`` and data ``column``."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field(column, pa.int64(), nullable=False),
            ]
        )
        table = pa.table({"id": ids, column: [1] * len(ids)}, schema=schema)
        return ArrowTableSource(table, tag_columns=["id"], source_id=source_id)

    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        _ = Join()(
            source("a", [0, 1], "src_a"),
            source("b", [7, 8], "src_b_disjoint"),
            label="joined",
        )
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    return job


@pytest.mark.parametrize("other_runs_stored", [False, True])
def test_a_loaded_run_with_an_empty_result_reads_as_empty(
    other_runs_stored: bool, tmp_path: pathlib.Path
) -> None:
    """A run whose result was empty (it stores nothing) reads back as empty."""
    store = InMemoryArrowDatabase()
    if other_runs_stored:
        # Another run of the same pipeline stores rows in the shared table.
        _ = _run(store, "non_null", with_function=False)
    job = _run_empty_join(store)
    assert job.nodes["joined"].as_table().num_rows == 0
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    _assert_reads_as_live_with_no_rows(node, _live_replaying(store, job))


def test_an_empty_nullable_list_means_every_column_is_non_null(
    tmp_path: pathlib.Path,
) -> None:
    """A recorded empty list is a fact; only an absent field is unknown."""
    store = InMemoryArrowDatabase()
    job = _run(store, "non_null")
    data = json.loads(_save(job, tmp_path).read_text())
    joined = next(
        d for d in data["pipeline"]["nodes"].values() if d["label"] == "joined"
    )
    assert joined["output_schema"]["nullable"] == {"tag": [], "data": []}


# ---------------------------------------------------------------------------
# Saving a loaded job again: known stays known, unknown stays unknown
# ---------------------------------------------------------------------------


def _recorded_fields(saved: pathlib.Path) -> dict[str, Any]:
    """The nullability records and operator run hash a saved file holds."""
    data = json.loads(saved.read_text())
    records: dict[str, Any] = {}
    for descriptor in data["pipeline"]["nodes"].values():
        label = descriptor["label"]
        records[label] = descriptor["output_schema"].get("nullable")
        if "source_config" in descriptor:
            records[f"{label}.source_config"] = descriptor["source_config"].get(
                "nullable"
            )
    records["joined.run_content_hash"] = data["run"]["node_job_content_hashes"].get(
        "joined"
    )
    return records


@pytest.mark.parametrize("nullability", ["non_null", "nullable_tag_and_data"])
def test_saving_a_loaded_job_again_keeps_what_its_file_recorded(
    nullability: str, tmp_path: pathlib.Path
) -> None:
    """A re-saved file records the same nullability and operator run hash."""
    store = InMemoryArrowDatabase()
    saved = _save(_run(store, nullability), tmp_path)
    resaved = tmp_path / "resaved.json"
    PipelineJob.load(str(saved), database=store).save(resaved)
    assert _recorded_fields(resaved) == _recorded_fields(saved)


def test_saving_a_lossy_loaded_job_again_records_nothing_it_does_not_know(
    tmp_path: pathlib.Path,
) -> None:
    """Unknown nullability and run hash are not written as facts when re-saving."""
    store = InMemoryArrowDatabase()

    def drop_both(data: dict[str, Any]) -> None:
        """Remove every field this fix adds."""
        _drop_saved_nullability(data)
        _drop_saved_operator_content_hash(data)

    saved = _save(_run(store, "nullable_tag"), tmp_path, drop_both)
    resaved = tmp_path / "resaved.json"
    PipelineJob.load(str(saved), database=store).save(resaved)
    assert all(value is None for value in _recorded_fields(resaved).values())


def test_a_source_proxy_keeps_the_nullability_of_the_source_it_stands_for() -> None:
    """A source rebuilt as a ``SourceProxy`` from its config has the source's schema."""
    from orcapod.pipeline.serialization import resolve_source_from_config

    source = _source("a", tag_nullable=True, data_nullable=False)
    config = {**source.to_config(), "source_type": "no_such_source_type"}
    proxy = resolve_source_from_config(config, fallback_to_proxy=True)
    assert proxy.output_schema() == source.output_schema()
    assert proxy.pipeline_hash() == source.pipeline_hash()


# ---------------------------------------------------------------------------
# A real format 0.1.0 file (tests/test_pipeline/fixtures, written by ITL-1026)
# ---------------------------------------------------------------------------

FORMAT_0_1_0 = pathlib.Path(__file__).parents[1] / "fixtures" / "job_format_0_1_0.json"


def _add_values(value: int, score: int) -> int:
    """The fixture job's function."""
    return value + score


def _store_the_fixture_join(store: ArrowDatabaseProtocol) -> Any:
    """Run the fixture's job with today's code, its join stored; the run's join node.

    The fixture was saved before running, with in-memory sources, so no store holds
    its rows: this run stores them at the path the file's join node names.
    """
    from orcapod.core.data_function import PythonDataFunction
    from orcapod.core.function_pod import FunctionPod

    def source(column: str, values: list[int]) -> ArrowTableSource:
        """One of the fixture's two sources."""
        table = pa.table(
            {
                "key": pa.array(["a", "b"], type=pa.large_string()),
                column: pa.array(values, type=pa.int64()),
            }
        )
        return ArrowTableSource(
            table,
            tag_columns=["key"],
            infer_nullable=True,
            source_id=f"fixture_{column}",
        )

    name = json.loads(FORMAT_0_1_0.read_text())["pipeline"]["pipeline"]["name"]
    job = PipelineJob(name=tuple(name), database=store)
    with job:
        joined = Join()(source("value", [10, 20]), source("score", [100, 200]))
        function = PythonDataFunction(_add_values, output_keys="total")
        # As in the fixture's generator: a protocol mismatch shared with other tests.
        pod = FunctionPod(data_function=function)  # pyright: ignore[reportArgumentType]
        _ = pod(joined)
    run_join = job.nodes["Join"]
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    run_join.attach_databases(
        pipeline_database=store.at(*name), cache_mode=CacheMode.LOG
    )
    _ = job.run()
    return run_join


def test_a_format_0_1_0_file_finds_its_join_table_and_never_shows_its_rows_as_none() -> (
    None
):
    """The real 0.1.0 file: the run's path, ``head()`` says why, ``as_table()`` raises."""
    store = InMemoryArrowDatabase()
    run_join = _store_the_fixture_join(store)
    loaded = PipelineJob.load(str(FORMAT_0_1_0), database=store)
    node = loaded.nodes["Join"]
    name = json.loads(FORMAT_0_1_0.read_text())["pipeline"]["pipeline"]["name"]
    node.attach_databases(
        pipeline_database=store.at(*name), cache_mode=CacheMode.REPLAY
    )
    assert node.pipeline_hash() == run_join.pipeline_hash()
    assert node.node_identity_path == run_join.node_identity_path
    head = node.head()
    assert head.status is HeadStatus.UNAVAILABLE
    assert "ITL-1048" in head.header()
    with pytest.raises(
        RuntimeError, match=r"'Join': the job file does not record .*ITL-1048"
    ):
        _ = node.as_table()


# ---------------------------------------------------------------------------
# get_cached_output on a loaded node (CodeRabbit, PR #352)
# ---------------------------------------------------------------------------


def test_get_cached_output_of_a_loaded_node_reads_its_runs_rows(
    tmp_path: pathlib.Path,
) -> None:
    """``get_cached_output()`` on a loaded REPLAY node returns its run's rows."""
    store = InMemoryArrowDatabase()
    job = _run(store, "nullable_tag")
    node = _load_replaying(_save(job, tmp_path), store).nodes["joined"]
    cached = node.get_cached_output()
    assert cached is not None
    assert _sorted(cached.as_table()).equals(_sorted(job.nodes["joined"].as_table()))


def test_get_cached_output_of_a_loaded_node_never_caches_unreadable_rows_as_none(
    tmp_path: pathlib.Path,
) -> None:
    """``get_cached_output()`` raises as ``as_table()`` does; later reads raise too."""
    store = InMemoryArrowDatabase()
    job = _run(store, "nullable_tag")
    node = _load_replaying(_save(job, tmp_path, _drop_both), store).nodes["joined"]
    with pytest.raises(RuntimeError, match="the job file does not record"):
        _ = node.get_cached_output()
    with pytest.raises(RuntimeError, match="the job file does not record"):
        _ = node.as_table()


# ---------------------------------------------------------------------------
# A file whose nodes record nullability unevenly (CodeRabbit, PR #352)
# ---------------------------------------------------------------------------


def test_saving_again_drops_only_the_records_its_file_lacked(
    tmp_path: pathlib.Path,
) -> None:
    """A node without a record stays unrecorded; the other nodes keep theirs."""
    store = InMemoryArrowDatabase()

    def drop_src_a(data: dict[str, Any]) -> None:
        """Remove the records of source node ``src_a`` only."""
        for descriptor in data["pipeline"]["nodes"].values():
            if descriptor["label"] == "src_a":
                del descriptor["output_schema"]["nullable"]
                del descriptor["source_config"]["nullable"]

    saved = _save(_run(store, "nullable_tag_and_data"), tmp_path, drop_src_a)
    resaved = tmp_path / "resaved.json"
    PipelineJob.load(str(saved), database=store).save(resaved)
    assert _recorded_fields(resaved) == _recorded_fields(saved)
    assert _recorded_fields(resaved)["src_a"] is None
    assert _recorded_fields(resaved)["src_b"] == {"tag": ["id"], "data": ["b"]}


# ---------------------------------------------------------------------------
# run.expected_rows: an optional, advisory count; the store stays the truth
# ---------------------------------------------------------------------------

READ_NODES = ["inc", "joined"]


def _read_all(job: PipelineJob, label: str) -> pa.Table:
    """Read a loaded node three ways and return its table."""
    node = job.nodes[label]
    _ = list(node.iter_data())
    _ = node.head()
    return node.as_table()


def _loaded(saved: pathlib.Path, store: ArrowDatabaseProtocol) -> PipelineJob:
    """Load the job; its join replays (the file does not record its cache mode)."""
    return _load_replaying(saved, store)


def test_the_job_file_records_the_rows_each_node_has_in_the_store(
    tmp_path: pathlib.Path,
) -> None:
    """``run.expected_rows``: the run's rows under each node's key, in its own section."""
    data = json.loads(
        _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path).read_text()
    )
    assert data["run"]["expected_rows"] == {"inc": 2, "joined": 2}


@pytest.mark.parametrize("label", READ_NODES)
@pytest.mark.parametrize("store_kind", STORES)
def test_fewer_rows_than_expected_warn_once_and_read_what_the_store_holds(
    store_kind: str, label: str, tmp_path: pathlib.Path
) -> None:
    """Another store with none of the run's rows: no rows, one warning per node."""
    saved = _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path)
    loaded = _loaded(saved, _store(store_kind, tmp_path))
    with pytest.warns(StoreContentWarning) as record:
        table = _read_all(loaded, label)
        _ = _read_all(loaded, label)
    assert table.num_rows == 0
    messages = [str(w.message) for w in record if w.category is StoreContentWarning]
    assert len(messages) == 1
    assert f"Node {label!r}: the job file records 2 rows for its run" in messages[0]
    assert "holds 0 under the run's key" in messages[0]


@pytest.mark.parametrize("label", READ_NODES)
@pytest.mark.parametrize("store_kind", STORES)
def test_more_rows_than_expected_warn_and_read_them_all(
    store_kind: str, label: str, tmp_path: pathlib.Path
) -> None:
    """The store holds more than the file records: all of them, one warning."""
    store = _store(store_kind, tmp_path)
    job = _run(store, "non_null")

    def expect_one(data: dict[str, Any]) -> None:
        """Record one row where the run left two."""
        data["run"]["expected_rows"][label] = 1

    loaded = _loaded(_save(job, tmp_path, expect_one), store)
    with pytest.warns(StoreContentWarning, match="records 1 row for its run.*holds 2"):
        table = _read_all(loaded, label)
    assert _sorted(table).equals(_sorted(job.nodes[label].as_table()))


@pytest.mark.parametrize("store_kind", STORES)
def test_matching_counts_and_zero_expected_zero_found_do_not_warn(
    store_kind: str, tmp_path: pathlib.Path
) -> None:
    """No warning when the store holds what the file records, zero included."""
    store = _store(store_kind, tmp_path)
    job = _run(store, "non_null")
    empty = _run_empty_join(_store("memory", tmp_path))
    with warnings.catch_warnings():
        warnings.simplefilter("error", StoreContentWarning)
        loaded = _loaded(_save(job, tmp_path), store)
        for label in READ_NODES:
            assert _read_all(loaded, label).num_rows == 2
        empty_saved = _save(empty, tmp_path / "empty")
        assert json.loads(empty_saved.read_text())["run"]["expected_rows"] == {
            "joined": 0
        }
        node = _load_replaying(empty_saved, InMemoryArrowDatabase()).nodes["joined"]
        assert node.as_table().num_rows == 0


def test_deleting_the_section_by_hand_loads_identically_without_a_check(
    tmp_path: pathlib.Path,
) -> None:
    """Without ``run.expected_rows`` every read gives the same rows and no warning."""
    store = InMemoryArrowDatabase()
    job = _run(store, "nullable_tag_and_data")

    def delete_section(data: dict[str, Any]) -> None:
        """Delete the advisory section."""
        del data["run"]["expected_rows"]

    with_section = _loaded(_save(job, tmp_path), store)
    without = _loaded(_save(job, tmp_path / "bare", delete_section), store)
    other = _loaded(
        _save(job, tmp_path / "bare2", delete_section), InMemoryArrowDatabase()
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error", StoreContentWarning)
        for label in READ_NODES:
            assert _sorted(_read_all(without, label)).equals(
                _sorted(_read_all(with_section, label))
            )
            assert _read_all(other, label).num_rows == 0


def test_a_run_that_did_not_finish_records_no_counts(tmp_path: pathlib.Path) -> None:
    """Counts are written only when certain: not before a run, nor after a failed one."""
    store = InMemoryArrowDatabase()
    sources = _sources("non_null")
    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        _ = Join()(sources["src_a"], sources["src_b"], label="joined")
    pending = _save(job, tmp_path / "pending")
    assert "expected_rows" not in json.loads(pending.read_text())["run"]

    @function_pod(output_keys="a1")
    def fails(a: int) -> int:
        """Fail on every row."""
        raise ValueError(f"no {a}")

    failing = PipelineJob(name=JOB_NAME, database=store)
    with failing:
        _ = fails.pod(_sources("non_null")["src_a"], label="fails")
    with pytest.raises(ValueError, match="no 0|no 1"):
        _ = failing.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))
    failed = _save(failing, tmp_path / "failed")
    assert "expected_rows" not in json.loads(failed.read_text())["run"]


# ---------------------------------------------------------------------------
# CodeRabbit round 2 (PR #352)
# ---------------------------------------------------------------------------


def test_get_all_records_of_a_loaded_operator_runs_the_advisory_check(
    tmp_path: pathlib.Path,
) -> None:
    """``get_all_records()`` warns once, like the other reads, and returns no rows."""
    saved = _save(_run(InMemoryArrowDatabase(), "non_null"), tmp_path)
    node = _loaded(saved, InMemoryArrowDatabase()).nodes["joined"]
    with pytest.warns(StoreContentWarning) as record:
        assert node.get_all_records() is None
        assert node.get_all_records() is None
    assert len([w for w in record if w.category is StoreContentWarning]) == 1


def test_a_source_node_without_its_own_record_takes_its_output_schemas(
    tmp_path: pathlib.Path,
) -> None:
    """A source config without the record restores nullability from the output schema."""
    store = InMemoryArrowDatabase()
    job = _run(store, "nullable_tag_and_data")

    def drop_source_config_records(data: dict[str, Any]) -> None:
        """Remove only the source configs' records, by hand."""
        for descriptor in data["pipeline"]["nodes"].values():
            descriptor.get("source_config", {}).pop("nullable", None)

    saved = _save(job, tmp_path, drop_source_config_records)
    loaded = PipelineJob.load(str(saved), database=store)
    for label in ("src_a", "src_b"):
        assert loaded.nodes[label].output_schema() == job.nodes[label].output_schema()
    resaved = tmp_path / "resaved.json"
    loaded.save(resaved)
    fields = _recorded_fields(resaved)
    assert fields["src_a"] == {"tag": ["id"], "data": ["a"]}
    assert fields["src_a.source_config"] is None
