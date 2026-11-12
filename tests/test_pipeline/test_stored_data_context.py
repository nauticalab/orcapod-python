"""Every stored row names its data context; reads use rows of the current one (ITL-944).

Orcapod v0.3 writes the data context key ``std:v1:default`` into the column
``__data_context`` of every table it stores, and every table moved to a new layout
(the last path component): ``pdb_v2``, ``rdb_v2``, ``tdb_v2``, ``odb_v1``, ``sdb_v1``.

- Rows of the current data context are the current entries. Rows of another data context
  in the same table are never returned or reused, are left unchanged, and are reported
  once per table.
- A table at an older layout (written before v0.3, without the column) is counted and
  reported once, never read as current and never modified.
- Side-effect logs are the exception: an old-layout log, or logged deliveries of another
  data context, raise before anything is delivered.

Stores "written by v0.2" and rows "of another data context" are produced here from a store
this version wrote, through the public database interface: the same rows are stored at the
old-layout paths without the column, or under another data context key with other keys.
"""

from __future__ import annotations

import json
import logging
import pathlib
import shutil
from collections.abc import Callable
from importlib import resources
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import FunctionPod, PipelineJob, contexts, function_pod
from orcapod.contexts import stored_data_contexts
from orcapod.core.data_function import CachedDataFunction, PythonDataFunction
from orcapod.core.datagrams import Data
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import SelectDataColumns
from orcapod.core.sources.cached_source import CachedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import (
    InputValidationError,
    SideEffectLogDataContextError,
    StoreError,
)
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, sink_pod
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode, NodeConfig

Orchestrator = SyncPipelineOrchestrator | AsyncPipelineOrchestrator
Store = InMemoryArrowDatabase | DeltaTableDatabase

JOB_NAME = "itl944"
ROWS = [{"k": "a", "x": 1}, {"k": "b", "x": 2}]
CURRENT = "std:v1:default"
PRE_V1 = "std:v0.1:default"
OTHER = "std:v0.9:other"
# Added to the data values of rows stored under ``OTHER``.
OTHER_OFFSET = 100
COLUMN = "__data_context"
RID = "__rid"

calls: list[int] = []
delivered: list[int] = []


@function_pod(output_keys="double")
def double(x: int) -> int:
    """Double ``x`` and record the call."""
    calls.append(x)
    return 2 * x


def _deliver(double: int, ctx: Any) -> None:
    """The side effect of the sink: record what was delivered."""
    delivered.append(double)


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Forget the calls and deliveries of earlier tests."""
    calls.clear()
    delivered.clear()


def build_job(
    store: Store,
    *,
    with_operator: bool = False,
    with_sink: bool = False,
    cache_mode: CacheMode = CacheMode.LOG,
) -> PipelineJob:
    """A job with function node ``dbl`` and optionally a stored operator ``sel`` and a sink."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(data=ROWS, tag_columns=["k"], source_id="src")
        out = double.pod(source, label="dbl")
        if with_operator:
            SelectDataColumns(["double"])(out, label="sel")
        if with_sink:
            sink = sink_pod(_deliver)
            assert isinstance(sink, SideEffectPod)
            sink.process(out, label="sink")
    if with_operator:
        # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=cache_mode
        )
    return job


def _cached_source(store: Store) -> CachedSource:
    """A two-row source cached in ``store``."""
    table = pa.table({"k": ["a", "b"], "x": [1, 2]})
    return CachedSource(
        ArrowTableSource(table, tag_columns=["k"], source_id="fixed"),
        cache_database=store,
    )


# Root directory of each Delta store handed out by the ``new_store`` fixture.
_delta_roots: dict[int, pathlib.Path] = {}


def reopened(store: Store) -> Store:
    """A database object that sees everything written to ``store`` so far.

    A ``DeltaTableDatabase`` keeps the table version it read first, so rows written
    later through another view of the same directory need a new object to be seen.
    """
    if isinstance(store, DeltaTableDatabase):
        return DeltaTableDatabase(_delta_roots[id(store)])
    return store


def delta_at(root: pathlib.Path) -> DeltaTableDatabase:
    """A Delta store rooted at ``root`` that ``reopened`` can reopen."""
    store = DeltaTableDatabase(root)
    _delta_roots[id(store)] = root
    return store


def snapshot(store: Store) -> dict[tuple[str, ...], list[dict[str, Any]]]:
    """Every table of ``store`` with its rows, record ids included."""
    store = reopened(store)
    tables: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for path in sorted(store.list_sources()):
        records = store.get_all_records(path, record_id_column=RID)
        tables[path] = [] if records is None else records.to_pylist()
    return tables


def delta_files(root: pathlib.Path) -> dict[str, bytes]:
    """Every file under ``root`` with its bytes."""
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


@pytest.fixture(params=["in_memory", "delta"])
def new_store(
    request: pytest.FixtureRequest, tmp_path: pathlib.Path
) -> Callable[[], Store]:
    """A factory of fresh, empty stores of one backend."""
    made: list[Store] = []

    def make() -> Store:
        """Create an empty store of the parametrized backend and remember it."""
        if request.param == "in_memory":
            store: Store = InMemoryArrowDatabase()
        else:
            store = delta_at(tmp_path / f"store{len(made)}")
        made.append(store)
        return store

    return make


@pytest.fixture
def store(new_store: Callable[[], Store]) -> Store:
    """One fresh, empty store."""
    return new_store()


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Orchestrator:
    """The synchronous or the asynchronous orchestrator."""
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


def old_layout_path(path: tuple[str, ...]) -> tuple[str, ...]:
    """Where Orcapod v0.2 stored the table that this version stores at ``path``."""
    layout = path[-1]
    renamed = {"pdb_v2": "pdb_v1", "rdb_v2": "rdb_v1", "tdb_v2": "tdb_v1"}
    if layout in renamed:
        return path[:-1] + (renamed[layout],)
    assert layout in ("odb_v1", "sdb_v1"), path
    return path[:-1]


# Columns holding a hash-derived key of a stored row.
KEY_COLUMNS = (RID, "__pipeline_base_entry_id", "__input_data_hash", "record_id_hash")


def _other_key(value: Any) -> Any:
    """A stored key as another generation of hashing would have written it: not this one."""
    return b"other:" + value if isinstance(value, bytes) else value


def copy_as_v02(written: Store, target: Store) -> None:
    """Store the rows of ``written`` in ``target`` the way Orcapod v0.2 laid them out."""
    source = reopened(written)
    for path in sorted(source.list_sources()):
        rows = source.get_all_records(path, record_id_column=RID)
        assert rows is not None
        target.add_records(
            old_layout_path(path), rows.drop([COLUMN]), record_id_column=RID
        )
    target.flush()


def copy_as_other_context(written: Store, target: Store) -> None:
    """Store the rows of ``written`` in ``target`` as rows of another data context.

    Same tables, same layout; the rows carry the key ``OTHER`` and the Arrow-derived
    keys another generation of hashing would have produced.
    """
    source = reopened(written)
    for path in sorted(source.list_sources()):
        rows = source.get_all_records(path, record_id_column=RID)
        assert rows is not None
        columns = {
            name: pa.array(
                [
                    _other_key(value)
                    if name in KEY_COLUMNS
                    # Other data values too, so a read that picks these rows shows.
                    else cast(int, value) + OTHER_OFFSET
                    if name in ("double", "x")
                    else value
                    for value in rows.column(name).to_pylist()
                ],
                type=rows.schema.field(name).type,
            )
            for name in rows.column_names
        }
        columns[COLUMN] = pa.array([OTHER] * rows.num_rows, type=pa.large_string())
        target.add_records(
            path, pa.table(columns, schema=rows.schema), record_id_column=RID
        )
    target.flush()


def copy_with_results_of_other_context(written: Store, target: Store) -> None:
    """Copy ``written`` to ``target``, relabelling only the result rows as ``OTHER``.

    The pipeline rows stay rows of the current data context and still point, by
    record id, at result rows that now belong to another data context.
    """
    source = reopened(written)
    for path in sorted(source.list_sources()):
        rows = source.get_all_records(path, record_id_column=RID)
        assert rows is not None
        if path[-1] == "rdb_v2":
            index = rows.schema.get_field_index(COLUMN)
            other = pa.array([OTHER] * rows.num_rows, type=pa.large_string())
            rows = rows.set_column(index, rows.schema.field(index), other)
        target.add_records(path, rows, record_id_column=RID)
    target.flush()


def run_everything(store: Store) -> None:
    """Write every table kind: function, operator cache, side-effect log, cached source."""
    _ = build_job(store, with_operator=True, with_sink=True).run()
    _ = _cached_source(store).as_table()
    calls.clear()
    delivered.clear()


def v02_store(new_store: Callable[[], Store], *, with_sink: bool = False) -> Store:
    """A store holding only what Orcapod v0.2 would have written."""
    written = new_store()
    _ = build_job(written, with_operator=True, with_sink=with_sink).run()
    _ = _cached_source(written).as_table()
    target = new_store()
    copy_as_v02(written, target)
    calls.clear()
    delivered.clear()
    return target


def other_context_store(
    new_store: Callable[[], Store], *, with_sink: bool = False
) -> Store:
    """A store at the current layout holding only rows of another data context."""
    written = new_store()
    _ = build_job(written, with_operator=True, with_sink=with_sink).run()
    _ = _cached_source(written).as_table()
    target = new_store()
    copy_as_other_context(written, target)
    calls.clear()
    delivered.clear()
    return target


def both_contexts_store(new_store: Callable[[], Store]) -> Store:
    """A store whose tables hold rows of another data context and of the current one."""
    store = other_context_store(new_store)
    _ = build_job(store, with_operator=True).run()
    _ = _cached_source(store).as_table()
    calls.clear()
    return store


def rows_of(table: pa.Table | None, column: str = "double") -> list[Any]:
    """The sorted values of ``column``; empty for a missing table."""
    return [] if table is None else sorted(table[column].to_pylist())


def table_path(store: Store, layout: str) -> tuple[str, ...]:
    """The one stored table whose layout component is ``layout``."""
    (path,) = [p for p in reopened(store).list_sources() if p[-1] == layout]
    return path


LAYOUTS = ("pdb_v2", "rdb_v2", "odb_v1", "sdb_v1", "tdb_v2")


# ---------------------------------------------------------------------------
# The column and the layouts
# ---------------------------------------------------------------------------


class TestColumnAndLayout:
    """What this version writes: the layout components and the ``__data_context``
    column.
    """

    def test_every_table_is_at_the_new_layout(self, store: Store):
        """The last path component of each written table is one of ``LAYOUTS``."""
        run_everything(store)
        layouts = sorted(path[-1] for path in reopened(store).list_sources())
        assert layouts == sorted(LAYOUTS)

    def test_explicit_cache_path_ends_with_the_layout_component(self, store: Store):
        """A ``cache_path`` given to ``CachedSource`` gets ``sdb_v1`` appended."""
        table = pa.table({"k": ["a"], "x": [1]})
        source = CachedSource(
            ArrowTableSource(table, tag_columns=["k"], source_id="fixed"),
            cache_database=store,
            cache_path=("my", "cache"),
        )
        _ = source.as_table()
        assert source.cache_path == ("my", "cache", "sdb_v1")
        assert reopened(store).list_sources() == [("my", "cache", "sdb_v1")]

    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_every_row_names_the_current_data_context(self, store: Store, layout: str):
        """Each table kind has a ``large_string`` ``__data_context`` column of
        ``CURRENT``.
        """
        run_everything(store)
        records = reopened(store).get_all_records(table_path(store, layout))
        assert records is not None
        assert records.schema.field(COLUMN).type == pa.large_string()
        assert records.column(COLUMN).to_pylist() == [CURRENT, CURRENT]

    def test_the_old_context_columns_are_gone(self, store: Store):
        """No table has a ``_context_key`` or ``__input_data_context_key`` column."""
        run_everything(store)
        for path in reopened(store).list_sources():
            records = reopened(store).get_all_records(path)
            assert records is not None
            assert "_context_key" not in records.column_names, path
            assert "__input_data_context_key" not in records.column_names, path

    def test_node_tables_show_the_stored_column_only_as_a_meta_column(
        self, store: Store
    ):
        """``__data_context`` is bookkeeping: hidden by default, like every ``__`` column."""
        _ = build_job(store, with_operator=True).run()
        job = build_job(reopened(store), with_operator=True)
        for label in ("dbl", "sel"):
            assert COLUMN not in job.nodes[label].as_table().column_names
            records = job.nodes[label].get_all_records()
            assert records is not None
            assert COLUMN not in records.column_names
        with_meta = job.nodes["dbl"].get_all_records(all_info=True)
        assert with_meta is not None
        assert with_meta.column(COLUMN).to_pylist() == [CURRENT, CURRENT]

    def test_store_written_by_this_version_is_reused(
        self, store: Store, orchestrator: Orchestrator
    ):
        """A second run calls and delivers nothing and yields rows of ``CURRENT``."""
        _ = build_job(store, with_operator=True, with_sink=True).run(
            orchestrator=orchestrator
        )
        calls.clear()
        delivered.clear()

        job = build_job(store, with_operator=True, with_sink=True)
        _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert delivered == []
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        for _, data in job.nodes["dbl"].iter_data():
            assert data.data_context_key == CURRENT


# ---------------------------------------------------------------------------
# The function version in the result table
# ---------------------------------------------------------------------------


def _triple(x: int) -> int:
    """Triple ``x``."""
    return 3 * x


@function_pod(output_keys="tripled", version="v3.7")
def triple_v3_7(x: int) -> int:
    """Triple ``x``; declared with version ``v3.7``."""
    return 3 * x


def _stored_function_versions(store: Store, pod: Any) -> list[str]:
    """Run ``pod`` over ``ROWS`` in a job and return the versions its result rows hold."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        pod(DictSource(data=ROWS, tag_columns=["k"], source_id="src"), label="fn")
    _ = job.run()
    records = reopened(store).get_all_records(table_path(store, "rdb_v2"))
    assert records is not None
    return cast("list[str]", records.column("__pf_var_function_version").to_pylist())


class TestFunctionVersionColumn:
    """Every result row holds the full version string of the function that produced it."""

    def test_declared_version_is_stored_in_full(self, store: Store):
        """A function declared with ``version="v3.7"`` stores ``v3.7`` on its rows."""
        versions = _stored_function_versions(store, triple_v3_7.pod)
        assert versions == ["v3.7", "v3.7"]

    def test_function_pod_decorator_default_version(self, store: Store):
        """A decorated function without a version stores the default version, ``v0.0``."""
        assert _stored_function_versions(store, double.pod) == ["v0.0", "v0.0"]

    def test_from_fn_default_version(self, store: Store):
        """A pod from ``FunctionPod.from_fn`` stores the same default version, ``v0.0``."""
        pod = FunctionPod.from_fn(_triple, output_keys="tripled")
        assert _stored_function_versions(store, pod) == ["v0.0", "v0.0"]

    def test_version_is_read_back_with_the_meta_columns(self, store: Store):
        """The version column shows with ``all_info=True`` and not in ``as_table()``."""

        def build(database: Store) -> PipelineJob:
            """A job running ``triple_v3_7`` over ``ROWS`` on ``database``."""
            job = PipelineJob(name=JOB_NAME, store=database)
            with job:
                triple_v3_7.pod(
                    DictSource(data=ROWS, tag_columns=["k"], source_id="src"),
                    label="fn",
                )
            return job

        _ = build(store).run()
        # Read through a newly built job: a Delta database object that read a table
        # before the run does not see the rows the run added to it.
        node = build(reopened(store)).nodes["fn"]
        records = node.get_all_records(all_info=True)
        assert records is not None
        assert records.column("__pf_var_function_version").to_pylist() == ["v3.7"] * 2
        assert "__pf_var_function_version" not in node.as_table().column_names


# ---------------------------------------------------------------------------
# A job file saved by Orcapod v0.2
# ---------------------------------------------------------------------------


def _saved_by_v02(job: PipelineJob, path: pathlib.Path) -> None:
    """Save ``job`` and rewrite the file as v0.2 wrote it: every node under the old key."""
    job.save(str(path))
    text = path.read_text()
    assert f'"data_context_key": "{CURRENT}"' in text
    path.write_text(
        text.replace(
            f'"data_context_key": "{CURRENT}"', f'"data_context_key": "{PRE_V1}"'
        )
    )


class TestJobSavedByV02:
    """Loading a job file that names the pre-v1 data context on every node."""

    def test_it_loads_under_the_current_data_context_and_shows_no_rows(
        self,
        new_store: Callable[[], Store],
        tmp_path: pathlib.Path,
        caplog: pytest.LogCaptureFixture,
    ):
        """Loading over a v0.2 store warns about both contexts and reads no rows."""
        written = new_store()
        completed = build_job(written, with_operator=True)
        _ = completed.run()
        saved = tmp_path / "job.json"
        _saved_by_v02(completed, saved)
        store = new_store()
        copy_as_v02(written, store)
        calls.clear()

        with caplog.at_level(logging.WARNING, logger="orcapod"):
            loaded = PipelineJob.load(str(saved), store=store)

        for label in ("dbl", "sel"):
            assert loaded.nodes[label].get_all_records() is None
        warnings = [
            r.getMessage() for r in caplog.records if "saved under" in r.getMessage()
        ]
        assert warnings
        assert all(PRE_V1 in w and CURRENT in w for w in warnings)

    def test_after_a_run_of_this_version_it_shows_the_new_rows(
        self, new_store: Callable[[], Store], tmp_path: pathlib.Path
    ):
        """Once this version has run on the store, the loaded job reads its rows."""
        written = new_store()
        completed = build_job(written, with_operator=True)
        _ = completed.run()
        saved = tmp_path / "job.json"
        _saved_by_v02(completed, saved)
        store = new_store()
        copy_as_v02(written, store)

        _ = build_job(store, with_operator=True).run()
        loaded = PipelineJob.load(str(saved), store=reopened(store))

        assert rows_of(loaded.nodes["dbl"].get_all_records()) == [2, 4]
        assert rows_of(loaded.nodes["sel"].get_all_records()) == [2, 4]


# ---------------------------------------------------------------------------
# Tables at an older layout
# ---------------------------------------------------------------------------


class TestOldLayout:
    """Tables at a v0.2 layout are skipped, reported and left unchanged."""

    def test_old_tables_are_not_read_and_inputs_are_recomputed(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """A run over a v0.2 store calls the function for every input."""
        store = v02_store(new_store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)

        assert sorted(calls) == [1, 2]
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]

    def test_old_tables_alone_give_no_rows(self, new_store: Callable[[], Store]):
        """In ``REPLAY`` mode a v0.2 store yields no rows and no function call."""
        store = v02_store(new_store)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        node = job.nodes["dbl"]
        assert node.as_table().num_rows == 0
        assert node.get_all_records() is None
        assert job.nodes["sel"].get_all_records() is None
        assert calls == []

    def test_old_tables_stay_unchanged(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """A run adds new tables and leaves the rows of the old ones as they were."""
        store = v02_store(new_store)
        before = snapshot(store)

        _ = build_job(store, with_operator=True).run(orchestrator=orchestrator)
        _ = _cached_source(store).as_table()

        after = snapshot(store)
        for path, old_rows in before.items():
            assert after[path] == old_rows, path
        assert set(before) < set(after)

    def test_delta_files_written_by_v02_are_byte_identical(
        self, tmp_path: pathlib.Path
    ):
        """A run on a Delta store adds files and changes none of the old ones."""
        written = DeltaTableDatabase(tmp_path / "written")
        _ = build_job(written, with_operator=True).run()
        root = tmp_path / "store"
        copy_as_v02(delta_at(tmp_path / "written"), delta_at(root))
        before = delta_files(root)

        _ = build_job(DeltaTableDatabase(root), with_operator=True).run()

        after = delta_files(root)
        for name, content in before.items():
            assert after.get(name) == content, name
        assert len(after) > len(before)

    def test_cached_source_ignores_and_keeps_an_old_cache(
        self, new_store: Callable[[], Store]
    ):
        """A cached source reads from its source and leaves the old cache table alone."""
        store = v02_store(new_store)
        before = snapshot(store)

        source = _cached_source(store)
        assert rows_of(source.as_table(), "x") == [1, 2]
        assert rows_of(source.get_all_records(), "x") == [1, 2]
        # The rows came from the source, not from the old cache (which holds the
        # same values): they were written to a cache of this version's layout.
        assert source.cache_path[-1] == "sdb_v1"
        assert stored_data_contexts(reopened(store), source.cache_path) == {CURRENT: 2}

        after = snapshot(store)
        for path, old_rows in before.items():
            assert after[path] == old_rows, path

    def test_each_old_table_is_reported_once_with_its_row_count(
        self,
        new_store: Callable[[], Store],
        orchestrator: Orchestrator,
        caplog: pytest.LogCaptureFixture,
    ):
        """Each of the four old tables gets one warning with its row count and context."""
        store = v02_store(new_store)
        old_paths = ["/".join(path) for path in sorted(reopened(store).list_sources())]

        with caplog.at_level(logging.WARNING, logger="orcapod"):
            job = build_job(store, with_operator=True)
            _ = job.run(orchestrator=orchestrator)
            _ = job.nodes["dbl"].as_table()
            _ = _cached_source(store).as_table()

        reports = [
            r.getMessage()
            for r in caplog.records
            if "older table layout" in r.getMessage()
        ]
        assert len(reports) == len(old_paths) == 4
        for path in old_paths:
            (report,) = [r for r in reports if repr(path) in r]
            assert report.startswith("Found 2 rows in stored table")
            assert PRE_V1 in report
            assert "left unchanged" in report

    def test_tables_without_a_layout_component_are_counted_too(
        self,
        new_store: Callable[[], Store],
        orchestrator: Orchestrator,
        caplog: pytest.LogCaptureFixture,
    ):
        """Function tables of Orcapod v0.1 had no layout component: also an old layout."""
        written = new_store()
        _ = build_job(written).run()
        store = new_store()
        source = reopened(written)
        bare_paths = []
        for path in sorted(source.list_sources()):
            rows = source.get_all_records(path, record_id_column=RID)
            assert rows is not None
            store.add_records(path[:-1], rows.drop([COLUMN]), record_id_column=RID)
            bare_paths.append("/".join(path[:-1]))
        store.flush()
        before = snapshot(store)
        calls.clear()

        with caplog.at_level(logging.WARNING, logger="orcapod"):
            job = build_job(store)
            _ = job.run(orchestrator=orchestrator)

        assert sorted(calls) == [1, 2]
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        after = snapshot(store)
        for path, old_rows in before.items():
            assert after[path] == old_rows, path
        reports = [
            r.getMessage()
            for r in caplog.records
            if "older table layout" in r.getMessage()
        ]
        assert len(reports) == 2
        for path in bare_paths:
            (report,) = [r for r in reports if repr(path) in r]
            assert report.startswith("Found 2 rows in stored table")

    def test_the_v0_migration_machinery_is_gone(self):
        """``SchemaVersionError``, ``ignore_schema`` and ``orcapod.migrations`` no
        longer exist.
        """
        import orcapod.errors
        from orcapod.types import NodeConfig

        assert not hasattr(orcapod.errors, "SchemaVersionError")
        assert "ignore_schema" not in NodeConfig.__dataclass_fields__
        with pytest.raises(ModuleNotFoundError):
            __import__("orcapod.migrations")

    def test_nothing_is_reported_for_a_store_of_this_version(
        self, store: Store, caplog: pytest.LogCaptureFixture
    ):
        """A rerun on a store of this version logs no data context warning."""
        run_everything(store)
        with caplog.at_level(logging.WARNING, logger="orcapod"):
            run_everything(store)
        assert [r for r in caplog.records if "data context" in r.getMessage()] == []


# ---------------------------------------------------------------------------
# Rows of another data context in a table at the current layout
# ---------------------------------------------------------------------------


class TestRowsOfAnotherDataContext:
    """Rows of another data context in a current-layout table are skipped."""

    def test_they_are_ignored_and_inputs_are_recomputed(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """A run recomputes every input and adds its rows beside the foreign ones."""
        store = other_context_store(new_store)
        before = snapshot(store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)
        assert rows_of(_cached_source(store).as_table(), "x") == [1, 2]

        assert sorted(calls) == [1, 2]
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]
        after = snapshot(store)
        for path, old_rows in before.items():
            assert len(after[path]) == 2 * len(old_rows), path
            for row in old_rows:
                assert row in after[path], path

    def test_alone_they_give_no_rows(self, new_store: Callable[[], Store]):
        """In ``REPLAY`` mode a store of only foreign rows yields no rows and no call."""
        store = other_context_store(new_store)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        node = job.nodes["dbl"]
        assert node.as_table().num_rows == 0
        assert node.get_all_records() is None
        assert list(node.iter_data()) == []
        assert job.nodes["sel"].get_all_records() is None
        assert calls == []

    def test_function_node_reads_current_rows_only(
        self, new_store: Callable[[], Store]
    ):
        """Every read method of a function node returns only the rows of ``CURRENT``."""
        store = both_contexts_store(new_store)
        node = build_job(store).nodes["dbl"]
        assert rows_of(node.as_table()) == [2, 4]
        assert rows_of(node.get_all_records()) == [2, 4]
        assert sorted(d.as_dict()["double"] for _, d in node.iter_data()) == [2, 4]
        assert rows_of(node.as_source().as_table()) == [2, 4]

    def test_current_rows_are_reused(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """With rows of both contexts stored, a run calls nothing and writes nothing."""
        store = both_contexts_store(new_store)
        before = snapshot(store)

        job = build_job(store, with_operator=True)
        _ = job.run(orchestrator=orchestrator)

        assert calls == []
        assert rows_of(job.nodes["dbl"].as_table()) == [2, 4]
        assert snapshot(store) == before

    def test_operator_node_reads_current_rows_only(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """An operator node in ``REPLAY`` mode returns only the rows of ``CURRENT``."""
        store = both_contexts_store(new_store)
        job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
        assert rows_of(job.nodes["sel"].get_all_records()) == [2, 4]
        _ = job.run(orchestrator=orchestrator)
        assert rows_of(job.nodes["sel"].as_table()) == [2, 4]

    def test_a_result_of_another_data_context_is_not_joined_to_a_current_entry(
        self, new_store: Callable[[], Store]
    ):
        """A current pipeline row whose result row is of another data context has no result."""
        written = new_store()
        _ = build_job(written).run()
        store = new_store()
        copy_with_results_of_other_context(written, store)
        calls.clear()

        node = build_job(store).nodes["dbl"]
        records = node.get_all_records()

        assert records is None or records.num_rows == 0
        assert calls == []

    def test_cached_source_reads_current_rows_only(
        self, new_store: Callable[[], Store]
    ):
        """A cached source returns only the cached rows of ``CURRENT``."""
        store = both_contexts_store(new_store)
        source = _cached_source(store)
        assert rows_of(source.as_table(), "x") == [1, 2]
        assert rows_of(source.get_all_records(), "x") == [1, 2]

    def test_result_lookup_matches_current_rows_only(self, store: Store):
        """A result stored under another data context for the same input hash is not used."""
        cached = CachedDataFunction(double.pod.data_function, result_database=store)
        _ = cached.call(Data({"x": 1}))
        (path,) = reopened(store).list_sources()
        rows = reopened(store).get_all_records(path, record_id_column=RID)
        assert rows is not None
        # The same input hash under another data context, newer, with another output.
        forged = rows.to_pylist()[0] | {
            RID: b"another-row-id16",
            COLUMN: OTHER,
            "double": 999,
            "__pod_ts": rows.column("__pod_ts")[0].as_py().replace(year=2099),
        }
        store.add_records(
            path,
            pa.Table.from_pylist([forged], schema=rows.schema),
            record_id_column=RID,
        )
        store.flush()
        calls.clear()

        again = CachedDataFunction(
            double.pod.data_function, result_database=reopened(store)
        )
        output = again.call(Data({"x": 1}))

        assert output is not None
        assert output.as_dict()["double"] == 2
        assert calls == []
        assert rows_of(again.get_all_cached_outputs()) == [2]

    def test_skipped_rows_are_reported_once_per_table(
        self,
        new_store: Callable[[], Store],
        orchestrator: Orchestrator,
        caplog: pytest.LogCaptureFixture,
    ):
        """The pipeline and operator tables each get one warning counting skipped rows."""
        store = both_contexts_store(new_store)
        caplog.clear()  # the store was prepared by a run that reported too
        with caplog.at_level(logging.WARNING, logger="orcapod"):
            job = build_job(store, with_operator=True, cache_mode=CacheMode.REPLAY)
            _ = job.run(orchestrator=orchestrator)

        reports = [
            r.getMessage()
            for r in caplog.records
            if "another data context" in r.getMessage()
        ]
        for layout in ("pdb_v2", "odb_v1"):
            path = "/".join(table_path(store, layout))
            (report,) = [r for r in reports if repr(path) in r]
            assert report.startswith("Skipped 2 rows of stored table")
            assert "left unchanged" in report


# ---------------------------------------------------------------------------
# Side effects
# ---------------------------------------------------------------------------


def _sink_job(store: Store) -> PipelineJob:
    """A job whose only stored node is a sink reading the source directly."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(
            data=[{"k": "a", "double": 2}, {"k": "b", "double": 4}],
            tag_columns=["k"],
            source_id="src",
        )
        sink = sink_pod(_deliver)
        assert isinstance(sink, SideEffectPod)
        sink.process(source, label="sink")
    return job


def _sink_store(
    new_store: Callable[[], Store], copy: Callable[[Store, Store], None]
) -> Store:
    """A store holding the invocation log of ``_sink_job``, rewritten by ``copy``."""
    written = new_store()
    _ = _sink_job(written).run()
    assert sorted(delivered) == [2, 4]
    delivered.clear()
    target = new_store()
    copy(written, target)
    return target


class TestSideEffects:
    """A side effect is an external action, so an old log fails instead of delivering again."""

    def test_an_old_layout_log_raises_and_nothing_is_delivered(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """A ``tdb_v1`` log raises an error naming the log, both contexts and the node."""
        store = _sink_store(new_store, copy_as_v02)
        before = snapshot(store)

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _sink_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        error = info.value
        assert isinstance(error, StoreError)
        assert error.found_context == PRE_V1
        assert error.current_context == CURRENT
        assert error.node_label == "sink"
        assert error.record_path[-1] == "tdb_v1"
        assert error.record_paths == (error.record_path,)
        message = str(error)
        assert "/".join(error.record_path) in message
        assert repr(PRE_V1) in message
        assert repr(CURRENT) in message
        assert "delivered" in message
        assert "migrat" in message

    def test_logged_deliveries_of_another_data_context_raise(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """A ``tdb_v2`` log holding rows of ``OTHER`` raises and nothing is delivered."""
        store = _sink_store(new_store, copy_as_other_context)
        before = snapshot(store)

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _sink_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        assert info.value.found_context == OTHER
        assert info.value.current_context == CURRENT
        assert info.value.record_path[-1] == "tdb_v2"

    def test_delta_log_files_are_untouched(self, tmp_path: pathlib.Path):
        """The refused run leaves every file of the Delta store byte for byte."""
        _ = _sink_job(DeltaTableDatabase(tmp_path / "written")).run()
        delivered.clear()
        root = tmp_path / "store"
        copy_as_v02(delta_at(tmp_path / "written"), delta_at(root))
        before = delta_files(root)

        with pytest.raises(SideEffectLogDataContextError):
            _ = _sink_job(DeltaTableDatabase(root)).run()

        assert delivered == []
        assert delta_files(root) == before

    def test_function_node_upstream_of_a_refused_sink_is_not_refused(
        self, new_store: Callable[[], Store]
    ):
        """Only the side-effect node fails; old function and operator tables are skipped."""
        store = v02_store(new_store, with_sink=True)

        with pytest.raises(SideEffectLogDataContextError):
            _ = build_job(store, with_sink=True).run()

        assert delivered == []
        assert rows_of(build_job(store).nodes["dbl"].as_table()) == [2, 4]

    def test_completions_logged_by_this_version_are_not_delivered_again(
        self, store: Store, orchestrator: Orchestrator
    ):
        """A second sink run on the same store delivers nothing."""
        _ = _sink_job(store).run(orchestrator=orchestrator)
        delivered.clear()
        _ = _sink_job(store).run(orchestrator=orchestrator)
        assert delivered == []


# ---------------------------------------------------------------------------
# Function pods that take the invocation context deliver side effects too
# ---------------------------------------------------------------------------


@function_pod(output_keys="sent", ctx_arg="ctx")
def notify(double: int, ctx: Any) -> int:
    """A function pod whose call is an external action (it takes the invocation context)."""
    delivered.append(double)
    return double


def _notify_job(store: Store) -> PipelineJob:
    """A job whose only stored node is a function pod that takes the invocation context."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(
            data=[{"k": "a", "double": 2}, {"k": "b", "double": 4}],
            tag_columns=["k"],
            source_id="src",
        )
        notify.pod(source, label="notify")
    return job


def _notify_store(
    new_store: Callable[[], Store], copy: Callable[[Store, Store], None]
) -> Store:
    """A store holding the completions of ``_notify_job``, rewritten by ``copy``."""
    written = new_store()
    _ = _notify_job(written).run()
    assert sorted(delivered) == [2, 4]
    delivered.clear()
    target = new_store()
    copy(written, target)
    return target


class TestSideEffectFunctionNodes:
    """Their completions are in function tables; an older store must not deliver again."""

    def test_old_layout_tables_raise_and_nothing_is_delivered(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """Old pipeline and result tables raise an error that names both of them."""
        store = _notify_store(new_store, copy_as_v02)
        before = snapshot(store)

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _notify_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        error = info.value
        assert error.found_context == PRE_V1
        assert error.current_context == CURRENT
        assert error.node_label == "notify"
        assert sorted(path[-1] for path in error.record_paths) == ["pdb_v1", "rdb_v1"]
        for path in error.record_paths:
            assert "/".join(path) in str(error)

    def test_rows_of_another_data_context_raise(
        self, new_store: Callable[[], Store], orchestrator: Orchestrator
    ):
        """Current-layout tables holding rows of ``OTHER`` raise and nothing is
        delivered.
        """
        store = _notify_store(new_store, copy_as_other_context)
        before = snapshot(store)

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _notify_job(store).run(orchestrator=orchestrator)

        assert delivered == []
        assert snapshot(store) == before
        assert info.value.found_context == OTHER
        assert sorted(path[-1] for path in info.value.record_paths) == [
            "pdb_v2",
            "rdb_v2",
        ]

    def test_only_an_old_result_table_raises(self, new_store: Callable[[], Store]):
        """The result table is shared by every node of the function: it alone is enough."""
        store = _notify_store(new_store, copy_as_v02)
        only_results = new_store()
        source = reopened(store)
        for path in source.list_sources():
            if path[-1] == "rdb_v1":
                rows = source.get_all_records(path, record_id_column=RID)
                assert rows is not None
                only_results.add_records(path, rows, record_id_column=RID)
        only_results.flush()

        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = _notify_job(only_results).run()

        assert delivered == []
        assert [path[-1] for path in info.value.record_paths] == ["rdb_v1"]

    def test_store_of_this_version_is_reused_without_delivering(
        self, store: Store, orchestrator: Orchestrator
    ):
        """A second run of the ``notify`` job on the same store delivers nothing."""
        _ = _notify_job(store).run(orchestrator=orchestrator)
        delivered.clear()
        _ = _notify_job(store).run(orchestrator=orchestrator)
        assert delivered == []

    def test_a_newly_attached_store_is_checked_again(
        self, new_store: Callable[[], Store]
    ):
        """Attaching another store resets what the node knows about stale tables."""
        # Only the old pipeline table: the result cache, which is rebuilt on attach,
        # has nothing to refuse, so the node's own check must run again.
        written = reopened(_notify_store(new_store, copy_as_v02))
        old = new_store()
        for path in written.list_sources():
            if path[-1] == "pdb_v1":
                rows = written.get_all_records(path, record_id_column=RID)
                assert rows is not None
                old.add_records(path, rows, record_id_column=RID)
        old.flush()
        fresh = new_store()
        source = DictSource(
            data=[{"k": "a", "double": 2}, {"k": "b", "double": 4}],
            tag_columns=["k"],
            source_id="src",
        )
        node = FunctionJobNode(
            function_pod=notify.pod,  # pyright: ignore[reportArgumentType]  # the decorator types .pod as the pod base class
            input_stream=source,
            pipeline_database=fresh.at(JOB_NAME),
            result_database=fresh.at(JOB_NAME, "_result"),
        )
        assert node.get_all_records() is None  # the fresh store has been checked

        node.attach_databases(
            pipeline_database=old.at(JOB_NAME),
            result_database=old.at(JOB_NAME, "_result"),
        )
        with pytest.raises(SideEffectLogDataContextError):
            node.run()

        assert delivered == []

    @pytest.mark.parametrize("read_first", [False, True], ids=["unread", "read_first"])
    def test_a_late_attached_ephemeral_store_is_checked_again(
        self, new_store: Callable[[], Store], read_first: bool
    ):
        """An ephemeral store attached after the node checked its stores is checked too
        (ITL-975): completions of ``OTHER`` in it raise and nothing is delivered.
        """
        relabelled = reopened(_notify_store(new_store, copy_as_other_context))
        ephemeral = new_store()
        for path in relabelled.list_sources():
            if path[-1] == "rdb_v2":
                rows = relabelled.get_all_records(path, record_id_column=RID)
                assert rows is not None
                # An ephemeral store holds the result table at the path relative to
                # the job's result database.
                assert path[:2] == (JOB_NAME, "_result")
                ephemeral.add_records(path[2:], rows, record_id_column=RID)
        ephemeral.flush()
        assert [path[-1] for path in reopened(ephemeral).list_sources()] == ["rdb_v2"]
        job = _notify_job(new_store())
        if read_first:
            # The node checks its (clean) stores when it first reads them.
            assert job.nodes["notify"].get_all_records() is None

        job.set_ephemeral_store(ephemeral)
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = job.run()

        assert delivered == []
        assert info.value.found_context == OTHER
        assert [path[-1] for path in info.value.record_paths] == ["rdb_v2"]

    def test_plain_function_node_in_the_same_store_is_only_reported(
        self, new_store: Callable[[], Store]
    ):
        """Count and skip stays the rule for nodes whose call is not an external action."""
        store = v02_store(new_store)
        _ = build_job(store).run()
        assert sorted(calls) == [1, 2]


# ---------------------------------------------------------------------------
# An input of another data context
# ---------------------------------------------------------------------------


@pytest.fixture
def other_context(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> contexts.DataContext:
    """Register a second data context, ``OTHER``, with the hashers of the default one.

    A row can only name a data context the registry knows, so the registry is replaced
    for the test by one that holds the shipped manifest and a copy of it under ``OTHER``.
    """
    shipped = pathlib.Path(str(resources.files("orcapod.contexts"))) / "data"
    manifests = tmp_path / "contexts"
    shutil.copytree(shipped, manifests)
    manifest = json.loads((manifests / "v1.json").read_text())
    manifest["context_key"] = OTHER
    manifest["version"] = "v0.9"
    (manifests / "v0.9.json").write_text(json.dumps(manifest))
    registry = contexts.create_registry(str(manifests))
    # Replacing the module's registry is the only way to register a second context.
    monkeypatch.setattr(contexts, "_registry", registry)
    return registry.get_context(OTHER)


def _other_context_stream(column: str = "x") -> ArrowTableStream:
    """A stream of the data context ``OTHER``: its rows name it, and so does its table."""
    schema = pa.schema(
        [
            pa.field("k", pa.large_string(), nullable=False),
            pa.field(column, pa.int64(), nullable=False),
            pa.field("_context_key", pa.large_string(), nullable=False),
        ]
    )
    table = pa.table(
        {"k": ["a", "b"], column: [1, 2], "_context_key": [OTHER, OTHER]},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["k"], data_context=OTHER)


def _assert_names_both_contexts(error: InputValidationError) -> None:
    """Assert the error names the input's data context and the node's."""
    assert repr(OTHER) in str(error)
    assert repr(CURRENT) in str(error)


@pytest.mark.usefixtures("other_context")
class TestInputOfAnotherDataContext:
    """A node does not write rows for an input hashed under another data context.

    These tests construct the nodes and caches directly. They do not cover a
    ``PipelineJob``: a job wraps every input stream in a source node of the default
    data context, so an input of another data context is not seen as such there (a
    known gap, ``design_issues/ITL-944-DC1.md``, ITL-951).
    """

    def test_function_node_raises_and_writes_nothing(self, store: Store):
        """Running a function node over a stream of ``OTHER`` raises before any call."""
        node = FunctionJobNode(
            function_pod=double.pod,  # pyright: ignore[reportArgumentType]  # the decorator types .pod as the pod base class
            input_stream=_other_context_stream(),
            pipeline_database=store,
            result_database=store,
        )

        with pytest.raises(InputValidationError) as info:
            node.run()

        _assert_names_both_contexts(info.value)
        assert calls == []
        assert reopened(store).list_sources() == []

    def test_add_pipeline_record_refuses_an_input_of_another_data_context(
        self, store: Store
    ):
        """The public record writer does not store a row under a mixed key."""
        import uuid

        node = FunctionJobNode(
            function_pod=double.pod,  # pyright: ignore[reportArgumentType]  # the decorator types .pod as the pod base class
            input_stream=DictSource(data=ROWS, tag_columns=["k"], source_id="src"),
            pipeline_database=store,
            result_database=store,
        )
        tag, data = next(iter(_other_context_stream().iter_data()))

        with pytest.raises(InputValidationError) as info:
            node.add_pipeline_record(tag, data, uuid.uuid4(), computed=True)

        _assert_names_both_contexts(info.value)
        assert reopened(store).list_sources() == []

    def test_cached_function_raises_before_calling_it(
        self, store: Store, other_context: contexts.DataContext
    ):
        """A cached function given a ``Data`` of ``OTHER`` raises and stores nothing."""
        cached = CachedDataFunction(double.pod.data_function, result_database=store)

        with pytest.raises(InputValidationError) as info:
            _ = cached.call(Data({"x": 1}, data_context=other_context))

        _assert_names_both_contexts(info.value)
        assert calls == []
        assert reopened(store).list_sources() == []

    def test_stored_operator_node_raises_and_writes_nothing(self, store: Store):
        """A logging operator node over a stream of ``OTHER`` raises and stores nothing."""
        node = OperatorJobNode(
            operator=SelectDataColumns(["x"]),
            input_streams=[_other_context_stream()],
            pipeline_database=store,
            cache_mode=CacheMode.LOG,
        )

        with pytest.raises(InputValidationError) as info:
            node.run()

        _assert_names_both_contexts(info.value)
        assert reopened(store).list_sources() == []

    def test_stored_operator_node_raises_before_forwarding_rows_async(self):
        """On the async path the refusal comes before any row goes downstream."""
        import asyncio

        from orcapod.channels import Channel

        async def run() -> tuple[InputValidationError | None, list[Any], list[Any]]:
            """Run the node asynchronously; return the error, the rows sent on, the tables."""
            store = InMemoryArrowDatabase()
            stream = _other_context_stream()
            node = OperatorJobNode(
                operator=SelectDataColumns(["x"]),
                input_streams=[stream],
                pipeline_database=store,
                cache_mode=CacheMode.LOG,
            )
            inputs, outputs = Channel(buffer_size=16), Channel(buffer_size=16)
            for item in stream.iter_data():
                await inputs.writer.send(item)
            await inputs.writer.close()
            error: InputValidationError | None = None
            try:
                await node.async_execute([inputs.reader], outputs.writer)
            except InputValidationError as exc:
                error = exc
            return error, await outputs.reader.collect(), store.list_sources()

        error, forwarded, tables = asyncio.run(run())
        assert error is not None
        _assert_names_both_contexts(error)
        assert forwarded == []
        assert tables == []

    def test_side_effect_pod_raises_before_delivering(self):
        """A sink pod over a stream of ``OTHER`` raises and delivers nothing."""
        sink = sink_pod(_deliver)
        assert isinstance(sink, SideEffectPod)

        with pytest.raises(InputValidationError) as info:
            _ = list(sink.process(_other_context_stream("double")).iter_data())

        _assert_names_both_contexts(info.value)
        assert delivered == []

    def test_cached_function_refuses_an_output_of_another_data_context(
        self, store: Store, other_context: contexts.DataContext
    ):
        """A function whose own data context differs produces outputs the cache refuses."""
        function = PythonDataFunction(
            _triple, output_keys="tripled", data_context=other_context
        )
        cached = CachedDataFunction(
            function,  # pyright: ignore[reportArgumentType]  # PythonDataFunction does not match DataFunctionProtocol for the checker
            result_database=store,
            data_context=CURRENT,
        )

        with pytest.raises(InputValidationError) as info:
            _ = cached.call(Data({"x": 1}))

        _assert_names_both_contexts(info.value)
        assert reopened(store).list_sources() == []

    @pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
    @pytest.mark.parametrize(
        ("skip_lookup", "skip_insert"),
        [(False, False), (True, False), (True, True)],
        ids=["default", "no_lookup", "no_lookup_no_insert"],
    )
    def test_cached_function_refuses_an_input_of_another_data_context(
        self,
        store: Store,
        other_context: contexts.DataContext,
        skip_lookup: bool,
        skip_insert: bool,
        asynchronous: bool,
    ):
        """The input's data context is checked before the function is called (ITL-975).

        Also when the cache lookup, which checks it too, is skipped.
        """
        import asyncio

        cached = CachedDataFunction(double.pod.data_function, result_database=store)
        data = Data({"x": 1}, data_context=other_context)

        with pytest.raises(InputValidationError) as info:
            if asynchronous:
                _ = asyncio.run(
                    cached.async_call(
                        data,
                        skip_cache_lookup=skip_lookup,
                        skip_cache_insert=skip_insert,
                    )
                )
            else:
                _ = cached.call(
                    data, skip_cache_lookup=skip_lookup, skip_cache_insert=skip_insert
                )

        _assert_names_both_contexts(info.value)
        assert calls == []
        assert reopened(store).list_sources() == []

    def test_cached_source_keeps_its_data_context_on_cached_rows(self, store: Store):
        """Rows read back from the cache carry the source's data context, not the default."""
        table = pa.table({"k": ["a", "b"], "x": [1, 2]})
        source = CachedSource(
            ArrowTableSource(
                table, tag_columns=["k"], source_id="fixed", data_context=OTHER
            ),
            cache_database=store,
        )
        assert [data.data_context_key for _, data in source.iter_data()] == [
            OTHER,
            OTHER,
        ]
        assert stored_data_contexts(reopened(store), table_path(store, "sdb_v1")) == {
            OTHER: 2
        }

    def test_cached_source_raises_and_writes_nothing(self, store: Store):
        """A cached source of ``CURRENT`` over a source of ``OTHER`` raises on read."""
        table = pa.table({"k": ["a", "b"], "x": [1, 2]})
        source = CachedSource(
            ArrowTableSource(
                table, tag_columns=["k"], source_id="fixed", data_context=OTHER
            ),
            cache_database=store,
            data_context=CURRENT,
        )

        with pytest.raises(InputValidationError) as info:
            _ = source.as_table()

        _assert_names_both_contexts(info.value)
        assert reopened(store).list_sources() == []


# ---------------------------------------------------------------------------
# Listing the data contexts a table holds
# ---------------------------------------------------------------------------


class TestStoredDataContexts:
    """``stored_data_contexts`` counts the rows of a table per data context."""

    @pytest.mark.parametrize("layout", LAYOUTS)
    def test_table_of_this_version(self, store: Store, layout: str):
        """Each table kind written by this version holds two rows of ``CURRENT``."""
        run_everything(store)
        path = table_path(store, layout)
        assert stored_data_contexts(reopened(store), path) == {CURRENT: 2}

    @pytest.mark.parametrize("layout", ["pdb_v2", "rdb_v2", "odb_v1", "sdb_v1"])
    def test_table_holding_two_data_contexts(
        self, new_store: Callable[[], Store], layout: str
    ):
        """A table with rows of both contexts reports two rows for each."""
        store = both_contexts_store(new_store)
        path = table_path(store, layout)
        assert stored_data_contexts(reopened(store), path) == {CURRENT: 2, OTHER: 2}

    def test_old_layout_table_is_the_pre_v1_data_context(
        self, new_store: Callable[[], Store]
    ):
        """Every table at a v0.2 layout counts its rows under ``PRE_V1``."""
        store = v02_store(new_store, with_sink=True)
        paths = reopened(store).list_sources()
        assert len(paths) == 5
        for path in paths:
            assert stored_data_contexts(reopened(store), path) == {PRE_V1: 2}

    def test_missing_table_holds_nothing(self, store: Store):
        """A path with no table gives an empty mapping."""
        assert stored_data_contexts(store, ("no", "such", "table")) == {}
