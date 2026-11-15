"""Tables written before v0.3 are found under a node's name, whatever their hashes (ITL-954).

The path of a stored table holds hashes (of the function's output schema, of the
pipeline), and Orcapod v0.3 computes other hashes than v0.2 did. A table v0.2 wrote is
therefore at a path v0.3 never computes. What does not change is the beginning of the
path: the job, and the name of the function or pod. Orcapod lists the tables under
that beginning (``list_table_paths``) and tells old from current by the last path
component, the layout: ``pdb_v2``, ``rdb_v2``, ``tdb_v2``, ``odb_v1`` and ``sdb_v1``
are current; any other table was written before v0.3.

- A node that performs side effects (a ``SideEffectPod``, or a function pod that takes
  the invocation context) raises ``SideEffectLogDataContextError`` before it does
  anything when such a table exists under its name.
- For other function nodes the tables are reported, and the job reports one total of
  the remaining ones (operator caches, cached sources, nodes no longer in the job).

The store used here was written by Orcapod v0.2.0 itself
(``tests/test_arrays/data/v02_full_store.tar.gz``). It is used twice: as written, and
with every hash in its paths replaced by another one, which is what a v0.2 store looks
like to a version whose hashes all moved.
"""

from __future__ import annotations

import hashlib
import logging
import re
import sys
import tarfile
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.core.operators import SelectDataColumns
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import SideEffectLogDataContextError, TableListingNotSupportedError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from tests.test_arrays import v02_full_store_pods as pods

FIXTURE = (
    Path(__file__).parent.parent / "test_arrays" / "data" / "v02_full_store.tar.gz"
)
JOB = pods.JOB_NAME
CURRENT_LAYOUTS = {"pdb_v2", "rdb_v2", "tdb_v2", "odb_v1", "sdb_v1"}
HASH = re.compile(r"[0-9a-f]{64}")

Orchestrator = SyncPipelineOrchestrator | AsyncPipelineOrchestrator


def _local_name(name: str) -> str:
    """The name a database path component has on disk on this platform."""
    # Windows forbids ':' in file names; DeltaTableDatabase stores it as '!' there.
    return name.replace(":", "!") if sys.platform == "win32" else name


def _extract(target: Path) -> None:
    """Unpack the database Orcapod v0.2.0 wrote into ``target``."""
    with tarfile.open(FIXTURE) as tar:
        for member in tar.getmembers():
            member.name = _local_name(member.name)
            path = Path(member.name)
            if (
                path.is_absolute()
                or ".." in path.parts
                or not (member.isfile() or member.isdir())
            ):
                raise ValueError(f"unsafe member in {FIXTURE}: {member.name!r}")
        tar.extractall(target)  # every member was checked above


def _move_hashes(root: Path) -> None:
    """Rename every directory under ``root`` so that each hash in its name is another one."""
    directories = sorted(
        (p for p in root.rglob("*") if p.is_dir() and HASH.search(p.name)),
        key=lambda p: len(p.parts),
        reverse=True,
    )
    for directory in directories:
        moved = HASH.sub(
            lambda found: hashlib.sha256(found.group().encode()).hexdigest(),
            directory.name,
        )
        directory.rename(directory.with_name(moved))


def _tables(root: Path) -> set[tuple[str, ...]]:
    """The path of every Delta table under ``root``."""
    return {p.parent.relative_to(root).parts for p in root.rglob("_delta_log")}


def _files(root: Path) -> dict[str, bytes]:
    """Every file under ``root`` with its bytes."""
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


def _table_under(root: Path, *prefix: str) -> list[tuple[str, ...]]:
    """The tables of the database whose path starts with ``prefix``, sorted."""
    return sorted(t for t in _tables(root) if t[: len(prefix)] == prefix)


def _warnings(caplog: pytest.LogCaptureFixture) -> str:
    """Every warning logged so far, one per line."""
    return "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING
    )


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Forget what the fixture's pods did in earlier tests."""
    pods.reset()


@pytest.fixture(params=["as_written", "hashes_moved"])
def root(request: pytest.FixtureRequest, tmp_path: Path) -> Path:
    """The v0.2.0 store: as written, or as a version whose hashes all moved sees it."""
    target = tmp_path / "store"
    _extract(target)
    if request.param == "hashes_moved":
        _move_hashes(target)
    return target


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Orchestrator:
    """The synchronous or the asynchronous orchestrator."""
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


class TestTheFixture:
    """The archived v0.2.0 store and the hash-moving helper are what the tests assume."""

    def test_it_holds_every_kind_of_table_v02_stored(self, tmp_path: Path):
        """The archive holds nine non-empty tables, none with a ``__data_context``
        column.
        """
        _extract(tmp_path)
        tables = _tables(tmp_path)
        assert len(tables) == 9
        assert sorted(t[-1] for t in tables if t[-1].endswith("_v1")) == [
            "pdb_v1",
            "pdb_v1",
            "pdb_v1",
            "rdb_v1",
            "rdb_v1",
            "rdb_v1",
            "tdb_v1",
        ]
        # The operator cache and the cached source had no layout component.
        assert len(_table_under(tmp_path, JOB, "SelectDataColumns")) == 1
        assert len(_table_under(tmp_path, JOB, "source")) == 1
        store = DeltaTableDatabase(tmp_path)
        for table in tables:
            records = store.get_all_records(table)
            assert records is not None and records.num_rows > 0
            assert "__data_context" not in records.column_names

    def test_moving_the_hashes_leaves_no_path_this_version_computes(
        self, tmp_path: Path
    ):
        """``_move_hashes`` changes every table path and keeps job, names and layouts."""
        _extract(tmp_path / "written")
        _extract(tmp_path / "moved")
        _move_hashes(tmp_path / "moved")
        written, moved = _tables(tmp_path / "written"), _tables(tmp_path / "moved")
        assert len(moved) == 9
        assert written.isdisjoint(moved)
        # Same job, same names, same layouts: only the hashes differ.
        assert {(t[:2], t[-1]) for t in written if t[-1].endswith("_v1")} == {
            (t[:2], t[-1]) for t in moved if t[-1].endswith("_v1")
        }


class TestSideEffectPod:
    """A sink refuses to run over the delivery log Orcapod v0.2 wrote."""

    def test_it_raises_before_delivering_and_names_the_log(
        self, root: Path, orchestrator: Orchestrator
    ):
        """The run raises naming the ``tdb_v1`` log and the sink, and delivers nothing."""
        job = pods.build_job(DeltaTableDatabase(root), with_notify=False)
        with pytest.raises(SideEffectLogDataContextError) as raised:
            job.run(orchestrator)
        (log,) = _table_under(root, JOB, "side_effects", "deliver")
        assert log[-1] == "tdb_v1"
        assert log in raised.value.record_paths
        assert "/".join(log) in str(raised.value)
        assert raised.value.node_label == "sink"
        assert raised.value.found_context == "std:v0.1:default"
        assert pods.DELIVERED == []

    def test_what_v02_wrote_stays_byte_identical(
        self, root: Path, orchestrator: Orchestrator
    ):
        """The refused sink run leaves every file of the v0.2 store unchanged."""
        before = _files(root)
        job = pods.build_job(DeltaTableDatabase(root), with_notify=False)
        with pytest.raises(SideEffectLogDataContextError):
            job.run(orchestrator)
        after = _files(root)
        assert {name: after[name] for name in before} == before


class TestFunctionPodThatTakesTheInvocationContext:
    """A function pod with a context argument refuses to run over its v0.2 tables."""

    def test_it_raises_before_running_and_names_its_tables(
        self, root: Path, orchestrator: Orchestrator
    ):
        """The run raises naming the pipeline and result tables, and calls nothing."""
        job = pods.build_job(DeltaTableDatabase(root), with_sink=False)
        with pytest.raises(SideEffectLogDataContextError) as raised:
            job.run(orchestrator)
        (pipeline,) = _table_under(root, JOB, "side_effect_function", "notify")
        (result,) = _table_under(root, JOB, "_result", "side_effect_function", "notify")
        assert (pipeline[-1], result[-1]) == ("pdb_v1", "rdb_v1")
        assert {pipeline, result} <= set(raised.value.record_paths)
        assert raised.value.node_label == "notify"
        assert pods.NOTIFIED == []

    def test_what_v02_wrote_stays_byte_identical(
        self, root: Path, orchestrator: Orchestrator
    ):
        """The refused function pod run leaves every file of the v0.2 store unchanged."""
        before = _files(root)
        job = pods.build_job(DeltaTableDatabase(root), with_sink=False)
        with pytest.raises(SideEffectLogDataContextError):
            job.run(orchestrator)
        after = _files(root)
        assert {name: after[name] for name in before} == before


class TestOrdinaryTables:
    """Tables of nodes without side effects are reported, skipped and left alone."""

    def test_function_tables_are_reported_and_inputs_computed_again(
        self, root: Path, orchestrator: Orchestrator, caplog: pytest.LogCaptureFixture
    ):
        """Old function tables are named in warnings and every input is computed anew."""
        job = pods.build_job(
            DeltaTableDatabase(root), with_notify=False, with_sink=False
        )
        with caplog.at_level(logging.WARNING):
            job.run(orchestrator)
        assert sorted(pods.COMPUTED) == [1, 2, 3]
        reported = _warnings(caplog)
        for name in ("plus_one", "plus_two"):
            for table in _table_under(root, JOB, name) + _table_under(
                root, JOB, "_result", name
            ):
                if table[-1] not in CURRENT_LAYOUTS:
                    assert "/".join(table) in reported, table
        # A new database object: one that read a table before the run does not see
        # the rows the run added.
        reread = pods.build_job(
            DeltaTableDatabase(root), with_notify=False, with_sink=False
        )
        records = reread.nodes["plus_two"].get_all_records()
        assert sorted(records["p"].to_pylist()) == [4, 5, 6]

    def test_the_job_reports_one_total_of_the_tables_no_node_claims(
        self, root: Path, orchestrator: Orchestrator, caplog: pytest.LogCaptureFixture
    ):
        """The job logs a single total of the five old tables none of its nodes own."""
        # The operator cache, the cached source, and the three tables of the two
        # side-effecting nodes, which this job does not have.
        job = pods.build_job(
            DeltaTableDatabase(root), with_notify=False, with_sink=False
        )
        with caplog.at_level(logging.WARNING):
            job.run(orchestrator)
        totals = [
            record.getMessage()
            for record in caplog.records
            if "more table" in record.getMessage()
        ]
        assert len(totals) == 1
        assert f"'{JOB}' holds 5 more tables" in totals[0]

    def test_the_total_leaves_out_what_the_nodes_of_the_job_claim(
        self, root: Path, caplog: pytest.LogCaptureFixture
    ):
        """With the side-effecting nodes in the job, the total drops to two tables."""
        job = pods.build_job(DeltaTableDatabase(root))
        with caplog.at_level(logging.WARNING):
            with pytest.raises(SideEffectLogDataContextError):
                job.run()
        # Only the operator cache and the cached source are left.
        assert f"'{JOB}' holds 2 more tables" in _warnings(caplog)

    def test_a_second_run_reuses_what_this_version_stored(
        self, root: Path, orchestrator: Orchestrator
    ):
        """A second run over the same database computes nothing."""
        pods.build_job(
            DeltaTableDatabase(root), with_notify=False, with_sink=False
        ).run(orchestrator)
        pods.reset()
        job = pods.build_job(
            DeltaTableDatabase(root), with_notify=False, with_sink=False
        )
        job.run(orchestrator)
        assert pods.COMPUTED == []

    def test_cached_source_reports_the_old_cache_and_fills_its_own(
        self, root: Path, caplog: pytest.LogCaptureFixture
    ):
        """A cached source warns about the old cache, keeps it and writes an ``sdb_v1``
        one.
        """
        (old,) = _table_under(root, JOB, "source")
        before = _files(root)
        with caplog.at_level(logging.WARNING):
            table = pods.build_cached_source(DeltaTableDatabase(root)).as_table()
        assert table.num_rows == 2
        assert "/".join(old) in _warnings(caplog)
        after = _files(root)
        assert {name: after[name] for name in before} == before
        assert [t[-1] for t in _table_under(root, JOB, "source") if t != old] == [
            "sdb_v1"
        ]


class TestStoreOfThisVersion:
    """A store written by this version is reused silently."""

    def test_nothing_is_raised_or_reported(
        self,
        tmp_path: Path,
        orchestrator: Orchestrator,
        caplog: pytest.LogCaptureFixture,
    ):
        """A second run on a database of this version logs no warning and repeats no work."""
        pods.build_job(DeltaTableDatabase(tmp_path)).run(orchestrator)
        _ = pods.build_cached_source(DeltaTableDatabase(tmp_path)).as_table()
        assert sorted(pods.DELIVERED) == [2, 3, 4]
        pods.reset()
        with caplog.at_level(logging.WARNING):
            pods.build_job(DeltaTableDatabase(tmp_path)).run(orchestrator)
            _ = pods.build_cached_source(DeltaTableDatabase(tmp_path)).as_table()
        assert _warnings(caplog) == ""
        assert (pods.DELIVERED, pods.NOTIFIED, pods.COMPUTED) == ([], [], [])


class TestTablesCountedByNameAlone:
    """Accepted: every table under the node's name counts, whoever wrote it."""

    def test_an_old_table_of_another_pod_with_the_same_name_raises(self):
        """An old log under the sink's name with another schema hash stops the sink."""
        store = InMemoryArrowDatabase()
        other = (JOB, "side_effects", "deliver", "schema:of-another-pod", "tdb_v1")
        store.add_record(other, b"r", pa.table({"x": [1]}), flush=True)
        job = pods.build_job(store, with_notify=False)
        with pytest.raises(SideEffectLogDataContextError) as raised:
            job.run()
        assert raised.value.record_paths == (other,)
        assert pods.DELIVERED == []

    def test_an_old_table_of_another_node_of_the_same_function_raises(self):
        """An old pipeline table of another node of ``notify`` stops this node."""
        store = InMemoryArrowDatabase()
        other = (
            JOB,
            "side_effect_function",
            "notify",
            "an-output-schema",
            "v0",
            "python.function.v0",
            "schema:of-another-node",
            "pdb_v1",
        )
        store.add_record(other, b"r", pa.table({"x": [1]}), flush=True)
        job = pods.build_job(store, with_sink=False)
        with pytest.raises(SideEffectLogDataContextError) as raised:
            job.run()
        assert raised.value.record_paths == (other,)
        assert pods.NOTIFIED == []

    def test_a_table_without_a_layout_component_counts(self):
        """A table under the sink's name with no layout component stops the sink."""
        store = InMemoryArrowDatabase()
        other = (JOB, "side_effects", "deliver", "anything")
        store.add_record(other, b"r", pa.table({"x": [1]}), flush=True)
        with pytest.raises(SideEffectLogDataContextError) as raised:
            pods.build_job(store, with_notify=False).run()
        assert raised.value.record_paths == (other,)

    def test_a_current_table_of_another_node_does_not_raise(self):
        """A ``tdb_v2`` log of another pod with the same name lets the sink deliver."""
        store = InMemoryArrowDatabase()
        other = (JOB, "side_effects", "deliver", "schema:of-another-pod", "tdb_v2")
        store.add_record(other, b"r", pa.table({"x": [1]}), flush=True)
        pods.build_job(store, with_notify=False).run()
        assert sorted(pods.DELIVERED) == [2, 3, 4]

    def test_a_table_under_a_longer_name_does_not_count(self):
        """An old log under ``deliver_more`` is not taken for one of ``deliver``."""
        store = InMemoryArrowDatabase()
        other = (JOB, "side_effects", "deliver_more", "schema:x", "tdb_v1")
        store.add_record(other, b"r", pa.table({"x": [1]}), flush=True)
        pods.build_job(store, with_notify=False).run()
        assert sorted(pods.DELIVERED) == [2, 3, 4]

    def test_the_job_total_counts_marked_tables_only(
        self, caplog: pytest.LogCaptureFixture
    ):
        """The job total counts old tables and skips observer tables and current ones."""
        store = InMemoryArrowDatabase()
        row = pa.table({"x": [1]})
        # Counted: a v0.2 operator cache, a v0.2 cached source, an old function table.
        store.add_record((JOB, "Join", "abc", "schema:h"), b"r", row)
        store.add_record((JOB, "source", "node:h"), b"r", row)
        store.add_record((JOB, "gone", "h", "v0", "t", "schema:h", "pdb_v1"), b"r", row)
        # Not counted: an observer's table and a current table.
        store.add_record((JOB, "_log", "plus_one", "run"), b"r", row)
        store.add_record((JOB, "Join", "abc", "schema:h", "odb_v1"), b"r", row)
        store.flush()
        job = pods.build_job(store, with_notify=False, with_sink=False)
        with caplog.at_level(logging.WARNING):
            job.run()
        assert f"'{JOB}' holds 3 more tables" in _warnings(caplog)


class TestStoreThatCannotListItsTables:
    """Runs against a database that does not support listing its tables."""

    @pytest.fixture
    def unlistable(self, monkeypatch: pytest.MonkeyPatch) -> InMemoryArrowDatabase:
        """An in-memory store whose tables cannot be listed."""

        def refuse(self: object, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
            """Stand in for ``list_table_paths`` and raise that listing is unsupported."""
            raise TableListingNotSupportedError("this database has no table listing")

        monkeypatch.setattr(InMemoryArrowDatabase, "list_table_paths", refuse)
        return InMemoryArrowDatabase()

    def test_side_effecting_nodes_warn_once_and_proceed(
        self, unlistable: InMemoryArrowDatabase, caplog: pytest.LogCaptureFixture
    ):
        """Each side-effecting node warns once over two runs, and the effects happen."""
        job = pods.build_job(unlistable)
        with caplog.at_level(logging.WARNING):
            job.run()
            job.run()
        assert sorted(pods.DELIVERED) == [2, 3, 4]
        assert sorted(pods.NOTIFIED) == [2, 3, 4]
        warned = [
            record.getMessage()
            for record in caplog.records
            if "cannot list its tables" in record.getMessage()
            and "side effect" in record.getMessage()
        ]
        assert len(warned) == 2  # one per side-effecting node, not one per run
        assert any(f"'{JOB}/side_effects/deliver'" in message for message in warned)
        assert any(
            f"'{JOB}/side_effect_function/notify' and "
            f"'{JOB}/_result/side_effect_function/notify'" in message
            for message in warned
        )
        assert all("this database has no table listing" in m for m in warned)

    def test_the_sibling_path_check_still_raises(
        self, unlistable: InMemoryArrowDatabase
    ):
        """A ``tdb_v1`` log beside the sink's own path is refused without a listing."""
        pods.build_job(unlistable, with_notify=False).run()
        pods.reset()
        (log,) = [p for p in unlistable.list_sources() if p[-1] == "tdb_v2"]
        rows = unlistable.get_all_records(log, record_id_column="__rid")
        assert rows is not None
        old = InMemoryArrowDatabase()
        old.add_records(
            log[:-1] + ("tdb_v1",),
            rows.drop(["__data_context"]),
            record_id_column="__rid",
            flush=True,
        )
        with pytest.raises(SideEffectLogDataContextError):
            pods.build_job(old, with_notify=False).run()
        assert pods.DELIVERED == []

    def test_ordinary_nodes_run_without_a_warning_per_node(
        self, unlistable: InMemoryArrowDatabase, caplog: pytest.LogCaptureFixture
    ):
        """Nodes without side effects compute and log no side-effect warning."""
        job = pods.build_job(unlistable, with_notify=False, with_sink=False)
        with caplog.at_level(logging.WARNING):
            job.run()
        assert sorted(pods.COMPUTED) == [1, 2, 3]
        assert "side effect" not in _warnings(caplog)


def test_operator_cache_of_v02_is_not_replayed(root: Path):
    """The old operator cache is counted by the job; REPLAY finds no current rows."""
    import deltalake

    from orcapod.types import CacheMode

    # The cache v0.2 wrote is there and holds rows, so "not replayed" is not vacuous.
    (old_cache,) = _table_under(root, JOB, "SelectDataColumns")
    old_table = deltalake.DeltaTable(str(root.joinpath(*old_cache)))
    assert old_table.to_pyarrow_dataset(as_large_types=True).count_rows() > 0
    old_files = {
        name: content
        for name, content in _files(root).items()
        if Path(name).parts[: len(old_cache)] == old_cache
    }

    job = pods.build_job(
        DeltaTableDatabase(root),
        cache_mode=CacheMode.REPLAY,
        with_notify=False,
        with_sink=False,
    )
    job.run()
    assert isinstance(job.nodes["select"]._operator, SelectDataColumns)  # noqa: SLF001  # the node under test
    records = job.nodes["select"].get_all_records()
    assert records is None or records.num_rows == 0
    # The old cache is left byte for byte.
    files = _files(root)
    assert all(files[name] == content for name, content in old_files.items())
