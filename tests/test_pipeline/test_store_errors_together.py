"""Store read and write failures in one run (ITL-884 with ITL-878).

``StoreReadError`` and ``StoreWriteError`` share the ``StoreError`` base and one
error path: whichever fails first in node order fails the run, the same on the
sync and the async orchestrator, and the error names its node. A read failure
met while writing is reported as the read failure; any other failed write is a
``StoreWriteError``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest
from deltalake.exceptions import DeltaError

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import (
    InputValidationError,
    StoreError,
    StoreReadError,
    StoreWriteError,
)
from orcapod.pipeline import PipelineJob
from orcapod.pipeline.async_orchestrator import (
    AsyncPipelineOrchestrator,
    _single_store_error,  # pyright: ignore[reportPrivateUsage]
    _start_node,  # pyright: ignore[reportPrivateUsage]
)
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

# Tables the read-failing function damages on its next call, standing in for
# another process breaking a table while the job runs.
_DAMAGE_ON_NEXT_CALL: list[Path] = []


def require_unknown_reader_feature(table: Path) -> None:
    """Commit a log version that requires a reader feature deltalake does not know."""
    log = table / "_delta_log"
    version = max(int(p.stem) for p in log.glob("*.json")) + 1
    protocol = {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["orcapodMadeUpFeature"],
        "writerFeatures": ["orcapodMadeUpFeature"],
    }
    (log / f"{version:020d}.json").write_text(json.dumps({"protocol": protocol}))


@function_pod(output_keys="y")
async def double(x: int) -> int:
    """Damage any queued table, then return twice ``x`` (on the event loop)."""
    while _DAMAGE_ON_NEXT_CALL:
        require_unknown_reader_feature(_DAMAGE_ON_NEXT_CALL.pop())
    return 2 * x


@function_pod(output_keys="z")
async def triple(x: int) -> int:
    """Return three times ``x`` (on the event loop)."""
    return 3 * x


ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": lambda: SyncPipelineOrchestrator(error_policy="continue"),
    "async": lambda: AsyncPipelineOrchestrator(error_policy="continue"),
}


@pytest.fixture(autouse=True)
def _reset_damage() -> Iterator[None]:
    """Start and end every test with no queued damage."""
    _DAMAGE_ON_NEXT_CALL.clear()
    yield
    _DAMAGE_ON_NEXT_CALL.clear()


def build_job(store_path: Path, keys: str, *, reader_first: bool) -> PipelineJob:
    """Two sibling nodes over one source: ``rd`` (doubles) and ``wr`` (triples).

    ``reader_first`` decides which node is added first, and so comes first in
    node order.
    """
    source = ArrowTableSource(
        pa.table({"k": list(keys), "x": list(range(1, len(keys) + 1))}),
        tag_columns=["k"],
        infer_nullable=True,
    )
    job = PipelineJob(
        name="together", database=DeltaTableDatabase(base_path=store_path)
    )
    with job:
        if reader_first:
            _ = double.pod(source, label="rd")
            _ = triple.pod(source, label="wr")
        else:
            _ = triple.pod(source, label="wr")
            _ = double.pod(source, label="rd")
    return job


def result_table(store_path: Path, function_name: str) -> Path:
    """The directory of ``function_name``'s result table."""
    dirs = {
        p.parent
        for p in (store_path / "together" / "_result" / function_name).rglob(
            "*.parquet"
        )
    }
    assert len(dirs) == 1, sorted(dirs)
    return dirs.pop()


def fail_result_writes_of(
    monkeypatch: pytest.MonkeyPatch, function_name: str
) -> list[tuple[str, ...]]:
    """Make every result-table write of ``function_name`` fail with ``OSError``.

    Returns:
        The record paths of the writes that were failed.
    """
    failed: list[tuple[str, ...]] = []
    original = DeltaTableDatabase.add_record

    def add_record(self: DeltaTableDatabase, record_path, *args, **kwargs):
        if "_result" in self.base_path and record_path[0] == function_name:
            failed.append(tuple(record_path))
            raise OSError(f"disk unavailable while writing {'/'.join(record_path)}")
        return original(self, record_path, *args, **kwargs)

    monkeypatch.setattr(DeltaTableDatabase, "add_record", add_record)
    return failed


def run_with_both_failures(
    tmp_path: Path,
    make_orchestrator: Callable[[], Any],
    *,
    reader_first: bool,
) -> StoreError:
    """Run a job in which ``rd``'s read and ``wr``'s write fail on the new row d.

    The first run stores rows a, b and c for both nodes. In the second run row d
    is new: ``rd``'s result table breaks while d is computed, so storing d's
    result fails to read the table, and ``wr``'s write of d's result fails.

    Returns:
        The store error the second run raised.
    """
    store_path = tmp_path / "store"
    build_job(store_path, "abc", reader_first=reader_first).run(
        orchestrator=make_orchestrator()
    )

    _DAMAGE_ON_NEXT_CALL.append(result_table(store_path, "double"))
    with pytest.MonkeyPatch.context() as monkeypatch:
        failed = fail_result_writes_of(monkeypatch, "triple")
        job = build_job(store_path, "abcd", reader_first=reader_first)
        with pytest.raises(StoreError) as info:
            job.run(orchestrator=make_orchestrator())
    if info.value.node_label == "wr":
        assert failed, "wr's error should come from its failed write"
    return info.value


@pytest.mark.parametrize("reader_first", [True, False], ids=["rd-first", "wr-first"])
def test_sync_and_async_raise_the_same_error(
    tmp_path: Path, reader_first: bool
) -> None:
    """Both orchestrators raise the error of the node that comes first."""
    raised = {
        name: run_with_both_failures(tmp_path / name, make, reader_first=reader_first)
        for name, make in ORCHESTRATORS.items()
    }

    expected_type, expected_node = (
        (StoreReadError, "rd") if reader_first else (StoreWriteError, "wr")
    )
    # rd's read failure surfaces when rd commits its batch, which names no row
    # (ITL-919); wr's failed write of row d names that row.
    expected_tag = None if reader_first else {"k": "d"}
    for name, err in raised.items():
        assert type(err) is expected_type, (name, err)
        assert err.node_label == expected_node, (name, err)
        assert err.tag == expected_tag, (name, err)
    sync, async_ = raised["sync"], raised["async"]
    assert str(sync).replace(str(tmp_path / "sync"), "") == str(async_).replace(
        str(tmp_path / "async"), ""
    )


@pytest.mark.parametrize("reader_first", [True, False], ids=["rd-first", "wr-first"])
def test_async_choice_does_not_depend_on_completion_order(
    tmp_path: Path, reader_first: bool
) -> None:
    """Repeated async runs always raise the first node's error, noting the other."""
    for attempt in range(5):
        err = run_with_both_failures(
            tmp_path / str(attempt),
            ORCHESTRATORS["async"],
            reader_first=reader_first,
        )
        assert err.node_label == ("rd" if reader_first else "wr"), attempt
        notes = list(getattr(err, "__notes__", []))
        if reader_first:
            assert any(
                n.startswith("Another store write failed in the same run")
                for n in notes
            ), attempt
            continue
        # rd's read failure surfaces only when its batch is committed (ITL-919). wr's
        # failed write stops the run first, so rd never commits inside the run: the
        # job's flush after the failure meets rd's unreadable table and notes it.
        assert not any("Another store read failed" in n for n in notes), attempt
        flush_notes = [
            n
            for n in notes
            if n.startswith(
                "Flushing pending writes after this failure also failed "
                + "(StoreReadError: Could not read the stored table at "
            )
        ]
        assert flush_notes, (attempt, notes)
        for note in flush_notes:
            assert "/together/_result/double/" in note, note
            assert note.endswith(
                "results that were still pending may not have been stored."
            ), note


def test_selection_is_independent_of_group_order() -> None:
    """``_single_store_error`` ranks by node order, whatever order the group has."""
    # Calls the private helper directly: the test is about its ordering rule,
    # which an end-to-end run cannot force into both group orders.

    class Node:
        def __init__(self, label: str) -> None:
            self.label = label

    topo_order = [Node("first"), Node("second")]
    for build in (lambda r, w: [r, w], lambda r, w: [w, r]):
        read = StoreReadError("table", ("t",))
        read.add_context(node_label="second")
        write = StoreWriteError(("t",), node_label="first")
        chosen = _single_store_error(
            ExceptionGroup("run", build(read, write)), topo_order
        )
        assert chosen is write


def test_read_failure_while_writing_is_a_store_read_error(tmp_path: Path) -> None:
    """A write that cannot read its stored table raises ``StoreReadError``."""
    store_path = tmp_path / "store"
    build_job(store_path, "abc", reader_first=True).run(
        orchestrator=ORCHESTRATORS["sync"]()
    )
    table = result_table(store_path, "double")
    _DAMAGE_ON_NEXT_CALL.append(table)

    with pytest.raises(StoreReadError) as info:
        build_job(store_path, "abcd", reader_first=True).run(
            orchestrator=ORCHESTRATORS["sync"]()
        )

    err = info.value
    assert Path(err.table) == table
    assert err.node_label == "rd"
    assert isinstance(err.__cause__, DeltaError)
    assert "node 'rd'" in str(err)
    # Surfaced when rd commits its batch (ITL-919): a commit names no row.
    assert err.tag is None


def test_refused_append_is_a_store_write_error(tmp_path: Path) -> None:
    """A refused append (a changed column type) fails the run as a ``StoreWriteError``."""
    store_path = tmp_path / "store"
    build_job(store_path, "abc", reader_first=True).run(
        orchestrator=ORCHESTRATORS["sync"]()
    )
    # Store the result column as int32, so the new row's int64 result differs.
    table = result_table(store_path, "double")
    stored = deltalake.DeltaTable(str(table)).to_pyarrow_table()
    index = stored.schema.get_field_index("y")
    narrowed = stored.set_column(index, "y", stored.column("y").cast(pa.int32()))
    deltalake.write_deltalake(
        str(table), narrowed, mode="overwrite", schema_mode="overwrite"
    )

    with pytest.raises(StoreWriteError) as info:
        build_job(store_path, "abcd", reader_first=True).run(
            orchestrator=ORCHESTRATORS["sync"]()
        )

    err = info.value
    assert err.node_label == "rd"
    assert isinstance(err.__cause__, InputValidationError)
    assert "'y'" in str(err.__cause__)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_pipeline_record_read_failure_names_node_and_row(
    tmp_path: Path, orchestrator: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A read failure while recording a row's pipeline entry names the node and row."""
    # The async node records pipeline entries outside the pod's per-row handler,
    # so this checks that the error still gets its node and row.
    store_path = tmp_path / "store"
    build_job(store_path, "abc", reader_first=True).run(
        orchestrator=ORCHESTRATORS[orchestrator]()
    )
    original = DeltaTableDatabase.add_record

    def add_record(self: DeltaTableDatabase, record_path, record_id, record, **kwargs):
        # Stand in for a pipeline table that cannot be read while row d's entry
        # is recorded (the skip_duplicates check reads the stored record ids).
        if "_result" not in self.base_path and record.column("k")[0].as_py() == "d":
            raise StoreReadError("pipeline-table", tuple(record_path)) from OSError(
                "data file missing"
            )
        return original(self, record_path, record_id, record, **kwargs)

    monkeypatch.setattr(DeltaTableDatabase, "add_record", add_record)
    with pytest.raises(StoreReadError) as info:
        build_job(store_path, "abcd", reader_first=True).run(
            orchestrator=ORCHESTRATORS[orchestrator]()
        )

    err = info.value
    assert err.table == "pipeline-table"
    assert err.node_label == "rd"
    assert err.tag == {"k": "d"}


def test_store_error_inside_a_node_group_gets_the_node_label() -> None:
    """A store error a node raises inside its own exception group is labelled."""
    # Calls the private helper directly: no public pipeline raises a store error
    # from a node's inner task group without an annotation on the way.

    class Node:
        label = "inner"

    read = StoreReadError("table", ("t",))

    async def node_work() -> None:
        raise ExceptionGroup("node", [read])

    async def main() -> None:
        async with asyncio.TaskGroup() as tg:
            _start_node(tg, Node(), node_work())

    with pytest.raises(ExceptionGroup):
        asyncio.run(main())
    assert read.node_label == "inner"
