"""A v0.2 store under v0.3: its tables are not read, inputs are recomputed (ITL-888, ITL-944).

The array format changed in v0.3 without a migration (ITL-888), and so did every Arrow-derived
hash (starfix hash format 2, label ``arrow_v1``, ITL-910). v0.3 stores every table at a new
layout (``pdb_v2``, ``rdb_v2``), with the data context of each row in ``__data_context``
(ITL-944). The tables a v0.2 store holds are at the old layout: v0.3 does not refuse the
store and does not read those tables as results. It computes every input again into its own
tables and leaves what v0.2 wrote byte-for-byte in place for a later migration (ITL-913,
ITL-914).
"""

from __future__ import annotations

import sys
import tarfile
from pathlib import Path

from typing import Any

import deltalake
import numpy as np
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.contexts import stored_data_contexts
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from tests.test_arrays.v02_store_pods import ARRAYS, CALLS, build_job

FIXTURE = Path(__file__).parent / "data" / "v02_array_store.tar.gz"


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


def _tables(root: Path) -> set[str]:
    return {str(p.parent.relative_to(root)) for p in root.rglob("_delta_log")}


def _local_name(name: str) -> str:
    # Windows forbids ':' in file names; DeltaTableDatabase stores it as '!' there.
    # Unverified on Windows: CI runs on Linux only.
    return name.replace(":", "!") if sys.platform == "win32" else name


def _local_data_filter(member: tarfile.TarInfo, dest: str) -> tarfile.TarInfo | None:
    return tarfile.data_filter(member.replace(name=_local_name(member.name)), dest)


@pytest.fixture
def store(tmp_path: Path) -> Path:
    target = tmp_path / "store"
    with tarfile.open(FIXTURE) as tar:
        if hasattr(tarfile, "data_filter"):
            tar.extractall(target, filter=_local_data_filter)
        else:  # Python 3.11.0-3.11.3 have no extraction filters
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
    return target


@pytest.fixture(params=["sync", "async"])
def orchestrator(request: pytest.FixtureRequest) -> Any:
    """A pipeline orchestrator, once synchronous and once asynchronous."""
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]()


def _table_path(table: str) -> tuple[str, ...]:
    """Split a slash-separated table path into its tuple of components."""
    return tuple(Path(table).parts)


def test_v02_rows_are_ignored_recomputed_and_kept(
    store: Path, orchestrator: Any
) -> None:
    """A v0.3 run on a v0.2 store computes every input again and changes no v0.2 file.

    No v0.2 result is reused, not even for the array-free ``after_drop`` pod: its v0.2
    result table is at the old layout.
    """
    files_before = _files(store)
    tables_before = _tables(store)
    CALLS.clear()

    job = build_job(store)
    job.run(orchestrator=orchestrator)

    assert CALLS == ["plus_two"] * len(ARRAYS)

    # Results are correct and complete: one row per input, never the v0.2 rows as well.
    # Read through a newly built job: a Delta database object that read a table before
    # the run does not see the rows the run added to it (not specific to hash labels).
    stored = build_job(store)
    # A node shows rows only after a run (ITL-1095): a load-only run reads them from the
    # store and computes nothing.
    stored.run(orchestrator=orchestrator, load_only=True)
    assert CALLS == ["plus_two"] * len(ARRAYS)
    rows = [
        (t.as_dict()["k"], d.as_dict()["y"])
        for t, d in stored.nodes["double"].iter_data()
    ]
    # Checked on the list: a dict would hide a row returned twice.
    assert sorted(k for k, _ in rows) == sorted(ARRAYS)
    doubled = dict(rows)
    for k, x in ARRAYS.items():
        assert doubled[k].dtype == x.dtype and np.array_equal(doubled[k], x * 2)
    for label in ("total", "direct_sum", "plus_one", "after_drop"):
        assert sum(1 for _ in stored.nodes[label].iter_data()) == len(ARRAYS), label
        assert stored.nodes[label].as_table().num_rows == len(ARRAYS), label

    # Nothing written by v0.2 was changed or removed.
    files_after = _files(store)
    for path, content in files_before.items():
        assert files_after.get(path) == content, path

    # Every table, old and new, is still readable, read the way Orcapod's Delta
    # database reads it; every v0.2 table still holds its v0.2 rows only, listed under
    # the data context of everything written before v0.3.
    database = DeltaTableDatabase(base_path=store)
    for table in _tables(store):
        dt = deltalake.DeltaTable(str(store / table))
        assert dt.to_pyarrow_dataset(as_large_types=True).to_table().num_rows > 0
    for table in tables_before:
        found = stored_data_contexts(database, _table_path(table))
        assert found == {"std:v0.1:default": len(ARRAYS)}, table


@pytest.mark.parametrize("label", ["plus_one", "after_drop", "total"])
def test_reading_a_node_with_only_v02_rows_returns_nothing(
    store: Path, label: str
) -> None:
    """Before v0.3 has run, a node whose tables exist only at the v0.2 layout reads as empty."""
    files_before = _files(store)
    node = build_job(store).nodes[label]
    assert node.as_table().num_rows == 0
    assert node.get_all_records() is None
    assert _files(store) == files_before


def test_second_run_reuses_the_rows_of_this_version(store: Path) -> None:
    """After one v0.3 run, a second run computes nothing and still gives one row per input."""
    build_job(store).run()
    CALLS.clear()

    build_job(store).run()

    assert CALLS == []
    stored = build_job(store)
    # A node shows rows only after a run (ITL-1095): a load-only run reads them.
    stored.run(load_only=True)
    assert CALLS == []
    for label in ("double", "total", "direct_sum", "plus_one", "after_drop"):
        assert stored.nodes[label].as_table().num_rows == len(ARRAYS), label
