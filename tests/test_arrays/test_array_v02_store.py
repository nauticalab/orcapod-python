"""A v0.2 store holding ``.npy`` arrays under v0.3: recomputed into new tables, never damaged (ITL-888).

The array format changed in v0.3 without a migration. Because the logical type
name moved to ``orcapod.ndarray``, every node with an array column, and every node
downstream of one (pipeline hashes are Merkle-chained), gets a new pipeline hash,
so v0.3 writes new pipeline tables for those nodes and leaves the v0.2 tables as
they were. Detection of old stores and their migration are deferred (ITL-889).
"""

from __future__ import annotations

import sys
import tarfile
from pathlib import Path

import deltalake
import numpy as np
import pytest

from tests.test_arrays.v02_store_pods import ARRAYS, CALLS, build_job

FIXTURE = Path(__file__).parent / "data" / "v02_array_store.tar.gz"


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()
    }


def _tables(root: Path) -> set[str]:
    return {str(p.parent.relative_to(root)) for p in root.rglob("_delta_log")}


def _is_result(table: str) -> bool:
    return Path(table).parts[1] == "_result"


def _node_of(table: str) -> str:
    parts = Path(table).parts
    return parts[2] if _is_result(table) else parts[1]


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


def test_v02_store_is_left_intact_and_array_nodes_get_new_tables(store: Path) -> None:
    """Under v0.3 a v0.2 store is left byte-identical and array nodes write new tables."""
    files_before = _files(store)
    tables_before = _tables(store)

    job = build_job(store)
    job.run()

    # Results are correct and complete under v0.3.
    doubled = {
        t.as_dict()["k"]: d.as_dict()["y"] for t, d in job.nodes["double"].iter_data()
    }
    assert sorted(doubled) == sorted(ARRAYS)
    for k, x in ARRAYS.items():
        assert doubled[k].dtype == x.dtype and np.array_equal(doubled[k], x * 2)
    for label in ("total", "direct_sum", "plus_one"):
        assert sum(1 for _ in job.nodes[label].iter_data()) == len(ARRAYS)

    # Nothing written by v0.2 was changed or removed.
    files_after = _files(store)
    for path, content in files_before.items():
        assert files_after.get(path) == content, path

    # Array nodes, and the array-free plus_two downstream of an array column, moved to
    # new pipeline tables; double's result table moved too (its function identity
    # includes the ndarray output). The array-free plus_one lineage did not move.
    new_tables = _tables(store) - tables_before
    assert {_node_of(t) for t in new_tables if not _is_result(t)} == {
        "double",
        "total",
        "direct_sum",
        "plus_two",
    }
    assert {_node_of(t) for t in new_tables if _is_result(t)} == {"double"}
    plus_one_files = {p for p in files_after if _node_of(p) == "plus_one"}
    assert plus_one_files == {p for p in files_before if _node_of(p) == "plus_one"}

    # Every table, old and new, is still readable, read the way Orcapod's Delta
    # database reads it (large Arrow types).
    for table in _tables(store):
        dt = deltalake.DeltaTable(str(store / table))
        assert dt.to_pyarrow_dataset(as_large_types=True).to_table().num_rows > 0


def test_v02_array_column_is_not_mixed_with_the_new_format(store: Path) -> None:
    """No table holds both the old ``numpy.ndarray`` and the new ``orcapod.ndarray`` format."""
    build_job(store).run()
    for table in _tables(store):
        schema = deltalake.DeltaTable(str(store / table)).schema().to_arrow()
        text = str(schema)
        assert not ("numpy.ndarray" in text and "orcapod.ndarray" in text), table


def _rows(store: Path, table: str) -> list[dict[str, object]]:
    dt = deltalake.DeltaTable(str(store / table))
    return dt.to_pyarrow_dataset(as_large_types=True).to_table().to_pylist()


def _result_table(tables: set[str], node: str) -> str:
    (table,) = (t for t in tables if _is_result(t) and _node_of(t) == node)
    return table


def test_array_free_pod_downstream_of_an_array_column_reuses_its_v02_results(
    store: Path,
) -> None:
    """A pod downstream of an array column, taking none, reuses its v0.2 results."""
    tables_before = _tables(store)
    result_table = _result_table(tables_before, "plus_two")
    result_rows_before = _rows(store, result_table)

    CALLS.clear()
    job = build_job(store)
    job.run()

    # Its function result table is the v0.2 one, and every result is a cache hit.
    assert CALLS == []
    assert _rows(store, result_table) == result_rows_before
    got = {
        t.as_dict()["k"]: d.as_dict()["p"]
        for t, d in job.nodes["after_drop"].iter_data()
    }
    assert got == {k: i + 2 for i, k in enumerate(ARRAYS)}
    # Its pipeline table is new: the upstream pipeline hash includes the array column.
    new_tables = {
        t for t in _tables(store) - tables_before if _node_of(t) == "plus_two"
    }
    assert len(new_tables) == 1
    assert all(not _is_result(t) for t in new_tables)


def test_shared_result_table_keeps_v02_rows_and_adds_new_ones_without_collision(
    store: Path,
) -> None:
    """``total`` keeps its result table: its v0.2 rows stay and v0.3 rows are added beside them."""
    tables_before = _tables(store)
    result_table = _result_table(tables_before, "total")
    old_rows = _rows(store, result_table)
    assert len(old_rows) == len(ARRAYS)

    job = build_job(store)
    job.run()

    rows = _rows(store, result_table)
    for row in old_rows:
        assert row in rows
    record_ids = [row["__record_id"] for row in rows]
    assert len(rows) == 2 * len(ARRAYS)
    assert len(set(record_ids)) == len(record_ids)
    # v0.3 reads back its own rows, one per input, with the right values.
    got = {
        t.as_dict()["k"]: d.as_dict()["total"]
        for t, d in job.nodes["total"].iter_data()
    }
    assert got == {k: float((x * 2).sum()) for k, x in ARRAYS.items()}
