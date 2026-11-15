"""The in-memory snapshot of a Delta table's committed rows: freshness and failures (ITL-919).

A ``DeltaTableDatabase`` family answers lookups from the rows of the table version it
loaded, read once and extended after its own commits. These tests pin the boundary:

- A cold handle (nothing loaded) on an unreadable table raises ``OrcapodDatabaseReadError`` for
  every read, including lookups of ids or values that are not stored.
- A warm family keeps answering from the version it loaded, as before (it reloads a
  table only at its own commit). At its own commit it notices another writer's commit,
  so the next read re-reads: it raises while storage is down and sees the other
  writer's rows once it is up.
- A snapshot that failed to load, or to extend after a commit, is never half-built: the
  next read loads it again.
- A commit appends when every pending id is new in the table version loaded for that
  commit, and MERGEs otherwise; both store the same rows.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import OrcapodDatabaseReadError

PATH = ("table",)


def _rows(ids: list[str], values: list[int]) -> pa.Table:
    """Build rows keyed by ``__record_id`` with an ``int64`` value ``v`` and group ``g``."""
    return pa.table(
        {
            "__record_id": pa.array([i.encode() for i in ids], type=pa.large_binary()),
            "v": pa.array(values, type=pa.int64()),
            "g": pa.array([b"g"] * len(ids), type=pa.large_binary()),
        }
    )


def _values(table: pa.Table | None) -> list[int] | None:
    """Return the sorted ``v`` values of ``table``, or ``None``."""
    if table is None:
        return None
    return sorted(cast(list[int], table.column("v").to_pylist()))


Read = Callable[[DeltaTableDatabase], "pa.Table | None"]

# Every read method, including lookups of an id or value that is not stored.
READS: dict[str, Read] = {
    "get_all_records": lambda db: db.get_all_records(PATH),
    "get_record_by_id": lambda db: db.get_record_by_id(PATH, b"a"),
    "get_record_by_id/absent": lambda db: db.get_record_by_id(PATH, b"zz"),
    "get_records_by_ids": lambda db: db.get_records_by_ids(PATH, [b"a", b"b"]),
    "get_records_by_ids/absent": lambda db: db.get_records_by_ids(PATH, [b"zz"]),
    "get_records_with_column_value": lambda db: db.get_records_with_column_value(
        PATH, {"g": b"g"}
    ),
    "get_records_with_column_value/absent": lambda db: db.get_records_with_column_value(
        PATH, {"g": b"none"}
    ),
}


def _storage_down(*_args: object, **_kwargs: object) -> Any:
    """Fail as a storage outage would."""
    raise OSError("storage unavailable")


@pytest.mark.parametrize("method", list(READS))
def test_cold_handle_on_an_unreadable_table_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    """With nothing loaded, every read of an unreadable table raises, never "no rows"."""
    DeltaTableDatabase(tmp_path).add_records(
        PATH, _rows(["a", "b"], [1, 2]), flush=True
    )
    cold = DeltaTableDatabase(tmp_path)
    monkeypatch.setattr(deltalake.DeltaTable, "to_pyarrow_dataset", _storage_down)
    with pytest.raises(OrcapodDatabaseReadError) as info:
        _ = READS[method](cold)
    assert isinstance(info.value.__cause__, OSError)


@pytest.mark.parametrize("method", list(READS))
def test_warm_family_reloads_at_its_own_commit_after_another_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str
) -> None:
    """A warm family answers from its loaded version until its own commit reloads it."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["a"], [1]), flush=True)
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1]
    assert db.get_records_by_ids(PATH, [b"a"]) is not None  # loads the snapshot

    DeltaTableDatabase(tmp_path).add_records(PATH, _rows(["b"], [2]), flush=True)
    # Not seen yet: the family reads the version it loaded, as before ITL-919.
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1]

    # Its own commit reloads the table and finds another writer's commit in between.
    db.add_records(PATH, _rows(["c"], [3]), flush=True)
    with monkeypatch.context() as patch:
        patch.setattr(deltalake.DeltaTable, "to_pyarrow_dataset", _storage_down)
        with pytest.raises(OrcapodDatabaseReadError):
            _ = READS[method](db)
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1, 2, 3]
    assert _values(db.get_records_by_ids(PATH, [b"a", b"b", b"c"])) == [1, 2, 3]


def test_a_failed_snapshot_load_is_retried(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A lookup whose snapshot load failed raises; the next lookup loads it fully."""
    DeltaTableDatabase(tmp_path).add_records(
        PATH, _rows(["a", "b"], [1, 2]), flush=True
    )
    db = DeltaTableDatabase(tmp_path)
    original = deltalake.DeltaTable.to_pyarrow_dataset
    calls = {"n": 0}

    def fail_first_scan(self: deltalake.DeltaTable, **kwargs: Any) -> Any:
        """Open the table, failing the first data scan only."""
        dataset = original(self, **kwargs)
        calls["n"] += 1
        if calls["n"] == 1:
            raise OSError("transient storage error")
        return dataset

    monkeypatch.setattr(deltalake.DeltaTable, "to_pyarrow_dataset", fail_first_scan)
    with pytest.raises(OrcapodDatabaseReadError):
        _ = db.get_records_with_column_value(PATH, {"g": b"g"})
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1, 2]
    assert _values(db.get_record_by_id(PATH, b"b")) == [2]


def test_a_failed_snapshot_extension_is_rebuilt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If reading a commit's new rows fails, the commit stands and the next read reloads."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["a"], [1]), flush=True)
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1]

    with monkeypatch.context() as patch:
        # The commit itself reads nothing (the ids are known); only reading the new
        # rows afterwards opens the table, and that fails.
        patch.setattr(deltalake.DeltaTable, "to_pyarrow_dataset", _storage_down)
        db.add_records(PATH, _rows(["b"], [2]), flush=True)
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1, 2]
    assert _values(DeltaTableDatabase(tmp_path).get_all_records(PATH)) == [1, 2]


def _operations(path: Path) -> list[str]:
    """Return the Delta history's operations, oldest first."""
    history = deltalake.DeltaTable(str(path)).history()
    return [str(entry["operation"]) for entry in reversed(history)]


def test_commits_with_new_ids_append(tmp_path: Path) -> None:
    """Batches whose ids are all new are appended (no MERGE) and stored unchanged."""
    db = DeltaTableDatabase(tmp_path)
    for start in range(0, 9, 3):
        ids = [f"id{i}" for i in range(start, start + 3)]
        db.add_records(PATH, _rows(ids, list(range(start, start + 3))))
        _ = db.get_records_with_column_value(PATH, {"g": b"g"})  # lookups as a job does
        db.flush()
    assert _operations(tmp_path / "table") == ["WRITE", "WRITE", "WRITE"]
    assert _values(DeltaTableDatabase(tmp_path).get_all_records(PATH)) == list(range(9))


def test_a_stored_id_goes_through_merge_and_keeps_the_stored_row(
    tmp_path: Path,
) -> None:
    """A batch holding a stored id is MERGEd: the stored row stays, the new ones land."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["a", "b"], [1, 2]), flush=True)
    db.add_records(PATH, _rows(["b", "c"], [20, 3]), flush=True)
    assert _operations(tmp_path / "table") == ["WRITE", "MERGE"]
    assert _values(DeltaTableDatabase(tmp_path).get_all_records(PATH)) == [1, 2, 3]
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1, 2, 3]


def test_ids_another_writer_stored_are_not_appended_twice(tmp_path: Path) -> None:
    """New-ness is decided against the version loaded for the commit, not a cached one."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["a"], [1]), flush=True)
    assert _values(db.get_records_with_column_value(PATH, {"g": b"g"})) == [1]
    # Another writer stores ``b`` after this family loaded the table.
    DeltaTableDatabase(tmp_path).add_records(PATH, _rows(["b"], [2]), flush=True)
    db.add_records(PATH, _rows(["b", "c"], [20, 3]), flush=True)
    stored = DeltaTableDatabase(tmp_path).get_all_records(PATH, record_id_column="rid")
    assert stored is not None
    assert sorted(cast(list[bytes], stored["rid"].to_pylist())) == [b"a", b"b", b"c"]
    assert _values(stored) == [1, 2, 3]
