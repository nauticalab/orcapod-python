"""Filtered Delta reads on the native reader keep the database's read contract (ITL-892).

Orcapod scans local Delta tables through a native pyarrow filesystem (ITL-892). These tests
pin how that reader combines with the read contract of ITL-884 and ITL-864, for every read
method, including the filtered ones:

- A stored table that cannot be read raises ``OrcapodDatabaseReadError``, never "no rows".
- A bad filter (an unknown column, a value that does not compare with the column) is the
  caller's error: it is raised as itself, not as ``OrcapodDatabaseReadError``, and the same error is
  raised before and after a flush.
- A filtered read returns the stored rows plus the pending rows a flush would insert,
  exactly the rows the unfiltered read returns, filtered in memory.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import OrcapodDatabaseError, OrcapodDatabaseReadError

PATH = ("table",)
ID = DeltaTableDatabase.RECORD_ID_COLUMN


def _rows(ids: list[str], values: list[int], groups: list[int]) -> pa.Table:
    """Build records keyed by ``__record_id`` with ``int64`` columns ``v`` and ``g``."""
    return pa.table(
        {
            ID: pa.array([i.encode() for i in ids], type=pa.large_binary()),
            "v": pa.array(values, type=pa.int64()),
            "g": pa.array(groups, type=pa.int64()),
        }
    )


Read = Callable[[DeltaTableDatabase], "pa.Table | None"]

# Each filtered read with the in-memory filter it must equal. ``a`` and ``c`` are stored,
# ``b`` and ``d`` pending, and the pending ``c`` repeats a stored id (the stored row wins).
FILTERED: dict[str, tuple[Read, pc.Expression]] = {
    "get_record_by_id_stored": (
        lambda db: db.get_record_by_id(PATH, b"a", record_id_column=ID),
        pc.field(ID) == b"a",
    ),
    "get_record_by_id_pending": (
        lambda db: db.get_record_by_id(PATH, b"b", record_id_column=ID),
        pc.field(ID) == b"b",
    ),
    "get_record_by_id_overlap": (
        lambda db: db.get_record_by_id(PATH, b"c", record_id_column=ID),
        pc.field(ID) == b"c",
    ),
    "get_records_by_ids": (
        lambda db: db.get_records_by_ids(
            PATH, [b"a", b"b", b"c", b"zz"], record_id_column=ID
        ),
        pc.is_in(
            pc.field(ID), pa.array([b"a", b"b", b"c", b"zz"], type=pa.large_binary())
        ),
    ),
    "get_records_with_column_value": (
        lambda db: db.get_records_with_column_value(
            PATH, {"g": 1}, record_id_column=ID
        ),
        pc.field("g") == 1,
    ),
}


def _stored_and_pending(store: Path) -> DeltaTableDatabase:
    """Store ``a`` and ``c``, then queue ``b``, ``d`` and a ``c`` that repeats a stored id."""
    db = DeltaTableDatabase(store)
    db.add_records(PATH, _rows(["a", "c"], [1, 3], [1, 0]), flush=True)
    db.add_records(PATH, _rows(["b", "c", "d"], [2, 30, 4], [1, 1, 1]))
    return db


def _sorted(table: pa.Table) -> pa.Table:
    """Return ``table`` ordered by record id."""
    return table.sort_by(ID)


@pytest.mark.parametrize("when", ["before flush", "after flush"])
@pytest.mark.parametrize("method", list(FILTERED))
def test_filtered_read_equals_the_unfiltered_read_filtered_in_memory(
    tmp_path: Path, method: str, when: str
) -> None:
    """A filtered read returns the rows of the unfiltered read that match, schema included."""
    db = _stored_and_pending(tmp_path)
    if when == "after flush":
        db.flush()
    read, condition = FILTERED[method]

    everything = db.get_all_records(PATH, record_id_column=ID)
    assert everything is not None
    # The overlapping pending ``c`` is never read: the stored row is what a flush keeps.
    assert sorted(cast(list[int], everything.column("v").to_pylist())) == [1, 2, 3, 4]
    expected = everything.filter(condition)

    actual = read(db)

    if expected.num_rows == 0:
        assert actual is None
        return
    assert actual is not None
    assert actual.schema.equals(expected.schema, check_metadata=True), (
        f"{actual.schema}\nvs\n{expected.schema}"
    )
    assert _sorted(actual).equals(_sorted(expected))


def _damage(store: Path, how: str) -> None:
    """Make every data file of the database's tables unreadable.

    Args:
        store: Root directory of the ``DeltaTableDatabase``.
        how: ``"missing"`` deletes the parquet files; ``"corrupt"`` overwrites them.
    """
    data_files = list(store.rglob("*.parquet"))
    assert data_files
    for data_file in data_files:
        if how == "missing":
            data_file.unlink()
        else:
            data_file.write_bytes(b"not a parquet file")


@pytest.mark.parametrize("how", ["missing", "corrupt"])
@pytest.mark.parametrize("pending", [False, True], ids=["stored only", "with pending"])
@pytest.mark.parametrize("method", [*FILTERED, "get_all_records"])
def test_filtered_read_of_an_unreadable_table_raises_store_read_error(
    tmp_path: Path, method: str, pending: bool, how: str
) -> None:
    """Every read of a table whose data files cannot be read raises ``OrcapodDatabaseReadError``."""
    writer = DeltaTableDatabase(tmp_path)
    writer.add_records(PATH, _rows(["a", "c"], [1, 3], [1, 0]), flush=True)
    _damage(tmp_path, how)

    db = DeltaTableDatabase(tmp_path)
    if pending:
        db.add_records(PATH, _rows(["b", "c", "d"], [2, 30, 4], [1, 1, 1]))
    if method == "get_all_records":
        read: Read = lambda db: db.get_all_records(PATH)  # noqa: E731 - one-line read
    else:
        read = FILTERED[method][0]

    with pytest.raises(OrcapodDatabaseReadError) as info:
        _ = read(db)
    assert info.value.__cause__ is not None


BAD_FILTERS = {
    "unknown column": {"no_such_column": 1},
    "value of another type": {"v": "one"},
}


@pytest.mark.parametrize("bad", list(BAD_FILTERS))
def test_bad_filter_raises_its_own_error_before_and_after_flush(
    tmp_path: Path, bad: str
) -> None:
    """A bad filter is the caller's error: never ``OrcapodDatabaseReadError``, the same at every stage."""
    db = _stored_and_pending(tmp_path)
    column_values = BAD_FILTERS[bad]

    errors: dict[str, Exception] = {}
    for when in ("before flush", "after flush"):
        with pytest.raises(Exception) as info:
            _ = db.get_records_with_column_value(PATH, column_values)
        errors[when] = info.value
        db.flush()

    for when, error in errors.items():
        assert not isinstance(error, OrcapodDatabaseError), f"{when}: {error!r}"
        assert isinstance(error, pa.ArrowException), f"{when}: {error!r}"
    assert type(errors["before flush"]) is type(errors["after flush"])
