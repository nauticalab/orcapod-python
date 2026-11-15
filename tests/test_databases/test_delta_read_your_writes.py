"""Reads through a ``DeltaTableDatabase`` handle reflect every write of its family (ITL-864).

Contract: a read through any handle of one ``at()`` family returns exactly the rows a
flush would leave, pending and stored together, with no duplicates and no missing
rows. Delta's flush inserts only rows whose record id is not stored yet, so on an id
overlap the stored row is the one returned.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast
from pathlib import Path

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase
from orcapod.errors import OrcapodDatabaseReadError

PATH = ("table",)
SCOPE = "scope"


def _rows(
    ids: list[str], values: list[int], groups: list[int] | None = None
) -> pa.Table:
    """Build a record table keyed by ``__record_id`` with ``int64`` columns ``v`` and ``g``."""
    return pa.table(
        {
            "__record_id": pa.array([i.encode() for i in ids], type=pa.large_binary()),
            "v": pa.array(values, type=pa.int64()),
            "g": pa.array(
                groups if groups is not None else [0] * len(ids), type=pa.int64()
            ),
        }
    )


def _values(table: pa.Table | None) -> list[int] | None:
    """Return the sorted ``v`` values of ``table``, or ``None`` when there is no table."""
    if table is None:
        return None
    return sorted(cast(list[int], table.column("v").to_pylist()))


Read = Callable[[DeltaTableDatabase, tuple[str, ...]], "pa.Table | None"]

# Each read method, called for rows ``a=1`` and ``b=2`` (both in group 0).
READS: dict[str, Read] = {
    "get_all_records": lambda db, path: db.get_all_records(path),
    "get_record_by_id": lambda db, path: db.get_record_by_id(path, b"b"),
    "get_records_by_ids": lambda db, path: db.get_records_by_ids(path, [b"a", b"b"]),
    "get_records_with_column_value": lambda db, path: db.get_records_with_column_value(
        path, {"g": 0}
    ),
}
EXPECTED = {
    "get_all_records": [1, 2],
    "get_record_by_id": [2],
    "get_records_by_ids": [1, 2],
    "get_records_with_column_value": [1, 2],
}


@pytest.mark.parametrize("flusher", ["writer", "root", "reader"])
@pytest.mark.parametrize("method", list(READS))
def test_sibling_handle_sees_rows_flushed_by_another_handle(
    tmp_path: Path, method: str, flusher: str
) -> None:
    """A handle that already loaded the table sees rows a sibling handle wrote and flushed."""
    root = DeltaTableDatabase(tmp_path)
    reader = root.at(SCOPE)
    writer = root.at(SCOPE)
    read = READS[method]

    reader.add_records(PATH, _rows(["a"], [1]), flush=True)
    assert _values(reader.get_all_records(PATH)) == [1]  # reader loads its snapshot

    writer.add_records(PATH, _rows(["b"], [2]))
    assert _values(read(reader, PATH)) == EXPECTED[method]
    assert _values(read(root, (SCOPE, *PATH))) == EXPECTED[method]

    {"writer": writer, "root": root, "reader": reader}[flusher].flush()

    assert _values(read(reader, PATH)) == EXPECTED[method]
    assert _values(read(writer, PATH)) == EXPECTED[method]
    assert _values(read(root, (SCOPE, *PATH))) == EXPECTED[method]
    assert _values(read(root.at(SCOPE), PATH)) == EXPECTED[method]


@pytest.fixture
def stored_and_pending(tmp_path: Path) -> DeltaTableDatabase:
    """A handle with stored rows ``a=1 (g=0)``, ``b=2 (g=1)`` and pending row ``c=3 (g=1)``."""
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["a", "b"], [1, 2], [0, 1]), flush=True)
    db.add_records(PATH, _rows(["c"], [3], [1]))
    return db


def test_get_all_records_returns_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    """``get_all_records`` returns the stored rows and the pending row together."""
    assert _values(stored_and_pending.get_all_records(PATH)) == [1, 2, 3]


def test_get_record_by_id_finds_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    """``get_record_by_id`` finds a stored row and a pending row alike."""
    assert _values(stored_and_pending.get_record_by_id(PATH, b"a")) == [1]
    assert _values(stored_and_pending.get_record_by_id(PATH, b"c")) == [3]


def test_get_records_by_ids_returns_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    """``get_records_by_ids`` returns the requested stored and pending rows together."""
    assert _values(stored_and_pending.get_records_by_ids(PATH, [b"a", b"c"])) == [1, 3]


def test_get_records_with_column_value_returns_stored_and_pending(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    """``get_records_with_column_value`` matches stored and pending rows alike."""
    db = stored_and_pending
    assert _values(db.get_records_with_column_value(PATH, {"g": 1})) == [2, 3]
    assert _values(db.get_records_with_column_value(PATH, {"g": 0})) == [1]


def test_reads_with_no_match_return_none(
    stored_and_pending: DeltaTableDatabase,
) -> None:
    """A read that matches no stored or pending row returns ``None``."""
    db = stored_and_pending
    assert db.get_records_with_column_value(PATH, {"g": 9}) is None
    assert db.get_records_by_ids(PATH, [b"zz"]) is None
    assert db.get_record_by_id(PATH, b"zz") is None


@pytest.mark.parametrize(
    ("read", "expected_ids"),
    [
        (
            lambda db: db.get_all_records(PATH, record_id_column="rid"),
            [b"a", b"b", b"c"],
        ),
        (
            lambda db: db.get_records_by_ids(
                PATH, [b"a", b"c"], record_id_column="rid"
            ),
            [b"a", b"c"],
        ),
        (
            lambda db: db.get_records_with_column_value(
                PATH, [("g", 1)], record_id_column="rid"
            ),
            [b"b", b"c"],
        ),
    ],
    ids=["get_all_records", "get_records_by_ids", "get_records_with_column_value"],
)
def test_record_id_column_names_the_merged_ids(
    stored_and_pending: DeltaTableDatabase,
    read: Callable[[DeltaTableDatabase], pa.Table | None],
    expected_ids: list[bytes],
) -> None:
    """With ``record_id_column`` the ids of stored and pending rows come back under that name."""
    table = read(stored_and_pending)
    assert table is not None
    assert "__record_id" not in table.column_names
    assert sorted(cast(list[bytes], table.column("rid").to_pylist())) == expected_ids


@pytest.mark.parametrize("skip_duplicates", [False, True])
def test_pending_row_with_stored_id_reads_as_flush_keeps_it(
    tmp_path: Path, skip_duplicates: bool
) -> None:
    """A pending row whose id is already stored is never returned; the stored row is."""
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["a"], [1]), flush=True)
    db.add_records(PATH, _rows(["a", "d"], [10, 4]), skip_duplicates=skip_duplicates)

    for when in ("before flush", "after flush"):
        assert _values(db.get_all_records(PATH)) == [1, 4], when
        assert _values(db.get_record_by_id(PATH, b"a")) == [1], when
        assert _values(db.get_records_by_ids(PATH, [b"a", b"d"])) == [1, 4], when
        assert _values(db.get_records_with_column_value(PATH, {"g": 0})) == [1, 4], when
        db.flush()


@pytest.fixture
def fail_id_refresh_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the next stored-ID refresh fail once, as a transient storage error would.

    Every scan opens the table with ``DeltaTable.to_pyarrow_dataset`` (through
    ``delta_to_pyarrow_dataset``, ITL-892). The read methods pass ``as_large_types=True``
    and the stored-ID refresh does not, so only the refresh fails.
    """
    original = deltalake.DeltaTable.to_pyarrow_dataset
    calls = {"n": 0}

    def to_pyarrow_dataset(self: deltalake.DeltaTable, **kwargs: Any) -> Any:
        """Raise on the refresh's first open, then open the table as the backend does."""
        if "as_large_types" not in kwargs:
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("transient storage error")
        return original(self, **kwargs)

    monkeypatch.setattr(deltalake.DeltaTable, "to_pyarrow_dataset", to_pyarrow_dataset)


@pytest.mark.usefixtures("fail_id_refresh_once")
@pytest.mark.parametrize("method", list(READS))
def test_failed_id_refresh_is_retried_by_the_next_read(
    tmp_path: Path, method: str
) -> None:
    """After a failed stored-ID refresh, a pending row with a stored id is still not returned."""
    # Another root stores ``b``: this handle has to read the stored ids. (A handle's
    # own commit tells it which ids it stored, so it reads none afterwards, ITL-919.)
    DeltaTableDatabase(tmp_path).at(SCOPE).add_records(
        PATH, _rows(["b"], [2]), flush=True
    )
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["a", "b"], [1, 20]))

    # The failed refresh is a failed stored read (ITL-884): never "no stored ids".
    with pytest.raises(OrcapodDatabaseReadError) as info:
        _ = db.get_all_records(PATH)
    assert isinstance(info.value.__cause__, OSError)

    assert _values(READS[method](db, PATH)) == EXPECTED[method]


@pytest.mark.usefixtures("fail_id_refresh_once")
def test_get_records_by_ids_raises_on_failed_id_refresh(tmp_path: Path) -> None:
    """A failed stored-ID refresh raises ``OrcapodDatabaseReadError``, never "no records" (ITL-884)."""
    # Another root stores ``a`` and ``b``: this handle has to read the stored ids (ITL-919).
    DeltaTableDatabase(tmp_path).at(SCOPE).add_records(
        PATH, _rows(["a", "b"], [1, 2], [0, 1]), flush=True
    )
    db = DeltaTableDatabase(tmp_path).at(SCOPE)
    db.add_records(PATH, _rows(["c"], [3], [1]))

    with pytest.raises(OrcapodDatabaseReadError) as info:
        _ = db.get_records_by_ids(PATH, [b"a", b"c"])
    assert isinstance(info.value.__cause__, OSError)
    assert _values(db.get_records_by_ids(PATH, [b"a", b"c"])) == [1, 3]


@pytest.mark.parametrize("cache", ["cold", "warm"])
@pytest.mark.parametrize("method", list(READS))
def test_unreadable_table_with_sibling_pending_rows_raises_and_drops_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str, cache: str
) -> None:
    """A read never answers from pending rows alone when the stored table is unreadable.

    ``b`` is stored and ``a`` is pending through a sibling handle. While the stored
    table cannot be read, every read method raises ``OrcapodDatabaseReadError`` (ITL-884)
    rather than returning only the pending rows (ITL-864); once it is readable again
    the same read returns both rows, so nothing was dropped from the pending batch.
    """
    DeltaTableDatabase(tmp_path).at(SCOPE).add_records(
        PATH, _rows(["b"], [2]), flush=True
    )
    root = DeltaTableDatabase(tmp_path)
    reader, writer = root.at(SCOPE), root.at(SCOPE)
    if cache == "warm":
        # The family has loaded the table and its stored ids before the failure.
        assert _values(reader.get_all_records(PATH)) == [2]
        assert reader.get_record_by_id(PATH, b"a") is None
    writer.add_records(PATH, _rows(["a"], [1]))

    def unreadable(*_args: object, **_kwargs: object) -> pa.Table:
        """Fail as a storage outage would."""
        raise OSError("storage unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(deltalake.DeltaTable, "to_pyarrow_dataset", unreadable)
        patch.setattr(deltalake.DeltaTable, "to_pyarrow_table", unreadable)
        if cache == "warm" and method == "get_record_by_id":
            # ``b`` is stored, and the warm family already holds the rows of the
            # version it loaded: it answers from them without reading storage
            # (ITL-919). Every read that needs storage still raises.
            assert _values(READS[method](reader, PATH)) == EXPECTED[method]
        else:
            with pytest.raises(OrcapodDatabaseReadError) as info:
                _ = READS[method](reader, PATH)
            assert isinstance(info.value.__cause__, OSError)

    assert _values(READS[method](reader, PATH)) == EXPECTED[method]
    root.flush()
    assert _values(READS[method](reader, PATH)) == EXPECTED[method]


@pytest.mark.parametrize("method", list(READS))
def test_fresh_handle_drops_pending_rows_with_stored_ids(
    tmp_path: Path, method: str
) -> None:
    """A handle that loaded a table written by another root still drops pending rows with stored ids."""
    DeltaTableDatabase(tmp_path).add_records(PATH, _rows(["b"], [2]), flush=True)
    db = DeltaTableDatabase(tmp_path)
    assert _values(db.get_all_records(PATH)) == [2]  # loads the table, no ids yet

    db.add_records(PATH, _rows(["a", "b"], [1, 20]))

    assert _values(READS[method](db, PATH)) == EXPECTED[method]


def _table(record_id: str, x: pa.Array[Any], x_nullable: bool = True) -> pa.Table:
    """Build one row keyed by ``record_id`` with ``v=0`` and the column ``x``."""
    schema = pa.schema(
        [
            pa.field("__record_id", pa.large_binary()),
            pa.field("v", pa.int64()),
            pa.field("x", x.type, nullable=x_nullable),
        ]
    )
    return pa.table(
        [
            pa.array([record_id.encode()], type=pa.large_binary()),
            pa.array([0], type=pa.int64()),
            x,
        ],
        schema=schema,
    )


def _without_x(table: pa.Table) -> pa.Table:
    """Return ``table`` without its ``x`` column."""
    return table.drop_columns(["x"])


# (stored row, pending row) pairs whose schemas differ in a way a flush accepts.
SCHEMA_CHANGES: dict[str, tuple[pa.Table, pa.Table]] = {
    "pending_adds_column": (
        _without_x(_table("a", pa.array([1], pa.int64()))),
        _table("c", pa.array([3], pa.int64())),
    ),
    "pending_lacks_column": (
        _table("a", pa.array([1], pa.int64())),
        _without_x(_table("c", pa.array([3], pa.int64()))),
    ),
    "non_null_pending_into_nullable": (
        _table("a", pa.array([1], pa.int64())),
        _table("c", pa.array([3], pa.int64()), x_nullable=False),
    ),
    "struct_child_added": (
        _table("a", pa.array([{"p": 1}])),
        _table("c", pa.array([{"p": 3, "q": 4}])),
    ),
    "non_large_string": (
        _table("a", pa.array(["s"], pa.string())),
        _table("c", pa.array(["t"], pa.string())),
    ),
    "non_large_list": (
        _table("a", pa.array([[1]], pa.list_(pa.int64()))),
        _table("c", pa.array([[3]], pa.list_(pa.int64()))),
    ),
}

ReadAc = Callable[[DeltaTableDatabase], "pa.Table | None"]

# Each read method that can return rows ``a`` and ``c`` together, with the record id kept.
READS_AC: dict[str, ReadAc] = {
    "get_all_records": lambda db: db.get_all_records(PATH, record_id_column="rid"),
    "get_records_by_ids": lambda db: db.get_records_by_ids(
        PATH, [b"a", b"c"], record_id_column="rid"
    ),
    "get_records_with_column_value": lambda db: db.get_records_with_column_value(
        PATH, {"v": 0}, record_id_column="rid"
    ),
}


def _read_sorted(db: DeltaTableDatabase, method: str) -> pa.Table:
    """Read rows ``a`` and ``c`` with ``method`` and sort them by record id."""
    table = READS_AC[method](db)
    assert table is not None
    return table.sort_by("rid")


@pytest.mark.parametrize("method", list(READS_AC))
@pytest.mark.parametrize("change", list(SCHEMA_CHANGES))
def test_read_before_flush_matches_read_after_flush_across_schema_change(
    tmp_path: Path, change: str, method: str
) -> None:
    """Stored and pending rows whose schemas differ read back as the flushed table does."""
    stored, pending = SCHEMA_CHANGES[change]
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, stored, flush=True)
    db.add_records(PATH, pending)

    before = _read_sorted(db, method)
    db.flush()
    after = _read_sorted(db, method)

    assert before.num_rows == 2
    assert before.schema == after.schema
    assert before.to_pylist() == after.to_pylist()


@pytest.mark.parametrize(
    ("stored_x", "pending_x"),
    [
        (pa.array([1], pa.int64()), pa.array([3], pa.int32())),
        (pa.array([1], pa.int64()), pa.array(["t"], pa.large_string())),
        (pa.array([1], pa.timestamp("us")), pa.array([3], pa.timestamp("ms"))),
        (
            pa.array([1], pa.decimal128(10, 2)),
            pa.array([3], pa.decimal128(12, 2)),
        ),
    ],
    ids=["int64_vs_int32", "int64_vs_string", "timestamp_unit", "decimal_precision"],
)
@pytest.mark.parametrize("method", list(READS_AC))
def test_read_of_stored_and_pending_rows_with_different_types_raises(
    tmp_path: Path, method: str, stored_x: pa.Array[Any], pending_x: pa.Array[Any]
) -> None:
    """A column whose stored and pending types differ is never widened: the read raises."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _table("a", stored_x), flush=True)
    db.add_records(PATH, _table("c", pending_x))

    with pytest.raises(pa.ArrowTypeError, match="Field x has incompatible types"):
        _ = READS_AC[method](db)


def test_pending_null_typed_column_reads_back_but_does_not_flush(
    tmp_path: Path,
) -> None:
    """A new all-null ``null``-typed column reads back as ``null``; Delta cannot store it.

    The flush fails and the batch stays pending, so the read is unchanged after it.
    """
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _without_x(_table("a", pa.array([1]))), flush=True)
    db.add_records(PATH, _table("c", pa.nulls(1)))

    before = _read_sorted(db, "get_all_records")
    assert before.schema.field("x").type == pa.null()
    assert before.column("x").to_pylist() == [None, None]

    with pytest.raises(RuntimeError, match="Failed to flush"):
        db.flush()
    assert _read_sorted(db, "get_all_records").equals(before)


@pytest.mark.parametrize(
    ("stored", "pending", "query", "expected_ids"),
    [
        (
            _without_x(_table("a", pa.array([1], pa.int64()))),
            _table("c", pa.array([3], pa.int64())),
            {"x": 3},
            [b"c"],
        ),
        (
            _table("a", pa.array([1], pa.int64())),
            _without_x(_table("c", pa.array([3], pa.int64()))),
            {"x": 1},
            [b"a"],
        ),
    ],
    ids=["column_only_pending", "column_only_stored"],
)
def test_query_on_column_one_side_lacks_matches_read_after_flush(
    tmp_path: Path,
    stored: pa.Table,
    pending: pa.Table,
    query: dict[str, int],
    expected_ids: list[bytes],
) -> None:
    """A query on a column only one side has returns before flush what it returns after."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, stored, flush=True)
    db.add_records(PATH, pending)

    before = db.get_records_with_column_value(PATH, query, record_id_column="rid")
    db.flush()
    after = db.get_records_with_column_value(PATH, query, record_id_column="rid")

    assert before is not None and after is not None
    assert before.column("rid").to_pylist() == expected_ids
    assert before.schema == after.schema
    assert before.to_pylist() == after.to_pylist()


@pytest.mark.parametrize("flushed", [False, True])
def test_query_on_column_neither_side_has_raises(tmp_path: Path, flushed: bool) -> None:
    """A query on a column neither the stored nor the pending rows have raises, flushed or not."""
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, _rows(["a"], [1]), flush=True)
    db.add_records(PATH, _rows(["c"], [3]))
    if flushed:
        db.flush()

    with pytest.raises(pa.ArrowInvalid, match="No match for FieldRef"):
        _ = db.get_records_with_column_value(PATH, {"y": 1})


def test_pending_row_with_null_id_reads_back_like_after_flush(tmp_path: Path) -> None:
    """A pending row with a null record id is returned when a stored row also has one.

    Delta's merge matches ids with ``=``, and ``null = null`` is not true, so a flush
    inserts a pending row whose id is null even when a stored row's id is null too.
    """
    null_id = pa.table(
        {
            "__record_id": pa.array([None], type=pa.large_binary()),
            "v": pa.array([5], type=pa.int64()),
            "g": pa.array([0], type=pa.int64()),
        }
    )
    db = DeltaTableDatabase(tmp_path)
    db.add_records(PATH, pa.concat_tables([_rows(["a"], [1]), null_id]), flush=True)
    db.add_records(PATH, null_id.set_column(1, "v", pa.array([6], type=pa.int64())))

    before = _values(db.get_all_records(PATH))
    db.flush()

    assert before == _values(db.get_all_records(PATH)) == [1, 5, 6]
