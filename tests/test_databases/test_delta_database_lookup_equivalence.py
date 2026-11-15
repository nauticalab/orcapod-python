"""DeltaTableDatabase lookups return exactly what a filter of all its rows would (ITL-919).

Lookups are served from an indexed in-memory snapshot of the committed rows plus the
pending rows. These tests check, through the public interface, that every lookup returns
the same rows and schema as filtering ``get_all_records``, across many commits, pending
rows, scoped views, another writer, schema evolution and lookups the indexes do not serve.
Delta does not define a row order, so rows are compared sorted by record id.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases import DeltaTableDatabase

PATH = ("results", "v1")


def _row(i: int, group: str, score: int) -> pa.Table:
    """Return row ``i`` with a binary key, a group, a score and a nullable string."""
    return pa.table(
        {
            "key": pa.array([f"key-{i % 7}".encode()], type=pa.large_binary()),
            "group": pa.array([group], type=pa.large_string()),
            "score": pa.array([score], type=pa.int64()),
            "maybe": pa.array(
                [None if i % 3 == 0 else f"m{i}"], type=pa.large_string()
            ),
        }
    )


def _write(db: DeltaTableDatabase, start: int, stop: int, flush_every: int) -> None:
    """Write rows ``start`` to ``stop`` one at a time, flushing every ``flush_every``."""
    for i in range(start, stop):
        db.add_record(PATH, f"id-{i}".encode(), _row(i, f"g{i % 4}", i % 5))
        if (i + 1) % flush_every == 0:
            db.flush()


def _sorted(table: pa.Table | None) -> list[dict[str, Any]] | None:
    """Return the rows of ``table`` sorted by record id, or ``None``."""
    if table is None:
        return None
    return sorted(table.to_pylist(), key=lambda row: row["rid"])


def _expected(db: DeltaTableDatabase, pairs: dict[str, Any]) -> pa.Table | None:
    """Return what a filter of every stored row by ``pairs`` gives, or ``None``."""
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    expression = None
    for column, value in pairs.items():
        term = pc.field(column) == value
        expression = term if expression is None else expression & term
    assert expression is not None
    matched = everything.filter(expression)
    return None if matched.num_rows == 0 else matched


def _assert_same(actual: pa.Table | None, expected: pa.Table | None) -> None:
    """Assert two lookup results have the same schema and the same rows."""
    if expected is None:
        assert actual is None
        return
    assert actual is not None
    assert actual.schema == expected.schema
    assert _sorted(actual) == _sorted(expected)


LOOKUPS: list[dict[str, Any]] = [
    {"key": b"key-3"},  # indexed binary
    {"group": "g1"},  # indexed string
    {"key": b"key-3", "group": "g1"},  # two indexed columns
    {"key": b"key-2", "score": 2},  # indexed + filtered
    {"score": 4},  # integer: filtered, not indexed
    {"maybe": "m4"},  # string column holding nulls
    {"key": b"absent"},  # no match
    {"key": b"key-1", "group": "g9"},  # indexed columns that never co-occur
]


def _assert_all_lookups(db: DeltaTableDatabase) -> None:
    """Assert every lookup kind agrees with a filter of all rows."""
    for pairs in LOOKUPS:
        actual = db.get_records_with_column_value(PATH, pairs, record_id_column="rid")
        _assert_same(actual, _expected(db, pairs))
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    ids = cast(list[bytes], everything["rid"].to_pylist())
    for wanted in (ids[:1], ids[::3], ids + [b"id-absent"], [b"id-absent"]):
        by_ids = db.get_records_by_ids(PATH, wanted, record_id_column="rid")
        expected = everything.filter(pc.is_in(everything["rid"], pa.array(wanted)))
        _assert_same(by_ids, None if expected.num_rows == 0 else expected)
    for record_id in (ids[0], ids[-1], b"id-absent"):
        one = db.get_record_by_id(PATH, record_id, record_id_column="rid")
        expected = everything.filter(pc.field("rid") == record_id)
        _assert_same(one, None if expected.num_rows == 0 else expected)


@pytest.mark.parametrize("flush_every", [1, 7, 1000])
def test_lookups_match_a_filter_of_all_rows(tmp_path: Path, flush_every: int) -> None:
    """Lookups over many commits, with or without pending rows, match a filter."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 30, flush_every)
    _assert_all_lookups(db)


def test_lookups_follow_every_commit(tmp_path: Path) -> None:
    """Lookups stay right as commits extend the committed rows after a first lookup."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 5, 1)
    _assert_all_lookups(db)
    for start in range(5, 25, 5):
        _write(db, start, start + 5, 5)
        _assert_all_lookups(db)
        # Pending rows on top of the committed ones.
        db.add_record(PATH, f"id-p{start}".encode(), _row(start, "g1", 4))
        _assert_all_lookups(db)


def test_committed_match_is_found_while_rows_are_pending(tmp_path: Path) -> None:
    """A committed match is returned while another row is pending (ITL-923 item 1)."""
    db = DeltaTableDatabase(tmp_path)
    db.add_record(PATH, b"id1", _row(1, "g1", 1))
    db.flush()
    db.add_record(PATH, b"id2", _row(2, "g2", 2))
    found = db.get_records_with_column_value(PATH, {"group": "g1"})
    assert found is not None and found.num_rows == 1
    by_ids = db.get_records_by_ids(PATH, [b"id1", b"id2"])
    assert by_ids is not None and by_ids.num_rows == 2


def test_scoped_views_share_records(tmp_path: Path) -> None:
    """Writes through a scoped view are found through the root and the view."""
    root = DeltaTableDatabase(tmp_path)
    view = root.at("pipeline")
    for i in range(12):
        view.add_record(PATH, f"id-{i}".encode(), _row(i, f"g{i % 4}", i % 5))
        if i % 3 == 2:
            root.flush()
        found = root.get_records_with_column_value(
            ("pipeline",) + PATH, {"key": f"key-{i % 7}".encode()}
        )
        assert found is not None
        assert found.num_rows == i // 7 + 1
    from_view = view.get_records_with_column_value(PATH, {"group": "g2"})
    from_root = root.get_records_with_column_value(
        ("pipeline",) + PATH, {"group": "g2"}
    )
    assert from_view is not None and from_root is not None
    assert sorted(map(repr, from_view.to_pylist())) == sorted(
        map(repr, from_root.to_pylist())
    )


def test_another_writer_is_seen_after_the_next_commit(tmp_path: Path) -> None:
    """Rows another writer committed are read after this database's next commit."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 10, 5)
    _assert_all_lookups(db)
    other = DeltaTableDatabase(tmp_path)
    _write(other, 100, 105, 5)
    _write(db, 10, 15, 5)
    _assert_all_lookups(db)
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    assert everything.num_rows == 20


def test_skip_duplicates_skips_committed_and_pending_ids(tmp_path: Path) -> None:
    """``skip_duplicates`` drops ids already committed or pending."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 10, 5)
    db.add_record(PATH, b"id-new", _row(99, "g0", 0))
    db.add_record(PATH, b"id-3", _row(3, "changed", 0), skip_duplicates=True)
    db.add_record(PATH, b"id-new", _row(99, "changed", 0), skip_duplicates=True)
    db.flush()
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    assert everything.num_rows == 11
    assert "changed" not in everything["group"].to_pylist()


def test_a_stored_id_written_again_keeps_the_stored_row(tmp_path: Path) -> None:
    """A commit never replaces a stored row (insert-if-not-exists), as before."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 10, 5)
    db.add_record(PATH, b"id-3", _row(3, "changed", 0))
    db.add_record(PATH, b"id-new", _row(99, "g0", 0))
    db.flush()
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    assert everything.num_rows == 11
    assert "changed" not in everything["group"].to_pylist()
    _assert_all_lookups(db)


def test_a_new_column_evolves_the_schema_and_lookups_follow(tmp_path: Path) -> None:
    """Rows with an extra column evolve the stored schema; lookups see both shapes."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 10, 5)
    _assert_all_lookups(db)
    extra = _row(50, "g1", 1).append_column(
        "extra", pa.array(["x"], type=pa.large_string())
    )
    db.add_record(PATH, b"id-50", extra)
    found = db.get_records_with_column_value(PATH, {"group": "g1"})
    assert found is not None
    db.flush()
    _assert_all_lookups(db)
    found = db.get_records_with_column_value(PATH, {"extra": "x"})
    assert found is not None and found.num_rows == 1


def test_lookup_of_a_missing_column_still_raises(tmp_path: Path) -> None:
    """Looking up a column the table lacks raises the filter's error, not a read error."""
    db = DeltaTableDatabase(tmp_path)
    _write(db, 0, 5, 5)
    with pytest.raises(pa.ArrowInvalid):
        db.get_records_with_column_value(PATH, {"no_such_column": b"x"})
