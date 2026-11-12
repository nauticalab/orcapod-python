"""InMemoryArrowDatabase's indexed lookups return exactly what a filter would (ITL-919).

Lookups by a binary or string column value use hash indexes over the committed rows. These
tests check, through the public interface, that every lookup returns the same rows, in the
same order and with the same schema, as filtering all stored rows, across many flushes,
pending rows, overwritten ids, scoped views and lookups the indexes do not serve.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from orcapod.databases import InMemoryArrowDatabase

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


def _fill(db: InMemoryArrowDatabase, n: int, flush_every: int = 1) -> None:
    """Write ``n`` rows one at a time, flushing after every ``flush_every`` rows."""
    for i in range(n):
        db.add_record(PATH, f"id-{i}".encode(), _row(i, f"g{i % 4}", i % 5))
        if (i + 1) % flush_every == 0:
            db.flush()


def _expected(db: InMemoryArrowDatabase, pairs: dict[str, Any]) -> pa.Table | None:
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
    """Assert two lookup results have the same schema and rows in the same order."""
    if expected is None:
        assert actual is None
        return
    assert actual is not None
    assert actual.schema == expected.schema
    assert actual.to_pylist() == expected.to_pylist()


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


@pytest.mark.parametrize("pairs", LOOKUPS, ids=lambda p: "-".join(p))
@pytest.mark.parametrize("flush_every", [1, 3, 1000])
def test_lookup_matches_a_filter_of_all_rows(pairs: dict[str, Any], flush_every: int):
    """Lookups over many flushes return what a filter of all rows returns."""
    db = InMemoryArrowDatabase()
    _fill(db, 90, flush_every=flush_every)
    actual = db.get_records_with_column_value(PATH, pairs, record_id_column="rid")
    _assert_same(actual, _expected(db, pairs))


@pytest.mark.parametrize("pairs", LOOKUPS, ids=lambda p: "-".join(p))
def test_lookup_includes_pending_rows_after_committed_rows(pairs: dict[str, Any]):
    """Pending matches follow committed matches, as in one filter of both."""
    db = InMemoryArrowDatabase()
    _fill(db, 60)
    for i in range(60, 75):
        db.add_record(PATH, f"id-{i}".encode(), _row(i, f"g{i % 4}", i % 5))
    actual = db.get_records_with_column_value(PATH, pairs, record_id_column="rid")
    _assert_same(actual, _expected(db, pairs))


def test_lookup_accepts_a_list_of_pairs():
    """A list of (column, value) pairs works like a mapping."""
    db = InMemoryArrowDatabase()
    _fill(db, 30)
    pairs = [("key", b"key-4"), ("group", "g0")]
    actual = db.get_records_with_column_value(PATH, pairs, record_id_column="rid")
    _assert_same(actual, _expected(db, dict(pairs)))


@pytest.mark.parametrize("pairs", LOOKUPS, ids=lambda p: "-".join(p))
def test_lookup_after_a_flush_overwrites_a_committed_id(pairs: dict[str, Any]):
    """A rewritten committed id moves its row to the end and lookups follow it."""
    db = InMemoryArrowDatabase()
    _fill(db, 40)
    _ = db.get_records_with_column_value(PATH, pairs)  # build the indexes first
    # A committed id written again replaces its row, which moves to the end.
    db.add_record(PATH, b"id-5", _row(5, "g1", 4))
    db.add_record(PATH, b"id-40", _row(40, "g1", 4))
    db.flush()
    actual = db.get_records_with_column_value(PATH, pairs, record_id_column="rid")
    _assert_same(actual, _expected(db, pairs))
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    assert everything.num_rows == 41
    assert everything["rid"].to_pylist()[-2:] == [b"id-5", b"id-40"]


def test_indexes_follow_appends_after_they_are_built():
    """Indexes built early stay correct as later flushes append rows."""
    db = InMemoryArrowDatabase()
    _fill(db, 20)
    _ = db.get_records_with_column_value(PATH, {"key": b"key-1"})  # build indexes
    for i in range(20, 50):
        db.add_record(PATH, f"id-{i}".encode(), _row(i, f"g{i % 4}", i % 5))
        db.flush()
        for pairs in LOOKUPS:
            actual = db.get_records_with_column_value(
                PATH, pairs, record_id_column="rid"
            )
            _assert_same(actual, _expected(db, pairs))


def test_scoped_views_share_records_and_indexes():
    """Writes through a scoped view are found through the root and the view."""
    root = InMemoryArrowDatabase()
    view = root.at("pipeline")
    for i in range(30):
        view.add_record(PATH, f"id-{i}".encode(), _row(i, f"g{i % 4}", i % 5))
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
    assert from_view.to_pylist() == from_root.to_pylist()


def test_skip_duplicates_skips_committed_and_pending_ids():
    """``skip_duplicates`` drops ids already committed or pending."""
    db = InMemoryArrowDatabase()
    _fill(db, 20)
    db.add_record(PATH, b"id-new", _row(99, "g0", 0))
    db.add_record(PATH, b"id-3", _row(3, "changed", 0), skip_duplicates=True)
    db.add_record(PATH, b"id-new", _row(99, "changed", 0), skip_duplicates=True)
    db.flush()
    everything = db.get_all_records(PATH, record_id_column="rid")
    assert everything is not None
    assert everything.num_rows == 21
    assert "changed" not in everything["group"].to_pylist()


def test_lookup_of_a_missing_column_still_raises():
    """Looking up a column the table lacks raises, as a filter would."""
    db = InMemoryArrowDatabase()
    _fill(db, 5)
    with pytest.raises(pa.ArrowInvalid):
        db.get_records_with_column_value(PATH, {"no_such_column": b"x"})
