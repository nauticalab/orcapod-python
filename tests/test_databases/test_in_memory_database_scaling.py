"""The work of InMemoryArrowDatabase's per-row calls must not grow with stored rows (ITL-919).

A ``PipelineJob`` function node does, for every row: a lookup by column value on the
result table, a write with ``flush()``, a lookup by column value on the pipeline table and
a write with ``skip_duplicates=True``. Each of those used to filter, copy or rebuild the
whole stored table, so a run was quadratic in its rows. These tests count the Arrow bytes
the calls allocate (deterministic, unlike timing) and require the cost of the last rows
written to stay flat as the table grows.
"""

from __future__ import annotations

import hashlib

import pyarrow as pa

from orcapod.databases import InMemoryArrowDatabase

from tests.test_databases.arrow_work_counter import CountingDatabase

PATH = ("node", "v1")
WINDOW = 100


def _key(i: int) -> bytes:
    """Return the binary lookup key of row ``i``, like an input data hash."""
    return hashlib.sha256(str(i).encode()).digest()


def _row(i: int) -> pa.Table:
    """Return row ``i`` as a one-row table with a binary key, a string and an integer."""
    return pa.table(
        {
            "key": pa.array([_key(i)], type=pa.large_binary()),
            "label": pa.array([f"row-{i}"], type=pa.large_string()),
            "value": pa.array([i], type=pa.int64()),
        }
    )


def _bytes_per_row_at(stored_rows: int) -> float:
    """Arrow bytes per row the job's per-row calls allocate after ``stored_rows`` rows."""
    db = CountingDatabase(InMemoryArrowDatabase())
    for i in range(stored_rows + WINDOW):
        if i == stored_rows:
            db.counter.bytes_allocated = 0
        db.get_records_with_column_value(PATH, {"key": _key(i)})
        db.add_record(PATH, _key(i), _row(i), skip_duplicates=True)
        db.flush()
    return db.counter.bytes_allocated / WINDOW


def test_per_row_work_does_not_grow_with_stored_rows() -> None:
    """Per-row lookup, write and flush cost the same after 1,600 stored rows as after 200."""
    small = _bytes_per_row_at(200)
    large = _bytes_per_row_at(1600)
    # Eight times the rows stored: a scan of the stored rows costs about eight times
    # as much per row; work proportional to the rows touched stays flat.
    assert large < 2 * small, (small, large)
