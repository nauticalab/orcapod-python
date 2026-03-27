# Non-Active Node Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `OperatorNode.iter_packets()` and `as_table()` read-only so they never trigger upstream computation — only `run()` / `execute()` may compute.

**Architecture:** Extract two private helpers (`_make_empty_table`, `_load_cached_stream_from_db`) then rewrite the two read methods to use a 3-step passive lookup: in-memory cache → DB (REPLAY mode only) → empty result. Fix four existing tests that relied on the old eager behaviour, and add a new test file. One docstring update in `StreamBase`.

**Tech Stack:** Python 3.12, PyArrow, pytest, `unittest.mock.patch`

---

## File Map

| Action | Path | What changes |
|--------|------|-------------|
| Modify | `src/orcapod/core/nodes/operator_node.py` | Add `_make_empty_table()`, `_load_cached_stream_from_db()`; rewrite `iter_packets()`, `as_table()`; refactor `_replay_from_cache()` |
| Modify | `src/orcapod/core/streams/base.py` | Docstring of `flow()` only |
| Modify | `tests/test_core/operators/test_operator_node.py` | Add `node.run()` to `test_iter_packets`, `test_as_table` |
| Modify | `tests/test_core/operators/test_operator_node_attach_db.py` | Add `node.run()` to `test_iter_packets_without_database`, `test_iter_packets_with_database` |
| Create | `tests/test_core/operators/test_operator_node_non_active.py` | New test file — passive semantics, cascade isolation, CacheMode variants |

---

## Key Source Orientation

Before starting, skim these sections of `operator_node.py`:

- **Lines 493–554** — `_compute_and_store`, `_replay_from_cache`, `run()` (the computation paths, left untouched)
- **Lines 555–568** — `iter_packets()`, `as_table()` (the two methods we fix)
- **Lines 506–525** — `_replay_from_cache()` contains the inline empty-table code we extract into `_make_empty_table()`
- **Line 14** — `ArrowTableStream` already imported
- **Lines 31–36** — `pa` is a `LazyModule`; return type annotations must use string literals (`"pa.Table"`)

The test helpers you'll need:
- `_make_node(operator, streams, db=None, prefix=(), cache_mode=CacheMode.OFF)` — defined in `test_operator_node.py` lines 97–112; includes a DB by default
- `_make_stream(name, n)` — defined in `test_operator_node_attach_db.py` lines 14–23

---

## Task 1: Write the core failing tests

**Files:**
- Create: `tests/test_core/operators/test_operator_node_non_active.py`

These tests establish the invariant we're about to implement. Running them now will show them all failing, confirming the bug exists.

- [ ] **Step 1.1: Create the test file with core passive tests**

```python
# tests/test_core/operators/test_operator_node_non_active.py
"""Tests verifying OperatorNode iteration is read-only (PLT-1182)."""

from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import Join, MapPackets
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_source() -> ArrowTableStream:
    """Single-tag stream: id (tag), x (packet), 3 rows."""
    return ArrowTableStream(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        ),
        tag_columns=["id"],
    )


@pytest.fixture
def map_op() -> MapPackets:
    return MapPackets({"x": "renamed_x"})


def _node(operator, streams, *, db=None, cache_mode=CacheMode.OFF):
    """Build an OperatorNode, attaching DB only when provided."""
    kwargs: dict = dict(operator=operator, input_streams=streams, cache_mode=cache_mode)
    if db is not None:
        kwargs["pipeline_database"] = db
    return OperatorNode(**kwargs)


# ---------------------------------------------------------------------------
# Core invariant: iteration never triggers computation
# ---------------------------------------------------------------------------


class TestIterPacketsIsPassive:
    def test_iter_packets_without_run_returns_empty(self, simple_source, map_op):
        """iter_packets() must not call operator.process before run()."""
        node = _node(map_op, (simple_source,))
        with patch.object(map_op, "process", wraps=map_op.process) as spy:
            result = list(node.iter_packets())
        assert result == []
        spy.assert_not_called()

    def test_as_table_without_run_returns_empty_table(self, simple_source, map_op):
        """as_table() must return a zero-row table with correct schema before run()."""
        node = _node(map_op, (simple_source,))
        table = node.as_table()
        assert table.num_rows == 0
        assert "id" in table.column_names
        assert "renamed_x" in table.column_names

    def test_dunder_iter_without_run_returns_empty(self, simple_source, map_op):
        """list(node) must return [] before run()."""
        node = _node(map_op, (simple_source,))
        assert list(node) == []

    def test_flow_without_run_returns_empty(self, simple_source, map_op):
        """node.flow() must return [] before run()."""
        node = _node(map_op, (simple_source,))
        assert node.flow() == []

    def test_iter_packets_after_run_returns_results(self, simple_source, map_op):
        """After run(), iter_packets() returns the computed packets."""
        node = _node(map_op, (simple_source,))
        node.run()
        result = list(node.iter_packets())
        assert len(result) == 3
        for _, packet in result:
            assert "renamed_x" in packet.keys()

    def test_as_table_after_run_returns_results(self, simple_source, map_op):
        """After run(), as_table() returns the computed table."""
        node = _node(map_op, (simple_source,))
        node.run()
        table = node.as_table()
        assert table.num_rows == 3
        assert "renamed_x" in table.column_names
```

- [ ] **Step 1.2: Run the new tests — expect failures**

```bash
cd /path/to/orcapod-python
uv run python -m pytest tests/test_core/operators/test_operator_node_non_active.py -v 2>&1 | tail -20
```

Expected: most tests FAIL.
`test_iter_packets_without_run_returns_empty` fails because `process` IS called.
`test_as_table_without_run_returns_empty_table` fails because `num_rows == 3`, not 0.
`test_iter_packets_after_run_returns_results` and `test_as_table_after_run_returns_results` may PASS (they call `run()` first).

- [ ] **Step 1.3: Confirm existing tests still pass (baseline)**

```bash
uv run python -m pytest tests/test_core/operators/test_operator_node.py tests/test_core/operators/test_operator_node_attach_db.py -q 2>&1 | tail -5
```

Expected: `44 passed`.

---

## Task 2: Extract `_make_empty_table()` and refactor `_replay_from_cache()`

This is a pure refactor — no behaviour change. The existing `test_replay_no_cache_returns_empty_stream` test acts as the regression guard.

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`

- [ ] **Step 2.1: Add `_make_empty_table()` after the `_store_output_stream` method**

In `operator_node.py`, locate the `_store_output_stream` method (around line 393). Add this new method immediately after it (before `get_cached_output`):

```python
def _make_empty_table(self) -> "pa.Table":
    """Build a zero-row PyArrow table matching this node's full output schema.

    Uses ``output_schema()`` for column names/types and
    ``data_context.type_converter`` for the Python → Arrow type mapping.
    Requires ``self._operator is not None`` (pre-existing limitation shared
    with ``_replay_from_cache``).
    """
    tag_schema, packet_schema = self.output_schema()
    type_converter = self.data_context.type_converter
    empty_fields: dict = {}
    for name, py_type in {**tag_schema, **packet_schema}.items():
        arrow_type = type_converter.python_type_to_arrow_type(py_type)
        empty_fields[name] = pa.array([], type=arrow_type)
    return pa.table(empty_fields)
```

- [ ] **Step 2.2: Refactor `_replay_from_cache()` to use `_make_empty_table()`**

Replace the existing `_replay_from_cache` body (lines 506–525) with:

```python
def _replay_from_cache(self) -> None:
    """Load cached results from DB, skip computation.

    If no cached records exist yet, produces an empty stream with
    the correct schema (zero rows, correct columns).
    """
    records = self._pipeline_database.get_all_records(self.pipeline_path)
    if records is None:
        records = self._make_empty_table()

    tag_keys = self.keys()[0]
    self._cached_output_stream = ArrowTableStream(records, tag_columns=tag_keys)
    self._update_modified_time()
```

- [ ] **Step 2.3: Run the existing test suite — no regressions**

```bash
uv run python -m pytest tests/test_core/operators/ -q 2>&1 | tail -5
```

Expected: `44 passed` (existing tests) + the previously-passing new tests still pass. The still-failing new tests remain failing — that's fine.

- [ ] **Step 2.4: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py
git commit -m "refactor(operator_node): extract _make_empty_table() helper from _replay_from_cache"
```

---

## Task 3: Add `_load_cached_stream_from_db()` helper

This helper is state-free: it reads from the DB in REPLAY mode only and returns an `ArrowTableStream | None`. It does not assign to `_cached_output_stream` or call `_update_modified_time()`.

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`

- [ ] **Step 3.1: Add `_load_cached_stream_from_db()` after `_make_empty_table()`**

```python
def _load_cached_stream_from_db(self) -> "ArrowTableStream | None":
    """Read from DB in CacheMode.REPLAY only, without modifying node state.

    Returns an ``ArrowTableStream`` (possibly wrapping zero rows) when
    ``CacheMode.REPLAY`` is active and a database is attached; ``None``
    otherwise.

    This method is intentionally **state-free**: it never assigns to
    ``_cached_output_stream`` and never calls ``_update_modified_time()``.
    Repeated calls re-query the DB each time — in-memory caching is the
    responsibility of the computation paths (``run()``, ``execute()``).

    Guards:
        - Returns ``None`` if ``_pipeline_database is None``.
        - Returns ``None`` if ``_cache_mode != CacheMode.REPLAY``
          (LOG/OFF modes may have stale historical records in the DB).
    """
    if self._pipeline_database is None:
        return None
    if self._cache_mode != CacheMode.REPLAY:
        return None
    records = self._pipeline_database.get_all_records(self.pipeline_path)
    if records is None:
        records_table = self._make_empty_table()
    else:
        records_table = records
    tag_keys = self.keys()[0]
    return ArrowTableStream(records_table, tag_columns=tag_keys)
```

- [ ] **Step 3.2: Run the existing test suite — no regressions**

```bash
uv run python -m pytest tests/test_core/operators/ -q 2>&1 | tail -5
```

Expected: `44 passed` (new helper doesn't affect behaviour yet).

- [ ] **Step 3.3: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py
git commit -m "feat(operator_node): add _load_cached_stream_from_db() state-free helper"
```

---

## Task 4: Fix `iter_packets()` and `as_table()` — make them passive

This is the core fix. Both methods stop calling `self.run()` and instead do a 3-step passive lookup. Four existing tests will break (they depended on the eager behaviour); fix them in the same task.

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `tests/test_core/operators/test_operator_node.py`
- Modify: `tests/test_core/operators/test_operator_node_attach_db.py`

- [ ] **Step 4.1: Replace `iter_packets()` in `operator_node.py`**

Replace lines 555–558 (current `iter_packets()`):

```python
def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
    """Return an iterator over (tag, packet) pairs.

    Read-only: never triggers computation. Returns empty before ``run()``
    or ``execute()`` populates the cache. Call ``node.is_stale`` before
    iterating if you need to detect outdated cached data.
    """
    if self._cached_output_stream is not None:
        return self._cached_output_stream.iter_packets()
    db_stream = self._load_cached_stream_from_db()
    if db_stream is not None:
        return db_stream.iter_packets()
    return iter([])
```

- [ ] **Step 4.2: Replace `as_table()` in `operator_node.py`**

Replace lines 560–568 (current `as_table()`):

```python
def as_table(
    self,
    *,
    columns: ColumnConfig | dict[str, Any] | None = None,
    all_info: bool = False,
) -> "pa.Table":
    """Return the output as a PyArrow table.

    Read-only: never triggers computation. Returns a zero-row table
    with correct schema before ``run()`` or ``execute()`` is called.
    """
    if self._cached_output_stream is not None:
        return self._cached_output_stream.as_table(columns=columns, all_info=all_info)
    db_stream = self._load_cached_stream_from_db()
    if db_stream is not None:
        return db_stream.as_table(columns=columns, all_info=all_info)
    return self._make_empty_table()
```

- [ ] **Step 4.3: Run the full operator test suite to see which tests now fail**

```bash
uv run python -m pytest tests/test_core/operators/ -q 2>&1 | tail -20
```

Expected failures (4 tests):
- `test_operator_node.py::TestOperatorNodeRunAndRetrieve::test_iter_packets`
- `test_operator_node.py::TestOperatorNodeRunAndRetrieve::test_as_table`
- `test_operator_node_attach_db.py::TestOperatorNodeWithoutDatabase::test_iter_packets_without_database`
- `test_operator_node_attach_db.py::TestOperatorNodeWithDatabase::test_iter_packets_with_database`

- [ ] **Step 4.4: Fix `test_iter_packets` and `test_as_table` in `test_operator_node.py`**

Find these two tests (around lines 332–345) and add `node.run()`:

```python
def test_iter_packets(self, simple_stream, db):
    op = MapPackets({"x": "renamed_x"})
    node = _make_node(op, (simple_stream,), db=db)
    node.run()                          # <-- add this line
    packets = list(node.iter_packets())
    assert len(packets) == 3
    for tag, packet in packets:
        assert "renamed_x" in packet.keys()

def test_as_table(self, simple_stream, db):
    op = MapPackets({"x": "renamed_x"})
    node = _make_node(op, (simple_stream,), db=db)
    node.run()                          # <-- add this line
    table = node.as_table()
    assert table.num_rows == 3
    assert "renamed_x" in table.column_names
```

- [ ] **Step 4.5: Fix `test_iter_packets_without_database` and `test_iter_packets_with_database` in `test_operator_node_attach_db.py`**

```python
def test_iter_packets_without_database(self):
    node = OperatorNode(
        operator=Join(),
        input_streams=(_make_stream("a"), _make_stream("b")),
    )
    node.run()                          # <-- add this line
    results = list(node.iter_packets())
    assert len(results) == 3

def test_iter_packets_with_database(self):
    db = InMemoryArrowDatabase()
    node = OperatorNode(
        operator=Join(),
        input_streams=(_make_stream("a"), _make_stream("b")),
        pipeline_database=db,
    )
    node.run()                          # <-- add this line
    results = list(node.iter_packets())
    assert len(results) == 3
```

- [ ] **Step 4.6: Run the full suite — all 44 existing tests pass + new tests improving**

```bash
uv run python -m pytest tests/test_core/operators/ -v 2>&1 | tail -25
```

Expected: all 44 existing tests pass. The new tests from Task 1 (`test_iter_packets_without_run_returns_empty`, `test_as_table_without_run_returns_empty_table`, `test_dunder_iter_without_run_returns_empty`, `test_flow_without_run_returns_empty`, `test_iter_packets_after_run_returns_results`, `test_as_table_after_run_returns_results`) now PASS too.

- [ ] **Step 4.7: Commit**

```bash
git add \
  src/orcapod/core/nodes/operator_node.py \
  tests/test_core/operators/test_operator_node.py \
  tests/test_core/operators/test_operator_node_attach_db.py \
  tests/test_core/operators/test_operator_node_non_active.py
git commit -m "fix(operator_node): make iter_packets() and as_table() read-only (PLT-1182)

Remove self.run() from both methods. Replace with 3-step passive lookup:
1. in-memory _cached_output_stream
2. DB in CacheMode.REPLAY via _load_cached_stream_from_db()
3. empty result (iter([]) / _make_empty_table())

Update 4 existing tests that relied on eager run() in iter_packets/as_table."
```

---

## Task 5: Add cascade isolation and CacheMode tests

Now that the fix is live, add the remaining tests.

**Files:**
- Modify: `tests/test_core/operators/test_operator_node_non_active.py`

- [ ] **Step 5.1: Append cascade isolation tests to the test file**

```python
# ---------------------------------------------------------------------------
# Cascade isolation: iterating OperatorNode(B) must not trigger OperatorNode(A)
# ---------------------------------------------------------------------------


class TestCascadeIsolation:
    def test_iter_packets_does_not_cascade_upstream_operator(self, simple_source):
        """Key regression test for PLT-1182.

        Chain: SourceNode → OperatorNode(A) → OperatorNode(B)
        Iterating B without run() must not call A's operator.process.
        """
        op_a = MapPackets({"x": "y"})
        op_b = MapPackets({"y": "z"})
        node_a = _node(op_a, (simple_source,))
        node_b = _node(op_b, (node_a,))

        with patch.object(op_a, "process", wraps=op_a.process) as spy_a:
            result = list(node_b.iter_packets())

        assert result == []
        spy_a.assert_not_called()

    def test_pipeline_run_then_iterate(self, simple_source):
        """After pipeline.run() (simulated), iteration returns correct results."""
        op_a = MapPackets({"x": "y"})
        op_b = MapPackets({"y": "z"})
        node_a = _node(op_a, (simple_source,))
        node_b = _node(op_b, (node_a,))

        # Simulate pipeline.run() — topological order
        node_a.run()
        node_b.run()

        result_a = list(node_a.iter_packets())
        result_b = list(node_b.iter_packets())
        assert len(result_a) == 3
        assert len(result_b) == 3
        for _, packet in result_b:
            assert "z" in packet.keys()
```

- [ ] **Step 5.2: Append CacheMode tests**

```python
# ---------------------------------------------------------------------------
# CacheMode semantics
# ---------------------------------------------------------------------------


class TestCacheModeNonActive:
    def test_iter_packets_no_db_no_run_returns_empty(self, simple_source, map_op):
        """No DB, no run() → empty (step 3 fallback)."""
        node = OperatorNode(operator=map_op, input_streams=(simple_source,))
        assert list(node.iter_packets()) == []

    def test_iter_packets_replay_mode_no_records_returns_empty(
        self, simple_source, map_op
    ):
        """REPLAY + fresh DB (no LOG run) → empty table with correct schema."""
        db = InMemoryArrowDatabase()
        node = _node(map_op, (simple_source,), db=db, cache_mode=CacheMode.REPLAY)
        result = list(node.iter_packets())
        assert result == []

    def test_iter_packets_replay_mode_returns_db_contents(
        self, simple_source, map_op
    ):
        """REPLAY + prior LOG run → iter_packets returns DB records without run()."""
        db = InMemoryArrowDatabase()
        # Populate DB via LOG run
        node_log = _node(map_op, (simple_source,), db=db, cache_mode=CacheMode.LOG)
        node_log.run()

        # Fresh REPLAY node — no in-memory cache
        node_replay = _node(
            map_op, (simple_source,), db=db, cache_mode=CacheMode.REPLAY
        )
        result = list(node_replay.iter_packets())
        assert len(result) == 3
        for _, packet in result:
            assert "renamed_x" in packet.keys()

    def test_iter_packets_log_mode_no_run_returns_empty(
        self, simple_source, map_op
    ):
        """LOG mode: DB has prior records but iter_packets() without run() returns empty.

        This verifies the CacheMode guard in _load_cached_stream_from_db.
        """
        db = InMemoryArrowDatabase()
        # Populate DB via a LOG run so the guard is meaningfully exercised
        node_log = _node(map_op, (simple_source,), db=db, cache_mode=CacheMode.LOG)
        node_log.run()
        # Confirm records exist so we know the guard, not an empty DB, returns empty
        assert db.get_all_records(node_log.pipeline_path) is not None

        # Fresh LOG node (same DB, no in-memory cache)
        node_fresh = _node(
            map_op, (simple_source,), db=db, cache_mode=CacheMode.LOG
        )
        result = list(node_fresh.iter_packets())
        assert result == []
```

- [ ] **Step 5.3: Run only the new test file**

```bash
uv run python -m pytest tests/test_core/operators/test_operator_node_non_active.py -v 2>&1 | tail -25
```

Expected: all tests PASS.

- [ ] **Step 5.4: Run the full suite**

```bash
uv run python -m pytest tests/test_core/operators/ -q 2>&1 | tail -5
```

Expected: all tests pass (44 existing + 14 new = 58 total).

- [ ] **Step 5.5: Commit**

```bash
git add tests/test_core/operators/test_operator_node_non_active.py
git commit -m "test(operator_node): add cascade isolation and CacheMode tests for PLT-1182"
```

---

## Task 6: Update `flow()` docstring in `StreamBase`

The `flow()` docstring currently says "This will trigger any upstream computation", which is incorrect after the fix.

**Files:**
- Modify: `src/orcapod/core/streams/base.py` (lines 328–331)

- [ ] **Step 6.1: Update the docstring**

Replace the body of the `flow()` docstring:

```python
def flow(
    self,
) -> Collection[tuple[TagProtocol, PacketProtocol]]:
    """
    Returns the entire collection of (TagProtocol, PacketProtocol) as a list.
    This is a read-only operation — results reflect whatever has been computed
    by a prior ``run()`` or ``execute()`` call. If no computation has been
    performed, returns an empty list.
    """
    return [e for e in self.iter_packets()]
```

- [ ] **Step 6.2: Run the full test suite to confirm nothing broke**

```bash
uv run python -m pytest tests/test_core/operators/ -q 2>&1 | tail -5
```

Expected: all pass.

- [ ] **Step 6.3: Commit**

```bash
git add src/orcapod/core/streams/base.py
git commit -m "docs(streams): update flow() docstring — iteration is now read-only (PLT-1182)"
```

---

## Task 7: Final verification

- [ ] **Step 7.1: Run the full test suite**

```bash
uv run python -m pytest -q 2>&1 | tail -10
```

Expected: all tests pass.

- [ ] **Step 7.2: Confirm the cascade is gone with a quick smoke test**

```python
# Quick sanity check — paste into uv run python
import pyarrow as pa
from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import MapPackets
from orcapod.core.streams.arrow_table_stream import ArrowTableStream

src = ArrowTableStream(
    pa.table({"id": [1, 2], "x": [10, 20]}),
    tag_columns=["id"],
)
op_a = MapPackets({"x": "y"})
op_b = MapPackets({"y": "z"})
node_a = OperatorNode(operator=op_a, input_streams=(src,))
node_b = OperatorNode(operator=op_b, input_streams=(node_a,))

# Before run: must be empty
assert list(node_b.iter_packets()) == [], "Should be empty before run()"

# After run: must have data
node_a.run()
node_b.run()
assert len(list(node_b.iter_packets())) == 2, "Should have data after run()"
print("All assertions passed.")
```

Run: `uv run python -c "..."` (or save to a temp file and run it).

- [ ] **Step 7.3: Create pull request against `dev`**

```bash
git push -u origin HEAD
gh pr create \
  --base dev \
  --title "fix(operator_node): enforce non-active node semantics (PLT-1182)" \
  --body "$(cat <<'EOF'
## Summary

- `OperatorNode.iter_packets()` and `as_table()` previously called `self.run()` unconditionally, cascading upstream computation on every read
- Add two private helpers: `_make_empty_table()` (pure schema → zero-row table) and `_load_cached_stream_from_db()` (state-free REPLAY-only DB read)
- Rewrite `iter_packets()` and `as_table()` to be passive: check in-memory cache → DB (REPLAY only) → return empty
- Refactor `_replay_from_cache()` to use `_make_empty_table()` (deduplication)
- Update 4 existing tests that relied on eager behaviour; add 14 new tests in `test_operator_node_non_active.py`
- Update `StreamBase.flow()` docstring

## Test plan

- [ ] All existing 44 operator node tests pass
- [ ] 14 new tests in `test_operator_node_non_active.py` pass: core passive invariants, cascade isolation (`test_iter_packets_does_not_cascade_upstream_operator`), all 3 CacheModes
- [ ] `uv run python -m pytest -q` passes

Closes PLT-1182

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
