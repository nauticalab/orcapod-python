# ENG-379 FunctionNode Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor `FunctionNode` so `iter_packets()` is strictly read-only, computation only triggers via `run()` / `execute()` / `async_execute()`, and all duplicated DB join logic is consolidated into one helper `_load_cached_entries()`.

**Architecture:** A new internal `_load_cached_entries()` method replaces four copies of the pipeline-DB + result-DB join. `_cached_output_packets` switches from `dict[int, tuple]` (position-keyed) to `dict[str, tuple]` (entry_id-keyed). `iter_packets()` becomes a pure yield from the in-memory store with a one-shot hot-load from DB on empty cache; `run()` becomes a load-status guard that delegates to `execute()`.

**Tech Stack:** Python, PyArrow, Polars (for DB joins), pytest, asyncio

---

## File Map

| Action | Path |
|---|---|
| **Modify** | `src/orcapod/core/nodes/function_node.py` |
| **Create** | `tests/test_core/nodes/test_function_node_iteration.py` |
| **Modify** | `tests/test_core/function_pod/test_function_pod_node_stream.py` |
| **Modify** | `tests/test_core/function_pod/test_function_pod_node.py` |
| **Modify** | `tests/test_core/function_pod/test_function_node_attach_db.py` |
| **Modify** | `tests/test_data/test_polars_nullability/test_function_node_nullability.py` |

`tests/test_core/nodes/test_function_node_get_cached.py` and `tests/test_core/nodes/test_node_execute.py` need minor verification but no code changes — the key type change (`int → str`) in `_cached_output_packets` still satisfies `len(node._cached_output_packets) == N` assertions.

`tests/test_core/test_regression_fixes.py::TestConcurrentFallbackInRunningLoop` tests `FunctionPodStream._iter_packets_concurrent()` in `function_pod.py` — a **different class** that is not changed. Leave it as-is and add equivalent coverage in the new test file.

---

## Concepts for implementers

**Entry ID**: `compute_pipeline_entry_id(tag, packet)` — a hash over `(tag columns + system_tags + input_packet_hash + node_content_hash)`. Uniquely identifies one (input, node) pair. Already computed in `execute()` for every upstream packet.

**`_cached_output_packets`**: Session-level result store. After the refactor it is `dict[str, tuple[TagProtocol, PacketProtocol | None]]` keyed by entry_id. Populated by `_process_packet_internal()` (computation) and by `_load_cached_entries()` (DB hot-load). Never cleared except by `clear_cache()` or `attach_databases()`. Overwriting an existing key is safe because in-memory and DB results for the same entry_id are always semantically equivalent.

**`_load_cached_entries(entry_ids=None)`**: The new single DB join helper. Returns `dict[str, tuple[Tag, Packet]]`. Does NOT mutate `_cached_output_packets`; callers do `self._cached_output_packets.update(loaded)`.

**`iter_packets()` after the refactor**: Strictly read-only. Never calls `_process_packet_internal()`. On first call with empty `_cached_output_packets` and a DB attached, hot-loads via `_load_cached_entries()`. Otherwise yields from `_cached_output_packets.values()`. On a fresh node with no prior `run()` and empty DB, yields nothing — this is correct.

**`run()`**: Guards on `load_status` (UNAVAILABLE → RuntimeError, CACHE_ONLY → no-op), then calls `execute(self._input_stream)`.

**Tests that called `iter_packets()` to trigger computation**: Must be updated to call `node.run()` or `node.execute(node._input_stream)` first. The new `iter_packets()` is read-only.

**`as_table()` is affected**: It calls `iter_packets()` internally. Without a prior `run()`, it returns an empty table. Tests that previously relied on `as_table()` triggering computation must be updated to call `run()` first.

---

## Task 1: Write failing tests for new iteration semantics

**Files:**
- Create: `tests/test_core/nodes/test_function_node_iteration.py`

- [ ] **Step 1: Write the test file**

```python
"""Tests for the refactored FunctionNode iteration semantics.

After ENG-379:
- iter_packets() is strictly read-only — never triggers computation
- Computation only via run() / execute() / async_execute()
- execute() is always sequential; async/concurrent path is only in async_execute()
"""
from __future__ import annotations

import asyncio
from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.executors import LocalExecutor


def _make_source(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"])


def _make_node(n: int = 3, db: InMemoryArrowDatabase | None = None) -> FunctionNode:
    def double(x: int) -> int:
        return x * 2

    pf = PythonPacketFunction(double, output_keys="result")
    pod = FunctionPod(pf)
    pipeline_db = db if db is not None else InMemoryArrowDatabase()
    return FunctionNode(pod, _make_source(n=n), pipeline_database=pipeline_db)


class TestIterPacketsReadOnly:
    def test_fresh_node_no_db_yields_nothing(self):
        """iter_packets() on a fresh node with no run() and empty DB yields nothing."""
        node = _make_node()
        assert list(node.iter_packets()) == []

    def test_iter_does_not_call_process_packet_internal(self):
        """iter_packets() never calls _process_packet_internal under any non-compute path."""
        node = _make_node()
        with patch.object(node, "_process_packet_internal") as mock_proc:
            list(node.iter_packets())
            mock_proc.assert_not_called()

    def test_iter_after_db_populated_hot_loads_without_compute(self):
        """iter_packets() on a node with DB records hot-loads without _process_packet_internal."""
        db = InMemoryArrowDatabase()
        node1 = _make_node(n=3, db=db)
        node1.run()  # populate DB

        node2 = _make_node(n=3, db=db)
        with patch.object(node2, "_process_packet_internal") as mock_proc:
            results = list(node2.iter_packets())
            mock_proc.assert_not_called()
        assert len(results) == 3

    def test_after_run_iter_yields_from_cache_no_db_query(self):
        """After run(), iter_packets() yields from _cached_output_packets without DB query."""
        node = _make_node()
        node.run()
        initial_count = len(node._cached_output_packets)
        assert initial_count == 3

        with patch.object(node, "_load_cached_entries") as mock_load:
            results = list(node.iter_packets())
            mock_load.assert_not_called()
        assert len(results) == 3

    def test_iter_twice_same_order_db_queried_once(self):
        """Two successive iter_packets() calls return same order; DB queried at most once."""
        db = InMemoryArrowDatabase()
        node1 = _make_node(n=3, db=db)
        node1.run()

        node2 = _make_node(n=3, db=db)
        with patch.object(node2, "_load_cached_entries", wraps=node2._load_cached_entries) as mock_load:
            first = [(t["id"], p["result"]) for t, p in node2.iter_packets()]
            second = [(t["id"], p["result"]) for t, p in node2.iter_packets()]
            assert mock_load.call_count <= 1  # at most one DB query
        assert first == second

    def test_cached_output_packets_keyed_by_entry_id_strings(self):
        """After run(), _cached_output_packets keys are entry_id strings, not ints."""
        node = _make_node()
        node.run()
        assert len(node._cached_output_packets) == 3
        for key in node._cached_output_packets:
            assert isinstance(key, str), f"Expected str key, got {type(key)}: {key!r}"

    def test_as_table_fresh_node_returns_empty_no_compute(self):
        """as_table() on a fresh node with no run() and empty DB returns empty table."""
        node = _make_node()
        with patch.object(node, "_process_packet_internal") as mock_proc:
            table = node.as_table()
            mock_proc.assert_not_called()
        assert isinstance(table, pa.Table)
        assert len(table) == 0

    def test_run_cache_only_is_noop(self):
        """run() on a CACHE_ONLY node returns without error and without computation."""
        from orcapod.pipeline.serialization import LoadStatus

        node = _make_node()
        node._load_status = LoadStatus.CACHE_ONLY
        node._input_stream = None  # simulate no upstream

        with patch.object(node, "execute") as mock_exec:
            node.run()
            mock_exec.assert_not_called()

    def test_run_unavailable_raises(self):
        """run() on an UNAVAILABLE node raises RuntimeError."""
        from orcapod.pipeline.serialization import LoadStatus

        node = _make_node()
        node._load_status = LoadStatus.UNAVAILABLE
        with pytest.raises(RuntimeError, match="unavailable"):
            node.run()

    def test_execute_error_policy_continue_skips_failures(self):
        """execute() sequential path: on_packet_crash fires per failing packet with error_policy='continue'."""
        errors = []

        def sometimes_fail(x: int) -> int:
            if x == 1:
                raise ValueError("intentional failure")
            return x * 2

        pf = PythonPacketFunction(sometimes_fail, output_keys="result")
        pf.executor = LocalExecutor()  # non-concurrent; tests sequential execute() path
        pod = FunctionPod(pf)
        db = InMemoryArrowDatabase()
        node = FunctionNode(pod, _make_source(n=3), pipeline_database=db)

        from orcapod.pipeline.observer import NoOpObserver

        class CapturingObserver(NoOpObserver):
            def on_packet_crash(self, node_label, tag, packet, exc):
                errors.append(exc)

        results = node.execute(node._input_stream, observer=CapturingObserver(), error_policy="continue")
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        # Two non-failing packets should succeed
        assert len(results) == 2
```

- [ ] **Step 2: Run the tests to confirm they all fail**

```bash
cd /home/kurouto/kurouto-jobs/d6d5fe6c-886b-4517-b09e-1371bda77ecd/orcapod-python
python -m pytest tests/test_core/nodes/test_function_node_iteration.py -v 2>&1 | head -60
```

Expected: Multiple failures — `_load_cached_entries` does not exist, `iter_packets()` currently computes, `run()` currently calls `iter_packets()`, etc.

---

## Task 2: Implement `_load_cached_entries()` — the single DB join helper

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

Insert a new method `_load_cached_entries()` in the `# Cache-only helpers` section (after `_require_pipeline_database()`, before `_load_all_cached_records()` — around line 1051). Place it just before `_load_all_cached_records`.

- [ ] **Step 1: Add `_load_cached_entries()` to `function_node.py`**

Insert after line 249 (`_filter_by_content_hash` end) and before line 251 (`# from_descriptor`), adding it as the last private helper in the internal helpers block. Alternatively, place it just before `_load_all_cached_records` (around line 1051 — the `# Cache-only helpers` section). The latter location is preferred for grouping.

```python
def _load_cached_entries(
    self,
    entry_ids: list[str] | None = None,
) -> "dict[str, tuple[TagProtocol, PacketProtocol]]":
    """Load (tag, packet) pairs from pipeline DB + result DB.

    Args:
        entry_ids: If provided, load only these specific entry IDs.
            If ``None``, load all records for this node.

    Returns:
        dict mapping entry_id → (tag, packet). Empty dict when either
        database is None, records are empty, or no rows match.

    Does NOT mutate ``_cached_output_packets``.
    Callers merge via ``self._cached_output_packets.update(loaded)``.
    """
    if self._cached_function_pod is None or self._pipeline_database is None:
        return {}

    PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"

    taginfo = self._pipeline_database.get_all_records(
        self.node_identity_path,
        record_id_column=PIPELINE_ENTRY_ID_COL,
    )
    results = self._cached_function_pod._result_database.get_all_records(
        self._cached_function_pod.record_path,
        record_id_column=constants.PACKET_RECORD_ID,
    )

    if taginfo is None or results is None:
        return {}

    taginfo = self._filter_by_content_hash(taginfo)
    taginfo_schema = taginfo.schema
    results_schema = results.schema

    joined_df = pl.DataFrame(taginfo).join(
        pl.DataFrame(results),
        on=constants.PACKET_RECORD_ID,
        how="inner",
    )
    if entry_ids is not None:
        joined_df = joined_df.filter(
            pl.col(PIPELINE_ENTRY_ID_COL).is_in(entry_ids)
        )
    joined = joined_df.to_arrow()
    joined = arrow_utils.restore_schema_nullability(
        joined, taginfo_schema, results_schema
    )

    if joined.num_rows == 0:
        return {}

    # Derive tag keys: prefer input_stream when available; fall back to
    # taginfo column exclusion for CACHE_ONLY / deserialized nodes.
    if self._input_stream is not None:
        tag_keys = self._input_stream.keys()[0]
    else:
        tag_keys = tuple(
            c
            for c in taginfo.column_names
            if not c.startswith(constants.META_PREFIX)
            and not c.startswith(constants.SOURCE_PREFIX)
            and not c.startswith(constants.SYSTEM_TAG_PREFIX)
            and c != PIPELINE_ENTRY_ID_COL
            and c != constants.NODE_CONTENT_HASH_COL
        )

    # Drop internal columns (SOURCE_PREFIX is kept — ArrowTableStream needs it)
    drop_cols = [
        c
        for c in joined.column_names
        if c.startswith(constants.META_PREFIX)
        or c == PIPELINE_ENTRY_ID_COL
        or c == constants.NODE_CONTENT_HASH_COL
    ]
    data_table = joined.drop([c for c in drop_cols if c in joined.column_names])

    entry_ids_col = joined.column(PIPELINE_ENTRY_ID_COL).to_pylist()
    stream = ArrowTableStream(data_table, tag_columns=tag_keys)

    loaded: dict[str, tuple[TagProtocol, PacketProtocol]] = {}
    for eid, (tag, packet) in zip(entry_ids_col, stream.iter_packets()):
        loaded[eid] = (tag, packet)
    return loaded
```

- [ ] **Step 2: Run existing test suite to verify no regressions from just adding the method**

```bash
python -m pytest tests/test_core/nodes/ tests/test_core/function_pod/ -x -q 2>&1 | tail -20
```

Expected: Same pass/fail ratio as before (method is new; nothing calls it yet).

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "feat(ENG-379): add _load_cached_entries() single DB join helper"
```

---

## Task 3: Simplify `_process_packet_internal()` and `_async_process_packet_internal()`

Remove the `cache_index: int | None` parameter from both methods. Store results by `entry_id` string instead of by integer position. Remove the lines that reset `_cached_input_iterator` and `_needs_iterator` (those fields are going away in Task 6).

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 652–852

- [ ] **Step 1: Update `_process_packet_internal()`**

Replace the entire method (lines 652–708) with:

```python
def _process_packet_internal(
    self,
    tag: TagProtocol,
    packet: PacketProtocol,
    *,
    logger: PacketExecutionLoggerProtocol | None = None,
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Core compute + persist + cache.

    Used by ``execute_packet`` and ``execute``.
    Stores result in ``_cached_output_packets`` keyed by entry_id.
    Exceptions propagate to the caller — no error handling here.

    Returns:
        A ``(tag, output_packet)`` 2-tuple.
    """
    if self._cached_function_pod is not None:
        tag_out, output_packet = self._cached_function_pod.process_packet(
            tag, packet, logger=logger
        )

        if output_packet is not None:
            result_computed = bool(
                output_packet.get_meta_value(
                    self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                )
            )
            self.add_pipeline_record(
                tag,
                packet,
                packet_record_id=output_packet.datagram_id,
                computed=result_computed,
            )
    else:
        tag_out, output_packet = self._function_pod.process_packet(
            tag, packet, logger=logger
        )

    # Store by entry_id and invalidate derived caches
    entry_id = self.compute_pipeline_entry_id(tag, packet)
    self._cached_output_packets[entry_id] = (tag_out, output_packet)
    self._cached_output_table = None
    self._cached_content_hash_column = None

    return tag_out, output_packet
```

- [ ] **Step 2: Update `_async_process_packet_internal()`**

Replace lines 793–852 with:

```python
async def _async_process_packet_internal(
    self,
    tag: TagProtocol,
    packet: PacketProtocol,
    *,
    logger: PacketExecutionLoggerProtocol | None = None,
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``_process_packet_internal``.

    Computes via async path, writes pipeline provenance, caches by entry_id.
    Exceptions propagate.

    Returns:
        A ``(tag, output_packet)`` 2-tuple.
    """
    if self._cached_function_pod is not None:
        tag_out, output_packet = (
            await self._cached_function_pod.async_process_packet(
                tag, packet, logger=logger
            )
        )

        if output_packet is not None:
            result_computed = bool(
                output_packet.get_meta_value(
                    self._cached_function_pod.RESULT_COMPUTED_FLAG, False
                )
            )
            self.add_pipeline_record(
                tag,
                packet,
                packet_record_id=output_packet.datagram_id,
                computed=result_computed,
            )
    else:
        tag_out, output_packet = (
            await self._function_pod.async_process_packet(
                tag, packet, logger=logger
            )
        )

    # Store by entry_id and invalidate derived caches
    entry_id = self.compute_pipeline_entry_id(tag, packet)
    self._cached_output_packets[entry_id] = (tag_out, output_packet)
    self._cached_output_table = None
    self._cached_content_hash_column = None

    return tag_out, output_packet
```

- [ ] **Step 3: Run tests to verify `execute_packet()` still works**

`execute_packet()` delegates to `_process_packet_internal()`. The `TestFunctionNodeExecutePacket` tests verify it stores results and writes DB records.

```bash
python -m pytest tests/test_core/nodes/test_node_execute.py::TestFunctionNodeExecutePacket -v 2>&1
```

Expected: PASS (the key type change from int to str means `len(_cached_output_packets) == 1` still holds).

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): store _cached_output_packets by entry_id, remove cache_index"
```

---

## Task 4: Rewrite `run()` — load_status guard

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 1347–1350

- [ ] **Step 1: Replace `run()` (lines 1347–1350)**

```python
def run(self) -> None:
    """Eagerly compute all input packets, filling pipeline and result databases.

    Raises:
        RuntimeError: If ``load_status`` is UNAVAILABLE (no pod, no DB).
    """
    from orcapod.pipeline.serialization import LoadStatus

    if self._load_status == LoadStatus.UNAVAILABLE:
        raise RuntimeError(
            f"FunctionNode {self.label!r} is unavailable: "
            "no function pod and no database attached."
        )
    if self._load_status == LoadStatus.CACHE_ONLY:
        # Upstream is unavailable; computation requires a live input stream.
        # Callers should use iter_packets() to serve existing DB results.
        return
    self.execute(self._input_stream)
```

- [ ] **Step 2: Verify new `run()` tests pass**

```bash
python -m pytest tests/test_core/nodes/test_function_node_iteration.py::TestIterPacketsReadOnly::test_run_cache_only_is_noop tests/test_core/nodes/test_function_node_iteration.py::TestIterPacketsReadOnly::test_run_unavailable_raises -v 2>&1
```

Expected: PASS.

- [ ] **Step 3: Verify existing `test_run_fills_database` still passes**

```bash
python -m pytest tests/test_core/function_pod/test_function_pod_node.py::TestFunctionNodeStreamInterface::test_run_fills_database -v 2>&1
```

Expected: PASS (`run()` now calls `execute(self._input_stream)` which does the same computation).

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): rewrite run() with load_status guard delegating to execute()"
```

---

## Task 5: Rewrite `iter_packets()` — strictly read-only

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 1172–1285

- [ ] **Step 1: Replace `iter_packets()` (lines 1172–1285) with the read-only version**

```python
def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
    """Yield all computed (tag, packet) pairs for this node.

    Strictly read-only — never triggers computation. Callers must call
    ``run()`` or ``execute()`` first if they want results computed.

    On the first call with an empty in-memory store and a DB attached,
    hot-loads all existing records from the DB (one-shot, no recompute).

    Raises:
        RuntimeError: If ``load_status`` is UNAVAILABLE.
    """
    from orcapod.pipeline.serialization import LoadStatus

    status = self.load_status
    if status == LoadStatus.UNAVAILABLE:
        raise RuntimeError(
            f"FunctionNode {self.label!r} is unavailable: "
            "no function pod and no database attached."
        )

    if status == LoadStatus.CACHE_ONLY:
        # Upstream unavailable; serve entirely from DB.
        if not self._cached_output_packets:
            loaded = self._load_cached_entries()
            self._cached_output_packets.update(loaded)
            if loaded:
                self._cached_output_table = None
                self._cached_content_hash_column = None
        yield from (
            (tag, pkt)
            for tag, pkt in self._cached_output_packets.values()
            if pkt is not None
        )
        return

    # FULL / READ_ONLY — in-memory store may be populated from computation
    # (via execute/run) or hot-loaded from DB.
    if self.is_stale:
        self.clear_cache()

    if not self._cached_output_packets and self._cached_function_pod is not None:
        # Hot-load from DB on the first call when store is empty.
        loaded = self._load_cached_entries()
        self._cached_output_packets.update(loaded)
        if loaded:
            self._cached_output_table = None
            self._cached_content_hash_column = None

    yield from (
        (tag, pkt)
        for tag, pkt in self._cached_output_packets.values()
        if pkt is not None
    )
```

- [ ] **Step 2: Run new iteration tests**

```bash
python -m pytest tests/test_core/nodes/test_function_node_iteration.py -v 2>&1 | grep -E "PASS|FAIL|ERROR"
```

Expected: Most of the 10 tests pass now. `test_execute_error_policy_continue_skips_failures` may still fail (Task 9).

- [ ] **Step 3: Quick smoke-test on execute path**

```bash
python -m pytest tests/test_core/nodes/test_node_execute.py -v 2>&1
```

Expected: PASS (execute is unchanged; iter_packets refactor doesn't break execute).

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): rewrite iter_packets() as strictly read-only with DB hot-load"
```

---

## Task 6: Remove iterator state from `__init__`, `from_descriptor()`, `clear_cache()`

Remove `_cached_input_iterator` and `_needs_iterator` — they were only used by the now-deleted computation path in `iter_packets()`.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

- [ ] **Step 1: Remove from `__init__` (lines 121–124)**

Delete these two lines:
```python
        self._cached_input_iterator: (
            Iterator[tuple[TagProtocol, PacketProtocol]] | None
        ) = None
        self._needs_iterator = True
```

Also remove the `Iterator` import from the top-level imports if it is no longer used anywhere else. Check with:
```bash
grep -n "Iterator" src/orcapod/core/nodes/function_node.py
```
If `Iterator` still appears in type annotations on other methods, keep the import.

- [ ] **Step 2: Remove from `from_descriptor()` (lines 358–359)**

Delete these two lines in the read-only mode `__new__` block:
```python
        node._cached_input_iterator = None
        node._needs_iterator = True
```

- [ ] **Step 3: Remove from `clear_cache()` (lines 543–545)**

Change `clear_cache()` from:
```python
    def clear_cache(self) -> None:
        self._cached_input_iterator = None
        self._needs_iterator = True
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._node_identity_path_cache = None
        self._update_modified_time()
```

To:
```python
    def clear_cache(self) -> None:
        self._cached_output_packets.clear()
        self._cached_output_table = None
        self._cached_content_hash_column = None
        self._node_identity_path_cache = None
        self._update_modified_time()
```

- [ ] **Step 4: Remove `_ensure_iterator()` (lines 536–541)**

Delete the entire method:
```python
    def _ensure_iterator(self) -> None:
        """Lazily acquire the upstream iterator on first use."""
        if self._needs_iterator:
            self._cached_input_iterator = self._input_stream.iter_packets()
            self._needs_iterator = False
            self._update_modified_time()
```

- [ ] **Step 5: Update `_cached_output_packets` type annotation in `__init__` (lines 125–127)**

Change the type annotation from:
```python
        self._cached_output_packets: dict[
            int, tuple[TagProtocol, PacketProtocol | None]
        ] = {}
```
To:
```python
        self._cached_output_packets: dict[
            str, tuple[TagProtocol, PacketProtocol | None]
        ] = {}
```

- [ ] **Step 6: Run core tests**

```bash
python -m pytest tests/test_core/nodes/ tests/test_core/function_pod/test_function_node_caching.py -v -q 2>&1 | tail -20
```

Expected: The existing tests that don't call `iter_packets()` for computation pass. Some tests in `test_function_pod_node_stream.py` will now fail (they used `iter_packets()` as a computation trigger) — those are fixed in Task 12.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): remove _cached_input_iterator/_needs_iterator iterator state"
```

---

## Task 7: Simplify `get_cached_results()` — delegate to `_load_cached_entries()`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 710–791

- [ ] **Step 1: Replace `get_cached_results()` (lines 710–791) with the simplified version**

```python
def get_cached_results(
    self, entry_ids: list[str]
) -> dict[str, tuple[TagProtocol, PacketProtocol]]:
    """Retrieve cached results for specific pipeline entry IDs.

    Checks in-memory cache first. Loads only truly missing entries from DB.
    Add-only semantics: existing in-memory entries are never cleared or
    overwritten (overwrite is safe since in-memory and DB entries for the
    same entry_id are always semantically equivalent).

    Args:
        entry_ids: Pipeline entry IDs to look up.

    Returns:
        Mapping from entry_id to ``(tag, output_packet)`` for found entries.
        Empty dict if no DB is attached or no matches found.
    """
    if self._cached_function_pod is None or not entry_ids:
        return {}

    missing = [eid for eid in entry_ids if eid not in self._cached_output_packets]
    if missing:
        loaded = self._load_cached_entries(missing)
        self._cached_output_packets.update(loaded)
        if loaded:
            self._cached_output_table = None
            self._cached_content_hash_column = None

    return {
        eid: self._cached_output_packets[eid]
        for eid in entry_ids
        if eid in self._cached_output_packets
    }
```

- [ ] **Step 2: Run `test_function_node_get_cached.py`**

```bash
python -m pytest tests/test_core/nodes/test_function_node_get_cached.py -v 2>&1
```

Expected: All 5 tests pass. The `test_get_cached_results_populates_internal_cache` test manually calls `node._cached_output_packets.clear()` before calling `get_cached_results()` — this is compatible with the new add-only semantics. After clearing, `get_cached_results()` loads missing entries from DB and adds them.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): simplify get_cached_results() using _load_cached_entries(), add-only semantics"
```

---

## Task 8: Simplify `_async_execute_cache_only()`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 1136–1166

- [ ] **Step 1: Replace `_async_execute_cache_only()` (lines 1136–1166)**

```python
async def _async_execute_cache_only(
    self,
    output: "WritableChannel[tuple[TagProtocol, PacketProtocol]]",
    *,
    observer: Any | None = None,
) -> None:
    """Send all DB-cached (tag, packet) pairs to *output*.

    Used in ``CACHE_ONLY`` mode when the upstream is unavailable.
    Does not access ``_input_stream``.
    """
    from orcapod.pipeline.observer import NoOpObserver

    obs = observer if observer is not None else NoOpObserver()
    node_label = self.label
    node_hash = self.content_hash().to_string()
    ctx_obs = obs.contextualize(*self.node_identity_path)

    ctx_obs.on_node_start(node_label, node_hash, tag_schema=None)
    try:
        loaded = self._load_cached_entries()
        self._cached_output_packets.update(loaded)
        if loaded:
            self._cached_output_table = None
            self._cached_content_hash_column = None

        for tag, packet in self._cached_output_packets.values():
            if packet is not None:
                ctx_obs.on_packet_start(node_label, tag, packet)
                ctx_obs.on_packet_end(node_label, tag, packet, packet, cached=True)
                await output.send((tag, packet))
        ctx_obs.on_node_end(node_label, node_hash)
    finally:
        await output.close()
```

- [ ] **Step 2: Run async-related tests**

```bash
python -m pytest tests/ -k "async_execute or cache_only" -v -q 2>&1 | tail -20
```

Expected: No regressions on async execution paths.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): simplify _async_execute_cache_only() using _load_cached_entries()"
```

---

## Task 9: Refactor `execute()` — selective DB reload (always sequential)

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 577–650

This is the most complex change. The goals:
1. After collecting all upstream entry_ids, load only the **missing** ones from DB (not all).
2. After DB load, compute any entries still missing (truly new computation).
3. When `_executor_supports_concurrent()` returns True, dispatch `_async_process_packet_internal` concurrently via `asyncio.gather(..., return_exceptions=True)`.
4. Observer hooks (`on_packet_start`, `on_packet_end`, `on_packet_crash`) fire correctly in both paths.

- [ ] **Step 1: Add `import concurrent.futures` at the top of `execute()`**

In the body of `execute()`, add `import concurrent.futures` alongside the existing `from orcapod.pipeline.observer import NoOpObserver`.

- [ ] **Step 2: Replace `execute()` (lines 577–650)**

```python
def execute(
    self,
    input_stream: StreamProtocol,
    *,
    observer: ExecutionObserverProtocol | None = None,
    error_policy: Literal["continue", "fail_fast"] = "continue",
) -> list[tuple[TagProtocol, PacketProtocol]]:
    """Execute all packets from a stream: compute, persist, and cache.

    Computation order:
    1. Collect all (tag, packet, entry_id) from input_stream.
    2. Load only missing entry_ids from DB (selective reload).
    3. Compute truly missing entries — concurrent if executor supports it,
       sequential otherwise.
    4. Build output list, firing observer hooks for every packet.

    Args:
        input_stream: The input stream to process.
        observer: Optional execution observer for hooks.
        error_policy: ``"continue"`` skips failed packets;
            ``"fail_fast"`` re-raises on the first failure.

    Returns:
        Materialized list of (tag, output_packet) pairs, excluding
        ``None`` outputs and failed packets.
    """
    import concurrent.futures
    from orcapod.pipeline.observer import NoOpObserver

    node_label = self.label
    node_hash = self.content_hash().to_string()

    obs = observer if observer is not None else NoOpObserver()
    ctx_obs = obs.contextualize(*self.node_identity_path)

    tag_schema = input_stream.output_schema(columns={"system_tags": True})[0]
    ctx_obs.on_node_start(node_label, node_hash, tag_schema=tag_schema)

    # --- Step 1: Collect upstream entries ---
    upstream_entries: list[tuple[TagProtocol, PacketProtocol, str]] = [
        (tag, packet, self.compute_pipeline_entry_id(tag, packet))
        for tag, packet in input_stream.iter_packets()
    ]

    # --- Step 2: Selective DB reload for missing entries ---
    if self._cached_function_pod is not None:
        missing_eids = [
            eid
            for _, _, eid in upstream_entries
            if eid not in self._cached_output_packets
        ]
        if missing_eids:
            loaded = self._load_cached_entries(missing_eids)
            self._cached_output_packets.update(loaded)
            if loaded:
                self._cached_output_table = None
                self._cached_content_hash_column = None

    # --- Step 3: Compute truly missing entries ---
    to_compute = [
        (tag, pkt, eid)
        for tag, pkt, eid in upstream_entries
        if eid not in self._cached_output_packets
    ]

    if to_compute and _executor_supports_concurrent(self._packet_function):
        # Concurrent path: dispatch all missing packets via asyncio.gather
        async def _gather():
            return await asyncio.gather(
                *[
                    self._async_process_packet_internal(tag, pkt)
                    for tag, pkt, _ in to_compute
                ],
                return_exceptions=True,
            )

        try:
            asyncio.get_running_loop()
            # Already in event loop — run in a separate thread
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                gather_results = pool.submit(asyncio.run, _gather()).result()
        except RuntimeError:
            gather_results = asyncio.run(_gather())

        for (tag, pkt, eid), result in zip(to_compute, gather_results):
            if isinstance(result, BaseException):
                logger.warning(
                    "Packet execution failed in %s: %s",
                    node_label,
                    result,
                    exc_info=result,
                )
                ctx_obs.on_packet_crash(node_label, tag, pkt, result)
                if error_policy == "fail_fast":
                    ctx_obs.on_node_end(node_label, node_hash)
                    raise result
    else:
        # Sequential path
        for tag, pkt, eid in to_compute:
            pkt_logger = ctx_obs.create_packet_logger(tag, pkt)
            try:
                self._process_packet_internal(tag, pkt, logger=pkt_logger)
            except Exception as exc:
                logger.warning(
                    "Packet execution failed in %s: %s",
                    node_label,
                    exc,
                    exc_info=True,
                )
                ctx_obs.on_packet_crash(node_label, tag, pkt, exc)
                if error_policy == "fail_fast":
                    ctx_obs.on_node_end(node_label, node_hash)
                    raise

    # --- Step 4: Build output, fire observer hooks for all packets ---
    to_compute_eids = {eid for _, _, eid in to_compute}
    output: list[tuple[TagProtocol, PacketProtocol]] = []
    for tag, pkt, eid in upstream_entries:
        ctx_obs.on_packet_start(node_label, tag, pkt)
        if eid in self._cached_output_packets:
            tag_out, result = self._cached_output_packets[eid]
            if result is not None:
                ctx_obs.on_packet_end(
                    node_label,
                    tag,
                    pkt,
                    result,
                    cached=(eid not in to_compute_eids),
                )
                output.append((tag_out, result))
        # Packets that failed are absent from _cached_output_packets — silently skipped

    ctx_obs.on_node_end(node_label, node_hash)
    return output
```

- [ ] **Step 3: Run `test_node_execute.py` tests to verify execute still works**

```bash
python -m pytest tests/test_core/nodes/test_node_execute.py::TestFunctionNodeExecute -v 2>&1
```

Expected: PASS.

- [ ] **Step 4: Run the concurrent execute test**

```bash
python -m pytest tests/test_core/nodes/test_function_node_iteration.py::TestIterPacketsReadOnly::test_execute_error_policy_continue_skips_failures -v 2>&1
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): refactor execute() with selective DB reload (always sequential)"
```

---

## Task 10: Simplify `async_execute()` Phase 1

Replace the inline DB join in `async_execute()` Phase 1 (lines 1519–1571) with a call to `_load_cached_entries()`.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — lines 1519–1572

- [ ] **Step 1: Replace Phase 1 of the DB-backed branch in `async_execute()`**

Locate the block starting with:
```python
            if self._cached_function_pod is not None:
                # DB-backed async execution:
                # Phase 1: build cache lookup from pipeline DB
                PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"
                cached_by_entry_id: dict[...] = {}

                taginfo = self._pipeline_database.get_all_records(...)
                ...
```

Replace all of Phase 1 (the inline join from `taginfo = ...` through `cached_by_entry_id[eid] = (tag_out, pkt_out)`) with:

```python
            if self._cached_function_pod is not None:
                # Phase 1: build cache lookup from pipeline DB
                loaded = self._load_cached_entries()
                self._cached_output_packets.update(loaded)
                if loaded:
                    self._cached_output_table = None
                    self._cached_content_hash_column = None
                cached_by_entry_id: dict[str, tuple[TagProtocol, PacketProtocol]] = dict(loaded)
```

Phase 2 (the `_process_one_db` async closure + TaskGroup) remains unchanged except that `cached_by_entry_id` now comes from `_load_cached_entries()`.

- [ ] **Step 2: Run async pipeline integration test if available, otherwise smoke test**

```bash
python -m pytest tests/ -k "async" -v -q 2>&1 | tail -30
```

Expected: No regressions on async paths.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): simplify async_execute() Phase 1 using _load_cached_entries()"
```

---

## Task 11: Remove dead methods

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`

The following five methods are no longer used and should be deleted entirely:

| Method | Location | Replaced by |
|---|---|---|
| `_iter_packets_sequential()` | ~line 1287 | `execute()` sequential path |
| `_iter_packets_concurrent()` | ~line 1301 | removed — concurrent path did not move to `execute()`; async execution is in `async_execute()` only |
| `_iter_all_from_database()` | ~line 1119 | `_load_cached_entries()` |
| `_load_all_cached_records()` | ~line 1051 | `_load_cached_entries()` |

`_ensure_iterator()` was already removed in Task 6.

- [ ] **Step 1: Delete all four dead methods from `function_node.py`**

Delete:
1. `_iter_packets_sequential()` (~lines 1287–1299)
2. `_iter_packets_concurrent()` (~lines 1301–1345)
3. `_iter_all_from_database()` (~lines 1119–1134)
4. `_load_all_cached_records()` (~lines 1051–1117)

Verify nothing else calls these methods:
```bash
grep -n "_iter_packets_sequential\|_iter_packets_concurrent\|_iter_all_from_database\|_load_all_cached_records\|_ensure_iterator" src/orcapod/core/nodes/function_node.py
```
Expected: no results.

- [ ] **Step 2: Run full test suite**

```bash
python -m pytest tests/ -x -q 2>&1 | tail -30
```

Expected: Some tests fail in `test_function_pod_node_stream.py`, `test_function_pod_node.py`, `test_function_node_attach_db.py`, `test_function_node_nullability.py` — those are fixed in Tasks 12–13.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "refactor(ENG-379): remove dead methods (iter_sequential/concurrent, iter_all_from_database, load_all_cached_records)"
```

---

## Task 12: Fix `test_function_pod_node_stream.py`

All tests below failed because they called `iter_packets()` or `as_table()` to trigger computation. Fix: add `node.run()` before the assertion (or where computation is expected).

**Files:**
- Modify: `tests/test_core/function_pod/test_function_pod_node_stream.py`

- [ ] **Step 1: Fix `TestFunctionNodeStreamBasic` fixture — add `node.run()` in the fixture**

Change the fixture from:
```python
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        return FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
```
To:
```python
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node.run()
        return node
```

This fixes: `test_iter_packets_yields_correct_count`, `test_iter_packets_correct_values`, `test_iter_is_repeatable`, `test_dunder_iter_delegates_to_iter_packets`, `test_as_table_returns_pyarrow_table`, `test_as_table_has_correct_row_count`, `test_as_table_contains_tag_columns`, `test_as_table_contains_packet_columns`.

- [ ] **Step 2: Fix `TestFunctionNodeColumnConfig` tests**

Add `node.run()` before `as_table()` in both tests:

`test_as_table_content_hash_column`:
```python
    def test_as_table_content_hash_column(self, double_pf):
        node = _make_node(double_pf, n=3)
        node.run()
        table = node.as_table(columns={"content_hash": True})
        assert "_content_hash" in table.column_names
        assert len(table.column("_content_hash")) == 3
```

`test_as_table_sort_by_tags`:
```python
    def test_as_table_sort_by_tags(self, double_pf):
        db = InMemoryArrowDatabase()
        reversed_table = pa.table(...)
        input_stream = ArrowTableStream(reversed_table, tag_columns=["id"])
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=input_stream,
            pipeline_database=db,
        )
        node.run()
        result = node.as_table(columns={"sort_by_tags": True})
        ids: list[int] = result.column("id").to_pylist()
        assert ids == sorted(ids)
```

- [ ] **Step 3: Fix `TestFunctionNodeInactive::test_as_table_returns_cached_results_when_packet_function_inactive`**

Change from calling `node1.as_table()` to calling `node1.run()` to populate DB:
```python
    def test_as_table_returns_cached_results_when_packet_function_inactive(self, double_pf):
        n = 3
        db = InMemoryArrowDatabase()
        node1 = _make_node(double_pf, n=n, db=db)
        node1.run()              # populate DB
        table1 = node1.as_table()  # now hot-loads from cache
        assert len(table1) == n

        double_pf.set_active(False)

        node2 = _make_node(double_pf, n=n, db=db)
        table2 = node2.as_table()  # hot-loads from DB (function inactive but DB has results)

        assert isinstance(table2, pa.Table)
        assert len(table2) == n
        assert table2.column("result").to_pylist() == table1.column("result").to_pylist()
```

- [ ] **Step 4: Fix `TestIterPacketsDbPhase::test_db_served_results_have_correct_values`**

The test calls `_make_node(...).as_table()` for both nodes. Add `run()` to the first node (which populates DB) — the second node should then hot-load:

```python
    def test_db_served_results_have_correct_values(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()

        node1 = _make_node(double_pf, n=n, db=db)
        node1.run()
        table1 = node1.as_table()
        table2 = _make_node(double_pf, n=n, db=db).as_table()  # hot-loads from DB

        assert sorted(table1.column("result").to_pylist()) == sorted(
            table2.column("result").to_pylist()
        )
```

- [ ] **Step 5: Fix `TestIterPacketsMissingEntriesOnly::test_partial_fill_total_row_count_correct`**

After the refactor, `iter_packets()` on a fresh node with 2 DB entries and 4 upstream inputs will hot-load and yield 2 (not 4). Add `run()` before `iter_packets()`:

```python
    def test_partial_fill_total_row_count_correct(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=2, db=db))
        node = _make_node(double_pf, n=n, db=db)
        node.run()  # computes the 2 missing entries
        packets = list(node.iter_packets())
        assert len(packets) == n
```

- [ ] **Step 6: Fix `TestIterPacketsMissingEntriesOnly::test_partial_fill_all_values_correct`**

```python
    def test_partial_fill_all_values_correct(self, double_pf):
        n = 4
        db = InMemoryArrowDatabase()
        _fill_node(_make_node(double_pf, n=2, db=db))
        node = _make_node(double_pf, n=n, db=db)
        node.run()  # computes the 2 missing entries
        table = node.as_table()
        assert sorted(table.column("result").to_pylist()) == [0, 2, 4, 6]
```

- [ ] **Step 7: Fix `TestFunctionNodeStaleness::test_clear_cache_resets_output_packets`**

Change `list(node.iter_packets())` to `node.run()`:
```python
    def test_clear_cache_resets_output_packets(self, double_pf):
        node = _make_node(double_pf, n=3)
        node.run()  # populate _cached_output_packets
        assert len(node._cached_output_packets) == 3

        node.clear_cache()
        assert len(node._cached_output_packets) == 0
        assert node._cached_output_table is None
```

- [ ] **Step 8: Fix `TestFunctionNodeStaleness::test_clear_cache_produces_same_results_on_re_iteration`**

Add `node.run()` before first `as_table()`:
```python
    def test_clear_cache_produces_same_results_on_re_iteration(self, double_pf):
        node = _make_node(double_pf, n=3)
        node.run()
        table_before = node.as_table()

        node.clear_cache()
        table_after = node.as_table()  # hot-loads from DB after clear

        assert sorted(table_before.column("result").to_pylist()) == sorted(
            table_after.column("result").to_pylist()
        )
```

- [ ] **Step 9: Fix `TestFunctionNodeStaleness::test_iter_packets_auto_detects_stale_and_repopulates`**

Add `node.run()` to populate DB before first `iter_packets()`:
```python
    def test_iter_packets_auto_detects_stale_and_repopulates(self, double_pf):
        import time

        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=input_stream,
            pipeline_database=db,
        )
        node.run()  # populate DB
        first = list(node.iter_packets())  # hot-loads or serves from cache

        time.sleep(0.01)
        input_stream._update_modified_time()
        assert node.is_stale

        second = list(node.iter_packets())  # detects stale, clears, hot-loads from DB
        assert len(second) == len(first)
        assert [p["result"] for _, p in second] == [p["result"] for _, p in first]
```

- [ ] **Step 10: Fix `TestFunctionNodeStaleness::test_as_table_auto_detects_stale_and_repopulates`**

Add `node.run()` before first `as_table()`. Read the full test first (around line 444) and add `node.run()` before `table_before = node.as_table()`.

- [ ] **Step 11: Run the fixed file**

```bash
python -m pytest tests/test_core/function_pod/test_function_pod_node_stream.py -v 2>&1 | tail -30
```

Expected: All tests pass.

- [ ] **Step 12: Commit**

```bash
git add tests/test_core/function_pod/test_function_pod_node_stream.py
git commit -m "test(ENG-379): update test_function_pod_node_stream.py — add run() before compute-dependent assertions"
```

---

## Task 13: Fix remaining test files

**Files:**
- Modify: `tests/test_core/function_pod/test_function_pod_node.py`
- Modify: `tests/test_core/function_pod/test_function_node_attach_db.py`
- Modify: `tests/test_data/test_polars_nullability/test_function_node_nullability.py`

- [ ] **Step 1: Fix `test_function_pod_node.py::TestFunctionNodeStreamInterface`**

Two tests use `iter_packets()` without prior `run()`:

```python
class TestFunctionNodeStreamInterface:
    @pytest.fixture
    def node(self, double_pf) -> FunctionNode:
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=FunctionPod(packet_function=double_pf),
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node.run()   # ← add this
        return node

    def test_iter_packets_correct_values(self, node):
        assert [packet["result"] for _, packet in node.iter_packets()] == [0, 2, 4]

    def test_node_is_stream_protocol(self, node):
        assert isinstance(node, StreamProtocol)

    def test_dunder_iter_delegates_to_iter_packets(self, node):
        assert len(list(node)) == len(list(node.iter_packets()))

    def test_run_fills_database(self, node):
        # node.run() already called in fixture; just verify DB
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3
```

- [ ] **Step 2: Fix `test_function_node_attach_db.py` — four tests**

**`test_iter_packets_without_database`** (line 46): Add `node.run()`:
```python
    def test_iter_packets_without_database(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream(n=3))
        node.run()
        results = list(node.iter_packets())
        assert len(results) == 3
        assert results[0][1]["result"] == 0
```

**`test_attach_databases_clears_caches`** (line 77): Use `node.run()` to populate cache:
```python
    def test_attach_databases_clears_caches(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream())
        node.run()  # populate _cached_output_packets
        assert len(node._cached_output_packets) > 0
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        assert len(node._cached_output_packets) == 0
```

**`test_iter_packets_after_attach_works`** (line 106): Add `node.run()`:
```python
    def test_iter_packets_after_attach_works(self):
        node = FunctionNode(function_pod=_make_pod(), input_stream=_make_stream(n=2))
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db, result_database=db)
        node.run()
        results = list(node.iter_packets())
        assert len(results) == 2
```

**`test_iter_packets_with_database`** (line 135): Add `node.run()`:
```python
    def test_iter_packets_with_database(self):
        db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=_make_pod(),
            input_stream=_make_stream(n=3),
            pipeline_database=db,
            result_database=db,
        )
        node.run()
        results = list(node.iter_packets())
        assert len(results) == 3
```

- [ ] **Step 3: Fix `test_function_node_nullability.py::TestFunctionNodeIterPacketsNullability`**

The test calls `fn_node._iter_all_from_database()` which is removed. Replace with `_load_cached_entries()`:

```python
    def test_iter_packets_from_database_preserves_non_nullable_output(self):
        """Packets loaded from DB via _load_cached_entries carry non-nullable output schema."""
        database = InMemoryArrowDatabase()
        source = op.sources.DictSource(
            [{"id": 1, "x": 7}],
            tag_columns=["id"],
        )

        @op.function_pod(output_keys=["result"])
        def add_one(x: int) -> int:
            return x + 1

        pipeline = op.Pipeline("test_iter_packets_nullable", database)
        with pipeline:
            add_one.pod(source)

        pipeline.run()

        fn_nodes = _get_function_nodes(pipeline)
        fn_node = fn_nodes[0]

        # Load from DB via the new helper
        loaded = fn_node._load_cached_entries()
        packets_seen = list(loaded.values())
        assert len(packets_seen) == 1, "Expected one packet from the database"

        _tag, packet = packets_seen[0]
        packet_schema = packet.arrow_schema()

        result_field = packet_schema.field("result")
        assert result_field.nullable is False, (
            f"Packet 'result' field should be non-nullable (int return type), "
            f"but got nullable={result_field.nullable}. "
            "Arrow→Polars→Arrow round-trip in _load_cached_entries dropped nullability."
        )
```

Update the docstring at the class level to reflect the method name change.

- [ ] **Step 4: Run all three fixed test files**

```bash
python -m pytest \
  tests/test_core/function_pod/test_function_pod_node.py \
  tests/test_core/function_pod/test_function_node_attach_db.py \
  tests/test_data/test_polars_nullability/test_function_node_nullability.py \
  -v 2>&1 | tail -30
```

Expected: All pass.

- [ ] **Step 5: Commit**

```bash
git add \
  tests/test_core/function_pod/test_function_pod_node.py \
  tests/test_core/function_pod/test_function_node_attach_db.py \
  tests/test_data/test_polars_nullability/test_function_node_nullability.py
git commit -m "test(ENG-379): update remaining test files for read-only iter_packets semantics"
```

---

## Task 14: Fix `as_table()` for empty result set

**Context:** After the `iter_packets()` refactor, `as_table()` on a fresh node with no prior `run()` and an empty DB will enter the `for tag, packet in self.iter_packets():` loop and exit immediately (zero iterations). `self._cached_output_table` is never set, so the `assert self._cached_output_table is not None` line (line ~1389) raises `AssertionError`. New test 7 (`test_as_table_fresh_node_returns_empty_no_compute`) requires `as_table()` to return a valid empty `pa.Table`, not raise.

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py` — `as_table()` method (~line 1356)

- [ ] **Step 1: Patch `as_table()` to handle the empty case**

Locate the block inside `as_table()` that starts with:
```python
        if self._cached_output_table is None:
            all_tags = []
            all_packets = []
            tag_schema, packet_schema = None, None
            for tag, packet in self.iter_packets():
                ...

            converter = self.data_context.type_converter
            ...
            self._cached_output_table = arrow_utils.hstack_tables(
                all_tags_as_tables, all_packets_as_tables
            )
        assert self._cached_output_table is not None, (
            "_cached_output_table should not be None here."
        )
```

After the `for` loop but before building `_cached_output_table`, add a short-circuit for the empty case:

```python
            if not all_tags:
                # No packets — return an empty table with the node's output schema.
                # Build column-typed empty arrays from the output schema so the
                # table is usable even with 0 rows.
                tag_schema_out, packet_schema_out = self.output_schema()
                empty_tag = pa.table(
                    {k: pa.array([], type=pa.from_numpy_dtype(object))
                     for k in tag_schema_out}
                ) if tag_schema_out else pa.table({})
                empty_pkt = pa.table(
                    {k: pa.array([], type=pa.from_numpy_dtype(object))
                     for k in packet_schema_out}
                ) if packet_schema_out else pa.table({})
                self._cached_output_table = arrow_utils.hstack_tables(
                    empty_tag, empty_pkt
                )
```

Then convert the `assert` to a conditional:
```python
        if self._cached_output_table is None:
            # This should not happen in practice, but guard defensively.
            self._cached_output_table = pa.table({})
```

> **Note:** The goal is simply that `as_table()` returns a valid `pa.Table` with 0 rows rather than raising. The exact column schema of an empty table is less critical — tests only assert `len(table) == 0` and `isinstance(table, pa.Table)`. A minimal empty table (`pa.table({})`) satisfies both. Use the simplest approach that works.

**Simplest acceptable implementation:**
```python
            if not all_tags:
                self._cached_output_table = pa.table({})
```

Replace the `assert` with:
```python
        if self._cached_output_table is None:
            self._cached_output_table = pa.table({})
```

- [ ] **Step 2: Run test 7 to verify it passes**

```bash
python -m pytest tests/test_core/nodes/test_function_node_iteration.py::TestIterPacketsReadOnly::test_as_table_fresh_node_returns_empty_no_compute -v 2>&1
```

Expected: PASS.

- [ ] **Step 3: Verify existing `as_table()` tests still pass**

```bash
python -m pytest tests/test_core/function_pod/test_function_pod_node_stream.py -k "as_table" -v 2>&1
```

Expected: All pass (tests that required computed results have `run()` added in Task 12).

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py
git commit -m "fix(ENG-379): as_table() returns empty pa.Table when iter_packets() yields nothing"
```

---

## Task 15: Final full test suite run

- [ ] **Step 1: Run the full test suite**

```bash
python -m pytest tests/ -v -q 2>&1 | tail -40
```

Expected: All tests pass. If any failures remain:

- `test_function_pod_node_stream.py` failures → check if `node.run()` was added in Task 12
- `as_table()` `AssertionError` → check Task 14 fix
- `test_regression_fixes.py::TestConcurrentFallbackInRunningLoop` → tests `FunctionPodStream` (not `FunctionNode`) — should pass untouched
- Import errors for `_iter_all_from_database` or `_load_all_cached_records` → check Task 11 deletion and Task 13 test fixes
- `_cached_output_packets` key type errors → ensure all accesses use str keys; any code reading `self._cached_output_packets[i]` with an int `i` must be updated

- [ ] **Step 2: Run only the new iteration tests to confirm all 10 pass**

```bash
python -m pytest tests/test_core/nodes/test_function_node_iteration.py -v 2>&1
```

Expected: 10/10 PASS.

- [ ] **Step 3: Final commit**

```bash
git add tests/test_core/nodes/test_function_node_iteration.py
git commit -m "test(ENG-379): add test_function_node_iteration.py — 10 tests for read-only iteration semantics

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Out of Scope

- **ENG-381**: Concurrency control flag/argument for `execute()` — tracked separately
- `get_all_records()` DB join — different return type / responsibility; not refactored here
- `OperatorNode`, `SourceNode` — not touched
- `FunctionPodStream` in `function_pod.py` — has its own `iter_packets()` with concurrent path; not changed
