# ITL-864 DeltaTableDatabase read-your-writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every read method of `DeltaTableDatabase` returns exactly the rows a flush would leave, through every handle of an `at()` family, so `node.get_all_records()` returns every row right after the first run on a Delta store.

**Architecture:** Two defects, one contract. (1) `at()` handles share the root's pending batches, and any handle's `flush()` writes every pending key, but each handle keeps its own `_delta_table_cache`, so a handle that loaded a table earlier keeps serving a stale `DeltaTable` snapshot after a sibling flushed. The fix shares `_delta_table_cache`, `_existing_ids_cache` and `_cache_dirty` across the family, exactly as pending state is already shared. (2) The read methods merge pending and stored rows inconsistently (`get_records_by_ids` ignores pending; `get_records_with_column_value` ignores stored whenever a pending batch exists; overlapping ids show twice). The fix: every read returns stored rows plus pending rows whose record id is not stored (Delta's flush is `when_not_matched_insert_all`, so the stored row wins), `None` when empty.

**Tech Stack:** Python, pyarrow, deltalake, pytest, `uv run`.

Approved by the coordinator (option A, Nov 10, 2026). No public API, hash, stored schema or dependency change. Error handling is untouched (PR #290 / ITL-884 rewrites it in the same file; merge main after it lands).

---

## Reproduction (Step 1 of the brief)

`PipelineJob`, `DictSource` of 2 or 3 ints, `int -> int` pod, `DeltaTableDatabase`:

| store | orchestrator | n | `get_all_records` | `iter_data` |
|---|---|---|---|---|
| delta | sync | 2 / 3 | 1 / 1 | 2 / 3 |
| delta | async | 2 / 3 | 1 / 2 | 2 / 3 |
| memory | both | 2 / 3 | 2 / 3 | 2 / 3 |

Trace: the result-store handle (`at("pipeline", "_result")`) flushes the pipeline table's rows; the node's pipeline handle (`at("pipeline")`) cached the table at version 0 and still reads version 0 while the table is at version 2. Its pending batch is empty; a fresh handle reads all rows.

## Contract and per-method findings (database level)

Contract: a read through a handle reflects all writes of its `at()` family, pending and flushed together, and equals what a flush would leave: no duplicates, no missing rows.

| case | `get_all_records` | `get_record_by_id` | `get_records_by_ids` | `get_records_with_column_value` |
|---|---|---|---|---|
| A sibling handle flushed | stale | stale | stale | stale |
| B stored + pending, one handle | ok | ok | misses pending | misses stored when pending exists; empty table instead of `None` |
| C pending id already stored, `skip_duplicates=False` | duplicate | returns pending row, flush keeps stored | (dup) | (dup) |
| D same, `skip_duplicates=True` | ok | ok | ok | ok |
| `record_id_column=` | returns (no hang) | | | |

Other backends: `InMemoryArrowDatabase` shares `_tables` across `at()` and `ConnectorArrowDatabase` re-reads committed rows on every call, so neither goes stale; both merge pending + committed in every method. Both flush with the NEW row winning on id overlap (Delta keeps the stored row) and show both rows before flush under `skip_duplicates=False`. Report only.

Thread safety: the async orchestrator runs only the user function in `run_in_executor` (`core/executors/local.py`); every database read and write happens on the event-loop thread, as for the already-shared pending dicts, so sharing the caches adds no cross-thread access.

## File structure

- Modify: `src/orcapod/databases/delta_lake_databases.py` — `__init__` / `at()` share caches; new helpers `_pending_not_stored` and `_combine_stored_and_pending`; the four read methods use them.
- Create: `tests/test_databases/test_delta_read_your_writes.py` — database-level regression tests.
- Create: `tests/test_pipeline/test_delta_node_records_after_run.py` — `PipelineJob` regression, both orchestrators.
- Modify: `DESIGN_ISSUES.md` — one entry appended at the end.

---

### Task 1: Failing database-level tests

**Files:** Create `tests/test_databases/test_delta_read_your_writes.py`

- [ ] **Step 1: Write the tests**

Helpers: `_rows(db, ids, vals, groups)` builds a table with `__record_id` (`large_binary`), `v` (`int64`), `g` (`int64`); `_values(table)` returns sorted `v` or `None`. A `READS` parametrization maps each read method to a call and its expected values:

```python
READS = {
    "get_all_records": (lambda db: db.get_all_records(PATH), [1, 2]),
    "get_record_by_id": (lambda db: db.get_record_by_id(PATH, b"b"), [2]),
    "get_records_by_ids": (lambda db: db.get_records_by_ids(PATH, [b"a", b"b"]), [1, 2]),
    "get_records_with_column_value": (
        lambda db: db.get_records_with_column_value(PATH, {"g": 0}), [1, 2]),
}
```

Tests:
1. `test_sibling_handle_sees_rows_flushed_by_another_handle[method][flusher]`: `root = DeltaTableDatabase(tmp_path)`, `h1, h2 = root.at("s"), root.at("s")`; `h1` writes `a=1` with `flush=True` and reads once (loads its snapshot); `h2` writes `b=2`; read through `h1` and through `root` (path `("s",) + PATH`) before flush; flush through `flusher` in (`h2`, `root`, `h1`); read through `h1`, `root` and a new `root.at("s")` after flush. All equal the expected values.
2. `test_pending_and_stored_rows_are_both_returned[method]`: one handle, stored `a=1 (g=0), b=2 (g=1)`, pending `c=3 (g=1)`: `get_records_by_ids([a, c]) == [1, 3]`, `get_records_with_column_value({"g": 1}) == [2, 3]`, `{"g": 0} == [1]`, `get_all_records == [1, 2, 3]`, `get_record_by_id(c) == [3]`.
3. `test_no_match_returns_none`: stored + pending, `get_records_with_column_value({"g": 9})` and `get_records_by_ids([b"zz"])` are `None`.
4. `test_pending_row_with_stored_id_reads_as_flush_keeps_it[skip_duplicates]`: stored `a=1`; pending add of `a=10, d=4`; before flush each method returns `a=1` and `d=4` once (`get_all_records == [1, 4]`, `get_record_by_id(a) == [1]`, `get_records_by_ids([a, d]) == [1, 4]`, `get_records_with_column_value({"g": 0}) == [1, 4]`); after `flush()` the same values.
5. `test_record_id_column_is_returned_for_merged_rows`: stored + pending, `get_all_records(PATH, record_id_column="rid")` has column `rid` with all three ids and no `__record_id`; same for `get_records_by_ids` and `get_records_with_column_value`.

- [ ] **Step 2: Run, verify the expected failures**

Run: `uv run pytest tests/test_databases/test_delta_read_your_writes.py -v`
Expected: test 1 fails for every method after flush; test 2 fails for `get_records_by_ids` / `get_records_with_column_value`; test 3 fails (empty table, not `None`); test 4 fails for `skip_duplicates=False`.

### Task 2: Failing PipelineJob test

**Files:** Create `tests/test_pipeline/test_delta_node_records_after_run.py`

- [ ] **Step 1: Write the test**

```python
@pytest.mark.parametrize("orchestrator", [SyncPipelineOrchestrator, AsyncPipelineOrchestrator])
def test_node_returns_every_row_right_after_first_run(tmp_path, orchestrator):
    source = op.sources.DictSource([{"id": i, "x": 10 * i} for i in range(3)], tag_columns=["id"])

    @op.function_pod(output_keys=["result"])
    def double(x: int) -> int:
        return x * 2

    job = PipelineJob(store=DeltaTableDatabase(tmp_path))
    with job:
        double.pod(source, label="double")
    job.run(orchestrator=orchestrator())

    records = job.nodes["double"].get_all_records()
    assert records is not None
    assert sorted(records.column("result").to_pylist()) == [0, 20, 40]
```

- [ ] **Step 2: Run, verify it fails** (`1 != 3` rows) for both orchestrators.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_databases/test_delta_read_your_writes.py tests/test_pipeline/test_delta_node_records_after_run.py
git commit -m "test(databases): regression tests for Delta reads missing own writes (ITL-864)"
```

### Task 3: Share the snapshot caches across the `at()` family

**Files:** Modify `src/orcapod/databases/delta_lake_databases.py` (`__init__`, `at()`)

- [ ] **Step 1:** Add keyword-only private constructor parameters `_shared_delta_table_cache`, `_shared_existing_ids_cache`, `_shared_cache_dirty`, used when given (same pattern as `_shared_pending_batches`); `at()` passes the root instance's dicts. Comment why: any family member's `flush()` writes every pending key and refreshes the cache entry for it, so the snapshot cache must be family-wide or a sibling keeps reading the old version.
- [ ] **Step 2:** Run Task 1 test 1 and the Task 2 test: expected PASS for test 1 and Task 2.

### Task 4: Merge stored and pending rows the way flush does

**Files:** Modify `src/orcapod/databases/delta_lake_databases.py` (read methods)

- [ ] **Step 1:** Add helpers:

```python
def _pending_not_stored(self, record_path):
    """Pending rows whose record id is not stored; flush inserts only these."""
    pending = self._pending_batches.get(self._get_record_key(record_path))
    if pending is None or pending.num_rows == 0:
        return None
    stored_ids = self._get_existing_ids(record_path)
    if stored_ids:
        id_type = pending.schema.field(self.RECORD_ID_COLUMN).type
        pending = pending.filter(pc.invert(pc.is_in(
            pending[self.RECORD_ID_COLUMN], value_set=pa.array(list(stored_ids), type=id_type))))
    return pending if pending.num_rows > 0 else None

def _combine_stored_and_pending(self, stored, pending, record_id_column):
    parts = [t for t in (stored, pending) if t is not None and t.num_rows > 0]
    if not parts:
        return None
    table = parts[0] if len(parts) == 1 else pa.concat_tables(parts)
    return self._handle_record_id_column(table, record_id_column)
```

- [ ] **Step 2:** `get_all_records`: pending part uses `_pending_not_stored`. `get_record_by_id`: serve from pending only when the id is pending and not stored. `get_records_by_ids`: stored read (existing try/except unchanged) plus pending filtered by the ids. `get_records_with_column_value`: stored read (existing try/except unchanged) plus pending filtered by the expression; `None` when empty.
- [ ] **Step 3:** Run `uv run pytest tests/test_databases tests/test_pipeline -q`: all pass.
- [ ] **Step 4:** Commit `fix(databases): Delta reads return every row the handle family wrote (ITL-864)`.

### Task 5: Design issue entry, full verification, PR

- [ ] Append one `DESIGN_ISSUES.md` entry (resolved, with **Fix:**; note that writes from other processes are still not seen until a handle reloads the table — follow-up for the coordinator to file).
- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`, `uv run --with basedpyright basedpyright` (exit code), `uv run pytest tests/` (golden hashes unchanged).
- [ ] Merge `origin/main`, rerun if it moved, push, PR "Fixes ITL-864", `orca linear attach`, `orca worktree set --pr`.

---

## Review rounds and continuation notes (successor-readable)

Done and pushed (verify, do not redo):
- Round 1 (e9c78b5c): a failed ID refresh keeps the cache dirty; `get_records_by_ids` refresh shares the stored-read handler (8f9adf8b).
- Round 2 (8f9adf8b): refresh when the record key has no ID-cache entry; stored + pending combined with `normalize_table_to_large_types` + `pa.concat_tables(promote_options="default")` (b8bcc817). Tests: `test_fresh_handle_drops_pending_rows_with_stored_ids`, evolved-schema tests.

Round 3 (CodeRabbit review of b8bcc817, outside-diff, `get_records_with_column_value`), approved fix:
- A side (stored schema, read without loading data; pending column names) that lacks a queried column contributes no rows; after a flush its rows hold null there and `col == value` never matches null. A column on neither side keeps today's `ArrowInvalid` ("No match for FieldRef").
- Tests: `test_query_on_column_one_side_lacks_matches_read_after_flush[column_only_pending|column_only_stored]`, `test_query_on_column_neither_side_has_raises[flushed]`.
- Also the round-1 outside-diff trivial: `_add_to_pending_batch` cast `list[str]` -> `list[bytes]`.
- Reply: signed top-level PR comment (no thread for outside-diff findings), and one for the round-1 trivial.

Then: merge origin/main (b197544e); wait for PR #290 (ITL-884, StoreReadError) to merge; merge main again, take #290's error handling around `_pending_not_stored` / `_combine_stored_and_pending`; a failed ID refresh must raise `StoreReadError` on every read method; full suite; worker_done only after the post-#290 merge is green.
