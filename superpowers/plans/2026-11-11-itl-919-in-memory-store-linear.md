# ITL-919 (part 1): linear PipelineJob runs on InMemoryArrowDatabase — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every per-row `InMemoryArrowDatabase` operation a `PipelineJob` performs cost
O(rows touched), not O(rows already stored), so job run time is linear in rows. Stored rows,
row order, record ids, hashes, paths, schemas, public API and write timing stay identical.

**Architecture:** Two internal changes to `src/orcapod/databases/in_memory_databases.py` only:
(1) the committed table is kept at O(log N) chunks by binary-counter compaction on `flush`;
(2) per-record-key, per-column hash indexes over committed rows (value → sorted row
positions), built lazily, extended on append, dropped when a flush overwrites an existing id.
Lookups (`get_records_with_column_value`, the skip-duplicates check, flush's overwrite check)
use the index and filter only candidate rows.

**Tech Stack:** PyArrow 23, pytest.

**Scope:** In-memory store only (coordinator decision, November 11, 2026). Delta is deferred
pending the developer's decision on write timing (options D1/D2/D3 in the ask); do NOT touch
`delta_lake_databases.py`, `result_cache.py` flush timing, `function_node.py` or `job.py`.

---

## Cause (STEP 1 findings, main c8efc092)

Only function nodes are affected (operator nodes write with one `add_records`). Sync and async
orchestrators both run, per row: `ResultCache.lookup` → `store` (+ auto-flush) →
`add_pipeline_record`. Per row, against the result table and the pipeline table:

1. `ResultCache.lookup` → `get_records_with_column_value(input hash)`: filters the whole table.
2. `add_pipeline_record` → `get_records_with_column_value(base_entry_id)`: filters the whole
   table; then `add_record(skip_duplicates=True)` → `_committed_ids` → `to_pylist` of every id.
3. `ResultCache.store` auto-flushes every row; in-memory `flush` does `is_in` over all committed
   ids + `filter` + `concat_tables`. Every flush adds a chunk, so after N rows the table has N
   chunks and every Arrow op (`filter`, `take`) costs O(N).

Delta (deferred): one MERGE commit per row per table (grows with table), lookups re-open and
read all files (one per row), existing-id cache refreshed after every flush; pending batch also
scanned per lookup. `get_records_with_column_value` on Delta returns only pending matches when a
pending batch exists (ignores committed rows) — repro requested by coordinator.

Timings on main (sync, seconds per row): in-memory 100/400/1000 = 7.2/20.9/64 ms;
Delta 100/200/400 = 78/104/150 ms; standalone pod 0.09 ms flat.

## Files

- Modify: `src/orcapod/databases/in_memory_databases.py`
- Create: `tests/test_databases/test_in_memory_database_scaling.py` (DB-level + job-level
  scaling tests, deterministic: Arrow bytes allocated per row through a counting wrapper)
- Create: `tests/test_databases/test_in_memory_database_lookup_equivalence.py` (index results
  equal a plain filter: multi-column queries, overwrite path, pending + committed, types not
  indexable, str vs bytes values)
- Modify: `CHANGELOG.md` (`[0.3.0] — unreleased`), `DESIGN_ISSUES.md` (one appended entry)

## Task 1: failing scaling tests

- [ ] Counting wrapper: a delegating `ArrowDatabaseProtocol` wrapper whose `at()` returns a
  wrapped view and whose methods add `pa.default_memory_pool().total_bytes_allocated()` deltas
  to a shared counter. Deterministic (bytes requested, not time).
- [ ] DB-level test: add N single-row records one by one, each followed by
  `get_records_with_column_value` on an indexed binary column, `add_record(skip_duplicates=True)`
  and `flush()` (the job's per-row pattern). Arrow bytes per row for the last 100 rows at
  N=1600 must be < 2x those at N=200 (main: ~8x, grows linearly).
- [ ] Job-level test: `PipelineJob` with `InMemoryArrowDatabase` wrapped by the counter, one
  function pod; run 200 rows and 800 rows; store bytes per row ratio < 2 (main ~4).
  Run with both sync and async orchestrators.
- [ ] Run, confirm both fail on main. Commit (test only) as WIP.

## Task 2: compaction on flush

```python
def _compact_tail(table: pa.Table) -> pa.Table:
    """Merge trailing record batches while the earlier one is not larger (binary counter)."""
    batches = [b for b in table.to_batches() if b.num_rows > 0]
    while len(batches) >= 2 and batches[-2].num_rows <= batches[-1].num_rows:
        merged = pa.Table.from_batches(batches[-2:]).combine_chunks().to_batches()
        batches[-2:] = merged
    return pa.Table.from_batches(batches, schema=table.schema)
```

In `flush`: append path `pa.concat_tables([committed, pending])` (keeps today's schema errors)
then `_compact_tail`; overwrite path (rare) filters then `combine_chunks()`.

## Task 3: column indexes

- `_indexes: dict[str, _KeyIndex]` shared across `at()` views (`_shared_indexes`).
- `_KeyIndex(table, columns: dict[str, dict[Any, list[int]]])`; valid only while
  `index.table is self._tables[key]` (rebuilt otherwise).
- Indexable: Arrow binary/large_binary/string/large_string/integer/bool columns queried with a
  value of the matching Python type (`bytes`, `str`, `int` not `bool`, `bool`). Anything else
  falls back to today's filter.
- `get_records_with_column_value`: pick the first indexable pair, `take` sorted candidate
  positions from committed, `filter` the full expression on them; pending part filtered as
  today; concat parts exactly as `_combined_table` did (same inclusion rule, same errors).
- `_filter_existing_records` and flush overwrite detection use membership in the
  `RECORD_ID_COLUMN` index instead of building a full id set.
- Flush append path extends existing column indexes with the pending rows' values at
  `old_len + i`.

## Task 4: verification

- [ ] New tests pass; `uv run pytest tests/test_databases tests/test_pipeline tests/test_core -q`.
- [ ] Differential check vs main (exported copy of main's `src` on `PYTHONPATH`): multi-node
  pipeline with a join (two sources → join → function pod → function pod), in-memory store,
  sync and async: compare every stored table (`list_sources` + `get_all_records`) — same
  schema, same rows (excluding run-dependent columns: result record UUIDs, timestamps, which
  are random/now() on main too), same pipeline record ids, content hashes, entry ids.
- [ ] After timings 100/400/1000/5000 for mem sync/async (+ Delta unchanged numbers).
- [ ] Golden hashes, full suite, ruff, basedpyright.
- [ ] CHANGELOG + DESIGN_ISSUES entry; PR "Fixes"? — No: this PR is part 1 of ITL-919, the
  Delta part follows; use "Part of ITL-919" unless the coordinator says otherwise (ask).

## Notes / progress log

- 2026-11-11: cause established, coordinator approved in-memory fix; Delta pending developer.
- Main 5000-row timings running in background (scratchpad before_mem.txt / before_delta.txt).
- 2026-11-11: in-memory fix implemented (commit "perf(databases): ..."). Scaling tests pass;
  equivalence tests pass on main and on the fix; differential check vs main identical
  (scratchpad diff/ scripts: dump_store.py + compare.py). Lookup uses zero-copy row slices
  (``take`` on a multi-chunk table copies every chunk) and skips the Arrow filter when every
  pair is indexed (acero filter has ~0.35 ms fixed overhead).
- Delta repro of pending-shadow bug reported to coordinator (msg_01ac95ae10d6).
- PR wording: part 1 of ITL-919 (Delta follows) — confirm Fixes vs Part of with coordinator.
