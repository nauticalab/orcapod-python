# ITL-919 (part 2): batched Delta commits (D2) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `PipelineJob` on a `DeltaTableDatabase` runs in time linear in its rows, close to
the in-memory store, by committing a function node's writes in batches instead of once per
row, and by serving the per-row lookups from indexed in-memory state instead of re-reading
the Delta table.

**Architecture:** (1) Function nodes running inside `PipelineJob.run` stop committing per row
and commit every `commit_every_rows` rows (a new `run()` keyword, default 100), at node end,
before raising on any failure, and at job end. (2) `DeltaTableDatabase` lookups read an
indexed snapshot of the committed rows (one per cached table handle, extended with only the
new data files after the family's own commit) plus the pending batch, which is bounded by the
commit size. (3) A commit appends when every pending id is new against a fresh table version,
and MERGEs only otherwise.

**Tech Stack:** deltalake 1.x, PyArrow 23, pytest.

**Branch / stack:** `eywalker/itl-919-delta-batched-commits`, created at #295's head
(14c1546b = main ebf2511b + #293 + #295), with #304's branch merged in (f84217b2) for its
index helpers and scaling tests. PR against `main`, stacked note first.

---

## Developer decisions (November 11, 2026) — these override the reviewed plan below

1. **Commit trigger is row count only.** `PipelineJob.run(..., commit_every_rows=100)`
   (public; coordinator approved the `run()` keyword placement, documented next to
   `materialize_results`; validated as an `int >= 1`, `bool` rejected, else `ValueError`).
   No time-based commit (ITL-925), no threads or timers. Commits also at node end, before
   raising on any failure, and at job end.
2. **Scope:** only function nodes running inside `PipelineJob.run` batch their commits.
   `FunctionJobNode.run()` / `execute()` called directly, and a `CachedFunctionPod` used
   directly, keep one commit per row (the node's attribute is `None` outside a job run).
3. **Commit failure:** `StoreWriteError(pipeline_db.base_path, node_label=..., flush=True)`
   raised at the commit point, naming the node (no row/tag), chained from the store's error.
   The failed batch stays pending (`_flush_by_key` restores it; keep), the job's best-effort
   flush retries once, earlier batches are kept, the run fails (PR #287 semantics). A failure
   writing one row still names table, node and row tag (unchanged). The node wraps; the store
   receives no labels (no message-only parameters). `StoreReadError` passes through, with the
   node label added by `add_context`.
4. **Amendments:** (a) on any failure the node flushes its ephemeral store best-effort
   itself, never replacing the run error; (b) the crash guarantee states the
   results-then-pipeline commit window and the zero-recomputation rerun recovery, with a
   subprocess test interrupting between the two commits; (c) append instead of MERGE when
   **every** pending id is new, MERGE otherwise; "all ids new" is decided against a FRESH
   table version (loaded at flush time), never the cached snapshot; a differential run must
   show identical tables.
5. **Also fix:** `side_effects.py` prior-completion `get_record_by_id` lookup raises a store
   error without the row's tag (CodeRabbit on #290) — the caller adds node and tag context;
   and `compute_base_entry_id` computed 3x per row in `function_node.py` — compute once per
   row and pass it down (no hash or id change).
6. **ITL-923:** keep items 1, 2, 4 fixed (#293); fix what falls inside this change; report
   precisely what remains (item 3 silent MERGE drop; async ephemeral re-run).

## Design details (worker, after reading the code)

### Commit order: results before pipeline records

Views of one store share one set of pending batches, and `flush()` commits every pending
key of the family in the order keys were first written since their last commit. That order
does not guarantee result table before pipeline table (a batch can start with a cache-hit
row, which writes only a pipeline record; in the async path the pod stores row k+1's result
after row k's pipeline record). The node therefore commits explicitly in this order:

1. ephemeral store `flush()` (separate database), when the node has one;
2. its result table alone, through `DeltaTableDatabase.flush_batch(record_path)` when the
   result database has it (only the Delta store commits durably table by table; the
   in-memory store has no crash window). Duck-typed `getattr(db, "flush_batch", None)` on
   the result view (the test `CountingDatabase` forwards it);
3. `pipeline_db.flush()` (pipeline records and anything else pending in the family).

A crash between 2 and 3 leaves results stored without pipeline records; the rerun finds them
in the result cache (no recomputation) and writes the pipeline records. A *failed* flush at
step 3 continues past failed keys (#287 `flush` semantics, unchanged): see "Remaining".

### Node (`FunctionJobNode`)

- `_commit_every_rows: int | None = None` set by `PipelineJob.run` on every
  `FunctionJobNode` it executes, reset to `None` in a `finally`. While set, the node's
  `CachedFunctionPod`s run with `auto_flush=False` (new `CachedFunctionPod.set_auto_flush`,
  mirroring `CachedDataFunction.set_auto_flush`), restored afterwards.
- Counter `_rows_since_commit`: incremented after each row that went through
  `_process_data_internal` successfully (sync) / each row recorded in `record_and_forward`
  (async). At `>= commit_every_rows`: `_commit_pending_writes()`.
- Node end (sync and async DB path): `_commit_pending_writes()` after the last row, before
  `on_node_end` (only when batching; outside a job nothing changes).
- `_commit_pending_writes()`: steps 1-3 above; `StoreReadError` gets
  `add_context(node_label=...)` and propagates; any other error becomes
  `StoreWriteError(self._pipeline_database.base_path, node_label=self.label, flush=True)`
  from it. The counter resets only after success.
- On any failure leaving `execute` / `async_execute` while batching: flush the ephemeral
  store best-effort (moved helper `flush_best_effort(run_error, *dbs)` from
  `pipeline/job.py` to `orcapod/databases/utils.py` so core can use it; job.py imports it).
- `compute_base_entry_id` once per row: `_process_data_internal` /
  `_async_process_data_internal` / `add_pipeline_record` accept an optional precomputed
  `base_entry_id`; `execute` passes its value; `record_and_forward` reuses it.

### Store (`DeltaTableDatabase`)

- Shared helpers moved from `in_memory_databases.py` (#304) to
  `src/orcapod/databases/_row_index.py`: `_KeyIndex`, `_is_indexed_lookup`, `_compact_tail`,
  plus `build_column_index(table, column)` and `extend_column_index(index, table, offset)`.
- `_CommittedSnapshot(handle, version, table, files, index: _KeyIndex)` per record key, in
  a family-shared dict `_snapshots` (passed through `at()` like the other shared caches).
  Valid while `snapshot.handle is self._delta_table_cache[key]`. Built lazily from the cached
  handle with `delta_to_pyarrow_dataset(handle, as_large_types=True).to_table()` +
  `normalize_table_view_types` (exactly today's read). A read failure raises `StoreReadError`
  (`_read_failed`); a bad filter raises as the caller's error (split kept).
- `_existing_ids_cache` / `_cache_dirty` / `_refresh_existing_ids_cache` are replaced by
  the snapshot's record-id index (no full-table reload per flush).
- Lookups (`get_records_with_column_value`, `get_record_by_id`, `get_records_by_ids`):
  committed rows from the snapshot (index when `_is_indexed_lookup`, else filter of the
  in-memory snapshot table) + `_pending_not_stored` filtered, combined by #293's
  `_combine_stored_and_pending` (unchanged schema rules). Null-id pending rows kept.
- `_flush_by_key`: load a fresh handle (as today). Existing table: get the snapshot for the
  fresh version (reuse when the cached snapshot's version equals the fresh version, else
  build one from the fresh handle). If no pending id is in it (nulls count as new): append
  with `write_deltalake(mode="append", schema_mode="merge" if allow_schema_evolution else
  None)`; else MERGE as today. After the commit, reload the handle; if its version is the
  fresh version + 1, no data file was removed and the dataset schema is unchanged, extend the
  snapshot with only the new fragments (read through `delta_to_pyarrow_dataset`), else drop
  it (rebuilt lazily). New table: `overwrite` write as today, snapshot built lazily.
  Failure: pending restored (kept), snapshot left for the old handle.
- Pending batch: bounded by `commit_every_rows` in a job (not indexed: YAGNI); its `concat`
  per row stays.

### Remaining (to report, not fixed here)

- ITL-923 item 3: a pending row (written with `skip_duplicates=False`) whose id is already
  stored is still dropped by the commit (MERGE `when_not_matched_insert_all`, and now the
  append path decides the same way); the in-memory store instead overwrites. Unifying the
  semantics is a behaviour change: out of scope.
- `flush()` keeps attempting later keys after a failed one (#287), so a failed result-table
  commit followed by a successful pipeline commit can store pipeline records whose results
  are still pending; the job's best-effort retry usually commits them.
- Concurrent writers: the append decision uses the version loaded at flush time; another
  writer committing between that load and the append could create a duplicate id (MERGE
  would not). Single-writer per store is assumed (as for the existing caches).

## Tests (TDD order; each must fail first where it encodes new behaviour)

- [ ] T1 **Delta scaling, sync + async:** `test_job_store_scaling.py` parametrized over the
  store (in-memory, Delta); Arrow bytes per row at 150 vs 600 rows flat (`< 2x`).
- [ ] T2 **Commit counts:** 250 rows, `commit_every_rows=100`: result and pipeline tables
  each have exactly 3 versions (`DeltaTable(path).history()`), sync and async; history
  operations are `WRITE`/`CREATE`... not `MERGE` (append path).
- [ ] T3 **Option validation:** `commit_every_rows=0`, `-1`, `True`, `1.5` raise
  `ValueError`/`TypeError` before anything runs.
- [ ] T4 **Outside a job unchanged:** `job.nodes["dbl"].run()` after compile (no `job.run`)
  and a `CachedFunctionPod` used directly on a Delta store: one result-table commit per row.
- [ ] T5 **Hard crash mid-run** (subprocess, `os._exit(1)` from the function at row 234 of
  250, `commit_every_rows=100`): reopen: exactly 200 result rows and 200 pipeline records;
  rerun without the crash: the function runs exactly 50 times (side file); final tables equal
  an uninterrupted run's on stable columns.
- [ ] T6 **Crash between result and pipeline commits** (subprocess; a store wrapper around
  `DeltaTableDatabase` whose `flush` on the pipeline view calls `os._exit` on its first
  call after the result table committed): reopen: results stored, pipeline records absent;
  rerun: zero function calls, pipeline records written, final tables equal an uninterrupted
  run's. Plus a cache-hit-first batch case.
- [ ] T7 **Commit failure, sync + async, both error policies:** a store wrapper whose
  `flush` raises on its 2nd call: `StoreWriteError`, `flush=True`, `node_label == "dbl"`,
  `tag is None`; the first 100 rows committed and readable from a fresh store.
- [ ] T8 **Ephemeral store flushed on failure:** a job with an `is_result_ephemeral=True`
  node whose later node raises under `fail_fast`: the ephemeral store's results are committed
  (fresh view), the run's own error is the one raised.
- [ ] T9 **Append vs MERGE:** fresh store commits use append; a store another writer
  committed to between batches still stores no duplicate id and the tables equal.
- [ ] T10 **Lookup equivalence, Delta:** #304's equivalence suite parametrized over
  `DeltaTableDatabase` (committed + pending, many commits, scoped views sharing snapshots,
  unindexable lookups, a column missing on one side) vs a filter over `get_all_records`.
- [ ] T11 **ITL-923:** existing #293 regressions stay green; ephemeral re-run on Delta (sync
  and async): 10 pipeline records, node reads 5 rows — fix if inside this change, else report.
- [ ] T12 **side_effects:** an unreadable completion table raises `StoreReadError` with
  `node_label` and `tag` set.
- [ ] T13 **compute_base_entry_id:** called once per computed row (spy on the method in a
  job run) — the one test that touches a non-public entry point, commented.
- [ ] T14 #287, #290, #293, #295 tests unchanged and passing; golden hashes unchanged.
- [ ] T15 **Differential vs main** (scratch script, not a test): joined multi-node pipeline
  (two sources, `Join`, two pods, 1,200 rows) on Delta, sync and async; every table dumped:
  identical paths, schemas, row counts, record ids, entry ids, content hashes.
- [ ] T16 **Timings:** 100 / 400 / 1,000 rows (5,000 after only), Delta sync + async,
  before (main numbers from the brief + this base) and after, in the PR body. No 5,000-row
  baseline on main (hours): extrapolate and say so.

## Tasks

- [x] 0. Rename branch, merge #304 (f84217b2), ask placement (approved: `run()` kwarg).
- [x] 1. Save this plan; WIP commit + push.
- [x] 2. `_row_index.py`: move helpers out of `in_memory_databases.py` (no behaviour change;
  #304 tests green).
- [x] 3. T10 Delta equivalence tests (should pass before and after: guard for the refactor).
- [x] 4. Store snapshot + indexed lookups (T1 store-side part fails first: Delta bytes per
  row grow); replace id cache; append path (T9).
- [x] 5. Node batching + `run(commit_every_rows=)` (T2, T3, T4 first); commit order;
  failure handling (T7, T8); crash tests (T5, T6).
- [x] 6. side_effects context (T12) and `compute_base_entry_id` reuse (T13).
- [x] 7. ITL-923 ephemeral re-run check (T11).
- [x] 8. Docs: `docs/concepts/function-pods.md` (When the store cannot be written: commit
  cadence, crash guarantee, commit failure), pipelines page (`commit_every_rows`),
  `CHANGELOG.md` [0.3.0], `DESIGN_ISSUES.md` PQ1 update.
- [ ] 9. Differential (T15), timings (T16), full suite on 3.13 / 3.12 / 3.11 one at a time,
  ruff, basedpyright.
- [ ] 10. Merge origin/main, push, PR (stack note, Fixes ITL-919, ITL-923 per result),
  `orca linear attach`, `orca worktree set --pr`, CodeRabbit rounds, worker_done.

## Notes / progress log

- 2026-11-11: plan copied from the part-1 worker's scratchpad and updated with the
  developer's decisions; placement of `commit_every_rows` approved (run() keyword).
- Store: snapshot = stored ids (id-only scan through a one-column schema, so logical list
  types cannot break it) + stored rows (indexed, `as_large_types`), extended after own
  commits from new data files; pending batch compacted + indexed (the unindexed pending
  batch cost ~185 KB per lookup). Append only when all ids are new AND the batch has the
  stored schema as the writer converts it (`_stores_as_is`), else MERGE.
- Coordinator approvals: three #293 tests changed (snapshot answers warm reads; own commit
  tells ids), with boundary tests added (`test_delta_committed_snapshot.py`); four #290
  tests changed to commit-point semantics + a `commit_every_rows=1` test; both lists go in
  the PR body ("Tests of #293 changed", "Tests of #290 changed").
- ITL-930 (get_all_records short after run): fixed by #293 in the stack; regression test
  added. ITL-932 (async run hangs when a node fails with > buffer rows in flight):
  pre-existing, fixed in its own commit (channels.close never blocks). ITL-923 async
  ephemeral rerun: fixed (route_inputs treated cached EmptyData as a hit).
- Found, not fixed: AsyncPipelineOrchestrator ignores error_policy (documented as ITL-879).
- Timings after (ms/row, Delta, 100/400/1000/5000): sync 2.59/1.28/1.13/1.18, async
  2.36/1.21/1.09/1.16. Main (part-1 worker): sync 80/198/509, async 69/164/359.

---

# Appendix: the reviewed plan (part-1 worker, with three amendments), for reference

The original plan used `ROWS_PER_COMMIT = 500`, a "drop known ids then append the rest"
commit, and per-key flush order; the decisions above override those. Its user-facing table
otherwise still holds with 100 in place of 500:

| | Today (`main`) | D2 |
|---|---|---|
| When a job node's rows are committed | after every row (one Delta commit per row per table) | every `commit_every_rows` (100) rows, at node end, before a failed run raises, at job end |
| Hard crash mid-node | loses at most the row in flight | loses at most the last 99 rows (and their pipeline records) of the running node; recomputed on rerun. A crash between a commit's result-table and pipeline-table writes leaves those results stored without pipeline records: joined reads miss them until a rerun, which finds them in the result cache and writes the pipeline records with no recomputation |
| A row whose record cannot be built/added | `StoreWriteError` naming table, node, row tag | unchanged |
| A commit that fails | `StoreWriteError(flush=True)` naming the store (raised at that row) | `StoreWriteError(flush=True)` naming the store and the node (no row); raised at the commit point |
| Rows committed before a failure | kept | kept (every earlier batch); the failed batch stays pending and is retried once by the run's best-effort flush |
| Files per table | one parquet file per row | one per commit |
| Outside a job | flush per row | unchanged |
| Stored rows, ids, hashes, paths, schemas | — | unchanged (differential check) |

Per-row reads on `main` with the per-row flush disabled: 7.5 / 12.6 / 34 ms per row at
100 / 400 / 1000 rows — still growing — because `get_records_with_column_value` reads the
whole Delta table per call (no predicate pushdown, ENG-601), the pending batch is scanned per
lookup, and `_refresh_existing_ids_cache` re-reads the whole table after every commit. Hence
the snapshot design above.
