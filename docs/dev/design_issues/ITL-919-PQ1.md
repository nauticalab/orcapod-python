## `src/orcapod/databases/` — PipelineJob run time quadratic in rows

### PQ1 — Every row a function node processes scans the rows already stored
**Status:** resolved
**Severity:** high
**Issue:** ITL-919

A function node in a `PipelineJob` does, per row: `ResultCache.lookup` (lookup by input data
hash in the result table), `ResultCache.store` with a flush of the whole database (both
tables), and `add_pipeline_record` (lookup by base entry id for the recomputation index,
then `add_record(skip_duplicates=True)`). Sync and async orchestrators share that path;
operator nodes write with one `add_records` and are not affected. Measured on `c8efc092`,
in-memory store, sync: 7.2 / 20.9 / 64 ms per row at 100 / 400 / 1,000 rows (standalone
pod: 0.09 ms).

- `InMemoryArrowDatabase`: each lookup filtered the whole table, the duplicate check built
  the set of every committed id, and each flush ran `is_in` over all ids, filtered and
  concatenated, adding one chunk per row, so every later Arrow operation cost O(rows).
  **Fix:** lazily built hash indexes over committed rows (binary and string columns, record
  ids), extended on append and rebuilt only after a flush overwrites an id; flush keeps the
  committed table at O(log N) record batches by binary-counter compaction. Stored rows, row
  order, ids and schemas are unchanged (differential check against `main` on a joined
  multi-node pipeline, both orchestrators). Store work per row is now independent of rows
  stored (tests count Arrow bytes allocated per row).
- `DeltaTableDatabase`: one Delta MERGE commit per row per table (cost grows with the
  table and leaves one file per row), lookups re-opened and read every file, the
  existing-id cache was re-read after every flush, and the pending batch was scanned per
  lookup (main: 80 / 198 / 509 ms per row at 100 / 400 / 1,000 rows, sync). **Fix:** function
  nodes inside `PipelineJob.run` commit every `commit_every_rows` rows (default 100), at node
  end, best-effort on failure and at job end, results before pipeline records; a node used
  on its own still commits per row. The store keeps, per loaded table version, the stored
  ids and rows in memory (indexed), extended after its own commits from the new data files
  only; the pending batch is compacted and indexed; a commit appends when every pending id is
  new in the version loaded for that commit, and MERGEs otherwise. Stored rows, ids, hashes,
  paths and schemas are unchanged (differential against `main`). Store failures that only a
  commit reveals now surface at the next commit point and name no row (developer decision,
  November 11, 2026). Found on the way: an async run hung on any node failure with more rows
  in flight than the channel buffers (ITL-932, fixed: closing a channel never blocks), and an
  async rerun of an ephemeral-result node skipped its rows (ITL-923, fixed). Remaining
  (ITL-923 item 3): a pending row written with `skip_duplicates=False` whose id is already
  stored is still dropped by the commit (Delta keeps the stored row; the in-memory store
  overwrites it). Memory: a database object keeps the committed rows of each table it looks rows
  up in for as long as it lives, all of them at once (main read one whole table per lookup
  and released it), so memory is cumulative over the tables: measured for an `int -> int`
  pod, about 820 bytes per stored row for the result table and 746 for the pipeline-record
  table, so about 1.6 GB per function node at 1M rows. Keeping only the lookup columns would
  need a read per matched row. A follow-up to bound it is for the coordinator to file. The store's own
  size-triggered flush (`batch_size`) now commits only the full table, and `PipelineJob.run`
  refuses `commit_every_rows >= batch_size`, so the results-first order holds.
