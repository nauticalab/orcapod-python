## `src/orcapod/core/sources/spiraldb_table_source.py` — Spiral tables as a workshop source (ITL-926)

### SP4 — A Spiral source read its whole table when built; large tables were unusable
**Status:** in progress
**Severity:** high
**Issue:** ITL-186

`SpiralDBTableSource` inherited `DBTableSource`'s constructor, which read every row and built
provenance for the whole table: about 4.6 GB for the 8,087,906-row `spike_event`. Design and
measurements: `docs/dev/specs/2026-11-12-itl-186-streaming-spiral-source-design.md`.

**Fix:** the source is built from metadata only (schema, key columns, SpiralDB's table id) and
reads rows in batches of at most `batch_rows` rows. The default `source_id` carries SpiralDB's
table id, record IDs come from the hash of the primary key, `where=` / `limit=` / `asof=`
restrict a read without entering identity, and `max_rows` caps a read.

**Still open (not in the first pull request):**

* A pipeline job keeps every row it processes (operator and function nodes, the orchestrator's
  collectors, `Join`'s buffers), about 5 KB per row and node, so a table of millions of rows
  cannot be run through a job (`docs/dev/specs/2026-11-12-itl-186-what-a-job-keeps.md`).
* The row token needs one hasher call per row (183 µs); a vectorized per-row digest needs a
  new hasher entry point.
* No watermark or polling for new rows within a run; no key ranges (pyspiral 0.16.33 cannot
  build a key holding a timestamp); no column selection; no lazy frame with pushdown.
* `PostgreSQLTableSource` and `SQLiteTableSource` still read the whole table when built and
  default their `source_id` to the bare table name.
