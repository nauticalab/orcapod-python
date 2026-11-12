## `src/orcapod/databases/delta_lake_databases.py` — reading back a handle family's own writes

### DW1 — A read through one `at()` handle could miss rows its family had just written
**Status:** resolved
**Severity:** high
**Issue:** ITL-864

Right after a first `PipelineJob` run on a `DeltaTableDatabase`, `node.get_all_records()`
returned 1 of 2 or 3 rows (plain `int -> int` pod, sync and async orchestrators), while
`iter_data` and a fresh handle saw every row. Every `at()` handle shares the root's pending
batches, and any handle's `flush()` writes every pending key, so the result-store handle
flushed the pipeline table's rows. But each handle kept its own loaded-table cache, so the
node's pipeline handle kept reading the table version it had loaded during the run. Every read
method missed the rows a sibling flushed. Separately, the read methods merged pending and stored
rows inconsistently: `get_records_by_ids` ignored pending rows, `get_records_with_column_value`
ignored stored rows whenever a pending batch existed (and returned an empty table instead of
`None`), and a pending row whose record id was already stored was returned as well, although a
flush keeps the stored row and drops the pending one.

**Fix:** the loaded-table cache and the committed snapshots (`_CommittedSnapshot`, which
replaced the existing-ids and cache-dirty dicts in ITL-919) are shared across the `at()`
family exactly as pending state (pending batches and their indexes) is. Every read method returns what a flush would leave: the
stored rows plus the pending rows whose record id is not stored (`_pending_not_stored`), and
`None` when there are none. These dicts are touched only from the thread that drives the
pipeline; the async orchestrator runs only user functions in worker threads. Regression tests:
`tests/test_databases/test_delta_read_your_writes.py` and
`tests/test_pipeline/test_delta_node_records_after_run.py`. Stored ids come from
`_stored_ids`: the snapshot of the loaded table version reads them on first use, through an
id-only scan (a one-column schema); a failed scan raises `StoreReadError` and leaves the
snapshot's ids unset (`None`), so the next read scans again instead of treating every stored
id as unstored and returning duplicates. A handle that loaded the table before it had any
stored ids (a fresh root over an existing table) reads them on its first read with pending
rows (`_pending_not_stored`). After the family's own commit the ids are extended from the
commit itself, with no re-read (ITL-919).

When stored and pending rows are read together, the result has the schema a flush would give:
both parts are large-normalized (`null` kept), then combined with
`pa.concat_tables(promote_options="default")`. A column missing on one side is null-filled, and a
`null`-typed column takes the other side's type. Columns whose types differ (`int64` vs `int32`,
a string vs an int, a timestamp unit, a decimal precision) raise `pyarrow.ArrowTypeError`; nothing
is widened. Before this, a read of stored rows next to pending rows with any schema difference,
including a pending `string` column next to the stored `large_string`, raised `ArrowInvalid`;
`get_all_records` already did so on `main`.
A `get_records_with_column_value` query on a column that only the stored or only the pending
rows have skips the side without it (after a flush that side holds null there, and
`column == value` never matches null), so it returns before a flush what it returns after; a
column neither side has still raises `ArrowInvalid`.

Recorded for the nullability audit (ITL-842) and ITL-890: Delta's merge relaxes nullability (a
non-null pending column into a nullable stored one stays nullable) and evolves struct children
(`struct<p>` stored + `struct<p, q>` pending becomes `struct<p, q>`, stored rows `q = null`). The
read path now mirrors both. Whether Orcapod should allow either at all is for those issues. A
pending all-null `null`-typed column reads back as `null`, but Delta cannot store it: the flush
fails and the batch stays pending (unchanged, pinned by
`test_pending_null_typed_column_reads_back_but_does_not_flush`).

Still open: a handle does not see writes made by another process, or by a separate
`DeltaTableDatabase` root on the same path, until its own next commit to that table reloads it;
this is unchanged. When it does reload (another writer committed in between), the stored ids
are read again with the id-only scan, and the stored rows on the next lookup, each once per
version (D3, now bounded by ITL-919's snapshot).
