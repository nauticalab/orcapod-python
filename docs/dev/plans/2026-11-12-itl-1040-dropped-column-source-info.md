# ITL-1040 Dropped data column keeps its source info: plan

**Goal:** a `Data` without a column carries no source info for it in any view, so sync and
async runs store identical pipeline records.

**Cause:** `Datagram.drop` (and `select`) are not overridden by `Data`; they leave the dropped
column's entry in `Data._source_info`, and `_ensure_source_info_table` builds the source-info
table (used by `as_table(source=True)` and `arrow_schema(source=True)`) from all of
`_source_info`, while `as_dict` / `source_info()` filter to `keys()`. The async
`SelectDataColumns` / `DropDataColumns` call `data.drop`; their sync paths drop `_source_*`
explicitly.

**Steps:**
1. MEASURE FIRST (send to coordinator before any push): with `main`, an async run of a job
   whose pod input is auto-selected writes a pipeline table with `_source_s`; then the fixed
   build appends new rows to that table (Delta and in memory). Record: succeeds / fails /
   writes nulls; what a read returns. Also state with evidence whether any hash, record id or
   stored path depends on the extra column.
2. Failing-first tests: `Data.drop` / `Data.select` + `as_table(source=True)`,
   `arrow_schema(source=True)`; sync == async stored records (auto select, explicit
   select, explicit drop; in-memory and Delta); other Datagram operations (`rename`,
   `update`, `with_columns`, `copy`) and `Tag` system tags checked for the same drift.
3. Fix: `Data` keeps `_source_info` to its own keys: override `drop` and `select` to prune,
   and build the source-info table from the entries of current keys only.
4. design_issues entry, changelog fragment, full suite, PR "Fixes ITL-1040".

No hash, golden, API change expected; stored schema of async-written tables changes (the
extra column disappears from new rows) - the developer decides handling from step 1.
