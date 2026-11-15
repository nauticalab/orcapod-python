## `src/orcapod/databases/delta_lake_databases.py` — read failures

### DR1 — A Delta table that could not be read was reported as having no rows
**Status:** resolved
**Severity:** critical
**Issue:** ITL-884

`DeltaTableDatabase.get_all_records` logged a read error at WARNING and returned `None`, and
`get_records_by_ids` returned `None` on any error. Both conflated "the table does not exist"
with "the table exists but cannot be read". Callers treat `None` as no rows. A function node
recomputed with a "deleted externally" warning, its per-row lookups failed inside the
`error_policy="continue"` handler, and the run finished without raising, so stored results
looked dropped. `_flush_by_key` also treated a `DeltaProtocolError` on load as "no table" and
wrote the batch with `mode="overwrite"`. The case that exposed it: an append whose column type
differs from the stored table, which Delta's merge either casts silently to the stored type or,
with an extension type involved, accepts and then cannot read. Other backends were not
affected: the in-memory and connector databases (SQLite, PostgreSQL, SpiralDB) return `None`
only for a table that does not exist and let read errors propagate.

**Fix:** only `TableNotFoundError` means absent. Every other load or read error raises
`orcapod.errors.OrcapodDatabaseReadError`, which names the table and chains the cause. `FunctionNode.execute`, the
async `FunctionPod` row handler and the async orchestrator let it fail the run whatever the
`error_policy`. Before merging into an existing table, `_flush_by_key` compares the batch with
the stored Delta schema through Delta's own type mapping (extension types by storage,
large-normalized, nullability ignored). On a difference it raises `InputValidationError` naming the
table, column and both types, and nothing is written; struct fields are compared by name at
every depth, so a new nested field is schema evolution, not a difference. The batch's types
are converted with the Delta writer's own conversion (`deltalake.writer._conversion`, the one
`write_deltalake` and `merge` use) before the comparison, and the stored schema is read without
field metadata, so a type the writer stores differently (timestamps in microseconds, unsigned as
signed) or a logical type whose Delta storage differs is never refused; the differential guard
`tests/test_databases/test_delta_append_types.py` appends an identical schema for every Arrow and
logical type Delta can store. `flush()` raises a `OrcapodDatabaseReadError` or
that `InputValidationError` as itself, not wrapped in `RuntimeError`, with every other failed
batch noted on it, so a read failure in a per-row result-cache flush still fails the run. The
async orchestrator selects the `OrcapodDatabaseReadError` from the `TaskGroup`'s exception group with a
plain `except`, noting the other failures, so a concurrent unrelated failure cannot turn it into
an `ExceptionGroup`. With ITL-878 (SW1) both errors derive from `orcapod.errors.OrcapodDatabaseError`
and share one path: the per-row handlers, `orcapod.utils.database_errors` and both orchestrators
handle `OrcapodDatabaseError`. A write that fails because the stored table cannot be read raises
`OrcapodDatabaseReadError`, not `OrcapodDatabaseWriteError`; a refused append is a `OrcapodDatabaseWriteError` caused by the
`InputValidationError`. The orchestrators label a database error with the node it escaped from,
and the async orchestrator ranks read and write errors alike by node order. Regressions:
`tests/test_databases/test_delta_read_failures.py`,
`tests/test_pipeline/test_store_read_failure.py` and
`tests/test_pipeline/test_store_errors_together.py`. Open: whether a nullability-only change
should be refused (Delta's merge relaxes a non-null nested field to nullable) belongs to the
nullability audit, ITL-842.
