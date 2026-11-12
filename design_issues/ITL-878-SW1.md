## `src/orcapod/pipeline/job.py`, orchestrators, node write paths — store write failures

### SW1 — A failed store write did not fail `PipelineJob.run()`
**Status:** resolved
**Severity:** critical
**Issue:** ITL-878

A node's write to the store ran inside the per-row handler meant for function failures:
`FunctionNode.execute` (sync) and `_FunctionPodBase.async_execute.process_one` (async) wrap
compute *and* the result-table and pipeline-table writes in one `except Exception`. With the
default `error_policy="continue"` a failed write was skipped like a crashed function: the sync
path logged a WARNING, the async path logged only at DEBUG, `job.run()` returned normally and
the node read back fewer rows (none when every write failed, e.g. a logical-type output on
`SQLiteConnector`, ITL-877). An operator LOG write failure did raise, but before `job.run()`'s
final flush, so the upstream function node's pending pipeline rows were never committed while
its result rows were; async callers got an `ExceptionGroup`.

**Fix:** Contract, for now (developer, November 10, 2026): a failed store write always fails
the run, whatever `error_policy` is. Every node write site (`ResultCache.store`,
`FunctionNode.add_pipeline_record`, `OperatorNode._store_output_stream`, the side-effect
invocation log) raises `orcapod.errors.StoreWriteError`, chained from the backend error and
naming the node, the row's tag and the table. The per-row handlers re-raise it, so the run
stops at the first failed write; `error_policy` still decides only about rows whose function
raised. `AsyncPipelineOrchestrator` unwraps it from the `TaskGroup` exception group, so both
orchestrators raise the same error. `PipelineJob.run()` flushes the writes that succeeded
before re-raising (a flush failure there is logged, not raised over the original), and wraps a
failure of its final flush. Nothing stored is rewritten; no hash, path or schema change. An
opt-in mode that tolerates store failures is a possible later option. A fail-loud post-run
hook or an observer callback that raises while the store error propagates is logged and noted
on it (`orcapod.utils.store_errors`), not raised in its place, so a node function that runs
another cached pod cannot turn that pod's store failure into a skipped row. When concurrent
node tasks fail together, the async orchestrator raises the store error of the node first in
topological order, notes the other failures on it and logs other store errors at WARNING.
A failed flush (the result cache's per-row auto-flush or the job's final flush) raises
`StoreWriteError(flush=True)` naming the store and no row: views of one store share their
pending writes, so the failing batch may be an earlier row's or another table's.
The rule (developer, November 11, 2026): store failures fail the run; a function output that
cannot be stored (a naive `datetime`, a value not matching the declared type, rejected while
`ResultCache.store` prepares the record, before any write) is a row failure governed by
`error_policy` exactly like a function that raised, not a `StoreWriteError`: the value is
specific to that row, the store is healthy, and every re-run would fail at the same row. The
async orchestrator logs row failures at DEBUG and ignores `fail_fast` (ITL-879), unchanged here.
Regressions: `tests/test_pipeline/test_store_write_failure.py`. Related, not fixed here: the async
orchestrator ignores `error_policy` (ITL-879); connector and Delta `flush` drop a batch whose
write fails (ITL-880).
