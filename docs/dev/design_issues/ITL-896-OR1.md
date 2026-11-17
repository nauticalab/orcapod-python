## `src/orcapod/core/nodes/operator_node.py` — operator node read-back after an async run

### OR1 — An operator node read back empty after an async job run
**Status:** resolved
**Severity:** high
**Issue:** ITL-896

An operator node keeps its output only in memory; inside a `PipelineJob` every operator runs
with `cache_mode=OFF` and stores nothing. The sync orchestrator calls `OperatorJobNode.execute()`,
which always keeps the materialized output on the node. The async orchestrator calls
`OperatorJobNode.async_execute()`, which kept the rows only in `CacheMode.LOG`; in `OFF` (and with
no database) it forwarded them downstream and dropped them. `iter_data()` and `as_table()` then
fell back to the database, which they read only in `REPLAY`, and returned the zero-row
"not yet run" table without an error. Every operator was affected, on every store.

**Fix:** `async_execute` collects the rows it forwards in every mode except `REPLAY` and keeps
them on the node with `StaticOutputOperatorPod._materialize_to_stream`, as the sync path does,
whatever `materialize_results` is; `LOG` still stores them. Nothing new is stored, and no hash,
path or stored schema changes. A run with no rows clears the previous run's output, so a node
never serves stale rows (CodeRabbit on PR #302). `get_all_records()` still returns `None` for an
operator node in a job, since with the default `cache_mode=OFF` (the only mode a job sets) it
stores nothing. `clear_cache()` still returns the node to its zero-row "not yet run"
state on both paths (a function node instead reloads from its database).
Regressions: `tests/test_pipeline/test_operator_node_async_readback.py`, which compares the sync
and async reads of every operator. Open, pinned by strict xfails there: a zero-row async result
reads back the table built from the Python output schema (ITL-858, ITL-865); that table widens
`int8` to `int64` and changes nullability on both paths (ITL-912); `MergeJoin` after a non-source
input names its system-tag columns differently on the async path (ITL-906).
