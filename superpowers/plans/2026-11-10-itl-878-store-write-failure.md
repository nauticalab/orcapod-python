# ITL-878: a failed store write fails the run — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** When any node's write to the store fails, `PipelineJob.run()` (and the orchestrators
under it) raises a `StoreWriteError` naming the node and table, on both orchestrators and
independent of `error_policy`, instead of returning normally with the rows silently missing.

**Status:** Option A proposed to the coordinator on 2026-11-10; the coordinator is taking it to
the developer (new public exception + documented behaviour change). Tasks 1 (failing tests)
may proceed now; Tasks 2+ wait for approval.

**Architecture:** Every node-owned store write site converts a backend exception into
`orcapod.errors.StoreWriteError` (chained `from` the backend error). The per-row
"skip failed rows" handlers (sync `FunctionNode.execute`, async
`_FunctionPodBase.async_execute.process_one`, side-effect delivery with `on_error="log"`)
re-raise `StoreWriteError` ahead of their generic `except Exception`, so `error_policy` keeps
meaning "skip rows whose *function* failed". The async orchestrator unwraps the
`TaskGroup` `ExceptionGroup` so async callers get the same `StoreWriteError` as sync callers.
`PipelineJob.run()` flushes pending writes best-effort before re-raising any run failure, so
rows that did write are committed and do not look dropped.

**Tech Stack:** Python 3.11+, PyArrow, asyncio TaskGroup, pytest.

---

## Investigation summary (Step 1, reproduced on main c438d4d1)

- Sync swallow: `core/nodes/function_node.py` `FunctionNode.execute` per-row
  `except Exception` (error_policy "continue") wraps `_process_data_internal`, which does
  compute AND both writes (result table via `ResultCache.store`, pipeline table via
  `add_pipeline_record`). WARNING log per row, run returns normally.
- Async swallow: `core/function_pod.py` `_FunctionPodBase.async_execute.process_one` catches
  every per-row exception, logs at DEBUG (invisible), drops the row.
- Async pipeline-table write (`record_and_forward`) and operator LOG writes already raise, but
  async callers get an `ExceptionGroup`.
- Operator-LOG failure aborts the run before `job.run`'s final flush, so pending pipeline rows
  of upstream function nodes are never committed while their result rows are: the node reads
  back fewer rows than were stored.
- Read failures raise on both orchestrators (not swallowed). Observer status/log writes are
  deliberately best-effort and stay so.
- Re-run on a healthy store recovers in every case (missing rows recomputed; orphan results
  hit the result cache).

## Contract (Option A)

1. A failed write of a node's results or records raises `StoreWriteError` and stops the run at
   the first failure, regardless of `error_policy`, on both orchestrators.
2. `str(err)` names the node label and the table path (`"/".join(record_path)`), says rows
   stored before the failure are kept and that re-running computes only what is missing.
   `err.node_label`, `err.record_path` are attributes; `err.__cause__` is the backend error.
3. Before re-raising, `PipelineJob.run()` flushes pending writes best-effort; a flush failure
   during that is logged and does not mask the original error. Nothing already stored is
   rewritten; no hash, path or stored-schema change.
4. A failure of `PipelineJob.run()`'s final flush raises `StoreWriteError` (no node label).
5. `error_policy="continue"` still skips rows whose function raised (unchanged).

## Files

- Modify: `src/orcapod/errors.py` — add `StoreWriteError(RuntimeError)`.
- Modify: `src/orcapod/core/result_cache.py` — wrap `add_record` + auto-flush in `store`.
- Modify: `src/orcapod/core/nodes/function_node.py` — wrap `add_pipeline_record`'s write;
  re-raise `StoreWriteError` in `execute`; tag node label in `async_execute`.
- Modify: `src/orcapod/core/function_pod.py` — `process_one` re-raises `StoreWriteError`.
- Modify: `src/orcapod/core/nodes/operator_node.py` — wrap `_store_output_stream`'s write.
- Modify: `src/orcapod/side_effects.py` — wrap `_write_invocation_row`; re-raise before the
  `on_error="log"` handler.
- Modify: `src/orcapod/pipeline/async_orchestrator.py` — unwrap `StoreWriteError` from the
  `ExceptionGroup`.
- Modify: `src/orcapod/pipeline/job.py` — best-effort flush on failure; wrap final flush.
- Create: `tests/test_pipeline/test_store_write_failure.py`.
- Modify: `docs/concepts/function-pods.md` (PipelineJob section), `README.md` execution
  walkthrough, `DESIGN_ISSUES.md` (one entry appended at the end).

## Task 1: Failing regression tests (public interface)

**Files:** Create `tests/test_pipeline/test_store_write_failure.py`.

Test double `FaultyStore`: wraps an `InMemoryArrowDatabase`; `at()` returns a wrapper sharing
one `Faults` config; fails `add_record`/`add_records` whose full path contains a marker
(`"rdb_v1"` result table, `"pdb_v1"` pipeline table, operator class name), optionally after N
successful matching writes, or fails `flush`. Records the failing relative record path so the
test can assert the message names it. `__getattr__` delegates everything else.

All tests parametrized over `SyncPipelineOrchestrator` / `AsyncPipelineOrchestrator`. Until
the exception class is approved, assert with `pytest.raises(Exception)` plus
`not isinstance(err, BaseExceptionGroup)`, node label in `str(err)` and failing path in
`str(err)`; tighten to `StoreWriteError` in Task 2.

- result-table write fails on the first row → run raises, names node + table.
- result-table write fails from the 2nd row → raises; a fresh job over the healthy store
  recomputes only the missing rows (function call counter) and returns all rows.
- pipeline-table write fails → raises; fresh healthy re-run makes zero function calls.
- store primed with 2 rows, 3rd (new) row's write fails → raises; the 2 primed rows are still
  read back by a fresh job (zero calls for them).
- `error_policy="fail_fast"` and `"continue"` both raise for a store failure.
- operator node with `cache_mode=LOG` whose write fails → raises naming the operator node;
  upstream function node's pipeline rows are committed (read through a fresh job's
  `nodes[label].get_all_records()` before running it).
- flush failure (`Faults(on_flush=True)`) → raises.
- SQLite trigger: function pod returning `pathlib.Path` on
  `ConnectorArrowDatabase(SQLiteConnector(tmp_path / "x.db"))` → raises naming the node.
- Guard (should pass before and after): a function that raises for one row with
  `error_policy="continue"` still returns normally and stores the other rows.

- [ ] Write the tests; run `uv run pytest tests/test_pipeline/test_store_write_failure.py -v`;
      confirm the store-failure tests FAIL (run returns normally / ExceptionGroup) and the
      guard passes. Commit `test(pipeline): failing regression tests for swallowed store write
      failures (ITL-878)`.

## Task 2: `StoreWriteError` (after approval)

```python
class StoreWriteError(RuntimeError):
    """Raised when a node's results or records could not be written to the store. ..."""

    def __init__(self, record_path: tuple[str, ...], node_label: str | None = None) -> None:
        self.record_path = tuple(record_path)
        self.node_label = node_label
        super().__init__(record_path, node_label)

    def __str__(self) -> str:
        where = f"node {self.node_label!r}" if self.node_label else "the pipeline"
        cause = self.__cause__
        reason = f": {type(cause).__name__}: {cause}" if cause is not None else ""
        return (
            f"Could not write to the store for {where} (table "
            f"{'/'.join(self.record_path)!r}){reason}. Rows stored before this failure are "
            "kept; re-running computes only the rows that are missing."
        )
```

`node_label` is filled in by the node that owns the write when the write site does not know
it (`ResultCache`): `if exc.node_label is None: exc.node_label = self.label`.
Tighten Task 1 assertions to `pytest.raises(StoreWriteError)`.

## Task 3: Write sites raise `StoreWriteError`

- `ResultCache.store`: `try: add_record(...); if auto_flush: flush()` /
  `except StoreWriteError: raise` / `except Exception as exc: raise
  StoreWriteError(self._versioned_record_path) from exc`.
- `FunctionNode.add_pipeline_record`: same around `self._pipeline_database.add_record`, with
  `node_label=self.label`, path `self._versioned_pipeline_path`.
- `OperatorNode._store_output_stream`: same around `add_records`, path
  `self.node_identity_path`.
- `side_effects._write_invocation_row`: same, path `table_path` (label filled by caller).

## Task 4: Per-row handlers re-raise

- `FunctionNode.execute`: before `except Exception`, add
  `except StoreWriteError as exc:` → set label, `ctx_obs.on_data_crash(...)`,
  `ctx_obs.on_node_end(...)`, `raise`.
- `_FunctionPodBase.async_execute.process_one`: `except StoreWriteError as exc:`
  → `obs.on_data_crash(...)`; `raise`. (Only this; no error_policy threading or log-level
  change — those are the separately filed async error_policy issue.)
- `FunctionNode.async_execute`: wrap the DB-path `TaskGroup` with `except* StoreWriteError`
  setting `node_label` on each leaf whose label is `None`, then re-raise the group.
- `side_effects` delivery: `except StoreWriteError: raise` before the `on_error` handler.

## Task 5: Orchestrator and job

- `AsyncPipelineOrchestrator._run_async`: `except* StoreWriteError as eg: raise
  _first_leaf(eg)` where `_first_leaf` walks nested groups to the first `StoreWriteError`.
- `PipelineJob.run()`: wrap `effective_orchestrator.run(...)` in `try/except Exception:`
  → `_flush_best_effort(pipeline_db, result_db)` (logs flush errors with `logger.exception`)
  → `raise`. Final flush: `except Exception as exc: raise
  StoreWriteError(pipeline_db.base_path) from exc`.

- [ ] Run new tests (pass), then `uv run pytest tests/` (full), ruff check/format,
      basedpyright exit code. Commit `fix(pipeline): a failed store write fails the run
      (ITL-878)`.

## Task 6: Docs and design log

- `docs/concepts/function-pods.md` PipelineJob section: subsection "When the store cannot be
  written" stating the contract (prose; no executed sample needs infrastructure).
- `README.md` execution walkthrough: note store errors are not skipped by `error_policy`.
- `DESIGN_ISSUES.md`: append one entry (resolved, Fix note, ITL-878).
- [ ] `uv run pytest tests/test_docs/`; commit `docs: state the store-write failure contract
      (ITL-878)`.

## Out of scope (reported, not fixed)

- Async orchestrator ignores `error_policy`; async per-row function errors logged at DEBUG
  (coordinator is filing). This plan touches `process_one` only to re-raise
  `StoreWriteError`.
- Connector/Delta `flush` pop the pending batch before writing (coordinator is filing).
- ITL-877 (SQLite logical types).

---

## Follow-up: CodeRabbit round 1 (approved by the coordinator if each finding reproduces)

**Finding 1 (comment 4534032564): a failing hook or observer callback replaces a
`StoreWriteError`.** A node's own `CachedFunctionPod` never carries hooks, so the reachable
case is a node function that runs another cached pod (`@function_pod(pod_cache_database=...,
post_run_hooks=[...])`) whose store write fails: the ERROR-status hook fires inside
`_invoke_with_hooks`'s `except`, a fail-loud hook raises, its exception replaces the store
error, and the outer node skips the row under `error_policy="continue"`. The same applies to
an observer's `on_data_crash` / `on_node_end` raised on the store-error branch.

- Test first (public interface): standalone `pod.pod(stream).as_table()` and
  `pod.pod.async_execute(...)` with a raising hook raise `StoreWriteError`; a pipeline whose
  node function runs such a pod raises `StoreWriteError` on both orchestrators under
  `continue`; a pipeline observer whose `on_data_crash` raises does not hide it.
- Fix: `_fire_post_run_hooks(payload, store_error=...)`: when a store error is propagating,
  a raising hook is logged at WARNING and recorded with `add_note` on the store error, which
  then propagates. Observer callbacks on the store-error branches go through one helper that
  does the same. Hook failures without a store error are unchanged.

**Finding 2 (comment 4534032576): mixed exception groups from the async orchestrator.**
`except* StoreWriteError` re-raises the unmatched rest as an `ExceptionGroup` together with
the store error, and drops further store errors silently.

- Test first: two sibling nodes failing concurrently (store write + another exception; two
  store writes) → the async caller gets one plain `StoreWriteError`.
- Fix: catch `ExceptionGroup`; if it holds any `StoreWriteError`, raise the one whose node
  comes first in topological order (ties: group order), the order the sync orchestrator meets
  them; log every other `StoreWriteError` at WARNING; add a note per other exception; the
  whole group stays reachable as `__context__` (`__cause__` stays the backend error). A
  group with no store error propagates unchanged.

No hash, schema or other behaviour change.

---

## Follow-up: CodeRabbit round 2, out-of-diff findings (review of 0940f995)

Three findings sat in the review body ("Outside diff range comments"), not in threads.

**Finding A (job.py final flush, approved if it reproduces): the final-flush `StoreWriteError`
does not warn that pending output may not be stored.** All views of one store share the
pending batches, so `pipeline_db.flush()` commits every table; a batch that fails stops the
flush and the batches after it (a `cache_mode=LOG` operator's output, other nodes' rows)
stay pending, and `result_db.flush()` never runs.

- Test first: `Faults(on_flush=True)` with the LOG operator; the raised `StoreWriteError`
  carries a note saying writes still pending may not have been stored.
- Fix: build the error, `add_note(...)`, `raise err from exc`. Same error class, same
  `record_path`, same `__cause__`.

**Finding B (job.py `_flush_best_effort`, approved if it reproduces): a failed best-effort
flush shows only in the log.**

- Test first: an operator LOG write fails and the best-effort flush fails too; the raised
  `StoreWriteError` is still the operator's, and carries a note that the flush failed and
  pending results may not have been stored (upstream rows read back: 0).
- Fix: `_flush_best_effort(run_error, *databases)` keeps logging and adds a note naming the
  flush failure to the propagating run error. The raised error is unchanged.

**Finding C (`ResultCache.store` record preparation): REPRODUCE AND REPORT ONLY.** Establish
on both orchestrators and both error policies which function outputs fail in record
preparation (naive datetime, value not matching the declared type, unsupported type, a
logical type whose `python_to_storage` raises), what the caller sees (row skipped? WARNING
log? observer `on_data_crash`? run returns? anything stored?), and what CodeRabbit's
proposal would change. Ask the coordinator to choose (a) row failure under `error_policy`
reported exactly like a function failure, or (b) `StoreWriteError`. No fix before the reply.

No hash, schema or stored-data change.

---

## Follow-up: Finding C decided — an unstorable output is a row failure (2026-11-11)

**Decision (developer, November 11, 2026):** a function output that cannot be turned into a
storable record (naive datetime, `str` for a declared `int`, `int` for a declared dataclass)
is a ROW FAILURE governed by `error_policy`, exactly like a function that raised. It is not a
`StoreWriteError`. No behaviour change.

Resumed after a machine restart; the previous worker's local commit ff4c25cb (12 pinning
tests) is kept.

- [x] Pinning tests (`TestUnstorableOutputIsARowFailure`, sync/async x continue/fail_fast x
      3 outputs, each compared with a function raising on the same row) also assert the
      per-row log record (level + message) matches the function-failure one, besides the
      observer `on_data_crash`, raise-or-return, rows and stored rows.
- [x] Docs (`docs/concepts/function-pods.md`, "When the store cannot be written"): store
      failures fail the run; a function output that cannot be stored is a row failure;
      async logs row failures at DEBUG and ignores `fail_fast` (ITL-879), unchanged here.
- [x] `DESIGN_ISSUES.md` SW1: the same rule.
- [x] Decline finding (1) with a signed top-level PR comment (issuecomment-6791981712).
- [x] Merged origin/main b197544e (DESIGN_ISSUES conflict: both appended sections kept);
      added the `[0.3.0] — unreleased` CHANGELOG section with this PR's entries.
- [x] Merge origin/main (b197544e; CHANGELOG entries under `[0.3.0] — unreleased`), full
      suite on 3.11/3.12/3.13, golden unchanged, ruff, format, basedpyright, push, CI,
      CodeRabbit round (bodies too). Reconcile with #290 if it merged first.

---

## Follow-up: CodeRabbit round 3 (review of d305c806), approved by the coordinator 2026-11-11

- (A) docs: a normal return stores every result that could be turned into a record; point to
  the row-failure paragraph.
- (B) docs: a side effect whose delivery completed but whose invocation-log write failed is
  delivered again on re-run (track_completion has no record); check or make it idempotent.
- (C) `ResultCache.store`: separate `add_record` from auto-`flush`. A flush failure raises
  `StoreWriteError(result_db.base_path, flush=True)`; new keyword-only `flush: bool`
  attribute; `add_context` then sets the node label but not the row tag; message says pending
  writes could not be committed and the failing write may belong to an earlier row or another
  table. `job.py` final flush uses `flush=True`. Test first.
- (D) `StoreWriteError` message: "Rows stored before this failure are kept; re-running the job
  does not recompute stored function results." Docs explain LOG operators recompute.
- Keep shape consistent with #290's `StoreReadError`.
- [x] tests, fix (392ee8ed), replies on the 4 threads.
- [ ] CI and CodeRabbit on the final head, worker_done.
