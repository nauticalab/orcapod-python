# ITL-1102: a run checks each source for new data first — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** At the start of every run (`job.run`, `run_async`, a job node's `run()`, `load_only`
included) every source feeding it is asked once whether its data changed; a source that
changed takes the new data in and moves its modified time (so the #380 state rule runs the
nodes below it); a check that raises warns once and the run goes on.

**Architecture:** One public hook, `RootSource.check_for_new_data() -> bool` (default: no-op,
`False`). One helper in `pipeline/node_run.py`, `check_sources(streams) -> tuple[str, ...]`,
walks the streams of the run up to its sources (a `SourceJobNode`'s bound source, any
`RootSource` met while walking `upstreams` of streams outside the job), calls the hook once
per distinct source, turns an exception into one `SourceCheckFailedWarning` and returns the
source ids that could not be checked. It is called before any currency is judged: in
`PipelineJob.run`/`run_async` before `plan_run`, and in a job node's `run()` (`run_job_node`
for function and side-effect nodes, `OperatorJobNode.run`) before `holds_current_run`; the
cascade (`run_with_dependencies`) receives the result and does not check again. The
unchecked ids go into `RunSummary.unchecked_sources`.

Coordinator approvals (orchestration ask, November 14, 2026): hook name; polling time moves
only on a committed batch with >= 1 row and moves to NOW; `Cursor.modified_at` kept but
informational (removal = follow-up); SQL/CSV re-read and compare ORDER-INSENSITIVELY
(multiset of row hashes), cost stated in docs/concepts/sources.md; Delta/SpiralDB/Cached/
Derived/Proxy as proposed; schema change or dropped-and-recreated table at check time = a
failed check; `SourceCheckFailedWarning`, `RunSummary.unchecked_sources`.

**Deviation to report:** the call site is the run entry points, not `plan_run` itself,
because `node.run()` judges `holds_current_run` before `plan_run` is reached, and a check
must happen once per run.

**Tech Stack:** Python, PyArrow, deltalake, pytest.

---

## Stage 1 (commit 1): hook + call sites + polling + warning + summary

### Task 1.1: failing tests — `tests/test_pipeline/test_run_checks_sources.py`

Public interface only, both orchestrators (`ORCHESTRATORS` param as in
`test_job_run_state_rule.py`), async also via `run_async`.

- A counting test source: subclass of `DictSource` whose `check_for_new_data` counts calls,
  optionally raises, optionally swaps in new rows (via the public construction of a new
  `DictSource` internal stream? no — use a `PollingSource` with a list-backed impl for the
  "new rows" tests, and a `DictSource` subclass overriding only `check_for_new_data` for
  the count/raise tests).
- Tests:
  1. unchanged polling source: second `job.run()` calls the function 0 times, store
     `add_records` untouched (mock), summary holds.
  2. polling impl gets new rows between runs: second run computes only the new rows
     (`CALLS` count == new rows), totals include all rows.
  3. regression: impl returns `Cursor(modified_at=<a time before the first run>)` for the
     new batch: the second run still processes it.
  4. a poll with has_new True but an empty batch does not move `last_modified`.
  5. check raises: exactly one `SourceCheckFailedWarning` (message has source id, error
     text, "proceeds with the data the source already holds"), no raise; current job
     computes nothing; `summary.unchecked_sources == ("s",)`; repr has the line; a job
     with a cleared node runs on the held data.
  6. `head()` and `_repr_html_` (preview) of the job node / source call no check.
  7. `load_only=True` run calls the check once and computes nothing.
  8. `node.run()` of the last node checks each source once (count == 1).
  9. one check per distinct source per run when a source feeds two nodes.

### Task 1.2: implementation

- `src/orcapod/errors.py`: `class SourceCheckFailedWarning(UserWarning)`.
- `src/orcapod/core/sources/base.py`: `RootSource.check_for_new_data(self) -> bool: return False`
  with docstring stating the contract.
- `src/orcapod/pipeline/node_run.py`: `run_sources(streams)` (collect distinct sources) and
  `check_sources(streams) -> tuple[str, ...]` (warn per failure, return ids);
  `run_with_dependencies(target, *, load_only, unchecked=())` passes into `RunSummary`.
- `src/orcapod/pipeline/run_summary.py`: `RunSummary(..., unchecked_sources=())`, text
  line `  sources not checked for new data (see the warning): a, b` after the headline,
  HTML `<p>` with the same text.
- `src/orcapod/pipeline/job.py`: `check_sources(prepared.exec_dag.nodes())` before
  `plan_run` in `run` and `run_async`; pass to `_finish_run` → `RunSummary`.
- `src/orcapod/core/nodes/function_node.py` `run_job_node` and
  `src/orcapod/core/nodes/operator_node.py` `OperatorJobNode.run`: `check_sources([node])`
  before `holds_current_run`; pass to `run_with_dependencies`.
- `src/orcapod/core/sources/polling_source.py`: `check_for_new_data` = one
  `_sync_poll_and_commit`, returns whether a non-empty batch was committed;
  `_sync_poll_and_commit` and the async loop move the time to now only for a committed
  batch with rows (`_mark_new_rows`); `_update_last_modified_from_cursor` removed;
  `Cursor.modified_at` doc: informational, not used for staleness.

### Task 1.3: docs, rules, changelog, design issue

- `docs/concepts/function-pods.md` (replace the "Staleness comes from modified times only"
  limit paragraph), `docs/concepts/streams.md` if it states it, `docs/concepts/sources.md`
  new section "What a source does at a run" (table per kind; stage 1 lists in-memory and
  polling; later stages add rows).
- `CLAUDE.md` + `.zed/rules`: one rule (developer, November 14, 2026, ITL-1102).
- `changelog.d/ITL-1102.md`; `design_issues/ITL-1102-SC1.md` (status in progress → resolved).
- Run targeted tests: new file, `tests/test_channels/test_polling_source*.py`,
  `tests/test_pipeline/test_job_run_state_rule.py`, `test_node_run_cascade.py`,
  `test_job_run_summary.py`, docs samples. Commit `feat(pipeline): a run checks each source
  for new data first (ITL-1102)`. Push; open DRAFT PR against main.

## Stage 2 (commit 2): Delta + SpiralDB

- Failing tests `tests/test_core/sources/test_source_check_for_new_data.py` (Delta: append
  via `deltalake.write_deltalake(mode="append")`, second run computes only new rows;
  unchanged → False and no recompute; schema change → warning, held rows kept) and SpiralDB
  with `fake_pyspiral` (write → version moves → pin advances → second run processes the new
  rows only; explicit `asof` never moves; check reads no row (scan_log unchanged) and does
  not raise `SourceTooLargeError` when over max_rows; dropped+recreated table and changed
  schema → failed check).
- `DeltaTableSource`: keep `_tag_columns`, `_system_tag_columns`, `_record_id_column`; hold
  `_version = self._delta_table.version()`; `check_for_new_data`: `update_incremental()`,
  compare version; reload into a new stream via the same builder; refuse a schema change
  (raise → warning by caller); move time.
- `SpiralDBTableSource.check_for_new_data`: given asof → False; else open connector, check
  table id equals `_table_id` and the schema equals construction schema (else raise
  ValueError), version = `get_table_version`; if `version and version != self._asof`:
  `_asof = version`, `_update_modified_time()`, True.

## Stage 3 (commit 3): CSV, SQLite, PostgreSQL, Cached, Derived, Proxy

- Order-insensitive multiset comparison helper (row hashes of the data-columns+tag-columns
  table, sorted) in `utils/arrow_utils.py` or a small module `core/sources/reload.py`.
- `CSVSource`: stat prefilter (`st_mtime_ns`, `st_size`), then re-read, compare multiset;
  reload when it differs.
- `DBTableSource` (SQLite/PostgreSQL): re-read with the construction query through a fresh
  connector (SQLite/PG reopen; DBTableSource uses its connector), compare multiset, reload.
- `CachedSource`: delegate; on change drop `_cached_stream`, move time.
- `DerivedSource`: RootSource origin → delegate; job-node origin → origin.last_modified
  after the time rows were cached → drop cache, move time.
- `SourceProxy`: delegate; on change move own time.
- Docs table rows; cost paragraph for SQL.

## Finishing

- Merge origin/#380 branch as it moves; when #380 merges, merge origin/main.
- ruff check/format, basedpyright exit code, changelog + design issue checks.
- PR draft until CI green and #380 merged; CodeRabbit flow per the brief.

## Notes for a successor

- Proposal and approval text: see the orchestration thread of task_b45dca7f38d2.

## Findings from the #380 worker (coordinator message, November 14, 2026)

1. Delta: a version can move with no row change (OPTIMIZE, metadata) -> after the version
   signal, compare reloaded rows with held ones order-insensitively; move time only if rows
   changed.
2. Positional record ids unstable after append (Delta scan order), SQL unordered SELECT,
   PollingSource restarts row_0 per batch: ITL-1104, not ours; docs state the consequence
   (done in sources.md); reload keeps each source's id rule.
3. CachedSource: check must drive the inner source's check; move the cached source's time
   only if the cache gained rows.
4. DerivedSource: plain form caches origin.get_all_records() forever (even an empty table from
   before the node ran); tags_as_data over a RootSource caches forever: refresh when the origin
   changed.
5. SourceProxy: forward last_modified of the delegate (bind/unbind do not stamp).
6. SpiralDB: compaction may move the version: note it (spurious re-run reuses everything).
7. ITL-1103 = follow-up for design revisits; cited in sources.md.

## Status

- Stage 1: committed 42826d93, pushed, draft PR #384.
- Stage 2: Delta + SpiralDB implemented (reload.same_rows helper), tests in tests/test_core/sources/test_source_check_for_new_data.py.
- Stage 2: committed cb988d7c. Merged #380 head 3cd6bef5 (9ad0b8e7; function-pods.md conflict: kept ours).
- Stage 3: CSV (stat + order-insensitive re-read), DBTableSource/SQLite/PG (re-read every run,
  _open_connector per class), CachedSource (inner check + record-hash compare, no write),
  DerivedSource (origin check / job node re-read after it ran), SourceProxy (forward
  last_modified, stamp bind/unbind, delegate check). Targeted suites: 10319 passed.
- Next: CI, wait for #380 to merge, then CodeRabbit per the brief.

## CodeRabbit round 1 (review of d801b22d; plan approved by the coordinator, November 14, 2026)

- Delta, CSV, DB table sources compare the raw table read with the raw table held (every
  column, `system_tag_columns` included), before building the stream.
- `CachedSource` and `DerivedSource` re-read when the inner source's / origin's modified time
  moved since they last read it (the inner check's return value is not trusted: another
  wrapper may have consumed the change), and record that time only after the read and
  comparison succeed.
- `SourceJobNode.run()` checks its bound source; a bare source's `run()` does not (decision).
- Declined: a held `node.run()` makes no summary; the warning is the report (docs say so).
- #380's findings (operator lazy materialization cleanup: fixed in #388; load-only
  `add_records` spy: declined) answered on their threads.

## CodeRabbit round 2 (incremental review of 5be53b0f; plan approved by the coordinator)

- `reload.keep_nullable_columns`: where nullability is inferred (CSV without `schema=`, a
  DB connector without column info), a column that lost its nulls is re-wrapped nullable
  (never cast); a null added to a non-nullable column is still a failed check.
- `DerivedSource` holding no rows moves when its origin's time moves.
- ITL-1095 fragment: "reads no source rows to process them".
- Declined: run_async checks off the event loop (follow-up), `run_sources` deque, test docstrings.

## CodeRabbit round 3 (incremental review of ae8323fc; plan approved by the coordinator)

- `CachedSource` compares with the record hashes it presents (recorded when it builds its
  merged rows), so a wrapper sharing a cache with another still moves.
- CSV and SQLite sources keep an absolute access path (`:memory:` and `file:` URIs as given).
- Docs: Delta's check cost stated exactly; a `PollingSource` a run reads is polled again,
  and a failing poll raises (follow-up ITL-1139).
- Declined: a summary for the direct side-effect run path (none exists on that path).

## CodeRabbit round 4 (incremental review of 48ce9c05; approved by the coordinator)

- `CachedSource` compares (live hashes | cached hashes) with the hashes it presents:
  invariant "the modified time moves exactly when the next read would present a different
  set of rows". Mirror tests: shared cache with add, add-then-delete, delete-only, from both
  wrappers in both orders.

## CodeRabbit round 5 (incremental review of ec0b7617; coordinator decision)

- A cache shared through `cache_path` by wrappers over different source objects is not
  followed by the check: documented (docstring, sources.md); comparing on every check is
  ITL-1143 (cost: a full cache read per run).

## Developer decision: row order follows the record id rule (November 14, 2026)

- `reload.same_rows(held, read, *, ordered)`: callers pass `ordered=self._record_id_column is None`
  (CSV, Delta, DB table, DerivedSource); without a `record_id_column` a reorder is a change
  and ids follow the new positions (function results reused); with one, only content counts.
  In doubt, order counts. Fuller design: ITL-1103 / ITL-1104.
