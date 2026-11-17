# ITL-1042: node head after an async run — implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After a completed run (sync or async), no job node is stale, and a node's `head()`,
`as_table()` and `iter_data()` agree that its results are there. A head with no rows has the
same columns as a head with rows, for every column configuration.

**Architecture:** One rule for a node's modified time, followed by every job node on both
paths: a node stamps its modified time when its run completes, after it has consumed all its
inputs and **before** it signals completion downstream (closes its output). Then every node is
stamped no earlier than its upstreams, so nothing is stale after the run.

**Tech stack:** Python, asyncio, PyArrow, pytest (`uv run`).

---

## Evidence (established before any code change)

Repro: the ITL-1034 suite (PR #346 head `ed333412`, copied to the scratchpad and run against
this branch) fails `test_every_row_of_a_node_view_is_aligned[unique-shuffled-async_run-natural-in_memory-joined.head]`:
`joined.head` has no rows. In isolation `joined.head` passes; it fails after the earlier views
on the same job (module-scoped run fixture).

Modified times (microseconds) and staleness (`*`) after an async run of the #346 job:

```
after run            B:135099 A:135112 SelectDataColumns:184056 scaled:136321* joined:202799 grouped:197372
scaled.as_table      B:135099 A:135112 SelectDataColumns:184056 scaled:276357  joined:202799* grouped:197372*
```

The same job after a sync run: no node is stale before or after any view.

- `A` has a data column `scaled` does not take, so `pod(A)` adds an automatic
  `SelectDataColumns` (ITL-807), an `OperatorJobNode` upstream of `scaled`.
- `OperatorJobNode.async_execute` stamps itself when it finishes
  (`core/nodes/operator_node.py:1613`, `_update_modified_time()`).
- `FunctionJobNode.async_execute` never stamps (`core/nodes/function_node.py:3200-3487`); the
  sync `execute` does (`function_node.py:1937`). So `scaled` keeps its construction time,
  which is older than its upstream's: `scaled.is_stale` is True right after the run (lead (a),
  confirmed).
- The first read of `scaled` (`iter_data`/`as_table`, `function_node.py:3151-3152`) sees it
  stale, calls `clear_cache()`, which stamps the node (`function_node.py:512`) and re-reads
  the store. Now `scaled` is newer than `joined` and `grouped`, which become stale, and
  `OperatorJobNode.head` reports `NOT_COMPUTED` / "inputs changed" (`operator_node.py:1337-1341`).
- Lead (b) (the upstream bumped per forwarded row) is wrong: `record_and_forward`
  (`function_node.py:3348-3390`) clears `_cached_output_table` but never stamps.

Without the auto-select (a source with only the function's input) nothing is stale after an
async run, because the source node never stamps. So the defect needs a stamped upstream: an
operator node before a function node.

## Cost measured today (before the fix)

Script: count `scale` calls and `InMemoryArrowDatabase` reads (`get_all_records`,
`get_records_with_column_value`, `get_records_by_ids`, `get_record_by_id`).

| After a run | sync | async |
|---|---|---|
| `scaled.head()` | 0 reads, ROWS | 2 `get_all_records` (whole pipeline + result tables), ROWS |
| `scaled.as_table()` | 0 reads | 2 `get_all_records`, in-memory results thrown away and reloaded |
| `joined.head()` / `grouped.head()` | ROWS | NOT_COMPUTED, 0 rows (after a `scaled` read) |
| `joined.as_table()` | 3 rows | 3 rows (no staleness check there) |
| function calls in any read / second run | 0 | 0 |

A second async run reads the store (2 `get_all_records`) on both paths' async entry
(`async_execute` always loads cached entries); that is not caused by staleness.

## Rule

A job node's modified time is stamped once per completed run, after it has consumed all its
inputs and before it closes its output (signals completion downstream), on the sync and the
async path. A failed run does not stamp. Consequences: after a completed run no node is
stale; `head()`, `as_table()` and `iter_data()` serve the run's in-memory results without
reading the store.

## Files

- Modify `src/orcapod/core/nodes/function_node.py`: `FunctionJobNode.async_execute` stamps
  after a completed run, before `output` is closed. The no-database branch hands the pod a
  writer whose `close()` is deferred, so the stamp still precedes the close.
- Modify `src/orcapod/side_effects.py`: `SideEffectJobNode.execute` / `async_execute` stamp
  likewise, so the rule holds for every job node (they keep no rows; staleness only).
- Create `tests/test_pipeline/test_node_staleness_after_run.py`.
- Item 4 (pending coordinator answer, option A recommended): rowless heads get the row-only
  column groups (`_source_*`, `_context_key`, content hash) in each kind's own column order.
- Create `design_issues/ITL-1042-<id>.md`, `changelog.d/ITL-1042.md`.

## Tasks

### Task 1: failing tests (public interface)

- [ ] Write `tests/test_pipeline/test_node_staleness_after_run.py`:
  job = source A (`id`, `g` tags; `w`, `x` data, rows not in tag order) → `scaled`
  (function pod on `x`, auto-select drops `w`) → `joined` = `Join(scaled, B)` and
  `grouped` = `GroupBy(by=["g"])(scaled)`. Parametrize orchestrator (sync, async) × store
  (in_memory, delta).
  - `test_no_node_is_stale_after_a_run`: every node with `is_stale` is not stale.
  - `test_heads_show_rows_after_reading_upstream`: read `scaled.as_table()`, then every
    node's `head(100)` is ROWS with as many rows as its `as_table()` and `iter_data()`.
  - `test_reads_after_a_run_do_not_touch_the_store`: after the run, `head`, `as_table` and
    `iter_data` of every node make zero store reads (patch the store class's read methods
    with counting wrappers) and zero function calls.
  - Async completion order forced without sleeps: an `async def` function pod gated by
    `asyncio.Event`s so rows complete in reverse input order; same assertions.
- [ ] Run: `uv run pytest tests/test_pipeline/test_node_staleness_after_run.py -q`; expect the
  async cases to FAIL and the sync cases to pass.
- [ ] Commit (WIP) `test(nodes): node staleness and head after sync and async runs (ITL-1042)`.

### Task 2: stamp on the async path

- [ ] In `FunctionJobNode.async_execute`, after `self._commit_at_node_end()` (DB branch) and
  after the pod returns (no-DB branch), call `self._update_modified_time()` before
  `on_node_end`; in the no-DB branch pass the pod a deferred-close writer so the stamp
  precedes the close (the outer `finally` closes `output`).
- [ ] `SideEffectJobNode.execute` stamps after its loop; `async_execute` stamps after the
  TaskGroup, before the `finally` close.
- [ ] Run the new tests: PASS. Run `tests/test_pipeline tests/test_core/function_pod tests/test_core/operators -q`.
- [ ] Run the #346 suite copy against the branch: `joined.head`/`grouped.head` pass.
- [ ] Commit `fix(nodes): stamp a node's modified time when its async run completes (ITL-1042)`.

### Task 3: rowless head columns — coordinator decision: option C (Nov 12, 2026)

Do not change rowless heads. Document in the `head()` docstring (`StreamBase.head`) and the
docs that a head with no rows shows the declared columns only (no `_source_*`,
`_context_key` or content-hash columns): their presence and, for `_source_*`, their type are
known only from rows. The coordinator records the finding (37 mismatches over 8 configs x 7
stream kinds) on ITL-985 and ITL-1002. Save the measurement script as
`superpowers/specs/itl-1042/rowless_head_columns.py`.

- [ ] Docstring + docs page; script saved; commit.

### Task 4: finish

- [ ] design_issues entry, changelog fragment, docs if a page states the old behaviour.
- [ ] ruff check/format, basedpyright exit 0, full suite on 3.11/3.12/3.13 (≤ 2 processes).
- [ ] Merge origin/main, push once, PR (Fixes ITL-1042; note #346 must drop its ITL-1042
  xfail), CodeRabbit, signed replies, worker_done.

## Notes for a successor

- ITL-1045 is a different defect (two passes in `as_table` with a genuinely stale upstream);
  this one is a false staleness after an async run.
- Scratchpad scripts: `repro.py`, `cost.py`, `emptyhead.py`, `a346/drive.py`.

---

## ITL-1052 (added Nov 12, 2026 by the coordinator; same PR: Fixes ITL-1042, Fixes ITL-1052)

**Cause.** `FunctionPodStream._ensure_iterator` stamps when the stream OPENS its input. The
upstream's `iter_data` is a lazy generator, so in `s2 = g.pod(f.pod(src))` the upstream opens
(and stamps) only after `s2` has stamped: `s2` ends its first full read older than `s1`, and its
second read clears and recomputes (6 calls, then 9).

**Rule (same as ITL-1042).** A stream or node stamps its modified time when its results are
complete (its input read to the end), before it signals the end downstream; never at the start
of a read. `clear_cache()` stamps (a reset is a change). A failed or partial read does not stamp.

**Mid-read reset (coordinator: must not become silent).** A reset of an upstream in the middle
of a downstream's read must leave the downstream stale. A modified-time snapshot alone cannot
tell a reset from the upstream's own completion stamp (which lands during the downstream's
read), so:
- `FunctionPodStream` keeps `_reset_generation`, bumped by `clear_cache()`.
- When a read takes its first item from the input (after the upstream's own stale refresh), it
  snapshots each upstream: the generation for a `FunctionPodStream` upstream, the modified time
  for any other. Kept across partial reads; cleared by `clear_cache()`.
- At the end of the input it stamps if the snapshot still holds and no upstream modified time is
  `None`; otherwise it sets its own modified time to `None` (stale; downstream streams then see a
  `None` upstream and are stale too, so it propagates).
- Concurrent path: same, at the end of the materialised compute.

**Tests.** `tests/test_core/function_pod/test_pod_stream_staleness.py` (28 cases; all fail
before the fix on the staleness assertion): chains of 2 and 3, sequential and concurrent, second
read by `iter_data` / `as_table` / `head` calls nothing; first read by `as_table`; through a
`Join`; cached chain reads no store; `clear_cache()` upstream recomputes (read by `iter_data`:
`as_table` on a stale pod stream is ITL-1045); upstream reset mid-read leaves the reader stale
and the next read is of one state.

**Waiting for** #348 (rewrites `_iter_data_sequential` / `_iter_data_concurrent`) to merge; then
merge origin/main and implement.
