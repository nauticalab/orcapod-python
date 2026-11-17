# ITL-1083: pass-through operator stream judges its own fresh result stale — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `DynamicPodStream` whose kept result is a lazy stream stays current when an upstream
completes its first computation after the result was kept; a real upstream change still makes it
stale.

**Architecture:** `DynamicPodStream` notes, when it keeps a result, the state of each upstream.
A pod stream (`FunctionPodStream`, `_CachedFunctionPodStream`) or another operator stream
(`DynamicPodStream`) is identified by its reset generation (the rule ITL-1065 set for
`FunctionPodStream` inputs); any other upstream (source, table stream, node) by its modified time,
as before. A stale pod stream counts one pending reset. The operator stream gets its own reset
generation, bumped whenever it discards a kept result.

**Tech Stack:** Python 3.12, pytest, `uv run`.

---

## Cause (verified on `512cece9`, scratchpad repro)

Order of timestamps for `SelectDataColumns(["y"])(f.pod(src))`, all UTC on one run:

1. `t0 = 00:49:45.364628` — `FunctionPodStream` constructed (`TraceableBase` stamps it).
2. `t1 = 00:49:45.365061` — `op.as_table()` → `DynamicPodStream.run()` →
   `static_process` returns the pod stream itself, unread → `_cached_time = t1`.
3. `t2 = 00:49:45.373009` — `as_table` reads the kept stream (the pod stream); the pod stream
   reaches its input's end and stamps `t2` (`_input_read_to_end`).
4. Next status check (`head()`, `is_stale`, `as_table()`): `_update_cache_status` sees upstream
   `t2 > t1` and drops `_cached_stream`, setting `_cached_time = t2`. So `is_stale` is `False`
   (own time = upstream time) while `head()` has 0 rows (`not computed`), and the next
   `as_table()` runs `static_process` again.

Second symptom, same cause: `g.pod.process(op)` with `op` as above. The pod stream `g` notes
`op.last_modified` (`t1`) at its first row; at its end, `op` has dropped and moved to `t2`, so
`g` thinks its input changed, clears its modified time, is stale after a full read, and a second
read calls `g` again (3 more calls).

## Operators: does `static_process` read its input?

| Operator | Reads input? | Can return the input stream itself (lazy, unread) |
|---|---|---|
| `SelectTagColumns` | yes, unless every tag is selected | yes: all tags selected |
| `SelectDataColumns` | yes, unless every data column is selected | yes: all data columns selected |
| `DropTagColumns` | yes, unless nothing to drop | yes: no tag to drop (`strict=False`, absent names) |
| `DropDataColumns` | yes, unless nothing to drop | yes: no data column to drop |
| `MapTags` (`column_selection.py`) | yes, unless no tag renamed | yes |
| `MapTags` (`mappers.py`) | yes, unless no tag renamed | yes |
| `MapData` | yes, unless no data column renamed | yes |
| `PolarsFilter` | yes, unless no predicate/constraint | yes: empty filter |
| `Join` | yes, unless one input | yes: single input |
| `SemiJoin` | yes, unless no common key | yes: left returned, right never read |
| `MergeJoin` | always (`as_table` of both) | no |
| `Batch` | always (`as_table`) | no |
| `GroupBy` | always (`as_table`) | no |
| `DropNulls` | always (`as_table`) | no |
| `Index` | always (`iter_data`) | no |
| `Pick` | always (`iter_data`) | no |

Every result an operator builds itself is an `ArrowTableStream` over a table it read, so only the
pass-through cases are lazy. The fix is in `DynamicPodStream` and does not depend on the list.

## Files

- Modify: `src/orcapod/core/operators/static_output_pod.py` (`DynamicPodStream`:
  `__init__`, `clear_cache`, `_update_cache_status`, `run`, new module helper `_upstream_state`).
- Create: `tests/test_core/streams/test_pass_through_operator_staleness.py`.
- Create: `changelog.d/ITL-1083.md`, `design_issues/ITL-1083-<id>.md`.

## Task 1: failing tests (public interface)

- [ ] Reproduction: `SelectDataColumns(["y"])(f.pod(src))`; after `as_table()` / `iter_data()` /
  `flow()`: `head()` has the rows with status `computed`, `is_stale` is `False`, a second
  `as_table()` does not run `static_process` (spy on the non-public method, commented: no public
  signal tells an operator run), and the function is not called again.
- [ ] Matrix: pass-through operator ∈ {SelectTagColumns, SelectDataColumns, DropTagColumns,
  DropDataColumns, MapTags, MapData, PolarsFilter(), Join(single), SemiJoin(no common key)} ×
  upstream ∈ {pod stream, cached pod stream, pass-through operator stream over a pod stream,
  chained two deep (pod → pod → op, op → op → pod)}.
- [ ] A pod stream reading a pass-through operator stream over a pod stream: second read calls
  no function, nothing stale.
- [ ] Real change still detected: `clear_cache()` of the pod stream (and of the first pod in a
  two-pod chain, a lazy reset) after the read makes the operator stream stale and the next read
  recomputes, with the new values.
- [ ] Run: `uv run pytest tests/test_core/streams/test_pass_through_operator_staleness.py` →
  the stability cases FAIL, the real-change cases PASS.
- [ ] Commit (WIP, failing tests).

## Task 2: fix in `DynamicPodStream`

- [ ] `_upstream_state(stream)`: `FunctionPodStream` → `("generation", gen + int(is_stale))`;
  `DynamicPodStream` → read `last_modified` (settles its own check) then
  `("generation", gen)`; anything else → `("time", last_modified)`.
- [ ] `__init__`: `self._reset_generation = 0`, `self._kept_states = None`.
- [x] `run`: note the states BEFORE `static_process` (a reset made while a reading operator
  reads must be seen; noting after hid it, caught by
  `test_a_reset_while_a_reading_operator_reads_leaves_it_stale`), keep them after the stamp.
- [x] `is_stale` overridden: `_update_cache_status()` then `_cached_stream is None` (the
  inherited time comparison called a fresh pass-through result stale).
- [ ] `_update_cache_status`: unknown time (pod, or a time-tracked upstream) → discard with
  `_cached_time = None` (as before); otherwise discard when a state differs from the kept one or
  the pod / a time-tracked upstream is newer than `_cached_time`. A discard bumps the generation,
  sets `_cached_time` to now and the kept states to the current ones (so the next check does not
  fire again without a new change).
- [ ] `clear_cache`: bump the generation, clear kept states.
- [ ] Run new tests → PASS. Run the existing staleness tests unchanged:
  `tests/test_core/streams/test_dynamic_pod_stream_staleness.py`,
  `tests/test_core/function_pod/test_pod_stream_staleness.py`,
  `tests/test_core/function_pod/test_pod_stream_interleaved_reads.py`,
  `tests/test_core/function_pod/test_resumed_iteration.py`,
  `tests/test_pipeline/test_node_staleness_after_run.py`, `tests/test_pipeline/test_node_head*.py`,
  `tests/test_core/streams/test_head*.py`, `tests/test_core/operators/`, golden suite.
- [ ] Commit.

## Task 3: changelog fragment, design issue entry, lint, types, PR

- [ ] `changelog.d/ITL-1083.md` (`### Fixed`), `design_issues/ITL-1083-<id>.md` (resolved).
- [ ] ruff check/format, basedpyright exit code, changelog/design-issues checks.
- [ ] Merge origin/main, push, PR "Fixes ITL-1083", `orca linear attach`, `orca worktree set`.

## Notes for a successor

- Status 2026-11-13: fix and tests committed; broad suite green locally (3.12). Next: PR, CI, CodeRabbit.
- Conflict: ITL-1081 renames `run()` → `_refresh()`; keep the edit inside `run` to one added
  line.
