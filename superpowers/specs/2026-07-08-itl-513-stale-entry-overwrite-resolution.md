# ITL-513: Stale-Entry Overwrite Resolution — Staleness Assessment & Cleanup

**Date:** 2026-07-08
**Issue:** [ITL-513](https://linear.app/enigma-metamorphic/issue/ITL-513)
**Status:** Approved

---

## Background

ITL-513 described a bug where `FunctionJobNode.add_pipeline_record` silently skipped
stale-entry overwrites because `skip_duplicates=True` was passed using the same
`entry_id` that the stale record occupied. This caused an infinite recomputation
cycle: Phase 2 recomputed, tried to write, was silently blocked, and on the next
call the stale entry still prevented a Phase 1 hit.

The issue was filed against the original `iter_packets()` two-phase code (commit
`ce1fff91`) where recomputation ran inline inside the iteration loop. Two
intervening changes made the original symptom description partially stale:

1. **ENG-379** (`840569f1`) rewrote `iter_data()` to be strictly read-only.
   Computation was moved to `execute()` / `run()`. The cycling concern shifted
   from `iter_data()` to `execute()`, but the underlying `skip_duplicates` bug
   remained.

2. **ITL-508** (`1418ff81`) redesigned `add_pipeline_record` to use indexed
   entry-ID versioning (`max_index + 1`). Each recomputation writes at a new
   DB primary key; stale entries at index N no longer block writes at index N+1.
   The `skip_cache_lookup` parameter was removed entirely. The stale-entry cycle
   is now prevented in all code paths.

---

## Remaining Work

ITL-508 fixed the root cause. Two small cleanup items remain:

### 1. DESIGN_ISSUES.md — Mark F7 resolved

Entry **F7** (`"TOCTOU race in FunctionPodNode.add_pipeline_record"`) describes
the pre-cursor form of the same bug. Its `Status` field still reads `open`.
Update it to `resolved` with a note pointing to ITL-508.

### 2. New test — `iter_data()` serves result after cross-session recompute

ITL-513's success criteria explicitly required a test covering the stale-entry
recompute case across `iter_data` calls. The existing tests
(`test_recompute_after_ephemeral_miss_no_infinite_cycle`,
`TestAddPipelineRecordIndexed`) verify the multi-session mechanics using
`execute()`. No test explicitly exercises the `iter_data()` read path after a
cross-session miss recompute.

**Location:** `tests/test_core/function_pod/test_ephemeral_result.py`,
appended to `TestEphemeralWritePath`.

**Test name:** `test_iter_data_serves_result_after_cross_session_recompute`

**Scenario:**

| Step | Action | Expected |
|------|--------|----------|
| Session 1 | `execute(stream)` with ephemeral store 1 | `call_count = 1`; index-0 record in pipeline DB |
| Session 2 | Fresh ephemeral store 2, `execute(stream)` | `call_count = 2` (miss → recompute); index-1 record written |
| Session 2 | `list(node2.iter_data())` | Returns 1 result with correct value; `call_count` still 2 |
| Session 2 | `list(node2.iter_data())` again | Same result; `call_count` still 2 (no second recompute) |

**No production code changes.** All fixes are in `function_node.py` via ITL-508.

---

## Files Changed

| File | Change |
|------|--------|
| `DESIGN_ISSUES.md` | Update F7 status to `resolved`; add Fix note |
| `tests/test_core/function_pod/test_ephemeral_result.py` | Add `test_iter_data_serves_result_after_cross_session_recompute` to `TestEphemeralWritePath` |
