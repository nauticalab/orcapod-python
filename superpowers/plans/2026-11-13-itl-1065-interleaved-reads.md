# ITL-1065 Interleaved reads of a pod stream recompute a yielded row — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Interleaved and resumed reads of a function pod stream call the function once per
input row and yield the same datagrams as a later full read, whatever its upstream kind, while
every staleness guarantee of PR #355 (ITL-1042, ITL-1052) still holds.

**Architecture:** `FunctionPodStream.is_stale` gets two rules: completed results are judged by
modified-time order (as now); results in progress (a read has noted its input's state and has
not reached the end) are judged by whether the input's noted state changed. In-memory
bookkeeping only.

**Tech Stack:** Python 3.12, pytest, `uv run`.

---

## Cause (confirmed on 89160c47, probe in the session scratchpad)

`s = f.pod(DictSource(rows with an extra column))` reads an automatic `SelectDataColumns`
stream (`DynamicPodStream`, last_modified `None` until it runs).

```
start             s.own=.655051 s.lm=None gen=0 stale=True  up.lm=None     calls=0
next(first)       s.own=.655087 s.lm=None gen=1 stale=True  up.lm=.655445  calls=1
3 x next(second)  s.own=.656093 s.lm=.656093 gen=2 stale=False up.lm=.655445 calls=4
drain first/second/third: calls=7, first read's row 0 is not the third read's
```

1. `first`'s `iter_data` sees the stream stale (input time unknown), `clear_cache()` stamps
   `s` at t1 (generation 1).
2. `_ensure_iterator` opens the operator stream, which computes now and takes t2 > t1.
3. Until the read completes, `s` is older than its input, so `last_modified` is `None` and
   `is_stale` is True mid-read. `second`'s `iter_data` clears the cache (generation 2) and
   recomputes row 0, which `first` had already yielded.

Before #355, `_ensure_iterator` stamped `s` after opening (and so computing) the operator
input, which hid this. Without the extra column the input is a source, older than `s`.

## Rule after the fix

- Completed results (input read to the end and stamped, or nothing read since a reset) are
  current while no direct upstream and not the pod is newer: modified-time order, as #355.
- Results in progress (a read has noted its input's state, the end not reached) are current
  while the input's state is the noted one and the pod did not change: a pod-stream input's
  reset generation; any other input's modified time, noted once it has delivered a row (an
  operator stream computes when opened, so its time then moves only when it is recomputed or
  invalidated). An upstream completing for the first time during the read is not a change.
- So the operator stream needs no reset generation: its modified time, noted after it has
  computed, is one.
- A reset during a read still changes the noted state: the next read of the stream clears it,
  and the stream ends its read stale (unchanged from #355).

## Files

- Modify: `src/orcapod/core/function_pod.py` (`FunctionPodStream`: `is_stale` override,
  `_input_read_to_end` ends the read in progress).
- Create: `tests/test_core/function_pod/test_pod_stream_interleaved_reads.py`.
- Create: `design_issues/ITL-1065-F19.md`; update `design_issues/ITL-1052-F18.md` (rule).
- Create: `changelog.d/ITL-1065.md`.

### Task 1: Failing tests

Public interface only: `iter_data`, `is_stale`, `clear_cache`, call counts, `is` identity of
yielded datagrams.

Upstream kinds: `source` (`pod.process(src)`), `operator` (`pod(src_with_extra_column)`, the
automatic select), `pod_stream` (`g.pod(f.pod(src))`), `pod_over_operator`
(`g.pod(f.pod(src_with_extra))`). Executors: sequential, concurrent. Pods: plain, cached
(`CachedFunctionPod`, fresh in-memory store).

- [ ] `test_interleaved_reads_compute_each_row_once`: `first`, `second` iterators; one row from
  `first`, three from `second`; drain both; a third full read. Every function called once per
  row; the three reads yield the same datagram objects; nothing stale.
- [ ] `test_a_read_after_an_abandoned_partial_read_computes_each_row_once`: one row, abandon,
  full read: once per row, same row 0 object.
- [ ] `test_a_reset_between_interleaved_reads_gives_one_state`: stamped (epoch) upstream pod,
  directly or through a join + automatic select; reset it between the two iterators; drain;
  third read is all epoch 1, derived from the upstream's rows, computes nothing, not stale.
- [ ] Run, expect `operator` / `pod_over_operator` sequential cases to fail (7 calls).
- [ ] WIP commit.

### Task 2: Fix

```python
@property
def is_stale(self) -> bool:
    if self._input_state is None:
        return super().is_stale
    current = self._current_input_state()
    if current != self._input_state or None in current:
        return True
    own = self._modified_time
    pod_time = self._function_pod.last_modified
    return own is None or pod_time is None or pod_time > own
```

`_input_read_to_end` sets `self._input_state = None` after stamping (the read is no longer in
progress). `clear_cache` already clears it.

- [ ] Implement; run the new tests, `test_pod_stream_staleness.py`,
  `tests/test_pipeline/test_node_staleness_after_run.py`, `tests/test_core/function_pod`,
  `tests/test_core/streams` (one pytest process, Python 3.12).
- [ ] Re-run the probe: 6 calls.
- [ ] Commit.

### Task 3: Docs and records

- [ ] `design_issues/ITL-1065-F19.md` (cause, rule, fix); F18 rule paragraph points to F19.
- [ ] `changelog.d/ITL-1065.md` under `### Fixed`.
- [ ] `uv run python scripts/design_issues.py --check`, `scripts/assemble_changelog.py --check`,
  ruff, basedpyright.
- [ ] Commit, merge origin/main, push, PR (Fixes ITL-1065), Copilot review.

## Notes for a successor

- Status (2026-11-13): tests (b5247136) failed 8 sequential cases before the fix; fix in
  `FunctionPodStream.is_stale` / `_input_read_to_end`; targeted suites pass (1472); repro and
  the ITL-1034 suite's interleaved cases (`--runxfail`, 117) pass. Docs, F19, F18 pointer and
  changelog fragment written. Next: merge origin/main, push, PR, Copilot.
- Coordinator follow-up (msg_7bf21fb00ea3): added `as_table`/`head` during an open read and
  an abandoned read followed by a reset. The latter found a gap two links up (`s <- s1 <- p`,
  only `s1`'s generation noted): the in-progress rule now also asks a pod-stream input whether
  it is stale. Merge origin/main after #358 (as_table clears a stale cache) and rerun.
- The ITL-1034 branch's strict `ITL_1065` xfail marks will XPASS once this merges: that branch
  must drop them.
