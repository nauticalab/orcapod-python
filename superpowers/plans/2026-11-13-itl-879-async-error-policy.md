# ITL-879 Async error_policy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `error_policy` means the same on `AsyncPipelineOrchestrator` as on `SyncPipelineOrchestrator`: `fail_fast` raises the failing row's own exception (same type and message as sync), `continue` skips and reports the row as it already does; an async run never raises an `ExceptionGroup` to its caller.

**Architecture:** Option A (coordinator-approved, November 13, 2026): a keyword-only
`error_policy: Literal["continue", "fail_fast"] = "continue"` on
`FunctionNodeProtocol.async_execute`, `FunctionJobNode.async_execute` and
`_FunctionPodBase.async_execute`, mirroring `execute`. The pod's per-row handler re-raises a
row failure under `fail_fast`; the existing task groups cancel the rest and every node closes
its output in its existing `finally`. The orchestrator's `except ExceptionGroup` handler picks
ONE exception: store errors, then `TypeIdentityError` (both unchanged), then the failure of
the node earliest in topological order (ties: group order = completion order); the others are
added to it as notes. The orchestrator's cancel/close machinery is not changed.

**Tech Stack:** Python 3.11+, asyncio `TaskGroup`, pytest.

## Measured on main b4fde330 (before)

6 rows, row r2 raises `ValueError("bad x=2")`, function node `f` -> function node `down`.

| | sync continue | async continue | sync fail_fast | async fail_fast |
|---|---|---|---|---|
| raises | no | no | `ValueError: bad x=2` (no cause, no notes) | **no** |
| RunSummary f | 5 computed, 1 failed | same | none (last_run unchanged) | **5 computed, 1 failed** |
| RowFailuresWarning | 1 | 1 | none | **1** |
| per-row log | DEBUG function_node | DEBUG function_pod | DEBUG | DEBUG |
| observer | crash r2, end x5, node_end f, down | same (interleaved) | end r0 r1, crash r2, node_end f, run_end; down never starts | **as continue** |
| stored / held f | r0 r1 r3 r4 r5 | same | r0 r1 | **r0 r1 r3 r4 r5** |
| stored / held down | r0 r1 r3 r4 r5 | same | nothing | **r0 r1 r3 r4 r5** |

SideEffectPod (own `on_error`, not `error_policy`): `on_error="raise"`: sync raises bare
`ValueError`, async raises `ExceptionGroup([ValueError])`. `on_error="log"`: identical
(WARNING log, other rows pass).

## Storage guarantee (approved wording)

"After a fail_fast raise, every stored row is a correct, complete result of its own input
row, written in the ITL-878 commit order (results before pipeline records; writes that
succeeded are flushed before the raise). The failing row is never stored. Other rows of the
failing node, before or after it in input order, rows that downstream nodes computed from its
successful rows, and rows of other branches may be stored; the set stored is not specified
beyond that. When the cancellation falls between a row's result write and its pipeline-record
write, the result is stored without its record: a re-run reuses it without calling the
function, as after a crash."

## Files

- Modify `src/orcapod/protocols/node_protocols.py` (`FunctionNodeProtocol.async_execute`).
- Modify `src/orcapod/core/function_pod.py` (`_FunctionPodBase.async_execute`).
- Modify `src/orcapod/core/nodes/function_node.py` (`FunctionJobNode.async_execute`).
- Modify `src/orcapod/pipeline/async_orchestrator.py` (pass the policy; `_start_node` records
  which node raised; `_single_failure` picks the exception).
- Modify `src/orcapod/utils/store_errors.py` (`store_error_of` helper for the node's
  `on_node_end` on failure).
- Create `tests/test_pipeline/test_async_error_policy.py`.
- Docs: `docs/concepts/function-pods.md`, `README.md` diagram if it states async behaviour.
- `changelog.d/ITL-879.md`, `design_issues/ITL-878-SW1.md` note, new
  `design_issues/ITL-879-EP1.md` entry (resolved).

## Tasks

### Task 1: failing tests (TDD)

`tests/test_pipeline/test_async_error_policy.py`, public interface only (PipelineJob.run /
run_async), except the channel-closed check (patched `Channel`/`BroadcastChannel` in
`orcapod.pipeline.async_orchestrator`, private `_closed` read; commented).

- `test_continue_parity[sync|async]` x scenarios fn / sef: no raise, RunSummary counts,
  one RowFailuresWarning, observer crash event, rows of f and down = all but the failing row.
- `test_fail_fast_raises_the_row_error[sync|async]` x fn / sef: `pytest.raises(ValueError)`,
  `type(err) is ValueError`, `str(err) == "bad x=2"`, not an ExceptionGroup, `job.last_run is
  None`, no RowFailuresWarning, observer got `on_data_crash` for the failing row and
  `on_node_end` for f.
- `test_fail_fast_stored_rows_are_correct[async]`: every row a fresh job reads for f and down
  is a correct result (y == x, z == 2x) and the failing row is absent.
- `test_downstream_never_completes_on_partial_input` parametrized failing row first / middle /
  last x max_concurrency 1 / None: async fail_fast raises; `down` gets no `on_node_end`.
- `test_max_concurrency_one_raises_the_first_row_in_input_order`: two failing rows, first one
  raised; the second is not computed (sync and async).
- `test_side_effect_pod_raise[sync|async]` x policies: bare `ValueError`, not a group.
- `test_side_effect_pod_log[sync|async]`: no raise, WARNING log, other rows pass.
- `test_nothing_left_running_after_fail_fast`: `await job.run_async(AsyncPipelineOrchestrator(
  error_policy="fail_fast"))` inside `async with asyncio.timeout(60)`, then
  `asyncio.all_tasks() - {asyncio.current_task()} == set()` and every channel created closed.
- `test_two_nodes_fail_raises_the_upstream_failure`: async, both f and a downstream fail_fast
  node fail; raised is f's, the other appears in `__notes__` (only when it happened).

Run: `uv run pytest tests/test_pipeline/test_async_error_policy.py -q` -> the async fail_fast
and side-effect-raise cases FAIL; continue and sync cases PASS. Commit (WIP test commit).

### Task 2: policy through the async path

1. `node_protocols.py`: add `error_policy: Literal["continue", "fail_fast"] = "continue"` to
   `FunctionNodeProtocol.async_execute` (keyword-only, after `observer`).
2. `function_pod.py` `_FunctionPodBase.async_execute`: same keyword; docstring; in
   `process_one`'s `except Exception` after `obs.on_data_crash(...)`:
   ```python
   if error_policy == "fail_fast":
       raise
   ```
3. `function_node.py` `FunctionJobNode.async_execute`: same keyword; pass
   `error_policy=error_policy` to `execution_pod.async_execute` and to the no-DB
   `self._function_pod.async_execute`. Track `node_started = True` after `on_node_start`; in
   `except Exception as run_error:` before `_commit_after_failure`:
   ```python
   if node_started:
       # The sync execute ends the node for the observer before it raises.
       with keep_store_error(store_error_of(run_error), "Observer on_node_end"):
           ctx_obs.on_node_end(node_label, node_hash)
   ```
4. `store_errors.py`: `store_error_of(error)` returns the `StoreError`/`TypeIdentityError`
   that is `error` or the first one inside it when it is an exception group, else `None`.
5. `async_orchestrator.py`: pass `error_policy=self._error_policy` to function and
   side-effect-function nodes' `async_execute`.

### Task 3: one exception from an async run

`async_orchestrator.py`:
- `_start_node(tg, node, work, failed_nodes)`: on `except Exception as exc` record
  `failed_nodes[id(leaf)] = node.label` for every leaf of `exc`, then re-raise (store-error
  annotation unchanged).
- Handler: after the store-error and identity-error branches,
  `raise _single_failure(group, topo_order, failed_nodes)`: the leaf whose node ranks first
  in `topo_order`, ties by group order; each other leaf noted as
  `f"Node {label!r} also failed in the same run: {type(other).__name__}: {other}"` (label
  omitted when unknown).
- Docstrings of `run`/`run_async`: "Raises: the exception of the failing node itself, never an
  exception group".

Run the new tests -> PASS. Run targeted existing suites (one pytest process):
`tests/test_pipeline/ tests/test_core/function_pod/ tests/test_core/side_effect_function/
tests/test_channels/` on 3.11 and 3.12 for the new file. Commit.

### Task 4: docs, changelog, design issues

- `docs/concepts/function-pods.md`: replace the two "async ... ITL-879" sentences; next to the
  `fail_fast` paragraph state behaviour on both orchestrators, "first" under concurrency, and
  the storage guarantee verbatim. Run `uv run pytest tests/test_docs -q -k function_pods`.
- `changelog.d/ITL-879.md`: `### Breaking changes and migration` (async fail_fast now raises;
  async runs raise the node's exception, never an ExceptionGroup) and `### Fixed`.
  `uv run python scripts/assemble_changelog.py --check`.
- `design_issues/ITL-878-SW1.md`: the "unchanged here" sentence gains "(fixed by ITL-879)".
  New `design_issues/ITL-879-EP1.md` (resolved, with Fix note).
  `uv run python scripts/design_issues.py --check`.
- ruff format/check, basedpyright exit code.

### Task 5: finish

Merge origin/main (after #378 if merged), rerun targeted tests, full suite once, push, draft
PR "Fixes ITL-879", CodeRabbit process per brief.

## Notes for a successor

- Proposal approved by coordinator (Option A, unwrap every node failure, guarantee wording).
- Merge order: PR #378 (ITL-1093) first; conflicts expected in `FunctionJobNode.async_execute`
  except/finally region. Its rule: a run that raised leaves the node holding nothing.
- If the downstream-partial-input test ever fails: STOP, ask coordinator (would need
  close-with-error channels).
