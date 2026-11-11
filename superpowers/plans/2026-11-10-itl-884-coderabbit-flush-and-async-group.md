# ITL-884 follow-up: store errors keep their type through `flush()` and out of the async TaskGroup

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Superseded in part (November 11, 2026):** the developer's review of PR #290 removed every
> parameter used only for an error message. `_read_delta_table` no longer takes `record_path`,
> `_require_compatible_append(table_uri, ...)` became `_incompatible_append_column(...)`,
> `_store_error_to_raise` takes only the errors, and `_single_store_error` no longer takes
> `topo_order`. See `2026-11-11-itl-884-no-message-only-parameters.md`.

**Goal:** Answer the two open CodeRabbit threads on PR #290 (ids 4534230764 and 4534230775).
A `StoreReadError` or an append-refusal `InputValidationError` raised while flushing reaches the
caller as itself, so a store read failure in a per-row result-cache flush fails the run under
`error_policy="continue"`. The async orchestrator raises a lone `StoreReadError` even when an
unrelated node failed in the same `TaskGroup`.

**Architecture:** `DeltaTableDatabase.flush()` keeps attempting every pending batch and logging
each failure. When at least one batch failed with a store error, it raises the first
`StoreReadError` (else the first `InputValidationError`) itself, with a note for every other
failed batch. Only non-store failures keep the `RuntimeError` wrapper. The async orchestrator
replaces `except* StoreReadError` with a plain `except ExceptionGroup` that selects one
`StoreReadError` (`_single_store_error`), notes and logs the rest, and raises it, in the same
shape as PR #287's `_single_store_error` for `StoreWriteError` so the second PR to merge has a
trivial conflict.

**Tech Stack:** deltalake 1.0.2, pyarrow, asyncio `TaskGroup`, pytest.

Approved by the coordinator (2026-11-10) on condition that each finding reproduces. Both do:

- (1b) Run a job, then run it again over two new rows, with the function corrupting the result
  table's log (a new log version that requires an unknown reader feature) during its first call.
  The per-row lookup still succeeds, the result-cache flush raises `StoreReadError` wrapped in
  `RuntimeError`, the sync per-row handler under `continue` skips the row, the next row is
  computed, and the run ends with `RuntimeError` from the job's end-of-run flush.
- (2) CI's 3.11 is 3.11.18, so the 3.11.0-3.11.3 single-leaf wrapping does not occur there. The
  concurrent case reproduces deterministically on 3.11.18 and 3.13.18: node `dbl` raises
  `StoreReadError` in `_load_cached_entries` and node `tri` fails in the observer's
  `on_node_start`, both before their first `await`, so both land in one group and `job.run`
  raises `ExceptionGroup([StoreReadError, ExceptionGroup([ValueError])])`.

No hash, path or stored-schema changes. No public API added.

---

## File map

- `src/orcapod/databases/delta_lake_databases.py`: `flush()` and a module helper
  `_store_error_to_raise(errors)`.
- `src/orcapod/pipeline/async_orchestrator.py`: plain `except ExceptionGroup`, helpers
  `_single_store_error(group, topo_order)` and `_leaves(group)` (identical to #287's `_leaves`).
- Tests: `tests/test_databases/test_delta_read_failures.py` (assert the bare error types, add a
  multi-batch test), `tests/test_pipeline/test_store_read_failure.py` (flush during per-row
  processing, both orchestrators; concurrent unrelated failure, async).
- Docs: `docs/concepts/function-pods.md` admonition, `CHANGELOG.md` ITL-884 entries,
  `DESIGN_ISSUES.md` DR1 fix note.

## Task 1: Failing tests for finding (1)

- [ ] `tests/test_databases/test_delta_read_failures.py`: the `refusal` helper is replaced by
  `pytest.raises(InputValidationError)` at each refusal site; the two unloadable-table flush tests
  use `pytest.raises(StoreReadError)`; the absent `__cause__` assertions go.
- [ ] New `test_flush_raises_the_store_error_and_notes_every_other_failed_batch`: two tables
  with pending batches (one whose stored `x` is int64 and receives float64, one whose log requires
  an unknown reader feature); `flush()` raises the `StoreReadError`, one note names the other
  table's key and `InputValidationError`, and both batches stay queued (a second `flush()` raises
  again).
- [ ] New `test_flush_keeps_runtime_error_for_other_failures`: control case. Monkeypatch
  `deltalake.write_deltalake` to raise `OSError` for a new table; `flush()` raises `RuntimeError`
  with the `OSError` as `__cause__` (unchanged behaviour).
- [ ] `tests/test_pipeline/test_store_read_failure.py`:
  `test_store_read_failure_in_a_row_flush_fails_the_run`, both orchestrators with
  `error_policy="continue"`. Rerun over `a..e` with the function corrupting the result table on
  its first call; `job.run` raises `StoreReadError` naming the result table, and the function ran
  for exactly one new row.
- [ ] Run `uv run pytest tests/test_databases/test_delta_read_failures.py
  tests/test_pipeline/test_store_read_failure.py -v`. Expected: the new and changed tests FAIL with
  `RuntimeError`, and the per-row test also shows two calls.

## Task 2: Fix `flush()`

```python
def flush(self) -> None:
    errors: list[tuple[str, Exception]] = []
    for record_key in list(self._pending_batches.keys()):
        try:
            self._flush_by_key(record_key)
        except Exception as e:
            logger.error(f"Error flushing batch for {record_key}: {e}")
            errors.append((record_key, e))
    if errors:
        store_error = _store_error_to_raise(errors)
        if store_error is not None:
            raise store_error
        failed_keys = [k for k, _ in errors]
        raise RuntimeError(
            f"Failed to flush {len(errors)} batch(es): {failed_keys}"
        ) from errors[0][1]


def _store_error_to_raise(errors):
    """First StoreReadError, else first InputValidationError, with notes; else None."""
    for kind in (StoreReadError, InputValidationError):
        chosen = next((e for _, e in errors if isinstance(e, kind)), None)
        if chosen is not None:
            break
    else:
        return None
    for key, other in errors:
        if other is not chosen:
            chosen.add_note(
                f"Another batch also failed to flush ({key}): "
                f"{type(other).__name__}: {other}"
            )
    return chosen
```

`StoreReadError` is preferred because it is the one the per-row handlers treat as fatal. The
docstring gets a Raises section. Run Task 1's tests: PASS.

## Task 3: Failing test for finding (2)

- [ ] `test_concurrent_unrelated_failure_still_raises_store_read_error` (async only): an
  observer (a `NoOpObserver` subclass whose `contextualize` returns itself and whose
  `on_node_start` raises `ValueError` for `tri`), two function nodes over one source, the `dbl`
  result table's data files deleted. `job.run` raises `StoreReadError` (not a group) naming the
  result table, `__cause__` is set, and a note names `ValueError: observer failed for tri`.
- [ ] Run it: FAIL with `ExceptionGroup`.

## Task 4: Fix the async orchestrator

```python
        except ExceptionGroup as group:
            # A failed store read stops the run. Raise it on its own, as the sync
            # orchestrator does, rather than inside the TaskGroup's exception group.
            # A plain except (not except*) so the raise is never wrapped again.
            store_error = _single_store_error(group, topo_order)
            if store_error is None:
                raise
            raise store_error
```

`_single_store_error(group, topo_order)`: `StoreReadError.in_group(group)`; none gives `None`; the first
is chosen; every other leaf (`_leaves`) is noted on it ("Another store read failed in the same
run: ..." also logged at WARNING; "Another node failed in the same run: <type>: <msg>"). Its
`__cause__` stays the backend error; the group is its `__context__`. Run: PASS.

## Task 5: Docs, design log, verification, commit, replies

- [ ] Concept page and CHANGELOG: `flush()` / `add_records(..., flush=True)` raise
  `StoreReadError` and `InputValidationError` themselves; when several batches fail, the first
  store error is raised with a note per other batch, every failed batch stays queued and each
  failure is logged; other flush failures still raise `RuntimeError`.
- [ ] `DESIGN_ISSUES.md` DR1: one sentence on the flush and async-group fix.
- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`, basedpyright (exit code), the full
  suite on 3.11, 3.12 and 3.13, `tests/test_hashing/golden` unchanged.
- [ ] One commit per finding, push, signed replies on both threads, wait for CI and CodeRabbit.

## Interaction with PR #287 (report, do not fix here)

#287's `ResultCache.store` wraps every `add_record`/`flush` failure in `StoreWriteError`, and its
`PipelineJob.run` wraps the end-of-run flush the same way. Once both PRs are in, a
`StoreReadError` from a flush reaches the caller as the `__cause__` of a `StoreWriteError`. It
is still fatal, but `except StoreReadError` misses it. Whichever PR merges second should decide
whether those wrappers let `StoreReadError` through.
