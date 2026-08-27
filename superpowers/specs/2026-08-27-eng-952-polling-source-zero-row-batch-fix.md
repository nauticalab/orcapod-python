# ENG-952: Fix PollingSource crash on zero-row batch after nullable column

**Date:** 2026-08-27
**Issue:** ENG-952 — Fix upstream: PollingSource dies when a poll returns a zero-row batch
**Status:** Implemented

---

## Overview

`PollingSource` terminates with `SchemaInconsistencyError` when a poll returns a batch with
zero rows, if any earlier batch contained a null in some column. The source infers nullability
per batch from actual null counts. A zero-row batch always has `null_count == 0` in every
column, so every field is inferred non-nullable — contradicting the accumulated stream's
nullable schema.

This is the common path for `WindowDiscoverySource` (the steady state is zero new rows per
poll), occurring roughly 144 times a day per pipeline stage at a 10-minute interval.

---

## Root cause

Three components interact:

1. **`_try_build_stream`** (`polling_source.py:548`) — only returns `None` for data with no
   *columns*. A zero-row DataFrame that carries its columns passes through and becomes a real
   `ArrowTableStream`.

2. **`infer_schema_nullable`** (`arrow_utils.py:967`) — sets `nullable = column.null_count > 0`.
   For a zero-row table, `null_count == 0` for every column, so every field is inferred
   `nullable=False` regardless of the real schema.

3. **`_combine`** → **`_validate_combining_schemas`** — compares the zero-row stream's
   non-nullable schema against the accumulated stream's nullable schema and raises
   `SchemaInconsistencyError`.

`SchemaInconsistencyError` is an `InputValidationError`, which `async_iter_data` re-raises
immediately (not charged against `max_consecutive_errors`). One zero-row poll kills the source.

---

## Chosen fix: Fix B — zero-row guard in `_combine`

Add an early-return guard at the top of `PollingSource._combine`: if `new_stream` has zero
rows, return `existing` unchanged — skip validation and concatenation entirely.

```python
def _combine(self, existing, new_stream):
    if new_stream.as_table().num_rows == 0:
        logger.debug(
            "PollingSource %r: zero-row batch — skipping combine", self._source_id
        )
        return existing
    self._validate_combining_schemas(existing, new_stream)
    ...
```

### Why Fix B over the alternatives

**Fix A** (return `None` from `_try_build_stream` for zero-row frames) was measured to regress
the first-fetch-empty case: `keys()` raises `ValueError: no data available yet`. A narrowed
version (skip only when `_accumulated_stream` is already set) would work, but it introduces
hidden state coupling into a method whose docstring says it should be stateless.

**Fix C** (change `infer_schema_nullable` to not use `null_count`) would affect
`pipeline_identity_structure`, moving cache identity — a much larger change than this defect
warrants.

**Fix B properties:**

- `_combine` is only called after `_accumulated_stream` is populated (the first-fetch-empty
  regression from Fix A cannot happen here).
- Strictly cheaper on the common poll: skips `_validate_combining_schemas`,
  `pa.concat_tables`, and `ArrowTableStream` construction for every zero-row batch.
- Matches the reference shim in `orcapod-sync-and-qc` (`StreamingPollingSource._combine`)
  that has been running in production.

### Interaction with PR #260 (ITL-617)

PR #260 (`_accumulated_stream` → optimistic-lock batch list) is open and conflicts at
`_combine`. Its checklist states `_combine` is unchanged, and its async loop stops calling
`_combine` entirely — it appends and validates directly against `_batches[0]`. If PR #260
lands before this fix, Fix B must be re-expressed against `_validate_combining_schemas`
(or `_try_build_stream` narrowed). Landing this fix first is cheaper.

---

## Test plan

Add class `TestPollingSourceZeroRowBatch` to `tests/test_channels/test_polling_source.py`
with two async tests:

1. **`test_zero_row_batch_after_nullable_column_streams_cleanly`** — impl emits one row with a
   nullable column (containing `None`), then zero-row frames for the remaining duration. Assert
   the source completes without exception and emits exactly 1 row.

2. **`test_zero_row_batch_is_not_accumulated`** — same impl; assert `_accumulated_stream`
   still contains only the original row after zero-row polls (zero-row batches are not
   concatenated).

The impl pattern follows the inline-class style already used by `DriftingImpl` in
`test_schema_mismatch_raises_on_column_change`.

---

## DESIGN_ISSUES.md

Add new entry **PS4** under `src/orcapod/core/sources/polling_source.py`:

> ### PS4 — `PollingSource` dies when a poll returns a zero-row batch after a nullable column
> **Status:** resolved
> **Severity:** high
> **Issue:** ENG-952

---

## Completion criteria (from Linear issue)

- [x] Upstream issue filed against orcapod-python (this spec + PR)
- [x] PR merged to `main` with regression test covering zero-row-after-null
- [ ] `orcapod-sync-and-qc` pin bumped, trigger test observed failing, `_combine` override deleted
  (handled separately in ENG-935 after merge)
