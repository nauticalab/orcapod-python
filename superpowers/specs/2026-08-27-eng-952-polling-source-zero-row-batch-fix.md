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

## Design principle

The deeper problem is that `_build_stream_from_df` re-infers the Arrow schema nullability on
**every batch**, from that batch's own data. This is wrong: the schema is a property of the
source, not of any individual batch. A zero-row batch, a null-free batch, and a batch with
nulls all represent data from the same source — they should produce streams with the same
schema.

The fix establishes a **canonical Arrow schema** exactly once and applies it to every
subsequent batch. There are two paths:

- **Declared-schema path** — when `impl.schema()` returns a non-`None` ``Schema``, the
  canonical Arrow schema is derived from those Python type annotations at construction time.
  `T | None` maps to `nullable=True`; plain `T` maps to `nullable=False`. No inference
  happens.

- **Infer-once path** — when `impl.schema()` returns `None`, the canonical schema is inferred
  from the **first** batch (which contains real data, so inference is meaningful). A
  `WARNING`-level log is emitted to prompt the caller to declare a schema. All subsequent
  batches are cast to the canonical schema instead of re-inferring.

This eliminates the zero-row crash, the "residual" nullability drift on null-free batches, and
the need for the `_combine` short-circuit (Fix B) that the issue originally recommended.

---

## Implementation

### New attribute

Add to `PollingSource.__init__`:

```python
self._canonical_arrow_schema: pa.Schema | None = None
```

### Modified `_build_stream_from_df`

Replace the single line:

```python
arrow_table = arrow_table.cast(arrow_utils.infer_schema_nullable(arrow_table))
```

with:

```python
# Establish canonical schema on first call; apply it on every call.
if self._canonical_arrow_schema is None:
    if self._tag_schema is not None and self._data_schema is not None:
        # Declared-schema path: derive Arrow schema from declared Python types.
        # T | None → nullable=True; plain T → nullable=False. No inference.
        combined = {**dict(self._tag_schema), **dict(self._data_schema)}
        self._canonical_arrow_schema = (
            self.data_context.type_converter.python_schema_to_arrow_schema(combined)
        )
    else:
        # Infer-once path: first batch establishes canonical nullability.
        logger.warning(
            "PollingSource %r: no schema declared via impl.schema(); "
            "inferring nullability from first batch. Implement impl.schema() "
            "to avoid schema drift on zero-row polls or null-free batches.",
            self._source_id,
        )
        self._canonical_arrow_schema = arrow_utils.infer_schema_nullable(arrow_table)

# Apply canonical nullability by column name (order-safe).
canonical_nullable = {f.name: f.nullable for f in self._canonical_arrow_schema}
target_schema = pa.schema([
    pa.field(f.name, f.type, nullable=canonical_nullable.get(f.name, f.nullable))
    for f in arrow_table.schema
])
arrow_table = arrow_table.cast(target_schema)
```

The cast overrides only the `nullable` flag; the Arrow type (from Polars conversion) is
preserved. Column matching is done by name, not position, so it is safe even if column order
in the DataFrame ever differs from the declared schema's field order.

### No changes elsewhere

- `_try_build_stream` — unchanged. Zero-row frames still pass through; the canonical-schema
  cast in `_build_stream_from_df` gives them the correct schema.
- `_combine` / `_validate_combining_schemas` — unchanged. Schema is now consistent across
  batches, so the comparison passes correctly.
- `async_iter_data` / `_run_sync` — unchanged.

---

## Test plan

Add class `TestPollingSourceZeroRowBatch` to `tests/test_channels/test_polling_source.py`
with four async or sync tests:

1. **`test_zero_row_batch_after_nullable_column_streams_cleanly`** — impl emits one row with a
   nullable column (containing `None`), then zero-row frames for the remaining duration. Assert
   the source completes without exception and emits exactly 1 row. (Regression for the exact
   repro in the Linear issue; no declared schema → infer-once path.)

2. **`test_zero_row_batch_is_not_accumulated`** — same impl; assert `_accumulated_stream`
   contains only the original row after zero-row polls.

3. **`test_declared_schema_no_inference_warning`** — impl declares a schema with a nullable
   field, emits a null-bearing row, then zero-row frames. Assert the source streams cleanly
   **and** no WARNING is emitted. Verifies the declared-schema path skips inference entirely.

4. **`test_infer_schema_emits_warning`** — impl with `schema()` returning `None` emits one
   row. Assert that a `WARNING` log containing `"inferring nullability from first batch"` is
   emitted exactly once (not on subsequent polls).

The impl pattern follows the inline-class style used in `test_schema_mismatch_raises_on_column_change`.

---

## DESIGN_ISSUES.md

Add new entry **PS4** under `src/orcapod/core/sources/polling_source.py`:

> ### PS4 — `PollingSource` re-infers Arrow schema nullability per batch, crashing on zero-row polls
> **Status:** resolved
> **Severity:** high
> **Issue:** ENG-952
>
> `_build_stream_from_df` called `infer_schema_nullable` on every batch. A zero-row batch has
> `null_count == 0` for all columns, so every field was inferred non-nullable.
> `_validate_combining_schemas` then rejected the batch against the accumulated stream's
> nullable schema.
>
> **Fix:** `_build_stream_from_df` now establishes a `_canonical_arrow_schema` exactly once —
> from `impl.schema()` when declared (no inference, no warning), or from the first batch
> otherwise (with a `WARNING`-level log). All subsequent batches are cast to the canonical
> schema by column name. Per-batch nullability inference is eliminated.

---

## Completion criteria (from Linear issue)

- [x] Upstream issue filed against orcapod-python (this spec + PR)
- [x] PR merged to `main` with regression test covering zero-row-after-null
- [ ] `orcapod-sync-and-qc` pin bumped, trigger test observed failing, `_combine` override deleted
  (handled separately in ENG-935 after merge)
