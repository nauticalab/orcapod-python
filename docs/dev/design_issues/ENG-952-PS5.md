## `src/orcapod/core/sources/polling_source.py`

### PS5 — `PollingSource` re-infers Arrow schema nullability per batch, crashing on zero-row polls
**Status:** resolved
**Severity:** high
**Issue:** ENG-952

`_build_stream_from_df` called `infer_schema_nullable` on every batch. A zero-row batch has
`null_count == 0` for all columns, so every field was inferred non-nullable.
`_validate_combining_schemas` then rejected the batch against the accumulated stream's
nullable schema.

**Fix:** `_build_stream_from_df` now establishes a `_canonical_arrow_schema` exactly once —
from `impl.schema()` when declared (no inference, no warning), or from the first non-empty
batch otherwise (with a `WARNING`-level log). All subsequent batches are cast to the canonical
schema by column name. Per-batch nullability inference is eliminated. Zero-row frames before
canonical schema establishment are skipped on the infer-once path.
