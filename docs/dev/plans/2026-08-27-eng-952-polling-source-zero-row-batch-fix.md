# ENG-952: PollingSource Zero-Row Batch Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `PollingSource` crashing with `SchemaInconsistencyError` on zero-row polls by establishing a canonical Arrow schema once and applying it to every batch.

**Architecture:** Add `_canonical_arrow_schema: pa.Schema | None` to `PollingSource.__init__`. Modify `_build_stream_from_df` to set the canonical schema on the first call (from `impl.schema()` when declared, or inferred from the first batch with a `WARNING`) and cast every subsequent batch to it by column name. No other methods change.

**Tech Stack:** PyArrow (`pa.Schema`, `Table.cast`), Polars (`pl.DataFrame`), pytest-asyncio, pytest `caplog` fixture.

---

## Files

| File | Change |
|------|--------|
| `src/orcapod/core/sources/polling_source.py` | Add `_canonical_arrow_schema` attribute; rewrite the single `infer_schema_nullable` call in `_build_stream_from_df` |
| `tests/test_channels/test_polling_source.py` | Add `TestPollingSourceZeroRowBatch` class with four tests |
| `DESIGN_ISSUES.md` | Add PS4 entry (resolved) |

---

### Task 1: Write the four failing tests

**Files:**
- Modify: `tests/test_channels/test_polling_source.py` (append after line 1144)

- [ ] **Step 1: Append `TestPollingSourceZeroRowBatch` to the test file**

Add the following class at the very end of `tests/test_channels/test_polling_source.py` (after the `_drain_async` helper):

```python
# ===========================================================================
# Task N: Zero-row batch regression (ENG-952)
# ===========================================================================


class TestPollingSourceZeroRowBatch:
    """Regression tests for ENG-952.

    PollingSource must not crash when a poll returns a zero-row batch after
    a batch that contained a null value in some column.  The root cause was
    per-batch nullability re-inference: a zero-row table has null_count == 0
    for every column, so every field was inferred non-nullable, conflicting
    with the accumulated stream's nullable schema.

    The fix establishes a canonical Arrow schema once (from impl.schema() when
    declared, or from the first batch otherwise) and casts every subsequent
    batch to it by column name.
    """

    # ------------------------------------------------------------------
    # Shared impl used by tests 1 and 2 (infer-once path, no declared schema)
    # ------------------------------------------------------------------

    @staticmethod
    def _make_emit_once_then_empty_impl():
        """Return a DynamicSourceProtocol impl that emits one nullable row then empty frames."""

        class EmitOnceThenEmpty:
            """Emits one row with a null 'note' on fetch 1, then zero-row frames."""

            def __init__(self):
                self.n = 0

            def identity(self):
                return ("EmitOnceThenEmpty",)

            def to_config(self):
                return None

            @classmethod
            def from_config(cls, config):
                raise NotImplementedError

            def schema(self):
                return None  # no declared schema — exercises the infer-once path

            async def poll(self, cursor=None):
                return True  # always claims new data

            async def fetch(self, cursor=None):
                self.n += 1
                if self.n == 1:
                    # First fetch: one row, nullable 'note' column contains None.
                    data = {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([1.0], type=pa.float64()),
                        "note": pa.array([None], type=pa.large_utf8()),
                    }
                else:
                    # Subsequent fetches: zero-row frame, same columns.
                    data = {
                        "id": pa.array([], type=pa.int64()),
                        "val": pa.array([], type=pa.float64()),
                        "note": pa.array([], type=pa.large_utf8()),
                    }
                return Cursor.now(self.n), data

            async def close(self):
                return None

        return EmitOnceThenEmpty()

    # ------------------------------------------------------------------
    # Test 1: core regression — source must not crash
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_zero_row_batch_after_nullable_column_streams_cleanly(self):
        """Zero-row poll after a nullable column must not raise SchemaInconsistencyError.

        This is the exact scenario from ENG-952: fetch 1 returns a row with a
        null in 'note' (inferred nullable=True); fetches 2+ return zero-row frames
        (would be inferred nullable=False without the fix).  The source must stream
        the single real row and then terminate cleanly after the duration expires.
        """
        src = PollingSource(
            self._make_emit_once_then_empty_impl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        rows = []
        async for tag, data in src.async_iter_data():
            rows.append((tag, data))

        assert len(rows) == 1

    # ------------------------------------------------------------------
    # Test 2: zero-row batches must not accumulate
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_zero_row_batch_is_not_accumulated(self):
        """After zero-row polls, the internal accumulated stream must hold only the real rows."""
        src = PollingSource(
            self._make_emit_once_then_empty_impl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        async for _ in src.async_iter_data():
            pass

        assert src._accumulated_stream is not None
        cached = list(src._accumulated_stream.iter_data())
        assert len(cached) == 1

    # ------------------------------------------------------------------
    # Test 3: declared-schema path — zero-row polls, no warning
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_declared_schema_zero_row_batch_no_warning(self, caplog):
        """Declared-schema path: zero-row polls stream cleanly and emit no WARNING.

        When impl.schema() returns a Schema, nullability is derived from the
        Python type annotations (str | None → nullable=True) without inference.
        No warning must be logged even when the first batch carries a null.
        """

        class DeclaredNullableImpl:
            def __init__(self):
                self.n = 0

            def identity(self):
                return ("DeclaredNullableImpl",)

            def to_config(self):
                return None

            @classmethod
            def from_config(cls, config):
                raise NotImplementedError

            def schema(self):
                # note is declared nullable via str | None
                return Schema({"id": int, "val": float, "note": str | None})

            async def poll(self, cursor=None):
                return True

            async def fetch(self, cursor=None):
                self.n += 1
                if self.n == 1:
                    data = {
                        "id": pa.array([1], type=pa.int64()),
                        "val": pa.array([1.0], type=pa.float64()),
                        "note": pa.array([None], type=pa.large_utf8()),
                    }
                else:
                    data = {
                        "id": pa.array([], type=pa.int64()),
                        "val": pa.array([], type=pa.float64()),
                        "note": pa.array([], type=pa.large_utf8()),
                    }
                return Cursor.now(self.n), data

            async def close(self):
                return None

        src = PollingSource(
            DeclaredNullableImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with caplog.at_level(logging.WARNING, logger="orcapod.core.sources.polling_source"):
            rows = []
            async for tag, data in src.async_iter_data():
                rows.append((tag, data))

        assert len(rows) == 1
        inference_warnings = [
            r for r in caplog.records if "inferring nullability" in r.message
        ]
        assert len(inference_warnings) == 0, (
            "Declared-schema path must not emit an inference warning"
        )

    # ------------------------------------------------------------------
    # Test 4: infer-once path emits exactly one WARNING
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_infer_once_emits_exactly_one_warning(self, caplog):
        """When impl.schema() returns None, exactly one WARNING is logged for schema inference.

        The warning must be emitted on the first batch only — not on every subsequent
        poll — because _canonical_arrow_schema is set after the first call.
        """
        # Two batches so _build_stream_from_df is called twice; warning fires only once.
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.05, duration=0.5, max_missed_intervals=50),
        )

        with caplog.at_level(logging.WARNING, logger="orcapod.core.sources.polling_source"):
            async for _ in src.async_iter_data():
                pass

        inference_warnings = [
            r for r in caplog.records if "inferring nullability" in r.message
        ]
        assert len(inference_warnings) == 1, (
            f"Expected exactly one inference warning, got {len(inference_warnings)}"
        )
```

- [ ] **Step 2: Run all four new tests to confirm they fail**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceZeroRowBatch -v
```

Expected: all four tests **FAIL**.

- Tests 1–3 should fail with `SchemaInconsistencyError` (or the underlying error propagation).
- Test 4 should fail with `AssertionError: Expected exactly one inference warning, got 0`.

If any test passes before the fix is applied, stop and investigate — the test may not be covering the right behaviour.

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_channels/test_polling_source.py
git commit -m "test(polling_source): add failing regression tests for ENG-952 zero-row batch crash"
```

---

### Task 2: Implement the canonical-schema fix in `PollingSource`

**Files:**
- Modify: `src/orcapod/core/sources/polling_source.py`

- [ ] **Step 1: Add `_canonical_arrow_schema` attribute to `__init__`**

In `PollingSource.__init__`, add one line immediately after the `_accumulated_stream` assignment (line 258):

```python
        self._accumulated_stream: ArrowTableStream | None = None
        self._canonical_arrow_schema: pa.Schema | None = None   # add this line
```

The full surrounding context (lines 255–262) should look like:

```python
        self._impl: DynamicSourceProtocol[T] = impl
        self._tag_columns: tuple[str, ...] = tuple(_normalize_column_list(tag_columns))
        self._polling_config = polling_config
        self._cursor: Cursor[T] | None = None
        self._accumulated_stream: ArrowTableStream | None = None
        self._canonical_arrow_schema: pa.Schema | None = None
        # Derive source_id from impl identity if not explicitly provided
        if self._source_id is None:
            self._source_id = str(self._impl.identity())
```

- [ ] **Step 2: Replace the single `infer_schema_nullable` call in `_build_stream_from_df`**

Locate line 578 in `polling_source.py`:

```python
        arrow_table = arrow_table.cast(arrow_utils.infer_schema_nullable(arrow_table))
```

Replace it with the following block (same indentation — 8 spaces):

```python
        # ------------------------------------------------------------------
        # Establish or apply the canonical Arrow schema.
        #
        # The schema is a property of the *source*, not of any individual
        # batch.  A zero-row batch, a null-free batch, and a batch with nulls
        # all represent data from the same source and must produce streams
        # with identical nullability.
        #
        # Two paths:
        #   Declared — impl.schema() returned a non-None Schema at
        #     construction, so _tag_schema / _data_schema are populated.
        #     Derive the Arrow schema from the Python type annotations:
        #     T | None → nullable=True; plain T → nullable=False.
        #     No inference; no warning.
        #   Infer-once — impl.schema() returned None.  Infer nullability from
        #     the first batch (which contains real data, so inference is
        #     meaningful) and cache the result.  Emit a WARNING to prompt the
        #     caller to declare a schema.
        #
        # All subsequent batches are cast to the canonical schema by column
        # name (order-safe).
        # ------------------------------------------------------------------
        if self._canonical_arrow_schema is None:
            if self._tag_schema is not None and self._data_schema is not None:
                combined = {**dict(self._tag_schema), **dict(self._data_schema)}
                self._canonical_arrow_schema = (
                    self.data_context.type_converter.python_schema_to_arrow_schema(combined)
                )
            else:
                logger.warning(
                    "PollingSource %r: no schema declared via impl.schema(); "
                    "inferring nullability from first batch. Implement impl.schema() "
                    "to avoid schema drift on zero-row polls or null-free batches.",
                    self._source_id,
                )
                self._canonical_arrow_schema = arrow_utils.infer_schema_nullable(arrow_table)

        canonical_nullable = {f.name: f.nullable for f in self._canonical_arrow_schema}
        target_schema = pa.schema([
            pa.field(f.name, f.type, nullable=canonical_nullable.get(f.name, f.nullable))
            for f in arrow_table.schema
        ])
        arrow_table = arrow_table.cast(target_schema)
```

The complete `_build_stream_from_df` method after the edit should be:

```python
    def _build_stream_from_df(self, df: pl.DataFrame) -> ArrowTableStream:
        """Build an ``ArrowTableStream`` from a Polars DataFrame."""
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        # Handle Object-dtype columns (same pattern as DataFrameSource)
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        df = polars_data_utils.drop_system_columns(df)

        arrow_table = df.to_arrow()

        if self._canonical_arrow_schema is None:
            if self._tag_schema is not None and self._data_schema is not None:
                combined = {**dict(self._tag_schema), **dict(self._data_schema)}
                self._canonical_arrow_schema = (
                    self.data_context.type_converter.python_schema_to_arrow_schema(combined)
                )
            else:
                logger.warning(
                    "PollingSource %r: no schema declared via impl.schema(); "
                    "inferring nullability from first batch. Implement impl.schema() "
                    "to avoid schema drift on zero-row polls or null-free batches.",
                    self._source_id,
                )
                self._canonical_arrow_schema = arrow_utils.infer_schema_nullable(arrow_table)

        canonical_nullable = {f.name: f.nullable for f in self._canonical_arrow_schema}
        target_schema = pa.schema([
            pa.field(f.name, f.type, nullable=canonical_nullable.get(f.name, f.nullable))
            for f in arrow_table.schema
        ])
        arrow_table = arrow_table.cast(target_schema)

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=self._tag_columns,
            source_id=self._source_id,
        )
        return result.stream
```

- [ ] **Step 3: Run the four new regression tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceZeroRowBatch -v
```

Expected: all four tests **PASS**.

If any test fails, re-read the error and check the implementation — do not proceed to the full suite until these pass.

- [ ] **Step 4: Run the full existing PollingSource test suite**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v
```

Expected: **all tests PASS**. Pay special attention to:

- `TestPollingSourceSyncMode` — sync path also calls `_build_stream_from_df`; the canonical schema must not break it.
- `TestPollingSourceSchemaValidation` — declared-schema tests; the canonical schema path feeds the same `_tag_schema` / `_data_schema` already validated there.
- `TestPollingSourceAsyncMode::test_schema_mismatch_raises_on_column_change` — intentional schema mismatch must still raise.

- [ ] **Step 5: Commit the implementation**

```bash
git add src/orcapod/core/sources/polling_source.py
git commit -m "fix(polling_source): establish canonical Arrow schema once per source (ENG-952)

Per-batch nullability re-inference via infer_schema_nullable caused
SchemaInconsistencyError whenever a poll returned a zero-row batch after
any batch that contained a null — a zero-row table always has
null_count == 0, so every field was inferred non-nullable.

_build_stream_from_df now establishes _canonical_arrow_schema exactly
once: from impl.schema() Python type annotations when declared (T | None
→ nullable=True; plain T → nullable=False, no inference, no warning), or
from the first batch otherwise (with a WARNING-level log). All subsequent
batches are cast to the canonical schema by column name, so nullability is
stable across the source's lifetime regardless of per-batch null counts."
```

---

### Task 3: Add DESIGN_ISSUES.md entry and final checks

**Files:**
- Modify: `DESIGN_ISSUES.md`
- Run: full channel test suite

- [ ] **Step 1: Add PS4 entry to DESIGN_ISSUES.md**

Locate the PS3 entry (ends around line 149). Insert the following block immediately after PS3's closing `---`:

```markdown
### PS4 — `PollingSource` re-infers Arrow schema nullability per batch, crashing on zero-row polls
**Status:** resolved
**Severity:** high
**Issue:** ENG-952

`_build_stream_from_df` called `infer_schema_nullable` on every batch.  A zero-row batch has
`null_count == 0` for all columns, so every field was inferred `nullable=False`.
`_validate_combining_schemas` then rejected the batch against the accumulated stream's nullable
schema, raising `SchemaInconsistencyError` — an `InputValidationError` that `async_iter_data`
re-raises immediately, killing the source.  The common-path trigger: `WindowDiscoverySource`
in watch mode emits zero-row deltas once the discovery window is fully emitted (~144
times/day per stage at a 10-minute interval).

**Fix:** `_build_stream_from_df` now establishes `_canonical_arrow_schema` exactly once.
Declared-schema path (`impl.schema()` non-`None`): Arrow schema derived from Python type
annotations via `python_schema_to_arrow_schema` — `T | None` → `nullable=True`, plain `T` →
`nullable=False`; no inference, no warning.  Infer-once path (`impl.schema()` returns `None`):
nullability inferred from the first batch with a `WARNING`-level log prompting the caller to
declare a schema.  All subsequent batches are cast to the canonical schema by column name.
Per-batch re-inference is eliminated.

---
```

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass. If any test other than the new ones fails, investigate before proceeding — the fix must not regress anything.

- [ ] **Step 3: Commit DESIGN_ISSUES.md**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs: add PS4 to DESIGN_ISSUES.md — PollingSource zero-row batch crash (ENG-952)"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| Canonical Arrow schema established once per source | Task 2 Step 2 |
| Declared-schema path: `T \| None` → `nullable=True`, no warning | Task 2 Step 2 (declared branch) |
| Infer-once path: infer from first batch, store, warn | Task 2 Step 2 (else branch) |
| Cast by column name (order-safe) | Task 2 Step 2 (`canonical_nullable` dict) |
| `_canonical_arrow_schema` attribute | Task 2 Step 1 |
| Regression test: zero-row after null streams cleanly | Task 1 (test 1) |
| Regression test: zero-row batch not accumulated | Task 1 (test 2) |
| Declared schema: no warning emitted | Task 1 (test 3) |
| Infer-once: exactly one warning | Task 1 (test 4) |
| DESIGN_ISSUES.md PS4 entry | Task 3 Step 1 |

**Placeholder scan:** No TBDs, no "similar to" references, no missing code blocks. ✓

**Type consistency:**
- `_canonical_arrow_schema` named identically in Task 2 Steps 1 and 2. ✓
- `canonical_nullable` dict built from `self._canonical_arrow_schema` and applied immediately in the same method. ✓
- `FakeDynamicSource` and `_batch` referenced in Task 1 Test 4 — both defined earlier in the test file. ✓
- `Schema`, `Cursor`, `PollingConfig`, `pa`, `logging` — all already imported at the top of the test file. ✓
