# ITL-616: Fix `PollingSource._combine` `_content_hash` Leak — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `PollingSource._combine` so that the synthetic `_content_hash` column is never stored in the accumulated stream, preventing a `SchemaInconsistencyError` on the third accumulating fetch.

**Architecture:** Add a module-level `_STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True, context=True)` constant to `polling_source.py` and replace the two `as_table(all_info=True)` calls in `_combine` with `as_table(columns=_STREAM_COMBINE_COLUMNS)`. No other files change except `DESIGN_ISSUES.md` (new PS3 entry) and the test file (two new regression tests).

**Tech Stack:** Python 3.11+, pytest, pytest-asyncio, pyarrow, orcapod internal classes (`PollingSource`, `ArrowTableStream`, `ColumnConfig`).

---

## File Map

| Action | Path | What changes |
|--------|------|-------------|
| Modify | `src/orcapod/core/sources/polling_source.py` | Add `_STREAM_COMBINE_COLUMNS` constant; replace `all_info=True` in `_combine` |
| Modify | `tests/test_channels/test_polling_source.py` | Add two regression tests to `TestPollingSourceSchemaValidation` |
| Modify | `DESIGN_ISSUES.md` | Add PS3 entry; update to `resolved` after fix |

---

## Setup: Create the branch

Before any task, create and check out the branch from `main`:

```bash
cd /path/to/orcapod-python
git checkout main
git checkout -b eywalker/itl-616-pollingsource_combine-leaks-_content_hash-into-the-data
```

Verify:

```bash
git branch --show-current
# eywalker/itl-616-pollingsource_combine-leaks-_content_hash-into-the-data
```

---

### Task 1: Add `DESIGN_ISSUES.md` PS3 entry

**Files:**
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Open `DESIGN_ISSUES.md` and insert the PS3 entry**

  Locate the block that ends with PS2 (search for `### PS2`). The section looks like this:

  ```markdown
  ### PS2 — Concurrent iteration over a single `PollingSource` has unguarded cursor/stream mutation
  **Status:** open
  ...

  **Fix:** Guard updates to `_cursor` and `_accumulated_stream` with an `asyncio.Lock` ...

  ---

  ## `src/orcapod/core/nodes/function_node.py`
  ```

  Insert the following block **between** the `---` separator and the `## src/orcapod/core/nodes/function_node.py` header:

  ```markdown
  ### PS3 — `_combine` leaks `_content_hash` into the data schema on the second accumulating fetch
  **Status:** in progress
  **Severity:** high
  **Issue:** ITL-616

  `_combine` calls `as_table(all_info=True)` on both streams before concatenating them.
  `all_info=True` resolves to `ColumnConfig.all()`, which includes `content_hash=True`.
  In `ArrowTableStream.as_table()`, `content_hash=True` dynamically appends a `_content_hash`
  column to the output table. This is a synthetic column — computed on demand, not stored in
  `ArrowTableStream._table`.

  `pa.concat_tables` then includes `_content_hash` in the combined table, which is passed
  directly to `ArrowTableStream.__init__`. Since `_content_hash` has no recognized prefix
  (`_tag::`, `_source_`, `_context_key`), it lands in `_data_columns` as if it were user data.

  On the next `_combine` call, `_validate_combining_schemas` compares:
  - `existing.keys()` → includes `_content_hash` in data keys (baked in from previous combine)
  - `new_stream.keys()` → no `_content_hash` (freshly built from raw fetched data)

  This raises `SchemaInconsistencyError`. A polling source emitting one new row per poll will
  change its data schema on the second new-data poll and crash on the third.

  **Fix:** Replace `all_info=True` with a named module-level constant
  `_STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True, context=True)`.
  `content_hash` is intentionally absent — it is a synthetic output column, never a stored one.

  ---
  ```

  Note: the `---` at the end of the PS3 entry replaces the existing `---` that was between PS2 and `## src/orcapod/core/nodes/function_node.py`.

- [ ] **Step 2: Commit**

  ```bash
  git add DESIGN_ISSUES.md
  git commit -m "docs(design-issues): add PS3 entry for _combine content_hash leak (ITL-616)"
  ```

---

### Task 2: Write the failing regression tests

**Files:**
- Modify: `tests/test_channels/test_polling_source.py`

The two tests go inside the existing `class TestPollingSourceSchemaValidation` (find it by searching for that class name). They must be placed **after** the last existing test in that class (`test_combining_column_set_mismatch_raises`).

- [ ] **Step 1: Add the two tests to `TestPollingSourceSchemaValidation`**

  The test file already imports `pyarrow as pa`, `pytest`, `FakeDynamicSource`, `_batch`, `PollingSource`, `PollingConfig` — no new imports needed.

  Add these two methods at the end of `class TestPollingSourceSchemaValidation`:

  ```python
      def test_sync_three_fetches_no_content_hash_leak(self):
          """After 3 accumulating fetches (2 combines), data schema is stable and
          contains no _content_hash column, and all rows are present.

          Regression test for ITL-616: _combine called as_table(all_info=True),
          which injected the synthetic _content_hash column into the stored stream.
          The second combine then raised SchemaInconsistencyError.
          """
          fake = FakeDynamicSource(
              batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)]
          )
          src = PollingSource(
              fake, tag_columns="id", polling_config=PollingConfig(interval=1.0)
          )

          rows1 = list(src.iter_data())   # fetch 1 — builds initial stream
          rows2 = list(src.iter_data())   # fetch 2 — first combine
          rows3 = list(src.iter_data())   # fetch 3 — second combine (crashed before fix)

          assert len(rows1) == 1
          assert len(rows2) == 2
          assert len(rows3) == 3

          _, data_keys = src.keys()
          assert "_content_hash" not in data_keys

          _, data_schema = src.output_schema()
          assert "_content_hash" not in data_schema

      @pytest.mark.asyncio
      async def test_async_three_fetches_no_content_hash_leak(self):
          """After 3 async batches (2 combines), data schema is stable and contains
          no _content_hash column, and all rows are accumulated.

          Regression test for ITL-616: same root cause as the sync path.
          """
          fake = FakeDynamicSource(
              batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)]
          )
          src = PollingSource(
              fake,
              tag_columns="id",
              polling_config=PollingConfig(
                  interval=0.05, duration=0.5, max_missed_intervals=50
              ),
          )

          items = []
          async for tag, data in src.async_iter_data():
              items.append((tag, data))

          assert len(items) == 3
          assert src._accumulated_stream is not None

          _, data_keys = src._accumulated_stream.keys()
          assert "_content_hash" not in data_keys

          _, data_schema = src._accumulated_stream.output_schema()
          assert "_content_hash" not in data_schema
  ```

- [ ] **Step 2: Run the tests to confirm they fail**

  ```bash
  uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSchemaValidation::test_sync_three_fetches_no_content_hash_leak tests/test_channels/test_polling_source.py::TestPollingSourceSchemaValidation::test_async_three_fetches_no_content_hash_leak -v
  ```

  Expected: both tests **FAIL**. The sync test fails with `SchemaInconsistencyError` on the third `iter_data()` call. The async test fails similarly.

- [ ] **Step 3: Commit the failing tests**

  ```bash
  git add tests/test_channels/test_polling_source.py
  git commit -m "test(polling_source): add three-fetch regression tests for content_hash leak (ITL-616)"
  ```

---

### Task 3: Implement the fix and close out

**Files:**
- Modify: `src/orcapod/core/sources/polling_source.py`
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Add `_STREAM_COMBINE_COLUMNS` constant to `polling_source.py`**

  Open `src/orcapod/core/sources/polling_source.py`. The file has three module-level
  functions before the class: `_get_sync_executor`, `_run_sync`, and `_assert_schema_match`.
  Find the end of `_assert_schema_match` (it ends with a `raise SchemaInconsistencyError(...)`
  block) and the `# PollingSource` section comment that follows it:

  ```python
  # ---------------------------------------------------------------------------
  # PollingSource
  # ---------------------------------------------------------------------------


  class PollingSource(RootSource, Generic[T]):
  ```

  Insert the constant in the gap between `_assert_schema_match` and that section comment:

  ```python
  # ColumnConfig used when concatenating streams in _combine.
  # Includes the provenance columns (system_tags, source, context) that
  # ArrowTableStream.__init__ knows how to parse and split into their
  # respective internal tables.
  # content_hash is intentionally absent: it is a synthetic, on-demand
  # column produced by as_table(); including it would bake it into stored
  # data and corrupt the data schema on the next combine.
  _STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True, context=True)


  # ---------------------------------------------------------------------------
  # PollingSource
  # ---------------------------------------------------------------------------
  ```

  Note: `ColumnConfig` is already imported at the top of the file via
  `from orcapod.types import ColumnConfig, Cursor, PollingConfig, Schema` — no new import needed.

- [ ] **Step 2: Fix `_combine` to use `_STREAM_COMBINE_COLUMNS`**

  In the same file, locate the `_combine` method. It contains:

  ```python
        combined = pa.concat_tables(
            [
                existing.as_table(all_info=True),
                new_stream.as_table(all_info=True),
            ],
            promote_options="default",
        )
  ```

  Replace with:

  ```python
        combined = pa.concat_tables(
            [
                existing.as_table(columns=_STREAM_COMBINE_COLUMNS),
                new_stream.as_table(columns=_STREAM_COMBINE_COLUMNS),
            ],
            promote_options="default",
        )
  ```

  Nothing else in `_combine` changes.

- [ ] **Step 3: Run the regression tests to confirm they now pass**

  ```bash
  uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSchemaValidation::test_sync_three_fetches_no_content_hash_leak tests/test_channels/test_polling_source.py::TestPollingSourceSchemaValidation::test_async_three_fetches_no_content_hash_leak -v
  ```

  Expected: both tests **PASS**.

- [ ] **Step 4: Run the full polling source test suite**

  ```bash
  uv run pytest tests/test_channels/test_polling_source.py tests/test_channels/test_polling_source_pipeline_integration.py -v
  ```

  Expected: all tests **PASS**. No regressions.

- [ ] **Step 5: Update PS3 status to `resolved` in `DESIGN_ISSUES.md`**

  Locate the PS3 entry added in Task 1. Change:

  ```markdown
  **Status:** in progress
  ```

  to:

  ```markdown
  **Status:** resolved
  ```

  Add a **Fix:** note immediately after the status line:

  ```markdown
  **Status:** resolved
  **Fix:** Added `_STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True,
  context=True)` constant and replaced `as_table(all_info=True)` with
  `as_table(columns=_STREAM_COMBINE_COLUMNS)` in `_combine`. `content_hash` is
  intentionally excluded — it is a synthetic output column, never a stored one.
  ```

- [ ] **Step 6: Commit the fix**

  ```bash
  git add src/orcapod/core/sources/polling_source.py DESIGN_ISSUES.md
  git commit -m "fix(polling_source): exclude _content_hash from _combine column config (ITL-616)

  _combine passed all_info=True to as_table(), which triggered content_hash=True
  and injected the synthetic _content_hash column into the concatenated table.
  ArrowTableStream.__init__ stored it as a data column, corrupting the schema
  on the second combine and raising SchemaInconsistencyError on the third fetch.

  Add _STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True,
  context=True) and use it in place of all_info=True. content_hash is excluded
  because it is a synthetic, on-demand output — not a stored column."
  ```

- [ ] **Step 7: Run the full test suite**

  ```bash
  uv run pytest tests/ -x -q
  ```

  Expected: all tests pass.
