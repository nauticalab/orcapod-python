# ITL-816 `ArrowTableStream.iter_data` Chunk-Layout Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `ArrowTableStream.iter_data` yields every row, correctly paired, whatever the chunk
layout of the tag columns, data columns and source-info table.

**Architecture:** `iter_data` currently calls `to_batches()` separately on the tag-column
selection and the data-column selection of `self._table`, then `zip`s the two batch lists.
`Table.to_batches()` cuts at the union of the chunk boundaries of the *selected* columns, so
the two lists differ whenever tag and data columns are chunked differently (which is exactly
what the orchestrator's `hstack_tables(concat(tags), concat(data))` produces). `zip`
truncates (rows dropped) and unequal batch lengths mis-pair rows. A second bug of the same
family sits two lines below: `self._source_info_table.slice(i, 1)` uses the *batch-local*
index `i`, so every row after the first batch gets another row's source info.

The fix iterates batches of the **whole** `self._table` once (`self._table.to_batches()`
yields batches in which all columns are aligned by construction), selects tag and data columns
out of each batch, and indexes source info by a global row offset from a single
`to_pylist()`. No per-row `table.slice`, no copy of the table.

**Tech Stack:** Python, PyArrow, pytest (`uv run pytest`).

**No change** to public API, schemas, hashes or record ids: the same single-row `RecordBatch`
slices reach `Tag` / `Data` as before, for the rows that were previously yielded correctly.

---

## File structure

- Modify: `src/orcapod/core/streams/arrow_table_stream.py` (`iter_data` only; drop the unused
  `repeat` import).
- Create: `tests/test_core/streams/test_iter_data_chunk_layouts.py` (regression tests).
- Modify: `DESIGN_ISSUES.md` (one entry appended at the end).

### Task 1: Failing regression tests

**Files:**
- Create: `tests/test_core/streams/test_iter_data_chunk_layouts.py`

- [ ] **Step 1: Write the tests**

Helper `_chunked_stream(tag_sizes, data_sizes, source_sizes=None)` builds a table whose tag
columns (`id`, `grp`) are `pa.chunked_array`s cut at `tag_sizes`, whose data columns (`x`,
`y`) are cut at `data_sizes`, and optionally a per-row `_source_x` column cut at
`source_sizes`. Row `i` has `id=i`, `grp=f"g{i}"`, `x=i*10`, `y=f"v{i}"`,
`_source_x=f"src::{i}"`, so any mis-pairing is detectable.

Parametrized layouts (tag sizes, data sizes), all summing to the same row count:

| id | tag | data |
|---|---|---|
| tag-single/data-many | `[6]` | `[1,1,1,1,1,1]` |
| tag-many/data-single | `[2,2,2]` | `[6]` |
| unequal-boundaries | `[4,2]` | `[1,5]` |
| fewer-tag-chunks | `[3,3]` | `[2,2,2]` |
| fewer-data-chunks | `[1,2,3]` | `[5,1]` |
| reporter-shape (6 rows → 4) | `[4,2]` | `[2,2,1,1]` |
| empty-chunk | `[0,6]` | `[3,0,3]` |
| zero-length data chunks (cache-hit shape) | `[1]*6` | `[1,0,0,1,0,1,0,0,1,1,0,1]` |
| zero-length tag chunks | `[1,0,0,1,0,1,0,0,1,1,0,1]` | `[1]*6` |
| zero-length chunks both sides | `[0,1,0,2,0,3]` | `[3,0,0,2,1,0]` |
| same-layout (control) | `[2,4]` | `[2,4]` |

Assertions per layout:
1. `len(list(stream.iter_data())) == stream.as_table().num_rows == n`.
2. For each yielded `(tag, data)`: `data["x"] == tag["id"] * 10`, `data["y"] ==
   f"v{tag['id']}"`, `tag["grp"] == f"g{tag['id']}"`, and ids come out in order `0..n-1`.
3. `as_table(columns={"content_hash": True})` succeeds (it appends one hash per iterated row).
4. A second `iter_data()` call (cache path) returns the same pairs.

Plus: a no-tag-column stream with multi-chunk data yields all rows; a per-row source-info test
(`_source_x` chunked `[2,2,2]`, others single chunk and vice versa) asserting
`data.source_info()["x"] == f"src::{id}"`; an empty (0-row) stream yields nothing.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_core/streams/test_iter_data_chunk_layouts.py -v`
Expected: the mismatched-layout cases FAIL (fewer rows than `as_table()` or mis-paired
values); the control passes.

### Task 2: Fix `iter_data`

**Files:**
- Modify: `src/orcapod/core/streams/arrow_table_stream.py`

- [ ] **Step 1: Replace the body of the uncached branch**

```python
        if self._cached_elements is None:
            cached_elements = []
            tag_columns = list(self._all_tag_columns)
            data_columns = list(self._data_columns)
            empty_tag = Tag({}) if not tag_columns else None

            # Indexed by absolute row position; the source-info table has its
            # own chunk layout, so it must never be indexed batch-locally.
            source_info_rows = self._source_info_table.to_pylist()

            # Batch the whole table once.  ``Table.to_batches`` cuts at the
            # union of the chunk boundaries of every column, so tag and data
            # columns of a batch always cover the same rows.  Batching the tag
            # and data selections separately and zipping them drops or
            # mis-pairs rows whenever the two are chunked differently (ITL-816).
            row_offset = 0
            for batch in self._table.to_batches():
                tag_batch = batch.select(tag_columns) if tag_columns else None
                data_batch = batch.select(data_columns)
                for i in range(batch.num_rows):
                    if tag_batch is not None:
                        tag = Tag(
                            tag_batch.slice(i, 1),  # type: ignore
                            data_context=self.data_context,
                        )
                    else:
                        tag = cast(Tag, empty_tag)

                    data = Data(
                        data_batch.slice(i, 1),
                        source_info=source_info_rows[row_offset + i],
                        data_context=self.data_context,
                    )

                    yield tag, data

                    cached_elements.append((tag, data))
                row_offset += batch.num_rows
            self._cached_elements = cached_elements
```

Remove `from itertools import repeat`.

- [ ] **Step 2: Run the new tests** — `uv run pytest tests/test_core/streams -v` → all PASS.

- [ ] **Step 3: Measure** — time `sum(1 for _ in stream.iter_data())` on 100k-row tables
  (single chunk, aligned 1k chunks, with a per-row source column), interleaving the pre-fix
  class (loaded from `git show origin/main:src/orcapod/core/streams/arrow_table_stream.py`
  into a scratch module) and the fixed class in one process. The fix must not be slower.

- [ ] **Step 4: Commit** — `fix(streams): iterate ArrowTableStream rows over aligned batches (ITL-816)`

### Task 3: Audit

Check every site in `src/orcapod` that calls `to_batches()`, iterates `.chunks`, zips two
row sequences, or slices rows by index; classify each as safe / fixed / out-of-ownership.
Record the list in the PR body and `worker_done`. Sites in the orchestrator materialization
code, Join system-tag handling and `side_effects.py` belong to ITL-817: report, do not edit.

### Task 4: Log, verify, ship

- [ ] Append one `DESIGN_ISSUES.md` entry (status `resolved`, **Fix:** note).
- [ ] `uv run pytest tests/` — full suite green; list pre-existing failures/skips by name.
- [ ] Rebase on `origin/main` if it moved, push, open PR against `main` with `Fixes ITL-816`.
