# ITL-186 Notebook Preview of SpiralDBTableSource Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Displaying a `SpiralDBTableSource` in a notebook (`_repr_html_`, `view()`) reads a
head of at most 10 rows through Spiral's limit, never trips `max_rows`, and says how many rows
the table has; every other stream renders exactly as before.

**Architecture:** `StreamBase` gains a private hook `_preview(columns, all_info) -> (polars
frame, note | None)` whose default is today's behaviour (`as_polars_df`, no note).
`_repr_html_` and `view()` go through it and append the note to the header line.
`SpiralDBTableSource` overrides it: one scan with `limit=min(10, limit)`, no cap, and a
manifest row count only when the head is not the whole read. A private class switch
`_preview_reads_rows` (default `True`) turns the override into schema only, no scan
(fallback for ITL-991, coordinator condition 1).

**Tech Stack:** Python 3.11+, pyarrow, polars, pyspiral fake, pytest via `uv run`.

---

## State (keep current; a successor continues from here)

* PR #333 open on `eywalker/itl-186-add-dynamic-mode-to-spiraldbsource`.
* Approach approved by the coordinator (Nov 12) with two conditions: (1) the no-scan
  fallback switch, tested; (2) check large cells: done, Polars' HTML already shortens lists
  (first 3, `…`, last) and strings (30 chars); no preview-only cell summary added.
* Part 1 done: 3e5773ed (CodeRabbit rate-limited on it).
* CodeRabbit round 1 (review of 012a86eb): 6 threads + nitpick + additional + linked-issues
  check, plan approved by the coordinator; fixed in 3bca8ec9, merged origin/main (23bb59d2),
  typed tests (dba32fad), pushed once. All 6 threads replied; top-level comment
  issuecomment-6838421621 answers the body-only findings. PR body updated.
* Live (read-only + scratch tables created and dropped): 7 live tests pass. One earlier run
  had a transient failure of `test_an_async_job_over_a_limited_read` that did not recur.
* Next: CI and CodeRabbit on dba32fad; further rounds; check every reviewThread has a reply;
  worker_done with the reviewed sha.

## Note texts

| Case | Header after `SpiralDBTableSource[<label>] — ` |
|---|---|
| fewer than 10 rows came back, or own `limit` <= 10 | `all 4 rows` |
| head of a larger read, no `where` | `first 10 rows of about 63,349` (min of `limit` and the estimate) |
| head of a larger read, with `where` | `first 10 matching rows (where unit_id=106); the table has about 63,349 rows` |
| switch off | `rows not read; the table has about 63,349 rows` (zero-row frame) |

## Tasks

### Task 1: failing tests (`tests/test_core/sources/test_spiraldb_table_source_streaming.py`)

- [ ] `TestNotebookPreview` on a 25-row table: display makes one scan with `limit=10`, header
  note, `*` tag columns, shape (10, 3); above the cap previews without raising; `all 5 rows`
  on a small table; source `limit=3` scans with 3; `where` note; `view()` same scan and note;
  `view(all_info=True)` has system columns; `repr()` reads nothing; switch off: no scan,
  zero rows, note.
- [ ] `tests/test_core/streams/test_streams.py`: an `ArrowTableStream`'s `_repr_html_` is the
  header `ArrowTableStream[<label>]\n` plus the Polars HTML of all rows, tags first and
  marked (pins today's behaviour).
- [ ] Run, see the Spiral ones fail.

### Task 2: implement

- [ ] `src/orcapod/core/streams/base.py`: `_preview`, `_preview_header`, `_repr_html_` and
  `view()` through it, `StreamView(stream, df, note=None)`.
- [ ] `src/orcapod/core/sources/spiraldb_table_source.py`: `PREVIEW_ROWS = 10`,
  `_preview_reads_rows = True`; `_batch_streams()` delegates to `_read_batches(limit,
  max_rows)`; `as_table` body moved to `_whole_table(streams, columns, all_info)`; `_preview`.
- [ ] Tests pass; ruff, basedpyright; commit; push.

### Task 3: docs

- [ ] `docs/concepts/sources.md`: a paragraph on what a displayed Spiral source shows.
- [ ] `changelog.d/ITL-186.md`: a line under `### Added`/`### Changed`.
- [ ] full suite; commit; push; then PART 2.
