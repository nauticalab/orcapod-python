# ITL-186 Streaming Spiral Table Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SpiralDBTableSource` builds from metadata only and streams its rows in bounded batches.

**Architecture:** Design note `superpowers/specs/2026-11-12-itl-186-streaming-spiral-source-design.md`.
The source stops inheriting the eager load of `DBTableSource`; it keeps a zero-row template
stream for schema questions and opens one Spiral scan per read. Tests run on the disk-backed
pyspiral fake, which gains a scan log.

**Tech Stack:** Python 3.11+, pyarrow, pyspiral 0.16.33 (`spiraldb` extra), pytest, `uv run`.

---

## State (keep current; a successor continues from here)

* Branch `eywalker/itl-186-add-dynamic-mode-to-spiraldbsource`, from `origin/main` 25650c42.
* Decisions (Nov 12): D1 option A (source id `<project>/<dataset>.<table>@<table id>`),
  D2 OUT (restrictions not in the content hash; one function, `_restriction_identity`),
  D3 row token = labelled Arrow hash of the primary key (key fields non-nullable),
  D4 batches of 8,192, D5 `max_rows` cap 100,000, D6 one class without a mode,
  D7 no orchestrator or Join change, D8 full re-scan, D9 scope, D10 tests.
* Done: Phase A and Phase B below. Fake extended; connector `scan_batches`,
  `get_arrow_schema`, `get_spiral_table_id`, `get_table_version`, `get_row_count_estimate`;
  `SourceStreamBuilder.attach_provenance`; the source; tests (`test_spiraldb_table_source.py`
  rewritten on the fake, `..._streaming.py`, `..._live.py` opt-in, three pins in `..._fake.py`);
  docs, changelog fragment, `design_issues/ITL-186-SP4.md`.
* Verified live, read-only, on `edi-tutorial-v1-841226`: all 26 tables build (about 1.5 s)
  and read with `limit=50`; `spike_event` 63,349 rows iterate in 25 s; the opt-in live tests
  pass, including the scratch-table append test.
* Waits (reported, not in this PR): bounded-memory jobs, vectorized row digest, watermark and
  polling, key ranges, column selection, lazy frame, PostgreSQL / SQLite sources.
* Live probe scripts: `.untracked/itl186_*.py` (untracked).

## File structure

| File | Role |
|---|---|
| `tests/test_databases/fake_pyspiral.py` | fake: scan log, lazy batches, `limit` (Phase A); `where`, `asof`, `table_id`, `last_modified_at`, row count (Phase B) |
| `tests/test_core/sources/test_spiraldb_table_source_streaming.py` | new: streaming behaviour through the public interface |
| `src/orcapod/databases/spiraldb_connector.py` | Phase B: metadata-only reads and a restricted scan |
| `src/orcapod/core/sources/spiraldb_table_source.py` | Phase B: the source |
| `docs/concepts/sources.md`, `changelog.d/ITL-186.md`, `design_issues/ITL-186-SP4.md` | Phase B |

## Phase A (approved now)

### Task A1: scan log and lazy batches in the fake

**Files:** Modify `tests/test_databases/fake_pyspiral.py` (`FakeScan`, `FakeSpiral.scan`, module docstring).

- [ ] **Step 1: add the log and the batch-size switch** after `_MAX_ENTRIES`:

```python
@dataclass
class ScanRecord:
    """One ``Spiral.scan(...)`` call: the table, its keyword arguments, batches handed out."""

    identifier: str
    kwargs: dict[str, Any]
    batches_read: int = 0


# Every scan opened in this process, oldest first. Tests clear it and read it.
scan_log: list[ScanRecord] = []
# Rows per batch a scan hands out (SpiralDB's batches follow its stored fragments).
scan_batch_rows = 2
```

- [ ] **Step 2: replace `FakeScan`**

```python
class FakeScan:
    """What ``Spiral.scan(table.select(), limit=...)`` returns."""

    def __init__(self, table: FakeTable, record: ScanRecord, limit: int | None) -> None:
        self._table = table
        self._record = record
        self._limit = limit

    def _rows(self) -> pa.Table:
        rows = self._table._read_all()
        return rows if self._limit is None else rows.slice(0, self._limit)

    def to_table(self) -> pa.Table:
        """Return the scanned rows, in key order."""
        return self._rows()

    def to_record_batches(self, **kwargs: Any) -> pa.RecordBatchReader:
        """Return the rows in key order, ``scan_batch_rows`` per batch, read on demand."""
        rows = self._rows()

        def batches() -> Iterator[pa.RecordBatch]:
            for batch in rows.to_batches(max_chunksize=scan_batch_rows):
                self._record.batches_read += 1
                yield batch

        return pa.RecordBatchReader.from_batches(rows.schema, batches())
```

(`Iterator` from `collections.abc`.)

- [ ] **Step 3: `FakeSpiral.scan` records the call and supports `limit`**

```python
    def scan(self, *projections: Any, **kwargs: Any) -> FakeScan:
        """Return a scan of one ``table.select()``, restricted by ``limit`` if given."""
        if len(projections) != 1 or not isinstance(projections[0], FakeExpr):
            raise NotImplementedError(
                "fake Spiral.scan supports only scan(table.select())"
            )
        unsupported = {k for k, v in kwargs.items() if v is not None} - {"limit"}
        if unsupported:
            raise NotImplementedError(f"fake Spiral.scan: {sorted(unsupported)}")
        table = projections[0].table
        record = ScanRecord(table._identifier, dict(kwargs))
        scan_log.append(record)
        return FakeScan(table, record, kwargs.get("limit"))
```

- [ ] **Step 4:** run `uv run pytest tests/test_databases tests/test_core/sources -q -k "spiral or fake"`; expected: all pass (the fake's behaviour for existing callers is unchanged).
- [ ] **Step 5:** commit `test(spiraldb): the pyspiral fake logs scans and reads batches on demand (ITL-186)`.

### Task A2: failing streaming tests

**Files:** Create `tests/test_core/sources/test_spiraldb_table_source_streaming.py`.

- [ ] **Step 1: write the tests** (fixture: a 5-row `default.event` table keyed on
  `(subject_id, timestamp)`, all columns nullable; `scan_batch_rows` set to 2 and
  `scan_log` cleared by the fixture):

  * `test_construction_reads_no_rows`: build the source, call `output_schema()`, `keys()`,
    `content_hash()`, `pipeline_hash()`, `to_config()`; assert `fake_pyspiral.scan_log == []`.
  * `test_iter_data_reads_batches_only_as_rows_are_consumed`: `rows = iter(source.iter_data())`;
    `next(rows)`; assert exactly one scan is logged and its `batches_read == 1`; exhaust the
    iterator; assert `batches_read == 3` and 5 rows were seen.
  * `test_streamed_rows_equal_as_table_rows`: for every row, the tag and data values with
    `all_info=True` equal the same columns of `source.as_table(all_info=True)`. (Passes today
    too: it pins the equality the streaming path must keep.)

- [ ] **Step 2:** run `uv run pytest tests/test_core/sources/test_spiraldb_table_source_streaming.py -q`;
  expected: the first two FAIL (a scan is logged at construction, with all batches read), the
  third passes.
- [ ] **Step 3:** commit `test(spiraldb): streaming source tests, failing until ITL-186 lands`.
  The two failing tests are marked `xfail(strict=True, reason="ITL-186")` in the commit so
  the suite stays green; the marks are removed in Phase B.

## Phase B (done)

Order followed: connector metadata reads and restricted scan; `attach_provenance` extracted
from `SourceStreamBuilder.build` (byte-identical for every other source); the source; tests
shown failing against the old source (32 of 37) and passing against the new one; save and
load; cap; async; docs, changelog fragment, design-issue entry; live opt-in tests; full
suite, ruff, basedpyright.
