# ITL-936 Spiral nullability record — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task (inline; no helper agents run suites: 16 GB machine). Steps use checkbox
> (`- [ ]`) syntax for tracking.

**Goal:** SpiralDB stores every column nullable (only list items keep `not null`). Orcapod
records each table's non-nullable column paths in the table's key-value metadata when it
writes, restores them on read, and checks every batch, so recorded columns arrive as `T`.

**Architecture:** A pure module `orcapod/databases/spiraldb_nullability.py` (no pyspiral import)
encodes/decodes a versioned, chunked JSON record, applies it to an Arrow schema and checks a
batch against it. `SpiralDBConnector` calls it in `upsert_records` (write the record on a
table's first write, refuse a disagreeing later write, refuse nulls in non-null paths before
anything is written), `get_column_info` (schema) and `iter_batches` (check + restore per
batch), wrapping errors with project/dataset/table. Tables without a record are untouched.

**Tech Stack:** pyarrow 23, pyspiral 0.16.33 (fake in `tests/test_databases/fake_pyspiral.py`).

**Status / handoff notes** (keep current; the machine has restarted before):
- Branch `eywalker/itl-936-spiral-connector-record-column-nullability`, stacked on PR #305
  (`eywalker/itl-926-re-verify-spiraldb-table-as-a-source-for`, 49956a6a).
- [x] Investigation (probe output, ITL-933 tool, connector, fake, tests).
- [x] Fake corrected to server behaviour; list of #305 tests only true on the fake (Notes).
- [x] Design sent to coordinator; provisionally approved November 11, 2026 (names, key and
  encoding pending developer confirmation: keep them easy to change). Additions requested and
  done: header written last with digest-named chunk keys; record never rewritten by the
  connector; per-batch check counted for unrecorded tables; result-store round trip.
- [x] Tasks 0-6 done; full suite green on 3.12 (7669 passed, 96 skipped, 19 xfailed).
- [ ] Live check (coordinator runs `superpowers/specs/itl-936-live/live_nullability.py`).
- [ ] PR, CodeRabbit rounds, CI.

pyspiral 0.16.33 `Table.set_metadata`: one upsert PUT of all entries; "not versioned, parallel
calls race"; limits: key + value <= 8 KiB, key <= 255 characters, <= 1024 entries per table;
`drop_metadata(keys)` exists. `Transaction.write(push_down_nulls=True)` turns a null struct
into a struct of null fields ("SpiralDB doesn't allow struct-level nullability").

---

## Evidence (live, pyspiral 0.16.33, November 11, 2026)

* `Schema({k=i64?, l=fixed_size_list(f32)[2]?, v=i32?, w=i32?, g={x=i32?, y=i32?}?})` after a
  write declaring k, v, g, g.x, l and l's items non-null: every top-level column and every
  struct field is nullable; list items keep `not null`.
* Null in top-level `v` declared non-null: refused client-side (Arrow, before sending).
* Null in struct field `g.x` declared non-null: **accepted and stored as `0`** (row k=4 reads
  back `g.x = 0`). So the connector must check nulls itself before writing.
* Measured on `ArrowTableSource`: nullability changes `output_schema`, `content_hash`,
  `pipeline_hash`, per-row `_content_hash` and the system-tag column names (schema hash);
  record-id values and source-info tokens do not change.
* `Join` refuses a key that is `T` on one side and `T | None` on the other
  (`InputValidationError: Types int | None and <class 'int'> are not compatible`).
* Arrow (`cast`, `view`) refuses a non-nullable field whose child array physically holds a
  null, even under a null parent struct slot. The record therefore uses Arrow's own meaning
  of non-nullable: the field's array holds no null at all.

## Design (to be approved by the coordinator)

### 1. The record

* Header key `__orcapod_nullability__`, value: compact JSON
  `{"format":"orcapod.nullability","version":1,"chunks":N,"sha256":"<hex>"}`.
* Chunk keys `__orcapod_nullability__/0` … `/N-1`: their values concatenated are the UTF-8
  payload, a compact JSON array of the non-nullable paths, sorted, e.g.
  `[["g"],["g","x"],["k"],["l"],["l","[]"]]`. `sha256` is over the payload.
* A path is a list of segments: the top-level column name, then a struct field name, or `"[]"`
  for the item of a `list` / `large_list` / `fixed_size_list`. Segments are read against the
  type, so a struct field literally named `"[]"` is not ambiguous. Map interiors are not
  recorded (no EDI table has maps; SpiralDB maps unverified): writing a table whose map item
  field is declared non-null raises rather than silently widening it.
* It asserts exactly: each listed field is non-nullable (Arrow meaning: no null in its array).
  Every field not listed keeps whatever nullability SpiralDB reports (list items stay
  non-null).
* Size: each chunk value is at most 8,192 − len(chunk key) − 64 bytes, so no key/value pair
  reaches SpiralDB's 8 KiB limit. A 400-column table with 20-character names (~10 KB of
  paths) takes 2 chunks. There is no size limit beyond SpiralDB's own total, which the write
  surfaces as SpiralDB's error before any data is written.
* Read errors (all `NullabilityRecordError`, never ignored): header not JSON or wrong shape,
  unknown `format` or `version`, a missing chunk, digest mismatch, payload not a list of
  string lists, a path that does not exist in the table's schema. Chunk keys with an index
  ≥ N (left by an earlier, longer record) are not part of the record.

### 2. Write

* Public helper (no pyspiral dependency): `orcapod.databases.nullability_record_metadata(schema)
  -> dict[str, bytes]`; any writer does `table.set_metadata(nullability_record_metadata(schema))`
  with the Arrow schema it writes. `orcapod.databases.read_nullability_record(metadata) ->
  frozenset[tuple[str, ...]] | None` reads it back (for `verify`).
* `SpiralDBConnector.upsert_records(records)`:
  1. Check `records` has no null in any non-null path (else `NullInNonNullableColumnError`,
     nothing written: SpiralDB would store `0` in a struct field).
  2. Table has a record: the incoming schema's non-null paths must equal it, else
     `NullabilityRecordMismatchError` listing paths only in the record / only in the write.
     Never rewritten, never widened or narrowed.
  3. Table has no record and no value columns yet (fresh): `set_metadata(record)` first, then
     write the rows. Record before data, so a failed metadata write leaves no unrecorded rows.
  4. Table has no record but already holds value columns (written before this change, or by
     another tool): write as today, add no record (adding one would assert something about rows
     already stored). `nullability_record_metadata` can annotate it deliberately.

### 3. Read

* `get_column_info` applies the record to Spiral's schema (top-level `nullable` and nested
  fields inside `arrow_type`), with no data read.
* `iter_batches` applies, per batch: restore `__arrow_metadata__` (as today), normalize to
  large types (as today), then `enforce_nullability_record(batch, paths)`: for each recorded
  path, count nulls in the field's array (top-level `null_count` is O(1); a struct field or
  list item is one child access / `flatten`, O(rows) and vectorized, negligible next to the
  scan); a null raises `NullInNonNullableColumnError(column="g.x")`; otherwise cast the batch
  to the recorded schema.
* Context: the helpers know only the column; the connector catches and re-raises the same
  class with `table="<project>/<dataset>/<table>"` added (no message-only parameters).
* Streaming (ITL-186): the streaming source declares its schema from `get_column_info` and runs
  each scanned batch through the same `enforce_nullability_record` before enrichment.

### 4. Identity

* No record: byte-identical to today (pinned by explicit hash values on the corrected fake).
* With the record: recorded columns become `T` in `output_schema`; `pipeline_hash`,
  `content_hash`, per-row content hashes and the system-tag column names change (they include
  the schema); record-id values and `_source_*` tokens do not. A table that gains a record is,
  to Orcapod, a table with a different schema, so downstream stored results are recomputed
  under new paths rather than reused; nothing stored is altered.

### 5. Interactions

* Join: tables from the tutorial project are all recorded, so shared keys are `T` on both
  sides. A recorded table joined with an unrecorded one (or any `T | None` key) is refused by
  the existing strict check; documented, not relaxed.
* Save/load: the source config carries no nullability; a loaded source re-reads the record, so
  hashes match while the record is unchanged. If the table gained a record after the save, the
  rebuilt source has a different pipeline hash (correct: its schema changed).
* `ConnectorArrowDatabase` on Spiral: new result tables get the record on their first flush, so
  results read back with their written nullability (today they come back all nullable, and a
  struct column with a non-null field would fail the flush type comparison on the next flush).
  Existing result tables have data and no record: unchanged. A later flush whose top-level
  nullability differs from the record now raises (step 2.2); result schemas come from pod
  annotations, so this is not expected in practice.

### 6. Workshop tutorial project (ITL-933 `annotate`)

```python
from orcapod.databases import nullability_record_metadata, read_nullability_record

kv = nullability_record_metadata(declared_schema)   # the pa.Schema the copy wrote
if read_nullability_record(table.get_metadata()) != read_nullability_record(kv):
    table.set_metadata(kv)
```

`declared_schema` is the schema `copy` builds from the source types and the map's nullability
(keys non-null, required columns non-null, the `unit` groups non-null with nullable fields,
`lfp_voltage_uv` items nullable).

---

## File structure

* Create `src/orcapod/databases/spiraldb_nullability.py`: record encode/decode, schema apply,
  batch enforce. Pure pyarrow.
* Modify `src/orcapod/errors.py`: `NullabilityRecordError`, `NullabilityRecordMismatchError`,
  `NullInNonNullableColumnError` (all `ValueError` subclasses; the last two carry
  `column`/`table` attributes).
* Modify `src/orcapod/databases/spiraldb_connector.py`: write/read integration.
* Modify `src/orcapod/databases/__init__.py`: export the two public helpers.
* Modify `tests/test_databases/fake_pyspiral.py`: server nullability behaviour.
* Tests: `tests/test_databases/test_spiraldb_nullability.py` (helpers),
  `tests/test_databases/test_spiraldb_connector_fake.py`,
  `tests/test_core/sources/test_spiraldb_table_source_fake.py`.
* Docs: `docs/concepts/sources.md` (Spiral nullability), `CHANGELOG.md` ([0.3.0] unreleased),
  `DESIGN_ISSUES.md` (one entry at the end).

## Tasks

### Task 0: Fake behaves like the server

- [ ] Replace `value_columns_nullable` with `server_nullability: bool = True`: every top-level
  field and every struct field nullable (keys included), list / fixed-size-list items keep
  their flag; a null in a top-level field declared non-null is refused on write (Arrow's
  client-side check); a null in a non-null struct field is stored as the type's zero (seen
  live). Applied in `create_table` and `write`.
- [ ] Run the Spiral fake suites; record which #305 tests fail (only true on the fake).
- [ ] Commit `test(spiraldb): fake reports every column nullable, as SpiralDB does (ITL-936)`.

### Task 1: Record helpers (TDD)

- [ ] Tests: round trip of paths for top-level, struct, list item, fixed_size_list item,
  nested list-of-struct; absent → None; unknown version / format; malformed JSON; missing
  chunk; digest mismatch; 400-column table splits into chunks each ≤ 8 KiB; map item non-null
  raises; apply to schema; path absent from schema raises; enforce: null at top level, in a
  struct field, in a list item raises with the dotted column path; clean batch is cast.
- [ ] Implement `spiraldb_nullability.py` and the errors. Commit.

### Task 2: Connector write path (TDD on the fake)

- [ ] Tests: first write records; later write with same schema fine; disagreeing write (wider
  and narrower) raises and writes nothing; null in non-null struct field raises before write;
  table with data and no record gets no record. Implement. Commit.

### Task 3: Connector read path (TDD on the fake)

- [ ] Tests: `get_column_info` and `iter_batches` report recorded nullability at every depth;
  externally corrupted record (unknown version) raises naming project/dataset/table; null in a
  recorded column (record set by hand over nullable data) raises naming table and column.
  Implement. Commit.

### Task 4: Source end to end

- [ ] Rewrite `TestNonNullableColumns` to write through `SpiralDBConnector` (record) and show:
  `T` schema, pod `x: float`/`int` runs, Join, save/load. Unrecorded table: pod refused.
- [ ] Unrecorded table hashes pinned by explicit values (computed before the change).
- [ ] `TestSourceId.test_explicit_source_id_keeps_its_hashes`: the table written through the
  connector (recorded) reproduces the pinned values. Commit.

### Task 5: ConnectorArrowDatabase on Spiral fake

- [ ] Test: two flushes of a struct result with a non-null field succeed and read back with
  the written schema. Commit.

### Task 6: Docs, CHANGELOG, DESIGN_ISSUES

- [ ] `docs/concepts/sources.md`: replace "annotate `T | None`" guidance for recorded tables.
- [ ] CHANGELOG `[0.3.0] — unreleased`; DESIGN_ISSUES entry at the end. Commit.

### Task 7: Verify, live commands, PR

- [ ] Full suite (one Python version), ruff, basedpyright, golden hashes unchanged.
- [ ] Live check script under `superpowers/specs/itl-936-live/` + exact commands for the
  coordinator on `edi-tutorial-v1-841226`.
- [ ] Merge origin/main, push, PR (stacked on #305), attach, worker_done.

## Notes

### Task 0 result: #305 tests only true on the fake

With the fake corrected (`server_nullability=True`), these #305 tests fail; each asserted a
non-null flag SpiralDB does not keep:

* `test_spiraldb_connector_fake.py::TestLargeTypeNormalization::test_iter_batches_normalizes_nested_types`,
  `::test_get_column_info_matches_iter_batches`, `::test_source_exposes_normalized_types`
  (struct field `st.a` written non-null; SpiralDB reports it nullable).
* `test_spiraldb_table_source_fake.py::TestSourceId::test_explicit_source_id_keeps_its_hashes`
  (pinned hashes assumed `subject_id`/`sex` stay non-null).
* `test_spiraldb_table_source_fake.py::TestNonNullableColumns` (all three: schema `T`, nested
  nullability, plain-typed pod through Join and save/load).

### Pinned hashes for an UNRECORDED table (computed at 49956a6a, before any library change)

One-row `edi-sample-a/default/subject` (`subject_id`, `sex`; written non-null, stored nullable),
`source_id="subject"`:

* `output_schema`: `({'subject_id': str | None}, {'sex': str | None})`
* `content_hash`: `semantic_v0.1:0380725acb9073df747380b348f72a8a66bbe52f02e46fb3df795e7518e7f4f6`
* `pipeline_hash`: `semantic_v0.1:61796d9cdbd5afce45b2b99b1efe306a745ee63bdf4952e841772a04c450498a`
* record id `18d1efa53fbc52b6a40f406eca35ca81`; `_content_hash`
  `arrow_v0.1:0000017a393c2f094ebfb6ecb72d0a0d5ef088243f38b709695c846dbd4a17b095caf9`
