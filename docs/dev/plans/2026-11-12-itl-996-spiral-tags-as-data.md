# ITL-996: SpiralDBTableSource `tags_as_data` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SpiralDBTableSource(project, table, tags_as_data={"timestamp": "spike_time_us"})`
also offers named key columns as data columns, copied per batch in Arrow.

**Architecture:** The copy is one function on the class, applied to the zero-row template at
construction (so the schema is known without reading) and to each slice in `_read_batches`
after the key-token null check, before `attach_provenance`. Validation reuses
`derived_source._validate_tags_as_data` plus a key-column rule. Identity: the sorted mapping is
appended to `identity_structure` only when given.

**Base:** branch `eywalker/itl-996-spiraldbtablesource-copy-key-columns-into` = `origin/main`
290889e8 + PR #333 (`848d90b2`, merged locally, fast-forward). Do NOT push until #333 is on
main; then merge main and push once.

## Decisions (coordinator approved, November 12, 2026)

* (a) Signature `tags_as_data: Mapping[str, str] | None = None`. Copy field:
  `pa.field(name, key_field.type, nullable=False, metadata=key_field.metadata)`. Copies follow
  the table's data columns, in mapping order.
* (b) Errors: `InputValidationError` from `_validate_tags_as_data` (not a tag column; target
  already a column; target repeated; reserved prefix) plus "not a key column"; the
  constructor prefixes `project/dataset/table`.
* (c) `identity_structure` gains `("tags_as_data", tuple(sorted(items)))` only when given.
  Absent: structure exactly as today. pipeline hash stays schema-only. PR body has a
  "What enters identity" heading with the measurement.
* (d) `to_config` writes `tags_as_data` (dict or None); `from_config` reads it.
* (e) Docs use the caveat: `group_by(["unit_id"])` alone sorts members by
  spike_extraction_hash, subject, session, recording, probe, then timestamp, so time order
  holds only within one session/probe; group on the full unit key.

## Measurements (prototype, scratch only)

Fake: 0 scans at construction; source_id, record ids, `_tag_source_id` values and
`_source_amplitude` tokens equal with and without; content/pipeline hash differ; tag datagram
hash equal, data datagram hash differs; system-tag column names differ (schema hash).
Live `edi-tutorial-v1-841226` `spike_event`, `limit=20000`: 32 units, every `spike_time_us`
list ascending after `group_by(all keys but timestamp).drop_tag_columns("timestamp")`.

## Files

* Modify `src/orcapod/core/sources/spiraldb_table_source.py`
* Create `tests/test_core/sources/test_spiraldb_table_source_tags_as_data.py`
* Modify `tests/test_core/sources/test_spiraldb_table_source_live.py` (one opt-in test)
* Modify `docs/concepts/sources.md` (one short section)
* Create `changelog.d/ITL-996.md`, `design_issues/ITL-996-SP5.md`

## Tasks

- [ ] 1. Failing tests on the fake (new test file): schema at construction with 0 scans; copy
  type/non-null/metadata/values across batches (`scan_batch_rows=2`); `iter_data`,
  `async_iter_data` and `as_table` agree; each error names the column; identity (source_id,
  record ids, tokens equal; content and pipeline hash differ; two mappings with one schema
  differ in content hash, equal pipeline hash; absent option identical to a source built
  without the argument); copy token `<source_id>::<key token>::<name>`; data hash moves, tag
  hash does not; `to_config`/`from_config` and job save/load; preview shows the copy;
  `limit`/`where`/`max_rows`; workshop order with rows written out of order over 2 subjects.
  Run: `uv run pytest tests/test_core/sources/test_spiraldb_table_source_tags_as_data.py` → FAIL
  (unexpected keyword `tags_as_data`). Commit WIP.
- [ ] 2. Implement: constructor arg, validation, `_with_tag_copies(table)`, template, read path,
  identity, config, docstrings. Run the new file and all `test_spiraldb_table_source*` → PASS.
  Commit.
- [ ] 3. Opt-in live test (read-only, `limit=20000`): workshop case ascending per unit. Run with
  `ORCAPOD_SPIRAL_LIVE_PROJECT=edi-tutorial-v1-841226 uv run --extra spiraldb pytest ... -k tags_as_data`.
- [ ] 4. Docs section in `docs/concepts/sources.md` (convenience methods, caveat (e)); run
  `uv run pytest tests/test_docs`. Changelog fragment + `--check`; design-issue file + `--check`.
- [ ] 5. ruff check/format, basedpyright (exit code), full `uv run pytest tests/`. Commit.
- [ ] 6. Wait for #333 on main; merge origin/main; rerun suite; push; PR "Fixes ITL-996";
  `orca linear attach`, `orca worktree set --pr`; CodeRabbit rounds; worker_done.

## Progress notes

(Successor: read this section first.)

* 2026-11-12: tasks 1 to 5 done (commits 68f8fd10, 0483972f, 82000e17); #333 merged into
  main, origin/main merged (conflicts in three files resolved to the branch's side: main's
  copies equal 848d90b2). Full suite green. PR #336 opened, attached to ITL-996, worktree
  linked. Next: CodeRabbit rounds (read review BODIES too), CI green, worker_done.
