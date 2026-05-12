# Tag → Key Rename Design

**Issue:** ENG-455
**Date:** 2026-05-12
**Status:** Approved

## Overview

Rename the `tags` concept and all related identifiers to `keys` throughout orcapod-python.
This is a hard break with no deprecation aliases or migration shims, targeting the v0.1 stable
release. `keys` is a clearer fit than `tags` because these fields are the primary key into a
stream of data and the term aligns naturally with the upcoming `SourceSpec` (key schema + data
schema) abstraction.

## Goals & Success Criteria

- All public and private symbols, class names, attributes, methods, function arguments, and
  module names containing `tag`/`tags`/`Tag` are renamed to `key`/`keys`/`Key`.
- Serialized/persisted column name prefixes (`_tag_`, `_tag::`) are renamed to `_key_` and
  `_key::` in-place (pre-v0.1 artifacts are not expected to load).
- All docstrings, comments, type stubs, error messages, and log strings use the new terminology.
- All examples, design docs, CLAUDE.md, `.zed/rules`, and `orcapod-design.md` are updated.
- Test suite renamed and passing.
- No deprecation shims, no backwards-compat aliases — old names are gone.

## Complete Rename Map

### Classes & Protocols

| Old | New |
|-----|-----|
| `Tag` | `Key` |
| `TagProtocol` | `KeyProtocol` |
| `DuplicateTagError` | `DuplicateKeyError` |
| `SelectTagColumns` | `SelectKeyColumns` |
| `DropTagColumns` | `DropKeyColumns` |
| `MapTags` | `MapKeys` |

### Methods & Functions

| Old | New |
|-----|-----|
| `system_tags()` | `system_keys()` |
| `_ensure_system_tags_table()` | `_ensure_system_keys_table()` |
| `map_tags()` | `map_keys()` |
| `select_tag_columns()` | `select_key_columns()` |
| `drop_tag_columns()` | `drop_key_columns()` |
| `add_system_tag_columns()` | `add_system_key_columns()` |
| `add_system_tag_column()` | `add_system_key_column()` |
| `append_to_system_tags()` | `append_to_system_keys()` |
| `_parse_system_tag_column()` | `_parse_system_key_column()` |
| `sort_system_tag_values()` | `sort_system_key_values()` |
| `_predict_system_tag_schema()` | `_predict_system_key_schema()` |
| `_compute_system_tag_suffixes()` | `_compute_system_key_suffixes()` |
| `_rename_sys_tags()` | `_rename_sys_keys()` |
| `_sort_merged_system_tags()` | `_sort_merged_system_keys()` |
| `tag_columns` (property) | `key_columns` |

### Attributes

| Old | New |
|-----|-----|
| `_system_tags` | `_system_keys` |
| `_system_tags_python_schema` | `_system_keys_python_schema` |
| `_system_tags_table` | `_system_keys_table` |

### Constants — identifiers and string values

| Old identifier | New identifier | Old value | New value |
|---|---|---|---|
| `SYSTEM_TAG_PREFIX_NAME` | `SYSTEM_KEY_PREFIX_NAME` | `"tag"` | `"key"` |
| `SYSTEM_TAG_SOURCE_ID_FIELD` | `SYSTEM_KEY_SOURCE_ID_FIELD` | unchanged | — |
| `SYSTEM_TAG_RECORD_ID_FIELD` | `SYSTEM_KEY_RECORD_ID_FIELD` | unchanged | — |
| `SYSTEM_TAG_PREFIX` | `SYSTEM_KEY_PREFIX` | generates `"_tag_"` | generates `"_key_"` |
| `SYSTEM_TAG_SOURCE_ID_PREFIX` | `SYSTEM_KEY_SOURCE_ID_PREFIX` | `"_tag_source_id"` | `"_key_source_id"` |
| `SYSTEM_TAG_RECORD_ID_PREFIX` | `SYSTEM_KEY_RECORD_ID_PREFIX` | `"_tag_record_id"` | `"_key_record_id"` |

### Type aliases & ColumnConfig fields

| Old | New |
|-----|-----|
| `TagValue` | `KeyValue` |
| `system_tags: bool` | `system_keys: bool` |
| `sort_by_tags: bool` | `sort_by_keys: bool` |

### File renames

| Old | New |
|-----|-----|
| `src/orcapod/core/datagrams/tag_data.py` | `src/orcapod/core/datagrams/key_data.py` |
| `test-objective/unit/test_tag.py` | `test-objective/unit/test_key.py` |

### Column name prefix strings (serialized Arrow table column headers)

| Old | New |
|-----|-----|
| `_tag_` | `_key_` (e.g. `_tag_source_id` → `_key_source_id`) |
| `_tag::` | `_key::` (e.g. `_tag::source:abc123` → `_key::source:abc123`) |

## Execution Approach

### Phase 1 — File renames

Use `git mv` to rename the two files with `tag` in their name, then update all import
statements that reference them.

### Phase 2 — Scripted identifier rename

A Python script applies the full rename map across all `.py` files using word-boundary
replacement, working from most-specific to least-specific pattern to avoid partial
substitutions. Handles all case variants:

- `tags` → `keys`, `tag` → `key`
- `Tags` → `Keys`, `Tag` → `Key`
- `TAGS` → `KEYS`, `TAG` → `KEY`

Targets: `src/`, `tests/`, `test-objective/`, `examples/`, `notebooks/`

Run `uv run pytest tests/` after this phase to catch any broken identifiers.

### Phase 3 — Manual string value fixes

- `system_constants.py`: set `SYSTEM_KEY_PREFIX_NAME = "key"` (drives the `_key_` column prefix)
- Scan for any remaining `"_tag_"` or `"_tag::"` string literals and update to `"_key_"` / `"_key::"`
- Verify no stray `"tag"` string literals remain in error messages or log strings

### Phase 4 — Docs & config files

Manual updates to non-Python files:

- `CLAUDE.md` + `.zed/rules`: update all terminology (class names, column prefix table,
  architecture overview section)
- `orcapod-design.md`: update all ~81 occurrences (section headings, concept descriptions,
  column name examples)
- `superpowers/specs/` and `docs/specs/`: update any existing spec docs that reference tag
  terminology
- `examples/`, `notebooks/`: update terminology in prose

### Phase 5 — Test run & cleanup

- `uv run pytest tests/` — fix any remaining failures
- Final grep: `grep -r '\btag\b\|\bTag\b\|\bTAG\b' src/ tests/` to confirm no stragglers
  (reviewing for intentional exceptions such as "git tag" in RELEASING.md)
- `uv run pytest tests/` — confirm fully green before PR

### Phase 6 — Single commit

All changes land in one commit:

```
refactor: rename tag → key across orcapod-python (ENG-455)
```

## Edge Cases

| Case | Risk | Resolution |
|------|------|-----------|
| `RELEASING.md` mentions "git tag" | Must NOT rename | Script targets only `.py` files; docs updated manually with explicit exclusion |
| `sorted(..., key=...)` Python builtin | `key=` keyword arg must not be renamed | Script uses word-boundary matching — `key=` is not a `tag` pattern so no collision |
| `dict.keys()` calls | No identifier collision | Monitor for readability confusion only; no action needed |
| `"system key"` could read as auth credential | Noted in issue | Accept the name; docstrings clarify it refers to provenance/dimensional columns |
| `tag_columns` property in `pipeline/observability_reader.py` | Must become `key_columns` | Caught by script |
| `_tag::` column format in Arrow tables | Both the constant AND generated string values need updating | Phase 3 manual pass; pre-v0.1 artifacts are not expected to load |
| Two `MapTags` class definitions | One in `column_selection.py`, one in `mappers.py` | Script renames both to `MapKeys` |
| CLAUDE.md and `.zed/rules` are non-Python | Not covered by Phase 2 script | Phase 4 manual update |

## Out of Scope

- orcapod-rust rename (separate sibling work)
- `packets` → `data` rename (already landed in ENG-454)
- Pipeline / PipelineJob refactor (tracked separately)
- Migration tooling for older saved artifacts
