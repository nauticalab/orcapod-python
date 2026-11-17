# Design: Rename `packets` → `data` across orcapod-python

**Linear issue:** ENG-454
**Date:** 2026-05-12
**Status:** Approved

## Overview

Rename every identifier containing `packet`/`packets`/`Packet` to `data`/`Data` throughout
orcapod-python. This is a hard break — no deprecation aliases, no migration shims — landing
before v0.1 stable.

## Goals & Success Criteria

- All public symbols, class names, attributes, methods, function arguments, and module names
  containing `packet`/`packets`/`Packet` renamed to `data`/`Data`.
- Internal/private identifiers renamed for consistency.
- All docstrings, comments, type stubs, error messages, and log strings use the new terminology.
- All examples, README, and docs updated.
- Test suite renamed and passing; test names that probed `packet`-named API are renamed.
- No deprecation shims, no backwards-compat aliases.
- `CHANGELOG.md` created with a `[Unreleased]` section calling out the rename with a
  name-mapping table.

## Scope & Boundaries

**In scope:**
- All `.py`, `.md`, `.rst`, `.ipynb`, `.toml` files in the repo
- Persisted/serialized field names — renamed in-place (pre-v0.1 artifacts not expected to load)
- File and directory renames (5 files, 1 directory)
- `CHANGELOG.md` creation

**Out of scope:**
- orcapod-rust rename
- `tags` → `keys` rename (tracked separately)
- Pipeline / PipelineJob refactor (tracked separately)
- Migration tooling for older saved artifacts

## Substitution Mapping

Substitutions are applied in longest/most-specific first order to prevent double-replacement.

### Class / type names (PascalCase)

| Old | New |
|---|---|
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunction` | `DataFunction` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `PacketLogger` | `DataLogger` |
| `PacketProtocol` | `DataProtocol` |
| `Packet` | `Data` |

### Methods, attributes, parameters (snake_case)

| Old | New |
|---|---|
| `packet_function_type_id` | `data_function_type_id` |
| `input_packet_schema` | `input_data_schema` |
| `output_packet_schema_hash` | `output_data_schema_hash` |
| `output_packet_schema` | `output_data_schema` |
| `_process_packet_internal` | `_process_data_internal` |
| `_async_process_packet_internal` | `_async_process_data_internal` |
| `_iter_packets_sequential` | `_iter_data_sequential` |
| `_iter_packets_concurrent` | `_iter_data_concurrent` |
| `_async_execute_one_packet` | `_async_execute_one_data` |
| `_build_output_packet` | `_build_output_data` |
| `get_cached_output_for_packet` | `get_cached_output_for_data` |
| `record_packet` | `record_data` |
| `async_process_packet` | `async_process_data` |
| `process_packet` | `process_data` |
| `execute_packet` | `execute_data` |
| `async_iter_packets` | `async_iter_data` |
| `iter_packets` | `iter_data` |
| `map_packets` | `map_data` |
| `select_packet_columns` | `select_data_columns` |
| `drop_packet_columns` | `drop_data_columns` |
| `packet_function` | `data_function` |
| `on_packet_start` | `on_data_start` |
| `on_packet_end` | `on_data_end` |
| `on_packet_crash` | `on_data_crash` |
| `create_packet_logger` | `create_data_logger` |
| `verify_packet_schema` | `verify_data_schema` |
| `_build_packet_function_registry` | `_build_data_function_registry` |
| `resolve_packet_function_from_config` | `resolve_data_function_from_config` |
| `register_packet_function` | `register_data_function` |
| `packet_schema` | `data_schema` |
| `input_packet` | `input_data` |
| `output_packet` | `output_data` |
| `packets` | `data` |
| `packet` | `data` |

### System constants

| Old | New |
|---|---|
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |

## File and Directory Renames

| Old path | New path |
|---|---|
| `src/orcapod/core/datagrams/tag_packet.py` | `src/orcapod/core/datagrams/tag_data.py` |
| `src/orcapod/core/packet_function.py` | `src/orcapod/core/data_function.py` |
| `src/orcapod/core/packet_function_proxy.py` | `src/orcapod/core/data_function_proxy.py` |
| `src/orcapod/protocols/core_protocols/packet_function.py` | `src/orcapod/protocols/core_protocols/data_function.py` |
| `tests/test_core/packet_function/` (directory) | `tests/test_core/data_function/` |

All `__init__.py` re-exports referencing the old module names are updated by the content
substitution pass before file renames occur.

## Execution Strategy

The rename runs as a Python script (`scripts/rename_packet_to_data.py`) in six phases:

### Phase 1 — Content substitution
Walk all files matching `*.py`, `*.md`, `*.rst`, `*.ipynb`, `*.toml`, `*.yaml`, `*.yml`,
`*.ini`, `*.cfg`, `*.txt`. Apply the full substitution table in longest-first order using
plain `str.replace` (not regex, to avoid false positives on partial word matches in
unrelated contexts). Write back changed files.

### Phase 2 — File renames
Use `git mv` for each file rename in the table above, preserving git history.

### Phase 3 — Directory rename
Use `git mv` to rename `tests/test_core/packet_function/` →
`tests/test_core/data_function/`.

### Phase 4 — Collision review
Inspect `git diff` for any same-scope `data` name clashes introduced by the rename.
Resolve on sight by making the existing `data` usage more specific (e.g. `variation_data`,
`exec_data`).

### Phase 5 — Verification
Run `uv run pytest tests/` — must pass clean before committing.

### Phase 6 — Cleanup
Delete `scripts/rename_packet_to_data.py` before committing (one-shot tool, not a
permanent artifact).

## Collision Handling

Any genuine same-scope collision (a renamed `packet` → `data` parameter clashes with an
existing `data` variable in the same function/method body) is resolved on sight by renaming
the pre-existing `data` usage to a more descriptive name. The number of real collisions is
expected to be small (< 10).

## Commit Strategy

Single atomic commit:
```
refactor: rename packet → data across orcapod-python (ENG-454)
```

This lands as one reviewable diff. Intermediate broken states are never committed.

## v0.1 Changelog

A new `CHANGELOG.md` is created at the repo root with the following content:

```markdown
# Changelog

## [Unreleased]

### Breaking Changes

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to `data`/`Data`.
No deprecation aliases. Pre-v0.1 artifacts will not load.

| Old name | New name |
|---|---|
| `Packet` | `Data` |
| `PacketProtocol` | `DataProtocol` |
| `PacketFunction` | `DataFunction` |
| `PacketFunctionBase` | `DataFunctionBase` |
| `PacketFunctionProtocol` | `DataFunctionProtocol` |
| `PacketFunctionProxy` | `DataFunctionProxy` |
| `PythonPacketFunction` | `PythonDataFunction` |
| `CachedPacketFunction` | `CachedDataFunction` |
| `PacketFunctionWrapper` | `DataFunctionWrapper` |
| `PacketFunctionExecutorProtocol` | `DataFunctionExecutorProtocol` |
| `PacketExecutionLoggerProtocol` | `DataExecutionLoggerProtocol` |
| `PacketLogger` | `DataLogger` |
| `SelectPacketColumns` | `SelectDataColumns` |
| `DropPacketColumns` | `DropDataColumns` |
| `MapPackets` | `MapData` |
| `iter_packets()` | `iter_data()` |
| `process_packet()` | `process_data()` |
| `async_process_packet()` | `async_process_data()` |
| `execute_packet()` | `execute_data()` |
| `map_packets()` | `map_data()` |
| `select_packet_columns()` | `select_data_columns()` |
| `drop_packet_columns()` | `drop_data_columns()` |
| `on_packet_start()` | `on_data_start()` |
| `on_packet_end()` | `on_data_end()` |
| `on_packet_crash()` | `on_data_crash()` |
| `INPUT_PACKET_HASH_COL` | `INPUT_DATA_HASH_COL` |
| `PACKET_RECORD_ID` | `DATA_RECORD_ID` |
```
