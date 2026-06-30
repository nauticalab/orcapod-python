# Changelog

## [Unreleased]

### New

#### `orcapod.File` — content-identified file type (ITL-450)

`orcapod.File` is a new first-class type for materialized files. It subclasses
`upath.extensions.ProxyUPath`, giving it full UPath-like behaviour across all
backends (local, S3, GCS, etc.) without polluting the global protocol registry.

**Construction validates eagerly:** `File(path)` raises `FileNotFoundError` if the path
does not exist, `IsADirectoryError` if it is a directory, and `ValueError` if it is not
a regular file. This means a `File` instance always refers to a file that existed at
construction time.

**Symlink handling:** By default (`follow_symlinks=True`) symlinks are followed. Pass
`follow_symlinks=False` to raise `ValueError` instead of following a symlink.

**Content hashing:** `orcapod.File` is the only type whose hash is derived from the
file's *content*. `pathlib.Path` and `upath.UPath` now hash from the path string only
(see Breaking Changes below).

**Arrow extension type:** `LogicalFile` stores `File` values as `large_string` (the path
string) with extension name `"orcapod.file"`. Re-reading a stored `File` column
re-validates existence, so missing files surface at read time.

```python
from orcapod import File

f = File("/data/results.csv")        # validates existence
f2 = File("s3://my-bucket/out.parquet")  # works with any UPath backend
```

**Exports:**
- `orcapod.File` — the `File` class (top-level package)
- `orcapod.extension_types.LogicalFile` — the Arrow extension type
- `orcapod.hashing.semantic_hashing.FileHandler` — the semantic hasher

### Changed

#### starfix v0.3.0 adoption + schema-metadata cleaner (PLT-1737)

Bumped `starfix` dependency to `~=0.3.0` and introduced a schema-metadata
cleaning step before all Arrow schema and table hashes.

**What changed:** `StarfixArrowHasher.hash_schema` and `hash_table` now strip
every metadata key that does not start with `ARROW:extension:` before passing
the schema to starfix. Only identity-bearing extension metadata (e.g.
`ARROW:extension:name`) is included in the hash. Unrelated keys (comments,
vendor annotations, source-file provenance, etc.) are ignored.

**Stability invariant:** Schemas with no `ARROW:extension:*` metadata anywhere
continue to produce byte-for-byte identical hashes to pre-v0.3.0 Orcapod.

**One-time hash invalidation:** Pipelines whose schemas contained
`ARROW:extension:*` keys *alongside* unrelated field metadata (e.g. a
`comment` key on the same field as `ARROW:extension:name`) will see a changed
hash. On-disk caches for those pipelines should be treated as stale and
recomputed.

**New utility:** `orcapod.hashing.schema_cleaner.clean_schema_for_hashing` and
`has_extension_metadata` are available as semi-public utilities for other
Orcapod code that needs to inspect or clean Arrow schema metadata.

### Breaking Changes

#### `pathlib.Path` and `upath.UPath` no longer hash file content (ITL-450)

`pathlib.Path` and `upath.UPath` values now hash from the **path string** only —
no file is read. This matches the semantics of all other string-like types in orcapod.

**Migration:** Replace any `Path`-typed pipeline column that requires content-based
identity with `orcapod.File`. `File` validates existence at construction and hashes
from file content, exactly as `Path` did before this change.

```python
# Before (Path hashed file content — now hashes path string instead)
{"input": Path("/data/results.csv")}

# After (File hashes file content, as Path previously did)
from orcapod import File
{"input": File("/data/results.csv")}
```

**Hash invalidation:** Any on-disk cache entry produced with a `Path`-typed data column
will no longer match. Treat those entries as stale and allow recomputation.

**Removed:** `PathHandler` and `UPathHandler` have been removed from
`orcapod.hashing.semantic_hashing`. Use `FileHandler` for content-based hashing.

#### `packets` → `data` rename (hard break)

All identifiers containing `packet`/`packets`/`Packet` have been renamed to
`data`/`Data`. No deprecation aliases. Pre-v0.1 artifacts will not load.

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
