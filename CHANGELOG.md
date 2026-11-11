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

#### Removed the `pygraphviz` dependency (ITL-134)

`pygraphviz` was declared as a dependency but never imported. It is built from
source against the Graphviz C headers, so installing Orcapod failed on systems
without `libgraphviz-dev` (or with a Graphviz newer than `pygraphviz` supports).
Installing Orcapod no longer needs a C toolchain or Graphviz headers. Rendering
pipeline graphs still uses the pure-Python `graphviz` package and therefore the
optional Graphviz `dot` binary at render time.

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

#### `GroupBy` and `Batch` accept already aggregated streams (ITL-849)

`GroupBy` or `Batch` after `MergeJoin`, `Batch` or `GroupBy` no longer raises
`ArrowTypeError: Expected bytes, got a 'list' object` when its rows are materialized. A list
column becomes a list of its incoming list type (`large_list<large_list<int8>>`, element types
unchanged, `Path` as `list[list[Path]]`), and its source info is a list of the members' source
info. Hashes of pipelines that already worked are unchanged.

### Fixed

#### Materialized rows keep their own source info (ITL-845)

When rows were collected and turned back into a stream, every row got the source-info
(`_source_*`) tokens of the first row, so rows after the first were attributed to row 0. This
affected the sync orchestrator, which does this between every pair of nodes (so every
function-node tag table written by a sync `PipelineJob` recorded the wrong input provenance),
the async path of every operator that collects its input (`MergeJoin`, `Batch`, `GroupBy`,
`PolarsFilter` and the other barrier operators), sync `Index` and `Pick`, and operator nodes
(`OperatorJobNode.execute` and `async_execute`). After `Batch`, a batch could even list more
tokens than it had members. Each row now keeps its own tokens.

Only `_source_*` values change. Data values, Arrow types and stored schemas, content hashes,
pipeline hashes and function-node record IDs are unchanged. The one stored ID that covers
`_source_*` values is an operator node's per-row `_record_hash`: it changes for every row
whose token was wrong, and stays the same for rows whose token was right.

**Stored data is not repaired.** Rows stored before this fix (function-node tag tables, and
operator-node tables written with `cache_mode=LOG`) keep the wrong source-info token for every
row after the first; nothing else in them is wrong. Orcapod does not rewrite, repair or
recompute them: a re-run serves function-node rows from the cache unchanged, but stores
corrected operator-node rows next to the old ones, so that table then holds both versions of
those rows. To get correct stored values, drop the pipeline tables (the function-node tag
tables, whose paths end in `pdb_v1`, and any operator-node tables) and re-run the job. The
function result tables (under `_result/`, ending in `rdb_v1`) can be kept: the re-run then
serves every result from them and runs no function again. A table's path is the job name
followed by the node's `node_identity_path` (plus `pdb_v1` for a function node); in a
`DeltaTableDatabase` it is that directory under the root, and the SQL connectors name the
table by joining the path components with `__`, each with non-alphanumeric characters
replaced by `_`. Correcting stored values is tracked in ITL-853.

### Breaking Changes

#### Operator outputs keep their inputs' Arrow types, normalized to large variants (ITL-832, ITL-813)

`MergeJoin` no longer infers Arrow types from Python values, and it no longer fails on a
zero-row input. Every output column of `MergeJoin` and `PolarsFilter` takes the
large-normalized form of its input Arrow type: `string` becomes `large_string`, `binary`
becomes `large_binary` and `list` becomes `large_list`, recursively. Width, nullability and
logical types are kept.

| Column | Before | After |
|---|---|---|
| `MergeJoin` merged column, `int8` input | `list<int64>` | `large_list<int8>` |
| `MergeJoin` merged column, `float32` input | `list<double>` | `large_list<float>` |
| `MergeJoin` merged column, `string` / `large_string` input | `list<string>` | `large_list<large_string>` |
| `MergeJoin` merged column, `decimal128(5, 2)` input | precision taken from the data | `large_list<decimal128(5, 2)>` |
| `MergeJoin` merged source-info column | `list<string>` | `large_list<large_string>` |
| `MergeJoin` result with a zero-row input | raised | same types as a non-empty result |
| `Batch` / `GroupBy` list column, `int8` input | `list<int8>` | `large_list<int8>` |
| `MergeJoin` / `PolarsFilter` pass-through `string` column | `large_string` (via Polars) | `large_string` |

**Inputs that disagree:** when the two inputs of `MergeJoin` give a colliding data column or a
shared tag column different Arrow types, both are normalized first. If they are then the same
(for example `string` and `large_string`), the join proceeds. Otherwise (for example `int8` and
`int64`, two timestamp units, or two decimal precisions) it now raises `InputValidationError`
naming the column and both types; cast one input explicitly. This used to produce a type
inferred from the values, or a Polars `SchemaError`.

**Hashes:** large and non-large variants hash identically, so normalization and
`list` → `large_list` change no content hash. The one change is that a merged column narrower
than `int64`/`double` no longer widens, so the data content hash of each row containing such a
merged column changes, along with the stored record of any operator node or function node that
consumes it. Tag hashes and pipeline hashes are unchanged.

**Nested nullability:** `normalize_to_large_types` now keeps every nested field's nullability
and metadata. A column such as `list<item: int64 not null>` keeps its non-null item flag
through `Data`, operators and storage instead of losing it. Measured `Data`, `Tag`, stream
and source hashes are unchanged; only the stored Arrow schema of such columns changes.

**Struct Python types:** `struct<b: string>` and `struct<b: large_string>` now map to one
synthesized Python class. Hashes are unchanged.

**Logical list elements:** an aggregating operator (`Batch`, `GroupBy`, `MergeJoin`) over a
column whose extension type has no logical list type in the type converter (for example
PyArrow's canonical `arrow.uuid`) raises a `ValueError` naming the column. It used to fail
with `ArrowNotImplementedError: extension`.

#### `Join` gives the same rows on its async path as on its sync path (ITL-837)

`Join`'s async path (`async_execute`, used by async pipelines) rebuilt every merged row from
Python values, so each column's Arrow type was inferred again: an `int16` tag or an `int8`
data column became `int64`, and a `dict` column could not be built at all. It now builds the
merged row from the input rows' Arrow tables, for two inputs and for three or more. Types,
nullability, column order, system tags and content hashes now equal those of the sync path.

**Hashes:** the `Tag` and `Data` content hashes of rows produced by an async `Join` over
columns narrower than `int64`/`double` change (they now equal the sync values), along with the
stored input hashes of any function node downstream of such a join. Sync hashes, stream content
hashes and pipeline hashes are unchanged.

**Behaviour change:** when a shared tag key has different Arrow types in two `Join` inputs
(for example `int16` and `int32`), `Join` now raises `InputValidationError` naming the column
and both types, on both paths. The sync path used to keep the right-hand input's type silently.
Types that differ only in layout (`string` and `large_string`) still join.

**Also fixed:** `Datagram.copy()` gave a copy of an Arrow-backed datagram an empty Python
`schema()` when the schema had not been computed yet; async `Index` and `Pick` returned such
rows (ITL-859).

#### `Join` no longer carries its inputs' meta columns (ITL-854, ITL-857)

`Join` now drops every input's `Data` meta (`__`-prefixed) columns, on the sync and async
paths, for two inputs or more: for example a cached function pod's `__computed` flag, the
result cache's `__pf_*` and `__pod_ts` columns, and the `__` columns of a raw
`ArrowTableStream`, along with their `_source___*` source-info columns. Previously the sync
path kept them, renaming colliding names with an index suffix (`__computed_1`), while the
async path dropped them. That rename could also give two columns the same name and fail with
a Polars `DuplicateError` (ITL-857). Nothing downstream of a `Join` reads these columns, and
none of them is hashed: content hashes, pipeline hashes, record IDs and stored tables are
unchanged. `Join.output_schema()` no longer lists the `__` columns a raw `ArrowTableStream`
reports as data, matching the joined table. Two inputs that share only a meta column name now
join instead of failing validation.

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
