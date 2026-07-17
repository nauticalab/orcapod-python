# Orcapod Schema Versioning + Pipeline/Result DB Migration (ITL-535)

**Date:** 2026-07-17
**Status:** Approved
**Issues:** ITL-535 (backfill migration), builds the schema versioning framework

---

## Overview

Two things happen in this work:

1. **Schema versioning framework** — a first-class, principled policy for how Orcapod manages
   breaking changes to its internal table schemas. Version identity is encoded directly in the
   storage path (a tuple of strings already accepted by `ArrowDatabaseProtocol`), so no new
   metadata infrastructure is required.

2. **v0 → v1 migration** — the first concrete migration: promotes pre-existing (v0, un-versioned)
   pipeline DB and result DB tables to v1 layout, which adds two new columns to pdb, converts all
   orcapod-produced `ContentHash` values from `large_string` to `large_binary`, and provides the
   hash needed for empty-datagram flow-through introduced in ITL-534.

---

## Schema Versioning Policy

### Rule: schema migrations are gated on minor version increments

A new Orcapod **minor version release** (e.g. v0.1.x → v0.2.x) is the only event that may
introduce a schema migration. Patch releases (v0.2.0 → v0.2.1) may never change schemas.

During active development of an upcoming minor release (e.g. on the `dev` branch targeting
v0.2.0), table schemas are **not** stable commit-to-commit. Users running pre-release builds
accept this.

**Upon every minor release**, Orcapod provides exactly one migration path:
`(previous stable schema)` → `(current stable schema)`. Users on v0.1.x can always run a single
migration command to reach the v0.2.x schema. There is no requirement to step through
intermediate development snapshots.

### Version numbering

Schema versions are simple monotone integers starting at 1 for the first explicitly versioned
release. The pre-versioning state (all tables created before this framework) is retroactively
called **v0**.

The schema version for each table type progresses independently — pdb and rdb may be at
different versions if only one of them changed in a given minor release.

### Version encoded in path

The version suffix is appended as the last component of the storage path tuple passed to
`ArrowDatabaseProtocol`:

```
pipeline DB path:  node_identity_path + ("pdb_v1",)
result DB path:    pod_record_path    + ("rdb_v1",)
```

v0 tables have no suffix — their path is the bare `node_identity_path` / `pod_record_path`.

Different schema versions therefore live at **different physical paths** and cannot interfere.
Migration is a read-from-old-path → transform → write-to-new-path operation; no in-place
Delta Lake update is required.

### Backward-compatibility guarantee

For any pair of consecutive minor releases `vX.Y` and `vX.(Y+1)`, Orcapod ships a migration
command that upgrades all affected table types from the `vX.Y` stable schema to the
`vX.(Y+1)` stable schema. Running the migration is opt-in; without it, old tables remain
at v0 paths and new code silently recomputes everything (because v1 paths are initially empty).

---

## Schema Version Changelog

### `ContentHash` binary serialization convention

Starting with v1 of every table that stores them: all orcapod-produced `ContentHash` values are
serialized to `large_binary` using `ContentHash.to_prefixed_digest()`, which returns
`b"{method}:{raw_digest_bytes}"` (method name as ASCII, colon separator, then the raw binary
digest). Parsing uses the new `ContentHash.from_prefixed_digest(data: bytes) -> ContentHash`
classmethod.

External identifiers that are not orcapod-produced `ContentHash` objects (e.g. git commit SHA
strings) remain as `large_string`.

---

### Pipeline DB (pdb)

#### pdb v0 — unversioned, pre-v0.2 (path: bare `node_identity_path`)

Created by all `FunctionJobNode` code prior to v0.2.

| Column | Type | Notes |
|--------|------|-------|
| `__record_id` *(DB-internal)* | `large_binary` | `versioned_entry_id` = hash of (tag + system_tags + input_data_hash + node_content_hash + recomputation_index). Surfaced as `__pipeline_entry_id` on reads. |
| user tag columns | varies | user-defined |
| `_tag_*` system tag columns | `large_string` | |
| `_source_*` source info columns | varies | |
| `__data_id` | `large_binary(16)` | UUID7 cross-reference to rdb row (`DATA_RECORD_ID`) |
| `__node_content_hash` | `large_string` | `ContentHash.to_string()` — **changed in v1** |
| `__input_data_context_key` | `large_string` | |
| `__computed` | `bool` | |
| `__is_ephemeral` | `bool` | added ITL-507 |
| `__pipeline_base_entry_id` | `large_binary` | added ITL-508 |
| `__pipeline_recomputation_index` | `int32` | added ITL-508 |
| `__input_data_hash` | *(absent)* | **added in v1** |
| `__output_data_hash` | *(absent)* | **added in v1** |

Note: v0 tables written after ITL-534 code landed may have `__input_data_hash` and
`__output_data_hash` present as `large_string` — but they are still considered v0 because
they use the string serialization format and lack the version path suffix.

#### pdb v1 — v0.2.x (path: `node_identity_path + ("pdb_v1",)`)

| Column | Type | Notes |
|--------|------|-------|
| `__record_id` *(DB-internal)* | `large_binary` | same as v0 |
| user tag columns | varies | unchanged |
| `_tag_*` system tag columns | `large_string` | unchanged |
| `_source_*` source info columns | varies | unchanged |
| `__data_id` | `large_binary(16)` | unchanged |
| `__node_content_hash` | **`large_binary`** | `ContentHash.to_prefixed_digest()` — format changed from v0 |
| `__input_data_context_key` | `large_string` | unchanged |
| `__computed` | `bool` | unchanged |
| `__is_ephemeral` | `bool` | unchanged |
| `__pipeline_base_entry_id` | `large_binary` | unchanged |
| `__pipeline_recomputation_index` | `int32` | unchanged |
| `__input_data_hash` | **`large_binary`** | NEW — `ContentHash.to_prefixed_digest()` of input data |
| `__output_data_hash` | **`large_binary`** (nullable) | NEW — `ContentHash.to_prefixed_digest()` of output data; null if output was filtered |

---

### Result DB (rdb)

#### rdb v0 — unversioned, pre-v0.2 (path: bare `pod_record_path`)

Created by all `CachedFunctionPod` / `ResultCache` code prior to v0.2.

| Column | Type | Notes |
|--------|------|-------|
| `__record_id` *(DB-internal)* | `large_binary(16)` | UUID7 = `output_data.datagram_uuid.bytes`. Surfaced as `__data_id` (`DATA_RECORD_ID`) on reads — matches pdb's `__data_id`. |
| `__input_data_hash` | `large_string` | `ContentHash.to_string()` — primary lookup key — **changed in v1** |
| `__pf_var_function_name` | `large_string` | unchanged in v1 |
| `__pf_var_function_signature_hash` | `large_string` | `ContentHash.to_string()` — **changed in v1** |
| `__pf_var_function_content_hash` | `large_string` | `ContentHash.to_string()` — **changed in v1** |
| `__pf_var_git_hash` | `large_string` | external git SHA1 — unchanged in v1 |
| `__pf_exec_executor_type` | `large_string` | unchanged in v1 |
| `__pf_exec_executor_info` | varies | unchanged in v1 |
| `__pf_exec_python_version` | `large_string` | unchanged in v1 |
| `__pf_exec_extra_info` | varies | unchanged in v1 |
| `__pod_ts` | `timestamp(us, UTC)` | unchanged in v1 |
| output data columns | varies | user-defined, unchanged |
| `_source_*` source info columns | varies | unchanged |
| `_context_key` | `large_string` | unchanged |

#### rdb v1 — v0.2.x (path: `pod_record_path + ("rdb_v1",)`)

| Column | Type | Notes |
|--------|------|-------|
| `__record_id` *(DB-internal)* | `large_binary(16)` | unchanged |
| `__input_data_hash` | **`large_binary`** | `ContentHash.to_prefixed_digest()` — format changed; still primary lookup key |
| `__pf_var_function_name` | `large_string` | unchanged |
| `__pf_var_function_signature_hash` | **`large_binary`** | `ContentHash.to_prefixed_digest()` — format changed |
| `__pf_var_function_content_hash` | **`large_binary`** | `ContentHash.to_prefixed_digest()` — format changed |
| `__pf_var_git_hash` | `large_string` | external identifier, unchanged |
| `__pf_exec_executor_type` | `large_string` | unchanged |
| `__pf_exec_executor_info` | varies | unchanged |
| `__pf_exec_python_version` | `large_string` | unchanged |
| `__pf_exec_extra_info` | varies | unchanged |
| `__pod_ts` | `timestamp(us, UTC)` | unchanged |
| output data columns | varies | user-defined, unchanged |
| `_source_*` source info columns | varies | unchanged |
| `_context_key` | `large_string` | unchanged |

---

## Architecture

### `ContentHash` changes

Add `ContentHash.from_prefixed_digest(data: bytes) -> ContentHash` classmethod:

```python
@classmethod
def from_prefixed_digest(cls, data: bytes) -> "ContentHash":
    colon_idx = data.index(b":")
    method = data[:colon_idx].decode()
    digest = data[colon_idx + 1:]
    return cls(method=method, digest=digest)
```

`to_prefixed_digest()` already exists and returns `b"{method}:{raw_digest_bytes}"`.

### Path versioning constants

Added to `system_constants.py`:

```python
PIPELINE_DB_SCHEMA_VERSION = "pdb_v1"
RESULT_DB_SCHEMA_VERSION   = "rdb_v1"
```

### `FunctionJobNode` / `CachedFunctionPod` changes

`FunctionJobNode`:
- `node_identity_path` property appends `constants.PIPELINE_DB_SCHEMA_VERSION` as the last
  component when accessing the pipeline DB
- All `ContentHash` values written via `to_prefixed_digest()` / read via `from_prefixed_digest()`
- ITL-508 hard-fail guard in `add_pipeline_record()` removed — v1 tables are always freshly
  written; old v0 rows cannot be present at a v1 path

`CachedFunctionPod` / `ResultCache`:
- `record_path` for the result DB appends `constants.RESULT_DB_SCHEMA_VERSION`
- `ResultCache.lookup()` and `store()` use `to_prefixed_digest()` / `from_prefixed_digest()`
  for `INPUT_DATA_HASH_COL`
- `get_function_variation_data()` hash fields serialized as binary in `ResultCache.store()`

### Migration package: `src/orcapod/migrations/`

```
src/orcapod/migrations/
├── __init__.py          # public API: migrate_pipeline, migrate_result
├── types.py             # MigrationResult dataclass
├── pipeline_db.py       # v0→v1 migration for pdb
└── result_db.py         # v0→v1 migration for rdb
```

#### `types.py`

```python
@dataclass
class MigrationResult:
    rows_total: int          # rows found at v0 path
    rows_migrated: int       # successfully written to v1 path
    rows_skipped: int        # already at v1 (idempotent re-run) or irrecoverable
    rows_unresolvable: int   # result data gone (ephemeral expired); hash not backfillable
    elapsed_s: float
    dry_run: bool
```

#### `pipeline_db.py` — `migrate_pipeline_v0_to_v1()`

Signature:
```python
def migrate_pipeline_v0_to_v1(
    pipeline_db: ArrowDatabaseProtocol,
    pipeline_path: tuple[str, ...],       # bare v0 path (no pdb_v1 suffix)
    result_db: ArrowDatabaseProtocol,
    result_path: tuple[str, ...],         # bare v0 rdb path (no rdb_v1 suffix)
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult
```

Algorithm per batch:
1. Read records from `pipeline_path` (v0)
2. Skip rows already present at `pipeline_path + ("pdb_v1",)` (idempotency)
3. For each row:
   - Convert `__node_content_hash` from string → binary
   - Recover `__input_data_hash`: look up rdb v0 row by `__data_id`; read its
     `__input_data_hash` string column; re-encode as binary
   - Recover `__output_data_hash`: load result data from rdb v0 by `__data_id`;
     reconstruct `Data` object; call `content_hash().to_prefixed_digest()`
   - If result not found (ephemeral expired): record as `rows_unresolvable`;
     write row without `__output_data_hash` (null — same degraded state as before)
4. Write batch to `pipeline_path + ("pdb_v1",)` with `skip_duplicates=True`

Convenience wrapper:
```python
def migrate_node(
    node: FunctionJobNode,
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult
```

#### `result_db.py` — `migrate_result_v0_to_v1()`

Signature:
```python
def migrate_result_v0_to_v1(
    result_db: ArrowDatabaseProtocol,
    result_path: tuple[str, ...],         # bare v0 path (no rdb_v1 suffix)
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult
```

Algorithm per batch:
1. Read records from `result_path` (v0)
2. Skip rows already present at `result_path + ("rdb_v1",)` (idempotency)
3. For each row: convert `__input_data_hash`, `__pf_var_function_signature_hash`,
   `__pf_var_function_content_hash` from `ContentHash.to_string()` format →
   `ContentHash.from_string()` → `to_prefixed_digest()` bytes
4. Write batch to `result_path + ("rdb_v1",)` with `skip_duplicates=True`

### CLI: `src/orcapod/cli/migrate.py`

Two sub-commands under `orcapod migrate`:

```
orcapod migrate pipeline-db PIPELINE_DB_PATH RESULT_DB_PATH NODE_PATH...
    [--dry-run] [--batch-size N] [--verbose] [--json-summary]

orcapod migrate result-db RESULT_DB_PATH RECORD_PATH...
    [--dry-run] [--batch-size N] [--verbose] [--json-summary]
```

Output (human-readable by default):
```
Migrating pipeline DB: /path/to/pipeline_db
  node path: my_pipeline/fn_abc123
  found 12 450 rows at v0 path
  [=========>        ] 6 200/12 450 rows  (51%)  ~42s remaining  220 rows/s
  ...done.
  migrated: 12 390   skipped (already v1): 0   unresolvable: 60
  elapsed: 56.3s
```

With `--json-summary`, prints a JSON object matching `MigrationResult` fields after completion.

### Concurrency

The migration runs while pods may be actively writing new v1 rows. This is safe because:
- New writes go to the v1 path; migration reads from v0 and writes to v1 with
  `skip_duplicates=True`
- Rows written after migration starts are at the v1 path already; they will not be
  re-processed (idempotency check skips them)
- The narrow window where a row appears at v0 but not yet migrated to v1 is harmless —
  it means the current pipeline run recomputes that row, then writes a fresh v1 row

### Rollback

Migration is considered irreversible. The v0 path is never modified or deleted by the migration
tool. To "rollback", a user simply stops using the v1 path (i.e., reverts to v0.1.x code) — the
v0 data is untouched. The migration tool does not provide a reverse direction.

---

## Documentation

A dedicated section in the user-facing docs (`docs/`) titled **"Schema versioning"** covers:

1. The versioning policy (minor-release gating, path encoding, no intra-release stability)
2. The migration command and when to run it
3. The schema version changelog (the tables above, kept up to date with every release)
4. Failure modes: what happens when result data is unresolvable; what `rows_unresolvable`
   means; how to inspect which rows were not fully backfilled

---

## Testing

- Unit tests for `ContentHash.from_prefixed_digest()` round-trip
- Unit tests for `migrate_pipeline_v0_to_v1()` and `migrate_result_v0_to_v1()`:
  - happy path: all result data resolvable → full backfill
  - partial path: some results expired → correct `rows_unresolvable` count
  - idempotent: running migration twice produces same result
  - dry-run: no writes occur
- Integration test: create v0 tables via old write path, migrate, assert v1 tables correct
- CLI smoke test: `orcapod migrate pipeline-db --dry-run` exits 0, prints expected summary
