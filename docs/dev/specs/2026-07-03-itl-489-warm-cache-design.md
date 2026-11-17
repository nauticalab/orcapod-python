# Warm Cache Design (ITL-489)

**Date:** 2026-07-03
**Issue:** [ITL-489](https://linear.app/enigma-metamorphic/issue/ITL-489)
**Status:** Approved

---

## Overview

Add a `populate_hash_cache()` utility function and an `orcapod warm-cache` CLI subcommand
that pre-populate the SQLite file-hash cache for large ephys recordings (default threshold:
500 MB). Rehashing multi-gigabyte recordings on every pipeline run is expensive; running
`orcapod warm-cache /data/recordings` once before a spike-sort run collapses all subsequent
hash lookups to a sub-millisecond SQLite read.

The implementation reuses `FileHasher` and `SqliteHashCacher` from ITL-472 verbatim.
No new cache backend is introduced.

---

## Design

### Core function: `populate_hash_cache()`

Lives in `src/orcapod/hashing/cache_population.py`.

```python
@dataclass(frozen=True)
class CachePopulationStats:
    hashed: int              # files newly hashed and written to cache
    already_cached: int      # cache hits — rehash skipped
    skipped_small: int       # below min_size_bytes — not hashed or cached
    errors: int              # files that raised an exception (logged as warnings)
    total_bytes_hashed: int  # sum of file sizes for newly hashed files
    total_duration: float    # wall-clock seconds for the full run
    avg_hashing_speed: float # bytes/second (total_bytes_hashed / total_duration; 0.0 if duration is zero)

def populate_hash_cache(
    path: PathLike,
    *,
    min_size_bytes: int = 500 * 1024 * 1024,   # 500 MB default
    db_path: UPath | PathLike | None = None,    # local paths only; see fallback below
    algorithm: str = "sha256",
    buffer_size: int = 65536,
) -> CachePopulationStats:
```

**`db_path` fallback (inherited from `SqliteHashCacher`):**
When `db_path=None`, the cacher checks the `ORCAPOD_HASH_CACHE_DB` environment variable,
then falls back to `~/.orcapod/file_hash_cache.db`. Only local paths are meaningful —
SQLite cannot operate on remote filesystems; passing a remote `UPath` will raise at
`SqliteHashCacher` construction.

**File traversal:**
All files under `path` are visited recursively via `UPath.iterdir()`. Hidden files
(names starting with `.`) are included. Symlinks are skipped — they are not real files
and following them could produce cycles. Subdirectories that raise `PermissionError`
during `iterdir()` are logged as warnings and counted in `errors`.

**Per-file logic:**
1. `stat()` the file to get `size` and `mtime_ns`.
2. If `size < min_size_bytes` → increment `skipped_small`, continue.
3. Construct `FileHashKey(resolved_path, mtime_ns, size)`, call `SqliteHashCacher.get()`.
   - Hit → increment `already_cached`, continue.
   - Miss → call `FileHasher.hash_file()`, call `SqliteHashCacher.put()`, increment
     `hashed` and add `size` to `total_bytes_hashed`.
4. Any exception on a single file → log warning with path, increment `errors`, continue.

`SqliteHashCacher` is used directly (not via `CachedFileHasher`) so the function has
precise hit/miss accounting for the stats fields.

---

### CLI: `orcapod warm-cache`

CLI framework: `typer>=0.12`, added to `[project.dependencies]`.

Registered in `pyproject.toml`:

```toml
[project.scripts]
orcapod = "orcapod.cli:app"
```

The `orcapod` entry point is a `typer.Typer()` app in `src/orcapod/cli/__init__.py`.
The `warm-cache` subcommand lives in `src/orcapod/cli/warm_cache.py` and is registered
via `app.add_typer()` or `@app.command()`.

**Usage:**

```
orcapod warm-cache PATH
    [--min-size FLOAT]     # MB, default 500.0
    [--db-path PATH]       # SQLite DB path; default: env var or ~/.orcapod/file_hash_cache.db
    [--algorithm TEXT]     # default sha256
    [--buffer-size INT]    # default 65536
```

**Example output:**

```
Scanning /data/recordings ...
Done in 14.2s — 12 hashed (45.2 GB), 4 already cached, 31 skipped (< 500 MB), 0 errors.
Average hashing speed: 3.18 GB/s
```

`--min-size` accepts MB as a float (e.g. `--min-size 1000` for 1 GB) and converts to
bytes internally. The `--db-path` option maps directly to the `db_path` argument of
`populate_hash_cache()`; omitting it triggers the same `SqliteHashCacher` fallback.

---

## File Layout

```
src/orcapod/
├── hashing/
│   ├── cache_population.py     # NEW: CachePopulationStats, populate_hash_cache()
│   └── __init__.py             # + CachePopulationStats, populate_hash_cache
└── cli/
    ├── __init__.py             # NEW: typer app root (app = typer.Typer())
    └── warm_cache.py           # NEW: warm-cache subcommand

tests/test_hashing/
└── test_cache_population.py   # NEW

pyproject.toml                 # + typer>=0.12 in dependencies
                               # + [project.scripts] orcapod = "orcapod.cli:app"
```

---

## Public API Additions

### `orcapod.hashing`

| Symbol | Type |
|---|---|
| `CachePopulationStats` | Frozen dataclass |
| `populate_hash_cache` | Function |

### `orcapod.cli` (new package)

| Symbol | Type |
|---|---|
| `app` | `typer.Typer` instance (root CLI app) |

---

## Tests

`tests/test_hashing/test_cache_population.py` covers:

- `test_skips_files_below_threshold` — files below `min_size_bytes` → `skipped_small` incremented, nothing written to DB
- `test_hashes_large_files` — files at or above threshold → `hashed` incremented, DB entry present
- `test_cache_hit_skips_rehash` — calling `populate_hash_cache` twice → second call sees `already_cached`
- `test_includes_hidden_files` — hidden files (`.hidden.bin`) above threshold are hashed
- `test_recursive_traversal` — nested subdirectories are visited
- `test_skips_symlinks` — symlinks are not followed or hashed
- `test_stats_total_bytes` — `total_bytes_hashed` equals the sum of qualifying file sizes
- `test_stats_duration_and_speed` — `total_duration > 0`, `avg_hashing_speed` matches bytes/duration
- `test_error_counted_on_permission_error` — mocked `PermissionError` increments `errors`
- `test_db_path_none_uses_default` — omitting `db_path` falls back to `SqliteHashCacher` default

---

## Out of Scope

- Caching file contents (this issue is about hashes only)
- Turso / libSQL backend (ITL-475, deferred)
- Cache eviction / TTL
- Following symlinks
- Remote SQLite / distributed cache
