# ITL-511: Directory Hashing Uses FileHasher Cache

**Date:** 2026-07-08
**Issue:** [ITL-511](https://linear.app/enigma-metamorphic/issue/ITL-511/verify-opdirectory-hashing-uses-filehasher-cache-document-how-to)

## Problem

`BasicDirectoryHasher._hash_dir()` calls `hash_utils.hash_file()` directly for file
leaves (line 74 of `directory_hashers.py`). It does not use the injected `file_hasher`
at all. As a result, even after calling `enable_file_hash_caching()`, `op.Directory`
hashing bypasses the cache entirely — every directory hash recomputes every file hash
from scratch, making the caching optimisation dead weight for the workloads that need it
most (spike-sort pipelines).

Additionally, `enable_file_hash_caching()` only patches `FileHandler` (for `op.File`),
leaving `DirectoryHandler`/`BasicDirectoryHasher` untouched.

## Design

### Part 1 — `BasicDirectoryHasher`: required `file_hasher` parameter

`file_hasher: FileContentHasherProtocol` becomes a required first argument. The
`buffer_size` parameter is removed from `_hash_dir()` (the injected hasher owns that
detail). The direct `hash_file()` call on file leaves is replaced by
`file_hasher.hash_file(child)`.

```python
class BasicDirectoryHasher:
    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ) -> None:
```

**Hash-value stability:** `FileHasher(algorithm="sha256").hash_file()` returns the same
`ContentHash` as `hash_utils.hash_file(algorithm="sha256")`. Existing directory hashes
are unchanged in the default configuration.

**Call-site updates required:**
- `src/orcapod/hashing/directory_hashers.py` — `_hash_dir()` signature + file-leaf call
- `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` —
  `register_builtin_python_type_handlers()` fallback when `directory_hasher is None`
- `tests/test_hashing/test_directory_handler.py` — every `BasicDirectoryHasher()`
  construction gains `file_hasher=FileHasher()`

### Part 2 — Context wiring (`v0.1.json` + `register_builtin_python_type_handlers`)

`v0.1.json` wires the existing `file_hasher` reference into `BasicDirectoryHasher`:

```json
"directory_hasher": {
    "_class": "orcapod.hashing.directory_hashers.BasicDirectoryHasher",
    "_config": {
        "algorithm": "sha256",
        "file_hasher": {"_ref": "file_hasher"}
    }
}
```

`register_builtin_python_type_handlers()` fallback (when `directory_hasher is None`)
constructs `BasicDirectoryHasher(file_hasher=file_hasher, algorithm="sha256")` using the
already-resolved `file_hasher`. The `file_hasher` resolution block must remain above the
`directory_hasher` resolution block.

The changelog entry in `v0.1.json` records this change.

### Part 3 — `enable_file_hash_caching()` extended to cover `DirectoryHandler`

After patching `FileHandler`, the function also patches `DirectoryHandler` using the
**same** `CachedFileHasher` instance. This means a file cached via a direct `op.File`
hash is also a cache hit when encountered during directory traversal.

```python
existing_dir_handler = registry.get_handler_for_type(Directory)
existing_dir_hasher = existing_dir_handler.directory_hasher
registry.register(
    Directory,
    DirectoryHandler(
        BasicDirectoryHasher(
            file_hasher=cached_file_hasher,         # shared with FileHandler
            algorithm=existing_dir_hasher.algorithm,
            buffer_size=existing_dir_hasher.buffer_size,
        )
    ),
)
```

`DirectoryHandler` is simply re-registered fresh each call (the idempotency/unwrap loop
only applies to extracting the base `FileHasher` from `FileHandler`).

### Part 4 — Tests

**New file: `tests/test_hashing/test_directory_hash_caching.py`**

| Test | What it verifies |
|------|-----------------|
| `test_cache_write_on_first_hash` | Each file in the directory hits the underlying `FileHasher` exactly once; results stored in cache |
| `test_cache_hit_on_second_hash` | Second directory hash does not increment the underlying hasher call count |
| `test_cache_invalidated_on_file_change` | Modifying a file (changing `mtime_ns`/`size`) causes that file's hasher to be called again |
| `test_enable_file_hash_caching_wires_directory_handler` | After `enable_file_hash_caching()`, the default context's `DirectoryHandler.directory_hasher.file_hasher` is a `CachedFileHasher` |
| `test_shared_cache_between_file_and_directory_handlers` | After `enable_file_hash_caching()`, a file hashed via `op.File` first is a cache hit when the containing directory is hashed |

Uses `InMemoryHashCacher` for all unit tests (no SQLite I/O). A call-counting wrapper
around `FileHasher` tracks invocations.

**Updates to existing tests:**
- `test_directory_handler.py` — add `file_hasher=FileHasher()` to every
  `BasicDirectoryHasher()` constructor call
- `test_hash_cachers.py` — `enable_file_hash_caching()` tests verify the `DirectoryHandler`
  is also patched

### Part 5 — User-facing documentation

**New file: `docs/concepts/file-hash-caching.md`**

Sections:

1. **`FileHasher` + `HashCacher` relationship** — what each class does, how they compose
   into `CachedFileHasher`, how the cache key `(path, mtime_ns, size)` drives automatic
   invalidation
2. **Activation** — `enable_file_hash_caching()` one-liner with code snippet; note that
   it must be called at application startup before any hashing occurs
3. **`op.Directory` hashing** — explain that after activation, directory traversal
   consults the per-file cache automatically; no additional steps required
4. **Cache storage** — default path (`~/.orcapod/file_hash_cache.db`), `ORCAPOD_HASH_CACHE_DB`
   env var override, how to inspect with `sqlite3` CLI, how to clear
5. **Trade-offs** — when caching helps (large files rehashed across runs, directories with
   few changes between runs) vs. when it doesn't (small files, files that change every run,
   cold cache first run)

## Files Changed

| File | Change |
|------|--------|
| `src/orcapod/hashing/directory_hashers.py` | `file_hasher` required arg; `_hash_dir` uses it for file leaves |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | `register_builtin_python_type_handlers` fallback passes `file_hasher` |
| `src/orcapod/contexts/data/v0.1.json` | Wire `{"_ref": "file_hasher"}` into `BasicDirectoryHasher` config; add changelog entry |
| `src/orcapod/contexts/__init__.py` | `enable_file_hash_caching()` also patches `DirectoryHandler` |
| `tests/test_hashing/test_directory_hash_caching.py` | New — caching integration tests |
| `tests/test_hashing/test_directory_handler.py` | Add `file_hasher=FileHasher()` to all `BasicDirectoryHasher()` calls |
| `tests/test_hashing/test_hash_cachers.py` | Update `enable_file_hash_caching` tests for new DirectoryHandler patching |
| `docs/concepts/file-hash-caching.md` | New — user-facing activation guide |
