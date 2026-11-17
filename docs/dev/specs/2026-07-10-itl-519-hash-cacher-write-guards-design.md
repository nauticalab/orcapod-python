# ITL-519: FileHasher Cache — Read-Only Mode + Minimum File Size Threshold

## Overview

Extend `InMemoryHashCacher` and `SqliteHashCacher` with two orthogonal write
guards that give callers finer control over when the cache is written to:

1. **`read_only: bool = False`** — the cacher performs lookups normally but all
   `put()` calls are silent no-ops.
2. **`min_cache_size_bytes: int | None = None`** — even when the cacher is
   writable, files whose `size` is below the threshold are not inserted.

Both knobs are applied in `put()`. Reads (`get()`) are always unchanged.

---

## Goals & Success Criteria

- `InMemoryHashCacher` and `SqliteHashCacher` accept `read_only` and
  `min_cache_size_bytes` with the defaults above.
- `get()` is unaffected by either knob — cache hits still work normally for
  entries that were written before the guard was enabled.
- `put()` guard order:
  1. If `read_only=True` → return immediately (no write, no exception).
  2. Else if `min_cache_size_bytes` is truthy and `key.size < min_cache_size_bytes`
     → return immediately.
  3. Otherwise → proceed with the existing write.
- `None` and `0` for `min_cache_size_bytes` both disable the threshold guard
  (falsy check; `0` means "all files qualify").
- `enable_file_hash_caching()` exposes both knobs as keyword arguments and
  forwards them to `SqliteHashCacher`.
- `__repr__` on each cacher includes `read_only` and `min_cache_size_bytes`
  so the state is visible in logs and debuggers.
- Tests pass for all four scenarios (read-only only, threshold only, combined,
  default/no guards).
- Docs updated with activation snippets for each knob.

---

## Design

### 1. Cacher constructor changes

Both cachers gain two new keyword arguments (keyword-only, after the existing
positional args):

```python
class InMemoryHashCacher:
    def __init__(
        self,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        self._cache: dict[FileHashKey, ContentHash] = {}
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
```

```python
class SqliteHashCacher:
    def __init__(
        self,
        db_path: Path | None = None,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        ...
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._ensure_schema()  # unchanged
```

### 2. `put()` guard

Both cachers share the same two-line guard at the top of `put()`:

```python
def put(self, key: FileHashKey, value: ContentHash) -> None:
    if self._read_only:
        return
    if self._min_cache_size_bytes and key.size < self._min_cache_size_bytes:
        return
    # ... existing write logic unchanged
```

`get()` is completely unchanged in both cachers.

### 3. `__repr__`

Both cachers implement `__repr__` showing their configuration:

```python
# InMemoryHashCacher
def __repr__(self) -> str:
    return (
        f"InMemoryHashCacher("
        f"read_only={self._read_only!r}, "
        f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
    )

# SqliteHashCacher
def __repr__(self) -> str:
    return (
        f"SqliteHashCacher("
        f"db_path={str(self.db_path)!r}, "
        f"read_only={self._read_only!r}, "
        f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
    )
```

### 4. `enable_file_hash_caching()` update

```python
def enable_file_hash_caching(
    db_path: "Path | None" = None,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
) -> None:
```

The two new args are forwarded to `SqliteHashCacher`:

```python
cached_file_hasher = CachedFileHasher(
    file_hasher=base_hasher,
    cacher=SqliteHashCacher(
        db_path,
        read_only=read_only,
        min_cache_size_bytes=min_cache_size_bytes,
    ),
)
```

No other changes to the function body.

---

## Tests

New file: `tests/test_hashing/test_hash_cacher_write_guards.py`

Tests are parametrized over both cacher types. `InMemoryHashCacher` is used
directly; `SqliteHashCacher` uses a `tmp_path` fixture.

### Read-only cacher

- `get()` on a pre-existing entry (added via a non-read-only instance sharing
  the same DB / dict) still returns the value — reads unaffected.
- `put()` then `get()` returns `None` — put is a no-op.
- `CachedFileHasher` with a read-only cacher: `hash_file()` returns the correct
  hash (computed by the inner `FileHasher`), but `cacher.get(key)` afterward
  returns `None`.
- `read_only=False` (default): `put()` stores normally.

### Threshold cacher

- `put(key_with_size=50, ...)` with `min_cache_size_bytes=100` → `get()` is `None`.
- `put(key_with_size=100, ...)` with `min_cache_size_bytes=100` → stored (boundary
  is inclusive: `>=`).
- `put(key_with_size=200, ...)` with `min_cache_size_bytes=100` → stored.
- `min_cache_size_bytes=None` (default) → all files stored.
- `min_cache_size_bytes=0` → all files stored (falsy, same as `None`).

### Combined

- `read_only=True` + `min_cache_size_bytes=100` + large file → nothing written
  (read-only wins).
- `read_only=False` + `min_cache_size_bytes=100` + large file → stored (both
  conditions allow write).

### `enable_file_hash_caching()` integration

Added to the existing `TestEnableFileHashCaching` class:

- `read_only=True` → the `SqliteHashCacher` in the registered `CachedFileHasher`
  has `_read_only=True`.
- `min_cache_size_bytes=1024` → passes through to the cacher.

---

## Documentation update

File: `docs/concepts/file-hash-caching.md`

Add a new section **"Controlling when the cache is written"** after the existing
"Activating file hash caching" section:

### Read-only mode

Use `read_only=True` when you want lookups from a shared/authoritative cache but
must not add new entries to it — for example, when consuming a cache pre-populated
by `populate_hash_cache()` without polluting it with ad-hoc entries.

```python
op.enable_file_hash_caching(db_path="/shared/cache.db", read_only=True)
```

Cache hits still work normally. Misses fall through to direct hashing; the result
is returned to the caller but is never stored.

### Minimum file size threshold

Use `min_cache_size_bytes` to skip the cache write overhead for small files.
Direct hashing of small files is fast enough that cache lookup + write is not
worth paying.

```python
# Skip caching for files smaller than 1 MB
op.enable_file_hash_caching(min_cache_size_bytes=1_048_576)
```

Files smaller than the threshold are still hashed and the hash is returned to the
caller — they are just not inserted into the cache. Files at or above the threshold
behave normally. Set to `None` or `0` to disable the threshold (default).

### Combining both

`read_only=True` and `min_cache_size_bytes` compose independently. `read_only`
takes precedence: when enabled, nothing is written regardless of file size.
`min_cache_size_bytes` is an additional guard that applies only when the cacher
is otherwise writable.

Also update the "When caching helps — and when it doesn't" section:

- Add one sentence to the "does not help much" list: "Files are small. Use
  `min_cache_size_bytes` to skip caching small files automatically."

---

## Scope & Boundaries

In scope:
- `InMemoryHashCacher` and `SqliteHashCacher` constructor + `put()` + `__repr__`
- `enable_file_hash_caching()` signature + body
- New test file + additions to `TestEnableFileHashCaching`
- `docs/concepts/file-hash-caching.md` update

Out of scope:
- Read-only enforcement at the SQLite file/connection layer (`PRAGMA query_only`)
- Per-namespace or per-path cache rules
- Cache eviction, TTL, or size cap
- Changes to `CachedFileHasher`, `CacherProtocol`, or `FileHashKey`

---

## Dependencies

- ITL-472 (FileHasher + HashCacher + SqliteHashCacher) — complete.
- Aligns with ITL-511 activation docs — new knobs should appear in that doc too
  once ITL-511 lands.
