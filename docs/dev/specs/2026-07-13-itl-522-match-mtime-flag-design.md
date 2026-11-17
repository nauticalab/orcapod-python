# ITL-522: FileHasher Cache — `match_mtime` Flag for Opt-In mtime Bypass

## Overview

Cache hits in `CachedFileHasher` currently require **path + mtime_ns + size** to all
match the stored entry. In environments where mtime is churned by benign operations
(rsync, restore-from-backup, container remounts, `touch` in CI), this causes spurious
misses and unnecessary re-hashing.

Add an opt-in `match_mtime: bool = True` flag to each hash cacher backend. When set
to `False`, the cache lookup ignores `mtime_ns` and matches on **path + size** only,
returning the entry with the **latest `mtime_ns`** when multiple hits exist. The write
path is unchanged — `mtime_ns` is always stored.

---

## Goals & Success Criteria

- `InMemoryHashCacher`, `SqliteHashCacher`, and `PostgresHashCacher` each accept
  `match_mtime: bool = True` as a keyword-only constructor argument.
- `enable_file_hash_caching()` exposes `match_mtime` and forwards it to the cacher.
- Default (`match_mtime=True`): existing strict behavior — cache hit only when
  `path`, `mtime_ns`, and `size` all match.
- `match_mtime=False`: cache hit when `path` and `size` match; `mtime_ns` is ignored
  in the lookup. Among multiple matching entries, the one with the **highest
  `mtime_ns`** is returned.
- Write path is unchanged in all cachers — `(path, mtime_ns, size, hash)` is always
  stored. Switching the flag on later still produces hits against entries written
  under `match_mtime=True`.
- Composes cleanly with `read_only` and `min_cache_size_bytes` (ITL-519 guards).
- `__repr__` on each cacher includes `match_mtime`.
- All five test scenarios from the issue spec pass (see Tests section).
- `docs/concepts/file-hash-caching.md` updated with the new knob.

---

## Design

### 1. Cacher constructor changes

All three cachers gain `match_mtime` as a new keyword-only argument after the
existing `min_cache_size_bytes`:

```python
class InMemoryHashCacher:
    def __init__(
        self,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
        match_mtime: bool = True,
    ) -> None:
        ...
        self._match_mtime = match_mtime

class SqliteHashCacher:
    def __init__(
        self,
        db_path: Path | None = None,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
        match_mtime: bool = True,
    ) -> None:
        ...
        self._match_mtime = match_mtime

class PostgresHashCacher:
    def __init__(
        self,
        conninfo: str,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
        match_mtime: bool = True,
    ) -> None:
        ...
        self._match_mtime = match_mtime
```

### 2. `get()` changes

**`InMemoryHashCacher`:**

```python
def get(self, key: FileHashKey) -> ContentHash | None:
    if self._match_mtime:
        return self._cache.get(key)
    # match_mtime=False: find all (path, size) matches and return
    # the one with the highest mtime_ns.
    best_key: FileHashKey | None = None
    best_value: ContentHash | None = None
    for cached_key, value in self._cache.items():
        if cached_key.path == key.path and cached_key.size == key.size:
            if best_key is None or cached_key.mtime_ns > best_key.mtime_ns:
                best_key = cached_key
                best_value = value
    return best_value
```

O(n) over the dict — acceptable since `InMemoryHashCacher` is intended for
testing and short-lived in-process use.

**`SqliteHashCacher`:**

```python
def get(self, key: FileHashKey) -> ContentHash | None:
    conn = self._connection()
    if self._match_mtime:
        cursor = conn.execute(
            "SELECT hash FROM file_hash_cache WHERE path=? AND mtime_ns=? AND size=?",
            (str(key.path), key.mtime_ns, key.size),
        )
    else:
        cursor = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=? AND size=? ORDER BY mtime_ns DESC LIMIT 1",
            (str(key.path), key.size),
        )
    row = cursor.fetchone()
    if row is None:
        return None
    blob: bytes = row[0]
    method_bytes, digest = blob.split(b":", 1)
    return ContentHash(method=method_bytes.decode("ascii"), digest=digest)
```

**`PostgresHashCacher`:**

```python
def get(self, key: FileHashKey) -> ContentHash | None:
    conn = self._connection()
    if self._match_mtime:
        row = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=%s AND mtime_ns=%s AND size=%s",
            (str(key.path), key.mtime_ns, key.size),
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=%s AND size=%s ORDER BY mtime_ns DESC LIMIT 1",
            (str(key.path), key.size),
        ).fetchone()
    if row is None:
        return None
    blob: bytes = bytes(row[0])
    method_bytes, digest = blob.split(b":", 1)
    return ContentHash(method=method_bytes.decode("ascii"), digest=digest)
```

### 3. `put()` — unchanged

All three cachers continue to store `(path, mtime_ns, size, hash)` on every write.
No schema changes required.

### 4. `__repr__` updates

Each cacher's `__repr__` gains `match_mtime=...`:

```python
# InMemoryHashCacher
f"InMemoryHashCacher(read_only={self._read_only!r}, " \
f"min_cache_size_bytes={self._min_cache_size_bytes!r}, " \
f"match_mtime={self._match_mtime!r})"

# SqliteHashCacher
f"SqliteHashCacher(db_path={str(self.db_path)!r}, " \
f"read_only={self._read_only!r}, " \
f"min_cache_size_bytes={self._min_cache_size_bytes!r}, " \
f"match_mtime={self._match_mtime!r})"

# PostgresHashCacher
f"PostgresHashCacher(conninfo={_redact_conninfo(self._conninfo)!r}, " \
f"read_only={self._read_only!r}, " \
f"min_cache_size_bytes={self._min_cache_size_bytes!r}, " \
f"match_mtime={self._match_mtime!r})"
```

### 5. `enable_file_hash_caching()` update

```python
def enable_file_hash_caching(
    *,
    db_path: "Path | None" = None,
    conninfo: str | None = None,
    read_only: bool = False,
    min_cache_size_bytes: int | None = None,
    match_mtime: bool = True,          # ← new
) -> None:
```

`match_mtime` is forwarded to `SqliteHashCacher(...)` or `PostgresHashCacher(...)`.
No other changes to the function body.

---

## Tests

New file: `tests/test_hashing/test_hash_cacher_match_mtime.py`

### Core scenarios (run against `InMemoryHashCacher` and `SqliteHashCacher`)

| # | Scenario | Expected |
|---|---|---|
| T1 | `match_mtime=True` (default), mtime changed, size unchanged | Cache **miss** → recompute |
| T2 | `match_mtime=False`, mtime changed, size unchanged | Cache **hit** → no recompute |
| T3 | `match_mtime=False`, content changed **and** size changed | Cache **miss** (size guard still works) |
| T4 | `match_mtime=False`, content changed but size preserved | Cache **hit** — known trade-off, tested and documented |
| T5 | Unchanged file: `match_mtime=True` vs `False` both return the same hash |

### Multi-entry tie-breaking (SQLite + in-memory)

- Populate two entries with the same `(path, size)` but different `mtime_ns` values.
- Verify `get()` with `match_mtime=False` returns the hash from the entry with the
  **higher** `mtime_ns`.

### `CachedFileHasher` integration (real files, in-memory cacher)

- T2 implemented via a real file: write content, populate cache, advance the file's
  mtime via `os.utime()`, then confirm `hash_file()` returns the cached hash without
  re-reading the file.
- T3 and T4 implemented via real files with `write_bytes()` and controlled sizes.

### `__repr__` tests

- `match_mtime=True` appears in `repr()`.
- `match_mtime=False` appears in `repr()`.

### `enable_file_hash_caching()` integration

- `match_mtime=False` → the `SqliteHashCacher` inside the registered
  `CachedFileHasher` has `_match_mtime=False`.

---

## Documentation

File: `docs/concepts/file-hash-caching.md`

Add a new section **"Ignoring mtime in cache lookups"** after the existing
"Controlling when the cache is written" section:

---

### Ignoring mtime in cache lookups

By default, a cache hit requires **path, mtime_ns, and size** to all match the stored
entry. In several common deployment scenarios, mtime is unreliable — it changes even
when file content has not — causing spurious cache misses and unnecessary re-hashing:

- **rsync / file transfer tools** — rsync and similar tools do not preserve mtime by
  default. Even with `--times`, sub-second precision is often lost on destination
  filesystems, producing a different `mtime_ns` for otherwise identical files.
- **Restore from backup** — backup and restore pipelines (tar, restic, Borg, cloud
  storage sync) frequently reset mtime to the restore timestamp rather than the
  original file timestamp.
- **Container bind mounts and volume remounts** — remounting a volume or restarting a
  container can reset or truncate mtime precision depending on the host filesystem and
  container runtime (Docker, Podman, Kubernetes).
- **CI `touch` / build system side-effects** — build scripts, test harnesses, and CI
  pipelines sometimes call `touch` on input files to force rebuilds, or copy files in
  ways that update mtime without changing content.
- **Network filesystems (NFS, CIFS/SMB)** — clock skew between the client and server,
  or coarse mtime granularity on older NFS versions, can produce stale or shifted
  timestamps that differ from the values recorded in the cache.

Set `match_mtime=False` to drop mtime from the lookup criterion. A cache hit then
requires only **path and size** to match:

```python
import orcapod as op

op.enable_file_hash_caching(match_mtime=False)
```

When multiple cache entries share the same path and size (recorded at different
mtimes), Orcapod returns the hash from the entry with the most recent `mtime_ns`.

**The write path is unchanged.** mtime is always recorded when a new entry is
inserted. Switching `match_mtime=False` on a cache that was already populated under
the default `match_mtime=True` still produces hits — no cache rebuild is needed.

#### Known trade-off

With `match_mtime=False`, a file modification that preserves the file's byte count
will **not** be detected by the cache. The stored hash from the previous version will
be returned silently. This is rare in practice (most writes change file size), but
you should be aware of the trade-off before enabling this mode.

Use `match_mtime=False` only in environments where mtime changes are known to be
unreliable. For most local-disk or NFS deployments the default (`match_mtime=True`)
is the right choice.

---

## Scope & Boundaries

In scope:
- `InMemoryHashCacher`, `SqliteHashCacher`, `PostgresHashCacher`: constructor +
  `get()` + `__repr__`
- `enable_file_hash_caching()` signature and body
- New test file `tests/test_hashing/test_hash_cacher_match_mtime.py`
- `docs/concepts/file-hash-caching.md` update

Out of scope:
- Per-file or per-namespace toggles — global on/off for v1
- `CachedFileHasher` changes — flag lives entirely in the cacher layer
- `FileHashKey` changes — write path and stored schema are unchanged
- Database schema changes — no migration needed
- Auto-detecting unreliable mtime environments

---

## Dependencies

- ITL-472 (FileHasher + HashCacher + `SqliteHashCacher`) — complete.
- ITL-519 (`read_only` + `min_cache_size_bytes`) — complete; `match_mtime` follows
  the same cacher-level pattern.
- ITL-520 (`PostgresHashCacher`) — complete; `match_mtime` applied identically.
- ITL-511 (activation docs) — complete; new knob added to same doc.
