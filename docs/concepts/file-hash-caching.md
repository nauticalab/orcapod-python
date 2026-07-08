# File Hash Caching

Orcapod can cache the SHA-256 digest of files so that re-hashing the same
unchanged file — across pipeline runs or between `op.File` and `op.Directory`
accesses — skips the disk read entirely. This page explains how the cache
works, how to turn it on, and when it helps.

## FileHasher, HashCacher, and CachedFileHasher

`FileHasher` is the base hasher. When you call `hash_file(path)` on it, it
reads the file's bytes from disk and returns a SHA-256 digest. It does this
every time, with no memory of previous results.

`HashCacher` is a protocol for any object that maps a `FileHashKey` to a
`ContentHash`. The key is a three-field struct:

```
FileHashKey(path, mtime_ns, size)
```

The `mtime_ns` and `size` fields encode the file's identity at a specific
point in time. If the file is modified — its modification time or byte count
changes — the key is different, so the old entry is never returned. You do
not need to explicitly invalidate entries; the key change handles it
automatically.

`CachedFileHasher` wraps a `FileHasher` with any `HashCacher`. When you call
`hash_file(path)` on it, it first constructs the key from the file's current
metadata and queries the cacher. On a cache hit, it returns the stored digest
immediately. On a miss, it delegates to the inner `FileHasher`, reads the
file, and stores the result in the cacher before returning.

## Activating file hash caching

Call `enable_file_hash_caching()` once at application startup, before any
hashing takes place:

```python
import orcapod as op
op.enable_file_hash_caching()
```

You can also supply an explicit path for the SQLite database:

```python
op.enable_file_hash_caching(db_path="/path/to/cache.db")
```

Calling the function more than once is safe. Orcapod logs a warning and
replaces the active cacher with the new one, so the prior cache entries
remain accessible at the same path if you use the same `db_path`.

## Directory hashing (op.Directory)

When Orcapod hashes an `op.Directory`, it traverses the directory tree and
builds a Merkle hash from all the files it finds. Each file's digest
contributes to the directory's overall hash. Before ITL-511, this traversal
called the file hasher directly, bypassing the per-file cache even when
caching was enabled.

After ITL-511, the `DirectoryHandler`'s `BasicDirectoryHasher` uses the
**same** `CachedFileHasher` instance as `FileHandler`. This means:

- If a file was previously hashed as `op.File`, it is a cache hit the next
  time it appears during directory traversal.
- Conversely, if a file was first encountered during directory traversal, it
  is a cache hit the next time it is hashed as `op.File`.

No extra configuration is required. Calling `enable_file_hash_caching()` once
activates the shared cacher for both file and directory hashing automatically.

## Cache storage

By default, Orcapod stores the cache in a SQLite database at:

```
~/.orcapod/file_hash_cache.db
```

You can override this path in two ways:

- Set the `ORCAPOD_HASH_CACHE_DB` environment variable to the desired path.
- Pass the `db_path` argument to `enable_file_hash_caching()`.

To inspect cached entries using the SQLite command-line tool:

```bash
sqlite3 ~/.orcapod/file_hash_cache.db "SELECT path, method FROM file_hashes LIMIT 10;"
```

To clear all cached entries:

```bash
sqlite3 ~/.orcapod/file_hash_cache.db "DELETE FROM file_hashes;"
```

## When caching helps -- and when it doesn't

**Caching helps when:**

- You work with large files (GB-scale recordings, model checkpoints) that are
  rehashed across multiple pipeline runs. Skipping the read saves significant
  time.
- You hash directories where most files are unchanged between runs. The
  majority of per-file lookups become cache hits, and only modified or new
  files require a full read.
- The same files are accessed both as `op.File` and inside an `op.Directory`
  in the same workflow. The shared `CachedFileHasher` turns the second access
  into a cache hit regardless of which path was first.

**Caching does not help much when:**

- Files are small. Disk I/O is not the bottleneck for small files, so the
  cache lookup adds overhead without meaningful savings.
- Files change on every run. A different `mtime_ns` or `size` means a
  different key, so every access is a cache miss.
- It is the first run on a new machine or a freshly cleared cache. Every file
  is a miss on a cold cache; you pay both the lookup cost and the file read.
