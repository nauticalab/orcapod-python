# File Hash Caching

Orcapod can cache the SHA-256 digest of files so that re-hashing the same
unchanged file — across pipeline runs or between `op.File` and `op.Directory`
accesses — skips the disk read entirely. This page explains how the cache
works, how to turn it on, and when it helps.

## FileHasher, HashCacher, and CachedFileHasher

`FileHasher` is the base hasher. When you call `hash_file(path)` on it, it
reads the file's bytes from disk and returns a content hash (SHA-256 by
default, configurable via the `algorithm` argument). It does this every time,
with no memory of previous results.

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

## Controlling when the cache is written

By default, every file that passes through `CachedFileHasher` is inserted
into the cache on a miss. Two optional knobs let you restrict this.

### Read-only mode

Use `read_only=True` when you want lookups from a shared or authoritative
cache but must not add new entries to it — for example, when consuming a
cache pre-populated by `populate_hash_cache()` without polluting it with
ad-hoc entries.

```python
import orcapod as op

op.enable_file_hash_caching(db_path="/shared/cache.db", read_only=True)
```

Cache hits still work normally. On a miss, the file is hashed directly and
the result is returned to the caller — but it is never written to the cache.

### Minimum file size threshold

Use `min_cache_size_bytes` to skip the cache write overhead for small
files. For small files, the disk I/O bottleneck does not apply, so the
cache lookup and write add latency without meaningful savings.

```python
import orcapod as op

# Skip caching for files smaller than 1 MB
op.enable_file_hash_caching(min_cache_size_bytes=1_048_576)
```

Files smaller than the threshold are still hashed and the hash is returned
to the caller — they are simply not inserted into the cache. Files at or
above the threshold behave normally. Set to `None` (the default) or `0`
to disable the threshold.

### Combining both

The two knobs compose independently. `read_only=True` takes precedence:
when enabled, no entry is ever written regardless of file size.
`min_cache_size_bytes` is an additional guard that applies only when the
cacher is writable.

```python
import orcapod as op

# Read-only + skip files below 512 KB (threshold is moot when read-only,
# but harmless and documents intent)
op.enable_file_hash_caching(
    db_path="/shared/cache.db",
    read_only=True,
    min_cache_size_bytes=524_288,
)
```

## Ignoring mtime in cache lookups

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

### Known trade-off

With `match_mtime=False`, a file modification that preserves the file's byte count
will **not** be detected by the cache. The stored hash from the previous version will
be returned silently. This is rare in practice (most writes change file size), but
you should be aware of the trade-off before enabling this mode.

Use `match_mtime=False` only in environments where mtime changes are known to be
unreliable. For most local-disk or NFS deployments the default (`match_mtime=True`)
is the right choice.

## Directory hashing (op.Directory)

When Orcapod hashes an `op.Directory`, it traverses the directory tree and
builds a Merkle hash from all the files it finds. Each file's content hash
contributes to the directory's overall hash.

The `DirectoryHandler`'s `BasicDirectoryHasher` uses the **same**
`CachedFileHasher` instance as `FileHandler`. This means:

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
sqlite3 ~/.orcapod/file_hash_cache.db "SELECT path, size, cached_at FROM file_hash_cache LIMIT 10;"
```

To clear all cached entries:

```bash
sqlite3 ~/.orcapod/file_hash_cache.db "DELETE FROM file_hash_cache;"
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

- **Files are small.** Disk I/O is not the bottleneck for small files, so the
  cache lookup adds overhead without meaningful savings. Use
  ``min_cache_size_bytes`` to skip caching small files automatically.
- Files change on every run. A different `mtime_ns` or `size` means a
  different key, so every access is a cache miss.
- It is the first run on a new machine or a freshly cleared cache. Every file
  is a miss on a cold cache; you pay both the lookup cost and the file read.

## Choosing a backend: SQLite vs Postgres

Orcapod ships two hash cache backends. Pick based on your deployment:

| | SQLite | Postgres |
|---|---|---|
| Setup | Zero — file on disk | Requires a running Postgres server |
| Scope | Per-machine | Shared across machines and pipeline runs |
| Concurrency | Single-writer | Multi-writer safe (`ON CONFLICT DO NOTHING`) |
| Best for | Local development, single-machine pipelines | Distributed pipelines, shared caches |

### Using the SQLite backend (default)

```python
import orcapod as op

op.enable_file_hash_caching()  # uses ~/.orcapod/file_hash_cache.db
# or specify a path:
op.enable_file_hash_caching(db_path="/shared/nfs/cache.db")
```

### Using the Postgres backend

Requires `psycopg` (install with `pip install 'orcapod[postgresql]'`):

```python
import orcapod as op

op.enable_file_hash_caching(
    conninfo="postgresql://user:pass@db-host:5432/orcapod_cache"
)
```

You can also construct `PostgresHashCacher` directly and pass it to
`CachedFileHasher` for advanced configurations:

```python
from orcapod.hashing import CachedFileHasher, FileHasher, PostgresHashCacher

cacher = PostgresHashCacher(
    conninfo="postgresql://user:pass@db-host:5432/orcapod_cache",
    read_only=True,           # lookup only, no writes
    min_cache_size_bytes=1_048_576,  # skip files < 1 MB
)
hasher = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
```

### Schema

`PostgresHashCacher` creates the following table on first use
(`CREATE TABLE IF NOT EXISTS`):

```sql
CREATE TABLE IF NOT EXISTS file_hash_cache (
    path      TEXT   NOT NULL,
    mtime_ns  BIGINT NOT NULL,
    size      BIGINT NOT NULL,
    hash      BYTEA  NOT NULL,
    cached_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (path, mtime_ns, size)
)
```

Minimum supported Postgres version: **14**.
