# File Hash Caching Design (ITL-472)

**Date:** 2026-07-02
**Issue:** [ITL-472](https://linear.app/enigma-metamorphic/issue/ITL-472)
**Status:** Approved

---

## Overview

Add optional, persistent file-hash caching to Orcapod's file hashing pipeline.
When caching is enabled, hashing a file becomes a sub-millisecond DB lookup on
repeat invocations (same path, same `mtime_ns`, same size) rather than a full
I/O-bound content hash.

The design introduces a generic `CacherProtocol[K, V]`, a file-hash-specific key
type `FileHashKey`, a `CachedFileHasher` decorator, a `SqliteHashCacher` v1
implementation backed by stdlib `sqlite3`, and a one-call opt-in API
(`enable_file_hash_caching()`) that wires caching into the default Orcapod
context.

---

## Motivation

Content hashing files is I/O-bound and expensive for large files. Orcapod
pipelines re-hash the same inputs repeatedly across multiple invocations, shared
datasets, and development iterations. A cache keyed on
`(absolute_path, mtime_ns, size)` collapses all repeat hashing to a single
lookup after the first computation.

---

## Design

### 1. `CacherProtocol[K, V]`

A generic get/put protocol added to `src/orcapod/protocols/hashing_protocols.py`.

```python
K = TypeVar("K")
V = TypeVar("V")

class CacherProtocol(Protocol[K, V]):
    def get(self, key: K) -> V | None: ...
    def put(self, key: K, value: V) -> None: ...
```

The protocol is intentionally general — it does not encode any file-specific
assumptions. `StringCacherProtocol` (used by Arrow column caching) is left
unchanged.

### 2. `FileHashKey`

A frozen dataclass representing the cache lookup key for a file.
Lives in `src/orcapod/hashing/file_hashers.py`.

```python
@dataclass(frozen=True)
class FileHashKey:
    path: Path       # absolute, resolved via Path.resolve()
    mtime_ns: int    # nanosecond mtime from os.stat().st_mtime_ns
    size: int        # file size in bytes from os.stat().st_size
```

**Key rationale:**
- `path` is always resolved to an absolute path so that `./data/x.bin` and
  `/full/path/data/x.bin` share a cache entry.
- `mtime_ns` uses nanosecond precision to reduce same-second collision risk.
- `size` is a cheap extra guard that catches most cases where mtime collides.

**Known limitations (documented, not fixed in v1):**
- Content changed but mtime preserved (`touch -r`, some editors): cache returns
  a stale hash. Escape hatch: don't call `enable_file_hash_caching()`.
- Some filesystems have second-level mtime resolution; size guard reduces risk.

### 3. `FileHasher` (rename of `BasicFileHasher`)

`BasicFileHasher` is renamed to `FileHasher`. No behavioural change.

```python
class FileHasher:
    def __init__(self, algorithm: str = "sha256", buffer_size: int = 65536): ...
    def hash_file(self, file_path: PathLike) -> ContentHash: ...
```

All existing references to `BasicFileHasher` are updated. No compatibility shim.

### 4. `CachedFileHasher`

A decorator that wraps any `FileContentHasherProtocol` with a
`CacherProtocol[FileHashKey, ContentHash]`. Lives in
`src/orcapod/hashing/file_hashers.py`.

```python
class CachedFileHasher:
    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        cacher: CacherProtocol[FileHashKey, ContentHash],
    ) -> None: ...

    def hash_file(self, file_path: PathLike) -> ContentHash:
        path = Path(file_path).resolve()
        stat = os.stat(file_path)
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        hit = self.cacher.get(key)
        if hit is not None:
            return hit
        result = self.file_hasher.hash_file(file_path)
        self.cacher.put(key, result)
        return result
```

Both `FileHasher` and `CachedFileHasher` implement `FileContentHasherProtocol`.
Callers never need to know which they have.

**Multi-layer caching:** Intentional layered caching (e.g., in-memory L1 +
SQLite L2) is supported by composing `CachedFileHasher` instances manually.
`enable_file_hash_caching()` does NOT support multiple layers — it always
produces exactly one layer around the base hasher (see §7).

### 5. `InMemoryHashCacher`

A simple dict-backed cacher for testing and ephemeral in-process use.
Lives in `src/orcapod/hashing/hash_cachers.py`.

```python
class InMemoryHashCacher:
    def __init__(self) -> None:
        self._cache: dict[FileHashKey, ContentHash] = {}

    def get(self, key: FileHashKey) -> ContentHash | None:
        return self._cache.get(key)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        self._cache[key] = value

    def clear(self) -> None:
        self._cache.clear()
```

### 6. `SqliteHashCacher`

A production-grade cacher backed by stdlib `sqlite3`.
Lives in `src/orcapod/hashing/hash_cachers.py`.

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS file_hash_cache (
    path      TEXT    NOT NULL,
    mtime_ns  INTEGER NOT NULL,
    size      INTEGER NOT NULL,
    hash      BLOB    NOT NULL,
    cached_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY (path, mtime_ns, size)
) WITHOUT ROWID;
```

- `WITHOUT ROWID` — the primary key IS the lookup key; no rowid overhead.
- WAL mode enabled on first connection (`PRAGMA journal_mode=WAL`) — handles
  single-writer/multi-reader concurrency.
- `hash` BLOB stores `ContentHash.to_prefixed_digest()` →
  `b"{method}:{raw_digest}"`. Parsed back by splitting on the first `b":"`.
- `INSERT OR REPLACE` for idempotent writes.
- `cached_at` is reserved for future TTL / eviction (not implemented in v1).

**Threading:** Thread-local `sqlite3.Connection` objects via `threading.local()`.
Each thread opens its own connection on first use; WAL mode ensures they coexist
safely.

**Default path:** `~/.orcapod/file_hash_cache.db`. Overridable via constructor
argument or the `ORCAPOD_HASH_CACHE_DB` environment variable.

```python
class SqliteHashCacher:
    DEFAULT_DB_PATH = Path.home() / ".orcapod" / "file_hash_cache.db"

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = Path(
            db_path
            or os.environ.get("ORCAPOD_HASH_CACHE_DB")
            or self.DEFAULT_DB_PATH
        )
        self._local = threading.local()
        self._ensure_schema()

    def get(self, key: FileHashKey) -> ContentHash | None: ...
    def put(self, key: FileHashKey, value: ContentHash) -> None: ...
    def clear(self) -> None: ...
    def close(self) -> None: ...  # closes the calling thread's connection
    def __enter__(self) -> "SqliteHashCacher": ...
    def __exit__(self, *_) -> None: ...
```

**Concurrency note:** Heavy multi-writer scenarios are a known SQLite limitation.
The Turso / libSQL migration (deferred, ITL follow-up) addresses this.

### 7. `enable_file_hash_caching()`

Added to `src/orcapod/contexts/__init__.py`. Wires caching into the default
Orcapod context by re-registering the `orcapod.File` handler.

```python
def enable_file_hash_caching(db_path: Path | None = None) -> None:
    """Enable SQLite-backed file hash caching on the default Orcapod context.

    Wraps the existing ``FileHandler``'s hasher in a ``CachedFileHasher``
    backed by ``SqliteHashCacher`` and re-registers it for ``orcapod.File``
    in the default context's semantic hasher registry.

    Call once at application startup before any file hashing occurs.

    If caching is already enabled (the handler is already a ``CachedFileHasher``),
    a warning is logged, existing caching layers are unwrapped, and the new
    cacher is applied around the original base hasher.

    Args:
        db_path: Path to the SQLite cache database. Defaults to
            ``~/.orcapod/file_hash_cache.db`` or the
            ``ORCAPOD_HASH_CACHE_DB`` environment variable.
    """
    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry

    existing_handler = registry.get_handler_for_type(File)
    base_hasher = existing_handler.file_hasher

    if isinstance(base_hasher, CachedFileHasher):
        logger.warning(
            "enable_file_hash_caching() called but the default FileHandler "
            "already has a CachedFileHasher. Unwrapping and replacing with "
            "the new cacher. If layered caching is intentional, construct a "
            "CachedFileHasher manually instead."
        )
        while isinstance(base_hasher, CachedFileHasher):
            base_hasher = base_hasher.file_hasher

    registry.register(File, FileHandler(
        CachedFileHasher(file_hasher=base_hasher, cacher=SqliteHashCacher(db_path))
    ))
```

---

## File Layout

```
src/orcapod/
├── protocols/
│   └── hashing_protocols.py       # + CacherProtocol[K, V]
├── hashing/
│   ├── file_hashers.py            # FileHashKey, FileHasher (renamed), CachedFileHasher
│   ├── hash_cachers.py            # NEW: InMemoryHashCacher, SqliteHashCacher
│   └── __init__.py                # + FileHasher, CacherProtocol, FileHashKey,
│                                  #   SqliteHashCacher, InMemoryHashCacher
│                                  # - BasicFileHasher
└── contexts/
    └── __init__.py                # + enable_file_hash_caching()

tests/
└── test_hashing/
    ├── test_file_hashers.py       # Update: BasicFileHasher → FileHasher,
    │                              #   CachedFileHasher uses InMemoryHashCacher
    └── test_hash_cachers.py       # NEW: InMemoryHashCacher + SqliteHashCacher tests

bench/
└── bench_file_hasher_cache.py     # NEW: timing on ≥100 MB file
```

---

## Public API Changes

### Additions to `orcapod.hashing`

| Symbol | Type |
|---|---|
| `CacherProtocol` | Generic protocol `[K, V]` |
| `FileHashKey` | Frozen dataclass |
| `FileHasher` | Class (renamed from `BasicFileHasher`) |
| `SqliteHashCacher` | Class |
| `InMemoryHashCacher` | Class |

### Removals from `orcapod.hashing`

| Symbol | Reason |
|---|---|
| `BasicFileHasher` | Renamed to `FileHasher` |

### Additions to `orcapod.contexts`

| Symbol | Type |
|---|---|
| `enable_file_hash_caching` | Function |

`StringCacherProtocol` and all existing string cachers are unchanged.

---

## Tests

`tests/test_hashing/test_hash_cachers.py` covers:

- **`InMemoryHashCacher`:** get returns `None` on miss; put then get returns hit;
  different keys are isolated; `clear()` resets state.
- **`SqliteHashCacher`:** same hit/miss/isolation tests; persistence across
  instances (second instance on same DB sees first's entries); WAL mode
  confirmed via `PRAGMA journal_mode`; `clear()` empties the table; env var
  path override; context manager closes cleanly.
- **`CachedFileHasher`:** hit path returns cached hash without calling inner
  hasher; miss path calls inner hasher and populates cache; `mtime_ns` change
  invalidates (different key); `size` change invalidates.
- **`enable_file_hash_caching()`:** re-registers handler; warning logged on
  double-call; unwraps correctly to base hasher on double-call.

---

## Bench Script

`bench/bench_file_hasher_cache.py`:
1. Creates a ≥100 MB temp file
2. Times uncached hash (baseline)
3. Times first cached hash (miss — overhead vs baseline)
4. Times second cached hash (hit — should be sub-millisecond)
5. Prints a summary table with timings and speedup factor

---

## Out of Scope

- Turso / libSQL backend (follow-up issue)
- Directory hashing cache (follow-up, ITL-451 analogue)
- Cache eviction / TTL (schema has `cached_at` reserved for this)
- Cross-machine cache sync
