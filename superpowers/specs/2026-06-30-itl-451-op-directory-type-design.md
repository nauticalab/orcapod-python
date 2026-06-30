# Design: `op.Directory` type with content-identified directory hashing

**Date:** 2026-06-30
**Issue:** ITL-451
**Status:** Approved

---

## Overview

Introduce `orcapod.Directory` as a first-class content-identified type for directory trees,
analogous to `orcapod.File` for individual files. `Directory` wraps a `UPath` and validates
that the path refers to a readable, traversable directory at construction time. Its hash is a
recursive Merkle tree hash of the entire directory tree.

This PR also migrates `LogicalFile` from a plain path string to a JSON storage format, for
consistency with `LogicalDirectory`.

---

## Design

### `orcapod.Directory` class

**File:** `src/orcapod/extension_types/directory_type.py` (new)

`Directory` subclasses `upath.extensions.ProxyUPath` — the same officially-supported UPath
extension mechanism used by `File`. `ProxyUPath` wraps a `UPath` instance (local, S3, GCS, …)
in `self.__wrapped__` and delegates all path/filesystem operations to it.

**Constructor:**

```python
class Directory(ProxyUPath):
    def __init__(self, *args, ignore=None, **kwargs):
        super().__init__(*args, **kwargs)         # builds self.__wrapped__ (a UPath)
        if not self.__wrapped__.exists():
            raise FileNotFoundError(...)
        if not self.__wrapped__.is_dir():
            raise NotADirectoryError(...)
        # Probe traversability — raises PermissionError if not accessible
        try:
            next(iter(self.__wrapped__.iterdir()), None)
        except PermissionError:
            raise PermissionError(...)
        self._ignore = ignore   # list[str] | Callable[[UPath], bool] | None
```

- **`FileNotFoundError`** — path does not exist.
- **`NotADirectoryError`** — path exists but is not a directory.
- **`PermissionError`** — directory exists but cannot be traversed.
- **Empty directory is valid** — produces a well-defined hash.
- **`ignore`** — see Ignore parameter section below.

No `follow_symlinks` constructor parameter. Symlinks *at the root* are accepted (the symlink
becomes the root path). Internal symlinks are recorded as symlink nodes during hashing (not
dereferenced) — see Symlink policy below.

**`_from_upath` override:**

```python
@classmethod
def _from_upath(cls, upath, /):
    obj = object.__new__(cls)
    object.__setattr__(obj, "__wrapped__", upath)
    object.__setattr__(obj, "_ignore", None)
    return obj
```

Used by `ProxyUPath` for derived paths (`.parent`, `/` operator). Skips validation —
derived paths from navigation may not exist yet. `_ignore` always defaults to `None` on
derived instances.

---

### Hashing scheme — Recursive Merkle tree

**Decision: Recursive Merkle.** Each node's hash is computed bottom-up: file leaves hash
their content, subdirectories hash their sorted children, and the root directory hash
propagates the entire tree.

**Justification:**
- Aligns with content-addressable storage precedents (git trees, IPFS UnixFS, nix store).
- Sub-tree identity: two parent directories sharing a common subdirectory produce the same
  intermediate hash for that subtree, enabling future sub-tree caching without API changes.
- Modest additional complexity over manifest hash, easily offset by future-proofing value.

#### Canonical serialization

Each entry in a directory is reduced to a 32-byte SHA-256 digest via a domain-separated
encoding before sorting and combining.

**Per-entry digest:**
```
file entry:    sha256(b"file\x00"    + name_utf8 + b"\x00" + file_content_sha256_digest)
dir entry:     sha256(b"dir\x00"     + name_utf8 + b"\x00" + child_dir_hash_digest)
symlink entry: sha256(b"symlink\x00" + name_utf8 + b"\x00" + readlink_target_utf8)
```

The `b"\x00"` separators are safe in this context because POSIX filenames cannot contain
null bytes.

**Directory-level hash** (after entries are sorted by `name.encode("utf-8")`, byte-wise):
```
sha256(b"directory\x00" + entry_digest_0 + entry_digest_1 + ...)
```

**Empty directory:** `sha256(b"directory\x00")` — well-defined, consistent.

The domain separator `b"directory\x00"` prevents a directory with a single entry from
colliding with any other hash value.

`ContentHash.method` is set to `"merkle_sha256"`.

#### Symlink policy — hash as symlink (do not follow)

Internal symlinks are recorded as symlink entries using `os.readlink()` for the target
string. They are never dereferenced.

- **Rationale:** Avoids traversal cycles and off-tree content inclusion. Two trees with
  identical files but different symlink targets hash differently — usually the correct
  semantic. Callers who want follow-semantics can resolve symlinks before constructing
  `Directory`.

#### Sort order

Entries are sorted by `name.encode("utf-8")` — byte-wise, locale-independent. This is
applied at every level of recursion.

#### Permissions, mtime, ownership

Excluded from the hash. Content addressability is about *content*, not filesystem metadata.

#### Special files (sockets, device nodes, named pipes)

Skipped silently with a `DEBUG`-level log message.

---

### Ignore parameter

```python
ignore: list[str] | Callable[[UPath], bool] | None = None
```

- **`None`** (default): all entries included, including hidden files and dotfiles.
- **`list[str]`**: glob patterns matched against the entry's *name* (not full path) using
  `fnmatch`. E.g. `["*.pyc", ".git"]`.
- **`Callable`**: called with each entry's absolute `UPath`; return `True` to exclude.

The filter is applied at every level of recursion (not just the top level). `_ignore` is
stored on the `Directory` instance and read by `DirectoryHandler` at hash time.

---

### `LogicalDirectory` Arrow extension type

**File:** `src/orcapod/extension_types/directory_type.py` (same file as `Directory`)

| Property | Value |
|---|---|
| `logical_type_name` | `"orcapod.directory"` |
| Arrow extension name | `"orcapod.directory"` |
| Storage type | `pa.large_string()` |
| `python_to_storage` | JSON string — see below |
| `storage_to_python` | parse JSON, reconstruct `Directory`, re-validates existence |

#### Storage format — always JSON

The serialized value is always a JSON object string:

| `ignore` value | Stored JSON |
|---|---|
| `None` | `{"path": "/abs/path"}` |
| `list[str]` | `{"path": "/abs/path", "ignore": ["*.pyc", ".git"]}` (patterns stored sorted) |
| Named callable (recoverable) | `{"path": "/abs/path", "ignore_callable": "my_mod:MyClass.fn"}` |
| Lambda / closure / built-in | `{"path": "/abs/path"}` + `warnings.warn(...)` at write time |

Named callables are serialized as `"module:qualname"` (colon separator). Callables whose
`__qualname__` contains `<` (lambdas, closures, built-ins) cannot be serialized; only the
path is stored and a `UserWarning` is emitted.

On `storage_to_python`:
- `ignore_callable` key present: call `_try_import_callable(full_name)`. If import succeeds,
  reconstruct `Directory(path, ignore=recovered_fn)`. If import fails (function moved,
  module unavailable), reconstruct `Directory(path, ignore=None)` and emit `UserWarning`.
- `ignore` key present: reconstruct `Directory(path, ignore=patterns)`.
- Neither key: reconstruct `Directory(path)`.

Re-validation on `storage_to_python` is intentional — raises `FileNotFoundError` /
`NotADirectoryError` if the directory no longer exists.

---

### `LogicalFile` JSON migration (piggyback)

**Files:** `src/orcapod/extension_types/file_type.py`, `tests/test_extension_types/test_file_type.py`

`LogicalFile` is migrated from plain path string to JSON for consistency with
`LogicalDirectory`:

```python
# python_to_storage: str(value)  →  json.dumps({"path": str(value)})
# storage_to_python: File(storage_value)  →  File(json.loads(storage_value)["path"])
```

No other changes to `File` or `LogicalFile`. Tests updated to expect JSON strings from
`python_to_storage`.

---

### `BasicDirectoryHasher`

**File:** `src/orcapod/hashing/directory_hashers.py` (new, mirrors `file_hashers.py`)

```python
class BasicDirectoryHasher:
    def __init__(self, algorithm: str = "sha256", buffer_size: int = 65536):
        ...

    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable | list[str] | None = None,
    ) -> ContentHash:
        digest = self._hash_dir(UPath(directory_path), _compile_ignore(ignore))
        return ContentHash(method="merkle_sha256", digest=digest)
```

`_compile_ignore(ignore)` converts `list[str]` to an `fnmatch`-based callable, passes
`Callable` through unchanged, and returns `None` for no filtering.

`_hash_dir(path, filter_fn)` recurses:
1. List `path.iterdir()`, skipping entries where `filter_fn(entry)` returns `True`.
2. For each entry, compute the per-entry digest (file / dir / symlink).
3. Sort entries by `entry.name.encode("utf-8")`.
4. Return `sha256(b"directory\x00" + b"".join(sorted_entry_digests))`.

---

### `DirectoryHandler`

**File:** `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` (modified)

```python
class DirectoryHandler:
    """Hasher for ``orcapod.Directory`` objects — hashes directory content via Merkle tree."""

    def __init__(self, directory_hasher: "DirectoryHasherProtocol") -> None:
        self.directory_hasher = directory_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        from orcapod.extension_types.directory_type import Directory  # deferred import
        if not isinstance(obj, Directory):
            raise TypeError(f"DirectoryHandler: expected an orcapod.Directory, got {type(obj)!r}")
        wrapped = getattr(obj, "__wrapped__")
        ignore = getattr(obj, "_ignore", None)
        return self.directory_hasher.hash_directory(wrapped, ignore=ignore)
```

`register_builtin_python_type_handlers` gains a `directory_hasher` parameter (defaults to
`BasicDirectoryHasher(sha256)`) and registers `Directory → DirectoryHandler(directory_hasher)`.

---

### `DirectoryHasherProtocol`

**File:** `src/orcapod/protocols/hashing_protocols.py` (modified)

```python
class DirectoryHasherProtocol(Protocol):
    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable | list[str] | None = None,
    ) -> ContentHash: ...
```

---

## Integration

### `src/orcapod/contexts/data/v0.1.json`

1. Add top-level `directory_hasher`: `BasicDirectoryHasher(sha256)`.
2. Add `LogicalDirectory` to `logical_types` list.
3. Add `Directory → DirectoryHandler(directory_hasher)` to `handlers` list.
4. Add changelog entry for this change and the `LogicalFile` JSON migration.

### Exports

| Symbol | Location | Change |
|---|---|---|
| `orcapod.Directory` | `src/orcapod/__init__.py` | **New** |
| `LogicalDirectory` | `src/orcapod/extension_types/__init__.py` | **New** |
| `DirectoryHandler` | `src/orcapod/hashing/semantic_hashing/__init__.py` | **New** |
| `BasicDirectoryHasher` | `src/orcapod/hashing/__init__.py` | **New** |
| `DirectoryHasherProtocol` | `src/orcapod/hashing/__init__.py` | **New** |

---

## Test coverage

### `tests/test_extension_types/test_directory_type.py` (new)

- Constructor rejects nonexistent path → `FileNotFoundError`
- Constructor rejects non-directory path → `NotADirectoryError`
- Constructor accepts empty directory
- `LogicalDirectory` properties: `logical_type_name`, `python_type`, Arrow extension name, storage type
- Roundtrip `ignore=None` → `{"path": "..."}` preserved
- Roundtrip `ignore=["*.pyc"]` → patterns preserved
- Roundtrip named callable → recovered on read (or `ignore=None` + warning if import fails)
- Roundtrip lambda → `{"path": "..."}` only, `UserWarning` emitted

### `tests/test_hashing/test_directory_handler.py` (new)

- Empty directory → well-defined, consistent `ContentHash`
- Identical content in two different parent locations → same hash
- Single-byte change in a deeply nested file → different hash
- Adding a file → different hash
- Removing a file → different hash
- Hidden files included by default; hash changes when dotfile added
- `ignore=["*.pyc"]` → `.pyc` files excluded; hash matches a tree without them
- Symlink recorded as symlink (not followed); cycle-safe (symlink pointing to parent)
- Large-tree smoke test (sanity-check, no assertion on value — just must not hang or error)
- `DirectoryHandler` rejects non-`Directory` objects → `TypeError`

### `tests/test_extension_types/test_file_type.py` (updated)

- `python_to_storage` now returns `{"path": "..."}` JSON string
- `storage_to_python` accepts JSON string

---

## Files changed

| File | Status |
|---|---|
| `src/orcapod/extension_types/directory_type.py` | **New** |
| `src/orcapod/hashing/directory_hashers.py` | **New** |
| `tests/test_extension_types/test_directory_type.py` | **New** |
| `tests/test_hashing/test_directory_handler.py` | **New** |
| `src/orcapod/extension_types/file_type.py` | Modified (JSON migration) |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Modified (add `DirectoryHandler`) |
| `src/orcapod/protocols/hashing_protocols.py` | Modified (add `DirectoryHasherProtocol`) |
| `src/orcapod/extension_types/__init__.py` | Modified (export `LogicalDirectory`) |
| `src/orcapod/__init__.py` | Modified (export `Directory`) |
| `src/orcapod/hashing/__init__.py` | Modified (export new symbols) |
| `src/orcapod/hashing/semantic_hashing/__init__.py` | Modified (export `DirectoryHandler`) |
| `src/orcapod/contexts/data/v0.1.json` | Modified (register new types and handlers) |

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| `LogicalFile` JSON migration breaks stored data | Greenfield pre-v0.1; no migration tooling required per project policy |
| Callable `ignore` silently lost on Arrow roundtrip (lambda/closure) | `UserWarning` emitted at write time; documented |
| Callable `ignore` recovery fails after function is moved/renamed | Falls back to `ignore=None` with `UserWarning`; documented as best-effort |
| Merkle traversal cycle via symlinks | Symlinks never dereferenced — cycle is impossible by design |
| Merkle traversal slow on large trees | Document the cost; no SLA for v0.1; large-tree smoke test verifies no hang |
| Determinism on remote fsspec backends | `iterdir()` order is not guaranteed; explicit sort by name before hashing |

---

## Out of scope

- Incremental / cached sub-tree hashing (Merkle structure supports it; caching is a follow-up)
- fsspec backend coverage beyond local (best-effort for v0.1)
- Mount-point / cross-filesystem traversal semantics
- Watching a directory for changes (`Directory` is a point-in-time snapshot)
- Glob pattern syntax sugar helper for `ignore` (callable interface covers it)
