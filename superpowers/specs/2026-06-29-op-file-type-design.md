# Design: `op.File` type and `pathlib.Path` / `upath.UPath` pure-path refactor

**Date:** 2026-06-29
**Issue:** ITL-450
**Status:** Approved

---

## Overview

Introduce `orcapod.File` as a first-class content-identified type and simultaneously
remove the content-hashing semantic handlers for `pathlib.Path` and `upath.UPath`.
After this change the type system has a clean semantic boundary:

| Category | Types | Hash = |
|---|---|---|
| Content-identified | `orcapod.File` | hash of file *content* |
| Pure path / string-identified | `pathlib.Path`, `upath.UPath` | hash of *path string* |

`op.File` is strictly for materialized files. Use `Path` / `UPath` for paths that may
not yet exist (e.g. a future pod output).

---

## Design

### `orcapod.File` class

**File:** `src/orcapod/extension_types/file_type.py`

`File` subclasses `upath.extensions.ProxyUPath` — the officially supported UPath
extension mechanism. `ProxyUPath` wraps any `UPath` instance (local, S3, GCS, …)
and delegates all path/filesystem operations to it, giving `File` full UPath-like
behaviour across all backends without touching UPath's global protocol registry.

Note: `isinstance(file_instance, UPath)` returns `False` because `ProxyUPath` does
not inherit from `UPath`. This is the accepted trade-off for using `ProxyUPath`.

**Constructor behaviour:**

```python
class File(ProxyUPath):
    def __init__(self, *args, follow_symlinks: bool = True, **kwargs):
        super().__init__(*args, **kwargs)   # builds self.__wrapped__ (a UPath)
        if not follow_symlinks and self.__wrapped__.is_symlink():
            raise ValueError(
                f"File: path is a symlink and follow_symlinks=False: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.exists():
            raise FileNotFoundError(...)
        if self.__wrapped__.is_dir():
            raise IsADirectoryError(...)
        if not self.__wrapped__.is_file():
            raise ValueError(...)
```

- Existence and file-ness are validated eagerly in `__init__`. Construction fails fast
  for nonexistent or non-file paths.
- `follow_symlinks=True` (default): symlinks are followed; hash = hash of the resolved
  target's content, consistent with `open()`-style semantics.
- `follow_symlinks=False`: raises `ValueError` if the path is a symlink. Treating a
  symlink as an invalid argument is safer than silently hashing the link itself or its
  target under a setting that explicitly opted out of following.
- `_follow_symlinks` does **not** need to be stored or forwarded to `FileHandler`. By
  the time a `File` instance reaches `FileHandler.handle()`, the constructor invariant
  guarantees that either the path is not a symlink (`follow_symlinks=False`) or symlinks
  are being followed (`follow_symlinks=True`). In both cases `path.open("rb")` produces
  the correct bytes without any extra flag.
- Zero-byte files are valid and produce a well-defined hash.

**`_from_upath` override:**

`ProxyUPath` uses `_from_upath` as a factory for derived paths (`.parent`, `/`
operator, `.resolve()`, etc.). The override skips validation — derived paths from
navigation are not re-validated because they may not exist yet:

```python
@classmethod
def _from_upath(cls, upath):
    obj = object.__new__(cls)
    obj.__wrapped__ = upath
    obj._follow_symlinks = True   # derived paths default to following symlinks
    return obj
```

### `LogicalFile` Arrow extension type

**File:** `src/orcapod/extension_types/file_type.py` (same file as `File`)

| Property | Value |
|---|---|
| `logical_type_name` | `"orcapod.file"` |
| Arrow extension name | `"orcapod.file"` |
| Storage type | `pa.large_string()` |
| `python_to_storage` | `str(value)` — the path string |
| `storage_to_python` | `File(storage_value)` — re-validates existence on read |

The extension name `"orcapod.file"` is distinct from `"orcapod.path"` and
`"orcapod.upath"`, preserving the on-disk semantic difference between a
content-identified file and a plain path string.

Re-validation on `storage_to_python` is intentional: reading an Arrow table with
`orcapod.file` columns from a context where the files no longer exist raises
`FileNotFoundError`. This is the correct semantic for a content-identified type.

### `FileHandler` (content hashing)

**File:** `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`

```python
class FileHandler:
    """Hasher for orcapod.File objects — hashes file *content*."""

    def __init__(self, file_hasher: FileContentHasherProtocol) -> None:
        self.file_hasher = file_hasher

    def handle(self, obj, hasher) -> ContentHash:
        from orcapod.extension_types.file_type import File   # deferred to avoid circular import
        if not isinstance(obj, File):
            raise TypeError(f"FileHandler: expected a File, got {type(obj)!r}")
        return self.file_hasher.hash_file(obj.__wrapped__)
```

The deferred import mirrors the pattern used by `ArrowTableHandler` to break
construction-time circular dependencies.

### `PathHandler` and `UPathHandler` — removed

Both classes are **deleted** from `builtin_handlers.py` and their registrations
removed from `register_builtin_python_type_handlers`. After removal:

- `pathlib.Path` and `upath.UPath` have no semantic handler.
- The Arrow hashing visitor falls through to hashing the Arrow storage value directly
  (the path string stored in `large_string()` columns).
- This is identical to how `uuid.UUID` is handled today — the Arrow string/binary
  content is hashed, no Python-level roundtrip or file read occurs.

`LogicalPath` and `LogicalUPath` are **not** removed — they continue to control
Arrow serialisation of `Path` and `UPath` column values. Only the hashing handler
changes.

### `register_builtin_python_type_handlers` update

```python
# Removed:
registry.register(Path, PathHandler(file_hasher))
registry.register(UPath, UPathHandler(file_hasher))

# Added:
from orcapod.extension_types.file_type import File
registry.register(File, FileHandler(file_hasher))
```

### Configuration (`v0.1.json`)

**File:** `src/orcapod/contexts/data/v0.1.json`

1. Add `LogicalFile` to `logical_types`:
   ```json
   {"_class": "orcapod.extension_types.file_type.LogicalFile", "_config": {}}
   ```

2. Remove `pathlib.Path` and `upath.core.UPath` entries from `handlers`.

3. Add `File` handler entry:
   ```json
   [
     {"_type": "orcapod.extension_types.file_type.File"},
     {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FileHandler",
      "_config": {"file_hasher": {"_ref": "file_hasher"}}}
   ]
   ```

The top-level `file_hasher` definition (`BasicFileHasher(sha256)`) is unchanged.

---

## Exports

| Symbol | Location | Change |
|---|---|---|
| `orcapod.File` | `src/orcapod/__init__.py` | **New** |
| `orcapod.Path` | `src/orcapod/__init__.py` | Unchanged |
| `orcapod.UPath` | `src/orcapod/__init__.py` | Unchanged |
| `FileHandler` | `src/orcapod/hashing/semantic_hashing/__init__.py` | **New** |
| `PathHandler` | `src/orcapod/hashing/semantic_hashing/__init__.py` | **Removed** |
| `UPathHandler` | `src/orcapod/hashing/semantic_hashing/__init__.py` | **Removed** |
| `LogicalFile` | `src/orcapod/extension_types/__init__.py` | **New** |

---

## Internal migration

During implementation, grep `src/` and `tests/` for `pathlib.Path` usages that
relied on content-hashing semantics and migrate them to `orcapod.File`. Key files
identified during exploration:

- `tests/test_hashing/test_extension_type_hashing.py` — port Path content-hash
  tests to use `File`
- `tests/test_hashing/test_semantic_hasher.py` — remove/replace `PathHandler`
  dependent tests

---

## Test coverage

### `tests/test_extension_types/test_file_type.py` (new)

- Constructor rejects nonexistent path → `FileNotFoundError`
- Constructor rejects directory → `IsADirectoryError`
- Constructor rejects symlink-to-directory → `IsADirectoryError`
- Constructor with `follow_symlinks=False` on a symlink → `ValueError`
- Constructor with `follow_symlinks=True` (default) on symlink to real file → succeeds
- Constructor accepts zero-byte file → succeeds
- Roundtrip: `File(path) → python_to_storage → storage_to_python → File` preserves path
- Arrow extension name is `"orcapod.file"`, storage type is `pa.large_string()`
- `LogicalFile.python_type` is `File`

### `tests/test_hashing/test_file_handler.py` (new)

- `FileHandler` on a real file → returns `ContentHash`
- Two files with same content → same hash
- Two files with different content → different hash
- Zero-byte file → well-defined, consistent hash (not an error)
- Hash value matches what `PathHandler` used to produce for the same file
  (migration compatibility assertion, using the old `BasicFileHasher(sha256)` directly)

### Updates to `tests/test_hashing/test_extension_type_hashing.py`

- Port `Path`-column content-hash tests to use `File` columns
- Add: hashing a `Path` column pointing at a nonexistent/unreadable path raises no
  error (verifies no file read occurs)

---

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Breaking change: existing `Path`-typed pipeline columns change hash values | Document in CHANGELOG; green-field stance, no migration tooling required |
| `isinstance(file, UPath)` returns `False` | Documented as accepted trade-off of `ProxyUPath`; use `isinstance(file, File)` |
| Re-validation on Arrow read raises if files moved/deleted | Correct semantic for content-identified type; document in API docs |
| Circular import between `file_type.py` and `builtin_handlers.py` | Deferred import inside `FileHandler.handle()`, mirroring `ArrowTableHandler` pattern |
| `_from_upath` skip of validation for derived paths | Intentional and documented; only `__init__` validates |
| `follow_symlinks` flag not preserved through `_from_upath` | `_from_upath` explicitly sets `_follow_symlinks = True` on derived instances; documented that path-navigation results always follow symlinks |

---

## Out of scope

- `op.Directory` — separate issue
- Lazy / promised file handles — use `Path` / `UPath`
- Remote backend coverage beyond local — broader coverage is a follow-up
- Migration tooling for external users
