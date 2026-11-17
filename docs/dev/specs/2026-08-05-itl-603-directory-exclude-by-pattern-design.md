# Design: `op.Directory` exclude-by-pattern — relative path matching and documentation

**Date:** 2026-08-05
**Issue:** ITL-603
**Status:** Approved

---

## Overview

`op.Directory` already has an `ignore=` parameter that accepts glob patterns (matched via
`fnmatch` against entry **names**) or a callable (receiving the absolute `UPath`). This design
upgrades both mechanisms to use **relative paths from the directory root** instead, making
patterns more expressive (e.g. `"sub/*.pyc"` targets a specific subdirectory) and adding the
explicit test coverage the issue mandates for the hash invariants.

No Arrow schema changes are made — the existing JSON-in-`large_string` storage format is kept.
No API naming changes are made — `ignore=` stays as-is.

---

## Design

### 1. Relative-path semantics for glob patterns

**File:** `src/orcapod/hashing/directory_hashers.py`

**Current behaviour:** `fnmatch.fnmatch(entry.name, pat)` — matches against the filename only,
at every recursion level independently. `"*.pyc"` excludes any `.pyc` file anywhere, but
`"sub/*.pyc"` never matches anything because entry names never contain `/`.

**New behaviour:** `PurePosixPath(child.relative_to(root)).match(pat)` — matches against the
POSIX relative path from the root. `pathlib.PurePath.match()` does right-anchored glob matching:

| Pattern | Matches |
|---|---|
| `"*.pyc"` | Any `.pyc` at any depth (last component match) |
| `"sub/*.pyc"` | `.pyc` files directly inside `sub/` only |
| `".git"` | Any entry named `.git` at any depth |
| `"__pycache__"` | Any entry named `__pycache__` at any depth |

This is consistent with how rsync, tar, and similar tools treat relative-path patterns.

#### Implementation

`_hash_dir` gains a `root: UPath` parameter (set to the top-level directory on first call,
threaded unchanged through all recursive calls):

```python
def _hash_dir(path, root, filter_fn, algorithm, file_hasher):
    for child in path.iterdir():
        relative = PurePosixPath(child.relative_to(root))
        if filter_fn is not None and filter_fn(relative):
            continue
        ...
        elif child.is_dir():
            subdir_digest = _hash_dir(child, root, filter_fn, algorithm, file_hasher)
```

`_compile_ignore` switches to `PurePosixPath.match()` for the glob case:

```python
def _compile_ignore(ignore):
    if ignore is None:
        return None
    if callable(ignore):
        return ignore  # caller now receives PurePosixPath — see §2
    patterns = list(ignore)
    def _glob_filter(relative: PurePosixPath) -> bool:
        return any(relative.match(pat) for pat in patterns)
    return _glob_filter
```

`BasicDirectoryHasher.hash_directory` passes `root=path`:

```python
digest = _hash_dir(path, path, filter_fn, self.algorithm, self.file_hasher)
```

### 2. Callable receives `PurePosixPath` (relative)

**Files:** `directory_hashers.py`, `directory_type.py`

The callable signature changes from `Callable[[UPath], bool]` (absolute path) to
`Callable[[PurePosixPath], bool]` (relative path from root, POSIX separators).

Pre-v0.1.0 — no shim needed; update all type hints and docstrings directly.

**Compatibility note:** Most real-world callables that access only `.name` (e.g.
`lambda p: p.name.startswith(".")`) continue to work unchanged because
`PurePosixPath.name` behaves identically to `UPath.name`.

### 3. Documentation

**`Directory.__init__` docstring** — expanded `ignore=` section:
- States that glob patterns use `pathlib.PurePath.match()` against the **POSIX relative path
  from the directory root** (e.g. `"sub/*.pyc"` matches only inside `sub/`; `"*.pyc"` matches
  at any depth because matching is right-anchored)
- States that callables receive a `pathlib.PurePosixPath` relative to the root
- Hash invariant summary: excluded entries are invisible to the hash — the result is identical
  to those entries never existing
- Three usage examples: no filter, glob list, callable

**`BasicDirectoryHasher.hash_directory` docstring** — mirrored clarifications on pattern
semantics and callable signature.

**Module docstring of `directory_type.py`** — brief note linking `ignore=` to the hash
invariant guarantee.

### 4. Test coverage (hash invariants)

**File:** `tests/test_hashing/test_directory_handler.py` — added to `TestBasicDirectoryHasher`

| Test | Invariant covered |
|---|---|
| `test_ignore_none_is_equivalent_to_no_ignore` | `hash(dir)` == `hash(dir, ignore=None)` |
| `test_non_matching_filter_is_equivalent_to_no_filter` | Non-matching pattern → same hash |
| `test_filter_identity_irrelevance` | Two patterns selecting same effective files → same hash |
| `test_relative_path_pattern_scopes_to_subdirectory` | `"sub/*.pyc"` excludes only in `sub/`, not in `other/` |

---

## Hash invariants (explicit guarantees)

These are properties of the implementation, tested explicitly:

1. **Empty filter**: `hash(Directory(path))` == `hash(Directory(path, ignore=None))` — the new
   argument in its absent/empty form must not shift any existing hash.
2. **Non-matching filter**: `hash(Directory(path))` == `hash(Directory(path, ignore=["*.nonexistent"]))` — a filter with no effect produces the same hash as no filter.
3. **Filter-identity irrelevance**: Two patterns `A` and `B` that select the same effective
   included files produce the same hash. The pattern string itself is not input to the hash.
4. **Filter-equivalence to physically-missing files** *(already tested)*: `hash(Directory(path,
   ignore=["*.pyc"]))` == `hash(directory_without_pyc_files)`.

---

## Scope

In scope:
- Relative-path semantics for glob patterns (code change)
- Callable signature change from `UPath` (absolute) to `PurePosixPath` (relative)
- Docstring improvements in `directory_type.py` and `directory_hashers.py`
- Four new tests for the hash invariants above

Out of scope:
- Arrow schema changes (JSON-in-`large_string` kept as-is)
- API naming changes (`ignore=` kept as-is)
- Regex pattern support
- Include-only / allowlist patterns
- Migration tooling

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/hashing/directory_hashers.py` | Relative-path matching; `root` parameter; type hints |
| `src/orcapod/extension_types/directory_type.py` | Type hint + docstring update |
| `tests/test_hashing/test_directory_handler.py` | 4 new tests |
