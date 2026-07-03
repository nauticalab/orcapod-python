# Design: `os.PathLike` conformance for `op.File` and `op.Directory`

**Date:** 2026-07-03
**Issue:** ITL-493
**Status:** Approved

---

## Overview

`op.File` and `op.Directory` subclass `upath.extensions.ProxyUPath`, which does not define
`__fspath__` and is not `os.PathLike`. This means `isinstance(f, os.PathLike)` returns `False`
and stdlib APIs (`open()`, `pathlib.Path()`, `shutil`) reject `File`/`Directory` instances
directly.

The fix is minimal: add a `__fspath__` method to `File` and `Directory` that delegates to
`os.fspath(self.__wrapped__)`. Because `os.PathLike` is an ABC whose `__subclasshook__` checks
`hasattr(subclass, '__fspath__')`, defining the method is sufficient — no explicit base-class
change is required.

---

## Audit: `ProxyUPath` subclass hierarchy

There are exactly **two** `ProxyUPath` subclasses in the codebase:

| Class | File | Semantically a path? |
|---|---|---|
| `File` | `src/orcapod/extension_types/file_type.py` | Yes |
| `Directory` | `src/orcapod/extension_types/directory_type.py` | Yes |

Both are semantically paths. No other `ProxyUPath` subclasses exist, so the "leaf-level opt-in"
design is straightforward: only `File` and `Directory` get `__fspath__`. `UPathProxy` itself
remains untouched.

---

## Design

### `__fspath__` implementation

Both `File` and `Directory` receive the same method:

```python
def __fspath__(self) -> str:
    return os.fspath(self.__wrapped__)
```

Pure delegation — no branching, no scheme inspection. The wrapped `UPath` determines the outcome:

| Backing | `os.fspath(f)` result |
|---|---|
| `PosixUPath` (local) | local path string, e.g. `"/tmp/data.csv"` |
| `FilePath` (`file://…`) | local path string, e.g. `"/tmp/data.csv"` |
| `S3Path`, `EngmPath`, … (remote) | raises `TypeError` |

This mirrors `UPath` exactly. `UPath("s3://bucket/key")` returns an `S3Path` that is not
`os.PathLike` and raises on `os.fspath()` — the same runtime failure mode applies to a
remote-backed `File` or `Directory`.

### `isinstance` behaviour

Because `File` and `Directory` define `__fspath__` at the class level, `os.PathLike`'s
`__subclasshook__` makes `isinstance(f, os.PathLike)` return `True` for **all** instances —
local or remote. This is correct: `File` *is* path-like; whether its backing supports local
filesystem operations is a separate runtime question, exactly as it is with `UPath`.

### No new base classes

`File` and `Directory` do **not** add `os.PathLike` to their explicit inheritance list.
`ProxyUPath` is also not changed. The ABC mechanism handles everything via `__fspath__`.

### Docstring contract

Both `__fspath__` methods carry a docstring:

> Returns ``os.fspath()`` of the underlying ``UPath``. Succeeds for local-backed paths
> (``PosixUPath``, ``FilePath``); raises ``TypeError`` for remote-backed paths (S3, GCS,
> engm, …), consistent with how ``UPath`` itself behaves for those backends.

---

## Changes

| File | Change |
|---|---|
| `src/orcapod/extension_types/file_type.py` | Add `__fspath__` method to `File`; add `import os` |
| `src/orcapod/extension_types/directory_type.py` | Add `__fspath__` method to `Directory`; add `import os` |
| `tests/test_extension_types/test_file_type.py` | Add `TestFilePathLike` test class |
| `tests/test_extension_types/test_directory_type.py` | Add `TestDirectoryPathLike` test class |

---

## Test plan

### `TestFilePathLike` (new, in `test_file_type.py`)

- `isinstance(File(local_path), os.PathLike)` → `True`
- `os.fspath(File(local_path))` → equals `str(local_path)`
- `open(File(local_path))` → succeeds and reads the file
- `pathlib.Path(File(local_path))` → succeeds
- Remote-backed negative: construct via `File._from_upath(UPath("s3://bucket/key.csv"))` to
  bypass construction-time existence validation, then assert `os.fspath()` raises `TypeError`
- Negative control: a plain `ProxyUPath` subclass stub (no `__fspath__`) is not `os.PathLike`

Note: `File("s3://…")` cannot be used directly in tests because `__init__` calls
`self.__wrapped__.exists()`, which requires live credentials. Use `_from_upath` to inject a
remote-backed `__wrapped__` without triggering validation.

### `TestDirectoryPathLike` (new, in `test_directory_type.py`)

- `isinstance(Directory(local_dir), os.PathLike)` → `True`
- `os.fspath(Directory(local_dir))` → equals `str(local_dir)`
- `pathlib.Path(Directory(local_dir))` → succeeds
- Remote-backed negative: construct via `Directory._from_upath(UPath("s3://bucket/prefix/"))`,
  assert `os.fspath()` raises `TypeError`

---

## Out of scope

- Making `UPathProxy` itself `os.PathLike`
- Any other `ProxyUPath` subclasses (none exist)
- Changes to how `File`/`Directory` interact with Arrow extension types or hashing
- engmfs integration
