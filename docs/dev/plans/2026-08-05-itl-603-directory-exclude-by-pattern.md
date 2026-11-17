# Directory exclude-by-pattern: relative path matching (ITL-603) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change `op.Directory` glob filtering to match against POSIX relative paths (via `PurePosixPath.match()`) instead of bare entry names, update the callable signature accordingly, improve docstrings, and add explicit test coverage for the hash invariants.

**Architecture:** `_hash_dir` gains a `root` parameter so every recursive call can compute `child.relative_to(root)` as a `PurePosixPath`. `_compile_ignore` switches from `fnmatch.fnmatch(entry.name, pat)` to `PurePosixPath(relative).match(pat)`. The callable type hint in both `directory_hashers.py` and `directory_type.py` changes from `Callable[[UPath], bool]` to `Callable[[PurePosixPath], bool]`. All existing tests continue to pass because `PurePosixPath.name` behaves identically to `UPath.name` for callable filters that only inspect `.name`.

**Tech Stack:** Python stdlib (`pathlib.PurePosixPath`), `uv run pytest`

**Spec:** `superpowers/specs/2026-08-05-itl-603-directory-exclude-by-pattern-design.md`

**Branch:** `eywalker/itl-603-opdirectory-optional-exclude-by-pattern-argument-for-hashing`

---

## File map

| File | Change |
|---|---|
| `src/orcapod/hashing/directory_hashers.py` | Add `root` param to `_hash_dir`; switch to `PurePosixPath.match()`; update imports and type hints |
| `src/orcapod/extension_types/directory_type.py` | Update `ignore=` type hint and docstring |
| `tests/test_hashing/test_directory_handler.py` | Add 4 new tests in `TestBasicDirectoryHasher` |

---

## Task 0: Create and check out the feature branch

**Files:** none

- [ ] **Step 1: Create the branch**

```bash
cd /path/to/orcapod-python
git checkout -b eywalker/itl-603-opdirectory-optional-exclude-by-pattern-argument-for-hashing
git branch --show-current
```

Expected output: `eywalker/itl-603-opdirectory-optional-exclude-by-pattern-argument-for-hashing`

---

## Task 1: Write the failing test that drives the implementation

The test `test_relative_path_pattern_scopes_to_subdirectory` is the only one that fails with
current code — `fnmatch.fnmatch("app.pyc", "sub/*.pyc")` returns `False` because fnmatch
matches only against the filename, so `"sub/*.pyc"` never excludes anything today.

**Files:**
- Modify: `tests/test_hashing/test_directory_handler.py`

- [ ] **Step 1: Add the failing test inside `TestBasicDirectoryHasher`**

Open `tests/test_hashing/test_directory_handler.py`. Inside `class TestBasicDirectoryHasher`,
add after the last test in that class:

```python
def test_relative_path_pattern_scopes_to_subdirectory(self, tmp_path):
    """Pattern 'sub/*.pyc' must exclude .pyc files in sub/ but not in other/."""
    base = tmp_path / "base"
    base.mkdir()
    (base / "sub").mkdir()
    (base / "other").mkdir()
    (base / "sub" / "app.py").write_bytes(b"code")
    (base / "sub" / "app.pyc").write_bytes(b"compiled in sub")
    (base / "other" / "app.py").write_bytes(b"code")

    # reference: same tree but with sub/app.pyc physically absent
    ref = tmp_path / "ref"
    ref.mkdir()
    (ref / "sub").mkdir()
    (ref / "other").mkdir()
    (ref / "sub" / "app.py").write_bytes(b"code")
    (ref / "other" / "app.py").write_bytes(b"code")

    hasher = BasicDirectoryHasher(file_hasher=FileHasher())
    # "sub/*.pyc" should exclude sub/app.pyc → same hash as ref
    assert hasher.hash_directory(base, ignore=["sub/*.pyc"]) == hasher.hash_directory(ref)
    # "other/*.pyc" should not match sub/app.pyc → different hash from ref
    assert hasher.hash_directory(base, ignore=["other/*.pyc"]) != hasher.hash_directory(ref)
```

- [ ] **Step 2: Run it to verify it fails**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py::TestBasicDirectoryHasher::test_relative_path_pattern_scopes_to_subdirectory -v
```

Expected: **FAIL** — `AssertionError` on the first assert (both hashes are equal when they
shouldn't differ yet, because `"sub/*.pyc"` currently matches nothing and acts like no filter).

---

## Task 2: Implement relative-path matching in `directory_hashers.py`

**Files:**
- Modify: `src/orcapod/hashing/directory_hashers.py`

- [ ] **Step 1: Update imports**

At the top of `src/orcapod/hashing/directory_hashers.py`, replace:

```python
import fnmatch
import hashlib
import logging
import os
from collections.abc import Callable

from upath import UPath
```

with:

```python
import hashlib
import logging
import os
from collections.abc import Callable
from pathlib import PurePosixPath

from upath import UPath
```

(`fnmatch` is no longer used after this task.)

- [ ] **Step 2: Update `_compile_ignore`**

Replace the entire `_compile_ignore` function (lines ~19–41) with:

```python
def _compile_ignore(
    ignore: Callable[[PurePosixPath], bool] | list[str] | None,
) -> Callable[[PurePosixPath], bool] | None:
    """Convert an ignore spec to a single callable filter.

    Args:
        ignore: ``None`` (no filtering), a list of glob patterns matched against
            the POSIX relative path from the root via ``pathlib.PurePosixPath.match()``
            (right-anchored: ``"*.pyc"`` matches at any depth; ``"sub/*.pyc"`` matches
            only inside ``sub/``), or a callable ``(PurePosixPath) -> bool``.

    Returns:
        A callable ``(PurePosixPath) -> bool`` returning ``True`` to exclude an entry,
        or ``None`` if no filtering is needed.
    """
    if ignore is None:
        return None
    if callable(ignore):
        return ignore
    patterns = list(ignore)

    def _glob_filter(relative: PurePosixPath) -> bool:
        return any(relative.match(pat) for pat in patterns)

    return _glob_filter
```

- [ ] **Step 3: Update `_hash_dir` signature and body**

Replace the entire `_hash_dir` function with:

```python
def _hash_dir(
    path: UPath,
    root: UPath,
    filter_fn: Callable[[PurePosixPath], bool] | None,
    algorithm: str,
    file_hasher: FileContentHasherProtocol,
) -> bytes:
    """Recursively compute the Merkle hash of a directory.

    Args:
        path: The directory to hash (current recursion node).
        root: The top-level directory passed to ``BasicDirectoryHasher.hash_directory``.
            Used to compute POSIX relative paths for filter evaluation.
        filter_fn: Optional filter callable; receives the POSIX relative path from
            ``root`` and returns ``True`` to exclude the entry.
        algorithm: Hash algorithm name used for structural (entry and node) hashing.
        file_hasher: Hasher used to compute ``ContentHash`` for each file leaf.

    Returns:
        The raw digest bytes for this directory node.
    """
    entries: list[tuple[bytes, bytes]] = []

    for child in path.iterdir():
        relative = PurePosixPath(child.relative_to(root))
        if filter_fn is not None and filter_fn(relative):
            continue

        name_bytes = child.name.encode("utf-8")

        if child.is_symlink():
            target = os.readlink(child)
            entry_bytes = b"symlink\x00" + name_bytes + b"\x00" + target.encode("utf-8")
        elif child.is_file():
            file_hash = file_hasher.hash_file(child)
            entry_bytes = b"file\x00" + name_bytes + b"\x00" + file_hash.digest
        elif child.is_dir():
            subdir_digest = _hash_dir(child, root, filter_fn, algorithm, file_hasher)
            entry_bytes = b"dir\x00" + name_bytes + b"\x00" + subdir_digest
        else:
            # Special file (socket, device node, named pipe) — skip silently.
            logger.debug("BasicDirectoryHasher: skipping special file %s", child)
            continue

        entry_digest = hashlib.new(algorithm, entry_bytes).digest()
        entries.append((name_bytes, entry_digest))

    # Sort byte-wise by name — locale-independent, deterministic.
    entries.sort(key=lambda x: x[0])

    h = hashlib.new(algorithm)
    h.update(b"directory\x00")
    for _, entry_digest in entries:
        h.update(entry_digest)
    return h.digest()
```

- [ ] **Step 4: Update `BasicDirectoryHasher.hash_directory`**

In `class BasicDirectoryHasher`, update the `hash_directory` method signature and body.
Replace the method with:

```python
def hash_directory(
    self,
    directory_path: PathLike,
    ignore: Callable[[PurePosixPath], bool] | list[str] | None = None,
) -> ContentHash:
    """Compute the recursive Merkle hash of a directory tree.

    Args:
        directory_path: Path to the directory to hash.
        ignore: Optional filter. A list of glob patterns matched against the
            **POSIX relative path from the root** via ``pathlib.PurePosixPath.match()``
            (right-anchored: ``"*.pyc"`` matches any ``.pyc`` at any depth;
            ``"sub/*.pyc"`` matches only ``.pyc`` files directly inside ``sub/``),
            or a callable ``(pathlib.PurePosixPath) -> bool`` returning ``True``
            to exclude an entry. Applied at every level of recursion.

            Hash invariant: excluded entries are invisible to the hash. The result
            is identical to those entries never existing. The pattern string itself
            is not input to the hash.

    Returns:
        A ``ContentHash`` with ``method="merkle_{algorithm}"``.

    Raises:
        FileNotFoundError: If ``directory_path`` does not exist.
        NotADirectoryError: If ``directory_path`` is not a directory.
        PermissionError: If the directory is not traversable.

    Example:
        >>> from orcapod.hashing.file_hashers import FileHasher
        >>> hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        >>> result = hasher.hash_directory("/tmp/mydir")
        >>> result.method
        'merkle_sha256'
        >>> # Exclude all .pyc files at any depth
        >>> result = hasher.hash_directory("/tmp/mydir", ignore=["*.pyc"])
        >>> # Exclude .pyc only inside build/
        >>> result = hasher.hash_directory("/tmp/mydir", ignore=["build/*.pyc"])
        >>> # Custom callable filter (receives PurePosixPath relative to root)
        >>> result = hasher.hash_directory("/tmp/mydir", ignore=lambda p: p.name.startswith("."))
    """
    path = UPath(directory_path)
    filter_fn = _compile_ignore(ignore)
    digest = _hash_dir(path, path, filter_fn, self.algorithm, self.file_hasher)
    return ContentHash(method=f"merkle_{self.algorithm}", digest=digest)
```

- [ ] **Step 5: Run the full test suite to check**

```bash
uv run pytest tests/test_hashing/ -v
```

Expected: the new test `test_relative_path_pattern_scopes_to_subdirectory` now **PASSES**.
All other tests also pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/hashing/directory_hashers.py
git commit -m "feat(directory): match ignore patterns against relative POSIX paths"
```

---

## Task 3: Add hash invariant tests

**Files:**
- Modify: `tests/test_hashing/test_directory_handler.py`

- [ ] **Step 1: Add three invariant tests inside `TestBasicDirectoryHasher`**

After the `test_relative_path_pattern_scopes_to_subdirectory` test added in Task 1, add:

```python
def test_ignore_none_is_equivalent_to_no_ignore(self, tmp_path):
    """hash(dir) == hash(dir, ignore=None) — the new arg in absent form must not shift the hash."""
    d = tmp_path / "d"
    d.mkdir()
    (d / "file.txt").write_bytes(b"content")
    (d / "sub").mkdir()
    (d / "sub" / "nested.txt").write_bytes(b"nested")
    hasher = BasicDirectoryHasher(file_hasher=FileHasher())
    assert hasher.hash_directory(d) == hasher.hash_directory(d, ignore=None)

def test_non_matching_filter_is_equivalent_to_no_filter(self, tmp_path):
    """A filter that matches nothing must produce the same hash as no filter."""
    d = tmp_path / "d"
    d.mkdir()
    (d / "file.txt").write_bytes(b"content")
    (d / "sub").mkdir()
    (d / "sub" / "nested.txt").write_bytes(b"nested")
    hasher = BasicDirectoryHasher(file_hasher=FileHasher())
    assert hasher.hash_directory(d) == hasher.hash_directory(d, ignore=["*.nonexistent_xyz"])

def test_filter_identity_irrelevance(self, tmp_path):
    """Two patterns selecting the same effective file set must produce the same hash."""
    d = tmp_path / "d"
    d.mkdir()
    (d / "app.py").write_bytes(b"code")
    (d / "app.pyc").write_bytes(b"compiled")
    hasher = BasicDirectoryHasher(file_hasher=FileHasher())
    # Both patterns exclude only app.pyc — results must be identical
    h1 = hasher.hash_directory(d, ignore=["*.pyc"])
    h2 = hasher.hash_directory(d, ignore=["app.pyc"])
    assert h1 == h2
```

- [ ] **Step 2: Run the new tests**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py::TestBasicDirectoryHasher::test_ignore_none_is_equivalent_to_no_ignore tests/test_hashing/test_directory_handler.py::TestBasicDirectoryHasher::test_non_matching_filter_is_equivalent_to_no_filter tests/test_hashing/test_directory_handler.py::TestBasicDirectoryHasher::test_filter_identity_irrelevance -v
```

Expected: all 3 **PASS**.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest tests/test_hashing/ -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_hashing/test_directory_handler.py
git commit -m "test(directory): add hash invariant tests for ignore= filter (ITL-603)"
```

---

## Task 4: Update type hint and docstring in `directory_type.py`

**Files:**
- Modify: `src/orcapod/extension_types/directory_type.py`

- [ ] **Step 1: Add `PurePosixPath` import**

At the top of `src/orcapod/extension_types/directory_type.py`, the existing imports include:

```python
from __future__ import annotations

import importlib
import json
import os
import warnings
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, Self
```

Add `from pathlib import PurePosixPath` after the stdlib imports block:

```python
from __future__ import annotations

import importlib
import json
import os
import warnings
from collections.abc import Callable, Iterable
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Self
```

- [ ] **Step 2: Update `Directory.__init__` signature and docstring**

Replace the `__init__` method signature and its docstring. Find:

```python
    def __init__(
        self,
        *args: Any,
        ignore: Callable[[UPath], bool] | Iterable[str] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if not self.__wrapped__.exists():
            raise FileNotFoundError(
                f"Directory: path does not exist: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.is_dir():
            raise NotADirectoryError(
                f"Directory: path is not a directory: {self.__wrapped__!r}"
            )
        try:
            next(iter(self.__wrapped__.iterdir()), None)
        except PermissionError as exc:
            raise PermissionError(
                f"Directory: path is not traversable: {self.__wrapped__!r}"
            ) from exc
        self._ignore = ignore
```

Replace only the signature (first line) with:

```python
    def __init__(
        self,
        *args: Any,
        ignore: Callable[[PurePosixPath], bool] | Iterable[str] | None = None,
        **kwargs: Any,
    ) -> None:
```

(Leave the body unchanged — only the type hint on `ignore` changes.)

- [ ] **Step 3: Update the `Directory` class docstring's `ignore=` section**

In the class docstring, find the `Args:` section's `ignore` entry:

```
        ignore: Optional filter for excluding entries from the content hash.
            Accepts an iterable of glob patterns matched against entry names (via
            ``fnmatch``), or a callable ``(UPath) -> bool`` returning ``True`` to
            exclude an entry. Applied at every level of recursion during hashing.
            Defaults to ``None`` (all entries included).
```

Replace it with:

```
        ignore: Optional filter for excluding entries from the content hash.
            Accepts an iterable of glob patterns matched against the **POSIX
            relative path from the directory root** via
            ``pathlib.PurePath.match()`` (right-anchored: ``"*.pyc"`` matches
            any ``.pyc`` at any depth; ``"sub/*.pyc"`` matches only ``.pyc``
            files directly inside ``sub/``), or a callable
            ``(pathlib.PurePosixPath) -> bool`` receiving the relative path
            and returning ``True`` to exclude an entry. Applied at every level
            of recursion during hashing.

            Hash invariant: excluded entries are invisible to the hash — the
            result is identical to those entries never existing. The pattern
            itself is not input to the hash.

            Defaults to ``None`` (all entries included).
```

- [ ] **Step 4: Update the `Example:` block in the class docstring**

Find the existing example block:

```
    Example:
        >>> d = Directory("/tmp/mydata")
        >>> str(d)
        '/tmp/mydata'
        >>> Directory("/tmp/nonexistent")
        FileNotFoundError: ...
```

Replace with:

```
    Example:
        >>> d = Directory("/tmp/mydata")
        >>> str(d)
        '/tmp/mydata'
        >>> # Exclude compiled Python files at any depth
        >>> d = Directory("/tmp/mydata", ignore=["*.pyc", "__pycache__"])
        >>> # Exclude files only inside a specific subdirectory
        >>> d = Directory("/tmp/mydata", ignore=["build/*.o"])
        >>> # Custom callable filter: exclude hidden files
        >>> d = Directory("/tmp/mydata", ignore=lambda p: p.name.startswith("."))
        >>> Directory("/tmp/nonexistent")
        FileNotFoundError: ...
```

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/directory_type.py
git commit -m "docs(directory): update ignore= type hint and docstring for relative-path semantics (ITL-603)"
```

---

## Task 5: Final verification

- [ ] **Step 1: Run the complete test suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests PASS. No warnings about unexpected failures.

- [ ] **Step 2: Verify the test count increased by 4**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py -v --co -q
```

Confirm the 4 new tests appear:
- `TestBasicDirectoryHasher::test_relative_path_pattern_scopes_to_subdirectory`
- `TestBasicDirectoryHasher::test_ignore_none_is_equivalent_to_no_ignore`
- `TestBasicDirectoryHasher::test_non_matching_filter_is_equivalent_to_no_filter`
- `TestBasicDirectoryHasher::test_filter_identity_irrelevance`
