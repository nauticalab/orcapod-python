# Directory Hash Caching (ITL-511) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `BasicDirectoryHasher` to use an injected `FileContentHasherProtocol` for file-leaf hashing, extend `enable_file_hash_caching()` to also patch `DirectoryHandler`, and add user-facing documentation.

**Architecture:** `BasicDirectoryHasher` gains a required `file_hasher: FileContentHasherProtocol` constructor argument; `_hash_dir()` calls `file_hasher.hash_file()` instead of the bare `hash_utils.hash_file()`. `enable_file_hash_caching()` creates a shared `CachedFileHasher` and registers it with both `FileHandler` and a new `BasicDirectoryHasher` inside `DirectoryHandler`. `v0.1.json` wires the context-level `file_hasher` ref into `BasicDirectoryHasher`.

**Tech Stack:** Python 3.12, `uv run pytest`, `sqlite3`, `upath`

**Spec:** `superpowers/specs/2026-07-08-itl-511-directory-hash-caching-design.md`

**Branch:** `eywalker/itl-511-verify-opdirectory-hashing-uses-filehasher-cache-document`

---

## Task 0: Create and check out the feature branch

**Files:** none

- [ ] **Step 0.1: Create the branch**

```bash
git checkout main
git pull
git checkout -b eywalker/itl-511-verify-opdirectory-hashing-uses-filehasher-cache-document
git branch --show-current
```

Expected: `eywalker/itl-511-verify-opdirectory-hashing-uses-filehasher-cache-document`

---

## Task 1: Write failing caching integration tests

These tests verify the fix before it exists. They will fail because `BasicDirectoryHasher`
does not yet accept a `file_hasher` argument.

**Files:**
- Create: `tests/test_hashing/test_directory_hash_caching.py`

- [ ] **Step 1.1: Create the test file**

Create `tests/test_hashing/test_directory_hash_caching.py` with the following content:

```python
"""Integration tests: op.Directory hashing with CachedFileHasher.

Verifies that BasicDirectoryHasher consults the file-hash cache at the
per-file level during Merkle tree traversal, and that
enable_file_hash_caching() wires the same CachedFileHasher instance into
both FileHandler and DirectoryHandler.
"""

from __future__ import annotations

import pytest

from orcapod.extension_types.directory_type import Directory
from orcapod.extension_types.file_type import File
from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher
from orcapod.hashing.hash_cachers import InMemoryHashCacher
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CountingFileHasher:
    """FileContentHasherProtocol wrapper that counts hash_file() calls."""

    def __init__(self, inner: FileHasher) -> None:
        self.inner = inner
        self.call_count = 0

    def hash_file(self, file_path):
        self.call_count += 1
        return self.inner.hash_file(file_path)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dir_with_files(tmp_path):
    """A directory containing three text files."""
    d = tmp_path / "mydir"
    d.mkdir()
    (d / "a.txt").write_bytes(b"content_a")
    (d / "b.txt").write_bytes(b"content_b")
    (d / "c.txt").write_bytes(b"content_c")
    return d


@pytest.fixture()
def restore_default_handlers():
    """Restore both FileHandler and DirectoryHandler after each test."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.directory_type import Directory
    from orcapod.extension_types.file_type import File

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_file_handler = registry.get_handler_for_type(File)
    original_dir_handler = registry.get_handler_for_type(Directory)
    yield
    registry.register(File, original_file_handler)
    registry.register(Directory, original_dir_handler)


# ---------------------------------------------------------------------------
# Cache write / hit / invalidation
# ---------------------------------------------------------------------------


class TestDirectoryHashCaching:
    def test_cache_write_on_first_hash(self, dir_with_files):
        """First hash: underlying FileHasher called once per file; results stored."""
        counting = CountingFileHasher(FileHasher())
        cacher = InMemoryHashCacher()
        cached = CachedFileHasher(file_hasher=counting, cacher=cacher)
        hasher = BasicDirectoryHasher(file_hasher=cached)

        result = hasher.hash_directory(dir_with_files)

        assert isinstance(result, ContentHash)
        assert result.method == "merkle_sha256"
        assert counting.call_count == 3  # one per file; none cached yet

    def test_cache_hit_on_second_hash(self, dir_with_files):
        """Second hash of the same directory returns all results from cache."""
        counting = CountingFileHasher(FileHasher())
        cacher = InMemoryHashCacher()
        cached = CachedFileHasher(file_hasher=counting, cacher=cacher)
        hasher = BasicDirectoryHasher(file_hasher=cached)

        h1 = hasher.hash_directory(dir_with_files)
        count_after_first = counting.call_count  # 3 calls for 3 files

        h2 = hasher.hash_directory(dir_with_files)

        assert h1 == h2  # same directory content → same hash
        assert counting.call_count == count_after_first  # zero new calls; all from cache

    def test_cache_invalidated_on_file_change(self, dir_with_files):
        """Modifying a file (mtime_ns/size change) causes a cache miss for that file."""
        counting = CountingFileHasher(FileHasher())
        cacher = InMemoryHashCacher()
        cached = CachedFileHasher(file_hasher=counting, cacher=cacher)
        hasher = BasicDirectoryHasher(file_hasher=cached)

        hasher.hash_directory(dir_with_files)
        count_after_first = counting.call_count  # 3

        # Write different content → changes both mtime_ns and size → cache miss
        (dir_with_files / "b.txt").write_bytes(b"modified_content_with_different_length")

        hasher.hash_directory(dir_with_files)

        # Only b.txt triggered a new hash call; a.txt and c.txt came from cache
        assert counting.call_count == count_after_first + 1

    def test_hash_value_unchanged_vs_uncached(self, dir_with_files):
        """Cached and uncached hashing produce identical ContentHash values."""
        plain = BasicDirectoryHasher(file_hasher=FileHasher())
        cached = BasicDirectoryHasher(
            file_hasher=CachedFileHasher(
                file_hasher=FileHasher(),
                cacher=InMemoryHashCacher(),
            )
        )

        assert plain.hash_directory(dir_with_files) == cached.hash_directory(dir_with_files)


# ---------------------------------------------------------------------------
# enable_file_hash_caching() wires DirectoryHandler
# ---------------------------------------------------------------------------


class TestEnableFileHashCachingWiresDirectory:
    def test_directory_handler_uses_cached_file_hasher(
        self, restore_default_handlers, tmp_path
    ):
        """After enable_file_hash_caching(), DirectoryHandler uses a CachedFileHasher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        dir_handler = registry.get_handler_for_type(Directory)
        assert isinstance(dir_handler.directory_hasher.file_hasher, CachedFileHasher)

    def test_shared_cache_instance(self, restore_default_handlers, tmp_path):
        """FileHandler and DirectoryHandler share the same CachedFileHasher instance."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry

        file_hasher = registry.get_handler_for_type(File).file_hasher
        dir_file_hasher = registry.get_handler_for_type(Directory).directory_hasher.file_hasher

        assert file_hasher is dir_file_hasher  # exact same object
```

- [ ] **Step 1.2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_directory_hash_caching.py -v 2>&1 | head -40
```

Expected: `TypeError: BasicDirectoryHasher.__init__() got an unexpected keyword argument 'file_hasher'` (or similar). Tests fail because `file_hasher` is not yet accepted.

---

## Task 2: Fix `BasicDirectoryHasher` — required `file_hasher` parameter

**Files:**
- Modify: `src/orcapod/hashing/directory_hashers.py`

- [ ] **Step 2.1: Update `directory_hashers.py`**

Replace the entire file content with:

```python
"""BasicDirectoryHasher — recursive Merkle tree hashing for directory trees."""

from __future__ import annotations

import fnmatch
import hashlib
import logging
import os
from collections.abc import Callable

from upath import UPath

from orcapod.protocols.hashing_protocols import FileContentHasherProtocol
from orcapod.types import ContentHash, PathLike

logger = logging.getLogger(__name__)


def _compile_ignore(
    ignore: Callable[[UPath], bool] | list[str] | None,
) -> Callable[[UPath], bool] | None:
    """Convert an ignore spec to a single callable filter.

    Args:
        ignore: ``None`` (no filtering), a list of ``fnmatch`` glob patterns matched
            against entry names, or a callable ``(UPath) -> bool``.

    Returns:
        A callable ``(UPath) -> bool`` returning ``True`` to exclude an entry, or
        ``None`` if no filtering is needed.
    """
    if ignore is None:
        return None
    if callable(ignore):
        return ignore
    patterns = list(ignore)

    def _glob_filter(entry: UPath) -> bool:
        return any(fnmatch.fnmatch(entry.name, pat) for pat in patterns)

    return _glob_filter


def _hash_dir(
    path: UPath,
    filter_fn: Callable[[UPath], bool] | None,
    algorithm: str,
    file_hasher: FileContentHasherProtocol,
) -> bytes:
    """Recursively compute the Merkle hash of a directory.

    Args:
        path: The directory to hash.
        filter_fn: Optional filter callable; return ``True`` to exclude an entry.
        algorithm: Hash algorithm name used for structural (entry and node) hashing.
        file_hasher: Hasher used to compute ``ContentHash`` for each file leaf.

    Returns:
        The raw digest bytes for this directory node (length depends on the algorithm).
    """
    entries: list[tuple[bytes, bytes]] = []

    for child in path.iterdir():
        if filter_fn is not None and filter_fn(child):
            continue

        name_bytes = child.name.encode("utf-8")

        if child.is_symlink():
            # Hash-as-symlink: record the link target, never dereference.
            target = os.readlink(child)
            entry_bytes = b"symlink\x00" + name_bytes + b"\x00" + target.encode("utf-8")
        elif child.is_file():
            file_hash = file_hasher.hash_file(child)
            entry_bytes = b"file\x00" + name_bytes + b"\x00" + file_hash.digest
        elif child.is_dir():
            subdir_digest = _hash_dir(child, filter_fn, algorithm, file_hasher)
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


class BasicDirectoryHasher:
    """Recursive Merkle tree hasher for directory trees.

    Computes a stable content hash of a directory tree using a recursive Merkle scheme:
    file leaves hash their content via the injected ``file_hasher``, subdirectory nodes
    hash their sorted children, and the root hash propagates the entire tree. Symlinks
    are recorded as ``(symlink, target)`` without dereferencing — cycle-safe and
    deterministic.

    Pass a ``CachedFileHasher`` as ``file_hasher`` to avoid re-reading unchanged files
    on repeated calls (e.g. across pipeline runs).

    Args:
        file_hasher: Hasher used to compute the ``ContentHash`` for each file leaf in
            the tree. Any object with a ``hash_file(path) -> ContentHash`` method.
        algorithm: Hash algorithm for structural (Merkle entry and node) hashing.
            Defaults to ``"sha256"``.
        buffer_size: Retained for context round-trip use. File content buffer sizing
            is owned by ``file_hasher``. Defaults to 65536.

    Example:
        >>> from orcapod.hashing.file_hashers import FileHasher
        >>> hasher = BasicDirectoryHasher(file_hasher=FileHasher())
        >>> result = hasher.hash_directory("/tmp/mydir")
        >>> result.method
        'merkle_sha256'
    """

    def __init__(
        self,
        file_hasher: FileContentHasherProtocol,
        algorithm: str = "sha256",
        buffer_size: int = 65536,
    ) -> None:
        self.file_hasher = file_hasher
        self.algorithm = algorithm
        self.buffer_size = buffer_size

    def hash_directory(
        self,
        directory_path: PathLike,
        ignore: Callable[[UPath], bool] | list[str] | None = None,
    ) -> ContentHash:
        """Compute the recursive Merkle hash of a directory tree.

        Args:
            directory_path: Path to the directory to hash.
            ignore: Optional filter. A callable ``(UPath) -> bool`` returning ``True``
                to exclude an entry, or a list of glob patterns matched against entry
                names via ``fnmatch``. Applied at every level of recursion.

        Returns:
            A ``ContentHash`` with ``method="merkle_{algorithm}"``.

        Raises:
            FileNotFoundError: If ``directory_path`` does not exist.
            NotADirectoryError: If ``directory_path`` is not a directory.
            PermissionError: If the directory is not traversable.
        """
        path = UPath(directory_path)
        filter_fn = _compile_ignore(ignore)
        digest = _hash_dir(path, filter_fn, self.algorithm, self.file_hasher)
        return ContentHash(method=f"merkle_{self.algorithm}", digest=digest)
```

- [ ] **Step 2.2: Update `register_builtin_python_type_handlers` fallback**

In `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`, find the `if directory_hasher is None:` block (currently around line 456–458) and replace it:

```python
    if directory_hasher is None:
        from orcapod.hashing.directory_hashers import BasicDirectoryHasher
        directory_hasher = BasicDirectoryHasher(
            file_hasher=file_hasher,
            algorithm="sha256",
        )
```

Note: `file_hasher` is resolved in the block immediately above this one, so it is always a concrete `FileContentHasherProtocol` by this point.

- [ ] **Step 2.3: Run the new caching tests — they should now pass**

```bash
uv run pytest tests/test_hashing/test_directory_hash_caching.py::TestDirectoryHashCaching -v
```

Expected: 4 tests PASS (the `TestEnableFileHashCachingWiresDirectory` class will still fail — that's fine for now).

- [ ] **Step 2.4: Run the full test suite to see what broke**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -30
```

Expected: failures in `test_directory_handler.py` — `BasicDirectoryHasher()` calls missing the required `file_hasher` argument.

---

## Task 3: Fix existing `test_directory_handler.py` call sites

**Files:**
- Modify: `tests/test_hashing/test_directory_handler.py`

- [ ] **Step 3.1: Add `FileHasher` import and update `TestBasicDirectoryHasher`**

At the top of `tests/test_hashing/test_directory_handler.py`, add to the existing imports:

```python
from orcapod.hashing.file_hashers import FileHasher
```

Then replace every `BasicDirectoryHasher()` call (no args) in `TestBasicDirectoryHasher` with `BasicDirectoryHasher(file_hasher=FileHasher())`. There are approximately 15 such calls; every test method that reads `hasher = BasicDirectoryHasher()` becomes `hasher = BasicDirectoryHasher(file_hasher=FileHasher())`. The affected methods are:

- `test_empty_directory_returns_content_hash`
- `test_empty_directory_hash_is_stable` (2 uses of `hasher =`)
- `test_identical_content_same_hash`
- `test_different_content_different_hash`
- `test_single_byte_change_in_nested_file_changes_hash`
- `test_adding_file_changes_hash`
- `test_removing_file_changes_hash`
- `test_hidden_files_included_by_default`
- `test_ignore_glob_excludes_matching_files`
- `test_ignore_callable_excludes_entries`
- `test_symlink_recorded_not_followed`
- `test_symlink_cycle_safe`
- `test_large_tree_smoke_test`
- `test_rename_changes_hash`
- `test_ignore_applied_recursively`
- `test_special_files_skipped`

Also update the `TestDirectoryHandler.handler` fixture:

```python
@pytest.fixture
def handler(self):
    return DirectoryHandler(BasicDirectoryHasher(file_hasher=FileHasher()))
```

- [ ] **Step 3.2: Run the directory handler tests**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py -v
```

Expected: all tests PASS.

- [ ] **Step 3.3: Run the full test suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: all tests pass except `TestEnableFileHashCachingWiresDirectory` (addressed in Task 5).

- [ ] **Step 3.4: Commit**

```bash
git add src/orcapod/hashing/directory_hashers.py \
        src/orcapod/hashing/semantic_hashing/builtin_handlers.py \
        tests/test_hashing/test_directory_hash_caching.py \
        tests/test_hashing/test_directory_handler.py
git commit -m "feat(hashing): require file_hasher in BasicDirectoryHasher; wire per-file cache (ITL-511)"
```

---

## Task 4: Wire `file_hasher` reference in `v0.1.json`

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 4.1: Update `directory_hasher` config in `v0.1.json`**

Find the `"directory_hasher"` key (currently around line 11–16) and replace it:

```json
"directory_hasher": {
    "_class": "orcapod.hashing.directory_hashers.BasicDirectoryHasher",
    "_config": {
        "algorithm": "sha256",
        "file_hasher": {"_ref": "file_hasher"}
    }
},
```

- [ ] **Step 4.2: Add a changelog entry**

In the `"metadata"` → `"changelog"` array, append at the end:

```json
"Wired file_hasher reference into BasicDirectoryHasher so op.Directory hashing uses the same FileContentHasherProtocol as op.File hashing; required file_hasher constructor arg enforces explicit injection (ITL-511)"
```

- [ ] **Step 4.3: Run the full test suite to verify the JSON change is valid**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: same results as after Task 3 (JSON is parsed at context load time; the `_ref` mechanism is already used for `FileHandler`).

- [ ] **Step 4.4: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json
git commit -m "feat(contexts): wire file_hasher ref into BasicDirectoryHasher in v0.1.json (ITL-511)"
```

---

## Task 5: Extend `enable_file_hash_caching()` + update `test_hash_cachers.py`

**Files:**
- Modify: `src/orcapod/contexts/__init__.py`
- Modify: `tests/test_hashing/test_hash_cachers.py`

- [ ] **Step 5.1: Write failing tests for the directory-patching behaviour**

In `tests/test_hashing/test_hash_cachers.py`, update the `restore_default_file_handler` fixture to also restore `DirectoryHandler`, then add two new test methods to `TestEnableFileHashCaching`:

Replace the existing `restore_default_file_handler` fixture with:

```python
@pytest.fixture()
def restore_default_file_handler():
    """Restore FileHandler and DirectoryHandler after each test."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.directory_type import Directory
    from orcapod.extension_types.file_type import File

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_file_handler = registry.get_handler_for_type(File)
    original_dir_handler = registry.get_handler_for_type(Directory)
    yield
    registry.register(File, original_file_handler)
    registry.register(Directory, original_dir_handler)
```

Then add these two methods to `TestEnableFileHashCaching`:

```python
    def test_registers_cached_file_hasher_in_directory_handler(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.directory_type import Directory
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        dir_handler = registry.get_handler_for_type(Directory)
        assert isinstance(dir_handler.directory_hasher.file_hasher, CachedFileHasher)

    def test_file_and_directory_handlers_share_same_cached_hasher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.directory_type import Directory
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry

        file_hasher = registry.get_handler_for_type(File).file_hasher
        dir_file_hasher = registry.get_handler_for_type(Directory).directory_hasher.file_hasher

        assert file_hasher is dir_file_hasher
```

- [ ] **Step 5.2: Run the new tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching::test_registers_cached_file_hasher_in_directory_handler \
              tests/test_hashing/test_hash_cachers.py::TestEnableFileHashCaching::test_file_and_directory_handlers_share_same_cached_hasher \
              tests/test_hashing/test_directory_hash_caching.py::TestEnableFileHashCachingWiresDirectory \
              -v
```

Expected: all FAIL — `enable_file_hash_caching()` does not yet patch `DirectoryHandler`.

- [ ] **Step 5.3: Extend `enable_file_hash_caching()` in `src/orcapod/contexts/__init__.py`**

Replace the tail of `enable_file_hash_caching()` — specifically the final `registry.register(File, ...)` call and everything after it — with the following. This extracts the `CachedFileHasher` into a named variable so it can be shared with `DirectoryHandler`:

```python
    cached_file_hasher = CachedFileHasher(
        file_hasher=base_hasher,
        cacher=SqliteHashCacher(db_path),
    )

    registry.register(
        File,
        FileHandler(cached_file_hasher),
    )

    from orcapod.extension_types.directory_type import Directory
    from orcapod.hashing.directory_hashers import BasicDirectoryHasher
    from orcapod.hashing.semantic_hashing.builtin_handlers import DirectoryHandler

    existing_dir_handler = registry.get_handler_for_type(Directory)
    if existing_dir_handler is None:
        raise RuntimeError(
            "enable_file_hash_caching(): no DirectoryHandler registered for "
            "orcapod.Directory in the default context. This should not happen "
            "with the standard v0.1 context."
        )

    existing_dir_hasher = existing_dir_handler.directory_hasher
    registry.register(
        Directory,
        DirectoryHandler(
            BasicDirectoryHasher(
                file_hasher=cached_file_hasher,
                algorithm=existing_dir_hasher.algorithm,
                buffer_size=existing_dir_hasher.buffer_size,
            )
        ),
    )
```

Both `FileHandler` and `DirectoryHandler` receive the exact same `cached_file_hasher` instance — sharing the underlying `SqliteHashCacher`.

- [ ] **Step 5.4: Run all caching tests**

```bash
uv run pytest tests/test_hashing/test_hash_cachers.py \
              tests/test_hashing/test_directory_hash_caching.py \
              -v
```

Expected: all tests PASS.

- [ ] **Step 5.5: Run the full test suite**

```bash
uv run pytest tests/ -q 2>&1 | tail -10
```

Expected: all tests PASS.

- [ ] **Step 5.6: Commit**

```bash
git add src/orcapod/contexts/__init__.py \
        tests/test_hashing/test_hash_cachers.py
git commit -m "feat(contexts): extend enable_file_hash_caching to also patch DirectoryHandler (ITL-511)"
```

---

## Task 6: User-facing documentation

**Files:**
- Create: `docs/concepts/file-hash-caching.md`

- [ ] **Step 6.1: Create the documentation file**

Create `docs/concepts/file-hash-caching.md`:

```markdown
# File Hash Caching

Orcapod can cache per-file content hashes so that large files are only read
once per modification. This is especially useful in pipelines that repeatedly
hash the same large files (e.g. electrophysiology recordings in spike-sort
pipelines).

## How it works

`FileHasher` reads file bytes and returns a `ContentHash`. `CachedFileHasher`
wraps any `FileHasher` with a cache backend (`CacherProtocol`):

```python
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher

cacher = SqliteHashCacher()                          # SQLite-backed cache
cached_hasher = CachedFileHasher(FileHasher(), cacher)
```

On each `hash_file()` call the cache is checked first using the key
`(absolute_path, mtime_ns, size)`. A hit returns the stored `ContentHash`
immediately with no disk I/O. A miss reads the file, stores the result, and
returns it. Changing a file's content updates its `mtime_ns` and/or `size`,
which automatically produces a new cache key and invalidates the old entry.

## Activating caching

Call `enable_file_hash_caching()` **once at application startup**, before any
file or directory hashing occurs:

```python
from orcapod.contexts import enable_file_hash_caching

enable_file_hash_caching()
# or with an explicit database path:
enable_file_hash_caching(db_path="/data/.orcapod/cache.db")
```

This wires a `CachedFileHasher` (backed by `SqliteHashCacher`) into the
default context's `FileHandler` and `DirectoryHandler`. After this call, all
`op.File` and `op.Directory` hashing in the default context consults the
cache.

## `op.Directory` hashing

`op.Directory` hashing uses a recursive Merkle tree. Each file leaf is hashed
via the injected `file_hasher`. Once `enable_file_hash_caching()` is called,
the same `CachedFileHasher` instance is shared between `FileHandler` and
`DirectoryHandler` — so a file cached via a direct `op.File` hash is also a
cache hit during subsequent directory traversal, and vice versa.

No additional configuration is required.

## Cache storage

| Setting | Value |
|---------|-------|
| Default path | `~/.orcapod/file_hash_cache.db` |
| Env var override | `ORCAPOD_HASH_CACHE_DB=/path/to/cache.db` |
| Custom path | `enable_file_hash_caching(db_path="/path/to/cache.db")` |

The SQLite database uses WAL mode (safe for single-writer / multi-reader
access) and thread-local connections.

### Inspecting the cache

```bash
sqlite3 ~/.orcapod/file_hash_cache.db \
  "SELECT path, mtime_ns, size, cached_at FROM file_hash_cache LIMIT 20;"
```

### Clearing the cache

```python
from orcapod.hashing.hash_cachers import SqliteHashCacher

SqliteHashCacher().clear()        # clears all entries, keeps the database file
```

Or delete the file entirely:

```bash
rm ~/.orcapod/file_hash_cache.db
```

## When caching helps

- **Large files rehashed across pipeline runs** — a 500 MB recording read once
  per modification instead of once per pipeline run.
- **Directories with few changes** — only modified files are re-read; unchanged
  files are served from cache.

## When caching doesn't help

- **Small files** — the SQLite lookup overhead can exceed the time saved.
- **Files that change on every run** — every run is a cache miss; the entry is
  written but never read back.
- **First (cold) run** — all files are hashed from scratch regardless.
```

- [ ] **Step 6.2: Run the full test suite one final time**

```bash
uv run pytest tests/ -q 2>&1 | tail -10
```

Expected: all tests PASS.

- [ ] **Step 6.3: Commit**

```bash
git add docs/concepts/file-hash-caching.md
git commit -m "docs(concepts): add file-hash-caching activation guide (ITL-511)"
```

---

## Final check

- [ ] Run the complete test suite once more and confirm zero failures:

```bash
uv run pytest tests/ -q
```

- [ ] Verify the branch is ready for a PR:

```bash
git log --oneline main..HEAD
```

Expected output (4 commits):
```
<sha> docs(concepts): add file-hash-caching activation guide (ITL-511)
<sha> feat(contexts): extend enable_file_hash_caching to also patch DirectoryHandler (ITL-511)
<sha> feat(contexts): wire file_hasher ref into BasicDirectoryHasher in v0.1.json (ITL-511)
<sha> feat(hashing): require file_hasher in BasicDirectoryHasher; wire per-file cache (ITL-511)
```
