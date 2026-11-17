# `op.Directory` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `orcapod.Directory` as a content-identified directory type with recursive Merkle tree hashing and Arrow serialization; simultaneously migrate `LogicalFile` to JSON storage format for consistency.

**Architecture:** `Directory` subclasses `upath.extensions.ProxyUPath` with constructor validation (same pattern as `File`). Hashing uses a bottom-up recursive Merkle tree in `BasicDirectoryHasher`, registered via `DirectoryHandler` in the semantic hashing pipeline. `LogicalDirectory` and the migrated `LogicalFile` both store to JSON in Arrow `pa.large_string()`. The `ignore` filter is stored on the `Directory` instance and propagated to the hasher at hash time.

**Tech Stack:** Python, `upath.extensions.ProxyUPath`, PyArrow `pa.large_string()`, `hashlib.sha256`, `fnmatch`, `importlib`, `os.readlink`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/directory_type.py` | **Create** | `Directory` class, `LogicalDirectory`, `_try_import_callable` helper |
| `src/orcapod/hashing/directory_hashers.py` | **Create** | `BasicDirectoryHasher`, `_compile_ignore`, `_hash_dir` |
| `tests/test_extension_types/test_directory_type.py` | **Create** | Constructor + `LogicalDirectory` tests |
| `tests/test_hashing/test_directory_handler.py` | **Create** | `BasicDirectoryHasher` + `DirectoryHandler` tests |
| `src/orcapod/extension_types/file_type.py` | Modify | Migrate `LogicalFile` storage from plain string to JSON |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Modify | Add `DirectoryHandler`; update `register_builtin_python_type_handlers` |
| `src/orcapod/protocols/hashing_protocols.py` | Modify | Add `DirectoryHasherProtocol` |
| `src/orcapod/extension_types/__init__.py` | Modify | Export `LogicalDirectory` |
| `src/orcapod/__init__.py` | Modify | Export `Directory` |
| `src/orcapod/hashing/__init__.py` | Modify | Export `BasicDirectoryHasher`, `DirectoryHandler`, `DirectoryHasherProtocol` |
| `src/orcapod/hashing/semantic_hashing/__init__.py` | Modify | Export `DirectoryHandler` |
| `src/orcapod/contexts/data/v0.1.json` | Modify | Register `LogicalDirectory`, `DirectoryHandler`, `directory_hasher` |
| `tests/test_extension_types/test_file_type.py` | Modify | Update assertions for JSON storage format |

---

## Task 1: Migrate `LogicalFile` to JSON storage format

**Files:**
- Modify: `src/orcapod/extension_types/file_type.py`
- Modify: `tests/test_extension_types/test_file_type.py`

- [ ] **Step 1: Update failing tests first**

In `tests/test_extension_types/test_file_type.py`, update the `TestLogicalFile` class. Replace the three affected test methods with these JSON-aware versions (add `import json` at the top of the file):

```python
import json  # add at top of file

# Replace test_python_to_storage_returns_string:
def test_python_to_storage_returns_json_string(self, tmp_path):
    p = tmp_path / "f.txt"
    p.write_text("x")
    f = File(p)
    lt = LogicalFile()
    result = lt.python_to_storage(f)
    data = json.loads(result)
    assert data == {"path": str(p)}

# Replace test_storage_to_python_returns_file:
def test_storage_to_python_accepts_json_string(self, tmp_path):
    p = tmp_path / "f.txt"
    p.write_text("x")
    lt = LogicalFile()
    result = lt.storage_to_python(json.dumps({"path": str(p)}))
    assert isinstance(result, File)
    assert str(result) == str(p)

# Replace test_storage_to_python_raises_if_file_missing:
def test_storage_to_python_raises_if_file_missing(self, tmp_path):
    lt = LogicalFile()
    with pytest.raises(FileNotFoundError):
        lt.storage_to_python(json.dumps({"path": str(tmp_path / "gone.txt")}))
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_file_type.py::TestLogicalFile -v
```

Expected: 3 FAILED (wrong storage format), other tests PASS.

- [ ] **Step 3: Update `LogicalFile` in `file_type.py`**

Add `import json` after `from __future__ import annotations`. Then replace `python_to_storage` and `storage_to_python`:

```python
import json  # add after "from __future__ import annotations"

# Replace python_to_storage:
def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> str:
    """Convert a ``File`` to its JSON storage representation.

    Args:
        value: A ``File`` instance.
        converter: Ignored. Present for protocol conformance.

    Returns:
        A JSON string ``{"path": "<path>"}`` encoding the file path.
    """
    return json.dumps({"path": str(value)})

# Replace storage_to_python:
def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> File:
    """Reconstruct a ``File`` from its stored JSON string.

    Re-validates existence on read — raises ``FileNotFoundError`` if the file
    no longer exists at the stored path.

    Args:
        storage_value: A JSON string as stored in Arrow.
        converter: Ignored. Present for protocol conformance.

    Returns:
        A ``File`` instance.

    Raises:
        FileNotFoundError: If the path no longer exists.
        IsADirectoryError: If the path is now a directory.
    """
    return File(json.loads(storage_value)["path"])
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_file_type.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/file_type.py tests/test_extension_types/test_file_type.py
git commit -m "fix(types): migrate LogicalFile Arrow storage from plain string to JSON (ITL-451)"
```

---

## Task 2: Add `DirectoryHasherProtocol`

**Files:**
- Modify: `src/orcapod/protocols/hashing_protocols.py`

- [ ] **Step 1: Add the protocol class**

In `src/orcapod/protocols/hashing_protocols.py`, add this class after `FileContentHasherProtocol` (after line 139):

```python
class DirectoryHasherProtocol(Protocol):
    """Protocol for directory tree hashing."""

    def hash_directory(
        self,
        directory_path: "PathLike",
        ignore: "Callable | list[str] | None" = None,
    ) -> "ContentHash": ...
```

Also add `Callable` to the existing imports at the top of the file:

```python
from collections.abc import Callable  # already present — verify it's there
```

(The file already imports `Callable` from `collections.abc` — no change needed if present.)

- [ ] **Step 2: Verify import works**

```bash
uv run python -c "from orcapod.protocols.hashing_protocols import DirectoryHasherProtocol; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/protocols/hashing_protocols.py
git commit -m "feat(protocols): add DirectoryHasherProtocol (ITL-451)"
```

---

## Task 3: Implement `BasicDirectoryHasher`

**Files:**
- Create: `src/orcapod/hashing/directory_hashers.py`
- Create: `tests/test_hashing/test_directory_handler.py` (hashing tests only in this task)

- [ ] **Step 1: Write failing tests for `BasicDirectoryHasher`**

Create `tests/test_hashing/test_directory_handler.py`:

```python
"""Tests for BasicDirectoryHasher and DirectoryHandler."""

from __future__ import annotations

import pytest

from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.types import ContentHash


class TestBasicDirectoryHasher:
    def test_empty_directory_returns_content_hash(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        hasher = BasicDirectoryHasher()
        result = hasher.hash_directory(empty)
        assert isinstance(result, ContentHash)
        assert result.method == "merkle_sha256"

    def test_empty_directory_hash_is_stable(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_identical_content_same_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"hello")
        (d2 / "file.txt").write_bytes(b"hello")
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_different_content_different_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"hello")
        (d2 / "file.txt").write_bytes(b"world")
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_single_byte_change_in_nested_file_changes_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        for base in (d1, d2):
            (base / "sub" / "deep").mkdir(parents=True)
            (base / "sub" / "deep" / "unchanged.txt").write_bytes(b"same content")
        (d1 / "sub" / "deep" / "target.txt").write_bytes(b"hello world")
        (d2 / "sub" / "deep" / "target.txt").write_bytes(b"hello World")  # capital W
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_adding_file_changes_hash(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "a.txt").write_bytes(b"content")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d)
        (d / "b.txt").write_bytes(b"new file")
        h2 = hasher.hash_directory(d)
        assert h1 != h2

    def test_removing_file_changes_hash(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "a.txt").write_bytes(b"content")
        (d / "b.txt").write_bytes(b"to remove")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d)
        (d / "b.txt").unlink()
        h2 = hasher.hash_directory(d)
        assert h1 != h2

    def test_hidden_files_included_by_default(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"same")
        (d2 / "file.txt").write_bytes(b"same")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d1)
        (d2 / ".hidden").write_bytes(b"dotfile")
        h2 = hasher.hash_directory(d2)
        assert h1 != h2

    def test_ignore_glob_excludes_matching_files(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "app.pyc").write_bytes(b"compiled")  # extra .pyc, excluded
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2, ignore=["*.pyc"])

    def test_ignore_callable_excludes_entries(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "excluded.txt").write_bytes(b"extra")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d1)
        h2 = hasher.hash_directory(d2, ignore=lambda p: p.name == "excluded.txt")
        assert h1 == h2

    def test_symlink_recorded_not_followed(self, tmp_path):
        """Two dirs with identical symlinks to the same target → same hash."""
        real_file = tmp_path / "real.txt"
        real_file.write_bytes(b"content")
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "link").symlink_to(real_file)
        (d2 / "link").symlink_to(real_file)
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_symlink_cycle_safe(self, tmp_path):
        """A symlink pointing to an ancestor directory must not cause infinite recursion."""
        d = tmp_path / "d"
        d.mkdir()
        (d / "self_link").symlink_to(d)  # points to its own parent
        hasher = BasicDirectoryHasher()
        result = hasher.hash_directory(d)  # must complete without error
        assert isinstance(result, ContentHash)

    def test_large_tree_smoke_test(self, tmp_path):
        """Hashing 200 files across 10 subdirectories must complete without error."""
        for i in range(10):
            sub = tmp_path / f"sub_{i:02d}"
            sub.mkdir()
            for j in range(20):
                (sub / f"file_{j:03d}.txt").write_bytes(f"content {i} {j}".encode())
        hasher = BasicDirectoryHasher()
        result = hasher.hash_directory(tmp_path)
        assert isinstance(result, ContentHash)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py::TestBasicDirectoryHasher -v
```

Expected: All FAILED (module not found).

- [ ] **Step 3: Create `src/orcapod/hashing/directory_hashers.py`**

```python
"""BasicDirectoryHasher — recursive Merkle tree hashing for directory trees."""

from __future__ import annotations

import fnmatch
import hashlib
import logging
import os
from collections.abc import Callable
from typing import Any

from upath import UPath

from orcapod.hashing.hash_utils import hash_file
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
    buffer_size: int,
) -> bytes:
    """Recursively compute the Merkle hash of a directory.

    Returns the raw 32-byte SHA-256 digest for the directory node.
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
            file_hash = hash_file(child, algorithm=algorithm, buffer_size=buffer_size)
            entry_bytes = b"file\x00" + name_bytes + b"\x00" + file_hash.digest
        elif child.is_dir():
            subdir_digest = _hash_dir(child, filter_fn, algorithm, buffer_size)
            entry_bytes = b"dir\x00" + name_bytes + b"\x00" + subdir_digest
        else:
            # Special file (socket, device node, named pipe) — skip silently.
            logger.debug("BasicDirectoryHasher: skipping special file %s", child)
            continue

        entry_digest = hashlib.sha256(entry_bytes).digest()
        entries.append((name_bytes, entry_digest))

    # Sort byte-wise by name — locale-independent, deterministic.
    entries.sort(key=lambda x: x[0])

    h = hashlib.sha256()
    h.update(b"directory\x00")
    for _, entry_digest in entries:
        h.update(entry_digest)
    return h.digest()


class BasicDirectoryHasher:
    """Recursive Merkle tree hasher for directory trees.

    Computes a stable content hash of a directory tree using a recursive Merkle scheme:
    file leaves hash their content, subdirectory nodes hash their sorted children, and
    the root hash propagates the entire tree. Symlinks are recorded as ``(symlink, target)``
    without dereferencing — cycle-safe and deterministic.

    Args:
        algorithm: Hash algorithm for file-content leaves. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes for file content. Defaults to 65536.

    Example:
        >>> hasher = BasicDirectoryHasher()
        >>> result = hasher.hash_directory("/tmp/mydir")
        >>> result.method
        'merkle_sha256'
    """

    def __init__(self, algorithm: str = "sha256", buffer_size: int = 65536) -> None:
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
            A ``ContentHash`` with ``method="merkle_sha256"``.
        """
        path = UPath(directory_path)
        filter_fn = _compile_ignore(ignore)
        digest = _hash_dir(path, filter_fn, self.algorithm, self.buffer_size)
        return ContentHash(method="merkle_sha256", digest=digest)
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py::TestBasicDirectoryHasher -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/directory_hashers.py tests/test_hashing/test_directory_handler.py
git commit -m "feat(hashing): add BasicDirectoryHasher with recursive Merkle tree hashing (ITL-451)"
```

---

## Task 4: Implement `Directory` class and `LogicalDirectory`

**Files:**
- Create: `src/orcapod/extension_types/directory_type.py`
- Create: `tests/test_extension_types/test_directory_type.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_extension_types/test_directory_type.py`:

```python
"""Tests for orcapod.extension_types.directory_type.Directory and LogicalDirectory."""

from __future__ import annotations

import json

import pytest
import pyarrow as pa

from orcapod.extension_types.directory_type import Directory, LogicalDirectory, _try_import_callable


class TestDirectoryConstructor:
    def test_rejects_nonexistent_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Directory(tmp_path / "does_not_exist")

    def test_rejects_non_directory_path(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("content")
        with pytest.raises(NotADirectoryError):
            Directory(p)

    def test_accepts_empty_directory(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        obj = Directory(d)
        assert str(obj) == str(d)

    def test_accepts_non_empty_directory(self, tmp_path):
        d = tmp_path / "nonempty"
        d.mkdir()
        (d / "file.txt").write_text("hello")
        obj = Directory(d)
        assert str(obj) == str(d)

    def test_str_returns_path_string(self, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        obj = Directory(d)
        assert str(obj) == str(d)

    def test_ignore_stored_on_instance(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        patterns = ["*.pyc", ".git"]
        obj = Directory(d, ignore=patterns)
        assert obj._ignore == patterns

    def test_ignore_none_by_default(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d)
        assert obj._ignore is None


class TestLogicalDirectory:
    def test_logical_type_name(self):
        lt = LogicalDirectory()
        assert lt.logical_type_name == "orcapod.directory"

    def test_python_type(self):
        lt = LogicalDirectory()
        assert lt.python_type is Directory

    def test_arrow_ext_name(self):
        lt = LogicalDirectory()
        assert lt.get_arrow_extension_type().extension_name == "orcapod.directory"

    def test_arrow_ext_storage_type(self):
        lt = LogicalDirectory()
        assert lt.get_arrow_extension_type().storage_type == pa.large_string()

    def test_arrow_extension_type_is_cached(self):
        lt = LogicalDirectory()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_python_to_storage_no_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d)
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data == {"path": str(d)}

    def test_python_to_storage_with_glob_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=["*.pyc", ".git"])
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        # patterns stored sorted
        assert data == {"path": str(d), "ignore": [".git", "*.pyc"]}

    def test_python_to_storage_with_lambda_warns_and_drops_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        fn = lambda p: p.name.endswith(".pyc")  # noqa: E731
        obj = Directory(d, ignore=fn)
        lt = LogicalDirectory()
        with pytest.warns(UserWarning, match="lambda"):
            storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data == {"path": str(d)}

    def test_python_to_storage_with_named_callable_stores_qualname(self, tmp_path):
        import json as _json_mod
        d = tmp_path / "d"
        d.mkdir()
        # json.dumps: __module__="json", __qualname__="dumps" — stable and importable
        obj = Directory(d, ignore=_json_mod.dumps)
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data.get("ignore_callable") == "json:dumps"

    def test_storage_to_python_no_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        lt = LogicalDirectory()
        result = lt.storage_to_python(json.dumps({"path": str(d)}))
        assert isinstance(result, Directory)
        assert str(result) == str(d)
        assert result._ignore is None

    def test_storage_to_python_with_glob_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        lt = LogicalDirectory()
        result = lt.storage_to_python(json.dumps({"path": str(d), "ignore": ["*.pyc"]}))
        assert isinstance(result, Directory)
        assert result._ignore == ["*.pyc"]

    def test_round_trip_no_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d)
        lt = LogicalDirectory()
        recovered = lt.storage_to_python(lt.python_to_storage(obj))
        assert str(recovered) == str(obj)
        assert recovered._ignore is None

    def test_round_trip_glob_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=["*.pyc", ".git"])
        lt = LogicalDirectory()
        recovered = lt.storage_to_python(lt.python_to_storage(obj))
        assert str(recovered) == str(obj)
        assert recovered._ignore == [".git", "*.pyc"]  # sorted on storage

    def test_round_trip_named_callable_recovered(self, tmp_path):
        import json as _json_mod
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=_json_mod.dumps)
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        recovered = lt.storage_to_python(storage)
        assert recovered._ignore is _json_mod.dumps

    def test_storage_to_python_raises_if_directory_missing(self, tmp_path):
        lt = LogicalDirectory()
        with pytest.raises(FileNotFoundError):
            lt.storage_to_python(json.dumps({"path": str(tmp_path / "gone")}))


class TestTryImportCallable:
    def test_imports_known_function(self):
        import json as _json_mod
        result = _try_import_callable("json:dumps")
        assert result is _json_mod.dumps

    def test_returns_none_on_bad_module(self):
        with pytest.warns(UserWarning):
            result = _try_import_callable("nonexistent_module_xyz:some_fn")
        assert result is None

    def test_returns_none_on_bad_format(self):
        with pytest.warns(UserWarning):
            result = _try_import_callable("no_colon_separator")
        assert result is None
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_directory_type.py -v
```

Expected: All FAILED (module not found).

- [ ] **Step 3: Create `src/orcapod/extension_types/directory_type.py`**

```python
"""orcapod.Directory — content-identified, existence-validated directory path.

``Directory`` wraps a ``upath.UPath`` and validates that the path points to a readable,
traversable directory at construction time. Use ``pathlib.Path`` / ``upath.UPath`` for
paths that may not yet exist.

``LogicalDirectory`` is the Arrow extension type that serialises ``Directory`` instances as
``large_string`` columns tagged with the ``"orcapod.directory"`` extension name. The stored
value is always a JSON object containing the path and, if set, the ignore parameter.
"""

from __future__ import annotations

import importlib
import json
import logging
import warnings
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Self

import polars as pl
import pyarrow as pa
from upath import UPath
from upath.extensions import ProxyUPath

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol

logger = logging.getLogger(__name__)


class Directory(ProxyUPath):
    """A content-identified, existence-validated directory path.

    Wraps a ``UPath`` and validates that the path refers to a readable, traversable
    directory at construction time. Works across local, S3, GCS, and any other
    fsspec-backed backend supported by ``upath``.

    Note:
        ``isinstance(directory_instance, UPath)`` returns ``False`` because
        ``ProxyUPath`` does not inherit from ``UPath``. Use
        ``isinstance(x, Directory)`` to type-check.

    Args:
        *args: Positional path arguments forwarded to ``UPath``.
        ignore: Optional filter for excluding entries from the content hash.
            Accepts a list of glob patterns matched against entry names (via
            ``fnmatch``), or a callable ``(UPath) -> bool`` returning ``True`` to
            exclude an entry. Applied at every level of recursion during hashing.
            Defaults to ``None`` (all entries included).
        **kwargs: Keyword arguments forwarded to ``UPath``.

    Raises:
        FileNotFoundError: If the path does not exist.
        NotADirectoryError: If the path is not a directory.
        PermissionError: If the directory cannot be traversed.

    Example:
        >>> d = Directory("/tmp/mydata")
        >>> str(d)
        '/tmp/mydata'
        >>> Directory("/tmp/nonexistent")
        FileNotFoundError: ...
    """

    def __init__(
        self,
        *args: Any,
        ignore: Callable[[UPath], bool] | list[str] | None = None,
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
        except PermissionError:
            raise PermissionError(
                f"Directory: path is not traversable: {self.__wrapped__!r}"
            )
        self._ignore = ignore

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        """Create a ``Directory`` from an existing ``UPath`` without validation.

        Used internally by ``ProxyUPath`` for derived paths (e.g. ``.parent``,
        ``/`` operator). Validation is intentionally skipped — derived paths from
        navigation may not exist yet. ``ignore`` defaults to ``None`` on all derived
        instances.
        """
        obj = object.__new__(cls)
        object.__setattr__(obj, "__wrapped__", upath)
        object.__setattr__(obj, "_ignore", None)
        return obj


def _try_import_callable(full_name: str) -> Callable[..., Any] | None:
    """Attempt to import a callable by its ``"module:qualname"`` serialised form.

    Args:
        full_name: A string of the form ``"module.path:QualifiedName"`` produced by
            ``LogicalDirectory.python_to_storage`` for named callables.

    Returns:
        The recovered callable, or ``None`` with a ``UserWarning`` if recovery fails.
    """
    if ":" not in full_name:
        warnings.warn(
            f"Directory: cannot recover ignore callable from {full_name!r} "
            "(expected 'module:qualname' format). Falling back to ignore=None.",
            UserWarning,
            stacklevel=2,
        )
        return None
    module_path, qualname = full_name.split(":", 1)
    try:
        mod = importlib.import_module(module_path)
        obj: Any = mod
        for attr in qualname.split("."):
            obj = getattr(obj, attr)
        return obj  # type: ignore[return-value]
    except (ImportError, AttributeError) as exc:
        warnings.warn(
            f"Directory: cannot recover ignore callable {full_name!r}: {exc}. "
            "Falling back to ignore=None.",
            UserWarning,
            stacklevel=2,
        )
        return None


class LogicalDirectory(BaseLogicalType):
    """Logical type for ``orcapod.Directory``.

    Stores ``Directory`` instances as Arrow large strings using the custom extension
    type ``"orcapod.directory"``. The stored value is always a JSON object containing
    the ``"path"`` key, and optionally ``"ignore"`` (glob pattern list) or
    ``"ignore_callable"`` (``"module:qualname"`` string for named callables).

    On read (``storage_to_python``), the path is used to reconstruct a ``Directory``
    instance, which re-validates existence. Reading an Arrow table with
    ``"orcapod.directory"`` columns will raise ``FileNotFoundError`` if the directory
    has been moved or deleted.

    Example:
        >>> import tempfile
        >>> lt = LogicalDirectory()
        >>> with tempfile.TemporaryDirectory() as tmp:
        ...     d = Directory(tmp)
        ...     lt.storage_to_python(lt.python_to_storage(d)) == d
        True
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.directory", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.directory", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.directory"
    python_type: type = Directory

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``Directory``.

        Returns:
            A cached ``pa.ExtensionType`` with extension name ``"orcapod.directory"``
            and storage type ``pa.large_string()``.
        """
        if LogicalDirectory._arrow_ext is None:
            LogicalDirectory._arrow_ext = LogicalDirectory._arrow_ext_class()
        return LogicalDirectory._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``Directory``.

        Returns:
            A cached ``pl.BaseExtension`` registered under ``"orcapod.directory"``.
        """
        if LogicalDirectory._polars_ext is None:
            LogicalDirectory._polars_ext = LogicalDirectory._polars_ext_class()
        return LogicalDirectory._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> str:
        """Convert a ``Directory`` to its JSON storage representation.

        The ``ignore`` parameter is serialised as follows:

        * ``None`` → ``{"path": "..."}``
        * ``list[str]`` → ``{"path": "...", "ignore": [...]}`` (patterns sorted)
        * Named callable → ``{"path": "...", "ignore_callable": "module:qualname"}``
        * Lambda / closure / built-in → ``{"path": "..."}`` + ``UserWarning``

        Args:
            value: A ``Directory`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A JSON string.
        """
        path_str = str(value)
        ignore = getattr(value, "_ignore", None)

        if ignore is None:
            return json.dumps({"path": path_str})

        if isinstance(ignore, list):
            return json.dumps({"path": path_str, "ignore": sorted(ignore)})

        # Callable — attempt best-effort serialisation via module:qualname.
        qualname = getattr(ignore, "__qualname__", "")
        module = getattr(ignore, "__module__", "")
        if module and qualname and "<" not in qualname:
            full_name = f"{module}:{qualname}"
            return json.dumps({"path": path_str, "ignore_callable": full_name})

        warnings.warn(
            f"Directory.ignore is a callable ({ignore!r}) that cannot be serialised "
            "(lambda, closure, or built-in). The ignore filter will be lost on "
            "roundtrip. Use a list of glob patterns for a lossless roundtrip.",
            UserWarning,
            stacklevel=2,
        )
        return json.dumps({"path": path_str})

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> Directory:
        """Reconstruct a ``Directory`` from its stored JSON string.

        Re-validates existence on read — raises ``FileNotFoundError`` if the directory
        no longer exists at the stored path.

        Args:
            storage_value: A JSON string as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``Directory`` instance.

        Raises:
            FileNotFoundError: If the path no longer exists.
            NotADirectoryError: If the path is now a non-directory.
        """
        data = json.loads(storage_value)
        path = data["path"]

        if "ignore_callable" in data:
            fn = _try_import_callable(data["ignore_callable"])
            return Directory(path, ignore=fn)

        if "ignore" in data:
            patterns = data["ignore"] or None
            return Directory(path, ignore=patterns)

        return Directory(path)
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_directory_type.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/directory_type.py tests/test_extension_types/test_directory_type.py
git commit -m "feat(types): add Directory and LogicalDirectory extension types (ITL-451)"
```

---

## Task 5: Add `DirectoryHandler`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Modify: `tests/test_hashing/test_directory_handler.py` (add `TestDirectoryHandler` class)

- [ ] **Step 1: Write failing tests**

Append to `tests/test_hashing/test_directory_handler.py` (after the existing imports and `TestBasicDirectoryHasher` class, add new imports and class):

```python
# Add these imports at the top of the file:
from orcapod.extension_types.directory_type import Directory
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    DirectoryHandler,
    register_builtin_python_type_handlers,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry


# Add this class after TestBasicDirectoryHasher:
class TestDirectoryHandler:
    @pytest.fixture
    def handler(self):
        return DirectoryHandler(BasicDirectoryHasher())

    @pytest.fixture
    def hasher(self):
        registry = PythonTypeHandlerRegistry()
        register_builtin_python_type_handlers(registry)
        return SemanticAwarePythonHasher(
            hasher_id="test_directory_v0",
            type_handler_registry=registry,
        )

    def test_returns_content_hash(self, handler, hasher, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "file.txt").write_bytes(b"content")
        result = handler.handle(Directory(d), hasher)
        assert isinstance(result, ContentHash)

    def test_rejects_non_directory_object(self, handler, hasher):
        with pytest.raises(TypeError, match="DirectoryHandler"):
            handler.handle("not_a_directory", hasher)

    def test_same_content_same_hash(self, handler, hasher, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").write_bytes(b"identical")
        (d2 / "a.txt").write_bytes(b"identical")
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2), hasher)
        assert h1 == h2

    def test_different_content_different_hash(self, handler, hasher, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").write_bytes(b"content A")
        (d2 / "a.txt").write_bytes(b"content B")
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2), hasher)
        assert h1 != h2

    def test_passes_ignore_to_hasher(self, handler, hasher, tmp_path):
        """ignore on the Directory instance is forwarded to the hasher."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "app.pyc").write_bytes(b"compiled")  # extra .pyc, should be excluded
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2, ignore=["*.pyc"]), hasher)
        assert h1 == h2
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py::TestDirectoryHandler -v
```

Expected: All FAILED (`DirectoryHandler` not found).

- [ ] **Step 3: Add `DirectoryHandler` to `builtin_handlers.py`**

In `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`:

**3a.** Update the module docstring — replace the first block comment to include `DirectoryHandler`:

```python
"""
Built-in PythonTypeHandlerProtocol implementations.

  UUIDHandler       -- uuid.UUID: 16-byte binary representation
  BytesHandler      -- bytes/bytearray: hex string representation
  FunctionHandler   -- callable with __code__: via FunctionInfoExtractorProtocol
  TypeObjectHandler -- type objects: stable "type:<module>.<qualname>" string
  SpecialFormHandler    -- typing._SpecialForm
  GenericAliasHandler   -- generic alias type annotations
  UnionTypeHandler      -- types.UnionType (Python 3.10+ X | Y syntax)
  ArrowTableHandler     -- pa.Table / pa.RecordBatch
  SchemaHandler         -- Schema objects
  FileHandler       -- orcapod.File: file content hash
  DirectoryHandler  -- orcapod.Directory: recursive Merkle tree hash

``register_builtin_python_type_handlers(registry)`` populates a registry
with all of the above.
"""
```

**3b.** Add `DirectoryHasherProtocol` to the `TYPE_CHECKING` import block:

```python
if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        DirectoryHasherProtocol,
        FileContentHasherProtocol,
        HandlerRegistryProtocol,
        SemanticHasherProtocol,
    )
```

**3c.** Add the `DirectoryHandler` class after `FileHandler` (after line 198):

```python
class DirectoryHandler:
    """Hasher for ``orcapod.Directory`` objects — hashes directory *content* via Merkle tree.

    By the time ``handle`` is called, ``Directory``'s constructor has already validated
    that the path exists and is a traversable directory. The hash is produced by
    ``BasicDirectoryHasher`` using a recursive Merkle scheme.

    Args:
        directory_hasher: Any object with a
            ``hash_directory(path, ignore) -> ContentHash`` method.
    """

    def __init__(self, directory_hasher: "DirectoryHasherProtocol") -> None:
        self.directory_hasher = directory_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        # Deferred import breaks the circular dependency between this module and
        # directory_type.py — the same pattern used by FileHandler.
        from orcapod.extension_types.directory_type import Directory
        if not isinstance(obj, Directory):
            raise TypeError(
                f"DirectoryHandler: expected an orcapod.Directory, got {type(obj)!r}"
            )
        wrapped = getattr(obj, "__wrapped__")
        ignore = getattr(obj, "_ignore", None)
        logger.debug("DirectoryHandler: hashing directory content at %s", wrapped)
        return self.directory_hasher.hash_directory(wrapped, ignore=ignore)
```

**3d.** Replace `register_builtin_python_type_handlers` entirely with the following complete implementation (adds `directory_hasher` parameter and `Directory` registration):

```python
def register_builtin_python_type_handlers(
    registry: "HandlerRegistryProtocol",
    file_hasher: Any = None,
    directory_hasher: Any = None,
    function_info_extractor: Any = None,
    arrow_hasher: "ArrowHasherProtocol | None" = None,
) -> None:
    if file_hasher is None:
        from orcapod.hashing.file_hashers import BasicFileHasher
        file_hasher = BasicFileHasher(algorithm="sha256")

    if directory_hasher is None:
        from orcapod.hashing.directory_hashers import BasicDirectoryHasher
        directory_hasher = BasicDirectoryHasher(algorithm="sha256")

    if function_info_extractor is None:
        from orcapod.hashing.semantic_hashing.function_info_extractors import (
            FunctionSignatureExtractor,
        )
        function_info_extractor = FunctionSignatureExtractor(
            include_module=True,
            include_defaults=True,
        )

    bytes_hasher = BytesHandler()
    registry.register(bytes, bytes_hasher)
    registry.register(bytearray, bytes_hasher)

    registry.register(UUID, UUIDHandler())

    from orcapod.extension_types.file_type import File
    registry.register(File, FileHandler(file_hasher))

    from orcapod.extension_types.directory_type import Directory
    registry.register(Directory, DirectoryHandler(directory_hasher))

    import types as _types

    function_hasher = FunctionHandler(function_info_extractor)
    registry.register(_types.FunctionType, function_hasher)
    registry.register(_types.BuiltinFunctionType, function_hasher)
    registry.register(_types.MethodType, function_hasher)

    registry.register(type, TypeObjectHandler())
    registry.register(_types.UnionType, UnionTypeHandler())

    generic_alias_hasher = GenericAliasHandler()
    registry.register(_types.GenericAlias, generic_alias_hasher)
    try:
        import typing as _typing
        registry.register(_typing._GenericAlias, generic_alias_hasher)  # type: ignore[attr-defined]
        registry.register(_typing._SpecialForm, SpecialFormHandler())  # type: ignore[attr-defined]
    except AttributeError:
        pass

    registry.register(Schema, SchemaHandler())

    import pyarrow as _pa
    arrow_table_hasher = ArrowTableHandler(arrow_hasher)
    registry.register(_pa.Table, arrow_table_hasher)
    registry.register(_pa.RecordBatch, arrow_table_hasher)
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_hashing/test_directory_handler.py -v
```

Expected: All PASS (both `TestBasicDirectoryHasher` and `TestDirectoryHandler`).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py tests/test_hashing/test_directory_handler.py
git commit -m "feat(hashing): add DirectoryHandler and wire into register_builtin_python_type_handlers (ITL-451)"
```

---

## Task 6: Wire up exports and `v0.1.json`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `src/orcapod/__init__.py`
- Modify: `src/orcapod/hashing/__init__.py`
- Modify: `src/orcapod/hashing/semantic_hashing/__init__.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Export `LogicalDirectory` from `extension_types/__init__.py`**

In `src/orcapod/extension_types/__init__.py`, add after the `LogicalFile` import and `__all__` entry:

```python
from .directory_type import LogicalDirectory  # ITL-451
```

And in `__all__`:
```python
    # ITL-451
    "LogicalDirectory",
```

- [ ] **Step 2: Export `Directory` from `orcapod/__init__.py`**

In `src/orcapod/__init__.py`, add after the `File` import:

```python
from orcapod.extension_types.directory_type import Directory
```

And add `"Directory"` to `__all__`:
```python
    # Stable type aliases
    "Directory",
    "File",
    "Path",
    "UPath",
    "UUID",
```

Also update the docstring comment above the stable type aliases block:

```python
# Stable type aliases — preferred over importing directly from pathlib/upath/uuid.
#
# These aliases are the recommended way to reference these types in orcapod user code.
# Even if an upstream library is renamed or restructured, these symbols remain stable
# at ``orcapod.Path``, ``orcapod.UPath``, ``orcapod.UUID``, ``orcapod.File``, and
# ``orcapod.Directory``. Their Arrow extension types are registered under the
# ``orcapod.*`` namespace (``"orcapod.path"``, ``"orcapod.upath"``, ``"orcapod.uuid"``,
# ``"orcapod.file"``, ``"orcapod.directory"``), so on-disk identity is also decoupled
# from upstream module paths.
```

- [ ] **Step 3: Export new symbols from `hashing/__init__.py`**

In `src/orcapod/hashing/__init__.py`:

Add imports:
```python
from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    DirectoryHandler,
    FileHandler,
    FunctionHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_python_type_handlers,
)
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    ContentIdentifiableProtocol,
    DirectoryHasherProtocol,
    FileContentHasherProtocol,
    FunctionInfoExtractorProtocol,
    PythonTypeHandlerProtocol,
    SemanticHasherProtocol,
    SemanticTypeHasherProtocol,
    StringCacherProtocol,
)
```

Update the module docstring to include the three new exports. The existing lines stay unchanged; add the three new lines shown below to the appropriate sections:

```python
"""
OrcaPod hashing package.

Public API
----------
  SemanticAwarePythonHasher            -- content-based recursive object hasher
  SemanticHasherProtocol               -- protocol for semantic hashers
  PythonTypeHandlerRegistry            -- registry mapping types to PythonTypeHandlerProtocol instances
  get_default_semantic_hasher          -- global default SemanticHasherProtocol factory
  get_default_python_type_handler_registry -- global default registry factory
  ContentIdentifiableMixin             -- convenience mixin for content-identifiable objects

Built-in hashers (importable for custom registry setup):
  UUIDHandler
  BytesHandler
  FunctionHandler
  TypeObjectHandler
  FileHandler
  DirectoryHandler                     -- built-in handler for orcapod.Directory
  register_builtin_python_type_handlers

Utility:
  FileContentHasherProtocol
  StringCacherProtocol
  FunctionInfoExtractorProtocol
  ArrowHasherProtocol
  BasicFileHasher
  CachedFileHasher
  BasicDirectoryHasher                 -- recursive Merkle tree directory hasher
  DirectoryHasherProtocol              -- protocol for directory hashers
  hash_file
  get_default_arrow_hasher
"""
```

Add to `__all__`:
```python
    "DirectoryHandler",
    "BasicDirectoryHasher",
    "DirectoryHasherProtocol",
```

- [ ] **Step 4: Export `DirectoryHandler` from `hashing/semantic_hashing/__init__.py`**

In `src/orcapod/hashing/semantic_hashing/__init__.py`:

Update the `from orcapod.hashing.semantic_hashing.builtin_handlers import (...)` block to add `DirectoryHandler`:

```python
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    DirectoryHandler,
    FileHandler,
    FunctionHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_python_type_handlers,
)
```

Add `"DirectoryHandler"` to `__all__`:
```python
    "DirectoryHandler",
```

Also update the module docstring to include:
```
  DirectoryHandler     -- orcapod.Directory → recursive Merkle tree hash
```

- [ ] **Step 5: Update `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, make these four changes:

**5a.** Add `directory_hasher` top-level entry (after `file_hasher`):
```json
"directory_hasher": {
    "_class": "orcapod.hashing.directory_hashers.BasicDirectoryHasher",
    "_config": {
        "algorithm": "sha256"
    }
},
```

**5b.** Add `LogicalDirectory` to the `logical_types` list (after `LogicalFile`):
```json
{
    "_class": "orcapod.extension_types.directory_type.LogicalDirectory",
    "_config": {}
}
```

**5c.** Add `Directory → DirectoryHandler` to the `handlers` list (after the `File` entry):
```json
[{"_type": "orcapod.extension_types.directory_type.Directory"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.DirectoryHandler", "_config": {"directory_hasher": {"_ref": "directory_hasher"}}}],
```

**5d.** Add two entries to the `changelog` array:
```json
"Migrated LogicalFile Arrow storage from plain path string to JSON {\"path\": \"...\"} for consistency with LogicalDirectory (ITL-451)",
"Added orcapod.Directory content-identified type with recursive Merkle tree hashing, LogicalDirectory Arrow extension, ignore filter support, and DirectoryHandler (ITL-451)"
```

- [ ] **Step 6: Verify all imports work**

```bash
uv run python -c "
import orcapod
print('Directory:', orcapod.Directory)
from orcapod.extension_types import LogicalDirectory
print('LogicalDirectory:', LogicalDirectory)
from orcapod.hashing import BasicDirectoryHasher, DirectoryHandler, DirectoryHasherProtocol
print('BasicDirectoryHasher:', BasicDirectoryHasher)
print('DirectoryHandler:', DirectoryHandler)
print('DirectoryHasherProtocol:', DirectoryHasherProtocol)
print('All imports OK')
"
```

Expected: All lines print without error.

- [ ] **Step 7: Commit**

```bash
git add \
  src/orcapod/extension_types/__init__.py \
  src/orcapod/__init__.py \
  src/orcapod/hashing/__init__.py \
  src/orcapod/hashing/semantic_hashing/__init__.py \
  src/orcapod/contexts/data/v0.1.json
git commit -m "feat(types): wire up Directory/LogicalDirectory exports and v0.1.json registration (ITL-451)"
```

---

## Task 7: Run full test suite and verify

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: All tests PASS. If any failures, fix them before creating the PR.

- [ ] **Step 2: Run the specific new test files in isolation**

```bash
uv run pytest tests/test_extension_types/test_directory_type.py tests/test_hashing/test_directory_handler.py tests/test_extension_types/test_file_type.py -v
```

Expected: All PASS.

- [ ] **Step 3: Smoke test the end-to-end flow**

```bash
uv run python -c "
import tempfile, pathlib, orcapod

# Create a temp directory with some files
with tempfile.TemporaryDirectory() as tmp:
    p = pathlib.Path(tmp)
    (p / 'a.txt').write_bytes(b'hello')
    (p / 'sub').mkdir()
    (p / 'sub' / 'b.txt').write_bytes(b'world')

    # Construct Directory
    d = orcapod.Directory(tmp)
    print('Directory:', str(d))

    # Hash it
    from orcapod.hashing import BasicDirectoryHasher
    hasher = BasicDirectoryHasher()
    h = hasher.hash_directory(tmp)
    print('Hash method:', h.method)
    print('Hash (hex):', h.digest.hex()[:16], '...')

    # Arrow roundtrip
    from orcapod.extension_types.directory_type import LogicalDirectory
    import json
    lt = LogicalDirectory()
    storage = lt.python_to_storage(d)
    data = json.loads(storage)
    print('Storage path:', data['path'])
    recovered = lt.storage_to_python(storage)
    print('Recovered:', str(recovered))
    print('Roundtrip OK:', str(recovered) == str(d))
"
```

Expected output (paths will vary):
```
Directory: /tmp/...
Hash method: merkle_sha256
Hash (hex): <16 hex chars> ...
Storage path: /tmp/...
Recovered: /tmp/...
Roundtrip OK: True
```

If all tests pass and the smoke test succeeds, the implementation is complete. Proceed to create a PR.
