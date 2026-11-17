# op.File Type and pathlib.Path Pure-Path Refactor — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce `orcapod.File` as a content-identified file type backed by `ProxyUPath`, and remove content-hashing from `pathlib.Path` and `upath.UPath` so they hash from their path string.

**Architecture:** `File` subclasses `upath.extensions.ProxyUPath` and validates existence on construction. A new `LogicalFile` Arrow extension type serialises `File` as a path string (`"orcapod.file"`). A new `FileHandler` replaces the removed `PathHandler` and `UPathHandler` in the semantic hasher registry.

**Tech Stack:** Python 3.12, `upath >= 0.3.8`, `pyarrow >= 20`, `polars >= 1.36`, `uv run pytest` for tests.

**Spec:** `superpowers/specs/2026-06-29-op-file-type-design.md`

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| **Create** | `src/orcapod/extension_types/file_type.py` | `File` class + `LogicalFile` Arrow extension |
| **Modify** | `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Add `FileHandler`; delete `PathHandler`, `UPathHandler` |
| **Modify** | `src/orcapod/hashing/semantic_hashing/__init__.py` | Swap `PathHandler`/`UPathHandler` for `FileHandler` in exports |
| **Modify** | `src/orcapod/extension_types/__init__.py` | Add `LogicalFile` to exports |
| **Modify** | `src/orcapod/contexts/data/v0.1.json` | Add `LogicalFile`; remove Path/UPath handlers; add `File` handler |
| **Modify** | `src/orcapod/__init__.py` | Add `orcapod.File` stable export |
| **Create** | `tests/test_extension_types/test_file_type.py` | Tests for `File` and `LogicalFile` |
| **Create** | `tests/test_hashing/test_file_handler.py` | Tests for `FileHandler` |
| **Modify** | `tests/test_hashing/test_extension_type_hashing.py` | Port Path tests → File; add no-read test for Path |
| **Modify** | `tests/test_hashing/test_semantic_hasher.py` | Remove `PathHandler`-dependent tests |
| **Modify** | `CHANGELOG.md` | Breaking-change entry |

---

## Task 1: Create branch

**Files:** none (git only)

- [ ] **Step 1: Check out the feature branch**

```bash
git checkout -b eywalker/itl-450-add-opfile-type-and-refactor-pathlibpath-to-be-a-pure-path
git branch --show-current
```

Expected output: `eywalker/itl-450-add-opfile-type-and-refactor-pathlibpath-to-be-a-pure-path`

---

## Task 2: `File` class (existence-validated ProxyUPath)

**Files:**
- Create: `src/orcapod/extension_types/file_type.py`
- Create: `tests/test_extension_types/test_file_type.py`

- [ ] **Step 1: Write the failing tests for `File`**

Create `tests/test_extension_types/test_file_type.py`:

```python
"""Tests for orcapod.extension_types.file_type.File."""

from __future__ import annotations

import os
import pytest

from orcapod.extension_types.file_type import File


class TestFileConstructor:
    def test_rejects_nonexistent_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            File(tmp_path / "does_not_exist.txt")

    def test_rejects_directory(self, tmp_path):
        with pytest.raises(IsADirectoryError):
            File(tmp_path)

    def test_rejects_symlink_to_directory_when_following(self, tmp_path):
        target_dir = tmp_path / "real_dir"
        target_dir.mkdir()
        link = tmp_path / "link_to_dir"
        link.symlink_to(target_dir)
        with pytest.raises(IsADirectoryError):
            File(link)  # follow_symlinks=True default, target is a dir

    def test_rejects_symlink_when_follow_symlinks_false(self, tmp_path):
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        link = tmp_path / "link_to_file"
        link.symlink_to(real_file)
        with pytest.raises(ValueError, match="symlink"):
            File(link, follow_symlinks=False)

    def test_accepts_symlink_to_file_when_following(self, tmp_path):
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        link = tmp_path / "link_to_file"
        link.symlink_to(real_file)
        f = File(link)  # follow_symlinks=True default
        assert str(f) == str(link)

    def test_accepts_zero_byte_file(self, tmp_path):
        empty = tmp_path / "empty.txt"
        empty.write_bytes(b"")
        f = File(empty)
        assert str(f) == str(empty)

    def test_accepts_regular_file(self, tmp_path):
        regular = tmp_path / "regular.txt"
        regular.write_text("hello")
        f = File(regular)
        assert str(f) == str(regular)

    def test_str_returns_path_string(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert str(f) == str(p)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_file_type.py -v
```

Expected: `ModuleNotFoundError` or `ImportError` — `file_type` does not exist yet.

- [ ] **Step 3: Implement `File` in `src/orcapod/extension_types/file_type.py`**

Create `src/orcapod/extension_types/file_type.py`:

```python
"""orcapod.File — content-identified, existence-validated file path.

``File`` wraps a ``upath.UPath`` and validates that the path points to a readable,
non-directory file at construction time. Use ``pathlib.Path`` / ``upath.UPath`` for
paths that may not yet exist.

``LogicalFile`` is the Arrow extension type that serialises ``File`` instances as
``large_string`` columns tagged with the ``"orcapod.file"`` extension name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa
from upath import UPath
from upath.extensions import ProxyUPath

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class File(ProxyUPath):
    """A content-identified, existence-validated file path.

    Wraps a ``UPath`` and validates that the path refers to a readable, non-directory
    file at construction time. Works across local, S3, GCS, and any other
    fsspec-backed backend supported by ``upath``.

    Note:
        ``isinstance(file_instance, UPath)`` returns ``False`` because ``ProxyUPath``
        does not inherit from ``UPath``. Use ``isinstance(x, File)`` to type-check.

    Args:
        *args: Positional path arguments forwarded to ``UPath``.
        follow_symlinks: If ``True`` (default), symlinks are followed and the
            resolved target is validated and hashed. If ``False``, raises
            ``ValueError`` on construction when the path is a symlink.
        **kwargs: Keyword arguments forwarded to ``UPath``.

    Raises:
        FileNotFoundError: If the path does not exist.
        IsADirectoryError: If the path is a directory (or a symlink to one when
            ``follow_symlinks=True``).
        ValueError: If the path is a symlink and ``follow_symlinks=False``, or if
            the path exists but is not a regular file.

    Example:
        >>> f = File("/tmp/data.csv")
        >>> str(f)
        '/tmp/data.csv'
        >>> File("/tmp/nonexistent.csv")
        FileNotFoundError: ...
    """

    def __init__(self, *args: Any, follow_symlinks: bool = True, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if not follow_symlinks and self.__wrapped__.is_symlink():
            raise ValueError(
                f"File: path is a symlink and follow_symlinks=False: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.exists():
            raise FileNotFoundError(
                f"File: path does not exist: {self.__wrapped__!r}"
            )
        if self.__wrapped__.is_dir():
            raise IsADirectoryError(
                f"File: path is a directory: {self.__wrapped__!r}"
            )
        if not self.__wrapped__.is_file():
            raise ValueError(
                f"File: path is not a regular file: {self.__wrapped__!r}"
            )

    @classmethod
    def _from_upath(cls, upath: UPath) -> "File":
        """Create a ``File`` from an existing ``UPath`` without validation.

        Used internally by ``ProxyUPath`` for derived paths (e.g. ``.parent``,
        ``/`` operator). Validation is intentionally skipped — derived paths from
        navigation may not exist yet. ``follow_symlinks`` defaults to ``True`` on
        all derived instances.
        """
        obj = object.__new__(cls)
        obj.__wrapped__ = upath
        return obj
```

- [ ] **Step 4: Run the tests**

```bash
uv run pytest tests/test_extension_types/test_file_type.py -v
```

Expected: All 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/file_type.py tests/test_extension_types/test_file_type.py
git commit -m "feat(types): add orcapod.File existence-validated ProxyUPath type (ITL-450)"
```

---

## Task 3: `LogicalFile` Arrow extension type

**Files:**
- Modify: `src/orcapod/extension_types/file_type.py`
- Modify: `tests/test_extension_types/test_file_type.py`

- [ ] **Step 1: Write the failing tests for `LogicalFile`**

Append to `tests/test_extension_types/test_file_type.py`:

```python
import pyarrow as pa
from orcapod.extension_types.file_type import File, LogicalFile


class TestLogicalFile:
    def test_logical_type_name(self):
        lt = LogicalFile()
        assert lt.logical_type_name == "orcapod.file"

    def test_python_type(self):
        lt = LogicalFile()
        assert lt.python_type is File

    def test_arrow_ext_name(self):
        lt = LogicalFile()
        assert lt.get_arrow_extension_type().extension_name == "orcapod.file"

    def test_arrow_ext_storage_type(self):
        lt = LogicalFile()
        assert lt.get_arrow_extension_type().storage_type == pa.large_string()

    def test_python_to_storage_returns_string(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("x")
        f = File(p)
        lt = LogicalFile()
        result = lt.python_to_storage(f)
        assert result == str(p)
        assert isinstance(result, str)

    def test_storage_to_python_returns_file(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("x")
        lt = LogicalFile()
        result = lt.storage_to_python(str(p))
        assert isinstance(result, File)
        assert str(result) == str(p)

    def test_round_trip_preserves_path(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("round trip")
        f = File(p)
        lt = LogicalFile()
        storage = lt.python_to_storage(f)
        recovered = lt.storage_to_python(storage)
        assert str(recovered) == str(f)

    def test_storage_to_python_raises_if_file_missing(self, tmp_path):
        lt = LogicalFile()
        with pytest.raises(FileNotFoundError):
            lt.storage_to_python(str(tmp_path / "gone.txt"))

    def test_arrow_extension_type_is_cached(self):
        lt = LogicalFile()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_extension_types/test_file_type.py::TestLogicalFile -v
```

Expected: `ImportError` — `LogicalFile` not defined yet.

- [ ] **Step 3: Implement `LogicalFile` in `src/orcapod/extension_types/file_type.py`**

Append to `file_type.py` (after the `File` class):

```python
class LogicalFile(BaseLogicalType):
    """Logical type for ``orcapod.File``.

    Stores ``File`` instances as Arrow large strings using the custom extension
    type ``"orcapod.file"``. The stored value is the path string (e.g.
    ``"/tmp/data.csv"`` or ``"s3://bucket/key"``).

    On read (``storage_to_python``), the path is used to reconstruct a ``File``
    instance, which re-validates existence. Reading an Arrow table with
    ``"orcapod.file"`` columns will raise ``FileNotFoundError`` if the underlying
    files have been moved or deleted — this is the correct semantic for a
    content-identified type.

    Example:
        >>> import tempfile, pathlib
        >>> lt = LogicalFile()
        >>> with tempfile.NamedTemporaryFile(delete=False) as f:
        ...     _ = f.write(b"hello")
        ...     tmp = f.name
        >>> file = File(tmp)
        >>> lt.storage_to_python(lt.python_to_storage(file)) == file
        True
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.file", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.file", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.file"
    python_type: type = File

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``File``.

        Returns:
            A cached ``pa.ExtensionType`` with extension name ``"orcapod.file"``
            and storage type ``pa.large_string()``.
        """
        if LogicalFile._arrow_ext is None:
            LogicalFile._arrow_ext = LogicalFile._arrow_ext_class()
        return LogicalFile._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``File``.

        Returns:
            A cached ``pl.BaseExtension`` registered under ``"orcapod.file"``.
        """
        if LogicalFile._polars_ext is None:
            LogicalFile._polars_ext = LogicalFile._polars_ext_class()
        return LogicalFile._polars_ext

    def python_to_storage(self, value: Any, converter: "TypeConverterProtocol | None" = None) -> str:
        """Convert a ``File`` to its string path representation.

        Args:
            value: A ``File`` instance.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The string form of the path (e.g. ``"/tmp/data.csv"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any, converter: "TypeConverterProtocol | None" = None) -> File:
        """Reconstruct a ``File`` from its stored string path.

        Re-validates existence on read — raises ``FileNotFoundError`` if the file
        no longer exists at the stored path.

        Args:
            storage_value: A string path as stored in Arrow.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``File`` instance.

        Raises:
            FileNotFoundError: If the path no longer exists.
            IsADirectoryError: If the path is now a directory.
        """
        return File(storage_value)
```

- [ ] **Step 4: Run the tests**

```bash
uv run pytest tests/test_extension_types/test_file_type.py -v
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/file_type.py tests/test_extension_types/test_file_type.py
git commit -m "feat(types): add LogicalFile Arrow extension type for orcapod.File (ITL-450)"
```

---

## Task 4: `FileHandler` content hasher

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Create: `tests/test_hashing/test_file_handler.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_hashing/test_file_handler.py`:

```python
"""Tests for FileHandler — content hashing for orcapod.File."""

from __future__ import annotations

import hashlib
import pytest

from orcapod.extension_types.file_type import File
from orcapod.hashing.file_hashers import BasicFileHasher
from orcapod.hashing.semantic_hashing.builtin_handlers import FileHandler
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
from orcapod.types import ContentHash


@pytest.fixture
def file_hasher():
    return BasicFileHasher(algorithm="sha256")


@pytest.fixture
def handler(file_hasher):
    return FileHandler(file_hasher)


@pytest.fixture
def hasher(handler):
    registry = PythonTypeHandlerRegistry()
    registry.register(File, handler)
    return SemanticAwarePythonHasher(
        hasher_id="test_file_v0",
        type_handler_registry=registry,
    )


class TestFileHandler:
    def test_returns_content_hash(self, handler, hasher, tmp_path):
        p = tmp_path / "a.txt"
        p.write_text("hello")
        f = File(p)
        result = handler.handle(f, hasher)
        assert isinstance(result, ContentHash)

    def test_same_content_same_hash(self, handler, hasher, tmp_path):
        p1 = tmp_path / "a.txt"
        p2 = tmp_path / "b.txt"
        p1.write_bytes(b"identical")
        p2.write_bytes(b"identical")
        h1 = handler.handle(File(p1), hasher)
        h2 = handler.handle(File(p2), hasher)
        assert h1 == h2

    def test_different_content_different_hash(self, handler, hasher, tmp_path):
        p1 = tmp_path / "a.txt"
        p2 = tmp_path / "b.txt"
        p1.write_bytes(b"content A")
        p2.write_bytes(b"content B")
        h1 = handler.handle(File(p1), hasher)
        h2 = handler.handle(File(p2), hasher)
        assert h1 != h2

    def test_zero_byte_file_produces_hash(self, handler, hasher, tmp_path):
        p = tmp_path / "empty.txt"
        p.write_bytes(b"")
        f = File(p)
        result = handler.handle(f, hasher)
        assert isinstance(result, ContentHash)

    def test_zero_byte_file_hash_is_consistent(self, handler, hasher, tmp_path):
        p1 = tmp_path / "empty1.txt"
        p2 = tmp_path / "empty2.txt"
        p1.write_bytes(b"")
        p2.write_bytes(b"")
        h1 = handler.handle(File(p1), hasher)
        h2 = handler.handle(File(p2), hasher)
        assert h1 == h2

    def test_hash_matches_direct_sha256(self, handler, hasher, tmp_path):
        """FileHandler must produce the same digest as BasicFileHasher(sha256) directly."""
        content = b"migration compatibility check"
        p = tmp_path / "compat.txt"
        p.write_bytes(content)
        f = File(p)
        handler_result = handler.handle(f, hasher)
        direct_result = BasicFileHasher(algorithm="sha256").hash_file(p)
        assert handler_result == direct_result

    def test_rejects_non_file_object(self, handler, hasher):
        from pathlib import Path
        with pytest.raises(TypeError, match="FileHandler"):
            handler.handle(Path("/tmp"), hasher)
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_hashing/test_file_handler.py -v
```

Expected: `ImportError` — `FileHandler` not defined yet.

- [ ] **Step 3: Add `FileHandler` to `builtin_handlers.py`**

In `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`, add after the existing handler classes (before `register_builtin_python_type_handlers`):

```python
class FileHandler:
    """Hasher for ``orcapod.File`` objects — hashes file *content*.

    By the time ``handle`` is called, ``File``'s constructor has already validated
    that the path exists and is a non-directory file (and is not a symlink when
    ``follow_symlinks=False``). The hash is produced by reading file bytes through
    the wrapped ``UPath``, which follows symlinks by default.

    Args:
        file_hasher: Any object with a ``hash_file(path) -> ContentHash`` method.
    """

    def __init__(self, file_hasher: "FileContentHasherProtocol") -> None:
        self.file_hasher = file_hasher

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        # Deferred import breaks the circular dependency between this module and
        # file_type.py — the same pattern used by ArrowTableHandler.
        from orcapod.extension_types.file_type import File
        if not isinstance(obj, File):
            raise TypeError(
                f"FileHandler: expected an orcapod.File, got {type(obj)!r}"
            )
        logger.debug("FileHandler: hashing file content at %s", obj.__wrapped__)
        return self.file_hasher.hash_file(obj.__wrapped__)
```

- [ ] **Step 4: Run the tests**

```bash
uv run pytest tests/test_hashing/test_file_handler.py -v
```

Expected: All 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py tests/test_hashing/test_file_handler.py
git commit -m "feat(hashing): add FileHandler for orcapod.File content hashing (ITL-450)"
```

---

## Task 5: Remove `PathHandler` and `UPathHandler`

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Modify: `tests/test_hashing/test_extension_type_hashing.py`
- Modify: `tests/test_hashing/test_semantic_hasher.py`

- [ ] **Step 1: Add the no-read test for `Path` columns**

Open `tests/test_hashing/test_extension_type_hashing.py`. Append a new test class at the end of the file:

```python
class TestPathColumnNoContentRead:
    """After handler removal, hashing a Path column must not read file content."""

    def test_path_column_hashing_does_not_read_file(self, ctx, tmp_path):
        """Hashing a Path pointing at a nonexistent location raises no error.

        This verifies that PathHandler has been removed and Path columns hash
        from their Arrow string content (the path string) without any file I/O.
        """
        from pathlib import Path
        from orcapod.hashing.visitors import SemanticHashingVisitor

        # Point at a path that definitely does not exist
        nonexistent = tmp_path / "will_never_exist_abc123.txt"
        assert not nonexistent.exists()

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage_val = ctx.type_converter.python_to_storage(nonexistent, Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        # Must not raise — no file read should occur
        new_type, new_data = visitor.visit(arrow_type, storage_val)

        # Result is the raw Arrow storage (path string) — no content hash substitution
        assert new_type == arrow_type
        assert new_data == storage_val
```

- [ ] **Step 2: Run the new test to confirm it currently fails**

```bash
uv run pytest tests/test_hashing/test_extension_type_hashing.py::TestPathColumnNoContentRead -v
```

Expected: `FAIL` — currently `PathHandler` reads the file, so hashing a nonexistent path raises `FileNotFoundError`.

- [ ] **Step 3: Remove `PathHandler` and `UPathHandler` from `builtin_handlers.py`**

In `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`:

1. Delete the `PathHandler` class entirely (lines defining `class PathHandler:` through its closing).
2. Delete the `UPathHandler` class entirely.
3. Remove these two lines from `register_builtin_python_type_handlers`:
   ```python
   registry.register(Path, PathHandler(file_hasher))
   registry.register(UPath, UPathHandler(file_hasher))
   ```
4. Add the `File` registration in their place (after the `bytes`/`bytearray` registrations):
   ```python
   from orcapod.extension_types.file_type import File
   registry.register(File, FileHandler(file_hasher))
   ```
5. Remove the unused imports at the top of the file:
   - `from pathlib import Path`  ← remove (no longer used in this module)
   - `from upath import UPath`   ← remove (no longer used in this module)

The module-level imports section should now look like:
```python
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from orcapod.types import ContentHash, PathLike, Schema

if TYPE_CHECKING:
    from orcapod.protocols.hashing_protocols import (
        ArrowHasherProtocol,
        FileContentHasherProtocol,
        HandlerRegistryProtocol,
        SemanticHasherProtocol,
    )
```

- [ ] **Step 4: Remove `PathHandler`-dependent tests from `test_semantic_hasher.py`**

Search `tests/test_hashing/test_semantic_hasher.py` for any test that imports or directly uses `PathHandler` or `UPathHandler`. Remove those tests (or the `PathHandler`/`UPathHandler`-specific portions). Tests that use `Path` only for incidental file creation (not for testing content hashing via `PathHandler`) can stay.

Run to find what needs removing:

```bash
grep -n "PathHandler\|UPathHandler\|hash_object.*Path\|Path.*hash_object" tests/test_hashing/test_semantic_hasher.py
```

Delete or rewrite any test that asserts `Path` objects are content-hashed. Leave tests that use `tmp_path` only for creating real files passed to `File` or other handlers.

- [ ] **Step 5: Port existing Path-column content-hash tests to use `File`**

In `tests/test_hashing/test_extension_type_hashing.py`, find every test in `TestSemanticHashingVisitorExtension` and `TestCrossPathConsistency` that uses `Path` as the type for content-hashing assertions. Replace `Path` with `File`:

```python
# Before:
arrow_type = ctx.type_converter.register_python_class(Path)
storage_val = ctx.type_converter.python_to_storage(Path(file), Path)

# After:
from orcapod.extension_types.file_type import File
arrow_type = ctx.type_converter.register_python_class(File)
storage_val = ctx.type_converter.python_to_storage(File(file), File)
```

Also update the `test_binary_encoding_format` assertion — the type prefix will now be `b"orcapod:file"` instead of `b"orcapod:path"`:

```python
# Before:
assert type_prefix == b"orcapod:path"

# After:
assert type_prefix == b"orcapod:file"
```

- [ ] **Step 6: Run the full test suite**

```bash
uv run pytest tests/test_hashing/ tests/test_extension_types/ -v
```

Expected: All tests pass. The `TestPathColumnNoContentRead` test that was failing in Step 2 now passes.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py \
        tests/test_hashing/test_extension_type_hashing.py \
        tests/test_hashing/test_semantic_hasher.py
git commit -m "refactor(hashing): remove PathHandler/UPathHandler; Path and UPath now string-hash (ITL-450)"
```

---

## Task 6: Update `v0.1.json` configuration

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Edit `v0.1.json`**

Open `src/orcapod/contexts/data/v0.1.json` and make three changes:

**Change 1:** In the `"logical_types"` array, add `LogicalFile` after `LogicalUUID`:

```json
{
    "_class": "orcapod.extension_types.file_type.LogicalFile",
    "_config": {}
}
```

**Change 2:** In the `"handlers"` array, remove these two entries:

```json
[{"_type": "pathlib.Path"},     {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PathHandler",  "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
[{"_type": "upath.core.UPath"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UPathHandler", "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
```

**Change 3:** In the `"handlers"` array, add the `File` handler entry (after the `bytearray` entry):

```json
[{"_type": "orcapod.extension_types.file_type.File"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FileHandler", "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
```

- [ ] **Step 2: Run the integration tests that exercise the full context**

```bash
uv run pytest tests/ -v -k "context or default_context or data_context or v0.1"
```

Expected: All pass. The context loads correctly with the new config.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json
git commit -m "feat(config): register LogicalFile and FileHandler; remove Path/UPath handlers in v0.1.json (ITL-450)"
```

---

## Task 7: Update exports and public API

**Files:**
- Modify: `src/orcapod/__init__.py`
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `src/orcapod/hashing/semantic_hashing/__init__.py`

- [ ] **Step 1: Add `File` to `src/orcapod/__init__.py`**

Open `src/orcapod/__init__.py`. Add the import alongside the existing stable type aliases:

```python
# Before the existing aliases:
from pathlib import Path
from upath import UPath
from uuid import UUID

# Add:
from orcapod.extension_types.file_type import File
```

Add `"File"` to `__all__`:

```python
__all__ = [
    # ... existing entries ...
    # Stable type aliases
    "File",   # ← add this
    "Path",
    "UPath",
    "UUID",
]
```

- [ ] **Step 2: Add `LogicalFile` to `src/orcapod/extension_types/__init__.py`**

In `src/orcapod/extension_types/__init__.py`, add the import and `__all__` entry. Add after the existing `DataclassLogicalTypeFactory` / `PydanticLogicalTypeFactory` imports:

```python
from .file_type import File, LogicalFile
```

Add to `__all__`:

```python
# add after the PYDANTIC_CATEGORY block:
"File",
"LogicalFile",
```

- [ ] **Step 3: Update `src/orcapod/hashing/semantic_hashing/__init__.py`**

In the imports block, replace:

```python
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    FunctionHandler,
    PathHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_python_type_handlers,
)
```

with:

```python
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    BytesHandler,
    FileHandler,
    FunctionHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_python_type_handlers,
)
```

Update `__all__` — replace `"PathHandler"` with `"FileHandler"` and remove `"UPathHandler"` (it was never explicitly listed but verify):

```python
__all__ = [
    "SemanticAwarePythonHasher",
    "PythonTypeHandlerRegistry",
    "BuiltinPythonTypeHandlerRegistry",
    "ContentIdentifiableMixin",
    "FileHandler",   # ← replaces PathHandler
    "UUIDHandler",
    "BytesHandler",
    "FunctionHandler",
    "TypeObjectHandler",
    "register_builtin_python_type_handlers",
    "FunctionNameExtractor",
    "FunctionSignatureExtractor",
    "FunctionInfoExtractorFactory",
]
```

- [ ] **Step 4: Verify the public API smoke test**

```bash
uv run python -c "
import orcapod
print('File:', orcapod.File)
print('Path:', orcapod.Path)
print('UPath:', orcapod.UPath)
print('UUID:', orcapod.UUID)
from orcapod.extension_types import LogicalFile
print('LogicalFile:', LogicalFile)
from orcapod.hashing.semantic_hashing import FileHandler
print('FileHandler:', FileHandler)
"
```

Expected output (no errors):
```
File: <class 'orcapod.extension_types.file_type.File'>
Path: <class 'pathlib.PosixPath'>
UPath: <class 'upath.core.UPath'>
UUID: <class 'uuid.UUID'>
LogicalFile: <class 'orcapod.extension_types.file_type.LogicalFile'>
FileHandler: <class 'orcapod.hashing.semantic_hashing.builtin_handlers.FileHandler'>
```

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/__init__.py \
        src/orcapod/extension_types/__init__.py \
        src/orcapod/hashing/semantic_hashing/__init__.py
git commit -m "feat(api): export orcapod.File, LogicalFile, FileHandler; remove PathHandler/UPathHandler exports (ITL-450)"
```

---

## Task 8: CHANGELOG entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add the entry to `CHANGELOG.md`**

Open `CHANGELOG.md` and add a new section at the top (or under the current `Unreleased` section if one exists):

```markdown
## Unreleased

### Added

- `orcapod.File` — a content-identified, existence-validated file path type backed by
  `upath.extensions.ProxyUPath`. Works with local, S3, GCS, and any other fsspec backend.
  Constructed with `File(path)` (or `File(path, follow_symlinks=False)` to reject symlinks).
  Hashes from file content (SHA-256); zero-byte files are valid.

### Changed

- **BREAKING:** `pathlib.Path` and `upath.UPath` columns no longer content-hash. They
  now hash from the Arrow string content (the path string itself), with no file I/O.
  This is identical to how `uuid.UUID` columns are hashed. Existing pipelines that
  relied on `pathlib.Path` or `upath.UPath` for content-identified file columns **must
  migrate to `orcapod.File`** — pipeline hashes will change otherwise.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add orcapod.File and breaking Path/UPath hash change (ITL-450)"
```

---

## Task 9: Final verification and push

- [ ] **Step 1: Run the complete test suite one final time**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: All tests pass, no warnings about removed symbols.

- [ ] **Step 2: Verify no stale references to `PathHandler` or `UPathHandler`**

```bash
grep -rn "PathHandler\|UPathHandler" src/ tests/
```

Expected: No matches (except possibly in comments or the CHANGELOG if mentioned by name).

- [ ] **Step 3: Push the branch**

```bash
git push -u origin eywalker/itl-450-add-opfile-type-and-refactor-pathlibpath-to-be-a-pure-path
```

- [ ] **Step 4: Create the PR**

```bash
gh pr create \
  --base main \
  --title "feat(types): add op.File and make pathlib.Path/UPath pure-path types (ITL-450)" \
  --body "$(cat <<'EOF'
## Summary

- Introduces `orcapod.File` as a content-identified, existence-validated file type backed by `upath.extensions.ProxyUPath`
- Adds `LogicalFile` Arrow extension type (`"orcapod.file"`, `large_string()` storage)
- Adds `FileHandler` for content hashing; removes `PathHandler` and `UPathHandler`
- `pathlib.Path` and `upath.UPath` now hash from path string (no file I/O) — **breaking change**

Closes ITL-450

## Test plan

- [ ] `tests/test_extension_types/test_file_type.py` — File constructor, LogicalFile roundtrip
- [ ] `tests/test_hashing/test_file_handler.py` — FileHandler content hash correctness
- [ ] `tests/test_hashing/test_extension_type_hashing.py` — Path no-read verification, File column hashing
- [ ] Full suite: `uv run pytest tests/ -v`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| `op.File` as `ProxyUPath` subclass with constructor validation | Task 2 |
| `follow_symlinks` parameter, default True, raises on False+symlink | Task 2 |
| `_from_upath` override skips validation | Task 2 |
| `LogicalFile` with `"orcapod.file"` extension, `large_string()` storage | Task 3 |
| `python_to_storage` → path string | Task 3 |
| `storage_to_python` → `File(...)` re-validates | Task 3 |
| `FileHandler` with deferred import | Task 4 |
| `FileHandler` hashes via `file_hasher.hash_file(obj.__wrapped__)` | Task 4 |
| Remove `PathHandler` and `UPathHandler` | Task 5 |
| `Path` and `UPath` fall through to Arrow-content hashing | Task 5 |
| Update `v0.1.json` | Task 6 |
| `orcapod.File` export | Task 7 |
| `LogicalFile`, `FileHandler` exports | Task 7 |
| Remove `PathHandler`/`UPathHandler` exports | Task 7 |
| Constructor rejects nonexistent / dir / symlink (tests) | Task 2 |
| Zero-byte file hash (tests) | Task 4 |
| Hash matches `BasicFileHasher(sha256)` directly (tests) | Task 4 |
| Path column no-read test | Task 5 |
| CHANGELOG breaking-change note | Task 8 |

All spec requirements covered. ✓
