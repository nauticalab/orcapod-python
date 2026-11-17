# PathLike Conformance for op.File and op.Directory — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `op.File` and `op.Directory` pass `isinstance(x, os.PathLike)` and work with `os.fspath()`, `open()`, and `pathlib.Path()` for local-backed instances.

**Architecture:** Add `__fspath__(self) -> str` to both `File` and `Directory`; the method delegates to `os.fspath(self.__wrapped__)`. The `os.PathLike` ABC's `__subclasshook__` picks up `__fspath__` automatically — no explicit base-class change is needed. Remote-backed instances raise `TypeError` on `os.fspath()`, mirroring `UPath`'s own behaviour.

**Tech Stack:** Python `os.PathLike` ABC, `upath.extensions.ProxyUPath`, `pytest`.

---

## File map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/extension_types/file_type.py` | Modify | Add `import os`; add `File.__fspath__` |
| `src/orcapod/extension_types/directory_type.py` | Modify | Add `import os`; add `Directory.__fspath__` |
| `tests/test_extension_types/test_file_type.py` | Modify | Add `TestFilePathLike` class |
| `tests/test_extension_types/test_directory_type.py` | Modify | Add `TestDirectoryPathLike` class |

---

## Task 0: Checkout feature branch

- [ ] **Step 1: Create and switch to the feature branch**

```bash
git checkout -b eywalker/itl-493-make-opfile-opdirectory-subclass-ospathlike
```

Expected: `Switched to a new branch 'eywalker/itl-493-make-opfile-opdirectory-subclass-ospathlike'`

---

## Task 1: `File.__fspath__` with tests

**Files:**
- Modify: `tests/test_extension_types/test_file_type.py`
- Modify: `src/orcapod/extension_types/file_type.py`

- [ ] **Step 1: Add imports to the test file**

Open `tests/test_extension_types/test_file_type.py`. The current imports are:

```python
from __future__ import annotations

import json

import pytest
import pyarrow as pa

from orcapod.extension_types.file_type import File, LogicalFile
```

Replace with:

```python
from __future__ import annotations

import json
import os
import pathlib

import pytest
import pyarrow as pa
from upath import UPath

from orcapod.extension_types.file_type import File, LogicalFile
```

- [ ] **Step 2: Add the failing test class**

Append the following class to the end of `tests/test_extension_types/test_file_type.py`:

```python
class TestFilePathLike:
    def test_isinstance_pathlike(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert isinstance(f, os.PathLike)

    def test_fspath_returns_path_string(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert os.fspath(f) == str(p)

    def test_open_accepts_file(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("hello")
        f = File(p)
        with open(f) as fh:
            assert fh.read() == "hello"

    def test_pathlib_path_accepts_file(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert pathlib.Path(f) == p

    def test_remote_backed_fspath_raises(self):
        remote = File._from_upath(UPath("s3://bucket/key.csv"))
        with pytest.raises(TypeError):
            os.fspath(remote)

    def test_plain_proxy_upath_subclass_not_pathlike(self):
        from upath.extensions import ProxyUPath

        class _Stub(ProxyUPath):
            pass

        assert not issubclass(_Stub, os.PathLike)
```

- [ ] **Step 3: Run the tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_file_type.py::TestFilePathLike -v
```

Expected: all 6 tests FAIL. `test_isinstance_pathlike` should fail with `AssertionError`
(isinstance returns False). `test_open_accepts_file` and `test_pathlib_path_accepts_file`
should fail with `TypeError`. `test_plain_proxy_upath_subclass_not_pathlike` should PASS
(it is already true — `_Stub` has no `__fspath__`).

- [ ] **Step 4: Add `import os` to `file_type.py`**

Open `src/orcapod/extension_types/file_type.py`. The current imports start with:

```python
from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Self
```

Add `import os` so it reads:

```python
from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, Any, Self
```

- [ ] **Step 5: Add `__fspath__` to `File`**

In `src/orcapod/extension_types/file_type.py`, the `File` class ends at line 91 with
`_from_upath`. Insert `__fspath__` between `__init__` and `_from_upath`:

```python
    def __fspath__(self) -> str:
        """Return the file system path representation of the underlying ``UPath``.

        Succeeds for local-backed paths (``PosixUPath``, ``FilePath``) and returns
        the local path string. Raises ``TypeError`` for remote-backed paths (S3, GCS,
        engm, …), consistent with how ``UPath`` itself behaves for those backends.

        Returns:
            The local filesystem path as a string.

        Raises:
            TypeError: If the underlying path is remote-backed and not ``os.PathLike``.
        """
        return os.fspath(self.__wrapped__)
```

After the edit the relevant portion of `File` should look like:

```python
class File(ProxyUPath):
    ...

    def __init__(self, *args: Any, follow_symlinks: bool = True, **kwargs: Any) -> None:
        ...

    def __fspath__(self) -> str:
        """Return the file system path representation of the underlying ``UPath``.

        Succeeds for local-backed paths (``PosixUPath``, ``FilePath``) and returns
        the local path string. Raises ``TypeError`` for remote-backed paths (S3, GCS,
        engm, …), consistent with how ``UPath`` itself behaves for those backends.

        Returns:
            The local filesystem path as a string.

        Raises:
            TypeError: If the underlying path is remote-backed and not ``os.PathLike``.
        """
        return os.fspath(self.__wrapped__)

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        ...
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_file_type.py::TestFilePathLike -v
```

Expected: all 6 tests PASS.

- [ ] **Step 7: Run the full file test suite to check for regressions**

```bash
uv run pytest tests/test_extension_types/test_file_type.py -v
```

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/extension_types/file_type.py tests/test_extension_types/test_file_type.py
git commit -m "feat(extension_types): add __fspath__ to File for os.PathLike conformance (ITL-493)"
```

---

## Task 2: `Directory.__fspath__` with tests

**Files:**
- Modify: `tests/test_extension_types/test_directory_type.py`
- Modify: `src/orcapod/extension_types/directory_type.py`

- [ ] **Step 1: Add imports to the test file**

Open `tests/test_extension_types/test_directory_type.py`. Find the existing imports at the top
and add `import os`, `import pathlib`, and `from upath import UPath`:

```python
from __future__ import annotations

import json
import os
import pathlib

import pytest
import pyarrow as pa
from upath import UPath

from orcapod.extension_types.directory_type import Directory, LogicalDirectory
```

(Add only what is missing — do not duplicate existing imports.)

- [ ] **Step 2: Add the failing test class**

Append the following class to the end of `tests/test_extension_types/test_directory_type.py`:

```python
class TestDirectoryPathLike:
    def test_isinstance_pathlike(self, tmp_path):
        d = Directory(tmp_path)
        assert isinstance(d, os.PathLike)

    def test_fspath_returns_path_string(self, tmp_path):
        d = Directory(tmp_path)
        assert os.fspath(d) == str(tmp_path)

    def test_pathlib_path_accepts_directory(self, tmp_path):
        d = Directory(tmp_path)
        assert pathlib.Path(d) == tmp_path

    def test_remote_backed_fspath_raises(self):
        remote = Directory._from_upath(UPath("s3://bucket/prefix/"))
        with pytest.raises(TypeError):
            os.fspath(remote)
```

- [ ] **Step 3: Run the tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_directory_type.py::TestDirectoryPathLike -v
```

Expected: `test_isinstance_pathlike`, `test_fspath_returns_path_string`, and
`test_pathlib_path_accepts_directory` FAIL. `test_remote_backed_fspath_raises` should
also FAIL (TypeError is not raised yet — `Directory` has no `__fspath__` so `os.fspath`
raises for a different reason or succeeds unexpectedly).

- [ ] **Step 4: Add `import os` to `directory_type.py`**

Open `src/orcapod/extension_types/directory_type.py`. The current imports start with:

```python
from __future__ import annotations

import importlib
import json
import warnings
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, Self
```

Add `import os` so it reads:

```python
from __future__ import annotations

import importlib
import json
import os
import warnings
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, Self
```

- [ ] **Step 5: Add `__fspath__` to `Directory`**

In `src/orcapod/extension_types/directory_type.py`, the `Directory` class ends at line 101
with `_from_upath`. Insert `__fspath__` between `__init__` and `_from_upath`:

```python
    def __fspath__(self) -> str:
        """Return the file system path representation of the underlying ``UPath``.

        Succeeds for local-backed paths (``PosixUPath``, ``FilePath``) and returns
        the local path string. Raises ``TypeError`` for remote-backed paths (S3, GCS,
        engm, …), consistent with how ``UPath`` itself behaves for those backends.

        Returns:
            The local filesystem path as a string.

        Raises:
            TypeError: If the underlying path is remote-backed and not ``os.PathLike``.
        """
        return os.fspath(self.__wrapped__)
```

After the edit the relevant portion of `Directory` should look like:

```python
class Directory(ProxyUPath):
    ...

    def __init__(self, *args, ignore=None, **kwargs):
        ...

    def __fspath__(self) -> str:
        """Return the file system path representation of the underlying ``UPath``.

        Succeeds for local-backed paths (``PosixUPath``, ``FilePath``) and returns
        the local path string. Raises ``TypeError`` for remote-backed paths (S3, GCS,
        engm, …), consistent with how ``UPath`` itself behaves for those backends.

        Returns:
            The local filesystem path as a string.

        Raises:
            TypeError: If the underlying path is remote-backed and not ``os.PathLike``.
        """
        return os.fspath(self.__wrapped__)

    @classmethod
    def _from_upath(cls, upath: UPath, /) -> Self:
        ...
```

- [ ] **Step 6: Run the tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_directory_type.py::TestDirectoryPathLike -v
```

Expected: all 4 tests PASS.

- [ ] **Step 7: Run the full directory test suite to check for regressions**

```bash
uv run pytest tests/test_extension_types/test_directory_type.py -v
```

Expected: all tests PASS.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/extension_types/directory_type.py tests/test_extension_types/test_directory_type.py
git commit -m "feat(extension_types): add __fspath__ to Directory for os.PathLike conformance (ITL-493)"
```

---

## Task 3: Final verification and PR

- [ ] **Step 1: Run the full test suite**

```bash
uv run pytest tests/ -v
```

Expected: all tests PASS with no regressions.

- [ ] **Step 2: Smoke-check `isinstance` and `os.fspath` at the REPL**

```bash
uv run python -c "
import os, tempfile, pathlib
from orcapod.extension_types.file_type import File
from orcapod.extension_types.directory_type import Directory

with tempfile.TemporaryDirectory() as d:
    p = pathlib.Path(d) / 'test.txt'
    p.write_text('hi')
    f = File(p)
    print('File isinstance:', isinstance(f, os.PathLike))
    print('File os.fspath:', os.fspath(f))
    print('open(File):', open(f).read())
    print('pathlib.Path(File):', pathlib.Path(f))
    dr = Directory(d)
    print('Directory isinstance:', isinstance(dr, os.PathLike))
    print('Directory os.fspath:', os.fspath(dr))
    print('pathlib.Path(Directory):', pathlib.Path(dr))
"
```

Expected output:
```
File isinstance: True
File os.fspath: /tmp/<tmpdir>/test.txt
open(File): hi
pathlib.Path(File): /tmp/<tmpdir>/test.txt
Directory isinstance: True
Directory os.fspath: /tmp/<tmpdir>
pathlib.Path(Directory): /tmp/<tmpdir>
```

- [ ] **Step 3: Create PR against `main`**

```bash
git push -u origin eywalker/itl-493-make-opfile-opdirectory-subclass-ospathlike
gh pr create \
  --title "feat(extension_types): add os.PathLike conformance to op.File and op.Directory (ITL-493)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds `__fspath__` to `File` and `Directory`, delegating to `os.fspath(self.__wrapped__)`.
- `isinstance(f, os.PathLike)` now returns `True` for both classes via the `os.PathLike` ABC's `__subclasshook__`.
- Local-backed instances work with `open()`, `pathlib.Path()`, and `shutil` directly.
- Remote-backed instances (S3, GCS, engm, …) raise `TypeError` on `os.fspath()`, mirroring `UPath`'s own behaviour.
- `UPathProxy` is unchanged.

Closes ITL-493

## Test plan
- [ ] `uv run pytest tests/test_extension_types/test_file_type.py -v` — all pass
- [ ] `uv run pytest tests/test_extension_types/test_directory_type.py -v` — all pass
- [ ] `uv run pytest tests/ -v` — no regressions
EOF
)"
```
