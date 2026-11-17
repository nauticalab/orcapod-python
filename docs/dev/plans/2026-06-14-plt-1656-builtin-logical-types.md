# PLT-1656: Built-in LogicalType Implementations (Path, UPath, UUID) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement three built-in `LogicalType` classes (`LogicalPath`, `LogicalUPath`, `LogicalUUID`), wire them into `DataContext` via `v0.1.json`, and expose a `get_default_logical_type_registry()` convenience accessor.

**Architecture:** Each `LogicalType` owns its Arrow/Polars extension type instances via class-level caching. A new `make_polars_extension_type` helper (parallel to the existing `make_arrow_extension_type`) synthesises `pl.BaseExtension` subclasses at runtime. The registry is populated via the existing `parse_objectspec` JSON object spec mechanism so `LogicalTypeRegistry` gains a `logical_types` constructor param. The module-level `default_logical_type_registry` in `extension_types/__init__.py` is removed — the canonical access path becomes `get_default_context().logical_type_registry`.

**Tech Stack:** Python 3.12+, PyArrow ≥ 20, Polars ≥ 1.36.0, pytest, uv.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/registry.py` | Modify | Add `make_polars_extension_type` helper; add `logical_types` param to `LogicalTypeRegistry.__init__` |
| `src/orcapod/extension_types/__init__.py` | Modify | Export `make_polars_extension_type`; remove `default_logical_type_registry` |
| `src/orcapod/extension_types/builtin_logical_types.py` | **New** | `LogicalPath`, `LogicalUPath`, `LogicalUUID` implementations |
| `src/orcapod/contexts/core.py` | Modify | Add `logical_type_registry: LogicalTypeRegistry` field to `DataContext` |
| `src/orcapod/contexts/registry.py` | Modify | Add `"logical_type_registry"` to required fields; pass it through in `_create_context_from_spec` |
| `src/orcapod/contexts/data/v0.1.json` | Modify | Add `logical_type_registry` object spec entry |
| `src/orcapod/contexts/data/schemas/context_schema.json` | Modify | Add `logical_type_registry` to `required` and `properties` |
| `src/orcapod/contexts/__init__.py` | Modify | Add `get_default_logical_type_registry()` convenience function |
| `tests/test_extension_types/test_registry.py` | Modify | Add tests for `make_polars_extension_type` and `logical_types` param; remove stale `default_logical_type_registry` tests |
| `tests/test_extension_types/test_builtin_logical_types.py` | **New** | Protocol conformance, property values, round-trips, default-context integration tests |

---

### Task 1: `make_polars_extension_type` helper

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Write the failing tests**

Add these tests at the end of `tests/test_extension_types/test_registry.py`, before the `# default_logical_type_registry tests` section:

```python
# ---------------------------------------------------------------------------
# make_polars_extension_type tests
# ---------------------------------------------------------------------------

from orcapod.extension_types.registry import make_polars_extension_type


def test_make_polars_extension_type_returns_class():
    """make_polars_extension_type returns a pl.BaseExtension subclass."""
    cls = make_polars_extension_type("test.MakePolarsExt", pa.large_utf8())
    assert issubclass(cls, pl.BaseExtension)


def test_make_polars_extension_type_instance_has_correct_name():
    """Instantiating the returned class yields the correct ext_name."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.large_utf8())
    inst = cls()
    assert inst.ext_name() == name


def test_make_polars_extension_type_ext_from_params_returns_instance():
    """ext_from_params classmethod returns an instance of the class."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.large_utf8())
    inst = cls.ext_from_params(name, pl.String, None)
    assert isinstance(inst, cls)


def test_make_polars_extension_type_with_binary_storage():
    """make_polars_extension_type works with pa.binary(16) storage (UUID case)."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.binary(16), None)
    inst = cls()
    assert inst.ext_name() == name


def test_make_polars_extension_type_with_metadata():
    """make_polars_extension_type captures metadata in the class."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.large_utf8(), "test.metadata")
    # Instantiating should not raise; ext_name is correct.
    inst = cls()
    assert inst.ext_name() == name
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/fccdf92d-a25e-4477-ae00-a1ee2b6dc236/orcapod-python
uv run pytest tests/test_extension_types/test_registry.py::test_make_polars_extension_type_returns_class -v
```

Expected: `ImportError` — `make_polars_extension_type` does not exist yet.

- [ ] **Step 3: Implement `make_polars_extension_type` in `registry.py`**

Add after `make_arrow_extension_type` (around line 98), before the `LogicalTypeRegistry` class:

```python
def make_polars_extension_type(
    extension_name: str,
    arrow_storage_type: pa.DataType,
    metadata: str | None = None,
) -> type[pl.BaseExtension]:
    """Synthesise and return a ``pl.BaseExtension`` subclass.

    Derives the Polars storage dtype from *arrow_storage_type* via
    ``pl.from_arrow``. Returns the *class*; callers instantiate it inside
    ``get_polars_extension_type()``.

    The returned class uses the Arrow extension name as its registration name
    (the same name passed to ``pl.register_extension_type``), so that Polars
    correctly maps Arrow extension columns on read.

    Args:
        extension_name: The extension type name used for Polars registration.
            Must match the Arrow extension name so Polars can round-trip the
            type through Arrow IPC.
        arrow_storage_type: The Arrow storage type. Converted once to the
            corresponding Polars dtype via ``pl.from_arrow``.
        metadata: Optional metadata string stored as ``metadata_str`` in the
            Polars extension. Defaults to ``None``.

    Returns:
        A ``pl.BaseExtension`` subclass. Call it with no arguments to obtain
        an instance suitable for passing to ``pl.register_extension_type`` or
        returning from ``get_polars_extension_type()``.
    """
    _name = extension_name
    _polars_dtype = pl.from_arrow(pa.array([], type=arrow_storage_type)).dtype
    _metadata = metadata

    def __init__(self: pl.BaseExtension) -> None:
        pl.BaseExtension.__init__(self, _name, _polars_dtype, _metadata)

    @classmethod  # type: ignore[misc]
    def ext_from_params(
        cls: type[pl.BaseExtension],
        ext_name: str,
        storage_dtype: pl.PolarsDataType,
        metadata_str: str | None,
    ) -> pl.BaseExtension:
        return cls()

    return type(
        f"_PolarsExt_{_sanitize(extension_name)}",
        (pl.BaseExtension,),
        {
            "__init__": __init__,
            "ext_from_params": ext_from_params,
        },
    )
```

- [ ] **Step 4: Export `make_polars_extension_type` from `extension_types/__init__.py`**

In `src/orcapod/extension_types/__init__.py`, update the import line and `__all__`:

```python
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
```

And add `"make_polars_extension_type"` to `__all__`:

```python
__all__ = [
    "LogicalType",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "make_polars_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_registry.py -k "polars_extension_type" -v
```

Expected: All 5 new tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py \
        src/orcapod/extension_types/__init__.py \
        tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add make_polars_extension_type helper"
```

---

### Task 2: `LogicalTypeRegistry` `logical_types` constructor param

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Write the failing tests**

Add after the existing `test_get_by_arrow_extension_name_miss` test, before the PyArrow global registry tests section:

```python
# ---------------------------------------------------------------------------
# LogicalTypeRegistry constructor logical_types param tests
# ---------------------------------------------------------------------------

def test_registry_init_with_logical_types_preregisters():
    """LogicalTypeRegistry(logical_types=[lt]) makes the type immediately retrievable."""
    lt = _make_stub()
    registry = LogicalTypeRegistry(logical_types=[lt])
    assert registry.get_by_logical_name(lt.logical_type_name) is lt
    assert registry.get_by_python_type(lt.python_type) is lt
    assert registry.get_by_arrow_extension_name(lt.get_arrow_extension_type().extension_name) is lt


def test_registry_init_with_none_is_empty():
    """LogicalTypeRegistry(logical_types=None) starts empty without error."""
    registry = LogicalTypeRegistry(logical_types=None)
    assert registry.get_by_logical_name("anything") is None


def test_registry_init_with_empty_list_is_empty():
    """LogicalTypeRegistry(logical_types=[]) starts empty without error."""
    registry = LogicalTypeRegistry(logical_types=[])
    assert registry.get_by_logical_name("anything") is None


def test_registry_init_with_multiple_logical_types():
    """LogicalTypeRegistry(logical_types=[lt1, lt2]) registers both."""
    lt1 = _make_stub(py_type=int)
    lt2 = _make_stub(py_type=float)
    registry = LogicalTypeRegistry(logical_types=[lt1, lt2])
    assert registry.get_by_logical_name(lt1.logical_type_name) is lt1
    assert registry.get_by_logical_name(lt2.logical_type_name) is lt2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_registry.py::test_registry_init_with_logical_types_preregisters -v
```

Expected: FAIL — `LogicalTypeRegistry.__init__` does not accept `logical_types` argument.

- [ ] **Step 3: Update `LogicalTypeRegistry.__init__` in `registry.py`**

Replace the current `__init__` method (lines 121–124):

```python
# OLD
def __init__(self) -> None:
    self._by_logical_name: dict[str, LogicalType] = {}
    self._by_arrow_name: dict[str, LogicalType] = {}
    self._by_python_type: dict[type, LogicalType] = {}
```

With:

```python
def __init__(self, logical_types: list[LogicalType] | None = None) -> None:
    self._by_logical_name: dict[str, LogicalType] = {}
    self._by_arrow_name: dict[str, LogicalType] = {}
    self._by_python_type: dict[type, LogicalType] = {}
    for lt in (logical_types or []):
        self.register(lt)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_registry.py -k "registry_init" -v
```

Expected: All 4 new tests PASS. Also run the full registry suite to confirm no regressions:

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: All tests PASS (the last 6 `default_logical_type_registry` tests still reference the old module-level instance and will continue passing for now — they are removed in Task 6).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/registry.py \
        tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add logical_types constructor param to LogicalTypeRegistry"
```

---

### Task 3: `LogicalPath` and `LogicalUPath` implementations

**Files:**
- Create: `src/orcapod/extension_types/builtin_logical_types.py`
- Create: `tests/test_extension_types/test_builtin_logical_types.py`

- [ ] **Step 1: Create the test file with failing tests for `LogicalPath` and `LogicalUPath`**

Create `tests/test_extension_types/test_builtin_logical_types.py`:

```python
"""Tests for built-in LogicalType implementations (LogicalPath, LogicalUPath, LogicalUUID)."""

from __future__ import annotations

import pathlib
import uuid as uuid_module
import warnings

import polars as pl
import pyarrow as pa
import pytest
from upath import UPath

from orcapod.extension_types.protocols import LogicalType
from orcapod.extension_types.registry import LogicalTypeRegistry


# ---------------------------------------------------------------------------
# LogicalPath tests
# ---------------------------------------------------------------------------


def test_logical_path_isinstance_logical_type():
    """LogicalPath() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert isinstance(LogicalPath(), LogicalType)


def test_logical_path_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().logical_type_name == "pathlib.Path"


def test_logical_path_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().python_type is pathlib.Path


def test_logical_path_arrow_ext_name():
    """get_arrow_extension_type().extension_name is 'pathlib.Path'."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().get_arrow_extension_type().extension_name == "pathlib.Path"


def test_logical_path_arrow_ext_storage_type():
    """Arrow extension storage type is pa.large_string()."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().get_arrow_extension_type().storage_type == pa.large_string()


def test_logical_path_get_arrow_extension_type_is_cached():
    """get_arrow_extension_type() returns the same object on repeated calls."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_logical_path_get_polars_extension_type_is_cached():
    """get_polars_extension_type() returns the same object on repeated calls."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


def test_logical_path_round_trip():
    """Path -> python_to_storage -> storage_to_python -> Path is identity."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    p = pathlib.Path("/tmp/foo/bar.txt")
    assert lt.storage_to_python(lt.python_to_storage(p)) == p


def test_logical_path_python_to_storage_returns_string():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    result = lt.python_to_storage(pathlib.Path("/tmp/test"))
    assert isinstance(result, str)
    assert result == "/tmp/test"


# ---------------------------------------------------------------------------
# LogicalUPath tests
# ---------------------------------------------------------------------------


def test_logical_upath_isinstance_logical_type():
    """LogicalUPath() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert isinstance(LogicalUPath(), LogicalType)


def test_logical_upath_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().logical_type_name == "upath.UPath"


def test_logical_upath_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().python_type is UPath


def test_logical_upath_arrow_ext_name():
    """get_arrow_extension_type().extension_name is 'upath.UPath'."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().get_arrow_extension_type().extension_name == "upath.UPath"


def test_logical_upath_arrow_ext_storage_type():
    """Arrow extension storage type is pa.large_string()."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().get_arrow_extension_type().storage_type == pa.large_string()


def test_logical_upath_get_arrow_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_logical_upath_round_trip():
    """UPath -> python_to_storage -> storage_to_python -> UPath is identity."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    up = UPath("s3://bucket/key/file.txt")
    assert lt.storage_to_python(lt.python_to_storage(up)) == up


def test_logical_upath_python_to_storage_returns_string():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    result = lt.python_to_storage(UPath("s3://bucket/key"))
    assert isinstance(result, str)
    assert result == "s3://bucket/key"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py::test_logical_path_isinstance_logical_type -v
```

Expected: `ModuleNotFoundError` — `builtin_logical_types` does not exist yet.

- [ ] **Step 3: Create `src/orcapod/extension_types/builtin_logical_types.py` with `LogicalPath` and `LogicalUPath`**

```python
"""Built-in LogicalType implementations for orcapod.

Provides three built-in logical types registered into the default
``DataContext.logical_type_registry`` via ``contexts/data/v0.1.json``:

- ``LogicalPath``: maps ``pathlib.Path`` ↔ Arrow large_string extension "pathlib.Path"
- ``LogicalUPath``: maps ``upath.UPath`` ↔ Arrow large_string extension "upath.UPath"
- ``LogicalUUID``: maps ``uuid.UUID`` ↔ PyArrow built-in ``pa.uuid()`` ("arrow.uuid")

Note:
    All imports from orcapod.extension_types use direct submodule paths
    (e.g. ``from orcapod.extension_types.registry import ...``) rather than
    the package ``__init__`` to avoid circular imports when the context system
    loads this module at startup.
"""

from __future__ import annotations

import pathlib
import uuid as _uuid_module
from typing import TYPE_CHECKING, Any

from upath import UPath

from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class LogicalPath:
    """Logical type for ``pathlib.Path``.

    Stores paths as Arrow large strings using the custom extension type
    ``"pathlib.Path"`` with metadata ``b"orcapod.builtin"``.

    Example:
        >>> lt = LogicalPath()
        >>> lt.python_to_storage(pathlib.Path("/tmp/foo"))
        '/tmp/foo'
        >>> lt.storage_to_python('/tmp/foo')
        PosixPath('/tmp/foo')
    """

    _arrow_ext_class = make_arrow_extension_type(
        "pathlib.Path", pa.large_string(), b"orcapod.builtin"
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "pathlib.Path", pa.large_string(), "orcapod.builtin"
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "pathlib.Path"
    python_type: type = pathlib.Path

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``pathlib.Path``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"pathlib.Path"`` and storage type ``pa.large_string()``.
        """
        if LogicalPath._arrow_ext is None:
            LogicalPath._arrow_ext = LogicalPath._arrow_ext_class()
        return LogicalPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``pathlib.Path``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"pathlib.Path"``.
        """
        if LogicalPath._polars_ext is None:
            LogicalPath._polars_ext = LogicalPath._polars_ext_class()
        return LogicalPath._polars_ext

    def python_to_storage(self, value: Any) -> str:
        """Convert a ``pathlib.Path`` to its string representation.

        Args:
            value: A ``pathlib.Path`` instance.

        Returns:
            The string form of the path (e.g. ``"/tmp/foo"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any) -> pathlib.Path:
        """Reconstruct a ``pathlib.Path`` from its string representation.

        Args:
            storage_value: A string path as stored in Arrow.

        Returns:
            A ``pathlib.Path`` instance.
        """
        return pathlib.Path(storage_value)


class LogicalUPath:
    """Logical type for ``upath.UPath``.

    Stores paths as Arrow large strings using the custom extension type
    ``"upath.UPath"`` with metadata ``b"orcapod.builtin"``.

    Example:
        >>> lt = LogicalUPath()
        >>> lt.python_to_storage(UPath("s3://bucket/key"))
        's3://bucket/key'
        >>> lt.storage_to_python("s3://bucket/key")
        UPath('s3://bucket/key')
    """

    _arrow_ext_class = make_arrow_extension_type(
        "upath.UPath", pa.large_string(), b"orcapod.builtin"
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        "upath.UPath", pa.large_string(), "orcapod.builtin"
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "upath.UPath"
    python_type: type = UPath

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``upath.UPath``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"upath.UPath"`` and storage type ``pa.large_string()``.
        """
        if LogicalUPath._arrow_ext is None:
            LogicalUPath._arrow_ext = LogicalUPath._arrow_ext_class()
        return LogicalUPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``upath.UPath``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"upath.UPath"``.
        """
        if LogicalUPath._polars_ext is None:
            LogicalUPath._polars_ext = LogicalUPath._polars_ext_class()
        return LogicalUPath._polars_ext

    def python_to_storage(self, value: Any) -> str:
        """Convert a ``upath.UPath`` to its string representation.

        Args:
            value: A ``upath.UPath`` instance.

        Returns:
            The string form of the path (e.g. ``"s3://bucket/key"``).
        """
        return str(value)

    def storage_to_python(self, storage_value: Any) -> UPath:
        """Reconstruct a ``upath.UPath`` from its string representation.

        Args:
            storage_value: A string path as stored in Arrow.

        Returns:
            A ``upath.UPath`` instance.
        """
        return UPath(storage_value)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -k "logical_path or logical_upath" -v
```

Expected: All `LogicalPath` and `LogicalUPath` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/builtin_logical_types.py \
        tests/test_extension_types/test_builtin_logical_types.py
git commit -m "feat(extension_types): implement LogicalPath and LogicalUPath"
```

---

### Task 4: `LogicalUUID` implementation

**Files:**
- Modify: `src/orcapod/extension_types/builtin_logical_types.py`
- Modify: `tests/test_extension_types/test_builtin_logical_types.py`

- [ ] **Step 1: Write the failing tests for `LogicalUUID`**

Append to `tests/test_extension_types/test_builtin_logical_types.py`:

```python
# ---------------------------------------------------------------------------
# LogicalUUID tests
# ---------------------------------------------------------------------------


def test_logical_uuid_isinstance_logical_type():
    """LogicalUUID() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert isinstance(LogicalUUID(), LogicalType)


def test_logical_uuid_logical_type_name():
    """logical_type_name is 'uuid.UUID', not the Arrow extension name."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().logical_type_name == "uuid.UUID"


def test_logical_uuid_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().python_type is uuid_module.UUID


def test_logical_uuid_arrow_ext_name_is_arrow_uuid():
    """Arrow extension name is 'arrow.uuid', intentionally different from logical_type_name."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type().extension_name == "arrow.uuid"
    assert lt.logical_type_name != lt.get_arrow_extension_type().extension_name


def test_logical_uuid_get_arrow_extension_type_returns_pa_uuid():
    """get_arrow_extension_type() returns PyArrow's built-in pa.uuid() type."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type() == pa.uuid()


def test_logical_uuid_get_arrow_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_logical_uuid_get_polars_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


def test_logical_uuid_round_trip():
    """UUID -> python_to_storage -> storage_to_python -> UUID is identity."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    u = uuid_module.uuid4()
    assert lt.storage_to_python(lt.python_to_storage(u)) == u


def test_logical_uuid_python_to_storage_returns_bytes():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.python_to_storage(u)
    assert isinstance(result, bytes)
    assert len(result) == 16


def test_logical_uuid_storage_to_python_accepts_bytes():
    """storage_to_python works when storage_value is plain bytes."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    recovered = lt.storage_to_python(u.bytes)
    assert recovered == u


def test_logical_uuid_registration_does_not_raise():
    """Registering LogicalUUID succeeds even though pa.uuid() is already in PyArrow's registry."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = LogicalTypeRegistry()
    lt = LogicalUUID()
    registry.register(lt)  # should NOT raise
    assert registry.get_by_logical_name("uuid.UUID") is lt
    assert registry.get_by_arrow_extension_name("arrow.uuid") is lt
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py::test_logical_uuid_isinstance_logical_type -v
```

Expected: `ImportError` — `LogicalUUID` does not exist yet.

- [ ] **Step 3: Add `LogicalUUID` to `builtin_logical_types.py`**

Append to the end of `src/orcapod/extension_types/builtin_logical_types.py`:

```python
class LogicalUUID:
    """Logical type for ``uuid.UUID``.

    Uses PyArrow's built-in ``pa.uuid()`` extension type (``"arrow.uuid"``)
    which stores UUID values as 16-byte binary (``pa.binary(16)``).

    Note:
        ``logical_type_name`` (``"uuid.UUID"``) intentionally differs from
        the Arrow extension name (``"arrow.uuid"``). The
        ``LogicalTypeRegistry`` stores both bindings so that lookups by
        either key resolve to this same instance.

    Example:
        >>> import uuid
        >>> lt = LogicalUUID()
        >>> u = uuid.uuid4()
        >>> lt.storage_to_python(lt.python_to_storage(u)) == u
        True
    """

    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("arrow.uuid", pa.binary(16), None)
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "uuid.UUID"
    python_type: type = _uuid_module.UUID

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return PyArrow's built-in ``pa.uuid()`` extension type.

        Returns:
            A cached ``pa.uuid()`` instance (Arrow extension name ``"arrow.uuid"``,
            storage type ``pa.binary(16)``).
        """
        if LogicalUUID._arrow_ext is None:
            LogicalUUID._arrow_ext = pa.uuid()
        return LogicalUUID._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``arrow.uuid``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"arrow.uuid"`` (matches the Arrow extension name, not the
            logical type name).
        """
        if LogicalUUID._polars_ext is None:
            LogicalUUID._polars_ext = LogicalUUID._polars_ext_class()
        return LogicalUUID._polars_ext

    def python_to_storage(self, value: Any) -> bytes:
        """Convert a ``uuid.UUID`` to its 16-byte binary representation.

        Args:
            value: A ``uuid.UUID`` instance.

        Returns:
            A 16-byte ``bytes`` object (big-endian byte order, as per
            ``uuid.UUID.bytes``).
        """
        return value.bytes

    def storage_to_python(self, storage_value: Any) -> _uuid_module.UUID:
        """Reconstruct a ``uuid.UUID`` from its 16-byte binary representation.

        Args:
            storage_value: A bytes-like object of length 16.

        Returns:
            A ``uuid.UUID`` instance.
        """
        return _uuid_module.UUID(bytes=bytes(storage_value))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v
```

Expected: All tests in the file PASS (LogicalPath, LogicalUPath, and LogicalUUID).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/builtin_logical_types.py \
        tests/test_extension_types/test_builtin_logical_types.py
git commit -m "feat(extension_types): implement LogicalUUID"
```

---

### Task 5: Wire built-in types into `DataContext`

**Files:**
- Modify: `src/orcapod/contexts/core.py`
- Modify: `src/orcapod/contexts/registry.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`
- Modify: `src/orcapod/contexts/data/schemas/context_schema.json`
- Modify: `src/orcapod/contexts/__init__.py`
- Modify: `tests/test_extension_types/test_builtin_logical_types.py`

This task wires everything together. The integration tests are written first, but they cannot pass until the DataContext and JSON spec are updated. Do all the sub-steps in a single commit.

- [ ] **Step 1: Write the failing integration tests**

Append to `tests/test_extension_types/test_builtin_logical_types.py`:

```python
# ---------------------------------------------------------------------------
# Default context integration tests
# ---------------------------------------------------------------------------


def test_default_context_has_logical_type_registry():
    """DataContext has a logical_type_registry attribute."""
    from orcapod.contexts import get_default_context

    ctx = get_default_context()
    assert hasattr(ctx, "logical_type_registry")


def test_default_context_registry_has_logical_path():
    """Default registry returns LogicalPath for 'pathlib.Path'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("pathlib.Path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_lookup_by_python_type_path():
    """Default registry routes pathlib.Path to LogicalPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_python_type(pathlib.Path)
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_lookup_by_arrow_name_path():
    """Default registry routes 'pathlib.Path' arrow ext name to LogicalPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_arrow_extension_name("pathlib.Path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_has_logical_upath():
    """Default registry returns LogicalUPath for 'upath.UPath'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("upath.UPath")
    assert isinstance(lt, LogicalUPath)


def test_default_context_registry_lookup_by_python_type_upath():
    """Default registry routes UPath to LogicalUPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_python_type(UPath)
    assert isinstance(lt, LogicalUPath)


def test_default_context_registry_has_logical_uuid():
    """Default registry returns LogicalUUID for 'uuid.UUID'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("uuid.UUID")
    assert isinstance(lt, LogicalUUID)


def test_default_context_registry_lookup_by_arrow_name_uuid():
    """Default registry routes 'arrow.uuid' arrow ext name to LogicalUUID."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_arrow_extension_name("arrow.uuid")
    assert isinstance(lt, LogicalUUID)


def test_default_context_registry_uuid_logical_name_differs_from_arrow_name():
    """The same LogicalUUID instance is found by both 'uuid.UUID' and 'arrow.uuid'."""
    from orcapod.contexts import get_default_context

    registry = get_default_context().logical_type_registry
    by_logical = registry.get_by_logical_name("uuid.UUID")
    by_arrow = registry.get_by_arrow_extension_name("arrow.uuid")
    assert by_logical is by_arrow


def test_get_default_logical_type_registry_returns_same_as_context():
    """get_default_logical_type_registry() is the same object as get_default_context().logical_type_registry."""
    from orcapod.contexts import get_default_context, get_default_logical_type_registry

    assert get_default_logical_type_registry() is get_default_context().logical_type_registry


def test_default_context_idempotent_registry():
    """Calling get_default_context() twice returns the same LogicalTypeRegistry instance."""
    from orcapod.contexts import get_default_context

    r1 = get_default_context().logical_type_registry
    r2 = get_default_context().logical_type_registry
    assert r1 is r2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py::test_default_context_has_logical_type_registry -v
```

Expected: FAIL — `DataContext` has no `logical_type_registry` attribute.

- [ ] **Step 3: Add `logical_type_registry` field to `DataContext` in `core.py`**

Current `core.py` imports (lines 1–16):

```python
"""
Core data structures and exceptions for the OrcaPod context system.
...
"""

from dataclasses import dataclass

from orcapod.hashing.semantic_hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    SemanticHasherProtocol,
)
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol
```

Add one import and one field. The final `core.py` content:

```python
"""
Core data structures and exceptions for the OrcaPod context system.

This module defines the basic types and exceptions used throughout
the context management system.
"""

from dataclasses import dataclass

from orcapod.extension_types.registry import LogicalTypeRegistry
from orcapod.hashing.semantic_hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    SemanticHasherProtocol,
)
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


@dataclass
class DataContext:
    """
    Data context containing all versioned components needed for data interpretation.

    A DataContext represents a specific version of the OrcaPod system configuration,
    including semantic type registries, hashers, and other components that affect
    how data is processed and interpreted.

    Attributes:
        context_key: Unique identifier (e.g., "std:v0.1:default")
        version: Version string (e.g., "v0.1")
        description: Human-readable description of this context
        semantic_type_registry: Registry of semantic type converters
        arrow_hasher: Arrow table hasher for this context
        semantic_hasher: General semantic hasher for this context
        type_handler_registry: Registry of TypeHandlerProtocol instances for SemanticHasherProtocol
        logical_type_registry: Registry of LogicalType instances (Path, UPath, UUID, etc.)
    """

    context_key: str
    version: str
    description: str
    type_converter: TypeConverterProtocol
    arrow_hasher: ArrowHasherProtocol
    semantic_hasher: SemanticHasherProtocol  # this is the currently the JSON hasher
    type_handler_registry: TypeHandlerRegistry
    logical_type_registry: LogicalTypeRegistry


class ContextValidationError(Exception):
    """Raised when context validation fails."""

    pass


class ContextResolutionError(Exception):
    """Raised when context cannot be resolved."""

    pass
```

- [ ] **Step 4: Update `contexts/registry.py` — add `logical_type_registry` to required fields and `_create_context_from_spec`**

In `_load_spec_file` (around line 148), add `"logical_type_registry"` to `required_fields`:

```python
required_fields = [
    "context_key",
    "version",
    "type_converter",
    "arrow_hasher",
    "semantic_hasher",
    "type_handler_registry",
    "logical_type_registry",
]
```

In `_create_context_from_spec` (around line 296), add `logical_type_registry` to the `DataContext(...)` call:

```python
return DataContext(
    context_key=context_key,
    version=version,
    description=description,
    type_converter=ref_lut["type_converter"],
    arrow_hasher=ref_lut["arrow_hasher"],
    semantic_hasher=ref_lut["semantic_hasher"],
    type_handler_registry=ref_lut["type_handler_registry"],
    logical_type_registry=ref_lut["logical_type_registry"],
)
```

- [ ] **Step 5: Add `logical_type_registry` entry to `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, add the following JSON block before the `"metadata"` key (after the `"semantic_hasher"` block):

```json
    "logical_type_registry": {
        "_class": "orcapod.extension_types.registry.LogicalTypeRegistry",
        "_config": {
            "logical_types": [
                {
                    "_class": "orcapod.extension_types.builtin_logical_types.LogicalPath",
                    "_config": {}
                },
                {
                    "_class": "orcapod.extension_types.builtin_logical_types.LogicalUPath",
                    "_config": {}
                },
                {
                    "_class": "orcapod.extension_types.builtin_logical_types.LogicalUUID",
                    "_config": {}
                }
            ]
        }
    },
```

The full updated `v0.1.json` after the edit:

```json
{
    "context_key": "std:v0.1:default",
    "version": "v0.1",
    "description": "Initial stable release with basic Path semantic type support",
    "file_hasher": {
        "_class": "orcapod.hashing.file_hashers.BasicFileHasher",
        "_config": {
            "algorithm": "sha256"
        }
    },
    "semantic_registry": {
        "_class": "orcapod.semantic_types.semantic_registry.SemanticTypeRegistry",
        "_config": {
            "converters": {
                "upath": {
                    "_class": "orcapod.semantic_types.semantic_struct_converters.UPathStructConverter",
                    "_config": {
                        "file_hasher": {"_ref": "file_hasher"}
                    }
                },
                "path": {
                    "_class": "orcapod.semantic_types.semantic_struct_converters.PythonPathStructConverter",
                    "_config": {
                        "file_hasher": {"_ref": "file_hasher"}
                    }
                }
            }
        }
    },
    "arrow_hasher": {
        "_class": "orcapod.hashing.arrow_hashers.StarfixArrowHasher",
        "_config": {
            "hasher_id": "arrow_v0.1",
            "semantic_registry": {
                "_ref": "semantic_registry"
            }
        }
    },
    "type_converter": {
        "_class": "orcapod.semantic_types.universal_converter.UniversalTypeConverter",
        "_config": {
            "semantic_registry": {
                "_ref": "semantic_registry"
            }
        }
    },
    "function_info_extractor": {
        "_class": "orcapod.hashing.semantic_hashing.function_info_extractors.FunctionSignatureExtractor",
        "_config": {
            "include_module": true,
            "include_defaults": true
        }
    },
    "type_handler_registry": {
        "_class": "orcapod.hashing.semantic_hashing.type_handler_registry.TypeHandlerRegistry",
        "_config": {
            "handlers": [
                [{"_type": "builtins.bytes"},    {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.BytesHandler",    "_config": {}}],
                [{"_type": "builtins.bytearray"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.BytesHandler",    "_config": {}}],
                [{"_type": "pathlib.Path"},        {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PathContentHandler", "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
                [{"_type": "upath.core.UPath"},    {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UPathContentHandler", "_config": {"file_hasher": {"_ref": "file_hasher"}}}],
                [{"_type": "uuid.UUID"},           {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UUIDHandler",         "_config": {}}],
                [{"_type": "types.FunctionType"},        {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FunctionHandler", "_config": {"function_info_extractor": {"_ref": "function_info_extractor"}}}],
                [{"_type": "types.BuiltinFunctionType"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FunctionHandler", "_config": {"function_info_extractor": {"_ref": "function_info_extractor"}}}],
                [{"_type": "types.MethodType"},          {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.FunctionHandler", "_config": {"function_info_extractor": {"_ref": "function_info_extractor"}}}],
                [{"_type": "builtins.type"},       {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.TypeObjectHandler",   "_config": {}}],
                [{"_type": "types.GenericAlias"},   {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.GenericAliasHandler",  "_config": {}}],
                [{"_type": "types.UnionType"},     {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.UnionTypeHandler",    "_config": {}}],
                [{"_type": "typing._GenericAlias"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.GenericAliasHandler",  "_config": {}}],
                [{"_type": "typing._SpecialForm"},  {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.SpecialFormHandler",   "_config": {}}],
                [{"_type": "pyarrow.Table"},        {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.ArrowTableHandler",  "_config": {"arrow_hasher": {"_ref": "arrow_hasher"}}}],
                [{"_type": "pyarrow.RecordBatch"},  {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.ArrowTableHandler",  "_config": {"arrow_hasher": {"_ref": "arrow_hasher"}}}]
            ]
        }
    },
    "semantic_hasher": {
        "_class": "orcapod.hashing.semantic_hashing.semantic_hasher.BaseSemanticHasher",
        "_config": {
            "hasher_id": "semantic_v0.1",
            "type_handler_registry": {
                "_ref": "type_handler_registry"
            }
        }
    },
    "logical_type_registry": {
        "_class": "orcapod.extension_types.registry.LogicalTypeRegistry",
        "_config": {
            "logical_types": [
                {
                    "_class": "orcapod.extension_types.builtin_logical_types.LogicalPath",
                    "_config": {}
                },
                {
                    "_class": "orcapod.extension_types.builtin_logical_types.LogicalUPath",
                    "_config": {}
                },
                {
                    "_class": "orcapod.extension_types.builtin_logical_types.LogicalUUID",
                    "_config": {}
                }
            ]
        }
    },
    "metadata": {
        "created_date": "2025-08-01",
        "author": "OrcaPod Core Team",
        "changelog": [
            "Initial release with Path semantic type support",
            "Basic SHA-256 hashing for files and objects",
            "Arrow logical serialization method",
            "Introduced arrow_v0.1 StarfixArrowHasher using starfix ArrowDigester for cross-language-compatible Arrow hashing"
        ]
    }
}
```

- [ ] **Step 6: Add `logical_type_registry` to `context_schema.json`**

In `src/orcapod/contexts/data/schemas/context_schema.json`:

Add `"logical_type_registry"` to the `"required"` array (after `"type_handler_registry"`):

```json
"required": [
    "context_key",
    "version",
    "semantic_registry",
    "type_converter",
    "arrow_hasher",
    "semantic_hasher",
    "type_handler_registry",
    "logical_type_registry"
],
```

Add `"logical_type_registry"` entry to the `"properties"` object (after `"type_handler_registry"`):

```json
"logical_type_registry": {
    "$ref": "#/$defs/objectspec",
    "description": "ObjectSpec for the LogicalTypeRegistry (Path, UPath, UUID built-ins)"
},
```

- [ ] **Step 7: Add `get_default_logical_type_registry()` to `contexts/__init__.py`**

In `src/orcapod/contexts/__init__.py`, add after `get_default_type_converter()`:

```python
def get_default_logical_type_registry() -> "LogicalTypeRegistry":
    """Get the default logical type registry.

    Returns:
        ``LogicalTypeRegistry`` instance from the default context.
    """
    return get_default_context().logical_type_registry
```

Add the import at the top of the file (after the `from orcapod.protocols` imports):

```python
from orcapod.extension_types.registry import LogicalTypeRegistry
```

Add `"get_default_logical_type_registry"` to `__all__`.

The updated `__all__` in `contexts/__init__.py`:

```python
__all__ = [
    # Core types
    "DataContext",
    "ContextValidationError",
    "ContextResolutionError",
    # Main functions
    "resolve_context",
    "get_available_contexts",
    "get_context_info",
    "get_default_context",
    # Convenience accessors
    "get_default_semantic_hasher",
    "get_default_arrow_hasher",
    "get_default_type_converter",
    "get_default_logical_type_registry",
    # Management functions
    "set_default_context_version",
    "validate_all_contexts",
    "reload_contexts",
    # Advanced usage
    "create_registry",
    "JSONDataContextRegistry",
]
```

- [ ] **Step 8: Run the integration tests**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v
```

Expected: All tests PASS, including the new integration tests.

- [ ] **Step 9: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: All previously-passing tests still PASS. The 6 `default_logical_type_registry` tests in `test_registry.py` still pass (the module-level variable is still there; we remove it next).

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/contexts/core.py \
        src/orcapod/contexts/registry.py \
        src/orcapod/contexts/data/v0.1.json \
        src/orcapod/contexts/data/schemas/context_schema.json \
        src/orcapod/contexts/__init__.py \
        tests/test_extension_types/test_builtin_logical_types.py
git commit -m "feat(contexts): add logical_type_registry to DataContext and v0.1 context"
```

---

### Task 6: Remove `default_logical_type_registry` and clean up stale tests

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `tests/test_extension_types/test_registry.py`

The module-level `default_logical_type_registry` in `extension_types/__init__.py` is replaced by the context-scoped registry. This task removes it and deletes the 6 tests that relied on it.

- [ ] **Step 1: Remove `default_logical_type_registry` from `extension_types/__init__.py`**

Replace the current content of `src/orcapod/extension_types/__init__.py`:

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

Built-in registrations (``LogicalPath``, ``LogicalUPath``, ``LogicalUUID``) are
wired into ``DataContext`` via ``contexts/data/v0.1.json``. The primary access
path for the default registry is:

- ``get_default_context().logical_type_registry``
- ``get_default_logical_type_registry()`` (from ``orcapod.contexts``)
"""

from __future__ import annotations

from .protocols import LogicalType
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

__all__ = [
    "LogicalType",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "make_polars_extension_type",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

- [ ] **Step 2: Remove the 6 stale `default_logical_type_registry` tests from `test_registry.py`**

Delete the entire section at the end of `tests/test_extension_types/test_registry.py` (lines 450–532):

```python
# ---------------------------------------------------------------------------
# default_logical_type_registry tests
# ---------------------------------------------------------------------------

def test_logical_type_registry_module_instance():
    ...

def test_default_registry_is_same_object_across_imports():
    ...

def test_default_registry_register_and_lookup():
    ...

def test_default_registry_register_idempotent():
    ...

def test_default_registry_populates_arrow_global():
    ...

def test_default_registry_populates_polars_global():
    ...
```

These tests are superseded by the integration tests in `test_builtin_logical_types.py`.

- [ ] **Step 3: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: All tests PASS. The 6 removed tests no longer exist. No regressions.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/extension_types/__init__.py \
        tests/test_extension_types/test_registry.py
git commit -m "refactor(extension_types): remove default_logical_type_registry module-level variable"
```

---

## Self-Review

### Spec coverage check

| Spec requirement | Covered by |
|---|---|
| `LogicalPath` implementation | Task 3 |
| `LogicalUPath` implementation | Task 3 |
| `LogicalUUID` implementation (with `pa.uuid()`) | Task 4 |
| `make_polars_extension_type` helper | Task 1 |
| `LogicalTypeRegistry.__init__` `logical_types` param | Task 2 |
| `DataContext.logical_type_registry` field | Task 5, Step 3 |
| `v0.1.json` `logical_type_registry` entry | Task 5, Step 5 |
| `context_schema.json` update | Task 5, Step 6 |
| `get_default_logical_type_registry()` convenience function | Task 5, Step 7 |
| Remove `default_logical_type_registry` from `__init__.py` | Task 6, Step 1 |
| Protocol conformance tests | Task 3 & 4 |
| Property value tests | Task 3 & 4 |
| Conversion round-trip tests | Task 3 & 4 |
| Default context registration tests | Task 5, Step 1 |
| Pre-existing Arrow type tolerance test (`LogicalUUID`) | Task 4, Step 1 |
| Idempotence test (context caching) | Task 5, Step 1 |
| UUID `logical_type_name` ≠ Arrow ext name test | Task 4, Step 1 |
| Circular import avoidance (submodule imports) | Task 3, Step 3 (in `builtin_logical_types.py`) |
| Class-level caching for extension type instances | Task 3, Step 3 & Task 4, Step 3 |
| Export `make_polars_extension_type` from `__init__.py` | Task 1, Step 4 |

### Type consistency check

- `make_polars_extension_type(name, arrow_storage_type, metadata)` — used consistently in Task 1 (definition) and Task 3/4 (class-body calls).
- `LogicalTypeRegistry(logical_types=[...])` — defined in Task 2, used in Task 5 JSON spec.
- `DataContext.logical_type_registry` field — added in Task 5 Step 3, passed in `_create_context_from_spec` in Task 5 Step 4.
- `get_default_logical_type_registry()` returns `LogicalTypeRegistry`, consistent with `get_default_type_converter()` pattern.
- `LogicalUUID.logical_type_name = "uuid.UUID"` vs `get_arrow_extension_type().extension_name = "arrow.uuid"` — intentional difference, tested in Task 4.

### No placeholder scan

All steps contain complete code or exact commands. No "TBD", "similar to", or "add validation" phrases.
