# PLT-1670: Namespace Built-in Extension Types under `orcapod.*` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rename the three built-in Arrow extension types from upstream module-path names (`"pathlib.Path"`, `"upath.UPath"`, `"uuid.UUID"`) to Orcapod-owned namespaced names (`"orcapod.path"`, `"orcapod.upath"`, `"orcapod.uuid"`), and expose `Path`, `UPath`, `UUID` type aliases at the top-level `orcapod` namespace.

**Architecture:** The three `LogicalType` classes in `builtin_logical_types.py` each carry a string constant used as both `logical_type_name` and the Arrow/Polars extension name — changing those constants is the entirety of the rename. Top-level aliases are simple re-exports in `__init__.py` that expose the upstream types under an Orcapod-stable symbol. Tests are updated last to match reality after the TDD red → green cycle.

**Tech Stack:** Python, PyArrow (extension types), Polars (extension types), pytest via `uv run pytest`.

---

## File Map

| File | Action | What changes |
|------|--------|-------------|
| `tests/test_extension_types/test_builtin_logical_types.py` | Modify | Update 13 string assertions from old extension names to new `orcapod.*` names; add 5 alias tests |
| `src/orcapod/extension_types/builtin_logical_types.py` | Modify | Rename 6 string constants (2 per class: `_arrow_ext_class`, `_polars_ext_class`), 3 `logical_type_name` class attributes, update module and class docstrings |
| `src/orcapod/__init__.py` | Modify | Add `Path`, `UPath`, `UUID` re-exports with stability docstring; add to `__all__` |

No other files need to change — the context config (`contexts/data/v0.1.json`) references classes by dotted name, not by extension name string, so it is unaffected.

---

## Task 1: Update tests to assert on new `orcapod.*` extension names

This is the TDD red step. After this task the test suite will fail with assertion errors until Task 2 fixes the implementation.

**Files:**
- Modify: `tests/test_extension_types/test_builtin_logical_types.py`

- [ ] **Step 1: Run the current test suite to confirm it is green before any changes**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v --tb=short 2>&1 | tail -20
```

Expected: all tests pass (green baseline).

- [ ] **Step 2: Update `test_logical_path_logical_type_name` (line 31)**

Change:
```python
assert LogicalPath().logical_type_name == "pathlib.Path"
```
To:
```python
assert LogicalPath().logical_type_name == "orcapod.path"
```

- [ ] **Step 3: Update `test_logical_path_arrow_ext_name` (line 44)**

Change:
```python
assert LogicalPath().get_arrow_extension_type().extension_name == "pathlib.Path"
```
To:
```python
assert LogicalPath().get_arrow_extension_type().extension_name == "orcapod.path"
```

- [ ] **Step 4: Update `test_logical_upath_logical_type_name` (line 103)**

Change:
```python
assert LogicalUPath().logical_type_name == "upath.UPath"
```
To:
```python
assert LogicalUPath().logical_type_name == "orcapod.upath"
```

- [ ] **Step 5: Update `test_logical_upath_arrow_ext_name` (line 116)**

Change:
```python
assert LogicalUPath().get_arrow_extension_type().extension_name == "upath.UPath"
```
To:
```python
assert LogicalUPath().get_arrow_extension_type().extension_name == "orcapod.upath"
```

- [ ] **Step 6: Update `test_logical_uuid_logical_type_name` (line 173)**

Change:
```python
assert LogicalUUID().logical_type_name == "uuid.UUID"
```
To:
```python
assert LogicalUUID().logical_type_name == "orcapod.uuid"
```

- [ ] **Step 7: Update `test_logical_uuid_arrow_ext_name` (lines 187–188)**

Change:
```python
    assert lt.get_arrow_extension_type().extension_name == "uuid.UUID"
    assert lt.get_arrow_extension_type().extension_name == lt.logical_type_name
```
To:
```python
    assert lt.get_arrow_extension_type().extension_name == "orcapod.uuid"
    assert lt.get_arrow_extension_type().extension_name == lt.logical_type_name
```

- [ ] **Step 8: Update `test_logical_uuid_registration_does_not_raise` (lines 248–249)**

Change:
```python
    assert registry.get_by_logical_name("uuid.UUID") is lt
    assert registry.get_by_arrow_extension_name("uuid.UUID") is lt
```
To:
```python
    assert registry.get_by_logical_name("orcapod.uuid") is lt
    assert registry.get_by_arrow_extension_name("orcapod.uuid") is lt
```

- [ ] **Step 9: Update default-context tests — `test_default_context_registry_has_logical_path` (line 384)**

Change:
```python
    lt = registry.get_by_logical_name("pathlib.Path")
```
To:
```python
    lt = registry.get_by_logical_name("orcapod.path")
```

- [ ] **Step 10: Update `test_default_context_registry_lookup_by_arrow_name_path` (line 404)**

Change:
```python
    lt = registry.get_by_arrow_extension_name("pathlib.Path")
```
To:
```python
    lt = registry.get_by_arrow_extension_name("orcapod.path")
```

- [ ] **Step 11: Update `test_default_context_registry_has_logical_upath` (line 414)**

Change:
```python
    lt = registry.get_by_logical_name("upath.UPath")
```
To:
```python
    lt = registry.get_by_logical_name("orcapod.upath")
```

- [ ] **Step 12: Update `test_default_context_registry_has_logical_uuid` (line 434)**

Change:
```python
    lt = registry.get_by_logical_name("uuid.UUID")
```
To:
```python
    lt = registry.get_by_logical_name("orcapod.uuid")
```

- [ ] **Step 13: Update `test_default_context_registry_lookup_by_arrow_name_uuid` (line 444)**

Change:
```python
    lt = registry.get_by_arrow_extension_name("uuid.UUID")
```
To:
```python
    lt = registry.get_by_arrow_extension_name("orcapod.uuid")
```

- [ ] **Step 14: Run tests to confirm they are now red**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v --tb=line 2>&1 | grep -E "FAILED|PASSED|ERROR" | head -30
```

Expected: 13 tests now fail with `AssertionError`; all others pass.

---

## Task 2: Rename extension type strings in `builtin_logical_types.py`

This makes the red tests green. All 6 extension-name string constants, all 3 `logical_type_name` class attributes, and docstrings are updated to the `orcapod.*` namespace.

**Files:**
- Modify: `src/orcapod/extension_types/builtin_logical_types.py`

- [ ] **Step 1: Update the module-level docstring**

Change the opening docstring lines 6–8 from:
```python
- ``LogicalPath``: maps ``pathlib.Path`` ↔ Arrow large_string extension "pathlib.Path"
- ``LogicalUPath``: maps ``upath.UPath`` ↔ Arrow large_string extension "upath.UPath"
- ``LogicalUUID``: maps ``uuid.UUID`` ↔ Arrow large_binary extension "uuid.UUID"
```
To:
```python
- ``LogicalPath``: maps ``pathlib.Path`` ↔ Arrow large_string extension ``"orcapod.path"``
- ``LogicalUPath``: maps ``upath.UPath`` ↔ Arrow large_string extension ``"orcapod.upath"``
- ``LogicalUUID``: maps ``uuid.UUID`` ↔ Arrow large_binary extension ``"orcapod.uuid"``
```

And replace the full module docstring with the updated version that adds the stability rationale note:

```python
"""Built-in LogicalType implementations for orcapod.

Provides three built-in logical types registered into the default
``DataContext.logical_type_registry`` via ``contexts/data/v0.1.json``:

- ``LogicalPath``: maps ``pathlib.Path`` ↔ Arrow large_string extension ``"orcapod.path"``
- ``LogicalUPath``: maps ``upath.UPath`` ↔ Arrow large_string extension ``"orcapod.upath"``
- ``LogicalUUID``: maps ``uuid.UUID`` ↔ Arrow large_binary extension ``"orcapod.uuid"``

All three types use the ``orcapod.*`` extension name namespace rather than the upstream
module-qualified names (``"pathlib.Path"``, etc.). This gives Orcapod stable ownership of
the on-disk extension identity: even if the upstream library is renamed or restructured,
data written with these extension names continues to be readable without modification.

Note:
    All imports from orcapod.extension_types use direct submodule paths
    (e.g. ``from orcapod.extension_types.registry import ...``) rather than
    the package ``__init__`` to avoid circular imports when the context system
    loads this module at startup.
"""
```

- [ ] **Step 2: Update `LogicalPath` class — class attributes and docstrings**

Replace the `LogicalPath` class definition (lines 30–94) with:

```python
class LogicalPath:
    """Logical type for ``pathlib.Path``.

    Stores paths as Arrow large strings using the custom extension type
    ``"orcapod.path"``.

    The extension name ``"orcapod.path"`` is Orcapod-owned and stable; it does not
    depend on the upstream ``pathlib`` module path. Use ``orcapod.Path`` (a top-level
    alias for ``pathlib.Path``) as the preferred way to reference this type in user code.

    Example:
        >>> lt = LogicalPath()
        >>> lt.python_to_storage(pathlib.Path("/tmp/foo"))
        '/tmp/foo'
        >>> lt.storage_to_python('/tmp/foo')
        PosixPath('/tmp/foo')
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.path", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.path", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.path"
    python_type: type = pathlib.Path

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``pathlib.Path``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"orcapod.path"`` and storage type ``pa.large_string()``.
        """
        if LogicalPath._arrow_ext is None:
            LogicalPath._arrow_ext = LogicalPath._arrow_ext_class()
        return LogicalPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``pathlib.Path``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"orcapod.path"``.
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
```

- [ ] **Step 3: Update `LogicalUPath` class — class attributes and docstrings**

Replace the `LogicalUPath` class definition (lines 97–161) with:

```python
class LogicalUPath:
    """Logical type for ``upath.UPath``.

    Stores paths as Arrow large strings using the custom extension type
    ``"orcapod.upath"``.

    The extension name ``"orcapod.upath"`` is Orcapod-owned and stable; it does not
    depend on the upstream ``upath`` module path. Use ``orcapod.UPath`` (a top-level
    alias for ``upath.UPath``) as the preferred way to reference this type in user code.

    Example:
        >>> lt = LogicalUPath()
        >>> lt.python_to_storage(UPath("s3://bucket/key"))
        's3://bucket/key'
        >>> lt.storage_to_python("s3://bucket/key")
        UPath('s3://bucket/key')
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.upath", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.upath", pa.large_string())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.upath"
    python_type: type = UPath

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``upath.UPath``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"orcapod.upath"`` and storage type ``pa.large_string()``.
        """
        if LogicalUPath._arrow_ext is None:
            LogicalUPath._arrow_ext = LogicalUPath._arrow_ext_class()
        return LogicalUPath._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``upath.UPath``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"orcapod.upath"``.
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

- [ ] **Step 4: Update `LogicalUUID` class — class attributes and docstrings**

Replace the `LogicalUUID` class definition (lines 164–236) with:

```python
class LogicalUUID:
    """Logical type for ``uuid.UUID``.

    Stores UUIDs as Arrow binary (16 bytes) using the custom extension type
    ``"orcapod.uuid"``. Both the Arrow extension name and ``logical_type_name``
    are ``"orcapod.uuid"``, consistent with ``LogicalPath`` and ``LogicalUPath``.

    The extension name ``"orcapod.uuid"`` is Orcapod-owned and stable, replacing
    the previous ``"uuid.UUID"`` name that mirrored PyArrow's ``"arrow.uuid"``
    territory. Use ``orcapod.UUID`` (a top-level alias for ``uuid.UUID``) as the
    preferred way to reference this type in user code.

    The storage type is ``pa.large_binary()`` (variable-length binary), using
    big-endian byte order as returned by ``uuid.UUID.bytes``. ``large_binary``
    is used rather than ``pa.binary(16)`` (fixed-size) because Polars maps
    fixed-size binary to variable-length on the round-trip, which would
    conflict with the deserializer's storage type check.

    Example:
        >>> import uuid
        >>> lt = LogicalUUID()
        >>> u = uuid.uuid4()
        >>> lt.storage_to_python(lt.python_to_storage(u)) == u
        True
    """

    _arrow_ext_class = make_arrow_extension_type("orcapod.uuid", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("orcapod.uuid", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "orcapod.uuid"
    python_type: type = _uuid_module.UUID

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for ``uuid.UUID``.

        Returns:
            A cached ``pa.ExtensionType`` instance with extension name
            ``"orcapod.uuid"`` and storage type ``pa.large_binary()``.
        """
        if LogicalUUID._arrow_ext is None:
            LogicalUUID._arrow_ext = LogicalUUID._arrow_ext_class()
        return LogicalUUID._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for ``uuid.UUID``.

        Returns:
            A cached ``pl.BaseExtension`` instance registered under
            ``"orcapod.uuid"``.
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

- [ ] **Step 5: Run the failing tests to confirm they are now green**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v --tb=short 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/builtin_logical_types.py \
        tests/test_extension_types/test_builtin_logical_types.py
git commit -m "feat(extension_types): rename built-in extension types to orcapod.* namespace

LogicalPath: 'pathlib.Path' -> 'orcapod.path'
LogicalUPath: 'upath.UPath' -> 'orcapod.upath'
LogicalUUID: 'uuid.UUID' -> 'orcapod.uuid'

Orcapod now owns the canonical extension identity for all three built-in
types, decoupling on-disk names from upstream library module paths."
```

---

## Task 3: Add tests for top-level `orcapod.Path`, `orcapod.UPath`, `orcapod.UUID` aliases

TDD red step for the alias feature. These tests will fail until Task 4 adds the aliases.

**Files:**
- Modify: `tests/test_extension_types/test_builtin_logical_types.py`

- [ ] **Step 1: Append the alias test block at the end of the test file**

Add the following to the end of `tests/test_extension_types/test_builtin_logical_types.py`:

```python
# ---------------------------------------------------------------------------
# Top-level orcapod namespace alias tests
# ---------------------------------------------------------------------------


def test_orcapod_path_alias_is_pathlib_path():
    """orcapod.Path is the same object as pathlib.Path."""
    import pathlib

    import orcapod

    assert orcapod.Path is pathlib.Path


def test_orcapod_upath_alias_is_upath_upath():
    """orcapod.UPath is the same object as upath.UPath."""
    from upath import UPath

    import orcapod

    assert orcapod.UPath is UPath


def test_orcapod_uuid_alias_is_uuid_uuid():
    """orcapod.UUID is the same object as uuid.UUID."""
    import uuid

    import orcapod

    assert orcapod.UUID is uuid.UUID


def test_orcapod_path_alias_in_all():
    """orcapod.Path appears in orcapod.__all__."""
    import orcapod

    assert "Path" in orcapod.__all__


def test_orcapod_upath_alias_in_all():
    """orcapod.UPath appears in orcapod.__all__."""
    import orcapod

    assert "UPath" in orcapod.__all__


def test_orcapod_uuid_alias_in_all():
    """orcapod.UUID appears in orcapod.__all__."""
    import orcapod

    assert "UUID" in orcapod.__all__
```

- [ ] **Step 2: Run the new tests to confirm they are red**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v -k "alias" --tb=short 2>&1
```

Expected: 6 tests fail with `AttributeError: module 'orcapod' has no attribute 'Path'` (or similar).

---

## Task 4: Add `Path`, `UPath`, `UUID` aliases to `src/orcapod/__init__.py`

**Files:**
- Modify: `src/orcapod/__init__.py`

- [ ] **Step 1: Add the alias imports and `__all__` entries**

Replace the entire content of `src/orcapod/__init__.py` with:

```python
from .config import (
    DEFAULT_CONFIG,
    DisplayConfig,
    HashingConfig,
    OrcapodConfig,
    load_config,
)
from .core.function_pod import (
    FunctionPod,
    function_pod,
)
from .core.nodes.source_node import SourceNode
from .pipeline import Pipeline, PipelineJob
from .semantic_types.dataclass_encoding import register_dataclass

# Subpackage re-exports for clean public API
from . import databases  # noqa: F401
from . import nodes  # noqa: F401
from . import operators  # noqa: F401
from . import sources  # noqa: F401
from . import streams  # noqa: F401
from . import types  # noqa: F401

# Stable type aliases — preferred over importing directly from pathlib/upath/uuid.
#
# These aliases are the recommended way to reference these types in orcapod user code.
# Even if an upstream library is renamed or restructured, these symbols remain stable
# at ``orcapod.Path``, ``orcapod.UPath``, and ``orcapod.UUID``. Their Arrow extension
# types are registered under the ``orcapod.*`` namespace (``"orcapod.path"``,
# ``"orcapod.upath"``, ``"orcapod.uuid"``), so on-disk identity is also decoupled
# from upstream module paths.
from pathlib import Path
from upath import UPath
from uuid import UUID

__all__ = [
    "DEFAULT_CONFIG",
    "DisplayConfig",
    "HashingConfig",
    "OrcapodConfig",
    "load_config",
    "FunctionPod",
    "function_pod",
    "Pipeline",
    "PipelineJob",
    "SourceNode",
    "register_dataclass",
    "databases",
    "nodes",
    "operators",
    "sources",
    "streams",
    "types",
    # Stable type aliases
    "Path",
    "UPath",
    "UUID",
]
```

- [ ] **Step 2: Run the alias tests to confirm they are now green**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v -k "alias" --tb=short 2>&1
```

Expected: all 6 alias tests pass.

- [ ] **Step 3: Run the full builtin logical types test suite**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v --tb=short 2>&1 | tail -20
```

Expected: all tests pass (the full suite, not just alias tests).

- [ ] **Step 4: Run the broader extension_types test suite to check for regressions**

```bash
uv run pytest tests/test_extension_types/ -v --tb=short 2>&1 | tail -30
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/__init__.py \
        tests/test_extension_types/test_builtin_logical_types.py
git commit -m "feat(orcapod): expose Path, UPath, UUID as stable top-level aliases

Adds orcapod.Path, orcapod.UPath, orcapod.UUID as re-exports of
pathlib.Path, upath.UPath, and uuid.UUID respectively. These are the
preferred symbols for user code — stable even if upstream libraries
rename their types or module paths."
```

---

## Task 5: Final verification — full test suite

- [ ] **Step 1: Run the complete test suite**

```bash
uv run pytest tests/ -x --tb=short 2>&1 | tail -40
```

Expected: all tests pass (no regressions in any other test module).

- [ ] **Step 2: Verify the branch is clean and ready for PR**

```bash
git status
git log --oneline origin/extension-type-system..HEAD
```

Expected: 2 commits ahead of `extension-type-system`, working tree clean.

---

## Self-Review Checklist

**Spec coverage:**

| Requirement | Task that covers it |
|-------------|-------------------|
| `LogicalPath` registers under `"orcapod.path"` | Task 2 Step 2 |
| `LogicalUPath` registers under `"orcapod.upath"` | Task 2 Step 3 |
| `LogicalUUID` registers under `"orcapod.uuid"` | Task 2 Step 4 |
| `orcapod.uuid` no longer conflicts with `arrow.uuid` | Task 2 Step 4 (new name `"orcapod.uuid"` vs PyArrow's `"arrow.uuid"`) |
| `orcapod.Path` alias exposed at top-level | Task 4 Step 1 |
| `orcapod.UPath` alias exposed at top-level | Task 4 Step 1 |
| `orcapod.UUID` alias exposed at top-level | Task 4 Step 1 |
| Aliases documented as preferred + stability rationale | Task 4 Step 1 (comment block) |
| Stability rationale in module docstring | Task 2 Step 1 |
| Existing round-trip behavior continues to work | Task 5 Step 1 |
| Unit tests updated to assert `orcapod.*` names | Task 1 + Task 3 |

**No placeholders:** All steps contain exact code. No "TBD" or "similar to above" references.

**Type consistency:** `logical_type_name` constants and `extension_name` strings are consistent across Tasks 1, 2, 3, and 4 — `"orcapod.path"`, `"orcapod.upath"`, `"orcapod.uuid"` throughout.
