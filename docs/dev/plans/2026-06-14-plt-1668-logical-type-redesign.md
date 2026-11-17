# PLT-1668: LogicalType Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `ExtensionTypeConverter`/`ExtensionTypeRegistry` with `LogicalType`/`LogicalTypeRegistry` so that each logical type owns its Arrow and Polars extension types directly via `get_arrow_extension_type()` / `get_polars_extension_type()`, and the registry enforces a three-way binding triplet `(logical_type_name, arrow_ext_name, python_type)`.

**Architecture:** The `LogicalType` protocol gains two new methods (`get_arrow_extension_type`, `get_polars_extension_type`) and loses three flat properties (`extension_name`, `extension_metadata`, `storage_type`). The registry drops module-level shadow dicts entirely — uniqueness is enforced per-instance via three internal dicts. A new `make_arrow_extension_type(extension_name, storage_type, metadata) -> type[pa.ExtensionType]` helper replaces the dynamic synthesis that previously lived inside the registry.

**Tech Stack:** Python 3.12+, PyArrow ≥ 20, Polars ≥ 1.36.0, pytest, uv.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/protocols.py` | Rewrite | `LogicalType` protocol |
| `src/orcapod/extension_types/registry.py` | Rewrite | `make_arrow_extension_type` helper + `LogicalTypeRegistry` |
| `src/orcapod/extension_types/__init__.py` | Update | Export new names + `default_logical_type_registry` |
| `src/orcapod/extension_types/schema_walker.py` | **No change** | Self-contained; no protocol imports |
| `tests/test_extension_types/test_protocols.py` | Rewrite | Protocol conformance tests |
| `tests/test_extension_types/test_registry.py` | Rewrite | Stub helpers + all registry tests |

---

### Task 1: Replace `ExtensionTypeConverter` with `LogicalType` in `protocols.py`

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`

- [ ] **Step 1: Overwrite `protocols.py` with the `LogicalType` protocol**

```python
# src/orcapod/extension_types/protocols.py
"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``LogicalType`` — the contract for all implementations
that bind a Python class to its Arrow and Polars extension type representation.

Note:
    This module is part of the parallel-build phase. The old
    ``SemanticStructConverterProtocol`` in ``protocols/semantic_types_protocols.py``
    is untouched; it is removed in PLT-1660.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


@runtime_checkable
class LogicalType(Protocol):
    """Protocol for Arrow/Polars extension-type-backed logical types.

    A ``LogicalType`` is a three-way binding between a unique logical type name
    (orcapod's identifier), a Python class, and Arrow/Polars extension types.
    Each implementation *owns* its Arrow and Polars extension types by providing
    them directly via ``get_arrow_extension_type`` and ``get_polars_extension_type``.

    This protocol is Arrow I/O only — hashing is not a logical type responsibility.
    """

    @property
    def logical_type_name(self) -> str:
        """Unique orcapod identifier for this logical type.

        By convention the Python FQCN (e.g. ``"uuid.UUID"``), but any unique
        string is valid. Does NOT need to match the Arrow extension type name.
        """
        ...

    @property
    def python_type(self) -> type:
        """The Python class this logical type represents."""
        ...

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for this logical type.

        ``storage_type``, ``extension_name``, and serialised metadata are
        encapsulated inside the returned type; they are no longer top-level
        properties on ``LogicalType``.

        For custom types: create and return an instance of a new
        ``pa.ExtensionType`` subclass (e.g. via ``make_arrow_extension_type``).
        For pre-existing types: return the existing instance directly
        (e.g. ``pa.uuid()``).
        """
        ...

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return an instance of the Polars extension type for this logical type.

        The registry calls ``type(instance)`` to obtain the class passed to
        ``pl.register_extension_type``.
        """
        ...

    def python_to_storage(self, value: Any) -> Any:
        """Convert a Python value to its Arrow storage representation.

        Args:
            value: A Python object of type ``python_type``.

        Returns:
            A value suitable for use as an Arrow scalar or array element
            matching the storage type of ``get_arrow_extension_type()``.
        """
        ...

    def storage_to_python(self, storage_value: Any) -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or array element from the Arrow storage array.

        Returns:
            A Python object of type ``python_type``.
        """
        ...
```

- [ ] **Step 2: Verify old protocol tests now fail**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: FAIL — `ExtensionTypeConverter` import error and protocol checks fail.

---

### Task 2: Update `test_protocols.py` for the new `LogicalType` protocol

**Files:**
- Modify: `tests/test_extension_types/test_protocols.py`

- [ ] **Step 1: Overwrite `test_protocols.py`**

```python
# tests/test_extension_types/test_protocols.py
"""Tests for LogicalType protocol."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from orcapod.extension_types.protocols import LogicalType
from orcapod.extension_types.registry import make_arrow_extension_type


_StubArrowExtClass = make_arrow_extension_type(
    "test.module.MyType", pa.large_string(), b"test.category"
)


class _StubLogicalType:
    """Minimal conforming implementation of LogicalType for use in tests."""

    @property
    def logical_type_name(self) -> str:
        return "test.module.MyType"

    @property
    def python_type(self) -> type:
        return str

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return _StubArrowExtClass()

    def get_polars_extension_type(self) -> pl.BaseExtension:
        class _StubPL(pl.BaseExtension):
            def __init__(self) -> None:
                super().__init__("test.module.MyType", pl.String, None)

            @classmethod
            def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
                return cls()

        return _StubPL()

    def python_to_storage(self, value):
        return str(value)

    def storage_to_python(self, storage_value):
        return storage_value


def test_protocol_is_importable():
    """LogicalType can be imported from extension_types.protocols."""
    assert LogicalType is not None


def test_protocol_defines_required_members():
    """A conforming class is recognized as a LogicalType instance."""
    assert isinstance(_StubLogicalType(), LogicalType)


def test_conforming_class_satisfies_protocol():
    """A class implementing all required members works correctly via the protocol interface."""
    lt: LogicalType = _StubLogicalType()
    assert lt.logical_type_name == "test.module.MyType"
    assert lt.python_type is str
    assert lt.get_arrow_extension_type().extension_name == "test.module.MyType"
    assert isinstance(lt.get_polars_extension_type(), pl.BaseExtension)
    assert lt.python_to_storage(42) == "42"
    assert lt.storage_to_python("hello") == "hello"
```

Note: `make_arrow_extension_type` is imported from `registry.py` — this task depends on Task 3 below having the helper in place before this test file is runnable. Write the file now; run after Task 3.

---

### Task 3: Add `make_arrow_extension_type` and `LogicalTypeRegistry` to `registry.py`

**Files:**
- Modify: `src/orcapod/extension_types/registry.py`

- [ ] **Step 1: Overwrite `registry.py` with the new implementation**

```python
# src/orcapod/extension_types/registry.py
"""Registry for LogicalType instances.

Registering a logical type automatically registers the corresponding
extension type in both PyArrow's and Polars' global registries.
"""

from __future__ import annotations

import re

import polars as pl
import pyarrow as pa

from orcapod.extension_types.protocols import LogicalType


def _sanitize(name: str) -> str:
    """Replace non-alphanumeric characters with underscores.

    Used to produce a valid Python identifier for the dynamically created
    ``pa.ExtensionType`` subclass name.
    """
    return re.sub(r"[^A-Za-z0-9]", "_", name)


def make_arrow_extension_type(
    extension_name: str,
    storage_type: pa.DataType,
    metadata: bytes | None = None,
) -> type[pa.ExtensionType]:
    """Synthesise and return a ``pa.ExtensionType`` subclass.

    Returns the *class*, not an instance — callers instantiate it inside their
    ``get_arrow_extension_type()`` implementation. Returning the class preserves
    the option to create multiple instances or future parameterised variants from
    the same class.

    This is a low-level building block. The full pattern for binding a Python
    type to a specific Arrow/Polars representation — the extension type factory —
    is the responsibility of each ``LogicalType`` implementation. See PLT-1656
    for the built-in implementations (``Path``, ``UPath``, ``UUID``).

    Args:
        extension_name: The Arrow extension name (``ARROW:extension:name``).
        storage_type: The underlying Arrow storage type.
        metadata: Optional bytes stored as ``ARROW:extension:metadata``.
            Defaults to ``None`` (serialised as empty bytes).

    Returns:
        A ``pa.ExtensionType`` subclass. Call it with no arguments to obtain
        an instance suitable for passing to ``pa.register_extension_type`` or
        returning from ``get_arrow_extension_type()``.
    """
    _name, _storage, _metadata = extension_name, storage_type, metadata or b""
    return type(
        f"_ArrowExt_{_sanitize(extension_name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            # __arrow_ext_deserialize__ reconstructs the type descriptor from schema
            # metadata (called once per IPC/Parquet read, not per value). The storage
            # type and metadata are baked into the constructor via closure, so
            # arguments are intentionally ignored.
            "__arrow_ext_deserialize__": classmethod(
                lambda cls, storage_type, serialized: cls()
            ),
        },
    )


class LogicalTypeRegistry:
    """Registry for ``LogicalType`` instances.

    Maintains a three-way binding: ``(logical_type_name, arrow_extension_name,
    python_type)`` → ``LogicalType``. Each key participates in at most one
    binding within a registry instance.

    Registering a logical type side-effect-registers the corresponding extension
    type in PyArrow's and Polars' global registries. Pre-existing types (those
    already registered externally, e.g. PyArrow's built-in ``"arrow.uuid"``) are
    accepted silently — the binding is stored without error.

    The process-global ``default_logical_type_registry`` instance provides
    effective process-wide uniqueness for normal use. Thread-safety is deferred.

    Example:
        >>> registry = LogicalTypeRegistry()
        >>> registry.register(my_logical_type)
        >>> lt = registry.get_by_logical_name("uuid.UUID")
    """

    def __init__(self) -> None:
        self._by_logical_name: dict[str, LogicalType] = {}
        self._by_arrow_name: dict[str, LogicalType] = {}
        self._by_python_type: dict[type, LogicalType] = {}

    def register(self, logical_type: LogicalType) -> None:
        """Register *logical_type* and its PyArrow/Polars extension types.

        Args:
            logical_type: A ``LogicalType`` instance to register.

        Raises:
            ValueError: If any of the three keys (``logical_type_name``,
                Arrow extension name, ``python_type``) is already bound to a
                *different* ``LogicalType`` in this registry.
        """
        arrow_ext_name = logical_type.get_arrow_extension_type().extension_name
        py_type = logical_type.python_type
        logical_name = logical_type.logical_type_name

        existing_by_logical = self._by_logical_name.get(logical_name)
        existing_by_arrow = self._by_arrow_name.get(arrow_ext_name)
        existing_by_python = self._by_python_type.get(py_type)

        # Triplet conflict check: raise if any key is bound to a different instance.
        for existing, label, key in [
            (existing_by_logical, "logical_type_name", logical_name),
            (existing_by_arrow, "arrow_extension_name", arrow_ext_name),
            (existing_by_python, "python_type", py_type.__qualname__),
        ]:
            if existing is not None and existing is not logical_type:
                raise ValueError(
                    f"Cannot register logical type '{logical_name}': "
                    f"{label} {key!r} is already bound to "
                    f"'{existing.logical_type_name}'."
                )

        # Idempotent check: all three keys already bound to this same instance.
        if (
            existing_by_logical is logical_type
            and existing_by_arrow is logical_type
            and existing_by_python is logical_type
        ):
            return

        # Register Arrow extension type. ArrowKeyError means the name is already
        # in PyArrow's global registry (pre-existing type or another registry
        # instance). Accept silently — PLT-1669 adds post-error validation.
        try:
            pa.register_extension_type(logical_type.get_arrow_extension_type())
        except pa.lib.ArrowKeyError:
            pass

        # Register Polars extension type. ValueError means already registered.
        polars_ext_class = type(logical_type.get_polars_extension_type())
        try:
            pl.register_extension_type(arrow_ext_name, polars_ext_class)
        except ValueError:
            pass

        # Store three-way binding.
        self._by_logical_name[logical_name] = logical_type
        self._by_arrow_name[arrow_ext_name] = logical_type
        self._by_python_type[py_type] = logical_type

    def get_by_logical_name(self, name: str) -> LogicalType | None:
        """Return the logical type registered under *name*, or ``None``."""
        return self._by_logical_name.get(name)

    def get_by_python_type(self, python_type: type) -> LogicalType | None:
        """Return the logical type for *python_type*, or ``None``.

        Checks exact match first, then falls back to an ``issubclass`` scan.
        When multiple registered types are superclasses of *python_type*, the
        one registered first wins (insertion-order dict, Python 3.7+).
        """
        lt = self._by_python_type.get(python_type)
        if lt is not None:
            return lt
        for registered_type, lt in self._by_python_type.items():
            if issubclass(python_type, registered_type):
                return lt
        return None

    def get_by_arrow_extension_name(self, arrow_name: str) -> LogicalType | None:
        """Return the logical type registered under *arrow_name*, or ``None``."""
        return self._by_arrow_name.get(arrow_name)
```

- [ ] **Step 2: Run protocol tests (both tasks together)**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: All 3 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/extension_types/protocols.py \
        src/orcapod/extension_types/registry.py \
        tests/test_extension_types/test_protocols.py
git commit -m "feat(extension_types): add LogicalType protocol and LogicalTypeRegistry (PLT-1668)"
```

---

### Task 4: Rework `test_registry.py` — stubs + basic tests

**Files:**
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Replace the imports and stub helpers at the top of `test_registry.py`**

Replace everything from the top of the file through the `_make_stub` function definition (roughly lines 1–65 in the original) with:

```python
"""Tests for LogicalTypeRegistry."""

from __future__ import annotations

import pathlib
import tempfile
import uuid
import warnings

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orcapod.extension_types.protocols import LogicalType
from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique extension/logical name to avoid cross-test global-registry collisions."""
    return f"test.registry.{uuid.uuid4().hex[:8]}"


def _make_stub(
    logical_name: str | None = None,
    arrow_name: str | None = None,
    storage: pa.DataType | None = None,
    metadata: bytes | None = b"test.category",
    py_type: type = str,
) -> LogicalType:
    """Factory for minimal LogicalType conforming stubs.

    ``arrow_name`` defaults to ``logical_name`` when omitted. Pass separate
    values to test cases that need a distinct Arrow extension name.
    """
    _logical_name = logical_name or _unique_name()
    _arrow_name = arrow_name or _logical_name
    _storage = storage if storage is not None else pa.large_utf8()
    _ArrowExt = make_arrow_extension_type(_arrow_name, _storage, metadata)
    _pl_storage = pl.from_arrow(pa.array([], type=_storage)).dtype
    _meta_str = metadata.decode("utf-8") if metadata else None

    class _StubPL(pl.BaseExtension):
        def __init__(self) -> None:
            super().__init__(_arrow_name, _pl_storage, _meta_str)

        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _Stub:
        @property
        def logical_type_name(self) -> str:
            return _logical_name

        @property
        def python_type(self) -> type:
            return py_type

        def get_arrow_extension_type(self) -> pa.ExtensionType:
            return _ArrowExt()

        def get_polars_extension_type(self) -> pl.BaseExtension:
            return _StubPL()

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()
```

- [ ] **Step 2: Replace all basic/lookup/PA/Polars/module-level tests (lines 70–436 in the original) with the updated equivalents**

Remove all tests that reference removed methods (`has_extension_name`, `has_python_type`, `list_extension_names`, `list_python_types`, `get_converter_for_name`, `get_converter_for_python_type`). Replace with:

```python
# ---------------------------------------------------------------------------
# Basic registration tests
# ---------------------------------------------------------------------------

def test_register_stores_three_way_binding():
    """After register(), all three lookup methods return the registered LogicalType."""
    stub = _make_stub()
    registry = LogicalTypeRegistry()
    registry.register(stub)

    arrow_name = stub.get_arrow_extension_type().extension_name
    assert registry.get_by_logical_name(stub.logical_type_name) is stub
    assert registry.get_by_arrow_extension_name(arrow_name) is stub
    assert registry.get_by_python_type(stub.python_type) is stub


def test_get_by_logical_name_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_logical_name("does.not.exist") is None


def test_get_by_python_type_exact():
    registry = LogicalTypeRegistry()
    stub = _make_stub(py_type=bytes)
    registry.register(stub)
    assert registry.get_by_python_type(bytes) is stub


def test_get_by_python_type_subclass():
    class _Base:
        pass

    class _Child(_Base):
        pass

    registry = LogicalTypeRegistry()
    stub = _make_stub(py_type=_Base)
    registry.register(stub)
    assert registry.get_by_python_type(_Child) is stub


def test_get_by_python_type_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_python_type(int) is None


def test_get_by_arrow_extension_name_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_arrow_extension_name("does.not.exist") is None


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------

def test_register_idempotent_same_instance():
    """Registering the same LogicalType object twice is a no-op."""
    stub = _make_stub()
    registry = LogicalTypeRegistry()
    registry.register(stub)
    registry.register(stub)  # should not raise
    assert registry.get_by_logical_name(stub.logical_type_name) is stub


# ---------------------------------------------------------------------------
# Triplet conflict tests
# ---------------------------------------------------------------------------

def test_triplet_conflict_same_logical_name_raises():
    """Two LogicalTypes sharing logical_type_name -> ValueError."""
    logical_name = _unique_name()
    stub1 = _make_stub(logical_name=logical_name, py_type=str)
    stub2 = _make_stub(logical_name=logical_name, py_type=int)

    registry = LogicalTypeRegistry()
    registry.register(stub1)
    with pytest.raises(ValueError, match=logical_name):
        registry.register(stub2)


def test_triplet_conflict_same_arrow_name_raises():
    """Two LogicalTypes sharing Arrow extension name -> ValueError."""
    shared_arrow_name = _unique_name()
    stub1 = _make_stub(arrow_name=shared_arrow_name, py_type=str)
    stub2 = _make_stub(arrow_name=shared_arrow_name, py_type=int)

    registry = LogicalTypeRegistry()
    registry.register(stub1)
    with pytest.raises(ValueError, match=shared_arrow_name):
        registry.register(stub2)


def test_triplet_conflict_same_python_type_raises():
    """Two LogicalTypes sharing python_type -> ValueError."""
    stub1 = _make_stub(py_type=float)
    stub2 = _make_stub(py_type=float)

    registry = LogicalTypeRegistry()
    registry.register(stub1)
    with pytest.raises(ValueError, match="float"):
        registry.register(stub2)


# ---------------------------------------------------------------------------
# Pre-existing type tolerance tests
# ---------------------------------------------------------------------------

def test_register_preexisting_arrow_type_succeeds():
    """ArrowKeyError from PA global registry is accepted silently; binding is stored."""
    name = _unique_name()

    class _ExternalPA(pa.ExtensionType):
        def __init__(self) -> None:
            pa.ExtensionType.__init__(self, pa.large_utf8(), name)

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    pa.register_extension_type(_ExternalPA())  # pre-register externally

    stub = _make_stub(arrow_name=name)
    registry = LogicalTypeRegistry()
    registry.register(stub)  # must not raise

    assert registry.get_by_logical_name(stub.logical_type_name) is stub
    assert registry.get_by_arrow_extension_name(name) is stub
    assert registry.get_by_python_type(stub.python_type) is stub


def test_register_preexisting_polars_type_succeeds():
    """ValueError from Polars global registry is accepted silently; binding is stored."""
    name = _unique_name()

    # Pre-register in PA first to avoid PA-level conflict
    class _ExternalPA(pa.ExtensionType):
        def __init__(self) -> None:
            pa.ExtensionType.__init__(self, pa.large_utf8(), name)

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    pa.register_extension_type(_ExternalPA())

    class _ExternalPL(pl.BaseExtension):
        def __init__(self) -> None:
            super().__init__(name, pl.String, None)

        @classmethod
        def ext_from_params(cls, n, s, m):
            return cls()

    pl.register_extension_type(name, _ExternalPL)

    stub = _make_stub(arrow_name=name)
    registry = LogicalTypeRegistry()
    registry.register(stub)  # must not raise

    assert registry.get_by_logical_name(stub.logical_type_name) is stub
    assert registry.get_by_arrow_extension_name(name) is stub
    assert registry.get_by_python_type(stub.python_type) is stub


# ---------------------------------------------------------------------------
# PyArrow global registry: our type gets registered
# ---------------------------------------------------------------------------

def test_register_populates_arrow_global_registry():
    """After register(), PA global registry contains the extension type."""
    stub = _make_stub()
    registry = LogicalTypeRegistry()
    registry.register(stub)

    arrow_name = stub.get_arrow_extension_type().extension_name

    class _Probe(pa.ExtensionType):
        def __init__(self) -> None:
            pa.ExtensionType.__init__(self, pa.large_utf8(), arrow_name)

        def __arrow_ext_serialize__(self):
            return b""

        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    with pytest.raises(pa.lib.ArrowKeyError):
        pa.register_extension_type(_Probe())
```

- [ ] **Step 3: Run the basic + idempotency + triplet + pre-existing tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v -k "not round_trip and not parquet and not module_instance"
```

Expected: All newly written tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_registry.py
git commit -m "test(extension_types): rework test_registry for LogicalTypeRegistry (PLT-1668)"
```

---

### Task 5: Update end-to-end tests in `test_registry.py`

**Files:**
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Replace the `_Color`, `_make_color_converter`, `_build_ext_array`, and end-to-end test functions**

Remove the old `_Color` / `_make_color_converter` / `_build_ext_array` block and the three round-trip tests. Replace with:

```python
# ---------------------------------------------------------------------------
# End-to-end helpers
# ---------------------------------------------------------------------------

class _Color:
    """Minimal Python class used to exercise the logical type contract end-to-end."""

    def __init__(self, hex_str: str) -> None:
        self.hex_str = hex_str

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Color) and self.hex_str == other.hex_str

    def __repr__(self) -> str:
        return f"Color({self.hex_str!r})"


def _make_color_logical_type() -> LogicalType:
    """LogicalType for _Color, backed by pa.large_utf8() storage."""
    _name = _unique_name()
    _ArrowExt = make_arrow_extension_type(_name, pa.large_utf8(), b"test.color")
    _pl_storage = pl.from_arrow(pa.array([], type=pa.large_utf8())).dtype

    class _ColorPL(pl.BaseExtension):
        def __init__(self) -> None:
            super().__init__(_name, _pl_storage, "test.color")

        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _ColorLogicalType:
        @property
        def logical_type_name(self) -> str:
            return _name

        @property
        def python_type(self) -> type:
            return _Color

        def get_arrow_extension_type(self) -> pa.ExtensionType:
            return _ArrowExt()

        def get_polars_extension_type(self) -> pl.BaseExtension:
            return _ColorPL()

        def python_to_storage(self, value: _Color) -> str:
            return value.hex_str

        def storage_to_python(self, storage_value: str) -> _Color:
            return _Color(storage_value)

    return _ColorLogicalType()


def _build_ext_array(lt: LogicalType, values: list) -> pa.Array:
    """Build a PA extension array from Python values using the logical type."""
    arrow_ext = lt.get_arrow_extension_type()
    storage_values = [lt.python_to_storage(v) for v in values]
    storage_arr = pa.array(storage_values, type=arrow_ext.storage_type)
    return storage_arr.cast(arrow_ext)


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------

def test_python_class_round_trip():
    """Python objects -> Arrow extension array -> Python objects via logical type methods."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    originals = [_Color("#ff0000"), _Color("#00ff00"), _Color("#0000ff")]
    ext_arr = _build_ext_array(lt, originals)

    recovered = [lt.storage_to_python(v.as_py()) for v in ext_arr.storage]
    assert recovered == originals


def test_arrow_polars_round_trip():
    """PA ext array -> pl.from_arrow -> to_arrow() preserves extension type and values."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    originals = [_Color("#aabbcc"), _Color("#112233")]
    ext_arr = _build_ext_array(lt, originals)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pl_series = pl.from_arrow(ext_arr)

    arrow_name = lt.get_arrow_extension_type().extension_name
    assert isinstance(pl_series.dtype, pl.BaseExtension)
    assert pl_series.dtype.ext_name() == arrow_name

    arr_back = pl_series.to_arrow()
    assert arr_back.type.extension_name == arrow_name

    recovered = [lt.storage_to_python(v.as_py()) for v in arr_back.storage]
    assert recovered == originals


def test_parquet_round_trip():
    """PA ext array -> Parquet -> read back; extension type and values preserved."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    originals = [_Color("#deadbe"), _Color("#cafeba")]
    ext_arr = _build_ext_array(lt, originals)
    schema = pa.schema([pa.field("color", ext_arr.type), pa.field("id", pa.int32())])
    table = pa.table(
        {"color": ext_arr, "id": pa.array([1, 2], type=pa.int32())},
        schema=schema,
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "test.parquet"
        pq.write_table(table, path)
        table_back = pq.read_table(path)

    arrow_name = lt.get_arrow_extension_type().extension_name
    assert table_back.schema.field("color").type.extension_name == arrow_name
    storage_arr = table_back.column("color").combine_chunks().storage
    recovered = [lt.storage_to_python(v.as_py()) for v in storage_arr]
    assert recovered == originals


# ---------------------------------------------------------------------------
# Module-level instance test
# ---------------------------------------------------------------------------

def test_logical_type_registry_module_instance():
    """extension_types.default_logical_type_registry is a LogicalTypeRegistry, starts empty."""
    from orcapod import extension_types

    assert isinstance(extension_types.default_logical_type_registry, LogicalTypeRegistry)
    # PLT-1668 scope: no built-in logical types registered yet (that is PLT-1656).
    assert extension_types.default_logical_type_registry.get_by_logical_name("uuid.UUID") is None
```

- [ ] **Step 2: Run all registry tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_registry.py
git commit -m "test(extension_types): add end-to-end and module-instance tests for LogicalTypeRegistry (PLT-1668)"
```

---

### Task 6: Update `__init__.py` exports

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`

- [ ] **Step 1: Overwrite `__init__.py`**

```python
# src/orcapod/extension_types/__init__.py
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that bind
Python classes to their Arrow and Polars extension type representation.

The module-level ``default_logical_type_registry`` instance is the process default.
Built-in registrations (``Path``, ``UPath``, ``UUID``) are added by PLT-1656.
``DataContext`` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import LogicalType
from .registry import LogicalTypeRegistry, make_arrow_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

default_logical_type_registry = LogicalTypeRegistry()

__all__ = [
    "LogicalType",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

- [ ] **Step 2: Run the full `test_extension_types` suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: All tests in `test_protocols.py`, `test_registry.py`, and `test_schema_walker.py` PASS.

- [ ] **Step 3: Run the complete test suite to catch any regressions**

```bash
uv run pytest --tb=short -q
```

Expected: All tests pass. No references to `ExtensionTypeConverter`, `ExtensionTypeRegistry`, or `default_extension_type_registry` remain outside of the deleted/replaced files.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "feat(extension_types): update __init__ exports for LogicalType redesign (PLT-1668)"
```

---

## Self-Review Checklist

After completing all tasks, verify:

- [ ] `LogicalType` has exactly 6 members: `logical_type_name`, `python_type`, `get_arrow_extension_type`, `get_polars_extension_type`, `python_to_storage`, `storage_to_python`
- [ ] `LogicalTypeRegistry` has exactly 3 lookup methods: `get_by_logical_name`, `get_by_python_type`, `get_by_arrow_extension_name`
- [ ] No reference to `ExtensionTypeConverter`, `ExtensionTypeRegistry`, `default_extension_type_registry`, `_ARROW_REGISTRY`, `_POLARS_REGISTRY`, `_register_arrow_ext_type`, or `_register_polars_ext_type` remains anywhere in `src/` or `tests/`
- [ ] `make_arrow_extension_type` returns `type[pa.ExtensionType]` (a class, not an instance)
- [ ] Triplet conflict error messages include the conflicting key name so `pytest.raises(ValueError, match=<key>)` works
- [ ] Pre-existing-type tests pre-register externally then call `registry.register()` — the call must not raise
- [ ] `test_schema_walker.py` still passes unchanged
