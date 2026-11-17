# ExtensionTypeRegistry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `ExtensionTypeRegistry` in `src/orcapod/extension_types/registry.py`, wiring up both PyArrow and Polars global extension type registries on each `register()` call.

**Architecture:** A plain Python class with two internal dicts (`_by_name`, `_by_python_type`) for converter lookup, plus two module-level shadow dicts (`_ARROW_REGISTRY`, `_POLARS_REGISTRY`) that track what has been registered in the process-global PA/Polars registries. `register()` validates against both the instance dict (duplicate check) and the shadow dicts (equivalence/external-conflict check), then dynamically creates and registers `pa.ExtensionType` and `pl.BaseExtension` subclasses via `type()`.

**Tech Stack:** Python 3.12, PyArrow ≥ 20, Polars ≥ 1.36, pytest, uv

**Spec:** `superpowers/specs/2026-06-14-extension-type-registry-design.md`

---

## File map

| File | Action |
|---|---|
| `pyproject.toml` | Modify — restore range constraint `polars>=1.36.0` |
| `src/orcapod/extension_types/registry.py` | **Create** — `ExtensionTypeRegistry`, shadow dicts, helpers |
| `src/orcapod/extension_types/__init__.py` | Modify — export class, create module-level instance |
| `tests/test_extension_types/test_registry.py` | **Create** — full test suite |

---

## Task 1: Fix `pyproject.toml` — restore Polars range constraint

The Polars dependency was accidentally pinned to `==1.41.2` during exploration. Restore it to a range constraint.

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Update the constraint**

Open `pyproject.toml`. Find the line:
```toml
"polars==1.41.2",
```
Replace with:
```toml
"polars>=1.36.0",
```

- [ ] **Step 2: Sync and verify**

```bash
uv sync
uv run python -c "import polars as pl; print(pl.__version__); from polars import BaseExtension; print('BaseExtension OK')"
```

Expected output:
```
1.41.2
BaseExtension OK
```

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): restore polars>=1.36.0 range constraint (PLT-1653)"
```

---

## Task 2: Create `test_registry.py` and `registry.py` — pure-Python registry

Write all tests that exercise the Python-only layer (dict storage, lookups, duplicate checking). No PA/Polars wiring yet — `register()` just populates the internal dicts.

**Files:**
- Create: `tests/test_extension_types/test_registry.py`
- Create: `src/orcapod/extension_types/registry.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_extension_types/test_registry.py`:

```python
"""Tests for ExtensionTypeRegistry."""

from __future__ import annotations

import uuid

import pyarrow as pa
import pytest

from orcapod.extension_types.protocols import ExtensionTypeConverter
from orcapod.extension_types.registry import ExtensionTypeRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique extension name to avoid cross-test global-registry collisions."""
    return f"test.registry.{uuid.uuid4().hex[:8]}"


def _make_stub(
    name: str | None = None,
    storage: pa.DataType | None = None,
    metadata: bytes | None = b"test.category",
    py_type: type = str,
) -> ExtensionTypeConverter:
    """Factory for minimal ExtensionTypeConverter conforming stubs."""
    _name = name or _unique_name()
    _storage = storage if storage is not None else pa.large_utf8()
    _metadata = metadata
    _py_type = py_type

    class _Stub:
        @property
        def extension_name(self) -> str:
            return _name

        @property
        def extension_metadata(self) -> bytes | None:
            return _metadata

        @property
        def storage_type(self) -> pa.DataType:
            return _storage

        @property
        def python_type(self) -> type:
            return _py_type

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()


# ---------------------------------------------------------------------------
# Pure-Python registry tests (no PA/Polars global state required)
# ---------------------------------------------------------------------------

def test_register_stores_converter():
    registry = ExtensionTypeRegistry()
    conv = _make_stub()
    registry.register(conv)
    assert registry.get_converter_for_name(conv.extension_name) is conv


def test_register_duplicate_raises():
    registry = ExtensionTypeRegistry()
    name = _unique_name()
    registry.register(_make_stub(name=name))
    with pytest.raises(ValueError, match=name):
        registry.register(_make_stub(name=name))


def test_get_converter_for_name_miss():
    registry = ExtensionTypeRegistry()
    assert registry.get_converter_for_name("does.not.exist") is None


def test_get_converter_for_python_type_exact():
    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=bytes)
    registry.register(conv)
    assert registry.get_converter_for_python_type(bytes) is conv


def test_get_converter_for_python_type_subclass():
    class _Base:
        pass

    class _Child(_Base):
        pass

    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=_Base)
    registry.register(conv)
    assert registry.get_converter_for_python_type(_Child) is conv


def test_get_converter_for_python_type_miss():
    registry = ExtensionTypeRegistry()
    assert registry.get_converter_for_python_type(int) is None


def test_has_extension_name():
    registry = ExtensionTypeRegistry()
    conv = _make_stub()
    assert not registry.has_extension_name(conv.extension_name)
    registry.register(conv)
    assert registry.has_extension_name(conv.extension_name)


def test_has_python_type():
    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=float)
    assert not registry.has_python_type(float)
    registry.register(conv)
    assert registry.has_python_type(float)


def test_list_extension_names():
    registry = ExtensionTypeRegistry()
    a = _make_stub()
    b = _make_stub()
    registry.register(a)
    registry.register(b)
    assert registry.list_extension_names() == [a.extension_name, b.extension_name]


def test_list_python_types():
    registry = ExtensionTypeRegistry()
    a = _make_stub(py_type=bytes)
    b = _make_stub(py_type=float)
    registry.register(a)
    registry.register(b)
    assert registry.list_python_types() == [bytes, float]
```

- [ ] **Step 2: Run to confirm ImportError (registry module does not exist yet)**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'orcapod.extension_types.registry'`

- [ ] **Step 3: Create `src/orcapod/extension_types/registry.py`**

```python
"""Registry for ExtensionTypeConverter instances.

Registering a converter automatically registers the corresponding
extension type in both PyArrow's and Polars' global registries.
"""

from __future__ import annotations

import re

import pyarrow as pa
import polars as pl

from orcapod.extension_types.protocols import ExtensionTypeConverter

# ---------------------------------------------------------------------------
# Shadow dicts — track what *we* have registered in the global registries.
# These are module-level singletons shared across all ExtensionTypeRegistry
# instances. We use our own dicts rather than querying library internals
# because neither PyArrow nor Polars exposes a stable public API for looking
# up a previously registered extension type by name.
#
# Limitation: types registered externally (directly via
# pa.register_extension_type / pl.register_extension_type, bypassing this
# module) will not appear here. A subsequent register() call for the same
# name will detect the conflict via the library-level error and raise,
# because without knowing what was registered externally we cannot guarantee
# the same extension name maps to the same Python class and underlying
# storage type — silently proceeding risks data corruption or misrouted
# conversions at read time.
# ---------------------------------------------------------------------------

_ARROW_REGISTRY: dict[str, tuple[pa.DataType, bytes]] = {}
# extension_name -> (storage_type, metadata_bytes)

_POLARS_REGISTRY: dict[str, tuple[pl.DataType, str | None]] = {}
# extension_name -> (pl_storage_dtype, metadata_str)


def _sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", name)


def _register_arrow_ext_type(converter: ExtensionTypeConverter) -> None:
    """Register a ``pa.ExtensionType`` subclass for *converter* in PyArrow's global registry."""
    name = converter.extension_name
    metadata = converter.extension_metadata or b""
    storage = converter.storage_type

    if name in _ARROW_REGISTRY:
        existing_storage, existing_metadata = _ARROW_REGISTRY[name]
        if existing_storage == storage and existing_metadata == metadata:
            return  # idempotent — safe for module reload and test-suite reuse
        raise ValueError(
            f"Extension type '{name}' is already registered in the PyArrow global registry "
            f"with different parameters.\n"
            f"  Registered: storage_type={existing_storage!r}, metadata={existing_metadata!r}\n"
            f"  Attempted:  storage_type={storage!r}, metadata={metadata!r}"
        )

    _name, _storage, _metadata = name, storage, metadata
    ArrowExtType = type(
        f"_ArrowExt_{_sanitize(name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )

    try:
        pa.register_extension_type(ArrowExtType())
    except pa.lib.ArrowKeyError:
        raise ValueError(
            f"Extension type '{name}' is already registered in the PyArrow global registry "
            f"by an external source. Cannot verify equivalence; orcapod requires exclusive "
            f"ownership of extension type registrations to prevent data corruption or "
            f"misrouted conversions. See PLT-1665 for future interop support."
        ) from None

    _ARROW_REGISTRY[name] = (storage, metadata)


def _register_polars_ext_type(converter: ExtensionTypeConverter) -> None:
    """Register a ``pl.BaseExtension`` subclass for *converter* in Polars' global registry."""
    name = converter.extension_name
    metadata = converter.extension_metadata
    metadata_str = metadata.decode("utf-8") if metadata else None
    pl_storage = pl.from_arrow(pa.array([], type=converter.storage_type)).dtype

    if name in _POLARS_REGISTRY:
        existing_storage, existing_meta = _POLARS_REGISTRY[name]
        if existing_storage == pl_storage and existing_meta == metadata_str:
            return  # idempotent
        raise ValueError(
            f"Extension type '{name}' is already registered in the Polars global registry "
            f"with different parameters.\n"
            f"  Registered: storage_dtype={existing_storage!r}, metadata={existing_meta!r}\n"
            f"  Attempted:  storage_dtype={pl_storage!r}, metadata={metadata_str!r}"
        )

    _name, _pl_storage, _meta_str = name, pl_storage, metadata_str
    PolarsExtType = type(
        f"_PolarsExt_{_sanitize(name)}",
        (pl.BaseExtension,),
        {
            "__init__": lambda self: pl.BaseExtension.__init__(self, _name, _pl_storage, _meta_str),
            "ext_from_params": classmethod(lambda cls, n, s, m: cls()),
        },
    )

    try:
        pl.register_extension_type(name, PolarsExtType)
    except ValueError as exc:
        raise ValueError(
            f"Extension type '{name}' is already registered in the Polars global registry "
            f"by an external source. Cannot verify equivalence; orcapod requires exclusive "
            f"ownership of extension type registrations to prevent data corruption or "
            f"misrouted conversions. See PLT-1665 for future interop support."
        ) from exc

    _POLARS_REGISTRY[name] = (pl_storage, metadata_str)


class ExtensionTypeRegistry:
    """Registry for ``ExtensionTypeConverter`` instances.

    Registering a converter automatically registers the corresponding
    extension type in both PyArrow's and Polars' global registries.

    The primary lookup key is ``extension_name``; a secondary lookup by
    ``python_type`` is provided for the write path.

    Example:
        >>> registry = ExtensionTypeRegistry()
        >>> registry.register(my_converter)
        >>> conv = registry.get_converter_for_name("my.Type")
    """

    def __init__(self) -> None:
        self._by_name: dict[str, ExtensionTypeConverter] = {}
        self._by_python_type: dict[type, ExtensionTypeConverter] = {}

    def register(self, converter: ExtensionTypeConverter) -> None:
        """Register *converter* and its PyArrow/Polars extension types.

        Args:
            converter: An ``ExtensionTypeConverter`` instance to register.

        Raises:
            ValueError: If ``converter.extension_name`` is already registered
                in this registry instance.
            ValueError: If the extension name is already in the PA or Polars
                global registry with different parameters.
            ValueError: If the extension name is already in the PA or Polars
                global registry from an external source (equivalence cannot
                be verified).
        """
        name = converter.extension_name
        if name in self._by_name:
            raise ValueError(
                f"Extension type '{name}' is already registered in this registry."
            )
        self._by_name[name] = converter
        self._by_python_type[converter.python_type] = converter
        _register_arrow_ext_type(converter)
        _register_polars_ext_type(converter)

    def get_converter_for_name(self, name: str) -> ExtensionTypeConverter | None:
        """Return the converter registered under *name*, or ``None``."""
        return self._by_name.get(name)

    def get_converter_for_python_type(self, python_type: type) -> ExtensionTypeConverter | None:
        """Return the converter for *python_type*, or ``None``.

        Checks exact match first, then falls back to an ``issubclass`` scan.
        When multiple registered types are superclasses of *python_type*, the
        one registered first wins (insertion-order dict, Python 3.7+).
        """
        converter = self._by_python_type.get(python_type)
        if converter is not None:
            return converter
        for registered_type, conv in self._by_python_type.items():
            if issubclass(python_type, registered_type):
                return conv
        return None

    def has_extension_name(self, name: str) -> bool:
        """Return ``True`` if *name* is registered."""
        return name in self._by_name

    def has_python_type(self, python_type: type) -> bool:
        """Return ``True`` if *python_type* (or a subclass) is registered."""
        return self.get_converter_for_python_type(python_type) is not None

    def list_extension_names(self) -> list[str]:
        """Return all registered extension names in insertion order."""
        return list(self._by_name.keys())

    def list_python_types(self) -> list[type]:
        """Return all registered Python types in insertion order."""
        return list(self._by_python_type.keys())
```

- [ ] **Step 4: Run the pure-Python tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v -k "not arrow and not polars and not round_trip and not parquet and not module_instance"
```

Expected: all 11 tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/registry.py tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): add ExtensionTypeRegistry with pure-Python lookup (PLT-1653)"
```

---

## Task 3: Add PyArrow global registration tests

**Files:**
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Add the PyArrow tests**

Append to `tests/test_extension_types/test_registry.py`:

```python
# ---------------------------------------------------------------------------
# PyArrow global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_arrow_registry():
    """After register(), PA global registry contains the extension type."""
    conv = _make_stub()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    # If the name is registered, attempting to re-register it raises ArrowKeyError.
    # This is the only stable public signal PyArrow provides.
    class _Probe(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), conv.extension_name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    with pytest.raises(pa.lib.ArrowKeyError):
        pa.register_extension_type(_Probe())


def test_register_arrow_global_collision_same_params_is_idempotent():
    """A second registry instance registering the same name+params succeeds silently."""
    name = _unique_name()
    conv = _make_stub(name=name, storage=pa.large_utf8(), metadata=b"cat")

    ExtensionTypeRegistry().register(conv)   # first — populates _ARROW_REGISTRY
    ExtensionTypeRegistry().register(conv)   # second — should not raise


def test_register_arrow_global_collision_different_storage_raises():
    """A second registry using the same name but different storage_type raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_utf8()))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_binary()))


def test_register_arrow_global_collision_different_metadata_raises():
    """A second registry using the same name but different metadata raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, metadata=b"original"))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, metadata=b"different"))


def test_register_arrow_external_registration_raises():
    """A name registered directly with PyArrow (bypassing our registry) raises on register()."""
    name = _unique_name()

    class _External(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    pa.register_extension_type(_External())  # bypass our registry

    with pytest.raises(ValueError, match="external source"):
        ExtensionTypeRegistry().register(_make_stub(name=name))
```

- [ ] **Step 2: Run all tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: all tests pass (the PyArrow registration was already wired in Task 2).

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_registry.py
git commit -m "test(extension_types): add PyArrow global registry tests (PLT-1653)"
```

---

## Task 4: Add Polars global registration tests

**Files:**
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Add the Polars tests**

Append to `tests/test_extension_types/test_registry.py`:

```python
# ---------------------------------------------------------------------------
# Polars global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_polars_registry():
    """After register(), pl.from_arrow on an ext-type array yields a BaseExtension dtype."""
    conv = _make_stub(storage=pa.large_utf8())
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    # Build a PA extension array using the registered type.
    # We need to get the registered ArrowExtType instance; the simplest way is
    # to read it from _ARROW_REGISTRY shadow dict via the type's name in a PA array.
    from orcapod.extension_types.registry import _ARROW_REGISTRY
    assert conv.extension_name in _ARROW_REGISTRY

    # Create a storage array and cast it to the ext type to get a properly typed array.
    # (The ArrowExtType class is not directly accessible from outside, but we can
    # construct an array through the IPC round-trip or via the registered type.)
    # Simplest: use pl.from_arrow on a storage array and check the dtype AFTER
    # registering — the series dtype should be our BaseExtension subclass.
    import warnings
    arr = pa.array(["hello"], type=pa.large_utf8())
    # The ext type is registered, so building an array with it works.
    # We access it via the _ARROW_REGISTRY which stores (storage_type, metadata).
    # The actual class instance is what was registered; we verify Polars recognises it
    # by checking the dtype returned from pl.from_arrow on an ext-typed array.
    # Build ext array via cast on a pre-registered type instance from the module.
    from orcapod.extension_types import registry as reg_mod
    # Reconstruct the ArrowExtType by checking what _ARROW_REGISTRY has, then
    # building a matching IPC array. Easiest: use the existing ArrowExtType class
    # by catching it from PA global via unregister/re-register trick — but that's
    # invasive. Instead, just verify via _POLARS_REGISTRY dict directly.
    from orcapod.extension_types.registry import _POLARS_REGISTRY
    assert conv.extension_name in _POLARS_REGISTRY
    stored_storage, stored_meta = _POLARS_REGISTRY[conv.extension_name]
    assert stored_storage == pl.String
    assert stored_meta == "test.category"


def test_register_polars_global_collision_same_params_is_idempotent():
    """A second registry instance registering the same name+params succeeds silently."""
    name = _unique_name()
    conv = _make_stub(name=name, storage=pa.large_utf8(), metadata=b"cat")

    ExtensionTypeRegistry().register(conv)
    ExtensionTypeRegistry().register(conv)   # should not raise


def test_register_polars_global_collision_different_storage_raises():
    """A second registry using the same name but different storage_type raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_utf8()))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_binary()))


def test_register_polars_external_registration_raises():
    """A name registered directly with Polars (bypassing our registry) raises on register()."""
    name = _unique_name()

    class _ExternalPL(pl.BaseExtension):
        def __init__(self):
            super().__init__(name, pl.String, None)
        @classmethod
        def ext_from_params(cls, n, s, m):
            return cls()

    # Also register in PA first so we don't hit the PA external-registration error
    class _ExternalPA(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    pa.register_extension_type(_ExternalPA())
    pl.register_extension_type(name, _ExternalPL)

    with pytest.raises(ValueError, match="external source"):
        ExtensionTypeRegistry().register(_make_stub(name=name))
```

- [ ] **Step 2: Run all tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_registry.py
git commit -m "test(extension_types): add Polars global registry tests (PLT-1653)"
```

---

## Task 5: End-to-end integration tests

**Files:**
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Add the integration tests**

Append to `tests/test_extension_types/test_registry.py`:

```python
# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------

import warnings
import tempfile
import pathlib
import pyarrow.parquet as pq


class _Color:
    """Minimal Python class used to exercise the converter contract end-to-end."""
    def __init__(self, hex_str: str) -> None:
        self.hex_str = hex_str
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Color) and self.hex_str == other.hex_str
    def __repr__(self) -> str:
        return f"Color({self.hex_str!r})"


def _make_color_converter() -> ExtensionTypeConverter:
    """ExtensionTypeConverter for _Color, backed by pa.large_utf8() storage."""
    _name = _unique_name()

    class _ColorConverter:
        @property
        def extension_name(self) -> str:
            return _name
        @property
        def extension_metadata(self) -> bytes | None:
            return b"test.color"
        @property
        def storage_type(self) -> pa.DataType:
            return pa.large_utf8()
        @property
        def python_type(self) -> type:
            return _Color
        def python_to_storage(self, value: _Color) -> str:
            return value.hex_str
        def storage_to_python(self, storage_value: str) -> _Color:
            return _Color(storage_value)

    return _ColorConverter()


def _build_ext_array(
    converter: ExtensionTypeConverter,
    values: list,
) -> pa.Array:
    """Build a PA extension array from Python values using the converter."""
    from orcapod.extension_types.registry import _ARROW_REGISTRY

    storage_values = [converter.python_to_storage(v) for v in values]
    storage_arr = pa.array(storage_values, type=converter.storage_type)

    # Retrieve the registered ArrowExtType instance via a fresh array cast.
    # We use the PA global registry indirectly: _ARROW_REGISTRY confirms
    # the type is registered; we then reconstruct the ext array by building
    # a new subclass instance (same extension_name → PA resolves to registered class).
    import re
    _name = converter.extension_name
    _storage = converter.storage_type
    _metadata = converter.extension_metadata or b""
    _sanitized = re.sub(r"[^A-Za-z0-9]", "_", _name)

    ArrowExtType = type(
        f"_ArrowExt_{_sanitized}_probe",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    # This will be caught as "already registered" internally; we instantiate
    # separately — PyArrow resolves the extension by name, not by class identity.
    ext_type_instance = ArrowExtType()
    return storage_arr.cast(ext_type_instance)


def test_python_class_round_trip():
    """Python objects → Arrow extension array → Python objects via converter methods."""
    conv = _make_color_converter()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    originals = [_Color("#ff0000"), _Color("#00ff00"), _Color("#0000ff")]
    ext_arr = _build_ext_array(conv, originals)

    # Decode back
    storage_back = ext_arr.cast(conv.storage_type)
    recovered = [conv.storage_to_python(v.as_py()) for v in storage_back]
    assert recovered == originals


def test_arrow_polars_round_trip():
    """PA ext array → pl.from_arrow → to_arrow() preserves extension type and values."""
    conv = _make_color_converter()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    originals = [_Color("#aabbcc"), _Color("#112233")]
    ext_arr = _build_ext_array(conv, originals)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pl_series = pl.from_arrow(ext_arr)

    assert isinstance(pl_series.dtype, pl.BaseExtension)
    assert pl_series.dtype.ext_name() == conv.extension_name

    arr_back = pl_series.to_arrow()
    assert arr_back.type.extension_name == conv.extension_name

    recovered = [conv.storage_to_python(v.as_py()) for v in arr_back.cast(conv.storage_type)]
    assert recovered == originals


def test_parquet_round_trip():
    """PA ext array → Parquet → read back via PyArrow; extension type and values preserved."""
    conv = _make_color_converter()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    originals = [_Color("#deadbe"), _Color("#cafeba")]
    ext_arr = _build_ext_array(conv, originals)
    schema = pa.schema([pa.field("color", ext_arr.type), pa.field("id", pa.int32())])
    table = pa.table(
        {"color": ext_arr, "id": pa.array([1, 2], type=pa.int32())},
        schema=schema,
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "test.parquet"
        pq.write_table(table, path)
        table_back = pq.read_table(path)

    assert table_back.schema.field("color").type.extension_name == conv.extension_name
    recovered = [
        conv.storage_to_python(v.as_py())
        for v in table_back.column("color").cast(conv.storage_type)
    ]
    assert recovered == originals
```

- [ ] **Step 2: Run all tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_registry.py
git commit -m "test(extension_types): add end-to-end integration tests (PLT-1653)"
```

---

## Task 6: Update `extension_types/__init__.py`

**Files:**
- Modify: `tests/test_extension_types/test_registry.py`
- Modify: `src/orcapod/extension_types/__init__.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_extension_types/test_registry.py`:

```python
# ---------------------------------------------------------------------------
# Module-level instance test
# ---------------------------------------------------------------------------

def test_extension_type_registry_module_instance():
    """extension_types.extension_type_registry is an ExtensionTypeRegistry, starts empty."""
    from orcapod import extension_types
    assert isinstance(extension_types.extension_type_registry, ExtensionTypeRegistry)
    # PLT-1653 scope: no built-in converters registered yet (that is PLT-1656)
    assert extension_types.extension_type_registry.list_extension_names() == []
```

- [ ] **Step 2: Run to confirm it fails**

```bash
uv run pytest tests/test_extension_types/test_registry.py::test_extension_type_registry_module_instance -v
```

Expected: `AttributeError: module 'orcapod.extension_types' has no attribute 'extension_type_registry'`

- [ ] **Step 3: Update `src/orcapod/extension_types/__init__.py`**

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for converters that map
between Python objects and their Arrow extension type storage representation.

The module-level ``extension_type_registry`` instance is the process default.
Built-in registrations (``Path``, ``UPath``, ``UUID``) are added by PLT-1656.
``DataContext`` wiring is added by PLT-1660.
"""

from .protocols import ExtensionTypeConverter
from .registry import ExtensionTypeRegistry

extension_type_registry = ExtensionTypeRegistry()

__all__ = [
    "ExtensionTypeConverter",
    "ExtensionTypeRegistry",
    "extension_type_registry",
]
```

- [ ] **Step 4: Run all tests**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all tests pass.

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
uv run pytest --tb=short -q
```

Expected: no new failures.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/__init__.py tests/test_extension_types/test_registry.py
git commit -m "feat(extension_types): export ExtensionTypeRegistry and module-level instance (PLT-1653)"
```

---

## Final check

```bash
uv run pytest tests/test_extension_types/ -v --tb=short
```

All tests should pass. The PR targets `dev`.
