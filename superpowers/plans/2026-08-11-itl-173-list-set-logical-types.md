# ITL-173: List and Set Logical Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `register_python_class(list[T])` / `set[T]` where T is a registered logical type — currently raises `ValueError` — by wrapping the entire container as a top-level Arrow extension type that embeds element reconstruction metadata.

**Architecture:** A new `ListLogicalType` class wraps `list[T]`/`set[T]` (where T resolves to a `pa.ExtensionType`) as a top-level Arrow extension over `large_list(<T storage>)`. Metadata JSON at the field level encodes the element extension name and its own metadata, enabling recursive reconstruction on read. Two guard removals in `UniversalTypeConverter` allow `GenericAlias` keys to be looked up in the registry. A shared helper `_make_or_get_list_logical_type` is called from both `_register_python_class_impl` and `_convert_python_to_arrow` for idempotent creation.

**Tech Stack:** PyArrow, Polars, Python `types.GenericAlias`, `typing.get_origin`/`get_args`, existing `make_arrow_extension_type`/`make_polars_extension_type` helpers, `BaseLogicalType`.

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/extension_types/list_logical_type_factory.py` | **Create** | `ListLogicalType`, `ListLogicalTypeFactory`, `LIST_CATEGORY`, `SET_CATEGORY` |
| `src/orcapod/extension_types/registry.py` | **Modify** line 297 | Guard `__qualname__` against `GenericAlias` |
| `src/orcapod/extension_types/protocols.py` | **Modify** | Add `arrow_type_to_python_type` to `TypeConverterProtocol` |
| `src/orcapod/semantic_types/universal_converter.py` | **Modify** 6 sites | Remove `isinstance(python_type, type)` guard ×2; replace `ValueError` raises ×2; update `_convert_python_to_arrow` list/set branches ×2; add `_make_or_get_list_logical_type` helper |
| `src/orcapod/extension_types/__init__.py` | **Modify** | Export new symbols |
| `src/orcapod/contexts/data/v0.1.json` | **Modify** | Register factory under `"list"` and `"set"` categories |
| `DESIGN_ISSUES.md` | **Modify** | Update ET2 status to `resolved` |
| `tests/test_extension_types/test_list_logical_type.py` | **Create** | Unit tests for `ListLogicalType` and `ListLogicalTypeFactory` |
| `tests/test_extension_types/test_roundtrips.py` | **Modify** | Add 9 integration round-trip tests |

---

## Task 1: Fix `registry.py` — guard `__qualname__` for `GenericAlias`

**Files:**
- Modify: `src/orcapod/extension_types/registry.py:297`
- Test: `tests/test_extension_types/test_registry.py`

`register_logical_type` uses `py_type.__qualname__` in the conflict-error message. `GenericAlias` (e.g. `list[uuid.UUID]`) has no `__qualname__`. The fix uses `getattr` with a fallback.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_extension_types/test_registry.py`:

```python
def test_register_logical_type_conflict_error_uses_repr_for_generic_alias():
    """Conflict error message for GenericAlias python_type must not raise AttributeError."""
    import uuid
    import pyarrow as pa
    from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type
    from orcapod.extension_types.base_logical_type import BaseLogicalType

    class _FakeListLT(BaseLogicalType):
        logical_type_name = "list[orcapod.uuid]"
        python_type = list[uuid.UUID]

        def get_arrow_extension_type(self):
            ext_cls = make_arrow_extension_type(
                "list[orcapod.uuid]", pa.large_list(pa.large_binary())
            )
            return ext_cls()

        def get_polars_extension_type(self):
            from orcapod.extension_types.registry import make_polars_extension_type
            ext_cls = make_polars_extension_type(
                "list[orcapod.uuid]", pa.large_list(pa.large_binary())
            )
            return ext_cls()

        def python_to_storage(self, value, converter):
            return value

        def storage_to_python(self, storage_value, converter):
            return storage_value

    class _FakeListLT2(_FakeListLT):
        """Different instance, same keys — should raise ValueError, not AttributeError."""

    registry = LogicalTypeRegistry()
    lt1 = _FakeListLT()
    lt2 = _FakeListLT2()
    registry.register_logical_type(lt1)

    with pytest.raises(ValueError, match="python_type"):
        registry.register_logical_type(lt2)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_registry.py::test_register_logical_type_conflict_error_uses_repr_for_generic_alias -v
```

Expected: `FAILED` with `AttributeError: 'types.GenericAlias' object has no attribute '__qualname__'`

- [ ] **Step 3: Apply the fix**

In `src/orcapod/extension_types/registry.py`, find line 297 (inside `register_logical_type`, the `for existing, label, key in [...]` block):

```python
# Before
(existing_by_python, "python_type", py_type.__qualname__),

# After
(existing_by_python, "python_type", getattr(py_type, "__qualname__", repr(py_type))),
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_registry.py::test_register_logical_type_conflict_error_uses_repr_for_generic_alias -v
```

Expected: `PASSED`

- [ ] **Step 5: Run full registry tests to confirm no regression**

```bash
uv run pytest tests/test_extension_types/test_registry.py -v
```

Expected: all `PASSED`

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/registry.py tests/test_extension_types/test_registry.py
git commit -m "fix(registry): guard __qualname__ against GenericAlias in conflict error message"
```

---

## Task 2: Add `arrow_type_to_python_type` to `TypeConverterProtocol`

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`
- Test: `tests/test_extension_types/test_protocols.py`

`ListLogicalTypeFactory.reconstruct_from_arrow` needs `converter.arrow_type_to_python_type(element_ext)` to recover the element's Python type after registering it. This method already exists on `UniversalTypeConverter` but is not in `TypeConverterProtocol`.

- [ ] **Step 1: Write the failing test**

Add to `tests/test_extension_types/test_protocols.py`:

```python
def test_type_converter_protocol_has_arrow_type_to_python_type():
    """TypeConverterProtocol must declare arrow_type_to_python_type."""
    import inspect
    from orcapod.extension_types.protocols import TypeConverterProtocol
    assert "arrow_type_to_python_type" in TypeConverterProtocol.__protocol_attrs__
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_extension_types/test_protocols.py::test_type_converter_protocol_has_arrow_type_to_python_type -v
```

Expected: `FAILED` with `AssertionError`

- [ ] **Step 3: Add the method to the protocol**

In `src/orcapod/extension_types/protocols.py`, add after `register_arrow_extension`:

```python
    def arrow_type_to_python_type(self, arrow_type: "pa.DataType") -> "DataType":
        """Convert an Arrow type to its Python type hint.

        Used by ``ListLogicalTypeFactory.reconstruct_from_arrow`` to recover
        the element Python type after registering the element extension type.

        Args:
            arrow_type: An Arrow type (may be a ``pa.ExtensionType``).

        Returns:
            The Python type hint corresponding to ``arrow_type``.
        """
        ...
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_protocols.py::test_type_converter_protocol_has_arrow_type_to_python_type -v
```

Expected: `PASSED`

- [ ] **Step 5: Run all protocol tests**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v
```

Expected: all `PASSED`

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/protocols.py tests/test_extension_types/test_protocols.py
git commit -m "feat(protocols): add arrow_type_to_python_type to TypeConverterProtocol"
```

---

## Task 3: Create `list_logical_type_factory.py` — `ListLogicalType`

**Files:**
- Create: `src/orcapod/extension_types/list_logical_type_factory.py`
- Create: `tests/test_extension_types/test_list_logical_type.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_extension_types/test_list_logical_type.py`:

```python
"""Tests for ListLogicalType and ListLogicalTypeFactory."""
from __future__ import annotations

import uuid as uuid_module

import pyarrow as pa
import pytest

# ── Helpers ───────────────────────────────────────────────────────────────────


def _uuid_ext_type() -> pa.ExtensionType:
    """Return the registered orcapod.uuid extension type."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID
    return LogicalUUID().get_arrow_extension_type()


class _StubConverter:
    """Minimal converter stub delegating UUID and list[UUID] conversions."""

    def python_to_storage(self, value, annotation):
        if annotation is uuid_module.UUID:
            return value.bytes
        if hasattr(annotation, "__origin__") and annotation.__origin__ is list:
            import typing
            args = typing.get_args(annotation)
            return [self.python_to_storage(item, args[0]) for item in value]
        return value

    def storage_to_python(self, storage_value, annotation):
        if annotation is uuid_module.UUID:
            return uuid_module.UUID(bytes=bytes(storage_value))
        if hasattr(annotation, "__origin__") and annotation.__origin__ is list:
            import typing
            args = typing.get_args(annotation)
            return [self.storage_to_python(item, args[0]) for item in storage_value]
        return storage_value

    def register_python_class(self, annotation):
        if annotation is uuid_module.UUID:
            return _uuid_ext_type()
        if annotation is str:
            return pa.large_string()
        if annotation is int:
            return pa.int64()
        raise ValueError(f"Unsupported annotation: {annotation}")

    def register_arrow_extension(self, ext_name, metadata_bytes, storage_type):
        if ext_name == "orcapod.uuid":
            return _uuid_ext_type()
        raise ValueError(f"Unknown extension: {ext_name}")

    def arrow_type_to_python_type(self, arrow_type):
        if hasattr(arrow_type, "extension_name"):
            if arrow_type.extension_name == "orcapod.uuid":
                return uuid_module.UUID
        return type(None)


# ── ListLogicalType unit tests ────────────────────────────────────────────────


def test_list_logical_type_importable():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    assert ListLogicalType is not None


def test_list_logical_type_name():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    assert lt.logical_type_name == "list[orcapod.uuid]"


def test_set_logical_type_name():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=True)
    assert lt.logical_type_name == "set[orcapod.uuid]"


def test_list_logical_type_python_type():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    assert lt.python_type == list[uuid_module.UUID]


def test_set_logical_type_python_type():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=True)
    assert lt.python_type == set[uuid_module.UUID]


def test_list_logical_type_arrow_extension_type_name():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    ext = lt.get_arrow_extension_type()
    assert hasattr(ext, "extension_name")
    assert ext.extension_name == "list[orcapod.uuid]"


def test_list_logical_type_storage_is_large_list_of_large_binary():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    ext = lt.get_arrow_extension_type()
    assert pa.types.is_large_list(ext.storage_type)
    assert ext.storage_type.value_type == pa.large_binary()


def test_list_logical_type_storage_is_et1_safe():
    """Value type of list storage must NOT be a pa.ExtensionType (ET1 invariant)."""
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    ext = lt.get_arrow_extension_type()
    assert not isinstance(ext.storage_type.value_type, pa.ExtensionType), (
        "ET1 violation: list value type must not be an ExtensionType"
    )


def test_list_logical_type_arrow_extension_type_cached():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_list_logical_type_index_element():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    assert lt.index_element() == uuid_module.UUID


def test_list_logical_type_python_to_storage():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")
    result = lt.python_to_storage([u1, u2], _StubConverter())
    assert result == [u1.bytes, u2.bytes]


def test_list_logical_type_python_to_storage_none():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    assert lt.python_to_storage(None, _StubConverter()) == []


def test_list_logical_type_storage_to_python():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.storage_to_python([u1.bytes], _StubConverter())
    assert result == [u1]
    assert isinstance(result, list)


def test_set_logical_type_storage_to_python_returns_set():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=True)
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.storage_to_python([u1.bytes], _StubConverter())
    assert isinstance(result, set)
    assert result == {u1}


def test_list_logical_type_metadata_contains_element_ext_name():
    """Extension metadata must contain element_ext_name for reconstruction."""
    import json
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType, LIST_CATEGORY
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    ext = lt.get_arrow_extension_type()
    meta = json.loads(ext.__arrow_ext_serialize__().decode("utf-8"))
    assert meta["category"] == LIST_CATEGORY
    assert meta["element_ext_name"] == "orcapod.uuid"
    assert "element_ext_metadata" in meta


def test_list_logical_type_protocol_conformance():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType
    from orcapod.extension_types.protocols import LogicalTypeProtocol
    lt = ListLogicalType(uuid_module.UUID, _uuid_ext_type(), is_set=False)
    assert isinstance(lt, LogicalTypeProtocol)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_list_logical_type.py -v 2>&1 | head -30
```

Expected: multiple `FAILED` or `ERROR` with `ModuleNotFoundError: No module named 'orcapod.extension_types.list_logical_type_factory'`

- [ ] **Step 3: Create `list_logical_type_factory.py` with `ListLogicalType`**

Create `src/orcapod/extension_types/list_logical_type_factory.py`:

```python
"""ListLogicalType and ListLogicalTypeFactory.

Provides ``ListLogicalType`` — a logical type that wraps ``list[T]`` or ``set[T]``
(where ``T`` is itself a registered logical type) as a top-level Arrow extension
type with storage ``large_list(<T storage>)``.

This solves ET2 (see ``DESIGN_ISSUES.md``): Arrow cannot preserve extension-type
metadata inside list value fields (ET1), but metadata at the outermost field level
IS preserved through Parquet and Delta. By wrapping the whole list as the extension
type, the element type information survives a Parquet round-trip with a fresh
converter.

Write path: triggered explicitly in ``UniversalTypeConverter._register_python_class_impl``
and ``_convert_python_to_arrow`` when ``list[T]``/``set[T]`` is encountered and ``T``
resolves to a ``pa.ExtensionType``.

Read path (``ListLogicalTypeFactory.reconstruct_from_arrow``): dispatched by
``register_arrow_extension`` when category is ``"list"`` or ``"set"``. Recursively
calls ``converter.register_arrow_extension`` for the element type.

Category tags: ``LIST_CATEGORY = "list"``, ``SET_CATEGORY = "set"``
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from orcapod.extension_types.protocols import TypeConverterProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)

#: Category tag embedded in Arrow extension metadata for ``list[T]`` types.
LIST_CATEGORY = "list"

#: Category tag embedded in Arrow extension metadata for ``set[T]`` types.
SET_CATEGORY = "set"


class ListLogicalType(BaseLogicalType):
    """Logical type for ``list[T]`` or ``set[T]`` where ``T`` is a registered logical type.

    Wraps the entire container as a top-level Arrow extension type with storage
    ``large_list(<T storage>)``. Extension-type metadata is only at the outermost
    field level (storage-safe invariant), encoding enough information to reconstruct
    the element type recursively on read.

    ``python_type`` returns the full generic alias (e.g. ``list[uuid.UUID]``) so that
    ``arrow_schema_to_python_schema`` produces a round-trippable schema.

    Args:
        element_python_type: The Python type for list elements
            (e.g. ``uuid.UUID``, ``list[uuid.UUID]`` for nested lists).
        element_ext_type: The ``pa.ExtensionType`` for the element logical type.
            Its ``storage_type`` becomes the list value type; its
            ``__arrow_ext_serialize__()`` bytes are embedded in this type's metadata.
        is_set: If ``True``, represents ``set[T]`` instead of ``list[T]``.
            Storage is identical; ``python_type`` returns ``set[T]`` and
            ``storage_to_python`` returns a ``set``.

    Example:
        >>> import uuid
        >>> uuid_ext = LogicalUUID().get_arrow_extension_type()
        >>> lt = ListLogicalType(uuid.UUID, uuid_ext, is_set=False)
        >>> lt.logical_type_name
        'list[orcapod.uuid]'
        >>> lt.python_type
        list[uuid.UUID]
    """

    def __init__(
        self,
        element_python_type: type,
        element_ext_type: "pa.ExtensionType",
        *,
        is_set: bool = False,
    ) -> None:
        self._element_python_type = element_python_type
        self._element_ext_type = element_ext_type
        self._is_set = is_set

        element_ext_name = element_ext_type.extension_name
        element_storage = element_ext_type.storage_type

        prefix = "set" if is_set else "list"
        self._logical_name = f"{prefix}[{element_ext_name}]"

        # Embed element metadata for recursive reconstruction on read.
        # Empty bytes (e.g. UUID has no metadata) → stored as null JSON value.
        element_meta_bytes = element_ext_type.__arrow_ext_serialize__()
        element_meta_str = element_meta_bytes.decode("utf-8") if element_meta_bytes else None

        category = SET_CATEGORY if is_set else LIST_CATEGORY
        _metadata_dict: dict[str, Any] = {
            "category": category,
            "element_ext_name": element_ext_name,
            "element_ext_metadata": element_meta_str,
        }
        _metadata_bytes = json.dumps(_metadata_dict).encode("utf-8")

        # Storage: large_list(<element storage>) — no nested extension type (ET1 safe).
        outer_storage = pa.large_list(element_storage)

        self._arrow_ext_class = make_arrow_extension_type(
            self._logical_name, outer_storage, metadata=_metadata_bytes
        )
        self._arrow_ext: pa.ExtensionType | None = None

        self._polars_ext_class = make_polars_extension_type(
            self._logical_name, outer_storage, metadata=json.dumps(_metadata_dict),
        )
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Arrow extension name, e.g. ``list[orcapod.uuid]`` or ``set[orcapod.uuid]``."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """Full generic alias for the container type.

        Returns ``list[element_python_type]`` or ``set[element_python_type]``.
        The full alias (not bare ``list``) is required so that
        ``arrow_schema_to_python_schema`` produces a round-trippable schema.
        """
        container = set if self._is_set else list
        return container[self._element_python_type]

    def get_arrow_extension_type(self) -> "pa.ExtensionType":
        """Return the Arrow extension type (cached).

        Returns:
            A ``pa.ExtensionType`` with ``extension_name == logical_type_name``
            and ``storage_type == large_list(<element storage>)``.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> "pl.BaseExtension":
        """Return the Polars extension type (cached).

        Returns:
            A ``pl.BaseExtension`` registered under ``logical_type_name``.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def index_element(self) -> type:
        """Return the Python element type for positional list access.

        Returns:
            The ``element_python_type`` passed at construction.
        """
        return self._element_python_type

    def python_to_storage(self, value: Any, converter: "TypeConverterProtocol | None") -> list:
        """Convert a ``list[T]`` or ``set[T]`` to its Arrow storage representation.

        Delegates per-element conversion to ``converter.python_to_storage``.

        Args:
            value: A Python ``list`` or ``set`` of elements of type ``T``.
            converter: Active converter for element-level delegation.

        Returns:
            A Python list of element storage values (one per input element).
        """
        if value is None:
            return []
        return [
            converter.python_to_storage(item, self._element_python_type)
            for item in value
        ]

    def storage_to_python(self, storage_value: Any, converter: "TypeConverterProtocol | None") -> list | set:
        """Reconstruct a ``list[T]`` or ``set[T]`` from Arrow storage values.

        Delegates per-element conversion to ``converter.storage_to_python``.

        Args:
            storage_value: Sequence of element storage values from an Arrow array.
            converter: Active converter for element-level delegation.

        Returns:
            A Python ``list`` (or ``set`` when ``is_set=True``) of reconstructed elements.
        """
        if storage_value is None:
            return set() if self._is_set else []
        elements = [
            converter.storage_to_python(item, self._element_python_type)
            for item in storage_value
        ]
        return set(elements) if self._is_set else elements
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_list_logical_type.py -v
```

Expected: all `PASSED`

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/list_logical_type_factory.py tests/test_extension_types/test_list_logical_type.py
git commit -m "feat(extension_types): add ListLogicalType for list[T]/set[T] wrapping"
```

---

## Task 4: Add `ListLogicalTypeFactory` to `list_logical_type_factory.py`

**Files:**
- Modify: `src/orcapod/extension_types/list_logical_type_factory.py`
- Modify: `tests/test_extension_types/test_list_logical_type.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_extension_types/test_list_logical_type.py`:

```python
# ── ListLogicalTypeFactory tests ──────────────────────────────────────────────


def test_list_logical_type_factory_importable():
    from orcapod.extension_types.list_logical_type_factory import ListLogicalTypeFactory
    assert ListLogicalTypeFactory is not None


def test_list_logical_type_factory_reconstruct_list_of_uuid():
    """reconstruct_from_arrow produces ListLogicalType(uuid.UUID, …, is_set=False)."""
    from orcapod.extension_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        LIST_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.large_binary())
    metadata = {
        "category": LIST_CATEGORY,
        "element_ext_name": "orcapod.uuid",
        "element_ext_metadata": None,
    }
    lt = factory.reconstruct_from_arrow(
        "list[orcapod.uuid]", storage_type, metadata, _StubConverter()
    )
    assert lt.logical_type_name == "list[orcapod.uuid]"
    assert lt.python_type == list[uuid_module.UUID]


def test_list_logical_type_factory_reconstruct_set_of_uuid():
    """reconstruct_from_arrow produces ListLogicalType(uuid.UUID, …, is_set=True)."""
    from orcapod.extension_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        SET_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    storage_type = pa.large_list(pa.large_binary())
    metadata = {
        "category": SET_CATEGORY,
        "element_ext_name": "orcapod.uuid",
        "element_ext_metadata": None,
    }
    lt = factory.reconstruct_from_arrow(
        "set[orcapod.uuid]", storage_type, metadata, _StubConverter()
    )
    assert lt.logical_type_name == "set[orcapod.uuid]"
    assert lt.python_type == set[uuid_module.UUID]


def test_list_logical_type_factory_reconstruct_raises_on_non_list_storage():
    from orcapod.extension_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        LIST_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    metadata = {"category": LIST_CATEGORY, "element_ext_name": "orcapod.uuid", "element_ext_metadata": None}
    with pytest.raises(ValueError, match="list storage"):
        factory.reconstruct_from_arrow(
            "list[orcapod.uuid]", pa.large_binary(), metadata, _StubConverter()
        )


def test_list_logical_type_factory_reconstruct_raises_on_missing_element_ext_name():
    from orcapod.extension_types.list_logical_type_factory import (
        ListLogicalTypeFactory,
        LIST_CATEGORY,
    )
    factory = ListLogicalTypeFactory()
    metadata = {"category": LIST_CATEGORY}
    with pytest.raises(ValueError, match="element_ext_name"):
        factory.reconstruct_from_arrow(
            "list[orcapod.uuid]", pa.large_list(pa.large_binary()), metadata, _StubConverter()
        )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_list_logical_type.py -k "factory" -v
```

Expected: `FAILED` with `ImportError` or `AttributeError`

- [ ] **Step 3: Add `ListLogicalTypeFactory` to `list_logical_type_factory.py`**

Append to `src/orcapod/extension_types/list_logical_type_factory.py`:

```python
class ListLogicalTypeFactory:
    """Stateless factory that reconstructs ``ListLogicalType`` instances from Arrow metadata.

    Registered for categories ``"list"`` and ``"set"`` in the ``LogicalTypeRegistry``.
    No ``python_bases`` are registered — write-path dispatch is explicit in
    ``UniversalTypeConverter._register_python_class_impl`` and ``_convert_python_to_arrow``.

    Read path only (``reconstruct_from_arrow``). ``create_for_python_type`` raises
    ``NotImplementedError`` because explicit dispatch makes it unnecessary.

    Register with::

        converter.register_logical_type_factory(ListLogicalTypeFactory(), category="list")
        converter.register_logical_type_factory(ListLogicalTypeFactory(), category="set")
    """

    def supports_class(self, python_type: type) -> bool:
        """Always ``False`` — write-path dispatch is explicit, not via base-class matching.

        Args:
            python_type: Ignored.

        Returns:
            ``False``.
        """
        return False

    def create_for_python_type(
        self,
        python_type: type,
        converter: "TypeConverterProtocol",
    ) -> ListLogicalType:
        """Not implemented — list/set types are registered directly in the converter.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ListLogicalTypeFactory does not implement create_for_python_type. "
            "list[T] and set[T] logical types are created explicitly in "
            "UniversalTypeConverter._register_python_class_impl."
        )

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: "pa.DataType",
        metadata: dict[str, Any],
        converter: "TypeConverterProtocol",
    ) -> ListLogicalType:
        """Reconstruct a ``ListLogicalType`` from Arrow schema metadata (read path).

        Recursively calls ``converter.register_arrow_extension`` for the element type,
        ensuring the element logical type is registered before constructing the outer
        ``ListLogicalType``. Handles arbitrary nesting depth via recursion.

        Args:
            arrow_extension_name: Extension name (e.g. ``"list[orcapod.uuid]"``).
            storage_type: Outer storage type (``large_list(<element storage>)``).
            metadata: Parsed metadata dict; must contain ``"category"`` and
                ``"element_ext_name"``; ``"element_ext_metadata"`` may be ``None``.
            converter: Active converter for recursive element registration.

        Returns:
            A ``ListLogicalType`` ready for registration.

        Raises:
            ValueError: If ``storage_type`` is not a list type, or required metadata
                keys are missing.
        """
        if not (pa.types.is_large_list(storage_type) or pa.types.is_list(storage_type)):
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: expected a list storage "
                f"type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        element_ext_name = metadata.get("element_ext_name")
        if not element_ext_name:
            raise ValueError(
                f"ListLogicalTypeFactory.reconstruct_from_arrow: missing 'element_ext_name' "
                f"in metadata for {arrow_extension_name!r}. metadata={metadata!r}."
            )

        element_meta_str = metadata.get("element_ext_metadata")
        element_meta_bytes = (
            element_meta_str.encode("utf-8") if element_meta_str else b""
        )
        # Element storage is the value type of the outer list storage.
        element_storage_type = storage_type.value_type

        # Recursively register the element logical type (handles nesting).
        element_ext_arrow_type = converter.register_arrow_extension(
            element_ext_name, element_meta_bytes, element_storage_type
        )

        # Recover element Python type from the now-registered extension type.
        element_python_type = converter.arrow_type_to_python_type(element_ext_arrow_type)

        is_set = metadata.get("category") == SET_CATEGORY
        logger.debug(
            "ListLogicalTypeFactory: reconstructed %r from Arrow (is_set=%s)",
            arrow_extension_name,
            is_set,
        )
        return ListLogicalType(element_python_type, element_ext_arrow_type, is_set=is_set)
```

- [ ] **Step 4: Run factory tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_list_logical_type.py -v
```

Expected: all `PASSED`

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/list_logical_type_factory.py tests/test_extension_types/test_list_logical_type.py
git commit -m "feat(extension_types): add ListLogicalTypeFactory for read-path reconstruction"
```

---

## Task 5: Update `universal_converter.py` — `_register_python_class_impl`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Test: `tests/test_semantic_types/test_universal_converter.py` (create or modify as appropriate)

Replace the `ValueError` raises in `_register_python_class_impl` for `list[T]`/`set[T]` when `T` resolves to an extension type. Also add the shared `_make_or_get_list_logical_type` helper.

- [ ] **Step 1: Write the failing tests**

Check if `tests/test_semantic_types/test_universal_converter.py` exists; create it if not, or append to existing. Add:

```python
def test_register_python_class_list_of_uuid_returns_extension_type():
    """register_python_class(list[uuid.UUID]) must return a pa.ExtensionType, not raise."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(list[uuid.UUID])

    assert isinstance(result, pa.ExtensionType), (
        f"Expected pa.ExtensionType, got {type(result)}: {result!r}"
    )
    assert result.extension_name == "list[orcapod.uuid]"


def test_register_python_class_set_of_uuid_returns_extension_type():
    """register_python_class(set[uuid.UUID]) must return a pa.ExtensionType."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(set[uuid.UUID])

    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "set[orcapod.uuid]"


def test_register_python_class_list_of_int_unchanged():
    """register_python_class(list[int]) must still return plain large_list(int64)."""
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(list[int])

    assert not isinstance(result, pa.ExtensionType)
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int64()


def test_register_python_class_list_of_uuid_idempotent():
    """Calling register_python_class(list[uuid.UUID]) twice returns the same ext type."""
    import uuid
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result1 = converter.register_python_class(list[uuid.UUID])
    result2 = converter.register_python_class(list[uuid.UUID])
    assert result1.extension_name == result2.extension_name
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/ -k "list_of_uuid or set_of_uuid or list_of_int_unchanged" -v
```

Expected: `FAILED` with `ValueError: 'list[...]' is not yet supported`

- [ ] **Step 3: Add `_make_or_get_list_logical_type` helper and update the two branches**

In `src/orcapod/semantic_types/universal_converter.py`:

**3a. Add the helper method** (insert after `_find_factory_for_class`, around line 490):

```python
def _make_or_get_list_logical_type(
    self,
    element_ext_type: "pa.ExtensionType",
    is_set: bool,
) -> "pa.ExtensionType":
    """Return (creating and registering if needed) a ``ListLogicalType`` for a container.

    Shared by ``_register_python_class_impl`` and ``_convert_python_to_arrow`` to
    ensure idempotent creation — looking up by extension name first avoids
    creating two different ``ListLogicalType`` instances for the same annotation.

    Args:
        element_ext_type: Arrow extension type of the element.
        is_set: ``True`` for ``set[T]``, ``False`` for ``list[T]``.

    Returns:
        The ``pa.ExtensionType`` of the created-or-existing ``ListLogicalType``.
    """
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType

    prefix = "set" if is_set else "list"
    list_ext_name = f"{prefix}[{element_ext_type.extension_name}]"

    # Idempotency: look up by extension name (GenericAlias key not yet in registry).
    lt = self._logical_type_registry.get_by_arrow_extension_name(list_ext_name)
    if lt is None:
        element_python_type = self.arrow_type_to_python_type(element_ext_type)
        lt = ListLogicalType(element_python_type, element_ext_type, is_set=is_set)
        self._logical_type_registry.register_logical_type(lt)
    return lt.get_arrow_extension_type()
```

**3b. Replace the `origin is list` block in `_register_python_class_impl`** (around lines 347–361):

```python
# BEFORE:
        if origin is list:
            if not args:
                raise ValueError(
                    "Unparameterized 'list' is not supported. Use 'list[T]' with a concrete "
                    "element type (e.g. list[int], list[str])."
                )
            inner = self.register_python_class(args[0])
            if isinstance(inner, pa.ExtensionType):
                raise ValueError(
                    f"'list[{args[0]}]' is not yet supported: the element type maps to Arrow "
                    f"extension type {inner.extension_name!r}, which cannot be preserved inside "
                    f"a list value field due to an Arrow limitation (ET2 in DESIGN_ISSUES.md). "
                    f"Native list-of-logical-type support is tracked in PLT-1732."
                )
            return pa.large_list(inner)

# AFTER:
        if origin is list:
            if not args:
                raise ValueError(
                    "Unparameterized 'list' is not supported. Use 'list[T]' with a concrete "
                    "element type (e.g. list[int], list[str])."
                )
            inner = self.register_python_class(args[0])
            if isinstance(inner, pa.ExtensionType):
                return self._make_or_get_list_logical_type(inner, is_set=False)
            return pa.large_list(inner)
```

**3c. Replace the `origin is set` block in `_register_python_class_impl`** (around lines 363–378):

```python
# BEFORE:
        if origin is set:
            if not args:
                raise ValueError(
                    "Unparameterized 'set' is not supported. Use 'set[T]' with a concrete "
                    "element type (e.g. set[int], set[str])."
                )
            inner = self.register_python_class(args[0])
            if isinstance(inner, pa.ExtensionType):
                raise ValueError(
                    f"'set[{args[0]}]' is not yet supported: the element type maps to Arrow "
                    f"extension type {inner.extension_name!r}, which cannot be preserved inside "
                    f"a list value field due to an Arrow limitation (ET2 in DESIGN_ISSUES.md). "
                    f"Native set-of-logical-type support is tracked in PLT-1732."
                )
            return pa.large_list(inner)

# AFTER:
        if origin is set:
            if not args:
                raise ValueError(
                    "Unparameterized 'set' is not supported. Use 'set[T]' with a concrete "
                    "element type (e.g. set[int], set[str])."
                )
            inner = self.register_python_class(args[0])
            if isinstance(inner, pa.ExtensionType):
                return self._make_or_get_list_logical_type(inner, is_set=True)
            return pa.large_list(inner)
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/test_semantic_types/ -k "list_of_uuid or set_of_uuid or list_of_int_unchanged" -v
```

Expected: all `PASSED`

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: no new failures

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/
git commit -m "feat(converter): register list[T]/set[T] as ListLogicalType when T is an extension type"
```

---

## Task 6: Update `_convert_python_to_arrow` — remove `isinstance` guard + update list/set branches

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Test: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write failing tests**

Append to the test file:

```python
def test_python_type_to_arrow_type_list_of_uuid_returns_extension_type():
    """python_type_to_arrow_type(list[UUID]) must return the list[orcapod.uuid] ext type."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    # Pre-register so the type is in the registry before python_type_to_arrow_type is called.
    converter.register_python_class(list[uuid.UUID])
    result = converter.python_type_to_arrow_type(list[uuid.UUID])

    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "list[orcapod.uuid]"


def test_python_type_to_arrow_type_list_of_uuid_without_prior_registration():
    """python_type_to_arrow_type(list[UUID]) must work even without prior register_python_class."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    # Fresh converter — ListLogicalType not yet registered
    converter = create_registry().get_context().type_converter
    result = converter.python_type_to_arrow_type(list[uuid.UUID])

    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "list[orcapod.uuid]"


def test_arrow_schema_to_python_schema_round_trip_list_of_uuid():
    """Schema round-trip: list[UUID] → Arrow ext → python schema → Arrow ext (same type)."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    python_schema = {"ids": list[uuid.UUID]}
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)

    # Arrow schema has list[orcapod.uuid] extension type
    assert arrow_schema.field("ids").type.extension_name == "list[orcapod.uuid]"

    # Recover Python schema
    recovered = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered["ids"] == list[uuid.UUID]

    # Re-derive Arrow schema — must be identical
    arrow_schema2 = converter.python_schema_to_arrow_schema(recovered)
    assert arrow_schema2.field("ids").type.extension_name == "list[orcapod.uuid]"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/ -k "python_type_to_arrow_type_list or arrow_schema_to_python_schema_round_trip_list" -v
```

Expected: `FAILED` — `python_type_to_arrow_type(list[UUID])` returns plain `large_list` instead of the ext type, because the early registry check skips `GenericAlias`.

- [ ] **Step 3: Remove `isinstance(python_type, type)` guard in `_convert_python_to_arrow`**

Find the early registry check in `_convert_python_to_arrow` (around line 1079):

```python
# BEFORE:
        if self._logical_type_registry is not None and isinstance(python_type, type):
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                return lt.get_arrow_extension_type()

# AFTER:
        if self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                return lt.get_arrow_extension_type()
```

Also update the `origin is list` branch in `_convert_python_to_arrow` (around line 1100) to create `ListLogicalType` when element is an ext type:

```python
# BEFORE:
        if origin is list:
            if len(args) != 1:
                raise ValueError(
                    f"list type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            return pa.large_list(element_type)

# AFTER:
        if origin is list:
            if len(args) != 1:
                raise ValueError(
                    f"list type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            if isinstance(element_type, pa.ExtensionType) and self._logical_type_registry is not None:
                return self._make_or_get_list_logical_type(element_type, is_set=False)
            return pa.large_list(element_type)
```

And the `origin is set` branch in `_convert_python_to_arrow` (around line 1169):

```python
# BEFORE:
        elif origin is set:
            if len(args) != 1:
                raise ValueError(
                    f"set type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            return pa.large_list(element_type)

# AFTER:
        elif origin is set:
            if len(args) != 1:
                raise ValueError(
                    f"set type must have exactly one type argument, got: {args}"
                )
            element_type = self.python_type_to_arrow_type(args[0])
            if isinstance(element_type, pa.ExtensionType) and self._logical_type_registry is not None:
                return self._make_or_get_list_logical_type(element_type, is_set=True)
            return pa.large_list(element_type)
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/test_semantic_types/ -k "python_type_to_arrow_type_list or arrow_schema_to_python_schema_round_trip_list" -v
```

Expected: all `PASSED`

- [ ] **Step 5: Run full suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: no new failures

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/
git commit -m "fix(converter): remove isinstance guard in _convert_python_to_arrow to handle GenericAlias registry lookup"
```

---

## Task 7: Update `_create_python_to_arrow_converter` — remove `isinstance` guard

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Test: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write failing test**

```python
def test_value_converter_list_of_uuid_produces_bytes_list():
    """get_python_to_arrow_converter(list[UUID]) converts [uuid, uuid] → [bytes, bytes]."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    converter.register_python_class(list[uuid.UUID])

    conv_fn = converter.get_python_to_arrow_converter(list[uuid.UUID])
    u1 = uuid.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid.UUID("87654321-4321-8765-4321-876543218765")
    result = conv_fn([u1, u2])

    assert result == [u1.bytes, u2.bytes]


def test_value_converter_set_of_uuid_produces_bytes_list():
    """get_python_to_arrow_converter(set[UUID]) converts {uuid} → [bytes]."""
    import uuid
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    converter.register_python_class(set[uuid.UUID])

    conv_fn = converter.get_python_to_arrow_converter(set[uuid.UUID])
    u1 = uuid.UUID("12345678-1234-5678-1234-567812345678")
    result = conv_fn({u1})

    assert result == [u1.bytes]
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_semantic_types/ -k "value_converter_list_of_uuid or value_converter_set_of_uuid" -v
```

Expected: `FAILED` — converter returns the list/set unchanged (passthrough) instead of converting UUIDs to bytes.

- [ ] **Step 3: Remove `isinstance(python_type, type)` guard in `_create_python_to_arrow_converter`**

Find the early registry check in `_create_python_to_arrow_converter` (around line 1391):

```python
# BEFORE:
        if self._logical_type_registry is not None and isinstance(python_type, type):
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                _lt = lt
                _self = self
                return lambda value: _lt.python_to_storage(value, _self)

# AFTER:
        if self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_python_type(python_type)
            if lt is not None:
                _lt = lt
                _self = self
                return lambda value: _lt.python_to_storage(value, _self)
```

- [ ] **Step 4: Run the new tests**

```bash
uv run pytest tests/test_semantic_types/ -k "value_converter_list_of_uuid or value_converter_set_of_uuid" -v
```

Expected: all `PASSED`

- [ ] **Step 5: Run full suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: no new failures

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/
git commit -m "fix(converter): remove isinstance guard in _create_python_to_arrow_converter for GenericAlias support"
```

---

## Task 8: Wire up exports and `v0.1.json`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`

- [ ] **Step 1: Add exports to `__init__.py`**

In `src/orcapod/extension_types/__init__.py`, add the import after the `LogicalPandasSeries` import line:

```python
from .list_logical_type_factory import LIST_CATEGORY, SET_CATEGORY, ListLogicalType, ListLogicalTypeFactory
```

And add to `__all__`:

```python
    # ITL-173
    "LIST_CATEGORY",
    "SET_CATEGORY",
    "ListLogicalType",
    "ListLogicalTypeFactory",
```

- [ ] **Step 2: Register the factory in `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, find the `"factories"` array and add two entries after the existing `pydantic` factory entry:

```json
        {
            "factory": {
                "_class": "orcapod.extension_types.list_logical_type_factory.ListLogicalTypeFactory",
                "_config": {}
            },
            "category": "list"
        },
        {
            "factory": {
                "_class": "orcapod.extension_types.list_logical_type_factory.ListLogicalTypeFactory",
                "_config": {}
            },
            "category": "set"
        }
```

- [ ] **Step 3: Verify the default context loads cleanly**

```bash
uv run python -c "
from orcapod.contexts import get_default_context
ctx = get_default_context()
print('context_key:', ctx.context_key)
print('list factory registered:', ctx.type_converter._logical_type_registry._category_factories.get('list'))
print('set factory registered:', ctx.type_converter._logical_type_registry._category_factories.get('set'))
"
```

Expected output:
```
context_key: std:v0.1:default
list factory registered: <orcapod.extension_types.list_logical_type_factory.ListLogicalTypeFactory object ...>
set factory registered: <orcapod.extension_types.list_logical_type_factory.ListLogicalTypeFactory object ...>
```

- [ ] **Step 4: Verify public import works**

```bash
uv run python -c "
from orcapod.extension_types import ListLogicalType, ListLogicalTypeFactory, LIST_CATEGORY, SET_CATEGORY
print('LIST_CATEGORY:', LIST_CATEGORY)
print('SET_CATEGORY:', SET_CATEGORY)
print('OK')
"
```

Expected: `LIST_CATEGORY: list`, `SET_CATEGORY: set`, `OK`

- [ ] **Step 5: Run full suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: no failures

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/__init__.py src/orcapod/contexts/data/v0.1.json
git commit -m "feat(extension_types): export ListLogicalType/Factory and register in v0.1 context"
```

---

## Task 9: Integration round-trip tests

**Files:**
- Modify: `tests/test_extension_types/test_roundtrips.py`

Add module-level dataclasses and 9 new test functions covering all spec test cases.

- [ ] **Step 1: Add module-level dataclasses** (after existing `_Outer` definition)

```python
@dataclasses.dataclass
class _TaggedPoint:
    """Dataclass with a list[uuid.UUID] field — tests ET2 fix in DataclassLogicalTypeFactory."""
    name: str
    ids: list[uuid_module.UUID]


@dataclasses.dataclass
class _SimplePoint:
    """Dataclass with only scalar fields — used as element of list[_SimplePoint]."""
    label: str
    value: int
```

- [ ] **Step 2: Write all 9 failing tests**

Append to `tests/test_extension_types/test_roundtrips.py`:

```python
# ── list[T] / set[T] round-trip tests (ITL-173) ──────────────────────────────


def test_list_of_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """list[uuid.UUID] round-trips with extension name list[orcapod.uuid]."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")

    result, read_converter = _write_and_read(
        {"ids": list[uuid_module.UUID]},
        [{"ids": [u1, u2]}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("ids")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on 'ids', got {field.type!r}"
    )
    assert field.type.extension_name == "list[orcapod.uuid]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert rows[0]["ids"] == [u1, u2]
    assert all(isinstance(v, uuid_module.UUID) for v in rows[0]["ids"])


def test_set_of_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """set[uuid.UUID] round-trips with extension name set[orcapod.uuid]; read back as set."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")

    result, read_converter = _write_and_read(
        {"ids": set[uuid_module.UUID]},
        [{"ids": {u1, u2}}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("ids")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "set[orcapod.uuid]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["ids"], set)
    assert rows[0]["ids"] == {u1, u2}


def test_list_of_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """list[_SimplePoint] round-trips — ListLogicalType wrapping DataclassLogicalType."""
    p1 = _SimplePoint(label="alpha", value=1)
    p2 = _SimplePoint(label="beta", value=2)

    result, read_converter = _write_and_read(
        {"points": list[_SimplePoint]},
        [{"points": [p1, p2]}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("points")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name.startswith("list[")

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["points"]
    assert len(reconstructed) == 2
    assert reconstructed[0] == p1
    assert reconstructed[1] == p2


def test_list_of_list_of_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """list[list[uuid.UUID]] round-trips — two-level nesting."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")

    result, read_converter = _write_and_read(
        {"groups": list[list[uuid_module.UUID]]},
        [{"groups": [[u1, u2], [u2]]}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("groups")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "list[list[orcapod.uuid]]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert rows[0]["groups"] == [[u1, u2], [u2]]


def test_dataclass_with_list_uuid_field_round_trip(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """Dataclass with list[uuid.UUID] field round-trips (previously broke DataclassLogicalTypeFactory).

    Before this fix, registering _TaggedPoint raised ValueError because
    register_python_class(list[uuid.UUID]) was not yet supported.
    """
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    u2 = uuid_module.UUID("87654321-4321-8765-4321-876543218765")
    obj = _TaggedPoint(name="test", ids=[u1, u2])

    result, read_converter = _write_and_read(
        {"data": _TaggedPoint},
        [{"data": obj}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("data")
    assert hasattr(field.type, "extension_name")

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["data"]
    assert isinstance(reconstructed, _TaggedPoint)
    assert reconstructed.name == "test"
    assert reconstructed.ids == [u1, u2]
    assert all(isinstance(v, uuid_module.UUID) for v in reconstructed.ids)


def test_list_of_int_produces_no_extension_type(tmp_path: Path) -> None:
    """list[int] must still produce plain large_list(int64) — no ListLogicalType wrapping."""
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter
    result = converter.register_python_class(list[int])

    assert not isinstance(result, pa.ExtensionType), (
        f"list[int] must not be wrapped as an extension type, got {result!r}"
    )
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int64()


def test_schema_round_trip_list_of_uuid(tmp_path: Path) -> None:
    """arrow_schema_to_python_schema then python_schema_to_arrow_schema is identity for list[UUID]."""
    import uuid
    import pyarrow as pa
    from orcapod.contexts import create_registry

    converter = create_registry().get_context().type_converter

    python_schema = {
        "ids": list[uuid.UUID],
        "groups": list[list[uuid.UUID]],
        "tag_set": set[uuid.UUID],
    }
    arrow_schema = converter.python_schema_to_arrow_schema(python_schema)

    recovered_python = converter.arrow_schema_to_python_schema(arrow_schema)
    assert recovered_python["ids"] == list[uuid.UUID]
    assert recovered_python["groups"] == list[list[uuid.UUID]]
    assert recovered_python["tag_set"] == set[uuid.UUID]

    arrow_schema2 = converter.python_schema_to_arrow_schema(recovered_python)
    assert arrow_schema2.field("ids").type.extension_name == "list[orcapod.uuid]"
    assert arrow_schema2.field("groups").type.extension_name == "list[list[orcapod.uuid]]"
    assert arrow_schema2.field("tag_set").type.extension_name == "set[orcapod.uuid]"


def test_python_type_property_list_and_set(tmp_path: Path) -> None:
    """ListLogicalType.python_type returns the exact generic alias."""
    import uuid
    from orcapod.extension_types.builtin_logical_types import LogicalUUID
    from orcapod.extension_types.list_logical_type_factory import ListLogicalType

    uuid_ext = LogicalUUID().get_arrow_extension_type()

    lt_list = ListLogicalType(uuid.UUID, uuid_ext, is_set=False)
    assert lt_list.python_type == list[uuid.UUID]

    lt_set = ListLogicalType(uuid.UUID, uuid_ext, is_set=True)
    assert lt_set.python_type == set[uuid.UUID]


def test_fresh_converter_reads_list_of_uuid(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """A fresh converter (no prior registration) can read list[UUID] via load_extension_types."""
    u1 = uuid_module.UUID("12345678-1234-5678-1234-567812345678")

    # Write with converter A
    write_converter = _fresh_converter()
    write_converter.register_python_class(list[uuid_module.UUID])
    arrow_schema = write_converter.python_schema_to_arrow_schema({"ids": list[uuid_module.UUID]})
    table = write_converter.python_dicts_to_arrow_table([{"ids": [u1]}], arrow_schema=arrow_schema)
    storage_backend.write(table, tmp_path)

    # Read with converter B — no prior registration; load_extension_types triggers factory
    read_converter = _fresh_converter()
    result = storage_backend.read(tmp_path, read_converter)

    field = result.schema.field("ids")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == "list[orcapod.uuid]"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert rows[0]["ids"] == [u1]
    assert isinstance(rows[0]["ids"][0], uuid_module.UUID)
```

- [ ] **Step 3: Run tests to verify they fail (before implementation is complete)**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py -k "list_of_uuid or set_of_uuid or list_of_dataclass or list_of_list or dataclass_with_list or list_of_int or schema_round_trip_list or python_type_property or fresh_converter_reads" -v 2>&1 | tail -30
```

Expected: most `FAILED` (we haven't wired up `v0.1.json` yet — wait, we did that in Task 8. So actually these should mostly pass now. If Task 8 was done first, these may all pass immediately.)

- [ ] **Step 4: Run all round-trip tests**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py -v
```

Expected: all `PASSED`

- [ ] **Step 5: Run the full test suite**

```bash
uv run pytest tests/ -q 2>&1 | tail -20
```

Expected: all `PASSED`

- [ ] **Step 6: Commit**

```bash
git add tests/test_extension_types/test_roundtrips.py
git commit -m "test(roundtrips): add list[T], set[T], list[list[T]], and dataclass-with-list-field round-trip tests"
```

---

## Task 10: Update `DESIGN_ISSUES.md` and commit spec

**Files:**
- Modify: `DESIGN_ISSUES.md`
- Modify: `superpowers/specs/2026-08-11-itl-173-list-set-logical-types-design.md` (status update)

- [ ] **Step 1: Update ET2 entry in `DESIGN_ISSUES.md`**

Find the ET2 entry (search for `ET2` or `list[T]`). Update status to `resolved` and add a Fix note:

```
**Status:** resolved

**Fix:** Introduced `ListLogicalType` and `ListLogicalTypeFactory` in
`src/orcapod/extension_types/list_logical_type_factory.py`. The entire
`list[T]`/`set[T]` is wrapped as a top-level Arrow extension type with storage
`large_list(<T storage>)`, embedding element reconstruction metadata in the field-level
metadata JSON. Wired into `_register_python_class_impl` and `_convert_python_to_arrow`
via `_make_or_get_list_logical_type`. Registered in `v0.1.json` under categories
`"list"` and `"set"`. Closes ITL-173.
```

- [ ] **Step 2: Run full test suite one final time**

```bash
uv run pytest tests/ -q 2>&1 | tail -10
```

Expected: all `PASSED`, zero failures

- [ ] **Step 3: Commit**

```bash
git add DESIGN_ISSUES.md superpowers/specs/2026-08-11-itl-173-list-set-logical-types-design.md
git commit -m "docs: mark ET2 resolved in DESIGN_ISSUES.md; update spec status"
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task(s) |
|---|---|
| `list[T]` `register_python_class` creates `ListLogicalType` | Task 5 |
| `set[T]` same | Task 5 |
| `_convert_python_to_arrow` GenericAlias registry lookup | Task 6 |
| `_convert_python_to_arrow` list/set branches call `_make_or_get_list_logical_type` | Task 6 |
| `_create_python_to_arrow_converter` GenericAlias registry lookup | Task 7 |
| `ListLogicalType` construction, `python_type` generic alias | Task 3 |
| `ListLogicalType` ET1 safe storage (`large_list(<storage>)`) | Task 3 |
| `ListLogicalTypeFactory.reconstruct_from_arrow` recursive element registration | Task 4 |
| `registry.py` `__qualname__` guard | Task 1 |
| `TypeConverterProtocol.arrow_type_to_python_type` | Task 2 |
| Exports and `v0.1.json` registration | Task 8 |
| Test: `list[UUID]` round-trip | Task 9 |
| Test: `set[UUID]` round-trip | Task 9 |
| Test: `list[MyDataclass]` round-trip | Task 9 |
| Test: `list[list[UUID]]` round-trip | Task 9 |
| Test: dataclass with `list[UUID]` field | Task 9 |
| Test: `list[int]` unchanged | Task 9 |
| Test: schema round-trip | Task 9 |
| Test: `python_type` property | Task 9 |
| Test: fresh converter read | Task 9 |
| `DESIGN_ISSUES.md` ET2 resolved | Task 10 |

All spec requirements covered. No placeholders found. Type names consistent throughout (`ListLogicalType`, `ListLogicalTypeFactory`, `LIST_CATEGORY`, `SET_CATEGORY`, `_make_or_get_list_logical_type`).
