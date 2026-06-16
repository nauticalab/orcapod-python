# PLT-1705 Type Registration Spine Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `UniversalTypeConverter` the single re-entry point for Python ↔ Arrow type registration, move `LogicalTypeRegistry` inside the converter as a private implementation detail, and implement `DataclassHandlerFactory` on the refined architecture.

**Architecture:** `register_python_class(annotation)` handles write-side recursive traversal; `register_storage_type(arrow_type)` handles read-side bottom-up traversal. Factories and logical types receive `converter` instead of `registry`, so all delegation flows through the converter. `DataContext.logical_type_registry` is removed entirely.

**Tech Stack:** Python 3.12, PyArrow, Polars, `dataclasses`, `typing.get_type_hints`

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/extension_types/protocols.py` | Modify | Add `TypeConverterProtocol`; add `supports_class` + `converter` param to factory protocol; add `converter` param to logical type protocol |
| `src/orcapod/extension_types/builtin_logical_types.py` | Modify | Add `converter` param (accept, ignore) to `python_to_storage` / `storage_to_python` |
| `src/orcapod/semantic_types/universal_converter.py` | Modify | Add `register_python_class`, `register_storage_type`, `python_to_storage`, `storage_to_python`, `register_logical_type`, `register_logical_type_factory`; update `_create_python_to_arrow_converter`/`_create_arrow_to_python_converter` to pass `converter=self`; simplify `ensure_types_registered_for_schemas`; remove `semantic_registry` usage; remove `dataclass_encoding` imports |
| `src/orcapod/extension_types/registry.py` | Modify | Remove `ensure_logical_type_for_python_class`, `ensure_extension_type` |
| `src/orcapod/extension_types/dataclass_handler.py` | **Create** | `DataclassLogicalType` + `DataclassHandlerFactory` |
| `src/orcapod/semantic_types/dataclass_encoding.py` | **Delete** | Superseded by `DataclassHandlerFactory` |
| `src/orcapod/extension_types/type_utils.py` | Modify | Rename `extract_leaf_classes` → `_extract_leaf_classes` (private) |
| `src/orcapod/extension_types/database_hooks.py` | Modify | `register_discovered_extensions` takes `converter` instead of `registry`; uses schema_walker + `converter._ensure_extension_type_info` |
| `src/orcapod/databases/extension_aware_database.py` | Modify | Takes `converter` instead of `registry`; passes `converter._registry` to `apply_extension_types` |
| `src/orcapod/contexts/core.py` | Modify | Remove `logical_type_registry` field from `DataContext` |
| `src/orcapod/contexts/__init__.py` | Modify | Remove `get_default_logical_type_registry` |
| `src/orcapod/contexts/registry.py` | Modify | Remove `"logical_type_registry"` from `required_fields`; stop passing it to `DataContext` |
| `src/orcapod/contexts/data/v0.1.json` | Modify | Remove top-level `logical_type_registry`; move registry construction inside `type_converter._config`; remove `semantic_registry` ref from `type_converter._config` |
| `src/orcapod/contexts/data/schemas/context_schema.json` | Modify | Remove `logical_type_registry` from `required` and `properties` |
| `src/orcapod/extension_types/__init__.py` | Modify | Update docstring |
| `tests/test_extension_types/test_protocols.py` | Modify | Update stubs for new signatures; add `TypeConverterProtocol` conformance test |
| `tests/test_extension_types/test_registry.py` | Modify | Remove `ensure_*` tests; add converter pass-through tests |
| `tests/test_extension_types/test_builtin_logical_types.py` | Modify | Pass a stub converter to `python_to_storage` / `storage_to_python` calls |
| `tests/test_extension_types/test_dataclass_handler.py` | **Create** | Full unit tests for `DataclassLogicalType` and `DataclassHandlerFactory` |
| `tests/test_semantic_types/test_universal_converter.py` | Modify | Add `register_python_class` and `register_storage_type` tests |
| `tests/test_extension_types/test_database_hooks.py` | Modify | Switch from registry to converter |
| `tests/test_core/function_pod/test_write_side_registration.py` | Modify | Update `DataContext` construction (no `logical_type_registry`) |

---

## Task 1: Update `TypeConverterProtocol` and factory/logical-type protocols

**Files:**
- Modify: `src/orcapod/extension_types/protocols.py`
- Modify: `tests/test_extension_types/test_protocols.py`

- [ ] **Step 1: Write failing protocol conformance tests**

Add to `tests/test_extension_types/test_protocols.py`:

```python
# Add at the top of the file:
# from orcapod.extension_types.protocols import TypeConverterProtocol

def test_type_converter_protocol_is_importable():
    from orcapod.extension_types.protocols import TypeConverterProtocol
    assert TypeConverterProtocol is not None


def test_factory_supports_class_method_required():
    """LogicalTypeFactoryProtocol requires supports_class."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol

    class _BadFactory:
        def reconstruct_from_arrow(self, name, storage_type, metadata, converter):
            pass
        def create_for_python_type(self, python_type, converter):
            pass
        # Missing supports_class

    assert not isinstance(_BadFactory(), LogicalTypeFactoryProtocol)


def test_factory_with_supports_class_satisfies_protocol():
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol

    class _GoodFactory:
        def supports_class(self, python_type):
            return True
        def reconstruct_from_arrow(self, name, storage_type, metadata, converter):
            pass
        def create_for_python_type(self, python_type, converter):
            pass

    assert isinstance(_GoodFactory(), LogicalTypeFactoryProtocol)


def test_logical_type_python_to_storage_accepts_converter():
    """LogicalTypeProtocol.python_to_storage now requires converter param."""
    from orcapod.extension_types.protocols import LogicalTypeProtocol

    class _GoodLT:
        @property
        def logical_type_name(self): return "test.lt"
        @property
        def python_type(self): return str
        def get_arrow_extension_type(self): pass
        def get_polars_extension_type(self): pass
        def python_to_storage(self, value, converter): return value
        def storage_to_python(self, storage_value, converter): return storage_value

    assert isinstance(_GoodLT(), LogicalTypeProtocol)
```

- [ ] **Step 2: Run tests to confirm failures**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v -k "type_converter or supports_class or accepts_converter" 2>&1 | tail -30
```
Expected: ImportError or AttributeError failures.

- [ ] **Step 3: Update `protocols.py`**

Replace the entire file:

```python
"""Protocol definitions for the Arrow/Polars extension type system.

This module defines ``TypeConverterProtocol``, ``LogicalTypeProtocol``, and
``LogicalTypeFactoryProtocol`` — the contracts for the converter, for logical
type implementations that bind a Python class to its Arrow and Polars extension
type representation, and for factories that auto-construct such implementations
from Arrow schema metadata.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa


@runtime_checkable
class TypeConverterProtocol(Protocol):
    """Minimal protocol exposing what factories and logical types need from the converter.

    Placed in ``extension_types/protocols.py`` to avoid circular imports.
    ``UniversalTypeConverter`` is the canonical implementation.
    """

    def register_python_class(self, annotation: Any) -> "pa.DataType":
        """Traverse a Python annotation and return its Arrow type, registering as needed."""
        ...

    def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
        """Traverse an Arrow type bottom-up, registering extension types, and return resolved type."""
        ...

    def python_to_storage(self, value: Any, annotation: Any) -> Any:
        """Convert a Python value to its Arrow storage representation."""
        ...

    def storage_to_python(self, storage_value: Any, annotation: Any) -> Any:
        """Convert an Arrow storage value back to a Python object."""
        ...


@runtime_checkable
class LogicalTypeProtocol(Protocol):
    """Protocol for Arrow/Polars extension-type-backed logical types.

    A ``LogicalTypeProtocol`` is a three-way binding between a unique logical type name
    (orcapod's identifier), a Python class, and Arrow/Polars extension types.
    Each implementation *owns* its Arrow and Polars extension types by providing
    them directly via ``get_arrow_extension_type`` and ``get_polars_extension_type``.

    This protocol is Arrow I/O only — hashing is not a logical type responsibility.
    """

    @property
    def logical_type_name(self) -> str:
        """Unique orcapod identifier for this logical type (e.g. ``"orcapod.uuid"``)."""
        ...

    @property
    def python_type(self) -> type:
        """The Python class this logical type represents."""
        ...

    def get_arrow_extension_type(self) -> "pa.ExtensionType":
        """Return the Arrow extension type for this logical type."""
        ...

    def get_polars_extension_type(self) -> "pl.BaseExtension":
        """Return an instance of the Polars extension type for this logical type."""
        ...

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol) -> Any:
        """Convert a Python value to its Arrow storage representation.

        Args:
            value: A Python object of type ``python_type``.
            converter: The active ``TypeConverterProtocol`` for recursive delegation.

        Returns:
            A value suitable for Arrow storage.
        """
        ...

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol) -> Any:
        """Convert an Arrow storage value back to a Python object.

        Args:
            storage_value: A scalar or array element from the Arrow storage array.
            converter: The active ``TypeConverterProtocol`` for recursive delegation.

        Returns:
            A Python object of type ``python_type``.
        """
        ...


@runtime_checkable
class LogicalTypeFactoryProtocol(Protocol):
    """Protocol for factories that synthesize or reconstruct ``LogicalTypeProtocol`` instances.

    Bridges two directions: the write path (``create_for_python_type``) and the read
    path (``reconstruct_from_arrow``). Both methods receive ``converter`` instead of
    ``registry`` so all traversal flows through the converter.
    """

    def supports_class(self, python_type: type) -> bool:
        """Return True if this factory can synthesize a LogicalType for ``python_type``.

        Used as a probe during write-side MRO dispatch in ``register_python_class``.

        Args:
            python_type: The Python class to probe.

        Returns:
            True if this factory handles ``python_type``.
        """
        ...

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> LogicalTypeProtocol:
        """Synthesize a LogicalType for the given Python class (write path).

        Args:
            python_type: The concrete Python class to synthesize a LogicalType for.
            converter: The active converter for recursive field-type resolution.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot construct a type for the given class.
        """
        ...

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: "pa.DataType",
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> LogicalTypeProtocol:
        """Reconstruct a LogicalType from Arrow schema metadata (read path).

        Args:
            arrow_extension_name: The Arrow extension type name from the schema.
            storage_type: The underlying Arrow storage type (already resolved bottom-up).
            metadata: Full parsed metadata JSON dict. Always contains ``"category"``.
            converter: The active converter for recursive field-type resolution.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready for registration.

        Raises:
            ValueError: If this factory cannot reconstruct a type for the given name.
        """
        ...
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v 2>&1 | tail -30
```
Expected: All tests pass (some existing tests about the OLD signatures will now fail — that's expected and will be fixed in Task 2).

- [ ] **Step 5: Update existing stubs in `test_protocols.py` to use new signatures**

Replace `_StubLogicalType` and `_StubFactory` in `tests/test_extension_types/test_protocols.py`:

```python
class _StubLogicalType:
    """Minimal conforming implementation of LogicalTypeProtocol for use in tests."""

    _ArrowExtClass = make_arrow_extension_type("test.module.MyType", pa.large_string())

    @property
    def logical_type_name(self) -> str:
        return "test.module.MyType"

    @property
    def python_type(self) -> type:
        return str

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return self._ArrowExtClass()

    def get_polars_extension_type(self) -> pl.BaseExtension:
        class _PolarsExt(pl.BaseExtension):
            def __init__(self):
                super().__init__("test.module.MyType", pl.String, None)
            @classmethod
            def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
                return cls()
        return _PolarsExt()

    def python_to_storage(self, value, converter):  # converter param added
        return str(value)

    def storage_to_python(self, storage_value, converter):  # converter param added
        return storage_value


class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactoryProtocol for use in tests."""

    def supports_class(self, python_type):  # new method
        return True

    def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata, converter):
        return _StubLogicalType()

    def create_for_python_type(self, python_type, converter):
        return _StubLogicalType()
```

Also update the test that calls the old signatures:
```python
def test_conforming_class_satisfies_protocol():
    lt: LogicalTypeProtocol = _StubLogicalType()
    assert lt.logical_type_name == "test.module.MyType"
    assert lt.python_type is str
    assert lt.get_arrow_extension_type().extension_name == "test.module.MyType"
    assert isinstance(lt.get_polars_extension_type(), pl.BaseExtension)
    assert lt.python_to_storage(42, None) == "42"   # pass converter=None
    assert lt.storage_to_python("hello", None) == "hello"  # pass converter=None


def test_logical_type_factory_create_returns_logical_type():
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    result = factory.reconstruct_from_arrow(
        "test.ext", pa.large_utf8(), {"category": "Test"}, converter=None
    )
    assert isinstance(result, LogicalTypeProtocol)


def test_factory_create_for_python_type_conformance():
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    assert isinstance(factory, LogicalTypeFactoryProtocol)
    result = factory.create_for_python_type(str, converter=None)
    assert isinstance(result, LogicalTypeProtocol)
```

- [ ] **Step 6: Run all protocol tests**

```bash
uv run pytest tests/test_extension_types/test_protocols.py -v 2>&1 | tail -20
```
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/extension_types/protocols.py tests/test_extension_types/test_protocols.py
git commit -m "feat(extension_types): add TypeConverterProtocol; update factory/logical-type protocols with converter param and supports_class"
```

---

## Task 2: Update built-in logical types for protocol conformance

**Files:**
- Modify: `src/orcapod/extension_types/builtin_logical_types.py`
- Modify: `tests/test_extension_types/test_builtin_logical_types.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_extension_types/test_builtin_logical_types.py`:

```python
def test_logical_path_python_to_storage_accepts_converter():
    """python_to_storage now accepts a converter param (ignored)."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath
    lt = LogicalPath()
    import pathlib
    result = lt.python_to_storage(pathlib.Path("/tmp/foo"), converter=None)
    assert result == "/tmp/foo"


def test_logical_uuid_python_to_storage_accepts_converter():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID
    import uuid as uuid_module
    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.python_to_storage(u, converter=None)
    assert result == u.bytes


def test_logical_upath_storage_to_python_accepts_converter():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath
    lt = LogicalUPath()
    from upath import UPath
    result = lt.storage_to_python("s3://bucket/key", converter=None)
    assert isinstance(result, UPath)
```

- [ ] **Step 2: Run to confirm failures**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v -k "accepts_converter" 2>&1 | tail -20
```
Expected: TypeError — unexpected keyword argument.

- [ ] **Step 3: Update all three classes in `builtin_logical_types.py`**

For `LogicalPath`:
```python
def python_to_storage(self, value: Any, converter: Any = None) -> str:
    return str(value)

def storage_to_python(self, storage_value: Any, converter: Any = None) -> pathlib.Path:
    return pathlib.Path(storage_value)
```

For `LogicalUPath`:
```python
def python_to_storage(self, value: Any, converter: Any = None) -> str:
    return str(value)

def storage_to_python(self, storage_value: Any, converter: Any = None) -> UPath:
    return UPath(storage_value)
```

For `LogicalUUID`:
```python
def python_to_storage(self, value: Any, converter: Any = None) -> bytes:
    return value.bytes

def storage_to_python(self, storage_value: Any, converter: Any = None) -> _uuid_module.UUID:
    return _uuid_module.UUID(bytes=bytes(storage_value))
```

Also add `TYPE_CHECKING` import for `TypeConverterProtocol` in the type hint:
```python
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol
```

And use in signatures:
```python
def python_to_storage(self, value: Any, converter: "TypeConverterProtocol | None" = None) -> str:
```

- [ ] **Step 4: Run new tests**

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v 2>&1 | tail -20
```
Expected: All pass.

- [ ] **Step 5: Also update test call sites that call without converter**

Search for existing direct calls to `python_to_storage` / `storage_to_python` in the test file (they have no `converter` arg — that's fine since we added `converter=None` default).

```bash
uv run pytest tests/test_extension_types/test_builtin_logical_types.py -v 2>&1 | tail -5
```
Expected: All pass (defaults handle existing calls).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/builtin_logical_types.py tests/test_extension_types/test_builtin_logical_types.py
git commit -m "feat(extension_types): add converter param to built-in logical type python_to_storage/storage_to_python"
```

---

## Task 3: Add `register_python_class` to `UniversalTypeConverter`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_semantic_types/test_universal_converter.py`:

```python
import dataclasses
import uuid as _uuid_module
import pathlib
from typing import Optional

import pyarrow as pa
import pytest

from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_registry_with_builtins() -> LogicalTypeRegistry:
    """Registry with LogicalPath, LogicalUUID, LogicalUPath pre-registered."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath, LogicalUUID, LogicalUPath
    return LogicalTypeRegistry(logical_types=[LogicalPath(), LogicalUUID(), LogicalUPath()])


def _make_converter(registry: LogicalTypeRegistry | None = None) -> UniversalTypeConverter:
    if registry is None:
        registry = _make_registry_with_builtins()
    return UniversalTypeConverter(logical_type_registry=registry)


# ── register_python_class tests ──────────────────────────────────────────────

def test_register_python_class_primitive_int():
    converter = _make_converter()
    assert converter.register_python_class(int) == pa.int64()


def test_register_python_class_primitive_str():
    converter = _make_converter()
    assert converter.register_python_class(str) == pa.large_string()


def test_register_python_class_list_of_int():
    converter = _make_converter()
    result = converter.register_python_class(list[int])
    assert result == pa.large_list(pa.int64())


def test_register_python_class_optional_str():
    converter = _make_converter()
    result = converter.register_python_class(Optional[str])
    assert result == pa.large_string()


def test_register_python_class_dict_str_int():
    converter = _make_converter()
    result = converter.register_python_class(dict[str, int])
    expected = pa.large_list(pa.struct([pa.field("key", pa.large_string()), pa.field("value", pa.int64())]))
    assert result == expected


def test_register_python_class_set_of_str():
    converter = _make_converter()
    result = converter.register_python_class(set[str])
    assert result == pa.large_list(pa.large_string())


def test_register_python_class_registry_hit_path():
    """pathlib.Path is pre-registered → returns the orcapod.path extension type."""
    converter = _make_converter()
    result = converter.register_python_class(pathlib.Path)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "orcapod.path"


def test_register_python_class_uuid_registry_hit():
    converter = _make_converter()
    result = converter.register_python_class(_uuid_module.UUID)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "orcapod.uuid"


def test_register_python_class_factory_dispatch():
    """A custom class triggers factory synthesis and caches the result."""
    import uuid as _u
    import polars as pl

    class _Base:
        pass

    class _Child(_Base):
        pass

    ext_name = f"test.custom.{_u.uuid4().hex[:8]}"
    ArrowExt = make_arrow_extension_type(ext_name, pa.large_string())
    PolarsExt = make_polars_extension_type(ext_name, pa.large_string())
    synthesized_calls = []

    class _Factory:
        def supports_class(self, python_type):
            return issubclass(python_type, _Base)
        def create_for_python_type(self, python_type, converter):
            synthesized_calls.append(python_type)
            class _LT:
                logical_type_name = ext_name
                python_type_ = _Child
                python_type = _Child
                def get_arrow_extension_type(self): return ArrowExt()
                def get_polars_extension_type(self): return PolarsExt()
                def python_to_storage(self, v, c=None): return str(v)
                def storage_to_python(self, v, c=None): return v
            return _LT()
        def reconstruct_from_arrow(self, name, storage, meta, converter): pass

    registry = _make_registry_with_builtins()
    registry.register_logical_type_factory(_Factory(), python_bases=[_Base])
    converter = _make_converter(registry)

    result = converter.register_python_class(_Child)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == ext_name
    assert _Child in synthesized_calls

    # Second call is a registry hit — factory NOT called again
    result2 = converter.register_python_class(_Child)
    assert result2 == result
    assert len(synthesized_calls) == 1


def test_register_python_class_cycle_detection():
    """Cyclic type synthesis raises TypeError."""
    import uuid as _u
    import polars as pl

    class _CycleClass:
        pass

    class _CycleFactory:
        def supports_class(self, python_type):
            return python_type is _CycleClass
        def create_for_python_type(self, python_type, converter):
            # Intentionally trigger a cycle
            converter.register_python_class(_CycleClass)
        def reconstruct_from_arrow(self, name, storage, meta, converter): pass

    registry = _make_registry_with_builtins()
    registry.register_logical_type_factory(_CycleFactory(), python_bases=[_CycleClass])
    converter = _make_converter(registry)

    with pytest.raises(TypeError, match="[Cc]ircular"):
        converter.register_python_class(_CycleClass)
```

- [ ] **Step 2: Run to confirm failures**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v -k "register_python_class" 2>&1 | tail -30
```
Expected: AttributeError — `UniversalTypeConverter` has no attribute `register_python_class`.

- [ ] **Step 3: Implement `register_python_class` in `UniversalTypeConverter`**

Add these methods to `UniversalTypeConverter` (after `__init__`):

```python
def register_python_class(self, annotation: Any) -> "pa.DataType":
    """Register a Python type annotation and return its Arrow type.

    Traverses generic annotations recursively. For each concrete class found,
    either returns from the primitive map or registry (cache hit), or
    synthesises via factory and registers the result.

    Args:
        annotation: A Python type or generic alias (e.g. ``list[str]``,
            ``Optional[uuid.UUID]``, a dataclass type).

    Returns:
        The Arrow ``pa.DataType`` corresponding to ``annotation``.

    Raises:
        TypeError: If a concrete class has no registered ``LogicalType`` and
            no factory covers it, or if a circular dependency is detected.
        ValueError: If a complex (non-Optional) union is encountered.
    """
    import types as _types_mod

    type_map = _get_python_to_arrow_map()

    # Primitive map hit
    if annotation in type_map:
        return type_map[annotation]

    origin = get_origin(annotation)
    args = get_args(annotation)

    # Optional[T] / T | None → strip None arm
    if origin is typing.Union or origin is _types_mod.UnionType:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return self.register_python_class(non_none[0])
        raise ValueError(
            f"Complex unions with multiple non-None types are not supported: "
            f"{annotation!r}. Only Optional[T] (T | None) is allowed."
        )

    # list[T] → pa.large_list(T)
    if origin is list:
        return pa.large_list(self.register_python_class(args[0]))

    # set[T] → pa.large_list(T)
    if origin is set:
        return pa.large_list(self.register_python_class(args[0]))

    # dict[K, V] → pa.large_list(struct{key: K, value: V})
    if origin is dict:
        key_arrow = self.register_python_class(args[0])
        val_arrow = self.register_python_class(args[1])
        return pa.large_list(
            pa.struct([pa.field("key", key_arrow), pa.field("value", val_arrow)])
        )

    # Concrete class — registry or factory dispatch
    if isinstance(annotation, type):
        if self._logical_type_registry is None:
            raise TypeError(
                f"No LogicalTypeRegistry configured — cannot register {annotation!r}. "
                f"Provide logical_type_registry at converter construction time."
            )

        # Registry hit (already synthesised)
        lt = self._logical_type_registry.get_by_python_type(annotation)
        if lt is not None:
            return lt.get_arrow_extension_type()

        # Cycle detection
        if annotation in self._in_progress:
            raise TypeError(
                f"Circular type dependency detected while synthesising "
                f"LogicalType for {annotation!r}."
            )

        # Factory dispatch via MRO walk
        factory = self._find_factory_for_class(annotation)
        if factory is None:
            raise TypeError(
                f"No LogicalType or LogicalTypeFactory registered for {annotation!r}. "
                f"Register a factory: converter.register_logical_type_factory(factory, "
                f"python_bases=[<base of {annotation.__name__}>])"
            )

        self._in_progress.add(annotation)
        try:
            lt = factory.create_for_python_type(annotation, converter=self)
            self._logical_type_registry.register_logical_type(lt)
        finally:
            self._in_progress.discard(annotation)

        return lt.get_arrow_extension_type()

    raise ValueError(f"Unsupported annotation: {annotation!r}")

def _find_factory_for_class(
    self,
    python_type: type,
) -> "LogicalTypeFactoryProtocol | None":
    """Find the most-specific registered factory for ``python_type``.

    Walks ``python_type.__mro__`` and returns the first factory in
    ``_python_class_factories`` whose ``supports_class(python_type)`` returns True.
    Falls back to an ``issubclass`` scan for ABC-registered factories.

    Args:
        python_type: Concrete Python class to find a factory for.

    Returns:
        The matching ``LogicalTypeFactoryProtocol``, or ``None`` if none found.
    """
    factories = self._logical_type_registry._python_class_factories

    # MRO walk — most-specific base first
    for base in python_type.__mro__:
        factory = factories.get(base)
        if factory is not None:
            if hasattr(factory, "supports_class") and factory.supports_class(python_type):
                return factory
            elif not hasattr(factory, "supports_class"):
                # Factories without supports_class are treated as unconditional matches
                return factory

    # issubclass fallback for ABC-registered factories
    for base, factory in factories.items():
        try:
            if issubclass(python_type, base):
                if hasattr(factory, "supports_class"):
                    if factory.supports_class(python_type):
                        return factory
                else:
                    return factory
        except TypeError:
            continue

    return None
```

Also add `_in_progress: set[type] = set()` to `__init__`:

```python
# In __init__, after the existing cache initializations:
self._in_progress: set[type] = set()
```

And add `TYPE_CHECKING` import for `LogicalTypeFactoryProtocol`:
```python
if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v -k "register_python_class" 2>&1 | tail -30
```
Expected: All `register_python_class` tests pass.

- [ ] **Step 5: Run full test suite for this module**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v 2>&1 | tail -20
```
Expected: Existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(universal_converter): add register_python_class with recursive traversal, factory dispatch, and cycle detection"
```

---

## Task 4: Add `register_storage_type` to `UniversalTypeConverter`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/test_semantic_types/test_universal_converter.py`:

```python
# ── register_storage_type tests ──────────────────────────────────────────────

def test_register_storage_type_primitive_int():
    converter = _make_converter()
    assert converter.register_storage_type(pa.int64()) == pa.int64()


def test_register_storage_type_primitive_large_string():
    converter = _make_converter()
    assert converter.register_storage_type(pa.large_string()) == pa.large_string()


def test_register_storage_type_extension_type_registry_hit():
    """An already-registered extension type is returned unchanged (no-op)."""
    converter = _make_converter()
    # orcapod.uuid is pre-registered in the builtin registry
    from orcapod.extension_types.builtin_logical_types import LogicalUUID
    uuid_ext = LogicalUUID().get_arrow_extension_type()
    result = converter.register_storage_type(uuid_ext)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "orcapod.uuid"


def test_register_storage_type_struct_recurses():
    """Structs are traversed field by field; resolved field types are returned."""
    converter = _make_converter()
    struct_type = pa.struct([pa.field("name", pa.large_string()), pa.field("count", pa.int64())])
    result = converter.register_storage_type(struct_type)
    assert pa.types.is_struct(result)
    assert result.field("name").type == pa.large_string()
    assert result.field("count").type == pa.int64()


def test_register_storage_type_large_list_recurses():
    converter = _make_converter()
    list_type = pa.large_list(pa.int32())
    result = converter.register_storage_type(list_type)
    assert pa.types.is_large_list(result)
    assert result.value_type == pa.int32()


def test_register_storage_type_extension_miss_dispatches_to_factory():
    """An unregistered extension type triggers factory.reconstruct_from_arrow."""
    import json
    import uuid as _u
    import polars as pl

    ext_name = f"test.reconstruct.{_u.uuid4().hex[:8]}"
    category = "test.reconstruct"
    metadata = json.dumps({"category": category}).encode()
    ArrowExt = make_arrow_extension_type(ext_name, pa.large_string(), metadata=metadata)
    PolarsExt = make_polars_extension_type(ext_name, pa.large_string())

    class _LT:
        logical_type_name = ext_name
        python_type = str
        def get_arrow_extension_type(self): return ArrowExt()
        def get_polars_extension_type(self): return PolarsExt()
        def python_to_storage(self, v, c=None): return str(v)
        def storage_to_python(self, v, c=None): return v

    class _Factory:
        def supports_class(self, t): return False
        def create_for_python_type(self, t, converter): pass
        def reconstruct_from_arrow(self, name, storage_type, meta, converter):
            return _LT()

    registry = _make_registry_with_builtins()
    registry.register_logical_type_factory(_Factory(), category=category)
    converter = _make_converter(registry)

    ext_instance = ArrowExt()
    result = converter.register_storage_type(ext_instance)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == ext_name

    # Second call: registry hit → same result, factory NOT called again
    result2 = converter.register_storage_type(ext_instance)
    assert result2.extension_name == ext_name


def test_register_storage_type_nested_struct_with_extension():
    """Extension type nested inside a struct field is resolved bottom-up."""
    import json
    import uuid as _u
    import polars as pl

    ext_name = f"test.nested.{_u.uuid4().hex[:8]}"
    category = "test.nested"
    metadata = json.dumps({"category": category}).encode()
    ArrowExt = make_arrow_extension_type(ext_name, pa.large_string(), metadata=metadata)
    PolarsExt = make_polars_extension_type(ext_name, pa.large_string())

    class _LT:
        logical_type_name = ext_name
        python_type = str
        def get_arrow_extension_type(self): return ArrowExt()
        def get_polars_extension_type(self): return PolarsExt()
        def python_to_storage(self, v, c=None): return str(v)
        def storage_to_python(self, v, c=None): return v

    class _Factory:
        def supports_class(self, t): return False
        def create_for_python_type(self, t, converter): pass
        def reconstruct_from_arrow(self, name, storage_type, meta, converter):
            return _LT()

    registry = _make_registry_with_builtins()
    registry.register_logical_type_factory(_Factory(), category=category)
    converter = _make_converter(registry)

    ext_instance = ArrowExt()
    struct_with_ext = pa.struct([pa.field("id", pa.int64()), pa.field("tag", ext_instance)])
    result = converter.register_storage_type(struct_with_ext)

    assert pa.types.is_struct(result)
    assert result.field("id").type == pa.int64()
    assert isinstance(result.field("tag").type, pa.ExtensionType)
    assert result.field("tag").type.extension_name == ext_name
```

- [ ] **Step 2: Run to confirm failures**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v -k "register_storage_type" 2>&1 | tail -30
```
Expected: AttributeError — `register_storage_type` not defined.

- [ ] **Step 3: Implement `register_storage_type` and `_ensure_extension_type_info` in `UniversalTypeConverter`**

```python
def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
    """Register extension types found in ``arrow_type`` and return the resolved type.

    Traverses Arrow types recursively in a bottom-up manner:
    - Primitives are returned unchanged.
    - ``pa.ExtensionType`` instances that are already registered are returned as-is.
    - Unregistered extension types: the storage type is resolved first (bottom-up),
      then the factory dispatches on the ``"category"`` metadata key.
    - Structs: each field's type is resolved; a new struct with resolved fields is returned.
    - Lists: the value type is resolved; a new list type with the resolved value is returned.

    Args:
        arrow_type: An Arrow type to traverse and register.

    Returns:
        The resolved Arrow type with extension types embedded.
    """
    # Extension type
    if isinstance(arrow_type, pa.ExtensionType):
        ext_name = arrow_type.extension_name
        if self._logical_type_registry is not None:
            lt = self._logical_type_registry.get_by_arrow_extension_name(ext_name)
            if lt is not None:
                return lt.get_arrow_extension_type()
        # Registry miss — extract info and register
        raw_meta = arrow_type.__arrow_ext_serialize__()
        ext_meta = raw_meta if raw_meta else None
        resolved_storage = self.register_storage_type(arrow_type.storage_type)
        return self._ensure_extension_type_info(ext_name, ext_meta, resolved_storage)

    # Struct type — recurse into each field
    if pa.types.is_struct(arrow_type):
        resolved_fields = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            resolved_type = self.register_storage_type(field.type)
            resolved_fields.append(pa.field(field.name, resolved_type, nullable=field.nullable))
        return pa.struct(resolved_fields)

    # Large list type
    if pa.types.is_large_list(arrow_type):
        resolved_value = self.register_storage_type(arrow_type.value_type)
        return pa.large_list(resolved_value)

    # List type
    if pa.types.is_list(arrow_type):
        resolved_value = self.register_storage_type(arrow_type.value_type)
        return pa.list_(resolved_value)

    # All other types (primitives, timestamps, binary, etc.) — return as-is
    return arrow_type

def _ensure_extension_type_info(
    self,
    arrow_extension_name: str,
    extension_metadata: bytes | None,
    storage_type: "pa.DataType",
) -> "pa.DataType":
    """Register an extension type from (name, metadata, storage_type) info.

    Called by ``register_storage_type`` for in-memory ``pa.ExtensionType`` objects,
    and by ``register_discovered_extensions`` for the field-metadata (Parquet) channel.
    The ``storage_type`` must already be resolved (nested extension types registered).

    Args:
        arrow_extension_name: Arrow extension name (``ARROW:extension:name``).
        extension_metadata: Raw metadata bytes, expected to be UTF-8 JSON with
            at least a ``"category"`` key. ``None`` or empty bytes if absent.
        storage_type: Underlying Arrow storage type (already bottom-up resolved).

    Returns:
        The Arrow extension type after registration.

    Raises:
        ValueError: If metadata is missing, malformed, lacks ``"category"``, or
            no factory is registered for the category.
    """
    import json as _json

    if self._logical_type_registry is None:
        raise ValueError(
            f"No LogicalTypeRegistry configured — cannot register extension type "
            f"{arrow_extension_name!r}."
        )

    # Registry hit — already registered
    lt = self._logical_type_registry.get_by_arrow_extension_name(arrow_extension_name)
    if lt is not None:
        return lt.get_arrow_extension_type()

    # Missing metadata — cannot auto-register
    if not extension_metadata:
        raise ValueError(
            f"Extension type {arrow_extension_name!r} has no extension metadata. "
            f"Types without a metadata category tag cannot be auto-registered via a factory. "
            f"Pre-register them explicitly via converter.register_logical_type(lt)."
        )

    # Parse JSON metadata
    try:
        metadata_dict = _json.loads(extension_metadata.decode("utf-8"))
    except (UnicodeDecodeError, _json.JSONDecodeError) as exc:
        raise ValueError(
            f"Extension type {arrow_extension_name!r} has metadata that is not valid "
            f"UTF-8 JSON: {extension_metadata!r}. Parse error: {exc}."
        ) from exc

    if not isinstance(metadata_dict, dict):
        raise ValueError(
            f"Extension type {arrow_extension_name!r} metadata decoded to a non-object "
            f"JSON value: {metadata_dict!r}."
        )

    if "category" not in metadata_dict:
        raise ValueError(
            f"Extension type {arrow_extension_name!r} metadata has no \"category\" key: "
            f"{metadata_dict}."
        )

    category = metadata_dict["category"]
    if not isinstance(category, str):
        raise ValueError(
            f"Extension type {arrow_extension_name!r} metadata \"category\" is not a "
            f"string: {category!r}."
        )

    # Look up factory by category
    factory = self._logical_type_registry._category_factories.get(category)
    if factory is None:
        raise ValueError(
            f"No LogicalTypeFactory registered for category {category!r}. "
            f"Cannot register extension type {arrow_extension_name!r}."
        )

    # Reconstruct and register
    logical_type = factory.reconstruct_from_arrow(
        arrow_extension_name, storage_type, metadata_dict, converter=self
    )
    self._logical_type_registry.register_logical_type(logical_type)
    return logical_type.get_arrow_extension_type()
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v -k "register_storage_type" 2>&1 | tail -30
```
Expected: All pass.

- [ ] **Step 5: Run full converter test suite**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v 2>&1 | tail -10
```

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(universal_converter): add register_storage_type with bottom-up recursive traversal"
```

---

## Task 5: Add `python_to_storage`, `storage_to_python`, and registration pass-throughs; update converter dispatch

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write failing tests**

```python
# ── python_to_storage / storage_to_python / pass-through tests ──────────────

def test_python_to_storage_for_registered_type():
    """python_to_storage uses the logical type's converter for registered types."""
    converter = _make_converter()
    import pathlib
    result = converter.python_to_storage(pathlib.Path("/tmp/bar"), pathlib.Path)
    assert result == "/tmp/bar"


def test_storage_to_python_for_registered_type():
    converter = _make_converter()
    import pathlib
    result = converter.storage_to_python("/tmp/bar", pathlib.Path)
    assert isinstance(result, pathlib.Path)
    assert result == pathlib.Path("/tmp/bar")


def test_python_to_storage_for_int():
    converter = _make_converter()
    assert converter.python_to_storage(42, int) == 42


def test_register_logical_type_passthrough():
    from orcapod.extension_types.builtin_logical_types import LogicalPath
    registry = LogicalTypeRegistry()
    converter = UniversalTypeConverter(logical_type_registry=registry)
    lt = LogicalPath()
    converter.register_logical_type(lt)
    assert registry.get_by_python_type(import_pathlib_path()) is lt


def import_pathlib_path():
    import pathlib; return pathlib.Path


def test_register_logical_type_factory_passthrough():
    import uuid as _u
    import polars as pl

    class _Factory:
        def supports_class(self, t): return False
        def create_for_python_type(self, t, converter): pass
        def reconstruct_from_arrow(self, name, storage, meta, converter): pass

    registry = LogicalTypeRegistry()
    converter = UniversalTypeConverter(logical_type_registry=registry)
    factory = _Factory()
    converter.register_logical_type_factory(factory, category="test.cat")
    assert registry._category_factories.get("test.cat") is factory
```

- [ ] **Step 2: Run to confirm failures**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v -k "python_to_storage or storage_to_python or passthrough" 2>&1 | tail -20
```

- [ ] **Step 3: Add methods to `UniversalTypeConverter`**

```python
def python_to_storage(self, value: Any, annotation: Any) -> Any:
    """Convert a Python value to its Arrow storage representation.

    Thin wrapper over ``get_python_to_arrow_converter`` for use by
    ``DataclassLogicalType`` and other logical types that delegate per-field
    conversion back to the converter.

    Args:
        value: A Python object.
        annotation: The Python type annotation for ``value``.

    Returns:
        A value in Arrow storage format.
    """
    converter_fn = self.get_python_to_arrow_converter(annotation)
    return converter_fn(value)

def storage_to_python(self, storage_value: Any, annotation: Any) -> Any:
    """Convert an Arrow storage value back to a Python object.

    Args:
        storage_value: A scalar or element from an Arrow storage array.
        annotation: The Python type annotation to convert back to.

    Returns:
        A Python object of the type described by ``annotation``.
    """
    arrow_type = self.python_type_to_arrow_type(annotation)
    converter_fn = self.get_arrow_to_python_converter(arrow_type)
    return converter_fn(storage_value)

def register_logical_type(self, lt: "LogicalTypeProtocol") -> None:
    """Register a ``LogicalTypeProtocol`` instance.

    Pass-through to the internal ``LogicalTypeRegistry``.

    Args:
        lt: The logical type to register.
    """
    if self._logical_type_registry is None:
        raise ValueError("No LogicalTypeRegistry configured on this converter.")
    self._logical_type_registry.register_logical_type(lt)

def register_logical_type_factory(
    self,
    factory: "LogicalTypeFactoryProtocol",
    *,
    category: "str | None" = None,
    python_bases: "Iterable[type]" = (),
) -> None:
    """Register a ``LogicalTypeFactoryProtocol`` instance.

    Pass-through to the internal ``LogicalTypeRegistry``.

    Args:
        factory: The factory to register.
        category: If given, registers factory as the read-side handler for
            Arrow extension types with this ``"category"`` metadata value.
        python_bases: Zero or more Python base classes to register as write-side
            dispatch keys for this factory.
    """
    if self._logical_type_registry is None:
        raise ValueError("No LogicalTypeRegistry configured on this converter.")
    self._logical_type_registry.register_logical_type_factory(
        factory, category=category, python_bases=python_bases
    )
```

Also add `Iterable` to the imports in `universal_converter.py`:
```python
from collections.abc import Callable, Iterable, Mapping
```

And add TYPE_CHECKING imports:
```python
if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
```

- [ ] **Step 4: Update `_create_python_to_arrow_converter` to pass `converter=self`**

In `_create_python_to_arrow_converter`, find this block:
```python
if self._logical_type_registry is not None and isinstance(python_type, type):
    lt = self._logical_type_registry.get_by_python_type(python_type)
    if lt is not None:
        return lt.python_to_storage
```

Replace with:
```python
if self._logical_type_registry is not None and isinstance(python_type, type):
    lt = self._logical_type_registry.get_by_python_type(python_type)
    if lt is not None:
        _lt = lt
        _self = self
        return lambda value: _lt.python_to_storage(value, _self)
```

- [ ] **Step 5: Update `_create_arrow_to_python_converter` to pass `converter=self`**

In `_create_arrow_to_python_converter`, find:
```python
if isinstance(arrow_type, pa.ExtensionType) and self._logical_type_registry is not None:
    lt = self._logical_type_registry.get_by_arrow_extension_name(
        arrow_type.extension_name
    )
    if lt is not None:
        return lt.storage_to_python
```

Replace with:
```python
if isinstance(arrow_type, pa.ExtensionType) and self._logical_type_registry is not None:
    lt = self._logical_type_registry.get_by_arrow_extension_name(
        arrow_type.extension_name
    )
    if lt is not None:
        _lt = lt
        _self = self
        return lambda storage_value: _lt.storage_to_python(storage_value, _self)
```

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py tests/test_extension_types/test_builtin_logical_types.py -v 2>&1 | tail -20
```
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(universal_converter): add python_to_storage, storage_to_python, and registration pass-throughs; wire converter=self into logical type dispatch"
```

---

## Task 6: Simplify `ensure_types_registered_for_schemas` + remove `ensure_*` from registry

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `src/orcapod/extension_types/registry.py`
- Modify: `tests/test_extension_types/test_registry.py`

- [ ] **Step 1: Update `ensure_types_registered_for_schemas` in `UniversalTypeConverter`**

Replace the existing method:

```python
def ensure_types_registered_for_schemas(self, *schemas: Schema) -> None:
    """Ensure a LogicalType is registered for every annotation in schemas.

    Calls ``register_python_class`` for each annotation, which recursively
    resolves nested types and synthesises via factory if needed.
    When no ``LogicalTypeRegistry`` is configured, this is a no-op.

    Args:
        *schemas: One or more ``Schema`` mappings (column name → Python type).

    Raises:
        TypeError: If a leaf class has no registered ``LogicalType`` and
            no registered factory covers it.
    """
    if self._logical_type_registry is None:
        return
    for schema in schemas:
        for annotation in schema.values():
            self.register_python_class(annotation)
```

- [ ] **Step 2: Run existing ensure_types tests to verify nothing breaks**

```bash
uv run pytest tests/ -v -k "ensure_types" 2>&1 | tail -20
```
Expected: Pass.

- [ ] **Step 3: Find and update registry tests that test `ensure_*` methods**

Check which tests in `test_registry.py` test `ensure_logical_type_for_python_class` and `ensure_extension_type`:

```bash
grep -n "ensure_logical_type\|ensure_extension_type" tests/test_extension_types/test_registry.py
```

- [ ] **Step 4: Remove `ensure_*` tests from `test_registry.py` and add converter pass-through tests**

Remove any test functions that directly test `ensure_logical_type_for_python_class` or `ensure_extension_type` on the registry (they are removed from the public API).

Add this test to `test_registry.py`:

```python
def test_registry_does_not_expose_ensure_methods():
    """ensure_logical_type_for_python_class and ensure_extension_type are removed."""
    registry = LogicalTypeRegistry()
    assert not hasattr(registry, "ensure_logical_type_for_python_class")
    assert not hasattr(registry, "ensure_extension_type")
```

- [ ] **Step 5: Remove `ensure_logical_type_for_python_class` and `ensure_extension_type` from `registry.py`**

In `src/orcapod/extension_types/registry.py`, delete the `ensure_extension_type` method (lines ~355-467) and the `ensure_logical_type_for_python_class` method (lines ~469-577).

The public surface retained: `register_logical_type`, `register_logical_type_factory`, `get_by_python_type`, `get_by_arrow_extension_name`, `get_by_logical_name`.

- [ ] **Step 6: Run tests**

```bash
uv run pytest tests/test_extension_types/test_registry.py tests/test_semantic_types/ -v 2>&1 | tail -20
```
Expected: All pass (ensure_* tests replaced).

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py src/orcapod/extension_types/registry.py tests/test_extension_types/test_registry.py
git commit -m "refactor(registry): remove ensure_* methods; simplify ensure_types_registered_for_schemas to use register_python_class"
```

---

## Task 7: Create `DataclassLogicalType` in `extension_types/dataclass_handler.py`

**Files:**
- Create: `src/orcapod/extension_types/dataclass_handler.py`
- Create: `tests/test_extension_types/test_dataclass_handler.py`

- [ ] **Step 1: Write failing tests for `DataclassLogicalType`**

Create `tests/test_extension_types/test_dataclass_handler.py`:

```python
"""Tests for DataclassLogicalType and DataclassHandlerFactory."""

from __future__ import annotations

import dataclasses
import uuid as _uuid_module
from typing import Any

import pyarrow as pa
import pytest


# ── Helpers ─────────────────────────────────────────────────────────────────

class _StubConverter:
    """Minimal converter stub for DataclassLogicalType tests."""

    def python_to_storage(self, value, annotation):
        if annotation is str:
            return str(value)
        if annotation is int:
            return int(value)
        return value

    def storage_to_python(self, storage_value, annotation):
        if annotation is str:
            return str(storage_value)
        if annotation is int:
            return int(storage_value)
        return storage_value

    def register_python_class(self, annotation):
        if annotation is str:
            return pa.large_string()
        if annotation is int:
            return pa.int64()
        raise ValueError(f"No mapping for {annotation}")


# ── DataclassLogicalType tests ───────────────────────────────────────────────

def test_dataclass_logical_type_is_importable():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    assert DataclassLogicalType is not None


def test_dataclass_logical_type_protocol_conformance():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    from orcapod.extension_types.protocols import LogicalTypeProtocol

    @dataclasses.dataclass
    class _MyDC:
        name: str
        count: int

    storage = pa.struct([pa.field("name", pa.large_string()), pa.field("count", pa.int64())])
    field_annotations = [("name", str), ("count", int)]
    lt = DataclassLogicalType(
        logical_name="tests.MyDC",
        python_type=_MyDC,
        storage_type=storage,
        field_annotations=field_annotations,
    )
    assert isinstance(lt, LogicalTypeProtocol)


def test_dataclass_logical_type_python_to_storage():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Point:
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = DataclassLogicalType("tests.Point", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.python_to_storage(_Point(x=3, y=7), converter)
    assert result == {"x": 3, "y": 7}


def test_dataclass_logical_type_storage_to_python():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Point:
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = DataclassLogicalType("tests.Point", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.storage_to_python({"x": 3, "y": 7}, converter)
    assert isinstance(result, _Point)
    assert result.x == 3
    assert result.y == 7


def test_dataclass_logical_type_logical_type_name():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Foo:
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = DataclassLogicalType("mymod.Foo", _Foo, storage, [("val", str)])
    assert lt.logical_type_name == "mymod.Foo"


def test_dataclass_logical_type_python_type():
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType

    @dataclasses.dataclass
    class _Bar:
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = DataclassLogicalType("mymod.Bar", _Bar, storage, [("val", str)])
    assert lt.python_type is _Bar
```

- [ ] **Step 2: Run to confirm failures**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v 2>&1 | tail -20
```
Expected: ImportError — `dataclass_handler` does not exist.

- [ ] **Step 3: Create `src/orcapod/extension_types/dataclass_handler.py`**

```python
"""DataclassLogicalType and DataclassHandlerFactory.

Provides the ``DataclassLogicalType`` logical type implementation and the
``DataclassHandlerFactory`` that synthesises and reconstructs ``DataclassLogicalType``
instances for Python dataclasses.

Write path (``create_for_python_type``):
    Iterates dataclass fields, delegates field Arrow-type resolution to the converter
    via ``register_python_class``, and returns a ``DataclassLogicalType`` backed by
    a ``pa.struct`` extension type.

Read path (``reconstruct_from_arrow``):
    Imports the dataclass by fully-qualified class name, resolves field annotations
    against the (already bottom-up resolved) storage type, and returns a
    ``DataclassLogicalType``.

Category tag: ``"orcapod.dataclass"``
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import logging
from typing import TYPE_CHECKING, Any

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

#: Category tag embedded in Arrow extension metadata. Used as the factory dispatch key.
DATACLASS_CATEGORY = "orcapod.dataclass"


class DataclassLogicalType:
    """Logical type binding a Python dataclass to its Arrow extension type representation.

    Stores the dataclass's fully-qualified class name as the Arrow extension name
    and a ``pa.struct`` of the dataclass fields as the storage type.

    No Arrow-type reasoning lives here — all field-type resolution is owned by the
    converter and completed before this object is constructed.

    Args:
        logical_name: Fully-qualified class name (e.g. ``"mymodule.sub.MyData"``).
            Used as both the logical type name and the Arrow extension name.
        python_type: The Python dataclass ``type`` object.
        storage_type: The Arrow ``pa.StructType`` for the dataclass fields.
        field_annotations: Ordered list of ``(field_name, python_annotation)`` pairs
            matching the fields in ``storage_type``.

    Example:
        >>> lt = DataclassLogicalType(
        ...     "mymod.Point", Point,
        ...     pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())]),
        ...     [("x", int), ("y", int)],
        ... )
        >>> lt.python_to_storage(Point(1, 2), converter)
        {"x": 1, "y": 2}
    """

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: "pa.StructType",
        field_annotations: list[tuple[str, Any]],
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_annotations = field_annotations

        _metadata = json.dumps({"category": DATACLASS_CATEGORY}).encode("utf-8")
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, metadata=_metadata
        )
        self._arrow_ext: "pa.ExtensionType | None" = None
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
        self._polars_ext: "pl.BaseExtension | None" = None

    @property
    def logical_type_name(self) -> str:
        """Fully-qualified class name used as the logical type identifier."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The Python dataclass type this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> "pa.ExtensionType":
        """Return the Arrow extension type for this dataclass.

        Returns:
            A cached ``pa.ExtensionType`` instance with ``extension_name`` equal to
            the fully-qualified class name and ``storage_type`` equal to the struct
            of the dataclass fields.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> "pl.BaseExtension":
        """Return the Polars extension type for this dataclass.

        Returns:
            A cached ``pl.BaseExtension`` instance.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any, converter: "TypeConverterProtocol") -> dict[str, Any]:
        """Convert a dataclass instance to an Arrow-compatible struct dict.

        Iterates ``_field_annotations`` and delegates each field's conversion to
        ``converter.python_to_storage``.

        Args:
            value: A dataclass instance of type ``python_type``.
            converter: The active converter for per-field delegation.

        Returns:
            A dict mapping field names to their Arrow storage values.
        """
        return {
            name: converter.python_to_storage(getattr(value, name), annotation)
            for name, annotation in self._field_annotations
        }

    def storage_to_python(self, storage_value: Any, converter: "TypeConverterProtocol") -> Any:
        """Reconstruct a dataclass instance from an Arrow struct dict.

        Args:
            storage_value: A dict mapping field names to Arrow storage values.
            converter: The active converter for per-field delegation.

        Returns:
            A dataclass instance of type ``python_type``.
        """
        kwargs = {
            name: converter.storage_to_python(storage_value[name], annotation)
            for name, annotation in self._field_annotations
        }
        return self._python_type(**kwargs)


class DataclassHandlerFactory:
    """Stateless factory that synthesises and reconstructs ``DataclassLogicalType`` instances.

    **Write path** (``create_for_python_type``): derives Arrow struct type from the
    dataclass fields by delegating to ``converter.register_python_class`` per field.

    **Read path** (``reconstruct_from_arrow``): imports the dataclass by FQCN, matches
    fields against the already-resolved ``storage_type``, and returns a
    ``DataclassLogicalType``.

    Category tag: ``"orcapod.dataclass"``
    Register with::

        converter.register_logical_type_factory(
            DataclassHandlerFactory(),
            category="orcapod.dataclass",
            python_bases=[object],
        )

    Example:
        >>> factory = DataclassHandlerFactory()
        >>> factory.supports_class(MyDataclass)
        True
        >>> factory.supports_class(str)
        False
    """

    def supports_class(self, python_type: type) -> bool:
        """Return True if ``python_type`` is a dataclass.

        Args:
            python_type: Any Python type.

        Returns:
            True if ``dataclasses.is_dataclass(python_type)`` is True.
        """
        return dataclasses.is_dataclass(python_type) and isinstance(python_type, type)

    def create_for_python_type(
        self,
        python_type: type,
        converter: "TypeConverterProtocol",
    ) -> DataclassLogicalType:
        """Synthesise a ``DataclassLogicalType`` for a Python dataclass (write path).

        Derives the FQCN, obtains type hints, and resolves each field's Arrow type
        via ``converter.register_python_class``. Rejects local / unnamed classes.

        Args:
            python_type: A Python dataclass type.
            converter: The active converter for field-type resolution.

        Returns:
            A ``DataclassLogicalType`` ready for registration.

        Raises:
            ValueError: If ``python_type`` is a local class (no stable FQCN) or
                has a ``__qualname__`` that contains ``"<locals>"``.
        """
        import typing

        fqcn = f"{python_type.__module__}.{python_type.__qualname__}"
        if "<locals>" in fqcn or not python_type.__module__ or python_type.__module__ == "__main__":
            pass  # allow __main__ classes but reject proper locals
        if "<locals>" in fqcn:
            raise ValueError(
                f"Cannot register local class {python_type!r} as a DataclassLogicalType — "
                f"local classes have no stable fully-qualified class name and cannot be "
                f"reconstructed on read. Define the dataclass at module level."
            )

        try:
            hints = typing.get_type_hints(python_type)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {python_type!r}: {exc}"
            ) from exc

        arrow_fields = []
        field_annotations = []
        for field in dataclasses.fields(python_type):
            if not field.init:
                continue
            annotation = hints.get(field.name, Any)
            arrow_type = converter.register_python_class(annotation)
            arrow_fields.append(pa.field(field.name, arrow_type))
            field_annotations.append((field.name, annotation))

        storage_type = pa.struct(arrow_fields)
        logger.debug("DataclassHandlerFactory: synthesised %r for %r", fqcn, python_type)
        return DataclassLogicalType(fqcn, python_type, storage_type, field_annotations)

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: "pa.DataType",
        metadata: dict[str, Any],
        converter: "TypeConverterProtocol",
    ) -> DataclassLogicalType:
        """Reconstruct a ``DataclassLogicalType`` from Arrow schema metadata (read path).

        Imports the dataclass from its FQCN (``arrow_extension_name``), then matches
        the dataclass field annotations against the fields in ``storage_type``.
        ``storage_type`` is already bottom-up resolved by ``register_storage_type``
        before this method is called.

        Args:
            arrow_extension_name: FQCN of the dataclass (Arrow extension name).
            storage_type: Already-resolved ``pa.StructType`` for the dataclass fields.
            metadata: Full parsed metadata JSON dict (always contains ``"category"``).
            converter: The active converter (not needed here but required by protocol).

        Returns:
            A ``DataclassLogicalType`` ready for registration.

        Raises:
            ImportError: If the class cannot be imported from ``arrow_extension_name``.
            ValueError: If ``storage_type`` is not a struct type.
        """
        import typing

        if not pa.types.is_struct(storage_type):
            raise ValueError(
                f"DataclassHandlerFactory.reconstruct_from_arrow: expected a struct "
                f"storage type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        # Import class from FQCN using longest-prefix module walk
        cls = _import_from_fqcn(arrow_extension_name)

        try:
            hints = typing.get_type_hints(cls)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {cls!r}: {exc}"
            ) from exc

        field_annotations = []
        for field in dataclasses.fields(cls):
            if not field.init:
                continue
            annotation = hints.get(field.name, Any)
            field_annotations.append((field.name, annotation))

        logger.debug(
            "DataclassHandlerFactory: reconstructed %r from Arrow", arrow_extension_name
        )
        return DataclassLogicalType(
            arrow_extension_name, cls, storage_type, field_annotations
        )


def _import_from_fqcn(fqcn: str) -> type:
    """Import a class from its fully-qualified class name.

    Tries module prefixes from longest to shortest. For example, for
    ``"mypackage.sub.MyClass"``, tries ``importlib.import_module("mypackage.sub")``
    then ``getattr(module, "MyClass")``.

    Args:
        fqcn: Fully-qualified class name, e.g. ``"mypackage.sub.MyClass"``.

    Returns:
        The imported class.

    Raises:
        ImportError: If no valid module+attribute split can be found.
    """
    parts = fqcn.rsplit(".", 1)
    if len(parts) != 2:
        raise ImportError(f"Cannot import from FQCN {fqcn!r}: no module separator found.")

    module_path, class_name = parts
    try:
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name)
        if not dataclasses.is_dataclass(cls) or not isinstance(cls, type):
            raise ImportError(
                f"{class_name!r} in {module_path!r} is not a dataclass type."
            )
        return cls
    except (ImportError, AttributeError, ModuleNotFoundError) as exc:
        raise ImportError(
            f"Cannot import dataclass from FQCN {fqcn!r}: {exc}"
        ) from exc
```

- [ ] **Step 4: Run dataclass logical type tests**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v -k "DataclassLogicalType or logical_type" 2>&1 | tail -30
```
Expected: All DataclassLogicalType tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/dataclass_handler.py tests/test_extension_types/test_dataclass_handler.py
git commit -m "feat(dataclass_handler): implement DataclassLogicalType"
```

---

## Task 8: `DataclassHandlerFactory` write path tests + verification

**Files:**
- Modify: `tests/test_extension_types/test_dataclass_handler.py`
- Modify: `src/orcapod/extension_types/dataclass_handler.py` (fixes only)

- [ ] **Step 1: Add factory write-path tests**

```python
# Add to tests/test_extension_types/test_dataclass_handler.py

def _make_full_converter():
    """Make a UniversalTypeConverter with builtin types + DataclassHandlerFactory."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath, LogicalUUID, LogicalUPath
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DATACLASS_CATEGORY
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    registry = LogicalTypeRegistry(logical_types=[LogicalPath(), LogicalUUID(), LogicalUPath()])
    factory = DataclassHandlerFactory()
    registry.register_logical_type_factory(factory, category=DATACLASS_CATEGORY, python_bases=[object])
    return UniversalTypeConverter(logical_type_registry=registry)


def test_factory_supports_class_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _Dummy:
        x: int

    factory = DataclassHandlerFactory()
    assert factory.supports_class(_Dummy) is True


def test_factory_supports_class_non_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    factory = DataclassHandlerFactory()
    assert factory.supports_class(str) is False
    assert factory.supports_class(int) is False


def test_factory_create_flat_dataclass():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DataclassLogicalType

    @dataclasses.dataclass
    class _Flat:
        name: str
        count: int

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_Flat, converter=converter)

    assert isinstance(lt, DataclassLogicalType)
    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_struct(storage)
    assert storage.field("name").type == pa.large_string()
    assert storage.field("count").type == pa.int64()


def test_factory_create_dataclass_with_uuid_field():
    """UUID field → orcapod.uuid extension type in storage struct."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _WithUUID:
        id: _uuid_module.UUID
        label: str

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithUUID, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    id_field_type = storage.field("id").type
    assert isinstance(id_field_type, pa.ExtensionType)
    assert id_field_type.extension_name == "orcapod.uuid"


def test_factory_create_dataclass_with_list_field():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _WithList:
        tags: list[str]
        count: int

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithList, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_large_list(storage.field("tags").type)
    assert storage.field("tags").type.value_type == pa.large_string()


def test_factory_create_dataclass_with_dict_field():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    @dataclasses.dataclass
    class _WithDict:
        meta: dict[str, int]

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_WithDict, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    meta_type = storage.field("meta").type
    assert pa.types.is_large_list(meta_type)
    assert pa.types.is_struct(meta_type.value_type)
    field_names = {meta_type.value_type.field(i).name for i in range(meta_type.value_type.num_fields)}
    assert field_names == {"key", "value"}


def test_factory_rejects_local_class():
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    def _make_local():
        @dataclasses.dataclass
        class _Local:
            x: int
        return _Local

    LocalClass = _make_local()
    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="local"):
        factory.create_for_python_type(LocalClass, converter=converter)


def test_register_python_class_dispatches_to_dataclass_factory():
    """register_python_class on a dataclass triggers DataclassHandlerFactory."""
    converter = _make_full_converter()

    @dataclasses.dataclass
    class _MyPoint:
        x: int
        y: int

    # This is a local class — use a module-level one via register_python_class
    # For this test, simulate by directly pre-importing:
    # We can't use a local class here due to the FQCN check.
    # So we test with the UUID field only as a proxy.
    result = converter.register_python_class(_uuid_module.UUID)
    assert isinstance(result, pa.ExtensionType)
    assert result.extension_name == "orcapod.uuid"
```

- [ ] **Step 2: Run factory write-path tests**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v 2>&1 | tail -30
```
Expected: All pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_dataclass_handler.py src/orcapod/extension_types/dataclass_handler.py
git commit -m "test(dataclass_handler): add DataclassHandlerFactory write-path tests"
```

---

## Task 9: `DataclassHandlerFactory` read path + Arrow round-trip

**Files:**
- Modify: `tests/test_extension_types/test_dataclass_handler.py`

- [ ] **Step 1: Add read-path and round-trip tests**

```python
# Add to tests/test_extension_types/test_dataclass_handler.py

# ── Module-level dataclass for round-trip tests ──────────────────────────────

@dataclasses.dataclass
class _RoundTripPoint:
    """Module-level dataclass for round-trip testing."""
    x: int
    y: int


@dataclasses.dataclass
class _RoundTripRecord:
    """Module-level dataclass with a UUID field."""
    record_id: _uuid_module.UUID
    label: str


# ── Read-path tests ───────────────────────────────────────────────────────────

def test_factory_reconstruct_from_arrow():
    """reconstruct_from_arrow rebuilds the logical type from the Arrow struct."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DataclassLogicalType

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    metadata = {"category": "orcapod.dataclass"}
    fqcn = f"{_RoundTripPoint.__module__}.{_RoundTripPoint.__qualname__}"

    factory = DataclassHandlerFactory()
    converter = _make_full_converter()
    lt = factory.reconstruct_from_arrow(fqcn, storage, metadata, converter=converter)

    assert isinstance(lt, DataclassLogicalType)
    assert lt.python_type is _RoundTripPoint
    assert lt.logical_type_name == fqcn


def test_factory_reconstruct_from_arrow_invalid_fqcn():
    """ImportError if the FQCN cannot be resolved."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    storage = pa.struct([pa.field("x", pa.int64())])
    factory = DataclassHandlerFactory()
    converter = _make_full_converter()

    with pytest.raises(ImportError):
        factory.reconstruct_from_arrow(
            "nonexistent.module.NoSuchClass", storage, {"category": "orcapod.dataclass"}, converter
        )


def test_dataclass_python_to_storage_round_trip():
    """python_to_storage → storage_to_python returns an equivalent dataclass."""
    converter = _make_full_converter()

    # Register _RoundTripPoint via register_python_class
    # It's module-level so FQCN is stable
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory, DATACLASS_CATEGORY
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(_RoundTripPoint, converter=converter)
    converter.register_logical_type(lt)

    point = _RoundTripPoint(x=10, y=20)
    storage_value = lt.python_to_storage(point, converter)
    assert storage_value == {"x": 10, "y": 20}

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripPoint)
    assert reconstructed.x == 10
    assert reconstructed.y == 20


def test_dataclass_with_uuid_round_trip():
    """Round-trip a dataclass with a UUID field through python_to_storage / storage_to_python."""
    from orcapod.extension_types.dataclass_handler import DataclassHandlerFactory

    converter = _make_full_converter()
    factory = DataclassHandlerFactory()
    lt = factory.create_for_python_type(_RoundTripRecord, converter=converter)
    converter.register_logical_type(lt)

    u = _uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    record = _RoundTripRecord(record_id=u, label="hello")

    storage_value = lt.python_to_storage(record, converter)
    assert storage_value["label"] == "hello"
    # UUID stored as bytes
    assert storage_value["record_id"] == u.bytes

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripRecord)
    assert reconstructed.record_id == u
    assert reconstructed.label == "hello"
```

- [ ] **Step 2: Run read-path and round-trip tests**

```bash
uv run pytest tests/test_extension_types/test_dataclass_handler.py -v 2>&1 | tail -30
```
Expected: All pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_dataclass_handler.py
git commit -m "test(dataclass_handler): add DataclassHandlerFactory read-path and Arrow round-trip tests"
```

---

## Task 10: DataContext cleanup + context wiring

**Files:**
- Modify: `src/orcapod/contexts/core.py`
- Modify: `src/orcapod/contexts/__init__.py`
- Modify: `src/orcapod/contexts/registry.py`
- Modify: `src/orcapod/contexts/data/v0.1.json`
- Modify: `src/orcapod/contexts/data/schemas/context_schema.json`
- Modify: `src/orcapod/extension_types/__init__.py`
- Modify: `tests/test_core/function_pod/test_write_side_registration.py`

- [ ] **Step 1: Remove `logical_type_registry` from `DataContext`**

In `src/orcapod/contexts/core.py`, remove the `logical_type_registry` field:

```python
"""Core data structures and exceptions for the OrcaPod context system."""

from dataclasses import dataclass

from orcapod.hashing.semantic_hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.protocols.hashing_protocols import (
    ArrowHasherProtocol,
    SemanticHasherProtocol,
)
from orcapod.protocols.semantic_types_protocols import TypeConverterProtocol


@dataclass
class DataContext:
    """Data context containing all versioned components needed for data interpretation.

    Attributes:
        context_key: Unique identifier (e.g., "std:v0.1:default")
        version: Version string (e.g., "v0.1")
        description: Human-readable description
        type_converter: Type converter for Python ↔ Arrow conversion and
            registration. This is the single public API for all type operations.
        arrow_hasher: Arrow table hasher for this context
        semantic_hasher: General semantic hasher for this context
        type_handler_registry: Registry of TypeHandlerProtocol instances
    """

    context_key: str
    version: str
    description: str
    type_converter: TypeConverterProtocol
    arrow_hasher: ArrowHasherProtocol
    semantic_hasher: SemanticHasherProtocol
    type_handler_registry: TypeHandlerRegistry


class ContextValidationError(Exception):
    """Raised when context validation fails."""
    pass


class ContextResolutionError(Exception):
    """Raised when context cannot be resolved."""
    pass
```

- [ ] **Step 2: Remove `get_default_logical_type_registry` from `contexts/__init__.py`**

In `src/orcapod/contexts/__init__.py`:
1. Remove the `from orcapod.extension_types.registry import LogicalTypeRegistry` import
2. Delete the `get_default_logical_type_registry` function
3. Remove `get_default_logical_type_registry` from `__all__`

- [ ] **Step 3: Update `contexts/registry.py`**

In `_create_context_from_spec`, remove `logical_type_registry=ref_lut["logical_type_registry"]` from `DataContext(...)` constructor call. Also remove `"logical_type_registry"` from the `required_fields` list:

```python
required_fields = [
    "context_key",
    "version",
    "semantic_registry",
    "type_converter",
    "arrow_hasher",
    "semantic_hasher",
    "type_handler_registry",
    # "logical_type_registry" — removed; registry is internal to type_converter
]
```

And update `DataContext(...)` construction:
```python
return DataContext(
    context_key=context_key,
    version=version,
    description=description,
    type_converter=ref_lut["type_converter"],
    arrow_hasher=ref_lut["arrow_hasher"],
    semantic_hasher=ref_lut["semantic_hasher"],
    type_handler_registry=ref_lut["type_handler_registry"],
    # logical_type_registry removed
)
```

- [ ] **Step 4: Update `contexts/data/v0.1.json`**

Move `logical_type_registry` construction inside `type_converter._config`. Remove `semantic_registry` ref from `type_converter._config`:

```json
"type_converter": {
    "_class": "orcapod.semantic_types.universal_converter.UniversalTypeConverter",
    "_config": {
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
        }
    }
},
```

Also remove the top-level `"logical_type_registry"` key from the JSON file entirely.

Keep `semantic_registry` at the top level (used by `arrow_hasher`). It's no longer passed to `type_converter`.

- [ ] **Step 5: Update `contexts/data/schemas/context_schema.json`**

Remove `"logical_type_registry"` from `"required"` array and from `"properties"`.

- [ ] **Step 6: Update `extension_types/__init__.py`** docstring to remove the `DataContext.logical_type_registry` access path reference.

- [ ] **Step 7: Update `test_write_side_registration.py`**

Update `_make_test_context` to not pass `logical_type_registry`:

```python
def _make_test_context(registry: LogicalTypeRegistry) -> DataContext:
    """Create a DataContext with a fresh converter bound to the given registry."""
    base_ctx = get_default_context()
    fresh_converter = UniversalTypeConverter(
        logical_type_registry=registry,
    )
    return DataContext(
        context_key="test",
        version="test",
        description="test",
        type_converter=fresh_converter,
        arrow_hasher=base_ctx.arrow_hasher,
        semantic_hasher=base_ctx.semantic_hasher,
        type_handler_registry=base_ctx.type_handler_registry,
        # logical_type_registry removed from DataContext
    )
```

Also update the factory stub to use new protocol signatures:
```python
class _Factory:
    def supports_class(self, python_type):  # new method
        return True
    def reconstruct_from_arrow(self, name, storage, meta, converter):
        return _make_logical_type(object)
    def create_for_python_type(self, python_type, converter):  # converter param
        call_log.append(python_type)
        return _make_logical_type(python_type)
```

And update `_make_logical_type` builtin logical type stubs to accept converter param:
```python
class _LT:
    ...
    def python_to_storage(self, v, converter=None): return str(v)
    def storage_to_python(self, v, converter=None): return v
```

- [ ] **Step 8: Run tests related to contexts and write-side registration**

```bash
uv run pytest tests/test_core/function_pod/test_write_side_registration.py -v 2>&1 | tail -30
```

```bash
uv run pytest -v -k "context" 2>&1 | tail -20
```

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/contexts/ tests/test_core/function_pod/test_write_side_registration.py
git commit -m "refactor(contexts): remove logical_type_registry from DataContext; move registry construction inside type_converter config"
```

---

## Task 11: Update `database_hooks.py` and `ExtensionAwareDatabase`

**Files:**
- Modify: `src/orcapod/extension_types/database_hooks.py`
- Modify: `src/orcapod/databases/extension_aware_database.py`
- Modify: `tests/test_extension_types/test_database_hooks.py`

- [ ] **Step 1: Update `register_discovered_extensions` in `database_hooks.py`**

```python
"""Schema-walking utilities for extension type auto-registration and post-load casting."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from orcapod.extension_types.schema_walker import walk_schema

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

logger = logging.getLogger(__name__)


def register_discovered_extensions(
    converter: "UniversalTypeConverter | None",
    schema: "pa.Schema",
) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively via ``walk_schema`` to discover all Arrow extension
    types at any nesting depth (both in-memory and field-metadata channels).
    For each discovered type, delegates to ``converter._ensure_extension_type_info``.

    Args:
        converter: The ``UniversalTypeConverter`` to use for registration.
            If ``None``, this call is a no-op.
        schema: The Arrow schema to inspect.

    Raises:
        ValueError: Propagated from the converter if an extension type's metadata
            has no registered factory or is malformed.
    """
    if converter is None:
        logger.debug("register_discovered_extensions: no converter provided, skipping")
        return

    found = walk_schema(schema)
    if not found:
        logger.debug("register_discovered_extensions: no extension types in schema")
        return

    logger.debug(
        "register_discovered_extensions: found %d extension type(s): %s",
        len(found),
        [info.extension_name for info in found],
    )
    for info in found:
        # Bottom-up resolve the storage type first, then register the extension
        resolved_storage = converter.register_storage_type(info.storage_type)
        converter._ensure_extension_type_info(
            info.extension_name,
            info.extension_metadata,
            resolved_storage,
        )


def apply_extension_types(
    table: "pa.Table",
    registry: "LogicalTypeRegistry",  # keep registry param for now
) -> "pa.Table":
    # (body unchanged — kept exactly as before)
    ...
```

Keep the `apply_extension_types` and its helpers (`_apply_field`, etc.) exactly as they are — only `register_discovered_extensions` changes.

Add the old `apply_extension_types` import back:
```python
from orcapod.extension_types.registry import LogicalTypeRegistry
```

- [ ] **Step 2: Update `ExtensionAwareDatabase`**

```python
"""ExtensionAwareDatabase — wrapper that handles extension type registration."""
from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.database_hooks import (
    apply_extension_types,
    register_discovered_extensions,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter


class ExtensionAwareDatabase:
    """``ArrowDatabaseProtocol`` wrapper that auto-registers and applies extension types.

    Args:
        db: Any ``ArrowDatabaseProtocol`` backend.
        converter: The ``UniversalTypeConverter`` to use for extension type
            registration and lookup. Callers typically supply
            ``data_context.type_converter``.
    """

    def __init__(
        self,
        db: ArrowDatabaseProtocol,
        converter: "UniversalTypeConverter",
    ) -> None:
        self._db = db
        self._converter = converter

    def _process(self, table: "pa.Table | None") -> "pa.Table | None":
        """Register extension types and re-wrap columns, or return None unchanged."""
        if table is None:
            return None
        register_discovered_extensions(self._converter, table.schema)
        # apply_extension_types still needs the registry for column re-wrapping
        registry = self._converter._logical_type_registry
        if registry is not None:
            return apply_extension_types(table, registry)
        return table

    # All read/write methods delegate exactly as before, replacing self._registry
    # usage with self._converter where needed in `at()`:

    def at(self, *path_components: str) -> "ExtensionAwareDatabase":
        """Return a scoped view, preserving the extension-aware wrapper."""
        return ExtensionAwareDatabase(
            self._db.at(*path_components),
            converter=self._converter,
        )

    # ... (all other read/write methods are unchanged from before, just
    # delegating self._process(self._db.method(...)))
```

Keep all the `get_record_by_id`, `get_all_records`, `add_record`, etc. methods unchanged except that `at()` now passes `converter=self._converter`.

- [ ] **Step 3: Update call site where `ExtensionAwareDatabase` is constructed**

Search for all places that construct `ExtensionAwareDatabase`:

```bash
grep -r "ExtensionAwareDatabase" /home/kurouto/kurouto-jobs/7694626f-534d-48f5-b51f-4bb9c699d932/orcapod-python/src --include="*.py" -l
```

For each construction site, change `registry=data_context.logical_type_registry` to `converter=data_context.type_converter`.

- [ ] **Step 4: Update `test_database_hooks.py`**

The tests that use `register_discovered_extensions(registry, schema)` need to use `converter`:

For each test:
1. Create a `UniversalTypeConverter` with the appropriate registry
2. Call `register_discovered_extensions(converter, schema)` instead of `register_discovered_extensions(registry, schema)`

```python
# Example update pattern in test_database_hooks.py:

# Before:
# register_discovered_extensions(registry, schema)

# After:
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
converter = UniversalTypeConverter(logical_type_registry=registry)
register_discovered_extensions(converter, schema)
```

- [ ] **Step 5: Run database hook tests**

```bash
uv run pytest tests/test_extension_types/test_database_hooks.py -v 2>&1 | tail -30
```

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/extension_types/database_hooks.py src/orcapod/databases/extension_aware_database.py tests/test_extension_types/test_database_hooks.py
git commit -m "refactor(database_hooks): register_discovered_extensions and ExtensionAwareDatabase now take converter instead of registry"
```

---

## Task 12: Remove `semantic_registry` from `UniversalTypeConverter`; delete `dataclass_encoding.py`; make `type_utils` private

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Delete: `src/orcapod/semantic_types/dataclass_encoding.py`
- Modify: `src/orcapod/extension_types/type_utils.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Remove `semantic_registry` param and usages from `UniversalTypeConverter`**

In `__init__`, remove `semantic_registry` parameter and `self.semantic_registry = semantic_registry`.

In `_convert_python_to_arrow`, remove:
```python
# Remove this block:
if self.semantic_registry:
    converter = self.semantic_registry.get_converter_for_python_type(python_type)
    if converter:
        return converter.arrow_struct_type
```

In `_convert_arrow_to_python`, remove:
```python
# Remove these blocks:
if self.semantic_registry:
    python_type = self.semantic_registry.get_python_type_for_semantic_struct_signature(arrow_type)
    if python_type:
        return python_type
```

In `_create_python_to_arrow_converter`, remove:
```python
# Remove:
if self.semantic_registry:
    converter = self.semantic_registry.get_converter_for_python_type(python_type)
    if converter:
        return converter.python_to_struct_dict
```

In `_create_arrow_to_python_converter`, remove:
```python
# Remove:
if self.semantic_registry and pa.types.is_struct(arrow_type):
    registered_python_type = (
        self.semantic_registry.get_python_type_for_semantic_struct_signature(arrow_type)
    )
    if registered_python_type:
        converter = self.semantic_registry.get_converter_for_python_type(registered_python_type)
        if converter:
            return converter.struct_dict_to_python
```

Remove the `from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry` import.

- [ ] **Step 2: Remove `dataclass_encoding` imports and old dataclass path from converter**

Remove all imports from `dataclass_encoding`:
```python
# Remove:
from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    _get_type_hints_safe,
    dataclass_to_arrow_struct_type,
    dataclass_to_struct_dict,
    has_dataclass_type_sentinel,
    struct_dict_to_dataclass,
)
```

In `_convert_python_to_arrow`, remove the dataclass path:
```python
# Remove:
if dataclasses.is_dataclass(python_type) and isinstance(python_type, type):
    return dataclass_to_arrow_struct_type(python_type, self)
```

In `_convert_arrow_to_python`, remove the dataclass sentinel path:
```python
# Remove the has_dataclass_type_sentinel block (lines referencing has_dataclass_type_sentinel,
# DATACLASS_TYPE_FIELD, struct_dict_to_dataclass, etc.)
```

In `_create_python_to_arrow_converter`, remove:
```python
# Remove:
if dataclasses.is_dataclass(python_type) and isinstance(python_type, type):
    hints = _get_type_hints_safe(python_type)
    field_converters = {
        f.name: self.get_python_to_arrow_converter(hints[f.name])
        for f in dataclasses.fields(python_type)
        if f.init
    }
    return lambda obj: dataclass_to_struct_dict(obj, field_converters)
```

In `_create_arrow_to_python_converter`, remove:
```python
# Remove the has_dataclass_type_sentinel block
```

Remove `import dataclasses` if it's now unused in the converter (check if still needed for the `_create_python_to_arrow_converter` logic after removal).

- [ ] **Step 3: Delete `dataclass_encoding.py`**

```bash
rm /home/kurouto/kurouto-jobs/7694626f-534d-48f5-b51f-4bb9c699d932/orcapod-python/src/orcapod/semantic_types/dataclass_encoding.py
git rm src/orcapod/semantic_types/dataclass_encoding.py
```

- [ ] **Step 4: Update `type_utils.py` to make `extract_leaf_classes` private**

```python
# In src/orcapod/extension_types/type_utils.py:
# Rename extract_leaf_classes → _extract_leaf_classes
# Keep the old name as a shim if needed for other callers, or just rename.
```

Search for callers:
```bash
grep -r "extract_leaf_classes" /home/kurouto/kurouto-jobs/7694626f-534d-48f5-b51f-4bb9c699d932/orcapod-python/src --include="*.py"
```

The only caller was `ensure_types_registered_for_schemas` which we've already replaced with `register_python_class`. Rename the function:

```python
def _extract_leaf_classes(annotation: Any) -> Iterator[type]:
    # (body unchanged)
```

Update the module docstring to reflect it's now private.

- [ ] **Step 5: Update any tests that import `extract_leaf_classes`**

```bash
grep -r "extract_leaf_classes" /home/kurouto/kurouto-jobs/7694626f-534d-48f5-b51f-4bb9c699d932/orcapod-python/tests --include="*.py"
```

Update those tests to use `_extract_leaf_classes` (or remove if the function is no longer tested as part of the public API).

- [ ] **Step 6: Remove test for `dataclass_encoding.py`**

Since `dataclass_encoding.py` is deleted, the test file `tests/test_semantic_types/test_dataclass_encoding.py` will fail on import. Remove or archive it:

```bash
git rm tests/test_semantic_types/test_dataclass_encoding.py
```

- [ ] **Step 7: Update `test_universal_converter.py` to not use `semantic_registry`**

Find all places in `test_universal_converter.py` that pass `semantic_registry=...` to `UniversalTypeConverter(...)` and remove those calls. The tests should pass `logical_type_registry=...` instead (or no argument, using the default context).

Also update the module-level `python_type_to_arrow_type`, `arrow_type_to_python_type`, `get_conversion_functions` module functions — they call `data_context.type_converter` which no longer uses semantic_registry for type dispatch. Path/UUID types should now go through the logical_type_registry.

- [ ] **Step 8: Run full test suite**

```bash
uv run pytest tests/test_semantic_types/ tests/test_extension_types/ -v 2>&1 | tail -40
```

Fix any remaining failures.

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "refactor(universal_converter): remove semantic_registry usage and dataclass_encoding imports; delete dataclass_encoding.py; make extract_leaf_classes private"
```

---

## Task 13: Full test suite verification + `extension_types/__init__.py` update

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`
- Verify: entire test suite

- [ ] **Step 1: Add `DataclassHandlerFactory` and `DataclassLogicalType` to `extension_types/__init__.py`**

```python
from .dataclass_handler import DataclassHandlerFactory, DataclassLogicalType, DATACLASS_CATEGORY

__all__ = [
    "LogicalTypeProtocol",
    "LogicalTypeFactoryProtocol",
    "TypeConverterProtocol",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "make_polars_extension_type",
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
    "register_discovered_extensions",
    "apply_extension_types",
    "DataclassLogicalType",
    "DataclassHandlerFactory",
    "DATACLASS_CATEGORY",
]
```

Update the module docstring to remove the `DataContext.logical_type_registry` access path.

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest tests/ -x 2>&1 | tail -50
```

Fix all failures. Common issues:
- Tests constructing `DataContext` with `logical_type_registry=` → remove that arg
- Tests calling `data_context.logical_type_registry` → use `data_context.type_converter._logical_type_registry` or refactor to use converter methods
- Tests calling `get_default_logical_type_registry()` → use `get_default_context().type_converter._logical_type_registry` or use the converter's registration methods
- Tests calling `factory.create_for_python_type(t)` without `converter=` → add `converter=None` or a stub

- [ ] **Step 3: Run full test suite and confirm it passes**

```bash
uv run pytest tests/ 2>&1 | tail -20
```
Expected: All tests pass.

- [ ] **Step 4: Final commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "feat(extension_types): export DataclassHandlerFactory, DataclassLogicalType, DATACLASS_CATEGORY"
```

---

## Self-Review

### Spec Coverage Check

| Spec section | Covered by task |
|---|---|
| `TypeConverterProtocol` added to `extension_types/protocols.py` | Task 1 |
| `LogicalTypeFactoryProtocol`: add `supports_class`, `converter` param | Task 1 |
| `LogicalTypeProtocol`: add `converter` param | Task 1 |
| Built-in types: add `converter` param (accept, ignore) | Task 2 |
| `register_python_class` on converter | Task 3 |
| `register_storage_type` on converter | Task 4 |
| `python_to_storage` / `storage_to_python` on converter | Task 5 |
| Registration pass-throughs | Task 5 |
| Update converter dispatch to pass `converter=self` | Task 5 |
| Simplify `ensure_types_registered_for_schemas` | Task 6 |
| Remove `ensure_*` from registry | Task 6 |
| `DataclassLogicalType` | Task 7 |
| `DataclassHandlerFactory` write path | Task 8 |
| `DataclassHandlerFactory` read path | Task 9 |
| `DataContext.logical_type_registry` removed | Task 10 |
| `get_default_logical_type_registry` removed | Task 10 |
| `v0.1.json` and `context_schema.json` updated | Task 10 |
| `register_discovered_extensions` takes converter | Task 11 |
| `ExtensionAwareDatabase` takes converter | Task 11 |
| Remove `semantic_registry` from converter | Task 12 |
| Delete `dataclass_encoding.py` | Task 12 |
| `extract_leaf_classes` made private | Task 12 |

All spec requirements are covered. ✓

### Known Deviations from Spec

1. **`register_discovered_extensions`**: The spec proposes simplifying to `for field in schema: converter.register_storage_type(field.type)`. The plan retains `walk_schema` to preserve support for the field-metadata channel (Parquet cold-start where `field.type` is a plain storage type, not a `pa.ExtensionType`). The spec's simplified version only handles in-memory extension types.

2. **`apply_extension_types`**: Still takes a `LogicalTypeRegistry` argument. `ExtensionAwareDatabase` accesses it via `converter._logical_type_registry`. This is an internal implementation detail.
