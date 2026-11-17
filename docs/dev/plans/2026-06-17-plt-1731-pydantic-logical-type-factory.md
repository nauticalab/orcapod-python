# PLT-1731 Pydantic Logical Type Factory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `PydanticLogicalType` and `PydanticLogicalTypeFactory` for pydantic v2 `BaseModel` subclasses, following the same thin-leaf factory pattern as `DataclassLogicalTypeFactory`.

**Architecture:** New `pydantic_logical_type_factory.py` mirrors `dataclass_logical_type_factory.py` with no cross-dependency between them. The FQCN walk loop shared by both factories is extracted into `type_utils._walk_fqcn`. Write path delegates all field-type resolution to `converter.register_python_class`; read path delegates to `converter.register_python_class` for the registration completeness invariant. Pydantic is an optional dependency — the factory is importable and `supports_class` returns `False` when pydantic is not installed.

**Tech Stack:** Python 3.12, PyArrow, Polars, pydantic v2 (`model_fields`, `BaseModel`), `typing.get_type_hints`

---

## File Map

| File | Action | What changes |
|---|---|---|
| `pyproject.toml` | Modify | Add `pydantic = ["pydantic>=2.0"]` optional extra; add to `all` |
| `src/orcapod/extension_types/type_utils.py` | Modify | Add `_walk_fqcn` shared FQCN walk helper; update module docstring |
| `src/orcapod/extension_types/dataclass_logical_type_factory.py` | Modify | `_import_from_fqcn` delegates to `type_utils._walk_fqcn` |
| `src/orcapod/extension_types/pydantic_logical_type_factory.py` | **Create** | `PYDANTIC_CATEGORY`, `PydanticLogicalType`, `PydanticLogicalTypeFactory`, `_import_pydantic_model_from_fqcn` |
| `src/orcapod/extension_types/__init__.py` | Modify | Export `PYDANTIC_CATEGORY`, `PydanticLogicalType`, `PydanticLogicalTypeFactory` |
| `tests/test_extension_types/test_pydantic_logical_type_factory.py` | **Create** | Full test suite |
| `tests/test_extension_types/test_type_utils.py` | Modify | Add tests for `_walk_fqcn` |

---

## Task 1: Add `pydantic` optional dependency

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Add pydantic to optional extras**

In `pyproject.toml`, find the `[project.optional-dependencies]` section. Add the `pydantic` entry and update `all` to include it:

```toml
[project.optional-dependencies]
redis = ["redis>=6.2.0"]
ray = ["ray[default]==2.48.0", "ipywidgets>=8.1.7"]
postgresql = ["psycopg[binary]>=3.0"]
spiraldb = [
    "pyspiral>=0.11.0",
]
pydantic = ["pydantic>=2.0"]
all = ["orcapod[redis]", "orcapod[ray]", "orcapod[postgresql]", "orcapod[spiraldb]", "orcapod[pydantic]"]
```

- [ ] **Step 2: Install pydantic**

```bash
uv sync --extra pydantic
```

Expected: pydantic installs without errors.

- [ ] **Step 3: Verify pydantic is available**

```bash
uv run python -c "import pydantic; print(pydantic.__version__)"
```

Expected: prints a version string starting with `2.`.

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml
git commit -m "chore(deps): add pydantic>=2.0 as optional dependency"
```

---

## Task 2: Factor `_walk_fqcn` into `type_utils.py`

**Files:**
- Modify: `src/orcapod/extension_types/type_utils.py`
- Modify: `src/orcapod/extension_types/dataclass_logical_type_factory.py`
- Modify: `tests/test_extension_types/test_type_utils.py`

- [ ] **Step 1: Write failing tests for `_walk_fqcn`**

Add to `tests/test_extension_types/test_type_utils.py`:

```python
import dataclasses
import pytest


# ── _walk_fqcn tests ─────────────────────────────────────────────────────────

def test_walk_fqcn_resolves_module_level_class():
    """_walk_fqcn resolves a top-level class from its FQCN."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    import pathlib
    obj = _walk_fqcn("pathlib.Path")
    assert obj is pathlib.Path


def test_walk_fqcn_resolves_nested_attribute():
    """_walk_fqcn walks nested attribute chains (e.g. module.Outer.Inner)."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    import os.path
    # os.path.join is a function reachable via attribute walk
    obj = _walk_fqcn("os.path.join")
    assert obj is os.path.join


def test_walk_fqcn_raises_import_error_on_bad_module():
    """_walk_fqcn raises ImportError when no module prefix can be imported."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    with pytest.raises(ImportError):
        _walk_fqcn("nonexistent.module.NoSuchClass")


def test_walk_fqcn_raises_import_error_on_missing_attr():
    """_walk_fqcn raises ImportError when module exists but attribute does not."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    with pytest.raises(ImportError):
        _walk_fqcn("pathlib.NoSuchClass")


def test_walk_fqcn_raises_import_error_on_single_part():
    """_walk_fqcn raises ImportError when FQCN has no module separator."""
    from orcapod.extension_types.type_utils import _walk_fqcn
    with pytest.raises(ImportError):
        _walk_fqcn("justname")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_type_utils.py -k "walk_fqcn" -v
```

Expected: all 5 tests FAIL with `ImportError: cannot import name '_walk_fqcn'`.

- [ ] **Step 3: Add `_walk_fqcn` to `type_utils.py`**

Replace the full content of `src/orcapod/extension_types/type_utils.py` with:

```python
"""Utility helpers for Python type annotation inspection and FQCN import.

Used by the write-side registration trigger to extract leaf Python classes from
complex generic annotations like ``list[dict[A, list[B]]]``, and by logical type
factories to import classes from fully-qualified class names.
"""

from __future__ import annotations

import importlib
import typing
from typing import Any, Iterator


def _extract_leaf_classes(annotation: Any) -> Iterator[type]:
    """Recursively yield all concrete leaf Python classes from a type annotation.

    Unwraps generic aliases (``list[T]``, ``dict[K, V]``, ``Optional[T]``,
    ``Union[A, B]``, ``A | B``, etc.) using ``typing.get_origin`` and
    ``typing.get_args`` and yields every non-generic leaf found. ``NoneType``
    that appears as a generic argument (from ``Optional`` and
    ``Union[..., None]`` / ``T | None``) is skipped — callers see only the
    concrete types. When ``type(None)`` is passed directly as the annotation,
    it is yielded as-is.

    Non-type, non-generic values (e.g. unresolved string annotations) are
    silently skipped.

    Args:
        annotation: A Python type or generic alias to inspect.

    Yields:
        Concrete Python ``type`` objects found at leaf positions.

    Examples:
        >>> list(_extract_leaf_classes(list[int]))
        [<class 'int'>]
        >>> set(_extract_leaf_classes(dict[str, list[MyClass]]))
        {<class 'str'>, <class 'MyClass'>}
    """
    origin = typing.get_origin(annotation)

    if origin is None:
        # Not a generic alias. Yield only if it is a plain type.
        if isinstance(annotation, type):
            yield annotation
        return

    # Generic alias — recurse into every type argument, skipping NoneType.
    for arg in typing.get_args(annotation):
        if arg is type(None):
            continue
        yield from _extract_leaf_classes(arg)


def _walk_fqcn(fqcn: str) -> Any:
    """Walk a fully-qualified class name and return the resolved object.

    Tries module prefixes from longest to shortest, then walks the remaining
    parts as attribute accesses. For example:

    - ``"mypackage.sub.MyClass"`` → import ``mypackage.sub``, then
      ``getattr(module, "MyClass")``.
    - ``"mypackage.sub.Outer.Inner"`` → import ``mypackage.sub``, then
      ``getattr(module, "Outer")``, then ``getattr(Outer, "Inner")``.

    Does **not** validate the type of the resolved object — callers are
    responsible for checking that the result is the expected kind of object
    (e.g. a dataclass, a ``BaseModel`` subclass).

    Args:
        fqcn: Fully-qualified name, e.g. ``"mypackage.sub.MyClass"``.

    Returns:
        The resolved Python object.

    Raises:
        ImportError: If no valid module+attribute split can be found.
    """
    parts = fqcn.split(".")
    if len(parts) < 2:
        raise ImportError(f"Cannot import from FQCN {fqcn!r}: no module separator found.")

    for i in range(len(parts) - 1, 0, -1):
        module_path = ".".join(parts[:i])
        attr_parts = parts[i:]
        try:
            module = importlib.import_module(module_path)
        except (ImportError, ModuleNotFoundError):
            continue
        obj: Any = module
        try:
            for attr in attr_parts:
                obj = getattr(obj, attr)
        except AttributeError:
            continue
        return obj

    raise ImportError(
        f"Cannot import from FQCN {fqcn!r}: no valid module+attribute path found."
    )
```

- [ ] **Step 4: Run `_walk_fqcn` tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_type_utils.py -k "walk_fqcn" -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Update `_import_from_fqcn` in `dataclass_logical_type_factory.py` to delegate to `_walk_fqcn`**

Replace the `_import_from_fqcn` function at the bottom of
`src/orcapod/extension_types/dataclass_logical_type_factory.py` with:

```python
def _import_from_fqcn(fqcn: str) -> type:
    """Import a dataclass from its fully-qualified class name.

    Delegates the module-prefix walk to ``type_utils._walk_fqcn``, then
    validates the resolved object is a dataclass type.

    Args:
        fqcn: Fully-qualified class name, e.g. ``"mypackage.sub.MyClass"``.

    Returns:
        The imported dataclass type.

    Raises:
        ImportError: If no valid module+attribute split can be found, or if the
            resolved object is not a dataclass type.
    """
    from orcapod.extension_types.type_utils import _walk_fqcn

    obj: Any = _walk_fqcn(fqcn)
    if not dataclasses.is_dataclass(obj) or not isinstance(obj, type):
        raise ImportError(
            f"{fqcn!r} does not resolve to a dataclass type."
        )
    return obj
```

Also remove the `import importlib` line at the top of the file since it is no longer used directly.

- [ ] **Step 6: Run existing dataclass factory tests to verify no regression**

```bash
uv run pytest tests/test_extension_types/test_dataclass_logical_type_factory.py -v
```

Expected: all tests PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/extension_types/type_utils.py \
        src/orcapod/extension_types/dataclass_logical_type_factory.py \
        tests/test_extension_types/test_type_utils.py
git commit -m "refactor(type-utils): extract _walk_fqcn shared FQCN helper; delegate from _import_from_fqcn"
```

---

## Task 3: `PydanticLogicalType`

**Files:**
- Create: `src/orcapod/extension_types/pydantic_logical_type_factory.py`
- Create: `tests/test_extension_types/test_pydantic_logical_type_factory.py`

- [ ] **Step 1: Write failing tests for `PydanticLogicalType`**

Create `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
"""Tests for PydanticLogicalType and PydanticLogicalTypeFactory."""

from __future__ import annotations

import uuid as _uuid_module
from typing import Any

import pyarrow as pa
import pytest
from pydantic import BaseModel, PrivateAttr


# ── Helpers ──────────────────────────────────────────────────────────────────

class _StubConverter:
    """Minimal converter stub for PydanticLogicalType tests."""

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


# ── PydanticLogicalType tests ────────────────────────────────────────────────

def test_pydantic_logical_type_is_importable():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType
    assert PydanticLogicalType is not None


def test_pydantic_logical_type_protocol_conformance():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType
    from orcapod.extension_types.protocols import LogicalTypeProtocol

    class _MyModel(BaseModel):
        name: str
        count: int

    storage = pa.struct([pa.field("name", pa.large_string()), pa.field("count", pa.int64())])
    lt = PydanticLogicalType(
        logical_name="tests._MyModel",
        python_type=_MyModel,
        storage_type=storage,
        field_annotations=[("name", str), ("count", int)],
    )
    assert isinstance(lt, LogicalTypeProtocol)


def test_pydantic_logical_type_python_to_storage():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Point(BaseModel):
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = PydanticLogicalType("tests._Point", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.python_to_storage(_Point(x=3, y=7), converter)
    assert result == {"x": 3, "y": 7}


def test_pydantic_logical_type_storage_to_python():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Point(BaseModel):
        x: int
        y: int

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    lt = PydanticLogicalType("tests._Point2", _Point, storage, [("x", int), ("y", int)])
    converter = _StubConverter()

    result = lt.storage_to_python({"x": 3, "y": 7}, converter)
    assert isinstance(result, _Point)
    assert result.x == 3
    assert result.y == 7


def test_pydantic_logical_type_logical_type_name():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Foo(BaseModel):
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = PydanticLogicalType("mymod.Foo", _Foo, storage, [("val", str)])
    assert lt.logical_type_name == "mymod.Foo"


def test_pydantic_logical_type_python_type():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _Bar(BaseModel):
        val: str

    storage = pa.struct([pa.field("val", pa.large_string())])
    lt = PydanticLogicalType("mymod.Bar", _Bar, storage, [("val", str)])
    assert lt.python_type is _Bar


def test_python_to_storage_raises_when_converter_none():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _DC(BaseModel):
        x: int

    storage = pa.struct([pa.field("x", pa.int64())])
    lt = PydanticLogicalType("mymod._DC", _DC, storage, [("x", int)])
    with pytest.raises(ValueError, match="converter"):
        lt.python_to_storage(_DC(x=1), None)


def test_storage_to_python_raises_when_converter_none():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalType

    class _DC2(BaseModel):
        x: int

    storage = pa.struct([pa.field("x", pa.int64())])
    lt = PydanticLogicalType("mymod._DC2", _DC2, storage, [("x", int)])
    with pytest.raises(ValueError, match="converter"):
        lt.storage_to_python({"x": 1}, None)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v
```

Expected: all tests FAIL with `ModuleNotFoundError: No module named 'orcapod.extension_types.pydantic_logical_type_factory'`.

- [ ] **Step 3: Create `pydantic_logical_type_factory.py` with `PydanticLogicalType`**

Create `src/orcapod/extension_types/pydantic_logical_type_factory.py`:

```python
"""PydanticLogicalType and PydanticLogicalTypeFactory.

Provides the ``PydanticLogicalType`` logical type implementation and the
``PydanticLogicalTypeFactory`` that synthesises and reconstructs
``PydanticLogicalType`` instances for pydantic v2 ``BaseModel`` subclasses.

Write path (``create_for_python_type``):
    Iterates model fields via ``model_fields`` (pydantic v2 API), delegates
    field Arrow-type resolution to the converter via ``register_python_class``,
    and returns a ``PydanticLogicalType`` backed by a ``pa.struct`` extension
    type.

Read path (``reconstruct_from_arrow``):
    Imports the model by fully-qualified class name, resolves field annotations
    against the (already bottom-up resolved) storage type, and returns a
    ``PydanticLogicalType``.

Category tag: ``"orcapod.pydantic"``
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type
from orcapod.extension_types.type_utils import _walk_fqcn
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
PYDANTIC_CATEGORY = "orcapod.pydantic"


class PydanticLogicalType:
    """Logical type binding a pydantic ``BaseModel`` subclass to its Arrow extension type.

    Stores the model's fully-qualified class name as the Arrow extension name
    and a ``pa.struct`` of the model fields as the storage type.

    No Arrow-type reasoning lives here — all field-type resolution is owned by
    the converter and completed before this object is constructed.

    Args:
        logical_name: Fully-qualified class name (e.g. ``"mymodule.sub.MyModel"``).
            Used as both the logical type name and the Arrow extension name.
        python_type: The pydantic ``BaseModel`` subclass.
        storage_type: The Arrow ``pa.StructType`` for the model fields.
        field_annotations: Ordered list of ``(field_name, python_annotation)``
            pairs matching the fields in ``storage_type``.

    Example:
        >>> lt = PydanticLogicalType(
        ...     "mymod.Point", Point,
        ...     pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())]),
        ...     [("x", int), ("y", int)],
        ... )
        >>> lt.python_to_storage(Point(x=1, y=2), converter)
        {"x": 1, "y": 2}
    """

    def __init__(
        self,
        logical_name: str,
        python_type: type,
        storage_type: pa.StructType,
        field_annotations: list[tuple[str, Any]],
    ) -> None:
        self._logical_name = logical_name
        self._python_type = python_type
        self._storage_type = storage_type
        self._field_annotations = field_annotations

        _metadata = json.dumps({"category": PYDANTIC_CATEGORY}).encode("utf-8")
        self._arrow_ext_class = make_arrow_extension_type(
            logical_name, storage_type, metadata=_metadata
        )
        self._arrow_ext: pa.ExtensionType | None = None
        # ``storage_type`` must not contain nested extension types (ET1 in DESIGN_ISSUES.md).
        # On the write path, ``PydanticLogicalTypeFactory.create_for_python_type`` strips any
        # top-level extension type from each field's Arrow type before inserting it into the
        # struct. On the read path, ``reconstruct_from_arrow`` receives a ``storage_type``
        # already guaranteed storage-safe by ``register_storage_type``.
        self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)
        self._polars_ext: pl.BaseExtension | None = None

    @property
    def logical_type_name(self) -> str:
        """Fully-qualified class name used as the logical type identifier."""
        return self._logical_name

    @property
    def python_type(self) -> type:
        """The pydantic ``BaseModel`` subclass this logical type represents."""
        return self._python_type

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for this model.

        Returns:
            A cached ``pa.ExtensionType`` instance with ``extension_name`` equal to
            the fully-qualified class name and ``storage_type`` equal to the struct
            of the model fields.
        """
        if self._arrow_ext is None:
            self._arrow_ext = self._arrow_ext_class()
        return self._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the Polars extension type for this model.

        Returns:
            A cached ``pl.BaseExtension`` instance.
        """
        if self._polars_ext is None:
            self._polars_ext = self._polars_ext_class()
        return self._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None) -> dict[str, Any]:
        """Convert a pydantic model instance to an Arrow-compatible struct dict.

        Iterates ``_field_annotations`` and delegates each field's conversion to
        ``converter.python_to_storage``.

        Args:
            value: A pydantic model instance of type ``python_type``.
            converter: The active converter for per-field delegation. Must not be ``None``.

        Returns:
            A dict mapping field names to their Arrow storage values.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                "PydanticLogicalType.python_to_storage requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        return {
            name: converter.python_to_storage(getattr(value, name), annotation)
            for name, annotation in self._field_annotations
        }

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None) -> Any:
        """Reconstruct a pydantic model instance from an Arrow struct dict.

        Args:
            storage_value: A dict mapping field names to Arrow storage values.
            converter: The active converter for per-field delegation. Must not be ``None``.

        Returns:
            A pydantic model instance of type ``python_type``. Pydantic validation
            runs on construction, ensuring the model is always in a valid state.

        Raises:
            ValueError: If ``converter`` is ``None``.
        """
        if converter is None:
            raise ValueError(
                "PydanticLogicalType.storage_to_python requires a converter — "
                "pass a TypeConverterProtocol instance for field-level conversion."
            )
        kwargs = {
            name: converter.storage_to_python(storage_value[name], annotation)
            for name, annotation in self._field_annotations
        }
        return self._python_type(**kwargs)
```

- [ ] **Step 4: Run `PydanticLogicalType` tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    -k "not factory" -v
```

Expected: all 8 `PydanticLogicalType` tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/pydantic_logical_type_factory.py \
        tests/test_extension_types/test_pydantic_logical_type_factory.py
git commit -m "feat(pydantic-factory): add PydanticLogicalType"
```

---

## Task 4: `PydanticLogicalTypeFactory` — write path

**Files:**
- Modify: `src/orcapod/extension_types/pydantic_logical_type_factory.py`
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py`

- [ ] **Step 1: Add module-level models and write-path tests to the test file**

Append to `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
# ── Module-level models for factory tests ────────────────────────────────────
# Must be at module scope (not inside functions) so FQCN reconstruction works.

class _FlatModel(BaseModel):
    name: str
    count: int


class _ModelWithUUID(BaseModel):
    id: _uuid_module.UUID
    label: str


class _ModelWithList(BaseModel):
    tags: list[str]
    count: int


class _ModelWithDict(BaseModel):
    meta: dict[str, int]


class _InnerModel(BaseModel):
    value: int


class _OuterModel(BaseModel):
    inner: _InnerModel
    label: str


class _ModelWithPrivateAttr(BaseModel):
    name: str
    _cache: str = PrivateAttr(default="")


# ── Factory helper ────────────────────────────────────────────────────────────

def _make_full_converter():
    """Make a UniversalTypeConverter with builtin types + PydanticLogicalTypeFactory."""
    from pydantic import BaseModel as _BaseModel
    from orcapod.extension_types.builtin_logical_types import LogicalPath, LogicalUUID, LogicalUPath
    from orcapod.extension_types.registry import LogicalTypeRegistry
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory, PYDANTIC_CATEGORY
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    registry = LogicalTypeRegistry(logical_types=[LogicalPath(), LogicalUUID(), LogicalUPath()])
    factory = PydanticLogicalTypeFactory()
    registry.register_logical_type_factory(factory, category=PYDANTIC_CATEGORY, python_bases=[_BaseModel])
    return UniversalTypeConverter(logical_type_registry=registry)


# ── PydanticLogicalTypeFactory write-path tests ───────────────────────────────

def test_factory_supports_class_pydantic_model():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    assert factory.supports_class(_FlatModel) is True


def test_factory_supports_class_non_pydantic():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    import dataclasses

    @dataclasses.dataclass
    class _DC:
        x: int

    factory = PydanticLogicalTypeFactory()
    assert factory.supports_class(str) is False
    assert factory.supports_class(int) is False
    assert factory.supports_class(_DC) is False


def test_factory_create_flat_model():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory, PydanticLogicalType

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_FlatModel, converter=converter)

    assert isinstance(lt, PydanticLogicalType)
    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_struct(storage)
    assert storage.field("name").type == pa.large_string()
    assert storage.field("count").type == pa.int64()


def test_factory_create_model_with_uuid_field():
    """UUID field → plain storage type (large_binary) in the struct, not extension type (ET1)."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithUUID, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    id_field_type = storage.field("id").type
    assert id_field_type == pa.large_binary()
    assert not isinstance(id_field_type, pa.ExtensionType)


def test_factory_create_model_with_list_field():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithList, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    assert pa.types.is_large_list(storage.field("tags").type)
    assert storage.field("tags").type.value_type == pa.large_string()


def test_factory_create_model_with_dict_field():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithDict, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    meta_type = storage.field("meta").type
    assert pa.types.is_large_list(meta_type)
    assert pa.types.is_struct(meta_type.value_type)
    field_names = {meta_type.value_type.field(i).name for i in range(meta_type.value_type.num_fields)}
    assert field_names == {"key", "value"}


def test_factory_rejects_local_class():
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    def _make_local():
        class _Local(BaseModel):
            x: int
        return _Local

    LocalModel = _make_local()
    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    with pytest.raises(ValueError, match="local"):
        factory.create_for_python_type(LocalModel, converter=converter)


def test_private_fields_not_stored():
    """Private attributes (PrivateAttr) must not appear in the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.create_for_python_type(_ModelWithPrivateAttr, converter=converter)

    storage = lt.get_arrow_extension_type().storage_type
    field_names = {storage.field(i).name for i in range(storage.num_fields)}
    assert "name" in field_names
    assert "_cache" not in field_names
    assert storage.num_fields == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    -k "factory" -v 2>&1 | head -30
```

Expected: all factory tests FAIL with `ImportError: cannot import name 'PydanticLogicalTypeFactory'`.

- [ ] **Step 3: Add `PydanticLogicalTypeFactory` and `_import_pydantic_model_from_fqcn` to the module**

Append to `src/orcapod/extension_types/pydantic_logical_type_factory.py`:

```python

class PydanticLogicalTypeFactory:
    """Stateless factory that synthesises and reconstructs ``PydanticLogicalType`` instances.

    **Write path** (``create_for_python_type``): derives Arrow struct type from the
    model fields by delegating to ``converter.register_python_class`` per field.
    Only fields in ``model_fields`` are stored — computed fields and private
    attributes are excluded.

    **Read path** (``reconstruct_from_arrow``): imports the model by FQCN, matches
    fields against the already-resolved ``storage_type``, and returns a
    ``PydanticLogicalType``.

    Category tag: ``"orcapod.pydantic"``

    Register with::

        from pydantic import BaseModel
        converter.register_logical_type_factory(
            PydanticLogicalTypeFactory(),
            category="orcapod.pydantic",
            python_bases=[BaseModel],
        )

    Example:
        >>> factory = PydanticLogicalTypeFactory()
        >>> factory.supports_class(MyModel)
        True
        >>> factory.supports_class(str)
        False
    """

    def supports_class(self, python_type: type) -> bool:
        """Return True if ``python_type`` is a pydantic ``BaseModel`` subclass.

        Args:
            python_type: Any Python type.

        Returns:
            True if pydantic is installed and ``python_type`` is a ``BaseModel``
            subclass. False if pydantic is not installed.
        """
        try:
            from pydantic import BaseModel
        except ImportError:
            return False
        return isinstance(python_type, type) and issubclass(python_type, BaseModel)

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> PydanticLogicalType:
        """Synthesise a ``PydanticLogicalType`` for a pydantic model (write path).

        Derives the FQCN, obtains type hints, and resolves each field's Arrow type
        via ``converter.register_python_class``. Only fields present in
        ``model_fields`` are stored — computed fields and private attributes are
        excluded. Rejects local / unnamed classes.

        Args:
            python_type: A pydantic ``BaseModel`` subclass.
            converter: The active converter for field-type resolution.

        Returns:
            A ``PydanticLogicalType`` ready for registration.

        Raises:
            ValueError: If ``python_type`` is a local class (``__qualname__`` contains
                ``"<locals>"``).
        """
        import typing

        fqcn = f"{python_type.__module__}.{python_type.__qualname__}"
        if "<locals>" in fqcn:
            raise ValueError(
                f"Cannot register local class {python_type!r} as a PydanticLogicalType — "
                f"local classes have no stable fully-qualified class name and cannot be "
                f"reconstructed on read. Define the model at module level."
            )

        try:
            hints = typing.get_type_hints(python_type)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {python_type!r}: {exc}"
            ) from exc

        arrow_fields = []
        field_annotations = []
        for field_name in python_type.model_fields:
            annotation = hints.get(field_name, Any)
            arrow_type = converter.register_python_class(annotation)
            # Strip top-level extension type before inserting into the struct (ET1;
            # see DESIGN_ISSUES.md): Arrow cannot represent extension types inside
            # struct field types.
            if isinstance(arrow_type, pa.ExtensionType):
                arrow_type = arrow_type.storage_type
            arrow_fields.append(pa.field(field_name, arrow_type))
            field_annotations.append((field_name, annotation))

        storage_type = pa.struct(arrow_fields)
        logger.debug("PydanticLogicalTypeFactory: synthesised %r for %r", fqcn, python_type)
        return PydanticLogicalType(fqcn, python_type, storage_type, field_annotations)

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> PydanticLogicalType:
        """Reconstruct a ``PydanticLogicalType`` from Arrow schema metadata (read path).

        Imports the model from its FQCN (``arrow_extension_name``), then matches
        the model field annotations against the fields in ``storage_type``.
        ``storage_type`` is already bottom-up resolved by ``register_storage_type``
        before this method is called.

        Args:
            arrow_extension_name: FQCN of the pydantic model (Arrow extension name).
            storage_type: Already-resolved ``pa.StructType`` for the model fields.
            metadata: Full parsed metadata JSON dict (always contains ``"category"``).
            converter: The active converter (used for registration completeness invariant).

        Returns:
            A ``PydanticLogicalType`` ready for registration.

        Raises:
            ImportError: If the class cannot be imported from ``arrow_extension_name``.
            ValueError: If ``storage_type`` is not a struct type.
        """
        import typing

        if not pa.types.is_struct(storage_type):
            raise ValueError(
                f"PydanticLogicalTypeFactory.reconstruct_from_arrow: expected a struct "
                f"storage type for {arrow_extension_name!r}, got {storage_type!r}."
            )

        cls = _import_pydantic_model_from_fqcn(arrow_extension_name)

        try:
            hints = typing.get_type_hints(cls)
        except Exception as exc:
            raise ValueError(
                f"Cannot get type hints for {cls!r}: {exc}"
            ) from exc

        field_annotations = []
        for field_name in cls.model_fields:
            annotation = hints.get(field_name, Any)
            # Register any logical type the field annotation maps to (registration
            # completeness invariant: all nested logical types must be registered when
            # the outer type is registered). The return value is discarded.
            converter.register_python_class(annotation)
            field_annotations.append((field_name, annotation))

        logger.debug(
            "PydanticLogicalTypeFactory: reconstructed %r from Arrow", arrow_extension_name
        )
        return PydanticLogicalType(
            arrow_extension_name, cls, storage_type, field_annotations
        )


def _import_pydantic_model_from_fqcn(fqcn: str) -> type:
    """Import a pydantic ``BaseModel`` subclass from its fully-qualified class name.

    Delegates the module-prefix walk to ``type_utils._walk_fqcn``, then
    validates the resolved object is a ``BaseModel`` subclass.

    Args:
        fqcn: Fully-qualified class name, e.g. ``"mypackage.sub.MyModel"``.

    Returns:
        The imported ``BaseModel`` subclass.

    Raises:
        ImportError: If no valid module+attribute split can be found, or if the
            resolved object is not a ``BaseModel`` subclass.
    """
    from pydantic import BaseModel

    obj: Any = _walk_fqcn(fqcn)
    if not (isinstance(obj, type) and issubclass(obj, BaseModel)):
        raise ImportError(
            f"{fqcn!r} does not resolve to a pydantic BaseModel subclass."
        )
    return obj
```

- [ ] **Step 4: Run write-path tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py \
    -k "factory" -v
```

Expected: all write-path factory tests PASS (reconstruct tests will still fail — that's fine for now).

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/pydantic_logical_type_factory.py \
        tests/test_extension_types/test_pydantic_logical_type_factory.py
git commit -m "feat(pydantic-factory): add PydanticLogicalTypeFactory write path"
```

---

## Task 5: Read path, round-trip tests, and Parquet integration

**Files:**
- Modify: `tests/test_extension_types/test_pydantic_logical_type_factory.py`

The `reconstruct_from_arrow` implementation is already in place from Task 4. This task adds the remaining tests that exercise the read path, value round-trips, and Parquet end-to-end.

- [ ] **Step 1: Add module-level models for read-path and round-trip tests**

Append to `tests/test_extension_types/test_pydantic_logical_type_factory.py` (after the write-path tests):

```python
# ── Module-level models for read-path and round-trip tests ───────────────────

class _RoundTripPoint(BaseModel):
    x: int
    y: int


class _RoundTripRecord(BaseModel):
    record_id: _uuid_module.UUID
    label: str
```

- [ ] **Step 2: Add read-path and round-trip tests**

Append to `tests/test_extension_types/test_pydantic_logical_type_factory.py`:

```python
# ── PydanticLogicalTypeFactory read-path tests ────────────────────────────────

def test_factory_reconstruct_from_arrow():
    """reconstruct_from_arrow rebuilds the logical type from the Arrow struct."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory, PydanticLogicalType

    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.int64())])
    metadata = {"category": "orcapod.pydantic"}
    fqcn = f"{_RoundTripPoint.__module__}.{_RoundTripPoint.__qualname__}"

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()
    lt = factory.reconstruct_from_arrow(fqcn, storage, metadata, converter=converter)

    assert isinstance(lt, PydanticLogicalType)
    assert lt.python_type is _RoundTripPoint
    assert lt.logical_type_name == fqcn


def test_factory_reconstruct_from_arrow_invalid_fqcn():
    """ImportError if the FQCN cannot be resolved."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    storage = pa.struct([pa.field("x", pa.int64())])
    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()

    with pytest.raises(ImportError):
        factory.reconstruct_from_arrow(
            "nonexistent.module.NoSuchModel", storage, {"category": "orcapod.pydantic"}, converter
        )


def test_reconstruct_from_arrow_registers_nested_types():
    """reconstruct_from_arrow for Outer must register Inner as a side effect."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    inner_storage = pa.struct([pa.field("value", pa.int64())])
    outer_storage = pa.struct([
        pa.field("inner", inner_storage),
        pa.field("label", pa.large_string()),
    ])
    outer_fqcn = f"{_OuterModel.__module__}.{_OuterModel.__qualname__}"

    factory = PydanticLogicalTypeFactory()
    converter = _make_full_converter()

    # Inner is NOT pre-registered
    assert converter._logical_type_registry.get_by_python_type(_InnerModel) is None

    factory.reconstruct_from_arrow(outer_fqcn, outer_storage, {"category": "orcapod.pydantic"}, converter)

    # Inner must now be registered as a side effect
    assert converter._logical_type_registry.get_by_python_type(_InnerModel) is not None


# ── Value round-trip tests ────────────────────────────────────────────────────

def test_pydantic_python_to_storage_round_trip():
    """python_to_storage → storage_to_python returns an equivalent model."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    converter = _make_full_converter()
    factory = PydanticLogicalTypeFactory()
    lt = factory.create_for_python_type(_RoundTripPoint, converter=converter)
    converter.register_logical_type(lt)

    point = _RoundTripPoint(x=10, y=20)
    storage_value = lt.python_to_storage(point, converter)
    assert storage_value == {"x": 10, "y": 20}

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripPoint)
    assert reconstructed.x == 10
    assert reconstructed.y == 20


def test_pydantic_with_uuid_round_trip():
    """Round-trip a pydantic model with a UUID field."""
    from orcapod.extension_types.pydantic_logical_type_factory import PydanticLogicalTypeFactory

    converter = _make_full_converter()
    factory = PydanticLogicalTypeFactory()
    lt = factory.create_for_python_type(_RoundTripRecord, converter=converter)
    converter.register_logical_type(lt)

    u = _uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    record = _RoundTripRecord(record_id=u, label="hello")

    storage_value = lt.python_to_storage(record, converter)
    assert storage_value["label"] == "hello"
    assert storage_value["record_id"] == u.bytes

    reconstructed = lt.storage_to_python(storage_value, converter)
    assert isinstance(reconstructed, _RoundTripRecord)
    assert reconstructed.record_id == u
    assert reconstructed.label == "hello"


# ── Parquet integration test ──────────────────────────────────────────────────

def test_nested_pydantic_model_parquet_roundtrip(tmp_path):
    """Fresh-process Parquet round-trip for a two-level nested pydantic model.

    Verifies that register_discovered_extensions triggers the chain:
      register_arrow_extension("Outer") -> reconstruct_from_arrow
        -> register_python_class(Inner) -> registers Inner
    so that storage_to_python can reconstruct the full nested object.
    """
    import pyarrow.parquet as pq
    from orcapod.extension_types.database_hooks import register_discovered_extensions, apply_extension_types

    # ── Write path ───────────────────────────────────────────────────────────
    write_converter = _make_full_converter()

    inner = _InnerModel(value=42)
    outer = _OuterModel(inner=inner, label="hello")

    write_converter.register_python_class(_OuterModel)

    arrow_schema = write_converter.python_schema_to_arrow_schema({"item": _OuterModel})
    rows = [{"item": outer}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "nested_pydantic.parquet"
    pq.write_table(table, parquet_path)

    # ── Read path (fresh converter — neither Inner nor Outer pre-registered) ──
    read_converter = _make_full_converter()
    read_table = pq.read_table(parquet_path)

    register_discovered_extensions(read_converter, read_table.schema)
    read_table = apply_extension_types(read_table, read_converter._logical_type_registry)

    assert read_converter._logical_type_registry.get_by_python_type(_OuterModel) is not None
    assert read_converter._logical_type_registry.get_by_python_type(_InnerModel) is not None

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    reconstructed = rows_out[0]["item"]
    assert isinstance(reconstructed, _OuterModel)
    assert isinstance(reconstructed.inner, _InnerModel)
    assert reconstructed.inner.value == 42
    assert reconstructed.label == "hello"
```

- [ ] **Step 3: Run all tests for the new factory**

```bash
uv run pytest tests/test_extension_types/test_pydantic_logical_type_factory.py -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_pydantic_logical_type_factory.py
git commit -m "test(pydantic-factory): add read-path, round-trip, and Parquet integration tests"
```

---

## Task 6: Export from `__init__.py` and full test suite

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`

- [ ] **Step 1: Add exports to `__init__.py`**

In `src/orcapod/extension_types/__init__.py`, add the pydantic import and update `__all__`:

```python
from .pydantic_logical_type_factory import PYDANTIC_CATEGORY, PydanticLogicalType, PydanticLogicalTypeFactory
```

Add to `__all__`:

```python
    # PLT-1731
    "PYDANTIC_CATEGORY",
    "PydanticLogicalType",
    "PydanticLogicalTypeFactory",
```

The full updated `__init__.py` should be:

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for logical types that map
between Python objects and their Arrow/Polars extension type representation.

Built-in registrations (``LogicalPath``, ``LogicalUPath``, ``LogicalUUID``) are
wired into ``DataContext`` via ``contexts/data/v0.1.json``. Use
``get_default_context().type_converter.register_python_class()`` to register new
types, ``register_logical_type_factory()`` to add factories, and
``apply_extension_types()`` to re-wrap Arrow tables with their registered extension types.

``DataclassLogicalTypeFactory`` provides automatic registration for Python dataclasses:
register it with a ``LogicalTypeRegistry`` and any dataclass used in a ``FunctionPod``
will be auto-registered on pod declaration.

``PydanticLogicalTypeFactory`` provides automatic registration for pydantic v2
``BaseModel`` subclasses: register it with a ``LogicalTypeRegistry`` using
``python_bases=[BaseModel]`` and any model used in a ``FunctionPod`` will be
auto-registered on pod declaration. Requires the ``pydantic`` optional extra.
"""

from __future__ import annotations

from .protocols import LogicalTypeProtocol, LogicalTypeFactoryProtocol
from .registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema
from .database_hooks import apply_extension_types, register_discovered_extensions
from .dataclass_logical_type_factory import DATACLASS_CATEGORY, DataclassLogicalType, DataclassLogicalTypeFactory
from .pydantic_logical_type_factory import PYDANTIC_CATEGORY, PydanticLogicalType, PydanticLogicalTypeFactory

__all__ = [
    "LogicalTypeProtocol",
    "LogicalTypeFactoryProtocol",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "make_polars_extension_type",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
    # PLT-1655
    "register_discovered_extensions",
    "apply_extension_types",
    # PLT-1705
    "DATACLASS_CATEGORY",
    "DataclassLogicalType",
    "DataclassLogicalTypeFactory",
    # PLT-1731
    "PYDANTIC_CATEGORY",
    "PydanticLogicalType",
    "PydanticLogicalTypeFactory",
]
```

- [ ] **Step 2: Verify the exports are importable**

```bash
uv run python -c "
from orcapod.extension_types import (
    PYDANTIC_CATEGORY, PydanticLogicalType, PydanticLogicalTypeFactory
)
print('PYDANTIC_CATEGORY:', PYDANTIC_CATEGORY)
print('PydanticLogicalType:', PydanticLogicalType)
print('PydanticLogicalTypeFactory:', PydanticLogicalTypeFactory)
"
```

Expected output:
```
PYDANTIC_CATEGORY: orcapod.pydantic
PydanticLogicalType: <class 'orcapod.extension_types.pydantic_logical_type_factory.PydanticLogicalType'>
PydanticLogicalTypeFactory: <class 'orcapod.extension_types.pydantic_logical_type_factory.PydanticLogicalTypeFactory'>
```

- [ ] **Step 3: Run the full extension_types test suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all tests PASS with no regressions.

- [ ] **Step 4: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "feat(pydantic-factory): export PydanticLogicalType and PydanticLogicalTypeFactory from extension_types"
```
