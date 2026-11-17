# Dataclass Arrow Struct Encoding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `@dataclass` serialization to Orcapod as Arrow structs with a `__type` sentinel field and three-tier (import → registry → synthesize) deserialization, wired automatically into the existing `UniversalTypeConverter` pipeline.

**Architecture:** A self-contained `dataclass_encoding.py` module handles all logic (encoding, decoding, registry, cache). `UniversalTypeConverter` gains thin shims in four methods that delegate to it when a dataclass type or `__type`-bearing struct is detected. No changes are needed outside these two source files plus the public `__init__.py`.

**Tech Stack:** Python `dataclasses`, `importlib`, `re`, `typing`, `pyarrow`, `unittest.mock` (tests only)

**Branch:** `eywalker/eng-555-add-dataclass-support-to-orcapod-via-arrow-struct-encoding`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/orcapod/semantic_types/dataclass_encoding.py` | **Create** | All encode/decode logic, registry, three-tier decoder |
| `src/orcapod/semantic_types/universal_converter.py` | **Modify** | 4 shim branches + `__init__` + `clear_cache` |
| `src/orcapod/__init__.py` | **Modify** | Expose `register_dataclass` in public API |
| `tests/test_semantic_types/test_dataclass_encoding.py` | **Create** | All unit + integration tests |

---

## Prerequisite: Verify branch

Before any task, confirm you are on the correct branch:

```bash
git branch --show-current
# Expected: eywalker/eng-555-add-dataclass-support-to-orcapod-via-arrow-struct-encoding
```

---

### Task 1: Module skeleton — constants and registry

**Files:**
- Create: `src/orcapod/semantic_types/dataclass_encoding.py`
- Create: `tests/test_semantic_types/test_dataclass_encoding.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_semantic_types/test_dataclass_encoding.py
from __future__ import annotations

import dataclasses
import typing

import pytest

from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    DATACLASS_TYPE_PREFIX,
    _DATACLASS_REGISTRY,
    register_dataclass,
)


@dataclasses.dataclass
class _Simple:
    a: int
    b: str


def test_constants():
    assert DATACLASS_TYPE_FIELD == "__type"
    assert DATACLASS_TYPE_PREFIX == "dataclass:"


def test_register_explicit():
    register_dataclass(_Simple)
    key = f"{_Simple.__module__}.{_Simple.__qualname__}"
    assert _DATACLASS_REGISTRY[key] is _Simple


def test_register_returns_class():
    result = register_dataclass(_Simple)
    assert result is _Simple


def test_register_as_decorator():
    @register_dataclass
    @dataclasses.dataclass
    class _Decorated:
        x: float

    key = f"{_Decorated.__module__}.{_Decorated.__qualname__}"
    assert _DATACLASS_REGISTRY[key] is _Decorated
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -v
```

Expected: `ModuleNotFoundError: No module named 'orcapod.semantic_types.dataclass_encoding'`

- [ ] **Step 3: Create `dataclass_encoding.py` with constants and registry**

```python
# src/orcapod/semantic_types/dataclass_encoding.py
"""
Dataclass <-> Arrow struct encoding for Orcapod.

Encodes Python dataclasses as Arrow structs with a ``__type`` sentinel field
carrying the fully-qualified class name. Decoding uses a three-tier fallback:
import -> registry -> synthesize.
"""

from __future__ import annotations

import dataclasses
import importlib
import logging
import re
import typing
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)

DATACLASS_TYPE_FIELD = "__type"
DATACLASS_TYPE_PREFIX = "dataclass:"

# Validates fully-qualified class names like "my_module.sub.MyClass"
_FQCN_RE = re.compile(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+$")

# Process-global registry for tier-2 reconstruction.
# Populated via register_dataclass(); persists for the process lifetime.
_DATACLASS_REGISTRY: dict[str, type] = {}


def register_dataclass(cls: type) -> type:
    """Register a dataclass for tier-2 reconstruction by fully-qualified name.

    Can be used as a class decorator or called directly. Returns ``cls``
    unchanged so it works transparently as a decorator.

    Args:
        cls: A Python dataclass type to register.

    Returns:
        The same ``cls`` that was passed in.
    """
    key = f"{cls.__module__}.{cls.__qualname__}"
    _DATACLASS_REGISTRY[key] = cls
    return cls
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py::test_constants tests/test_semantic_types/test_dataclass_encoding.py::test_register_explicit tests/test_semantic_types/test_dataclass_encoding.py::test_register_returns_class tests/test_semantic_types/test_dataclass_encoding.py::test_register_as_decorator -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/dataclass_encoding.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): add module skeleton with constants and registry (ENG-555)"
```

---

### Task 2: `has_dataclass_type_sentinel` helper

**Files:**
- Modify: `src/orcapod/semantic_types/dataclass_encoding.py`
- Modify: `tests/test_semantic_types/test_dataclass_encoding.py`

- [ ] **Step 1: Add tests**

Append to `tests/test_semantic_types/test_dataclass_encoding.py`:

```python
import pyarrow as pa
from orcapod.semantic_types.dataclass_encoding import has_dataclass_type_sentinel


def test_sentinel_large_string():
    t = pa.struct([pa.field("__type", pa.large_string()), pa.field("a", pa.int64())])
    assert has_dataclass_type_sentinel(t) is True


def test_sentinel_string_compat():
    # older Arrow versions wrote pa.string() instead of pa.large_string()
    t = pa.struct([pa.field("__type", pa.string()), pa.field("a", pa.int64())])
    assert has_dataclass_type_sentinel(t) is True


def test_sentinel_missing_field():
    t = pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.large_string())])
    assert has_dataclass_type_sentinel(t) is False


def test_sentinel_non_struct():
    assert has_dataclass_type_sentinel(pa.int64()) is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "sentinel" -v
```

Expected: `ImportError: cannot import name 'has_dataclass_type_sentinel'`

- [ ] **Step 3: Implement `has_dataclass_type_sentinel`**

Append to `src/orcapod/semantic_types/dataclass_encoding.py` (after `register_dataclass`):

```python
def has_dataclass_type_sentinel(arrow_type: pa.DataType) -> bool:
    """Return ``True`` if ``arrow_type`` is a struct with a ``__type`` string field.

    Accepts both ``pa.large_string()`` and ``pa.string()`` for compatibility
    with data written by older Arrow versions.

    Args:
        arrow_type: Any PyArrow data type.

    Returns:
        True if ``arrow_type`` is a struct containing a ``__type: (large_)string``
        field.
    """
    if not pa.types.is_struct(arrow_type):
        return False
    for i in range(arrow_type.num_fields):
        field = arrow_type.field(i)
        if field.name == DATACLASS_TYPE_FIELD:
            return pa.types.is_large_string(field.type) or pa.types.is_string(field.type)
    return False
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "sentinel" -v
```

Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/dataclass_encoding.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): add has_dataclass_type_sentinel helper (ENG-555)"
```

---

### Task 3: `dataclass_to_arrow_struct_type` encoder

**Files:**
- Modify: `src/orcapod/semantic_types/dataclass_encoding.py`
- Modify: `tests/test_semantic_types/test_dataclass_encoding.py`

The `converter` argument is a `UniversalTypeConverter` instance. These tests use a plain
(not-yet-dataclass-wired) converter — it handles basic field types `int`, `str`, `float`.
Nested dataclass round-trips are tested in Task 6 once UTC is fully wired.

- [ ] **Step 1: Add tests**

Append to `tests/test_semantic_types/test_dataclass_encoding.py`:

```python
from orcapod.semantic_types.universal_converter import UniversalTypeConverter
from orcapod.semantic_types.dataclass_encoding import dataclass_to_arrow_struct_type


def test_struct_type_basic_fields():
    @dataclasses.dataclass
    class _Point:
        x: int
        y: float

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_Point, converter)

    assert pa.types.is_struct(result)
    # __type must be the first field
    assert result[0].name == "__type"
    assert result[0].type == pa.large_string()
    assert result.field("x").type == pa.int64()
    assert result.field("y").type == pa.float64()


def test_struct_type_string_field():
    @dataclasses.dataclass
    class _Named:
        name: str

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_Named, converter)
    assert result.field("name").type == pa.large_string()


def test_struct_type_non_dataclass_raises():
    converter = UniversalTypeConverter()
    with pytest.raises(TypeError, match="not a dataclass"):
        dataclass_to_arrow_struct_type(int, converter)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "struct_type" -v
```

Expected: `ImportError: cannot import name 'dataclass_to_arrow_struct_type'`

- [ ] **Step 3: Implement `dataclass_to_arrow_struct_type`**

Append to `src/orcapod/semantic_types/dataclass_encoding.py`:

```python
def dataclass_to_arrow_struct_type(
    cls: type,
    converter: Any,
) -> pa.StructType:
    """Derive the Arrow struct type for a dataclass class.

    The resulting struct has ``__type: large_string`` as its first field,
    followed by one field per dataclass field. Field types are resolved via
    ``converter`` (a ``UniversalTypeConverter``), so nested dataclasses
    produce nested structs automatically once the converter has the dataclass
    branch wired in.

    Args:
        cls: A Python dataclass type.
        converter: A ``UniversalTypeConverter`` instance used for field type
            resolution.

    Returns:
        A ``pa.StructType`` with ``__type`` as the first field.

    Raises:
        TypeError: If ``cls`` is not a dataclass type.
    """
    if not dataclasses.is_dataclass(cls) or not isinstance(cls, type):
        raise TypeError(f"{cls!r} is not a dataclass type")

    hints = typing.get_type_hints(cls)
    fields: list[pa.Field] = [pa.field(DATACLASS_TYPE_FIELD, pa.large_string())]
    for f in dataclasses.fields(cls):
        arrow_type = converter.python_type_to_arrow_type(hints[f.name])
        fields.append(pa.field(f.name, arrow_type))
    return pa.struct(fields)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "struct_type" -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/dataclass_encoding.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): add dataclass_to_arrow_struct_type (ENG-555)"
```

---

### Task 4: `dataclass_to_struct_dict` encoder

**Files:**
- Modify: `src/orcapod/semantic_types/dataclass_encoding.py`
- Modify: `tests/test_semantic_types/test_dataclass_encoding.py`

- [ ] **Step 1: Add tests**

Append to `tests/test_semantic_types/test_dataclass_encoding.py`:

```python
from orcapod.semantic_types.dataclass_encoding import dataclass_to_struct_dict


def _build_field_converters(cls: type, converter: UniversalTypeConverter) -> dict:
    """Helper: build per-field Arrow-value converters for a dataclass."""
    hints = typing.get_type_hints(cls)
    return {
        f.name: converter.get_python_to_arrow_converter(hints[f.name])
        for f in dataclasses.fields(cls)
    }


def test_struct_dict_simple():
    @dataclasses.dataclass
    class _Box:
        width: int
        label: str

    converter = UniversalTypeConverter()
    field_converters = _build_field_converters(_Box, converter)
    obj = _Box(width=10, label="big")
    result = dataclass_to_struct_dict(obj, field_converters)

    fqcn = f"{_Box.__module__}.{_Box.__qualname__}"
    assert result[DATACLASS_TYPE_FIELD] == f"dataclass:{fqcn}"
    assert result["width"] == 10
    assert result["label"] == "big"


def test_struct_dict_type_error_on_class():
    # Passing a class (not instance) should raise
    with pytest.raises(TypeError, match="not a dataclass instance"):
        dataclass_to_struct_dict(_Simple, {})


def test_struct_dict_type_error_on_non_dataclass():
    with pytest.raises(TypeError, match="not a dataclass instance"):
        dataclass_to_struct_dict(42, {})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "struct_dict" -v
```

Expected: `ImportError: cannot import name 'dataclass_to_struct_dict'`

- [ ] **Step 3: Implement `dataclass_to_struct_dict`**

Append to `src/orcapod/semantic_types/dataclass_encoding.py`:

```python
def dataclass_to_struct_dict(
    obj: Any,
    field_converters: dict[str, Any],
) -> dict[str, Any]:
    """Encode a dataclass instance to an Arrow-compatible struct dict.

    Args:
        obj: A dataclass instance to encode.
        field_converters: Pre-built per-field converter callables keyed by
            field name. Build these once per type at converter-creation time
            and reuse per row to avoid repeated type dispatch.

    Returns:
        A dict with ``__type`` as the first key followed by encoded field values.

    Raises:
        TypeError: If ``obj`` is not a dataclass instance (e.g. a class itself
            or a non-dataclass value).
    """
    # dataclasses.is_dataclass() returns True for both classes and instances;
    # isinstance(obj, type) distinguishes: True for classes, False for instances.
    if not dataclasses.is_dataclass(obj) or isinstance(obj, type):
        raise TypeError(f"{obj!r} is not a dataclass instance")

    cls = type(obj)
    type_str = f"{DATACLASS_TYPE_PREFIX}{cls.__module__}.{cls.__qualname__}"
    result: dict[str, Any] = {DATACLASS_TYPE_FIELD: type_str}
    for f in dataclasses.fields(cls):
        value = getattr(obj, f.name)
        converter_fn = field_converters.get(f.name, lambda v: v)
        result[f.name] = converter_fn(value)
    return result
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "struct_dict" -v
```

Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/dataclass_encoding.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): add dataclass_to_struct_dict encoder (ENG-555)"
```

---

### Task 5: Three-tier decoder — `struct_dict_to_dataclass`

**Files:**
- Modify: `src/orcapod/semantic_types/dataclass_encoding.py`
- Modify: `tests/test_semantic_types/test_dataclass_encoding.py`

Tests use `unittest.mock.patch` to control import behaviour without filesystem dependencies.

- [ ] **Step 1: Add tests**

Append to `tests/test_semantic_types/test_dataclass_encoding.py`:

```python
from unittest.mock import MagicMock, patch
from orcapod.semantic_types.dataclass_encoding import struct_dict_to_dataclass


@dataclasses.dataclass
class _TierOne:
    value: int


def test_tier1_import():
    """Tier 1: class is importable via importlib."""
    fqcn = f"{_TierOne.__module__}.{_TierOne.__qualname__}"
    struct_dict = {
        "__type": f"dataclass:{fqcn}",
        "value": 7,
    }
    field_converters = {"value": lambda v: v}
    cache: dict = {}

    # Patch importlib so tier 1 returns _TierOne
    mock_module = MagicMock()
    mock_module.test_dataclass_encoding = MagicMock()
    # The FQCN splits as module=<everything before last dot>, attr=<last part>
    module_path, _, class_attr = fqcn.rpartition(".")
    with patch("importlib.import_module") as mock_import:
        mock_mod = MagicMock()
        setattr(mock_mod, class_attr, _TierOne)
        mock_import.return_value = mock_mod

        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _TierOne)
    assert result.value == 7
    # Cache should be populated
    assert cache[fqcn] is _TierOne


def test_tier1_cache_hit():
    """Tier 1: cache hit skips importlib entirely."""
    fqcn = "some.module.SomeClass"
    cache = {fqcn: _TierOne}
    struct_dict = {"__type": f"dataclass:{fqcn}", "value": 3}
    field_converters = {"value": lambda v: v}

    with patch("importlib.import_module") as mock_import:
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)
        mock_import.assert_not_called()

    assert isinstance(result, _TierOne)
    assert result.value == 3


def test_tier2_registry():
    """Tier 2: importlib fails, class found in registry."""
    @dataclasses.dataclass
    class _RegClass:
        score: float

    fqcn = "fake.module.RegClass"
    _DATACLASS_REGISTRY[fqcn] = _RegClass

    struct_dict = {"__type": f"dataclass:{fqcn}", "score": 9.5}
    field_converters = {"score": lambda v: v}
    cache: dict = {}

    with patch("importlib.import_module", side_effect=ImportError("no module")):
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _RegClass)
    assert result.score == 9.5
    assert cache[fqcn] is _RegClass


def test_tier3_synthesize():
    """Tier 3: neither importable nor registered — synthesize a dataclass."""
    fqcn = "totally.unknown.Ghost"
    struct_dict = {"__type": f"dataclass:{fqcn}", "name": "phantom", "age": 99}
    field_converters = {"name": lambda v: v, "age": lambda v: v}
    cache: dict = {}

    with patch("importlib.import_module", side_effect=ImportError("no module")):
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.name == "phantom"  # type: ignore[attr-defined]
    assert result.age == 99  # type: ignore[attr-defined]
    # Synthesized class cached under fqcn for future rows
    assert fqcn in cache


def test_missing_type_field_tier3():
    """Struct without __type falls through to tier 3 silently."""
    struct_dict = {"value": 42}
    field_converters = {"value": lambda v: v}
    cache: dict = {}

    result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.value == 42  # type: ignore[attr-defined]
    # No cache entry — no valid fqcn to cache under
    assert len(cache) == 0


def test_malformed_type_field_tier3():
    """Invalid __type format (fails regex) falls through to tier 3."""
    struct_dict = {"__type": "not-valid!!!", "x": 1}
    field_converters = {"x": lambda v: v}
    cache: dict = {}

    result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.x == 1  # type: ignore[attr-defined]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "tier" -v
```

Expected: `ImportError: cannot import name 'struct_dict_to_dataclass'`

- [ ] **Step 3: Implement `struct_dict_to_dataclass`**

Append to `src/orcapod/semantic_types/dataclass_encoding.py`:

```python
def struct_dict_to_dataclass(
    struct_dict: dict[str, Any],
    field_converters: dict[str, Any],
    lookup_cache: dict[str, type],
) -> Any:
    """Decode an Arrow struct dict to a Python dataclass instance.

    Uses a three-tier fallback:

    1. **Import** — ``importlib``-import the class from its fully-qualified name.
    2. **Registry** — look up the FQCN in the process-global ``_DATACLASS_REGISTRY``.
    3. **Synthesize** — create a throwaway dataclass with ``dataclasses.make_dataclass``
       matching the struct's field names (all fields typed as ``Any``).

    Tier 3 never raises. A ``lookup_cache`` (keyed by FQCN) amortises repeated
    resolution across rows in the same read operation.

    Args:
        struct_dict: Arrow struct row dict as produced by ``pa.Table.to_pylist()``.
        field_converters: Per-field Arrow->Python converter callables (keyed by
            field name, excluding ``__type``).
        lookup_cache: Mutable dict used as a per-read cache. Pass the same dict
            for all rows in a read operation; clear between operations if needed.

    Returns:
        A dataclass instance (real or synthesized) with field values set.
    """
    type_str = struct_dict.get(DATACLASS_TYPE_FIELD)

    fqcn: str | None = None
    class_name = "SynthesizedDataclass"

    if type_str and isinstance(type_str, str) and type_str.startswith(DATACLASS_TYPE_PREFIX):
        candidate = type_str[len(DATACLASS_TYPE_PREFIX):]
        if _FQCN_RE.match(candidate):
            fqcn = candidate
            class_name = fqcn.rsplit(".", 1)[-1]
        else:
            logger.warning(
                "struct_dict_to_dataclass: invalid __type value %r — falling back to tier 3",
                type_str,
            )

    cls: type | None = None

    if fqcn is not None:
        # Check lookup cache first (amortises tiers 1-3 across rows)
        if fqcn in lookup_cache:
            cls = lookup_cache[fqcn]
        else:
            # Tier 1: import
            module_path, _, class_attr = fqcn.rpartition(".")
            try:
                module = importlib.import_module(module_path)
                cls = getattr(module, class_attr)
                lookup_cache[fqcn] = cls
            except (ImportError, AttributeError) as exc:
                logger.debug(
                    "struct_dict_to_dataclass: tier 1 import failed for %r: %s",
                    fqcn, exc,
                )

            # Tier 2: registry
            if cls is None:
                cls = _DATACLASS_REGISTRY.get(fqcn)
                if cls is not None:
                    lookup_cache[fqcn] = cls

            # Tier 3: synthesize (fqcn valid but unresolvable)
            if cls is None:
                field_names = [k for k in struct_dict if k != DATACLASS_TYPE_FIELD]
                cls = dataclasses.make_dataclass(
                    class_name, [(name, typing.Any) for name in field_names]
                )
                lookup_cache[fqcn] = cls
    else:
        # No valid fqcn — tier 3 with no caching (no stable key)
        field_names = [k for k in struct_dict if k != DATACLASS_TYPE_FIELD]
        cls = dataclasses.make_dataclass(
            class_name, [(name, typing.Any) for name in field_names]
        )

    # Instantiate: apply field converters, skip the __type sentinel
    data_kwargs: dict[str, Any] = {}
    for key, value in struct_dict.items():
        if key == DATACLASS_TYPE_FIELD:
            continue
        converter_fn = field_converters.get(key, lambda v: v)
        data_kwargs[key] = converter_fn(value) if value is not None else None

    return cls(**data_kwargs)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "tier or missing or malformed" -v
```

Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/dataclass_encoding.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): add three-tier struct_dict_to_dataclass decoder (ENG-555)"
```

---

### Task 6: Wire into `UniversalTypeConverter`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `tests/test_semantic_types/test_dataclass_encoding.py`

- [ ] **Step 1: Add tests (simple and nested round-trips through UTC)**

Append to `tests/test_semantic_types/test_dataclass_encoding.py`:

```python
from orcapod.semantic_types.universal_converter import UniversalTypeConverter as _UTC


def test_utc_simple_round_trip():
    """Full encode->decode round-trip through UniversalTypeConverter."""
    @dataclasses.dataclass
    class _Color:
        r: int
        g: int
        b: int

    converter = _UTC()
    arrow_type = converter.python_type_to_arrow_type(_Color)
    assert has_dataclass_type_sentinel(arrow_type)

    obj = _Color(r=255, g=128, b=0)
    encode = converter.get_python_to_arrow_converter(_Color)
    encoded = encode(obj)
    assert encoded["__type"] == f"dataclass:{_Color.__module__}.{_Color.__qualname__}"

    decode = converter.get_arrow_to_python_converter(arrow_type)
    with patch("importlib.import_module") as mock_import:
        mock_mod = MagicMock()
        setattr(mock_mod, "_Color", _Color)
        mock_import.return_value = mock_mod
        result = decode(encoded)

    assert isinstance(result, _Color)
    assert result.r == 255 and result.g == 128 and result.b == 0


def test_utc_nested_round_trip():
    """Nested dataclass encodes and decodes recursively."""
    @dataclasses.dataclass
    class _Inner:
        y: float

    @dataclasses.dataclass
    class _Outer:
        x: int
        inner: _Inner

    converter = _UTC()
    arrow_type = converter.python_type_to_arrow_type(_Outer)

    # Nested struct: inner field should itself be a __type-bearing struct
    inner_arrow = arrow_type.field("inner").type
    assert has_dataclass_type_sentinel(inner_arrow)

    obj = _Outer(x=1, inner=_Inner(y=3.14))
    encode = converter.get_python_to_arrow_converter(_Outer)
    encoded = encode(obj)

    assert encoded["inner"]["__type"] == f"dataclass:{_Inner.__module__}.{_Inner.__qualname__}"
    assert encoded["inner"]["y"] == 3.14

    decode = converter.get_arrow_to_python_converter(arrow_type)

    inner_fqcn = f"{_Inner.__module__}.{_Inner.__qualname__}"
    outer_fqcn = f"{_Outer.__module__}.{_Outer.__qualname__}"
    inner_attr = inner_fqcn.rpartition(".")[2]
    outer_attr = outer_fqcn.rpartition(".")[2]

    with patch("importlib.import_module") as mock_import:
        def fake_import(module_path):
            mod = MagicMock()
            setattr(mod, inner_attr, _Inner)
            setattr(mod, outer_attr, _Outer)
            return mod
        mock_import.side_effect = fake_import
        result = decode(encoded)

    assert isinstance(result, _Outer)
    assert result.x == 1
    assert isinstance(result.inner, _Inner)
    assert result.inner.y == 3.14


def test_utc_clear_cache_clears_dataclass_cache():
    converter = _UTC()

    @dataclasses.dataclass
    class _Temp:
        n: int

    fqcn = f"{_Temp.__module__}.{_Temp.__qualname__}"
    converter._dataclass_lookup_cache[fqcn] = _Temp
    converter.clear_cache()
    assert fqcn not in converter._dataclass_lookup_cache
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "utc" -v
```

Expected: The round-trip tests fail because `UniversalTypeConverter` does not yet handle dataclasses.

- [ ] **Step 3: Modify `universal_converter.py`**

**3a.** Add imports at the top of `src/orcapod/semantic_types/universal_converter.py`, after the existing imports:

```python
import dataclasses

from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    dataclass_to_arrow_struct_type,
    dataclass_to_struct_dict,
    has_dataclass_type_sentinel,
    struct_dict_to_dataclass,
)
```

**3b.** In `UniversalTypeConverter.__init__`, add one line after the existing cache dicts:

```python
self._dataclass_lookup_cache: dict[str, type] = {}
```

**3c.** In `_convert_python_to_arrow`, insert before the `if origin is None:` block:

```python
# Dataclass types → struct with __type sentinel
if dataclasses.is_dataclass(python_type) and isinstance(python_type, type):
    return dataclass_to_arrow_struct_type(python_type, self)
```

**3d.** In `_create_python_to_arrow_converter`, insert before the `origin = get_origin(python_type)` line:

```python
# Dataclass instances → struct dict with __type sentinel
if dataclasses.is_dataclass(python_type) and isinstance(python_type, type):
    hints = typing.get_type_hints(python_type)
    field_converters = {
        f.name: self.get_python_to_arrow_converter(hints[f.name])
        for f in dataclasses.fields(python_type)
    }
    return lambda obj: dataclass_to_struct_dict(obj, field_converters)
```

**3e.** In `_convert_arrow_to_python`, inside the `elif pa.types.is_struct(arrow_type):` block, insert before the check for heterogeneous tuples:

```python
# Dataclass structs: actual type is resolved per-row at decode time via __type value
if has_dataclass_type_sentinel(arrow_type):
    return Any
```

**3f.** In `_create_arrow_to_python_converter`, inside the `elif pa.types.is_struct(arrow_type):` block, insert before the `if python_type is tuple ...` check:

```python
# Dataclass structs: per-row dispatch via __type value
if has_dataclass_type_sentinel(arrow_type):
    field_converters = {
        field.name: self.get_arrow_to_python_converter(field.type)
        for field in arrow_type
        if field.name != DATACLASS_TYPE_FIELD
    }
    cache = self._dataclass_lookup_cache
    return lambda d: struct_dict_to_dataclass(d, field_converters, cache)
```

**3g.** In `clear_cache`, add one line at the end:

```python
self._dataclass_lookup_cache.clear()
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "utc" -v
```

Expected: 3 passed

- [ ] **Step 5: Run the full test suite to check for regressions**

```bash
uv run pytest tests/test_semantic_types/ -v
```

Expected: all existing tests still pass

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): wire dataclass encoding into UniversalTypeConverter (ENG-555)"
```

---

### Task 7: Polymorphic decode and public API

**Files:**
- Modify: `src/orcapod/__init__.py`
- Modify: `tests/test_semantic_types/test_dataclass_encoding.py`

- [ ] **Step 1: Add polymorphic decode test and Parquet integration test**

Append to `tests/test_semantic_types/test_dataclass_encoding.py`:

```python
import tempfile
import os


def test_polymorphic_decode():
    """Two rows with different __type values each decode to their own class."""
    @dataclasses.dataclass
    class _Cat:
        name: str

    @dataclasses.dataclass
    class _Dog:
        name: str

    cat_fqcn = f"{_Cat.__module__}.{_Cat.__qualname__}"
    dog_fqcn = f"{_Dog.__module__}.{_Dog.__qualname__}"

    # Both have the same Arrow schema (name: large_string) plus __type
    arrow_type = pa.struct([
        pa.field("__type", pa.large_string()),
        pa.field("name", pa.large_string()),
    ])
    converter = _UTC()
    decode = converter.get_arrow_to_python_converter(arrow_type)

    cat_attr = cat_fqcn.rpartition(".")[2]
    dog_attr = dog_fqcn.rpartition(".")[2]

    with patch("importlib.import_module") as mock_import:
        def fake_import(module_path):
            mod = MagicMock()
            setattr(mod, cat_attr, _Cat)
            setattr(mod, dog_attr, _Dog)
            return mod
        mock_import.side_effect = fake_import

        row0 = decode({"__type": f"dataclass:{cat_fqcn}", "name": "Whiskers"})
        row1 = decode({"__type": f"dataclass:{dog_fqcn}", "name": "Rex"})

    assert isinstance(row0, _Cat) and row0.name == "Whiskers"
    assert isinstance(row1, _Dog) and row1.name == "Rex"


@pytest.mark.integration
def test_parquet_round_trip():
    """Full round-trip: python_dicts_to_arrow_table -> Parquet -> arrow_table_to_python_dicts."""
    import pyarrow.parquet as pq
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter as _UTC2

    @dataclasses.dataclass
    class _Record:
        score: float
        label: str

    converter = _UTC2()

    python_dicts = [
        {"rec": _Record(score=0.9, label="good")},
        {"rec": _Record(score=0.1, label="bad")},
    ]
    from orcapod.types import Schema
    python_schema = Schema({"rec": _Record})
    table = converter.python_dicts_to_arrow_table(python_dicts, python_schema=python_schema)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "test.parquet")
        pq.write_table(table, path)
        loaded = pq.read_table(path)

    rec_fqcn = f"{_Record.__module__}.{_Record.__qualname__}"
    rec_attr = rec_fqcn.rpartition(".")[2]

    with patch("importlib.import_module") as mock_import:
        mod = MagicMock()
        setattr(mod, rec_attr, _Record)
        mock_import.return_value = mod
        results = converter.arrow_table_to_python_dicts(loaded)

    assert len(results) == 2
    assert isinstance(results[0]["rec"], _Record)
    assert results[0]["rec"].score == 0.9
    assert results[0]["rec"].label == "good"
    assert isinstance(results[1]["rec"], _Record)
    assert results[1]["rec"].score == 0.1
    assert results[1]["rec"].label == "bad"
```

- [ ] **Step 2: Run tests to verify they fail (or mark correctly)**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -k "polymorphic or parquet" -v
```

Expected: `test_polymorphic_decode` passes, `test_parquet_round_trip` fails (schema issue — `Schema` doesn't know `_Record` type yet until we wire it in a moment)

- [ ] **Step 3: Expose `register_dataclass` in `orcapod/__init__.py`**

Edit `src/orcapod/__init__.py` to add the import and export:

```python
from .config import OrcapodConfig
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

__all__ = [
    "OrcapodConfig",
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
]
```

- [ ] **Step 4: Run all new tests**

```bash
uv run pytest tests/test_semantic_types/test_dataclass_encoding.py -v
```

Expected: all tests pass (the Parquet integration test should now pass too)

- [ ] **Step 5: Run full test suite to confirm no regressions**

```bash
uv run pytest tests/ -v
```

Expected: all existing tests pass

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/__init__.py tests/test_semantic_types/test_dataclass_encoding.py
git commit -m "feat(dataclass): expose register_dataclass in public API, add integration tests (ENG-555)"
```

---

## Final Verification

Run the complete test suite one last time:

```bash
uv run pytest tests/ -v
```

All tests should pass. Then push the branch:

```bash
git push -u origin eywalker/eng-555-add-dataclass-support-to-orcapod-via-arrow-struct-encoding
```

---

## Self-Review Checklist (completed inline)

- **Spec coverage:**
  - `__type` sentinel field: Task 3 (`dataclass_to_arrow_struct_type`)
  - `dataclass:` prefix value: Task 4 (`dataclass_to_struct_dict`)
  - Tier 1 import: Task 5 `test_tier1_import`
  - Tier 2 registry + `register_dataclass`: Tasks 1 + 5 `test_tier2_registry`
  - Tier 3 synthesize: Task 5 `test_tier3_synthesize`
  - Lookup cache: Task 5 `test_tier1_cache_hit` + Task 6 `test_utc_clear_cache_clears_dataclass_cache`
  - Nested dataclasses: Task 6 `test_utc_nested_round_trip`
  - Polymorphic decode: Task 7 `test_polymorphic_decode`
  - Parquet round-trip: Task 7 `test_parquet_round_trip`
  - Public API `orcapod.register_dataclass`: Task 7 + `test_register_as_decorator`
  - Malformed `__type` fallback: Task 5 `test_malformed_type_field_tier3`
  - Missing `__type` fallback: Task 5 `test_missing_type_field_tier3`
  - `TypeError` on non-dataclass: Task 4 + Task 3

- **No placeholders:** Verified — all steps contain complete code.

- **Type consistency:** `field_converters: dict[str, Any]` used consistently across Tasks 4-6. `lookup_cache: dict[str, type]` consistent across Tasks 5-6. `converter: Any` (typed as `UniversalTypeConverter` in practice) consistent in Tasks 3-6.
