# PLT-1654: Schema Walker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `src/orcapod/extension_types/schema_walker.py` — a pure discovery utility that walks a `pa.Schema` or `pa.Field` recursively and returns all extension-typed fields as `ExtensionTypeInfo` instances.

**Architecture:** Three-layer design: `ExtensionTypeInfo` frozen dataclass as the return type; `_detect_extension` handles single-field two-channel detection; `_collect` drives recursive container descent with inline deduplication. Two public entry points — `walk_schema` and `walk_field` — each initialise a fresh `seen` set and delegate to `_collect`.

**Tech Stack:** PyArrow ≥ 20.0.0, Python 3.11+, pytest, uv

---

## File Map

| File | Change |
|---|---|
| `src/orcapod/extension_types/schema_walker.py` | **New** — full module |
| `src/orcapod/extension_types/__init__.py` | Additive — append three new exports |
| `tests/test_extension_types/test_schema_walker.py` | **New** — full test suite |

No other files are touched.

---

## Task 1: Core module — `ExtensionTypeInfo`, detection, top-level walk, deduplication

This task produces the full `schema_walker.py`. Container recursion (struct/list/map) is
added in Task 2. After this task, `walk_schema` and `walk_field` work for top-level
fields only; nesting tests are left for Task 2.

**Files:**
- Create: `src/orcapod/extension_types/schema_walker.py`
- Create: `tests/test_extension_types/test_schema_walker.py`

---

- [ ] **Step 1.1: Write the failing tests**

Create `tests/test_extension_types/test_schema_walker.py` with this content:

```python
"""Tests for schema_walker — recursive Arrow extension type discovery."""

from __future__ import annotations

import re
import uuid

import pyarrow as pa
import pytest

from orcapod.extension_types.schema_walker import (
    ExtensionTypeInfo,
    walk_field,
    walk_schema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_name() -> str:
    """Return a unique extension name to avoid cross-test collisions."""
    return f"test.walker.{uuid.uuid4().hex[:8]}"


def _make_reg_field(
    field_name: str,
    ext_name: str,
    storage: pa.DataType | None = None,
    metadata: bytes = b"test.cat",
) -> pa.Field:
    """Create a ``pa.Field`` with an in-memory ``pa.ExtensionType`` (registered channel).

    The extension type is NOT registered in PyArrow's global registry — this
    is intentional. ``pa.types.is_extension(field.type)`` returns ``True``
    for any ``pa.ExtensionType`` instance regardless of global registration.
    """
    _n = ext_name
    _s = storage if storage is not None else pa.large_utf8()
    _m = metadata
    ExtType = type(
        f"_RegExt_{re.sub(r'[^A-Za-z0-9]', '_', ext_name)}",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: _m,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    return pa.field(field_name, ExtType())


def _make_unreg_field(
    field_name: str,
    ext_name: str,
    storage: pa.DataType | None = None,
    metadata: bytes = b"test.cat",
) -> pa.Field:
    """Create a ``pa.Field`` with raw Arrow extension metadata (unregistered channel)."""
    _s = storage if storage is not None else pa.large_utf8()
    return pa.field(
        field_name,
        _s,
        metadata={
            b"ARROW:extension:name": ext_name.encode(),
            b"ARROW:extension:metadata": metadata,
        },
    )


# ---------------------------------------------------------------------------
# Task 1 tests: top-level detection and deduplication
# ---------------------------------------------------------------------------


def test_empty_schema():
    result = walk_schema(pa.schema([]))
    assert result == []


def test_no_extension_types():
    schema = pa.schema([
        pa.field("x", pa.int64()),
        pa.field("y", pa.large_utf8()),
    ])
    assert walk_schema(schema) == []


def test_top_level_registered():
    name = _unique_name()
    schema = pa.schema([_make_reg_field("col", name, metadata=b"my.cat")])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"
    assert result[0].storage_type == pa.large_utf8()


def test_top_level_unregistered():
    name = _unique_name()
    schema = pa.schema([_make_unreg_field("col", name, metadata=b"my.cat")])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"
    assert result[0].storage_type == pa.large_utf8()


def test_empty_metadata_normalised_to_none_registered():
    """b'' from __arrow_ext_serialize__ is normalised to None."""
    name = _unique_name()
    _n, _s = name, pa.large_utf8()
    ExtType = type(
        "_EmptyMetaExt",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: b"",
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    result = walk_field(pa.field("col", ExtType()))
    assert len(result) == 1
    assert result[0].extension_metadata is None


def test_empty_metadata_normalised_to_none_unregistered():
    """b'' ARROW:extension:metadata value is normalised to None."""
    name = _unique_name()
    field = pa.field(
        "col",
        pa.large_utf8(),
        metadata={
            b"ARROW:extension:name": name.encode(),
            b"ARROW:extension:metadata": b"",
        },
    )
    result = walk_field(field)
    assert len(result) == 1
    assert result[0].extension_metadata is None


def test_walk_field_returns_single_field_result():
    name = _unique_name()
    field = _make_reg_field("col", name, metadata=b"cat")
    result = walk_field(field)
    assert len(result) == 1
    assert result[0].extension_name == name


def test_deduplication():
    """Same (extension_name, extension_metadata) in two columns → one result."""
    name = _unique_name()
    meta = b"test.cat"
    _n, _m, _s = name, meta, pa.large_utf8()
    ExtType = type(
        "_DupExt",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: _m,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    schema = pa.schema([
        pa.field("col_a", ExtType()),
        pa.field("col_b", ExtType()),
    ])
    result = walk_schema(schema)
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == meta
```

- [ ] **Step 1.2: Run tests to verify they all fail**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_extension_types/test_schema_walker.py -v 2>&1 | head -30
```

Expected: `ModuleNotFoundError` or `ImportError` — `schema_walker` does not exist yet.

- [ ] **Step 1.3: Implement `schema_walker.py`**

Create `src/orcapod/extension_types/schema_walker.py` with this content:

```python
"""Recursive Arrow schema walker for extension type discovery.

Given a ``pa.Schema`` or a single ``pa.Field``, walks the Arrow type tree
recursively and returns all extension-typed fields found at any depth of
nesting (struct, list, map, etc.).

This is a pure discovery utility — it never triggers any registration.
"""

from __future__ import annotations

import dataclasses

import pyarrow as pa


@dataclasses.dataclass(frozen=True)
class ExtensionTypeInfo:
    """Metadata for a single Arrow extension type found in a schema.

    Attributes:
        extension_name: The extension type's unique name stored as
            ``ARROW:extension:name`` (e.g. ``"pathlib.Path"``).
        extension_metadata: The category tag stored as
            ``ARROW:extension:metadata`` (e.g. ``b"orcapod.dataclass"``).
            ``None`` when absent or serialised as empty bytes.
        storage_type: The underlying Arrow storage type
            (e.g. ``pa.large_string()``).
    """

    extension_name: str
    extension_metadata: bytes | None
    storage_type: pa.DataType


def walk_schema(schema: pa.Schema) -> list[ExtensionTypeInfo]:
    """Walk *schema* and return all extension types found, deduplicated.

    Iterates every top-level field and descends recursively into struct,
    list, and map container types. The result is deduplicated by
    ``(extension_name, extension_metadata)``; the first occurrence of each
    pair is kept.

    Args:
        schema: A PyArrow schema to inspect.

    Returns:
        Deduplicated list of ``ExtensionTypeInfo`` in depth-first,
        first-seen order.
    """
    seen: set[tuple[str, bytes | None]] = set()
    results: list[ExtensionTypeInfo] = []
    for i in range(schema.num_fields):
        _collect(schema.field(i), seen, results)
    return results


def walk_field(field: pa.Field) -> list[ExtensionTypeInfo]:
    """Walk *field*'s type tree and return all extension types found, deduplicated.

    Args:
        field: A PyArrow field to inspect.

    Returns:
        Deduplicated list of ``ExtensionTypeInfo`` in depth-first,
        first-seen order.
    """
    seen: set[tuple[str, bytes | None]] = set()
    results: list[ExtensionTypeInfo] = []
    _collect(field, seen, results)
    return results


def _collect(
    field: pa.Field,
    seen: set[tuple[str, bytes | None]],
    results: list[ExtensionTypeInfo],
) -> None:
    """Recursively walk *field* and accumulate ``ExtensionTypeInfo`` into *results*.

    Mutates *seen* and *results* in place. Stops descending once a field is
    identified as extension-typed — the storage type of an extension type is
    not descended into.

    Args:
        field: The field to inspect.
        seen: Deduplication set of ``(extension_name, extension_metadata)``
            pairs already appended to *results*.
        results: Accumulator list.
    """
    info = _detect_extension(field)
    if info is not None:
        key = (info.extension_name, info.extension_metadata)
        if key not in seen:
            seen.add(key)
            results.append(info)
        return

    t = field.type
    if pa.types.is_struct(t):
        for i in range(t.num_fields):
            _collect(t.field(i), seen, results)
    elif (
        pa.types.is_list(t)
        or pa.types.is_large_list(t)
        or pa.types.is_fixed_size_list(t)
        or pa.types.is_list_view(t)
        or pa.types.is_large_list_view(t)
    ):
        _collect(t.value_field, seen, results)
    elif pa.types.is_map(t):
        key_field = getattr(t, "key_field", None)
        item_field = getattr(t, "item_field", None)
        if key_field is not None:
            _collect(key_field, seen, results)
        if item_field is not None:
            _collect(item_field, seen, results)


def _detect_extension(field: pa.Field) -> ExtensionTypeInfo | None:
    """Extract ``ExtensionTypeInfo`` from *field*, or ``None`` if not extension-typed.

    Checks two channels in order:

    1. **Registered channel** — ``pa.types.is_extension(field.type)`` is
       true. The Python type object carries the name, serialised metadata,
       and storage type.
    2. **Unregistered channel** — ``field.metadata`` contains
       ``b"ARROW:extension:name"``. The type survived a Parquet/IPC
       round-trip without being registered in this process.

    In both cases empty bytes metadata (``b""``) is normalised to ``None``.

    Args:
        field: The field to inspect.

    Returns:
        ``ExtensionTypeInfo`` if the field is extension-typed, else ``None``.
    """
    if pa.types.is_extension(field.type):
        ext_type = field.type
        raw_meta = ext_type.__arrow_ext_serialize__()
        return ExtensionTypeInfo(
            extension_name=ext_type.extension_name,
            extension_metadata=raw_meta or None,
            storage_type=ext_type.storage_type,
        )

    if field.metadata and b"ARROW:extension:name" in field.metadata:
        name = field.metadata[b"ARROW:extension:name"].decode("utf-8")
        raw_meta = field.metadata.get(b"ARROW:extension:metadata")
        return ExtensionTypeInfo(
            extension_name=name,
            extension_metadata=raw_meta or None,
            storage_type=field.type,
        )

    return None
```

- [ ] **Step 1.4: Run Task 1 tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_schema_walker.py -v -k "empty_schema or no_extension or top_level or empty_metadata or walk_field or deduplication"
```

Expected: all 8 tests PASS.

- [ ] **Step 1.5: Commit**

```bash
git add src/orcapod/extension_types/schema_walker.py tests/test_extension_types/test_schema_walker.py
git commit -m "feat(extension_types): add schema_walker with ExtensionTypeInfo and top-level detection (PLT-1654)"
```

---

## Task 2: Container recursion — struct, list, map, nested combinations

This task adds the nesting tests and verifies the container recursion already present in
`_collect` (written in Task 1) handles them correctly.

**Files:**
- Modify: `tests/test_extension_types/test_schema_walker.py` — append new tests

---

- [ ] **Step 2.1: Append the nesting tests**

Append to `tests/test_extension_types/test_schema_walker.py`:

```python
# ---------------------------------------------------------------------------
# Task 2 tests: container recursion
# ---------------------------------------------------------------------------


def test_list_of_registered():
    """Registered extension type as the value field of a list."""
    name = _unique_name()
    value_field = _make_reg_field("item", name, metadata=b"my.cat")
    list_field = pa.field("col", pa.list_(value_field))
    result = walk_schema(pa.schema([list_field]))
    assert len(result) == 1
    assert result[0].extension_name == name


def test_list_of_unregistered():
    """Unregistered extension type as the value field of a list."""
    name = _unique_name()
    value_field = _make_unreg_field("item", name, metadata=b"my.cat")
    list_field = pa.field("col", pa.list_(value_field))
    result = walk_schema(pa.schema([list_field]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"


def test_struct_containing_registered():
    """Registered extension type as a field inside a struct."""
    name = _unique_name()
    struct_field = pa.field(
        "col",
        pa.struct([
            _make_reg_field("a", name, metadata=b"my.cat"),
            pa.field("b", pa.int64()),
        ]),
    )
    result = walk_schema(pa.schema([struct_field]))
    assert len(result) == 1
    assert result[0].extension_name == name


def test_struct_containing_unregistered():
    """Unregistered extension type as a field inside a struct."""
    name = _unique_name()
    struct_field = pa.field(
        "col",
        pa.struct([
            _make_unreg_field("a", name, metadata=b"my.cat"),
            pa.field("b", pa.int64()),
        ]),
    )
    result = walk_schema(pa.schema([struct_field]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"my.cat"


def test_nested_list_struct():
    """Registered extension type nested inside list<struct<...>>."""
    name = _unique_name()
    struct_type = pa.struct([
        _make_reg_field("x", name, metadata=b"deep.cat"),
        pa.field("y", pa.int32()),
    ])
    value_field = pa.field("item", struct_type)
    col = pa.field("col", pa.list_(value_field))
    result = walk_schema(pa.schema([col]))
    assert len(result) == 1
    assert result[0].extension_name == name
    assert result[0].extension_metadata == b"deep.cat"


def test_map_type():
    """Extension type as the item type of a map (registered channel)."""
    name = _unique_name()
    _n, _m, _s = name, b"map.cat", pa.large_utf8()
    # Build a pa.ExtensionType instance — it IS a pa.DataType and can be
    # passed directly to pa.map_() as the item type.
    ExtType = type(
        "_MapItemExt",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _s, _n),
            "__arrow_ext_serialize__": lambda self: _m,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    map_field = pa.field("col", pa.map_(pa.large_utf8(), ExtType()))
    result = walk_schema(pa.schema([map_field]))
    # _collect uses getattr(t, "item_field") to retrieve the item pa.Field.
    # pa.types.is_extension(item_field.type) will be True for the ExtType above.
    assert any(r.extension_name == name for r in result)
```

- [ ] **Step 2.2: Run the nesting tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_schema_walker.py -v -k "list_of or struct_containing or nested or map_type"
```

Expected: all 6 tests PASS. The recursion was already written in `_collect` in Task 1.

If `test_map_type` fails because `key_field` / `item_field` are not available on `MapType`
in this PyArrow version, skip it with `@pytest.mark.skip` and open a follow-up note.

- [ ] **Step 2.3: Run the full test file to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_schema_walker.py -v
```

Expected: all 14 tests PASS.

- [ ] **Step 2.4: Commit**

```bash
git add tests/test_extension_types/test_schema_walker.py
git commit -m "test(extension_types): add nesting and map tests for schema_walker (PLT-1654)"
```

---

## Task 3: Export from `__init__.py`

**Files:**
- Modify: `src/orcapod/extension_types/__init__.py`

---

- [ ] **Step 3.1: Update `__init__.py`**

Open `src/orcapod/extension_types/__init__.py`. It currently reads:

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for converters that map
between Python objects and their Arrow extension type storage representation.

The module-level `default_extension_type_registry` instance is the process default.
Built-in registrations (`Path`, `UPath`, `UUID`) are added by PLT-1656.
`DataContext` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import ExtensionTypeConverter
from .registry import ExtensionTypeRegistry

default_extension_type_registry = ExtensionTypeRegistry()

__all__ = [
    "ExtensionTypeConverter",
    "ExtensionTypeRegistry",
    "default_extension_type_registry",
]
```

Replace the entire file with:

```python
"""Arrow/Polars extension type system for orcapod.

This subpackage provides the registry and protocol for converters that map
between Python objects and their Arrow extension type storage representation.

The module-level `default_extension_type_registry` instance is the process default.
Built-in registrations (`Path`, `UPath`, `UUID`) are added by PLT-1656.
`DataContext` wiring is added by PLT-1660.
"""

from __future__ import annotations

from .protocols import ExtensionTypeConverter
from .registry import ExtensionTypeRegistry
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

default_extension_type_registry = ExtensionTypeRegistry()

__all__ = [
    "ExtensionTypeConverter",
    "ExtensionTypeRegistry",
    "default_extension_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

- [ ] **Step 3.2: Verify the exports are importable**

```bash
uv run python -c "
from orcapod.extension_types import ExtensionTypeInfo, walk_schema, walk_field
import pyarrow as pa
schema = pa.schema([pa.field('x', pa.int64())])
print(walk_schema(schema))  # should print []
print('OK')
"
```

Expected output:
```
[]
OK
```

- [ ] **Step 3.3: Run the full test suite for `test_extension_types/`**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all tests PASS (no regressions in `test_protocols.py` or `test_registry.py`).

- [ ] **Step 3.4: Commit**

```bash
git add src/orcapod/extension_types/__init__.py
git commit -m "feat(extension_types): export ExtensionTypeInfo, walk_schema, walk_field (PLT-1654)"
```

---

## Done

After Task 3:

- `src/orcapod/extension_types/schema_walker.py` is complete with `ExtensionTypeInfo`,
  `walk_schema`, `walk_field`, `_collect`, and `_detect_extension`.
- `ExtensionTypeInfo`, `walk_schema`, `walk_field` are exported from
  `orcapod.extension_types`.
- 14 tests in `tests/test_extension_types/test_schema_walker.py` all pass.
- No existing code was modified; no regressions in other `test_extension_types/` tests.

Create a PR targeting the `extension-type-system` branch (not `dev`).
