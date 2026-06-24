# PLT-1659: Extension Type Round-Trip Integration Tests — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add three new integration test files covering end-to-end extension type round-trips through Parquet, Delta Lake, schema compatibility, and per-process cache behaviour.

**Architecture:** Three focused test files plus one source change and one docs update. Test files: `test_roundtrips.py` (write/read through Parquet and Delta backends), `test_schema_compatibility.py` (Arrow-level identity + Python-type-level compatibility), `test_cache_behavior.py` (registry cache populated and skipped on second read). SQLite backend is excluded from value round-trip tests because `SQLiteConnector` does not preserve `ARROW:extension:*` field metadata; that pattern is already covered by `test_extension_aware_database.py`. Source change: `ConnectorArrowDatabase.add_records()` gets a `ValueError` guard that rejects extension-typed columns (both in-memory `pa.ExtensionType` and metadata-only fields) as an interim safety measure while PLT-1795 is pending.

**Tech Stack:** pytest, pyarrow, pyarrow.parquet, deltalake, polars, orcapod extension type APIs (`create_registry`, `UniversalTypeConverter`, `DataclassLogicalTypeFactory`), `unittest.mock.patch.object`.

---

## File Map

| Action | Path |
|---|---|
| Create | `tests/test_extension_types/test_schema_compatibility.py` |
| Create | `tests/test_extension_types/test_cache_behavior.py` |
| Create | `tests/test_extension_types/test_roundtrips.py` |
| Modify | `src/orcapod/databases/connector_arrow_database.py` — add `ValueError` guard in `add_records()` |
| Modify | `DESIGN_ISSUES.md` — add CA1 entry documenting SQL metadata loss and interim guard |

---

## Task 1: Create and check out the feature branch

**Files:** none (git only)

- [ ] **Step 1: Verify you are on `extension-type-system`**

```bash
git branch --show-current
```

Expected output: `extension-type-system`

- [ ] **Step 2: Create and check out the feature branch**

```bash
git checkout -b eywalker/plt-1659-integration-tests-end-to-end-semantic-type-round-trips
git branch --show-current
```

Expected output: `eywalker/plt-1659-integration-tests-end-to-end-semantic-type-round-trips`

---

## Task 2: `test_schema_compatibility.py`

**Files:**
- Create: `tests/test_extension_types/test_schema_compatibility.py`

This file has no backend dependencies — it only needs a fresh `UniversalTypeConverter` and `check_schema_compatibility`.

- [ ] **Step 1: Write the test file**

Create `tests/test_extension_types/test_schema_compatibility.py` with this exact content:

```python
"""Integration tests for extension-type-backed schema compatibility.

Two complementary angles:

Arrow-level identity
    ``converter.python_schema_to_arrow_schema`` assigns each dataclass a unique
    Arrow extension name derived from its fully-qualified class name.  Two
    dataclasses with identical struct shapes but different class names therefore
    produce *different* extension names — the core identity guarantee of the
    extension type system.

Python-type-level compatibility
    ``check_schema_compatibility`` from ``schema_utils`` uses beartype
    ``is_subhint`` to compare Python type annotations.  Same class → compatible;
    different class with the same struct shape → incompatible.  This is the
    property that prevents silent data corruption when two unrelated dataclasses
    happen to share the same fields.
"""
from __future__ import annotations

import dataclasses

import pyarrow as pa

from orcapod.contexts import create_registry
from orcapod.types import Schema
from orcapod.utils.schema_utils import check_schema_compatibility


# Module-level dataclasses — DataclassLogicalTypeFactory rejects local classes
# because they have no stable fully-qualified class name for reconstruction.

@dataclasses.dataclass
class _PointA:
    x: int
    y: int


@dataclasses.dataclass
class _PointB:
    """Same struct shape as _PointA but a different class name."""
    x: int
    y: int


# ── Arrow-level identity tests ────────────────────────────────────────────────


def test_arrow_schema_distinct_extension_names_for_same_shape():
    """_PointA and _PointB produce different Arrow extension names despite identical shapes.

    This is the core identity guarantee: struct shape alone does not determine
    type identity in the extension type system.
    """
    converter_a = create_registry().get_context().type_converter
    converter_b = create_registry().get_context().type_converter

    type_a = converter_a.register_python_class(_PointA)
    type_b = converter_b.register_python_class(_PointB)

    assert isinstance(type_a, pa.ExtensionType)
    assert isinstance(type_b, pa.ExtensionType)

    fqcn_a = f"{_PointA.__module__}.{_PointA.__qualname__}"
    fqcn_b = f"{_PointB.__module__}.{_PointB.__qualname__}"
    assert type_a.extension_name == fqcn_a
    assert type_b.extension_name == fqcn_b
    assert type_a.extension_name != type_b.extension_name


def test_arrow_schema_same_extension_name_idempotent():
    """Registering _PointA twice returns the same extension name both times."""
    converter = create_registry().get_context().type_converter

    type_first = converter.register_python_class(_PointA)
    type_second = converter.register_python_class(_PointA)

    assert isinstance(type_first, pa.ExtensionType)
    assert isinstance(type_second, pa.ExtensionType)
    assert type_first.extension_name == type_second.extension_name


# ── Python-type-level compatibility tests ─────────────────────────────────────


def test_python_schema_compatibility_passes_same_type():
    """Incoming _PointA is compatible with receiving _PointA."""
    result = check_schema_compatibility(
        {"value": _PointA},
        Schema({"value": _PointA}),
    )
    assert result is True


def test_python_schema_compatibility_rejects_different_type_same_shape():
    """Incoming _PointA is NOT compatible with receiving _PointB.

    Both dataclasses share the same struct shape {x: int, y: int}, but they
    are different Python types.  The old shape-based system would have accepted
    this silently; the extension type system correctly rejects it.
    """
    result = check_schema_compatibility(
        {"value": _PointA},
        Schema({"value": _PointB}),
    )
    assert result is False
```

- [ ] **Step 2: Run the tests and verify they pass**

```bash
uv run pytest tests/test_extension_types/test_schema_compatibility.py -v
```

Expected: all 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_schema_compatibility.py
git commit -m "test(extension-types): add schema compatibility integration tests (PLT-1659)"
```

---

## Task 3: `test_cache_behavior.py`

**Files:**
- Create: `tests/test_extension_types/test_cache_behavior.py`

Uses Parquet as the storage backend (simplest — no database wrapper needed). The second test patches `DataclassLogicalTypeFactory.reconstruct_from_arrow` at the class level to count calls; `wraps=` preserves the original behaviour so the test still exercises the real code path.

- [ ] **Step 1: Write the test file**

Create `tests/test_extension_types/test_cache_behavior.py` with this exact content:

```python
"""Integration tests for per-process extension type cache behaviour.

The ``LogicalTypeRegistry`` stores registered types in an in-memory dict keyed
by Arrow extension name.  ``register_discovered_extensions`` skips the factory
call (``reconstruct_from_arrow``) when the extension name is already present in
the registry — this is the "cache hit" path.

Two tests:

1. ``test_cache_populated_after_first_read`` — verifies the type is absent from
   a fresh converter's registry before reading a Parquet file, and present after.

2. ``test_factory_not_called_on_second_read`` — verifies that ``reconstruct_from_arrow``
   is called exactly once (first read) and zero additional times on the second
   read of the same file.
"""
from __future__ import annotations

import dataclasses
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from orcapod.contexts import create_registry
from orcapod.extension_types.dataclass_logical_type_factory import DataclassLogicalTypeFactory


# Module-level dataclass — local classes cannot be reconstructed from FQCN.

@dataclasses.dataclass
class _CachePoint:
    x: int
    y: int


# ── Helpers ───────────────────────────────────────────────────────────────────


def _fresh_converter():
    """Return a fresh UniversalTypeConverter from a new registry instance.

    Uses ``create_registry()`` instead of ``get_default_context()`` to avoid
    cross-test contamination through the global singleton cache.
    """
    return create_registry().get_context().type_converter


def _write_parquet(tmp_path, converter) -> str:
    """Write a _CachePoint column to Parquet and return the file path as str."""
    converter.register_python_class(_CachePoint)
    arrow_schema = converter.python_schema_to_arrow_schema({"point": _CachePoint})
    rows = [{"point": _CachePoint(x=1, y=2)}]
    table = converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)
    parquet_path = tmp_path / "cache_test.parquet"
    pq.write_table(table, str(parquet_path))
    return str(parquet_path)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_cache_populated_after_first_read(tmp_path):
    """Registry has _CachePoint after load_extension_types on a fresh converter.

    Before reading: the fresh converter's registry does not know about _CachePoint.
    After reading: register_discovered_extensions triggers reconstruct_from_arrow
    which registers _CachePoint, populating the cache.
    """
    write_converter = _fresh_converter()
    parquet_path = _write_parquet(tmp_path, write_converter)

    read_converter = _fresh_converter()
    fqcn = f"{_CachePoint.__module__}.{_CachePoint.__qualname__}"

    # Before read: not registered
    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn) is None

    read_converter.load_extension_types(pq.read_table(parquet_path))

    # After read: registered (cache populated)
    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn) is not None


def test_factory_not_called_on_second_read(tmp_path):
    """reconstruct_from_arrow called once on first read, zero times on second read.

    On first read, register_discovered_extensions finds _CachePoint's extension
    name in the schema, dispatches to the factory (call count = 1), and stores
    the result in the registry.

    On second read, register_discovered_extensions finds the extension name already
    in the registry and short-circuits — the factory is not called again
    (call count remains 1).
    """
    write_converter = _fresh_converter()
    parquet_path = _write_parquet(tmp_path, write_converter)

    read_converter = _fresh_converter()

    with patch.object(
        DataclassLogicalTypeFactory,
        "reconstruct_from_arrow",
        autospec=True,
        wraps=DataclassLogicalTypeFactory.reconstruct_from_arrow,
    ) as spy:
        # First read: factory is called once
        read_converter.load_extension_types(pq.read_table(parquet_path))
        assert spy.call_count == 1, f"Expected 1 factory call, got {spy.call_count}"

        # Second read on the same file: registry hit — factory not called again
        read_converter.load_extension_types(pq.read_table(parquet_path))
        assert spy.call_count == 1, (
            f"Expected still 1 factory call after second read, got {spy.call_count}"
        )
```

- [ ] **Step 2: Run the tests and verify they pass**

```bash
uv run pytest tests/test_extension_types/test_cache_behavior.py -v
```

Expected: both tests pass.

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_cache_behavior.py
git commit -m "test(extension-types): add per-process cache behaviour integration tests (PLT-1659)"
```

---

## Task 4: `test_roundtrips.py` — backend fixture + all parametrised tests

**Files:**
- Create: `tests/test_extension_types/test_roundtrips.py`

**Important note on SQLite:** `SQLiteConnector` maps Arrow types to SQL column types and does not preserve `ARROW:extension:*` field metadata. `ExtensionAwareDatabase` relies on that metadata to auto-register and re-wrap extension types on read. Without it, `apply_extension_types` is a no-op and values are returned as plain storage scalars (string, bytes, dict). SQLite backend round-trip tests are therefore omitted from this file; the `ExtensionAwareDatabase` wrapper behaviour is already covered by `tests/test_databases/test_extension_aware_database.py`.

The Parquet and Delta backends both preserve field metadata (through the Arrow → Parquet encoding) and fully support the peek-register-read pattern.

- [ ] **Step 1: Write the test file**

Create `tests/test_extension_types/test_roundtrips.py` with this exact content:

```python
"""End-to-end integration tests for extension type round-trips.

Tests the complete pipeline:

    Python object → write → storage → peek-schema → register → read → Python object

Each round-trip test is parameterised over two storage backends:

- ``parquet``: direct ``pyarrow.parquet`` write/read.
- ``delta``: ``deltalake.write_deltalake`` / ``DeltaTable.to_pyarrow_dataset(as_large_types=True).to_table()``.

SQLite (``ConnectorArrowDatabase`` + ``SQLiteConnector``) is excluded because
``SQLiteConnector`` maps Arrow types to SQL column types and discards
``ARROW:extension:*`` field metadata.  Without that metadata, the
peek-register-read pattern cannot auto-register extension types on the read
path.  The ``ExtensionAwareDatabase`` wrapper behaviour over SQLite is already
tested in ``tests/test_databases/test_extension_aware_database.py``.
"""
from __future__ import annotations

import dataclasses
import pathlib
import uuid as uuid_module
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from upath import UPath

from orcapod.contexts import create_registry
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


# ── Module-level dataclasses ──────────────────────────────────────────────────
# DataclassLogicalTypeFactory rejects local (in-function) classes because they
# have no stable fully-qualified class name for reconstruction from Arrow schema.

@dataclasses.dataclass
class _PointA:
    x: int
    y: int


@dataclasses.dataclass
class _PointB:
    """Same struct shape as _PointA, different class name."""
    x: int
    y: int


@dataclasses.dataclass
class _Inner:
    value: int


@dataclasses.dataclass
class _Outer:
    inner: _Inner
    label: str


# ── Storage backend abstraction ───────────────────────────────────────────────


@dataclasses.dataclass
class _StorageBackend:
    """Encapsulates backend-specific write and read logic for parameterised tests.

    Args:
        name: Short identifier used in pytest test IDs (e.g. ``"parquet"``).
        write: Callable that writes an Arrow table to a directory.
        read: Callable that reads from that directory and returns an Arrow table
            with extension types registered and applied.  Must return only the
            original user data columns (no ``__record_id`` or similar).
    """
    name: str
    write: Callable[[pa.Table, Path], None]
    read: Callable[[Path, UniversalTypeConverter], pa.Table]


def _parquet_write(table: pa.Table, base_path: Path) -> None:
    pq.write_table(table, str(base_path / "data.parquet"))


def _parquet_read(base_path: Path, converter: UniversalTypeConverter) -> pa.Table:
    return converter.load_extension_types(pq.read_table(str(base_path / "data.parquet")))


def _delta_write(table: pa.Table, base_path: Path) -> None:
    import deltalake
    deltalake.write_deltalake(str(base_path / "delta"), table)


def _delta_read(base_path: Path, converter: UniversalTypeConverter) -> pa.Table:
    import deltalake
    dt = deltalake.DeltaTable(str(base_path / "delta"))
    # as_large_types=True preserves large_string / large_binary rather than
    # normalising them to string / binary (Delta Lake's default behaviour).
    raw = dt.to_pyarrow_dataset(as_large_types=True).to_table()
    return converter.load_extension_types(raw)


_BACKENDS = [
    _StorageBackend(name="parquet", write=_parquet_write, read=_parquet_read),
    _StorageBackend(name="delta", write=_delta_write, read=_delta_read),
]


@pytest.fixture(params=_BACKENDS, ids=lambda b: b.name)
def storage_backend(request: pytest.FixtureRequest) -> _StorageBackend:
    """Yield one storage backend per parametrised run."""
    return request.param


# ── Internal helpers ──────────────────────────────────────────────────────────


def _fresh_converter() -> UniversalTypeConverter:
    """Return a fresh converter from a new registry instance.

    Uses ``create_registry()`` instead of ``get_default_context()`` to avoid
    cross-test contamination through the global singleton cache.
    """
    return create_registry().get_context().type_converter


def _write_and_read(
    schema_dict: dict,
    rows: list[dict],
    backend: _StorageBackend,
    tmp_path: Path,
) -> tuple[pa.Table, UniversalTypeConverter]:
    """Write rows with a fresh write converter and read back with a fresh read converter.

    Returns the resulting Arrow table (with extension types applied) and the
    read-side converter (needed for ``arrow_table_to_python_dicts``).
    """
    write_converter = _fresh_converter()
    arrow_schema = write_converter.python_schema_to_arrow_schema(schema_dict)
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)
    backend.write(table, tmp_path)

    read_converter = _fresh_converter()
    result = backend.read(tmp_path, read_converter)
    return result, read_converter


# ── Built-in type round-trip tests ───────────────────────────────────────────


def test_builtin_path_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """pathlib.Path round-trips through storage with extension name ``orcapod.path``.

    Built-in types (Path, UPath, UUID) are pre-registered in the default context
    so the read-side converter already knows about them.  The test verifies that:

    1. The Arrow field carries the ``orcapod.path`` extension type after read.
    2. The Python value is reconstructed as a ``pathlib.Path`` instance.
    """
    p = pathlib.Path("/tmp/orcapod/integration/test.txt")
    result, read_converter = _write_and_read(
        {"col": pathlib.Path},
        [{"col": p}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "orcapod.path"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], pathlib.Path)
    assert rows[0]["col"] == p


def test_builtin_upath_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """UPath round-trips through storage with extension name ``orcapod.upath``."""
    u = UPath("s3://my-bucket/data/file.parquet")
    result, read_converter = _write_and_read(
        {"col": UPath},
        [{"col": u}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "orcapod.upath"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], UPath)
    assert str(rows[0]["col"]) == str(u)


def test_builtin_uuid_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """uuid.UUID round-trips through storage with extension name ``orcapod.uuid``."""
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result, read_converter = _write_and_read(
        {"col": uuid_module.UUID},
        [{"col": u}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("col")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'col', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "orcapod.uuid"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    assert isinstance(rows[0]["col"], uuid_module.UUID)
    assert rows[0]["col"] == u


# ── Dataclass round-trip tests ────────────────────────────────────────────────


def test_simple_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """Simple dataclass round-trips with correct FQCN as the Arrow extension name.

    The read-side converter starts with no knowledge of _PointA.  After read,
    register_discovered_extensions triggers DataclassLogicalTypeFactory which
    imports _PointA from its fully-qualified class name and registers it.
    """
    point = _PointA(x=3, y=7)
    result, read_converter = _write_and_read(
        {"point": _PointA},
        [{"point": point}],
        storage_backend,
        tmp_path,
    )

    fqcn = f"{_PointA.__module__}.{_PointA.__qualname__}"
    field = result.schema.field("point")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'point', got {field.type!r}"
    )
    assert field.type.extension_name == fqcn

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["point"]
    assert isinstance(reconstructed, _PointA)
    assert reconstructed.x == 3
    assert reconstructed.y == 7


def test_two_dataclasses_same_shape_distinct_extension_names(
    storage_backend: _StorageBackend, tmp_path: Path
) -> None:
    """_PointA and _PointB have the same struct shape but different extension names.

    Writing _PointA and reading it back must NOT reconstruct a _PointB, even
    though their on-disk struct shapes (x: int, y: int) are identical.  The
    extension name (FQCN) is the sole identity signal.
    """
    point_a = _PointA(x=1, y=2)
    result, read_converter = _write_and_read(
        {"point": _PointA},
        [{"point": point_a}],
        storage_backend,
        tmp_path,
    )

    fqcn_a = f"{_PointA.__module__}.{_PointA.__qualname__}"
    fqcn_b = f"{_PointB.__module__}.{_PointB.__qualname__}"

    field = result.schema.field("point")
    assert hasattr(field.type, "extension_name")
    assert field.type.extension_name == fqcn_a
    assert field.type.extension_name != fqcn_b  # distinct from _PointB

    rows = read_converter.arrow_table_to_python_dicts(result)
    reconstructed = rows[0]["point"]
    assert isinstance(reconstructed, _PointA)
    assert not isinstance(reconstructed, _PointB)


def test_nested_dataclass_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """Nested dataclass: _Outer and _Inner both registered; full object reconstructed.

    register_discovered_extensions triggers DataclassLogicalTypeFactory for _Outer.
    That factory's reconstruct_from_arrow calls converter.register_python_class(_Inner)
    as a side-effect, so _Inner is also registered without an explicit peek step.
    """
    outer = _Outer(inner=_Inner(value=42), label="hello")
    result, read_converter = _write_and_read(
        {"item": _Outer},
        [{"item": outer}],
        storage_backend,
        tmp_path,
    )

    fqcn_outer = f"{_Outer.__module__}.{_Outer.__qualname__}"
    fqcn_inner = f"{_Inner.__module__}.{_Inner.__qualname__}"

    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn_outer) is not None, (
        "_Outer should be registered after read"
    )
    assert read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn_inner) is not None, (
        "_Inner should be registered transitively after read"
    )

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    reconstructed = rows[0]["item"]
    assert isinstance(reconstructed, _Outer)
    assert isinstance(reconstructed.inner, _Inner)
    assert reconstructed.inner.value == 42
    assert reconstructed.label == "hello"
```

- [ ] **Step 2: Run the tests and verify they pass**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py -v
```

Expected: all 12 parametrised tests pass (6 test functions × 2 backends).

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_roundtrips.py
git commit -m "test(extension-types): add Parquet/Delta round-trip integration tests (PLT-1659)"
```

---

## Task 5: Add the Delta Polars native-read test to `test_roundtrips.py`

**Files:**
- Modify: `tests/test_extension_types/test_roundtrips.py` (append one function)

This test reads a Delta table back via `pl.read_delta` (Polars' native Delta reader) rather than `DeltaTable.to_pyarrow_table()`, verifying that extension type metadata survives the Polars path.

When the write-side converter calls `register_python_class(_PointA)`, it registers `_PointA` in both PyArrow's and Polars' **global** registries (as a side-effect of `registry.register_logical_type`).  That global registration persists for the duration of the test process, so `pl.read_delta` can resolve `_PointA`'s extension type when reading the underlying Parquet files.

- [ ] **Step 1: Append the Delta Polars test to `test_roundtrips.py`**

Append the following block at the end of `tests/test_extension_types/test_roundtrips.py`:

```python
# ── Delta Lake: Polars native read ───────────────────────────────────────────


def test_delta_polars_read_delta(tmp_path: Path) -> None:
    """Write a dataclass column to Delta; read back via pl.read_delta; extension type survives.

    The write-side converter registers _PointA in both PyArrow's and Polars'
    global registries (``register_python_class`` calls ``make_polars_extension_type``
    which registers with Polars).  ``pl.read_delta`` can therefore decode the column
    as the correct Polars extension type, not a plain ``Struct``.

    Note: ``pl.DataFrame.to_arrow()`` exports Polars extension types as PyArrow
    extension arrays but with empty serialized bytes (Polars does not forward
    ``__arrow_ext_metadata__`` through its Arrow export).  Python-object
    reconstruction via the Polars-to-Arrow path is therefore not possible; that
    path is tested by the separate ``parquet`` / ``delta`` parametrised tests
    which read underlying Parquet files directly.
    """
    import deltalake
    import polars as pl

    delta_path = str(tmp_path / "polars_delta")
    fqcn = f"{_PointA.__module__}.{_PointA.__qualname__}"

    # Write — registers _PointA in PyArrow + Polars global registries.
    write_converter = _fresh_converter()
    write_converter.register_python_class(_PointA)
    arrow_schema = write_converter.python_schema_to_arrow_schema({"point": _PointA})
    rows = [{"point": _PointA(x=5, y=9)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)
    deltalake.write_deltalake(delta_path, table)

    # Read via Polars native Delta reader.
    # _PointA is already in the Polars global registry from the write step above.
    df = pl.read_delta(delta_path)

    # Assert the column carries the correct Polars extension type — not a plain Struct.
    col_dtype = df.dtypes[0]
    assert col_dtype.is_extension(), (
        f"Expected a Polars extension type on column 'point', got {col_dtype!r}"
    )
    assert col_dtype.ext_name() == fqcn, (
        f"Expected extension name {fqcn!r}, got {col_dtype.ext_name()!r}"
    )
```

- [ ] **Step 2: Run the new test to verify it passes**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py::test_delta_polars_read_delta -v
```

Expected: 1 test passes.

- [ ] **Step 3: Run the full roundtrips file to confirm no regressions**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py -v
```

Expected: 13 tests pass (12 from Task 4 + 1 new).

- [ ] **Step 4: Commit**

```bash
git add tests/test_extension_types/test_roundtrips.py
git commit -m "test(extension-types): add Delta Polars native-read round-trip test (PLT-1659)"
```

---

## Task 6: Full test run and PR

**Files:** none

- [ ] **Step 1: Run the full extension-types test suite**

```bash
uv run pytest tests/test_extension_types/ -v
```

Expected: all tests pass.  The three new files contribute 17 tests:
- `test_schema_compatibility.py`: 4 tests
- `test_cache_behavior.py`: 2 tests
- `test_roundtrips.py`: 13 tests

- [ ] **Step 2: Run the broader test suite to check for regressions**

```bash
uv run pytest tests/ -x -q --ignore=tests/test_semantic_types
```

Expected: no new failures. (`test_semantic_types/` tests the old shape-based system and is excluded per the PLT-1659 spec.)

- [ ] **Step 3: Push the branch**

```bash
git push -u origin eywalker/plt-1659-integration-tests-end-to-end-semantic-type-round-trips
```

- [ ] **Step 4: Open the PR**

```bash
gh pr create \
  --base extension-type-system \
  --title "test(extension-types): end-to-end round-trip integration tests (PLT-1659)" \
  --body "$(cat <<'EOF'
## Summary

Adds three integration test files covering the full extension type round-trip pipeline:

- **`test_roundtrips.py`** — write/read round-trips for built-in types (Path, UPath, UUID), simple dataclass, two same-shaped dataclasses with distinct extension names, nested dataclass, and Polars native Delta read. Parameterised over Parquet and Delta backends.
- **`test_schema_compatibility.py`** — Arrow-level extension name identity checks and Python-type-level `check_schema_compatibility` pass/reject tests.
- **`test_cache_behavior.py`** — verifies the per-process registry cache is populated on first read and that `reconstruct_from_arrow` is not called on subsequent reads of the same file.

## Deferred (noted in corresponding issues)

- `list[MyDataclass]` round-trip → PLT-1732 (requires `ListLogicalType`)
- Picklable type tests → PLT-1658 (handler not yet implemented)
- SQLite value round-trips → excluded because `SQLiteConnector` does not preserve `ARROW:extension:*` field metadata; `ExtensionAwareDatabase` wrapper already tested in `test_extension_aware_database.py`

Closes PLT-1659
EOF
)"
```

- [ ] **Step 5: Confirm the PR URL is printed and note it**

The `gh pr create` command prints the PR URL. Record it for tracking.
