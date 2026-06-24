# PLT-1659: End-to-End Extension Type Round-Trip Integration Tests — Design Spec

**Date:** 2026-06-23
**Linear issue:** PLT-1659
**Branch:** `eywalker/plt-1659-integration-tests-end-to-end-semantic-type-round-trips`
**PR target:** `extension-type-system`

---

## Overview

This spec covers the design of end-to-end integration tests for the Arrow/Polars extension type
system introduced in the `extension-type-system` branch. The tests validate the complete pipeline:

```
Python object → write → storage → peek-schema → register → read → Python object
```

These are *integration* tests only. Existing unit tests in `tests/test_extension_types/` (registry,
schema walker, database hooks, built-in logical types, protocols) are not duplicated.

---

## What Is Tested

### Built-in types: `Path`, `UPath`, `UUID`

Round-trip through two storage backends (Parquet and Delta — SQLite excluded, see
`test_roundtrips.py` note). Assertions:
- Python object is faithfully reconstructed after read.
- Arrow extension names are in the `orcapod.*` namespace (`orcapod.path`, `orcapod.upath`,
  `orcapod.uuid`).

### Dataclass types

- **Simple dataclass** (scalar fields only): write → read → verify field values.
- **Two dataclasses with identical struct shape, different class names** (`_PointA` vs `_PointB`):
  verify they are stored and recovered as distinct extension types (distinct Arrow extension names).
- **Nested dataclass** (outer contains inner as a field): write → read → verify recursive
  reconstruction; assert both inner and outer types are registered after the read.

### Delta Lake direct read

Write a dataclass column to Delta Lake. Read back via `pl.read_delta` (Polars native Delta
reader). Assert the column dtype carries the correct extension type.

### Schema compatibility

Two sub-areas:

- **Arrow-level identity**: `converter.python_schema_to_arrow_schema` for `_PointA` and `_PointB`
  produces distinct Arrow extension names, even though the underlying struct shapes are identical.
- **Python-type-level compatibility**: `check_schema_compatibility` from `schema_utils` correctly
  passes when types match and rejects when the same-shaped-but-different-named types are used.

### Per-process cache behavior

- **Cache populated on read**: fresh converter + Parquet file containing a registered dataclass →
  after `converter.load_extension_types(...)`, the type is present in the registry.
- **Factory skipped on second read**: patching `factory.reconstruct_from_arrow` confirms it is
  called exactly once on first read and zero times on second read (registry hit short-circuits
  factory dispatch).

---

## What Is Explicitly Out of Scope

| Excluded | Reason | Tracked in |
|---|---|---|
| `list[MyDataclass]` round-trip | Known limitation (ET2); requires `ListLogicalType` infrastructure | PLT-1732 |
| Picklable types | `PicklableLogicalTypeFactory` (PLT-1658) not yet implemented | PLT-1658 |
| Pydantic round-trips | Already covered in `test_default_context_factories.py` | — |
| Duplicate unit tests | Existing unit tests in `test_extension_types/` are not repeated | — |

---

## File Organisation

Three new files, all in `tests/test_extension_types/`:

```
tests/test_extension_types/
├── test_roundtrips.py          # Write/read round-trips across backends
├── test_schema_compatibility.py # Arrow-level + Python-type-level compatibility
└── test_cache_behavior.py       # Per-process cache: populated / skipped on second read
```

---

## Backend Parameterisation

`test_roundtrips.py` parameterises over **two** storage backends via a `_StorageBackend`
dataclass with two callables. SQLite (`ConnectorArrowDatabase` + `SQLiteConnector`) is
excluded because `SQLiteConnector` discards `ARROW:extension:*` field metadata during type
mapping — see `DESIGN_ISSUES.md` CA1 and PLT-1795.

```python
@dataclasses.dataclass
class _StorageBackend:
    name: str
    write: Callable[[pa.Table, Path], None]
    read: Callable[[Path, UniversalTypeConverter], pa.Table]
```

| `name` | `write` | `read` |
|---|---|---|
| `"parquet"` | `pq.write_table(table, path / "data.parquet")` | `converter.load_extension_types(pq.read_table(path / "data.parquet"))` |
| `"delta"` | `deltalake.write_deltalake(str(path / "delta"), table)` | `converter.load_extension_types(DeltaTable(str(path / "delta")).to_pyarrow_dataset(as_large_types=True).to_table())` |

`as_large_types=True` is required for the Delta backend: without it, Delta Lake normalises
`large_string` → `string` and `large_binary` → `binary`, which causes the extension type
deserializer to reject the storage type mismatch.

The `read` callable always returns a `pa.Table` containing only the original user data columns.

A `@pytest.fixture(params=[...])` named `storage_backend` yields one `_StorageBackend` per run.

---

## Module-Level Test Fixtures

All test dataclasses must be defined at module level — `DataclassLogicalTypeFactory` rejects
local classes because they have no stable FQCN for reconstruction on read.

```python
# test_roundtrips.py, test_schema_compatibility.py, and test_cache_behavior.py
# Each file defines its own module-level dataclasses — no sharing across files.
@dataclasses.dataclass
class _PointA:
    x: int
    y: int

@dataclasses.dataclass
class _PointB:      # same shape as _PointA, different class name
    x: int
    y: int

@dataclasses.dataclass
class _Inner:
    value: int

@dataclasses.dataclass
class _Outer:
    inner: _Inner
    label: str
```

Each test creates its own converter via `create_registry().get_context().type_converter` (not
`get_default_context()`) to prevent cross-test contamination through the global singleton cache.

---

## Test Descriptions

### `test_roundtrips.py`

#### Parameterised over both backends

**`test_builtin_path_round_trip[backend]`**
Write a `Path` column, read back, assert `pathlib.Path` values are reconstructed and the Arrow
field extension name is `"orcapod.path"`.

**`test_builtin_upath_round_trip[backend]`**
Same for `UPath` / `"orcapod.upath"`.

**`test_builtin_uuid_round_trip[backend]`**
Same for `uuid.UUID` / `"orcapod.uuid"`.

**`test_simple_dataclass_round_trip[backend]`**
Write a `_PointA` column, read back, assert field values match and the Arrow field is an
`pa.ExtensionType` with extension name equal to the FQCN of `_PointA`.

**`test_nested_dataclass_round_trip[backend]`**
Write an `_Outer` column. Read back. Assert:
- `_Outer` and `_Inner` are both in the registry after read.
- Reconstructed value is an `_Outer` with an `_Inner` field; all values correct.

#### Delta Lake only

**`test_delta_polars_read_delta`**
Write a `_PointA` column to Delta via `deltalake.write_deltalake`. Read back via
`pl.read_delta(str(delta_path))`. Assert the resulting Polars DataFrame column has dtype
that is a Polars extension type (i.e. the extension type survived the Delta round-trip).

### `test_schema_compatibility.py`

**`test_arrow_schema_distinct_extension_names_for_same_shape`**
Register `_PointA` and `_PointB` with a fresh converter. Assert:
```python
schema_a.field("value").type.extension_name != schema_b.field("value").type.extension_name
```

**`test_arrow_schema_same_extension_name_idempotent`**
Register `_PointA` twice. Assert the extension name is the same both times.

**`test_python_schema_compatibility_passes_same_type`**
`check_schema_compatibility({"value": _PointA}, Schema({"value": _PointA}))` → `True`.

**`test_python_schema_compatibility_rejects_different_type_same_shape`**
`check_schema_compatibility({"value": _PointA}, Schema({"value": _PointB}))` → `False`.
This is the core guarantee: the extension type system prevents same-shape-different-class
confusion that would have been silently accepted by the old shape-based system.

### `test_cache_behavior.py`

**`test_cache_populated_after_first_read`**
1. Write a Parquet file with a `_PointA` column (fresh converter, type registered for write).
2. Create a second fresh converter (type *not* pre-registered).
3. Call `read_converter.load_extension_types(pq.read_table(path))`.
4. Assert `read_converter._logical_type_registry.get_by_arrow_extension_name(fqcn)` is not `None`.

**`test_factory_not_called_on_second_read`**
1. Write Parquet as above.
2. Fresh converter. Patch `DataclassLogicalTypeFactory.reconstruct_from_arrow` with a spy.
3. First `load_extension_types` call → spy called exactly once.
4. Second `load_extension_types` call on the same file → spy call count unchanged (registry hit).

---

## Key Implementation Notes

- Use `uv run pytest` (never bare `pytest`) per CLAUDE.md.
- No `POLARS_UNKNOWN_EXTENSION_TYPE_BEHAVIOR` env var needed — tests rely on registration.
- All tests use `tmp_path` (pytest built-in) for temp dirs; no external cluster required.
- SQLite backend uses `SQLiteConnector(str(tmp_path / "db.sqlite"))` — not `:memory:`, because
  the `ConnectorArrowDatabase` instance is recreated between write and read to simulate
  the separate-process scenario.
- Delta backend requires `deltalake` package (already a project dependency).
