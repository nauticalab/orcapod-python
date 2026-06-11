# UUID Arrow Type Mapping Design

**Date:** 2026-06-11
**Linear issue:** [PLT-1162](https://linear.app/enigma-metamorphic/issue/PLT-1162/design-spike-uuid-arrow-type-mapping-large_string-vs-dedicated-struct)
**Status:** Approved
**Breaking change:** Yes — intentional and acceptable at pre-v0.1.0

---

## Overview

OrcaPod has historically stored UUID values (both internal system identifiers and
user-facing data) as `pa.large_string()`. This spec formalises the canonical Arrow
representation for all UUID values: **`pa.binary(16)` (`fixed_size_binary[16]`)**,
with a named constant for system use and a dedicated semantic struct type for
user-facing `uuid.UUID` round-trips through the type system.

All UUID values are stored at full 16-byte precision. No truncation or abbreviation
is permitted.

---

## Goals & Success Criteria

- A single named constant `UUID_ARROW_TYPE = pa.binary(16)` is the authoritative
  Arrow type for all internal UUID columns (`datagram_id`, `record_id`, `_log_id`,
  `_status_id`, system tag identifier columns).
- A semantic struct type `pa.struct([pa.field("uuid", pa.binary(16))])` enables
  `uuid.UUID` Python objects to round-trip through Arrow, registering in the
  semantic type registry alongside `path`/`upath`.
- Every UUID stored in Arrow uses the full 16 bytes. No hex strings, no dashes, no
  truncated representations.
- The `# TODO: revisit mapping once PLT-1162 decides` comment in
  `databases/postgresql_connector.py` is removed and replaced with the correct
  binary mapping.
- All DB connectors (`PostgreSQLConnector`, `SQLiteConnector`) decode string UUID
  values from the DB driver into `bytes` before constructing Arrow arrays.
- No `pa.large_string()` UUID columns remain in system-managed schemas.

---

## Design

### Two-layer UUID representation

| Layer | Arrow type | Used for |
|---|---|---|
| **System** | `pa.binary(16)` via `UUID_ARROW_TYPE` | `datagram_id`, `record_id`, `_log_id`, `_status_id`, system tag ID columns |
| **Semantic** | `pa.struct([pa.field("uuid", pa.binary(16))])` via `UUID_STRUCT_ARROW_TYPE` | User functions returning `uuid.UUID`; DB `uuid` columns mapped through the semantic type system |

### Named constants

Place both constants in `src/orcapod/types.py` alongside `Schema`, `ColumnConfig`,
and `ContentHash`:

```python
import pyarrow as pa

# Canonical Arrow type for all UUID values in OrcaPod.
# Stored as fixed_size_binary[16] — 16 raw bytes, no hex encoding, no dashes.
UUID_ARROW_TYPE: pa.DataType = pa.binary(16)

# Semantic struct type for Python uuid.UUID round-trips through the type system.
# Follows the same single-field struct pattern as path/upath.
UUID_STRUCT_ARROW_TYPE: pa.StructType = pa.struct(
    [pa.field("uuid", UUID_ARROW_TYPE)]
)
```

### UUID generation at all sites

All sites that generate a UUID for Arrow storage must produce `bytes`, not `str`:

```python
# Before
self._datagram_id = str(uuid7())

# After
self._datagram_id = uuid7().bytes
```

`uuid_utils.uuid7()` returns a `uuid.UUID`-compatible object with a `.bytes`
property. Standard `uuid.uuid4()` and `uuid.uuid5()` likewise provide `.bytes`.

`run_id` in `sync_orchestrator.py` and `async_orchestrator.py` is a **transient
Python string** passed to observer callbacks — it is never stored in an Arrow
column and is explicitly out of scope for this change. It remains `str(uuid.uuid4())`.

### Semantic struct converter

Add `UUIDStructConverter` in `src/orcapod/semantic_types/semantic_struct_converters.py`,
following the `PythonPathStructConverter`/`UPathStructConverter` pattern:

```python
class UUIDStructConverter(SemanticStructConverterBase):
    """Converts Python ``uuid.UUID`` objects to/from the OrcaPod UUID struct."""

    python_type = uuid.UUID
    arrow_struct_type = UUID_STRUCT_ARROW_TYPE

    def python_to_struct_dict(self, value: uuid.UUID) -> dict[str, bytes]:
        return {"uuid": value.bytes}

    def struct_dict_to_python(self, struct_dict: dict[str, Any]) -> uuid.UUID:
        return uuid.UUID(bytes=struct_dict["uuid"])
```

Register `UUIDStructConverter` in the default semantic type registry alongside
`PythonPathStructConverter` and `UPathStructConverter`.

### DB connector mapping

**PostgreSQL** (`databases/postgresql_connector.py`):

The `_pg_type_to_arrow` function currently maps `uuid` to `pa.large_string()` with
a TODO comment. Replace with `UUID_ARROW_TYPE`:

```python
if t == "uuid":
    return UUID_ARROW_TYPE
```

The PostgreSQL driver (`psycopg2`/`asyncpg`) returns UUID column values as Python
`uuid.UUID` objects or strings depending on the driver and registration. During
array construction, extract `.bytes` from each value before passing to the
`pa.binary(16)` array builder.

**SQLite** (`databases/sqlite_connector.py`):

SQLite has no native UUID type; all UUID columns are stored as `TEXT` and arrive
as strings from the driver. Because `TEXT` is used for all string-like data, the
connector cannot automatically distinguish UUID columns from other string columns.
`TEXT` columns therefore continue to map to `pa.large_string()` by default.
No change is required to the SQLite connector as part of this spec. The broader
question of how OrcaPod preserves Arrow type fidelity when round-tripping through
loosely-typed backends is tracked in
[PLT-1615](https://linear.app/enigma-metamorphic/issue/PLT-1615).

### No truncation — enforcement

The rule "no truncation of UUID values" specifically means:

- No `uuid4().hex[:N]` patterns for UUID identity columns.
- No `str(uuid7())[:N]` style abbreviations.
- UUID-carrying Arrow columns always use the full 16-byte representation.

The broader audit of hardcoded hash truncation lengths across the codebase (covering
`job.py`, `serialization.py`, `universal_converter.py`, etc.) is tracked separately
under [PLT-1614](https://linear.app/enigma-metamorphic/issue/PLT-1614).

---

## Alternatives Considered

### `pa.large_string()` for all UUIDs

Current state. Simple and Polars-compatible everywhere including `write_json()`.
Rejected because: semantically imprecise (UUID is binary, not a string); 2.75× larger
storage per UUID value; no type-level distinction from arbitrary strings.

### `pa.uuid()` PyArrow extension type

Available in PyArrow ≥ 20.0 as `pa.uuid()` (`arrow.uuid` extension over
`fixed_size_binary[16]`). Rejected because Polars 1.31 raises
`ComputeError: cannot create series from Extension(...)` on conversion — a hard
incompatibility.

### `pa.struct([pa.field("uuid", pa.large_string())])` for semantic type

Considered for the semantic struct inner type to stay consistent with the `path`/
`upath` precedent. Rejected in favour of `binary(16)` because: paths are
fundamentally strings (their string form is their canonical representation), while
UUIDs are fundamentally binary (strings are merely a display convention). Using
`binary(16)` also aligns the semantic struct's inner type with the system constant,
giving a single canonical representation.

---

## Known Limitations

**Polars `write_json()` panics on `Binary`/`BinaryView` columns.**

Tracked upstream: [pola-rs/polars#15410](https://github.com/pola-rs/polars/issues/15410)
(filed April 2024, open as of Polars 1.31.0).

This is acceptable because:
- OrcaPod has zero calls to `pl.DataFrame.write_json()` anywhere in its codebase.
- The custom `serialize_pyarrow_table` in `hashing/arrow_utils.py` uses
  `column.to_pylist()` + stdlib `json.dumps` and already handles `bytes` values
  via base64 encoding — it is unaffected.
- The Polars team has this on their radar; the fix will propagate automatically
  once merged.

---

## Implementation Scope

This spec covers the design decision. Implementation is a follow-on task:

| File | Change |
|---|---|
| `src/orcapod/types.py` | Add `UUID_ARROW_TYPE`, `UUID_STRUCT_ARROW_TYPE` constants |
| `src/orcapod/semantic_types/semantic_struct_converters.py` | Add `UUIDStructConverter` |
| `src/orcapod/semantic_types/` (registry setup) | Register `UUIDStructConverter` |
| `src/orcapod/utils/arrow_data_utils.py` | Update system column field definitions |
| `src/orcapod/core/datagrams/datagram.py` | `str(uuid7())` → `uuid7().bytes` |
| `src/orcapod/core/data_function.py` | `str(uuid7())` → `uuid7().bytes` |
| `src/orcapod/pipeline/logging_observer.py` | `str(uuid7())` → `uuid7().bytes` |
| `src/orcapod/pipeline/status_observer.py` | `str(uuid7())` → `uuid7().bytes` |
| `src/orcapod/databases/postgresql_connector.py` | Remove TODO; map `uuid` → `UUID_ARROW_TYPE`; decode driver values to bytes |
| `src/orcapod/databases/sqlite_connector.py` | No change — `TEXT` columns remain `pa.large_string()` |
| `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Update `UUIDHandler` to accept `bytes` input |
| Tests | Update all tests that assert `pa.large_string()` for UUID columns |
