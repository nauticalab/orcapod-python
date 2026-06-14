# PLT-1655: Peek-Schema → Register → Read Pattern with Per-Process Cache

**Date:** 2026-06-14
**Linear issue:** PLT-1655
**Status:** DRAFT — blocked on PLT-1668

> ⚠️ **This spec is a work-in-progress and is expected to be revisited and updated once
> PLT-1668 lands.** PLT-1668 redesigns `ExtensionTypeConverter` → `LogicalType` and
> `ExtensionTypeRegistry` → `LogicalTypeRegistry`. Several naming and signature decisions
> below will change when that redesign is complete. See the
> [Pending PLT-1668](#pending-plt-1668) section for an explicit list of what is unsettled.

---

## Overview

Wire a single, additive call into the two existing database read methods so that any Arrow
extension types present in a schema are automatically registered in both PyArrow's and
Polars' global registries before data is returned. Repeated reads within the same process
are cheap because already-registered types are detected and skipped by the registry.

The peek helper itself stays deliberately dumb: it walks the schema, then delegates each
found type to the registry. All handler dispatch logic lives in the registry.

---

## Goals & Success Criteria

* `ensure_extensions_registered(schema)` in `extension_types/database_hooks.py` is
  called before every table return in `DeltaTableDatabase._read_delta_table()` and
  `ConnectorArrowDatabase._get_committed_table()`.
* When the schema contains no extension types the call is a no-op; existing tests continue
  to pass unchanged.
* When the schema contains a known extension type (one whose category handler is
  registered) the type is registered in PyArrow and Polars before the table is returned.
* When the schema contains an extension type whose category metadata is unknown, a clear
  `ValueError` is raised naming the extension name and metadata bytes.
* Repeated reads that encounter the same extension type are effectively free — the
  registry is idempotent (already-registered types are detected and skipped).

---

## Scope & Boundaries

In scope:
* New `src/orcapod/extension_types/database_hooks.py`
* Additive modification of `src/orcapod/databases/delta_lake_databases.py`
  (`_read_delta_table`)
* Additive modification of `src/orcapod/databases/connector_arrow_database.py`
  (`_get_committed_table`)
* New `CategoryHandler` Protocol in `src/orcapod/extension_types/protocols.py`
* New methods on `ExtensionTypeRegistry` (pending rename to `LogicalTypeRegistry`):
  `register_category_handler` and `prepare_extension_type`
* Additive exports in `src/orcapod/extension_types/__init__.py`
* Tests for all new code

Out of scope:
* Implementing concrete category handlers (PLT-1657 `dataclass_handler`,
  PLT-1658 `picklable_handler`) — they will call `register_category_handler` on
  the module-level registry instance at import time
* Built-in logical type registrations (PLT-1656)
* Thread safety of the global shadow dicts (deferred)
* Any change to `semantic_types/` (old system, untouched until PLT-1660)

---

## Architecture

### File map

| File | Change |
|---|---|
| `src/orcapod/extension_types/protocols.py` | Add `CategoryHandler` Protocol |
| `src/orcapod/extension_types/registry.py` | Add `register_category_handler`, `prepare_extension_type` |
| `src/orcapod/extension_types/database_hooks.py` | **New** — `ensure_extensions_registered` |
| `src/orcapod/extension_types/__init__.py` | Additive exports |
| `src/orcapod/databases/delta_lake_databases.py` | Additive — call in `_read_delta_table` |
| `src/orcapod/databases/connector_arrow_database.py` | Additive — call in `_get_committed_table` |
| `tests/test_extension_types/test_database_hooks.py` | **New** |

---

## `CategoryHandler` Protocol

**Location:** `src/orcapod/extension_types/protocols.py`

`CategoryHandler` is a pure factory. Given an Arrow extension name and its storage type
(both extracted from the schema by the walker), it constructs a fully-formed converter
instance (currently `ExtensionTypeConverter`; renamed to `LogicalType` after PLT-1668).
The category tag that routes to this handler is declared by the caller at registration
time — the handler itself has no knowledge of its dispatch key.

```python
class CategoryHandler(Protocol):
    def create_converter(
        self,
        extension_name: str,
        storage_type: pa.DataType,
    ) -> ExtensionTypeConverter:
        """Construct a converter for the given extension name and storage type.

        Args:
            extension_name: The Arrow extension type name extracted from the schema
                (i.e. the value of ``ARROW:extension:name`` field metadata).
            storage_type: The underlying Arrow storage type for this extension field.

        Returns:
            A fully constructed ``ExtensionTypeConverter`` ready to be passed to
            ``ExtensionTypeRegistry.register()``.

        Raises:
            ValueError: If this handler cannot construct a converter for the given
                extension name (e.g. the Python class cannot be resolved).
        """
        ...
```

> **Post-PLT-1668 note:** `create_converter` return type changes from
> `ExtensionTypeConverter` to `LogicalType`. The `extension_name` parameter meaning may
> shift slightly depending on how `logical_type_name` vs Arrow extension name are
> distinguished in the new design — see [Pending PLT-1668](#pending-plt-1668).

---

## `ExtensionTypeRegistry` additions

**Location:** `src/orcapod/extension_types/registry.py`

Two new methods are added to `ExtensionTypeRegistry` (to be renamed `LogicalTypeRegistry`
post-PLT-1668). The existing public API is unchanged.

### `register_category_handler`

```python
def register_category_handler(
    self,
    metadata_tag: bytes,
    handler: CategoryHandler,
) -> None:
    """Register a category handler for the given metadata tag.

    When ``prepare_extension_type`` encounters an extension type whose
    ``extension_metadata`` bytes match ``metadata_tag``, it calls
    ``handler.create_converter(extension_name, storage_type)`` to construct
    the converter and then registers it.

    Args:
        metadata_tag: The ``extension_metadata`` bytes value that identifies
            this category (e.g. ``b"orcapod.dataclass"``).
        handler: A ``CategoryHandler`` instance responsible for constructing
            converters for this category.

    Raises:
        ValueError: If ``metadata_tag`` is already registered to a different handler.
    """
```

The registry stores handlers in a new `_category_handlers: dict[bytes, CategoryHandler]`
instance attribute, populated at construction with an empty dict.

### `prepare_extension_type`

```python
def prepare_extension_type(
    self,
    extension_name: str,
    extension_metadata: bytes | None,
    storage_type: pa.DataType,
) -> None:
    """Ensure the extension type identified by ``extension_name`` is registered.

    This is the single call-site for ``ensure_extensions_registered`` in
    ``database_hooks.py``. The registry owns all dispatch logic:

    1. If ``extension_name`` is already registered — return immediately (no-op).
    2. Look up a ``CategoryHandler`` by ``extension_metadata`` in
       ``_category_handlers``.
    3. If no handler is found, raise ``ValueError`` with a clear message
       naming both the extension name and metadata bytes.
    4. Call ``handler.create_converter(extension_name, storage_type)`` to
       obtain a converter.
    5. Call ``self.register(converter)`` to register it in this registry and
       in PyArrow's / Polars' global registries.

    Args:
        extension_name: Arrow extension type name (``ARROW:extension:name``).
        extension_metadata: Category tag bytes (``ARROW:extension:metadata``),
            or ``None`` if absent.
        storage_type: Underlying Arrow storage type for this extension field.

    Raises:
        ValueError: If no category handler is registered for ``extension_metadata``.
        ValueError: If handler raises during converter construction.
    """
```

The "already registered" check in step 1 reuses `has_extension_name(extension_name)`.
This is the per-process caching mechanism — no separate module-level `set` is needed in
`database_hooks.py`; the registry's own `_by_name` dict is the cache.

> **Post-PLT-1668 note:** The "already registered" check will use
> `get_by_arrow_extension_name(arrow_name)` from `LogicalTypeRegistry`. The parameter
> names and exact semantics of `extension_name` here will be reconciled with the
> `logical_type_name` / Arrow extension name distinction introduced in PLT-1668.

---

## `database_hooks.py`

**Location:** `src/orcapod/extension_types/database_hooks.py`

```python
"""Peek-schema hook for extension type auto-registration at database read time.

Call ``ensure_extensions_registered(schema)`` before returning any Arrow table
from a database read path. It is a no-op when the schema contains no extension
types.
"""

from __future__ import annotations

import pyarrow as pa

from orcapod.extension_types import default_extension_type_registry
from orcapod.extension_types.schema_walker import walk_schema


def ensure_extensions_registered(schema: pa.Schema) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively using the schema walker to discover all Arrow
    extension types at any nesting depth. For each discovered type, delegates
    to ``default_extension_type_registry.prepare_extension_type(...)``.

    Already-registered types are detected and skipped inside the registry —
    this function itself is stateless.

    Args:
        schema: The Arrow schema to inspect. May contain no extension types,
            in which case this call is a no-op.

    Raises:
        ValueError: Propagated from the registry if an extension type's category
            metadata has no registered handler.
    """
    for info in walk_schema(schema):
        default_extension_type_registry.prepare_extension_type(
            info.extension_name,
            info.extension_metadata,
            info.storage_type,
        )
```

This function is intentionally stateless and contains no dispatch logic. All complexity
lives in the registry.

---

## Database call-site hooks

Both modifications are strictly additive — a single new line in each method, no existing
logic altered.

### `DeltaTableDatabase._read_delta_table`

**Schema peek:** `DeltaTable.schema().to_arrow()` — this is a cheap metadata-only read
that does not scan any Parquet data files.

The call is placed **after** the schema is obtained and **before** `dataset.to_table()`
is called. Registering extension types before materialising the Arrow table ensures
PyArrow can deserialise extension-typed columns correctly.

```python
# Inside _read_delta_table, after: dataset = delta_table.to_pyarrow_dataset(...)
schema = delta_table.schema().to_arrow()
ensure_extensions_registered(schema)
# Existing table materialisation continues unchanged
```

### `ConnectorArrowDatabase._get_committed_table`

**Schema peek:** The existing `iter_batches` call already fetches data; the schema is
available on the first batch via `batches[0].schema`. No additional query is needed.

The call is placed after `batches` is populated but before the final `pa.Table.from_batches`:

```python
batches = list(self._connector.iter_batches(f'SELECT * FROM "{table_name}"'))
if not batches:
    return None
ensure_extensions_registered(batches[0].schema)
return pa.Table.from_batches(batches)
```

> **Note:** A `LIMIT 0` pre-query was considered to avoid fetching data before knowing
> whether extension type registration is needed, but was rejected: the existing code
> already fetches all batches in a single pass, and adding a second round-trip for a
> schema-only peek would increase latency for the common case where no extension types
> are present. The first-batch schema approach adds zero extra queries.

---

## Per-process cache design

The "per-process cache" described in the PLT-1655 issue is realised via the registry's
own `_by_name` dict. `prepare_extension_type` checks `has_extension_name(name)` as its
first step and returns immediately if the type is already registered. Because the
module-level `default_extension_type_registry` instance lives for the lifetime of the
process, this is equivalent to a module-level `set` cache — without the redundancy of
maintaining a parallel data structure.

**No separate `set` in `database_hooks.py`.** The function is stateless; the registry
is the cache.

---

## Error handling

Unknown category metadata raises a `ValueError` from inside `prepare_extension_type`:

```
ValueError: No category handler is registered for extension metadata b"orcapod.custom".
Cannot prepare extension type 'com.example.MyType' for registration.
Register a CategoryHandler for this metadata tag via
default_extension_type_registry.register_category_handler(b"orcapod.custom", handler).
```

The message includes: the metadata bytes, the extension name, and a pointer to the
registration call needed to fix the problem.

---

## Tests

**`tests/test_extension_types/test_database_hooks.py`**

| Test | What it covers |
|---|---|
| `test_no_extension_types_is_noop` | Schema with only primitives — `ensure_extensions_registered` returns without touching the registry |
| `test_known_type_is_registered` | Schema with one extension type whose category handler is registered — converter is registered in PA/Polars |
| `test_already_registered_is_skipped` | Call `ensure_extensions_registered` twice with the same schema — second call is a no-op (no duplicate registration error) |
| `test_unknown_metadata_raises` | Schema with extension type whose metadata has no handler — raises `ValueError` with extension name and metadata in message |
| `test_nested_extension_type` | Extension type inside a struct column — walker descends and hook registers it |
| `test_none_metadata_raises` | Extension type with `None` metadata and no `None`-keyed handler — raises `ValueError` |

**`tests/test_extension_types/test_registry.py`** additions:

| Test | What it covers |
|---|---|
| `test_register_category_handler` | Handler registered; `prepare_extension_type` dispatches to it |
| `test_prepare_already_registered_noop` | `prepare_extension_type` called twice — second is no-op |
| `test_prepare_unknown_metadata_raises` | Clear `ValueError` for unknown metadata |
| `test_register_duplicate_handler_raises` | `register_category_handler` with same tag twice raises `ValueError` |

---

## Pending PLT-1668

PLT-1668 renames and redesigns the core extension type protocol and registry. The
following items in this spec are expected to change:

| Item | Current (pre-PLT-1668) | Expected change |
|---|---|---|
| `ExtensionTypeConverter` | Protocol with `extension_name`, `extension_metadata`, `storage_type` properties | Renamed to `LogicalType`; extension type details encapsulated in `get_arrow_extension_type()` |
| `ExtensionTypeRegistry` | Registry keyed by `extension_name` | Renamed to `LogicalTypeRegistry`; three-way binding (`logical_type_name`, arrow ext name, python type) |
| `CategoryHandler.create_converter` return type | `ExtensionTypeConverter` | `LogicalType` |
| `prepare_extension_type` "already registered" check | `has_extension_name(name)` | `get_by_arrow_extension_name(arrow_ext_name)` from `LogicalTypeRegistry` |
| `prepare_extension_type` parameter `extension_name` | Arrow `ARROW:extension:name` value | Will need to reconcile with `logical_type_name` vs Arrow extension name distinction |
| `default_extension_type_registry` | `ExtensionTypeRegistry` instance | Renamed to `default_logical_type_registry` |

**None of the `database_hooks.py` logic or the database call-site hooks are expected to
change** — the function signature `ensure_extensions_registered(schema: pa.Schema)` and
its stateless delegation pattern are stable regardless of the registry redesign.

---

## Dependencies

* PLT-1653 (`ExtensionTypeRegistry`) — **merged** into `extension-type-system`
* PLT-1654 (`schema_walker`) — **merged** into `extension-type-system`
* **PLT-1668** (`LogicalType` / `LogicalTypeRegistry` redesign) — **blocks this issue**
