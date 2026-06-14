# PLT-1655: Peek-Schema → Register → Read Pattern with Per-Process Cache

**Date:** 2026-06-14
**Linear issue:** PLT-1655
**Status:** Approved

---

## Overview

Wire a single, additive call into the two existing database read methods so that any Arrow
extension types present in a schema are automatically registered in both PyArrow's and
Polars' global registries before data is returned. Repeated reads within the same process
are cheap because already-registered types are detected and skipped by the registry's
three-way binding.

The peek helper itself stays deliberately dumb: it walks the schema, then delegates each
found type to the registry. All factory dispatch logic lives in the registry.

---

## Goals & Success Criteria

* `ensure_extensions_registered(schema)` in `extension_types/database_hooks.py` is
  called before every table return in `DeltaTableDatabase._read_delta_table()` and
  `ConnectorArrowDatabase._get_committed_table()`.
* When the schema contains no extension types the call is a no-op; existing tests
  continue to pass unchanged.
* For each extension type found in the schema, `prepare_extension_type` applies checks
  in this order:
  1. **Already registered** (by Arrow extension name in `default_logical_type_registry`)
     → silent no-op. This is the common fast path for all types after first registration,
     including built-ins like `arrow.uuid` pre-registered at import time by PLT-1656.
     Metadata value is irrelevant — `None` metadata on an already-registered type never
     causes an error.
  2. **Not registered, non-`None` metadata, matching factory** → factory constructs a
     `LogicalType` and it is registered in PyArrow, Polars, and the registry before the
     table is returned.
  3. **Not registered, non-`None` metadata, no matching factory** → clear `ValueError`
     naming the extension name and metadata tag, with a pointer to
     `register_logical_type_factory`.
  4. **Not registered, `None` metadata** → clear `ValueError` explaining that types
     without a category tag cannot be auto-registered via a factory and must be
     pre-registered explicitly via `registry.register(logical_type)`.
* Sufficient `DEBUG`-level logging throughout so that extension type discovery,
  registration decisions, and factory dispatch are observable without code changes.

---

## Scope & Boundaries

In scope:
* New `src/orcapod/extension_types/database_hooks.py`
* Additive modification of `src/orcapod/databases/delta_lake_databases.py`
  (`_read_delta_table`)
* Additive modification of `src/orcapod/databases/connector_arrow_database.py`
  (`_get_committed_table`)
* New `LogicalTypeFactory` Protocol in `src/orcapod/extension_types/protocols.py`
* New methods on `LogicalTypeRegistry` (`registry.py`):
  `register_logical_type_factory` and `prepare_extension_type`
* Additive exports in `src/orcapod/extension_types/__init__.py`
* Tests for all new code

Out of scope:
* Implementing concrete `LogicalTypeFactory` instances (PLT-1657 `dataclass_handler`,
  PLT-1658 `picklable_handler`) — they will call `register_logical_type_factory` on
  the module-level registry instance at import time
* Built-in logical type registrations (PLT-1656)
* Thread safety of the global registry dicts (deferred)
* Any change to `semantic_types/` (old system, untouched until PLT-1660)

---

## Architecture

### File map

| File | Change |
|---|---|
| `src/orcapod/extension_types/protocols.py` | Add `LogicalTypeFactory` Protocol |
| `src/orcapod/extension_types/registry.py` | Add `register_logical_type_factory`, `prepare_extension_type` |
| `src/orcapod/extension_types/database_hooks.py` | **New** — `ensure_extensions_registered` |
| `src/orcapod/extension_types/__init__.py` | Additive exports |
| `src/orcapod/databases/delta_lake_databases.py` | Additive — call in `_read_delta_table` |
| `src/orcapod/databases/connector_arrow_database.py` | Additive — call in `_get_committed_table` |
| `tests/test_extension_types/test_database_hooks.py` | **New** |

---

## `LogicalTypeFactory` Protocol

**Location:** `src/orcapod/extension_types/protocols.py`

`LogicalTypeFactory` is a pure factory. Given an Arrow extension name, its storage type,
and the full parsed metadata dict (both the Arrow fields extracted from the schema by the
walker, and the metadata parsed from JSON), it constructs a fully-formed `LogicalType`
instance ready to pass to `LogicalTypeRegistry.register()`.

The `category` string that routes to this factory is declared by the caller at
registration time — the factory itself has no knowledge of its dispatch key, but receives
the full metadata dict so it can read additional hints (e.g. version, serialisation
format) beyond just the category.

### Metadata format

`extension_metadata` bytes are expected to be **UTF-8-encoded JSON** with at least a
`"category"` key:

```json
{"category": "Dataclass"}
{"category": "Pickle", "protocol": 5}
{"category": "Pydantic", "pydantic_version": 2}
```

The `category` value is the factory dispatch key. All other fields are passed through to
the factory as-is and interpreted by the factory implementation.

### Protocol definition

```python
class LogicalTypeFactory(Protocol):
    def create_logical_type(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict,
    ) -> LogicalType:
        """Construct a ``LogicalType`` for the given Arrow extension name and storage type.

        Args:
            arrow_extension_name: The Arrow extension type name extracted from the
                schema (i.e. the value of ``ARROW:extension:name`` field metadata).
            storage_type: The underlying Arrow storage type for this extension field.
            metadata: The full parsed JSON metadata dict. Always contains at least a
                ``"category"`` key. May contain additional keys the factory uses (e.g.
                ``"protocol"``, ``"pydantic_version"``).

        Returns:
            A fully constructed ``LogicalType`` ready to be passed to
            ``LogicalTypeRegistry.register()``.

        Raises:
            ValueError: If this factory cannot construct a logical type for the given
                extension name (e.g. the Python class cannot be resolved by name).
        """
        ...
```

This protocol is `@runtime_checkable`, consistent with `LogicalType`.

---

## `LogicalTypeRegistry` additions

**Location:** `src/orcapod/extension_types/registry.py`

Two new methods are added to `LogicalTypeRegistry`. The existing public API is unchanged.

### `register_logical_type_factory`

```python
def register_logical_type_factory(
    self,
    category: str,
    factory: LogicalTypeFactory,
) -> None:
    """Register a factory for the given metadata category string.

    When ``prepare_extension_type`` encounters an Arrow extension type whose
    ``extension_metadata`` JSON contains ``{"category": "<category>", ...}``,
    it calls ``factory.create_logical_type(arrow_extension_name, storage_type,
    metadata_dict)`` to construct the logical type and then registers it.

    Args:
        category: The ``"category"`` value from the extension metadata JSON that
            identifies this category (e.g. ``"Dataclass"``).
        factory: A ``LogicalTypeFactory`` instance responsible for constructing
            logical types for this category.

    Raises:
        ValueError: If ``category`` is already registered to a different factory.
    """
```

Stores factories in a new `_factories: dict[str, LogicalTypeFactory]` instance
attribute initialised to `{}` in `__init__`.

Logging:
* `DEBUG`: `"registered LogicalTypeFactory for category %r: %r"` on success.

### `prepare_extension_type`

```python
def prepare_extension_type(
    self,
    arrow_extension_name: str,
    extension_metadata: bytes | None,
    storage_type: pa.DataType,
) -> None:
    """Ensure the Arrow extension type identified by ``arrow_extension_name``
    is registered as a ``LogicalType``.

    This is the single entry point called by ``ensure_extensions_registered``
    in ``database_hooks``. The registry owns all dispatch logic:

    1. If ``arrow_extension_name`` is already in the three-way binding
       (``get_by_arrow_extension_name`` returns non-``None``) — return
       immediately (per-process cache hit). Metadata is not inspected.
    2. If ``extension_metadata`` is ``None``, raise ``ValueError`` directing
       the caller to pre-register the type explicitly.
    3. Attempt to decode ``extension_metadata`` as UTF-8 JSON. If decoding
       or parsing fails, raise ``ValueError`` with the raw bytes and the
       parse error.
    4. Extract the ``"category"`` key from the parsed dict. If absent, raise
       ``ValueError`` naming the extension and the raw metadata.
    5. Look up a ``LogicalTypeFactory`` by the ``category`` string in
       ``_factories``. If not found, raise ``ValueError`` naming the extension,
       the category, and the registration call needed.
    6. Call ``factory.create_logical_type(arrow_extension_name, storage_type,
       metadata_dict)`` to obtain a ``LogicalType``.
    7. Call ``self.register(logical_type)`` to complete the three-way binding
       and side-effect-register in PyArrow's and Polars' global registries.

    Args:
        arrow_extension_name: Arrow extension type name (``ARROW:extension:name``).
        extension_metadata: Raw metadata bytes (``ARROW:extension:metadata``),
            expected to be UTF-8 JSON containing at least a ``"category"`` key.
            ``None`` if absent.
        storage_type: Underlying Arrow storage type for this extension field.

    Raises:
        ValueError: If ``extension_metadata`` is ``None``.
        ValueError: If ``extension_metadata`` is not valid UTF-8 JSON.
        ValueError: If the parsed JSON has no ``"category"`` key.
        ValueError: If no factory is registered for the ``"category"`` value.
        ValueError: Propagated from the factory if it cannot construct a type.
    """
```

Logging:
* `DEBUG`: `"prepare_extension_type: %r already registered, skipping"` on cache hit (step 1).
* `DEBUG`: `"prepare_extension_type: %r not registered — dispatching to category %r factory"` before factory call (step 6).
* `DEBUG`: `"prepare_extension_type: successfully registered %r via %r factory"` after `self.register` returns (step 7).

Error messages:

**Step 2 — `None` metadata:**
```
ValueError: Extension type '<name>' has no extension metadata (metadata is None).
Types without a metadata category tag cannot be auto-registered via a factory —
they must be pre-registered explicitly via
default_logical_type_registry.register(logical_type).
```

**Step 3 — metadata not valid JSON:**
```
ValueError: Extension type '<name>' has extension metadata that is not valid UTF-8 JSON:
b'<raw bytes>'. Parse error: <error>.
Extension metadata must be a JSON object with at least a "category" key, e.g.
{"category": "Dataclass"}.
```

**Step 4 — JSON missing `"category"` key:**
```
ValueError: Extension type '<name>' has extension metadata JSON with no "category" key:
<parsed dict>. Extension metadata must be a JSON object with at least a "category" key,
e.g. {"category": "Dataclass"}.
```

**Step 5 — no factory for category:**
```
ValueError: No LogicalTypeFactory is registered for category '<category>'.
Cannot prepare extension type '<name>' for registration.
Register a factory via default_logical_type_registry.register_logical_type_factory(
    '<category>', factory
).
```

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

import logging

import pyarrow as pa

from orcapod.extension_types import default_logical_type_registry
from orcapod.extension_types.schema_walker import walk_schema

logger = logging.getLogger(__name__)


def ensure_extensions_registered(schema: pa.Schema) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively to discover all Arrow extension types at any
    nesting depth. For each discovered type, delegates to
    ``default_logical_type_registry.prepare_extension_type``.

    Already-registered types are detected and skipped inside the registry —
    this function itself is stateless.

    Args:
        schema: The Arrow schema to inspect. May contain no extension types,
            in which case this call is a no-op.

    Raises:
        ValueError: Propagated from the registry if an extension type's category
            metadata has no registered factory.
    """
    found = walk_schema(schema)
    if not found:
        logger.debug("ensure_extensions_registered: no extension types in schema")
        return
    logger.debug(
        "ensure_extensions_registered: found %d extension type(s) in schema: %s",
        len(found),
        [info.extension_name for info in found],
    )
    for info in found:
        default_logical_type_registry.prepare_extension_type(
            info.extension_name,
            info.extension_metadata,
            info.storage_type,
        )
```

This function is intentionally stateless and contains no dispatch logic.

---

## Database call-site hooks

Both modifications are strictly additive — a single new import and a single new call in
each method, no existing logic altered.

### `DeltaTableDatabase._read_delta_table`

**Schema peek:** `DeltaTable.schema().to_arrow()` — cheap metadata-only read, no Parquet
data scan.

The call is placed **immediately after** `dataset = delta_table.to_pyarrow_dataset(...)`,
before the filter-building block. Failing fast before any filter work is done if a
category metadata has no registered factory.

```python
# Immediately after: dataset = delta_table.to_pyarrow_dataset(as_large_types=True)
schema = delta_table.schema().to_arrow()
ensure_extensions_registered(schema)
# Existing filter-building and table materialisation continue unchanged
```

Logging (in `delta_lake_databases.py`):
* `DEBUG`: `"_read_delta_table: peeking schema for extension type registration"` before the
  peek call.

### `ConnectorArrowDatabase._get_committed_table`

**Schema peek:** `batches[0].schema` — schema from the already-fetched first batch. No
additional query needed; no extra round-trip.

```python
batches = list(self._connector.iter_batches(f'SELECT * FROM "{table_name}"'))
if not batches:
    return None
ensure_extensions_registered(batches[0].schema)
return pa.Table.from_batches(batches)
```

Logging (in `connector_arrow_database.py`):
* `DEBUG`: `"_get_committed_table: peeking schema for extension type registration"` before
  the peek call.

> **Design note:** A `LIMIT 0` pre-query was considered to avoid fetching all data before
> knowing whether extension type registration is needed, but was rejected. The existing
> code already fetches all batches in a single pass; adding a second round-trip for a
> schema-only peek would increase latency for the common no-extension-types case. The
> first-batch schema approach adds zero extra queries.

---

## Per-process cache design

The per-process cache is `LogicalTypeRegistry._by_arrow_name`. The first call to
`prepare_extension_type` for a given `arrow_extension_name` performs factory dispatch and
registers the `LogicalType`. Every subsequent call for the same name hits the
`get_by_arrow_extension_name` check and returns immediately.

Because `default_logical_type_registry` is a module-level singleton that lives for the
process lifetime, this provides exactly the per-process caching semantics described in
PLT-1655. No separate `set` is needed in `database_hooks.py` — the registry is the cache.

---

## Logging summary

| Location | Level | Message |
|---|---|---|
| `database_hooks.ensure_extensions_registered` | DEBUG | No extension types found in schema |
| `database_hooks.ensure_extensions_registered` | DEBUG | N extension types found, lists names |
| `registry.prepare_extension_type` | DEBUG | Already registered — skipping |
| `registry.prepare_extension_type` | DEBUG | Not registered — dispatching to category factory |
| `registry.prepare_extension_type` | DEBUG | Successfully registered via factory |
| `registry.register_logical_type_factory` | DEBUG | Factory registered for category string |
| `delta_lake_databases._read_delta_table` | DEBUG | Peeking schema for extension type registration |
| `connector_arrow_database._get_committed_table` | DEBUG | Peeking schema for extension type registration |

All messages use `%r`/`%s` lazy formatting (no f-strings in log calls).

---

## Tests

**`tests/test_extension_types/test_database_hooks.py`**

| Test | What it covers |
|---|---|
| `test_no_extension_types_is_noop` | Schema with only primitives — returns without touching registry |
| `test_known_type_is_registered` | Schema with one extension type whose factory is registered — logical type registered in PA/Polars |
| `test_already_registered_is_skipped` | Call `ensure_extensions_registered` twice — second call is no-op, no duplicate error |
| `test_unknown_metadata_raises` | Unregistered extension type with valid JSON metadata but no matching factory — raises `ValueError` with name and category in message |
| `test_metadata_not_json_raises` | Unregistered extension type with metadata bytes that are not valid JSON — raises `ValueError` with raw bytes and parse error |
| `test_metadata_json_missing_category_raises` | Unregistered extension type with valid JSON metadata but no `"category"` key — raises `ValueError` naming the extension and parsed dict |
| `test_none_metadata_not_registered_raises` | Unregistered extension type with `None` metadata — raises `ValueError` telling caller to pre-register explicitly (not via factory) |
| `test_none_metadata_already_registered_noop` | Extension type with `None` metadata that IS already in the registry — silent no-op, no error |
| `test_nested_extension_type` | Extension type inside a struct column — walker descends and hook registers it |

**`tests/test_extension_types/test_registry.py`** additions:

| Test | What it covers |
|---|---|
| `test_register_logical_type_factory` | Factory registered by category; `prepare_extension_type` dispatches to it and registers result |
| `test_factory_receives_full_metadata_dict` | Factory `create_logical_type` is called with the full parsed JSON dict, not just the category |
| `test_prepare_already_registered_noop` | `prepare_extension_type` called twice — second call is no-op |
| `test_prepare_already_registered_none_metadata_noop` | Type pre-registered; `None` metadata on subsequent call → no-op, no error |
| `test_prepare_none_metadata_not_registered_raises` | `None` metadata, type not in registry — `ValueError` telling caller to pre-register directly |
| `test_prepare_invalid_json_raises` | `extension_metadata` is not valid UTF-8 JSON — `ValueError` with raw bytes and parse error |
| `test_prepare_json_missing_category_raises` | Valid JSON but no `"category"` key — `ValueError` naming the extension and parsed dict |
| `test_prepare_unknown_category_raises` | Valid JSON with `"category"` but no matching factory — `ValueError` with category and registration hint |
| `test_register_duplicate_category_raises` | `register_logical_type_factory` with same category twice raises `ValueError` |

---

## Dependencies

* PLT-1653 (`ExtensionTypeRegistry` → `LogicalTypeRegistry`) — **merged**
* PLT-1654 (`schema_walker`) — **merged**
* PLT-1668 (`LogicalType` / `LogicalTypeRegistry` redesign) — **merged** (unblocked)
