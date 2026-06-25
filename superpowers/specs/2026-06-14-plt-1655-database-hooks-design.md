# PLT-1655: Peek-Schema → Register → Read Pattern with Per-Process Cache

**Date:** 2026-06-14
**Linear issue:** PLT-1655
**Status:** Implemented

> **Implementation note (2026-06-15):** During implementation the design was
> refined: rather than wiring hooks directly into `DeltaTableDatabase` and
> `ConnectorArrowDatabase`, a dedicated `ExtensionAwareDatabase` wrapper was
> introduced. Database classes remain pure storage; the wrapper applies
> `register_discovered_extensions` + `apply_extension_types` on every read.
> The table below documents the actual shipped API.

---

## Overview

Two complementary utilities in `extension_types/database_hooks.py` handle
extension type awareness at database read time:

1. **`register_discovered_extensions(registry, schema)`** — walks the schema and
   registers any unknown extension types via the registry's factory dispatch. No-op
   when `registry` is `None` or the schema contains no extension types. Repeated reads
   are cheap: already-registered types are detected and skipped inside the registry.

2. **`apply_extension_types(table, registry)`** — Arrow preserves
   `ARROW:extension:name` / `ARROW:extension:metadata` field metadata even when an
   extension type is not registered at read time (columns load as plain storage types).
   After registration, this function re-wraps those storage columns into their correct
   Arrow extension types using `pa.ExtensionArray.from_storage` per chunk — zero-copy
   and no data movement. Struct columns are handled recursively.

Callers use these through the **`ExtensionAwareDatabase`** wrapper, which applies
both steps on every read result automatically.

---

## Goals & Success Criteria

* `register_discovered_extensions(registry, schema)` in
  `extension_types/database_hooks.py` correctly discovers all extension type fields
  at any nesting depth and delegates registration to the registry.
* `apply_extension_types(table, registry)` correctly re-wraps storage columns into
  extension types per-chunk without data copies; preserves schema-level metadata;
  handles struct columns recursively; skips structs with no extension children.
* When the schema contains no extension types both calls are no-ops; existing tests
  continue to pass unchanged.
* For each extension type found in the schema, `ensure_extension_type` applies checks
  in this order:
  1. **Already registered** (by Arrow extension name) → silent no-op. This is the
     common fast path for all types after first registration.
     Metadata value is irrelevant — `None` metadata on an already-registered type
     never causes an error.
  2. **Not registered, non-`None` metadata, matching factory** → factory constructs a
     `LogicalTypeProtocol` and it is registered in PyArrow, Polars, and the registry
     before the table is returned.
  3. **Not registered, non-`None` metadata, no matching factory** → clear `ValueError`
     naming the extension name and metadata tag, with a pointer to
     `register_logical_type_factory`.
  4. **Not registered, `None` metadata** → clear `ValueError` explaining that types
     without a category tag cannot be auto-registered via a factory and must be
     pre-registered explicitly via `registry.register_logical_type(logical_type)`.
* `ExtensionAwareDatabase` correctly wraps any `ArrowDatabaseProtocol` backend,
  applies both steps on every read, and passes writes through unchanged.
* Sufficient `DEBUG`-level logging throughout so that extension type discovery,
  registration decisions, and factory dispatch are observable without code changes.

---

## Scope & Boundaries

In scope:
* New `src/orcapod/extension_types/database_hooks.py`
  — `register_discovered_extensions` and `apply_extension_types`
* New `src/orcapod/databases/extension_aware_database.py` — `ExtensionAwareDatabase`
* New `LogicalTypeFactoryProtocol` Protocol in
  `src/orcapod/extension_types/protocols.py`
* New methods on `LogicalTypeRegistry` (`registry.py`):
  `register_logical_type_factory` and `ensure_extension_type`
* Additive exports in `src/orcapod/extension_types/__init__.py`
* Tests for all new code

Out of scope (database classes are pure storage, unchanged):
* `src/orcapod/databases/delta_lake_databases.py` — no extension type hooks
* `src/orcapod/databases/connector_arrow_database.py` — no extension type hooks
* Implementing concrete `LogicalTypeFactoryProtocol` instances (PLT-1657
  `dataclass_handler`, PLT-1658 `picklable_handler`)
* Built-in logical type registrations (PLT-1656)
* Thread safety of the global registry dicts (deferred)
* Any change to `semantic_types/` (old system, untouched until PLT-1660)

---

## Architecture

### File map

| File | Change |
|---|---|
| `src/orcapod/extension_types/protocols.py` | Add `LogicalTypeFactoryProtocol` Protocol |
| `src/orcapod/extension_types/registry.py` | Add `register_logical_type_factory`, `ensure_extension_type` |
| `src/orcapod/extension_types/database_hooks.py` | **New** — `register_discovered_extensions`, `apply_extension_types` |
| `src/orcapod/extension_types/__init__.py` | Additive exports |
| `src/orcapod/databases/extension_aware_database.py` | **New** — `ExtensionAwareDatabase` wrapper |
| `tests/test_extension_types/test_database_hooks.py` | **New** |
| `tests/test_databases/test_extension_aware_database.py` | **New** |

---

## `LogicalTypeFactoryProtocol` Protocol

**Location:** `src/orcapod/extension_types/protocols.py`

`LogicalTypeFactoryProtocol` is a pure factory. Given an Arrow extension name, its
storage type, and the full parsed metadata dict, it constructs a fully-formed
`LogicalTypeProtocol` instance ready to pass to
`LogicalTypeRegistry.register_logical_type()`.

The `category` string that routes to this factory is declared by the caller at
registration time — the factory itself has no knowledge of its dispatch key, but
receives the full metadata dict so it can read additional hints (e.g. version,
serialisation format) beyond just the category.

### Metadata format

`extension_metadata` bytes are expected to be **UTF-8-encoded JSON** with at least a
`"category"` key:

```json
{"category": "Dataclass"}
{"category": "Pickle", "protocol": 5}
{"category": "Pydantic", "pydantic_version": 2}
```

The `category` value is the factory dispatch key. All other fields are passed through
to the factory as-is and interpreted by the factory implementation.

### Protocol definition

```python
class LogicalTypeFactoryProtocol(Protocol):
    def create_logical_type(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict,
    ) -> LogicalTypeProtocol:
        """Construct a ``LogicalTypeProtocol`` for the given Arrow extension name.

        Args:
            arrow_extension_name: The Arrow extension type name extracted from the
                schema (i.e. the value of ``ARROW:extension:name`` field metadata).
            storage_type: The underlying Arrow storage type for this extension field.
            metadata: The full parsed JSON metadata dict. Always contains at least a
                ``"category"`` key. May contain additional keys the factory uses.

        Returns:
            A fully constructed ``LogicalTypeProtocol`` ready to be passed to
            ``LogicalTypeRegistry.register_logical_type()``.

        Raises:
            ValueError: If this factory cannot construct a logical type for the given
                extension name.
        """
        ...
```

This protocol is `@runtime_checkable`, consistent with `LogicalTypeProtocol`.

---

## `LogicalTypeRegistry` additions

**Location:** `src/orcapod/extension_types/registry.py`

Two new methods are added to `LogicalTypeRegistry`. The existing public API is
unchanged.

### `register_logical_type_factory`

```python
def register_logical_type_factory(
    self,
    category: str,
    factory: LogicalTypeFactoryProtocol,
) -> None:
    """Register a factory for the given metadata category string.

    When ``ensure_extension_type`` encounters an Arrow extension type whose
    ``extension_metadata`` JSON contains ``{"category": "<category>", ...}``,
    it calls ``factory.create_logical_type(arrow_extension_name, storage_type,
    metadata_dict)`` to construct the logical type and then registers it.

    Args:
        category: The ``"category"`` value from the extension metadata JSON.
        factory: A ``LogicalTypeFactoryProtocol`` instance responsible for
            constructing logical types for this category.

    Raises:
        ValueError: If ``category`` is already registered to a different factory.
    """
```

### `ensure_extension_type`

```python
def ensure_extension_type(
    self,
    arrow_extension_name: str,
    extension_metadata: bytes | None,
    storage_type: pa.DataType,
) -> None:
    """Ensure the Arrow extension type identified by ``arrow_extension_name``
    is registered as a ``LogicalTypeProtocol``.

    This is the single entry point called by ``register_discovered_extensions``
    in ``database_hooks``. The registry owns all dispatch logic:

    1. Already registered → return immediately (per-process cache hit).
    2. ``extension_metadata`` is ``None`` → ``ValueError``.
    3. Decode metadata as UTF-8 JSON → ``ValueError`` on failure.
    4. Extract ``"category"`` key → ``ValueError`` if absent.
    5. Look up factory by category → ``ValueError`` if not found.
    6. Call factory.create_logical_type(...) → ``LogicalTypeProtocol``.
    7. Call self.register_logical_type(logical_type).
    """
```

Error messages direct callers to use `registry.register_logical_type(logical_type)` or
`registry.register_logical_type_factory(category, factory)` on the registry instance
used for reads — no references to any module-level singleton.

---

## `database_hooks.py`

**Location:** `src/orcapod/extension_types/database_hooks.py`

### `register_discovered_extensions`

```python
def register_discovered_extensions(
    registry: LogicalTypeRegistry | None,
    schema: pa.Schema,
) -> None:
    """Register any extension types found in ``schema`` that are not yet known.

    Walks ``schema`` recursively; for each discovered type calls
    ``registry.ensure_extension_type``.  No-op when ``registry`` is ``None``
    or the schema has no extension types.
    """
```

This function is intentionally stateless and contains no dispatch logic.

### `apply_extension_types`

```python
def apply_extension_types(
    table: pa.Table,
    registry: LogicalTypeRegistry,
) -> pa.Table:
    """Re-wrap *table* columns into their registered Arrow extension types.

    Arrow preserves ``ARROW:extension:name`` / ``ARROW:extension:metadata``
    field metadata even when an extension type was not registered at read time.
    Once registered, this function reconstructs extension-typed columns from
    storage using ``pa.ExtensionArray.from_storage`` per chunk (zero-copy).
    Struct columns are handled recursively; structs with no extension children
    are skipped entirely.

    Returns the original table unchanged when no columns need re-wrapping.
    Schema-level metadata is preserved on the rebuilt table.
    """
```

---

## `ExtensionAwareDatabase` wrapper

**Location:** `src/orcapod/databases/extension_aware_database.py`

```python
class ExtensionAwareDatabase:
    """ArrowDatabaseProtocol wrapper that auto-registers and applies extension types.

    Takes any ArrowDatabaseProtocol backend and a LogicalTypeRegistry. Every
    read result flows through:
      1. register_discovered_extensions(registry, table.schema)
      2. apply_extension_types(table, registry)

    Write methods and structural methods (at, flush, base_path) delegate
    directly to the wrapped database without modification.
    """

    def __init__(self, db: ArrowDatabaseProtocol, registry: LogicalTypeRegistry) -> None: ...
    def at(self, *path_components: str) -> ExtensionAwareDatabase: ...
    # All ArrowDatabaseProtocol read/write methods delegated
```

Database classes (`DeltaTableDatabase`, `ConnectorArrowDatabase`) remain pure
storage with no extension type awareness. Callers that need extension type handling
wrap their database explicitly:

```python
db = DeltaTableDatabase("/path/to/store")
ext_db = ExtensionAwareDatabase(db, registry=data_context.logical_type_registry)
table = ext_db.get_all_records(("results", "my_fn"))
# table columns have proper extension types applied
```

---

## Per-process cache design

The per-process cache is `LogicalTypeRegistry._by_arrow_name`. The first call to
`ensure_extension_type` for a given `arrow_extension_name` performs factory dispatch
and registers the `LogicalTypeProtocol`. Every subsequent call for the same name hits
the `get_by_arrow_extension_name` check and returns immediately.

Because the registry instance lives for the process lifetime (typically as
`data_context.logical_type_registry`), this provides exactly the per-process caching
semantics described in PLT-1655. No separate `set` is needed in `database_hooks.py`
— the registry is the cache.

---

## Logging summary

| Location | Level | Message |
|---|---|---|
| `database_hooks.register_discovered_extensions` | DEBUG | No extension types found in schema |
| `database_hooks.register_discovered_extensions` | DEBUG | N extension types found, lists names |
| `database_hooks.apply_extension_types` | DEBUG | Wrapped column X as extension type Y |
| `registry.ensure_extension_type` | DEBUG | Already registered — skipping |
| `registry.ensure_extension_type` | DEBUG | Not registered — dispatching to category factory |
| `registry.ensure_extension_type` | DEBUG | Successfully registered via factory for category |
| `registry.register_logical_type_factory` | DEBUG | Factory registered for category string |

All messages use `%r`/`%s` lazy formatting (no f-strings in log calls).

---

## Tests

**`tests/test_extension_types/test_database_hooks.py`**

| Test | What it covers |
|---|---|
| `test_no_extension_types_is_noop` | Schema with only primitives — `register_discovered_extensions` returns without touching registry |
| `test_known_type_is_registered` | Schema with one extension type whose factory is registered — logical type registered |
| `test_already_registered_is_skipped` | Call `register_discovered_extensions` twice — second call is no-op |
| `test_unknown_metadata_raises` | Unregistered extension type with valid JSON metadata but no matching factory — `ValueError` |
| `test_metadata_not_json_raises` | Unregistered type with non-JSON metadata — `ValueError` with raw bytes |
| `test_metadata_json_missing_category_raises` | Valid JSON but no `"category"` key — `ValueError` |
| `test_none_metadata_not_registered_raises` | `None` metadata on unregistered type — `ValueError` |
| `test_none_metadata_already_registered_noop` | `None` metadata on already-registered type — silent no-op |
| `test_nested_extension_type` | Extension type inside a struct column — walker descends and registers it |
| `test_noop_when_no_extension_metadata` | `apply_extension_types`: plain-types table returned as-is (same object) |
| `test_wraps_storage_column_into_extension_type` | `apply_extension_types`: storage column with metadata re-wrapped |
| `test_zero_copy_single_chunk` | `apply_extension_types`: from_storage shares the underlying buffer |
| `test_zero_copy_multiple_chunks` | `apply_extension_types`: multi-chunk columns wrapped per-chunk |
| `test_already_extension_type_passthrough` | Column already extension-typed returned as-is |
| `test_unregistered_extension_metadata_left_as_storage` | Unregistered ext metadata column stays as storage type |
| `test_nested_struct_extension_type` | Extension type inside struct child field reconstructed recursively |
| `test_mixed_columns_only_ext_columns_changed` | Plain columns untouched when an extension column is processed |

**`tests/test_databases/test_extension_aware_database.py`**

| Test | What it covers |
|---|---|
| `test_get_all_records_applies_extension_types` | Wrapper applies extension types on `get_all_records` |
| `test_get_record_by_id_applies_extension_types` | Wrapper applies extension types on `get_record_by_id` |
| `test_get_records_by_ids_applies_extension_types` | Wrapper applies extension types on `get_records_by_ids` |
| `test_get_all_records_returns_none_when_no_records` | Returns `None` when inner DB has no records |
| `test_write_methods_passthrough` | `add_record` / `add_records` write correctly through wrapper |
| `test_at_returns_extension_aware_database` | `at()` returns `ExtensionAwareDatabase` with same registry |
| `test_base_path_delegates_to_inner` | `base_path` reflects inner database's `base_path` |
| `test_plain_table_passthrough_unchanged` | Tables with no extension metadata returned as-is |

---

## Dependencies

* PLT-1653 (`ExtensionTypeRegistry` → `LogicalTypeRegistry`) — **merged**
* PLT-1654 (`schema_walker`) — **merged**
* PLT-1668 (`LogicalTypeProtocol` / `LogicalTypeRegistry` redesign) — **merged** (unblocked)
