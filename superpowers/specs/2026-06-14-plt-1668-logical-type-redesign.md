# PLT-1668: Redesign ExtensionTypeConverter → LogicalType protocol with converter-owned extension types and three-way binding in LogicalTypeRegistry

**Date:** 2026-06-14
**Issue:** [PLT-1668](https://linear.app/enigma-metamorphic/issue/PLT-1668)
**Branch:** `eywalker/plt-1668-redesign-extensiontypeconverter-logicaltype-protocol-with`
**Target:** `extension-type-system`

---

## Problem

`ExtensionTypeConverter` and `ExtensionTypeRegistry` have a separation-of-concerns violation:
the registry dynamically synthesises `pa.ExtensionType` and `pl.BaseExtension` subclasses at
registration time, reading raw ingredient properties (`extension_name`, `extension_metadata`,
`storage_type`) directly off the converter. The converter supplies ingredients; the registry
manufactures the types. This is the wrong ownership model.

It also breaks when the Arrow extension type already exists as a pre-registered type (e.g.
PyArrow's built-in `"arrow.uuid"`) because the registry always tries to create a fresh type and
errors on the resulting `ArrowKeyError`.

---

## Solution

Introduce **`LogicalType`**: a protocol where each implementation owns and returns its Arrow and
Polars extension types directly. The registry's job shrinks to storing the binding, triggering
side-effect registrations in the PA/Polars global registries, and enforcing that no two logical
types share any member of their three-way identity triplet
`(logical_type_name, arrow_ext_name, python_type)`.

---

## Design

### `LogicalType` protocol (`extension_types/protocols.py`)

Replaces `ExtensionTypeConverter`. All six members are required.

```python
@runtime_checkable
class LogicalType(Protocol):
    @property
    def logical_type_name(self) -> str:
        """Unique orcapod identifier for this logical type.

        By convention the Python FQCN (e.g. ``"uuid.UUID"``), but any unique
        string is valid. Does NOT need to match the Arrow extension type name.
        """

    @property
    def python_type(self) -> type:
        """The Python class this logical type represents."""

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the Arrow extension type for this logical type.

        ``storage_type``, ``extension_name``, and serialised metadata are
        encapsulated inside the returned type; they are no longer top-level
        properties on ``LogicalType``.

        For custom types: create and return an instance of a new
        ``pa.ExtensionType`` subclass (e.g. via ``make_arrow_extension_type``).
        For pre-existing types: return the existing instance directly
        (e.g. ``pa.uuid()``).
        """

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return an instance of the Polars extension type for this logical type.

        The registry calls ``type(instance)`` to obtain the class passed to
        ``pl.register_extension_type``.
        """

    def python_to_storage(self, value: Any) -> Any:
        """Convert a Python value to its Arrow storage representation."""

    def storage_to_python(self, storage_value: Any) -> Any:
        """Convert an Arrow storage value back to a Python object."""
```

**Removed from protocol** (now encapsulated inside the extension type instances):
- `extension_name` → accessible via `get_arrow_extension_type().extension_name`
- `extension_metadata` → `get_arrow_extension_type().__arrow_ext_serialize__()`
- `storage_type` → `get_arrow_extension_type().storage_type`

---

### `make_arrow_extension_type` helper (`extension_types/registry.py`)

A module-level convenience factory for custom `LogicalType` implementations that need to
synthesise a new `pa.ExtensionType` subclass. Returns the **class** (not an instance), so
callers can instantiate it on demand and create parameterised variants in the future.

```python
def make_arrow_extension_type(
    extension_name: str,
    storage_type: pa.DataType,
    metadata: bytes | None = None,
) -> type[pa.ExtensionType]:
    """Synthesise and return a ``pa.ExtensionType`` subclass.

    Returns the *class*, not an instance — callers instantiate it inside their
    ``get_arrow_extension_type()`` implementation. Returning the class preserves
    the option to create multiple instances or future parameterised variants from
    the same class.

    This is a low-level building block. The full pattern for binding a Python
    type to a specific Arrow/Polars representation — the extension type factory —
    is the responsibility of each ``LogicalType`` implementation. See PLT-1656
    for the built-in implementations (``Path``, ``UPath``, ``UUID``).
    """
```

Internally uses `type()` dynamic class synthesis (the same technique previously inside
`_register_arrow_ext_type`), now surfaced as a public utility.

**Typical usage pattern:**

```python
_MyArrowExt = make_arrow_extension_type("my.Type", pa.large_string(), b"my.category")

class MyLogicalType:
    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return _MyArrowExt()
```

---

### `LogicalTypeRegistry` (`extension_types/registry.py`)

Replaces `ExtensionTypeRegistry`.

#### Storage

Three per-instance dicts — no module-level shadow dicts:

```python
_by_logical_name: dict[str, LogicalType]
_by_arrow_name:   dict[str, LogicalType]   # keyed by arrow_ext_type.extension_name
_by_python_type:  dict[type, LogicalType]
```

Uniqueness is enforced per-instance. The process-global `default_logical_type_registry`
singleton provides effective process-wide uniqueness for normal use.

#### `register(logical_type: LogicalType)` — full behaviour

1. Derive `arrow_ext_name = logical_type.get_arrow_extension_type().extension_name`
2. Derive `py_type = logical_type.python_type`
3. **Triplet conflict check** — for each of the three keys (`logical_type_name`,
   `arrow_ext_name`, `py_type`): if already bound to a *different* `LogicalType`,
   raise `ValueError` naming the conflicting key and both logical type names.
4. **Idempotent check** — if all three keys are already bound to the *same* `LogicalType`,
   return immediately (no-op).
5. Attempt `pa.register_extension_type(logical_type.get_arrow_extension_type())`.
   If `pa.lib.ArrowKeyError` is raised (type already registered — by a prior call on
   another registry instance, or by an external source such as PyArrow itself), accept
   silently and continue. Validation of the pre-existing type against the expected class
   is deferred to PLT-1669.
6. Derive `polars_ext_class = type(logical_type.get_polars_extension_type())`.
   Attempt `pl.register_extension_type(arrow_ext_name, polars_ext_class)`.
   If `ValueError` is raised (already registered), accept silently and continue.
7. Store three-way binding:
   - `_by_logical_name[logical_type_name] = logical_type`
   - `_by_arrow_name[arrow_ext_name] = logical_type`
   - `_by_python_type[py_type] = logical_type`

#### Lookup methods

| Method | Description |
|---|---|
| `get_by_logical_name(name: str) -> LogicalType \| None` | Direct dict lookup by logical type name |
| `get_by_python_type(python_type: type) -> LogicalType \| None` | Exact match first; falls back to `issubclass` scan (first registered wins) |
| `get_by_arrow_extension_name(arrow_name: str) -> LogicalType \| None` | Direct dict lookup by Arrow extension name; required for the Arrow schema read path |

#### Removed

- `_register_arrow_ext_type`, `_register_polars_ext_type` (synthesis logic moved to
  `make_arrow_extension_type` and individual `LogicalType` implementations)
- `_ARROW_REGISTRY`, `_POLARS_REGISTRY` module-level shadow dicts
- `get_converter_for_name`, `get_converter_for_python_type`
- `has_extension_name`, `has_python_type`, `list_extension_names`, `list_python_types`

---

### `extension_types/__init__.py`

```python
from .protocols import LogicalType
from .registry import LogicalTypeRegistry, make_arrow_extension_type
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

default_logical_type_registry = LogicalTypeRegistry()

__all__ = [
    "LogicalType",
    "LogicalTypeRegistry",
    "make_arrow_extension_type",
    "default_logical_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

`default_extension_type_registry` is removed with no backward-compat alias (greenfield pre-v0.1.0).

---

### `extension_types/schema_walker.py`

No logic changes. `schema_walker.py` has no imports of `ExtensionTypeConverter` or
`ExtensionTypeRegistry` — it is self-contained around `ExtensionTypeInfo`, which is
unchanged.

---

## Tests

### `tests/test_extension_types/test_protocols.py`

Replace `_StubConverter` with a `_StubLogicalType` conforming to the new protocol
(owns a `pa.ExtensionType` subclass and a `pl.BaseExtension` subclass). Three tests:

- `test_protocol_is_importable` — `LogicalType` can be imported
- `test_protocol_defines_required_members` — `isinstance(stub, LogicalType)` passes
- `test_conforming_class_satisfies_protocol` — exercises all six protocol members

### `tests/test_extension_types/test_registry.py`

**Stub rework:** `_make_stub()` produces a `LogicalType` conforming object. Each stub creates
its own `pa.ExtensionType` subclass (via `make_arrow_extension_type`) and `pl.BaseExtension`
subclass, returned from the respective getter methods. Factory gains `logical_name` parameter.

**Renamed/updated existing tests:**
- `test_register_stores_converter` → `test_register_stores_three_way_binding` (asserts all three
  lookup methods return the registered object)
- `test_register_duplicate_raises` → becomes a triplet conflict case
- Lookup tests updated for `get_by_logical_name`, `get_by_python_type`, `get_by_arrow_extension_name`
- Tests for removed methods (`has_extension_name`, `has_python_type`, `list_*`) deleted

**New tests for three-way binding and conflict detection:**

| Test | What it verifies |
|---|---|
| `test_register_idempotent_same_instance` | Registering the same `LogicalType` object twice is a no-op |
| `test_triplet_conflict_same_arrow_name_raises` | Different `logical_type_name`, same Arrow ext name → `ValueError` naming conflicting key |
| `test_triplet_conflict_same_python_type_raises` | Shared `python_type` → `ValueError` |
| `test_triplet_conflict_same_logical_name_raises` | Shared `logical_type_name` → `ValueError` |
| `test_register_preexisting_arrow_type_succeeds` | Pre-registered Arrow type (`ArrowKeyError`) → no error; three-way binding stored |
| `test_register_preexisting_polars_type_succeeds` | Pre-registered Polars type (`ValueError`) → no error; three-way binding stored |
| `test_get_by_arrow_extension_name_miss` | Returns `None` for unknown arrow name |
| `test_get_by_python_type_subclass` | `issubclass` fallback still works |

**End-to-end tests** (round-trip, Parquet) retained — stubs updated to `LogicalType` shape;
`_build_ext_array` uses `conv.get_arrow_extension_type()` directly.

**Module-level instance test:** `default_logical_type_registry` is a `LogicalTypeRegistry`,
starts empty.

---

## Out of Scope

- Built-in `LogicalType` implementations (`PathLogicalType`, `UPathLogicalType`,
  `UUIDLogicalType`) — PLT-1656
- Wiring `LogicalTypeRegistry` into `DataContext` — PLT-1660
- Validation of pre-existing Arrow type class on `ArrowKeyError` — PLT-1669
- Thread-safety of the global registry instance — deferred
