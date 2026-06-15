# Design: `orcapod.dataclass` Category Handler (PLT-1657)

## Overview

Implement `DataclassHandlerFactory` — a `LogicalTypeFactoryProtocol` that both
dynamically constructs a `LogicalTypeProtocol` for any Python dataclass on the
**read path** (encountered during schema peek) and synthesises one from a Python
class on the **write path** (unregistered dataclass annotation in a pod).

New file: `src/orcapod/extension_types/dataclass_handler.py`

Existing code (`semantic_types/dataclass_encoding.py`) is untouched. Deletion
is deferred to PLT-1660 (hard cut).

---

## Part 1: Protocol additions — `protocols.py`

### `ResolutionContext`

A small frozen dataclass added to `protocols.py`. Passed through the entire
factory call chain so cycle detection works across factory boundaries (e.g.
dataclass `A` contains non-dataclass `X`, `X`'s factory would still propagate
the context):

```python
@dataclass(frozen=True)
class ResolutionContext:
    visited_types: frozenset[type] = field(default_factory=frozenset)
    visited_arrow_names: frozenset[str] = field(default_factory=frozenset)
```

Immutable — updates produce new instances via `dataclasses.replace(...)`.

### Updated `LogicalTypeFactoryProtocol`

Both methods gain two optional trailing parameters:

```python
def create_for_python_type(
    self,
    python_type: type,
    registry: LogicalTypeRegistry | None = None,
    context: ResolutionContext = ResolutionContext(),
) -> LogicalTypeProtocol: ...

def reconstruct_from_arrow(
    self,
    arrow_extension_name: str,
    storage_type: pa.DataType,
    metadata: dict[str, Any],
    registry: LogicalTypeRegistry | None = None,
    context: ResolutionContext = ResolutionContext(),
) -> LogicalTypeProtocol: ...
```

Both parameters default to "no registry, empty context" so simple factories
require no changes. `LogicalTypeRegistry` is imported under `TYPE_CHECKING` to
avoid a circular import.

### Registry call sites (registry.py)

`ensure_logical_type_for_python_class` and `ensure_extension_type` each:
- Accept an optional `context: ResolutionContext = ResolutionContext()`
- Forward it (updated) when invoking a factory

Three-line change per call site; no structural change to either method.

---

## Part 2: `DataclassBase` sentinel ABC

Registered as `python_bases=[DataclassBase]` for write-side dispatch. Because
Python dataclasses share no common base class, a sentinel ABC detects them
structurally:

```python
class DataclassBase(ABC):
    @classmethod
    def __subclasshook__(cls, C: type) -> bool:
        return bool(dataclasses.is_dataclass(C))
```

The registry's `issubclass` fallback scan (`ensure_logical_type_for_python_class`)
will match any dataclass automatically.

---

## Part 3: Field resolution — `_resolve_field`

Private factory method (not module-level) combining Arrow type inference and
converter construction in a single recursive pass:

```
DataclassHandlerFactory._resolve_field(
    annotation, registry, context
) -> tuple[pa.DataType, Callable, Callable]
```

Returns `(arrow_type, to_storage, from_storage)` for one field annotation.
Handles all supported annotation shapes:

| Python annotation | Arrow type | `to_storage` | `from_storage` |
|---|---|---|---|
| `int` | `pa.int64()` | identity | identity |
| `float` | `pa.float64()` | identity | identity |
| `str` | `pa.large_string()` | identity | identity |
| `bool` | `pa.bool_()` | identity | identity |
| `bytes` | `pa.large_binary()` | identity | identity |
| `list[T]` | `pa.list_(_resolve_field(T)[0])` | elementwise `to_storage` for T | elementwise `from_storage` for T |
| nested dataclass | `pa.struct([...])` via `create_for_python_type` | `nested_lt.python_to_storage` | `nested_lt.storage_to_python` |
| anything else | — | `TypeError` with a clear message naming the annotation |

For a nested dataclass annotation, `_resolve_field` calls
`self.create_for_python_type(annotation, registry, context)` to obtain the
`DataclassLogicalType`. Cycle detection happens inside `create_for_python_type`
(not here), so cycles in `list[nested]` or multi-level nesting are all caught
at the same point.

Registered logical types (e.g. `pathlib.Path`, `uuid.UUID`) as dataclass field
types are **not supported** in this PR and raise `TypeError`. A follow-up issue
will add the registry-lookup bridge.

---

## Part 4: `DataclassLogicalType`

Concrete `LogicalTypeProtocol` for a specific dataclass. Constructed once by the
factory; **holds no registry reference** — all conversion logic is baked in at
construction time via pre-built field converters.

### Fields

| Field | Type | Purpose |
|---|---|---|
| `_logical_name` | `str` | FQCN (e.g. `"my.module.Data1"`) |
| `_python_type` | `type` | The dataclass class |
| `_storage_type` | `pa.DataType` | `pa.struct([...])` |
| `_field_converters` | `list[tuple[str, Callable, Callable]]` | `(name, to_storage, from_storage)` |
| `_arrow_ext_class` | `type` | Result of `make_arrow_extension_type(...)` |
| `_arrow_ext` | `pa.ExtensionType \| None` | Cached instance |
| `_polars_ext_class` | `type` | Result of `make_polars_extension_type(...)` |
| `_polars_ext` | `pl.BaseExtension \| None` | Cached instance |

### Properties

- `logical_type_name` → FQCN string
- `python_type` → the dataclass class

### Arrow extension type

Created via `make_arrow_extension_type(fqcn, storage_type, metadata_bytes)` where:

```python
metadata_bytes = b'{"category": "orcapod.dataclass"}'
```

This constant metadata is how the read path dispatches back to this factory.

### `get_arrow_extension_type()`

Returns a cached `pa.ExtensionType` instance (same pattern as `LogicalPath`).

### `get_polars_extension_type()`

Returns a cached `pl.BaseExtension` instance via `make_polars_extension_type(fqcn, storage_type)`.

### `python_to_storage(value)`

Converts a dataclass instance to a Python dict for Arrow struct storage:

```python
{name: to_fn(getattr(value, name)) for name, to_fn, _ in self._field_converters}
```

For primitive fields: `to_fn` is identity.
For nested dataclass fields: `to_fn` is the nested `DataclassLogicalType.python_to_storage`.
For `list[T]` fields: `to_fn` maps the element converter over the list.

### `storage_to_python(storage_value)`

Reconstructs the dataclass from a Python dict (the `.as_py()` form of a struct scalar):

```python
self._python_type(**{name: from_fn(storage_value[name]) for name, _, from_fn in self._field_converters})
```

For primitive fields: `from_fn` is identity.
For nested dataclass fields: `from_fn` is the nested `DataclassLogicalType.storage_to_python`.
For `list[T]` fields: `from_fn` maps the element converter over the list.

---

## Part 5: `DataclassHandlerFactory`

Implements `LogicalTypeFactoryProtocol`. Stateless — no stored registry reference.

### Write path: `create_for_python_type`

```python
def create_for_python_type(
    self,
    python_type: type,
    registry: LogicalTypeRegistry | None = None,
    context: ResolutionContext = ResolutionContext(),
) -> DataclassLogicalType:
```

1. Verify `dataclasses.is_dataclass(python_type)` — raise `ValueError` if not.
2. Check `python_type in context.visited_types` — raise `TypeError` (cycle) if found.
3. Update context: `context = dataclasses.replace(context, visited_types=context.visited_types | {python_type})`.
4. Resolve `get_type_hints(python_type)` for field annotations.
5. For each `dataclasses.fields(python_type)` entry (where `field.init=True`):
   a. Call `self._resolve_field(annotation, registry, context)` → `(arrow_type, to_storage, from_storage)`.
   b. For nested dataclass annotations, `_resolve_field` internally calls `self.create_for_python_type(nested_cls, registry, context)` to produce the nested `DataclassLogicalType`. After the call returns, register the nested type: `registry.register_logical_type(nested_lt)` (only if `registry is not None`) so it is cached for future lookups.
6. Construct `DataclassLogicalType` with `(fqcn, python_type, struct_type, field_converters)`.
7. Return it (caller — the registry — registers it).

### Read path: `reconstruct_from_arrow`

```python
def reconstruct_from_arrow(
    self,
    arrow_extension_name: str,
    storage_type: pa.DataType,
    metadata: dict[str, Any],
    registry: LogicalTypeRegistry | None = None,
    context: ResolutionContext = ResolutionContext(),
) -> DataclassLogicalType:
```

1. Check `arrow_extension_name in context.visited_arrow_names` — raise `ValueError` (cycle) if found.
2. Update context: `context = dataclasses.replace(context, visited_arrow_names=context.visited_arrow_names | {arrow_extension_name})`.
3. Import the class by FQCN (`arrow_extension_name`):
   - Split on the last `.` to get `(module_path, class_name)`.
   - `importlib.import_module(module_path)` → `getattr(module, class_name)`.
   - On `ImportError` / `AttributeError`: raise `ValueError` with a clear message including the FQCN and the original error.
4. Verify `dataclasses.is_dataclass(imported_class)` — raise `ValueError` if not.
5. Use `storage_type` as-is from the schema (do not re-derive).
6. Build field converters from `get_type_hints(imported_class)` + `dataclasses.fields(imported_class)`:
   - Same logic as write path: call `self._resolve_field(annotation, registry, context)` per field.
   - For nested dataclass fields: `_resolve_field` calls `self.create_for_python_type(nested_cls, registry, context)`, registers it in the registry (if `registry is not None`), and uses its converters.
7. Construct and return `DataclassLogicalType`.

### FQCN derivation (write path helper)

```python
f"{python_type.__module__}.{python_type.__qualname__}"
```

Used as both `logical_type_name` and Arrow extension name.

---

## Part 6: Registration (informational)

The factory is registered against the `LogicalTypeRegistry` like this:

```python
factory = DataclassHandlerFactory()
registry.register_logical_type_factory(
    factory,
    category="orcapod.dataclass",
    python_bases=[DataclassBase],
)
```

Wiring into the default context (`v0.1.json` or context init) is **deferred** to
a dedicated follow-up issue (to be filed separately). This PR only implements the
module.

---

## Part 7: Testing strategy

Tests live in `tests/test_extension_types/test_dataclass_handler.py`.

### Protocol conformance
- `DataclassHandlerFactory()` satisfies `LogicalTypeFactoryProtocol` (isinstance check).
- A `DataclassLogicalType` instance satisfies `LogicalTypeProtocol`.

### Write path — flat dataclass
- `create_for_python_type` with `@dataclass class Flat: x: int; y: str` produces correct `logical_type_name`, `python_type`, Arrow struct layout.
- `python_to_storage` round-trips: `Flat(x=1, y="hi")` → `{"x": 1, "y": "hi"}` → `Flat(x=1, y="hi")`.
- All primitive field types (`int`, `float`, `str`, `bool`, `bytes`) round-trip correctly.

### Write path — `list[T]` fields
- `list[int]` field maps to `pa.list_(pa.int64())`.
- Round-trip: `Data(items=[1, 2, 3])` → storage → `Data(items=[1, 2, 3])`.

### Write path — nested dataclass
- `Outer(inner=Inner(x=1))` round-trips through `python_to_storage` → `storage_to_python`.
- Nested `Inner` type is registered in the registry as a side effect.

### Cycle detection (write path)
- `@dataclass class A: self_ref: A` raises `TypeError` mentioning "circular reference".
- Indirect cycle `A → B → A` raises `TypeError`.

### Read path — `reconstruct_from_arrow`
- Given a valid FQCN and struct `storage_type`, reconstructs a correct `DataclassLogicalType`.
- Bad FQCN (`"no.such.module.Foo"`) raises `ValueError` with a clear message.
- Non-dataclass FQCN raises `ValueError`.

### Unsupported field types
- `@dataclass class Bad: p: pathlib.Path` raises `TypeError` from `_resolve_field`.
- Error message names the unsupported annotation.

### Arrow array round-trip
- Build a `pa.array` from struct-converted instances; cast to extension type; extract values back; verify Python equality.

### `ResolutionContext` propagation (cross-factory cycle)
- Demonstrates that a cycle across two different factory types is detected via the shared `context`.

---

## Out of scope (this PR)

- Registered logical type field support (e.g. `pathlib.Path` fields) — follow-up issue.
- Default context wiring — follow-up issue.
- `dict[K, V]` field support — follow-up issue.
- Deletion of `semantic_types/dataclass_encoding.py` — PLT-1660.
