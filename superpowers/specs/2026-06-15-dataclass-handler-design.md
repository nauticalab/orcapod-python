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
    visited_types: frozenset[type] = frozenset()
    visited_arrow_names: frozenset[str] = frozenset()
```

Immutable — updates produce new instances via `dataclasses.replace(...)`.

### Updated `LogicalTypeFactoryProtocol`

Three additions:

**`supports_class`** — write-side probe used by the registry to confirm whether
a factory actually handles a given Python type before committing to it:

```python
def supports_class(self, python_type: type) -> bool: ...
```

Read-side dispatch (via category metadata) bypasses `supports_class` entirely —
the category string is fully definitive and `supports_class` is not consulted.

**`registry` and `context`** — both methods gain two optional trailing parameters:

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

All three additions default gracefully — simple factories that don't need
registry/context can ignore them, and a factory registered for a specific base
class can implement `supports_class` as a trivial `return True`.
`LogicalTypeRegistry` is imported under `TYPE_CHECKING` to avoid a circular
import.

### Registry write-side dispatch changes (`registry.py`)

`_python_class_factories` changes from `dict[type, LogicalTypeFactoryProtocol]`
to `dict[type, list[LogicalTypeFactoryProtocol]]` — multiple factories may be
registered against the same base class (e.g. both the dataclass and picklable
factories register against `object`).

During `ensure_logical_type_for_python_class`, the MRO walk is updated:
for each base class hit in the dict, iterate through its factory list in
registration order and call `factory.supports_class(python_type)`. The first
factory that returns `True` wins. Its result is cached in a
`_python_class_cache: dict[type, LogicalTypeFactoryProtocol]` for fast future
lookups. When multiple factories could match (same base, both support the class),
registration order is the tiebreaker — first registered wins.

`ensure_logical_type_for_python_class` and `ensure_extension_type` also each:
- Accept an optional `context: ResolutionContext = ResolutionContext()`
- Forward it (updated) when invoking a factory

---

## Part 2: Write-side dispatch — `supports_class` replaces `DataclassBase`

The sentinel `DataclassBase` ABC is **not needed**. Instead, `DataclassHandlerFactory`
registers against `object` (the universal base, always last in every MRO) and
implements `supports_class` as the actual gate:

```python
def supports_class(self, python_type: type) -> bool:
    return dataclasses.is_dataclass(python_type)
```

Registration call:

```python
registry.register_logical_type_factory(
    factory,
    category="orcapod.dataclass",
    python_bases=[object],
)
```

Because `object` is at the tail of every MRO, this factory is the least specific
match. Any factory registered against a more specific base class (e.g. a concrete
dataclass subclass) will win first. `supports_class` is only consulted after the
MRO walk reaches `object`, where it confirms the class is actually a dataclass.

---

## Part 3: Field resolution — `_resolve_field`

Private factory method (not module-level) combining Arrow type inference and
converter construction in a single recursive pass:

```
DataclassHandlerFactory._resolve_field(
    annotation, registry, context: ResolutionContext
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
| `list[T]` | `pa.list_(_resolve_field(T, ...)[0])` | elementwise `to_storage` for T | elementwise `from_storage` for T |
| nested dataclass | `pa.struct([...])` via `create_for_python_type` | `nested_lt.python_to_storage` | `nested_lt.storage_to_python` |
| anything else | — | `TypeError` with a clear message naming the annotation |

For a nested dataclass annotation, `_resolve_field` calls
`self.create_for_python_type(annotation, registry, context)` directly — `context`
is already the updated instance (with the outer class added to `visited_types`)
so cycle detection works naturally at any nesting depth or inside `list[nested]`.

Registered logical types (e.g. `pathlib.Path`, `uuid.UUID`) as dataclass field
types are **not supported** in this PR and raise `TypeError`. A follow-up issue
will add the registry-lookup bridge.

**Nested dataclass fields use plain sub-structs, not extension types.** For a
field `inner: Inner`, `_resolve_field` uses
`inner_lt.get_arrow_extension_type().storage_type` (the raw `pa.struct(...)`)
as the Arrow field type — not the extension type itself. This means the nested
`Inner` sub-field carries no FQCN or category metadata in the schema; only the
outermost column is self-describing. The factory converters (precomputed from
annotations) handle reconstruction of all nested fields.

This is a deliberate simplification. Supporting nested extension types inside
structs (so that every nesting level is self-describing) is tracked in a
dedicated v0.2 issue and deferred from this PR.

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

### `supports_class`

```python
def supports_class(self, python_type: type) -> bool:
    return dataclasses.is_dataclass(python_type)
```

Called by the registry during the MRO walk after hitting `object` in
`_python_class_factories`. Not called on the read path.

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
   b. For nested dataclass annotations, `_resolve_field` internally calls `self.create_for_python_type(nested_cls, registry, context)`. After the call returns, register the nested type: `registry.register_logical_type(nested_lt)` (only if `registry is not None`) so it is cached for future lookups.
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
    python_bases=[object],
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
- `supports_class` returns `True` for a `@dataclass` class and `False` for a plain class.

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
- Nested extension types inside struct sub-fields (self-describing nesting) — v0.2 issue.
- Deletion of `semantic_types/dataclass_encoding.py` — PLT-1660.
