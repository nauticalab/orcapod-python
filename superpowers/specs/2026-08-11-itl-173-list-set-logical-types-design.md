# ITL-173: List and Set Logical Types for Nested Extension-Type Preservation — Design Spec

**Date:** 2026-08-11
**Linear issue:** ITL-173
**Branch:** `eywalker/itl-173-add-list-and-struct-extension-types-for-nested-logical-type`
**PR target:** `main`

---

## Overview

When a logical type (e.g. `uuid.UUID`, a dataclass, `numpy.ndarray`) appears as the element
of a `list[T]` or `set[T]` annotation, Arrow/Polars cannot preserve the extension-type
information inside the list value field. This is the **ET2 problem**: `register_python_class`
currently raises `ValueError` for any `list[T]` where `T` maps to a `pa.ExtensionType`.

The fix wraps the **entire list** as a single top-level Arrow extension type (e.g.
`list[orcapod.uuid]` with storage `large_list(large_binary)`). Extension-type metadata sits
at the outermost field level, which Arrow/Parquet/Delta always preserve, and the metadata
encodes everything needed to reconstruct the element type recursively.

---

## Problem Statement

### ET1 (background — not fixed here)

Arrow forbids extension types nested inside struct fields or list value positions at the
array-construction level:

```python
pa.array([], type=pa.large_list(uuid_ext_type))  # ArrowNotImplementedError
```

This is a C++ Arrow limitation. Solving ET1 requires compile-time changes to Arrow itself and
is explicitly out of scope.

### ET2 (this spec)

Because of ET1, the write path in `UniversalTypeConverter.register_python_class` for
`list[T]` where `T` resolves to a `pa.ExtensionType` currently raises:

```python
if isinstance(inner, pa.ExtensionType):
    raise ValueError(
        f"'list[{args[0]}]' is not yet supported: the element type maps to Arrow "
        f"extension type {inner.extension_name!r} ..."
    )
```

The read path (`_convert_arrow_to_python`) can only recover element type from
`large_list.value_type`, which after a Parquet round-trip with a fresh converter is a plain
storage type (all extension metadata stripped). So even if ET1 were worked around for
construction, schema metadata would be lost.

---

## Design

### Core invariant: extension types only at the outermost field level

Arrow preserves `ARROW:extension:name` + `ARROW:extension:metadata` only at the **field
level** of a schema (not inside list value types or struct field types). All orcapod logical
types therefore follow the **storage-safe invariant**:

> Extension types appear only at the outermost Arrow field type. All nested types (list value
> type, struct fields) are plain Arrow storage types.

The `DataclassLogicalType` already does this: it strips inner extension types from struct
fields and stores the Python FQCN in metadata. The `ListLogicalType` and `SetLogicalType`
introduced here follow the same pattern.

### Arrow extension name scheme

| Python annotation  | Arrow extension name  | Storage type         |
|--------------------|-----------------------|----------------------|
| `list[uuid.UUID]`  | `list[orcapod.uuid]`  | `large_list(large_binary)` |
| `set[uuid.UUID]`   | `set[orcapod.uuid]`   | `large_list(large_binary)` |
| `list[list[uuid.UUID]]` | `list[list[orcapod.uuid]]` | `large_list(large_list(large_binary))` |
| `list[MyDataclass]` | `list[orcapod.dataclass.mymodule.MyDataclass]` | `large_list(pa.struct(...))` |

The extension name encodes the **full nesting tree** as a string, making it:
1. Globally unique — two different element types produce different names
2. Human-readable in schema dumps
3. Stable — constructed deterministically from the element extension name

Formation rule:
```
list_ext_name(element) = f"list[{element_ext_name}]"
set_ext_name(element)  = f"set[{element_ext_name}]"
```

Where `element_ext_name` is either:
- The extension name of the element's `LogicalTypeProtocol` (if the element is already a
  registered logical type), or
- A plain Arrow type name string for primitive types — but this case never arises here since
  we only create a `ListLogicalType` when the element itself resolves to an extension type.

### Metadata schema

Each `ListLogicalType` / `SetLogicalType` extension stores JSON metadata at the field level:

```json
{
  "category": "list",
  "element_ext_name": "orcapod.uuid",
  "element_ext_metadata": "<base64 or raw bytes of element extension's own metadata>"
}
```

For `set[uuid.UUID]`:
```json
{
  "category": "set",
  "element_ext_name": "orcapod.uuid",
  "element_ext_metadata": "<...>"
}
```

For `list[list[uuid.UUID]]` the outer metadata is:
```json
{
  "category": "list",
  "element_ext_name": "list[orcapod.uuid]",
  "element_ext_metadata": "{\"category\": \"list\", \"element_ext_name\": \"orcapod.uuid\", \"element_ext_metadata\": \"...\"}"
}
```

`element_ext_metadata` is the raw UTF-8 metadata bytes of the element's extension type,
stored as a JSON string (embedded JSON-in-JSON). If the element has no metadata (e.g.
primitives) the value is `null`; for all orcapod-managed logical types it is always present.

The `element_ext_name` is stored explicitly so the read path knows which extension to
reconstruct without having to infer it from the storage type.

### Write path

`list[uuid.UUID]` is a `types.GenericAlias`, **not a `type`**. `isinstance(list[uuid.UUID], type)` is `False`. This matters in two places.

#### Two entry points — same logic

The `list[T]`→`ListLogicalType` creation runs identically in two methods:

**`_register_python_class_impl`** — triggered by `ensure_types_registered_for_schemas` before
schema conversion. Registers as a side effect and returns the Arrow type.

**`_convert_python_to_arrow`** — triggered by `python_type_to_arrow_type` on a schema lookup.
Must also create and register the `ListLogicalType` if it was not yet registered, because
callers of `python_type_to_arrow_type` (e.g. `python_schema_to_arrow_schema`) may run
without a prior `register_python_class` call.

#### `_convert_python_to_arrow`: remove `isinstance(python_type, type)` guard

The existing early registry check gates on `isinstance(python_type, type)`, which excludes
generic aliases:

```python
# BEFORE (misses list[uuid.UUID])
if self._logical_type_registry is not None and isinstance(python_type, type):
    lt = self._logical_type_registry.get_by_python_type(python_type)
    ...
```

Change to:

```python
# AFTER (hits for list[uuid.UUID] via direct dict lookup)
if self._logical_type_registry is not None:
    lt = self._logical_type_registry.get_by_python_type(python_type)
    if lt is not None:
        return lt.get_arrow_extension_type()
```

`get_by_python_type` does a plain `dict.get(python_type)` first — `list[uuid.UUID]` is a
hashable `GenericAlias` and works as a dict key. The `issubclass` fallback raises `TypeError`
for non-types and is already caught with `continue`. Removing the outer `isinstance` guard is
safe.

The same guard change must be applied in `_create_python_to_arrow_converter`.

#### Shared creation logic (both entry points)

```
annotation = list[uuid.UUID]                      # GenericAlias, not a type
origin = get_origin(annotation)                   # = list
args = get_args(annotation)                       # = (uuid.UUID,)
inner = register_python_class(args[0])            # = uuid_ext_type (pa.ExtensionType)

# inner IS an extension type → wrap as ListLogicalType
list_ext_name = f"list[{inner.extension_name}]"   # = "list[orcapod.uuid]"

# Idempotency: use extension-name lookup, not python_type lookup
# (ListLogicalType may not yet be in _by_python_type on first call)
lt = registry.get_by_arrow_extension_name(list_ext_name)
if lt is None:
    element_python_type = self.arrow_type_to_python_type(inner)  # = uuid.UUID
    lt = ListLogicalType(element_python_type, inner, is_set=False)
    registry.register_logical_type(lt)
    # After this, _by_python_type[list[uuid.UUID]] = lt
    # so subsequent _convert_python_to_arrow calls hit the early registry check

return lt.get_arrow_extension_type()
```

For a plain `list[int]` (element is not an extension type) the current behaviour is
unchanged: returns `pa.large_list(pa.int64())` directly.

### Read path

`ListLogicalTypeFactory.reconstruct_from_arrow(arrow_ext_name, storage_type, metadata_dict,
converter)`:

1. Extract `element_ext_name` and `element_ext_metadata_str` from `metadata_dict`.
2. Derive `element_storage_type = storage_type.value_type` (the inner storage).
3. Recursively call `converter.register_arrow_extension(element_ext_name,
   element_ext_metadata_bytes, element_storage_type)`.
   - This registers the element's logical type and returns its `pa.ExtensionType`.
4. Look up the element logical type by `element_ext_name` to recover `element_python_type`.
5. Construct the generic alias: `python_type = list[element_python_type]` (or `set[…]`).
6. Instantiate and return `ListLogicalType(element_python_type, element_ext_type)`.

The converter's `register_arrow_extension` already handles the registry-hit case
(idempotent), so re-reading the same schema twice is safe.

### `python_type` round-trip guarantee

`_convert_arrow_to_python` uses `lt.python_type` verbatim for extension types:

```python
if isinstance(arrow_type, pa.ExtensionType) ...:
    lt = registry.get_by_arrow_extension_name(arrow_type.extension_name)
    if lt is not None:
        return lt.python_type   # returned directly
```

Therefore `ListLogicalType.python_type` **must** return the full generic alias
`list[uuid.UUID]`, not bare `list`. If it returned `list`, `arrow_schema_to_python_schema`
would lose element-type information, and any downstream call to `python_schema_to_arrow_schema`
with the recovered schema would raise `ValueError: unparameterized list`.

The generic alias is constructed as `list[element_python_type]` in Python 3.9+ using the
built-in `__class_getitem__` syntax. This produces a `types.GenericAlias`, which is hashable
and usable as a cache key in `python_type_to_arrow_type`.

For nested types the chain is fully recursive:
```
list[list[UUID]] → outer ListLogicalType.python_type = list[list[UUID]]
                              ↑ built from inner ListLogicalType.python_type = list[UUID]
                                              ↑ built from UUIDLogicalType.python_type = UUID
```

### `set` handling

`set[T]` uses identical storage (`large_list`) but differs in:
- Extension name prefix: `set[…]` instead of `list[…]`
- Metadata `"category": "set"`
- `python_type` returns `set[element_python_type]`

In the write path (`_convert_python_to_arrow`), the `origin is set` branch is modified to
mirror the `origin is list` branch. Both branches share `ListLogicalTypeFactory` (or a
dedicated `SetLogicalTypeFactory` — see implementation notes below).

In the read path, the `"list"` and `"set"` categories are registered as two separate factory
entries (same factory class, different category).

### Value serialisation / deserialisation

The actual Python values (`list[uuid.UUID]` → Arrow array) are handled by
`get_python_to_arrow_converter` / `get_arrow_to_python_converter`. The list-level converter:

**Write** (`list[uuid.UUID]` → `large_list(large_binary)` array):
1. Get element converter: `elem_conv = get_python_to_arrow_converter(uuid.UUID)`.
2. For each list value: apply `elem_conv` to each element.
3. Build `pa.array([[elem_conv(v) for v in row] for row in column], type=storage_type)`.

**Read** (`large_list(large_binary)` array → `list[uuid.UUID]`):
1. Get element converter: `elem_conv = get_arrow_to_python_converter(element_ext_type)`.
2. For each storage row: apply `elem_conv` to each element.
3. Return `[list(map(elem_conv, row)) for row in column]` (or `set(…)` for sets).

The converter lookup happens via `python_type_to_arrow_type` (for the element annotation) or
by reading `ListLogicalType._element_ext_type` directly.

---

## Scope

### In scope

- `list[T]` where `T` resolves to an orcapod `pa.ExtensionType` (UUID, Path, UPath, File,
  Directory, NumpyArray, PandasDataFrame, PandasSeries, dataclasses, pydantic models).
- `set[T]` under the same constraints (serialised as a sorted list in Arrow; round-tripped
  back as `set`).
- Arbitrary nesting depth: `list[list[T]]`, `list[set[T]]`, etc.
- Full schema round-trip: `python_type` returns the exact generic alias.
- Parquet and Delta back-ends (same as all other logical types).

### Out of scope

- **`dict[K, V]` with extension-type keys or values** — deferred. Dict is stored as
  `large_list(struct<key, value>)` which hits ET1 (extension types inside struct fields).
- **ET1 itself** — the inner Arrow limitation that forbids extension types in list value
  positions or struct fields at array-construction time. Not fixed here.
- **Struct extension types for dataclass fields of extension-type** — the existing
  `DataclassLogicalTypeFactory` already strips inner extension types and stores FQCNs in
  metadata. That path is not changed.

---

## Files Changed

| File | Change |
|------|--------|
| `src/orcapod/extension_types/list_logical_type_factory.py` | **New** — `ListLogicalType`, `ListLogicalTypeFactory`, `LIST_CATEGORY`, `SET_CATEGORY` |
| `src/orcapod/semantic_types/universal_converter.py` | Modify `origin is list` / `origin is set` branches in `_convert_python_to_arrow`; add early `ListLogicalType` creation; add element converter wiring in `get_python_to_arrow_converter` / `get_arrow_to_python_converter` |
| `src/orcapod/extension_types/__init__.py` | Export `ListLogicalType`, `ListLogicalTypeFactory`, `LIST_CATEGORY`, `SET_CATEGORY` |
| `src/orcapod/contexts/data/v0.1.json` | Register `ListLogicalTypeFactory` twice (category `"list"` and `"set"`) under `factories` |
| `DESIGN_ISSUES.md` | Update ET2 status to `resolved` with fix note |
| `tests/test_extension_types/test_roundtrips.py` | Add `list[UUID]`, `set[UUID]`, `list[MyDataclass]`, `list[list[UUID]]` round-trip tests |

---

## Implementation Notes

### `ListLogicalType` class

```python
class ListLogicalType(BaseLogicalType):
    def __init__(
        self,
        element_python_type: type,
        element_ext_type: pa.ExtensionType,
        *,
        is_set: bool = False,
    ):
        self._element_python_type = element_python_type
        self._element_ext_type = element_ext_type
        self._is_set = is_set

    @property
    def python_type(self) -> type:
        container = set if self._is_set else list
        return container[self._element_python_type]

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        ...  # returns cached extension type wrapping large_list(element_ext_type.storage_type)

    def index_element(self) -> type:
        return self._element_python_type
```

### Factory: single class, two categories

`ListLogicalTypeFactory` is registered for both `"list"` and `"set"`. The metadata
`category` value distinguishes them. The factory's `create_for_python_type` sets
`is_set = (origin is set)`.

### v0.1.json registration

```json
{
  "factory": {
    "_class": "orcapod.extension_types.list_logical_type_factory.ListLogicalTypeFactory",
    "_config": {}
  },
  "category": "list"
},
{
  "factory": {
    "_class": "orcapod.extension_types.list_logical_type_factory.ListLogicalTypeFactory",
    "_config": {}
  },
  "category": "set"
}
```

No `python_bases` — dispatch to this factory is explicit in `_convert_python_to_arrow`
rather than through base-class matching.

### Extension type registration in PyArrow

`pa.register_extension_type` is global and keyed by extension name. Since each
`list[orcapod.uuid]` name is unique, there is no collision risk. However, registering
the same name twice (e.g. reading two different Parquet files with the same schema) is an
error in PyArrow. The registry idempotency check in `register_arrow_extension` (lines
667–669 of `universal_converter.py`) handles this: a registry hit short-circuits before the
factory call.

---

## Test Plan

1. **`list[UUID]` Parquet round-trip** — write `[uuid1, uuid2]` in a `list[UUID]` column,
   read back with a fresh converter, assert element equality and Python type is `list[UUID]`.
2. **`set[UUID]` round-trip** — write `{uuid1, uuid2}`, read back as `set`, assert equality.
3. **`list[MyDataclass]` round-trip** — `MyDataclass` has scalar fields (`name: str`,
   `value: int`). Write `[MyDataclass("a", 1), MyDataclass("b", 2)]`, read back, assert
   element equality.
4. **`list[list[UUID]]` round-trip** — two-level nesting; assert element equality and that
   `python_type == list[list[uuid.UUID]]`.
5. **Dataclass with `list[UUID]` field** — defines a dataclass with a `list[uuid.UUID]`
   field (e.g. `ids: list[uuid.UUID]`). Verifies that `DataclassLogicalTypeFactory`
   correctly calls `register_python_class(list[uuid.UUID])` for the field (currently raises
   ValueError — fixed here), and that the value round-trips correctly end-to-end.
6. **`list[int]` unchanged** — `register_python_class(list[int])` returns
   `large_list(int64)`, not a `ListLogicalType` extension. No entry in `_by_python_type`
   for `list[int]`.
7. **Schema round-trip** — `arrow_schema_to_python_schema(schema)` followed by
   `python_schema_to_arrow_schema(recovered)` returns the identical Arrow schema for
   columns of type `list[UUID]`, `set[UUID]`, and `list[list[UUID]]`.
8. **`python_type` property** — `ListLogicalType.python_type == list[uuid.UUID]` for the
   list case and `set[uuid.UUID]` for the set case.
9. **Fresh converter read** — schema registered in converter A; converter B (new instance)
   reads the same Parquet file via `load_extension_types` and round-trips successfully.
   Covers `list[UUID]` and dataclass-with-list-field cases.
