# Dataclass ↔ Arrow Struct Encoding Design

**Date:** 2026-06-02
**Linear issue:** ENG-555
**Status:** Approved

---

## Overview

Orcapod has no first-class way to round-trip a Python `@dataclass` through its Arrow-backed
columnar storage. This spec describes **dataclass-as-struct encoding** with a `__type` sentinel
field carrying fully-qualified class identity, plus a three-tier deserialization strategy.

---

## Goals & Success Criteria

- A `@dataclass` instance serializes to an Arrow struct with a `__type: Utf8` field plus one
  field per dataclass field.
- The `__type` value is `dataclass:<module>.<qualname>` (e.g. `dataclass:my_mod.MyClass`).
- Round-trips through Parquet, Polars, and pyarrow work without library-specific metadata.
- Reconstruction follows a three-tier fallback:
  1. **Tier 1 (import):** parse `__type`, `importlib`-import the module, retrieve the class.
  2. **Tier 2 (registry):** look up the FQCN in a process-local registry populated via
     `orcapod.register_dataclass(cls)`.
  3. **Tier 3 (synthesize):** call `dataclasses.make_dataclass()` from the struct field schema
     — never raises, always returns *something*.
- Nested dataclasses (dataclass fields whose type is itself a dataclass) round-trip correctly
  via recursive struct encoding.
- A name→class **lookup cache** (per `UniversalTypeConverter` instance) amortises repeated
  resolution within a single read operation.
- V1 encoding assumes all rows in a column use the **same dataclass type** (monomorphic write).
  Polymorphic decode (varying `__type` per row, e.g. reading legacy or multi-class data) works
  transparently because dispatch is value-driven at decode time.

---

## Architecture

### File map

| File | Change |
|---|---|
| `src/orcapod/semantic_types/dataclass_encoding.py` | **New** — all dataclass encode/decode logic |
| `src/orcapod/semantic_types/universal_converter.py` | **Modified** — thin shims (3-4 lines per method) |
| `src/orcapod/__init__.py` | **Modified** — expose `register_dataclass` |
| `tests/test_semantic_types/test_dataclass_encoding.py` | **New** — unit + integration tests |

`dataclass_encoding.py` never imports from `universal_converter.py`; the dependency flows
inward only (UTC delegates to the new module).

---

## `dataclass_encoding.py` Module

### Constants

```python
DATACLASS_TYPE_FIELD  = "__type"      # field name inside every dataclass struct
DATACLASS_TYPE_PREFIX = "dataclass:"  # value prefix; forward-compatible with pydantic:, attrs:, etc.
```

### Process-global registry

```python
_DATACLASS_REGISTRY: dict[str, type] = {}

def register_dataclass(cls: type) -> type:
    """Register a dataclass for tier-2 reconstruction by fully-qualified name.

    Returns ``cls`` unchanged so the function may be used as a class decorator.
    """
    key = f"{cls.__module__}.{cls.__qualname__}"
    _DATACLASS_REGISTRY[key] = cls
    return cls
```

The registry is process-global and module-level. There is no per-context isolation — the
intent is that `register_dataclass` is called at import time (decorator style) or once during
application setup, and the registration persists for the process lifetime.

### Encoding helpers

**`dataclass_to_arrow_struct_type(cls, converter) -> pa.StructType`**

1. Call `dataclasses.fields(cls)`.
2. Map each field's type annotation to an Arrow type via `converter.python_type_to_arrow_type`
   (recursive: a field whose annotation is itself a dataclass goes through this same path).
3. Prepend `pa.field(DATACLASS_TYPE_FIELD, pa.large_string())`.
4. Return `pa.struct([type_field, *data_fields])`.

**`dataclass_to_struct_dict(obj, field_converters) -> dict[str, Any]`**

1. Build `__type` value: `f"dataclass:{type(obj).__module__}.{type(obj).__qualname__}"`.
2. For each field in `dataclasses.fields(type(obj))`, apply the pre-built converter from
   `field_converters` to the field value.
3. Return `{DATACLASS_TYPE_FIELD: type_str, field_name: converted_value, ...}`.

Field converters are built once at converter-creation time (captured in a closure) so
per-row encoding is a plain dict lookup with no type dispatch overhead.

**`has_dataclass_type_sentinel(arrow_type) -> bool`**

Returns `True` if `arrow_type` is a struct that contains a field named `__type` with type
`pa.large_string()` or `pa.string()` (the `string` variant ensures compatibility with data
written by older Arrow versions that lacked `large_string`).

### Three-tier decoder

**`struct_dict_to_dataclass(struct_dict, field_converters, lookup_cache) -> Any`**

```
1. type_str = struct_dict.get(DATACLASS_TYPE_FIELD)
   If absent or doesn't start with DATACLASS_TYPE_PREFIX → tier 3

2. fqcn = type_str[len(DATACLASS_TYPE_PREFIX):]
   Validate against r'^[A-Za-z_]\w*(\.[A-Za-z_]\w*)+$'
   On failure → log warning → tier 3

3. Tier 1 — check lookup_cache[fqcn] first (cache hit → skip import)
            split fqcn at last '.' → module_path, class_name
            importlib.import_module(module_path) + getattr(module, class_name)
            on ImportError / AttributeError → log debug → tier 2

4. Tier 2 — lookup_cache[fqcn] = _DATACLASS_REGISTRY.get(fqcn)
            on miss → tier 3

5. Tier 3 — derive field specs from struct_dict (excluding __type field)
             map each value's type to a Python annotation via converter
             cls = dataclasses.make_dataclass(class_name, field_specs)
             if fqcn is valid (regex passed, import/registry simply failed):
               cache in lookup_cache[fqcn] to avoid re-synthesizing on subsequent rows
             (no caching when __type is absent or regex-invalid — no key to cache under)

6. Instantiate: filter out __type from struct_dict, apply field_converters,
                return cls(**data_kwargs)
```

Tier 3 always succeeds. The caller never catches exceptions from this function — any
irrecoverable error (e.g. a field value incompatible with the synthesized type) surfaces as
a normal Python exception with a clear message.

---

## `UniversalTypeConverter` Wiring

Four methods each receive one new early-exit branch. All logic stays in `dataclass_encoding.py`.

### `__init__`

```python
self._dataclass_lookup_cache: dict[str, type] = {}
```

### `_convert_python_to_arrow` (new branch before the `origin is None` error)

```python
if dataclasses.is_dataclass(python_type) and isinstance(python_type, type):
    return dataclass_to_arrow_struct_type(python_type, self)
```

### `_create_python_to_arrow_converter` (new branch before generic type dispatch)

```python
if dataclasses.is_dataclass(python_type) and isinstance(python_type, type):
    hints = typing.get_type_hints(python_type)  # resolves string annotations
    field_converters = {
        f.name: self.get_python_to_arrow_converter(hints[f.name])
        for f in dataclasses.fields(python_type)
    }
    return lambda obj: dataclass_to_struct_dict(obj, field_converters)
```

### `_convert_arrow_to_python` (new branch in the struct arm, before TypedDict fallback)

```python
if pa.types.is_struct(arrow_type) and has_dataclass_type_sentinel(arrow_type):
    return Any  # actual class resolved per-row via __type value at decode time
```

### `_create_arrow_to_python_converter` (new branch in the struct arm, before existing handler)

```python
if pa.types.is_struct(arrow_type) and has_dataclass_type_sentinel(arrow_type):
    field_converters = {
        field.name: self.get_arrow_to_python_converter(field.type)
        for field in arrow_type
        if field.name != DATACLASS_TYPE_FIELD
    }
    cache = self._dataclass_lookup_cache
    return lambda d: struct_dict_to_dataclass(d, field_converters, cache)
```

### `clear_cache`

```python
self._dataclass_lookup_cache.clear()
```

### Import added to `universal_converter.py`

```python
import dataclasses
from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    dataclass_to_arrow_struct_type,
    dataclass_to_struct_dict,
    has_dataclass_type_sentinel,
    struct_dict_to_dataclass,
)
```

---

## Public API

### `orcapod/__init__.py`

```python
from .semantic_types.dataclass_encoding import register_dataclass

__all__ = [..., "register_dataclass"]
```

### Usage

```python
import orcapod
from dataclasses import dataclass

# Option A — decorator (registers at class-definition time)
@orcapod.register_dataclass
@dataclass
class PipelineInputs:
    learning_rate: float
    batch_size: int

# Option B — explicit call (e.g. for third-party classes)
orcapod.register_dataclass(ThirdPartyDataclass)
```

---

## Data Flow Examples

### Simple encode/decode

```
# Encoding
python_type_to_arrow_type(MyClass)
  → pa.struct([("__type", large_string), ("a", int64), ("b", large_string)])

converter(MyClass(a=1, b="hi"))
  → {"__type": "dataclass:my_mod.MyClass", "a": 1, "b": "hi"}

# Decoding
struct_dict_to_dataclass({"__type": "dataclass:my_mod.MyClass", "a": 1, "b": "hi"}, ...)
  → Tier 1 import → MyClass(a=1, b="hi")
```

### Nested dataclasses

```
@dataclass Inner(y: float)
@dataclass Outer(x: int, inner: Inner)

Arrow type:
  struct<__type: string, x: int64, inner: struct<__type: string, y: float64>>

Encoded:
  {"__type": "dataclass:mod.Outer", "x": 1,
   "inner": {"__type": "dataclass:mod.Inner", "y": 3.14}}

Decoded:
  Inner reconstructed first (recursive call on "inner" field converter)
  → Outer(x=1, inner=Inner(y=3.14))
```

### Polymorphic decode

```
Row 0: {"__type": "dataclass:mod.Cat", "name": "Whiskers"}
Row 1: {"__type": "dataclass:mod.Dog", "name": "Rex", "breed": "Lab"}

Each row's converter call independently resolves its own __type value.
The struct schema for the column is whatever was present at write time;
missing fields in a given row are None.
```

---

## Error Handling

| Situation | Behaviour |
|---|---|
| `__type` absent from struct | Fall through to tier 3 (backward-compat read of legacy data) |
| `__type` present but no `dataclass:` prefix | Fall through to tier 3 |
| `__type` fails FQCN regex validation | Log `WARNING`, fall through to tier 3 |
| `ImportError` / `AttributeError` in tier 1 | Log `DEBUG`, fall through to tier 2 |
| FQCN not in tier-2 registry | Fall through to tier 3 |
| Tier 3 `make_dataclass` | Never raises; always produces a usable instance |
| Non-dataclass passed to `dataclass_to_struct_dict` | Raise `TypeError` immediately |
| `dataclass_to_arrow_struct_type` called with non-dataclass type | Raise `TypeError` immediately |

---

## Tests

File: `tests/test_semantic_types/test_dataclass_encoding.py`

| Test | What it verifies |
|---|---|
| `test_simple_round_trip` | Encode `MyClass(a=1, b="hi")` → Arrow table → decode → identical instance |
| `test_nested_round_trip` | `Outer(x=1, inner=Inner(y=3.14))` full round-trip |
| `test_tier1_import` | Class importable, not registered → tier 1 resolves it |
| `test_tier2_registry` | Class not importable, registered via `register_dataclass` → tier 2 resolves it |
| `test_tier3_synthesize` | Class neither importable nor registered → tier 3 synthesizes a valid instance with correct field values |
| `test_polymorphic_decode` | Table with two rows carrying different `__type` values → each row decodes to its own class |
| `test_lookup_cache` | Cache populated on first decode, reused on second (no import call on cache hit) |
| `test_malformed_type_field` | `__type` with invalid format → tier 3 fallback, no exception |
| `test_missing_type_field` | Struct without `__type` → tier 3 fallback, no exception |
| `test_register_as_decorator` | `@register_dataclass @dataclass class Foo` → `Foo` returned unchanged, registered |
| `test_integration_parquet` | Full `python_dicts_to_arrow_table` → write Parquet → read → `arrow_table_to_python_dicts` round-trip |
| `test_type_error_on_non_dataclass` | `dataclass_to_struct_dict(42, {})` → `TypeError` |

---

## Out of Scope (V1)

- Non-dataclass objects (Pydantic, attrs, arbitrary classes) — the `__type` prefix namespace is forward-compatible
- Polymorphic encoding (write-time widest-schema derivation from mixed-type rows)
- Migration tooling for existing data without `__type` (tier 3 handles reads transparently)
- Cross-language reconstruction
- Performance benchmarks
