# Schema ↔ Arrow Logical Equality — Design Spec

**Linear issue:** PLT-923
**Date:** 2026-04-02
**Status:** Approved — full nullability scope

---

## Overview

`Schema.__eq__` (Python-side) must correspond to logical Arrow schema equality when
comparing two schemas for compatibility (e.g. in `process_packet` / `process` schema
validation).

This spec covers three coordinated changes:

1. Fix `python_schema_to_arrow_schema` to set Arrow field `nullable` from the Python
   type (`T | None` → `nullable=True`, plain `T` → `nullable=False`).
2. Fix `arrow_schema_to_python_schema` to reconstruct `T | None` for nullable fields
   (completing the bidirectional round-trip).
3. Add `Schema.as_required()` and a new test file proving the Schema ↔ Arrow
   correspondence.

All work uses strict TDD (red → green → refactor).

---

## Background & audit findings

### Current state

| Aspect | Behaviour |
|---|---|
| `Schema.__eq__` | Compares `_data` (dict, order-insensitive) **and** `_optional` (frozenset) |
| `python_schema_to_arrow_schema` | Iterates fields in insertion order; ignores `optional_fields`; **always sets `nullable=True`** |
| `arrow_schema_to_python_schema` | Ignores `field.nullable`; always returns plain `T` |
| Arrow schema `==` | Order-sensitive, not normalised for `Utf8`/`LargeUtf8` |
| `StarfixArrowHasher.hash_schema` | Column-order-independent; normalises `Utf8`/`LargeUtf8` and `Binary`/`LargeBinary`; **treats `nullable` as meaningful** |

### The nullability gap

`_convert_python_to_arrow` for `T | None` already contains the comment
"nullability handled at field level" — the intent was always correct, but
`python_schema_to_arrow_schema` never set the `nullable` kwarg on the field.

Consequence: `Schema(a=int)` and `Schema(a=int | None)` are Python-unequal but
currently produce identical Arrow schemas (`nullable=True` for both).
After the fix they will correctly differ:
- `int` → `pa.field("a", int64(), nullable=False)`
- `int | None` → `pa.field("a", int64(), nullable=True)`

Since `StarfixArrowHasher.hash_schema` is nullability-sensitive these schemas will
also have different logical hashes — closing the correspondence gap.

### The `optional_fields` asymmetry

`optional_fields` marks fields that have a Python default value.  It is
Python-only metadata with no Arrow representation and is intentionally NOT
reflected in Arrow nullability: a field with type `int` and a default value is
still a non-nullable `int` at the Arrow level.

`Schema.as_required()` strips `optional_fields` to expose the structural
(Arrow-correspondent) view, making `s1.as_required() == s2.as_required()` the
correct predicate for "do these two schemas describe the same Arrow structure?"

### Arrow nullability is standard practice

`nullable=False` is the Arrow / SQL equivalent of `NOT NULL`.  The binary format
is identical either way (validity bitmap always present); `nullable` is a
schema-level constraint enforced on write.  Polars ignores it at the series level,
so Polars-facing code is unaffected.  Most packet fields in OrcaPod carry concrete
types (`int`, `str`, …) that should never be null — `nullable=False` makes this
explicit and catches accidental `None` writes early.

### None-writing in conversion paths

Both `python_dicts_to_struct_dicts` methods write `None` for fields absent from an
input record.  After the fix, a missing required field (`int`, `nullable=False`) will
correctly fail Arrow validation.  Any field that legitimately receives `None` must
be declared `T | None` in the Schema — enforcing correctness rather than hiding it.

### Impact on hash values

`StarfixArrowHasher` treats `nullable` as meaningful, so schema hashes derived
from Python schemas via `python_schema_to_arrow_schema` will change once the fix
is in.  This is acceptable: the project is pre-v0.1.0 greenfield and the old hashes
were semantically incorrect.  No hasher version bump is needed.

---

## Code changes

### 1. `Schema.as_required()` — `src/orcapod/types.py`

```python
def as_required(self) -> Schema:
    """Return a copy of this schema with all fields marked as required.

    Strips optional-field metadata so the result reflects the structural
    (Arrow-level) schema, where every field is unconditionally present.

    Returns:
        A new ``Schema`` containing the same fields and types but with
        ``optional_fields`` set to the empty frozenset.
    """
    return Schema(self._data)
```

Idempotent: `s.as_required().as_required() == s.as_required()`.

### 2. `python_schema_to_arrow_schema` — `src/orcapod/semantic_types/universal_converter.py`

Detect whether the Python type is `Optional[T]` (i.e. `T | None`) and set
`nullable` accordingly:

```python
def python_schema_to_arrow_schema(self, python_schema: SchemaLike) -> pa.Schema:
    fields = []
    for field_name, python_type in python_schema.items():
        arrow_type = self.python_type_to_arrow_type(python_type)
        nullable = _is_optional_type(python_type)
        fields.append(pa.field(field_name, arrow_type, nullable=nullable))
    return pa.schema(fields)
```

Helper (private, module-level):

```python
def _is_optional_type(python_type: DataType) -> bool:
    """Return True if python_type is T | None (Optional[T])."""
    origin = get_origin(python_type)
    if origin is Union or origin is types.UnionType:
        return type(None) in get_args(python_type)
    return False
```

### 3. `arrow_schema_to_python_schema` — `src/orcapod/semantic_types/universal_converter.py`

Inspect `field.nullable` and wrap in `Optional` when true:

```python
def arrow_schema_to_python_schema(self, arrow_schema: pa.Schema) -> Schema:
    fields = {}
    for field in arrow_schema:
        python_type = self.arrow_type_to_python_type(field.type)
        if field.nullable:
            python_type = python_type | None
        fields[field.name] = python_type
    return Schema(fields)
```

Round-trip guarantee:
- `int` → `nullable=False` → `int` ✓
- `int | None` → `nullable=True` → `int | None` ✓

---

## New test file — `tests/test_semantic_types/test_schema_arrow_equality.py`

### Infrastructure

```python
# SemanticTypeRegistry is empty: hash_schema operates on Arrow types only
# and never consults the semantic registry (unlike hash_table).
_hasher = StarfixArrowHasher(SemanticTypeRegistry(), hasher_id="test")

def _to_arrow(schema: Schema) -> pa.Schema:
    return get_default_context().type_converter.python_schema_to_arrow_schema(schema)

def _arrow_logical_eq(s1: pa.Schema, s2: pa.Schema) -> bool:
    return _hasher.hash_schema(s1).digest == _hasher.hash_schema(s2).digest
```

### Test classes

#### `TestEqualSchemasHaveLogicallyEqualArrowSchemas`
Core positive claim: Python-equal schemas → logically equal Arrow schemas.

Cases:
- Single primitive field (`int`, `float`, `str`, `bool`, `bytes`)
- Multiple primitive fields
- Keyword-argument vs mapping construction
- Empty schema (`Schema.empty()` vs `Schema({})`)
- `Schema` vs plain `dict` (exercises the `Mapping` branch of `__eq__`;
  note `Schema.__eq__` raises `NotImplementedError` for non-`Mapping` types)

#### `TestUnequalSchemasHaveLogicallyUnequalArrowSchemas`
Negative direction.

Cases:
- Different field names (same type)
- Different field types for the same name (`int` vs `float`)
- One schema is a strict subset (missing field)

#### `TestFieldOrderingDoesNotAffectLogicalEquality`
Python dict `==` is order-insensitive; `StarfixArrowHasher.hash_schema` is
column-order-independent.  Both must agree.

Cases:
- Two-field schema with reversed insertion order: Python-equal **and** Arrow-logically-equal
- Three-field schema with permuted insertion order

#### `TestNullabilityCorrespondence`
Verifies the nullability fix.

Cases:
- `int` field → Arrow `nullable=False`
- `int | None` field → Arrow `nullable=True`
- `str`, `float`, `bool`, `bytes` — all plain → `nullable=False`
- `str | None`, `float | None` — all Optional → `nullable=True`
- `Schema(a=int) != Schema(a=int | None)` and their Arrow schemas are
  logically unequal (different starfix hashes)
- `Schema(a=int)` and `Schema(a=int | None)`: Arrow schemas differ under
  `_arrow_logical_eq`

#### `TestRoundTrip`
`python_schema_to_arrow_schema` followed by `arrow_schema_to_python_schema`
must recover the original Python types.

Cases:
- `int` round-trips to `int` (not `int | None`)
- `int | None` round-trips to `int | None`
- `str`, `float`, `bool`, `bytes` round-trip cleanly
- `str | None`, `float | None` round-trip cleanly
- Multi-field schema with mixed nullable/non-nullable fields

#### `TestNestedAndComplexTypes`
Verifies correspondence holds for non-primitive types.

Cases:
- `list[int]`, `list[str]` — nullable=False (not Optional)
- `list[int] | None` — nullable=True
- `list[list[int]]` (nested list)
- `Path` → Arrow struct `{path: large_string}`, nullable=False

#### `TestAsRequired`
Verifies `as_required()` and documents the `optional_fields` design
intentionality.

Cases:
1. `schema.as_required()` on a schema with optional fields equals a schema
   with no optional fields
2. `schema.as_required()` on a schema without optional fields is unchanged (no-op)
3. Idempotency: `s.as_required().as_required() == s.as_required()`
4. Two schemas differing only in `optional_fields` are Python-unequal
5. Those same schemas produce logically equal Arrow schemas
6. `s1.as_required() == s2.as_required()` **implies**
   `_arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))` for representative pairs

---

## Updates to `tests/test_semantic_types/test_universal_converter.py`

Add to the existing file:

- `test_python_schema_to_arrow_non_nullable`: plain `int`/`str` fields → `nullable=False`
- `test_python_schema_to_arrow_optional_nullable`: `int | None`/`str | None` → `nullable=True`
- `test_arrow_schema_to_python_nullable_becomes_optional`: `nullable=True` field → `T | None`
- `test_arrow_schema_to_python_non_nullable_stays_plain`: `nullable=False` field → `T`
- `test_round_trip_preserves_optionality`: `int | None` → Arrow → `int | None`; `int` → Arrow → `int`

---

## Success criteria (from PLT-923)

| Criterion | Status |
|---|---|
| Tests: Python-equal schemas → logically equal Arrow schemas | ✓ |
| Tests: field ordering | ✓ |
| Tests: metadata differences | ✓ (implicit — no metadata produced) |
| Tests: nested types | ✓ |
| Tests: `int \| None` → nullable, `int` → non-nullable | ✓ |
| Arrow field `nullable` set correctly by `python_schema_to_arrow_schema` | ✓ |
| `arrow_schema_to_python_schema` reconstructs `T \| None` from nullable fields | ✓ |
| `Schema.as_required()` added and tested | ✓ |
| `optional_fields` asymmetry documented and tested | ✓ |

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/types.py` | Add `Schema.as_required()` |
| `src/orcapod/semantic_types/universal_converter.py` | Add `_is_optional_type()`; fix `python_schema_to_arrow_schema`; fix `arrow_schema_to_python_schema` |
| `tests/test_semantic_types/test_schema_arrow_equality.py` | New file |
| `tests/test_semantic_types/test_universal_converter.py` | Add nullability and round-trip tests |
