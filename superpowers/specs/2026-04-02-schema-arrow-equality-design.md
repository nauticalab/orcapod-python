# Schema ↔ Arrow Logical Equality — Design Spec

**Linear issue:** PLT-923
**Date:** 2026-04-02
**Status:** Approved

---

## Overview

`Schema.__eq__` (Python-side) must correspond to logical Arrow schema equality when
comparing two schemas for compatibility (e.g. in `process_packet` / `process` schema
validation).  This spec defines a small code addition to `Schema` and a new test file
that together prove and document this correspondence.

---

## Audit findings

### What exists today

| Aspect | Behaviour |
|---|---|
| `Schema.__eq__` | Compares `_data` (dict, order-insensitive) **and** `_optional` (frozenset) |
| `python_schema_to_arrow_schema` | Iterates fields in insertion order; ignores `optional_fields`; always sets `nullable=True` |
| Arrow schema `==` | Order-sensitive, not normalised for `Utf8`/`LargeUtf8` |
| `StarfixArrowHasher.hash_schema` | Column-order-independent; normalises `Utf8`/`LargeUtf8` and `Binary`/`LargeBinary` |

### The `optional_fields` asymmetry

`optional_fields` captures whether a Python parameter has a default value.  It is a
Python-only concept with no Arrow representation.  This creates a directional mismatch:

- `Schema(a=int) != Schema(a=int, optional_fields=["a"])` — Python-unequal
- Both produce `pa.schema([pa.field("a", int64())])` — Arrow-equal

The fix is not to paper over this with test caveats but to give `Schema` a method that
returns its **structural** (Arrow-correspondent) view by stripping optionality.

### Arrow nullability

`python_schema_to_arrow_schema` always constructs Arrow fields with the default
`nullable=True`, regardless of whether a field is in `optional_fields` or not.  There
is therefore no Arrow-level nullability distinction between required and optional fields
in this converter.  The "nullable vs non-nullable" edge case listed in PLT-923 is
addressed by testing that `int | None` (a Python union) maps to the same Arrow type
as plain `int` — both become `int64` with `nullable=True` — confirming no structural
difference arises from this type variation.

---

## Code change — `Schema.as_required()`

**File:** `src/orcapod/types.py`

Add one method to the `Schema` class:

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

This makes structural (Arrow-correspondent) equality expressible as:

```python
s1.as_required() == s2.as_required()
```

The method is idempotent: `s.as_required().as_required() == s.as_required()`.

---

## New test file — `tests/test_semantic_types/test_schema_arrow_equality.py`

### Infrastructure

Two module-level helpers power all tests:

```python
# SemanticTypeRegistry is empty because hash_schema operates on Arrow types only
# and does not consult the semantic registry (unlike hash_table).
_hasher = StarfixArrowHasher(SemanticTypeRegistry(), hasher_id="test")

def _to_arrow(schema: Schema) -> pa.Schema:
    return get_default_context().type_converter.python_schema_to_arrow_schema(schema)

def _arrow_logical_eq(s1: pa.Schema, s2: pa.Schema) -> bool:
    return _hasher.hash_schema(s1).digest == _hasher.hash_schema(s2).digest
```

`_arrow_logical_eq` uses `StarfixArrowHasher.hash_schema` which is:
- Column-order-independent (field sequence does not affect the hash)
- `Utf8`/`LargeUtf8` and `Binary`/`LargeBinary` normalised

### Test classes

#### `TestEqualSchemasHaveLogicallyEqualArrowSchemas`

The core positive claim: if two `Schema` objects are Python-equal, their Arrow
conversions are logically equal.

Cases:
- Single primitive field (`int`, `float`, `str`, `bool`, `bytes`)
- Multiple primitive fields
- Schema created with keyword-argument syntax vs mapping syntax
- Empty schema — `Schema.empty()` vs `Schema({})` (degenerate but valid; exercises
  `ArrowDigester.hash_schema` on an empty `pa.schema([])`)
- `Schema` compared against a plain `dict` (uses the `Mapping` branch of `__eq__`;
  note: `Schema.__eq__` raises `NotImplementedError` for non-`Mapping` types rather
  than returning `NotImplemented`, so test inputs must always be `Mapping` instances)

#### `TestUnequalSchemasHaveLogicallyUnequalArrowSchemas`

Negative direction: structurally different schemas (ignoring optionality) produce
logically unequal Arrow schemas.

Cases:
- Different field names (same type)
- Different field types for the same field name (e.g. `int` vs `float`)
- One schema is a strict subset of the other (missing field)

#### `TestFieldOrderingDoesNotAffectLogicalEquality`

`Schema.__eq__` uses Python dict equality which is order-insensitive.
`StarfixArrowHasher.hash_schema` is also column-order-independent.
Both properties must agree.

Cases:
- Two-field schema with reversed insertion order: Python-equal **and** Arrow-logically-equal
- Three-field schema with permuted insertion order

#### `TestNestedAndComplexTypes`

Verify the correspondence holds for non-primitive types.

Cases:
- `list[int]`, `list[str]`, `list[float]`
- `list[list[int]]` (nested list)
- `Path` (maps to Arrow struct `{path: large_string}`)
- `int | None` — maps to the same `int64` Arrow type as plain `int` (the Python union
  strips `None`; `nullable=True` is the default for all fields regardless; no Arrow
  structural difference arises)

#### `TestAsRequired`

Verify the `as_required()` method and document the `optional_fields` design
intentionality.

Cases:
1. `schema.as_required()` on a schema with optional fields returns a schema equal to
   the same schema constructed without `optional_fields`
2. `schema.as_required()` on a schema with no optional fields returns a schema equal
   to the original (no-op)
3. Idempotency: `s.as_required().as_required() == s.as_required()`
4. Two schemas that differ only in `optional_fields` are Python-unequal but
   `s1.as_required() == s2.as_required()` is `True`
5. Two schemas that differ only in `optional_fields` produce logically equal Arrow
   schemas: `_arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))` is `True`
6. The central correspondence claim (one direction):
   `s1.as_required() == s2.as_required()` **implies** `_arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))`
   for representative pairs (equal schemas, schemas differing only in optional fields).
   The reverse direction (`arrow_logical_eq` implies `as_required()` equality) is not
   separately asserted because it is already covered by the negative cases in
   `TestUnequalSchemasHaveLogicallyUnequalArrowSchemas`.

---

## Success criteria (from PLT-923)

| Criterion | Status |
|---|---|
| Tests verifying: if two `Schema` objects are equal, their Arrow conversions are also logically equal | ✓ |
| Tests covering field ordering | ✓ |
| Tests covering metadata differences | ✓ (Python schemas produce no Arrow field metadata; covered implicitly by all cases) |
| Tests covering nested types | ✓ |
| Tests covering `int \| None` union (nullable variant of a type) | ✓ (maps to same Arrow type as `int`; tested explicitly) |
| Arrow field `nullable=True/False` distinction | N/A — converter always emits `nullable=True`; no Python-Schema concept maps to `nullable=False` |
| `Schema.as_required()` method added and tested | ✓ |

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/types.py` | Add `Schema.as_required()` method |
| `tests/test_semantic_types/test_schema_arrow_equality.py` | New test file (≈ 150 lines) |
