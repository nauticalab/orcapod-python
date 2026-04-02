# Schema ↔ Arrow Logical Equality Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `python_schema_to_arrow_schema` and `arrow_schema_to_python_schema` to correctly honour Python nullability (`T | None` → `nullable=True`, plain `T` → `nullable=False`), add `Schema.as_required()`, and create a comprehensive test file verifying Schema ↔ Arrow logical equality.

**Architecture:** Pure bug-fix + additive change. `_is_optional_type()` is a private module-level helper in `universal_converter.py`. The `python_schema_to_arrow_schema` method reads it to set `nullable` per field; `arrow_schema_to_python_schema` reads `field.nullable` to reconstruct `T | None`. `Schema.as_required()` is a one-liner that strips `optional_fields`. All changes verified with TDD (red → green → commit).

**Tech Stack:** Python 3.11+, PyArrow, `starfix-python` (`ArrowDigester`), pytest, uv (always run commands via `uv run`)

**Spec:** `superpowers/specs/2026-04-02-schema-arrow-equality-design.md`

---

## File Map

| Action | File | What changes |
|--------|------|-------------|
| Modify | `src/orcapod/types.py` | Add `Schema.as_required()` |
| Modify | `src/orcapod/semantic_types/universal_converter.py` | Add module-level `_is_optional_type()`; fix `python_schema_to_arrow_schema`; fix `arrow_schema_to_python_schema` |
| Modify | `tests/test_semantic_types/test_universal_converter.py` | Add 5 nullability / round-trip tests |
| Create | `tests/test_semantic_types/test_schema_arrow_equality.py` | New comprehensive equality test file |

---

## Background knowledge for implementers

### What is optional_fields?

`Schema` stores two things: `_data` (dict of `{name: type}`) and `_optional` (frozenset of field names that have a Python default value in the source function). Two schemas are Python-equal when both `_data` and `_optional` match.

`optional_fields` is **Python-only metadata** — it has no Arrow representation. A field typed `int` with a default is still `nullable=False` at the Arrow level. Only `T | None` → `nullable=True`.

### What is _is_optional_type?

A Python type is "optional" for the purposes of Arrow nullability if it is `T | None` (or `Optional[T]`). The check is: `get_origin` returns `Union` or `types.UnionType`, and `type(None)` appears in `get_args`.

### What is logical Arrow equality?

Arrow schema `==` is order-sensitive and does not normalise `Utf8`/`LargeUtf8`. Instead, we use `StarfixArrowHasher.hash_schema` which is column-order-independent, normalises string/binary widths, and is nullability-sensitive. Two schemas are "logically equal" when their starfix hashes are equal.

---

## Task 1: Schema.as_required()

Add a one-liner method to `Schema` that returns a copy with no optional fields.

**Files:**
- Modify: `src/orcapod/types.py` (inside `Schema` class, after `is_compatible_with`, before the `# Convenience constructors` section)
- Modify: `tests/test_semantic_types/test_universal_converter.py` (append at end of file)

- [ ] **Step 1: Write the failing tests**

Append to the end of `tests/test_semantic_types/test_universal_converter.py`:

```python
def test_schema_as_required_strips_optional_fields():
    from orcapod.types import Schema

    s = Schema({"a": int, "b": str}, optional_fields=["b"])
    result = s.as_required()
    assert result == Schema({"a": int, "b": str})
    assert result.optional_fields == frozenset()


def test_schema_as_required_idempotent():
    from orcapod.types import Schema

    s = Schema({"a": int, "b": str}, optional_fields=["a", "b"])
    once = s.as_required()
    twice = s.as_required().as_required()
    assert once == twice
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_schema_as_required_strips_optional_fields -v
```

Expected output: `FAILED` with `AttributeError: 'Schema' object has no attribute 'as_required'`

- [ ] **Step 3: Add as_required() to Schema**

In `src/orcapod/types.py`, inside the `Schema` class, insert after the `is_compatible_with` method and before the `# ==================== Convenience constructors ====================` comment:

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

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_schema_as_required_strips_optional_fields tests/test_semantic_types/test_universal_converter.py::test_schema_as_required_idempotent -v
```

Expected output: both `PASSED`

- [ ] **Step 5: Run the full test file to catch regressions**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v
```

Expected: all `PASSED`

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/types.py tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(types): add Schema.as_required() to expose structural Arrow-level view"
```

---

## Task 2: Fix python_schema_to_arrow_schema — set nullable from Python type

Currently `python_schema_to_arrow_schema` always creates fields with Arrow's default `nullable=True`. After this fix, plain types (`int`, `str`, …) produce `nullable=False` and `T | None` types produce `nullable=True`.

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic_types/test_universal_converter.py`:

```python
def test_python_schema_to_arrow_non_nullable():
    """Plain types (no | None) must produce nullable=False Arrow fields."""
    from orcapod.types import Schema

    ctx = get_default_context()
    schema = ctx.type_converter.python_schema_to_arrow_schema(
        Schema({"a": int, "b": str, "c": float, "d": bool, "e": bytes})
    )
    for name in ("a", "b", "c", "d", "e"):
        assert schema.field(name).nullable is False, (
            f"Field '{name}' should be nullable=False for a plain type"
        )


def test_python_schema_to_arrow_optional_nullable():
    """Optional types (T | None) must produce nullable=True Arrow fields."""
    from orcapod.types import Schema

    ctx = get_default_context()
    schema = ctx.type_converter.python_schema_to_arrow_schema(
        Schema({"x": int | None, "y": str | None})
    )
    assert schema.field("x").nullable is True
    assert schema.field("y").nullable is True
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_python_schema_to_arrow_non_nullable tests/test_semantic_types/test_universal_converter.py::test_python_schema_to_arrow_optional_nullable -v
```

Expected: `FAILED` — both fail because `nullable` is currently always `True`

- [ ] **Step 3: Add _is_optional_type helper**

In `src/orcapod/semantic_types/universal_converter.py`, add this function after the `_get_python_to_arrow_map` function and before the `UniversalTypeConverter` class definition:

```python
def _is_optional_type(python_type: DataType) -> bool:
    """Return True if python_type is T | None (Optional[T]).

    Args:
        python_type: A Python type annotation.

    Returns:
        True if the type has ``None`` as one of its union arms,
        False otherwise (including for plain types and complex
        non-optional unions).
    """
    origin = get_origin(python_type)
    if origin is typing.Union or origin is types.UnionType:
        return type(None) in get_args(python_type)
    return False
```

- [ ] **Step 4: Fix python_schema_to_arrow_schema**

In `src/orcapod/semantic_types/universal_converter.py`, replace the `python_schema_to_arrow_schema` method body (the method starts at line ~152). Change it from:

```python
    def python_schema_to_arrow_schema(self, python_schema: SchemaLike) -> pa.Schema:
        """
        Convert a Python schema (dict of field names to data types) to an Arrow schema.

        This uses the main conversion logic, using caches for known type conversion for
        an improved performance.
        """
        fields = []
        for field_name, python_type in python_schema.items():
            arrow_type = self.python_type_to_arrow_type(python_type)
            fields.append(pa.field(field_name, arrow_type))

        return pa.schema(fields)
```

To:

```python
    def python_schema_to_arrow_schema(self, python_schema: SchemaLike) -> pa.Schema:
        """
        Convert a Python schema (dict of field names to data types) to an Arrow schema.

        Field nullability is derived from the Python type: ``T | None``
        (Optional[T]) maps to ``nullable=True``; plain ``T`` maps to
        ``nullable=False``.  This uses caches for type conversion.
        """
        fields = []
        for field_name, python_type in python_schema.items():
            arrow_type = self.python_type_to_arrow_type(python_type)
            nullable = _is_optional_type(python_type)
            fields.append(pa.field(field_name, arrow_type, nullable=nullable))
        return pa.schema(fields)
```

- [ ] **Step 5: Run failing tests to verify they now pass**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_python_schema_to_arrow_non_nullable tests/test_semantic_types/test_universal_converter.py::test_python_schema_to_arrow_optional_nullable -v
```

Expected: both `PASSED`

- [ ] **Step 6: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all pass. If any failure appears, read the error message. The most likely cause is a test that previously assumed `nullable=True` for plain types and now gets `nullable=False`. Fix the test to reflect the correct semantic (plain `T` → non-nullable).

> **Note on hash changes:** `StarfixArrowHasher` treats `nullable` as meaningful. Tests with golden hash values that were computed before this fix (i.e., with `nullable=True` on plain types) will produce different digests after the fix. Update those golden values to the new (semantically correct) digests.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "fix(converter): derive Arrow field nullable from Python type in python_schema_to_arrow_schema"
```

---

## Task 3: Fix arrow_schema_to_python_schema — reconstruct T | None from nullable fields

Currently `arrow_schema_to_python_schema` ignores `field.nullable` and always returns plain `T`. After this fix, `nullable=True` → `T | None` and `nullable=False` → `T`, completing the round-trip.

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_semantic_types/test_universal_converter.py`:

```python
def test_arrow_schema_to_python_nullable_becomes_optional():
    """nullable=True Arrow fields must reconstruct as T | None."""
    ctx = get_default_context()
    arrow_schema = pa.schema([pa.field("x", pa.int64(), nullable=True)])
    python_schema = ctx.type_converter.arrow_schema_to_python_schema(arrow_schema)
    assert python_schema["x"] == int | None


def test_arrow_schema_to_python_non_nullable_stays_plain():
    """nullable=False Arrow fields must reconstruct as plain T."""
    ctx = get_default_context()
    arrow_schema = pa.schema([pa.field("x", pa.int64(), nullable=False)])
    python_schema = ctx.type_converter.arrow_schema_to_python_schema(arrow_schema)
    assert python_schema["x"] == int


def test_round_trip_preserves_optionality():
    """Python schema → Arrow → Python schema is lossless for nullable/non-nullable."""
    from orcapod.types import Schema

    ctx = get_default_context()
    original = Schema({"required": int, "nullable_field": int | None})
    arrow = ctx.type_converter.python_schema_to_arrow_schema(original)
    recovered = ctx.type_converter.arrow_schema_to_python_schema(arrow)

    assert recovered["required"] == int
    assert recovered["nullable_field"] == int | None
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_arrow_schema_to_python_nullable_becomes_optional tests/test_semantic_types/test_universal_converter.py::test_arrow_schema_to_python_non_nullable_stays_plain tests/test_semantic_types/test_universal_converter.py::test_round_trip_preserves_optionality -v
```

Expected: `FAILED` — `arrow_schema_to_python_schema` currently ignores `field.nullable`

- [ ] **Step 3: Fix arrow_schema_to_python_schema**

In `src/orcapod/semantic_types/universal_converter.py`, replace the `arrow_schema_to_python_schema` method body. Change it from:

```python
    def arrow_schema_to_python_schema(self, arrow_schema: pa.Schema) -> Schema:
        """
        Convert an Arrow schema to a Python Schema (mapping of field names to types).

        This uses the main conversion logic, using caches for known type conversion for
        an improved performance.
        """
        fields = {}
        for field in arrow_schema:
            fields[field.name] = self.arrow_type_to_python_type(field.type)

        return Schema(fields)
```

To:

```python
    def arrow_schema_to_python_schema(self, arrow_schema: pa.Schema) -> Schema:
        """
        Convert an Arrow schema to a Python Schema (mapping of field names to types).

        ``nullable=True`` fields are reconstructed as ``T | None``; ``nullable=False``
        fields are reconstructed as plain ``T``, completing the bidirectional
        round-trip with ``python_schema_to_arrow_schema``.

        Round-trip guarantee:
            - ``int``       → ``nullable=False`` → ``int``
            - ``int | None`` → ``nullable=True``  → ``int | None``
        """
        fields = {}
        for field in arrow_schema:
            python_type = self.arrow_type_to_python_type(field.type)
            if field.nullable:
                python_type = python_type | None
            fields[field.name] = python_type
        return Schema(fields)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_arrow_schema_to_python_nullable_becomes_optional tests/test_semantic_types/test_universal_converter.py::test_arrow_schema_to_python_non_nullable_stays_plain tests/test_semantic_types/test_universal_converter.py::test_round_trip_preserves_optionality -v
```

Expected: all `PASSED`

- [ ] **Step 5: Run the full test suite for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all pass. If tests that call `arrow_schema_to_python_schema` and check for plain types on nullable fields fail, update them: a `nullable=True` Arrow field now correctly returns `T | None`.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "fix(converter): reconstruct T | None from nullable Arrow fields in arrow_schema_to_python_schema"
```

---

## Task 4: Create test_schema_arrow_equality.py

With all three implementation fixes in place, this task creates the comprehensive test file that verifies the full Schema ↔ Arrow logical equality guarantee. Write the file, run it, confirm everything passes.

**Files:**
- Create: `tests/test_semantic_types/test_schema_arrow_equality.py`

- [ ] **Step 1: Create the test file**

Create `tests/test_semantic_types/test_schema_arrow_equality.py` with the following content:

```python
"""
Tests verifying Schema ↔ Arrow logical equality (PLT-923).

Coverage
--------
- Python-equal schemas produce logically equal Arrow schemas
- Python-unequal schemas produce logically unequal Arrow schemas
- Field insertion order does not affect logical equality
- Nullability correspondence: T | None → nullable=True, plain T → nullable=False
- Round-trip: python_schema_to_arrow_schema ∘ arrow_schema_to_python_schema is lossless
- Nested/complex types maintain the correspondence
- Schema.as_required() strips optional_fields for Arrow-level comparison

"Logical equality" is determined by StarfixArrowHasher.hash_schema digest equality:
column-order-independent, Utf8/LargeUtf8 and Binary/LargeBinary normalised,
nullability-sensitive.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.semantic_types import SemanticTypeRegistry
from orcapod.types import Schema

# ---------------------------------------------------------------------------
# Shared infrastructure
# ---------------------------------------------------------------------------

# SemanticTypeRegistry is empty: hash_schema operates on Arrow types only and
# never consults the semantic registry (unlike hash_table).
_hasher = StarfixArrowHasher(SemanticTypeRegistry(), hasher_id="test")


def _to_arrow(schema: Schema) -> pa.Schema:
    """Convert a Python Schema to an Arrow schema via the default context."""
    return get_default_context().type_converter.python_schema_to_arrow_schema(schema)


def _arrow_logical_eq(s1: pa.Schema, s2: pa.Schema) -> bool:
    """Return True if two Arrow schemas are logically equal under the starfix hash."""
    return _hasher.hash_schema(s1).digest == _hasher.hash_schema(s2).digest


# ---------------------------------------------------------------------------
# Positive: equal Python schemas → logically equal Arrow schemas
# ---------------------------------------------------------------------------


class TestEqualSchemasHaveLogicallyEqualArrowSchemas:
    def test_single_int_field(self):
        s1 = Schema(a=int)
        s2 = Schema(a=int)
        assert s1 == s2
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_single_float_field(self):
        s1 = Schema(a=float)
        s2 = Schema(a=float)
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_single_str_field(self):
        s1 = Schema(a=str)
        s2 = Schema(a=str)
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_single_bool_field(self):
        s1 = Schema(a=bool)
        s2 = Schema(a=bool)
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_single_bytes_field(self):
        s1 = Schema(a=bytes)
        s2 = Schema(a=bytes)
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_multiple_primitive_fields(self):
        s1 = Schema({"a": int, "b": float, "c": str})
        s2 = Schema({"a": int, "b": float, "c": str})
        assert s1 == s2
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_kwargs_vs_mapping_construction(self):
        """Schema(a=int, b=str) must equal Schema({"a": int, "b": str})."""
        s_kwargs = Schema(a=int, b=str)
        s_mapping = Schema({"a": int, "b": str})
        assert s_kwargs == s_mapping
        assert _arrow_logical_eq(_to_arrow(s_kwargs), _to_arrow(s_mapping))

    def test_empty_schema(self):
        s1 = Schema.empty()
        s2 = Schema({})
        assert s1 == s2
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_schema_equals_plain_dict(self):
        """Schema.__eq__ accepts plain Mapping; dict → Arrow conversion must match."""
        s = Schema({"x": int})
        d = {"x": int}
        # Schema.__eq__ raises NotImplementedError for non-Mapping non-Schema; plain
        # dict is a Mapping so this should work.
        assert s == d
        assert _arrow_logical_eq(
            _to_arrow(s),
            get_default_context().type_converter.python_schema_to_arrow_schema(d),
        )


# ---------------------------------------------------------------------------
# Negative: unequal Python schemas → logically unequal Arrow schemas
# ---------------------------------------------------------------------------


class TestUnequalSchemasHaveLogicallyUnequalArrowSchemas:
    def test_different_field_names(self):
        s1 = Schema(a=int)
        s2 = Schema(b=int)
        assert s1 != s2
        assert not _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_different_field_types(self):
        s1 = Schema(a=int)
        s2 = Schema(a=float)
        assert s1 != s2
        assert not _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_subset_schema_differs(self):
        s1 = Schema({"a": int, "b": str})
        s2 = Schema({"a": int})
        assert s1 != s2
        assert not _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))


# ---------------------------------------------------------------------------
# Field ordering
# ---------------------------------------------------------------------------


class TestFieldOrderingDoesNotAffectLogicalEquality:
    def test_two_fields_reversed_insertion_order(self):
        """Both Python equality and Arrow logical equality are order-insensitive."""
        s1 = Schema({"a": int, "b": str})
        s2 = Schema({"b": str, "a": int})
        assert s1 == s2
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_three_fields_permuted_order(self):
        s1 = Schema({"x": int, "y": float, "z": str})
        s2 = Schema({"z": str, "x": int, "y": float})
        assert s1 == s2
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))


# ---------------------------------------------------------------------------
# Nullability correspondence
# ---------------------------------------------------------------------------


class TestNullabilityCorrespondence:
    def test_plain_int_is_non_nullable(self):
        arrow = _to_arrow(Schema(a=int))
        assert arrow.field("a").nullable is False

    def test_optional_int_is_nullable(self):
        arrow = _to_arrow(Schema({"a": int | None}))
        assert arrow.field("a").nullable is True

    def test_plain_primitives_all_non_nullable(self):
        arrow = _to_arrow(Schema({"a": str, "b": float, "c": bool, "d": bytes}))
        for name in ("a", "b", "c", "d"):
            assert arrow.field(name).nullable is False, (
                f"Expected {name} to be non-nullable"
            )

    def test_optional_primitives_all_nullable(self):
        arrow = _to_arrow(Schema({"a": str | None, "b": float | None}))
        assert arrow.field("a").nullable is True
        assert arrow.field("b").nullable is True

    def test_int_and_optional_int_are_python_unequal(self):
        assert Schema(a=int) != Schema({"a": int | None})

    def test_int_and_optional_int_are_arrow_logically_unequal(self):
        s_plain = Schema(a=int)
        s_optional = Schema({"a": int | None})
        assert not _arrow_logical_eq(_to_arrow(s_plain), _to_arrow(s_optional))


# ---------------------------------------------------------------------------
# Round-trip: Python → Arrow → Python
# ---------------------------------------------------------------------------


class TestRoundTrip:
    def _round_trip(self, schema: Schema) -> Schema:
        converter = get_default_context().type_converter
        return converter.arrow_schema_to_python_schema(
            converter.python_schema_to_arrow_schema(schema)
        )

    def test_int_stays_int(self):
        result = self._round_trip(Schema(a=int))
        assert result["a"] == int

    def test_optional_int_stays_optional_int(self):
        result = self._round_trip(Schema({"a": int | None}))
        assert result["a"] == int | None

    def test_plain_str_stays_str(self):
        result = self._round_trip(Schema(a=str))
        assert result["a"] == str

    def test_optional_str_stays_optional_str(self):
        result = self._round_trip(Schema({"a": str | None}))
        assert result["a"] == str | None

    def test_plain_float_stays_float(self):
        result = self._round_trip(Schema(a=float))
        assert result["a"] == float

    def test_plain_bool_stays_bool(self):
        result = self._round_trip(Schema(a=bool))
        assert result["a"] == bool

    def test_plain_bytes_stays_bytes(self):
        result = self._round_trip(Schema(a=bytes))
        assert result["a"] == bytes

    def test_optional_float_stays_optional_float(self):
        result = self._round_trip(Schema({"a": float | None}))
        assert result["a"] == float | None

    def test_mixed_nullable_and_non_nullable(self):
        original = Schema({"req": int, "opt": str | None, "also_req": float})
        result = self._round_trip(original)
        assert result["req"] == int
        assert result["opt"] == str | None
        assert result["also_req"] == float


# ---------------------------------------------------------------------------
# Nested and complex types
# ---------------------------------------------------------------------------


class TestNestedAndComplexTypes:
    def test_list_int_is_non_nullable(self):
        arrow = _to_arrow(Schema({"a": list[int]}))
        assert arrow.field("a").nullable is False

    def test_list_str_is_non_nullable(self):
        arrow = _to_arrow(Schema({"a": list[str]}))
        assert arrow.field("a").nullable is False

    def test_optional_list_int_is_nullable(self):
        arrow = _to_arrow(Schema({"a": list[int] | None}))
        assert arrow.field("a").nullable is True

    def test_nested_list_is_non_nullable(self):
        arrow = _to_arrow(Schema({"a": list[list[int]]}))
        assert arrow.field("a").nullable is False

    def test_path_is_non_nullable(self):
        """Path → Arrow struct {path: large_string}, nullable=False."""
        arrow = _to_arrow(Schema({"p": Path}))
        assert arrow.field("p").nullable is False
        assert pa.types.is_struct(arrow.field("p").type)

    def test_equal_list_schemas_are_logically_equal(self):
        s1 = Schema({"items": list[int]})
        s2 = Schema({"items": list[int]})
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_list_int_and_list_str_are_logically_unequal(self):
        s1 = Schema({"items": list[int]})
        s2 = Schema({"items": list[str]})
        assert not _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))


# ---------------------------------------------------------------------------
# Schema.as_required()
# ---------------------------------------------------------------------------


class TestAsRequired:
    def test_as_required_equals_schema_without_optional_fields(self):
        """Schema with optional_fields equals a Schema without after as_required()."""
        s_with_optional = Schema({"a": int, "b": str}, optional_fields=["b"])
        s_without = Schema({"a": int, "b": str})
        assert s_with_optional.as_required() == s_without

    def test_as_required_on_schema_without_optional_is_noop(self):
        """as_required() on a fully required schema is idempotent."""
        s = Schema({"a": int, "b": str})
        assert s.as_required() == s

    def test_as_required_idempotent(self):
        """Calling as_required() twice gives the same result as once."""
        s = Schema({"a": int}, optional_fields=["a"])
        assert s.as_required().as_required() == s.as_required()

    def test_schemas_differing_only_in_optional_fields_are_python_unequal(self):
        """Two schemas with the same fields but different optional_fields are unequal."""
        s1 = Schema({"a": int, "b": str}, optional_fields=["b"])
        s2 = Schema({"a": int, "b": str})
        assert s1 != s2

    def test_schemas_differing_only_in_optional_fields_have_equal_arrow_schemas(self):
        """optional_fields has no Arrow representation — Arrow schemas must be equal."""
        s1 = Schema({"a": int, "b": str}, optional_fields=["b"])
        s2 = Schema({"a": int, "b": str})
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))

    def test_as_required_implies_arrow_logical_equality(self):
        """If s1.as_required() == s2.as_required(), their Arrow schemas are logically equal."""
        s1 = Schema({"x": int, "y": float}, optional_fields=["x"])
        s2 = Schema({"x": int, "y": float})
        assert s1.as_required() == s2.as_required()
        assert _arrow_logical_eq(_to_arrow(s1), _to_arrow(s2))
```

- [ ] **Step 2: Run the new test file**

```bash
uv run pytest tests/test_semantic_types/test_schema_arrow_equality.py -v
```

Expected: all tests `PASSED`. If any fail, examine the output carefully:
- A `nullable` assertion failure means Task 2 or 3 was not fully applied — re-check the implementation.
- An `as_required()` failure means Task 1 was not applied — re-check the implementation.
- A logical equality failure when it should be equal suggests a field ordering or type normalisation issue — check the `_arrow_logical_eq` helper and the starfix hasher setup.

- [ ] **Step 3: Run the full test suite one final time**

```bash
uv run pytest tests/ -q
```

Expected: all pass with no warnings about deprecated APIs.

- [ ] **Step 4: Commit**

```bash
git add tests/test_semantic_types/test_schema_arrow_equality.py
git commit -m "test(schema): add comprehensive Schema ↔ Arrow logical equality test suite (PLT-923)"
```

---

## Final verification

- [ ] **Run the full suite cleanly**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -20
```

Expected: summary line showing all tests passed with zero failures.

- [ ] **Confirm the four files changed are exactly as scoped**

```bash
git diff --name-only HEAD~4
```

Expected output (order may vary):
```
src/orcapod/types.py
src/orcapod/semantic_types/universal_converter.py
tests/test_semantic_types/test_universal_converter.py
tests/test_semantic_types/test_schema_arrow_equality.py
```
