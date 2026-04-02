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
