"""Property-based tests for Schema algebra using Hypothesis.

Tests algebraic properties that must hold for any valid input,
not just hand-picked examples.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from orcapod.types import Schema

# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Simple Python types that Schema supports
simple_types = st.sampled_from([int, float, str, bool, bytes])

# Field name strategy
field_names = st.text(
    alphabet=st.characters(whitelist_categories=("L", "N"), whitelist_characters="_"),
    min_size=1,
    max_size=10,
).filter(lambda s: s[0].isalpha())

# Schema strategy: dict of 1-5 fields
schema_dicts = st.dictionaries(field_names, simple_types, min_size=1, max_size=5)


def make_schema(d: dict) -> Schema:
    return Schema(d)


# ===================================================================
# Schema merge commutativity
# ===================================================================


class TestSchemaMergeCommutativity:
    """merge(A, B) == merge(B, A) when schemas are compatible."""

    @given(schema_dicts, schema_dicts)
    @settings(max_examples=50)
    def test_merge_commutative_when_compatible(self, d1, d2):
        s1 = make_schema(d1)
        s2 = make_schema(d2)

        # Check if they're compatible (no type conflicts)
        conflicts = {k for k in d2 if k in d1 and d1[k] != d2[k]}
        if conflicts:
            return  # Skip incompatible schemas

        merged_ab = s1.merge(s2)
        merged_ba = s2.merge(s1)
        assert dict(merged_ab) == dict(merged_ba)


# ===================================================================
# Schema is_compatible_with is reflexive
# ===================================================================


class TestSchemaCompatibilityReflexive:
    """A.is_compatible_with(A) should always be True."""

    @given(schema_dicts)
    @settings(max_examples=50)
    def test_reflexive(self, d):
        s = make_schema(d)
        assert s.is_compatible_with(s)


# ===================================================================
# Schema select/drop complementarity
# ===================================================================


class TestSchemaSelectDropComplementary:
    """select(X) ∪ drop(X) should recover the original schema's fields."""

    @given(schema_dicts)
    @settings(max_examples=50)
    def test_select_drop_complementary(self, d):
        s = make_schema(d)
        if len(s) < 2:
            return  # Need at least 2 fields

        fields = list(s.keys())
        mid = len(fields) // 2
        selected_fields = fields[:mid]
        dropped_fields = fields[:mid]

        selected = s.select(*selected_fields)
        dropped = s.drop(*dropped_fields)

        # Union of selected and dropped should cover all fields
        all_keys = set(selected.keys()) | set(dropped.keys())
        assert all_keys == set(s.keys())


# ===================================================================
# Schema optional_fields is subset of all fields
# ===================================================================


class TestSchemaOptionalFieldsSubset:
    """optional_fields should always be a subset of all field names."""

    @given(schema_dicts, st.lists(field_names, max_size=3))
    @settings(max_examples=50)
    def test_optional_subset(self, d, optional_candidates):
        # Only use candidates that are actual fields
        valid_optional = [f for f in optional_candidates if f in d]
        s = Schema(d, optional_fields=valid_optional)
        assert s.optional_fields.issubset(set(s.keys()))

    @given(schema_dicts)
    @settings(max_examples=50)
    def test_required_plus_optional_equals_all(self, d):
        s = make_schema(d)
        assert s.required_fields | s.optional_fields == set(s.keys())
