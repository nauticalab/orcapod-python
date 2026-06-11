"""Tests for orcapod.types — Schema operations."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.types import UUID_ARROW_TYPE, UUID_STRUCT_ARROW_TYPE, Schema


def test_uuid_arrow_type_is_binary16():
    assert UUID_ARROW_TYPE == pa.binary(16)


def test_uuid_struct_arrow_type_structure():
    assert UUID_STRUCT_ARROW_TYPE == pa.struct([pa.field("uuid", pa.binary(16))])


def test_uuid_struct_inner_type_matches_constant():
    assert UUID_STRUCT_ARROW_TYPE.field("uuid").type == UUID_ARROW_TYPE


class TestSchemaAdd:
    """Tests for Schema.__add__."""

    def test_add_returns_merged_schema(self):
        """s1 + s2 returns a new Schema containing all fields from both."""
        s1 = Schema({"a": int, "b": str})
        s2 = Schema({"c": float})
        result = s1 + s2
        assert isinstance(result, Schema)
        assert result["a"] is int
        assert result["b"] is str
        assert result["c"] is float

    def test_add_delegates_to_merge(self):
        """s1 + s2 is identical to s1.merge(s2)."""
        s1 = Schema({"x": int})
        s2 = Schema({"y": str})
        assert (s1 + s2) == s1.merge(s2)

    def test_add_empty_schemas(self):
        """Adding two empty schemas returns an empty schema."""
        assert (Schema.empty() + Schema.empty()) == Schema.empty()

    def test_add_with_empty(self):
        """Adding an empty schema to a non-empty schema is a no-op."""
        s = Schema({"a": int})
        assert (s + Schema.empty()) == s
        assert (Schema.empty() + s) == s

    def test_add_raises_on_type_conflict(self):
        """Adding schemas with conflicting field types raises ValueError."""
        s1 = Schema({"a": int})
        s2 = Schema({"a": str})
        with pytest.raises(ValueError, match="conflict"):
            _ = s1 + s2

    def test_add_raises_not_implemented_for_non_schema(self):
        """Adding a non-Schema raises NotImplementedError."""
        s = Schema({"a": int})
        with pytest.raises(NotImplementedError):
            _ = s + {"b": str}  # type: ignore[operator]

    def test_add_preserves_optional_fields(self):
        """__add__ preserves optional_fields from both schemas."""
        s1 = Schema({"a": int, "b": str}, optional_fields={"b"})
        s2 = Schema({"c": float}, optional_fields={"c"})
        result = s1 + s2
        assert "b" in result.optional_fields
        assert "c" in result.optional_fields
        assert "a" not in result.optional_fields
