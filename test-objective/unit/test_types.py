"""Specification-derived tests for Schema, ColumnConfig, and ContentHash."""

import uuid

import pytest

from orcapod.types import ColumnConfig, ContentHash, Schema


# ---------------------------------------------------------------------------
# Schema basics
# ---------------------------------------------------------------------------

class TestSchemaImmutableMapping:
    """Schema behaves as an immutable Mapping[str, DataType]."""

    def test_schema_acts_as_mapping(self):
        s = Schema({"x": int, "y": str})
        assert "x" in s
        assert s["x"] == int
        assert len(s) == 2
        assert set(s) == {"x", "y"}

    def test_schema_is_immutable(self):
        s = Schema({"x": int})
        with pytest.raises(TypeError):
            s["x"] = float

    def test_schema_equality(self):
        a = Schema({"x": int, "y": str})
        b = Schema({"x": int, "y": str})
        assert a == b

    def test_schema_inequality_different_types(self):
        a = Schema({"x": int})
        b = Schema({"x": float})
        assert a != b


class TestSchemaOptionalFields:
    """Schema supports optional_fields."""

    def test_optional_fields_default_empty(self):
        s = Schema({"x": int})
        assert s.optional_fields == frozenset()

    def test_optional_fields_set_at_construction(self):
        s = Schema({"x": int, "y": str}, optional_fields={"y"})
        assert "y" in s.optional_fields
        assert "x" not in s.optional_fields

    def test_optional_fields_can_include_unknown_fields(self):
        # Schema doesn't validate optional_fields against actual fields
        s = Schema({"x": int}, optional_fields={"z"})
        assert "z" in s.optional_fields


class TestSchemaEmpty:
    """Schema.empty() returns a zero-field schema."""

    def test_empty_schema_has_no_fields(self):
        s = Schema.empty()
        assert len(s) == 0
        assert list(s) == []


class TestSchemaMerge:
    """Schema.merge() raises ValueError on type conflicts."""

    def test_merge_disjoint_schemas(self):
        a = Schema({"x": int})
        b = Schema({"y": str})
        merged = a.merge(b)
        assert "x" in merged
        assert "y" in merged

    def test_merge_overlapping_same_type(self):
        a = Schema({"x": int, "y": str})
        b = Schema({"x": int, "z": float})
        merged = a.merge(b)
        assert merged["x"] == int
        assert "z" in merged

    def test_merge_raises_on_type_conflict(self):
        a = Schema({"x": int})
        b = Schema({"x": str})
        with pytest.raises(ValueError):
            a.merge(b)


class TestSchemaSelect:
    """Schema.select() raises KeyError on missing fields."""

    def test_select_existing_fields(self):
        s = Schema({"x": int, "y": str, "z": float})
        selected = s.select("x", "z")
        assert set(selected) == {"x", "z"}

    def test_select_raises_on_missing_field(self):
        s = Schema({"x": int})
        with pytest.raises(KeyError):
            s.select("x", "missing")


class TestSchemaDrop:
    """Schema.drop() silently ignores missing fields."""

    def test_drop_existing_fields(self):
        s = Schema({"x": int, "y": str, "z": float})
        dropped = s.drop("y")
        assert set(dropped) == {"x", "z"}

    def test_drop_missing_field_silently_ignored(self):
        s = Schema({"x": int, "y": str})
        dropped = s.drop("nonexistent")
        assert set(dropped) == {"x", "y"}

    def test_drop_mix_of_existing_and_missing(self):
        s = Schema({"x": int, "y": str})
        dropped = s.drop("x", "nonexistent")
        assert set(dropped) == {"y"}


class TestSchemaCompatibility:
    """Schema.is_compatible_with() returns True when other is superset."""

    def test_compatible_when_other_is_superset(self):
        small = Schema({"x": int})
        big = Schema({"x": int, "y": str})
        assert small.is_compatible_with(big)

    def test_compatible_with_itself(self):
        s = Schema({"x": int})
        assert s.is_compatible_with(s)

    def test_not_compatible_when_field_missing(self):
        a = Schema({"x": int, "y": str})
        b = Schema({"x": int})
        assert not a.is_compatible_with(b)

    def test_not_compatible_when_type_differs(self):
        a = Schema({"x": int})
        b = Schema({"x": str})
        assert not a.is_compatible_with(b)


class TestSchemaWithValues:
    """Schema.with_values() overrides silently (no errors)."""

    def test_with_values_adds_new_field(self):
        s = Schema({"x": int})
        updated = s.with_values({"y": str})
        assert "y" in updated
        assert "x" in updated

    def test_with_values_overrides_existing_type(self):
        s = Schema({"x": int})
        updated = s.with_values({"x": float})
        assert updated["x"] == float

    def test_with_values_does_not_mutate_original(self):
        s = Schema({"x": int})
        s.with_values({"x": float})
        assert s["x"] == int


# ---------------------------------------------------------------------------
# ContentHash
# ---------------------------------------------------------------------------

class TestContentHash:
    """ContentHash is a frozen dataclass with method+digest."""

    def test_content_hash_is_frozen(self):
        h = ContentHash(method="sha256", digest=b"\x00" * 32)
        with pytest.raises(AttributeError):
            h.method = "md5"

    def test_content_hash_has_method_and_digest(self):
        h = ContentHash(method="sha256", digest=b"\xab\xcd")
        assert h.method == "sha256"
        assert h.digest == b"\xab\xcd"


class TestContentHashConversions:
    """ContentHash conversions: to_hex, to_int, to_uuid, to_base64, to_string."""

    def _make_hash(self):
        return ContentHash(method="sha256", digest=b"\x01\x02\x03\x04" * 4)

    def test_to_hex_returns_string(self):
        h = self._make_hash()
        hex_str = h.to_hex()
        assert isinstance(hex_str, str)
        assert all(c in "0123456789abcdef" for c in hex_str)

    def test_to_int_returns_integer(self):
        h = self._make_hash()
        assert isinstance(h.to_int(), int)

    def test_to_uuid_returns_uuid(self):
        h = self._make_hash()
        result = h.to_uuid()
        assert isinstance(result, uuid.UUID)

    def test_to_base64_returns_string(self):
        h = self._make_hash()
        b64 = h.to_base64()
        assert isinstance(b64, str)

    def test_to_string_returns_string(self):
        h = self._make_hash()
        s = h.to_string()
        assert isinstance(s, str)

    def test_from_string_roundtrip(self):
        h = self._make_hash()
        s = h.to_string()
        restored = ContentHash.from_string(s)
        assert restored.method == h.method
        assert restored.digest == h.digest


# ---------------------------------------------------------------------------
# ColumnConfig
# ---------------------------------------------------------------------------

class TestColumnConfig:
    """ColumnConfig is frozen, has .all() and .data_only() convenience methods."""

    def test_column_config_is_frozen(self):
        cc = ColumnConfig()
        with pytest.raises(AttributeError):
            cc.meta = True

    def test_all_sets_everything_true(self):
        cc = ColumnConfig.all()
        assert cc.meta is True
        assert cc.source is True
        assert cc.system_tags is True
        assert cc.context is True

    def test_data_only_excludes_extras(self):
        cc = ColumnConfig.data_only()
        assert cc.meta is False
        assert cc.source is False
        assert cc.system_tags is False

    def test_default_construction(self):
        cc = ColumnConfig()
        assert isinstance(cc, ColumnConfig)


class TestColumnConfigHandleConfig:
    """ColumnConfig.handle_config() normalizes dict/None/instance inputs."""

    def test_handle_config_none_returns_default(self):
        result = ColumnConfig.handle_config(None)
        assert isinstance(result, ColumnConfig)

    def test_handle_config_instance_passes_through(self):
        cc = ColumnConfig.all()
        result = ColumnConfig.handle_config(cc)
        assert result is cc

    def test_handle_config_dict_creates_config(self):
        result = ColumnConfig.handle_config({"meta": True})
        assert isinstance(result, ColumnConfig)
        assert result.meta is True

    def test_handle_config_all_info_flag(self):
        result = ColumnConfig.handle_config(None, all_info=True)
        assert result.meta is True
        assert result.source is True
        assert result.system_tags is True
