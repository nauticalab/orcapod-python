"""Tests for orcapod.types — Schema operations."""
from __future__ import annotations

import pytest

from orcapod.types import ContentHash, NodeConfig, Schema


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


class TestContentHashPrefixedDigest:
    """Tests for ContentHash.from_prefixed_digest() round-trip."""

    def test_roundtrip_sha256(self):
        h = ContentHash("sha256", bytes(range(32)))
        assert ContentHash.from_prefixed_digest(h.to_prefixed_digest()) == h

    def test_roundtrip_arrow_method(self):
        h = ContentHash("arrow_v2.1", b"\xde\xad\xbe\xef" * 8)
        result = ContentHash.from_prefixed_digest(h.to_prefixed_digest())
        assert result.method == "arrow_v2.1"
        assert result.digest == b"\xde\xad\xbe\xef" * 8

    def test_preserves_binary_digest_with_colon_bytes(self):
        # digest bytes that themselves contain a colon — only the first colon splits
        digest = b"abc:def"
        h = ContentHash("sha256", digest)
        assert ContentHash.from_prefixed_digest(h.to_prefixed_digest()) == h

    def test_inverse_of_to_prefixed_digest(self):
        """from_prefixed_digest is the exact inverse of to_prefixed_digest."""
        for method, digest in [
            ("sha256", b"\x00" * 32),
            ("md5", b"\xff" * 16),
            ("arrow_v2.1", b"\x01\x02\x03"),
        ]:
            h = ContentHash(method, digest)
            assert ContentHash.from_prefixed_digest(h.to_prefixed_digest()) == h


class TestNodeConfig:
    """Tests for NodeConfig dataclass and merge()."""

    def test_ignore_schema_defaults_to_none(self):
        config = NodeConfig()
        assert config.ignore_schema is None

    def test_ignore_schema_set(self):
        config = NodeConfig(ignore_schema=("v0",))
        assert config.ignore_schema == ("v0",)

    def test_merge_ignore_schema_none_in_other_self_wins(self):
        base = NodeConfig(ignore_schema=("v0",))
        other = NodeConfig()  # ignore_schema=None
        result = base.merge(other)
        assert result.ignore_schema == ("v0",)

    def test_merge_ignore_schema_other_overrides(self):
        base = NodeConfig(ignore_schema=("v0",))
        other = NodeConfig(ignore_schema=("v0", "v1"))
        result = base.merge(other)
        assert result.ignore_schema == ("v0", "v1")

    def test_merge_ignore_schema_empty_tuple_overrides(self):
        """Empty tuple () is a valid explicit value that overrides None."""
        base = NodeConfig(ignore_schema=("v0",))
        other = NodeConfig(ignore_schema=())
        result = base.merge(other)
        assert result.ignore_schema == ()


class TestSchemaVersionError:
    def test_is_exception(self):
        from orcapod.errors import SchemaVersionError
        err = SchemaVersionError("test message")
        assert isinstance(err, Exception)

    def test_message_preserved(self):
        from orcapod.errors import SchemaVersionError
        err = SchemaVersionError("Pipeline DB at v0 path")
        assert "v0" in str(err)
