"""
Tests for the new sources package.

Covers:
- RootSource: source_id, resolve_field default raises FieldNotResolvableError
- ArrowTableSource: row-index record IDs, column-value record IDs, resolve_field
- DictSource / ListSource: inherit default resolve_field (not overridden)
- SourceRegistry: register, get, replace, collision behaviour, list_ids
- source_info provenance token format in produced tables
"""

from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.sources import (
    ArrowTableSource,
    DictSource,
    ListSource,
    SourceRegistry,
    GLOBAL_SOURCE_REGISTRY,
)
from orcapod.errors import FieldNotResolvableError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_arrow_source(record_id_column=None, source_id=None):
    table = pa.table(
        {
            "user_id": pa.array(["u1", "u2", "u3"], type=pa.large_string()),
            "score": pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    return ArrowTableSource(
        table=table,
        tag_columns=["user_id"],
        record_id_column=record_id_column,
        source_id=source_id,
    )


# ---------------------------------------------------------------------------
# RootSource: source_id
# ---------------------------------------------------------------------------


class TestSourceId:
    def test_explicit_source_id_is_used(self):
        src = _make_arrow_source(source_id="my_source")
        assert src.source_id == "my_source"

    def test_default_source_id_is_content_hash(self):
        src = _make_arrow_source()
        # Should be a non-empty hex string, deterministic for same content.
        sid = src.source_id
        assert isinstance(sid, str)
        assert len(sid) > 0

    def test_same_content_same_source_id(self):
        src1 = _make_arrow_source()
        src2 = _make_arrow_source()
        assert src1.source_id == src2.source_id

    def test_different_content_different_source_id(self):
        table_a = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        table_b = pa.table({"x": pa.array([4, 5, 6], type=pa.int64())})
        src_a = ArrowTableSource(table=table_a)
        src_b = ArrowTableSource(table=table_b)
        assert src_a.source_id != src_b.source_id


# ---------------------------------------------------------------------------
# RootSource: default resolve_field raises FieldNotResolvableError
# ---------------------------------------------------------------------------


class TestDefaultResolveField:
    def test_dict_source_raises_field_not_resolvable(self):
        src = DictSource(
            data=[{"id": 1, "val": "a"}, {"id": 2, "val": "b"}],
            tag_columns=["id"],
        )
        with pytest.raises(FieldNotResolvableError):
            src.resolve_field("row_0", "val")

    def test_list_source_raises_field_not_resolvable(self):
        src = ListSource(name="item", data=["x", "y", "z"])
        with pytest.raises(FieldNotResolvableError):
            src.resolve_field("row_0", "item")

    def test_error_message_contains_source_id_and_field(self):
        src = DictSource(
            data=[{"id": 1, "val": "a"}],
            tag_columns=["id"],
            source_id="test_source",
        )
        with pytest.raises(FieldNotResolvableError, match="test_source"):
            src.resolve_field("row_0", "val")


# ---------------------------------------------------------------------------
# ArrowTableSource: resolve_field with row-index record IDs
# ---------------------------------------------------------------------------


class TestArrowTableSourceResolveFieldRowIndex:
    def setup_method(self):
        self.src = _make_arrow_source()  # no record_id_column → row_N IDs

    def test_resolve_first_row(self):
        assert self.src.resolve_field("row_0", "score") == 10

    def test_resolve_middle_row(self):
        assert self.src.resolve_field("row_1", "score") == 20

    def test_resolve_last_row(self):
        assert self.src.resolve_field("row_2", "score") == 30

    def test_resolve_tag_column(self):
        assert self.src.resolve_field("row_0", "user_id") == "u1"

    def test_unknown_field_raises(self):
        with pytest.raises(FieldNotResolvableError, match="nonexistent"):
            self.src.resolve_field("row_0", "nonexistent")

    def test_out_of_range_index_raises(self):
        with pytest.raises(FieldNotResolvableError):
            self.src.resolve_field("row_99", "score")

    def test_malformed_record_id_raises(self):
        with pytest.raises(FieldNotResolvableError):
            self.src.resolve_field("user_id=u1", "score")

    def test_non_integer_index_raises(self):
        with pytest.raises(FieldNotResolvableError):
            self.src.resolve_field("row_abc", "score")


# ---------------------------------------------------------------------------
# ArrowTableSource: resolve_field with column-value record IDs
# ---------------------------------------------------------------------------


class TestArrowTableSourceResolveFieldColumnValue:
    def setup_method(self):
        self.src = _make_arrow_source(record_id_column="user_id")

    def test_resolve_by_column_value(self):
        assert self.src.resolve_field("user_id=u1", "score") == 10

    def test_resolve_second_record(self):
        assert self.src.resolve_field("user_id=u2", "score") == 20

    def test_resolve_id_column_itself(self):
        assert self.src.resolve_field("user_id=u3", "user_id") == "u3"

    def test_unknown_field_raises(self):
        with pytest.raises(FieldNotResolvableError, match="nonexistent"):
            self.src.resolve_field("user_id=u1", "nonexistent")

    def test_unknown_record_id_value_raises(self):
        with pytest.raises(FieldNotResolvableError):
            self.src.resolve_field("user_id=no_such_user", "score")

    def test_wrong_format_raises(self):
        # Providing a row_N token when column-value format is expected
        with pytest.raises(FieldNotResolvableError):
            self.src.resolve_field("row_0", "score")

    def test_wrong_column_name_in_token_raises(self):
        with pytest.raises(FieldNotResolvableError):
            self.src.resolve_field("score=10", "score")


# ---------------------------------------------------------------------------
# ArrowTableSource: record_id_column validation
# ---------------------------------------------------------------------------


class TestArrowTableSourceRecordIdColumnValidation:
    def test_nonexistent_record_id_column_raises_at_construction(self):
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        with pytest.raises(ValueError, match="record_id_column"):
            ArrowTableSource(table=table, record_id_column="nonexistent")


# ---------------------------------------------------------------------------
# ArrowTableSource: source_info tokens in produced table
# ---------------------------------------------------------------------------


class TestArrowTableSourceInfoTokens:
    def test_row_index_tokens_in_source_info(self):
        src = _make_arrow_source()  # no record_id_column
        table = src.as_table(all_info=True)
        # Source info column for "score" should contain "row_0", "row_1", "row_2"
        source_col = [c for c in table.column_names if c.startswith("_source_score")]
        assert source_col, "Expected a _source_score column"
        values = table.column(source_col[0]).to_pylist()
        assert all("row_" in v for v in values)

    def test_column_value_tokens_in_source_info(self):
        src = _make_arrow_source(record_id_column="user_id")
        table = src.as_table(all_info=True)
        source_col = [c for c in table.column_names if c.startswith("_source_score")]
        assert source_col, "Expected a _source_score column"
        values = table.column(source_col[0]).to_pylist()
        assert all("user_id=" in v for v in values)

    def test_source_id_appears_in_provenance_token(self):
        src = _make_arrow_source(source_id="my_ds")
        table = src.as_table(all_info=True)
        source_col = [c for c in table.column_names if c.startswith("_source_score")]
        values = table.column(source_col[0]).to_pylist()
        assert all(v.startswith("my_ds::") for v in values)


# ---------------------------------------------------------------------------
# SourceRegistry
# ---------------------------------------------------------------------------


class TestSourceRegistry:
    def setup_method(self):
        self.registry = SourceRegistry()
        self.src = _make_arrow_source(source_id="test_src")

    def test_register_and_get(self):
        self.registry.register("test_src", self.src)
        assert self.registry.get("test_src") is self.src

    def test_get_missing_raises_key_error(self):
        with pytest.raises(KeyError):
            self.registry.get("nonexistent")

    def test_get_optional_missing_returns_none(self):
        assert self.registry.get_optional("nonexistent") is None

    def test_register_same_object_twice_is_idempotent(self):
        self.registry.register("test_src", self.src)
        self.registry.register("test_src", self.src)  # should not raise
        assert len(self.registry) == 1

    def test_register_different_object_same_id_keeps_existing(self):
        other = _make_arrow_source(source_id="test_src")
        self.registry.register("test_src", self.src)
        self.registry.register("test_src", other)  # warns, keeps original
        assert self.registry.get("test_src") is self.src

    def test_replace_overwrites(self):
        other = _make_arrow_source(source_id="test_src")
        self.registry.register("test_src", self.src)
        old = self.registry.replace("test_src", other)
        assert old is self.src
        assert self.registry.get("test_src") is other

    def test_unregister_removes_and_returns(self):
        self.registry.register("test_src", self.src)
        returned = self.registry.unregister("test_src")
        assert returned is self.src
        assert "test_src" not in self.registry

    def test_unregister_missing_raises_key_error(self):
        with pytest.raises(KeyError):
            self.registry.unregister("nonexistent")

    def test_contains(self):
        self.registry.register("test_src", self.src)
        assert "test_src" in self.registry
        assert "other" not in self.registry

    def test_len(self):
        assert len(self.registry) == 0
        self.registry.register("test_src", self.src)
        assert len(self.registry) == 1

    def test_list_ids(self):
        self.registry.register("test_src", self.src)
        assert self.registry.list_ids() == ["test_src"]

    def test_clear(self):
        self.registry.register("test_src", self.src)
        self.registry.clear()
        assert len(self.registry) == 0

    def test_empty_source_id_raises(self):
        with pytest.raises(ValueError):
            self.registry.register("", self.src)

    def test_iter(self):
        self.registry.register("test_src", self.src)
        ids = list(self.registry)
        assert ids == ["test_src"]

    def test_items(self):
        self.registry.register("test_src", self.src)
        pairs = list(self.registry.items())
        assert pairs == [("test_src", self.src)]


class TestSourceRegistryRoundTrip:
    """Registry + resolve_field end-to-end."""

    def test_resolve_via_registry(self):
        src = _make_arrow_source(source_id="sales", record_id_column="user_id")
        registry = SourceRegistry()
        registry.register("sales", src)

        # Simulate what downstream code does: parse the source_id from a
        # provenance token, look up the source, resolve the field.
        resolved_src = registry.get("sales")
        value = resolved_src.resolve_field("user_id=u2", "score")
        assert value == 20

    def test_global_registry_is_a_source_registry_instance(self):
        assert isinstance(GLOBAL_SOURCE_REGISTRY, SourceRegistry)
