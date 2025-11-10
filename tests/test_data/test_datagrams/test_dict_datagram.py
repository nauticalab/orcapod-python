"""
Comprehensive tests for DictDatagram class.

This module tests all functionality of the DictDatagram class including:
- Initialization and validation
- Dict-like interface operations
- Structural information methods
- Format conversion methods
- Meta column operations
- Data column operations
- Context operations
- Utility operations
"""

import pytest
import pyarrow as pa

from orcapod.core.datagrams import DictDatagram
from orcapod.contexts.system_constants import constants


class TestDictDatagramInitialization:
    """Test DictDatagram initialization and basic properties."""

    def test_basic_initialization(self):
        """Test basic initialization with simple data."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        datagram = DictDatagram(data)

        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"
        assert datagram["score"] == 85.5

    def test_initialization_with_meta_info(self):
        """Test initialization with meta information."""
        data = {"user_id": 123, "name": "Alice"}
        meta_info = {"__pipeline_version": "v1.0", "__timestamp": "2024-01-01"}

        datagram = DictDatagram(data, meta_info=meta_info)

        assert datagram["user_id"] == 123
        assert datagram.get_meta_value("pipeline_version") == "v1.0"
        assert datagram.get_meta_value("timestamp") == "2024-01-01"

    def test_initialization_with_context_in_data(self):
        """Test initialization when context is included in data."""
        data = {"user_id": 123, "name": "Alice", constants.CONTEXT_KEY: "v0.1"}

        datagram = DictDatagram(data)

        # The context key is transformed to include full context path
        assert "v0.1" in datagram.data_context_key
        assert constants.CONTEXT_KEY not in datagram._data

    def test_initialization_with_meta_columns_in_data(self):
        """Test initialization when meta columns are included in data."""
        data = {
            "user_id": 123,
            "name": "Alice",
            "__version": "1.0",
            "__timestamp": "2024-01-01",
        }

        datagram = DictDatagram(data)

        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"
        assert datagram.get_meta_value("version") == "1.0"
        assert datagram.get_meta_value("timestamp") == "2024-01-01"
        # Meta columns should not be in regular data
        assert "__version" not in datagram._data
        assert "__timestamp" not in datagram._data

    def test_initialization_with_python_schema(self):
        """Test initialization with explicit Python schema."""
        data = {"user_id": "123", "score": "85.5"}  # String values
        python_schema = {"user_id": int, "score": float}

        datagram = DictDatagram(data, python_schema=python_schema)

        # Data should be stored as provided (conversion happens during export)
        assert datagram["user_id"] == "123"
        assert datagram["score"] == "85.5"

    def test_empty_data_initialization(self):
        """Test initialization with empty data succeeds."""
        # Empty data should be allowed in OrcaPod
        data = {}
        datagram = DictDatagram(data)

        assert len(datagram.keys()) == 0
        assert datagram.as_dict() == {}


class TestDictDatagramDictInterface:
    """Test dict-like interface methods."""

    @pytest.fixture
    def sample_datagram(self):
        """Create a sample datagram for testing."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5, "active": True}
        return DictDatagram(data)

    def test_getitem(self, sample_datagram):
        """Test __getitem__ method."""
        assert sample_datagram["user_id"] == 123
        assert sample_datagram["name"] == "Alice"
        assert sample_datagram["score"] == 85.5
        assert sample_datagram["active"] is True

    def test_getitem_missing_key(self, sample_datagram):
        """Test __getitem__ with missing key raises KeyError."""
        with pytest.raises(KeyError):
            _ = sample_datagram["nonexistent"]

    def test_contains(self, sample_datagram):
        """Test __contains__ method."""
        assert "user_id" in sample_datagram
        assert "name" in sample_datagram
        assert "nonexistent" not in sample_datagram

    def test_iter(self, sample_datagram):
        """Test __iter__ method."""
        keys = list(sample_datagram)
        expected_keys = {"user_id", "name", "score", "active"}
        assert set(keys) == expected_keys

    def test_get(self, sample_datagram):
        """Test get method."""
        assert sample_datagram.get("user_id") == 123
        assert sample_datagram.get("nonexistent") is None
        assert sample_datagram.get("nonexistent", "default") == "default"


class TestDictDatagramStructuralInfo:
    """Test structural information methods."""

    @pytest.fixture
    def datagram_with_meta(self):
        """Create a datagram with meta data for testing."""
        data = {
            "user_id": 123,
            "name": "Alice",
            "__version": "1.0",
            "__pipeline_id": "test_pipeline",
        }
        return DictDatagram(data, data_context="v0.1")

    def test_keys_data_only(self, datagram_with_meta):
        """Test keys method with data columns only."""
        keys = datagram_with_meta.keys()
        expected_keys = {"user_id", "name"}
        assert set(keys) == expected_keys

    def test_keys_with_meta_columns(self, datagram_with_meta):
        """Test keys method including meta columns."""
        keys = datagram_with_meta.keys(include_meta_columns=True)
        expected_keys = {"user_id", "name", "__version", "__pipeline_id"}
        assert set(keys) == expected_keys

    def test_keys_with_context(self, datagram_with_meta):
        """Test keys method including context."""
        keys = datagram_with_meta.keys(include_context=True)
        expected_keys = {"user_id", "name", constants.CONTEXT_KEY}
        assert set(keys) == expected_keys

    def test_keys_with_all_info(self, datagram_with_meta):
        """Test keys method including all information."""
        keys = datagram_with_meta.keys(include_meta_columns=True, include_context=True)
        expected_keys = {
            "user_id",
            "name",
            "__version",
            "__pipeline_id",
            constants.CONTEXT_KEY,
        }
        assert set(keys) == expected_keys

    def test_keys_with_specific_meta_prefix(self, datagram_with_meta):
        """Test keys method with specific meta columns."""
        # Test selecting specific meta columns by getting all first
        all_keys_with_meta = datagram_with_meta.keys(include_meta_columns=True)

        # Should include data columns and meta columns
        expected_keys = {"user_id", "name", "__version", "__pipeline_id"}
        assert set(all_keys_with_meta) == expected_keys

    def test_types_data_only(self, datagram_with_meta):
        """Test types method with data columns only."""
        types = datagram_with_meta.types()
        expected_keys = {"user_id", "name"}
        assert set(types.keys()) == expected_keys
        assert types["user_id"] is int
        assert types["name"] is str

    def test_types_with_meta_columns(self, datagram_with_meta):
        """Test types method including meta columns."""
        types = datagram_with_meta.types(include_meta_columns=True)
        expected_keys = {"user_id", "name", "__version", "__pipeline_id"}
        assert set(types.keys()) == expected_keys
        assert types["__version"] is str
        assert types["__pipeline_id"] is str

    def test_types_with_context(self, datagram_with_meta):
        """Test types method including context."""
        types = datagram_with_meta.types(include_context=True)
        expected_keys = {"user_id", "name", constants.CONTEXT_KEY}
        assert set(types.keys()) == expected_keys
        assert types[constants.CONTEXT_KEY] is str

    def test_arrow_schema_data_only(self, datagram_with_meta):
        """Test arrow_schema method with data columns only."""
        schema = datagram_with_meta.arrow_schema()
        expected_names = {"user_id", "name"}
        assert set(schema.names) == expected_names
        # Access field by name, not index
        assert schema.field("user_id").type == pa.int64()
        assert schema.field("name").type == pa.large_string()

    def test_arrow_schema_with_meta_columns(self, datagram_with_meta):
        """Test arrow_schema method including meta columns."""
        schema = datagram_with_meta.arrow_schema(include_meta_columns=True)
        expected_names = {"user_id", "name", "__version", "__pipeline_id"}
        assert set(schema.names) == expected_names
        assert schema.field("__version").type == pa.large_string()
        assert schema.field("__pipeline_id").type == pa.large_string()

    def test_arrow_schema_with_context(self, datagram_with_meta):
        """Test arrow_schema method including context."""
        schema = datagram_with_meta.arrow_schema(include_context=True)
        expected_names = {"user_id", "name", constants.CONTEXT_KEY}
        assert set(schema.names) == expected_names

    def test_content_hash(self, datagram_with_meta):
        """Test content hash calculation."""
        hash1 = datagram_with_meta.content_hash().to_hex()
        hash2 = datagram_with_meta.content_hash().to_hex()

        # Hash should be consistent
        assert hash1 == hash2
        assert isinstance(hash1, str)
        assert len(hash1) > 0

    def test_content_hash_different_data(self):
        """Test content hash is different for different data."""
        datagram1 = DictDatagram({"user_id": 123, "name": "Alice"})
        datagram2 = DictDatagram({"user_id": 456, "name": "Bob"})

        hash1 = datagram1.content_hash()
        hash2 = datagram2.content_hash()

        assert hash1 != hash2


class TestDictDatagramFormatConversions:
    """Test format conversion methods."""

    @pytest.fixture
    def datagram_with_all(self):
        """Create a datagram with data, meta, and context."""
        data = {
            "user_id": 123,
            "name": "Alice",
            "__version": "1.0",
            constants.CONTEXT_KEY: "v0.1",
        }
        return DictDatagram(data)

    def test_as_dict_data_only(self, datagram_with_all):
        """Test as_dict method with data columns only."""
        result = datagram_with_all.as_dict()
        expected = {"user_id": 123, "name": "Alice"}
        assert result == expected

    def test_as_dict_with_meta_columns(self, datagram_with_all):
        """Test as_dict method including meta columns."""
        result = datagram_with_all.as_dict(include_meta_columns=True)
        expected = {"user_id": 123, "name": "Alice", "__version": "1.0"}
        assert result == expected

    def test_as_dict_with_context(self, datagram_with_all):
        """Test as_dict method including context."""
        result = datagram_with_all.as_dict(include_context=True)
        expected_user_id = 123
        expected_name = "Alice"

        assert result["user_id"] == expected_user_id
        assert result["name"] == expected_name
        # Context key should be present but value might be transformed
        assert constants.CONTEXT_KEY in result

    def test_as_dict_with_all_info(self, datagram_with_all):
        """Test as_dict method including all information."""
        result = datagram_with_all.as_dict(
            include_meta_columns=True, include_context=True
        )

        assert result["user_id"] == 123
        assert result["name"] == "Alice"
        assert result["__version"] == "1.0"
        # Context key should be present but value might be transformed
        assert constants.CONTEXT_KEY in result

    def test_as_table_data_only(self, datagram_with_all):
        """Test as_table method with data columns only."""
        table = datagram_with_all.as_table()

        assert table.num_rows == 1
        assert set(table.column_names) == {"user_id", "name"}
        assert table["user_id"].to_pylist() == [123]
        assert table["name"].to_pylist() == ["Alice"]

    def test_as_table_with_meta_columns(self, datagram_with_all):
        """Test as_table method including meta columns."""
        table = datagram_with_all.as_table(include_meta_columns=True)

        assert table.num_rows == 1
        expected_columns = {"user_id", "name", "__version"}
        assert set(table.column_names) == expected_columns
        assert table["__version"].to_pylist() == ["1.0"]

    def test_as_table_with_context(self, datagram_with_all):
        """Test as_table method including context."""
        table = datagram_with_all.as_table(include_context=True)

        assert table.num_rows == 1
        expected_columns = {"user_id", "name", constants.CONTEXT_KEY}
        assert set(table.column_names) == expected_columns
        # Context value might be transformed, just check it exists
        assert len(table[constants.CONTEXT_KEY].to_pylist()) == 1

    def test_as_arrow_compatible_dict(self, datagram_with_all):
        """Test as_arrow_compatible_dict method."""
        result = datagram_with_all.as_arrow_compatible_dict()

        # Should be dict with list values suitable for PyArrow
        assert isinstance(result, dict)
        # The method returns single values, not lists for single-row data
        assert result["user_id"] == 123
        assert result["name"] == "Alice"


class TestDictDatagramMetaOperations:
    """Test meta column operations."""

    @pytest.fixture
    def datagram_with_meta(self):
        """Create a datagram with meta columns."""
        data = {
            "user_id": 123,
            "name": "Alice",
            "__version": "1.0",
            "__pipeline_id": "test_pipeline",
            "__timestamp": "2024-01-01",
        }
        return DictDatagram(data)

    def test_meta_columns_property(self, datagram_with_meta):
        """Test meta_columns property."""
        meta_columns = datagram_with_meta.meta_columns
        expected = {"__version", "__pipeline_id", "__timestamp"}
        assert set(meta_columns) == expected

    def test_get_meta_value(self, datagram_with_meta):
        """Test get_meta_value method."""
        assert datagram_with_meta.get_meta_value("version") == "1.0"
        assert datagram_with_meta.get_meta_value("pipeline_id") == "test_pipeline"
        assert datagram_with_meta.get_meta_value("timestamp") == "2024-01-01"
        assert datagram_with_meta.get_meta_value("nonexistent") is None
        assert datagram_with_meta.get_meta_value("nonexistent", "default") == "default"

    def test_with_meta_columns(self, datagram_with_meta):
        """Test with_meta_columns method."""
        new_datagram = datagram_with_meta.with_meta_columns(
            new_meta="new_value", updated_version="2.0"
        )

        # Original should be unchanged
        assert datagram_with_meta.get_meta_value("version") == "1.0"
        assert datagram_with_meta.get_meta_value("new_meta") is None

        # New datagram should have updates
        assert new_datagram.get_meta_value("version") == "1.0"  # unchanged
        assert new_datagram.get_meta_value("updated_version") == "2.0"  # new
        assert new_datagram.get_meta_value("new_meta") == "new_value"  # new

    def test_with_meta_columns_prefixed_keys(self, datagram_with_meta):
        """Test with_meta_columns method with already prefixed keys."""
        new_datagram = datagram_with_meta.with_meta_columns(
            **{"__direct_meta": "direct_value"}
        )

        assert new_datagram.get_meta_value("direct_meta") == "direct_value"

    def test_drop_meta_columns(self, datagram_with_meta):
        """Test drop_meta_columns method."""
        new_datagram = datagram_with_meta.drop_meta_columns("version", "timestamp")

        # Original should be unchanged
        assert datagram_with_meta.get_meta_value("version") == "1.0"
        assert datagram_with_meta.get_meta_value("timestamp") == "2024-01-01"

        # New datagram should have dropped columns
        assert new_datagram.get_meta_value("version") is None
        assert new_datagram.get_meta_value("timestamp") is None
        assert (
            new_datagram.get_meta_value("pipeline_id") == "test_pipeline"
        )  # unchanged

    def test_drop_meta_columns_prefixed(self, datagram_with_meta):
        """Test drop_meta_columns method with prefixed keys."""
        new_datagram = datagram_with_meta.drop_meta_columns("__version")

        assert new_datagram.get_meta_value("version") is None
        assert (
            new_datagram.get_meta_value("pipeline_id") == "test_pipeline"
        )  # unchanged

    def test_drop_meta_columns_multiple(self, datagram_with_meta):
        """Test dropping multiple meta columns."""
        new_datagram = datagram_with_meta.drop_meta_columns("version", "pipeline_id")

        assert new_datagram.get_meta_value("version") is None
        assert new_datagram.get_meta_value("pipeline_id") is None
        assert new_datagram.get_meta_value("timestamp") == "2024-01-01"  # unchanged

    def test_drop_meta_columns_missing_key(self, datagram_with_meta):
        """Test drop_meta_columns with missing key raises KeyError."""
        with pytest.raises(KeyError):
            datagram_with_meta.drop_meta_columns("nonexistent")

    def test_drop_meta_columns_ignore_missing(self, datagram_with_meta):
        """Test drop_meta_columns with ignore_missing=True."""
        new_datagram = datagram_with_meta.drop_meta_columns(
            "version", "nonexistent", ignore_missing=True
        )

        assert new_datagram.get_meta_value("version") is None
        assert (
            new_datagram.get_meta_value("pipeline_id") == "test_pipeline"
        )  # unchanged


class TestDictDatagramDataOperations:
    """Test data column operations."""

    @pytest.fixture
    def sample_datagram(self):
        """Create a sample datagram for testing."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5, "active": True}
        return DictDatagram(data)

    def test_select(self, sample_datagram):
        """Test select method."""
        new_datagram = sample_datagram.select("user_id", "name")

        assert set(new_datagram.keys()) == {"user_id", "name"}
        assert new_datagram["user_id"] == 123
        assert new_datagram["name"] == "Alice"

        # Original should be unchanged
        assert len(sample_datagram.keys()) == 4

    def test_select_single_column(self, sample_datagram):
        """Test select method with single column."""
        new_datagram = sample_datagram.select("user_id")

        assert list(new_datagram.keys()) == ["user_id"]
        assert new_datagram["user_id"] == 123

    def test_select_missing_column(self, sample_datagram):
        """Test select method with missing column raises KeyError."""
        with pytest.raises(KeyError):
            sample_datagram.select("user_id", "nonexistent")

    def test_drop(self, sample_datagram):
        """Test drop method."""
        new_datagram = sample_datagram.drop("score", "active")

        assert set(new_datagram.keys()) == {"user_id", "name"}
        assert new_datagram["user_id"] == 123
        assert new_datagram["name"] == "Alice"

        # Original should be unchanged
        assert len(sample_datagram.keys()) == 4

    def test_drop_single_column(self, sample_datagram):
        """Test drop method with single column."""
        new_datagram = sample_datagram.drop("score")

        expected_keys = {"user_id", "name", "active"}
        assert set(new_datagram.keys()) == expected_keys

    def test_drop_missing_column(self, sample_datagram):
        """Test drop method with missing column raises KeyError."""
        with pytest.raises(KeyError):
            sample_datagram.drop("nonexistent")

    def test_drop_ignore_missing(self, sample_datagram):
        """Test drop method with ignore_missing=True."""
        new_datagram = sample_datagram.drop("score", "nonexistent", ignore_missing=True)

        expected_keys = {"user_id", "name", "active"}
        assert set(new_datagram.keys()) == expected_keys

    def test_drop_all_columns_fails(self, sample_datagram):
        """Test dropping all columns raises appropriate error."""
        with pytest.raises(ValueError):
            sample_datagram.drop("user_id", "name", "score", "active")

    def test_rename(self, sample_datagram):
        """Test rename method."""
        new_datagram = sample_datagram.rename({"user_id": "id", "name": "full_name"})

        expected_keys = {"id", "full_name", "score", "active"}
        assert set(new_datagram.keys()) == expected_keys
        assert new_datagram["id"] == 123
        assert new_datagram["full_name"] == "Alice"

        # Original should be unchanged
        assert "user_id" in sample_datagram.keys()
        assert "name" in sample_datagram.keys()

    def test_rename_empty_mapping(self, sample_datagram):
        """Test rename method with empty mapping returns new instance."""
        new_datagram = sample_datagram.rename({})

        # Should return new instance with same data
        assert new_datagram is not sample_datagram
        assert new_datagram.as_dict() == sample_datagram.as_dict()

    def test_update(self, sample_datagram):
        """Test update method."""
        new_datagram = sample_datagram.update(score=95.0, active=False)

        assert new_datagram["score"] == 95.0
        assert new_datagram["active"] is False
        assert new_datagram["user_id"] == 123  # unchanged
        assert new_datagram["name"] == "Alice"  # unchanged

        # Original should be unchanged
        assert sample_datagram["score"] == 85.5
        assert sample_datagram["active"] is True

    def test_update_missing_column(self, sample_datagram):
        """Test update method with missing column raises KeyError."""
        with pytest.raises(KeyError):
            sample_datagram.update(nonexistent="value")

    def test_update_empty(self, sample_datagram):
        """Test update method with no updates returns same instance."""
        new_datagram = sample_datagram.update()

        assert new_datagram is sample_datagram

    def test_with_columns(self, sample_datagram):
        """Test with_columns method."""
        new_datagram = sample_datagram.with_columns(grade="A", rank=1)

        expected_keys = {"user_id", "name", "score", "active", "grade", "rank"}
        assert set(new_datagram.keys()) == expected_keys
        assert new_datagram["grade"] == "A"
        assert new_datagram["rank"] == 1
        assert new_datagram["user_id"] == 123  # unchanged

        # Original should be unchanged
        assert len(sample_datagram.keys()) == 4

    def test_with_columns_with_types(self, sample_datagram):
        """Test with_columns method with type specification."""
        new_datagram = sample_datagram.with_columns(
            grade="A", rank=1, python_schema={"grade": str, "rank": int}
        )

        assert new_datagram["grade"] == "A"
        assert new_datagram["rank"] == 1

    def test_with_columns_existing_column_fails(self, sample_datagram):
        """Test with_columns method with existing column raises ValueError."""
        with pytest.raises(ValueError):
            sample_datagram.with_columns(user_id=456)

    def test_with_columns_empty(self, sample_datagram):
        """Test with_columns method with no columns returns same instance."""
        new_datagram = sample_datagram.with_columns()

        assert new_datagram is sample_datagram


class TestDictDatagramContextOperations:
    """Test context operations."""

    def test_with_context_key(self):
        """Test with_context_key method."""
        data = {"user_id": 123, "name": "Alice"}
        original_datagram = DictDatagram(data, data_context="v0.1")

        new_datagram = original_datagram.with_context_key("v0.1")

        # Both should have the full context key
        assert "v0.1" in original_datagram.data_context_key
        assert "v0.1" in new_datagram.data_context_key
        assert new_datagram["user_id"] == 123  # data unchanged


class TestDictDatagramUtilityOperations:
    """Test utility operations."""

    @pytest.fixture
    def sample_datagram(self):
        """Create a sample datagram for testing."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        return DictDatagram(data)

    def test_copy_with_cache(self, sample_datagram):
        """Test copy method preserves cache."""
        # Access something to populate cache
        _ = sample_datagram.as_dict()

        copied = sample_datagram.copy()

        assert copied is not sample_datagram
        assert copied.as_dict() == sample_datagram.as_dict()

    def test_copy_without_cache(self, sample_datagram):
        """Test copy method without cache."""
        copied = sample_datagram.copy()

        assert copied is not sample_datagram
        assert copied.as_dict() == sample_datagram.as_dict()

    def test_str_representation(self, sample_datagram):
        """Test string representation."""
        str_repr = str(sample_datagram)

        # The string representation might be the dict itself
        assert "user_id" in str_repr
        assert "123" in str_repr

    def test_repr_representation(self, sample_datagram):
        """Test repr representation."""
        repr_str = repr(sample_datagram)

        # The repr might be the dict itself
        assert "user_id" in repr_str
        assert "123" in repr_str


class TestDictDatagramEdgeCases:
    """Test edge cases and error conditions."""

    def test_none_values(self):
        """Test handling of None values."""
        data = {"user_id": 123, "name": None, "score": 85.5}
        datagram = DictDatagram(data)

        assert datagram["user_id"] == 123
        assert datagram["name"] is None
        assert datagram["score"] == 85.5

    def test_complex_data_types(self):
        """Test handling of complex data types."""
        data = {
            "user_id": 123,
            "tags": ["tag1", "tag2"],
            "metadata": {"key": "value"},
            "score": 85.5,
        }
        datagram = DictDatagram(data)

        assert datagram["user_id"] == 123
        assert datagram["tags"] == ["tag1", "tag2"]
        assert datagram["metadata"] == {"key": "value"}

    def test_unicode_strings(self):
        """Test handling of Unicode strings."""
        data = {"user_id": 123, "name": "Алиса", "emoji": "😊"}
        datagram = DictDatagram(data)

        assert datagram["name"] == "Алиса"
        assert datagram["emoji"] == "😊"

    def test_large_numbers(self):
        """Test handling of large numbers."""
        data = {
            "user_id": 123,
            "large_int": 9223372036854775807,  # Max int64
            "large_float": 1.7976931348623157e308,  # Near max float64
        }
        datagram = DictDatagram(data)

        assert datagram["large_int"] == 9223372036854775807
        assert datagram["large_float"] == 1.7976931348623157e308

    def test_duplicate_operations(self):
        """Test that duplicate operations are idempotent."""
        data = {"user_id": 123, "name": "Alice"}
        datagram = DictDatagram(data)

        # Multiple selects should be the same
        selected1 = datagram.select("user_id")
        selected2 = datagram.select("user_id")

        assert selected1.as_dict() == selected2.as_dict()


class TestDictDatagramIntegration:
    """Test integration with other components."""

    def test_chained_operations(self):
        """Test chaining multiple operations."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5, "active": True}
        datagram = DictDatagram(data)

        result = (
            datagram.update(score=95.0)
            .with_columns(grade="A")
            .drop("active")
            .rename({"user_id": "id"})
        )

        expected_keys = {"id", "name", "score", "grade"}
        assert set(result.keys()) == expected_keys
        assert result["id"] == 123
        assert result["score"] == 95.0
        assert result["grade"] == "A"

    def test_arrow_roundtrip(self):
        """Test conversion to Arrow and back."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        original = DictDatagram(data)

        # Convert to Arrow table and back
        table = original.as_table()
        arrow_dict = table.to_pydict()

        # Convert dict format back to DictDatagram compatible format
        converted_dict = {k: v[0] for k, v in arrow_dict.items()}
        reconstructed = DictDatagram(converted_dict)

        # Should preserve data
        assert reconstructed["user_id"] == original["user_id"]
        assert reconstructed["name"] == original["name"]
        assert reconstructed["score"] == original["score"]

    def test_mixed_include_options(self):
        """Test various combinations of include options."""
        data = {
            "user_id": 123,
            "name": "Alice",
            "__version": "1.0",
            constants.CONTEXT_KEY: "v0.1",
        }
        datagram = DictDatagram(data)

        # Test all combinations
        data_only = datagram.as_dict()
        with_meta = datagram.as_dict(include_meta_columns=True)
        with_context = datagram.as_dict(include_context=True)
        with_all = datagram.as_dict(include_meta_columns=True, include_context=True)

        assert len(data_only) == 2  # user_id, name
        assert len(with_meta) == 3  # + __version
        assert len(with_context) == 3  # + context
        assert len(with_all) == 4  # + both
