"""
Comprehensive tests for ArrowDatagram class.

This module tests all functionality of the ArrowDatagram class including:
- Initialization and validation
- Dict-like interface operations
- Structural information methods
- Format conversion methods
- Meta column operations
- Data column operations
- Context operations
- Utility operations
"""
# Verified by Edgar Y. Walker

from typing import cast
import pytest
import pyarrow as pa
from datetime import datetime, date

from orcapod.core.datagrams import ArrowDatagram
from orcapod.contexts.system_constants import constants
from orcapod.protocols.core_protocols import Datagram
from orcapod.protocols.hashing_protocols import ContentHash


class TestArrowDatagramInitialization:
    def test_basic_initialization(self):
        """Test basic initialization with PyArrow table."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )

        datagram = ArrowDatagram(table)

        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"
        assert datagram["score"] == 85.5

    def test_initialization_multiple_rows_fails(self):
        """Test initialization with multiple rows fails."""
        table = pa.Table.from_pydict({"user_id": [123, 456], "name": ["Alice", "Bob"]})

        with pytest.raises(ValueError, match="exactly one row"):
            ArrowDatagram(table)

    def test_initialization_empty_table_fails(self):
        """Test initialization with empty table fails."""
        table = pa.Table.from_pydict({"user_id": [], "name": []})

        with pytest.raises(ValueError, match="exactly one row"):
            ArrowDatagram(table)

    def test_string_type_initialization(self) -> None:
        """Initializing with pa.string() table should yield table with pa.large_string()"""
        table = pa.Table.from_pydict({"name": ["John"]})
        datagram = ArrowDatagram(table)
        # TODO: fix this type annotation mistake in the pyi of pyarrow-stubs
        assert datagram._data_table.schema[0].type == pa.large_string()  # type: ignore

    def test_initialization_with_meta_info(self):
        """Test initialization with meta information."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        meta_info = {"pipeline_version": "v1.0", "timestamp": "2024-01-01"}

        datagram = ArrowDatagram(table, meta_info=meta_info)

        assert datagram["user_id"] == 123
        assert datagram.get_meta_value("pipeline_version") == "v1.0"
        assert datagram.get_meta_value("timestamp") == "2024-01-01"
        assert [
            f"{constants.META_PREFIX}pipeline_version"
            in datagram.as_table(include_meta_columns=True).column_names
        ]

    def test_initialization_with_context_in_table(self):
        """Test initialization when context is included in table."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                constants.CONTEXT_KEY: ["v0.1"],
            }
        )

        datagram = ArrowDatagram(table)

        assert datagram.data_context_key == "std:v0.1:default"
        assert constants.CONTEXT_KEY not in datagram._data_table.column_names

    def test_initialization_with_meta_columns_in_table(self):
        """Test initialization when meta columns are included in table."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.META_PREFIX}version": ["1.0"],
                f"{constants.META_PREFIX}timestamp": ["2024-01-01"],
            }
        )

        datagram = ArrowDatagram(table)

        assert datagram["user_id"] == 123
        assert datagram.get_meta_value("version") == "1.0"
        assert datagram.get_meta_value("timestamp") == "2024-01-01"

    def test_initialization_with_explicit_context(self):
        """Test initialization with explicit data context."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})

        datagram = ArrowDatagram(table, data_context="std:v0.1:default")

        assert datagram.data_context_key == "std:v0.1:default"

    def test_initialization_no_data_columns_fails(self):
        """Test initialization with no data columns fails."""
        table = pa.Table.from_pydict(
            {
                f"{constants.META_PREFIX}version": ["1.0"],
                constants.CONTEXT_KEY: ["std:v0.1:default"],
            }
        )

        with pytest.raises(ValueError, match="at least one data column"):
            ArrowDatagram(table)


class TestArrowDatagramDictInterface:
    """Test dict-like interface operations."""

    @pytest.fixture
    def sample_datagram(self):
        """Create a sample datagram for testing."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                "score": [85.5],
                "active": [True],
                f"{constants.META_PREFIX}version": ["1.0"],
            }
        )
        return ArrowDatagram(table)

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
        # this should not include the meta column
        expected_keys = ["user_id", "name", "score", "active"]
        assert set(keys) == set(expected_keys)

    def test_get(self, sample_datagram):
        """Test get method with and without default."""
        assert sample_datagram.get("user_id") == 123
        assert sample_datagram.get("nonexistent") is None
        assert sample_datagram.get("nonexistent", "default") == "default"


class TestArrowDatagramProtocolAdherance:
    @pytest.fixture
    def basic_datagram(self) -> ArrowDatagram:
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                "score": [85.5],
                "active": [True],
                f"{constants.META_PREFIX}version": ["1.0"],
            }
        )
        return ArrowDatagram(table)

    def test_is_instance_of_datagram(self, basic_datagram):
        # ArrowDatagram should ben an instance of Datagram protocol
        assert isinstance(basic_datagram, Datagram)

        # verify that it is NOT possible to check for inheritance
        with pytest.raises(TypeError):
            issubclass(ArrowDatagram, Datagram)


class TestArrowDatagramStructuralInfo:
    """Test structural information methods."""

    @pytest.fixture
    def datagram_with_meta(self):
        """Create a datagram with meta columns."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.META_PREFIX}version": ["1.0"],
                f"{constants.META_PREFIX}pipeline_id": ["test_pipeline"],
            }
        )
        return ArrowDatagram(table)

    def test_keys_data_only(self, datagram_with_meta):
        """Test keys method with data columns only."""
        keys = datagram_with_meta.keys()
        expected = ("user_id", "name")
        assert set(keys) == set(expected)

    def test_keys_with_meta_columns(self, datagram_with_meta):
        """Test keys method including meta columns."""
        keys = datagram_with_meta.keys(include_meta_columns=True)
        expected = (
            "user_id",
            "name",
            f"{constants.META_PREFIX}version",
            f"{constants.META_PREFIX}pipeline_id",
        )
        assert set(keys) == set(expected)

    def test_keys_with_context(self, datagram_with_meta):
        """Test keys method including context."""
        keys = datagram_with_meta.keys(include_context=True)
        expected = ("user_id", "name", constants.CONTEXT_KEY)
        assert set(keys) == set(expected)

    def test_keys_with_all_info(self, datagram_with_meta):
        """Test keys method including all information."""
        keys = datagram_with_meta.keys(include_all_info=True)
        expected = (
            "user_id",
            "name",
            f"{constants.META_PREFIX}version",
            f"{constants.META_PREFIX}pipeline_id",
            constants.CONTEXT_KEY,
        )
        assert set(keys) == set(expected)

    def test_keys_with_specific_meta_prefix(self, datagram_with_meta):
        """Test keys method with specific meta column prefixes."""
        keys = datagram_with_meta.keys(
            include_meta_columns=[f"{constants.META_PREFIX}version"]
        )
        expected = ("user_id", "name", f"{constants.META_PREFIX}version")
        assert set(keys) == set(expected)

    def test_keys_with_nonexistent_meta_prefix(self, datagram_with_meta):
        """Test keys methods when called with non-existent meta column prefixes"""
        # non-existing prefix should be ignored
        keys = datagram_with_meta.keys(
            include_meta_columns=[
                f"{constants.META_PREFIX}nonexistent",
                f"{constants.META_PREFIX}version",
            ]
        )
        expected = ("user_id", "name", f"{constants.META_PREFIX}version")
        assert set(keys) == set(expected)

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
        expected_keys = {
            "user_id",
            "name",
            f"{constants.META_PREFIX}version",
            f"{constants.META_PREFIX}pipeline_id",
        }
        assert set(types.keys()) == expected_keys

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

    def test_arrow_schema_with_meta_columns(self, datagram_with_meta):
        """Test arrow_schema method including meta columns."""
        schema = datagram_with_meta.arrow_schema(include_meta_columns=True)
        expected_names = {
            "user_id",
            "name",
            f"{constants.META_PREFIX}version",
            f"{constants.META_PREFIX}pipeline_id",
        }
        assert set(schema.names) == expected_names

    def test_arrow_schema_with_context(self, datagram_with_meta):
        """Test arrow_schema method including context."""
        schema = datagram_with_meta.arrow_schema(include_context=True)
        expected_names = {"user_id", "name", constants.CONTEXT_KEY}
        assert set(schema.names) == expected_names

    def test_content_hash(self, datagram_with_meta):
        """Test content hash calculation."""
        hash1 = datagram_with_meta.content_hash()
        hash2 = datagram_with_meta.content_hash()

        # Hash should be consistent
        assert hash1 == hash2
        assert isinstance(hash1, ContentHash)
        assert len(hash1.digest) > 0

    def test_content_hash_same_data_different_meta_data(self):
        """Test that the content hash is the same for identical data with different meta data."""
        table1 = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                "__version": ["1.0"],
                "__pipeline_id": ["pipeline_1"],
            }
        )
        table2 = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                "__version": ["1.1"],
                "__pipeline_id": ["pipeline_2"],
            }
        )
        datagram1 = ArrowDatagram(table1)
        datagram2 = ArrowDatagram(table2)
        hash1 = datagram1.content_hash()
        hash2 = datagram2.content_hash()

        assert hash1 == hash2

    def test_content_hash_different_data(self):
        """Test that different data produces different hashes."""
        table1 = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        table2 = pa.Table.from_pydict({"user_id": [456], "name": ["Bob"]})

        datagram1 = ArrowDatagram(table1)
        datagram2 = ArrowDatagram(table2)

        hash1 = datagram1.content_hash()
        hash2 = datagram2.content_hash()

        assert hash1 != hash2


class TestArrowDatagramFormatConversions:
    """Test format conversion methods."""

    @pytest.fixture
    def datagram_with_all(self):
        """Create a datagram with data, meta, and context."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.META_PREFIX}version": ["1.0"],
                constants.CONTEXT_KEY: ["std:v0.1:default"],
            }
        )
        return ArrowDatagram(table)

    def test_as_dict_data_only(self, datagram_with_all):
        """Test as_dict method with data columns only."""
        result = datagram_with_all.as_dict()
        expected = {"user_id": 123, "name": "Alice"}
        assert result == expected

    def test_as_dict_with_meta_columns(self, datagram_with_all):
        """Test as_dict method including meta columns."""
        result = datagram_with_all.as_dict(include_meta_columns=True)
        expected = {
            "user_id": 123,
            "name": "Alice",
            f"{constants.META_PREFIX}version": "1.0",
        }
        assert result == expected

    def test_as_dict_with_context(self, datagram_with_all):
        """Test as_dict method including context."""
        result = datagram_with_all.as_dict(include_context=True)
        expected = {
            "user_id": 123,
            "name": "Alice",
            constants.CONTEXT_KEY: "std:v0.1:default",
        }
        assert result == expected

    def test_as_dict_with_all_info(self, datagram_with_all):
        """Test as_dict method including all information."""
        result = datagram_with_all.as_dict(include_all_info=True)
        all_placed = datagram_with_all.as_dict(
            include_meta_columns=True, include_context=True
        )
        expected = {
            "user_id": 123,
            "name": "Alice",
            f"{constants.META_PREFIX}version": "1.0",
            constants.CONTEXT_KEY: "std:v0.1:default",
        }
        assert result == expected
        assert result == all_placed

    def test_as_table_data_only(self, datagram_with_all):
        """Test as_table method with data columns only."""
        table = datagram_with_all.as_table()

        assert len(table) == 1
        assert set(table.column_names) == {"user_id", "name"}
        assert table["user_id"].to_pylist()[0] == 123
        assert table["name"].to_pylist()[0] == "Alice"

    def test_as_table_with_meta_columns(self, datagram_with_all):
        """Test as_table method including meta columns."""
        table = datagram_with_all.as_table(include_meta_columns=True)

        assert len(table) == 1
        expected_columns = {"user_id", "name", f"{constants.META_PREFIX}version"}
        assert set(table.column_names) == expected_columns
        assert table[f"{constants.META_PREFIX}version"].to_pylist() == ["1.0"]

    def test_as_table_with_context(self, datagram_with_all):
        """Test as_table method including context."""
        table = datagram_with_all.as_table(include_context=True)

        assert len(table) == 1
        expected_columns = {"user_id", "name", constants.CONTEXT_KEY}
        assert set(table.column_names) == expected_columns
        assert table[constants.CONTEXT_KEY].to_pylist() == ["std:v0.1:default"]

    def test_as_arrow_compatible_dict(self, datagram_with_all):
        """Test as_arrow_compatible_dict method."""
        result = datagram_with_all.as_arrow_compatible_dict()
        # TODO: add test case including complex data types

        # Should have same keys as as_dict
        dict_result = datagram_with_all.as_dict()
        assert set(result.keys()) == set(dict_result.keys())


class TestArrowDatagramMetaOperations:
    """Test meta column operations."""

    @pytest.fixture
    def datagram_with_meta(self):
        """Create a datagram with meta columns."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.META_PREFIX}version": ["1.0"],
                f"{constants.META_PREFIX}pipeline_id": ["test"],
            }
        )
        return ArrowDatagram(table)

    def test_meta_columns_property(self, datagram_with_meta):
        """Test meta_columns property."""
        meta_cols = datagram_with_meta.meta_columns
        expected = (
            f"{constants.META_PREFIX}version",
            f"{constants.META_PREFIX}pipeline_id",
        )
        assert set(meta_cols) == set(expected)

    def test_get_meta_value(self, datagram_with_meta):
        """Test get_meta_value method."""
        # With prefix
        assert (
            datagram_with_meta.get_meta_value(f"{constants.META_PREFIX}version")
            == "1.0"
        )

        # Without prefix
        assert datagram_with_meta.get_meta_value("version") == "1.0"

        # With default
        assert datagram_with_meta.get_meta_value("nonexistent", "default") == "default"

    def test_with_meta_columns(self, datagram_with_meta):
        """Test with_meta_columns method."""
        updated = datagram_with_meta.with_meta_columns(
            version="2.0",  # Update existing
            new_meta=3.5,  # Add new
        )

        # Original should be unchanged
        assert datagram_with_meta.get_meta_value("version") == "1.0"

        # Updated should have new values
        assert updated.get_meta_value("version") == "2.0"
        assert updated.get_meta_value("new_meta") == 3.5

        # meta data should be available as meta-prefixed column
        table_with_meta = updated.as_table(include_meta_columns=True)
        assert table_with_meta[f"{constants.META_PREFIX}version"].to_pylist() == ["2.0"]
        assert table_with_meta[f"{constants.META_PREFIX}new_meta"].to_pylist() == [3.5]

        assert (
            table_with_meta[f"{constants.META_PREFIX}version"].type == pa.large_string()
        )

        # Data should be preserved
        assert updated["user_id"] == 123
        assert updated["name"] == "Alice"

    def test_with_meta_columns_prefixed_keys(self, datagram_with_meta):
        """Test with_meta_columns method with prefixed keys."""
        updated = datagram_with_meta.with_meta_columns(
            **{f"{constants.META_PREFIX}version": "2.0"}
        )

        assert updated.get_meta_value("version") == "2.0"

    def test_drop_meta_columns(self, datagram_with_meta):
        """Test drop_meta_columns method."""
        updated = datagram_with_meta.drop_meta_columns("version")

        # Original should be unchanged
        assert datagram_with_meta.get_meta_value("version") == "1.0"

        # Updated should not have dropped other  metadata columns
        assert updated.get_meta_value("version") is None
        assert updated.get_meta_value("pipeline_id") == "test"

        # Data should be preserved
        assert updated["user_id"] == 123

    def test_drop_meta_columns_prefixed(self, datagram_with_meta):
        """Test drop_meta_columns method with prefixed keys."""
        updated = datagram_with_meta.drop_meta_columns(
            f"{constants.META_PREFIX}version"
        )

        assert updated.get_meta_value("version") is None

    def test_drop_meta_columns_multiple(self, datagram_with_meta):
        """Test dropping multiple meta columns."""
        updated = datagram_with_meta.drop_meta_columns("version", "pipeline_id")

        # original should not be modified
        assert datagram_with_meta.get_meta_value("version") == "1.0"
        assert datagram_with_meta.get_meta_value("pipeline_id") == "test"

        assert updated.get_meta_value("version") is None
        assert updated.get_meta_value("pipeline_id") is None

        # Data should be preserved
        assert updated["user_id"] == 123

    def test_drop_meta_columns_missing_key(self, datagram_with_meta):
        """Test drop_meta_columns with missing key raises KeyError."""
        with pytest.raises(KeyError):
            datagram_with_meta.drop_meta_columns("nonexistent")

    def test_drop_meta_columns_ignore_missing(self, datagram_with_meta):
        """Test drop_meta_columns with ignore_missing=True."""
        updated = datagram_with_meta.drop_meta_columns(
            "version", "nonexistent", ignore_missing=True
        )

        assert updated.get_meta_value("version") is None
        assert updated.get_meta_value("pipeline_id") == "test"


class TestArrowDatagramDataOperations:
    """Test data column operations."""

    @pytest.fixture
    def sample_datagram(self):
        """Create a sample datagram for testing."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                "score": [85.5],
                "active": [True],
                f"{constants.META_PREFIX}version": ["1.0"],
                f"{constants.META_PREFIX}pipeline_id": ["test"],
            }
        )
        return ArrowDatagram(table)

    def test_select(self, sample_datagram: ArrowDatagram):
        """Test select method."""
        selected = sample_datagram.select("user_id", "name")

        assert set(selected.keys()) == {"user_id", "name"}
        assert selected["user_id"] == 123
        assert selected["name"] == "Alice"
        # meta values should be copied over
        assert selected.get_meta_value("version") == "1.0"
        assert selected.get_meta_value("pipeline_id") == "test"

        # context should be preserved
        assert selected.data_context_key == sample_datagram.data_context_key

        # Original should be unchanged
        assert set(sample_datagram.keys()) == {"user_id", "name", "score", "active"}

    def test_select_single_column(self, sample_datagram: ArrowDatagram):
        """Test select method with single column."""
        selected = sample_datagram.select("user_id")

        assert set(selected.keys()) == {"user_id"}
        assert selected["user_id"] == 123

    def test_select_missing_column(self, sample_datagram):
        """Test select method with missing column raises ValueError."""
        with pytest.raises(ValueError):
            sample_datagram.select("user_id", "nonexistent")

    def test_drop(self, sample_datagram: ArrowDatagram):
        """Test drop method."""
        dropped = sample_datagram.drop("score", "active")

        assert set(dropped.keys()) == {"user_id", "name"}
        assert dropped["user_id"] == 123
        assert dropped["name"] == "Alice"

        # drop should preserve context and meta values
        assert dropped.get_meta_value("version") == "1.0"
        assert dropped.get_meta_value("pipeline_id") == "test"
        assert dropped.data_context_key == sample_datagram.data_context_key

        # Original should be unchanged
        assert set(sample_datagram.keys()) == {"user_id", "name", "score", "active"}

    def test_drop_single_column(self, sample_datagram: ArrowDatagram):
        """Test drop method with single column."""
        dropped = sample_datagram.drop("score")
        # drop should preserve context and meta values
        assert dropped.get_meta_value("version") == "1.0"
        assert dropped.get_meta_value("pipeline_id") == "test"
        assert dropped.data_context_key == sample_datagram.data_context_key

        assert set(dropped.keys()) == {"user_id", "name", "active"}

    def test_drop_missing_column(self, sample_datagram: ArrowDatagram):
        """Test drop method with missing column raises KeyError."""
        with pytest.raises(KeyError):
            sample_datagram.drop("nonexistent")

    def test_drop_ignore_missing(self, sample_datagram: ArrowDatagram):
        """Test drop method with ignore_missing=True."""
        dropped = sample_datagram.drop("score", "nonexistent", ignore_missing=True)

        assert set(dropped.keys()) == {"user_id", "name", "active"}

    def test_rename(self, sample_datagram: ArrowDatagram):
        """Test rename method."""
        renamed = sample_datagram.rename({"user_id": "id", "name": "username"})

        expected_keys = {"id", "username", "score", "active"}
        assert set(renamed.keys()) == expected_keys
        assert renamed["id"] == 123
        assert renamed["username"] == "Alice"
        assert renamed["score"] == 85.5

        # meta and context should be unaffected
        assert renamed.get_meta_value("version") == "1.0"
        assert renamed.get_meta_value("pipeline_id") == "test"
        assert renamed.data_context_key == sample_datagram.data_context_key

        # Original should be unchanged
        assert "user_id" in sample_datagram
        assert "id" not in sample_datagram

    def test_rename_empty_mapping(self, sample_datagram: ArrowDatagram):
        """Test rename method with empty mapping."""
        renamed = sample_datagram.rename({})

        # Should be identical
        assert set(renamed.keys()) == set(sample_datagram.keys())
        assert renamed["user_id"] == sample_datagram["user_id"]

    def test_update(self, sample_datagram: ArrowDatagram):
        """Test update method."""
        updated = sample_datagram.update(score=95.0, active=False)

        # Original should be unchanged
        assert sample_datagram["score"] == 85.5
        assert sample_datagram["active"] is True

        # Updated should have new values
        assert updated["score"] == 95.0
        assert not updated["active"]
        assert updated["user_id"] == 123  # Unchanged columns preserved

    def test_update_missing_column(self, sample_datagram: ArrowDatagram):
        """Test update method with missing column raises KeyError."""
        with pytest.raises(KeyError):
            sample_datagram.update(nonexistent="value")

    def test_update_empty(self, sample_datagram: ArrowDatagram):
        """Test update method with no updates returns same instance."""
        updated = sample_datagram.update()

        # Should return the same instance
        # TODO: reconsider if this behavior is what is specified by the protocol
        assert updated is sample_datagram

    def test_with_columns(self, sample_datagram: ArrowDatagram):
        """Test with_columns method."""
        new_datagram = sample_datagram.with_columns(
            department="Engineering", salary=75000
        )

        # Original should be unchanged
        assert "department" not in sample_datagram
        assert "salary" not in sample_datagram

        # New datagram should have additional columns
        expected_keys = {"user_id", "name", "score", "active", "department", "salary"}
        assert set(new_datagram.keys()) == expected_keys
        assert new_datagram["department"] == "Engineering"
        assert new_datagram["salary"] == 75000

    def test_with_columns_with_types(self, sample_datagram: ArrowDatagram):
        """Test with_columns method with explicit types."""
        new_datagram = sample_datagram.with_columns(
            column_types={"salary": int, "rate": float}, salary=75000, rate=85.5
        )

        types = new_datagram.types()
        assert types["salary"] is int
        assert types["rate"] is float

    def test_with_columns_existing_column_fails(self, sample_datagram):
        """Test with_columns method with existing column raises ValueError."""
        with pytest.raises(ValueError):
            sample_datagram.with_columns(user_id=456)

    def test_with_columns_empty(self, sample_datagram):
        """Test with_columns method with no columns returns same instance."""
        new_datagram = sample_datagram.with_columns()

        # TODO: again consider if this behavior is what's specified by protocol
        assert new_datagram is sample_datagram


class TestArrowDatagramContextOperations:
    """Test context operations."""

    def test_with_context_key(self):
        """Test with_context_key method."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        original_datagram = ArrowDatagram(table, data_context="std:v0.1:default")

        new_datagram = original_datagram.with_context_key("std:v0.1:default")

        # Original should be unchanged
        assert original_datagram.data_context_key == "std:v0.1:default"

        # New should have updated context
        assert new_datagram.data_context_key == "std:v0.1:default"

        # Data should be preserved
        assert new_datagram["user_id"] == 123
        assert new_datagram["name"] == "Alice"


class TestArrowDatagramUtilityOperations:
    """Test utility operations."""

    @pytest.fixture
    def sample_datagram(self):
        """Create a sample datagram for testing."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.META_PREFIX}version": ["1.0"],
            }
        )
        return ArrowDatagram(table)

    def test_copy_with_cache(self, sample_datagram):
        """Test copy method with cache included."""
        # Force cache creation
        _ = sample_datagram.as_dict()

        copied = sample_datagram.copy(include_cache=True)

        # Should be different instances
        assert copied is not sample_datagram

        # Should have same data
        assert copied["user_id"] == sample_datagram["user_id"]
        assert copied["name"] == sample_datagram["name"]

        # Should share cached values
        assert copied._cached_python_dict is sample_datagram._cached_python_dict

    def test_copy_without_cache(self, sample_datagram):
        """Test copy method without cache."""
        # Force cache creation
        _ = sample_datagram.as_dict()

        copied = sample_datagram.copy(include_cache=False)

        # Should be different instances
        assert copied is not sample_datagram

        # Should have same data
        assert copied["user_id"] == sample_datagram["user_id"]

        # Should not share cached values
        assert copied._cached_python_dict is None

    def test_str_representation(self, sample_datagram):
        """Test string representation."""
        str_repr = str(sample_datagram)

        # Should contain data values
        assert "123" in str_repr
        assert "Alice" in str_repr

        # Should not contain meta columns
        assert f"{constants.META_PREFIX}version" not in str_repr

    def test_repr_representation(self, sample_datagram):
        """Test repr representation."""
        repr_str = repr(sample_datagram)

        # Should contain data values
        assert "123" in repr_str
        assert "Alice" in repr_str


class TestArrowDatagramEdgeCases:
    """Test edge cases and error conditions."""

    def test_none_values(self):
        """Test handling of None values."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": [None], "optional": [None]}
        )
        datagram = ArrowDatagram(table)

        assert datagram["user_id"] == 123
        assert datagram["name"] is None
        assert datagram["optional"] is None

    def test_complex_data_types(self):
        """Test handling of complex Arrow data types."""
        # Create table with various Arrow types
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array(["Alice"], type=pa.string()),
                pa.array([85.5], type=pa.float64()),
                pa.array([True], type=pa.bool_()),
                pa.array([[1, 2, 3]], type=pa.list_(pa.int32())),
            ],
            names=["id", "name", "score", "active", "numbers"],
        )

        datagram = ArrowDatagram(table)

        assert datagram["id"] == 123
        assert datagram["name"] == "Alice"
        assert datagram["score"] == 85.5
        assert datagram["active"] is True
        assert datagram["numbers"] == [1, 2, 3]

    def test_large_string_types(self):
        """Test handling of large string types."""
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array(["A very long string " * 100], type=pa.large_string()),
            ],
            names=["id", "text"],
        )

        datagram = ArrowDatagram(table)

        assert datagram["id"] == 123
        assert len(cast(str, datagram["text"])) > 1000

    def test_timestamp_types(self):
        """Test handling of timestamp types."""
        now = datetime.now()
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([now], type=pa.timestamp("ns")),
            ],
            names=["id", "timestamp"],
        )

        datagram = ArrowDatagram(table)

        assert datagram["id"] == 123
        # Arrow timestamps are returned as pandas Timestamp objects
        assert datagram["timestamp"] is not None

    def test_date_types(self):
        """Test handling of date types."""
        today = date.today()
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([today], type=pa.date32()),
            ],
            names=["id", "date"],
        )

        datagram = ArrowDatagram(table)

        assert datagram["id"] == 123
        assert datagram["date"] is not None

    def test_duplicate_operations(self):
        """Test operations that shouldn't change anything."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        datagram = ArrowDatagram(table)

        # Select all columns
        selected = datagram.select("user_id", "name")
        assert set(selected.keys()) == set(datagram.keys())

        # Update with same values
        updated = datagram.update(user_id=123, name="Alice")
        assert updated["user_id"] == datagram["user_id"]
        assert updated["name"] == datagram["name"]

        # Rename with identity mapping
        renamed = datagram.rename({"user_id": "user_id", "name": "name"})
        assert set(renamed.keys()) == set(datagram.keys())

    def test_conversion_to_large_types(self):
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int8()),
                pa.array(["A very long string " * 100], type=pa.string()),
            ],
            names=["id", "text"],
        )

        datagram = ArrowDatagram(table)

        returned_table = datagram.as_table()

        # integer should be preserved but string should become large_string
        assert returned_table["id"].type == pa.int8()
        assert returned_table["text"].type == pa.large_string()


class TestArrowDatagramIntegration:
    """Test integration between different operations."""

    def test_chained_operations(self):
        """Test chaining multiple operations."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "first_name": ["Alice"],
                "last_name": ["Smith"],
                "score": [85.5],
                "active": [True],
                f"{constants.META_PREFIX}version": ["1.0"],
            }
        )
        original_keys = set(table.column_names) - {f"{constants.META_PREFIX}version"}

        datagram = ArrowDatagram(table)

        # Chain operations
        result = (
            datagram.with_columns(full_name="Alice Smith")
            .drop("first_name", "last_name")
            .update(score=90.0)
            .with_meta_columns(version="2.0")
        )

        # verify original is not modified
        assert set(datagram.keys()) == original_keys
        assert datagram["first_name"] == "Alice"
        assert datagram["score"] == 85.5

        # Verify final state
        assert set(result.keys()) == {"user_id", "score", "active", "full_name"}
        assert result["full_name"] == "Alice Smith"
        assert result["score"] == 90.0
        assert result.get_meta_value("version") == "2.0"

    def test_dict_roundtrip(self):
        """Test conversion to dict and back preserves data."""

        # TODO: perform this test but using semantic types

        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )
        original = ArrowDatagram(table)

        # Convert to dict
        data_dict = original.as_dict()

        # Create new table from dict
        new_table = pa.Table.from_pylist([data_dict])
        reconstructed = ArrowDatagram(new_table)

        # Should have same data
        assert reconstructed["user_id"] == original["user_id"]
        assert reconstructed["name"] == original["name"]
        assert reconstructed["score"] == original["score"]

    def test_mixed_include_options(self):
        """Test various combinations of include options."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.META_PREFIX}version": ["1.0"],
                f"{constants.META_PREFIX}pipeline": ["test"],
            }
        )

        datagram = ArrowDatagram(table)

        # Test different combinations
        dict1 = datagram.as_dict(include_meta_columns=True, include_context=True)
        dict2 = datagram.as_dict(include_all_info=True)

        # Should be equivalent
        assert dict1 == dict2

        # Test specific meta prefixes
        dict3 = datagram.as_dict(
            include_meta_columns=[f"{constants.META_PREFIX}version"]
        )
        expected_keys = {"user_id", "name", f"{constants.META_PREFIX}version"}
        assert set(dict3.keys()) == expected_keys

    def test_arrow_table_schema_preservation(self):
        """Test that Arrow table schemas are preserved through operations."""
        # Create table with specific Arrow types
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int32()),  # Specific int type
                pa.array(["Alice"], type=pa.large_string()),  # Large string
                pa.array([85.5], type=pa.float32()),  # Specific float type
            ],
            names=["id", "name", "score"],
        )

        datagram = ArrowDatagram(table)

        # Get schema
        schema = datagram.arrow_schema()

        # Types should be preserved
        assert schema.field("id").type == pa.int32()
        assert schema.field("name").type == pa.large_string()
        assert schema.field("score").type == pa.float32()

        # Operations should preserve types
        updated = datagram.update(score=90.0)
        updated_schema = updated.arrow_schema()
        assert updated_schema.field("score").type == pa.float32()


class TestArrowDatagramPerformance:
    """Test performance-related aspects."""

    def test_caching_behavior(self):
        """Test that caching works as expected."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        datagram = ArrowDatagram(table)

        # First call should populate cache
        dict1 = datagram.as_dict()
        assert datagram._cached_python_dict is not None
        cached_dict_id = id(datagram._cached_python_dict)

        # Second call should use same cache (not create new one)
        dict2 = datagram.as_dict()
        assert id(datagram._cached_python_dict) == cached_dict_id  # Same cached object
        # Returned dicts are copies for safety, so they're not identical
        assert dict1 == dict2  # Same content
        assert dict1 is not dict2  # Different objects (copies)

        # Operations should invalidate cache
        updated = datagram.update(name="Bob")
        assert updated._cached_python_dict is None

    def test_lazy_evaluation(self):
        """Test that expensive operations are performed lazily."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        datagram = ArrowDatagram(table)

        # Hash should not be calculated until requested
        assert datagram._cached_content_hash is None

        # First hash call should calculate
        hash1 = datagram.content_hash()
        assert datagram._cached_content_hash is not None

        # Second call should use cache
        hash2 = datagram.content_hash()
        assert hash1 == hash2
        assert hash1 is hash2  # Should be same object
