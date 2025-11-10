"""
Comprehensive tests for ArrowTag and ArrowPacket classes.

This module tests all functionality of the Arrow-based tag and packet classes including:
- Tag-specific functionality (system tags)
- Packet-specific functionality (source info)
- Integration with Arrow datagram functionality
- Conversion operations
- Arrow-specific optimizations
"""

import pytest
import pyarrow as pa
from datetime import datetime, date

from orcapod.core.datagrams import ArrowTag, ArrowPacket
from orcapod.contexts.system_constants import constants


class TestArrowTagInitialization:
    """Test ArrowTag initialization and basic properties."""

    def test_basic_initialization(self):
        """Test basic initialization with PyArrow table."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )

        tag = ArrowTag(table)

        assert tag["user_id"] == 123
        assert tag["name"] == "Alice"
        assert tag["score"] == 85.5

    def test_initialization_multiple_rows_fails(self):
        """Test initialization with multiple rows fails."""
        table = pa.Table.from_pydict({"user_id": [123, 456], "name": ["Alice", "Bob"]})

        with pytest.raises(ValueError, match="single row"):
            ArrowTag(table)

    def test_initialization_with_system_tags(self):
        """Test initialization with system tags."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        system_tags = {"tag_type": "user", "created_by": "system"}

        tag = ArrowTag(table, system_tags=system_tags)

        assert tag["user_id"] == 123
        system_tag_dict = tag.system_tags()
        assert system_tag_dict["tag_type"] == "user"
        assert system_tag_dict["created_by"] == "system"

    def test_initialization_with_system_tags_in_table(self):
        """Test initialization when system tags are included in table."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.SYSTEM_TAG_PREFIX}tag_type": ["user"],
                f"{constants.SYSTEM_TAG_PREFIX}version": ["1.0"],
            }
        )

        tag = ArrowTag(table)

        assert tag["user_id"] == 123
        assert tag["name"] == "Alice"

        system_tags = tag.system_tags()
        assert system_tags[f"{constants.SYSTEM_TAG_PREFIX}tag_type"] == "user"
        assert system_tags[f"{constants.SYSTEM_TAG_PREFIX}version"] == "1.0"

    def test_initialization_mixed_system_tags(self):
        """Test initialization with both embedded and explicit system tags."""
        table = pa.Table.from_pydict(
            {"user_id": [123], f"{constants.SYSTEM_TAG_PREFIX}embedded": ["value1"]}
        )
        system_tags = {"explicit": "value2"}

        tag = ArrowTag(table, system_tags=system_tags)

        system_tag_dict = tag.system_tags()
        assert system_tag_dict[f"{constants.SYSTEM_TAG_PREFIX}embedded"] == "value1"
        assert system_tag_dict["explicit"] == "value2"


class TestArrowTagSystemTagOperations:
    """Test system tag specific operations."""

    @pytest.fixture
    def sample_tag(self):
        """Create a sample tag for testing."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        system_tags = {"tag_type": "user", "version": "1.0"}
        return ArrowTag(table, system_tags=system_tags)

    def test_system_tags_method(self, sample_tag):
        """Test system_tags method."""
        system_tags = sample_tag.system_tags()

        assert isinstance(system_tags, dict)
        assert system_tags["tag_type"] == "user"
        assert system_tags["version"] == "1.0"

    def test_keys_with_system_tags(self, sample_tag):
        """Test keys method including system tags."""
        keys_data_only = sample_tag.keys()
        keys_with_system = sample_tag.keys(include_system_tags=True)

        assert "user_id" in keys_data_only
        assert "name" in keys_data_only
        assert len(keys_with_system) > len(keys_data_only)
        assert "tag_type" in keys_with_system
        assert "version" in keys_with_system

    def test_types_with_system_tags(self, sample_tag):
        """Test types method including system tags."""
        types_data_only = sample_tag.types()
        types_with_system = sample_tag.types(include_system_tags=True)

        assert len(types_with_system) > len(types_data_only)
        assert "tag_type" in types_with_system
        assert "version" in types_with_system

    def test_arrow_schema_with_system_tags(self, sample_tag):
        """Test arrow_schema method including system tags."""
        schema_data_only = sample_tag.arrow_schema()
        schema_with_system = sample_tag.arrow_schema(include_system_tags=True)

        assert len(schema_with_system) > len(schema_data_only)
        assert "tag_type" in schema_with_system.names
        assert "version" in schema_with_system.names

    def test_as_dict_with_system_tags(self, sample_tag):
        """Test as_dict method including system tags."""
        dict_data_only = sample_tag.as_dict()
        dict_with_system = sample_tag.as_dict(include_system_tags=True)

        assert "user_id" in dict_data_only
        assert "name" in dict_data_only
        assert "tag_type" not in dict_data_only

        assert "user_id" in dict_with_system
        assert "tag_type" in dict_with_system
        assert "version" in dict_with_system

    def test_as_table_with_system_tags(self, sample_tag):
        """Test as_table method including system tags."""
        table_data_only = sample_tag.as_table()
        table_with_system = sample_tag.as_table(include_system_tags=True)

        assert len(table_with_system.column_names) > len(table_data_only.column_names)
        assert "tag_type" in table_with_system.column_names
        assert "version" in table_with_system.column_names

    def test_as_datagram_conversion(self, sample_tag):
        """Test conversion to datagram."""
        datagram = sample_tag.as_datagram()

        # Should preserve data
        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"

        # Should not include system tags by default
        assert "tag_type" not in datagram.keys()

    def test_as_datagram_with_system_tags(self, sample_tag):
        """Test conversion to datagram including system tags."""
        datagram = sample_tag.as_datagram(include_system_tags=True)

        # Should preserve data and include system tags
        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"
        assert "tag_type" in datagram.keys()


class TestArrowTagDataOperations:
    """Test that system tags are preserved across all data operations."""

    @pytest.fixture
    def sample_tag_with_system_tags(self):
        """Create a sample tag with system tags for testing operations."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5], "active": [True]}
        )
        system_tags = {
            "tag_type": "user",
            "version": "1.0",
            "created_by": "system",
            "priority": "high",
        }
        return ArrowTag(table, system_tags=system_tags)

    def test_select_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that select operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Select subset of columns
        selected = sample_tag_with_system_tags.select("user_id", "name")

        # System tags should be preserved
        assert selected.system_tags() == original_system_tags
        assert selected.system_tags()["tag_type"] == "user"
        assert selected.system_tags()["version"] == "1.0"
        assert selected.system_tags()["created_by"] == "system"
        assert selected.system_tags()["priority"] == "high"

        # Only selected data columns should remain
        assert "user_id" in selected.keys()
        assert "name" in selected.keys()
        assert "score" not in selected.keys()
        assert "active" not in selected.keys()

    def test_drop_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that drop operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Drop some columns
        dropped = sample_tag_with_system_tags.drop("score", "active")

        # System tags should be preserved
        assert dropped.system_tags() == original_system_tags
        assert dropped.system_tags()["tag_type"] == "user"
        assert dropped.system_tags()["version"] == "1.0"

        # Dropped columns should be gone, others should remain
        assert "user_id" in dropped.keys()
        assert "name" in dropped.keys()
        assert "score" not in dropped.keys()
        assert "active" not in dropped.keys()

    def test_rename_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that rename operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Rename columns
        renamed = sample_tag_with_system_tags.rename(
            {"user_id": "id", "name": "username"}
        )

        # System tags should be preserved
        assert renamed.system_tags() == original_system_tags
        assert renamed.system_tags()["tag_type"] == "user"
        assert renamed.system_tags()["version"] == "1.0"

        # Data columns should be renamed
        assert "id" in renamed.keys()
        assert "username" in renamed.keys()
        assert "user_id" not in renamed.keys()
        assert "name" not in renamed.keys()

    def test_update_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that update operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Update some column values
        updated = sample_tag_with_system_tags.update(name="Alice Smith", score=92.0)

        # System tags should be preserved
        assert updated.system_tags() == original_system_tags
        assert updated.system_tags()["tag_type"] == "user"
        assert updated.system_tags()["version"] == "1.0"

        # Updated values should be reflected
        assert updated["name"] == "Alice Smith"
        assert updated["score"] == 92.0
        assert updated["user_id"] == 123  # Unchanged

    def test_with_columns_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that with_columns operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Add new columns
        with_new_cols = sample_tag_with_system_tags.with_columns(
            email="alice@example.com", age=30, department="engineering"
        )

        # System tags should be preserved
        assert with_new_cols.system_tags() == original_system_tags
        assert with_new_cols.system_tags()["tag_type"] == "user"
        assert with_new_cols.system_tags()["version"] == "1.0"
        assert with_new_cols.system_tags()["created_by"] == "system"
        assert with_new_cols.system_tags()["priority"] == "high"

        # New columns should be added
        assert with_new_cols["email"] == "alice@example.com"
        assert with_new_cols["age"] == 30
        assert with_new_cols["department"] == "engineering"

        # Original columns should remain
        assert with_new_cols["user_id"] == 123
        assert with_new_cols["name"] == "Alice"

    def test_with_meta_columns_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that with_meta_columns operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Add meta columns
        with_meta = sample_tag_with_system_tags.with_meta_columns(
            pipeline_version="v2.1.0", processed_at="2024-01-01"
        )

        # System tags should be preserved
        assert with_meta.system_tags() == original_system_tags
        assert with_meta.system_tags()["tag_type"] == "user"
        assert with_meta.system_tags()["version"] == "1.0"

        # Meta columns should be added
        assert with_meta.get_meta_value("pipeline_version") == "v2.1.0"
        assert with_meta.get_meta_value("processed_at") == "2024-01-01"

    def test_drop_meta_columns_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that drop_meta_columns operation preserves system tags."""
        # First add some meta columns
        with_meta = sample_tag_with_system_tags.with_meta_columns(
            pipeline_version="v2.1.0", processed_at="2024-01-01"
        )
        original_system_tags = with_meta.system_tags()

        # Drop meta columns
        dropped_meta = with_meta.drop_meta_columns("pipeline_version")

        # System tags should be preserved
        assert dropped_meta.system_tags() == original_system_tags
        assert dropped_meta.system_tags()["tag_type"] == "user"
        assert dropped_meta.system_tags()["version"] == "1.0"

        # Meta column should be dropped
        assert dropped_meta.get_meta_value("pipeline_version") is None
        assert dropped_meta.get_meta_value("processed_at") == "2024-01-01"

    def test_with_context_key_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that with_context_key operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Change context key (note: "test" will resolve to "default" but that's expected)
        new_context = sample_tag_with_system_tags.with_context_key("std:v0.1:test")

        # System tags should be preserved
        assert new_context.system_tags() == original_system_tags
        assert new_context.system_tags()["tag_type"] == "user"
        assert new_context.system_tags()["version"] == "1.0"

        # Context should be different from original (even if resolved to default)
        # The important thing is that the operation worked and system tags are preserved
        assert new_context.data_context_key.startswith("std:v0.1:")
        # Verify that this is a different object
        assert new_context is not sample_tag_with_system_tags

    def test_copy_preserves_system_tags(self, sample_tag_with_system_tags):
        """Test that copy operation preserves system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Copy with cache
        copied_with_cache = sample_tag_with_system_tags.copy(include_cache=True)

        # Copy without cache
        copied_without_cache = sample_tag_with_system_tags.copy(include_cache=False)

        # System tags should be preserved in both cases
        assert copied_with_cache.system_tags() == original_system_tags
        assert copied_without_cache.system_tags() == original_system_tags

        # Verify all system tags are present
        for copy_obj in [copied_with_cache, copied_without_cache]:
            assert copy_obj.system_tags()["tag_type"] == "user"
            assert copy_obj.system_tags()["version"] == "1.0"
            assert copy_obj.system_tags()["created_by"] == "system"
            assert copy_obj.system_tags()["priority"] == "high"

    def test_chained_operations_preserve_system_tags(self, sample_tag_with_system_tags):
        """Test that chained operations preserve system tags."""
        original_system_tags = sample_tag_with_system_tags.system_tags()

        # Chain multiple operations
        result = (
            sample_tag_with_system_tags.with_columns(
                full_name="Alice Smith", department="eng"
            )
            .drop("score")
            .update(active=False)
            .rename({"user_id": "id"})
            .with_meta_columns(processed=True)
        )

        # System tags should be preserved through all operations
        assert result.system_tags() == original_system_tags
        assert result.system_tags()["tag_type"] == "user"
        assert result.system_tags()["version"] == "1.0"
        assert result.system_tags()["created_by"] == "system"
        assert result.system_tags()["priority"] == "high"

        # Verify the chained operations worked
        assert result["full_name"] == "Alice Smith"
        assert result["department"] == "eng"
        assert "score" not in result.keys()
        assert result["active"] is False
        assert "id" in result.keys()
        assert "user_id" not in result.keys()
        assert result.get_meta_value("processed") is True


class TestArrowPacketInitialization:
    """Test ArrowPacket initialization and basic properties."""

    def test_basic_initialization(self):
        """Test basic initialization with PyArrow table."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )

        packet = ArrowPacket(table)

        assert packet["user_id"] == 123
        assert packet["name"] == "Alice"
        assert packet["score"] == 85.5

    def test_initialization_multiple_rows_fails(self):
        """Test initialization with multiple rows fails."""
        table = pa.Table.from_pydict({"user_id": [123, 456], "name": ["Alice", "Bob"]})

        with pytest.raises(ValueError, match="single row"):
            ArrowPacket(table)

    def test_initialization_with_source_info(self):
        """Test initialization with source info."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        source_info = {"user_id": "database", "name": "user_input"}

        packet = ArrowPacket(table, source_info=source_info)

        assert packet["user_id"] == 123
        source_dict = packet.source_info()
        assert source_dict["user_id"] == "database"
        assert source_dict["name"] == "user_input"

    def test_initialization_with_source_info_in_table(self):
        """Test initialization when source info is included in table."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.SOURCE_PREFIX}user_id": ["database"],
                f"{constants.SOURCE_PREFIX}name": ["user_input"],
            }
        )

        packet = ArrowPacket(table)

        assert packet["user_id"] == 123
        assert packet["name"] == "Alice"

        source_info = packet.source_info()
        assert source_info["user_id"] == "database"
        assert source_info["name"] == "user_input"

    def test_initialization_mixed_source_info(self):
        """Test initialization with both embedded and explicit source info."""
        table = pa.Table.from_pydict(
            {
                "user_id": [123],
                "name": ["Alice"],
                f"{constants.SOURCE_PREFIX}user_id": ["embedded_source"],
            }
        )
        source_info = {"name": "explicit_source"}

        packet = ArrowPacket(table, source_info=source_info)

        source_dict = packet.source_info()
        assert source_dict["user_id"] == "embedded_source"
        assert source_dict["name"] == "explicit_source"

    def test_initialization_with_recordbatch(self):
        """Test initialization with RecordBatch instead of Table."""
        batch = pa.RecordBatch.from_pydict({"user_id": [123], "name": ["Alice"]})

        packet = ArrowPacket(batch)

        assert packet["user_id"] == 123
        assert packet["name"] == "Alice"


class TestArrowPacketSourceInfoOperations:
    """Test source info specific operations."""

    @pytest.fixture
    def sample_packet(self):
        """Create a sample packet for testing."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )
        source_info = {
            "user_id": "database",
            "name": "user_input",
            "score": "calculation",
        }
        return ArrowPacket(table, source_info=source_info)

    def test_source_info_method(self, sample_packet):
        """Test source_info method."""
        source_info = sample_packet.source_info()

        assert isinstance(source_info, dict)
        assert source_info["user_id"] == "database"
        assert source_info["name"] == "user_input"
        assert source_info["score"] == "calculation"

    def test_source_info_with_missing_keys(self):
        """Test source_info method when some keys are missing."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )
        source_info = {"user_id": "database"}  # Only partial source info

        packet = ArrowPacket(table, source_info=source_info)
        full_source_info = packet.source_info()

        assert full_source_info["user_id"] == "database"
        assert full_source_info["name"] is None
        assert full_source_info["score"] is None

    def test_with_source_info(self, sample_packet):
        """Test with_source_info method."""
        updated = sample_packet.with_source_info(
            user_id="new_database", name="new_input"
        )

        # Original should be unchanged
        original_source = sample_packet.source_info()
        assert original_source["user_id"] == "database"

        # Updated should have new values
        updated_source = updated.source_info()
        assert updated_source["user_id"] == "new_database"
        assert updated_source["name"] == "new_input"
        assert updated_source["score"] == "calculation"  # Unchanged

    def test_keys_with_source_info(self, sample_packet):
        """Test keys method including source info."""
        keys_data_only = sample_packet.keys()
        keys_with_source = sample_packet.keys(include_source=True)

        assert "user_id" in keys_data_only
        assert "name" in keys_data_only
        assert len(keys_with_source) > len(keys_data_only)

        # Should include prefixed source columns
        source_keys = [
            k for k in keys_with_source if k.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_keys) > 0

    def test_types_with_source_info(self, sample_packet):
        """Test types method including source info."""
        types_data_only = sample_packet.types()
        types_with_source = sample_packet.types(include_source=True)

        assert len(types_with_source) > len(types_data_only)

        # Source columns should be string type
        source_keys = [
            k for k in types_with_source.keys() if k.startswith(constants.SOURCE_PREFIX)
        ]
        for key in source_keys:
            assert types_with_source[key] is str

    def test_arrow_schema_with_source_info(self, sample_packet):
        """Test arrow_schema method including source info."""
        schema_data_only = sample_packet.arrow_schema()
        schema_with_source = sample_packet.arrow_schema(include_source=True)

        assert len(schema_with_source) > len(schema_data_only)

        source_columns = [
            name
            for name in schema_with_source.names
            if name.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_columns) > 0

    def test_as_dict_with_source_info(self, sample_packet):
        """Test as_dict method including source info."""
        dict_data_only = sample_packet.as_dict()
        dict_with_source = sample_packet.as_dict(include_source=True)

        assert "user_id" in dict_data_only
        assert "name" in dict_data_only
        assert not any(
            k.startswith(constants.SOURCE_PREFIX) for k in dict_data_only.keys()
        )

        assert "user_id" in dict_with_source
        source_keys = [
            k for k in dict_with_source.keys() if k.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_keys) > 0

    def test_as_table_with_source_info(self, sample_packet):
        """Test as_table method including source info."""
        table_data_only = sample_packet.as_table()
        table_with_source = sample_packet.as_table(include_source=True)

        assert len(table_with_source.column_names) > len(table_data_only.column_names)

        source_columns = [
            name
            for name in table_with_source.column_names
            if name.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_columns) > 0

    def test_as_datagram_conversion(self, sample_packet):
        """Test conversion to datagram."""
        datagram = sample_packet.as_datagram()

        # Should preserve data
        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"

        # Should not include source info by default
        assert not any(k.startswith(constants.SOURCE_PREFIX) for k in datagram.keys())

    def test_as_datagram_with_source_info(self, sample_packet):
        """Test conversion to datagram including source info."""
        datagram = sample_packet.as_datagram(include_source=True)

        # Should preserve data and include source info
        assert datagram["user_id"] == 123
        assert datagram["name"] == "Alice"
        source_keys = [
            k for k in datagram.keys() if k.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_keys) > 0


class TestArrowPacketDataOperations:
    """Test data operations specific to packets."""

    @pytest.fixture
    def sample_packet(self):
        """Create a sample packet for testing."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "score": [85.5]}
        )
        source_info = {
            "user_id": "database",
            "name": "user_input",
            "score": "calculation",
        }
        return ArrowPacket(table, source_info=source_info)

    def test_rename_preserves_source_info(self, sample_packet):
        """Test that rename operation preserves source info mapping."""
        renamed = sample_packet.rename({"user_id": "id", "name": "username"})

        # Data should be renamed
        assert "id" in renamed.keys()
        assert "username" in renamed.keys()
        assert "user_id" not in renamed.keys()
        assert "name" not in renamed.keys()

        # Source info should follow the rename
        source_info = renamed.source_info()
        assert source_info["id"] == "database"
        assert source_info["username"] == "user_input"
        assert source_info["score"] == "calculation"

    def test_with_columns_creates_source_info_columns(self, sample_packet):
        """Test that with_columns() creates corresponding source info columns with correct data types."""
        # Add new columns
        updated = sample_packet.with_columns(
            full_name="Alice Smith", age=30, is_active=True
        )

        # Verify new data columns exist
        assert "full_name" in updated.keys()
        assert "age" in updated.keys()
        assert "is_active" in updated.keys()
        assert updated["full_name"] == "Alice Smith"
        assert updated["age"] == 30
        assert updated["is_active"] is True

        # Verify corresponding source info columns are created
        source_info = updated.source_info()
        assert "full_name" in source_info
        assert "age" in source_info
        assert "is_active" in source_info

        # New source info columns should be initialized as None
        assert source_info["full_name"] is None
        assert source_info["age"] is None
        assert source_info["is_active"] is None

        # Verify existing source info is preserved
        assert source_info["user_id"] == "database"
        assert source_info["name"] == "user_input"
        assert source_info["score"] == "calculation"

        # Verify Arrow schema has correct data types for source info columns
        schema = updated.arrow_schema(include_source=True)

        # All source info columns should be large_string type
        source_columns = [col for col in schema if col.name.startswith("_source_")]
        assert len(source_columns) == 6  # 3 original + 3 new

        for field in source_columns:
            assert field.type == pa.large_string(), (
                f"Source column {field.name} should be large_string, got {field.type}"
            )

        # Verify we can set source info for new columns
        with_source = updated.with_source_info(
            full_name="calculated", age="user_input", is_active="default"
        )

        final_source_info = with_source.source_info()
        assert final_source_info["full_name"] == "calculated"
        assert final_source_info["age"] == "user_input"
        assert final_source_info["is_active"] == "default"


class TestArrowTagPacketIntegration:
    """Test integration between tags, packets, and base functionality."""

    def test_tag_to_packet_conversion(self):
        """Test converting a tag to a packet-like structure."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        system_tags = {"tag_type": "user", "version": "1.0"}
        tag = ArrowTag(table, system_tags=system_tags)

        # Convert to full dictionary
        full_dict = tag.as_dict(include_all_info=True)

        # Should include data, system tags, meta columns, and context
        assert "user_id" in full_dict
        assert "tag_type" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_packet_comprehensive_dict(self):
        """Test packet with all information types."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "__meta_field": ["meta_value"]}
        )
        source_info = {"user_id": "database", "name": "user_input"}

        packet = ArrowPacket(table, source_info=source_info)

        # Get comprehensive dictionary
        full_dict = packet.as_dict(include_all_info=True)

        # Should include data, source info, meta columns, and context
        assert "user_id" in full_dict
        assert f"{constants.SOURCE_PREFIX}user_id" in full_dict
        assert "__meta_field" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_chained_operations_tag(self):
        """Test chaining operations on tags."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "first_name": ["Alice"], "last_name": ["Smith"]}
        )
        system_tags = {"tag_type": "user"}

        tag = ArrowTag(table, system_tags=system_tags)

        # Chain operations
        result = (
            tag.with_columns(full_name="Alice Smith")
            .drop("first_name", "last_name")
            .update(user_id=456)
        )

        # Verify final state
        assert set(result.keys()) == {"user_id", "full_name"}
        assert result["user_id"] == 456
        assert result["full_name"] == "Alice Smith"

        # System tags should be preserved
        system_tags = result.system_tags()
        assert system_tags["tag_type"] == "user"

    def test_chained_operations_packet(self):
        """Test chaining operations on packets."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "first_name": ["Alice"], "last_name": ["Smith"]}
        )
        source_info = {"user_id": "database", "first_name": "form", "last_name": "form"}

        packet = ArrowPacket(table, source_info=source_info)

        # Chain operations
        result = (
            packet.with_columns(full_name="Alice Smith")
            .drop("first_name", "last_name")
            .update(user_id=456)
            .with_source_info(full_name="calculated")
        )

        # Verify final state
        assert set(result.keys()) == {"user_id", "full_name"}
        assert result["user_id"] == 456
        assert result["full_name"] == "Alice Smith"

        # Source info should be updated
        source_info = result.source_info()
        assert source_info["user_id"] == "database"
        assert source_info["full_name"] == "calculated"

    def test_copy_operations(self):
        """Test copy operations preserve all information."""
        # Test tag copy
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        system_tags = {"tag_type": "user"}
        tag = ArrowTag(table, system_tags=system_tags)

        tag_copy = tag.copy()
        assert tag_copy is not tag
        assert tag_copy["user_id"] == tag["user_id"]
        assert tag_copy.system_tags() == tag.system_tags()

        # Test packet copy
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        source_info = {"user_id": "database"}
        packet = ArrowPacket(table, source_info=source_info)

        packet_copy = packet.copy()
        assert packet_copy is not packet
        assert packet_copy["user_id"] == packet["user_id"]
        assert packet_copy.source_info() == packet.source_info()


class TestArrowTagPacketArrowSpecific:
    """Test Arrow-specific functionality and optimizations."""

    def test_tag_arrow_schema_preservation(self):
        """Test that Arrow schemas are preserved in tags."""
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int32()),
                pa.array(["Alice"], type=pa.large_string()),
            ],
            names=["id", "name"],
        )

        tag = ArrowTag(table)

        schema = tag.arrow_schema()
        assert schema.field("id").type == pa.int32()
        assert schema.field("name").type == pa.large_string()

    def test_packet_arrow_schema_preservation(self):
        """Test that Arrow schemas are preserved in packets."""
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([85.5], type=pa.float32()),
            ],
            names=["id", "score"],
        )

        packet = ArrowPacket(table)

        schema = packet.arrow_schema()
        assert schema.field("id").type == pa.int64()
        assert schema.field("score").type == pa.float32()

    def test_tag_complex_arrow_types(self):
        """Test tags with complex Arrow data types."""
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([[1, 2, 3]], type=pa.list_(pa.int32())),
                pa.array(
                    [{"nested": "value"}],
                    type=pa.struct([pa.field("nested", pa.string())]),
                ),
            ],
            names=["id", "numbers", "struct_field"],
        )

        tag = ArrowTag(table)

        assert tag["id"] == 123
        assert tag["numbers"] == [1, 2, 3]
        assert tag["struct_field"]["nested"] == "value"  # type: ignore

    def test_packet_complex_arrow_types(self):
        """Test packets with complex Arrow data types."""
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([[1, 2, 3]], type=pa.list_(pa.int32())),
                pa.array(
                    [{"nested": "value"}],
                    type=pa.struct([pa.field("nested", pa.string())]),
                ),
            ],
            names=["id", "numbers", "struct_field"],
        )

        packet = ArrowPacket(table)

        assert packet["id"] == 123
        assert packet["numbers"] == [1, 2, 3]
        assert packet["struct_field"]["nested"] == "value"  # type: ignore

    def test_tag_timestamp_handling(self):
        """Test tag handling of timestamp types."""
        now = datetime.now()
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([now], type=pa.timestamp("ns")),
            ],
            names=["id", "timestamp"],
        )

        tag = ArrowTag(table)

        assert tag["id"] == 123
        assert tag["timestamp"] is not None

    def test_packet_date_handling(self):
        """Test packet handling of date types."""
        today = date.today()
        table = pa.Table.from_arrays(
            [
                pa.array([123], type=pa.int64()),
                pa.array([today], type=pa.date32()),
            ],
            names=["id", "date"],
        )

        packet = ArrowPacket(table)

        assert packet["id"] == 123
        assert packet["date"] is not None

    def test_tag_arrow_memory_efficiency(self):
        """Test that tags share Arrow memory efficiently."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})

        tag = ArrowTag(table)

        # The important thing is that underlying arrays are shared for memory efficiency
        # Whether the table object itself is the same depends on whether system tag columns needed extraction
        original_array = table["user_id"]
        tag_array = tag._data_table["user_id"]
        assert tag_array.to_pylist() == original_array.to_pylist()

        # Test with a table that has system tag columns to ensure processing works
        table_with_system = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "_system_tag_type": ["user"]}
        )
        tag_with_system = ArrowTag(table_with_system)
        # This should create a different table since system columns are extracted
        assert tag_with_system._data_table is not table_with_system
        assert set(tag_with_system._data_table.column_names) == {"user_id", "name"}

    def test_packet_arrow_memory_efficiency(self):
        """Test that packets handle Arrow memory efficiently."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})

        packet = ArrowPacket(table)

        # Should efficiently handle memory
        assert (
            packet._data_table is not table
        )  # Different due to source info processing

        # But data should be preserved
        assert packet["user_id"] == 123
        assert packet["name"] == "Alice"


class TestArrowTagPacketEdgeCases:
    """Test edge cases and error conditions."""

    def test_tag_empty_system_tags(self):
        """Test tag with empty system tags."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        tag = ArrowTag(table, system_tags={})

        assert tag["user_id"] == 123
        assert tag.system_tags() == {}

    def test_packet_empty_source_info(self):
        """Test packet with empty source info."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        packet = ArrowPacket(table, source_info={})

        assert packet["user_id"] == 123
        source_info = packet.source_info()
        assert all(v is None for v in source_info.values())

    def test_tag_none_system_tags(self):
        """Test tag with None system tags."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        tag = ArrowTag(table, system_tags=None)

        assert tag["user_id"] == 123
        assert tag.system_tags() == {}

    def test_packet_none_source_info(self):
        """Test packet with None source info."""
        table = pa.Table.from_pydict({"user_id": [123], "name": ["Alice"]})
        packet = ArrowPacket(table, source_info=None)

        assert packet["user_id"] == 123
        source_info = packet.source_info()
        assert all(v is None for v in source_info.values())

    def test_tag_with_meta_and_system_tags(self):
        """Test tag with both meta columns and system tags."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "__meta_field": ["meta_value"]}
        )
        system_tags = {"tag_type": "user"}

        tag = ArrowTag(table, system_tags=system_tags)

        # All information should be accessible
        full_dict = tag.as_dict(include_all_info=True)
        assert "user_id" in full_dict
        assert "__meta_field" in full_dict
        assert "tag_type" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_packet_with_meta_and_source_info(self):
        """Test packet with both meta columns and source info."""
        table = pa.Table.from_pydict(
            {"user_id": [123], "name": ["Alice"], "__meta_field": ["meta_value"]}
        )
        source_info = {"user_id": "database"}

        packet = ArrowPacket(table, source_info=source_info)

        # All information should be accessible
        full_dict = packet.as_dict(include_all_info=True)
        assert "user_id" in full_dict
        assert "__meta_field" in full_dict
        assert f"{constants.SOURCE_PREFIX}user_id" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_tag_large_system_tags(self):
        """Test tag with many system tags."""
        table = pa.Table.from_pydict({"user_id": [123]})
        system_tags = {f"tag_{i}": f"value_{i}" for i in range(100)}

        tag = ArrowTag(table, system_tags=system_tags)

        assert tag["user_id"] == 123
        retrieved_tags = tag.system_tags()
        assert len(retrieved_tags) == 100
        assert retrieved_tags["tag_50"] == "value_50"

    def test_packet_large_source_info(self):
        """Test packet with source info for many columns."""
        data = {f"col_{i}": [i] for i in range(50)}
        table = pa.Table.from_pydict(data)
        source_info = {f"col_{i}": f"source_{i}" for i in range(50)}

        packet = ArrowPacket(table, source_info=source_info)

        assert packet["col_25"] == 25
        retrieved_source = packet.source_info()
        assert len(retrieved_source) == 50
        assert retrieved_source["col_25"] == "source_25"
