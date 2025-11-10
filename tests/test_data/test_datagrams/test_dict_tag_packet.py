"""
Comprehensive tests for DictTag and DictPacket classes.

This module tests all functionality of the dictionary-based tag and packet classes including:
- Tag-specific functionality (system tags)
- Packet-specific functionality (source info)
- Integration with base datagram functionality
- Conversion operations
"""

import pytest

from orcapod.core.datagrams import DictTag, DictPacket
from orcapod.contexts.system_constants import constants


class TestDictTagInitialization:
    """Test DictTag initialization and basic properties."""

    def test_basic_initialization(self):
        """Test basic initialization with simple data."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        tag = DictTag(data)

        assert tag["user_id"] == 123
        assert tag["name"] == "Alice"
        assert tag["score"] == 85.5

    def test_initialization_with_system_tags(self):
        """Test initialization with system tags."""
        data = {"user_id": 123, "name": "Alice"}
        system_tags = {"tag_type": "user", "created_by": "system"}

        tag = DictTag(data, system_tags=system_tags)

        assert tag["user_id"] == 123
        system_tag_dict = tag.system_tags()
        assert system_tag_dict["tag_type"] == "user"
        assert system_tag_dict["created_by"] == "system"

    def test_initialization_with_system_tags_in_data(self):
        """Test initialization when system tags are included in data."""
        data = {
            "user_id": 123,
            "name": "Alice",
            f"{constants.SYSTEM_TAG_PREFIX}tag_type": "user",
            f"{constants.SYSTEM_TAG_PREFIX}version": "1.0",
        }

        tag = DictTag(data)

        assert tag["user_id"] == 123
        assert tag["name"] == "Alice"

        system_tags = tag.system_tags()
        assert system_tags[f"{constants.SYSTEM_TAG_PREFIX}tag_type"] == "user"
        assert system_tags[f"{constants.SYSTEM_TAG_PREFIX}version"] == "1.0"

    def test_initialization_mixed_system_tags(self):
        """Test initialization with both embedded and explicit system tags."""
        data = {"user_id": 123, f"{constants.SYSTEM_TAG_PREFIX}embedded": "value1"}
        system_tags = {"explicit": "value2"}

        tag = DictTag(data, system_tags=system_tags)

        system_tag_dict = tag.system_tags()
        assert system_tag_dict[f"{constants.SYSTEM_TAG_PREFIX}embedded"] == "value1"
        assert system_tag_dict["explicit"] == "value2"


class TestDictTagSystemTagOperations:
    """Test system tag specific operations."""

    @pytest.fixture
    def sample_tag(self):
        """Create a sample tag for testing."""
        data = {"user_id": 123, "name": "Alice"}
        system_tags = {"tag_type": "user", "version": "1.0"}
        return DictTag(data, system_tags=system_tags)

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


class TestDictPacketInitialization:
    """Test DictPacket initialization and basic properties."""

    def test_basic_initialization(self):
        """Test basic initialization with simple data."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        packet = DictPacket(data)

        assert packet["user_id"] == 123
        assert packet["name"] == "Alice"
        assert packet["score"] == 85.5

    def test_initialization_with_source_info(self):
        """Test initialization with source info."""
        data = {"user_id": 123, "name": "Alice"}
        source_info = {"user_id": "database", "name": "user_input"}

        packet = DictPacket(data, source_info=source_info)

        assert packet["user_id"] == 123
        source_dict = packet.source_info()
        assert source_dict["user_id"] == "database"
        assert source_dict["name"] == "user_input"

    def test_initialization_with_source_info_in_data(self):
        """Test initialization when source info is included in data."""
        data = {
            "user_id": 123,
            "name": "Alice",
            f"{constants.SOURCE_PREFIX}user_id": "database",
            f"{constants.SOURCE_PREFIX}name": "user_input",
        }

        packet = DictPacket(data)

        assert packet["user_id"] == 123
        assert packet["name"] == "Alice"

        source_info = packet.source_info()
        assert source_info["user_id"] == "database"
        assert source_info["name"] == "user_input"

    def test_initialization_mixed_source_info(self):
        """Test initialization with both embedded and explicit source info."""
        data = {
            "user_id": 123,
            "name": "Alice",
            f"{constants.SOURCE_PREFIX}user_id": "embedded_source",
        }
        source_info = {"name": "explicit_source"}

        packet = DictPacket(data, source_info=source_info)

        source_dict = packet.source_info()
        assert source_dict["user_id"] == "embedded_source"
        assert source_dict["name"] == "explicit_source"


class TestDictPacketSourceInfoOperations:
    """Test source info specific operations."""

    @pytest.fixture
    def sample_packet(self):
        """Create a sample packet for testing."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        source_info = {
            "user_id": "database",
            "name": "user_input",
            "score": "calculation",
        }
        return DictPacket(data, source_info=source_info)

    def test_source_info_method(self, sample_packet):
        """Test source_info method."""
        source_info = sample_packet.source_info()

        assert isinstance(source_info, dict)
        assert source_info["user_id"] == "database"
        assert source_info["name"] == "user_input"
        assert source_info["score"] == "calculation"

    def test_source_info_with_missing_keys(self):
        """Test source_info method when some keys are missing."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        source_info = {"user_id": "database"}  # Only partial source info

        packet = DictPacket(data, source_info=source_info)
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


class TestDictPacketDataOperations:
    """Test data operations specific to packets."""

    @pytest.fixture
    def sample_packet(self):
        """Create a sample packet for testing."""
        data = {"user_id": 123, "name": "Alice", "score": 85.5}
        source_info = {
            "user_id": "database",
            "name": "user_input",
            "score": "calculation",
        }
        return DictPacket(data, source_info=source_info)

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


class TestDictTagPacketIntegration:
    """Test integration between tags, packets, and base functionality."""

    def test_tag_to_packet_conversion(self):
        """Test converting a tag to a packet-like structure."""
        data = {"user_id": 123, "name": "Alice"}
        system_tags = {"tag_type": "user", "version": "1.0"}
        tag = DictTag(data, system_tags=system_tags)

        # Convert to full dictionary
        full_dict = tag.as_dict(include_all_info=True)

        # Should include data, system tags, meta columns, and context
        assert "user_id" in full_dict
        assert "tag_type" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_packet_comprehensive_dict(self):
        """Test packet with all information types."""
        data = {"user_id": 123, "name": "Alice", "__meta_field": "meta_value"}
        source_info = {"user_id": "database", "name": "user_input"}

        packet = DictPacket(data, source_info=source_info)

        # Get comprehensive dictionary
        full_dict = packet.as_dict(include_all_info=True)

        # Should include data, source info, meta columns, and context
        assert "user_id" in full_dict
        assert f"{constants.SOURCE_PREFIX}user_id" in full_dict
        assert "__meta_field" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_chained_operations_tag(self):
        """Test chaining operations on tags."""
        data = {"user_id": 123, "first_name": "Alice", "last_name": "Smith"}
        system_tags = {"tag_type": "user"}

        tag = DictTag(data, system_tags=system_tags)

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
        data = {"user_id": 123, "first_name": "Alice", "last_name": "Smith"}
        source_info = {"user_id": "database", "first_name": "form", "last_name": "form"}

        packet = DictPacket(data, source_info=source_info)

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
        tag_data = {"user_id": 123, "name": "Alice"}
        system_tags = {"tag_type": "user"}
        tag = DictTag(tag_data, system_tags=system_tags)

        tag_copy = tag.copy()
        assert tag_copy is not tag
        assert tag_copy["user_id"] == tag["user_id"]
        assert tag_copy.system_tags() == tag.system_tags()

        # Test packet copy
        packet_data = {"user_id": 123, "name": "Alice"}
        source_info = {"user_id": "database"}
        packet = DictPacket(packet_data, source_info=source_info)

        packet_copy = packet.copy()
        assert packet_copy is not packet
        assert packet_copy["user_id"] == packet["user_id"]
        assert packet_copy.source_info() == packet.source_info()


class TestDictTagPacketEdgeCases:
    """Test edge cases and error conditions."""

    def test_tag_empty_system_tags(self):
        """Test tag with empty system tags."""
        data = {"user_id": 123, "name": "Alice"}
        tag = DictTag(data, system_tags={})

        assert tag["user_id"] == 123
        assert tag.system_tags() == {}

    def test_packet_empty_source_info(self):
        """Test packet with empty source info."""
        data = {"user_id": 123, "name": "Alice"}
        packet = DictPacket(data, source_info={})

        assert packet["user_id"] == 123
        source_info = packet.source_info()
        assert all(v is None for v in source_info.values())

    def test_tag_none_system_tags(self):
        """Test tag with None system tags."""
        data = {"user_id": 123, "name": "Alice"}
        tag = DictTag(data, system_tags=None)

        assert tag["user_id"] == 123
        assert tag.system_tags() == {}

    def test_packet_none_source_info(self):
        """Test packet with None source info."""
        data = {"user_id": 123, "name": "Alice"}
        packet = DictPacket(data, source_info=None)

        assert packet["user_id"] == 123
        source_info = packet.source_info()
        assert all(v is None for v in source_info.values())

    def test_tag_with_meta_and_system_tags(self):
        """Test tag with both meta columns and system tags."""
        data = {"user_id": 123, "name": "Alice", "__meta_field": "meta_value"}
        system_tags = {"tag_type": "user"}

        tag = DictTag(data, system_tags=system_tags)

        # All information should be accessible
        full_dict = tag.as_dict(include_all_info=True)
        assert "user_id" in full_dict
        assert "__meta_field" in full_dict
        assert "tag_type" in full_dict
        assert constants.CONTEXT_KEY in full_dict

    def test_packet_with_meta_and_source_info(self):
        """Test packet with both meta columns and source info."""
        data = {"user_id": 123, "name": "Alice", "__meta_field": "meta_value"}
        source_info = {"user_id": "database"}

        packet = DictPacket(data, source_info=source_info)

        # All information should be accessible
        full_dict = packet.as_dict(include_all_info=True)
        assert "user_id" in full_dict
        assert "__meta_field" in full_dict
        assert f"{constants.SOURCE_PREFIX}user_id" in full_dict
        assert constants.CONTEXT_KEY in full_dict
