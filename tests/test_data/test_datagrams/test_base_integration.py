"""
Comprehensive tests for base datagram functionality and integration tests.

This module tests:
- Base datagram abstract interface
- Integration between different datagram implementations
- Cross-format conversions
- Performance and memory considerations
"""

import pytest
import pyarrow as pa

from orcapod.core.datagrams import (
    DictDatagram,
    ArrowDatagram,
    DictTag,
    DictPacket,
    ArrowTag,
    ArrowPacket,
)
from orcapod.core.datagrams.base import (
    BaseDatagram,
    ImmutableDict,
    contains_prefix_from,
)
from orcapod.contexts.system_constants import constants


class TestImmutableDict:
    """Test ImmutableDict utility class."""

    def test_basic_functionality(self):
        """Test basic ImmutableDict operations."""
        data = {"a": 1, "b": 2, "c": 3}
        immutable = ImmutableDict(data)

        assert immutable["a"] == 1
        assert immutable["b"] == 2
        assert immutable["c"] == 3
        assert len(immutable) == 3

    def test_iteration(self):
        """Test iteration over ImmutableDict."""
        data = {"a": 1, "b": 2, "c": 3}
        immutable = ImmutableDict(data)

        keys = list(immutable)
        assert set(keys) == {"a", "b", "c"}

        items = list(immutable.items())
        assert set(items) == {("a", 1), ("b", 2), ("c", 3)}

    def test_merge_operation(self):
        """Test merge operation with | operator."""
        data1 = {"a": 1, "b": 2}
        data2 = {"c": 3, "d": 4}

        immutable1 = ImmutableDict(data1)
        immutable2 = ImmutableDict(data2)

        merged = immutable1 | immutable2

        assert len(merged) == 4
        assert merged["a"] == 1
        assert merged["c"] == 3

    def test_merge_with_dict(self):
        """Test merge operation with regular dict."""
        data1 = {"a": 1, "b": 2}
        data2 = {"c": 3, "d": 4}

        immutable = ImmutableDict(data1)
        merged = immutable | data2

        assert len(merged) == 4
        assert merged["a"] == 1
        assert merged["c"] == 3

    def test_string_representations(self):
        """Test string representations."""
        data = {"a": 1, "b": 2}
        immutable = ImmutableDict(data)

        str_repr = str(immutable)
        repr_str = repr(immutable)

        assert "a" in str_repr and "1" in str_repr
        assert "a" in repr_str and "1" in repr_str


class TestUtilityFunctions:
    """Test utility functions."""

    def test_contains_prefix_from(self):
        """Test contains_prefix_from function."""
        prefixes = ["__", "_source_", "_system_"]

        assert contains_prefix_from("__version", prefixes)
        assert contains_prefix_from("_source_file", prefixes)
        assert contains_prefix_from("_system_tag", prefixes)
        assert not contains_prefix_from("regular_column", prefixes)
        assert not contains_prefix_from("_other_prefix", prefixes)

    def test_contains_prefix_from_empty(self):
        """Test contains_prefix_from with empty prefixes."""
        assert not contains_prefix_from("any_column", [])

    def test_contains_prefix_from_edge_cases(self):
        """Test contains_prefix_from edge cases."""
        prefixes = ["__"]

        assert contains_prefix_from("__", prefixes)
        assert not contains_prefix_from("_", prefixes)
        assert not contains_prefix_from("", prefixes)


class TestBaseDatagram:
    """Test BaseDatagram abstract interface."""

    def test_is_abstract(self):
        """Test that BaseDatagram cannot be instantiated directly."""
        try:
            # This should raise TypeError for abstract class
            BaseDatagram()  # type: ignore
            pytest.fail("Expected TypeError for abstract class instantiation")
        except TypeError as e:
            # Expected behavior - BaseDatagram is abstract
            assert "abstract" in str(e).lower() or "instantiate" in str(e).lower()

    def test_abstract_methods(self):
        """Test that all abstract methods are defined."""
        # Get all abstract methods
        abstract_methods = BaseDatagram.__abstractmethods__

        # Verify key abstract methods exist
        expected_methods = {
            "__getitem__",
            "__contains__",
            "__iter__",
            "get",
            "keys",
            "types",
            "arrow_schema",
            "content_hash",
            "as_dict",
            "as_table",
            "meta_columns",
            "get_meta_value",
            "with_meta_columns",
            "drop_meta_columns",
            "select",
            "drop",
            "rename",
            "update",
            "with_columns",
        }

        assert expected_methods.issubset(abstract_methods)


class TestCrossFormatConversions:
    """Test conversions between different datagram formats."""

    @pytest.fixture
    def sample_data(self):
        """Sample data for conversion tests."""
        return {
            "user_id": 123,
            "name": "Alice",
            "score": 85.5,
            "active": True,
            "__version": "1.0",
            "__pipeline": "test",
        }

    def test_dict_to_arrow_conversion(self, sample_data):
        """Test converting DictDatagram to ArrowDatagram."""
        dict_datagram = DictDatagram(sample_data)

        # Convert via table
        table = dict_datagram.as_table(include_all_info=True)
        arrow_datagram = ArrowDatagram(table)

        # Data should be preserved
        assert arrow_datagram["user_id"] == dict_datagram["user_id"]
        assert arrow_datagram["name"] == dict_datagram["name"]
        assert arrow_datagram["score"] == dict_datagram["score"]
        assert arrow_datagram["active"] == dict_datagram["active"]

        # Meta columns should be preserved
        assert arrow_datagram.get_meta_value("version") == dict_datagram.get_meta_value(
            "version"
        )
        assert arrow_datagram.get_meta_value(
            "pipeline"
        ) == dict_datagram.get_meta_value("pipeline")

    def test_arrow_to_dict_conversion(self, sample_data):
        """Test converting ArrowDatagram to DictDatagram."""
        table = pa.Table.from_pylist([sample_data])
        arrow_datagram = ArrowDatagram(table)

        # Convert via dict
        data_dict = arrow_datagram.as_dict(include_all_info=True)
        dict_datagram = DictDatagram(data_dict)

        # Data should be preserved
        assert dict_datagram["user_id"] == arrow_datagram["user_id"]
        assert dict_datagram["name"] == arrow_datagram["name"]
        assert dict_datagram["score"] == arrow_datagram["score"]
        assert dict_datagram["active"] == arrow_datagram["active"]

        # Meta columns should be preserved
        assert dict_datagram.get_meta_value("version") == arrow_datagram.get_meta_value(
            "version"
        )

    def test_tag_conversions(self):
        """Test conversions between tag formats."""
        data = {"user_id": 123, "name": "Alice"}
        system_tags = {"tag_type": "user", "version": "1.0"}

        # Dict to Arrow tag
        dict_tag = DictTag(data, system_tags=system_tags)
        table = dict_tag.as_table(include_all_info=True)
        arrow_tag = ArrowTag(table)

        # Data and system tags should be preserved
        assert arrow_tag["user_id"] == dict_tag["user_id"]
        assert arrow_tag["name"] == dict_tag["name"]

        # Arrow to Dict tag
        full_dict = arrow_tag.as_dict(include_all_info=True)
        reconstructed_dict_tag = DictTag(full_dict)

        assert reconstructed_dict_tag["user_id"] == arrow_tag["user_id"]
        assert reconstructed_dict_tag["name"] == arrow_tag["name"]

    def test_packet_conversions(self):
        """Test conversions between packet formats."""
        data = {"user_id": 123, "name": "Alice"}
        source_info = {"user_id": "database", "name": "user_input"}

        # Dict to Arrow packet
        dict_packet = DictPacket(data, source_info=source_info)
        table = dict_packet.as_table(include_all_info=True)
        arrow_packet = ArrowPacket(table)

        # Data and source info should be preserved
        assert arrow_packet["user_id"] == dict_packet["user_id"]
        assert arrow_packet["name"] == dict_packet["name"]

        # Arrow to Dict packet
        full_dict = arrow_packet.as_dict(include_all_info=True)
        reconstructed_dict_packet = DictPacket(full_dict)

        assert reconstructed_dict_packet["user_id"] == arrow_packet["user_id"]
        assert reconstructed_dict_packet["name"] == arrow_packet["name"]


class TestDatagramIntegration:
    """Test integration between different datagram types."""

    def test_mixed_operations(self):
        """Test operations that mix different datagram types."""
        # Start with dict datagram
        dict_data = {"user_id": 123, "name": "Alice", "score": 85.5}
        dict_datagram = DictDatagram(dict_data)

        # Convert to arrow
        table = dict_datagram.as_table()
        arrow_datagram = ArrowDatagram(table)

        # Perform operations on arrow datagram
        modified_arrow = arrow_datagram.update(score=90.0).with_columns(grade="A")

        # Convert back to dict
        modified_dict = DictDatagram(modified_arrow.as_dict())

        # Verify final state
        assert modified_dict["user_id"] == 123
        assert modified_dict["score"] == 90.0
        assert modified_dict["grade"] == "A"

    def test_tag_packet_interoperability(self):
        """Test interoperability between tags and packets."""
        # Create a tag
        tag_data = {"entity_id": "user_123", "entity_type": "user"}
        system_tags = {"created_by": "system", "version": "1.0"}
        tag = DictTag(tag_data, system_tags=system_tags)

        # Convert tag to packet-like structure
        tag_as_dict = tag.as_dict(include_system_tags=True)
        packet = DictPacket(tag_as_dict, source_info={"entity_id": "tag_system"})

        # Verify data preservation
        assert packet["entity_id"] == tag["entity_id"]
        assert packet["entity_type"] == tag["entity_type"]

        # Source info should be available
        source_info = packet.source_info()
        assert source_info["entity_id"] == "tag_system"

    def test_comprehensive_roundtrip(self):
        """Test comprehensive roundtrip through all formats."""
        original_data = {
            "user_id": 123,
            "name": "Alice",
            "score": 85.5,
            "active": True,
            "__version": "1.0",
            constants.CONTEXT_KEY: "v0.1",
        }

        # Start with DictDatagram
        dict_datagram = DictDatagram(original_data)

        # Convert to ArrowDatagram
        table = dict_datagram.as_table(include_all_info=True)
        arrow_datagram = ArrowDatagram(table)

        # Convert to DictTag with some system tags
        tag_dict = arrow_datagram.as_dict(include_all_info=True)
        dict_tag = DictTag(tag_dict, system_tags={"tag_type": "test", "version": "1.0"})

        # Convert to ArrowTag
        tag_table = dict_tag.as_table(include_all_info=True)
        arrow_tag = ArrowTag(tag_table)

        # Convert to DictPacket with some source info
        packet_dict = arrow_tag.as_dict(include_all_info=True)
        dict_packet = DictPacket(
            packet_dict, source_info={"source": "test", "timestamp": "2024-01-01"}
        )

        # Convert to ArrowPacket
        packet_table = dict_packet.as_table(include_all_info=True)
        arrow_packet = ArrowPacket(packet_table)

        # Convert back to DictDatagram
        final_dict = arrow_packet.as_dict(include_all_info=True)
        final_datagram = DictDatagram(final_dict)

        # Verify data preservation through the entire journey
        assert final_datagram["user_id"] == original_data["user_id"]
        assert final_datagram["name"] == original_data["name"]
        assert final_datagram["score"] == original_data["score"]
        assert final_datagram["active"] == original_data["active"]
        assert final_datagram.get_meta_value("version") == "1.0"
        assert final_datagram.data_context_key == "std:v0.1:default"


class TestDatagramConsistency:
    """Test consistency across different datagram implementations."""

    @pytest.fixture
    def equivalent_datagrams(self):
        """Create equivalent datagrams in different formats."""
        data = {
            "user_id": 123,
            "name": "Alice",
            "score": 85.5,
            "active": True,
            "__version": "1.0",
        }

        dict_datagram = DictDatagram(data)
        table = pa.Table.from_pylist([data])
        arrow_datagram = ArrowDatagram(table)

        return dict_datagram, arrow_datagram

    def test_consistent_dict_interface(self, equivalent_datagrams):
        """Test that dict-like interface is consistent."""
        dict_dg, arrow_dg = equivalent_datagrams

        # __getitem__
        assert dict_dg["user_id"] == arrow_dg["user_id"]
        assert dict_dg["name"] == arrow_dg["name"]
        assert dict_dg["score"] == arrow_dg["score"]
        assert dict_dg["active"] == arrow_dg["active"]

        # __contains__
        assert ("user_id" in dict_dg) == ("user_id" in arrow_dg)
        assert ("nonexistent" in dict_dg) == ("nonexistent" in arrow_dg)

        # get
        assert dict_dg.get("user_id") == arrow_dg.get("user_id")
        assert dict_dg.get("nonexistent", "default") == arrow_dg.get(
            "nonexistent", "default"
        )

    def test_consistent_structural_info(self, equivalent_datagrams):
        """Test that structural information is consistent."""
        dict_dg, arrow_dg = equivalent_datagrams

        # keys
        assert set(dict_dg.keys()) == set(arrow_dg.keys())
        assert set(dict_dg.keys(include_meta_columns=True)) == set(
            arrow_dg.keys(include_meta_columns=True)
        )

        # meta_columns
        assert set(dict_dg.meta_columns) == set(arrow_dg.meta_columns)

        # types (basic structure, not exact types due to inference differences)
        dict_types = dict_dg.types()
        arrow_types = arrow_dg.types()
        assert set(dict_types.keys()) == set(arrow_types.keys())

    def test_consistent_meta_operations(self, equivalent_datagrams):
        """Test that meta operations are consistent."""
        dict_dg, arrow_dg = equivalent_datagrams

        # get_meta_value
        assert dict_dg.get_meta_value("version") == arrow_dg.get_meta_value("version")
        assert dict_dg.get_meta_value(
            "nonexistent", "default"
        ) == arrow_dg.get_meta_value("nonexistent", "default")

    def test_consistent_data_operations(self, equivalent_datagrams):
        """Test that data operations produce consistent results."""
        dict_dg, arrow_dg = equivalent_datagrams

        # select
        dict_selected = dict_dg.select("user_id", "name")
        arrow_selected = arrow_dg.select("user_id", "name")

        assert set(dict_selected.keys()) == set(arrow_selected.keys())
        assert dict_selected["user_id"] == arrow_selected["user_id"]
        assert dict_selected["name"] == arrow_selected["name"]

        # update
        dict_updated = dict_dg.update(score=95.0)
        arrow_updated = arrow_dg.update(score=95.0)

        assert dict_updated["score"] == arrow_updated["score"]
        assert dict_updated["user_id"] == arrow_updated["user_id"]  # Unchanged

    def test_consistent_format_conversions(self, equivalent_datagrams):
        """Test that format conversions are consistent."""
        dict_dg, arrow_dg = equivalent_datagrams

        # as_dict
        dict_as_dict = dict_dg.as_dict()
        arrow_as_dict = arrow_dg.as_dict()

        assert dict_as_dict == arrow_as_dict

        # as_table
        dict_as_table = dict_dg.as_table()
        arrow_as_table = arrow_dg.as_table()

        assert dict_as_table.column_names == arrow_as_table.column_names
        assert len(dict_as_table) == len(arrow_as_table)


class TestDatagramPerformance:
    """Test performance characteristics of different implementations."""

    def test_memory_efficiency(self):
        """Test memory efficiency considerations."""
        # Create large-ish data
        n_cols = 100
        data = {f"col_{i}": [i * 1.5] for i in range(n_cols)}

        # Dict implementation
        dict_datagram = DictDatagram(data)

        # Arrow implementation - get data in correct format from dict datagram
        arrow_data = dict_datagram.as_dict()
        # Convert scalar values to single-element lists for PyArrow
        arrow_data_lists = {k: [v] for k, v in arrow_data.items()}
        table = pa.Table.from_pydict(arrow_data_lists)
        arrow_datagram = ArrowDatagram(table)

        # Both should handle the data efficiently
        assert len(dict_datagram.keys()) == n_cols
        assert len(arrow_datagram.keys()) == n_cols

        # Verify data integrity - both should have consistent data
        # Note: The original data has lists, so both implementations should handle lists consistently
        assert dict_datagram["col_50"] == [
            75.0
        ]  # DictDatagram preserves list structure
        assert arrow_datagram["col_50"] == [
            75.0
        ]  # ArrowDatagram also preserves list structure

    def test_caching_behavior(self):
        """Test caching behavior across implementations."""
        data = {"user_id": [123], "name": ["Alice"]}  # Lists for PyArrow

        # Test dict caching
        dict_datagram = DictDatagram(data)
        dict1 = dict_datagram.as_dict()
        dict2 = dict_datagram.as_dict()
        # Dict implementation may or may not cache, but should be consistent
        assert dict1 == dict2

        # Test arrow caching
        table = pa.Table.from_pydict(data)
        arrow_datagram = ArrowDatagram(table)
        arrow_dict1 = arrow_datagram.as_dict()
        arrow_dict2 = arrow_datagram.as_dict()
        # Arrow implementation should cache
        assert arrow_dict1 == arrow_dict2  # Same content
        # Note: ArrowDatagram returns copies for safety, not identical objects

    def test_operation_efficiency(self):
        """Test efficiency of common operations."""
        # Create moderately sized data
        data = {f"col_{i}": [i] for i in range(50)}  # Lists for PyArrow

        dict_datagram = DictDatagram(data)
        table = pa.Table.from_pydict(data)
        arrow_datagram = ArrowDatagram(table)

        # Select operations should be efficient
        dict_selected = dict_datagram.select("col_0", "col_25", "col_49")
        arrow_selected = arrow_datagram.select("col_0", "col_25", "col_49")

        assert len(dict_selected.keys()) == 3
        assert len(arrow_selected.keys()) == 3

        # Update operations should be efficient
        dict_updated = dict_datagram.update(col_25=999)
        arrow_updated = arrow_datagram.update(col_25=999)

        assert dict_updated["col_25"] == 999
        assert arrow_updated["col_25"] == 999


class TestDatagramErrorHandling:
    """Test error handling consistency across implementations."""

    def test_consistent_key_errors(self):
        """Test that KeyError handling is consistent."""
        data = {"user_id": [123], "name": ["Alice"]}  # Lists for PyArrow

        dict_datagram = DictDatagram(data)
        table = pa.Table.from_pydict(data)
        arrow_datagram = ArrowDatagram(table)

        # Both should raise KeyError for missing keys
        with pytest.raises(KeyError):
            _ = dict_datagram["nonexistent"]

        with pytest.raises(KeyError):
            _ = arrow_datagram["nonexistent"]

    def test_consistent_operation_errors(self):
        """Test that operation errors are consistent."""
        data = {"user_id": [123], "name": ["Alice"]}  # Lists for PyArrow

        dict_datagram = DictDatagram(data)
        table = pa.Table.from_pydict(data)
        arrow_datagram = ArrowDatagram(table)

        # Both should raise appropriate errors for invalid operations
        with pytest.raises((KeyError, ValueError)):
            dict_datagram.select("nonexistent")

        with pytest.raises((KeyError, ValueError)):
            arrow_datagram.select("nonexistent")

        with pytest.raises(KeyError):
            dict_datagram.update(nonexistent="value")

        with pytest.raises(KeyError):
            arrow_datagram.update(nonexistent="value")

    def test_consistent_validation(self):
        """Test that validation is consistent."""
        data = {"user_id": 123, "name": "Alice"}  # Lists for PyArrow

        dict_datagram = DictDatagram(data)
        table = pa.Table.from_pylist([data])
        arrow_datagram = ArrowDatagram(table)

        # Both should handle edge cases consistently
        # Test that empty select behavior is consistent (may select all or raise error)
        try:
            dict_result = dict_datagram.select()
            arrow_result = arrow_datagram.select()
            # If both succeed, they should have the same keys
            assert set(dict_result.keys()) == set(arrow_result.keys())
        except (ValueError, TypeError):
            # If one raises an error, both should raise similar errors
            with pytest.raises((ValueError, TypeError)):
                arrow_datagram.select()

        # Note: The important thing is that both behave the same way
