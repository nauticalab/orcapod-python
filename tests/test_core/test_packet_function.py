"""
Tests for core/packet_function.py.

Covers:
- parse_function_outputs helper
- PacketFunctionBase (version parsing, URI, schema hash, identity) via PythonPacketFunction
- PythonPacketFunction construction, properties, call behaviour, error paths
- PacketFunction protocol conformance
"""

from __future__ import annotations

import asyncio
import sys
from typing import Any
from unittest.mock import MagicMock

import pytest

from orcapod.core.datagrams import DictPacket
from orcapod.core.packet_function import PythonPacketFunction, parse_function_outputs
from orcapod.protocols.core_protocols import PacketFunction

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_stub(output_keys: list[str]) -> Any:
    """Minimal stub that satisfies the `self` interface expected by parse_function_outputs."""
    stub = MagicMock()
    stub.output_keys = output_keys
    return stub


def add(x: int, y: int) -> int:
    return x + y


def multi(a: int, b: int) -> tuple[int, int]:
    return a + b, a * b


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def add_pf() -> PythonPacketFunction:
    """PythonPacketFunction wrapping a simple two-arg addition."""
    return PythonPacketFunction(add, output_keys="result")


@pytest.fixture
def multi_pf() -> PythonPacketFunction:
    """PythonPacketFunction wrapping a two-output function."""
    return PythonPacketFunction(multi, output_keys=["sum", "product"])


@pytest.fixture
def add_packet() -> DictPacket:
    return DictPacket({"x": 1, "y": 2})


# ---------------------------------------------------------------------------
# 1. parse_function_outputs
# ---------------------------------------------------------------------------


class TestParseFunctionOutputs:
    def test_no_output_keys_returns_empty_dict(self):
        stub = _make_stub([])
        assert parse_function_outputs(stub, 42) == {}

    def test_single_key_wraps_value(self):
        stub = _make_stub(["result"])
        assert parse_function_outputs(stub, 99) == {"result": 99}

    def test_single_key_wraps_iterable_as_single_value(self):
        # A list should be stored as-is, not unpacked, when there's one key
        stub = _make_stub(["items"])
        result = parse_function_outputs(stub, [1, 2, 3])
        assert result == {"items": [1, 2, 3]}

    def test_multiple_keys_unpacks_iterable(self):
        stub = _make_stub(["a", "b"])
        assert parse_function_outputs(stub, (10, 20)) == {"a": 10, "b": 20}

    def test_multiple_keys_non_iterable_raises(self):
        stub = _make_stub(["a", "b"])
        with pytest.raises(ValueError):
            parse_function_outputs(stub, 42)

    def test_mismatched_count_raises(self):
        stub = _make_stub(["a", "b", "c"])
        with pytest.raises(ValueError):
            parse_function_outputs(stub, (1, 2))  # only 2 values for 3 keys


# ---------------------------------------------------------------------------
# 2. PacketFunctionBase — version parsing
# ---------------------------------------------------------------------------


class TestVersionParsing:
    @pytest.mark.parametrize(
        "version, expected_major, expected_minor",
        [
            ("v0.0", 0, "0"),
            ("v1.3", 1, "3"),
            ("1.5.2", 1, "5.2"),
            ("v2.0rc", 2, "0rc"),
            ("0.1", 0, "1"),
        ],
    )
    def test_valid_version_parses(self, version, expected_major, expected_minor):
        pf = PythonPacketFunction(add, output_keys="result", version=version)
        assert pf.major_version == expected_major
        assert pf.minor_version_string == expected_minor

    def test_invalid_version_raises(self):
        with pytest.raises(ValueError):
            PythonPacketFunction(add, output_keys="result", version="no_dots")


# ---------------------------------------------------------------------------
# 3. PacketFunctionBase properties
# ---------------------------------------------------------------------------


class TestPacketFunctionBaseProperties:
    def test_major_version_type(self, add_pf):
        assert isinstance(add_pf.major_version, int)

    def test_minor_version_string_type(self, add_pf):
        assert isinstance(add_pf.minor_version_string, str)

    def test_uri_is_four_tuple(self, add_pf):
        uri = add_pf.uri
        assert isinstance(uri, tuple)
        assert len(uri) == 4

    def test_uri_components(self, add_pf):
        name, schema_hash, version_part, type_id = add_pf.uri
        assert name == add_pf.canonical_function_name
        assert version_part == f"v{add_pf.major_version}"
        assert type_id == add_pf.packet_function_type_id
        assert isinstance(schema_hash, str)

    def test_output_packet_schema_hash_is_string(self, add_pf):
        h = add_pf.output_packet_schema_hash
        assert isinstance(h, str)
        assert len(h) > 0

    def test_output_packet_schema_hash_matches_uri(self, add_pf):
        _, schema_hash, _, _ = add_pf.uri
        assert schema_hash == add_pf.output_packet_schema_hash

    def test_identity_structure_equals_uri(self, add_pf):
        assert add_pf.identity_structure() == add_pf.uri

    def test_label_defaults_to_function_name(self, add_pf):
        assert add_pf.label == add_pf.canonical_function_name

    def test_explicit_label_overrides_computed(self):
        pf = PythonPacketFunction(add, output_keys="result", label="my_label")
        assert pf.label == "my_label"


# ---------------------------------------------------------------------------
# 4. PythonPacketFunction — construction
# ---------------------------------------------------------------------------


class TestPythonPacketFunctionConstruction:
    def test_packet_function_type_id(self, add_pf):
        assert add_pf.packet_function_type_id == "python.function.v0"

    def test_canonical_name_from_dunder_name(self):
        pf = PythonPacketFunction(add, output_keys="result")
        assert pf.canonical_function_name == "add"

    def test_explicit_function_name_overrides(self):
        pf = PythonPacketFunction(add, output_keys="result", function_name="custom")
        assert pf.canonical_function_name == "custom"

    def test_no_name_on_callable_raises(self):
        # A callable object (non-function) without __name__ should trigger ValueError
        class NamelessCallable:
            def __call__(self, x: int) -> int:
                return x

        obj = NamelessCallable()
        # callable objects don't have __name__ by default
        assert not hasattr(obj, "__name__")
        with pytest.raises(ValueError):
            PythonPacketFunction(obj, output_keys="result")

    def test_input_packet_schema_has_correct_keys(self, add_pf):
        schema = add_pf.input_packet_schema
        assert "x" in schema
        assert "y" in schema

    def test_input_packet_schema_has_correct_types(self, add_pf):
        schema = add_pf.input_packet_schema
        assert schema["x"] is int
        assert schema["y"] is int

    def test_output_packet_schema_has_correct_keys(self, add_pf):
        schema = add_pf.output_packet_schema
        assert "result" in schema

    def test_output_packet_schema_has_correct_types(self, add_pf):
        schema = add_pf.output_packet_schema
        assert schema["result"] is int

    def test_output_keys_string_normalised_to_list(self):
        pf = PythonPacketFunction(add, output_keys="result")
        assert pf._output_keys == ["result"]

    def test_output_keys_collection_preserved(self):
        pf = PythonPacketFunction(multi, output_keys=["sum", "product"])
        assert list(pf._output_keys) == ["sum", "product"]


# ---------------------------------------------------------------------------
# 5. get_function_variation_data
# ---------------------------------------------------------------------------


class TestGetFunctionVariationData:
    def test_returns_expected_keys(self, add_pf):
        data = add_pf.get_function_variation_data()
        assert set(data.keys()) == {
            "function_name",
            "function_signature_hash",
            "function_content_hash",
            "git_hash",
        }

    def test_all_values_are_strings(self, add_pf):
        data = add_pf.get_function_variation_data()
        for k, v in data.items():
            assert isinstance(v, str), f"Value for '{k}' is not a string: {v!r}"

    def test_function_name_matches_canonical(self, add_pf):
        data = add_pf.get_function_variation_data()
        assert data["function_name"] == add_pf.canonical_function_name


# ---------------------------------------------------------------------------
# 6. get_execution_data
# ---------------------------------------------------------------------------


class TestGetExecutionData:
    def test_returns_expected_keys(self, add_pf):
        data = add_pf.get_execution_data()
        assert "python_version" in data
        assert "execution_context" in data

    def test_python_version_matches_runtime(self, add_pf):
        vi = sys.version_info
        expected = f"{vi.major}.{vi.minor}.{vi.micro}"
        assert add_pf.get_execution_data()["python_version"] == expected

    def test_execution_context_is_local(self, add_pf):
        assert add_pf.get_execution_data()["execution_context"] == "local"


# ---------------------------------------------------------------------------
# 7. is_active / set_active
# ---------------------------------------------------------------------------


class TestActiveState:
    def test_active_by_default(self, add_pf):
        assert add_pf.is_active() is True

    def test_set_active_false(self, add_pf):
        add_pf.set_active(False)
        assert add_pf.is_active() is False

    def test_set_active_true_re_enables(self, add_pf):
        add_pf.set_active(False)
        add_pf.set_active(True)
        assert add_pf.is_active() is True


# ---------------------------------------------------------------------------
# 8. call — core behaviour
# ---------------------------------------------------------------------------


class TestCall:
    def test_returns_packet_when_active(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        assert result is not None

    def test_output_has_correct_key(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        assert "result" in result.keys()

    def test_output_has_correct_value(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        assert result["result"] == 3  # 1 + 2

    def test_source_info_contains_result_key(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        source = result.source_info()
        assert "result" in source

    def test_source_info_ends_with_key_name(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        source_str = result.source_info()["result"]
        assert source_str.endswith("::result")

    def test_source_info_contains_uri_components(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        source_str = result.source_info()["result"]
        for component in add_pf.uri:
            assert component in source_str

    def test_source_info_record_id_is_uuid(self, add_pf, add_packet):
        import re

        result = add_pf.call(add_packet)
        source_str = result.source_info()["result"]
        # The record_id segment is between the URI components and the key name
        # Format: uri_part1:uri_part2:..::record_id::key
        uuid_pattern = re.compile(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        )
        assert uuid_pattern.search(source_str), f"No UUID found in {source_str!r}"

    def test_inactive_returns_none(self, add_pf, add_packet):
        add_pf.set_active(False)
        assert add_pf.call(add_packet) is None

    def test_multiple_output_keys(self, multi_pf):
        packet = DictPacket({"a": 3, "b": 4})
        result = multi_pf.call(packet)
        assert result["sum"] == 7  # 3 + 4
        assert result["product"] == 12  # 3 * 4

    def test_multiple_output_keys_source_info(self, multi_pf):
        packet = DictPacket({"a": 3, "b": 4})
        result = multi_pf.call(packet)
        source = result.source_info()
        assert "sum" in source
        assert "product" in source
        assert source["sum"].endswith("::sum")
        assert source["product"].endswith("::product")

    def test_output_packet_schema_applied(self, add_pf, add_packet):
        result = add_pf.call(add_packet)
        assert result is not None
        # schema from the packet function should carry through
        schema = result.schema()
        assert "result" in schema


# ---------------------------------------------------------------------------
# 9. call — error paths
# ---------------------------------------------------------------------------


class TestCallErrors:
    def test_multi_key_non_iterable_result_raises(self):
        # Returns a scalar but two output keys are declared; error comes from call()
        def returns_scalar(a, b):
            return a + b

        pf = PythonPacketFunction(
            returns_scalar,
            output_keys=["x", "y"],
            input_schema={"a": int, "b": int},
            output_schema={"x": int, "y": int},
        )
        packet = DictPacket({"a": 1, "b": 2})
        with pytest.raises(ValueError):
            pf.call(packet)

    def test_too_few_values_raises(self):
        # Returns only one value but two keys are expected
        def returns_one(a, b):
            return (a,)

        pf = PythonPacketFunction(
            returns_one,
            output_keys=["x", "y"],
            input_schema={"a": int, "b": int},
            output_schema={"x": int, "y": int},
        )
        packet = DictPacket({"a": 1, "b": 2})
        with pytest.raises(ValueError):
            pf.call(packet)


# ---------------------------------------------------------------------------
# 10. async_call
# ---------------------------------------------------------------------------


class TestAsyncCall:
    def test_async_call_raises_not_implemented(self, add_pf, add_packet):
        with pytest.raises(NotImplementedError):
            asyncio.run(add_pf.async_call(add_packet))


# ---------------------------------------------------------------------------
# 11. PacketFunction protocol conformance
# ---------------------------------------------------------------------------


class TestPacketFunctionProtocolConformance:
    def test_python_packet_function_satisfies_protocol(self, add_pf):
        assert isinstance(add_pf, PacketFunction), (
            "PythonPacketFunction does not satisfy the PacketFunction protocol"
        )
