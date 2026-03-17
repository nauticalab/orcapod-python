"""
Tests for core/packet_function.py.

Covers:
- parse_function_outputs helper
- PacketFunctionBase (version parsing, URI, schema hash, identity) via PythonPacketFunction
- PythonPacketFunction construction, properties, call behaviour, error paths
- PacketFunctionProtocol protocol conformance
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from orcapod.core.datagrams import Packet
from orcapod.core.packet_function import PythonPacketFunction, parse_function_outputs
from orcapod.protocols.core_protocols import PacketFunctionProtocol

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def add(x: int, y: int) -> int:
    return x + y


def multi(a: int, b: int) -> tuple[int, int]:
    return a + b, a * b


async def async_add(x: int, y: int) -> int:
    return x + y


async def async_multi(a: int, b: int) -> tuple[int, int]:
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
def async_add_pf() -> PythonPacketFunction:
    """PythonPacketFunction wrapping an async two-arg addition."""
    return PythonPacketFunction(async_add, output_keys="result")


@pytest.fixture
def async_multi_pf() -> PythonPacketFunction:
    """PythonPacketFunction wrapping an async two-output function."""
    return PythonPacketFunction(async_multi, output_keys=["sum", "product"])


@pytest.fixture
def add_packet() -> Packet:
    return Packet({"x": 1, "y": 2})


# ---------------------------------------------------------------------------
# 1. parse_function_outputs
# ---------------------------------------------------------------------------


class TestParseFunctionOutputs:
    def test_no_output_keys_returns_empty_dict(self):
        assert parse_function_outputs([], 42) == {}

    def test_single_key_wraps_value(self):
        assert parse_function_outputs(["result"], 99) == {"result": 99}

    def test_single_key_wraps_iterable_as_single_value(self):
        # A list should be stored as-is, not unpacked, when there's one key
        result = parse_function_outputs(["items"], [1, 2, 3])
        assert result == {"items": [1, 2, 3]}

    def test_multiple_keys_unpacks_iterable(self):
        assert parse_function_outputs(["a", "b"], (10, 20)) == {"a": 10, "b": 20}

    def test_multiple_keys_non_iterable_raises(self):
        with pytest.raises(ValueError):
            parse_function_outputs(["a", "b"], 42)

    def test_mismatched_count_raises(self):
        with pytest.raises(ValueError):
            parse_function_outputs(["a", "b", "c"], (1, 2))  # only 2 values for 3 keys


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

    def test_var_positional_args_raises(self):
        def func_with_args(*args: int) -> int:
            return sum(args)

        with pytest.raises(ValueError, match=r"\*args"):
            PythonPacketFunction(func_with_args, output_keys="result")

    def test_var_keyword_args_raises(self):
        def func_with_kwargs(**kwargs: int) -> int:
            return sum(kwargs.values())

        with pytest.raises(ValueError, match=r"\*\*kwargs"):
            PythonPacketFunction(func_with_kwargs, output_keys="result")

    def test_mixed_variadic_raises(self):
        def func_mixed(x: int, *args: int, **kwargs: int) -> int:
            return x

        with pytest.raises(ValueError):
            PythonPacketFunction(func_mixed, output_keys="result")

    def test_fixed_params_with_defaults_accepted(self):
        def func_with_default(x: int, y: int = 10) -> int:
            return x + y

        # Should not raise -- default values are fine, only variadic are rejected
        pf = PythonPacketFunction(func_with_default, output_keys="result")
        assert "x" in pf.input_packet_schema
        assert "y" in pf.input_packet_schema

    def test_bare_dict_return_type_raises(self):
        """Bare ``dict`` (no type params) is not a valid output type."""

        def func(x: int) -> dict:
            return {"result": x}

        with pytest.raises(ValueError, match="dict"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_list_return_type_raises(self):
        """Bare ``list`` (no type params) is not a valid output type."""

        def func(x: int) -> list:
            return [x]

        with pytest.raises(ValueError, match="list"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_set_return_type_raises(self):
        """Bare ``set`` (no type params) is not a valid output type."""

        def func(x: int) -> set:
            return {x}

        with pytest.raises(ValueError, match="set"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_tuple_return_type_raises(self):
        """Bare ``tuple`` (no type params) is not a valid output type."""

        def func(x: int) -> tuple:
            return (x,)

        with pytest.raises(ValueError, match="tuple"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_dict_input_type_raises(self):
        """Bare ``dict`` (no type params) is not a valid input type."""

        def func(x: dict) -> int:
            return 1

        with pytest.raises(ValueError, match="dict"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_list_input_type_raises(self):
        """Bare ``list`` (no type params) is not a valid input type."""

        def func(x: list) -> int:
            return 1

        with pytest.raises(ValueError, match="list"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_set_input_type_raises(self):
        """Bare ``set`` (no type params) is not a valid input type."""

        def func(x: set) -> int:
            return 1

        with pytest.raises(ValueError, match="set"):
            PythonPacketFunction(func, output_keys="result")

    def test_bare_tuple_input_type_raises(self):
        """Bare ``tuple`` (no type params) is not a valid input type."""

        def func(x: tuple) -> int:
            return 1

        with pytest.raises(ValueError, match="tuple"):
            PythonPacketFunction(func, output_keys="result")

    def test_parameterized_dict_return_type_accepted(self):
        """``dict[str, int]`` (with type params) is a valid output type."""

        def func(x: int) -> dict[str, int]:
            return {"result": x}

        pf = PythonPacketFunction(func, output_keys="result")
        assert "result" in pf.output_packet_schema

    def test_parameterized_list_return_type_accepted(self):
        """``list[int]`` (with type params) is a valid output type."""

        def func(x: int) -> list[int]:
            return [x]

        pf = PythonPacketFunction(func, output_keys="result")
        assert "result" in pf.output_packet_schema


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
        result, _captured = add_pf.call(add_packet)
        assert result is not None

    def test_output_has_correct_key(self, add_pf, add_packet):
        result, _captured = add_pf.call(add_packet)
        assert "result" in result.keys()

    def test_output_has_correct_value(self, add_pf, add_packet):
        result, _captured = add_pf.call(add_packet)
        assert result["result"] == 3  # 1 + 2

    def test_source_info_contains_result_key(self, add_pf, add_packet):
        result, _captured = add_pf.call(add_packet)
        source = result.source_info()
        assert "result" in source

    def test_source_info_ends_with_key_name(self, add_pf, add_packet):
        result, _captured = add_pf.call(add_packet)
        source_str = result.source_info()["result"]
        assert source_str.endswith("::result")

    def test_source_info_contains_uri_components(self, add_pf, add_packet):
        result, _captured = add_pf.call(add_packet)
        source_str = result.source_info()["result"]
        for component in add_pf.uri:
            assert component in source_str

    def test_source_info_record_id_is_uuid(self, add_pf, add_packet):
        import re

        result, _captured = add_pf.call(add_packet)
        source_str = result.source_info()["result"]
        # The record_id segment is between the URI components and the key name
        # Format: uri_part1:uri_part2:..::record_id::key
        uuid_pattern = re.compile(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
        )
        assert uuid_pattern.search(source_str), f"No UUID found in {source_str!r}"

    def test_inactive_returns_none(self, add_pf, add_packet):
        add_pf.set_active(False)
        result, _captured = add_pf.call(add_packet)
        assert result is None

    def test_multiple_output_keys(self, multi_pf):
        packet = Packet({"a": 3, "b": 4})
        result, _captured = multi_pf.call(packet)
        assert result["sum"] == 7  # 3 + 4
        assert result["product"] == 12  # 3 * 4

    def test_multiple_output_keys_source_info(self, multi_pf):
        packet = Packet({"a": 3, "b": 4})
        result, _captured = multi_pf.call(packet)
        source = result.source_info()
        assert "sum" in source
        assert "product" in source
        assert source["sum"].endswith("::sum")
        assert source["product"].endswith("::product")

    def test_output_packet_schema_applied(self, add_pf, add_packet):
        result, _captured = add_pf.call(add_packet)
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
        packet = Packet({"a": 1, "b": 2})
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
        packet = Packet({"a": 1, "b": 2})
        with pytest.raises(ValueError):
            pf.call(packet)


# ---------------------------------------------------------------------------
# 10. async_call
# ---------------------------------------------------------------------------


class TestAsyncCall:
    def test_async_call_returns_correct_result(self, add_pf, add_packet):
        result, _captured = asyncio.run(add_pf.async_call(add_packet))
        assert result is not None
        assert result.as_dict()["result"] == 3  # 1 + 2


# ---------------------------------------------------------------------------
# 11. PacketFunctionProtocol protocol conformance
# ---------------------------------------------------------------------------


class TestPacketFunctionProtocolConformance:
    def test_python_packet_function_satisfies_protocol(self, add_pf):
        assert isinstance(add_pf, PacketFunctionProtocol), (
            "PythonPacketFunction does not satisfy the PacketFunctionProtocol protocol"
        )

    def test_async_python_packet_function_satisfies_protocol(self, async_add_pf):
        assert isinstance(async_add_pf, PacketFunctionProtocol), (
            "Async PythonPacketFunction does not satisfy the PacketFunctionProtocol protocol"
        )


# ---------------------------------------------------------------------------
# 12. Async function support — construction
# ---------------------------------------------------------------------------


class TestAsyncConstruction:
    def test_is_async_true_for_async_function(self, async_add_pf):
        assert async_add_pf.is_async is True

    def test_is_async_false_for_sync_function(self, add_pf):
        assert add_pf.is_async is False

    def test_packet_function_type_id_same_for_async(self, async_add_pf):
        assert async_add_pf.packet_function_type_id == "python.function.v0"

    def test_input_schema_correct_for_async(self, async_add_pf):
        schema = async_add_pf.input_packet_schema
        assert "x" in schema and "y" in schema
        assert schema["x"] is int
        assert schema["y"] is int

    def test_output_schema_correct_for_async(self, async_add_pf):
        schema = async_add_pf.output_packet_schema
        assert "result" in schema
        assert schema["result"] is int

    def test_canonical_name_from_async_function(self, async_add_pf):
        assert async_add_pf.canonical_function_name == "async_add"

    def test_variadic_async_rejected(self):
        async def bad(*args: int) -> int:
            return sum(args)

        with pytest.raises(ValueError, match=r"\*args"):
            PythonPacketFunction(bad, output_keys="result")


# ---------------------------------------------------------------------------
# 13. Async function support — sync call
# ---------------------------------------------------------------------------


class TestAsyncFunctionSyncCall:
    def test_direct_call_returns_correct_result(self, async_add_pf, add_packet):
        result, _captured = async_add_pf.direct_call(add_packet)
        assert result is not None
        assert result["result"] == 3

    def test_call_returns_correct_result(self, async_add_pf, add_packet):
        result, _captured = async_add_pf.call(add_packet)
        assert result is not None
        assert result["result"] == 3

    def test_inactive_returns_none(self, async_add_pf, add_packet):
        async_add_pf.set_active(False)
        result, _captured = async_add_pf.call(add_packet)
        assert result is None

    def test_multiple_outputs(self, async_multi_pf):
        packet = Packet({"a": 3, "b": 4})
        result, _captured = async_multi_pf.call(packet)
        assert result["sum"] == 7
        assert result["product"] == 12

    def test_source_info_present(self, async_add_pf, add_packet):
        result, _captured = async_add_pf.call(add_packet)
        source = result.source_info()
        assert "result" in source
        assert source["result"].endswith("::result")


# ---------------------------------------------------------------------------
# 14. Async function support — async call
# ---------------------------------------------------------------------------


class TestAsyncFunctionAsyncCall:
    def test_direct_async_call_awaits_directly(self, async_add_pf, add_packet):
        result, _captured = asyncio.run(async_add_pf.direct_async_call(add_packet))
        assert result is not None
        assert result["result"] == 3

    def test_async_call_returns_correct_result(self, async_add_pf, add_packet):
        result, _captured = asyncio.run(async_add_pf.async_call(add_packet))
        assert result is not None
        assert result["result"] == 3

    def test_inactive_returns_none(self, async_add_pf, add_packet):
        async_add_pf.set_active(False)
        result, _captured = asyncio.run(async_add_pf.async_call(add_packet))
        assert result is None

    def test_multiple_outputs(self, async_multi_pf):
        packet = Packet({"a": 3, "b": 4})
        result, _captured = asyncio.run(async_multi_pf.async_call(packet))
        assert result["sum"] == 7
        assert result["product"] == 12
