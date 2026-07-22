"""
Tests for core/data_function.py.

Covers:
- parse_function_outputs helper
- DataFunctionBase (version parsing, URI, schema hash, identity) via PythonDataFunction
- PythonDataFunction construction, properties, call behaviour, error paths
- DataFunctionProtocol protocol conformance
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from orcapod.core.datagrams import Data
from orcapod.core.data_function import PythonDataFunction, parse_function_outputs
from orcapod.protocols.core_protocols import DataFunctionProtocol
from orcapod.types import ContentHash

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
def add_pf() -> PythonDataFunction:
    """PythonDataFunction wrapping a simple two-arg addition."""
    return PythonDataFunction(add, output_keys="result")


@pytest.fixture
def multi_pf() -> PythonDataFunction:
    """PythonDataFunction wrapping a two-output function."""
    return PythonDataFunction(multi, output_keys=["sum", "product"])


@pytest.fixture
def async_add_pf() -> PythonDataFunction:
    """PythonDataFunction wrapping an async two-arg addition."""
    return PythonDataFunction(async_add, output_keys="result")


@pytest.fixture
def async_multi_pf() -> PythonDataFunction:
    """PythonDataFunction wrapping an async two-output function."""
    return PythonDataFunction(async_multi, output_keys=["sum", "product"])


@pytest.fixture
def add_data() -> Data:
    return Data({"x": 1, "y": 2})


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
# 2. DataFunctionBase — version parsing
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
        pf = PythonDataFunction(add, output_keys="result", version=version)
        assert pf.major_version == expected_major
        assert pf.minor_version_string == expected_minor

    def test_invalid_version_raises(self):
        with pytest.raises(ValueError):
            PythonDataFunction(add, output_keys="result", version="no_dots")


# ---------------------------------------------------------------------------
# 3. DataFunctionBase properties
# ---------------------------------------------------------------------------


class TestDataFunctionBaseProperties:
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
        assert type_id == add_pf.data_function_type_id
        assert isinstance(schema_hash, str)

    def test_output_data_schema_hash_is_string(self, add_pf):
        h = add_pf.output_data_schema_hash
        assert isinstance(h, str)
        assert len(h) > 0

    def test_output_data_schema_hash_matches_uri(self, add_pf):
        _, schema_hash, _, _ = add_pf.uri
        assert schema_hash == add_pf.output_data_schema_hash

    def test_identity_structure_equals_uri(self, add_pf):
        assert add_pf.identity_structure() == add_pf.uri

    def test_label_defaults_to_function_name(self, add_pf):
        assert add_pf.label == add_pf.canonical_function_name

    def test_explicit_label_overrides_computed(self):
        pf = PythonDataFunction(add, output_keys="result", label="my_label")
        assert pf.label == "my_label"


# ---------------------------------------------------------------------------
# 4. PythonDataFunction — construction
# ---------------------------------------------------------------------------


class TestPythonDataFunctionConstruction:
    def test_data_function_type_id(self, add_pf):
        assert add_pf.data_function_type_id == "python.function.v0"

    def test_canonical_name_from_dunder_name(self):
        pf = PythonDataFunction(add, output_keys="result")
        assert pf.canonical_function_name == "add"

    def test_explicit_function_name_overrides(self):
        pf = PythonDataFunction(add, output_keys="result", function_name="custom")
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
            PythonDataFunction(obj, output_keys="result")

    def test_input_data_schema_has_correct_keys(self, add_pf):
        schema = add_pf.input_data_schema
        assert "x" in schema
        assert "y" in schema

    def test_input_data_schema_has_correct_types(self, add_pf):
        schema = add_pf.input_data_schema
        assert schema["x"] is int
        assert schema["y"] is int

    def test_output_data_schema_has_correct_keys(self, add_pf):
        schema = add_pf.output_data_schema
        assert "result" in schema

    def test_output_data_schema_has_correct_types(self, add_pf):
        schema = add_pf.output_data_schema
        assert schema["result"] is int

    def test_output_keys_string_normalised_to_list(self):
        pf = PythonDataFunction(add, output_keys="result")
        assert pf._output_keys == ["result"]

    def test_output_keys_collection_preserved(self):
        pf = PythonDataFunction(multi, output_keys=["sum", "product"])
        assert list(pf._output_keys) == ["sum", "product"]

    def test_var_positional_args_raises(self):
        def func_with_args(*args: int) -> int:
            return sum(args)

        with pytest.raises(ValueError, match=r"\*args"):
            PythonDataFunction(func_with_args, output_keys="result")

    def test_var_keyword_args_raises(self):
        def func_with_kwargs(**kwargs: int) -> int:
            return sum(kwargs.values())

        with pytest.raises(ValueError, match=r"\*\*kwargs"):
            PythonDataFunction(func_with_kwargs, output_keys="result")

    def test_mixed_variadic_raises(self):
        def func_mixed(x: int, *args: int, **kwargs: int) -> int:
            return x

        with pytest.raises(ValueError):
            PythonDataFunction(func_mixed, output_keys="result")

    def test_fixed_params_with_defaults_accepted(self):
        def func_with_default(x: int, y: int = 10) -> int:
            return x + y

        # Should not raise -- default values are fine, only variadic are rejected
        pf = PythonDataFunction(func_with_default, output_keys="result")
        assert "x" in pf.input_data_schema
        assert "y" in pf.input_data_schema

    def test_bare_dict_return_type_raises(self):
        """Bare ``dict`` (no type params) is not a valid output type."""

        def func(x: int) -> dict:
            return {"result": x}

        with pytest.raises(ValueError, match="dict"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_list_return_type_raises(self):
        """Bare ``list`` (no type params) is not a valid output type."""

        def func(x: int) -> list:
            return [x]

        with pytest.raises(ValueError, match="list"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_set_return_type_raises(self):
        """Bare ``set`` (no type params) is not a valid output type."""

        def func(x: int) -> set:
            return {x}

        with pytest.raises(ValueError, match="set"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_tuple_return_type_raises(self):
        """Bare ``tuple`` (no type params) is not a valid output type."""

        def func(x: int) -> tuple:
            return (x,)

        with pytest.raises(ValueError, match="tuple"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_dict_input_type_raises(self):
        """Bare ``dict`` (no type params) is not a valid input type."""

        def func(x: dict) -> int:
            return 1

        with pytest.raises(ValueError, match="dict"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_list_input_type_raises(self):
        """Bare ``list`` (no type params) is not a valid input type."""

        def func(x: list) -> int:
            return 1

        with pytest.raises(ValueError, match="list"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_set_input_type_raises(self):
        """Bare ``set`` (no type params) is not a valid input type."""

        def func(x: set) -> int:
            return 1

        with pytest.raises(ValueError, match="set"):
            PythonDataFunction(func, output_keys="result")

    def test_bare_tuple_input_type_raises(self):
        """Bare ``tuple`` (no type params) is not a valid input type."""

        def func(x: tuple) -> int:
            return 1

        with pytest.raises(ValueError, match="tuple"):
            PythonDataFunction(func, output_keys="result")

    def test_parameterized_dict_return_type_accepted(self):
        """``dict[str, int]`` (with type params) is a valid output type."""

        def func(x: int) -> dict[str, int]:
            return {"result": x}

        pf = PythonDataFunction(func, output_keys="result")
        assert "result" in pf.output_data_schema

    def test_parameterized_list_return_type_accepted(self):
        """``list[int]`` (with type params) is a valid output type."""

        def func(x: int) -> list[int]:
            return [x]

        pf = PythonDataFunction(func, output_keys="result")
        assert "result" in pf.output_data_schema


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

    def test_non_hash_values_are_strings(self, add_pf):
        data = add_pf.get_function_variation_data()
        assert isinstance(data["function_name"], str)
        assert isinstance(data["git_hash"], str)

    def test_function_name_matches_canonical(self, add_pf):
        data = add_pf.get_function_variation_data()
        assert data["function_name"] == add_pf.canonical_function_name


# ---------------------------------------------------------------------------
# 6. get_execution_data
# ---------------------------------------------------------------------------


class TestGetExecutionData:
    def test_returns_expected_keys(self, add_pf):
        data = add_pf.get_execution_data()
        assert set(data.keys()) == {
            "executor_type",
            "executor_info",
            "python_version",
            "extra_info",
        }

    def test_python_version_matches_runtime(self, add_pf):
        vi = sys.version_info
        expected = f"{vi.major}.{vi.minor}.{vi.micro}"
        assert add_pf.get_execution_data()["python_version"] == expected

    def test_executor_type_is_string(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert isinstance(data["executor_type"], str)

    def test_executor_info_is_dict_str_str(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert isinstance(data["executor_info"], dict)
        for k, v in data["executor_info"].items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def test_extra_info_is_empty_dict(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert data["extra_info"] == {}


class TestExecutionDataSchema:
    def test_execution_data_has_expected_keys(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert set(data.keys()) == {
            "executor_type",
            "executor_info",
            "python_version",
            "extra_info",
        }

    def test_executor_type_is_string(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert isinstance(data["executor_type"], str)

    def test_executor_info_is_dict_str_str(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert isinstance(data["executor_info"], dict)
        for k, v in data["executor_info"].items():
            assert isinstance(k, str)
            assert isinstance(v, str)

    def test_python_version_matches_sys(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        expected = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        assert data["python_version"] == expected

    def test_extra_info_is_empty_dict(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        assert data["extra_info"] == {}

    def test_execution_data_schema_matches_data_keys(self):
        pf = PythonDataFunction(add, output_keys="result")
        data = pf.get_execution_data()
        schema = pf.get_execution_data_schema()
        assert set(schema.keys()) == set(data.keys())

    def test_execution_data_schema_types(self):
        pf = PythonDataFunction(add, output_keys="result")
        schema = pf.get_execution_data_schema()
        assert schema["executor_type"] is str
        assert schema["executor_info"] == dict[str, str]
        assert schema["python_version"] is str
        assert schema["extra_info"] == dict[str, str]


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
    def test_returns_data_when_active(self, add_pf, add_data):
        result = add_pf.call(add_data)
        assert result is not None

    def test_output_has_correct_key(self, add_pf, add_data):
        result = add_pf.call(add_data)
        assert "result" in result.keys()

    def test_output_has_correct_value(self, add_pf, add_data):
        result = add_pf.call(add_data)
        assert result["result"] == 3  # 1 + 2

    def test_source_info_contains_result_key(self, add_pf, add_data):
        result = add_pf.call(add_data)
        source = result.source_info()
        assert "result" in source

    def test_source_info_ends_with_key_name(self, add_pf, add_data):
        result = add_pf.call(add_data)
        source_str = result.source_info()["result"]
        assert source_str.endswith("::result")

    def test_source_info_contains_uri_components(self, add_pf, add_data):
        result = add_pf.call(add_data)
        source_str = result.source_info()["result"]
        for component in add_pf.uri:
            assert component in source_str

    def test_source_info_record_id_is_uuid(self, add_pf, add_data):
        import re

        result = add_pf.call(add_data)
        source_str = result.source_info()["result"]
        # Format: <uri_components_colon_joined>::<record_id_hex>::<key>
        # Extract the second "::" segment which is always the UUID hex.
        parts = source_str.split("::")
        assert len(parts) == 3, (
            f"Expected exactly 3 '::'-separated segments in {source_str!r}, got {len(parts)}"
        )
        uuid_hex_segment = parts[1]
        assert re.fullmatch(r"[0-9a-f]{32}", uuid_hex_segment), (
            f"Record ID segment {uuid_hex_segment!r} is not a 32-char lowercase hex string"
        )

    def test_inactive_returns_none(self, add_pf, add_data):
        add_pf.set_active(False)
        result = add_pf.call(add_data)
        assert result is None

    def test_multiple_output_keys(self, multi_pf):
        data = Data({"a": 3, "b": 4})
        result = multi_pf.call(data)
        assert result["sum"] == 7  # 3 + 4
        assert result["product"] == 12  # 3 * 4

    def test_multiple_output_keys_source_info(self, multi_pf):
        data = Data({"a": 3, "b": 4})
        result = multi_pf.call(data)
        source = result.source_info()
        assert "sum" in source
        assert "product" in source
        assert source["sum"].endswith("::sum")
        assert source["product"].endswith("::product")

    def test_output_data_schema_applied(self, add_pf, add_data):
        result = add_pf.call(add_data)
        assert result is not None
        # schema from the data function should carry through
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

        pf = PythonDataFunction(
            returns_scalar,
            output_keys=["x", "y"],
            input_schema={"a": int, "b": int},
            output_schema={"x": int, "y": int},
        )
        data = Data({"a": 1, "b": 2})
        with pytest.raises(ValueError):
            pf.call(data)

    def test_too_few_values_raises(self):
        # Returns only one value but two keys are expected
        def returns_one(a, b):
            return (a,)

        pf = PythonDataFunction(
            returns_one,
            output_keys=["x", "y"],
            input_schema={"a": int, "b": int},
            output_schema={"x": int, "y": int},
        )
        data = Data({"a": 1, "b": 2})
        with pytest.raises(ValueError):
            pf.call(data)


# ---------------------------------------------------------------------------
# 10. async_call
# ---------------------------------------------------------------------------


class TestAsyncCall:
    def test_async_call_returns_correct_result(self, add_pf, add_data):
        result = asyncio.run(add_pf.async_call(add_data))
        assert result is not None
        assert result.as_dict()["result"] == 3  # 1 + 2


# ---------------------------------------------------------------------------
# 11. DataFunctionProtocol protocol conformance
# ---------------------------------------------------------------------------


class TestDataFunctionProtocolConformance:
    def test_python_data_function_satisfies_protocol(self, add_pf):
        assert isinstance(add_pf, DataFunctionProtocol), (
            "PythonDataFunction does not satisfy the DataFunctionProtocol protocol"
        )

    def test_async_python_data_function_satisfies_protocol(self, async_add_pf):
        assert isinstance(async_add_pf, DataFunctionProtocol), (
            "Async PythonDataFunction does not satisfy the DataFunctionProtocol protocol"
        )


# ---------------------------------------------------------------------------
# 12. Async function support — construction
# ---------------------------------------------------------------------------


class TestAsyncConstruction:
    def test_is_async_true_for_async_function(self, async_add_pf):
        assert async_add_pf.is_async is True

    def test_is_async_false_for_sync_function(self, add_pf):
        assert add_pf.is_async is False

    def test_data_function_type_id_same_for_async(self, async_add_pf):
        assert async_add_pf.data_function_type_id == "python.function.v0"

    def test_input_schema_correct_for_async(self, async_add_pf):
        schema = async_add_pf.input_data_schema
        assert "x" in schema and "y" in schema
        assert schema["x"] is int
        assert schema["y"] is int

    def test_output_schema_correct_for_async(self, async_add_pf):
        schema = async_add_pf.output_data_schema
        assert "result" in schema
        assert schema["result"] is int

    def test_canonical_name_from_async_function(self, async_add_pf):
        assert async_add_pf.canonical_function_name == "async_add"

    def test_variadic_async_rejected(self):
        async def bad(*args: int) -> int:
            return sum(args)

        with pytest.raises(ValueError, match=r"\*args"):
            PythonDataFunction(bad, output_keys="result")


# ---------------------------------------------------------------------------
# 13. Async function support — sync call
# ---------------------------------------------------------------------------


class TestAsyncFunctionSyncCall:
    def test_direct_call_returns_correct_result(self, async_add_pf, add_data):
        result = async_add_pf.direct_call(add_data)
        assert result is not None
        assert result["result"] == 3

    def test_call_returns_correct_result(self, async_add_pf, add_data):
        result = async_add_pf.call(add_data)
        assert result is not None
        assert result["result"] == 3

    def test_inactive_returns_none(self, async_add_pf, add_data):
        async_add_pf.set_active(False)
        result = async_add_pf.call(add_data)
        assert result is None

    def test_multiple_outputs(self, async_multi_pf):
        data = Data({"a": 3, "b": 4})
        result = async_multi_pf.call(data)
        assert result["sum"] == 7
        assert result["product"] == 12

    def test_source_info_present(self, async_add_pf, add_data):
        result = async_add_pf.call(add_data)
        source = result.source_info()
        assert "result" in source
        assert source["result"].endswith("::result")


# ---------------------------------------------------------------------------
# 14. Async function support — async call
# ---------------------------------------------------------------------------


class TestAsyncFunctionAsyncCall:
    def test_direct_async_call_awaits_directly(self, async_add_pf, add_data):
        result = asyncio.run(async_add_pf.direct_async_call(add_data))
        assert result is not None
        assert result["result"] == 3

    def test_async_call_returns_correct_result(self, async_add_pf, add_data):
        result = asyncio.run(async_add_pf.async_call(add_data))
        assert result is not None
        assert result["result"] == 3

    def test_inactive_returns_none(self, async_add_pf, add_data):
        async_add_pf.set_active(False)
        result = asyncio.run(async_add_pf.async_call(add_data))
        assert result is None

    def test_multiple_outputs(self, async_multi_pf):
        data = Data({"a": 3, "b": 4})
        result = asyncio.run(async_multi_pf.async_call(data))
        assert result["sum"] == 7
        assert result["product"] == 12


# ---------------------------------------------------------------------------
# TestSignatureHashUnionOrderIndependence
# ---------------------------------------------------------------------------


class TestSignatureHashUnionOrderIndependence:
    """_function_signature_hash must be order-independent over union members."""

    def _sig_hash(self, func):
        # PythonDataFunction is already imported at the top of this test file.
        df = PythonDataFunction(func, output_keys="result")
        return df.get_function_variation_data()["function_signature_hash"]

    def test_two_member_union_param_order_independent(self):
        """str | Path and Path | str produce the same signature hash."""
        def foo(x: str | Path) -> str:
            return str(x)
        h1 = self._sig_hash(foo)

        def foo(x: Path | str) -> str:
            return str(x)
        h2 = self._sig_hash(foo)

        assert h1 == h2

    def test_three_member_union_all_permutations(self):
        """All permutations of str | Path | bytes produce the same signature hash."""
        def foo(x: str | Path | bytes) -> str:
            return str(x)
        h1 = self._sig_hash(foo)

        def foo(x: bytes | str | Path) -> str:
            return str(x)
        h2 = self._sig_hash(foo)

        def foo(x: Path | bytes | str) -> str:
            return str(x)
        h3 = self._sig_hash(foo)

        assert h2 == h1
        assert h3 == h1

    def test_return_type_union_order_independent(self):
        """Return-type unions are also order-independent."""
        def foo(x: int) -> str | Path:
            return str(x)
        h1 = self._sig_hash(foo)

        def foo(x: int) -> Path | str:
            return str(x)
        h2 = self._sig_hash(foo)

        assert h1 == h2

    def test_non_union_param_hash_unchanged(self):
        """A non-union function's signature hash is stable and unaffected by the union fix."""
        def foo(x: int) -> str:
            return str(x)

        # Hash must be deterministic across multiple calls.
        # This exercises that the fix does not disturb non-union annotations.
        h1 = self._sig_hash(foo)
        h2 = self._sig_hash(foo)
        assert h1 == h2

    def test_different_union_types_still_differ(self):
        """str | Path and str | bytes are different and must not hash the same."""
        def foo(x: str | Path) -> str:
            return str(x)
        h1 = self._sig_hash(foo)

        def foo(x: str | bytes) -> str:
            return str(x)
        h2 = self._sig_hash(foo)

        assert h1 != h2

    def test_union_vs_non_union_differ(self):
        """A union-typed param and a plain-typed param produce different hashes."""
        def foo(x: str | Path) -> str:
            return str(x)
        h1 = self._sig_hash(foo)

        def foo(x: str) -> str:
            return str(x)
        h2 = self._sig_hash(foo)

        assert h1 != h2


class TestVariationHashSchema:
    def test_function_signature_hash_is_bytes(self, add_pf):
        """PythonDataFunction stores variation hashes as bytes (-> large_binary)."""
        variation = add_pf.get_function_variation_data()
        assert isinstance(variation["function_signature_hash"], bytes)

    def test_function_content_hash_is_bytes(self, add_pf):
        """PythonDataFunction stores content hashes as bytes (-> large_binary)."""
        variation = add_pf.get_function_variation_data()
        assert isinstance(variation["function_content_hash"], bytes)

    def test_variation_schema_has_bytes_types(self, add_pf):
        schema = add_pf.get_function_variation_data_schema()
        assert schema["function_signature_hash"] is bytes
        assert schema["function_content_hash"] is bytes

    def test_variation_hash_decodes_to_content_hash(self, add_pf):
        """Both variation hash bytes round-trip through ContentHash.from_prefixed_digest."""
        variation = add_pf.get_function_variation_data()
        sig_hash = ContentHash.from_prefixed_digest(variation["function_signature_hash"])
        content_hash = ContentHash.from_prefixed_digest(variation["function_content_hash"])
        assert isinstance(sig_hash, ContentHash)
        assert isinstance(content_hash, ContentHash)
