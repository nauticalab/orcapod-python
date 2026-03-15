"""Tests for pipeline serialization registries and helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.pipeline.serialization import (
    DATABASE_REGISTRY,
    OPERATOR_REGISTRY,
    PACKET_FUNCTION_REGISTRY,
    SOURCE_REGISTRY,
    LoadStatus,
    PIPELINE_FORMAT_VERSION,
    deserialize_schema,
    parse_arrow_type_string,
    resolve_database_from_config,
    resolve_operator_from_config,
    serialize_schema,
)
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.core.operators import Join, Batch
from orcapod.types import Schema


class TestRegistries:
    def test_database_registry_has_all_types(self):
        assert "delta_table" in DATABASE_REGISTRY
        assert "in_memory" in DATABASE_REGISTRY
        assert "noop" in DATABASE_REGISTRY

    def test_source_registry_has_all_types(self):
        assert "csv" in SOURCE_REGISTRY
        assert "delta_table" in SOURCE_REGISTRY
        assert "dict" in SOURCE_REGISTRY

    def test_operator_registry_has_all_types(self):
        assert "Join" in OPERATOR_REGISTRY
        assert "Batch" in OPERATOR_REGISTRY
        assert "SelectTagColumns" in OPERATOR_REGISTRY

    def test_packet_function_registry(self):
        assert "python.function.v0" in PACKET_FUNCTION_REGISTRY


class TestLoadStatus:
    def test_enum_values(self):
        assert LoadStatus.FULL.value == "full"
        assert LoadStatus.READ_ONLY.value == "read_only"
        assert LoadStatus.UNAVAILABLE.value == "unavailable"


class TestResolveDatabaseFromConfig:
    def test_resolve_in_memory(self):
        config = {"type": "in_memory", "max_hierarchy_depth": 10}
        db = resolve_database_from_config(config)
        assert isinstance(db, InMemoryArrowDatabase)

    def test_resolve_unknown_type_raises(self):
        config = {"type": "unknown_db"}
        with pytest.raises(ValueError, match="Unknown database type"):
            resolve_database_from_config(config)


class TestResolveOperatorFromConfig:
    def test_resolve_join(self):
        config = {
            "class_name": "Join",
            "module_path": "orcapod.core.operators.join",
            "config": {},
        }
        op = resolve_operator_from_config(config)
        assert isinstance(op, Join)

    def test_resolve_batch(self):
        config = {
            "class_name": "Batch",
            "module_path": "orcapod.core.operators.batch",
            "config": {"batch_size": 5},
        }
        op = resolve_operator_from_config(config)
        assert isinstance(op, Batch)

    def test_resolve_unknown_raises(self):
        config = {
            "class_name": "UnknownOp",
            "module_path": "orcapod.core.operators",
            "config": {},
        }
        with pytest.raises(ValueError, match="Unknown operator"):
            resolve_operator_from_config(config)


class TestPipelineFormatVersion:
    def test_version_is_string(self):
        assert isinstance(PIPELINE_FORMAT_VERSION, str)
        assert PIPELINE_FORMAT_VERSION == "0.1.0"


# ---------------------------------------------------------------------------
# Arrow type string parser
# ---------------------------------------------------------------------------


class TestParseArrowTypeStringPrimitives:
    """parse_arrow_type_string for primitive (non-nested) types."""

    @pytest.mark.parametrize(
        "type_str, expected",
        [
            ("int8", pa.int8()),
            ("int16", pa.int16()),
            ("int32", pa.int32()),
            ("int64", pa.int64()),
            ("uint8", pa.uint8()),
            ("uint16", pa.uint16()),
            ("uint32", pa.uint32()),
            ("uint64", pa.uint64()),
            ("float16", pa.float16()),
            ("float32", pa.float32()),
            ("float64", pa.float64()),
            ("double", pa.float64()),
            ("string", pa.string()),
            ("utf8", pa.utf8()),
            ("large_string", pa.large_string()),
            ("large_utf8", pa.large_utf8()),
            ("binary", pa.binary()),
            ("large_binary", pa.large_binary()),
            ("bool_", pa.bool_()),
            ("bool", pa.bool_()),
            ("date32", pa.date32()),
            ("date64", pa.date64()),
            ("null", pa.null()),
        ],
    )
    def test_primitive_type(self, type_str, expected):
        assert parse_arrow_type_string(type_str) == expected

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown Arrow type"):
            parse_arrow_type_string("not_a_type")

    def test_strips_whitespace(self):
        assert parse_arrow_type_string("  int64  ") == pa.int64()


class TestParseArrowTypeStringNested:
    """parse_arrow_type_string for nested (parameterized) types."""

    def test_list(self):
        assert parse_arrow_type_string("list<item: int64>") == pa.list_(pa.int64())

    def test_large_list(self):
        assert parse_arrow_type_string(
            "large_list<item: large_string>"
        ) == pa.large_list(pa.large_string())

    def test_struct_single_field(self):
        assert parse_arrow_type_string("struct<a: int64>") == pa.struct(
            [("a", pa.int64())]
        )

    def test_struct_multiple_fields(self):
        result = parse_arrow_type_string("struct<a: int64, b: large_string>")
        expected = pa.struct([("a", pa.int64()), ("b", pa.large_string())])
        assert result == expected

    def test_map(self):
        assert parse_arrow_type_string("map<large_string, int64>") == pa.map_(
            pa.large_string(), pa.int64()
        )

    def test_list_of_struct(self):
        result = parse_arrow_type_string(
            "list<item: struct<x: int64, y: large_string>>"
        )
        expected = pa.list_(pa.struct([("x", pa.int64()), ("y", pa.large_string())]))
        assert result == expected

    def test_struct_with_nested_list_and_map(self):
        type_str = "struct<a: list<item: int64>, b: map<large_string, int64>>"
        result = parse_arrow_type_string(type_str)
        expected = pa.struct(
            [
                ("a", pa.list_(pa.int64())),
                ("b", pa.map_(pa.large_string(), pa.int64())),
            ]
        )
        assert result == expected

    def test_list_of_list(self):
        result = parse_arrow_type_string("list<item: list<item: int64>>")
        expected = pa.list_(pa.list_(pa.int64()))
        assert result == expected

    def test_map_with_struct_value(self):
        type_str = "map<large_string, struct<x: int64, y: float64>>"
        result = parse_arrow_type_string(type_str)
        expected = pa.map_(
            pa.large_string(),
            pa.struct([("x", pa.int64()), ("y", pa.float64())]),
        )
        assert result == expected

    def test_unknown_nested_type_raises(self):
        with pytest.raises(ValueError, match="Unknown nested Arrow type"):
            parse_arrow_type_string("frobnicate<int64>")


# ---------------------------------------------------------------------------
# Schema serialization round-trip
# ---------------------------------------------------------------------------


class TestSerializeSchema:
    """serialize_schema produces correct Arrow type strings."""

    def test_simple_python_types(self):
        from orcapod.contexts import resolve_context

        tc = resolve_context(None).type_converter
        schema = Schema({"x": int, "y": str})
        result = serialize_schema(schema, type_converter=tc)
        assert result["x"] == "int64"
        assert result["y"] == "large_string"

    def test_without_type_converter_uses_str(self):
        schema = {"a": int, "b": str}
        result = serialize_schema(schema)
        # Without converter, values are str() of the Python type
        assert result["a"] == str(int)
        assert result["b"] == str(str)


class TestDeserializeSchema:
    """deserialize_schema recovers Python types from Arrow type strings."""

    def test_simple_types(self):
        serialized = {"x": "int64", "y": "large_string"}
        result = deserialize_schema(serialized)
        assert result["x"] is int
        assert result["y"] is str

    def test_unknown_type_falls_back_to_string(self):
        serialized = {"x": "int64", "mystery": "unknown_type_xyz"}
        result = deserialize_schema(serialized)
        assert result["x"] is int
        assert result["mystery"] == "unknown_type_xyz"


class TestSchemaRoundTrip:
    """Full serialize → deserialize round-trip preserves Python types."""

    @staticmethod
    def _round_trip(python_schema: dict[str, type]) -> dict[str, type]:
        from orcapod.contexts import resolve_context

        tc = resolve_context(None).type_converter
        serialized = serialize_schema(Schema(python_schema), type_converter=tc)
        return deserialize_schema(serialized, type_converter=tc)

    def test_basic_types(self):
        schema = {"a": int, "b": str, "c": float, "d": bool}
        result = self._round_trip(schema)
        assert result == schema

    def test_list_type(self):
        schema = {"items": list[int]}
        result = self._round_trip(schema)
        assert result == schema

    def test_nested_list(self):
        schema = {"matrix": list[list[int]]}
        result = self._round_trip(schema)
        assert result == schema

    def test_dict_type(self):
        schema = {"mapping": dict[str, int]}
        result = self._round_trip(schema)
        assert result == schema

    def test_mixed_types(self):
        schema = {
            "id": int,
            "name": str,
            "score": float,
            "tags": list[str],
            "active": bool,
        }
        result = self._round_trip(schema)
        assert result == schema

    def test_preserves_field_order(self):
        schema = {"z": int, "a": str, "m": float}
        result = self._round_trip(schema)
        assert list(result.keys()) == ["z", "a", "m"]


# ---------------------------------------------------------------------------
# PacketFunction proxy fallback
# ---------------------------------------------------------------------------


class TestResolvePacketFunctionFallbackToProxy:
    """resolve_packet_function_from_config with fallback_to_proxy."""

    def test_resolve_packet_function_fallback_to_proxy(self):
        from orcapod.core.packet_function_proxy import PacketFunctionProxy
        from orcapod.pipeline.serialization import resolve_packet_function_from_config

        config = {
            "packet_function_type_id": "python.function.v0",
            "config": {
                "module_path": "nonexistent.module.that.does.not.exist",
                "callable_name": "some_func",
                "version": "v1.0",
                "input_packet_schema": {"x": "int64"},
                "output_packet_schema": {"y": "float64"},
                "output_keys": ["y"],
            },
        }
        # Without fallback, should raise
        with pytest.raises(Exception):
            resolve_packet_function_from_config(config)

        # With fallback, should return proxy
        result = resolve_packet_function_from_config(config, fallback_to_proxy=True)
        assert isinstance(result, PacketFunctionProxy)
        assert result.canonical_function_name == "some_func"

    def test_unknown_type_id_fallback(self):
        from orcapod.core.packet_function_proxy import PacketFunctionProxy
        from orcapod.pipeline.serialization import resolve_packet_function_from_config

        config = {
            "packet_function_type_id": "unknown.type.v99",
            "config": {
                "callable_name": "mystery",
                "version": "v1.0",
                "input_packet_schema": {"a": "int64"},
                "output_packet_schema": {"b": "int64"},
                "output_keys": ["b"],
            },
        }
        with pytest.raises(ValueError, match="Unknown packet function type"):
            resolve_packet_function_from_config(config)

        result = resolve_packet_function_from_config(config, fallback_to_proxy=True)
        assert isinstance(result, PacketFunctionProxy)
        assert result.canonical_function_name == "mystery"


# ---------------------------------------------------------------------------
# FunctionPod.from_config proxy fallback
# ---------------------------------------------------------------------------


class TestFunctionPodFromConfigFallbackToProxy:
    """FunctionPod.from_config with fallback_to_proxy."""

    def test_function_pod_from_config_fallback_to_proxy(self):
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function_proxy import PacketFunctionProxy

        config = {
            "uri": ["some_func", "hash123", "v1", "python.function.v0"],
            "packet_function": {
                "packet_function_type_id": "python.function.v0",
                "config": {
                    "module_path": "nonexistent.module",
                    "callable_name": "some_func",
                    "version": "v1.0",
                    "input_packet_schema": {"x": "int64"},
                    "output_packet_schema": {"y": "float64"},
                    "output_keys": ["y"],
                },
            },
            "node_config": None,
        }

        with pytest.raises(Exception):
            FunctionPod.from_config(config)

        pod = FunctionPod.from_config(config, fallback_to_proxy=True)
        assert isinstance(pod.packet_function, PacketFunctionProxy)
        assert pod.packet_function.canonical_function_name == "some_func"
        # URI should be injected from parent config
        assert pod.packet_function.uri == (
            "some_func",
            "hash123",
            "v1",
            "python.function.v0",
        )
