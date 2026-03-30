"""Tests for pipeline serialization registries and helpers."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.pipeline.serialization import (
    DATABASE_REGISTRY,
    OPERATOR_REGISTRY,
    PACKET_FUNCTION_REGISTRY,
    SOURCE_REGISTRY,
    DatabaseRegistry,
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


def test_database_registry_register_returns_key():
    reg = DatabaseRegistry()
    config = {"type": "delta_table", "base_path": "/tmp/db"}
    key = reg.register(config)
    assert key.startswith("db_")
    assert len(key) == len("db_") + 8  # db_ + 8 hex chars


def test_database_registry_same_config_same_key():
    reg = DatabaseRegistry()
    config = {"type": "delta_table", "base_path": "/tmp/db"}
    key1 = reg.register(config)
    key2 = reg.register(config)
    assert key1 == key2


def test_database_registry_different_config_different_key():
    reg = DatabaseRegistry()
    key1 = reg.register({"type": "delta_table", "base_path": "/tmp/a"})
    key2 = reg.register({"type": "delta_table", "base_path": "/tmp/b"})
    assert key1 != key2


def test_database_registry_collision_handling():
    """Simulate a collision: same 8-char prefix, different configs."""
    reg = DatabaseRegistry()
    # Inject a config under a known key
    reg._entries["db_aabbccdd"] = {"type": "delta_table", "base_path": "/a"}
    # Patch _make_key to always return the same key prefix, simulating collision
    original_make_key = staticmethod(DatabaseRegistry._make_key)
    DatabaseRegistry._make_key = staticmethod(lambda config: "db_aabbccdd")
    try:
        key2 = reg.register({"type": "delta_table", "base_path": "/b"})
        assert key2 == "db_aabbccdd_2"
        key3 = reg.register({"type": "delta_table", "base_path": "/c"})
        assert key3 == "db_aabbccdd_3"
    finally:
        DatabaseRegistry._make_key = original_make_key


def test_database_registry_to_dict():
    reg = DatabaseRegistry()
    config = {"type": "in_memory"}
    key = reg.register(config)
    d = reg.to_dict()
    assert key in d
    assert d[key] == config


def test_database_registry_from_dict():
    data = {"db_abc12345": {"type": "delta_table", "base_path": "/x"}}
    reg = DatabaseRegistry.from_dict(data)
    assert reg.resolve("db_abc12345") == {"type": "delta_table", "base_path": "/x"}


def test_database_registry_resolve_unknown_raises():
    reg = DatabaseRegistry()
    with pytest.raises(KeyError):
        reg.resolve("db_nonexistent")


def test_database_registry_key_is_deterministic():
    """Same config always produces same key, even across fresh instances."""
    config = {"type": "delta_table", "base_path": "/deterministic"}
    reg1 = DatabaseRegistry()
    reg2 = DatabaseRegistry()
    key1 = reg1.register(config)
    key2 = reg2.register(config)
    assert key1 == key2


def test_database_registry_key_is_content_based():
    """Key depends on config content, not insertion order."""
    config_a = {"b": 2, "a": 1}
    config_b = {"a": 1, "b": 2}
    reg = DatabaseRegistry()
    key_a = reg.register(config_a)
    key_b = reg.register(config_b)
    # same logical content → same key
    assert key_a == key_b


def test_database_registry_collision_suffix_idempotent():
    """Re-registering a collision-suffix config returns the existing key, not a new one."""
    reg = DatabaseRegistry()
    # Patch _make_key to always return same prefix, simulating collision
    original_make_key = staticmethod(DatabaseRegistry._make_key)
    DatabaseRegistry._make_key = staticmethod(lambda config: "db_aabbccdd")
    try:
        key1 = reg.register({"type": "delta_table", "base_path": "/a"})
        assert key1 == "db_aabbccdd"
        key2 = reg.register({"type": "delta_table", "base_path": "/b"})
        assert key2 == "db_aabbccdd_2"
        # Re-register config /b — must return db_aabbccdd_2, not db_aabbccdd_3
        key2_again = reg.register({"type": "delta_table", "base_path": "/b"})
        assert key2_again == "db_aabbccdd_2"
        assert len(reg.to_dict()) == 2  # no duplicate created
    finally:
        DatabaseRegistry._make_key = original_make_key


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
        # Without fallback, should raise ImportError/ModuleNotFoundError
        with pytest.raises((ImportError, ModuleNotFoundError)):
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
                "uri": ["some_func", "hash123", "v1", "python.function.v0"],
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

        with pytest.raises((ImportError, ModuleNotFoundError)):
            FunctionPod.from_config(config)

        pod = FunctionPod.from_config(config, fallback_to_proxy=True)
        assert isinstance(pod.packet_function, PacketFunctionProxy)
        assert pod.packet_function.canonical_function_name == "some_func"
        # URI comes from the packet function config
        assert pod.packet_function.uri == (
            "some_func",
            "hash123",
            "v1",
            "python.function.v0",
        )


# ---------------------------------------------------------------------------
# Schema round-trip consistency
# ---------------------------------------------------------------------------


class TestSchemaRoundTripConsistency:
    """Verify serialize/deserialize round-trips preserve schema structure and hashes."""

    def test_schema_round_trip_consistency(self):
        """Verify deserialize_schema(serialize_schema(schema)) produces equivalent schemas."""
        from orcapod.contexts import resolve_context

        tc = resolve_context(None).type_converter

        schemas = [
            Schema({"x": int, "y": float, "name": str}),
            Schema({"flag": bool, "data": bytes}),
            Schema({"items": list[int], "mapping": dict[str, float]}),
        ]
        for schema in schemas:
            serialized = serialize_schema(schema, type_converter=tc)
            deserialized = Schema(deserialize_schema(serialized, type_converter=tc))
            assert set(deserialized.keys()) == set(schema.keys()), (
                f"Keys mismatch: {set(schema.keys())} vs {set(deserialized.keys())}"
            )

    def test_schema_round_trip_hash_consistency(self):
        """Verify that schema hash is preserved through serialize/deserialize."""
        from orcapod.contexts import resolve_context

        ctx = resolve_context(None)
        hasher = ctx.semantic_hasher
        tc = ctx.type_converter

        schema = Schema({"age": int, "name": str, "score": float})
        original_hash = hasher.hash_object(schema).to_string()

        serialized = serialize_schema(schema, type_converter=tc)
        deserialized = Schema(deserialize_schema(serialized, type_converter=tc))
        round_trip_hash = hasher.hash_object(deserialized).to_string()

        assert original_hash == round_trip_hash, (
            f"Hash diverged: {original_hash} vs {round_trip_hash}"
        )


# ---------------------------------------------------------------------------
# Task 3: CachedSource serialization with DatabaseRegistry
# ---------------------------------------------------------------------------


def test_cached_source_to_config_without_registry_embeds_db():
    """Default: cache_database is an inline dict."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db = InMemoryArrowDatabase()
    inner = DictSource([{"x": 1}, {"x": 2}], source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    config = src.to_config()
    assert isinstance(config["cache_database"], dict)


def test_cached_source_to_config_with_registry_emits_key():
    """With registry: cache_database is a string key."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db = InMemoryArrowDatabase()
    inner = DictSource([{"x": 1}, {"x": 2}], source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    registry = DatabaseRegistry()
    config = src.to_config(db_registry=registry)
    assert isinstance(config["cache_database"], str)
    assert config["cache_database"].startswith("db_")
    assert config["cache_database"] in registry.to_dict()


def test_cached_source_to_config_no_identity_fields():
    """source_config must NOT contain identity fields (content_hash, pipeline_hash, etc.)."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db = InMemoryArrowDatabase()
    inner = DictSource([{"x": 1}], source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    config = src.to_config()
    for field in ("content_hash", "pipeline_hash", "tag_schema", "packet_schema"):
        assert field not in config, f"Identity field {field!r} must not be in source_config"


def test_cached_source_from_config_with_registry_resolves_key():
    """from_config with registry resolves key back to database instance."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db = InMemoryArrowDatabase()
    inner = DictSource([{"x": 1}, {"x": 2}], source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    registry = DatabaseRegistry()
    config = src.to_config(db_registry=registry)
    # Round-trip
    src2 = CachedSource.from_config(config, db_registry=registry)
    assert src2._cache_database is not None


def test_cached_source_from_config_without_registry_uses_inline():
    """from_config without registry works with inline dict (default/old behavior)."""
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db = InMemoryArrowDatabase()
    inner = DictSource([{"x": 1}], source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    config = src.to_config()  # no registry → inline
    src2 = CachedSource.from_config(config)
    assert src2._cache_database is not None


def test_cached_source_to_config_forwards_db_registry_to_inner_source():
    """CachedSource.to_config must forward db_registry to inner source's to_config.

    If the inner source is itself a CachedSource, its cache_database should also
    become a registry key string, not remain an inline dict.
    """
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db_inner = InMemoryArrowDatabase()
    db_outer = InMemoryArrowDatabase()
    base_source = DictSource([{"x": 1}], source_id="base")
    inner_cached = CachedSource(
        source=base_source, cache_database=db_inner, cache_path_prefix=("inner",)
    )
    outer_cached = CachedSource(
        source=inner_cached, cache_database=db_outer, cache_path_prefix=("outer",)
    )

    registry = DatabaseRegistry()
    config = outer_cached.to_config(db_registry=registry)

    assert isinstance(config["cache_database"], str), "outer cache_database should be a key"
    # Inner source's cache_database must also use the registry key, not inline dict
    assert isinstance(config["inner_source"]["cache_database"], str), (
        "inner cache_database should be a registry key when db_registry is forwarded"
    )


def test_nested_cached_source_from_config_forwards_db_registry():
    """CachedSource.from_config must forward db_registry to inner source resolution.

    If the inner source is itself a CachedSource serialized with a registry key
    for its cache_database, from_config must pass the registry through so the
    inner source can resolve the key.  Without forwarding, from_config raises
    ValueError('cache_database is a registry key ... but no db_registry was provided').
    """
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.dict_source import DictSource

    db_inner = InMemoryArrowDatabase()
    db_outer = InMemoryArrowDatabase()
    base_source = DictSource([{"x": 1}], source_id="base")
    inner_cached = CachedSource(
        source=base_source, cache_database=db_inner, cache_path_prefix=("inner",)
    )
    outer_cached = CachedSource(
        source=inner_cached, cache_database=db_outer, cache_path_prefix=("outer",)
    )

    registry = DatabaseRegistry()
    config = outer_cached.to_config(db_registry=registry)
    # Manually replace inner_source cache_database with a registry key to
    # simulate a correctly serialized nested CachedSource config (testing
    # from_config in isolation from the to_config bug).
    inner_db_config = db_inner.to_config()
    inner_key = registry.register(inner_db_config)
    config["inner_source"]["cache_database"] = inner_key

    # Round-trip: from_config must forward db_registry to resolve inner key
    outer2 = CachedSource.from_config(config, db_registry=registry)
    assert outer2._cache_database is not None
    assert outer2._source._cache_database is not None


# ---------------------------------------------------------------------------
# Task 4: _source_proxy_from_config with node_descriptor
# ---------------------------------------------------------------------------


def test_source_proxy_from_node_descriptor_fields():
    """_source_proxy_from_config reads identity from node_descriptor when provided."""
    from orcapod.pipeline.serialization import _source_proxy_from_config

    source_config = {"source_type": "dict", "source_id": "my_src"}
    node_descriptor = {
        "content_hash": "semantic_v0.1:abc123",
        "pipeline_hash": "semantic_v0.1:def456",
        "output_schema": {
            "tag": {"x": "int64"},
            "packet": {"result": "int64"},
        },
    }
    proxy = _source_proxy_from_config(source_config, node_descriptor=node_descriptor)
    assert proxy.content_hash().to_string() == "semantic_v0.1:abc123"


def test_source_proxy_from_config_backward_compat():
    """Without node_descriptor, falls back to reading identity from source_config."""
    from orcapod.pipeline.serialization import _source_proxy_from_config

    source_config = {
        "source_type": "dict",
        "source_id": "my_src",
        "content_hash": "semantic_v0.1:abc123",
        "pipeline_hash": "semantic_v0.1:def456",
        "tag_schema": {"x": "int64"},
        "packet_schema": {"result": "int64"},
    }
    proxy = _source_proxy_from_config(source_config)
    assert proxy.content_hash().to_string() == "semantic_v0.1:abc123"


# ---------------------------------------------------------------------------
# Task 5: node_uri property on FunctionNode and OperatorNode
# ---------------------------------------------------------------------------


def test_function_node_has_node_uri():
    import pyarrow as pa

    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.nodes import FunctionNode
    from orcapod.core.packet_function import PythonPacketFunction
    from orcapod.core.sources import ArrowTableSource
    from orcapod.pipeline import Pipeline

    db = InMemoryArrowDatabase()

    def add_one(x: int) -> int:
        return x + 1

    table = pa.table({"id": pa.array(["a", "b"], type=pa.large_string()), "x": pa.array([1, 2], type=pa.int64())})
    source = ArrowTableSource(table, tag_columns=["id"])
    pf = PythonPacketFunction(add_one, output_keys="result")
    pod = FunctionPod(packet_function=pf)

    pipeline = Pipeline(name="test", pipeline_database=db)
    with pipeline:
        pod(source, label="fn")

    fn_node = pipeline.compiled_nodes["fn"]
    assert isinstance(fn_node, FunctionNode)
    assert hasattr(fn_node, "node_uri")
    uri = fn_node.node_uri
    assert isinstance(uri, tuple)
    assert len(uri) >= 1


def test_function_node_stored_node_uri_from_descriptor():
    """Deserialized FunctionNode must return node_uri from stored value."""
    from orcapod.core.nodes.function_node import FunctionNode

    descriptor = {
        "node_type": "function",
        "label": "fn",
        "content_hash": "semantic_v0.1:abc",
        "pipeline_hash": "semantic_v0.1:def",
        "node_uri": ["add_one", "v0", "python.function.v0", "schema_repr"],
        "output_schema": {"tag": {"x": "int64"}, "packet": {"result": "int64"}},
        "data_context_key": "std:v0.1:default",
    }
    node = FunctionNode.from_descriptor(descriptor, function_pod=None, input_stream=None, databases={})
    assert node.node_uri == ("add_one", "v0", "python.function.v0", "schema_repr")


def test_operator_node_has_node_uri():
    import pyarrow as pa

    from orcapod.core.nodes import OperatorNode
    from orcapod.core.operators import Join
    from orcapod.core.sources import ArrowTableSource
    from orcapod.pipeline import Pipeline

    db = InMemoryArrowDatabase()
    table_a = pa.table({"key": pa.array(["a"], type=pa.large_string()), "val_a": pa.array([10], type=pa.int64())})
    table_b = pa.table(
        {"key": pa.array(["a"], type=pa.large_string()), "val_b": pa.array([1], type=pa.int64())}
    )
    src_a = ArrowTableSource(table_a, tag_columns=["key"])
    src_b = ArrowTableSource(table_b, tag_columns=["key"])

    pipeline = Pipeline(name="test", pipeline_database=db)
    with pipeline:
        Join()(src_a, src_b, label="joined")

    op_node = pipeline.compiled_nodes["joined"]
    assert isinstance(op_node, OperatorNode)
    assert hasattr(op_node, "node_uri")
    uri = op_node.node_uri
    assert isinstance(uri, tuple)
    assert len(uri) >= 1
