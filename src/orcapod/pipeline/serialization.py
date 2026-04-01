"""Pipeline serialization registries, helpers, and constants."""

from __future__ import annotations

import hashlib
import json as _json
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Format version
# ---------------------------------------------------------------------------

PIPELINE_FORMAT_VERSION = "0.1.0"
SUPPORTED_FORMAT_VERSIONS = frozenset({"0.1.0"})

# ---------------------------------------------------------------------------
# LoadStatus
# ---------------------------------------------------------------------------


class LoadStatus(Enum):
    """Status of a node after loading from a serialized pipeline."""

    FULL = "full"
    READ_ONLY = "read_only"
    CACHE_ONLY = "cache_only"
    UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# DatabaseRegistry
# ---------------------------------------------------------------------------


class DatabaseRegistry:
    """Deduplicates database configs into a keyed registry at save time.

    Keys are deterministic: same config → same key, cross-session stable.
    Format: ``db_{sha256[:8]}`` with ``_2``, ``_3``... suffixes on collision.
    """

    def __init__(self) -> None:
        self._entries: dict[str, dict] = {}
        # Reverse map: canonical JSON string → assigned key, for O(1) re-registration
        self._reverse: dict[str, str] = {}

    def register(self, config: dict) -> str:
        """Register a database config and return its registry key.

        If the config is already registered (under any key), returns the existing key.
        On hash collision (different configs with same 8-char prefix), appends
        ``_2``, ``_3``... to the key.
        """
        canonical = _json.dumps(config, sort_keys=True, separators=(",", ":"))

        # O(1) lookup via reverse map
        if canonical in self._reverse:
            return self._reverse[canonical]

        # Not registered yet — find the right key
        base_key = self._make_key(config)
        if base_key not in self._entries:
            key = base_key
        else:
            i = 2
            while f"{base_key}_{i}" in self._entries:
                i += 1
            key = f"{base_key}_{i}"

        self._entries[key] = config
        self._reverse[canonical] = key
        return key

    def resolve(self, key: str) -> dict:
        """Return the config dict for a registry key."""
        return self._entries[key]

    def to_dict(self) -> dict[str, dict]:
        """Return the full registry for JSON serialization."""
        return dict(self._entries)

    @classmethod
    def from_dict(cls, data: dict[str, dict]) -> DatabaseRegistry:
        """Reconstruct a registry from the saved ``databases`` block."""
        registry = cls()
        registry._entries = dict(data)
        registry._reverse = {
            _json.dumps(cfg, sort_keys=True, separators=(",", ":")): key
            for key, cfg in data.items()
        }
        return registry

    @staticmethod
    def _make_key(config: dict) -> str:
        canonical = _json.dumps(config, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()[:8]
        return f"db_{digest}"


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------


def _build_database_registry() -> dict[str, type]:
    """Build the database type registry mapping type keys to classes.

    Returns:
        Dict mapping type key strings to database classes.
    """
    from orcapod.databases.delta_lake_databases import DeltaTableDatabase
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.databases.noop_database import NoOpArrowDatabase

    return {
        "delta_table": DeltaTableDatabase,
        "in_memory": InMemoryArrowDatabase,
        "noop": NoOpArrowDatabase,
    }


def _build_source_registry() -> dict[str, type]:
    """Build the source type registry mapping type keys to classes.

    Returns:
        Dict mapping type key strings to source classes.
    """
    from orcapod.core.sources.arrow_table_source import ArrowTableSource
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.csv_source import CSVSource
    from orcapod.core.sources.data_frame_source import DataFrameSource
    from orcapod.core.sources.delta_table_source import DeltaTableSource
    from orcapod.core.sources.dict_source import DictSource
    from orcapod.core.sources.list_source import ListSource
    from orcapod.core.sources.spiraldb_table_source import SpiralDBTableSource
    from orcapod.core.sources.sqlite_table_source import SQLiteTableSource
    from orcapod.core.sources.postgresql_table_source import PostgreSQLTableSource

    return {
        "csv": CSVSource,
        "delta_table": DeltaTableSource,
        "dict": DictSource,
        "list": ListSource,
        "data_frame": DataFrameSource,
        "arrow_table": ArrowTableSource,
        "cached": CachedSource,
        "spiraldb_table": SpiralDBTableSource,
        "sqlite_table": SQLiteTableSource,
        "postgresql_table": PostgreSQLTableSource,
    }


def _build_operator_registry() -> dict[str, type]:
    """Build the operator type registry mapping class names to classes.

    Returns:
        Dict mapping class name strings to operator classes.
    """
    from orcapod.core.operators import (
        Batch,
        DropPacketColumns,
        DropTagColumns,
        Join,
        MapPackets,
        MapTags,
        MergeJoin,
        PolarsFilter,
        SelectPacketColumns,
        SelectTagColumns,
        SemiJoin,
    )

    return {
        "Join": Join,
        "MergeJoin": MergeJoin,
        "SemiJoin": SemiJoin,
        "Batch": Batch,
        "SelectTagColumns": SelectTagColumns,
        "DropTagColumns": DropTagColumns,
        "SelectPacketColumns": SelectPacketColumns,
        "DropPacketColumns": DropPacketColumns,
        "MapTags": MapTags,
        "MapPackets": MapPackets,
        "PolarsFilter": PolarsFilter,
    }


def _build_packet_function_registry() -> dict[str, type]:
    """Build the packet function type registry mapping type IDs to classes.

    Returns:
        Dict mapping type ID strings to packet function classes.
    """
    from orcapod.core.packet_function import PythonPacketFunction

    return {
        "python.function.v0": PythonPacketFunction,
    }


# Registries populated at module load time.
# Each registry maps a type key (or class name) to the corresponding class.
DATABASE_REGISTRY: dict[str, type] = _build_database_registry()
SOURCE_REGISTRY: dict[str, type] = _build_source_registry()
OPERATOR_REGISTRY: dict[str, type] = _build_operator_registry()
PACKET_FUNCTION_REGISTRY: dict[str, type] = _build_packet_function_registry()


def _ensure_registries() -> None:
    """Ensure registries are populated.

    The registries are built at module import time, so this is a no-op in
    normal use.  It exists as a hook for tests or code that calls
    ``register_*`` helpers before the first resolver call.
    """
    # Registries are already populated at module load; nothing to do.


# ---------------------------------------------------------------------------
# Resolver helpers
# ---------------------------------------------------------------------------


def resolve_database_from_config(config: dict[str, Any], db_registry: dict | None = None) -> Any:
    """Reconstruct a database instance from a config dict.

    Args:
        config: Dict with at least a ``"type"`` key matching a registered
            database type, or ``"type": "scoped"`` for a scoped database
            reference.
        db_registry: Optional plain dict mapping ``"db_<key>"`` → config dict
            (the output of ``DatabaseRegistry.to_dict()``).  Required when
            ``config["type"] == "scoped"``; ignored otherwise.

    Returns:
        A new database instance constructed from the config.

    Raises:
        ValueError: If the ``"type"`` key is missing or unknown, or if
            ``config["type"] == "scoped"`` and no db_registry is provided.
    """
    _ensure_registries()
    db_type = config.get("type")
    if db_type == "scoped":
        if db_registry is None:
            raise ValueError("db_registry required to resolve scoped database config")
        ref = config["ref"]
        if ref not in db_registry:
            raise ValueError(f"Registry key {ref!r} not found; available keys: {sorted(db_registry.keys())}")
        root_config = db_registry[ref]
        root_db = resolve_database_from_config(root_config)
        return root_db.at(*config["path"])
    if db_type not in DATABASE_REGISTRY:
        raise ValueError(
            f"Unknown database type: {db_type!r}. "
            f"Known types: {sorted(DATABASE_REGISTRY.keys())}"
        )
    if db_type == "in_memory":
        logger.warning(
            "Loading pipeline with in-memory database. Cached data from the "
            "original run is not available — nodes will have UNAVAILABLE status."
        )
    cls = DATABASE_REGISTRY[db_type]
    return cls.from_config(config)


def resolve_operator_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct an operator instance from a config dict.

    Args:
        config: Dict with at least a ``"class_name"`` key matching a registered
            operator class.

    Returns:
        A new operator instance constructed from the config.

    Raises:
        ValueError: If the ``"class_name"`` key is missing or unknown.
    """
    _ensure_registries()
    class_name = config.get("class_name")
    if class_name not in OPERATOR_REGISTRY:
        raise ValueError(
            f"Unknown operator: {class_name!r}. "
            f"Known operators: {sorted(OPERATOR_REGISTRY.keys())}"
        )
    cls = OPERATOR_REGISTRY[class_name]
    return cls.from_config(config)


def resolve_packet_function_from_config(
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
) -> Any:
    """Reconstruct a packet function from a config dict.

    Args:
        config: Dict with at least a ``"packet_function_type_id"`` key matching
            a registered packet function type.
        fallback_to_proxy: If ``True`` and reconstruction fails (unknown type,
            ``ImportError``, ``AttributeError``, etc.), return a
            ``PacketFunctionProxy`` instead of raising.

    Returns:
        A new packet function instance constructed from the config, or a
        ``PacketFunctionProxy`` if reconstruction fails and
        *fallback_to_proxy* is ``True``.

    Raises:
        ValueError: If the type ID is missing or unknown and
            *fallback_to_proxy* is ``False``.
    """
    _ensure_registries()
    type_id = config.get("packet_function_type_id")
    if type_id not in PACKET_FUNCTION_REGISTRY:
        if fallback_to_proxy:
            from orcapod.core.packet_function_proxy import PacketFunctionProxy

            return PacketFunctionProxy.from_config(config)
        raise ValueError(
            f"Unknown packet function type: {type_id!r}. "
            f"Known types: {sorted(PACKET_FUNCTION_REGISTRY.keys())}"
        )
    cls = PACKET_FUNCTION_REGISTRY[type_id]
    try:
        return cls.from_config(config)
    except (ImportError, ModuleNotFoundError, AttributeError) as exc:
        if fallback_to_proxy:
            logger.warning(
                "Could not reconstruct packet function from config (%s); "
                "returning PacketFunctionProxy.",
                exc,
            )
            from orcapod.core.packet_function_proxy import PacketFunctionProxy

            return PacketFunctionProxy.from_config(config)
        raise


def resolve_source_from_config(
    config: dict[str, Any],
    *,
    fallback_to_proxy: bool = False,
    db_registry: DatabaseRegistryProtocol | None = None,
    node_descriptor: dict[str, Any] | None = None,
) -> Any:
    """Reconstruct a source instance from a config dict.

    Args:
        config: Dict with at least a ``"source_type"`` key matching a registered
            source type.
        fallback_to_proxy: If ``True`` and reconstruction fails, return a
            ``SourceProxy`` preserving identity hashes and schemas.
            Identity fields (``content_hash``, ``pipeline_hash``, and
            ``output_schema``) are taken from *node_descriptor* when provided,
            or from *config* when available (e.g. inner sources embedded inside
            a ``CachedSource`` always include identity via ``_identity_config()``).
        db_registry: Optional registry forwarded to ``from_config`` for all
            source types.  Sources that do not embed database references ignore
            it; ``CachedSource`` uses it to resolve its cache database key.
        node_descriptor: Optional node descriptor dict (new save format) containing
            identity fields used when creating a ``SourceProxy`` fallback.
            If omitted, identity is inferred from *config* where possible.

    Returns:
        A new source instance constructed from the config, or a ``SourceProxy``
        if reconstruction fails and *fallback_to_proxy* is ``True``.

    Raises:
        ValueError: If the source type is missing or unknown.
        Exception: Re-raised from ``from_config`` when *fallback_to_proxy* is
            ``False`` and reconstruction fails.
    """
    _ensure_registries()
    source_type = config.get("source_type")
    if source_type not in SOURCE_REGISTRY:
        if fallback_to_proxy:
            return _source_proxy_from_config(config, node_descriptor=node_descriptor)
        raise ValueError(
            f"Unknown source type: {source_type!r}. "
            f"Known types: {sorted(SOURCE_REGISTRY.keys())}"
        )
    cls = SOURCE_REGISTRY[source_type]
    try:
        # All sources accept db_registry — always forward it.
        # Sources that don't embed database references simply ignore it.
        return cls.from_config(config, db_registry=db_registry)
    except Exception:
        if fallback_to_proxy:
            logger.warning(
                "Could not reconstruct %s source; returning SourceProxy.",
                source_type,
            )
            return _source_proxy_from_config(config, node_descriptor=node_descriptor)
        raise


def _source_proxy_from_config(
    config: dict[str, Any],
    node_descriptor: dict[str, Any] | None = None,
) -> Any:
    """Create a SourceProxy from identity fields.

    Identity fields are read from ``node_descriptor`` when provided (new
    format for top-level pipeline nodes).  When ``node_descriptor`` is
    ``None``, identity is read directly from ``config`` — this supports
    inner sources (e.g. the wrapped source inside a ``CachedSource``) which
    always embed identity via ``_identity_config()``.

    Args:
        config: Source config dict.  Always consulted for ``source_type``
            and ``source_id``; also used for identity fields when
            ``node_descriptor`` is ``None``.
        node_descriptor: Node descriptor dict (new format) containing
            ``content_hash``, ``pipeline_hash``, and ``output_schema``.
            When provided, identity fields are taken from here rather than
            from ``config``.

    Returns:
        A ``SourceProxy`` preserving the original source's identity.

    Raises:
        ValueError: If required identity fields are not available from
            either ``node_descriptor`` or ``config``.
    """
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.types import Schema

    if node_descriptor is not None:
        # New format: top-level node descriptor carries identity
        content_hash = node_descriptor.get("content_hash")
        pipeline_hash_val = node_descriptor.get("pipeline_hash")
        output_schema = node_descriptor.get("output_schema", {})
        tag_schema_dict = output_schema.get("tag", {})
        packet_schema_dict = output_schema.get("packet", {})
    else:
        # Inner sources (e.g. inside CachedSource) embed identity via _identity_config()
        content_hash = config.get("content_hash")
        pipeline_hash_val = config.get("pipeline_hash")
        tag_schema_dict = config.get("tag_schema", {})
        packet_schema_dict = config.get("packet_schema", {})

    if not content_hash or not pipeline_hash_val:
        raise ValueError(
            "Cannot create SourceProxy: missing identity fields. "
            "For top-level pipeline nodes, pass node_descriptor with "
            "content_hash, pipeline_hash, and output_schema. "
            "For inner sources, ensure the source config includes "
            "_identity_config() fields."
        )

    source_type = config.get("source_type")
    expected_class_name: str | None = None
    if source_type and source_type in SOURCE_REGISTRY:
        expected_class_name = SOURCE_REGISTRY[source_type].__name__

    tag_schema = Schema(deserialize_schema(tag_schema_dict))
    packet_schema = Schema(deserialize_schema(packet_schema_dict))

    return SourceProxy(
        source_id=config.get("source_id", "unknown"),
        content_hash_str=content_hash,
        pipeline_hash_str=pipeline_hash_val,
        tag_schema=tag_schema,
        packet_schema=packet_schema,
        expected_class_name=expected_class_name,
        source_config=config,
    )


# ---------------------------------------------------------------------------
# Registration helpers (extensibility)
# ---------------------------------------------------------------------------


def register_database(type_key: str, cls: type) -> None:
    """Register a custom database implementation for deserialization.

    Args:
        type_key: The string key to use in serialized configs.
        cls: The database class to register.
    """
    _ensure_registries()
    DATABASE_REGISTRY[type_key] = cls


def register_source(type_key: str, cls: type) -> None:
    """Register a custom source implementation for deserialization.

    Args:
        type_key: The string key to use in serialized configs.
        cls: The source class to register.
    """
    _ensure_registries()
    SOURCE_REGISTRY[type_key] = cls


def register_operator(class_name: str, cls: type) -> None:
    """Register a custom operator implementation for deserialization.

    Args:
        class_name: The class name string to use in serialized configs.
        cls: The operator class to register.
    """
    _ensure_registries()
    OPERATOR_REGISTRY[class_name] = cls


def register_packet_function(type_id: str, cls: type) -> None:
    """Register a custom packet function implementation for deserialization.

    Args:
        type_id: The type ID string to use in serialized configs.
        cls: The packet function class to register.
    """
    _ensure_registries()
    PACKET_FUNCTION_REGISTRY[type_id] = cls


# ---------------------------------------------------------------------------
# Schema serialization helpers
# ---------------------------------------------------------------------------


def serialize_schema(schema: Any, type_converter: Any | None = None) -> dict[str, str]:
    """Convert a Schema mapping to JSON-serializable Arrow type strings.

    The result contains human-readable Arrow type strings for each field
    (e.g. ``"int64"``, ``"large_string"``, ``"list<item: int64>"``).
    These strings follow Arrow's canonical format and can be parsed back
    by `deserialize_schema` in any language that implements the
    Arrow type grammar.

    Args:
        schema: A Schema-like mapping from field name to data type.
        type_converter: Optional type converter for Python→Arrow conversion.
            When provided, Python types (e.g. ``int``, ``str``) are converted
            to Arrow type strings (e.g. ``"int64"``, ``"large_string"``).
            When ``None``, values are stringified directly.

    Returns:
        A dict mapping field names to Arrow type string representations.
    """
    if type_converter is not None:
        result = {}
        for k, v in schema.items():
            try:
                arrow_type = type_converter.python_type_to_arrow_type(v)
                result[k] = str(arrow_type)
            except Exception:
                result[k] = str(v)
        return result
    return {k: str(v) for k, v in schema.items()}


def deserialize_schema(
    schema_dict: dict[str, str],
    type_converter: Any | None = None,
) -> dict[str, Any]:
    """Reconstruct a Python-type schema from Arrow type string values.

    Parses Arrow type strings (e.g. ``"int64"``, ``"list<item: int64>"``)
    back into ``pa.DataType`` objects, then converts them to Python types
    via the type converter.  Falls back to raw strings for fields that
    cannot be parsed.

    Args:
        schema_dict: Dict mapping field names to Arrow type strings, as
            produced by `serialize_schema`.
        type_converter: Optional type converter for Arrow→Python conversion.
            When ``None``, the default data context's converter is used.

    Returns:
        A dict mapping field names to Python types (or raw strings if
        parsing fails).
    """
    if type_converter is None:
        from orcapod.contexts import resolve_context

        type_converter = resolve_context(None).type_converter

    result: dict[str, Any] = {}
    for name, type_str in schema_dict.items():
        try:
            arrow_type = parse_arrow_type_string(type_str)
            result[name] = type_converter.arrow_type_to_python_type(arrow_type)
        except Exception:
            result[name] = type_str
    return result


def parse_arrow_type_string(type_str: str) -> Any:
    """Parse an Arrow type string into a ``pa.DataType``.

    Handles both primitive types (``"int64"``, ``"large_string"``) and
    nested types (``"list<item: int64>"``, ``"struct<a: int64, b: string>"``,
    ``"map<string, int64>"``).

    The grammar follows Arrow's canonical ``str(pa.DataType)`` output.

    Args:
        type_str: Arrow type string to parse.

    Returns:
        The corresponding ``pa.DataType``.

    Raises:
        ValueError: If the type string cannot be parsed.
    """
    import pyarrow as pa

    type_str = type_str.strip()

    # Primitive types — try direct lookup via pa.<name>()
    if "<" not in type_str:
        factory = _get_arrow_primitive_types().get(type_str)
        if factory is not None:
            return factory()
        raise ValueError(f"Unknown Arrow type: {type_str!r}")

    # Nested types — parse the outer type and recurse
    bracket_pos = type_str.index("<")
    outer = type_str[:bracket_pos].strip()
    inner = type_str[bracket_pos + 1 : -1].strip()  # strip < and >

    if outer in ("list", "large_list"):
        # "list<item: int64>" or "list<item: struct<...>>"
        child_type_str = _strip_field_name(inner)
        child_type = parse_arrow_type_string(child_type_str)
        return (
            pa.large_list(child_type) if outer == "large_list" else pa.list_(child_type)
        )

    if outer == "struct":
        # "struct<a: int64, b: string>" — split on top-level commas
        fields = _split_struct_fields(inner)
        pa_fields = []
        for field_str in fields:
            colon_pos = field_str.index(":")
            field_name = field_str[:colon_pos].strip()
            field_type_str = field_str[colon_pos + 1 :].strip()
            pa_fields.append(
                pa.field(field_name, parse_arrow_type_string(field_type_str))
            )
        return pa.struct(pa_fields)

    if outer == "map":
        # "map<large_string, int64>" — split on first top-level comma
        parts = _split_top_level(inner, ",", max_splits=1)
        if len(parts) != 2:
            raise ValueError(f"Cannot parse map type: {type_str!r}")
        key_type = parse_arrow_type_string(parts[0].strip())
        value_type = parse_arrow_type_string(parts[1].strip())
        return pa.map_(key_type, value_type)

    raise ValueError(f"Unknown nested Arrow type: {type_str!r}")


def _strip_field_name(s: str) -> str:
    """Strip the ``"item: "`` or ``"field_name: "`` prefix from a child type string."""
    if ":" in s:
        # Only strip if there's no nested '<' before the ':'
        colon_pos = s.index(":")
        bracket_pos = s.find("<")
        if bracket_pos == -1 or colon_pos < bracket_pos:
            return s[colon_pos + 1 :].strip()
    return s


def _split_top_level(s: str, sep: str, max_splits: int = -1) -> list[str]:
    """Split *s* on *sep*, but only at the top level (not inside ``<>``).

    Args:
        s: String to split.
        sep: Separator character.
        max_splits: Maximum number of splits (-1 for unlimited).

    Returns:
        List of substrings.
    """
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for ch in s:
        if ch == "<":
            depth += 1
        elif ch == ">":
            depth -= 1
        if ch == sep and depth == 0 and (max_splits == -1 or len(parts) < max_splits):
            parts.append("".join(current))
            current = []
            continue
        current.append(ch)
    parts.append("".join(current))
    return parts


def _split_struct_fields(inner: str) -> list[str]:
    """Split struct fields on top-level commas."""
    return _split_top_level(inner, ",")


def _build_arrow_primitive_types() -> dict[str, Any]:
    """Build a mapping of Arrow type string names to factory callables."""
    import pyarrow as pa

    types = {}
    for name in [
        "null",
        "bool_",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "string",
        "utf8",
        "large_string",
        "large_utf8",
        "binary",
        "large_binary",
        "date32",
        "date64",
        "time32",
        "time64",
        "duration",
        "timestamp",
    ]:
        factory = getattr(pa, name, None)
        if factory is not None:
            types[name] = factory
            # Also map the str() output if it differs from the factory name
            try:
                canonical = str(factory())
                if canonical != name:
                    types[canonical] = factory
            except Exception:
                pass
    # Common aliases
    types["double"] = pa.float64
    types["float"] = pa.float32
    types["bool"] = pa.bool_
    return types


# Initialized to None and built on first use by _get_arrow_primitive_types() so
# that importing this module does not trigger an eager pyarrow import.  pyarrow
# is only needed when actually deserializing an Arrow type string, not at module
# load time.
_ARROW_PRIMITIVE_TYPES: dict[str, Any] | None = None


def _get_arrow_primitive_types() -> dict[str, Any]:
    """Return the Arrow primitive types map, building it on first call."""
    global _ARROW_PRIMITIVE_TYPES
    if _ARROW_PRIMITIVE_TYPES is None:
        _ARROW_PRIMITIVE_TYPES = _build_arrow_primitive_types()
    return _ARROW_PRIMITIVE_TYPES


# ---------------------------------------------------------------------------
# Builtin Python type map for schema deserialization from to_config() format
# ---------------------------------------------------------------------------

_BUILTIN_TYPE_MAP: dict[str, type] = {
    "<class 'int'>": int,
    "<class 'float'>": float,
    "<class 'str'>": str,
    "<class 'bool'>": bool,
    "<class 'bytes'>": bytes,
}
