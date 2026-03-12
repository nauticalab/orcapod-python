"""Pipeline serialization registries, helpers, and constants."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any

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
    UNAVAILABLE = "unavailable"


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

    return {
        "csv": CSVSource,
        "delta_table": DeltaTableSource,
        "dict": DictSource,
        "list": ListSource,
        "data_frame": DataFrameSource,
        "arrow_table": ArrowTableSource,
        "cached": CachedSource,
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


def resolve_database_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct a database instance from a config dict.

    Args:
        config: Dict with at least a ``"type"`` key matching a registered
            database type.

    Returns:
        A new database instance constructed from the config.

    Raises:
        ValueError: If the ``"type"`` key is missing or unknown.
    """
    _ensure_registries()
    db_type = config.get("type")
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


def resolve_packet_function_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct a packet function from a config dict.

    Args:
        config: Dict with at least a ``"packet_function_type_id"`` key matching
            a registered packet function type.

    Returns:
        A new packet function instance constructed from the config.

    Raises:
        ValueError: If the type ID is missing or unknown.
    """
    _ensure_registries()
    type_id = config.get("packet_function_type_id")
    if type_id not in PACKET_FUNCTION_REGISTRY:
        raise ValueError(
            f"Unknown packet function type: {type_id!r}. "
            f"Known types: {sorted(PACKET_FUNCTION_REGISTRY.keys())}"
        )
    cls = PACKET_FUNCTION_REGISTRY[type_id]
    return cls.from_config(config)


def resolve_source_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct a source instance from a config dict.

    Args:
        config: Dict with at least a ``"source_type"`` key matching a registered
            source type.

    Returns:
        A new source instance constructed from the config.

    Raises:
        ValueError: If the source type is missing or unknown.
    """
    _ensure_registries()
    source_type = config.get("source_type")
    if source_type not in SOURCE_REGISTRY:
        raise ValueError(
            f"Unknown source type: {source_type!r}. "
            f"Known types: {sorted(SOURCE_REGISTRY.keys())}"
        )
    cls = SOURCE_REGISTRY[source_type]
    return cls.from_config(config)


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


def deserialize_schema(schema_dict: dict[str, str]) -> dict[str, str]:
    """Convert an Arrow type string dict back to a schema-compatible mapping.

    Note:
        Full type reconstruction (Arrow type strings → Python types) requires
        ``arrow_type_string_to_python_type()`` on the type converter, which is
        not yet implemented. This function returns the raw string values as-is;
        the strings are primarily informational in descriptors rather than used
        for reconstruction.

    Args:
        schema_dict: Dict mapping field names to Arrow type strings, as
            produced by :func:`serialize_schema`.

    Returns:
        A dict mapping field names to type string values (pass-through).
    """
    return dict(schema_dict)
