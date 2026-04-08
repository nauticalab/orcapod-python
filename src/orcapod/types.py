"""Core type definitions for OrcaPod.

Defines the fundamental data types, type aliases, and data structures used
throughout the OrcaPod framework, including:

    - Type aliases for data values, schemas, paths, and tags.
    - ``Schema`` -- an immutable, hashable mapping of field names to Python types.
    - ``ContentHash`` -- a content-addressable hash pairing a method name with
      a raw digest, with convenience conversions to hex, int, UUID, and base64.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass
from enum import Enum
from types import UnionType
from typing import TYPE_CHECKING, Any, Self, TypeAlias

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols import PacketFunctionExecutorProtocol
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)

# TODO: revisit and consider a way to incorporate older Union type
DataType: TypeAlias = type | UnionType  # | type[Union]
"""A Python type or union of types used to describe the data type of a single
field within a ``Schema``."""

# TODO: accomodate other Path-like objects
PathLike: TypeAlias = str | os.PathLike
"""Convenience alias for any filesystem-path-like object (``str`` or
``os.PathLike``)."""

# TODO: accomodate other common data types such as datetime
TagValue: TypeAlias = int | str | None | Collection["TagValue"]
"""A tag metadata value: an int, string, ``None``, or an arbitrarily nested
collection thereof. Tags are used to label and organise packets and
datagrams."""

PathSet: TypeAlias = PathLike | Collection[PathLike | None]
"""A single path or an arbitrarily nested collection of paths (with optional
``None`` entries). Used when operations need to address multiple files at
once, e.g. batch hashing."""

SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes
"""The simple Python scalar types that have a direct Arrow / Polars
correspondence."""

ExtendedSupportedPythonData: TypeAlias = SupportedNativePythonData | PathSet
"""Native scalar types extended with filesystem paths."""

DataValue: TypeAlias = ExtendedSupportedPythonData | Collection["DataValue"] | None
"""The universe of values that can appear in a packet column -- scalars,
paths, arbitrarily nested collections, or ``None``."""

PacketLike: TypeAlias = Mapping[str, DataValue]
"""A dict-like structure mapping field names to ``DataValue`` entries. Serves
as a lightweight, protocol-free representation of a packet."""

SchemaLike: TypeAlias = Mapping[str, DataType]
"""A dict-like structure mapping field names to ``DataType`` entries.
Accepted wherever a ``Schema`` is expected so callers can pass plain dicts."""


class Schema(Mapping[str, DataType]):
    """Immutable schema representing a mapping of field names to Python types.

    Serves as the canonical internal schema representation in OrcaPod,
    with interop to/from Arrow schemas. Hashable and suitable for use
    in content-addressable contexts.

    Args:
        fields: An optional mapping of field names to their data types.
        **kwargs: Additional field name / type pairs. These are merged with
            ``fields``; keyword arguments take precedence on conflict.

    Example::

        schema = Schema({"x": int, "y": float})
        schema = Schema(x=int, y=float)
    """

    def __init__(
        self,
        fields: Mapping[str, DataType] | None = None,
        optional_fields: Collection[str] | None = None,
        **kwargs: type,
    ) -> None:
        combined = dict(fields or {})
        combined.update(kwargs)
        self._data: dict[str, DataType] = combined
        self._optional: frozenset[str] = frozenset(optional_fields or ())

    # ==================== Mapping interface ====================

    def __getitem__(self, key: str) -> DataType:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        def _fmt(t: DataType) -> str:
            """Return a clean, copy-pasteable name for a DataType value.

            - Plain ``type`` objects (e.g. ``int``, ``bool``) are rendered via
              ``__name__`` so they appear as bare identifiers, not as
              ``<class 'int'>``.
            - ``UnionType`` objects (e.g. ``int | str``) already produce clean
              output via ``str()``.
            - Any other value falls back to ``repr()``.
            """
            if isinstance(t, type):
                return t.__name__
            return str(t)

        inner = ", ".join(f"{k!r}: {_fmt(v)}" for k, v in self._data.items())
        return f"Schema({{{inner}}})"

    # ==================== Value semantics ====================

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Schema):
            return self._data == other._data and self._optional == other._optional
        if isinstance(other, Mapping):
            return self._data == dict(other)
        raise NotImplementedError(
            f"Equality check is not implemented for object of type {type(other)}"
        )

    # ==================== Optionality ====================

    @property
    def optional_fields(self) -> frozenset[str]:
        """Field names that are optional (have a default value in the source function)."""
        return self._optional

    @property
    def required_fields(self) -> frozenset[str]:
        """Field names that must be present in an incoming packet."""
        return frozenset(self._data.keys()) - self._optional

    def is_required(self, field: str) -> bool:
        """Return True if *field* must be present (has no default)."""
        return field not in self._optional

    # ==================== Schema operations ====================

    def merge(self, other: Mapping[str, type]) -> Schema:
        """Return a new Schema that is the union of ``self`` and ``other``.

        Args:
            other: A mapping of field names to types to merge in.

        Returns:
            A new ``Schema`` containing all fields from both schemas.

        Raises:
            ValueError: If any shared field has a different type in ``other``.
        """
        conflicts = {k for k in other if k in self._data and self._data[k] != other[k]}
        if conflicts:
            raise ValueError(f"Schema merge conflict on fields: {conflicts}")
        other_optional = other._optional if isinstance(other, Schema) else frozenset()
        return Schema(
            {**self._data, **other}, optional_fields=self._optional | other_optional
        )

    def with_values(self, other: dict[str, type] | None, **kwargs: type) -> Schema:
        """Return a new Schema with the specified fields added or overridden.

        Unlike ``merge``, this method silently overrides existing fields when
        a key already exists.

        Args:
            other: An optional mapping of field names to types.
            **kwargs: Additional field name / type pairs.

        Returns:
            A new ``Schema`` with the updated fields.
        """
        if other is None:
            other = {}
        return Schema({**self._data, **other, **kwargs})

    def select(self, *fields: str) -> Schema:
        """Return a new Schema containing only the specified fields.

        Args:
            *fields: Names of the fields to keep.

        Returns:
            A new ``Schema`` with only the requested fields.

        Raises:
            KeyError: If any of the requested fields are not present.
        """
        missing = set(fields) - self._data.keys()
        if missing:
            raise KeyError(f"Fields not in schema: {missing}")
        kept = frozenset(fields)
        return Schema(
            {k: self._data[k] for k in fields}, optional_fields=self._optional & kept
        )

    def drop(self, *fields: str) -> Schema:
        """Return a new Schema with the specified fields removed.

        Args:
            *fields: Names of the fields to drop. Fields not present in the
                schema are silently ignored.

        Returns:
            A new ``Schema`` without the dropped fields.
        """
        dropped = frozenset(fields)
        return Schema(
            {k: v for k, v in self._data.items() if k not in fields},
            optional_fields=self._optional - dropped,
        )

    def is_compatible_with(self, other: Schema) -> bool:
        """Check whether ``other`` is a superset of this schema.

        Args:
            other: The schema to compare against.

        Returns:
            ``True`` if ``other`` contains every field in ``self`` with a
            matching type.
        """
        return all(other.get(k) == v for k, v in self._data.items())

    def as_required(self) -> Schema:
        """Return a copy of this schema with all fields marked as required.

        Strips optional-field metadata so the result reflects the structural
        (Arrow-level) schema, where every field is unconditionally present.

        Returns:
            A new ``Schema`` containing the same fields and types but with
            ``optional_fields`` set to the empty frozenset.
        """
        return Schema(self._data)

    # ==================== Convenience constructors ====================

    @classmethod
    def empty(cls) -> Schema:
        """Create an empty schema with no fields.

        Returns:
            A new ``Schema`` containing zero fields.
        """
        return cls({})


class ExecutorType(Enum):
    """Pipeline execution strategy.

    Attributes:
        SYNCHRONOUS: Current behavior -- ``static_process`` chain with
            pull-based materialization.
        ASYNC_CHANNELS: Push-based async channel execution via
            ``async_execute``.
    """

    SYNCHRONOUS = "synchronous"
    ASYNC_CHANNELS = "async_channels"


@dataclass(frozen=True, slots=True)
class PipelineConfig:
    """Pipeline-level execution configuration.

    Attributes:
        executor: Which execution strategy to use.
        channel_buffer_size: Max items buffered per channel edge.
        default_max_concurrency: Pipeline-wide default for per-node
            concurrency.  ``None`` means unlimited.
        execution_engine: Optional packet-function executor applied to all
            function nodes (e.g. ``RayExecutor``).  ``None`` means in-process
            execution.
        execution_engine_opts: Resource/options dict forwarded to the engine
            via ``with_options()`` (e.g. ``{"num_cpus": 4}``).
    """

    executor: ExecutorType = ExecutorType.SYNCHRONOUS
    channel_buffer_size: int = 64
    default_max_concurrency: int | None = None
    execution_engine: PacketFunctionExecutorProtocol | None = None
    execution_engine_opts: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Per-node execution configuration.

    Attributes:
        max_concurrency: Override for this node's concurrency limit.
            ``None`` inherits from ``PipelineConfig.default_max_concurrency``.
            ``1`` means sequential (rate-limited APIs, preserves ordering).
    """

    max_concurrency: int | None = None


def resolve_concurrency(
    node_config: NodeConfig, pipeline_config: PipelineConfig
) -> int | None:
    """Resolve effective concurrency from node and pipeline configs.

    Returns:
        The concurrency limit to use, or ``None`` for unlimited.

    Raises:
        ValueError: If the resolved value is ``<= 0``.
    """
    if node_config.max_concurrency is not None:
        result = node_config.max_concurrency
    else:
        result = pipeline_config.default_max_concurrency
    if result is not None and result <= 0:
        raise ValueError(f"max_concurrency must be >= 1, got {result}")
    return result


class CacheMode(Enum):
    """Controls operator pod caching behaviour.

    Attributes:
        OFF: No cache writes, always compute.  Default for operator pods.
        LOG: Cache writes and computation.  The operator always recomputes;
            the cache serves as an append-only historical record.
        REPLAY: Skip computation and flow cached results downstream.  Only
            appropriate when the user explicitly wants to use the historical
            record (e.g. auditing or run-over-run comparison).
    """

    OFF = "off"
    LOG = "log"
    REPLAY = "replay"


@dataclass(frozen=True, slots=True)
class ColumnConfig:
    """
    Configuration for column inclusion in DatagramProtocol/PacketProtocol/TagProtocol operations.

    Controls which column types to include when converting to tables, dicts,
    or querying keys/types.

    Attributes:
        meta: Include meta columns (with '__' prefix).
              - False: exclude all meta columns (default)
              - True: include all meta columns
              - Collection[str]: include specific meta columns by name
                (prefix '__' is added automatically if not present)
        context: Include context column
        source: Include source info columns (PacketProtocol only, ignored for others)
        system_tags: Include system tag columns (TagProtocol only, ignored for others)
        all_info: Include all available columns (overrides other settings)

    Examples:
        >>> # Data columns only (default)
        >>> ColumnConfig()

        >>> # Everything
        >>> ColumnConfig(all_info=True)
        >>> # Or use convenience method:
        >>> ColumnConfig.all()

        >>> # Specific combinations
        >>> ColumnConfig(meta=True, context=True)
        >>> ColumnConfig(meta=["pipeline", "processed"], source=True)

        >>> # As dict (alternative syntax)
        >>> {"meta": True, "source": True}
    """

    meta: bool | Collection[str] = False
    context: bool = False
    source: bool = False  # Only relevant for PacketProtocol
    system_tags: bool = False  # Only relevant for TagProtocol
    content_hash: bool | str = False  # Only relevant for PacketProtocol
    sort_by_tags: bool = False  # Only relevant for TagProtocol
    all_info: bool = False

    @classmethod
    def all(cls) -> Self:
        """Convenience: include all available columns"""
        return cls(
            meta=True,
            context=True,
            source=True,
            system_tags=True,
            content_hash=True,
            sort_by_tags=True,
            all_info=True,
        )

    @classmethod
    def data_only(cls) -> Self:
        """Convenience: include only data columns (default)"""
        return cls()

    # TODO: consider renaming this to something more intuitive
    @classmethod
    def handle_config(
        cls, config: Self | dict[str, Any] | None, all_info: bool = False
    ) -> Self:
        """
        Normalize column configuration input.

        Args:
            config: ColumnConfig instance or dict to normalize.
            all_info: If True, override config to include all columns.

        Returns:
            Normalized ColumnConfig instance.
        """
        if all_info:
            return cls.all()
        # TODO: properly handle non-boolean values when using all_info

        if config is None:
            column_config = cls()
        elif isinstance(config, dict):
            column_config = cls(**config)
        elif isinstance(config, cls):
            column_config = config
        else:
            raise TypeError(
                f"Invalid column config type: {type(config)}. "
                "Expected ColumnConfig instance or dict."
            )

        return column_config


@dataclass(frozen=True)
class ColumnInfo:
    """Metadata for a single relational database column with its Arrow-mapped type.

    ``ColumnInfo`` is produced by ``DBConnectorProtocol.get_column_info()`` and
    consumed by ``ConnectorArrowDatabase`` and ``DBTableSource``.  Type mapping
    (DB-native → Arrow) is always the connector's responsibility; callers always
    receive Arrow types.

    Args:
        name: Column name.
        arrow_type: Arrow data type (already mapped from the DB-native type).
        nullable: Whether the column accepts NULL values.  Defaults to ``True``.
    """

    name: str
    arrow_type: pa.DataType
    nullable: bool = True


@dataclass(frozen=True, slots=True)
class ContentHash:
    """Content-addressable hash pairing a hashing method with a raw digest.

    ``ContentHash`` is the standard way to represent hashes throughout OrcaPod.
    It is immutable (frozen dataclass) and provides convenience methods to
    convert the digest into hex strings, integers, UUIDs, base64, and
    human-friendly display names.

    Attributes:
        method: Identifier for the hashing algorithm / strategy used
            (e.g. ``"arrow_v2.1"``).
        digest: The raw hash bytes.
    """

    method: str
    digest: bytes

    # TODO: make the default char count configurable
    def to_hex(self, char_count: int | None = None) -> str:
        """Convert the digest to a hexadecimal string.

        Args:
            char_count: If given, truncate the hex string to this many
                characters.

        Returns:
            The full (or truncated) hex representation of the digest.
        """
        hex_str = self.digest.hex()
        return hex_str[:char_count] if char_count else hex_str

    def to_int(self, hexdigits: int | None = None) -> int:
        """Convert the digest to an integer.

        Args:
            hexdigits: Number of hex digits to use. If provided, the hex
                string is truncated before conversion.

        Returns:
            Integer representation of the (optionally truncated) digest.
        """
        return int(self.to_hex(hexdigits), 16)

    def to_uuid(self, namespace: uuid.UUID = uuid.NAMESPACE_OID) -> uuid.UUID:
        """Derive a deterministic UUID from the digest.

        Uses ``uuid5`` with the full hex string to ensure deterministic output.

        Args:
            namespace: UUID namespace for ``uuid5`` generation. Defaults to
                ``uuid.NAMESPACE_OID``.

        Returns:
            A UUID derived from this hash.
        """
        # Using uuid5 with the hex string ensures deterministic UUIDs
        return uuid.uuid5(namespace, self.to_hex())

    def to_base64(self) -> str:
        """Convert the digest to a base64-encoded ASCII string.

        Returns:
            Base64 representation of the digest.
        """
        import base64

        return base64.b64encode(self.digest).decode("ascii")

    def to_string(
        self, prefix_method: bool = True, hexdigits: int | None = None
    ) -> str:
        """Convert the digest to a human-readable string.

        Args:
            prefix_method: If ``True`` (the default), prepend the method name
                followed by a colon (e.g. ``"sha256:abcd1234"``).
            hexdigits: Optional number of hex digits to include.

        Returns:
            String representation of the hash.
        """
        if prefix_method:
            return f"{self.method}:{self.to_hex(hexdigits)}"
        return self.to_hex(hexdigits)

    def __str__(self) -> str:
        return self.to_string()

    @classmethod
    def from_string(cls, hash_string: str) -> "ContentHash":
        """Parse a ``"method:hex_digest"`` string into a ``ContentHash``.

        Args:
            hash_string: A string in the format ``"method:hex_digest"``.

        Returns:
            A new ``ContentHash`` instance.
        """
        method, hex_digest = hash_string.split(":", 1)
        return cls(method, bytes.fromhex(hex_digest))

    def display_name(self, length: int = 8) -> str:
        """Return a short, human-friendly label for this hash.

        Args:
            length: Number of hex characters to include after the method
                prefix. Defaults to 8.

        Returns:
            A string like ``"arrow_v2.1:1a2b3c4d"``.
        """
        return f"{self.method}:{self.to_hex(length)}"
