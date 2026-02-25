from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Collection, Iterator, Mapping
from dataclasses import dataclass
from types import UnionType
from typing import TypeAlias, Union

import pyarrow as pa

logger = logging.getLogger(__name__)

# Mapping from Python types to Arrow types
_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    int: pa.int64(),
    float: pa.float64(),
    str: pa.string(),
    bool: pa.bool_(),
    bytes: pa.binary(),
}

# Reverse mapping
_ARROW_TO_PYTHON: dict[pa.DataType, type] = {v: k for k, v in _PYTHON_TO_ARROW.items()}

# TODO: revisit and consider a way to incorporate older Union type
DataType: TypeAlias = type | UnionType  # | type[Union]


# Convenience alias for anything pathlike
PathLike = str | os.PathLike

# an (optional) string or a collection of (optional) string values
# Note that TagValue can be nested, allowing for an arbitrary depth of nested lists
TagValue: TypeAlias = int | str | None | Collection["TagValue"]

# a pathset is a path or an arbitrary depth of nested list of paths
PathSet: TypeAlias = PathLike | Collection[PathLike | None]

# Simple data types that we support (with clear Polars correspondence)
SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes

ExtendedSupportedPythonData: TypeAlias = SupportedNativePythonData | PathSet

# Extended data values that can be stored in packets
# Either the original PathSet or one of our supported simple data types
DataValue: TypeAlias = ExtendedSupportedPythonData | Collection["DataValue"] | None

PacketLike: TypeAlias = Mapping[str, DataValue]


SchemaLike: TypeAlias = Mapping[str, DataType]


class Schema(Mapping[str, DataType]):
    """
    Immutable schema representing a mapping of field names to Python types.

    Serves as the canonical internal schema representation in OrcaPod,
    with interop to/from Arrow schemas. Hashable and suitable for use
    in content-addressable contexts.
    """

    def __init__(
        self, fields: Mapping[str, DataType] | None = None, **kwargs: type
    ) -> None:
        combined = dict(fields or {})
        combined.update(kwargs)
        self._data: dict[str, DataType] = combined

    # ==================== Mapping interface ====================

    def __getitem__(self, key: str) -> DataType:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"Schema({self._data!r})"

    # ==================== Value semantics ====================

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Schema):
            return self._data == other._data
        if isinstance(other, Mapping):
            return self._data == dict(other)
        raise NotImplementedError(
            f"Equality check is not implemented for object of type {type(other)}"
        )

    def __hash__(self) -> int:
        # sort all fields based on their key entries
        # TODO: consider nested structured type
        return hash(tuple(sorted(self._data.items(), key=lambda kv: kv[0])))

    # ==================== Schema operations ====================

    def merge(self, other: Mapping[str, type]) -> Schema:
        """Return a new Schema merging self with other. Raises on conflicts."""
        conflicts = {k for k in other if k in self._data and self._data[k] != other[k]}
        if conflicts:
            raise ValueError(f"Schema merge conflict on fields: {conflicts}")
        return Schema({**self._data, **other})

    def with_values(self, other: dict[str, type] | None, **kwargs: type) -> Schema:
        """Return a new Schema, setting specified keys to the type. If the key already
        exists in the schema, the new value will override the old value."""
        if other is None:
            other = {}
        return Schema({**self._data, **other, **kwargs})

    def select(self, *fields: str) -> Schema:
        """Return a new Schema with only the specified fields."""
        missing = set(fields) - self._data.keys()
        if missing:
            raise KeyError(f"Fields not in schema: {missing}")
        return Schema({k: self._data[k] for k in fields})

    def drop(self, *fields: str) -> Schema:
        """Return a new Schema without the specified fields."""
        return Schema({k: v for k, v in self._data.items() if k not in fields})

    def is_compatible_with(self, other: Schema) -> bool:
        """True if other contains at least all fields in self with matching types."""
        return all(other.get(k) == v for k, v in self._data.items())

    # ==================== Convenience constructors ====================

    @classmethod
    def empty(cls) -> Schema:
        return cls({})


@dataclass(frozen=True, slots=True)
class ContentHash:
    method: str
    digest: bytes

    # TODO: make the default char count configurable
    def to_hex(self, char_count: int | None = None) -> str:
        """Convert digest to hex string, optionally truncated."""
        hex_str = self.digest.hex()
        return hex_str[:char_count] if char_count else hex_str

    def to_int(self, hexdigits: int | None = None) -> int:
        """
        Convert digest to integer representation.

        Args:
            hexdigits: Number of hex digits to use (truncates if needed)

        Returns:
            Integer representation of the hash
        """
        return int(self.to_hex(hexdigits), 16)

    def to_uuid(self, namespace: uuid.UUID = uuid.NAMESPACE_OID) -> uuid.UUID:
        """
        Convert digest to UUID format.

        Args:
            namespace: UUID namespace for uuid5 generation

        Returns:
            UUID derived from this hash
        """
        # Using uuid5 with the hex string ensures deterministic UUIDs
        return uuid.uuid5(namespace, self.to_hex())

    def to_base64(self) -> str:
        """Convert digest to base64 string."""
        import base64

        return base64.b64encode(self.digest).decode("ascii")

    def to_string(
        self, prefix_method: bool = True, hexdigits: int | None = None
    ) -> str:
        """Convert digest to a string representation."""
        if prefix_method:
            return f"{self.method}:{self.to_hex(hexdigits)}"
        return self.to_hex(hexdigits)

    def __str__(self) -> str:
        return self.to_string()

    @classmethod
    def from_string(cls, hash_string: str) -> "ContentHash":
        """Parse 'method:hex_digest' format."""
        method, hex_digest = hash_string.split(":", 1)
        return cls(method, bytes.fromhex(hex_digest))

    def display_name(self, length: int = 8) -> str:
        """Return human-friendly display like 'arrow_v2.1:1a2b3c4d'."""
        return f"{self.method}:{self.to_hex(length)}"
