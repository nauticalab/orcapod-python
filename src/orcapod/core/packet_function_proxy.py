"""Proxy for unavailable packet functions in deserialized pipelines.

When a pipeline is loaded in an environment where the original packet
function cannot be imported, ``PacketFunctionProxy`` stands in so that
``FunctionPod``, ``FunctionNode``, and ``CachedFunctionPod`` can still be
constructed and cached data can be accessed.  Invoking the proxy raises
``PacketFunctionUnavailableError`` unless a real function has been bound
via `bind`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.core.packet_function import PacketFunctionBase
from orcapod.errors import PacketFunctionUnavailableError
from orcapod.protocols.core_protocols import PacketFunctionProtocol
from orcapod.types import ContentHash, Schema

if TYPE_CHECKING:
    from orcapod.protocols.core_protocols import (
        PacketFunctionExecutorProtocol,
        PacketProtocol,
    )
    from orcapod.protocols.observability_protocols import PacketExecutionLoggerProtocol


class PacketFunctionProxy(PacketFunctionBase):
    """Stand-in for an unavailable packet function.

    Satisfies ``PacketFunctionProtocol`` so pipeline construction succeeds.
    All execution methods raise ``PacketFunctionUnavailableError`` until a
    real function is attached via `bind`.

    Args:
        config: Serialized packet function config dict (as produced by
            ``PythonPacketFunction.to_config()``).
        content_hash_str: Optional stored content hash string.
        pipeline_hash_str: Optional stored pipeline hash string.
    """

    def __init__(
        self,
        config: dict[str, Any],
        content_hash_str: str | None = None,
        pipeline_hash_str: str | None = None,
    ) -> None:
        inner = config["config"]
        self._original_config = config
        self._packet_function_type_id = config["packet_function_type_id"]
        self._canonical_function_name = inner["callable_name"]

        # Eagerly deserialize schemas.
        self._raw_input_schema_dict = inner["input_packet_schema"]
        self._raw_output_schema_dict = inner["output_packet_schema"]
        self._input_packet_schema = _deserialize_schema_from_config(
            self._raw_input_schema_dict
        )
        self._output_packet_schema = _deserialize_schema_from_config(
            self._raw_output_schema_dict
        )

        # Call super().__init__ so that major_version and
        # output_packet_schema_hash are available for URI fallback.
        version = inner["version"]
        super().__init__(version=version)

        # URI: read from config if present, otherwise compute from metadata.
        uri_list = config.get("uri")
        if uri_list is not None:
            self._stored_uri = tuple(uri_list)
        else:
            # Fallback: compute from available metadata
            # (same structure as PacketFunctionBase.uri).
            self._stored_uri = (
                self._canonical_function_name,
                self.output_packet_schema_hash,
                f"v{self.major_version}",
                self._packet_function_type_id,
            )

        # Stored identity hashes
        self._stored_content_hash = content_hash_str
        self._stored_pipeline_hash = pipeline_hash_str

        # Late-binding slot
        self._bound_function: PacketFunctionProtocol | None = None

    # ==================== Identity properties ====================

    @property
    def packet_function_type_id(self) -> str:
        """Unique function type identifier."""
        return self._packet_function_type_id

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier."""
        return self._canonical_function_name

    @property
    def input_packet_schema(self) -> Schema:
        """Schema describing the input packets this function accepts."""
        return self._input_packet_schema

    @property
    def output_packet_schema(self) -> Schema:
        """Schema describing the output packets this function produces."""
        return self._output_packet_schema

    @property
    def uri(self) -> tuple[str, ...]:
        """Return the stored URI, avoiding recomputation via semantic_hasher."""
        return self._stored_uri

    # ==================== Hash overrides ====================

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the stored content hash."""
        if self._stored_content_hash is not None:
            return ContentHash.from_string(self._stored_content_hash)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the stored pipeline hash."""
        if self._stored_pipeline_hash is not None:
            return ContentHash.from_string(self._stored_pipeline_hash)
        return super().pipeline_hash(hasher)

    # ==================== Execution ====================

    def _raise_unavailable(self) -> None:
        """Raise ``PacketFunctionUnavailableError`` with context."""
        raise PacketFunctionUnavailableError(
            f"Packet function '{self._canonical_function_name}' is not available. "
            f"Use bind() to attach a real function, or access cached results only."
        )

    def call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        """Process a single packet; delegates to bound function or raises."""
        if self._bound_function is not None:
            return self._bound_function.call(packet, logger=logger)
        self._raise_unavailable()

    async def async_call(
        self,
        packet: PacketProtocol,
        *,
        logger: PacketExecutionLoggerProtocol | None = None,
    ) -> PacketProtocol | None:
        """Async counterpart of ``call``."""
        if self._bound_function is not None:
            return await self._bound_function.async_call(packet, logger=logger)
        self._raise_unavailable()

    def direct_call(self, packet: PacketProtocol) -> PacketProtocol | None:
        """Direct execution; delegates to bound function or raises."""
        if self._bound_function is not None:
            return self._bound_function.direct_call(packet)
        self._raise_unavailable()

    async def direct_async_call(self, packet: PacketProtocol) -> PacketProtocol | None:
        """Async direct execution; delegates to bound function or raises."""
        if self._bound_function is not None:
            return await self._bound_function.direct_async_call(packet)
        self._raise_unavailable()

    # ==================== Variation / execution data ====================

    def get_function_variation_data(self) -> dict[str, Any]:
        """Return function variation data, or empty dict when unbound."""
        if self._bound_function is not None:
            return self._bound_function.get_function_variation_data()
        return {}

    def get_function_variation_data_schema(self) -> Schema:
        """Return function variation data schema from bound function, or empty schema."""
        if self._bound_function is not None:
            return self._bound_function.get_function_variation_data_schema()
        return Schema({})

    def get_execution_data(self) -> dict[str, Any]:
        """Return execution data, or empty dict when unbound."""
        if self._bound_function is not None:
            return self._bound_function.get_execution_data()
        return {}

    def get_execution_data_schema(self) -> Schema:
        """Return execution data schema from bound function, or empty schema."""
        if self._bound_function is not None:
            return self._bound_function.get_execution_data_schema()
        return Schema({})

    # ==================== Executor ====================

    @property
    def executor(self) -> PacketFunctionExecutorProtocol | None:
        """Return executor from bound function, or None."""
        if self._bound_function is not None:
            return self._bound_function.executor
        return None

    @executor.setter
    def executor(self, executor: PacketFunctionExecutorProtocol | None) -> None:
        """Set executor on bound function; no-op when unbound."""
        if self._bound_function is not None:
            self._bound_function.executor = executor

    # ==================== Serialization ====================

    def to_config(self) -> dict[str, Any]:
        """Return the original config dict."""
        return self._original_config

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PacketFunctionProxy:
        """Construct a proxy from a serialized config dict.

        The URI is read from the config's ``"uri"`` field if present;
        otherwise a fallback is computed from config metadata.

        Args:
            config: A dict as produced by ``PythonPacketFunction.to_config()``.

        Returns:
            A new ``PacketFunctionProxy`` instance.
        """
        return cls(config=config)

    # ==================== Bind / unbind ====================

    @property
    def is_bound(self) -> bool:
        """Return whether a real function is currently bound."""
        return self._bound_function is not None

    def bind(self, packet_function: PacketFunctionProtocol) -> None:
        """Attach a real packet function, validating compatibility.

        Checks that the bound function matches this proxy on:
        ``canonical_function_name``, ``major_version``,
        ``packet_function_type_id``, ``input_packet_schema``,
        ``output_packet_schema``, ``uri``, and ``content_hash()``
        (if a stored hash exists).

        Args:
            packet_function: The real function to bind.

        Raises:
            ValueError: If any compatibility check fails, listing all
                mismatches.
        """
        mismatches: list[str] = []

        if packet_function.canonical_function_name != self._canonical_function_name:
            mismatches.append(
                f"canonical_function_name: expected {self._canonical_function_name!r}, "
                f"got {packet_function.canonical_function_name!r}"
            )

        if packet_function.major_version != self.major_version:
            mismatches.append(
                f"major_version: expected {self.major_version!r}, "
                f"got {packet_function.major_version!r}"
            )

        if packet_function.packet_function_type_id != self._packet_function_type_id:
            mismatches.append(
                f"packet_function_type_id: expected {self._packet_function_type_id!r}, "
                f"got {packet_function.packet_function_type_id!r}"
            )

        if packet_function.input_packet_schema != self._input_packet_schema:
            # Also compare via serialized form in case type repr differs
            bound_input = {
                k: str(v) for k, v in packet_function.input_packet_schema.items()
            }
            if bound_input != self._raw_input_schema_dict:
                mismatches.append(
                    f"input_packet_schema: expected {self._input_packet_schema!r}, "
                    f"got {packet_function.input_packet_schema!r}"
                )

        if packet_function.output_packet_schema != self._output_packet_schema:
            bound_output = {
                k: str(v) for k, v in packet_function.output_packet_schema.items()
            }
            if bound_output != self._raw_output_schema_dict:
                mismatches.append(
                    f"output_packet_schema: expected {self._output_packet_schema!r}, "
                    f"got {packet_function.output_packet_schema!r}"
                )

        if tuple(packet_function.uri) != tuple(self._stored_uri):
            mismatches.append(
                f"uri: expected {self._stored_uri!r}, "
                f"got {tuple(packet_function.uri)!r}"
            )

        if self._stored_content_hash is not None:
            bound_hash = packet_function.content_hash().to_string()
            if bound_hash != self._stored_content_hash:
                mismatches.append(
                    f"content_hash: expected {self._stored_content_hash!r}, "
                    f"got {bound_hash!r}"
                )

        if mismatches:
            raise ValueError(
                f"Cannot bind packet function: {', '.join(mismatches)}"
            )

        self._bound_function = packet_function

    def unbind(self) -> None:
        """Detach the bound function, reverting to proxy mode."""
        self._bound_function = None


def _deserialize_schema_from_config(schema_dict: dict[str, str]) -> Schema:
    """Reconstruct a Schema from a to_config() schema dict.

    Handles both ``str(python_type)`` format (e.g. ``"<class 'int'>"``),
    used by ``PythonPacketFunction.to_config()``, and Arrow type string
    format (e.g. ``"int64"``), used by the serialization module.

    Unrecognized type strings are coerced to ``object``.

    Args:
        schema_dict: Dict mapping field names to type strings.

    Returns:
        A Schema with Python types reconstructed from the strings.
    """
    from orcapod.pipeline.serialization import (
        _BUILTIN_TYPE_MAP,
        deserialize_schema,
    )

    result: dict[str, Any] = {}
    needs_arrow_fallback: list[str] = []

    for name, type_str in schema_dict.items():
        if type_str in _BUILTIN_TYPE_MAP:
            result[name] = _BUILTIN_TYPE_MAP[type_str]
        else:
            needs_arrow_fallback.append(name)

    if needs_arrow_fallback:
        arrow_result = deserialize_schema(
            {k: schema_dict[k] for k in needs_arrow_fallback}
        )
        for k, v in arrow_result.items():
            # If deserialize_schema fell back to a raw string, coerce to object
            if isinstance(v, str):
                result[k] = object
            else:
                result[k] = v

    return Schema(result)
