"""SourceNode — wraps a root source stream in the computation graph."""

from __future__ import annotations

from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.channels import WritableChannel
from orcapod.config import Config, DEFAULT_CONFIG
from orcapod.core.streams.base import StreamBase
from orcapod.protocols import core_protocols as cp
from orcapod.types import ColumnConfig, ContentHash, Schema

if TYPE_CHECKING:
    import pyarrow as pa


class SourceNode(StreamBase):
    """Represents a root source stream in the computation graph."""

    node_type = "source"

    def __init__(
        self,
        stream: cp.StreamProtocol,
        label: str | None = None,
        config: Config | None = None,
    ):
        super().__init__(label=label, config=config)
        self.stream = stream
        self._cached_results: list[tuple[cp.TagProtocol, cp.PacketProtocol]] | None = (
            None
        )

    # ------------------------------------------------------------------
    # from_descriptor — reconstruct from a serialized pipeline descriptor
    # ------------------------------------------------------------------

    @classmethod
    def from_descriptor(
        cls,
        descriptor: dict[str, Any],
        stream: cp.StreamProtocol | None,
        databases: dict[str, Any],
    ) -> SourceNode:
        """Construct a SourceNode from a serialized descriptor.

        When *stream* is provided the node operates in full mode — all
        delegation goes through the live stream.  When *stream* is ``None``
        the node is created in read-only mode with metadata from the
        descriptor; data-access methods (``iter_packets``, ``as_table``)
        will raise ``RuntimeError``.

        Args:
            descriptor: The serialized node descriptor dict.
            stream: An optional live stream to wrap.  ``None`` for
                read-only mode.
            databases: Mapping of database role names to database
                instances (currently unused for source nodes but kept
                for interface consistency with other node types).

        Returns:
            A new ``SourceNode`` instance.
        """
        from orcapod.pipeline.serialization import LoadStatus

        if stream is not None:
            node = cls(stream=stream, label=descriptor.get("label"))
            node._descriptor = descriptor
            node._load_status = LoadStatus.FULL
            return node

        # Read-only mode: bypass __init__, set minimum required state
        node = cls.__new__(cls)

        # From LabelableMixin
        node._label = descriptor.get("label")

        # From DataContextMixin
        node._data_context = contexts.resolve_context(
            descriptor.get("data_context_key")
        )
        node._orcapod_config = DEFAULT_CONFIG

        # From ContentIdentifiableBase
        node._content_hash_cache = {}
        node._cached_int_hash = None

        # From PipelineElementBase
        node._pipeline_hash_cache = {}

        # From TemporalMixin
        node._modified_time = None

        # SourceNode's own state
        node.stream = None
        node._cached_results = None
        node._descriptor = descriptor
        node._load_status = LoadStatus.UNAVAILABLE
        node._stored_schema = descriptor.get("output_schema", {})
        node._stored_content_hash = descriptor.get("content_hash")
        node._stored_pipeline_hash = descriptor.get("pipeline_hash")

        return node

    # ------------------------------------------------------------------
    # load_status
    # ------------------------------------------------------------------

    @property
    def load_status(self) -> Any:
        """Return the load status of this node.

        Returns:
            The ``LoadStatus`` enum value indicating how this node was
            loaded.  Defaults to ``FULL`` for nodes created via
            ``__init__``.
        """
        from orcapod.pipeline.serialization import LoadStatus

        return getattr(self, "_load_status", LoadStatus.FULL)

    # ------------------------------------------------------------------
    # Delegation — with read-only guards
    # ------------------------------------------------------------------

    @property
    def data_context(self) -> contexts.DataContext:
        if self.stream is None:
            return self._data_context
        return contexts.resolve_context(self.stream.data_context_key)

    @property
    def data_context_key(self) -> str:
        if self.stream is None:
            return self._data_context.context_key
        return self.stream.data_context_key

    def computed_label(self) -> str | None:
        if self.stream is None:
            return None
        return self.stream.label

    def identity_structure(self) -> Any:
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        # TODO: revisit this logic for case where stream is not a root source
        return self.stream.identity_structure()

    def pipeline_identity_structure(self) -> Any:
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        return self.stream.pipeline_identity_structure()

    def content_hash(self, hasher=None) -> ContentHash:
        """Return the content hash, using stored value in read-only mode."""
        stored = getattr(self, "_stored_content_hash", None)
        if self.stream is None and stored is not None:
            return ContentHash.from_string(stored)
        return super().content_hash(hasher)

    def pipeline_hash(self, hasher=None) -> ContentHash:
        """Return the pipeline hash, using stored value in read-only mode."""
        stored = getattr(self, "_stored_pipeline_hash", None)
        if self.stream is None and stored is not None:
            return ContentHash.from_string(stored)
        return super().pipeline_hash(hasher)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self.stream is None:
            stored = getattr(self, "_stored_schema", {})
            tag_keys = tuple(stored.get("tag", {}).keys())
            packet_keys = tuple(stored.get("packet", {}).keys())
            return tag_keys, packet_keys
        return self.stream.keys(columns=columns, all_info=all_info)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        if self.stream is None:
            stored = getattr(self, "_stored_schema", {})
            tag = Schema(stored.get("tag", {}))
            packet = Schema(stored.get("packet", {}))
            return tag, packet
        return self.stream.output_schema(columns=columns, all_info=all_info)

    @property
    def producer(self) -> None:
        return None

    @property
    def upstreams(self) -> tuple[cp.StreamProtocol, ...]:
        return ()

    @upstreams.setter
    def upstreams(self, value: tuple[cp.StreamProtocol, ...]) -> None:
        if len(value) != 0:
            raise ValueError("SourceNode upstreams must be empty")

    def __repr__(self) -> str:
        return f"SourceNode(stream={self.stream!r}, label={self.label!r})"

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        return self.stream.as_table(columns=columns, all_info=all_info)

    def iter_packets(self) -> Iterator[tuple[cp.TagProtocol, cp.PacketProtocol]]:
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        if self._cached_results is not None:
            return iter(self._cached_results)
        return self.stream.iter_packets()

    def execute(
        self,
        *,
        observer: Any = None,
    ) -> list[tuple[cp.TagProtocol, cp.PacketProtocol]]:
        """Execute this source: materialize packets and return.

        Args:
            observer: Optional execution observer for hooks.

        Returns:
            List of (tag, packet) tuples.
        """
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        if observer is not None:
            observer.on_node_start(self)
        result = list(self.stream.iter_packets())
        self._cached_results = result
        if observer is not None:
            observer.on_node_end(self)
        return result

    def run(self) -> None:
        """No-op for source nodes — data is already available."""

    async def async_execute(
        self,
        output: WritableChannel[tuple[cp.TagProtocol, cp.PacketProtocol]],
        *,
        observer: Any = None,
    ) -> None:
        """Push all (tag, packet) pairs from the wrapped stream to the output channel.

        Args:
            output: Channel to write results to.
            observer: Optional execution observer for hooks.
        """
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        try:
            if observer is not None:
                observer.on_node_start(self)
            for tag, packet in self.stream.iter_packets():
                await output.send((tag, packet))
            if observer is not None:
                observer.on_node_end(self)
        finally:
            await output.close()
