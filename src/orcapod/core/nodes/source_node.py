"""SourceNode — wraps a root source stream in the computation graph."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.streams.base import StreamBase
from orcapod.protocols import core_protocols as cp
from orcapod.types import ColumnConfig, Schema

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

    @property
    def data_context(self) -> contexts.DataContext:
        return contexts.resolve_context(self.stream.data_context_key)

    @property
    def data_context_key(self) -> str:
        return self.stream.data_context_key

    def computed_label(self) -> str | None:
        return self.stream.label

    def identity_structure(self) -> Any:
        # TODO: revisit this logic for case where stream is not a root source
        return self.stream.identity_structure()

    def pipeline_identity_structure(self) -> Any:
        return self.stream.pipeline_identity_structure()

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self.stream.keys(columns=columns, all_info=all_info)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
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
        return self.stream.as_table(columns=columns, all_info=all_info)

    def iter_packets(self) -> Iterator[tuple[cp.TagProtocol, cp.PacketProtocol]]:
        return self.stream.iter_packets()

    def run(self) -> None:
        """No-op for source nodes — data is already available."""

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[cp.TagProtocol, cp.PacketProtocol]]],
        output: WritableChannel[tuple[cp.TagProtocol, cp.PacketProtocol]],
    ) -> None:
        """Push all (tag, packet) pairs from the wrapped stream to the output channel."""
        try:
            for tag, packet in self.stream.iter_packets():
                await output.send((tag, packet))
        finally:
            await output.close()
