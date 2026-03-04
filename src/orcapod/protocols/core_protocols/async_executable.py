"""Protocol for async channel-based pipeline execution."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.protocols.core_protocols.datagrams import PacketProtocol, TagProtocol


@runtime_checkable
class AsyncExecutableProtocol(Protocol):
    """Unified interface for all pipeline nodes in async channel mode.

    Every pipeline node — source, operator, or function pod — implements
    this single method.  The orchestrator wires up channels and launches
    tasks without needing to know what kind of node it is.
    """

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Consume (tag, packet) pairs from input channels, produce to output channel.

        Implementations MUST call ``await output.close()`` when done to signal
        completion to downstream consumers.
        """
        ...
