from typing import Protocol, runtime_checkable

from orcapod.protocols.core_protocols.pod import Pod


@runtime_checkable
class OperatorPod(Pod, Protocol):
    """
    Pod that performs operations on streams.

    This is a base protocol for pods that perform operations on streams.
    TODO: add a method to map out source relationship
    """
