from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import DataProtocol, TagProtocol
from orcapod.protocols.core_protocols.data_function import DataFunctionProtocol
from orcapod.protocols.core_protocols.pod import PodProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import PodConfig


@runtime_checkable
class FunctionPodProtocol(PodProtocol, PipelineElementProtocol, Protocol):
    """
    PodProtocol based on DataFunctionProtocol.
    """

    @property
    def data_function(self) -> DataFunctionProtocol:
        """The ``DataFunctionProtocol`` that defines the computation for this pod."""
        ...

    @property
    def pod_config(self) -> PodConfig:
        """Per-pod executor configuration."""
        ...

    @property
    def ctx_arg_name(self) -> str | None:
        """Parameter name auto-injected with ``InvocationContext``, or ``None``."""
        ...

    def process_data(
        self, tag: TagProtocol, data: DataProtocol
    ) -> tuple[TagProtocol, DataProtocol | None]: ...

    async def async_process_data(
        self, tag: TagProtocol, data: DataProtocol
    ) -> tuple[TagProtocol, DataProtocol | None]: ...

    def to_config(self) -> dict[str, Any]:
        """Serialize this function pod to a JSON-compatible config dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "FunctionPodProtocol":
        """Reconstruct a function pod from a config dict."""
        ...
