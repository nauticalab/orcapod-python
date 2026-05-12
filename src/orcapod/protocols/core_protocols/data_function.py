from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.datagrams import DataProtocol
from orcapod.protocols.core_protocols.executor import DataFunctionExecutorProtocol
from orcapod.protocols.core_protocols.labelable import LabelableProtocol
from orcapod.protocols.hashing_protocols import (
    ContentIdentifiableProtocol,
    PipelineElementProtocol,
)
from orcapod.types import Schema

if TYPE_CHECKING:
    from orcapod.protocols.observability_protocols import DataExecutionLoggerProtocol


@runtime_checkable
class DataFunctionProtocol(
    ContentIdentifiableProtocol, PipelineElementProtocol, LabelableProtocol, Protocol
):
    """Protocol for a data-processing function.

    Processes individual data with declared input/output schemas.
    """

    # ==================== Identity & Metadata ====================
    @property
    def data_function_type_id(self) -> str:
        """How functions are defined and executed (e.g., python.function.v2)"""
        ...

    @property
    def canonical_function_name(self) -> str:
        """Human-readable function identifier"""
        ...

    @property
    def major_version(self) -> int:
        """Breaking changes increment this"""
        ...

    @property
    def minor_version_string(self) -> str:
        """Flexible minor version (e.g., "1", "4.3rc", "apple")"""
        ...

    @property
    def input_data_schema(self) -> Schema:
        """Schema describing the input data this function accepts."""
        ...

    @property
    def output_data_schema(self) -> Schema:
        """Schema describing the output data this function produces."""
        ...

    # ==================== Content-Addressable Identity ====================
    def get_function_variation_data(self) -> dict[str, Any]:
        """Raw data defining function variation - system computes hash"""
        ...

    def get_function_variation_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_function_variation_data``."""
        ...

    def get_execution_data(self) -> dict[str, Any]:
        """Raw data defining execution context - system computes hash"""
        ...

    def get_execution_data_schema(self) -> Schema:
        """Schema for the data returned by ``get_execution_data``."""
        ...

    # ==================== Executor ====================

    @property
    def executor(self) -> DataFunctionExecutorProtocol | None:
        """The executor used to run this function, or ``None`` for direct execution."""
        ...

    @executor.setter
    def executor(self, executor: DataFunctionExecutorProtocol | None) -> None:
        """Set or clear the executor."""
        ...

    # ==================== Execution ====================

    def call(
        self,
        data: DataProtocol,
        *,
        logger: "DataExecutionLoggerProtocol | None" = None,
    ) -> DataProtocol | None:
        """Process a single data, routing through the executor if one is set.

        Args:
            data: The data payload to process.
            logger: Optional logger for recording captured I/O.

        Returns:
            The output data, or ``None`` when the function filters the
            data out or when execution failed (exception is re-raised).
        """
        ...

    async def async_call(
        self,
        data: DataProtocol,
        *,
        logger: "DataExecutionLoggerProtocol | None" = None,
    ) -> DataProtocol | None:
        """Asynchronously process a single data, routing through the executor if set.

        Args:
            data: The data payload to process.
            logger: Optional logger for recording captured I/O.

        Returns:
            The output data, or ``None``.
        """
        ...

    def direct_call(
        self,
        data: DataProtocol,
    ) -> DataProtocol | None:
        """Execute the function's native computation on *data*.

        This is the method executors invoke, bypassing executor routing.
        On user-function failure the exception is re-raised.

        Args:
            data: The data payload to process.

        Returns:
            The output data, or ``None`` if filtered.
        """
        ...

    async def direct_async_call(
        self,
        data: DataProtocol,
    ) -> DataProtocol | None:
        """Asynchronous counterpart of ``direct_call``."""
        ...

    # ==================== Serialization ====================

    def to_config(self) -> dict[str, Any]:
        """Serialize this data function to a JSON-compatible config dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DataFunctionProtocol":
        """Reconstruct a data function from a config dict."""
        ...
