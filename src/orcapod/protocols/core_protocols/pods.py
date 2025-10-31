from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.base import ExecutionEngine
from orcapod.protocols.core_protocols.datagrams import Packet, Tag
from orcapod.protocols.core_protocols.kernel import Kernel
from orcapod.types import PythonSchema

if TYPE_CHECKING:
    import pyarrow as pa


@runtime_checkable
class Pod(Kernel, Protocol):
    """
    Specialized kernel for packet-level processing with advanced caching.

    Pods represent a different computational model from regular kernels:
    - Process data one packet at a time (enabling fine-grained parallelism)
    - Support just-in-time evaluation (computation deferred until needed)
    - Provide stricter type contracts (clear input/output schemas)
    - Enable advanced caching strategies (packet-level caching)

    The Pod abstraction is ideal for:
    - Expensive computations that benefit from caching
    - Operations that can be parallelized at the packet level
    - Transformations with strict type contracts
    - Processing that needs to be deferred until access time
    - Functions that operate on individual data items

    Pods use a different execution model where computation is deferred
    until results are actually needed, enabling efficient resource usage
    and fine-grained caching.
    """

    @property
    def version(self) -> str: ...

    def get_record_id(self, packet: Packet, execution_engine_hash: str) -> str: ...

    @property
    def tiered_pod_id(self) -> dict[str, str]:
        """
        Return a dictionary representation of the tiered pod's unique identifier.
        The key is supposed to be ordered from least to most specific, allowing
        for hierarchical identification of the pod.

        This is primarily used for tiered memoization/caching strategies.

        Returns:
            dict[str, str]: Dictionary representation of the pod's ID
        """
        ...

    def input_packet_types(self) -> PythonSchema:
        """
        TypeSpec for input packets that this Pod can process.

        Defines the exact schema that input packets must conform to.
        Pods are typically much stricter about input types than regular
        kernels, requiring precise type matching for their packet-level
        processing functions.

        This specification is used for:
        - Runtime type validation
        - Compile-time type checking
        - Schema inference and documentation
        - Input validation and error reporting

        Returns:
            TypeSpec: Dictionary mapping field names to required packet types
        """
        ...

    def output_packet_types(self) -> PythonSchema:
        """
        TypeSpec for output packets that this Pod produces.

        Defines the schema of packets that will be produced by this Pod.
        This is typically determined by the Pod's computational function
        and is used for:
        - Type checking downstream kernels
        - Schema inference in complex pipelines
        - Query planning and optimization
        - Documentation and developer tooling

        Returns:
            TypeSpec: Dictionary mapping field names to output packet types
        """
        ...

    async def async_call(
        self,
        tag: Tag,
        packet: Packet,
        record_id: str | None = None,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[Tag, Packet | None]: ...

    def call(
        self,
        tag: Tag,
        packet: Packet,
        record_id: str | None = None,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> tuple[Tag, Packet | None]:
        """
        Process a single packet with its associated tag.

        This is the core method that defines the Pod's computational behavior.
        It processes one (tag, packet) pair at a time, enabling:
        - Fine-grained caching at the packet level
        - Parallelization opportunities
        - Just-in-time evaluation
        - Filtering operations (by returning None)

        The method signature supports:
        - Tag transformation (modify metadata)
        - Packet transformation (modify content)
        - Filtering (return None to exclude packet)
        - Pass-through (return inputs unchanged)

        Args:
            tag: Metadata associated with the packet
            packet: The data payload to process

        Returns:
            tuple[Tag, Packet | None]:
                - Tag: Output tag (may be modified from input)
                - Packet: Processed packet, or None to filter it out

        Raises:
            TypeError: If packet doesn't match input_packet_types
            ValueError: If packet data is invalid for processing
        """
        ...


@runtime_checkable
class CachedPod(Pod, Protocol):
    async def async_call(
        self,
        tag: Tag,
        packet: Packet,
        record_id: str | None = None,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[Tag, Packet | None]: ...

    def set_mode(self, mode: str) -> None: ...

    @property
    def mode(self) -> str: ...

    # @mode.setter
    # def mode(self, value: str) -> None: ...

    def call(
        self,
        tag: Tag,
        packet: Packet,
        record_id: str | None = None,
        execution_engine: ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        skip_cache_lookup: bool = False,
        skip_cache_insert: bool = False,
    ) -> tuple[Tag, Packet | None]:
        """
        Process a single packet with its associated tag.

        This is the core method that defines the Pod's computational behavior.
        It processes one (tag, packet) pair at a time, enabling:
        - Fine-grained caching at the packet level
        - Parallelization opportunities
        - Just-in-time evaluation
        - Filtering operations (by returning None)

        The method signature supports:
        - Tag transformation (modify metadata)
        - Packet transformation (modify content)
        - Filtering (return None to exclude packet)
        - Pass-through (return inputs unchanged)

        Args:
            tag: Metadata associated with the packet
            packet: The data payload to process

        Returns:
            tuple[Tag, Packet | None]:
                - Tag: Output tag (may be modified from input)
                - Packet: Processed packet, or None to filter it out

        Raises:
            TypeError: If packet doesn't match input_packet_types
            ValueError: If packet data is invalid for processing
        """
        ...

    def get_cached_output_for_packet(self, input_packet: Packet) -> Packet | None:
        """
        Retrieve the cached output packet for a given input packet.

        Args:
            input_packet: The input packet to look up in the cache

        Returns:
            Packet | None: The cached output packet, or None if not found
        """
        ...

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all packets processed by this Pod.

        This method returns a table containing all packets processed by the Pod,
        including metadata and system columns if requested. It is useful for:
        - Debugging and analysis
        - Auditing and data lineage tracking
        - Performance monitoring

        Args:
            include_system_columns: Whether to include system columns in the output

        Returns:
            pa.Table | None: A table containing all processed records, or None if no records are available
        """
        ...
