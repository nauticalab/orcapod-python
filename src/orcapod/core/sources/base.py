from abc import abstractmethod
from ast import Not
from collections.abc import Collection, Iterator
from typing import TYPE_CHECKING, Any


from orcapod.core.kernels import TrackedKernelBase
from orcapod.core.streams import (
    KernelStream,
    StatefulStreamBase,
)
from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class InvocationBase(TrackedKernelBase, StatefulStreamBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Cache the KernelStream for reuse across all stream method calls
        self._cached_kernel_stream: KernelStream | None = None

    def computed_label(self) -> str | None:
        return None

    @abstractmethod
    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any: ...

    # Redefine the reference to ensure subclass would provide a concrete implementation
    @property
    @abstractmethod
    def reference(self) -> tuple[str, ...]:
        """Return the unique identifier for the kernel."""
        ...

    # =========================== Kernel Methods ===========================

    # The following are inherited from TrackedKernelBase as abstract methods.
    # @abstractmethod
    # def forward(self, *streams: dp.Stream) -> dp.Stream:
    #     """
    #     Pure computation: return a static snapshot of the data.

    #     This is the core method that subclasses must implement.
    #     Each call should return a fresh stream representing the current state of the data.
    #     This is what KernelStream calls when it needs to refresh its data.
    #     """
    #     ...

    # @abstractmethod
    # def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
    #     """Return the tag and packet types this source produces."""
    #     ...

    # @abstractmethod
    # def kernel_identity_structure(
    #     self, streams: Collection[dp.Stream] | None = None
    # ) -> dp.Any: ...

    def prepare_output_stream(
        self, *streams: cp.Stream, label: str | None = None
    ) -> KernelStream:
        if self._cached_kernel_stream is None:
            self._cached_kernel_stream = super().prepare_output_stream(
                *streams, label=label
            )
        return self._cached_kernel_stream

    def track_invocation(self, *streams: cp.Stream, label: str | None = None) -> None:
        raise NotImplementedError("Behavior for track invocation is not determined")

    # ==================== Stream Protocol (Delegation) ====================

    @property
    def source(self) -> cp.Kernel | None:
        """Sources are their own source."""
        return self

    # @property
    # def upstreams(self) -> tuple[cp.Stream, ...]: ...

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Delegate to the cached KernelStream."""
        return self().keys(include_system_tags=include_system_tags)

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """Delegate to the cached KernelStream."""
        return self().types(include_system_tags=include_system_tags)

    @property
    def last_modified(self):
        """Delegate to the cached KernelStream."""
        return self().last_modified

    @property
    def is_current(self) -> bool:
        """Delegate to the cached KernelStream."""
        return self().is_current

    def __iter__(self) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        """
        Iterate over the cached KernelStream.

        This allows direct iteration over the source as if it were a stream.
        """
        return self().iter_packets()

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().iter_packets(
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pa.Table":
        """Delegate to the cached KernelStream."""
        return self().as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def flow(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Collection[tuple[cp.Tag, cp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().flow(
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Run the source node, executing the contained source.

        This is a no-op for sources since they are not executed like pods.
        """
        self().run(
            *args,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
            **kwargs,
        )

    async def run_async(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Run the source node asynchronously, executing the contained source.

        This is a no-op for sources since they are not executed like pods.
        """
        await self().run_async(
            *args,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
            **kwargs,
        )

    # ==================== LiveStream Protocol (Delegation) ====================

    def refresh(self, force: bool = False) -> bool:
        """Delegate to the cached KernelStream."""
        return self().refresh(force=force)

    def invalidate(self) -> None:
        """Delegate to the cached KernelStream."""
        return self().invalidate()


class SourceBase(TrackedKernelBase, StatefulStreamBase):
    """
    Base class for sources that act as both Kernels and LiveStreams.

    Design Philosophy:
    1. Source is fundamentally a Kernel (data loader)
    2. forward() returns static snapshots as a stream (pure computation)
    3. __call__() returns a cached KernelStream (live, tracked)
    4. All stream methods delegate to the cached KernelStream

    This ensures that direct source iteration and source() iteration
    are identical and both benefit from KernelStream's lifecycle management.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Cache the KernelStream for reuse across all stream method calls
        self._cached_kernel_stream: KernelStream | None = None
        self._schema_hash: str | None = None

    # reset, so that computed label won't be used from StatefulStreamBase
    def computed_label(self) -> str | None:
        return None

    def schema_hash(self) -> str:
        if self._schema_hash is None:
            self._schema_hash = self.data_context.object_hasher.hash_object(
                (self.tag_types(), self.packet_types())
            ).to_hex(self.orcapod_config.schema_hash_n_char)
        return self._schema_hash

    def kernel_identity_structure(
        self, streams: Collection[cp.Stream] | None = None
    ) -> Any:
        if streams is not None:
            # when checked for invocation id, act as a source
            # and just return the output packet types
            # _, packet_types = self.stream.types()
            # return packet_types
            return self.schema_hash()
        # otherwise, return the identity structure of the stream
        return self.source_identity_structure()

    @property
    def source_id(self) -> str:
        return ":".join(self.reference)

    # Redefine the reference to ensure subclass would provide a concrete implementation
    @property
    @abstractmethod
    def reference(self) -> tuple[str, ...]:
        """Return the unique identifier for the kernel."""
        ...

    def kernel_output_types(
        self, *streams: cp.Stream, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        return self.source_output_types(include_system_tags=include_system_tags)

    @abstractmethod
    def source_identity_structure(self) -> Any: ...

    @abstractmethod
    def source_output_types(self, include_system_tags: bool = False) -> Any: ...

    # =========================== Kernel Methods ===========================

    # The following are inherited from TrackedKernelBase as abstract methods.
    # @abstractmethod
    # def forward(self, *streams: dp.Stream) -> dp.Stream:
    #     """
    #     Pure computation: return a static snapshot of the data.

    #     This is the core method that subclasses must implement.
    #     Each call should return a fresh stream representing the current state of the data.
    #     This is what KernelStream calls when it needs to refresh its data.
    #     """
    #     ...

    # @abstractmethod
    # def kernel_output_types(self, *streams: dp.Stream) -> tuple[TypeSpec, TypeSpec]:
    #     """Return the tag and packet types this source produces."""
    #     ...

    # @abstractmethod
    # def kernel_identity_structure(
    #     self, streams: Collection[dp.Stream] | None = None
    # ) -> dp.Any: ...

    def validate_inputs(self, *streams: cp.Stream) -> None:
        """Sources take no input streams."""
        if len(streams) > 0:
            raise ValueError(
                f"{self.__class__.__name__} is a source and takes no input streams"
            )

    def prepare_output_stream(
        self, *streams: cp.Stream, label: str | None = None
    ) -> KernelStream:
        if self._cached_kernel_stream is None:
            self._cached_kernel_stream = super().prepare_output_stream(
                *streams, label=label
            )
        return self._cached_kernel_stream

    def track_invocation(self, *streams: cp.Stream, label: str | None = None) -> None:
        if not self._skip_tracking and self._tracker_manager is not None:
            self._tracker_manager.record_source_invocation(self, label=label)

    # ==================== Stream Protocol (Delegation) ====================

    @property
    def source(self) -> cp.Kernel | None:
        """Sources are their own source."""
        return self

    @property
    def upstreams(self) -> tuple[cp.Stream, ...]:
        """Sources have no upstream dependencies."""
        return ()

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Delegate to the cached KernelStream."""
        return self().keys(include_system_tags=include_system_tags)

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """Delegate to the cached KernelStream."""
        return self().types(include_system_tags=include_system_tags)

    @property
    def last_modified(self):
        """Delegate to the cached KernelStream."""
        return self().last_modified

    @property
    def is_current(self) -> bool:
        """Delegate to the cached KernelStream."""
        return self().is_current

    def __iter__(self) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        """
        Iterate over the cached KernelStream.

        This allows direct iteration over the source as if it were a stream.
        """
        return self().iter_packets()

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().iter_packets(
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pa.Table":
        """Delegate to the cached KernelStream."""
        return self().as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def flow(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Collection[tuple[cp.Tag, cp.Packet]]:
        """Delegate to the cached KernelStream."""
        return self().flow(
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Run the source node, executing the contained source.

        This is a no-op for sources since they are not executed like pods.
        """
        self().run(
            *args,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
            **kwargs,
        )

    async def run_async(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Run the source node asynchronously, executing the contained source.

        This is a no-op for sources since they are not executed like pods.
        """
        await self().run_async(
            *args,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
            **kwargs,
        )

    # ==================== LiveStream Protocol (Delegation) ====================

    def refresh(self, force: bool = False) -> bool:
        """Delegate to the cached KernelStream."""
        return self().refresh(force=force)

    def invalidate(self) -> None:
        """Delegate to the cached KernelStream."""
        return self().invalidate()

    # ==================== Source Protocol ====================

    def reset_cache(self) -> None:
        """
        Clear the cached KernelStream, forcing a fresh one on next access.

        Useful when the underlying data source has fundamentally changed
        (e.g., file path changed, database connection reset).
        """
        if self._cached_kernel_stream is not None:
            self._cached_kernel_stream.invalidate()
        self._cached_kernel_stream = None


class StreamSource(SourceBase):
    def __init__(self, stream: cp.Stream, label: str | None = None, **kwargs) -> None:
        """
        A placeholder source based on stream
        This is used to represent a kernel that has no computation.
        """
        label = label or stream.label
        self.stream = stream
        super().__init__(label=label, **kwargs)

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Returns the types of the tag and packet columns in the stream.
        This is useful for accessing the types of the columns in the stream.
        """
        return self.stream.types(include_system_tags=include_system_tags)

    @property
    def reference(self) -> tuple[str, ...]:
        return ("stream", self.stream.content_hash().to_string())

    def forward(self, *args: Any, **kwargs: Any) -> cp.Stream:
        """
        Forward the stream through the stub kernel.
        This is a no-op and simply returns the stream.
        """
        return self.stream

    def source_identity_structure(self) -> Any:
        return self.stream.identity_structure()

    # def __hash__(self) -> int:
    #     # TODO: resolve the logic around identity structure on a stream / stub kernel
    #     """
    #     Hash the StubKernel based on its label and stream.
    #     This is used to uniquely identify the StubKernel in the tracker.
    #     """
    #     identity_structure = self.identity_structure()
    #     if identity_structure is None:
    #         return hash(self.stream)
    #     return identity_structure


# ==================== Example Implementation ====================
