import logging
from collections.abc import Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Any

from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.streams.base import StreamBase


if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
    import polars as pl
    import pandas as pd
    import asyncio
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")
    pd = LazyModule("pandas")
    asyncio = LazyModule("asyncio")


# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


class KernelStream(StreamBase):
    """
    Recomputable stream that wraps a stream produced by a kernel to provide
    an abstraction over the stream, taking the stream's source and upstreams as the basis of
    recomputing the stream.

    This stream is used to represent the output of a kernel invocation.
    """

    def __init__(
        self,
        output_stream: cp.Stream | None = None,
        source: cp.Kernel | None = None,
        upstreams: tuple[
            cp.Stream, ...
        ] = (),  # if provided, this will override the upstreams of the output_stream
        **kwargs,
    ) -> None:
        if (output_stream is None or output_stream.source is None) and source is None:
            raise ValueError(
                "Either output_stream must have a kernel assigned to it or source must be provided in order to be recomputable."
            )
        if source is None:
            if output_stream is None or output_stream.source is None:
                raise ValueError(
                    "Either output_stream must have a kernel assigned to it or source must be provided in order to be recomputable."
                )
            source = output_stream.source
            upstreams = upstreams or output_stream.upstreams

        super().__init__(source=source, upstreams=upstreams, **kwargs)
        self.kernel = source
        self._cached_stream = output_stream

    def clear_cache(self) -> None:
        """
        Clears the cached stream.
        This is useful for re-processing the stream with the same kernel.
        """
        self._cached_stream = None
        self._set_modified_time(invalidate=True)

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and packet columns in the stream.
        This is useful for accessing the columns in the stream.
        """
        tag_types, packet_types = self.kernel.output_types(
            *self.upstreams, include_system_tags=include_system_tags
        )
        return tuple(tag_types.keys()), tuple(packet_types.keys())

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Returns the types of the tag and packet columns in the stream.
        This is useful for accessing the types of the columns in the stream.
        """
        return self.kernel.output_types(
            *self.upstreams, include_system_tags=include_system_tags
        )

    @property
    def is_current(self) -> bool:
        if self._cached_stream is None or not super().is_current:
            status = self.refresh()
            if not status:  # if it failed to update for whatever reason
                return False
        return True

    def refresh(self, force: bool = False) -> bool:
        updated = False
        if force or (self._cached_stream is not None and not super().is_current):
            self.clear_cache()

        if self._cached_stream is None:
            assert self.source is not None, (
                "Stream source must be set to recompute the stream."
            )
            self._cached_stream = self.source.forward(*self.upstreams)
            self._set_modified_time()
            updated = True

        if self._cached_stream is None:
            # TODO: use beter error type
            raise ValueError(
                "Stream could not be updated. Ensure that the source is valid and upstreams are correct."
            )

        return updated

    def invalidate(self) -> None:
        """
        Invalidate the stream, marking it as needing recomputation.
        This will clear the cached stream and set the last modified time to None.
        """
        self.clear_cache()
        self._set_modified_time(invalidate=True)

    @property
    def last_modified(self) -> datetime | None:
        if self._cached_stream is None:
            return None
        return self._cached_stream.last_modified

    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.refresh()
        assert self._cached_stream is not None, (
            "Stream has not been updated or is empty."
        )
        self._cached_stream.run(
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
        self.refresh()
        assert self._cached_stream is not None, (
            "Stream has not been updated or is empty."
        )
        await self._cached_stream.run_async(
            *args,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
            **kwargs,
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
        self.refresh()
        assert self._cached_stream is not None, (
            "Stream has not been updated or is empty."
        )
        return self._cached_stream.as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        self.refresh()
        assert self._cached_stream is not None, (
            "Stream has not been updated or is empty."
        )
        return self._cached_stream.iter_packets(
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(kernel={self.source}, upstreams={self.upstreams})"
