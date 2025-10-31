import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.streams.base import StreamBase


if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


class WrappedStream(StreamBase):
    def __init__(
        self,
        stream: cp.Stream,
        source: cp.Kernel,
        input_streams: tuple[cp.Stream, ...],
        label: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(source=source, upstreams=input_streams, label=label, **kwargs)
        self._stream = stream

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and packet columns in the stream.
        This is useful for accessing the columns in the stream.
        """
        return self._stream.keys(include_system_tags=include_system_tags)

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        """
        Returns the types of the tag and packet columns in the stream.
        This is useful for accessing the types of the columns in the stream.
        """
        return self._stream.types(include_system_tags=include_system_tags)

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
        """
        Returns the underlying table representation of the stream.
        This is useful for converting the stream to a table format.
        """
        return self._stream.as_table(
            include_data_context=include_data_context,
            include_source=include_source,
            include_system_tags=include_system_tags,
            include_content_hash=include_content_hash,
            sort_by_tags=sort_by_tags,
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )
        # TODO handle default execution engine

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        """
        Iterates over the packets in the stream.
        Each packet is represented as a tuple of (Tag, Packet).
        """
        return self._stream.iter_packets(
            execution_engine=execution_engine,
            execution_engine_opts=execution_engine_opts,
        )

    def identity_structure(self) -> Any:
        return self._stream.identity_structure()
