from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import PacketProtocol, StreamProtocol, TagProtocol
from orcapod.types import ColumnConfig
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

from orcapod.types import Schema


class Batch(UnaryOperator):
    """
    Base class for all operators.
    """

    def __init__(self, batch_size: int = 0, drop_partial_batch: bool = False, **kwargs):
        if batch_size < 0:
            raise ValueError("Batch size must be non-negative.")

        super().__init__(**kwargs)

        self.batch_size = batch_size
        self.drop_partial_batch = drop_partial_batch

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        Batch works on any input stream, so no validation is needed.
        """
        return None

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """
        This method should be implemented by subclasses to define the specific behavior of the binary operator.
        It takes two streams as input and returns a new stream as output.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})

        tag_columns, packet_columns = stream.keys()

        data_list = table.to_pylist()

        batched_data = []

        next_batch = {}

        i = 0
        for entry in data_list:
            i += 1
            for c in entry:
                next_batch.setdefault(c, []).append(entry[c])

            if self.batch_size > 0 and i >= self.batch_size:
                batched_data.append(next_batch)
                next_batch = {}
                i = 0

        if i > 0 and not self.drop_partial_batch:
            batched_data.append(next_batch)

        batched_table = pa.Table.from_pylist(batched_data)
        return ArrowTableStream(
            batched_table,
            tag_columns=tag_columns,
        )

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        This method should be implemented by subclasses to return the schemas of the input and output streams.
        It takes two streams as input and returns a tuple of schemas.
        """
        tag_types, packet_types = stream.output_schema(
            columns=columns, all_info=all_info
        )
        batched_tag_types = {k: list[v] for k, v in tag_types.items()}
        batched_packet_types = {k: list[v] for k, v in packet_types.items()}

        # TODO: check if this is really necessary
        return Schema(batched_tag_types), Schema(batched_packet_types)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming batch: emit full batches as they accumulate.

        When ``batch_size > 0``, each group of ``batch_size`` rows is
        materialized and emitted immediately, allowing downstream consumers
        to start processing before all input is consumed.  When
        ``batch_size == 0`` (batch everything), falls back to barrier mode.
        """
        try:
            if self.batch_size == 0:
                # Must collect all rows — barrier fallback
                rows = await inputs[0].collect()
                if rows:
                    stream = self._materialize_to_stream(rows)
                    result = self.unary_static_process(stream)
                    for tag, packet in result.iter_packets():
                        await output.send((tag, packet))
                return

            batch: list[tuple[TagProtocol, PacketProtocol]] = []
            async for tag, packet in inputs[0]:
                batch.append((tag, packet))
                if len(batch) >= self.batch_size:
                    stream = self._materialize_to_stream(batch)
                    result = self.unary_static_process(stream)
                    for out_tag, out_packet in result.iter_packets():
                        await output.send((out_tag, out_packet))
                    batch = []

            # Flush partial batch
            if batch and not self.drop_partial_batch:
                stream = self._materialize_to_stream(batch)
                result = self.unary_static_process(stream)
                for out_tag, out_packet in result.iter_packets():
                    await output.send((out_tag, out_packet))
        finally:
            await output.close()

    def to_config(self) -> dict[str, Any]:
        """Serialize this Batch operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``batch_size`` and ``drop_partial_batch``.
        """
        config = super().to_config()
        config["config"] = {
            "batch_size": self.batch_size,
            "drop_partial_batch": self.drop_partial_batch,
        }
        return config

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.batch_size, self.drop_partial_batch)
