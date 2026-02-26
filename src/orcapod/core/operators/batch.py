from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import TableStream
from orcapod.protocols.core_protocols import Stream
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

    def validate_unary_input(self, stream: Stream) -> None:
        """
        Batch works on any input stream, so no validation is needed.
        """
        return None

    def unary_static_process(self, stream: Stream) -> Stream:
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
        return TableStream(batched_table, tag_columns=tag_columns)

    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        This method should be implemented by subclasses to return the typespecs of the input and output streams.
        It takes two streams as input and returns a tuple of typespecs.
        """
        tag_types, packet_types = stream.output_schema(
            columns=columns, all_info=all_info
        )
        batched_tag_types = {k: list[v] for k, v in tag_types.items()}
        batched_packet_types = {k: list[v] for k, v in packet_types.items()}

        # TODO: check if this is really necessary
        return Schema(batched_tag_types), Schema(batched_packet_types)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.batch_size, self.drop_partial_batch)
