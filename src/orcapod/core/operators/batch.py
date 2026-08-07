from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig
from orcapod.utils import arrow_utils
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
        """Group rows into fixed-size batches, list-wrapping their values.

        Tag and data columns become list-valued.  Source-info columns become
        list-valued too, one element per batch member.  System-tag columns are
        folded to a scalar instead -- record identity hashes them directly, so
        they must not become lists.

        Args:
            stream: The upstream stream.

        Returns:
            A stream with one row per batch.
        """
        table = stream.as_table(columns={"source": True, "system_tags": True})

        tag_columns, _ = stream.keys()

        system_tag_columns = tuple(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        member_columns = tuple(
            c for c in table.column_names if c not in system_tag_columns
        )

        data_list = table.to_pylist()

        batches: list[list[dict[str, Any]]] = []
        next_batch: list[dict[str, Any]] = []

        for entry in data_list:
            next_batch.append(entry)
            if self.batch_size > 0 and len(next_batch) >= self.batch_size:
                batches.append(next_batch)
                next_batch = []

        if next_batch and not self.drop_partial_batch:
            batches.append(next_batch)

        batched_data = [
            {
                **{c: [m[c] for m in members] for c in member_columns},
                **{
                    c: arrow_utils.fold_system_tag_values(c, [m[c] for m in members])
                    for c in system_tag_columns
                },
            }
            for members in batches
        ]

        input_fields = {f.name: f for f in table.schema}
        batched_schema = pa.schema([
            pa.field(c, pa.list_(input_fields[c].type), nullable=False)
            if c in member_columns
            else input_fields[c]
            for c in table.column_names
        ])
        batched_table = pa.Table.from_pylist(batched_data, schema=batched_schema)

        n_char = self.orcapod_config.hashing.system_tag_n_char
        batched_table = arrow_utils.append_to_system_tags(
            batched_table, stream.pipeline_hash().to_hex(n_char)
        )

        return ArrowTableStream(
            batched_table,
            tag_columns=tag_columns,
            data_context=stream.data_context,
        )

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Predict the batched output schemas without batching.

        Every user tag, data, and source column becomes ``list[T]``.  System
        tag columns keep their scalar type and gain a ``::{pipeline_hash}``
        name suffix.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_types, data_types = stream.output_schema(columns=columns, all_info=all_info)
        n_char = self.orcapod_config.hashing.system_tag_n_char
        suffix = stream.pipeline_hash().to_hex(n_char)

        batched_tag_types: dict[str, Any] = {}
        for name, col_type in tag_types.items():
            if name.startswith(constants.SYSTEM_TAG_PREFIX):
                batched_tag_types[f"{name}{constants.BLOCK_SEPARATOR}{suffix}"] = (
                    col_type
                )
            else:
                batched_tag_types[name] = list[col_type]

        batched_data_types = {k: list[v] for k, v in data_types.items()}

        return Schema(batched_tag_types), Schema(batched_data_types)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
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
                    for tag, data in result.iter_data():
                        await output.send((tag, data))
                return

            batch: list[tuple[TagProtocol, DataProtocol]] = []
            async for tag, data in inputs[0]:
                batch.append((tag, data))
                if len(batch) >= self.batch_size:
                    stream = self._materialize_to_stream(batch)
                    result = self.unary_static_process(stream)
                    for out_tag, out_data in result.iter_data():
                        await output.send((out_tag, out_data))
                    batch = []

            # Flush partial batch
            if batch and not self.drop_partial_batch:
                stream = self._materialize_to_stream(batch)
                result = self.unary_static_process(stream)
                for out_tag, out_data in result.iter_data():
                    await output.send((out_tag, out_data))
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
