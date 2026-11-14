"""No operator carries its inputs' Data meta columns, on either path (ITL-1080).

Meta columns (``__``-prefixed: a cached pod's ``__computed``, the result cache's
``__pf_*`` and ``__pod_ts``) describe how one input row was produced. An operator's
output row is produced by the operator, so it carries none of them: ``Join`` set the
rule (ITL-854, ITL-857) and every operator follows it, on the sync path (a table, its
prediction) and on the async path (each row's ``Data``) alike.

A raw ``ArrowTableStream`` lists the ``__`` columns of its table as data columns, with
a source-info column for each (``_source___x``); other streams hide them from
``as_table()`` while each row's ``Data`` carries them. Both forms are removed here.
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING

from orcapod.channels import WritableChannel
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

_META_SOURCE_PREFIX = f"{constants.SOURCE_PREFIX}{constants.META_PREFIX}"


def without_meta_columns(table: "pa.Table", tag_columns: Collection[str]) -> "pa.Table":
    """Drop the data meta columns, and a raw stream's source-info columns for them.

    A tag column is never a meta column, whatever its name: a raw stream may name
    a tag with the ``__`` prefix, and it stays.

    Args:
        table: A stream's table.
        tag_columns: The stream's tag columns.

    Returns:
        The table without its ``__`` data columns and ``_source___`` columns.
    """
    meta = [
        name
        for name in table.column_names
        if name.startswith((constants.META_PREFIX, _META_SOURCE_PREFIX))
        and name not in tag_columns
    ]
    return table.drop_columns(meta) if meta else table


def without_meta_fields(schema: Schema) -> Schema:
    """Drop the meta fields from a data schema (a raw stream lists them as data).

    Args:
        schema: A data schema.

    Returns:
        The schema without its ``__`` fields.
    """
    if not any(name.startswith(constants.META_PREFIX) for name in schema):
        return schema
    return Schema(
        {
            name: data_type
            for name, data_type in schema.items()
            if not name.startswith(constants.META_PREFIX)
        },
        optional_fields={
            name
            for name in schema.optional_fields
            if not name.startswith(constants.META_PREFIX)
        },
    )


def without_data_meta(data: DataProtocol) -> DataProtocol:
    """Return ``data`` without its meta columns (``data`` itself when it has none)."""
    if not data.meta_columns:
        return data
    return data.drop_meta_columns(*data.meta_columns)


def _carries_meta(stream: StreamProtocol, inputs: Sequence[StreamProtocol]) -> bool:
    """Whether ``stream``, an operator's result, holds an input's meta columns.

    A table built by the operator holds them only as ``__`` data columns of a raw
    stream. A result that is one of the inputs, returned unaltered, holds whatever
    its rows' ``Data`` hold.
    """
    if any(name.startswith(constants.META_PREFIX) for name in stream.keys()[1]):
        return True
    if not any(stream is input_stream for input_stream in inputs):
        return False
    first = next(iter(stream.iter_data()), None)
    return first is not None and bool(first[1].meta_columns)


def without_input_meta(
    stream: StreamProtocol, inputs: Sequence[StreamProtocol]
) -> StreamProtocol:
    """Return an operator's sync result without its inputs' meta columns.

    Args:
        stream: What the operator's ``static_process`` built.
        inputs: The streams it was given.

    Returns:
        ``stream`` itself when it holds no meta column; otherwise the same rows
        rebuilt without them, with every other column (tags, system tags, data,
        source info, data context) as it was.
    """
    if not _carries_meta(stream, inputs):
        return stream
    table = stream.as_table(
        columns={"source": True, "system_tags": True, "context": True}
    )
    tag_columns = stream.keys()[0]
    return ArrowTableStream(
        without_meta_columns(table, tag_columns),
        tag_columns=tag_columns,
        data_context=stream.data_context_key,
    )


class WithoutMetaWriter:
    """An output channel writer that sends every row without its Data meta columns.

    The async counterpart of ``without_input_meta``: an operator whose
    ``async_execute`` passes rows on wraps its output in it.
    """

    def __init__(self, output: WritableChannel[tuple[TagProtocol, DataProtocol]]):
        self._output = output

    async def send(self, item: tuple[TagProtocol, DataProtocol]) -> None:
        """Send ``item`` without its Data meta columns."""
        tag, data = item
        await self._output.send((tag, without_data_meta(data)))

    async def close(self) -> None:
        """Close the wrapped writer."""
        await self._output.close()
