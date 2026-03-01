from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.function_pod import FunctionNode
    from orcapod.core.operator_node import OperatorNode
else:
    pa = LazyModule("pyarrow")


class DerivedSource(RootSource):
    """
    A static stream backed by the computed records of a DB-backed stream node.

    Created by ``FunctionNode.as_source()`` or ``OperatorNode.as_source()``,
    this source reads from the pipeline database, presenting the computed
    results as an immutable stream usable as input to downstream processing.

    The origin must implement ``get_all_records()``, ``output_schema()``,
    ``keys()``, and ``content_hash()``.

    Identity
    --------
    - ``content_hash``: tied to the specific origin node's content hash —
      unique to this exact computation.
    - ``pipeline_hash``: inherited from RootSource — schema-only, so multiple
      DerivedSources with identical schemas share the same pipeline DB table.

    Usage
    -----
    Call ``origin.run()`` before accessing a DerivedSource to ensure the
    pipeline database has been populated.  Accessing iter_packets / as_table on
    an empty database raises ``ValueError``.
    """

    def __init__(
        self,
        origin: "FunctionNode | OperatorNode",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._origin = origin
        self._cached_table: pa.Table | None = None

    def identity_structure(self) -> Any:
        # Tied precisely to the specific FunctionNode's data identity
        return (self._origin.content_hash(),)

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._origin.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._origin.keys(columns=columns, all_info=all_info)

    def _get_stream(self) -> ArrowTableStream:
        if self._cached_table is None:
            records = self._origin.get_all_records()
            if records is None:
                raise ValueError(
                    "DerivedSource has no computed records. "
                    "Call origin.run() first to populate the pipeline database."
                )
            self._cached_table = records
        tag_keys = self._origin.keys()[0]
        return ArrowTableStream(self._cached_table, tag_columns=tag_keys)

    def iter_packets(self):
        return self._get_stream().iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._get_stream().as_table(columns=columns, all_info=all_info)
