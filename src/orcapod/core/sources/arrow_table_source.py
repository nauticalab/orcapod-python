from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    import pyarrow as pa

logger = logging.getLogger(__name__)


class ArrowTableSource(RootSource):
    """A source backed by an in-memory PyArrow Table.

    Uses ``SourceStreamBuilder`` to strip system columns, add per-row
    source-info provenance columns and a system tag column encoding the
    schema hash, then wraps the result in an ``ArrowTableStream``.

    Nullable handling
    -----------------
    Arrow represents "this field has no nulls" and "this field may have nulls"
    via a per-field ``nullable`` flag.  By default **all** Arrow fields are
    ``nullable=True`` regardless of actual content, so a plain ``pa.table({...})``
    or a Polars ``.to_arrow()`` result will always produce ``T | None`` schema
    types unless the caller has explicitly constructed the schema.

    ``ArrowTableSource`` therefore defaults to ``infer_nullable=False``: the
    incoming table's schema is trusted as-is.  This is safe when the caller
    has already set nullable flags deliberately (e.g. loaded from a typed
    Parquet file, or produced by an upstream pipeline stage).

    If you are passing a *raw* Arrow table whose schema carries only Arrow's
    all-nullable default (``nullable=True`` everywhere), set
    ``infer_nullable=True`` or call
    ``table.cast(arrow_utils.infer_schema_nullable(table))`` yourself before
    constructing the source.  ``infer_schema_nullable`` derives
    ``nullable = (null_count > 0)`` per column — i.e. only columns that
    *actually contain* nulls in the supplied data are marked nullable.

    Note: other source classes (``DictSource``, ``ListSource``,
    ``DataFrameSource``, ``CsvSource``, etc.) construct their Arrow tables
    internally from non-Arrow inputs and always apply ``infer_schema_nullable``
    automatically, because the all-nullable Arrow default is never intentional
    in those paths.  ``ArrowTableSource`` is unique in accepting a pre-existing
    ``pa.Table`` whose schema may already be deliberate.
    """

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        infer_nullable: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        if infer_nullable:
            logger.debug(
                "infer_nullable=True: deriving nullable flags from data "
                "(nullable=True only for columns with null_count > 0)"
            )
            table = table.cast(arrow_utils.infer_schema_nullable(table))
        # else: schema is trusted as-is.

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._schema_hash = result.schema_hash
        self._table_hash = result.table_hash
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        self._record_id_column = record_id_column

        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (in-memory table is not serializable)."""
        return {
            "source_type": "arrow_table",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> ArrowTableSource:
        """Not supported — ArrowTableSource cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "ArrowTableSource cannot be reconstructed from config — "
            "the in-memory Arrow table is not serializable."
        )

    @property
    def table(self) -> "pa.Table":
        """Return the enriched table (with source-info and system tags)."""
        return self._stream.as_table(columns={"source": True, "system_tags": True})
