from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    import pyarrow as pa


class ArrowTableSource(RootSource):
    """A source backed by an in-memory PyArrow Table.

    Uses ``SourceStreamBuilder`` to strip system columns, add per-row
    source-info provenance columns and a system tag column encoding the
    schema hash, then wraps the result in an ``ArrowTableStream``.
    """

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        # Infer nullable from actual data: Arrow tables default all fields to
        # nullable=True regardless of content; derive the correct flags here
        # before the builder, which trusts the schema as-is.
        table = table.cast(arrow_utils.infer_schema_nullable(table))

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
