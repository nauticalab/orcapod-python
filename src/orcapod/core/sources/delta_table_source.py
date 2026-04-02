from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.types import PathLike
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import deltalake
else:
    pa = LazyModule("pyarrow")
    deltalake = LazyModule("deltalake")


class DeltaTableSource(RootSource):
    """A source backed by a Delta Lake table.

    The table is read once at construction time using ``deltalake``'s
    PyArrow integration. The resulting Arrow table is enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        delta_table_path: PathLike,
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        resolved = Path(delta_table_path).resolve()

        if source_id is None:
            source_id = resolved.name
        super().__init__(source_id=source_id, **kwargs)

        self._delta_table_path = resolved

        try:
            self._delta_table = deltalake.DeltaTable(str(self._delta_table_path))
        except deltalake.exceptions.TableNotFoundError:
            raise ValueError(f"Delta table not found at {self._delta_table_path}")

        table: pa.Table = self._delta_table.to_pyarrow_dataset(as_large_types=True).to_table()
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
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        self._record_id_column = record_id_column
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize this source's configuration to a JSON-compatible dict."""
        return {
            "source_type": "delta_table",
            "delta_table_path": str(self._delta_table_path),
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "record_id_column": self._record_id_column,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> "DeltaTableSource":
        """Reconstruct a DeltaTableSource from a config dict."""
        return cls(
            delta_table_path=config["delta_table_path"],
            tag_columns=config.get("tag_columns", ()),
            system_tag_columns=config.get("system_tag_columns", ()),
            record_id_column=config.get("record_id_column"),
            source_id=config.get("source_id"),
        )
