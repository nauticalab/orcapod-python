from __future__ import annotations

from collections.abc import Collection
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.base import RootSource
from orcapod.types import ColumnConfig, PathLike, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DeltaTableSource(RootSource):
    """
    A source backed by a Delta Lake table.

    The table is read once at construction time using ``deltalake``'s
    PyArrow integration.  The resulting Arrow table is handed off to
    ``ArrowTableSource`` which adds source-info provenance and schema-hash
    system tags.

    Parameters
    ----------
    delta_table_path:
        Filesystem path to the Delta table directory.
    tag_columns:
        Column names whose values form the tag for each row.
    system_tag_columns:
        Additional system-level tag columns.
    record_id_column:
        Column whose values serve as stable record identifiers in provenance
        strings and ``resolve_field`` lookups.  When ``None`` (default) the
        positional row index is used instead.  For Delta tables a dedicated
        primary-key column is strongly recommended for stable lineage.
    source_id:
        Canonical registry name for this source (passed to ``RootSource``).
    auto_register:
        Whether to auto-register with the source registry on construction.
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
        from deltalake import DeltaTable
        from deltalake.exceptions import TableNotFoundError

        resolved = Path(delta_table_path).resolve()

        if source_id is None:
            source_id = resolved.name
        super().__init__(source_id=source_id, **kwargs)

        self._delta_table_path = resolved

        try:
            delta_table = DeltaTable(str(resolved))
        except TableNotFoundError:
            raise ValueError(f"Delta table not found at {resolved}")

        table: pa.Table = delta_table.to_pyarrow_dataset(as_large_types=True).to_table()

        self._arrow_source = ArrowTableSource(
            table=table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            record_id_column=record_id_column,
            source_id=self.source_id,
            data_context=self.data_context,
            config=self.orcapod_config,
        )

    def resolve_field(self, record_id: str, field_name: str) -> Any:
        return self._arrow_source.resolve_field(record_id, field_name)

    def identity_structure(self) -> Any:
        return self._arrow_source.identity_structure()

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._arrow_source.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._arrow_source.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        return self._arrow_source.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._arrow_source.as_table(columns=columns, all_info=all_info)
