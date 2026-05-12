from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils import arrow_utils, polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from polars._typing import FrameInitTypes
else:
    pl = LazyModule("polars")
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class DataFrameSource(RootSource):
    """A source backed by a Polars DataFrame (or any Polars-compatible data).

    The DataFrame is converted to an Arrow table and enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system keys).
    """

    def __init__(
        self,
        data: FrameInitTypes,
        key_columns: str | Collection[str] = (),
        system_key_columns: Collection[str] = (),
        source_id: str | None = None,
        schema: pa.Schema | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)

        df = pl.DataFrame(data)

        # Convert any Object-dtype columns to Arrow-compatible types.
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            logger.info(
                f"Converting {len(object_columns)} object column(s) to Arrow format"
            )
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        if isinstance(key_columns, str):
            key_columns = [key_columns]
        key_columns = list(key_columns)

        df = polars_data_utils.drop_system_columns(df)

        missing = set(key_columns) - set(df.columns)
        if missing:
            raise ValueError(f"KeyProtocol column(s) not found in data: {missing}")

        arrow_table = df.to_arrow()
        if schema is not None:
            arrow_table = arrow_table.cast(schema)
        else:
            # Polars defaults all fields to nullable=True; infer the correct
            # flags from the actual data before passing to the builder.
            arrow_table = arrow_table.cast(arrow_utils.infer_schema_nullable(arrow_table))

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            key_columns=key_columns,
            source_id=self._source_id,
            system_key_columns=system_key_columns,
        )

        self._stream = result.stream
        self._key_columns = result.key_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (DataFrame is not serializable)."""
        return {
            "source_type": "data_frame",
            "key_columns": list(self._key_columns),
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> DataFrameSource:
        """Not supported — DataFrameSource cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DataFrameSource cannot be reconstructed from config — "
            "the original DataFrame is not serializable."
        )
