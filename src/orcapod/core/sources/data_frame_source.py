from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils import polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    from polars._typing import FrameInitTypes
else:
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


class DataFrameSource(RootSource):
    """A source backed by a Polars DataFrame (or any Polars-compatible data).

    The DataFrame is converted to an Arrow table and enriched by
    ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        data: "FrameInitTypes",
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_id: str | None = None,
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

        if isinstance(tag_columns, str):
            tag_columns = [tag_columns]
        tag_columns = list(tag_columns)

        df = polars_data_utils.drop_system_columns(df)

        missing = set(tag_columns) - set(df.columns)
        if missing:
            raise ValueError(f"TagProtocol column(s) not found in data: {missing}")

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            df.to_arrow(),
            tag_columns=tag_columns,
            source_id=self._source_id,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (DataFrame is not serializable)."""
        return {
            "source_type": "data_frame",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> "DataFrameSource":
        """Not supported — DataFrameSource cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DataFrameSource cannot be reconstructed from config — "
            "the original DataFrame is not serializable."
        )
