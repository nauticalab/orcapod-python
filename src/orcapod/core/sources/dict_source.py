from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.types import DataValue, SchemaLike


class DictSource(RootSource):
    """A source backed by a collection of Python dictionaries.

    Each dict becomes one (tag, packet) pair in the stream. The dicts are
    converted to an Arrow table via the data-context type converter, then
    enriched by ``SourceStreamBuilder`` (source-info, schema-hash, system tags).
    """

    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        data_schema: SchemaLike | None = None,
        source_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(source_id=source_id, **kwargs)

        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
            [dict(row) for row in data],
            python_schema=data_schema,
        )

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        if self._source_id is None:
            self._source_id = result.source_id

    def to_config(self) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": "dict",
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "DictSource":
        """Not supported — DictSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DictSource cannot be reconstructed from config — "
            "original data is not serializable."
        )
