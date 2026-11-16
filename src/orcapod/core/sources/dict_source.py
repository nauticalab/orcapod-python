from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.core.referential_tags import TagOnlyColumns, check_tag_only_columns
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import (
    SourceStreamBuilder,
    normalize_record_id_columns,
)
from orcapod.types import DataValue, SchemaLike
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import _normalize_column_list
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.tensor_conversions import warn_source_tensor_conversions

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DictSource(RootSource):
    """A source backed by a collection of Python dictionaries.

    Each dict becomes one (tag, data) pair in the stream. The dicts are
    converted to an Arrow table via the data-context type converter, then
    enriched by ``SourceStreamBuilder`` (source-info, schema-hash, system tags).

    Plain tags: ``tag_only_columns`` (keyword-only) is ``True``, the default, and keeps
    every tag column a tag only, not data, as every source does in v0.3. Any other value
    is refused: referential tags arrive in v0.4, where tags become referential by default
    and ``tag_only_columns`` names the tags kept plain.
    """

    # The config type key; also the kind prefix of the default source id.
    SOURCE_KIND: ClassVar[str] = "dict"

    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        data_schema: SchemaLike | pa.Schema | None = None,
        source_id: str | None = None,
        *,
        tag_only_columns: TagOnlyColumns = True,
        record_id_columns: str | Collection[str] | None = None,
        **kwargs: Any,
    ) -> None:
        check_tag_only_columns(tag_only_columns)
        super().__init__(source_id=source_id, **kwargs)
        tag_columns = _normalize_column_list(tag_columns)

        # Accept either a Python SchemaLike (Mapping[str, type]) or a
        # pa.Schema directly via data_schema; route to the converter accordingly.
        rows = [dict(row) for row in data]
        if isinstance(data_schema, pa.Schema):
            arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                rows,
                arrow_schema=data_schema,
            )
        else:
            arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                rows,
                python_schema=data_schema,
            )
            if data_schema is None:
                # No explicit schema — infer nullable from actual values.
                # The type converter defaults all fields to nullable=True; derive
                # the correct flags here so the builder can trust the schema as-is.
                arrow_table = arrow_table.cast(
                    arrow_utils.infer_schema_nullable(arrow_table)
                )

        self._key_columns = normalize_record_id_columns(record_id_columns) or ()
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=tag_columns,
            source_id=self._source_id,
            source_kind=self.SOURCE_KIND,
            record_id_columns=self._key_columns,
            system_tag_columns=system_tag_columns,
        )

        self._stream = result.stream
        self._tag_columns = result.tag_columns
        if self._source_id is None:
            self._source_id = result.source_id
        warn_source_tensor_conversions(self._source_id, rows)

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize metadata-only config (data is not serializable)."""
        return {
            "source_type": self.SOURCE_KIND,
            "tag_columns": list(self._tag_columns),
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry=None) -> DictSource:
        """Not supported — DictSource data cannot be reconstructed from config.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError(
            "DictSource cannot be reconstructed from config — "
            "original data is not serializable."
        )
