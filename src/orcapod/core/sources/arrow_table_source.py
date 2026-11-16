from __future__ import annotations

import logging
from collections.abc import Collection
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.core.referential_tags import TagOnlyColumns, check_tag_only_columns
from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.utils import arrow_utils
from orcapod.utils.schema_utils import _normalize_column_list

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

    Record identity
    ---------------
    Every row gets a record ID system tag derived from ``source_id`` and a
    per-row token.  With ``record_id_column`` the token is that column's value,
    so the ID follows the row.  Without it the token is the row's position
    (``row_0``, ``row_1``, ...), so a row supplied at a different position
    under the same ``source_id`` gets a different record ID.  Downstream
    pipeline entries and ``SideEffectPod`` completion records are keyed on
    the system tags plus the hash of the row's data, so the moved row is
    treated as a new entry: it is recorded again (two entries with identical
    tag values but distinct record IDs) and delivered again by a
    ``SideEffectPod``.  No result is paired with the wrong row, because every
    key includes the row's own data hash.  ``source_id`` must be stable too:
    when omitted it defaults to ``arrow_table:<algorithm label>:<hex>`` of the
    table hash, which changes with row order.
    Whenever row order can vary between runs, pass an explicit ``source_id``
    and set ``record_id_column`` to a column that is unique per row.

    Plain tags: ``tag_only_columns`` (keyword-only) is ``True``, the default, and keeps
    every tag column a tag only, not data, as every source does in v0.3. Any other value
    is refused: referential tags arrive in v0.4, where tags become referential by default
    and ``tag_only_columns`` names the tags kept plain.
    """

    # The config type key; also the kind prefix of the default source id.
    SOURCE_KIND: ClassVar[str] = "arrow_table"

    def __init__(
        self,
        table: "pa.Table",
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        record_id_column: str | None = None,
        infer_nullable: bool = False,
        *,
        tag_only_columns: TagOnlyColumns = True,
        **kwargs: Any,
    ) -> None:
        check_tag_only_columns(tag_only_columns)
        super().__init__(**kwargs)
        tag_columns = _normalize_column_list(tag_columns)

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
            source_kind=self.SOURCE_KIND,
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
            "source_type": self.SOURCE_KIND,
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
