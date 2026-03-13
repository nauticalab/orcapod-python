from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import Config
from orcapod.core.streams.base import StreamBase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import ColumnConfig, Schema

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


class RootSource(StreamBase):
    """Abstract base class for all root sources in Orcapod.

    A RootSource is a pure stream — the root of a computational graph, producing
    data from an external source (file, database, in-memory data, etc.) with no
    upstream dependencies.

    As a StreamProtocol:
    - ``source`` returns ``None`` (no upstream source pod)
    - ``upstreams`` is always empty
    - ``keys``, ``output_schema``, ``iter_packets``, ``as_table`` delegate to
      ``self._stream`` by default; concrete subclasses may override them.

    As a PipelineElementProtocol:
    - ``pipeline_identity_structure()`` returns ``(tag_schema, packet_schema)``
      — schema-only, no data content — forming the base case of the pipeline
      identity Merkle chain.

    Source identity:
        Every source has a ``source_id`` — a canonical name that determines the
        source's content identity and is used in the ``SourceRegistry`` so that
        provenance tokens embedded in downstream data can be resolved back to the
        originating source object.

        Concrete subclasses must ensure ``_source_id`` is set by the end of
        ``__init__``.  File-backed sources (DeltaTableSource, CSVSource) default
        to the file path; ``ArrowTableSource`` defaults to the table's data hash.

    Field resolution:
        All sources expose ``resolve_field(record_id, field_name)``.  The default
        implementation raises ``NotImplementedError``; concrete subclasses
        that back addressable data should override it.

    Concrete subclasses must set ``self._stream`` in their ``__init__`` to get
    the default stream delegation behavior. They may also override
    ``identity_structure()``.
    """

    def __init__(
        self,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        super().__init__(
            label=label,
            data_context=data_context,
            config=config,
        )
        self._source_id = source_id

    # -------------------------------------------------------------------------
    # Source identity
    # -------------------------------------------------------------------------

    @property
    def source_id(self) -> str:
        """Canonical name for this source used in the registry and provenance
        strings. If not set, raises ``ValueError``.
        """
        if self._source_id is None:
            raise ValueError("source_id is not set")
        return self._source_id

    # -------------------------------------------------------------------------
    # Content identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Default identity based on class name, output schema, and source_id."""
        return (self.__class__.__name__, self.output_schema(), self.source_id)

    # -------------------------------------------------------------------------
    # Field resolution
    # -------------------------------------------------------------------------

    def resolve_field(self, record_id: str, field_name: str) -> Any:
        """Resolve a field value for a record.

        Not implemented by default. Subclasses that back addressable data
        should override this method.

        Raises:
            NotImplementedError: Always, by default.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement resolve_field. "
            f"Cannot resolve field {field_name!r} for record {record_id!r}."
        )

    def computed_label(self) -> str | None:
        """Return the source_id as the label."""
        return self._source_id

    # -------------------------------------------------------------------------
    # PipelineElementProtocol — schema-only identity (base case of Merkle chain)
    # -------------------------------------------------------------------------

    def pipeline_identity_structure(self) -> Any:
        """Return (tag_schema, packet_schema) as the pipeline identity for this
        source.  Schema-only: no data content is included, so sources with
        identical schemas share the same pipeline hash and therefore the same
        pipeline database table.
        """
        tag_schema, packet_schema = self.output_schema()
        return (tag_schema, packet_schema)

    # -------------------------------------------------------------------------
    # StreamProtocol protocol
    # -------------------------------------------------------------------------

    @property
    def producer(self) -> None:
        """Root sources have no upstream source pod."""
        return None

    @property
    def upstreams(self) -> tuple[StreamProtocol, ...]:
        """Sources have no upstream dependencies."""
        return ()

    # -------------------------------------------------------------------------
    # Stream delegation defaults
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Delegate to the underlying stream's output_schema."""
        return self._stream.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Delegate to the underlying stream's keys."""
        return self._stream.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        """Delegate to the underlying stream's iter_packets."""
        return self._stream.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Delegate to the underlying stream's as_table."""
        return self._stream.as_table(columns=columns, all_info=all_info)

    # -------------------------------------------------------------------------
    # Convenience — caching
    # -------------------------------------------------------------------------

    def cached(
        self,
        cache_database: ArrowDatabaseProtocol,
        cache_path_prefix: tuple[str, ...] = (),
        **kwargs: Any,
    ) -> CachedSource:
        """Return a ``CachedSource`` wrapping this source.

        Args:
            cache_database: Database to store cached records in.
            cache_path_prefix: Path prefix for the cache table.
            **kwargs: Additional keyword arguments passed to ``CachedSource``.

        Returns:
            A ``CachedSource`` that caches this source's output.
        """
        from orcapod.core.sources.cached_source import CachedSource

        return CachedSource(
            source=self,
            cache_database=cache_database,
            cache_path_prefix=cache_path_prefix,
            **kwargs,
        )
