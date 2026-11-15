from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.streams.base import StreamBase, typed_zero_row_table
from orcapod.core.streams.view import StreamView
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
    - ``keys``, ``output_schema``, ``iter_data``, ``as_table`` delegate to
      ``self._stream`` by default; concrete subclasses may override them.

    As a PipelineElementProtocol:
    - ``pipeline_identity_structure()`` returns ``(tag_schema, data_schema)``
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

    Concrete subclasses must set ``self._stream`` in their ``__init__`` to get
    the default stream delegation behavior. They may also override
    ``identity_structure()``.
    """

    def __init__(
        self,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
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
    # PipelineElementProtocol — schema-only identity (base case of Merkle chain)
    # -------------------------------------------------------------------------

    def _identity_config(self) -> dict[str, Any]:
        """Return identity fields for inclusion in ``to_config()`` output.

        These fields allow ``SourceProxy`` to be constructed when the source
        cannot be reconstructed from config, preserving identity hashes and
        schemas for downstream consumers.
        """
        from orcapod.pipeline.serialization import (
            serialize_nullability,
            serialize_schema,
        )

        tag_schema, data_schema = self.output_schema()
        type_converter = self.data_context.type_converter
        return {
            "content_hash": self.content_hash().to_string(),
            "pipeline_hash": self.pipeline_hash().to_string(),
            "tag_schema": serialize_schema(tag_schema, type_converter),
            "data_schema": serialize_schema(data_schema, type_converter),
            "nullable": serialize_nullability(tag_schema, data_schema, type_converter),
        }

    def pipeline_identity_structure(self) -> Any:
        """Return (tag_schema, data_schema) as the pipeline identity for this
        source.  Schema-only: no data content is included, so sources with
        identical schemas share the same pipeline hash and therefore the same
        pipeline database table.
        """
        tag_schema, data_schema = self.output_schema()
        return (tag_schema, data_schema)

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

    def iter_data(self):
        """Delegate to the underlying stream's iter_data."""
        return self._stream.iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Delegate to the underlying stream's as_table."""
        return self._stream.as_table(columns=columns, all_info=all_info)

    # -------------------------------------------------------------------------
    # The start-of-run check (ITL-1102)
    # -------------------------------------------------------------------------

    def check_for_new_data(self) -> bool:
        """Check whether the data behind the source changed; if so, take it in.

        Every run (``job.run()``, ``run_async()``, a job node's ``run()``, a load-only
        run too) calls this once on each source feeding it, before deciding which
        nodes to run (developer, November 14, 2026, ITL-1102). A source that finds new
        data takes it in and moves its modified time to that moment, so every job node
        below it is stale and runs. A source that finds nothing changes nothing, so a
        current job stays current. ``head()`` and the notebook preview never call it.

        A source built over data held in memory (``ArrowTableSource``, ``DictSource``,
        ``ListSource``, ``DataFrameSource``) cannot change: this default returns
        ``False``. A source that raises here does not fail the run: the run issues a
        ``SourceCheckFailedWarning`` and proceeds with the data the source holds, so a
        source must leave its data and modified time as they were when it raises. A
        source whose reads refresh it (``PollingSource``: a read polls) is refreshed
        again by a run that reads it, and a refresh that fails then raises (ITL-1139).

        Returns:
            ``True`` if the source found new data and took it in.
        """
        return False

    # A source's rows are its own, not computed: a run() view says "all N rows".
    _RUN_ORIGIN: str = "held"

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """The source's zero-row table, from the stream it holds; see ``typed_zero_row_table``.

        A source that reads its table when it is built (Arrow table, dict, list, data
        frame, CSV, Delta, SQLite, PostgreSQL) holds it in ``_stream``, and a SpiralDB
        source holds a zero-row stream with the table's schema: either is cut to no
        rows, so no row is read. A source with no such stream (polling, cached,
        derived, proxy) reads or polls its rows later and answers ``None``.
        """
        return typed_zero_row_table(getattr(self, "_stream", None))

    def _display_name(self) -> str:
        """The name a preview header shows: the label when one was given, else the
        source id (a source id is the source's name), e.g. a table's name. The label
        itself is not changed."""
        if self.has_assigned_label or self._source_id is None:
            return self.label
        return self._source_id

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first ``n`` rows of the table the source holds, without computing.

        A source that reads its table when it is built holds it in memory, so this is a
        slice of it. A source that reads its table later overrides this with a bounded
        read. See ``StreamBase.head``.
        """
        # _stream is set by each concrete subclass (see the class docstring), as for
        # the other delegating methods here.
        stream = self._stream  # pyright: ignore[reportAttributeAccessIssue]
        return stream.head(n, columns=columns, all_info=all_info).for_stream(self)

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
