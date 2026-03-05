from __future__ import annotations

import logging
from collections.abc import Iterator, Sequence
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.config import Config
from orcapod.core.nodes import SourceNode
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.protocols.core_protocols import PacketProtocol, StreamProtocol, TagProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.types import ColumnConfig
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class PersistentSourceNode(SourceNode):
    """
    DB-backed wrapper around any stream, used by ``Pipeline.compile()``
    to cache leaf stream data.

    Extends ``SourceNode`` (which delegates identity/schema to the wrapped
    stream) and adds:

    - Materialization of the stream's output into a cache database
    - Per-row deduplication via content hash
    - Cached ``ArrowTableStream`` for downstream consumption

    Cache path structure::

        cache_path_prefix / source / node:{content_hash}
    """

    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        stream: StreamProtocol,
        cache_database: ArrowDatabaseProtocol,
        cache_path_prefix: tuple[str, ...] = (),
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        super().__init__(
            stream=stream,
            label=label,
            data_context=data_context,
            config=config,
        )
        self._cache_database = cache_database
        self._cache_path_prefix = cache_path_prefix
        self._cached_stream: ArrowTableStream | None = None

    # -------------------------------------------------------------------------
    # Cache path
    # -------------------------------------------------------------------------

    @property
    def cache_path(self) -> tuple[str, ...]:
        """Cache table path, scoped to the wrapped stream's content hash."""
        return self._cache_path_prefix + (
            "source",
            f"node:{self.stream.content_hash().to_string()}",
        )

    # -------------------------------------------------------------------------
    # Caching logic
    # -------------------------------------------------------------------------

    def _build_cached_stream(self) -> ArrowTableStream:
        """
        Materialize the wrapped stream, store rows in the cache DB
        (deduped by per-row hash), and return the cached table as an
        ``ArrowTableStream``.
        """
        live_table = self.stream.as_table(columns={"source": True, "system_tags": True})

        # Per-row content hashes for dedup
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes: list[str] = []
        for batch in live_table.to_batches():
            for i in range(len(batch)):
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_hex()
                )

        live_with_hash = live_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_string()),
        )

        self._cache_database.add_records(
            self.cache_path,
            live_with_hash,
            record_id_column=self.HASH_COLUMN_NAME,
            skip_duplicates=True,
        )
        self._cache_database.flush()

        # Load all cached records (union of current + prior runs)
        all_records = self._cache_database.get_all_records(self.cache_path)
        assert all_records is not None, (
            "Cache should contain records after storing live data."
        )

        tag_keys = self.stream.keys()[0]
        return ArrowTableStream(all_records, tag_columns=tag_keys)

    def _ensure_stream(self) -> None:
        """Build the cached stream on first access."""
        if self._cached_stream is None:
            self._cached_stream = self._build_cached_stream()
            self._update_modified_time()

    # -------------------------------------------------------------------------
    # Stream interface overrides
    # -------------------------------------------------------------------------

    def run(self) -> None:
        """Eagerly populate the cache with live stream data."""
        self._ensure_stream()

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
        self._ensure_stream()
        assert self._cached_stream is not None
        return self._cached_stream.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        self._ensure_stream()
        assert self._cached_stream is not None
        return self._cached_stream.as_table(columns=columns, all_info=all_info)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Materialize to cache DB, then push cached rows to the output channel."""
        try:
            self._ensure_stream()
            assert self._cached_stream is not None
            for tag, packet in self._cached_stream.iter_packets():
                await output.send((tag, packet))
        finally:
            await output.close()

    def get_all_records(self) -> "pa.Table | None":
        """Retrieve all stored records from the cache database."""
        return self._cache_database.get_all_records(self.cache_path)
