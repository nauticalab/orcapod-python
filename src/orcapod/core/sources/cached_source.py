from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import Config
from orcapod.core.sources.base import RootSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class CachedSource(RootSource):
    """DB-backed wrapper around a ``RootSource`` that caches every packet.

    Implements ``StreamProtocol`` transparently so downstream consumers
    are unaware of caching.  Cache table is scoped to the source's
    ``content_hash()`` — each unique source gets its own table.

    Behavior:
        - Cache is **always on**.
        - On first access, live source data is stored in the cache table
          (deduped by per-row content hash).
        - Returns the union of all cached data (cumulative across runs).

    Semantic guarantee:
        The cache is a correct cumulative record.  The union of cache + live
        packets is the full set of data ever available from that source.

    Example::

        source = ArrowTableSource(table, tag_columns=["id"])
        cached = CachedSource(source, cache_database=db)
        # or equivalently:
        cached = source.cached(cache_database=db)
    """

    HASH_COLUMN_NAME = "_record_hash"

    def __init__(
        self,
        source: RootSource,
        cache_database: ArrowDatabaseProtocol,
        cache_path_prefix: tuple[str, ...] = (),
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: Config | None = None,
    ) -> None:
        if data_context is None:
            data_context = source.data_context_key
        if config is None:
            config = source.orcapod_config
        super().__init__(
            source_id=source.source_id,
            label=label,
            data_context=data_context,
            config=config,
        )
        self._source = source
        self._cache_database = cache_database
        self._cache_path_prefix = cache_path_prefix
        self._cached_stream: ArrowTableStream | None = None

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self) -> dict[str, Any]:
        """Serialize this CachedSource configuration to a JSON-compatible dict.

        Returns:
            Dict containing the inner source config, cache database config, and
            cache path prefix.
        """
        return {
            "source_type": "cached",
            "inner_source": self._source.to_config(),
            "cache_database": self._cache_database.to_config(),
            "cache_path_prefix": list(self._cache_path_prefix),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "CachedSource":
        """Reconstruct a CachedSource from a config dict.

        Args:
            config: Dict as produced by :meth:`to_config`.

        Returns:
            A new CachedSource constructed from the config.
        """
        from orcapod.pipeline.serialization import (
            resolve_database_from_config,
            resolve_source_from_config,
        )

        inner_source = resolve_source_from_config(config["inner_source"])
        cache_db = resolve_database_from_config(config["cache_database"])
        return cls(
            source=inner_source,
            cache_database=cache_db,
            cache_path_prefix=tuple(config.get("cache_path_prefix", ())),
        )

    # -------------------------------------------------------------------------
    # Identity — delegate to wrapped source
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return self._source.identity_structure()

    def resolve_field(self, record_id: str, field_name: str) -> Any:
        return self._source.resolve_field(record_id, field_name)

    # -------------------------------------------------------------------------
    # Cache path — scoped to source's content hash
    # -------------------------------------------------------------------------

    @property
    def cache_path(self) -> tuple[str, ...]:
        """Cache table path, scoped to the source's content hash."""
        return self._cache_path_prefix + (
            "source",
            f"node:{self._source.content_hash().to_string()}",
        )

    # -------------------------------------------------------------------------
    # Stream interface — delegate schema, materialize with cache
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._source.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._source.keys(columns=columns, all_info=all_info)

    def _build_merged_stream(self) -> ArrowTableStream:
        """
        Run the live source, store new rows in the cache, load all cached
        rows, and return the merged result as an ArrowTableStream.
        """
        # Get live source table with source info and system tags
        live_table = self._source.as_table(
            columns={"source": True, "system_tags": True}
        )

        # Compute per-row record hashes for dedup: hash(full row)
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes: list[str] = []
        for batch in live_table.to_batches():
            for i in range(len(batch)):
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_hex()
                )

        # Store in DB with hash as record ID (skip_duplicates deduplicates)
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

        # Build stream from merged table
        tag_keys = self._source.keys()[0]
        return ArrowTableStream(all_records, tag_columns=tag_keys)

    @property
    def is_stale(self) -> bool:
        """True if the wrapped source has been modified since the last build.

        Overrides ``StreamBase.is_stale`` because CachedSource is a RootSource
        (no upstreams/producer) yet still depends on the wrapped ``_source``.
        """
        own_time = self.last_modified
        if own_time is None:
            return True
        src_time = self._source.last_modified
        return src_time is None or src_time > own_time

    def _ensure_stream(self) -> None:
        """Build the merged stream on first access, or rebuild if source is stale."""
        if self._cached_stream is not None and self.is_stale:
            self._cached_stream = None
        if self._cached_stream is None:
            self._cached_stream = self._build_merged_stream()
            self._update_modified_time()

    def clear_cache(self) -> None:
        """Discard in-memory cached stream (forces rebuild on next access)."""
        self._cached_stream = None

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

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """Retrieve all stored records from the cache database."""
        return self._cache_database.get_all_records(self.cache_path)
