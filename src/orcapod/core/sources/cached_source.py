from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.sources.base import RootSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.contexts.stored import (
    keep_rows_of_data_context,
    require_same_data_context,
    account_for_stale_tables,
    find_stale_tables,
    report_rows_of_other_data_contexts,
    with_data_context,
)
from orcapod.protocols.core_protocols import DataProtocol, SourceProtocol, TagProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import SOURCE_DB_SCHEMA_VERSION, constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class CachedSource(RootSource):
    """DB-backed wrapper around a source that caches every data.

    Accepts any ``SourceProtocol`` implementation as the inner source.
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
        data is the full set of data ever available from that source.

    Example::

        source = ArrowTableSource(table, tag_columns=["id"])
        cached = CachedSource(source, cache_database=db)
        # or equivalently:
        cached = source.cached(cache_database=db)
    """

    HASH_COLUMN_NAME = "_record_hash"

    # Set once a read has reported the cached rows it skipped.
    _skipped_rows_reported: bool = False
    # Set once a cache table found at the older layout has been accounted for.
    _stale_tables_checked: bool = False

    def __init__(
        self,
        source: SourceProtocol,
        cache_database: ArrowDatabaseProtocol,
        cache_path_prefix: tuple[str, ...] = (),
        cache_path: tuple[str, ...] | None = None,
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | contexts.DataContext | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        if data_context is None:
            data_context = source.data_context_key
        if source_id is None:
            source_id = source.source_id
        super().__init__(
            source_id=source_id,
            label=label,
            data_context=data_context,
            config=config,
        )
        self._source: SourceProtocol = source
        self._cache_database = cache_database
        self._cache_path_prefix = cache_path_prefix
        self._explicit_cache_path = cache_path
        self._cached_stream: ArrowTableStream | None = None

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(
        self, db_registry: DatabaseRegistryProtocol | None = None
    ) -> dict[str, Any]:
        """Serialize this CachedSource configuration to a JSON-compatible dict.

        Args:
            db_registry: Optional registry. When provided, the cache database
                config is registered (deduplicated) and replaced by its key
                string.  When None, the full database config is embedded inline.

        Returns:
            Dict containing the inner source config, cache database config,
            cache path prefix, and resolved cache path (for cache-only loading).
        """
        db_config = self._cache_database.to_config()
        if db_registry is not None:
            cache_db_ref = db_registry.register(db_config)
        else:
            cache_db_ref = db_config

        # All sources accept db_registry — always forward it so nested
        # CachedSources can also deduplicate their cache database configs.
        inner_source_config = self._source.to_config(db_registry=db_registry)

        return {
            "source_type": "cached",
            "inner_source": inner_source_config,
            "cache_database": cache_db_ref,
            "cache_path_prefix": list(self._cache_path_prefix),
            # The path without its layout component, as ``cache_path`` takes it.
            "cache_path": list(self._unversioned_cache_path),
            "source_id": self.source_id,
            # Note: identity fields (content_hash, pipeline_hash, etc.) intentionally omitted
        }

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        db_registry: DatabaseRegistryProtocol | None = None,
    ) -> CachedSource:
        """Reconstruct a CachedSource from a config dict.

        If the inner source cannot be resolved (e.g. it requires live data
        that is unavailable), ``resolve_source_from_config`` returns a
        ``SourceProxy`` preserving the original source's identity.  The
        CachedSource can still serve data from its cache database.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional registry. When ``cache_database`` is a
                string key, it is resolved via ``db_registry.resolve()``.
                When None and the cache_database value is a string key,
                a ValueError is raised.

        Returns:
            A new CachedSource constructed from the config.
        """
        from orcapod.pipeline.serialization import (
            resolve_database_from_config,
            resolve_source_from_config,
        )

        cache_db_ref = config["cache_database"]
        if isinstance(cache_db_ref, str):
            if db_registry is None:
                raise ValueError(
                    f"cache_database is a registry key {cache_db_ref!r} but no "
                    "db_registry was provided."
                )
            cache_db = resolve_database_from_config(db_registry.resolve(cache_db_ref))
        else:
            cache_db = resolve_database_from_config(cache_db_ref)

        # All sources accept db_registry — always forward it so nested
        # CachedSources can resolve their own cache database references.
        inner_source = resolve_source_from_config(
            config["inner_source"], fallback_to_proxy=True, db_registry=db_registry
        )

        return cls(
            source=inner_source,
            cache_database=cache_db,
            cache_path_prefix=tuple(config.get("cache_path_prefix", ())),
            cache_path=tuple(config["cache_path"]) if "cache_path" in config else None,
            source_id=config.get("source_id"),
        )

    # -------------------------------------------------------------------------
    # Identity — delegate to wrapped source
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        return self._source.identity_structure()

    # -------------------------------------------------------------------------
    # Cache path — scoped to source's content hash
    # -------------------------------------------------------------------------

    @property
    def _unversioned_cache_path(self) -> tuple[str, ...]:
        """The cache path without its layout component.

        This is where Orcapod up to v0.2 stored the cache table (the old layout).
        """
        if self._explicit_cache_path is not None:
            return self._explicit_cache_path
        return self._cache_path_prefix + (
            "source",
            f"node:{self._source.content_hash().to_hex()}",
        )

    @property
    def cache_path(self) -> tuple[str, ...]:
        """Cache table path: scoped to the source's content hash, then the layout version.

        The layout version (``sdb_v1``) is always the last component, also when an
        explicit ``cache_path`` was given.
        """
        return self._unversioned_cache_path + (SOURCE_DB_SCHEMA_VERSION,)

    def _account_for_stale_tables(self) -> None:
        """Report, once per source object, a cache table found at the older layout.

        A cache written by an earlier version of Orcapod holds no ``__data_context``
        column. Its rows are not read and are left unchanged; their number is
        reported (ITL-944). Without an explicit ``cache_path`` the path holds a hash
        that can differ between versions, so older caches are also looked for
        anywhere under ``<cache_path_prefix>/source`` (ITL-954): those are caches of
        any source stored there, and they are reported without a number of rows.
        """
        if self._stale_tables_checked:
            return
        self._stale_tables_checked = True
        account_for_stale_tables(
            find_stale_tables(
                self._cache_database,
                (self._unversioned_cache_path,),
                name_path=None
                if self._explicit_cache_path is not None
                else self._cache_path_prefix + ("source",),
            ),
            delivers_side_effects=False,
            current_context=self.data_context_key,
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

    def _ingest_live_data(self) -> None:
        """Fetch live data from the source and store new rows in the cache.

        Raises if the source cannot provide data (e.g. an unbound
        ``SourceProxy``).
        """
        require_same_data_context(self._source.data_context_key, self.data_context_key)
        live_table = self._source.as_table(
            columns={"source": True, "system_tags": True}
        )

        # Compute per-row record hashes for dedup: hash the full row including
        # all system tag columns.  The record_id system tag encodes row origin
        # (source_id + provenance token), so two rows that are byte-identical in
        # their data columns but originate from different positions in the source
        # will have different record_ids and must be stored as distinct entries.
        arrow_hasher = self.data_context.arrow_hasher
        record_hashes: list[bytes] = []
        for batch in live_table.to_batches():
            for i in range(len(batch)):
                # A hash stored as a value carries its label: b"<label>:<digest>".
                record_hashes.append(
                    arrow_hasher.hash_table(batch.slice(i, 1)).to_prefixed_digest()
                )

        # Store in DB with hash as record ID (skip_duplicates deduplicates)
        live_with_hash = live_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_binary()),
        )
        # The data context column is appended after the record hashes are computed:
        # it names the generation of hashing that produced them and is not hashed.
        self._account_for_stale_tables()
        self._cache_database.add_records(
            self.cache_path,
            with_data_context(live_with_hash, self.data_context_key),
            record_id_column=self.HASH_COLUMN_NAME,
            skip_duplicates=True,
        )
        self._cache_database.flush()

    def _read_cached_records(self) -> "pa.Table | None":
        """Read the cached rows written under the current data context.

        Every read of the whole cache table goes through here. Rows of another data
        context stay in the table untouched, and a cache at the older layout is not
        read (ITL-944).

        Returns:
            The cached rows without the record hash and ``__data_context`` columns,
            or ``None`` when the cache table does not exist.
        """
        self._account_for_stale_tables()
        records = self._cache_database.get_all_records(
            self.cache_path, record_id_column=self.HASH_COLUMN_NAME
        )
        if records is None:
            return None
        stored_rows = records.num_rows
        records = keep_rows_of_data_context(records, self.data_context_key)
        if records.num_rows < stored_rows and not self._skipped_rows_reported:
            self._skipped_rows_reported = True
            report_rows_of_other_data_contexts(
                self._cache_database.base_path + self.cache_path,
                stored_rows - records.num_rows,
            )
        return records.drop([self.HASH_COLUMN_NAME, constants.DATA_CONTEXT_COL])

    def _build_merged_stream(self) -> ArrowTableStream:
        """Ingest live data (if available), then return all cached records.

        If the inner source cannot provide data (e.g. an unbound
        ``SourceProxy``), the method falls back to returning whatever is
        already stored in the cache database.  If the cache is empty, an
        empty stream is returned.
        """
        try:
            self._ingest_live_data()
        except NotImplementedError:
            logger.info(
                "Inner source %r cannot provide data; serving from cache only.",
                self._source.source_id,
            )

        all_records = self._read_cached_records()
        if all_records is None:
            all_records = self._empty_table()

        tag_keys = self._source.keys()[0]
        # Cached rows are this source's: give them its data context, not the default.
        return ArrowTableStream(
            all_records, tag_columns=tag_keys, data_context=self.data_context
        )

    def _empty_table(self) -> pa.Table:
        """Build an empty Arrow table matching the source's output schema."""
        tag_schema, data_schema = self._source.output_schema()
        merged = dict(tag_schema)
        merged.update(data_schema)
        type_converter = self.data_context.type_converter
        arrow_schema = type_converter.python_schema_to_arrow_schema(merged)
        return pa.Table.from_pylist([], schema=arrow_schema)

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

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        self._ensure_stream()
        assert self._cached_stream is not None
        return self._cached_stream.iter_data()

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
        """Retrieve the cached records that the current Arrow hasher wrote."""
        return self._read_cached_records()
