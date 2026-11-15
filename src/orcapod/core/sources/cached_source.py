from __future__ import annotations


import logging
from datetime import datetime
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.sources.base import RootSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import (
    check_head_size,
    schema_requires_io,
    typed_zero_row_table,
)
from orcapod.core.streams.view import (
    ViewStatus,
    StreamView,
    head_of,
)
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
        # The wrapped source's modified time when the cache last took in its rows, or
        # last found it had them all (ITL-1102): a check compares it with the wrapped
        # source's time, not with what the wrapped source's own check returned.
        self._source_seen: datetime | None = self._source_modified_time()
        # The record hashes of the rows the merged stream presents, recorded when it is
        # built; ``None`` before a build. A cache shared with another source can hold
        # rows this source has not presented yet (ITL-1102).
        self._presented_hashes: set[bytes] | None = None

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

    def _live_records(self) -> pa.Table:
        """Read the wrapped source's rows, each with its record hash in front.

        Raises if the source cannot provide data (e.g. an unbound ``SourceProxy``).
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

        return live_table.add_column(
            0,
            self.HASH_COLUMN_NAME,
            pa.array(record_hashes, type=pa.large_binary()),
        )

    def _ingest_live_data(self) -> None:
        """Fetch live data from the source and store new rows in the cache.

        Raises if the source cannot provide data (e.g. an unbound
        ``SourceProxy``).
        """
        seen = self._source_modified_time()
        live_with_hash = self._live_records()
        # Store in DB with hash as record ID (skip_duplicates deduplicates).
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
        self._source_seen = seen

    def _source_modified_time(self) -> datetime | None:
        """The wrapped source's modified time, or ``None`` if it has none."""
        return getattr(self._source, "last_modified", None)

    def _cached_record_hashes(self) -> set[bytes]:
        """The record hashes of the rows cached under the current data context."""
        self._account_for_stale_tables()
        records = self._cache_database.get_all_records(
            self.cache_path, record_id_column=self.HASH_COLUMN_NAME
        )
        if records is None:
            return set()
        records = keep_rows_of_data_context(records, self.data_context_key)
        return {
            h
            for h in records.column(self.HASH_COLUMN_NAME).to_pylist()
            if h is not None
        }

    @override
    def check_for_new_data(self) -> bool:
        """Check the wrapped source; move only if the next read shows new rows (ITL-1102).

        The wrapped source is checked first. When its modified time moved since the
        cache last took in its rows (by this check, or by a check made through another
        source that wraps it), its rows are hashed and compared with the rows this
        source presents (recorded when it built its merged rows; before a build, the
        rows already cached: a read of the cache; nothing is written). The cache keeps
        every row it ever held, so rows that were removed from the wrapped source change
        nothing. When this source would present rows it has not (another source sharing
        the cache may already have written them), the merged rows held in memory are
        dropped (the next read writes the new rows to the cache and merges them) and the
        modified time moves. A comparison that raises leaves everything as it was, so
        the next check compares again.

        The invariant this keeps: the modified time moves exactly when the next read
        would present a different set of rows. That read presents the rows cached
        (every row any source sharing the cache wrote, including rows since deleted from
        the wrapped source) and the live rows, so a row another source wrote to a shared
        cache moves this source too, and a row deleted from the wrapped source moves
        nothing.

        The check guarantees nothing for two cached sources that share one cache (the
        same database and ``cache_path``, or the same table): give each cached source
        its own cache. A change missed there means a run does not compute the new rows;
        nothing wrong is stored (ITL-1143).

        Returns:
            ``True`` if this source will present new rows at the next read.
        """
        if not isinstance(self._source, RootSource):
            return False
        _ = self._source.check_for_new_data()
        seen = self._source_modified_time()
        if seen == self._source_seen:
            return False
        live = self._live_records().column(self.HASH_COLUMN_NAME).to_pylist()
        # The next read presents the cache's rows and the live rows together (the
        # cache keeps every row any source sharing it wrote).
        cached = self._cached_record_hashes()
        presented = (
            self._presented_hashes if self._presented_hashes is not None else cached
        )
        gains = not ({h for h in live if h is not None} | cached) <= presented
        self._source_seen = seen
        if not gains:
            return False
        self._cached_stream = None
        self._update_modified_time()
        return True

    def _read_cached_records(self) -> "pa.Table | None":
        """Read the cached rows written under the current data context.

        Returns:
            The cached rows without the record hash and ``__data_context`` columns,
            or ``None`` when the cache table does not exist.
        """
        records = self._read_cached_records_with_hashes()
        if records is None:
            return None
        return records.drop([self.HASH_COLUMN_NAME, constants.DATA_CONTEXT_COL])

    def _read_cached_records_with_hashes(self) -> "pa.Table | None":
        """Read the cached rows written under the current data context, as stored.

        Every read of the whole cache table goes through here. Rows of another data
        context stay in the table untouched, and a cache at the older layout is not
        read (ITL-944).

        Returns:
            The cached rows with their record hash and ``__data_context`` columns, or
            ``None`` when the cache table does not exist.
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
        return records

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

        stored = self._read_cached_records_with_hashes()
        if stored is None:
            self._presented_hashes = set()
            all_records = self._empty_table()
        else:
            self._presented_hashes = {
                h
                for h in stored.column(self.HASH_COLUMN_NAME).to_pylist()
                if h is not None
            }
            all_records = stored.drop(
                [self.HASH_COLUMN_NAME, constants.DATA_CONTEXT_COL]
            )

        tag_keys = self._source.keys()[0]
        # Cached rows are this source's: give them its data context, not the default.
        return ArrowTableStream(
            all_records, tag_columns=tag_keys, data_context=self.data_context
        )

    def _empty_table(self) -> pa.Table:
        """Build an empty Arrow table matching the source's output schema.

        The wrapped source's zero-row table when it gives one (its Arrow types and
        system-tag columns, as ``head()`` shows them; ITL-912), otherwise one built
        from its Python output schema.
        """
        typed = self._typed_zero_row_table()
        if typed is not None:
            return typed
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

    def _typed_zero_row_table(self) -> "pa.Table | None":
        """The wrapped source's zero-row table: the cache stores that source's rows.

        See ``typed_zero_row_table``; neither the cache nor the source is read.
        """
        return typed_zero_row_table(self._source)

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading the schema with ``config`` would poll: as for the wrapped source."""
        return schema_requires_io(self._source, config)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows without reading or writing the cache.

        Once the source has been used, its merged stream is in memory and this is a slice
        of it. Before that its rows are not known: they are the cache's earlier rows
        merged with the wrapped source's, and reading them writes the cache. The head
        then has the wrapped source's columns, no rows and ``NOT_COMPUTED`` (or the
        wrapped source's own reason when it has none to give).
        """
        check_head_size(n)
        if self._cached_stream is not None and not self.is_stale:
            return head_of(
                self._cached_stream.head(n, columns=columns, all_info=all_info), self
            )
        wrapped = head_of(
            self._source.head(0, columns=columns, all_info=all_info), self
        )
        if wrapped.status is not ViewStatus.ROWS:
            return wrapped
        return StreamView.without_rows(
            self,
            ViewStatus.NOT_COMPUTED,
            wrapped.table,
            tag_columns=wrapped.tag_columns,
            note="not read yet; run() reads and updates the cache",
        )

    def get_all_records(
        self,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table | None":
        """Retrieve the cached records that the current Arrow hasher wrote."""
        return self._read_cached_records()
