"""SpiralDBTableSource — a read-only RootSource that streams a SpiralDB table.

The source is built from the table's metadata alone (schema, key columns, SpiralDB's
table id): no row is read until one is asked for. Rows then arrive in batches, so a
read holds one of SpiralDB's batches at a time whatever the table's size. The table's
primary-key (key-schema) columns are the tag columns by default.

Requires the ``spiraldb`` optional extra: ``pip install orcapod[spiraldb]``.
Authentication is handled externally — run ``spiral login`` once; pyspiral keeps
the credentials in its per-user application directory
(``~/Library/Application Support/pyspiral/`` on macOS, ``~/.config/pyspiral/`` on Linux).

Example::

    # Default dataset, PK columns become tag columns automatically
    source = SpiralDBTableSource("my-project-123456", "spike_event", limit=1000)

    # Only the rows of one unit
    source = SpiralDBTableSource(
        "my-project-123456", "spike_event", where={"unit_id": 106}
    )

    # The spike time is a key column (a tag); also offer it as data
    source = SpiralDBTableSource(
        "my-project-123456", "spike_event", tags_as_data={"timestamp": "spike_time_us"}
    )

    # Explicit dataset, tag columns overridden
    source = SpiralDBTableSource(
        "my-project-123456", "spike_event", dataset="prod", tag_columns=["unit_id"]
    )
"""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
from collections.abc import AsyncIterator, Collection, Generator, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, NamedTuple

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.derived_source import validate_tags_as_data
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.base import check_head_size, head_column_config
from orcapod.core.streams.head import HeadStatus, StreamHead
from orcapod.databases.spiraldb_connector import SpiralDBConnector
from orcapod.errors import InputValidationError, SourceTooLargeError
from orcapod.types import ColumnConfig
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.utils.schema_utils import _normalize_column_list

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa

    from orcapod import contexts
    from orcapod.config import OrcapodConfig
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

DEFAULT_MAX_ROWS = 100_000
DEFAULT_BATCH_ROWS = 8_192
# The most rows a notebook display of the source reads.
PREVIEW_ROWS = 10

# How a datetime, date or bytes value of ``where`` is written in a saved config.
_DATETIME_KEY = "datetime"
_DATE_KEY = "date"
_BYTES_KEY = "bytes"


def _locationto_config(where: Mapping[str, object]) -> dict[str, Any]:
    """Return ``where`` with its date, datetime and bytes values in a JSON form."""
    out: dict[str, Any] = {}
    for column, value in where.items():
        if isinstance(value, datetime.datetime):
            out[column] = {_DATETIME_KEY: value.isoformat()}
        elif isinstance(value, datetime.date):
            out[column] = {_DATE_KEY: value.isoformat()}
        elif isinstance(value, bytes):
            out[column] = {_BYTES_KEY: value.hex()}
        else:
            out[column] = value
    return out


def _locationfrom_config(saved: Mapping[str, Any]) -> dict[str, object]:
    """Return the ``where`` mapping that ``_locationto_config`` wrote."""
    out: dict[str, object] = {}
    for column, value in saved.items():
        if isinstance(value, dict) and set(value) == {_DATETIME_KEY}:
            out[column] = datetime.datetime.fromisoformat(value[_DATETIME_KEY])
        elif isinstance(value, dict) and set(value) == {_DATE_KEY}:
            out[column] = datetime.date.fromisoformat(value[_DATE_KEY])
        elif isinstance(value, dict) and set(value) == {_BYTES_KEY}:
            out[column] = bytes.fromhex(value[_BYTES_KEY])
        else:
            out[column] = value
    return out


def _is_positive_int(value: object) -> bool:
    """Return whether ``value`` is an integer of at least 1 (``True`` is not)."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


class _Head(NamedTuple):
    """The first rows of a source, and what is known of how many rows it reads.

    Attributes:
        rows: The rows read; zero rows (the source's columns) when ``rows_read`` is
            false.
        rows_read: Whether rows were read at all.
        total_rows: The rows the source reads or, as an estimate, the table's recorded
            row count.
        total_is_estimate: Whether ``total_rows`` is the recorded row count rather than
            an exact count.
    """

    rows: "pa.Table"
    rows_read: bool
    total_rows: int
    total_is_estimate: bool


def _schema_difference(found: "pa.Schema", expected: "pa.Schema") -> str:
    """Describe how a scanned batch's columns differ from the ones the source expects."""
    if found.names != expected.names:
        return (
            f"a scan returned columns {found.names} but the source was built with "
            f"{expected.names}"
        )
    for got, want in zip(found, expected, strict=True):
        if not got.equals(want, check_metadata=True):
            return (
                f"a scan returned column {got.name!r} as {got} with metadata "
                f"{got.metadata} but the source was built with {want} with metadata "
                f"{want.metadata}"
            )
    return (
        f"a scan returned schema metadata {found.metadata} but the source was built "
        f"with {expected.metadata}"
    )


class SpiralDBTableSource(RootSource):
    """A read-only Source over a table in a SpiralDB dataset, read in batches.

    Building the source reads the table's metadata only: that the table exists, its
    key columns, its schema (with the table's nullability record applied) and
    SpiralDB's own id of the table. No row is read. ``output_schema``, ``keys``,
    ``content_hash``, ``pipeline_hash`` and ``to_config`` never read a row either.

    Rows are read when asked for (``iter_data``, ``async_iter_data``, ``as_table`` and
    the dataframe methods). Each read is one scan of the table in key order. SpiralDB
    sends it in batches whose sizes follow its stored fragments (they cannot be
    chosen); the source hands each on in slices of at most ``batch_rows`` rows, so
    iterating holds one SpiralDB batch, and the slice being processed, at a time.
    ``as_table`` and the dataframe methods hold every row they return. Nothing is
    cached: every read scans again.

    Display. ``head(n)`` and the notebook display (its ``head()``) read only the first
    ``n`` rows (``PREVIEW_ROWS``, 10, by default), in one scan to which ``max_rows`` does
    not apply, and the header says how many rows the table has.

    Identity. The source is identified by where the table is, never by its rows. The
    default ``source_id`` is ``<project_id>/<dataset>.<table_name>@<table id>``, where
    the table id is SpiralDB's own: a table dropped and created again under the same
    name is a different source, and a renamed table gets a new source id too.
    ``where``, ``limit`` and ``asof`` choose which rows are read and are not part of
    the source's identity. ``tags_as_data`` is: it decides what a data column holds,
    so its sorted items enter the content hash (when it is given), and its copies
    are columns of the schema, so they enter both the content and the pipeline hash.
    The source id and every row's record ID do not depend on it.

    Key columns as data. A function pod never sees tags, so a key it needs (a
    spike's time) is offered as data too with ``tags_as_data={"timestamp":
    "spike_time_us"}``. The tag keeps its name; the copy is a data column of the
    given name, after the table's own data columns in mapping order, with the key
    column's Arrow type and field metadata, non-nullable (a key is never null). It is
    copied in Arrow as each batch is read, and its source-info value has the form of
    every data column's, ``<source id>::<row token>::<copy name>``.

    Record identity. A row is identified by its primary key: the row's token is the
    data context's Arrow hash of its key columns (in key order, as non-nullable
    fields), written with its algorithm label. A row therefore has the same record ID
    whichever read returned it, and keeps it when other rows are added.

    Args:
        project_id: SpiralDB project identifier (e.g. ``"my-project-123456"``).
        table_name: Name of the SpiralDB table to expose as a source.
        dataset: Dataset within the project. Defaults to ``"default"``.
        tag_columns: Columns to use as tag columns. A bare string is accepted as a
            single column name. If ``None`` (default), the table's primary-key
            (key-schema) columns are used.
        system_tag_columns: Additional system-level tag columns.
        where: Column name to value. Only rows in which every named column equals its
            value are read; SpiralDB applies this before any row is transferred.
        limit: Read at most this many rows: the first in key order (after ``where``).
        asof: Read the table as of this version (microseconds, as SpiralDB's
            transaction timestamps). If ``None`` (default), the version the table has
            when the source is built is used for every read of this source object.
        max_rows: The most rows one read may return (default 100,000); ``None``
            removes the cap. A read that would return more raises
            ``SourceTooLargeError``. A pipeline job keeps every row it processes in
            memory, about 5 KB per row and node.
        batch_rows: The most rows handed on at once while iterating (default 8,192).
            A SpiralDB batch larger than this is held while its slices are handed on.
        source_id: Canonical source name for the registry and provenance tokens.
            Defaults to ``"<project_id>/<dataset>.<table_name>@<table id>"``.
        label: Human-readable label for this source node.
        data_context: Data context governing type conversion and hashing.
        config: OrcaPod configuration.
        overrides: Optional pyspiral client config overrides passed through to
            ``SpiralDBConnector``, e.g. ``{"server.url": "http://api.spiraldb.dev"}``.
        tags_as_data: Maps a key column that is a tag column of this source to the
            name of a data column that receives a copy of it, as in
            ``DerivedSource``. ``None`` (default) or an empty mapping copies nothing.

    Raises:
        ValueError: If the table is not found, has no key schema, a tag column or a
            ``where`` column is not in the table, or ``limit``, ``max_rows`` or
            ``batch_rows`` is not a positive integer.
        InputValidationError: If ``tags_as_data`` names a column that is not a tag
            column or not a key column, or a new name that is already a column, is
            given twice, or is reserved for system columns.
    """

    # Whether a notebook display reads the first rows. False shows the schema and the
    # table's recorded row count only, with no scan (kept for the ITL-991 ruling on what
    # a preview may read).
    _preview_reads_rows: bool = True

    def __init__(
        self,
        project_id: str,
        table_name: str,
        dataset: str = "default",
        tag_columns: str | Collection[str] | None = None,
        system_tag_columns: Collection[str] = (),
        where: Mapping[str, object] | None = None,
        limit: int | None = None,
        asof: int | None = None,
        max_rows: int | None = DEFAULT_MAX_ROWS,
        batch_rows: int = DEFAULT_BATCH_ROWS,
        source_id: str | None = None,
        label: str | None = None,
        data_context: "str | contexts.DataContext | None" = None,
        config: "OrcapodConfig | None" = None,
        overrides: dict[str, str] | None = None,
        tags_as_data: Mapping[str, str] | None = None,
    ) -> None:
        for name, value in (("limit", limit), ("max_rows", max_rows)):
            if value is not None and not _is_positive_int(value):
                raise ValueError(
                    f"{name} must be a positive integer or None, got {value!r}"
                )
        if not _is_positive_int(batch_rows):
            raise ValueError(
                f"batch_rows must be a positive integer, got {batch_rows!r}"
            )

        super().__init__(
            source_id=source_id, label=label, data_context=data_context, config=config
        )
        self._project_id = project_id
        self._dataset = dataset
        self._table_name = table_name
        self._overrides = overrides
        self._where: dict[str, object] = dict(where) if where else {}
        self._limit = limit
        self._max_rows = max_rows
        self._batch_rows = batch_rows
        self._given_asof = asof
        self._tags_as_data: dict[str, str] = dict(tags_as_data) if tags_as_data else {}
        location = f"SpiralDB project {project_id!r}, dataset {dataset!r}"

        with self._connector() as connector:
            if table_name not in connector.get_table_names():
                raise ValueError(
                    f"{location}: Table {table_name!r} not found in database."
                )
            self._key_columns: tuple[str, ...] = tuple(
                connector.get_pk_columns(table_name)
            )
            if not self._key_columns:
                raise ValueError(
                    f"{location}: Table {table_name!r} has no primary key columns, so "
                    "its rows cannot be identified."
                )
            self._table_id = connector.get_spiral_table_id(table_name)
            schema = connector.get_arrow_schema(table_name)
            # One version for every read of this source object. A given version is
            # used as it is; a table that was never written has no version to pin.
            self._asof = (
                asof
                if asof is not None
                else (connector.get_table_version(table_name) or None)
            )

        resolved_tags = (
            list(self._key_columns)
            if tag_columns is None
            else _normalize_column_list(tag_columns)
        )
        unknown = [c for c in self._where if c not in schema.names]
        if unknown:
            raise ValueError(
                f"{location}: where names columns that table {table_name!r} does not "
                f"have: {unknown}. Columns: {schema.names}"
            )
        if self._source_id is None:
            self._source_id = f"{project_id}/{dataset}.{table_name}@{self._table_id}"

        # A zero-row stream with the table's columns answers every schema question.
        self._builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        try:
            template = self._builder.build(
                schema.empty_table(),
                tag_columns=resolved_tags,
                source_id=self._source_id,
                system_tag_columns=system_tag_columns,
            )
        except ValueError as exc:
            raise ValueError(f"{location}, table {table_name!r}: {exc}") from exc
        self._table_schema: pa.Schema = arrow_utils.drop_system_columns(
            schema.empty_table()
        ).schema
        if self._tags_as_data:
            try:
                self._check_tags_as_data(template.stream.keys())
            except InputValidationError as exc:
                raise InputValidationError(
                    f"{location}, table {table_name!r}: {exc}"
                ) from exc
            # The copies are columns of the schema: build the template with them.
            template = self._builder.build(
                self._with_tag_copies(self._table_schema.empty_table()),
                tag_columns=template.tag_columns,
                source_id=self._source_id,
                system_tag_columns=template.system_tag_columns,
            )
        self._stream = template.stream
        self._schema_hash = template.schema_hash
        self._tag_columns = template.tag_columns
        self._system_tag_columns = template.system_tag_columns

    def _check_tags_as_data(
        self, keys: tuple[tuple[str, ...], tuple[str, ...]]
    ) -> None:
        """Check ``tags_as_data`` against the source's tag and data columns.

        Args:
            keys: The source's tag and data columns, without the copies.

        Raises:
            InputValidationError: If the mapping fails ``DerivedSource``'s rule, or a
                tag it names is not a key column of the table.
        """
        tag_keys, data_keys = keys
        validate_tags_as_data(self._tags_as_data, tag_keys, data_keys)
        for tag in self._tags_as_data:
            if tag not in self._key_columns:
                raise InputValidationError(
                    f"tags_as_data: {tag!r} is not a key column, so it may hold a "
                    f"null. Key columns, which can be copied: {list(self._key_columns)}."
                )

    def _with_tag_copies(self, table: "pa.Table") -> "pa.Table":
        """Return ``table`` with each ``tags_as_data`` copy appended, in mapping order.

        A copy is the key column's own Arrow column under a field of the new name,
        with the key's type and field metadata and declared non-nullable (a key is
        never null; a read checks it before copying).
        """
        for tag, name in self._tags_as_data.items():
            key = table.schema.field(tag)
            table = table.append_column(
                pa.field(name, key.type, nullable=False, metadata=key.metadata),
                table.column(tag),
            )
        return table

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    @property
    def table_id(self) -> str:
        """SpiralDB's own id of the table this source reads."""
        return self._table_id

    def _restriction_identity(self) -> None:
        """Return what ``where``, ``limit`` and ``asof`` add to the source's identity.

        Nothing: a source is identified by where its table is, and a restriction
        chooses rows of that same table. This is the one place that decides it.
        """
        return None

    def identity_structure(self) -> Any:
        """Identity from the class, the schema and the source id (the table's locator).

        With ``tags_as_data`` given, its sorted items follow: two mappings can give
        the same schema (two key columns of one type copied to one name) while the
        copy holds different values. Without it the structure is the plain one.
        """
        extra: list[Any] = []
        restriction = self._restriction_identity()
        if restriction is not None:
            extra.append(restriction)
        if self._tags_as_data:
            extra.append(("tags_as_data", tuple(sorted(self._tags_as_data.items()))))
        base = super().identity_structure()
        return (*base, *extra) if extra else base

    # -------------------------------------------------------------------------
    # Reading
    # -------------------------------------------------------------------------

    @contextmanager
    def _connector(self) -> Iterator[SpiralDBConnector]:
        """Open a connector to the source's dataset and close it afterwards."""
        connector = SpiralDBConnector(
            project_id=self._project_id,
            dataset=self._dataset,
            overrides=self._overrides,
        )
        try:
            yield connector
        finally:
            connector.close()

    @property
    def _qualified(self) -> str:
        """The table as error messages name it."""
        return f"{self._project_id}/{self._dataset}.{self._table_name}"

    @property
    def row_count_estimate(self) -> int:
        """The whole table's row count as SpiralDB's manifests record it.

        The count is of the version this source reads (the one it was built at, or
        the given ``asof``). No row is read. ``where`` and ``limit`` are not taken into
        account, and the count can be above what a scan returns when rows were
        deleted or overwritten and not yet compacted.
        """
        with self._connector() as connector:
            return connector.get_row_count_estimate(self._table_name, asof=self._asof)

    def _too_large(self, found: str) -> SourceTooLargeError:
        """Return the error for a read that goes over ``max_rows``."""
        return SourceTooLargeError(
            f"SpiralDB table {self._qualified!r}: {found}, more than "
            f"max_rows={self._max_rows}. Read fewer rows with limit=<n> or "
            "where={column: value}, or raise max_rows (max_rows=None removes the "
            "cap; a pipeline job keeps every row in memory)."
        )

    def _key_tokens(self, table: "pa.Table") -> list[str]:
        """Return each row's token: the labelled Arrow hash of its primary key.

        Raises:
            ValueError: If a key column holds a null.
        """
        keys = table.select(list(self._key_columns))
        for name in self._key_columns:
            if keys.column(name).null_count:
                raise ValueError(
                    f"SpiralDB table {self._qualified!r}: key column {name!r} holds "
                    "a null, so the row cannot be identified."
                )
        # A key is never null: hash its fields as non-nullable whatever is reported,
        # so a token does not depend on the table's nullability record.
        keys = keys.cast(pa.schema([f.with_nullable(False) for f in keys.schema]))
        hasher = self.data_context.arrow_hasher
        return [
            hasher.hash_table(keys.slice(i, 1)).to_string()
            for i in range(keys.num_rows)
        ]

    def _batch_streams(self) -> Generator["ArrowTableStream", None, None]:
        """Scan the table once and yield its rows as streams of at most ``batch_rows`` rows.

        Each SpiralDB batch is held while its slices are yielded.

        Raises:
            SourceTooLargeError: If the read would return, or returns, more than
                ``max_rows`` rows.
            ValueError: If a batch's columns differ from the source's schema, or a
                key column holds a null.
        """
        return self._read_batches(limit=self._limit, max_rows=self._max_rows)

    def _read_batches(
        self, *, limit: int | None, max_rows: int | None
    ) -> Generator["ArrowTableStream", None, None]:
        """Scan the table once, reading at most ``limit`` rows, in streams of ``batch_rows``.

        ``where`` and the pinned version apply as for every read.

        Args:
            limit: Read at most this many rows, the first in key order.
            max_rows: Raise when the read would return more rows; ``None`` for no cap.

        Raises:
            SourceTooLargeError: If the read would return, or returns, more than
                ``max_rows`` rows.
            ValueError: If a batch's columns differ from the source's schema, or a
                key column holds a null.
        """
        with self._connector() as connector:
            if (
                max_rows is not None
                and not self._where
                and (limit is None or limit > max_rows)
            ):
                # Nothing but limit bounds the read, and the limit does not keep it
                # under the cap: the table's recorded row count decides, before any
                # row is read (so no row reaches a function of a job that would fail).
                # The count can be above the rows a scan returns, never below.
                recorded = connector.get_row_count_estimate(
                    self._table_name, asof=self._asof
                )
                bound = recorded if limit is None else min(limit, recorded)
                if bound > max_rows:
                    raise self._too_large(
                        f"the read may return up to {bound:,} rows: the table records "
                        f"about {recorded:,} rows (SpiralDB's count, which can include "
                        "deleted rows not yet compacted)"
                    )
            seen = 0
            for batch in connector.scan_batches(
                self._table_name,
                where=self._where or None,
                limit=limit,
                asof=self._asof,
            ):
                for start in range(0, batch.num_rows, self._batch_rows):
                    rows = arrow_utils.drop_system_columns(
                        pa.Table.from_batches([batch.slice(start, self._batch_rows)])
                    )
                    # Metadata too: field metadata is how a column's logical type is
                    # recorded, and the schema hash was computed at construction.
                    if not rows.schema.equals(self._table_schema, check_metadata=True):
                        raise ValueError(
                            f"SpiralDB table {self._qualified!r}: "
                            f"{_schema_difference(rows.schema, self._table_schema)}. "
                            "The table changed after the source was built; build the "
                            "source again to read it as it is now."
                        )
                    seen += rows.num_rows
                    if max_rows is not None and seen > max_rows:
                        raise self._too_large("the read returned more rows")
                    # Tokens first: they refuse a null key before it is copied into
                    # a non-nullable column.
                    tokens = self._key_tokens(rows)
                    yield self._builder.attach_provenance(
                        self._with_tag_copies(rows),
                        tag_columns=self._tag_columns,
                        system_tag_columns=self._system_tag_columns,
                        schema_hash=self._schema_hash,
                        source_id=self.source_id,
                        provenance_tokens=tokens,
                    )

    def iter_data(self) -> Iterator["tuple[TagProtocol, DataProtocol]"]:
        """Scan the table and yield its ``(tag, data)`` pairs, one batch held at a time."""
        for stream in self._batch_streams():
            yield from stream.iter_data()

    async def async_iter_data(
        self,
    ) -> AsyncIterator["tuple[TagProtocol, DataProtocol]"]:
        """Yield the table's ``(tag, data)`` pairs, reading each batch off the event loop."""
        streams = self._batch_streams()
        try:
            while True:
                stream = await asyncio.to_thread(next, streams, None)
                if stream is None:
                    break
                for pair in stream.iter_data():
                    yield pair
        finally:
            try:
                streams.close()
            except ValueError:
                # Cancelled while a batch was being read in the worker thread: the
                # scan cannot be closed from here and ends with that read.
                pass

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Scan the table and return its rows as one PyArrow table (all held in memory)."""
        return self._whole_table(
            self._batch_streams(), columns=columns, all_info=all_info
        )

    def _whole_table(
        self,
        streams: Iterator["ArrowTableStream"],
        *,
        columns: ColumnConfig | dict[str, Any] | None,
        all_info: bool,
    ) -> "pa.Table":
        """Return the rows of ``streams`` (batches of one read) as one table."""
        # One stream over all the rows, so that a whole-table option (sorting by
        # tags) applies across batches.
        enriched = {"source": True, "system_tags": True}
        tables = [stream.as_table(columns=enriched) for stream in streams]
        if not tables:
            return self._stream.as_table(columns=columns, all_info=all_info)
        whole = ArrowTableStream(
            table=pa.concat_tables(tables),
            tag_columns=self._tag_columns,
            system_tag_columns=self._system_tag_columns,
        )
        return whole.as_table(columns=columns, all_info=all_info)

    def _head(
        self,
        n: int,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "_Head":
        """Return the source's first ``n`` rows, read in one bounded scan, and its size.

        At most ``n`` rows are read (fewer when the source's ``limit`` is smaller), with
        ``where`` and the pinned version applied and ``max_rows`` not, so a table of any
        size can be looked at. When these are not all the rows the source reads, the
        size is the table's recorded row count (no scan), an estimate.

        Args:
            n: The most rows to read.
            columns: Column configuration, as for ``as_table``.
            all_info: Include all column groups, as for ``as_table``.
        """
        if not self._preview_reads_rows:
            schema_only = self._stream.as_table(columns=columns, all_info=all_info)
            return _Head(schema_only, False, self.row_count_estimate, True)
        head_limit = n if self._limit is None else min(n, self._limit)
        rows = self._whole_table(
            self._read_batches(limit=head_limit, max_rows=None),
            columns=columns,
            all_info=all_info,
        )
        if rows.num_rows < head_limit or head_limit == self._limit:
            return _Head(rows, True, rows.num_rows, False)
        recorded = self.row_count_estimate
        if self._limit is not None and not self._where:
            recorded = min(self._limit, recorded)
        return _Head(rows, True, recorded, True)

    def _head_note(self, head: "_Head") -> str:
        """Return the words a notebook display puts after the header: the one place."""
        shown = head.rows.num_rows
        if not head.rows_read:
            return f"rows not read; the table has about {head.total_rows:,} rows"
        if not head.total_is_estimate:
            return f"all {shown:,} row{'' if shown == 1 else 's'}"
        if self._where:
            condition = ", ".join(f"{c}={v!r}" for c, v in self._where.items())
            return (
                f"first {shown:,} matching rows (where {condition}); the table has "
                f"about {head.total_rows:,} rows"
            )
        return f"first {shown:,} rows of about {head.total_rows:,}"

    def head(
        self,
        n: int = PREVIEW_ROWS,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamHead:
        """Return the source's first ``n`` rows, read in one bounded scan.

        At most ``n`` rows are read (fewer when the source's ``limit`` is smaller), with
        ``where`` and the pinned version applied and ``max_rows`` not, so a table of any
        size can be looked at. The total is exact when these are all the rows the
        source reads, else the table's recorded row count (an estimate). The notebook
        preview is this head. See ``StreamBase.head``.
        """
        check_head_size(n)
        config = head_column_config(columns, all_info)
        head = self._head(n, columns=config)
        tag_columns = self.keys(columns=config)[0]
        if not head.rows_read:
            empty = StreamHead.without_rows(
                self, HeadStatus.NOT_COMPUTED, head.rows, tag_columns=tag_columns
            )
            return dataclasses.replace(
                empty,
                total_rows=head.total_rows,
                total_is_estimate=True,
                description=self._head_note(head),
            )
        return StreamHead.of_rows(
            self,
            head.rows,
            tag_columns=tag_columns,
            total_rows=head.total_rows,
            total_is_estimate=head.total_is_estimate,
            origin="read",
            description=self._head_note(head),
        )

    def _display_name(self) -> str:
        """The name a preview header shows: the label when one was given, else the
        table's name (the default source id also carries the table's id)."""
        return self.label if self.has_assigned_label else self._table_name

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry=None) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict.

        The table is saved by its name and by SpiralDB's table id; ``from_config``
        checks that the name still leads to that table. A version pinned by the
        source itself is not saved: a loaded source reads the table's then-current
        version. An ``asof`` the user gave is saved.
        """
        return {
            "source_type": "spiraldb_table",
            "project_id": self._project_id,
            "dataset": self._dataset,
            "overrides": self._overrides,
            "table_name": self._table_name,
            "table_id": self._table_id,
            "tag_columns": list(self._tag_columns),
            "system_tag_columns": list(self._system_tag_columns),
            "where": _locationto_config(self._where),
            "limit": self._limit,
            "asof": self._given_asof,
            "max_rows": self._max_rows,
            "batch_rows": self._batch_rows,
            "tags_as_data": dict(self._tags_as_data) or None,
            "source_id": self.source_id,
            **self._identity_config(),
        }

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry=None
    ) -> "SpiralDBTableSource":
        """Reconstruct a SpiralDBTableSource from a config dict, reading no rows.

        Args:
            config: Dict as produced by ``to_config()``.

        Returns:
            A new ``SpiralDBTableSource`` instance.

        Raises:
            ValueError: If the saved table name now leads to a table with another
                SpiralDB table id than the one saved (the table was dropped and
                created again, or another table took the name).
        """
        source = cls(
            project_id=config["project_id"],
            table_name=config["table_name"],
            dataset=config.get("dataset", "default"),
            tag_columns=config.get("tag_columns"),
            system_tag_columns=config.get("system_tag_columns", ()),
            where=_locationfrom_config(config.get("where") or {}),
            limit=config.get("limit"),
            asof=config.get("asof"),
            max_rows=config.get("max_rows", DEFAULT_MAX_ROWS),
            batch_rows=config.get("batch_rows", DEFAULT_BATCH_ROWS),
            tags_as_data=config.get("tags_as_data"),
            source_id=config.get("source_id"),
            label=config.get("label"),
            data_context=config.get("data_context"),
            overrides=config.get("overrides"),
        )
        saved_id = config.get("table_id")
        if saved_id is not None and saved_id != source.table_id:
            raise ValueError(
                f"SpiralDB table {source._qualified!r} was saved with table id "
                f"{saved_id!r} but that name now leads to table id "
                f"{source.table_id!r}: it is a different table."
            )
        return source
