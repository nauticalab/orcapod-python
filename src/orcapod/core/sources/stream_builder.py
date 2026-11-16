"""Compositional builder for enriching raw Arrow tables into source streams.

Extracts the enrichment pipeline that was previously embedded in
``ArrowTableSource.__init__``: dropping system columns, validating tags,
computing schema/table hashes, adding source-info provenance, adding system
tag columns, and wrapping the result in an ``ArrowTableStream``.
"""

from __future__ import annotations

import datetime
import json
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast

from orcapod.core.sources.source_ids import hash_source_id
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import RowIdentifierError
from orcapod.system_constants import constants
from orcapod.types import ContentHash
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.schema_utils import compute_source_schema_hash
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.config import OrcapodConfig
    from orcapod.contexts import DataContext
    from orcapod.protocols.hashing_protocols import ArrowHasherProtocol
else:
    pa = LazyModule("pyarrow")


def normalize_record_id_columns(
    record_id_columns: str | Collection[str] | None,
) -> tuple[str, ...] | None:
    """Normalize a source's ``record_id_columns`` argument (ITL-1154).

    Args:
        record_id_columns: ``None`` for the source's default (the primary key of a
            database table, the position otherwise), a column name or several key
            columns in key order, or ``()`` for the position.

    Returns:
        ``None``, or the key columns as a tuple (empty for the position).

    Raises:
        TypeError: If several key columns are given in a collection without an
            order (a set or a mapping): the key order is part of every row
            identifier, so it must be stated.
    """
    if record_id_columns is None:
        return None
    if isinstance(record_id_columns, str):
        return (record_id_columns,)
    if not isinstance(record_id_columns, Sequence):
        raise TypeError(
            "record_id_columns must list the key columns in key order (a list or "
            f"a tuple), not a {type(record_id_columns).__name__}: the order is part "
            "of every row identifier."
        )
    return tuple(record_id_columns)


def positional_row_identifier(row_index: int) -> str:
    """Return the row identifier of a row named by its position: ``row_<n>``.

    The identifier names a row position, not a row: rows supplied in another order
    under the same source id get other identifiers. Name key columns
    (``record_id_columns``) when row order is not fixed.

    Args:
        row_index: The row's position, from 0.

    Returns:
        ``"row_<n>"``.
    """
    return f"row_{row_index}"


_FRACTION_DIGITS = {"s": 0, "ms": 3, "us": 6, "ns": 9}
"""Fractional-second digits of a timestamp key value, fixed by the column's unit."""


def _timestamp_text(value: int, unit: str, *, utc: bool) -> str:
    """Return a timestamp key value as ISO-8601 text, from its stored integer.

    Formatted from the integer, never from a float or a library object, so it is
    exact at every unit. A time-zone-aware timestamp is written in UTC with ``Z``; a
    naive one without an offset, so the two never share a form.

    Args:
        value: The stored integer: units since the epoch (UTC for a zoned column).
        unit: The column's unit, ``s``, ``ms``, ``us`` or ``ns``.
        utc: Whether the column has a time zone.

    Returns:
        ``YYYY-MM-DDTHH:MM:SS[.fraction][Z]``, the year always four digits.

    Raises:
        OverflowError: If the moment lies outside years 1 to 9999.
    """
    digits = _FRACTION_DIGITS[unit]
    seconds, fraction = divmod(value, 10**digits)
    moment = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
    # ``isoformat`` always writes four year digits; ``strftime("%Y")`` does not on
    # every platform.
    text = moment.isoformat(timespec="seconds")
    if digits:
        text += "." + str(fraction).zfill(digits)
    return text + ("Z" if utc else "")


def _key_values(column: "pa.ChunkedArray[Any]") -> list[object]:
    """Return a key column's values as JSON values (ITL-1154).

    Strings, integers and booleans as they are. A timestamp is ISO-8601 text
    (``_timestamp_text``) and a date ``YYYY-MM-DD``, both formatted from the stored
    integers. A null stays ``None`` (``row_identifiers`` refuses it).

    Args:
        column: A key column of an allowed type (``key_column_type_allowed``).

    Returns:
        One value per row.

    Raises:
        OverflowError: If a timestamp or date lies outside years 1 to 9999, which
            Arrow can store and ISO-8601 text here cannot write.
    """
    arrow_type = column.type
    if pa.types.is_timestamp(arrow_type):
        unit, utc = arrow_type.unit, arrow_type.tz is not None
        ints = column.cast(pa.int64()).to_pylist()
        return [None if v is None else _timestamp_text(v, unit, utc=utc) for v in ints]
    if pa.types.is_date32(arrow_type):
        days = column.cast(pa.int32()).to_pylist()
        epoch = datetime.date(1970, 1, 1)
        return [
            None if d is None else (epoch + datetime.timedelta(days=d)).isoformat()
            for d in days
        ]
    if pa.types.is_date64(arrow_type):
        millis = column.cast(pa.int64()).to_pylist()
        epoch = datetime.date(1970, 1, 1)
        return [
            None
            if m is None
            else (epoch + datetime.timedelta(milliseconds=m)).isoformat()
            for m in millis
        ]
    return cast("list[object]", column.to_pylist())


def key_row_identifier(key: Sequence[tuple[str, object]]) -> str:
    """Return the row identifier of a row named by its key columns.

    The canonical JSON object of the key columns in the declared key order, with no
    spaces: ``{"unit":"u1","time":1700000000}``. It handles composite keys and keeps
    value types apart (``17`` and ``"17"`` differ). This is the one definition of the
    form (ITL-1154).

    Args:
        key: ``(column, value)`` pairs in the declared key order; each value a
            ``str``, ``int`` or ``bool`` (``key_columns_problem`` checks the types).

    Returns:
        The JSON text.
    """
    return json.dumps(dict(key), separators=(",", ":"), ensure_ascii=False)


def key_column_type_allowed(arrow_type: pa.DataType) -> bool:
    """Whether a column of ``arrow_type`` may be a key column (ITL-1154).

    Allowed: strings, integers, booleans, timestamps and dates (written as ISO-8601
    text, ``_key_values``). Everything else (floats, bytes, times of day, durations,
    UUIDs, containers, logical types) is refused; more types can be added later
    without moving an existing identifier.

    Args:
        arrow_type: The column's Arrow type.

    Returns:
        True for a string, integer or boolean type.
    """
    return (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_integer(arrow_type)
        or pa.types.is_boolean(arrow_type)
        or pa.types.is_timestamp(arrow_type)
        or pa.types.is_date32(arrow_type)
        or pa.types.is_date64(arrow_type)
    )


def row_identifiers(table: pa.Table, record_id_columns: Sequence[str]) -> list[str]:
    """Return each row's identifier: its key, or its position when no key is named.

    Args:
        table: The source's rows.
        record_id_columns: The key columns, in key order; empty for position.

    Returns:
        One identifier per row, in row order.

    Raises:
        RowIdentifierError: If a key column is missing or of a type that is not allowed, a
            key value is null, a timestamp or date key lies outside years 1 to 9999,
            or two rows have the same key. The message names the column or the key.
    """
    if not record_id_columns:
        return [positional_row_identifier(i) for i in range(table.num_rows)]
    missing = [c for c in record_id_columns if c not in table.column_names]
    if missing:
        raise RowIdentifierError(
            f"record_id_columns {missing} not found in table columns: "
            f"{table.column_names}"
        )
    for column in record_id_columns:
        arrow_type = table.schema.field(column).type
        if not key_column_type_allowed(arrow_type):
            raise RowIdentifierError(
                f"key column {column!r} has type {arrow_type}, which cannot identify "
                "a row: a key column must hold strings, integers, booleans, "
                "timestamps or dates. Name a string or integer key column in "
                "record_id_columns, or pass record_id_columns=() to identify rows by "
                "position."
            )
    columns: dict[str, list[object]] = {}
    for column in record_id_columns:
        try:
            columns[column] = _key_values(table.column(column))
        except OverflowError as exc:
            raise RowIdentifierError(
                f"key column {column!r} holds a {table.schema.field(column).type} "
                "value outside years 1 to 9999, which a row identifier cannot write"
            ) from exc
    identifiers: list[str] = []
    seen: dict[str, int] = {}
    for i in range(table.num_rows):
        key = [(c, columns[c][i]) for c in record_id_columns]
        nulls = [c for c, value in key if value is None]
        if nulls:
            raise RowIdentifierError(
                f"key column {nulls[0]!r} is null in row {i}: a key value identifies "
                "the row and cannot be null"
            )
        identifier = key_row_identifier(key)
        if identifier in seen:
            raise RowIdentifierError(
                f"key {identifier} appears in rows {seen[identifier]} and {i}: key "
                "values must be unique, so each row has its own identifier"
            )
        seen[identifier] = i
        identifiers.append(identifier)
    return identifiers


def _unhashable_columns(
    hasher: ArrowHasherProtocol, table: pa.Table
) -> list[tuple[str, pa.DataType]]:
    """Return the columns of ``table`` that ``hasher`` cannot hash, with their types.

    Each column is hashed on its own. A zero-row slice is not enough, because the hasher
    rejects a type only when it visits a value, so the whole column is probed. This runs
    only after hashing the whole table has failed.

    Args:
        hasher: The data context's Arrow hasher.
        table: The table whose hash failed.

    Returns:
        ``(column name, Arrow type)`` for every column whose hash raises
        ``ValueError``, in column order.
    """
    failing: list[tuple[str, pa.DataType]] = []
    for name in table.column_names:
        try:
            hasher.hash_table(table.select([name]))
        except ValueError:
            failing.append((name, table.schema.field(name).type))
    return failing


@dataclass(frozen=True)
class SourceStreamResult:
    """Artifacts produced by ``SourceStreamBuilder.build()``."""

    stream: ArrowTableStream
    schema_hash: str
    table_hash: ContentHash
    source_id: str
    tag_columns: tuple[str, ...]
    system_tag_columns: tuple[str, ...]


class SourceStreamBuilder:
    """Builds an enriched ``ArrowTableStream`` from a raw Arrow table.

    Args:
        data_context: Provides type_converter, semantic_hasher, arrow_hasher.
        config: Orcapod config (controls hash character counts).
    """

    def __init__(self, data_context: DataContext, config: OrcapodConfig) -> None:
        self._data_context = data_context
        self._config = config

    def build(
        self,
        table: pa.Table,
        tag_columns: Collection[str],
        source_id: str | None = None,
        record_id_columns: str | Collection[str] | None = (),
        system_tag_columns: Collection[str] = (),
        source_kind: str | None = None,
    ) -> SourceStreamResult:
        """Run the full enrichment pipeline.

        The builder trusts the incoming table's nullable flags as-is.
        Callers are responsible for setting nullable correctly before calling
        this method — use ``arrow_utils.infer_schema_nullable(table)`` when
        receiving a raw Arrow table whose schema has not been set deliberately
        (e.g. from Polars or plain ``pa.table({})``, which default all fields
        to ``nullable=True`` regardless of content).

        Args:
            table: Arrow table with nullable flags already set correctly.
            tag_columns: Column names forming the tag for each row.
            source_id: Canonical source name. When not given, the default is
                ``<source_kind>:<algorithm label>:<hex>`` of the table hash
                (``hash_source_id``), which changes when rows are reordered;
                pass an explicit value when record IDs must survive reordering.
            record_id_columns: The key columns whose values identify a row, in key
                order: the row identifier is their canonical JSON
                (``key_row_identifier``), which follows the row regardless of its
                position (given a stable ``source_id``). Empty: the row's position,
                ``row_<n>``, stable only while row order is.
            system_tag_columns: Additional system-level tag columns.
            source_kind: Kind prefix of the default source id, for example
                ``dict``. Required when ``source_id`` is not given; not used
                otherwise.

        Returns:
            SourceStreamResult with enriched stream and metadata.

        Raises:
            ValueError: If the table has no columns ("Table is empty"), if
                tag_columns or a key column are not in the table, a key column's
                type is not allowed, a key value is null or repeated, or if
                neither ``source_id`` nor ``source_kind`` is given.
        """
        tag_columns_tuple = tuple(tag_columns)
        system_tag_columns_tuple = tuple(system_tag_columns)

        # A tag never has a meta column's name; the meta columns are dropped next,
        # so say so here rather than report the tag as missing (ITL-1080).
        schema_utils.require_names_not_meta(tag_columns_tuple)

        # 1. Drop system columns from raw input.
        table = arrow_utils.drop_system_columns(table)

        # 2. Validate tag_columns. A table with no columns at all (e.g. a
        # DictSource built from no rows and no schema) gets its own error, since
        # "tag column not found" would point at the wrong problem.
        if not table.column_names:
            raise ValueError(
                "Table is empty: the input has no columns, so its tag and data "
                "columns cannot be found. To build a source with no rows, give it "
                "an explicit schema (for example data_schema= on DictSource)."
            )
        missing_tags = set(tag_columns_tuple) - set(table.column_names)
        if missing_tags:
            raise ValueError(
                f"tag_columns not found in table: {missing_tags}. "
                f"Available columns: {list(table.column_names)}"
            )

        # 3. The row identifiers (validates the key columns).
        identifiers = row_identifiers(
            table, normalize_record_id_columns(record_id_columns) or ()
        )

        # 4. Compute schema hash from tag/data python schemas.
        # Nullable flags in the incoming table are trusted as-is — callers must
        # set them correctly before calling build().
        non_sys = arrow_utils.drop_system_columns(table)
        tag_schema = non_sys.select(list(tag_columns_tuple)).schema
        data_schema = non_sys.drop(list(tag_columns_tuple)).schema
        tag_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            tag_schema
        )
        data_python = self._data_context.type_converter.arrow_schema_to_python_schema(
            data_schema
        )
        schema_hash = compute_source_schema_hash(
            tag_python,
            data_python,
            self._data_context,
            self._config,
        )

        # 5. Compute table hash for data identity.
        hasher = self._data_context.arrow_hasher
        try:
            table_hash = hasher.hash_table(table)
        except ValueError as exc:
            # The hasher (starfix 0.5) raises ValueError for a type it does not hash
            # and names the first such column only; name every column it refuses.
            unhashable = _unhashable_columns(hasher, table)
            if not unhashable:
                raise
            described = ", ".join(
                f"{name!r} of type {kind}" for name, kind in unhashable
            )
            noun = "column" if len(unhashable) == 1 else "columns"
            raise ValueError(
                f"cannot hash {noun} {described}: the Arrow hasher refused "
                f"({exc}). Drop or convert before building the source."
            ) from exc

        # 6. Default source_id: the source kind and the labelled table hash, at
        # full length. A source id is the source's name, so a default built from a
        # hash says what kind of source it is and which algorithm made the hash.
        if source_id is None:
            if source_kind is None:
                raise ValueError(
                    "build() needs a source_id or a source_kind to make a default "
                    "source id from."
                )
            source_id = hash_source_id(source_kind, table_hash)

        # 7. Attach the source-info tokens and the system tags.
        stream = self.attach_provenance(
            table,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
            schema_hash=schema_hash,
            source_id=source_id,
            row_identifiers=identifiers,
        )

        return SourceStreamResult(
            stream=stream,
            schema_hash=schema_hash,
            table_hash=table_hash,
            source_id=source_id,
            tag_columns=tag_columns_tuple,
            system_tag_columns=system_tag_columns_tuple,
        )

    def attach_provenance(
        self,
        table: pa.Table,
        *,
        tag_columns: Sequence[str],
        system_tag_columns: Sequence[str],
        schema_hash: str,
        source_id: str,
        row_identifiers: Sequence[str],
    ) -> ArrowTableStream:
        """Add source-info and system-tag columns to ``table`` and wrap it as a stream.

        A row's record id is its row identifier itself, as text (``row_<n>`` or the
        key's canonical JSON), and its source id is the source's name: the two
        system-tag columns. Each source-info value is
        ``<source_id>::<record id>::<column>`` (for example ``_source_amplitude``
        holds ``<source_id>::row_3::amplitude``). A row keeps its record id wherever
        and however often it is read, as long as its identifier is the same
        (ITL-1154).

        Args:
            table: The rows, with nullable flags already set correctly.
            tag_columns: Column names forming the tag for each row.
            system_tag_columns: Additional system-level tag columns.
            schema_hash: The source's schema hash, which names its system-tag columns.
            source_id: The canonical source name.
            row_identifiers: One identifier per row, identifying the row within the
                source.

        Returns:
            The stream over the enriched table.
        """
        source_info = [
            f"{source_id}{constants.BLOCK_SEPARATOR}{identifier}"
            for identifier in row_identifiers
        ]
        table = arrow_utils.add_source_info(
            table, source_info, exclude_columns=tag_columns
        )
        table = arrow_utils.add_system_tag_columns(
            table,
            schema_hash,
            source_id,
            row_identifiers,
        )
        # Nullable flags are already correct — the caller set them.
        return ArrowTableStream(
            table=table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
        )
