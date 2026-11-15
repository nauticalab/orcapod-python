from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import override

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.reload import same_rows
from orcapod.core.sources.source_ids import hash_source_id
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.core.streams.view import (
    ViewStatus,
    StreamView,
)
from orcapod.core.streams.base import (
    check_head_size,
    schema_requires_io,
)
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.core.nodes.function_node import FunctionJobNode
    from orcapod.core.nodes.operator_node import OperatorJobNode
else:
    pa = LazyModule("pyarrow")


def _derivation_hash(
    origin: "FunctionJobNode | OperatorJobNode | RootSource",
    tags_as_data: Mapping[str, str],
    record_id_column: str | None,
) -> ContentHash:
    """Return the content hash an operator node doing this derivation would have.

    The hashed structure has the shape an operator's output stream hashes for its
    ``content_hash``: the producer's identity (class name and parameters) followed
    by its upstreams. Here that is ``(("DerivedSource", sorted tags_as_data items),
    (origin,))``, with the origin entering through its own ``content_hash``. A
    ``record_id_column`` is a parameter of the derivation too, since it decides
    the record IDs: when one is set, ``("record_id_column", name)`` is appended to
    the parameters. The hasher is the origin's, so the result depends on the
    origin's identity and on these parameters, and on nothing else.

    Args:
        origin: The source or job node the derived source reads from.
        tags_as_data: Maps a tag column of the origin to the name of its data copy.
        record_id_column: The column that identifies a row, or ``None`` for the
            row position.

    Returns:
        The derivation hash, from which ``_default_source_id`` builds the default
        ``source_id`` of a ``DerivedSource`` built with ``tags_as_data``.
    """
    hasher = origin.data_context.semantic_hasher
    parameters: tuple[Any, ...] = ("DerivedSource", tuple(sorted(tags_as_data.items())))
    if record_id_column is not None:
        parameters += (("record_id_column", record_id_column),)
    return hasher.hash_object(
        (parameters, (origin,)),
        resolver=lambda obj: obj.content_hash(hasher),
    )


def validate_tags_as_data(
    tags_as_data: Mapping[str, str],
    tag_keys: tuple[str, ...],
    data_keys: tuple[str, ...],
) -> None:
    """Check a ``tags_as_data`` mapping against the columns it copies from.

    The one rule for ``DerivedSource`` and ``SpiralDBTableSource``.

    Args:
        tags_as_data: Maps a tag column to the name of its data copy.
        tag_keys: The user tag columns of the stream copied from.
        data_keys: The data columns of the stream copied from.

    Raises:
        InputValidationError: If a key is not a tag column, or a new
            name is already a column, is used twice, or starts with the prefix
            reserved for system columns.
    """
    seen: set[str] = set()
    for tag, name in tags_as_data.items():
        if tag not in tag_keys:
            kind = "a data column" if tag in data_keys else "not a column"
            raise InputValidationError(
                f"tags_as_data: {tag!r} is {kind}, not a tag column. "
                f"Tag columns: {list(tag_keys)}."
            )
        if name in tag_keys or name in data_keys:
            raise InputValidationError(
                f"tags_as_data: cannot copy tag {tag!r} to {name!r}, which is "
                "already a column. Choose a new name."
            )
        if name in seen:
            raise InputValidationError(
                f"tags_as_data: the name {name!r} is given to more than one tag. "
                "Each copy needs its own name."
            )
        if name.startswith(constants.DATAGRAM_PREFIX):
            raise InputValidationError(
                f"tags_as_data: cannot copy tag {tag!r} to {name!r}: names starting "
                f"with {constants.DATAGRAM_PREFIX!r} are reserved for system columns."
            )
        seen.add(name)


class DerivedSource(RootSource):
    """A source whose rows are derived from another source or from a job node.

    There are two forms.

    **Plain** (``tags_as_data`` not given), created by ``FunctionNode.as_source()``
    or ``OperatorNode.as_source()``: a static stream backed by the computed records
    of a DB-backed stream node. It presents the node's stored results with the
    node's own tag and data columns. The origin must implement
    ``get_all_records()``, ``output_schema()``, ``keys()`` and ``content_hash()``.

    **With ``tags_as_data``**: a new root source over any source or job node that
    exposes chosen tag columns of the origin *also* as data columns, under the
    given names. A function pod never sees tags, and an operator never puts new
    content into the data group; a source defines its own tag and data columns, so
    this is where a key (for example a session start time) can also become an
    input to computation. The tags are the origin's tags. The data columns are the
    origin's data columns followed by the copies, in mapping order. Each copy has
    the tag column's exact Arrow type, nullability and field metadata. The copy
    equals the tag when the source is read; downstream it is plain data.

    Being a source, this form starts provenance afresh by the rule every source
    follows: its own system-tag columns, record IDs derived from its ``source_id``
    and each row's position (or ``record_id_column``), and a source-info token
    for every data column, the copies included. It stores nothing itself; like
    any source it can be wrapped with ``cached(database)``.

    Identity (``tags_as_data`` form):
        - ``source_id`` defaults to ``tags_as_data:<algorithm label>:<hex digest>``,
          where label and digest are the derivation hash: the content hash an
          operator node doing this derivation over the origin would have, built
          from the origin's content hash, the sorted ``tags_as_data`` items and
          the ``record_id_column`` when one is set. It changes when the origin's
          identity or one of these parameters changes. An explicit ``source_id``
          replaces it.
        - ``content_hash`` follows the rule of every root source (class name,
          output schema and ``source_id``) plus the sorted ``tags_as_data`` items,
          since two mappings can give one schema and one explicit ``source_id``.
        - ``pipeline_hash`` is schema-only, as for every root source.

    Identity (plain form):
        - ``content_hash`` is tied to the origin node's content hash.
        - ``pipeline_hash`` is schema-only, so plain derived sources with
          identical schemas share the same pipeline DB table.

    If the origin is a job node that has not run yet, the source presents zero
    rows with the correct columns. In the ``tags_as_data`` form the rows appear
    once the node has run.

    Args:
        origin: The source or DB-backed job node to derive from.
        source_id: Canonical source name. Defaults as described above.
        tags_as_data: Maps a tag column of the origin to the name of the data
            column that receives a copy of it.
        record_id_column: Column whose value identifies a row in record IDs and
            provenance tokens, instead of the row position. Only with
            ``tags_as_data``.
        **kwargs: ``label``, ``data_context`` (defaults to the origin's) and
            ``config``, passed to ``RootSource``.

    Raises:
        InputValidationError: If ``tags_as_data`` names a column that is not a tag
            of the origin, or a new name that is already a column, is repeated,
            or is reserved.
        ValueError: If ``record_id_column`` is given without ``tags_as_data``.
        TypeError: If the origin is a source and ``tags_as_data`` is not given.

    Example::

        experiment = DerivedSource(
            SpiralDBTableSource(project, "experiment"),
            tags_as_data={"session_start_datetime": "session_start"},
        )
    """

    # The config type key (names the class a saved config is rebuilt with) and
    # the kind prefix of the plain form's default source id.
    SOURCE_KIND: ClassVar[str] = "derived"
    # The kind prefix of a hash-based default source id names the operation that
    # derives the source. One constant per operation.
    TAGS_AS_DATA_KIND: ClassVar[str] = "tags_as_data"

    @classmethod
    def _default_source_id(
        cls,
        origin: "FunctionJobNode | OperatorJobNode | RootSource",
        tags_as_data: Mapping[str, str],
        record_id_column: str | None,
    ) -> str:
        """Return the default ``source_id`` of a source built with ``tags_as_data``.

        The format is ``<kind>:<algorithm label>:<hex digest>``, for example
        ``tags_as_data:semantic_v1:01bb...``: the kind names the operation that
        derives the source, and the label and digest are the derivation hash
        (``_derivation_hash``) in its labelled form. An empty mapping is still
        this operation and gets the same kind. This one string is the
        source's name everywhere it appears: the system-tag ``source_id`` values,
        the source-info tokens, and the name of the source in a job.

        Args:
            origin: The source or job node the derived source reads from.
            tags_as_data: Maps a tag column of the origin to the name of its copy.
            record_id_column: The column that identifies a row, or ``None`` for
                the row position.

        Returns:
            The default source id.
        """
        derivation_hash = _derivation_hash(origin, tags_as_data, record_id_column)
        return f"{cls.TAGS_AS_DATA_KIND}:{derivation_hash.to_string()}"

    def __init__(
        self,
        origin: "FunctionJobNode | OperatorJobNode | RootSource",
        source_id: str | None = None,
        tags_as_data: Mapping[str, str] | None = None,
        record_id_column: str | None = None,
        **kwargs: Any,
    ) -> None:
        if tags_as_data is None:
            if isinstance(origin, RootSource):
                raise TypeError(
                    "A DerivedSource over a source needs tags_as_data: the plain "
                    "form reads the stored records of a job node."
                )
            if record_id_column is not None:
                raise ValueError(
                    "record_id_column is only used together with tags_as_data."
                )
            self._tags_as_data: dict[str, str] | None = None
        else:
            self._tags_as_data = dict(tags_as_data)
            tag_keys, data_keys = origin.keys()
            validate_tags_as_data(self._tags_as_data, tag_keys, data_keys)
            kwargs.setdefault("data_context", origin.data_context_key)

        self._explicit_source_id = source_id
        if source_id is None:
            # A source id is the source's name, used as-is in system tags and
            # source-info values. A default built from a hash keeps a source-kind
            # prefix and the hash's label (developer, November 11, 2026):
            # ``derived:<algorithm label>:<hex>`` of the origin's content hash.
            if self._tags_as_data is None:
                source_id = hash_source_id(self.SOURCE_KIND, origin.content_hash())
            else:
                source_id = self._default_source_id(
                    origin, self._tags_as_data, record_id_column
                )
        super().__init__(source_id=source_id, **kwargs)
        self._origin = origin
        self._record_id_column = record_id_column
        self._cached_table: pa.Table | None = None
        self._cached_stream: ArrowTableStream | None = None
        # The origin job node's modified time when its rows were cached: a check reads
        # its stored rows again only after it ran again (ITL-1102).
        self._origin_seen: datetime | None = None

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return what the content hash is built from.

        The plain form is tied to the origin node's content hash. The
        ``tags_as_data`` form is the root source rule (class name, output schema,
        ``source_id``) followed by the sorted mapping items: two mappings can give
        the same schema (two tag columns of one type copied to one name) while the
        copy holds different values, so with an explicit ``source_id`` the mapping
        has to tell the sources apart (ITL-1000).

        Returns:
            The identity structure.
        """
        if self._tags_as_data is None:
            # Tied precisely to the specific node's data identity
            return (self._origin.content_hash(),)
        return (
            *super().identity_structure(),
            ("tags_as_data", tuple(sorted(self._tags_as_data.items()))),
        )

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize this source to a JSON-compatible dict.

        The origin's own config is embedded when the origin is a source, which
        makes the derived source reloadable whenever its origin is. A default
        ``source_id`` is not written: it is recomputed from the reloaded origin.

        Args:
            db_registry: Optional database registry, forwarded to the origin.

        Returns:
            The config dict, with ``source_type`` ``"derived"``.
        """
        config: dict[str, Any] = {
            "source_type": self.SOURCE_KIND,
            "tags_as_data": self._tags_as_data,
            "record_id_column": self._record_id_column,
            "source_id": self._explicit_source_id,
        }
        # Every concrete source has to_config; the abstract RootSource does not
        # declare it, so look it up as PipelineJob.save does.
        origin_to_config = getattr(self._origin, "to_config", None)
        if (
            self._tags_as_data is not None
            and isinstance(self._origin, RootSource)
            and origin_to_config is not None
        ):
            config["origin"] = origin_to_config(db_registry=db_registry)
        config.update(self._identity_config())
        return config

    @classmethod
    def from_config(
        cls, config: dict[str, Any], db_registry: Any = None
    ) -> "DerivedSource":
        """Rebuild a derived source by first rebuilding its origin.

        Args:
            config: Dict as produced by ``to_config()``.
            db_registry: Optional database registry, forwarded to the origin.

        Returns:
            A new ``DerivedSource`` over the rebuilt origin.

        Raises:
            NotImplementedError: If the config holds no origin: a derived source
                over a job node cannot be rebuilt from a config.
        """
        from orcapod.pipeline.serialization import resolve_source_from_config

        if config.get("origin") is None:
            raise NotImplementedError(
                "A DerivedSource over a job node cannot be reconstructed from "
                "config: its rows live in the node's pipeline database."
            )
        origin = resolve_source_from_config(config["origin"], db_registry=db_registry)
        return cls(
            origin,
            source_id=config.get("source_id"),
            tags_as_data=config["tags_as_data"],
            record_id_column=config.get("record_id_column"),
        )

    # -------------------------------------------------------------------------
    # Stream interface
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        if self._tags_as_data is None:
            return self._origin.output_schema(columns=columns, all_info=all_info)
        return self._get_stream().output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        if self._tags_as_data is None:
            return self._origin.keys(columns=columns, all_info=all_info)
        return self._get_stream().keys(columns=columns, all_info=all_info)

    def _origin_records(self) -> "pa.Table | None":
        """Return the origin's rows, or ``None`` when a job node has none stored."""
        if isinstance(self._origin, RootSource):
            return self._origin.as_table()
        return self._origin.get_all_records()

    def _empty_origin_table(self) -> "pa.Table":
        """Return a zero-row table with the origin's declared columns."""
        # Build empty table preserving declared field nullability.
        tag_schema, data_schema = self._origin.output_schema()
        tag_keys = self._origin.keys()[0]
        tc = self.data_context.type_converter
        python_schema = {k: tag_schema[k] for k in tag_keys}
        python_schema.update(data_schema)
        return arrow_utils.make_empty_table(python_schema, tc)

    def _build_derived_stream(
        self, tags_as_data: Mapping[str, str]
    ) -> tuple[ArrowTableStream, bool]:
        """Read the origin's rows, append the tag copies and enrich as a source.

        Args:
            tags_as_data: Maps a tag column of the origin to the name of its copy.

        Returns:
            The enriched stream, and whether the origin had rows to give. A job
            node that has not run yet has none; the stream is then empty.
        """
        table = self._origin_records()
        has_rows = table is not None
        if table is None:
            table = self._empty_origin_table()
        return self._derive_from(table, tags_as_data), has_rows

    def _derive_from(
        self, table: "pa.Table", tags_as_data: Mapping[str, str]
    ) -> ArrowTableStream:
        """Append the tag copies to the origin rows ``table`` and enrich it as a source.

        Args:
            table: Rows of the origin.
            tags_as_data: Maps a tag column of the origin to the name of its copy.
        """
        for tag, name in tags_as_data.items():
            # The copy is the tag's own Arrow column under a renamed field, so its
            # type, nullability and field metadata are the tag's exactly.
            table = table.append_column(
                table.schema.field(tag).with_name(name), table.column(tag)
            )
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table,
            tag_columns=self._origin.keys()[0],
            source_id=self.source_id,
            record_id_column=self._record_id_column,
        )
        return result.stream

    def _get_stream(self) -> ArrowTableStream:
        if self._tags_as_data is not None:
            if self._cached_stream is not None:
                return self._cached_stream
            self._origin_seen = self._origin.last_modified
            stream, has_rows = self._build_derived_stream(self._tags_as_data)
            if has_rows:
                # An origin node that has not run yet is not cached, so the rows
                # appear on this object once it has.
                self._cached_stream = stream
            return stream

        if self._cached_table is None:
            self._origin_seen = self._origin.last_modified
            records = self._origin_records()
            if records is None:
                self._cached_table = self._empty_origin_table()
            else:
                self._cached_table = records
        tag_keys = self._origin.keys()[0]
        return ArrowTableStream(self._cached_table, tag_columns=tag_keys)

    def _held_rows(self) -> "pa.Table | None":
        """The rows this source holds from its origin, or ``None`` before a read."""
        if self._tags_as_data is not None:
            return (
                None if self._cached_stream is None else self._cached_stream.as_table()
            )
        return self._cached_table

    @override
    def check_for_new_data(self) -> bool:
        """Take in the origin's rows if they changed since this source read them.

        Developer, November 14, 2026 (ITL-1102). Over a source, the origin is checked
        first (``check_for_new_data``). Then the origin's rows are read again only when
        its modified time moved since this source last read them: a source origin that
        took in new data, by this check or by a check made through another source over
        it, or a job node that ran again. The rows read are compared with the rows
        held (in any order only when a ``record_id_column`` gives the record ids); when
        they differ the source holds the new ones and moves its
        modified time. A read or comparison that raises leaves everything as it was, so
        the next check reads again. A source that holds nothing (it has not read its
        origin yet, or read none of its rows: a job node that had not run) reads the
        origin at its next read; when the origin's time moved, its own time moves too,
        so a node that read no rows from it runs again.

        Returns:
            ``True`` if the source's modified time moved: it holds new rows, or it holds
            none and its origin changed.
        """
        if isinstance(self._origin, RootSource):
            _ = self._origin.check_for_new_data()
        seen = self._origin.last_modified
        if seen == self._origin_seen:
            return False
        held = self._held_rows()
        if held is None:
            self._origin_seen = seen
            self._update_modified_time()
            return True
        if self._tags_as_data is not None:
            stream, has_rows = self._build_derived_stream(self._tags_as_data)
            unchanged = same_rows(
                held, stream.as_table(), ordered=self._record_id_column is None
            )
            self._origin_seen = seen
            if unchanged:
                return False
            self._cached_stream = stream if has_rows else None
        else:
            records = self._origin_records()
            read = self._empty_origin_table() if records is None else records
            # The stored rows' ids may not be theirs to keep in another order: order
            # counts unless a record_id_column was given (never assume).
            unchanged = same_rows(held, read, ordered=self._record_id_column is None)
            self._origin_seen = seen
            if unchanged:
                return False
            self._cached_table = read
        self._update_modified_time()
        return True

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Whether reading the schema with ``config`` would poll: as for the origin."""
        return schema_requires_io(self._origin, config)

    def head(
        self,
        n: int = 10,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> StreamView:
        """Return the first rows of the origin's records, without computing.

        The records are the ones this source already read. Otherwise, an origin source
        gives its own ``head(n)`` (so a polling or caching origin is never polled or
        written), and an origin job node gives its stored rows, read now and not kept.
        An origin job node that has not run gives no rows. With ``sort_by_tags`` (or
        ``all_info``), the rows are sorted by tag as ``as_table`` sorts them: all the
        stored rows of an origin job node, but only the rows an origin source reads for
        its head (see ``StreamBase.head``).
        """
        check_head_size(n)
        config = ColumnConfig.handle_config(columns, all_info=all_info)
        origin_kind = "held" if isinstance(self._origin, RootSource) else "stored"
        if self._tags_as_data is not None and self._cached_stream is not None:
            return self._cached_stream.head(n, columns=config).for_stream(
                self, origin=origin_kind
            )
        if self._tags_as_data is None and self._cached_table is not None:
            stream = ArrowTableStream(
                self._cached_table, tag_columns=self._origin.keys()[0]
            )
            return stream.head(n, columns=config).for_stream(self, origin=origin_kind)
        if isinstance(self._origin, RootSource):
            origin_head = self._origin.head(n)
            if (
                config.sort_by_tags
                and origin_head.status is ViewStatus.ROWS
                and not origin_head.total_is_estimate
                and origin_head.total_rows is not None
                and origin_head.total_rows > n
            ):
                # The origin has all its rows (its total is exact): sorting needs them
                # all, and the system tags this source adds follow the origin's row
                # order, so the origin is not sorted first. A bounded origin (an
                # estimated total) is sorted among the rows it read.
                origin_head = self._origin.head(origin_head.total_rows)
            if origin_head.status is not ViewStatus.ROWS:
                if origin_head.table.num_columns == 0:
                    # The origin's columns are not known without polling it.
                    return origin_head.for_stream(self)
                # No rows: this source's own columns (tag copies included), with the
                # origin's status and explanation.
                empty = self._stream_over(origin_head.table.slice(0, 0))
                return dataclasses.replace(
                    StreamView.without_rows(
                        self,
                        origin_head.status,
                        empty.as_table(columns=config),
                        tag_columns=empty.keys(columns=config)[0],
                        note=origin_head.note,
                    ),
                    total_rows=origin_head.total_rows,
                    total_is_estimate=origin_head.total_is_estimate,
                    description=origin_head.description,
                )
            stream = self._stream_over(origin_head.table)
            return dataclasses.replace(
                stream.head(n, columns=config).for_stream(self),
                total_rows=origin_head.total_rows,
                total_is_estimate=origin_head.total_is_estimate,
                origin=origin_head.origin,
                note=origin_head.note,
            )
        records = self._origin_records()
        if records is None or records.num_rows == 0:
            stream = self._stream_over(self._empty_origin_table())
            return StreamView.without_rows(
                self,
                ViewStatus.NOTHING_STORED,
                stream.as_table(columns=config),
                tag_columns=stream.keys(columns=config)[0],
                note="nothing stored: its origin has not run",
            )
        # Without sorting only the shown rows are enriched. With sorting every stored
        # row is (they are all read already), so the head is as_table cut to n rows.
        # The total is the whole stored count.
        rows = records if config.sort_by_tags else records.slice(0, n)
        head = self._stream_over(rows).head(n, columns=config)
        return dataclasses.replace(
            head.for_stream(self, origin="stored"), total_rows=records.num_rows
        )

    def _stream_over(self, table: "pa.Table") -> ArrowTableStream:
        """Return this source's stream over the origin rows ``table``."""
        if self._tags_as_data is not None:
            return self._derive_from(table, self._tags_as_data)
        return ArrowTableStream(table, tag_columns=self._origin.keys()[0])

    def iter_data(self):
        return self._get_stream().iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._get_stream().as_table(columns=columns, all_info=all_info)
