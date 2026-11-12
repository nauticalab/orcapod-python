from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
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


def _validate_tags_as_data(
    tags_as_data: Mapping[str, str],
    tag_keys: tuple[str, ...],
    data_keys: tuple[str, ...],
) -> None:
    """Check a ``tags_as_data`` mapping against the origin's columns.

    Args:
        tags_as_data: Maps a tag column of the origin to the name of its data copy.
        tag_keys: The origin's user tag columns.
        data_keys: The origin's data columns.

    Raises:
        InputValidationError: If a key is not a tag column of the origin, or a new
            name is already a column, is used twice, or starts with the prefix
            reserved for system columns.
    """
    seen: set[str] = set()
    for tag, name in tags_as_data.items():
        if tag not in tag_keys:
            kind = "a data column" if tag in data_keys else "not a column"
            raise InputValidationError(
                f"tags_as_data: {tag!r} is {kind} of the origin, not a tag column. "
                f"Tag columns: {list(tag_keys)}."
            )
        if name in tag_keys or name in data_keys:
            raise InputValidationError(
                f"tags_as_data: cannot copy tag {tag!r} to {name!r}, which is "
                "already a column of the origin. Choose a new name."
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
        - ``content_hash`` follows the rule of every root source: class name,
          output schema and ``source_id``.
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

    # The config type key: names the class a saved config is rebuilt with.
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
        ``tags_as_data:semantic_v0.1:01bb...``: the kind names the operation that
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
            _validate_tags_as_data(self._tags_as_data, tag_keys, data_keys)
            kwargs.setdefault("data_context", origin.data_context_key)

        self._explicit_source_id = source_id
        if source_id is None:
            if self._tags_as_data is None:
                source_id = f"derived:{origin.content_hash().to_string()}"
            else:
                source_id = self._default_source_id(
                    origin, self._tags_as_data, record_id_column
                )
        super().__init__(source_id=source_id, **kwargs)
        self._origin = origin
        self._record_id_column = record_id_column
        self._cached_table: pa.Table | None = None
        self._cached_stream: ArrowTableStream | None = None

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        if self._tags_as_data is None:
            # Tied precisely to the specific node's data identity
            return (self._origin.content_hash(),)
        return super().identity_structure()

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
        return result.stream, has_rows

    def _get_stream(self) -> ArrowTableStream:
        if self._tags_as_data is not None:
            if self._cached_stream is not None:
                return self._cached_stream
            stream, has_rows = self._build_derived_stream(self._tags_as_data)
            if has_rows:
                # An origin node that has not run yet is not cached, so the rows
                # appear on this object once it has.
                self._cached_stream = stream
            return stream

        if self._cached_table is None:
            records = self._origin_records()
            if records is None:
                self._cached_table = self._empty_origin_table()
            else:
                self._cached_table = records
        tag_keys = self._origin.keys()[0]
        return ArrowTableStream(self._cached_table, tag_columns=tag_keys)

    def iter_data(self):
        return self._get_stream().iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._get_stream().as_table(columns=columns, all_info=all_info)
