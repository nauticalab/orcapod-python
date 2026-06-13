"""Source node hierarchy for Pipeline and PipelineJob.

SourceNode — schema-only input-slot declaration.
SourceJobNode — execution variant that wraps a concrete StreamProtocol.
Both share SourceNodeBase which provides schema-based identity.
"""
from __future__ import annotations

import functools
import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.config import OrcapodConfig
from orcapod.core.base import TraceableBase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.arrow_utils import system_tag_column_names
from orcapod.utils.schema_utils import compute_schema_hash

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.channels import WritableChannel
    from orcapod.protocols.core_protocols import StreamProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

logger = logging.getLogger(__name__)


class SourceNodeBase(TraceableBase, ABC):
    """Abstract base for SourceNode and SourceJobNode.

    Provides schema-based identity (content_hash, pipeline_hash) and
    shared properties.  Both sub-types carry identical schemas so their
    pipeline_hash() values always match; content_hash() diverges only
    when SourceJobNode has a concrete source bound.

    Args:
        name: The input-slot name used as the key in
            ``PipelineJob.bind(sources={name: source})``.
        tag_schema: Mapping of tag column names to Python types.
        data_schema: Mapping of data column names to Python types.
        data_context: Optional data context override.
        label: Optional display label override.
        config: Optional config override.
    """

    node_type = "source"

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        data_context: str | contexts.DataContext | None = None,
        label: str | None = None,
        config: OrcapodConfig | None = None,
    ) -> None:
        super().__init__(label=label, data_context=data_context, config=config)
        self._name = name
        self._tag_schema = tag_schema
        self._data_schema = data_schema

    # ------------------------------------------------------------------
    # Identity
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return the content identity: ``("source_node", name, tag_schema, data_schema)``."""
        return ("source_node", self._name, self._tag_schema, self._data_schema)

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity: ``(tag_schema, data_schema)`` (name-independent).

        Sources with identical schemas share the same DB table paths regardless
        of name.
        """
        return (self._tag_schema, self._data_schema)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """The input-slot name used as the key in ``PipelineJob.bind(sources={...})``."""
        return self._name

    def computed_label(self) -> str | None:
        """Resolve the node label to the slot name.

        Implements ``LabelableMixin.computed_label()`` so that ``self.label``
        resolves to the slot name without an explicit label assignment.

        Returns:
            The slot name.
        """
        return self.name

    @property
    def tag_schema(self) -> Schema:
        """Tag schema for this input slot."""
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot."""
        return self._data_schema

    @functools.cached_property
    def _schema_hash_str(self) -> str:
        """Cached schema hash used for system-tag column naming.

        Produces the same hash that ``SourceStreamBuilder`` embeds in system-tag
        column names, making it possible to predict those names from the declared
        schemas alone without requiring a live source. Computed once and cached
        since ``_tag_schema`` and ``_data_schema`` are immutable after construction.

        Returns:
            Hex string schema hash.
        """
        return compute_schema_hash(
            self._tag_schema,
            self._data_schema,
            self.data_context.semantic_hasher,
            self.orcapod_config.hashing.schema_n_char,
        )

    @property
    def node_uri(self) -> tuple[str, ...]:
        """Canonical URI tuple for this source node.

        Returns a tuple identifying this node as a named schema-only source slot.
        """
        return ("source_node", self._name)

    @property
    def producer(self) -> None:
        """Source nodes have no producer pod — they are root nodes.

        Returns:
            Always ``None``.
        """
        return None

    @property
    def upstreams(self) -> "tuple[StreamProtocol, ...]":
        """Source nodes have no upstream streams — they are root nodes.

        Returns:
            Always an empty tuple.
        """
        return ()

    @upstreams.setter
    def upstreams(self, value: "tuple[StreamProtocol, ...]") -> None:
        if len(value) != 0:
            raise ValueError("SourceNode upstreams must be empty")

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``.

        When ``columns.system_tags`` is ``True`` (or ``all_info=True``), the
        returned tag schema is extended with the two system-tag entries for this
        node's schema (both typed as ``str``). Their names are deterministic from
        the declared schemas and match what a concrete source with the same schema
        would produce.

        Other ``ColumnConfig`` flags (``meta``, ``context``, ``source``,
        ``content_hash``, ``sort_by_tags``) are no-ops at the node level —
        consistent with ``ArrowTableStream.output_schema()`` which also ignores them.

        Args:
            columns: Column selection config.
            all_info: If ``True``, equivalent to ``ColumnConfig(system_tags=True)``
                for this method.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)
        tag_schema = self._tag_schema
        if columns_config.system_tags:
            source_id_col, record_id_col = system_tag_column_names(self._schema_hash_str)
            tag_schema = Schema(
                {**dict(tag_schema), source_id_col: str, record_id_col: str}
            )
        return tag_schema, self._data_schema

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``.

        Derived from ``output_schema()`` to ensure the two methods are always
        consistent. See ``output_schema()`` for the full description of which
        ``ColumnConfig`` flags are honoured at the node level.

        Args:
            columns: Column selection config.
            all_info: If ``True``, include all available column groups.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        tag_schema, data_schema = self.output_schema(columns=columns, all_info=all_info)
        return tuple(tag_schema.keys()), tuple(data_schema.keys())

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self, source: "StreamProtocol") -> None:
        """Check that *source* is schema-compatible with this node's declared schema.

        Args:
            source: A concrete stream to validate.

        Raises:
            SourceSpecMismatchError: If schema columns don't match.
        """
        source_tag, source_data = source.output_schema()

        tag_issues: list[str] = []
        data_issues: list[str] = []

        spec_tag_cols = set(self._tag_schema.keys())
        src_tag_cols = set(source_tag.keys())
        if spec_tag_cols != src_tag_cols:
            missing = spec_tag_cols - src_tag_cols
            extra = src_tag_cols - spec_tag_cols
            if missing:
                tag_issues.append(f"missing tag columns: {sorted(missing)}")
            if extra:
                tag_issues.append(f"unexpected tag columns: {sorted(extra)}")

        spec_data_cols = set(self._data_schema.keys())
        src_data_cols = set(source_data.keys())
        if spec_data_cols != src_data_cols:
            missing = spec_data_cols - src_data_cols
            extra = src_data_cols - spec_data_cols
            if missing:
                data_issues.append(f"missing data columns: {sorted(missing)}")
            if extra:
                data_issues.append(f"unexpected data columns: {sorted(extra)}")

        if tag_issues or data_issues:
            raise SourceSpecMismatchError(
                f"SourceNode '{self._name}' is not compatible with the provided source. "
                + "; ".join(tag_issues + data_issues)
            )

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Materialize stream as a PyArrow Table.

        Delegates to the concrete source (SourceJobNode), or raises for
        schema-only SourceNode.

        Args:
            columns: Column selection config.
            all_info: If True, include all metadata columns.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        # Calling iter_data() will raise UnboundSourceError for SourceNode,
        # or delegate to concrete for SourceJobNode.
        # For SourceJobNode with a concrete source, delegate directly.
        raise UnboundSourceError(
            f"SourceNode '{self._name}' is not bound to a concrete source. "
            "Use PipelineJob.bind() to attach data before calling as_table()."
        )

    async def async_iter_data(self):
        """Asynchronous iterator over (tag, data) pairs.

        Yields:
            tuple[TagProtocol, DataProtocol]: A ``(tag, data)`` pair from the
            concrete source.  Raises before yielding anything when unbound.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        for pair in self.iter_data():
            yield pair

    # ------------------------------------------------------------------
    # Abstract
    # ------------------------------------------------------------------

    @abstractmethod
    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Yield ``(tag, data)`` pairs, or raise if data is unavailable."""
        ...

    def execute(
        self,
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> list[tuple[TagProtocol, DataProtocol]]:
        """Execute this source node: materialize and return data.

        Args:
            observer: Optional execution observer.

        Returns:
            List of (tag, data) tuples.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        node_label = self.label
        node_hash = self.content_hash().to_string()
        if observer is not None:
            observer.on_node_start(node_label, node_hash)
        result = list(self.iter_data())
        if observer is not None:
            observer.on_node_end(node_label, node_hash)
        return result

    async def async_execute(
        self,
        output: "WritableChannel[tuple[TagProtocol, DataProtocol]]",
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None:
        """Push all (tag, data) pairs to the output channel.

        Delegates to ``async_iter_data`` so that dynamic sources
        (e.g. ``PollingSource``) stream continuously without modification
        to this node.

        Args:
            output: Channel to write results to.
            observer: Optional execution observer.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        node_label = self.label
        node_hash = self.content_hash().to_string()
        try:
            if observer is not None:
                observer.on_node_start(node_label, node_hash)
            async for tag, data in self.async_iter_data():
                await output.send((tag, data))
            if observer is not None:
                observer.on_node_end(node_label, node_hash)
        finally:
            await output.close()

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self._name!r}, "
            f"tag_schema={dict(self._tag_schema)!r}, "
            f"data_schema={dict(self._data_schema)!r})"
        )


class SourceNode(SourceNodeBase):
    """Schema-only input-slot declaration for ``Pipeline`` recording.

    Replaces ``SourceSpec`` as the user-facing way to declare typed pipeline
    inputs.  Pass a ``SourceNode`` inside a ``with pipeline:`` block as the
    upstream for any pod invocation.

    Example::

        slot = SourceNode(name="data", tag_schema={"id": int}, data_schema={"v": float})
        with pipeline:
            result = my_pod(slot)

        job = PipelineJob.from_pipeline(pipeline, store=db, sources={"data": my_source})
        job.run()

    Hash-stability note:
        ``identity_structure()`` returns ``("SourceSpec", name, tag_schema, data_schema)``
        — identical to the old ``SourceSpec`` — so existing DB paths remain valid.
    """

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Raise ``UnboundSourceError`` — ``SourceNode`` carries no data.

        Raises:
            UnboundSourceError: Always.
        """
        raise UnboundSourceError(
            f"SourceNode '{self._name}' is not bound to a concrete source. "
            "Use PipelineJob.from_pipeline(..., sources={'<name>': source}) "
            "or job.bind(sources={'<name>': source}) to attach data."
        )

    @classmethod
    def from_stream(
        cls,
        stream: StreamProtocol,
        name: str | None = None,
    ) -> SourceNode:
        """Wrap *stream* in a ``SourceNode``, or return it unchanged if already one.

        Derives tag and data schemas from ``stream.output_schema()``.  The slot
        name is resolved in priority order:

        1. *name* parameter (explicit override — highest priority).
        2. ``stream.source_id`` when *stream* is a ``RootSource`` — the canonical
           source identity, stable across pipeline serialisation / deserialisation.
        3. ``"{stream.label}:{hash_prefix}"`` for any other concrete stream type,
           combining the cosmetic label with a short content-hash suffix to ensure
           uniqueness.

        Args:
            stream: The upstream stream to wrap.
            name: Optional explicit slot name.

        Returns:
            The original *stream* if it is already a ``SourceNode``; otherwise
            a new ``SourceNode`` with schemas derived from *stream*.
        """
        if isinstance(stream, SourceNode):
            return stream
        tag_schema, data_schema = stream.output_schema()
        if name is not None:
            slot_name = name
        else:
            # Local import avoids a module-level circular dependency between
            # core.nodes and core.sources.
            from orcapod.core.sources.base import RootSource
            if isinstance(stream, RootSource):
                slot_name = stream.source_id
            else:
                slot_name = f"{stream.label}:{stream.content_hash().to_string()}"
        return cls(
            name=slot_name,
            tag_schema=tag_schema,
            data_schema=data_schema,
        )


class SourceJobNode(SourceNodeBase):
    """Execution-ready source node wrapping an optional concrete stream.

    Used inside ``PipelineJob._persistent_node_map``.  The ``_concrete``
    field is **mutable** — ``PipelineJob.bind(sources={...})`` updates it
    in-place so that downstream ``FunctionJobNode`` objects (which hold a
    reference to this same object) automatically see the new concrete source
    without cascading reference updates.

    Hash behaviour:

    * ``content_hash()`` — delegates to the bound source's
      ``identity_structure()`` when bound (via ``identity_structure()``
      override); falls back to schema-based identity when unbound.
    * ``pipeline_hash()`` — always schema-based (inherited); never
      data-inclusive.  This invariant keeps DB paths stable across different
      data sources bound to the same slot.

    Args:
        name: Slot name.
        tag_schema: Tag schema.  Optional when *bound_source* is provided;
            inferred from ``bound_source.output_schema()`` when omitted.
            Stored for future rebind validation.
        data_schema: Data schema.  Optional when *bound_source* is provided;
            inferred from ``bound_source.output_schema()`` when omitted.
            Stored for future rebind validation.
        bound_source: Optional concrete stream.  Can be set or replaced later
            via ``job_node.bound_source = source``.
        data_context: Optional data context override.

    Raises:
        ValueError: If both *tag_schema* and *data_schema* are omitted and
            *bound_source* is ``None``.
    """

    def __init__(
        self,
        name: str,
        tag_schema: Schema | None = None,
        data_schema: Schema | None = None,
        bound_source: StreamProtocol | None = None,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        if tag_schema is None or data_schema is None:
            if bound_source is None:
                raise ValueError(
                    "SourceJobNode requires either explicit tag_schema/data_schema "
                    "or a bound_source to derive schemas from."
                )
            derived_tag, derived_data = bound_source.output_schema()
            if tag_schema is None:
                tag_schema = derived_tag
            if data_schema is None:
                data_schema = derived_data
        super().__init__(
            name=name,
            tag_schema=tag_schema,
            data_schema=data_schema,
            data_context=data_context,
        )
        # Direct assignment to the backing attribute — super().__init__() has
        # already initialised _content_hash_cache, so the property setter is
        # safe to use here; we bypass it only to make the init path explicit.
        self._bound_source: StreamProtocol | None = bound_source

    # ------------------------------------------------------------------
    # bound_source property — explicit binding with cache invalidation
    # ------------------------------------------------------------------

    @property
    def bound_source(self) -> StreamProtocol | None:
        """The concrete stream currently bound to this slot, or ``None``."""
        return self._bound_source

    @bound_source.setter
    def bound_source(self, value: StreamProtocol | None) -> None:
        """Bind *value* as the concrete source and invalidate both hash caches."""
        self._bound_source = value
        self._invalidate_content_hash_cache()
        self._invalidate_pipeline_hash_cache()

    # ------------------------------------------------------------------
    # Identity — delegate to bound source when set
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Delegate to ``bound_source.identity_structure()`` when bound.

        This is the correct extension point: content_hash() flows from
        identity_structure(), so overriding here avoids bypassing the
        caching and resolver logic in ContentIdentifiableBase.content_hash().

        Returns:
            Bound source's identity structure when bound; schema-based
            identity (inherited from SourceNodeBase) when unbound.
        """
        if self._bound_source is not None:
            return self._bound_source.identity_structure()
        return super().identity_structure()

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Delegate to concrete source, or raise if unbound.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._bound_source is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before running."
            )
        return self._bound_source.iter_data()

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return ``(tag_schema, data_schema)``, delegating to ``bound_source`` when set.

        When ``bound_source`` is present, this is a transparent pass-through to
        ``bound_source.output_schema(columns=columns, all_info=all_info)`` so callers
        get the same result as querying the source directly.

        When unbound, delegates to ``SourceNodeBase.output_schema()`` which includes
        system-tag schema entries derived from the declared schemas.

        Args:
            columns: Column selection config.
            all_info: If ``True``, include all available column groups.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        if self._bound_source is None:
            return super().output_schema(columns=columns, all_info=all_info)
        return self._bound_source.output_schema(columns=columns, all_info=all_info)

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Materialize the concrete source as a PyArrow Table.

        Args:
            columns: Column selection config.
            all_info: If True, include all metadata columns.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._bound_source is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before calling as_table()."
            )
        return self._bound_source.as_table(columns=columns, all_info=all_info)

    @classmethod
    def from_stream(
        cls,
        stream: StreamProtocol,
        name: str | None = None,
    ) -> "SourceJobNode":
        """Create a ``SourceJobNode`` from *stream* using three-way logic.

        The three cases are:

        1. *stream* is already a ``SourceJobNode`` — copy it, preserving the
           existing ``bound_source`` (the SJN itself is **not** used as the
           bound source).
        2. *stream* is a ``SourceNode`` (schema-only, no data) — create an
           **unbound** ``SourceJobNode`` with the same name and schemas. The
           resulting SJN has ``bound_source=None`` and the same
           ``content_hash()`` as *stream* (schema-based).
        3. Any other concrete stream (``ArrowTableSource``, etc.) — create a
           **bound** ``SourceJobNode`` whose ``content_hash()`` delegates to
           the concrete stream.

        Args:
            stream: The stream to wrap.
            name: Optional explicit slot name. In all three cases, when *name*
                is provided it takes precedence over the stream's own name.
                For concrete streams (Case 3), when *name* is ``None`` the slot
                name is resolved as: ``stream.source_id`` for ``RootSource``
                instances, or ``"{stream.label}:{hash_prefix}"`` for any other
                concrete stream type.

        Returns:
            A ``SourceJobNode`` configured according to the case above.
        """
        match stream:
            case SourceJobNode():
                # Case 1: copy — preserve bound_source, do NOT wrap the SJN itself.
                return cls(
                    name=name if name is not None else stream.name,
                    tag_schema=stream.tag_schema,
                    data_schema=stream.data_schema,
                    bound_source=stream.bound_source,
                )
            case SourceNode():
                # Case 2: unbound — schema placeholder; SJN hash = SourceNode hash.
                return cls(
                    name=name if name is not None else stream.name,
                    tag_schema=stream.tag_schema,
                    data_schema=stream.data_schema,
                    bound_source=None,
                )
            case _:
                # Case 3: concrete stream — bound SJN.
                # Slot name priority:
                #   1. explicit *name* parameter
                #   2. stream.source_id for RootSource (canonical, stable identity)
                #   3. label + short hash prefix for any other concrete stream type
                tag_schema, data_schema = stream.output_schema()
                if name is not None:
                    slot_name = name
                else:
                    # Local import avoids a module-level circular dependency between
                    # core.nodes and core.sources.
                    from orcapod.core.sources.base import RootSource
                    if isinstance(stream, RootSource):
                        slot_name = stream.source_id
                    else:
                        slot_name = f"{stream.label}:{stream.content_hash().to_string()}"
                return cls(
                    name=slot_name,
                    tag_schema=tag_schema,
                    data_schema=data_schema,
                    bound_source=stream,
                )

    def as_node(self) -> SourceNode:
        """Return the lightweight ``SourceNode`` equivalent of this job node.

        Returns:
            A new ``SourceNode`` with the same name and schemas.
        """
        return SourceNode(
            name=self._name,
            tag_schema=self._tag_schema,
            data_schema=self._data_schema,
        )
