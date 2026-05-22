"""Source node hierarchy for Pipeline and PipelineJob.

SourceNode — schema-only input-slot declaration (replaces SourceSpec).
SourceJobNode — execution variant that wraps a concrete StreamProtocol.
Both share SourceNodeBase which provides hash-stable identity.

Hash-stability guarantee:
    SourceNode(name=n, tag_schema=t, data_schema=d).content_hash()
    == SourceSpec(name=n, tag_schema=t, data_schema=d).content_hash()

This is achieved by using identical identity_structure():
    ("SourceSpec", name, tag_schema, data_schema)
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod import contexts
from orcapod.core.base import TraceableBase
from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.types import ColumnConfig, ContentHash, Schema

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
    """

    node_type = "source"

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        super().__init__(data_context=data_context)
        self._name = name
        self._tag_schema = tag_schema
        self._data_schema = data_schema

    # ------------------------------------------------------------------
    # Identity — hash-stable against old SourceSpec
    # ------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Return the content identity: ``("SourceSpec", name, tag_schema, data_schema)``.

        Deliberately matches ``SourceSpec.identity_structure()`` so that a
        ``SourceNode`` constructed with the same arguments as a ``SourceSpec``
        produces an identical ``content_hash()``.  This preserves all DB paths
        computed from pre-refactor pipelines.
        """
        return ("SourceSpec", self._name, self._tag_schema, self._data_schema)

    def pipeline_identity_structure(self) -> Any:
        """Return the pipeline identity: ``(tag_schema, data_schema)`` (name-independent).

        Matches ``RootSource.pipeline_identity_structure()`` so that sources
        with identical schemas share the same DB table paths regardless of name.
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
        return self._name

    @property
    def tag_schema(self) -> Schema:
        """Tag schema for this input slot."""
        return self._tag_schema

    @property
    def data_schema(self) -> Schema:
        """Data schema for this input slot."""
        return self._data_schema

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

        Args:
            columns: Ignored.
            all_info: Ignored.

        Returns:
            Tuple of ``(tag_schema, data_schema)``.
        """
        return (self._tag_schema, self._data_schema)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Return ``(tag_keys, data_keys)``.

        Args:
            columns: Ignored.
            all_info: Ignored.

        Returns:
            Tuple of ``(tag_column_names, data_column_names)``.
        """
        return (tuple(self._tag_schema.keys()), tuple(self._data_schema.keys()))

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
    ) -> "pa.Table":
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
        observer: "ExecutionObserverProtocol | None" = None,
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
        node_hash = ""
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
        observer: "ExecutionObserverProtocol | None" = None,
    ) -> None:
        """Push all (tag, data) pairs to the output channel.

        Args:
            output: Channel to write results to.
            observer: Optional execution observer.

        Raises:
            UnboundSourceError: When no concrete data is available.
        """
        node_label = self.label
        node_hash = ""
        try:
            if observer is not None:
                observer.on_node_start(node_label, node_hash)
            for tag, data in self.iter_data():
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


class SourceJobNode(SourceNodeBase):
    """Execution-ready source node wrapping an optional concrete stream.

    Used inside ``PipelineJob._persistent_node_map``.  The ``_concrete``
    field is **mutable** — ``PipelineJob.bind(sources={...})`` updates it
    in-place so that downstream ``FunctionJobNode`` objects (which hold a
    reference to this same object) automatically see the new concrete source
    without cascading reference updates.

    Hash behaviour:

    * ``content_hash()`` — delegates to ``_concrete.content_hash()`` when
      bound; falls back to schema-based ``SourceNodeBase.content_hash()`` (==
      ``SourceNode.content_hash()``) when unbound.
    * ``pipeline_hash()`` — always schema-based (inherited); never
      data-inclusive.  This invariant keeps DB paths stable across different
      data sources bound to the same slot.

    Args:
        name: Slot name.
        tag_schema: Tag schema.
        data_schema: Data schema.
        concrete: Optional concrete stream.  Can be set or replaced later via
            ``job_node._concrete = source``.
        data_context: Optional data context override.
    """

    def __init__(
        self,
        name: str,
        tag_schema: Schema,
        data_schema: Schema,
        concrete: "StreamProtocol | None" = None,
        data_context: str | contexts.DataContext | None = None,
    ) -> None:
        super().__init__(
            name=name,
            tag_schema=tag_schema,
            data_schema=data_schema,
            data_context=data_context,
        )
        # Use object.__setattr__ to bypass the property setter during __init__
        # (the cache doesn't exist yet at this point).
        object.__setattr__(self, "_concrete", concrete)

    def __setattr__(self, name: str, value: object) -> None:
        """Clear ``_content_hash_cache`` whenever ``_concrete`` is mutated.

        This prevents a stale schema-based hash from being returned by the
        parent ``content_hash()`` cache after the concrete source is updated.
        """
        object.__setattr__(self, name, value)
        if name == "_concrete" and hasattr(self, "_content_hash_cache"):
            self._content_hash_cache.clear()

    def content_hash(self, hasher=None) -> ContentHash:
        """Return data-inclusive hash when bound; schema-based hash when unbound.

        Args:
            hasher: Optional semantic hasher.

        Returns:
            ``_concrete.content_hash(hasher)`` when bound, otherwise
            ``SourceNodeBase.content_hash(hasher)``.
        """
        if self._concrete is not None:
            if hasher is None:
                hasher = self.data_context.semantic_hasher
            return self._concrete.content_hash(hasher)
        return super().content_hash(hasher)

    def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
        """Delegate to concrete source, or raise if unbound.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._concrete is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before running."
            )
        return self._concrete.iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        """Materialize the concrete source as a PyArrow Table.

        Args:
            columns: Column selection config.
            all_info: If True, include all metadata columns.

        Raises:
            UnboundSourceError: When no concrete source is attached.
        """
        if self._concrete is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={'<name>': source}) before calling as_table()."
            )
        return self._concrete.as_table(columns=columns, all_info=all_info)

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
