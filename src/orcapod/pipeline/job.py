"""PipelineJob — pipeline + source bindings + execution context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    from orcapod.core.nodes import FunctionNode, GraphNode, OperatorNode, SourceNode
    from orcapod.core.sources.source_spec import SourceSpec
    from orcapod.pipeline.execution_context import ExecutionContext
    from orcapod.pipeline.graph import Pipeline
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


class PipelineJob(AutoRegisteringContextBasedTracker):
    """Pipeline + source bindings + execution context.

    ``PipelineJob`` is the everyday working object. It is built incrementally:
    its ``with``-block records both the DAG structure and any concrete source
    bindings simultaneously. Concrete sources are automatically promoted to
    ``SourceSpec`` declarations in the underlying ``Pipeline``, with their
    concrete instances stored in ``job.sources``.

    After the ``with`` block, ``job.pipeline`` is a fully compiled, pure
    ``Pipeline`` (SourceSpec-only leaves). ``job.run()`` executes the
    resolvable subgraph — nodes whose upstream SourceSpecs are all bound.

    ``PipelineJob`` can also be created from a ``Pipeline`` via
    ``pipeline.bind(sources=..., store=...)`` for the "explicit blueprint"
    workflow.

    Args:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run. Defaults to
            ``"pipeline"``.
        store: Database for result caching and operator records.
        execution_context: Optional execution configuration.
        tracker_manager: Optional tracker manager override.
        _pipeline: Internal — pre-built pipeline (used by Pipeline.bind()).
        sources: Internal — pre-bound sources (used by Pipeline.bind() /
            bind()).
    """

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        *,
        _pipeline: "Pipeline | None" = None,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)
        self._store = store
        self._execution_context = execution_context
        self._compiled_pipeline: "Pipeline | None" = _pipeline
        self._sources: dict[str, cp.StreamProtocol] = dict(sources or {})

        # Recording state (populated during with-block)
        self._rec_graph_edges: list[tuple[str, str]] = []
        self._rec_upstreams: dict[str, cp.StreamProtocol] = {}
        self._rec_node_lut: dict[str, "GraphNode"] = {}
        self._spec_by_name: dict[str, "SourceSpec"] = {}
        self._pipeline_name: tuple[str, ...] = (name,) if isinstance(name, str) else tuple(name)

    # ------------------------------------------------------------------
    # Context manager — recording
    # ------------------------------------------------------------------

    def __enter__(self) -> "PipelineJob":
        # Reset recording state
        self._rec_graph_edges = []
        self._rec_upstreams = {}
        self._rec_node_lut = {}
        self._spec_by_name = {}
        return super().__enter__()  # type: ignore[return-value]

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self._compile_from_recording()

    def _compile_from_recording(self) -> None:
        """Compile the recorded edges into a pure Pipeline."""
        import networkx as nx

        from orcapod.pipeline.graph import Pipeline

        pipeline = Pipeline(name=self._pipeline_name, auto_compile=False)
        # Inject the recording state into the pipeline
        pipeline._graph_edges = list(self._rec_graph_edges)
        pipeline._upstreams = dict(self._rec_upstreams)
        pipeline._node_lut = dict(self._rec_node_lut)
        # Rebuild hash graph from edges
        for edge in self._rec_graph_edges:
            pipeline._hash_graph.add_edge(*edge)

        # Annotate node_type on each recorded node (function/operator).
        for node_hash, node in self._rec_node_lut.items():
            if node_hash in pipeline._hash_graph.nodes:
                pipeline._hash_graph.nodes[node_hash]["node_type"] = node.node_type
                if node.label:
                    pipeline._hash_graph.nodes[node_hash]["label"] = node.label

        # Annotate upstream (source) nodes that are not in _rec_node_lut.
        for node_hash, stream in self._rec_upstreams.items():
            if node_hash in pipeline._hash_graph.nodes:
                if not pipeline._hash_graph.nodes[node_hash].get("node_type"):
                    pipeline._hash_graph.nodes[node_hash]["node_type"] = "source"

        pipeline.compile()
        self._compiled_pipeline = pipeline

    def _ensure_spec(self, source: cp.StreamProtocol) -> "SourceSpec":
        """Promote *source* to a SourceSpec, storing the concrete binding.

        If the spec already exists (same label/hash key), returns the cached spec.

        When a source has an explicitly assigned label, that label is used as the
        SourceSpec name. When no label is assigned (the source falls back to its
        class name), the source's content hash is used to ensure uniqueness.
        """
        from orcapod.core.sources.source_spec import SourceSpec

        # Use explicit label when set; otherwise fall back to content hash
        # to avoid two unlabeled sources getting the same spec name.
        has_label = source.has_assigned_label
        if has_label:
            name = source.label  # type: ignore[attr-defined]
        else:
            name = source.content_hash().to_string()

        if name not in self._spec_by_name:
            tag_schema, data_schema = source.output_schema()
            spec = SourceSpec(name=name, tag_schema=tag_schema, data_schema=data_schema)
            self._spec_by_name[name] = spec
            self._sources[name] = source
        return self._spec_by_name[name]

    @staticmethod
    def _is_concrete_source(stream: cp.StreamProtocol) -> bool:
        """True if *stream* is a concrete RootSource (not a SourceSpec)."""
        from orcapod.core.sources.base import RootSource
        from orcapod.core.sources.source_spec import SourceSpec

        return isinstance(stream, RootSource) and not isinstance(stream, SourceSpec)

    # ------------------------------------------------------------------
    # TrackerProtocol — recording with source interception
    # ------------------------------------------------------------------

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation, promoting concrete sources to specs.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream (concrete source or spec).
            label: Optional label for the resulting node.
        """
        from orcapod.core.nodes import FunctionNode

        if self._is_concrete_source(input_stream):
            input_stream = self._ensure_spec(input_stream)

        input_hash = input_stream.content_hash().to_string()
        function_node = FunctionNode(function_pod=pod, input_stream=input_stream, label=label)
        fn_hash = function_node.content_hash().to_string()

        self._rec_node_lut[fn_hash] = function_node
        self._rec_upstreams[input_hash] = input_stream
        self._rec_graph_edges.append((input_hash, fn_hash))

    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation, promoting concrete sources to specs.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams (concrete sources or specs).
            label: Optional label for the resulting node.
        """
        from orcapod.core.nodes import OperatorNode

        processed = tuple(
            self._ensure_spec(s) if self._is_concrete_source(s) else s
            for s in upstreams
        )

        operator_node = OperatorNode(operator=pod, input_streams=processed, label=label)
        op_hash = operator_node.content_hash().to_string()

        self._rec_node_lut[op_hash] = operator_node
        for upstream in processed:
            up_hash = upstream.content_hash().to_string()
            self._rec_upstreams[up_hash] = upstream
            self._rec_graph_edges.append((up_hash, op_hash))

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pipeline(self) -> "Pipeline":
        """The compiled pure Pipeline (SourceSpec-only leaves).

        Raises:
            RuntimeError: If the with-block has not been completed yet.
        """
        if self._compiled_pipeline is None:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline yet. "
                "Use 'with job:' to record a DAG first."
            )
        return self._compiled_pipeline

    @property
    def sources(self) -> dict[str, cp.StreamProtocol]:
        """Mapping of SourceSpec name to bound concrete source."""
        return dict(self._sources)

    @property
    def store(self) -> "ArrowDatabaseProtocol | None":
        """The database used for result caching."""
        return self._store

    @property
    def execution_context(self) -> "ExecutionContext | None":
        """The execution configuration, or ``None`` if unset."""
        return self._execution_context

    # ------------------------------------------------------------------
    # bind() — non-mutating
    # ------------------------------------------------------------------

    def bind(
        self,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
    ) -> "PipelineJob":
        """Return a new ``PipelineJob`` with updated bindings.

        Non-mutating — the original ``PipelineJob`` is unchanged. Existing
        bindings not mentioned in this call are carried forward.

        ``SourceSpec.validate()`` is called for each source in *sources*;
        ``SourceSpecMismatchError`` is raised on schema mismatch.

        Args:
            sources: Mapping of SourceSpec name to concrete source. Each
                source is validated against the matching SourceSpec.
            store: Replaces the current store.
            execution_context: Replaces the current execution context.

        Returns:
            A new ``PipelineJob`` with merged bindings.

        Raises:
            SourceSpecMismatchError: If any source's schema is incompatible.
        """
        from orcapod.core.nodes import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        merged_sources = dict(self._sources)
        if sources is not None:
            # Validate each supplied source against its SourceSpec
            pipeline = self._compiled_pipeline
            if pipeline is not None:
                for node in pipeline._persistent_node_map.values():
                    if (
                        isinstance(node, SourceNode)
                        and isinstance(node.stream, SourceSpec)
                        and node.stream.name in sources
                    ):
                        node.stream.validate(sources[node.stream.name])
            merged_sources.update(sources)

        return PipelineJob(
            name=self._pipeline_name,
            store=store if store is not None else self._store,
            execution_context=execution_context if execution_context is not None else self._execution_context,
            _pipeline=self._compiled_pipeline,
            sources=merged_sources,
        )

    # ------------------------------------------------------------------
    # Completeness introspection
    # ------------------------------------------------------------------

    def unbound_specs(self) -> "list[SourceSpec]":
        """Return all SourceSpec slots not yet bound in this job.

        Returns:
            List of unbound ``SourceSpec`` instances, in order of appearance
            in the pipeline graph.
        """
        from orcapod.core.nodes import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        if self._compiled_pipeline is None:
            return []

        unbound = []
        seen: set[str] = set()
        for node in self._compiled_pipeline._persistent_node_map.values():
            if (
                isinstance(node, SourceNode)
                and isinstance(node.stream, SourceSpec)
                and node.stream.name not in self._sources
                and node.stream.name not in seen
            ):
                unbound.append(node.stream)
                seen.add(node.stream.name)
        return unbound

    def is_complete(self) -> bool:
        """Return ``True`` when all specs are bound and a store is set.

        Returns:
            ``True`` if all SourceSpec slots are bound and a store is set.
        """
        return self._store is not None and len(self.unbound_specs()) == 0

    def is_runnable(self, node_label: str) -> bool:
        """Return ``True`` if all upstream inputs of *node_label* are resolved.

        Args:
            node_label: Label of the node to check.

        Returns:
            ``True`` if the node can be executed with current bindings.
        """
        from orcapod.core.nodes import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        pipeline = self._compiled_pipeline
        if pipeline is None:
            return False

        target = pipeline._nodes.get(node_label)
        if target is None:
            return False

        if pipeline._node_graph is None:
            return False

        import networkx as nx

        for node in nx.ancestors(pipeline._node_graph, target) | {target}:
            if (
                isinstance(node, SourceNode)
                and isinstance(node.stream, SourceSpec)
                and node.stream.name not in self._sources
            ):
                return False
        return True

    def __repr__(self) -> str:
        n_sources = len(self._sources)
        has_store = self._store is not None
        has_pipeline = self._compiled_pipeline is not None
        return (
            f"PipelineJob(sources={n_sources}, store={has_store}, "
            f"pipeline={'compiled' if has_pipeline else 'unrecorded'})"
        )
