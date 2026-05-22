"""PipelineJob — pipeline + source bindings + execution context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.base import AbstractPipelineBase
from orcapod.protocols import core_protocols as cp
from orcapod.types import CacheMode
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    from orcapod.core.nodes import FunctionNode, GraphNode, OperatorNode
    from orcapod.core.nodes.source_node import SourceNode
    from orcapod.pipeline.execution_context import ExecutionContext
    from orcapod.pipeline.graph import Pipeline
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


class PipelineJob(AbstractPipelineBase):
    """Pipeline + source bindings + execution context.

    ``PipelineJob`` is the everyday working object. It is built incrementally:
    its ``with``-block records both the DAG structure and any concrete source
    bindings simultaneously. Concrete sources are automatically promoted to
    ``SourceNode`` declarations in the underlying ``Pipeline``, with their
    concrete instances stored in ``job.sources``.

    After the ``with`` block, ``job.pipeline`` is a fully compiled, pure
    ``Pipeline`` (SourceNode-only leaves). ``job.run()`` executes the
    resolvable subgraph — nodes whose upstream SourceNodes are all bound.

    ``PipelineJob`` can also be created from a ``Pipeline`` via
    ``PipelineJob.from_pipeline(pipeline, sources=..., store=...)`` for the "explicit blueprint"
    workflow.

    Args:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run. Defaults to
            ``"pipeline"``.
        store: Database for result caching and operator records.
        execution_context: Optional execution configuration.
        tracker_manager: Optional tracker manager override.
        _pipeline: Internal — pre-built pipeline (used by PipelineJob.from_pipeline()).
        sources: Internal — pre-bound sources (used by PipelineJob.from_pipeline() /
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
        super().__init__(name=name, tracker_manager=tracker_manager)
        self._store = store
        self._execution_context = execution_context
        self._compiled_pipeline: "Pipeline | None" = _pipeline
        self._sources: dict[str, cp.StreamProtocol] = dict(sources or {})

        # Recording state (populated during with-block)
        self._rec_graph_edges: list[tuple[str, str]] = []
        self._rec_upstreams: dict[str, cp.StreamProtocol] = {}
        self._rec_node_lut: dict[str, "GraphNode"] = {}
        self._spec_by_name: dict[str, "SourceNode"] = {}
        self._unresolved_specs: list[str] = []
        self._has_run: bool = False
        self._run_id: str | None = None

        # Job-node map (populated by from_pipeline(); None for with-block-created jobs)
        # Note: _persistent_node_map and _nodes are initialized by AbstractPipelineBase.__init__
        # but overridden here for PipelineJob-specific types.
        self._persistent_node_map: "dict[str, Any] | None" = None
        self._nodes: "dict[str, Any]" = {}

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

    def compile(self) -> None:
        """Compile recorded invocations into a Pipeline (implements AbstractPipelineBase.compile)."""
        self._compile_from_recording()

    def _compile_from_recording(self) -> None:
        """Compile the recorded edges into a pure Pipeline and build the job node map.

        ``_rec_node_lut`` now contains ``FunctionJobNode`` / ``OperatorJobNode`` objects
        (set by ``record_function_pod_invocation`` / ``record_operator_pod_invocation``).
        This method:

        1. Converts each recorded job node to its lightweight blueprint counterpart via
           ``.as_node()`` and injects the result into ``pipeline._node_lut`` so that
           ``Pipeline.compile()`` sees only ``FunctionNode`` / ``OperatorNode`` objects.
        2. After compiling the blueprint pipeline, walks it topologically to build
           ``self._persistent_node_map`` using ``SourceJobNode`` for leaf nodes
           (concrete sources are taken from ``self._sources``) and fresh
           ``FunctionJobNode`` / ``OperatorJobNode`` objects rewired to upstream job nodes
           for non-leaf nodes.
        """
        import networkx as _nx
        from orcapod.pipeline.graph import Pipeline
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNodeBase
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNodeBase
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNodeBase

        pipeline = Pipeline(name=self._name, auto_compile=False)
        # Inject the recording state into the pipeline, converting job nodes → blueprint nodes
        pipeline._graph_edges = list(self._rec_graph_edges)
        pipeline._upstreams = dict(self._rec_upstreams)
        pipeline._node_lut = {
            h: node.as_node() for h, node in self._rec_node_lut.items()
        }
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

        # Build PipelineJob's own job node map walking compiled pipeline topologically.
        # Leaf nodes (SourceNode) become SourceJobNode with concrete binding from _sources.
        # Non-leaf nodes are fresh FunctionJobNode/OperatorJobNode rewired to upstream job nodes.
        G = pipeline._hash_graph
        job_node_map: dict[str, object] = {}

        for node_hash in _nx.topological_sort(G):
            if node_hash not in pipeline._persistent_node_map:
                continue

            bp_node = pipeline._persistent_node_map[node_hash]

            if isinstance(bp_node, SourceNodeBase):
                concrete = self._sources.get(bp_node.name)
                job_node: object = SourceJobNode(
                    name=bp_node.name,
                    tag_schema=bp_node.tag_schema,
                    data_schema=bp_node.data_schema,
                    concrete=concrete,
                )
            elif isinstance(bp_node, FunctionNodeBase):
                # Create fresh FunctionJobNode rewired to the upstream job node.
                rec_node = self._rec_node_lut[node_hash]
                original_input_hash = bp_node._input_stream.content_hash().to_string()
                upstream_job_node = job_node_map[original_input_hash]
                job_node = FunctionJobNode(
                    function_pod=rec_node._function_pod,
                    input_stream=upstream_job_node,
                    label=rec_node._label,
                    table_scope=rec_node._table_scope,
                    tracker_manager=rec_node.tracker_manager,
                )
            elif isinstance(bp_node, OperatorNodeBase):
                rec_node = self._rec_node_lut[node_hash]
                upstream_job_nodes = tuple(
                    job_node_map[s.content_hash().to_string()]
                    for s in bp_node._input_streams
                )
                job_node = OperatorJobNode(
                    operator=rec_node._operator,
                    input_streams=upstream_job_nodes,
                    label=rec_node._label,
                    table_scope=rec_node._table_scope,
                    tracker_manager=rec_node.tracker_manager,
                )
            else:
                raise TypeError(
                    f"Unknown blueprint node type in compiled pipeline: {type(bp_node)}"
                )

            job_node_map[node_hash] = job_node

        self._persistent_node_map = job_node_map

        # Build label → job node map from pipeline._nodes
        self._nodes = {
            label: job_node_map[node.content_hash().to_string()]
            for label, node in pipeline._nodes.items()
            if node.content_hash().to_string() in job_node_map
        }

        # Wire databases if store is already set
        if self._store is not None:
            self._distribute_databases()

    def _ensure_source_node(self, source: cp.StreamProtocol) -> "SourceNode":
        """Promote *source* to a SourceNode, storing the concrete binding.

        If the node already exists (same label/hash key), returns the cached node.

        When a source has an explicitly assigned label, that label is used as the
        SourceNode name. When no label is assigned (the source falls back to its
        class name), the source's content hash is used to ensure uniqueness.
        """
        from orcapod.core.nodes.source_node import SourceNode

        # Use explicit label when set; otherwise fall back to content hash
        # to avoid two unlabeled sources getting the same node name.
        has_label = source.has_assigned_label
        if has_label:
            name = source.label  # type: ignore[attr-defined]
        else:
            name = source.content_hash().to_string()

        if name not in self._spec_by_name:
            tag_schema, data_schema = source.output_schema()
            node = SourceNode(name=name, tag_schema=tag_schema, data_schema=data_schema)
            self._spec_by_name[name] = node
            self._sources[name] = source
        return self._spec_by_name[name]

    @staticmethod
    def _is_concrete_source(stream: cp.StreamProtocol) -> bool:
        """True if *stream* is a concrete RootSource (not a SourceNode)."""
        from orcapod.core.sources.base import RootSource
        from orcapod.core.nodes.source_node import SourceNode

        return isinstance(stream, RootSource) and not isinstance(stream, SourceNode)

    # ------------------------------------------------------------------
    # TrackerProtocol — recording with source interception
    # ------------------------------------------------------------------

    def _to_node_stream(self, stream: cp.StreamProtocol) -> cp.StreamProtocol:
        """Convert *stream* to a node-based equivalent for consistent hash recording.

        Concrete ``RootSource`` instances are promoted to ``SourceNode`` via
        ``_ensure_source_node``. ``DynamicPodStream`` instances have their upstreams
        recursively converted so that their content hash matches the
        ``OperatorNode`` recorded in ``_rec_node_lut``.

        Args:
            stream: The upstream stream to convert.

        Returns:
            A node-based stream with a stable hash for recording.
        """
        from orcapod.core.operators.static_output_pod import DynamicPodStream

        if self._is_concrete_source(stream):
            return self._ensure_source_node(stream)
        if isinstance(stream, DynamicPodStream):
            node_upstreams = tuple(self._to_node_stream(s) for s in stream.upstreams)
            return DynamicPodStream(
                pod=stream._pod,
                upstreams=node_upstreams,
                label=stream._label,
            )
        return stream

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
        from orcapod.core.nodes.function_node import FunctionJobNode

        input_stream = self._to_node_stream(input_stream)

        input_hash = input_stream.content_hash().to_string()
        function_node = FunctionJobNode(function_pod=pod, input_stream=input_stream, label=label)
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
        from orcapod.core.nodes.operator_node import OperatorJobNode

        processed = tuple(self._to_node_stream(s) for s in upstreams)

        operator_node = OperatorJobNode(operator=pod, input_streams=processed, label=label)
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
        """The compiled pure Pipeline (SourceNode-only leaves).

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
        """Mapping of SourceNode name to bound concrete source."""
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
    # from_pipeline() — classmethod constructor
    # ------------------------------------------------------------------

    @classmethod
    def from_pipeline(
        cls,
        pipeline: "Pipeline",
        store: "ArrowDatabaseProtocol | None" = None,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        execution_context: "ExecutionContext | None" = None,
    ) -> "PipelineJob":
        """Create a runnable ``PipelineJob`` from a compiled ``Pipeline``.

        Walks the pipeline's ``_persistent_node_map`` topologically and
        creates corresponding ``JobNode`` variants:

        * ``SourceNode`` → ``SourceJobNode(name, schemas, concrete=sources.get(name))``
        * ``FunctionNode`` → ``FunctionJobNode(function_pod, upstream_job_node, label)``
        * ``OperatorNode`` → ``OperatorJobNode(operator, upstream_job_nodes, label)``

        Args:
            pipeline: A compiled ``Pipeline`` (``pipeline._compiled`` must be ``True``).
            store: Database for result caching and operator records.
            sources: Mapping of ``SourceNode.name`` → concrete source.
            execution_context: Optional execution configuration.

        Returns:
            A new ``PipelineJob`` ready to run (or ``bind()`` further).

        Raises:
            ValueError: If *pipeline* has not been compiled.
        """
        import networkx as _nx
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode, FunctionNodeBase
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode, OperatorNodeBase
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode, SourceNodeBase

        if not pipeline._compiled:
            raise ValueError(
                "Pipeline must be compiled before creating a PipelineJob from it. "
                "Call pipeline.compile() or use auto_compile=True."
            )

        bound_sources: dict[str, cp.StreamProtocol] = dict(sources or {})

        # Validate sources against SourceNode schemas (mirrors bind() validation).
        if bound_sources:
            spec_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(bound_sources.keys()) - spec_names
            if unknown:
                raise ValueError(
                    f"from_pipeline() received source keys with no matching SourceNode: "
                    f"{sorted(unknown)}. Known names: {sorted(spec_names)}"
                )
            for node in pipeline._persistent_node_map.values():
                if isinstance(node, SourceNodeBase) and node.name in bound_sources:
                    node.validate(bound_sources[node.name])

        G = pipeline._hash_graph
        job_node_map: dict[str, object] = {}

        for node_hash in _nx.topological_sort(G):
            if node_hash not in pipeline._persistent_node_map:
                continue

            node = pipeline._persistent_node_map[node_hash]

            if isinstance(node, SourceNodeBase):
                # Handles both SourceNode (blueprint) and SourceJobNode (loaded pipeline)
                concrete = bound_sources.get(node.name)
                job_node = SourceJobNode(
                    name=node.name,
                    tag_schema=node.tag_schema,
                    data_schema=node.data_schema,
                    concrete=concrete,
                )

            elif isinstance(node, FunctionNodeBase):
                # Handles both FunctionNode (blueprint) and FunctionJobNode (loaded pipeline)
                original_input_hash = node._input_stream.content_hash().to_string()
                upstream_job_node = job_node_map[original_input_hash]
                job_node = FunctionJobNode(
                    function_pod=node._function_pod,
                    input_stream=upstream_job_node,
                    label=node._label,
                    table_scope=node._table_scope,
                    tracker_manager=node.tracker_manager,
                )

            elif isinstance(node, OperatorNodeBase):
                # Handles both OperatorNode (blueprint) and OperatorJobNode (loaded pipeline)
                upstream_job_nodes = tuple(
                    job_node_map[s.content_hash().to_string()]
                    for s in node._input_streams
                )
                job_node = OperatorJobNode(
                    operator=node._operator,
                    input_streams=upstream_job_nodes,
                    label=node._label,
                    table_scope=node._table_scope,
                    tracker_manager=node.tracker_manager,
                )

            else:
                raise TypeError(
                    f"Unknown node type in pipeline._persistent_node_map: {type(node)}"
                )

            job_node_map[node_hash] = job_node

        # Construct the PipelineJob using __new__ to bypass __init__
        job = cls.__new__(cls)
        super(PipelineJob, job).__init__()
        job._store = store
        job._execution_context = execution_context
        job._sources = bound_sources
        job._name = pipeline._name
        job._has_run = False
        job._run_id = None
        job._unresolved_specs = []
        job._rec_graph_edges = []
        job._rec_upstreams = {}
        job._rec_node_lut = {}
        job._spec_by_name = {}

        job._compiled_pipeline = pipeline
        job._persistent_node_map = job_node_map
        job._nodes = {}

        for label, node in pipeline._nodes.items():
            node_hash = node.content_hash().to_string()
            if node_hash in job_node_map:
                job._nodes[label] = job_node_map[node_hash]

        if store is not None:
            job._distribute_databases()

        return job

    # ------------------------------------------------------------------
    # bind() — mutating
    # ------------------------------------------------------------------

    def bind(
        self,
        sources: "dict[str, cp.StreamProtocol] | None" = None,
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
    ) -> None:
        """Update bindings in place. Returns ``None``.

        Mutating — modifies ``self`` directly. Existing bindings not mentioned
        in this call are preserved.

        When *sources* is provided, each concrete source is validated against
        its matching ``SourceNode`` slot schema, then the corresponding
        ``SourceJobNode._concrete`` is updated in-place.

        When *store* is provided and differs from the current store,
        ``_distribute_databases()`` is called so that all job nodes receive
        live DB references immediately.

        Args:
            sources: Mapping of ``SourceNode.name`` → concrete source.
            store: Replaces the current store and triggers DB redistribution.
            execution_context: Replaces the current execution context.

        Raises:
            SourceSpecMismatchError: If any source's schema is incompatible.
            ValueError: If a source key has no matching ``SourceNode`` slot.
        """
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        store_changed = store is not None and store is not self._store

        if store is not None:
            self._store = store

        if sources is not None:
            pipeline = self._compiled_pipeline
            if pipeline is not None:
                spec_names = {
                    node.name
                    for node in pipeline._persistent_node_map.values()
                    if isinstance(node, SourceNode)
                }
                unknown = set(sources.keys()) - spec_names
                if unknown:
                    raise ValueError(
                        f"bind() received source keys with no matching SourceNode: "
                        f"{sorted(unknown)}. Known names: {sorted(spec_names)}"
                    )
                for node in pipeline._persistent_node_map.values():
                    if isinstance(node, SourceNode) and node.name in sources:
                        node.validate(sources[node.name])

            for job_node in (self._persistent_node_map or {}).values():
                if isinstance(job_node, SourceJobNode) and job_node.name in sources:
                    job_node._concrete = sources[job_node.name]

            self._sources.update(sources)

        if execution_context is not None:
            self._execution_context = execution_context

        if store_changed:
            self._distribute_databases()

    # ------------------------------------------------------------------
    # _distribute_databases()
    # ------------------------------------------------------------------

    def _distribute_databases(self) -> None:
        """Wire live DB references to all FunctionJobNode and OperatorJobNode objects.

        Called by ``bind()`` when *store* is changed and by ``from_pipeline()``
        when *store* is provided at construction time.

        Raises:
            RuntimeError: If ``_store`` is not set.
        """
        from orcapod.core.nodes.function_node import FunctionJobNode
        from orcapod.core.nodes.operator_node import OperatorJobNode
        from orcapod.types import CacheMode

        if self._store is None:
            raise RuntimeError(
                "Cannot distribute databases: no store is set. "
                "Call bind(store=...) or from_pipeline(..., store=...) first."
            )

        pipeline = self._compiled_pipeline
        pipeline_name = pipeline.name if pipeline is not None else self._name
        pipeline_db = self._store.at(*pipeline_name)
        result_db = pipeline_db.at("_result")

        for node in (self._persistent_node_map or {}).values():
            if isinstance(node, FunctionJobNode):
                node.attach_databases(
                    pipeline_database=pipeline_db,
                    result_database=result_db,
                )
            elif isinstance(node, OperatorJobNode):
                op_cache_mode = getattr(node, "_cache_mode", None) or CacheMode.OFF
                node.attach_databases(
                    pipeline_database=pipeline_db,
                    cache_mode=op_cache_mode,
                )

    # ------------------------------------------------------------------
    # as_pipeline()
    # ------------------------------------------------------------------

    def as_pipeline(self) -> "Pipeline":
        """Return the lightweight ``Pipeline`` blueprint for this job.

        Walks ``_persistent_node_map`` topologically and rewires each job node
        to a fresh blueprint node whose upstreams point at the already-built
        blueprint nodes in ``node_map`` (not at job nodes). This ensures that
        the content_hash of every node in the returned ``Pipeline`` matches its
        key in ``_persistent_node_map``.

        Returns:
            A compiled ``Pipeline`` whose ``_persistent_node_map`` contains
            only lightweight ``SourceNode`` / ``FunctionNode`` / ``OperatorNode``
            objects with blueprint (non-job) upstream references.

        Raises:
            RuntimeError: If this job has no compiled pipeline.
        """
        import networkx as _nx
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.pipeline.graph import Pipeline

        if self._compiled_pipeline is None:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline. "
                "Either use 'with job:' to record a DAG, "
                "or create the job via PipelineJob.from_pipeline()."
            )

        G = self._compiled_pipeline._hash_graph
        persistent = self._persistent_node_map or {}
        node_map: dict[str, object] = {}

        # Build a reverse lookup from Python object identity to blueprint hash so
        # that FunctionJobNode._input_stream and OperatorJobNode._input_streams
        # (which are themselves job nodes) can be mapped to their blueprint-hash keys.
        job_id_to_bp_hash: dict[int, str] = {
            id(job_node): bp_hash for bp_hash, job_node in persistent.items()
        }

        for node_hash in _nx.topological_sort(G):
            if node_hash not in persistent:
                continue
            job_node = persistent[node_hash]

            if isinstance(job_node, SourceJobNode):
                # SourceNode has no upstream — as_node() is safe as-is.
                node_map[node_hash] = job_node.as_node()

            elif isinstance(job_node, FunctionJobNode):
                # Wire _input_stream to the already-built blueprint upstream so
                # the resulting FunctionNode.content_hash() == node_hash.
                upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
                node_map[node_hash] = FunctionNode(
                    function_pod=job_node._function_pod,
                    input_stream=node_map[upstream_bp_hash],
                    label=job_node._label,
                    table_scope=job_node._table_scope,
                    tracker_manager=job_node.tracker_manager,
                )

            elif isinstance(job_node, OperatorJobNode):
                # Preserve original _input_streams order (important for non-commutative
                # operators such as SemiJoin) via the object-identity reverse lookup.
                blueprint_upstreams = tuple(
                    node_map[job_id_to_bp_hash[id(s)]]
                    for s in job_node._input_streams
                )
                node_map[node_hash] = OperatorNode(
                    operator=job_node._operator,
                    input_streams=blueprint_upstreams,
                    label=job_node._label,
                    table_scope=job_node._table_scope,
                    tracker_manager=job_node.tracker_manager,
                )

            else:
                # Fallback for any future node types — may not rewire upstreams.
                node_map[node_hash] = job_node.as_node()

        pipeline = Pipeline(name=self._name, auto_compile=False)
        pipeline._graph_edges = list(self._compiled_pipeline._graph_edges)
        pipeline._upstreams = dict(self._compiled_pipeline._upstreams)
        pipeline._node_lut = dict(self._compiled_pipeline._node_lut)
        pipeline._hash_graph = self._compiled_pipeline._hash_graph
        pipeline._persistent_node_map = node_map
        pipeline._nodes = {
            label: node_map[node.content_hash().to_string()]
            for label, node in self._compiled_pipeline._nodes.items()
            if node.content_hash().to_string() in node_map
        }
        pipeline._compiled = True

        return pipeline

    # ------------------------------------------------------------------
    # Completeness introspection
    # ------------------------------------------------------------------

    def unbound_source_nodes(self) -> "list[SourceNode]":
        """Return all SourceNode slots not yet bound in this job.

        Returns:
            List of unbound ``SourceNode`` instances, in order of appearance
            in the pipeline graph.
        """
        from orcapod.core.nodes.source_node import SourceNode

        if self._compiled_pipeline is None:
            return []

        unbound: list[SourceNode] = []
        seen: set[str] = set()
        for node in self._compiled_pipeline._persistent_node_map.values():
            if (
                isinstance(node, SourceNode)
                and node.name not in self._sources
                and node.name not in seen
            ):
                unbound.append(node)
                seen.add(node.name)
        return unbound

    def is_complete(self) -> bool:
        """Return ``True`` when all source nodes are bound and a store is set.

        Returns:
            ``True`` if all SourceNode slots are bound and a store is set.
        """
        return self._store is not None and len(self.unbound_source_nodes()) == 0

    def is_runnable(self, node_label: str) -> bool:
        """Return ``True`` if all upstream inputs of *node_label* are resolved.

        Args:
            node_label: Label of the node to check.

        Returns:
            ``True`` if the node can be executed with current bindings.
        """
        from orcapod.core.nodes.source_node import SourceNode

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
                and node.name not in self._sources
            ):
                return False
        return True

    # ------------------------------------------------------------------
    # unresolved_specs property
    # ------------------------------------------------------------------

    @property
    def unresolved_specs(self) -> list[str]:
        """Spec names that were unbound at run time (excluded from execution).

        Returns:
            List of unbound SourceSpec names from the most recent run.
            Empty list if run() has not been called or all specs were bound.
        """
        return list(self._unresolved_specs)

    # ------------------------------------------------------------------
    # build_execution_graph (public) / _build_execution_graph (impl)
    # ------------------------------------------------------------------

    def build_execution_graph(self) -> "tuple[Any, list[str], Pipeline]":  # Any = nx.DiGraph
        """Public entry point for building a fresh execution-ready graph.

        Builds a fresh execution graph with concrete sources substituted for
        bound SourceSpecs. Suitable for orchestrator-driven execution patterns
        where callers need direct access to the graph.

        Returns:
            Tuple of ``(exec_graph, unresolved_spec_names, exec_pipeline)``.

        Raises:
            ValueError: If ``self.store`` is ``None``.
            RuntimeError: If no compiled pipeline is available.
        """
        return self._build_execution_graph()

    def _build_execution_graph(self) -> "tuple[Any, list[str], Pipeline]":  # Any = nx.DiGraph
        """Build a fresh execution-ready graph with concrete sources substituted.

        Creates new SourceNode/FunctionNode/OperatorNode objects — does NOT
        mutate the existing node objects in ``pipeline._persistent_node_map``.

        Note:
            Updates the cloned ``exec_pipeline._nodes`` with the fresh exec nodes
            (keyed by label) so that the returned pipeline's ``compiled_nodes``
            returns execution-ready nodes after a run. The original
            ``self._compiled_pipeline`` is never mutated.

        Returns:
            Tuple of (exec_graph, unresolved_spec_names, exec_pipeline).

        Raises:
            ValueError: If ``self.store`` is ``None``.
            RuntimeError: If no compiled pipeline is available.
        """
        import networkx as nx
        from orcapod.core.nodes import FunctionNode, OperatorNode
        from orcapod.core.nodes.function_node import FunctionJobNode
        from orcapod.core.nodes.operator_node import OperatorJobNode
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.executors.local import LocalPythonFunctionExecutor

        pipeline = self._compiled_pipeline
        if pipeline is None:
            raise RuntimeError("No compiled pipeline — use 'with job:' first.")

        store = self._store
        if store is None:
            raise ValueError(
                "PipelineJob.run() requires a store. "
                "Call job.bind(store=db) before run()."
            )

        # Clone pipeline so exec-node label mutations don't affect the original
        exec_pipeline = pipeline._clone_for_execution()

        pipeline_db = store.at(*pipeline.name)
        result_db = pipeline_db.at("_result")

        # Build topological graph from hash-based edges
        G: "nx.DiGraph" = nx.DiGraph()
        for edge in pipeline._graph_edges:
            G.add_edge(*edge)
        # Also add isolated nodes that might not appear in edges
        for node_hash in list(pipeline._node_lut.keys()) + list(pipeline._upstreams.keys()):
            if node_hash not in G:
                G.add_node(node_hash)

        # Build reverse lookup: node object id → label in pipeline._nodes
        # compile() stores nodes by their computed label (node.label) even when
        # _label is None, so we need this reverse map to propagate labels to
        # freshly-created exec nodes.
        node_to_label: dict[int, str] = {
            id(node): label for label, node in pipeline._nodes.items()
        }

        exec_node_map: dict[str, "Any"] = {}
        excluded_hashes: set[str] = set()
        unresolved_specs: list[str] = []

        for node_hash in nx.topological_sort(G):
            if node_hash in excluded_hashes:
                continue

            if node_hash not in pipeline._node_lut:
                # Leaf stream — must be in _upstreams
                upstream = pipeline._upstreams.get(node_hash)
                if upstream is None:
                    continue
                if isinstance(upstream, SourceNode):
                    if upstream.name in self._sources:
                        # Bound — create a SourceJobNode with the concrete source.
                        concrete = self._sources[upstream.name]
                        exec_job_node = SourceJobNode(
                            name=upstream.name,
                            tag_schema=upstream.tag_schema,
                            data_schema=upstream.data_schema,
                            concrete=concrete,
                        )
                        exec_node_map[node_hash] = exec_job_node
                    else:
                        # Unbound — exclude this branch
                        excluded_hashes.add(node_hash)
                        if upstream.name not in unresolved_specs:
                            unresolved_specs.append(upstream.name)
                else:
                    # Raw non-SourceNode stream (shouldn't happen in new design,
                    # but handle gracefully for robustness).
                    exec_node_map[node_hash] = upstream
            else:
                template = pipeline._node_lut[node_hash]
                preds = list(G.predecessors(node_hash))

                if any(p in excluded_hashes for p in preds):
                    excluded_hashes.add(node_hash)
                    continue

                if isinstance(template, FunctionNode):
                    if not preds:
                        excluded_hashes.add(node_hash)
                        continue
                    input_node = exec_node_map[preds[0]]
                    new_fn = FunctionJobNode(
                        function_pod=template._function_pod,
                        input_stream=input_node,
                        label=template._label,
                    )
                    new_fn.attach_databases(
                        pipeline_database=pipeline_db,
                        result_database=result_db,
                    )
                    if template.executor is not None:
                        new_fn.executor = template.executor
                    else:
                        new_fn.executor = LocalPythonFunctionExecutor()
                    exec_node_map[node_hash] = new_fn

                elif isinstance(template, OperatorNode):
                    # All predecessors that are not excluded must be in exec_node_map.
                    # The excluded_hashes guard above (via `continue`) ensures any
                    # excluded predecessor causes this node to be skipped already.
                    missing = [p for p in preds if p not in exec_node_map]
                    if missing:
                        raise RuntimeError(
                            f"OperatorNode predecessor missing from exec_node_map: {missing}"
                        )
                    upstream_nodes = tuple(exec_node_map[p] for p in preds)
                    # blueprint OperatorNode has no _cache_mode; default is OFF
                    op_cache_mode = getattr(template, "_cache_mode", None) or CacheMode.OFF
                    new_op = OperatorJobNode(
                        operator=template._operator,
                        input_streams=upstream_nodes,
                        label=template._label,
                        table_scope=template._table_scope,
                    )
                    new_op.attach_databases(
                        pipeline_database=pipeline_db,
                        cache_mode=op_cache_mode,
                    )
                    exec_node_map[node_hash] = new_op

        # Build execution DiGraph with node objects as vertices
        exec_graph: "nx.DiGraph" = nx.DiGraph()
        for up_hash, down_hash in pipeline._graph_edges:
            if up_hash in exec_node_map and down_hash in exec_node_map:
                exec_graph.add_edge(exec_node_map[up_hash], exec_node_map[down_hash])
        for node in exec_node_map.values():
            if node not in exec_graph:
                exec_graph.add_node(node)

        # Update exec_pipeline._nodes with fresh exec nodes (keyed by label).
        # The clone's _nodes is independent of the original pipeline, so
        # running one job does not affect other jobs sharing the same blueprint.
        # Labels come from (in priority order):
        #   1. node._label — explicit label set at node creation
        #   2. template lookup — for FunctionNode/OperatorNode reconstructed from blueprint
        #   3. node.computed_label() — for SourceNodes, delegates to stream.label
        for node_hash, node in exec_node_map.items():
            label = node._label if node._label else None
            if label is None and node_hash in pipeline._node_lut:
                template = pipeline._node_lut[node_hash]
                label = node_to_label.get(id(template))
            if label is None:
                # SourceNodes (not in _node_lut) use computed_label() to delegate
                # their label to the wrapped stream (SourceSpec name or concrete source label).
                label = node.computed_label()
            if label:
                exec_pipeline._nodes[label] = node

        return exec_graph, unresolved_specs, exec_pipeline

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(
        self,
        observer: "ExecutionObserverProtocol | None" = None,
    ) -> "PipelineJob":
        """Execute the resolvable subgraph.

        Nodes whose upstream includes an unbound SourceSpec (and all their
        dependents) are excluded from execution. Partial execution is a
        first-class outcome — excluded spec names are recorded in the
        returned job's ``unresolved_specs``.

        After a successful run, ``job.pipeline.compiled_nodes`` returns the
        execution-ready nodes with databases attached.

        Args:
            observer: Optional execution observer.

        Returns:
            A new ``PipelineJob`` with run metadata populated.

        Raises:
            ValueError: If no store is set.
            RuntimeError: If no pipeline has been recorded.
        """
        import hashlib
        import uuid

        from orcapod.pipeline.observer import NoOpObserver
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        run_id = uuid.uuid4().hex[:16]

        exec_graph, unresolved_specs, exec_pipeline = self._build_execution_graph()

        effective_observer = observer or NoOpObserver()

        # Compute snapshot hash for run URI using only the leaf (sink) nodes.
        # Each node's content_hash() is a Merkle chain that encodes its own identity
        # and all of its transitive inputs, so the set of leaf-node hashes uniquely
        # identifies the full graph topology without needing to enumerate edges.
        leaf_hashes = sorted(
            n.content_hash().to_string()
            for n in exec_graph.nodes()
            if exec_graph.out_degree(n) == 0 and hasattr(n, "content_hash")
        )
        snapshot_hash = hashlib.sha256("\n".join(leaf_hashes).encode()).hexdigest()[:16]
        pipeline_uri = "/".join(self._compiled_pipeline.name) + "@" + snapshot_hash

        SyncPipelineOrchestrator().run(
            exec_graph,
            observer=effective_observer,
            pipeline_uri=pipeline_uri,
        )

        # Flush databases
        store = self._store
        if store is not None:
            pipeline_db = store.at(*self._compiled_pipeline.name)
            result_db = pipeline_db.at("_result")
            pipeline_db.flush()
            result_db.flush()

        # Return new job (different object); uses exec_pipeline (clone with exec nodes)
        result = PipelineJob(
            name=self._name,
            store=self._store,
            execution_context=self._execution_context,
            _pipeline=exec_pipeline,        # exec_pipeline (clone), not self._compiled_pipeline
            sources=dict(self._sources),
        )
        result._unresolved_specs = unresolved_specs
        result._has_run = True
        result._run_id = run_id
        return result

    def __repr__(self) -> str:
        n_sources = len(self._sources)
        has_store = self._store is not None
        has_pipeline = self._compiled_pipeline is not None
        return (
            f"PipelineJob(sources={n_sources}, store={has_store}, "
            f"pipeline={'compiled' if has_pipeline else 'unrecorded'})"
        )

    # ------------------------------------------------------------------
    # save() / load()
    # ------------------------------------------------------------------

    def save(self, path: "str | Path") -> None:
        """Serialize this job to a JSON file.

        Saves topology (via the embedded Pipeline) plus bindings metadata and
        run state. The format covers both "template" (pre-run) and
        "completed" (post-run) states — distinguished by ``run.status``.

        Args:
            path: File path to write JSON output to.

        Raises:
            ValueError: If no compiled pipeline exists.
        """
        import json as _json
        import os
        import tempfile
        from pathlib import Path as _Path

        from orcapod.pipeline.serialization import PIPELINE_JOB_FORMAT_VERSION

        pipeline = self._compiled_pipeline
        if pipeline is None:
            raise ValueError("No compiled pipeline to save.")

        path = _Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Save the pipeline blueprint inline via a temporary file
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
            tmp_path = tmp.name
        try:
            pipeline.save(tmp_path)
            with open(tmp_path) as f:
                pipeline_data = _json.load(f)
        finally:
            os.unlink(tmp_path)

        # Serialize source configs (best-effort — skip non-reconstructable sources)
        sources_block: dict[str, Any] = {}
        for spec_name, source in self._sources.items():
            if hasattr(source, "to_config"):
                sources_block[spec_name] = source.to_config()
            else:
                sources_block[spec_name] = {"source_type": "unknown"}

        # Store config (best-effort)
        store_block = None
        if self._store is not None and hasattr(self._store, "to_config"):
            store_block = self._store.to_config()

        if not self._has_run:
            status = "pending"
        elif self._unresolved_specs:
            status = "partial"
        else:
            status = "complete"

        output: dict[str, Any] = {
            "orcapod_pipeline_job_version": PIPELINE_JOB_FORMAT_VERSION,
            "run": {
                "run_id": self._run_id,
                "status": status,
                "unresolved_specs": list(self._unresolved_specs),
            },
            "pipeline": pipeline_data,
            "bindings": {
                "sources": sources_block,
                "store": store_block,
            },
        }

        with open(path, "w") as f:
            _json.dump(output, f, indent=2)

    @classmethod
    def load(
        cls,
        path: "str | Path",
        store: "ArrowDatabaseProtocol | None" = None,
    ) -> "PipelineJob":
        """Deserialize a ``PipelineJob`` from a JSON file.

        The embedded ``Pipeline`` blueprint is always restored. Concrete source
        bindings are restored for reconstructable source types. Pass *store*
        explicitly to override any serialized store configuration.

        Args:
            path: Path to the JSON file produced by :meth:`save`.
            store: Optional store override. When provided, takes precedence
                over any store configuration in the file.

        Returns:
            A ``PipelineJob`` ready to run (call ``bind()`` if sources are
            missing).

        Raises:
            ValueError: If the file's format version is unsupported.
        """
        import json as _json
        import os
        import tempfile
        from pathlib import Path as _Path

        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.serialization import (
            SUPPORTED_JOB_FORMAT_VERSIONS,
            resolve_source_from_config,
        )

        path = _Path(path)
        with open(path) as f:
            data = _json.load(f)

        version = data.get("orcapod_pipeline_job_version", "")
        if version not in SUPPORTED_JOB_FORMAT_VERSIONS:
            raise ValueError(
                f"Unsupported PipelineJob format version {version!r}. "
                f"Supported: {sorted(SUPPORTED_JOB_FORMAT_VERSIONS)}"
            )

        # Reconstruct pipeline from embedded blueprint data
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w") as tmp:
            tmp_path = tmp.name
            _json.dump(data["pipeline"], tmp)
        try:
            pipeline = Pipeline.load(tmp_path)
        finally:
            os.unlink(tmp_path)

        # Reconstruct sources (best-effort)
        sources: dict[str, cp.StreamProtocol] = {}
        for spec_name, src_config in data.get("bindings", {}).get("sources", {}).items():
            try:
                source = resolve_source_from_config(src_config)
                if source is not None:
                    sources[spec_name] = source
            except Exception:
                logger.warning(
                    "Could not reconstruct source %r from config — skipping.", spec_name
                )

        # Reconstruct store
        effective_store = store
        if effective_store is None:
            store_config = data.get("bindings", {}).get("store")
            if store_config:
                try:
                    from orcapod.pipeline.serialization import resolve_database_from_config
                    effective_store = resolve_database_from_config(store_config)
                except Exception:
                    logger.warning("Could not reconstruct store from config — skipping.")

        job = cls(
            name=pipeline.name,
            store=effective_store,
            _pipeline=pipeline,
            sources=sources,
        )

        # Restore run metadata
        run_block = data.get("run", {})
        status = run_block.get("status", "pending")
        if status in ("complete", "partial"):
            job._has_run = True
            job._unresolved_specs = run_block.get("unresolved_specs", [])
        if run_id := run_block.get("run_id"):
            job._run_id = run_id

        return job
