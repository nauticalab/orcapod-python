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
    ``SourceJobNode`` declarations in the underlying graph, with their
    concrete instances stored in ``job.sources``.

    After the ``with`` block, ``job.pipeline`` returns a lightweight
    ``Pipeline`` blueprint (SourceNode-only leaves). ``job.run()`` executes
    the resolvable subgraph — nodes whose upstream SourceNodes are all bound.

    ``PipelineJob`` can also be created from a ``Pipeline`` via
    ``PipelineJob.from_pipeline(pipeline, sources=..., store=...)`` for the
    "explicit blueprint" workflow.

    Args:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run.
        store: Database for result caching and operator records.
        execution_context: Optional execution configuration.
        tracker_manager: Optional tracker manager override.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
    """

    # ------------------------------------------------------------------
    # Node-factory class properties (used by AbstractPipelineBase.compile())
    # ------------------------------------------------------------------

    @property
    def source_node_class(self) -> type:
        """SourceJobNode — execution-ready source node class for PipelineJob."""
        from orcapod.core.nodes.source_node import SourceJobNode
        return SourceJobNode

    @property
    def function_node_class(self) -> type:
        """FunctionJobNode — execution-ready function node class for PipelineJob."""
        from orcapod.core.nodes.function_node import FunctionJobNode
        return FunctionJobNode

    @property
    def operator_node_class(self) -> type:
        """OperatorJobNode — execution-ready operator node class for PipelineJob."""
        from orcapod.core.nodes.operator_node import OperatorJobNode
        return OperatorJobNode

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        store: "ArrowDatabaseProtocol | None" = None,
        execution_context: "ExecutionContext | None" = None,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        auto_compile: bool = True,
    ) -> None:
        super().__init__(name=name, tracker_manager=tracker_manager)
        self._store = store
        self._execution_context = execution_context
        self._auto_compile = auto_compile
        self._sources: dict[str, cp.StreamProtocol] = {}
        # Hash-keyed lookup for _build_execution_graph() — same key as
        # _persistent_node_map.  Avoids name collisions when multiple concrete
        # sources share the same class name (e.g. two ArrowTableSource objects).
        self._sources_by_hash: dict[str, cp.StreamProtocol] = {}
        self._unresolved_specs: list[str] = []
        self._has_run: bool = False
        self._run_id: str | None = None

        # Lazy cache for the corresponding Pipeline blueprint.
        # Set to None after compile() to force recomputation on next access.
        # Pre-set by from_pipeline() / run() via _set_compiled_pipeline().
        self._compiled_pipeline: "Pipeline | None" = None

    # ------------------------------------------------------------------
    # Context manager — respects auto_compile flag
    # ------------------------------------------------------------------

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Exit the recording context, compiling if no exception occurred."""
        AutoRegisteringContextBasedTracker.__exit__(self, exc_type, exc_value, traceback)
        if exc_type is None and self._auto_compile:
            self.compile()

    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG of job nodes.

        Delegates to ``AbstractPipelineBase.compile()`` which uses
        ``source_node_class``, ``function_node_class``, and
        ``operator_node_class`` to create job node instances.

        Concrete source bindings already in ``_sources`` are preserved.
        Any ``SourceJobNode`` in ``_persistent_node_map`` that has a bound
        source also contributes to ``_sources`` (covers both the
        direct-recording path and the ``bind()``-before-compile path).

        The lazy ``compiled_pipeline`` cache is invalidated.

        If ``_store`` is set, distributes databases to all function and
        operator job nodes.
        """
        # Preserve any sources already bound (e.g. from a prior bind() call).
        pre_sources = dict(self._sources)
        pre_sources_by_hash = dict(self._sources_by_hash)

        super().compile()

        # Collect bound SourceJobNodes created during compilation.
        # _sources is name-keyed (user-facing API via job.sources).
        # _sources_by_hash is hash-keyed (internal use by _build_execution_graph()).
        # Two concrete sources of the same class (e.g. two ArrowTableSource objects)
        # would collide on name but have distinct hash keys.
        from orcapod.core.nodes.source_node import SourceJobNode
        compiled_by_name: dict[str, Any] = {}
        compiled_by_hash: dict[str, Any] = {}
        for h, n in self._persistent_node_map.items():
            if isinstance(n, SourceJobNode) and n.bound_source is not None:
                compiled_by_name[n.name] = n.bound_source
                compiled_by_hash[h] = n.bound_source
                # Also index by schema-based hash (matches blueprint pipeline keys).
                schema_hash = n.as_node().content_hash().to_string()
                compiled_by_hash[schema_hash] = n.bound_source
        # pre_sources / pre_sources_by_hash win over compiled (explicit bind() takes priority).
        self._sources = {**compiled_by_name, **pre_sources}
        self._sources_by_hash = {**compiled_by_hash, **pre_sources_by_hash}

        # Invalidate the lazy compiled_pipeline cache.
        self._compiled_pipeline = None

        # Wire databases if a store is already set.
        if self._store is not None:
            self._distribute_databases()

    # ------------------------------------------------------------------
    # compiled_pipeline — lazy cache
    # ------------------------------------------------------------------

    @property
    def compiled_pipeline(self) -> "Pipeline":
        """The lightweight ``Pipeline`` blueprint for this job (lazy cache).

        Computed on first access by calling ``as_pipeline()``.  Invalidated
        by ``compile()`` and ``bind()`` (when sources change).

        Raises:
            RuntimeError: If the job has not been compiled yet.
        """
        if self._compiled_pipeline is None:
            if not self._compiled:
                raise RuntimeError(
                    "No compiled pipeline — use 'with job:' to record a DAG first."
                )
            self._compiled_pipeline = self.as_pipeline()
        return self._compiled_pipeline

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def pipeline(self) -> "Pipeline":
        """The compiled pure Pipeline (SourceNode-only leaves).

        Raises:
            RuntimeError: If the with-block has not been completed yet.
        """
        return self.compiled_pipeline

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

        Converts the Pipeline blueprint to an ``InvocationGraph`` via
        ``pipeline.to_invocations()``, then reconstructs a PipelineJob
        using ``from_invocations()`` (which calls ``SourceJobNode.from_stream``
        for each source, creating unbound job nodes).  If *sources* are
        provided, validates and binds them.

        Args:
            pipeline: A compiled ``Pipeline`` (``pipeline._compiled`` must
                be ``True``).
            store: Database for result caching and operator records.
            sources: Mapping of ``SourceNode.name`` → concrete source.
            execution_context: Optional execution configuration.

        Returns:
            A new ``PipelineJob`` ready to run (or ``bind()`` further).

        Raises:
            ValueError: If *pipeline* has not been compiled.
        """
        from orcapod.core.nodes.source_node import SourceNodeBase

        if not pipeline._compiled:
            raise ValueError(
                "Pipeline must be compiled before creating a PipelineJob from it. "
                "Call pipeline.compile() or use auto_compile=True."
            )

        bound_sources: dict[str, cp.StreamProtocol] = dict(sources or {})

        # Validate sources against SourceNode schemas.
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

        # Reconstruct via InvocationGraph (creates unbound SourceJobNodes).
        # _compiled_pipeline is intentionally left as None here so the lazy
        # property computes a schema-normalized blueprint via as_pipeline().
        # Setting it to the original pipeline would cause hash-key mismatches in
        # _build_execution_graph(): the original pipeline may carry concrete
        # (data-inclusive) hash keys in _upstreams, while _sources_by_hash is
        # keyed by schema-based hashes derived from the unbound SourceJobNodes.
        job = cls.from_invocations(pipeline.to_invocations(), name=pipeline._name)
        job._store = store
        job._execution_context = execution_context

        # Bind concrete sources and distribute databases.
        if bound_sources:
            job.bind(sources=bound_sources)
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
        its matching ``SourceJobNode`` slot schema, then the corresponding
        ``SourceJobNode.bound_source`` is updated in-place.

        When *store* is provided and differs from the current store,
        ``_distribute_databases()`` is called so that all job nodes receive
        live DB references immediately.

        Args:
            sources: Mapping of ``SourceNode.name`` → concrete source.
            store: Replaces the current store and triggers DB redistribution.
            execution_context: Replaces the current execution context.

        Raises:
            SourceSpecMismatchError: If any source's schema is incompatible.
            ValueError: If a source key has no matching ``SourceJobNode`` slot.
        """
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNodeBase

        store_changed = store is not None and store is not self._store

        if store is not None:
            self._store = store

        if sources is not None:
            spec_names = {
                node.name
                for node in (self._persistent_node_map or {}).values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(sources.keys()) - spec_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching source slot "
                    f"(SourceJobNode): {sorted(unknown)}. "
                    f"Known slot names: {sorted(spec_names)}"
                )
            for node in (self._persistent_node_map or {}).values():
                if isinstance(node, SourceNodeBase) and node.name in sources:
                    node.validate(sources[node.name])

            # Update bound_source on SourceJobNodes and maintain both dicts.
            for h, job_node in (self._persistent_node_map or {}).items():
                if isinstance(job_node, SourceJobNode) and job_node.name in sources:
                    job_node.bound_source = sources[job_node.name]
                    self._sources_by_hash[h] = sources[job_node.name]
                    # Also index by schema-based hash (matches blueprint pipeline keys).
                    schema_hash = job_node.as_node().content_hash().to_string()
                    self._sources_by_hash[schema_hash] = sources[job_node.name]
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

        Called by ``compile()`` when a store is already set, and by ``bind()``
        when *store* is changed.

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

        pipeline_db = self._store.at(*self._name)
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

        Walks ``_persistent_node_map`` topologically and rewires each job
        node to a fresh blueprint node whose upstreams point at the
        already-built blueprint nodes in ``node_map`` (not at job nodes).
        This ensures that the content_hash of every node in the returned
        ``Pipeline`` is consistent with the hash keys in
        ``_persistent_node_map``.

        Returns:
            A compiled ``Pipeline`` whose ``_persistent_node_map`` contains
            only lightweight ``SourceNode`` / ``FunctionNode`` / ``OperatorNode``
            objects with blueprint (non-job) upstream references.  Keys in
            ``_persistent_node_map`` match those in ``self._persistent_node_map``.

        Raises:
            RuntimeError: If this job has not been compiled.
        """
        import networkx as _nx
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.pipeline.graph import Pipeline

        if not self._compiled:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline. "
                "Either use 'with job:' to record a DAG, "
                "or create the job via PipelineJob.from_pipeline()."
            )

        persistent = self._persistent_node_map

        # Reverse lookup: job node Python id → its key in _persistent_node_map.
        # Required to wire blueprint upstreams to the correct hash key even when
        # job node identity (content_hash) differs from the map key.
        job_id_to_bp_hash: dict[int, str] = {
            id(job_node): bp_hash
            for bp_hash, job_node in persistent.items()
        }

        node_map: dict[str, object] = {}

        for node_hash in _nx.topological_sort(self._hash_graph):
            if node_hash not in persistent:
                continue
            job_node = persistent[node_hash]

            if isinstance(job_node, SourceJobNode):
                node_map[node_hash] = job_node.as_node()

            elif isinstance(job_node, FunctionJobNode):
                upstream_bp_hash = job_id_to_bp_hash[id(job_node._input_stream)]
                node_map[node_hash] = FunctionNode(
                    function_pod=job_node._function_pod,
                    input_stream=node_map[upstream_bp_hash],
                    label=job_node._label,
                    table_scope=job_node._table_scope,
                    tracker_manager=job_node.tracker_manager,
                )

            elif isinstance(job_node, OperatorJobNode):
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
                node_map[node_hash] = job_node.as_node()

        # Translate old (concrete) hash keys → new (schema-based) node content hashes.
        # Blueprint nodes use schema-based content_hash() (SourceNode is schema-only,
        # so FunctionNode/OperatorNode upstreaming SourceNodes are also schema-based).
        old_to_new: dict[str, str] = {
            h: bp_node.content_hash().to_string()
            for h, bp_node in node_map.items()
        }
        # Re-key node_map with schema-based hashes.
        new_node_map: dict[str, object] = {
            new_h: node_map[h]
            for h, new_h in old_to_new.items()
        }

        # Rebuild hash graph with schema-based keys.
        # Carry over node attributes (node_type, label, pipeline_hash) from the
        # job's hash graph — these are structural/type attributes that remain
        # valid for the blueprint nodes.
        bp_hash_graph = _nx.DiGraph()
        for old_h, new_h in old_to_new.items():
            old_attrs = dict(self._hash_graph.nodes[old_h]) if old_h in self._hash_graph else {}
            bp_hash_graph.add_node(new_h, **old_attrs)
        for up_h, down_h in self._hash_graph.edges():
            new_up = old_to_new.get(up_h)
            new_down = old_to_new.get(down_h)
            if new_up is not None and new_down is not None:
                bp_hash_graph.add_edge(new_up, new_down)

        new_graph_edges = [
            (old_to_new[u], old_to_new[v])
            for u, v in self._graph_edges
            if u in old_to_new and v in old_to_new
        ]

        pipeline = Pipeline(name=self._name, auto_compile=False)
        pipeline._hash_graph = bp_hash_graph
        pipeline._graph_edges = new_graph_edges
        pipeline._persistent_node_map = new_node_map
        pipeline._upstreams = {
            old_to_new[h]: node_map[h]
            for h in self._upstreams
            if h in old_to_new
        }
        pipeline._node_lut = {
            old_to_new[h]: node_map[h]
            for h in self._node_lut
            if h in old_to_new
        }
        pipeline._nodes = {
            label: node_map[job_id_to_bp_hash[id(job_node)]]
            for label, job_node in self._nodes.items()
            if id(job_node) in job_id_to_bp_hash
            and job_id_to_bp_hash[id(job_node)] in node_map
        }

        # Build _node_graph (DiGraph with node objects as vertices).
        # Mirrors AbstractPipelineBase.compile() step 5.
        pipeline._node_graph = _nx.DiGraph()
        for up_h, down_h in self._hash_graph.edges():
            up_node = node_map.get(up_h)
            down_node = node_map.get(down_h)
            if up_node is not None and down_node is not None:
                pipeline._node_graph.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in pipeline._node_graph:
                pipeline._node_graph.add_node(node)

        pipeline._compiled = True

        return pipeline

    def to_invocations(self):  # type: ignore[override]
        """Extract an ``InvocationGraph`` from the schema-based blueprint.

        Delegates to ``as_pipeline().to_invocations()`` so that the
        produced invocations always use schema-based (not data-based)
        hashes, regardless of whether sources are currently bound.

        This ensures stable serialization across bind/rebind cycles.

        Raises:
            RuntimeError: If this job has not been compiled.
        """
        return self.as_pipeline().to_invocations()

    # ------------------------------------------------------------------
    # Completeness introspection
    # ------------------------------------------------------------------

    def unbound_source_nodes(self) -> "list[Any]":
        """Return all SourceJobNode slots not yet bound in this job.

        Returns:
            List of unbound ``SourceJobNode`` instances, in order of
            appearance in the pipeline graph.
        """
        from orcapod.core.nodes.source_node import SourceJobNode

        if not self._compiled:
            return []

        unbound: list[Any] = []
        seen: set[str] = set()
        for node in (self._persistent_node_map or {}).values():
            if (
                isinstance(node, SourceJobNode)
                and node.bound_source is None
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
        from orcapod.core.nodes.source_node import SourceJobNode

        if not self._compiled or self._node_graph is None:
            return False

        target = self._nodes.get(node_label)
        if target is None:
            return False

        import networkx as nx

        for node in nx.ancestors(self._node_graph, target) | {target}:
            if isinstance(node, SourceJobNode) and node.bound_source is None:
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
            ``self.compiled_pipeline`` is never mutated.

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

        pipeline = self.compiled_pipeline  # raises RuntimeError("No compiled pipeline...") if not compiled

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
                    # _sources_by_hash is keyed by the _persistent_node_map hash,
                    # which is always unique even when multiple sources share a name.
                    if node_hash in self._sources_by_hash:
                        # Bound — create a SourceJobNode with the concrete source.
                        concrete = self._sources_by_hash[node_hash]
                        exec_job_node = SourceJobNode(
                            name=upstream.name,
                            tag_schema=upstream.tag_schema,
                            data_schema=upstream.data_schema,
                            bound_source=concrete,
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
                    missing = [p for p in preds if p not in exec_node_map]
                    if missing:
                        raise RuntimeError(
                            f"OperatorNode predecessor missing from exec_node_map: {missing}"
                        )
                    upstream_nodes = tuple(exec_node_map[p] for p in preds)
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
        for node_hash, node in exec_node_map.items():
            label = node._label if node._label else None
            if label is None and node_hash in pipeline._node_lut:
                template = pipeline._node_lut[node_hash]
                label = node_to_label.get(id(template))
            if label is None:
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
        leaf_hashes = sorted(
            n.content_hash().to_string()
            for n in exec_graph.nodes()
            if exec_graph.out_degree(n) == 0 and hasattr(n, "content_hash")
        )
        snapshot_hash = hashlib.sha256("\n".join(leaf_hashes).encode()).hexdigest()[:16]
        pipeline_uri = "/".join(self._name) + "@" + snapshot_hash

        SyncPipelineOrchestrator().run(
            exec_graph,
            observer=effective_observer,
            pipeline_uri=pipeline_uri,
        )

        # Flush databases
        store = self._store
        if store is not None:
            pipeline_db = store.at(*self._name)
            result_db = pipeline_db.at("_result")
            pipeline_db.flush()
            result_db.flush()

        # Return new job with exec_pipeline (contains exec nodes with databases).
        result = PipelineJob(
            name=self._name,
            store=self._store,
            execution_context=self._execution_context,
        )
        result._compiled_pipeline = exec_pipeline
        result._sources = dict(self._sources)
        result._sources_by_hash = dict(self._sources_by_hash)
        result._unresolved_specs = unresolved_specs
        result._has_run = True
        result._run_id = run_id
        return result

    def __repr__(self) -> str:
        n_sources = len(self._sources)
        has_store = self._store is not None
        is_compiled = self._compiled
        return (
            f"PipelineJob(sources={n_sources}, store={has_store}, "
            f"compiled={is_compiled})"
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

        # Allow save when either compile() was called or _compiled_pipeline is
        # pre-set (e.g. result of run() which has no invocation_lut but does
        # have a compiled_pipeline from exec graph construction).
        if not self._compiled and self._compiled_pipeline is None:
            raise ValueError("No compiled pipeline to save.")

        pipeline = self.compiled_pipeline

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

        # Reconstruct the job directly without going through to_invocations().
        # The loaded pipeline may have stub nodes (function_pod=None, operator=None)
        # that cannot compute content hashes, so the to_invocations() → from_invocations()
        # path used by from_pipeline() would fail.  Instead, attach the loaded
        # pipeline directly as the compiled blueprint and set up source bindings.
        job = cls(name=pipeline._name, store=effective_store)
        job._compiled_pipeline = pipeline
        # Mark compiled so save() / compiled_pipeline can be accessed.
        # _persistent_node_map etc. remain empty (job was not recorded in a
        # with-block), but all public API goes through _compiled_pipeline.
        job._compiled = True
        job._nodes = dict(pipeline._nodes)

        # Populate _upstreams / _node_lut / _graph_edges / _hash_graph from pipeline.
        job._upstreams = dict(pipeline._upstreams)
        job._node_lut = dict(pipeline._node_lut)
        job._graph_edges = list(pipeline._graph_edges)
        job._hash_graph = pipeline._hash_graph.copy()

        # Build _sources and _sources_by_hash from reconstructed sources.
        from orcapod.core.nodes.source_node import SourceNode as _SourceNode
        job._sources = dict(sources)
        for h, source_node in pipeline._upstreams.items():
            if isinstance(source_node, _SourceNode) and source_node.name in sources:
                job._sources_by_hash[h] = sources[source_node.name]

        if effective_store is not None:
            job._distribute_databases()

        # Restore run metadata
        run_block = data.get("run", {})
        status = run_block.get("status", "pending")
        if status in ("complete", "partial"):
            job._has_run = True
            job._unresolved_specs = run_block.get("unresolved_specs", [])
        if run_id := run_block.get("run_id"):
            job._run_id = run_id

        return job
