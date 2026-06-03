"""PipelineJob — pipeline + source bindings + execution context."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from orcapod.core.nodes import JobNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.base import AbstractPipelineBase
from orcapod.pipeline.dag import OrcaDAG
from orcapod.protocols import core_protocols as cp
from orcapod.types import CacheMode

if TYPE_CHECKING:
    from orcapod.core.nodes import FunctionNode, GraphNode, OperatorNode
    from orcapod.core.nodes.source_node import SourceNode
    from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
    from orcapod.pipeline.execution_context import ExecutionContext
    from orcapod.pipeline.graph import Pipeline
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol

logger = logging.getLogger(__name__)


class PipelineJob(AbstractPipelineBase[JobNode]):
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
    # Node-factory class attributes (used by AbstractPipelineBase.compile())
    # ------------------------------------------------------------------

    source_node_class = SourceJobNode
    function_node_class = FunctionJobNode
    operator_node_class = OperatorJobNode

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
        # name → bound concrete source (user-facing API: job.sources).
        self._sources: dict[str, cp.StreamProtocol] = {}
        # hash → bound concrete source, keyed by the same hash as
        # _persistent_node_map.  Used by bind() for O(1) lookup by content
        # hash; avoids name collisions when multiple concrete sources share
        # the same class name (e.g. two ArrowTableSource objects).
        self._sources_by_hash: dict[str, cp.StreamProtocol] = {}
        self._has_run: bool = False
        self._run_id: str | None = None

        # Lazy cache for the corresponding Pipeline blueprint.
        # Set to None after compile() to force recomputation on next access.
        # Pre-set by from_pipeline() via _set_compiled_pipeline().
        self._compiled_pipeline: Pipeline | None = None

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
        # _sources_by_hash is hash-keyed (internal use by bind()).
        # Two concrete sources of the same class (e.g. two ArrowTableSource objects)
        # would collide on name but have distinct hash keys.
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
        by ``compile()``, which sets ``_compiled_pipeline`` to ``None`` so the
        next access recomputes a fresh schema-normalised blueprint.

        ``bind()`` does **not** invalidate this cache: the blueprint contains
        only ``SourceNode`` schema-slot declarations with no concrete data, so
        it is unaffected by changes to concrete source bindings.

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
            source_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(bound_sources.keys()) - source_names
            if unknown:
                raise ValueError(
                    f"from_pipeline() received source keys with no matching SourceNode: "
                    f"{sorted(unknown)}. Known names: {sorted(source_names)}"
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
        from orcapod.core.nodes.source_node import SourceNodeBase

        store_changed = store is not None and store is not self._store

        if store is not None:
            self._store = store

        if sources is not None:
            source_names = {
                node.name
                for node in (self._persistent_node_map or {}).values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(sources.keys()) - source_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching source slot "
                    f"(SourceJobNode): {sorted(unknown)}. "
                    f"Known slot names: {sorted(source_names)}"
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
        from orcapod.core.nodes.function_node import FunctionNode
        from orcapod.core.nodes.operator_node import OperatorNode
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

        # Build _node_graph (OrcaDAG with node objects as vertices).
        # node_map here contains GraphNode objects (blueprint nodes).
        bp_dag: OrcaDAG = OrcaDAG()
        for up_h, down_h in self._hash_graph.edges():
            up_node = node_map.get(up_h)
            down_node = node_map.get(down_h)
            if up_node is not None and down_node is not None:
                bp_dag.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in bp_dag:
                bp_dag.add_node(node)
        pipeline._node_graph = bp_dag

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

    @property
    def unbound_sources(self) -> list[str]:
        """Names of source slots not yet bound in this job.

        Computed live from ``_persistent_node_map`` — reflects the current
        binding state at all times (before and after run).

        Returns:
            List of unbound source slot names, in order of appearance in
            the pipeline graph. Empty list if the job is not yet compiled.
        """
        if not self._compiled:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for node in (self._persistent_node_map or {}).values():
            if (
                isinstance(node, SourceJobNode)
                and node.bound_source is None
                and node.name not in seen
            ):
                result.append(node.name)
                seen.add(node.name)
        return result

    def is_complete(self) -> bool:
        """Return ``True`` when all source nodes are bound and a store is set.

        Returns:
            ``True`` if all SourceNode slots are bound and a store is set.
        """
        return self._store is not None and not self.unbound_sources

    def is_runnable(self, node_label: str) -> bool:
        """Return ``True`` if all upstream inputs of *node_label* are resolved.

        Uses hash-based traversal of ``_hash_graph`` combined with
        ``_persistent_node_map`` to check whether every upstream source slot
        is bound.  This avoids relying on the object-identity graph
        (``_node_graph``), which breaks after ``bind()`` because
        ``SourceJobNode.__hash__()`` changes when ``bound_source`` is mutated.

        Args:
            node_label: Label of the node to check.

        Returns:
            ``True`` if the node can be executed with current bindings.
        """
        if not self._compiled:
            return False

        target = self._nodes.get(node_label)
        if target is None:
            return False

        # Build a reverse lookup: node object id → hash key.
        # _persistent_node_map covers SourceJobNode slots (all compiled paths)
        # and all node types for directly-compiled jobs.
        # _node_lut covers non-source nodes for loaded jobs, where
        # _persistent_node_map only holds SourceJobNodes.
        node_id_to_hash: dict[int, str] = {}
        for h, node in self._node_lut.items():
            node_id_to_hash[id(node)] = h
        for h, node in self._persistent_node_map.items():
            # _persistent_node_map wins on collision (it contains live job nodes)
            node_id_to_hash[id(node)] = h

        target_hash = node_id_to_hash.get(id(target))
        if target_hash is None:
            return False

        import networkx as nx

        for h in nx.ancestors(self._hash_graph, target_hash) | {target_hash}:
            node = self._persistent_node_map.get(h)
            if isinstance(node, SourceJobNode) and node.bound_source is None:
                return False
        return True

    # ------------------------------------------------------------------
    # run()
    # ------------------------------------------------------------------

    def run(
        self,
        orchestrator: "SyncPipelineOrchestrator | AsyncPipelineOrchestrator | None" = None,
        observer: "ExecutionObserverProtocol | None" = None,
        materialize_results: bool = True,
    ) -> "PipelineJob":
        """Execute the resolvable subgraph of this job in place.

        Nodes whose upstream includes an unbound source (and all their
        dependents) are excluded from execution. Partial execution is a
        first-class outcome — ``unbound_sources`` reports which source
        slots were excluded.

        After a successful run, ``job.nodes`` returns the execution-ready
        nodes with populated database caches.

        Args:
            orchestrator: Orchestrator to use for execution. Defaults to
                ``SyncPipelineOrchestrator`` when ``None``.
            observer: Optional execution observer.
            materialize_results: Controls whether the orchestrator retains
                node outputs in its transient in-process buffer during
                execution. When ``True`` (the default) the buffer is kept for
                the duration of the run. When ``False`` each node's buffer is
                released as soon as the node completes, reducing peak memory.
                This setting does not affect database persistence or post-run
                result access — DB records are written regardless, and
                ``job.nodes["label"].get_all_records()`` works the same in
                either case.

        Returns:
            ``self`` — the same ``PipelineJob`` instance, with
            ``_has_run`` and ``_run_id`` set.

        Raises:
            ValueError: If no store is set.
            RuntimeError: If no pipeline has been compiled.
        """
        import hashlib
        import uuid

        from orcapod.pipeline.observer import NoOpObserver
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        if not self._compiled:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline. "
                "Record invocations inside a 'with job:' block first."
            )
        if self._store is None:
            raise ValueError(
                "PipelineJob.run() requires a store. "
                "Call job.bind(store=db) before run(). "
                "For transient (in-memory) storage use "
                "job.bind(store=InMemoryArrowDatabase())."
            )

        # Reject loaded read-only stubs: nodes created by load() have no live
        # function pod / operator and cannot compute new outputs.  Replaying
        # cached results would silently produce no output for cache misses; a
        # clear error is better.  Use get_all_records() on the loaded job to
        # inspect results.
        from orcapod.pipeline.serialization import LoadStatus

        unavailable_labels = [
            node.label
            for node in self._persistent_node_map.values()
            if isinstance(node, (FunctionJobNode, OperatorJobNode))
            and node.load_status == LoadStatus.UNAVAILABLE
        ]
        if unavailable_labels:
            raise RuntimeError(
                "PipelineJob.run() cannot execute a loaded job: the following "
                f"nodes are read-only stubs: {unavailable_labels}. "
                "PipelineJob.load() is for result inspection only. "
                "To run a new computation, create a fresh PipelineJob from the "
                "original pipeline definition."
            )

        # --- Build hash-keyed OrcaDAG for topological ordering + exclusion ---
        hash_dag: OrcaDAG[str] = OrcaDAG()
        for u, v in self._graph_edges:
            hash_dag.add_edge(u, v)
        for node_hash in self._persistent_node_map:
            if node_hash not in hash_dag:
                hash_dag.add_node(node_hash)

        # Walk topologically; exclude unbound sources and all their dependents.
        excluded_hashes: set[str] = set()
        for node_hash in hash_dag.topological_sort():
            if node_hash not in self._persistent_node_map:
                continue
            node = self._persistent_node_map[node_hash]
            if isinstance(node, SourceJobNode) and node.bound_source is None:
                excluded_hashes.add(node_hash)
            elif any(p in excluded_hashes for p in hash_dag.predecessors(node_hash)):
                excluded_hashes.add(node_hash)

        # --- Build execution OrcaDAG from existing node objects (no cloning) ---
        exec_dag: OrcaDAG = OrcaDAG()
        for node_hash, node in self._persistent_node_map.items():
            if node_hash not in excluded_hashes:
                exec_dag.add_node(node)
        for u_hash, v_hash in self._graph_edges:
            if (
                u_hash not in excluded_hashes
                and v_hash not in excluded_hashes
                and u_hash in self._persistent_node_map
                and v_hash in self._persistent_node_map
            ):
                exec_dag.add_edge(
                    self._persistent_node_map[u_hash],
                    self._persistent_node_map[v_hash],
                )

        # --- Execute ---
        effective_observer = observer or NoOpObserver()
        run_id = uuid.uuid4().hex[:16]

        # Snapshot hash: SHA-256 of sink-node content hashes for the run URI.
        leaf_hashes = sorted(
            node.content_hash().to_string()
            for node in exec_dag.nodes()
            if not exec_dag.successors(node) and hasattr(node, "content_hash")
        )
        snapshot_hash = hashlib.sha256("\n".join(leaf_hashes).encode()).hexdigest()[:16]
        pipeline_uri = "/".join(self._name) + "@" + snapshot_hash

        effective_orchestrator = orchestrator or SyncPipelineOrchestrator()
        effective_orchestrator.run(
            exec_dag,
            observer=effective_observer,
            run_id=run_id,
            pipeline_uri=pipeline_uri,
            materialize_results=materialize_results,
        )

        # Flush databases.
        pipeline_db = self._store.at(*self._name)
        result_db = pipeline_db.at("_result")
        pipeline_db.flush()
        result_db.flush()

        # Mutate self in place.
        self._has_run = True
        self._run_id = run_id

        return self

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
        elif self.unbound_sources:
            status = "partial"
        else:
            status = "complete"

        # Collect the live (data-inclusive) content hashes for FunctionJobNodes.
        # The pipeline blueprint stores schema-only blueprint hashes; but the DB
        # records are written with the live hash (which includes the upstream data
        # content).  Saving these ensures that PipelineJob.load() can reconstruct
        # stubs whose content_hash() matches the _node_content_hash column in the
        # DB — required for _filter_by_content_hash() in get_all_records().
        node_job_content_hashes: dict[str, str] = {}
        for node in (self._persistent_node_map or {}).values():
            if isinstance(node, FunctionJobNode):
                node_job_content_hashes[node.label] = node.content_hash().to_string()

        output: dict[str, Any] = {
            "orcapod_pipeline_job_version": PIPELINE_JOB_FORMAT_VERSION,
            "run": {
                "run_id": self._run_id,
                "status": status,
                "unbound_sources": list(self.unbound_sources),
                "node_job_content_hashes": node_job_content_hashes,
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
        job._compiled = True

        # Populate _upstreams / _node_lut / _graph_edges / _hash_graph from pipeline.
        job._upstreams = dict(pipeline._upstreams)
        job._node_lut = dict(pipeline._node_lut)
        job._graph_edges = list(pipeline._graph_edges)
        job._hash_graph = pipeline._hash_graph.copy()

        # Populate _persistent_node_map with job-node instances for ALL node types,
        # keyed by the same hash keys as pipeline._persistent_node_map.
        #
        # SourceJobNode stubs must be present so that bind() can locate source slots
        # and update them in-place.  FunctionJobNode and OperatorJobNode stubs must
        # also be present so that:
        #   • _distribute_databases() wires their databases correctly
        #   • job.nodes["label"].get_all_records() works after loading a completed job
        #   • run() includes them in the execution DAG
        import networkx as _nx
        from orcapod.core.nodes.function_node import FunctionNode as _FunctionNode
        from orcapod.core.nodes.operator_node import OperatorNode as _OperatorNode
        from orcapod.core.nodes.source_node import SourceNode as _SourceNode

        job._sources = dict(sources)

        # Extract the live (data-inclusive) content hashes saved at run time.
        # These are keyed by node label and used below to override the blueprint
        # hash stored in each FunctionNode descriptor so that loaded stubs'
        # content_hash() matches the _node_content_hash column in the DB.
        run_block = data.get("run", {})
        node_job_content_hashes: dict[str, str] = run_block.get(
            "node_job_content_hashes", {}
        )

        for node_hash in _nx.topological_sort(job._hash_graph):
            bp_node = pipeline._persistent_node_map.get(node_hash)
            if bp_node is None:
                continue

            if isinstance(bp_node, _SourceNode):
                sjn = SourceJobNode(
                    name=bp_node.name,
                    tag_schema=bp_node.tag_schema,
                    data_schema=bp_node.data_schema,
                    bound_source=sources.get(bp_node.name),
                )
                job._persistent_node_map[node_hash] = sjn
                if bp_node.name in sources:
                    job._sources_by_hash[node_hash] = sources[bp_node.name]

            elif isinstance(bp_node, _FunctionNode):
                descriptor = getattr(bp_node, "_descriptor", None) or {}
                preds = list(job._hash_graph.predecessors(node_hash))
                input_stream = (
                    job._persistent_node_map.get(preds[0]) if preds else None
                )
                # Pass the live content hash (from the run) so the stub's
                # content_hash() matches the _node_content_hash column in
                # the DB — required for _filter_by_content_hash() to work
                # in get_all_records() on a loaded job.
                live_content_hash = node_job_content_hashes.get(
                    bp_node.label
                )
                fjn = FunctionJobNode.from_descriptor(
                    descriptor,
                    input_stream=input_stream,
                    job_content_hash=live_content_hash,
                )
                job._persistent_node_map[node_hash] = fjn

            elif isinstance(bp_node, _OperatorNode):
                descriptor = getattr(bp_node, "_descriptor", None) or {}
                preds = list(job._hash_graph.predecessors(node_hash))
                input_streams = tuple(
                    job._persistent_node_map[p]
                    for p in preds
                    if p in job._persistent_node_map
                )
                # Use the reconstructed operator from the blueprint if available.
                operator = getattr(bp_node, "_operator", None)
                ojn = OperatorJobNode.from_descriptor(
                    descriptor,
                    operator=operator,
                    input_streams=input_streams,
                    databases={},
                )
                job._persistent_node_map[node_hash] = ojn

        # Update _nodes to point to job nodes (FunctionJobNode/OperatorJobNode stubs)
        # rather than blueprint nodes, so that job.nodes["label"].get_all_records()
        # works on a loaded job.
        bp_id_to_hash: dict[int, str] = {
            id(bp): h for h, bp in pipeline._persistent_node_map.items()
        }
        job._nodes = {
            label: job._persistent_node_map.get(
                bp_id_to_hash.get(id(bp_node), ""), bp_node
            )
            for label, bp_node in pipeline._nodes.items()
        }

        # Build job._node_graph as OrcaDAG[JobNode] from the job's own persistent
        # node map.  This makes job.dag consistent with the compiled-job path
        # (where compile() builds _node_graph from job nodes).
        # Callers needing blueprint nodes for rendering use job.pipeline.dag.
        job_dag: OrcaDAG = OrcaDAG()
        for node in job._persistent_node_map.values():
            job_dag.add_node(node)
        for u_hash, v_hash in job._graph_edges:
            u_node = job._persistent_node_map.get(u_hash)
            v_node = job._persistent_node_map.get(v_hash)
            if u_node is not None and v_node is not None:
                job_dag.add_edge(u_node, v_node)
        job._node_graph = job_dag

        if effective_store is not None:
            job._distribute_databases()

        # Restore run metadata (run_block was already parsed earlier in this method)
        status = run_block.get("status", "pending")
        if status in ("complete", "partial"):
            job._has_run = True
        if run_id := run_block.get("run_id"):
            job._run_id = run_id

        return job
