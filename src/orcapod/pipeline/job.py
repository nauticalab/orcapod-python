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
        self._unresolved_specs: list[str] = []
        self._has_run: bool = False
        self._run_id: str | None = None

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

    def _to_spec_stream(self, stream: cp.StreamProtocol) -> cp.StreamProtocol:
        """Convert *stream* to a spec-based equivalent for consistent hash recording.

        Concrete ``RootSource`` instances are promoted to ``SourceSpec`` via
        ``_ensure_spec``. ``DynamicPodStream`` instances have their upstreams
        recursively converted so that their content hash matches the
        ``OperatorNode`` recorded in ``_rec_node_lut``.

        Args:
            stream: The upstream stream to convert.

        Returns:
            A spec-based stream with a stable hash for recording.
        """
        from orcapod.core.operators.static_output_pod import DynamicPodStream

        if self._is_concrete_source(stream):
            return self._ensure_spec(stream)
        if isinstance(stream, DynamicPodStream):
            spec_upstreams = tuple(self._to_spec_stream(s) for s in stream.upstreams)
            return DynamicPodStream(
                pod=stream._pod,
                upstreams=spec_upstreams,
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
        from orcapod.core.nodes import FunctionNode

        input_stream = self._to_spec_stream(input_stream)

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

        processed = tuple(self._to_spec_stream(s) for s in upstreams)

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
    # _build_execution_graph
    # ------------------------------------------------------------------

    def _build_execution_graph(self) -> "tuple[Any, list[str]]":  # Any = nx.DiGraph
        """Build a fresh execution-ready graph with concrete sources substituted.

        Creates new SourceNode/FunctionNode/OperatorNode objects — does NOT
        mutate the existing node objects in ``pipeline._persistent_node_map``.

        Note:
            Updates ``pipeline._nodes`` with the fresh exec nodes (keyed by label)
            so that ``job.pipeline.compiled_nodes`` returns execution-ready nodes
            after a run. Repeated calls overwrite ``pipeline._nodes`` with a new
            set of exec nodes each time.

        Returns:
            Tuple of (exec_graph, unresolved_spec_names).

        Raises:
            ValueError: If ``self.store`` is ``None``.
            RuntimeError: If no compiled pipeline is available.
        """
        import networkx as nx
        from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
        from orcapod.core.sources.source_spec import SourceSpec
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

        exec_node_map: dict[str, "Any"] = {}
        excluded_hashes: set[str] = set()
        unresolved_specs: list[str] = []

        for node_hash in nx.topological_sort(G):
            if node_hash in excluded_hashes:
                continue

            if node_hash not in pipeline._node_lut:
                # Leaf stream — must be in _upstreams
                stream = pipeline._upstreams.get(node_hash)
                if stream is None:
                    continue
                if isinstance(stream, SourceSpec):
                    if stream.name in self._sources:
                        # Bound — use concrete source
                        exec_node_map[node_hash] = SourceNode(
                            stream=self._sources[stream.name]
                        )
                    else:
                        # Unbound — exclude this branch
                        excluded_hashes.add(node_hash)
                        if stream.name not in unresolved_specs:
                            unresolved_specs.append(stream.name)
                else:
                    exec_node_map[node_hash] = SourceNode(stream=stream)
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
                    new_fn = FunctionNode(
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
                    assert all(p in exec_node_map for p in preds), (
                        f"OperatorNode predecessor missing from exec_node_map: "
                        f"{[p for p in preds if p not in exec_node_map]}"
                    )
                    upstream_nodes = tuple(exec_node_map[p] for p in preds)
                    new_op = OperatorNode(
                        operator=template._operator,
                        input_streams=upstream_nodes,
                        label=template._label,
                    )
                    new_op.attach_databases(pipeline_database=pipeline_db)
                    exec_node_map[node_hash] = new_op

        # Build execution DiGraph with node objects as vertices
        exec_graph: "nx.DiGraph" = nx.DiGraph()
        for up_hash, down_hash in pipeline._graph_edges:
            if up_hash in exec_node_map and down_hash in exec_node_map:
                exec_graph.add_edge(exec_node_map[up_hash], exec_node_map[down_hash])
        for node in exec_node_map.values():
            if node not in exec_graph:
                exec_graph.add_node(node)

        # Update pipeline._nodes with fresh exec nodes (keyed by label)
        # This allows job.pipeline.compiled_nodes["adder"] to return the exec node
        for node in exec_node_map.values():
            if hasattr(node, "_label") and node._label:
                pipeline._nodes[node._label] = node

        return exec_graph, unresolved_specs

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

        from orcapod.pipeline.observer import NoOpObserver
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        exec_graph, unresolved_specs = self._build_execution_graph()

        effective_observer = observer or NoOpObserver()

        # Compute snapshot hash for run URI
        node_strs = sorted(
            str(n.content_hash().to_string())
            for n in exec_graph.nodes()
            if hasattr(n, "content_hash")
        )
        snapshot_hash = hashlib.sha256("\n".join(node_strs).encode()).hexdigest()[:16]
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

        # Return new job (different object); shares same compiled pipeline
        result = PipelineJob(
            name=self._pipeline_name,
            store=self._store,
            execution_context=self._execution_context,
            _pipeline=self._compiled_pipeline,
            sources=dict(self._sources),
        )
        result._unresolved_specs = unresolved_specs
        result._has_run = True
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

        status = "complete" if self._has_run else "pending"

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
            store=effective_store,
            _pipeline=pipeline,
            sources=sources,
        )

        # Restore run metadata
        run_block = data.get("run", {})
        if run_block.get("status") == "complete":
            job._has_run = True
            job._unresolved_specs = run_block.get("unresolved_specs", [])

        return job
