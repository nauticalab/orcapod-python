from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.nodes import (
    FunctionNode,
    GraphNode,
    OperatorNode,
    SourceNode,
)
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.protocols import core_protocols as cp
from orcapod.protocols import database_protocols as dbp
from orcapod.types import PipelineConfig
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Visualization helper (unrelated to pipeline node types)
# ---------------------------------------------------------------------------


class VizGraphNode:
    def __init__(self, label: str, id: int, kernel_type: str):
        self.label = label
        self.id = id
        self.kernel_type = kernel_type

    def __hash__(self):
        return hash((self.id, self.kernel_type))

    def __eq__(self, other):
        if not isinstance(other, VizGraphNode):
            return NotImplemented
        return (self.id, self.kernel_type) == (
            other.id,
            other.kernel_type,
        )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline(AutoRegisteringContextBasedTracker):
    """A persistent pipeline that records operator and function pod invocations.

    During the ``with`` block, operator and function pod invocations are
    recorded into an internal graph.  On context exit, ``compile()`` rewires
    the graph into execution-ready nodes:

    - Leaf streams -> ``SourceNode`` (thin wrapper for graph vertex)
    - Function pod invocations -> ``FunctionNode``
    - Operator invocations -> ``OperatorNode``

    Source caching is not a pipeline concern -- sources that need caching
    should be wrapped in a ``CachedSource`` before being used in the
    pipeline.

    All persistent nodes share the same ``pipeline_database`` and use
    ``pipeline_name`` as path prefix, scoping their cache tables.

    Parameters:
        name: Pipeline name (string or tuple).  Used as the path prefix for
            all cache/pipeline paths within the databases.
        pipeline_database: Database for pipeline records and operator caches.
        function_database: Optional separate database for function pod result
            caches.  When ``None``, ``pipeline_database`` is used with a
            ``_results`` subfolder under the pipeline name.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
    """

    def __init__(
        self,
        name: str | tuple[str, ...],
        pipeline_database: dbp.ArrowDatabaseProtocol,
        function_database: dbp.ArrowDatabaseProtocol | None = None,
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        auto_compile: bool = True,
    ) -> None:
        super().__init__(tracker_manager=tracker_manager)
        self._node_lut: dict[str, GraphNode] = {}
        self._upstreams: dict[str, cp.StreamProtocol] = {}
        self._graph_edges: list[tuple[str, str]] = []
        self._hash_graph: "nx.DiGraph" = nx.DiGraph()
        self._name = (name,) if isinstance(name, str) else tuple(name)
        self._pipeline_database = pipeline_database
        self._function_database = function_database
        self._pipeline_path_prefix = self._name
        self._nodes: dict[str, GraphNode] = {}
        self._persistent_node_map: dict[str, GraphNode] = {}
        self._node_graph: "nx.DiGraph | None" = None
        self._auto_compile = auto_compile
        self._compiled = False

    # ------------------------------------------------------------------
    # Recording (TrackerProtocol)
    # ------------------------------------------------------------------

    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        input_stream_hash = input_stream.content_hash().to_string()
        function_node = FunctionNode(
            function_pod=pod,
            input_stream=input_stream,
            label=label,
        )
        function_node_hash = function_node.content_hash().to_string()
        self._node_lut[function_node_hash] = function_node
        self._upstreams[input_stream_hash] = input_stream
        self._graph_edges.append((input_stream_hash, function_node_hash))
        self._hash_graph.add_edge(input_stream_hash, function_node_hash)
        if not self._hash_graph.nodes[function_node_hash].get("node_type"):
            self._hash_graph.nodes[function_node_hash]["node_type"] = "function"

    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        operator_node = OperatorNode(
            operator=pod,
            input_streams=upstreams,
            label=label,
        )
        operator_node_hash = operator_node.content_hash().to_string()
        self._node_lut[operator_node_hash] = operator_node
        upstream_hashes = [stream.content_hash().to_string() for stream in upstreams]
        for upstream_hash, upstream in zip(upstream_hashes, upstreams):
            self._upstreams[upstream_hash] = upstream
            self._graph_edges.append((upstream_hash, operator_node_hash))
            self._hash_graph.add_edge(upstream_hash, operator_node_hash)
        if not self._hash_graph.nodes[operator_node_hash].get("node_type"):
            self._hash_graph.nodes[operator_node_hash]["node_type"] = "operator"

    @property
    def nodes(self) -> list[GraphNode]:
        """Return the list of recorded (non-persistent) nodes."""
        return list(self._node_lut.values())

    @property
    def graph(self) -> "nx.DiGraph":
        """Directed graph of content-hash strings representing the accumulated
        pipeline structure.  Vertices are ``content_hash`` strings; node
        attributes include ``node_type`` ("source" / "function" / "operator")
        and, after ``compile()``, ``label`` and ``pipeline_hash``.

        The graph accumulates across multiple ``with`` blocks and is never
        cleared by ``reset()``.
        """
        return self._hash_graph

    def reset(self) -> None:
        """Clear session-scoped recorded state (node LUT, upstreams, edge list).

        Note: ``_hash_graph`` is intentionally *not* cleared -- it accumulates
        the pipeline structure across ``with`` blocks.
        """
        self._node_lut.clear()
        self._upstreams.clear()
        self._graph_edges.clear()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> tuple[str, ...]:
        return self._name

    @property
    def pipeline_database(self) -> dbp.ArrowDatabaseProtocol:
        return self._pipeline_database

    @property
    def function_database(self) -> dbp.ArrowDatabaseProtocol | None:
        return self._function_database

    @property
    def compiled_nodes(self) -> dict[str, GraphNode]:
        """Return a copy of the compiled nodes dict."""
        return self._nodes.copy()

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __exit__(self, exc_type=None, exc_value=None, traceback=None):
        super().__exit__(exc_type, exc_value, traceback)
        if self._auto_compile:
            self.compile()

    # ------------------------------------------------------------------
    # Compile
    # ------------------------------------------------------------------

    def compile(self) -> None:
        """Compile recorded invocations into execution-ready nodes.

        Walks the graph in topological order and:

        - Wraps leaf streams in ``SourceNode``
        - Rewires upstream references on recorded ``FunctionNode`` /
          ``OperatorNode`` to point at persistent (compiled) nodes
        - Attaches databases to function/operator nodes via
          ``attach_databases()``

        After compile, nodes are accessible by label as attributes on the
        pipeline instance.
        """
        from orcapod.core.nodes import (
            FunctionNode,
            OperatorNode,
        )

        G = nx.DiGraph()
        for edge in self._graph_edges:
            G.add_edge(*edge)

        # Seed from existing persistent nodes (incremental compile)
        persistent_node_map: dict[str, GraphNode] = dict(self._persistent_node_map)
        name_candidates: dict[str, list[GraphNode]] = {}

        for node_hash in nx.topological_sort(G):
            if node_hash in persistent_node_map:
                # Already compiled — reuse, but track for label assignment
                existing_node = persistent_node_map[node_hash]
                name_candidates.setdefault(existing_node.label, []).append(
                    existing_node
                )
                continue

            if node_hash not in self._node_lut:
                # -- Leaf stream: wrap in SourceNode --
                stream = self._upstreams[node_hash]
                node = SourceNode(stream=stream)
                persistent_node_map[node_hash] = node
            else:
                node = self._node_lut[node_hash]

                if isinstance(node, FunctionNode):
                    # Rewire input stream to persistent upstream
                    input_hash = node._input_stream.content_hash().to_string()
                    rewired_input = persistent_node_map[input_hash]
                    node.upstreams = (rewired_input,)

                    # Determine result database and path prefix
                    if self._function_database is not None:
                        result_db = self._function_database
                        result_prefix = None
                    else:
                        result_db = self._pipeline_database
                        result_prefix = self._name + ("_results",)

                    node.attach_databases(
                        pipeline_database=self._pipeline_database,
                        result_database=result_db,
                        result_path_prefix=result_prefix,
                        pipeline_path_prefix=self._pipeline_path_prefix,
                    )

                    # Default to LocalExecutor so capture/logging works
                    # out of the box. Replaced if execution_engine is set.
                    if node.executor is None:
                        from orcapod.core.executors.local import LocalExecutor

                        node.executor = LocalExecutor()

                elif isinstance(node, OperatorNode):
                    # Rewire all input streams to persistent upstreams
                    rewired_inputs = tuple(
                        persistent_node_map[s.content_hash().to_string()]
                        for s in node.upstreams
                    )
                    node.upstreams = rewired_inputs

                    node.attach_databases(
                        pipeline_database=self._pipeline_database,
                        pipeline_path_prefix=self._pipeline_path_prefix,
                    )

                else:
                    raise TypeError(
                        f"Unknown node type in pipeline graph: {type(node)}"
                    )

                persistent_node_map[node_hash] = node

            # Track all nodes for label assignment
            name_candidates.setdefault(node.label, []).append(node)

        # Save persistent node map for incremental re-compile
        self._persistent_node_map = persistent_node_map

        # Build node graph for run() ordering
        self._node_graph = nx.DiGraph()
        for upstream_hash, downstream_hash in self._graph_edges:
            upstream_node = persistent_node_map.get(upstream_hash)
            downstream_node = persistent_node_map.get(downstream_hash)
            if upstream_node is not None and downstream_node is not None:
                self._node_graph.add_edge(upstream_node, downstream_node)
        # Add isolated nodes (sources with no downstream in edges)
        for node in persistent_node_map.values():
            if node not in self._node_graph:
                self._node_graph.add_node(node)

        # Enrich hash graph with compiled node metadata (label, pipeline_hash, node_type)
        for node_hash, node in persistent_node_map.items():
            if node_hash not in self._hash_graph:
                continue
            attrs = self._hash_graph.nodes[node_hash]
            if not attrs.get("node_type"):
                if isinstance(node, SourceNode):
                    attrs["node_type"] = "source"
                elif isinstance(node, FunctionNode):
                    attrs["node_type"] = "function"
                elif isinstance(node, OperatorNode):
                    attrs["node_type"] = "operator"
            if not attrs.get("label"):
                computed = node.label or (
                    node.computed_label() if hasattr(node, "computed_label") else None
                )
                if computed:
                    attrs["label"] = computed
            if not attrs.get("pipeline_hash"):
                attrs["pipeline_hash"] = node.pipeline_hash().to_string()

        # Assign labels, disambiguating collisions by content hash
        self._nodes.clear()
        for label, nodes in name_candidates.items():
            if len(nodes) > 1:
                # Sort by content hash for deterministic disambiguation
                sorted_nodes = sorted(nodes, key=lambda n: n.content_hash().to_string())
                for i, node in enumerate(sorted_nodes, start=1):
                    key = f"{label}_{i}"
                    self._nodes[key] = node
                    node._label = key
            else:
                self._nodes[label] = nodes[0]

        self._compiled = True

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(
        self,
        orchestrator=None,
        config: PipelineConfig | None = None,
        execution_engine: cp.PacketFunctionExecutorProtocol | None = None,
        execution_engine_opts: "dict[str, Any] | None" = None,
    ) -> None:
        """Execute all compiled nodes.

        Args:
            orchestrator: Optional orchestrator instance. When provided,
                the orchestrator drives execution and nodes handle their
                own persistence internally. When omitted, defaults to
                ``SyncPipelineOrchestrator`` (sync mode) or
                ``AsyncPipelineOrchestrator`` (async mode).
            config: Pipeline configuration. When ``config.executor`` is
                ``ExecutorType.ASYNC_CHANNELS``, the pipeline runs
                asynchronously via the orchestrator. When ``config`` is
                omitted and an ``execution_engine`` is provided, async mode
                is used by default. Passing an explicit ``config`` always
                takes priority — supply ``ExecutorType.SYNCHRONOUS`` to force
                synchronous execution even when an engine is present.
            execution_engine: Optional packet-function executor applied to
                every function node before execution (e.g. a ``RayExecutor``).
                Overrides ``config.execution_engine`` when both are provided.
            execution_engine_opts: Resource/options dict forwarded to the
                engine via ``with_options()`` (e.g. ``{"num_cpus": 4}``).
                Overrides ``config.execution_engine_opts`` when both are
                provided.
        """
        from orcapod.types import ExecutorType, PipelineConfig

        explicit_config = config is not None
        config = config or PipelineConfig()

        # Explicit kwargs take precedence over values baked into config.
        effective_engine = (
            execution_engine
            if execution_engine is not None
            else config.execution_engine
        )
        effective_opts = (
            execution_engine_opts
            if execution_engine_opts is not None
            else config.execution_engine_opts
        )

        if not self._compiled:
            self.compile()

        if effective_engine is not None:
            self._apply_execution_engine(effective_engine, effective_opts)

        snapshot_hash = self._compute_pipeline_snapshot_hash()
        pipeline_uri = "/".join(self._name) + "@" + snapshot_hash

        if orchestrator is not None:
            orchestrator.run(
                self._node_graph,
                pipeline_uri=pipeline_uri,
            )
        else:
            # Default to async when an execution engine is provided, unless
            # the caller explicitly supplied a config — in which case
            # config.executor is authoritative and takes priority.
            use_async = config.executor == ExecutorType.ASYNC_CHANNELS or (
                effective_engine is not None and not explicit_config
            )
            if use_async:
                from orcapod.pipeline.async_orchestrator import (
                    AsyncPipelineOrchestrator,
                )

                AsyncPipelineOrchestrator(
                    buffer_size=config.channel_buffer_size,
                ).run(
                    self._node_graph,
                    pipeline_uri=pipeline_uri,
                )
            else:
                from orcapod.pipeline.sync_orchestrator import (
                    SyncPipelineOrchestrator,
                )

                SyncPipelineOrchestrator().run(
                    self._node_graph,
                    pipeline_uri=pipeline_uri,
                )

        self.flush()

    def _apply_execution_engine(
        self,
        execution_engine: cp.PacketFunctionExecutorProtocol,
        execution_engine_opts: dict[str, Any] | None,
    ) -> None:
        """Apply *execution_engine* to every ``FunctionNode`` in the pipeline.

        Each node receives its own executor instance via
        ``engine.with_options(**opts)`` — even when *opts* is empty.
        The executor's ``with_options`` implementation decides which
        components to copy vs share (e.g. connection handles may be
        shared while per-node state is copied).

        Args:
            execution_engine: Executor to apply (must implement
                ``PythonFunctionExecutorBase`` or at minimum expose
                ``with_options``).
            execution_engine_opts: Pipeline-level options dict, or
                ``None`` for no defaults.
        """
        assert self._node_graph is not None, (
            "_apply_execution_engine called before compile()"
        )

        opts = execution_engine_opts or {}

        for node in self._node_graph.nodes:
            if not isinstance(node, FunctionNode):
                continue
            node.executor = execution_engine.with_options(**opts)
            logger.debug(
                "Applied execution engine %r to node %r (opts=%r)",
                type(execution_engine).__name__,
                node.label,
                opts or None,
            )

    def _compute_pipeline_snapshot_hash(self) -> str:
        """Compute a content hash of the compiled pipeline structure.

        Uses a deterministic topological ordering (Kahn's algorithm with a
        min-heap frontier for O((n+e) log n) content-hash tie-breaking) over
        the ``_hash_graph``, whose node keys are content-hash strings.
        The canonical input to SHA-256 includes both the ordered node
        sequence *and* all edges (sorted ``u->v`` pairs), so the digest
        changes whenever nodes or edges are added, removed, or modified.

        Returns:
            A 16-character hex string (truncated SHA-256 prefix), or
            ``""`` if the graph is empty.
        """
        import hashlib
        import heapq

        g = self._hash_graph
        if not g or len(g) == 0:
            return ""

        # Kahn's algorithm with min-heap frontier for deterministic ordering.
        in_degree: dict[str, int] = {n: g.in_degree(n) for n in g}
        frontier: list[str] = [n for n, deg in in_degree.items() if deg == 0]
        heapq.heapify(frontier)
        ordered: list[str] = []

        while frontier:
            node = heapq.heappop(frontier)
            ordered.append(node)
            for successor in g.successors(node):
                in_degree[successor] -= 1
                if in_degree[successor] == 0:
                    heapq.heappush(frontier, successor)

        # Include both nodes (topo order) and edges (sorted) so topology
        # changes that preserve node identity still change the hash.
        node_lines = [f"N:{n}" for n in ordered]
        edge_lines = [f"E:{u}->{v}" for u, v in sorted(g.edges())]
        combined = "\n".join(node_lines + edge_lines)
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def flush(self) -> None:
        """Flush all databases."""
        self._pipeline_database.flush()
        if self._function_database is not None:
            self._function_database.flush()

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Serialize the compiled pipeline to a JSON file.

        Args:
            path: File path to write the JSON output to.

        Raises:
            ValueError: If the pipeline has not been compiled.
        """
        if not self._compiled:
            raise ValueError(
                "Pipeline is not compiled. Call compile() or use "
                "auto_compile=True before saving."
            )

        from orcapod.core.nodes import OperatorNode
        from orcapod.pipeline.serialization import (
            PIPELINE_FORMAT_VERSION,
            serialize_schema,
        )

        # -- Pipeline metadata --
        pipeline_meta: dict[str, Any] = {
            "name": list(self._name),
            "databases": {
                "pipeline_database": self._pipeline_database.to_config(),
                "function_database": (
                    self._function_database.to_config()
                    if self._function_database is not None
                    else None
                ),
            },
        }

        # -- Build node descriptors --
        nodes: dict[str, dict[str, Any]] = {}
        for content_hash_str, node in self._persistent_node_map.items():
            tag_schema, packet_schema = node.output_schema()
            type_converter = node.data_context.type_converter
            descriptor: dict[str, Any] = {
                "node_type": node.node_type,
                "label": node.label,
                "content_hash": node.content_hash().to_string(),
                "pipeline_hash": node.pipeline_hash().to_string(),
                "data_context_key": node.data_context_key,
                "output_schema": {
                    "tag": serialize_schema(tag_schema, type_converter),
                    "packet": serialize_schema(packet_schema, type_converter),
                },
            }

            if isinstance(node, SourceNode):
                descriptor.update(self._build_source_descriptor(node))
            elif isinstance(node, FunctionNode):
                descriptor.update(self._build_function_descriptor(node))
            elif isinstance(node, OperatorNode):
                descriptor.update(self._build_operator_descriptor(node))

            nodes[content_hash_str] = descriptor

        # -- Edges --
        edges = [list(edge) for edge in self._graph_edges]

        # -- Assemble top-level structure --
        output = {
            "orcapod_pipeline_version": PIPELINE_FORMAT_VERSION,
            "pipeline": pipeline_meta,
            "nodes": nodes,
            "edges": edges,
        }

        with open(path, "w") as f:
            json.dump(output, f, indent=2)

    # Reconstructable source types: file-backed sources that can be
    # rebuilt from config alone.
    _RECONSTRUCTABLE_SOURCE_TYPES = frozenset({"csv", "delta_table", "cached"})

    def _build_source_descriptor(self, node: SourceNode) -> dict[str, Any]:
        """Build source-specific descriptor fields for a SourceNode.

        Args:
            node: The SourceNode to describe.

        Returns:
            Dict with source-specific fields.
        """
        stream = node.stream

        # Determine if stream implements SourceProtocol and build descriptor accordingly
        # TODO: revisit this logic
        if isinstance(stream, cp.SourceProtocol):
            config = stream.to_config()
            stream_type = config.get("source_type", "stream")
            source_config = config
            reconstructable = stream_type in self._RECONSTRUCTABLE_SOURCE_TYPES
        else:
            stream_type = "stream"
            source_config = None
            reconstructable = False

        source_id = getattr(stream, "source_id", None)

        return {
            "stream_type": stream_type,
            "source_id": source_id,
            "reconstructable": reconstructable,
            "source_config": source_config,
        }

    def _build_function_descriptor(self, node: "FunctionNode") -> dict[str, Any]:
        """Build function-specific descriptor fields for a FunctionNode.

        Args:
            node: The FunctionNode to describe.

        Returns:
            Dict with function-specific fields.
        """
        return {
            "function_pod": node._function_pod.to_config(),
            "pipeline_path": list(node.pipeline_path),
            "result_record_path": list(node._cached_function_pod.record_path),
        }

    def _build_operator_descriptor(self, node: OperatorNode) -> dict[str, Any]:
        """Build operator-specific descriptor fields for a OperatorNode.

        Args:
            node: The OperatorNode to describe.

        Returns:
            Dict with operator-specific fields.
        """
        return {
            "operator": node._operator.to_config(),
            "cache_mode": node._cache_mode.name,
            "pipeline_path": list(node.pipeline_path),
        }

    @classmethod
    def load(cls, path: str | Path, mode: str = "full") -> "Pipeline":
        """Deserialize a pipeline from a JSON file.

        Reconstructs the pipeline graph from the serialized descriptor,
        rebuilding nodes in topological order.  The *mode* parameter
        controls how aggressively live objects are reconstructed:

        - ``"full"``: attempt to reconstruct live sources, function pods,
          and operators so the pipeline can be re-run.  Falls back to
          read-only per-node when reconstruction fails.
        - ``"read_only"``: load metadata only; no live sources or
          function pods are reconstructed.

        Args:
            path: Path to the JSON file produced by `save`.
            mode: ``"full"`` (default) or ``"read_only"``.

        Returns:
            A compiled ``Pipeline`` instance.

        Raises:
            ValueError: If the file's format version is unsupported.
        """

        from orcapod.pipeline.serialization import (
            SUPPORTED_FORMAT_VERSIONS,
            LoadStatus,
            resolve_database_from_config,
            resolve_operator_from_config,
            resolve_source_from_config,
        )

        path = Path(path)
        with open(path) as f:
            data = json.load(f)

        # 1. Validate version
        version = data.get("orcapod_pipeline_version", "")
        if version not in SUPPORTED_FORMAT_VERSIONS:
            raise ValueError(
                f"Unsupported pipeline format version {version!r}. "
                f"Supported versions: {sorted(SUPPORTED_FORMAT_VERSIONS)}"
            )

        # 2. Reconstruct databases
        pipeline_meta = data["pipeline"]
        db_configs = pipeline_meta["databases"]

        pipeline_db = resolve_database_from_config(db_configs["pipeline_database"])
        function_db = (
            resolve_database_from_config(db_configs["function_database"])
            if db_configs.get("function_database") is not None
            else None
        )

        # 3. Build edge graph and derive topological order
        nodes_data = data["nodes"]
        edges = data["edges"]

        edge_graph: nx.DiGraph = nx.DiGraph()
        for upstream_hash, downstream_hash in edges:
            edge_graph.add_edge(upstream_hash, downstream_hash)
        # Add isolated nodes (nodes with no edges)
        for node_hash in nodes_data:
            if node_hash not in edge_graph:
                edge_graph.add_node(node_hash)

        topo_order = list(nx.topological_sort(edge_graph))

        # 4. Walk nodes in topological order, reconstruct each
        reconstructed: dict[str, SourceNode | FunctionNode | OperatorNode] = {}

        # Build reverse edge map: downstream -> list of upstream hashes
        upstream_map: dict[str, list[str]] = {}
        for up_hash, down_hash in edges:
            upstream_map.setdefault(down_hash, []).append(up_hash)

        for node_hash in topo_order:
            descriptor = nodes_data.get(node_hash)
            if descriptor is None:
                continue

            node_type = descriptor.get("node_type")

            if node_type == "source":
                node = cls._load_source_node(
                    descriptor, mode, resolve_source_from_config
                )
                reconstructed[node_hash] = node

            elif node_type == "function":
                # Determine upstream node
                up_hashes = upstream_map.get(node_hash, [])
                upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None

                # Check if upstream is usable for full mode
                upstream_usable = (
                    upstream_node is not None
                    and hasattr(upstream_node, "load_status")
                    and upstream_node.load_status
                    in (LoadStatus.FULL, LoadStatus.READ_ONLY)
                )

                # Build databases dict
                result_db = function_db if function_db is not None else pipeline_db
                dbs = {"pipeline": pipeline_db, "result": result_db}

                node = cls._load_function_node(
                    descriptor, mode, upstream_node, upstream_usable, dbs
                )
                reconstructed[node_hash] = node

            elif node_type == "operator":
                up_hashes = upstream_map.get(node_hash, [])
                upstream_nodes = tuple(
                    reconstructed[h] for h in up_hashes if h in reconstructed
                )

                # Check if all upstreams are usable
                all_upstreams_usable = (
                    all(
                        hasattr(n, "load_status")
                        and n.load_status
                        in (LoadStatus.FULL, LoadStatus.READ_ONLY)
                        for n in upstream_nodes
                    )
                    if upstream_nodes
                    else False
                )

                dbs = {"pipeline": pipeline_db}

                node = cls._load_operator_node(
                    descriptor,
                    mode,
                    upstream_nodes,
                    all_upstreams_usable,
                    dbs,
                    resolve_operator_from_config,
                )
                reconstructed[node_hash] = node

        # 5. Build Pipeline instance
        name = tuple(pipeline_meta["name"])
        pipeline = cls(
            name=name,
            pipeline_database=pipeline_db,
            function_database=function_db,
            auto_compile=False,
        )

        # Populate persistent node map
        pipeline._persistent_node_map = dict(reconstructed)

        # Populate _nodes (label -> node) for all labeled nodes.
        # Unlike compile() which excludes source nodes from _nodes,
        # loaded pipelines include them so users can inspect load_status
        # and metadata for all nodes via attribute access.
        pipeline._nodes = {}
        for node_hash, node in reconstructed.items():
            label = node.label
            if label:
                pipeline._nodes[label] = node

        # Build node graph
        pipeline._node_graph = nx.DiGraph()
        for up_hash, down_hash in edges:
            up_node = reconstructed.get(up_hash)
            down_node = reconstructed.get(down_hash)
            if up_node is not None and down_node is not None:
                pipeline._node_graph.add_edge(up_node, down_node)
        for node in reconstructed.values():
            if node not in pipeline._node_graph:
                pipeline._node_graph.add_node(node)

        # Restore graph edges as content_hash string pairs
        pipeline._graph_edges = [(up, down) for up, down in edges]

        # Rebuild _hash_graph
        pipeline._hash_graph = nx.DiGraph()
        for up_hash, down_hash in edges:
            pipeline._hash_graph.add_edge(up_hash, down_hash)
        for node_hash, node in reconstructed.items():
            if node_hash not in pipeline._hash_graph:
                pipeline._hash_graph.add_node(node_hash)
            attrs = pipeline._hash_graph.nodes[node_hash]
            attrs["node_type"] = node.node_type
            if node.label:
                attrs["label"] = node.label

        pipeline._compiled = True

        return pipeline

    @staticmethod
    def _load_source_node(
        descriptor: dict[str, Any],
        mode: str,
        resolve_source_from_config: Any,
    ) -> SourceNode:
        """Reconstruct a SourceNode from a descriptor.

        Args:
            descriptor: The serialized node descriptor.
            mode: Load mode (``"full"`` or ``"read_only"``).
            resolve_source_from_config: Callable to reconstruct a source.

        Returns:
            A ``SourceNode`` instance.
        """

        reconstructable = descriptor.get("reconstructable", False)
        source_config = descriptor.get("source_config")

        stream = None
        if reconstructable and mode != "read_only" and source_config is not None:
            try:
                stream = resolve_source_from_config(source_config)
            except Exception:
                logger.warning(
                    "Failed to reconstruct source %r, falling back to read-only.",
                    descriptor.get("label"),
                )
                stream = None

        return SourceNode.from_descriptor(descriptor, stream=stream, databases={})

    @staticmethod
    def _load_function_node(
        descriptor: dict[str, Any],
        mode: str,
        upstream_node: Any | None,
        upstream_usable: bool,
        databases: dict[str, Any],
    ) -> FunctionNode:
        """Reconstruct a FunctionNode from a descriptor.

        When the upstream is usable and mode is not ``"read_only"``, attempts
        to reconstruct the function pod with ``fallback_to_proxy=True`` so
        that a ``PacketFunctionProxy`` is used when the original function
        cannot be imported.

        When the upstream is UNAVAILABLE (but exists), still builds the proxy
        pod and wires it up — ``from_descriptor`` will detect the unavailable
        stream and set ``LoadStatus.CACHE_ONLY`` so the node can serve all
        cached results from persistent storage without touching the upstream.

        Args:
            descriptor: The serialized node descriptor.
            mode: Load mode.
            upstream_node: The reconstructed upstream node, or ``None``.
            upstream_usable: Whether the upstream is usable (FULL or
                READ_ONLY).
            databases: Database role mapping.

        Returns:
            A ``FunctionNode`` instance.
        """
        from orcapod.core.function_pod import FunctionPod
        from orcapod.pipeline.serialization import LoadStatus

        if mode != "read_only" and upstream_usable:
            try:
                pod = FunctionPod.from_config(
                    descriptor["function_pod"], fallback_to_proxy=True
                )
                node = FunctionNode.from_descriptor(
                    descriptor,
                    function_pod=pod,
                    input_stream=upstream_node,
                    databases=databases,
                )
                # load_status is set inside from_descriptor based on
                # upstream availability and function proxy status.
                return node
            except Exception:
                logger.warning(
                    "Failed to reconstruct function node %r, "
                    "falling back to read-only.",
                    descriptor.get("label"),
                )
        elif mode != "read_only" and not upstream_usable and upstream_node is not None:
            # Upstream exists but is UNAVAILABLE — build a proxy pod so the
            # node can serve cached results in CACHE_ONLY mode.
            try:
                pod = FunctionPod.from_config(
                    descriptor["function_pod"], fallback_to_proxy=True
                )
                node = FunctionNode.from_descriptor(
                    descriptor,
                    function_pod=pod,
                    input_stream=upstream_node,
                    databases=databases,
                )
                return node
            except Exception:
                logger.warning(
                    "Failed to reconstruct function node %r in cache-only mode, "
                    "falling back to unavailable.",
                    descriptor.get("label"),
                )
        elif mode != "read_only" and not upstream_usable:
            logger.warning(
                "Upstream for function node %r is not usable, "
                "falling back to read-only.",
                descriptor.get("label"),
            )

        return FunctionNode.from_descriptor(
            descriptor,
            function_pod=None,
            input_stream=None,
            databases=databases,
        )

    @staticmethod
    def _load_operator_node(
        descriptor: dict[str, Any],
        mode: str,
        upstream_nodes: tuple,
        all_upstreams_usable: bool,
        databases: dict[str, Any],
        resolve_operator_from_config: Any,
    ) -> "OperatorNode":
        """Reconstruct a OperatorNode from a descriptor.

        Args:
            descriptor: The serialized node descriptor.
            mode: Load mode.
            upstream_nodes: Tuple of reconstructed upstream nodes.
            all_upstreams_usable: Whether all upstreams are in FULL mode.
            databases: Database role mapping.
            resolve_operator_from_config: Callable to reconstruct an operator.

        Returns:
            A ``OperatorNode`` instance.
        """
        from orcapod.core.nodes import OperatorNode

        if mode != "read_only":
            if not all_upstreams_usable:
                logger.warning(
                    "Upstream(s) for operator node %r are not usable, "
                    "falling back to read-only.",
                    descriptor.get("label"),
                )
            else:
                try:
                    op = resolve_operator_from_config(descriptor["operator"])
                    return OperatorNode.from_descriptor(
                        descriptor,
                        operator=op,
                        input_streams=upstream_nodes,
                        databases=databases,
                    )
                except Exception:
                    logger.warning(
                        "Failed to reconstruct operator node %r, "
                        "falling back to read-only.",
                        descriptor.get("label"),
                    )

        return OperatorNode.from_descriptor(
            descriptor,
            operator=None,
            input_streams=(),
            databases=databases,
        )

    # ------------------------------------------------------------------
    # Node access by label
    # ------------------------------------------------------------------

    def __getattr__(self, item: str) -> Any:
        # Use __dict__ to avoid recursion during __init__
        nodes = self.__dict__.get("_nodes", {})
        if item in nodes:
            return nodes[item]
        raise AttributeError(f"Pipeline has no attribute '{item}'")

    def __dir__(self) -> list[str]:
        return list(super().__dir__()) + list(self._nodes.keys())


# ===========================================================================
# Graph Rendering Utilities
# ===========================================================================


class GraphRenderer:
    """Improved GraphRenderer with centralized default styling"""

    # ====================
    # CENTRALIZED DEFAULTS
    # ====================
    DEFAULT_STYLES = {
        "rankdir": "TB",
        "node_shape": "box",
        "node_style": "filled",
        "node_color": "navy",
        "font_color": "white",
        "type_font_color": "#54508C",  # muted navy blue
        "font_name": "sans-serif",
        "font_path": None,  # Set to None by default
        # 'font_path': './assets/fonts/LexendDeca-Medium.ttf',
        "edge_color": "black",
        "dpi": 150,
        # HTML Label defaults
        "main_font_size": 14,  # Main label font size
        "type_font_size": 11,  # PodProtocol type font size (small)
        "type_style": "normal",  # PodProtocol type text style
    }

    DEFAULT_STYLE_RULES = {
        "source": {
            "fillcolor": "white",
            "shape": "rect",
            "fontcolor": "black",
            "style": "filled",
            "typefontcolor": "#3A3737",  # dark gray
        },
        "operator": {
            "fillcolor": "#DFD6CF",  # pale beige
            "shape": "diamond",
            "fontcolor": "black",
            "style": "filled",
            "typefontcolor": "#3A3737",  # dark gray
        },
        "function": {
            "fillcolor": "#f5f5f5",  # off white
            "shape": "cylinder",
            "fontcolor": "#090271",  # darker navy blue
            "style": "filled",
            "typefontcolor": "#3A3737",  # dark gray
        },
    }

    DARK_THEME_RULES = {
        "source": {
            "fillcolor": "black",
            "shape": "rect",
            "fontcolor": "white",
            "style": "filled",
            "typefontcolor": "lightgray",  # Light text for dark background
        },
        "operator": {
            "fillcolor": "#026e8e",  # ocean blue
            "shape": "diamond",
            "fontcolor": "white",
            "style": "filled",
            "typefontcolor": "lightgray",  # Light text for dark background
        },
        "pod": {
            "fillcolor": "#090271",  # darker navy blue
            "shape": "cylinder",
            "fontcolor": "white",
            "style": "filled",
            "typefontcolor": "lightgray",  # Light text for dark background
        },
    }

    def __init__(self):
        pass

    def _sanitize_node_id(self, node_id: Any) -> str:
        return f"node_{hash(node_id)}"

    def _create_default_html_label(self, node, node_attrs) -> str:
        """
        Create HTML for the label (text) section of the node

        Format:
        kernel_type     (11pt, small text)
        main_label     (14pt, normal text)
        """

        main_label = str(node.label) if hasattr(node, "label") else str(node)
        kernel_type = str(node.kernel_type) if hasattr(node, "kernel_type") else ""

        if not kernel_type:
            # No kernel_type, just return main label
            return f'<FONT POINT-SIZE="{self.DEFAULT_STYLES["main_font_size"]}">{main_label}</FONT>'

        # Create HTML label: small kernel_type above, main label below
        main_size = self.DEFAULT_STYLES["main_font_size"]
        type_size = self.DEFAULT_STYLES["type_font_size"]
        font_name = self.DEFAULT_STYLES["font_name"]
        type_font_color = node_attrs.get(
            "typefontcolor", self.DEFAULT_STYLES["type_font_color"]
        )

        html_label = f'''<
        <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
            <TR><TD ALIGN="CENTER"><FONT POINT-SIZE="{type_size}" COLOR="{type_font_color}" FACE="{font_name}, bold">{kernel_type}</FONT></TD></TR>
            <TR><TD ALIGN="CENTER"><FONT POINT-SIZE="{main_size}">{main_label}</FONT></TD></TR>
        </TABLE>
        >'''

        return html_label

    def _get_node_label(
        self, node_id: Any, label_lut: dict[Any, str] | None = None
    ) -> str:
        if label_lut and node_id in label_lut:
            return label_lut[node_id]
        return str(node_id)

    def _get_node_attributes(
        self, node_id: Any, style_rules: dict | None = None
    ) -> dict[str, str]:
        """
        Get styling attributes for a specific node based on its properties
        """
        # Use provided rules or defaults
        rules = style_rules or self.DEFAULT_STYLE_RULES

        # Default attributes
        default_attrs = {
            "fillcolor": self.DEFAULT_STYLES["node_color"],
            "shape": self.DEFAULT_STYLES["node_shape"],
            "fontcolor": self.DEFAULT_STYLES["font_color"],
            "fontname": self.DEFAULT_STYLES["font_name"],
            "fontsize": self.DEFAULT_STYLES.get("fontsize", "14"),
            "style": self.DEFAULT_STYLES["node_style"],
            "typefontcolor": self.DEFAULT_STYLES["type_font_color"],
        }

        # Check if node has kernel_type attribute
        if hasattr(node_id, "kernel_type"):
            kernel_type = node_id.kernel_type
            if kernel_type in rules:
                # Override defaults with rule-specific attributes
                rule_attrs = rules[kernel_type].copy()
                default_attrs.update(rule_attrs)

        return default_attrs

    def _merge_styles(self, **override_styles) -> dict:
        """
        CENTRAL STYLE MERGING
        Takes the default styles and overrides them with any user-provided styles.
        """
        merged = self.DEFAULT_STYLES.copy()
        merged.update(override_styles)  # Override defaults with user choices
        return merged

    def generate_dot(
        self,
        graph: "nx.DiGraph",
        label_lut: dict[Any, str] | None = None,
        style_rules: dict | None = None,
        **style_overrides,
    ) -> str:
        # Get final styles (defaults + overrides)
        styles = self._merge_styles(**style_overrides)

        import graphviz

        dot = graphviz.Digraph(comment="NetworkX Graph")

        # Apply global styles
        dot.attr(rankdir=styles["rankdir"], dpi=str(styles["dpi"]))
        dot.attr(fontname=styles["font_name"])
        if styles.get("font_size"):
            dot.attr(fontsize=styles["fontsize"])
        if styles["font_path"]:
            dot.attr(fontpath=styles["font_path"])

        # Set default edge attributes
        dot.attr("edge", color=styles["edge_color"])

        # Add nodes with default attribute specific styling
        for node_id in graph.nodes():
            sanitized_id = self._sanitize_node_id(node_id)

            node_attrs = self._get_node_attributes(node_id, style_rules)

            if label_lut and node_id in label_lut:
                # Use custom label if provided
                label = label_lut[node_id]
            else:
                # Use default HTML label with kernel_type above main label
                label = self._create_default_html_label(node_id, node_attrs)

            # Add nodes with its specific attributes
            dot.node(sanitized_id, label=label, **node_attrs)

        # Add edges
        for source, target in graph.edges():
            source_id = self._sanitize_node_id(source)
            target_id = self._sanitize_node_id(target)
            dot.edge(source_id, target_id)

        return dot.source

    def render_graph(
        self,
        graph: "nx.DiGraph",
        label_lut: dict[Any, str] | None = None,
        show: bool = True,
        output_path: str | None = None,
        raw_output: bool = False,
        figsize: tuple = (12, 8),
        dpi: int = 150,
        style_rules: dict | None = None,
        **style_overrides,
    ) -> str | None:
        # Always generate DOT first
        dot_text = self.generate_dot(graph, label_lut, style_rules, **style_overrides)

        if raw_output:
            return dot_text

        # For rendering, continue with the existing logic but return DOT text
        styles = self._merge_styles(**style_overrides)

        import graphviz

        dot = graphviz.Digraph(comment="NetworkX Graph")

        # Apply styles directly
        dot.attr(rankdir=styles["rankdir"], dpi=str(dpi))
        dot.attr(fontname=styles["font_name"])
        if styles.get("fontsize"):
            dot.attr(fontsize=styles["fontsize"])
        if styles["font_path"]:
            dot.attr(fontpath=styles["font_path"])

        # Set default edge attributes
        dot.attr("edge", color=styles["edge_color"])

        # Add nodes with specific styling
        for node_id in graph.nodes():
            sanitized_id = self._sanitize_node_id(node_id)
            node_attrs = self._get_node_attributes(node_id, style_rules)

            if label_lut and node_id in label_lut:
                label = label_lut[node_id]
            else:
                label = self._create_default_html_label(node_id, node_attrs)

            dot.node(sanitized_id, label=label, **node_attrs)

        # Add edges
        for source, target in graph.edges():
            source_id = self._sanitize_node_id(source)
            target_id = self._sanitize_node_id(target)
            dot.edge(source_id, target_id)

        if output_path:
            name, ext = os.path.splitext(output_path)
            format_type = ext[1:] if ext else "png"
            dot.render(name, format=format_type, cleanup=True)
            print(f"Graph saved to {output_path}")

        import matplotlib.image as mpimg
        import matplotlib.pyplot as plt

        if show:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                dot.render(tmp.name[:-4], format="png", cleanup=True)
                img = mpimg.imread(tmp.name)
                plt.figure(figsize=figsize, dpi=dpi)
                plt.imshow(img)
                plt.axis("off")
                plt.tight_layout()
                plt.show()
                os.unlink(tmp.name)

        # Always return DOT text (like the spec)
        return dot_text


# =====================
# CONVENIENCE FUNCTION
# =====================
def render_graph(
    graph: "nx.DiGraph",
    label_lut: dict[Any, str] | None = None,
    style_rules: dict | None = None,
    **kwargs,
) -> str | None:
    """
    Convenience function with conditional node styling

    Args:
        graph: NetworkX DiGraph
        label_lut: Optional node labels
        style_rules: Dict mapping node attributes to styling rules
        **kwargs: Other styling arguments
    """
    renderer = GraphRenderer()
    return renderer.render_graph(graph, label_lut, style_rules=style_rules, **kwargs)


def render_graph_dark_theme(
    graph: "nx.DiGraph", label_lut: dict[Any, str] | None = None, **kwargs
) -> str | None:
    """
    Render with dark theme - all backgrounds dark, all pod type fonts light
    Perfect for dark themed presentations or displays
    """
    renderer = GraphRenderer()
    return renderer.render_graph(
        graph, label_lut, style_rules=renderer.DARK_THEME_RULES, **kwargs
    )


# =============================================
# STYLE RULE SETS
# =============================================


class StyleRuleSets:
    """Access to different theme style rules"""

    @staticmethod
    def get_default_rules():
        """Mixed theme - light node fill colors with dark colored fonts"""
        return GraphRenderer.DEFAULT_STYLE_RULES

    @staticmethod
    def get_dark_rules():
        """Dark theme - dark node fill colors with light colored fonts"""
        return GraphRenderer.DARK_THEME_RULES

    @staticmethod
    def create_custom_rules(
        source_bg="lightgreen",
        operator_bg="orange",
        pod_bg="darkslateblue",
        source_main_fcolor="black",
        operator_main_fcolor="black",
        pod_main_fcolor="white",
        source_type_fcolor="darkgray",
        operator_type_fcolor="darkgray",
        kernel_type_fcolor="lightgray",
    ):
        """Create custom theme rules"""
        return {
            "source": {
                "fillcolor": source_bg,
                "shape": "ellipse",
                "fontcolor": source_main_fcolor,
                "style": "filled",
                "type_font_color": source_type_fcolor,
            },
            "operator": {
                "fillcolor": operator_bg,
                "shape": "diamond",
                "fontcolor": operator_main_fcolor,
                "style": "filled",
                "type_font_color": operator_type_fcolor,
            },
            "function": {
                "fillcolor": pod_bg,
                "shape": "box",
                "fontcolor": pod_main_fcolor,
                "style": "filled,rounded",
                "type_font_color": kernel_type_fcolor,
            },
        }
