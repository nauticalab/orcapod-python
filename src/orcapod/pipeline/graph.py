from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.nodes import GraphNode, SourceNode
from orcapod.core.tracker import GraphTracker
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


class Pipeline(GraphTracker):
    """
    A persistent pipeline that extends ``GraphTracker``.

    During the ``with`` block, operator and function pod invocations are
    recorded as non-persistent nodes (same as ``GraphTracker``).  On context
    exit, ``compile()`` rewires the graph into execution-ready nodes:

    - Leaf streams → ``SourceNode`` (thin wrapper for graph vertex)
    - Function pod invocations → ``PersistentFunctionNode``
    - Operator invocations → ``PersistentOperatorNode``

    Source caching is not a pipeline concern — sources that need caching
    should be wrapped in a ``CachedSource`` before being used in the
    pipeline.

    All persistent nodes share the same ``pipeline_database`` and use
    ``pipeline_name`` as path prefix, scoping their cache tables.

    Parameters
    ----------
    name:
        Pipeline name (string or tuple).  Used as the path prefix for
        all cache/pipeline paths within the databases.
    pipeline_database:
        Database for pipeline records and operator caches.
    function_database:
        Optional separate database for function pod result caches.
        When ``None``, ``pipeline_database`` is used with a ``_results``
        subfolder under the pipeline name.
    auto_compile:
        If ``True`` (default), ``compile()`` is called automatically
        when the context manager exits.
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
        """
        Replace all recorded nodes with persistent variants.

        Walks the graph in topological order and creates:

        - ``SourceNode`` for every leaf stream
        - ``PersistentFunctionNode`` for every function pod invocation
        - ``PersistentOperatorNode`` for every operator invocation

        After compile, nodes are accessible by label as attributes on the
        pipeline instance.
        """
        from orcapod.core.nodes import (
            FunctionNode,
            OperatorNode,
            PersistentFunctionNode,
            PersistentOperatorNode,
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
                if node_hash in self._node_lut:
                    label = (
                        existing_node.label
                        or existing_node.computed_label()
                        or "unnamed"
                    )
                    name_candidates.setdefault(label, []).append(existing_node)
                continue

            if node_hash not in self._node_lut:
                # -- Leaf stream: wrap in SourceNode --
                stream = self._upstreams[node_hash]
                persistent_node = SourceNode(stream=stream)
                persistent_node_map[node_hash] = persistent_node
            else:
                node = self._node_lut[node_hash]

                if isinstance(node, FunctionNode):
                    # Rewire input stream to persistent upstream
                    input_hash = node._input_stream.content_hash().to_string()
                    rewired_input = persistent_node_map[input_hash]

                    # Determine result database and path prefix
                    if self._function_database is not None:
                        result_db = self._function_database
                        result_prefix = None
                    else:
                        result_db = self._pipeline_database
                        result_prefix = self._name + ("_results",)

                    persistent_node = PersistentFunctionNode(
                        function_pod=node._function_pod,
                        input_stream=rewired_input,
                        pipeline_database=self._pipeline_database,
                        result_database=result_db,
                        result_path_prefix=result_prefix,
                        pipeline_path_prefix=self._pipeline_path_prefix,
                        label=node.label,
                    )
                    persistent_node_map[node_hash] = persistent_node

                elif isinstance(node, OperatorNode):
                    # Rewire all input streams to persistent upstreams
                    rewired_inputs = tuple(
                        persistent_node_map[s.content_hash().to_string()]
                        for s in node.upstreams
                    )

                    persistent_node = PersistentOperatorNode(
                        operator=node._operator,
                        input_streams=rewired_inputs,
                        pipeline_database=self._pipeline_database,
                        pipeline_path_prefix=self._pipeline_path_prefix,
                        label=node.label,
                    )
                    persistent_node_map[node_hash] = persistent_node

                else:
                    raise TypeError(
                        f"Unknown node type in pipeline graph: {type(node)}"
                    )

                # Track for label assignment (only non-leaf nodes)
                label = (
                    persistent_node.label
                    or persistent_node.computed_label()
                    or "unnamed"
                )
                name_candidates.setdefault(label, []).append(persistent_node)

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
                elif isinstance(node, PersistentFunctionNode):
                    attrs["node_type"] = "function"
                elif isinstance(node, PersistentOperatorNode):
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
        config: PipelineConfig | None = None,
        execution_engine: cp.PacketFunctionExecutorProtocol | None = None,
        execution_engine_opts: "dict[str, Any] | None" = None,
    ) -> None:
        """Execute all compiled nodes.

        Args:
            config: Pipeline configuration.  When ``config.executor`` is
                ``ExecutorType.ASYNC_CHANNELS``, the pipeline runs
                asynchronously via the orchestrator.  When ``config`` is
                omitted and an ``execution_engine`` is provided, async mode
                is used by default.  Passing an explicit ``config`` always
                takes priority — supply ``ExecutorType.SYNCHRONOUS`` to force
                synchronous execution even when an engine is present.
            execution_engine: Optional packet-function executor applied to
                every function node before execution (e.g. a ``RayExecutor``).
                Overrides ``config.execution_engine`` when both are provided.
            execution_engine_opts: Default resource/options dict forwarded to
                the engine for every node (e.g. ``{"num_cpus": 4}``).
                Individual nodes may override via their
                ``execution_engine_opts`` attribute.  Overrides
                ``config.execution_engine_opts`` when both are provided.
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

        # Default to async when an execution engine is provided, unless the
        # caller explicitly supplied a config — in which case config.executor
        # is authoritative and takes priority.
        use_async = config.executor == ExecutorType.ASYNC_CHANNELS or (
            effective_engine is not None and not explicit_config
        )
        if use_async:
            self._run_async(config)
        else:
            assert self._node_graph is not None
            for node in nx.topological_sort(self._node_graph):
                node.run()

        self.flush()

    def _apply_execution_engine(
        self,
        execution_engine: cp.PacketFunctionExecutorProtocol,
        execution_engine_opts: dict[str, Any] | None,
    ) -> None:
        """Apply *execution_engine* to every ``PersistentFunctionNode`` in the pipeline.

        For each function node, the pipeline-level *execution_engine_opts* are
        merged with any per-node ``execution_engine_opts`` override (node opts
        win).  If the merged opts dict is non-empty, ``engine.with_options``
        is called to produce a node-specific executor; otherwise the engine
        instance is used directly.

        Args:
            execution_engine: Executor to apply (must implement
                ``PacketFunctionExecutorBase`` or at minimum expose
                ``with_options``).
            execution_engine_opts: Pipeline-level default options dict, or
                ``None`` for no defaults.
        """
        from orcapod.core.nodes import PersistentFunctionNode

        assert self._node_graph is not None, (
            "_apply_execution_engine called before compile()"
        )

        pipeline_opts = execution_engine_opts or {}

        for node in self._node_graph.nodes:
            if not isinstance(node, PersistentFunctionNode):
                continue
            node_opts = node.execution_engine_opts or {}
            merged = {**pipeline_opts, **node_opts}
            node.executor = (
                execution_engine.with_options(**merged) if merged else execution_engine
            )
            logger.debug(
                "Applied execution engine %r to node %r (opts=%r)",
                type(execution_engine).__name__,
                node.label,
                merged or None,
            )

    def _run_async(self, config: PipelineConfig) -> None:
        """Run the pipeline asynchronously using the orchestrator."""
        from orcapod.pipeline.orchestrator import AsyncPipelineOrchestrator

        orchestrator = AsyncPipelineOrchestrator()
        orchestrator.run(self, config)

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

        from orcapod.core.nodes import (
            PersistentFunctionNode,
            PersistentOperatorNode,
        )
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
            descriptor: dict[str, Any] = {
                "node_type": node.node_type,
                "label": node.label,
                "content_hash": node.content_hash().to_string(),
                "pipeline_hash": node.pipeline_hash().to_string(),
                "data_context_key": node.data_context_key,
                "output_schema": {
                    "tag": serialize_schema(tag_schema),
                    "packet": serialize_schema(packet_schema),
                },
            }

            if isinstance(node, SourceNode):
                descriptor.update(self._build_source_descriptor(node))
            elif isinstance(node, PersistentFunctionNode):
                descriptor.update(self._build_function_descriptor(node))
            elif isinstance(node, PersistentOperatorNode):
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

        # Determine stream_type, source_config, and reconstructable
        if hasattr(stream, "to_config"):
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

    def _build_function_descriptor(
        self, node: "PersistentFunctionNode"
    ) -> dict[str, Any]:
        """Build function-specific descriptor fields for a PersistentFunctionNode.

        Args:
            node: The PersistentFunctionNode to describe.

        Returns:
            Dict with function-specific fields.
        """
        return {
            "function_pod": node._function_pod.to_config(),
            "pipeline_path": list(node.pipeline_path),
            "result_record_path": list(node._packet_function.record_path),
            "execution_engine_opts": node.execution_engine_opts,
        }

    def _build_operator_descriptor(
        self, node: "PersistentOperatorNode"
    ) -> dict[str, Any]:
        """Build operator-specific descriptor fields for a PersistentOperatorNode.

        Args:
            node: The PersistentOperatorNode to describe.

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
            path: Path to the JSON file produced by :meth:`save`.
            mode: ``"full"`` (default) or ``"read_only"``.

        Returns:
            A compiled ``Pipeline`` instance.

        Raises:
            ValueError: If the file's format version is unsupported.
        """
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.nodes import (
            PersistentFunctionNode,
            PersistentOperatorNode,
            SourceNode,
        )
        from orcapod.core.tracker import DEFAULT_TRACKER_MANAGER
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
        reconstructed: dict[
            str, SourceNode | PersistentFunctionNode | PersistentOperatorNode
        ] = {}

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
                    and upstream_node.load_status == LoadStatus.FULL
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
                        hasattr(n, "load_status") and n.load_status == LoadStatus.FULL
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

        # Populate _nodes (label -> node) for labeled non-source nodes.
        # This matches compile() behavior where freshly-created source
        # nodes are not added to the named-node dictionary.
        pipeline._nodes = {}
        for node_hash, node in reconstructed.items():
            label = node.label
            if label and node.node_type != "source":
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
    ) -> "SourceNode":
        """Reconstruct a SourceNode from a descriptor.

        Args:
            descriptor: The serialized node descriptor.
            mode: Load mode (``"full"`` or ``"read_only"``).
            resolve_source_from_config: Callable to reconstruct a source.

        Returns:
            A ``SourceNode`` instance.
        """
        from orcapod.core.nodes import SourceNode

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
    ) -> "PersistentFunctionNode":
        """Reconstruct a PersistentFunctionNode from a descriptor.

        Args:
            descriptor: The serialized node descriptor.
            mode: Load mode.
            upstream_node: The reconstructed upstream node, or ``None``.
            upstream_usable: Whether the upstream is in FULL mode.
            databases: Database role mapping.

        Returns:
            A ``PersistentFunctionNode`` instance.
        """
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.nodes import PersistentFunctionNode

        if mode == "full" and upstream_usable:
            try:
                pod = FunctionPod.from_config(descriptor["function_pod"])
                return PersistentFunctionNode.from_descriptor(
                    descriptor,
                    function_pod=pod,
                    input_stream=upstream_node,
                    databases=databases,
                )
            except Exception:
                logger.warning(
                    "Failed to reconstruct function node %r, falling back to read-only.",
                    descriptor.get("label"),
                )

        return PersistentFunctionNode.from_descriptor(
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
    ) -> "PersistentOperatorNode":
        """Reconstruct a PersistentOperatorNode from a descriptor.

        Args:
            descriptor: The serialized node descriptor.
            mode: Load mode.
            upstream_nodes: Tuple of reconstructed upstream nodes.
            all_upstreams_usable: Whether all upstreams are in FULL mode.
            databases: Database role mapping.
            resolve_operator_from_config: Callable to reconstruct an operator.

        Returns:
            A ``PersistentOperatorNode`` instance.
        """
        from orcapod.core.nodes import PersistentOperatorNode

        if all_upstreams_usable and mode != "read_only":
            try:
                op = resolve_operator_from_config(descriptor["operator"])
                return PersistentOperatorNode.from_descriptor(
                    descriptor,
                    operator=op,
                    input_streams=upstream_nodes,
                    databases=databases,
                )
            except Exception:
                logger.warning(
                    "Failed to reconstruct operator node %r, falling back to read-only.",
                    descriptor.get("label"),
                )

        return PersistentOperatorNode.from_descriptor(
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
