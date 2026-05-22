from __future__ import annotations

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
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
    from orcapod.pipeline.execution_context import ExecutionContext
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline(AutoRegisteringContextBasedTracker):
    """A pure computational blueprint recording operator and function pod invocations.

    During the ``with`` block, operator and function pod invocations are
    recorded into an internal graph. On context exit, ``compile()`` rewires
    the graph into a frozen DAG:

    - Leaf ``SourceSpec`` declarations → ``SourceNode`` (schema-only placeholders)
    - Function pod invocations → ``FunctionNode``
    - Operator invocations → ``OperatorNode``

    All leaf inputs must be ``SourceSpec`` instances. To run a ``Pipeline``,
    use ``Pipeline.bind(sources=..., store=...)`` to create a ``PipelineJob``.

    Parameters:
        name: Pipeline name (string or tuple). Used as the path prefix for
            all cache/pipeline paths when the pipeline is run via a
            ``PipelineJob``.
        auto_compile: If ``True`` (default), ``compile()`` is called
            automatically when the context manager exits.
    """

    def __init__(
        self,
        name: str | tuple[str, ...],
        tracker_manager: cp.TrackerManagerProtocol | None = None,
        auto_compile: bool = True,
    ) -> None:
        """Initialize a pure computational blueprint pipeline.

        Args:
            name: Pipeline name (string or tuple). Used to scope database paths
                when the pipeline is run via a ``PipelineJob``.
            tracker_manager: Optional tracker manager override. Defaults to
                ``DEFAULT_TRACKER_MANAGER``.
            auto_compile: If ``True`` (default), ``compile()`` is called
                automatically when the context manager exits.
        """
        super().__init__(tracker_manager=tracker_manager)
        self._node_lut: dict[str, GraphNode] = {}
        self._upstreams: dict[str, cp.StreamProtocol] = {}
        self._graph_edges: list[tuple[str, str]] = []
        self._hash_graph: "nx.DiGraph" = nx.DiGraph()
        self._name = (name,) if isinstance(name, str) else tuple(name)
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
        """Compile recorded invocations into a frozen DAG.

        Walks the graph in topological order and:

        - Verifies leaf streams are ``SourceSpec`` instances (raises ``ValueError`` otherwise)
        - Wraps ``SourceSpec`` leaves in ``SourceNode``
        - Rewires upstream references on recorded ``FunctionNode`` /
          ``OperatorNode`` to point at persistent (compiled) nodes

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
                # -- Leaf stream: must be a SourceNode in the new design --
                from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
                stream = self._upstreams[node_hash]
                if not isinstance(stream, SourceNodeClass):
                    raise ValueError(
                        f"Pipeline: all leaf inputs must be SourceNode instances, "
                        f"but found {type(stream).__name__!r}. "
                        "Use 'with PipelineJob:' to record a pipeline with concrete sources, "
                        "or replace concrete sources with SourceNode declarations."
                    )
                node = stream  # SourceNode IS the leaf — no wrapping needed
                persistent_node_map[node_hash] = node
            else:
                node = self._node_lut[node_hash]

                if isinstance(node, FunctionNode):
                    # Rewire input stream to persistent upstream
                    input_hash = node._input_stream.content_hash().to_string()
                    rewired_input = persistent_node_map[input_hash]
                    node.upstreams = (rewired_input,)

                elif isinstance(node, OperatorNode):
                    # Rewire all input streams to persistent upstreams
                    rewired_inputs = tuple(
                        persistent_node_map[s.content_hash().to_string()]
                        for s in node.upstreams
                    )
                    node.upstreams = rewired_inputs

                else:
                    raise TypeError(
                        f"Unknown node type in pipeline graph: {type(node)}"
                    )

                persistent_node_map[node_hash] = node

            # Track all nodes for label assignment
            name_candidates.setdefault(node.label, []).append(node)

        # Save persistent node map for incremental re-compile
        self._persistent_node_map = persistent_node_map

        # Build node graph for show_graph() and PipelineJob execution
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
    # Graph display
    # ------------------------------------------------------------------

    def show_graph(self, **kwargs) -> str | None:
        """Render the pipeline's node graph.

        Args:
            **kwargs: Forwarded to ``render_graph``.

        Raises:
            RuntimeError: If the pipeline has not been compiled yet.
        """
        if self._node_graph is None:
            raise RuntimeError("Pipeline must be compiled before showing the graph.")
        return render_graph(self._node_graph, **kwargs)

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def save(self, path: str | Path) -> None:
        """Serialize the pure pipeline blueprint to a JSON file.

        Saves topology and SourceSpec declarations only — no databases,
        no execution context, no run metadata.

        Args:
            path: File path to write JSON output to.

        Raises:
            ValueError: If the pipeline has not been compiled.
        """
        if not self._compiled:
            raise ValueError(
                "Pipeline is not compiled. Call compile() or use "
                "auto_compile=True before saving."
            )

        import json as _json
        from orcapod.pipeline.serialization import (
            PIPELINE_FORMAT_VERSION,
            serialize_schema,
        )
        from orcapod.core.nodes import OperatorNode, FunctionNode
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass

        nodes: dict[str, Any] = {}
        for content_hash_str, node in self._persistent_node_map.items():
            tag_schema, data_schema = node.output_schema()
            try:
                type_converter = node.data_context.type_converter
            except (AttributeError, TypeError):
                from orcapod.contexts import resolve_context
                type_converter = resolve_context(None).type_converter

            try:
                data_context_key = node.data_context_key
            except (AttributeError, TypeError):
                # Stub nodes (loaded with no live operator/function_pod) store
                # the data context directly on _data_context; fall back to it.
                _dc = getattr(node, "_data_context", None)
                data_context_key = _dc.context_key if _dc is not None else None

            descriptor: dict[str, Any] = {
                "node_type": node.node_type,
                "label": node.label,
                "content_hash": node.content_hash().to_string(),
                "pipeline_hash": node.pipeline_hash().to_string(),
                "output_schema": {
                    "tag": serialize_schema(tag_schema, type_converter),
                    "data": serialize_schema(data_schema, type_converter),
                },
                "node_uri": list(node.node_uri),
                "data_context_key": data_context_key,
            }

            if isinstance(node, SourceNodeClass):
                descriptor["source_config"] = {
                    "source_type": "node",
                    "node_name": node.name,
                }
                descriptor["reconstructable"] = True

            elif isinstance(node, FunctionNode):
                if node._function_pod is not None:
                    descriptor["function_config"] = node._function_pod.to_config()
                descriptor["table_scope"] = node._table_scope

            elif isinstance(node, OperatorNode):
                if node._operator is not None:
                    descriptor["operator_config"] = node._operator.to_config()
                descriptor["table_scope"] = node._table_scope

            nodes[content_hash_str] = descriptor

        output: dict[str, Any] = {
            "orcapod_pipeline_version": PIPELINE_FORMAT_VERSION,
            "pipeline": {"name": list(self._name)},
            "nodes": nodes,
            "edges": [list(edge) for edge in self._graph_edges],
        }

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            _json.dump(output, f, indent=2)

    @classmethod
    def load(cls, path: str | Path) -> "Pipeline":
        """Deserialize a pure pipeline blueprint from a JSON file.

        Reconstructs topology and SourceSpec declarations. The loaded
        pipeline is topology-only — to run it, call
        ``pipeline.bind(sources=..., store=...)`` first.

        Args:
            path: Path to the JSON file produced by :meth:`save`.

        Returns:
            A compiled ``Pipeline`` instance with SourceSpec leaf nodes.

        Raises:
            ValueError: If the file's format version is unsupported.
        """
        import json as _json
        from orcapod.pipeline.serialization import (
            SUPPORTED_FORMAT_VERSIONS,
            deserialize_schema,
        )
        from orcapod.core.nodes import FunctionNode, OperatorNode
        from orcapod.core.nodes.source_node import SourceNode as SourceNodeClass
        from orcapod.types import Schema

        path = Path(path)
        with open(path) as f:
            data = _json.load(f)

        version = data.get("orcapod_pipeline_version", "")
        if version not in SUPPORTED_FORMAT_VERSIONS:
            raise ValueError(
                f"Unsupported pipeline format version {version!r}. "
                f"Supported: {sorted(SUPPORTED_FORMAT_VERSIONS)}"
            )

        pipeline_meta = data["pipeline"]
        name = tuple(pipeline_meta["name"])
        nodes_data = data["nodes"]
        edges = data["edges"]

        # Build topological order
        edge_graph: "nx.DiGraph" = nx.DiGraph()
        for up_hash, down_hash in edges:
            edge_graph.add_edge(up_hash, down_hash)
        for node_hash in nodes_data:
            if node_hash not in edge_graph:
                edge_graph.add_node(node_hash)
        topo_order = list(nx.topological_sort(edge_graph))

        upstream_map: dict[str, list[str]] = {}
        for up_hash, down_hash in edges:
            upstream_map.setdefault(down_hash, []).append(up_hash)

        reconstructed: dict[str, GraphNode] = {}

        for node_hash in topo_order:
            descriptor = nodes_data.get(node_hash)
            if descriptor is None:
                continue

            node_type = descriptor.get("node_type")
            source_config = descriptor.get("source_config") or {}

            if node_type == "source":
                tag_schema = Schema(deserialize_schema(descriptor["output_schema"]["tag"]))
                data_schema = Schema(deserialize_schema(descriptor["output_schema"]["data"]))
                # Support both old "spec" format and new "node" format
                if source_config.get("source_type") == "node":
                    node_name = source_config["node_name"]
                elif source_config.get("source_type") == "spec":
                    # Legacy format compatibility: spec_name becomes node name
                    node_name = source_config["spec_name"]
                else:
                    # Fall back to stored label
                    node_name = descriptor.get("label") or "unknown"
                node = SourceNodeClass(
                    name=node_name,
                    tag_schema=tag_schema,
                    data_schema=data_schema,
                )
                # Restore label from descriptor if set explicitly
                stored_label = descriptor.get("label")
                if stored_label and stored_label != node_name:
                    node._label = stored_label
                reconstructed[node_hash] = node

            elif node_type == "function":
                up_hashes = upstream_map.get(node_hash, [])
                upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None
                node = FunctionNode.from_descriptor(
                    descriptor, function_pod=None, input_stream=upstream_node, databases={}
                )
                reconstructed[node_hash] = node

            elif node_type == "operator":
                up_hashes = upstream_map.get(node_hash, [])
                upstream_nodes = tuple(
                    reconstructed[h] for h in up_hashes if h in reconstructed
                )
                # Attempt to reconstruct the operator from its saved config via
                # the centralised resolver so registry logic and error handling
                # are consistent with all other deserialization paths.
                operator = None
                op_config = descriptor.get("operator_config")
                if op_config:
                    try:
                        from orcapod.pipeline.serialization import resolve_operator_from_config
                        operator = resolve_operator_from_config(op_config)
                    except Exception as exc:
                        logger.warning(
                            "Could not reconstruct operator %r from config — "
                            "node will be in read-only mode: %s",
                            op_config.get("class_name"),
                            exc,
                        )
                node = OperatorJobNode.from_descriptor(
                    descriptor, operator=operator, input_streams=upstream_nodes, databases={}
                )
                reconstructed[node_hash] = node

        # Build Pipeline instance
        pipeline = cls(name=name, auto_compile=False)
        pipeline._persistent_node_map = dict(reconstructed)

        nodes_by_label: dict[str, GraphNode] = {}
        for node in reconstructed.values():
            if node.label:
                if node.label in nodes_by_label:
                    logger.warning(
                        "Label collision in loaded pipeline: %r. "
                        "The first node with this label wins.",
                        node.label,
                    )
                else:
                    nodes_by_label[node.label] = node
        pipeline._nodes = nodes_by_label

        pipeline._node_graph = nx.DiGraph()
        for up_hash, down_hash in edges:
            up_node = reconstructed.get(up_hash)
            down_node = reconstructed.get(down_hash)
            if up_node is not None and down_node is not None:
                pipeline._node_graph.add_edge(up_node, down_node)
        for node in reconstructed.values():
            if node not in pipeline._node_graph:
                pipeline._node_graph.add_node(node)

        pipeline._graph_edges = [(up, down) for up, down in edges]
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

        # Restore _node_lut and _upstreams so PipelineJob._build_execution_graph()
        # can substitute bound sources and build a correct execution graph.
        pipeline._node_lut = {
            h: n
            for h, n in reconstructed.items()
            if not isinstance(n, SourceNodeClass)
        }
        # SourceNode IS the upstream — store it directly so _build_execution_graph()
        # can find it by hash and substitute a concrete source at run time.
        pipeline._upstreams = {
            h: n
            for h, n in reconstructed.items()
            if isinstance(n, SourceNodeClass)
        }

        pipeline._compiled = True
        return pipeline

    def _clone_for_execution(self) -> "Pipeline":
        """Create a lightweight copy of this compiled pipeline for isolated execution.

        All structural state (``_node_lut``, ``_upstreams``, ``_graph_edges``,
        ``_hash_graph``, ``_node_graph``, ``_persistent_node_map``) is shared
        read-only with the original — these are immutable after ``compile()``.
        Only ``_nodes`` (the label → exec-node mapping) gets its own copy so
        that ``_build_execution_graph()`` can update it without affecting other
        ``PipelineJob`` instances that reference this blueprint.

        The clone is never registered as a tracker context manager.

        Returns:
            A new ``Pipeline`` instance sharing read-only state with ``self``.
        """
        clone = Pipeline.__new__(Pipeline)
        # Base class state — clone is inactive and never registered
        clone._tracker_manager = self._tracker_manager
        clone._active = False
        # Shared read-only structural state
        clone._name = self._name
        clone._node_lut = self._node_lut
        clone._upstreams = self._upstreams
        clone._graph_edges = self._graph_edges
        clone._hash_graph = self._hash_graph
        clone._persistent_node_map = self._persistent_node_map
        clone._node_graph = self._node_graph
        clone._auto_compile = self._auto_compile
        clone._compiled = self._compiled
        # Mutable per-execution state — own copy so runs don't interfere
        clone._nodes = dict(self._nodes)
        return clone

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
        "function": {
            "fillcolor": "#090271",  # darker navy blue
            "shape": "cylinder",
            "fontcolor": "white",
            "style": "filled",
            "typefontcolor": "lightgray",  # Light text for dark background
        },
    }

    def __init__(self):
        pass

    def _sanitize_node_id(self, node_id: GraphNode) -> str:
        return f"node_{hash(node_id)}"

    def _create_default_html_label(
        self, node: GraphNode, node_attrs: dict[str, str]
    ) -> str:
        """Create HTML for the label (text) section of the node.

        Format:
        node_type      (11pt, small text)
        main_label     (14pt, normal text)
        """
        main_label = str(node.label)
        node_type = node.node_type

        # Create HTML label: small node_type above, main label below
        main_size = self.DEFAULT_STYLES["main_font_size"]
        type_size = self.DEFAULT_STYLES["type_font_size"]
        font_name = self.DEFAULT_STYLES["font_name"]
        type_font_color = node_attrs.get(
            "typefontcolor", self.DEFAULT_STYLES["type_font_color"]
        )

        html_label = f'''<
        <TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0">
            <TR><TD ALIGN="CENTER"><FONT POINT-SIZE="{type_size}" COLOR="{type_font_color}" FACE="{font_name}, bold">{node_type}</FONT></TD></TR>
            <TR><TD ALIGN="CENTER"><FONT POINT-SIZE="{main_size}">{main_label}</FONT></TD></TR>
        </TABLE>
        >'''

        return html_label

    def _get_node_label(
        self,
        node: GraphNode,
        label_lut: dict[GraphNode, str] | None = None,
    ) -> str:
        if label_lut and node in label_lut:
            return label_lut[node]
        return str(node.label)

    def _get_node_attributes(
        self,
        node: GraphNode,
        style_rules: dict[str, dict[str, str]] | None = None,
    ) -> dict[str, str]:
        """Get styling attributes for a specific node based on its node_type."""
        rules = style_rules or self.DEFAULT_STYLE_RULES

        default_attrs = {
            "fillcolor": self.DEFAULT_STYLES["node_color"],
            "shape": self.DEFAULT_STYLES["node_shape"],
            "fontcolor": self.DEFAULT_STYLES["font_color"],
            "fontname": self.DEFAULT_STYLES["font_name"],
            "fontsize": self.DEFAULT_STYLES.get("fontsize", "14"),
            "style": self.DEFAULT_STYLES["node_style"],
            "typefontcolor": self.DEFAULT_STYLES["type_font_color"],
        }

        if node.node_type in rules:
            default_attrs.update(rules[node.node_type])

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
        label_lut: dict[GraphNode, str] | None = None,
        style_rules: dict[str, dict[str, str]] | None = None,
        **style_overrides,
    ) -> str:
        styles = self._merge_styles(**style_overrides)

        import graphviz

        dot = graphviz.Digraph(comment="NetworkX Graph")

        # Apply global styles
        dot.attr(rankdir=styles["rankdir"], dpi=str(styles["dpi"]))
        dot.attr(fontname=styles["font_name"])
        if styles.get("fontsize"):
            dot.attr(fontsize=str(styles["fontsize"]))
        if styles["font_path"]:
            dot.attr(fontpath=styles["font_path"])

        # Set default edge attributes
        dot.attr("edge", color=styles["edge_color"])

        # Add nodes with styling based on node_type
        for node in graph.nodes():
            sanitized_id = self._sanitize_node_id(node)
            node_attrs = self._get_node_attributes(node, style_rules)

            if label_lut and node in label_lut:
                label = label_lut[node]
            else:
                label = self._create_default_html_label(node, node_attrs)

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
        label_lut: dict[GraphNode, str] | None = None,
        show: bool = True,
        output_path: str | None = None,
        raw_output: bool = False,
        figsize: tuple = (12, 8),
        dpi: int = 150,
        style_rules: dict[str, dict[str, str]] | None = None,
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
        for node in graph.nodes():
            sanitized_id = self._sanitize_node_id(node)
            node_attrs = self._get_node_attributes(node, style_rules)

            if label_lut and node in label_lut:
                label = label_lut[node]
            else:
                label = self._create_default_html_label(node, node_attrs)

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
    label_lut: dict[GraphNode, str] | None = None,
    style_rules: dict[str, dict[str, str]] | None = None,
    **kwargs,
) -> str | None:
    """Convenience function with conditional node styling.

    Args:
        graph: NetworkX DiGraph whose nodes are GraphNode instances.
        label_lut: Optional mapping from node to custom display label.
        style_rules: Mapping from node_type to graphviz attribute overrides.
        **kwargs: Other styling arguments forwarded to GraphRenderer.
    """
    renderer = GraphRenderer()
    return renderer.render_graph(graph, label_lut, style_rules=style_rules, **kwargs)


def render_graph_dark_theme(
    graph: "nx.DiGraph",
    label_lut: dict[GraphNode, str] | None = None,
    **kwargs,
) -> str | None:
    """Render with dark theme — dark backgrounds, light fonts."""
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
        node_type_fcolor="lightgray",
    ) -> dict[str, dict[str, str]]:
        """Create custom theme rules."""
        return {
            "source": {
                "fillcolor": source_bg,
                "shape": "ellipse",
                "fontcolor": source_main_fcolor,
                "style": "filled",
                "typefontcolor": source_type_fcolor,
            },
            "operator": {
                "fillcolor": operator_bg,
                "shape": "diamond",
                "fontcolor": operator_main_fcolor,
                "style": "filled",
                "typefontcolor": operator_type_fcolor,
            },
            "function": {
                "fillcolor": pod_bg,
                "shape": "box",
                "fontcolor": pod_main_fcolor,
                "style": "filled,rounded",
                "typefontcolor": node_type_fcolor,
            },
        }
