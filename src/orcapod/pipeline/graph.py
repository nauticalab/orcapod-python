from __future__ import annotations

import logging
import os
import tempfile
from typing import TYPE_CHECKING, Any

from orcapod.core.tracker import GraphTracker, GraphNode as GraphNodeType
from orcapod.pipeline.nodes import PersistentSourceNode
from orcapod.protocols import core_protocols as cp
from orcapod.protocols import database_protocols as dbp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Visualization helper (unrelated to pipeline node types)
# ---------------------------------------------------------------------------


class GraphNode:
    def __init__(self, label: str, id: int, kernel_type: str):
        self.label = label
        self.id = id
        self.kernel_type = kernel_type

    def __hash__(self):
        return hash((self.id, self.kernel_type))

    def __eq__(self, other):
        if not isinstance(other, GraphNode):
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
    exit, ``compile()`` replaces every node with its persistent variant:

    - Leaf streams → ``PersistentSourceNode``
    - Function pod invocations → ``PersistentFunctionNode``
    - Operator invocations → ``PersistentOperatorNode``

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
        self._nodes: dict[str, GraphNodeType] = {}
        self._persistent_node_map: dict[str, GraphNodeType] = {}
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
    def compiled_nodes(self) -> dict[str, GraphNodeType]:
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

        - ``PersistentSourceNode`` for every leaf stream
        - ``PersistentFunctionNode`` for every function pod invocation
        - ``PersistentOperatorNode`` for every operator invocation

        After compile, nodes are accessible by label as attributes on the
        pipeline instance.
        """
        from orcapod.core.function_pod import FunctionNode, PersistentFunctionNode
        from orcapod.core.operator_node import OperatorNode, PersistentOperatorNode

        G = nx.DiGraph()
        for edge in self._graph_edges:
            G.add_edge(*edge)

        # Seed from existing persistent nodes (incremental compile)
        persistent_node_map: dict[str, GraphNodeType] = dict(self._persistent_node_map)
        name_candidates: dict[str, list[GraphNodeType]] = {}

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
                # -- Leaf stream: wrap in PersistentSourceNode --
                stream = self._upstreams[node_hash]
                persistent_node = PersistentSourceNode(
                    stream=stream,
                    cache_database=self._pipeline_database,
                    cache_path_prefix=self._pipeline_path_prefix,
                )
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

    def run(self) -> None:
        """Execute all compiled nodes in topological order."""
        if not self._compiled:
            self.compile()
        assert self._node_graph is not None
        for node in nx.topological_sort(self._node_graph):
            node.run()
        self.flush()

    def flush(self) -> None:
        """Flush all databases."""
        self._pipeline_database.flush()
        if self._function_database is not None:
            self._function_database.flush()

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
