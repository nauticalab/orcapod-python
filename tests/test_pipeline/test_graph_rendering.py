"""Tests for pipeline graph rendering utilities.

Covers GraphRenderer, render_graph, render_graph_dark_theme,
StyleRuleSets, and Pipeline.show_graph.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import networkx as nx
import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, GraphNode, OperatorNode, SourceNode
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.graph import (
    GraphRenderer,
    StyleRuleSets,
    render_graph,
    render_graph_dark_theme,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(tag_col: str, packet_col: str, data: dict) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            packet_col: pa.array(data[packet_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _make_two_sources() -> tuple[ArrowTableSource, ArrowTableSource]:
    src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
    src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
    return src_a, src_b


def _add_values(value: int, score: int) -> int:
    return value + score


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline_db() -> InMemoryArrowDatabase:
    return InMemoryArrowDatabase()


@pytest.fixture
def compiled_pipeline(pipeline_db: InMemoryArrowDatabase) -> Pipeline:
    """A compiled pipeline with source, operator, and function nodes."""
    src_a, src_b = _make_two_sources()
    pf = PythonPacketFunction(_add_values, output_keys="total")
    pod = FunctionPod(packet_function=pf)

    pipeline = Pipeline(name="test_render", pipeline_database=pipeline_db)
    with pipeline:
        joined = Join()(src_a, src_b)
        pod(joined, label="adder")

    return pipeline


@pytest.fixture
def node_graph(compiled_pipeline: Pipeline) -> nx.DiGraph:
    assert compiled_pipeline._node_graph is not None
    return compiled_pipeline._node_graph


# ---------------------------------------------------------------------------
# Tests: GraphRenderer
# ---------------------------------------------------------------------------


class TestGraphRenderer:
    def test_generate_dot_returns_dot_source(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        dot_text = renderer.generate_dot(node_graph)

        assert isinstance(dot_text, str)
        assert "digraph" in dot_text

    def test_generate_dot_includes_all_nodes(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        dot_text = renderer.generate_dot(node_graph)

        for node in node_graph.nodes():
            sanitized = renderer._sanitize_node_id(node)
            assert sanitized in dot_text

    def test_generate_dot_includes_edges(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        dot_text = renderer.generate_dot(node_graph)

        for source, target in node_graph.edges():
            source_id = renderer._sanitize_node_id(source)
            target_id = renderer._sanitize_node_id(target)
            assert source_id in dot_text
            assert target_id in dot_text

    def test_generate_dot_with_label_lut(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        first_node = next(iter(node_graph.nodes()))
        label_lut = {first_node: "Custom Label"}
        dot_text = renderer.generate_dot(node_graph, label_lut=label_lut)

        assert "Custom Label" in dot_text

    def test_generate_dot_with_custom_style_rules(
        self, node_graph: nx.DiGraph
    ) -> None:
        renderer = GraphRenderer()
        custom_rules = {
            "source": {
                "fillcolor": "red",
                "shape": "ellipse",
                "fontcolor": "blue",
                "style": "filled",
            },
        }
        dot_text = renderer.generate_dot(node_graph, style_rules=custom_rules)

        assert "red" in dot_text
        assert "ellipse" in dot_text

    def test_render_graph_raw_output(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        dot_text = renderer.render_graph(node_graph, raw_output=True, show=False)

        assert isinstance(dot_text, str)
        assert "digraph" in dot_text

    def test_render_graph_with_dark_theme(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        dot_text = renderer.render_graph(
            node_graph,
            raw_output=True,
            show=False,
            style_rules=renderer.DARK_THEME_RULES,
        )

        assert isinstance(dot_text, str)
        assert "digraph" in dot_text


# ---------------------------------------------------------------------------
# Tests: node attribute resolution
# ---------------------------------------------------------------------------


class TestNodeAttributes:
    def test_source_node_gets_source_style(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        source_nodes = [n for n in node_graph.nodes() if isinstance(n, SourceNode)]
        assert len(source_nodes) > 0

        attrs = renderer._get_node_attributes(source_nodes[0])
        expected = GraphRenderer.DEFAULT_STYLE_RULES["source"]
        assert attrs["fillcolor"] == expected["fillcolor"]
        assert attrs["shape"] == expected["shape"]

    def test_function_node_gets_function_style(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        fn_nodes = [n for n in node_graph.nodes() if isinstance(n, FunctionNode)]
        assert len(fn_nodes) > 0

        attrs = renderer._get_node_attributes(fn_nodes[0])
        expected = GraphRenderer.DEFAULT_STYLE_RULES["function"]
        assert attrs["fillcolor"] == expected["fillcolor"]
        assert attrs["shape"] == expected["shape"]

    def test_operator_node_gets_operator_style(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        op_nodes = [n for n in node_graph.nodes() if isinstance(n, OperatorNode)]
        assert len(op_nodes) > 0

        attrs = renderer._get_node_attributes(op_nodes[0])
        expected = GraphRenderer.DEFAULT_STYLE_RULES["operator"]
        assert attrs["fillcolor"] == expected["fillcolor"]
        assert attrs["shape"] == expected["shape"]

    def test_custom_rules_override_defaults(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        source_node = next(
            n for n in node_graph.nodes() if isinstance(n, SourceNode)
        )
        custom = {"source": {"fillcolor": "purple", "shape": "hexagon"}}
        attrs = renderer._get_node_attributes(source_node, style_rules=custom)

        assert attrs["fillcolor"] == "purple"
        assert attrs["shape"] == "hexagon"


# ---------------------------------------------------------------------------
# Tests: HTML label generation
# ---------------------------------------------------------------------------


class TestHtmlLabel:
    def test_label_contains_node_type_and_label(
        self, node_graph: nx.DiGraph
    ) -> None:
        renderer = GraphRenderer()
        for node in node_graph.nodes():
            attrs = renderer._get_node_attributes(node)
            html = renderer._create_default_html_label(node, attrs)

            assert node.node_type in html
            assert str(node.label) in html

    def test_label_lut_overrides_default(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        node = next(iter(node_graph.nodes()))
        label_lut = {node: "overridden"}

        assert renderer._get_node_label(node, label_lut) == "overridden"

    def test_default_label_uses_node_label(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        node = next(iter(node_graph.nodes()))

        assert renderer._get_node_label(node) == str(node.label)


# ---------------------------------------------------------------------------
# Tests: convenience functions
# ---------------------------------------------------------------------------


class TestConvenienceFunctions:
    def test_render_graph_function(self, node_graph: nx.DiGraph) -> None:
        dot_text = render_graph(node_graph, raw_output=True, show=False)

        assert isinstance(dot_text, str)
        assert "digraph" in dot_text

    def test_render_graph_dark_theme_function(self, node_graph: nx.DiGraph) -> None:
        dot_text = render_graph_dark_theme(
            node_graph, raw_output=True, show=False
        )

        assert isinstance(dot_text, str)
        assert "digraph" in dot_text


# ---------------------------------------------------------------------------
# Tests: StyleRuleSets
# ---------------------------------------------------------------------------


class TestStyleRuleSets:
    def test_default_rules_match_renderer(self) -> None:
        assert StyleRuleSets.get_default_rules() == GraphRenderer.DEFAULT_STYLE_RULES

    def test_dark_rules_match_renderer(self) -> None:
        assert StyleRuleSets.get_dark_rules() == GraphRenderer.DARK_THEME_RULES

    def test_create_custom_rules_returns_all_node_types(self) -> None:
        rules = StyleRuleSets.create_custom_rules()
        assert "source" in rules
        assert "operator" in rules
        assert "function" in rules

    def test_create_custom_rules_applies_overrides(self) -> None:
        rules = StyleRuleSets.create_custom_rules(
            source_bg="cyan",
            operator_bg="magenta",
            pod_bg="yellow",
        )
        assert rules["source"]["fillcolor"] == "cyan"
        assert rules["operator"]["fillcolor"] == "magenta"
        assert rules["function"]["fillcolor"] == "yellow"


# ---------------------------------------------------------------------------
# Tests: Pipeline.show_graph
# ---------------------------------------------------------------------------


class TestPipelineShowGraph:
    def test_show_graph_raises_before_compile(self, pipeline_db) -> None:
        pipeline = Pipeline(name="uncompiled", pipeline_database=pipeline_db)

        with pytest.raises(RuntimeError, match="compiled"):
            pipeline.show_graph(raw_output=True, show=False)

    def test_show_graph_returns_dot(
        self, compiled_pipeline: Pipeline
    ) -> None:
        dot_text = compiled_pipeline.show_graph(raw_output=True, show=False)

        assert isinstance(dot_text, str)
        assert "digraph" in dot_text

    def test_show_graph_with_label_lut(
        self, compiled_pipeline: Pipeline
    ) -> None:
        assert compiled_pipeline._node_graph is not None
        first_node = next(iter(compiled_pipeline._node_graph.nodes()))
        label_lut = {first_node: "MyCustomLabel"}

        dot_text = compiled_pipeline.show_graph(
            label_lut=label_lut, raw_output=True, show=False
        )

        assert "MyCustomLabel" in dot_text

    def test_show_graph_with_style_rules(
        self, compiled_pipeline: Pipeline
    ) -> None:
        custom = StyleRuleSets.create_custom_rules(source_bg="lime")
        dot_text = compiled_pipeline.show_graph(
            style_rules=custom, raw_output=True, show=False
        )

        assert "lime" in dot_text


# ---------------------------------------------------------------------------
# Tests: style merging
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests: full rendering paths (non-raw_output)
# ---------------------------------------------------------------------------


class TestRenderGraphFullPath:
    """Exercise the graphviz rendering code paths (not just raw DOT output)."""

    def test_render_no_show_no_output(self, node_graph: nx.DiGraph) -> None:
        """render_graph with show=False exercises graphviz build without display."""
        renderer = GraphRenderer()
        result = renderer.render_graph(node_graph, show=False)

        assert isinstance(result, str)
        assert "digraph" in result

    def test_render_with_output_path(
        self, node_graph: nx.DiGraph, tmp_path
    ) -> None:
        output_file = str(tmp_path / "graph.png")
        renderer = GraphRenderer()
        result = renderer.render_graph(
            node_graph, show=False, output_path=output_file
        )

        assert isinstance(result, str)
        assert os.path.exists(output_file)

    def test_render_with_show_mocked(self, node_graph: nx.DiGraph) -> None:
        """Exercise the show=True path with matplotlib mocked."""
        renderer = GraphRenderer()
        with (
            patch("matplotlib.pyplot.figure"),
            patch("matplotlib.pyplot.imshow"),
            patch("matplotlib.pyplot.axis"),
            patch("matplotlib.pyplot.tight_layout"),
            patch("matplotlib.pyplot.show"),
        ):
            result = renderer.render_graph(node_graph, show=True)

        assert isinstance(result, str)
        assert "digraph" in result

    def test_render_with_label_lut_full_path(
        self, node_graph: nx.DiGraph
    ) -> None:
        first_node = next(iter(node_graph.nodes()))
        label_lut = {first_node: "FullPathLabel"}
        renderer = GraphRenderer()
        result = renderer.render_graph(
            node_graph, label_lut=label_lut, show=False
        )

        assert isinstance(result, str)

    def test_render_with_style_rules_full_path(
        self, node_graph: nx.DiGraph
    ) -> None:
        custom = StyleRuleSets.create_custom_rules(source_bg="coral")
        renderer = GraphRenderer()
        result = renderer.render_graph(
            node_graph, style_rules=custom, show=False
        )

        assert isinstance(result, str)

    def test_render_with_style_overrides(self, node_graph: nx.DiGraph) -> None:
        renderer = GraphRenderer()
        result = renderer.render_graph(
            node_graph, show=False, dpi=72, rankdir="LR"
        )

        assert isinstance(result, str)


class TestConvenienceFunctionsFullPath:
    def test_render_graph_no_show(self, node_graph: nx.DiGraph) -> None:
        result = render_graph(node_graph, show=False)

        assert isinstance(result, str)
        assert "digraph" in result

    def test_render_graph_dark_theme_no_show(
        self, node_graph: nx.DiGraph
    ) -> None:
        result = render_graph_dark_theme(node_graph, show=False)

        assert isinstance(result, str)
        assert "digraph" in result


class TestPipelineShowGraphFullPath:
    def test_show_graph_no_show(self, compiled_pipeline: Pipeline) -> None:
        result = compiled_pipeline.show_graph(show=False)

        assert isinstance(result, str)
        assert "digraph" in result

    def test_show_graph_with_show_mocked(
        self, compiled_pipeline: Pipeline
    ) -> None:
        with (
            patch("matplotlib.pyplot.figure"),
            patch("matplotlib.pyplot.imshow"),
            patch("matplotlib.pyplot.axis"),
            patch("matplotlib.pyplot.tight_layout"),
            patch("matplotlib.pyplot.show"),
        ):
            result = compiled_pipeline.show_graph(show=True)

        assert isinstance(result, str)
        assert "digraph" in result


# ---------------------------------------------------------------------------
# Tests: style merging
# ---------------------------------------------------------------------------


class TestStyleMerging:
    def test_merge_styles_preserves_defaults(self) -> None:
        renderer = GraphRenderer()
        merged = renderer._merge_styles()

        assert merged == GraphRenderer.DEFAULT_STYLES

    def test_merge_styles_overrides(self) -> None:
        renderer = GraphRenderer()
        merged = renderer._merge_styles(dpi=300, rankdir="LR")

        assert merged["dpi"] == 300
        assert merged["rankdir"] == "LR"
        # Other defaults preserved
        assert merged["edge_color"] == GraphRenderer.DEFAULT_STYLES["edge_color"]
