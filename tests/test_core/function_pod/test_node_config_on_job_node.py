import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import NodeConfig


def _make_node() -> FunctionJobNode:
    def double(x: int) -> int:
        return x * 2

    pf = PythonDataFunction(double, output_keys="result")
    pod = FunctionPod(pf)
    table = pa.table({"id": [1], "x": [10]})
    source = ArrowTableSource(table, tag_columns=["id"], infer_nullable=True)
    db = InMemoryArrowDatabase()
    return FunctionJobNode(pod, source, pipeline_database=db)


class TestFunctionJobNodeNodeConfig:
    def test_default_node_config(self):
        """FunctionJobNode initialises with NodeConfig() as default."""
        node = _make_node()
        assert isinstance(node.node_config, NodeConfig)
        assert node.node_config.is_result_ephemeral is None

    def test_node_config_setter(self):
        """node_config property setter replaces the config."""
        node = _make_node()
        new_config = NodeConfig(is_result_ephemeral=True)
        node.node_config = new_config
        assert node.node_config is new_config

    def test_set_then_replace(self):
        """Setting node_config twice replaces it cleanly."""
        node = _make_node()
        node.node_config = NodeConfig(is_result_ephemeral=True)
        node.node_config = NodeConfig(is_result_ephemeral=False)
        assert node.node_config.is_result_ephemeral is False

    def test_ephemeral_false_resolves_correctly(self):
        """is_result_ephemeral=None resolves to False at execution."""
        node = _make_node()
        assert (node.node_config.is_result_ephemeral or False) is False

    def test_ephemeral_true_resolves_correctly(self):
        """is_result_ephemeral=True resolves to True at execution."""
        node = _make_node()
        node.node_config = NodeConfig(is_result_ephemeral=True)
        assert (node.node_config.is_result_ephemeral or False) is True
