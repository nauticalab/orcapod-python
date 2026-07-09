import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob
from orcapod.types import NodeConfig


def _make_job() -> PipelineJob:
    """Two-node pipeline: double → triple."""

    def double(x: int) -> int:
        return x * 2

    def triple(doubled: int) -> int:
        return doubled * 3

    pf1 = PythonDataFunction(double, output_keys="doubled")
    pf2 = PythonDataFunction(triple, output_keys="tripled")
    pod1 = FunctionPod(pf1)
    pod2 = FunctionPod(pf2)
    db = InMemoryArrowDatabase()
    table = pa.table(
        {
            "id": pa.array(["r1", "r2"], type=pa.large_string()),
            "x": pa.array([10, 20], type=pa.int64()),
        }
    )
    source = ArrowTableSource(table, tag_columns=["id"], infer_nullable=True)
    job = PipelineJob(name="test_job", store=db)
    with job:
        out1 = pod1(source)
        pod2(out1)
    return job


class TestApplyNodeConfig:
    def test_apply_sets_config_on_all_nodes(self):
        """apply_node_config sets config on every FunctionJobNode."""
        job = _make_job()
        config = NodeConfig(is_result_ephemeral=True)
        job.apply_node_config(config)
        nodes = list(job._iter_function_job_nodes())
        assert len(nodes) == 2
        for node in nodes:
            assert node.node_config.is_result_ephemeral is True

    def test_apply_replace_existing_false_wholesale(self):
        """replace_existing=False replaces each node's config wholesale."""
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        # Pre-set one node to ephemeral=True
        nodes[0].node_config = NodeConfig(is_result_ephemeral=True)
        # Apply a new config wholesale — replaces whatever each node had
        job.apply_node_config(NodeConfig(is_result_ephemeral=False), replace_existing=False)
        for node in job._iter_function_job_nodes():
            assert node.node_config.is_result_ephemeral is False

    def test_apply_replace_existing_true_merges_none_no_op(self):
        """replace_existing=True: None in new config leaves existing values untouched."""
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        nodes[0].node_config = NodeConfig(is_result_ephemeral=True)
        # Merge with a config that has None — should not override True
        job.apply_node_config(NodeConfig(), replace_existing=True)
        assert nodes[0].node_config.is_result_ephemeral is True

    def test_apply_replace_existing_true_overrides_with_explicit_value(self):
        """replace_existing=True: explicit False in new config overrides existing True."""
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        nodes[0].node_config = NodeConfig(is_result_ephemeral=True)
        job.apply_node_config(NodeConfig(is_result_ephemeral=False), replace_existing=True)
        assert nodes[0].node_config.is_result_ephemeral is False

    def test_iter_function_job_nodes_yields_only_function_nodes(self):
        """_iter_function_job_nodes does not yield source or operator nodes."""
        from orcapod.core.nodes.function_node import FunctionJobNode
        job = _make_job()
        nodes = list(job._iter_function_job_nodes())
        assert all(isinstance(n, FunctionJobNode) for n in nodes)

    def test_apply_before_compilation_raises(self):
        """apply_node_config raises RuntimeError if the job is not compiled yet."""
        db = InMemoryArrowDatabase()
        job = PipelineJob(name="uncompiled_job", store=db)
        with pytest.raises(RuntimeError, match="No compiled pipeline"):
            job.apply_node_config(NodeConfig(is_result_ephemeral=True))
