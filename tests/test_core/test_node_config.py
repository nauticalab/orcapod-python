import pytest

from orcapod.types import NodeConfig, PipelineConfig, PodConfig, resolve_concurrency


class TestPodConfig:
    def test_defaults(self):
        config = PodConfig()
        assert config.max_concurrency is None

    def test_max_concurrency(self):
        config = PodConfig(max_concurrency=4)
        assert config.max_concurrency == 4

    def test_immutable(self):
        config = PodConfig(max_concurrency=4)
        with pytest.raises((AttributeError, TypeError)):
            config.max_concurrency = 8  # type: ignore[misc]


class TestNodeConfig:
    def test_defaults(self):
        config = NodeConfig()
        assert config.is_result_ephemeral is None

    def test_is_result_ephemeral_true(self):
        config = NodeConfig(is_result_ephemeral=True)
        assert config.is_result_ephemeral is True

    def test_is_result_ephemeral_false(self):
        config = NodeConfig(is_result_ephemeral=False)
        assert config.is_result_ephemeral is False

    def test_immutable(self):
        config = NodeConfig(is_result_ephemeral=True)
        with pytest.raises((AttributeError, TypeError)):
            config.is_result_ephemeral = False  # type: ignore[misc]

    def test_merge_none_in_other_self_wins(self):
        """None in other does not override self's value."""
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig())
        assert result.is_result_ephemeral is True

    def test_merge_non_none_in_other_other_wins(self):
        """Non-None in other overrides self."""
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig(is_result_ephemeral=False))
        assert result.is_result_ephemeral is False

    def test_merge_false_overrides_true(self):
        """Explicit False wins over True."""
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig(is_result_ephemeral=False))
        assert result.is_result_ephemeral is False

    def test_merge_both_none(self):
        result = NodeConfig().merge(NodeConfig())
        assert result.is_result_ephemeral is None

    def test_merge_returns_new_instance(self):
        base = NodeConfig(is_result_ephemeral=True)
        result = base.merge(NodeConfig())
        assert result is not base


class TestResolveConcurrency:
    def test_pod_config_wins_over_pipeline(self):
        pod = PodConfig(max_concurrency=4)
        pipeline = PipelineConfig(default_max_concurrency=2)
        assert resolve_concurrency(pod, pipeline) == 4

    def test_falls_back_to_pipeline_when_pod_is_none(self):
        pod = PodConfig(max_concurrency=None)
        pipeline = PipelineConfig(default_max_concurrency=2)
        assert resolve_concurrency(pod, pipeline) == 2

    def test_both_none_returns_none(self):
        pod = PodConfig(max_concurrency=None)
        pipeline = PipelineConfig(default_max_concurrency=None)
        assert resolve_concurrency(pod, pipeline) is None

    def test_invalid_zero_raises(self):
        pod = PodConfig(max_concurrency=0)
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            resolve_concurrency(pod, PipelineConfig())

    def test_invalid_negative_raises(self):
        pod = PodConfig(max_concurrency=-1)
        with pytest.raises(ValueError, match="max_concurrency must be >= 1"):
            resolve_concurrency(pod, PipelineConfig())
