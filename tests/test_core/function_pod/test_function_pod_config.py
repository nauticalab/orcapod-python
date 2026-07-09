"""Tests for FunctionPod to_config / from_config serialization."""

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.types import PodConfig


def sample_transform(age: int) -> dict[str, int]:
    return {"age_plus_one": age + 1}


class TestFunctionPodConfig:
    def test_to_config_includes_uri(self):
        pf = PythonDataFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(data_function=pf)
        config = pod.to_config()
        assert "uri" in config
        assert config["uri"] == list(pod.uri)

    def test_to_config_includes_data_function(self):
        pf = PythonDataFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(data_function=pf)
        config = pod.to_config()
        assert "data_function" in config
        assert (
            config["data_function"]["data_function_type_id"] == "python.function.v0"
        )

    def test_round_trip(self):
        pf = PythonDataFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(data_function=pf)
        config = pod.to_config()
        restored = FunctionPod.from_config(config)
        assert isinstance(restored, FunctionPod)
        assert restored.uri == pod.uri


class TestPodConfigSerialization:
    """Tests for pod_config serialization and round-trip."""

    def test_to_config_with_non_default_pod_config(self):
        """Verify that a FunctionPod with PodConfig(max_concurrency=4) serializes correctly.

        A FunctionPod with a non-default PodConfig should serialize to a dict
        where "pod_config" contains the configuration values.
        """
        pf = PythonDataFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(
            data_function=pf,
            pod_config=PodConfig(max_concurrency=4),
        )
        config = pod.to_config()
        assert "pod_config" in config
        assert config["pod_config"] is not None
        assert config["pod_config"]["max_concurrency"] == 4

    def test_to_config_with_default_pod_config(self):
        """Verify that a FunctionPod with default PodConfig() serializes with pod_config=None.

        A FunctionPod with a default (unspecified) PodConfig should serialize
        to a dict where "pod_config" is None.
        """
        pf = PythonDataFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(data_function=pf)
        config = pod.to_config()
        assert "pod_config" in config
        assert config["pod_config"] is None

    def test_pod_config_round_trip(self):
        """Verify that FunctionPod.from_config() round-trips the pod_config field.

        A FunctionPod with PodConfig(max_concurrency=4) should reconstruct
        from its config with the same max_concurrency value.
        """
        pf = PythonDataFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        original_pod = FunctionPod(
            data_function=pf,
            pod_config=PodConfig(max_concurrency=4),
        )
        config = original_pod.to_config()
        restored_pod = FunctionPod.from_config(config)

        assert restored_pod.pod_config.max_concurrency == 4
