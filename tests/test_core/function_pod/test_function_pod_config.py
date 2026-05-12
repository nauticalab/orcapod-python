"""Tests for FunctionPod to_config / from_config serialization."""

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction


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
