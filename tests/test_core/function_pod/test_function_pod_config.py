"""Tests for FunctionPod to_config / from_config serialization."""

import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction


def sample_transform(age: int) -> dict[str, int]:
    return {"age_plus_one": age + 1}


class TestFunctionPodConfig:
    def test_to_config_includes_uri(self):
        pf = PythonPacketFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(packet_function=pf)
        config = pod.to_config()
        assert "uri" in config
        assert config["uri"] == list(pod.uri)

    def test_to_config_includes_packet_function(self):
        pf = PythonPacketFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(packet_function=pf)
        config = pod.to_config()
        assert "packet_function" in config
        assert (
            config["packet_function"]["packet_function_type_id"] == "python.function.v0"
        )

    def test_round_trip(self):
        pf = PythonPacketFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(packet_function=pf)
        config = pod.to_config()
        restored = FunctionPod.from_config(config)
        assert isinstance(restored, FunctionPod)
        assert restored.uri == pod.uri
