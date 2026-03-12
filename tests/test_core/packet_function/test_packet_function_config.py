"""Tests for PacketFunction to_config / from_config serialization."""

import pytest

from orcapod.core.packet_function import PythonPacketFunction


def sample_transform(age: int, name: str) -> dict:
    return {"age_plus_one": age + 1}


class TestPythonPacketFunctionConfig:
    def test_to_config_includes_type_id(self):
        pf = PythonPacketFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
            version="v1.0",
        )
        config = pf.to_config()
        assert config["packet_function_type_id"] == "python.function.v0"

    def test_to_config_includes_module_and_name(self):
        pf = PythonPacketFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
        )
        config = pf.to_config()
        assert "module_path" in config["config"]
        assert "callable_name" in config["config"]
        assert config["config"]["callable_name"] == "sample_transform"

    def test_to_config_includes_version(self):
        pf = PythonPacketFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
            version="v2.1",
        )
        config = pf.to_config()
        assert config["config"]["version"] == "v2.1"

    def test_to_config_includes_schemas(self):
        pf = PythonPacketFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
        )
        config = pf.to_config()
        assert "input_packet_schema" in config["config"]
        assert "output_packet_schema" in config["config"]

    def test_round_trip(self):
        pf = PythonPacketFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
            version="v1.0",
        )
        config = pf.to_config()
        restored = PythonPacketFunction.from_config(config)
        assert restored.canonical_function_name == pf.canonical_function_name
        assert restored.packet_function_type_id == pf.packet_function_type_id

    def test_from_config_with_missing_module_raises(self):
        config = {
            "packet_function_type_id": "python.function.v0",
            "config": {
                "module_path": "nonexistent.module",
                "callable_name": "func",
                "version": "v0.0",
                "input_packet_schema": {},
                "output_packet_schema": {},
            },
        }
        with pytest.raises((ImportError, ModuleNotFoundError)):
            PythonPacketFunction.from_config(config)
