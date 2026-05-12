"""Tests for DataFunction to_config / from_config serialization."""

import pytest

from orcapod.core.data_function import PythonDataFunction


def sample_transform(age: int, name: str) -> dict[str, int]:
    return {"age_plus_one": age + 1}


class TestPythonDataFunctionConfig:
    def test_to_config_includes_type_id(self):
        pf = PythonDataFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
            version="v1.0",
        )
        config = pf.to_config()
        assert config["data_function_type_id"] == "python.function.v0"

    def test_to_config_includes_module_and_name(self):
        pf = PythonDataFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
        )
        config = pf.to_config()
        assert "module_path" in config["config"]
        assert "callable_name" in config["config"]
        assert config["config"]["callable_name"] == "sample_transform"

    def test_to_config_includes_version(self):
        pf = PythonDataFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
            version="v2.1",
        )
        config = pf.to_config()
        assert config["config"]["version"] == "v2.1"

    def test_to_config_includes_schemas(self):
        pf = PythonDataFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
        )
        config = pf.to_config()
        assert "input_data_schema" in config["config"]
        assert "output_data_schema" in config["config"]

    def test_round_trip(self):
        pf = PythonDataFunction(
            function=sample_transform,
            output_keys=["age_plus_one"],
            version="v1.0",
        )
        config = pf.to_config()
        restored = PythonDataFunction.from_config(config)
        assert restored.canonical_function_name == pf.canonical_function_name
        assert restored.data_function_type_id == pf.data_function_type_id

    def test_from_config_with_missing_module_raises(self):
        config = {
            "data_function_type_id": "python.function.v0",
            "config": {
                "module_path": "nonexistent.module",
                "callable_name": "func",
                "version": "v0.0",
                "input_data_schema": {},
                "output_data_schema": {},
            },
        }
        with pytest.raises((ImportError, ModuleNotFoundError)):
            PythonDataFunction.from_config(config)
