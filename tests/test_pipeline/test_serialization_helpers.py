"""Tests for pipeline serialization registries and helpers."""

import pytest

from orcapod.pipeline.serialization import (
    DATABASE_REGISTRY,
    OPERATOR_REGISTRY,
    PACKET_FUNCTION_REGISTRY,
    SOURCE_REGISTRY,
    LoadStatus,
    PIPELINE_FORMAT_VERSION,
    resolve_database_from_config,
    resolve_operator_from_config,
)
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.core.operators import Join, Batch


class TestRegistries:
    def test_database_registry_has_all_types(self):
        assert "delta_table" in DATABASE_REGISTRY
        assert "in_memory" in DATABASE_REGISTRY
        assert "noop" in DATABASE_REGISTRY

    def test_source_registry_has_all_types(self):
        assert "csv" in SOURCE_REGISTRY
        assert "delta_table" in SOURCE_REGISTRY
        assert "dict" in SOURCE_REGISTRY

    def test_operator_registry_has_all_types(self):
        assert "Join" in OPERATOR_REGISTRY
        assert "Batch" in OPERATOR_REGISTRY
        assert "SelectTagColumns" in OPERATOR_REGISTRY

    def test_packet_function_registry(self):
        assert "python.function.v0" in PACKET_FUNCTION_REGISTRY


class TestLoadStatus:
    def test_enum_values(self):
        assert LoadStatus.FULL.value == "full"
        assert LoadStatus.READ_ONLY.value == "read_only"
        assert LoadStatus.UNAVAILABLE.value == "unavailable"


class TestResolveDatabaseFromConfig:
    def test_resolve_in_memory(self):
        config = {"type": "in_memory", "max_hierarchy_depth": 10}
        db = resolve_database_from_config(config)
        assert isinstance(db, InMemoryArrowDatabase)

    def test_resolve_unknown_type_raises(self):
        config = {"type": "unknown_db"}
        with pytest.raises(ValueError, match="Unknown database type"):
            resolve_database_from_config(config)


class TestResolveOperatorFromConfig:
    def test_resolve_join(self):
        config = {
            "class_name": "Join",
            "module_path": "orcapod.core.operators.join",
            "config": {},
        }
        op = resolve_operator_from_config(config)
        assert isinstance(op, Join)

    def test_resolve_batch(self):
        config = {
            "class_name": "Batch",
            "module_path": "orcapod.core.operators.batch",
            "config": {"batch_size": 5},
        }
        op = resolve_operator_from_config(config)
        assert isinstance(op, Batch)

    def test_resolve_unknown_raises(self):
        config = {
            "class_name": "UnknownOp",
            "module_path": "orcapod.core.operators",
            "config": {},
        }
        with pytest.raises(ValueError, match="Unknown operator"):
            resolve_operator_from_config(config)


class TestPipelineFormatVersion:
    def test_version_is_string(self):
        assert isinstance(PIPELINE_FORMAT_VERSION, str)
        assert PIPELINE_FORMAT_VERSION == "0.1.0"
