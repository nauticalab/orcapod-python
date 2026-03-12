# Pipeline Serialization Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Pipeline.save()` / `Pipeline.load()` with read-only mode, enabling pipeline sharing via JSON files.

**Architecture:** Protocol-level `to_config()` / `from_config()` on databases, sources, operators, and packet functions. Registries map type strings to classes for deserialization. Node classes gain `from_descriptor()` for read-only instantiation. `Pipeline` gets `save()` and `load()` methods that serialize/deserialize the compiled graph.

**Tech Stack:** Python, JSON, PyArrow (type serialization), NetworkX (graph topology)

**Spec:** `docs/superpowers/specs/2026-03-12-pipeline-serialization-design.md`

---

## File Structure

### New files
- `src/orcapod/pipeline/serialization.py` — registries, `LoadStatus` enum, schema serialization helpers, `PIPELINE_FORMAT_VERSION` constant
- `tests/test_pipeline/test_serialization.py` — end-to-end save/load tests

### Modified files
- `src/orcapod/protocols/database_protocols.py` — add `to_config()` / `from_config()` to `ArrowDatabaseProtocol`
- `src/orcapod/protocols/core_protocols/sources.py` — add `to_config()` / `from_config()` to `SourceProtocol`
- `src/orcapod/protocols/core_protocols/packet_function.py` — add `to_config()` / `from_config()` to `PacketFunctionProtocol`
- `src/orcapod/protocols/core_protocols/operator_pod.py` — add `to_config()` / `from_config()` to `OperatorPodProtocol`
- `src/orcapod/protocols/core_protocols/function_pod.py` — add `to_config()` / `from_config()` to `FunctionPodProtocol`
- `src/orcapod/databases/delta_lake_databases.py` — implement `to_config()` / `from_config()`
- `src/orcapod/databases/in_memory_databases.py` — implement `to_config()` / `from_config()`
- `src/orcapod/databases/noop_database.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/base.py` — add `to_config()` / `from_config()` to `RootSource`
- `src/orcapod/core/sources/arrow_table_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/csv_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/delta_table_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/dict_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/list_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/data_frame_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/sources/cached_source.py` — implement `to_config()` / `from_config()`
- `src/orcapod/core/packet_function.py` — implement `to_config()` / `from_config()` on `PythonPacketFunction`
- `src/orcapod/core/function_pod.py` — implement `to_config()` / `from_config()` on `FunctionPod`
- `src/orcapod/core/operators/base.py` — add default `to_config()` / `from_config()` on `StaticOutputOperatorPod`
- `src/orcapod/core/operators/batch.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/operators/column_selection.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/operators/mappers.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/operators/filters.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/operators/join.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/operators/merge_join.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/operators/semijoin.py` — override `to_config()` / `from_config()`
- `src/orcapod/core/nodes/source_node.py` — add `from_descriptor()` classmethod
- `src/orcapod/core/nodes/function_node.py` — add `from_descriptor()` to `PersistentFunctionNode`
- `src/orcapod/core/nodes/operator_node.py` — add `from_descriptor()` to `PersistentOperatorNode`
- `src/orcapod/pipeline/graph.py` — add `save()` and `load()` methods to `Pipeline`

---

## Chunk 1: Protocol Extensions and Database Serialization

### Task 1: Add `to_config` / `from_config` to `ArrowDatabaseProtocol`

**Files:**
- Modify: `src/orcapod/protocols/database_protocols.py`
- Test: `tests/test_databases/test_database_config.py`

- [ ] **Step 1: Write failing tests for database config round-trip**

Create `tests/test_databases/test_database_config.py`:

```python
"""Tests for database to_config / from_config serialization."""
import pytest

from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.databases.noop_database import NoOpArrowDatabase


class TestDeltaTableDatabaseConfig:
    def test_to_config_includes_type(self, tmp_path):
        db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
        config = db.to_config()
        assert config["type"] == "delta_table"

    def test_to_config_includes_base_path(self, tmp_path):
        db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
        config = db.to_config()
        assert config["base_path"] == str(tmp_path / "delta_db")

    def test_to_config_includes_all_settings(self, tmp_path):
        db = DeltaTableDatabase(
            base_path=str(tmp_path / "delta_db"),
            batch_size=500,
            max_hierarchy_depth=5,
            allow_schema_evolution=False,
        )
        config = db.to_config()
        assert config["batch_size"] == 500
        assert config["max_hierarchy_depth"] == 5
        assert config["allow_schema_evolution"] is False

    def test_round_trip(self, tmp_path):
        db = DeltaTableDatabase(
            base_path=str(tmp_path / "delta_db"),
            batch_size=500,
            max_hierarchy_depth=5,
        )
        config = db.to_config()
        restored = DeltaTableDatabase.from_config(config)
        assert restored.to_config() == config


class TestInMemoryDatabaseConfig:
    def test_to_config_includes_type(self):
        db = InMemoryArrowDatabase()
        config = db.to_config()
        assert config["type"] == "in_memory"

    def test_round_trip(self):
        db = InMemoryArrowDatabase(max_hierarchy_depth=5)
        config = db.to_config()
        restored = InMemoryArrowDatabase.from_config(config)
        assert restored.to_config() == config


class TestNoOpDatabaseConfig:
    def test_to_config_includes_type(self):
        db = NoOpArrowDatabase()
        config = db.to_config()
        assert config["type"] == "noop"

    def test_round_trip(self):
        db = NoOpArrowDatabase()
        config = db.to_config()
        restored = NoOpArrowDatabase.from_config(config)
        assert restored.to_config() == config
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_databases/test_database_config.py -v`
Expected: FAIL — `to_config` / `from_config` not defined

- [ ] **Step 3: Add `to_config` / `from_config` to `ArrowDatabaseProtocol`**

In `src/orcapod/protocols/database_protocols.py`, add to `ArrowDatabaseProtocol`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict.

    The returned dict must include a ``"type"`` key identifying the
    database implementation (e.g., ``"delta_table"``, ``"in_memory"``).
    """
    ...

@classmethod
def from_config(cls, config: "dict[str, Any]") -> "ArrowDatabaseProtocol":
    """Reconstruct a database instance from a config dict."""
    ...
```

Note: Add `Any` to the existing typing imports.

- [ ] **Step 4: Implement `to_config` / `from_config` on `DeltaTableDatabase`**

In `src/orcapod/databases/delta_lake_databases.py`, add methods to `DeltaTableDatabase`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict."""
    return {
        "type": "delta_table",
        "base_path": str(self.base_path),
        "batch_size": self.batch_size,
        "max_hierarchy_depth": self.max_hierarchy_depth,
        "allow_schema_evolution": self.allow_schema_evolution,
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> "DeltaTableDatabase":
    """Reconstruct a DeltaTableDatabase from a config dict."""
    return cls(
        base_path=config["base_path"],
        create_base_path=True,
        batch_size=config.get("batch_size", 1000),
        max_hierarchy_depth=config.get("max_hierarchy_depth", 10),
        allow_schema_evolution=config.get("allow_schema_evolution", True),
    )
```

Note: Attributes are public (`self.base_path`, not `self._base_path`). `from_config()` always passes `create_base_path=True` so the directory is auto-created on reconstruction.

- [ ] **Step 5: Implement `to_config` / `from_config` on `InMemoryArrowDatabase`**

In `src/orcapod/databases/in_memory_databases.py`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict."""
    return {
        "type": "in_memory",
        "max_hierarchy_depth": self.max_hierarchy_depth,
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> "InMemoryArrowDatabase":
    """Reconstruct an InMemoryArrowDatabase from a config dict."""
    return cls(
        max_hierarchy_depth=config.get("max_hierarchy_depth", 10),
    )
```

Note: Attribute is public (`self.max_hierarchy_depth`).

- [ ] **Step 6: Implement `to_config` / `from_config` on `NoOpArrowDatabase`**

In `src/orcapod/databases/noop_database.py`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict."""
    return {"type": "noop"}

@classmethod
def from_config(cls, config: dict[str, Any]) -> "NoOpArrowDatabase":
    """Reconstruct a NoOpArrowDatabase from a config dict."""
    return cls()
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `uv run pytest tests/test_databases/test_database_config.py -v`
Expected: All PASS

- [ ] **Step 8: Run full database test suite to check for regressions**

Run: `uv run pytest tests/test_databases/ -v`
Expected: All PASS

- [ ] **Step 9: Commit**

```bash
git add -A && git commit -m "feat(databases): add to_config/from_config to ArrowDatabaseProtocol and implementations"
```

---

### Task 2: Add `to_config` / `from_config` to operator protocol and all operators

**Files:**
- Modify: `src/orcapod/protocols/core_protocols/operator_pod.py`
- Modify: `src/orcapod/core/operators/base.py`
- Modify: `src/orcapod/core/operators/batch.py`
- Modify: `src/orcapod/core/operators/column_selection.py`
- Modify: `src/orcapod/core/operators/mappers.py`
- Modify: `src/orcapod/core/operators/filters.py`
- Modify: `src/orcapod/core/operators/join.py`
- Modify: `src/orcapod/core/operators/merge_join.py`
- Modify: `src/orcapod/core/operators/semijoin.py`
- Test: `tests/test_core/operators/test_operator_config.py`

- [ ] **Step 1: Write failing tests for operator config round-trip**

Create `tests/test_core/operators/test_operator_config.py`:

```python
"""Tests for operator to_config / from_config serialization."""
import pytest

from orcapod.core.operators import (
    Batch,
    DropPacketColumns,
    DropTagColumns,
    Join,
    MapPackets,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectPacketColumns,
    SelectTagColumns,
    SemiJoin,
)


class TestJoinConfig:
    def test_to_config(self):
        op = Join()
        config = op.to_config()
        assert config["class_name"] == "Join"
        assert config["module_path"] == "orcapod.core.operators.join"

    def test_round_trip(self):
        op = Join()
        config = op.to_config()
        restored = Join.from_config(config)
        assert isinstance(restored, Join)


class TestMergeJoinConfig:
    def test_to_config(self):
        op = MergeJoin()
        config = op.to_config()
        assert config["class_name"] == "MergeJoin"

    def test_round_trip(self):
        op = MergeJoin()
        config = op.to_config()
        restored = MergeJoin.from_config(config)
        assert isinstance(restored, MergeJoin)


class TestSemiJoinConfig:
    def test_to_config(self):
        op = SemiJoin()
        config = op.to_config()
        assert config["class_name"] == "SemiJoin"

    def test_round_trip(self):
        op = SemiJoin()
        config = op.to_config()
        restored = SemiJoin.from_config(config)
        assert isinstance(restored, SemiJoin)


class TestBatchConfig:
    def test_to_config_default(self):
        op = Batch()
        config = op.to_config()
        assert config["class_name"] == "Batch"
        assert config["config"]["batch_size"] == 0
        assert config["config"]["drop_partial_batch"] is False

    def test_to_config_custom(self):
        op = Batch(batch_size=10, drop_partial_batch=True)
        config = op.to_config()
        assert config["config"]["batch_size"] == 10
        assert config["config"]["drop_partial_batch"] is True

    def test_round_trip(self):
        op = Batch(batch_size=10, drop_partial_batch=True)
        config = op.to_config()
        restored = Batch.from_config(config)
        assert isinstance(restored, Batch)
        assert restored.batch_size == 10
        assert restored.drop_partial_batch is True


class TestSelectTagColumnsConfig:
    def test_to_config(self):
        op = SelectTagColumns(columns=["a", "b"], strict=False)
        config = op.to_config()
        assert config["class_name"] == "SelectTagColumns"
        assert config["config"]["columns"] == ["a", "b"]
        assert config["config"]["strict"] is False

    def test_round_trip(self):
        op = SelectTagColumns(columns=["a", "b"])
        config = op.to_config()
        restored = SelectTagColumns.from_config(config)
        assert isinstance(restored, SelectTagColumns)


class TestDropTagColumnsConfig:
    def test_round_trip(self):
        op = DropTagColumns(columns=["x"])
        config = op.to_config()
        restored = DropTagColumns.from_config(config)
        assert isinstance(restored, DropTagColumns)
        assert config["class_name"] == "DropTagColumns"


class TestSelectPacketColumnsConfig:
    def test_round_trip(self):
        op = SelectPacketColumns(columns=["a"])
        config = op.to_config()
        restored = SelectPacketColumns.from_config(config)
        assert isinstance(restored, SelectPacketColumns)


class TestDropPacketColumnsConfig:
    def test_round_trip(self):
        op = DropPacketColumns(columns=["a"])
        config = op.to_config()
        restored = DropPacketColumns.from_config(config)
        assert isinstance(restored, DropPacketColumns)


class TestMapTagsConfig:
    def test_to_config(self):
        op = MapTags(name_map={"old": "new"}, drop_unmapped=True)
        config = op.to_config()
        assert config["config"]["name_map"] == {"old": "new"}
        assert config["config"]["drop_unmapped"] is True

    def test_round_trip(self):
        op = MapTags(name_map={"old": "new"})
        config = op.to_config()
        restored = MapTags.from_config(config)
        assert isinstance(restored, MapTags)


class TestMapPacketsConfig:
    def test_round_trip(self):
        op = MapPackets(name_map={"old": "new"}, drop_unmapped=True)
        config = op.to_config()
        restored = MapPackets.from_config(config)
        assert isinstance(restored, MapPackets)


class TestPolarsFilterConfig:
    def test_to_config_with_constraints(self):
        op = PolarsFilter(constraints={"age": 25})
        config = op.to_config()
        assert config["class_name"] == "PolarsFilter"
        assert config["config"]["constraints"] == {"age": 25}

    def test_round_trip_constraints_only(self):
        op = PolarsFilter(constraints={"age": 25})
        config = op.to_config()
        restored = PolarsFilter.from_config(config)
        assert isinstance(restored, PolarsFilter)

    def test_non_serializable_predicates_marked(self):
        """PolarsFilter with Expr predicates should attempt serialization."""
        import polars as pl

        op = PolarsFilter(predicates=[pl.col("age") > 18])
        config = op.to_config()
        # Should either serialize the expression or mark as non-reconstructable
        assert "config" in config
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/operators/test_operator_config.py -v`
Expected: FAIL — `to_config` / `from_config` not defined

- [ ] **Step 3: Add `to_config` / `from_config` to `OperatorPodProtocol`**

In `src/orcapod/protocols/core_protocols/operator_pod.py`, add:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this operator to a JSON-compatible config dict."""
    ...

@classmethod
def from_config(cls, config: dict[str, Any]) -> "OperatorPodProtocol":
    """Reconstruct an operator from a config dict."""
    ...
```

- [ ] **Step 4: Implement default `to_config` / `from_config` on `StaticOutputOperatorPod`**

In `src/orcapod/core/operators/base.py`, add to `StaticOutputOperatorPod`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this operator to a JSON-compatible config dict.

    Subclasses with constructor parameters should override this to include
    their specific config in the ``"config"`` key.
    """
    return {
        "class_name": self.__class__.__name__,
        "module_path": self.__class__.__module__,
        "config": {},
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> "StaticOutputOperatorPod":
    """Reconstruct an operator from a config dict."""
    return cls(**config.get("config", {}))
```

This provides a sensible default for operators with no constructor parameters (Join, MergeJoin, SemiJoin).

- [ ] **Step 5: Override `to_config` on operators with constructor parameters**

For each operator that has constructor args, override `to_config()`. The `from_config()` default usually works since it passes `config["config"]` as kwargs. Override `from_config()` only if the constructor signature doesn't match a simple `**kwargs` pattern.

**Batch** (`src/orcapod/core/operators/batch.py`):
```python
def to_config(self) -> dict[str, Any]:
    config = super().to_config()
    config["config"] = {
        "batch_size": self.batch_size,
        "drop_partial_batch": self.drop_partial_batch,
    }
    return config
```

**SelectTagColumns** (`src/orcapod/core/operators/column_selection.py`):
```python
def to_config(self) -> dict[str, Any]:
    config = super().to_config()
    config["config"] = {
        "columns": list(self.columns),
        "strict": self.strict,
    }
    return config
```

Do the same for `DropTagColumns`, `SelectPacketColumns`, `DropPacketColumns` (same pattern — they store `self.columns` and `self.strict`).

**MapTags** and **MapPackets** (`src/orcapod/core/operators/mappers.py`):
```python
def to_config(self) -> dict[str, Any]:
    config = super().to_config()
    config["config"] = {
        "name_map": dict(self.name_map),
        "drop_unmapped": self.drop_unmapped,
    }
    return config
```

**PolarsFilter** (`src/orcapod/core/operators/filters.py`):
```python
def to_config(self) -> dict[str, Any]:
    config = super().to_config()
    serialized_predicates = []
    reconstructable = True
    for pred in self.predicates:
        if hasattr(pred, "meta") and hasattr(pred.meta, "serialize"):
            serialized_predicates.append(
                pred.meta.serialize(format="json").decode()
            )
        else:
            reconstructable = False
            break
    config["config"] = {
        "constraints": dict(self.constraints) if self.constraints else None,
        "predicates": serialized_predicates if reconstructable else None,
        "reconstructable": reconstructable,
    }
    return config

@classmethod
def from_config(cls, config: dict[str, Any]) -> "PolarsFilter":
    inner = config.get("config", {})
    if not inner.get("reconstructable", True):
        raise NotImplementedError(
            "PolarsFilter with non-serializable predicates cannot be reconstructed"
        )
    predicates = []
    if inner.get("predicates"):
        import polars as pl
        predicates = [
            pl.Expr.deserialize(p.encode(), format="json")
            for p in inner["predicates"]
        ]
    constraints = inner.get("constraints")
    return cls(predicates=predicates, constraints=constraints)
```

Note: Attributes are public (`self.predicates`, `self.constraints`). Polars expression
serialization uses `format="json"` for JSON-safe output. Check the installed Polars version
supports `Expr.meta.serialize(format=...)` / `Expr.deserialize(data, format=...)` — this
API is available in Polars >= 0.19.

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/operators/test_operator_config.py -v`
Expected: All PASS

- [ ] **Step 7: Run full operator test suite**

Run: `uv run pytest tests/test_core/operators/ -v`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(operators): add to_config/from_config to OperatorPodProtocol and all operators"
```

---

### Task 3: Add `to_config` / `from_config` to `PacketFunctionProtocol` and `PythonPacketFunction`

**Files:**
- Modify: `src/orcapod/protocols/core_protocols/packet_function.py`
- Modify: `src/orcapod/core/packet_function.py`
- Test: `tests/test_core/packet_function/test_packet_function_config.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_core/packet_function/test_packet_function_config.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_config.py -v`
Expected: FAIL

- [ ] **Step 3: Add `to_config` / `from_config` to `PacketFunctionProtocol`**

In `src/orcapod/protocols/core_protocols/packet_function.py`, add:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this packet function to a JSON-compatible config dict."""
    ...

@classmethod
def from_config(cls, config: dict[str, Any]) -> "PacketFunctionProtocol":
    """Reconstruct a packet function from a config dict."""
    ...
```

- [ ] **Step 4: Implement `to_config` / `from_config` on `PythonPacketFunction`**

In `src/orcapod/core/packet_function.py`, add to `PythonPacketFunction`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this packet function to a JSON-compatible config dict."""
    return {
        "packet_function_type_id": self.packet_function_type_id,
        "config": {
            "module_path": self._function.__module__,
            "callable_name": self._function_name,
            "version": self._version,
            "input_packet_schema": {
                k: str(v) for k, v in self.input_packet_schema.items()
            },
            "output_packet_schema": {
                k: str(v) for k, v in self.output_packet_schema.items()
            },
            "output_keys": list(self._output_keys) if self._output_keys else None,
        },
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> "PythonPacketFunction":
    """Reconstruct a PythonPacketFunction by importing the callable."""
    import importlib

    inner = config.get("config", config)
    module = importlib.import_module(inner["module_path"])
    func = getattr(module, inner["callable_name"])
    return cls(
        function=func,
        output_keys=inner.get("output_keys"),
        version=inner.get("version", "v0.0"),
    )
```

Note: Check that `self._function.__module__` and `self._function_name` give the correct import path. Read `packet_function.py` to confirm how `_function_name` is set (it may be `_function.__qualname__` or custom). The `from_config` uses `importlib` to dynamically import.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/packet_function/test_packet_function_config.py -v`
Expected: All PASS

- [ ] **Step 6: Run full packet_function test suite**

Run: `uv run pytest tests/test_core/packet_function/ -v`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(packet-function): add to_config/from_config to PacketFunctionProtocol and PythonPacketFunction"
```

---

### Task 4: Add `to_config` / `from_config` to `FunctionPod`

**Files:**
- Modify: `src/orcapod/core/function_pod.py`
- Test: `tests/test_core/function_pod/test_function_pod_config.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_core/function_pod/test_function_pod_config.py`:

```python
"""Tests for FunctionPod to_config / from_config serialization."""
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction


def sample_transform(age: int) -> dict:
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
        assert config["packet_function"]["packet_function_type_id"] == "python.function.v0"

    def test_round_trip(self):
        pf = PythonPacketFunction(
            function=sample_transform, output_keys=["age_plus_one"]
        )
        pod = FunctionPod(packet_function=pf)
        config = pod.to_config()
        restored = FunctionPod.from_config(config)
        assert isinstance(restored, FunctionPod)
        assert restored.uri == pod.uri
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/function_pod/test_function_pod_config.py -v`
Expected: FAIL

- [ ] **Step 3: Add `to_config` / `from_config` to `FunctionPodProtocol`**

In `src/orcapod/protocols/core_protocols/function_pod.py`, add to `FunctionPodProtocol`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this function pod to a JSON-compatible config dict."""
    ...

@classmethod
def from_config(cls, config: dict[str, Any]) -> "FunctionPodProtocol":
    """Reconstruct a function pod from a config dict."""
    ...
```

- [ ] **Step 4: Implement `to_config` / `from_config` on `FunctionPod`**

In `src/orcapod/core/function_pod.py`, add to `FunctionPod`:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize this function pod to a JSON-compatible config dict."""
    config = {
        "uri": list(self.uri),
        "packet_function": self.packet_function.to_config(),
        "node_config": None,
    }
    if self._node_config is not None:
        config["node_config"] = {
            "max_concurrency": self._node_config.max_concurrency,
        }
    return config

@classmethod
def from_config(cls, config: dict[str, Any]) -> "FunctionPod":
    """Reconstruct a FunctionPod from a config dict."""
    from orcapod.pipeline.serialization import PACKET_FUNCTION_REGISTRY
    from orcapod.types import NodeConfig

    pf_config = config["packet_function"]
    type_id = pf_config["packet_function_type_id"]
    pf_cls = PACKET_FUNCTION_REGISTRY[type_id]
    packet_function = pf_cls.from_config(pf_config)

    node_config = None
    if config.get("node_config") is not None:
        node_config = NodeConfig(**config["node_config"])

    return cls(packet_function=packet_function, node_config=node_config)
```

**Important:** This depends on `PACKET_FUNCTION_REGISTRY` from `serialization.py`. Task 6 (which creates `serialization.py`) must be completed before this task. If executing out of order, create a minimal `serialization.py` with just the `PACKET_FUNCTION_REGISTRY` dict first.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/function_pod/test_function_pod_config.py -v`
Expected: All PASS (requires Task 6 / `serialization.py` to exist)

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(function-pod): add to_config/from_config to FunctionPodProtocol and FunctionPod"
```

---

### Task 5: Add `to_config` / `from_config` to `SourceProtocol` and all source implementations

**Files:**
- Modify: `src/orcapod/protocols/core_protocols/sources.py`
- Modify: `src/orcapod/core/sources/base.py`
- Modify: `src/orcapod/core/sources/csv_source.py`
- Modify: `src/orcapod/core/sources/delta_table_source.py`
- Modify: `src/orcapod/core/sources/dict_source.py`
- Modify: `src/orcapod/core/sources/list_source.py`
- Modify: `src/orcapod/core/sources/data_frame_source.py`
- Modify: `src/orcapod/core/sources/arrow_table_source.py`
- Modify: `src/orcapod/core/sources/cached_source.py`
- Test: `tests/test_core/sources/test_source_config.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_core/sources/test_source_config.py`:

```python
"""Tests for source to_config / from_config serialization."""
import pytest

from orcapod.core.sources.csv_source import CSVSource
from orcapod.core.sources.delta_table_source import DeltaTableSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.list_source import ListSource


class TestCSVSourceConfig:
    def test_to_config(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        source = CSVSource(
            file_path=str(csv_file),
            tag_columns=["a"],
            source_id="test_csv",
        )
        config = source.to_config()
        assert config["source_type"] == "csv"
        assert config["file_path"] == str(csv_file)
        assert config["tag_columns"] == ["a"]
        assert config["source_id"] == "test_csv"

    def test_round_trip(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        source = CSVSource(
            file_path=str(csv_file),
            tag_columns=["a"],
        )
        config = source.to_config()
        restored = CSVSource.from_config(config)
        assert isinstance(restored, CSVSource)
        assert restored.source_id == source.source_id


class TestDictSourceConfig:
    def test_to_config(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            tag_columns=["a"],
            source_id="test_dict",
        )
        config = source.to_config()
        assert config["source_type"] == "dict"
        assert config["tag_columns"] == ["a"]
        assert config["source_id"] == "test_dict"

    def test_from_config_raises(self):
        config = {
            "source_type": "dict",
            "tag_columns": ["a"],
            "source_id": "test_dict",
        }
        with pytest.raises(NotImplementedError):
            DictSource.from_config(config)


class TestArrowTableSourceConfig:
    def test_from_config_raises(self):
        config = {"source_type": "arrow_table", "tag_columns": ["a"]}
        with pytest.raises(NotImplementedError):
            ArrowTableSource.from_config(config)


class TestListSourceConfig:
    def test_from_config_raises(self):
        config = {"source_type": "list", "name": "test"}
        with pytest.raises(NotImplementedError):
            ListSource.from_config(config)


class TestDeltaTableSourceConfig:
    def test_to_config(self, tmp_path):
        # Create a minimal delta table for testing
        import pyarrow as pa
        from deltalake import write_deltalake

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        delta_path = str(tmp_path / "delta")
        write_deltalake(delta_path, table)
        source = DeltaTableSource(
            delta_table_path=delta_path,
            tag_columns=["a"],
            source_id="test_delta",
        )
        config = source.to_config()
        assert config["source_type"] == "delta_table"
        assert "delta_table_path" in config

    def test_round_trip(self, tmp_path):
        import pyarrow as pa
        from deltalake import write_deltalake

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        delta_path = str(tmp_path / "delta")
        write_deltalake(delta_path, table)
        source = DeltaTableSource(
            delta_table_path=delta_path,
            tag_columns=["a"],
        )
        config = source.to_config()
        restored = DeltaTableSource.from_config(config)
        assert isinstance(restored, DeltaTableSource)


class TestCachedSourceConfig:
    def test_to_config(self):
        from orcapod.core.sources.cached_source import CachedSource
        from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

        inner = DictSource(
            data=[{"a": 1, "b": 2}], tag_columns=["a"], source_id="inner"
        )
        cache_db = InMemoryArrowDatabase()
        source = CachedSource(source=inner, cache_database=cache_db)
        config = source.to_config()
        assert config["source_type"] == "cached"
        assert "inner_source" in config
        assert "cache_database" in config
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_core/sources/test_source_config.py -v`
Expected: FAIL

- [ ] **Step 3: Add `to_config` / `from_config` to `SourceProtocol`**

In `src/orcapod/protocols/core_protocols/sources.py`, add:

```python
def to_config(self) -> dict[str, Any]:
    """Serialize source configuration to a JSON-compatible dict."""
    ...

@classmethod
def from_config(cls, config: dict[str, Any]) -> "SourceProtocol":
    """Reconstruct a source instance from a config dict."""
    ...
```

- [ ] **Step 4: Implement `to_config` / `from_config` on each source class**

Read each source file to identify the correct attribute names, then implement:

**CSVSource** — `to_config()` returns file_path, tag_columns, system_tag_columns, record_id_column, source_id. `from_config()` reconstructs from these.

**DeltaTableSource** — `to_config()` returns delta_table_path, tag_columns, etc. `from_config()` reconstructs.

**DictSource, ListSource, DataFrameSource, ArrowTableSource** — `to_config()` returns metadata only (tag_columns, source_id, etc.). `from_config()` raises `NotImplementedError`.

**CachedSource** — `to_config()` returns the inner source config + cache database config + cache_path_prefix. `from_config()` reconstructs both inner source and cache database, wraps in CachedSource.

Note: For each source, read the actual file to confirm attribute names. The delegating sources (CSV, Delta, Dict, etc.) store their constructor args differently — some on `self`, some passed directly to the inner `ArrowTableSource`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_core/sources/test_source_config.py -v`
Expected: All PASS

- [ ] **Step 6: Run full source test suite**

Run: `uv run pytest tests/test_core/sources/ -v`
Expected: All PASS

- [ ] **Step 7: Commit**

```bash
git add -A && git commit -m "feat(sources): add to_config/from_config to SourceProtocol and all source implementations"
```

---

## Chunk 2: Registries, LoadStatus, and Node `from_descriptor()`

**Execution order note:** Task 6 must be completed before Task 4 (FunctionPod config)
because `FunctionPod.from_config()` imports from `serialization.py`. When executing this
plan, do: Tasks 1-3, then Task 6, then Tasks 4-5, then Tasks 7-9.

### Task 6: Create `serialization.py` with registries and helpers

**Files:**
- Create: `src/orcapod/pipeline/serialization.py`
- Test: `tests/test_pipeline/test_serialization_helpers.py`

- [ ] **Step 1: Write failing tests for registries and helpers**

Create `tests/test_pipeline/test_serialization_helpers.py`:

```python
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
        config = {"class_name": "Join", "module_path": "orcapod.core.operators.join", "config": {}}
        op = resolve_operator_from_config(config)
        assert isinstance(op, Join)

    def test_resolve_batch(self):
        config = {"class_name": "Batch", "module_path": "orcapod.core.operators.batch", "config": {"batch_size": 5}}
        op = resolve_operator_from_config(config)
        assert isinstance(op, Batch)

    def test_resolve_unknown_raises(self):
        config = {"class_name": "UnknownOp", "module_path": "orcapod.core.operators", "config": {}}
        with pytest.raises(ValueError, match="Unknown operator"):
            resolve_operator_from_config(config)


class TestPipelineFormatVersion:
    def test_version_is_string(self):
        assert isinstance(PIPELINE_FORMAT_VERSION, str)
        assert PIPELINE_FORMAT_VERSION == "0.1.0"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py -v`
Expected: FAIL — module does not exist

- [ ] **Step 3: Create `serialization.py`**

Create `src/orcapod/pipeline/serialization.py`:

```python
"""Pipeline serialization registries, helpers, and constants."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Format version
# ---------------------------------------------------------------------------

PIPELINE_FORMAT_VERSION = "0.1.0"
SUPPORTED_FORMAT_VERSIONS = frozenset({"0.1.0"})

# ---------------------------------------------------------------------------
# LoadStatus
# ---------------------------------------------------------------------------


class LoadStatus(Enum):
    """Status of a node after loading from a serialized pipeline."""

    FULL = "full"
    READ_ONLY = "read_only"
    UNAVAILABLE = "unavailable"


# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------


def _build_database_registry() -> dict[str, type]:
    from orcapod.databases.delta_lake_databases import DeltaTableDatabase
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.databases.noop_database import NoOpArrowDatabase

    return {
        "delta_table": DeltaTableDatabase,
        "in_memory": InMemoryArrowDatabase,
        "noop": NoOpArrowDatabase,
    }


def _build_source_registry() -> dict[str, type]:
    from orcapod.core.sources.arrow_table_source import ArrowTableSource
    from orcapod.core.sources.cached_source import CachedSource
    from orcapod.core.sources.csv_source import CSVSource
    from orcapod.core.sources.data_frame_source import DataFrameSource
    from orcapod.core.sources.delta_table_source import DeltaTableSource
    from orcapod.core.sources.dict_source import DictSource
    from orcapod.core.sources.list_source import ListSource

    return {
        "csv": CSVSource,
        "delta_table": DeltaTableSource,
        "dict": DictSource,
        "list": ListSource,
        "data_frame": DataFrameSource,
        "arrow_table": ArrowTableSource,
        "cached": CachedSource,
    }


def _build_operator_registry() -> dict[str, type]:
    from orcapod.core.operators import (
        Batch,
        DropPacketColumns,
        DropTagColumns,
        Join,
        MapPackets,
        MapTags,
        MergeJoin,
        PolarsFilter,
        SelectPacketColumns,
        SelectTagColumns,
        SemiJoin,
    )

    return {
        "Join": Join,
        "MergeJoin": MergeJoin,
        "SemiJoin": SemiJoin,
        "Batch": Batch,
        "SelectTagColumns": SelectTagColumns,
        "DropTagColumns": DropTagColumns,
        "SelectPacketColumns": SelectPacketColumns,
        "DropPacketColumns": DropPacketColumns,
        "MapTags": MapTags,
        "MapPackets": MapPackets,
        "PolarsFilter": PolarsFilter,
    }


def _build_packet_function_registry() -> dict[str, type]:
    from orcapod.core.packet_function import PythonPacketFunction

    return {
        "python.function.v0": PythonPacketFunction,
    }


# Lazy-initialized registries (populated on first access)
DATABASE_REGISTRY: dict[str, type] = {}
SOURCE_REGISTRY: dict[str, type] = {}
OPERATOR_REGISTRY: dict[str, type] = {}
PACKET_FUNCTION_REGISTRY: dict[str, type] = {}


def _ensure_registries() -> None:
    """Populate registries on first use."""
    if not DATABASE_REGISTRY:
        DATABASE_REGISTRY.update(_build_database_registry())
    if not SOURCE_REGISTRY:
        SOURCE_REGISTRY.update(_build_source_registry())
    if not OPERATOR_REGISTRY:
        OPERATOR_REGISTRY.update(_build_operator_registry())
    if not PACKET_FUNCTION_REGISTRY:
        PACKET_FUNCTION_REGISTRY.update(_build_packet_function_registry())


# ---------------------------------------------------------------------------
# Resolver helpers
# ---------------------------------------------------------------------------


def resolve_database_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct a database instance from a config dict."""
    _ensure_registries()
    db_type = config.get("type")
    if db_type not in DATABASE_REGISTRY:
        raise ValueError(
            f"Unknown database type: {db_type!r}. "
            f"Known types: {sorted(DATABASE_REGISTRY.keys())}"
        )
    if db_type == "in_memory":
        logger.warning(
            "Loading pipeline with in-memory database. Cached data from the "
            "original run is not available — nodes will have UNAVAILABLE status."
        )
    cls = DATABASE_REGISTRY[db_type]
    return cls.from_config(config)


def resolve_operator_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct an operator instance from a config dict."""
    _ensure_registries()
    class_name = config.get("class_name")
    if class_name not in OPERATOR_REGISTRY:
        raise ValueError(
            f"Unknown operator: {class_name!r}. "
            f"Known operators: {sorted(OPERATOR_REGISTRY.keys())}"
        )
    cls = OPERATOR_REGISTRY[class_name]
    return cls.from_config(config)


def resolve_packet_function_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct a packet function from a config dict."""
    _ensure_registries()
    type_id = config.get("packet_function_type_id")
    if type_id not in PACKET_FUNCTION_REGISTRY:
        raise ValueError(
            f"Unknown packet function type: {type_id!r}. "
            f"Known types: {sorted(PACKET_FUNCTION_REGISTRY.keys())}"
        )
    cls = PACKET_FUNCTION_REGISTRY[type_id]
    return cls.from_config(config)


def resolve_source_from_config(config: dict[str, Any]) -> Any:
    """Reconstruct a source instance from a config dict."""
    _ensure_registries()
    source_type = config.get("source_type")
    if source_type not in SOURCE_REGISTRY:
        raise ValueError(
            f"Unknown source type: {source_type!r}. "
            f"Known types: {sorted(SOURCE_REGISTRY.keys())}"
        )
    cls = SOURCE_REGISTRY[source_type]
    return cls.from_config(config)


# ---------------------------------------------------------------------------
# Registration helpers (extensibility)
# ---------------------------------------------------------------------------


def register_database(type_key: str, cls: type) -> None:
    """Register a custom database implementation for deserialization."""
    _ensure_registries()
    DATABASE_REGISTRY[type_key] = cls


def register_source(type_key: str, cls: type) -> None:
    """Register a custom source implementation for deserialization."""
    _ensure_registries()
    SOURCE_REGISTRY[type_key] = cls


def register_operator(class_name: str, cls: type) -> None:
    """Register a custom operator implementation for deserialization."""
    _ensure_registries()
    OPERATOR_REGISTRY[class_name] = cls


def register_packet_function(type_id: str, cls: type) -> None:
    """Register a custom packet function implementation for deserialization."""
    _ensure_registries()
    PACKET_FUNCTION_REGISTRY[type_id] = cls


# ---------------------------------------------------------------------------
# Schema serialization helpers
# ---------------------------------------------------------------------------


def serialize_schema(schema: Any) -> dict[str, str]:
    """Convert a Schema mapping to JSON-serializable Arrow type strings."""
    return {k: str(v) for k, v in schema.items()}


def deserialize_schema(schema_dict: dict[str, str]) -> dict[str, Any]:
    """Convert Arrow type string dict back to a Schema-compatible mapping.

    Uses pyarrow type parsing to convert strings like 'int64', 'large_string'
    back to Python type representations compatible with Schema.
    """
    from orcapod import contexts

    ctx = contexts.resolve_context(None)
    result = {}
    for k, v in schema_dict.items():
        result[k] = ctx.type_converter.arrow_type_string_to_python_type(v)
    return result
```

Note: The `deserialize_schema` helper uses `DataContext.type_converter` to convert Arrow type
strings back to Python types. Check that `arrow_type_string_to_python_type()` exists on the
type converter — if not, you may need to add it or use `pyarrow` parsing directly (e.g.,
`pa.field("_", pa.type_for_alias(v)).type`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_serialization_helpers.py -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(pipeline): add serialization module with registries, LoadStatus, and resolver helpers"
```

---

### Task 7: Add `from_descriptor()` to `SourceNode`

**Files:**
- Modify: `src/orcapod/core/nodes/source_node.py`
- Test: `tests/test_pipeline/test_node_descriptors.py`

- [ ] **Step 1: Write failing tests for SourceNode.from_descriptor()**

Start `tests/test_pipeline/test_node_descriptors.py`:

```python
"""Tests for node from_descriptor() classmethods."""
import pytest

from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import LoadStatus


class TestSourceNodeFromDescriptor:
    def _make_source_and_descriptor(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            tag_columns=["a"],
            source_id="test",
        )
        node = SourceNode(stream=source, label="my_source")
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "source",
            "label": "my_source",
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
            "stream_type": "dict",
            "source_id": "test",
            "reconstructable": False,
        }
        return source, node, descriptor

    def test_from_descriptor_with_stream(self):
        source, original, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=source,
            databases={},
        )
        assert loaded.load_status == LoadStatus.FULL
        assert loaded.label == "my_source"

    def test_from_descriptor_without_stream_read_only(self):
        _, original, descriptor = self._make_source_and_descriptor()
        db = InMemoryArrowDatabase()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={"pipeline": db},
        )
        assert loaded.load_status in (LoadStatus.READ_ONLY, LoadStatus.UNAVAILABLE)

    def test_from_descriptor_output_schema_from_metadata(self):
        _, original, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={},
        )
        tag_schema, packet_schema = loaded.output_schema()
        assert set(tag_schema.keys()) == set(descriptor["output_schema"]["tag"].keys())
        assert set(packet_schema.keys()) == set(descriptor["output_schema"]["packet"].keys())
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py::TestSourceNodeFromDescriptor -v`
Expected: FAIL

- [ ] **Step 3: Implement `from_descriptor()` on `SourceNode`**

In `src/orcapod/core/nodes/source_node.py`, modify the class to support read-only mode.

**Approach:** Add a `_descriptor` and `_load_status` attribute to `SourceNode`. Use a
`_read_only` sentinel flag pattern rather than `__new__` (which is fragile). The
`from_descriptor()` classmethod constructs a normal `SourceNode` when a stream is available,
or sets up read-only state when it's not.

```python
@classmethod
def from_descriptor(
    cls,
    descriptor: dict[str, Any],
    stream: cp.StreamProtocol | None,
    databases: dict[str, Any],
) -> "SourceNode":
    """Construct a SourceNode from a serialized descriptor.

    Args:
        descriptor: Node descriptor dict from serialized JSON.
        stream: Reconstructed stream, or None for read-only mode.
        databases: Dict of database instances (e.g., {"pipeline": db}).
    """
    from orcapod.pipeline.serialization import LoadStatus

    if stream is not None:
        node = cls(stream=stream, label=descriptor.get("label"))
        node._descriptor = descriptor
        node._load_status = LoadStatus.FULL
        return node

    # Read-only mode: no stream available
    node = cls.__new__(cls)
    # Manually initialize the minimum required state from StreamBase/TraceableBase.
    # Read StreamBase.__init__ and TraceableBase.__init__ to identify all fields
    # that need initialization. At minimum: _label, _config, _last_modified, _hasher_cache.
    node._label = descriptor.get("label")
    node._descriptor = descriptor
    node._load_status = LoadStatus.UNAVAILABLE
    node.stream = None
    node._stored_schema = descriptor.get("output_schema", {})
    node._stored_content_hash = descriptor.get("content_hash")
    node._stored_pipeline_hash = descriptor.get("pipeline_hash")
    return node
```

Also add a `load_status` property and override methods for read-only mode:

```python
@property
def load_status(self) -> "LoadStatus":
    from orcapod.pipeline.serialization import LoadStatus
    return getattr(self, "_load_status", LoadStatus.FULL)
```

Override `output_schema()`, `content_hash()`, `pipeline_hash()` to check `self.stream is None`
and return stored values. Override `iter_packets()` / `as_table()` to raise a clear error.

**Important:** Read `StreamBase.__init__` and `TraceableBase.__init__` carefully to identify
ALL fields that need manual initialization in the `__new__` path. Missing fields will cause
`AttributeError` at runtime. Consider adding a `_init_base_fields()` helper on `StreamBase`
that initializes just the bookkeeping fields without requiring a stream.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py::TestSourceNodeFromDescriptor -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(source-node): add from_descriptor classmethod for read-only loading"
```

---

### Task 8: Add `from_descriptor()` to `PersistentFunctionNode`

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Test: `tests/test_pipeline/test_node_descriptors.py` (append)

- [ ] **Step 1: Write failing tests**

Append to `tests/test_pipeline/test_node_descriptors.py`:

```python
from orcapod.core.nodes.function_node import PersistentFunctionNode
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction


def _sample_func(a: int, b: int) -> dict:
    return {"result": a + b}


class TestPersistentFunctionNodeFromDescriptor:
    def _make_function_node_descriptor(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}],
            tag_columns=["a"],
            source_id="test",
        )
        pf = PythonPacketFunction(
            function=_sample_func, output_keys=["result"]
        )
        pod = FunctionPod(packet_function=pf)
        db = InMemoryArrowDatabase()
        from orcapod.core.nodes import PersistentFunctionNode as PFN

        node = PFN(
            function_pod=pod,
            input_stream=source,
            pipeline_database=db,
            pipeline_path_prefix=("test_pipeline",),
        )
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "function",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
            "function_pod": pod.to_config(),
            "pipeline_path": list(node.pipeline_path),
            "result_record_path": list(node._packet_function.record_path),
            "execution_engine_opts": None,
        }
        return node, descriptor, db

    def test_from_descriptor_full_mode(self):
        original, descriptor, db = self._make_function_node_descriptor()
        source = DictSource(
            data=[{"a": 1, "b": 2}],
            tag_columns=["a"],
            source_id="test",
        )
        pf = PythonPacketFunction(
            function=_sample_func, output_keys=["result"]
        )
        pod = FunctionPod(packet_function=pf)
        loaded = PersistentFunctionNode.from_descriptor(
            descriptor=descriptor,
            function_pod=pod,
            input_stream=source,
            databases={"pipeline": db, "result": db},
        )
        assert loaded.load_status == LoadStatus.FULL

    def test_from_descriptor_read_only(self):
        original, descriptor, db = self._make_function_node_descriptor()
        loaded = PersistentFunctionNode.from_descriptor(
            descriptor=descriptor,
            function_pod=None,
            input_stream=None,
            databases={"pipeline": db, "result": db},
        )
        assert loaded.load_status in (LoadStatus.READ_ONLY, LoadStatus.UNAVAILABLE)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py::TestPersistentFunctionNodeFromDescriptor -v`
Expected: FAIL

- [ ] **Step 3: Implement `from_descriptor()` on `PersistentFunctionNode`**

In `src/orcapod/core/nodes/function_node.py`, add a `from_descriptor()` classmethod to `PersistentFunctionNode`. When `function_pod` is None:
- Store descriptor metadata
- `output_schema()` returns from stored schema
- `content_hash()` / `pipeline_hash()` return stored values
- `iter_packets()` / `as_table()` attempt to read from cache via `pipeline_path` and `result_record_path`
- `process_packet()` raises explaining function is unavailable
- `run()` is a no-op

When `function_pod` is provided:
- Construct normally, but also store the descriptor for validation
- `load_status` returns `FULL`

Read `function_node.py` carefully before implementing — the constructor does validation that needs to be skipped in read-only mode.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py::TestPersistentFunctionNodeFromDescriptor -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(function-node): add from_descriptor classmethod for read-only loading"
```

---

### Task 9: Add `from_descriptor()` to `PersistentOperatorNode`

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Test: `tests/test_pipeline/test_node_descriptors.py` (append)

- [ ] **Step 1: Write failing tests**

Append to `tests/test_pipeline/test_node_descriptors.py`:

```python
from orcapod.core.nodes.operator_node import PersistentOperatorNode
from orcapod.core.operators import Join


class TestPersistentOperatorNodeFromDescriptor:
    def test_from_descriptor_read_only(self):
        db = InMemoryArrowDatabase()
        descriptor = {
            "node_type": "operator",
            "label": "my_join",
            "content_hash": "fake_hash",
            "pipeline_hash": "fake_pipeline_hash",
            "data_context_key": "std:v0.1:default",
            "output_schema": {
                "tag": {"a": "int64"},
                "packet": {"b": "int64", "c": "int64"},
            },
            "operator": {
                "class_name": "Join",
                "module_path": "orcapod.core.operators.join",
                "config": {},
            },
            "cache_mode": "OFF",
            "pipeline_path": ["test", "Join", "hash", "node:abc"],
        }
        loaded = PersistentOperatorNode.from_descriptor(
            descriptor=descriptor,
            operator=None,
            input_streams=(),
            databases={"pipeline": db},
        )
        assert loaded.load_status in (LoadStatus.READ_ONLY, LoadStatus.UNAVAILABLE)
        assert loaded.label == "my_join"

    def test_from_descriptor_full_mode(self):
        db = InMemoryArrowDatabase()
        source1 = DictSource(
            data=[{"a": 1, "b": 2}], tag_columns=["a"], source_id="s1"
        )
        source2 = DictSource(
            data=[{"a": 1, "c": 3}], tag_columns=["a"], source_id="s2"
        )
        op = Join()
        node = PersistentOperatorNode(
            operator=op,
            input_streams=(source1, source2),
            pipeline_database=db,
            pipeline_path_prefix=("test",),
        )
        descriptor = {
            "node_type": "operator",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "output_schema": {
                "tag": {"a": "int64"},
                "packet": {"b": "int64", "c": "int64"},
            },
            "operator": op.to_config(),
            "cache_mode": "OFF",
            "pipeline_path": list(node.pipeline_path),
        }
        loaded = PersistentOperatorNode.from_descriptor(
            descriptor=descriptor,
            operator=op,
            input_streams=(source1, source2),
            databases={"pipeline": db},
        )
        assert loaded.load_status == LoadStatus.FULL
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py::TestPersistentOperatorNodeFromDescriptor -v`
Expected: FAIL

- [ ] **Step 3: Implement `from_descriptor()` on `PersistentOperatorNode`**

Same pattern as FunctionNode — read-only when operator is None, full when provided. Read `operator_node.py` to understand the constructor validation that must be skipped.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py::TestPersistentOperatorNodeFromDescriptor -v`
Expected: All PASS

- [ ] **Step 5: Run all node descriptor tests**

Run: `uv run pytest tests/test_pipeline/test_node_descriptors.py -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(operator-node): add from_descriptor classmethod for read-only loading"
```

---

## Chunk 3: Pipeline.save() and Pipeline.load()

### Task 10: Implement `Pipeline.save()`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`
- Test: `tests/test_pipeline/test_serialization.py`

- [ ] **Step 1: Write failing tests for Pipeline.save()**

Create `tests/test_pipeline/test_serialization.py`:

```python
"""End-to-end tests for Pipeline.save() and Pipeline.load()."""
import json
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join, Batch
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.serialization import PIPELINE_FORMAT_VERSION


def transform_func(a: int, b: int) -> dict:
    return {"result": a + b}


@pytest.fixture
def simple_pipeline(tmp_path):
    """Create a simple pipeline: source -> function_pod."""
    db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))
    source = DictSource(
        data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
        tag_columns=["x"],
        source_id="test_source",
    )
    pf = PythonPacketFunction(
        function=transform_func,
        output_keys=["result"],
        function_name="transform_func",
    )
    pod = FunctionPod(packet_function=pf)
    pipeline = Pipeline(name="test", pipeline_database=db)
    with pipeline:
        result = pod.process(source, label="transform")
    return pipeline, tmp_path


class TestPipelineSave:
    def test_save_creates_json_file(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        assert path.exists()

    def test_save_valid_json(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert "orcapod_pipeline_version" in data
        assert "pipeline" in data
        assert "nodes" in data
        assert "edges" in data

    def test_save_version(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_version"] == PIPELINE_FORMAT_VERSION

    def test_save_pipeline_metadata(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["pipeline"]["name"] == ["test"]
        assert data["pipeline"]["databases"]["pipeline_database"]["type"] == "delta_table"

    def test_save_includes_nodes(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        nodes = data["nodes"]
        # Should have source + function nodes
        node_types = {n["node_type"] for n in nodes.values()}
        assert "source" in node_types
        assert "function" in node_types

    def test_save_includes_edges(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert len(data["edges"]) > 0

    def test_save_raises_if_not_compiled(self, tmp_path):
        db = InMemoryArrowDatabase()
        pipeline = Pipeline(name="test", pipeline_database=db, auto_compile=False)
        with pytest.raises(ValueError, match="not compiled"):
            pipeline.save(str(tmp_path / "pipeline.json"))
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineSave -v`
Expected: FAIL — `save()` not defined

- [ ] **Step 3: Implement `Pipeline.save()`**

In `src/orcapod/pipeline/graph.py`, add a `save()` method to `Pipeline`. The method should:

1. Check `self._compiled` — raise `ValueError` if not compiled
2. Build the top-level JSON structure with version, pipeline metadata, and database configs
3. Walk `_persistent_node_map` in topological order to build node descriptors
4. For each node, inspect its type and build the appropriate descriptor dict
5. Serialize edges from `_graph_edges`
6. Write JSON to the given path

Read the file structure section of the spec for exact JSON format. Use the `to_config()` methods implemented in earlier tasks.

Helper: You'll need a function to serialize schemas to Arrow type strings. Add a helper to `serialization.py`:

```python
def serialize_schema(schema: Schema) -> dict[str, str]:
    """Convert a Schema mapping to JSON-serializable Arrow type strings."""
    return {k: str(v) for k, v in schema.items()}
```

Note: Check how `Schema` values are represented — they may be Python types (like `int`, `str`) or Arrow types. Use the `DataContext.type_converter` if needed to get Arrow type strings.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineSave -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(pipeline): implement Pipeline.save() for JSON serialization"
```

---

### Task 11: Implement `Pipeline.load()`

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`
- Test: `tests/test_pipeline/test_serialization.py` (append)

- [ ] **Step 1: Write failing tests for Pipeline.load()**

Append to `tests/test_pipeline/test_serialization.py`:

```python
from orcapod.pipeline.serialization import LoadStatus


class TestPipelineLoad:
    def test_load_full_mode(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")
        assert loaded.name == pipeline.name
        assert len(loaded.compiled_nodes) == len(pipeline.compiled_nodes)

    def test_load_read_only_mode(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")
        assert loaded.name == pipeline.name
        # Function nodes should be read-only
        for name, node in loaded.compiled_nodes.items():
            if hasattr(node, "load_status"):
                if node.node_type == "function":
                    assert node.load_status == LoadStatus.READ_ONLY

    def test_load_version_mismatch_raises(self, simple_pipeline, tmp_path):
        pipeline, _ = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        data["orcapod_pipeline_version"] = "99.0.0"
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="version"):
            Pipeline.load(str(path))

    def test_load_preserves_node_labels(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")
        original_labels = set(pipeline.compiled_nodes.keys())
        loaded_labels = set(loaded.compiled_nodes.keys())
        assert original_labels == loaded_labels

    def test_load_preserves_graph_structure(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")
        assert len(loaded._graph_edges) == len(pipeline._graph_edges)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineLoad -v`
Expected: FAIL — `load()` not defined

- [ ] **Step 3: Implement `Pipeline.load()`**

In `src/orcapod/pipeline/graph.py`, add a `@classmethod` `load()` method to `Pipeline`:

```python
@classmethod
def load(cls, path: str | Path, mode: str = "full") -> "Pipeline":
    ...
```

The method should:
1. Read and parse JSON
2. Validate `orcapod_pipeline_version` against `SUPPORTED_FORMAT_VERSIONS`
3. Reconstruct databases via `resolve_database_from_config()`
4. Derive topological order from edges
5. Walk nodes in order, reconstructing each based on type and mode
6. Wire upstream references
7. Build `_node_graph` and `_nodes` dict
8. Return the Pipeline in compiled state

This is the most complex method. Follow the spec's "Pipeline.load() Flow" section closely. Use the resolver helpers from `serialization.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_pipeline/test_serialization.py::TestPipelineLoad -v`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add -A && git commit -m "feat(pipeline): implement Pipeline.load() with full and read-only modes"
```

---

### Task 12: End-to-end integration tests

**Files:**
- Test: `tests/test_pipeline/test_serialization.py` (append)

- [ ] **Step 1: Write integration tests**

Append to `tests/test_pipeline/test_serialization.py`:

```python
class TestPipelineSaveLoadIntegration:
    def test_save_load_run_full_cycle(self, tmp_path):
        """Save a pipeline, load in full mode, and re-run it."""
        db_path = str(tmp_path / "db")
        db = DeltaTableDatabase(base_path=db_path)
        source = DictSource(
            data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            tag_columns=["x"],
            source_id="test_source",
        )
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            result = pod.process(source, label="transform")
        pipeline.run()

        # Save and reload
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        # The loaded pipeline should have the same nodes
        assert set(loaded.compiled_nodes.keys()) == set(pipeline.compiled_nodes.keys())

    def test_read_only_can_access_cached_data(self, tmp_path):
        """Save a pipeline after run, load read-only, access cached results."""
        db_path = str(tmp_path / "db")
        db = DeltaTableDatabase(base_path=db_path)
        source = DictSource(
            data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            tag_columns=["x"],
            source_id="test_source",
        )
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            result = pod.process(source, label="transform")
        pipeline.run()
        db.flush()

        # Save and reload in read-only mode
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        # Function node should be read-only but have cached data accessible
        transform_node = loaded.compiled_nodes.get("transform")
        assert transform_node is not None
        assert transform_node.load_status == LoadStatus.READ_ONLY

    def test_pipeline_with_operator(self, tmp_path):
        """Save/load a pipeline with an operator node."""
        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source1 = DictSource(
            data=[{"a": 1, "b": 10}], tag_columns=["a"], source_id="s1"
        )
        source2 = DictSource(
            data=[{"a": 1, "c": 20}], tag_columns=["a"], source_id="s2"
        )
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            joined = Join().process(source1, source2, label="my_join")
        pipeline.run()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        assert "my_join" in loaded.compiled_nodes
```

- [ ] **Step 2: Run all serialization tests**

Run: `uv run pytest tests/test_pipeline/test_serialization.py -v`
Expected: All PASS

- [ ] **Step 3: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All PASS (no regressions)

- [ ] **Step 4: Commit**

```bash
git add -A && git commit -m "test(pipeline): add end-to-end save/load integration tests"
```

---

### Task 13: Update pipeline `__init__.py` exports and final cleanup

**Files:**
- Modify: `src/orcapod/pipeline/__init__.py`

- [ ] **Step 1: Update exports**

Add `LoadStatus` and `PIPELINE_FORMAT_VERSION` to the pipeline package exports:

```python
from .graph import Pipeline
from .orchestrator import AsyncPipelineOrchestrator
from .serialization import LoadStatus, PIPELINE_FORMAT_VERSION

__all__ = [
    "AsyncPipelineOrchestrator",
    "LoadStatus",
    "PIPELINE_FORMAT_VERSION",
    "Pipeline",
]
```

- [ ] **Step 2: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All PASS

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "chore(pipeline): export LoadStatus and PIPELINE_FORMAT_VERSION from pipeline package"
```
