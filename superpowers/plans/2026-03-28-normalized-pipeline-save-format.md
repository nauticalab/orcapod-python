# Normalized Pipeline Save Format Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the normalized pipeline save format with database deduplication, four save levels, and fix the `pipeline_path` two-level formula bug (ENG-342 + ENG-320).

**Architecture:** Add a `DatabaseRegistry` class to `serialization.py` that deduplicates database configs into a top-level registry keyed by SHA-256 hash. Extend `Pipeline.save(level=)` to support four save levels (`minimal`, `definition`, `standard`, `full`). Remove identity fields from `source_config` (they now live in the node descriptor). Fix `pipeline_path` formula in both node types. Update `Pipeline.load()` to handle the new format and re-derive `pipeline_path` from stored identity fields instead of reading it.

**Tech Stack:** Python 3.12+, `uv run pytest` for tests, JSON serialization via `json`, SHA-256 via `hashlib`. No new dependencies.

**Spec:** `superpowers/specs/2026-03-28-pipeline-save-format-design.md`

---

## File Map

| File | Change |
|---|---|
| `src/orcapod/pipeline/serialization.py` | Add `DatabaseRegistry` class; update `_source_proxy_from_config` signature |
| `src/orcapod/pipeline/graph.py` | Rewrite `save()` with `level=` param; rewrite `load()` for new format |
| `src/orcapod/core/sources/base.py` | Remove `_identity_config()` from `to_config()` (move to node descriptor only) |
| `src/orcapod/core/sources/cached_source.py` | Add `db_registry` param to `to_config()`/`from_config()` |
| `src/orcapod/core/nodes/function_node.py` | Fix `pipeline_path` formula; add `node_uri` property |
| `src/orcapod/core/nodes/operator_node.py` | Fix `pipeline_path` formula; add `node_uri` property |
| `tests/test_pipeline/test_serialization.py` | Update existing tests; add tests for new format and all levels |

---

## Task 1: Fix `pipeline_path` Two-Level Formula (ENG-342)

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py:457-467`
- Modify: `src/orcapod/core/nodes/operator_node.py:362-380`
- Test: `tests/test_pipeline/test_serialization.py`

**Context:** `FunctionNode._pipeline_node_hash` is set to `self.pipeline_hash().to_string()` (line 196) — it uses only the schema hash. `OperatorNode._pipeline_node_hash` is set to `self.content_hash().to_string()` (line 136) — it uses only the instance hash. Both should use the two-level formula: `schema:{pipeline_hash}` + `instance:{content_hash}`.

- [ ] **Step 1: Write failing test for FunctionNode pipeline_path format**

```python
# In tests/test_pipeline/test_serialization.py
def test_function_node_pipeline_path_two_level(tmp_path):
    """pipeline_path must end with schema:... and instance:... components."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1, 2, 3]}, source_id="test_src")
    pipeline = op.Pipeline("test", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    fn_node = pipeline._nodes["fn"]
    path = fn_node.pipeline_path

    # Must have exactly two hash components at the end
    assert path[-2].startswith("schema:"), f"Expected schema:... got {path[-2]!r}"
    assert path[-1].startswith("instance:"), f"Expected instance:... got {path[-1]!r}"
    # schema hash uses pipeline_hash, instance uses content_hash
    assert fn_node.pipeline_hash().to_string() in path[-2]
    assert fn_node.content_hash().to_string() in path[-1]
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_pipeline/test_serialization.py::test_function_node_pipeline_path_two_level -v
```

Expected: FAIL — `schema:` assertion fails (currently only one `node:` component).

- [ ] **Step 3: Fix FunctionNode.pipeline_path**

In `src/orcapod/core/nodes/function_node.py`, find the `pipeline_path` property (around line 457):

```python
# BEFORE (lines ~464-467):
return (
    self._pipeline_path_prefix
    + self._packet_function.uri
    + (f"node:{self._pipeline_node_hash}",)
)

# AFTER:
return (
    self._pipeline_path_prefix
    + self._packet_function.uri
    + (
        f"schema:{self.pipeline_hash().to_string()}",
        f"instance:{self.content_hash().to_string()}",
    )
)
```

- [ ] **Step 4: Fix OperatorNode.pipeline_path**

In `src/orcapod/core/nodes/operator_node.py`, find the `pipeline_path` property (around line 362):

```python
# BEFORE (lines ~373-376):
return (
    self._pipeline_path_prefix
    + self._operator.uri
    + (f"node:{self._pipeline_node_hash}",)
)

# AFTER:
return (
    self._pipeline_path_prefix
    + self._operator.uri
    + (
        f"schema:{self.pipeline_hash().to_string()}",
        f"instance:{self.content_hash().to_string()}",
    )
)
```

- [ ] **Step 5: Add the same test for OperatorNode**

```python
def test_operator_node_pipeline_path_two_level(tmp_path):
    """OperatorNode pipeline_path must also use two-level schema/instance formula."""
    import orcapod as op
    from orcapod.core.operators import Join
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()
    src_a = op.DictSource({"key": ["a"]}, source_id="src_a")
    src_b = op.DictSource({"key": ["a"], "val": [1]}, source_id="src_b")

    pipeline = op.Pipeline("test", pipeline_database=db)
    pipeline.add(src_a, label="a")
    pipeline.add(src_b, label="b")
    pipeline.add(Join(), label="joined", inputs=["a", "b"])
    pipeline.compile()

    op_node = pipeline._nodes["joined"]
    path = op_node.pipeline_path

    assert path[-2].startswith("schema:"), f"Expected schema:... got {path[-2]!r}"
    assert path[-1].startswith("instance:"), f"Expected instance:... got {path[-1]!r}"
    assert op_node.pipeline_hash().to_string() in path[-2]
    assert op_node.content_hash().to_string() in path[-1]
```

- [ ] **Step 6: Run both tests to confirm they pass**

```bash
uv run pytest tests/test_pipeline/test_serialization.py::test_function_node_pipeline_path_two_level tests/test_pipeline/test_serialization.py::test_operator_node_pipeline_path_two_level -v
```

Expected: PASS.

- [ ] **Step 7: Run full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: All passing. If any test checks the old `node:` format in `pipeline_path`, update it to expect the two-level format.

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py src/orcapod/core/nodes/operator_node.py tests/test_pipeline/test_serialization.py
git commit -m "fix(nodes): use two-level schema/instance formula for pipeline_path (ENG-342)

FunctionNode was using only pipeline_hash; OperatorNode was using only
content_hash. Both now emit (schema:{pipeline_hash}, instance:{content_hash})
to enable hierarchical result storage grouping."
```

---

## Task 2: Add `DatabaseRegistry` to `serialization.py`

**Files:**
- Modify: `src/orcapod/pipeline/serialization.py` — add class after `LoadStatus`
- Test: `tests/test_pipeline/test_serialization_helpers.py`

- [ ] **Step 1: Write failing tests for DatabaseRegistry**

```python
# In tests/test_pipeline/test_serialization_helpers.py (or new file)
import pytest
from orcapod.pipeline.serialization import DatabaseRegistry


def test_database_registry_register_returns_key():
    reg = DatabaseRegistry()
    config = {"type": "delta_table", "base_path": "/tmp/db"}
    key = reg.register(config)
    assert key.startswith("db_")
    assert len(key) == len("db_") + 8  # db_ + 8 hex chars


def test_database_registry_same_config_same_key():
    reg = DatabaseRegistry()
    config = {"type": "delta_table", "base_path": "/tmp/db"}
    key1 = reg.register(config)
    key2 = reg.register(config)
    assert key1 == key2


def test_database_registry_different_config_different_key():
    reg = DatabaseRegistry()
    key1 = reg.register({"type": "delta_table", "base_path": "/tmp/a"})
    key2 = reg.register({"type": "delta_table", "base_path": "/tmp/b"})
    assert key1 != key2


def test_database_registry_collision_handling():
    """Simulate a collision: same 8-char prefix, different configs."""
    reg = DatabaseRegistry()
    # Inject two different configs that happen to produce the same key prefix
    reg._entries["db_aabbccdd"] = {"type": "delta_table", "base_path": "/a"}
    # Force-register a different config that would produce the same key
    # by manually patching _make_key:
    original_make_key = DatabaseRegistry._make_key
    DatabaseRegistry._make_key = staticmethod(lambda config: "db_aabbccdd")
    try:
        key2 = reg.register({"type": "delta_table", "base_path": "/b"})
        assert key2 == "db_aabbccdd_2"
    finally:
        DatabaseRegistry._make_key = staticmethod(original_make_key)


def test_database_registry_to_dict():
    reg = DatabaseRegistry()
    config = {"type": "in_memory"}
    key = reg.register(config)
    d = reg.to_dict()
    assert key in d
    assert d[key] == config


def test_database_registry_from_dict():
    data = {"db_abc12345": {"type": "delta_table", "base_path": "/x"}}
    reg = DatabaseRegistry.from_dict(data)
    assert reg.resolve("db_abc12345") == {"type": "delta_table", "base_path": "/x"}


def test_database_registry_resolve_unknown_raises():
    reg = DatabaseRegistry()
    with pytest.raises(KeyError):
        reg.resolve("db_nonexistent")
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
uv run pytest tests/test_pipeline/test_serialization_helpers.py -v -k "DatabaseRegistry"
```

Expected: FAIL — `DatabaseRegistry` not found.

- [ ] **Step 3: Implement DatabaseRegistry in serialization.py**

Add this class after the `LoadStatus` enum (around line 30):

```python
import hashlib
import json as _json

class DatabaseRegistry:
    """Deduplicates database configs into a keyed registry at save time.

    Keys are deterministic: same config → same key, cross-session stable.
    Format: ``db_{sha256[:8]}`` with ``_2``, ``_3``... suffixes on collision.
    """

    def __init__(self) -> None:
        self._entries: dict[str, dict] = {}

    def register(self, config: dict) -> str:
        """Register a database config and return its registry key.

        If the config is already registered, returns the existing key.
        On hash collision (different configs with same 8-char prefix),
        appends ``_2``, ``_3``... to the key.
        """
        base_key = self._make_key(config)
        # Check if already registered with this exact config
        if base_key in self._entries and self._entries[base_key] == config:
            return base_key
        # Collision: find next available suffix
        if base_key in self._entries:
            i = 2
            while f"{base_key}_{i}" in self._entries:
                i += 1
            key = f"{base_key}_{i}"
        else:
            key = base_key
        self._entries[key] = config
        return key

    def resolve(self, key: str) -> dict:
        """Return the config dict for a registry key."""
        return self._entries[key]

    def to_dict(self) -> dict[str, dict]:
        """Return the full registry for JSON serialization."""
        return dict(self._entries)

    @classmethod
    def from_dict(cls, data: dict[str, dict]) -> "DatabaseRegistry":
        """Reconstruct a registry from the saved ``databases`` block."""
        registry = cls()
        registry._entries = dict(data)
        return registry

    @staticmethod
    def _make_key(config: dict) -> str:
        canonical = _json.dumps(config, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()[:8]
        return f"db_{digest}"
```

Also add `DatabaseRegistry` to the module's `__all__` or ensure it's importable.

- [ ] **Step 4: Run tests to confirm they pass**

```bash
uv run pytest tests/test_pipeline/test_serialization_helpers.py -v -k "DatabaseRegistry"
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/serialization.py tests/test_pipeline/test_serialization_helpers.py
git commit -m "feat(serialization): add DatabaseRegistry for db config deduplication (ENG-320)"
```

---

## Task 3: Add `db_registry` Parameter to `CachedSource` Serialization

**Files:**
- Modify: `src/orcapod/core/sources/cached_source.py:83-120`
- Test: `tests/test_pipeline/test_serialization_helpers.py`

**Context:** `CachedSource` is the only source that currently embeds a database config inline (`self._cache_database.to_config()`). We add an optional `db_registry` parameter: when provided, the DB config is registered and replaced by its key string.

- [ ] **Step 1: Write failing tests**

```python
# In tests/test_pipeline/test_serialization_helpers.py
from orcapod.core.sources.cached_source import CachedSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import DatabaseRegistry


def test_cached_source_to_config_without_registry_embeds_db():
    """Default behavior: database config embedded inline."""
    db = InMemoryArrowDatabase()
    inner = DictSource({"x": [1, 2]}, source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    config = src.to_config()
    # DB config is inline dict, not a string key
    assert isinstance(config["cache_database"], dict)


def test_cached_source_to_config_with_registry_emits_key():
    """With registry: cache_database field is a string key."""
    db = InMemoryArrowDatabase()
    inner = DictSource({"x": [1, 2]}, source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    registry = DatabaseRegistry()
    config = src.to_config(db_registry=registry)
    assert isinstance(config["cache_database"], str)
    assert config["cache_database"].startswith("db_")
    # Key must exist in registry
    assert config["cache_database"] in registry.to_dict()


def test_cached_source_from_config_with_registry_resolves_key():
    """from_config with registry resolves key back to database instance."""
    db = InMemoryArrowDatabase()
    inner = DictSource({"x": [1, 2]}, source_id="s")
    src = CachedSource(source=inner, cache_database=db, cache_path_prefix=("cache",))
    registry = DatabaseRegistry()
    config = src.to_config(db_registry=registry)

    # Round-trip via from_config with registry
    src2 = CachedSource.from_config(config, db_registry=registry)
    assert src2._cache_database is not None
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_serialization_helpers.py -v -k "cached_source"
```

Expected: FAIL — `to_config` doesn't accept `db_registry`.

- [ ] **Step 3: Update CachedSource.to_config**

In `src/orcapod/core/sources/cached_source.py`:

```python
def to_config(self, db_registry=None) -> dict[str, Any]:
    """Serialize to config. If db_registry provided, register DB and emit key."""
    db_config = self._cache_database.to_config()
    if db_registry is not None:
        cache_db_ref = db_registry.register(db_config)
    else:
        cache_db_ref = db_config

    return {
        "source_type": "cached",
        "inner_source": self._source.to_config(),
        "cache_database": cache_db_ref,
        "cache_path_prefix": list(self._cache_path_prefix),
        "cache_path": list(self.cache_path),
        "source_id": self.source_id,
    }
```

Note: `**self._identity_config()` is intentionally **removed** here — identity fields belong in the node descriptor, not in source_config. This is the Task 4 change, but it's easiest to do both at once.

- [ ] **Step 4: Update CachedSource.from_config**

```python
@classmethod
def from_config(cls, config: dict[str, Any], db_registry=None) -> "CachedSource":
    from orcapod.pipeline.serialization import (
        resolve_database_from_config,
        resolve_source_from_config,
    )

    cache_db_ref = config["cache_database"]
    if isinstance(cache_db_ref, str):
        # Key reference: resolve from registry
        if db_registry is None:
            raise ValueError(
                f"cache_database is a registry key {cache_db_ref!r} but no "
                "db_registry was provided."
            )
        cache_db = resolve_database_from_config(db_registry.resolve(cache_db_ref))
    else:
        # Inline config (old format or definition-level without registry)
        cache_db = resolve_database_from_config(cache_db_ref)

    inner_source = resolve_source_from_config(
        config["inner_source"], fallback_to_proxy=True
    )

    return cls(
        source=inner_source,
        cache_database=cache_db,
        cache_path_prefix=tuple(config.get("cache_path_prefix", ())),
        cache_path=tuple(config["cache_path"]) if "cache_path" in config else None,
        source_id=config.get("source_id"),
    )
```

- [ ] **Step 5: Run tests**

```bash
uv run pytest tests/test_pipeline/test_serialization_helpers.py -v -k "cached_source"
```

Expected: PASS.

- [ ] **Step 6: Run full test suite**

```bash
uv run pytest tests/ -x -q
```

Fix any tests that rely on `_identity_config()` fields being present in `CachedSource.to_config()`.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/sources/cached_source.py tests/test_pipeline/test_serialization_helpers.py
git commit -m "feat(sources): add db_registry param to CachedSource serialization (ENG-320)

- to_config(db_registry=None): registers cache_database in registry if provided
- from_config(config, db_registry=None): resolves key or inline config
- Remove identity fields from source_config (moved to node descriptor level)"
```

---

## Task 4: Update `_source_proxy_from_config` for Separated Identity Fields

**Files:**
- Modify: `src/orcapod/pipeline/serialization.py:294-335`

**Context:** `_source_proxy_from_config` currently reads `content_hash`, `pipeline_hash`, `tag_schema`, `packet_schema` from the source config dict. In the new format these fields are in the node descriptor, not the source config. We update the function to accept them as an optional separate argument.

- [ ] **Step 1: Write failing test**

```python
def test_source_proxy_from_node_descriptor_fields():
    """_source_proxy_from_config reads identity from node_descriptor when provided."""
    from orcapod.pipeline.serialization import _source_proxy_from_config

    source_config = {"source_type": "dict", "source_id": "my_src"}
    node_descriptor = {
        "content_hash": "semantic_v0.1:abc",
        "pipeline_hash": "semantic_v0.1:def",
        "output_schema": {
            "tag": {"x": "int64"},
            "packet": {"result": "int64"},
        },
    }
    proxy = _source_proxy_from_config(source_config, node_descriptor=node_descriptor)
    assert proxy.content_hash().to_string() == "semantic_v0.1:abc"
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_serialization_helpers.py -v -k "proxy_from_node_descriptor"
```

- [ ] **Step 3: Update `_source_proxy_from_config` signature**

In `src/orcapod/pipeline/serialization.py`, update the function (around line 294):

```python
def _source_proxy_from_config(
    config: dict[str, Any],
    node_descriptor: dict[str, Any] | None = None,
) -> Any:
    """Create a SourceProxy from identity fields.

    Identity fields (content_hash, pipeline_hash, tag_schema, packet_schema)
    are read from node_descriptor if provided, falling back to config for
    backward compatibility with old-format saves that embed them in source_config.
    """
    from orcapod.core.sources.source_proxy import SourceProxy
    from orcapod.types import Schema

    # Prefer node_descriptor for identity fields (new format); fall back to config
    identity_source = node_descriptor or config

    # For output_schema in node_descriptor format: {"tag": {...}, "packet": {...}}
    if node_descriptor is not None and "output_schema" in node_descriptor:
        tag_schema_dict = node_descriptor["output_schema"]["tag"]
        packet_schema_dict = node_descriptor["output_schema"]["packet"]
    else:
        tag_schema_dict = identity_source.get("tag_schema", {})
        packet_schema_dict = identity_source.get("packet_schema", {})

    content_hash = identity_source.get("content_hash")
    pipeline_hash_val = identity_source.get("pipeline_hash")

    if not content_hash or not pipeline_hash_val:
        raise ValueError(
            "Cannot create SourceProxy: missing content_hash or pipeline_hash. "
            "Provide node_descriptor with identity fields."
        )

    source_type = config.get("source_type")
    expected_class_name: str | None = None
    if source_type and source_type in SOURCE_REGISTRY:
        expected_class_name = SOURCE_REGISTRY[source_type].__name__

    tag_schema = Schema(deserialize_schema(tag_schema_dict))
    packet_schema = Schema(deserialize_schema(packet_schema_dict))

    return SourceProxy(
        source_id=config.get("source_id", "unknown"),
        content_hash_str=content_hash,
        pipeline_hash_str=pipeline_hash_val,
        tag_schema=tag_schema,
        packet_schema=packet_schema,
        expected_class_name=expected_class_name,
        source_config=config,
    )
```

- [ ] **Step 4: Run tests and fix regressions**

```bash
uv run pytest tests/ -x -q
```

The existing `_source_proxy_from_config` call sites in `resolve_source_from_config` still pass a single `config` arg — this is backward compatible since `node_descriptor=None` preserves old behavior.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/serialization.py tests/test_pipeline/test_serialization_helpers.py
git commit -m "feat(serialization): update _source_proxy_from_config to accept node_descriptor (ENG-320)"
```

---

## Task 5: Add `node_uri` Property to All Node Types

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `src/orcapod/pipeline/graph.py` (`_build_source_descriptor`)

**Context:** The save format requires a `node_uri` field on every node descriptor. For function/operator nodes this already exists as `pod.uri` / `operator.uri`. For source nodes it must be synthesized from the stream config.

- [ ] **Step 1: Write failing tests**

```python
def test_function_node_has_node_uri(simple_pipeline):
    fn_node = simple_pipeline._nodes["fn"]  # adjust label to match fixture
    assert hasattr(fn_node, "node_uri")
    uri = fn_node.node_uri
    assert isinstance(uri, tuple)
    assert len(uri) >= 1


def test_operator_node_has_node_uri(multi_source_pipeline):
    op_node = multi_source_pipeline._nodes["joined"]  # adjust label
    assert hasattr(op_node, "node_uri")
    uri = op_node.node_uri
    assert isinstance(uri, tuple)
    assert len(uri) >= 1
```

- [ ] **Step 2: Add `node_uri` property to FunctionNode**

In `src/orcapod/core/nodes/function_node.py`, add after the `pipeline_path` property:

```python
@property
def node_uri(self) -> tuple[str, ...]:
    """Return the canonical URI tuple identifying this computation.

    Identical to ``pod.uri`` on the underlying packet function at runtime.
    Returns stored value in read-only (deserialized) mode.
    """
    if self._packet_function is None:
        return tuple(getattr(self, "_stored_node_uri", ()))
    return self._packet_function.uri
```

- [ ] **Step 3: Add `node_uri` property to OperatorNode**

In `src/orcapod/core/nodes/operator_node.py`:

```python
@property
def node_uri(self) -> tuple[str, ...]:
    """Return the canonical URI tuple identifying this computation.

    Identical to ``operator.uri`` on the underlying operator at runtime.
    Returns stored value in read-only (deserialized) mode.
    """
    if self._operator is None:
        return tuple(getattr(self, "_stored_node_uri", ()))
    return self._operator.uri
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline/test_serialization_helpers.py -v -k "node_uri"
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py src/orcapod/core/nodes/operator_node.py
git commit -m "feat(nodes): add node_uri property to FunctionNode and OperatorNode (ENG-320)"
```

---

## Task 6: Implement `Pipeline.save(level=)` with New Format

**Files:**
- Modify: `src/orcapod/pipeline/graph.py:536-660`
- Test: `tests/test_pipeline/test_serialization.py`

**Context:** Replace the current monolithic `save()` with a level-aware version. Key structural changes:
- Add top-level `"databases"` registry block (not inside `"pipeline"`)
- Add `"level"` field to output
- Remove `"pipeline_path"` and `"result_record_path"` from node descriptors at all levels
- Add `"node_uri"` to all node descriptors
- Rename `"function_pod"` → `"function_config"`, `"operator"` → `"operator_config"`
- Move `pipeline_database`/`function_database` into `"pipeline"` block (as registry keys at `standard`+)
- Omit `data_context_key` at `minimal`
- Omit node-specific configs at `minimal`
- Omit `cache_mode` at `definition`

- [ ] **Step 1: Write failing tests for the new format**

```python
def test_save_minimal_level_structure(simple_pipeline, tmp_path):
    """minimal level: no databases block, no configs, has node_uri."""
    path = tmp_path / "pipeline.json"
    simple_pipeline.save(str(path), level="minimal")
    with open(path) as f:
        data = json.load(f)

    assert data["level"] == "minimal"
    assert "databases" not in data  # no DBs at minimal
    assert "pipeline_database" not in data.get("pipeline", {})

    for node in data["nodes"].values():
        assert "node_uri" in node
        assert "content_hash" in node
        assert "pipeline_hash" in node
        assert "output_schema" in node
        # No configs at minimal
        assert "function_config" not in node
        assert "source_config" not in node
        assert "operator_config" not in node
        # No pipeline_path ever
        assert "pipeline_path" not in node
        assert "result_record_path" not in node


def test_save_standard_level_has_database_registry(tmp_path):
    """standard level: top-level databases block with pipeline_db as key."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1, 2, 3]}, source_id="src")
    pipeline = op.Pipeline("p", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    path = tmp_path / "pipeline.json"
    pipeline.save(str(path), level="standard")

    with open(path) as f:
        data = json.load(f)

    assert data["level"] == "standard"
    assert "databases" in data  # top-level registry
    pipeline_db_key = data["pipeline"]["pipeline_database"]
    assert isinstance(pipeline_db_key, str)
    assert pipeline_db_key in data["databases"]  # key resolves in registry


def test_save_definition_level_has_node_configs_no_pipeline_db(tmp_path):
    """definition level: function_config present, no pipeline_database key."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1]}, source_id="src")
    pipeline = op.Pipeline("p", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    path = tmp_path / "pipeline.json"
    pipeline.save(str(path), level="definition")

    with open(path) as f:
        data = json.load(f)

    assert data["level"] == "definition"
    # pipeline-level DB keys absent
    assert "pipeline_database" not in data.get("pipeline", {})
    # node configs present
    fn_node = next(v for v in data["nodes"].values() if v["node_type"] == "function")
    assert "function_config" in fn_node
    # cache_mode absent at definition level
    assert "cache_mode" not in fn_node


def test_save_never_stores_pipeline_path(tmp_path):
    """pipeline_path and result_record_path must not appear in any level."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    import json

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1]}, source_id="src")
    pipeline = op.Pipeline("p", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    for level in ("minimal", "definition", "standard"):
        path = tmp_path / f"{level}.json"
        pipeline.save(str(path), level=level)
        raw = path.read_text()
        assert "pipeline_path" not in raw, f"pipeline_path leaked into {level} save"
        assert "result_record_path" not in raw, f"result_record_path leaked into {level} save"
```

- [ ] **Step 2: Run tests to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_serialization.py -v -k "save_minimal or save_standard or save_definition or save_never"
```

Expected: FAIL — `save()` doesn't accept `level` parameter.

- [ ] **Step 3: Rewrite `Pipeline.save()` in `graph.py`**

Replace the `save()` method (lines 536-604) with:

```python
def save(self, path: str, level: str = "standard") -> None:
    """Serialize the pipeline to a JSON file.

    Args:
        path: File path to write JSON output to.
        level: Save detail level. One of:
            - ``"minimal"``: topology + identity only (PLT-1161 schema)
            - ``"definition"``: + full pod/stream configs (no pipeline DBs)
            - ``"standard"`` (default): + pipeline-level DB registry (round-trippable)
            - ``"full"``: same as standard (observer serialization TBD)
    """
    _LEVELS = ("minimal", "definition", "standard", "full")
    if level not in _LEVELS:
        raise ValueError(f"level must be one of {_LEVELS}, got {level!r}")

    if not self._compiled:
        raise ValueError(
            "Pipeline is not compiled. Call compile() or use "
            "auto_compile=True before saving."
        )

    from orcapod.core.nodes import OperatorNode
    from orcapod.pipeline.serialization import (
        PIPELINE_FORMAT_VERSION,
        DatabaseRegistry,
        serialize_schema,
    )

    include_configs = level in ("definition", "standard", "full")
    include_pipeline_dbs = level in ("standard", "full")

    # Build database registry (populated as nodes serialize)
    db_registry = DatabaseRegistry() if include_configs else None

    # --- Build node descriptors ---
    nodes: dict[str, dict] = {}
    for content_hash_str, node in self._persistent_node_map.items():
        tag_schema, packet_schema = node.output_schema()
        type_converter = node.data_context.type_converter

        descriptor: dict = {
            "node_type": node.node_type,
            "label": node.label,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "output_schema": {
                "tag": serialize_schema(tag_schema, type_converter),
                "packet": serialize_schema(packet_schema, type_converter),
            },
        }

        # node_uri: present at all levels
        if isinstance(node, SourceNode):
            stream = node.stream
            if isinstance(stream, cp.SourceProtocol):
                cfg = stream.to_config()
                stream_type = cfg.get("source_type", "unknown")
                source_id = cfg.get("source_id") or getattr(stream, "source_id", "")
            else:
                stream_type = "stream"
                source_id = ""
            descriptor["node_uri"] = [stream_type, str(source_id or "")]
        else:
            descriptor["node_uri"] = list(node.node_uri)

        # data_context_key: definition+ only
        if include_configs:
            descriptor["data_context_key"] = node.data_context_key

        # Node-type-specific config fields
        if include_configs:
            if isinstance(node, SourceNode):
                descriptor.update(self._build_source_descriptor(node, db_registry))
            elif isinstance(node, FunctionNode):
                descriptor.update(self._build_function_descriptor(node))
            elif isinstance(node, OperatorNode):
                descriptor.update(self._build_operator_descriptor(node, level))

        nodes[content_hash_str] = descriptor

    # --- Pipeline block ---
    pipeline_block: dict = {
        "name": list(self._name),
        "run_id": None,
        "snapshot_time": None,
    }
    if include_pipeline_dbs:
        pipeline_db_key = db_registry.register(self._pipeline_database.to_config())
        pipeline_block["pipeline_database"] = pipeline_db_key
        if self._function_database is not None:
            fn_db_key = db_registry.register(self._function_database.to_config())
            pipeline_block["function_database"] = fn_db_key
        else:
            pipeline_block["function_database"] = None

    # --- Top-level output ---
    output: dict = {
        "orcapod_pipeline_version": PIPELINE_FORMAT_VERSION,
        "level": level,
        "pipeline": pipeline_block,
        "nodes": nodes,
        "edges": [list(edge) for edge in self._graph_edges],
    }
    if db_registry is not None and db_registry.to_dict():
        output["databases"] = db_registry.to_dict()

    with open(path, "w") as f:
        json.dump(output, f, indent=2)
```

- [ ] **Step 4: Update `_build_source_descriptor` signature**

```python
def _build_source_descriptor(
    self, node: "SourceNode", db_registry=None
) -> dict:
    stream = node.stream
    if isinstance(stream, cp.SourceProtocol):
        # Pass db_registry so CachedSource (etc.) can register embedded DBs
        if db_registry is not None and hasattr(stream, "to_config"):
            import inspect
            sig = inspect.signature(stream.to_config)
            if "db_registry" in sig.parameters:
                config = stream.to_config(db_registry=db_registry)
            else:
                config = stream.to_config()
        else:
            config = stream.to_config()
        stream_type = config.get("source_type", "stream")
        # Strip identity fields — they live in the node descriptor
        source_config = {
            k: v for k, v in config.items()
            if k not in ("content_hash", "pipeline_hash", "tag_schema", "packet_schema")
        }
        reconstructable = stream_type in self._RECONSTRUCTABLE_SOURCE_TYPES
    else:
        source_config = None
        reconstructable = False

    return {
        "source_config": source_config,
        "reconstructable": reconstructable,
    }
```

- [ ] **Step 5: Update `_build_function_descriptor`**

```python
def _build_function_descriptor(self, node: "FunctionNode") -> dict:
    return {
        "function_config": node._function_pod.to_config(),
        # pipeline_path and result_record_path intentionally omitted
    }
```

- [ ] **Step 6: Update `_build_operator_descriptor`**

```python
def _build_operator_descriptor(self, node: "OperatorNode", level: str) -> dict:
    result = {
        "operator_config": node._operator.to_config(),
    }
    if level in ("standard", "full"):
        result["cache_mode"] = node._cache_mode.value
    return result
```

- [ ] **Step 7: Run new tests**

```bash
uv run pytest tests/test_pipeline/test_serialization.py -v -k "save_minimal or save_standard or save_definition or save_never"
```

Expected: PASS.

- [ ] **Step 8: Run full test suite and fix regressions**

```bash
uv run pytest tests/ -x -q
```

Existing tests that check the old format structure (e.g. `test_save_pipeline_metadata` checking `pipeline.databases.pipeline_database`) will need updating to match the new structure. Update them to check `data["databases"]` and `data["pipeline"]["pipeline_database"]`.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_serialization.py
git commit -m "feat(pipeline): implement save(level=) with normalized format (ENG-320)

- Add level parameter: minimal/definition/standard/full
- Top-level databases registry with SHA-256 keys
- pipeline_path and result_record_path never stored
- node_uri field present at all levels
- Rename function_pod->function_config, operator->operator_config
- Identity fields removed from source_config (in node descriptor)"
```

---

## Task 7: Update `Pipeline.load()` for New Format

**Files:**
- Modify: `src/orcapod/pipeline/graph.py:662-810`
- Test: `tests/test_pipeline/test_serialization.py`

**Context:** `load()` must handle the new format: read top-level `databases` registry, resolve DB keys, re-derive `pipeline_path` for nodes from stored identity fields (not from a stored `pipeline_path` field), and pass node descriptor identity fields to `_source_proxy_from_config`.

- [ ] **Step 1: Write failing round-trip tests**

```python
def test_round_trip_standard_level(tmp_path):
    """Save at standard level and reload; pipeline must be runnable."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1, 2, 3]}, source_id="src")
    pipeline = op.Pipeline("roundtrip", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    path = tmp_path / "pipeline.json"
    pipeline.save(str(path), level="standard")
    loaded = op.Pipeline.load(str(path))

    assert loaded is not None
    assert loaded._compiled


def test_round_trip_minimal_level_loads_without_error(tmp_path):
    """minimal level loads without error (read-only, no reconstruction)."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1]}, source_id="src")
    pipeline = op.Pipeline("minimal_test", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    path = tmp_path / "minimal.json"
    pipeline.save(str(path), level="minimal")
    loaded = op.Pipeline.load(str(path))
    assert loaded._compiled


def test_load_stores_node_uri_on_nodes(tmp_path):
    """Loaded nodes have _stored_node_uri populated from the saved node_uri."""
    import orcapod as op
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    import json

    db = InMemoryArrowDatabase()

    @op.packet_function(output_schema={"result": int})
    def add_one(x: int) -> dict:
        return {"result": x + 1}

    source = op.DictSource({"x": [1]}, source_id="src")
    pipeline = op.Pipeline("p", pipeline_database=db)
    pipeline.add(source, label="src")
    pipeline.add(add_one, label="fn", inputs=["src"])
    pipeline.compile()

    path = tmp_path / "p.json"
    pipeline.save(str(path), level="standard")
    loaded = op.Pipeline.load(str(path))

    fn_node = loaded._nodes["fn"]
    assert fn_node.node_uri == pipeline._nodes["fn"].node_uri
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_serialization.py -v -k "round_trip"
```

- [ ] **Step 3: Rewrite `Pipeline.load()` in `graph.py`**

The key structural changes to `load()`:

1. **Database resolution**: Read top-level `data.get("databases", {})` to build a `DatabaseRegistry`. Read `data["pipeline"].get("pipeline_database")` — if it's a string key, resolve via registry; if it's a dict (old format), use directly.

2. **Source proxy creation**: Pass the node descriptor when calling `_source_proxy_from_config`.

3. **`pipeline_path` re-derivation**: After loading a node, set `_stored_pipeline_path = None` (force re-derivation) rather than reading a stored value. The `pipeline_path` property already falls back to the live formula when `_stored_pipeline_path` is `None` and the live objects are present.

4. **`node_uri` restoration**: Store `node_descriptor["node_uri"]` on the node as `_stored_node_uri`.

```python
@classmethod
def load(cls, path: str | Path, mode: str = "full") -> "Pipeline":
    from orcapod.pipeline.serialization import (
        SUPPORTED_FORMAT_VERSIONS,
        DatabaseRegistry,
        LoadStatus,
        resolve_database_from_config,
        resolve_operator_from_config,
        resolve_source_from_config,
        _source_proxy_from_config,
    )

    path = Path(path)
    with open(path) as f:
        data = json.load(f)

    # 1. Validate version
    version = data.get("orcapod_pipeline_version", "")
    if version not in SUPPORTED_FORMAT_VERSIONS:
        raise ValueError(
            f"Unsupported pipeline format version {version!r}. "
            f"Supported: {sorted(SUPPORTED_FORMAT_VERSIONS)}"
        )

    level = data.get("level", "standard")  # default for old saves

    # 2. Build database registry
    db_registry = DatabaseRegistry.from_dict(data.get("databases", {}))

    def _resolve_db(ref):
        """Resolve a DB reference: string key or inline dict."""
        if ref is None:
            return None
        if isinstance(ref, str):
            return resolve_database_from_config(db_registry.resolve(ref))
        return resolve_database_from_config(ref)  # old format: inline dict

    # 3. Reconstruct pipeline-level databases
    pipeline_meta = data["pipeline"]

    # Handle both new format (key in pipeline block) and old (nested in pipeline.databases)
    if "databases" in pipeline_meta:
        # Old format: pipeline.databases.pipeline_database was an inline config
        old_db_block = pipeline_meta["databases"]
        pipeline_db = _resolve_db(old_db_block.get("pipeline_database"))
        function_db = _resolve_db(old_db_block.get("function_database"))
    else:
        pipeline_db = _resolve_db(pipeline_meta.get("pipeline_database"))
        function_db = _resolve_db(pipeline_meta.get("function_database"))

    # Fallback: if no pipeline_db (minimal level), use in-memory
    if pipeline_db is None:
        from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
        pipeline_db = InMemoryArrowDatabase()

    # 4. Build edge graph and topological order (unchanged)
    nodes_data = data["nodes"]
    edges = data["edges"]

    edge_graph: nx.DiGraph = nx.DiGraph()
    for upstream_hash, downstream_hash in edges:
        edge_graph.add_edge(upstream_hash, downstream_hash)
    for node_hash in nodes_data:
        if node_hash not in edge_graph:
            edge_graph.add_node(node_hash)
    topo_order = list(nx.topological_sort(edge_graph))

    # Build reverse edge map
    upstream_map: dict[str, list[str]] = {}
    for up_hash, down_hash in edges:
        upstream_map.setdefault(down_hash, []).append(up_hash)

    # 5. Reconstruct nodes in topological order
    reconstructed: dict[str, Any] = {}

    for node_hash in topo_order:
        descriptor = nodes_data.get(node_hash)
        if descriptor is None:
            continue

        node_type = descriptor.get("node_type")

        if node_type == "source":
            node = cls._load_source_node(
                descriptor, mode, resolve_source_from_config,
                db_registry=db_registry,
            )
            reconstructed[node_hash] = node

        elif node_type == "function":
            up_hashes = upstream_map.get(node_hash, [])
            upstream_node = reconstructed.get(up_hashes[0]) if up_hashes else None
            upstream_usable = (
                upstream_node is not None
                and hasattr(upstream_node, "load_status")
                and upstream_node.load_status
                in (LoadStatus.FULL, LoadStatus.READ_ONLY, LoadStatus.CACHE_ONLY)
            )
            result_db = function_db if function_db is not None else pipeline_db
            dbs = {"pipeline": pipeline_db, "result": result_db}
            node = cls._load_function_node(
                descriptor, mode, upstream_node, upstream_usable, dbs
            )
            reconstructed[node_hash] = node

        elif node_type == "operator":
            up_hashes = upstream_map.get(node_hash, [])
            upstream_nodes = tuple(
                reconstructed[h] for h in up_hashes if h in reconstructed
            )
            all_upstreams_usable = (
                all(
                    hasattr(n, "load_status")
                    and n.load_status in (LoadStatus.FULL, LoadStatus.READ_ONLY)
                    for n in upstream_nodes
                )
                if upstream_nodes
                else False
            )
            dbs = {"pipeline": pipeline_db}
            node = cls._load_operator_node(
                descriptor, mode, upstream_nodes, all_upstreams_usable, dbs,
                resolve_operator_from_config,
            )
            reconstructed[node_hash] = node

        # Store node_uri for read-only access
        if node is not None and "node_uri" in descriptor:
            node._stored_node_uri = tuple(descriptor["node_uri"])

        # Clear stored pipeline_path — re-derive from live formula
        if hasattr(node, "_stored_pipeline_path"):
            node._stored_pipeline_path = None

    # 6. Build Pipeline instance (identical to current code)
    name = tuple(pipeline_meta["name"])
    pipeline = cls(
        name=name,
        pipeline_database=pipeline_db,
        function_database=function_db,
        auto_compile=False,
    )
    pipeline._persistent_node_map = dict(reconstructed)
    pipeline._nodes = {}
    for node_hash, node in reconstructed.items():
        label = node.label
        if label:
            pipeline._nodes[label] = node

    pipeline._node_graph = nx.DiGraph()
    for up_hash, down_hash in edges:
        up_node = reconstructed.get(up_hash)
        down_node = reconstructed.get(down_hash)
        if up_node is not None and down_node is not None:
            pipeline._node_graph.add_edge(up_node, down_node)
    for node in reconstructed.values():
        if node not in pipeline._node_graph:
            pipeline._node_graph.add_node(node)

    pipeline._graph_edges = [(up, down) for up, down in edges]
    pipeline._hash_graph = nx.DiGraph()
    for up_hash, down_hash in edges:
        pipeline._hash_graph.add_edge(up_hash, down_hash)
    for node_hash, node in reconstructed.items():
        if node_hash not in pipeline._hash_graph:
            pipeline._hash_graph.add_node(node_hash)
        attrs = pipeline._hash_graph.nodes[node_hash]
        attrs["node_type"] = node.node_type
        if node.label:
            attrs["label"] = node.label

    pipeline._compiled = True
    return pipeline
```

- [ ] **Step 4: Update `_load_source_node` to pass node descriptor to SourceProxy**

Find `_load_source_node` in `graph.py` and ensure that when falling back to `_source_proxy_from_config`, it passes the node descriptor:

```python
# In the fallback path inside _load_source_node:
from orcapod.pipeline.serialization import _source_proxy_from_config
proxy = _source_proxy_from_config(
    source_config or {},
    node_descriptor=descriptor,  # provides content_hash, pipeline_hash, output_schema
)
```

Also update the function signature:
```python
@classmethod
def _load_source_node(cls, descriptor, mode, resolve_source_from_config, db_registry=None):
    ...
```

And when calling `resolve_source_from_config`, pass the `db_registry` if the source config has a string DB reference (i.e. `CachedSource`):
```python
# Check if source_config has a db_registry-dependent field
source_config = descriptor.get("source_config") or {}
# Reconstruct source; pass db_registry for sources with embedded DB keys
if db_registry is not None:
    try:
        import inspect
        cls_sig = inspect.signature(source_cls.from_config)
        if "db_registry" in cls_sig.parameters:
            source = source_cls.from_config(source_config, db_registry=db_registry)
        else:
            source = source_cls.from_config(source_config)
    except Exception:
        source = _source_proxy_from_config(source_config, node_descriptor=descriptor)
```

- [ ] **Step 5: Update `function_config` / `operator_config` key reading in load**

In `_load_function_node`, update the key it reads from:

```python
# Old: descriptor.get("function_pod")
# New: descriptor.get("function_config") or descriptor.get("function_pod")  # compat
function_config = descriptor.get("function_config") or descriptor.get("function_pod")
```

Same for `_load_operator_node`:
```python
operator_config = descriptor.get("operator_config") or descriptor.get("operator")
```

This ensures both old and new format saves are loadable.

- [ ] **Step 6: Run round-trip tests**

```bash
uv run pytest tests/test_pipeline/test_serialization.py -v -k "round_trip"
```

Expected: PASS.

- [ ] **Step 7: Run full test suite**

```bash
uv run pytest tests/ -x -q
```

Fix any remaining regressions. The most common issues will be:
- Tests checking `data["pipeline"]["databases"]` — now it's `data["databases"]`
- Tests checking `node["pipeline_path"]` — now absent
- Tests checking `node["function_pod"]` — now `node["function_config"]`

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_serialization.py
git commit -m "feat(pipeline): update load() for normalized format (ENG-320)

- Read top-level databases registry; resolve string keys or inline dicts
- Handle both old (pipeline.databases) and new (pipeline.pipeline_database key) formats
- Clear stored pipeline_path to force re-derivation from live formula
- Pass node descriptor identity fields to _source_proxy_from_config
- Read function_config/operator_config keys with fallback to old names"
```

---

## Task 8: Update `.gitignore` and Commit Plan

**Files:**
- Modify: `.gitignore` — add `!superpowers/plans` exception

- [ ] **Step 1: Update `.gitignore`**

```
# Superpowers (Claude Code skill artifacts - ignore everything except specs/plans)
superpowers/*
!superpowers/specs
!superpowers/plans
```

- [ ] **Step 2: Stage and commit the plan document**

```bash
git add .gitignore superpowers/plans/2026-03-28-normalized-pipeline-save-format.md
git commit -m "docs(pipeline): add normalized save format implementation plan (ENG-320)"
```

---

## Final Verification

- [ ] **Run the complete test suite one last time**

```bash
uv run pytest tests/ -q
```

Expected: All tests passing.

- [ ] **Smoke test: save at each level and confirm JSON structure**

```bash
uv run python -c "
import orcapod as op
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
import json, tempfile, os

db = InMemoryArrowDatabase()

@op.packet_function(output_schema={'result': int})
def add_one(x: int) -> dict:
    return {'result': x + 1}

src = op.DictSource({'x': [1, 2, 3]}, source_id='test')
p = op.Pipeline('smoke_test', pipeline_database=db)
p.add(src, label='src')
p.add(add_one, label='fn', inputs=['src'])
p.compile()

with tempfile.TemporaryDirectory() as d:
    for level in ('minimal', 'definition', 'standard'):
        path = os.path.join(d, f'{level}.json')
        p.save(path, level=level)
        with open(path) as f:
            data = json.load(f)
        print(f'{level}: level={data[\"level\"]}, keys={list(data.keys())}')
        fn_node = next(v for v in data['nodes'].values() if v['node_type'] == 'function')
        print(f'  fn node keys: {list(fn_node.keys())}')
        assert 'pipeline_path' not in str(data), 'pipeline_path leaked!'
        print(f'  pipeline_path check: OK')
"
```

- [ ] **Push branch and open PR**

```bash
git push -u origin eywalker/eng-256-design-normalize-pipeline-save-format-deduplicate-databases
gh pr create --title "feat(pipeline): normalized save format with database deduplication and save levels (ENG-256/ENG-320/ENG-342)" --body "$(cat <<'EOF'
## Summary
- Fixes pipeline_path formula bug (ENG-342): both FunctionNode and OperatorNode now use two-level \`schema:{pipeline_hash}\` + \`instance:{content_hash}\` formula
- Adds DatabaseRegistry for deduplicating database configs into top-level registry keyed by SHA-256 hash
- Implements \`pipeline.save(level=)\` with four levels: minimal, definition, standard, full
- Removes pipeline_path and result_record_path from all save levels (re-derived on load)
- Removes identity fields from source_config (moved to node descriptor)
- Updates Pipeline.load() to handle new format with backward compat for old saves

## Test plan
- [ ] \`uv run pytest tests/test_pipeline/test_serialization.py -v\`
- [ ] \`uv run pytest tests/ -q\` (full suite)
- [ ] Smoke test: save at each level, verify no pipeline_path in output

Closes ENG-256, ENG-320, ENG-342

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
