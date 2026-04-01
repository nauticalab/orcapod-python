# ENG-340 + ENG-349: Decouple Nodes from pipeline_path via ScopedDatabase Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove `pipeline_path` from all node and observer APIs by pre-scoping databases at pipeline compile time, replacing the old path-prefix threading pattern with clean pre-rooted database views.

**Architecture:** `Pipeline.compile()` creates scoped database views (`at("_result")`, `at("_status")`, `at("_log")`) and passes them directly to nodes and a default `CompositeObserver`. Nodes expose `node_identity_path` (without pipeline prefix) and call `observer.contextualize(*identity_path)` to get a bound wrapper before firing hooks. Observer implementations no longer mirror paths — they write relative to their pre-scoped database root.

**Tech Stack:** Python 3.11+, PyArrow, pytest, `orcapod` internal protocols. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-04-01-eng340-eng349-design.md`

---

## Parallelism Note

Tasks 1–2 (database scoping) and Tasks 3–6 (observer refactor) are independent of each other and can be started in parallel. Tasks 7–12 depend on both groups being complete.

---

## Task 1: Add `_root`/`_scoped_path` tracking to database `at()` methods

**Files:**
- Modify: `src/orcapod/databases/in_memory_databases.py:270-296`
- Modify: `src/orcapod/databases/delta_lake_databases.py:966-1002`
- Modify: `src/orcapod/databases/connector_arrow_database.py:284-311`
- Modify: `src/orcapod/databases/noop_database.py:90-97`
- Create: `tests/test_databases/test_scoped_database_tracking.py`

**Context:** Each database class has an `at(*path_components)` method that returns a new instance scoped to a sub-path. After this task, every instance created by `at()` will carry `_root` (the original root instance) and `_scoped_path` (the full path from root). Root instances have `_root = None` and `_scoped_path = ()`. The existing `_path_prefix` field is kept for functional routing.

- [ ] **Step 1: Write failing tests**

```python
# tests/test_databases/test_scoped_database_tracking.py
from orcapod.databases import InMemoryArrowDatabase
from orcapod.databases.noop_database import NoOpArrowDatabase


def test_root_database_has_no_root_ref():
    db = InMemoryArrowDatabase()
    assert db._root is None
    assert db._scoped_path == ()


def test_scoped_database_tracks_root_and_path():
    root = InMemoryArrowDatabase()
    scoped = root.at("pipeline", "_result")
    assert scoped._root is root
    assert scoped._scoped_path == ("pipeline", "_result")


def test_doubly_scoped_database_tracks_original_root():
    root = InMemoryArrowDatabase()
    mid = root.at("pipeline")
    leaf = mid.at("_result")
    assert leaf._root is root
    assert leaf._scoped_path == ("pipeline", "_result")


def test_noop_scoped_database_tracks_root_and_path():
    root = NoOpArrowDatabase()
    scoped = root.at("pipeline", "_status")
    assert scoped._root is root
    assert scoped._scoped_path == ("pipeline", "_status")
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
cd /path/to/orcapod-python
pytest tests/test_databases/test_scoped_database_tracking.py -v
```
Expected: `AttributeError: '_root'` or similar.

- [ ] **Step 3: Update `InMemoryArrowDatabase.__init__` and `at()`**

In `src/orcapod/databases/in_memory_databases.py`, add two new keyword-only constructor params (default `None` / `()`) and populate them in `at()`:

```python
# In __init__ signature, add after existing params:
_root: "InMemoryArrowDatabase | None" = None,
_scoped_path: tuple[str, ...] = (),

# In __init__ body:
self._root = _root
self._scoped_path = _scoped_path
```

In `at()` (line ~292), change the returned instance construction to pass:
```python
_root=self._root if self._root is not None else self,
_scoped_path=self._scoped_path + path_components,
```

- [ ] **Step 4: Update `DeltaTableDatabase.at()`** (`delta_lake_databases.py` line ~995)

Same pattern: add `_root` and `_scoped_path` kwargs to `__init__`, populate in `at()`:
```python
_root=self._root if self._root is not None else self,
_scoped_path=self._scoped_path + path_components,
```

- [ ] **Step 5: Update `ConnectorArrowDatabase.at()`** (`connector_arrow_database.py` line ~303)

Same pattern.

- [ ] **Step 6: Update `NoOpArrowDatabase.at()`** (`noop_database.py` line ~90)

Same pattern.

- [ ] **Step 7: Run tests**

```bash
pytest tests/test_databases/test_scoped_database_tracking.py -v
```
Expected: all 4 tests pass.

- [ ] **Step 8: Run full test suite to check for regressions**

```bash
pytest tests/ -x -q
```
Expected: all existing tests still pass.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/databases/ tests/test_databases/test_scoped_database_tracking.py
git commit -m "feat: add _root/_scoped_path tracking to database at() methods"
```

---

## Task 2: Scoped database serialization

**Files:**
- Modify: `src/orcapod/databases/in_memory_databases.py:384-398`
- Modify: `src/orcapod/databases/delta_lake_databases.py` (to_config section)
- Modify: `src/orcapod/databases/connector_arrow_database.py` (to_config section)
- Modify: `src/orcapod/databases/noop_database.py:98-103`
- Modify: `src/orcapod/pipeline/serialization.py:231-260`
- Test: `tests/test_databases/test_scoped_database_tracking.py` (extend)
- Test: `tests/test_pipeline/test_serialization.py` (extend)

**Context:** Scoped instances (where `_root is not None`) should serialize as `{"type": "scoped", "ref": "<key>", "path": [...]}` instead of their full config. `resolve_database_from_config` needs a second optional `registry` dict to handle the `"scoped"` type.

- [ ] **Step 1: Write failing tests**

```python
# Add to tests/test_databases/test_scoped_database_tracking.py

from orcapod.pipeline.serialization import DatabaseRegistry, resolve_database_from_config


def test_root_database_serializes_as_full_config():
    db = InMemoryArrowDatabase()
    registry = DatabaseRegistry()
    config = db.to_config(db_registry=registry)
    assert config["type"] == "in_memory"
    assert "ref" not in config


def test_scoped_database_serializes_as_reference():
    root = InMemoryArrowDatabase()
    scoped = root.at("pipeline", "_result")
    registry = DatabaseRegistry()
    config = scoped.to_config(db_registry=registry)
    assert config["type"] == "scoped"
    assert config["path"] == ["pipeline", "_result"]
    assert "ref" in config
    # Root config is registered
    assert config["ref"] in registry.to_dict()


def test_scoped_database_round_trips_via_registry():
    root = InMemoryArrowDatabase()
    scoped = root.at("my_pipeline", "_result")
    registry = DatabaseRegistry()
    config = scoped.to_config(db_registry=registry)
    db_dict = registry.to_dict()
    reconstructed = resolve_database_from_config(config, db_dict)
    # Should be rooted at the same path prefix
    assert reconstructed._path_prefix == scoped._path_prefix
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
pytest tests/test_databases/test_scoped_database_tracking.py::test_root_database_serializes_as_full_config -v
pytest tests/test_databases/test_scoped_database_tracking.py::test_scoped_database_serializes_as_reference -v
```
Expected: failures (to_config doesn't check `_root` yet).

- [ ] **Step 3: Update `InMemoryArrowDatabase.to_config()`**

```python
def to_config(self, db_registry=None) -> dict[str, Any]:
    if self._root is not None and db_registry is not None:
        ref = db_registry.register(self._root.to_config())
        return {
            "type": "scoped",
            "ref": ref,
            "path": list(self._scoped_path),
        }
    return {
        "type": "in_memory",
        "base_path": list(self._path_prefix),
        "max_hierarchy_depth": self.max_hierarchy_depth,
    }
```

- [ ] **Step 4: Apply same `to_config()` update to `DeltaTableDatabase`, `ConnectorArrowDatabase`, `NoOpArrowDatabase`**

Each follows the same pattern: check `self._root is not None and db_registry is not None`, emit scoped ref, otherwise emit existing full config.

- [ ] **Step 5: Extend `resolve_database_from_config` in `serialization.py`**

Current signature: `resolve_database_from_config(config: dict[str, Any]) -> Any`

New signature (backward compatible):
```python
def resolve_database_from_config(
    config: dict[str, Any],
    registry: dict[str, dict] | None = None,
) -> Any:
    db_type = config.get("type")

    if db_type == "scoped":
        if registry is None:
            raise ValueError("registry required to resolve scoped database config")
        root_config = registry[config["ref"]]
        root_db = resolve_database_from_config(root_config, registry)
        return root_db.at(*config["path"])

    # existing logic unchanged below...
```

- [ ] **Step 6: Run all serialization-related tests**

```bash
pytest tests/test_databases/test_scoped_database_tracking.py -v
pytest tests/test_pipeline/test_serialization.py -v
```
Expected: all pass.

- [ ] **Step 7: Full suite regression check**

```bash
pytest tests/ -x -q
```

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/databases/ src/orcapod/pipeline/serialization.py tests/test_databases/
git commit -m "feat: scoped database serialization via registry reference format"
```

---

## Task 3: Update observer protocol and NoOpObserver

**Files:**
- Modify: `src/orcapod/protocols/observability_protocols.py`
- Modify: `src/orcapod/pipeline/observer.py`
- Test: `tests/test_pipeline/test_observer.py`

**Context:** `ExecutionObserverProtocol.contextualize` currently takes `(node_hash, node_label)`. It becomes `(*identity_path: str)` and must return `ExecutionObserverProtocol`. The `pipeline_path` kwarg is removed from `on_node_start`, `on_node_end`, and `create_packet_logger`. The `on_run_start` docstring must be updated. `NoOpObserver.contextualize` returns `self`.

- [ ] **Step 1: Write failing protocol conformance test**

```python
# Add to tests/test_pipeline/test_observer.py
from orcapod.pipeline.observer import NoOpObserver
from orcapod.protocols.observability_protocols import ExecutionObserverProtocol


def test_noop_observer_contextualize_returns_self():
    obs = NoOpObserver()
    bound = obs.contextualize("my_pod", "schema:abc", "instance:xyz")
    assert bound is obs


def test_noop_observer_on_node_start_no_pipeline_path():
    obs = NoOpObserver()
    # Must not raise with new signature (no pipeline_path kwarg)
    obs.on_node_start("label", "hash", tag_schema=None)


def test_noop_observer_on_node_end_no_pipeline_path():
    obs = NoOpObserver()
    obs.on_node_end("label", "hash")


def test_noop_observer_create_packet_logger_no_pipeline_path():
    obs = NoOpObserver()
    logger = obs.create_packet_logger("tag", {})
    assert logger is not None
```

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_pipeline/test_observer.py -v
```

- [ ] **Step 3: Update `ExecutionObserverProtocol` in `observability_protocols.py`**

Change:
```python
# BEFORE
def contextualize(self, node_hash: str, node_label: str) -> ExecutionObserverProtocol: ...
def on_node_start(self, node_label, node_hash, pipeline_path=(), tag_schema=None): ...
def on_node_end(self, node_label, node_hash, pipeline_path=()): ...
def create_packet_logger(self, tag, packet, pipeline_path=()): ...

# AFTER
def contextualize(self, *identity_path: str) -> "ExecutionObserverProtocol": ...
def on_node_start(self, node_label, node_hash, tag_schema=None): ...
def on_node_end(self, node_label, node_hash): ...
def create_packet_logger(self, tag, packet): ...
```

Also rewrite the `on_run_start` docstring — remove the sentence referencing `pipeline_path` on `on_node_start`.

- [ ] **Step 4: Update `NoOpObserver` in `observer.py`**

```python
def contextualize(self, *identity_path: str) -> "NoOpObserver":
    return self

def on_node_start(self, node_label, node_hash, tag_schema=None):
    pass

def on_node_end(self, node_label, node_hash):
    pass

def create_packet_logger(self, tag, packet):
    return NoOpLogger()
```

- [ ] **Step 5: Run observer tests**

```bash
pytest tests/test_pipeline/test_observer.py -v
```

- [ ] **Step 6: Full suite (expect failures in observer integration tests — that's fine for now)**

```bash
pytest tests/test_pipeline/test_observer.py tests/test_pipeline/test_node_protocols.py -v
```

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/protocols/observability_protocols.py src/orcapod/pipeline/observer.py tests/test_pipeline/test_observer.py
git commit -m "feat: update observer protocol - contextualize(*identity_path), remove pipeline_path"
```

---

## Task 4: Rewrite StatusObserver

**Files:**
- Modify: `src/orcapod/pipeline/status_observer.py`
- Test: `tests/test_pipeline/test_status_observer_integration.py`

**Context:** `StatusObserver` currently takes `(status_database, pipeline_path_prefix)` and mutates `_node_context` on `contextualize`. New design: takes only pre-scoped `status_database`; `contextualize(*identity_path)` returns a `_ContextualizedStatusObserver` wrapping the shared db + bound path. `get_status()` loses `pipeline_path` param. Add `to_config(db_registry)` / `from_config(config, registry)`.

- [ ] **Step 1: Update existing tests to use new API**

In `tests/test_pipeline/test_status_observer_integration.py`:
- Replace `StatusObserver(db, pipeline_path_prefix=(...))` with `StatusObserver(db.at(...))`
- Replace `observer.contextualize(node_hash, node_label)` with `observer.contextualize(*identity_path)`
- Remove `pipeline_path=` kwargs from `on_node_start` / `on_node_end` calls
- Remove `pipeline_path=` from `get_status()` calls

- [ ] **Step 2: Run tests to confirm failures**

```bash
pytest tests/test_pipeline/test_status_observer_integration.py -v
```

- [ ] **Step 3: Rewrite `StatusObserver`**

```python
class StatusObserver:
    def __init__(self, status_database: ArrowDatabaseProtocol) -> None:
        self._db = status_database

    def contextualize(self, *identity_path: str) -> "_ContextualizedStatusObserver":
        return _ContextualizedStatusObserver(self._db, identity_path)

    def get_status(self) -> ...:
        # query self._db without pipeline_path param
        ...

    def to_config(self, db_registry=None) -> dict:
        return {
            "type": "status",
            "database": self._db.to_config(db_registry=db_registry),
        }

    @classmethod
    def from_config(cls, config: dict, registry: dict | None = None) -> "StatusObserver":
        db = resolve_database_from_config(config["database"], registry)
        return cls(db)


class _ContextualizedStatusObserver:
    """Implements ExecutionObserverProtocol. Writes to db at the bound identity_path."""

    def __init__(self, db: ArrowDatabaseProtocol, identity_path: tuple[str, ...]) -> None:
        self._db = db
        self._identity_path = identity_path

    def contextualize(self, *identity_path: str) -> "_ContextualizedStatusObserver":
        return _ContextualizedStatusObserver(self._db, identity_path)

    def on_node_start(self, node_label, node_hash, tag_schema=None):
        # write to self._db at self._identity_path
        ...

    def on_node_end(self, node_label, node_hash):
        ...

    def create_packet_logger(self, tag, packet):
        return NoOpLogger()  # status observer doesn't log packets

    def on_run_start(self, run_id, **kwargs): pass
    def on_run_end(self, run_id): pass
```

Remove the `_node_context` dict entirely. Fill in the existing status record-writing logic (currently in the old `on_node_start`) inside `_ContextualizedStatusObserver.on_node_start`, using `self._identity_path` as the record path.

- [ ] **Step 4: Write and run serialization round-trip test**

```python
# Add to tests/test_pipeline/test_status_observer_integration.py
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import DatabaseRegistry, resolve_database_from_config
from orcapod.pipeline.status_observer import StatusObserver


def test_status_observer_to_config_shape():
    db = InMemoryArrowDatabase()
    status_db = db.at("my_pipeline", "_status")
    obs = StatusObserver(status_db)
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    assert config["type"] == "status"
    assert config["database"]["type"] == "scoped"
    assert config["database"]["path"] == ["my_pipeline", "_status"]


def test_status_observer_from_config_round_trip():
    db = InMemoryArrowDatabase()
    status_db = db.at("my_pipeline", "_status")
    obs = StatusObserver(status_db)
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    db_dict = registry.to_dict()
    restored = StatusObserver.from_config(config, db_dict)
    assert restored._db._path_prefix == status_db._path_prefix
```

```bash
pytest tests/test_pipeline/test_status_observer_integration.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/status_observer.py tests/test_pipeline/test_status_observer_integration.py
git commit -m "feat: rewrite StatusObserver with pre-scoped db and contextualize wrapper"
```

---

## Task 5: Rewrite LoggingObserver

**Files:**
- Modify: `src/orcapod/pipeline/logging_observer.py`
- Test: `tests/test_pipeline/test_logging_observer_integration.py`

**Context:** Mirror of Task 4. `LoggingObserver(log_database)` — no prefix. `contextualize(*identity_path)` returns `_ContextualizedLoggingObserver`. `PacketLogger` receives `log_path=identity_path`. `get_logs()` loses `pipeline_path`. Add `to_config`/`from_config`.

- [ ] **Step 1: Update existing tests to use new API**

In `tests/test_pipeline/test_logging_observer_integration.py`:
- Replace constructor calls, `contextualize`, and `create_packet_logger` call sites with new API
- Remove `pipeline_path=` from all hook calls and `get_logs()`

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_pipeline/test_logging_observer_integration.py -v
```

- [ ] **Step 3: Rewrite `LoggingObserver`**

Same structural pattern as Task 4. `_ContextualizedLoggingObserver.create_packet_logger(tag, packet)` returns a `PacketLogger` constructed with `log_path=self._identity_path`.

```python
class LoggingObserver:
    def __init__(self, log_database: ArrowDatabaseProtocol) -> None:
        self._db = log_database

    def contextualize(self, *identity_path: str) -> "_ContextualizedLoggingObserver":
        return _ContextualizedLoggingObserver(self._db, identity_path)

    def to_config(self, db_registry=None) -> dict:
        return {
            "type": "logging",
            "database": self._db.to_config(db_registry=db_registry),
        }

    @classmethod
    def from_config(cls, config: dict, registry: dict | None = None) -> "LoggingObserver":
        db = resolve_database_from_config(config["database"], registry)
        return cls(db)


class _ContextualizedLoggingObserver:
    def __init__(self, db, identity_path):
        self._db = db
        self._identity_path = identity_path

    def contextualize(self, *identity_path: str) -> "_ContextualizedLoggingObserver":
        return _ContextualizedLoggingObserver(self._db, identity_path)

    def create_packet_logger(self, tag, packet):
        return PacketLogger(db=self._db, log_path=self._identity_path, tag=tag, packet=packet)

    def on_node_start(self, node_label, node_hash, tag_schema=None): pass
    def on_node_end(self, node_label, node_hash): pass
    def on_run_start(self, run_id, **kwargs): pass
    def on_run_end(self, run_id): pass
```

- [ ] **Step 4: Write and run serialization round-trip test**

```python
# Add to tests/test_pipeline/test_logging_observer_integration.py
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import DatabaseRegistry
from orcapod.pipeline.logging_observer import LoggingObserver


def test_logging_observer_to_config_shape():
    db = InMemoryArrowDatabase()
    log_db = db.at("my_pipeline", "_log")
    obs = LoggingObserver(log_db)
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    assert config["type"] == "logging"
    assert config["database"]["type"] == "scoped"
    assert config["database"]["path"] == ["my_pipeline", "_log"]


def test_logging_observer_from_config_round_trip():
    db = InMemoryArrowDatabase()
    log_db = db.at("my_pipeline", "_log")
    obs = LoggingObserver(log_db)
    registry = DatabaseRegistry()
    config = obs.to_config(db_registry=registry)
    restored = LoggingObserver.from_config(config, registry.to_dict())
    assert restored._db._path_prefix == log_db._path_prefix
```

```bash
pytest tests/test_pipeline/test_logging_observer_integration.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/logging_observer.py tests/test_pipeline/test_logging_observer_integration.py
git commit -m "feat: rewrite LoggingObserver with pre-scoped db and contextualize wrapper"
```

---

## Task 6: Update CompositeObserver + add OBSERVER_REGISTRY

**Files:**
- Modify: `src/orcapod/pipeline/composite_observer.py`
- Modify: `src/orcapod/pipeline/serialization.py` (add `OBSERVER_REGISTRY`)
- Test: `tests/test_pipeline/test_composite_observer.py`

**Context:** `CompositeObserver.contextualize(*identity_path)` should call `contextualize` on each child and return a new `CompositeObserver` wrapping the bound children. Add `to_config(db_registry)` / `from_config(config, registry)`. Update module docstring example. Add `OBSERVER_REGISTRY` dict to `serialization.py`.

- [ ] **Step 1: Update existing tests**

In `tests/test_pipeline/test_composite_observer.py`:
- Replace all `observer.contextualize(node_hash, node_label)` with `observer.contextualize(*identity_path)`
- Remove `pipeline_path=` from all hook calls
- Update any `SyncPipelineOrchestrator(observer=...)` construction to use `orchestrator.run(graph, observer=...)`

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_pipeline/test_composite_observer.py -v
```

- [ ] **Step 3: Update `CompositeObserver`**

```python
class CompositeObserver:
    def __init__(self, *observers) -> None:
        self._observers = list(observers)

    def contextualize(self, *identity_path: str) -> "CompositeObserver":
        return CompositeObserver(*[o.contextualize(*identity_path) for o in self._observers])

    def on_node_start(self, node_label, node_hash, tag_schema=None):
        for o in self._observers:
            o.on_node_start(node_label, node_hash, tag_schema=tag_schema)

    def on_node_end(self, node_label, node_hash):
        for o in self._observers:
            o.on_node_end(node_label, node_hash)

    def create_packet_logger(self, tag, packet):
        # return first non-noop logger (existing logic)
        ...

    def on_run_start(self, run_id, **kwargs):
        for o in self._observers:
            o.on_run_start(run_id, **kwargs)

    def on_run_end(self, run_id):
        for o in self._observers:
            o.on_run_end(run_id)

    def to_config(self, db_registry=None) -> dict:
        return {
            "type": "composite",
            "observers": [o.to_config(db_registry=db_registry) for o in self._observers],
        }

    @classmethod
    def from_config(cls, config: dict, registry: dict | None = None) -> "CompositeObserver":
        children = [
            OBSERVER_REGISTRY[c["type"]](c, registry)
            for c in config["observers"]
        ]
        return cls(*children)
```

Update the module-level docstring example to use `orchestrator.run(graph, observer=observer)`.

- [ ] **Step 4: Add `OBSERVER_REGISTRY` to `serialization.py`**

```python
# Near the bottom of serialization.py, alongside DATABASE_REGISTRY

def _build_observer_registry() -> dict[str, Any]:
    from orcapod.pipeline.status_observer import StatusObserver
    from orcapod.pipeline.logging_observer import LoggingObserver
    from orcapod.pipeline.composite_observer import CompositeObserver
    return {
        "status":    lambda cfg, reg: StatusObserver.from_config(cfg, reg),
        "logging":   lambda cfg, reg: LoggingObserver.from_config(cfg, reg),
        "composite": lambda cfg, reg: CompositeObserver.from_config(cfg, reg),
    }

OBSERVER_REGISTRY: dict[str, Any] = _build_observer_registry()


def resolve_observer_from_config(config: dict, registry: dict | None = None):
    obs_type = config.get("type")
    if obs_type not in OBSERVER_REGISTRY:
        raise ValueError(f"Unknown observer type: {obs_type!r}")
    return OBSERVER_REGISTRY[obs_type](config, registry)
```

- [ ] **Step 5: Run composite observer tests**

```bash
pytest tests/test_pipeline/test_composite_observer.py -v
```

- [ ] **Step 6: Run all observer tests together**

```bash
pytest tests/test_pipeline/test_observer.py tests/test_pipeline/test_status_observer_integration.py tests/test_pipeline/test_logging_observer_integration.py tests/test_pipeline/test_composite_observer.py -v
```

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/pipeline/composite_observer.py src/orcapod/pipeline/serialization.py tests/test_pipeline/test_composite_observer.py
git commit -m "feat: update CompositeObserver contextualize + to_config/from_config + OBSERVER_REGISTRY"
```

---

## Task 7: Simplify CachedFunctionPod

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py:44-58`
- Test: `tests/test_core/function_pod/test_cached_function_pod.py`

**Context:** Remove `record_path_prefix: tuple[str, ...] = ()` from `__init__`. Change `record_path=record_path_prefix + self.uri` to `record_path=self.uri`. The `result_database` passed in is already pre-scoped; no prefix needed.

- [ ] **Step 1: Add a test asserting the parameter is gone, and remove existing usages**

In `tests/test_core/function_pod/test_cached_function_pod.py`:
- Remove any `record_path_prefix=` kwargs from existing `CachedFunctionPod(...)` calls
- Add a new test that confirms passing the old kwarg now raises:
```python
import pytest
from orcapod.core.cached_function_pod import CachedFunctionPod

def test_record_path_prefix_param_removed(some_pod, some_db):
    with pytest.raises(TypeError, match="record_path_prefix"):
        CachedFunctionPod(some_pod, result_database=some_db, record_path_prefix=("x",))
```

- [ ] **Step 2: Run to confirm the new test fails (parameter still exists)**

```bash
pytest tests/test_core/function_pod/test_cached_function_pod.py::test_record_path_prefix_param_removed -v
```

- [ ] **Step 3: Update `CachedFunctionPod.__init__`**

```python
def __init__(
    self,
    function_pod: FunctionPodProtocol,
    result_database: ArrowDatabaseProtocol | None = None,
    # record_path_prefix removed
) -> None:
    self._function_pod = function_pod
    self._result_database = result_database
    if result_database is not None:
        self._cache = ResultCache(
            result_database=result_database,
            record_path=self.uri,  # no prefix; db is pre-scoped
        )
    else:
        self._cache = None
```

- [ ] **Step 4: Run tests**

```bash
pytest tests/test_core/function_pod/test_cached_function_pod.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/core/cached_function_pod.py tests/test_core/function_pod/test_cached_function_pod.py
git commit -m "refactor: remove record_path_prefix from CachedFunctionPod"
```

---

## Task 8: Refactor FunctionNode

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Test: `tests/test_core/function_pod/test_function_node_attach_db.py`
- Test: `tests/test_core/function_pod/test_function_node_caching.py`

**Context:** Remove `_pipeline_path_prefix` field and all backward-compat prefix-stripping in `from_descriptor()`. Rename `pipeline_path` property → `node_identity_path` (returns `pf.uri + (schema_hash, instance_hash)` only — no prefix). Simplify `attach_databases()` to remove `pipeline_path_prefix` param. Update `execute()` to use `observer.contextualize(*self.node_identity_path)`. Update `CachedFunctionPod` instantiation inside `attach_databases()` to pass only `result_database` (no prefix).

- [ ] **Step 1: Update existing tests**

In `tests/test_core/function_pod/test_function_node_attach_db.py`:
- Remove `pipeline_path_prefix=` kwargs from `attach_databases()` calls
- Replace `.pipeline_path` property access with `.node_identity_path`
- Verify that `node_identity_path` returns `pod.uri + (schema_hash, instance_hash)` (no pipeline prefix)

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_core/function_pod/test_function_node_attach_db.py -v
```

- [ ] **Step 3: Update `FunctionNode.__init__`**

Remove `pipeline_path_prefix: tuple[str, ...] = ()` parameter. Remove `self._pipeline_path_prefix = ()` initialization.

- [ ] **Step 4: Update `FunctionNode.attach_databases()`**

```python
def attach_databases(
    self,
    pipeline_database: ArrowDatabaseProtocol,
    result_database: ArrowDatabaseProtocol | None = None,
    # pipeline_path_prefix removed
) -> None:
    self._pipeline_database = pipeline_database
    result_db = result_database if result_database is not None else pipeline_database
    self._cached_pod = CachedFunctionPod(
        function_pod=self._function_pod,
        result_database=result_db,
    )
```

- [ ] **Step 5: Rename `pipeline_path` → `node_identity_path` and simplify**

```python
@property
def node_identity_path(self) -> tuple[str, ...]:
    pf = self._function_pod
    return pf.uri + (
        f"schema:{self.pipeline_hash().to_string()}",
        f"instance:{self.content_hash().to_string()}",
    )
```

Remove the old `pipeline_path` property and all `_stored_pipeline_path` / `_pipeline_path_prefix` logic.

- [ ] **Step 6: Update `execute()` to use `contextualize`**

In `execute()` (and `async_execute()` if present), replace:
```python
# BEFORE
observer.on_node_start(label, hash, pipeline_path=self.pipeline_path, tag_schema=...)
```
With:
```python
# AFTER
ctx_obs = observer.contextualize(*self.node_identity_path)
ctx_obs.on_node_start(label, hash, tag_schema=...)
```
And use `ctx_obs` for all subsequent observer calls within that execute scope.

- [ ] **Step 7: Update `from_descriptor()` — remove prefix-stripping logic**

Remove the `hint_prefix` / `pipeline_path_prefix` derivation block entirely (lines ~255-322). The `databases` dict no longer contains `"pipeline_path_prefix"`. The cleaned-up database extraction should look like:

```python
@classmethod
def from_descriptor(cls, descriptor, function_pod, input_stream, databases):
    pipeline_db = databases.get("pipeline")
    result_db = databases.get("result")  # pre-scoped; None if not provided

    node = cls(
        function_pod=function_pod,
        input_stream=input_stream,
        label=descriptor.get("label"),
    )
    if pipeline_db is not None:
        node.attach_databases(
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
    return node
```

All prefix inference from stored `pipeline_path` is gone.

- [ ] **Step 8: Run tests**

```bash
pytest tests/test_core/function_pod/test_function_node_attach_db.py tests/test_core/function_pod/test_function_node_caching.py -v
```

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py tests/test_core/function_pod/
git commit -m "refactor: FunctionNode - remove pipeline_path_prefix, rename to node_identity_path, use contextualize"
```

---

## Task 9: Refactor OperatorNode

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Test: `tests/test_core/operators/test_operator_node_attach_db.py`

**Context:** Mirror of Task 8. Remove `_pipeline_path_prefix`, rename `pipeline_path` → `node_identity_path`, remove `pipeline_path_prefix` from `attach_databases()` and `from_descriptor()`, update `execute()` / `async_execute()` to use `contextualize`.

- [ ] **Step 1: Update existing tests**

In `tests/test_core/operators/test_operator_node_attach_db.py`:
- Remove `pipeline_path_prefix=` kwargs from `attach_databases()` calls
- Replace `.pipeline_path` with `.node_identity_path`

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_core/operators/test_operator_node_attach_db.py -v
```

- [ ] **Step 3: Update `OperatorNode.__init__`**

Remove `pipeline_path_prefix` parameter and `self._pipeline_path_prefix = ()`.

- [ ] **Step 4: Update `attach_databases()`**

```python
def attach_databases(
    self,
    pipeline_database: ArrowDatabaseProtocol,
    cache_mode: CacheMode = CacheMode.OFF,
    # pipeline_path_prefix removed
) -> None:
    self._pipeline_database = pipeline_database
    self._cache_mode = cache_mode
    self.clear_cache()
    self._content_hash_cache.clear()
    self._pipeline_hash_cache.clear()
```

- [ ] **Step 5: Rename `pipeline_path` → `node_identity_path`**

```python
@property
def node_identity_path(self) -> tuple[str, ...]:
    return (
        self._operator.uri
        + (
            f"schema:{self.pipeline_hash().to_string()}",
            f"instance:{self.content_hash().to_string()}",
        )
    )
```

- [ ] **Step 6: Update `execute()` / `async_execute()` to use `contextualize`**

Same pattern as Task 8 — replace `observer.on_node_start(..., pipeline_path=...)` with:
```python
ctx_obs = observer.contextualize(*self.node_identity_path)
ctx_obs.on_node_start(label, hash, tag_schema=...)
```

- [ ] **Step 7: Update `from_descriptor()` — remove `pipeline_path_prefix` handling**

Remove the block at lines ~176-206 that reads `databases["pipeline_path_prefix"]` or derives prefix from stored `pipeline_path`. The node is now constructed with only `pipeline_database` and `cache_mode`.

- [ ] **Step 8: Run tests**

```bash
pytest tests/test_core/operators/test_operator_node_attach_db.py tests/test_core/operators/test_operator_node.py -v
```

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py tests/test_core/operators/
git commit -m "refactor: OperatorNode - remove pipeline_path_prefix, rename to node_identity_path, use contextualize"
```

---

## Task 10: Overhaul Pipeline compile() and default observer

**Files:**
- Modify: `src/orcapod/pipeline/graph.py`
- Test: `tests/test_pipeline/test_pipeline.py`
- Test: `tests/test_pipeline/test_serialization.py`

**Context:** This is the integration point. `Pipeline.__init__` renames `function_database` → `result_database`. `compile()` creates all four scoped databases eagerly and constructs `_default_observer`. `run()` gains `observer=None` param and uses `effective_observer = observer or self._default_observer`. `save(level="full")` serializes `_default_observer`. `load()` reconstructs the scoped hierarchy and restores `_default_observer` from config. All `pipeline_path_prefix` hint passing in `compile()` is removed.

- [ ] **Step 1: Write new tests for scoped database creation and default observer**

```python
# Add to tests/test_pipeline/test_pipeline.py

from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.composite_observer import CompositeObserver


def test_compile_creates_scoped_databases():
    db = InMemoryArrowDatabase()
    with Pipeline("my_pipeline", pipeline_database=db) as p:
        pass
    assert p.result_database._scoped_path == ("my_pipeline", "_result")
    assert p.status_database._scoped_path == ("my_pipeline", "_status")
    assert p.log_database._scoped_path == ("my_pipeline", "_log")


def test_compile_creates_default_composite_observer():
    db = InMemoryArrowDatabase()
    with Pipeline("my_pipeline", pipeline_database=db) as p:
        pass
    assert isinstance(p._default_observer, CompositeObserver)


def test_compile_without_pipeline_database_uses_noop_observer():
    from orcapod.pipeline.observer import NoOpObserver
    with Pipeline("no_db_pipeline") as p:
        pass
    assert isinstance(p._default_observer, NoOpObserver)


def test_run_with_override_observer_does_not_raise():
    """Passing an explicit observer to run() should be accepted without error."""
    from unittest.mock import MagicMock
    db = InMemoryArrowDatabase()
    obs = MagicMock()
    obs.contextualize.return_value = obs
    with Pipeline("my_pipeline", pipeline_database=db) as p:
        pass
    # An empty compiled pipeline has no nodes to execute; run() must not raise
    p.run(observer=obs)
```

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_pipeline/test_pipeline.py -k "scoped_database or default_observer or noop_observer" -v
```

- [ ] **Step 3: Rename `function_database` → `result_database` in `__init__` and align private field names**

In `graph.py`:
- Change `function_database` param name to `result_database` in `__init__` (line 68)
- The existing `self._pipeline_database` stores the **raw user-provided root database** — keep this name as-is; it is the `user_db` from which scoped views are derived in `compile()`
- Change `self._function_database = function_database` to `self._result_database = result_database` (this optional override stores an explicit result database; when `None`, `compile()` derives one from `_pipeline_database`)
- Remove the `function_database` property (lines 173-175)
- Add four read-only properties after `pipeline_database`:
```python
@property
def result_database(self):
    """The _result-scoped database view (set after compile())."""
    return self._result_database_scoped

@property
def scoped_pipeline_database(self):
    return self._scoped_pipeline_database

@property
def status_database(self):
    return self._status_database

@property
def log_database(self):
    return self._log_database
```
- Update `compile()`, `save()`, and `load()` references from `_function_database` to `_result_database`

- [ ] **Step 4: Update `compile()` to create scoped databases and default observer**

Replace the current `pipeline_path_prefix` passing (lines ~253-277) with:

```python
def compile(self) -> None:
    ...
    # self._pipeline_database is the RAW USER ROOT DATABASE
    # self._result_database is an optional explicit override
    if self._pipeline_database is not None:
        pipeline_db = self._pipeline_database.at(*self._name)  # user_db.at(*name)
        result_db = pipeline_db.at("_result")
        status_db = pipeline_db.at("_status")
        log_db = pipeline_db.at("_log")
        self._scoped_pipeline_database = pipeline_db
        self._result_database_scoped = result_db
        self._status_database = status_db
        self._log_database = log_db

        from orcapod.pipeline.composite_observer import CompositeObserver
        from orcapod.pipeline.status_observer import StatusObserver
        from orcapod.pipeline.logging_observer import LoggingObserver
        self._default_observer = CompositeObserver(
            StatusObserver(status_db),
            LoggingObserver(log_db),
        )
    else:
        result_db = self._result_database  # explicit override or None
        self._default_observer = NoOpObserver()

    # attach databases to nodes using the pipeline-scoped view, not the raw root
    for node in function_nodes:
        node.attach_databases(
            pipeline_database=pipeline_db,   # scoped: user_db.at(*name)
            result_database=result_db,        # scoped: pipeline_db.at("_result")
        )
    for node in operator_nodes:
        node.attach_databases(
            pipeline_database=pipeline_db,   # scoped: user_db.at(*name)
        )
    ...
```

Add read-only properties:
```python
@property
def scoped_pipeline_database(self): return self._scoped_pipeline_database

@property
def status_database(self): return self._status_database

@property
def log_database(self): return self._log_database
```

- [ ] **Step 5: Add `observer` param to `run()`**

```python
def run(
    self,
    *,
    observer=None,
    materialize_results=True,
    run_id=None,
    pipeline_uri="",
):
    effective_observer = observer or self._default_observer
    orchestrator = SyncPipelineOrchestrator()
    return orchestrator.run(
        self._node_graph,
        observer=effective_observer,
        materialize_results=materialize_results,
        run_id=run_id,
        pipeline_uri=pipeline_uri,
    )
```

- [ ] **Step 6: Update `save()` to serialize `_default_observer`**

In the `"full"` save level block:
```python
if level == "full" and self._default_observer is not None:
    pipeline_block["observer"] = self._default_observer.to_config(db_registry=db_registry)
```
(No import needed here — `to_config` is called directly on the observer instance.)

- [ ] **Step 7: Update `load()` to reconstruct `_default_observer`**

After restoring the databases:
```python
if "observer" in pipeline_meta:
    from orcapod.pipeline.serialization import resolve_observer_from_config
    pipeline._default_observer = resolve_observer_from_config(
        pipeline_meta["observer"], db_registry_data
    )
```

Direct assignment to `_default_observer` is acceptable here because `load()` is constructing the object. To enforce immutability post-construction, add a `_compiled` flag check: set `self._compiled = True` at the end of `compile()`, and guard `_default_observer` against reassignment after that point (e.g., with an `assert not self._compiled` check or by making `_default_observer` a property that raises after compile). This ensures external callers cannot replace the default observer after the pipeline is compiled.

Remove all `pipeline_path_prefix` hint construction from `load()` (the `pipeline_path_prefix = name` block and its usage at lines ~823-915).

- [ ] **Step 8: Run pipeline tests**

```bash
pytest tests/test_pipeline/test_pipeline.py tests/test_pipeline/test_serialization.py -v
```

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/pipeline/graph.py tests/test_pipeline/test_pipeline.py tests/test_pipeline/test_serialization.py
git commit -m "feat: Pipeline compile() creates scoped DBs and default observer eagerly"
```

---

## Task 11: Move observer from orchestrator `__init__` to `run()`

**Files:**
- Modify: `src/orcapod/pipeline/sync_orchestrator.py:43-55`
- Modify: `src/orcapod/pipeline/async_orchestrator.py:50-60`
- Test: `tests/test_pipeline/test_sync_orchestrator.py`
- Test: `tests/test_pipeline/test_orchestrator.py`

**Context:** Currently `SyncPipelineOrchestrator(observer=obs)` stores the observer on `self`. Change to stateless orchestrator with `observer=None` in `run()`. Any `SyncPipelineOrchestrator(observer=obs).run(graph)` pattern in tests must become `SyncPipelineOrchestrator().run(graph, observer=obs)`.

- [ ] **Step 1: Update tests**

In `tests/test_pipeline/test_sync_orchestrator.py` and `test_orchestrator.py`:
- Replace all `SyncPipelineOrchestrator(observer=obs)` with `SyncPipelineOrchestrator()`
- Move the observer to `.run(graph, observer=obs)`
- Update inline observer definitions — `contextualize(node_hash, node_label)` → `contextualize(*identity_path)` (there are 6 such definitions in `test_sync_orchestrator.py` at lines 147, 228, 438, 482, 504, 537)
- Remove `pipeline_path=` from any `on_node_start` / `on_node_end` calls in those inline observers

- [ ] **Step 2: Run to confirm failures**

```bash
pytest tests/test_pipeline/test_sync_orchestrator.py -v
```

- [ ] **Step 3: Update `SyncPipelineOrchestrator`**

```python
class SyncPipelineOrchestrator:
    def __init__(self) -> None:  # observer param removed
        pass

    def run(
        self,
        graph,
        *,
        observer=None,
        materialize_results=True,
        run_id=None,
        pipeline_uri="",
    ):
        effective_observer = observer or NoOpObserver()
        if effective_observer is not None:
            effective_observer.on_run_start(run_id, pipeline_uri=pipeline_uri)
        # ... rest of run logic using effective_observer instead of self._observer
        if effective_observer is not None:
            effective_observer.on_run_end(run_id)
```

- [ ] **Step 4: Update `AsyncPipelineOrchestrator`**

Remove `observer` from `__init__` and add to `run()` and `run_async()`. Note `buffer_size` remains as an existing param:

```python
class AsyncPipelineOrchestrator:
    def __init__(self) -> None:  # observer param removed
        pass

    def run(
        self,
        graph,
        *,
        observer=None,
        materialize_results=True,
        run_id=None,
        pipeline_uri="",
        buffer_size=...,  # preserve existing default
    ):
        import asyncio
        return asyncio.run(self.run_async(
            graph,
            observer=observer,
            materialize_results=materialize_results,
            run_id=run_id,
            pipeline_uri=pipeline_uri,
            buffer_size=buffer_size,
        ))

    async def run_async(
        self,
        graph,
        *,
        observer=None,
        materialize_results=True,
        run_id=None,
        pipeline_uri="",
        buffer_size=...,
    ):
        effective_observer = observer or NoOpObserver()
        effective_observer.on_run_start(run_id, pipeline_uri=pipeline_uri)
        # ... rest of async execution using effective_observer ...
        effective_observer.on_run_end(run_id)
```

Also add a test in `tests/test_pipeline/test_orchestrator.py` for the async orchestrator:
```python
def test_async_orchestrator_accepts_observer_in_run():
    """AsyncPipelineOrchestrator() has no observer param; it goes to run()."""
    from orcapod.pipeline import AsyncPipelineOrchestrator
    from orcapod.pipeline.observer import NoOpObserver
    import inspect
    orch = AsyncPipelineOrchestrator()
    # __init__ must not accept observer
    assert "observer" not in inspect.signature(orch.__init__).parameters
    # run() must accept observer as keyword arg
    assert "observer" in inspect.signature(orch.run).parameters
    # run_async() too
    assert "observer" in inspect.signature(orch.run_async).parameters
```

- [ ] **Step 5: Run orchestrator tests**

```bash
pytest tests/test_pipeline/test_sync_orchestrator.py tests/test_pipeline/test_orchestrator.py tests/test_pipeline/test_orchestrator_executor_matrix.py -v
```

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/sync_orchestrator.py src/orcapod/pipeline/async_orchestrator.py tests/test_pipeline/test_sync_orchestrator.py tests/test_pipeline/test_orchestrator.py
git commit -m "refactor: move observer from orchestrator __init__ to run()"
```

---

## Task 12: Update ObservabilityReader path components

**Files:**
- Modify: `src/orcapod/pipeline/observability_reader.py:56-68`
- Test: `tests/test_pipeline/test_observability_reader.py`

**Context:** `_discover_tables()` currently checks for `"status"` and `"logs"` in path parts. Change to `"_status"` and `"_log"` (underscore-prefixed, and `"logs"` → `"_log"` is plural-to-singular).

- [ ] **Step 1: Write a test for the new path components**

```python
# In tests/test_pipeline/test_observability_reader.py
# (add alongside existing tests or replace old path tests)

import tempfile
from pathlib import Path


def _make_fake_delta_table(path: Path) -> None:
    """Create a minimal _delta_log directory to simulate a Delta table."""
    (path / "_delta_log").mkdir(parents=True, exist_ok=True)


def test_discover_tables_finds_status_and_log_paths(tmp_path):
    from orcapod.pipeline.observability_reader import ObservabilityReader

    # Create fake Delta tables at new-format paths
    _make_fake_delta_table(tmp_path / "my_pipeline" / "_status" / "my_pod" / "schema:abc" / "instance:xyz")
    _make_fake_delta_table(tmp_path / "my_pipeline" / "_log" / "my_pod" / "schema:abc" / "instance:xyz")

    reader = ObservabilityReader(tmp_path)
    assert "my_pod" in reader.nodes
```

- [ ] **Step 2: Run to confirm failure**

```bash
pytest tests/test_pipeline/test_observability_reader.py::test_discover_tables_finds_status_and_log_paths -v
```

- [ ] **Step 3: Update `_discover_tables()` in `observability_reader.py`**

```python
# BEFORE (lines 56-68):
if "status" in parts:
    idx = parts.index("status")
    ...
elif "logs" in parts:
    idx = parts.index("logs")

# AFTER:
if "_status" in parts:
    idx = parts.index("_status")
    ...
elif "_log" in parts:
    idx = parts.index("_log")
```

- [ ] **Step 4: Run all observability reader tests**

```bash
pytest tests/test_pipeline/test_observability_reader.py -v
```

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/observability_reader.py tests/test_pipeline/test_observability_reader.py
git commit -m "feat: ObservabilityReader discovers _status/_log paths (was status/logs)"
```

---

## Task 13: Full integration pass + cleanup

**Files:** All modified files (review pass)

**Context:** Run the complete test suite. Fix any remaining failures from tests that reference old APIs. Check for any remaining uses of `pipeline_path`, `pipeline_path_prefix`, `function_database`, or old `contextualize(hash, label)` signatures.

- [ ] **Step 1: Run the full test suite**

```bash
pytest tests/ -v 2>&1 | tee /tmp/test_output.txt
```

- [ ] **Step 2: Search for any remaining old API references**

```bash
grep -rn "pipeline_path_prefix\|pipeline_path\b\|function_database\|record_path_prefix\|contextualize(node_hash\|contextualize(.*node_label" src/ tests/ --include="*.py" | grep -v "node_identity_path\|spec\|plan"
```

Fix any remaining hits.

- [ ] **Step 3: Verify serialization round-trip with a full pipeline**

```python
# Manual smoke test (run in Python REPL or add as test)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline

db = InMemoryArrowDatabase()
with Pipeline("smoke_test", pipeline_database=db) as p:
    pass  # empty pipeline

assert p.status_database._scoped_path == ("smoke_test", "_status")
assert p.log_database._scoped_path == ("smoke_test", "_log")
assert p.result_database._scoped_path == ("smoke_test", "_result")

import tempfile, json
with tempfile.TemporaryDirectory() as d:
    path = f"{d}/pipeline.json"
    p.save(path, level="full")
    data = json.load(open(path))
    assert "observer" in data["pipeline"]
    assert data["pipeline"]["observer"]["type"] == "composite"
```

- [ ] **Step 4: Final commit**

```bash
git add src/ tests/
git commit -m "fix: clean up remaining old API references after ENG-340/349 refactor"
```

---

## Task 14: Update Linear issue status

- [ ] **Step 1: Mark ENG-340 as In Progress (or Done if all tests pass)**

```bash
# Use Linear MCP or gh CLI if configured
# Mark ENG-340 and ENG-349 as complete
```
