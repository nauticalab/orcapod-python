# Database Path Contextualization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `at(*path_components)` and `base_path` to `ArrowDatabaseProtocol` and all four database backends, enabling sub-scoped database views that namespace reads/writes under a relative path prefix.

**Architecture:** Each backend stores a `_path_prefix: tuple[str, ...]` (starts as `()`) and exposes it via a `base_path` property. `at()` returns a new instance with an extended prefix. `InMemoryArrowDatabase` and `ConnectorArrowDatabase` share their underlying storage dicts by reference; `DeltaTableDatabase` creates a fresh instance (the filesystem is the shared storage). Prefix application happens in `_get_record_key` for InMemory/Connector and in `_get_table_uri` for Delta (to avoid a double-prefix bug in `flush()`).

**Tech Stack:** Python 3.11+, PyArrow, deltalake, uv (run all commands via `uv run`)

**Spec:** `superpowers/specs/2026-04-01-database-path-contextualization-design.md`

---

## File Map

| Action | File | What changes |
|--------|------|-------------|
| Modify | `src/orcapod/protocols/database_protocols.py` | Add `base_path` property and `at()` to `ArrowDatabaseProtocol` |
| Modify | `src/orcapod/databases/noop_database.py` | Add `_path_prefix`, `base_path`, `at()`, update config |
| Modify | `src/orcapod/databases/in_memory_databases.py` | Add `_path_prefix`, shared-dict `at()`, update `_get_record_key`, `_validate_record_path`, config |
| Modify | `src/orcapod/databases/connector_arrow_database.py` | Add `_path_prefix`, shared-dict `at()`, update `_get_record_key`, `_validate_record_path`, config |
| Modify | `src/orcapod/databases/delta_lake_databases.py` | Rename `_base_uri`→`_root_uri`, `base_path`→`_local_root`; add `_path_prefix`; update `_get_table_uri`, `_validate_record_path`, `list_sources()`, `at()`, config |
| Modify | `tests/test_databases/test_noop_database.py` | Add `TestAtMethod` class |
| Modify | `tests/test_databases/test_in_memory_database.py` | Add `TestAtMethod` class |
| Modify | `tests/test_databases/test_connector_arrow_database.py` | Add `TestAtMethod` class |
| Modify | `tests/test_databases/test_delta_table_database.py` | Add `TestAtMethod` class |
| Modify | `tests/test_databases/test_database_config.py` | Update `DeltaTableDatabase` config tests for renamed key; add `base_path` round-trip tests for all backends |

---

## Task 1: Protocol + Stubs

Add `base_path` and `at()` to `ArrowDatabaseProtocol`, then add minimal stubs to all four backends so existing conformance tests (`isinstance(db, ArrowDatabaseProtocol)`) keep passing.

**Files:**
- Modify: `src/orcapod/protocols/database_protocols.py`
- Modify: `src/orcapod/databases/noop_database.py`
- Modify: `src/orcapod/databases/in_memory_databases.py`
- Modify: `src/orcapod/databases/connector_arrow_database.py`
- Modify: `src/orcapod/databases/delta_lake_databases.py`

- [ ] **Step 1: Add `base_path` and `at()` to `ArrowDatabaseProtocol`**

In `src/orcapod/protocols/database_protocols.py`, add after the `flush` method:

```python
@property
def base_path(self) -> tuple[str, ...]:
    """The current relative root of this database view.

    Always () for a root (non-scoped) instance. Extended by at().
    The absolute storage root (filesystem URI, SQL connector, etc.)
    is a separate, backend-specific implementation detail.
    """
    ...

def at(self, *path_components: str) -> "ArrowDatabaseProtocol":
    """Return a new database scoped to the given sub-path.

    All reads and writes on the returned database are relative to
    this database's base_path extended by path_components. The
    original is unmodified.

    Calling at() with no arguments returns a new view equivalent
    to the current one (same base_path, fresh or shared state
    depending on the backend).

    For backends with shared pending state (InMemoryArrowDatabase,
    ConnectorArrowDatabase), calling flush() on any view drains
    the entire shared pending queue — not just the caller's prefix.
    This is intentional: all views share the same underlying store.

    Args:
        *path_components: Zero or more path components to append.

    Returns:
        A new database instance with
        base_path == self.base_path + path_components.
    """
    ...
```

Also add both names to `__all__` at the bottom (they're part of the protocol, no separate export needed — `ArrowDatabaseProtocol` is already exported).

- [ ] **Step 2: Add stubs to all four backends**

In each of the four backend files, add these two members (exact bodies shown; full implementations come in later tasks):

**`noop_database.py`** — add after `flush`:
```python
@property
def base_path(self) -> tuple[str, ...]:
    return ()

def at(self, *path_components: str) -> "NoOpArrowDatabase":
    return NoOpArrowDatabase()
```

**`in_memory_databases.py`** — add after `flush`:
```python
@property
def base_path(self) -> tuple[str, ...]:
    return ()

def at(self, *path_components: str) -> "InMemoryArrowDatabase":
    return InMemoryArrowDatabase(max_hierarchy_depth=self.max_hierarchy_depth)
```

**`connector_arrow_database.py`** — add after `flush`:
```python
@property
def base_path(self) -> tuple[str, ...]:
    return ()

def at(self, *path_components: str) -> "ConnectorArrowDatabase":
    return ConnectorArrowDatabase(
        connector=self._connector,
        max_hierarchy_depth=self.max_hierarchy_depth,
    )
```

**`delta_lake_databases.py`** — add after `flush`:
```python
@property
def base_path(self) -> tuple[str, ...]:
    return ()

def at(self, *path_components: str) -> "DeltaTableDatabase":
    return DeltaTableDatabase(
        base_path=self._base_uri,
        storage_options=self._storage_options,
        batch_size=self.batch_size,
        max_hierarchy_depth=self.max_hierarchy_depth,
        allow_schema_evolution=self.allow_schema_evolution,
    )
```

- [ ] **Step 3: Run the full test suite to verify nothing is broken**

```bash
uv run pytest tests/test_databases/ -x -q
```

Expected: all previously passing tests still pass. No new failures.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/protocols/database_protocols.py \
        src/orcapod/databases/noop_database.py \
        src/orcapod/databases/in_memory_databases.py \
        src/orcapod/databases/connector_arrow_database.py \
        src/orcapod/databases/delta_lake_databases.py
git commit -m "feat(databases): add at() and base_path stubs to ArrowDatabaseProtocol and all backends"
```

---

## Task 2: `NoOpArrowDatabase` — full implementation

**Files:**
- Modify: `src/orcapod/databases/noop_database.py`
- Modify: `tests/test_databases/test_noop_database.py`
- Modify: `tests/test_databases/test_database_config.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_databases/test_noop_database.py`:

```python
class TestAtMethod:
    def test_base_path_is_empty_on_root_instance(self):
        db = NoOpArrowDatabase()
        assert db.base_path == ()
        assert isinstance(db.base_path, tuple)

    def test_at_sets_base_path(self):
        db = NoOpArrowDatabase()
        scoped = db.at("a", "b")
        assert scoped.base_path == ("a", "b")
        assert isinstance(scoped.base_path, tuple)

    def test_at_chaining_equivalent_to_multi_component(self):
        db = NoOpArrowDatabase()
        assert db.at("a").at("b").base_path == db.at("a", "b").base_path

    def test_at_does_not_modify_original(self):
        db = NoOpArrowDatabase()
        db.at("a", "b")
        assert db.base_path == ()

    def test_scoped_reads_still_return_none(self):
        db = NoOpArrowDatabase()
        scoped = db.at("pipeline", "node1")
        import pyarrow as pa
        scoped.add_record(("outputs",), "id1", pa.table({"v": [1]}))
        assert scoped.get_record_by_id(("outputs",), "id1") is None
        assert scoped.get_all_records(("outputs",)) is None
```

Add to `TestNoOpDatabaseConfig` in `tests/test_databases/test_database_config.py`:

```python
def test_to_config_includes_base_path(self):
    db = NoOpArrowDatabase()
    assert db.to_config()["base_path"] == []

def test_round_trip_preserves_base_path(self):
    db = NoOpArrowDatabase()
    scoped = db.at("pipeline", "v1")
    config = scoped.to_config()
    restored = NoOpArrowDatabase.from_config(config)
    assert restored.base_path == ("pipeline", "v1")
    assert isinstance(restored.base_path, tuple)
```

- [ ] **Step 2: Run to verify tests fail**

```bash
uv run pytest tests/test_databases/test_noop_database.py::TestAtMethod \
              tests/test_databases/test_database_config.py::TestNoOpDatabaseConfig -x -q
```

Expected: FAIL — `base_path` returns `()` for scoped instances, `to_config` missing `base_path`.

- [ ] **Step 3: Implement full `NoOpArrowDatabase`**

Replace the stub `base_path` and `at()` in `src/orcapod/databases/noop_database.py` and update config:

Add `_path_prefix` to `__init__`:
```python
def __init__(self, _path_prefix: tuple[str, ...] = ()) -> None:
    self._path_prefix = _path_prefix
```

Replace the stub `base_path` property:
```python
@property
def base_path(self) -> tuple[str, ...]:
    """The current relative root of this database view (always () for root instances)."""
    return self._path_prefix
```

Replace the stub `at()`:
```python
def at(self, *path_components: str) -> "NoOpArrowDatabase":
    return NoOpArrowDatabase(_path_prefix=self._path_prefix + path_components)
```

Replace `to_config`:
```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict."""
    return {
        "type": "noop",
        "base_path": list(self._path_prefix),
    }
```

Replace `from_config`:
```python
@classmethod
def from_config(cls, config: dict[str, Any]) -> "NoOpArrowDatabase":
    """Reconstruct a NoOpArrowDatabase from a config dict."""
    return cls(_path_prefix=tuple(config.get("base_path", [])))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_noop_database.py \
              tests/test_databases/test_database_config.py::TestNoOpDatabaseConfig -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/noop_database.py \
        tests/test_databases/test_noop_database.py \
        tests/test_databases/test_database_config.py
git commit -m "feat(databases): implement at() and base_path for NoOpArrowDatabase"
```

---

## Task 3: `InMemoryArrowDatabase` — full implementation

**Files:**
- Modify: `src/orcapod/databases/in_memory_databases.py`
- Modify: `tests/test_databases/test_in_memory_database.py`
- Modify: `tests/test_databases/test_database_config.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_databases/test_in_memory_database.py`:

```python
class TestAtMethod:
    def test_base_path_is_empty_on_root_instance(self, db):
        assert db.base_path == ()
        assert isinstance(db.base_path, tuple)

    def test_at_sets_base_path(self, db):
        scoped = db.at("a", "b")
        assert scoped.base_path == ("a", "b")
        assert isinstance(scoped.base_path, tuple)

    def test_at_chaining_equivalent_to_multi_component(self, db):
        assert db.at("a").at("b").base_path == db.at("a", "b").base_path

    def test_at_does_not_modify_original(self, db):
        db.at("a", "b")
        assert db.base_path == ()

    def test_writes_through_scoped_view_readable_from_same_view(self, db):
        scoped = db.at("pipeline", "node1")
        record = make_table(value=[42])
        scoped.add_record(("outputs",), "id1", record)
        result = scoped.get_record_by_id(("outputs",), "id1")
        assert result is not None
        assert result.column("value").to_pylist() == [42]

    def test_scoped_write_not_visible_via_parent_at_same_path(self, db):
        scoped = db.at("pipeline", "node1")
        scoped.add_record(("outputs",), "id1", make_table(value=[42]))
        # Parent sees ("outputs",) as a different key from ("pipeline","node1","outputs")
        assert db.get_record_by_id(("outputs",), "id1") is None

    def test_two_scoped_views_share_storage(self, db):
        view_a = db.at("pipeline", "node1")
        view_b = db.at("pipeline", "node1")
        view_a.add_record(("outputs",), "id1", make_table(value=[99]))
        view_a.flush()
        result = view_b.get_record_by_id(("outputs",), "id1")
        assert result is not None
        assert result.column("value").to_pylist() == [99]

    def test_validate_record_path_checks_combined_depth(self, db):
        # max_hierarchy_depth=10 (default). Fill prefix with 9 components.
        deep_db = InMemoryArrowDatabase(max_hierarchy_depth=10)
        scoped = deep_db.at("a", "b", "c", "d", "e", "f", "g", "h", "i")
        # 9 prefix + 1 record_path = 10: OK
        scoped.add_record(("z",), "id1", make_table(value=[1]))
        # 9 prefix + 2 record_path = 11: should raise
        with pytest.raises(ValueError):
            scoped.add_record(("z", "extra"), "id2", make_table(value=[2]))
```

Add to `TestInMemoryDatabaseConfig` in `tests/test_databases/test_database_config.py`:

```python
def test_to_config_includes_base_path(self):
    db = InMemoryArrowDatabase()
    assert db.to_config()["base_path"] == []

def test_round_trip_preserves_base_path(self):
    db = InMemoryArrowDatabase()
    scoped = db.at("pipeline", "v1")
    config = scoped.to_config()
    restored = InMemoryArrowDatabase.from_config(config)
    assert restored.base_path == ("pipeline", "v1")
    assert isinstance(restored.base_path, tuple)
```

- [ ] **Step 2: Run to verify tests fail**

```bash
uv run pytest tests/test_databases/test_in_memory_database.py::TestAtMethod \
              tests/test_databases/test_database_config.py::TestInMemoryDatabaseConfig -x -q
```

Expected: FAIL.

- [ ] **Step 3: Implement full `InMemoryArrowDatabase`**

Update `__init__` signature in `src/orcapod/databases/in_memory_databases.py`:

```python
def __init__(
    self,
    max_hierarchy_depth: int = 10,
    _path_prefix: tuple[str, ...] = (),
    _shared_tables: "dict[str, pa.Table] | None" = None,
    _shared_pending_batches: "dict[str, pa.Table] | None" = None,
    _shared_pending_record_ids: "dict[str, set[str]] | None" = None,
):
    self._path_prefix = _path_prefix
    self.max_hierarchy_depth = max_hierarchy_depth
    self._tables: dict[str, pa.Table] = _shared_tables if _shared_tables is not None else {}
    self._pending_batches: dict[str, pa.Table] = _shared_pending_batches if _shared_pending_batches is not None else {}
    self._pending_record_ids: dict[str, set[str]] = _shared_pending_record_ids if _shared_pending_record_ids is not None else defaultdict(set)
```

Replace `_get_record_key`:
```python
def _get_record_key(self, record_path: tuple[str, ...]) -> str:
    return "/".join(self._path_prefix + record_path)
```

Update `_validate_record_path` — change the depth check line to:
```python
if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
    raise ValueError(
        f"record_path depth {len(record_path)} exceeds maximum "
        f"{self.max_hierarchy_depth - len(self._path_prefix)} "
        f"(base_path uses {len(self._path_prefix)} components)"
    )
```

Replace stub `base_path` property:
```python
@property
def base_path(self) -> tuple[str, ...]:
    """The current relative root of this database view (always () for root instances)."""
    return self._path_prefix
```

Replace stub `at()`:
```python
def at(self, *path_components: str) -> "InMemoryArrowDatabase":
    return InMemoryArrowDatabase(
        max_hierarchy_depth=self.max_hierarchy_depth,
        _path_prefix=self._path_prefix + path_components,
        _shared_tables=self._tables,
        _shared_pending_batches=self._pending_batches,
        _shared_pending_record_ids=self._pending_record_ids,
    )
```

Replace `to_config`:
```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict."""
    return {
        "type": "in_memory",
        "base_path": list(self._path_prefix),
        "max_hierarchy_depth": self.max_hierarchy_depth,
    }
```

Replace `from_config`:
```python
@classmethod
def from_config(cls, config: dict[str, Any]) -> "InMemoryArrowDatabase":
    """Reconstruct an InMemoryArrowDatabase from a config dict."""
    return cls(
        max_hierarchy_depth=config.get("max_hierarchy_depth", 10),
        _path_prefix=tuple(config.get("base_path", [])),
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_in_memory_database.py \
              tests/test_databases/test_database_config.py::TestInMemoryDatabaseConfig -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/in_memory_databases.py \
        tests/test_databases/test_in_memory_database.py \
        tests/test_databases/test_database_config.py
git commit -m "feat(databases): implement at() and base_path for InMemoryArrowDatabase"
```

---

## Task 4: `ConnectorArrowDatabase` — full implementation

**Files:**
- Modify: `src/orcapod/databases/connector_arrow_database.py`
- Modify: `tests/test_databases/test_connector_arrow_database.py`
- Modify: `tests/test_databases/test_database_config.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_databases/test_connector_arrow_database.py`. Note: look at the top of the test file for the `MockDBConnector` fixture — use the same pattern as other tests in that file to get a `ConnectorArrowDatabase` instance backed by the mock connector.

```python
class TestAtMethod:
    def test_base_path_is_empty_on_root_instance(self, db):
        assert db.base_path == ()
        assert isinstance(db.base_path, tuple)

    def test_at_sets_base_path(self, db):
        scoped = db.at("a", "b")
        assert scoped.base_path == ("a", "b")
        assert isinstance(scoped.base_path, tuple)

    def test_at_chaining_equivalent_to_multi_component(self, db):
        assert db.at("a").at("b").base_path == db.at("a", "b").base_path

    def test_at_does_not_modify_original(self, db):
        db.at("a", "b")
        assert db.base_path == ()

    def test_writes_through_scoped_view_readable_from_same_view(self, db):
        scoped = db.at("pipeline", "node1")
        record = pa.table({"value": pa.array([42])})
        scoped.add_record(("outputs",), "id1", record, flush=True)
        result = scoped.get_record_by_id(("outputs",), "id1")
        assert result is not None
        assert result.column("value").to_pylist() == [42]

    def test_scoped_write_not_visible_via_parent_at_same_path(self, db):
        scoped = db.at("pipeline", "node1")
        scoped.add_record(("outputs",), "id1", pa.table({"v": pa.array([1])}), flush=True)
        assert db.get_record_by_id(("outputs",), "id1") is None

    def test_two_scoped_views_share_storage(self, db):
        view_a = db.at("pipeline", "node1")
        view_b = db.at("pipeline", "node1")
        view_a.add_record(("outputs",), "id1", pa.table({"v": pa.array([99])}), flush=True)
        result = view_b.get_record_by_id(("outputs",), "id1")
        assert result is not None
        assert result.column("v").to_pylist() == [99]

    def test_prefix_appears_in_sql_table_name(self, db):
        """Prefix components are included in the SQL table name via _path_to_table_name."""
        scoped = db.at("pipeline", "node1")
        scoped.add_record(("outputs",), "id1", pa.table({"v": pa.array([1])}), flush=True)
        # Table name should be pipeline__node1__outputs
        table_names = db._connector.get_table_names()
        assert "pipeline__node1__outputs" in table_names

    def test_validate_record_path_checks_combined_depth(self, db):
        scoped = db.at("a", "b", "c", "d", "e", "f", "g", "h", "i")  # 9 prefix components
        scoped.add_record(("z",), "id1", pa.table({"v": pa.array([1])}))
        with pytest.raises(ValueError):
            scoped.add_record(("z", "extra"), "id2", pa.table({"v": pa.array([2])}))
```

Add to `TestConnectorArrowDatabaseConfig` (or the config section) in `test_database_config.py`:

```python
# Note: ConnectorArrowDatabase.from_config raises NotImplementedError,
# so only test to_config here.
def test_connector_to_config_includes_base_path():
    from unittest.mock import MagicMock
    from orcapod.databases import ConnectorArrowDatabase
    mock_connector = MagicMock()
    mock_connector.to_config.return_value = {"type": "mock"}
    db = ConnectorArrowDatabase(connector=mock_connector)
    scoped = db.at("pipeline", "v1")
    config = scoped.to_config()
    assert config["base_path"] == ["pipeline", "v1"]
```

- [ ] **Step 2: Run to verify tests fail**

```bash
uv run pytest tests/test_databases/test_connector_arrow_database.py::TestAtMethod -x -q
```

Expected: FAIL.

- [ ] **Step 3: Implement full `ConnectorArrowDatabase`**

Update `__init__` in `src/orcapod/databases/connector_arrow_database.py`:

```python
def __init__(
    self,
    connector: DBConnectorProtocol,
    max_hierarchy_depth: int = 10,
    _path_prefix: tuple[str, ...] = (),
    _shared_pending_batches: "dict[str, pa.Table] | None" = None,
    _shared_pending_record_ids: "dict[str, set[str]] | None" = None,
    _shared_pending_skip_existing: "dict[str, bool] | None" = None,
) -> None:
    self._connector = connector
    self.max_hierarchy_depth = max_hierarchy_depth
    self._path_prefix = _path_prefix
    self._pending_batches: dict[str, pa.Table] = _shared_pending_batches if _shared_pending_batches is not None else {}
    self._pending_record_ids: dict[str, set[str]] = _shared_pending_record_ids if _shared_pending_record_ids is not None else defaultdict(set)
    self._pending_skip_existing: dict[str, bool] = _shared_pending_skip_existing if _shared_pending_skip_existing is not None else {}
```

Replace `_get_record_key`:
```python
def _get_record_key(self, record_path: tuple[str, ...]) -> str:
    return "/".join(self._path_prefix + record_path)
```

Update `_validate_record_path` depth check:
```python
if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
    raise ValueError(
        f"record_path depth {len(record_path)} exceeds maximum "
        f"{self.max_hierarchy_depth - len(self._path_prefix)} "
        f"(base_path uses {len(self._path_prefix)} components)"
    )
```

Replace stub `base_path` property:
```python
@property
def base_path(self) -> tuple[str, ...]:
    """The current relative root of this database view (always () for root instances)."""
    return self._path_prefix
```

Replace stub `at()`:
```python
def at(self, *path_components: str) -> "ConnectorArrowDatabase":
    return ConnectorArrowDatabase(
        connector=self._connector,
        max_hierarchy_depth=self.max_hierarchy_depth,
        _path_prefix=self._path_prefix + path_components,
        _shared_pending_batches=self._pending_batches,
        _shared_pending_record_ids=self._pending_record_ids,
        _shared_pending_skip_existing=self._pending_skip_existing,
    )
```

Update `to_config`:
```python
def to_config(self) -> dict[str, Any]:
    """Serialize configuration to a JSON-compatible dict."""
    return {
        "type": "connector_arrow_database",
        "connector": self._connector.to_config(),
        "base_path": list(self._path_prefix),
        "max_hierarchy_depth": self.max_hierarchy_depth,
    }
```

(`from_config` still raises `NotImplementedError` — no change needed.)

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_connector_arrow_database.py -q
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/connector_arrow_database.py \
        tests/test_databases/test_connector_arrow_database.py \
        tests/test_databases/test_database_config.py
git commit -m "feat(databases): implement at() and base_path for ConnectorArrowDatabase"
```

---

## Task 5: `DeltaTableDatabase` — full implementation

This is the most complex task. It involves attribute renames, `_get_table_uri` changes, `list_sources()` update, and a config key rename that requires updating an existing test.

**Files:**
- Modify: `src/orcapod/databases/delta_lake_databases.py`
- Modify: `tests/test_databases/test_delta_table_database.py`
- Modify: `tests/test_databases/test_database_config.py`

- [ ] **Step 1: Write failing tests**

Add this class to `tests/test_databases/test_delta_table_database.py`:

```python
class TestAtMethod:
    def test_base_path_is_empty_on_root_instance(self, db):
        assert db.base_path == ()
        assert isinstance(db.base_path, tuple)

    def test_at_sets_base_path(self, db):
        scoped = db.at("pipeline", "node1")
        assert scoped.base_path == ("pipeline", "node1")
        assert isinstance(scoped.base_path, tuple)

    def test_at_chaining_equivalent_to_multi_component(self, db):
        assert db.at("a").at("b").base_path == db.at("a", "b").base_path

    def test_at_does_not_modify_original(self, db):
        db.at("pipeline", "node1")
        assert db.base_path == ()

    def test_writes_through_scoped_view_readable_from_same_view(self, db):
        scoped = db.at("pipeline", "node1")
        record = make_table(value=[42])
        scoped.add_record(("outputs",), "id1", record, flush=True)
        result = scoped.get_record_by_id(("outputs",), "id1", flush=True)
        assert result is not None
        assert result.column("value").to_pylist() == [42]

    def test_scoped_write_not_visible_via_parent_at_same_path(self, db):
        scoped = db.at("pipeline", "node1")
        scoped.add_record(("outputs",), "id1", make_table(value=[42]), flush=True)
        assert db.get_record_by_id(("outputs",), "id1") is None

    def test_scoped_write_stored_under_correct_filesystem_path(self, db, tmp_path):
        scoped = db.at("pipeline", "node1")
        scoped.add_record(("outputs",), "id1", make_table(value=[1]), flush=True)
        # Delta table should be at <tmp_path>/db/pipeline/node1/outputs/
        import os
        expected_path = tmp_path / "db" / "pipeline" / "node1" / "outputs"
        assert expected_path.exists()

    def test_validate_record_path_checks_combined_depth(self, tmp_path):
        db = DeltaTableDatabase(base_path=tmp_path / "db", max_hierarchy_depth=10)
        scoped = db.at("a", "b", "c", "d", "e", "f", "g", "h", "i")  # 9 prefix
        # 9 + 1 = 10: OK
        scoped.add_record(("z",), "id1", make_table(value=[1]))
        # 9 + 2 = 11: should raise
        with pytest.raises(ValueError):
            scoped.add_record(("z", "extra"), "id2", make_table(value=[2]))

    def test_list_sources_on_scoped_instance(self, tmp_path):
        db = DeltaTableDatabase(base_path=tmp_path / "db")
        scoped = db.at("pipeline")
        scoped.add_record(("node1",), "id1", make_table(value=[1]), flush=True)
        scoped.add_record(("node2",), "id2", make_table(value=[2]), flush=True)
        sources = scoped.list_sources()
        assert ("node1",) in sources
        assert ("node2",) in sources
        # Root db should NOT see these under bare ("node1",) — they live under pipeline/
        root_sources = db.list_sources()
        assert ("node1",) not in root_sources
        assert ("pipeline", "node1") in root_sources
```

Update `TestDeltaTableDatabaseConfig` in `test_database_config.py` — the existing `test_to_config_includes_base_path` test **must be updated** (it now tests the wrong key):

```python
# REPLACE the existing test_to_config_includes_base_path with:
def test_to_config_includes_root_uri(self, tmp_path):
    db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
    config = db.to_config()
    assert config["root_uri"] == str(tmp_path / "delta_db")

def test_to_config_includes_base_path_tuple(self, tmp_path):
    db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
    config = db.to_config()
    assert config["base_path"] == []

def test_to_config_scoped_includes_base_path_tuple(self, tmp_path):
    db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
    scoped = db.at("pipeline", "v1")
    config = scoped.to_config()
    assert config["root_uri"] == str(tmp_path / "delta_db")
    assert config["base_path"] == ["pipeline", "v1"]

def test_round_trip_preserves_base_path(self, tmp_path):
    db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
    scoped = db.at("pipeline", "v1")
    config = scoped.to_config()
    restored = DeltaTableDatabase.from_config(config)
    assert restored.base_path == ("pipeline", "v1")
    assert isinstance(restored.base_path, tuple)
```

Also update the existing `test_round_trip` to use `root_uri`:
```python
def test_round_trip(self, tmp_path):
    db = DeltaTableDatabase(
        base_path=str(tmp_path / "delta_db"),
        batch_size=500,
        max_hierarchy_depth=5,
    )
    config = db.to_config()
    restored = DeltaTableDatabase.from_config(config)
    assert restored.to_config() == config
```
(This test doesn't check keys by name so it should still pass once the implementation is consistent.)

- [ ] **Step 2: Run to verify tests fail**

```bash
uv run pytest tests/test_databases/test_delta_table_database.py::TestAtMethod \
              tests/test_databases/test_database_config.py::TestDeltaTableDatabaseConfig -x -q
```

Expected: FAIL — `base_path` not scoped, config key still `"base_path"` for URI, etc.

- [ ] **Step 3: Implement `DeltaTableDatabase` — rename attributes**

In `src/orcapod/databases/delta_lake_databases.py`, update `__init__`:

```python
def __init__(
    self,
    base_path: str | Path | UPath,
    storage_options: dict[str, str] | None = None,
    create_base_path: bool = True,
    batch_size: int = 1000,
    max_hierarchy_depth: int = 10,
    allow_schema_evolution: bool = True,
    _path_prefix: tuple[str, ...] = (),
):
    self._root_uri, self._storage_options = parse_base_path(base_path, storage_options)
    self._is_cloud: bool = is_cloud_uri(self._root_uri)
    self._path_prefix = _path_prefix
    self.batch_size = batch_size
    self.max_hierarchy_depth = max_hierarchy_depth
    self.allow_schema_evolution = allow_schema_evolution

    if not self._is_cloud:
        # _local_root is the absolute filesystem root (for list_sources, mkdir, etc.)
        # NOTE: do NOT access self._local_root on cloud instances.
        self._local_root = Path(self._root_uri)
        if create_base_path:
            self._local_root.mkdir(parents=True, exist_ok=True)
        elif not self._local_root.exists():
            raise ValueError(
                f"Base path {self._local_root} does not exist and create_base_path=False"
            )

    self._delta_table_cache: dict[str, deltalake.DeltaTable] = {}
    self._pending_batches: dict[str, pa.Table] = {}
    self._pending_record_ids: dict[str, set[str]] = defaultdict(set)
    self._existing_ids_cache: dict[str, set[str]] = defaultdict(set)
    self._cache_dirty: dict[str, bool] = defaultdict(lambda: True)
```

Add `base_path` property (replace stub):
```python
@property
def base_path(self) -> tuple[str, ...]:
    """The current relative root of this database view (always () for root instances)."""
    return self._path_prefix
```

- [ ] **Step 4: Update `_get_table_uri` to apply prefix**

Replace the existing `_get_table_uri` method:

```python
def _get_table_uri(self, record_path: tuple[str, ...], create_dir: bool = False) -> str:
    """Get the URI for a given record path, incorporating base_path prefix.

    Args:
        record_path: Tuple of path components (relative to base_path).
        create_dir: If True, create the local directory (no-op for cloud paths).
    """
    full_path = self._path_prefix + record_path  # prefix applied once, here only
    if self._is_cloud:
        return self._root_uri.rstrip("/") + "/" + "/".join(full_path)
    else:
        path = self._local_root
        for subpath in full_path:
            path = path / self._sanitize_path_component(subpath)
        if create_dir:
            path.mkdir(parents=True, exist_ok=True)
        return str(path)
```

- [ ] **Step 5: Update `_validate_record_path` for combined depth**

Change the depth check line in `_validate_record_path`:

```python
if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
    raise ValueError(
        f"Source path depth {len(record_path)} exceeds maximum "
        f"{self.max_hierarchy_depth - len(self._path_prefix)} "
        f"(base_path uses {len(self._path_prefix)} components)"
    )
```

- [ ] **Step 6: Update `list_sources()` to scan from scoped root**

Replace `_scan(self.base_path, ())` at the end of `list_sources()` with:

```python
# Build the effective scoped root directory
scoped_root = self._local_root
for component in self._path_prefix:
    scoped_root = scoped_root / self._sanitize_path_component(component)

_scan(scoped_root, ())
return sources
```

Also update the `_scan` closure's depth guard to use relative depth (not combined):
```python
def _scan(current_path: Path, path_components: tuple[str, ...]) -> None:
    if len(path_components) >= self.max_hierarchy_depth:
        return
    ...
```

- [ ] **Step 7: Implement `at()` and update config**

Replace the stub `at()`:
```python
def at(self, *path_components: str) -> "DeltaTableDatabase":
    return DeltaTableDatabase(
        base_path=self._root_uri,
        storage_options=self._storage_options,
        batch_size=self.batch_size,
        max_hierarchy_depth=self.max_hierarchy_depth,
        allow_schema_evolution=self.allow_schema_evolution,
        _path_prefix=self._path_prefix + path_components,
    )
```

Replace `to_config`:
```python
def to_config(self) -> dict[str, Any]:
    """Serialize database configuration to a JSON-compatible dict."""
    config: dict[str, Any] = {
        "type": "delta_table",
        "root_uri": self._root_uri,           # renamed from "base_path"
        "base_path": list(self._path_prefix),  # new: relative prefix tuple
        "batch_size": self.batch_size,
        "max_hierarchy_depth": self.max_hierarchy_depth,
        "allow_schema_evolution": self.allow_schema_evolution,
    }
    if self._storage_options:
        config["storage_options"] = self._storage_options
    return config
```

Replace `from_config`:
```python
@classmethod
def from_config(cls, config: dict[str, Any]) -> "DeltaTableDatabase":
    """Reconstruct a DeltaTableDatabase from a config dict."""
    return cls(
        base_path=config["root_uri"],
        storage_options=config.get("storage_options"),
        create_base_path=True,
        batch_size=config.get("batch_size", 1000),
        max_hierarchy_depth=config.get("max_hierarchy_depth", 10),
        allow_schema_evolution=config.get("allow_schema_evolution", True),
        _path_prefix=tuple(config.get("base_path", [])),
    )
```

- [ ] **Step 8: Run the full database test suite**

```bash
uv run pytest tests/test_databases/ -q --ignore=tests/test_databases/test_delta_table_database_s3.py
```

Expected: all pass. The S3 tests are excluded because they require a live MinIO server.

- [ ] **Step 9: Commit**

```bash
git add src/orcapod/databases/delta_lake_databases.py \
        tests/test_databases/test_delta_table_database.py \
        tests/test_databases/test_database_config.py
git commit -m "feat(databases): implement at() and base_path for DeltaTableDatabase"
```

---

## Task 6: Full suite verification and PR

- [ ] **Step 1: Run the complete test suite**

```bash
uv run pytest tests/ -q --ignore=tests/test_databases/test_delta_table_database_s3.py \
              --ignore=tests/test_databases/test_spiraldb_connector_integration.py \
              --ignore=tests/test_databases/test_postgresql_connector_integration.py \
              --ignore=tests/test_databases/test_sqlite_connector_integration.py
```

Expected: all pass. (Integration tests are excluded as they require live external services.)

- [ ] **Step 2: Authenticate with GitHub and push branch**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
git checkout -b eywalker/eng-341-feat-database-path-contextualization-create-sub-scoped
git push -u origin eywalker/eng-341-feat-database-path-contextualization-create-sub-scoped
```

- [ ] **Step 3: Create PR**

```bash
gh pr create \
  --base dev \
  --title "feat(databases): add at() and base_path for sub-scoped database views (ENG-341)" \
  --body "$(cat <<'EOF'
## Summary

Implements ENG-341: database path contextualization — sub-scoped database views.

- Adds `at(*path_components)` and `base_path: tuple[str, ...]` to `ArrowDatabaseProtocol`
- All four backends implemented: `DeltaTableDatabase`, `InMemoryArrowDatabase`, `ConnectorArrowDatabase`, `NoOpArrowDatabase`
- `InMemoryArrowDatabase` and `ConnectorArrowDatabase` share underlying storage by reference; `DeltaTableDatabase` creates a fresh instance (filesystem is shared storage)
- `to_config`/`from_config` round-trips `base_path` for all backends
- `DeltaTableDatabase` config key rename: `"base_path"` (URI root) → `"root_uri"`; `"base_path"` now carries the scoping prefix tuple
- `_validate_record_path` updated to check combined `len(base_path) + len(record_path)` depth
- `list_sources()` on scoped `DeltaTableDatabase` instances scans from the scoped root

Closes ENG-341
EOF
)"
```

- [ ] **Step 4: Update Linear issue to In Progress → In Review**

```bash
# Use Linear MCP tool or leave for reviewer to handle
```
