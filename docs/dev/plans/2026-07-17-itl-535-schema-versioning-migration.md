# Schema Versioning + pdb/rdb v0→v1 Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce a first-class schema versioning framework for Orcapod's pipeline DB (pdb) and result DB (rdb), and provide a v0→v1 migration that converts ContentHash columns to binary, adds missing hash columns, and detects legacy tables with a hard stop.

**Architecture:** Schema version is encoded in the storage path tuple (e.g., `node_identity_path + ("pdb_v1",)`), so different schema versions live at physically separate paths with no interference. `FunctionJobNode` and `ResultCache` detect v0 tables on first DB access and raise `SchemaVersionError` unless the node has explicitly opted in via `NodeConfig.ignore_schema`. The migration package reads from v0, transforms, and writes to v1 paths using `skip_duplicates=True` for idempotency and concurrency safety.

**Tech Stack:** Python 3.11+, PyArrow, Delta Lake (`deltalake` 1.0.2), Typer (CLI), pytest + `uv run`

---

## File Map

**Created:**
- `src/orcapod/migrations/__init__.py` — public migration API
- `src/orcapod/migrations/types.py` — `MigrationResult` dataclass
- `src/orcapod/migrations/pipeline_db.py` — `migrate_pipeline_v0_to_v1()`, `migrate_node()`
- `src/orcapod/migrations/result_db.py` — `migrate_result_v0_to_v1()`
- `src/orcapod/cli/migrate.py` — `orcapod migrate pipeline-db` / `result-db` sub-commands
- `tests/test_migrations/__init__.py`
- `tests/test_migrations/test_migration_types.py`
- `tests/test_migrations/test_result_db.py`
- `tests/test_migrations/test_pipeline_db.py`
- `tests/test_migrations/test_golden.py`
- `tests/test_cli/test_migrate.py`
- `tests/fixtures/` — committed sample Delta Lake tables for each schema version

**Modified:**
- `src/orcapod/types.py` — add `ContentHash.from_prefixed_digest()`, `NodeConfig.ignore_schema`
- `src/orcapod/errors.py` — add `SchemaVersionError`
- `src/orcapod/system_constants.py` — add `PIPELINE_DB_SCHEMA_VERSION`, `RESULT_DB_SCHEMA_VERSION`
- `src/orcapod/protocols/database_protocols.py` — add `table_exists()` to `ArrowDatabaseProtocol`
- `src/orcapod/databases/delta_lake_databases.py` — implement `table_exists()`
- `src/orcapod/databases/in_memory_databases.py` — implement `table_exists()`
- `src/orcapod/databases/noop_database.py` — implement `table_exists()`
- `src/orcapod/databases/connector_arrow_database.py` — implement `table_exists()`
- `src/orcapod/databases/extension_aware_database.py` — implement `table_exists()`
- `src/orcapod/core/nodes/function_node.py` — path versioning, schema detection, binary ContentHash, remove ITL-508 guard
- `src/orcapod/core/result_cache.py` — path versioning, schema detection, binary ContentHash
- `src/orcapod/core/cached_function_pod.py` — binary variation hash serialization
- `src/orcapod/cli/__init__.py` — register `migrate` sub-group

---

## Task 1: `ContentHash.from_prefixed_digest()` classmethod

**Files:**
- Modify: `src/orcapod/types.py` (after `to_prefixed_digest()`, around line 648)
- Test: `tests/test_types.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_types.py`:

```python
class TestContentHashPrefixedDigest:
    """Tests for ContentHash.from_prefixed_digest() round-trip."""

    def test_roundtrip_sha256(self):
        h = ContentHash("sha256", bytes(range(32)))
        assert ContentHash.from_prefixed_digest(h.to_prefixed_digest()) == h

    def test_roundtrip_arrow_method(self):
        h = ContentHash("arrow_v2.1", b"\xde\xad\xbe\xef" * 8)
        result = ContentHash.from_prefixed_digest(h.to_prefixed_digest())
        assert result.method == "arrow_v2.1"
        assert result.digest == b"\xde\xad\xbe\xef" * 8

    def test_preserves_binary_digest_with_colon_bytes(self):
        # digest bytes that themselves contain a colon — only the first colon splits
        digest = b"abc:def"
        h = ContentHash("sha256", digest)
        assert ContentHash.from_prefixed_digest(h.to_prefixed_digest()) == h

    def test_inverse_of_to_prefixed_digest(self):
        """from_prefixed_digest is the exact inverse of to_prefixed_digest."""
        for method, digest in [
            ("sha256", b"\x00" * 32),
            ("md5", b"\xff" * 16),
            ("arrow_v2.1", b"\x01\x02\x03"),
        ]:
            h = ContentHash(method, digest)
            assert ContentHash.from_prefixed_digest(h.to_prefixed_digest()) == h
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_types.py::TestContentHashPrefixedDigest -v
```
Expected: `AttributeError: type object 'ContentHash' has no attribute 'from_prefixed_digest'`

- [ ] **Step 3: Implement the classmethod**

In `src/orcapod/types.py`, insert after `to_prefixed_digest()` (after line 648):

```python
    @classmethod
    def from_prefixed_digest(cls, data: bytes) -> "ContentHash":
        """Parse method-prefixed raw bytes back into a ``ContentHash``.

        Inverse of ``to_prefixed_digest()``.

        Args:
            data: Bytes in the form ``b"{method}:{raw_digest}"``, as
                produced by ``to_prefixed_digest()``.

        Returns:
            A new ``ContentHash`` instance.

        Raises:
            ValueError: If ``data`` does not contain a colon separator.
        """
        colon_idx = data.index(b":")
        method = data[:colon_idx].decode("ascii")
        digest = data[colon_idx + 1:]
        return cls(method=method, digest=digest)
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_types.py::TestContentHashPrefixedDigest -v
```
Expected: 4 PASSED

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/types.py tests/test_types.py
git commit -m "feat(types): add ContentHash.from_prefixed_digest() classmethod"
```

---

## Task 2: `SchemaVersionError` + `NodeConfig.ignore_schema`

**Files:**
- Modify: `src/orcapod/errors.py`
- Modify: `src/orcapod/types.py`
- Test: `tests/test_types.py` (extend `TestNodeConfig`)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_types.py` in `TestNodeConfig`:

```python
    def test_ignore_schema_defaults_to_none(self):
        config = NodeConfig()
        assert config.ignore_schema is None

    def test_ignore_schema_set(self):
        config = NodeConfig(ignore_schema=("v0",))
        assert config.ignore_schema == ("v0",)

    def test_merge_ignore_schema_none_in_other_self_wins(self):
        base = NodeConfig(ignore_schema=("v0",))
        other = NodeConfig()  # ignore_schema=None
        result = base.merge(other)
        assert result.ignore_schema == ("v0",)

    def test_merge_ignore_schema_other_overrides(self):
        base = NodeConfig(ignore_schema=("v0",))
        other = NodeConfig(ignore_schema=("v0", "v1"))
        result = base.merge(other)
        assert result.ignore_schema == ("v0", "v1")

    def test_merge_ignore_schema_empty_tuple_overrides(self):
        """Empty tuple () is a valid explicit value that overrides None."""
        base = NodeConfig(ignore_schema=("v0",))
        other = NodeConfig(ignore_schema=())
        result = base.merge(other)
        assert result.ignore_schema == ()
```

Also add to `tests/test_types.py` (new class):

```python
class TestSchemaVersionError:
    def test_is_exception(self):
        from orcapod.errors import SchemaVersionError
        err = SchemaVersionError("test message")
        assert isinstance(err, Exception)

    def test_message_preserved(self):
        from orcapod.errors import SchemaVersionError
        err = SchemaVersionError("Pipeline DB at v0 path")
        assert "v0" in str(err)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_types.py::TestNodeConfig::test_ignore_schema_defaults_to_none tests/test_types.py::TestSchemaVersionError -v
```
Expected: AttributeError on `ignore_schema`, ImportError on `SchemaVersionError`

- [ ] **Step 3: Add `SchemaVersionError` to `errors.py`**

Append to `src/orcapod/errors.py`. Note: `errors.py` has no `__all__`, so no export list needs updating.

```python
class SchemaVersionError(Exception):
    """Raised when an old schema version is detected at a database path.

    Occurs when a table exists at the unversioned (v0) path but not at the
    expected versioned path, and the node has not opted in to tolerating
    the old schema via ``NodeConfig.ignore_schema``.

    To suppress this error, set ``node.node_config = NodeConfig(ignore_schema=("v0",))``
    and re-run. The node will recompute all results from scratch rather than
    reading from the v0 cache.
    """
```

- [ ] **Step 4: Add `ignore_schema` to `NodeConfig` in `types.py`**

Find the `NodeConfig` dataclass (around line 347). Replace the existing class with:

```python
@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Per-node pipeline execution configuration.

    Attributes:
        is_result_ephemeral: ``None`` inherits the default (``False``).
            ``True`` writes new computation results to the pipeline-scoped
            ephemeral store instead of the persistent result database.
            Persistent cache hits are still served when available. Raises
            ``RuntimeError`` at execution time if ``True`` but no ephemeral
            store has been injected via ``set_ephemeral_store()``.
        ignore_schema: Tuple of schema version strings that this node will
            tolerate without raising ``SchemaVersionError``. ``None`` (default)
            means no old schema is tolerated — any detected v0 table raises
            ``SchemaVersionError``. Pass ``("v0",)`` to suppress the error
            and allow the node to recompute all results from scratch.
    """

    is_result_ephemeral: bool | None = None
    ignore_schema: tuple[str, ...] | None = None

    def merge(self, other: "NodeConfig") -> "NodeConfig":
        """Return a new ``NodeConfig`` with ``other``'s non-``None`` fields overriding self.

        ``None`` fields in ``other`` are treated as "not set" and leave
        self's value unchanged.

        Args:
            other: The ``NodeConfig`` whose non-``None`` fields take precedence.

        Returns:
            A new immutable ``NodeConfig``.

        Example:
            NodeConfig(is_result_ephemeral=True).merge(NodeConfig())
            # → NodeConfig(is_result_ephemeral=True)

            NodeConfig(is_result_ephemeral=True).merge(NodeConfig(is_result_ephemeral=False))
            # → NodeConfig(is_result_ephemeral=False)
        """
        return NodeConfig(
            is_result_ephemeral=(
                other.is_result_ephemeral
                if other.is_result_ephemeral is not None
                else self.is_result_ephemeral
            ),
            ignore_schema=(
                other.ignore_schema
                if other.ignore_schema is not None
                else self.ignore_schema
            ),
        )
```

- [ ] **Step 5: Run all tests to verify they pass**

```bash
uv run pytest tests/test_types.py::TestNodeConfig tests/test_types.py::TestSchemaVersionError -v
```
Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/errors.py src/orcapod/types.py tests/test_types.py
git commit -m "feat(schema): add SchemaVersionError and NodeConfig.ignore_schema"
```

---

## Task 3: `ArrowDatabaseProtocol.table_exists()` + all backends

**Files:**
- Modify: `src/orcapod/protocols/database_protocols.py`
- Modify: `src/orcapod/databases/delta_lake_databases.py`
- Modify: `src/orcapod/databases/in_memory_databases.py`
- Modify: `src/orcapod/databases/noop_database.py`
- Modify: `src/orcapod/databases/connector_arrow_database.py`
- Modify: `src/orcapod/databases/extension_aware_database.py`
- Test: `tests/test_databases/test_in_memory_database.py`, `tests/test_databases/test_delta_table_database.py`, `tests/test_databases/test_noop_database.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_databases/test_in_memory_database.py`:

```python
class TestTableExists:
    def test_returns_false_when_path_absent(self, db):
        assert db.table_exists(("a", "b")) is False

    def test_returns_true_after_adding_record(self, db):
        record = make_table(val=[1])
        db.add_record(("mypath",), b"id1", record)
        assert db.table_exists(("mypath",)) is True

    def test_returns_true_before_flush(self, db):
        """Pending (pre-flush) writes count as existing."""
        record = make_table(val=[1])
        db.add_record(("pending_path",), b"id1", record)
        # InMemoryArrowDatabase doesn't require explicit flush,
        # but verify pending_batches are seen.
        assert db.table_exists(("pending_path",)) is True

    def test_scoped_db_sees_correct_path(self, db):
        scoped = db.at("scope")
        record = make_table(val=[1])
        scoped.add_record(("sub",), b"id1", record)
        assert scoped.table_exists(("sub",)) is True
        assert db.table_exists(("scope", "sub")) is True
        assert db.table_exists(("sub",)) is False
```

Add to `tests/test_databases/test_delta_table_database.py`:

```python
class TestTableExists:
    def test_returns_false_when_path_absent(self, db):
        assert db.table_exists(("no", "table", "here")) is False

    def test_returns_true_after_write_and_flush(self, db):
        record = make_table(val=[1])
        db.add_record(("mypath",), b"id1", record)
        db.flush()
        assert db.table_exists(("mypath",)) is True

    def test_returns_false_for_different_path(self, db):
        record = make_table(val=[1])
        db.add_record(("path_a",), b"id1", record)
        db.flush()
        assert db.table_exists(("path_b",)) is False
```

Add to `tests/test_databases/test_noop_database.py`:

```python
class TestTableExists:
    def test_always_returns_false(self):
        from orcapod.databases.noop_database import NoOpArrowDatabase
        db = NoOpArrowDatabase()
        assert db.table_exists(("any", "path")) is False
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_databases/test_in_memory_database.py::TestTableExists tests/test_databases/test_delta_table_database.py::TestTableExists tests/test_databases/test_noop_database.py::TestTableExists -v
```
Expected: `AttributeError: 'InMemoryArrowDatabase' object has no attribute 'table_exists'`

- [ ] **Step 3: Add `table_exists` to `ArrowDatabaseProtocol`**

In `src/orcapod/protocols/database_protocols.py`, add after `flush()` (around line 67), and add `"table_exists"` to the existing `__all__` list at the bottom of the file (around line 168):

```python
    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a table exists at the given path, even if it has no rows.

        For Delta Lake backends: checks whether the ``_delta_log/`` directory
        exists at the resolved path. For in-memory backends: checks whether the
        path key is present in the internal store (committed or pending).

        Args:
            record_path: Path tuple identifying the table.

        Returns:
            ``True`` if the table has been created; ``False`` if the path is
            entirely absent.
        """
        ...
```

- [ ] **Step 4: Implement in `DeltaTableDatabase`**

In `src/orcapod/databases/delta_lake_databases.py`, add after `flush()`:

```python
    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if a Delta Lake table exists at the given path.

        Args:
            record_path: Path tuple identifying the table.

        Returns:
            ``True`` if the table's ``_delta_log/`` directory is present on
            the underlying storage; ``False`` otherwise.
        """
        return self._get_delta_table(record_path) is not None
```

- [ ] **Step 5: Implement in `InMemoryArrowDatabase`**

In `src/orcapod/databases/in_memory_databases.py`, add after `flush()`:

```python
    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if any records exist at the given path (committed or pending).

        Args:
            record_path: Path tuple identifying the table.

        Returns:
            ``True`` if the path key is present in committed or pending store.
        """
        record_key = "/".join(self._path_prefix + record_path)
        return record_key in self._tables or record_key in self._pending_batches
```

- [ ] **Step 6: Implement in `NoOpArrowDatabase`**

In `src/orcapod/databases/noop_database.py`, add after the existing `flush()` or similar no-op method:

```python
    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Always returns ``False`` — the no-op database never stores anything.

        Args:
            record_path: Path tuple identifying the table.

        Returns:
            Always ``False``.
        """
        return False
```

- [ ] **Step 7: Implement in `ConnectorArrowDatabase`**

In `src/orcapod/databases/connector_arrow_database.py`, add after `flush()`:

```python
    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Return ``True`` if the SQL table backing this path exists in the connector.

        Checks both committed (connector table) and pending (in-memory batch).

        Args:
            record_path: Path tuple identifying the table.

        Returns:
            ``True`` if the table exists in the connector or has pending writes.
        """
        record_key = self._get_record_key(record_path)
        if record_key in self._pending_batches:
            return True
        table_name = self._path_to_table_name(self._path_prefix + record_path)
        return table_name in self._connector.get_table_names()
```

- [ ] **Step 8: Implement in `ExtensionAwareDatabase`**

`ExtensionAwareDatabase` has no `_path_prefix` — it simply wraps `self._db` and passes all paths through unchanged. In `src/orcapod/databases/extension_aware_database.py`, add after the `flush()` delegation:

```python
    def table_exists(self, record_path: tuple[str, ...]) -> bool:
        """Delegate to the underlying database's ``table_exists()``.

        ``ExtensionAwareDatabase`` does not modify paths, so the call is
        passed through unchanged to the wrapped backend.

        Args:
            record_path: Path tuple identifying the table.

        Returns:
            Result of the underlying database's ``table_exists()`` call.
        """
        return self._db.table_exists(record_path)
```

- [ ] **Step 9: Run all tests to verify they pass**

```bash
uv run pytest tests/test_databases/test_in_memory_database.py::TestTableExists tests/test_databases/test_delta_table_database.py::TestTableExists tests/test_databases/test_noop_database.py::TestTableExists -v
```
Expected: all PASSED

- [ ] **Step 10: Run the full database test suite to check for regressions**

```bash
uv run pytest tests/test_databases/ -v --tb=short
```
Expected: all PASSED

- [ ] **Step 11: Commit**

```bash
git add src/orcapod/protocols/database_protocols.py \
        src/orcapod/databases/delta_lake_databases.py \
        src/orcapod/databases/in_memory_databases.py \
        src/orcapod/databases/noop_database.py \
        src/orcapod/databases/connector_arrow_database.py \
        src/orcapod/databases/extension_aware_database.py \
        tests/test_databases/test_in_memory_database.py \
        tests/test_databases/test_delta_table_database.py \
        tests/test_databases/test_noop_database.py
git commit -m "feat(db): add table_exists() to ArrowDatabaseProtocol and all backends"
```

---

## Task 4: Schema version constants

**Files:**
- Modify: `src/orcapod/system_constants.py`

- [ ] **Step 1: Add the constants**

In `src/orcapod/system_constants.py`, add after the existing module-level constants (after line 23, before the `SystemConstant` class):

```python
PIPELINE_DB_SCHEMA_VERSION = "pdb_v1"
RESULT_DB_SCHEMA_VERSION = "rdb_v1"
```

- [ ] **Step 2: Verify the constants import**

```bash
uv run python -c "from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, RESULT_DB_SCHEMA_VERSION; print(PIPELINE_DB_SCHEMA_VERSION, RESULT_DB_SCHEMA_VERSION)"
```
Expected: `pdb_v1 rdb_v1`

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/system_constants.py
git commit -m "feat(schema): add PIPELINE_DB_SCHEMA_VERSION and RESULT_DB_SCHEMA_VERSION constants"
```

---

## Task 5: `FunctionJobNode` — path versioning, schema detection, binary ContentHash, remove ITL-508 guard

**Files:**
- Modify: `src/orcapod/core/nodes/function_node.py`
- Test: `tests/test_core/nodes/test_function_node_iteration.py` (extend) and new `tests/test_core/nodes/test_schema_detection.py`

- [ ] **Step 1: Write failing tests for schema detection**

Create `tests/test_core/nodes/test_schema_detection.py`:

```python
"""Tests for FunctionJobNode old-schema detection and hard stop."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes.function_node import FunctionJobNode, _checked_pdb_paths
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SchemaVersionError
from orcapod.types import NodeConfig
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION


def _make_node(db: InMemoryArrowDatabase) -> FunctionJobNode:
    def double(x: int) -> int:
        return x * 2

    source = ArrowTableSource(
        pa.table({"id": [1, 2], "x": [10, 20]}), tag_columns=["id"]
    )
    pf = PythonDataFunction(double, output_keys="result")
    from orcapod.core.function_pod import FunctionPod
    pod = FunctionPod(source, pf)
    return FunctionJobNode(pod, pipeline_database=db)


@pytest.fixture(autouse=True)
def clear_checked_paths():
    """Clear the process-level cache between tests."""
    _checked_pdb_paths.clear()
    yield
    _checked_pdb_paths.clear()


class TestPdbSchemaDetection:
    def test_no_error_when_v1_table_exists(self, tmp_path):
        """v1 table present → schema check passes immediately."""
        db = InMemoryArrowDatabase()
        node = _make_node(db)
        v1_path = node.node_identity_path + (PIPELINE_DB_SCHEMA_VERSION,)
        # Seed the v1 path so it appears to exist
        db.add_record(v1_path, b"\x00" * 16, pa.table({"x": [1]}))
        # iter_data should not raise
        list(node.iter_data())  # no exception

    def test_no_error_when_both_paths_absent(self, tmp_path):
        """Fresh database (neither v0 nor v1 exists) → no error."""
        db = InMemoryArrowDatabase()
        node = _make_node(db)
        list(node.iter_data())  # no exception

    def test_raises_schema_version_error_when_v0_exists(self):
        """v1 absent, v0 has records, no ignore_schema → SchemaVersionError."""
        db = InMemoryArrowDatabase()
        node = _make_node(db)
        v0_path = node.node_identity_path
        # Seed the v0 path
        db.add_record(v0_path, b"\x00" * 16, pa.table({"x": [1]}))
        with pytest.raises(SchemaVersionError, match="v0"):
            list(node.iter_data())

    def test_no_error_when_v0_exists_and_ignored(self):
        """v1 absent, v0 has records, ignore_schema=('v0',) → proceeds."""
        db = InMemoryArrowDatabase()
        node = _make_node(db)
        node.node_config = NodeConfig(ignore_schema=("v0",))
        v0_path = node.node_identity_path
        db.add_record(v0_path, b"\x00" * 16, pa.table({"x": [1]}))
        list(node.iter_data())  # no exception

    def test_cache_hit_skips_db_call(self, mocker):
        """Second access to same v1 path skips table_exists entirely."""
        db = InMemoryArrowDatabase()
        node = _make_node(db)
        # Pre-populate the cache as if the check already ran
        v1_path = node.node_identity_path + (PIPELINE_DB_SCHEMA_VERSION,)
        _checked_pdb_paths.add(v1_path)
        spy = mocker.spy(db, "table_exists")
        list(node.iter_data())
        spy.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_core/nodes/test_schema_detection.py -v
```
Expected: `ImportError: cannot import name '_checked_pdb_paths'` or `AttributeError`

- [ ] **Step 3: Add imports and module-level cache to `function_node.py`**

At the top of `src/orcapod/core/nodes/function_node.py`, add to the imports section:

```python
from orcapod.errors import SchemaVersionError
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION
```

Just before the class definition (after existing module-level constants), add:

```python
# Process-level cache of v1 pipeline DB paths that have already been checked.
# Once a path is in this set, no further DB call is issued for it.
_checked_pdb_paths: set[tuple[str, ...]] = set()
```

- [ ] **Step 4: Add `_versioned_pipeline_path` property and `_ensure_pdb_schema()` method**

In `FunctionJobNode`, after the `node_identity_path` property (around line 315), add:

```python
    @property
    def _versioned_pipeline_path(self) -> tuple[str, ...]:
        """Pipeline DB path with the current schema version suffix appended.

        This is the path used for all actual pipeline DB reads and writes.
        ``node_identity_path`` (without the suffix) is retained for observer
        contextualization and error messages only.
        """
        return self.node_identity_path + (PIPELINE_DB_SCHEMA_VERSION,)

    def _ensure_pdb_schema(self) -> None:
        """Check for an old-schema (v0) pipeline DB on first access per path.

        Detection flow (runs at most once per v1 path per process):

        1. If the v1 path is already in ``_checked_pdb_paths`` → return immediately.
        2. If the v1 table exists → mark checked and return.
        3. If the v0 path (bare ``node_identity_path``) has a table:
           - ``"v0"`` in ``node_config.ignore_schema`` → log info, continue.
           - Otherwise → raise ``SchemaVersionError``.
        4. Neither path exists → fresh database, continue.
        5. Mark v1 path as checked.

        Raises:
            SchemaVersionError: If a v0 table is detected and not ignored.
        """
        if self._pipeline_database is None:
            return
        v1_path = self._versioned_pipeline_path
        if v1_path in _checked_pdb_paths:
            return
        if self._pipeline_database.table_exists(v1_path):
            _checked_pdb_paths.add(v1_path)
            return
        v0_path = self.node_identity_path
        if self._pipeline_database.table_exists(v0_path):
            _checked_pdb_paths.add(v1_path)
            ignore = self._node_config.ignore_schema or ()
            if "v0" not in ignore:
                raise SchemaVersionError(
                    f"Pipeline DB rows found at v0 schema path {v0_path!r}.\n"
                    "Run migration first:\n"
                    "  orcapod migrate pipeline-db <DB_PATH> <NODE_PATH>\n"
                    "To suppress this error and recompute all results instead, set:\n"
                    "  node.node_config = NodeConfig(ignore_schema=(\"v0\",))"
                )
            logger.info(
                "Pipeline DB v0 schema detected at %r — proceeding because "
                "ignore_schema=%r",
                v0_path,
                ignore,
            )
        _checked_pdb_paths.add(v1_path)
```

- [ ] **Step 5: Update `_fetch_joined_records()` to call `_ensure_pdb_schema()` and use the versioned path**

In `_fetch_joined_records()` (around line 1818), at the very start of the method body, add:

```python
        self._ensure_pdb_schema()
```

Then replace every occurrence of `self.node_identity_path` passed to `self._pipeline_database.*` calls with `self._versioned_pipeline_path`. These calls include:
- `self._pipeline_database.get_all_records(self.node_identity_path, ...)`
- `self._pipeline_database.get_records_by_ids(self.node_identity_path, ...)`
- `self._pipeline_database.get_records_with_column_value(self.node_identity_path, ...)`

Do NOT replace occurrences used for observer contextualization or error messages (e.g., `obs.contextualize(*self.node_identity_path)`, `node_identity_path=self.node_identity_path`).

- [ ] **Step 6: Update `add_pipeline_record()` — remove ITL-508 guard, use versioned path, binary ContentHash**

In `add_pipeline_record()` (around line 1583):

**a) Remove the ITL-508 guard** (lines ~1629–1644). Delete this block entirely:

```python
        # Guard against pre-ITL-508 pipeline DB records that are missing the new
        # versioning columns. If such records exist, fail fast with a clear message
        # ...
        _all_existing = self._pipeline_database.get_all_records(self.node_identity_path)
        if _all_existing is not None and _all_existing.num_rows > 0:
            _missing = [...]
            if _missing:
                raise ValueError(...)
```

**b) Replace `self.node_identity_path` with `self._versioned_pipeline_path`** in all DB calls within this method:
- `get_records_with_column_value(self.node_identity_path, ...)` → `self._versioned_pipeline_path`
- `add_record(self.node_identity_path, ...)` → `self._versioned_pipeline_path`

**c) Change ContentHash columns in `meta_table` from `large_string` to `large_binary`**:

```python
        meta_table = pa.table(
            {
                constants.DATA_RECORD_ID: pa.array(
                    [data_record_id.bytes], type=pa.large_binary()
                ),
                constants.NODE_CONTENT_HASH_COL: pa.array(
                    [self.content_hash().to_prefixed_digest()], type=pa.large_binary()
                ),
                constants.INPUT_DATA_HASH_COL: pa.array(
                    [input_data.content_hash().to_prefixed_digest()], type=pa.large_binary()
                ),
                constants.OUTPUT_DATA_HASH_COL: pa.array(
                    [output_data_hash.to_prefixed_digest() if output_data_hash is not None else None],
                    type=pa.large_binary(),
                ),
                f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(
                    [input_data.data_context_key], type=pa.large_string()
                ),
                f"{constants.META_PREFIX}computed": pa.array(
                    [computed], type=pa.bool_()
                ),
                constants.IS_EPHEMERAL_COL: pa.array(
                    [is_ephemeral], type=pa.bool_()
                ),
                _PIPELINE_BASE_ENTRY_ID_COL: pa.array(
                    [base_entry_id], type=pa.large_binary()
                ),
                _PIPELINE_RECOMPUTATION_INDEX_COL: pa.array(
                    [new_index], type=pa.int32()
                ),
            }
        )
```

- [ ] **Step 7: Update `_fetch_joined_records()` to read ContentHash as binary**

In `_fetch_joined_records()`, wherever `__node_content_hash`, `__input_data_hash`, or `__output_data_hash` are read from the pipeline DB table and passed as `ContentHash` objects, use `ContentHash.from_prefixed_digest(bytes_value)` instead of `ContentHash.from_string(string_value)`.

Search for uses of `NODE_CONTENT_HASH_COL`, `INPUT_DATA_HASH_COL`, `OUTPUT_DATA_HASH_COL` in `_fetch_joined_records()` and update accordingly. The column values will now be `bytes` (PyArrow `large_binary`), not strings.

- [ ] **Step 8: Run schema detection tests**

```bash
uv run pytest tests/test_core/nodes/test_schema_detection.py -v
```
Expected: all PASSED

- [ ] **Step 9: Run the full FunctionJobNode test suite**

```bash
uv run pytest tests/test_core/nodes/ -v --tb=short
```
Expected: all PASSED (no regressions)

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/core/nodes/function_node.py \
        tests/test_core/nodes/test_schema_detection.py
git commit -m "feat(schema): FunctionJobNode path versioning, binary ContentHash, schema detection"
```

---

## Task 6: `ResultCache` — path versioning, schema detection, binary ContentHash

**Files:**
- Modify: `src/orcapod/core/result_cache.py`
- Test: `tests/test_core/test_result_cache.py` (extend)

- [ ] **Step 1: Write failing tests for rdb schema detection**

Add to `tests/test_core/test_result_cache.py`:

```python
from orcapod.core.result_cache import _checked_rdb_paths
from orcapod.errors import SchemaVersionError
from orcapod.types import NodeConfig, ContentHash
from orcapod.system_constants import RESULT_DB_SCHEMA_VERSION


@pytest.fixture(autouse=True)
def clear_rdb_checked_paths():
    _checked_rdb_paths.clear()
    yield
    _checked_rdb_paths.clear()


class TestRdbSchemaDetection:
    def _make_cache_with_db(self, db, record_path=("test",)):
        return ResultCache(
            result_database=db,
            record_path=record_path,
        )

    def test_no_error_fresh_db(self):
        db = InMemoryArrowDatabase()
        cache = self._make_cache_with_db(db)
        result = cache.lookup(make_input_data())
        assert result is None  # no error

    def test_raises_when_v0_exists(self):
        db = InMemoryArrowDatabase()
        v0_path = ("test",)
        db.add_record(v0_path, b"\x00" * 16, pa.table({"x": [1]}))
        cache = self._make_cache_with_db(db, record_path=v0_path)
        with pytest.raises(SchemaVersionError, match="v0"):
            cache.lookup(make_input_data())

    def test_no_error_when_ignored(self):
        db = InMemoryArrowDatabase()
        v0_path = ("test",)
        db.add_record(v0_path, b"\x00" * 16, pa.table({"x": [1]}))
        cache = self._make_cache_with_db(db, record_path=v0_path)
        cache.set_ignore_schema(("v0",))
        result = cache.lookup(make_input_data())
        assert result is None

    def test_no_error_when_v1_exists(self):
        db = InMemoryArrowDatabase()
        v0_path = ("test",)
        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        # Seed v0 (would trigger error) AND v1 (should suppress check)
        db.add_record(v0_path, b"\x00" * 16, pa.table({"x": [1]}))
        db.add_record(v1_path, b"\x01" * 16, pa.table({"x": [2]}))
        cache = self._make_cache_with_db(db, record_path=v0_path)
        result = cache.lookup(make_input_data())
        assert result is None  # v1 exists, no error
```

Note: `make_input_data()` is a helper returning a simple `Data` object — add it near the top of the test file:

```python
def make_input_data():
    from orcapod.core.datagrams import Data
    return Data(pa.table({"val": pa.array([42], type=pa.int64())}))
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_core/test_result_cache.py::TestRdbSchemaDetection -v
```
Expected: `ImportError: cannot import name '_checked_rdb_paths'`

- [ ] **Step 3: Add imports and module-level cache + `set_ignore_schema` to `ResultCache`**

At the top of `src/orcapod/core/result_cache.py`, add:

```python
from orcapod.errors import SchemaVersionError
from orcapod.system_constants import RESULT_DB_SCHEMA_VERSION
```

After the imports (module level), add:

```python
# Process-level cache of v1 result DB paths that have already been checked.
_checked_rdb_paths: set[tuple[str, ...]] = set()
```

In the `ResultCache` class, add `_ignore_schema` attribute and a setter:

```python
    def __init__(
        self,
        result_database: ArrowDatabaseProtocol,
        record_path: tuple[str, ...],
        auto_flush: bool = True,
    ) -> None:
        self._result_database = result_database
        self._record_path = record_path
        self._auto_flush = auto_flush
        self._ignore_schema: tuple[str, ...] | None = None  # NEW

    def set_ignore_schema(self, ignore_schema: tuple[str, ...] | None) -> None:
        """Set which old schema versions to tolerate without raising ``SchemaVersionError``.

        Args:
            ignore_schema: Tuple of schema version strings to tolerate (e.g. ``("v0",)``),
                or ``None`` to use the default (raise on any old schema).
        """
        self._ignore_schema = ignore_schema
```

- [ ] **Step 4: Add `_versioned_record_path` property and `_ensure_rdb_schema()` method**

In `ResultCache`:

```python
    @property
    def _versioned_record_path(self) -> tuple[str, ...]:
        """Result DB path with the current schema version suffix appended."""
        return self._record_path + (RESULT_DB_SCHEMA_VERSION,)

    def _ensure_rdb_schema(self) -> None:
        """Check for an old-schema (v0) result DB on first access per path.

        Same detection flow as ``FunctionJobNode._ensure_pdb_schema()``:
        checks the v1 path first (cheap set lookup), then falls back to
        calling ``table_exists()`` on both v1 and v0 paths.

        Raises:
            SchemaVersionError: If a v0 table is detected and not ignored.
        """
        v1_path = self._versioned_record_path
        if v1_path in _checked_rdb_paths:
            return
        if self._result_database.table_exists(v1_path):
            _checked_rdb_paths.add(v1_path)
            return
        v0_path = self._record_path
        if self._result_database.table_exists(v0_path):
            _checked_rdb_paths.add(v1_path)
            ignore = self._ignore_schema or ()
            if "v0" not in ignore:
                raise SchemaVersionError(
                    f"Result DB rows found at v0 schema path {v0_path!r}.\n"
                    "Run migration first:\n"
                    "  orcapod migrate result-db <DB_PATH> <RECORD_PATH>\n"
                    "To suppress this error and recompute all results instead, set:\n"
                    "  node.node_config = NodeConfig(ignore_schema=(\"v0\",))"
                )
            logger.info(
                "Result DB v0 schema detected at %r — proceeding because "
                "ignore_schema=%r",
                v0_path,
                ignore,
            )
        _checked_rdb_paths.add(v1_path)
```

- [ ] **Step 5: Update `lookup()` — call detection + use versioned path + binary hash**

In `lookup()`:

At the top: `self._ensure_rdb_schema()`

Change the constraint dict to use binary:
```python
        constraints: dict[str, Any] = {
            constants.INPUT_DATA_HASH_COL: input_data.content_hash().to_prefixed_digest(),
        }
```

Change `self._record_path` to `self._versioned_record_path` in the `get_records_with_column_value()` call.

- [ ] **Step 6: Update `store()` — use versioned path + binary ContentHash columns**

In `store()`:

Change `self._record_path` to `self._versioned_record_path` in the `add_record()` call.

Change the input hash column from `large_string` to `large_binary`:
```python
        data_table = data_table.add_column(
            0,
            constants.INPUT_DATA_HASH_COL,
            pa.array(
                [input_data.content_hash().to_prefixed_digest()], type=pa.large_binary()
            ),
        )
```

After the variation columns are added (inside the `for name in var_table.column_names:` loop or after), convert the ContentHash variation columns to binary. Add this block after all variation columns are inserted:

```python
        # Convert ContentHash variation columns from string to binary (v1 schema).
        _HASH_VAR_COLS = {
            f"{constants.PF_VARIATION_PREFIX}function_signature_hash",
            f"{constants.PF_VARIATION_PREFIX}function_content_hash",
        }
        for col_name in _HASH_VAR_COLS:
            if col_name in data_table.column_names:
                col_idx = data_table.column_names.index(col_name)
                string_vals = data_table.column(col_name).to_pylist()
                binary_vals = pa.array(
                    [
                        ContentHash.from_string(s).to_prefixed_digest() if s is not None else None
                        for s in string_vals
                    ],
                    type=pa.large_binary(),
                )
                data_table = data_table.set_column(col_idx, col_name, binary_vals)
```

Add the import at the top of `result_cache.py`:
```python
from orcapod.types import ContentHash
```

- [ ] **Step 7: Run schema detection and result cache tests**

```bash
uv run pytest tests/test_core/test_result_cache.py -v --tb=short
```
Expected: all PASSED

- [ ] **Step 8: Commit**

```bash
git add src/orcapod/core/result_cache.py tests/test_core/test_result_cache.py
git commit -m "feat(schema): ResultCache path versioning, binary ContentHash, schema detection"
```

---

## Task 7: `CachedFunctionPod` — wire `ignore_schema` from `NodeConfig` to `ResultCache`

**Files:**
- Modify: `src/orcapod/core/cached_function_pod.py`

The `ignore_schema` setting lives on `FunctionJobNode.node_config`. `CachedFunctionPod` is constructed by `FunctionJobNode` and holds a `ResultCache`. When the node config changes (via `.node_config = NodeConfig(ignore_schema=...)`), the cache needs to receive the updated value.

- [ ] **Step 1: Add `set_ignore_schema` call when `node_config` is set**

In `src/orcapod/core/nodes/function_node.py`, update the `node_config` setter (around line 786):

```python
    @node_config.setter
    def node_config(self, value: NodeConfig) -> None:
        self._node_config = value
        # Propagate ignore_schema to the result cache so it uses the same policy.
        if self._cached_function_pod is not None:
            self._cached_function_pod.set_ignore_schema(value.ignore_schema)
        if self._ephemeral_cached_pod is not None:
            self._ephemeral_cached_pod.set_ignore_schema(value.ignore_schema)
```

- [ ] **Step 2: Add `set_ignore_schema` method to `CachedFunctionPod`**

In `src/orcapod/core/cached_function_pod.py`:

```python
    def set_ignore_schema(self, ignore_schema: tuple[str, ...] | None) -> None:
        """Propagate ``ignore_schema`` setting to the underlying ``ResultCache``.

        Args:
            ignore_schema: Tuple of schema version strings to tolerate, or ``None``.
        """
        self._cache.set_ignore_schema(ignore_schema)
```

- [ ] **Step 3: Run integration test to confirm node_config propagates**

```bash
uv run pytest tests/test_core/ -v --tb=short -q
```
Expected: all PASSED

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/cached_function_pod.py src/orcapod/core/nodes/function_node.py
git commit -m "feat(schema): propagate NodeConfig.ignore_schema to ResultCache via CachedFunctionPod"
```

---

## Task 8: Migration package — `types.py` and `result_db.py`

**Files:**
- Create: `src/orcapod/migrations/__init__.py`
- Create: `src/orcapod/migrations/types.py`
- Create: `src/orcapod/migrations/result_db.py`
- Create: `tests/test_migrations/__init__.py`
- Create: `tests/test_migrations/test_migration_types.py`
- Create: `tests/test_migrations/test_result_db.py`

- [ ] **Step 1: Write failing tests for `MigrationResult` and `migrate_result_v0_to_v1()`**

Create `tests/test_migrations/__init__.py` (empty).

Create `tests/test_migrations/test_migration_types.py`:

```python
"""Tests for MigrationResult dataclass."""
from __future__ import annotations

from orcapod.migrations.types import MigrationResult


class TestMigrationResult:
    def test_fields(self):
        r = MigrationResult(
            rows_total=100,
            rows_migrated=95,
            rows_skipped=4,
            rows_unresolvable=1,
            elapsed_s=3.14,
            dry_run=False,
        )
        assert r.rows_total == 100
        assert r.rows_migrated == 95
        assert r.rows_skipped == 4
        assert r.rows_unresolvable == 1
        assert r.elapsed_s == 3.14
        assert r.dry_run is False

    def test_dry_run_field(self):
        r = MigrationResult(
            rows_total=10,
            rows_migrated=0,
            rows_skipped=0,
            rows_unresolvable=0,
            elapsed_s=0.1,
            dry_run=True,
        )
        assert r.dry_run is True
```

Create `tests/test_migrations/test_result_db.py`:

```python
"""Tests for migrate_result_v0_to_v1()."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases import InMemoryArrowDatabase
from orcapod.migrations.result_db import migrate_result_v0_to_v1
from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import RESULT_DB_SCHEMA_VERSION
from orcapod.types import ContentHash


def _make_v0_rdb_row(input_hash: ContentHash, sig_hash: ContentHash, content_hash: ContentHash) -> pa.Table:
    """Build a single v0 rdb row in the old large_string format."""
    return pa.table({
        "__input_data_hash": pa.array([input_hash.to_string()], type=pa.large_string()),
        "__pf_var_function_name": pa.array(["my_func"], type=pa.large_string()),
        "__pf_var_function_signature_hash": pa.array([sig_hash.to_string()], type=pa.large_string()),
        "__pf_var_function_content_hash": pa.array([content_hash.to_string()], type=pa.large_string()),
        "__pf_var_git_hash": pa.array(["abc123"], type=pa.large_string()),
        "__pf_exec_executor_type": pa.array(["local"], type=pa.large_string()),
        "__pf_exec_python_version": pa.array(["3.11"], type=pa.large_string()),
        "__pod_ts": pa.array([0], type=pa.timestamp("us", tz="UTC")),
        "result": pa.array([42], type=pa.int64()),
    })


_INPUT_HASH = ContentHash("sha256", bytes(range(32)))
_SIG_HASH = ContentHash("sha256", bytes(range(1, 33)))
_CONTENT_HASH = ContentHash("sha256", bytes(range(2, 34)))


class TestMigrateResultV0ToV1:
    def test_happy_path_migrates_all_rows(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_records(v0_path, row, skip_duplicates=False)
        db.flush()

        result = migrate_result_v0_to_v1(db, v0_path, progress=False)

        assert result.rows_total == 1
        assert result.rows_migrated == 1
        assert result.rows_skipped == 0
        assert result.rows_unresolvable == 0
        assert result.dry_run is False

    def test_v1_row_has_binary_hash_columns(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_records(v0_path, row, skip_duplicates=False)
        db.flush()

        migrate_result_v0_to_v1(db, v0_path, progress=False)

        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        assert v1_table.schema.field("__input_data_hash").type == pa.large_binary()
        assert v1_table.schema.field("__pf_var_function_signature_hash").type == pa.large_binary()
        assert v1_table.schema.field("__pf_var_function_content_hash").type == pa.large_binary()
        # git_hash stays as string
        assert v1_table.schema.field("__pf_var_git_hash").type == pa.large_string()

    def test_v1_binary_values_decode_correctly(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_records(v0_path, row, skip_duplicates=False)
        db.flush()

        migrate_result_v0_to_v1(db, v0_path, progress=False)

        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        row_dict = v1_table.to_pylist()[0]
        assert ContentHash.from_prefixed_digest(bytes(row_dict["__input_data_hash"])) == _INPUT_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict["__pf_var_function_signature_hash"])) == _SIG_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict["__pf_var_function_content_hash"])) == _CONTENT_HASH

    def test_idempotent_second_run_skips_all(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_records(v0_path, row, skip_duplicates=False)
        db.flush()

        migrate_result_v0_to_v1(db, v0_path, progress=False)
        result2 = migrate_result_v0_to_v1(db, v0_path, progress=False)

        assert result2.rows_migrated == 0
        assert result2.rows_skipped == 1

    def test_dry_run_writes_nothing(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_records(v0_path, row, skip_duplicates=False)
        db.flush()

        result = migrate_result_v0_to_v1(db, v0_path, dry_run=True, progress=False)

        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        assert not db.table_exists(v1_path)
        assert result.dry_run is True
        assert result.rows_total == 1
        assert result.rows_migrated == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_migrations/ -v
```
Expected: `ModuleNotFoundError: No module named 'orcapod.migrations'`

- [ ] **Step 3: Create `src/orcapod/migrations/types.py`**

```python
"""Migration result types for Orcapod schema migrations."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class MigrationResult:
    """Summary of a completed schema migration run.

    Attributes:
        rows_total: Total rows found at the v0 path.
        rows_migrated: Rows successfully written to the v1 path.
        rows_skipped: Rows already present at the v1 path (idempotent re-run).
        rows_unresolvable: Rows whose result data was unreachable (e.g. ephemeral
            result expired); ``__output_data_hash`` written as ``None`` for these.
        elapsed_s: Wall-clock seconds elapsed during the migration.
        dry_run: ``True`` if the run was a dry run (no writes performed).
    """

    rows_total: int
    rows_migrated: int
    rows_skipped: int
    rows_unresolvable: int
    elapsed_s: float
    dry_run: bool
```

- [ ] **Step 4: Create `src/orcapod/migrations/result_db.py`**

```python
"""v0 → v1 migration for the Orcapod result DB (rdb)."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import RESULT_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)

# rdb columns that hold orcapod-produced ContentHash values and must be
# converted from large_string (v0) to large_binary (v1).
_HASH_COLS = (
    constants.INPUT_DATA_HASH_COL,
    f"{constants.PF_VARIATION_PREFIX}function_signature_hash",
    f"{constants.PF_VARIATION_PREFIX}function_content_hash",
)


def migrate_result_v0_to_v1(
    result_db: "ArrowDatabaseProtocol",
    result_path: tuple[str, ...],
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult:
    """Migrate a result DB table from v0 schema to v1 schema.

    Reads records from ``result_path`` (v0, no suffix), converts all
    orcapod-produced ``ContentHash`` columns from ``large_string`` to
    ``large_binary`` (using ``ContentHash.to_prefixed_digest()``), and writes
    the transformed rows to ``result_path + ("rdb_v1",)``.

    Rows already present at the v1 path are skipped (idempotent re-runs).

    Args:
        result_db: The database containing the v0 table.
        result_path: Bare v0 path tuple (no ``rdb_v1`` suffix).
        dry_run: If ``True``, read and count rows but write nothing.
        batch_size: Number of rows to process per batch.
        progress: If ``True``, log progress at INFO level.

    Returns:
        ``MigrationResult`` summarising the run.
    """
    v1_path = result_path + (RESULT_DB_SCHEMA_VERSION,)
    start = time.monotonic()

    v0_table = result_db.get_all_records(result_path)
    if v0_table is None or v0_table.num_rows == 0:
        logger.info("No v0 records found at %r — nothing to migrate.", result_path)
        return MigrationResult(
            rows_total=0,
            rows_migrated=0,
            rows_skipped=0,
            rows_unresolvable=0,
            elapsed_s=time.monotonic() - start,
            dry_run=dry_run,
        )

    rows_total = v0_table.num_rows
    rows_migrated = 0
    rows_skipped = 0

    # Collect IDs already at v1 for idempotency check.
    v1_existing = result_db.get_all_records(v1_path)
    existing_ids: set[bytes] = set()
    if v1_existing is not None and "__record_id" in v1_existing.schema.names:
        existing_ids = {bytes(r) for r in v1_existing.column("__record_id").to_pylist() if r is not None}

    if progress:
        logger.info("migrate_result_v0_to_v1: found %d rows at v0 path %r", rows_total, result_path)

    # Process in batches.
    for batch_start in range(0, rows_total, batch_size):
        batch = v0_table.slice(batch_start, batch_size)

        # Filter rows already at v1.
        if existing_ids and "__record_id" in batch.schema.names:
            mask = pa.array(
                [bytes(rid) not in existing_ids for rid in batch.column("__record_id").to_pylist()],
                type=pa.bool_(),
            )
            new_rows = batch.filter(mask)
            rows_skipped += batch.num_rows - new_rows.num_rows
        else:
            new_rows = batch

        if new_rows.num_rows == 0:
            continue

        # Convert hash columns from string → binary.
        transformed = _convert_hash_cols(new_rows)

        if not dry_run:
            result_db.add_records(v1_path, transformed, skip_duplicates=True)

        rows_migrated += new_rows.num_rows
        if progress:
            logger.info(
                "migrate_result_v0_to_v1: %d/%d rows processed",
                min(batch_start + batch_size, rows_total),
                rows_total,
            )

    if not dry_run:
        result_db.flush()

    elapsed = time.monotonic() - start
    return MigrationResult(
        rows_total=rows_total,
        rows_migrated=rows_migrated,
        rows_skipped=rows_skipped,
        rows_unresolvable=0,  # rdb migration has no unresolvable rows
        elapsed_s=elapsed,
        dry_run=dry_run,
    )


def _convert_hash_cols(table: pa.Table) -> pa.Table:
    """Convert all v0 large_string ContentHash columns to v1 large_binary.

    Args:
        table: Arrow table with v0-format hash columns.

    Returns:
        New table with hash columns converted to ``large_binary``.
    """
    for col_name in _HASH_COLS:
        if col_name not in table.schema.names:
            continue
        col_idx = table.schema.names.index(col_name)
        string_vals = table.column(col_name).to_pylist()
        binary_vals = pa.array(
            [
                ContentHash.from_string(s).to_prefixed_digest() if s is not None else None
                for s in string_vals
            ],
            type=pa.large_binary(),
        )
        table = table.set_column(col_idx, col_name, binary_vals)
    return table
```

- [ ] **Step 5: Create `src/orcapod/migrations/__init__.py`** (stub — full public API added in Task 9)

```python
"""Orcapod schema migration utilities."""
from orcapod.migrations.types import MigrationResult
from orcapod.migrations.result_db import migrate_result_v0_to_v1

__all__ = [
    "MigrationResult",
    "migrate_result_v0_to_v1",
]
```

- [ ] **Step 6: Run the tests**

```bash
uv run pytest tests/test_migrations/test_migration_types.py tests/test_migrations/test_result_db.py -v
```
Expected: all PASSED

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/migrations/ tests/test_migrations/
git commit -m "feat(migrations): add MigrationResult dataclass and migrate_result_v0_to_v1()"
```

---

## Task 9: Migration package — `pipeline_db.py` + `migrate_node()` + update `__init__.py`

**Files:**
- Create: `src/orcapod/migrations/pipeline_db.py`
- Modify: `src/orcapod/migrations/__init__.py`
- Create: `tests/test_migrations/test_pipeline_db.py`

- [ ] **Step 1: Write failing tests**

Create `tests/test_migrations/test_pipeline_db.py`:

```python
"""Tests for migrate_pipeline_v0_to_v1()."""
from __future__ import annotations

import uuid
import pyarrow as pa
import pytest

from orcapod.databases import InMemoryArrowDatabase
from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1
from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, RESULT_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash


_NODE_HASH = ContentHash("sha256", bytes(range(32)))
_INPUT_HASH = ContentHash("sha256", bytes(range(1, 33)))
_OUTPUT_HASH = ContentHash("sha256", bytes(range(2, 34)))


def _make_data_id() -> bytes:
    return uuid.uuid7().bytes


def _write_v0_rdb_row(db: InMemoryArrowDatabase, rdb_path: tuple, data_id: bytes, input_hash: ContentHash) -> None:
    """Write a minimal v0 rdb row that the pdb migration can look up."""
    row = pa.table({
        "__input_data_hash": pa.array([input_hash.to_string()], type=pa.large_string()),
        "__pf_var_function_name": pa.array(["fn"], type=pa.large_string()),
        "__pf_var_function_signature_hash": pa.array(["sha256:aabb"], type=pa.large_string()),
        "__pf_var_function_content_hash": pa.array(["sha256:ccdd"], type=pa.large_string()),
        "__pf_var_git_hash": pa.array(["abc"], type=pa.large_string()),
        "__pf_exec_executor_type": pa.array(["local"], type=pa.large_string()),
        "__pf_exec_python_version": pa.array(["3.11"], type=pa.large_string()),
        "__pod_ts": pa.array([0], type=pa.timestamp("us", tz="UTC")),
        "result": pa.array([99], type=pa.int64()),
    })
    db.add_record(rdb_path, data_id, row)
    db.flush()


def _write_v0_pdb_row(db: InMemoryArrowDatabase, pdb_path: tuple, data_id: bytes) -> None:
    """Write a minimal v0 pdb row."""
    row = pa.table({
        constants.DATA_RECORD_ID: pa.array([data_id], type=pa.large_binary()),
        constants.NODE_CONTENT_HASH_COL: pa.array([_NODE_HASH.to_string()], type=pa.large_string()),
        constants.INPUT_DATA_HASH_COL: pa.array([_INPUT_HASH.to_string()], type=pa.large_string()),
        constants.OUTPUT_DATA_HASH_COL: pa.array([_OUTPUT_HASH.to_string()], type=pa.large_string()),
        f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(["ctx"], type=pa.large_string()),
        f"{constants.META_PREFIX}computed": pa.array([True], type=pa.bool_()),
        constants.IS_EPHEMERAL_COL: pa.array([False], type=pa.bool_()),
        "__pipeline_base_entry_id": pa.array([b"\x00" * 16], type=pa.large_binary()),
        "__pipeline_recomputation_index": pa.array([0], type=pa.int32()),
    })
    record_id = b"\x01" * 32
    db.add_record(pdb_path, record_id, row)
    db.flush()


class TestMigratePipelineV0ToV1:
    def test_happy_path_full_backfill(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)
        data_id = _make_data_id()

        _write_v0_rdb_row(db, rdb_path, data_id, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, data_id)

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, progress=False
        )

        assert result.rows_total == 1
        assert result.rows_migrated == 1
        assert result.rows_unresolvable == 0
        assert result.rows_skipped == 0

    def test_v1_hash_columns_are_binary(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)
        data_id = _make_data_id()

        _write_v0_rdb_row(db, rdb_path, data_id, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, data_id)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        assert v1_table.schema.field(constants.NODE_CONTENT_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.INPUT_DATA_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.OUTPUT_DATA_HASH_COL).type == pa.large_binary()

    def test_unresolvable_row_written_with_null_output_hash(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)
        data_id = _make_data_id()

        # Write a pdb row whose data_id does NOT exist in rdb (ephemeral expired)
        _write_v0_pdb_row(db, pdb_path, data_id)
        # Do NOT write rdb row

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, progress=False
        )

        assert result.rows_unresolvable == 1
        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        output_hash_col = v1_table.column(constants.OUTPUT_DATA_HASH_COL)
        assert output_hash_col.to_pylist()[0] is None

    def test_idempotent(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)
        data_id = _make_data_id()

        _write_v0_rdb_row(db, rdb_path, data_id, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, data_id)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)
        result2 = migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        assert result2.rows_migrated == 0
        assert result2.rows_skipped == 1

    def test_dry_run_writes_nothing(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)
        data_id = _make_data_id()

        _write_v0_rdb_row(db, rdb_path, data_id, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, data_id)

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, dry_run=True, progress=False
        )

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        assert not db.table_exists(v1_path)
        assert result.dry_run is True
        assert result.rows_total == 1
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_migrations/test_pipeline_db.py -v
```
Expected: `ImportError: cannot import name 'migrate_pipeline_v0_to_v1' from 'orcapod.migrations.pipeline_db'`

- [ ] **Step 3: Create `src/orcapod/migrations/pipeline_db.py`**

```python
"""v0 → v1 migration for the Orcapod pipeline DB (pdb)."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, RESULT_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.core.nodes.function_node import FunctionJobNode

logger = logging.getLogger(__name__)


def migrate_pipeline_v0_to_v1(
    pipeline_db: "ArrowDatabaseProtocol",
    pipeline_path: tuple[str, ...],
    result_db: "ArrowDatabaseProtocol",
    result_path: tuple[str, ...],
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult:
    """Migrate a pipeline DB table from v0 schema to v1 schema.

    Reads records from ``pipeline_path`` (v0, no suffix), converts
    ``__node_content_hash`` from ``large_string`` to ``large_binary``,
    recovers ``__input_data_hash`` and ``__output_data_hash`` from the v0
    result DB, and writes the transformed rows to
    ``pipeline_path + ("pdb_v1",)``.

    Rows whose result data cannot be found (e.g. ephemeral results that have
    since expired) are written with a ``null`` ``__output_data_hash`` and
    counted as ``rows_unresolvable``.

    Rows already present at the v1 path are skipped (idempotent re-runs).

    Args:
        pipeline_db: The database containing the v0 pipeline table.
        pipeline_path: Bare v0 pipeline path tuple (no ``pdb_v1`` suffix).
        result_db: The database containing the v0 result table.
        result_path: Bare v0 result path tuple (no ``rdb_v1`` suffix).
        dry_run: If ``True``, read and count rows but write nothing.
        batch_size: Number of rows to process per batch.
        progress: If ``True``, log progress at INFO level.

    Returns:
        ``MigrationResult`` summarising the run.
    """
    v1_path = pipeline_path + (PIPELINE_DB_SCHEMA_VERSION,)
    start = time.monotonic()

    v0_table = pipeline_db.get_all_records(pipeline_path)
    if v0_table is None or v0_table.num_rows == 0:
        logger.info("No v0 records found at %r — nothing to migrate.", pipeline_path)
        return MigrationResult(
            rows_total=0,
            rows_migrated=0,
            rows_skipped=0,
            rows_unresolvable=0,
            elapsed_s=time.monotonic() - start,
            dry_run=dry_run,
        )

    rows_total = v0_table.num_rows
    rows_migrated = 0
    rows_skipped = 0
    rows_unresolvable = 0

    # Collect IDs already at v1 for idempotency.
    v1_existing = pipeline_db.get_all_records(v1_path)
    existing_ids: set[bytes] = set()
    if v1_existing is not None and "__record_id" in v1_existing.schema.names:
        existing_ids = {
            bytes(r)
            for r in v1_existing.column("__record_id").to_pylist()
            if r is not None
        }

    # Load the entire v0 result DB into memory for lookups by __data_id.
    rdb_v0 = result_db.get_all_records(result_path)
    rdb_index: dict[bytes, dict] = {}
    if rdb_v0 is not None:
        for row in rdb_v0.to_pylist():
            rid = bytes(row.get("__record_id") or b"")
            if rid:
                rdb_index[rid] = row

    if progress:
        logger.info(
            "migrate_pipeline_v0_to_v1: found %d rows at v0 path %r",
            rows_total,
            pipeline_path,
        )

    for batch_start in range(0, rows_total, batch_size):
        batch = v0_table.slice(batch_start, batch_size)

        # Skip rows already at v1.
        if existing_ids and "__record_id" in batch.schema.names:
            mask = pa.array(
                [
                    bytes(rid) not in existing_ids
                    for rid in batch.column("__record_id").to_pylist()
                ],
                type=pa.bool_(),
            )
            new_rows = batch.filter(mask)
            rows_skipped += batch.num_rows - new_rows.num_rows
        else:
            new_rows = batch

        if new_rows.num_rows == 0:
            continue

        transformed, batch_unresolvable = _transform_pdb_batch(new_rows, rdb_index)
        rows_unresolvable += batch_unresolvable

        if not dry_run:
            pipeline_db.add_records(v1_path, transformed, skip_duplicates=True)

        rows_migrated += new_rows.num_rows
        if progress:
            logger.info(
                "migrate_pipeline_v0_to_v1: %d/%d rows processed",
                min(batch_start + batch_size, rows_total),
                rows_total,
            )

    if not dry_run:
        pipeline_db.flush()

    elapsed = time.monotonic() - start
    return MigrationResult(
        rows_total=rows_total,
        rows_migrated=rows_migrated,
        rows_skipped=rows_skipped,
        rows_unresolvable=rows_unresolvable,
        elapsed_s=elapsed,
        dry_run=dry_run,
    )


def _transform_pdb_batch(
    batch: pa.Table,
    rdb_index: dict[bytes, dict],
) -> tuple[pa.Table, int]:
    """Transform a batch of v0 pdb rows into v1 format.

    Args:
        batch: Arrow table slice of v0 pdb rows.
        rdb_index: Dict mapping rdb record ID bytes to row dicts (from v0 rdb).

    Returns:
        Tuple of (transformed Arrow table, count of unresolvable rows).
    """
    node_hash_col = constants.NODE_CONTENT_HASH_COL
    input_hash_col = constants.INPUT_DATA_HASH_COL
    output_hash_col = constants.OUTPUT_DATA_HASH_COL
    data_id_col = constants.DATA_RECORD_ID

    rows = batch.to_pylist()
    unresolvable = 0
    out_rows: list[dict] = []

    for row in rows:
        new_row = dict(row)

        # Convert __node_content_hash from string → binary.
        if node_hash_col in new_row and new_row[node_hash_col] is not None:
            new_row[node_hash_col] = ContentHash.from_string(
                new_row[node_hash_col]
            ).to_prefixed_digest()

        # Recover __input_data_hash from rdb v0 row.
        data_id = bytes(row.get(data_id_col) or b"")
        rdb_row = rdb_index.get(data_id)
        if rdb_row is not None:
            raw_input_hash = rdb_row.get(input_hash_col)
            if raw_input_hash is not None:
                new_row[input_hash_col] = ContentHash.from_string(
                    raw_input_hash
                ).to_prefixed_digest()
            else:
                new_row[input_hash_col] = None
            # Recover __output_data_hash: re-encode from pdb v0 string column.
            raw_output_hash = row.get(output_hash_col)
            if raw_output_hash is not None:
                new_row[output_hash_col] = ContentHash.from_string(
                    raw_output_hash
                ).to_prefixed_digest()
            else:
                new_row[output_hash_col] = None
        else:
            # Result data gone (ephemeral expired or deleted).
            new_row[input_hash_col] = None
            new_row[output_hash_col] = None
            unresolvable += 1

        out_rows.append(new_row)

    # Rebuild Arrow table with corrected column types.
    transformed = pa.Table.from_pylist(out_rows, schema=_v1_pdb_schema(batch))
    return transformed, unresolvable


def _v1_pdb_schema(v0_batch: pa.Table) -> pa.Schema:
    """Derive the v1 pdb Arrow schema from a v0 batch.

    Replaces the three hash columns with ``large_binary`` equivalents;
    all other columns retain their original types.

    Args:
        v0_batch: A v0 pdb Arrow table (used to read non-hash column types).

    Returns:
        Arrow schema for the v1 pdb table.
    """
    binary_cols = {
        constants.NODE_CONTENT_HASH_COL,
        constants.INPUT_DATA_HASH_COL,
        constants.OUTPUT_DATA_HASH_COL,
    }
    fields = []
    for field in v0_batch.schema:
        if field.name in binary_cols:
            fields.append(pa.field(field.name, pa.large_binary(), nullable=True))
        else:
            fields.append(field)
    # Ensure the new hash columns exist even if absent in v0.
    existing_names = {f.name for f in fields}
    for col in binary_cols:
        if col not in existing_names:
            fields.append(pa.field(col, pa.large_binary(), nullable=True))
    return pa.schema(fields)


def migrate_node(
    node: "FunctionJobNode",
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult:
    """Convenience wrapper: migrate a single ``FunctionJobNode``'s pipeline DB.

    Extracts the pipeline DB, pipeline path, result DB, and result path
    directly from the node and delegates to ``migrate_pipeline_v0_to_v1()``.

    Args:
        node: The ``FunctionJobNode`` whose pipeline DB to migrate.
        dry_run: If ``True``, read and count rows but write nothing.
        batch_size: Rows to process per batch.
        progress: If ``True``, log progress at INFO level.

    Returns:
        ``MigrationResult`` summarising the run.

    Raises:
        RuntimeError: If the node has no pipeline database attached.
    """
    if node._pipeline_database is None:
        raise RuntimeError(
            f"Node {node.label!r} has no pipeline database — cannot migrate."
        )
    cached_pod = node._cached_function_pod
    if cached_pod is None:
        raise RuntimeError(
            f"Node {node.label!r} has no cached function pod — cannot locate result DB."
        )
    return migrate_pipeline_v0_to_v1(
        pipeline_db=node._pipeline_database,
        pipeline_path=node.node_identity_path,
        result_db=cached_pod._cache.result_database,
        result_path=cached_pod._cache.record_path,
        dry_run=dry_run,
        batch_size=batch_size,
        progress=progress,
    )
```

- [ ] **Step 4: Update `src/orcapod/migrations/__init__.py`**

```python
"""Orcapod schema migration utilities.

Public API:
    ``migrate_pipeline_v0_to_v1`` — migrate a pipeline DB table from v0 to v1.
    ``migrate_result_v0_to_v1`` — migrate a result DB table from v0 to v1.
    ``migrate_node`` — convenience wrapper: migrate a ``FunctionJobNode`` in one call.
    ``MigrationResult`` — dataclass summarising a migration run.
"""
from orcapod.migrations.types import MigrationResult
from orcapod.migrations.result_db import migrate_result_v0_to_v1
from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1, migrate_node

__all__ = [
    "MigrationResult",
    "migrate_pipeline_v0_to_v1",
    "migrate_result_v0_to_v1",
    "migrate_node",
]
```

- [ ] **Step 5: Run the tests**

```bash
uv run pytest tests/test_migrations/ -v --tb=short
```
Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/migrations/ tests/test_migrations/
git commit -m "feat(migrations): add migrate_pipeline_v0_to_v1() and migrate_node() convenience wrapper"
```

---

## Task 10: CLI — `orcapod migrate` sub-commands

**Files:**
- Create: `src/orcapod/cli/migrate.py`
- Modify: `src/orcapod/cli/__init__.py`
- Create: `tests/test_cli/test_migrate.py`

- [ ] **Step 1: Write failing CLI smoke test**

Create `tests/test_cli/test_migrate.py`:

```python
"""Smoke tests for the `orcapod migrate` CLI sub-commands."""
from __future__ import annotations

import json
import subprocess
import sys


def _run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "orcapod.cli", *args],
        capture_output=True,
        text=True,
    )


class TestMigratePipelineDbCli:
    def test_help_exits_zero(self):
        result = subprocess.run(
            ["uv", "run", "orcapod", "migrate", "pipeline-db", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "PIPELINE_DB_PATH" in result.stdout or "pipeline" in result.stdout.lower()

    def test_dry_run_exits_zero(self, tmp_path):
        """--dry-run with a non-existent DB path exits 0 (nothing to migrate)."""
        db_path = str(tmp_path / "pipeline_db")
        result = subprocess.run(
            [
                "uv", "run", "orcapod", "migrate", "pipeline-db",
                db_path, db_path, "my_node/path",
                "--dry-run", "--no-progress",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_json_summary_output(self, tmp_path):
        db_path = str(tmp_path / "pipeline_db")
        result = subprocess.run(
            [
                "uv", "run", "orcapod", "migrate", "pipeline-db",
                db_path, db_path, "my_node/path",
                "--dry-run", "--json-summary", "--no-progress",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        summary = json.loads(result.stdout.strip())
        assert "rows_total" in summary
        assert "dry_run" in summary
        assert summary["dry_run"] is True


class TestMigrateResultDbCli:
    def test_help_exits_zero(self):
        result = subprocess.run(
            ["uv", "run", "orcapod", "migrate", "result-db", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0

    def test_dry_run_exits_zero(self, tmp_path):
        db_path = str(tmp_path / "result_db")
        result = subprocess.run(
            [
                "uv", "run", "orcapod", "migrate", "result-db",
                db_path, "my_pod/path",
                "--dry-run", "--no-progress",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_cli/test_migrate.py -v
```
Expected: `SystemExit` or `AssertionError` because `orcapod migrate` doesn't exist yet.

- [ ] **Step 3: Create `src/orcapod/cli/migrate.py`**

```python
"""``orcapod migrate`` sub-commands.

Provides ``orcapod migrate pipeline-db`` and ``orcapod migrate result-db``
for upgrading v0 pipeline/result DB tables to the v1 schema.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import typer

from orcapod.databases.delta_lake_databases import DeltaTableDatabase

migrate_app = typer.Typer(
    name="migrate",
    help="Migrate Orcapod pipeline and result DB tables to the current schema version.",
    no_args_is_help=True,
)


@migrate_app.command("pipeline-db")
def migrate_pipeline_db(
    pipeline_db_path: str = typer.Argument(..., help="Path to the pipeline DB (Delta Lake root)."),
    result_db_path: str = typer.Argument(..., help="Path to the result DB (Delta Lake root)."),
    node_paths: list[str] = typer.Argument(..., help="One or more bare v0 node paths (e.g. 'my_node/schema:abc123')."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count rows to migrate without writing."),
    batch_size: int = typer.Option(500, "--batch-size", help="Rows processed per batch."),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Log progress messages."),
    json_summary: bool = typer.Option(False, "--json-summary", help="Print JSON summary to stdout on completion."),
) -> None:
    """Migrate one or more pipeline DB node paths from v0 to v1 schema."""
    from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1

    pipeline_db = DeltaTableDatabase(base_path=pipeline_db_path)
    result_db = DeltaTableDatabase(base_path=result_db_path)

    for node_path_str in node_paths:
        pipeline_path = tuple(node_path_str.split("/"))
        result_path = pipeline_path  # by convention result DB mirrors pipeline path

        if progress:
            typer.echo(f"Migrating pipeline DB: {pipeline_db_path}")
            typer.echo(f"  node path: {node_path_str}")

        result = migrate_pipeline_v0_to_v1(
            pipeline_db=pipeline_db,
            pipeline_path=pipeline_path,
            result_db=result_db,
            result_path=result_path,
            dry_run=dry_run,
            batch_size=batch_size,
            progress=progress,
        )

        if progress:
            typer.echo(
                f"  migrated: {result.rows_migrated}   "
                f"skipped (already v1): {result.rows_skipped}   "
                f"unresolvable: {result.rows_unresolvable}"
            )
            typer.echo(f"  elapsed: {result.elapsed_s:.1f}s")

        if json_summary:
            summary = {
                "rows_total": result.rows_total,
                "rows_migrated": result.rows_migrated,
                "rows_skipped": result.rows_skipped,
                "rows_unresolvable": result.rows_unresolvable,
                "elapsed_s": result.elapsed_s,
                "dry_run": result.dry_run,
            }
            typer.echo(json.dumps(summary))


@migrate_app.command("result-db")
def migrate_result_db(
    result_db_path: str = typer.Argument(..., help="Path to the result DB (Delta Lake root)."),
    record_paths: list[str] = typer.Argument(..., help="One or more bare v0 record paths."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Count rows to migrate without writing."),
    batch_size: int = typer.Option(500, "--batch-size", help="Rows processed per batch."),
    progress: bool = typer.Option(True, "--progress/--no-progress", help="Log progress messages."),
    json_summary: bool = typer.Option(False, "--json-summary", help="Print JSON summary to stdout on completion."),
) -> None:
    """Migrate one or more result DB record paths from v0 to v1 schema."""
    from orcapod.migrations.result_db import migrate_result_v0_to_v1

    result_db = DeltaTableDatabase(base_path=result_db_path)

    for record_path_str in record_paths:
        result_path = tuple(record_path_str.split("/"))

        if progress:
            typer.echo(f"Migrating result DB: {result_db_path}")
            typer.echo(f"  record path: {record_path_str}")

        result = migrate_result_v0_to_v1(
            result_db=result_db,
            result_path=result_path,
            dry_run=dry_run,
            batch_size=batch_size,
            progress=progress,
        )

        if progress:
            typer.echo(
                f"  migrated: {result.rows_migrated}   "
                f"skipped (already v1): {result.rows_skipped}"
            )
            typer.echo(f"  elapsed: {result.elapsed_s:.1f}s")

        if json_summary:
            summary = {
                "rows_total": result.rows_total,
                "rows_migrated": result.rows_migrated,
                "rows_skipped": result.rows_skipped,
                "rows_unresolvable": result.rows_unresolvable,
                "elapsed_s": result.elapsed_s,
                "dry_run": result.dry_run,
            }
            typer.echo(json.dumps(summary))
```

- [ ] **Step 4: Register `migrate_app` in `src/orcapod/cli/__init__.py`**

Add to `src/orcapod/cli/__init__.py`:

```python
from orcapod.cli.migrate import migrate_app

app.add_typer(migrate_app, name="migrate")
```

- [ ] **Step 5: Run the CLI tests**

```bash
uv run pytest tests/test_cli/test_migrate.py -v --tb=short
```
Expected: all PASSED

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/cli/migrate.py src/orcapod/cli/__init__.py tests/test_cli/test_migrate.py
git commit -m "feat(cli): add orcapod migrate pipeline-db and result-db sub-commands"
```

---

## Task 11: Create committed test fixtures (sample Delta Lake tables)

**Files:**
- Create: `tests/fixtures/` directory with four sample Delta Lake tables
- Create: `tests/fixtures/generate_fixtures.py` — script to regenerate fixtures

The fixtures represent a pair of pdb tables (v0 and v1) and a pair of rdb tables (v0 and v1) containing the same three logical rows. They are committed to git and serve as golden references for migration tests.

- [ ] **Step 1: Create the fixture-generation script**

Create `tests/fixtures/generate_fixtures.py`:

```python
"""Script to generate sample fixture Delta Lake tables for migration tests.

Run once and commit the output:
    uv run python tests/fixtures/generate_fixtures.py

The generated tables are stored under tests/fixtures/ and checked into git.
They are the authoritative golden references for migration correctness tests.
"""
from __future__ import annotations

import sys
from pathlib import Path

# Add project src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pyarrow as pa
from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, RESULT_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash

FIXTURES_DIR = Path(__file__).parent

# --- Shared test data ---
_DATA_ID_1 = b"\x01" * 16
_DATA_ID_2 = b"\x02" * 16
_DATA_ID_3 = b"\x03" * 16

_NODE_HASH = ContentHash("sha256", b"\xaa" * 32)
_INPUT_HASH_1 = ContentHash("sha256", b"\x11" * 32)
_INPUT_HASH_2 = ContentHash("sha256", b"\x22" * 32)
_INPUT_HASH_3 = ContentHash("sha256", b"\x33" * 32)
_OUTPUT_HASH_1 = ContentHash("sha256", b"\x44" * 32)
_OUTPUT_HASH_2 = ContentHash("sha256", b"\x55" * 32)
_OUTPUT_HASH_3 = ContentHash("sha256", b"\x66" * 32)
_SIG_HASH = ContentHash("sha256", b"\x77" * 32)
_CONTENT_HASH = ContentHash("sha256", b"\x88" * 32)


def write_pdb_v0():
    db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "pdb_v0_sample"))
    path = ("node",)
    rows = pa.table({
        constants.DATA_RECORD_ID: pa.array([_DATA_ID_1, _DATA_ID_2, _DATA_ID_3], type=pa.large_binary()),
        constants.NODE_CONTENT_HASH_COL: pa.array(
            [_NODE_HASH.to_string()] * 3, type=pa.large_string()
        ),
        constants.INPUT_DATA_HASH_COL: pa.array(
            [h.to_string() for h in [_INPUT_HASH_1, _INPUT_HASH_2, _INPUT_HASH_3]],
            type=pa.large_string(),
        ),
        constants.OUTPUT_DATA_HASH_COL: pa.array(
            [h.to_string() for h in [_OUTPUT_HASH_1, _OUTPUT_HASH_2, _OUTPUT_HASH_3]],
            type=pa.large_string(),
        ),
        f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(
            ["ctx1", "ctx2", "ctx3"], type=pa.large_string()
        ),
        f"{constants.META_PREFIX}computed": pa.array([True, True, False], type=pa.bool_()),
        constants.IS_EPHEMERAL_COL: pa.array([False, False, False], type=pa.bool_()),
        "__pipeline_base_entry_id": pa.array(
            [_DATA_ID_1, _DATA_ID_2, _DATA_ID_3], type=pa.large_binary()
        ),
        "__pipeline_recomputation_index": pa.array([0, 0, 0], type=pa.int32()),
    })
    record_ids = [b"\xr1" * 32 for b in [b"\x01", b"\x02", b"\x03"]]
    # Use simple unique IDs
    db.add_records(path, rows, skip_duplicates=False)
    db.flush()
    print(f"Written pdb_v0_sample: {rows.num_rows} rows")


def write_pdb_v1():
    db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "pdb_v1_sample"))
    path = ("node", PIPELINE_DB_SCHEMA_VERSION)
    rows = pa.table({
        constants.DATA_RECORD_ID: pa.array([_DATA_ID_1, _DATA_ID_2, _DATA_ID_3], type=pa.large_binary()),
        constants.NODE_CONTENT_HASH_COL: pa.array(
            [_NODE_HASH.to_prefixed_digest()] * 3, type=pa.large_binary()
        ),
        constants.INPUT_DATA_HASH_COL: pa.array(
            [h.to_prefixed_digest() for h in [_INPUT_HASH_1, _INPUT_HASH_2, _INPUT_HASH_3]],
            type=pa.large_binary(),
        ),
        constants.OUTPUT_DATA_HASH_COL: pa.array(
            [h.to_prefixed_digest() for h in [_OUTPUT_HASH_1, _OUTPUT_HASH_2, _OUTPUT_HASH_3]],
            type=pa.large_binary(),
        ),
        f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(
            ["ctx1", "ctx2", "ctx3"], type=pa.large_string()
        ),
        f"{constants.META_PREFIX}computed": pa.array([True, True, False], type=pa.bool_()),
        constants.IS_EPHEMERAL_COL: pa.array([False, False, False], type=pa.bool_()),
        "__pipeline_base_entry_id": pa.array(
            [_DATA_ID_1, _DATA_ID_2, _DATA_ID_3], type=pa.large_binary()
        ),
        "__pipeline_recomputation_index": pa.array([0, 0, 0], type=pa.int32()),
    })
    db.add_records(path, rows, skip_duplicates=False)
    db.flush()
    print(f"Written pdb_v1_sample: {rows.num_rows} rows")


def write_rdb_v0():
    db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "rdb_v0_sample"))
    path = ("pod",)
    rows = pa.table({
        "__input_data_hash": pa.array(
            [h.to_string() for h in [_INPUT_HASH_1, _INPUT_HASH_2, _INPUT_HASH_3]],
            type=pa.large_string(),
        ),
        "__pf_var_function_name": pa.array(["my_func"] * 3, type=pa.large_string()),
        "__pf_var_function_signature_hash": pa.array(
            [_SIG_HASH.to_string()] * 3, type=pa.large_string()
        ),
        "__pf_var_function_content_hash": pa.array(
            [_CONTENT_HASH.to_string()] * 3, type=pa.large_string()
        ),
        "__pf_var_git_hash": pa.array(["abc123"] * 3, type=pa.large_string()),
        "__pf_exec_executor_type": pa.array(["local"] * 3, type=pa.large_string()),
        "__pf_exec_python_version": pa.array(["3.11"] * 3, type=pa.large_string()),
        "__pod_ts": pa.array([0, 1, 2], type=pa.timestamp("us", tz="UTC")),
        "result": pa.array([10, 20, 30], type=pa.int64()),
    })
    for i, data_id in enumerate([_DATA_ID_1, _DATA_ID_2, _DATA_ID_3]):
        db.add_record(path, data_id, rows.slice(i, 1))
    db.flush()
    print(f"Written rdb_v0_sample: {rows.num_rows} rows")


def write_rdb_v1():
    db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "rdb_v1_sample"))
    path = ("pod", RESULT_DB_SCHEMA_VERSION)
    rows = pa.table({
        "__input_data_hash": pa.array(
            [h.to_prefixed_digest() for h in [_INPUT_HASH_1, _INPUT_HASH_2, _INPUT_HASH_3]],
            type=pa.large_binary(),
        ),
        "__pf_var_function_name": pa.array(["my_func"] * 3, type=pa.large_string()),
        "__pf_var_function_signature_hash": pa.array(
            [_SIG_HASH.to_prefixed_digest()] * 3, type=pa.large_binary()
        ),
        "__pf_var_function_content_hash": pa.array(
            [_CONTENT_HASH.to_prefixed_digest()] * 3, type=pa.large_binary()
        ),
        "__pf_var_git_hash": pa.array(["abc123"] * 3, type=pa.large_string()),
        "__pf_exec_executor_type": pa.array(["local"] * 3, type=pa.large_string()),
        "__pf_exec_python_version": pa.array(["3.11"] * 3, type=pa.large_string()),
        "__pod_ts": pa.array([0, 1, 2], type=pa.timestamp("us", tz="UTC")),
        "result": pa.array([10, 20, 30], type=pa.int64()),
    })
    for i, data_id in enumerate([_DATA_ID_1, _DATA_ID_2, _DATA_ID_3]):
        db.add_record(path, data_id, rows.slice(i, 1))
    db.flush()
    print(f"Written rdb_v1_sample: {rows.num_rows} rows")


if __name__ == "__main__":
    write_pdb_v0()
    write_pdb_v1()
    write_rdb_v0()
    write_rdb_v1()
    print("All fixtures written.")
```

- [ ] **Step 2: Run the generation script**

```bash
uv run python tests/fixtures/generate_fixtures.py
```
Expected output:
```
Written pdb_v0_sample: 3 rows
Written pdb_v1_sample: 3 rows
Written rdb_v0_sample: 3 rows
Written rdb_v1_sample: 3 rows
All fixtures written.
```

- [ ] **Step 3: Verify fixtures were created**

```bash
ls tests/fixtures/
```
Expected: `generate_fixtures.py  pdb_v0_sample/  pdb_v1_sample/  rdb_v0_sample/  rdb_v1_sample/`

- [ ] **Step 4: Commit the fixtures**

```bash
git add tests/fixtures/
git commit -m "test(fixtures): add committed sample Delta Lake tables for pdb/rdb v0 and v1 schema versions"
```

---

## Task 12: Golden migration tests

**Files:**
- Create: `tests/test_migrations/test_golden.py`

These tests are the authoritative correctness check: migrate the v0 fixture and assert the output matches the v1 fixture row-for-row.

- [ ] **Step 1: Write the golden tests**

Create `tests/test_migrations/test_golden.py`:

```python
"""Golden migration tests.

Migrates the committed v0 fixture tables and asserts the output
matches the committed v1 fixtures row-for-row.

Maintenance rule: when a new schema version vX is introduced, add
new v{X-1} and vX fixtures (do not modify existing ones), then add a
new test here following the same pattern.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.databases import InMemoryArrowDatabase
from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1
from orcapod.migrations.result_db import migrate_result_v0_to_v1
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, RESULT_DB_SCHEMA_VERSION

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


def _sort_table(table: pa.Table) -> pa.Table:
    """Sort by all columns for stable comparison."""
    # Sort by the first column (record_id or input_data_hash) for determinism.
    if table.num_rows == 0:
        return table
    sort_col = table.schema.names[0]
    indices = pa.compute.sort_indices(table, sort_keys=[(sort_col, "ascending")])
    return table.take(indices)


def _tables_equal(a: pa.Table, b: pa.Table) -> bool:
    """Return True if two tables contain the same data (same schema and rows, order-independent)."""
    if set(a.schema.names) != set(b.schema.names):
        return False
    # Align column order.
    b_aligned = b.select(a.schema.names)
    a_sorted = _sort_table(a)
    b_sorted = _sort_table(b_aligned)
    return a_sorted.equals(b_sorted)


class TestGoldenPdbMigration:
    """Migrate pdb_v0_sample and compare against pdb_v1_sample."""

    def test_migrate_pdb_v0_to_v1_matches_golden(self, tmp_path):
        # Load v0 fixture from Delta Lake.
        v0_db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "pdb_v0_sample"))
        rdb_v0_db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "rdb_v0_sample"))

        # Write migration output to a temp in-memory DB.
        out_db = InMemoryArrowDatabase()

        # Mirror the v0 data into out_db so migrate_pipeline_v0_to_v1 can read it.
        # (The function reads from pipeline_db at pipeline_path.)
        # We'll use a wrapper that reads from v0_db for v0 and writes to out_db for v1.
        # Simpler: copy v0 data into out_db first, then migrate within out_db.
        pdb_v0_path = ("node",)
        rdb_v0_path = ("pod",)

        v0_table = v0_db.get_all_records(pdb_v0_path)
        assert v0_table is not None, "pdb_v0_sample fixture missing"
        out_db.add_records(pdb_v0_path, v0_table, skip_duplicates=False)
        out_db.flush()

        rdb_v0_table = rdb_v0_db.get_all_records(rdb_v0_path)
        assert rdb_v0_table is not None, "rdb_v0_sample fixture missing"
        rdb_out_db = InMemoryArrowDatabase()
        rdb_out_db.add_records(rdb_v0_path, rdb_v0_table, skip_duplicates=False)
        rdb_out_db.flush()

        # Run migration.
        result = migrate_pipeline_v0_to_v1(
            pipeline_db=out_db,
            pipeline_path=pdb_v0_path,
            result_db=rdb_out_db,
            result_path=rdb_v0_path,
            progress=False,
        )

        assert result.rows_total == 3
        assert result.rows_migrated == 3
        assert result.rows_unresolvable == 0

        # Read migrated output.
        migrated = out_db.get_all_records(pdb_v0_path + (PIPELINE_DB_SCHEMA_VERSION,))
        assert migrated is not None

        # Read golden v1 fixture.
        v1_db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "pdb_v1_sample"))
        golden = v1_db.get_all_records(("node", PIPELINE_DB_SCHEMA_VERSION))
        assert golden is not None, "pdb_v1_sample fixture missing"

        # Compare.
        assert _tables_equal(
            migrated.select(golden.schema.names),
            golden,
        ), "Migrated pdb table does not match golden v1 fixture"


class TestGoldenRdbMigration:
    """Migrate rdb_v0_sample and compare against rdb_v1_sample."""

    def test_migrate_rdb_v0_to_v1_matches_golden(self):
        v0_db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "rdb_v0_sample"))

        rdb_v0_path = ("pod",)
        v0_table = v0_db.get_all_records(rdb_v0_path)
        assert v0_table is not None, "rdb_v0_sample fixture missing"

        out_db = InMemoryArrowDatabase()
        out_db.add_records(rdb_v0_path, v0_table, skip_duplicates=False)
        out_db.flush()

        result = migrate_result_v0_to_v1(
            result_db=out_db,
            result_path=rdb_v0_path,
            progress=False,
        )

        assert result.rows_total == 3
        assert result.rows_migrated == 3
        assert result.rows_unresolvable == 0

        migrated = out_db.get_all_records(rdb_v0_path + (RESULT_DB_SCHEMA_VERSION,))
        assert migrated is not None

        v1_db = DeltaTableDatabase(base_path=str(FIXTURES_DIR / "rdb_v1_sample"))
        golden = v1_db.get_all_records(("pod", RESULT_DB_SCHEMA_VERSION))
        assert golden is not None, "rdb_v1_sample fixture missing"

        assert _tables_equal(
            migrated.select(golden.schema.names),
            golden,
        ), "Migrated rdb table does not match golden v1 fixture"
```

- [ ] **Step 2: Run the golden tests**

```bash
uv run pytest tests/test_migrations/test_golden.py -v --tb=short
```
Expected: 2 PASSED

- [ ] **Step 3: Run the full test suite to check for regressions**

```bash
uv run pytest --tb=short -q
```
Expected: all PASSED (no regressions)

- [ ] **Step 4: Commit**

```bash
git add tests/test_migrations/test_golden.py
git commit -m "test(migrations): add golden migration tests against committed fixtures"
```

---

## Final: Full regression check + push

- [ ] **Run the complete test suite**

```bash
uv run pytest --tb=short -q
```
Expected: all PASSED

- [ ] **Push the feature branch**

```bash
git push -u origin HEAD
```
