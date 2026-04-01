# Database Path Contextualization — Design Spec

**Linear issue:** ENG-341
**Date:** 2026-04-01
**Status:** Approved

---

## Overview

Databases need a general mechanism to produce a new database instance scoped to a
sub-path, without modifying the original. The primitive is `at(*path_components)`,
which returns a new database view whose root is the caller's root extended by the
given path components.

This is the foundational primitive needed by ENG-340 (ScopedDatabase /
`pipeline_path` decoupling) and is useful anywhere a component needs its own
storage namespace derived from a parent database.

---

## Core Concepts

### Two-level path model

Every database instance carries two path concepts:

| Concept | Type | Meaning |
|---------|------|---------|
| **Absolute root** | backend-specific | The storage location configured at construction (filesystem URI, SQL connector, none). Fixed forever. Implementation detail of each backend. |
| **`base_path`** | `tuple[str, ...]` | The relative "cwd" within the absolute root. Starts as `()`. Extended by `at()`. Public, queryable. |

When a record is written at `record_path`, its full resolved location is:

```
absolute_root / base_path / record_path
```

Each backend translates this concatenated tuple into its own format:

| Backend | Translation |
|---------|-------------|
| `DeltaTableDatabase` | `root_uri / "p0" / "p1" / ... / "r0" / "r1" / ...` (filesystem path) |
| `InMemoryArrowDatabase` | `"p0/p1/.../r0/r1/..."` (dict key) |
| `ConnectorArrowDatabase` | `"p0__p1__...__r0__r1__..."` (SQL table name) |
| `NoOpArrowDatabase` | discarded (prefix tracked for introspection only) |

### Prefix application: where it happens

Each backend applies the prefix at a different point in its stack:

- **`DeltaTableDatabase`**: prefix is applied only inside `_get_table_uri()`.
  `_get_record_key()` is NOT changed — it continues to return `"/".join(record_path)`.
  This preserves the correctness of `flush()`, which reconstructs `record_path` from
  the key and then calls `flush_batch(record_path)` → `_get_table_uri(record_path)`.
  If the prefix were baked into `_get_record_key`, `_get_table_uri` would apply it a
  second time. Keeping the prefix out of `_get_record_key` avoids the double-prefix.

- **`InMemoryArrowDatabase`** and **`ConnectorArrowDatabase`**: prefix is applied inside
  `_get_record_key()`. Both backends' `flush()` methods reconstruct `record_path` from
  the key and then call a terminal translation function (`_tables` dict access for
  in-memory; `_path_to_table_name` for connector). Neither re-applies any prefix in
  that terminal step, so including the prefix in the key is safe and correct.

### Consistency with existing pipeline path pattern

The existing `pipeline_path_prefix: tuple[str, ...]` pattern in `FunctionNode` and
`CachedFunctionPod` already uses this model: a single shared database receives
writes at namespaced keys derived by prepending a prefix tuple to `record_path`.
`at()` formalizes this pattern — instead of threading a prefix tuple through every
call site, callers create a scoped view once.

---

## Interface Changes

### `ArrowDatabaseProtocol`

Two new members added to the protocol:

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

Concrete implementations return their own type (e.g. `DeltaTableDatabase`) for
type narrowness, even though the protocol signature uses `ArrowDatabaseProtocol`.

The `_path_prefix` constructor parameter used internally by each backend is
prefixed with `_` to signal it is not part of the public API. Callers should
always use `at()` to create scoped views.

---

## Per-Backend Implementation

### `DeltaTableDatabase`

**Naming clarification:**

- The constructor parameter remains `base_path: str | Path | UPath` (unchanged).
- The internal attribute `self._base_uri: str` is renamed to `self._root_uri: str`.
- The existing `self.base_path: Path` (local-only, the absolute root as a `Path`
  object) is renamed to `self._local_root: Path`. This frees the name `base_path`
  for the new `tuple[str, ...]` property.
- The new `base_path` property returns `self._path_prefix: tuple[str, ...]`.

**Depth validation:**

`_validate_record_path` is updated to check the combined depth:

```python
if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
    raise ValueError(...)
```

**`_get_table_uri` update (prefix applied here only):**

```python
def _get_table_uri(self, record_path: tuple[str, ...], create_dir: bool = False) -> str:
    full_path = self._path_prefix + record_path  # prefix applied once, here
    if self._is_cloud:
        return self._root_uri.rstrip("/") + "/" + "/".join(full_path)
    else:
        path = self._local_root
        for component in full_path:
            path = path / self._sanitize_path_component(component)
        if create_dir:
            path.mkdir(parents=True, exist_ok=True)
        return str(path)
```

`_get_record_key` is **not changed** — it continues to return `"/".join(record_path)`
(without prefix). This keeps `flush()` correct: it reconstructs the original
`record_path` from the pending key and calls `_get_table_uri(record_path)`, which
applies the prefix exactly once.

**`list_sources()` on scoped instances:**

For a scoped instance, `list_sources()` scans from `self._local_root` joined with
the prefix components (i.e. the effective scoped root directory) and returns paths
relative to that scoped root — consistent with `base_path` semantics. Cloud paths
continue to raise `NotImplementedError`.

**`at()` implementation:**

Returns a new independent `DeltaTableDatabase` instance (fresh pending state, fresh
caches). The underlying filesystem is the shared storage — no dict sharing needed.
A write through `db.at("x")` that is flushed will be visible to a second `db.at("x")`
only after it is read back from disk (no in-process cache sharing).

```python
def at(self, *path_components: str) -> "DeltaTableDatabase":
    return DeltaTableDatabase(
        base_path=self._root_uri,           # constructor param name unchanged
        storage_options=self._storage_options,
        batch_size=self.batch_size,
        max_hierarchy_depth=self.max_hierarchy_depth,
        allow_schema_evolution=self.allow_schema_evolution,
        _path_prefix=self._path_prefix + path_components,
    )
```

**Config serialization:**

The existing `to_config` key `"base_path"` (which stored the filesystem URI root)
is renamed to `"root_uri"` to free `"base_path"` for the tuple. The new config
shape:

```python
{
    "type": "delta_table",
    "root_uri": self._root_uri,          # renamed from "base_path"
    "base_path": list(self._path_prefix), # new; () serializes as []
    "batch_size": ...,
    "max_hierarchy_depth": ...,
    "allow_schema_evolution": ...,
}
```

`from_config` reads both keys:
```python
base_path=config["root_uri"],
_path_prefix=tuple(config.get("base_path", [])),
```

**Existing test update required:** `test_database_config.py::test_to_config_includes_base_path`
currently asserts `config["base_path"] == str(tmp_path / "delta_db")`. After the
rename this key becomes `"root_uri"`. The test must be updated to assert
`config["root_uri"] == str(tmp_path / "delta_db")` and also assert
`config["base_path"] == []`.

---

### `InMemoryArrowDatabase`

**Internal changes:**

- Add `_path_prefix: tuple[str, ...]` (default `()`).
- Add `base_path` property returning `_path_prefix`.
- Update `_get_record_key` to prepend the prefix:

  ```python
  def _get_record_key(self, record_path: tuple[str, ...]) -> str:
      return "/".join(self._path_prefix + record_path)
  ```

- Update `_validate_record_path` to check combined depth:

  ```python
  if len(self._path_prefix) + len(record_path) > self.max_hierarchy_depth:
      raise ValueError(...)
  ```

**`at()` implementation:**

Returns a new `InMemoryArrowDatabase` that **shares** the underlying storage dicts
(`_tables`, `_pending_batches`, `_pending_record_ids`) by reference. The prefix
ensures all keys are namespaced; there is no collision. Calling `flush()` on any
view drains the **entire** shared pending queue (all prefixes), not just the
caller's. This is intentional and is documented in the `at()` docstring.

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

The `_path_prefix` and `_shared_*` parameters are internal (leading `_`).
A root instance is always created with no positional arguments:
`InMemoryArrowDatabase()`.

**Config serialization:**

```python
{
    "type": "in_memory",
    "base_path": list(self._path_prefix),  # new
    "max_hierarchy_depth": ...,
}
```

`from_config` restores the prefix as a tuple:
```python
_path_prefix=tuple(config.get("base_path", [])),
```

Note: `from_config` reconstructs a root instance with empty `_tables` (in-memory
data does not survive serialization). The `base_path` is preserved so the scoped
position is restored even though data is not.

---

### `ConnectorArrowDatabase`

**Internal changes:**

- Add `_path_prefix: tuple[str, ...]` (default `()`).
- Add `base_path` property returning `_path_prefix`.
- Update `_get_record_key` to prepend the prefix:

  ```python
  def _get_record_key(self, record_path: tuple[str, ...]) -> str:
      return "/".join(self._path_prefix + record_path)
  ```

- Update `_validate_record_path` to check combined depth.
- Add `_shared_pending_skip_existing` as a shared-dict parameter in `__init__`
  (alongside `_shared_pending_batches` and `_shared_pending_record_ids`).

**`flush()` correctness:**

`flush()` reconstructs `record_path = tuple(record_key.split("/"))`. Because
`_get_record_key` now includes the prefix, the reconstructed tuple is
`_path_prefix + original_record_path`. `_path_to_table_name` translates the
full tuple in one step (e.g. `("pipeline", "node1")` → `"pipeline__node1"`).
There is no second prefix application, so `flush()` remains correct with no
changes needed.

**`at()` implementation:**

Returns a new `ConnectorArrowDatabase` sharing the same `_connector` (SQL
database is the shared storage) and all three pending dicts:

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

**Config serialization:**

```python
{
    "type": "connector_arrow_database",
    "connector": ...,
    "base_path": list(self._path_prefix),  # new
    "max_hierarchy_depth": ...,
}
```

`from_config` currently raises `NotImplementedError` (PLT-1074/1075/1076); the
`"base_path"` key is added to the config shape for forward-compatibility but
`from_config` itself is not implemented in this issue.

---

### `NoOpArrowDatabase`

**Internal changes:**

- Add `_path_prefix: tuple[str, ...]` (default `()`).
- Add `base_path` property returning `_path_prefix`.
- All read/write behaviour unchanged (discards writes, returns `None` for reads).

**`at()` implementation:**

```python
def at(self, *path_components: str) -> "NoOpArrowDatabase":
    return NoOpArrowDatabase(_path_prefix=self._path_prefix + path_components)
```

**Config serialization:**

```python
def to_config(self) -> dict[str, Any]:
    return {
        "type": "noop",
        "base_path": list(self._path_prefix),
    }

@classmethod
def from_config(cls, config: dict[str, Any]) -> "NoOpArrowDatabase":
    return cls(_path_prefix=tuple(config.get("base_path", [])))
```

---

## Data Flow Example

```python
db = DeltaTableDatabase("/experiments")
# db.base_path == ()

run_db = db.at("run_2026")
# run_db.base_path == ("run_2026",)
# run_db._root_uri == "/experiments"  (unchanged)

node_db = run_db.at("transform_fn", "v1")
# node_db.base_path == ("run_2026", "transform_fn", "v1")
# node_db == db.at("run_2026", "transform_fn", "v1")  (equivalent)

node_db.add_record(("outputs",), record_id="abc", record=table)
# resolves to: /experiments/run_2026/transform_fn/v1/outputs/
```

---

## Config Round-Trip

```python
node_db = DeltaTableDatabase("/experiments").at("run_2026", "transform_fn", "v1")
config = node_db.to_config()
# {
#   "type": "delta_table",
#   "root_uri": "/experiments",
#   "base_path": ["run_2026", "transform_fn", "v1"],
#   "batch_size": 1000,
#   "max_hierarchy_depth": 10,
#   "allow_schema_evolution": True,
# }
reconstructed = DeltaTableDatabase.from_config(config)
assert reconstructed.base_path == ("run_2026", "transform_fn", "v1")
assert isinstance(reconstructed.base_path, tuple)  # not list
```

---

## Testing

Each backend's existing test file gains a `TestAtMethod` class. The `DeltaTableDatabase`
tests use `tmp_path` fixtures; the others need no fixtures.

**Test cases per backend:**

1. `base_path` is `()` on a fresh root instance.
2. `at("a", "b")` produces `base_path == ("a", "b")` and `isinstance(base_path, tuple)`.
3. `at("a").at("b")` is equivalent to `at("a", "b")`.
4. Writes through a scoped view are readable from the same scoped view.
5. Writes through a scoped view are **not** visible via the parent at the same
   bare `record_path` (different namespace).
6. **(InMemory and Connector only)** Writes through `db.at("x")` are visible from
   a second `db.at("x")` view created from the same root instance (shared storage).
   `DeltaTableDatabase` explicitly does NOT require this — scoped views are independent
   instances with no shared in-process caches.
7. `to_config` / `from_config` round-trips `base_path` correctly, including:
   - Correct value
   - `isinstance(restored.base_path, tuple)` (not list)
8. `_validate_record_path` raises `ValueError` when `len(base_path) + len(record_path)`
   exceeds `max_hierarchy_depth`.

**Existing test update:**

`test_database_config.py::test_to_config_includes_base_path` must be updated:
- Assert `config["root_uri"] == str(tmp_path / "delta_db")` (renamed key)
- Assert `config["base_path"] == []` (new key, empty tuple for root instance)

---

## Scope

**In scope (ENG-341):**

- `ArrowDatabaseProtocol`: add `base_path` property and `at()` method.
- `DeltaTableDatabase`: implement `at()` and `base_path`; rename internal attrs;
  update `_get_table_uri`, `_validate_record_path`, `list_sources()`, config.
- `InMemoryArrowDatabase`: implement `at()` and `base_path` with shared storage;
  update `_get_record_key`, `_validate_record_path`, config.
- `ConnectorArrowDatabase`: implement `at()` and `base_path` with shared storage;
  update `_get_record_key`, `_validate_record_path`, config.
- `NoOpArrowDatabase`: implement `at()` and `base_path`; update config.
- Unit tests for all backends including existing config test update.

**Out of scope:**

- Integrating `at()` into node/pipeline wiring (ENG-340).
- Cloud-path `list_sources()` (pre-existing limitation; scoped local `list_sources()`
  is in scope).
- `from_config` for `ConnectorArrowDatabase` (PLT-1074/1075/1076).
