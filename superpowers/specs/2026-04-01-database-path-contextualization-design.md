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
| **Absolute root** | backend-specific | The storage location configured at construction (filesystem URI, SQL connector, none). Fixed forever. Implementation detail. |
| **`base_path`** | `tuple[str, ...]` | The relative "cwd" within the absolute root. Starts as `()`. Extended by `at()`. Public, queryable. |

When a record is written at `record_path`, its full resolved location is:

```
absolute_root / base_path / record_path
```

Each backend translates this concatenated tuple into its own format:

| Backend | Translation |
|---------|-------------|
| `DeltaTableDatabase` | `root_uri / "p0" / "p1" / ... / "r0" / "r1" / ...` |
| `InMemoryArrowDatabase` | `"p0/p1/.../r0/r1/..."` (dict key) |
| `ConnectorArrowDatabase` | `"p0__p1__...__r0__r1__..."` (SQL table name) |
| `NoOpArrowDatabase` | discarded (prefix tracked for introspection) |

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

    Args:
        *path_components: One or more path components to append.

    Returns:
        A new database instance with base_path = self.base_path + path_components.
    """
    ...
```

Concrete implementations return their own type (e.g. `DeltaTableDatabase`) for
type narrowness, even though the protocol signature uses `ArrowDatabaseProtocol`.

---

## Per-Backend Implementation

### `DeltaTableDatabase`

**Internal changes:**

- Rename `_base_uri` → `_root_uri` (the absolute filesystem/cloud root; private).
- Rename existing `self.base_path: Path` (local-only, absolute root as `Path`) →
  `self._local_root: Path` (private, used only by `list_sources()`).
- Add `_path_prefix: tuple[str, ...]= ()`.
- Add `base_path` property returning `_path_prefix`.
- Update `_get_table_uri(record_path)` to insert `_path_prefix` between the root
  and `record_path`:

  ```python
  def _get_table_uri(self, record_path: tuple[str, ...], create_dir: bool = False) -> str:
      full_path = self._path_prefix + record_path
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

**`at()` implementation:**

Returns a new independent `DeltaTableDatabase` instance (fresh pending state).
The underlying filesystem is the shared storage — no dict sharing needed.

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

**Config serialization:**

`to_config` adds `"base_path": list(self._path_prefix)` (serialized as a list for
JSON compatibility). `from_config` reads it back as a tuple.

---

### `InMemoryArrowDatabase`

**Internal changes:**

- Add `_path_prefix: tuple[str, ...]= ()`.
- Add `base_path` property returning `_path_prefix`.
- Update `_get_record_key` to prepend the prefix:

  ```python
  def _get_record_key(self, record_path: tuple[str, ...]) -> str:
      return "/".join(self._path_prefix + record_path)
  ```

**`at()` implementation:**

Returns a new `InMemoryArrowDatabase` that **shares** the underlying storage dicts
(`_tables`, `_pending_batches`, `_pending_record_ids`) by reference. The prefix
ensures all keys are namespaced; there is no collision. Calling `flush()` on any
view drains the entire shared pending queue.

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

The shared-dict parameters are internal (prefixed with `_`). A root instance is
always created with no arguments: `InMemoryArrowDatabase()`.

**Config serialization:**

`to_config` adds `"base_path": list(self._path_prefix)`. Note that `from_config`
always reconstructs a root instance (empty `_tables`) since in-memory data does
not survive serialization; the `base_path` is preserved so the scoped position is
restored even though data is not.

---

### `ConnectorArrowDatabase`

**Internal changes:**

- Add `_path_prefix: tuple[str, ...]= ()`.
- Add `base_path` property returning `_path_prefix`.
- Update `_get_record_key` to prepend the prefix:

  ```python
  def _get_record_key(self, record_path: tuple[str, ...]) -> str:
      return "/".join(self._path_prefix + record_path)
  ```

  Because `flush()` reconstructs `record_path = tuple(record_key.split("/"))` and
  then calls `_path_to_table_name`, the prefix components are naturally included in
  the SQL table name (e.g. prefix `("pipeline",)` + path `("node1",)` →
  `"pipeline__node1"`). No changes to `flush()` are needed.

**`at()` implementation:**

Returns a new `ConnectorArrowDatabase` sharing the same `_connector` (SQL database
is the shared storage) and the pending dicts, with an extended `_path_prefix`:

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

`to_config` adds `"base_path": list(self._path_prefix)`. `from_config` currently
raises `NotImplementedError` (PLT-1074/1075/1076); no change to that.

---

### `NoOpArrowDatabase`

**Internal changes:**

- Add `_path_prefix: tuple[str, ...]= ()`.
- Add `base_path` property returning `_path_prefix`.
- All read/write behaviour unchanged (discards writes, returns `None` for reads).

**`at()` implementation:**

Returns a new `NoOpArrowDatabase` with the extended prefix. Prefix is tracked for
introspection but has no effect on storage (there is none).

```python
def at(self, *path_components: str) -> "NoOpArrowDatabase":
    return NoOpArrowDatabase(_path_prefix=self._path_prefix + path_components)
```

**Config serialization:**

`to_config` adds `"base_path": list(self._path_prefix)`. `from_config` restores
the prefix.

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
#   "base_path": ["run_2026", "transform_fn", "v1"],
#   "root_uri": "/experiments",
#   ...
# }
reconstructed = DeltaTableDatabase.from_config(config)
assert reconstructed.base_path == ("run_2026", "transform_fn", "v1")
```

---

## Testing

Each backend's existing test file gains a `TestAtMethod` class covering:

1. `base_path` is `()` on a fresh root instance.
2. `at("a", "b")` produces `base_path == ("a", "b")`.
3. `at("a").at("b")` is equivalent to `at("a", "b")`.
4. Writes through a scoped view are readable from the same view.
5. Writes through a scoped view are **not** visible via the parent at the same
   `record_path` (they live under the prefix namespace).
6. Writes through a scoped view **are** visible from a second view created with
   `at()` using the same path (shared storage test — applies to InMemory and
   Connector backends).
7. `to_config` / `from_config` round-trips `base_path`.

For `DeltaTableDatabase`, tests use `tmp_path` fixtures (pytest).
For `InMemoryArrowDatabase` and `ConnectorArrowDatabase`, no fixtures needed.

---

## Scope

**In scope (ENG-341):**

- `ArrowDatabaseProtocol`: add `base_path` property and `at()` method.
- `DeltaTableDatabase`: implement `at()` and `base_path`; rename internal attrs.
- `InMemoryArrowDatabase`: implement `at()` and `base_path` with shared storage.
- `ConnectorArrowDatabase`: implement `at()` and `base_path` with shared storage.
- `NoOpArrowDatabase`: implement `at()` and `base_path`.
- `to_config` / `from_config` updated for all backends.
- Unit tests for all backends.

**Out of scope:**

- Integrating `at()` into node/pipeline wiring (ENG-340).
- Cloud-path `list_sources()` (pre-existing limitation).
- `from_config` for `ConnectorArrowDatabase` (PLT-1074/1075/1076).
