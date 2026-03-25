# SpiralDB Exploration Findings

**Date:** 2026-03-25
**Linear:** PLT-1163
**Blocks:** PLT-1074 (SpiralDBConnector implementation)

---

## 1. Devenv Setup

### Authentication

pyspiral uses a device-code OAuth flow. Credentials are stored in
`~/.config/pyspiral/auth.json` and are **version-specific** — upgrading
pyspiral requires a fresh login.

```bash
uv run pyspiral login   # opens a browser device-code flow
```

The token is long-lived (JWT with ~24 h expiry; a refresh token is stored
alongside it).

### pyspiral version

`pyspiral>=0.11.6` has been added as an optional dependency under the
`spiraldb` extra in `pyproject.toml`:

```toml
[project.optional-dependencies]
spiraldb = ["pyspiral>=0.11.6"]
```

Install with `uv sync --extra spiraldb`.

### Dev project

Use **`test-orcapod-362211`** (`api.spiraldb.dev`) for exploration and
integration tests. It is empty by default and is scoped to the dev
environment.

### Verified connectivity

```python
import spiral
sp = spiral.Spiral()
sp.list_projects()   # returns 8 projects — connectivity confirmed
```

---

## 2. Design Question Answers (for PLT-1074)

### Q1 — Constructor scope: project-scoped or dataset-scoped?

**Dataset-scoped** (`project_id` + `dataset`).

Every project can contain multiple datasets, and tables with identical names
can exist in different datasets within the same project. Scoping the connector
to a dataset gives unambiguous table names. The dataset defaults to
`"default"`, which is where all real data currently lives.

```python
# proposed constructor
SpiralDBConnector(project_id="enigma-spiral-poc-r2-3-852265", dataset="default")
```

`project.table(identifier)` accepts either a bare table name (uses `default`
dataset) or a `"dataset.table"` qualified identifier.

### Q2 — Table naming: `"dataset.table"` or `"table"` strings?

**Plain `"table"` strings** within the connector's dataset scope.

`project.list_tables()` returns `TableResource` objects with separate
`.dataset` and `.table` fields. Since the connector is dataset-scoped,
`get_table_names()` filters to the connector's dataset and returns the plain
`.table` name only.

```python
def get_table_names(self) -> list[str]:
    resources = self._project.list_tables()
    return [r.table for r in resources if r.dataset == self._dataset]
```

Note: `TableResource.id` (e.g., `table_g4o78g`) is an internal opaque handle
and should not be exposed — it is distinct from the user-visible table name.

Attempting to create two tables with the same name in the same dataset raises
`409 Conflict`; `exist_ok=True` is the idempotent alternative.

### Q3 — Write support: upsert-by-key? Can we approximate `skip_existing=True`?

**`table.write()` is always upsert-by-key (INSERT OR REPLACE).**

Confirmed with live data: rows whose key columns match existing rows are
overwritten; rows with new keys are inserted.

`push_down_nulls` (default `False`) is unrelated to skip-existing — it
converts null structs to `{"field": null}` for SpiralDB's no-struct-null
constraint.

**There is no native `skip_existing=True` equivalent.** To approximate it:

1. Scan the table to retrieve all existing key-column values.
2. Filter the incoming `pa.Table` to rows whose keys are not in that set.
3. Write only the novel rows.

```python
# approximate skip_existing=True
existing = sp.scan(tbl.select()).to_table()
existing_keys = set(zip(*[existing.column(k).to_pylist() for k in pk_cols]))
mask = pa.array([
    tuple(row[k] for k in pk_cols) not in existing_keys
    for row in records.to_pylist()
])
tbl.write(records.filter(mask))
```

Key-only scans are **not** supported (SpiralDB requires at least one non-key
column group column in every scan), so the full row must be read.

### Q4 — Type fidelity: plain Arrow types or edge cases?

**Mostly plain Arrow types, with one notable edge case: timezone is silently
dropped from timestamps.**

| Vortex type (internal)                          | Arrow type from `schema().to_arrow()` |
|-------------------------------------------------|---------------------------------------|
| `utf8?`                                         | `string`                              |
| `i16? / i32? / i64?`                            | `int16 / int32 / int64`               |
| `u16? / u32?`                                   | `uint16 / uint32`                     |
| `f32?`                                          | `float`                               |
| `f64?`                                          | `double`                              |
| `bool?`                                         | `bool`                                |
| `list(T?)?`                                     | `list<item: T>`                       |
| `fixed_size_list(T?)[N]?`                       | `fixed_size_list<item: T>[N]`         |
| `vortex.timestamp[us, tz=America/Los_Angeles]`  | `timestamp[us]` ⚠️ **tz dropped**    |
| struct                                          | Arrow struct                          |

The timezone stripping is the only known fidelity gap. `schema().to_arrow()`
and scan results (`scan.to_table()`) return identical Arrow types — no
secondary mapping is needed.

---

## 3. Scan / Read API Summary

The pyspiral API is **expression-based**, not SQL. `ConnectorArrowDatabase`
calls only `iter_batches('SELECT * FROM "table_name"')`; the connector just
needs to parse the table name from that pattern and issue a full scan.

```python
# full table scan — equivalent to SELECT *
scan = sp.scan(tbl.select())
scan.to_table()            # → pa.Table
scan.to_record_batches()   # → RecordBatchReader (streaming)
```

`Scan.key_schema` returns a `Schema` with Vortex-notation types (e.g.
`utf8?`). Use `key_schema.to_arrow()` or `key_schema.names` for Arrow-native
access.

`get_pk_columns()` implementation:

```python
def get_pk_columns(self, table_name: str) -> list[str]:
    return self._project.table(f"{self._dataset}.{table_name}").key_schema.names
```

---

## 4. Key Schema Observations

Multi-column composite keys are common:

| Table             | PK columns                                        |
|-------------------|---------------------------------------------------|
| `session_meta`    | `session_id`, `subject`, `date`                   |
| `stim_trial_info` | `session_id`, `timestamp`, `trial_number`         |
| `spike_data`      | `session_id`, `timestamp`, `probe_id`, `unit_id`  |
| `gaze`            | `session_id`, `timestamp`                         |

`key_schema` is a `spiral.Schema` (same type as `table.schema()`). Both
support `.names` (list of column names) and `.to_arrow()` (Arrow schema).

---

## 5. `create_table` Notes

```python
project.create_table(
    "dataset.table_name",          # or just "table_name" for default dataset
    key_schema=[("id", pa.string()), ...],   # required — no default
    exist_ok=True,                 # idempotent
)
```

Key schema must be declared at creation time; it cannot be changed after.
All Arrow types tested (string, int32, float, double, bool, fixed_size_list)
are accepted.
