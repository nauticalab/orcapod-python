# Design: Refactor Pipeline into Source-Agnostic DAG; Introduce PipelineJob and SourceSpec

**Date:** 2026-05-19
**Issue:** ENG-456
**Status:** Approved

---

## Overview

Today's `Pipeline` conflates structural DAG definition with concrete runtime concerns: it holds
references to concrete sources, a data store, and an execution context, and it exposes a `run()`
method. This design separates those concerns into three distinct types:

- **`SourceSpec`** — a named, typed input-slot declaration. Describes what a pipeline input looks
  like (key schema + data schema) without referencing any concrete data.
- **`Pipeline`** — the pure computational DAG. All leaf inputs are `SourceSpec` instances.
  Carries no concrete data sources, no store, no execution context. The "explicit blueprint" path.
- **`PipelineJob`** — the everyday working object. Built incrementally: its `with`-block accepts
  both concrete sources and `SourceSpec`s as leaf inputs; concrete sources are recorded as
  bindings simultaneously with DAG construction. A `Pipeline` can always be extracted from a
  `PipelineJob` via `job.pipeline`.

`Pipeline` and `PipelineJob` are distinct types with no inheritance relationship. `PipelineJob`
holds a `Pipeline` (composition). The `Pipeline` inside a `PipelineJob` is derived from the
recorded DAG — concrete leaf sources are automatically represented as `SourceSpec`s using
`source.label` as the spec name.

---

## 1. `SourceSpec`

### Purpose

`SourceSpec` is a named, immutable schema declaration. It is the typed input-slot concept for
both `Pipeline` and `PipelineJob` — it describes what an input looks like independently of
where the data comes from.

### Definition

```python
@dataclass(frozen=True)
class SourceSpec:
    name: str                # required; part of identity
    tag_schema: Schema       # field name → key type
    data_schema: Schema      # field name → data type
```

### Identity and Hashing

`SourceSpec` is hashable as a Python object via `@dataclass(frozen=True)`. Its identity is
`(name, tag_schema, data_schema)` — two specs with the same schemas but different names are
distinct.

`SourceSpec.pipeline_hash()` is computed from `(tag_schema, data_schema)` only — no name. This
matches `RootSource.pipeline_identity_structure()`, which also returns schema pairs. Two specs
with identical schemas share the same pipeline path, which is correct: pipeline hash is for
structural/schema scope, not name-based identity.

`SourceSpec.content_hash()` is computed from `(name, tag_schema, data_schema)` — name included.
Two specs with the same schemas but different names are distinct elements and hash differently at
the content level.

### Stream Protocol Surface

`SourceSpec` implements the minimal subset of `StreamProtocol` required to be passed as an
upstream in operator chains during a `with`-block:

- `output_schema()` → `(tag_schema, data_schema)`
- `keys()` → derived from schemas
- `pipeline_hash()` / `content_hash()` → as above

Data-producing methods raise `UnboundSourceError`:

- `iter_data()` → raises `UnboundSourceError("SourceSpec '{name}' is not bound to a concrete source")`
- `as_table()` → raises `UnboundSourceError`

`SourceSpec` has no reference to any data store, execution context, or pipeline instance.

### Validation Against a Concrete Source

```python
SourceSpec.validate(source: StreamProtocol) -> None
```

Checks that `source.output_schema()` is compatible with `(tag_schema, data_schema)`. Raises
`SourceSpecMismatchError` with a message naming the spec and the incompatible field(s). Called at
`bind()` time — schema mismatches are rejected before execution, never at run time.

---

## 2. `Pipeline` (Refactored)

### Purpose

`Pipeline` is the strict, pure computational blueprint. All leaf inputs are `SourceSpec`
instances — no concrete sources, no store, no execution context. This is the "explicit blueprint"
path, used when you want to define a reusable, fully abstract DAG before deciding on any concrete
inputs.

### Relationship to the Old `Pipeline`

The old `Pipeline` held databases, execution config, and a `run()` method — essentially the
same concerns that now live in `PipelineJob`. `PipelineJob` is the functional successor to the
old `Pipeline`: it absorbs all execution, database, and configuration surface. The new `Pipeline`
is a strict subset — topology and `SourceSpec` declarations only.

### What Changes

**Removed from `Pipeline`:**
- `_pipeline_database`, `_result_database`, and all scoped database views
- `_auto_save_path`
- `run()` method
- `_default_observer` and observer construction
- `PipelineConfig` / execution engine parameters from `__init__`

**Kept on `Pipeline`:**
- `with`-block recording mechanism (`AutoRegisteringContextBasedTracker` inheritance,
  `record_function_pod_invocation`, `record_operator_pod_invocation`)
- `compile()` — walks recorded edges; leaf `SourceSpec` instances become `SourceNode`s wrapping
  the spec. Raises `ValueError` if any leaf stream is not a `SourceSpec`.
- `_hash_graph`, `_node_graph`, `_persistent_node_map`, `_nodes` — topological structure
- `name` — pipeline name tuple (DAG identity)
- `save()` / `load()` — persistence of the pure blueprint

**New constructor:**
```python
pipeline = Pipeline(name="my_pipe")
```

### Declaring Inputs

All leaf inputs in a `Pipeline` must be `SourceSpec` instances:

```python
spec_a = SourceSpec("input_a", tag_schema=..., data_schema=...)
spec_b = SourceSpec("input_b", tag_schema=..., data_schema=...)

pipeline = Pipeline(name="my_pipe")
with pipeline:
    result = Join()(spec_a, spec_b)
```

`Pipeline.compile()` raises `ValueError` if any leaf stream is not a `SourceSpec`.

### `bind()` — Entry Point to `PipelineJob`

`Pipeline` exposes `bind()` with the same signature as `PipelineJob.bind()` (see Section 3).
Calling it wraps `self` into a fresh `PipelineJob` with the given components. `Pipeline` is
unchanged.

### `Pipeline.save()` — Pure Blueprint

Serializes topology and `SourceSpec` declarations (name + schemas) only. No concrete source
configs, no store, no execution context, no run metadata.

```python
pipeline.save("pipelines/my_pipeline.json")
```

Format version key: `"orcapod_pipeline_version"` (unchanged from current).

---

## 3. `PipelineJob`

### Purpose

`PipelineJob` is the everyday working object. It is built incrementally — its `with`-block
records both the DAG structure and any concrete source bindings simultaneously. It holds a
`Pipeline` (derived from the recorded DAG) plus accumulated bindings: concrete sources, a store,
and/or an execution context.

`PipelineJob` covers the full range:

| State | Description |
|---|---|
| Under construction | `with job:` block in progress |
| Partially configured | Some sources / store / context bound, not yet run |
| Fully configured | All specs bound, store and context set, ready to run |
| Partial run record | Ran resolvable subgraph; some `SourceSpec` slots still unbound |
| Complete run record | Full DAG executed |
| Failed run record | Execution encountered an error |

### State

```python
class PipelineJob:
    pipeline: Pipeline                           # derived from recorded DAG
    sources: dict[str, StreamProtocol]           # SourceSpec name → concrete source
    store: ArrowDatabaseProtocol | None
    execution_context: ExecutionContext | None

    # Run metadata (None until run() is called)
    run_id: str | None
    status: RunStatus | None                     # pending | partial | complete | failed
    started_at: datetime | None
    completed_at: datetime | None
    unresolved_specs: list[str]                  # spec names unbound at run time
    results: dict[str, str]                      # node label → content-addressed path
```

### `with PipelineJob:` — Recording DAG and Bindings Together

`PipelineJob` supports the same `with`-block recording mechanism as `Pipeline`. Both concrete
sources and `SourceSpec`s may appear as leaf inputs:

```python
job = PipelineJob(store=db)
with job:
    src_a = SomeSource(...)                      # concrete source
    spec_b = SourceSpec("input_b", ...)          # unbound spec
    result = Join()(src_a, spec_b)
```

**Concrete leaf sources** are handled as follows:
- A `SourceSpec` is automatically created from the source: `SourceSpec(name=source.label, tag_schema=..., data_schema=...)`.
- The source is recorded in `job.sources` under `source.label`.
- The underlying `Pipeline` represents this leaf as a `SourceSpec` — no concrete source is
  stored in `Pipeline`.

**`SourceSpec` leaf inputs** are recorded as unbound slots in `job.pipeline`; they are not added
to `job.sources` unless explicitly bound via `bind()`.

After the `with` block, `job.pipeline` is a fully compiled, pure `Pipeline` where every leaf
node is a `SourceSpec`.

### Extracting a Pure `Pipeline`

At any point, `job.pipeline` gives you the pure `Pipeline` corresponding to the recorded DAG.
This is the same `Pipeline` you would have built manually using `with Pipeline:` and explicit
`SourceSpec`s — concrete sources appear as `SourceSpec(name=source.label, ...)`.

```python
# These two are equivalent:

# Via PipelineJob (common case)
job = PipelineJob(store=db)
with job:
    result = Join()(SomeSource(label="input_a"), AnotherSource(label="input_b"))
pipeline = job.pipeline   # ← pure Pipeline, leaf specs named "input_a", "input_b"

# Via Pipeline directly (explicit blueprint)
spec_a = SourceSpec("input_a", tag_schema=..., data_schema=...)
spec_b = SourceSpec("input_b", tag_schema=..., data_schema=...)
pipeline = Pipeline(name="my_pipe")
with pipeline:
    result = Join()(spec_a, spec_b)
```

### Binding API — `bind()`

Identical signature on both `Pipeline` and `PipelineJob`. Always non-mutating — returns a new
`PipelineJob` leaving the receiver unchanged.

```python
def bind(
    self,
    sources: dict[str, StreamProtocol] | None = None,
    store: ArrowDatabaseProtocol | None = None,
    execution_context: ExecutionContext | None = None,
) -> PipelineJob: ...
```

- On `Pipeline`: wraps `self` into a fresh `PipelineJob` with the given components
- On `PipelineJob`: returns a new `PipelineJob` with bindings merged/substituted; existing
  bindings not mentioned are carried forward unchanged
- `SourceSpec.validate(source)` is called at `bind()` time for each supplied source;
  `SourceSpecMismatchError` is raised immediately on schema mismatch

Key vocabulary (matches `bind()` parameter names):
- `"sources"` → `{spec_name: concrete_source}`
- `"store"` → `ArrowDatabaseProtocol`
- `"execution_context"` → `ExecutionContext`

### Completeness Introspection

```python
job.unbound_specs() -> list[SourceSpec]
# All SourceSpec slots in the pipeline not yet bound in this PipelineJob

job.is_complete() -> bool
# True when all specs are bound, store is set, and execution context is set (or defaultable)

job.is_runnable(node_label: str) -> bool
# True if all upstream nodes of node_label have their inputs fully resolved
```

### Execution — `run()`

```python
job.run(observer: ExecutionObserverProtocol | None = None) -> PipelineJob
```

Returns a new `PipelineJob` with run metadata populated (does not mutate `self`).

**Execution steps:**

1. Build the **resolved execution graph**: walk the pipeline's `_node_graph`; for each leaf
   `SourceNode` wrapping a `SourceSpec`, substitute with a concrete `SourceNode` using the bound
   source (looked up by spec name in `job.sources`). Nodes whose upstream includes an unbound
   `SourceSpec` — and all their dependents — are excluded from the resolved graph.
2. Attach `store` databases to all nodes in the resolved graph (same scoping logic as current
   `Pipeline.compile()`).
3. Apply execution context settings.
4. Delegate to the existing orchestrator with the resolved graph.
5. Record `unresolved_specs` — spec names that were unbound and caused subgraph exclusion. This
   is a structured outcome, not an error.
6. Flush databases.
7. Return a new `PipelineJob` with `status`, `run_id`, `started_at`, `completed_at`,
   `unresolved_specs`, and `results` populated.

**Partial execution:** if some `SourceSpec` slots are unbound, the resolvable subgraph executes
and halts cleanly at unresolved boundaries. Re-running after binding the missing specs (via
`bind()`) produces the full result; previously computed nodes are reused automatically via
content-addressed storage.

### `PipelineJob.save()` — Template or Run Record

The same format covers both "configured template" (not yet run) and "run record" (executed).
The `run.*` fields are null/empty until `run()` is called.

```json
{
  "orcapod_pipeline_job_version": "0.1.0",
  "run": {
    "run_id": null,
    "status": "pending",
    "started_at": null,
    "completed_at": null,
    "unresolved_specs": [],
    "results": {}
  },
  "pipeline": "<path to pipeline JSON or inline>",
  "bindings": {
    "sources": {
      "spec_name": { "source_type": "...", "source_config": {...} }
    },
    "store": { "type": "delta_table", "path": "..." },
    "execution_context": { "config": {...} }
  }
}
```

- `pipeline` references the `Pipeline`'s saved JSON by path; inline is a fallback when the
  Pipeline has not been separately saved
- `status` values: `pending`, `partial`, `complete`, `failed`
- Dangling result pointers (store moved/deleted) surface as `LoadStatus.CACHE_ONLY` or
  `LoadStatus.UNAVAILABLE` on load — not an error

Format version key: `"orcapod_pipeline_job_version"` (distinct from `"orcapod_pipeline_version"`).

**Typical workflows:**

```python
# Workflow A — PipelineJob-first (common case)
job = PipelineJob(store=db)
with job:
    result = Join()(SomeSource(label="data"), AnotherSource(label="reference"))
completed = job.run()
completed.save("runs/run_001.json")

# Workflow B — explicit Pipeline blueprint, then bind
pipeline = Pipeline(name="my_pipe")
with pipeline:
    result = Join()(SourceSpec("data", ...), SourceSpec("reference", ...))
pipeline.save("pipelines/my_pipeline.json")

job = pipeline.bind(
    sources={"data": SomeSource(...), "reference": AnotherSource(...)},
    store=db,
)
completed = job.run()
completed.save("runs/run_001.json")

# Workflow C — save PipelineJob as a shareable configured template
template = pipeline.bind(sources={"data": known_source}, store=shared_db)
template.save("pipelines/my_pipeline_configured.json")
```

---

## 4. `ExecutionContext` (Stub)

`ExecutionContext` is introduced as a minimal placeholder in this issue. Its full definition
(including how `PipelineConfig` maps into it) is deferred to a follow-up issue.

```python
@dataclass(frozen=True)
class ExecutionContext:
    config: PipelineConfig | None = None
```

---

## 5. Hashing Correctness

**`SourceSpec.pipeline_hash()`** is schema-only: `hash(tag_schema, data_schema)`. This matches
`RootSource.pipeline_identity_structure()` exactly, so a `SourceSpec` and a schema-compatible
concrete source produce the same `pipeline_hash()` — and therefore the same DB table paths.

**Content-address correctness falls out of the existing chain — no new mechanism needed:**

- `SourceSpec.content_hash()` = `hash(name, tag_schema, data_schema)` — name-inclusive, so two
  specs with the same schemas but different names are distinct elements
- A concrete `SourceNode`'s `content_hash()` is derived from the concrete source's data content
  and schema
- When `PipelineJob.run()` substitutes a bound `SourceNode` for a `SourceSpec`-backed `SourceNode`,
  downstream nodes receive the concrete source's `content_hash()` as their upstream hash input
- Two `PipelineJob`s binding different concrete sources to the same spec → different upstream
  `content_hash()` → different downstream `content_hash()` → different result paths → no collision

**Audit during implementation:** verify that `SourceSpec.pipeline_hash()` and a schema-compatible
concrete source's `pipeline_hash()` produce identical values (both are `hash(tag_schema,
data_schema)`). This is the property that enables result reuse: two `PipelineJob`s binding
different concrete sources with the same schema to the same spec map to the same DB table paths,
and only diverge at the `content_hash()` level (different data → different rows/partitions).

---

## 6. Observer Integration

The existing `ExecutionObserverProtocol` is unchanged in surface. `PipelineJob.run()` accepts an
optional `observer` parameter. Partial execution (unresolved boundary) is a **first-class
outcome**, not an error:

- Excluded nodes (unbound upstream) are not passed to `on_node_start` / `on_node_end` / `on_data_start` / `on_data_end`
- `unresolved_specs` in the run record captures which spec names caused exclusion
- Observer implementations need no changes — they simply never see the excluded nodes

---

## 7. Tests

### Migration note

The old `Pipeline` class was the primary execution object — it held databases, ran nodes, and was
the target of most existing pipeline tests (compilation, execution, database handling,
serialization, observer integration). `PipelineJob` is its functional successor and absorbs all
of that surface. Existing tests that targeted the old `Pipeline`'s execution behaviour should be
migrated to target `PipelineJob`. The new `Pipeline` is structural-only, so tests for it cover
only DAG construction, `SourceSpec` enforcement, and blueprint save/load.

### Test cases

| Test | What it verifies |
|---|---|
| `with PipelineJob:` with concrete source leaf; assert `job.sources` populated and `job.pipeline` has matching `SourceSpec` named from `source.label` | PipelineJob DAG recording + auto-SourceSpec |
| `with PipelineJob:` with mixed concrete + `SourceSpec` leaves; assert concrete source bound, spec unbound | Mixed leaf recording |
| `with Pipeline:` with concrete source leaf raises `ValueError` | Pipeline enforces SourceSpec-only |
| `pipeline.bind(sources={...})` returns a `PipelineJob`; original `Pipeline` unchanged | Non-mutating bind on Pipeline |
| `job.bind(sources={...})`, `job.bind(store=...)`, `job.bind(execution_context=...)` each return a new `PipelineJob`; original unchanged | Non-mutating bind on PipelineJob |
| `pipeline.bind(sources=..., store=..., execution_context=...)` equivalent to chained `bind()` calls | `bind()` consistency |
| Run a complete `PipelineJob` end-to-end | Happy path |
| Run an incomplete `PipelineJob`; only resolvable subgraph executes; `unresolved_specs` populated | Partial execution |
| `PipelineJob.save()` / `PipelineJob.load()` round-trip for both template and run record | PipelineJob persistence |
| `Pipeline.save()` / `Pipeline.load()` round-trip; `SourceSpec` declarations preserved | Blueprint persistence |
| Schema-mismatched source rejected at `bind()` with `SourceSpecMismatchError` | Binding-time validation |
| Result reuse: run incomplete `PipelineJob`, then complete version on same store; previously computed nodes not recomputed | Content-address reuse |
| Result divergence: two `PipelineJob`s differing in one bound source; downstream nodes recompute | Content-address correctness |

---

## 8. Scope and Boundaries

**In scope:**
- `SourceSpec` type (new)
- `PipelineJob` type (new)
- Refactor of `Pipeline` to remove execution surface and enforce SourceSpec-only leaves
- `with PipelineJob:` recording both DAG structure and concrete bindings
- `job.pipeline` extraction of pure `Pipeline` from a `PipelineJob`
- `bind()` on both `Pipeline` and `PipelineJob`
- Partial / progressive execution of incomplete `PipelineJob`s
- `PipelineJob` save/load as template and run record
- `Pipeline` save/load as pure blueprint
- Hashing correctness audit
- Updated examples, README, quick-start

**Out of scope:**
- `ExecutionContext` full definition (follow-up issue)
- `PipelineConfig` integration into `ExecutionContext` (follow-up issue)
- `tags → keys` rename (ENG-1435, blocking)
- Distributed / remote execution
- orcapod-rust mirror
- Backwards compatibility / migration shims
