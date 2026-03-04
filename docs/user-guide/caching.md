# Caching & Persistence

orcapod uses a differentiated caching strategy across its three pod types — source, function,
and operator — reflecting the distinct computational semantics of each.

## The Three Caching Tiers

| Property | Source Pod | Function Pod | Operator Pod |
|----------|-----------|--------------|--------------|
| Cache scope | Content hash | Pipeline hash | Content hash |
| Default state | Always on | Always on | Off |
| Semantic role | Cumulative record | Reusable lookup | Historical record |
| Cross-source sharing | No | Yes | No |
| Computation on hit | Dedup and merge | Skip | Recompute by default |

## Source Pod Caching

Source caching is **always on**. Each source gets its own dedicated cache table scoped to
its content hash.

```python
from orcapod.core.sources import PersistentSource, ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase

source_db = InMemoryArrowDatabase()
source = PersistentSource(
    ArrowTableSource(table, tag_columns=["id"]),
    cache_database=source_db,
)
source.run()
```

**Behavior:**

- Each packet yielded by the source is stored, keyed by its content-addressable hash.
- On access, the source yields the **merged content of cache + new packets**.
- Deduplication is performed during merge using content-addressable hashes.
- The cache is a **correct cumulative record** of all data ever observed from that source.

**Named vs. unnamed sources:**

- **Named sources** (DeltaTable, CSV): `source_id = canonical name`. Identity is
  data-independent — same name + same schema = same cache table. Updates accumulate.
- **Unnamed sources** (ArrowTableSource): `source_id = table hash`. Identity is
  data-dependent — different data = different cache table.

## Function Pod Caching

Function pod caching is **always on** and split into two tiers:

1. **Packet-level cache (global)** — maps `input_hash → output_packet`. Shared across
   all pipelines.
2. **Tag-level cache (per structural pipeline)** — maps `tag → input_hash`. Scoped to
   `pipeline_hash()`.

```python
from orcapod import PersistentFunctionNode, FunctionPod
from orcapod.databases import InMemoryArrowDatabase

pipeline_db = InMemoryArrowDatabase()
result_db = InMemoryArrowDatabase()

node = PersistentFunctionNode(
    function_pod=pod,
    input_stream=input_stream,
    pipeline_database=pipeline_db,
    result_database=result_db,
)
node.run()
```

**Cross-source sharing:**

Because the cache is scoped to `pipeline_hash()` (schema + topology only, not data content),
different source instances with identical schemas **share the same cache table**. Rows are
distinguished by system tags.

```
Pipeline 1: clinic_a (3 patients) → Join → risk_score → cache table T
Pipeline 2: clinic_b (2 patients) → Join → risk_score → cache table T (same!)

Table T now has 5 rows, differentiated by system tag source_id columns.
```

**Two-phase iteration:**

1. **Phase 1** — yield cached results for inputs already in the database.
2. **Phase 2** — compute missing inputs, store results, yield.

## Operator Pod Caching

Operator caching is **off by default** and uses a three-tier opt-in model via `CacheMode`:

```python
from orcapod.core.operator_node import PersistentOperatorNode
from orcapod.types import CacheMode

# Default: compute only, no DB writes
node_off = PersistentOperatorNode(
    operator=join_op,
    input_streams=[source_a, source_b],
    pipeline_database=db,
    cache_mode=CacheMode.OFF,
)

# Log: compute AND write to DB (audit trail)
node_log = PersistentOperatorNode(
    operator=join_op,
    input_streams=[source_a, source_b],
    pipeline_database=db,
    cache_mode=CacheMode.LOG,
)

# Replay: skip computation, load from cache
node_replay = PersistentOperatorNode(
    operator=join_op,
    input_streams=[source_a, source_b],
    pipeline_database=db,
    cache_mode=CacheMode.REPLAY,
)
```

### CacheMode Summary

| Mode | Cache Writes | Computation | Use Case |
|------|-------------|-------------|----------|
| `OFF` | No | Always | Normal pipeline execution |
| `LOG` | Yes | Always | Audit trail, run-over-run comparison |
| `REPLAY` | No (prior) | Skipped | Explicitly flowing prior results downstream |

**Why off by default?**

Operators compute over entire streams (joins, aggregations). Their outputs are meaningful
only as a complete set. Unlike function pods, operator results cannot be safely mixed across
source combinations:

```
(X ⋈ Y) ∪ (X' ⋈ Y') ≠ (X ∪ X') ⋈ (Y ∪ Y')
```

The operator cache is an **append-only historical record**, not a cumulative materialization.

## Pipeline Database Scoping

### FunctionNode Pipeline Path

```
{prefix} / {function_name} / {output_schema_hash} / v{major_version} / {type_id} / node:{pipeline_hash}
```

### OperatorNode Pipeline Path

```
{prefix} / {operator_class} / {operator_content_hash} / node:{pipeline_hash}
```

### Multi-Source Table Sharing

Sources with identical schemas produce identical `pipeline_hash` values. Through the same
pipeline structure, they share database tables automatically:

```python
# Both use the same pipeline path (same schema, same topology)
node_a = PersistentFunctionNode(pod, stream_from_clinic_a, pipeline_database=db)
node_b = PersistentFunctionNode(pod, stream_from_clinic_b, pipeline_database=db)

assert node_a.pipeline_path == node_b.pipeline_path  # True!
```

## Database Backends

| Backend | Persistence | Best For |
|---------|------------|----------|
| `InMemoryArrowDatabase` | Process lifetime | Testing, exploration |
| `DeltaTableDatabase` | Disk (Delta Lake parquet) | Production pipelines |
