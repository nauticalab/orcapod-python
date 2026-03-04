# OrcaPod — Design Specification

---

## Core Abstractions

### Datagram

The **datagram** is the universal immutable data container in OrcaPod. A datagram holds named columns with explicit type information and supports lazy conversion between Python dict and Apache Arrow representations. Datagrams come in two specialized forms:

- **Tag** — metadata columns attached to a packet for routing, filtering, and annotation. Tags carry additional **system tags** — framework-managed hidden provenance columns that are excluded from content identity by default.

- **Packet** — data columns carrying the computational payload. Packets carry additional **source info** — per-column provenance tokens tracing each value back to its originating source and record.

Datagrams are always constructed from either a Python dict or an Arrow table/record batch. The alternative representation is computed lazily and cached. Content hashing always uses the Arrow representation; value access always uses the Python dict.

### Stream

A **stream** is a sequence of (Tag, Packet) pairs over a shared schema. Streams define two column groups — tag columns and packet columns — and provide lazy iteration, table materialization, and schema introspection. Streams are the fundamental data-flow abstraction: every source emits one, every operator consumes and produces them, and every function pod iterates over them.

The concrete implementation is `ArrowTableStream`, backed by an immutable PyArrow Table with explicit tag/packet column assignment.

### Source

A **source** acts as a stream from external data with no upstream dependencies, forming the base case of the pipeline graph. Sources establish provenance: each row gets a source-info token and system tag columns encoding the source's identity.

- **Root source** — loads data from the external world (file, database, in-memory table). All root sources delegate to `ArrowTableSource`, which wraps the data in an `ArrowTableStream` with provenance annotations. Concrete subclasses include `CSVSource`, `DeltaTableSource`, `DataFrameSource`, `DictSource`, and `ListSource`.

- **Derived source** — wraps the computed output of a `FunctionNode` or `OperatorNode`, reading from their pipeline database. Represents an explicit materialization declaration — an intermediate result given durable identity in the pipeline database, detached from the upstream topology that produced it.

Every source has a `source_id` — a canonical registry name used to register the source in a `SourceRegistry` so that provenance tokens in downstream data can be resolved back to the originating source. If not explicitly provided, `source_id` defaults to a truncated content hash.

### Function Pod

A **function pod** wraps a **packet function** — a stateless computation that consumes a single packet and produces an output packet. Function pods never inspect tags or stream structure; they operate purely on packet content. When given multiple input streams, a function pod joins them via a configurable multi-stream handler (defaulting to `Join`) before iterating.

Packet functions support pluggable executors (see **Packet Function Executor System**). When an executor is set, `call()` routes through `executor.execute()` and `async_call()` routes through `executor.async_execute()`. When no executor is set, the function's native `direct_call()` / `direct_async_call()` is invoked directly. For `PythonPacketFunction`, `direct_async_call` runs the synchronous function in a thread pool via `asyncio.run_in_executor`.

Two execution models exist:

- **FunctionPod + FunctionPodStream** — lazy, in-memory evaluation. The function pod processes each (tag, packet) pair from the input stream on demand, caching results by index. When the attached executor declares `supports_concurrent_execution = True`, `iter_packets()` materializes all remaining inputs and dispatches them concurrently via `asyncio.gather` over `async_call`, yielding results in order.

- **FunctionNode** — database-backed evaluation with incremental computation. Execution proceeds in two phases:
  1. **Phase 1**: yield cached results from the pipeline database for inputs whose hashes are already stored.
  2. **Phase 2**: compute results for any remaining input packets, store them in the database, and yield.

  Pipeline database scoping uses `pipeline_hash()` (schema+topology only), so FunctionNodes with identical functions and schema-compatible sources share the same database table.

### Operator

An **operator** is a structural pod that transforms streams without synthesizing new packet values. Every packet value in an operator's output must be traceable to a concrete value already present in the input packets — operators perform joins, merges, splits, selections, column renames, batching, and tag operations within this constraint.

Operators are subclasses of `StaticOutputPod` organized by input arity:

| Base Class | Arity | Examples |
|---|---|---|
| `UnaryOperator` | Exactly 1 input | Batch, SelectTagColumns, DropPacketColumns, MapTags, MapPackets, PolarsFilter |
| `BinaryOperator` | Exactly 2 inputs | MergeJoin, SemiJoin |
| `NonZeroInputOperator` | 1 or more inputs | Join |

Each operator declares its **argument symmetry** — whether inputs commute (`frozenset`, order-invariant) or have fixed positions (`tuple`, order-dependent). This determines how upstream hashes are combined for pipeline identity.

The `OperatorNode` is the database-backed counterpart, analogous to `FunctionNode` for function pods. It applies the operator, materializes the output with per-row record hashes, and stores the result in the pipeline database.

Every operator inherits a default barrier-mode `async_execute` from its base class (collect all inputs, run `static_process`, emit results). Subclasses can override for streaming or incremental strategies (see **Execution Models**).

---

## Operator Catalog

### Join
Variable-arity inner join on shared tag columns. Non-overlapping packet columns are required — colliding packet columns raise `InputValidationError`. Tag schema is the union of all input tag schemas; packet schema is the union. Inputs are canonically ordered by `pipeline_hash` for deterministic system tag column naming. Commutative (declared via `frozenset` argument symmetry).

### MergeJoin
Binary inner join that handles colliding packet columns by merging their values into sorted `list[T]`. Colliding columns must have identical types. Non-colliding columns are kept as scalars. Corresponding source-info columns are reordered to match the sort order of their packet column. Commutative — commutativity comes from sorting merged values, not from ordering input streams.

### SemiJoin
Binary semi-join: returns entries from the left stream that match on overlapping columns in the right stream. Output schema matches the left stream exactly. Non-commutative.

### Batch
Groups rows into batches of a configurable size. All column types become `list[T]`. Optionally drops incomplete final batches.

### SelectTagColumns / SelectPacketColumns
Keep only specified tag or packet columns. Optional `strict` mode raises on missing columns.

### DropTagColumns / DropPacketColumns
Remove specified tag or packet columns. `DropPacketColumns` also removes associated source-info columns.

### MapTags / MapPackets
Rename tag or packet columns via a name mapping. `MapPackets` automatically renames associated source-info columns. Optional `drop_unmapped` mode removes columns not in the mapping.

### PolarsFilter
Applies Polars filtering predicates to rows. Output schema is unchanged from input.

---

## Schema as a First-Class Citizen

Every stream exposes `output_schema()` returning `(tag_schema, packet_schema)` as `Schema` objects — immutable mappings from field names to Python types with support for optional fields. Schema is embedded explicitly at every level rather than resolved against a central registry, making streams fully self-describing.

The `ColumnConfig` dataclass controls what metadata columns are included in schema and data output:

| Field | Controls |
|---|---|
| `meta` | System metadata columns (`__` prefix) |
| `context` | Data context column |
| `source` | Source-info provenance columns (`_source_` prefix) |
| `system_tags` | System tag columns (`_tag_` prefix) |
| `content_hash` | Per-row content hash column |
| `sort_by_tags` | Whether to sort output by tag columns |

Operators predict their output schema — including system tag column names — without performing the actual computation.

---

## Tags

Tags are key-value pairs attached to every packet providing human-friendly metadata for navigation, filtering, and annotation. They are:

- **Non-authoritative** — never used for cache lookup or pod identity computation
- **Auto-propagated** — tags flow forward through the pipeline automatically
- **The basis for joins** — operator pods join streams by matching tag keys, never by inspecting packet content

**Tag merging in joins:**
- **Shared tag keys** — act as the join predicate; values must match for packets to be joined
- **Non-shared tag keys** — propagate freely into the joined output's tags

---

## Operator / Function Pod Boundary

This is a strict and critical separation:

| | Operator | Function Pod |
|---|---|---|
| Inspects packet content | Never | Yes |
| Inspects / uses tags | Yes | No |
| Can rename columns | Yes | No |
| Stream arity | Configurable (unary/binary/N-ary) | Single stream in, single stream out |
| Cached by content hash | No | Yes |
| Synthesizes new values | No | Yes |

Column renaming by operators allows join conflicts to be avoided without contaminating source info — the column name changes but the source info pointer remains intact, always traceable to the original producing pod.

---

## Identity and Hashing

OrcaPod maintains two parallel identity chains implemented as recursive Merkle-like hash trees:

### Content Hash (`content_hash()`)

Data-inclusive identity capturing the precise semantic content of an object:

| Component | What Gets Hashed |
|---|---|
| RootSource | Class name + tag columns + table content hash |
| PacketFunction | URI (canonical name + output schema hash + version + type ID) |
| FunctionPodStream | Function pod + argument symmetry of inputs |
| Operator | Operator class + identity structure |
| ArrowTableStream | Producer + upstreams (or table content if no producer) |
| Datagram | Arrow table content |
| DerivedSource | Origin node's content hash |

Content hashes use a `BaseSemanticHasher` that recursively expands structures, dispatches to type-specific handlers, and terminates at `ContentHash` leaves (preventing hash-of-hash inflation).

### Pipeline Hash (`pipeline_hash()`)

Schema-and-topology-only identity used for database path scoping. Excludes data content so that different sources with identical schemas share database tables:

| Component | What Gets Hashed |
|---|---|
| RootSource | `(tag_schema, packet_schema)` — base case |
| PacketFunction | Raw packet function object (via content hash) |
| FunctionPodStream | Function pod + input stream pipeline hashes |
| Operator | Operator class + argument symmetry (pipeline hashes of inputs) |
| ArrowTableStream | Producer + upstreams pipeline hashes (or schema if no producer) |
| DerivedSource | Inherited from RootSource: `(tag_schema, packet_schema)` |

Pipeline hash uses a **resolver pattern** — a callback that routes `PipelineElementProtocol` objects through `pipeline_hash()` and other `ContentIdentifiable` objects through `content_hash()` — ensuring the correct identity chain is used for nested objects within a single hash computation.

### ContentHash Type

All hashes are represented as `ContentHash` — a frozen dataclass pairing a method identifier (e.g., `"object_v0.1"`, `"arrow_v2.1"`) with raw digest bytes. The method name enables detecting version mismatches across hash configurations. Conversions: `.to_hex()`, `.to_int()`, `.to_uuid()`, `.to_base64()`, `.to_string()`.

### Argument Symmetry and Upstream Commutativity

Each pod declares how upstream hashes are combined:

- **Commutative** (`frozenset`) — upstream hashes sorted before combining. Used when input order is semantically irrelevant (Join, MergeJoin).
- **Non-commutative** (`tuple`) — upstream hashes combined in declared order. Used when input position is significant (SemiJoin).
- **Partial symmetry** — nesting expresses mixed constraints, e.g. `(frozenset([a, b]), c)`.

---

## Packet Function Signatures

Every packet function has a unique signature reflecting its input/output schemas and implementation. The function's URI encodes:

```
(canonical_function_name, output_schema_hash, major_version, packet_function_type_id)
```

For Python functions specifically, the identity structure includes the function's bytecode hash, input parameters signature, and Git version information.

---

## Source Info

Every packet column carries a **source info** string — a provenance pointer to the source and record that produced the value:

```
{source_id}::{record_id}::{column_name}
```

Where:
- `source_id` — canonical identifier of the originating source (defaults to content hash)
- `record_id` — row identifier, either positional (`row_0`) or column-based (`user_id=abc123`)
- `column_name` — the original column name

Source info columns are stored with a `_source_` prefix and are excluded from content hashing and standard output by default. They are included when `ColumnConfig(source=True)` is set.

Source info is **immutable through the pipeline** — set once when a source creates the data and preserved through all downstream operator transformations including column renames.

---

## System Tags

System tags are **framework-managed, hidden provenance columns** automatically attached to every packet. Unlike user tags, they are authoritative and guaranteed to maintain perfect traceability from any result row back to its original source rows.

### Flat Column Design

System tags store `source_id` and `record_id` as **separate flat columns** rather than a combined string value. This is a deliberate design choice driven by the caching strategy (see **Caching Strategy** section below).

In function pod cache tables, which are scoped to a structural pipeline hash and thus shared across different source combinations, filtering by source identity is a first-class operation. Storing `source_id` and `record_id` as separate columns makes this a straightforward equality predicate (`WHERE _tag_source_id::schema1 = 'X'`) with clean standard indexing, rather than a prefix match or string parse against a combined value.

This is safe because within any given cache table, the system tag schema is fixed — every row has the same set of system tag fields, determined by the pipeline structure. The column count grows with pipeline depth (more join stages produce more system tag column pairs), but this growth is per-table-schema, not within a table. Different pipeline structures produce different tables with different column layouts, which is the expected and correct behavior.

### Source System Tags

Each source automatically adds a pair of system tag columns using the `_tag_` prefix convention:

```
_tag_source_id::{schema_hash}    — the source's canonical source_id
_tag_record_id::{schema_hash}    — the row identifier within that source
```

Where `schema_hash` is derived from the source's `(tag_schema, packet_schema)`. The `::` delimiter separates segments of the system tag column name, maintaining consistency with the extension pattern used downstream.

Example at the root level:

```
_tag_source_id::schema1   (e.g., value: "customers_2024")
_tag_record_id::schema1   (e.g., value: "row_42" or "user_id=abc123")
```

### Three Evolution Rules

**1. Name-Preserving (~90% of operations)**
Single-stream operations (filter, select, rename, batch, map). System tag column names and values pass through unchanged.

**2. Name-Extending (multi-input operations)**
Joins and merges. Each incoming system tag column name is extended by appending `::node_pipeline_hash:canonical_position`. The `::` delimiter separates each extension segment, and `:` separates the pipeline hash from the canonical position within a segment. Canonical position assignment respects commutativity — for commutative operations, inputs are sorted by `pipeline_hash` to ensure identical column names regardless of wiring order.

For example, joining two streams that each carry `_tag_source_id::schema1` / `_tag_record_id::schema1`, through a join with pipeline hash `abc123`:

```
_tag_source_id::schema1::abc123:0    _tag_record_id::schema1::abc123:0    (first stream by canonical position)
_tag_source_id::schema1::abc123:1    _tag_record_id::schema1::abc123:1    (second stream by canonical position)
```

A subsequent join (pipeline hash `def456`) over those results would further extend:

```
_tag_source_id::schema1::abc123:0::def456:0
_tag_record_id::schema1::abc123:0::def456:0
```

The full column name is a chain of `::` delimited segments tracing the provenance path: `_tag_{field}::{source_schema_hash}::{join1_hash}:{position}::{join2_hash}:{position}::...`

**3. Type-Evolving (aggregation operations)**
Batch and similar grouping operations. Column names are unchanged but types evolve: `str → list[str]` as values collect all contributing source row IDs. Both `source_id` and `record_id` columns evolve independently.

### System Tag Value Sorting

For commutative operators (Join, MergeJoin), system tag values from same-`pipeline_hash` streams are sorted per row after the join. This ensures `Op(A, B)` and `Op(B, A)` produce identical system tag columns and values.

### Schema Prediction

Operators predict output system tag column names at schema time — without performing the actual computation — by computing `pipeline_hash` values and canonical positions. This is exposed via `output_schema(columns={"system_tags": True})`.

---

## Caching Strategy

OrcaPod uses a differentiated caching strategy across its three pod types — source, function, and operator — reflecting the distinct computational semantics of each. The guiding principle is that caching behavior should follow naturally from whether the computation is **cumulative**, **independent**, or **holistic**.

### Source Pod Caching

**Cache table identity:** Canonical source identity (content hash).

Each source gets its own dedicated cache table. Sources are provenance roots — there is no upstream system tag mechanism to disambiguate rows from different sources within a shared table. A cached source table represents a cumulative record of all packets ever observed from that specific source.

**Behavior:**
- Cache is **always on** by default.
- Each packet yielded by the source is stored in the cache table keyed by its content-addressable hash.
- On access, the source pod yields the **merged content of the cache and any new packets** from the live source.
- **Deduplication is performed at the source pod level** during merge, using content-addressable packet hashes. This ensures the yielded stream represents the complete known universe from the source with no redundancy.

**Semantic guarantee:** The cache is a **correct cumulative record**. The union of cache + live packets is the full set of data ever available from that source.

### Function Pod Caching

Function pod caching is split into two tiers:

1. **Packet-level cache (global):** Maps input packet hash → output packet. Shared globally across all pipelines, enabling identical function calls to reuse results regardless of context.
2. **Tag-level cache (per structural pipeline):** Maps tag → input packet hash. Scoped to the structural pipeline hash.

**Tag-level cache table identity:** Structural pipeline hash (`pipeline_hash()`).

A single cache table is used for all runs of structurally identical pipelines (same tag and packet schemas at source, followed by the same sequence of operator and function pods), regardless of which specific source combinations were involved. This is safe because function pods operate on individual packets independently — each cached mapping is self-contained and valid regardless of what other rows exist in the table.

**Why structural hash, not content hash:**
- System tags already carry full provenance, including source identity as separate queryable columns. Rows from different source combinations are distinguishable within a shared table via equality predicates on `source_id` columns (e.g., `WHERE _tag_source_id::schema1 = 'X'`).
- A shared table provides a natural **cross-source view** — comparing how the same analytical pipeline behaves across different source populations without needing cross-table joins.
- Content-hash scoping would duplicate disambiguation that system tags already provide, violating the principle against redundant mechanisms.

**Behavior:**
- Cache is **always on** by default.
- On a pipeline run, incoming packets are scoped to the current source combination (determined by upstream source pods).
- The function pod checks the tag-level cache for existing mappings among the incoming tag-packets.
- **Cache hits** (from this or any prior run over the same structural pipeline) are yielded directly. Cross-source sharing falls out naturally because packet-level computation is source-independent.
- **Cache misses** trigger computation; results are stored in both the packet-level and tag-level caches.

**Semantic guarantee:** The cache is a **correct reusable lookup**. Every entry is independently valid. The table as a whole is a historical record of all computations processed through this function within this structural pipeline context.

**User guidance:** If a user finds the mixture of results from different source combinations within one table to be unpredictable or undesirable, they should separate pipeline identity explicitly (e.g., by parameterizing the pipeline to produce distinct structural hashes).

### Operator Pod Caching

**Cache table identity:** Content hash (structural pipeline hash + identity hashes of all upstream sources).

Each unique combination of pipeline structure and source identities gets its own cache table. This reflects the fact that operator results are holistic — they depend on the entire input stream, not individual packets.

**Why content hash, not structural hash:**
Operators compute over the stream (joins, aggregations, window functions). Their outputs are meaningful only as a complete set given a specific input. Unlike function pods, operator results cannot be safely mixed across source combinations within a shared table because the distributive property does not hold for most operators. For example, with a join: `(X ⋈ Y) ∪ (X' ⋈ Y') ≠ (X ∪ X') ⋈ (Y ∪ Y')`. The shared table would miss cross-terms `X ⋈ Y'` and `X' ⋈ Y`. Cache invalidation is also cleaner per-table (drop/mark stale) rather than selectively purging rows by system tag.

**Critical correctness caveat:**
Even scoped to content hash, operator caches are **not guaranteed to be complete** with respect to the full picture of all packets ever yielded by the sources. Because sources may use canonical identity for their content hash, the same source identity may yield different packet sets over time. The cache accumulates result rows across runs:

- Run 1: `X ⋈ Y` is cached.
- Run 2: Sources yield `X'` and `Y'`. The operator computes `X' ⋈ Y'` and appends new rows to cache.
- The cache now contains `(X ⋈ Y) ∪ (X' ⋈ Y')`, which is **not** equivalent to `(X ∪ X') ⋈ (Y ∪ Y')`.

The operator cache is strictly an **append-only historical record**, not a cumulative materialization. Identical output rows across runs naturally deduplicate (keyed by `hash(tag + packet + system_tag)`). Run-level grouping and tracking is managed separately outside the cache mechanism.

**Behavior:**
- Cache is **off by default**. Operator computation is always triggered fresh in a typical run.
- Cache can be **explicitly opted into** for historical logging purposes. Even when enabled, the operator still recomputes — the cache serves as a record, not a substitute.
- A separate, explicit configuration is required to **skip computation and flow the historical cache** to the rest of the pipeline. This is only appropriate when the user intentionally wants to use the historical record (e.g., for auditing or comparing run-over-run results), not as a performance optimization.

**Three-tier opt-in model:**

| Mode | Cache writes | Computation | Use case |
|------|-------------|-------------|----------|
| Default (off) | No | Always | Normal pipeline execution |
| Logging | Yes | Always | Audit trail, run-over-run comparison |
| Historical replay | Yes (prior) | Skipped | Explicitly flowing prior results downstream |

**Semantic guarantee:** The cache is a **historical record**. It records what was produced, not what would be produced now. Identical output rows across runs are deduplicated. It must never be silently substituted for fresh computation.

### Caching Summary

| Property | Source Pod | Function Pod | Operator Pod |
|----------|-----------|--------------|--------------|
| Cache table scope | Canonical source identity | Structural pipeline hash | Content hash (structure + sources) |
| Default state | Always on | Always on | Off |
| Semantic role | Cumulative record | Reusable lookup | Historical record |
| Correctness | Always correct | Always correct | Per-run snapshots only |
| Cross-source sharing | N/A (one source per table) | Yes, via system tag columns | No (separate tables) |
| Computation on cache hit | Dedup and merge | Skip (use cached result) | Recompute by default |

The overall gradient: sources are always cached and always correct, function pods are always cached and always reusable, operators are optionally logged and never silently substituted. Each level directly follows from whether the computation is cumulative, independent, or holistic.

---

## Pipeline Database Scoping

Function pods and operators use `pipeline_hash()` to scope their database tables:

### FunctionNode Pipeline Path

```
{pipeline_path_prefix} / {function_name} / {output_schema_hash} / v{major_version} / {function_type_id} / node:{pipeline_hash}
```

### OperatorNode Pipeline Path

```
{pipeline_path_prefix} / {operator_class} / {operator_content_hash} / node:{pipeline_hash}
```

### Multi-Source Table Sharing

Sources with identical schemas produce identical `pipeline_hash` values. When processed through the same pipeline structure, they share database tables automatically. Different source instances (e.g., `customers_2023`, `customers_2024`) coexist in the same table, differentiated by system tag values and record hashes. This enables natural cross-source analytics without separate table management.

---

## Derived Sources and Pipeline Composition

Derived sources bridge pipeline stages by materializing intermediate results:

- **Construction**: `function_node.as_source()` or `operator_node.as_source()` returns a `DerivedSource` that reads from the node's pipeline database.
- **Identity**: Content hash ties to the origin node's content hash; pipeline hash is schema-only (inherited from `RootSource`).
- **Use case**: Downstream pipelines reference the derived source directly, independent of the upstream topology that produced it.

Derived sources serve two purposes:
1. **Semantic materialization** — domain-meaningful intermediate constructs (e.g., a daily top-3 selection, a trial, a session) are given durable identity in the pipeline database.
2. **Pipeline decoupling** — once materialized, downstream pipelines can evolve independently of upstream topology.

---

## Provenance Graph

Data provenance focuses on **data-generating entities only** — sources and function pods. Since operators never synthesize new packet values, they leave no computational footprint on the data itself.

The provenance graph is a **bipartite graph of sources and function pods**, with edges encoded as source info pointers per output field. Operator pod topology is captured implicitly in system tag column names and the pipeline Merkle chain but operators do not appear as nodes in the provenance graph.

This means:
- **Operators can be refactored** without invalidating data provenance
- **Provenance queries are simpler** — tracing a result requires only following source info pointers between function pod table entries
- **Provenance is robust** — lineage is told by what generated and transformed the data, not by how it was routed

---

## Execution Models

OrcaPod supports two complementary execution strategies — **synchronous pull-based** and **asynchronous push-based** — that produce semantically identical results. The choice of strategy is an execution concern, not a data-identity concern: neither content hashes nor pipeline hashes depend on how the pipeline was executed.

### Synchronous Execution (Pull-Based)

The default model. Callers invoke `process()` on a pod, which returns a stream. Iteration over the stream triggers computation lazily.

Three variants exist within the synchronous model:

**1. Lazy In-Memory (FunctionPod → FunctionPodStream)**
The function pod processes each packet on demand via `iter_packets()`. Results are cached by index in memory. No database persistence. Suitable for exploration and one-off computations.

**2. Static with Recomputation (StaticOutputPod → DynamicPodStream)**
The operator's `static_process` produces a complete output stream. `DynamicPodStream` wraps it with timestamp-based staleness detection and automatic recomputation when upstreams change.

**3. Database-Backed Incremental (FunctionNode / OperatorNode → PersistentFunctionNode / PersistentOperatorNode)**
Results are persisted in a pipeline database. Incremental computation: only process inputs whose hashes are not already in the database. Per-row record hashes enable deduplication. Suitable for production pipelines with expensive computations. `PersistentFunctionNode` extends `FunctionNode` with result caching via `CachedPacketFunction` and two-phase iteration (Phase 1: yield cached results, Phase 2: compute missing). `PersistentOperatorNode` extends `OperatorNode` with three-tier caching (off / log / replay).

**Concurrent execution within sync mode:**
When a `PacketFunctionExecutor` with `supports_concurrent_execution = True` is attached (e.g. `RayExecutor`), `FunctionPodStream.iter_packets()` materializes all remaining input packets and dispatches them concurrently via the executor's `async_execute`, collecting results in order. This provides data-parallel speedup without leaving the synchronous call model.

### Asynchronous Execution (Push-Based Channels)

Every pipeline node — source, operator, or function pod — implements the `AsyncExecutableProtocol`:

```python
async def async_execute(
    inputs: Sequence[ReadableChannel[tuple[Tag, Packet]]],
    output: WritableChannel[tuple[Tag, Packet]],
) -> None
```

Nodes consume `(Tag, Packet)` pairs from input channels and produce them to an output channel. This enables push-based, streaming execution where data flows through the pipeline as soon as it's available, with backpressure propagated via bounded channel buffers.

**FunctionPod async strategy:** Streaming mode — each input `(tag, packet)` is processed independently with semaphore-controlled concurrency. Uses `asyncio.TaskGroup` for structured concurrency.

#### Operator Async Strategies

Each operator overrides `async_execute` with the most efficient streaming pattern its semantics permit. The default fallback (inherited from `StaticOutputPod`) is barrier mode: collect all inputs via `asyncio.gather`, materialize to `ArrowTableStream`, call `static_process`, and emit results. Operators override this default when a more incremental strategy is possible.

| Strategy | Description | Operators |
|---|---|---|
| **Per-row streaming** | Transform each `(Tag, Packet)` independently as it arrives; zero buffering beyond the current row | SelectTagColumns, SelectPacketColumns, DropTagColumns, DropPacketColumns, MapTags, MapPackets |
| **Accumulate-and-emit** | Buffer rows up to `batch_size`, emit full batches immediately, flush partial at end | Batch (`batch_size > 0`) |
| **Build-probe** | Collect one side fully (build), then stream the other through a hash lookup (probe) | SemiJoin |
| **Symmetric hash join** | Read both sides concurrently, buffer + index both, emit matches as they're found | Join (2 inputs) |
| **Barrier mode** | Collect all inputs, run `static_process`, emit results | PolarsFilter, MergeJoin, Batch (`batch_size = 0`), Join (N > 2 inputs) |

#### Per-Row Streaming (Unary Column/Map Operators)

For operators that transform each row independently (column selection, column dropping, column renaming), the async path iterates `async for tag, packet in inputs[0]` and applies the transformation per row. Column metadata (which columns to drop, the rename map, etc.) is computed lazily on the first row and cached for subsequent rows. This avoids materializing the entire input into an Arrow table, enabling true pipeline-level streaming where upstream producers and downstream consumers run concurrently.

#### Accumulate-and-Emit (Batch)

When `batch_size > 0`, Batch accumulates rows into a buffer and emits a batched result stream each time the buffer reaches `batch_size`. Any partial batch at the end is emitted unless `drop_partial_batch` is set. When `batch_size = 0` (meaning "batch everything into one group"), the operator must see all input before producing output, so it falls back to barrier mode.

#### Build-Probe (SemiJoin)

SemiJoin is non-commutative: the left side is filtered by the right side. The async implementation collects the right (build) side fully, constructs a hash set of its key tuples, then streams the left (probe) side through the lookup — emitting each left row whose keys appear in the right set. This is the same pattern as Kafka's KStream-KTable join: the table side is materialized, the stream side drives output.

#### Symmetric Hash Join

The 2-input Join uses a symmetric hash join — the same algorithm used by Apache Kafka for KStream-KStream joins and by Apache Flink for regular streaming joins. Both input channels are drained concurrently into a shared `asyncio.Queue`. For each arriving row:

1. Buffer the row on its side and index it by the shared key columns.
2. Probe the opposite side's index for matching keys.
3. Emit all matches immediately.

When the first rows from both sides have arrived, the shared key columns are determined (intersection of tag column names). Any rows that arrived before shared keys were known are re-indexed and cross-matched in a one-time reconciliation step.

**Comparison with industry stream processors:**

| Aspect | Kafka Streams (KStream-KStream) | Apache Flink (Regular Join) | OrcaPod |
|---|---|---|---|
| Algorithm | Symmetric windowed hash join | Symmetric hash join with state TTL | Symmetric hash join |
| Windowing | Required (sliding window bounds state) | Optional (TTL evicts old state) | Not needed (finite streams) |
| State backend | RocksDB state stores for fault tolerance | RocksDB / heap state with checkpointing | In-memory buffers |
| State cleanup | Window expiry evicts old records | TTL or watermark eviction | Natural termination — inputs are finite |
| N-way joins | Chained pairwise joins | Chained pairwise joins | 2-way: symmetric hash; N > 2: barrier + Arrow join |

The symmetric hash join is optimal for our use case: it emits results with minimum latency (as soon as a match exists on both sides) and requires no windowing complexity since OrcaPod streams are finite. For N > 2 inputs, the operator falls back to barrier mode with Arrow-level join execution, which is efficient for bounded data and avoids the complexity of chaining pairwise streaming joins.

**Why not build-probe for Join?** Since Join is commutative and input sizes are unknown upfront, there is no principled way to choose which side to build vs. probe. Symmetric hash join avoids this asymmetry. SemiJoin, being non-commutative, has a natural build (right) and probe (left) side.

**Why barrier for PolarsFilter and MergeJoin?** PolarsFilter requires a Polars DataFrame context for predicate evaluation, which needs full materialization. MergeJoin's column-merging semantics (colliding columns become sorted `list[T]`) require seeing all rows to produce correctly typed output columns.

### Sync / Async Equivalence

Both execution paths produce identical output given identical inputs. The sync path is simpler to debug and compose; the async path enables pipeline-level parallelism and streaming. The `PipelineConfig.executor` field selects between them:

| `ExecutorType` | Mechanism | Use case |
|---|---|---|
| `SYNCHRONOUS` | `process()` chain with pull-based materialization | Interactive exploration, debugging |
| `ASYNC_CHANNELS` | `async_execute()` with push-based channels | Production pipelines, concurrent I/O |

---

## Channel System

Channels are the communication primitive for push-based async execution. They are bounded async queues with explicit close/done signaling and backpressure.

### Channel

A `Channel[T]` is a bounded async buffer (default capacity 64) with separate reader and writer views:

- **`WritableChannel`** — `send(item)` blocks when the buffer is full (backpressure). `close()` signals that no more items will be sent.
- **`ReadableChannel`** — `receive()` blocks until an item is available. Raises `ChannelClosed` when the channel is closed and drained. Supports `async for` iteration and `collect()` to drain into a list.

### BroadcastChannel

A `BroadcastChannel[T]` fans out items from a single writer to multiple independent readers. Each `add_reader()` creates a reader with its own queue, so downstream consumers read at their own pace without interfering.

### Backpressure

Backpressure propagates naturally: when a downstream reader is slow, the writer blocks on `send()` once the buffer fills. This prevents unbounded memory growth and creates natural flow control through the pipeline graph.

---

## Packet Function Executor System

Executors decouple **what** a packet function computes from **where** and **how** it runs. Every `PacketFunctionBase` has an optional `executor` slot. When set, `call()` and `async_call()` route through the executor instead of calling the function directly.

### Routing

```
packet_function.call(packet)
    ├── executor is set → executor.execute(packet_function, packet)
    └── executor is None → packet_function.direct_call(packet)

packet_function.async_call(packet)
    ├── executor is set → executor.async_execute(packet_function, packet)
    └── executor is None → packet_function.direct_async_call(packet)
```

Executors call `direct_call()` / `direct_async_call()` internally, which are the native computation methods that subclasses implement. This two-level routing ensures executors can wrap the computation without infinite recursion.

### Executor Types

| Executor | `executor_type_id` | Supported Types | Concurrent | Description |
|---|---|---|---|---|
| `LocalExecutor` | `"local"` | All | No | Runs in-process. Default. |
| `RayExecutor` | `"ray.v0"` | `"python.function.v0"` | Yes | Dispatches to a Ray cluster. Configurable CPUs/GPUs/resources. |

### Type Safety

Each executor declares `supported_function_type_ids()`. Setting an incompatible executor raises `ValueError` at assignment time, not at execution time. An empty set means "supports all types" (used by `LocalExecutor`).

### Identity Separation

Executors are **not** part of content or pipeline identity. The same function produces the same hash regardless of whether it runs locally or on Ray. Executor metadata is captured separately via `get_execution_data()` for observability but does not affect hashing or caching.

### Concurrency Configuration

Two-level configuration controls per-node concurrency in async mode:

- **`PipelineConfig`** — pipeline-level defaults: `executor` type, `channel_buffer_size`, `default_max_concurrency`.
- **`NodeConfig`** — per-node override: `max_concurrency`. `None` inherits from pipeline config. `1` forces sequential execution (useful for rate-limited APIs or order-preserving operations).

`resolve_concurrency(node_config, pipeline_config)` returns the effective limit. In `FunctionPod.async_execute`, this limit governs an `asyncio.Semaphore` controlling how many packets are in-flight concurrently.

---

## Pipeline Compilation and Orchestration

### Graph Tracking

All pod invocations are automatically recorded by a global `BasicTrackerManager`. When a `StaticOutputPod.process()` or `FunctionPod.process()` is called, the tracker manager broadcasts the invocation to all registered trackers. This enables transparent DAG construction — the user writes normal imperative code, and the computation graph is captured behind the scenes.

`GraphTracker` is the base tracker implementation. It maintains:
- A **node lookup table** (`_node_lut`) mapping content hashes to `FunctionNode`, `OperatorNode`, or `SourceNode` objects.
- An **upstream map** (`_upstreams`) mapping stream content hashes to stream objects.
- A directed **edge list** (`_graph_edges`) recording (upstream_hash → downstream_hash) relationships.

`GraphTracker.compile()` builds a `networkx.DiGraph`, topologically sorts it, and wraps unregistered leaf hashes in `SourceNode` objects, producing a complete typed DAG.

### Pipeline

`Pipeline` extends `GraphTracker` with persistence. Its lifecycle has three phases:

**1. Recording phase (context manager).** Within a `with pipeline:` block, the pipeline registers itself as an active tracker. All pod invocations are captured as non-persistent nodes.

**2. Compilation phase (`compile()`).** On context exit (if `auto_compile=True`), `compile()` walks the graph in topological order and replaces every node with its persistent variant:

| Non-persistent | Persistent | Scoped by |
|---|---|---|
| Leaf stream | `PersistentSourceNode` | Stream content hash |
| `FunctionNode` | `PersistentFunctionNode` | Pipeline hash (schema+topology) |
| `OperatorNode` | `PersistentOperatorNode` | Content hash (structure+sources) |

All persistent nodes share the same `pipeline_database` with the pipeline's name as path prefix. An optional separate `function_database` can be provided for function pod result caches.

Compilation is **incremental**: re-entering the context, adding more operations, and compiling again preserves existing persistent nodes. Labels are disambiguated by content hash on collision.

**3. Execution phase (`run()`).** Executes all compiled nodes in topological order by calling `node.run()` on each, then flushes all databases. Compiled nodes are accessible by label as attributes on the pipeline instance (e.g., `pipeline.compute_grades`).

### Persistent Nodes

| Node type | Behavior |
|---|---|
| `PersistentSourceNode` | Materializes the wrapped stream into a cache DB with per-row deduplication via content hash. On subsequent access, returns the union of cached + live data. |
| `PersistentFunctionNode` | DB-backed two-phase iteration: Phase 1 yields cached results from the pipeline database, Phase 2 computes only missing inputs. Uses `CachedPacketFunction` for packet-level result caching. |
| `PersistentOperatorNode` | DB-backed with three-tier cache mode: OFF (default, always recompute), LOG (compute and write to DB), REPLAY (skip computation, load from DB). |

### Pipeline Composition

Pipelines can be composed across boundaries:
- **Cross-pipeline references** — Pipeline B can use Pipeline A's compiled nodes as input streams.
- **Chain detachment** via `.as_source()` — `PersistentFunctionNode.as_source()` and `PersistentOperatorNode.as_source()` return a `DerivedSource` that reads from the pipeline database, breaking the upstream Merkle chain. Downstream pipelines reference the derived source directly, independent of the upstream topology that produced it.

---

## Fused Pod Pattern

### Motivation

The strict operator / function pod boundary is central to OrcaPod's provenance guarantees: operators never synthesize values (provenance transparent), function pods always synthesize values (provenance tracked). This two-category model keeps provenance tracking simple and robust.

However, certain common patterns require combining both behaviors in a single logical operation. The most common is **enrichment** — running a function on a packet and appending the computed columns to the original packet rather than replacing it. The naïve decomposition into `FunctionPod + Join` works but incurs unnecessary overhead: an intermediate stream is materialized only to be immediately joined back, and the join must re-match tags that trivially correspond because they came from the same input row.

### Fused Pods as Optimization, Not Extension

A **fused pod** is an implementation-level pod type that combines the behaviors of multiple existing pod types into a single pass, without introducing a new provenance category. Its correctness is verified by checking equivalence with its decomposition.

The key invariant: **every column in a fused pod's output maps to exactly one existing provenance category.**

- **Preserved columns** (from upstream) — provenance transparent, source-info passes through unchanged. This is the operator-like component.
- **Computed columns** (from the wrapped PacketFunction) — provenance tracked, source-info references the PacketFunction. This is the function-pod-like component.

There is no third kind of output column. The theoretical provenance model stays clean (Source, Operator, FunctionPod), and fused pods are justified as performance/ergonomic optimizations whose provenance semantics are *derived from* the existing model rather than extending it.

This is analogous to how a database query optimizer fuses filter+project into a single scan without changing the relational algebra semantics.

### AddResult

The first planned fused pod. Wraps a `PacketFunction` and merges the function output back into the original packet:

```python
grade_pf = PythonPacketFunction(compute_letter_grade, output_keys="letter_grade")
enriched = AddResult(grade_pf).process(stream)
# enriched has all original columns + "letter_grade"
```

Equivalent decomposition: `FunctionPod(pf).process(stream)` → `Join()(stream, computed)`.

Efficiency gains: no intermediate stream materialization, no redundant tag matching, no broadcast/rejoin wiring. The async path streams row-by-row like FunctionPod.

Implementation constraints:
- `output_schema()` returns `(input_tag_schema, input_packet_schema | function_output_schema)`.
- Raises `InputValidationError` if function output keys collide with existing packet column names.
- `pipeline_hash` commits to the wrapped PacketFunction's identity plus the upstream's pipeline hash (as if the decomposition were performed).
- Source-info on computed columns references the PacketFunction. Source-info on preserved columns passes through unchanged.

---

## Data Context

Every object is associated with a `DataContext` providing:

| Component | Purpose |
|---|---|
| `semantic_hasher` | Recursive, type-aware object hashing for content/pipeline identity |
| `arrow_hasher` | Arrow table/record batch hashing |
| `type_converter` | Python ↔ Arrow type conversion |
| `context_key` | Identifier for this context configuration |

The data context ensures consistent hashing and type conversion across the pipeline. It is propagated through construction and accessible via the `DataContextMixin`.

---

## Verification

The ability to rerun and verify the exact chain of computation is a core feature. A pipeline run in verify mode recomputes every step and checks output hashes against stored results, producing a reproducibility certificate.

Function pods carry a determinism declaration:
- **Deterministic pods** — verified by exact hash equality
- **Non-deterministic pods** — verified by an associated equivalence measure

Equivalence measures are externally associated with function pods — not with schemas — because the same data type can require different notions of closeness in different computational contexts.

---

## Separation of Concerns

A consistent architectural principle: **computational identity is separated from computational semantics**.

The content-addressed computation layer handles identity — pure, self-contained, uncontaminated by higher-level concerns. External associations carry richer semantic context:

| Association | Informs |
|---|---|
| Schema linkage | Pipeline assembler / wiring validation |
| Equivalence measures | Verifier |
| Confidence levels | Registry / ecosystem tooling |

None of these influence actual pod execution.
