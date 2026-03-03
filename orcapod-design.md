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

Two execution models exist:

- **FunctionPod + FunctionPodStream** — lazy, in-memory evaluation. The function pod processes each (tag, packet) pair from the input stream on demand, caching results by index.

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

Three execution models coexist:

### Lazy In-Memory (FunctionPod → FunctionPodStream)
The function pod processes each packet on demand. Results are cached by index in memory. No database persistence. Suitable for exploration and one-off computations.

### Static with Recomputation (StaticOutputPod → DynamicPodStream)
The operator's `static_process` produces a complete output stream. `DynamicPodStream` wraps it with timestamp-based staleness detection and automatic recomputation when upstreams change.

### Database-Backed Incremental (FunctionNode / OperatorNode)
Results are persisted in a pipeline database. Incremental computation: only process inputs whose hashes are not already in the database. Per-row record hashes enable deduplication. Suitable for production pipelines with expensive computations.

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
