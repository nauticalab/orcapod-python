# OrcaPod — Comprehensive Design Specification

---

## Core Abstractions

- **Packet** — the atomic unit of data flowing through the system. Every packet carries:
  - **Data** — content organized into named columns
  - **Schema** — explicit type information, embedded in the packet (not resolved from a central registry)
  - **Source info** — per-field provenance pointers (see below)
  - **Tags** — key-value metadata, human-friendly and non-authoritative
  - **System tags** — framework-managed hidden provenance columns (see below)

- **Stream** — a sequence of packets, analogous to a channel in concurrent programming. Streams are abstract and composable — they can be joined, merged, or otherwise combined by operator pods to yield new streams.

- **Source Pod** — creates new packets with new provenance and system tags, representing a **provenance boundary** by definition. Generalizes over zero or more input streams:

  - **Root source pod** — takes zero input streams and pulls data from the external world (file, database, API, etc.). The zero-input case is the degenerate special case of the general form.
  - **Derived source pod** — takes one or more input streams and may read their tags, packet content, or both to drive packet creation. Represents an **explicit materialization declaration** — a way of saying "this intermediate result is semantically meaningful enough to be treated as a first-class source entry in the pipeline database," detached from the upstream stream that produced it.

  Derived source pods serve two distinct and well-motivated purposes:
  1. **Semantic materialization** — domain-meaningful intermediate constructs (e.g. a daily top-3 selection by a content-carried metric, a trial, a session) are given durable identity in the pipeline database. Without this, such constructs exist only as transient operator outputs with no stable reference point or historical record.
  2. **Pipeline decoupling** — once materialized, downstream pipelines reference the derived source directly, independent of the upstream topology that produced it. Upstream pipelines can evolve without destabilizing downstream analyses built against the materialized intermediate.

  Derived source pods support two run modes:

  - **Live mode** — the upstream stream is fully executed, the derived source materializes the new output into the pipeline database, and feeds it into the downstream pipeline. Used for processing current data, e.g. computing today's top-3 models and running downstream analysis on them.
  - **Historical mode** — the upstream stream is bypassed entirely. The derived source queries the pipeline database directly, replaying past materialized entries into the downstream pipeline. Used for analyzing past sets, e.g. running downstream analysis across all previously recorded top-3 sets.

  In both modes, downstream function pod caching operates identically — cache lookup is purely `pod_signature + input_packet_hash → output`, with no awareness of provenance, tags, run mode, or how the packet arrived. If a packet from a historical entry was previously fed through the same downstream function pods, cached results are served automatically. This means historical mode reruns are computationally cheap for entries whose downstream results are already cached, and the benefit compounds as the pipeline database accumulates more materialized entries over time.

  Since source pods establish new provenance, the framework makes no claims about what drove their creation. Tags are not a fundamental provenance source for data — they are routing and metadata signals. The fundamental distinction between pod types is their relationship to provenance: **source pods start a provenance chain, function pods continue one**.

- **Function Pod** — a computation that consumes a **single packet** from a single stream and produces an output packet. Function pods never inspect stream structure or tags.

- **Operator Pod** — a structural pod that operates on streams. Operator pods can read packet content and tags, and can introduce arbitrary tags, but are subject to one fundamental constraint: **every packet value in an operator pod's output must be traceable to a concrete value already present in the input packets.** Operator pods cannot synthesize or compute new packet values — doing so would break the source info chain. They perform joins, merges, splits, selections, column renames, batching, and tag operations within this constraint. Examples: join, merge, rename, batch, tag-promote.

- **Pipeline** — a specifically wired graph of function pods and operator pods, itself hashed from its composition to serve as a unique pipeline signature.

---

## Schema as a First-Class Citizen

Every object in OrcaPod has a clear type and schema association. Schema is embedded explicitly in every packet rather than resolved against a central registry, making packets fully self-describing and the system decentralized.

**Schema linkage** — distinct schemas can be linked to each other to express relationships (equivalence, subtyping, evolution, transformation). These links are maintained as external metadata and do not influence individual pod computations. Schema linkage informs pipeline assembly and validation but is not part of the execution record.

---

## Tags

Tags are key-value pairs attached to every packet providing human-friendly metadata for navigation, filtering, and annotation. They are:

- **Non-authoritative** — never used for cache lookup or pod identity computation
- **Auto-propagated** — tags flow forward through the pipeline automatically
- **Mutable** — can be annotated after the fact without affecting packet identity
- **The basis for joins** — operator pods join streams by matching tag keys, never by inspecting packet content

**Tag merging in joins:**
- **Shared tag keys** — act as the join predicate; values must match for packets to be joined
- **Non-shared tag keys** — propagate freely into the merged output packet's tags



---

## Operator Pod / Function Pod Boundary

This is a strict and critical separation:

| | Operator Pod | Function Pod |
|---|---|---|
| Inspects packet content | Never | Yes |
| Inspects / uses tags | Yes | No |
| Can rename columns | Yes | No |
| Stream arity | Multiple in, one out | Single stream in, single stream out |
| Cached by content hash | No | Yes |

Column renaming by operator pods allows join conflicts to be avoided without contaminating source info — the column name changes but the source info pointer remains intact, always traceable to the original producing pod.

---

## Identity and Hashing

OrcaPod uses a cascading content-addressed identity model:

- **Packet identity** — hash of data + schema
- **Function pod identity** — hash of canonical name + input/output schemas + implementation artifact (type-dependent)
- **Pipeline identity** — hash of the specific composition of specifically identified function pods and operator pods

A change anywhere in this chain produces a distinct identity, making silent drift impossible.

---

## Function Pod Signatures

Every function pod has a unique signature reflecting its input/output schemas and implementation. Signature computation is type-dependent:

| Pod Type | Signature Inputs |
|---|---|
| Python function | Canonical name + I/O schemas + source/bytecode hash + input parameters signature hash + Git version |
| REST endpoint | Canonical name + I/O schemas + interface contract hash |
| RPC | Canonical name + I/O schemas + service/method + interface definition hash |
| Docker image | Canonical name + I/O schemas + image digest |

Docker image-based pods offer the strongest reproducibility guarantee as the image digest captures code, dependencies, and runtime environment completely.

**Canonical naming** follows a URL-style convention (e.g. `github.com/eywalker/sampler`) providing global uniqueness and discoverability. OrcaPod fetches implementation artifacts directly from the specified source via a pluggable fetcher abstraction. A local artifact cache keyed by content hash avoids redundant remote fetches.

Canonical names are user-assigned. Renaming a pod should be treated as creating a new pod — it invalidates downstream pipeline hashes.

---

## Function Pod Storage Model

Function pod outputs are stored in tables using a two-tier identity structure:

### Table Identity (coarse-grained, schema-defining)
Determines which table outputs are stored in:
- Function type
- Canonical name
- Major version
- Output schema hash

A new table is created when any of these change. Major version signals a breaking change.

### Row Identity (fine-grained, execution-defining)
Each row contains:
- **Unique row ID** — UUID, finest-grain identifier for a specific execution result
- **Input packet hash** — the hash of the single input packet consumed
- **Minor version**
- **Output columns** — one column per output field
- **Function-type-dependent identifying info**, e.g. for Python: function content hash, input parameters signature hash, Git version, execution environment info

---

## Source Info

Every field in every packet carries a **source info** string — a fully qualified provenance pointer to the exact function pod table row and column that produced it:

```
{function_type}:{function_name}:{major_version}:{output_schema_hash}::{row_uuid}:{output_column}[::[indexer]]
```

The `::` separates table-level identity (left) from row/column-level identity (right).

**Nested indexing** follows Python-style syntax, e.g.:
```
...::row_uuid:output_column::[5]["name"][3]
```

Source info is **immutable through the pipeline** — set once when a function pod produces an output and survives all downstream operator transformations including column renames.

---

## Pipeline Graph Identity — Merkle Chain

Pipeline identity is computed as a Merkle tree over the computation graph. Each node's chain hash commits to:

1. **The node's own identifying elements** — for operator pods: canonical name + critical parameters; for function pods: function type + canonical name + version + input/output schemas
2. **The recursive chain hashes of its parent nodes**

Any node's hash is a cryptographic summary of its entire upstream computation history. Source nodes (raw input packets) are identified purely by their content hash, forming the base case of the recursion.

**Subgraph reuse** follows naturally — shared upstream subgraphs have identical chain hashes and cached results are reusable across pipelines.

### Upstream Commutativity

Each pod defines how parent chain hashes are combined:

- **Ordered `[A, B]`** — parent chain hashes combined in declared order. Used when input position is semantically significant.
- **Unordered `(A, B)`** — parent chain hashes sorted by hash value then combined. Used when the pod is symmetric over its inputs.

For library-provided operator pods, commutativity is implicitly encoded in the canonical name. For user-defined function pods, ordered inputs is the default.

---

## System Tags

System tags are **framework-managed, hidden provenance columns** automatically attached to every packet. Unlike user tags, they are authoritative and guaranteed to maintain perfect traceability from any result row back to its original source rows, regardless of user tagging discipline.

### Source System Tags

Each source packet is assigned a system tag that uniquely identifies its origin in a source-type-dependent way:
- **File source** → full file path
- **CSV source** → file path + row number
- Other source types → appropriate unique locator

System tag **values** have the format:
```
source_id:original_row_id
```

### System Tag Column Naming

System tag **column names** encode both source identity and pipeline path:

```
source_hash:canonical_position:upstream_template_id:canonical_position:upstream_template_id:...
```

Where:
- `source_hash` — hash combining source packet schema + source user tag schema
- `canonical_position` — position of input stream, canonically ordered for commutative operations
- `upstream_template_id` — recursive template hash of the upstream node feeding this position
- Chain length equals the number of name-extending operations in the path

### Three Evolution Rules

**1. Name-Preserving (~90% of operations)**
Single-table operations (filter, transform, sort, select, rename). System tag column name, type, and value all pass through unchanged.

**2. Name-Extending (multi-input operations)**
Joins, merges, unions, stacks. Each incoming system tag column name is extended with `:canonical_position:upstream_template_id`. Values remain unchanged (`source_id:row_id`). Canonical position assignment respects commutativity — for commutative operations, inputs are sorted by upstream template ID to ensure identical column names regardless of wiring order.

**3. Type-Evolving (aggregation operations)**
Group-by, batch, window, reduce operations. Column name is unchanged but type evolves: `String → List[String] → List[List[String]]` for nested aggregations. Values collect all contributing source row IDs.

### Chained Joins

When joins are chained, system tag column names grow by appending `:position:template_id` at each join. Column name length is naturally bounded by pipeline DAG depth (typically 5–15 operations deep, yielding ~35–65 character names). Pipelines grow wide (multiple sources) rather than deep in practice, so the number of system tag columns scales with source count, not individual name length.

### Template ID and Instance ID

The caching system separates **source-agnostic pipeline logic** from **source-specific execution context**:

- **Template ID** — recursive hash of pipeline structure and operations only, no source schema information. Same pipeline topology → same template ID regardless of which sources are bound. Commutative operations sort parent template IDs for canonical ordering.

- **Instance ID** — hash of template ID + source assignment mapping + concrete source schemas. Determines the exact cache table path for a specific pipeline instantiation.

### Cache Table Path

```
pipeline_name:kernel_id:template_id:instance_id
```

For function pods specifically:
```
pipeline_name:pod_name:output_schema_hash:major_version:pipeline_identity:tag_schema_hash
```

### Multi-Source Table Sharing

Sources with identical packet schema and user tag schema processed through the same pipeline structure share cache tables automatically. Different source instances (e.g. `customers_2023`, `customers_2024`) coexist in the same table, differentiated by system tag values and a `_source_identity` metadata column. This enables natural cross-source analytics without separate table management.

### Pipeline Composition Modes

**Pipeline Extension** — logically extending an existing pipeline. System tags preserve full lineage history, column names continue accumulating position:template extensions, values preserve original source identity.

**Pipeline Boundary** — materializing a pipeline result as a new independent source. System tags reset to a fresh source schema based on the materialized result. Enables clean provenance breaks when results become general-purpose data sources.

---

## Provenance Graph

Data provenance in OrcaPod fundamentally focuses on **data-generating pods only** — namely source pods and function pods. Since operator pods never inspect or transform packet content, and joins are driven purely by tags, operator pods leave no meaningful computational footprint on the data itself.

The provenance graph is therefore a **bipartite graph of sources and function pods**, with edges encoded as source info pointers per output field. This is significantly simpler than the full pipeline graph.

Operator pod topology is captured implicitly and structurally in system tag column names (via template/instance ID chains) and in the pipeline Merkle chain — but operator pods do not appear as nodes in the provenance graph. This means:

- **Operator pods can be refactored, reordered, or replaced** without invalidating the fundamental data provenance story, as long as the source and function pod chain remains intact
- **Provenance queries are simpler** — tracing a result back to its origins only requires traversing source info pointers between function pod table entries, not reconstructing the full operator topology
- **Provenance is robust** — the data lineage story is told entirely by what generated and transformed the data, not by how it was routed

---

## Two-Tier Caching

### Function-Level Caching
Caches pure computational results independent of pipeline context. Entry keyed by `function_content_hash + input_packet_hash`. Results shared across pipelines and minor versions. Provenance-agnostic — caches by packet content, not source identity.

### Pipeline-Level Caching
Caches pipeline-specific results with full provenance context via the template/instance ID structure. Schema-compatible sources share tables automatically. System tags maintained throughout.

These two tiers are complementary: function-level caching maximizes computational reuse; pipeline-level caching maintains perfect provenance.

---

## Caching and Execution Modes

Every computation record explicitly distinguishes execution modes:

- **Computed** — pod executed fresh, result produced and cached
- **Cache hit** — result retrieved from cache, prior provenance referenced
- **Verified** — result recomputed and matched cached hash, confirming reproducibility

---

## Verification as a Core Feature

The ability to rerun and verify the exact chain of computation is a critical feature of OrcaPod. A pipeline run in verify mode recomputes every step and checks output hashes against stored results, producing a **reproducibility certificate**.

Verification is all-or-nothing per chain. Failures identify precisely which pod on which packet produced a divergent hash.

---

## Determinism and Equivalence

Function pods carry a field declaring expected determinism. This gates verification behavior:

- **Deterministic pods** — verified by exact hash equality
- **Non-deterministic pods** — verified by an associated equivalence measure

**Equivalence measures** are externally associative on function pods — not on schemas — because the same data type can require different notions of closeness in different computational contexts (floating point tolerance, distributional similarity, domain-specific metrics, etc.).

The determinism flag is the simple case today, intended to generalize into a richer equivalence specification. Exact hash equality is the degenerate case where tolerance is zero.

---

## Separation of Concerns

A consistent architectural principle runs through OrcaPod: **computational identity is separated from computational semantics**.

The content-addressed computation layer handles identity — pure, self-contained, uncontaminated by higher-level concerns. External associations carry richer semantic context for different consumers:

| Association | Informs |
|---|---|
| Schema linkage | Pipeline assembler / wiring validation |
| Equivalence measures | Verifier |
| Confidence levels | Registry / ecosystem tooling |

None of these influence actual pod execution.

---

## Confidence Levels

Reproducibility guarantees vary by pod type and naming discipline. Confidence levels will be maintained by a future pod library/registry service rather than the core framework. The core framework emits sufficient execution metadata (fetcher type, ref pinning, execution mode) for a registry to compute confidence levels without re-examination.
