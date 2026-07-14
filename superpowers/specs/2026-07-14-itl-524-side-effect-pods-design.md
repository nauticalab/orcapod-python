# ITL-524: Side-Effect Pods — First-Class Support with Pipeline-Linked Execution Hash

## Overview

Orcapod pipelines are built around **function pods** — nodes that transform an input
packet into an output packet. Many real pipelines also need steps whose primary purpose
is a side effect: writing a structured log line, appending a row to an external database,
emitting a metric, or sending a notification. Today those steps must be shoehorned into
function pods with fake outputs or breadcrumb return values.

This design introduces **side-effect pods** as a first-class pipeline concept, with a
crucial property: any external artifact the side effect produces can be **linked back to
the specific pipeline execution** that produced it — via a deterministic **invocation
signature** embedded into the artifact. That signature bridges the provenance boundary:
anyone holding an artifact can look it up in Orcapod's invocation log and trace it back
to the originating pipeline run, pod version, and input data — following the same
provenance chains as any other Orcapod node.

This document resolves the open design axes (taxonomy, output contract, caching
semantics, hash formula, API surface, failure handling, DAG ordering, observability, and
serialization conventions) and specifies the follow-up implementation work.

---

## Goals & Success Criteria

This is an **interactive design spike**. Every design axis listed in this document must
be explicitly discussed with Edgar Walker before a decision is recorded. The agent's
role is to surface options, trade-offs, and codebase constraints; the final call on each
axis belongs to Edgar. The spec is a summary of those concluded discussions — not a
set of decisions made independently.

The spike is complete when:

- Every design axis below has been discussed and a final decision recorded, with the
  chosen option and the rationale for rejecting alternatives.
- The pipeline execution hash is fully specified: its formula, format, stability
  properties, and how a pod author accesses it from inside a pod body.
- At least one worked example is included: a side-effect pod definition, the external
  artifact it produces, and a concrete walk-through of the reverse-lookup path from
  that artifact back to the originating pipeline run and inputs.
- The design is explicitly reconciled with ITL-523 (post-run hook): where the two
  concepts share primitives, that is called out; where they diverge, the reason is
  stated.
- A follow-up implementation issue is filed in Linear with a concrete task breakdown,
  ready to pick up.

---

## First-Principles Framing

For function pods, Orcapod holds the complete provenance loop: inputs → code → outputs
are all in-framework, and the result store ties them together natively. For side-effect
pods, the "output" lives **outside** — in Loki, a Postgres table, a Slack message, a
Prometheus metric. Orcapod cannot hold the artifact, so it cannot natively close the
provenance loop.

The **invocation signature** bridges that gap. If the side-effect pod embeds the
signature into every artifact it produces, and Orcapod records the mapping from
signature → (pod version, input data, pipeline run) in its own invocation log, then:

```
external artifact (has invocation_signature)
  → Orcapod invocation log (signature → pod + inputs)
    → Orcapod result store (inputs → upstream lineage)
      → original source rows
```

This is the reverse-lookup chain. The signature must be:
- **Plain text** — embeddable in log fields, DB columns, message bodies.
- **Deterministic** — stable across identical re-runs (same pod code + same inputs →
  same signature), so an artifact produced months ago can still be resolved.
- **Short enough** — 32 hex characters (128 bits) is compact and unambiguous.

---

## Design Axes

### 1. Pod Taxonomy

**Decision:** Introduce `SideEffectPodProtocol` as a proper protocol and `SideEffectPod`
as its concrete implementation.

**Protocol layer — `SideEffectPodProtocol`** (in `src/orcapod/protocols/`):
Defines the contract of a side-effect pod purely in terms of its methods and type
signatures, independent of any implementation detail. This is the type that user code,
type checkers, and the framework itself reason about. It follows the same pattern as
`StreamProtocol`, `DataProtocol`, `PodProtocol`, and other protocol definitions
throughout Orcapod.

**Implementation layer — `SideEffectPod`** (in `src/orcapod/core/`):
Implements `SideEffectPodProtocol` by extending `_FunctionPodBase`. This inheritance
is purely an implementation choice — it gives `SideEffectPod` hashing, executor
routing, async channel execution, and observer wiring for free, without those
mechanisms needing to be duplicated. The public contract is determined by the protocol,
not by what `_FunctionPodBase` exposes.

**Rejected alternatives:**
- **Flag on `FunctionPod`** (`side_effect=True`): creates a dead branch in every
  `FunctionPod` code path and makes the type contract ambiguous at the call site.
- **Subclass of `FunctionPod`**: inherits the output-oriented contract and then must
  override it, producing a leaky abstraction. `_FunctionPodBase` is the right
  inheritance target — not `FunctionPod` itself.

**Class structure:**
```
protocols/
└── SideEffectPodProtocol     (contract: methods + type signatures)

core/
└── _FunctionPodBase          (shared execution infrastructure)
    ├── FunctionPod           (implements FunctionPodProtocol)
    └── SideEffectPod         (implements SideEffectPodProtocol)
```

### 2. Pod Kinds and Output Contract

**Decision:** There are two categorically distinct kinds of side-effect pod. They differ
enough in semantics, infrastructure requirements, and output contract to warrant
separate types rather than a single class with a flag. Whether they share a common
`SideEffectPod` umbrella type is deferred — it is not necessary to decide for the
initial implementation.

#### `SinkPod` — completion-tracked, terminal delivery

A `SinkPod` writes to an external source with a meaningful notion of success or failure
*per input*. The intent is exactly-once delivery: each (pod, input) pair should result
in exactly one successful write.

**Output contract: terminal.** The pod ends the DAG branch. Delivering data outward is
the purpose; what happens next in the pipeline is a separate concern expressed as a
separate branch before this node.

**Completion tracking.** The framework records completion state per (pod, input) in an
internal log:

- **Not yet seen** → execute the delivery.
- **Previously succeeded** → skip silently on re-run.
- **Previously failed** → re-attempt on the next explicit pipeline re-run.

Retry policy (automatic retries, max attempts, backoff) is **deferred to ITL-526**,
which will design a shared retry mechanism for both `FunctionPod` and `SinkPod`. For
now, re-attempt occurs only when the pipeline is explicitly re-run by the user.

Completion state is internalized — tracked in the pod's invocation log, visible to
observers, not emitted as pipeline data.

**Canonical examples:** writing an audit row to a database, archiving a file to object
storage, posting a one-time notification.

#### `TapPod` — always-fire, pass-through observation

A `TapPod` reacts to every data packet passing through it, unconditionally. There is no
notion of completion, no memory of past encounters, no skip or retry logic.

**Output contract: pass-through.** The input tag and data flow through unchanged. The
pod is a transparent interceptor — a tap on the stream.

**No completion tracking.** Every pipeline run, every input triggers the effect. This
is correct and expected: a logger should log every packet every time, not once.

**Canonical examples:** structured logging, metrics emission, real-time monitoring
feeds, debug tracing.

#### Comparison

| | `SinkPod` | `TapPod` |
|---|---|---|
| Output contract | Terminal | Pass-through |
| Completion tracking | Yes — success / failed / not-seen | No |
| Re-run behavior | Skip on success; retry on re-run if failed | Always fires |
| Invocation log | Required (completion state) | Optional (observability only) |
| Retry policy | Deferred to ITL-526 | N/A |

**Rejected alternative — single class with `idempotent: bool`:** The semantic
difference between "always fire" and "fire once and track completion" is substantial
enough that a flag would obscure rather than clarify intent. A pod author should not
need to reason about idempotency to write a logger.

### 3. Re-Execution and Completion Semantics

**`TapPod`:** No caching or completion tracking. Every pipeline run, every input packet
triggers the effect unconditionally. There is no state to consult and no state to record
beyond optional observability logging.

**`SinkPod`:** Completion-tracked per (pod topology, input packet). The framework
maintains a per-pod invocation table (see Axis 8) and consults it at the start of each
execution:

| Prior state | Action |
|---|---|
| Not yet seen | Execute the delivery |
| Previously succeeded | Skip silently |
| Previously failed | Re-attempt |

Re-attempt occurs when the pipeline is **explicitly re-run by the user**. There is no
automatic background retry in v1. Full retry policy (max attempts, backoff, retryable
exception types) is deferred to ITL-526, which will design a shared mechanism for both
`FunctionPod` and `SinkPod`.

`SinkPod` does **not** participate in the `ResultCache` / `FunctionNode` result table
system. Its execution state is tracked exclusively in its own invocation table.

### 4. Invocation Hash

#### Raw identity components

The raw identity of any invocation is a **three-part compound** (two parts for
`SinkPod`, three for `TapPod`):

1. **`pipeline_hash(pod)`** — the pipeline hash of the pod, encoding both the pod's own
   function identity and the full upstream topology. This is the same `pipeline_hash()`
   mechanism used by `FunctionNode` for DB path scoping: different pipeline topologies
   using the same function produce different hashes, so completion records are namespaced
   per topology.

2. **`full_input_packet_hash`** — a hash of the entire input packet: tag columns, data
   columns, source columns (`_source_*`), and system tag columns (`_tag_*`). This is
   more inclusive than the `input_data.content_hash()` used by `ResultCache`, which
   covers data columns only. Including tags and source/system columns means two packets
   with identical payload but different originating provenance — different sources,
   different upstream routes — produce different invocation hashes. This is correct: a
   sink that writes an audit row needs to distinguish rows by their full provenance, not
   just their data values.

3. **`pipeline_run_id`** (`TapPod` only, when available) — a verbatim string
   identifying the specific pipeline execution (e.g. `"run-2026-07-14-001"`). This is
   user-controlled and included as-is without further hashing. When no run ID is
   available (lazy/in-memory pipeline), the third component is omitted.

**Why `pipeline_hash` rather than `content_hash` for component 1:**
`content_hash(pod)` captures only the pod's function identity. `pipeline_hash(pod)`
captures function identity *plus* the full upstream topology, forming a Merkle chain.
Using `pipeline_hash` ensures that the same sink function used in two pipelines with
different upstream structures never shares a completion namespace — a delivery confirmed
in pipeline A does not suppress delivery in pipeline B.

**Why `TapPod` includes the run ID:**
`SinkPod`'s hash must be run-agnostic so the framework can detect "already delivered"
across re-runs. `TapPod` fires every run intentionally and has no deduplication to
preserve — including the run ID makes every firing distinguishable, which is exactly
right for traceable observation.

#### Internal record format

Internally, the full compound is stored with `::` as the component separator (using
`::` avoids ambiguity with the `:` in `method:digest` form of `ContentHash.to_string()`):

```
SinkPod:  {pipeline_hash.to_string()}::{full_input_packet_hash.to_string()}
TapPod:   {pipeline_hash.to_string()}::{full_input_packet_hash.to_string()}::{run_id}
```

Example (SinkPod):
```
blake3_v1:a3f8c2d1e5b7f0c9...::blake3_v1:b7e491f0a2c3d8e1...
```

The internal record always stores the full-fidelity compound. No output format
configuration is stored alongside it.

#### User-facing output and `InvocationHashConfig`

When a pod author accesses `ctx.invocation_hash`, the compound is serialized according
to the pod's `InvocationHashConfig`. This serialized form is what gets embedded in
external artifacts (log fields, DB columns, message bodies) **and** what is used as
the lookup key in the invocation table — the same string, in both places.

```python
@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    encoding: Literal["hex", "base64", "binary"] = "hex"
    component_length: int | None = None  # bytes of raw digest to use; None = full length
```

`component_length` is specified in **bytes of raw digest** (encoding-independent): 8 bytes
produces 16 hex characters or 11 base64 characters depending on `encoding`. The `::` separator
is fixed.

The format is self-describing: hex strings contain only `[0-9a-f]`, base64 contains
`[A-Za-z0-9+/=]`, binary is raw bytes. No configuration metadata needs to be stored
alongside the hash — the format is recoverable by inspection.

Example outputs for `component_length=8`:

| `encoding` | Example |
|---|---|
| `"hex"` (default) | `a3f8c2d1e5b7f0c9::b7e491f0a2c3d8e1` |
| `"base64"` | `o/jC0eW3::t+SR8KLD` |
| `"binary"` | `<8 raw bytes>::<8 raw bytes>` |

`InvocationHashConfig` lives inside `SinkPodConfig` and `TapPodConfig` respectively,
so each pod can independently configure its output format.

#### Lookup table organisation (mirrors `FunctionNode`)

`SinkPod` records are stored in a table scoped by `pipeline_hash(pod)` — exactly the
same pattern `FunctionNode` uses for its result tables. Within that table, the row
lookup key is the `full_input_packet_hash`. The full compound is also stored as a column
for completeness.

Reverse lookup from a user-provided hash:
1. Decode both components (format is self-describing).
2. Find tables whose scoping pipeline hash starts with component 1 (prefix match).
3. Within the matching table, find rows whose input packet hash starts with component 2.

Shorter `component_length` values increase the chance of a prefix collision but the
lookup path remains valid — the user resolves any ambiguity by inspecting multiple
candidate rows. Collision probability at 8 bytes (64 bits) per component is negligible
for any realistic invocation volume.

### 5. Hash Surface — `InvocationContext` Parameter Injection

**Decision:** The framework injects an `InvocationContext` instance when the user
function declares a parameter typed `InvocationContext`. Detection is by type
annotation only — not by parameter name. No magic.

```python
@dataclasses.dataclass(frozen=True)
class InvocationContext:
    invocation_hash: str        # serialized per pod's InvocationHashConfig
    pod_name: str               # human-readable pod label (pod.label)
    pod_content_hash: str       # pod.content_hash().to_string() — exact code version
    pipeline_run_id: str | None # None for lazy/non-compiled pipelines
```

`InvocationContext` is shared by both `SinkPod` and `TapPod`. The `invocation_hash`
field contains the serialized compound described in Axis 4 — two-part for `SinkPod`,
three-part (with verbatim `pipeline_run_id`) for `TapPod` when a run ID is available.
`pipeline_run_id` is also exposed as a first-class field so pod authors can store or
log it directly without parsing it back out of `invocation_hash`.

If no `InvocationContext` parameter is present in the user function, the framework
calls the function without one. There is zero overhead — no context object is
constructed — for pods that don't need the hash.

**Usage:**

```python
@sink_pod
def audit_write(data: PatientRecord, ctx: InvocationContext) -> None:
    db.insert(ctx.invocation_hash, data.patient_id, ctx.pod_content_hash)

@tap_pod
def log_row(data: PatientRecord, ctx: InvocationContext) -> None:
    logger.info("[%s] Processing patient %s", ctx.invocation_hash, data.patient_id)

@sink_pod
def simple_sink(data: PatientRecord) -> None:
    # No ctx needed — valid, no overhead
    external_api.push(data.patient_id)
```

**Why typed injection, not a magic parameter name:**
A parameter named `ctx` with any other type annotation (e.g. a user-defined context
object) must not be intercepted. Relying on name alone would make the mechanism fragile
and surprising. Type annotation detection is unambiguous — it fires if and only if the
declared type is exactly `InvocationContext`.

### 6. Failure Semantics

**Decision:** Each pod type has its own config class carrying `on_error`:

```python
@dataclasses.dataclass(frozen=True)
class SinkPodConfig:
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = field(default_factory=InvocationHashConfig)

@dataclasses.dataclass(frozen=True)
class TapPodConfig:
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = field(default_factory=InvocationHashConfig)
```

Separate config classes rather than a shared `SideEffectConfig`: `SinkPodConfig` will
gain `max_attempts` and `retry_on` from ITL-526 that are meaningless for `TapPod`.
Starting separate prevents a future config class with inapplicable fields.

**`on_error` vocabulary** (consistent with ITL-523's `HookConfig`):
- `"raise"` (default): exception propagates; the pipeline row is aborted.
- `"log"`: exception logged at `WARNING` level; the input row continues downstream.

No `"ignore"` option in v1.

**`SinkPod` completion state on failure:**
Regardless of `on_error`, a failed delivery always records `status="failed"` in the
invocation log. `on_error` controls whether the exception propagates to the caller —
it does not affect completion tracking. A `"failed"` row remains eligible for
re-attempt on the next explicit pipeline re-run (Axis 3).

| `on_error` | Pipeline behaviour | Completion state |
|---|---|---|
| `"raise"` (default) | Exception propagates; row aborted | `"failed"` |
| `"log"` | Exception logged; row continues downstream | `"failed"` |

**`TapPod` on failure:**
No completion tracking. `on_error` controls pipeline propagation only:
- `"raise"`: exception propagates; row aborted; downstream sees nothing.
- `"log"`: exception logged at `WARNING`; input row emitted unchanged downstream.
An optional invocation log row with `status="error"` may be written for observability.

**Relationship to `FunctionPod`:**
`FunctionPod` has no pod-level `on_error` config today. The sync path propagates
exceptions unconditionally; the async path silently drops failed items with no
configuration — an inconsistency. Aligning `FunctionPod` to adopt the same `on_error`
vocabulary is deferred; see DESIGN_ISSUES.md F15 and the corresponding Linear issue.

### 7. Ordering & DAG Placement

Side-effect pods are **synchronous barriers** in the DAG:

1. Consume input row.
2. Execute side effect (call user function with `(data, ctx)` or `(data,)` as applicable).
3. On success: emit the same row as output. Record `status="success"` in invocation log.
4. On error with `on_error="raise"`: propagate exception; do not emit output.
5. On error with `on_error="log"`: log + record; emit input row as output anyway.

For the **async channel execution path**, `async_execute()` must process each input
row, complete the side effect, and write to the output channel before returning.
Concurrent per-row execution within a single side-effect pod is permitted when
`PodConfig.max_concurrency > 1` and the pod is so configured — subject to the same
concurrency model as `FunctionPod`.

**No fire-and-forget in v1.** All side effects complete (or fail) synchronously with
respect to the pipeline's progress on that row. Downstream pods cannot begin processing
a row until the side effect for that row has finished.

### 8. Observability — `_orcapod_side_effect_invocations` Table

**Decision:** Orcapod persists every side-effect invocation to a dedicated table in the
pipeline database.

| Column | Type | Notes |
|--------|------|-------|
| `execution_id` | TEXT | Primary key — unique per actual execution |
| `invocation_signature` | TEXT | Indexed — stable per (pod, inputs) pair |
| `pod_name` | TEXT | Human-readable pod label |
| `pod_content_hash` | TEXT | `pod.content_hash().to_string()` — exact code version |
| `input_hash` | TEXT | `input_data.content_hash().to_string()` |
| `pipeline_run_id` | TEXT NULLABLE | `None` for lazy/non-compiled pipelines |
| `executed_at` | TIMESTAMP | UTC wall-clock time of execution |
| `status` | TEXT | `"success"` / `"error"` / `"skipped"` |
| `error_message` | TEXT NULLABLE | Set when `status = "error"` |

**Schema location:** A shared table in the pipeline database (not scoped per-node),
allowing cross-pipeline lookup by `invocation_signature` alone. Table is created on
first invocation if it does not exist.

This table is the **near side of the reverse-lookup chain**. Without it, a signature
extracted from an external artifact has nothing to resolve against.

**For lazy/ephemeral pipelines** (no `PipelineJob`, no DB-backed nodes): the invocation
log still records the invocation, but `pipeline_run_id` is `None` and `input_hash` can
be used only to identify the data structure — the actual data rows are not persisted
and cannot be retrieved from the framework.

### 9. Reverse Lookup Path

**Given:** `invocation_signature = "d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f"` found in an
external artifact (e.g., a Postgres audit row or a log line).

**Step 1 — Signature → invocation record:**
```sql
SELECT *
FROM _orcapod_side_effect_invocations
WHERE invocation_signature = 'd4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f'
ORDER BY executed_at
```
Returns: `{pod_name, pod_content_hash, input_hash, pipeline_run_id, executed_at, status}`.

Multiple rows may exist if `idempotent=False` and the pod ran multiple times with the
same inputs (each run has a distinct `execution_id`).

**Step 2 — Input hash → input data:**
Query the pipeline's function node result tables for rows where
`_input_data_hash = input_hash`. This locates the exact `(Tag, Data)` packet that
triggered the side effect.

*(Requires a DB-backed pipeline — `FunctionJobNode` or `OperatorJobNode`. For lazy
pipelines, the input hash identifies the data structure but rows cannot be retrieved.)*

**Step 3 — Input data → upstream lineage:**
Follow `_tag_source_id` and `_tag_record_id` system tag columns on the retrieved input
row. These columns encode the full upstream provenance chain, back to the original
source and row number.

**Step 4 — Pod code version:**
`pod_content_hash` identifies the exact pod function that ran. If the codebase is
version-controlled, this hash can be resolved to a specific commit and function definition.

**Full reverse-lookup chain:**
```
external artifact (orcapod-d4a8f3b2...)
  → _orcapod_side_effect_invocations[invocation_signature]
    → input_hash → function node result table → (Tag, Data) packet
      → _tag_source_id / _tag_record_id → upstream nodes → root source rows
        + pod_content_hash → exact pod code version
```

**Minimum persistence requirements:**
- `_orcapod_side_effect_invocations` table must be durable (not ephemeral).
- Input data rows must be persisted in DB-backed nodes for step 2 to work.
- For full lineage, all intermediate nodes must use DB-backed execution.

### 10. Serialization — Prescribe Canonical Format, Provide Helpers

**Decision:** Define a canonical artifact tag format and expose a `format_id()` helper.
Deviation is allowed but not recommended.

**Canonical format:** `orcapod-{invocation_signature}` where `invocation_signature` is
32 lowercase hex characters (128 bits).

**Examples:**

Log line (structured logging):
```
{"level": "info", "msg": "Patient processed", "orcapod_id": "orcapod-d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f", "patient_id": "P-12345"}
```

Database column:
```sql
INSERT INTO audit_log (orcapod_record_id, patient_id, processed_at)
VALUES ('d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f', 'P-12345', NOW())
```
*(DB column stores the raw signature; the `orcapod-` prefix is for human-facing contexts.)*

Slack notification body:
```
Pipeline run complete for patient P-12345.
Trace: orcapod-d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f
```

`SideEffectContext` provides:
- `ctx.format_id()` → `"orcapod-d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f"` (full canonical tag)
- `ctx.invocation_signature` → `"d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f"` (raw hex for DB columns)

Pod authors who deviate from the canonical format do so at their own risk. The reverse
lookup machinery works as long as `invocation_signature` appears in the invocation log —
but human discoverability and tooling support will be reduced for non-canonical embeddings.

---

## Reconciliation with ITL-523 (Post-Run Hook)

| Dimension | Post-run hook (ITL-523) | Side-effect pod (ITL-524) |
|-----------|------------------------|--------------------------|
| DAG presence | No — passive observer | Yes — first-class node |
| Triggers on | After a function pod runs | IS the pipeline step |
| Gates downstream | No | Yes |
| Composable | No | Yes |
| Hash name | `record_id_hash` | `invocation_signature` |
| Hash formula | `str(output_data.datagram_uuid)` (UUIDv7 per execution) | `content_hash(pod ∥ inputs).to_hex()` (deterministic) |
| Hash stability | Stable for cache hits; unique for fresh computations | Always deterministic |
| `on_error` vocabulary | `"raise"` / `"log"` | Same: `"raise"` / `"log"` |

**Hash formula divergence — justified:**

ITL-523's `record_id_hash` uses `output_data.datagram_uuid`, a UUIDv7 that is unique
per fresh execution but stable on cache hits (UUID stored in DB, restored on retrieval).
This is appropriate for function pods because the "result record ID" IS the output
datagram identity.

Side-effect pods have no output datagram, so the equivalent must be derived from
inputs. Using `content_hash(pod ∥ inputs)` is more fundamental: it is deterministic
regardless of caching, enabling idempotency checks across re-runs. For a deterministic
function pod, the two formulas converge: `content_hash(pod ∥ inputs)` ≡
`output_data.datagram_uuid` whenever the output is content-addressed from the same
inputs.

**Shared `InvocationIdentity` base type:**

To avoid duplicating the canonical `format_id()` logic and enable shared tooling
(e.g., a logging helper that works for both hook payloads and side-effect contexts),
extract a common type from `src/orcapod/hooks.py`:

```python
@dataclasses.dataclass(frozen=True)
class InvocationIdentity:
    """Shared identity carrier for post-run hooks and side-effect pods.

    Attributes:
        record_id: The invocation identifier. For post-run hooks this is
            ``str(output_data.datagram_uuid)``; for side-effect pods this is
            ``content_hash(pod || inputs).to_hex()``. Both are embeddable
            plain-text strings.
        pod_name: Human-readable pod label.
        pipeline_run_id: Run context; ``None`` for lazy pipelines.
    """
    record_id: str
    pod_name: str
    pipeline_run_id: str | None

    def format_id(self) -> str:
        """Canonical artifact tag: 'orcapod-{record_id}'."""
        return f"orcapod-{self.record_id}"
```

`SideEffectContext` may directly embed the `InvocationIdentity` fields or inherit from it
(implementation decision); it adds `execution_id` and `pod_content_hash`:

```python
@dataclasses.dataclass(frozen=True)
class SideEffectContext:
    # InvocationIdentity fields:
    invocation_signature: str   # = record_id for side-effect pods
    pod_name: str
    pipeline_run_id: str | None

    # SideEffectPod-specific fields:
    execution_id: str           # UUIDv7 unique per execution
    pod_content_hash: str       # pod.content_hash().to_string()

    def format_id(self) -> str: ...
```

Post-run hook `PodContext` gains `InvocationIdentity` fields or a reference in the
updated `PostRunPayload` (minor ITL-523 extension, not in scope for this ticket but
called out for the implementer).

---

## Worked Example

**Scenario:** An ML pipeline processes patient records and writes an audit row to an
external compliance database for each processed patient. The audit table must trace
back to the exact pipeline run and input data.

### Pod definition

```python
import orcapod as op
from orcapod import SideEffectContext, SideEffectConfig
from datetime import datetime, timezone

@op.side_effect_pod(config=SideEffectConfig(idempotent=True, on_error="log"))
def audit_patient_processing(
    data: PatientRecord,
    ctx: SideEffectContext,
) -> None:
    """Write a compliance audit row to the external database.

    Uses ctx.invocation_signature as the stable provenance key so that
    re-running the same pipeline with the same inputs does NOT produce a
    duplicate audit row (idempotent=True skips execution if the signature
    is already in the invocation log, and ON CONFLICT DO NOTHING handles
    any race between instances).
    """
    audit_db.execute(
        """
        INSERT INTO patient_audit
            (orcapod_record_id, patient_id, processed_at, pipeline_run)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (orcapod_record_id) DO NOTHING
        """,
        (
            ctx.invocation_signature,
            data.patient_id,
            datetime.now(timezone.utc),
            ctx.pipeline_run_id,
        ),
    )
```

### Pipeline

```python
patient_stream    = op.csv_source("patients_2026.csv")
validated_stream  = validate_patient_pod(patient_stream)
enriched_stream   = enrich_patient_pod(validated_stream)
# Side-effect node inserted mid-pipeline; downstream sees the same data.
audited_stream    = audit_patient_processing(enriched_stream)
results_stream    = aggregate_results_pod(audited_stream)
```

### External artifact

Row in the external `patient_audit` table:

| orcapod_record_id | patient_id | processed_at | pipeline_run |
|---|---|---|---|
| `d4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f` | P-12345 | 2026-07-14 02:30:45 UTC | run-2026-07-14-001 |

### Reverse-lookup walk-through

**Problem:** Six months later, an auditor finds the row for `P-12345` and asks: "What
input data produced this audit record, and what pipeline logic ran?"

**Step 1 — Signature → invocation record:**
```sql
SELECT pod_name, pod_content_hash, input_hash, pipeline_run_id, executed_at, status
FROM _orcapod_side_effect_invocations
WHERE invocation_signature = 'd4a8f3b2c1e90a7b5f3e8c2a9b6d4a1f'
```
```
pod_name:           "audit_patient_processing"
pod_content_hash:   "object_v0.1:c9f1a3b2..."
input_hash:         "object_v0.1:8f3a4b2c..."
pipeline_run_id:    "run-2026-07-14-001"
executed_at:        2026-07-14T02:30:45Z
status:             "success"
```

**Step 2 — Input hash → input data:**
```sql
-- Query enrich_patient_pod result table (shared pipeline hash table).
-- _input_data_hash is system_constants.INPUT_DATA_HASH_COL = "_input_data_hash"
SELECT * FROM enrich_patient_pod_results
WHERE _input_data_hash = 'object_v0.1:8f3a4b2c...'
```
```
patient_id:   "P-12345"
birth_year:   1985
diagnosis:    "T2D"
risk_score:   0.73
_tag_source_id::...::abc...   "patients_2026_csv::row47"
```

**Step 3 — System tags → upstream lineage:**
The `_tag_source_id` column resolves to the original source:
```
Source:  patients_2026.csv
Row:     47
Column:  patient_id → "P-12345"
```

**Full lineage:**
```
patient_audit[orcapod_record_id=d4a8f3b2...]
  ← audit_patient_processing (invocation_signature=d4a8f3b2..., pod_content_hash=c9f1a3b2...)
    ← enrich_patient_pod(PatientRecord{patient_id=P-12345})
      ← validate_patient_pod(PatientRecord{patient_id=P-12345})
        ← patients_2026.csv : row 47
```

**Pod code version:** `pod_content_hash = "object_v0.1:c9f1a3b2..."` can be resolved
to the exact Python function definition (git commit, code hash) via Orcapod's pod
registry or by running `content_hash()` against the current function definition.

---

## Public API Summary

```python
# src/orcapod/side_effects.py

@dataclasses.dataclass(frozen=True)
class SideEffectConfig:
    idempotent: bool = False
    on_error: Literal["raise", "log"] = "raise"


@dataclasses.dataclass(frozen=True)
class SideEffectContext:
    invocation_signature: str    # 32-char hex; deterministic per (pod, inputs)
    execution_id: str            # UUID string; unique per actual execution
    pod_name: str                # human-readable pod label
    pod_content_hash: str        # pod.content_hash().to_string()
    pipeline_run_id: str | None  # None for lazy/non-compiled pipelines

    def format_id(self) -> str:
        """Return 'orcapod-{invocation_signature}'."""
        return f"orcapod-{self.invocation_signature}"


class SideEffectPod(_FunctionPodBase):
    """A pipeline node whose primary purpose is a side effect.

    The wrapped function returns None. The input stream is passed through
    unchanged as output. Every invocation is recorded in the pipeline
    database's _orcapod_side_effect_invocations table.
    """
    ...


def side_effect_pod(
    fn: SideEffectFn | None = None,
    *,
    config: SideEffectConfig | None = None,
) -> SideEffectPod | Callable[[SideEffectFn], SideEffectPod]:
    """Decorator that wraps a function as a SideEffectPod.

    Examples:
        @side_effect_pod
        def emit_metric(data: MyData, ctx: SideEffectContext) -> None:
            metrics.emit("my.metric", data.value, tags={"id": ctx.format_id()})

        @side_effect_pod(config=SideEffectConfig(idempotent=True))
        def write_audit_row(data: MyData, ctx: SideEffectContext) -> None:
            audit_db.execute("INSERT ...", (ctx.invocation_signature, ...))
    """
    ...
```

Re-exported from `orcapod.__init__`:
- `SideEffectPod`
- `SideEffectConfig`
- `SideEffectContext`
- `side_effect_pod`

---

## Tests

New test file: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

| # | Scenario | Assertion |
|---|---|---|
| T1 | Basic execution — pod runs, output equals input | Pass-through: input (Tag, Data) emitted unchanged |
| T2 | `ctx` auto-injection — function with `ctx: SideEffectContext` | `ctx.invocation_signature` is 32 hex chars; `ctx.format_id()` returns `"orcapod-{sig}"` |
| T3 | No-ctx function — function without `ctx` param | Pod runs without error; `SideEffectContext` not constructed |
| T4 | Invocation log written — DB-backed pipeline | `_orcapod_side_effect_invocations` has one row; `status="success"` |
| T5 | `idempotent=True` — same inputs re-run | Second invocation skipped; `status="skipped"` row added; side-effect function called once only |
| T6 | `idempotent=False` (default) — same inputs re-run | Side-effect function called both times; two rows in invocation log (different `execution_id`) |
| T7 | `on_error="raise"` — function raises | Exception propagates; no output row; `status="error"` in log |
| T8 | `on_error="log"` — function raises | Exception logged; input row emitted unchanged; `status="error"` in log |
| T9 | Multiple inputs — N input rows | N log rows; pass-through of all N rows |
| T10 | `invocation_signature` determinism | Re-running pod with identical inputs + identical code → identical `invocation_signature` |
| T11 | `execution_id` uniqueness | Two runs with identical inputs → same `invocation_signature`, different `execution_id` |
| T12 | Async channel execution | Pod runs correctly through `async_execute()` path; log rows written |
| T13 | Parallel execution — `max_concurrency > 1` | All invocations complete; all log rows written; no data loss |
| T14 | Pipeline composition — side-effect pod mid-pipeline | Downstream pod receives same data as upstream; side effect runs |
| T15 | `@side_effect_pod` decorator — functional form | `@side_effect_pod` without args and `@side_effect_pod(config=...)` both produce `SideEffectPod` |

---

## Scope & Boundaries

**In scope:**
- `SideEffectPod` class, `SideEffectConfig`, `SideEffectContext`, `side_effect_pod`
  decorator.
- `_orcapod_side_effect_invocations` table: creation, insertion, idempotency check.
- `SideEffectContext` auto-injection by type annotation.
- Pass-through output stream.
- Sync and async execution paths.
- `InvocationIdentity` shared base type extracted into `hooks.py`.
- All re-exports from `orcapod.__init__`.
- Tests covering T1–T15.
- Documentation update: new `docs/concepts/side-effect-pods.md`.

**Out of scope:**
- Actual implementation (this is a design spike).
- Retry logic.
- External adapter helpers (Slack notifier, structured-logging wrapper).
- Run history UI or searchable index service (downstream of this primitive; see PLT-1950).
- Pre-run or on-error hooks for side-effect pods.
- Remote/cross-process side-effect execution.
- `idempotency_key` customization (v1 uses `invocation_signature` only).

---

## Dependencies & Risks

**Dependencies:**
- ITL-523 (post-run hook, In Progress) — `hooks.py` is the insertion point for the
  shared `InvocationIdentity` base type. Coordinate to avoid conflicting changes to
  `_FunctionPodBase`.
- PLT-1950 (EDI provenance) — the `_orcapod_side_effect_invocations` table is a
  natural data source for provenance stamping. Schema should be coordinated.

**Risks:**
- **Framework creep:** Side-effect pods that "almost" produce output. Enforced by
  the type signature (`-> None` required) and the pass-through contract.
- **Overly long hash:** 32 hex chars (128 bits) is short enough for all common embedding
  contexts. Test with real log pipelines before finalizing.
- **Invocation log growth:** A high-volume pipeline calling a side-effect pod per row
  will produce large invocation logs. Add a note in docs about pruning / archiving.
- **Idempotency false-positive:** If pod code changes between runs, the same inputs
  produce the same `invocation_signature` (code change is not reflected). This is
  correct behavior (the signature is input + code hash, so code changes produce
  different signatures), but implementers should verify the `pod_content_hash` column
  includes code identity, not just name.

---

## Follow-Up Implementation Issue

See ITL-525: *Implement `SideEffectPod` with invocation hash and invocation log
(ITL-524 follow-up).*

Task breakdown for ITL-525:

1. **`InvocationIdentity` base type** — add to `src/orcapod/hooks.py`; update
   `PostRunPayload` to embed it (minor ITL-523 coordination).
2. **`src/orcapod/side_effects.py`** — new module: `SideEffectConfig`,
   `SideEffectContext`, `SideEffectPod`, `side_effect_pod` decorator.
3. **`SideEffectPod` core execution** — `process_data()` / `async_process_data()`
   pass-through implementation; `SideEffectContext` injection; invocation log writes.
4. **Invocation log** — `_orcapod_side_effect_invocations` table creation and insertion
   in the pipeline DB; idempotency check for `idempotent=True`.
5. **`orcapod.__init__` re-exports** — `SideEffectPod`, `SideEffectConfig`,
   `SideEffectContext`, `side_effect_pod`.
6. **Tests** — `tests/test_core/side_effect_pod/test_side_effect_pod.py` covering T1–T15.
7. **Documentation** — `docs/concepts/side-effect-pods.md` with overview, usage
   examples, reverse-lookup walk-through, and idempotency guidance.
