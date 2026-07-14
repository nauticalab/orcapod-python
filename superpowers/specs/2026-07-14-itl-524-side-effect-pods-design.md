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

Convenience decorators (both wrap SideEffectPod with preset config):
├── @sink_pod   — track_completion=True,  drop_on_failure=True
└── @tap_pod    — track_completion=False, drop_on_failure=False
```

### 2. Pod Configuration Axes and Output Contract

**Decision:** A single `SideEffectPod` type governed by two independent configuration
axes in `SideEffectPodConfig`. There are no separate `SinkPod` and `TapPod` types —
only convenience decorators that preset those axes.

**Axis A — Execution frequency** (`track_completion: bool = True`):
- `True`: the framework tracks per-(pod, input) completion state. Inputs that already
  succeeded are skipped on re-run; failed inputs are re-attempted.
- `False`: no completion tracking. The pod fires unconditionally on every pipeline run.

**Axis B — Drop-on-failure control** (`drop_on_failure: bool = True`):
- `True`: rows where delivery failed are dropped; only successfully-delivered rows flow
  downstream.
- `False`: all rows flow downstream regardless of delivery outcome.

The four combinations and their canonical use cases:

| `track_completion` | `drop_on_failure` | Canonical use case |
|---|---|---|
| `True` | `True` | Audit DB write, exactly-once delivery that gates downstream |
| `True` | `False` | Deliver once; don't gate downstream on delivery success |
| `False` | `True` | Re-notify every run; downstream only sees acknowledged rows |
| `False` | `False` | Structured logging, metrics, trace everything |

**Output contract: pass-through** (all configurations). `SideEffectPod` always returns
a stream. When `drop_on_failure=True`, failed rows are dropped and only
successfully-delivered rows appear downstream. When `drop_on_failure=False`, all rows
flow through regardless of delivery outcome.

**Convenience decorators:**
`@sink_pod` presets `track_completion=True, drop_on_failure=True` — the most common
"deliver and gate" pattern.
`@tap_pod` presets `track_completion=False, drop_on_failure=False` — the most common
"observe everything" pattern.
Both decorators wrap the same underlying `SideEffectPod` class. All four combinations
remain expressible via `SideEffectPodConfig` directly.

**Rejected alternative — two distinct types (`SinkPod` / `TapPod`):**
The two axes are genuinely orthogonal; all four combinations have legitimate use cases.
Encoding just two of them as named types leaves the other two without a natural
expression and forces users to reason about type identity rather than their actual intent.
A single configurable type with convenience shortcuts is cleaner and more extensible.

### 3. Re-Execution and Completion Semantics

**When `track_completion=False`:** No completion state is maintained. Every pipeline
run, every input packet triggers the delivery unconditionally.

**When `track_completion=True`:** The framework maintains a per-pod invocation table
(see Axis 8) and consults it at the start of each execution:

| Prior state | Action |
|---|---|
| Not yet seen | Execute the delivery |
| Previously succeeded | Skip delivery; emit row downstream without re-delivery |
| Previously failed | Re-attempt |

Emitting previously-succeeded rows on re-run (without re-delivery) ensures downstream
pods receive the full dataset when the pipeline is re-run to catch up on failed rows —
not just the newly-caught rows.

Re-attempt occurs only when the pipeline is **explicitly re-run by the user**. No
automatic background retry in v1. Full retry policy (max attempts, backoff, retryable
exception types) is deferred to ITL-526, which will design a shared mechanism for both
`FunctionPod` and `SideEffectPod`.

`SideEffectPod` with `track_completion=True` does **not** participate in the
`ResultCache` / `FunctionNode` result table system. Completion state is tracked
exclusively in its own invocation table.

### 4. Invocation Hash

#### Raw identity components

The raw identity of any invocation is a **compound hash** whose number of components
depends on `track_completion`:

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
   pod that writes an audit row needs to distinguish rows by their full provenance, not
   just their data values.

3. **`pipeline_run_id`** (only when `track_completion=False`, when available) — a
   verbatim string identifying the specific pipeline execution (e.g.
   `"run-2026-07-14-001"`). This is user-controlled and included as-is without further
   hashing. When no run ID is available (lazy/in-memory pipeline), the third component
   is omitted.

**Why `pipeline_hash` rather than `content_hash` for component 1:**
`content_hash(pod)` captures only the pod's function identity. `pipeline_hash(pod)`
captures function identity *plus* the full upstream topology, forming a Merkle chain.
Using `pipeline_hash` ensures that the same pod function used in two pipelines with
different upstream structures never shares a completion namespace — a delivery confirmed
in pipeline A does not suppress delivery in pipeline B.

**Why `track_completion=False` includes the run ID:**
When `track_completion=True`, the hash must be run-agnostic so the framework can
detect "already delivered" across re-runs — including the run ID would make the hash
unique per run and break deduplication. When `track_completion=False`, the pod fires
every run intentionally and has no deduplication to preserve — including the run ID
makes every firing distinguishable, which is exactly right for traceable observation.

#### Internal record format

Internally, the full compound is stored with `::` as the component separator (using
`::` avoids ambiguity with the `:` in `method:digest` form of `ContentHash.to_string()`):

```
track_completion=True:  {pipeline_hash.to_string()}::{full_input_packet_hash.to_string()}
track_completion=False: {pipeline_hash.to_string()}::{full_input_packet_hash.to_string()}::{run_id}
```

Example (track_completion=True):
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

`InvocationContext` is shared across all `SideEffectPod` configurations. The
`invocation_hash` field contains the serialized compound described in Axis 4 —
two-part when `track_completion=True`, three-part (with verbatim `pipeline_run_id`)
when `track_completion=False` and a run ID is available. `pipeline_run_id` is also
exposed as a first-class field so pod authors can store or log it directly without
parsing it back out of `invocation_hash`.

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

**Decision:** A single unified `SideEffectPodConfig` carries all configuration axes:

```python
@dataclasses.dataclass(frozen=True)
class SideEffectPodConfig:
    track_completion: bool = True
    drop_on_failure: bool = True
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = field(default_factory=InvocationHashConfig)
```

When ITL-526 adds retry fields (`max_attempts`, `retry_on`), they are added to
`SideEffectPodConfig` — they are only meaningful when `track_completion=True`, and
that relationship is enforced at construction or with documentation.

**`on_error` vocabulary** (consistent with ITL-523's `HookConfig`):
- `"raise"` (default): exception propagates; the pipeline row is aborted.
- `"log"`: exception logged at `WARNING` level; delivery outcome determines whether
  the row passes downstream (see interaction with `drop_on_failure` below).

No `"ignore"` option in v1.

**Interaction between `on_error` and `drop_on_failure`:**

| `on_error` | `drop_on_failure` | Delivery fails → |
|---|---|---|
| `"raise"` | either | Exception propagates; row aborted |
| `"log"` | `True` | Exception logged; failed row **dropped** |
| `"log"` | `False` | Exception logged; row **emitted** downstream regardless |

**Completion state on failure (when `track_completion=True`):**
Regardless of `on_error`, a failed delivery always records `status="failed"` in the
invocation log. `on_error` controls pipeline propagation, not the completion record.
A `"failed"` row remains eligible for re-attempt on the next explicit re-run (Axis 3).

**Relationship to `FunctionPod`:**
`FunctionPod` has no pod-level `on_error` config today. The sync path propagates
exceptions unconditionally; the async path silently drops failed items with no
configuration — an inconsistency. Aligning `FunctionPod` to adopt the same `on_error`
vocabulary is deferred; see DESIGN_ISSUES.md F15 and ITL-527.

### 7. Ordering & DAG Placement

`SideEffectPod` is a per-data-packet operation — structurally identical to `FunctionPod`
in its relationship to the DAG. It can appear anywhere in the pipeline (beginning, middle,
or end), chains naturally with other pods, and follows the same concurrency model.

**Row-by-row execution flow:**

1. Receive `(tag, data)` from upstream.
2. If `track_completion=True`: check invocation log — skip delivery for previously-succeeded inputs (emit without re-delivery); re-attempt for previously-failed inputs.
3. Call user function (with `InvocationContext` injection if declared).
4. On success: write `status="success"` to invocation log; emit row downstream.
5. On error with `on_error="raise"`: propagate exception; row aborted.
6. On error with `on_error="log"` + `drop_on_failure=True`: log + write `status="failed"`; drop row.
7. On error with `on_error="log"` + `drop_on_failure=False`: log + write `status="failed"`; emit row downstream.

**No fire-and-forget in v1.** Delivery completes (or fails) before the row is emitted
or dropped. This applies to all configurations including `drop_on_failure=False`.
`drop_on_failure=False` is the natural candidate for fire-and-forget in the future
(since downstream is not gated on delivery outcome), but that optimisation is deferred
to ITL-528. See also ITL-526 for retry policy.

**Concurrency across rows:** `SideEffectPod` respects `max_concurrency` exactly as
`FunctionPod` does — multiple rows can be processed concurrently when `max_concurrency > 1`.
Invocation log writes are per-row and atomic; `track_completion=True` completion checks
use the same row-level idempotency guarantees as `FunctionNode`.

**No global barrier.** `SideEffectPod` does not wait for all rows to complete before
emitting any. It processes and emits row-by-row (or in concurrent batches), consistent
with the streaming model.

### 8. Observability — Per-Pipeline-Hash Invocation Tables

**Decision:** Each `SideEffectPod`'s invocation log is stored in a table scoped by
`pipeline_hash(pod)`, mirroring how `FunctionNode` organises its result tables.

**Table path:**
```
<db_root>/<pipeline_hash(pod)>/side_effect_invocations
```

Created on first invocation if it does not exist.

**Schema:**

| Column | Type | Notes |
|---|---|---|
| `full_input_packet_hash` | TEXT | Primary lookup key — second component of the invocation hash |
| `invocation_hash` | TEXT | Full serialized hash per pod's `InvocationHashConfig` |
| `pod_content_hash` | TEXT | `pod.content_hash().to_string()` — exact code version |
| `pipeline_run_id` | TEXT NULL | User-supplied run ID; `None` for lazy pipelines |
| `executed_at` | TIMESTAMP | UTC wall-clock time |
| `status` | TEXT | `"success"` / `"failed"` / `"skipped"` |
| `error_message` | TEXT NULL | Set when `status="failed"` |

**`status` values:**
- `"success"`: delivery completed without exception.
- `"failed"`: delivery raised an exception (regardless of `on_error` setting).
- `"skipped"`: `track_completion=True` and this input was previously succeeded; delivery skipped, row re-emitted without re-delivery.

**When the log is written:**
- `track_completion=True`: always written. The framework reads this table at the start of each execution to determine skip/retry behaviour.
- `track_completion=False`: written by default. No completion state is required, but the log enables observability (what fired, when, with what outcome). A future opt-out may be added if log volume becomes a concern.

**This table is the near side of the reverse-lookup chain.** Without it, an invocation hash extracted from an external artifact has nothing to resolve against inside Orcapod.

**For lazy/ephemeral pipelines** (no DB-backed nodes): the invocation log is still written if a DB is configured, but `pipeline_run_id` is `None` and the upstream data rows themselves are not persisted — only the hash identifies the data structure.

### 9. Reverse Lookup Path

**Given:** an invocation hash embedded in an external artifact — e.g.,
`blake3_v1:a3f8c2d1e5b7f0c9::blake3_v1:b7e491f0a2c3d8e1` found in a Postgres audit row,
a log line, or a Slack message body.

The hash is self-routing: the `::` separator partitions it into its components, and the
first component is always the `pipeline_hash(pod)` value identifying which invocation
table to open.

**Step 1 — Parse the invocation hash:**
Split on `::` to extract components:
- Component 1: `pipeline_hash` value (routes to the invocation table).
- Component 2: `full_input_packet_hash` (row lookup key within the table).
- Component 3 (if present): `pipeline_run_id` (for `track_completion=False` pods only).

**Step 2 — Locate the invocation table:**
```
<db_root>/{pipeline_hash_component}/side_effect_invocations
```
If `component_length` truncation was used, prefix-match on the pipeline hash component
to find the matching table path.

**Step 3 — Look up the invocation record:**
```sql
SELECT *
FROM side_effect_invocations
WHERE full_input_packet_hash = '{input_packet_hash_component}'
ORDER BY executed_at
```
Returns: `{invocation_hash, pod_content_hash, pipeline_run_id, executed_at, status, error_message}`.

Multiple rows exist for `track_completion=False` pods (the pod fired on multiple runs
with the same input; each firing has a distinct `executed_at`).

**Step 4 — Input packet hash → upstream data:**
`full_input_packet_hash` covers the full input packet (tag, data, source columns,
system tag columns). Query the upstream `FunctionNode` result tables to find the
`(Tag, Data)` row whose content hash matches. This locates the exact input packet that
triggered the side effect.

*(Requires a DB-backed pipeline. For lazy pipelines, the hash identifies the data
structure but the rows are not persisted and cannot be retrieved.)*

**Step 5 — Input data → upstream lineage:**
Follow system tag columns on the retrieved input row back through upstream nodes to
the original source rows.

**Step 6 — Pod code version:**
`pod_content_hash` identifies the exact function that ran. Resolve against the
codebase's version history to recover the exact function definition at that version.

**Full reverse-lookup chain:**
```
external artifact (contains invocation_hash)
  → parse → pipeline_hash component
    → <db_root>/{pipeline_hash}/side_effect_invocations[full_input_packet_hash]
      → upstream FunctionNode result tables → (Tag, Data) packet
        → system tag columns → upstream nodes → root source rows
          + pod_content_hash → exact pod code version
```

**Minimum persistence requirements:**
- The pod's per-pipeline-hash invocation table must be durable.
- Input data rows must be persisted in DB-backed upstream nodes for step 4 to work.
- For full lineage to source rows, all intermediate nodes must use DB-backed execution.

### 10. Serialization Conventions

**Decision:** Prescribe a canonical `orcapod-{hash}` artifact tag format. Expose
`ctx.format_id()` on `InvocationContext` to produce it, accepting an optional
`InvocationHashConfig` override so each call site can independently control encoding
and component length.

**`InvocationContext.format_id()`:**

```python
def format_id(self, config: InvocationHashConfig | None = None) -> str:
    """Return 'orcapod-{hash}' with optional format override.

    Args:
        config: Optional ``InvocationHashConfig`` controlling ``encoding``
            (``"hex"``, ``"base64"``, ``"binary"``) and ``component_length``
            (bytes of raw digest per component; ``None`` = full length).
            When ``None``, the pod's own ``InvocationHashConfig`` is used,
            producing the same serialization as ``ctx.invocation_hash``.

    Returns:
        ``'orcapod-{serialized_hash}'`` ready for embedding in log fields,
        DB columns, message bodies, or any plain-text artifact.
    """
```

`InvocationContext` stores the raw hash components internally and re-serializes on
demand, so `format_id(config=...)` works without the user needing to re-construct
anything.

**Usage examples:**

```python
@side_effect_pod
def audit_write(data: PatientRecord, ctx: InvocationContext) -> None:
    # Pod's own InvocationHashConfig (full hex by default):
    db.insert(ctx.format_id(), data.patient_id)
    # → "orcapod-blake3_v1:a3f8c2d1e5b7f0c9...::blake3_v1:b7e491f0a2c3d8e1..."

    # 8-byte truncated hex — compact enough for a Slack message:
    short = ctx.format_id(InvocationHashConfig(encoding="hex", component_length=8))
    slack.send(f"Patient {data.patient_id} processed. Trace: {short}")
    # → "orcapod-a3f8c2d1e5b7f0c9::b7e491f0a2c3d8e1"

    # base64 — more compact for the same bit count:
    b64 = ctx.format_id(InvocationHashConfig(encoding="base64", component_length=16))
    logger.info("invocation_id=%s", b64)
    # → "orcapod-o/jC0eW38fCp::t+SR8KLDOOE="

    # Raw hash without prefix — for a DB column where the field name implies origin:
    db.insert_raw_hash(ctx.invocation_hash, data.patient_id)
```

**Canonical format:** `orcapod-{invocation_hash}` using the pod's configured
`InvocationHashConfig`. The `orcapod-` prefix makes artifacts machine-discoverable with
a single grep pattern across all log files, DB dumps, and message bodies — the prefix
unambiguously identifies the origin as an Orcapod pipeline provenance token.

**Using `ctx.invocation_hash` directly (no prefix):**
Valid for DB columns where the field name already implies the origin (e.g.,
`orcapod_record_id`). The reverse-lookup machinery (Axis 9) requires only the raw
hash string — the `orcapod-` prefix is for human discoverability, not lookup routing.

**No shared `InvocationIdentity` base type:**
An earlier design draft proposed a shared base type for post-run hooks and side-effect
pods. With `format_id()` living directly on `InvocationContext`, no shared base is
needed. Post-run hooks (ITL-523) may adopt the `orcapod-` prefix convention
independently if desired.

---

## Reconciliation with ITL-523 (Post-Run Hook)

| Dimension | Post-run hook (ITL-523) | Side-effect pod (ITL-524) |
|-----------|------------------------|--------------------------|
| DAG presence | No — passive observer | Yes — first-class node |
| Triggers on | After a function pod runs | IS the pipeline step |
| Gates downstream | No | Yes (when `drop_on_failure=True`) |
| Composable | No | Yes |
| Hash name | `record_id_hash` | `invocation_hash` |
| Hash formula | `str(output_data.datagram_uuid)` (UUIDv7 per execution) | `pipeline_hash :: full_input_packet_hash [:: run_id]` (deterministic) |
| Hash stability | Stable for cache hits; unique for fresh computations | Always deterministic (when `track_completion=True`) |
| `on_error` vocabulary | `"raise"` / `"log"` | Same: `"raise"` / `"log"` |
| Context type | `PodContext` in `PostRunPayload` | `InvocationContext` |

**Hash formula divergence — justified:**

ITL-523's `record_id_hash` uses `output_data.datagram_uuid`, a UUIDv7 that is unique
per fresh execution but stable on cache hits (UUID stored in DB, restored on retrieval).
This is appropriate for function pods because the "result record ID" IS the output
datagram identity.

Side-effect pods have no output datagram, so the equivalent must be derived from
inputs. Using `pipeline_hash :: full_input_packet_hash` is deterministic regardless of
caching, enabling completion-tracking across re-runs. For a deterministic function pod,
the two formulas converge whenever the output is content-addressed from the same inputs.

**No shared base type:**
An earlier draft proposed extracting a shared `InvocationIdentity` base type from
`hooks.py`. `format_id(config=None)` lives directly on `InvocationContext` (Axis 10),
so no shared base is needed. Post-run hooks (ITL-523) may adopt the `orcapod-` prefix
convention independently if desired — no coordinated change to `hooks.py` is required.

---

## Worked Example

**Scenario:** An ML pipeline processes patient records and writes an audit row to an
external compliance database for each processed patient. The audit table must trace
back to the exact pipeline run and input data.

### Pod definition

```python
import orcapod as op
from orcapod import InvocationContext, SideEffectPodConfig
from datetime import datetime, timezone

# @sink_pod presets track_completion=True, drop_on_failure=True.
# on_error="log" so a failed DB write logs and drops the row rather than aborting.
@op.sink_pod(config=SideEffectPodConfig(on_error="log"))
def audit_patient_processing(
    data: PatientRecord,
    ctx: InvocationContext,
) -> None:
    """Write a compliance audit row to the external database.

    ctx.invocation_hash is deterministic per (pod topology, input packet):
    re-running the pipeline with the same inputs produces the same hash, so
    ON CONFLICT DO NOTHING provides a DB-level idempotency guard even if
    the pipeline retries a row that was previously delivered.
    """
    audit_db.execute(
        """
        INSERT INTO patient_audit
            (orcapod_record_id, patient_id, processed_at, pipeline_run)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (orcapod_record_id) DO NOTHING
        """,
        (
            ctx.invocation_hash,
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
# Audit pod: only successfully-audited rows reach aggregate_results_pod.
audited_stream    = audit_patient_processing(enriched_stream)
results_stream    = aggregate_results_pod(audited_stream)
```

### External artifact

Row in the external `patient_audit` table:

| orcapod_record_id | patient_id | processed_at | pipeline_run |
|---|---|---|---|
| `blake3_v1:a3f8c2d1::blake3_v1:b7e491f0` | P-12345 | 2026-07-14 02:30:45 UTC | run-2026-07-14-001 |

### Reverse-lookup walk-through

**Problem:** Six months later, an auditor finds the row for `P-12345` and asks: "What
input data produced this audit record, and what pipeline logic ran?"

**Step 1 — Parse the invocation hash:**
`blake3_v1:a3f8c2d1::blake3_v1:b7e491f0` → split on `::`:
- Component 1 (pipeline hash): `blake3_v1:a3f8c2d1`
- Component 2 (input packet hash): `blake3_v1:b7e491f0`

**Step 2 — Locate the invocation table and look up the record:**
```
table: <db_root>/blake3_v1:a3f8c2d1.../side_effect_invocations
```
```sql
SELECT * FROM side_effect_invocations
WHERE full_input_packet_hash = 'blake3_v1:b7e491f0...'
```
```
invocation_hash:    "blake3_v1:a3f8c2d1...::blake3_v1:b7e491f0..."
pod_content_hash:   "blake3_v1:c9f1a3b2..."
pipeline_run_id:    "run-2026-07-14-001"
executed_at:        2026-07-14T02:30:45Z
status:             "success"
```

**Step 3 — Input packet hash → input data:**
```sql
-- Query enrich_patient_pod result table using the full_input_packet_hash.
SELECT * FROM enrich_patient_pod_results
WHERE _input_data_hash = 'blake3_v1:b7e491f0...'
```
```
patient_id:   "P-12345"
birth_year:   1985
diagnosis:    "T2D"
risk_score:   0.73
_tag_source_id::...   "patients_2026_csv::row47"
```

**Step 4 — System tags → upstream lineage:**
```
Source:  patients_2026.csv
Row:     47
```

**Full lineage:**
```
patient_audit[orcapod_record_id=blake3_v1:a3f8c2d1::blake3_v1:b7e491f0]
  ← audit_patient_processing (invocation_hash, pod_content_hash=blake3_v1:c9f1a3b2...)
    ← enrich_patient_pod(PatientRecord{patient_id=P-12345})
      ← validate_patient_pod(PatientRecord{patient_id=P-12345})
        ← patients_2026.csv : row 47
```

---

## Public API Summary

```python
# src/orcapod/side_effects.py

@dataclasses.dataclass(frozen=True)
class InvocationHashConfig:
    encoding: Literal["hex", "base64", "binary"] = "hex"
    component_length: int | None = None  # bytes of raw digest per component; None = full


@dataclasses.dataclass(frozen=True)
class SideEffectPodConfig:
    track_completion: bool = True   # True = fire-once with completion tracking
    drop_on_failure: bool = True    # True = drop failed rows; False = always pass through
    on_error: Literal["raise", "log"] = "raise"
    hash_config: InvocationHashConfig = field(default_factory=InvocationHashConfig)


@dataclasses.dataclass(frozen=True)
class InvocationContext:
    invocation_hash: str        # serialized per pod's InvocationHashConfig
    pod_name: str               # human-readable pod label (pod.label)
    pod_content_hash: str       # pod.content_hash().to_string() — exact code version
    pipeline_run_id: str | None # None for lazy/non-compiled pipelines

    def format_id(self, config: InvocationHashConfig | None = None) -> str:
        """Return 'orcapod-{hash}' with optional format override.

        Args:
            config: Optional ``InvocationHashConfig`` overriding encoding and
                component_length for this specific call. When ``None``, uses
                the pod's own ``InvocationHashConfig``.
        """
        ...


class SideEffectPod(_FunctionPodBase):
    """A pipeline node whose primary purpose is a side effect.

    Takes an input stream and returns a filtered output stream. When
    ``drop_on_failure=True``, only rows where delivery succeeded are emitted
    downstream. Every invocation is recorded in a per-pipeline-hash invocation
    table at ``<db_root>/<pipeline_hash>/side_effect_invocations``.
    """
    ...


def side_effect_pod(
    fn: SideEffectFn | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[SideEffectFn], SideEffectPod]:
    """Wrap a function as a SideEffectPod with explicit config.

    Examples:
        @side_effect_pod
        def emit_metric(data: MyData, ctx: InvocationContext) -> None:
            metrics.emit("my.metric", data.value, tags={"id": ctx.format_id()})

        @side_effect_pod(config=SideEffectPodConfig(track_completion=False, drop_on_failure=False))
        def trace_row(data: MyData, ctx: InvocationContext) -> None:
            logger.debug("[%s] row seen", ctx.invocation_hash)
    """
    ...


def sink_pod(
    fn: SideEffectFn | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[SideEffectFn], SideEffectPod]:
    """Shortcut: track_completion=True, drop_on_failure=True.

    Examples:
        @sink_pod(config=SideEffectPodConfig(on_error="log"))
        def write_audit_row(data: MyData, ctx: InvocationContext) -> None:
            audit_db.execute("INSERT ...", (ctx.invocation_hash, ...))
    """
    ...


def tap_pod(
    fn: SideEffectFn | None = None,
    *,
    config: SideEffectPodConfig | None = None,
) -> SideEffectPod | Callable[[SideEffectFn], SideEffectPod]:
    """Shortcut: track_completion=False, drop_on_failure=False.

    Examples:
        @tap_pod
        def log_row(data: MyData, ctx: InvocationContext) -> None:
            logger.info("[%s] Processing %s", ctx.invocation_hash, data.id)
    """
    ...
```

Re-exported from `orcapod.__init__`:
- `SideEffectPod`
- `SideEffectPodConfig`
- `InvocationHashConfig`
- `InvocationContext`
- `side_effect_pod`
- `sink_pod`
- `tap_pod`

---

## Tests

New test file: `tests/test_core/side_effect_pod/test_side_effect_pod.py`

| # | Scenario | Assertion |
|---|---|---|
| T1 | Basic pass-through — `drop_on_failure=False`, success | Output stream contains all input rows unchanged |
| T2 | `drop_on_failure=True` (default), success | Successfully-delivered rows emitted; no rows dropped |
| T3 | `ctx: InvocationContext` auto-injection | `ctx.invocation_hash` is a non-empty string; `ctx.format_id()` returns `"orcapod-{hash}"` |
| T4 | No-`ctx` function | Pod runs without error; `InvocationContext` not constructed |
| T5 | Invocation log written — DB-backed pipeline | Row written to `<pipeline_hash>/side_effect_invocations`; `status="success"` |
| T6 | `track_completion=True` — same inputs re-run | Second run: delivery skipped, `status="skipped"` row added; function called once only; row still emitted downstream |
| T7 | `track_completion=False` — same inputs re-run | Both runs: function called; two `status="success"` log rows |
| T8 | `on_error="raise"` — function raises | Exception propagates; row aborted; `status="failed"` in log |
| T9 | `on_error="log"` + `drop_on_failure=True` — function raises | Exception logged; row **dropped** from output; `status="failed"` in log |
| T10 | `on_error="log"` + `drop_on_failure=False` — function raises | Exception logged; row **emitted** downstream; `status="failed"` in log |
| T11 | `invocation_hash` determinism | Re-running pod with identical inputs + code → identical `invocation_hash` |
| T12 | `invocation_hash` format override | `ctx.format_id(InvocationHashConfig(encoding="base64", component_length=8))` produces valid base64 compound |
| T13 | Async channel execution | Pod runs correctly through `async_execute()` path; log rows written |
| T14 | Parallel execution — `max_concurrency > 1` | All invocations complete; all log rows written; no data loss |
| T15 | Pipeline composition mid-pipeline | Downstream pod receives correct filtered output; side effect runs |
| T16 | `@sink_pod` shortcut | Produces `SideEffectPod` with `track_completion=True, drop_on_failure=True` |
| T17 | `@tap_pod` shortcut | Produces `SideEffectPod` with `track_completion=False, drop_on_failure=False` |
| T18 | `@side_effect_pod(config=...)` with all four combinations | Each combination behaves per spec |

---

## Scope & Boundaries

**In scope:**
- `SideEffectPod` class, `SideEffectPodConfig`, `InvocationHashConfig`,
  `InvocationContext`, `side_effect_pod` / `sink_pod` / `tap_pod` decorators.
- Per-pipeline-hash invocation tables: creation, insertion, completion-state lookup.
- `InvocationContext` auto-injection by type annotation.
- Filtered pass-through output stream (`drop_on_failure` axis).
- Completion tracking and skip-on-success logic (`track_completion` axis).
- Sync and async execution paths.
- All re-exports from `orcapod.__init__`.
- Tests covering T1–T18.
- Documentation: new `docs/concepts/side-effect-pods.md`.

**Out of scope:**
- Actual implementation (this is a design spike).
- Retry policy (ITL-526).
- Fire-and-forget delivery mode (ITL-528).
- `FunctionPod` error handling alignment (ITL-527).
- External adapter helpers (Slack notifier, structured-logging wrapper).
- Run history UI or searchable index service (PLT-1950).
- Remote/cross-process side-effect execution.

---

## Dependencies & Risks

**Dependencies:**
- ITL-523 (post-run hook, In Progress) — coordinate to avoid conflicting changes to
  `_FunctionPodBase`. No shared base type needed; `InvocationContext` is independent.
- ITL-526 (retry policy) — deferred; will add fields to `SideEffectPodConfig`.
- ITL-527 (`FunctionPod` error handling alignment) — deferred; will reuse the same
  `on_error` vocabulary defined here.
- ITL-528 (fire-and-forget mode) — deferred; will add `async_delivery` to
  `SideEffectPodConfig`.
- PLT-1950 (EDI provenance) — the per-pipeline-hash invocation tables are a natural
  data source for provenance stamping. Schema should be coordinated when that work begins.

**Risks:**
- **Invocation log growth:** A high-volume pipeline with `track_completion=False` will
  write one log row per row per run. Document pruning / archiving guidance.
- **Hash truncation collision:** Shorter `component_length` values (via `InvocationHashConfig`)
  increase prefix-match ambiguity in reverse lookup. Document the trade-off; 8 bytes
  (64 bits) per component is negligible collision probability for realistic volumes.
- **`track_completion=True` + code change:** `pipeline_hash(pod)` captures the full
  upstream topology and function identity, so a code change does produce a different
  pipeline hash and a different invocation table — previously-succeeded rows in the old
  table are not seen, and delivery re-runs. This is correct but should be documented
  clearly so pod authors are not surprised.

---

## Follow-Up Implementation Issue

See ITL-525: *Implement `SideEffectPod` (ITL-524 follow-up).*

Task breakdown for ITL-525:

1. **`src/orcapod/side_effects.py`** — new module:
   - `InvocationHashConfig` dataclass
   - `SideEffectPodConfig` dataclass (`track_completion`, `drop_on_failure`, `on_error`,
     `hash_config`)
   - `InvocationContext` dataclass with `format_id(config=None)` method
   - `SideEffectPod` class (extends `_FunctionPodBase`)
   - `side_effect_pod`, `sink_pod`, `tap_pod` decorators

2. **`SideEffectPod` core execution** — `process_data()` / `async_process_data()`:
   - `InvocationContext` injection (detect by type annotation, construct from raw
     hash components)
   - Row filtering per `drop_on_failure`
   - `on_error` handling: `"raise"` propagates; `"log"` logs + drops-or-emits per
     `drop_on_failure`

3. **Invocation table** — per-pipeline-hash table at
   `<db_root>/<pipeline_hash>/side_effect_invocations`:
   - Table creation on first invocation
   - Row insertion with all schema columns
   - Completion-state lookup for `track_completion=True` (skip / re-attempt logic)
   - `"skipped"` row for re-emitted previously-succeeded inputs

4. **Hash computation** — `full_input_packet_hash` covering tag + data + source +
   system tag columns; invocation hash compound construction and serialization per
   `InvocationHashConfig`

5. **`orcapod.__init__` re-exports** — `SideEffectPod`, `SideEffectPodConfig`,
   `InvocationHashConfig`, `InvocationContext`, `side_effect_pod`, `sink_pod`, `tap_pod`

6. **Tests** — `tests/test_core/side_effect_pod/test_side_effect_pod.py` covering T1–T18

7. **Documentation** — `docs/concepts/side-effect-pods.md` with overview, config axes,
   usage examples (all four `track_completion` × `drop_on_failure` combinations),
   reverse-lookup walk-through, and log growth guidance
