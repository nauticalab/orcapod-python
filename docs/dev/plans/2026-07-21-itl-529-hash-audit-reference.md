# Hash Audit Reference Doc Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce `docs/reference/hashing.md` — a developer reference documenting all 14 hash computation sites in the orcapod-python framework — and `superpowers/scripts/hash_audit_example.py` — a runnable script that prints concrete hash values for copy-paste into the reference doc.

**Architecture:** Write the script first (it generates the concrete hash values needed for Section 7 of the reference doc), then write the reference doc section by section, filling in the worked example at the end.  This is a documentation-only task — no hash computation code is changed.

**Tech Stack:** Python, orcapod-python (DictSource, Join, FunctionPod, FunctionJobNode, SideEffectPod, PipelineJob), PyArrow, mkdocs-material.

---

### Task 1: Create feature branch and directories

**Files:**
- No source files yet — just git and directory setup

- [ ] **Step 1: Create and check out the feature branch**

```bash
git checkout main
git pull
git checkout -b eywalker/itl-529-audit-all-hash-computation-sites-document-inputs-algorithm
```

- [ ] **Step 2: Create the output directories**

```bash
mkdir -p docs/reference
mkdir -p superpowers/scripts
```

- [ ] **Step 3: Verify the directories exist**

Run: `ls docs/reference docs/metamorphic/plans/ superpowers/scripts/`
Expected: all three directories listed with no error.

- [ ] **Step 4: Commit the empty directories with placeholder files**

```bash
touch docs/reference/.gitkeep
touch superpowers/scripts/.gitkeep
git add docs/reference/.gitkeep superpowers/scripts/.gitkeep
git commit -m "chore: create docs/reference and superpowers/scripts directories (ITL-529)"
```

---

### Task 2: Write hash_audit_example.py — Part 1 (Sources + Schema Tags)

**Files:**
- Create: `superpowers/scripts/hash_audit_example.py`

- [ ] **Step 1: Write the script skeleton and Part 1 (framework object identity + source provenance)**

Create `superpowers/scripts/hash_audit_example.py` with this full content:

```python
#!/usr/bin/env python
"""hash_audit_example.py — concrete hash values for docs/reference/hashing.md.

Constructs a deterministic example pipeline (two DictSources joined on "student",
fed into a FunctionPod + FunctionJobNode + SideEffectPod + PipelineJob) and prints
all 14 hash site values.  Use explicit source_id values so every hash is
reproducible across runs.

Run with:  uv run python superpowers/scripts/hash_audit_example.py
"""
from __future__ import annotations

import hashlib

from orcapod.config import DEFAULT_CONFIG
from orcapod.contexts import resolve_context
from orcapod.core.function_pod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.join import Join
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob
from orcapod.side_effects import InvocationContext, SideEffectPod
from orcapod.utils.schema_utils import compute_schema_hash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def section(title: str) -> None:
    print(f"\n{'=' * 64}")
    print(f"  {title}")
    print("=" * 64)


def sub(title: str) -> None:
    print(f"\n  --- {title} ---")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    ctx = resolve_context()
    semantic_hasher = ctx.semantic_hasher
    schema_n_char = DEFAULT_CONFIG.hashing.schema_n_char

    # ------------------------------------------------------------------
    # Build the example pipeline (deterministic source_id values)
    # ------------------------------------------------------------------

    scores = DictSource(
        [
            {"student": "alice", "math": 90},
            {"student": "bob",   "math": 75},
        ],
        tag_columns=["student"],
        source_id="scores_v1",
    )
    attendance = DictSource(
        [
            {"student": "alice", "days": 180},
            {"student": "bob",   "days": 160},
        ],
        tag_columns=["student"],
        source_id="attendance_v1",
    )

    join_op = Join()
    joined = join_op(scores, attendance)

    @function_pod(output_keys="grade")
    def grade(math: int, days: int) -> float:
        return math * 0.7 + days / 180.0 * 30.0

    pod = grade.pod

    # ==================================================================
    # [1]  FRAMEWORK OBJECT IDENTITY
    # ==================================================================
    section("[1] FRAMEWORK OBJECT IDENTITY")

    sub("Sources (content_hash vs pipeline_hash)")
    print(f"    scores.content_hash()       = {scores.content_hash()}")
    print(f"    scores.pipeline_hash()      = {scores.pipeline_hash()}")
    print(f"    attendance.content_hash()   = {attendance.content_hash()}")
    print(f"    attendance.pipeline_hash()  = {attendance.pipeline_hash()}")

    sub("Joined stream")
    print(f"    joined.content_hash()       = {joined.content_hash()}")
    print(f"    joined.pipeline_hash()      = {joined.pipeline_hash()}")

    sub("FunctionPod")
    print(f"    pod.content_hash()          = {pod.content_hash()}")
    print(f"    pod.pipeline_hash()         = {pod.pipeline_hash()}")
    print(f"    pod.ctx_arg_name            = {pod.ctx_arg_name!r}  (None → content == pipeline)")

    sub("DataFunction URI components")
    data_fn = pod._data_function
    print(f"    data_function.uri           = {data_fn.uri}")

    # ==================================================================
    # [2]  SOURCE PROVENANCE & SYSTEM TAGS
    # ==================================================================
    section("[2] SOURCE PROVENANCE & SYSTEM TAGS")

    sub("Site 3 — Schema hash embedded in system-tag column names")
    tag_schema, data_schema = scores.output_schema()
    schema_hash = compute_schema_hash(tag_schema, data_schema, semantic_hasher, schema_n_char)
    print(f"    (tag_schema, data_schema)   = ({tag_schema!r}, {data_schema!r})")
    print(f"    schema_hash (char_count={schema_n_char!r}) = {schema_hash!r}")

    sub("Site 4 — Default source_id (explicit in this example → no hash computed)")
    print(f"    scores.source_id            = {scores.source_id!r}  (explicit; skips Arrow hash)")

    sub("Sites 3 + 5 — System tag column names and per-row record_id values")
    scores_rows = list(scores.iter_data())
    alice_tag_src, _ = scores_rows[0]
    bob_tag_src,   _ = scores_rows[1]

    alice_sys = alice_tag_src.as_table(columns={"system_tags": True})
    bob_sys   = bob_tag_src.as_table(columns={"system_tags": True})

    print(f"    system tag columns          = {alice_sys.schema.names!r}")

    record_id_col = next(n for n in alice_sys.schema.names if "record_id" in n)
    source_id_col = next(n for n in alice_sys.schema.names if "source_id" in n)

    print(f"    alice {source_id_col!r}  = {alice_sys[source_id_col][0].as_py()!r}")
    print(f"    alice {record_id_col!r}  = {alice_sys[record_id_col][0].as_py().hex()!r}  (UUID v5 bytes)")
    print(f"    bob   {record_id_col!r}  = {bob_sys[record_id_col][0].as_py().hex()!r}  (UUID v5 bytes)")

    sub("Site 6 — Post-join system tag column names (topology suffix appended)")
    joined_rows = list(joined.iter_data())
    alice_tag_j, alice_data_j = joined_rows[0]
    bob_tag_j,   bob_data_j   = joined_rows[1]

    alice_joined_sys = alice_tag_j.as_table(columns={"system_tags": True})
    print(f"    post-join system tag cols   = {alice_joined_sys.schema.names!r}")
    print("    (each original col name → col name + '::{pipeline_hash}:{idx}')")
```

- [ ] **Step 2: Verify the file exists and is syntactically valid**

Run: `uv run python -c "import ast; ast.parse(open('superpowers/scripts/hash_audit_example.py').read()); print('syntax OK')"`
Expected: `syntax OK`

- [ ] **Step 3: Commit Part 1**

```bash
git add superpowers/scripts/hash_audit_example.py
git commit -m "feat(scripts): add hash_audit_example.py Part 1 — sources and schema tags (ITL-529)"
```

---

### Task 3: Extend hash_audit_example.py — Part 2 (Entry IDs + FunctionJobNode + Side-Effect)

**Files:**
- Modify: `superpowers/scripts/hash_audit_example.py` (append to `main()`)

- [ ] **Step 1: Append Part 2 to main() — entry IDs and side-effect invocation hash**

Open `superpowers/scripts/hash_audit_example.py`. At the end of `main()`, **after** the Site 6 section, add:

```python
    # ==================================================================
    # [3]  PIPELINE DB ENTRY KEYS  (sites 7 + 8)
    # ==================================================================
    section("[3] PIPELINE DB ENTRY KEYS (FunctionJobNode)")

    db_for_node = InMemoryArrowDatabase()
    node = FunctionJobNode(
        function_pod=pod,
        input_stream=joined,
        pipeline_database=db_for_node,
    )

    sub("Site 7 — compute_base_entry_id() — recomputation-index-free")
    alice_base = node.compute_base_entry_id(alice_tag_j, alice_data_j)
    bob_base   = node.compute_base_entry_id(bob_tag_j, bob_data_j)
    print(f"    alice base_entry_id (hex)   = {alice_base.hex()!r}")
    print(f"    bob   base_entry_id (hex)   = {bob_base.hex()!r}")

    sub("Site 8 — compute_pipeline_entry_id() — includes recomputation_index=0")
    alice_pipe = node.compute_pipeline_entry_id(alice_tag_j, alice_data_j, recomputation_index=0)
    bob_pipe   = node.compute_pipeline_entry_id(bob_tag_j, bob_data_j,   recomputation_index=0)
    print(f"    alice pipeline_entry_id (hex) = {alice_pipe.hex()!r}")
    print(f"    bob   pipeline_entry_id (hex) = {bob_pipe.hex()!r}")

    sub("FunctionJobNode hashes")
    print(f"    node.content_hash()         = {node.content_hash()}")
    print(f"    node.pipeline_hash()        = {node.pipeline_hash()}")

    # ==================================================================
    # [4]  SIDE-EFFECT RECORD ID & INVOCATION HASH  (sites 9 + 10)
    # ==================================================================
    section("[4] SIDE-EFFECT RECORD ID & INVOCATION HASH")

    captured_ctx: list[InvocationContext] = []

    def _log_fn(data, ctx: InvocationContext) -> None:  # noqa: ANN001
        captured_ctx.append(ctx)

    side_pod = SideEffectPod(_log_fn, name="audit_logger")
    side_stream = side_pod.process(joined)
    for _ in side_stream.iter_data():  # trigger execution to populate captured_ctx
        pass

    for i, ctx in enumerate(captured_ctx):
        row_label = "alice" if i == 0 else "bob"
        sub(f"Site 10 — invocation_hash for {row_label}")
        print(f"    invocation_hash             = {ctx.invocation_hash!r}")
```

- [ ] **Step 2: Verify syntax**

Run: `uv run python -c "import ast; ast.parse(open('superpowers/scripts/hash_audit_example.py').read()); print('syntax OK')"`
Expected: `syntax OK`

- [ ] **Step 3: Commit Part 2**

```bash
git add superpowers/scripts/hash_audit_example.py
git commit -m "feat(scripts): add hash_audit_example.py Part 2 — entry IDs and invocation hash (ITL-529)"
```

---

### Task 4: Extend hash_audit_example.py — Part 3 (Data Function URI + PipelineJob + Datagram) and run

**Files:**
- Modify: `superpowers/scripts/hash_audit_example.py` (append to `main()`, add `if __name__ == "__main__"`)

- [ ] **Step 1: Append Part 3 to main() and add entry point**

At the end of `main()`, after the Site 10 section, add:

```python
    # ==================================================================
    # [5]  DATA FUNCTION URI HASH  (site 11)
    # ==================================================================
    section("[5] DATA FUNCTION URI HASH")

    sub("Site 11 — output_schema_hash embedded in data_function.uri")
    print(f"    data_function.uri           = {data_fn.uri!r}")
    print(f"    output_schema_hash (site 11)= {data_fn.uri[1]!r}")
    print("    uri layout: (canonical_name, output_schema_hash, major_version, type_id)")

    # ==================================================================
    # [6]  PIPELINE RUN IDENTITY  (sites 12 + 13)
    # ==================================================================
    section("[6] PIPELINE RUN IDENTITY")

    # Use a simple single-source pipeline so snapshot_hash is easy to reproduce.
    @function_pod(output_keys="grade_simple")
    def grade_simple(math: int) -> float:
        return math * 0.7

    simple_pod = grade_simple.pod
    job_db = InMemoryArrowDatabase()

    with PipelineJob(name="hash_audit", store=job_db) as job:
        job_scores = DictSource(
            [
                {"student": "alice", "math": 90},
                {"student": "bob",   "math": 75},
            ],
            tag_columns=["student"],
            source_id="scores_v1",
        )
        simple_pod.process(job_scores)

    job.run()

    sub("Site 12 — run_id (non-deterministic UUID v4 truncated)")
    print(f"    run_id                      = {job._run_id!r}  (random each run)")

    sub("Site 13 — snapshot_hash (deterministic from leaf content hashes)")
    # Reproduce PipelineJob.run() algorithm from the compiled node map.
    # Edge (u_hash, v_hash): u is upstream → v is downstream.
    # Leaf = node whose hash does NOT appear as the upstream (u) in any edge.
    upstream_set = {u_hash for u_hash, _ in job._graph_edges}
    leaf_hashes = sorted(
        n.content_hash().to_string()
        for h, n in job._persistent_node_map.items()
        if h not in upstream_set and hasattr(n, "content_hash")
    )
    snapshot_hash = hashlib.sha256("\n".join(leaf_hashes).encode()).hexdigest()[:16]
    print(f"    leaf node count             = {len(leaf_hashes)}")
    for lh in leaf_hashes:
        print(f"    leaf content_hash           = {lh!r}")
    print(f"    snapshot_hash (site 13)     = {snapshot_hash!r}")
    print(f"    pipeline_uri                = hash_audit@{snapshot_hash}")

    # ==================================================================
    # [14] DATAGRAM UUID
    # ==================================================================
    section("[14] DATAGRAM UUID")

    sub("site 14 — datagram_uuid (time-ordered, not a content hash)")
    print(f"    alice_data_j.datagram_uuid  = {alice_data_j.datagram_uuid}")
    print(f"    bob_data_j.datagram_uuid    = {bob_data_j.datagram_uuid}")
    print("    (UUIDs differ between runs — not reproducible)")
    print()
```

Then, **below the `main()` function definition**, add:

```python
if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify syntax**

Run: `uv run python -c "import ast; ast.parse(open('superpowers/scripts/hash_audit_example.py').read()); print('syntax OK')"`
Expected: `syntax OK`

- [ ] **Step 3: Run the script and capture its output**

Run: `uv run python superpowers/scripts/hash_audit_example.py 2>&1 | tee /tmp/hash_audit_output.txt`

Expected: The script runs to completion and prints structured output with 6 sections (`[1]` through `[6]` plus `[14]`).  No stack trace.

If the script fails:
- `ImportError`: check that all imports match the file paths in the project layout in `CLAUDE.md`.
- `ValueError` from `FunctionJobNode`: `joined` may be a `DynamicPodStream` — try materialising it first with `list(joined.iter_data())` before constructing the node.
- `AttributeError: 'NoneType' has no attribute 'hex'`: the `record_id` column may store `bytes` differently — try `.as_py()` and check what it returns.

- [ ] **Step 4: Save the output for use in Task 11**

Run: `cat /tmp/hash_audit_output.txt`

Annotate each value — you will use these to fill in the worked example table in Task 11. The values are deterministic (except `run_id` and `datagram_uuid`) and stable across Python / orcapod versions as long as the hashing implementation doesn't change.

- [ ] **Step 5: Commit Part 3**

```bash
git add superpowers/scripts/hash_audit_example.py
git commit -m "feat(scripts): complete hash_audit_example.py — all 14 hash sites (ITL-529)"
```

---

### Task 5: Create docs/reference/hashing.md — Header, Introduction, and Index Table

**Files:**
- Create: `docs/reference/hashing.md`

- [ ] **Step 1: Write the file header, introduction, and 14-row index table**

Create `docs/reference/hashing.md` with this content:

````markdown
# Hashing Reference

This document is a developer-facing reference for every hash computation site in the
orcapod-python framework. For conceptual background on the two identity chains
(`content_hash` and `pipeline_hash`), see [Identity & Hashing](../concepts/identity.md).

---

## Hash Site Index

The table below summarises all 14 hash computation sites across the six usage groups
documented in the sections that follow.

| # | Site | Algorithm | Output format | One-line guarantee |
|---|------|-----------|---------------|--------------------|
| 1 | `content_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per semantic content of the object; data-inclusive |
| 2 | `pipeline_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per pipeline topology + schema; data-exclusive |
| 3 | Schema hash (system tag column naming) | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(n)` | Truncated hex `str` | Unique per `(tag_schema, data_schema)` pair; embedded in system tag column names |
| 4 | Default `source_id` | `StarfixArrowHasher.hash_table(table).to_hex(n)` | Truncated hex `str` | Unique per raw table content; used as source identifier when none is provided |
| 5 | Per-row `record_id` (system tag value) | `uuid.uuid5(NAMESPACE, f"{source_id}::{provenance_token}")` | `bytes` (16, UUID v5) | Deterministic per `(source_id, row_identity)`; stable across re-runs of the same source |
| 6 | Join system tag suffix | `stream.pipeline_hash().to_hex(n)` + `:{idx}` appended to column name | Column name suffix | Unique per `(input topology, canonical join position)`; encodes full join lineage in column name |
| 7 | `compute_base_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts |
| 8 | `compute_pipeline_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + recomputation_index)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content, recomputation attempt)` |
| 9 | Side-effect `record_id` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + NODE_CONTENT_HASH_COL + recomputation_index=0)` | `bytes` | Unique per `(tag lineage, input_data content, pod version)` |
| 10 | `invocation_hash` | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` | `str` | Unique per `(pod topology, tag lineage, input_data content, pod version)` |
| 11 | Output schema hash | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` | `str` (ContentHash `.to_string()`) | Unique per output schema definition; embedded in function URI |
| 12 | `run_id` | `uuid.uuid4().hex[:16]` | 16-char hex `str` | Non-deterministic; unique per pipeline execution |
| 13 | `snapshot_hash` | `hashlib.sha256(sorted_leaf_content_hash_strings joined by newline).hexdigest()[:16]` | 16-char hex `str` | Unique per `(DAG leaf topology + data state)` at run time |
| 14 | `datagram_uuid` | `uuid7()` normalised to `stdlib uuid.UUID` | `uuid.UUID` | Unique per datagram instance; time-ordered; not a content hash |

---
````

- [ ] **Step 2: Verify file was created**

Run: `wc -l docs/reference/hashing.md`
Expected: file exists and has ~35–45 lines.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add header, intro, and 14-row hash site index (ITL-529)"
```

---

### Task 6: Write Section 1 — Framework Object Identity

**Files:**
- Modify: `docs/reference/hashing.md` (append after the `---` at end of index table)

- [ ] **Step 1: Append Section 1 to hashing.md**

Append the following after the closing `---` of the index table:

````markdown
## 1. Framework Object Identity

Every class that inherits from `TraceableBase` carries both a `content_hash()` and a
`pipeline_hash()`. Both use `SemanticAwarePythonHasher`, which recursively expands the
object's identity structure into a JSON-serialisable representation and takes its
SHA-256 digest.

### Algorithm — `SemanticAwarePythonHasher`

1. Call `identity_structure()` (for content hash) or `pipeline_identity_structure()` (for pipeline hash) on the object.
2. Recursively expand the structure: `ContentIdentifiable` objects are replaced by their hash via a **resolver callback** (see below); containers (list, dict, tuple, set) are serialised to nested JSON.
3. JSON-serialise the expanded structure and compute `hashlib.sha256(json_bytes).digest()` → `ContentHash`.

**Resolver pattern:** The resolver callback is threaded through the entire recursive hash computation to ensure one consistent context per call:

- **`content_resolver`** (used by `content_hash()`): routes every `ContentIdentifiable` leaf through `leaf.content_hash(hasher)`.
- **`pipeline_resolver`** (used by `pipeline_hash()`): routes `PipelineElementProtocol` objects through `leaf.pipeline_hash(hasher)`; routes all other `ContentIdentifiable` objects (e.g. raw data values) through `leaf.content_hash(hasher)`.

Both hashes are **cached by `hasher_id`** on each object, so repeated calls are free.

---

### 1a. `content_hash()` — data-inclusive identity

The table below shows what each class returns from `identity_structure()`.

| Class | `identity_structure()` return value | Notes |
|-------|--------------------------------------|-------|
| `Datagram` | `self._ensure_data_table()` — the raw Arrow table | Dispatched to `ArrowTableHandler` → `StarfixArrowHasher` |
| `Tag` | User tag columns only (raw Arrow table) | System tag columns (`_tag::*`) excluded — they are provenance metadata, not tag content |
| `Data` | Data columns only (raw Arrow table) | Source info columns (`_source_*`) excluded — they are provenance metadata, not data content |
| `EmptyData` | Raises `EmptyDataAccessError` | `content_hash()` is overridden to return a stored `cached_content_hash` directly |
| `DataFunctionBase` | `self.uri` — `(canonical_function_name, output_schema_hash, major_version, data_function_type_id)` | `output_schema_hash` is site 11; see §5 |
| `FunctionPod` | `(self.data_function,)` when no ctx arg; `(self.data_function, self._ctx_arg_name)` when ctx arg present | `ctx_arg_name` is included here so a ctx-aware pod has a distinct `content_hash` from a regular pod using the same data function |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams))` | Falls back to table content hash if no producer |
| `RootSource` (`ArrowTableSource`) | Class name + tag columns + table content hash | Data-inclusive base case of the Merkle chain |
| `DerivedSource` | Origin node's `content_hash` | Inherits its generating node's identity |
| Operators (unary) | `(operator_class_name, upstream_stream)` | Stream reference resolved via `content_resolver` |
| Operators (binary/N-ary) | `(operator_class_name, argument_symmetry(streams))` | `frozenset` for commutative (Join, MergeJoin); `tuple` for ordered (SemiJoin) |

**Known exclusions from `identity_structure()`:**

| Class | Excluded | Reason |
|-------|----------|--------|
| `Tag` | System tag columns (`_tag::*`) | Provenance metadata, not content |
| `Data` | Source info columns (`_source_*`) | Provenance metadata, not content |
| `EmptyData` | All data | No payload; `content_hash()` overridden to return stored hash |

---

### 1b. `pipeline_hash()` — schema and topology only

The pipeline hash uses the same `SemanticAwarePythonHasher` with the **`pipeline_resolver`**. The key difference is what each class returns from `pipeline_identity_structure()`.

| Class | `pipeline_identity_structure()` return value | Notes |
|-------|----------------------------------------------|-------|
| `RootSource` | `(tag_schema, data_schema)` | Base case — schemas only, no data content |
| `DerivedSource` | `(tag_schema, data_schema)` | Acts as a new root in the pipeline Merkle chain |
| `DataFunctionBase` | `self.uri` (same as `identity_structure()`) | Function identity is already schema-only |
| `FunctionPod` | `self.data_function` **only** (excludes `ctx_arg_name`) | A ctx-aware pod and a regular pod sharing the same data function share a `pipeline_hash` and therefore the same DB table path |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams pipeline hashes))` | `pipeline_resolver` routes upstreams through `pipeline_hash()` |
| Operators | `(operator_class_name, argument_symmetry(upstream pipeline hashes))` | Same structure as content identity but using pipeline hashes of upstreams |
| `SideEffectPodStream` | `(pod, argument_symmetry(upstreams))` | Same as `identity_structure()` |

> **Note:** `FunctionPod.ctx_arg_name` IS included in `content_hash()` (via `identity_structure()`). It is excluded only from `pipeline_identity_structure()`. This means a ctx-aware pod and a plain pod using the same underlying data function write to and read from the same pipeline DB table — but they have different `content_hash` values and therefore different per-row memoization keys.

---
````

- [ ] **Step 2: Verify the file grows correctly**

Run: `wc -l docs/reference/hashing.md`
Expected: ~115–130 lines.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add Section 1 — Framework Object Identity (ITL-529)"
```

---

### Task 7: Write Section 2 — Source Provenance & System Tags

**Files:**
- Modify: `docs/reference/hashing.md` (append)

- [ ] **Step 1: Append Section 2**

````markdown
## 2. Source Provenance & System Tags

When a source is built by `SourceStreamBuilder`, four hash-related operations happen in
sequence before the stream is returned:

1. **Site 3 — Schema hash:** computed from `(tag_schema, data_schema)`; embedded in system tag column names.
2. **Site 4 — Default `source_id`:** derived from the raw table hash if no explicit `source_id` is given.
3. **Site 5 — Per-row `record_id`:** a UUID v5 computed from `(source_id, row_provenance_token)` and stored per row.
4. Two system tag columns, `_tag::source_id::{schema_hash}` and `_tag::record_id::{schema_hash}`, are appended to every row.

At Join time (site 6), all existing system tag columns are **renamed** by appending a
topology suffix, encoding the full join lineage into each column name.

---

### Site 3 — Schema hash

| Field | Value |
|---|---|
| **Inputs** | `(tag_schema, data_schema)` — Python `Schema` objects mapping column names to Python types |
| **Algorithm** | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(schema_n_char)` where `schema_n_char = OrcapodConfig.hashing.schema_n_char` |
| **Output format** | Truncated hex `str` (length = `schema_n_char`) |
| **Uniqueness guarantee** | Unique per `(tag_schema, data_schema)` pair; two sources with identical schemas produce identically-named system tag columns, enabling consistent system tag lookup across sources of the same schema |
| **Known exclusions** | No data content; no source identity; purely structural |

---

### Site 4 — Default `source_id`

| Field | Value |
|---|---|
| **Inputs** | Full raw Arrow table (all columns, before any system tag injection) |
| **Algorithm** | `StarfixArrowHasher.hash_table(table).to_hex(path_n_char)` — versioned SHA-256 via the `starfix` Rust crate |
| **Output format** | Truncated hex `str` (length = `path_n_char`) |
| **Uniqueness guarantee** | Unique per raw table content; changes if any cell value changes |
| **Known exclusions** | Used **only** as a fallback when no explicit `source_id` is provided. A user-supplied `source_id` bypasses this computation entirely. |

---

### Site 5 — Per-row `record_id` (system tag value)

| Field | Value |
|---|---|
| **Inputs** | `source_id` string + provenance token: `"{col}={value}"` when `record_id_column` is specified, otherwise `"row_{index}"` |
| **Algorithm** | `uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, f"{source_id}::{provenance_token}")` where `_SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "orcapod::record_id")` is a fixed constant |
| **Output format** | `bytes` (16 bytes, UUID v5 bit pattern), stored in a `pa.binary(16)` Arrow column |
| **Uniqueness guarantee** | Deterministic per `(source_id, row_identity)`; stable across identical re-runs of the same source |
| **Known exclusions** | When `record_id_column` is not specified, the row index is used as the provenance token; `record_id` changes if rows are **reordered** within the source table |

---

### Site 6 — Join system tag suffix

| Field | Value |
|---|---|
| **Inputs** | `stream.pipeline_hash()` of each canonically-ordered input stream + its 0-based canonical position index `idx` |
| **Algorithm** | Streams are first sorted by `stream.pipeline_hash().to_string()` for determinism. For each input at canonical position `idx`, every existing system tag column name has `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(n_char)}:{idx}` appended via `arrow_utils.append_to_system_tags()`. |
| **Output format** | Column name suffix; no separate value is stored |
| **Uniqueness guarantee** | Each post-join system tag column name uniquely identifies `(original schema, input topology, canonical join position)`; no collision even when joining streams with identical schemas |
| **Known exclusions** | `SemiJoin` passes system tags through unchanged. `Batch` changes the column type from `str` to `list[str]` but preserves the column name. |

> **Key insight for entry IDs:** Because `compute_base_entry_id()` (§3) calls
> `tag.as_table(columns={"system_tags": True})`, its preimage captures the full set of
> chained system tag column names. After a two-way join, the preimage contains four system
> tag columns — two per input — each with the join topology embedded in its name. The entry
> ID therefore implicitly commits to the complete join provenance graph without any
> join-awareness in the entry ID computation itself.

---
````

- [ ] **Step 2: Verify line count**

Run: `wc -l docs/reference/hashing.md`
Expected: ~185–205 lines.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add Section 2 — Source Provenance & System Tags (ITL-529)"
```

---

### Task 8: Write Section 3 — Pipeline DB Entry Keys

**Files:**
- Modify: `docs/reference/hashing.md` (append)

- [ ] **Step 1: Append Section 3**

````markdown
## 3. Pipeline DB Entry Keys

`FunctionJobNode` uses two related Arrow-based hashes as database primary keys. Both are
computed by `StarfixArrowHasher` over a single-row preimage table.

The **base entry ID** (site 7) is stable across all recomputation attempts for the same
logical input — it is used as an in-memory cache key and for Phase 1 DB lookups. The
**pipeline entry ID** (site 8) adds a `recomputation_index` column and is the actual
primary key stored in the pipeline DB.

The preimage for both sites is built by `_build_record_id_preimage(tag, input_data)`:

```
preimage = tag.as_table(columns={"system_tags": True})
         .append_column(INPUT_DATA_HASH_COL,
                        pa.array([input_data.content_hash().to_prefixed_digest()],
                                 type=pa.large_binary()))
```

Because `tag.as_table(columns={"system_tags": True})` includes all chained system tag
columns (see §2, site 6), the preimage implicitly captures the full join provenance of
the row.

---

### Site 7 — `compute_base_entry_id()`

| Field | Value |
|---|---|
| **Inputs** | All system tag columns from `tag.as_table(columns={"system_tags": True})` (including join-chained columns) + `INPUT_DATA_HASH_COL` (`input_data.content_hash().to_prefixed_digest()` as `pa.large_binary()`) |
| **Algorithm** | `StarfixArrowHasher.hash_table(preimage).to_prefixed_digest()` — single-row Arrow table hash |
| **Output format** | `bytes` in `b"{method}:{digest}"` format |
| **Uniqueness guarantee** | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts; used as the in-memory cache key and Phase 1 DB filter |
| **Known exclusions** | `NODE_CONTENT_HASH_COL` excluded — the node's content hash is fully determined by the pipeline DB table path (scoped by `pipeline_hash()`). Recomputation index excluded by design — use site 8 for a versioned key. |

---

### Site 8 — `compute_pipeline_entry_id()`

| Field | Value |
|---|---|
| **Inputs** | Same preimage as site 7 + `_PIPELINE_RECOMPUTATION_INDEX_COL` (value: `recomputation_index`, type `pa.int32()`; default `0`) |
| **Algorithm** | `StarfixArrowHasher.hash_table(preimage).to_prefixed_digest()` |
| **Output format** | `bytes` in `b"{method}:{digest}"` format |
| **Uniqueness guarantee** | Unique per `(node, tag lineage, input_data content, recomputation attempt)` — the primary key for all rows in the pipeline DB |
| **Known exclusions** | At `recomputation_index=0` this produces a hash that differs from the pre-ITL-508 implementation (the index column is now part of the preimage); existing pipeline DB records were intentionally invalidated when ITL-508 landed |

---
````

- [ ] **Step 2: Verify line count**

Run: `wc -l docs/reference/hashing.md`
Expected: ~250–270 lines.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add Section 3 — Pipeline DB Entry Keys (ITL-529)"
```

---

### Task 9: Write Section 4 — Side-Effect Record ID & Invocation Hash

**Files:**
- Modify: `docs/reference/hashing.md` (append)

- [ ] **Step 1: Append Section 4**

````markdown
## 4. Side-Effect Record ID & Invocation Hash

Side-effect pods (and `FunctionPod` instances with a `ctx_arg_name`) use a parallel but
distinct record key scheme. The key difference from §3 is the inclusion of
`NODE_CONTENT_HASH_COL` in the preimage — so that changing the pod's implementation
invalidates prior delivery records, even for identical inputs.

The `invocation_hash` string is composed from two `ContentHash` components and exposed to
the pod function as an idempotency key via `InvocationContext.invocation_hash`.

---

### Site 9 — Side-effect `record_id`

| Field | Value |
|---|---|
| **Inputs** | System tags + `INPUT_DATA_HASH_COL` (as `pa.large_string()`) + `NODE_CONTENT_HASH_COL` (pod's `content_hash().to_string()`, as `pa.large_string()`) + `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` (fixed `0`, `pa.int32()`) |
| **Algorithm** | `StarfixArrowHasher.hash_table(preimage)` |
| **Output format** | `ContentHash`; `.to_prefixed_digest()` → `bytes` when stored in the delivery log |
| **Uniqueness guarantee** | Unique per `(tag lineage, input_data content, pod version)`. Recomputation index is always `0` — side-effect pods do not version recomputations. |
| **Known exclusions** | Unlike sites 7–8, this includes `NODE_CONTENT_HASH_COL` — a deliberate difference ensuring that changing the pod's version invalidates the delivery record even for unchanged inputs |

---

### Site 10 — `invocation_hash`

| Field | Value |
|---|---|
| **Inputs** | `pipeline_hash_ch` — the `SideEffectPodStream`'s `pipeline_hash()` as `ContentHash` + `record_id_hash_ch` — the `ContentHash` from site 9 |
| **Algorithm** | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` where each component is serialised as `f"{method}:{hex_or_base64_digest}"` via `InvocationHashConfig` (default: hex, full digest) |
| **Output format** | `str` of the form `"{method}:{digest}::{method}:{digest}"` |
| **Uniqueness guarantee** | Unique per `(pod topology, tag lineage, input_data content, pod version)`; exposed to pod functions as an idempotency key |
| **Known exclusions** | When `track_completion=True` (default): `run_id` is **excluded** — hash is run-independent for idempotency. When `track_completion=False` **and** `pipeline_run_id` is set: `run_id` is appended as a third `::` component so that each run produces a distinct hash. |

---
````

- [ ] **Step 2: Verify line count**

Run: `wc -l docs/reference/hashing.md`
Expected: ~310–335 lines.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add Section 4 — Side-Effect Record ID and Invocation Hash (ITL-529)"
```

---

### Task 10: Write Sections 5 and 6 — Data Function URI Hash + Pipeline Run Identity

**Files:**
- Modify: `docs/reference/hashing.md` (append)

- [ ] **Step 1: Append Sections 5 and 6**

````markdown
## 5. Data Function URI Hash

`DataFunctionBase.uri` is a tuple used as the canonical identity of a data function:

```
uri = (canonical_function_name, output_schema_hash, major_version, data_function_type_id)
```

The `output_schema_hash` component (site 11) is the only hash in the URI; the other
components are plain strings.

---

### Site 11 — Output schema hash

| Field | Value |
|---|---|
| **Inputs** | `output_data_schema` — a `Schema` mapping output column names to Python types |
| **Algorithm** | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` |
| **Output format** | `str` (ContentHash string representation including method prefix, e.g. `"object_v0.1:abcd1234..."`) |
| **Uniqueness guarantee** | Unique per output schema definition; changing any output column name or type changes this hash and therefore the function's entire URI, `content_hash`, and `pipeline_hash` |
| **Known exclusions** | Input schema not included (changing input schema alone does not change the URI); function code not included (tracked separately via `major_version`); `data_function_type_id` not included (plain string component) |

---

## 6. Pipeline Run Identity

`PipelineJob.run()` generates three identifiers at execution time. None of these are used
in any data-level preimage; they serve logging, observability, and result inspection.

---

### Site 12 — `run_id`

| Field | Value |
|---|---|
| **Inputs** | None (random) |
| **Algorithm** | `uuid.uuid4().hex[:16]` |
| **Output format** | 16-char hex `str` |
| **Uniqueness guarantee** | Non-deterministic; unique per execution with overwhelming probability |
| **Known exclusions** | Does not reflect pipeline structure, data content, or any input. Two runs with identical pipelines and data produce different `run_id` values. |

---

### Site 13 — `snapshot_hash`

| Field | Value |
|---|---|
| **Inputs** | Sorted `content_hash().to_string()` values of all DAG **leaf** nodes (nodes with no downstream successors in the execution DAG) |
| **Algorithm** | `hashlib.sha256("\n".join(sorted_leaf_hashes).encode()).hexdigest()[:16]` |
| **Output format** | 16-char hex `str`; embedded in `pipeline_uri` as `{pipeline_name}@{snapshot_hash}` |
| **Uniqueness guarantee** | Unique per `(leaf node topology + data state)` at run time; changes if any leaf node's schema, function code, or source data changes |
| **Known exclusions** | Covers only leaf (sink) nodes — intermediate nodes not included. Truncated to 16 chars (collision-resistant in practice but not cryptographically guaranteed at this length). |

---

### Site 14 — `datagram_uuid`

| Field | Value |
|---|---|
| **Inputs** | Current wall-clock time (monotonic within a process) |
| **Algorithm** | `uuid_utils.uuid7()` normalised to `stdlib uuid.UUID` via `uuid.UUID(bytes=uuid7().bytes)` |
| **Output format** | `uuid.UUID` |
| **Uniqueness guarantee** | Unique per datagram instance; time-ordered (monotonically increasing within a process) |
| **Known exclusions** | **Not a content hash.** Two datagrams with identical content have different UUIDs. Not used in any hash preimage. Serves as an object identity token, not a content fingerprint. |

---
````

- [ ] **Step 2: Verify line count**

Run: `wc -l docs/reference/hashing.md`
Expected: ~400–430 lines.

- [ ] **Step 3: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add Sections 5+6 — Data Function URI Hash and Pipeline Run Identity (ITL-529)"
```

---

### Task 11: Write Section 7 — Worked Example (using script output)

**Files:**
- Modify: `docs/reference/hashing.md` (insert before Section 1, after the index table `---` divider)

> **Note:** This task inserts Section 7 *between* the index table and Section 1, so the reader sees concrete values before the detailed per-site reference. If the file was written sequentially in Tasks 5–10, the insert point is the line containing `## 1. Framework Object Identity`.

- [ ] **Step 1: Run the script (if output from Task 4 is not still in /tmp)**

Run: `uv run python superpowers/scripts/hash_audit_example.py 2>&1`

Capture the output. Identify the concrete hash values for each of the items below.

- [ ] **Step 2: Insert the worked example section before `## 1.`**

Open `docs/reference/hashing.md`. Find the line `## 1. Framework Object Identity`. Insert the following block **immediately before it**, replacing every `FILL_FROM_SCRIPT_OUTPUT` marker with the actual value from the script output (the comment next to each marker tells you which script output line to use):

````markdown
## 7. Worked Example

The pipeline below exercises all 14 hash sites. `source_id` values are set explicitly so
every content hash is reproducible across runs. Run the script to regenerate the values
in this section:

```
uv run python superpowers/scripts/hash_audit_example.py
```

### Example pipeline

```python
from orcapod.core.function_pod import function_pod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.operators.join import Join
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.side_effects import SideEffectPod, InvocationContext

scores = DictSource(
    [{"student": "alice", "math": 90}, {"student": "bob", "math": 75}],
    tag_columns=["student"],
    source_id="scores_v1",
)
attendance = DictSource(
    [{"student": "alice", "days": 180}, {"student": "bob", "days": 160}],
    tag_columns=["student"],
    source_id="attendance_v1",
)

joined = Join()(scores, attendance)      # sites 3–6

@function_pod(output_keys="grade")
def grade(math: int, days: int) -> float:
    return math * 0.7 + days / 180.0 * 30.0

pod  = grade.pod
node = FunctionJobNode(pod, joined, pipeline_database=InMemoryArrowDatabase())

captured: list[InvocationContext] = []
side_pod = SideEffectPod(lambda data, ctx: captured.append(ctx))
```

### Concrete hash values

#### §1 — Framework Object Identity

| Object | `content_hash()` | `pipeline_hash()` |
|--------|-----------------|-------------------|
| `scores` | `FILL_FROM_SCRIPT_OUTPUT`<!-- scores.content_hash() --> | `FILL_FROM_SCRIPT_OUTPUT`<!-- scores.pipeline_hash() --> |
| `attendance` | `FILL_FROM_SCRIPT_OUTPUT`<!-- attendance.content_hash() --> | `FILL_FROM_SCRIPT_OUTPUT`<!-- attendance.pipeline_hash() --> |
| `joined` | `FILL_FROM_SCRIPT_OUTPUT`<!-- joined.content_hash() --> | `FILL_FROM_SCRIPT_OUTPUT`<!-- joined.pipeline_hash() --> |
| `pod` | `FILL_FROM_SCRIPT_OUTPUT`<!-- pod.content_hash() --> | `FILL_FROM_SCRIPT_OUTPUT`<!-- pod.pipeline_hash() --> |
| `node` | `FILL_FROM_SCRIPT_OUTPUT`<!-- node.content_hash() --> | `FILL_FROM_SCRIPT_OUTPUT`<!-- node.pipeline_hash() --> |

Observation: `scores.pipeline_hash() == attendance.pipeline_hash()` — both sources have
the same schema `(tag: {student: str}, data: {math: int})` / `(tag: {student: str}, data: {days: int})`.
Wait — they differ because `math` ≠ `days` in the data schema. But `scores.pipeline_hash() ≠ attendance.pipeline_hash()` is expected.

#### §2 — Source Provenance & System Tags

| Site | Item | Value |
|------|------|-------|
| 3 | Schema hash | `FILL_FROM_SCRIPT_OUTPUT`<!-- schema_hash --> |
| 3 | System tag column names (scores) | `FILL_FROM_SCRIPT_OUTPUT`<!-- system tag columns --> |
| 4 | `scores.source_id` | `"scores_v1"` (explicit; no Arrow hash computed) |
| 5 | `alice` record_id bytes (hex) | `FILL_FROM_SCRIPT_OUTPUT`<!-- alice record_id --> |
| 5 | `bob` record_id bytes (hex) | `FILL_FROM_SCRIPT_OUTPUT`<!-- bob record_id --> |
| 6 | Post-join system tag column names | `FILL_FROM_SCRIPT_OUTPUT`<!-- post-join system tag cols --> |

#### §3 — Pipeline DB Entry Keys

| Site | Row | Value |
|------|-----|-------|
| 7 | `alice` `base_entry_id` (hex) | `FILL_FROM_SCRIPT_OUTPUT`<!-- alice base_entry_id --> |
| 7 | `bob` `base_entry_id` (hex) | `FILL_FROM_SCRIPT_OUTPUT`<!-- bob base_entry_id --> |
| 8 | `alice` `pipeline_entry_id` idx=0 (hex) | `FILL_FROM_SCRIPT_OUTPUT`<!-- alice pipeline_entry_id --> |
| 8 | `bob` `pipeline_entry_id` idx=0 (hex) | `FILL_FROM_SCRIPT_OUTPUT`<!-- bob pipeline_entry_id --> |

#### §4 — Side-Effect Record ID & Invocation Hash

| Site | Row | Value |
|------|-----|-------|
| 10 | `alice` `invocation_hash` | `FILL_FROM_SCRIPT_OUTPUT`<!-- alice invocation_hash --> |
| 10 | `bob` `invocation_hash` | `FILL_FROM_SCRIPT_OUTPUT`<!-- bob invocation_hash --> |

#### §5 — Data Function URI Hash

| Site | Item | Value |
|------|------|-------|
| 11 | `data_function.uri` | `FILL_FROM_SCRIPT_OUTPUT`<!-- data_function.uri --> |
| 11 | `output_schema_hash` (uri[1]) | `FILL_FROM_SCRIPT_OUTPUT`<!-- output_schema_hash --> |

#### §6 — Pipeline Run Identity

| Site | Item | Value |
|------|------|-------|
| 12 | `run_id` | *(non-deterministic — changes each run)* |
| 13 | `snapshot_hash` | `FILL_FROM_SCRIPT_OUTPUT`<!-- snapshot_hash --> |
| 14 | `datagram_uuid` | *(time-ordered UUID v7 — changes each run)* |

---
````

- [ ] **Step 3: Verify all `FILL_FROM_SCRIPT_OUTPUT` markers have been replaced**

Run: `grep -c "FILL_FROM_SCRIPT_OUTPUT" docs/reference/hashing.md`
Expected: `0`

- [ ] **Step 4: Verify line count**

Run: `wc -l docs/reference/hashing.md`
Expected: ~500–560 lines (depending on hash value string lengths).

- [ ] **Step 5: Commit**

```bash
git add docs/reference/hashing.md
git commit -m "docs(hashing): add Section 7 — Worked Example with concrete hash values (ITL-529)"
```

---

### Task 12: Wire mkdocs.yml, verify build, and open PR

**Files:**
- Modify: `mkdocs.yml`
- Delete: `docs/reference/.gitkeep` (no longer needed)
- Delete: `superpowers/scripts/.gitkeep` (no longer needed)

- [ ] **Step 1: Add the Reference section to mkdocs.yml nav**

Open `mkdocs.yml`. Find the `nav:` block. After the `API Reference:` section, add:

```yaml
  - Reference:
    - Hashing: reference/hashing.md
```

The full nav block should now end with:

```yaml
nav:
  - Home: index.md
  - Getting Started: getting-started.md
  - Concepts:
    - Sources: concepts/sources.md
    - Streams: concepts/streams.md
    - Operators: concepts/operators.md
    - Function Pods: concepts/function-pods.md
    - Identity & Hashing: concepts/identity.md
  - API Reference:
    - Overview: api/index.md
    - Sources: api/sources.md
    - Streams: api/streams.md
    - Operators: api/operators.md
    - Function Pods: api/function-pods.md
    - Databases: api/databases.md
    - Nodes: api/nodes.md
    - Pipeline: api/pipeline.md
    - Types: api/types.md
  - Reference:
    - Hashing: reference/hashing.md
```

- [ ] **Step 2: Remove placeholder .gitkeep files**

```bash
rm docs/reference/.gitkeep
rm superpowers/scripts/.gitkeep
```

- [ ] **Step 3: Verify the docs build without errors**

Run: `uv run mkdocs build --strict 2>&1 | tail -20`
Expected: Build completes with `INFO    -  Documentation built in ...s` and no `WARNING` or `ERROR` lines.

If it fails with `mkdocs not found`: install it first with `uv add --dev mkdocs mkdocs-material mkdocstrings[python]`.

If it warns about missing links or pages: check that the nav path `reference/hashing.md` matches the actual file location under `docs/`.

- [ ] **Step 4: Run the tests to confirm no regressions**

Run: `uv run pytest tests/ -x -q 2>&1 | tail -20`
Expected: All tests pass. This task adds documentation only — no test regressions are expected.

- [ ] **Step 5: Commit the mkdocs change and removed placeholders**

```bash
git add mkdocs.yml
git rm docs/reference/.gitkeep superpowers/scripts/.gitkeep
git commit -m "docs(hashing): wire docs/reference/hashing.md into mkdocs.yml nav (ITL-529)"
```

- [ ] **Step 6: Push the branch and open the PR**

```bash
git push -u origin eywalker/itl-529-audit-all-hash-computation-sites-document-inputs-algorithm
```

Then create the PR:

```bash
gh pr create \
  --title "docs: hash audit reference — all 14 sites with worked example (ITL-529)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds `docs/reference/hashing.md` — a developer reference cataloguing all 14 hash computation sites in orcapod-python, grouped by usage context (framework object identity, source provenance & system tags, pipeline DB entry keys, side-effect record ID & invocation hash, data function URI hash, pipeline run identity).
- Adds `superpowers/scripts/hash_audit_example.py` — a standalone runnable script that constructs a deterministic example pipeline and prints concrete hash values for all 14 sites.
- Wires the new reference page into the mkdocs nav under a new "Reference" section.

Fixes ITL-529

## Test plan

- [ ] `uv run python superpowers/scripts/hash_audit_example.py` runs to completion and prints all 6 sections with no errors
- [ ] `uv run mkdocs build --strict` passes with no warnings or errors
- [ ] `uv run pytest tests/ -x -q` passes (no code changes; tests are for regression only)
- [ ] All `FILL_FROM_SCRIPT_OUTPUT` markers replaced in the worked example section: `grep -c "FILL_FROM_SCRIPT_OUTPUT" docs/reference/hashing.md` returns `0`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

### Spec coverage

Reviewing `superpowers/specs/2026-07-21-itl-529-hash-audit-design.md` against this plan:

| Spec requirement | Covered by task |
|-----------------|----------------|
| Hash Site Index (14 rows) | Task 5 |
| Section 1: Framework Object Identity (content_hash + pipeline_hash) | Task 6 |
| Section 2: Source Provenance & System Tags (sites 3–6) | Task 7 |
| Section 3: Pipeline DB Entry Keys (sites 7–8) | Task 8 |
| Section 4: Side-Effect Record ID & Invocation Hash (sites 9–10) | Task 9 |
| Section 5: Data Function URI Hash (site 11) | Task 10 |
| Section 6: Pipeline Run Identity (sites 12–14) | Task 10 |
| Section 7: Worked Example with concrete values | Tasks 2–4 (script) + Task 11 (fill-in) |
| `superpowers/scripts/hash_audit_example.py` | Tasks 2–4 |
| mkdocs nav wired | Task 12 |
| Branch `eywalker/itl-529-...` | Task 1 |
| PR targeting `main` | Task 12 |

All 14 hash sites covered. ✓

### Placeholder scan

- Task 11 uses `FILL_FROM_SCRIPT_OUTPUT` markers with comments — these are instruction markers, not doc placeholders. Step 3 of Task 11 explicitly checks `grep -c "FILL_FROM_SCRIPT_OUTPUT" docs/reference/hashing.md` returns `0`, ensuring they are replaced before the commit. ✓
- No other TBD/TODO/placeholder patterns found. ✓

### Type consistency

- `compute_schema_hash` — imported from `orcapod.utils.schema_utils`; signature `(tag_schema, data_schema, semantic_hasher, char_count)` — used correctly in Task 2.
- `FunctionJobNode` — imported from `orcapod.core.nodes.function_node`; constructor `(function_pod, input_stream, pipeline_database=...)` — used correctly in Task 3.
- `SideEffectPod` — imported from `orcapod.side_effects`; constructor `(fn, name=...)` — used correctly in Task 3.
- `PipelineJob` — imported from `orcapod.pipeline.job`; `with PipelineJob(name=..., store=...)` pattern — used correctly in Task 4.
- `job._run_id` — accessed after `job.run()`; private attribute; confirmed in job.py line 851. ✓
- `job._graph_edges` / `job._persistent_node_map` — accessed after compile+run; confirmed in base.py. ✓
