# NPIPE-204 — Tag-based grouping via a `GroupBy` operator (many→one reduction)

**Linear:** [NPIPE-204](https://linear.app/metamorphic/issue/NPIPE-204/orcapod-add-tag-based-grouping-to-batch-manyone-reduction-fix-batch)
**Branch:** `arnoldb/npipe-204-orcapod-add-tag-based-grouping-to-batch-manyone-reduction`
**Base rev:** `966d759a`

> **API deviation from the issue.** NPIPE-204 specifies `Batch(group_by=[...])`. This spec
> introduces a separate `GroupBy(by=[...])` operator instead — see *Why a separate operator*.
> The Linear issue and the downstream `common_clock_op` wiring both need updating to match.

## Overview

Every existing orcapod pipeline fans *out*. Nothing reduces. The consumer that motivates this
change — `common_clock_op` in orcapod-sync-and-qc — produces one `AlignmentResult` per recording
session, computed from *all* of that session's spikeglx-sync result parquets at once. Expressing
that requires a many→one operator keyed on tag values, which orcapod does not have.

Two independent problems, verified against `966d759a`:

1. **No tag-based reduction exists.** `Batch` groups by row count only; there is no way to say
   "one packet per `(subject, date)`".
2. **List-valued source info breaks `job.run()`.** The `Data` datagram cannot represent a
   non-scalar `_source_*` value.

Reproduction of (2):

```
pyarrow.lib.ArrowTypeError: Expected bytes, got a 'list' object
  core/nodes/operator_node.py:946          execute
  core/operators/static_output_pod.py:214  _materialize_to_stream
  core/datagrams/tag_data.py:433           as_table
  core/datagrams/tag_data.py:342           _ensure_source_info_table
```

### Problem 2 is not Batch-specific

`MergeJoin` merges colliding data columns into `list[T]` and carries their `_source_*` columns
along as parallel lists (`merge_join.py:262`). It therefore fails with the identical
`ArrowTypeError` inside `job.run()`, reproduced independently of `Batch`. The root cause is in the
`Data` datagram, not in either operator.

This is the already-logged `DESIGN_ISSUES.md` **U1 — Source-info column type hard-coded to
`large_string`** (severity: critical), whose recorded location was the sibling call site
`arrow_utils.add_source_info_to_table()`. Part 1 fixes the `tag_data.py` half; the `arrow_utils.py`
half stays open.

### Why a separate operator

`Batch` exists for throughput and pipelining — its `async_execute` docstring is explicit that
batching lets "downstream consumers start processing before all input is consumed" — and its git
history contains only refactors. Semantic reduction was never its intent.

Folding grouping into `Batch` would give one class two meaningfully different output contracts,
selected by which kwarg the caller passed, guarded by a mutually-exclusive-args check. Splitting
gives each class one contract:

| | `Batch` | `GroupBy` |
|---|---|---|
| Partitions by | row count | tag values |
| Purpose | throughput / pipelining | many→one reduction |
| Output tags | list-valued, unchanged | scalar group keys |
| Async | streams when `batch_size > 0` | always a barrier |

`Batch` is then semantically untouched by this change — it receives only the provenance fix. All
new reduction semantics live in `GroupBy`, and its barrier behavior is structural rather than a
conditional inside a streaming operator.

## Goals & Success Criteria

* `GroupBy(by=["subject", "date"])` emits one packet per distinct tag tuple, with the group keys as
  scalar tag columns and the members as list-valued data columns.
* `GroupBy`, `Batch`, and `MergeJoin` all survive `job.run()` end to end.
* Memoization over a grouped stream holds across two identical runs and invalidates when a single
  member's data changes.
* Group member order and the folded provenance digests are deterministic **across processes**, so
  an unchanged member set never produces a different list hash or a cache miss on a new driver run.
* No pipeline-DB schema version bump.

## Scope & Boundaries

In scope:
* `core/datagrams/tag_data.py` — type-aware source info on `Data`.
* `core/operators/group_by.py` — new operator.
* `core/operators/batch.py` — provenance handling only.
* `utils/arrow_utils.py` — shared system-tag fold helper.
* `utils/polars_data_utils.py` — delete dead `add_source_info`.
* Operator-level and job-level tests, including a `MergeJoin` job-level regression test.

Out of scope:
* `arrow_utils.add_source_info_to_table()` (the other half of U1).
* Operators discarding a non-default `_context_key` (new DESIGN_ISSUES entry, not fixed here).
* Incremental emission for `GroupBy` — see *Async execution*.
* The downstream rev bump in orcapod-sync-and-qc and orcapod-spikesorting.

---

## Part 1 — Type-aware source info in `Data`

`Data` stores per-data-column provenance tokens in `self._source_info`, surfaced as `_source_*`
columns. Two places assume those values are always scalar strings:

* `_ensure_source_info_table()` (`tag_data.py:330`) builds the Arrow schema as
  `pa.field(k, pa.large_string())` for every key.
* `Data.schema()` (`tag_data.py:384`) sets `schema[f"{SOURCE_PREFIX}{key}"] = str` for every key.

Both become derived from the stored value:

| Stored value | Arrow type | Python type |
|---|---|---|
| `str` | `large_string` | `str` |
| `None` | `large_string` | `str` |
| `list[str]` | `large_list(large_string)` | `list[str]` |
| nested list | `large_list(<elem>)`, recursive | `list[...]` |
| `[]` | `large_list(large_string)` | `list[str]` |

`None` keeps mapping to `large_string`, so unknown-provenance columns behave exactly as today.

`self._source_info`'s annotation widens from `dict[str, str | None]` to a recursive
`SourceInfoValue = str | None | list["SourceInfoValue"]`, with `source_info()`,
`with_source_info()`, `rename()`, and `with_columns()` following. The dict and table construction
paths in `__init__` already pass values through untouched — the table path recovers lists correctly
via `to_pylist()`.

**No schema version bump.** A node's `_source_*` column type is fixed by that node's own output
schema. A `FunctionNode` downstream of a `GroupBy` writes list-typed source columns from its first
record; every pre-existing node keeps `large_string`. Nothing re-reads an old table under a new
type.

This part alone fixes `MergeJoin` inside `job.run()`.

### Third hard-coded site: delete it

`polars_data_utils.add_source_info` (line 119) forces `dtype=pl.String()`, making it a third site
with the same assumption. It is dead code — nothing in `src/` calls it (only `drop_system_columns`
is imported from that module), and the tests that import `add_source_info` import it from
`arrow_utils`. It also carries a latent shadowing bug: `source_column` is rebound to a `pl.Series`
inside the per-column loop, so from the second column onward it formats `f"{<Series repr>}::{col}"`.

Delete the function rather than fix it. Greenfield pre-v0.1.0 means no back-compat obligation, and
removing it eliminates the site outright instead of leaving a trap for whoever wires it up later.

---

## Part 2a — `Batch` provenance fix

`Batch`'s partitioning, its list-valued tag columns, and its streaming `async_execute` are all
unchanged. The only change: `_source_*` columns keep their list-valued form (now representable
thanks to Part 1), and the system-tag columns fold to scalars via the shared helper below instead of
being list-wrapped.

That is the minimum needed to make plain `Batch(batch_size=N)` work inside `job.run()`.

---

## Part 2b — The `GroupBy` operator

New module `core/operators/group_by.py`. A `UnaryOperator`.

### Constructor and validation

```python
def __init__(self, by: Collection[str], **kwargs):
```

* Empty `by` → `ValueError`.
* `validate_unary_input` raises `InputValidationError` if any name in `by` is not a tag column of
  the input stream.

### Partitioning

Rows are keyed on the tuple of their group-key tag values and accumulated into a plain dict, so
first-seen group order is preserved in the output.

### Member ordering

Within a group, members are sorted by the tuple of their **non-group-key tag values**, falling back
to the member's system `record_id` bytes when `by` covers every tag column. Tags are unique within a
stream, so this is a total order, and it does not depend on which data column happens to hold a
path. Sort keys wrap each value as `(v is None, v)` so nulls order consistently without comparing
against `None`.

This matters because orcapod hashes the emitted list to build the cache key. Upstream emission order
is not stable across runs (Ray executor scheduling, DB fetch order), so an unsorted list would make
an identical member set hash differently and trigger a spurious recompute.

### Column treatment

| Column class | Result |
|---|---|
| group-key tags | **scalar**, remain tag columns |
| other user tags | list-valued **data** columns |
| data columns | list-valued |
| `_source_*` | list-valued, one element per member |
| `_tag_source_id` / `_tag_record_id` | **scalar digest**, name extended |

Non-key tags become list-valued *data* rather than being dropped, so a consumer can tell which
member each list element came from. Those promoted columns have no provenance token, so
`Data.source_info()` reports `None` for them — its existing behavior for unknown keys.

`_context_key` does not appear: `stream.as_table(columns={"source": True, "system_tags": True})`
returns only user tags, data, system tags, and `_source_*`. Every operator in the repo uses that
same column set, and `ArrowTableStream` re-defaults the context key on construction. `GroupBy`
therefore cannot silently pick one member's context. That layer-wide behavior is logged separately.

Nullability: list-wrapped columns are `nullable=False` (a group always has at least one member, so
the list itself is never null). Scalar group-key columns inherit the input column's nullable flag.

### System tag folding

`_build_record_id_preimage` (`core/nodes/function_node.py:82`) computes a record's identity from the
system-tag columns plus a hash of the input data. System tags must therefore be scalar. A many→one
operator has to define how N members' system tags collapse into one record's provenance.

**Rule:** fold each system-tag column independently over its own ordered member values, preserving
the column's declared Arrow type, then extend the column name with
`{BLOCK_SEPARATOR}{pipeline_hash}` via the existing `arrow_utils.append_to_system_tags` — mirroring
the name-extending rule already used by joins.

Both primitives are reused from the codebase rather than invented, and both are **SHA-based and
stable across processes**. Neither `hash()` nor set iteration order may appear anywhere in the fold:

| Column | Arrow type | Fold |
|---|---|---|
| `_tag_source_id::<h>` | `large_string` | `hash_utils.combine_hashes(*member_source_ids, order=False)` — SHA-256 hex |
| `_tag_record_id::<h>` | `binary(16)` | `uuid.uuid5(_GROUP_RECORD_ID_NAMESPACE, "::".join(rid.hex() for rid in member_record_ids)).bytes` |

`order=False` preserves member order, which is already deterministic from the sort above, so the
digest reflects both the member set and its order — matching the data lists it accompanies.
`uuid5` is the same construction `stream_builder._make_record_id` (line 55) already uses to mint
record IDs, and `.bytes` is exactly the 16 bytes `pa.binary(16)` wants. Record IDs are globally
unique (uuid5 over `source_id::token`), so folding them alone captures full member identity.

Why this rule:

* **Invalidation is correct.** Any member whose record identity changes changes the digest, so the
  downstream record_id changes and the cache misses. This is strictly stronger than relying on the
  input-data hash alone, which would miss an upstream recompute that produced identical data.
* **Nothing else has to change.** `function_node.py` and the pdb schema are untouched.
  `_tag_record_id` is consumed only for identity and sorting (`join.py:687`,
  `arrow_utils.py:1101/1169/1235`), never as a key to look a record up, so synthesizing a value
  cannot break a lookup.
* **Member identities remain recoverable** from the list-valued `_source_*` columns, which Part 1
  now preserves per member.

The extended name `_tag_source_id::<schema_hash>::<pipeline_hash>` has no trailing `:position`, so
`_parse_system_tag_column` returns `None` for it and `sort_system_tag_values` skips it. That is
correct — `GroupBy` is unary, so there is no cross-input commutativity to normalize.

Two alternatives were rejected. Having `GroupBy` mint a fresh source-like identity severs the Merkle
link to the member records. Keeping system tags list-valued and teaching `_build_record_id_preimage`
to hash lists is the most information-preserving option but changes the record-identity machinery
and the pdb column types, requiring a v1→v2 migration on top of an already cross-repo change.

The fold lives in `arrow_utils` as a shared helper, next to `append_to_system_tags` and
`sort_system_tag_values`, since both `Batch` and `GroupBy` need it.

### Output schema

`unary_output_schema` must mirror the table exactly: scalar group keys in the tag schema, promoted
non-key tags and list-wrapped data columns in the data schema, list-typed `_source_*` entries when
`columns={"source": True}`, and renamed scalar system-tag entries when
`columns={"system_tags": True}`. The operator predicts this without performing the computation,
consistent with every other operator — including for empty input, which yields zero groups but the
same schema.

### Registration

Four sites, mirroring `Batch`:

* `core/operators/group_by.py` — the module.
* `core/operators/__init__.py` — import and `__all__`.
* `pipeline/serialization.py:_build_operator_registry` — import and registry entry.
* `core/streams/base.py` — a `group_by(by, label=None)` fluent method alongside `batch()`.

`to_config()` and `identity_structure()` both include `by`. `from_config` needs no change — it
already forwards `config["config"]` as kwargs.

### Async execution

`async_execute` is barrier-only: no group can be emitted before the input channel closes, because
any row not yet seen could belong to a group already started.

Under `AsyncPipelineOrchestrator` this stalls one node, not the pipeline. Upstream nodes still run
concurrently and stream into the `GroupBy`; downstream resumes full concurrency once the barrier
releases, fanning the N groups out to N concurrent invocations. The unavoidable cost is that the
last straggler member in *any* group delays the first downstream invocation for *every* group.

This is not a regression relative to what already exists: `Batch(batch_size=0)` takes the barrier
path today (`batch.py:118`), and `SyncPipelineOrchestrator` is node-at-a-time regardless. `Batch`'s
`batch_size=N>0` streaming path is untouched by this change.

Emitting groups early would require a guarantee that input arrives clustered by group key. orcapod
streams carry no ordering guarantee, so that would need an unverifiable `assume_grouped=True`
opt-in. Deliberately not built.

---

## Part 3 — Tests

### Operator-level (`tests/test_core/operators/`)

* `GroupBy` produces one row per distinct tag tuple; group keys scalar, members list-valued.
* Non-key tags are promoted to list-valued data columns, not dropped.
* Empty `by` raises `ValueError`; `by` naming a non-tag column raises `InputValidationError`.
* Members are sorted by non-key tags — the same rows fed in two different orders yield
  byte-identical output tables.
* Fallback ordering by `record_id` when `by` covers every tag column.
* `unary_output_schema` matches `as_table().schema` for every `ColumnConfig` combination.
* System tag columns are scalar and renamed; `_source_*` columns are list-valued with one element
  per member.
* Empty input → zero groups, schema still predicted correctly.
* Existing `TestBatchBehavior` cases continue to pass unchanged, plus new assertions that `Batch`
  emits scalar system tags and list-valued `_source_*`.

### Cross-process digest stability

The failure this guards against: a fold that accidentally used `hash()` or set-iteration order
would pass every same-process test — including the reordering test, which only checks byte-identity
*within* one run — and then miss the cache on every new driver run.

A test runs the fold in a `subprocess` (fresh interpreter, therefore fresh `PYTHONHASHSEED`) over a
fixed member list and asserts the digests equal hard-coded expected values.

### Job-level (new)

The gap that let problem 2 ship: `Batch`'s existing tests are all `op.process(stream)` followed by
`as_table()`. None run through `job.run()`.

Backed by a `DeltaTableDatabase` store:

* `GroupBy` completes end to end inside `job.run()` feeding a `@function_pod` that takes a
  `list[str]` parameter.
* `Batch(batch_size=N)` likewise — the provenance fix is independent of grouping.
* **Memoization holds:** two identical `job.run()` calls; the pod body executes only on the first.
* **Memoization invalidates:** a fixture with **at least two groups**; change one file in one
  group's list and assert only that group's pod invocation re-runs while the other stays cached.
  With a single group the assertion would be vacuous.
* **`MergeJoin` regression:** a `MergeJoin` with colliding data columns completes inside
  `job.run()`.

---

## Dependencies & Risks

* `orcapod-python` is upstream (`walkerlab/orcapod-python`); orcapod-sync-and-qc pins
  `nauticalab/orcapod-python @ 966d759a`. This needs an upstream PR followed by a coordinated rev
  bump in **both** orcapod-sync-and-qc and orcapod-spikesorting, which are deliberately kept on the
  same rev because they target the same Ray cluster.
* The `GroupBy` split means NPIPE-204's stated API and the documented `common_clock_op` wiring both
  need updating before the downstream change is written.
* The schedule risk is the upstream PR and two-repo rev bump, not the code.

## Resources & References

* `DESIGN_ISSUES.md` §U1 — source-info type hard-coding (this change fixes the `tag_data.py` half
  and deletes the dead `polars_data_utils` site).
* `DESIGN_ISSUES.md` §O2 — operators discard a non-default `_context_key` (logged by this change,
  not fixed).
* `CLAUDE.md` §"System tag evolution rules" — needs updating. Rule 3 currently describes `Batch` as
  purely type-evolving (`str` → `list[str]`), which is true only of user tag and data columns;
  system tags fold to a scalar and extend their name. `GroupBy` needs adding as a fourth,
  reducing category, along with a row in the operator/function-pod boundary table and the project
  layout tree.
